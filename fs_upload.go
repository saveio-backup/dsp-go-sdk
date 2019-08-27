package dsp

import (
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/saveio/dsp-go-sdk/actor/client"
	"github.com/saveio/dsp-go-sdk/common"
	serr "github.com/saveio/dsp-go-sdk/error"
	"github.com/saveio/dsp-go-sdk/network/message"
	"github.com/saveio/dsp-go-sdk/network/message/types/file"
	"github.com/saveio/dsp-go-sdk/task"
	"github.com/saveio/dsp-go-sdk/utils"
	chainCom "github.com/saveio/themis/common"
	"github.com/saveio/themis/common/log"
	"github.com/saveio/themis/crypto/pdp"

	fs "github.com/saveio/themis/smartcontract/service/native/savefs"
)

// fetchedDone. handle fetched request channel data structure
type fetchedDone struct {
	done bool
	err  error
}

// blockMsgData. data to send of block msg
type blockMsgData struct {
	blockData []byte
	dataLen   uint64
	tag       []byte
	offset    uint64
}

// CalculateUploadFee. pre-calculate upload cost by its upload options
func (this *Dsp) CalculateUploadFee(opt *fs.UploadOption) (*fs.StorageFee, error) {
	return this.Chain.Native.Fs.GetUploadStorageFee(opt)
}

// UploadTaskExist. check if upload task is existed by filePath hash string
func (this *Dsp) UploadTaskExist(filePath string) (bool, error) {
	taskId := this.taskMgr.TaskId(filePath, this.WalletAddress(), task.TaskTypeUpload)
	if len(taskId) == 0 {
		return false, nil
	}
	opt, err := this.taskMgr.GetFileUploadOptions(taskId)
	if err != nil || opt == nil {
		return true, fmt.Errorf("get file upload option is nil %s", err)
	}
	now, err := this.Chain.GetCurrentBlockHeight()
	if err != nil {
		log.Errorf("get current block height err %s", err)
		return true, err
	}
	if opt.ExpiredHeight > uint64(now) {
		return true, nil
	}
	fileHashStr := this.taskMgr.TaskFileHash(taskId)
	if len(fileHashStr) == 0 {
		return true, fmt.Errorf("file hash not found for %s", taskId)
	}
	log.Debugf("upload task exist, but expired, delete it")
	err = this.taskMgr.DeleteTask(taskId, true)
	if err != nil {
		return true, err
	}
	return false, nil
}

// UploadFile upload new file logic synchronously
func (this *Dsp) UploadFile(taskId, filePath string, opt *fs.UploadOption) (*common.UploadResult, error) {
	var uploadRet *common.UploadResult
	var sdkerr *serr.SDKError
	var err error
	newTask := false
	if len(taskId) == 0 {
		taskId, err = this.taskMgr.NewTask(task.TaskTypeUpload)
		newTask = true
	}
	this.taskMgr.EmitProgress(taskId, task.TaskUploadFileMakeSlice)
	// emit result finally
	defer func() {
		log.Debugf("emit result finally id %v, ret %v err %v, %v", taskId, uploadRet, err, sdkerr)
		if uploadRet == nil {
			this.taskMgr.EmitResult(taskId, nil, sdkerr)
		} else {
			this.taskMgr.EmitResult(taskId, uploadRet, sdkerr)
		}
		// delete task from cache in the end when success
		if uploadRet != nil && sdkerr == nil {
			this.taskMgr.DeleteTask(taskId, false)
		}
	}()
	if err != nil {
		sdkerr = serr.NewDetailError(serr.NEW_TASK_FAILED, err.Error())
		return nil, err
	}
	// check options valid
	if err = uploadOptValid(filePath, opt); err != nil {
		sdkerr = serr.NewDetailError(serr.INVALID_PARAMS, err.Error())
		return nil, err
	}
	err = this.taskMgr.SetFilePath(taskId, filePath)
	if err != nil {
		sdkerr = serr.NewDetailError(serr.SET_FILEINFO_DB_ERROR, err.Error())
		return nil, err
	}
	err = this.taskMgr.SetFileUploadOptions(taskId, opt)
	if err != nil {
		sdkerr = serr.NewDetailError(serr.SET_FILEINFO_DB_ERROR, err.Error())
		return nil, err
	}
	this.taskMgr.SetTaskInfos(taskId, "", filePath, string(opt.FileDesc), this.WalletAddress())
	this.taskMgr.SetCopyNum(taskId, uint64(opt.CopyNum))
	this.taskMgr.BindTaskId(taskId)
	this.taskMgr.EmitProgress(taskId, task.TaskUploadFileMakeSlice)
	var tx, fileHashStr string
	var totalCount uint64
	log.Debugf("upload task: %s, will split for file", taskId)
	// split file to pieces
	pause, sdkerr := this.checkIfPause(taskId, "")
	if sdkerr != nil {
		return nil, errors.New(sdkerr.Error.Error())
	}
	if pause {
		return nil, nil
	}
	filePrefix := &utils.FilePrefix{
		Version:    utils.PREFIX_VERSION,
		Encrypt:    opt.Encrypt,
		EncryptPwd: string(opt.EncryptPassword),
		Owner:      this.CurrentAccount().Address,
		FileSize:   opt.FileSize,
	}
	prefixStr := filePrefix.String()
	log.Debugf("node from file prefix: %v, len: %d", prefixStr, len(prefixStr))
	hashes, err := this.Fs.NodesFromFile(filePath, prefixStr, opt.Encrypt, string(opt.EncryptPassword))
	if err != nil {
		sdkerr = serr.NewDetailError(serr.SHARDING_FAIELD, err.Error())
		log.Errorf("node from file err: %s", err)
		return nil, err
	}

	totalCount = uint64(len(hashes))
	log.Debugf("node from file finished, taskId:%s, path: %s, fileHash: %s, totalCount:%d", taskId, filePath, hashes[0], totalCount)
	if totalCount == 0 {
		err = errors.New("sharding failed no blocks")
		sdkerr = serr.NewDetailError(serr.SHARDING_FAIELD, err.Error())
		log.Errorf("node from file err: %s", err)
		return nil, err
	}
	this.taskMgr.EmitProgress(taskId, task.TaskUploadFileMakeSliceDone)
	fileHashStr = hashes[0]
	log.Debugf("after bind task id")
	err = this.taskMgr.BatchSetFileInfo(taskId, fileHashStr, nil, nil, totalCount)
	if err != nil {
		sdkerr = serr.NewDetailError(serr.SET_FILEINFO_DB_ERROR, err.Error())
		return nil, err
	}
	if newTask {
		hasUploading := this.checkFileHasUpload(taskId, hashes[0])
		if hasUploading {
			err = errors.New("file has uploading or uploaded, please cancel the task")
			sdkerr = serr.NewDetailError(serr.UPLOAD_TASK_EXIST, err.Error())
			return nil, err
		}
	}
	err = this.taskMgr.BindTaskId(taskId)
	if err != nil {
		sdkerr = serr.NewDetailError(serr.SET_FILEINFO_DB_ERROR, err.Error())
		return nil, err
	}
	log.Debugf("check if pause after node from file")
	pause, sdkerr = this.checkIfPause(taskId, fileHashStr)
	if sdkerr != nil {
		return nil, errors.New(sdkerr.Error.Error())
	}
	if pause {
		return nil, nil
	}
	// check has uploaded this file
	isUploaded := this.taskMgr.IsFileUploaded(taskId)
	if isUploaded {
		uploadRet, sdkerr = this.finishUpload(taskId, fileHashStr, opt, totalCount)
		if sdkerr != nil {
			return nil, errors.New(sdkerr.Error.Error())
		}
		log.Debug("file:%s has uploaded upload success uploadRet: %v!!", fileHashStr, uploadRet)
		return uploadRet, nil
	}
	log.Debugf("root:%s, list.len:%d", fileHashStr, totalCount)
	// get nodeList
	nodeList := this.getUploadNodeFromDB(taskId, fileHashStr)
	if len(nodeList) == 0 {
		nodeList, err = this.getUploadNodeFromChain(filePath, taskId, fileHashStr, int((opt.CopyNum+1)*10))
		if err != nil {
			sdkerr = serr.NewDetailError(serr.GET_STORAGE_NODES_FAILED, err.Error())
			return nil, err
		}
	}
	log.Debugf("uploading nodelist %v, opt.CopyNum %d", nodeList, opt.CopyNum)
	if uint64(len(nodeList)) < opt.CopyNum+1 {
		sdkerr = serr.NewDetailError(serr.ONLINE_NODES_NOT_ENOUGH, fmt.Sprintf("node is not enough %d, copyNum %d", len(nodeList), opt.CopyNum))
		return nil, sdkerr.Error
	}
	this.taskMgr.EmitProgress(taskId, task.TaskUploadFileFindReceivers)
	receivers, err := this.waitFileReceivers(taskId, fileHashStr, prefixStr, nodeList, hashes, int(opt.CopyNum)+1)
	if err != nil {
		sdkerr = serr.NewDetailError(serr.SEARCH_RECEIVERS_FAILED, err.Error())
		return nil, err
	}
	log.Debugf("receivers:%v", receivers)
	if len(receivers) < int(opt.CopyNum)+1 {
		err = errors.New("file receivers is not enough")
		sdkerr = serr.NewDetailError(serr.RECEIVERS_NOT_ENOUGH, err.Error())
		return nil, err
	}
	if len(receivers) > int(opt.CopyNum)+1 {
		receivers = receivers[:int(opt.CopyNum)+1]
	}
	this.taskMgr.EmitProgress(taskId, task.TaskUploadFilePaying)
	pause, sdkerr = this.checkIfPause(taskId, fileHashStr)
	if sdkerr != nil {
		return nil, errors.New(sdkerr.Error.Error())
	}
	if pause {
		return nil, nil
	}
	// pay file
	payRet, err := this.payForSendFile(filePath, taskId, fileHashStr, totalCount, opt)
	if err != nil {
		sdkerr = serr.NewDetailError(serr.PAY_FOR_STORE_FILE_FAILED, err.Error())
		return nil, err
	}
	payTxHeight, err := this.Chain.GetBlockHeightByTxHash(payRet.Tx)
	if err != nil {
		sdkerr = serr.NewDetailError(serr.PAY_FOR_STORE_FILE_FAILED, err.Error())
		return nil, err
	}
	log.Debugf("pay for send file success %v %d", payRet, payTxHeight)
	pause, sdkerr = this.checkIfPause(taskId, fileHashStr)
	if sdkerr != nil {
		return nil, errors.New(sdkerr.Error.Error())
	}
	if pause {
		return nil, nil
	}
	this.taskMgr.EmitProgress(taskId, task.TaskUploadFilePayingDone)
	_, sdkerr = this.addWhitelist(taskId, fileHashStr, opt)
	log.Debugf("add whitelist sdkerr %v", sdkerr)
	if sdkerr != nil {
		return nil, errors.New(sdkerr.Error.Error())
	}
	tx = payRet.Tx
	p, err := this.Chain.Native.Fs.ProveParamDes(payRet.ParamsBuf)
	if err != nil {
		sdkerr = serr.NewDetailError(serr.GET_PDP_PARAMS_ERROR, err.Error())
		return nil, err
	}
	if opt != nil && opt.Share {
		go this.shareUploadedFile(filePath, string(opt.FileDesc), this.WalletAddress(), hashes)
	}
	if !opt.Share && opt.Encrypt {
		// TODO: delete encrypted block store
	}
	pause, sdkerr = this.checkIfPause(taskId, fileHashStr)
	if sdkerr != nil {
		return nil, errors.New(sdkerr.Error.Error())
	}
	if pause {
		return nil, nil
	}
	// notify fetch ready to all receivers
	err = this.notifyFetchReady(taskId, fileHashStr, receivers, tx, uint64(payTxHeight))
	if err != nil {
		sdkerr = serr.NewDetailError(serr.RECEIVERS_REJECTED, err.Error())
		return nil, err
	}
	pause, sdkerr = this.checkIfPause(taskId, fileHashStr)
	if sdkerr != nil {
		return nil, errors.New(sdkerr.Error.Error())
	}
	if pause {
		return nil, nil
	}
	this.taskMgr.EmitProgress(taskId, task.TaskUploadFileFindReceiversDone)

	// TODO: set max count
	maxFetchRoutines := len(receivers)
	if maxFetchRoutines > common.MAX_UPLOAD_ROUTINES {
		maxFetchRoutines = common.MAX_UPLOAD_ROUTINES
	}
	sdkerr = this.waitForFetchBlock(taskId, hashes, maxFetchRoutines, int(opt.CopyNum), p.G0, p.FileId, payRet.PrivateKey)
	log.Debugf("waitForFetchBlock finish id: %s, %v ", taskId, sdkerr)
	if sdkerr != nil {
		return nil, errors.New(sdkerr.Error.Error())
	}
	pause, sdkerr = this.checkIfPause(taskId, fileHashStr)
	if sdkerr != nil {
		return nil, errors.New(sdkerr.Error.Error())
	}
	if pause {
		return nil, nil
	}
	uploadRet, sdkerr = this.finishUpload(taskId, fileHashStr, opt, totalCount)
	if sdkerr != nil {
		return nil, errors.New(sdkerr.Error.Error())
	}
	log.Debug("upload success uploadRet: %v!!", uploadRet)
	return uploadRet, nil
}

// PauseUpload. pause a task
func (this *Dsp) PauseUpload(taskId string) error {
	taskType := this.taskMgr.TaskType(taskId)
	if taskType != task.TaskTypeUpload {
		return fmt.Errorf("task %s is not a upload task", taskId)
	}
	canPause, err := this.taskMgr.IsTaskCanPause(taskId)
	if err != nil {
		log.Errorf("pause task err %s", err)
		return err
	}
	if !canPause {
		log.Debugf("task is pausing")
		return nil
	}
	err = this.taskMgr.SetTaskState(taskId, task.TaskStatePause)
	if err != nil {
		return err
	}
	this.taskMgr.EmitProgress(taskId, task.TaskPause)
	return nil
}

func (this *Dsp) ResumeUpload(taskId string) error {
	taskType := this.taskMgr.TaskType(taskId)
	if taskType != task.TaskTypeUpload {
		return fmt.Errorf("task %s is not a upload task", taskId)
	}
	canResume, err := this.taskMgr.IsTaskCanResume(taskId)
	if err != nil {
		log.Errorf("resume task err %s", err)
		return err
	}
	if !canResume {
		log.Debugf("task is resuming")
		return nil
	}
	err = this.taskMgr.SetTaskState(taskId, task.TaskStateDoing)
	if err != nil {
		log.Errorf("resume task err %s", err)
		return err
	}
	this.taskMgr.EmitProgress(taskId, task.TaskDoing)
	return this.checkIfResume(taskId)
}

func (this *Dsp) CancelUpload(taskId string) (*common.DeleteUploadFileResp, error) {
	taskType := this.taskMgr.TaskType(taskId)
	if taskType != task.TaskTypeUpload {
		return nil, fmt.Errorf("task %s is not a upload task", taskId)
	}
	cancel, err := this.taskMgr.IsTaskCancel(taskId)
	if err != nil {
		return nil, err
	}
	if cancel {
		return nil, fmt.Errorf("task is cancelling: %s", taskId)
	}
	fileHashStr := this.taskMgr.TaskFileHash(taskId)
	oldState := this.taskMgr.GetTaskState(taskId)
	err = this.taskMgr.SetTaskState(taskId, task.TaskStateCancel)
	if err != nil {
		return nil, err
	}
	log.Debugf("fileHashStr :%v", fileHashStr)
	if len(fileHashStr) == 0 {
		err = this.taskMgr.DeleteTask(taskId, true)
		if err == nil {
			return nil, nil
		}
		log.Errorf("delete task err %s", err)
		this.taskMgr.SetTaskState(taskId, oldState)
		return nil, fmt.Errorf("delete task failed %v", fileHashStr, err)
	}
	nodeList := this.taskMgr.GetUploadedBlockNodeList(taskId, fileHashStr, 0)
	if len(nodeList) == 0 {
		resps, err := this.DeleteUploadedFiles([]string{fileHashStr})
		if err != nil {
			this.taskMgr.SetTaskState(taskId, oldState)
			return nil, err
		}
		if len(resps) == 0 {
			return nil, fmt.Errorf("delete file but not response %s", fileHashStr)
		}
		return resps[0], nil
	}
	// send pause msg
	msg := message.NewFileFetchCancel(taskId, fileHashStr)
	ret, err := client.P2pBroadcast(nodeList, msg.ToProtoMsg(), true, nil, nil)
	log.Debugf("broadcast cancel msg ret %v, err: %s", ret, err)
	resps, err := this.DeleteUploadedFiles([]string{fileHashStr})
	if err != nil {
		this.taskMgr.SetTaskState(taskId, oldState)
		return nil, err
	}
	if len(resps) == 0 {
		return nil, fmt.Errorf("delete file but not response %s", fileHashStr)
	}
	return resps[0], nil
}

func (this *Dsp) RetryUpload(taskId string) error {
	taskType := this.taskMgr.TaskType(taskId)
	if taskType != task.TaskTypeUpload {
		return fmt.Errorf("task %s is not a upload task", taskId)
	}
	failed, err := this.taskMgr.IsTaskFailed(taskId)
	if err != nil {
		return err
	}
	if !failed {
		return fmt.Errorf("task %s is not failed", taskId)
	}
	err = this.taskMgr.SetTaskState(taskId, task.TaskStateDoing)
	if err != nil {
		return err
	}
	this.taskMgr.EmitProgress(taskId, task.TaskDoing)
	return this.checkIfResume(taskId)
}

func (this *Dsp) DeleteUploadFilesFromChain(fileHashStrs []string) (string, uint32, *serr.SDKError) {
	if len(fileHashStrs) == 0 {
		return "", 0, &serr.SDKError{Code: serr.DELETE_FILE_HASHES_EMPTY, Error: errors.New("delete file hash string is empty")}
	}
	needDeleteFile := false
	for _, fileHashStr := range fileHashStrs {
		info, err := this.Chain.Native.Fs.GetFileInfo(fileHashStr)
		log.Debugf("delete file get fileinfo %v, err %v", info, err)
		if err != nil && !strings.Contains(err.Error(), "[FS Profit] FsGetFileInfo not found") {
			log.Debugf("info:%v, other err:%s", info, err)
			return "", 0, &serr.SDKError{Code: serr.FILE_NOT_FOUND_FROM_CHAIN, Error: fmt.Errorf("file info not found, %s has deleted", fileHashStr)}
		}
		if info != nil && info.FileOwner.ToBase58() != this.WalletAddress() {
			return "", 0, &serr.SDKError{Code: serr.DELETE_FILE_ACCESS_DENIED, Error: fmt.Errorf("file %s can't be deleted, you are not the owner", fileHashStr)}
		}
		if info != nil && err == nil {
			needDeleteFile = true
		}
	}
	if !needDeleteFile {
		return "", 0, &serr.SDKError{Code: serr.NO_FILE_NEED_DELETED, Error: errors.New("no file to delete")}
	}
	txHash, err := this.Chain.Native.Fs.DeleteFiles(fileHashStrs)
	log.Debugf("delete file tx %v, err %v", txHash, err)
	if err != nil {
		return "", 0, &serr.SDKError{Code: serr.CHAIN_ERROR, Error: err}
	}
	txHashStr := hex.EncodeToString(chainCom.ToArrayReverse(txHash))
	log.Debugf("delete file txHash %s", txHashStr)
	confirmed, err := this.Chain.PollForTxConfirmed(time.Duration(common.TX_CONFIRM_TIMEOUT)*time.Second, txHash)
	if err != nil || !confirmed {
		return "", 0, &serr.SDKError{Code: serr.CHAIN_ERROR, Error: errors.New("wait for tx confirmed failed")}
	}
	txHeight, err := this.Chain.GetBlockHeightByTxHash(txHashStr)
	log.Debugf("delete file tx height %d, err %v", txHeight, err)
	if err != nil {
		return "", 0, &serr.SDKError{Code: serr.CHAIN_ERROR, Error: err}
	}
	return txHashStr, txHeight, nil
}

// DeleteUploadedFile. Delete uploaded file from remote nodes. it is called by the owner
func (this *Dsp) DeleteUploadedFiles(fileHashStrs []string) ([]*common.DeleteUploadFileResp, error) {
	if len(fileHashStrs) == 0 {
		return nil, errors.New("delete file hash string is empty")
	}
	txHashStr, txHeight, sdkErr := this.DeleteUploadFilesFromChain(fileHashStrs)
	if sdkErr != nil && sdkErr.Code != serr.NO_FILE_NEED_DELETED {
		return nil, sdkErr.Error
	}
	resps := make([]*common.DeleteUploadFileResp, 0, len(fileHashStrs))
	log.Debugf("resps-len :%v", len(resps))
	for _, fileHashStr := range fileHashStrs {
		storingNode := this.getFileProvedNode(fileHashStr)
		taskId := this.taskMgr.TaskId(fileHashStr, this.WalletAddress(), task.TaskTypeUpload)
		log.Debugf("taskId of to delete file %v", taskId)
		if len(storingNode) == 0 {
			// find breakpoint keep nodelist
			storingNode = append(storingNode, this.taskMgr.GetUploadedBlockNodeList(taskId, fileHashStr, 0)...)
			log.Debugf("storingNode: %v", storingNode)
		}
		log.Debugf("will broadcast delete msg to %v", storingNode)
		resp := &common.DeleteUploadFileResp{
			Tx:       txHashStr,
			FileHash: fileHashStr,
			FileName: this.taskMgr.FileNameFromTask(taskId),
		}
		if len(storingNode) == 0 {
			err := this.taskMgr.DeleteTask(taskId, true)
			log.Debugf("delete task donne ")
			if err != nil {
				log.Errorf("delete upload info from db err: %s", err)
				return nil, err
			}
			log.Debugf("delete task success %v", resp)
			resps = append(resps, resp)
			continue
		}

		log.Debugf("send delete msg to nodes :%v", storingNode)
		msg := message.NewFileDelete(taskId, fileHashStr, this.WalletAddress(), txHashStr, uint64(txHeight))
		nodeStatusLock := new(sync.Mutex)
		nodeStatus := make([]common.DeleteFileStatus, 0, len(storingNode))
		reply := func(msg proto.Message, addr string) {
			ackMsg := message.ReadMessage(msg)
			nodeStatusLock.Lock()
			defer nodeStatusLock.Unlock()
			errCode := uint32(0)
			errMsg := ""
			if ackMsg.Error != nil {
				errCode = ackMsg.Error.Code
				errMsg = ackMsg.Error.Message
			}
			nodeStatus = append(nodeStatus, common.DeleteFileStatus{
				HostAddr: addr,
				Code:     errCode,
				Error:    errMsg,
			})
		}
		m, err := client.P2pBroadcast(storingNode, msg.ToProtoMsg(), true, nil, reply)
		resp.Nodes = nodeStatus
		log.Debugf("send delete msg done ret: %v, nodeStatus: %v, err: %s", m, nodeStatus, err)
		err = this.taskMgr.DeleteTask(taskId, true)
		if err != nil {
			log.Errorf("delete upload info from db err: %s", err)
			return nil, err
		}
		log.Debugf("delete task success %v", resp)
		resps = append(resps, resp)
	}
	return resps, nil
}

func (this *Dsp) checkIfPause(taskId, fileHashStr string) (bool, *serr.SDKError) {
	pause, err := this.taskMgr.IsTaskPause(taskId)
	if err != nil {
		sdkerr := serr.NewDetailError(serr.TASK_PAUSE_ERROR, err.Error())
		return false, sdkerr
	}
	if !pause {
		return false, nil
	}
	if len(fileHashStr) == 0 {
		return pause, nil
	}
	nodeList := this.taskMgr.GetUploadedBlockNodeList(taskId, fileHashStr, 0)
	if len(nodeList) == 0 {
		return pause, nil
	}
	// send pause msg
	msg := message.NewFileFetchPause(taskId, fileHashStr)
	ret, err := client.P2pBroadcast(nodeList, msg.ToProtoMsg(), true, nil, nil)
	if err != nil {
		sdkerr := serr.NewDetailError(serr.TASK_PAUSE_ERROR, err.Error())
		return false, sdkerr
	}
	log.Debugf("broadcast pause msg ret %v", ret)
	return pause, nil
}

func (this *Dsp) checkIfResume(taskId string) error {
	filePath, err := this.taskMgr.GetFilePath(taskId)
	if err != nil {
		return err
	}
	opt, err := this.taskMgr.GetFileUploadOptions(taskId)
	if err != nil {
		return err
	}
	if opt == nil {
		return errors.New("can't find download options, please retry")
	}
	log.Debugf("get upload options : %v", opt)
	fileHashStr := this.taskMgr.TaskFileHash(taskId)
	// send resume msg
	if len(fileHashStr) == 0 {
		go this.UploadFile(taskId, filePath, opt)
		return nil
	}
	fileInfo, _ := this.Chain.Native.Fs.GetFileInfo(fileHashStr)
	if fileInfo != nil {
		now, err := this.Chain.GetCurrentBlockHeight()
		if err != nil {
			return err
		}
		if fileInfo.ExpiredHeight <= uint64(now) {
			return fmt.Errorf("file:%s has expired", fileHashStr)
		}
		hasProvedFile := uint64(len(this.getFileProvedNode(fileHashStr))) == opt.CopyNum+1
		hasSentAllBlock := uint64(this.taskMgr.UploadedBlockCount(taskId)) == fileInfo.FileBlockNum*(fileInfo.CopyNum+1)
		if hasProvedFile || hasSentAllBlock {
			log.Debugf("hasProvedFile: %t, hasSentAllBlock: %t", hasProvedFile, hasSentAllBlock)
			uploadRet, sdkerr := this.finishUpload(taskId, fileHashStr, opt, fileInfo.FileBlockNum)
			if uploadRet == nil {
				this.taskMgr.EmitResult(taskId, nil, sdkerr)
			} else {
				this.taskMgr.EmitResult(taskId, uploadRet, sdkerr)
			}
			// delete task from cache in the end when success
			if uploadRet != nil && sdkerr == nil {
				this.taskMgr.DeleteTask(taskId, false)
			}
			return nil
		}
	}
	nodeList := this.taskMgr.GetUploadedBlockNodeList(taskId, fileHashStr, 0)
	if len(nodeList) == 0 {
		go this.UploadFile(taskId, filePath, opt)
		return nil
	}
	// drop all pending request first
	req, err := this.taskMgr.TaskBlockReq(taskId)
	if err == nil && req != nil {
		log.Warnf("drain request len: %d", len(req))
		for len(req) > 0 {
			<-req
		}
		log.Warnf("drain request len: %d", len(req))
	}
	msg := message.NewFileFetchResume(taskId, fileHashStr)
	_, err = client.P2pBroadcast(nodeList, msg.ToProtoMsg(), true, nil, nil)
	if err != nil {
		return err
	}
	log.Debugf("resume upload file")
	go this.UploadFile(taskId, filePath, opt)
	return nil
}

// checkFileHasUpload. check the file is uploading or uploaded
func (this *Dsp) checkFileHasUpload(taskId, fileHashStr string) bool {
	info, _ := this.Chain.Native.Fs.GetFileInfo(fileHashStr)
	if info != nil {
		return true
	}
	return this.taskMgr.UploadingFileExist(taskId, fileHashStr)
}

// payForSendFile pay before send a file
// return PoR params byte slice, PoR private key of the file or error
func (this *Dsp) payForSendFile(filePath, taskId, fileHashStr string, blockNum uint64, opt *fs.UploadOption) (*common.PayStoreFileResult, error) {
	fileInfo, _ := this.Chain.Native.Fs.GetFileInfo(fileHashStr)
	var paramsBuf, privateKey []byte
	var tx string
	if fileInfo == nil {
		log.Info("first upload file")

		blockSizeInKB := uint64(math.Ceil(float64(common.CHUNK_SIZE) / 1024.0))
		g, g0, pubKey, privKey, fileID := pdp.Init(filePath)
		var err error
		paramsBuf, err = this.Chain.Native.Fs.ProveParamSer(g, g0, pubKey, fileID)
		privateKey = privKey
		if err != nil {
			log.Errorf("serialization prove params failed:%s", err)
			return nil, err
		}
		txHash, err := this.Chain.Native.Fs.StoreFile(fileHashStr, blockNum, blockSizeInKB, opt.ProveInterval,
			opt.ExpiredHeight, uint64(opt.CopyNum), []byte(opt.FileDesc), uint64(opt.Privilege), paramsBuf, uint64(opt.StorageType), opt.FileSize)
		if err != nil {
			log.Errorf("pay store file order failed:%s", err)
			return nil, err
		}
		tx = hex.EncodeToString(chainCom.ToArrayReverse(txHash))
		// TODO: check confirmed on receivers
		confirmed, err := this.Chain.PollForTxConfirmed(time.Duration(common.TX_CONFIRM_TIMEOUT)*time.Second, txHash)
		if err != nil || !confirmed {
			return nil, errors.New("tx is not confirmed")
		}
		err = this.taskMgr.SetStoreTx(taskId, tx)
		if err != nil {
			return nil, err
		}
		err = this.taskMgr.SetPrivateKey(taskId, privateKey)
		log.Debugf("set private key for task %s, key: %d", taskId, len(privateKey))
		if err != nil {
			return nil, err
		}
	} else {
		paramsBuf = fileInfo.FileProveParam
		var err error
		privateKey, err = this.taskMgr.GetFilePrivateKey(taskId)
		log.Debugf("get private key from task %s, key: %d", taskId, len(privateKey))
		if err != nil {
			return nil, err
		}
		tx, err = this.taskMgr.GetStoreTx(taskId)
		if err != nil {
			return nil, err
		}
	}
	if len(paramsBuf) == 0 || len(privateKey) == 0 {
		log.Errorf("param buf len: %d, private key len: %d", len(paramsBuf), len(privateKey))
		return nil, errors.New("PDP private key is not found. Please delete file and retry to upload it")
	}
	return &common.PayStoreFileResult{
		Tx:         tx,
		ParamsBuf:  paramsBuf,
		PrivateKey: privateKey,
	}, nil
}

func (this *Dsp) addWhitelist(taskId, fileHashStr string, opt *fs.UploadOption) (string, *serr.SDKError) {
	var addWhiteListTx string
	var err error
	// Don't add whitelist if resume upload
	if opt.WhiteList.Num == 0 {
		return "", nil
	}
	tx, err := this.taskMgr.GetWhitelistTx(taskId)
	if err != nil {
		return "", serr.NewDetailError(serr.GET_FILEINFO_FROM_DB_ERROR, err.Error())
	}
	if len(tx) > 0 {
		log.Debugf("%s get tx %s from db", taskId, tx)
		return tx, nil
	}
	this.taskMgr.EmitProgress(taskId, task.TaskUploadFileCommitWhitelist)
	addWhiteListTx, err = this.addWhitelistsForFile(fileHashStr, opt.WhiteList.List)
	log.Debugf("add whitelist tx, err %s", err)
	if err != nil {
		return "", serr.NewDetailError(serr.ADD_WHITELIST_ERROR, err.Error())
	}
	err = this.taskMgr.SetWhitelistTx(taskId, addWhiteListTx)
	if err != nil {
		return "", serr.NewDetailError(serr.SET_FILEINFO_DB_ERROR, err.Error())
	}
	this.taskMgr.EmitProgress(taskId, task.TaskUploadFileCommitWhitelistDone)
	return addWhiteListTx, nil
}

func (this *Dsp) registerUrls(taskId, fileHashStr, saveLink string, opt *fs.UploadOption, totalCount uint64) (string, string) {
	this.taskMgr.EmitProgress(taskId, task.TaskUploadFileRegisterDNS)
	var dnsRegTx, dnsBindTx string
	var err error
	if opt.RegisterDNS && len(opt.DnsURL) > 0 {
		dnsRegTx, err = this.RegisterFileUrl(string(opt.DnsURL), saveLink)
		if err != nil {
			log.Errorf("register url err: %s", err)
		}
		log.Debugf("acc %s, reg dns %s", this.Chain.Native.Dns.DefAcc.Address.ToBase58(), dnsRegTx)
	}
	if opt.BindDNS && len(opt.DnsURL) > 0 {
		dnsBindTx, err = this.BindFileUrl(string(opt.DnsURL), saveLink)
		if err != nil {
			log.Errorf("bind url err: %s", err)
		}
		log.Debugf("bind dns %s", dnsBindTx)
	}
	this.taskMgr.EmitProgress(taskId, task.TaskUploadFileRegisterDNSDone)
	return dnsRegTx, dnsBindTx
}

// addWhitelistsForFile. add whitelist for file with added blockcount to current height
func (this *Dsp) addWhitelistsForFile(fileHashStr string, rules []fs.Rule) (string, error) {
	txHash, err := this.Chain.Native.Fs.AddWhiteLists(fileHashStr, rules)
	log.Debugf("err %v", err)
	if err != nil {
		return "", err
	}
	tx := hex.EncodeToString(chainCom.ToArrayReverse(txHash))
	_, err = this.Chain.PollForTxConfirmed(time.Duration(common.TX_CONFIRM_TIMEOUT)*time.Second, txHash)
	if err != nil {
		return "", err
	}
	return tx, nil
}

// getFileProveNode. get file storing nodes  and stored (expired) nodes
func (this *Dsp) getFileProvedNode(fileHashStr string) []string {
	proveDetails, err := this.Chain.Native.Fs.GetFileProveDetails(fileHashStr)
	if proveDetails == nil {
		return nil
	}
	nodes := make([]string, 0)
	log.Debugf("details :%v, err:%s", len(proveDetails.ProveDetails), err)
	for _, detail := range proveDetails.ProveDetails {
		log.Debugf("node:%s, prove times %d", string(detail.NodeAddr), detail.ProveTimes)
		if detail.ProveTimes == 0 {
			continue
		}
		nodes = append(nodes, string(detail.NodeAddr))
	}
	return nodes
}

func (this *Dsp) getUploadNodeFromDB(taskId, fileHashStr string) []string {
	nodeList := this.taskMgr.GetUploadedBlockNodeList(taskId, fileHashStr, 0)
	if len(nodeList) != 0 {
		return nodeList
	}
	return nil
}

// getUploadNodeList get nodelist from cache first. if empty, get nodelist from chain
func (this *Dsp) getUploadNodeFromChain(filePath, taskKey, fileHashStr string, count int) ([]string, error) {
	fileStat, err := os.Stat(filePath)
	if err != nil {
		return nil, err
	}
	nodeList := make([]string, 0)
	infos, err := this.Chain.Native.Fs.GetNodeList()
	if err != nil {
		return nil, err
	}
	for _, info := range infos.NodeInfo {
		fileSizeInKb := uint64(math.Ceil(float64(fileStat.Size()) / 1024.0))
		if info.RestVol < fileSizeInKb {
			continue
		}
		fullAddress := string(info.NodeAddr)
		if fullAddress == client.P2pGetPublicAddr() {
			continue
		}
		nodeList = append(nodeList, fullAddress)
		if len(nodeList) >= count {
			break
		}
	}
	return nodeList, nil
}

// checkFileBeProved thread-block method. check if the file has been proved for all store nodes.
func (this *Dsp) checkFileBeProved(fileHashStr string, copyNum uint64) bool {
	retry := 0
	timewait := 5
	for {
		if retry > common.CHECK_PROVE_TIMEOUT/timewait {
			return false
		}
		nodes := this.getFileProvedNode(fileHashStr)
		if uint64(len(nodes)) >= copyNum+1 {
			break
		}
		retry++
		time.Sleep(time.Duration(timewait) * time.Second)
	}
	return true
}

// waitFileReceivers find nodes and send file give msg, waiting for file fetch msg
// client -> fetch_ask -> peer.
// client <- fetch_ack <- peer.
// return receivers
func (this *Dsp) waitFileReceivers(taskId, fileHashStr, prefix string, nodeList, blockHashes []string, receiverCount int) ([]string, error) {
	var receiverLock sync.Mutex
	receivers := make([]string, 0)
	receiverBreakpoint := make(map[string]*file.Breakpoint, 0)
	sessionId, err := this.taskMgr.GetSessionId(taskId, "")
	if err != nil {
		return nil, err
	}
	log.Debugf("waitFileReceivers prefix hex: %s", hex.EncodeToString([]byte(prefix)))
	msg := message.NewFileFetchAsk(sessionId, fileHashStr, blockHashes, this.WalletAddress(), []byte(prefix))
	type responseData struct {
		res  proto.Message
		addr string
	}
	responseCh := make(chan *responseData, 0)
	action := func(res proto.Message, addr string) {
		log.Debugf("send file ask msg success %s", addr)
		responseCh <- &responseData{
			res:  res,
			addr: addr,
		}
	}
	// TODO: make stop func sense in parallel mode
	stop := func() bool {
		isStop := false
		receiverLock.Lock()
		isStop = int32(len(receivers)) >= int32(receiverCount)
		receiverLock.Unlock()
		log.Debugf("stop :%t\n", isStop)
		return isStop
	}
	go func() {
		for {
			data := <-responseCh
			log.Debugf("receive response %v", data)
			p2pMsg := message.ReadMessage(data.res)
			if p2pMsg.Error != nil && p2pMsg.Error.Code != serr.SUCCESS {
				log.Errorf("get file fetch_ack msg err %s", p2pMsg.Error.Message)
				continue
			}
			// waiting for ack msg
			fileMsg := p2pMsg.Payload.(*file.File)
			receiverLock.Lock()
			receivers = append(receivers, data.addr)
			receiverBreakpoint[data.addr] = fileMsg.Breakpoint
			receiverLock.Unlock()
			log.Debugf("send file_ask msg success of file: %s, to: %s, receivers: %v", fileMsg.Hash, data.addr, receivers)
			if len(receivers) >= receiverCount || len(receivers) >= len(nodeList) {
				log.Debugf("break receiver goroutine")
				return
			}
			log.Debugf("continue....")
		}
	}()
	log.Debugf("broadcast fetch_ask msg of file: %s to %v", fileHashStr, nodeList)
	ret, err := client.P2pBroadcast(nodeList, msg.ToProtoMsg(), true, stop, action)
	if err != nil {
		log.Errorf("wait file receivers broadcast err")
		return nil, err
	}
	log.Debugf("receives :%v, receiverCount %v", receivers, receiverCount)
	if len(receivers) > receiverCount {
		log.Warnf("unbelievable not matched receiver count")
		receivers = append([]string{}, receivers[:receiverCount]...)
	}
	// check breakpoint
	// for _, addr := range receivers {

	// }
	log.Debugf("receives :%v, ret %v", receivers, ret)
	return receivers, nil
}

// notifyFetchReady send fetch_rdy msg to receivers
func (this *Dsp) notifyFetchReady(taskId, fileHashStr string, receivers []string, tx string, txHeight uint64) error {
	sessionId, err := this.taskMgr.GetSessionId(taskId, "")
	if err != nil {
		return err
	}
	log.Debugf("send ready msg %s %d", tx, txHeight)
	msg := message.NewFileFetchRdy(sessionId, fileHashStr, this.WalletAddress(), tx, txHeight)
	this.taskMgr.SetTaskTransferring(taskId, true)
	ret, err := client.P2pBroadcast(receivers, msg.ToProtoMsg(), false, nil, nil)
	if err != nil {
		log.Errorf("notify err %s", err)
		this.taskMgr.SetTaskTransferring(taskId, false)
		return err
	}
	for _, e := range ret {
		if e != nil {
			log.Errorf("notify err %s", err)
			this.taskMgr.SetTaskTransferring(taskId, false)
			return e
		}
	}
	return nil
}

// generateBlockMsgData. generate blockdata, blockEncodedData, tagData with prove params
func (this *Dsp) generateBlockMsgData(hash string, index uint32, offset uint64, fileID, g0, privateKey []byte) (*blockMsgData, error) {
	// TODO: use disk fetch rather then memory access
	block := this.Fs.GetBlock(hash)
	blockData := this.Fs.BlockDataOfAny(block)
	tag, err := pdp.SignGenerate(blockData, fileID, uint32(index+1), g0, privateKey)
	if err != nil {
		return nil, err
	}
	msgData := &blockMsgData{
		blockData: blockData,
		tag:       tag,
		offset:    offset,
	}
	return msgData, nil
}

// waitForFetchBlock. wait for fetched blocks concurrent
func (this *Dsp) waitForFetchBlock(taskId string, hashes []string, maxFetchRoutines, copyNum int, g0, fileID, privateKey []byte) *serr.SDKError {
	req, err := this.taskMgr.TaskBlockReq(taskId)
	if err != nil {
		return serr.NewDetailError(serr.PREPARE_UPLOAD_ERROR, err.Error())
	}
	this.taskMgr.EmitProgress(taskId, task.TaskUploadFileGeneratePDPData)
	allOffset, err := this.Fs.GetAllOffsets(hashes[0])
	if err != nil {
		return serr.NewDetailError(serr.PREPARE_UPLOAD_ERROR, err.Error())
	}
	this.taskMgr.EmitProgress(taskId, task.TaskUploadFileTransferBlocks)
	timeout := time.NewTimer(time.Duration(common.BLOCK_FETCH_TIMEOUT) * time.Second)
	sessionId, err := this.taskMgr.GetSessionId(taskId, "")
	if err != nil {
		return serr.NewDetailError(serr.GET_SESSION_ID_FAILED, err.Error())
	}
	fileHashStr := this.taskMgr.TaskFileHash(taskId)
	totalCount := uint64(len(hashes))
	stateChange := this.taskMgr.TaskStateChange(taskId)
	doneCh := make(chan *fetchedDone)
	closeDoneChannel := uint32(0)
	defer func() {
		log.Debugf("close doneCh")
		// close(doneCh)
	}()
	var getMsgDataLock sync.Mutex
	blockMsgDataMap := make(map[string]*blockMsgData)
	getMsgData := func(hash string, index uint32) *blockMsgData {
		getMsgDataLock.Lock()
		defer getMsgDataLock.Unlock()
		key := keyOfUnixNode(hash, index)
		data, ok := blockMsgDataMap[key]
		if ok {
			return data
		}
		offset, _ := allOffset[hash]
		var err error
		data, err = this.generateBlockMsgData(hash, index, offset, fileID, g0, privateKey)
		if err != nil {
			return nil
		}
		blockMsgDataMap[key] = data
		return data
	}
	cleanMsgData := func(hash string, index uint32) {
		// TODO
	}
	cancelFetch := make(chan struct{})
	log.Debugf("open %d go routines for fetched file %s, stateChange: %v", maxFetchRoutines, fileHashStr, stateChange)
	for i := 0; i < maxFetchRoutines; i++ {
		go func() {
			for {
				select {
				case reqInfo := <-req:
					pause, cancel, _ := this.taskMgr.IsTaskPauseOrCancel(taskId)
					if pause || cancel {
						log.Debugf("stop handle request because task is pause: %t, cancel: %t", pause, cancel)
						return
					}
					timeout.Reset(time.Duration(common.BLOCK_FETCH_TIMEOUT) * time.Second)
					key := keyOfUnixNode(reqInfo.Hash, uint32(reqInfo.Index))
					log.Debugf("handle request key:%s, %s-%s-%d from peer: %s", key, fileHashStr, reqInfo.Hash, reqInfo.Index, reqInfo.PeerAddr)
					msgData := getMsgData(reqInfo.Hash, uint32(reqInfo.Index))
					// handle fetch request async
					done, err := this.handleFetchBlockRequest(taskId, sessionId, fileHashStr, reqInfo, copyNum, totalCount, msgData)
					pause, cancel, _ = this.taskMgr.IsTaskPauseOrCancel(taskId)
					if pause || cancel {
						log.Debugf("stop handle request because task is pause: %t, cancel: %t", pause, cancel)
						return
					}
					if done == false && err == nil {
						cleanMsgData(reqInfo.Hash, uint32(reqInfo.Index))
						continue
					}
					swapped := atomic.CompareAndSwapUint32(&closeDoneChannel, 0, 1)
					if !swapped {
						log.Debugf("stop because done channel has close")
						return
					}
					doneCh <- &fetchedDone{done: done, err: err}
					log.Debugf("stop because fetching has finish %t, err %s", done, err)
					return
				case _, ok := <-doneCh:
					if !ok {
						log.Debugf("stop rotines")
						return
					}
				case _, ok := <-cancelFetch:
					if !ok {
						log.Debugf("fetch cancel")
						return
					}
				}
			}
		}()
	}
	for {
		select {
		case ret, ok := <-doneCh:
			if !ok {
				log.Debugf("done channel has close")
			}
			log.Debugf("receive fetch file done channel done: %v, err: %s", ret.done, ret.err)
			if ret.err != nil {
				close(cancelFetch)
				return serr.NewDetailError(serr.TASK_INTERNAL_ERROR, ret.err.Error())
			}
			if !ret.done {
				log.Debugf("no done break")
				break
			}
			// check all blocks has sent
			log.Infof("all block has sent %s", taskId)
			this.taskMgr.EmitProgress(taskId, task.TaskUploadFileTransferBlocksDone)
			return nil
		case <-timeout.C:
			close(cancelFetch)
			sent := uint64(this.taskMgr.UploadedBlockCount(taskId) / (uint64(copyNum) + 1))
			log.Debugf("timeout check totalCount %d != sent %d %d", totalCount, sent, this.taskMgr.UploadedBlockCount(taskId))
			if totalCount != sent {
				err = errors.New("wait for fetch block timeout")
				return serr.NewDetailError(serr.TASK_WAIT_TIMEOUT, err.Error())
			}
			return nil
		case newState := <-stateChange:
			log.Debugf("receive stateChange newState: %d", newState)
			switch newState {
			case task.TaskStatePause, task.TaskStateCancel:
				close(cancelFetch)
				return nil
			}
		}
	}
	return nil
}

// handleFetchBlockRequest. handle fetch request and send block
func (this *Dsp) handleFetchBlockRequest(taskId, sessionId, fileHashStr string,
	reqInfo *task.GetBlockReq,
	copyNum int,
	totalCount uint64,
	blockMsgData *blockMsgData) (bool, error) {
	// send block
	log.Debugf("receive fetch block msg of %s-%s-%d-%d from %s", reqInfo.FileHash, reqInfo.Hash, reqInfo.Index, totalCount, reqInfo.PeerAddr)
	if len(blockMsgData.blockData) == 0 || len(blockMsgData.tag) == 0 {
		err := fmt.Errorf("block len %d, tag len %d hash %s, peer %s failed", len(blockMsgData.blockData), len(blockMsgData.tag), reqInfo.Hash, reqInfo.PeerAddr)
		return false, err
	}
	isStored := this.taskMgr.IsBlockUploaded(taskId, reqInfo.Hash, reqInfo.PeerAddr, uint32(reqInfo.Index))
	if isStored {
		log.Debugf("block has stored %s", reqInfo.Hash)
		return false, nil
	}
	msg := message.NewBlockMsg(sessionId, int32(reqInfo.Index), fileHashStr, reqInfo.Hash, blockMsgData.blockData, blockMsgData.tag, int64(blockMsgData.offset))
	log.Debugf("sending block %s, index:%d, taglen:%d, offset:%d to %s", reqInfo.Hash, reqInfo.Index, len(blockMsgData.tag), blockMsgData.offset, reqInfo.PeerAddr)
	_, err := client.P2pRequestWithRetry(msg.ToProtoMsg(), reqInfo.PeerAddr, common.MAX_SEND_BLOCK_RETRY)
	if err != nil {
		log.Errorf("send block msg hash %s to peer %s failed, err %s", reqInfo.Hash, reqInfo.PeerAddr, err)
		return false, err
	}
	log.Debugf("send block success %s-%s-%d, taglen:%d, offset:%d to %s", fileHashStr, reqInfo.Hash, reqInfo.Index, len(blockMsgData.tag), blockMsgData.offset, reqInfo.PeerAddr)
	// stored
	this.taskMgr.AddUploadedBlock(taskId, reqInfo.Hash, reqInfo.PeerAddr, uint32(reqInfo.Index), blockMsgData.dataLen, blockMsgData.offset)
	// update progress
	this.taskMgr.EmitProgress(taskId, task.TaskUploadFileTransferBlocks)
	log.Debugf("upload node list len %d, taskkey %s, hash %s, index %d", len(this.taskMgr.GetUploadedBlockNodeList(taskId, reqInfo.Hash, uint32(reqInfo.Index))), taskId, reqInfo.Hash, reqInfo.Index)
	sent := uint64(this.taskMgr.UploadedBlockCount(taskId) / (uint64(copyNum) + 1))
	if totalCount != sent {
		log.Debugf("totalCount %d != sent %d %d", totalCount, sent, this.taskMgr.UploadedBlockCount(taskId))
		return false, nil
	}
	log.Debugf("fetched job done of id %s hash %s index %d peer %s", taskId, fileHashStr, reqInfo.Index, reqInfo.PeerAddr)
	return true, nil
}

func (this *Dsp) finishUpload(taskId, fileHashStr string, opt *fs.UploadOption, totalCount uint64) (*common.UploadResult, *serr.SDKError) {
	this.taskMgr.EmitProgress(taskId, task.TaskUploadFileWaitForPDPProve)
	proved := this.checkFileBeProved(fileHashStr, opt.CopyNum)
	log.Debugf("checking file: %s proved done %t", fileHashStr, proved)
	var sdkerr *serr.SDKError
	if !proved {
		sdkerr = serr.NewDetailError(serr.FILE_UPLOADED_CHECK_PDP_FAILED, "file has sent, but no enough prove is finished")
		return nil, sdkerr
	}
	pause, sdkerr := this.checkIfPause(taskId, fileHashStr)
	if sdkerr != nil {
		return nil, sdkerr
	}
	if pause {
		return nil, nil
	}
	this.taskMgr.EmitProgress(taskId, task.TaskUploadFileWaitForPDPProveDone)
	dnsRegTx, err := this.taskMgr.GetRegUrlTx(taskId)
	if err != nil {
		sdkerr = serr.NewDetailError(serr.GET_FILEINFO_FROM_DB_ERROR, err.Error())
		return nil, sdkerr
	}
	dnsBindTx, err := this.taskMgr.GetBindUrlTx(taskId)
	if err != nil {
		sdkerr = serr.NewDetailError(serr.GET_FILEINFO_FROM_DB_ERROR, err.Error())
		return nil, sdkerr
	}
	saveLink := utils.GenOniLink(fileHashStr, string(opt.FileDesc), opt.FileSize, totalCount, this.DNS.TrackerUrls)
	if len(dnsRegTx) == 0 || len(dnsBindTx) == 0 {
		dnsRegTx, dnsBindTx = this.registerUrls(taskId, fileHashStr, saveLink, opt, totalCount)
	}
	storeTx, err := this.taskMgr.GetStoreTx(taskId)
	if err != nil {
		sdkerr = serr.NewDetailError(serr.GET_FILEINFO_FROM_DB_ERROR, err.Error())
		return nil, sdkerr
	}
	addWhiteListTx, err := this.taskMgr.GetWhitelistTx(taskId)
	if err != nil {
		sdkerr = serr.NewDetailError(serr.GET_FILEINFO_FROM_DB_ERROR, err.Error())
		return nil, sdkerr
	}
	uploadRet := &common.UploadResult{
		Tx:             storeTx,
		FileHash:       fileHashStr,
		Url:            string(opt.DnsURL),
		Link:           saveLink,
		RegisterDnsTx:  dnsRegTx,
		BindDnsTx:      dnsBindTx,
		AddWhiteListTx: addWhiteListTx,
	}
	err = this.taskMgr.SetRegAndBindUrlTx(taskId, dnsRegTx, dnsBindTx)
	if err != nil {
		sdkerr = serr.NewDetailError(serr.SET_FILEINFO_DB_ERROR, err.Error())
		return nil, sdkerr
	}
	return uploadRet, nil
}

// uploadOptValid check upload opt valid
func uploadOptValid(filePath string, opt *fs.UploadOption) error {
	if !common.FileExisted(filePath) {
		return errors.New("file not exist")
	}
	if opt.Encrypt && len(opt.EncryptPassword) == 0 {
		return errors.New("encrypt password is missing")
	}
	return nil
}

func keyOfUnixNode(hash string, index uint32) string {
	return fmt.Sprintf("%s-%d", hash, index)
}
