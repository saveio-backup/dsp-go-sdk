package dsp

import (
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/saveio/dsp-go-sdk/actor/client"
	"github.com/saveio/dsp-go-sdk/common"
	serr "github.com/saveio/dsp-go-sdk/error"
	netcomm "github.com/saveio/dsp-go-sdk/network/common"
	"github.com/saveio/dsp-go-sdk/network/message"
	"github.com/saveio/dsp-go-sdk/network/message/types/block"
	"github.com/saveio/dsp-go-sdk/network/message/types/file"
	"github.com/saveio/dsp-go-sdk/store"
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
	refCnt    int
}

// CalculateUploadFee. pre-calculate upload cost by its upload options
func (this *Dsp) CalculateUploadFee(opt *fs.UploadOption) (*fs.StorageFee, error) {
	return this.Chain.Native.Fs.GetUploadStorageFee(opt)
}

// UploadTaskExist. check if upload task is existed by filePath hash string
func (this *Dsp) UploadTaskExist(filePath string) (bool, error) {
	taskId := this.taskMgr.TaskId(filePath, this.WalletAddress(), store.TaskTypeUpload)
	if len(taskId) == 0 {
		checksum, err := utils.GetSimpleChecksumOfFile(filePath)
		if err != nil {
			return true, err
		}
		taskId = this.taskMgr.TaskId(checksum, this.WalletAddress(), store.TaskTypeUpload)
		if len(taskId) == 0 {
			return false, nil
		}
		log.Debugf("upload task exist checksum: %s, filepath: %s", checksum, filePath)
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
	fileHashStr, _ := this.taskMgr.TaskFileHash(taskId)
	if len(fileHashStr) == 0 {
		return true, fmt.Errorf("file hash not found for %s", taskId)
	}
	log.Debugf("upload task exist, but expired, delete it")
	err = this.taskMgr.CleanTask(taskId)
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
		taskId, err = this.taskMgr.NewTask(store.TaskTypeUpload)
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
			this.taskMgr.DeleteTask(taskId)
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
	if err = this.taskMgr.SetFilePath(taskId, filePath); err != nil {
		sdkerr = serr.NewDetailError(serr.SET_FILEINFO_DB_ERROR, err.Error())
		return nil, err
	}
	if err = this.taskMgr.SetFileUploadOptions(taskId, opt); err != nil {
		sdkerr = serr.NewDetailError(serr.SET_FILEINFO_DB_ERROR, err.Error())
		return nil, err
	}
	checksum, err := utils.GetSimpleChecksumOfFile(filePath)
	if err != nil {
		sdkerr = serr.NewDetailError(serr.INVALID_PARAMS, err.Error())
		return nil, err
	}
	this.taskMgr.NewBatchSet(taskId)
	this.taskMgr.SetFilePath(taskId, filePath)
	this.taskMgr.SetSimpleCheckSum(taskId, checksum)
	this.taskMgr.SetFileName(taskId, string(opt.FileDesc))
	this.taskMgr.SetWalletAddr(taskId, this.WalletAddress())
	this.taskMgr.SetCopyNum(taskId, uint64(opt.CopyNum))
	err = this.taskMgr.BatchCommit(taskId)
	if err != nil {
		sdkerr = serr.NewDetailError(serr.SET_FILEINFO_DB_ERROR, err.Error())
		return nil, err
	}
	// bind task id with file path
	this.taskMgr.BindTaskId(taskId)
	this.taskMgr.EmitProgress(taskId, task.TaskUploadFileMakeSlice)
	var tx, fileHashStr, prefixStr string
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
	tx, _ = this.taskMgr.GetStoreTx(taskId)
	var hashes []string
	if len(tx) == 0 {
		filePrefix := &utils.FilePrefix{
			Version:    utils.PREFIX_VERSION,
			Encrypt:    opt.Encrypt,
			EncryptPwd: string(opt.EncryptPassword),
			Owner:      this.CurrentAccount().Address,
			FileSize:   opt.FileSize,
		}
		filePrefix.SetSalt()
		prefixStr = filePrefix.String()
		log.Debugf("node from file prefix: %v, len: %d", prefixStr, len(prefixStr))
		hashes, err = this.Fs.NodesFromFile(filePath, prefixStr, opt.Encrypt, string(opt.EncryptPassword))
		if err != nil {
			sdkerr = serr.NewDetailError(serr.SHARDING_FAIELD, err.Error())
			log.Errorf("node from file err: %s", err)
			return nil, err
		}
	} else {
		prefixBuf, err := this.taskMgr.GetFilePrefix(taskId)
		if err != nil {
			sdkerr = serr.NewDetailError(serr.GET_FILEINFO_FROM_DB_ERROR, err.Error())
			return nil, err
		}
		prefixStr = string(prefixBuf)
		fileHashStr, err = this.taskMgr.TaskFileHash(taskId)
		if err != nil {
			sdkerr = serr.NewDetailError(serr.GET_FILEINFO_FROM_DB_ERROR, err.Error())
			return nil, err
		}
		hashes, err = this.Fs.GetFileAllHashes(fileHashStr)
		if err != nil {
			sdkerr = serr.NewDetailError(serr.GET_FILEINFO_FROM_DB_ERROR, err.Error())
			return nil, err
		}
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
	this.taskMgr.NewBatchSet(taskId)
	this.taskMgr.SetFileHash(taskId, fileHashStr)
	this.taskMgr.SetTotalBlockCount(taskId, totalCount)
	err = this.taskMgr.BatchCommit(taskId)
	if err != nil {
		sdkerr = serr.NewDetailError(serr.SET_FILEINFO_DB_ERROR, err.Error())
		return nil, err
	}
	// bind task id with file hash
	err = this.taskMgr.BindTaskId(taskId)
	if err != nil {
		sdkerr = serr.NewDetailError(serr.SET_FILEINFO_DB_ERROR, err.Error())
		return nil, err
	}
	if newTask {
		hasUploading := this.checkFileHasUpload(taskId, fileHashStr)
		if hasUploading {
			err = errors.New("file has uploading or uploaded, please cancel the task")
			sdkerr = serr.NewDetailError(serr.UPLOAD_TASK_EXIST, err.Error())
			return nil, err
		}
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
	if len(nodeList) != int(opt.CopyNum)+1 {
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
		// rollback transfer state
		this.taskMgr.EmitProgress(taskId, task.TaskUploadFileFindReceivers)
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
	go func() {
		err := this.notifyFetchReady(taskId, fileHashStr, receivers, tx, uint64(payTxHeight))
		if err != nil {
			log.Errorf("notify fetch ready msg failed, err %s", err)
		}
	}()
	this.taskMgr.EmitProgress(taskId, task.TaskUploadFileFindReceiversDone)

	sdkerr = this.waitForFetchBlock(taskId, hashes, receivers, int(opt.CopyNum), p.G0, p.FileId, payRet.PrivateKey)
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
	taskType, _ := this.taskMgr.TaskType(taskId)
	if taskType != store.TaskTypeUpload {
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
	err = this.taskMgr.SetTaskState(taskId, store.TaskStatePause)
	if err != nil {
		return err
	}
	this.taskMgr.EmitProgress(taskId, task.TaskPause)
	return nil
}

func (this *Dsp) ResumeUpload(taskId string) error {
	taskType, _ := this.taskMgr.TaskType(taskId)
	if taskType != store.TaskTypeUpload {
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
	err = this.taskMgr.SetTaskState(taskId, store.TaskStateDoing)
	if err != nil {
		log.Errorf("resume task err %s", err)
		return err
	}
	this.taskMgr.EmitProgress(taskId, task.TaskDoing)
	return this.checkIfResume(taskId)
}

func (this *Dsp) CancelUpload(taskId string) (*common.DeleteUploadFileResp, error) {
	taskType, _ := this.taskMgr.TaskType(taskId)
	if taskType != store.TaskTypeUpload {
		return nil, fmt.Errorf("task %s is not a upload task", taskId)
	}
	cancel, err := this.taskMgr.IsTaskCancel(taskId)
	if err != nil {
		return nil, err
	}
	if cancel {
		return nil, fmt.Errorf("task is cancelling: %s", taskId)
	}
	paying, err := this.taskMgr.IsTaskPaying(taskId)
	if err != nil {
		return nil, err
	}
	if paying {
		return nil, fmt.Errorf("task is paying: %s", taskId)
	}
	fileHashStr, _ := this.taskMgr.TaskFileHash(taskId)
	oldState, _ := this.taskMgr.GetTaskState(taskId)
	err = this.taskMgr.SetTaskState(taskId, store.TaskStateCancel)
	if err != nil {
		return nil, err
	}
	log.Debugf("cancel upload task id: %s,  fileHash: %v", taskId, fileHashStr)
	if len(fileHashStr) == 0 {
		err = this.taskMgr.CleanTask(taskId)
		if err == nil {
			return nil, nil
		}
		log.Errorf("delete task err %s", err)
		this.taskMgr.SetTaskState(taskId, oldState)
		return nil, fmt.Errorf("delete task failed %s, err: %v", fileHashStr, err)
	}
	nodeList := this.taskMgr.GetUploadedBlockNodeList(taskId, fileHashStr, 0)
	if len(nodeList) == 0 {
		resps, err := this.DeleteUploadedFileByIds([]string{taskId})
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
	msg := message.NewFileMsg(fileHashStr, netcomm.FILE_OP_FETCH_CANCEL,
		message.WithSessionId(taskId),
		message.WithWalletAddress(this.WalletAddress()),
		message.WithSign(this.Account),
	)
	ret, err := client.P2pBroadcast(nodeList, msg.ToProtoMsg(), true, nil)
	log.Debugf("broadcast cancel msg ret %v, err: %s", ret, err)
	resps, err := this.DeleteUploadedFileByIds([]string{taskId})
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
	taskType, _ := this.taskMgr.TaskType(taskId)
	if taskType != store.TaskTypeUpload {
		return fmt.Errorf("task %s is not a upload task", taskId)
	}
	failed, err := this.taskMgr.IsTaskFailed(taskId)
	if err != nil {
		return err
	}
	if !failed {
		return fmt.Errorf("task %s is not failed", taskId)
	}
	err = this.taskMgr.SetTaskState(taskId, store.TaskStateDoing)
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
		if err != nil && !this.IsFileInfoDeleted(err) {
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

// DeleteUploadedFileByIds. Delete uploaded file from remote nodes. it is called by the owner
func (this *Dsp) DeleteUploadedFileByIds(ids []string) ([]*common.DeleteUploadFileResp, error) {
	if len(ids) == 0 {
		return nil, errors.New("delete file ids is empty")
	}
	log.Debugf("DeleteUploadedFileByIds: %v", ids)
	fileHashStrs := make([]string, 0, len(ids))
	taskIds := make([]string, 0, len(ids))
	taskIdM := make(map[string]struct{}, 0)
	hashM := make(map[string]struct{}, 0)
	for _, id := range ids {
		if _, ok := taskIdM[id]; ok {
			continue
		}
		taskIdM[id] = struct{}{}
		taskIds = append(taskIds, id)
		hash, _ := this.taskMgr.TaskFileHash(id)
		if len(hash) == 0 {
			continue
		}
		if _, ok := hashM[hash]; ok {
			continue
		}
		hashM[hash] = struct{}{}
		fileHashStrs = append(fileHashStrs, hash)
	}

	txHashStr, txHeight, sdkErr := this.DeleteUploadFilesFromChain(fileHashStrs)
	log.Debugf("delete upload files from chain :%s, %d, %v", txHashStr, txHeight, sdkErr)
	if sdkErr != nil && sdkErr.Code != serr.NO_FILE_NEED_DELETED {
		return nil, sdkErr.Error
	}
	resps := make([]*common.DeleteUploadFileResp, 0, len(fileHashStrs))
	for _, taskId := range taskIds {
		fileHashStr, err := this.taskMgr.TaskFileHash(taskId)
		if err != nil {
			continue
		}
		if len(fileHashStr) == 0 {
			err := this.taskMgr.CleanTask(taskId)
			log.Debugf("delete task donne ")
			if err != nil {
				log.Errorf("delete upload info from db err: %s", err)
				return nil, err
			}
			resps = append(resps, &common.DeleteUploadFileResp{
				Tx: txHashStr,
			})
			continue
		}
		storingNode := this.getFileProvedNode(fileHashStr)
		log.Debugf("taskId of to delete file %v", taskId)
		if len(storingNode) == 0 {
			// find breakpoint keep nodelist
			storingNode = append(storingNode, this.taskMgr.GetUploadedBlockNodeList(taskId, fileHashStr, 0)...)
			log.Debugf("storingNode: %v", storingNode)
		}
		log.Debugf("will broadcast delete msg to %v", storingNode)
		fileName, _ := this.taskMgr.FileNameFromTask(taskId)
		resp := &common.DeleteUploadFileResp{
			Tx:       txHashStr,
			FileHash: fileHashStr,
			FileName: fileName,
		}
		if len(storingNode) == 0 {
			err := this.taskMgr.CleanTask(taskId)
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
		msg := message.NewFileMsg(fileHashStr, netcomm.FILE_OP_DELETE,
			message.WithSessionId(taskId),
			message.WithWalletAddress(this.WalletAddress()),
			message.WithTxHash(txHashStr),
			message.WithTxHeight(uint64(txHeight)),
			message.WithSign(this.Account),
		)
		nodeStatusLock := new(sync.Mutex)
		nodeStatus := make([]common.DeleteFileStatus, 0, len(storingNode))
		reply := func(msg proto.Message, addr string) bool {
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
			return false
		}
		m, err := client.P2pBroadcast(storingNode, msg.ToProtoMsg(), true, reply)
		resp.Nodes = nodeStatus
		log.Debugf("send delete msg done ret: %v, nodeStatus: %v, err: %s", m, nodeStatus, err)
		err = this.taskMgr.CleanTask(taskId)
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
	msg := message.NewFileMsg(fileHashStr, netcomm.FILE_OP_FETCH_PAUSE,
		message.WithSessionId(taskId),
		message.WithWalletAddress(this.WalletAddress()),
		message.WithSign(this.Account),
	)
	ret, err := client.P2pBroadcast(nodeList, msg.ToProtoMsg(), true, nil)
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
	fileHashStr, _ := this.taskMgr.TaskFileHash(taskId)
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
				this.taskMgr.DeleteTask(taskId)
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
	msg := message.NewFileMsg(fileHashStr, netcomm.FILE_OP_FETCH_RESUME,
		message.WithSessionId(taskId),
		message.WithWalletAddress(this.WalletAddress()),
		message.WithSign(this.Account),
	)
	_, err = client.P2pBroadcast(nodeList, msg.ToProtoMsg(), true, nil)
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
		log.Debugf("checkFileHasUpload fileinfo exist %s", fileHashStr)
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
		if this.Config.BlockConfirm > 0 {
			_, err := this.Chain.WaitForGenerateBlock(time.Duration(common.TX_CONFIRM_TIMEOUT)*time.Second, this.Config.BlockConfirm)
			if err != nil {
				return nil, fmt.Errorf("wait for generate block failed, err %s", err)
			}
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

	if this.Config.BlockConfirm > 0 && (len(dnsRegTx) > 0 || len(dnsBindTx) > 0) {
		_, err := this.Chain.WaitForGenerateBlock(time.Duration(common.TX_CONFIRM_TIMEOUT)*time.Second, this.Config.BlockConfirm)
		if err != nil {
			log.Errorf("[Dsp registerUrls] wait for generate block failed, err %s", err)
			return "", ""
		}
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
		log.Debugf("%s, node:%s, prove times %d", fileHashStr, string(detail.NodeAddr), detail.ProveTimes)
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
	receiverLock := new(sync.Mutex)
	receivers := make([]string, 0)
	stop := false
	receiverBreakpoint := make(map[string]*file.Breakpoint, 0)
	sessionId, err := this.taskMgr.GetSessionId(taskId, "")
	if err != nil {
		return nil, err
	}
	log.Debugf("waitFileReceivers sessionId: %s prefix hex: %s", sessionId, hex.EncodeToString([]byte(prefix)))
	msg := message.NewFileMsg(fileHashStr, netcomm.FILE_OP_FETCH_ASK,
		message.WithSessionId(sessionId),
		message.WithBlockHashes(blockHashes), // TODO: refactor this to reduce msg data length
		message.WithWalletAddress(this.WalletAddress()),
		message.WithPrefix([]byte(prefix)),
		message.WithSign(this.Account),
	)
	action := func(res proto.Message, addr string) bool {
		p2pMsg := message.ReadMessage(res)
		if p2pMsg.Error != nil && p2pMsg.Error.Code != serr.SUCCESS {
			log.Errorf("get file fetch_ack msg err %s", p2pMsg.Error.Message)
			return false
		}
		receiverLock.Lock()
		defer receiverLock.Unlock()
		if stop {
			log.Debugf("break here after stop is true")
			return true
		}
		fileMsg := p2pMsg.Payload.(*file.File)
		receivers = append(receivers, addr)
		receiverBreakpoint[addr] = fileMsg.Breakpoint
		log.Debugf("send file_ask msg success of file: %s, to: %s  receivers: %v", fileMsg.Hash, addr, receivers)
		if len(receivers) >= receiverCount || len(receivers) >= len(nodeList) {
			log.Debugf("break receiver goroutine")
			stop = true
			return true
		}
		log.Debugf("continue....")
		return false
	}
	log.Debugf("broadcast fetch_ask msg of file: %s to %v", fileHashStr, nodeList)
	ret, err := client.P2pBroadcast(nodeList, msg.ToProtoMsg(), true, action)
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
	msg := message.NewFileMsg(fileHashStr, netcomm.FILE_OP_FETCH_RDY,
		message.WithSessionId(sessionId),
		message.WithWalletAddress(this.WalletAddress()),
		message.WithTxHash(tx),
		message.WithTxHeight(txHeight),
		message.WithSign(this.Account),
	)
	ret, err := client.P2pBroadcast(receivers, msg.ToProtoMsg(), false, nil)
	if err != nil {
		log.Errorf("notify err %s", err)
		return err
	}
	for _, e := range ret {
		if e != nil {
			log.Errorf("notify err %s", err)
			return e
		}
	}
	return nil
}

// generateBlockMsgData. generate blockdata, blockEncodedData, tagData with prove params
func (this *Dsp) generateBlockMsgData(hash string, index uint32, offset uint64, copyNum int, fileID, g0, privateKey []byte) (*blockMsgData, error) {
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
		refCnt:    copyNum + 1,
	}
	return msgData, nil
}

// waitForFetchBlock. wait for fetched blocks concurrent
func (this *Dsp) waitForFetchBlock(taskId string, hashes, addrs []string, copyNum int, g0, fileID, privateKey []byte) *serr.SDKError {
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
	sessionId, err := this.taskMgr.GetSessionId(taskId, "")
	if err != nil {
		return serr.NewDetailError(serr.GET_SESSION_ID_FAILED, err.Error())
	}
	fileHashStr, _ := this.taskMgr.TaskFileHash(taskId)
	totalCount := uint64(len(hashes))
	doneCh := make(chan *fetchedDone)
	closeDoneChannel := uint32(0)
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
		data, err = this.generateBlockMsgData(hash, index, offset, copyNum, fileID, g0, privateKey)
		if err != nil {
			return nil
		}
		blockMsgDataMap[key] = data
		return data
	}
	cleanMsgData := func(reqInfos []*task.GetBlockReq) {
		getMsgDataLock.Lock()
		defer getMsgDataLock.Unlock()
		for _, reqInfo := range reqInfos {
			hash := reqInfo.Hash
			index := uint32(reqInfo.Index)
			key := keyOfUnixNode(hash, index)
			data, ok := blockMsgDataMap[key]
			if !ok {
				continue
			}
			data.refCnt--
			if data.refCnt > 0 {
				continue
			}
			log.Debugf("delete block msg data of key: %s", key)
			delete(blockMsgDataMap, key)
			this.Fs.ReturnBuffer(data.blockData)
		}
	}
	cancelFetch := make(chan struct{})
	log.Debugf("open %d go routines for fetched file taskId: %s, %s", len(addrs), taskId, fileHashStr)
	this.taskMgr.NewWorkers(taskId, utils.StringSliceToKeyMap(addrs), true, nil)

	reqForPeers := make(map[string]chan []*task.GetBlockReq)
	for _, p := range addrs {
		reqForPeers[p] = make(chan []*task.GetBlockReq, 1)
	}
	go func() {
		for {
			select {
			case reqInfo := <-req:
				if len(reqInfo) == 0 {
					continue
				}
				ch := reqForPeers[reqInfo[0].PeerAddr]
				go func() {
					ch <- reqInfo
				}()
			case <-cancelFetch:
				log.Debugf("fetch cancel")
				return
			}
		}
	}()
	// TODO: add max go routines check
	for _, ch := range reqForPeers {
		go func(req chan []*task.GetBlockReq) {
			for {
				select {
				case reqInfos := <-req:
					stop, _ := this.stopUpload(taskId)
					if stop {
						log.Debugf("stop handle request because task is stop: %t", stop)
						return
					}
					msgDataM := make(map[string]*blockMsgData, 0)
					for _, reqInfo := range reqInfos {
						this.taskMgr.ActiveUploadTaskPeer(reqInfo.PeerAddr)
						key := keyOfUnixNode(reqInfo.Hash, uint32(reqInfo.Index))
						log.Debugf("handle request key:%s, %s-%s-%d from peer: %s", key, fileHashStr, reqInfo.Hash, reqInfo.Index, reqInfo.PeerAddr)
						msgData := getMsgData(reqInfo.Hash, uint32(reqInfo.Index))
						msgDataM[key] = msgData
					}
					// handle fetch request async
					done, err := this.handleFetchBlockRequests(taskId, sessionId, fileHashStr, reqInfos, copyNum, totalCount, msgDataM)
					stop, _ = this.stopUpload(taskId)
					if stop {
						log.Debugf("stop handle request because task is stop: %t", stop)
						return
					}
					if err != nil {
						log.Errorf("handle fetch file %s err %s", fileHashStr, err)
						continue
					}
					// because of the network transfer time consuming, reset timer here after sending blocks finish.
					if done == false && err == nil {
						cleanMsgData(reqInfos)
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
				case <-cancelFetch:
					log.Debugf("fetch cancel")
					return
				}
			}
		}(ch)
	}
	checkStopTimer := time.NewTicker(time.Duration(common.TASK_STATE_CHECK_DURATION) * time.Second)
	defer checkStopTimer.Stop()
	closeCancelFetch := false
	for {
		select {
		case ret, ok := <-doneCh:
			log.Debugf("receive fetch file done channel: %v, ok: %t", ret, ok)
			if !ok {
				log.Debugf("done channel has close")
			}
			if ret.err != nil {
				if !closeCancelFetch {
					closeCancelFetch = true
					close(cancelFetch)
				}
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
		case <-checkStopTimer.C:
			stop, _ := this.stopUpload(taskId)
			if stop {
				log.Debugf("stop wait for fetch because task is stop: %t", stop)
				if !closeCancelFetch {
					closeCancelFetch = true
					close(cancelFetch)
				}
				return nil
			}
			if !this.taskMgr.Task(taskId).IsTimeout() {
				continue
			}
			if !closeCancelFetch {
				closeCancelFetch = true
				close(cancelFetch)
			}
			sent := uint64(this.taskMgr.UploadedBlockCount(taskId) / (uint64(copyNum) + 1))
			log.Debugf("timeout check totalCount %d != sent %d %d", totalCount, sent, this.taskMgr.UploadedBlockCount(taskId))
			if totalCount != sent {
				err = fmt.Errorf("wait for fetch file: %s timeout", fileHashStr)
				return serr.NewDetailError(serr.TASK_WAIT_TIMEOUT, err.Error())
			}
			return nil
		}
	}
}

// handleFetchBlockRequest. handle fetch request and send block
func (this *Dsp) handleFetchBlockRequests(taskId, sessionId, fileHashStr string,
	reqInfos []*task.GetBlockReq,
	copyNum int,
	totalCount uint64,
	blockMsgDataM map[string]*blockMsgData) (bool, error) {
	// send block
	if len(reqInfos) == 0 {
		log.Warnf("no request to handle of file: %s", fileHashStr)
		return true, nil
	}
	blocks := make([]*block.Block, 0, len(reqInfos))
	blockInfos := make([]*store.BlockInfo, 0, len(reqInfos))
	nodeAddr := reqInfos[0].PeerAddr
	for _, reqInfo := range reqInfos {
		log.Debugf("receive fetch block msg of %s-%s-%d-%d from %s", reqInfo.FileHash, reqInfo.Hash, reqInfo.Index, totalCount, reqInfo.PeerAddr)
		blockMsgData := blockMsgDataM[keyOfUnixNode(reqInfo.Hash, uint32(reqInfo.Index))]
		if len(blockMsgData.blockData) == 0 || len(blockMsgData.tag) == 0 {
			log.Errorf("block len %d, tag len %d hash %s, peer %s failed", len(blockMsgData.blockData), len(blockMsgData.tag), reqInfo.Hash, reqInfo.PeerAddr)
			return false, fmt.Errorf("block len %d, tag len %d hash %s, peer %s failed", len(blockMsgData.blockData), len(blockMsgData.tag), reqInfo.Hash, reqInfo.PeerAddr)
		}
		isStored := this.taskMgr.IsBlockUploaded(taskId, reqInfo.Hash, reqInfo.PeerAddr, uint32(reqInfo.Index))
		if !isStored {
			bi := &store.BlockInfo{
				Hash:       reqInfo.Hash,
				Index:      uint32(reqInfo.Index),
				DataSize:   blockMsgData.dataLen,
				DataOffset: blockMsgData.offset,
			}
			blockInfos = append(blockInfos, bi)
		}
		b := &block.Block{
			SessionId: taskId,
			Index:     reqInfo.Index,
			FileHash:  fileHashStr,
			Hash:      reqInfo.Hash,
			Data:      blockMsgData.blockData,
			Tag:       blockMsgData.tag,
			Operation: netcomm.BLOCK_OP_NONE,
			Offset:    int64(blockMsgData.offset),
		}
		blocks = append(blocks, b)
	}
	if len(blocks) == 0 {
		return false, fmt.Errorf("no block to send of file: %s", fileHashStr)
	}
	flights := &block.BlockFlights{
		TimeStamp: reqInfos[0].TimeStamp,
		Blocks:    blocks,
	}
	msg := message.NewBlockFlightsMsg(flights)
	sendLogMsg := fmt.Sprintf("file: %s, block %s-%s, index:%d-%d to %s", fileHashStr, reqInfos[0].Hash, reqInfos[len(reqInfos)-1].Hash, reqInfos[0].Index, reqInfos[len(reqInfos)-1].Index, reqInfos[0].PeerAddr)
	sendingTime := time.Now().Unix()
	log.Debugf("sending %s", sendLogMsg)
	_, err := client.P2pRequestWithRetry(msg.ToProtoMsg(), reqInfos[0].PeerAddr, common.MAX_SEND_BLOCK_RETRY, common.P2P_REQUEST_WAIT_REPLY_TIMEOUT)
	if err != nil {
		log.Errorf("%v, err %s", sendLogMsg, err)
		return false, err
	}
	this.taskMgr.ActiveUploadTaskPeer(reqInfos[0].PeerAddr)
	log.Debugf("send block success %s\n used %ds", sendLogMsg, time.Now().Unix()-sendingTime)
	// TODO: add pause here

	// stored
	this.taskMgr.SetBlocksUploaded(taskId, nodeAddr, blockInfos)
	// update progress
	this.taskMgr.EmitProgress(taskId, task.TaskUploadFileTransferBlocks)
	// log.Debugf("upload node list len %d, taskkey %s, hash %s, index %d", len(this.taskMgr.GetUploadedBlockNodeList(taskId, reqInfo.Hash, uint32(reqInfo.Index))), taskId, reqInfo.Hash, reqInfo.Index)
	sent := uint64(this.taskMgr.UploadedBlockCount(taskId) / (uint64(copyNum) + 1))
	if totalCount != sent {
		log.Debugf("totalCount %d != sent %d %d", totalCount, sent, this.taskMgr.UploadedBlockCount(taskId))
		return false, nil
	}
	log.Debugf("fetched job done of id %s  %s", taskId, sendLogMsg)
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
	saveLink := utils.GenOniLink(fileHashStr, string(opt.FileDesc), this.WalletAddress(), opt.FileSize, totalCount, this.DNS.TrackerUrls)
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
	this.taskMgr.NewBatchSet(taskId)
	this.taskMgr.SetRegUrlTx(taskId, dnsRegTx)
	this.taskMgr.SetBindUrlTx(taskId, dnsBindTx)
	err = this.taskMgr.BatchCommit(taskId)
	if err != nil {
		sdkerr = serr.NewDetailError(serr.SET_FILEINFO_DB_ERROR, err.Error())
		return nil, sdkerr
	}
	return uploadRet, nil
}

func (this *Dsp) stopUpload(taskId string) (bool, *serr.SDKError) {
	stop, err := this.taskMgr.IsTaskStop(taskId)
	if err != nil {
		sdkerr := serr.NewDetailError(serr.TASK_INTERNAL_ERROR, err.Error())
		return false, sdkerr
	}
	return stop, nil
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
