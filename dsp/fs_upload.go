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
	dspErr "github.com/saveio/dsp-go-sdk/error"
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

// UploadTaskExist. check if upload task is existed by filePath hash string
func (this *Dsp) UploadTaskExist(filePath string) (bool, error) {
	taskId := this.taskMgr.TaskId(filePath, this.chain.WalletAddress(), store.TaskTypeUpload)
	if len(taskId) == 0 {
		checksum, err := utils.GetSimpleChecksumOfFile(filePath)
		if err != nil {
			return true, dspErr.NewWithError(dspErr.GET_SIMPLE_CHECKSUM_ERROR, err)
		}
		taskId = this.taskMgr.TaskId(checksum, this.chain.WalletAddress(), store.TaskTypeUpload)
		if len(taskId) == 0 {
			return false, nil
		}
		log.Debugf("upload task exist checksum: %s, filepath: %s", checksum, filePath)
	}
	opt, err := this.taskMgr.GetFileUploadOptions(taskId)
	if err != nil || opt == nil {
		return true, dspErr.New(dspErr.GET_FILEINFO_FROM_DB_ERROR, "get file upload option is nil %s", err)
	}
	now, err := this.chain.GetCurrentBlockHeight()
	if err != nil {
		log.Errorf("get current block height err %s", err)
		return true, err
	}
	if opt.ExpiredHeight > uint64(now) {
		return true, nil
	}
	fileHashStr, _ := this.taskMgr.GetTaskFileHash(taskId)
	if len(fileHashStr) == 0 {
		return true, dspErr.New(dspErr.GET_FILEINFO_FROM_DB_ERROR, "file hash not found for %s", taskId)
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
	var err error
	// emit result finally
	defer func() {
		sdkErr, _ := err.(*dspErr.Error)
		if uploadRet == nil {
			log.Errorf("emit result finally id %v, ret %v, err: %v", taskId, uploadRet, err)
			this.taskMgr.EmitResult(taskId, nil, sdkErr)
		} else {
			log.Debugf("emit result finally id %v, ret %v", taskId, uploadRet)
			this.taskMgr.EmitResult(taskId, uploadRet, sdkErr)
		}
		// delete task from cache in the end when success
		if uploadRet != nil && err == nil {
			this.taskMgr.DeleteTask(taskId)
		}
	}()
	newTask := false
	if len(taskId) == 0 {
		// new task because of task id is empty, generate a new task
		if taskId, err = this.taskMgr.NewTask(store.TaskTypeUpload); err != nil {
			return nil, err
		}
		newTask = true
	}
	this.taskMgr.EmitProgress(taskId, task.TaskUploadFileMakeSlice)
	// check options valid
	if err = uploadOptValid(filePath, opt); err != nil {
		return nil, err
	}
	// calculate check sum for setting to DB
	checksum, err := utils.GetSimpleChecksumOfFile(filePath)
	if err != nil {
		return nil, dspErr.New(dspErr.INVALID_PARAMS, err.Error())
	}
	if err = this.taskMgr.SetTaskInfoWithOptions(taskId, task.FilePath(filePath), task.SimpleCheckSum(checksum),
		task.StoreType(opt.StorageType), task.Url(string(opt.DnsURL)), task.FileName(string(opt.FileDesc)),
		task.Walletaddr(this.chain.WalletAddress()), task.CopyNum(uint64(opt.CopyNum))); err != nil {
		return nil, err
	}
	// save other upload options
	if err = this.taskMgr.SetFileUploadOptions(taskId, opt); err != nil {
		return nil, err
	}
	// bind task id with file path
	if err = this.taskMgr.BindTaskId(taskId); err != nil {
		return nil, err
	}
	this.taskMgr.EmitProgress(taskId, task.TaskUploadFileMakeSlice)
	var tx, fileHashStr, prefixStr string
	var totalCount uint64
	log.Debugf("upload task: %s, will split for file", taskId)
	// split file to pieces
	if pause, err := this.checkIfPause(taskId, ""); err != nil || pause {
		return nil, err
	}
	tx, _ = this.taskMgr.GetStoreTx(taskId)
	var hashes []string
	// sharing file or get hashes from DB
	if len(tx) == 0 {
		// file not paid
		filePrefix := &utils.FilePrefix{
			Version:    utils.PREFIX_VERSION,
			Encrypt:    opt.Encrypt,
			EncryptPwd: string(opt.EncryptPassword),
			Owner:      this.chain.Address(),
			FileSize:   opt.FileSize,
		}
		filePrefix.MakeSalt()
		prefixStr = filePrefix.String()
		log.Debugf("node from file prefix: %v, len: %d", prefixStr, len(prefixStr))
		if hashes, err = this.fs.NodesFromFile(filePath, prefixStr, opt.Encrypt, string(opt.EncryptPassword)); err != nil {
			return nil, err
		}
	} else {
		// file has paid, get prefix
		prefixBuf, err := this.taskMgr.GetFilePrefix(taskId)
		if err != nil {
			return nil, err
		}
		prefixStr = string(prefixBuf)
		fileHashStr, err = this.taskMgr.GetTaskFileHash(taskId)
		if err != nil {
			return nil, err
		}
		hashes, err = this.fs.GetFileAllHashes(fileHashStr)
		if err != nil {
			return nil, err
		}
	}
	if totalCount = uint64(len(hashes)); totalCount == 0 {
		err = dspErr.New(dspErr.SHARDING_FAIELD, "no blocks to upload")
		return nil, err
	}
	log.Debugf("sharding file finished, taskId:%s, path: %s, fileHash: %s, totalCount:%d", taskId, filePath, hashes[0], totalCount)
	this.taskMgr.EmitProgress(taskId, task.TaskUploadFileMakeSliceDone)
	fileHashStr = hashes[0]
	log.Debugf("after bind task id")
	if err = this.taskMgr.SetTaskInfoWithOptions(taskId, task.FileHash(fileHashStr), task.TotalBlockCnt(totalCount)); err != nil {
		return nil, err
	}
	// bind task id with file hash
	if err = this.taskMgr.BindTaskId(taskId); err != nil {
		return nil, err
	}
	if newTask && this.checkFileHasUpload(taskId, fileHashStr) {
		err = dspErr.New(dspErr.UPLOAD_TASK_EXIST, "file has uploading or uploaded, please cancel the task")
		return nil, err
	}
	log.Debugf("check if pause after node from file")
	if pause, err := this.checkIfPause(taskId, fileHashStr); err != nil || pause {
		return nil, err
	}
	// check has uploaded this file
	if this.taskMgr.IsFileUploaded(taskId) {
		uploadRet, err = this.finishUpload(taskId, fileHashStr, opt, totalCount)
		return uploadRet, err
	}
	log.Debugf("root:%s, list.len:%d", fileHashStr, totalCount)
	// get nodeList
	nodeWalletAddrList, err := this.getUploadNodeFromChain(filePath, taskId, fileHashStr, opt.CopyNum)
	if err != nil {
		return nil, dspErr.NewWithError(dspErr.GET_STORAGE_NODES_FAILED, err)
	}
	log.Debugf("uploading nodelist %v, opt.CopyNum %d", nodeWalletAddrList, opt.CopyNum)
	if uint64(len(nodeWalletAddrList)) < opt.CopyNum+1 {
		err = dspErr.New(dspErr.ONLINE_NODES_NOT_ENOUGH, "node is not enough %d, copyNum %d", len(nodeWalletAddrList), opt.CopyNum)
		return nil, err
	}
	this.taskMgr.EmitProgress(taskId, task.TaskUploadFileFindReceivers)
	receivers, err := this.waitFileReceivers(taskId, fileHashStr, prefixStr, nodeWalletAddrList, hashes, opt.CopyNum)
	if err != nil {
		return nil, dspErr.NewWithError(dspErr.SEARCH_RECEIVERS_FAILED, err)
	}
	log.Debugf("receivers:%v", receivers)
	if len(receivers) < int(opt.CopyNum)+1 {
		err = errors.New("file receivers is not enough")
		return nil, dspErr.NewWithError(dspErr.RECEIVERS_NOT_ENOUGH, err)
	}
	if len(receivers) > int(opt.CopyNum)+1 {
		receivers = receivers[:int(opt.CopyNum)+1]
	}
	this.taskMgr.EmitProgress(taskId, task.TaskUploadFilePaying)
	if pause, err := this.checkIfPause(taskId, fileHashStr); err != nil || pause {
		return nil, err
	}
	// pay file
	payRet, err := this.payForSendFile(filePath, taskId, fileHashStr, totalCount, opt)
	if err != nil {
		// rollback transfer state
		this.taskMgr.EmitProgress(taskId, task.TaskUploadFileFindReceivers)
		return nil, dspErr.NewWithError(dspErr.PAY_FOR_STORE_FILE_FAILED, err)
	}
	payTxHeight, err := this.chain.GetBlockHeightByTxHash(payRet.Tx)
	if err != nil {
		return nil, dspErr.NewWithError(dspErr.PAY_FOR_STORE_FILE_FAILED, err)
	}
	log.Debugf("pay for send file success %v %d", payRet, payTxHeight)
	if pause, err := this.checkIfPause(taskId, fileHashStr); err != nil || pause {
		return nil, err
	}
	this.taskMgr.EmitProgress(taskId, task.TaskUploadFilePayingDone)
	if _, err = this.addWhitelist(taskId, fileHashStr, opt); err != nil {
		return nil, err
	}
	tx = payRet.Tx
	p, err := this.chain.ProveParamDes(payRet.ParamsBuf)
	if err != nil {
		return nil, dspErr.New(dspErr.GET_PDP_PARAMS_ERROR, err.Error())
	}
	if opt != nil && opt.Share {
		go func() {
			if err := this.shareUploadedFile(filePath, string(opt.FileDesc), this.chain.WalletAddress(), hashes); err != nil {
				log.Errorf("share upload fle err %s", err)
			}
		}()
	}
	if !opt.Share && opt.Encrypt {
		// TODO: delete encrypted block store
	}
	if pause, err := this.checkIfPause(taskId, fileHashStr); err != nil || pause {
		return nil, err
	}
	// notify fetch ready to all receivers
	go func() {
		err := this.notifyFetchReady(taskId, fileHashStr, receivers, tx, uint64(payTxHeight))
		if err != nil {
			log.Errorf("notify fetch ready msg failed, err %s", err)
		}
	}()
	this.taskMgr.EmitProgress(taskId, task.TaskUploadFileFindReceiversDone)
	if err = this.waitForFetchBlock(taskId, hashes, receivers, int(opt.CopyNum), p.G0, p.FileId, payRet.PrivateKey); err != nil {
		log.Errorf("wait for fetch block err %s", err)
		return nil, err
	}
	log.Debugf("waitForFetchBlock finish id: %s ", taskId)
	if pause, err := this.checkIfPause(taskId, fileHashStr); err != nil || pause {
		return nil, err
	}
	uploadRet, err = this.finishUpload(taskId, fileHashStr, opt, totalCount)
	if err != nil {
		return nil, err
	}
	log.Debug("upload success uploadRet: %v!!", uploadRet)
	return uploadRet, nil
}

// PauseUpload. pause a task
func (this *Dsp) PauseUpload(taskId string) error {
	taskType, _ := this.taskMgr.GetTaskType(taskId)
	if taskType != store.TaskTypeUpload {
		return dspErr.New(dspErr.WRONG_TASK_TYPE, "task %s is not a upload task", taskId)
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
	taskType, _ := this.taskMgr.GetTaskType(taskId)
	if taskType != store.TaskTypeUpload {
		return dspErr.New(dspErr.WRONG_TASK_TYPE, "task %s is not a upload task", taskId)
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
	taskType, _ := this.taskMgr.GetTaskType(taskId)
	if taskType != store.TaskTypeUpload {
		return nil, dspErr.New(dspErr.WRONG_TASK_TYPE, "task %s is not a upload task", taskId)
	}
	cancel, err := this.taskMgr.IsTaskCancel(taskId)
	if err != nil {
		return nil, err
	}
	if cancel {
		return nil, dspErr.New(dspErr.WRONG_TASK_TYPE, "task is cancelling: %s", taskId)
	}
	paying, err := this.taskMgr.IsTaskPaying(taskId)
	if err != nil {
		return nil, err
	}
	if paying {
		return nil, dspErr.New(dspErr.WRONG_TASK_TYPE, "task is paying: %s", taskId)
	}
	fileHashStr, _ := this.taskMgr.GetTaskFileHash(taskId)
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
		return nil, dspErr.New(dspErr.DELETE_FILE_FAILED, "delete task failed %s, err: %v", fileHashStr, err)
	}
	nodeList := this.taskMgr.GetUploadedBlockNodeList(taskId, fileHashStr, 0)
	if len(nodeList) == 0 {
		resp, err := this.DeleteUploadedFileByIds([]string{taskId})
		if err != nil {
			this.taskMgr.SetTaskState(taskId, oldState)
			return nil, err
		}
		if len(resp) == 0 {
			return nil, dspErr.New(dspErr.DELETE_FILE_FAILED, "delete file but not response %s", fileHashStr)
		}
		return resp[0], nil
	}
	// send pause msg
	msg := message.NewFileMsg(fileHashStr, netcomm.FILE_OP_FETCH_CANCEL,
		message.WithSessionId(taskId),
		message.WithWalletAddress(this.chain.WalletAddress()),
		message.WithSign(this.chain.CurrentAccount()),
	)
	ret, err := client.P2pBroadcast(nodeList, msg.ToProtoMsg(), true, nil)
	log.Debugf("broadcast cancel msg ret %v, err: %s", ret, err)
	resp, err := this.DeleteUploadedFileByIds([]string{taskId})
	if err != nil {
		this.taskMgr.SetTaskState(taskId, oldState)
		return nil, err
	}
	if len(resp) == 0 {
		return nil, dspErr.New(dspErr.DELETE_FILE_FAILED, "delete file but not response %s", fileHashStr)
	}
	return resp[0], nil
}

func (this *Dsp) RetryUpload(taskId string) error {
	taskType, _ := this.taskMgr.GetTaskType(taskId)
	if taskType != store.TaskTypeUpload {
		return dspErr.New(dspErr.WRONG_TASK_TYPE, "task %s is not a upload task", taskId)
	}
	failed, err := this.taskMgr.IsTaskFailed(taskId)
	if err != nil {
		return err
	}
	if !failed {
		return dspErr.New(dspErr.WRONG_TASK_TYPE, "task %s is not failed", taskId)
	}
	err = this.taskMgr.SetTaskState(taskId, store.TaskStateDoing)
	if err != nil {
		return err
	}
	this.taskMgr.EmitProgress(taskId, task.TaskDoing)
	return this.checkIfResume(taskId)
}

func (this *Dsp) DeleteUploadFilesFromChain(fileHashStrs []string) (string, uint32, error) {
	if len(fileHashStrs) == 0 {
		return "", 0, dspErr.New(dspErr.DELETE_FILE_HASHES_EMPTY, "delete file hash string is empty")
	}
	needDeleteFile := false
	for _, fileHashStr := range fileHashStrs {
		info, err := this.chain.GetFileInfo(fileHashStr)
		log.Debugf("delete file get fileinfo %v, err %v", info, err)
		if err != nil && !this.chain.IsFileInfoDeleted(err) {
			log.Debugf("info:%v, other err:%s", info, err)
			return "", 0, dspErr.New(dspErr.FILE_NOT_FOUND_FROM_CHAIN, "file info not found, %s has deleted", fileHashStr)
		}
		if info != nil && info.FileOwner.ToBase58() != this.chain.WalletAddress() {
			return "", 0, dspErr.New(dspErr.DELETE_FILE_ACCESS_DENIED, "file %s can't be deleted, you are not the owner", fileHashStr)
		}
		if info != nil && err == nil {
			needDeleteFile = true
		}
	}
	if !needDeleteFile {
		return "", 0, dspErr.New(dspErr.NO_FILE_NEED_DELETED, "no file to delete")
	}
	txHashStr, err := this.chain.DeleteFiles(fileHashStrs)
	log.Debugf("delete file tx %v, err %v", txHashStr, err)
	if err != nil {
		return "", 0, dspErr.NewWithError(dspErr.CHAIN_ERROR, err)
	}
	log.Debugf("delete file txHash %s", txHashStr)
	confirmed, err := this.chain.PollForTxConfirmed(time.Duration(common.TX_CONFIRM_TIMEOUT)*time.Second, txHashStr)
	if err != nil || !confirmed {
		return "", 0, dspErr.New(dspErr.CHAIN_ERROR, "wait for tx confirmed failed")
	}
	txHeight, err := this.chain.GetBlockHeightByTxHash(txHashStr)
	log.Debugf("delete file tx height %d, err %v", txHeight, err)
	if err != nil {
		return "", 0, dspErr.NewWithError(dspErr.CHAIN_ERROR, err)
	}
	return txHashStr, txHeight, nil
}

// DeleteUploadedFileByIds. Delete uploaded file from remote nodes. it is called by the owner
func (this *Dsp) DeleteUploadedFileByIds(ids []string) ([]*common.DeleteUploadFileResp, error) {
	if len(ids) == 0 {
		return nil, dspErr.New(dspErr.DELETE_FILE_FAILED, "delete file ids is empty")
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
		hash, _ := this.taskMgr.GetTaskFileHash(id)
		if len(hash) == 0 {
			continue
		}
		if _, ok := hashM[hash]; ok {
			continue
		}
		hashM[hash] = struct{}{}
		fileHashStrs = append(fileHashStrs, hash)
	}

	txHashStr, txHeight, err := this.DeleteUploadFilesFromChain(fileHashStrs)
	log.Debugf("delete upload files from chain :%s, %d, %v", txHashStr, txHeight, err)
	if err != nil {
		if sdkErr, ok := err.(*dspErr.Error); ok && sdkErr.Code != dspErr.NO_FILE_NEED_DELETED {
			return nil, sdkErr
		}
	}
	resps := make([]*common.DeleteUploadFileResp, 0, len(fileHashStrs))
	for _, taskId := range taskIds {
		fileHashStr, err := this.taskMgr.GetTaskFileHash(taskId)
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
		fileName, _ := this.taskMgr.GetTaskFileName(taskId)
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
			message.WithWalletAddress(this.chain.WalletAddress()),
			message.WithTxHash(txHashStr),
			message.WithTxHeight(uint64(txHeight)),
			message.WithSign(this.chain.CurrentAccount()),
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

func (this *Dsp) checkIfPause(taskId, fileHashStr string) (bool, error) {
	pause, err := this.taskMgr.IsTaskPause(taskId)
	if err != nil {
		return false, err
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
		message.WithWalletAddress(this.chain.WalletAddress()),
		message.WithSign(this.chain.CurrentAccount()),
	)
	ret, err := client.P2pBroadcast(nodeList, msg.ToProtoMsg(), true, nil)
	if err != nil {
		return false, err
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
		return dspErr.New(dspErr.GET_FILEINFO_FROM_DB_ERROR, "can't find download options, please retry")
	}
	log.Debugf("get upload options : %v", opt)
	fileHashStr, _ := this.taskMgr.GetTaskFileHash(taskId)
	// send resume msg
	if len(fileHashStr) == 0 {
		go this.UploadFile(taskId, filePath, opt)
		return nil
	}
	fileInfo, _ := this.chain.GetFileInfo(fileHashStr)
	if fileInfo != nil {
		now, err := this.chain.GetCurrentBlockHeight()
		if err != nil {
			return err
		}
		if fileInfo.ExpiredHeight <= uint64(now) {
			return dspErr.New(dspErr.FILE_IS_EXPIRED, "file:%s has expired", fileHashStr)
		}
		hasProvedFile := uint64(len(this.getFileProvedNode(fileHashStr))) == opt.CopyNum+1
		hasSentAllBlock := uint64(this.taskMgr.UploadedBlockCount(taskId)) == fileInfo.FileBlockNum*(fileInfo.CopyNum+1)
		if hasProvedFile || hasSentAllBlock {
			log.Debugf("hasProvedFile: %t, hasSentAllBlock: %t", hasProvedFile, hasSentAllBlock)
			uploadRet, err := this.finishUpload(taskId, fileHashStr, opt, fileInfo.FileBlockNum)
			sdkerr, _ := err.(*dspErr.Error)
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
		message.WithWalletAddress(this.chain.WalletAddress()),
		message.WithSign(this.chain.CurrentAccount()),
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
	info, _ := this.chain.GetFileInfo(fileHashStr)
	if info != nil {
		log.Debugf("checkFileHasUpload fileinfo exist %s", fileHashStr)
		return true
	}
	return this.taskMgr.UploadingFileExist(taskId, fileHashStr)
}

// payForSendFile pay before send a file
// return PoR params byte slice, PoR private key of the file or error
func (this *Dsp) payForSendFile(filePath, taskId, fileHashStr string, blockNum uint64, opt *fs.UploadOption) (*common.PayStoreFileResult, error) {
	fileInfo, _ := this.chain.GetFileInfo(fileHashStr)
	var paramsBuf, privateKey []byte
	var tx string
	var err error
	if fileInfo == nil {
		log.Info("first upload file")
		blockSizeInKB := uint64(math.Ceil(float64(common.CHUNK_SIZE) / 1024.0))
		g, g0, pubKey, privKey, fileID := pdp.Init(filePath)
		paramsBuf, err = this.chain.ProveParamSer(g, g0, pubKey, fileID)
		privateKey = privKey
		if err != nil {
			log.Errorf("serialization prove params failed:%s", err)
			return nil, err
		}
		tx, err = this.chain.StoreFile(fileHashStr, blockNum, blockSizeInKB, opt.ProveInterval,
			opt.ExpiredHeight, uint64(opt.CopyNum), []byte(opt.FileDesc), uint64(opt.Privilege), paramsBuf, uint64(opt.StorageType), opt.FileSize)
		if err != nil {
			log.Errorf("pay store file order failed:%s", err)
			return nil, err
		}
		// TODO: check confirmed on receivers
		confirmed, err := this.chain.PollForTxConfirmed(time.Duration(common.TX_CONFIRM_TIMEOUT)*time.Second, tx)
		if err != nil || !confirmed {
			log.Errorf("poll tx failed %s", err)
			return nil, err
		}
		if this.config.BlockConfirm > 0 {
			_, err = this.chain.WaitForGenerateBlock(time.Duration(common.TX_CONFIRM_TIMEOUT)*time.Second, this.config.BlockConfirm)
			if err != nil {
				log.Errorf("wait for generate err %s", err)
				return nil, err
			}
		}
		if err = this.taskMgr.SetTaskInfoWithOptions(taskId, task.StoreTx(tx), task.PrivateKey(privateKey)); err != nil {
			log.Errorf("set task info err %s", err)
			return nil, err
		}
	} else {
		paramsBuf = fileInfo.FileProveParam
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
		return nil, dspErr.New(dspErr.PDP_PRIVKEY_NOT_FOUND, "PDP private key is not found. Please delete file and retry to upload it")
	}
	log.Debugf("pay for file success")
	return &common.PayStoreFileResult{
		Tx:         tx,
		ParamsBuf:  paramsBuf,
		PrivateKey: privateKey,
	}, nil
}

func (this *Dsp) addWhitelist(taskId, fileHashStr string, opt *fs.UploadOption) (string, error) {
	var addWhiteListTx string
	var err error
	// Don't add whitelist if resume upload
	if opt.WhiteList.Num == 0 {
		return "", nil
	}
	tx, err := this.taskMgr.GetWhitelistTx(taskId)
	if err != nil {
		return "", err
	}
	if len(tx) > 0 {
		log.Debugf("%s get tx %s from db", taskId, tx)
		return tx, nil
	}
	this.taskMgr.EmitProgress(taskId, task.TaskUploadFileCommitWhitelist)
	addWhiteListTx, err = this.addWhitelistsForFile(fileHashStr, opt.WhiteList.List)
	log.Debugf("add whitelist tx, err %s", err)
	if err != nil {
		return "", dspErr.NewWithError(dspErr.ADD_WHITELIST_ERROR, err)
	}
	err = this.taskMgr.SetWhitelistTx(taskId, addWhiteListTx)
	if err != nil {
		return "", dspErr.NewWithError(dspErr.SET_FILEINFO_DB_ERROR, err)
	}
	this.taskMgr.EmitProgress(taskId, task.TaskUploadFileCommitWhitelistDone)
	return addWhiteListTx, nil
}

func (this *Dsp) registerUrls(taskId, fileHashStr, saveLink string, opt *fs.UploadOption, totalCount uint64) (string, string) {
	this.taskMgr.EmitProgress(taskId, task.TaskUploadFileRegisterDNS)
	var dnsRegTx, dnsBindTx string
	var err error
	if opt.RegisterDNS && len(opt.DnsURL) > 0 {
		dnsRegTx, err = this.dns.RegisterFileUrl(string(opt.DnsURL), saveLink)
		if err != nil {
			log.Errorf("register url err: %s", err)
		}
		log.Debugf("acc %s, reg dns %s", this.chain.WalletAddress(), dnsRegTx)
	}
	if opt.BindDNS && len(opt.DnsURL) > 0 {
		dnsBindTx, err = this.dns.BindFileUrl(string(opt.DnsURL), saveLink)
		if err != nil {
			log.Errorf("bind url err: %s", err)
		}
		log.Debugf("bind dns %s", dnsBindTx)
	}

	if this.config.BlockConfirm > 0 && (len(dnsRegTx) > 0 || len(dnsBindTx) > 0) {
		_, err := this.chain.WaitForGenerateBlock(time.Duration(common.TX_CONFIRM_TIMEOUT)*time.Second, this.config.BlockConfirm)
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
	tx, err := this.chain.AddWhiteLists(fileHashStr, rules)
	log.Debugf("err %v", err)
	if err != nil {
		return "", err
	}
	_, err = this.chain.PollForTxConfirmed(time.Duration(common.TX_CONFIRM_TIMEOUT)*time.Second, tx)
	if err != nil {
		return "", err
	}
	return tx, nil
}

// getFileProveNode. get file storing nodes  and stored (expired) nodes
func (this *Dsp) getFileProvedNode(fileHashStr string) []string {
	proveDetails, err := this.chain.GetFileProveDetails(fileHashStr)
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

// getUploadNodeFromChain. get primary nodelist (primary nodes mean the nodes fetch files) from file infofirst. if empty, get nodelist from chain
func (this *Dsp) getUploadNodeFromChain(filePath, taskKey, fileHashStr string, copyNum uint64) ([]chainCom.Address, error) {
	fi, _ := this.chain.GetFileInfo(fileHashStr)
	if fi != nil {
		return fi.PrimaryNodes.AddrList, nil
	}
	fileStat, err := os.Stat(filePath)
	if err != nil {
		return nil, dspErr.NewWithError(dspErr.INVALID_PARAMS, err)
	}
	infos, err := this.chain.GetNodeList()
	if err != nil {
		return nil, err
	}
	expectNodeNum := 2 * (copyNum + 1)
	nodeList := make([]chainCom.Address, 0, expectNodeNum)
	for _, info := range infos.NodeInfo {
		fileSizeInKb := uint64(math.Ceil(float64(fileStat.Size()) / 1024.0))
		if info.RestVol < fileSizeInKb {
			continue
		}
		fullAddress := string(info.NodeAddr)
		if fullAddress == client.P2pGetPublicAddr() {
			continue
		}
		nodeList = append(nodeList, info.WalletAddr)
		if uint64(len(nodeList)) >= expectNodeNum {
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
func (this *Dsp) waitFileReceivers(taskId, fileHashStr, prefix string, nodeWalletList []chainCom.Address, blockHashes []string, copyNum uint64) ([]string, error) {
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
		message.WithWalletAddress(this.chain.WalletAddress()),
		message.WithPrefix([]byte(prefix)),
		message.WithSign(this.chain.CurrentAccount()),
	)
	action := func(res proto.Message, addr string) bool {
		p2pMsg := message.ReadMessage(res)
		if p2pMsg.Error != nil && p2pMsg.Error.Code != dspErr.SUCCESS {
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
		message.WithWalletAddress(this.chain.WalletAddress()),
		message.WithTxHash(tx),
		message.WithTxHeight(txHeight),
		message.WithSign(this.chain.CurrentAccount()),
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
	block := this.fs.GetBlock(hash)
	blockData := this.fs.BlockDataOfAny(block)
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
func (this *Dsp) waitForFetchBlock(taskId string, hashes, addrs []string, copyNum int, g0, fileID, privateKey []byte) error {
	req, err := this.taskMgr.TaskBlockReq(taskId)
	if err != nil {
		return dspErr.New(dspErr.PREPARE_UPLOAD_ERROR, err.Error())
	}
	this.taskMgr.EmitProgress(taskId, task.TaskUploadFileGeneratePDPData)
	allOffset, err := this.fs.GetAllOffsets(hashes[0])
	if err != nil {
		return dspErr.New(dspErr.PREPARE_UPLOAD_ERROR, err.Error())
	}
	this.taskMgr.EmitProgress(taskId, task.TaskUploadFileTransferBlocks)
	sessionId, err := this.taskMgr.GetSessionId(taskId, "")
	if err != nil {
		return dspErr.New(dspErr.GET_SESSION_ID_FAILED, err.Error())
	}
	fileHashStr, _ := this.taskMgr.GetTaskFileHash(taskId)
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
			this.fs.ReturnBuffer(data.blockData)
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
					stop, _ := this.taskMgr.IsTaskStop(taskId)
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
					stop, _ = this.taskMgr.IsTaskStop(taskId)
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
				return dspErr.New(dspErr.TASK_INTERNAL_ERROR, ret.err.Error())
			}
			if !ret.done {
				log.Debugf("no done break")
				break
			}
			// check all blocks has sent
			log.Infof("all block has sent %s, return nil", taskId)
			this.taskMgr.EmitProgress(taskId, task.TaskUploadFileTransferBlocksDone)
			return nil
		case <-checkStopTimer.C:
			stop, _ := this.taskMgr.IsTaskStop(taskId)
			if stop {
				log.Debugf("stop wait for fetch because task is stop: %t", stop)
				if !closeCancelFetch {
					closeCancelFetch = true
					close(cancelFetch)
				}
				return nil
			}
			if timeout, _ := this.taskMgr.IsTaskTimeout(taskId); !timeout {
				continue
			}
			if !closeCancelFetch {
				closeCancelFetch = true
				close(cancelFetch)
			}
			sent := uint64(this.taskMgr.UploadedBlockCount(taskId) / (uint64(copyNum) + 1))
			log.Debugf("timeout check totalCount %d != sent %d %d", totalCount, sent, this.taskMgr.UploadedBlockCount(taskId))
			if totalCount != sent {
				return dspErr.New(dspErr.TASK_WAIT_TIMEOUT, "wait for fetch file: %s timeout", fileHashStr)
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
			return false, dspErr.New(dspErr.FS_GET_DATA_ERROR, "block len %d, tag len %d hash %s, peer %s failed", len(blockMsgData.blockData), len(blockMsgData.tag), reqInfo.Hash, reqInfo.PeerAddr)
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
		return false, dspErr.New(dspErr.BLOCK_HAS_SENT, "no block to send of file: %s", fileHashStr)
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

func (this *Dsp) finishUpload(taskId, fileHashStr string, opt *fs.UploadOption, totalCount uint64) (*common.UploadResult, error) {
	this.taskMgr.EmitProgress(taskId, task.TaskUploadFileWaitForPDPProve)
	proved := this.checkFileBeProved(fileHashStr, opt.CopyNum)
	log.Debugf("checking file: %s proved done %t", fileHashStr, proved)
	if !proved {
		return nil, dspErr.New(dspErr.FILE_UPLOADED_CHECK_PDP_FAILED, "file has sent, but no enough prove is finished")
	}
	if pause, err := this.checkIfPause(taskId, fileHashStr); err != nil || pause {
		return nil, err
	}
	this.taskMgr.EmitProgress(taskId, task.TaskUploadFileWaitForPDPProveDone)
	dnsRegTx, err := this.taskMgr.GetRegUrlTx(taskId)
	if err != nil {
		return nil, err
	}
	dnsBindTx, err := this.taskMgr.GetBindUrlTx(taskId)
	if err != nil {
		return nil, err
	}
	saveLink := utils.GenOniLink(fileHashStr, string(opt.FileDesc), this.chain.WalletAddress(), opt.FileSize, totalCount, this.dns.TrackerUrls)
	if len(dnsRegTx) == 0 || len(dnsBindTx) == 0 {
		dnsRegTx, dnsBindTx = this.registerUrls(taskId, fileHashStr, saveLink, opt, totalCount)
	}
	storeTx, err := this.taskMgr.GetStoreTx(taskId)
	if err != nil {
		return nil, err
	}
	addWhiteListTx, err := this.taskMgr.GetWhitelistTx(taskId)
	if err != nil {
		return nil, err
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
	if err := this.taskMgr.SetTaskInfoWithOptions(taskId, task.RegUrlTx(dnsRegTx), task.BindUrlTx(dnsBindTx)); err != nil {
		return nil, err
	}
	return uploadRet, nil
}

// uploadOptValid check upload opt valid
func uploadOptValid(filePath string, opt *fs.UploadOption) error {
	if !common.FileExisted(filePath) {
		return dspErr.New(dspErr.INVALID_PARAMS, "file not exist")
	}
	if opt.Encrypt && len(opt.EncryptPassword) == 0 {
		return dspErr.New(dspErr.INVALID_PARAMS, "encrypt password is missing")
	}
	return nil
}

func keyOfUnixNode(hash string, index uint32) string {
	return fmt.Sprintf("%s-%d", hash, index)
}
