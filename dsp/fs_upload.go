package dsp

import (
	"fmt"
	"math"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/saveio/dsp-go-sdk/actor/client"
	"github.com/saveio/dsp-go-sdk/common"
	"github.com/saveio/dsp-go-sdk/core/chain"
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
	taskInfo, err := this.taskMgr.GetTaskInfoCopy(taskId)
	if err != nil || taskInfo == nil {
		return true, dspErr.New(dspErr.GET_FILEINFO_FROM_DB_ERROR, "get file task info is nil %s", err)
	}
	now, err := this.chain.GetCurrentBlockHeight()
	if err != nil {
		log.Errorf("get current block height err %s", err)
		return true, err
	}
	if taskInfo.ExpiredHeight > uint64(now) {
		return true, nil
	}
	if len(taskInfo.FileHash) == 0 {
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
func (this *Dsp) UploadFile(newTask bool, taskId, filePath string, opt *fs.UploadOption) (uploadRet *common.UploadResult, err error) {
	// emit result finally
	defer func() {
		sdkErr, _ := err.(*dspErr.Error)
		if uploadRet == nil {
			if sdkErr != nil {
				log.Errorf("task %s upload finish %v, err: %v", err)
			} else {
				log.Debugf("task %s is paused", taskId)
			}
			this.taskMgr.EmitResult(taskId, nil, sdkErr)
		} else {
			log.Debugf("task %v upload finish, result %v", taskId, uploadRet)
			this.taskMgr.EmitResult(taskId, uploadRet, sdkErr)
		}
		// delete task from cache in the end when success
		if uploadRet != nil && err == nil {
			this.taskMgr.DeleteTask(taskId)
		}
	}()
	if newTask {
		// new task because of task id is empty, generate a new task
		if taskId, err = this.taskMgr.NewTask(taskId, store.TaskTypeUpload); err != nil {
			return nil, err
		}
		if err := this.taskMgr.SetTaskState(taskId, store.TaskStateDoing); err != nil {
			return nil, err
		}
		this.taskMgr.EmitProgress(taskId, task.TaskCreate)
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
	if err = this.taskMgr.SetTaskInfoWithOptions(taskId,
		task.FilePath(filePath),
		task.SimpleCheckSum(checksum),
		task.StoreType(uint32(opt.StorageType)),
		task.RealFileSize(opt.FileSize),
		task.ProveInterval(opt.ProveInterval),
		task.ExpiredHeight(opt.ExpiredHeight),
		task.Privilege(opt.Privilege),
		task.CopyNum(uint32(opt.CopyNum)),
		task.Encrypt(opt.Encrypt),
		task.EncryptPassword(opt.EncryptPassword),
		task.RegisterDNS(opt.RegisterDNS),
		task.BindDNS(opt.BindDNS),
		task.WhiteList(fsWhiteListToWhiteList(opt.WhiteList)),
		task.Share(opt.Share),
		task.StoreType(uint32(opt.StorageType)),
		task.Url(string(opt.DnsURL)),
		task.FileName(string(opt.FileDesc)),
		task.Walletaddr(this.chain.WalletAddress()),
		task.CopyNum(uint32(opt.CopyNum))); err != nil {
		return nil, err
	}

	// bind task id with file path
	if err = this.taskMgr.BindTaskId(taskId); err != nil {
		return nil, err
	}
	this.taskMgr.EmitProgress(taskId, task.TaskUploadFileMakeSlice)
	var tx, fileHashStr, prefixStr string
	var totalCount uint64
	log.Debugf("upload task %s, will split for file", taskId)
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
			FileName:   string(opt.FileDesc),
		}
		filePrefix.MakeSalt()
		prefixStr = filePrefix.String()
		log.Debugf("node from file prefix: %v, len: %d", prefixStr, len(prefixStr))
		if hashes, err = this.fs.NodesFromFile(filePath, prefixStr,
			opt.Encrypt, string(opt.EncryptPassword)); err != nil {
			return nil, err
		}
	} else {
		// file has paid, get prefix
		prefixBuf, err := this.taskMgr.GetFilePrefix(taskId)
		if err != nil {
			return nil, err
		}
		prefixStr = string(prefixBuf)
		log.Debugf("prefix of file %s is %v, its len: %d", fileHashStr, prefixStr, len(prefixStr))
		fileHashStr, err = this.taskMgr.GetTaskFileHash(taskId)
		if err != nil {
			return nil, err
		}
		hashes, err = this.fs.GetFileAllHashes(fileHashStr)
		if err != nil {
			return nil, err
		}
	}
	if len(prefixStr) == 0 {
		return nil, dspErr.New(dspErr.SHARDING_FAIELD, "missing file prefix")
	}
	if totalCount = uint64(len(hashes)); totalCount == 0 {
		return nil, dspErr.New(dspErr.SHARDING_FAIELD, "no blocks to upload")
	}
	blocksRoot := utils.ComputeStringHashRoot(hashes)
	log.Debugf("sharding file finished, taskId:%s, path: %s, fileHash: %s, totalCount:%d, blocksRoot: %x",
		taskId, filePath, hashes[0], totalCount, blocksRoot)
	this.taskMgr.EmitProgress(taskId, task.TaskUploadFileMakeSliceDone)
	fileHashStr = hashes[0]
	if err = this.taskMgr.SetTaskInfoWithOptions(taskId,
		task.FileHash(fileHashStr),
		task.BlocksRoot(blocksRoot),
		task.TotalBlockCnt(totalCount),
		task.FileSize(getFileSizeWithBlockCount(totalCount)),
		task.Prefix(prefixStr)); err != nil {
		return nil, err
	}
	fi, _ := this.chain.GetFileInfo(fileHashStr)
	if newTask && (fi != nil || this.taskMgr.PauseDuplicatedTask(taskId, fileHashStr)) {
		return nil, dspErr.New(dspErr.UPLOAD_TASK_EXIST, "file has uploading or uploaded, please cancel the task")
	}
	// bind task id with file hash
	if err = this.taskMgr.BindTaskId(taskId); err != nil {
		return nil, err
	}
	if pause, err := this.checkIfPause(taskId, fileHashStr); err != nil || pause {
		return nil, err
	}
	// bind task id with file hash
	if err = this.taskMgr.BindTaskId(taskId); err != nil {
		return nil, err
	}
	// check has uploaded this file
	if this.taskMgr.IsFileUploaded(taskId, false) {
		taskInfo, err := this.taskMgr.GetTaskInfoCopy(taskId)
		if err != nil {
			return nil, err
		}
		return this.getFileUploadResult(taskInfo, opt)
	}
	// pay file
	if err := this.payForSendFile(taskId); err != nil {
		// rollback transfer state
		log.Errorf("task %s pay for file %s failed %s", taskId, fileHashStr, err)
		this.taskMgr.EmitProgress(taskId, task.TaskUploadFileFindReceivers)
		return nil, dspErr.NewWithError(dspErr.PAY_FOR_STORE_FILE_FAILED, err)
	}
	if pause, err := this.checkIfPause(taskId, fileHashStr); err != nil || pause {
		return nil, err
	}
	this.taskMgr.EmitProgress(taskId, task.TaskUploadFilePayingDone)
	if _, err = this.addWhitelist(taskId, fileHashStr, opt); err != nil {
		return nil, err
	}
	if opt != nil && opt.Share {
		go func() {
			if err := this.shareUploadedFile(filePath, string(opt.FileDesc), this.chain.WalletAddress(),
				hashes); err != nil {
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
	this.taskMgr.EmitProgress(taskId, task.TaskUploadFileFindReceiversDone)
	if err = this.sendBlocks(taskId, hashes); err != nil {
		log.Errorf("send blocks err %s", err)
		return nil, err
	}
	log.Debugf("wait for fetch blocks finish id: %s ", taskId)
	if pause, err := this.checkIfPause(taskId, fileHashStr); err != nil || pause {
		return nil, err
	}
	taskInfo, err := this.taskMgr.GetTaskInfoCopy(taskId)
	if err != nil {
		return nil, err
	}
	uploadRet, err = this.getFileUploadResult(taskInfo, opt)
	if err != nil {
		return nil, err
	}
	log.Debug("upload success result %v", uploadRet)
	return uploadRet, nil
}

// PauseUpload. pause a task
func (this *Dsp) PauseUpload(taskId string) error {
	tsk, ok := this.taskMgr.GetTaskById(taskId)
	if !ok || tsk == nil {
		return dspErr.New(dspErr.GET_FILEINFO_FROM_DB_ERROR, "task %s not found", taskId)
	}
	if err := tsk.Pause(); err != nil {
		return dspErr.NewWithError(dspErr.SET_FILEINFO_DB_ERROR, err)
	}
	this.taskMgr.EmitProgress(taskId, task.TaskPause)
	return nil
}

func (this *Dsp) ResumeUpload(taskId string) error {
	tsk, ok := this.taskMgr.GetTaskById(taskId)
	if !ok || tsk == nil {
		return dspErr.New(dspErr.GET_FILEINFO_FROM_DB_ERROR, "task %s not found", taskId)
	}
	if err := tsk.Resume(); err != nil {
		return dspErr.NewWithError(dspErr.SET_FILEINFO_DB_ERROR, err)
	}
	this.taskMgr.EmitProgress(taskId, task.TaskDoing)
	return this.checkIfResume(taskId)
}

func (this *Dsp) CancelUpload(taskId string, gasLimit uint64) (*common.DeleteUploadFileResp, error) {
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
		resp, err := this.DeleteUploadedFileByIds([]string{taskId}, gasLimit)
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
	ret, err := client.P2pBroadcast(nodeList, msg.ToProtoMsg(), msg.MessageId)
	log.Debugf("broadcast cancel msg ret %v, err: %s", ret, err)
	resp, err := this.DeleteUploadedFileByIds([]string{taskId}, gasLimit)
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

func (this *Dsp) DeleteUploadFilesFromChain(fileHashStrs []string, gasLimit uint64) (string, uint32, error) {
	if len(fileHashStrs) == 0 {
		return "", 0, nil
		// return "", 0, dspErr.New(dspErr.DELETE_FILE_HASHES_EMPTY, "delete file hash string is empty")
	}
	needDeleteFile := false
	for _, fileHashStr := range fileHashStrs {
		info, err := this.chain.GetFileInfo(fileHashStr)
		log.Debugf("delete file get fileinfo %v, err %v", info, err)
		if err != nil && !this.chain.IsFileInfoDeleted(err) {
			log.Debugf("info:%v, other err:%s", info, err)
			return "", 0, dspErr.New(dspErr.FILE_NOT_FOUND_FROM_CHAIN,
				"file info not found, %s has deleted", fileHashStr)
		}
		if info != nil && info.FileOwner.ToBase58() != this.chain.WalletAddress() {
			return "", 0, dspErr.New(dspErr.DELETE_FILE_ACCESS_DENIED,
				"file %s can't be deleted, you are not the owner", fileHashStr)
		}
		if info != nil && err == nil {
			needDeleteFile = true
		}
	}
	if !needDeleteFile {
		return "", 0, dspErr.New(dspErr.NO_FILE_NEED_DELETED, "no file to delete")
	}
	txHashStr, err := this.chain.DeleteFiles(fileHashStrs, gasLimit)
	log.Debugf("delete file tx %v, err %v", txHashStr, err)
	if err != nil {
		return "", 0, dspErr.NewWithError(dspErr.CHAIN_ERROR, err)
	}
	log.Debugf("delete file txHash %s", txHashStr)
	txHeight, err := this.chain.PollForTxConfirmed(time.Duration(common.TX_CONFIRM_TIMEOUT)*time.Second, txHashStr)
	if err != nil || txHeight == 0 {
		return "", 0, dspErr.New(dspErr.CHAIN_ERROR, "wait for tx confirmed failed")
	}
	log.Debugf("delete file tx height %d, err %v", txHeight, err)
	if err != nil {
		return "", 0, dspErr.NewWithError(dspErr.CHAIN_ERROR, err)
	}
	return txHashStr, txHeight, nil
}

// DeleteUploadedFileByIds. Delete uploaded file from remote nodes. it is called by the owner
func (this *Dsp) DeleteUploadedFileByIds(ids []string, gasLimit uint64) ([]*common.DeleteUploadFileResp, error) {
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
		if this.taskMgr.ExistSameUploadFile(id, hash) {
			log.Debugf("exist same file %s uploading, ignore delete it from chain", hash)
			continue
		}
		fileHashStrs = append(fileHashStrs, hash)
	}

	txHashStr, txHeight, err := this.DeleteUploadFilesFromChain(fileHashStrs, gasLimit)
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
			nodesFromTsk := utils.Base58ToWalletAddrs(this.taskMgr.GetUploadedBlockNodeList(taskId, fileHashStr, 0))
			host, err := this.chain.GetNodeHostAddrListByWallets(nodesFromTsk)
			if err != nil {
				log.Errorf("get nodes empty from wallets %v", nodesFromTsk)
			}
			storingNode = append(storingNode, host...)
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
		reply := func(msg proto.Message, hostAddr string) bool {
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
				HostAddr: hostAddr,
				Code:     errCode,
				Error:    errMsg,
			})
			return false
		}
		m, err := client.P2pBroadcast(storingNode, msg.ToProtoMsg(), msg.MessageId, reply)
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

func (this *Dsp) GetDeleteFilesStorageFee(addr chainCom.Address, fileHashStrs []string) (uint64, error) {
	return this.chain.GetDeleteFilesStorageFee(addr, fileHashStrs)
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
	tsk, err := this.taskMgr.GetTaskInfoCopy(taskId)
	if err != nil || tsk == nil {
		return false, dspErr.New(dspErr.GET_FILEINFO_FROM_DB_ERROR, "task %s not found", taskId)
	}
	if len(tsk.PrimaryNodes) == 0 {
		return pause, nil
	}
	hostAddr := this.getMasterNodeHostAddr(tsk.PrimaryNodes[0])
	// send pause msg
	msg := message.NewFileMsg(fileHashStr, netcomm.FILE_OP_FETCH_PAUSE,
		message.WithSessionId(taskId),
		message.WithWalletAddress(this.chain.WalletAddress()),
		message.WithSign(this.chain.CurrentAccount()),
	)
	ret, err := client.P2pBroadcast([]string{hostAddr}, msg.ToProtoMsg(), msg.MessageId)
	if err != nil {
		return false, err
	}
	log.Debugf("broadcast pause msg ret %v", ret)
	return pause, nil
}

func (this *Dsp) checkIfResume(taskId string) error {
	tsk, err := this.taskMgr.GetTaskInfoCopy(taskId)
	if err != nil || tsk == nil {
		return dspErr.New(dspErr.GET_FILEINFO_FROM_DB_ERROR, "can't find download options, please retry")
	}
	opt := &fs.UploadOption{
		FileDesc:        []byte(tsk.FileName),
		FileSize:        tsk.RealFileSize,
		ProveInterval:   tsk.ProveInterval,
		ExpiredHeight:   tsk.ExpiredHeight,
		Privilege:       tsk.Privilege,
		CopyNum:         uint64(tsk.CopyNum),
		Encrypt:         tsk.Encrypt,
		EncryptPassword: tsk.EncryptPassword,
		RegisterDNS:     tsk.RegisterDNS,
		BindDNS:         tsk.BindDNS,
		DnsURL:          []byte(tsk.Url),
		WhiteList:       whiteListToFsWhiteList(tsk.WhiteList),
		Share:           tsk.Share,
		StorageType:     uint64(tsk.StoreType),
	}
	// send resume msg
	if len(tsk.FileHash) == 0 {
		go this.UploadFile(false, taskId, tsk.FilePath, opt)
		return nil
	}
	if this.taskMgr.ExistSameUploadFile(taskId, tsk.FileHash) {
		err := dspErr.New(dspErr.UPLOAD_TASK_EXIST, "file has uploading or uploaded, please cancel the task")
		this.taskMgr.EmitResult(taskId, nil, err)
		return err
	}
	fileInfo, _ := this.chain.GetFileInfo(tsk.FileHash)
	if fileInfo != nil {
		now, err := this.chain.GetCurrentBlockHeight()
		if err != nil {
			return err
		}
		if fileInfo.ExpiredHeight <= uint64(now) {
			return dspErr.New(dspErr.FILE_IS_EXPIRED, "file:%s has expired", tsk.FileHash)
		}
		hasProvedFile := uint64(len(this.getFileProvedNode(tsk.FileHash))) == opt.CopyNum+1
		if hasProvedFile || this.taskMgr.IsFileUploaded(taskId, false) {
			log.Debugf("hasProvedFile: %t ", hasProvedFile)
			uploadRet, err := this.getFileUploadResult(tsk, opt)
			if err != nil {
				return err
			}
			log.Debug("upload success result %v", uploadRet)
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

	if len(tsk.PrimaryNodes) == 0 {
		go this.UploadFile(false, taskId, tsk.FilePath, opt)
		return nil
	}
	hostAddr := this.getMasterNodeHostAddr(tsk.PrimaryNodes[0])
	if len(hostAddr) == 0 {
		return dspErr.New(dspErr.GET_HOST_ADDR_ERROR, "host addr is empty for %s", tsk.PrimaryNodes)
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
	msg := message.NewFileMsg(tsk.FileHash, netcomm.FILE_OP_FETCH_RESUME,
		message.WithSessionId(taskId),
		message.WithWalletAddress(this.chain.WalletAddress()),
		message.WithSign(this.chain.CurrentAccount()),
	)
	_, err = client.P2pBroadcast([]string{hostAddr}, msg.ToProtoMsg(), msg.MessageId)
	if err != nil {
		return err
	}
	log.Debugf("resume upload file")
	go this.UploadFile(false, taskId, tsk.FilePath, opt)
	return nil
}

// payForSendFile pay before send a file
// return PoR params byte slice, PoR private key of the file or error
func (this *Dsp) payForSendFile(taskId string) error {
	taskInfo, err := this.taskMgr.GetTaskInfoCopy(taskId)
	if err != nil {
		return err
	}
	fileInfo, err := this.chain.GetFileInfo(taskInfo.FileHash)
	if err != nil && !strings.Contains(err.Error(), chain.ErrNoFileInfo) {
		// FsGetFileInfo not found
		log.Errorf("get file info err %s", err)
	}
	var paramsBuf, privateKey []byte
	var tx string

	var primaryNodes []chainCom.Address
	if fileInfo != nil {
		primaryNodes = fileInfo.PrimaryNodes.AddrList
	}
	// get nodeList
	this.taskMgr.EmitProgress(taskId, task.TaskUploadFileFindReceivers)
	walletAddrs, err := this.findReceivers(taskId, primaryNodes)
	if err != nil {
		return dspErr.NewWithError(dspErr.SEARCH_RECEIVERS_FAILED, err)
	}
	log.Debugf("find receivers %v", walletAddrs)
	this.taskMgr.EmitProgress(taskId, task.TaskUploadFilePaying)
	if pause, err := this.checkIfPause(taskId, taskInfo.FileHash); err != nil || pause {
		return err
	}
	if fileInfo == nil {
		log.Info("first upload file")
		blockSizeInKB := uint64(math.Ceil(float64(common.CHUNK_SIZE) / 1024.0))
		g, g0, pubKey, privKey, fileID := pdp.Init(taskInfo.FilePath)
		paramsBuf, err = this.chain.ProveParamSer(g, g0, pubKey, fileID)
		privateKey = privKey
		if err != nil {
			log.Errorf("serialization prove params failed:%s", err)
			return err
		}
		candidateNodes, err := this.chain.GetNodeListWithoutAddrs(walletAddrs, 2*len(walletAddrs))
		if err != nil {
			return err
		}
		log.Debugf("primary nodes: %v, candidate nodes: %v", walletAddrs, candidateNodes)
		tx, err = this.chain.StoreFile(taskInfo.FileHash, taskInfo.BlocksRoot, taskInfo.TotalBlockCount,
			blockSizeInKB, taskInfo.ProveInterval, taskInfo.ExpiredHeight, uint64(taskInfo.CopyNum),
			[]byte(taskInfo.FileName), uint64(taskInfo.Privilege),
			paramsBuf, uint64(taskInfo.StoreType), taskInfo.RealFileSize, walletAddrs, candidateNodes)
		if err != nil {
			log.Errorf("pay store file order failed:%s", err)
			return err
		}
		this.taskMgr.EmitProgress(taskId, task.TaskWaitForBlockConfirmed)
		// TODO: check confirmed on receivers
		txHeight, err := this.chain.PollForTxConfirmed(time.Duration(common.TX_CONFIRM_TIMEOUT)*time.Second, tx)
		if err != nil || txHeight == 0 {
			log.Debugf("store file params fileHash:%v, blockNum:%v, blockSize:%v, "+
				"proveInterval:%v, expiredHeight:%v, copyNum:%v, fileName:%v, privilege:%v, params:%x,"+
				" storeType:%v, fileSize:%v, primary:%v, candidates:%v",
				taskInfo.FileHash, taskInfo.TotalBlockCount, blockSizeInKB, taskInfo.ProveInterval,
				taskInfo.ExpiredHeight, uint64(taskInfo.CopyNum), []byte(taskInfo.FileName),
				uint64(taskInfo.Privilege), paramsBuf, uint64(taskInfo.StoreType), taskInfo.RealFileSize,
				walletAddrs, candidateNodes)
			log.Errorf("poll tx failed %s", err)
			return err
		}
		if this.config.BlockConfirm > 0 {
			_, err = this.chain.WaitForGenerateBlock(time.Duration(common.TX_CONFIRM_TIMEOUT)*time.Second,
				this.config.BlockConfirm)
			if err != nil {
				log.Errorf("wait for generate err %s", err)
				return err
			}
		}
		primaryNodeHostAddrs, _ := this.chain.GetNodeHostAddrListByWallets(walletAddrs)
		candidateNodeHostAddrs, _ := this.chain.GetNodeHostAddrListByWallets(candidateNodes)
		primaryNodeMap := utils.WalletHostAddressMap(walletAddrs, primaryNodeHostAddrs)
		candidateNodeHostMap := utils.WalletHostAddressMap(candidateNodes, candidateNodeHostAddrs)
		this.taskMgr.EmitProgress(taskId, task.TaskWaitForBlockConfirmedDone)
		if err = this.taskMgr.SetTaskInfoWithOptions(taskId,
			task.StoreTx(tx),
			task.StoreTxHeight(txHeight),
			task.FileOwner(this.chain.WalletAddress()),
			task.PrimaryNodes(utils.WalletAddrsToBase58(walletAddrs)),
			task.CandidateNodes(utils.WalletAddrsToBase58(candidateNodes)),
			task.NodeHostAddrs(utils.MergeTwoAddressMap(primaryNodeMap, candidateNodeHostMap)),
			task.ProveParams(paramsBuf),
			task.PrivateKey(privateKey)); err != nil {
			log.Errorf("set task info err %s", err)
			return err
		}
	} else {
		paramsBuf = fileInfo.FileProveParam
		privateKey, err = this.taskMgr.GetFilePrivateKey(taskId)
		log.Debugf("get private key from task %s, key: %d", taskId, len(privateKey))
		if err != nil {
			return err
		}
		tx, err = this.taskMgr.GetStoreTx(taskId)
		if err != nil {
			return err
		}
	}
	if len(paramsBuf) == 0 || len(privateKey) == 0 {
		log.Errorf("param buf len: %d, private key len: %d", len(paramsBuf), len(privateKey))
		return dspErr.New(dspErr.PDP_PRIVKEY_NOT_FOUND,
			"PDP private key is not found. Please delete file and retry to upload it")
	}
	log.Debugf("pay for file success")
	return nil
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

func (this *Dsp) registerUrls(taskId, fileHashStr, saveLink string, opt *fs.UploadOption) (string, string) {
	this.taskMgr.EmitProgress(taskId, task.TaskUploadFileRegisterDNS)
	var dnsRegTx, dnsBindTx string
	var err error
	if opt.RegisterDNS && len(opt.DnsURL) > 0 {
		dnsRegTx, err = this.dns.RegisterFileUrl(string(opt.DnsURL), saveLink)
		if err != nil {
			log.Errorf("register url err: %s", err)
		}
		log.Debugf("acc %s, reg dns %s for %s", this.chain.WalletAddress(), dnsRegTx, fileHashStr)
	}
	if opt.BindDNS && len(opt.DnsURL) > 0 {
		dnsBindTx, err = this.dns.BindFileUrl(string(opt.DnsURL), saveLink)
		if err != nil {
			log.Errorf("bind url err: %s", err)
		}
		log.Debugf("bind dns %s for file %s", dnsBindTx, fileHashStr)
	}

	if this.config.BlockConfirm > 0 && (len(dnsRegTx) > 0 || len(dnsBindTx) > 0) {
		this.taskMgr.EmitProgress(taskId, task.TaskWaitForBlockConfirmed)
		_, err := this.chain.WaitForGenerateBlock(time.Duration(common.TX_CONFIRM_TIMEOUT)*time.Second,
			this.config.BlockConfirm)
		this.taskMgr.EmitProgress(taskId, task.TaskWaitForBlockConfirmedDone)
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
	log.Debugf("fileHashStr %s prove details :%v, err:%s", fileHashStr, len(proveDetails.ProveDetails), err)
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

// getUploadNodeFromChain. get primary nodelist (primary nodes mean the nodes fetch files) from file infofirst.
// if empty, get nodelist from chain
func (this *Dsp) getUploadNodeFromChain(filePath, taskKey, fileHashStr string,
	copyNum uint64) ([]chainCom.Address, error) {
	fileStat, err := os.Stat(filePath)
	if err != nil {
		return nil, dspErr.NewWithError(dspErr.INVALID_PARAMS, err)
	}
	infos, err := this.chain.GetNodeList()
	if err != nil {
		return nil, err
	}
	expectNodeNum := 4 * (copyNum + 1)
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
func (this *Dsp) checkFileBeProved(fileHashStr string) bool {
	retry := 0
	timewait := 5
	for {
		if retry > common.CHECK_PROVE_TIMEOUT/timewait {
			return false
		}
		nodes := this.getFileProvedNode(fileHashStr)
		if uint64(len(nodes)) > 0 {
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
func (this *Dsp) findReceivers(taskId string, primaryNodes []chainCom.Address) ([]chainCom.Address, error) {
	taskInfo, err := this.taskMgr.GetTaskInfoCopy(taskId)
	if err != nil {
		return nil, err
	}
	var nodeWalletAddrList []chainCom.Address
	var nodeList []string
	receiverCount := int(taskInfo.CopyNum + 1)
	if primaryNodes != nil && len(primaryNodes) > 0 {
		// get host addr from chain
		nodeWalletAddrList = primaryNodes
		nodeList, err = this.chain.GetNodeHostAddrListByWallets(nodeWalletAddrList[:1])
		if err != nil {
			return nil, err
		}
		receiverCount = 1
	} else {
		// get nodes
		nodeWalletAddrList, err = this.getUploadNodeFromChain(taskInfo.FilePath, taskId,
			taskInfo.FileHash, uint64(taskInfo.CopyNum))
		if err != nil {
			return nil, dspErr.NewWithError(dspErr.GET_STORAGE_NODES_FAILED, err)
		}
		// addr1, _ := chainCom.AddressFromBase58("AVR3MZhqbvG8wwqxmTMF45BJFJMJLvTh4i")
		// addr2, _ := chainCom.AddressFromBase58("ARKjRDdJ1ABBus3UtPqGwn3ZyS4WzL23LK")
		// addr3, _ := chainCom.AddressFromBase58("AdPV78xrEhmPo1ehVUNwvJtZ9BJd8mNJMw")
		// nodeWalletAddrList = []chainCom.Address{addr1, addr2, addr3}
		log.Debugf("uploading nodelist %v, opt.CopyNum %d", nodeWalletAddrList, taskInfo.CopyNum)
		if len(nodeWalletAddrList) < int(taskInfo.CopyNum+1) {
			return nil, dspErr.New(dspErr.ONLINE_NODES_NOT_ENOUGH, "node is not enough %d, copyNum %d",
				len(nodeWalletAddrList), taskInfo.CopyNum)
		}
		// get host addrs
		nodeList, err = this.chain.GetNodeHostAddrListByWallets(nodeWalletAddrList)
		if err != nil {
			return nil, err
		}
	}
	msg := message.NewFileMsg(taskInfo.FileHash, netcomm.FILE_OP_FETCH_ASK,
		message.WithSessionId(taskId), // use task id as session id to remote peers
		message.WithWalletAddress(this.chain.WalletAddress()),
		message.WithSign(this.chain.CurrentAccount()),
	)
	log.Debugf("broadcast fetch_ask msg of file: %s, to %v", taskInfo.FileHash, nodeList)
	this.taskMgr.SetTaskNetPhase(taskId, nodeList, int(common.WorkerSendFileAsk))
	walletAddrs, err := this.broadcastAskMsg(taskId, msg, nodeList, receiverCount)
	if err != nil {
		return nil, err
	}
	if len(walletAddrs) < receiverCount {
		return nil, dspErr.New(dspErr.RECEIVERS_NOT_ENOUGH, "file receivers is not enough")
	}
	for _, walletAddress := range walletAddrs {
		this.taskMgr.SetTaskNetPhase(taskId, []string{walletAddress.ToBase58()}, int(common.WorkerRecvFileAck))
	}
	return walletAddrs, nil
}

// broadcastAskMsg. send ask msg to nodelist, and expect min response count of node to response a ack msg
// return the host addr list in order
func (this *Dsp) broadcastAskMsg(taskId string, msg *message.Message, nodeList []string, minResponse int) (
	[]chainCom.Address, error) {
	lock := new(sync.Mutex)
	walletAddrs := make([]chainCom.Address, 0)
	backupAddrs := make([]chainCom.Address, 0)
	existWallet := make(map[string]struct{})
	stop := false
	height, _ := this.chain.GetCurrentBlockHeight()
	nodeListLen := len(nodeList)
	action := func(res proto.Message, hostAddr string) bool {
		lock.Lock()
		defer lock.Unlock()
		p2pMsg := message.ReadMessage(res)
		if p2pMsg.Error != nil && p2pMsg.Error.Code != dspErr.SUCCESS {
			log.Errorf("get file fetch_ack msg err code %d, msg %s", p2pMsg.Error.Code, p2pMsg.Error.Message)
			return false
		}
		if stop {
			log.Debugf("break here after stop is true")
			return true
		}
		fileMsg := p2pMsg.Payload.(*file.File)
		walletAddr := fileMsg.PayInfo.WalletAddress
		log.Debugf("recv file ack msg from %s file %s", walletAddr, fileMsg.Hash)
		if _, ok := existWallet[fileMsg.PayInfo.WalletAddress]; ok {
			return false
		}
		existWallet[fileMsg.PayInfo.WalletAddress] = struct{}{}
		walletAddress, err := chainCom.AddressFromBase58(fileMsg.PayInfo.WalletAddress)
		if err != nil {
			return false
		}
		// compare chain info
		if fileMsg.ChainInfo.Height > height && fileMsg.ChainInfo.Height > height+common.MAX_BLOCK_HEIGHT_DIFF {
			log.Debugf("remote node block height is %d, but current block height is %d",
				fileMsg.ChainInfo.Height, height)
			backupAddrs = append(backupAddrs, walletAddress)
			return false
		}
		if height > fileMsg.ChainInfo.Height && height > fileMsg.ChainInfo.Height+common.MAX_BLOCK_HEIGHT_DIFF {
			log.Debugf("remote node block height is %d, but current block height is %d",
				fileMsg.ChainInfo.Height, height)
			backupAddrs = append(backupAddrs, walletAddress)
			return false
		}

		// lower priority node, put to backup list
		if this.taskMgr.IsWorkerBusy(taskId, walletAddr, int(common.WorkerSendFileAsk)) {
			log.Debugf("peer %s is busy, put to backup list", walletAddr)
			backupAddrs = append(backupAddrs, walletAddress)
			return false
		}
		if bad, err := client.P2pIsPeerNetQualityBad(walletAddr, client.P2pNetTypeDsp); bad || err != nil {
			log.Debugf("peer %s network quality is bad, put to backup list", walletAddr)
			backupAddrs = append(backupAddrs, walletAddress)
			return false
		}
		walletAddrs = append(walletAddrs, walletAddress)
		log.Debugf("send file_ask msg success of file: %s, to: %s  receivers: %v", fileMsg.Hash, walletAddr, walletAddrs)
		primaryListLen := len(walletAddrs)
		backupListLen := len(backupAddrs)
		if primaryListLen >= minResponse || primaryListLen >= nodeListLen ||
			backupListLen >= nodeListLen {
			log.Debugf("break receiver goroutine")
			stop = true
			return true
		}
		log.Debugf("continue....")
		return false
	}
	ret, err := client.P2pBroadcast(nodeList, msg.ToProtoMsg(), msg.MessageId, action)
	if err != nil {
		log.Errorf("wait file receivers broadcast err %s", err)
		return nil, err
	}
	log.Debugf("receives %v, backupAddrs: %v, broadcast file ask ret %v, minResponse %d",
		walletAddrs, backupAddrs, ret, minResponse)
	if len(walletAddrs) >= minResponse {
		return walletAddrs, nil
	}
	if len(walletAddrs)+len(backupAddrs) < minResponse {
		log.Errorf("find %d primary nodes and %d back up nodes, but the file need %d nodes",
			len(walletAddrs), len(backupAddrs), minResponse)
		return nil, dspErr.New(dspErr.RECEIVERS_NOT_ENOUGH, "no enough nodes, please retry later")
	}
	// append backup nodes to tail with random range
	log.Debugf("backup list %v", backupAddrs)
	for _, walletAddr := range backupAddrs {
		walletAddrs = append(walletAddrs, walletAddr)
		if len(walletAddrs) >= minResponse {
			break
		}
	}
	log.Debugf("receives :%v, ret %v", walletAddrs, ret)
	return walletAddrs, nil
}

// notifyFetchReady send fetch_rdy msg to receivers
func (this *Dsp) sendFetchReadyMsg(taskId, peerWalletAddr string) (*message.Message, error) {
	tsk, err := this.taskMgr.GetTaskInfoCopy(taskId)
	if err != nil || tsk == nil {
		return nil, dspErr.New(dspErr.GET_FILEINFO_FROM_DB_ERROR, "get file task info is nil %s", err)
	}
	msg := message.NewFileMsg(tsk.FileHash, netcomm.FILE_OP_FETCH_RDY,
		message.WithSessionId(taskId),
		message.WithWalletAddress(this.chain.WalletAddress()),
		message.WithBlocksRoot(tsk.BlocksRoot),
		message.WithTxHash(tsk.StoreTx),
		message.WithTxHeight(uint64(tsk.StoreTxHeight)),
		message.WithPrefix(tsk.Prefix),
		message.WithTotalBlockCount(tsk.TotalBlockCount),
		message.WithSign(this.chain.CurrentAccount()),
	)
	log.Debugf("send ready msg tx %s, height %d, sessionId %s, prefix %s, blocks root %s",
		tsk.StoreTx, tsk.StoreTxHeight, taskId, tsk.Prefix, tsk.BlocksRoot)
	resp, err := client.P2pSendAndWaitReply(peerWalletAddr, msg.MessageId, msg.ToProtoMsg())
	if err != nil {
		return nil, err
	}
	p2pMsg := message.ReadMessage(resp)
	if p2pMsg == nil {
		return nil, dspErr.New(dspErr.INTERNAL_ERROR, "read msg failed")
	}
	return p2pMsg, nil
}

// generateBlockMsgData. generate blockdata, blockEncodedData, tagData with prove params
func (this *Dsp) generateBlockMsgData(hash string, index, offset uint64,
	fileID, g0, privateKey []byte) (*blockMsgData, error) {
	// TODO: use disk fetch rather then memory access
	block := this.fs.GetBlock(hash)
	blockData := this.fs.BlockDataOfAny(block)
	if len(blockData) == 0 {
		log.Warnf("get empty block of %s %d", hash, index)
	}
	tag, err := pdp.SignGenerate(blockData, fileID, uint32(index+1), g0, privateKey)
	if err != nil {
		return nil, err
	}
	msgData := &blockMsgData{
		blockData: blockData,
		tag:       tag,
		offset:    offset,
		refCnt:    1,
	}
	return msgData, nil
}

// sendBlocks. prepare block data and tags, send blocks to the node
func (this *Dsp) sendBlocks(taskId string, hashes []string) error {
	tsk, err := this.taskMgr.GetTaskInfoCopy(taskId)
	if err != nil || tsk == nil {
		return dspErr.New(dspErr.GET_FILEINFO_FROM_DB_ERROR, "get file task info is nil %s", err)
	}
	p, err := this.chain.ProveParamDes(tsk.ProveParams)
	if err != nil {
		return dspErr.New(dspErr.GET_PDP_PARAMS_ERROR, err.Error())
	}

	if len(tsk.PrimaryNodes) == 0 || len(tsk.PrimaryNodes[0]) == 0 {
		return dspErr.New(dspErr.GET_TASK_PROPERTY_ERROR, "master node host addr is nil")
	}
	payTxHeight, err := this.chain.GetBlockHeightByTxHash(tsk.StoreTx)
	if err != nil {
		return dspErr.NewWithError(dspErr.PAY_FOR_STORE_FILE_FAILED, err)
	}
	if err := this.taskMgr.SetTaskInfoWithOptions(taskId, task.StoreTxHeight(payTxHeight)); err != nil {
		return dspErr.NewWithError(dspErr.SET_FILEINFO_DB_ERROR, err)
	}
	log.Debugf("pay for send file success %v ", payTxHeight)
	this.taskMgr.EmitProgress(taskId, task.TaskUploadFileGeneratePDPData)
	allOffset, err := this.fs.GetAllOffsets(hashes[0])
	if err != nil {
		return dspErr.New(dspErr.PREPARE_UPLOAD_ERROR, err.Error())
	}
	this.taskMgr.EmitProgress(taskId, task.TaskUploadFileTransferBlocks)
	var getMsgDataLock sync.Mutex
	blockMsgDataMap := make(map[string]*blockMsgData)
	getMsgData := func(hash string, index uint64) *blockMsgData {
		getMsgDataLock.Lock()
		defer getMsgDataLock.Unlock()
		key := keyOfBlockHashAndIndex(hash, index)
		data, ok := blockMsgDataMap[key]
		if ok {
			return data
		}
		offsetKey := fmt.Sprintf("%s-%d", hash, index)
		offset, _ := allOffset[offsetKey]
		var err error
		data, err = this.generateBlockMsgData(hash, index, offset, p.FileId, p.G0, tsk.ProvePrivKey)
		if err != nil {
			return nil
		}
		blockMsgDataMap[key] = data
		return data
	}
	cleanMsgData := func(reqInfos []*block.Block) {
		getMsgDataLock.Lock()
		defer getMsgDataLock.Unlock()
		for _, reqInfo := range reqInfos {
			hash := reqInfo.Hash
			index := uint64(reqInfo.Index)
			key := keyOfBlockHashAndIndex(hash, index)
			data, ok := blockMsgDataMap[key]
			if !ok {
				continue
			}
			data.refCnt--
			if data.refCnt > 0 {
				continue
			}
			delete(blockMsgDataMap, key)
			this.fs.ReturnBuffer(data.blockData)
		}
		if len(reqInfos) > 0 {
			log.Debugf("delete block msg data of key %s-%d to %s-%d", reqInfos[0].Hash, reqInfos[0].Index,
				reqInfos[len(reqInfos)-1].Hash, reqInfos[len(reqInfos)-1].Index)
		}
	}
	return this.sendBlocksToPeer(taskId, tsk.PrimaryNodes[0], hashes, getMsgData, cleanMsgData)
}

// sendBlocksToPeer. send rdy msg, wait its response, and send blocks to the node
func (this *Dsp) sendBlocksToPeer(taskId, peerWalletAddr string, blockHashes []string,
	getMsgData func(hash string, index uint64) *blockMsgData, cleanMsgData func(reqInfos []*block.Block)) error {
	tsk, err := this.taskMgr.GetTaskInfoCopy(taskId)
	if err != nil || tsk == nil {
		return dspErr.New(dspErr.GET_FILEINFO_FROM_DB_ERROR, "get file task info is nil %s", err)
	}
	fileHashStr := tsk.FileHash
	log.Debugf("sendBlocksToPeer %v, taskId: %s, file: %s", peerWalletAddr, taskId, fileHashStr)
	this.taskMgr.NewWorkers(taskId, []string{peerWalletAddr}, true, nil)
	// notify fetch ready to all receivers
	resp, err := this.sendFetchReadyMsg(taskId, peerWalletAddr)
	if err != nil {
		log.Errorf("notify fetch ready msg failed, err %s", err)
		return err
	}
	if resp.Error != nil && resp.Error.Code != netcomm.MSG_ERROR_CODE_NONE {
		log.Errorf("receive rdy msg reply err %d, msg %s", resp.Error.Code, resp.Error.Message)
		return dspErr.New(dspErr.RECEIVE_ERROR_MSG, resp.Error.Message)
	}
	sessionId, err := this.taskMgr.GetSessionId(taskId, "")
	if err != nil {
		return dspErr.New(dspErr.GET_SESSION_ID_FAILED, err.Error())
	}
	respFileMsg := resp.Payload.(*file.File)

	log.Debugf("blockHashes :%d", len(blockHashes))
	startIndex := uint64(0)
	if respFileMsg.Breakpoint != nil && len(respFileMsg.Breakpoint.Hash) != 0 &&
		respFileMsg.Breakpoint.Index != 0 && respFileMsg.Breakpoint.Index < uint64(len(blockHashes)) &&
		blockHashes[respFileMsg.Breakpoint.Index] == respFileMsg.Breakpoint.Hash {
		startIndex = respFileMsg.Breakpoint.Index + 1
	}
	if err := this.taskMgr.UpdateTaskProgress(taskId, peerWalletAddr, startIndex); err != nil {
		return err
	}
	log.Debugf("task %s, start at %d", taskId, startIndex)
	if stop, err := this.taskMgr.IsTaskStop(taskId); stop || err != nil {
		log.Debugf("stop handle request because task is stop: %t", stop)
		return nil
	}
	blocks := make([]*block.Block, 0, common.MAX_SEND_BLOCK_COUNT)
	blockInfos := make([]*store.BlockInfo, 0, common.MAX_SEND_BLOCK_COUNT)
	sending := false
	sendLock := new(sync.Mutex)

	limitWg := new(sync.WaitGroup)
	stopTickerCh := make(chan struct{})
	defer close(stopTickerCh)
	go func() {
		speedLimitTicker := time.NewTicker(time.Second)
		defer speedLimitTicker.Stop()
		for {
			select {
			case <-speedLimitTicker.C:
				sendLock.Lock()
				if sending {
					limitWg.Done()
					sending = false
					log.Debugf("sending is 1, set 0")
				}
				sendLock.Unlock()
			case <-stopTickerCh:
				log.Debugf("stop check limit ticker")
				return
			}
		}
	}()
	for index, hash := range blockHashes {
		if uint64(index) < startIndex {
			continue
		}
		blockMsgData := getMsgData(hash, uint64(index))
		if blockMsgData == nil {
			return dspErr.New(dspErr.DISPATCH_FILE_ERROR, "no block msg data to send")
		}
		isStored := this.taskMgr.IsBlockUploaded(taskId, hash, peerWalletAddr, uint64(index))
		if !isStored {
			bi := &store.BlockInfo{
				Hash:       hash,
				Index:      uint64(index),
				DataSize:   blockMsgData.dataLen,
				DataOffset: blockMsgData.offset,
			}
			blockInfos = append(blockInfos, bi)
		}
		b := &block.Block{
			SessionId: sessionId,
			Index:     uint64(index),
			FileHash:  fileHashStr,
			Hash:      hash,
			Data:      blockMsgData.blockData,
			Tag:       blockMsgData.tag,
			Operation: netcomm.BLOCK_OP_NONE,
			Offset:    int64(blockMsgData.offset),
		}
		blocks = append(blocks, b)
		if index != len(blockHashes)-1 && len(blocks) < common.MAX_SEND_BLOCK_COUNT {
			continue
		}
		log.Debugf("will send blocks %d", len(blocks))
		this.taskMgr.ActiveUploadTaskPeer(peerWalletAddr)
		sendLock.Lock()
		if !sending {
			limitWg.Add(1)
			sending = true
			log.Debugf("sending is 0, add 1")
		}
		sendLock.Unlock()
		if err := this.sendBlockFlightMsg(taskId, fileHashStr, peerWalletAddr, blocks); err != nil {
			limitWg.Wait()
			return err
		}
		limitWg.Wait()
		if cleanMsgData != nil {
			cleanMsgData(blocks)
		}
		// update progress
		this.taskMgr.SetBlocksUploaded(taskId, peerWalletAddr, blockInfos)
		this.taskMgr.EmitProgress(taskId, task.TaskUploadFileTransferBlocks)
		this.taskMgr.ActiveUploadTaskPeer(peerWalletAddr)
		blocks = blocks[:0]
		blockInfos = blockInfos[:0]
		if stop, err := this.taskMgr.IsTaskStop(taskId); stop || err != nil {
			log.Debugf("stop handle request because task is stop: %t", stop)
			return nil
		}
		fileOwner, _ := this.taskMgr.GetFileOwner(taskId)
		if !this.taskMgr.IsFileUploaded(taskId, fileOwner != this.WalletAddress()) {
			continue
		}
		log.Debugf("task %s send blocks done", taskId)
	}
	return nil
}

// sendBlockFlightMsg. send block flight msg to peer
func (this *Dsp) sendBlockFlightMsg(taskId, fileHashStr, peerAddr string, blocks []*block.Block) error {
	// send block
	if len(blocks) == 0 {
		log.Warnf("task %s has no block to send", taskId)
		return nil
	}
	flights := &block.BlockFlights{
		TimeStamp: time.Now().UnixNano(),
		Blocks:    blocks,
	}
	msg := message.NewBlockFlightsMsg(flights)
	sendLogMsg := fmt.Sprintf("file: %s, block %s-%s, index:%d-%d to %s with msg id %s",
		fileHashStr, blocks[0].Hash, blocks[len(blocks)-1].Hash,
		blocks[0].Index, blocks[len(blocks)-1].Index, peerAddr, msg.MessageId)
	sendingTime := time.Now().Unix()
	log.Debugf("sending %s", sendLogMsg)
	if err := client.P2pSend(peerAddr, msg.MessageId, msg.ToProtoMsg()); err != nil {
		log.Errorf("%v, err %s", sendLogMsg, err)
		return err
	}
	log.Debugf("sending %s success\n, used %ds", sendLogMsg, time.Now().Unix()-sendingTime)
	return nil
}

// getFileUploadResult. get file upload result
func (this *Dsp) getFileUploadResult(taskInfo *store.TaskInfo, opt *fs.UploadOption) (*common.UploadResult, error) {
	saveLink := this.getSaveLink(taskInfo, opt)
	dnsRegTx, dnsBindTx, err := this.checkProveAndCommitUrlTxs(taskInfo.Id, taskInfo.FileHash, taskInfo.RegisterDNSTx,
		taskInfo.BindDNSTx, saveLink, opt)
	if err != nil {
		return nil, err
	}
	return &common.UploadResult{
		Tx:             taskInfo.StoreTx,
		FileHash:       taskInfo.FileHash,
		Url:            string(opt.DnsURL),
		Link:           saveLink,
		RegisterDnsTx:  dnsRegTx,
		BindDnsTx:      dnsBindTx,
		AddWhiteListTx: taskInfo.WhitelistTx,
	}, nil
}

// getSaveLink. get save link by task info and upload options
func (this *Dsp) getSaveLink(taskInfo *store.TaskInfo, opt *fs.UploadOption) string {
	link := &utils.URLLink{
		FileHashStr: taskInfo.FileHash,
		FileName:    taskInfo.FileName,
		FileOwner:   taskInfo.FileOwner,
		FileSize:    opt.FileSize,
		BlockNum:    uint64(taskInfo.TotalBlockCount),
		Trackers:    this.dns.TrackerUrls,
		BlocksRoot:  taskInfo.BlocksRoot,
	}
	return link.String()
}

// checkProveAndCommitUrlTxs. check file has been proved, if proved, commit url txs if not register url before
func (this *Dsp) checkProveAndCommitUrlTxs(taskId, fileHashStr, dnsRegTx, dnsBindTx, saveLink string,
	opt *fs.UploadOption) (string, string, error) {
	this.taskMgr.EmitProgress(taskId, task.TaskUploadFileWaitForPDPProve)
	proved := this.checkFileBeProved(fileHashStr)
	log.Debugf("checking file: %s proved done %t", fileHashStr, proved)
	if !proved {
		return "", "", dspErr.New(dspErr.FILE_UPLOADED_CHECK_PDP_FAILED,
			"file has sent, but no enough prove is finished")
	}
	if pause, err := this.checkIfPause(taskId, fileHashStr); err != nil || pause {
		return "", "", err
	}
	this.taskMgr.EmitProgress(taskId, task.TaskUploadFileWaitForPDPProveDone)
	if len(dnsRegTx) == 0 || len(dnsBindTx) == 0 {
		dnsRegTx, dnsBindTx = this.registerUrls(taskId, fileHashStr, saveLink, opt)
	}

	if err := this.taskMgr.SetTaskInfoWithOptions(taskId,
		task.Url(string(opt.DnsURL)),
		task.RegUrlTx(dnsRegTx),
		task.BindUrlTx(dnsBindTx)); err != nil {
		return "", "", err
	}
	return dnsRegTx, dnsBindTx, nil
}

// dispatchBlocks. dispatch blocks to other primary nodes
func (this *Dsp) dispatchBlocks(taskId, referId, fileHashStr string) error {
	log.Debugf("task %s dispatch blocks, refer to %s, file %s", taskId, referId, fileHashStr)
	fileInfo, err := this.chain.GetFileInfo(fileHashStr)
	if err != nil {
		return dspErr.NewWithError(dspErr.FILEINFO_NOT_EXIST, err)
	}
	if len(fileInfo.PrimaryNodes.AddrList) == 0 {
		return dspErr.New(dspErr.INTERNAL_ERROR, "primary node is nil")
	}
	if fileInfo.PrimaryNodes.AddrList[0].ToBase58() != this.WalletAddress() {
		log.Debugf("no need to dispatch file %s, because i am not the master node", fileHashStr)
		return nil
	}
	details, err := this.chain.GetFileProveDetails(fileHashStr)
	if err != nil || details == nil {
		log.Errorf("dispatch file, but file prove detail not exist %s, err %s", fileHashStr, err)
		return dspErr.New(dspErr.INTERNAL_ERROR,
			"dispatch file, but file prove detail not exist %s, err %s", fileHashStr, err)
	}
	nodesToDispatch := make([]*common.NodeInfo, 0)
	provedNodes := make(map[string]struct{}, 0)
	for _, d := range details.ProveDetails {
		log.Debugf("wallet %v, node %v, prove times %d", d.WalletAddr.ToBase58(),
			string(d.NodeAddr), d.ProveTimes)
		if d.ProveTimes > 0 {
			provedNodes[d.WalletAddr.ToBase58()] = struct{}{}
		}
	}
	hostAddrs, err := this.chain.GetNodeHostAddrListByWallets(fileInfo.PrimaryNodes.AddrList)
	if err != nil {
		return dspErr.New(dspErr.INTERNAL_ERROR,
			"dispatch file, get node host addr failed %s, err %s", fileHashStr, err)
	}
	for idx, n := range fileInfo.PrimaryNodes.AddrList {
		if n.ToBase58() == this.WalletAddress() {
			continue
		}
		if _, ok := provedNodes[n.ToBase58()]; ok {
			continue
		}
		if len(taskId) > 0 {
			doingOrDone, _ := this.taskMgr.IsNodeTaskDoingOrDone(taskId, n.ToBase58())
			log.Debugf("upload task %s transfer to %s is doing or done %t",
				taskId, n.ToBase58(), doingOrDone)
			if doingOrDone {
				continue
			}
		}
		nodesToDispatch = append(nodesToDispatch, &common.NodeInfo{
			HostAddr:   hostAddrs[idx],
			WalletAddr: n.ToBase58(),
		})
	}
	log.Debugf("nodes to dispatch %v", nodesToDispatch)
	if len(nodesToDispatch) == 0 {
		log.Debugf("all nodes have dispatched, set %s file %s done", taskId, fileHashStr)
		if len(taskId) > 0 {
			this.taskMgr.SetTaskState(taskId, store.TaskStateDone)
		}
		return nil
	}
	refTaskInfo, err := this.taskMgr.GetTaskInfoCopy(referId)
	if err != nil {
		return err
	}
	blockHashes := this.taskMgr.FileBlockHashes(referId)
	totalCount := len(blockHashes)
	if len(taskId) == 0 {
		var err error
		taskId, err = this.taskMgr.NewTask("", store.TaskTypeUpload)
		log.Debugf("new task id %s upload file %s, total count %d", taskId, fileHashStr, totalCount)
		if err != nil {
			return err
		}
		if err := this.taskMgr.SetTaskInfoWithOptions(taskId,
			task.FileHash(fileHashStr),
			task.BlocksRoot(refTaskInfo.BlocksRoot),
			task.Walletaddr(this.WalletAddress()),
			task.Prefix(string(refTaskInfo.Prefix)),
			task.StoreTx(refTaskInfo.StoreTx),
			task.StoreTxHeight(refTaskInfo.StoreTxHeight),
			task.ReferId(referId),
			task.CopyNum(uint32(fileInfo.CopyNum)),
			task.FileOwner(fileInfo.FileOwner.ToBase58()),
			task.TotalBlockCnt(uint64(totalCount))); err != nil {
			return err
		}
		if err := this.taskMgr.BindTaskId(taskId); err != nil {
			return err
		}
	}
	getMsgData := func(hash string, index uint64) *blockMsgData {
		block := this.fs.GetBlock(hash)
		blockData := this.fs.BlockDataOfAny(block)
		tag, err := this.fs.GetTag(hash, fileHashStr, uint64(index))
		if err != nil {
			log.Errorf("get tag of file %s, block %s, index %d err %v",
				fileHashStr, hash, index, err)
			return nil
		}
		offset, err := this.taskMgr.GetBlockOffset(referId, hash, index)
		if err != nil {
			log.Errorf("get block offset err %v, task id %s, file %s, block %s, index %d",
				err, referId, fileHashStr, hash, index)
			return nil
		}
		return &blockMsgData{
			blockData: blockData,
			tag:       tag,
			offset:    offset,
		}
	}
	for _, node := range nodesToDispatch {
		go func(peerWalletAddr, peerHostAddr string) {
			client.P2pAppendAddrForHealthCheck(peerWalletAddr, client.P2pNetTypeDsp)
			if err := client.P2pConnect(peerHostAddr); err != nil {
				log.Errorf("dispatch task %s connect to peer failed %s", taskId, err)
				return
			}
			sessionId, _ := this.taskMgr.GetSessionId(taskId, "")
			msg := message.NewFileMsg(fileHashStr, netcomm.FILE_OP_FETCH_ASK,
				message.WithSessionId(sessionId),
				message.WithWalletAddress(this.chain.WalletAddress()),
				message.WithSign(this.chain.CurrentAccount()),
			)
			log.Debugf("task %s, file %s send file ask msg to %s", taskId, fileHashStr, peerWalletAddr)
			resp, err := client.P2pSendAndWaitReply(peerWalletAddr, msg.MessageId, msg.ToProtoMsg())
			if err != nil {
				log.Errorf("send file ask msg err %s", err)
				return
			}
			p2pMsg := message.ReadMessage(resp)
			if p2pMsg.Error != nil && p2pMsg.Error.Code != dspErr.SUCCESS {
				log.Errorf("get file fetch_ack msg err %s", p2pMsg.Error.Message)
				return
			}
			// compare chain info
			fileMsg := p2pMsg.Payload.(*file.File)
			if fileMsg.ChainInfo.Height < refTaskInfo.StoreTxHeight {
				log.Debugf("task %s dispatch interrupt, height %d less than %d",
					taskId, fileMsg.ChainInfo.Height, refTaskInfo.StoreTxHeight)
				return
			}
			log.Debugf("dispatch send file taskId %s, fileHashStr %s, peerAddr %s, prefix %s, storeTx %s, "+
				"totalCount %d, storeTxHeight %d", taskId, fileHashStr, peerWalletAddr, string(refTaskInfo.Prefix),
				refTaskInfo.StoreTx, uint32(totalCount), refTaskInfo.StoreTxHeight)
			this.taskMgr.UpdateTaskNodeState(taskId, peerWalletAddr, store.TaskStateDoing)
			if err := this.sendBlocksToPeer(taskId, peerWalletAddr, blockHashes,
				getMsgData, nil); err != nil {
				log.Errorf("send block err %s", err)
				this.taskMgr.EmitResult(taskId, nil, dspErr.NewWithError(dspErr.DISPATCH_FILE_ERROR, err))
				this.taskMgr.UpdateTaskNodeState(taskId, peerWalletAddr, store.TaskStateFailed)
				client.P2pRemoveAddrFormHealthCheck(peerWalletAddr, client.P2pNetTypeDsp)
				return
			}
			this.taskMgr.UpdateTaskNodeState(taskId, peerWalletAddr, store.TaskStateDone)
			client.P2pRemoveAddrFormHealthCheck(peerWalletAddr, client.P2pNetTypeDsp)
		}(node.WalletAddr, node.HostAddr)
	}
	return nil
}

func (this *Dsp) startDispatchFileService() {
	ticker := time.NewTicker(time.Duration(common.DISPATCH_FILE_DURATION) * time.Second)
	for {
		<-ticker.C
		if !this.Running() {
			log.Debugf("stop fetch file service")
			ticker.Stop()
			return
		}
		tasks, err := this.taskMgr.GetUnDispatchTaskInfos(this.WalletAddress())
		if err != nil {
			log.Errorf("get no dispatched task failed %s", err)
			continue
		}
		for _, t := range tasks {
			if len(t.ReferId) == 0 {
				continue
			}
			if this.taskMgr.IsFileUploaded(t.Id, true) {
				// update task state
				this.taskMgr.SetTaskState(t.Id, store.TaskStateDone)
				continue
			}
			log.Debugf("get task %s to dispatch %s", t.Id, t.FileHash)
			go this.dispatchBlocks(t.Id, t.ReferId, t.FileHash)
		}
	}
}

// getMasterNodeHostAddr. get master node host address
func (this *Dsp) getMasterNodeHostAddr(walletAddr string) string {
	hostAddr, _ := client.P2pGetHostAddrFromWalletAddr(walletAddr, client.P2pNetTypeDsp)
	if len(hostAddr) != 0 {
		return hostAddr
	}
	hosts, err := this.chain.GetNodeHostAddrListByWallets(utils.Base58ToWalletAddrs([]string{walletAddr}))
	if err != nil || len(hosts) == 0 {
		return ""
	}
	return hosts[0]
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

func keyOfBlockHashAndIndex(hash string, index uint64) string {
	return fmt.Sprintf("%s-%d", hash, index)
}

func fsWhiteListToWhiteList(whiteList fs.WhiteList) []*store.WhiteList {
	wh := make([]*store.WhiteList, 0)
	for _, r := range whiteList.List {
		wh = append(wh, &store.WhiteList{
			Address:     r.Addr.ToBase58(),
			StartHeight: r.BaseHeight,
			EndHeight:   r.ExpireHeight,
		})
	}
	return wh
}

func whiteListToFsWhiteList(whiteList []*store.WhiteList) fs.WhiteList {
	wh := fs.WhiteList{
		Num: uint64(len(whiteList)),
	}
	wh.List = make([]fs.Rule, 0)
	for _, r := range whiteList {
		addr, _ := chainCom.AddressFromBase58(r.Address)
		wh.List = append(wh.List, fs.Rule{
			Addr:         addr,
			BaseHeight:   r.StartHeight,
			ExpireHeight: r.EndHeight,
		})
	}
	return wh
}

func getFileSizeWithBlockCount(cnt uint64) uint64 {
	size := common.CHUNK_SIZE * cnt / 1024
	if size == 0 {
		return 1
	}
	return size
}
