package task

import (
	"fmt"

	sdkErr "github.com/saveio/dsp-go-sdk/error"
	"github.com/saveio/dsp-go-sdk/store"
	"github.com/saveio/themis/common/log"
)

// SetFileInfoWithOptions
func (this *TaskMgr) SetTaskInfoWithOptions(taskId string, opts ...InfoOption) error {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		return sdkErr.New(sdkErr.SET_FILEINFO_DB_ERROR, fmt.Sprintf("task: %s, not exist", taskId))
	}
	err := v.SetInfoWithOptions(opts...)
	if err != nil {
		return sdkErr.New(sdkErr.SET_FILEINFO_DB_ERROR, err.Error())
	}
	return nil
}

// SetFileHash. set file hash of task
func (this *TaskMgr) SetFileHash(taskId, fileHash string) error {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		return sdkErr.New(sdkErr.SET_FILEINFO_DB_ERROR, fmt.Sprintf("task: %s, not exist", taskId))
	}
	err := v.SetFileHash(fileHash)
	if err != nil {
		return sdkErr.New(sdkErr.SET_FILEINFO_DB_ERROR, err.Error())
	}
	return nil
}

func (this *TaskMgr) SetSessionId(taskId, peerWalletAddr, id string) error {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		return sdkErr.New(sdkErr.SET_FILEINFO_DB_ERROR, fmt.Sprintf("task: %s, not exist", taskId))
	}
	v.SetSessionId(peerWalletAddr, id)
	return nil
}

// SetFileName. set file name of task
func (this *TaskMgr) SetFileName(taskId, fileName string) error {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		return fmt.Errorf("[TaskMgr SetFileName] task not found: %s", taskId)
	}
	err := v.SetFileName(fileName)
	if err != nil {
		return sdkErr.New(sdkErr.SET_FILEINFO_DB_ERROR, err.Error())
	}
	return nil
}
func (this *TaskMgr) SetTotalBlockCount(taskId string, totalBlockCount uint64) error {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		return fmt.Errorf("[TaskMgr SetFileName] task not found: %s", taskId)
	}
	err := v.SetTotalBlockCnt(totalBlockCount)
	if err != nil {
		return sdkErr.New(sdkErr.SET_FILEINFO_DB_ERROR, err.Error())
	}
	return nil
}

// SetPrefix. set file prefix of task
func (this *TaskMgr) SetPrefix(taskId string, prefix string) error {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		return fmt.Errorf("[TaskMgr SetFileName] task not found: %s", taskId)
	}
	err := v.SetPrefix(prefix)
	if err != nil {
		return sdkErr.New(sdkErr.SET_FILEINFO_DB_ERROR, err.Error())
	}
	return nil
}

func (this *TaskMgr) SetWalletAddr(taskId, walletAddr string) error {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		return fmt.Errorf("[TaskMgr SetWalletAddr] task not found: %s", taskId)
	}
	err := v.SetWalletaddr(walletAddr)
	if err != nil {
		return sdkErr.New(sdkErr.SET_FILEINFO_DB_ERROR, err.Error())
	}
	return nil
}

func (this *TaskMgr) SetCopyNum(taskId string, copyNum uint32) error {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		return fmt.Errorf("[TaskMgr SetCopyNum] task not found: %s", taskId)
	}
	err := v.SetCopyNum(copyNum)
	if err != nil {
		return sdkErr.New(sdkErr.SET_FILEINFO_DB_ERROR, err.Error())
	}
	return nil
}

func (this *TaskMgr) SetOnlyBlock(taskId string, only bool) error {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		return fmt.Errorf("[TaskMgr SetFileName] task not found: %s", taskId)
	}
	err := v.SetOnlyBlock(only)
	if err != nil {
		return sdkErr.New(sdkErr.SET_FILEINFO_DB_ERROR, err.Error())
	}
	return nil
}

func (this *TaskMgr) SetTaskState(taskId string, state store.TaskState) error {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		return sdkErr.New(sdkErr.SET_FILEINFO_DB_ERROR, fmt.Sprintf("task: %s, not exist", taskId))
	}
	log.Debugf("set task state: %s %d", taskId, state)
	err := v.SetTaskState(state)
	if err != nil {
		return sdkErr.New(sdkErr.SET_FILEINFO_DB_ERROR, err.Error())
	}
	return nil
}

func (this *TaskMgr) SetWhitelistTx(taskId string, whitelistTx string) error {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		return sdkErr.New(sdkErr.SET_FILEINFO_DB_ERROR, fmt.Sprintf("task: %s, not exist", taskId))
	}
	err := v.SetWhiteListTx(whitelistTx)
	if err != nil {
		return sdkErr.New(sdkErr.SET_FILEINFO_DB_ERROR, err.Error())
	}
	return nil
}

func (this *TaskMgr) SetFileOwner(taskId, owner string) error {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		return sdkErr.New(sdkErr.SET_FILEINFO_DB_ERROR, fmt.Sprintf("task: %s, not exist", taskId))
	}
	err := v.SetFileOwner(owner)
	if err != nil {
		return sdkErr.New(sdkErr.SET_FILEINFO_DB_ERROR, err.Error())
	}
	return nil
}

func (this *TaskMgr) SetPrivateKey(taskId string, value []byte) error {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		return sdkErr.New(sdkErr.SET_FILEINFO_DB_ERROR, fmt.Sprintf("task: %s, not exist", taskId))
	}
	err := v.SetPrivateKey(value)
	if err != nil {
		return sdkErr.New(sdkErr.SET_FILEINFO_DB_ERROR, err.Error())
	}
	return nil
}

func (this *TaskMgr) SetStoreTx(taskId, tx string) error {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		return sdkErr.New(sdkErr.SET_FILEINFO_DB_ERROR, fmt.Sprintf("task: %s, not exist", taskId))
	}
	err := v.SetStoreTx(tx)
	if err != nil {
		return sdkErr.New(sdkErr.SET_FILEINFO_DB_ERROR, err.Error())
	}
	return nil
}

func (this *TaskMgr) SetRegUrlTx(taskId, tx string) error {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		return sdkErr.New(sdkErr.SET_FILEINFO_DB_ERROR, fmt.Sprintf("task: %s, not exist", taskId))
	}
	err := v.SetRegUrlTx(tx)
	if err != nil {
		return sdkErr.New(sdkErr.SET_FILEINFO_DB_ERROR, err.Error())
	}
	return nil
}

func (this *TaskMgr) SetBindUrlTx(taskId, tx string) error {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		return sdkErr.New(sdkErr.SET_FILEINFO_DB_ERROR, fmt.Sprintf("task: %s, not exist", taskId))
	}
	err := v.SetBindUrlTx(tx)
	if err != nil {
		return sdkErr.New(sdkErr.SET_FILEINFO_DB_ERROR, err.Error())
	}
	return nil
}

func (this *TaskMgr) SetUrl(taskId, url string) error {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		return sdkErr.New(sdkErr.SET_FILEINFO_DB_ERROR, fmt.Sprintf("task: %s, not exist", taskId))
	}
	err := v.SetUrl(url)
	if err != nil {
		return sdkErr.New(sdkErr.SET_FILEINFO_DB_ERROR, err.Error())
	}
	return nil
}

func (this *TaskMgr) SetFilePath(taskId, path string) error {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		return sdkErr.New(sdkErr.SET_FILEINFO_DB_ERROR, fmt.Sprintf("task: %s, not exist", taskId))
	}
	err := v.SetFilePath(path)
	if err != nil {
		return sdkErr.New(sdkErr.SET_FILEINFO_DB_ERROR, err.Error())
	}
	return nil
}

func (this *TaskMgr) SetSimpleCheckSum(taskId, checksum string) error {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		return sdkErr.New(sdkErr.SET_FILEINFO_DB_ERROR, fmt.Sprintf("task: %s, not exist", taskId))
	}
	err := v.SetSimpleCheckSum(checksum)
	if err != nil {
		return sdkErr.New(sdkErr.SET_FILEINFO_DB_ERROR, err.Error())
	}
	return nil
}

func (this *TaskMgr) BatchCommit(taskId string) error {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		return sdkErr.New(sdkErr.SET_FILEINFO_DB_ERROR, fmt.Sprintf("task: %s, not exist", taskId))
	}
	err := v.BatchCommit()
	if err != nil {
		return sdkErr.New(sdkErr.SET_FILEINFO_DB_ERROR, err.Error())
	}
	return nil
}

func (this *TaskMgr) SetBlocksUploaded(taskId, nodeAddr string, blockInfos []*store.BlockInfo) error {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		return sdkErr.New(sdkErr.SET_FILEINFO_DB_ERROR, fmt.Sprintf("task: %s, not exist", taskId))
	}
	err := v.SetBlocksUploaded(taskId, nodeAddr, blockInfos)
	if err != nil {
		return sdkErr.New(sdkErr.SET_FILEINFO_DB_ERROR, err.Error())
	}
	return nil
}

// func (this *TaskMgr) SetFileUploadOptions(taskId string, opt *fs.UploadOption) error {
// 	if err := this.db.SetFileUploadOptions(taskId, opt); err != nil {
// 		return sdkErr.New(sdkErr.SET_FILEINFO_DB_ERROR, err.Error())
// 	}
// 	return nil
// }

func (this *TaskMgr) SetUploadProgressDone(taskId, nodeAddr string) error {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		return sdkErr.New(sdkErr.SET_FILEINFO_DB_ERROR, fmt.Sprintf("task: %s, not exist", taskId))
	}
	if v.State() == store.TaskStateCancel || v.State() == store.TaskStateFailed {
		return nil
	}
	err := v.SetUploadProgressDone(taskId, nodeAddr)
	if err != nil {
		return sdkErr.New(sdkErr.SET_FILEINFO_DB_ERROR, err.Error())
	}
	return nil
}

func (this *TaskMgr) UpdateTaskNodeState(taskId, nodeAddr string, state store.TaskState) error {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		return sdkErr.New(sdkErr.SET_FILEINFO_DB_ERROR, fmt.Sprintf("task: %s, not exist", taskId))
	}
	if v.State() == store.TaskStateCancel || v.State() == store.TaskStateFailed {
		return nil
	}
	err := this.db.UpdateTaskProgressState(taskId, nodeAddr, state)
	if err != nil {
		return sdkErr.New(sdkErr.SET_FILEINFO_DB_ERROR, err.Error())
	}
	return nil
}

func (this *TaskMgr) UpdateTaskProgress(taskId, nodeAddr string, progress uint64) error {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		return sdkErr.New(sdkErr.SET_FILEINFO_DB_ERROR, fmt.Sprintf("task: %s, not exist", taskId))
	}
	if v.State() == store.TaskStateCancel || v.State() == store.TaskStateFailed {
		return nil
	}
	err := this.db.UpdateTaskProgress(taskId, nodeAddr, progress)
	if err != nil {
		return sdkErr.New(sdkErr.SET_FILEINFO_DB_ERROR, err.Error())
	}
	return nil
}

func (this *TaskMgr) IsNodeTaskDoingOrDone(taskId, nodeAddr string) (bool, error) {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		return false, sdkErr.New(sdkErr.SET_FILEINFO_DB_ERROR, fmt.Sprintf("task: %s, not exist", taskId))
	}
	if v.State() == store.TaskStateCancel || v.State() == store.TaskStateFailed {
		return false, nil
	}
	doingOrdone, err := this.db.IsNodeTaskDoingOrDone(taskId, nodeAddr)
	if err != nil {
		return false, sdkErr.New(sdkErr.SET_FILEINFO_DB_ERROR, err.Error())
	}
	return doingOrdone, nil
}

func (this *TaskMgr) SetBlockDownloaded(taskId, blockHashStr, nodeAddr string, index uint64, offset int64, links []string) error {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		return sdkErr.New(sdkErr.SET_FILEINFO_DB_ERROR, fmt.Sprintf("task: %s, not exist", taskId))
	}
	err := v.SetBlockDownloaded(taskId, blockHashStr, nodeAddr, index, offset, links)
	if err != nil {
		return sdkErr.New(sdkErr.SET_FILEINFO_DB_ERROR, err.Error())
	}
	return nil
}

func (this *TaskMgr) AddShareTo(id, walletAddress string) error {
	err := this.db.AddShareTo(id, walletAddress)
	if err != nil {
		return sdkErr.New(sdkErr.SET_FILEINFO_DB_ERROR, err.Error())
	}
	return nil
}

// func (this *TaskMgr) SetFileDownloadOptions(id string, opt *common.DownloadOption) error {
// 	err := this.db.SetFileDownloadOptions(id, opt)
// 	if err != nil {
// 		return sdkErr.New(sdkErr.SET_FILEINFO_DB_ERROR, err.Error())
// 	}
// 	return nil
// }

func (this *TaskMgr) AddFileSession(fileInfoId string, sessionId, walletAddress, hostAddress string,
	asset uint32, unitPrice uint64) error {
	this.SetSessionId(fileInfoId, walletAddress, sessionId)
	err := this.db.AddFileSession(fileInfoId, sessionId, walletAddress, hostAddress, asset, unitPrice)
	if err != nil {
		return sdkErr.New(sdkErr.SET_FILEINFO_DB_ERROR, err.Error())
	}
	return nil
}

func (this *TaskMgr) AddFileUnpaid(id, walletAddress string, paymentId, asset int32, amount uint64) error {
	err := this.db.AddFileUnpaid(id, walletAddress, paymentId, asset, amount)
	if err != nil {
		return sdkErr.New(sdkErr.SET_FILEINFO_DB_ERROR, err.Error())
	}
	return nil
}

func (this *TaskMgr) AddFileBlockHashes(id string, blocks []string) error {
	err := this.db.AddFileBlockHashes(id, blocks)
	if err != nil {
		return sdkErr.New(sdkErr.SET_FILEINFO_DB_ERROR, err.Error())
	}
	return nil
}

func (this *TaskMgr) DeleteFileUnpaid(id, walletAddress string, paymentId, asset int32, amount uint64) error {
	err := this.db.DeleteFileUnpaid(id, walletAddress, paymentId, asset, amount)
	if err != nil {
		return sdkErr.New(sdkErr.SET_FILEINFO_DB_ERROR, err.Error())
	}
	return nil
}

func (this *TaskMgr) HideTaskIds(ids []string) error {
	err := this.db.HideTaskIds(ids)
	if err != nil {
		return sdkErr.New(sdkErr.SET_FILEINFO_DB_ERROR, err.Error())
	}
	return nil
}

func (this *TaskMgr) SetTaskNetPhase(taskId string, addrs []string, phase int) error {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		return sdkErr.New(sdkErr.SET_FILEINFO_DB_ERROR, fmt.Sprintf("task: %s, not exist", taskId))
	}
	for _, addr := range addrs {
		v.SetWorkerNetPhase(addr, phase)
	}
	return nil
}

func (this *TaskMgr) RemoveUnSlavedTasks(id string) error {
	this.lock.RLock()
	defer this.lock.RUnlock()
	err := this.db.RemoveFromUnSalvedList(nil, id, store.TaskTypeUpload)
	if err != nil {
		return sdkErr.New(sdkErr.SET_FILEINFO_DB_ERROR, err.Error())
	}
	return nil
}

func (this *TaskMgr) UpdateTaskPeerProgress(id, nodeAddr string, count uint64) error {
	err := this.db.UpdateTaskPeerProgress(id, nodeAddr, count)
	if err != nil {
		return sdkErr.New(sdkErr.SET_FILEINFO_DB_ERROR, err.Error())
	}
	return nil
}

func (this *TaskMgr) UpdateTaskPeerSpeed(id, nodeAddr string, speed uint64) error {
	err := this.db.UpdateTaskPeerSpeed(id, nodeAddr, speed)
	if err != nil {
		return sdkErr.New(sdkErr.SET_FILEINFO_DB_ERROR, err.Error())
	}
	return nil
}
