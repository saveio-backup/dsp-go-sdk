package task

import (
	"fmt"

	sdkErr "github.com/saveio/dsp-go-sdk/error"
	"github.com/saveio/dsp-go-sdk/store"
	"github.com/saveio/themis/common/log"
)

// getter list for task info

func (this *TaskMgr) GetSessionId(taskId, peerWalletAddr string) (string, error) {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		return "", sdkErr.New(sdkErr.GET_FILEINFO_FROM_DB_ERROR, fmt.Sprintf("task: %s, not exist", taskId))
	}
	switch v.GetTaskType() {
	case store.TaskTypeUpload:
		// upload or share task, local node is a server
		return taskId, nil
	case store.TaskTypeShare:
		return taskId, nil
	case store.TaskTypeDownload:
		return v.GetSessionId(peerWalletAddr), nil
	}
	return "", sdkErr.New(sdkErr.GET_FILEINFO_FROM_DB_ERROR, fmt.Sprintf("unknown task type %d", v.GetTaskType()))
}

func (this *TaskMgr) GetTaskFileName(taskId string) (string, error) {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		return "", sdkErr.New(sdkErr.GET_FILEINFO_FROM_DB_ERROR, fmt.Sprintf("task: %s, not exist", taskId))
	}
	return v.GetFileName(), nil
}

// TaskType.
func (this *TaskMgr) GetTaskType(taskId string) (store.TaskType, error) {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		return store.TaskTypeNone, sdkErr.New(sdkErr.GET_FILEINFO_FROM_DB_ERROR, fmt.Sprintf("task: %s, not exist", taskId))
	}
	return v.GetTaskType(), nil
}

func (this *TaskMgr) GetTaskFileHash(taskId string) (string, error) {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		return "", sdkErr.New(sdkErr.GET_FILEINFO_FROM_DB_ERROR, fmt.Sprintf("task: %s, not exist", taskId))
	}
	return v.GetFileHash(), nil

}

func (this *TaskMgr) OnlyBlock(taskId string) (bool, error) {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		return false, sdkErr.New(sdkErr.GET_FILEINFO_FROM_DB_ERROR, fmt.Sprintf("task: %s, not exist", taskId))
	}
	return v.GetOnlyblock(), nil
}

func (this *TaskMgr) GetTaskState(taskId string) (store.TaskState, error) {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		return store.TaskStateNone, sdkErr.New(sdkErr.GET_FILEINFO_FROM_DB_ERROR, fmt.Sprintf("task: %s, not exist", taskId))
	}
	return v.State(), nil
}

func (this *TaskMgr) GetTaskDetailState(taskId string) (TaskProgressState, error) {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		return 0, sdkErr.New(sdkErr.GET_FILEINFO_FROM_DB_ERROR, fmt.Sprintf("task: %s, not exist", taskId))
	}
	return v.TransferingState(), nil
}

func (this *TaskMgr) GetFileTotalBlockCount(taskId string) (uint64, error) {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		return 0, sdkErr.New(sdkErr.GET_FILEINFO_FROM_DB_ERROR, fmt.Sprintf("task: %s, not exist", taskId))
	}
	return v.GetTotalBlockCnt(), nil
}

func (this *TaskMgr) GetFilePrefix(taskId string) ([]byte, error) {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		return nil, sdkErr.New(sdkErr.GET_FILEINFO_FROM_DB_ERROR, fmt.Sprintf("task: %s, not exist", taskId))
	}
	return v.GetPrefix(), nil
}

func (this *TaskMgr) GetTaskUpdatedAt(taskId string) (uint64, error) {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		return 0, sdkErr.New(sdkErr.GET_FILEINFO_FROM_DB_ERROR, fmt.Sprintf("task: %s, not exist", taskId))
	}
	return v.GetUpdatedAt(), nil
}

func (this *TaskMgr) GetFileName(taskId string) (string, error) {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		return "", sdkErr.New(sdkErr.GET_FILEINFO_FROM_DB_ERROR, fmt.Sprintf("task: %s, not exist", taskId))
	}
	return v.GetFileName(), nil
}

func (this *TaskMgr) GetFileOwner(taskId string) (string, error) {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		return "", sdkErr.New(sdkErr.GET_FILEINFO_FROM_DB_ERROR, fmt.Sprintf("task: %s, not exist", taskId))
	}
	return v.GetFileOwner(), nil
}

func (this *TaskMgr) GetStoreTx(taskId string) (string, error) {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		return "", sdkErr.New(sdkErr.GET_FILEINFO_FROM_DB_ERROR, fmt.Sprintf("task: %s, not exist", taskId))
	}
	return v.GetStoreTx(), nil
}

func (this *TaskMgr) GetTaskInfoCopy(taskId string) (*store.TaskInfo, error) {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		return nil, sdkErr.New(sdkErr.GET_FILEINFO_FROM_DB_ERROR, fmt.Sprintf("task: %s, not exist", taskId))
	}
	return v.GetTaskInfoCopy(), nil
}

func (this *TaskMgr) GetRegUrlTx(taskId string) (string, error) {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		return "", sdkErr.New(sdkErr.GET_FILEINFO_FROM_DB_ERROR, fmt.Sprintf("task: %s, not exist", taskId))
	}
	return v.GetRegUrlTx(), nil
}

func (this *TaskMgr) GetBindUrlTx(taskId string) (string, error) {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		return "", sdkErr.New(sdkErr.GET_FILEINFO_FROM_DB_ERROR, fmt.Sprintf("task: %s, not exist", taskId))
	}
	return v.GetBindUrlTx(), nil
}

func (this *TaskMgr) GetWhitelistTx(taskId string) (string, error) {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		return "", sdkErr.New(sdkErr.GET_FILEINFO_FROM_DB_ERROR, fmt.Sprintf("task: %s, not exist", taskId))
	}
	return v.GetWhitelistTx(), nil
}

func (this *TaskMgr) GetFilePath(taskId string) (string, error) {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		return "", sdkErr.New(sdkErr.GET_FILEINFO_FROM_DB_ERROR, fmt.Sprintf("task: %s, not exist", taskId))
	}
	return v.GetFilePath(), nil
}

func (this *TaskMgr) GetProgressInfo(taskId string) *ProgressInfo {
	taskExist := this.TaskExist(taskId)
	if taskExist {
		v, ok := this.GetTaskById(taskId)
		if !ok {
			return nil
		}
		return v.GetProgressInfo()
	}
	v, _ := GetTaskFromDB(taskId, this.db)
	if v == nil {
		return nil
	}
	return v.GetProgressInfo()
}

func (this *TaskMgr) GetUndownloadedBlockInfo(id, rootBlockHash string) ([]*store.BlockInfo, error) {
	blks, err := this.db.GetUndownloadedBlockInfo(id, rootBlockHash)
	if err != nil {
		return blks, sdkErr.New(sdkErr.GET_FILEINFO_FROM_DB_ERROR, err.Error())
	}
	return blks, nil
}

func (this *TaskMgr) GetFileSessions(fileInfoId string) (map[string]*store.Session, error) {
	sessions, err := this.db.GetFileSessions(fileInfoId)
	if err != nil {
		return nil, sdkErr.New(sdkErr.GET_FILEINFO_FROM_DB_ERROR, err.Error())
	}
	return sessions, nil
}

func (this *TaskMgr) GetCurrentSetBlock(fileInfoId string) (string, uint64, error) {
	hash, index, err := this.db.GetCurrentSetBlock(fileInfoId)
	if err != nil {
		return "", 0, sdkErr.New(sdkErr.GET_FILEINFO_FROM_DB_ERROR, err.Error())
	}
	return hash, index, nil
}

func (this *TaskMgr) IsFileUploaded(id string, isDispatched bool) bool {
	return this.db.IsFileUploaded(id, isDispatched)
}

func (this *TaskMgr) IsFileDownloaded(id string) bool {
	return this.db.IsFileDownloaded(id)
}

func (this *TaskMgr) GetFileInfo(id string) (*store.TaskInfo, error) {
	info, err := this.db.GetTaskInfo(id)
	if err != nil {
		return nil, sdkErr.New(sdkErr.GET_FILEINFO_FROM_DB_ERROR, err.Error())
	}
	return info, nil
}

func (this *TaskMgr) IsBlockDownloaded(id, blockHashStr string, index uint64) bool {
	return this.db.IsBlockDownloaded(id, blockHashStr, index)
}

func (this *TaskMgr) GetUploadedBlockNodeList(id, blockHashStr string, index uint64) []string {
	if len(blockHashStr) == 0 {
		return nil
	}
	return this.db.GetUploadedBlockNodeList(id, blockHashStr, index)
}

func (this *TaskMgr) IsBlockUploaded(id, blockHashStr, nodeAddr string, index uint64) bool {
	return this.db.IsBlockUploaded(id, blockHashStr, nodeAddr, index)
}

func (this *TaskMgr) FileBlockHashes(id string) []string {
	return this.db.FileBlockHashes(id)
}

func (this *TaskMgr) IsFileInfoExist(id string) bool {
	return this.db.IsTaskInfoExist(id)
}

func (this *TaskMgr) AllDownloadFiles() ([]*store.TaskInfo, []string, error) {
	info, hashes, err := this.db.AllDownloadFiles()
	if err != nil {
		return nil, nil, sdkErr.New(sdkErr.GET_FILEINFO_FROM_DB_ERROR, err.Error())
	}
	return info, hashes, nil
}

func (this *TaskMgr) GetUnpaidAmount(id, walletAddress string, asset int32) (uint64, error) {
	amount, err := this.db.GetUnpaidAmount(id, walletAddress, asset)
	if err != nil {
		return 0, sdkErr.New(sdkErr.GET_FILEINFO_FROM_DB_ERROR, err.Error())
	}
	return amount, nil
}

func (this *TaskMgr) GetBlockOffset(id, blockHash string, index uint64) (uint64, error) {
	offset, err := this.db.GetBlockOffset(id, blockHash, index)
	if err != nil {
		return 0, sdkErr.New(sdkErr.GET_FILEINFO_FROM_DB_ERROR, err.Error())
	}
	return offset, nil
}

func (this *TaskMgr) GetShareTaskReferId(id string) (string, error) {
	info, err := this.db.GetTaskInfo(id)
	if err != nil {
		return "", sdkErr.New(sdkErr.GET_FILEINFO_FROM_DB_ERROR, err.Error())
	}
	if info == nil {
		return "", sdkErr.New(sdkErr.GET_FILEINFO_FROM_DB_ERROR, fmt.Sprintf("task %s not exist", id))
	}
	return info.ReferId, nil
}

func (this *TaskMgr) GetTaskIdList(offset, limit uint32, createdAt, createdAtEnd, updatedAt, updatedAtEnd uint64,
	ft store.TaskType, complete, reverse, includeFailed, ignoreHide bool) []string {
	return this.db.GetTaskIdList(offset, limit, createdAt, createdAtEnd, updatedAt, updatedAtEnd, ft,
		complete, reverse, includeFailed, ignoreHide)
}

// GetUnpaidPayments. get unpaid payments
func (this *TaskMgr) GetUnpaidPayments(id, payToAddress string, asset int32) (map[int32]*store.Payment, error) {
	m, err := this.db.GetUnpaidPayments(id, payToAddress, asset)
	if err != nil {
		return nil, sdkErr.New(sdkErr.GET_FILEINFO_FROM_DB_ERROR, err.Error())
	}
	return m, nil
}

func (this *TaskMgr) GetTaskIdWithPaymentId(paymentId int32) (string, error) {
	id, err := this.db.GetTaskIdWithPaymentId(paymentId)
	if err != nil {
		return "", sdkErr.New(sdkErr.GET_FILEINFO_FROM_DB_ERROR, err.Error())
	}
	return id, nil
}

// GetTaskPeerProgress. get task peer progress
func (this *TaskMgr) GetTaskPeerProgress(id, nodeAddr string) *store.FileProgress {
	return this.db.GetTaskPeerProgress(id, nodeAddr)
}

// GetDownloadedTaskId. get a downloaded task id with file hash str
func (this *TaskMgr) GetDownloadedTaskId(fileHashStr string) (string, error) {
	id, err := this.db.GetDownloadedTaskId(fileHashStr)
	if err != nil {
		return "", sdkErr.New(sdkErr.GET_FILEINFO_FROM_DB_ERROR, err.Error())
	}
	return id, nil
}

func (this *TaskMgr) GetUnDispatchTaskInfos(curWalletAddr string) ([]*store.TaskInfo, error) {
	tasks, err := this.db.GetUnDispatchTaskInfos(curWalletAddr)
	if err != nil {
		return nil, sdkErr.New(sdkErr.GET_FILEINFO_FROM_DB_ERROR, err.Error())
	}
	return tasks, nil
}

// ExistSameUploadingFile.
func (this *TaskMgr) ExistSameUploadFile(taskId, fileHashStr string) bool {
	this.lock.RLock()
	defer this.lock.RUnlock()
	exist, err := this.db.ExistSameUploadTaskInfo(taskId, fileHashStr)
	if err != nil {
		log.Errorf("exist same upload task info err %s", err)
		return false
	}
	return exist
}

func (this *TaskMgr) GetFileNameWithPath(filePath string) string {
	return this.db.GetFileNameWithPath(filePath)
}

func (this *TaskMgr) GetUploadTaskInfos() ([]*store.TaskInfo, error) {
	infos, err := this.db.GetUploadTaskInfos()
	if err != nil {
		return nil, sdkErr.New(sdkErr.GET_FILEINFO_FROM_DB_ERROR, err.Error())
	}
	return infos, nil
}
