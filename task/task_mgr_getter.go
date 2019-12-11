package task

import (
	"fmt"

	"github.com/saveio/dsp-go-sdk/common"
	sdkErr "github.com/saveio/dsp-go-sdk/error"
	"github.com/saveio/dsp-go-sdk/store"
	fs "github.com/saveio/themis/smartcontract/service/native/savefs"
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

func (this *TaskMgr) GetFilePrivateKey(taskId string) ([]byte, error) {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		return nil, sdkErr.New(sdkErr.GET_FILEINFO_FROM_DB_ERROR, fmt.Sprintf("task: %s, not exist", taskId))
	}
	return v.GetPrivateKey(), nil
}

func (this *TaskMgr) GetStoreTx(taskId string) (string, error) {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		return "", sdkErr.New(sdkErr.GET_FILEINFO_FROM_DB_ERROR, fmt.Sprintf("task: %s, not exist", taskId))
	}
	return v.GetStoreTx(), nil
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

func (this *TaskMgr) GetUndownloadedBlockInfo(id, rootBlockHash string) ([]string, map[string]uint32, error) {
	undownloadedList, undownloadedMap, err := this.db.GetUndownloadedBlockInfo(id, rootBlockHash)
	if err != nil {
		return nil, nil, sdkErr.New(sdkErr.GET_FILEINFO_FROM_DB_ERROR, err.Error())
	}
	return undownloadedList, undownloadedMap, nil
}

func (this *TaskMgr) GetFileSessions(fileInfoId string) (map[string]*store.Session, error) {
	sessions, err := this.db.GetFileSessions(fileInfoId)
	if err != nil {
		return nil, sdkErr.New(sdkErr.GET_FILEINFO_FROM_DB_ERROR, err.Error())
	}
	return sessions, nil
}

func (this *TaskMgr) GetFileUploadOptions(id string) (*fs.UploadOption, error) {
	opt, err := this.db.GetFileUploadOptions(id)
	if err != nil {
		return nil, sdkErr.New(sdkErr.GET_FILEINFO_FROM_DB_ERROR, err.Error())
	}
	return opt, nil
}

func (this *TaskMgr) GetFileDownloadOptions(id string) (*common.DownloadOption, error) {
	opt, err := this.db.GetFileDownloadOptions(id)
	if err != nil {
		return nil, sdkErr.New(sdkErr.GET_FILEINFO_FROM_DB_ERROR, err.Error())
	}
	return opt, nil
}

func (this *TaskMgr) GetCurrentSetBlock(fileInfoId string) (string, uint64, error) {
	hash, index, err := this.db.GetCurrentSetBlock(fileInfoId)
	if err != nil {
		return "", 0, sdkErr.New(sdkErr.GET_FILEINFO_FROM_DB_ERROR, err.Error())
	}
	return hash, index, nil
}

func (this *TaskMgr) IsFileUploaded(id string) bool {
	return this.db.IsFileUploaded(id)
}

func (this *TaskMgr) IsFileDownloaded(id string) bool {
	return this.db.IsFileDownloaded(id)
}

func (this *TaskMgr) GetFileInfo(id string) (*store.TaskInfo, error) {
	info, err := this.db.GetFileInfo(id)
	if err != nil {
		return nil, sdkErr.New(sdkErr.GET_FILEINFO_FROM_DB_ERROR, err.Error())
	}
	return info, nil
}

func (this *TaskMgr) IsBlockDownloaded(id, blockHashStr string, index uint32) bool {
	return this.db.IsBlockDownloaded(id, blockHashStr, index)
}

func (this *TaskMgr) GetUploadedBlockNodeList(id, blockHashStr string, index uint32) []string {
	if len(blockHashStr) == 0 {
		return nil
	}
	return this.db.GetUploadedBlockNodeList(id, blockHashStr, index)
}

func (this *TaskMgr) UploadedBlockCount(id string) uint64 {
	return this.db.UploadedBlockCount(id)
}

func (this *TaskMgr) IsBlockUploaded(id, blockHashStr, nodeAddr string, index uint32) bool {
	return this.db.IsBlockUploaded(id, blockHashStr, nodeAddr, index)
}

func (this *TaskMgr) FileBlockHashes(id string) []string {
	return this.db.FileBlockHashes(id)
}

func (this *TaskMgr) IsFileInfoExist(id string) bool {
	return this.db.IsFileInfoExist(id)
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

func (this *TaskMgr) GetBlockOffset(id, blockHash string, index uint32) (uint64, error) {
	offset, err := this.db.GetBlockOffset(id, blockHash, index)
	if err != nil {
		return 0, sdkErr.New(sdkErr.GET_FILEINFO_FROM_DB_ERROR, err.Error())
	}
	return offset, nil
}

func (this *TaskMgr) GetTaskIdList(offset, limit uint32, ft store.TaskType, allType, reverse, includeFailed bool) []string {
	return this.db.GetTaskIdList(offset, limit, ft, allType, reverse, includeFailed)
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
func (this *TaskMgr) GetTaskPeerProgress(id, nodeAddr string) uint64 {
	return this.db.GetTaskPeerProgress(id, nodeAddr)
}
