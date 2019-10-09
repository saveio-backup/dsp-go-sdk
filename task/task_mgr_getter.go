package task

import (
	"fmt"

	"github.com/saveio/dsp-go-sdk/common"
	"github.com/saveio/dsp-go-sdk/store"
	fs "github.com/saveio/themis/smartcontract/service/native/savefs"
)

// getter list for task info

func (this *TaskMgr) GetSessionId(taskId, peerWalletAddr string) (string, error) {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		return "", fmt.Errorf("task: %s, not exist", taskId)
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
	return "", fmt.Errorf("unknown task type %d", v.GetTaskType())
}

func (this *TaskMgr) FileNameFromTask(taskId string) (string, error) {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		return "", fmt.Errorf("task: %s, not exist", taskId)
	}
	return v.GetFileName(), nil
}

// TaskType.
func (this *TaskMgr) TaskType(taskId string) (store.TaskType, error) {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		return store.TaskTypeNone, fmt.Errorf("task: %s, not exist", taskId)
	}
	return v.GetTaskType(), nil
}

func (this *TaskMgr) TaskFileHash(taskId string) (string, error) {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		return "", fmt.Errorf("task: %s, not exist", taskId)
	}
	return v.GetFileHash(), nil

}

func (this *TaskMgr) OnlyBlock(taskId string) (bool, error) {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		return false, fmt.Errorf("task: %s, not exist", taskId)
	}
	return v.GetOnlyblock(), nil
}

func (this *TaskMgr) GetTaskState(taskId string) (TaskState, error) {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		return TaskStateNone, fmt.Errorf("task: %s, not exist", taskId)
	}
	return v.State(), nil
}

func (this *TaskMgr) GetTaskDetailState(taskId string) (TaskProgressState, error) {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		return 0, fmt.Errorf("task: %s, not exist", taskId)
	}
	return v.TransferingState(), nil
}

func (this *TaskMgr) GetFileTotalBlockCount(taskId string) (uint64, error) {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		return 0, fmt.Errorf("task: %s, not exist", taskId)
	}
	return v.GetTotalBlockCnt(), nil
}

func (this *TaskMgr) GetFilePrefix(taskId string) ([]byte, error) {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		return nil, fmt.Errorf("task: %s, not exist", taskId)
	}
	return v.GetPrefix(), nil
}

func (this *TaskMgr) GetTaskUpdatedAt(taskId string) (uint64, error) {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		return 0, fmt.Errorf("task: %s, not exist", taskId)
	}
	return v.GetUpdatedAt(), nil
}

func (this *TaskMgr) GetFileName(taskId string) (string, error) {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		return "", fmt.Errorf("task: %s, not exist", taskId)
	}
	return v.GetFileName(), nil
}

func (this *TaskMgr) GetFileOwner(taskId string) (string, error) {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		return "", fmt.Errorf("task: %s, not exist", taskId)
	}
	return v.GetFileOwner(), nil
}

func (this *TaskMgr) GetFilePrivateKey(taskId string) ([]byte, error) {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		return nil, fmt.Errorf("task: %s, not exist", taskId)
	}
	return v.GetPrivateKey(), nil
}

func (this *TaskMgr) GetStoreTx(taskId string) (string, error) {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		return "", fmt.Errorf("task: %s, not exist", taskId)
	}
	return v.GetStoreTx(), nil
}

func (this *TaskMgr) GetRegUrlTx(taskId string) (string, error) {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		return "", fmt.Errorf("task: %s, not exist", taskId)
	}
	return v.GetRegUrlTx(), nil
}

func (this *TaskMgr) GetBindUrlTx(taskId string) (string, error) {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		return "", fmt.Errorf("task: %s, not exist", taskId)
	}
	return v.GetBindUrlTx(), nil
}

func (this *TaskMgr) GetWhitelistTx(taskId string) (string, error) {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		return "", fmt.Errorf("task: %s, not exist", taskId)
	}
	return v.GetWhitelistTx(), nil
}

func (this *TaskMgr) GetFilePath(taskId string) (string, error) {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		return "", fmt.Errorf("task: %s, not exist", taskId)
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
	return this.db.GetUndownloadedBlockInfo(id, rootBlockHash)
}

func (this *TaskMgr) GetFileSessions(fileInfoId string) (map[string]*store.Session, error) {
	return this.db.GetFileSessions(fileInfoId)
}

func (this *TaskMgr) GetFileUploadOptions(id string) (*fs.UploadOption, error) {
	return this.db.GetFileUploadOptions(id)
}

func (this *TaskMgr) GetFileDownloadOptions(id string) (*common.DownloadOption, error) {
	return this.db.GetFileDownloadOptions(id)
}

func (this *TaskMgr) GetCurrentSetBlock(fileInfoId string) (string, uint64, error) {
	return this.db.GetCurrentSetBlock(fileInfoId)
}

func (this *TaskMgr) IsFileUploaded(id string) bool {
	return this.db.IsFileUploaded(id)
}

func (this *TaskMgr) IsFileDownloaded(id string) bool {
	return this.db.IsFileDownloaded(id)
}

func (this *TaskMgr) GetFileInfo(id string) (*store.TaskInfo, error) {
	return this.db.GetFileInfo(id)
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
	return this.db.AllDownloadFiles()
}

func (this *TaskMgr) CanShareTo(id, walletAddress string, asset int32) (bool, error) {
	return this.db.CanShareTo(id, walletAddress, asset)
}

func (this *TaskMgr) GetBlockOffset(id, blockHash string, index uint32) (uint64, error) {
	return this.db.GetBlockOffset(id, blockHash, index)
}

func (this *TaskMgr) GetTaskIdList(offset, limit uint32, ft store.TaskType, allType, reverse bool) []string {
	return this.db.GetTaskIdList(offset, limit, ft, allType, reverse)
}

func (this *TaskMgr) GetUnpaidAmount(id, payToAddress string, asset int32) (uint64, error) {
	return this.db.GetUnpaidAmount(id, payToAddress, asset)
}
