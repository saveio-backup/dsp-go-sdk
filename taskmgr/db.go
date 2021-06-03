package taskmgr

import (
	sdkErr "github.com/saveio/dsp-go-sdk/error"
	"github.com/saveio/dsp-go-sdk/store"
	"github.com/saveio/dsp-go-sdk/task/base"
	"github.com/saveio/themis/common/log"
)

func (this *TaskMgr) GetUploadTaskId(fileHashStr, walletAddr string) string {
	fields := make(map[string]string)
	fields[store.TaskInfoFieldFileHash] = fileHashStr
	if len(walletAddr) > 0 {
		fields[store.TaskInfoFieldWalletAddress] = walletAddr
	}
	task := this.db.GetUploadTaskByFields(fields)
	if task == nil {
		log.Errorf("get upload task by fields %v ", fields)
		return ""
	}
	return task.Id
}

func (this *TaskMgr) GetUploadTaskByFileHash(fileHashStr string) *store.TaskInfo {
	fields := make(map[string]string)
	fields[store.TaskInfoFieldFileHash] = fileHashStr
	fields[store.TaskInfoFieldWalletAddress] = this.chain.WalletAddress()
	task := this.db.GetUploadTaskByFields(fields)
	if task == nil {
		log.Errorf("get upload task by fields %v", fields)
		return nil
	}
	return task
}

// GetDownloadedTaskId. get a downloaded task id with file hash str
func (this *TaskMgr) GetDownloadedTaskId(fileHashStr, walletAddr string) string {
	fields := make(map[string]string)
	fields[store.TaskInfoFieldFileHash] = fileHashStr
	if len(walletAddr) > 0 {
		fields[store.TaskInfoFieldWalletAddress] = walletAddr
	}
	tsk := this.db.GetDownloadTaskInfoByField(fields)
	if tsk == nil {
		log.Debugf("get download task nil by fields %v ", fields)
		return ""
	}
	return tsk.Id
}

func (this *TaskMgr) GetDownloadTaskInfoByFileHash(fileHash string) *store.TaskInfo {
	fields := make(map[string]string)
	fields[store.TaskInfoFieldFileHash] = fileHash
	fields[store.TaskInfoFieldWalletAddress] = this.chain.WalletAddress()
	task := this.db.GetDownloadTaskInfoByField(fields)
	if task == nil {
		log.Debugf("get download task nil by fields %v ", fields)
		return nil
	}
	return task
}

// GetDownloadedTaskId. get a downloaded task id with file hash str
func (this *TaskMgr) GetShareTaskId(fileHashStr, walletAddr string) string {
	fields := make(map[string]string)
	fields[store.TaskInfoFieldFileHash] = fileHashStr
	if len(walletAddr) > 0 {
		fields[store.TaskInfoFieldWalletAddress] = walletAddr
	}
	tsk := this.db.GetShareTaskByFields(fields)
	if tsk == nil {
		log.Debugf("get download task nil by fields %v ", fields)
		return ""
	}
	return tsk.Id
}

func (this *TaskMgr) GetShareTaskInfoByFileHash(fileHash string) *store.TaskInfo {
	fields := make(map[string]string)
	fields[store.TaskInfoFieldFileHash] = fileHash
	fields[store.TaskInfoFieldWalletAddress] = this.chain.WalletAddress()
	task := this.db.GetShareTaskByFields(fields)
	if task == nil {
		log.Debugf("get download task nil by fields %v ", fields)
		return nil
	}
	return task
}

func (this *TaskMgr) GetTaskFileName(id string) string {
	info, _ := this.db.GetTaskInfo(id)
	if info == nil {
		return ""
	}
	return info.FileName
}

func (this *TaskMgr) GetTaskFileHash(id string) string {
	info, _ := this.db.GetTaskInfo(id)
	if info == nil {
		return ""
	}
	return info.FileHash
}

func (this *TaskMgr) GetTaskIdWithPaymentId(paymentId int32) (string, error) {
	id, err := this.db.GetTaskIdWithPaymentId(paymentId)
	if err != nil {
		return "", sdkErr.New(sdkErr.GET_FILEINFO_FROM_DB_ERROR, err.Error())
	}
	return id, nil
}

func (this *TaskMgr) GetUnSlavedTasks() ([]string, error) {
	return this.db.UnSlavedList(store.TaskTypeUpload)
}

func (this *TaskMgr) AllDownloadFiles() ([]*store.TaskInfo, []string, error) {
	info, hashes, err := this.db.AllDownloadFiles()
	if err != nil {
		return nil, nil, sdkErr.New(sdkErr.GET_FILEINFO_FROM_DB_ERROR, err.Error())
	}
	return info, hashes, nil
}

// ExistSameUploadingFile.
func (this *TaskMgr) ExistSameUploadFile(taskId, fileHashStr string) bool {
	this.uploadTaskLock.RLock()
	defer this.uploadTaskLock.RUnlock()
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

func (this *TaskMgr) IsFileDownloaded(id string) bool {
	return this.db.IsFileDownloaded(id)
}

func (this *TaskMgr) GetTaskIdList(offset, limit uint32, createdAt, createdAtEnd, updatedAt, updatedAtEnd uint64,
	ft store.TaskType, complete, reverse, includeFailed, ignoreHide bool) []string {
	return this.db.GetTaskIdList(offset, limit, createdAt, createdAtEnd, updatedAt, updatedAtEnd, ft,
		complete, reverse, includeFailed, ignoreHide)
}

func (this *TaskMgr) HideTaskIds(ids []string) error {
	err := this.db.HideTaskIds(ids)
	if err != nil {
		return sdkErr.New(sdkErr.SET_FILEINFO_DB_ERROR, err.Error())
	}
	return nil
}

func (this *TaskMgr) GetUploadTaskInfos() ([]*store.TaskInfo, error) {
	infos, err := this.db.GetUploadTaskInfos()
	if err != nil {
		return nil, sdkErr.New(sdkErr.GET_FILEINFO_FROM_DB_ERROR, err.Error())
	}
	return infos, nil
}

func (this *TaskMgr) RecoverLossTaskFromDB() error {
	if this.chain == nil || this.chain.CurrentAccount() == nil {
		return nil
	}
	// get uploaded file from chain
	list, err := this.chain.GetFileList(this.chain.CurrentAccount().Address)
	if err != nil {
		log.Errorf("get file list err %s", err)
	}
	if list == nil {
		return nil
	}

	for _, h := range list.List {
		// get upload taskId from fileHash
		fileHashStr := string(h.Hash)
		tsk := this.GetUploadTaskByFileHash(fileHashStr)
		if tsk != nil {
			continue
		}
		info, _ := this.chain.GetFileInfo(fileHashStr)
		if info == nil {
			log.Debugf("recover loss task for file %s get info is nil : %v", fileHashStr)
			continue
		}

		newUploadTask, err := this.NewUploadTask("")
		if err != nil {
			return err
		}
		if err := newUploadTask.SetInfoWithOptions(
			base.FileHash(fileHashStr),
			base.Walletaddr(this.chain.WalletAddress()),
			base.FileName(string(info.FileDesc)),
		); err != nil {
			return err
		}
		// err = this.BindTaskId(newId)
		// if err != nil {
		// 	return err
		// }
		// t, _ = this.GetTaskById(newId)
		// if t == nil {
		// 	return fmt.Errorf("set new task with id failed %s", newId)
		// }
		log.Debugf("recover db loss task %s %s", newUploadTask.GetId(), fileHashStr)
		newUploadTask.SetResult(nil, sdkErr.GET_FILEINFO_FROM_DB_ERROR, "DB has damaged. Can't recover the task")
	}

	return nil
}

func (this *TaskMgr) FileBlockHashes(id string) []string {
	return this.db.FileBlockHashes(id)
}

func (this *TaskMgr) InsertShareRecord(id, fileHash, fileName, fileOwner, toWalletAddr string, profit uint64) error {
	if this.shareRecordDB == nil {
		return nil
	}
	return this.shareRecordDB.InsertShareRecord(id, fileHash, fileName, fileOwner, toWalletAddr, profit)
}

func (this *TaskMgr) IncreaseShareRecordProfit(id string, profit uint64) error {
	if this.shareRecordDB == nil {
		return nil
	}
	return this.shareRecordDB.IncreaseShareRecordProfit(id, profit)
}

func (this *TaskMgr) FindShareRecordById(id string) (*store.ShareRecord, error) {
	if this.shareRecordDB == nil {
		return nil, nil
	}
	return this.shareRecordDB.FindShareRecordById(id)
}
func (this *TaskMgr) FindShareRecordsByCreatedAt(beginedAt, endedAt, offset, limit int64) (
	[]*store.ShareRecord, int, error) {
	if this.shareRecordDB == nil {
		return nil, 0, nil
	}
	return this.shareRecordDB.FindShareRecordsByCreatedAt(beginedAt, endedAt, offset, limit)
}
func (this *TaskMgr) FindLastShareTime(fileHash string) (uint64, error) {
	if this.shareRecordDB == nil {
		return 0, nil
	}
	return this.shareRecordDB.FindLastShareTime(fileHash)
}
func (this *TaskMgr) CountRecordByFileHash(fileHash string) (uint64, error) {
	if this.shareRecordDB == nil {
		return 0, nil
	}
	return this.shareRecordDB.CountRecordByFileHash(fileHash)
}
func (this *TaskMgr) SumRecordsProfit() (uint64, error) {
	if this.shareRecordDB == nil {
		return 0, nil
	}
	return this.shareRecordDB.SumRecordsProfit()
}
func (this *TaskMgr) SumRecordsProfitByFileHash(fileHashStr string) (uint64, error) {
	if this.shareRecordDB == nil {
		return 0, nil
	}
	return this.shareRecordDB.SumRecordsProfitByFileHash(fileHashStr)
}
func (this *TaskMgr) SumRecordsProfitById(id string) (uint64, error) {
	if this.shareRecordDB == nil {
		return 0, nil
	}
	return this.shareRecordDB.SumRecordsProfitById(id)
}

func (this *TaskMgr) IsFileInfoExist(id string) bool {
	return this.db.IsTaskInfoExist(id)
}

func (this *TaskMgr) AddFileSession(fileInfoId string, sessionId, walletAddress, hostAddress string,
	asset uint32, unitPrice uint64) error {
	err := this.db.AddFileSession(fileInfoId, sessionId, walletAddress, hostAddress, asset, unitPrice)
	if err != nil {
		return sdkErr.New(sdkErr.SET_FILEINFO_DB_ERROR, err.Error())
	}
	return nil
}

func (this *TaskMgr) GetCurrentSetBlock(fileInfoId string) (string, uint64, error) {
	hash, index, err := this.db.GetCurrentSetBlock(fileInfoId)
	if err != nil {
		return "", 0, sdkErr.New(sdkErr.GET_FILEINFO_FROM_DB_ERROR, err.Error())
	}
	return hash, index, nil
}

func (this *TaskMgr) AddShareTo(id, walletAddress string) error {
	err := this.db.AddShareTo(id, walletAddress)
	if err != nil {
		return sdkErr.New(sdkErr.SET_FILEINFO_DB_ERROR, err.Error())
	}
	return nil
}

// GetTaskPeerProgress. get task peer progress
func (this *TaskMgr) GetTaskPeerProgress(id, nodeAddr string) *store.FileProgress {
	return this.db.GetTaskPeerProgress(id, nodeAddr)
}

func (this *TaskMgr) IsBlockDownloaded(id, blockHashStr string, index uint64) bool {
	return this.db.IsBlockDownloaded(id, blockHashStr, index)
}
