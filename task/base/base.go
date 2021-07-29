package base

import (
	"context"
	"fmt"
	"sync"

	"github.com/google/uuid"
	"github.com/saveio/dsp-go-sdk/consts"
	sdkErr "github.com/saveio/dsp-go-sdk/error"
	"github.com/saveio/dsp-go-sdk/store"
	"github.com/saveio/dsp-go-sdk/task/types"
	"github.com/saveio/dsp-go-sdk/utils/task"
	uTime "github.com/saveio/dsp-go-sdk/utils/time"
	"github.com/saveio/themis/common/log"
)

type Task struct {
	Id string // task id
	// Info        *store.TaskInfo          // task info from local DB
	Lock        *sync.RWMutex            // lock
	DB          *store.TaskDB            // db
	Batch       bool                     // batch save to db
	Workers     map[string]*Worker       // set remote connection as worker
	Mgr         ITaskMgr                 // task mgr interface delegate
	progress    chan *types.ProgressInfo // emit progress channel
	progressCtx context.Context          // context to close the channel
}

// NewTask. new task for file, and set the task info to DB.
func NewTask(taskId string, taskType store.TaskType, db *store.TaskDB) *Task {
	if len(taskId) == 0 {
		id, _ := uuid.NewUUID()
		taskId = id.String()
	}
	info, err := db.NewTaskInfo(taskId, taskType)
	if err != nil {
		log.Errorf("new file info failed %s", err)
		return nil
	}
	t := newTask(taskId, info, db)
	// t.payOnL1 = true
	info.Id = taskId
	log.Debugf("set task %s state %v, original state %v", taskId, store.TaskStatePrepare, info.TaskState)
	info.TaskState = store.TaskStatePrepare

	err = db.SaveTaskInfo(info)
	if err != nil {
		log.Debugf("save file info failed err %s", err)
		return nil
	}
	return t
}

// func (this *Task) NewBatchSet() {
// 	this.Lock.Lock()
// 	defer this.Lock.Unlock()
// 	if this.Batch {
// 		log.Warnf("batch set flag is active")
// 	}
// 	this.Batch = true
// }

func (this *Task) SetTaskState(newState store.TaskState) error {
	this.Lock.Lock()
	defer this.Lock.Unlock()
	return this.setTaskState(newState)
}

func (this *Task) UnsafeSetTaskState(newState store.TaskState) error {
	return this.setTaskState(newState)
}

func (this *Task) SetSessionId(peerWalletAddr, id string) {
	this.Lock.Lock()
	defer this.Lock.Unlock()
	info := this.getTaskInfo()
	if info.PeerToSessionIds == nil {
		info.PeerToSessionIds = make(map[string]string)
	}
	info.PeerToSessionIds[peerWalletAddr] = id
	if this.Batch {
		return
	}
	this.DB.SaveTaskInfo(info)
}

func (this *Task) SetResult(result interface{}, errorCode uint32, errorMsg string) error {
	this.Lock.Lock()
	defer this.Lock.Unlock()
	info := this.getTaskInfo()
	log.Debugf("task %s state %v", this.Id, info.TaskState)
	if info.TaskState == store.TaskStateCancel {
		return fmt.Errorf("task %s is canceled", info.Id)
	}
	log.Debugf("set task result %v %v", errorCode, errorMsg)
	if errorCode != 0 {
		if info.Retry >= consts.MAX_TASK_RETRY {
			// task hasn't pay on chain, make it failed
			info.TaskState = store.TaskStateFailed
			info.ErrorCode = errorCode
			info.ErrorMsg = errorMsg
		} else {
			// task has paid, retry it later
			info.TaskState = store.TaskStateIdle
			info.Retry++
			info.RetryAt = uTime.GetMilliSecTimestamp() + task.GetJitterDelay(info.Retry, 30)*1000
			log.Errorf("task %s, file %s  is failed, error code %d, error msg %v, retry %d times, retry at %d",
				info.Id, info.FileHash, errorCode, errorMsg, info.Retry, info.RetryAt)
		}
	} else if result != nil {
		info.ErrorCode = 0
		info.ErrorMsg = ""
		info.Result = result
		info.TaskState = store.TaskStateDone
		log.Debugf("task: %s, type %v has done, task state %v",
			info.Id, info.Type, info.TaskState)
		switch info.Type {
		case store.TaskTypeUpload:
			err := this.DB.SaveFileUploaded(info.Id, info.Type)
			if err != nil {
				return err
			}
		case store.TaskTypeDispatch:
			err := this.DB.SaveFileUploaded(info.Id, info.Type)
			if err != nil {
				return err
			}
		case store.TaskTypeDownload:
			err := this.DB.SaveFileDownloaded(info.Id)
			if err != nil {
				return err
			}
		}
	}
	if this.Batch {
		return nil
	}
	return this.DB.SaveTaskInfo(info)
}

func (this *Task) SetInfoWithOptions(opts ...InfoOption) error {
	this.Lock.Lock()
	defer this.Lock.Unlock()
	info := this.getTaskInfo()
	log.Infof("set info with options %v id %v", info, this.Id)
	for _, opt := range opts {
		opt.apply(info)
	}
	return this.DB.SaveTaskInfo(info)
}

// func (this *Task) SetInfo(info *store.TaskInfo) {
// 	this.Lock.Lock()
// 	defer this.Lock.Unlock()
// 	this.Info = info
// }

// func (this *Task) BatchCommit() error {
// 	this.Lock.Lock()
// 	defer this.Lock.Unlock()
// 	if !this.Batch {
// 		return nil
// 	}
// 	this.Batch = false
// 	return this.DB.SaveTaskInfo(this.Info)
// }

func (this *Task) GetCurrentWalletAddr() string {
	return this.Mgr.Chain().WalletAddress()
}

func (this *Task) GetTaskType() store.TaskType {
	this.Lock.RLock()
	defer this.Lock.RUnlock()
	info := this.getTaskInfo()
	return info.Type
}

func (this *Task) GetUrl() string {
	this.Lock.RLock()
	defer this.Lock.RUnlock()
	info := this.getTaskInfo()
	return info.Url
}

func (this *Task) GetSessionId(peerWalletAddr string) string {
	this.Lock.RLock()
	defer this.Lock.RUnlock()
	info := this.getTaskInfo()
	return info.PeerToSessionIds[peerWalletAddr]
}

// GetTaskInfoCopy. get task info deep copy object.
// clone it for read-only
func (this *Task) GetTaskInfoCopy() *store.TaskInfo {
	this.Lock.RLock()
	defer this.Lock.RUnlock()
	info := this.getTaskInfo()
	return this.DB.CopyTask(info)
}

func (this *Task) GetFileHash() string {
	this.Lock.RLock()
	defer this.Lock.RUnlock()
	info := this.getTaskInfo()
	return info.FileHash
}

func (this *Task) GetFileName() string {
	this.Lock.RLock()
	defer this.Lock.RUnlock()
	info := this.getTaskInfo()
	return info.FileName
}

func (this *Task) GetFileOwner() string {
	this.Lock.RLock()
	defer this.Lock.RUnlock()
	info := this.getTaskInfo()
	return info.FileOwner
}

func (this *Task) GetId() string {
	this.Lock.RLock()
	defer this.Lock.RUnlock()
	info := this.getTaskInfo()
	return info.Id
}

func (this *Task) GetTotalBlockCnt() uint64 {
	this.Lock.RLock()
	defer this.Lock.RUnlock()
	info := this.getTaskInfo()
	return info.TotalBlockCount
}

func (this *Task) GetPrefix() []byte {
	this.Lock.RLock()
	defer this.Lock.RUnlock()
	info := this.getTaskInfo()
	return info.Prefix
}

func (this *Task) GetCreatedAt() uint64 {
	this.Lock.RLock()
	defer this.Lock.RUnlock()
	info := this.getTaskInfo()
	return info.CreatedAt
}

func (this *Task) GetCopyNum() uint32 {
	this.Lock.RLock()
	defer this.Lock.RUnlock()
	info := this.getTaskInfo()
	return info.CopyNum
}

func (this *Task) GetRetryAt() uint64 {
	this.Lock.RLock()
	defer this.Lock.RUnlock()
	info := this.getTaskInfo()
	return info.RetryAt
}

func (this *Task) GetWalletAddr() string {
	this.Lock.RLock()
	defer this.Lock.RUnlock()
	info := this.getTaskInfo()
	return info.WalletAddress
}

func (this *Task) GetFilePath() string {
	this.Lock.RLock()
	defer this.Lock.RUnlock()
	info := this.getTaskInfo()
	return info.FilePath
}

func (this *Task) GetStoreTx() string {
	this.Lock.RLock()
	defer this.Lock.RUnlock()
	info := this.getTaskInfo()
	return info.StoreTx
}

func (this *Task) GetStoreTxHeight() uint32 {
	this.Lock.RLock()
	defer this.Lock.RUnlock()
	info := this.getTaskInfo()
	return info.StoreTxHeight
}

func (this *Task) TransferingState() types.TaskProgressState {
	this.Lock.RLock()
	defer this.Lock.RUnlock()
	info := this.getTaskInfo()
	return types.TaskProgressState(info.TranferState)
}

func (this *Task) GetProgressInfo() *types.ProgressInfo {
	this.Lock.RLock()
	defer this.Lock.RUnlock()
	return this.getProgressInfo()
}

func (this *Task) State() store.TaskState {
	this.Lock.RLock()
	defer this.Lock.RUnlock()
	info := this.getTaskInfo()
	return store.TaskState(info.TaskState)
}

func (this *Task) Pause() error {
	this.Lock.Lock()
	defer this.Lock.Unlock()
	info := this.getTaskInfo()
	state := info.TaskState
	if state != store.TaskStatePrepare && state != store.TaskStatePause && state != store.TaskStateDoing {
		return fmt.Errorf("task %s can't pause with state %v", info.Id, state)
	}
	if state == store.TaskStateDoing || state == store.TaskStatePrepare {
		return this.setTaskState(store.TaskStatePause)
	}
	return fmt.Errorf("task %s can't pause with unknown state %v", info.Id, state)
}

func (this *Task) Resume() error {
	this.Lock.Lock()
	defer this.Lock.Unlock()
	info := this.getTaskInfo()
	state := info.TaskState
	if state != store.TaskStatePrepare && state != store.TaskStatePause &&
		state != store.TaskStateDoing && state != store.TaskStateIdle {
		return fmt.Errorf("task %s can't resume with state %v", info.Id, state)
	}
	if state == store.TaskStatePause {
		return this.setTaskState(store.TaskStateDoing)
	}
	return fmt.Errorf("task %s can't resume with unknown state %v", info.Id, state)
}

func (this *Task) Cancel() error {
	this.Lock.Lock()
	defer this.Lock.Unlock()
	info := this.getTaskInfo()
	if info.TaskState == store.TaskStateCancel {
		return nil
	}

	if types.TaskProgressState(info.TranferState) == types.TaskUploadFilePaying ||
		types.TaskProgressState(info.TranferState) == types.TaskWaitForBlockConfirmed ||
		types.TaskProgressState(info.TranferState) == types.TaskDownloadPayForBlocks {
		return fmt.Errorf("task %v is paying, can't cancel", info.Id)
	}

	return this.setTaskState(store.TaskStateCancel)

}

func (this *Task) Retry() error {
	this.Lock.Lock()
	defer this.Lock.Unlock()
	info := this.getTaskInfo()
	state := info.TaskState
	if state != store.TaskStateFailed {
		return fmt.Errorf("state is %d, can't retry", state)
	}
	if state == store.TaskStateFailed {
		return this.setTaskState(store.TaskStateDoing)
	}
	return nil
}

func (this *Task) IsTaskPaused() bool {
	this.Lock.RLock()
	defer this.Lock.RUnlock()
	info := this.getTaskInfo()
	return info.TaskState == store.TaskStatePause
}

func (this *Task) NeedRetry() bool {
	this.Lock.RLock()
	defer this.Lock.RUnlock()
	info := this.getTaskInfo()
	if info.TaskState != store.TaskStateIdle {
		return false
	}
	if uTime.GetMilliSecTimestamp() >= info.RetryAt {
		log.Debugf("uTime.GetMilliSecTimestamp() %d, retry at %d", uTime.GetMilliSecTimestamp(), info.RetryAt)
		return true
	}
	return false
}

func (this *Task) IsTaskDone() bool {
	this.Lock.RLock()
	defer this.Lock.RUnlock()
	info := this.getTaskInfo()
	return info.TaskState == store.TaskStateDone
}

// IsTaskCancel. check if task is cancel
func (this *Task) IsTaskCancel() bool {
	this.Lock.RLock()
	defer this.Lock.RUnlock()
	info := this.getTaskInfo()
	return info.TaskState == store.TaskStateCancel
}

func (this *Task) IsTaskPaying() bool {
	this.Lock.RLock()
	defer this.Lock.RUnlock()
	info := this.getTaskInfo()
	return types.TaskProgressState(info.TranferState) == types.TaskUploadFilePaying ||
		types.TaskProgressState(info.TranferState) == types.TaskWaitForBlockConfirmed ||
		types.TaskProgressState(info.TranferState) == types.TaskDownloadPayForBlocks
}

func (this *Task) IsTaskPauseOrCancel() (bool, bool) {
	this.Lock.RLock()
	defer this.Lock.RUnlock()
	info := this.getTaskInfo()
	state := info.TaskState
	return state == store.TaskStatePause, state == store.TaskStateCancel
}

func (this *Task) IsTaskStop() bool {
	this.Lock.RLock()
	defer this.Lock.RUnlock()
	info := this.getTaskInfo()
	state := info.TaskState
	if state != store.TaskStatePause && state != store.TaskStateCancel {
		return false
	}
	return state == store.TaskStatePause || state == store.TaskStateCancel
}

func (this *Task) IsTaskPreparingOrDoing() (bool, bool) {
	this.Lock.RLock()
	defer this.Lock.RUnlock()
	info := this.getTaskInfo()
	state := info.TaskState
	return state == store.TaskStatePrepare, state == store.TaskStateDoing
}

func (this *Task) IsTaskFailed() bool {
	this.Lock.RLock()
	defer this.Lock.RUnlock()
	info := this.getTaskInfo()
	return info.TaskState == store.TaskStateFailed
}

func (this *Task) SetProgressNotifyCh(ch chan *types.ProgressInfo, ctx context.Context) {
	this.progress = ch
	this.progressCtx = ctx
}

// EmitProgress. emit progress to channel with taskId
func (this *Task) EmitProgress(state types.TaskProgressState) {
	this.Lock.Lock()
	defer this.Lock.Unlock()
	info := this.getTaskInfo()
	if this.progress == nil {
		return
	}
	if info.TaskState != store.TaskStateCancel {
		info.TranferState = uint32(state)
		if err := this.DB.SaveTaskInfo(info); err != nil {
			log.Errorf("save task into db failed %v", err)
		}
	} else {
		log.Debugf("skip save upload task %v because it's canceled", info.Id)
	}

	pInfo := this.getProgressInfo()
	log.Debugf("emit progress taskId: %s, transfer state: %v, progress: %v", info.Id, state, pInfo)
	go func() {
		select {
		case <-this.progressCtx.Done():
			log.Debugf("progress channel has closed")
			this.progress = nil
		default:
			this.progress <- pInfo
		}
	}()
}

func (this *Task) NewWorkers(addrs []string, job types.JobFunc) {
	this.Lock.Lock()
	defer this.Lock.Unlock()
	if this.Workers == nil {
		this.Workers = make(map[string]*Worker, 0)
	}
	for _, walletAddr := range addrs {
		w := NewWorker(walletAddr, job)
		this.Workers[walletAddr] = w
	}
}

func (this *Task) SetWorkerWorking(walletAddr string, working bool) {
	this.Lock.Lock()
	defer this.Lock.Unlock()

	w, ok := this.Workers[walletAddr]
	if !ok {
		log.Warnf("task %s, set remote peer %s working failed, peer not found",
			this.Id, walletAddr)
		return
	}
	log.Debugf("task %s, set peer %s working %t", this.Id, walletAddr, working)
	w.SetWorking(working)
}

func (this *Task) ActiveWorker(addr string) {
	this.Lock.Lock()
	defer this.Lock.Unlock()
	w, ok := this.Workers[addr]
	if !ok {
		return
	}
	w.Active()
}

func (this *Task) Clean() error {
	this.Lock.Lock()
	defer this.Lock.Unlock()
	err := this.DB.DeleteTaskInfo(this.Id)
	if err != nil {
		return sdkErr.NewWithError(sdkErr.SET_FILEINFO_DB_ERROR, err)
	}
	return nil
}

func (this *Task) GetTaskInfo() *store.TaskInfo {
	return this.getTaskInfo()
}

func (this *Task) getTaskInfo() *store.TaskInfo {
	info, err := this.DB.GetTaskInfo(this.Id)
	if err != nil {
		log.Warnf("get task info id %s from db err %s", this.Id, err)
	}
	return info
}

func (this *Task) setTaskState(newState store.TaskState) error {
	info := this.getTaskInfo()
	oldState := store.TaskState(info.TaskState)
	log.Debugf("set task %s state %v, original state %v", info.Id, newState, oldState)
	if oldState == newState {
		return nil
	}
	switch newState {
	case store.TaskStatePause:
		if oldState == store.TaskStateFailed || oldState == store.TaskStateDone {
			return fmt.Errorf("can't stop a failed or completed task")
		}
	case store.TaskStateDoing:
		if oldState == store.TaskStateDone {
			return fmt.Errorf("can't continue a failed or completed task")
		}
		info.ErrorCode = 0
		info.ErrorMsg = ""
	case store.TaskStateDone:
	case store.TaskStateCancel:
	}
	info.TaskState = newState
	changeFromPause := (oldState == store.TaskStatePause && (newState == store.TaskStateDoing || newState == store.TaskStateCancel))
	changeFromDoing := (oldState == store.TaskStateDoing && (newState == store.TaskStatePause || newState == store.TaskStateCancel))
	if changeFromPause {
		log.Debugf("task: %s changeFromPause, send new state change: %d to %d", info.Id, oldState, newState)
	}
	if changeFromDoing {
		log.Debugf("task: %s changeFromDoing, send new state change: %d to %d", info.Id, oldState, newState)
	}
	if this.Batch {
		return nil
	}
	return this.DB.SaveTaskInfo(info)
}

func (this *Task) getProgressInfo() *types.ProgressInfo {
	info := this.getTaskInfo()
	pInfo := &types.ProgressInfo{
		TaskId:        info.Id,
		Type:          info.Type,
		Url:           info.Url,
		StoreType:     info.StoreType,
		FileName:      info.FileName,
		FileHash:      info.FileHash,
		FilePath:      info.FilePath,
		Total:         info.TotalBlockCount,
		CopyNum:       info.CopyNum,
		FileSize:      info.FileSize,
		RealFileSize:  info.RealFileSize,
		Encrypt:       info.Encrypt,
		Progress:      this.DB.FileProgress(info.Id),
		NodeHostAddrs: info.NodeHostAddrs,
		TaskState:     store.TaskState(info.TaskState),
		ProgressState: types.TaskProgressState(info.TranferState),
		CreatedAt:     info.CreatedAt / consts.MILLISECOND_PER_SECOND,
		UpdatedAt:     info.UpdatedAt / consts.MILLISECOND_PER_SECOND,
		Result:        info.Result,
		ErrorCode:     info.ErrorCode,
		ErrorMsg:      info.ErrorMsg,
	}
	if info.Type != store.TaskTypeUpload {
		return pInfo
	}
	if len(pInfo.Progress) == 0 {
		return pInfo
	}
	if len(info.PrimaryNodes) == 0 {
		return pInfo
	}
	masterNode := info.PrimaryNodes[0]
	if len(masterNode) == 0 {
		return pInfo
	}
	masterNodeProgress := make(map[string]store.FileProgress, 0)
	pInfo.SlaveProgress = make(map[string]store.FileProgress, 0)
	for node, progress := range pInfo.Progress {
		if node == masterNode {
			masterNodeProgress[node] = progress
			continue
		}
		pInfo.SlaveProgress[node] = progress
	}
	// only show master node progress here
	pInfo.Progress = masterNodeProgress
	return pInfo
}

func newTask(id string, info *store.TaskInfo, db *store.TaskDB) *Task {
	t := &Task{
		Id: id,
		// Info: info,
		DB:   db,
		Lock: new(sync.RWMutex),
	}
	// t.Info.Id = id
	return t
}
