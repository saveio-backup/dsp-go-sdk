package base

import (
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
	Info     *store.TaskInfo          // task info from local DB
	Lock     *sync.RWMutex            // lock
	DB       *store.TaskDB            // db
	Batch    bool                     // batch save to db
	Workers  map[string]*Worker       // set remote connection as worker
	Mgr      ITaskMgr                 // task mgr interface delegate
	progress chan *types.ProgressInfo // emit progress channel
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
	log.Debugf("set task %s state %v, original state %v", taskId, store.TaskStatePrepare, t.Info.TaskState)
	t.Info.TaskState = store.TaskStatePrepare

	err = db.SaveTaskInfo(t.Info)
	if err != nil {
		log.Debugf("save file info failed err %s", err)
		return nil
	}
	return t
}

func (this *Task) NewBatchSet() {
	this.Lock.Lock()
	defer this.Lock.Unlock()
	if this.Batch {
		log.Warnf("batch set flag is active")
	}
	this.Batch = true
}

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
	if this.Info.PeerToSessionIds == nil {
		this.Info.PeerToSessionIds = make(map[string]string)
	}
	this.Info.PeerToSessionIds[peerWalletAddr] = id
	if this.Batch {
		return
	}
	this.DB.SaveTaskInfo(this.Info)
}

func (this *Task) SetResult(result interface{}, errorCode uint32, errorMsg string) error {
	this.Lock.Lock()
	defer this.Lock.Unlock()
	log.Debugf("task %s state %v", this.Info.Id, this.Info.TaskState)
	if this.Info.TaskState == store.TaskStateCancel {
		return fmt.Errorf("task %s is canceled", this.Info.Id)
	}
	log.Debugf("set task result %v %v", errorCode, errorMsg)
	if errorCode != 0 {
		if this.Info.Retry >= consts.MAX_TASK_RETRY {
			// task hasn't pay on chain, make it failed
			this.Info.TaskState = store.TaskStateFailed
			this.Info.ErrorCode = errorCode
			this.Info.ErrorMsg = errorMsg
		} else {
			// task has paid, retry it later
			this.Info.TaskState = store.TaskStateIdle
			this.Info.Retry++
			this.Info.RetryAt = uTime.GetMilliSecTimestamp() + task.GetJitterDelay(this.Info.Retry, 30)*1000
			log.Errorf("task %s, file %s  is failed, error code %d, error msg %v, retry %d times, retry at %d",
				this.Info.Id, this.Info.FileHash, errorCode, errorMsg, this.Info.Retry, this.Info.RetryAt)
		}
	} else if result != nil {
		this.Info.ErrorCode = 0
		this.Info.ErrorMsg = ""
		this.Info.Result = result
		this.Info.TaskState = store.TaskStateDone
		log.Debugf("task: %s, type %v has done, task state %v",
			this.Info.Id, this.Info.Type, this.Info.TaskState)
		switch this.Info.Type {
		case store.TaskTypeUpload:
			err := this.DB.SaveFileUploaded(this.Info.Id, this.Info.Type)
			if err != nil {
				return err
			}
		case store.TaskTypeDispatch:
			err := this.DB.SaveFileUploaded(this.Info.Id, this.Info.Type)
			if err != nil {
				return err
			}
		case store.TaskTypeDownload:
			err := this.DB.SaveFileDownloaded(this.Info.Id)
			if err != nil {
				return err
			}
		}
	}
	if this.Batch {
		return nil
	}
	return this.DB.SaveTaskInfo(this.Info)
}

func (this *Task) SetInfoWithOptions(opts ...InfoOption) error {
	this.Lock.Lock()
	defer this.Lock.Unlock()
	for _, opt := range opts {
		opt.apply(this.Info)
	}
	return this.DB.SaveTaskInfo(this.Info)
}

func (this *Task) SetInfo(info *store.TaskInfo) {
	this.Lock.Lock()
	defer this.Lock.Unlock()
	this.Info = info
}

func (this *Task) BatchCommit() error {
	this.Lock.Lock()
	defer this.Lock.Unlock()
	if !this.Batch {
		return nil
	}
	this.Batch = false
	return this.DB.SaveTaskInfo(this.Info)
}

func (this *Task) GetCurrentWalletAddr() string {
	return this.Mgr.Chain().WalletAddress()
}

func (this *Task) GetTaskType() store.TaskType {
	this.Lock.RLock()
	defer this.Lock.RUnlock()
	return this.Info.Type
}

func (this *Task) GetUrl() string {
	this.Lock.RLock()
	defer this.Lock.RUnlock()
	return this.Info.Url
}

func (this *Task) GetSessionId(peerWalletAddr string) string {
	this.Lock.RLock()
	defer this.Lock.RUnlock()
	return this.Info.PeerToSessionIds[peerWalletAddr]
}

// GetTaskInfoCopy. get task info deep copy object.
// clone it for read-only
func (this *Task) GetTaskInfoCopy() *store.TaskInfo {
	this.Lock.RLock()
	defer this.Lock.RUnlock()
	return this.DB.CopyTask(this.Info)
}

func (this *Task) GetFileHash() string {
	this.Lock.RLock()
	defer this.Lock.RUnlock()
	return this.Info.FileHash
}

func (this *Task) GetFileName() string {
	this.Lock.RLock()
	defer this.Lock.RUnlock()
	return this.Info.FileName
}

func (this *Task) GetFileOwner() string {
	this.Lock.RLock()
	defer this.Lock.RUnlock()
	return this.Info.FileOwner
}

func (this *Task) GetId() string {
	this.Lock.RLock()
	defer this.Lock.RUnlock()
	return this.Info.Id
}

func (this *Task) GetTotalBlockCnt() uint64 {
	this.Lock.RLock()
	defer this.Lock.RUnlock()
	return this.Info.TotalBlockCount
}

func (this *Task) GetPrefix() []byte {
	this.Lock.RLock()
	defer this.Lock.RUnlock()
	return this.Info.Prefix
}

func (this *Task) GetCreatedAt() uint64 {
	this.Lock.RLock()
	defer this.Lock.RUnlock()
	return this.Info.CreatedAt
}

func (this *Task) GetCopyNum() uint32 {
	this.Lock.RLock()
	defer this.Lock.RUnlock()
	return this.Info.CopyNum
}

func (this *Task) GetRetryAt() uint64 {
	this.Lock.RLock()
	defer this.Lock.RUnlock()
	return this.Info.RetryAt
}

func (this *Task) GetWalletAddr() string {
	this.Lock.RLock()
	defer this.Lock.RUnlock()
	return this.Info.WalletAddress
}

func (this *Task) GetFilePath() string {
	this.Lock.RLock()
	defer this.Lock.RUnlock()
	return this.Info.FilePath
}

func (this *Task) GetStoreTx() string {
	this.Lock.RLock()
	defer this.Lock.RUnlock()
	return this.Info.StoreTx
}

func (this *Task) GetStoreTxHeight() uint32 {
	this.Lock.RLock()
	defer this.Lock.RUnlock()
	return this.Info.StoreTxHeight
}

func (this *Task) TransferingState() types.TaskProgressState {
	this.Lock.RLock()
	defer this.Lock.RUnlock()
	return types.TaskProgressState(this.Info.TranferState)
}

func (this *Task) GetProgressInfo() *types.ProgressInfo {
	this.Lock.RLock()
	defer this.Lock.RUnlock()
	return this.getProgressInfo()
}

func (this *Task) State() store.TaskState {
	this.Lock.RLock()
	defer this.Lock.RUnlock()
	return store.TaskState(this.Info.TaskState)
}

func (this *Task) Pause() error {
	this.Lock.Lock()
	defer this.Lock.Unlock()

	state := this.Info.TaskState
	if state != store.TaskStatePrepare && state != store.TaskStatePause && state != store.TaskStateDoing {
		return fmt.Errorf("task %s can't pause with state %v", this.Info.Id, state)
	}
	if state == store.TaskStateDoing || state == store.TaskStatePrepare {
		return this.setTaskState(store.TaskStatePause)
	}
	return fmt.Errorf("task %s can't pause with unknown state %v", this.Info.Id, state)
}

func (this *Task) Resume() error {
	this.Lock.Lock()
	defer this.Lock.Unlock()

	state := this.Info.TaskState
	if state != store.TaskStatePrepare && state != store.TaskStatePause && state != store.TaskStateDoing {
		return fmt.Errorf("task %s can't resume with state %v", this.Info.Id, state)
	}
	if state == store.TaskStatePause {
		return this.setTaskState(store.TaskStateDoing)
	}
	return fmt.Errorf("task %s can't resume with unknown state %v", this.Info.Id, state)
}

func (this *Task) Cancel() error {
	this.Lock.Lock()
	defer this.Lock.Unlock()
	if this.Info.TaskState == store.TaskStateCancel {
		return nil
	}

	if types.TaskProgressState(this.Info.TranferState) == types.TaskUploadFilePaying ||
		types.TaskProgressState(this.Info.TranferState) == types.TaskWaitForBlockConfirmed ||
		types.TaskProgressState(this.Info.TranferState) == types.TaskDownloadPayForBlocks {
		return fmt.Errorf("task %v is paying, can't cancel", this.Info.Id)
	}

	return this.setTaskState(store.TaskStateCancel)

}

func (this *Task) Retry() error {
	this.Lock.Lock()
	defer this.Lock.Unlock()

	state := this.Info.TaskState
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
	return this.Info.TaskState == store.TaskStatePause
}

func (this *Task) NeedRetry() bool {
	this.Lock.RLock()
	defer this.Lock.RUnlock()
	if this.Info.TaskState != store.TaskStateIdle {
		return false
	}
	if uTime.GetMilliSecTimestamp() >= this.Info.RetryAt {
		log.Debugf("uTime.GetMilliSecTimestamp() %d, retry at %d", uTime.GetMilliSecTimestamp(), this.Info.RetryAt)
		return true
	}
	return false
}

func (this *Task) IsTaskDone() bool {
	this.Lock.RLock()
	defer this.Lock.RUnlock()
	return this.Info.TaskState == store.TaskStateDone
}

// IsTaskCancel. check if task is cancel
func (this *Task) IsTaskCancel() bool {
	this.Lock.RLock()
	defer this.Lock.RUnlock()
	return this.Info.TaskState == store.TaskStateCancel
}

func (this *Task) IsTaskPaying() bool {
	this.Lock.RLock()
	defer this.Lock.RUnlock()
	return types.TaskProgressState(this.Info.TranferState) == types.TaskUploadFilePaying ||
		types.TaskProgressState(this.Info.TranferState) == types.TaskWaitForBlockConfirmed ||
		types.TaskProgressState(this.Info.TranferState) == types.TaskDownloadPayForBlocks
}

func (this *Task) IsTaskPauseOrCancel() (bool, bool) {
	this.Lock.RLock()
	defer this.Lock.RUnlock()
	state := this.Info.TaskState
	return state == store.TaskStatePause, state == store.TaskStateCancel
}

func (this *Task) IsTaskStop() bool {
	this.Lock.RLock()
	defer this.Lock.RUnlock()
	state := this.Info.TaskState
	if state != store.TaskStatePause && state != store.TaskStateCancel {
		return false
	}
	return state == store.TaskStatePause || state == store.TaskStateCancel
}

func (this *Task) IsTaskPreparingOrDoing() (bool, bool) {
	this.Lock.RLock()
	defer this.Lock.RUnlock()
	state := this.Info.TaskState
	return state == store.TaskStatePrepare, state == store.TaskStateDoing
}

func (this *Task) IsTaskFailed() bool {
	this.Lock.RLock()
	defer this.Lock.RUnlock()
	return this.Info.TaskState == store.TaskStateFailed
}

func (this *Task) SetProgressNotifyCh(ch chan *types.ProgressInfo) {
	this.progress = ch
}

// EmitProgress. emit progress to channel with taskId
func (this *Task) EmitProgress(state types.TaskProgressState) {
	this.Lock.Lock()
	defer this.Lock.Unlock()
	if this.progress == nil {
		return
	}
	if this.Info.TaskState != store.TaskStateCancel {
		this.Info.TranferState = uint32(state)
		if err := this.DB.SaveTaskInfo(this.Info); err != nil {
			log.Errorf("save task into db failed %v", err)
		}
	} else {
		log.Debugf("skip save upload task %v because it's canceled", this.Info.Id)
	}

	pInfo := this.getProgressInfo()
	log.Debugf("emit progress taskId: %s, transfer state: %v, progress: %v", this.Info.Id, state, pInfo)
	go func() {
		this.progress <- pInfo
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
			this.Info.Id, walletAddr)
		return
	}
	log.Debugf("task %s, set peer %s working %t", this.Info.Id, walletAddr, working)
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
	err := this.DB.DeleteTaskInfo(this.Info.Id)
	if err != nil {
		return sdkErr.NewWithError(sdkErr.SET_FILEINFO_DB_ERROR, err)
	}
	return nil
}

func (this *Task) setTaskState(newState store.TaskState) error {
	oldState := store.TaskState(this.Info.TaskState)
	log.Debugf("set task %s state %v, original state %v", this.Info.Id, newState, oldState)
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
		this.Info.ErrorCode = 0
		this.Info.ErrorMsg = ""
	case store.TaskStateDone:
	case store.TaskStateCancel:
	}
	this.Info.TaskState = newState
	changeFromPause := (oldState == store.TaskStatePause && (newState == store.TaskStateDoing || newState == store.TaskStateCancel))
	changeFromDoing := (oldState == store.TaskStateDoing && (newState == store.TaskStatePause || newState == store.TaskStateCancel))
	if changeFromPause {
		log.Debugf("task: %s changeFromPause, send new state change: %d to %d", this.Info.Id, oldState, newState)
	}
	if changeFromDoing {
		log.Debugf("task: %s changeFromDoing, send new state change: %d to %d", this.Info.Id, oldState, newState)
	}
	if this.Batch {
		return nil
	}
	return this.DB.SaveTaskInfo(this.Info)
}

func (this *Task) getProgressInfo() *types.ProgressInfo {

	pInfo := &types.ProgressInfo{
		TaskId:        this.Info.Id,
		Type:          this.Info.Type,
		Url:           this.Info.Url,
		StoreType:     this.Info.StoreType,
		FileName:      this.Info.FileName,
		FileHash:      this.Info.FileHash,
		FilePath:      this.Info.FilePath,
		Total:         this.Info.TotalBlockCount,
		CopyNum:       this.Info.CopyNum,
		FileSize:      this.Info.FileSize,
		RealFileSize:  this.Info.RealFileSize,
		Encrypt:       this.Info.Encrypt,
		Progress:      this.DB.FileProgress(this.Info.Id),
		NodeHostAddrs: this.Info.NodeHostAddrs,
		TaskState:     store.TaskState(this.Info.TaskState),
		ProgressState: types.TaskProgressState(this.Info.TranferState),
		CreatedAt:     this.Info.CreatedAt / consts.MILLISECOND_PER_SECOND,
		UpdatedAt:     this.Info.UpdatedAt / consts.MILLISECOND_PER_SECOND,
		Result:        this.Info.Result,
		ErrorCode:     this.Info.ErrorCode,
		ErrorMsg:      this.Info.ErrorMsg,
	}
	if this.Info.Type != store.TaskTypeUpload {
		return pInfo
	}
	if len(pInfo.Progress) == 0 {
		return pInfo
	}
	if len(this.Info.PrimaryNodes) == 0 {
		return pInfo
	}
	masterNode := this.Info.PrimaryNodes[0]
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
		Info: info,
		DB:   db,
		Lock: new(sync.RWMutex),
	}
	t.Info.Id = id
	return t
}
