package taskmgr

import (
	"fmt"
	"runtime/debug"
	"sort"

	"github.com/saveio/dsp-go-sdk/consts"
	sdkErr "github.com/saveio/dsp-go-sdk/error"
	"github.com/saveio/dsp-go-sdk/store"
	"github.com/saveio/dsp-go-sdk/task/base"
	"github.com/saveio/dsp-go-sdk/task/download"
	"github.com/saveio/dsp-go-sdk/task/types"
	"github.com/saveio/dsp-go-sdk/types/uint64pair"
	uTime "github.com/saveio/dsp-go-sdk/utils/time"
	"github.com/saveio/themis/common/log"
)

// NewDownloadTask. init a download task and cache it
func (this *TaskMgr) NewDownloadTask(taskId string) (*download.DownloadTask, error) {
	this.downloadTaskLock.Lock()
	defer this.downloadTaskLock.Unlock()
	t, err := this.newDownloadTask(taskId)
	if err != nil {
		return nil, err
	}
	this.downloadTasks[t.GetId()] = t
	return t, nil
}

func (this *TaskMgr) GetDownloadTask(taskId string) *download.DownloadTask {
	this.downloadTaskLock.Lock()
	defer this.downloadTaskLock.Unlock()
	tsk, ok := this.downloadTasks[taskId]
	if ok {
		return tsk
	}
	t, _ := this.newDownloadTaskFromDB(taskId)
	if t == nil {
		return nil
	}
	if t.State() != store.TaskStateDone {
		// only cache unfinished task
		this.downloadTasks[taskId] = t
	}

	return t
}

func (this *TaskMgr) GetDownloadTaskImpl(taskId string) interface{} {
	return this.GetDownloadTask(taskId)
}

// TaskExist. Check if task exist in memory
func (this *TaskMgr) IsDownloadTaskExist(taskId string) bool {
	this.downloadTaskLock.RLock()
	defer this.downloadTaskLock.RUnlock()
	if len(taskId) == 0 {
		return false
	}
	_, ok := this.downloadTasks[taskId]
	return ok
}

// TaskExist. Check if task exist in memory
func (this *TaskMgr) GetDownloadTaskByFileHash(fileHash string) *download.DownloadTask {
	this.downloadTaskLock.RLock()
	defer this.downloadTaskLock.RUnlock()
	if len(fileHash) == 0 {
		return nil
	}
	for _, tsk := range this.downloadTasks {
		if tsk.GetFileHash() == fileHash {
			return tsk
		}
	}
	return nil
}

// EmitResult. emit result or error async
func (this *TaskMgr) EmitDownloadResult(taskId string, ret interface{}, sdkErr *sdkErr.Error) {
	v := this.GetDownloadTask(taskId)
	if v == nil {
		log.Errorf("[TaskMgr EmitResult] emit result get no task")
		return
	}
	if this.progress == nil {
		log.Errorf("[TaskMgr EmitResult] progress is nil")
		return
	}
	if sdkErr != nil {
		err := v.SetResult(nil, sdkErr.Code, sdkErr.Message)
		if err != nil {
			log.Errorf("set task state err %s, %s", taskId, err)
		}
		this.AddDownloadTaskToRetry(v)
		log.Debugf("emit download failed result err %v, sdk err %v", err, sdkErr)
		if this.retryTaskTicker != nil {
			this.retryTaskTicker.Run()
		}
	} else {
		this.DeleteRetryDownloadTask(taskId)
		log.Debugf("emit download success result %v", ret)
		err := v.SetResult(ret, 0, "")
		if err != nil {
			log.Errorf("set task result err %s, %s", taskId, err)
		}
	}
	pInfo := v.GetProgressInfo()
	this.progress <- pInfo
}

func (this *TaskMgr) AddDownloadTaskToRetry(task *download.DownloadTask) {
	this.downloadTaskLock.Lock()
	defer this.downloadTaskLock.Unlock()
	if task == nil {
		return
	}
	taskId := task.GetId()
	log.Debugf("task %s need retry", task.GetId())
	this.retryDownloadTaskTs[taskId] = task.GetRetryAt()
}

func (this *TaskMgr) DeleteRetryDownloadTask(taskId string) {
	this.downloadTaskLock.Lock()
	defer this.downloadTaskLock.Unlock()
	delete(this.retryDownloadTaskTs, taskId)
}

// DeleteUploadTask. delete task with task id from memory. runtime delete action.
func (this *TaskMgr) DeleteDownloadTask(taskId string) {
	this.downloadTaskLock.Lock()
	defer this.downloadTaskLock.Unlock()
	log.Debugf("delete task %s, %s", taskId, debug.Stack())
	delete(this.downloadTasks, taskId)
	delete(this.retryDownloadTaskTs, taskId)
}

// CleanTask. clean task from memory and DB
func (this *TaskMgr) CleanDownloadTask(taskId string) error {
	this.downloadTaskLock.Lock()
	defer this.downloadTaskLock.Unlock()
	delete(this.downloadTasks, taskId)
	delete(this.retryDownloadTaskTs, taskId)
	log.Debugf("clean task %s, %s", taskId, debug.Stack())
	err := this.db.DeleteTaskInfo(taskId)
	if err != nil {
		return sdkErr.NewWithError(sdkErr.SET_FILEINFO_DB_ERROR, err)
	}
	return nil
}

func (this *TaskMgr) HasDownloadRetryTask() bool {
	this.downloadTaskLock.RLock()
	defer this.downloadTaskLock.RUnlock()
	return len(this.retryDownloadTaskTs) > 0
}

func (this *TaskMgr) GetDownloadTasksToRetry() []string {
	this.downloadTaskLock.Lock()
	defer this.downloadTaskLock.Unlock()
	if len(this.retryDownloadTaskTs) == 0 {
		return nil
	}
	list := make(uint64pair.Uint64PairList, 0)
	for key, value := range this.retryDownloadTaskTs {
		list = append(list, uint64pair.Uint64Pair{
			Key:   key,
			Value: value,
		})
	}
	sort.Sort(list)
	taskIds := make([]string, 0)
	for _, l := range list {
		tsk, ok := this.downloadTasks[l.Key]
		if !ok {
			continue
		}
		if !tsk.NeedRetry() {
			continue
		}
		taskIds = append(taskIds, l.Key)
	}
	return taskIds
}

func (this *TaskMgr) DeleteDownloadedFile(taskId string) error {
	if len(taskId) == 0 {
		return sdkErr.New(sdkErr.DELETE_FILE_FAILED, "delete taskId is empty")
	}
	tsk := this.GetDownloadTask(taskId)
	if tsk == nil {
		return sdkErr.New(sdkErr.DELETE_FILE_FAILED, "task %s is nil", taskId)
	}
	fileHashStr := tsk.GetFileHash()
	filePath := tsk.GetFilePath()
	err := this.fs.DeleteFile(fileHashStr, filePath)
	if err != nil {
		log.Errorf("fs delete file: %s, path: %s, err: %s", fileHashStr, filePath, err)
		return err
	}
	log.Debugf("delete local file success fileHash:%s, path:%s", fileHashStr, filePath)
	return this.CleanDownloadTask(taskId)
}

func (this *TaskMgr) HasRunningDownloadTask() bool {
	this.downloadTaskLock.RLock()
	defer this.downloadTaskLock.RUnlock()
	for _, tsk := range this.downloadTasks {

		if prepare, doing := tsk.IsTaskPreparingOrDoing(); prepare || doing {
			return true
		}
	}
	return false
}

func (this *TaskMgr) RecoverUndoneDownloadTask() error {
	this.downloadTaskLock.Lock()
	defer this.downloadTaskLock.Unlock()
	downloadTaskIds, err := this.db.UndoneList(store.TaskTypeDownload)
	if err != nil {
		return err
	}

	for _, id := range downloadTaskIds {
		t, err := this.newDownloadTaskFromDB(id)
		if err != nil {
			continue
		}
		if t == nil || t.State() == store.TaskStateDone {
			log.Warnf("can't recover this task %s", id)
			this.db.RemoveFromUndoneList(nil, id, store.TaskTypeDownload)
			continue
		}
		this.downloadTasks[id] = t
	}

	return nil
}

// GetDoingTaskNum. calculate the sum of task number with task type
func (this *TaskMgr) GetDoingDownloadTaskNum() uint32 {
	this.downloadTaskLock.RLock()
	defer this.downloadTaskLock.RUnlock()
	cnt := uint32(0)
	for _, t := range this.downloadTasks {
		if t.State() == store.TaskStateDoing {
			cnt++
		}
	}
	return cnt
}

// newDownloadTask. init a download task and cache it, thread-unsafe
func (this *TaskMgr) newDownloadTask(taskId string) (*download.DownloadTask, error) {
	taskType := store.TaskTypeDownload

	t := download.NewDownloadTask(taskId, taskType, this.db)
	if t == nil {
		return nil, sdkErr.New(sdkErr.NEW_TASK_FAILED, fmt.Sprintf("new task of type %d", taskType))
	}
	t.Mgr = this
	t.SetProgressNotifyCh(this.progress, this.progressCtx)
	log.Debugf("new download task %v", taskId)
	return t, nil
}

// NewDownloadTaskFromDB. Read file info from DB and recover a task by the file info.
func (this *TaskMgr) newDownloadTaskFromDB(id string) (*download.DownloadTask, error) {
	info, err := this.db.GetTaskInfo(id)
	if err != nil {
		log.Errorf("get task from db, get file info failed, id: %s", id)
		return nil, err
	}
	if info == nil {
		log.Warnf("get task from db, recover task get file info is nil, id: %v", id)
		return nil, nil
	}
	if info.Type != store.TaskTypeDownload {
		return nil, nil
	}
	if (store.TaskState(info.TaskState) == store.TaskStatePause ||
		store.TaskState(info.TaskState) == store.TaskStateDoing) &&
		info.UpdatedAt+consts.DOWNLOAD_FILE_TIMEOUT*1000 < uTime.GetMilliSecTimestamp() {
		log.Warnf("get download task from db, task: %s is expired, type: %d, updatedAt: %d", id, info.Type, info.UpdatedAt)
	}
	sessions, err := this.db.GetFileSessions(id)
	if err != nil {
		log.Errorf("get task from db, set task session: %s", err)
		return nil, err
	}
	state := store.TaskState(info.TaskState)
	if state == store.TaskStatePrepare || state == store.TaskStateDoing ||
		state == store.TaskStateCancel || state == store.TaskStateIdle {
		state = store.TaskStatePause
	}

	t := download.InitDownloadTask(this.db)
	t.Mgr = this
	t.SetProgressNotifyCh(this.progress, this.progressCtx)
	sessionIds := make(map[string]string)
	for _, session := range sessions {
		sessionIds[session.WalletAddr] = session.SessionId
	}
	// t.SetInfo(info)
	t.SetInfoWithOptions(
		base.TaskState(state),
		base.TransferState(uint32(types.TaskPause)),
		base.PeerToSessionIds(sessionIds),
	)
	log.Debugf("get donload task from db, task id %s, file name %s, task type %d, state %d",
		info.Id, info.FileName, info.Type, info.TaskState)
	return t, nil
}
