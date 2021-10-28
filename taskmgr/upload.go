package taskmgr

import (
	"fmt"
	"runtime/debug"
	"sort"

	"github.com/saveio/dsp-go-sdk/consts"
	sdkErr "github.com/saveio/dsp-go-sdk/error"
	"github.com/saveio/dsp-go-sdk/store"
	"github.com/saveio/dsp-go-sdk/task/base"
	"github.com/saveio/dsp-go-sdk/task/types"
	"github.com/saveio/dsp-go-sdk/task/upload"
	"github.com/saveio/dsp-go-sdk/types/uint64pair"
	"github.com/saveio/dsp-go-sdk/utils/crypto"
	uTime "github.com/saveio/dsp-go-sdk/utils/time"
	"github.com/saveio/themis/common/log"
)

// NewUploadTask. init a upload task and cache it
func (this *TaskMgr) NewUploadTask(taskId string) (*upload.UploadTask, error) {
	this.uploadTaskLock.Lock()
	defer this.uploadTaskLock.Unlock()
	t, err := this.newUploadTask(taskId)
	if err != nil {
		return nil, err
	}
	this.uploadTasks[t.GetId()] = t
	return t, nil

}

func (this *TaskMgr) GetUploadTask(taskId string) *upload.UploadTask {
	this.uploadTaskLock.Lock()
	defer this.uploadTaskLock.Unlock()
	tsk, ok := this.uploadTasks[taskId]
	if ok {
		return tsk
	}
	t, _ := this.newUploadTaskFromDB(taskId)
	if t == nil {
		return nil
	}
	if t.State() != store.TaskStateDone {
		// only cache unfinished task
		this.uploadTasks[taskId] = t
	}
	return t
}

// UploadTaskExist. check if upload task is existed by filePath hash string
func (this *TaskMgr) UploadTaskExist(filePath string) (bool, error) {
	this.uploadTaskLock.RLock()
	defer this.uploadTaskLock.RUnlock()

	// check uploading task, if filePath match, task is exist
	for _, tsk := range this.uploadTasks {
		if tsk.GetFilePath() == filePath {
			log.Debugf("upload task %s,  filepath: %s",
				tsk.GetId(), filePath)
			return true, nil
		}
	}

	// check uploading task simple checksum, if matched, task is exist
	checksum, err := crypto.GetSimpleChecksumOfFile(filePath)
	if err != nil {
		return true, sdkErr.NewWithError(sdkErr.GET_SIMPLE_CHECKSUM_ERROR, err)
	}
	for _, tsk := range this.uploadTasks {
		if tsk.GetSimpleChecksum() == checksum {
			log.Debugf("upload task %s, exist checksum: %s, filepath: %s",
				tsk.GetId(), checksum, filePath)
			return true, nil
		}
	}

	// get uploaded task from db
	fields := make(map[string]string, 0)
	fields[store.TaskInfoFieldWalletAddress] = this.chain.WalletAddress()
	fields[store.TaskInfoFieldFilePath] = filePath
	fields[store.TaskInfoFieldSimpleCheckSum] = checksum
	taskInfo := this.db.GetUploadTaskByFields(fields)
	if taskInfo == nil {
		return false, nil
	}
	// taskId := this.TaskId(filePath, this.chain.WalletAddress(), store.TaskTypeUpload)
	// if len(taskId) == 0 {
	// 	checksum, err := utils.GetSimpleChecksumOfFile(filePath)
	// 	if err != nil {
	// 		return true, sdkErr.NewWithError(sdkErr.GET_SIMPLE_CHECKSUM_ERROR, err)
	// 	}
	// 	taskId = this.taskMgr.TaskId(checksum, this.chain.WalletAddress(), store.TaskTypeUpload)
	// 	if len(taskId) == 0 {
	// 		return false, nil
	// 	}
	// 	log.Debugf("upload task exist checksum: %s, filepath: %s", checksum, filePath)
	// }
	// taskInfo, err := this.taskMgr.GetTaskInfoCopy(taskId)
	// if err != nil || taskInfo == nil {
	// 	return true, sdkErr.New(sdkErr.GET_FILEINFO_FROM_DB_ERROR, "get file task info is nil %s", err)
	// }
	// check if uploaded task is expired
	now, err := this.chain.GetCurrentBlockHeight()
	if err != nil {
		log.Errorf("get current block height err %s", err)
		return true, err
	}
	if taskInfo.ExpiredHeight > uint64(now) {
		return true, nil
	}
	if len(taskInfo.FileHash) == 0 {
		return true, sdkErr.New(sdkErr.GET_FILEINFO_FROM_DB_ERROR, "file hash not found for %s", taskInfo.Id)
	}
	log.Debugf("upload task exist, but expired, delete it")
	// delete expired task, enable upload it again
	err = this.cleanUploadTask(taskInfo.Id)
	if err != nil {
		return true, err
	}
	return false, nil
}

// EmitUploadResult. emit result or error async
func (this *TaskMgr) EmitUploadResult(taskId string, result interface{}, sdkErr *sdkErr.Error) {
	tsk := this.GetUploadTask(taskId)
	if tsk == nil {
		log.Errorf("stop emit upload result, task %s not found", taskId)
		return
	}

	if sdkErr != nil {
		err := tsk.SetResult(nil, sdkErr.Code, sdkErr.Message)
		if err != nil {
			log.Errorf("set task state err %s, %s", taskId, err)
		}
		this.AddUploadTaskToRetry(tsk)
		log.Debugf("EmitResult err %v, %v", err, sdkErr)
		if this.retryTaskTicker != nil {
			this.retryTaskTicker.Run()
		}
	} else {
		this.DeleteRetryUploadTask(taskId)
		log.Debugf("EmitResult ret %v ret == nil %t", result, result == nil)
		err := tsk.SetResult(result, 0, "")
		if err != nil {
			log.Errorf("set task result err %s, %s", taskId, err)
		}
		if this.progressTicker != nil && tsk.GetCopyNum() > 0 {
			log.Debugf("run progress ticker")
			this.progressTicker.Run()
		}
	}

	if this.progress == nil {
		log.Errorf("[TaskMgr EmitResult] progress is nil")
		return
	}
	pInfo := tsk.GetProgressInfo()
	this.progress <- pInfo
}

// CleanTask. clean task from memory and DB
func (this *TaskMgr) CleanUploadTask(taskId string) error {
	this.uploadTaskLock.Lock()
	defer this.uploadTaskLock.Unlock()
	return this.cleanUploadTask(taskId)

}

// DeleteUploadTask. delete task with task id from memory. runtime delete action.
func (this *TaskMgr) DeleteUploadTask(taskId string) {
	this.uploadTaskLock.Lock()
	defer this.uploadTaskLock.Unlock()
	log.Debugf("delete task %s, %s", taskId, debug.Stack())
	delete(this.uploadTasks, taskId)
	delete(this.retryUploadTaskTs, taskId)
}

func (this *TaskMgr) AddUploadTaskToRetry(task *upload.UploadTask) {
	this.uploadTaskLock.Lock()
	defer this.uploadTaskLock.Unlock()
	if task == nil {
		return
	}
	taskId := task.GetId()
	log.Debugf("task %s need retry", task.GetId())
	this.retryUploadTaskTs[taskId] = task.GetRetryAt()
}

func (this *TaskMgr) DeleteRetryUploadTask(taskId string) {
	this.uploadTaskLock.Lock()
	defer this.uploadTaskLock.Unlock()
	delete(this.retryUploadTaskTs, taskId)
}

// PauseDuplicatedTask. check if a uploading task has contained the file, if exist pause it
func (this *TaskMgr) PauseDuplicatedUploadTask(taskId, fileHashStr string) bool {
	log.Debugf("PauseDuplicatedUploadTask +++")
	this.uploadTaskLock.Lock()
	defer this.uploadTaskLock.Unlock()
	log.Debugf("PauseDuplicatedUploadTask +++---")
	tsk := this.uploadTasks[taskId]
	var tskCreatedAt uint64
	if tsk != nil {
		tskCreatedAt = tsk.GetCreatedAt()
	} else {
		tskCreatedAt = uTime.GetMilliSecTimestamp()
	}
	log.Debugf("pause duplicated check start %s", taskId)
	defer func() {
		log.Debugf("pause duplicated check done %s", taskId)

	}()

	for _, t := range this.uploadTasks {
		if t.GetFileHash() != fileHashStr {
			continue
		}
		if t.GetId() == taskId {
			// skip self
			continue
		}
		if t.GetTaskType() != store.TaskTypeUpload {
			continue
		}
		log.Debugf("duplicated check %s, %s %v", t.GetId(), taskId, t.State())
		if t.State() != store.TaskStateDoing {
			continue
		}
		log.Debugf("fileHashStr %s, taskId %s , newTaskId %s, taskCreatedAt: %d, newTaskCreatedAt: %d",
			fileHashStr, t.GetId(), taskId, t.GetCreatedAt(), tskCreatedAt)
		if err := tsk.Pause(); err != nil {
			continue
		}
		return true
	}
	return false
}

func (this *TaskMgr) HasUploadRetryTask() bool {
	this.uploadTaskLock.RLock()
	defer this.uploadTaskLock.RUnlock()
	return len(this.retryUploadTaskTs) > 0
}

func (this *TaskMgr) GetUploadTasksToRetry() []string {
	this.uploadTaskLock.Lock()
	defer this.uploadTaskLock.Unlock()
	if len(this.retryUploadTaskTs) == 0 {
		return nil
	}
	list := make(uint64pair.Uint64PairList, 0)
	for key, value := range this.retryUploadTaskTs {
		list = append(list, uint64pair.Uint64Pair{
			Key:   key,
			Value: value,
		})
	}
	sort.Sort(list)
	taskIds := make([]string, 0)
	for _, l := range list {
		tsk, ok := this.uploadTasks[l.Key]
		if !ok {
			continue
		}
		if !tsk.NeedRetry() {
			log.Debugf("task %s not need to retry", tsk.GetId())
			continue
		}
		taskIds = append(taskIds, l.Key)
	}
	return taskIds
}

// RecoverUndoneTask. recover unfinished task from DB
func (this *TaskMgr) RecoverUndoneUploadTask() error {
	this.uploadTaskLock.Lock()
	defer this.uploadTaskLock.Unlock()
	if this.db == nil {
		return nil
	}
	taskIds, err := this.db.UndoneList(store.TaskTypeUpload)
	if err != nil {
		return err
	}

	for _, id := range taskIds {
		t, err := this.newUploadTaskFromDB(id)
		log.Infof("new upload task %v from db %v err %v", id, t, err)
		if err != nil {
			continue
		}
		if t == nil || t.State() == store.TaskStateDone {
			log.Warnf("can't recover this task %s", id)
			this.db.RemoveFromUndoneList(nil, id, store.TaskTypeUpload)
			continue
		}
		this.uploadTasks[id] = t
	}

	return nil
}

func (this *TaskMgr) HasRunningUploadTask() bool {
	this.uploadTaskLock.RLock()
	defer this.uploadTaskLock.RUnlock()
	for _, tsk := range this.uploadTasks {
		if prepare, doing := tsk.IsTaskPreparingOrDoing(); prepare || doing {
			return true
		}
	}
	return false

}

// GetDoingTaskNum. calculate the sum of task number with task type
func (this *TaskMgr) GetDoingUploadTaskNum() uint32 {
	this.uploadTaskLock.RLock()
	defer this.uploadTaskLock.RUnlock()
	cnt := uint32(0)
	for _, t := range this.uploadTasks {
		if t.State() == store.TaskStateDoing {
			cnt++
		}
	}
	return cnt
}

// newUploadTask. init a upload task and cache it, thread-unsafe
func (this *TaskMgr) newUploadTask(taskId string) (*upload.UploadTask, error) {
	taskType := store.TaskTypeUpload
	// if len(taskId) > 0 {
	// 	if _, ok := this.uploadTasks[taskId]; ok {
	// 		return nil, sdkErr.New(sdkErr.NEW_TASK_FAILED, fmt.Sprintf("task %s already exist", taskId))
	// 	}
	// 	info, _ := this.db.GetTaskInfo(taskId)
	// 	if info != nil {
	// 		return nil, sdkErr.New(sdkErr.NEW_TASK_FAILED, fmt.Sprintf("task %s already exist", taskId))
	// 	}
	// }
	t := upload.NewUploadTask(taskId, taskType, this.db)
	if t == nil {
		return nil, sdkErr.New(sdkErr.NEW_TASK_FAILED, fmt.Sprintf("new task of type %d", taskType))
	}
	t.Mgr = this
	t.SetProgressNotifyCh(this.progress, this.progressCtx)
	return t, nil
}

// newUploadTaskFromDB. Read file info from DB and recover a task by the file info.
func (this *TaskMgr) newUploadTaskFromDB(id string) (*upload.UploadTask, error) {
	info, err := this.db.GetTaskInfo(id)
	log.Debugf("get info %v from %v", info, id)
	if err != nil {
		log.Errorf("new upload task get task from db, get file info failed, id: %s", id)
		return nil, err
	}
	if info == nil {
		log.Warnf("new upload task get task from db, recover task get file info is nil, id: %v", id)
		return nil, nil
	}
	if info.Type != store.TaskTypeUpload {
		return nil, nil
	}
	if (store.TaskState(info.TaskState) == store.TaskStatePause ||
		store.TaskState(info.TaskState) == store.TaskStateDoing) &&
		info.UpdatedAt+consts.DOWNLOAD_FILE_TIMEOUT*1000 < uTime.GetMilliSecTimestamp() {
		log.Warnf("get upload task from db, task: %s is expired, type: %d, task state %v updatedAt: %d", id, info.Type, info.TaskState, info.UpdatedAt)
	}

	state := store.TaskState(info.TaskState)
	if state == store.TaskStatePrepare || state == store.TaskStateDoing ||
		state == store.TaskStateCancel || state == store.TaskStateIdle {
		state = store.TaskStatePause
	}

	t := upload.InitUploadTask(this.db)
	t.Id = id
	t.Mgr = this
	t.SetProgressNotifyCh(this.progress, this.progressCtx)
	t.SetInfoWithOptions(
		base.TaskState(state),
		base.TransferState(uint32(types.TaskPause)),
	)

	log.Debugf("new upload task get task from db, task id %s, file name %s, task type %d, state %d",
		info.Id, info.FileName, info.Type, info.TaskState)
	return t, nil
}

func (this *TaskMgr) cleanUploadTask(taskId string) error {
	delete(this.uploadTasks, taskId)
	delete(this.retryUploadTaskTs, taskId)
	log.Debugf("clean upload task %s", taskId)
	err := this.db.DeleteTaskInfo(taskId)
	if err != nil {
		log.Errorf("delete task info %s err %s", taskId, err)
		return sdkErr.NewWithError(sdkErr.SET_FILEINFO_DB_ERROR, err)
	}
	return nil
}
