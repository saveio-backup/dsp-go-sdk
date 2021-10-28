package taskmgr

import (
	"fmt"
	"runtime/debug"

	sdkErr "github.com/saveio/dsp-go-sdk/error"
	"github.com/saveio/dsp-go-sdk/store"
	"github.com/saveio/dsp-go-sdk/task/base"
	"github.com/saveio/dsp-go-sdk/task/dispatch"
	"github.com/saveio/dsp-go-sdk/task/types"
	"github.com/saveio/themis/common/log"
)

// NewDispatchTask. init a dispatch task and cache it
func (this *TaskMgr) NewDispatchTask(taskId string) (*dispatch.DispatchTask, error) {
	this.dispatchTaskLock.Lock()
	defer this.dispatchTaskLock.Unlock()
	t, err := this.newDispatchTask(taskId)
	if err != nil {
		return nil, err
	}
	id := t.GetId()
	this.dispatchTasks[id] = t
	return t, nil

}

func (this *TaskMgr) GetDispatchTask(taskId string) *dispatch.DispatchTask {
	this.dispatchTaskLock.Lock()
	defer this.dispatchTaskLock.Unlock()
	tsk, ok := this.dispatchTasks[taskId]
	if ok {
		return tsk
	}
	t, _ := this.newDispatchTaskFromDB(taskId)
	if t == nil {
		return nil
	}
	if t.State() != store.TaskStateDone {
		// only cache unfinished task
		this.dispatchTasks[taskId] = t
	}

	return t
}

func (this *TaskMgr) GetDispatchTaskByReferId(referId string) *dispatch.DispatchTask {
	this.dispatchTaskLock.Lock()
	defer this.dispatchTaskLock.Unlock()

	for _, t := range this.dispatchTasks {
		if t.GetReferId() == referId {
			return t
		}
	}

	return nil
}

func (this *TaskMgr) DispatchTask(origTaskId, fileHashStr string) {
	if ok := this.dispatchExistTask(fileHashStr); ok {
		return
	}

	t, err := this.newDispatchTask("")
	log.Debugf("new dispatch task id %s dispatch file %s, type %v", t.GetId(), fileHashStr, t.GetTaskType())
	if err != nil {
		log.Errorf("dispatch task err new dispatch failed origin task %s, fileHash %s", origTaskId, fileHashStr)
		return
	}
	refDownloadTask := this.GetDownloadTask(origTaskId)
	if refDownloadTask == nil {
		log.Errorf("dispatch file %s, origin download task %s is nil", fileHashStr, origTaskId)
		return
	}
	log.Debugf("dispatchTask original download task id %v, file %s, original task %v",
		origTaskId, fileHashStr, refDownloadTask)
	log.Debugf("BlocksRoot : %v, Prefix : %v, StoreTx : %v, "+
		"StoreTxHeight : %v, CopyNum : %v, FileOwner : %v, TotalBlockCnt : %v ",
		refDownloadTask.GetBlocksRoot(),
		refDownloadTask.GetPrefix(),
		refDownloadTask.GetStoreTx(),
		refDownloadTask.GetStoreTxHeight(),
		refDownloadTask.GetCopyNum(),
		refDownloadTask.GetFileOwner(),
		refDownloadTask.GetTotalBlockCnt(),
	)

	if err := t.SetInfoWithOptions(
		base.FileHash(fileHashStr),
		base.BlocksRoot(refDownloadTask.GetBlocksRoot()),
		base.Walletaddr(this.chain.WalletAddress()),
		base.Prefix(string(refDownloadTask.GetPrefix())),
		base.StoreTx(refDownloadTask.GetStoreTx()),
		base.StoreTxHeight(refDownloadTask.GetStoreTxHeight()),
		base.ReferId(origTaskId),
		base.CopyNum(uint32(refDownloadTask.GetCopyNum())),
		base.FileOwner(refDownloadTask.GetFileOwner()),
		base.TotalBlockCnt(uint64(refDownloadTask.GetTotalBlockCnt()))); err != nil {
		log.Errorf("dispatch task %s setup info failed origin task %s, fileHash %s",
			t.GetId(), origTaskId, fileHashStr)
		return
	}

	this.dispatchTaskLock.Lock()
	this.dispatchTasks[t.GetId()] = t
	this.dispatchTaskLock.Unlock()

	provedCh, _ := this.fileProvedCh.Load(fileHashStr)
	if provedCh != nil {
		ch := provedCh.(chan uint32)
		log.Debugf("dispatch task %s wait for file %s pdp", t.GetId(), fileHashStr)
		provedHeight := <-ch
		this.fileProvedCh.Delete(fileHashStr)
		log.Debugf("dispatch task %s of file %s has proved at %v", t.GetId(), fileHashStr, provedHeight)
	}

	go func() {
		err := t.Start()
		if err != nil {
			log.Errorf("start dispatch task %s err %v", t.GetId(), err)
		}
	}()
}

func (this *TaskMgr) AddDispatchTaskToRetry(task *dispatch.DispatchTask) {
	this.dispatchTaskLock.Lock()
	defer this.dispatchTaskLock.Unlock()
	if task == nil {
		return
	}
	taskId := task.GetId()
	log.Debugf("add dispatch task %s to retry list", task.GetId())
	this.retryDispatchTaskTs[taskId] = task.GetRetryAt()
}

func (this *TaskMgr) DeleteRetryDispatchTask(taskId string) {
	this.dispatchTaskLock.Lock()
	defer this.dispatchTaskLock.Unlock()
	delete(this.retryDispatchTaskTs, taskId)
}

// CleanTask. clean task from memory and DB
func (this *TaskMgr) CleanDispatchTask(taskId string) error {
	this.dispatchTaskLock.Lock()
	defer this.dispatchTaskLock.Unlock()
	delete(this.dispatchTasks, taskId)
	delete(this.retryDispatchTaskTs, taskId)
	log.Debugf("clean dispatch task %s", taskId)
	err := this.db.DeleteTaskInfo(taskId)
	if err != nil {
		return sdkErr.NewWithError(sdkErr.SET_FILEINFO_DB_ERROR, err)
	}
	return nil
}

// DeleteDispatchTask. delete task with task id from memory. runtime delete action.
func (this *TaskMgr) DeleteDispatchTask(taskId string) {
	this.dispatchTaskLock.Lock()
	defer this.dispatchTaskLock.Unlock()
	log.Debugf("delete task %s, %s", taskId, debug.Stack())
	delete(this.dispatchTasks, taskId)
	delete(this.retryDispatchTaskTs, taskId)
}

// CleanPocTask. clean task from memory and DB
func (this *TaskMgr) CleanPocTask(taskId string) error {
	this.pocTaskLock.Lock()
	defer this.pocTaskLock.Unlock()
	delete(this.pocTasks, taskId)
	log.Debugf("clean poc task %s", taskId)
	err := this.db.DeleteTaskInfo(taskId)
	if err != nil {
		return sdkErr.NewWithError(sdkErr.SET_FILEINFO_DB_ERROR, err)
	}
	return nil
}

// EmitDispatchResult. emit result or error async
func (this *TaskMgr) EmitDispatchResult(taskId string, result interface{}, sdkErr *sdkErr.Error) {
	tsk := this.GetDispatchTask(taskId)
	if tsk == nil {
		log.Errorf("[TaskMgr EmitResult] emit result get no task")
		return
	}
	if this.progress == nil {
		log.Errorf("[TaskMgr EmitResult] progress is nil")
		return
	}
	if sdkErr != nil {
		err := tsk.SetResult(nil, sdkErr.Code, sdkErr.Message)
		if err != nil {
			log.Errorf("set task state err %s, %s", taskId, err)
		}
		// this.AddDispatchTaskToRetry(tsk)
		// log.Debugf("EmitResult err %v, %v", err, sdkErr)
		// if this.retryTaskTicker != nil {
		// 	this.retryTaskTicker.Run()
		// }
	} else {
		// this.DeleteRetryDispatchTask(taskId)
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
	pInfo := tsk.GetProgressInfo()
	this.progress <- pInfo
}

func (this *TaskMgr) HasRunningDispatchTask() bool {
	this.dispatchTaskLock.RLock()
	defer this.dispatchTaskLock.RUnlock()
	for _, tsk := range this.dispatchTasks {
		if prepare, doing := tsk.IsTaskPreparingOrDoing(); prepare || doing {
			return true
		}
	}

	return false
}

// newDispatchTask. init a dispatch task and cache it, thread-unsafe
func (this *TaskMgr) newDispatchTask(taskId string) (*dispatch.DispatchTask, error) {
	taskType := store.TaskTypeDispatch

	t := dispatch.NewDispatchTask(taskId, taskType, this.db)
	if t == nil {
		return nil, sdkErr.New(sdkErr.NEW_TASK_FAILED, fmt.Sprintf("new task of type %d", taskType))
	}
	t.Mgr = this
	t.SetProgressNotifyCh(this.progress, this.progressCtx)

	return t, nil
}

// newDispatchTaskFromDB. Read file info from DB and recover a task by the file info.
func (this *TaskMgr) newDispatchTaskFromDB(id string) (*dispatch.DispatchTask, error) {
	info, err := this.db.GetTaskInfo(id)
	if err != nil {
		log.Errorf("new dispatch task get task from db, get file info failed, id: %s", id)
		return nil, err
	}
	if info == nil {
		log.Warnf("new dispatch task  get task from db, recover task get file info is nil, id: %v", id)
		return nil, nil
	}
	if info.Type != store.TaskTypeDispatch {
		return nil, nil
	}

	state := store.TaskState(info.TaskState)
	if state == store.TaskStatePrepare || state == store.TaskStateDoing ||
		state == store.TaskStateCancel || state == store.TaskStateIdle {
		state = store.TaskStatePause
	}
	t := dispatch.InitDispatchTask(this.db)
	t.Id = id
	t.Mgr = this
	t.SetProgressNotifyCh(this.progress, this.progressCtx)
	// t.SetInfo(info)

	t.SetInfoWithOptions(
		base.TaskState(state),
		base.TransferState(uint32(types.TaskPause)),
	)
	log.Debugf("get dispatch task from db task id %s, file name %s, task type %d, state %d",
		info.Id, info.FileName, info.Type, info.TaskState)
	return t, nil
}

// dispatchExistTask. Find exist dispatch task. If exists, dispatch it.
func (this *TaskMgr) dispatchExistTask(fileHashStr string) bool {
	this.dispatchTaskLock.RLock()
	defer this.dispatchTaskLock.RUnlock()

	for _, t := range this.dispatchTasks {
		if t.GetFileHash() != fileHashStr {
			continue
		}
		if t.GetWalletAddr() != this.chain.WalletAddress() {
			continue
		}

		if _, doing := t.IsTaskPreparingOrDoing(); doing {
			return true
		}
		log.Debugf("get exist dispatch task %s for file %s", t.GetId(), fileHashStr)

		go t.Start()
		return true
	}
	return false
}
