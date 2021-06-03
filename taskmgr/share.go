package taskmgr

import (
	"fmt"
	"runtime/debug"

	"github.com/saveio/dsp-go-sdk/consts"
	sdkErr "github.com/saveio/dsp-go-sdk/error"
	"github.com/saveio/dsp-go-sdk/store"
	"github.com/saveio/dsp-go-sdk/task/base"
	"github.com/saveio/dsp-go-sdk/task/share"
	"github.com/saveio/dsp-go-sdk/task/types"
	uTime "github.com/saveio/dsp-go-sdk/utils/time"
	"github.com/saveio/themis/common/log"
)

// NewShareTask. init a share task and cache it
func (this *TaskMgr) NewShareTask(taskId string) (*share.ShareTask, error) {
	this.shareTaskLock.Lock()
	defer this.shareTaskLock.Unlock()
	t, err := this.newShareTask(taskId)
	if err != nil {
		return nil, err
	}
	id := t.GetId()
	this.shareTasks[id] = t
	return t, nil

}

func (this *TaskMgr) GetShareTask(taskId string) *share.ShareTask {
	this.shareTaskLock.Lock()
	defer this.shareTaskLock.Unlock()
	tsk, ok := this.shareTasks[taskId]
	if ok {
		return tsk
	}
	t, _ := this.newShareTaskFromDB(taskId)
	if t == nil {
		return nil
	}
	if t.State() != store.TaskStateDone {
		// only cache unfinished task
		this.shareTasks[taskId] = t
	}
	return t
}

func (this *TaskMgr) GetShareTaskByFileHash(fileHash, walletAddr string) *share.ShareTask {
	this.shareTaskLock.RLock()
	defer this.shareTaskLock.RUnlock()
	fields := make(map[string]string)
	fields[store.TaskInfoFieldFileHash] = fileHash
	fields[store.TaskInfoFieldWalletAddress] = walletAddr

	for _, tsk := range this.shareTasks {

		walletAddr, ok := fields[store.TaskInfoFieldWalletAddress]
		if !ok || len(walletAddr) == 0 {
			continue
		}
		if tsk.GetWalletAddr() != walletAddr {
			continue
		}

		if fileHash, ok := fields[store.TaskInfoFieldFileHash]; ok && tsk.GetFileHash() == fileHash {
			return tsk
		}

	}

	return nil
}

// newShareTask. init a share task and cache it, thread-unsafe
func (this *TaskMgr) newShareTask(taskId string) (*share.ShareTask, error) {
	taskType := store.TaskTypeShare
	t := share.NewShareTask(taskId, taskType, this.db)
	if t == nil {
		return nil, sdkErr.New(sdkErr.NEW_TASK_FAILED, fmt.Sprintf("new task of type %d", taskType))
	}
	t.Mgr = this
	t.SetProgressNotifyCh(this.progress)
	return t, nil
}

// newShareTaskFromDB. Read file info from DB and recover a task by the file info.
func (this *TaskMgr) newShareTaskFromDB(id string) (*share.ShareTask, error) {
	info, err := this.db.GetTaskInfo(id)
	if err != nil {
		log.Errorf("get task from db, get file info failed, id: %s", id)
		return nil, err
	}
	if info == nil {
		log.Warnf("get task from db, recover task get file info is nil, id: %v", id)
		return nil, nil
	}
	if info.Type != store.TaskTypeShare {
		return nil, nil
	}
	if (store.TaskState(info.TaskState) == store.TaskStatePause ||
		store.TaskState(info.TaskState) == store.TaskStateDoing) &&
		info.UpdatedAt+consts.DOWNLOAD_FILE_TIMEOUT*1000 < uTime.GetMilliSecTimestamp() {
		log.Warnf("get share task from db, task: %s is expired, type: %d, updatedAt: %d", id, info.Type, info.UpdatedAt)
	}

	state := store.TaskState(info.TaskState)
	if state == store.TaskStatePrepare || state == store.TaskStateDoing ||
		state == store.TaskStateCancel || state == store.TaskStateIdle {
		state = store.TaskStatePause
	}

	t := share.InitShareTask(this.db)
	t.Mgr = this
	t.SetProgressNotifyCh(this.progress)
	t.SetInfo(info)
	t.SetInfoWithOptions(
		base.TaskState(state),
		base.TransferState(uint32(types.TaskPause)),
	)

	log.Debugf("get share task from db, task id %s, file name %s, task type %d, state %d",
		info.Id, info.FileName, info.Type, info.TaskState)
	return t, nil
}

// CleanTask. clean task from memory and DB
func (this *TaskMgr) CleanShareTask(taskId string) error {
	this.shareTaskLock.Lock()
	defer this.shareTaskLock.Unlock()
	delete(this.shareTasks, taskId)
	log.Debugf("clean task %s, %s", taskId, debug.Stack())
	err := this.db.DeleteTaskInfo(taskId)
	if err != nil {
		return sdkErr.NewWithError(sdkErr.SET_FILEINFO_DB_ERROR, err)
	}
	return nil
}

// DeleteShareTask. delete task with task id from memory. runtime delete action.
func (this *TaskMgr) DeleteShareTask(taskId string) {
	this.shareTaskLock.Lock()
	defer this.shareTaskLock.Unlock()
	log.Debugf("delete task %s, %s", taskId, debug.Stack())
	delete(this.shareTasks, taskId)
}
