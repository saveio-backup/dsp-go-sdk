package dispatch

import (
	sdkErr "github.com/saveio/dsp-go-sdk/error"
	"github.com/saveio/dsp-go-sdk/store"
	"github.com/saveio/dsp-go-sdk/task/upload"
)

type DispatchTask struct {
	*upload.UploadTask // base task
}

func NewDispatchTask(taskId string, taskType store.TaskType, db *store.TaskDB) *DispatchTask {
	dt := &DispatchTask{
		UploadTask: upload.NewUploadTask(taskId, taskType, db),
	}
	return dt
}

// DispatchTask. init a dispatch task
func InitDispatchTask(db *store.TaskDB) *DispatchTask {
	t := &DispatchTask{
		UploadTask: upload.InitUploadTask(db),
	}

	return t
}

// GetReferId. get reference download task id
func (this *DispatchTask) GetReferId() string {
	this.Lock.RLock()
	defer this.Lock.RUnlock()
	return this.Info.ReferId
}

func (this *DispatchTask) isNodeTaskDoingOrDone(nodeAddr string) (bool, error) {
	if this.IsTaskCancel() || this.IsTaskFailed() || this.IsTaskPaused() {
		return false, nil
	}
	doingOrdone, err := this.DB.IsNodeTaskDoingOrDone(this.GetId(), nodeAddr)
	if err != nil {
		return false, sdkErr.New(sdkErr.SET_FILEINFO_DB_ERROR, err.Error())
	}
	return doingOrdone, nil
}

func (this *DispatchTask) updateTaskNodeState(nodeAddr string, state store.TaskState) error {
	if this.IsTaskCancel() || this.IsTaskFailed() {
		return nil
	}
	err := this.DB.UpdateTaskProgressState(this.GetId(), nodeAddr, state)
	if err != nil {
		return sdkErr.New(sdkErr.SET_FILEINFO_DB_ERROR, err.Error())
	}
	return nil
}
