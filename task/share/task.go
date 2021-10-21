package share

import (
	"sync"

	sdkErr "github.com/saveio/dsp-go-sdk/error"
	"github.com/saveio/dsp-go-sdk/store"
	"github.com/saveio/dsp-go-sdk/task/base"
)

type ShareTask struct {
	*base.Task // embedded base task
}

func NewShareTask(taskId string, taskType store.TaskType, db *store.TaskDB) *ShareTask {
	dt := &ShareTask{
		Task: base.NewTask(taskId, taskType, db),
	}
	return dt
}

// UploadTask. init a share task
func InitShareTask(db *store.TaskDB) *ShareTask {
	t := &ShareTask{}

	baseTask := &base.Task{
		DB:   db,
		Lock: new(sync.RWMutex),
	}

	t.Task = baseTask

	return t
}

func (this *ShareTask) DeleteFileUnpaid(walletAddress string, paymentId, asset int32, amount uint64) error {
	err := this.DB.DeleteFileUnpaid(this.Id, walletAddress, paymentId, asset, amount)
	if err != nil {
		return sdkErr.New(sdkErr.SET_FILEINFO_DB_ERROR, err.Error())
	}
	return nil
}

func (this *ShareTask) AddFileUnpaid(walletAddress string, paymentId, asset int32, amount uint64) error {
	err := this.DB.AddFileUnpaid(this.Id, walletAddress, paymentId, asset, amount)
	if err != nil {
		return sdkErr.New(sdkErr.SET_FILEINFO_DB_ERROR, err.Error())
	}
	return nil
}

func (this *ShareTask) GetReferId() string {
	this.Lock.RLock()
	defer this.Lock.RUnlock()
	info := this.GetTaskInfo()
	if info == nil {
		return ""
	}
	return info.ReferId
}
