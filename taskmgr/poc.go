package taskmgr

import (
	"fmt"

	sdkErr "github.com/saveio/dsp-go-sdk/error"
	"github.com/saveio/dsp-go-sdk/store"
	"github.com/saveio/dsp-go-sdk/task/poc"
)

// NewPocTask. init a upload task and cache it
func (this *TaskMgr) NewPocTask(taskId string) (*poc.PocTask, error) {
	taskType := store.TaskTypePoC
	t := poc.NewPocTask(taskId, taskType, this.db)
	if t == nil {
		return nil, sdkErr.New(sdkErr.NEW_TASK_FAILED, fmt.Sprintf("new task of type %d", taskType))
	}
	t.Mgr = this
	return t, nil
}
