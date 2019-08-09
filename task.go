package dsp

import (
	"fmt"

	"github.com/saveio/dsp-go-sdk/task"
	"github.com/saveio/themis/common/log"
)

func (this *Dsp) GetProgressInfo(taskId string) *task.ProgressInfo {
	return this.taskMgr.GetProgressInfo(taskId)
}

func (this *Dsp) GetTaskState(taskId string) (task.TaskState, error) {
	if this.taskMgr == nil {
		return task.TaskStateNone, nil
	}
	_, ok := this.taskMgr.GetTaskById(taskId)
	if !ok {
		return 0, fmt.Errorf("get task by id not found %s", taskId)
	}
	return this.taskMgr.GetTaskState(taskId), nil
}

func (this *Dsp) IsTaskExist(taskId string) bool {
	return this.taskMgr.TaskExist(taskId)
}

func (this *Dsp) Progress() {
	this.RegProgressChannel()
	go func() {
		stop := false
		for {
			v := <-this.ProgressChannel()
			for node, cnt := range v.Count {
				log.Infof("file:%s, hash:%s, total:%d, peer:%s, uploaded:%d, progress:%f", v.FileName, v.FileHash, v.Total, node, cnt, float64(cnt)/float64(v.Total))
				stop = (cnt == v.Total)
			}
			if stop {
				break
			}
		}
		// TODO: why need close
		this.CloseProgressChannel()
	}()
}

// RegProgressChannel. register progress channel
func (this *Dsp) RegProgressChannel() {
	if this == nil {
		log.Errorf("this.taskMgr == nil")
	}
	this.taskMgr.RegProgressCh()
}

// GetProgressChannel.
func (this *Dsp) ProgressChannel() chan *task.ProgressInfo {
	return this.taskMgr.ProgressCh()
}

// CloseProgressChannel.
func (this *Dsp) CloseProgressChannel() {
	this.taskMgr.CloseProgressCh()
}
