package taskmgr

import (
	"fmt"

	sdkErr "github.com/saveio/dsp-go-sdk/error"
	"github.com/saveio/dsp-go-sdk/store"
	"github.com/saveio/dsp-go-sdk/task/base"
	"github.com/saveio/dsp-go-sdk/task/poc"
	"github.com/saveio/dsp-go-sdk/task/types"
	"github.com/saveio/themis/common/log"
)

// NewPocTask. init a upload task and cache it
func (this *TaskMgr) NewPocTask(taskId string) (*poc.PocTask, error) {
	this.pocTaskLock.Lock()
	defer this.pocTaskLock.Unlock()

	taskType := store.TaskTypePoC
	t := poc.NewPocTask(taskId, taskType, this.db)
	log.Infof("new poc task %v", t.GetTaskType())
	if t == nil {
		return nil, sdkErr.New(sdkErr.NEW_TASK_FAILED, fmt.Sprintf("new task of type %d", taskType))
	}
	t.Mgr = this
	id := t.GetId()
	this.pocTasks[id] = t
	return t, nil
}

func (this *TaskMgr) GetPocTask(taskId string) *poc.PocTask {
	this.pocTaskLock.Lock()
	defer this.pocTaskLock.Unlock()
	tsk, ok := this.pocTasks[taskId]
	if ok {
		return tsk
	}
	if tsk == nil {
		return nil
	}
	if tsk.State() != store.TaskStateDone {
		// only cache unfinished task
		this.pocTasks[taskId] = tsk
	}
	return tsk
}

func (this *TaskMgr) AddPlotFile(taskId string, createSector bool, plotCfg *poc.PlotConfig) (*types.AddPlotFileResp, error) {

	tsk := this.GetPocTask(taskId)

	updateNodeTxHash := ""
	createSectorTxHash := ""
	if createSector {
		updateTx, sectorTx, err := tsk.CreateSectorForPlot(plotCfg)
		if err != nil {
			return nil, err
		}
		createSectorTxHash = sectorTx
		updateNodeTxHash = updateTx
	}
	log.Infof("CreateSectorForPlot %v %v", updateNodeTxHash, createSectorTxHash)

	if err := tsk.AddPlotFile(plotCfg); err != nil {
		return nil, err
	}

	go this.WaitPlotFileProved(taskId)

	return &types.AddPlotFileResp{
		TaskId:             tsk.GetId(),
		UpdateNodeTxHash:   updateNodeTxHash,
		CreateSectorTxHash: createSectorTxHash,
	}, nil
}

func (this *TaskMgr) WaitPlotFileProved(taskId string) error {
	tsk := this.GetPocTask(taskId)
	fileHash := tsk.GetFileHash()
	log.Infof("get file hash %s", fileHash)
	provedCh, _ := this.fileProvedCh.Load(fileHash)
	if provedCh == nil {
		return nil
	}
	ch := provedCh.(chan uint32)
	log.Debugf("poc task %s wait for file %s pdp", tsk.GetId(), fileHash)
	provedHeight := <-ch
	this.fileProvedCh.Delete(fileHash)
	log.Debugf("poc task %s of file %s has proved at %v", tsk.GetId(), fileHash, provedHeight)
	if err := tsk.SetInfoWithOptions(base.TaskState(store.TaskStateDone)); err != nil {
		return err
	}
	return nil
}
