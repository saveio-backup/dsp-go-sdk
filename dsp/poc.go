package dsp

import (
	"github.com/saveio/dsp-go-sdk/task/poc"
	"github.com/saveio/dsp-go-sdk/task/types"
)

// AddNewPocFile. Add new plot file and generate new poc task.
func (this *Dsp) AddNewPlotFile(taskId string, createSector bool, plotCfg *poc.PlotConfig) (*types.AddPlotFileResp, error) {
	tsk, err := this.TaskMgr.NewPocTask(taskId)
	if err != nil {
		return nil, err
	}
	return this.TaskMgr.AddPlotFile(tsk.GetId(), createSector, plotCfg)
}
