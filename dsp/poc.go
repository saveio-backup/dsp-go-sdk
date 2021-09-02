package dsp

import (
	"path"

	"github.com/saveio/dsp-go-sdk/task/poc"
	"github.com/saveio/dsp-go-sdk/task/types"
	tskUtils "github.com/saveio/dsp-go-sdk/utils/task"
	"github.com/saveio/themis/common/log"
)

func (this *Dsp) NewPocTask(taskId string) (string, error) {
	tsk, err := this.TaskMgr.NewPocTask(taskId)
	if err != nil {
		return "", err
	}
	return tsk.GetId(), nil
}

func (this *Dsp) GetPocTaskIdByFileName(fileName string) (string, error) {
	return this.TaskMgr.GetPocTaskIdByFileName(fileName)
}

// GenPocData. Genearate tags for pdp
func (this *Dsp) GenPlotPDPData(taskId string, plotCfg *poc.PlotConfig) error {
	return this.TaskMgr.GenPlotPDPData(taskId, plotCfg)
}

// AddNewPocFile. Add new plot file and generate new poc task.
func (this *Dsp) AddNewPlotFile(taskId string, createSector bool, plotCfg *poc.PlotConfig) (*types.AddPlotFileResp, error) {
	if len(taskId) == 0 {
		fileName := tskUtils.GetPlotFileName(plotCfg.Nonces, plotCfg.StartNonce, plotCfg.NumericID)
		fileName = path.Join(plotCfg.Path, fileName)
		tId, err := this.TaskMgr.GetPocTaskIdByFileName(fileName)
		if err != nil {
			return nil, err
		}
		taskId = tId
	}

	log.Debugf("add plot for task %v", taskId)
	return this.TaskMgr.AddPlotFile(taskId, createSector, plotCfg)
}

func (this *Dsp) GetAllProvedPlotFile() (*types.AllPlotsFileResp, error) {
	return this.TaskMgr.GetAllProvedPlotFile()
}

func (this *Dsp) GetAllPocTasks() (*types.AllPocTaskResp, error) {
	return this.TaskMgr.GetAllPocTasks()
}
