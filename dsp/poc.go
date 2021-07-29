package dsp

import "github.com/saveio/dsp-go-sdk/task/poc"

// AddNewPocFile. Add new plot file and generate new poc task.
func (this *Dsp) AddNewPlotFile(taskId string, plotCfg *poc.PlotConfig) (string, error) {

	tsk, err := this.TaskMgr.NewPocTask(taskId)
	if err != nil {
		return "", err
	}

	if err := tsk.AddPlotFile(plotCfg); err != nil {
		return "", err
	}

	return tsk.GetId(), nil
}
