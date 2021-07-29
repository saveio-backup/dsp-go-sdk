package poc

import (
	"github.com/saveio/dsp-go-sdk/store"
	"github.com/saveio/dsp-go-sdk/task/upload"
)

type PocTask struct {
	*upload.UploadTask // base task
	plotCfg            *PlotConfig
	createSector       bool
}

func NewPocTask(taskId string, taskType store.TaskType, db *store.TaskDB) *PocTask {
	dt := &PocTask{
		UploadTask: upload.NewUploadTask(taskId, taskType, db),
	}
	return dt
}

func (p *PocTask) SetPlotCfg(cfg *PlotConfig) {
	p.Lock.Lock()
	defer p.Lock.Unlock()
	p.plotCfg = cfg
}

func (p *PocTask) GetPlotCfg() *PlotConfig {
	p.Lock.RLock()
	defer p.Lock.RUnlock()
	return p.plotCfg
}

func (p *PocTask) SetCreateSector(create bool) {
	p.Lock.Lock()
	defer p.Lock.Unlock()
	p.createSector = create
}

func (p *PocTask) GetCreateSectorg() bool {
	p.Lock.RLock()
	defer p.Lock.RUnlock()
	return p.createSector
}
