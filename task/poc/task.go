package poc

import (
	"github.com/saveio/dsp-go-sdk/store"
	"github.com/saveio/dsp-go-sdk/task/upload"
)

type PocTask struct {
	*upload.UploadTask // base task
	plotCfg            *PlotConfig
	createSector       bool
	generateProgress   upload.GenearatePdpProgress
	storeTimestamp     uint64
}

func NewPocTask(taskId string, taskType store.TaskType, db *store.TaskDB) *PocTask {
	dt := &PocTask{
		UploadTask: upload.NewUploadTask(taskId, taskType, db),
	}
	return dt
}

// InitPoCTask. init a dispatch task
func InitPoCTask(taskId string, db *store.TaskDB) *PocTask {

	t := &PocTask{
		UploadTask: upload.InitUploadTask(db),
	}

	return t
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

func (p *PocTask) SetGenerateProgress(progress upload.GenearatePdpProgress) {
	p.Lock.Lock()
	defer p.Lock.Unlock()
	p.generateProgress = progress
}

func (p *PocTask) AllTagGenerated() bool {
	p.Lock.RLock()
	defer p.Lock.RUnlock()
	return p.generateProgress.Generated == p.generateProgress.Total && p.generateProgress.Total != 0
}

func (p *PocTask) GetGenerateProgress() upload.GenearatePdpProgress {
	p.Lock.RLock()
	defer p.Lock.RUnlock()
	return p.generateProgress
}
