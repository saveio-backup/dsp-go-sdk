package taskmgr

import (
	"fmt"
	"sort"

	"github.com/saveio/dsp-go-sdk/consts"
	sdkErr "github.com/saveio/dsp-go-sdk/error"
	"github.com/saveio/dsp-go-sdk/store"
	"github.com/saveio/dsp-go-sdk/task/base"
	"github.com/saveio/dsp-go-sdk/task/poc"
	"github.com/saveio/dsp-go-sdk/task/types"
	"github.com/saveio/dsp-go-sdk/task/upload"
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
	tsk, _ = this.newPoCTaskFromDB(taskId)
	if tsk == nil {
		return nil
	}
	log.Debugf("new task from db %v", tsk.GetFileName())
	if tsk.State() != store.TaskStateDone {
		// only cache unfinished task
		this.pocTasks[taskId] = tsk
	}
	return tsk
}

func (this *TaskMgr) GenPlotPDPData(taskId string, plotCfg *poc.PlotConfig) error {
	tsk := this.GetPocTask(taskId)
	err := tsk.GenPlotPDPData(plotCfg)
	if err != nil {
		tsk.SetResult("", sdkErr.POC_TASK_ERROR, err.Error())
		return err
	}
	return err
}

func (this *TaskMgr) GetPocTaskIdByFileName(fileName string) (string, error) {
	info := this.db.GetPoCTaskByFields(map[string]string{
		store.TaskInfoFieldFileName: fileName,
	})
	if info == nil {
		return "", fmt.Errorf("poc task of %s not found", fileName)
	}
	return info.Id, nil
}

func (this *TaskMgr) AddPlotFile(taskId string, createSector bool, plotCfg *poc.PlotConfig) (*types.AddPlotFileResp, error) {

	tsk := this.GetPocTask(taskId)
	tsk.SetTaskState(store.TaskStateDoing)

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
	log.Infof("CreateSectorForPlot %v  createSectorTxHash %v, tsk %v", updateNodeTxHash, createSectorTxHash, tsk)

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

func (this *TaskMgr) GetAllProvedPlotFile() (*types.AllPlotsFileResp, error) {

	taskInfos, err := this.db.GetPocTaskInfos()
	if err != nil {
		return nil, err
	}

	totalNum := 0
	totalSize := uint64(0)
	resp := &types.AllPlotsFileResp{
		FileInfos: make([]*types.PlotFileInfo, 0),
	}
	for _, info := range taskInfos {
		proveDetails, _ := this.chain.GetFileProveDetails(info.FileHash)
		if proveDetails == nil {
			continue
		}
		proved := false
		for _, d := range proveDetails.ProveDetails {
			if d.ProveTimes > 0 {
				proved = true
				break
			}
		}

		if !proved {
			continue
		}

		fileInfo, _ := this.chain.GetFileInfo(info.FileHash)
		if fileInfo == nil {
			continue
		}
		fileSize := (fileInfo.FileBlockSize * fileInfo.FileBlockNum)
		resp.FileInfos = append(resp.FileInfos, &types.PlotFileInfo{
			FileName:    string(fileInfo.FileDesc),
			FileHash:    string(fileInfo.FileHash),
			FileOwner:   fileInfo.FileOwner.ToBase58(),
			FileSize:    fileSize,
			BlockHeight: fileInfo.BlockHeight,
			PlotInfo:    fileInfo.PlotInfo,
		})
		totalNum++
		totalSize += fileSize
	}

	resp.TotalCount = totalNum
	resp.TotalSize = totalSize
	return resp, nil
}

func (this *TaskMgr) GetAllPocTasks() (*types.AllPocTaskResp, error) {

	taskInfos, err := this.db.GetPocTaskInfos()
	if err != nil {
		return nil, err
	}

	totalNum := 0
	resp := &types.AllPocTaskResp{
		PocTaskInfos: make(types.PocTaskInfos, 0),
	}
	for _, info := range taskInfos {

		// 0 undone, 1: generating pdp, 2: generated pdp success, 3. submit to chain
		var progress upload.GenearatePdpProgress
		var state types.PocTaskPDPState
		progressF := 0.0
		if len(info.ProveParams) > 0 {
			state = types.PocTaskPDPStatePdpSaved
			progressF = 1
		} else {
			tsk := this.GetPocTask(info.Id)
			if tsk != nil {
				progress = tsk.GetGenerateProgress()
				if progress.Total != 0 {
					state = types.PocTaskPDPStateGeneratingPdp
					progressF = float64(progress.Generated) / float64(progress.Total)
				}
				// set to 0.99 because prove params haven't saved to db yet
				if progressF == 1 {
					progressF = 0.99
				}
			}

		}

		fileSize := info.TotalBlockCount * consts.CHUNK_SIZE_KB
		pocTaskInfo := &types.PocTaskInfo{
			TaskId:       info.Id,
			FileSize:     fileSize,
			RealFileSize: info.RealFileSize,
			FileName:     info.FileName,
			FileHash:     string(info.FileHash),
			BlockHeight:  uint64(info.StoreTxHeight),
			Progress:     progressF,
			PDPState:     state,
			TaskState:    info.TaskState,
			EstimateTime: uint64(progress.EstimateTime),
			CreatedAt:    info.CreatedAt,
			StoredAt:     info.StoreTxTime,
		}
		resp.TotalSize += fileSize
		if len(info.StoreTx) == 0 {
			resp.PocTaskInfos = append(resp.PocTaskInfos, pocTaskInfo)
			totalNum++
			continue
		}

		progressF = 1
		state = types.PocTaskPDPStatePdpProving
		proveDetails, _ := this.chain.GetFileProveDetails(info.FileHash)
		if proveDetails == nil {
			pocTaskInfo.PDPState = state
			resp.PocTaskInfos = append(resp.PocTaskInfos, pocTaskInfo)
			totalNum++
			continue
		}

		proveTimes := 0
		proved := false
		for _, d := range proveDetails.ProveDetails {
			if d.ProveTimes > 0 {
				proved = true
				proveTimes = int(d.ProveTimes)
				break
			}
		}

		if !proved {
			resp.PocTaskInfos = append(resp.PocTaskInfos, pocTaskInfo)
			totalNum++
			continue
		}
		state = types.PocTaskPDPStateSubmitted
		fileInfo, _ := this.chain.GetFileInfo(info.FileHash)
		if fileInfo == nil {
			resp.PocTaskInfos = append(resp.PocTaskInfos, pocTaskInfo)
			totalNum++
			continue
		}

		pocTaskInfo.FileOwner = fileInfo.FileOwner.ToBase58()
		pocTaskInfo.PlotInfo = fileInfo.PlotInfo
		pocTaskInfo.PDPState = state

		pocTaskInfo.ProveTimes = uint64(proveTimes)
		resp.TotalProvedSize += fileSize
		resp.PocTaskInfos = append(resp.PocTaskInfos, pocTaskInfo)
		totalNum++

	}

	sort.Sort(sort.Reverse(resp.PocTaskInfos))
	resp.TotalCount = totalNum
	return resp, nil
}

// newPoCTaskFromDB. Read file info from DB and recover a task by the file info.
func (this *TaskMgr) newPoCTaskFromDB(id string) (*poc.PocTask, error) {
	info, err := this.db.GetTaskInfo(id)
	if err != nil {
		log.Errorf("new poc task get task from db, get file info failed, id: %s", id)
		return nil, err
	}
	if info == nil {
		log.Warnf("new poc task  get task from db, recover task get file info is nil, id: %v", id)
		return nil, nil
	}
	if info.Type != store.TaskTypePoC {
		return nil, nil
	}
	log.Debugf("new poc task info name %v", info)

	state := store.TaskState(info.TaskState)
	if state == store.TaskStatePrepare || state == store.TaskStateDoing ||
		state == store.TaskStateCancel || state == store.TaskStateIdle {
		state = store.TaskStatePause
	}
	t := poc.InitPoCTask(id, this.db)
	t.Id = id
	t.Mgr = this

	t.SetInfoWithOptions(
		base.TaskType(store.TaskTypePoC),
		base.TaskState(state),
		base.TransferState(uint32(types.TaskPause)),
	)
	log.Debugf("get poc task from db task id %s, file name %s, task type %d, state %d",
		info.Id, info.FileName, info.Type, info.TaskState)
	return t, nil
}
