package dsp

import (
	"github.com/saveio/dsp-go-sdk/consts"
	dspErr "github.com/saveio/dsp-go-sdk/error"
	"github.com/saveio/dsp-go-sdk/task/types"
	"github.com/saveio/dsp-go-sdk/taskmgr"

	"github.com/saveio/dsp-go-sdk/store"
	"github.com/saveio/themis/common/log"
)

func (this *Dsp) GetTaskMgr() *taskmgr.TaskMgr {
	return this.TaskMgr
}

func (this *Dsp) RecoverDBLossTask() error {
	return this.TaskMgr.RecoverLossTaskFromDB()
}

func (this *Dsp) GetProgressInfo(taskId string) *types.ProgressInfo {
	return this.TaskMgr.GetProgressInfo(taskId)
}

func (this *Dsp) GetTaskState(taskId string) (store.TaskState, error) {
	return this.TaskMgr.GetTaskState(taskId), nil
}

func (this *Dsp) IsTaskExist(taskId string) bool {
	return this.TaskMgr.TaskExist(taskId)
}

func (this *Dsp) CleanTasks(taskIds []string) error {
	return this.TaskMgr.CleanTasks(taskIds)
}

// func (this *Dsp) Progress() {
// 	this.RegProgressChannel()
// 	go func() {
// 		stop := false
// 		for {
// 			v := <-this.ProgressChannel()
// 			for node, pro := range v.Progress {
// 				log.Infof("file progress:%s, hash:%s, total:%d, peer:%s, uploaded:%d, progress:%f, speed: %d",
// 					v.FileName, v.FileHash, v.Total, node, pro.Progress, float64(pro.Progress)/float64(v.Total),
// 					pro.AvgSpeed())
// 				stop = (pro.Progress == v.Total)
// 			}
// 			if stop {
// 				break
// 			}
// 		}
// 		// TODO: why need close
// 		this.CloseProgressChannel()
// 	}()
// }

// RegProgressChannel. register progress channel
func (this *Dsp) RegProgressChannel() {
	if this == nil {
		log.Errorf("this.taskMgr == nil")
	}
	this.TaskMgr.RegProgressCh()
}

// GetProgressChannel.
func (this *Dsp) ProgressChannel() chan *types.ProgressInfo {
	return this.TaskMgr.ProgressCh()
}

// CloseProgressChannel.
func (this *Dsp) CloseProgressChannel() {
	this.TaskMgr.CloseProgressCh()
}

func (this *Dsp) GetTaskInfo(id string) *store.TaskInfo {
	return this.TaskMgr.GetTaskInfoCopy(id)
}

func (this *Dsp) GetUploadTaskInfoByHash(fileHashStr string) *store.TaskInfo {
	return this.TaskMgr.GetUploadTaskByFileHash(fileHashStr)
}

func (this *Dsp) GetPlotTaskByFileHash(fileHashStr string) *store.TaskInfo {
	return this.TaskMgr.GetPlotTaskByFileHash(fileHashStr)
}

func (this *Dsp) GetTaskFileName(id string) string {
	return this.TaskMgr.GetTaskFileName(id)
}

func (this *Dsp) GetTaskFileHash(id string) string {
	return this.TaskMgr.GetTaskFileHash(id)
}

func (this *Dsp) GetUploadTaskId(fileHashStr string) string {
	tsk := this.GetUploadTaskInfoByHash(fileHashStr)
	if tsk == nil {
		return ""
	}
	return tsk.Id
}

func (this *Dsp) GetDownloadTaskIdByUrl(url string) string {
	fileHash := this.DNS.GetFileHashFromUrl(url)
	return this.TaskMgr.GetDownloadedTaskId(fileHash, this.Chain.WalletAddress())
}

func (this *Dsp) GetUrlOfUploadedfile(fileHashStr string) string {
	tsk := this.TaskMgr.GetUploadTaskByFileHash(fileHashStr)
	if tsk == nil {
		return ""
	}
	return tsk.Url
}

func (this *Dsp) GetPlotTaskId(fileHashStr string) string {
	tsk := this.GetPlotTaskByFileHash(fileHashStr)
	if tsk == nil {
		return ""
	}
	return tsk.Id
}

func (this *Dsp) GetPlotTaskFileName(fileHashStr string) string {
	tsk := this.GetPlotTaskByFileHash(fileHashStr)
	if tsk == nil {
		return ""
	}
	return tsk.FileName
}

func (this *Dsp) GetTaskIdList(offset, limit uint32, createdAt, createdAtEnd, updatedAt, updatedAtEnd uint64,
	ft store.TaskType, complete, reverse, includeFailed, ignoreHide bool) []string {
	if this == nil || this.TaskMgr == nil {
		return nil
	}
	return this.TaskMgr.GetTaskIdList(offset, limit, createdAt, createdAtEnd, updatedAt, updatedAtEnd, ft,
		complete, reverse, includeFailed, ignoreHide)
}

func (this *Dsp) HideTaskIds(ids []string) error {
	return this.TaskMgr.HideTaskIds(ids)
}

// GetFileUploadSize. get file upload size
func (this *Dsp) GetFileUploadSize(fileHashStr, nodeAddr string) (uint64, error) {
	info := this.TaskMgr.GetUploadTaskByFileHash(fileHashStr)
	if info == nil {
		return 0, dspErr.New(dspErr.TASK_NOT_EXIST, "task not found for file %s", fileHashStr)
	}
	id := info.Id
	if len(id) == 0 {
		return 0, dspErr.New(dspErr.TASK_NOT_EXIST, "task id is empty")
	}
	progressInfo := this.TaskMgr.GetProgressInfo(id)
	if progressInfo == nil {
		return 0, nil
	}
	progress, ok := progressInfo.Progress[nodeAddr]
	if ok {
		return uint64(progress.Progress) * consts.CHUNK_SIZE, nil
	}
	return uint64(progressInfo.SlaveProgress[nodeAddr].Progress) * consts.CHUNK_SIZE, nil
}

func (this *Dsp) GetDownloadTaskRemainSize(taskId string) uint64 {
	progress := this.TaskMgr.GetProgressInfo(taskId)
	sum := uint64(0)
	for _, c := range progress.Progress {
		sum += c.Progress
	}
	if progress.Total > sum {
		return progress.Total - sum
	}
	return 0
}

func (this *Dsp) InsertUserspaceRecord(id, walletAddr string, size uint64,
	sizeOp store.UserspaceOperation, second uint64, secondOp store.UserspaceOperation, amount uint64,
	transferType store.UserspaceTransferType) error {
	return this.userspaceRecordDB.InsertUserspaceRecord(id, walletAddr, size,
		sizeOp, second, secondOp, amount,
		transferType)
}

// SelectUserspaceRecordByWalletAddr.
func (this *Dsp) SelectUserspaceRecordByWalletAddr(
	walletAddr string, offset, limit uint64) ([]*store.UserspaceRecord, error) {
	return this.userspaceRecordDB.SelectUserspaceRecordByWalletAddr(walletAddr, offset, limit)
}

func (this *Dsp) GetUploadTaskInfos() ([]*store.TaskInfo, error) {
	return this.TaskMgr.GetUploadTaskInfos()
}
