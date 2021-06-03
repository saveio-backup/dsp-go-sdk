package taskmgr

import (
	"sync"
	"time"

	"github.com/saveio/dsp-go-sdk/config"
	"github.com/saveio/dsp-go-sdk/consts"
	"github.com/saveio/dsp-go-sdk/core/chain"
	"github.com/saveio/dsp-go-sdk/core/channel"
	"github.com/saveio/dsp-go-sdk/core/dns"
	"github.com/saveio/dsp-go-sdk/core/fs"
	sdkErr "github.com/saveio/dsp-go-sdk/error"
	"github.com/saveio/dsp-go-sdk/store"
	"github.com/saveio/dsp-go-sdk/task/base"
	"github.com/saveio/dsp-go-sdk/task/dispatch"
	"github.com/saveio/dsp-go-sdk/task/download"
	"github.com/saveio/dsp-go-sdk/task/share"
	"github.com/saveio/dsp-go-sdk/task/types"
	"github.com/saveio/dsp-go-sdk/task/upload"
	"github.com/saveio/dsp-go-sdk/types/ticker"
	"github.com/saveio/themis/common/log"
)

// TaskMgr. implement upload/download task manager.
// only save needed information to db by fileDB, the other field save in memory
type TaskMgr struct {
	chain               *chain.Chain                      // chain SDK
	fs                  *fs.Fs                            // fs module
	dns                 *dns.DNS                          // dns module
	channel             *channel.Channel                  // channel module
	cfg                 *config.DspConfig                 // sdk config
	db                  *store.TaskDB                     // task db
	running             bool                              // state of service
	shareRecordDB       *store.ShareRecordDB              // share_record db
	uploadTasks         map[string]*upload.UploadTask     // upload tasks map, task id => upload task
	uploadTaskLock      *sync.RWMutex                     // upload task lock
	downloadTasks       map[string]*download.DownloadTask // download tasks map, task id => download task
	downloadTaskLock    *sync.RWMutex                     // download task lock
	dispatchTasks       map[string]*dispatch.DispatchTask // dispatch tasks map, task id => upload task
	dispatchTaskLock    *sync.RWMutex                     // dispatch task lock
	shareTasks          map[string]*share.ShareTask       // upload tasks map, task id => upload task
	shareTaskLock       *sync.RWMutex                     // upload task lock
	retryUploadTaskTs   map[string]uint64                 // retry task taskId <==> retry at timestamp
	retryDownloadTaskTs map[string]uint64                 // retry task taskId <==> retry at timestamp
	retryDispatchTaskTs map[string]uint64                 // retry task taskId <==> retry at timestamp
	progress            chan *types.ProgressInfo          // progress channel
	blockReqCh          chan []*types.GetBlockReq         // used for share blocks
	progressTicker      *ticker.Ticker                    // get upload progress ticker
	retryTaskTicker     *ticker.Ticker                    // retry task ticker
	shareNoticeCh       chan *types.ShareNotification     // share notification channel
}

func NewTaskMgr(chain *chain.Chain, fs *fs.Fs, dns *dns.DNS, channel *channel.Channel, cfg *config.DspConfig) *TaskMgr {
	tmgr := &TaskMgr{
		chain:               chain,
		fs:                  fs,
		dns:                 dns,
		channel:             channel,
		cfg:                 cfg,
		uploadTasks:         make(map[string]*upload.UploadTask, 0),
		downloadTasks:       make(map[string]*download.DownloadTask, 0),
		dispatchTasks:       make(map[string]*dispatch.DispatchTask, 0),
		shareTasks:          make(map[string]*share.ShareTask, 0),
		uploadTaskLock:      new(sync.RWMutex),
		downloadTaskLock:    new(sync.RWMutex),
		dispatchTaskLock:    new(sync.RWMutex),
		shareTaskLock:       new(sync.RWMutex),
		retryUploadTaskTs:   make(map[string]uint64),
		retryDownloadTaskTs: make(map[string]uint64),
		retryDispatchTaskTs: make(map[string]uint64),
		blockReqCh:          make(chan []*types.GetBlockReq, consts.MAX_GOROUTINES_FOR_WORK_TASK),
	}
	tmgr.progressTicker = ticker.NewTicker(time.Duration(consts.TASK_PROGRESS_TICKER_DURATION)*time.Second,
		tmgr.runGetProgressTicker)
	tmgr.retryTaskTicker = ticker.NewTicker(time.Duration(consts.TASK_RETRY_DURATION)*time.Second,
		tmgr.retryTaskService)
	return tmgr
}

func (this *TaskMgr) Chain() *chain.Chain {
	return this.chain
}

func (this *TaskMgr) Fs() *fs.Fs {
	return this.fs
}

func (this *TaskMgr) DNS() *dns.DNS {
	return this.dns
}

func (this *TaskMgr) Channel() *channel.Channel {
	return this.channel
}

func (this *TaskMgr) Config() *config.DspConfig {
	return this.cfg
}

func (this *TaskMgr) SetFileDB(d *store.LevelDBStore) {
	this.db = store.NewTaskDB(d)
}

func (this *TaskMgr) IsClient() bool {
	return this.cfg.FsType == consts.FS_FILESTORE
}

func (this *TaskMgr) Stop() error {
	this.running = false
	if this.db == nil {
		return nil
	}
	err := this.db.Close()
	if err != nil {
		return sdkErr.NewWithError(sdkErr.CLOSE_DB_ERROR, err)
	}
	return nil
}

// RecoverUndoneTask. recover unfinished task from DB
func (this *TaskMgr) RecoverUndoneTask() error {
	if err := this.RecoverUndoneUploadTask(); err != nil {
		return err
	}
	if err := this.RecoverUndoneDownloadTask(); err != nil {
		return err
	}
	return nil

}

func (this *TaskMgr) HasRunningTask() bool {
	if this.HasRunningUploadTask() {
		return true
	}
	if this.HasRunningDownloadTask() {
		return true
	}
	if this.HasRunningDispatchTask() {
		return true
	}
	return false
}

// IsWorkerBusy. check if the worker is busy in 1 min, or net phase not equal to expected phase
func (this *TaskMgr) IsWorkerBusy(taskId, walletAddr string, excludePhase int) bool {
	this.uploadTaskLock.RLock()
	defer this.uploadTaskLock.RUnlock()
	for id, t := range this.uploadTasks {
		if id == taskId {
			continue
		}
		phase := t.GetNodeNetPhase(walletAddr)
		if phase == excludePhase {
			log.Debugf("%s included phase %d", walletAddr, excludePhase)
			return true
		}
		if !t.HasWorker(walletAddr) {
			continue
		}
		idleDuration := t.WorkerIdleDuration(walletAddr)
		if idleDuration > 0 && idleDuration < 60*1000 {
			log.Debugf("%s is active", walletAddr)
			return true
		}
	}
	return false
}

// RegShareNotification. register share notification
func (this *TaskMgr) RegShareNotification() {
	if this.shareNoticeCh == nil {
		this.shareNoticeCh = make(chan *types.ShareNotification, 0)
	}
}

// ShareNotification. get share notification channel
func (this *TaskMgr) ShareNotification() chan *types.ShareNotification {
	return this.shareNoticeCh
}

// CloseShareNotification. close
func (this *TaskMgr) CloseShareNotification() {
	close(this.shareNoticeCh)
	this.shareNoticeCh = nil
}

func (this *TaskMgr) GetBaseTaskById(taskId string) *base.Task {
	if uploadTask := this.GetUploadTask(taskId); uploadTask != nil {
		log.Debugf("get base upload task %s", taskId)
		return uploadTask.Task
	}
	if downloadTask := this.GetDownloadTask(taskId); downloadTask != nil {
		log.Debugf("get base download task %s", taskId)
		return downloadTask.Task
	}
	if dispatchTask := this.GetDispatchTask(taskId); dispatchTask != nil {
		log.Debugf("get base dispatch task %s", taskId)
		return dispatchTask.Task
	}

	if shareTask := this.GetShareTask(taskId); shareTask != nil {
		log.Debugf("get base share task %s", taskId)
		return shareTask.Task
	}

	return nil
}

// GetTaskFileHashById. get task file hash by task id
func (this *TaskMgr) GetTaskFileHashById(taskId string) string {
	tsk := this.GetBaseTaskById(taskId)
	if tsk == nil {
		return ""
	}
	return tsk.GetFileHash()
}

// GetProgressInfo. get task progress info
func (this *TaskMgr) GetProgressInfo(taskId string) *types.ProgressInfo {
	tsk := this.GetBaseTaskById(taskId)
	if tsk == nil {
		return nil
	}
	return tsk.GetProgressInfo()
}

func (this *TaskMgr) GetTaskState(taskId string) store.TaskState {
	tsk := this.GetBaseTaskById(taskId)
	if tsk == nil {
		return store.TaskStateNone
	}
	return tsk.State()
}

// TaskExist. Check if task exist in memory
func (this *TaskMgr) TaskExist(taskId string) bool {
	uploadTask := this.GetUploadTask(taskId)
	if uploadTask != nil {
		return true
	}
	downloadTask := this.GetDownloadTask(taskId)
	if downloadTask != nil {
		return true
	}
	dispatchTask := this.GetDispatchTask(taskId)
	if dispatchTask != nil {
		return true
	}
	return false
}

// CleanTask. clean task from memory and DB
func (this *TaskMgr) CleanTasks(taskId []string) error {
	for _, id := range taskId {
		if err := this.CleanUploadTask(id); err != nil {
			log.Errorf("clean upload task %s failed, err %s", id, err)
			continue
		}
		if err := this.CleanDownloadTask(id); err != nil {
			log.Errorf("clean download task %s failed, err %s", id, err)
			continue
		}
	}
	return nil
}

func (this *TaskMgr) RegProgressCh() {
	if this.progress == nil {
		this.progress = make(chan *types.ProgressInfo, consts.MAX_PROGRESS_CHANNEL_SIZE)
	}
}

func (this *TaskMgr) ProgressCh() chan *types.ProgressInfo {
	return this.progress
}

func (this *TaskMgr) CloseProgressCh() {
	close(this.progress)
	this.progress = nil
}

func (this *TaskMgr) GetTaskInfoCopy(taskId string) *store.TaskInfo {
	tsk := this.GetBaseTaskById(taskId)
	if tsk == nil {
		return nil
	}
	return tsk.GetTaskInfoCopy()
}

// GetDoingTaskNum. calculate the sum of task number with task type
func (this *TaskMgr) GetDoingTaskNum(tskType store.TaskType) uint32 {
	switch tskType {
	case store.TaskTypeUpload:
		return this.GetDoingUploadTaskNum()
	case store.TaskTypeDownload:
		return this.GetDoingDownloadTaskNum()
	}
	return 0
}

func (this *TaskMgr) BlockReqCh() chan []*types.GetBlockReq {
	return this.blockReqCh
}
