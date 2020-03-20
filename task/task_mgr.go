package task

import (
	"errors"
	"fmt"
	"runtime/debug"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/saveio/dsp-go-sdk/common"
	dspErr "github.com/saveio/dsp-go-sdk/error"
	netcom "github.com/saveio/dsp-go-sdk/network/common"
	"github.com/saveio/dsp-go-sdk/network/message/types/block"
	"github.com/saveio/dsp-go-sdk/network/message/types/payment"
	"github.com/saveio/dsp-go-sdk/store"
	"github.com/saveio/dsp-go-sdk/utils"
	"github.com/saveio/dsp-go-sdk/utils/ticker"
	"github.com/saveio/themis/common/log"
)

// TaskMgr. implement upload/download task manager.
// only save needed information to db by fileDB, the other field save in memory
type TaskMgr struct {
	tasks          map[string]*Task
	retryTasks     map[string]uint64 // retry task taskId <==> retry at timestamp
	walletHostAddr map[string]string
	lock           sync.RWMutex
	blockReqCh     chan []*GetBlockReq // used for share blocks
	progress       chan *ProgressInfo  // progress channel
	shareNoticeCh  chan *ShareNotification
	db             *store.TaskDB
	progressTicker *ticker.Ticker // get upload progress ticker
}

func NewTaskMgr(t *ticker.Ticker) *TaskMgr {
	ts := make(map[string]*Task, 0)
	tmgr := &TaskMgr{
		tasks: ts,
	}
	tmgr.blockReqCh = make(chan []*GetBlockReq, common.MAX_GOROUTINES_FOR_WORK_TASK)
	tmgr.progressTicker = t
	tmgr.retryTasks = make(map[string]uint64)
	return tmgr
}

func (this *TaskMgr) SetFileDB(d *store.LevelDBStore) {
	this.db = store.NewTaskDB(d)
}

func (this *TaskMgr) CloseDB() error {
	if this.db == nil {
		return nil
	}
	err := this.db.Close()
	if err != nil {
		return dspErr.NewWithError(dspErr.CLOSE_DB_ERROR, err)
	}
	return nil
}

// NewTask. start a task for a file
func (this *TaskMgr) NewTask(taskId string, taskT store.TaskType) (string, error) {
	this.lock.Lock()
	defer this.lock.Unlock()
	t := NewTask(taskId, taskT, this.db)
	if t == nil {
		return "", dspErr.New(dspErr.NEW_TASK_FAILED, fmt.Sprintf("new task of type %d", taskT))
	}
	id := t.GetId()
	this.tasks[id] = t
	return id, nil
}

// BindTaskId. set key to taskId, for upload task, if fileHash is empty, use Hex(filePath) instead.
// for download/share task, use fileHash
func (this *TaskMgr) BindTaskId(id string) error {
	t, ok := this.GetTaskById(id)
	if !ok {
		return dspErr.New(dspErr.SET_FILEINFO_DB_ERROR, fmt.Sprintf("task %s not found", id))
	}
	err := t.BindIdWithWalletAddr()
	if err != nil {
		return dspErr.New(dspErr.SET_FILEINFO_DB_ERROR, err.Error())
	}
	return nil
}

// RecoverUndoneTask. recover unfinished task from DB
func (this *TaskMgr) RecoverUndoneTask() error {
	this.lock.Lock()
	defer this.lock.Unlock()
	taskIds, err := this.db.UndoneList(store.TaskTypeUpload)
	if err != nil {
		return err
	}
	unloadTaskLen := len(taskIds)
	downloadTaskIds, err := this.db.UndoneList(store.TaskTypeDownload)
	if err != nil {
		return err
	}
	taskIds = append(taskIds, downloadTaskIds...)
	log.Debugf("total recover task len: %d", len(taskIds))
	for i, id := range taskIds {
		t, err := NewTaskFromDB(id, this.db)
		if err != nil {
			continue
		}
		if t == nil || t.State() == store.TaskStateDone {
			log.Warnf("can't recover this task %s", id)
			if i < unloadTaskLen {
				this.db.RemoveFromUndoneList(nil, id, store.TaskTypeUpload)
			} else {
				this.db.RemoveFromUndoneList(nil, id, store.TaskTypeDownload)
			}
			continue
		}
		this.tasks[id] = t
	}
	return nil
}

func (this *TaskMgr) RecoverDBLossTask(fileHashStrs []string, fileNameMap map[string]string, walletAddr string) error {
	for _, fileHashStr := range fileHashStrs {
		id := this.TaskId(fileHashStr, walletAddr, store.TaskTypeUpload)
		t, ok := this.GetTaskById(id)
		if ok && t != nil {
			continue
		}
		newId, err := this.NewTask(id, store.TaskTypeUpload)
		if err != nil {
			return err
		}
		if err := this.SetTaskInfoWithOptions(newId, FileHash(fileHashStr), Walletaddr(walletAddr),
			FileName(fileNameMap[fileHashStr])); err != nil {
			return err
		}
		err = this.BindTaskId(newId)
		if err != nil {
			return err
		}
		t, _ = this.GetTaskById(newId)
		if t == nil {
			return fmt.Errorf("set new task with id failed %s", newId)
		}
		log.Debugf("recover db loss task %s %s", newId, fileHashStr)
		t.SetResult(nil, dspErr.GET_FILEINFO_FROM_DB_ERROR, "DB has damaged. Can't recover the task")
	}
	return nil
}

// TaskId from hash-walletaddress-type
func (this *TaskMgr) TaskId(prefix, walletAddress string, tp store.TaskType) string {
	var key string
	switch tp {
	case store.TaskTypeUpload:
		// use filePath to get id
		hexStr := utils.StringToSha256Hex(prefix)
		key = taskIdKey(hexStr, walletAddress, tp)
		id, _ := this.db.GetFileInfoId(key)
		if len(id) > 0 {
			return id
		}
		// use fileHash to get id
		key = taskIdKey(prefix, walletAddress, tp)
		id, _ = this.db.GetFileInfoId(key)
		return id
	case store.TaskTypeDownload, store.TaskTypeShare:
		key = taskIdKey(prefix, walletAddress, tp)
		id, _ := this.db.GetFileInfoId(key)
		return id
	}
	return ""
}

// DeleteTask. delete task with task id from memory. runtime delete action.
func (this *TaskMgr) DeleteTask(taskId string) {
	this.lock.Lock()
	defer this.lock.Unlock()
	log.Debugf("delete task %s, %s", taskId, debug.Stack())
	delete(this.tasks, taskId)
	delete(this.retryTasks, taskId)
}

// CleanTask. clean task from memory and DB
func (this *TaskMgr) CleanTask(taskId string) error {
	this.lock.Lock()
	delete(this.tasks, taskId)
	delete(this.retryTasks, taskId)
	this.lock.Unlock()
	log.Debugf("clean task %s, %s", taskId, debug.Stack())
	err := this.db.DeleteTaskInfo(taskId)
	if err != nil {
		return dspErr.NewWithError(dspErr.SET_FILEINFO_DB_ERROR, err)
	}
	return nil
}

// CleanTask. clean task from memory and DB
func (this *TaskMgr) CleanTasks(taskId []string) error {
	for _, id := range taskId {
		if err := this.CleanTask(id); err != nil {
			return err
		}
	}
	return nil
}

func (this *TaskMgr) TaskNum() int {
	return len(this.tasks)
}

func (this *TaskMgr) GetDoingTaskNum(tskType store.TaskType) uint32 {
	this.lock.RLock()
	defer this.lock.RUnlock()
	cnt := uint32(0)
	for _, t := range this.tasks {
		if t.State() == store.TaskStateDoing && t.GetTaskType() == tskType {
			cnt++
		}
	}
	return cnt
}

func (this *TaskMgr) BlockReqCh() chan []*GetBlockReq {
	return this.blockReqCh
}

func (this *TaskMgr) GetTaskById(taskId string) (*Task, bool) {
	this.lock.Lock()
	defer this.lock.Unlock()
	v, ok := this.tasks[taskId]
	if ok {
		return v, ok
	}
	t, err := NewTaskFromDB(taskId, this.db)
	if t == nil {
		log.Debugf("get task by memory and DB failed %s, err: %s", taskId, err)
		return nil, false
	}
	if t.State() != store.TaskStateDone {
		// only cache unfinished task
		this.tasks[taskId] = t
	}
	return t, true
}

func (this *TaskMgr) HasRetryTask() bool {
	this.lock.RLock()
	defer this.lock.RUnlock()
	return len(this.retryTasks) > 0
}

func (this *TaskMgr) GetUploadTaskToRetry() []string {
	this.lock.Lock()
	defer this.lock.Unlock()
	if len(this.retryTasks) == 0 {
		return nil
	}
	list := make(utils.Uint64PairList, 0)
	for key, value := range this.retryTasks {
		list = append(list, utils.Uint64Pair{
			Key:   key,
			Value: value,
		})
	}
	sort.Sort(list)
	taskIds := make([]string, 0)
	for _, l := range list {
		tsk, ok := this.tasks[l.Key]
		if !ok {
			continue
		}
		if !tsk.NeedRetry() {
			continue
		}
		if tsk.GetTaskType() != store.TaskTypeUpload {
			continue
		}
		taskIds = append(taskIds, l.Key)
	}
	return taskIds
}

func (this *TaskMgr) HasRunningTask() bool {
	this.lock.RLock()
	defer this.lock.RUnlock()
	for _, tsk := range this.tasks {
		if prepare, doing := tsk.IsTaskPreparingOrDoing(); prepare || doing {
			return true
		}
	}
	return false
}

func (this *TaskMgr) HasRunningDownloadTask() bool {
	this.lock.RLock()
	defer this.lock.RUnlock()
	for _, tsk := range this.tasks {
		if tsk.GetTaskType() != store.TaskTypeDownload {
			continue
		}
		if prepare, doing := tsk.IsTaskPreparingOrDoing(); prepare || doing {
			return true
		}
	}
	return false
}

// TaskExist. Check if task exist in memory
func (this *TaskMgr) TaskExist(taskId string) bool {
	this.lock.RLock()
	defer this.lock.RUnlock()
	if len(taskId) == 0 {
		return false
	}
	_, ok := this.tasks[taskId]
	return ok
}

// TaskExistInDB. Check if task exist in DB with task id
func (this *TaskMgr) TaskExistInDB(taskId string) bool {
	this.lock.RLock()
	defer this.lock.RUnlock()
	tsk, err := this.db.GetTaskInfo(taskId)
	if err != nil || tsk == nil {
		return false
	}
	return true
}

// PauseDuplicatedTask. check if a uploading task has contained the file, if exist pause it
func (this *TaskMgr) PauseDuplicatedTask(taskId, fileHashStr string) bool {
	this.lock.Lock()
	defer this.lock.Unlock()
	tsk := this.tasks[taskId]
	var tskCreatedAt uint64
	if tsk != nil {
		tskCreatedAt = tsk.GetCreatedAt()
	} else {
		tskCreatedAt = utils.GetMilliSecTimestamp()
	}

	for _, t := range this.tasks {
		if t.GetFileHash() != fileHashStr {
			continue
		}
		if t.GetId() == taskId {
			// skip self
			continue
		}
		if t.GetTaskType() != store.TaskTypeUpload {
			continue
		}
		log.Debugf("duplicated check %s, %s %v", t.GetId(), taskId, t.State())
		if t.State() != store.TaskStateDoing {
			continue
		}
		log.Debugf("fileHashStr %s, taskId %s , newTaskId %s, taskCreatedAt: %d, newTaskCreatedAt: %d",
			fileHashStr, t.GetId(), taskId, t.GetCreatedAt(), tskCreatedAt)
		if err := tsk.SetTaskState(store.TaskStatePause); err != nil {
			continue
		}
		return true
	}
	return false
}

func (this *TaskMgr) TaskBlockReq(taskId string) (chan []*GetBlockReq, error) {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		return nil, dspErr.New(dspErr.GET_FILEINFO_FROM_DB_ERROR, "task %s not found", taskId)
	}
	return v.GetBlockReq(), nil
}

func (this *TaskMgr) PushGetBlock(taskId, sessionId string, index int32, block *BlockResp) {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		return
	}
	v.PushGetBlock(sessionId, block.Hash, index, block)
}

func (this *TaskMgr) PushGetBlockFlights(taskId, sessionId string, blocks []*BlockResp, timeStamp int64) {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		return
	}
	v.PushGetBlockFlights(sessionId, blocks, timeStamp)
}

func (this *TaskMgr) BlockFlightsChannelExists(taskId, sessionId string, timeStamp int64) bool {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		return false
	}
	return v.BlockFlightsChannelExists(sessionId, timeStamp)
}

func (this *TaskMgr) NewBlockRespCh(taskId, sessionId, blockHash string, index int32) chan *BlockResp {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		log.Warnf("get block resp channel taskId not found: %d", taskId)
		return nil
	}
	return v.NewBlockRespCh(sessionId, blockHash, index)
}

func (this *TaskMgr) DropBlockRespCh(taskId, sessionId, blockHash string, index int32) {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		return
	}
	v.DropBlockRespCh(sessionId, blockHash, index)
}
func (this *TaskMgr) NewBlockFlightsRespCh(taskId, sessionId string, timeStamp int64) chan []*BlockResp {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		log.Warnf("get block resp channel taskId not found: %d", taskId)
		return nil
	}
	return v.NewBlockFlightsRespCh(sessionId, timeStamp)
}

func (this *TaskMgr) DropBlockFlightsRespCh(taskId, sessionId string, timeStamp int64) {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		return
	}
	v.DropBlockFlightsRespCh(sessionId, timeStamp)
}

func (this *TaskMgr) RegProgressCh() {
	if this.progress == nil {
		this.progress = make(chan *ProgressInfo, common.MAX_PROGRESS_CHANNEL_SIZE)
	}
}

func (this *TaskMgr) ProgressCh() chan *ProgressInfo {
	return this.progress
}

func (this *TaskMgr) CloseProgressCh() {
	close(this.progress)
	this.progress = nil
}

// EmitProgress. emit progress to channel with taskId
func (this *TaskMgr) EmitProgress(taskId string, state TaskProgressState) {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		return
	}
	if this.progress == nil {
		return
	}
	v.SetTransferState(uint32(state))
	pInfo := v.GetProgressInfo()
	log.Debugf("EmitProgress taskId: %s, state: %v pInfo: %v", taskId, state, pInfo)
	this.progress <- pInfo
}

// EmitResult. emit result or error async
func (this *TaskMgr) EmitResult(taskId string, ret interface{}, sdkErr *dspErr.Error) {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		log.Errorf("[TaskMgr EmitResult] emit result get no task")
		return
	}
	if this.progress == nil {
		log.Errorf("[TaskMgr EmitResult] progress is nil")
		return
	}
	if sdkErr != nil {
		err := v.SetResult(nil, sdkErr.Code, sdkErr.Message)
		if err != nil {
			log.Errorf("set task state err %s, %s", taskId, err)
		}
		this.AddTaskToRetry(v)
		log.Debugf("EmitResult err %v, %v", err, sdkErr)
	} else {
		this.DeleteRetryTask(taskId)
		log.Debugf("EmitResult ret %v ret == nil %t", ret, ret == nil)
		err := v.SetResult(ret, 0, "")
		if err != nil {
			log.Errorf("set task result err %s, %s", taskId, err)
		}
		if v.GetTaskType() == store.TaskTypeUpload && this.progressTicker != nil && v.GetCopyNum() > 0 {
			log.Debugf("run progress ticker")
			this.progressTicker.Run()
		}
	}
	pInfo := v.GetProgressInfo()
	this.progress <- pInfo
}

func (this *TaskMgr) AddTaskToRetry(task *Task) {
	this.lock.Lock()
	defer this.lock.Unlock()
	if task == nil {
		return
	}
	log.Debugf("task %s need retry", task.GetId())
	taskInfo := task.GetTaskInfoCopy()
	if taskInfo == nil {
		return
	}
	this.retryTasks[taskInfo.Id] = taskInfo.RetryAt
}

func (this *TaskMgr) DeleteRetryTask(taskId string) {
	this.lock.Lock()
	defer this.lock.Unlock()
	delete(this.retryTasks, taskId)
}

// RegShareNotification. register share notification
func (this *TaskMgr) RegShareNotification() {
	if this.shareNoticeCh == nil {
		this.shareNoticeCh = make(chan *ShareNotification, 0)
	}
}

// ShareNotification. get share notification channel
func (this *TaskMgr) ShareNotification() chan *ShareNotification {
	return this.shareNoticeCh
}

// CloseShareNotification. close
func (this *TaskMgr) CloseShareNotification() {
	close(this.shareNoticeCh)
	this.shareNoticeCh = nil
}

func (this *TaskMgr) NewWorkers(taskId string, walletAddrs []string, inOrder bool, job jobFunc) {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		return
	}
	err := v.SetInorder(inOrder)
	if err != nil {
		log.Errorf("[TaskMgr NewWorkers] set task inOrder failed, err: %s", err)
		return
	}
	v.NewWorkers(walletAddrs, job)
}

func (this *TaskMgr) SetWorkerUnpaid(taskId, worker string, unpaid bool) {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		return
	}
	v.SetWorkerUnPaid(worker, unpaid)
}

// WorkBackground. Run n goroutines to check request pool one second a time.
// If there exist a idle request, find the idle worker to do the job
func (this *TaskMgr) WorkBackground(taskId string) {
	tsk, ok := this.GetTaskById(taskId)
	if !ok {
		return
	}
	fileHash := tsk.GetFileHash()
	tskWalletAddr := tsk.GetWalletAddr()
	addrs := tsk.GetWorkerAddrs()
	inOrder := tsk.GetInorder()
	notifyIndex := uint64(0)
	// lock for local go routines variables
	max := len(addrs)
	if max > common.MAX_GOROUTINES_FOR_WORK_TASK {
		max = common.MAX_GOROUTINES_FOR_WORK_TASK
	}

	flightMap := &sync.Map{}
	blockCache := &sync.Map{}
	getBlockCacheLen := func() int {
		len := int(0)
		blockCache.Range(func(k, v interface{}) bool {
			len++
			return true
		})
		return len
	}

	blockIndexKey := func(hash string, index uint64) string {
		return fmt.Sprintf("%s-%d", hash, index)
	}

	type job struct {
		req       []*GetBlockReq
		worker    *Worker
		flightKey []string
	}
	jobCh := make(chan *job, max)

	type getBlocksResp struct {
		worker        *Worker
		flightKey     []string
		failedFlights []*GetBlockReq
		ret           []*BlockResp
		err           error
	}
	dropDoneCh := uint32(0)

	done := make(chan *getBlocksResp, 1)
	maxFlightLen := this.GetMaxFlightLen(tsk)
	log.Debugf("max flight len %d", maxFlightLen)
	// trigger to start
	go tsk.notifyBlockReqPoolLen()
	go func() {
		for {
			if tsk.State() == store.TaskStateDone {
				log.Debugf("task job break because task is done")
				close(jobCh)
				atomic.AddUint32(&dropDoneCh, 1)
				close(done)
				break
			}
			if tsk.State() == store.TaskStatePause || tsk.State() == store.TaskStateFailed {
				log.Debugf("task job break because task is pause or failed")
				close(jobCh)
				break
			}
			// check pool has item or no
			log.Debugf("wait for block pool notify")
			select {
			case <-tsk.blockReqPoolNotify:
			case <-time.After(time.Duration(3) * time.Second):
			}
			reqPoolLen := tsk.GetBlockReqPoolLen()
			log.Debugf("wait for block pool notify done %d", reqPoolLen)
			if reqPoolLen == 0 {
				log.Debugf("req pool is empty but state is %d", tsk.State())
				continue
			}
			// get the idle request
			var req []*GetBlockReq
			var flightKey string
			var flights []string
			pool := tsk.GetBlockReqPool()
			for _, r := range pool {
				flightKey = blockIndexKey(r.Hash, r.Index)
				if _, ok := flightMap.Load(flightKey); ok {
					continue
				}
				if _, ok := blockCache.Load(flightKey); ok {
					continue
				}
				req = append(req, r)
				flights = append(flights, flightKey)
				if len(req) == maxFlightLen {
					break
				}
			}
			if req == nil {
				continue
			}
			// get next index idle worker
			worker := tsk.GetIdleWorker(addrs, fileHash, req[0].Hash)
			if worker == nil {
				// can't find a valid worker
				log.Debugf("no idle workers of flights %s-%s to %s-%s",
					fileHash, flights[0], fileHash, flights[len(flights)-1])
				continue
			}
			for _, v := range flights {
				flightMap.Store(v, struct{}{})
			}
			if len(flights) > 0 {
				log.Debugf("add flight %s-%s to %s-%s, worker %s",
					fileHash, flights[0], fileHash, flights[len(flights)-1], worker.WalletAddr())
			}
			tsk.DelBlockReqFromPool(req)
			tsk.SetWorkerWorking(worker.walletAddr, true)
			jobCh <- &job{
				req:       req,
				flightKey: flights,
				worker:    worker,
			}
		}
		log.Debugf("outside for loop")
	}()

	go func() {
		for {
			if tsk.State() == store.TaskStateDone {
				log.Debugf("receive job task is done break")
				break
			}
			select {
			case resp, ok := <-done:
				if !ok {
					log.Debugf("done channel has close")
					break
				}
				// delete failed key
				for _, req := range resp.failedFlights {
					flightMap.Delete(blockIndexKey(req.Hash, req.Index))
				}
				for k, v := range resp.flightKey {
					if resp.err != nil {
						flightMap.Delete(v)
						// remove the request from flight
						log.Errorf("worker %s do job err continue %s", resp.worker.walletAddr, resp.err)
						continue
					}
					blockCache.Store(v, resp.ret[k])
					flightMap.Delete(v)
				}
				go func() {
					log.Debugf("notify when block reqs receive response", resp.flightKey)
					tsk.blockReqPoolNotify <- &blockReqPool{}
					log.Debugf("notify when block reqs  receive response done")
				}()
				if resp.err != nil {
					break
				}
				if len(resp.flightKey) > 0 {
					log.Debugf("store flight from %s to %s from peer %s",
						resp.flightKey[0], resp.flightKey[len(resp.flightKey)-1], resp.worker.walletAddr)
				}
				// notify outside
				if inOrder {
					blockCache.Range(func(blkKey, blktemp interface{}) bool {
						blktemp, ok := blockCache.Load(blkKey)
						if !ok {
							log.Warnf("continue because block cache not has %v!", blkKey)
							return true
						}
						blk := blktemp.(*BlockResp)
						if blk.Index != notifyIndex {
							return true
						}
						notifyIndex++
						tsk.NotifyBlock(blk)
						blockCache.Delete(blkKey)
						return true
					})
				} else {
					for _, blkKey := range resp.flightKey {
						blktemp, ok := blockCache.Load(blkKey)
						if !ok {
							log.Warnf("continue because block cache not has %v!", blkKey)
							continue
						}
						blk := blktemp.(*BlockResp)
						tsk.NotifyBlock(blk)
						blockCache.Delete(blkKey)
					}
				}
				log.Debugf("remain %d response at block cache", getBlockCacheLen())
			}
			if tsk.State() == store.TaskStatePause || tsk.State() == store.TaskStateFailed {
				log.Debugf("receive state %d", tsk.State())
				atomic.AddUint32(&dropDoneCh, 1)
				close(done)
				break
			}
		}
		log.Debugf("outside receive job task")
	}()

	// open max routine to do jobs
	log.Debugf("open %d routines to work task %s, file %s background", max, taskId, fileHash)
	for i := 0; i < max; i++ {
		go func() {
			for {
				state := tsk.State()
				if state == store.TaskStateDone || state == store.TaskStatePause || state == store.TaskStateFailed {
					log.Debugf("task is break, state: %d", state)
					break
				}
				job, ok := <-jobCh
				if !ok {
					log.Debugf("job channel has close")
					break
				}
				flights := make([]*block.Block, 0)
				sessionId, err := this.GetSessionId(taskId, job.worker.WalletAddr())
				if err != nil {
					log.Warnf("task %s get session id failed of peer %s", taskId, job.worker.WalletAddr())
				}
				for _, v := range job.req {
					b := &block.Block{
						SessionId: sessionId,
						Index:     v.Index,
						FileHash:  v.FileHash,
						Hash:      v.Hash,
						Operation: netcom.BLOCK_OP_GET,
						Payment: &payment.Payment{
							Sender: tskWalletAddr,
							Asset:  common.ASSET_USDT,
						},
					}
					flights = append(flights, b)
				}
				log.Debugf("start request block %s-%s-%d to %s-%d to %s, peer wallet: %s",
					fileHash, job.req[0].Hash, job.req[0].Index, job.req[len(job.req)-1].Hash,
					job.req[len(job.req)-1].Index, job.worker.WalletAddr(), job.worker.WalletAddr())
				ret, err := job.worker.Do(taskId, fileHash, job.worker.WalletAddr(), flights)
				tsk.SetWorkerWorking(job.worker.walletAddr, false)
				if err != nil {
					if derr, ok := err.(*dspErr.Error); ok && derr.Code == dspErr.PAY_UNPAID_BLOCK_FAILED {
						log.Errorf("request blocks paid failed %v from %s, err %s",
							job.req, job.worker.walletAddr, err)
					} else {
					}
					if len(job.req) > 0 {
						log.Errorf("request blocks %s of %s to %s from %s, err %s",
							fileHash, job.req[0].Hash, job.req[len(job.req)-1].Hash, job.worker.walletAddr, err)
					} else {
						log.Errorf("request blocks %v from %s, err %s", job.req, job.worker.walletAddr, err)
					}
				} else {
					log.Debugf("request blocks %s-%s to %s from %s success",
						fileHash, ret[0].Hash, ret[len(ret)-1].Hash, job.worker.walletAddr)
				}
				stop := atomic.LoadUint32(&dropDoneCh) > 0
				if stop {
					log.Debugf("stop when drop channel is not 0")
					break
				}
				successFlightKeys := make([]string, 0)
				failedFlightReqs := make([]*GetBlockReq, 0)
				successFlightMap := make(map[string]struct{}, 0)
				for _, v := range ret {
					key := blockIndexKey(v.Hash, v.Index)
					successFlightMap[key] = struct{}{}
					successFlightKeys = append(successFlightKeys, key)
				}
				if len(successFlightKeys) != len(job.req) {
					for _, r := range job.req {
						if _, ok := successFlightMap[blockIndexKey(r.Hash, r.Index)]; ok {
							continue
						}
						failedFlightReqs = append(failedFlightReqs, r)
					}
				}
				if len(ret) > 0 {
					log.Debugf("push flightskey from %s to %s",
						fmt.Sprintf("%s-%d", ret[0].Hash, ret[0].Index),
						fmt.Sprintf("%s-%d", ret[len(ret)-1].Hash, ret[len(ret)-1].Index))
				}

				resp := &getBlocksResp{
					worker:        job.worker,        // worker info
					flightKey:     successFlightKeys, // success flight keys
					failedFlights: failedFlightReqs,  // failed flight keys
					ret:           ret,               // success flight response
					err:           err,               // job error
				}
				if len(failedFlightReqs) > 0 {
					tsk.InsertBlockReqToPool(failedFlightReqs)
				}
				done <- resp
			}
			log.Debugf("workers outside for loop")
		}()
	}
}

func (this *TaskMgr) GetTaskWorkerState(taskId string) map[string]*WorkerState {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		return nil
	}
	return v.GetWorkerState()
}

func (this *TaskMgr) TaskNotify(taskId string) chan *BlockResp {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		return nil
	}
	return v.GetTaskNotify()
}

func (this *TaskMgr) AddBlockReq(taskId string, blockReqs []*GetBlockReq) error {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		return errors.New("task not found")
	}
	v.AddBlockReqToPool(blockReqs)
	return nil
}

func (this *TaskMgr) DelBlockReq(taskId string, blockReqs []*GetBlockReq) {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		return
	}
	v.DelBlockReqFromPool(blockReqs)
}

func (this *TaskMgr) GetBlockReqPoolLen(taskId string) (int, error) {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		return 0, errors.New("task not found")
	}
	return v.GetBlockReqPoolLen(), nil
}

func (this *TaskMgr) IsTaskCanResume(taskId string) (bool, error) {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		return false, dspErr.New(dspErr.GET_FILEINFO_FROM_DB_ERROR, "task not found: %v", taskId)
	}
	return v.IsTaskCanResume(), nil
}
func (this *TaskMgr) IsTaskCanPause(taskId string) (bool, error) {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		return false, dspErr.New(dspErr.GET_FILEINFO_FROM_DB_ERROR, "task not found: %v", taskId)
	}
	return v.IsTaskCanPause(), nil
}

func (this *TaskMgr) IsTaskPause(taskId string) (bool, error) {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		return false, dspErr.New(dspErr.GET_FILEINFO_FROM_DB_ERROR, "task not found: %v", taskId)
	}
	return v.IsTaskPause(), nil
}
func (this *TaskMgr) IsTaskDone(taskId string) (bool, error) {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		return false, dspErr.New(dspErr.GET_FILEINFO_FROM_DB_ERROR, "task not found: %v", taskId)
	}
	return v.IsTaskDone(), nil
}
func (this *TaskMgr) IsTaskCancel(taskId string) (bool, error) {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		return false, dspErr.New(dspErr.GET_FILEINFO_FROM_DB_ERROR, "task not found: %v", taskId)
	}
	return v.IsTaskCancel(), nil
}
func (this *TaskMgr) IsTaskPaying(taskId string) (bool, error) {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		return false, dspErr.New(dspErr.GET_FILEINFO_FROM_DB_ERROR, "task not found: %v", taskId)
	}
	return v.IsTaskPaying(), nil
}
func (this *TaskMgr) IsTaskPauseOrCancel(taskId string) (bool, bool, error) {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		return false, false, dspErr.New(dspErr.GET_FILEINFO_FROM_DB_ERROR, "task not found: %v", taskId)
	}
	pause, cancel := v.IsTaskPauseOrCancel()
	return pause, cancel, nil
}
func (this *TaskMgr) IsTaskStop(taskId string) (bool, error) {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		return false, dspErr.New(dspErr.GET_FILEINFO_FROM_DB_ERROR, "task not found: %v", taskId)
	}
	return v.IsTaskStop(), nil
}
func (this *TaskMgr) IsTaskPreparingOrDoing(taskId string) (bool, bool, error) {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		return false, false, dspErr.New(dspErr.GET_FILEINFO_FROM_DB_ERROR, "task not found: %v", taskId)
	}
	preparing, doing := v.IsTaskPreparingOrDoing()
	return preparing, doing, nil
}
func (this *TaskMgr) IsTaskFailed(taskId string) (bool, error) {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		return false, dspErr.New(dspErr.GET_FILEINFO_FROM_DB_ERROR, "task not found: %v", taskId)
	}
	return v.IsTaskFailed(), nil
}

func (this *TaskMgr) GetDownloadTaskIdFromUrl(url string) string {
	this.lock.RLock()
	defer this.lock.RUnlock()
	for id, t := range this.tasks {
		if t.GetTaskType() != store.TaskTypeDownload {
			continue
		}
		if t.GetUrl() != url {
			continue
		}
		return id
	}
	return ""
}

func (this *TaskMgr) GetUrlOfUploadedfile(fileHash, walletAddr string) string {
	id := this.TaskId(fileHash, walletAddr, store.TaskTypeUpload)
	v, err := GetTaskFromDB(id, this.db)
	if err != nil || v == nil || v.State() != store.TaskStateDone {
		return ""
	}
	return v.GetUrl()
}

// ActiveUploadTask. make a upload task peer active
func (this *TaskMgr) ActiveUploadTaskPeer(peerAddr string) {
	this.lock.Lock()
	defer this.lock.Unlock()
	for _, t := range this.tasks {
		if t.GetTaskType() != store.TaskTypeUpload {
			continue
		}
		t.ActiveWorker(peerAddr)
	}
}

// ActiveDownloadTaskPeer. make a download task peer active
func (this *TaskMgr) ActiveDownloadTaskPeer(peerAddr string) {
	this.lock.Lock()
	defer this.lock.Unlock()
	for _, t := range this.tasks {
		if t.GetTaskType() != store.TaskTypeDownload {
			continue
		}
		t.ActiveWorker(peerAddr)
	}
}

// IsTaskTimeout. Check is task timeout
func (this *TaskMgr) IsTaskTimeout(taskId string) (bool, error) {
	tsk, ok := this.GetTaskById(taskId)
	if !ok || tsk == nil {
		return false, fmt.Errorf("task %s not found", taskId)
	}
	return tsk.IsTimeout(), nil
}

func (this *TaskMgr) AllPeerPaidFailed(taskId string) (bool, error) {
	tsk, ok := this.GetTaskById(taskId)
	if !ok || tsk == nil {
		return false, fmt.Errorf("task %s not found", taskId)
	}
	return tsk.AllPeerPaidFailed(), nil
}

// GetTaskWorkerIdleDuration.
func (this *TaskMgr) GetTaskWorkerIdleDuration(taskId, peerAddr string) (uint64, error) {
	tsk, ok := this.GetTaskById(taskId)
	if !ok || tsk == nil {
		return 0, fmt.Errorf("task %s not found", taskId)
	}
	return tsk.WorkerIdleDuration(peerAddr), nil
}

// IsWorkerBusy. check if the worker is busy in 1 min, or net phase not equal to expected phase
func (this *TaskMgr) IsWorkerBusy(taskId, walletAddr string, excludePhase int) bool {
	this.lock.RLock()
	defer this.lock.RUnlock()
	for id, t := range this.tasks {
		if id == taskId {
			continue
		}
		phase := t.GetWorkerNetPhase(walletAddr)
		if phase == excludePhase {
			log.Debugf("%s included phase %d", walletAddr, excludePhase)
			return true
		}
		if !t.HasWorker(walletAddr) {
			continue
		}
		if t.WorkerIdleDuration(walletAddr) > 0 && t.WorkerIdleDuration(walletAddr) < 60*1000 {
			log.Debugf("%s is active", walletAddr)
			return true
		}
	}
	return false
}

func (this *TaskMgr) GetUnSlavedTasks() ([]string, error) {
	this.lock.RLock()
	defer this.lock.RUnlock()
	return this.db.UnSlavedList(store.TaskTypeUpload)
}

func (this *TaskMgr) RunGetProgress() {
	if this.progressTicker == nil {
		return
	}
	this.progressTicker.Run()
}

func (this *TaskMgr) GetUploadDoneNodeAddr(taskId string) (string, error) {
	this.lock.RLock()
	defer this.lock.RUnlock()
	nodeAddr, err := this.db.GetUploadDoneNodeAddr(taskId)
	if err != nil {
		return "", dspErr.New(dspErr.GET_FILEINFO_FROM_DB_ERROR, "upload file info not found")
	}
	return nodeAddr, nil
}

func (this *TaskMgr) GetMaxFlightLen(tsk *Task) int {
	maxFlightLen := 0
	if len(tsk.GetWorkerAddrs()) > 0 && int(tsk.GetTotalBlockCnt()) < common.MAX_REQ_BLOCK_COUNT {
		maxFlightLen = int(tsk.GetTotalBlockCnt()) / len(tsk.GetWorkerAddrs()) * 3 / 4
	}
	if maxFlightLen <= 0 || maxFlightLen > common.MAX_REQ_BLOCK_COUNT {
		maxFlightLen = common.MAX_REQ_BLOCK_COUNT
	}
	return maxFlightLen
}
