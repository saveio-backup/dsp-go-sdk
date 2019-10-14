package task

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/saveio/dsp-go-sdk/common"
	sdkErr "github.com/saveio/dsp-go-sdk/error"
	netcom "github.com/saveio/dsp-go-sdk/network/common"
	"github.com/saveio/dsp-go-sdk/network/message/types/block"
	"github.com/saveio/dsp-go-sdk/network/message/types/payment"
	"github.com/saveio/dsp-go-sdk/store"
	"github.com/saveio/dsp-go-sdk/utils"
	"github.com/saveio/themis/common/log"
)

// TaskMgr. implement upload/download task manager.
// only save needed information to db by fileDB, the other field save in memory
type TaskMgr struct {
	tasks          map[string]*Task
	walletHostAddr map[string]string
	lock           sync.RWMutex
	blockReqCh     chan []*GetBlockReq // used for share blocks
	progress       chan *ProgressInfo  // progress channel
	shareNoticeCh  chan *ShareNotification
	db             *store.FileDB
}

func NewTaskMgr() *TaskMgr {
	ts := make(map[string]*Task, 0)
	tmgr := &TaskMgr{
		tasks: ts,
	}
	tmgr.blockReqCh = make(chan []*GetBlockReq, common.MAX_GOROUTINES_FOR_WORK_TASK)
	return tmgr
}

func (this *TaskMgr) SetFileDB(d *store.LevelDBStore) {
	this.db = store.NewFileDB(d)
}

func (this *TaskMgr) CloseDB() error {
	if this.db == nil {
		return nil
	}
	return this.db.Close()
}

// NewTask. start a task for a file
func (this *TaskMgr) NewTask(taskT store.TaskType) (string, error) {
	this.lock.Lock()
	defer this.lock.Unlock()
	t := NewTask(taskT, this.db)
	if t == nil {
		return "", fmt.Errorf("[TaskMgr NewTask] new task failed %d", taskT)
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
		return fmt.Errorf("[TaskMgr BindTaskId] task not found: %s", id)
	}
	return t.BindIdWithWalletAddr()
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
		newId, err := this.NewTask(store.TaskTypeUpload)
		if err != nil {
			return err
		}
		this.NewBatchSet(newId)
		this.SetFileHash(newId, fileHashStr)
		this.SetWalletAddr(newId, walletAddr)
		this.SetFileName(newId, fileNameMap[fileHashStr])
		err = this.BatchCommit(newId)
		if err != nil {
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
		t.SetResult(nil, sdkErr.GET_FILEINFO_FROM_DB_ERROR, "DB has damaged. Can't recover the task")
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
	delete(this.tasks, taskId)
}

// CleanTask. clean task from memory and DB
func (this *TaskMgr) CleanTask(taskId string) error {
	this.lock.Lock()
	delete(this.tasks, taskId)
	this.lock.Unlock()
	return this.db.DeleteFileInfo(taskId)
}

func (this *TaskMgr) TaskNum() int {
	return len(this.tasks)
}

func (this *TaskMgr) ShareTaskNum() int {
	this.lock.RLock()
	defer this.lock.RUnlock()
	cnt := 0
	for _, t := range this.tasks {
		if t.State() == store.TaskStateDoing && t.GetTaskType() == store.TaskTypeShare {
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
	tsk, err := this.db.GetFileInfo(taskId)
	if err != nil || tsk == nil {
		return false
	}
	return true
}

// UploadingFileHashExist. check if a uploading task has contained the file
func (this *TaskMgr) UploadingFileExist(taskId, fileHashStr string) bool {
	this.lock.RLock()
	defer this.lock.RUnlock()
	tsk := this.tasks[taskId]
	var tskCreatedAt uint64
	if tsk != nil {
		tskCreatedAt = tsk.GetCreatedAt()
	} else {
		tskCreatedAt = utils.GetMilliSecTimestamp()
	}
	for _, t := range this.tasks {
		if t.GetFileHash() == fileHashStr && t.GetId() != taskId && t.GetTaskType() == store.TaskTypeUpload && t.GetCreatedAt() < tskCreatedAt {
			log.Debugf("fileHashStr %s, taskId %s , newTaskId %s, taskCreatedAt: %d, newTaskCreatedAt: %d", fileHashStr, t.GetId(), taskId, t.GetCreatedAt(), tskCreatedAt)
			return true
		}
	}
	return false
}

func (this *TaskMgr) TaskBlockReq(taskId string) (chan []*GetBlockReq, error) {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		return nil, errors.New("task not found")
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
	v.SetTransferState(uint64(state))
	pInfo := v.GetProgressInfo()
	log.Debugf("EmitProgress taskId: %s, state: %v pInfo: %v", taskId, state, pInfo)
	this.progress <- pInfo
}

// EmitResult. emit result or error async
func (this *TaskMgr) EmitResult(taskId string, ret interface{}, sdkErr *sdkErr.SDKError) {
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
		log.Debugf("EmitResult, err %v, %v", err, sdkErr)
	} else {
		log.Debugf("EmitResult ret %v ret == nil %t", ret, ret == nil)
		err := v.SetResult(ret, 0, "")
		if err != nil {
			log.Errorf("set task result err %s, %s", taskId, err)
		}
	}
	pInfo := v.GetProgressInfo()
	this.progress <- pInfo
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

// EmitNotification. emit notification
func (this *TaskMgr) EmitNotification(taskId string, state ShareState, fileHashStr, fileName, fileOwner, toWalletAddr string, paymentId, paymentAmount uint64) {
	n := &ShareNotification{
		TaskKey:       taskId,
		State:         state,
		FileHash:      fileHashStr,
		FileName:      fileName,
		FileOwner:     fileOwner,
		ToWalletAddr:  toWalletAddr,
		PaymentId:     paymentId,
		PaymentAmount: paymentAmount,
	}
	go func() {
		this.shareNoticeCh <- n
	}()
}

func (this *TaskMgr) NewWorkers(taskId string, addrs map[string]string, inOrder bool, job jobFunc) {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		return
	}
	err := v.SetInorder(inOrder)
	if err != nil {
		log.Errorf("[TaskMgr NewWorkers] set task inOrder failed, err: %s", err)
		return
	}
	v.NewWorkers(addrs, job)
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
	getFlightMapLen := func() int {
		len := int(0)
		flightMap.Range(func(k, v interface{}) bool {
			len++
			return true
		})
		return len
	}

	type job struct {
		req       []*GetBlockReq
		worker    *Worker
		flightKey []string
	}
	jobCh := make(chan *job, max)

	type getBlocksResp struct {
		worker          *Worker
		flightKey       []string
		failedFlightKey map[string]struct{}
		ret             []*BlockResp
		err             error
	}
	dropDoneCh := uint32(0)

	done := make(chan *getBlocksResp, 1)
	go func() {
		for {
			if tsk.State() == store.TaskStateDone {
				log.Debugf("distribute job task is done break")
				close(jobCh)
				atomic.AddUint32(&dropDoneCh, 1)
				close(done)
				break
			}
			if tsk.State() == store.TaskStatePause || tsk.State() == store.TaskStateFailed {
				log.Debugf("distribute job break at pause")
				close(jobCh)
				break
			}
			// check pool has item or no
			reqPoolLen := tsk.GetBlockReqPoolLen()
			if reqPoolLen == 0 {
				log.Debug("sleeping for no request block in pool")
				time.Sleep(time.Duration(3) * time.Second)
				continue
			}
			// check all pool items are in request flights
			if getFlightMapLen()+getBlockCacheLen() >= reqPoolLen {
				log.Debug("sleeping for all request are on flights")
				time.Sleep(time.Duration(3) * time.Second)
				continue
			}
			// get the idle request
			var req []*GetBlockReq
			var flightKey string
			var flights []string
			pool := tsk.GetBlockReqPool()
			for _, r := range pool {
				flightKey = fmt.Sprintf("%s-%d", r.Hash, r.Index)
				if _, ok := flightMap.Load(flightKey); ok {
					continue
				}
				if _, ok := blockCache.Load(flightKey); ok {
					continue
				}
				req = append(req, r)
				flights = append(flights, flightKey)
				if len(req) == common.MAX_REQ_BLOCK_COUNT {
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
				log.Debugf("no worker... of flights %s-%s to %s-%s", fileHash, flights[0], fileHash, flights[len(flights)-1])
				time.Sleep(time.Duration(3) * time.Second)
				continue
			}
			for _, v := range flights {
				flightMap.Store(v, struct{}{})
			}
			if len(flights) > 0 {
				log.Debugf("add flight %s-%s to %s-%s, worker %s", fileHash, flights[0], fileHash, flights[len(flights)-1], worker.RemoteAddress())
			}

			tsk.SetWorkerUnPaid(worker.remoteAddr, true)
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
				for k, _ := range resp.failedFlightKey {
					flightMap.Delete(k)
				}
				for k, v := range resp.flightKey {

					log.Debugf("receive response of flight %s, err %s", v, resp.err)
					if resp.err != nil {
						flightMap.Delete(v)
						// remove the request from flight
						log.Errorf("worker %s do job err continue %s", resp.worker.remoteAddr, resp.err)
						continue
					}
					log.Debugf("add flightkey to cache %s, blockhash %s", v, resp.ret[k].Hash)
					blockCache.Store(v, resp.ret[k])
					flightMap.Delete(v)
					// notify outside
					pool := tsk.GetBlockReqPool()
					type toDeleteInfo struct {
						hash  string
						index int32
					}
					toDelete := make([]*toDeleteInfo, 0)
					for poolIdx, r := range pool {
						blkKey := fmt.Sprintf("%s-%d", r.Hash, r.Index)
						_, ok := blockCache.Load(blkKey)
						log.Debugf("loop req poolIdx %d pool %v", poolIdx, blkKey)
						if !ok {
							log.Debugf("break because block cache not has %v", blkKey)
							break
						}
						toDelete = append(toDelete, &toDeleteInfo{
							hash:  r.Hash,
							index: r.Index,
						})
					}
					log.Debugf("remove %d response from req pool", len(toDelete))
					for _, toD := range toDelete {
						blkKey := fmt.Sprintf("%s-%d", toD.hash, toD.index)
						blktemp, ok := blockCache.Load(blkKey)
						if !ok {
							log.Warnf("break because block cache not has %v!", blkKey)
							continue
						}
						this.DelBlockReq(taskId, toD.hash, toD.index)
						blk := blktemp.(*BlockResp)
						log.Debugf("notify flightkey from cache %s-%d", blk.Hash, blk.Index)
						tsk.NotifyBlock(blk)
						blockCache.Delete(blkKey)
					}
				}
				log.Debugf("remain %d response at block cache", getBlockCacheLen())
				log.Debugf("receive response process done")
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
	log.Debugf("open %d routines to work background", max)
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

				allFlightskey := make(map[string]struct{}, 0)
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
					allFlightskey[fmt.Sprintf("%s-%d", v.Hash, v.Index)] = struct{}{}
				}
				log.Debugf("start request block %s-%s to %s from %s, peer wallet: %s", fileHash, job.req[0].Hash, job.req[len(job.req)-1].Hash, job.worker.RemoteAddress(), job.worker.WalletAddr())
				ret, err := job.worker.Do(taskId, fileHash, job.worker.RemoteAddress(), job.worker.WalletAddr(), flights)
				tsk.SetWorkerUnPaid(job.worker.remoteAddr, false)
				if err != nil {
					if len(job.req) > 0 {
						log.Errorf("request blocks %s of %s to %s from %s, err %s", fileHash, job.req[0].Hash, job.req[len(job.req)-1].Hash, job.worker.remoteAddr, err)
					} else {
						log.Errorf("request blocks %v from %s, err %s", job.req, job.worker.remoteAddr, err)
					}
				} else {
					log.Debugf("request block %s-%s to %s from %s success", fileHash, ret[0].Hash, ret[len(ret)-1].Hash, job.worker.remoteAddr)
				}
				stop := atomic.LoadUint32(&dropDoneCh) > 0
				if stop {
					log.Debugf("stop when drop channel is not 0")
					break
				}
				flightskey := make([]string, 0)
				for _, v := range ret {
					key := fmt.Sprintf("%s-%d", v.Hash, v.Index)
					flightskey = append(flightskey, key)
					log.Debugf("push flightskey %q", key)
					delete(allFlightskey, key)
				}
				resp := &getBlocksResp{
					worker:          job.worker,
					flightKey:       flightskey,
					failedFlightKey: allFlightskey,
					ret:             ret,
					err:             err,
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

func (this *TaskMgr) AddBlockReq(taskId, blockHash string, index int32) error {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		return errors.New("task not found")
	}
	v.AddBlockReqToPool(blockHash, index)
	return nil
}

func (this *TaskMgr) DelBlockReq(taskId, blockHash string, index int32) {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		return
	}
	v.DelBlockReqFromPool(blockHash, index)
}

func (this *TaskMgr) IsTaskCanResume(taskId string) (bool, error) {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		return false, fmt.Errorf("task not found: %v", taskId)
	}
	state := v.State()
	if state != store.TaskStatePrepare && state != store.TaskStatePause && state != store.TaskStateDoing {
		return false, fmt.Errorf("can't resume the task, it's state: %d", state)
	}
	if state == store.TaskStatePause {
		return true, nil
	}
	return false, nil
}

func (this *TaskMgr) IsTaskCanPause(taskId string) (bool, error) {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		return false, fmt.Errorf("task not found: %v", taskId)
	}
	state := v.State()
	if state != store.TaskStatePrepare && state != store.TaskStatePause && state != store.TaskStateDoing {
		return false, fmt.Errorf("can't pause the task, it's state: %d", state)
	}
	if state == store.TaskStateDoing || state == store.TaskStatePrepare {
		return true, nil
	}
	return false, nil
}

func (this *TaskMgr) IsTaskPause(taskId string) (bool, error) {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		return false, fmt.Errorf("task: %s, not exist", taskId)
	}
	return v.State() == store.TaskStatePause, nil
}

func (this *TaskMgr) IsTaskDone(taskId string) (bool, error) {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		return false, fmt.Errorf("task: %s, not exist", taskId)
	}
	return v.State() == store.TaskStateDone, nil
}

func (this *TaskMgr) IsTaskCancel(taskId string) (bool, error) {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		return false, fmt.Errorf("task: %s, not exist", taskId)
	}
	log.Debugf("task state %s, %d", taskId, v.State())
	return v.State() == store.TaskStateCancel, nil
}

func (this *TaskMgr) IsTaskPaying(taskId string) (bool, error) {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		return false, fmt.Errorf("task: %s, not exist", taskId)
	}
	log.Debugf("task detail state %s, %d", taskId, v.DetailState())
	return v.DetailState() == TaskUploadFilePaying || v.DetailState() == TaskDownloadPayForBlocks, nil
}

func (this *TaskMgr) IsTaskPauseOrCancel(taskId string) (bool, bool, error) {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		return false, false, fmt.Errorf("task: %s, not exist", taskId)
	}
	state := v.State()
	return state == store.TaskStatePause, state == store.TaskStateCancel, nil
}

func (this *TaskMgr) IsTaskStop(taskId string) (bool, error) {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		return false, fmt.Errorf("task: %s, not exist", taskId)
	}
	state := v.State()
	if state != store.TaskStatePause && state != store.TaskStateCancel {
		return false, nil
	}
	return state == store.TaskStatePause || state == store.TaskStateCancel, nil
}

func (this *TaskMgr) IsTaskPreparingOrDoing(taskId string) (bool, bool, error) {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		return false, false, fmt.Errorf("task: %s, not exist", taskId)
	}
	state := v.State()
	return state == store.TaskStatePrepare, state == store.TaskStateDoing, nil
}

func (this *TaskMgr) IsTaskFailed(taskId string) (bool, error) {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		return false, fmt.Errorf("task: %s, not exist", taskId)
	}
	log.Debugf("v.state: %d", v.State())
	return v.State() == store.TaskStateFailed, nil
}

func (this *TaskMgr) GetDownloadTaskIdFromUrl(url string) string {
	this.lock.Lock()
	defer this.lock.Unlock()
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
