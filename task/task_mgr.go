package task

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/saveio/dsp-go-sdk/common"
	"github.com/saveio/dsp-go-sdk/store"
	"github.com/saveio/themis/common/log"
)

// TaskMgr. implement upload/download task manager.
// only save needed information to db by fileDB, the other field save in memory
type TaskMgr struct {
	tasks         map[string]*Task
	lock          sync.RWMutex
	blockReqCh    chan *GetBlockReq
	progress      chan *ProgressInfo // progress channel
	shareNoticeCh chan *ShareNotification
	*store.FileDB
}

func NewTaskMgr() *TaskMgr {
	ts := make(map[string]*Task, 0)
	tmgr := &TaskMgr{
		tasks: ts,
	}
	tmgr.blockReqCh = make(chan *GetBlockReq, 500)
	// tmgr.FileDB = store.NewFileDB(common.FILE_DB_DIR_PATH)
	return tmgr
}

// NewTask. start a task for a file
func (this *TaskMgr) NewTask() string {
	this.lock.Lock()
	defer this.lock.Unlock()
	t := &Task{
		ack:           make(chan struct{}, 1),
		blockReq:      make(chan *GetBlockReq, 1),
		blockResp:     make(chan *BlockResp, 1),
		notify:        make(chan *BlockResp, 100),
		lastWorkerIdx: -1,
	}
	id, _ := uuid.NewUUID()
	t.id = id.String()
	this.tasks[t.id] = t
	return t.id
}

func (this *TaskMgr) SetNewTaskInfo(id, fileHash, fileName, walletAddress string, tp TaskType) error {
	this.lock.Lock()
	t := this.tasks[id]
	this.lock.Unlock()
	t.SetTaskType(tp)
	t.SetStringValue(FIELD_NAME_FILENAME, fileName)
	if len(fileHash) == 0 {
		return nil
	}
	t.SetStringValue(FIELD_NAME_FILEHASH, fileHash)
	key := fmt.Sprintf("%s-%s-%d", fileHash, walletAddress, tp)
	oldId, _ := this.GetId(key)
	if len(oldId) != 0 {
		return errors.New("task exists")
	}
	log.Debugf("SetNewTaskInfo id %s, key %s", id, key)
	switch tp {
	case TaskTypeUpload:
		this.NewFileUploadInfo(id)
	}
	this.SetIdIndex(id, key)
	return nil
}

func (this *TaskMgr) TaskId(fileHash, walletAddress string, tp TaskType) string {
	key := fmt.Sprintf("%s-%s-%d", fileHash, walletAddress, tp)
	id, _ := this.GetId(key)
	log.Debugf("TaskId key %s id %s", key, id)
	return id
}

func (this *TaskMgr) TaskNum() int {
	return len(this.tasks)
}

func (this *TaskMgr) BlockReqCh() chan *GetBlockReq {
	return this.blockReqCh
}

func (this *TaskMgr) TryGetTaskKey(fileHashStr, currentAddress, senderAddress string) (string, error) {
	myUploadTaskKey := this.TaskId(fileHashStr, currentAddress, TaskTypeUpload)
	v, ok := this.tasks[myUploadTaskKey]
	if ok && v != nil {
		return myUploadTaskKey, nil
	}
	myShareTaskKey := this.TaskId(fileHashStr, senderAddress, TaskTypeShare)
	v, ok = this.tasks[myShareTaskKey]
	if ok && v != nil {
		return myShareTaskKey, nil
	}
	return "", errors.New("task key not found")
}

func (this *TaskMgr) GetTaskById(taskId string) (*Task, bool) {
	this.lock.RLock()
	v, ok := this.tasks[taskId]
	this.lock.RUnlock()
	if !ok {
		log.Debugf("[dsp-go-sdk-taskmgr]: GetTaskByKey failed %s", taskId)
	}
	return v, ok
}

// TaskType.
func (this *TaskMgr) TaskType(taskId string) TaskType {
	v, ok := this.GetTaskById(taskId)
	if ok {
		return v.GetTaskType()
	}
	return TaskTypeNone
}

func (this *TaskMgr) TaskExist(taskId string) bool {
	_, ok := this.GetTaskById(taskId)
	return ok
}

// DeleteTask. delete task with task id
func (this *TaskMgr) DeleteTask(taskId string) {
	this.lock.Lock()
	defer this.lock.Unlock()
	delete(this.tasks, taskId)
}

func (this *TaskMgr) TaskTimeout(taskId string) (bool, error) {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		return false, errors.New("task not found")
	}
	return v.GetBoolValue(FIELD_NAME_ASKTIMEOUT), nil
}

func (this *TaskMgr) TaskAck(taskId string) (chan struct{}, error) {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		return nil, errors.New("task not found")
	}
	return v.GetAckCh(), nil
}

func (this *TaskMgr) TaskReady(taskId string) (bool, error) {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		return false, errors.New("task not found")
	}
	return v.GetBoolValue(FIELD_NAME_READY), nil
}

func (this *TaskMgr) TaskBlockReq(taskId string) (chan *GetBlockReq, error) {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		return nil, errors.New("task not found")
	}
	return v.GetBlockReq(), nil
}

func (this *TaskMgr) TaskBlockResp(taskId string) (chan *BlockResp, error) {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		return nil, errors.New("task not found")
	}
	return v.GetBlockResp(), nil
}

// SetTaskTimeout. set task timeout with taskid
func (this *TaskMgr) SetTaskTimeout(taskId string, timeout bool) {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		return
	}
	v.SetBoolValue(FIELD_NAME_ASKTIMEOUT, timeout)
}

// SetTaskReady. set task is ready with taskid
func (this *TaskMgr) SetTaskReady(taskId string, ready bool) {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		return
	}
	v.SetBoolValue(FIELD_NAME_READY, ready)
}

// SetFileName. set file name of task
func (this *TaskMgr) SetFileName(taskId, fileName string) {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		return
	}
	v.SetStringValue(FIELD_NAME_FILENAME, fileName)
}

func (this *TaskMgr) RegProgressCh() {
	if this.progress == nil {
		this.progress = make(chan *ProgressInfo, 0)
	}
}

func (this *TaskMgr) ProgressCh() chan *ProgressInfo {
	return this.progress
}

func (this *TaskMgr) CloseProgressCh() {
	close(this.progress)
	this.progress = nil
}

func (this *TaskMgr) FileNameFromTask(taskId string) string {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		return ""
	}
	return v.GetStringValue(FIELD_NAME_FILENAME)
}

func (this *TaskMgr) SetFileBlocksTotalCount(taskId string, count uint64) {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		return
	}
	v.SetTotalBlockCnt(count)
}

func (this *TaskMgr) SetOnlyBlock(taskId string, only bool) {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		return
	}
	v.SetBoolValue(FIELD_NAME_ONLYBLOCK, only)
}

func (this *TaskMgr) SetBackupOpt(taskId string, opt *BackupFileOpt) {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		return
	}
	v.SetBackupOpt(opt)
}

func (this *TaskMgr) OnTaskAck(taskId string) {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		return
	}
	v.OnTaskAck()
}

func (this *TaskMgr) OnlyBlock(taskId string) bool {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		return false
	}
	return v.GetBoolValue(FIELD_NAME_ONLYBLOCK)
}

// EmitProgress. emit progress to channel with taskId
func (this *TaskMgr) EmitProgress(taskId string) {
	v, ok := this.GetTaskById(taskId)
	log.Debugf("EmitProgress ok %t, this.progress %v", ok, this.progress)
	if !ok {
		return
	}
	if this.progress == nil {
		return
	}
	pInfo := &ProgressInfo{
		TaskKey:  taskId,
		Type:     v.GetTaskType(),
		FileName: v.GetStringValue(FIELD_NAME_FILENAME),
		FileHash: v.GetStringValue(FIELD_NAME_FILEHASH),
		Total:    v.GetTotalBlockCnt(),
		Count:    this.FileProgress(taskId),
	}
	log.Debugf("pInfo %v", pInfo)
	this.progress <- pInfo
}

func (this *TaskMgr) GetTask(taskId string) *Task {
	this.lock.RLock()
	v, ok := this.tasks[taskId]
	this.lock.RUnlock()
	if !ok {
		return nil
	}
	v.SetStringValue(FIELD_NAME_ID, taskId)
	return v
}

// EmitResult. emit result or error async
func (this *TaskMgr) EmitResult(taskId string, ret interface{}, err error) {
	v := this.GetTask(taskId)
	if v == nil {
		return
	}
	if this.progress == nil {
		return
	}
	pInfo := &ProgressInfo{
		TaskKey:  v.GetStringValue(FIELD_NAME_ID),
		Type:     v.GetTaskType(),
		FileName: v.GetStringValue(FIELD_NAME_FILENAME),
		FileHash: v.GetStringValue(FIELD_NAME_FILEHASH),
		Total:    v.GetTotalBlockCnt(),
		Count:    this.FileProgress(v.GetStringValue(FIELD_NAME_ID)),
	}
	if err != nil {
		pInfo.ErrorMsg = err.Error()
	} else if ret != nil {
		pInfo.Result = ret
	}
	go func(pi *ProgressInfo) {
		this.progress <- pi
	}(pInfo)
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
func (this *TaskMgr) EmitNotification(taskId string, state ShareState, fileHashStr, toWalletAddr string, paymentId, paymentAmount uint64) {
	n := &ShareNotification{
		TaskKey:       taskId,
		State:         state,
		FileHash:      fileHashStr,
		ToWalletAddr:  toWalletAddr,
		PaymentId:     paymentId,
		PaymentAmount: paymentAmount,
	}
	go func() {
		this.shareNoticeCh <- n
	}()
}

func (this *TaskMgr) NewWorkers(taskId string, addrs []string, inOrder bool, job jobFunc) {
	this.lock.Lock()
	v, ok := this.tasks[taskId]
	this.lock.Unlock()
	if !ok {
		return
	}
	v.SetBoolValue(FIELD_NAME_INORDER, inOrder)
	v.NewWorkers(addrs, job)
}

// WorkBackground. Run n goroutines to check request pool one second a time.
// If there exist a idle request, find the idle worker to do the job
func (this *TaskMgr) WorkBackground(taskId string) {
	this.lock.RLock()
	v, ok := this.tasks[taskId]
	this.lock.RUnlock()
	if !ok {
		return
	}
	fileHash := v.GetStringValue(FIELD_NAME_FILEHASH)
	addrs := v.GetWorkerAddrs()
	// lock for local go routines variables
	var workLock sync.Mutex
	max := len(addrs)
	if max > common.MAX_GOROUTINES_FOR_WORK_TASK {
		max = common.MAX_GOROUTINES_FOR_WORK_TASK
	}
	// block request flights
	flight := make([]string, 0)
	// flightMap := make(map[string]struct{}, 0)
	// blockCache := make(map[string]*BlockResp, 0)
	flightMap := sync.Map{}
	blockCache := sync.Map{}
	getBlockCacheLen := func() int {
		len := int(0)
		blockCache.Range(func(k, v interface{}) bool {
			len++
			return true
		})
		return len
	}
	log.Debugf("open %d routines to work background", max)
	for i := 0; i < max; i++ {
		go func() {
			for {
				if v.GetBoolValue(FIELD_NAME_DONE) {
					log.Debugf("task is done break")
					break
				}
				// check pool has item or not
				// check all pool items are in request flights
				if v.GetBlockReqPoolLen() == 0 || len(flight)+getBlockCacheLen() >= v.GetBlockReqPoolLen() {
					time.Sleep(time.Second)
					continue
				}
				// get the idle request
				var req *GetBlockReq
				var flightKey string
				for _, r := range v.GetBlockReqPool() {
					flightKey = fmt.Sprintf("%s%d", r.Hash, r.Index)
					if _, ok := flightMap.Load(flightKey); ok {
						continue
					}
					if _, ok := blockCache.Load(flightKey); ok {
						continue
					}
					req = r
					break
				}
				if req == nil {
					continue
				}
				// get next index idle worker
				worker := v.GetIdleWorker(addrs, req.Hash)
				if worker == nil {
					// can't find a valid worker
					log.Debugf("no worker...")
					time.Sleep(time.Duration(1) * time.Second)
					continue
				}
				workLock.Lock()
				flight = append(flight, flightKey)
				log.Debugf("append flightkey %v workLock:%p", flight, &workLock)
				workLock.Unlock()
				flightMap.Store(flightKey, struct{}{})
				log.Debugf("start request block %s from %s", req.Hash, worker.RemoteAddress())
				ret, err := worker.Do(fileHash, req.Hash, worker.RemoteAddress(), req.Index, v.GetBlockResp())
				log.Debugf("request block %s, err %s", req.Hash, err)
				workLock.Lock()
				// remove the request from flight
				for j, key := range flight {
					if key == flightKey {
						flight = append(flight[:j], flight[j+1:]...)
						break
					}
				}
				workLock.Unlock()
				log.Debugf("delete flight key %s %v", flightKey, flight)
				flightMap.Delete(flightKey)
				if err != nil {
					continue
				}
				v.SetWorkerUnPaid(worker.remoteAddr, true)
				log.Debugf("add flightkey to cache %s", flightKey)
				blockCache.Store(flightKey, ret)
				// notify outside
				for _, r := range v.GetBlockReqPool() {
					blkKey := fmt.Sprintf("%s%d", r.Hash, r.Index)
					blktemp, ok := blockCache.Load(blkKey)
					log.Debugf("loop req pool len %d, first %v", v.GetBlockReqPoolLen(), v.GetBlockReqPool()[0].Hash)
					if !ok {
						log.Debugf("break because block cache not has %v", blkKey)
						break
					}
					blk := blktemp.(*BlockResp)
					v.NotifyBlock(blk)
					log.Debugf("delete flightkty from cache %s", blkKey)
					blockCache.Delete(blkKey)
					this.DelBlockReq(taskId, r.Hash, r.Index)
				}
				log.Debugf("remain block cache len %d", getBlockCacheLen())
			}
			log.Debugf("outside for loop")
		}()
	}
}

func (this *TaskMgr) SetWorkerPaid(taskId, addr string) {
	this.lock.Lock()
	v, ok := this.tasks[taskId]
	this.lock.Unlock()
	if !ok {
		return
	}
	v.SetWorkerUnPaid(addr, false)
}

func (this *TaskMgr) TaskNotify(taskId string) chan *BlockResp {
	this.lock.RLock()
	v, ok := this.tasks[taskId]
	this.lock.RUnlock()
	if !ok {
		return nil
	}
	return v.GetTaskNotify()
}

func (this *TaskMgr) AddBlockReq(taskId, blockHash string, index int32) error {
	this.lock.Lock()
	v, ok := this.tasks[taskId]
	this.lock.Unlock()
	if !ok {
		return errors.New("task not found")
	}
	v.AddBlockReqToPool(blockHash, index)
	return nil
}

func (this *TaskMgr) DelBlockReq(taskId, blockHash string, index int32) {
	this.lock.Lock()
	v, ok := this.tasks[taskId]
	this.lock.Unlock()
	if !ok {
		return
	}
	v.DelBlockReqFromPool(blockHash, index)
}

func (this *TaskMgr) SetTaskDone(taskId string, done bool) {
	this.lock.Lock()
	v, ok := this.tasks[taskId]
	this.lock.Unlock()
	if !ok {
		return
	}
	v.SetBoolValue(FIELD_NAME_DONE, done)
}
