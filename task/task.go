package task

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/oniio/dsp-go-sdk/common"
	"github.com/oniio/dsp-go-sdk/store"
	"github.com/oniio/oniChain/common/log"
)

type TaskType int

const (
	TaskTypeNone TaskType = iota
	TaskTypeUpload
	TaskTypeDownload
	TaskTypeShare
	TaskTypeBackup
)

type GetBlockReq struct {
	FileHash      string
	Hash          string
	Index         int32
	PeerAddr      string
	WalletAddress string
	Asset         int32
}

type BlockResp struct {
	Hash     string
	Index    int32
	PeerAddr string
	Block    []byte
	Tag      []byte
	Offset   int64
}

type ProgressInfo struct {
	FileName string            // file name
	FileHash string            // file hash
	Total    uint64            // total file's blocks count
	Count    map[string]uint64 // address <=> count
}

type BackupFileOpt struct {
	LuckyNum   uint64
	BakNum     uint64
	BakHeight  uint64
	BackUpAddr string
	BrokenAddr string
}

type Task struct {
	fileHash   string        // task file hash
	fileName   string        // file name
	total      uint64        // total blockes count
	taskType   TaskType      // task type
	askTimeout bool          // fetch ask timeout flag
	ack        chan struct{} // fetch ack channel
	ready      bool          // fetch ready flag
	// TODO: refactor, delete below two channels
	blockReq     chan *GetBlockReq  // fetch block request channel
	blockResp    chan *BlockResp    // fetch block response channel from msg router
	blockReqPool []*GetBlockReq     // get block request pool
	workers      map[string]*Worker // workers to request block
	inOrder      bool               // keep work in order
	onlyBlock    bool               // only send block data, without tag data
	notify       chan *BlockResp    // notify download block
	done         bool               // task done
	backupOpt    *BackupFileOpt
}

// TaskMgr. implement upload/download task manager.
// only save needed information to db by fileDB, the other field save in memory
type TaskMgr struct {
	tasks      map[string]*Task
	lock       sync.RWMutex
	blockReqCh chan *GetBlockReq
	progress   chan *ProgressInfo // progress channel
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
func (this *TaskMgr) NewTask(fileHash, walletAddress string, tp TaskType) string {
	this.lock.Lock()
	defer this.lock.Unlock()
	t := &Task{
		fileHash:  fileHash,
		taskType:  tp,
		ack:       make(chan struct{}, 1),
		blockReq:  make(chan *GetBlockReq, 1),
		blockResp: make(chan *BlockResp, 1),
		notify:    make(chan *BlockResp, 100),
	}
	taskKey := this.TaskKey(fileHash, walletAddress, tp)
	this.tasks[taskKey] = t
	return taskKey
}

func (this *TaskMgr) TaskKey(fileHash, walletAddress string, tp TaskType) string {
	return fmt.Sprintf("%s-%s-%d", fileHash, walletAddress, tp)
}

func (this *TaskMgr) TaskNum() int {
	return len(this.tasks)
}

func (this *TaskMgr) BlockReqCh() chan *GetBlockReq {
	return this.blockReqCh
}

func (this *TaskMgr) TryGetTaskKey(fileHashStr, currentAddress, senderAddress string) (string, error) {
	myUploadTaskKey := this.TaskKey(fileHashStr, currentAddress, TaskTypeUpload)
	v, ok := this.tasks[myUploadTaskKey]
	if ok && v != nil {
		return myUploadTaskKey, nil
	}
	myShareTaskKey := this.TaskKey(fileHashStr, senderAddress, TaskTypeShare)
	v, ok = this.tasks[myShareTaskKey]
	if ok && v != nil {
		return myShareTaskKey, nil
	}
	return "", errors.New("task key not found")
}

// TaskType.
func (this *TaskMgr) TaskType(taskKey string) TaskType {
	this.lock.RLock()
	defer this.lock.RUnlock()
	v, ok := this.tasks[taskKey]
	if ok {
		return v.taskType
	}
	return TaskTypeNone
}

func (this *TaskMgr) TaskExist(taskKey string) bool {
	this.lock.RLock()
	defer this.lock.RUnlock()
	_, ok := this.tasks[taskKey]
	return ok
}

// DeleteTask. delete task with task id
func (this *TaskMgr) DeleteTask(taskKey string) {
	this.lock.Lock()
	defer this.lock.Unlock()
	delete(this.tasks, taskKey)
}

func (this *TaskMgr) TaskTimeout(taskKey string) (bool, error) {
	this.lock.RLock()
	defer this.lock.RUnlock()
	v, ok := this.tasks[taskKey]
	if !ok {
		return false, errors.New("task not found")
	}
	return v.askTimeout, nil
}

func (this *TaskMgr) TaskAck(taskKey string) (chan struct{}, error) {
	this.lock.RLock()
	defer this.lock.RUnlock()
	v, ok := this.tasks[taskKey]
	if !ok {
		return nil, errors.New("task not found")
	}
	return v.ack, nil
}

func (this *TaskMgr) TaskReady(taskKey string) (bool, error) {
	this.lock.RLock()
	defer this.lock.RUnlock()
	v, ok := this.tasks[taskKey]
	if !ok {
		return false, errors.New("task not found")
	}
	return v.ready, nil
}

func (this *TaskMgr) TaskBlockReq(taskKey string) (chan *GetBlockReq, error) {
	this.lock.RLock()
	defer this.lock.RUnlock()
	v, ok := this.tasks[taskKey]
	if !ok {
		return nil, errors.New("task not found")
	}
	return v.blockReq, nil
}

func (this *TaskMgr) TaskBlockResp(taskKey string) (chan *BlockResp, error) {
	this.lock.RLock()
	defer this.lock.RUnlock()
	v, ok := this.tasks[taskKey]
	if !ok {
		return nil, errors.New("task not found")
	}
	return v.blockResp, nil
}

// SetTaskTimeout. set task timeout with taskid
func (this *TaskMgr) SetTaskTimeout(taskKey string, timeout bool) {
	this.lock.Lock()
	defer this.lock.Unlock()
	v, ok := this.tasks[taskKey]
	if !ok {
		return
	}
	v.askTimeout = timeout
}

// SetTaskReady. set task is ready with taskid
func (this *TaskMgr) SetTaskReady(taskKey string, ready bool) {
	this.lock.Lock()
	defer this.lock.Unlock()
	v, ok := this.tasks[taskKey]
	if !ok {
		return
	}
	v.ready = ready
}

// SetFileName. set file name of task
func (this *TaskMgr) SetFileName(taskKey, fileName string) {
	this.lock.Lock()
	defer this.lock.Unlock()
	v, ok := this.tasks[taskKey]
	if !ok {
		return
	}
	v.fileName = fileName
}

func (this *TaskMgr) SetFileBlocksTotalCount(taskKey string, count uint64) {
	this.lock.Lock()
	defer this.lock.Unlock()
	v, ok := this.tasks[taskKey]
	if !ok {
		return
	}
	v.total = count
}

func (this *TaskMgr) SetOnlyBlock(taskKey string, only bool) {
	this.lock.Lock()
	defer this.lock.Unlock()
	v, ok := this.tasks[taskKey]
	if !ok {
		return
	}
	v.onlyBlock = only
}

func (this *TaskMgr) SetBackupOpt(taskKey string, opt *BackupFileOpt) {
	this.lock.Lock()
	defer this.lock.Unlock()
	v, ok := this.tasks[taskKey]
	if !ok {
		return
	}
	v.backupOpt = opt
}

func (this *TaskMgr) OnTaskAck(taskKey string) {
	this.lock.Lock()
	defer this.lock.Unlock()
	v, ok := this.tasks[taskKey]
	if !ok {
		return
	}
	v.ack <- struct{}{}
}

func (this *TaskMgr) OnlyBlock(taskKey string) bool {
	this.lock.RLock()
	defer this.lock.RUnlock()
	v, ok := this.tasks[taskKey]
	if !ok {
		return false
	}
	return v.onlyBlock
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

// EmitProgress. emit progress to channel with taskKey
func (this *TaskMgr) EmitProgress(taskKey string) {
	this.lock.RLock()
	defer this.lock.RUnlock()
	v, ok := this.tasks[taskKey]
	if !ok {
		return
	}
	if this.progress == nil {
		return
	}
	switch v.taskType {
	case TaskTypeUpload:
		this.progress <- &ProgressInfo{
			FileName: v.fileName,
			FileHash: v.fileHash,
			Total:    v.total,
			Count:    this.FileProgress(taskKey),
		}
	case TaskTypeDownload:
		this.progress <- &ProgressInfo{
			FileHash: v.fileHash,
			Total:    v.total,
			Count:    this.FileProgress(taskKey),
		}
	default:
	}
}

func (this *TaskMgr) NewWorkers(taskKey string, addrs []string, inOrder bool, job jobFunc) {
	this.lock.Lock()
	defer this.lock.Unlock()
	v, ok := this.tasks[taskKey]
	if !ok {
		return
	}
	v.inOrder = inOrder
	if v.workers == nil {
		v.workers = make(map[string]*Worker, 0)
	}
	for _, addr := range addrs {
		w := NewWorker(addr, job)
		v.workers[addr] = w
	}
}

// WorkBackground. Run n goroutines to check request pool one second a time.
// If there exist a idle request, find the idle worker to do the job
func (this *TaskMgr) WorkBackground(taskKey string) {
	this.lock.RLock()
	v, ok := this.tasks[taskKey]
	if !ok {
		this.lock.RUnlock()
		return
	}
	this.lock.RUnlock()
	fileHash := v.fileHash
	addrs := make([]string, 0)
	for k := range v.workers {
		addrs = append(addrs, k)
	}
	// lock for local go routines variables
	var workLock sync.Mutex
	// worker index
	idx := -1
	max := len(addrs)
	if max > common.MAX_GOROUTINES_FOR_WORK_TASK {
		max = common.MAX_GOROUTINES_FOR_WORK_TASK
	}
	// block request flights
	flight := make([]string, 0)
	flightMap := make(map[string]struct{}, 0)
	blockCache := make(map[string]*BlockResp, 0)
	log.Debugf("open %d routines to work background", max)
	for i := 0; i < max; i++ {
		go func() {
			for {
				if v.done {
					break
				}
				// check pool has item or not
				// check all pool items are in request flights
				if len(v.blockReqPool) == 0 || len(flight)+len(blockCache) >= len(v.blockReqPool) {
					time.Sleep(time.Second)
					continue
				}
				workLock.Lock()
				// get the idle request
				var req *GetBlockReq
				var flightKey string
				for _, r := range v.blockReqPool {
					flightKey = fmt.Sprintf("%s%d", r.Hash, r.Index)
					if _, ok := flightMap[flightKey]; ok {
						continue
					}
					if _, ok := blockCache[flightKey]; ok {
						continue
					}
					req = r
					break
				}
				if req == nil {
					workLock.Unlock()
					continue
				}
				// get next index idle worker
				var worker *Worker
				for i, _ := range addrs {
					idx++
					if idx >= len(addrs) {
						idx = 0
					}
					w := v.workers[addrs[idx]]
					if w.Working() || w.WorkFailed(req.Hash) || w.Unpaid() {
						log.Debugf("%d worker is working: %t, failed: %t, unpaid: %t, for %s, pool-len: %d, flight-len: %d, cache-len: %d", i, w.Working(), w.WorkFailed(req.Hash), w.Unpaid(), req.Hash, len(v.blockReqPool), len(flight), len(blockCache))
						continue
					}
					worker = w
					break
				}
				if worker == nil {
					// can't find a valid worker
					workLock.Unlock()
					log.Debugf("no worker...")
					time.Sleep(time.Duration(1) * time.Second)
					continue
				}
				flight = append(flight, flightKey)
				flightMap[flightKey] = struct{}{}
				workLock.Unlock()
				log.Debugf("start request block %s", req.Hash)
				ret, err := worker.Do(fileHash, req.Hash, worker.RemoteAddress(), req.Index, v.blockResp)
				log.Debugf("request block %s, err %s", req.Hash, err)
				workLock.Lock()
				// remove the request from flight
				for j, key := range flight {
					if key == flightKey {
						flight = append(flight[:j], flight[j+1:]...)
						break
					}
				}
				delete(flightMap, flightKey)
				if err != nil {
					workLock.Unlock()
					continue
				}
				worker.SetUnpaid(true)
				blockCache[flightKey] = ret
				// notify outside
				for _, r := range v.blockReqPool {
					blkKey := fmt.Sprintf("%s%d", r.Hash, r.Index)
					blk, ok := blockCache[blkKey]
					if !ok {
						break
					}
					v.notify <- blk
					delete(blockCache, blkKey)
					this.DelBlockReq(taskKey, r.Hash, r.Index)
				}
				workLock.Unlock()
			}
		}()
	}
}

func (this *TaskMgr) SetWorkerPaid(taskKey, addr string) {
	this.lock.Lock()
	defer this.lock.Unlock()
	v, ok := this.tasks[taskKey]
	if !ok {
		return
	}
	w, ok := v.workers[addr]
	if !ok {
		return
	}
	w.SetUnpaid(false)
}

func (this *TaskMgr) TaskNotify(taskKey string) chan *BlockResp {
	this.lock.RLock()
	defer this.lock.RUnlock()
	v, ok := this.tasks[taskKey]
	if !ok {
		return nil
	}
	return v.notify
}

func (this *TaskMgr) AddBlockReq(taskKey, blockHash string, index int32) error {
	this.lock.Lock()
	defer this.lock.Unlock()
	v, ok := this.tasks[taskKey]
	if !ok {
		return errors.New("task not found")
	}
	if v.blockReqPool == nil {
		v.blockReqPool = make([]*GetBlockReq, 0)
	}
	log.Debugf("add block req %s-%s-%d", v.fileHash, blockHash, index)
	v.blockReqPool = append(v.blockReqPool, &GetBlockReq{
		FileHash: v.fileHash,
		Hash:     blockHash,
		Index:    index,
	})
	return nil
}

func (this *TaskMgr) DelBlockReq(taskKey, blockHash string, index int32) {
	this.lock.Lock()
	defer this.lock.Unlock()
	v, ok := this.tasks[taskKey]
	if !ok {
		return
	}
	if v.blockReqPool == nil {
		return
	}
	log.Debugf("del block req %s-%s-%d", taskKey, blockHash, index)
	for i, req := range v.blockReqPool {
		if req.Hash == blockHash && req.Index == index {
			v.blockReqPool = append(v.blockReqPool[:i], v.blockReqPool[i+1:]...)
			break
		}
	}
	log.Debugf("block req pool len: %d", len(v.blockReqPool))
}

func (this *TaskMgr) SetTaskDone(taskKey string, done bool) {
	this.lock.Lock()
	defer this.lock.Unlock()
	v, ok := this.tasks[taskKey]
	if !ok {
		return
	}
	v.done = done
}
