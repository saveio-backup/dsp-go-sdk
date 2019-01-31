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
)

type GetBlockReq struct {
	FileHash string
	Hash     string
	Index    int32
	PeerAddr string
}

type BlockResp struct {
	Hash     string
	Index    int32
	PeerAddr string
	Block    []byte
	Tag      []byte
	Offset   int64
}

type Task struct {
	fileHash   string        // task file hash
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
}

// TaskMgr. implement upload/download task manager.
// only save needed information to db by fileDB, the other field save in memory
type TaskMgr struct {
	tasks      map[string]*Task
	lock       sync.RWMutex
	blockReqCh chan *GetBlockReq
	*store.FileDB
}

func NewTaskMgr() *TaskMgr {
	ts := make(map[string]*Task, 0)
	tmgr := &TaskMgr{
		tasks: ts,
	}
	tmgr.blockReqCh = make(chan *GetBlockReq, 500)
	tmgr.FileDB = store.NewFileDB(common.FILE_DB_DIR_PATH)
	return tmgr
}

// NewTask. start a task for a file
func (this *TaskMgr) NewTask(fileHash string, tp TaskType) {
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
	this.tasks[fileHash] = t
}

func (this *TaskMgr) TaskNum() int {
	return len(this.tasks)
}

func (this *TaskMgr) BlockReqCh() chan *GetBlockReq {
	return this.blockReqCh
}

func (this *TaskMgr) TaskType(fileHash string) TaskType {
	this.lock.RLock()
	defer this.lock.RUnlock()
	v, ok := this.tasks[fileHash]
	if !ok {
		return TaskTypeNone
	}
	return v.taskType
}

func (this *TaskMgr) TaskExist(fileHash string) bool {
	this.lock.RLock()
	defer this.lock.RUnlock()
	_, ok := this.tasks[fileHash]
	return ok
}

func (this *TaskMgr) DeleteTask(fileHash string) {
	this.lock.Lock()
	defer this.lock.Unlock()
	delete(this.tasks, fileHash)
}

func (this *TaskMgr) TaskTimeout(fileHash string) (bool, error) {
	this.lock.RLock()
	defer this.lock.RUnlock()
	v, ok := this.tasks[fileHash]
	if !ok {
		return false, errors.New("task not found")
	}
	return v.askTimeout, nil
}

func (this *TaskMgr) TaskAck(fileHash string) (chan struct{}, error) {
	this.lock.RLock()
	defer this.lock.RUnlock()
	v, ok := this.tasks[fileHash]
	if !ok {
		return nil, errors.New("task not found")
	}
	return v.ack, nil
}

func (this *TaskMgr) TaskReady(fileHash string) (bool, error) {
	this.lock.RLock()
	defer this.lock.RUnlock()
	v, ok := this.tasks[fileHash]
	if !ok {
		return false, errors.New("task not found")
	}
	return v.ready, nil
}

func (this *TaskMgr) TaskBlockReq(fileHash string) (chan *GetBlockReq, error) {
	this.lock.RLock()
	defer this.lock.RUnlock()
	v, ok := this.tasks[fileHash]
	if !ok {
		return nil, errors.New("task not found")
	}
	return v.blockReq, nil
}

func (this *TaskMgr) TaskBlockResp(fileHash string) (chan *BlockResp, error) {
	this.lock.RLock()
	defer this.lock.RUnlock()
	v, ok := this.tasks[fileHash]
	if !ok {
		return nil, errors.New("task not found")
	}
	return v.blockResp, nil
}

func (this *TaskMgr) SetTaskTimeout(fileHash string, timeout bool) {
	this.lock.Lock()
	defer this.lock.Unlock()
	v, ok := this.tasks[fileHash]
	if !ok {
		return
	}
	v.askTimeout = timeout
}

func (this *TaskMgr) SetTaskReady(fileHash string, ready bool) {
	this.lock.Lock()
	defer this.lock.Unlock()
	v, ok := this.tasks[fileHash]
	if !ok {
		return
	}
	v.ready = ready
}

func (this *TaskMgr) OnTaskAck(fileHash string) {
	this.lock.Lock()
	defer this.lock.Unlock()
	v, ok := this.tasks[fileHash]
	if !ok {
		return
	}
	v.ack <- struct{}{}
}

func (this *TaskMgr) SetOnlyBlock(fileHash string, only bool) {
	this.lock.Lock()
	defer this.lock.Unlock()
	v, ok := this.tasks[fileHash]
	if !ok {
		return
	}
	v.onlyBlock = only
}

func (this *TaskMgr) OnlyBlock(fileHash string) bool {
	this.lock.RLock()
	defer this.lock.RUnlock()
	v, ok := this.tasks[fileHash]
	if !ok {
		return false
	}
	return v.onlyBlock
}

func (this *TaskMgr) NewWorkers(fileHash string, addrs []string, inOrder bool, job jobFunc) {
	this.lock.Lock()
	defer this.lock.Unlock()
	v, ok := this.tasks[fileHash]
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
func (this *TaskMgr) WorkBackground(fileHash string) {
	this.lock.RLock()
	v, ok := this.tasks[fileHash]
	if !ok {
		this.lock.RUnlock()
		return
	}
	this.lock.RUnlock()
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
				idx++
				if idx >= len(addrs) {
					idx = 0
				}
				worker := v.workers[addrs[idx]]
				if worker.Working() || worker.WorkFailed(req.Hash) {
					log.Debugf("worker is working for %s", req.Hash)
					workLock.Unlock()
					continue
				}
				flight = append(flight, flightKey)
				flightMap[flightKey] = struct{}{}
				workLock.Unlock()
				ret, err := worker.Do(fileHash, req.Hash, worker.RemoteAddress(), req.Index, v.blockResp)
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
					this.DelBlockReq(fileHash, r.Hash, r.Index)
				}
				workLock.Unlock()
			}
		}()
	}
}

func (this *TaskMgr) TaskNotify(fileHash string) chan *BlockResp {
	this.lock.RLock()
	defer this.lock.RUnlock()
	v, ok := this.tasks[fileHash]
	if !ok {
		return nil
	}
	return v.notify
}

func (this *TaskMgr) AddBlockReq(fileHash, blockHash string, index int32) error {
	this.lock.Lock()
	defer this.lock.Unlock()
	v, ok := this.tasks[fileHash]
	if !ok {
		return errors.New("task not found")
	}
	if v.blockReqPool == nil {
		v.blockReqPool = make([]*GetBlockReq, 0)
	}
	log.Debugf("add block req %s-%s-%d", fileHash, blockHash, index)
	v.blockReqPool = append(v.blockReqPool, &GetBlockReq{
		Hash:  blockHash,
		Index: index,
	})
	return nil
}

func (this *TaskMgr) DelBlockReq(fileHash, blockHash string, index int32) {
	this.lock.Lock()
	defer this.lock.Unlock()
	v, ok := this.tasks[fileHash]
	if !ok {
		return
	}
	if v.blockReqPool == nil {
		return
	}
	log.Debugf("del block req %s-%s-%d", fileHash, blockHash, index)
	for i, req := range v.blockReqPool {
		if req.Hash == blockHash && req.Index == index {
			v.blockReqPool = append(v.blockReqPool[:i], v.blockReqPool[i+1:]...)
			break
		}
	}
	log.Debugf("block req pool len: %d", len(v.blockReqPool))
}

func (this *TaskMgr) SetTaskDone(fileHash string, done bool) {
	this.lock.Lock()
	defer this.lock.Unlock()
	v, ok := this.tasks[fileHash]
	if !ok {
		return
	}
	v.done = done
}
