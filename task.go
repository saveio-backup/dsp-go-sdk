package dsp

import (
	"errors"
	"sync"

	"github.com/oniio/dsp-go-sdk/common"
	"github.com/oniio/dsp-go-sdk/store"
)

type GetBlockReq struct {
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
}

type Task struct {
	fileHash   string
	peerAddr   string
	askTimeout bool
	ack        chan struct{}
	ready      bool
	blockReq   chan *GetBlockReq
	blockResp  chan *BlockResp
}

// TaskMgr. implement upload/download task manager.
// only save needed information to db by fileDB, the other field save in memory
type TaskMgr struct {
	tasks map[string]*Task
	lock  sync.RWMutex
	*store.FileDB
}

func NewTaskMgr() *TaskMgr {
	ts := make(map[string]*Task, 0)
	tmgr := &TaskMgr{
		tasks: ts,
	}
	tmgr.FileDB = store.NewFileDB(common.FILE_DB_DIR_PATH)
	return tmgr
}

func (this *TaskMgr) NewTask(fileHash string) {
	this.lock.Lock()
	defer this.lock.Unlock()
	t := &Task{
		fileHash:  fileHash,
		ack:       make(chan struct{}, 1),
		blockReq:  make(chan *GetBlockReq, 1),
		blockResp: make(chan *BlockResp, 1),
	}
	this.tasks[fileHash] = t
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
