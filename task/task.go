package task

import (
	"fmt"
	"sync"

	"github.com/saveio/themis/common/log"
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
	TaskId        string
	Type          TaskType          // task type
	FileName      string            // file name
	FileHash      string            // file hash
	FilePath      string            // file path
	Total         uint64            // total file's blocks count
	Count         map[string]uint64 // address <=> count
	TaskState     TaskState         // task state
	ProgressState TaskProgressState // TaskProgressState
	Result        interface{}       // finish result
	ErrorCode     uint64            // error code
	ErrorMsg      string            // interrupt error
	CreatedAt     uint64
	UpdatedAt     uint64
}
type ShareState int

const (
	ShareStateBegin ShareState = iota
	ShareStateReceivedPaying
	ShareStateEnd
)

type ShareNotification struct {
	TaskKey       string
	State         ShareState
	FileHash      string
	ToWalletAddr  string
	PaymentId     uint64
	PaymentAmount uint64
}

type BackupFileOpt struct {
	LuckyNum   uint64
	BakNum     uint64
	BakHeight  uint64
	BackUpAddr string
	BrokenAddr string
}

type WorkerState struct {
	Working     bool
	Unpaid      bool
	TotalFailed map[string]uint32
}

const (
	FIELD_NAME_ASKTIMEOUT = iota
	FIELD_NAME_TRANSFERRING
	FIELD_NAME_INORDER
	FIELD_NAME_ONLYBLOCK
	FIELD_NAME_STATE
	FIELD_NAME_FILEHASH
	FIELD_NAME_FILENAME
	FIELD_NAME_ID
	FIELD_NAME_WALLETADDR
	FIELD_NAME_FILEPATH
	FIELD_NAME_TRANSFERSTATE
)

type TaskState int

const (
	TaskStatePause TaskState = iota
	TaskStatePrepare
	TaskStateDoing
	TaskStateDone
	TaskStateFailed
	TaskStateCancel
	TaskStateNone
)

type Task struct {
	id           string            // id
	sessionIds   map[string]string // request peerAddr <=> session id
	fileHash     string            // task file hash
	fileName     string            // file name
	total        uint64            // total blocks count
	filePath     string            // file path
	walletAddr   string            // operator wallet address
	taskType     TaskType          // task type
	transferring bool              // fetch is transferring flag
	// TODO: refactor, delete below two channels, use request and reply
	blockReq         chan *GetBlockReq          // fetch block request channel
	blockRespsMap    map[string]chan *BlockResp // map key <=> *BlockResp
	blockReqPool     []*GetBlockReq             // get block request pool
	workers          map[string]*Worker         // workers to request block
	inOrder          bool                       // keep work in order
	onlyBlock        bool                       // only send block data, without tag data
	notify           chan *BlockResp            // notify download block
	state            TaskState                  // task state
	transferingState TaskProgressState          // transfering state
	stateChange      chan TaskState             // state change between pause and resume
	backupOpt        *BackupFileOpt             // backup file options
	lock             sync.RWMutex               // lock
	lastWorkerIdx    int                        // last worker index
	createdAt        int64                      // createdAt
}

func (this *Task) SetTaskType(ty TaskType) {
	this.lock.Lock()
	defer this.lock.Unlock()
	this.taskType = ty
}

func (this *Task) GetTaskType() TaskType {
	this.lock.RLock()
	defer this.lock.RUnlock()
	return this.taskType
}

func (this *Task) SetSessionId(peerWalletAddr, id string) {
	this.lock.Lock()
	defer this.lock.Unlock()
	this.sessionIds[peerWalletAddr] = id
}

func (this *Task) GetRequestId(peerWalletAddr string) string {
	this.lock.RLock()
	defer this.lock.RUnlock()
	return this.sessionIds[peerWalletAddr]
}

func (this *Task) GetBlockReq() chan *GetBlockReq {
	this.lock.RLock()
	defer this.lock.RUnlock()
	return this.blockReq
}

func (this *Task) SetFieldValue(name int, value interface{}) {
	this.lock.Lock()
	defer this.lock.Unlock()
	switch name {
	case FIELD_NAME_TRANSFERRING:
		this.transferring = value.(bool)
	case FIELD_NAME_STATE:
		newState := value.(TaskState)
		oldState := this.state
		this.state = value.(TaskState)
		// changeFromPause := (oldState == TaskStatePause && (newState == TaskStateDoing || newState == TaskStateCancel))
		changeFromDoing := (oldState == TaskStateDoing && (newState == TaskStatePause || newState == TaskStateCancel))
		if changeFromDoing {
			log.Debugf("task: %s, send new state change: %d to %d", this.id, oldState, newState)
			this.stateChange <- newState
			log.Debugf("task: %s, send new state change: %d to %d done", this.id, oldState, newState)
		}
	case FIELD_NAME_INORDER:
		this.inOrder = value.(bool)
	case FIELD_NAME_ONLYBLOCK:
		this.onlyBlock = value.(bool)
	case FIELD_NAME_FILEHASH:
		this.fileHash = value.(string)
	case FIELD_NAME_FILENAME:
		this.fileName = value.(string)
	case FIELD_NAME_ID:
		this.id = value.(string)
	case FIELD_NAME_WALLETADDR:
		this.walletAddr = value.(string)
	case FIELD_NAME_FILEPATH:
		this.filePath = value.(string)
	case FIELD_NAME_TRANSFERSTATE:
		this.transferingState = value.(TaskProgressState)
	}
}

func (this *Task) GetBoolValue(name int) bool {
	this.lock.RLock()
	defer this.lock.RUnlock()
	switch name {
	case FIELD_NAME_TRANSFERRING:
		return this.transferring
	case FIELD_NAME_INORDER:
		return this.inOrder
	case FIELD_NAME_ONLYBLOCK:
		return this.onlyBlock

	}
	return false
}

func (this *Task) GetStringValue(name int) string {
	this.lock.RLock()
	defer this.lock.RUnlock()
	switch name {
	case FIELD_NAME_FILEHASH:
		return this.fileHash
	case FIELD_NAME_FILENAME:
		return this.fileName
	case FIELD_NAME_ID:
		return this.id
	case FIELD_NAME_WALLETADDR:
		return this.walletAddr
	case FIELD_NAME_FILEPATH:
		return this.filePath
	}
	return ""
}

func (this *Task) State() TaskState {
	this.lock.RLock()
	defer this.lock.RUnlock()
	return this.state
}

func (this *Task) TransferingState() TaskProgressState {
	this.lock.RLock()
	defer this.lock.RUnlock()
	return this.transferingState
}

func (this *Task) PushGetBlock(blockHash string, index int32, block *BlockResp) {
	this.lock.Lock()
	defer this.lock.Unlock()
	key := fmt.Sprintf("%s-%s-%s-%d", this.id, this.fileHash, blockHash, index)
	log.Debugf("push block %s", key)
	ch, ok := this.blockRespsMap[key]
	if !ok {
		log.Errorf("get block resp channel is nil with key %s", key)
		return
	}
	log.Debugf("push block done")
	go func() {
		log.Debugf("send block to channel")
		ch <- block
		log.Debugf("send block to channel done")
	}()
}

func (this *Task) GetBlockRespCh(blockHash string, index int32) chan *BlockResp {
	this.lock.Lock()
	defer this.lock.Unlock()
	key := fmt.Sprintf("%s-%s-%s-%d", this.id, this.fileHash, blockHash, index)
	if this.blockRespsMap == nil {
		this.blockRespsMap = make(map[string]chan *BlockResp)
	}
	ch, ok := this.blockRespsMap[key]
	if !ok {
		ch = make(chan *BlockResp, 1)
		this.blockRespsMap[key] = ch
	}
	return ch
}

func (this *Task) DropBlockRespCh(blockHash string, index int32) {
	this.lock.Lock()
	defer this.lock.Unlock()
	key := fmt.Sprintf("%s-%s-%s-%d", this.id, this.fileHash, blockHash, index)
	ch := this.blockRespsMap[key]
	delete(this.blockRespsMap, key)
	if ch != nil {
		close(ch)
	}
}

func (this *Task) SetTotalBlockCnt(cnt uint64) {
	this.lock.Lock()
	defer this.lock.Unlock()
	this.total = cnt
}

func (this *Task) GetTotalBlockCnt() uint64 {
	this.lock.RLock()
	defer this.lock.RUnlock()
	return this.total
}

func (this *Task) GetCreatedAt() int64 {
	this.lock.RLock()
	defer this.lock.RUnlock()
	return this.createdAt
}

func (this *Task) SetBackupOpt(opt *BackupFileOpt) {
	this.lock.Lock()
	defer this.lock.Unlock()
	this.backupOpt = opt
}

func (this *Task) NewWorkers(addrs map[string]string, job jobFunc) {
	this.lock.Lock()
	defer this.lock.Unlock()
	if this.workers == nil {
		this.workers = make(map[string]*Worker, 0)
	}
	for addr, walletAddr := range addrs {
		w := NewWorker(addr, walletAddr, job)
		this.workers[addr] = w
	}
}

func (this *Task) SetWorkerUnPaid(remoteAddr string, unpaid bool) {
	this.lock.Lock()
	defer this.lock.Unlock()
	w, ok := this.workers[remoteAddr]
	if !ok {
		return
	}
	w.SetUnpaid(unpaid)
}

func (this *Task) GetTaskNotify() chan *BlockResp {
	this.lock.RLock()
	defer this.lock.RUnlock()
	return this.notify
}

func (this *Task) AddBlockReqToPool(blockHash string, index int32) {
	this.lock.Lock()
	defer this.lock.Unlock()
	if this.blockReqPool == nil {
		this.blockReqPool = make([]*GetBlockReq, 0)
	}
	log.Debugf("add block req %s-%s-%d", this.fileHash, blockHash, index)
	this.blockReqPool = append(this.blockReqPool, &GetBlockReq{
		FileHash: this.fileHash,
		Hash:     blockHash,
		Index:    index,
	})
}

func (this *Task) DelBlockReqFromPool(blockHash string, index int32) {
	this.lock.Lock()
	defer this.lock.Unlock()
	if this.blockReqPool == nil {
		return
	}
	log.Debugf("del block req %s-%s-%d", this.id, blockHash, index)
	for i, req := range this.blockReqPool {
		if req.Hash == blockHash && req.Index == index {
			this.blockReqPool = append(this.blockReqPool[:i], this.blockReqPool[i+1:]...)
			break
		}
	}
	log.Debugf("block req pool len: %d", len(this.blockReqPool))
}

func (this *Task) CleanBlockReqPool() {
	this.lock.Lock()
	defer this.lock.Unlock()
	log.Debugf("CleanBlockReqPool")
	if this.blockReqPool == nil {
		this.blockReqPool = make([]*GetBlockReq, 0)
		return
	}
	this.blockReqPool = this.blockReqPool[:]
}

func (this *Task) GetBlockReqPool() []*GetBlockReq {
	this.lock.RLock()
	defer this.lock.RUnlock()
	return this.blockReqPool
}

func (this *Task) GetBlockReqPoolLen() int {
	return len(this.GetBlockReqPool())
}

func (this *Task) GetWorkerAddrs() []string {
	this.lock.RLock()
	defer this.lock.RUnlock()
	addrs := make([]string, 0, len(this.workers))
	for k := range this.workers {
		addrs = append(addrs, k)
	}
	return addrs
}

func (this *Task) GetWorkerState() map[string]*WorkerState {
	this.lock.Lock()
	defer this.lock.Unlock()
	s := make(map[string]*WorkerState)
	for addr, w := range this.workers {
		state := &WorkerState{
			Working:     w.Working(),
			Unpaid:      w.Unpaid(),
			TotalFailed: w.totalFailed,
		}
		s[addr] = state
	}
	return s
}

func (this *Task) GetIdleWorker(addrs []string, fileHash, reqHash string) *Worker {
	this.lock.Lock()
	defer this.lock.Unlock()
	var worker *Worker
	for i, _ := range addrs {
		this.lastWorkerIdx++
		if this.lastWorkerIdx >= len(addrs) {
			this.lastWorkerIdx = 0
		}
		idx := this.lastWorkerIdx
		w := this.workers[addrs[idx]]
		if w.Working() || w.WorkFailed(reqHash) || w.Unpaid() || w.FailedTooMuch(fileHash) {
			log.Debugf("%d worker is working: %t, failed: %t, unpaid: %t", i, w.Working(), w.WorkFailed(reqHash), w.Unpaid())
			continue
		}
		worker = w
		break
	}
	log.Debugf("GetIdleWorker %s, pool-len: %d, worker %v", reqHash, len(this.blockReqPool), worker)
	return worker
}

func (this *Task) NotifyBlock(blk *BlockResp) {
	this.lock.Lock()
	defer this.lock.Unlock()
	log.Debugf("notify block %s-%d-%d-%s", blk.Hash, blk.Index, blk.Offset, blk.PeerAddr)
	this.notify <- blk
}
