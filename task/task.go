package task

import (
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
	TaskKey  string
	Type     TaskType          //task type
	FileName string            // file name
	FileHash string            // file hash
	Total    uint64            // total file's blocks count
	Count    map[string]uint64 // address <=> count
	Result   interface{}       // finish result
	ErrorMsg string            // interrupt error
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

const (
	FIELD_NAME_ASKTIMEOUT = "asktimeout"
	FIELD_NAME_READY      = "ready"
	FIELD_NAME_INORDER    = "inorder"
	FIELD_NAME_ONLYBLOCK  = "onlyblock"
	FIELD_NAME_DONE       = "done"
	FIELD_NAME_FILEHASH   = "filehash"
	FIELD_NAME_FILENAME   = "filename"
	FIELD_NAME_ID         = "id"
)

type Task struct {
	id         string        // id
	fileHash   string        // task file hash
	fileName   string        // file name
	total      uint64        // total blockes count
	taskType   TaskType      // task type
	askTimeout bool          // fetch ask timeout flag
	ack        chan struct{} // fetch ack channel
	ready      bool          // fetch ready flag
	// TODO: refactor, delete below two channels
	blockReq      chan *GetBlockReq  // fetch block request channel
	blockResp     chan *BlockResp    // fetch block response channel from msg router
	blockReqPool  []*GetBlockReq     // get block request pool
	workers       map[string]*Worker // workers to request block
	inOrder       bool               // keep work in order
	onlyBlock     bool               // only send block data, without tag data
	notify        chan *BlockResp    // notify download block
	done          bool               // task done
	backupOpt     *BackupFileOpt     // backup file options
	lock          sync.RWMutex       // lock
	lastWorkerIdx int                // last worker index
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

func (this *Task) GetAckCh() chan struct{} {
	this.lock.RLock()
	defer this.lock.RUnlock()
	return this.ack
}

func (this *Task) GetBlockReq() chan *GetBlockReq {
	this.lock.RLock()
	defer this.lock.RUnlock()
	return this.blockReq
}

func (this *Task) GetBlockResp() chan *BlockResp {
	this.lock.RLock()
	defer this.lock.RUnlock()
	return this.blockResp
}

func (this *Task) SetBoolValue(name string, value bool) {
	this.lock.Lock()
	defer this.lock.Unlock()
	switch name {
	case FIELD_NAME_ASKTIMEOUT:
		this.askTimeout = value
	case FIELD_NAME_READY:
		this.ready = value
	case FIELD_NAME_DONE:
		this.done = value
	case FIELD_NAME_INORDER:
		this.inOrder = value
	case FIELD_NAME_ONLYBLOCK:
		this.onlyBlock = value
	}
}

func (this *Task) GetBoolValue(name string) bool {
	this.lock.RLock()
	defer this.lock.RUnlock()
	switch name {
	case FIELD_NAME_ASKTIMEOUT:
		return this.askTimeout
	case FIELD_NAME_READY:
		return this.ready
	case FIELD_NAME_DONE:
		return this.done
	case FIELD_NAME_INORDER:
		return this.inOrder
	case FIELD_NAME_ONLYBLOCK:
		return this.onlyBlock
	}
	return false
}

func (this *Task) SetStringValue(name, value string) {
	this.lock.Lock()
	defer this.lock.Unlock()
	switch name {
	case FIELD_NAME_FILEHASH:
		this.fileHash = value
	case FIELD_NAME_FILENAME:
		this.fileName = value
	case FIELD_NAME_ID:
		this.id = value
	}
}

func (this *Task) GetStringValue(name string) string {
	this.lock.RLock()
	defer this.lock.RUnlock()
	switch name {
	case FIELD_NAME_FILEHASH:
		return this.fileHash
	case FIELD_NAME_FILENAME:
		return this.fileName
	case FIELD_NAME_ID:
		return this.id
	}
	return ""
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

func (this *Task) SetBackupOpt(opt *BackupFileOpt) {
	this.lock.Lock()
	defer this.lock.Unlock()
	this.backupOpt = opt
}

func (this *Task) OnTaskAck() {
	this.lock.Lock()
	defer this.lock.Unlock()
	this.ack <- struct{}{}
}

func (this *Task) NewWorkers(addrs []string, job jobFunc) {
	this.lock.Lock()
	defer this.lock.Unlock()
	if this.workers == nil {
		this.workers = make(map[string]*Worker, 0)
	}
	for _, addr := range addrs {
		w := NewWorker(addr, job)
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
	log.Debugf("add blockReqPool %v", this.blockReqPool)
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
	log.Debugf("delete blockReqPool %v", this.blockReqPool)
}

func (this *Task) GetBlockReqPool() []*GetBlockReq {
	this.lock.RLock()
	defer this.lock.RUnlock()
	log.Debugf("get blockReqPool %v", this.blockReqPool)
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

func (this *Task) GetIdleWorker(addrs []string, reqHash string) *Worker {
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
		if w.Working() || w.WorkFailed(reqHash) || w.Unpaid() {
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
