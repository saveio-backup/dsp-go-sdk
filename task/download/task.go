package download

import (
	"fmt"
	"sync"

	"github.com/saveio/dsp-go-sdk/consts"
	"github.com/saveio/dsp-go-sdk/network/message/types/file"
	"github.com/saveio/dsp-go-sdk/store"
	"github.com/saveio/dsp-go-sdk/task/base"
	"github.com/saveio/dsp-go-sdk/task/types"
	uPrefix "github.com/saveio/dsp-go-sdk/types/prefix"
	uTask "github.com/saveio/dsp-go-sdk/utils/task"
	uTime "github.com/saveio/dsp-go-sdk/utils/time"
	"github.com/saveio/themis/common/log"
)

type DownloadTask struct {
	*base.Task                                             // embedded base task
	peerPrices          map[string]*file.Payment           // download task peer price
	lastWorkerIdx       int                                // last worker index
	blockReqPool        []*types.GetBlockReq               // get block request pool
	blockReqPoolNotify  chan struct{}                      // block req pool change notify
	notify              chan *types.BlockResp              // notify download block
	blockFlightRespsMap map[string]chan []*types.BlockResp // map key <=> []*BlockResp
}

func NewDownloadTask(taskId string, taskType store.TaskType, db *store.TaskDB) *DownloadTask {
	dt := &DownloadTask{
		notify:        make(chan *types.BlockResp, consts.MAX_TASK_BLOCK_NOTIFY),
		lastWorkerIdx: -1,
	}
	dt.Task = base.NewTask(taskId, taskType, db)

	return dt
}

func InitDownloadTask(db *store.TaskDB) *DownloadTask {
	dt := &DownloadTask{
		notify:        make(chan *types.BlockResp, consts.MAX_TASK_BLOCK_NOTIFY),
		lastWorkerIdx: -1,
	}

	baseTask := &base.Task{
		DB:   db,
		Lock: new(sync.RWMutex),
	}

	dt.Task = baseTask

	return dt
}

func (this *DownloadTask) SetTaskState(newState store.TaskState) error {
	if err := this.Task.SetTaskState(newState); err != nil {
		return err
	}
	this.Lock.Lock()
	defer this.Lock.Unlock()
	info := this.GetTaskInfo()
	if info.TaskState == store.TaskStatePause {
		this.cleanBlockReqPool()
	}
	return nil
}

func (this *DownloadTask) SetBlockDownloaded(id, blockHashStr, nodeAddr string, index uint64, offset int64, links []string) error {
	this.Lock.Lock()
	defer this.Lock.Unlock()
	err := this.DB.SetBlockDownloaded(id, blockHashStr, nodeAddr, index, offset, links)
	if err != nil {
		return err
	}

	return nil
}

func (this *DownloadTask) SetBlocksDownloaded(id string, blkInfos []*store.BlockInfo) error {
	this.Lock.Lock()
	defer this.Lock.Unlock()
	err := this.DB.SetBlocksDownloaded(id, blkInfos)
	if err != nil {
		return err
	}

	return nil
}

func (this *DownloadTask) IsInOrder() bool {
	this.Lock.RLock()
	defer this.Lock.RUnlock()
	info := this.GetTaskInfo()
	return info.InOrder
}

func (this *DownloadTask) GetAsset() int32 {
	this.Lock.RLock()
	defer this.Lock.RUnlock()
	info := this.GetTaskInfo()
	return info.Asset
}

func (this *DownloadTask) GetMaxPeerCnt() int {
	this.Lock.RLock()
	defer this.Lock.RUnlock()
	info := this.GetTaskInfo()
	return info.MaxPeerCnt
}

func (this *DownloadTask) GetDecryptPwd() string {
	this.Lock.RLock()
	defer this.Lock.RUnlock()
	info := this.GetTaskInfo()
	return info.DecryptPwd
}

func (this *DownloadTask) IsFree() bool {
	this.Lock.RLock()
	defer this.Lock.RUnlock()
	info := this.GetTaskInfo()
	return info.Free
}

func (this *DownloadTask) IsSetFileName() bool {
	this.Lock.RLock()
	defer this.Lock.RUnlock()
	info := this.GetTaskInfo()
	return info.SetFileName
}

func (this *DownloadTask) PayOnLayer1() bool {
	this.Lock.RLock()
	defer this.Lock.RUnlock()
	info := this.GetTaskInfo()
	return info.PayOnL1
}

func (this *DownloadTask) SetPayOnLayer1(payOnL1 bool) error {
	this.Lock.Lock()
	defer this.Lock.Unlock()
	info := this.GetTaskInfo()
	info.PayOnL1 = payOnL1
	if this.Batch {
		return nil
	}
	return this.DB.SaveTaskInfo(info)
}

func (this *DownloadTask) setWorkerUnPaid(walletAddr string, unpaid bool) {
	w, ok := this.Workers[walletAddr]
	if !ok {
		log.Warnf("task %s, set remote peer %s unpaid failed, peer not found", this.Id, walletAddr)
		return
	}
	log.Debugf("task %s, set peer %s unpaid %v", this.Id, walletAddr, unpaid)
	w.SetUnpaid(unpaid)
}

func (this *DownloadTask) GetWorkerAddrs() []string {
	this.Lock.RLock()
	defer this.Lock.RUnlock()
	addrs := make([]string, 0, len(this.Workers))
	for k := range this.Workers {
		addrs = append(addrs, k)
	}
	return addrs
}

func (this *DownloadTask) GetWorkerState() map[string]*types.WorkerState {
	this.Lock.RLock()
	defer this.Lock.RUnlock()
	s := make(map[string]*types.WorkerState)
	for addr, w := range this.Workers {
		state := &types.WorkerState{
			Working:     w.Working(),
			Unpaid:      w.Unpaid(),
			TotalFailed: w.TotalFailed(),
		}
		s[addr] = state
	}
	return s
}

func (this *DownloadTask) GetIdleWorker(addrs []string, fileHash, reqHash string) *base.Worker {
	this.Lock.Lock()
	defer this.Lock.Unlock()
	var worker *base.Worker
	backupWorkers := make([]*base.Worker, 0)
	for i := 0; i < len(addrs); i++ {
		this.lastWorkerIdx++
		if this.lastWorkerIdx >= len(addrs) {
			this.lastWorkerIdx = 0
		}
		idx := this.lastWorkerIdx
		w := this.Workers[addrs[idx]]
		working := w.Working()
		workFailed := w.WorkFailed(reqHash)
		workUnpaid := w.Unpaid()
		failedTooMuch := w.FailedTooMuch(fileHash)
		if working || workUnpaid || workFailed || failedTooMuch {
			log.Debugf("task %s, #%d worker is working: %t, failed: %t, unpaid: %t, file: %s, block: %s",
				this.Id, i, working, workFailed, workUnpaid, fileHash, reqHash)
			if workFailed && !working && !workUnpaid && !failedTooMuch {
				backupWorkers = append(backupWorkers, w)
			}
			continue
		}
		worker = w
		break
	}
	log.Debugf("task %s, getIdleWorker %s, pool-len: %d, worker %v",
		this.Id, reqHash, len(this.blockReqPool), worker)
	if worker != nil {
		return worker
	}
	if len(backupWorkers) > 0 {
		return backupWorkers[0]
	}
	return worker
}

func (this *DownloadTask) AddBlockReqToPool(blockReqs []*types.GetBlockReq) {
	this.Lock.Lock()
	defer this.Lock.Unlock()
	if this.blockReqPool == nil {
		this.blockReqPool = make([]*types.GetBlockReq, 0)
	}
	if len(blockReqs) == 0 {
		return
	}
	info := this.GetTaskInfo()
	log.Debugf("task %s, add block req %s-%s-%d to %s-%d",
		this.Id, info.FileHash, blockReqs[0].Hash, blockReqs[0].Index,
		blockReqs[len(blockReqs)-1].Hash, blockReqs[len(blockReqs)-1].Index)
	this.blockReqPool = append(this.blockReqPool, blockReqs...)
	if len(this.Workers) == 0 {
		return
	}
	go this.notifyBlockReqPoolLen()
}

func (this *DownloadTask) InsertBlockReqToPool(blockReqs []*types.GetBlockReq) {
	this.Lock.Lock()
	defer this.Lock.Unlock()
	if this.blockReqPool == nil {
		this.blockReqPool = make([]*types.GetBlockReq, 0)
	}
	if len(blockReqs) == 0 {
		return
	}
	info := this.GetTaskInfo()
	log.Debugf("task %s, insert block req %s-%s-%d to %s-%d",
		this.Id, info.FileHash, blockReqs[0].Hash, blockReqs[0].Index,
		blockReqs[len(blockReqs)-1].Hash, blockReqs[len(blockReqs)-1].Index)
	this.blockReqPool = append(blockReqs, this.blockReqPool...)
	if len(this.Workers) == 0 {
		return
	}
	go this.notifyBlockReqPoolLen()
}

func (this *DownloadTask) DelBlockReqFromPool(blockReqs []*types.GetBlockReq) {
	this.Lock.Lock()
	defer this.Lock.Unlock()
	if this.blockReqPool == nil {
		return
	}
	if len(blockReqs) == 0 {
		return
	}
	info := this.GetTaskInfo()
	log.Debugf("task %s, del block req %s-%s-%d to %s-%d",
		this.Id, info.FileHash, blockReqs[0].Hash, blockReqs[0].Index,
		blockReqs[len(blockReqs)-1].Hash, blockReqs[len(blockReqs)-1].Index)
	hashKey := make(map[string]struct{}, 0)
	for _, r := range blockReqs {
		hashKey[fmt.Sprintf("%s-%d", r.Hash, r.Index)] = struct{}{}
	}
	newBlockReqPool := make([]*types.GetBlockReq, 0)
	for _, req := range this.blockReqPool {
		if _, ok := hashKey[fmt.Sprintf("%s-%d", req.Hash, req.Index)]; ok {
			continue
		}
		newBlockReqPool = append(newBlockReqPool, req)
	}
	this.blockReqPool = newBlockReqPool
	log.Debugf("task %s, block req pool len: %d",
		this.Id, len(this.blockReqPool))
	if len(this.Workers) == 0 {
		return
	}
	go this.notifyBlockReqPoolLen()
}

func (this *DownloadTask) GetBlockReqPool() []*types.GetBlockReq {
	this.Lock.RLock()
	defer this.Lock.RUnlock()
	return this.blockReqPool
}

func (this *DownloadTask) GetBlockReqPoolLen() int {
	return len(this.GetBlockReqPool())
}

func (this *DownloadTask) ActiveWorker(addr string) {
	this.Lock.Lock()
	defer this.Lock.Unlock()
	w, ok := this.Workers[addr]
	if !ok {
		return
	}
	w.Active()
}

func (this *DownloadTask) IsTimeout() bool {
	this.Lock.RLock()
	defer this.Lock.RUnlock()
	timeout := uint64(consts.DOWNLOAD_FILE_TIMEOUT * 1000)
	now := uTime.GetMilliSecTimestamp()
	for _, w := range this.Workers {
		if now-w.ActiveTime() < timeout {
			log.Debugf("worker active time %s %d", w.WalletAddr(), w.ActiveTime())
			return false
		}
	}
	return true
}

func (this *DownloadTask) AllPeerPaidFailed() bool {
	this.Lock.RLock()
	defer this.Lock.RUnlock()
	if len(this.Workers) == 0 {
		return false
	}
	paidFailed := true
	for _, w := range this.Workers {
		if !w.Unpaid() {
			paidFailed = false
			break
		}
	}
	return paidFailed
}

// HasWorker. check if worker exist
func (this *DownloadTask) HasWorker(addr string) bool {
	this.Lock.RLock()
	defer this.Lock.RUnlock()
	_, ok := this.Workers[addr]
	return ok
}

// WorkerIdleDuration. worker idle duration
func (this *DownloadTask) WorkerIdleDuration(addr string) uint64 {
	this.Lock.RLock()
	defer this.Lock.RUnlock()
	w, ok := this.Workers[addr]
	if !ok {
		return 0
	}
	now := uTime.GetMilliSecTimestamp()
	return now - w.ActiveTime()
}

func (this *DownloadTask) SetWorkerNetPhase(addr string, phase int) error {
	this.Lock.Lock()
	defer this.Lock.Unlock()
	info := this.GetTaskInfo()
	info.WorkerNetPhase[addr] = phase
	if this.Batch {
		return nil
	}
	return this.DB.SaveTaskInfo(info)
}

func (this *DownloadTask) GetWorkerNetPhase(addr string) int {
	this.Lock.RLock()
	defer this.Lock.RUnlock()
	info := this.GetTaskInfo()
	return info.WorkerNetPhase[addr]
}

func (this *DownloadTask) GetBlocksRoot() string {
	this.Lock.RLock()
	defer this.Lock.RUnlock()
	info := this.GetTaskInfo()
	return info.BlocksRoot
}

func (this *DownloadTask) GetMaxFlightLen() int {
	maxFlightLen := 0
	if len(this.GetWorkerAddrs()) > 0 && int(this.GetTotalBlockCnt()) < consts.MAX_REQ_BLOCK_COUNT {
		maxFlightLen = int(this.GetTotalBlockCnt()) / len(this.GetWorkerAddrs()) * 3 / 4
	}
	if maxFlightLen <= 0 || maxFlightLen > consts.MAX_REQ_BLOCK_COUNT {
		maxFlightLen = consts.MAX_REQ_BLOCK_COUNT
	}
	return maxFlightLen
}

func (this *DownloadTask) GetTaskNotify() chan *types.BlockResp {
	return this.notify
}

func (this *DownloadTask) NotifyBlock(blk *types.BlockResp) {
	log.Debugf("notify block %s-%d-%d-%s", blk.Hash, blk.Index, blk.Offset, blk.PeerAddr)
	this.notify <- blk
}

func (this *DownloadTask) NewBlockFlightsRespCh(sessionId string, timeStamp int64) chan []*types.BlockResp {
	this.Lock.Lock()
	defer this.Lock.Unlock()
	info := this.GetTaskInfo()
	key := fmt.Sprintf("%s-%s-%s-%d", this.Id, sessionId, info.FileHash, timeStamp)

	if this.blockFlightRespsMap == nil {
		this.blockFlightRespsMap = make(map[string]chan []*types.BlockResp)
	}
	ch, ok := this.blockFlightRespsMap[key]
	if !ok {
		ch = make(chan []*types.BlockResp, 1)
		this.blockFlightRespsMap[key] = ch
	}
	log.Debugf("generated block flight resp channel %s", key)
	return ch
}

func (this *DownloadTask) DropBlockFlightsRespCh(sessionId string, timeStamp int64) {
	this.Lock.Lock()
	defer this.Lock.Unlock()
	info := this.GetTaskInfo()
	key := fmt.Sprintf("%s-%s-%s-%d", this.Id, sessionId, info.FileHash, timeStamp)
	log.Debugf("drop block resp channel key: %s", key)
	delete(this.blockFlightRespsMap, key)
}

func (this *DownloadTask) BlockFlightsChannelExists(sessionId string, timeStamp int64) bool {
	this.Lock.RLock()
	defer this.Lock.RUnlock()
	info := this.GetTaskInfo()
	key := fmt.Sprintf("%s-%s-%s-%d", this.Id, sessionId, info.FileHash, timeStamp)
	_, ok := this.blockFlightRespsMap[key]
	return ok
}

func (this *DownloadTask) PushGetBlockFlights(sessionId string, blocks []*types.BlockResp, timeStamp int64) {
	this.Lock.Lock()
	defer this.Lock.Unlock()
	info := this.GetTaskInfo()
	key := fmt.Sprintf("%s-%s-%s-%d", this.Id, sessionId, info.FileHash, timeStamp)
	log.Debugf("push block to resp channel: %s", key)
	ch, ok := this.blockFlightRespsMap[key]
	if !ok {
		log.Errorf("get block resp channel is nil with key %s", key)
		return
	}
	if len(ch) > 0 {
		log.Warnf("block has pushing, ignore the new coming blocks")
		return
	}
	go func() {
		log.Debugf("send block to channel: %s", key)
		defer func() {
			if r := recover(); r != nil {
				log.Errorf("recover in push block flights to channel %v", r)
			}
		}()
		ch <- blocks
		log.Debugf("send block to channel done: %s", key)
	}()
}

func (this *DownloadTask) GetFullFilePath() string {
	this.Lock.RLock()
	defer this.Lock.RUnlock()
	info := this.GetTaskInfo()
	filePath := info.FilePath
	if len(filePath) != 0 {
		return filePath
	}

	if info.SetFileName {
		filePath = uTask.GetFileFullPath(
			this.Mgr.Config().FsFileRoot,
			info.FileHash,
			info.FileName,
			uPrefix.GetPrefixEncrypted(info.Prefix),
		)

	} else {
		filePath = uTask.GetFileFullPath(
			this.Mgr.Config().FsFileRoot,
			info.FileHash,
			"",
			uPrefix.GetPrefixEncrypted(info.Prefix),
		)
	}
	return filePath
}

func (this *DownloadTask) setTaskState(newState store.TaskState) error {
	if err := this.Task.UnsafeSetTaskState(newState); err != nil {
		return err
	}
	info := this.GetTaskInfo()
	if info.TaskState == store.TaskStatePause {
		this.cleanBlockReqPool()
	}
	return nil
}

// notifyBlockReqPoolLen. notify block req pool len
func (this *DownloadTask) notifyBlockReqPoolLen() {
	log.Debugf("task %s, notify block req update", this.Id)
	this.blockReqPoolNotify <- struct{}{}
	log.Debugf("task %s, notify block req update done", this.Id)
}

// cleanBlockReqPool. clean all block req pool. non thread-safe
func (this *DownloadTask) cleanBlockReqPool() {
	log.Debugf("CleanBlockReqPool")
	if this.blockReqPool == nil {
		this.blockReqPool = make([]*types.GetBlockReq, 0)
		return
	}
	this.blockReqPool = this.blockReqPool[:0]
	go this.notifyBlockReqPoolLen()
}

func (this *DownloadTask) setPayOnLayer1(payOnL1 bool) error {
	info := this.GetTaskInfo()
	info.PayOnL1 = payOnL1
	if this.Batch {
		return nil
	}
	return this.DB.SaveTaskInfo(info)
}

func (this *DownloadTask) SetFilePath(filePath string) error {
	this.Lock.Lock()
	defer this.Lock.Unlock()
	info := this.GetTaskInfo()
	// TODO: temp solution for concurrent update path, will fixed by refactor code
	if len(info.FilePath) != 0 {
		log.Warnf("task %s has path %s, ignore new path", this.Id, info.FilePath)
		return nil
	}
	info.FilePath = filePath
	if this.Batch {
		return nil
	}
	return this.DB.SaveTaskInfo(info)
}
