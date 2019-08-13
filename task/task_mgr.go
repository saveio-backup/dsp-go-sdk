package task

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/saveio/dsp-go-sdk/common"
	sdkErr "github.com/saveio/dsp-go-sdk/error"
	"github.com/saveio/dsp-go-sdk/store"
	"github.com/saveio/dsp-go-sdk/utils"
	"github.com/saveio/themis/common/log"

	fs "github.com/saveio/themis/smartcontract/service/native/savefs"
)

// TaskMgr. implement upload/download task manager.
// only save needed information to db by fileDB, the other field save in memory
type TaskMgr struct {
	tasks          map[string]*Task
	walletHostAddr map[string]string
	lock           sync.RWMutex
	blockReqCh     chan *GetBlockReq  // used for share blocks
	progress       chan *ProgressInfo // progress channel
	shareNoticeCh  chan *ShareNotification
	db             *store.FileDB
}

func NewTaskMgr() *TaskMgr {
	ts := make(map[string]*Task, 0)
	tmgr := &TaskMgr{
		tasks: ts,
	}
	tmgr.blockReqCh = make(chan *GetBlockReq, 500)
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
func (this *TaskMgr) NewTask(taskT TaskType) (string, error) {
	this.lock.Lock()
	defer this.lock.Unlock()
	t := &Task{
		blockReq:      make(chan *GetBlockReq, common.MAX_TASK_BLOCK_REQ),
		notify:        make(chan *BlockResp, common.MAX_TASK_BLOCK_NOTIFY),
		lastWorkerIdx: -1,
		createdAt:     time.Now().Unix(),
		taskType:      taskT,
		sessionIds:    make(map[string]string, common.MAX_TASK_SESSION_NUM),
		state:         TaskStatePrepare,
		stateChange:   make(chan TaskState, 1),
	}
	id, _ := uuid.NewUUID()
	t.id = id.String()
	this.tasks[t.id] = t
	var err error
	switch taskT {
	case TaskTypeUpload:
		err = this.db.NewFileInfo(id.String(), store.FileInfoTypeUpload)
	case TaskTypeDownload:
		err = this.db.NewFileInfo(id.String(), store.FileInfoTypeDownload)
	case TaskTypeShare:
		err = this.db.NewFileInfo(id.String(), store.FileInfoTypeShare)
	}

	if err != nil {
		return "", err
	}
	return t.id, nil
}

// BindTaskId. set key to taskId, for upload task, if fileHash is empty, use Hex(filePath) instead.
// for download/share task, use fileHash
func (this *TaskMgr) BindTaskId(id string) error {
	this.lock.Lock()
	t := this.tasks[id]
	this.lock.Unlock()
	hash := ""
	switch t.taskType {
	case TaskTypeUpload:
		if len(t.fileHash) == 0 {
			hash = utils.StringToSha256Hex(t.filePath)
		} else {
			hash = t.fileHash
		}
	default:
		hash = t.fileHash
	}
	key := this.TaskIdKey(hash, t.walletAddr, t.taskType)
	return this.db.SaveFileInfoId(key, id)
}

// RecoverUndoneTask. recover unfinished task from DB
func (this *TaskMgr) RecoverUndoneTask() error {
	this.lock.Lock()
	defer this.lock.Unlock()
	taskIds, err := this.db.UndoneList(store.FileInfoTypeUpload)
	log.Debugf("recover upload task : %v", taskIds)
	if err != nil {
		return err
	}
	downloadTaskIds, err := this.db.UndoneList(store.FileInfoTypeDownload)
	log.Debugf("recover download task : %v", downloadTaskIds)
	if err != nil {
		return err
	}
	taskIds = append(taskIds, downloadTaskIds...)
	log.Debugf("total recover task len: %d", len(taskIds))
	for _, id := range taskIds {
		info, err := this.db.GetFileInfo([]byte(id))
		if err != nil {
			return err
		}
		if info == nil {
			log.Warnf("get file info is nil of %v", id)
			continue
		}
		log.Debugf("info :%v", info)
		state := TaskState(info.TaskState)
		if state == TaskStateDoing {
			state = TaskStatePause
		}
		t := &Task{
			id:            id,
			fileHash:      info.FileHash,
			fileName:      info.FileName,
			total:         info.TotalBlockCount,
			copyNum:       info.CopyNum,
			filePath:      info.FilePath,
			walletAddr:    info.WalletAddress,
			createdAt:     int64(info.CreatedAt),
			blockReq:      make(chan *GetBlockReq, common.MAX_TASK_BLOCK_REQ),
			notify:        make(chan *BlockResp, common.MAX_TASK_BLOCK_NOTIFY),
			lastWorkerIdx: -1,
			sessionIds:    make(map[string]string, common.MAX_TASK_SESSION_NUM),
			state:         state,
			stateChange:   make(chan TaskState, 1),
		}
		sessions, err := this.db.GetFileSessions(id)
		if err != nil {
			return err
		}
		for _, session := range sessions {
			log.Debugf("set setssion : %s %s", session.WalletAddr, session.SessionId)
			t.SetSessionId(session.WalletAddr, session.SessionId)
		}
		log.Debugf("recover task %s, state %d", id, t.state)
		switch info.InfoType {
		case store.FileInfoTypeUpload:
			t.taskType = TaskTypeUpload
			opt, _ := this.GetFileUploadOptions(id)
			if opt != nil {
				t.storeType = opt.StorageType
			}
		case store.FileInfoTypeDownload:
			t.taskType = TaskTypeDownload
		case store.FileInfoTypeShare:
			t.taskType = TaskTypeShare
		}
		this.tasks[id] = t
	}
	return nil
}

func (this *TaskMgr) TaskIdKey(hash, walletAddress string, taskType TaskType) string {
	key := fmt.Sprintf("%s-%s-%d", hash, walletAddress, taskType)
	return key
}

func (this *TaskMgr) SetTaskInfos(id, fileHash, filePath, fileName, walletAddress string) {
	this.lock.Lock()
	t := this.tasks[id]
	this.lock.Unlock()
	m := make(map[int]interface{})
	t.SetFieldValue(FIELD_NAME_FILENAME, fileName)
	m[store.FILEINFO_FIELD_FILENAME] = fileName
	if len(walletAddress) != 0 {
		t.SetFieldValue(FIELD_NAME_WALLETADDR, walletAddress)
		m[store.FILEINFO_FIELD_WALLETADDR] = walletAddress
	}
	if len(filePath) != 0 {
		t.SetFieldValue(FIELD_NAME_FILEPATH, filePath)
	}
	if len(fileHash) != 0 {
		t.SetFieldValue(FIELD_NAME_FILEHASH, fileHash)
		m[store.FILEINFO_FIELD_FILEHASH] = fileHash
	}
	this.db.SetFileInfoFields(id, m)
}

func (this *TaskMgr) SetCopyNum(id string, copyNum uint64) {
	this.db.SetFileInfoField(id, store.FILEINFO_FIELD_COPYNUM, copyNum)
}

// TaskId from hash-walletaddress-type
func (this *TaskMgr) TaskId(prefix, walletAddress string, tp TaskType) string {
	var key string
	switch tp {
	case TaskTypeUpload:
		// use filePath to get id
		hexStr := utils.StringToSha256Hex(prefix)
		key = this.TaskIdKey(hexStr, walletAddress, tp)
		id, _ := this.db.GetFileInfoId(key)
		if len(id) > 0 {
			return id
		}
		// use fileHash to get id
		key = this.TaskIdKey(prefix, walletAddress, tp)
		id, _ = this.db.GetFileInfoId(key)
		return id
	case TaskTypeDownload, TaskTypeShare:
		key = this.TaskIdKey(prefix, walletAddress, tp)
		id, _ := this.db.GetFileInfoId(key)
		return id
	}
	return ""
}

// DeleteTask. delete task with task id
func (this *TaskMgr) DeleteTask(taskId string, deleteStore bool) error {
	if !deleteStore {
		this.lock.Lock()
		defer this.lock.Unlock()
		delete(this.tasks, taskId)
		return nil
	}
	var fileHash, walletAddress, filePath string
	var tp TaskType
	this.lock.Lock()
	task, ok := this.tasks[taskId]
	if !ok {
		fileInfo, err := this.db.GetFileInfo([]byte(taskId))
		if err != nil {
			this.lock.Unlock()
			return err
		}
		fileHash = fileInfo.FileHash
		walletAddress = fileInfo.WalletAddress
		switch fileInfo.InfoType {
		case store.FileInfoTypeUpload:
			tp = TaskTypeUpload
		case store.FileInfoTypeDownload:
			tp = TaskTypeDownload
		case store.FileInfoTypeShare:
			tp = TaskTypeShare
		}
		filePath = fileInfo.FilePath
		log.Debugf("get value from db hash :%v, wallet: %v, tp: %v, path: %v", fileHash, walletAddress, tp, filePath)
	} else {
		fileHash = task.fileHash
		walletAddress = task.walletAddr
		tp = task.taskType
		filePath = task.filePath
	}
	delete(this.tasks, taskId)
	this.lock.Unlock()
	log.Debugf(" will delete db info")
	if tp == TaskTypeUpload {
		hexStr := utils.StringToSha256Hex(filePath)
		key := this.TaskIdKey(hexStr, walletAddress, tp)
		err := this.db.DeleteFileInfoId(key)
		if err != nil {
			log.Debugf("delete file innfo err %s", err)
			return err
		}
	}
	key := this.TaskIdKey(fileHash, walletAddress, tp)
	log.Debugf("delete local file info key %s", key)
	err := this.db.DeleteFileInfoId(key)
	if err != nil {
		return err
	}
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
		if t.state == TaskStateDoing && t.taskType == TaskTypeShare {
			cnt++
		}
	}
	return cnt
}

func (this *TaskMgr) BlockReqCh() chan *GetBlockReq {
	return this.blockReqCh
}

func (this *TaskMgr) SetSessionId(taskId, peerWalletAddr, id string) {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		return
	}
	v.SetSessionId(peerWalletAddr, id)
}

func (this *TaskMgr) GetSeesionId(taskId, peerWalletAddr string) (string, error) {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		return "", fmt.Errorf("get request id failed, no task of %s", taskId)
	}
	switch v.taskType {
	case TaskTypeUpload:
		// upload or share task, local node is a server
		return taskId, nil
	case TaskTypeShare:
		return taskId, nil
	case TaskTypeDownload:
		return v.GetRequestId(peerWalletAddr), nil
	}
	return "", fmt.Errorf("unknown task type %d", v.taskType)
}

func (this *TaskMgr) GetTaskById(taskId string) (*Task, bool) {
	this.lock.RLock()
	v, ok := this.tasks[taskId]
	this.lock.RUnlock()
	if !ok {
		log.Debugf("[dsp-go-sdk-taskmgr]: GetTaskById failed %s", taskId)
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

func (this *TaskMgr) TaskFileHash(taskId string) string {
	v, ok := this.GetTaskById(taskId)
	if ok {
		return v.GetStringValue(FIELD_NAME_FILEHASH)
	}
	return ""
}

func (this *TaskMgr) TaskExist(taskId string) bool {
	_, ok := this.GetTaskById(taskId)
	return ok
}

func (this *TaskMgr) TaskTimeout(taskId string) (bool, error) {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		return false, errors.New("task not found")
	}
	return v.GetBoolValue(FIELD_NAME_ASKTIMEOUT), nil
}

func (this *TaskMgr) TaskTransferring(taskId string) (bool, error) {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		return false, errors.New("task not found")
	}
	return v.GetBoolValue(FIELD_NAME_TRANSFERRING), nil
}

func (this *TaskMgr) TaskBlockReq(taskId string) (chan *GetBlockReq, error) {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		return nil, errors.New("task not found")
	}
	return v.GetBlockReq(), nil
}

func (this *TaskMgr) PushGetBlock(taskId, sessionId string, block *BlockResp) {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		return
	}
	v.PushGetBlock(sessionId, block.Hash, block.Index, block)
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

// SetTaskTimeout. set task timeout with taskid
func (this *TaskMgr) SetTaskTimeout(taskId string, timeout bool) {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		return
	}
	v.SetFieldValue(FIELD_NAME_ASKTIMEOUT, timeout)
}

// SetTaskTransferring. set task is transferring with taskid
func (this *TaskMgr) SetTaskTransferring(taskId string, transferring bool) {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		return
	}
	v.SetFieldValue(FIELD_NAME_TRANSFERRING, transferring)
}

// SetFileName. set file name of task
func (this *TaskMgr) SetFileName(taskId, fileName string) {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		return
	}
	v.SetFieldValue(FIELD_NAME_FILENAME, fileName)
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

func (this *TaskMgr) FileNameFromTask(taskId string) string {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		return ""
	}
	return v.GetStringValue(FIELD_NAME_FILENAME)
}

func (this *TaskMgr) SetOnlyBlock(taskId string, only bool) {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		return
	}
	v.SetFieldValue(FIELD_NAME_ONLYBLOCK, only)
}

func (this *TaskMgr) SetBackupOpt(taskId string, opt *BackupFileOpt) {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		return
	}
	v.SetBackupOpt(opt)
}

func (this *TaskMgr) OnlyBlock(taskId string) bool {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		return false
	}
	return v.GetBoolValue(FIELD_NAME_ONLYBLOCK)
}

func (this *TaskMgr) GetProgressInfo(taskId string) *ProgressInfo {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		return nil
	}
	pInfo := &ProgressInfo{
		TaskId:    taskId,
		Type:      v.GetTaskType(),
		StoreType: v.GetStoreType(),
		FileName:  v.GetStringValue(FIELD_NAME_FILENAME),
		FileHash:  v.GetStringValue(FIELD_NAME_FILEHASH),
		FilePath:  v.GetStringValue(FIELD_NAME_FILEPATH),
		Total:     v.GetTotalBlockCnt(),
		CopyNum:   v.GetCopyNum(),
		Count:     this.db.FileProgress(taskId),
		TaskState: v.State(),
		CreatedAt: uint64(v.GetCreatedAt()),
		UpdatedAt: uint64(time.Now().Unix()),
	}
	return pInfo
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
	log.Debugf("EmitProgress taskId: %s, state: %v path %v", taskId, state, v.GetStringValue(FIELD_NAME_FILEPATH))
	v.SetFieldValue(FIELD_NAME_TRANSFERSTATE, state)
	pInfo := &ProgressInfo{
		TaskId:        taskId,
		Type:          v.GetTaskType(),
		StoreType:     v.GetStoreType(),
		FileName:      v.GetStringValue(FIELD_NAME_FILENAME),
		FileHash:      v.GetStringValue(FIELD_NAME_FILEHASH),
		FilePath:      v.GetStringValue(FIELD_NAME_FILEPATH),
		Total:         v.GetTotalBlockCnt(),
		CopyNum:       v.GetCopyNum(),
		Count:         this.db.FileProgress(taskId),
		TaskState:     v.State(),
		ProgressState: state,
		CreatedAt:     uint64(v.GetCreatedAt()),
		UpdatedAt:     uint64(time.Now().Unix()),
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
	v.SetFieldValue(FIELD_NAME_ID, taskId)
	return v
}

// EmitResult. emit result or error async
func (this *TaskMgr) EmitResult(taskId string, ret interface{}, sdkErr *sdkErr.SDKError) {
	v := this.GetTask(taskId)
	if v == nil {
		log.Errorf("emit result get no task")
		return
	}
	if this.progress == nil {
		log.Errorf("progress is nil")
		return
	}
	log.Debugf("emit result filepath %v", v.GetStringValue(FIELD_NAME_FILEPATH))
	pInfo := &ProgressInfo{
		TaskId:    taskId,
		Type:      v.GetTaskType(),
		StoreType: v.GetStoreType(),
		FileName:  v.GetStringValue(FIELD_NAME_FILENAME),
		FileHash:  v.GetStringValue(FIELD_NAME_FILEHASH),
		FilePath:  v.GetStringValue(FIELD_NAME_FILEPATH),
		Total:     v.GetTotalBlockCnt(),
		CopyNum:   v.GetCopyNum(),
		Count:     this.db.FileProgress(v.GetStringValue(FIELD_NAME_ID)),
		CreatedAt: uint64(v.GetCreatedAt()),
		UpdatedAt: uint64(time.Now().Unix()),
	}
	if sdkErr != nil {
		pInfo.ErrorCode = sdkErr.Code
		pInfo.ErrorMsg = sdkErr.Message
		err := this.SetTaskState(taskId, TaskStateFailed)
		if err != nil {
			log.Errorf("set task state err %s, %s", taskId, err)
		}
		log.Debugf("EmitResult, err %v, %v", err, sdkErr)
	} else if ret != nil {
		log.Debugf("EmitResult ret %v ret == nil %t", ret, ret == nil)
		pInfo.Result = ret
		err := this.SetTaskState(taskId, TaskStateDone)
		if err != nil {
			log.Errorf("set task state err %s, %s", taskId, err)
		}
	}
	pInfo.TaskState = v.State()
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

func (this *TaskMgr) NewWorkers(taskId string, addrs map[string]string, inOrder bool, job jobFunc) {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		return
	}
	v.SetFieldValue(FIELD_NAME_INORDER, inOrder)
	v.NewWorkers(addrs, job)
}

// WorkBackground. Run n goroutines to check request pool one second a time.
// If there exist a idle request, find the idle worker to do the job
func (this *TaskMgr) WorkBackground(taskId string) {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		return
	}
	fileHash := v.GetStringValue(FIELD_NAME_FILEHASH)
	addrs := v.GetWorkerAddrs()
	// lock for local go routines variables
	max := len(addrs)
	if max > common.MAX_GOROUTINES_FOR_WORK_TASK {
		max = common.MAX_GOROUTINES_FOR_WORK_TASK
	}

	// pengding block count. including block at flight or at cache both
	pendingCount := int32(0)
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

	type job struct {
		req       *GetBlockReq
		worker    *Worker
		flightKey string
	}
	jobCh := make(chan *job, max)

	type getBlockResp struct {
		worker    *Worker
		flightKey string
		ret       *BlockResp
		err       error
	}
	dropDoneCh := uint32(0)

	done := make(chan *getBlockResp, 1)
	go func() {
		for {
			if v.State() == TaskStateDone {
				log.Debugf("distribute job task is done break")
				close(jobCh)
				atomic.AddUint32(&dropDoneCh, 1)
				close(done)
				break
			}
			if v.State() == TaskStatePause || v.State() == TaskStateFailed {
				log.Debugf("distribute job break at pause")
				close(jobCh)
				break
			}
			// check pool has item or no
			// check all pool items are in request flights
			if v.GetBlockReqPoolLen() == 0 || atomic.LoadInt32(&pendingCount) >= int32(v.GetBlockReqPoolLen()) {
				// if v.GetBlockReqPoolLen() == 0 || len(flight)+getBlockCacheLen() >= v.GetBlockReqPoolLen() {
				log.Debugf("sleep for pending block...")
				time.Sleep(time.Duration(3) * time.Second)
				continue
			}
			// get the idle request
			var req *GetBlockReq
			var flightKey string
			pool := v.GetBlockReqPool()
			for _, r := range pool {
				flightKey = fmt.Sprintf("%s-%d", r.Hash, r.Index)
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
			worker := v.GetIdleWorker(addrs, fileHash, req.Hash)
			if worker == nil {
				// can't find a valid worker
				log.Debugf("no worker...")
				time.Sleep(time.Duration(3) * time.Second)
				continue
			}
			atomic.AddInt32(&pendingCount, 1)
			// addFlight(flightKey)
			flightMap.Store(flightKey, struct{}{})
			v.SetWorkerUnPaid(worker.remoteAddr, true)
			jobCh <- &job{
				req:       req,
				flightKey: flightKey,
				worker:    worker,
			}
		}
		log.Debugf("outside for loop")
	}()

	go func() {
		for {
			if v.State() == TaskStateDone {
				log.Debugf("receive job task is done break")
				break
			}
			select {
			case resp, ok := <-done:
				if !ok {
					log.Debugf("done channel has close")
					break
				}
				log.Debugf("receive resp++++ %s, err %s", resp.flightKey, resp.err)
				// remove the request from flight
				// removeFlight(resp.flightKey)
				atomic.AddInt32(&pendingCount, -1)
				flightMap.Delete(resp.flightKey)
				if resp.err != nil {
					log.Errorf("worker %s do job err continue %s", resp.worker.remoteAddr, resp.err)
					continue
				}
				log.Debugf("add flightkey to cache %s, blockhash %s", resp.flightKey, resp.ret.Hash)
				atomic.AddInt32(&pendingCount, 1)
				blockCache.Store(resp.flightKey, resp.ret)
				// notify outside
				pool := v.GetBlockReqPool()
				type toDeleteInfo struct {
					hash  string
					index int32
				}
				toDelete := make([]*toDeleteInfo, 0)
				for poolIdx, r := range pool {
					blkKey := fmt.Sprintf("%s-%d", r.Hash, r.Index)
					blktemp, ok := blockCache.Load(blkKey)
					log.Debugf("loop req poolIdx %d pool %v", poolIdx, blkKey)
					if !ok {
						log.Debugf("break because block cache not has %v", blkKey)
						break
					}
					blk := blktemp.(*BlockResp)
					log.Debugf("notify flightkey from cache %s-%d", blk.Hash, blk.Index)
					v.NotifyBlock(blk)
					blockCache.Delete(blkKey)
					atomic.AddInt32(&pendingCount, -1)
					toDelete = append(toDelete, &toDeleteInfo{
						hash:  r.Hash,
						index: r.Index,
					})
				}
				log.Debugf("to delete len %d", len(toDelete))
				for _, toD := range toDelete {
					this.DelBlockReq(taskId, toD.hash, toD.index)
				}
				log.Debugf("remain block cache len %d", getBlockCacheLen())
				log.Debugf("receive resp++++ done")
			}
			if v.State() == TaskStatePause || v.State() == TaskStateFailed {
				log.Debugf("receive state %d", v.State())
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
				state := v.State()
				if state == TaskStateDone || state == TaskStatePause || state == TaskStateFailed {
					log.Debugf("task is break, state: %d", state)
					break
				}
				job, ok := <-jobCh
				if !ok {
					log.Debugf("job channel has close")
					break
				}
				log.Debugf("start request block %s from %s, wallet: %s", job.req.Hash, job.worker.RemoteAddress(), job.worker.WalletAddr())
				ret, err := job.worker.Do(taskId, fileHash, job.req.Hash, job.worker.RemoteAddress(), job.worker.WalletAddr(), job.req.Index)
				v.SetWorkerUnPaid(job.worker.remoteAddr, false)
				if err != nil {
					log.Errorf("request block %s, err %s", job.req.Hash, err)
				} else {
					log.Debugf("request block %s success", job.req.Hash)
				}
				stop := atomic.LoadUint32(&dropDoneCh) > 0
				if stop {
					log.Debugf("stop when drop channel is not 0")
					break
				}
				done <- &getBlockResp{
					worker:    job.worker,
					flightKey: job.flightKey,
					ret:       ret,
					err:       err,
				}
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

func (this *TaskMgr) SetTaskState(taskId string, state TaskState) error {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		return fmt.Errorf("task: %s, not exist", taskId)
	}
	log.Debugf("set task state: %s %d", taskId, state)
	switch state {
	case TaskStatePause:
		oldState := v.State()
		if oldState == TaskStateFailed || oldState == TaskStateDone {
			return fmt.Errorf("can't stop a failed or completed task")
		}
		v.CleanBlockReqPool()
	case TaskStateDoing:
		oldState := v.State()
		log.Debugf("oldstate:%d, newstate: %d", oldState, state)
		if oldState == TaskStateDone {
			return fmt.Errorf("can't continue a failed or completed task")
		}
	case TaskStateDone:
		log.Debugf("task: %s has done", taskId)
		switch v.taskType {
		case TaskTypeUpload:
			err := this.db.SaveFileUploaded(taskId)
			if err != nil {
				return err
			}
		case TaskTypeDownload:
			err := this.db.SaveFileDownloaded(taskId)
			if err != nil {
				return err
			}
		}
	case TaskStateCancel:
	}
	v.SetFieldValue(FIELD_NAME_STATE, state)
	this.db.SetFileInfoField(taskId, store.FILEINFO_FIELD_TASKSTATE, uint64(state))
	return nil
}

func (this *TaskMgr) GetTaskState(taskId string) TaskState {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		return TaskStateNone
	}
	return v.State()
}

func (this *TaskMgr) IsTaskCanResume(taskId string) (bool, error) {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		return false, fmt.Errorf("task not found: %v", taskId)
	}
	state := v.State()
	if state != TaskStatePrepare && state != TaskStatePause && state != TaskStateDoing {
		return false, fmt.Errorf("can't resume the task, it's state: %d", state)
	}
	if state == TaskStatePause {
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
	if state != TaskStatePrepare && state != TaskStatePause && state != TaskStateDoing {
		return false, fmt.Errorf("can't pause the task, it's state: %d", state)
	}
	if state == TaskStateDoing || state == TaskStatePrepare {
		return true, nil
	}
	return false, nil
}

func (this *TaskMgr) IsTaskPause(taskId string) (bool, error) {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		return false, fmt.Errorf("task: %s, not exist", taskId)
	}
	return v.State() == TaskStatePause, nil
}

func (this *TaskMgr) IsTaskDone(taskId string) (bool, error) {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		return false, fmt.Errorf("task: %s, not exist", taskId)
	}
	return v.State() == TaskStateDone, nil
}

func (this *TaskMgr) IsTaskCancel(taskId string) (bool, error) {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		return false, fmt.Errorf("task: %s, not exist", taskId)
	}
	return v.State() == TaskStateCancel, nil
}

func (this *TaskMgr) IsTaskPauseOrCancel(taskId string) (bool, bool, error) {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		return false, false, fmt.Errorf("task: %s, not exist", taskId)
	}
	state := v.State()
	return state == TaskStatePause, state == TaskStateCancel, nil

}

func (this *TaskMgr) IsTaskFailed(taskId string) (bool, error) {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		return false, fmt.Errorf("task: %s, not exist", taskId)
	}
	log.Debugf("v.state: %d", v.State())
	return v.State() == TaskStateFailed, nil
}

func (this *TaskMgr) GetTaskDetailState(taskId string) (TaskProgressState, error) {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		return 0, fmt.Errorf("task: %s, not exist", taskId)
	}
	return v.TransferingState(), nil
}

func (this *TaskMgr) TaskStateChange(taskId string) chan TaskState {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		return nil
	}
	return v.stateChange
}

func (this *TaskMgr) BatchSetFileInfo(taskId string, fileHash, prefix, fileName, totalCount interface{}) error {
	log.Debugf("BatchSetFileInfo")
	v, ok := this.GetTaskById(taskId)
	m := make(map[int]interface{})
	if fileHash != nil {
		m[store.FILEINFO_FIELD_FILEHASH] = fileHash
		if ok {
			v.SetFieldValue(FIELD_NAME_FILEHASH, fileHash)
		}
	}
	if prefix != nil {
		m[store.FILEINFO_FIELD_PREFIX] = prefix
	}
	if fileName != nil {
		m[store.FILEINFO_FIELD_FILENAME] = fileName
		if ok {
			v.SetFieldValue(FIELD_NAME_FILENAME, fileName)
		}
	}
	log.Debugf("SetTotalBlockCnt totalCount %v", totalCount)
	if totalCount != nil {
		m[store.FILEINFO_FIELD_TOTALCOUNT] = totalCount
		if ok {
			v.SetTotalBlockCnt(totalCount.(uint64))
		}
		v.SetFieldValue(FIELD_NAME_STATE, TaskStateDoing)
	}
	if len(m) == 0 {
		return nil
	}
	log.Debugf("SetFileInfoFields :%v", m)
	err := this.db.SetFileInfoFields(taskId, m)
	return err
}

func (this *TaskMgr) SetWhitelistTx(taskId string, whitelistTx string) error {
	err := this.db.SetFileInfoField(taskId, store.FILEINFO_FIELD_WHITELISTTX, whitelistTx)
	return err
}

func (this *TaskMgr) GetFileTotalBlockCount(taskId string) uint64 {
	v, ok := this.GetTaskById(taskId)
	if ok {
		return v.GetTotalBlockCnt()
	}
	count, err := this.db.GetFileInfoUint64Value(taskId, store.FILEINFO_FIELD_TOTALCOUNT)
	if err != nil {
		log.Errorf("GetFileInfoUint64Value from db err %s", err)
	}
	return count
}

func (this *TaskMgr) GetFilePrefix(taskId string) (string, error) {
	return this.db.GetFileInfoStringValue(taskId, store.FILEINFO_FIELD_PREFIX)
}

func (this *TaskMgr) GetTaskUpdatedAt(taskId string) int64 {
	v, _ := this.GetTaskById(taskId)
	return v.GetUpdatedAt()
}

func (this *TaskMgr) GetFileName(taskId string) (string, error) {
	v, ok := this.GetTaskById(taskId)
	if ok {
		return v.GetStringValue(FIELD_NAME_FILENAME), nil
	}
	return this.db.GetFileInfoStringValue(taskId, store.FILEINFO_FIELD_FILENAME)
}

func (this *TaskMgr) AllDownloadFiles() ([]*store.FileInfo, []string, error) {
	return this.db.AllDownloadFiles()
}

func (this *TaskMgr) CanShareTo(id, walletAddress string, asset int32) (bool, error) {
	return this.db.CanShareTo(id, walletAddress, asset)
}

func (this *TaskMgr) GetBlockOffset(id, blockHash string, index uint32) (uint64, error) {
	return this.db.GetBlockOffset(id, blockHash, index)
}

func (this *TaskMgr) AddFileUnpaid(id, walletAddress string, asset int32, amount uint64) error {
	return this.db.AddFileUnpaid(id, walletAddress, asset, amount)
}

func (this *TaskMgr) AddFileBlockHashes(id string, blocks []string) error {
	return this.db.AddFileBlockHashes(id, blocks)
}
func (this *TaskMgr) IsFileInfoExist(id string) bool {
	return this.db.IsFileInfoExist(id)
}

func (this *TaskMgr) DeleteFileUnpaid(id, walletAddress string, asset int32, amount uint64) error {
	return this.db.DeleteFileUnpaid(id, walletAddress, asset, amount)
}

func (this *TaskMgr) FileBlockHashes(id string) []string {
	return this.db.FileBlockHashes(id)
}

func (this *TaskMgr) SetBlockDownloaded(id, blockHashStr, nodeAddr string, index uint32, offset int64, links []string) error {
	return this.db.SetBlockDownloaded(id, blockHashStr, nodeAddr, index, offset, links)
}

func (this *TaskMgr) IsFileUploaded(id string) bool {
	return this.db.IsFileUploaded(id)
}

func (this *TaskMgr) IsFileDownloaded(id string) bool {
	return this.db.IsFileDownloaded(id)
}

func (this *TaskMgr) GetFileInfo(id string) (*store.FileInfo, error) {
	return this.db.GetFileInfo([]byte(id))
}

func (this *TaskMgr) IsBlockDownloaded(id, blockHashStr string, index uint32) bool {
	return this.db.IsBlockDownloaded(id, blockHashStr, index)
}

func (this *TaskMgr) GetUploadedBlockNodeList(id, blockHashStr string, index uint32) []string {
	return this.db.GetUploadedBlockNodeList(id, blockHashStr, index)
}

func (this *TaskMgr) UploadedBlockCount(id string) uint64 {
	return this.db.UploadedBlockCount(id)
}

func (this *TaskMgr) SetPrivateKey(id string, value interface{}) error {
	return this.db.SetFileInfoField(id, store.FILEINFO_FIELD_PROVE_PRIVATEKEY, value)
}

func (this *TaskMgr) GetFilePrivateKey(id string) ([]byte, error) {
	return this.db.GetFileInfoBytesValue(id, store.FILEINFO_FIELD_PROVE_PRIVATEKEY)
}

func (this *TaskMgr) SetStoreTx(id string, value interface{}) error {
	return this.db.SetFileInfoField(id, store.FILEINFO_FIELD_STORETX, value)
}

func (this *TaskMgr) GetStoreTx(id string) (string, error) {
	return this.db.GetFileInfoStringValue(id, store.FILEINFO_FIELD_STORETX)
}

func (this *TaskMgr) GetRegUrlTx(id string) (string, error) {
	return this.db.GetFileInfoStringValue(id, store.FILEINFO_FIELD_REGURL_TX)
}

func (this *TaskMgr) GetBindUrlTx(id string) (string, error) {
	return this.db.GetFileInfoStringValue(id, store.FILEINFO_FIELD_BIND_TX)
}

func (this *TaskMgr) SetRegAndBindUrlTx(id, regTx, bindTx string) error {
	m := make(map[int]interface{})
	m[store.FILEINFO_FIELD_REGURL_TX] = regTx
	m[store.FILEINFO_FIELD_BIND_TX] = bindTx
	return this.db.SetFileInfoFields(id, m)
}

func (this *TaskMgr) GetWhitelistTx(id string) (string, error) {
	return this.db.GetFileInfoStringValue(id, store.FILEINFO_FIELD_WHITELISTTX)
}

func (this *TaskMgr) IsBlockUploaded(id, blockHashStr, nodeAddr string, index uint32) bool {
	return this.db.IsBlockUploaded(id, blockHashStr, nodeAddr, index)
}

func (this *TaskMgr) AddUploadedBlock(id, blockHashStr, nodeAddr string, index uint32, dataSize, offset uint64) error {
	return this.db.AddUploadedBlock(id, blockHashStr, nodeAddr, index, dataSize, offset)
}

func (this *TaskMgr) AddShareTo(id, walletAddress string) error {
	return this.db.AddShareTo(id, walletAddress)
}

func (this *TaskMgr) SetFilePath(id, path string) error {
	v, ok := this.GetTaskById(id)
	if ok {
		v.SetFieldValue(FIELD_NAME_FILEPATH, path)
	}
	return this.db.SetFileInfoField(id, store.FILEINFO_FIELD_FILEPATH, path)
}

func (this *TaskMgr) GetFilePath(id string) (string, error) {
	return this.db.GetFileInfoStringValue(id, store.FILEINFO_FIELD_FILEPATH)
}

func (this *TaskMgr) SetFileUploadOptions(id string, opt *fs.UploadOption) error {
	task, _ := this.GetTaskById(id)
	if task != nil {
		task.SetFieldValue(FIELD_NAME_STORE_TYPE, opt.StorageType)
		task.SetFieldValue(FIELD_NAME_COPYNUM, opt.CopyNum)
	}
	return this.db.SetFileUploadOptions(id, opt)
}
func (this *TaskMgr) GetFileUploadOptions(id string) (*fs.UploadOption, error) {
	return this.db.GetFileUploadOptions(id)
}

func (this *TaskMgr) SetFileDownloadOptions(id string, opt *common.DownloadOption) error {
	return this.db.SetFileDownloadOptions(id, opt)
}
func (this *TaskMgr) GetFileDownloadOptions(id string) (*common.DownloadOption, error) {
	return this.db.GetFileDownloadOptions(id)
}

func (this *TaskMgr) GetUndownloadedBlockInfo(id, rootBlockHash string) ([]string, map[string]uint32, error) {
	return this.db.GetUndownloadedBlockInfo(id, rootBlockHash)
}

func (this *TaskMgr) GetFileSessions(fileInfoId string) (map[string]*store.Session, error) {
	return this.db.GetFileSessions(fileInfoId)
}

func (this *TaskMgr) AddFileSession(fileInfoId string, sessionId, walletAddress, hostAddress string, asset, unitPrice uint64) error {
	this.SetSessionId(fileInfoId, walletAddress, sessionId)
	return this.db.AddFileSession(fileInfoId, sessionId, walletAddress, hostAddress, asset, unitPrice)
}

func (this *TaskMgr) GetCurrentSetBlock(fileInfoId string) (string, uint64, error) {
	return this.db.GetCurrentSetBlock(fileInfoId)
}

func (this *TaskMgr) SetUploadProgressDone(id, nodeAddr string) error {
	return this.db.SetUploadProgressDone(id, nodeAddr)
}
