package task

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/saveio/dsp-go-sdk/common"
	sdkErr "github.com/saveio/dsp-go-sdk/error"
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
	blockReqCh     chan *GetBlockReq
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
	// tmgr.FileDB = store.NewFileDB(common.FILE_DB_DIR_PATH)
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

func (this *TaskMgr) BindTaskId(id string) error {
	this.lock.Lock()
	t := this.tasks[id]
	this.lock.Unlock()
	hash := ""
	switch t.taskType {
	case TaskTypeUpload:
		hash = utils.StringToSha256Hex(t.filePath)
	default:
		hash = t.fileHash
	}
	key := this.TaskIdKey(hash, t.walletAddr, t.taskType)
	return this.db.SaveFileInfoId(key, id)
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
	t.SetFieldValue(FIELD_NAME_WALLETADDR, walletAddress)
	m[store.FILEINFO_FIELD_FILENAME] = fileName
	if len(filePath) != 0 {
		t.SetFieldValue(FIELD_NAME_FILEPATH, filePath)
	}
	if len(fileHash) != 0 {
		t.SetFieldValue(FIELD_NAME_FILEHASH, fileHash)
		m[store.FILEINFO_FIELD_FILEHASH] = fileHash
	}
	this.db.SetFileInfoFields(id, m)
}

// func (this *TaskMgr) UploadFileExist(fileHash, walletAddress string) bool {
// 	key := this.TaskIdKey(fileHash, walletAddress, TaskTypeUpload)
// 	oldId, _ := this.GetFileInfoId(key)
// 	if len(oldId) != 0 {
// 		return true
// 	}
// 	return false
// }

// TaskId from hash-walletaddress-type
func (this *TaskMgr) TaskId(prefix, walletAddress string, tp TaskType) string {
	var key string
	switch tp {
	case TaskTypeUpload:
		hexStr := utils.StringToSha256Hex(prefix)
		key = this.TaskIdKey(hexStr, walletAddress, tp)
	case TaskTypeDownload, TaskTypeShare:
		key = this.TaskIdKey(prefix, walletAddress, tp)
	}
	id, _ := this.db.GetFileInfoId(key)
	return id
}

// DeleteTask. delete task with task id
func (this *TaskMgr) DeleteTask(taskId string, deleteStore bool) error {
	var fileHash, walletAddress, filePath string
	var tp TaskType
	this.lock.Lock()
	task, ok := this.tasks[taskId]
	if !ok {
		this.lock.Unlock()
		return fmt.Errorf("task not found of id %s", taskId)
	}
	fileHash = task.fileHash
	walletAddress = task.walletAddr
	tp = task.taskType
	filePath = task.filePath
	delete(this.tasks, taskId)
	this.lock.Unlock()
	if !deleteStore {
		return nil
	}
	var key string
	if task.taskType == TaskTypeUpload {
		hexStr := utils.StringToSha256Hex(filePath)
		key = this.TaskIdKey(hexStr, walletAddress, tp)
	} else {
		key = this.TaskIdKey(fileHash, walletAddress, tp)
	}
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

func (this *TaskMgr) PushGetBlock(taskId string, block *BlockResp) {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		return
	}
	v.PushGetBlock(block.Hash, block.Index, block)
}

func (this *TaskMgr) GetBlockRespCh(taskId, blockHash string, index int32) chan *BlockResp {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		return nil
	}
	return v.GetBlockRespCh(blockHash, index)
}

func (this *TaskMgr) DropBlockRespCh(taskId, blockHash string, index int32) {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		return
	}
	v.DropBlockRespCh(blockHash, index)
}

// SetTaskTimeout. set task timeout with taskid
func (this *TaskMgr) SetTaskTimeout(taskId string, timeout bool) {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		return
	}
	v.SetFieldValue(FIELD_NAME_ASKTIMEOUT, timeout)
}

// SetTaskReady. set task is ready with taskid
func (this *TaskMgr) SetTaskReady(taskId string, ready bool) {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		return
	}
	v.SetFieldValue(FIELD_NAME_READY, ready)
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

// EmitProgress. emit progress to channel with taskId
func (this *TaskMgr) EmitProgress(taskId string, state TaskProgressState) {
	v, ok := this.GetTaskById(taskId)
	log.Debugf("EmitProgress ok %t, this.progress %v", ok, this.progress)
	if !ok {
		return
	}
	if this.progress == nil {
		return
	}
	pInfo := &ProgressInfo{
		TaskId:    taskId,
		Type:      v.GetTaskType(),
		FileName:  v.GetStringValue(FIELD_NAME_FILENAME),
		FileHash:  v.GetStringValue(FIELD_NAME_FILEHASH),
		Total:     v.GetTotalBlockCnt(),
		Count:     this.db.FileProgress(taskId),
		State:     state,
		CreatedAt: uint64(v.GetCreatedAt()),
		UpdatedAt: uint64(time.Now().Unix()),
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
func (this *TaskMgr) EmitResult(taskId string, ret interface{}, err *sdkErr.SDKError) {
	v := this.GetTask(taskId)
	if v == nil {
		log.Errorf("emit result get no task")
		return
	}
	if this.progress == nil {
		log.Errorf("progress is nil")
		return
	}
	pInfo := &ProgressInfo{
		TaskId:    taskId,
		Type:      v.GetTaskType(),
		FileName:  v.GetStringValue(FIELD_NAME_FILENAME),
		FileHash:  v.GetStringValue(FIELD_NAME_FILEHASH),
		Total:     v.GetTotalBlockCnt(),
		Count:     this.db.FileProgress(v.GetStringValue(FIELD_NAME_ID)),
		CreatedAt: uint64(v.GetCreatedAt()),
		UpdatedAt: uint64(time.Now().Unix()),
	}
	if err != nil {
		pInfo.ErrorCode = err.Code
		pInfo.ErrorMsg = err.Message
	} else if ret != nil {
		pInfo.Result = ret
	}
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
	this.lock.Lock()
	v, ok := this.tasks[taskId]
	this.lock.Unlock()
	if !ok {
		return
	}
	v.SetFieldValue(FIELD_NAME_INORDER, inOrder)
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
	max := len(addrs)
	if max > common.MAX_GOROUTINES_FOR_WORK_TASK {
		max = common.MAX_GOROUTINES_FOR_WORK_TASK
	}
	// block request flights
	var workLock sync.Mutex
	flight := make([]string, 0)
	addFlight := func(f string) {
		workLock.Lock()
		flight = append(flight, f)
		log.Debugf("append flightkey %v workLock:%p", flight, &workLock)
		workLock.Unlock()
	}
	removeFlight := func(f string) {
		workLock.Lock()
		for j, key := range flight {
			if key == f {
				flight = append(flight[:j], flight[j+1:]...)
				break
			}
		}
		log.Debugf("delete flight key %s %v", f, flight)
		workLock.Unlock()
	}
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

	done := make(chan *getBlockResp, 1)
	go func() {
		for {
			if v.GetBoolValue(FIELD_NAME_DONE) {
				log.Debugf("distribute job task is done break")
				close(jobCh)
				close(done)
				break
			}
			// check pool has item or not
			// check all pool items are in request flights
			if v.GetBlockReqPoolLen() == 0 || len(flight)+getBlockCacheLen() >= v.GetBlockReqPoolLen() {
				// time.Sleep(time.Second)
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
			worker := v.GetIdleWorker(addrs, req.Hash)
			if worker == nil {
				// can't find a valid worker
				log.Debugf("no worker...")
				time.Sleep(time.Duration(3) * time.Second)
				continue
			}
			addFlight(flightKey)
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
			if v.GetBoolValue(FIELD_NAME_DONE) {
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
				removeFlight(resp.flightKey)
				flightMap.Delete(resp.flightKey)
				if resp.err != nil {
					v.SetWorkerUnPaid(resp.worker.remoteAddr, false)
					log.Errorf("worker %s do job err continue %s", resp.worker.remoteAddr, resp.err)
					continue
				}
				log.Debugf("add flightkey to cache %s, blockhash %s", resp.flightKey, resp.ret.Hash)
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
						// break
						continue
					}
					blk := blktemp.(*BlockResp)
					log.Debugf("notify flightkey from cache %s-%d", blk.Hash, blk.Index)
					v.NotifyBlock(blk)
					blockCache.Delete(blkKey)
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
		}
		log.Debugf("outside receive job task")
	}()

	// open max routine to do jobs
	log.Debugf("open %d routines to work background", max)
	for i := 0; i < max; i++ {
		go func() {
			for {
				if v.GetBoolValue(FIELD_NAME_DONE) {
					log.Debugf("task is done break")
					break
				}
				job, ok := <-jobCh
				if !ok {
					log.Debugf("job channel has close")
					break
				}

				log.Debugf("start request block %s from %s", job.req.Hash, job.worker.RemoteAddress())
				ret, err := job.worker.Do(taskId, fileHash, job.req.Hash, job.worker.RemoteAddress(), job.worker.WalletAddr(), job.req.Index)
				log.Debugf("request block %s, err %s", job.req.Hash, err)
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
	v.SetFieldValue(FIELD_NAME_DONE, done)
	switch v.taskType {
	case TaskTypeDownload:
		this.db.SaveFileDownloaded(taskId)
	}
}

func (this *TaskMgr) BatchSetFileInfo(taskId string, fileHash, prefix, fileName, totalCount interface{}) error {
	m := make(map[int]interface{})
	v, ok := this.GetTaskById(taskId)
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
	if totalCount != nil {
		m[store.FILEINFO_FIELD_TOTALCOUNT] = totalCount
		if ok {
			v.SetTotalBlockCnt(totalCount.(uint64))
		}
	}
	if len(m) == 0 {
		return nil
	}
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

func (this *TaskMgr) GetFileName(taskId string) (string, error) {
	v, ok := this.GetTaskById(taskId)
	if ok {
		return v.GetStringValue(FIELD_NAME_FILENAME), nil
	}
	return this.db.GetFileInfoStringValue(taskId, store.FILEINFO_FIELD_FILENAME)
}

func (this *TaskMgr) AllDownloadFiles() ([]string, error) {
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

func (this *TaskMgr) IsBlockUploaded(id, blockHashStr, nodeAddr string, index uint32) bool {
	return this.db.IsBlockUploaded(id, blockHashStr, nodeAddr, index)
}

func (this *TaskMgr) GetBlockTail(id string, index uint32) (uint64, error) {
	return this.db.GetBlockTail(id, index)
}

func (this *TaskMgr) AddUploadedBlock(id, blockHashStr, nodeAddr string, index uint32, dataSize, offset uint64) error {
	return this.db.AddUploadedBlock(id, blockHashStr, nodeAddr, index, dataSize, offset)
}

func (this *TaskMgr) AddShareTo(id, walletAddress string) error {
	return this.db.AddShareTo(id, walletAddress)
}
