package task

import (
	"fmt"
	"sync"

	"github.com/google/uuid"
	"github.com/saveio/dsp-go-sdk/common"
	"github.com/saveio/dsp-go-sdk/store"
	"github.com/saveio/dsp-go-sdk/utils"
	"github.com/saveio/themis/common/log"
)

type GetBlockReq struct {
	TimeStamp     int64
	FileHash      string
	Hash          string
	Index         uint64
	PeerAddr      string
	WalletAddress string
	Asset         int32
	Syn           string
}

type BlockResp struct {
	Hash      string
	Index     uint64
	PeerAddr  string
	Block     []byte
	Tag       []byte
	Offset    int64
	PaymentId int32
}

type ProgressInfo struct {
	TaskId        string
	Type          store.TaskType                // task type
	Url           string                        // task url
	StoreType     uint32                        // store type
	FileName      string                        // file name
	FileHash      string                        // file hash
	FilePath      string                        // file path
	CopyNum       uint32                        // copyNum
	FileSize      uint64                        // file size
	RealFileSize  uint64                        // real file size
	Total         uint64                        // total file's blocks count
	Encrypt       bool                          // file encrypted or not
	Progress      map[string]store.FileProgress // address <=> progress
	SlaveProgress map[string]store.FileProgress // progress for slave nodes
	NodeHostAddrs map[string]string             // node host addrs map
	TaskState     store.TaskState               // task state
	ProgressState TaskProgressState             // TaskProgressState
	Result        interface{}                   // finish result
	ErrorCode     uint32                        // error code
	ErrorMsg      string                        // interrupt error
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
	FileName      string
	FileOwner     string
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

type blockReqPool struct {
}

type Task struct {
	id           string            // id
	info         *store.TaskInfo   // task info from local DB
	peerSenIds   map[string]string // request peerAddr <=> session id
	transferring bool              // fetch is transferring flag
	// TODO: refactor, delete below two channels, use request and reply
	blockReq            chan []*GetBlockReq          // fetch block request channel
	blockRespsMap       map[string]chan *BlockResp   // map key <=> *BlockResp
	blockFlightRespsMap map[string]chan []*BlockResp // map key <=> []*BlockResp
	blockReqPool        []*GetBlockReq               // get block request pool
	blockReqPoolNotify  chan *blockReqPool           // block req pool change notify
	workers             map[string]*Worker           // workers to request block
	notify              chan *BlockResp              // notify download block
	backupOpt           *BackupFileOpt               // backup file options
	lock                *sync.RWMutex                // lock
	lastWorkerIdx       int                          // last worker index
	batch               bool                         // flag of batch set
	db                  *store.TaskDB                // db
	workerNetPhase      map[string]int               // network msg interact phase, used to check msg transaction, wallet addr <=> phase
}

// NewTask. new task for file, and set the task info to DB.
func NewTask(taskId string, taskT store.TaskType, db *store.TaskDB) *Task {
	if len(taskId) == 0 {
		id, _ := uuid.NewUUID()
		taskId = id.String()
	}
	info, err := db.NewTaskInfo(taskId, taskT)
	if err != nil {
		log.Errorf("new file info failed %s", err)
		return nil
	}
	t := newTask(taskId, info, db)
	t.info.TaskState = store.TaskStatePrepare
	err = db.SaveTaskInfo(t.info)
	if err != nil {
		log.Debugf("save file info failed err %s", err)
		return nil
	}
	return t
}

// GetTaskFromDB. get a task object from DB with file info id
func GetTaskFromDB(id string, db *store.TaskDB) (*Task, error) {
	info, err := db.GetTaskInfo(id)
	if err != nil {
		log.Errorf("[Task GetTaskFromDB] get file info failed, id: %s", id)
		return nil, err
	}
	if info == nil {
		log.Warnf("[Task GetTaskFromDB] recover task get file info is nil, id: %v", id)
		return nil, nil
	}
	t := newTask(id, info, db)
	return t, nil
}

// NewTaskFromDB. Read file info from DB and recover a task by the file info.
func NewTaskFromDB(id string, db *store.TaskDB) (*Task, error) {
	info, err := db.GetTaskInfo(id)
	if err != nil {
		log.Errorf("get task from db, get file info failed, id: %s", id)
		return nil, err
	}
	if info == nil {
		log.Warnf("get task from db, recover task get file info is nil, id: %v", id)
		return nil, nil
	}
	if (store.TaskState(info.TaskState) == store.TaskStatePause ||
		store.TaskState(info.TaskState) == store.TaskStateDoing) &&
		info.UpdatedAt+common.DOWNLOAD_FILE_TIMEOUT*1000 < utils.GetMilliSecTimestamp() {
		log.Warnf("get task from db, task: %s is expired, type: %d, updatedAt: %d", id, info.Type, info.UpdatedAt)
	}
	sessions, err := db.GetFileSessions(id)
	if err != nil {
		log.Errorf("get task from db, set task session: %s", err)
		return nil, err
	}
	state := store.TaskState(info.TaskState)
	if state == store.TaskStatePrepare || state == store.TaskStateDoing ||
		state == store.TaskStateCancel || state == store.TaskStateIdle {
		state = store.TaskStatePause
	}
	t := newTask(id, info, db)
	t.info.TaskState = state
	t.info.TranferState = uint32(TaskPause)
	err = db.SaveTaskInfo(t.info)
	if err != nil {
		log.Errorf("get task from db, set file info failed: %s", err)
		return nil, err
	}
	for _, session := range sessions {
		t.peerSenIds[session.WalletAddr] = session.SessionId
	}
	log.Debugf("get task from db, task id %s, file name %s, task type %d, state %d, sessions %v",
		t.id, t.info.FileName, t.info.StoreType, t.State, t.peerSenIds)
	return t, nil
}

func (this *Task) GetTaskType() store.TaskType {
	this.lock.RLock()
	defer this.lock.RUnlock()
	return this.info.Type
}

func (this *Task) SetSessionId(peerWalletAddr, id string) {
	this.lock.Lock()
	defer this.lock.Unlock()
	this.peerSenIds[peerWalletAddr] = id
}

func (this *Task) GetSessionId(peerWalletAddr string) string {
	this.lock.RLock()
	defer this.lock.RUnlock()
	return this.peerSenIds[peerWalletAddr]
}

func (this *Task) GetBlockReq() chan []*GetBlockReq {
	return this.blockReq
}

func (this *Task) NewBatchSet() {
	this.lock.Lock()
	defer this.lock.Unlock()
	if this.batch {
		log.Warnf("batch set flag is active")
	}
	this.batch = true
}

func (this *Task) SetFileName(fileName string) error {
	this.lock.Lock()
	defer this.lock.Unlock()
	this.info.FileName = fileName
	if this.batch {
		return nil
	}
	return this.db.SaveTaskInfo(this.info)
}

func (this *Task) SetFileHash(fileHash string) error {
	this.lock.Lock()
	defer this.lock.Unlock()
	this.info.FileHash = fileHash
	if this.batch {
		return nil
	}
	return this.db.SaveTaskInfo(this.info)
}

func (this *Task) SetPrefix(prefix string) error {
	this.lock.Lock()
	defer this.lock.Unlock()
	this.info.Prefix = []byte(prefix)
	if this.batch {
		return nil
	}
	return this.db.SaveTaskInfo(this.info)
}
func (this *Task) SetFileOwner(fileOwner string) error {
	this.lock.Lock()
	defer this.lock.Unlock()
	this.info.FileOwner = fileOwner
	if this.batch {
		return nil
	}
	return this.db.SaveTaskInfo(this.info)
}

func (this *Task) SetWalletaddr(walletAddr string) error {
	this.lock.Lock()
	defer this.lock.Unlock()
	this.info.WalletAddress = walletAddr
	if this.batch {
		return nil
	}
	return this.db.SaveTaskInfo(this.info)
}

func (this *Task) SetFilePath(filePath string) error {
	this.lock.Lock()
	defer this.lock.Unlock()
	// TODO: temp solution for concurrent update path, will fixed by refactor code
	if len(this.info.FilePath) != 0 {
		log.Warnf("task %s has path %s, ignore new path", this.info.Id, this.info.FilePath)
		return nil
	}
	this.info.FilePath = filePath
	if this.batch {
		return nil
	}
	return this.db.SaveTaskInfo(this.info)
}

func (this *Task) SetSimpleCheckSum(checksum string) error {
	this.lock.Lock()
	defer this.lock.Unlock()
	this.info.SimpleChecksum = checksum
	if this.batch {
		return nil
	}
	return this.db.SaveTaskInfo(this.info)
}

func (this *Task) SetStoreType(storeType uint32) error {
	this.lock.Lock()
	defer this.lock.Unlock()
	this.info.StoreType = storeType
	if this.batch {
		return nil
	}
	return this.db.SaveTaskInfo(this.info)
}

func (this *Task) SetCopyNum(copyNum uint32) error {
	this.lock.Lock()
	defer this.lock.Unlock()
	this.info.CopyNum = copyNum
	if this.batch {
		return nil
	}
	return this.db.SaveTaskInfo(this.info)
}

func (this *Task) SetUrl(url string) error {
	this.lock.Lock()
	defer this.lock.Unlock()
	this.info.Url = url
	if this.batch {
		return nil
	}
	return this.db.SaveTaskInfo(this.info)
}

func (this *Task) SetOwner(owner string) error {
	this.lock.Lock()
	defer this.lock.Unlock()
	this.info.FileOwner = owner
	if this.batch {
		return nil
	}
	return this.db.SaveTaskInfo(this.info)
}

func (this *Task) SetTaskState(newState store.TaskState) error {
	this.lock.Lock()
	defer this.lock.Unlock()
	return this.setTaskState(newState)
}

func (this *Task) setTaskState(newState store.TaskState) error {
	oldState := store.TaskState(this.info.TaskState)
	if oldState == newState {
		log.Debugf("set task with same state id: %s, state: %d", this.id, oldState)
		return nil
	}
	switch newState {
	case store.TaskStatePause:
		if oldState == store.TaskStateFailed || oldState == store.TaskStateDone {
			return fmt.Errorf("can't stop a failed or completed task")
		}
		this.cleanBlockReqPool()
	case store.TaskStateDoing:
		log.Debugf("oldstate:%d, newstate: %d", oldState, newState)
		if oldState == store.TaskStateDone {
			return fmt.Errorf("can't continue a failed or completed task")
		}
		this.info.ErrorCode = 0
		this.info.ErrorMsg = ""
	case store.TaskStateDone:
		log.Debugf("task: %s has done", this.id)
	case store.TaskStateCancel:
	}
	this.info.TaskState = newState
	changeFromPause := (oldState == store.TaskStatePause && (newState == store.TaskStateDoing || newState == store.TaskStateCancel))
	changeFromDoing := (oldState == store.TaskStateDoing && (newState == store.TaskStatePause || newState == store.TaskStateCancel))
	if changeFromPause {
		log.Debugf("task: %s changeFromPause, send new state change: %d to %d", this.id, oldState, newState)
	}
	if changeFromDoing {
		log.Debugf("task: %s changeFromDoing, send new state change: %d to %d", this.id, oldState, newState)
	}
	if this.batch {
		return nil
	}
	return this.db.SaveTaskInfo(this.info)
}

func (this *Task) SetInorder(inOrder bool) error {
	this.lock.Lock()
	defer this.lock.Unlock()
	this.info.InOrder = inOrder
	if this.batch {
		return nil
	}
	return this.db.SaveTaskInfo(this.info)
}

func (this *Task) SetOnlyBlock(onlyBlock bool) error {
	this.lock.Lock()
	defer this.lock.Unlock()
	this.info.OnlyBlock = onlyBlock
	if this.batch {
		return nil
	}
	return this.db.SaveTaskInfo(this.info)
}

func (this *Task) SetTransferState(transferState uint32) error {
	this.lock.Lock()
	defer this.lock.Unlock()
	this.info.TranferState = transferState
	if this.batch {
		return nil
	}
	return this.db.SaveTaskInfo(this.info)
}

func (this *Task) SetTotalBlockCnt(cnt uint64) error {
	this.lock.Lock()
	defer this.lock.Unlock()
	this.info.TotalBlockCount = cnt
	if this.batch {
		return nil
	}
	return this.db.SaveTaskInfo(this.info)
}

func (this *Task) SetStoreTx(tx string) error {
	this.lock.Lock()
	defer this.lock.Unlock()
	this.info.StoreTx = tx
	if this.batch {
		return nil
	}
	return this.db.SaveTaskInfo(this.info)
}

func (this *Task) SetResult(result interface{}, errorCode uint32, errorMsg string) error {
	this.lock.Lock()
	defer this.lock.Unlock()
	log.Debugf("set task result %v %v", errorCode, errorMsg)
	if errorCode != 0 {
		if this.info.Retry >= common.MAX_TASK_RETRY {
			// task hasn't pay on chain, make it failed
			this.info.TaskState = store.TaskStateFailed
			this.info.ErrorCode = errorCode
			this.info.ErrorMsg = errorMsg
		} else {
			// task has paid, retry it later
			this.info.TaskState = store.TaskStateIdle
			this.info.Retry++
			this.info.RetryAt = utils.GetMilliSecTimestamp() + utils.GetJitterDelay(this.info.Retry, 30)*1000
			log.Errorf("task %s, file %s  is failed, error code %d, error msg %v, retry %d times, retry at %d",
				this.info.Id, this.info.FileHash, errorCode, errorMsg, this.info.Retry, this.info.RetryAt)
		}
	} else if result != nil {
		log.Debugf("task: %s has done", this.id)
		this.info.ErrorCode = 0
		this.info.ErrorMsg = ""
		this.info.Result = result
		this.info.TaskState = store.TaskStateDone
		switch this.info.Type {
		case store.TaskTypeUpload:
			err := this.db.SaveFileUploaded(this.id)
			if err != nil {
				return err
			}
		case store.TaskTypeDownload:
			err := this.db.SaveFileDownloaded(this.id)
			if err != nil {
				return err
			}
		}
	}
	if this.batch {
		return nil
	}
	return this.db.SaveTaskInfo(this.info)
}

func (this *Task) SetWhiteListTx(whiteListTx string) error {
	this.lock.Lock()
	defer this.lock.Unlock()
	this.info.WhitelistTx = whiteListTx
	if this.batch {
		return nil
	}
	return this.db.SaveTaskInfo(this.info)
}

func (this *Task) SetRegUrlTx(regUrlTx string) error {
	this.lock.Lock()
	defer this.lock.Unlock()
	this.info.RegisterDNSTx = regUrlTx
	if this.batch {
		return nil
	}
	return this.db.SaveTaskInfo(this.info)
}

func (this *Task) SetBindUrlTx(bindUrlTx string) error {
	this.lock.Lock()
	defer this.lock.Unlock()
	this.info.BindDNSTx = bindUrlTx
	if this.batch {
		return nil
	}
	return this.db.SaveTaskInfo(this.info)
}

func (this *Task) BatchCommit() error {
	this.lock.Lock()
	defer this.lock.Unlock()
	if !this.batch {
		return nil
	}
	this.batch = false
	return this.db.SaveTaskInfo(this.info)
}

// BindIdWithWalletAddr. set key to taskId, for upload task, if fileHash is empty, use Hex(filePath) instead.
// for download/share task, use fileHash
func (this *Task) BindIdWithWalletAddr() error {
	this.lock.Lock()
	defer this.lock.Unlock()
	switch this.info.Type {
	case store.TaskTypeUpload:
		if len(this.info.FileHash) > 0 {
			key := taskIdKey(this.info.FileHash, this.info.WalletAddress, this.info.Type)
			return this.db.SaveFileInfoId(key, this.id)
		}
		var err error
		if len(this.info.FilePath) > 0 {
			prefix := utils.StringToSha256Hex(this.info.FilePath)
			key := taskIdKey(prefix, this.info.WalletAddress, this.info.Type)
			err = this.db.SaveFileInfoId(key, this.id)
		}
		if err != nil {
			return err
		}
		if len(this.info.SimpleChecksum) > 0 {
			prefix := this.info.SimpleChecksum
			key := taskIdKey(prefix, this.info.WalletAddress, this.info.Type)
			err = this.db.SaveFileInfoId(key, this.id)
		}
		return err
	default:
		key := taskIdKey(this.info.FileHash, this.info.WalletAddress, this.info.Type)
		return this.db.SaveFileInfoId(key, this.id)
	}
}

func (this *Task) SetBlocksUploaded(id, nodeAddr string, blockInfos []*store.BlockInfo) error {
	this.lock.Lock()
	defer this.lock.Unlock()
	err := this.db.SetBlocksUploaded(id, nodeAddr, blockInfos)
	if err != nil {
		return err
	}
	newInfo, err := this.db.GetTaskInfo(id)
	if err != nil {
		return err
	}
	this.info = newInfo
	return nil
}

func (this *Task) SetUploadProgressDone(id, nodeAddr string) error {
	this.lock.Lock()
	defer this.lock.Unlock()
	err := this.db.SetUploadProgressDone(id, nodeAddr)
	if err != nil {
		return err
	}
	newInfo, err := this.db.GetTaskInfo(id)
	if err != nil {
		return err
	}
	this.info = newInfo
	return nil
}

func (this *Task) SetBlockDownloaded(id, blockHashStr, nodeAddr string, index uint64, offset int64, links []string) error {
	this.lock.Lock()
	defer this.lock.Unlock()
	err := this.db.SetBlockDownloaded(id, blockHashStr, nodeAddr, index, offset, links)
	if err != nil {
		return err
	}
	newInfo, err := this.db.GetTaskInfo(id)
	if err != nil {
		return err
	}
	this.info = newInfo
	return nil
}

func (this *Task) SetBlocksDownloaded(id string, blkInfos []*store.BlockInfo) error {
	this.lock.Lock()
	defer this.lock.Unlock()
	err := this.db.SetBlocksDownloaded(id, blkInfos)
	if err != nil {
		return err
	}
	newInfo, err := this.db.GetTaskInfo(id)
	if err != nil {
		return err
	}
	this.info = newInfo
	return nil
}

// GetTaskInfoCopy. get task info deep copy object.
// clone it for read-only
func (this *Task) GetTaskInfoCopy() *store.TaskInfo {
	this.lock.RLock()
	defer this.lock.RUnlock()
	return this.db.CopyTask(this.info)
}

func (this *Task) GetInorder() bool {
	this.lock.RLock()
	defer this.lock.RUnlock()
	return this.info.InOrder

}

func (this *Task) GetOnlyblock() bool {
	this.lock.RLock()
	defer this.lock.RUnlock()
	return this.info.OnlyBlock
}

func (this *Task) GetFileHash() string {
	this.lock.RLock()
	defer this.lock.RUnlock()
	return this.info.FileHash
}

func (this *Task) GetFileName() string {
	this.lock.RLock()
	defer this.lock.RUnlock()
	return this.info.FileName
}

func (this *Task) GetFileOwner() string {
	this.lock.RLock()
	defer this.lock.RUnlock()
	return this.info.FileOwner
}

func (this *Task) GetId() string {
	this.lock.RLock()
	defer this.lock.RUnlock()
	return this.id
}

func (this *Task) GetWalletAddr() string {
	this.lock.RLock()
	defer this.lock.RUnlock()
	return this.info.WalletAddress
}

func (this *Task) GetFilePath() string {
	this.lock.RLock()
	defer this.lock.RUnlock()
	return this.info.FilePath
}

func (this *Task) GetOwner() string {
	this.lock.RLock()
	defer this.lock.RUnlock()
	return this.info.FileOwner
}

func (this *Task) State() store.TaskState {
	this.lock.RLock()
	defer this.lock.RUnlock()
	return store.TaskState(this.info.TaskState)
}

func (this *Task) Pause() error {
	this.lock.Lock()
	defer this.lock.Unlock()

	state := this.info.TaskState
	if state != store.TaskStatePrepare && state != store.TaskStatePause && state != store.TaskStateDoing {
		return fmt.Errorf("state is %d, can't pause", state)
	}
	if state == store.TaskStateDoing || state == store.TaskStatePrepare {
		return this.setTaskState(store.TaskStatePause)
	}
	return nil
}

func (this *Task) Resume() error {
	this.lock.Lock()
	defer this.lock.Unlock()

	state := this.info.TaskState
	if state != store.TaskStatePrepare && state != store.TaskStatePause && state != store.TaskStateDoing {
		return fmt.Errorf("state is %d, can't resume", state)
	}
	if state == store.TaskStatePause {
		return this.setTaskState(store.TaskStateDoing)
	}
	return nil
}

func (this *Task) Retry() error {
	this.lock.Lock()
	defer this.lock.Unlock()

	state := this.info.TaskState
	if state != store.TaskStateFailed {
		return fmt.Errorf("state is %d, can't retry", state)
	}
	if state == store.TaskStateFailed {
		return this.setTaskState(store.TaskStateDoing)
	}
	return nil
}

func (this *Task) IsTaskCanResume() bool {
	this.lock.RLock()
	defer this.lock.RUnlock()
	state := this.info.TaskState
	if state != store.TaskStatePrepare && state != store.TaskStatePause && state != store.TaskStateDoing {
		return false
	}
	if state == store.TaskStatePause {
		return true
	}
	return false
}

func (this *Task) IsTaskCanPause() bool {
	this.lock.RLock()
	defer this.lock.RUnlock()
	state := this.info.TaskState
	if state != store.TaskStatePrepare && state != store.TaskStatePause && state != store.TaskStateDoing {
		return false
	}
	if state == store.TaskStateDoing || state == store.TaskStatePrepare {
		return true
	}
	return false
}

func (this *Task) IsTaskPause() bool {
	this.lock.RLock()
	defer this.lock.RUnlock()
	return this.info.TaskState == store.TaskStatePause
}

func (this *Task) NeedRetry() bool {
	this.lock.RLock()
	defer this.lock.RUnlock()
	if this.info.TaskState != store.TaskStateIdle {
		return false
	}
	if utils.GetMilliSecTimestamp() >= this.info.RetryAt {
		log.Debugf("utils.GetMilliSecTimestamp() %d, retry at %d", utils.GetMilliSecTimestamp(), this.info.RetryAt)
		return true
	}
	return false
}

func (this *Task) IsTaskDone() bool {
	this.lock.RLock()
	defer this.lock.RUnlock()
	return this.info.TaskState == store.TaskStateDone
}

func (this *Task) IsTaskCancel() bool {
	this.lock.RLock()
	defer this.lock.RUnlock()
	return this.info.TaskState == store.TaskStateCancel
}

func (this *Task) IsTaskPaying() bool {
	this.lock.RLock()
	defer this.lock.RUnlock()
	return TaskProgressState(this.info.TranferState) == TaskUploadFilePaying ||
		TaskProgressState(this.info.TranferState) == TaskWaitForBlockConfirmed ||
		TaskProgressState(this.info.TranferState) == TaskDownloadPayForBlocks
}

func (this *Task) IsTaskPauseOrCancel() (bool, bool) {
	this.lock.RLock()
	defer this.lock.RUnlock()
	state := this.info.TaskState
	return state == store.TaskStatePause, state == store.TaskStateCancel
}

func (this *Task) IsTaskStop() bool {
	this.lock.RLock()
	defer this.lock.RUnlock()
	state := this.info.TaskState
	if state != store.TaskStatePause && state != store.TaskStateCancel {
		return false
	}
	return state == store.TaskStatePause || state == store.TaskStateCancel
}

func (this *Task) IsTaskPreparingOrDoing() (bool, bool) {
	this.lock.RLock()
	defer this.lock.RUnlock()
	state := this.info.TaskState
	return state == store.TaskStatePrepare, state == store.TaskStateDoing
}

func (this *Task) IsTaskFailed() bool {
	this.lock.RLock()
	defer this.lock.RUnlock()
	return this.info.TaskState == store.TaskStateFailed
}

func (this *Task) DetailState() TaskProgressState {
	this.lock.RLock()
	defer this.lock.RUnlock()
	return TaskProgressState(this.info.TranferState)
}

func (this *Task) GetStoreTx() string {
	this.lock.RLock()
	defer this.lock.RUnlock()
	return this.info.StoreTx
}

func (this *Task) GetRegUrlTx() string {
	this.lock.RLock()
	defer this.lock.RUnlock()
	return this.info.RegisterDNSTx
}

func (this *Task) GetBindUrlTx() string {
	this.lock.RLock()
	defer this.lock.RUnlock()
	return this.info.BindDNSTx
}

func (this *Task) GetWhitelistTx() string {
	this.lock.RLock()
	defer this.lock.RUnlock()
	return this.info.WhitelistTx
}

func (this *Task) TransferingState() TaskProgressState {
	this.lock.RLock()
	defer this.lock.RUnlock()
	return TaskProgressState(this.info.TranferState)
}

func (this *Task) GetProgressInfo() *ProgressInfo {
	this.lock.RLock()
	defer this.lock.RUnlock()

	pInfo := &ProgressInfo{
		TaskId:        this.id,
		Type:          this.info.Type,
		Url:           this.info.Url,
		StoreType:     this.info.StoreType,
		FileName:      this.info.FileName,
		FileHash:      this.info.FileHash,
		FilePath:      this.info.FilePath,
		Total:         this.info.TotalBlockCount,
		CopyNum:       this.info.CopyNum,
		FileSize:      this.info.FileSize,
		RealFileSize:  this.info.RealFileSize,
		Encrypt:       this.info.Encrypt,
		Progress:      this.db.FileProgress(this.id),
		NodeHostAddrs: this.info.NodeHostAddrs,
		TaskState:     store.TaskState(this.info.TaskState),
		ProgressState: TaskProgressState(this.info.TranferState),
		CreatedAt:     this.info.CreatedAt / common.MILLISECOND_PER_SECOND,
		UpdatedAt:     this.info.UpdatedAt / common.MILLISECOND_PER_SECOND,
		Result:        this.info.Result,
		ErrorCode:     this.info.ErrorCode,
		ErrorMsg:      this.info.ErrorMsg,
	}
	if this.info.Type != store.TaskTypeUpload {
		return pInfo
	}
	if len(pInfo.Progress) == 0 {
		return pInfo
	}
	if len(this.info.PrimaryNodes) == 0 {
		return pInfo
	}
	masterNode := this.info.PrimaryNodes[0]
	if len(masterNode) == 0 {
		return pInfo
	}
	masterNodeProgress := make(map[string]store.FileProgress, 0)
	pInfo.SlaveProgress = make(map[string]store.FileProgress, 0)
	for node, progress := range pInfo.Progress {
		if node == masterNode {
			masterNodeProgress[node] = progress
			continue
		}
		pInfo.SlaveProgress[node] = progress
	}
	// only show master node progress here
	pInfo.Progress = masterNodeProgress
	return pInfo
}

func (this *Task) PushGetBlock(sessionId, blockHash string, index int32, block *BlockResp) {
	this.lock.Lock()
	defer this.lock.Unlock()
	key := fmt.Sprintf("%s-%s-%s-%s-%d", this.id, sessionId, this.info.FileHash, blockHash, index)
	log.Debugf("push block to resp channel: %s", key)
	ch, ok := this.blockRespsMap[key]
	if !ok {
		log.Errorf("get block resp channel is nil with key %s", key)
		return
	}
	log.Debugf("push block done")
	go func() {
		log.Debugf("send block to channel: %s", key)
		ch <- block
		log.Debugf("send block to channel done: %s", key)
	}()
}

func (this *Task) BlockFlightsChannelExists(sessionId string, timeStamp int64) bool {
	this.lock.RLock()
	defer this.lock.RUnlock()
	key := fmt.Sprintf("%s-%s-%s-%d", this.id, sessionId, this.info.FileHash, timeStamp)
	_, ok := this.blockFlightRespsMap[key]
	return ok
}

func (this *Task) PushGetBlockFlights(sessionId string, blocks []*BlockResp, timeStamp int64) {
	this.lock.Lock()
	defer this.lock.Unlock()
	key := fmt.Sprintf("%s-%s-%s-%d", this.id, sessionId, this.info.FileHash, timeStamp)
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

func (this *Task) NewBlockRespCh(sessionId, blockHash string, index int32) chan *BlockResp {
	this.lock.Lock()
	defer this.lock.Unlock()
	key := fmt.Sprintf("%s-%s-%s-%s-%d", this.id, sessionId, this.info.FileHash, blockHash, index)
	if this.blockRespsMap == nil {
		this.blockRespsMap = make(map[string]chan *BlockResp)
	}
	ch, ok := this.blockRespsMap[key]
	if !ok {
		ch = make(chan *BlockResp, 1)
		this.blockRespsMap[key] = ch
	}
	log.Debugf("generated block resp channel %s", key)
	return ch
}

func (this *Task) DropBlockRespCh(sessionId, blockHash string, index int32) {
	this.lock.Lock()
	defer this.lock.Unlock()
	key := fmt.Sprintf("%s-%s-%s-%s-%d", this.id, sessionId, this.info.FileHash, blockHash, index)
	log.Debugf("drop block resp channel key: %s", key)
	delete(this.blockRespsMap, key)
}

func (this *Task) NewBlockFlightsRespCh(sessionId string, timeStamp int64) chan []*BlockResp {
	this.lock.Lock()
	defer this.lock.Unlock()
	key := fmt.Sprintf("%s-%s-%s-%d", this.id, sessionId, this.info.FileHash, timeStamp)

	if this.blockFlightRespsMap == nil {
		this.blockFlightRespsMap = make(map[string]chan []*BlockResp)
	}
	ch, ok := this.blockFlightRespsMap[key]
	if !ok {
		ch = make(chan []*BlockResp, 1)
		this.blockFlightRespsMap[key] = ch
	}
	log.Debugf("generated block flight resp channel %s", key)
	return ch
}

func (this *Task) DropBlockFlightsRespCh(sessionId string, timeStamp int64) {
	this.lock.Lock()
	defer this.lock.Unlock()
	key := fmt.Sprintf("%s-%s-%s-%d", this.id, sessionId, this.info.FileHash, timeStamp)
	log.Debugf("drop block resp channel key: %s", key)
	delete(this.blockFlightRespsMap, key)
}

func (this *Task) GetTotalBlockCnt() uint64 {
	this.lock.RLock()
	defer this.lock.RUnlock()
	return this.info.TotalBlockCount
}

func (this *Task) GetPrefix() []byte {
	this.lock.RLock()
	defer this.lock.RUnlock()
	return this.info.Prefix
}

func (this *Task) GetCreatedAt() uint64 {
	this.lock.RLock()
	defer this.lock.RUnlock()
	return this.info.CreatedAt
}

func (this *Task) GetCopyNum() uint32 {
	this.lock.RLock()
	defer this.lock.RUnlock()
	return this.info.CopyNum
}

func (this *Task) NewWorkers(addrs []string, job jobFunc) {
	this.lock.Lock()
	defer this.lock.Unlock()
	if this.workers == nil {
		this.workers = make(map[string]*Worker, 0)
	}
	for _, walletAddr := range addrs {
		w := NewWorker(walletAddr, job)
		this.workers[walletAddr] = w
	}
}

func (this *Task) SetWorkerWorking(walletAddr string, working bool) {
	this.lock.Lock()
	defer this.lock.Unlock()
	w, ok := this.workers[walletAddr]
	if !ok {
		log.Warnf("set remote peer %s working failed, peer not found", walletAddr)
		return
	}
	log.Debugf("set peer %s working %t", walletAddr, working)
	w.SetWorking(working)
}

func (this *Task) SetWorkerUnPaid(walletAddr string, unpaid bool) {
	this.lock.Lock()
	defer this.lock.Unlock()
	w, ok := this.workers[walletAddr]
	if !ok {
		log.Warnf("set remote peer %s unpaid failed, peer not found", walletAddr)
		return
	}
	log.Debugf("set peer %s unpaid %s", walletAddr, unpaid)
	w.SetUnpaid(unpaid)
}

func (this *Task) GetTaskNotify() chan *BlockResp {
	return this.notify
}

func (this *Task) AddBlockReqToPool(blockReqs []*GetBlockReq) {
	this.lock.Lock()
	defer this.lock.Unlock()
	if this.blockReqPool == nil {
		this.blockReqPool = make([]*GetBlockReq, 0)
	}
	if len(blockReqs) == 0 {
		return
	}
	log.Debugf("add block req %s-%s-%d to %s-%d", this.info.FileHash, blockReqs[0].Hash, blockReqs[0].Index,
		blockReqs[len(blockReqs)-1].Hash, blockReqs[len(blockReqs)-1].Index)
	this.blockReqPool = append(this.blockReqPool, blockReqs...)
	if len(this.workers) == 0 {
		return
	}
	go this.notifyBlockReqPoolLen()
}

func (this *Task) InsertBlockReqToPool(blockReqs []*GetBlockReq) {
	this.lock.Lock()
	defer this.lock.Unlock()
	if this.blockReqPool == nil {
		this.blockReqPool = make([]*GetBlockReq, 0)
	}
	if len(blockReqs) == 0 {
		return
	}
	log.Debugf("insert block req %s-%s-%d to %s-%d", this.info.FileHash, blockReqs[0].Hash, blockReqs[0].Index,
		blockReqs[len(blockReqs)-1].Hash, blockReqs[len(blockReqs)-1].Index)
	this.blockReqPool = append(blockReqs, this.blockReqPool...)
	if len(this.workers) == 0 {
		return
	}
	go this.notifyBlockReqPoolLen()
}

func (this *Task) DelBlockReqFromPool(blockReqs []*GetBlockReq) {
	this.lock.Lock()
	defer this.lock.Unlock()
	if this.blockReqPool == nil {
		return
	}
	if len(blockReqs) == 0 {
		return
	}
	log.Debugf("del block req %s-%s-%d to %s-%d", this.info.FileHash, blockReqs[0].Hash, blockReqs[0].Index,
		blockReqs[len(blockReqs)-1].Hash, blockReqs[len(blockReqs)-1].Index)
	hashKey := make(map[string]struct{}, 0)
	for _, r := range blockReqs {
		hashKey[fmt.Sprintf("%s-%d", r.Hash, r.Index)] = struct{}{}
	}
	newBlockReqPool := make([]*GetBlockReq, 0)
	for _, req := range this.blockReqPool {
		if _, ok := hashKey[fmt.Sprintf("%s-%d", req.Hash, req.Index)]; ok {
			continue
		}
		newBlockReqPool = append(newBlockReqPool, req)
	}
	this.blockReqPool = newBlockReqPool
	log.Debugf("block req pool len: %d", len(this.blockReqPool))
	if len(this.workers) == 0 {
		return
	}
	go this.notifyBlockReqPoolLen()
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
	this.lock.RLock()
	defer this.lock.RUnlock()
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
	backupWorkers := make([]*Worker, 0)
	for i, _ := range addrs {
		this.lastWorkerIdx++
		if this.lastWorkerIdx >= len(addrs) {
			this.lastWorkerIdx = 0
		}
		idx := this.lastWorkerIdx
		w := this.workers[addrs[idx]]
		working := w.Working()
		workFailed := w.WorkFailed(reqHash)
		workUnpaid := w.Unpaid()
		failedTooMuch := w.FailedTooMuch(fileHash)
		if working || workUnpaid || workFailed || failedTooMuch {
			log.Debugf("#%d worker is working: %t, failed: %t, unpaid: %t, file: %s, block: %s",
				i, working, workFailed, workUnpaid, fileHash, reqHash)
			if workFailed && !working && !workUnpaid && !failedTooMuch {
				backupWorkers = append(backupWorkers, w)
			}
			continue
		}
		worker = w
		break
	}
	log.Debugf("GetIdleWorker %s, pool-len: %d, worker %v", reqHash, len(this.blockReqPool), worker)
	if worker != nil {
		return worker
	}
	if len(backupWorkers) > 0 {
		return backupWorkers[0]
	}
	return worker
}

func (this *Task) NotifyBlock(blk *BlockResp) {
	log.Debugf("notify block %s-%d-%d-%s", blk.Hash, blk.Index, blk.Offset, blk.PeerAddr)
	this.notify <- blk
}

func (this *Task) GetStoreType() uint32 {
	this.lock.RLock()
	defer this.lock.RUnlock()
	return this.info.StoreType
}

func (this *Task) GetUpdatedAt() uint64 {
	this.lock.RLock()
	defer this.lock.RUnlock()
	return this.info.UpdatedAt
}

func (this *Task) GetUrl() string {
	this.lock.RLock()
	defer this.lock.RUnlock()
	return this.info.Url
}

func (this *Task) ActiveWorker(addr string) {
	this.lock.Lock()
	defer this.lock.Unlock()
	w, ok := this.workers[addr]
	if !ok {
		return
	}
	w.Active()
}

func (this *Task) IsTimeout() bool {
	this.lock.RLock()
	defer this.lock.RUnlock()
	timeout := uint64(common.DOWNLOAD_FILE_TIMEOUT * 1000)
	now := utils.GetMilliSecTimestamp()
	for _, w := range this.workers {
		if now-w.ActiveTime() < timeout {
			log.Debugf("worker active time %s %d", w.WalletAddr(), w.ActiveTime())
			return false
		}
	}
	return true
}

func (this *Task) AllPeerPaidFailed() bool {
	this.lock.RLock()
	defer this.lock.RUnlock()
	if len(this.workers) == 0 {
		return false
	}
	paidFailed := true
	for _, w := range this.workers {
		if !w.Unpaid() {
			paidFailed = false
			break
		}
	}
	return paidFailed
}

// HasWorker. check if worker exist
func (this *Task) HasWorker(addr string) bool {
	this.lock.RLock()
	defer this.lock.RUnlock()
	_, ok := this.workers[addr]
	return ok
}

// WorkerIdleDuration. worker idle duration
func (this *Task) WorkerIdleDuration(addr string) uint64 {
	this.lock.RLock()
	defer this.lock.RUnlock()
	w, ok := this.workers[addr]
	if !ok {
		return 0
	}
	now := utils.GetMilliSecTimestamp()
	return now - w.ActiveTime()
}

func (this *Task) SetWorkerNetPhase(addr string, phase int) {
	this.lock.Lock()
	defer this.lock.Unlock()
	this.workerNetPhase[addr] = phase
}

func (this *Task) GetWorkerNetPhase(addr string) int {
	this.lock.RLock()
	defer this.lock.RUnlock()
	return this.workerNetPhase[addr]
}

// cleanBlockReqPool. clean all block req pool. non thread-safe
func (this *Task) cleanBlockReqPool() {
	log.Debugf("CleanBlockReqPool")
	if this.blockReqPool == nil {
		this.blockReqPool = make([]*GetBlockReq, 0)
		return
	}
	this.blockReqPool = this.blockReqPool[:0]
	go this.notifyBlockReqPoolLen()
}

// notifyBlockReqPoolLen. notify block req pool len
func (this *Task) notifyBlockReqPoolLen() {
	log.Debugf("notify block req update")
	this.blockReqPoolNotify <- &blockReqPool{}
	log.Debugf("notify block req update done")
}

func newTask(id string, info *store.TaskInfo, db *store.TaskDB) *Task {
	t := &Task{
		id:                 id,
		info:               info,
		blockReq:           make(chan []*GetBlockReq, common.MAX_TASK_BLOCK_REQ),
		notify:             make(chan *BlockResp, common.MAX_TASK_BLOCK_NOTIFY),
		lastWorkerIdx:      -1,
		peerSenIds:         make(map[string]string, common.MAX_TASK_SESSION_NUM),
		db:                 db,
		lock:               new(sync.RWMutex),
		workerNetPhase:     make(map[string]int),
		blockReqPoolNotify: make(chan *blockReqPool),
	}
	return t
}

func taskIdKey(hash, walletAddress string, taskType store.TaskType) string {
	return store.TaskIdWithFile(hash, walletAddress, taskType)
}
