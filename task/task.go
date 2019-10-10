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
	Type          store.TaskType    // task type
	Url           string            // task url
	StoreType     uint64            // store type
	FileName      string            // file name
	FileHash      string            // file hash
	FilePath      string            // file path
	CopyNum       uint64            // copyNum
	Total         uint64            // total file's blocks count
	Count         map[string]uint64 // address <=> count
	TaskState     store.TaskState   // task state
	ProgressState TaskProgressState // TaskProgressState
	Result        interface{}       // finish result
	ErrorCode     uint32            // error code
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

type Task struct {
	id           string // id
	info         *store.TaskInfo
	peerSenIds   map[string]string // request peerAddr <=> session id
	transferring bool              // fetch is transferring flag
	// TODO: refactor, delete below two channels, use request and reply
	blockReq            chan []*GetBlockReq          // fetch block request channel
	blockRespsMap       map[string]chan *BlockResp   // map key <=> *BlockResp
	blockFlightRespsMap map[string]chan []*BlockResp // map key <=> []*BlockResp
	blockReqPool        []*GetBlockReq               // get block request pool
	workers             map[string]*Worker           // workers to request block
	notify              chan *BlockResp              // notify download block
	backupOpt           *BackupFileOpt               // backup file options
	lock                *sync.RWMutex                // lock
	lastWorkerIdx       int                          // last worker index
	batch               bool                         // flag of batch set
	db                  *store.FileDB                // db
}

// NewTask. new task for file, and set the task info to DB.
func NewTask(taskT store.TaskType, db *store.FileDB) *Task {
	id, _ := uuid.NewUUID()
	info, err := db.NewTaskInfo(id.String(), taskT)
	if err != nil {
		log.Errorf("new file info failed %s", err)
		return nil
	}
	t := newTask(id.String(), info, db)
	t.info.TaskState = uint64(store.TaskStatePrepare)
	err = db.SaveFileInfo(t.info)
	if err != nil {
		log.Debugf("save file info failed err %s", err)
		return nil
	}
	return t
}

// GetTaskFromDB. get a task object from DB with file info id
func GetTaskFromDB(id string, db *store.FileDB) (*Task, error) {
	info, err := db.GetFileInfo(id)
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
func NewTaskFromDB(id string, db *store.FileDB) (*Task, error) {
	info, err := db.GetFileInfo(id)
	if err != nil {
		log.Errorf("[Task NewTaskFromDB] get file info failed, id: %s", id)
		return nil, err
	}
	if info == nil {
		log.Warnf("[Task NewTaskFromDB] recover task get file info is nil, id: %v", id)
		return nil, nil
	}
	if (store.TaskState(info.TaskState) == store.TaskStatePause || store.TaskState(info.TaskState) == store.TaskStateDoing) && info.UpdatedAt+common.DOWNLOAD_FILE_TIMEOUT*1000 < utils.GetMilliSecTimestamp() {
		log.Warnf("[Task NewTaskFromDB] task: %s is expired, updatedAt: %d", id, info.UpdatedAt)
	}
	sessions, err := db.GetFileSessions(id)
	if err != nil {
		log.Errorf("[Task NewTaskFromDB] set task session: %s", err)
		return nil, err
	}
	state := store.TaskState(info.TaskState)
	if state == store.TaskStatePrepare || state == store.TaskStateDoing || state == store.TaskStateCancel {
		state = store.TaskStatePause
	}
	t := newTask(id, info, db)
	t.info.TaskState = uint64(state)
	err = db.SaveFileInfo(t.info)
	if err != nil {
		log.Errorf("[Task NewTaskFromDB] set file info failed: %s", err)
		return nil, err
	}
	log.Debugf("task name: %s, state: %d", t.info.FileName, state)
	for _, session := range sessions {
		log.Debugf("set setssion : %s %s", session.WalletAddr, session.SessionId)
		t.peerSenIds[session.WalletAddr] = session.SessionId
	}
	log.Debugf("NewTaskFromDB store type: %d", t.info.StoreType)
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
	return this.db.SaveFileInfo(this.info)
}

func (this *Task) SetFileHash(fileHash string) error {
	this.lock.Lock()
	defer this.lock.Unlock()
	this.info.FileHash = fileHash
	if this.batch {
		return nil
	}
	return this.db.SaveFileInfo(this.info)
}

func (this *Task) SetPrefix(prefix string) error {
	this.lock.Lock()
	defer this.lock.Unlock()
	this.info.Prefix = []byte(prefix)
	if this.batch {
		return nil
	}
	return this.db.SaveFileInfo(this.info)
}
func (this *Task) SetFileOwner(fileOwner string) error {
	this.lock.Lock()
	defer this.lock.Unlock()
	this.info.FileOwner = fileOwner
	if this.batch {
		return nil
	}
	return this.db.SaveFileInfo(this.info)
}

func (this *Task) SetWalletaddr(walletAddr string) error {
	this.lock.Lock()
	defer this.lock.Unlock()
	this.info.WalletAddress = walletAddr
	if this.batch {
		return nil
	}
	return this.db.SaveFileInfo(this.info)
}

func (this *Task) SetFilePath(filePath string) error {
	this.lock.Lock()
	defer this.lock.Unlock()
	this.info.FilePath = filePath
	if this.batch {
		return nil
	}
	return this.db.SaveFileInfo(this.info)
}

func (this *Task) SetSimpleCheckSum(checksum string) error {
	this.lock.Lock()
	defer this.lock.Unlock()
	this.info.SimpleChecksum = checksum
	if this.batch {
		return nil
	}
	return this.db.SaveFileInfo(this.info)
}

func (this *Task) SetStoreType(storeType uint64) error {
	this.lock.Lock()
	defer this.lock.Unlock()
	this.info.StoreType = storeType
	if this.batch {
		return nil
	}
	return this.db.SaveFileInfo(this.info)
}

func (this *Task) SetCopyNum(copyNum uint64) error {
	this.lock.Lock()
	defer this.lock.Unlock()
	this.info.CopyNum = copyNum
	if this.batch {
		return nil
	}
	return this.db.SaveFileInfo(this.info)
}

func (this *Task) SetUrl(url string) error {
	this.lock.Lock()
	defer this.lock.Unlock()
	this.info.Url = url
	if this.batch {
		return nil
	}
	return this.db.SaveFileInfo(this.info)
}

func (this *Task) SetOwner(owner string) error {
	this.lock.Lock()
	defer this.lock.Unlock()
	this.info.FileOwner = owner
	if this.batch {
		return nil
	}
	return this.db.SaveFileInfo(this.info)
}

func (this *Task) SetTaskState(newState store.TaskState) error {
	this.lock.Lock()
	defer this.lock.Unlock()
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
		log.Debugf("CleanBlockReqPool")
		if this.blockReqPool == nil {
			this.blockReqPool = make([]*GetBlockReq, 0)
		} else {
			this.blockReqPool = this.blockReqPool[:0]
		}
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
	this.info.TaskState = uint64(newState)
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
	return this.db.SaveFileInfo(this.info)
}

func (this *Task) SetInorder(inOrder bool) error {
	this.lock.Lock()
	defer this.lock.Unlock()
	this.info.InOrder = inOrder
	if this.batch {
		return nil
	}
	return this.db.SaveFileInfo(this.info)
}

func (this *Task) SetOnlyBlock(onlyBlock bool) error {
	this.lock.Lock()
	defer this.lock.Unlock()
	this.info.OnlyBlock = onlyBlock
	if this.batch {
		return nil
	}
	return this.db.SaveFileInfo(this.info)
}

func (this *Task) SetTransferState(transferState uint64) error {
	this.lock.Lock()
	defer this.lock.Unlock()
	this.info.TranferState = transferState
	if this.batch {
		return nil
	}
	return this.db.SaveFileInfo(this.info)
}

func (this *Task) SetTotalBlockCnt(cnt uint64) error {
	this.lock.Lock()
	defer this.lock.Unlock()
	this.info.TotalBlockCount = cnt
	if this.batch {
		return nil
	}
	return this.db.SaveFileInfo(this.info)
}

func (this *Task) SetPrivateKey(priKey []byte) error {
	this.lock.Lock()
	defer this.lock.Unlock()
	this.info.ProvePrivKey = priKey
	if this.batch {
		return nil
	}
	return this.db.SaveFileInfo(this.info)
}

func (this *Task) SetStoreTx(tx string) error {
	this.lock.Lock()
	defer this.lock.Unlock()
	this.info.StoreTx = tx
	if this.batch {
		return nil
	}
	return this.db.SaveFileInfo(this.info)
}

func (this *Task) SetResult(result interface{}, errorCode uint32, errorMsg string) error {
	this.lock.Lock()
	defer this.lock.Unlock()
	this.info.ErrorCode = errorCode
	this.info.ErrorMsg = errorMsg
	if errorCode != 0 {
		this.info.TaskState = uint64(store.TaskStateFailed)
	} else {
		log.Debugf("task: %s has done", this.id)
		this.info.Result = result
		this.info.TaskState = uint64(store.TaskStateDone)
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
	return this.db.SaveFileInfo(this.info)
}

func (this *Task) SetWhiteListTx(whiteListTx string) error {
	this.lock.Lock()
	defer this.lock.Unlock()
	this.info.WhitelistTx = whiteListTx
	if this.batch {
		return nil
	}
	return this.db.SaveFileInfo(this.info)
}

func (this *Task) SetRegUrlTx(regUrlTx string) error {
	this.lock.Lock()
	defer this.lock.Unlock()
	this.info.RegisterDNSTx = regUrlTx
	if this.batch {
		return nil
	}
	return this.db.SaveFileInfo(this.info)
}

func (this *Task) SetBindUrlTx(bindUrlTx string) error {
	this.lock.Lock()
	defer this.lock.Unlock()
	this.info.BindDNSTx = bindUrlTx
	if this.batch {
		return nil
	}
	return this.db.SaveFileInfo(this.info)
}

func (this *Task) BatchCommit() error {
	this.lock.Lock()
	defer this.lock.Unlock()
	if !this.batch {
		return nil
	}
	this.batch = false
	return this.db.SaveFileInfo(this.info)
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

func (this *Task) AddUploadedBlock(id, blockHashStr, nodeAddr string, index uint32, dataSize, offset uint64) error {
	this.lock.Lock()
	defer this.lock.Unlock()
	err := this.db.AddUploadedBlock(id, blockHashStr, nodeAddr, index, dataSize, offset)
	if err != nil {
		return err
	}
	newInfo, err := this.db.GetFileInfo(id)
	if err != nil {
		return err
	}
	this.info = newInfo
	return nil
}

func (this *Task) SetBlocksUploaded(id, nodeAddr string, blockInfos []*store.BlockInfo) error {
	this.lock.Lock()
	defer this.lock.Unlock()
	err := this.db.SetBlocksUploaded(id, nodeAddr, blockInfos)
	if err != nil {
		return err
	}
	newInfo, err := this.db.GetFileInfo(id)
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
	newInfo, err := this.db.GetFileInfo(id)
	if err != nil {
		return err
	}
	this.info = newInfo
	return nil
}

func (this *Task) SetBlockDownloaded(id, blockHashStr, nodeAddr string, index uint32, offset int64, links []string) error {
	this.lock.Lock()
	defer this.lock.Unlock()
	err := this.db.SetBlockDownloaded(id, blockHashStr, nodeAddr, index, offset, links)
	if err != nil {
		return err
	}
	newInfo, err := this.db.GetFileInfo(id)
	if err != nil {
		return err
	}
	this.info = newInfo
	return nil
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

func (this *Task) DetailState() TaskProgressState {
	this.lock.RLock()
	defer this.lock.RUnlock()
	return TaskProgressState(this.info.TranferState)
}

func (this *Task) GetPrivateKey() []byte {
	this.lock.RLock()
	defer this.lock.RUnlock()
	return this.info.ProvePrivKey
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
		Count:         this.db.FileProgress(this.id),
		TaskState:     store.TaskState(this.info.TaskState),
		ProgressState: TaskProgressState(this.info.TranferState),
		CreatedAt:     this.info.CreatedAt / common.MILLISECOND_PER_SECOND,
		UpdatedAt:     this.info.UpdatedAt / common.MILLISECOND_PER_SECOND,
		Result:        this.info.Result,
		ErrorCode:     this.info.ErrorCode,
		ErrorMsg:      this.info.ErrorMsg,
	}
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

	go func() {
		log.Debugf("send block to channel: %s", key)
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
	ch := this.blockFlightRespsMap[key]
	delete(this.blockFlightRespsMap, key)
	if ch != nil {
		close(ch)
	}
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

func (this *Task) GetCopyNum() uint64 {
	this.lock.RLock()
	defer this.lock.RUnlock()
	return this.info.CopyNum
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
	return this.notify
}

func (this *Task) AddBlockReqToPool(blockHash string, index int32) {
	this.lock.Lock()
	defer this.lock.Unlock()
	if this.blockReqPool == nil {
		this.blockReqPool = make([]*GetBlockReq, 0)
	}
	log.Debugf("add block req %s-%s-%d", this.info.FileHash, blockHash, index)
	this.blockReqPool = append(this.blockReqPool, &GetBlockReq{
		FileHash: this.info.FileHash,
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
	for i, _ := range addrs {
		this.lastWorkerIdx++
		if this.lastWorkerIdx >= len(addrs) {
			this.lastWorkerIdx = 0
		}
		idx := this.lastWorkerIdx
		w := this.workers[addrs[idx]]
		if w.Working() || w.WorkFailed(reqHash) || w.Unpaid() || w.FailedTooMuch(fileHash) {
			log.Debugf("#%d worker is working: %t, failed: %t, unpaid: %t, file: %s, block: %s", i, w.Working(), w.WorkFailed(reqHash), w.Unpaid(), fileHash, reqHash)
			continue
		}
		worker = w
		break
	}
	log.Debugf("GetIdleWorker %s, pool-len: %d, worker %v", reqHash, len(this.blockReqPool), worker)
	return worker
}

func (this *Task) NotifyBlock(blk *BlockResp) {
	log.Debugf("notify block %s-%d-%d-%s", blk.Hash, blk.Index, blk.Offset, blk.PeerAddr)
	this.notify <- blk
}

func (this *Task) GetStoreType() uint64 {
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

func newTask(id string, info *store.TaskInfo, db *store.FileDB) *Task {
	t := &Task{
		id:            id,
		info:          info,
		blockReq:      make(chan []*GetBlockReq, common.MAX_TASK_BLOCK_REQ),
		notify:        make(chan *BlockResp, common.MAX_TASK_BLOCK_NOTIFY),
		lastWorkerIdx: -1,
		peerSenIds:    make(map[string]string, common.MAX_TASK_SESSION_NUM),
		db:            db,
		lock:          new(sync.RWMutex),
	}
	return t
}

func taskIdKey(hash, walletAddress string, taskType store.TaskType) string {
	return store.TaskIdWithFile(hash, walletAddress, taskType)
}
