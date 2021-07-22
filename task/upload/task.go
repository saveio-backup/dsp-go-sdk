package upload

import (
	"sync"

	sdkErr "github.com/saveio/dsp-go-sdk/error"
	"github.com/saveio/dsp-go-sdk/store"
	"github.com/saveio/dsp-go-sdk/task/base"
	uTime "github.com/saveio/dsp-go-sdk/utils/time"
)

type UploadTask struct {
	*base.Task                  // base task
	nodeNetPhase map[string]int // network msg interact phase, used to check msg transaction, wallet addr <=> phase
}

// NewUploadTask. create a new upload task and save to db
func NewUploadTask(taskId string, taskType store.TaskType, db *store.TaskDB) *UploadTask {
	dt := &UploadTask{
		Task:         base.NewTask(taskId, taskType, db),
		nodeNetPhase: make(map[string]int),
	}
	return dt
}

// UploadTask. init a upload task
func InitUploadTask(db *store.TaskDB) *UploadTask {
	t := &UploadTask{
		nodeNetPhase: make(map[string]int),
	}

	baseTask := &base.Task{
		DB:   db,
		Lock: new(sync.RWMutex),
	}

	t.Task = baseTask

	return t
}

func (this *UploadTask) SetNodeNetPhase(addres []string, phase int) {
	for _, addr := range addres {
		this.nodeNetPhase[addr] = phase
	}
}

func (this *UploadTask) GetNodeNetPhase(addr string) int {
	this.Lock.RLock()
	defer this.Lock.RUnlock()
	return this.nodeNetPhase[addr]
}

// HasWorker. check if worker exist
func (this *UploadTask) HasWorker(addr string) bool {
	this.Lock.RLock()
	defer this.Lock.RUnlock()
	_, ok := this.Workers[addr]
	return ok
}

// WorkerIdleDuration. worker idle duration
func (this *UploadTask) WorkerIdleDuration(addr string) uint64 {
	this.Lock.RLock()
	defer this.Lock.RUnlock()
	w, ok := this.Workers[addr]
	if !ok {
		return 0
	}
	now := uTime.GetMilliSecTimestamp()
	return now - w.ActiveTime()
}

func (this *UploadTask) SetRegUrlTx(regUrlTx string) error {
	this.Lock.Lock()
	defer this.Lock.Unlock()
	info := this.GetTaskInfo()
	info.RegisterDNSTx = regUrlTx
	if this.Batch {
		return nil
	}
	return this.DB.SaveTaskInfo(info)
}

func (this *UploadTask) SetBindUrlTx(bindUrlTx string) error {
	this.Lock.Lock()
	defer this.Lock.Unlock()
	info := this.GetTaskInfo()
	info.BindDNSTx = bindUrlTx
	if this.Batch {
		return nil
	}
	return this.DB.SaveTaskInfo(info)
}

func (this *UploadTask) SetBlocksUploaded(id, nodeAddr string, blockInfos []*store.BlockInfo) error {
	this.Lock.Lock()
	defer this.Lock.Unlock()
	err := this.DB.SetBlocksUploaded(id, nodeAddr, blockInfos)
	if err != nil {
		return err
	}

	return nil
}

func (this *UploadTask) SetUploadProgressDone(id, nodeAddr string) error {
	this.Lock.Lock()
	defer this.Lock.Unlock()
	err := this.DB.SetUploadProgressDone(id, nodeAddr)
	if err != nil {
		return err
	}

	return nil
}

func (this *UploadTask) SetInOrder(inOrder bool) error {
	this.Lock.Lock()
	defer this.Lock.Unlock()
	info := this.GetTaskInfo()
	info.InOrder = inOrder
	if this.Batch {
		return nil
	}
	return this.DB.SaveTaskInfo(info)
}

func (this *UploadTask) SetWhiteListTx(whiteListTx string) error {
	info := this.GetTaskInfo()
	info.WhitelistTx = whiteListTx
	if this.Batch {
		return nil
	}
	return this.DB.SaveTaskInfo(info)
}

func (this *UploadTask) GetRegUrlTx() string {
	this.Lock.RLock()
	defer this.Lock.RUnlock()
	info := this.GetTaskInfo()
	return info.RegisterDNSTx
}

func (this *UploadTask) GetBindUrlTx() string {
	this.Lock.RLock()
	defer this.Lock.RUnlock()
	info := this.GetTaskInfo()
	return info.BindDNSTx
}

func (this *UploadTask) GetWhitelistTx() string {
	this.Lock.RLock()
	defer this.Lock.RUnlock()
	info := this.GetTaskInfo()
	return info.WhitelistTx
}

func (this *UploadTask) GetSimpleChecksum() string {
	this.Lock.RLock()
	defer this.Lock.RUnlock()
	info := this.GetTaskInfo()
	return info.SimpleChecksum
}

func (this *UploadTask) GetProveLevel() uint64 {
	this.Lock.RLock()
	defer this.Lock.RUnlock()
	info := this.GetTaskInfo()
	return info.ProveLevel
}

func (this *UploadTask) GetBlocksRoot() string {
	this.Lock.RLock()
	defer this.Lock.RUnlock()
	info := this.GetTaskInfo()
	return info.BlocksRoot
}

func (this *UploadTask) GetExpiredHeight() uint64 {
	this.Lock.RLock()
	defer this.Lock.RUnlock()
	info := this.GetTaskInfo()
	return info.ExpiredHeight
}

func (this *UploadTask) GetRealFileSize() uint64 {
	this.Lock.RLock()
	defer this.Lock.RUnlock()
	info := this.GetTaskInfo()
	return info.RealFileSize
}

func (this *UploadTask) GetStoreType() uint32 {
	this.Lock.RLock()
	defer this.Lock.RUnlock()
	info := this.GetTaskInfo()
	return info.StoreType
}

func (this *UploadTask) GetPrivilege() uint64 {
	this.Lock.RLock()
	defer this.Lock.RUnlock()
	info := this.GetTaskInfo()
	return info.Privilege
}

func (this *UploadTask) GetProveInterval() uint64 {
	this.Lock.RLock()
	defer this.Lock.RUnlock()
	info := this.GetTaskInfo()
	return info.ProveInterval
}

func (this *UploadTask) GetEncrypt() bool {
	this.Lock.RLock()
	defer this.Lock.RUnlock()
	info := this.GetTaskInfo()
	return info.Encrypt
}

func (this *UploadTask) GetEncryptPassword() []byte {
	this.Lock.RLock()
	defer this.Lock.RUnlock()
	info := this.GetTaskInfo()
	return info.EncryptPassword
}

func (this *UploadTask) GetRegisterDNS() bool {
	this.Lock.RLock()
	defer this.Lock.RUnlock()
	info := this.GetTaskInfo()
	return info.RegisterDNS
}

func (this *UploadTask) GetBindDNS() bool {
	this.Lock.RLock()
	defer this.Lock.RUnlock()
	info := this.GetTaskInfo()
	return info.BindDNS
}

func (this *UploadTask) GetWhiteList() []*store.WhiteList {
	this.Lock.RLock()
	defer this.Lock.RUnlock()
	info := this.GetTaskInfo()
	return info.WhiteList
}

func (this *UploadTask) GetShare() bool {
	this.Lock.RLock()
	defer this.Lock.RUnlock()
	info := this.GetTaskInfo()
	return info.Share
}

func (this *UploadTask) GetPrimaryNodes() []string {
	this.Lock.RLock()
	defer this.Lock.RUnlock()
	info := this.GetTaskInfo()
	return info.PrimaryNodes
}

func (this *UploadTask) GetProveParams() []byte {
	this.Lock.RLock()
	defer this.Lock.RUnlock()
	info := this.GetTaskInfo()
	return info.ProveParams
}

func (this *UploadTask) UpdateTaskProgress(taskId, nodeAddr string, progress uint64) error {
	if this.IsTaskCancel() || this.IsTaskFailed() {
		return nil
	}
	err := this.DB.UpdateTaskProgress(taskId, nodeAddr, progress)
	if err != nil {
		return sdkErr.New(sdkErr.SET_FILEINFO_DB_ERROR, err.Error())
	}
	return nil
}

// // deprecated
// func (this *UploadTask) GetBlockReq() chan []*types.GetBlockReq {
// 	return this.blockReq
// }

func (this *UploadTask) getUploadNodeFromDB(taskId, fileHashStr string) []string {
	nodeList := this.DB.GetUploadedBlockNodeList(taskId, fileHashStr, 0)
	if len(nodeList) != 0 {
		return nodeList
	}
	return nil
}
