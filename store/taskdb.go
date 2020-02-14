package store

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/saveio/dsp-go-sdk/common"
	"github.com/saveio/dsp-go-sdk/utils"
	"github.com/saveio/themis/common/log"
	"github.com/syndtr/goleveldb/leveldb"

	fs "github.com/saveio/themis/smartcontract/service/native/savefs"
)

// TaskDB. implement a db storage for save information of sending/downloading/downloaded files
type TaskDB struct {
	db   *LevelDBStore
	lock *sync.RWMutex
}

type TaskType int

const (
	TaskTypeNone TaskType = iota
	TaskTypeUpload
	TaskTypeDownload
	TaskTypeShare
	TaskTypeBackup
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

// blockInfo record a block infomation of a file
type BlockInfo struct {
	TaskId     string            `json:"task_id"`
	FileHash   string            `json:"file_hash"`
	Hash       string            `json:"hash"`                  // block  hash
	Index      uint64            `json:"index"`                 // block index of file
	DataOffset uint64            `json:"data_offset"`           // block raw data offset
	DataSize   uint64            `json:"data_size"`             // block data size
	NodeList   []string          `json:"node_list,omitempty"`   // uploaded node list
	ReqTimes   map[string]uint32 `json:"block_req_times"`       // record block request times for peer
	LinkHashes []string          `json:"link_hashes,omitempty"` // child link hashes slice
}

type Payment struct {
	WalletAddress string `json:"wallet_address"`
	Asset         int32  `json:"asset"`
	Amount        uint64 `json:"amount"`
	PaymentId     int32  `json:"paymentId"`
}

type WhiteList struct {
	Address     string
	StartHeight uint64
	EndHeight   uint64
}

// fileInfo keep all blocks infomation and the prove private key for generating tags
type TaskInfo struct {
	Id                 string            `json:"id"`                               // task id
	Index              uint32            `json:"index"`                            // task index
	FileHash           string            `json:"file_hash"`                        // file hash
	BlocksRoot         string            `json:"blocks_root"`                      // blocks hash root
	FileName           string            `json:"file_name"`                        // file name
	FileDesc           string            `json:"file_desc,omitempty"`              // file desc
	FilePath           string            `json:"file_path"`                        // file absolute path
	FileOwner          string            `json:"file_owner"`                       // file owner wallet address
	SimpleChecksum     string            `json:"simple_checksum,omitempty"`        // hash of first 128 KB and last 128 KB from file content
	WalletAddress      string            `json:"wallet_address"`                   // task belong to
	CopyNum            uint32            `json:"copy_num,omitempty"`               // copy num
	Type               TaskType          `json:"file_info_type"`                   // task type
	StoreTx            string            `json:"store_tx"`                         // store tx hash
	StoreTxHeight      uint32            `json:"store_tx_height"`                  // store tx height
	RegisterDNSTx      string            `json:"register_dns_tx,omitempty"`        // register dns tx
	BindDNSTx          string            `json:"bind_dns_tx,omitempty"`            // bind dns tx
	WhitelistTx        string            `json:"whitelist_tx,omitempty"`           // first op whitelist tx
	TotalBlockCount    uint64            `json:"total_block_count"`                // total block count
	TaskState          uint32            `json:"task_state"`                       // task state
	ProvePrivKey       []byte            `json:"prove_private_key,omitempty"`      // prove private key params
	Prefix             []byte            `json:"prefix"`                           // file prefix
	EncryptHash        string            `json:"encrypt_hash,omitempty"`           // encrypt hash
	EncryptSalt        string            `json:"encrypt_salt,omitempty"`           // encrypt salt
	Url                string            `json:"url"`                              // url
	Link               string            `json:"link"`                             // url <=> link
	CurrentBlock       string            `json:"current_block_hash,omitempty"`     // current transferred block
	CurrentIndex       uint64            `json:"current_block_index,omitempty"`    // current transferred block index
	StoreType          uint32            `json:"store_type"`                       // store type
	InOrder            bool              `json:"in_order,omitempty"`               // is in order
	OnlyBlock          bool              `json:"only_block,omitempty"`             // send only raw block data
	TranferState       uint32            `json:"transfer_state"`                   // transfer state
	ReferId            string            `json:"refer_id,omitempty"`               // refer task id
	PrimaryNodes       []string          `json:"primary_nodes,omitempty"`          // primary nodes wallet address
	PrimaryHostAddrs   map[string]string `json:"primary_node_hosts,omitempty"`     // primary nodes wallet address <=> host addrs
	CandidateNodes     []string          `json:"candidate_nodes,omitempty"`        // candidate nodes wallet address
	CandidateHostAddrs map[string]string `json:"candidate_node_hosts,omitempty"`   // candidate nodes wallet address <=> host addrs
	CreatedAt          uint64            `json:"createdAt"`                        // createAt, unit ms
	CreatedAtHeight    uint32            `json:"createdAt_block_height,omitempty"` // created at block height
	UpdatedAt          uint64            `json:"updatedAt"`                        // updatedAt, unit ms
	UpdatedAtHeight    uint32            `json:"updatedAt_block_height,omitempty"` // updatedAt block height
	ExpiredHeight      uint64            `json:"expired_block_height,omitempty"`   // expiredAt block height
	Asset              int32             `json:"asset,omitempty"`                  // download task pay asset
	DecryptPwd         string            `json:"decrypt_pwd,omitempty"`            // download task with decrypt pwd
	Free               bool              `json:"free,omitempty"`                   // download task with free opts
	SetFileName        bool              `json:"set_file_name,omitempty"`          // download task with set file name
	MaxPeerCnt         int               `json:"max_peer_count,omitempty"`         // download task with max peer count to download
	RealFileSize       uint64            `json:"real_file_size"`                   // real file size in KB
	FileSize           uint64            `json:"file_size"`                        // real file size in block
	ProveInterval      uint64            `json:"prove_interval,omitempty"`         // prove interval
	Privilege          uint64            `json:"privilege,omitempty"`              // file privilege
	Encrypt            bool              `json:"encrypt,omitempty"`                // encrypt or not
	EncryptPassword    []byte            `json:"encrypt_pwd,omitempty"`            // encrypted pwd
	RegisterDNS        bool              `json:"register_dns,omitempty"`           // register dns or not
	BindDNS            bool              `json:"bind_dns,omitempty"`               // bind dns or not
	WhiteList          []*WhiteList      `json:"white_list,omitempty"`             // white list
	Share              bool              `json:"share,omitempty"`                  // share or not
	ErrorCode          uint32            `json:"error_code,omitempty"`             // error code
	ErrorMsg           string            `json:"error_msg,omitempty"`              // error msg
	Result             interface{}       `json:"result"`                           // task complete result
}

type FileProgress struct {
	TaskId         string    `json:"task_id"`
	NodeHostAddr   string    `json:"node_host_addr"`
	NodeWalletAddr string    `json:"node_wallet_addr"`
	Progress       uint64    `json:"progress"`
	TransferCount  uint32    `json:"transfer_count"`
	State          TaskState `json:"progress_state"`
	CreatedAt      uint64    `json:"createdAt"`
	UpdatedAt      uint64    `json:"updatedAt"`
	NextUpdatedAt  uint64    `json:"next_updatedAt"`
	Speeds         []uint64  `json:"speeds"`
}

func (prog FileProgress) AvgSpeed() uint64 {
	sum := uint64(0)
	for _, speed := range prog.Speeds {
		sum += speed
	}
	if sum == 0 {
		return 0
	}
	return sum / uint64(len(prog.Speeds))
}

type FileDownloadUnPaid struct {
	TaskId       string             `json:"task_id"`
	NodeHostAddr string             `json:"node_host_addr"`
	Payments     map[int32]*Payment `json:"payments"`
}

type Session struct {
	SessionId  string `json:"session_id"`
	WalletAddr string `json:"wallet_addr"`
	HostAddr   string `json:"host_addr"`
	Asset      uint32 `json:"asset"`
	UnitPrice  uint64 `json:"unit_price"`
}

type TaskCount struct {
	Index         uint32 `json:"index"`
	TotalCount    uint32 `json:"total_count"`
	UploadCount   uint32 `json:"upload_count"`
	DownloadCount uint32 `json:"download_count"`
	ShareCount    uint32 `json:"share_count"`
}

func NewTaskDB(db *LevelDBStore) *TaskDB {
	return &TaskDB{
		db:   db,
		lock: new(sync.RWMutex),
	}
}

func (this *TaskDB) Close() error {
	return this.db.Close()
}

func (this *TaskDB) NewTaskInfo(id string, ft TaskType) (*TaskInfo, error) {
	this.lock.Lock()
	defer this.lock.Unlock()
	taskCount, err := this.getTaskCount()
	if err != nil {
		return nil, err
	}
	fi := &TaskInfo{
		Id:        id,
		Index:     taskCount.Index,
		Type:      ft,
		CreatedAt: utils.GetMilliSecTimestamp(),
	}
	batch := this.db.NewBatch()
	// store to undone task list
	err = this.batchAddToUndoneList(batch, id, ft)
	if err != nil {
		return nil, err
	}
	err = this.batchAddToUnSlavedList(batch, id, ft)
	if err != nil {
		return nil, err
	}
	// store task info
	err = this.batchSaveTaskInfo(batch, fi)
	if err != nil {
		return nil, err
	}
	// store index <=> taskId
	this.db.BatchPut(batch, []byte(TaskIdIndexKey(uint32(taskCount.Index))), []byte(id))
	// store total task count
	taskCount.Index++
	taskCount.TotalCount++
	switch ft {
	case TaskTypeUpload:
		taskCount.UploadCount++
	case TaskTypeDownload:
		taskCount.DownloadCount++
	case TaskTypeShare:
		taskCount.ShareCount++
	}
	err = this.batchSaveTaskCount(batch, taskCount)
	if err != nil {
		return nil, err
	}
	err = this.db.BatchCommit(batch)
	if err != nil {
		return nil, err
	}
	return fi, nil
}

func (this *TaskDB) getTaskCount() (*TaskCount, error) {
	countBuf, err := this.db.Get([]byte(TaskCountKey()))
	if err != nil && err != leveldb.ErrNotFound {
		return nil, err
	}
	taskCount := &TaskCount{}
	if len(countBuf) == 0 {
		return taskCount, nil
	}
	err = json.Unmarshal(countBuf, &taskCount)
	return taskCount, err
}

// GetTaskIdList. Get all task id list with offset, limit, task type
func (this *TaskDB) GetTaskIdList(offset, limit uint32, ft TaskType, allType, reverse, includeFailed bool) []string {
	prefix := TaskIdIndexKey(0)
	keys, err := this.db.QueryStringKeysByPrefix([]byte(prefix))
	if err != nil {
		log.Errorf("get task id list query key %s err %s", prefix, err)
		return nil
	}
	infos := make(TaskInfos, 0)
	for _, k := range keys {
		buf, err := this.db.Get([]byte(k))
		if err != nil && err != leveldb.ErrNotFound {
			log.Errorf("get err %s", err)
			continue
		}
		if len(buf) == 0 {
			log.Errorf("get buf is 0 %s", k)
			continue
		}
		id := string(buf)
		info, err := this.GetTaskInfo(id)
		if err != nil || info == nil {
			log.Warnf("get file info of id %s failed", id)
			continue
		}

		if !allType {
			if info.Type != ft {
				continue
			}
			if info.TaskState == uint32(TaskStateDone) {
				continue
			}
		}
		if !includeFailed && info.TaskState == uint32(TaskStateFailed) {
			continue
		}
		infos = append(infos, info)
	}
	if allType {
		sort.Sort(sort.Reverse(infos))
	} else {
		sort.Sort(TaskInfosByCreatedAt(infos))
	}

	end := offset + limit
	if limit == 0 {
		end = uint32(len(infos))
	}
	infos = infos[offset:end]
	ids := make([]string, 0)
	for _, info := range infos {
		ids = append(ids, info.Id)
	}
	return ids
}

func (this *TaskDB) SaveFileInfoId(key, id string) error {
	return this.db.Put([]byte(TaskInfoIdWithFile(key)), []byte(id))
}

func (this *TaskDB) GetFileInfoId(key string) (string, error) {
	id, err := this.db.Get([]byte(TaskInfoIdWithFile(key)))
	if err != nil {
		return "", err
	}
	return string(id), nil
}

func (this *TaskDB) GetDownloadedTaskId(fileHashStr string) (string, error) {
	prefix := TaskInfoKey("")
	keys, err := this.db.QueryStringKeysByPrefix([]byte(prefix))
	if err != nil {
		log.Errorf("query task failed %s", err)
		return "", err
	}
	for _, key := range keys {
		info, _ := this.getTaskInfoByKey(key)
		if info == nil {
			continue
		}
		if info.Type != TaskTypeDownload {
			continue
		}
		if info.TaskState != uint32(TaskStateDone) {
			continue
		}
		if info.FileHash != fileHashStr {
			continue
		}
		return info.Id, nil
	}
	return "", fmt.Errorf("no downloaded task for file %s", fileHashStr)
}

func (this *TaskDB) DeleteFileInfoId(key string) error {
	return this.db.Delete([]byte(key))
}

func (this *TaskDB) SetFileName(id string, fileName string) error {

	fi, err := this.GetTaskInfo(id)
	if err != nil {
		return err
	}
	if fi == nil {
		return fmt.Errorf("fileinfo not found of %s", id)
	}
	fi.FileName = fileName
	return this.batchSaveTaskInfo(nil, fi)
}

func (this *TaskDB) DeleteTaskIds(ids []string) error {
	batch := this.db.NewBatch()
	for _, id := range ids {
		taskInfo, err := this.GetTaskInfo(id)
		if err != nil || taskInfo == nil {
			continue
		}
		key := TaskIdIndexKey(taskInfo.Index)
		this.db.BatchDelete(batch, []byte(key))
	}
	return this.db.BatchCommit(batch)
}

// DeleteTaskInfo. delete file info from db
func (this *TaskDB) DeleteTaskInfo(id string) error {
	taskCount, err := this.getTaskCount()
	if err != nil {
		return err
	}
	batch := this.db.NewBatch()
	// delete session
	countKey := []byte(FileSessionCountKey(id))
	data, err := this.db.Get(countKey)
	if err != nil && err != leveldb.ErrNotFound {
		return err
	}
	if len(data) > 0 {
		count, err := strconv.ParseInt(string(data), 10, 64)
		if err != nil {
			return err
		}
		for i := 0; i < int(count); i++ {
			sessionKey := FileSessionKey(id, i)
			this.db.BatchDelete(batch, []byte(sessionKey))
		}
		this.db.BatchDelete(batch, countKey)
	}
	fi, _ := this.GetTaskInfo(id)
	if fi != nil {
		// delete undone list
		this.RemoveFromUndoneList(batch, id, fi.Type)
	}
	// delete blocks
	err = this.batchDeleteBlocks(batch, fi)
	if err != nil {
		return err
	}

	// delete progress
	err = this.batchDeleteProgress(batch, id)
	if err != nil {
		return err
	}

	// delete from un salved list
	if fi != nil {
		err = this.RemoveFromUnSalvedList(batch, id, fi.Type)
		if err != nil {
			return err
		}
	}

	// delete options
	optionKey := FileOptionsKey(id)
	this.db.BatchDelete(batch, []byte(optionKey))

	// delete task id index
	if fi != nil {
		this.db.BatchDelete(batch, []byte(TaskIdIndexKey(fi.Index)))
		// delete task count
		taskCount.TotalCount--
		switch fi.Type {
		case TaskTypeUpload:
			taskCount.UploadCount--
		case TaskTypeDownload:
			taskCount.DownloadCount--
		case TaskTypeShare:
			taskCount.ShareCount--
		}
		err = this.batchSaveTaskCount(batch, taskCount)
		if err != nil {
			return err
		}
		// delete file info id
		if fi.Type == TaskTypeUpload {
			if len(fi.FilePath) > 0 {
				hexStr := utils.StringToSha256Hex(fi.FilePath)
				taskIdWithFilekey := TaskIdWithFile(hexStr, fi.WalletAddress, fi.Type)
				log.Debugf("will delete taskIdWithFilekey: %s", TaskInfoIdWithFile(taskIdWithFilekey))
				this.db.BatchDelete(batch, []byte(TaskInfoIdWithFile(taskIdWithFilekey)))
			}
			if len(fi.SimpleChecksum) > 0 {
				taskIdWithFilekey := TaskIdWithFile(fi.SimpleChecksum, fi.WalletAddress, fi.Type)
				log.Debugf("will delete taskIdWithFilekey: %s", TaskInfoIdWithFile(taskIdWithFilekey))
				this.db.BatchDelete(batch, []byte(TaskInfoIdWithFile(taskIdWithFilekey)))
			}
		}

		// delete unpaid info
		if fi.Type == TaskTypeDownload {
			unpaidKeys, _ := this.db.QueryStringKeysByPrefix([]byte(FileUnpaidQueryKey(id)))
			for _, unpaidKey := range unpaidKeys {
				this.db.BatchDelete(batch, []byte(unpaidKey))
			}
		}
		taskIdWithFilekey := TaskIdWithFile(fi.FileHash, fi.WalletAddress, fi.Type)
		log.Debugf("delete local file info key %s", TaskInfoIdWithFile(taskIdWithFilekey))
		this.db.BatchDelete(batch, []byte(TaskInfoIdWithFile(taskIdWithFilekey)))
	}
	// delete fileInfo
	this.db.BatchDelete(batch, []byte(TaskInfoKey(id)))
	// commit
	return this.db.BatchCommit(batch)
}

func (this *TaskDB) SetBlocksUploaded(id, nodeAddr string, blockInfos []*BlockInfo) error {
	this.lock.Lock()
	defer this.lock.Unlock()
	fi, err := this.GetTaskInfo(id)
	if err != nil {
		log.Errorf("get info err %s", err)
		return err
	}
	if fi == nil {
		log.Errorf("file info not found %d", id)
		return errors.New("file info not found")
	}
	batch := this.db.NewBatch()
	// save upload progress info
	progressKey := FileProgressKey(fi.Id, nodeAddr)
	progress, _ := this.getProgressInfo(progressKey)
	if progress == nil {
		progress = &FileProgress{
			TaskId:       fi.Id,
			NodeHostAddr: nodeAddr,
			CreatedAt:    utils.GetMilliSecTimestamp(),
		}
	}
	for _, bi := range blockInfos {
		index := bi.Index
		blockHashStr := bi.Hash
		// save block info
		blockKey := BlockInfoKey(id, index, blockHashStr)
		block, _ := this.getBlockInfo(blockKey)
		if block == nil {
			block = &BlockInfo{
				TaskId:   id,
				FileHash: fi.FileHash,
				Hash:     blockHashStr,
				Index:    index,
				NodeList: make([]string, 0),
				ReqTimes: make(map[string]uint32),
			}
		}
		reqTime := block.ReqTimes[nodeAddr]
		if reqTime > 0 {
			block.ReqTimes[nodeAddr] = reqTime + 1
			log.Debugf("the node has request this block: %s, times: %d", blockHashStr, reqTime)
			blockBuf, err := json.Marshal(block)
			if err != nil {
				return err
			}
			err = this.db.Put([]byte(blockKey), blockBuf)
			if err != nil {
				return err
			}
			continue
		}
		block.ReqTimes[nodeAddr] = reqTime + 1
		block.NodeList = append(block.NodeList, nodeAddr)
		offset := bi.DataOffset
		if block.DataOffset < offset {
			block.DataOffset = offset
		}
		if block.DataSize < bi.DataSize {
			block.DataSize = bi.DataSize
		}
		blockBuf, err := json.Marshal(block)
		if err != nil {
			return err
		}
		if progress.Progress == fi.TotalBlockCount && fi.TotalBlockCount > 0 {
			// has done
			log.Debugf("block has added: %d, %v, %v, %s", progress.Progress, fi.TotalBlockCount, nodeAddr)
			return nil
		}
		progress.Progress++
		fi.CurrentBlock = blockHashStr
		fi.CurrentIndex = index
		this.db.BatchPut(batch, []byte(blockKey), blockBuf)
	}
	if len(blockInfos) > 0 {
		log.Debugf("set offset for %s-%d-%d to %s-%d-%d", fi.FileHash, blockInfos[0].Index, blockInfos[0].DataOffset,
			blockInfos[len(blockInfos)-1].Index, blockInfos[len(blockInfos)-1].DataOffset)
	}
	log.Debugf("%s, nodeAddr %s increase progress %v", fi.Id, nodeAddr, progress)
	progressBuf, err := json.Marshal(progress)
	if err != nil {
		return err
	}
	this.db.BatchPut(batch, []byte(progressKey), progressBuf)
	fiBuf, err := json.Marshal(fi)
	if err != nil {
		return err
	}
	this.db.BatchPut(batch, []byte(TaskInfoKey(fi.Id)), fiBuf)
	return this.db.BatchCommit(batch)
}

// UpdateTaskPeerProgress. increase count of progress for a peer
func (this *TaskDB) UpdateTaskPeerProgress(id, nodeAddr string, count uint64) error {
	this.lock.Lock()
	defer this.lock.Unlock()
	batch := this.db.NewBatch()
	// save upload progress info
	progressKey := FileProgressKey(id, nodeAddr)
	progress, _ := this.getProgressInfo(progressKey)
	if progress == nil {
		progress = &FileProgress{
			TaskId:       id,
			NodeHostAddr: nodeAddr,
			CreatedAt:    utils.GetMilliSecTimestamp(),
		}
	}
	progress.Progress = count
	progress.UpdatedAt = utils.GetMilliSecTimestamp()
	log.Debugf("%s, nodeAddr %s progress %v", id, nodeAddr, progress)
	progressBuf, err := json.Marshal(progress)
	if err != nil {
		return err
	}
	this.db.BatchPut(batch, []byte(progressKey), progressBuf)
	return this.db.BatchCommit(batch)
}

// UpdateTaskPeerSpeed. update speed progress for a peer
func (this *TaskDB) UpdateTaskPeerSpeed(id, nodeAddr string, speed uint64) error {
	this.lock.Lock()
	defer this.lock.Unlock()
	batch := this.db.NewBatch()
	// save upload progress info
	progressKey := FileProgressKey(id, nodeAddr)
	progress, _ := this.getProgressInfo(progressKey)
	if progress == nil {
		progress = &FileProgress{
			TaskId:       id,
			NodeHostAddr: nodeAddr,
			CreatedAt:    utils.GetMilliSecTimestamp(),
		}
	}
	if progress.Speeds == nil {
		progress.Speeds = make([]uint64, 0)
	}
	progress.Speeds = append(progress.Speeds, speed)
	if len(progress.Speeds) > common.PROGRESS_SPEED_LEN {
		progress.Speeds = progress.Speeds[len(progress.Speeds)-common.PROGRESS_SPEED_LEN:]
	}
	progress.UpdatedAt = utils.GetMilliSecTimestamp()
	log.Debugf("%s, nodeAddr %s progress %v, speed %v", id, nodeAddr, progress, progress.Speeds)
	progressBuf, err := json.Marshal(progress)
	if err != nil {
		return err
	}
	this.db.BatchPut(batch, []byte(progressKey), progressBuf)
	return this.db.BatchCommit(batch)
}

// GetTaskPeerProgress. get progress for a peer
func (this *TaskDB) GetTaskPeerProgress(id, nodeAddr string) *FileProgress {
	this.lock.Lock()
	defer this.lock.Unlock()
	// save upload progress info
	progressKey := FileProgressKey(id, nodeAddr)
	progress, _ := this.getProgressInfo(progressKey)
	return progress
}

// GetCurrentSetBlock.
func (this *TaskDB) GetCurrentSetBlock(id string) (string, uint64, error) {
	fi, err := this.GetTaskInfo(id)
	if err != nil || fi == nil {
		return "", 0, fmt.Errorf("get file info not found: %s", id)
	}
	return fi.CurrentBlock, fi.CurrentIndex, nil
}

func (this *TaskDB) GetBlockOffset(id, blockHash string, index uint64) (uint64, error) {
	if index == 0 {
		return 0, nil
	}
	block, err := this.getBlockInfo(BlockInfoKey(id, index, blockHash))
	if err != nil {
		log.Errorf("get block info err %s", err)
		return 0, err
	}
	if block == nil {
		return 0, fmt.Errorf("block %s index %d offset not found", blockHash, index)
	}
	log.Debugf("blockKey %s, get block offset %d size %d of %d", BlockInfoKey(id, index, blockHash), block.DataOffset, block.DataSize, index)
	return block.DataOffset, nil
}

func (this *TaskDB) IsFileUploaded(id string, isDispatched bool) bool {
	this.lock.RLock()
	defer this.lock.RUnlock()
	fi, err := this.GetTaskInfo(id)
	if err != nil || fi == nil {
		log.Errorf("query upload progress keys failed, file info not found %s", err)
		return false
	}
	progressPrefix := FileProgressKey(fi.Id, "")
	keys, err := this.db.QueryStringKeysByPrefix([]byte(progressPrefix))
	if err != nil {
		log.Errorf("query upload progress keys failed %s", err)
		return false
	}
	sum := uint64(0)
	for _, key := range keys {
		progress, _ := this.getProgressInfo(key)
		if progress == nil {
			continue
		}
		sum += progress.Progress
	}
	log.Debugf("check is file %s uploaded total block %d, progress sum %d, copyNum: %d, is dispatched %t",
		fi.FileHash, fi.TotalBlockCount, sum, fi.CopyNum, isDispatched)
	if !isDispatched {
		return fi.TotalBlockCount > 0 && fi.TotalBlockCount == sum
	}
	return fi.TotalBlockCount > 0 && fi.TotalBlockCount*uint64(fi.CopyNum) == sum
}

// IsBlockUploaded. check if a block is uploaded
func (this *TaskDB) IsBlockUploaded(id, blockHashStr, nodeAddr string, index uint64) bool {
	blockKey := BlockInfoKey(id, index, blockHashStr)
	block, err := this.getBlockInfo(blockKey)
	if block == nil || err != nil {
		return false
	}
	for _, addr := range block.NodeList {
		reqTime := block.ReqTimes[nodeAddr]
		if nodeAddr == addr && reqTime > common.MAX_SAME_UPLOAD_BLOCK_NUM {
			return true
		}
	}
	return false
}

// GetUploadedBlockNodeList. get uploaded block nodelist
func (this *TaskDB) GetUploadedBlockNodeList(id, blockHashStr string, index uint64) []string {
	blockKey := BlockInfoKey(id, index, blockHashStr)
	block, err := this.getBlockInfo(blockKey)
	if block == nil || err != nil {
		return nil
	}
	return block.NodeList
}

// AddFileBlockHashes add all blocks' hash, using for detect whether the node has stored the file
func (this *TaskDB) AddFileBlockHashes(id string, blocks []string) error {
	// TODO: test performance
	batch := this.db.NewBatch()
	for index, hash := range blocks {
		key := BlockInfoKey(id, uint64(index), hash)
		info := &BlockInfo{
			TaskId: id,
			Hash:   hash,
			Index:  uint64(index),
		}
		buf, err := json.Marshal(info)
		if err != nil {
			return err
		}
		this.db.BatchPut(batch, []byte(key), buf)
	}
	return this.db.BatchCommit(batch)
}

func (this *TaskDB) AddFileUnpaid(id, recipientWalAddr string, paymentId, asset int32, amount uint64) error {
	this.lock.Lock()
	defer this.lock.Unlock()
	unpaidKey := FileUnpaidKey(id, recipientWalAddr, asset)
	info, err := this.getFileUnpaidInfo(unpaidKey)
	if err != nil {
		log.Errorf("getFileUnpaidInfo err %s", err)
		return err
	}
	if info == nil {
		info = &FileDownloadUnPaid{
			TaskId:   id,
			Payments: make(map[int32]*Payment, 0),
		}
	}

	if p, ok := info.Payments[paymentId]; ok {
		p.Amount += amount
		info.Payments[paymentId] = p
		log.Debugf("add file unpaid %s taskId: %s, sender:%s, amount: %d, remain: %d", unpaidKey, id, recipientWalAddr, amount, p.Amount)
	} else {
		p := &Payment{
			PaymentId:     paymentId,
			WalletAddress: recipientWalAddr,
			Asset:         asset,
			Amount:        amount,
		}
		info.Payments[paymentId] = p
		log.Debugf("add file unpaid %s taskId: %s, sender:%s, amount: %d, remain: %d", unpaidKey, id, recipientWalAddr, amount, amount)
	}
	batch := this.db.NewBatch()
	this.db.BatchPut(batch, []byte(TaskIdOfPaymentIDKey(paymentId)), []byte(id))
	buf, err := json.Marshal(info)
	if err != nil {
		return err
	}
	this.db.BatchPut(batch, []byte(unpaidKey), buf)
	return this.db.BatchCommit(batch)
}

// GetUnpaidPayments. get unpaid amount of task to payee
func (this *TaskDB) GetUnpaidPayments(id, payToAddress string, asset int32) (map[int32]*Payment, error) {
	this.lock.Lock()
	defer this.lock.Unlock()
	unpaidKey := FileUnpaidKey(id, payToAddress, asset)
	info, err := this.getFileUnpaidInfo(unpaidKey)
	if err != nil {
		log.Errorf("getFileUnpaidInfo err %s", err)
		return nil, err
	}
	if info == nil {
		return nil, nil
	}
	return info.Payments, nil
}

func (this *TaskDB) DeleteFileUnpaid(id, payToAddress string, paymentId, asset int32, amount uint64) error {
	this.lock.Lock()
	defer this.lock.Unlock()
	unpaidKey := FileUnpaidKey(id, payToAddress, asset)
	info, err := this.getFileUnpaidInfo(unpaidKey)
	if err != nil {
		log.Debug("getFileUnpaidInfo err %s", err)
		return err
	}
	if info == nil {
		return fmt.Errorf("can't find file info of id %s, unpaidkey %s", id, unpaidKey)
	}
	payment, ok := info.Payments[paymentId]
	if !ok {
		return fmt.Errorf("can't find file info of paymentId %d, unpaidkey %s", paymentId, unpaidKey)
	}
	if payment.Amount > amount {
		payment.Amount = payment.Amount - amount
		info.Payments[paymentId] = payment
		log.Debugf("delete file unpaid %s, taskId: %s, sender:%s, amount: %d, remain: %d", unpaidKey, id, payToAddress, amount, payment.Amount)
		buf, err := json.Marshal(info)
		if err != nil {
			return err
		}
		return this.db.Put([]byte(unpaidKey), buf)
	}
	delete(info.Payments, paymentId)
	log.Debugf("delete file unpaid %s, taskId: %s, sender:%s, amount: %d, paymentId: %d", unpaidKey, id, payToAddress, amount, paymentId)
	batch := this.db.NewBatch()
	this.db.BatchDelete(batch, []byte(TaskIdOfPaymentIDKey(paymentId)))
	this.db.BatchDelete(batch, []byte(unpaidKey))
	return this.db.BatchCommit(batch)
}

// IsTaskInfoExist return a file is exist or not
func (this *TaskDB) IsTaskInfoExist(id string) bool {
	fi, err := this.GetTaskInfo(id)
	if err != nil || fi == nil {
		return false
	}
	return true
}

// FileBlockHashes. return file block hashes
func (this *TaskDB) FileBlockHashes(id string) []string {
	fi, err := this.GetTaskInfo(id)
	if err != nil || fi == nil {
		return nil
	}
	hashes := make([]string, 0, fi.TotalBlockCount)
	for i := uint64(0); i < fi.TotalBlockCount; i++ {
		prefix := BlockInfoKey(id, i, "")
		keys, err := this.db.QueryStringKeysByPrefix([]byte(prefix))
		if len(keys) != 1 || err != nil {
			return nil
		}
		str := keys[0]
		items := strings.Split(str, "_")
		hashes = append(hashes, items[len(items)-1])
	}
	return hashes
}

// FileProgress. return each node count progress
func (this *TaskDB) FileProgress(id string) map[string]FileProgress {
	prefix := FileProgressKey(id, "")
	keys, err := this.db.QueryStringKeysByPrefix([]byte(prefix))
	if err != nil {
		return nil
	}
	m := make(map[string]FileProgress)
	for _, key := range keys {
		progress, err := this.getProgressInfo(key)
		if err != nil || progress == nil {
			continue
		}
		m[progress.NodeHostAddr] = *progress
	}
	return m
}

//  SetBlockStored set the flag of store state
func (this *TaskDB) SetBlockDownloaded(id, blockHashStr, nodeAddr string, index uint64, offset int64, links []string) error {
	this.lock.Lock()
	defer this.lock.Unlock()
	blockKey := BlockInfoKey(id, index, blockHashStr)
	block, err := this.getBlockInfo(blockKey)
	if block == nil || err != nil {
		block = &BlockInfo{
			NodeList:   make([]string, 0),
			LinkHashes: make([]string, 0),
		}
	}
	if block.ReqTimes == nil {
		block.ReqTimes = make(map[string]uint32)
	}
	count := block.ReqTimes[nodeAddr]
	block.TaskId = id
	block.Hash = blockHashStr
	block.Index = index
	block.DataOffset = uint64(offset)
	block.NodeList = append(block.NodeList, nodeAddr)
	block.LinkHashes = append(block.LinkHashes, links...)
	block.ReqTimes[nodeAddr] = count + 1

	blockBuf, err := json.Marshal(block)
	if err != nil {
		return err
	}

	if count > 0 {
		log.Warnf("set a downloaded block to db task %s, %s-%d", id, blockHashStr, index)
		return this.db.Put([]byte(blockKey), blockBuf)
	}

	progressKey := FileProgressKey(id, nodeAddr)
	progress, err := this.getProgressInfo(progressKey)
	if progress == nil || err != nil {
		progress = &FileProgress{
			TaskId:       id,
			NodeHostAddr: nodeAddr,
		}
	}
	progress.Progress++
	progressBuf, err := json.Marshal(progress)
	if err != nil {
		return err
	}

	fi, err := this.GetTaskInfo(id)
	if err != nil {
		return err
	}
	fi.CurrentBlock = blockHashStr
	fi.CurrentIndex = index
	fiBuf, err := json.Marshal(fi)
	if err != nil {
		return err
	}
	batch := this.db.NewBatch()
	log.Debugf("set block %s, len %d", blockKey, len(blockBuf))
	this.db.BatchPut(batch, []byte(blockKey), blockBuf)
	this.db.BatchPut(batch, []byte(progressKey), progressBuf)
	this.db.BatchPut(batch, []byte(TaskInfoKey(id)), fiBuf)
	return this.db.BatchCommit(batch)
}

//  IsBlockDownloaded
func (this *TaskDB) IsBlockDownloaded(id, blockHashStr string, index uint64) bool {
	blockKey := BlockInfoKey(id, index, blockHashStr)
	block, err := this.getBlockInfo(blockKey)
	if block == nil || err != nil {
		return false
	}
	if len(block.NodeList) == 0 {
		return false
	}
	return true
}

// IsFileDownloaded check if a downloaded file task has finished storing all blocks
func (this *TaskDB) IsFileDownloaded(id string) bool {
	fi, err := this.GetTaskInfo(id)
	if err != nil || fi == nil {
		log.Errorf("query download progress keys failed, file info %s not found %s", id, err)
		return false
	}
	progressPrefix := FileProgressKey(fi.Id, "")
	keys, err := this.db.QueryStringKeysByPrefix([]byte(progressPrefix))
	if err != nil {
		log.Errorf("query upload progress keys failed %s", err)
		return false
	}
	sum := uint64(0)
	for _, key := range keys {
		progress, _ := this.getProgressInfo(key)
		if progress == nil {
			continue
		}
		sum += progress.Progress
	}
	return sum == fi.TotalBlockCount && fi.TotalBlockCount > 0
}

// GetUndownloadedBlockInfo. check undownloaded block in-order
func (this *TaskDB) GetUndownloadedBlockInfo(id, rootBlockHash string) ([]string, map[string]uint64, error) {
	fi, err := this.GetTaskInfo(id)
	if err != nil || fi == nil {
		return nil, nil, errors.New("file not found")
	}
	blockHashes := this.FileBlockHashes(id)
	if len(blockHashes) == 0 {
		return nil, nil, nil
	}
	hashes := make([]string, 0)
	indexMap := make(map[string]uint64)
	for index, hash := range blockHashes {
		if this.IsBlockDownloaded(id, hash, uint64(index)) {
			continue
		}
		hashes = append(hashes, hash)
		indexMap[hash] = uint64(index)
	}
	return hashes, indexMap, nil
}

func (this *TaskDB) RemoveFromUndoneList(batch *leveldb.Batch, id string, ft TaskType) error {
	var list []string
	var undoneKey string
	switch ft {
	case TaskTypeUpload:
		undoneKey = FileUploadUndoneKey()
	case TaskTypeDownload:
		undoneKey = FileDownloadUndoneKey()
	case TaskTypeShare:
		return nil
	}
	log.Debugf("remove undone file id: %s, key %s", id, undoneKey)
	data, err := this.db.Get([]byte(undoneKey))
	if err != nil && err != leveldb.ErrNotFound {
		return err
	}
	if len(data) == 0 {
		return nil
	}
	err = json.Unmarshal(data, &list)
	if err != nil {
		return err
	}
	for i, v := range list {
		if id == v {
			list = append(list[:i], list[i+1:]...)
			break
		}
	}
	newData, err := json.Marshal(list)
	if err != nil {
		return err
	}
	if batch == nil {
		return this.db.Put([]byte(undoneKey), newData)
	} else {
		this.db.BatchPut(batch, []byte(undoneKey), newData)
		return nil
	}
}

func (this *TaskDB) UndoneList(ft TaskType) ([]string, error) {
	var list []string
	var undoneKey string
	switch ft {
	case TaskTypeUpload:
		undoneKey = FileUploadUndoneKey()
	case TaskTypeDownload:
		undoneKey = FileDownloadUndoneKey()
	case TaskTypeShare:
		return nil, nil
	}
	data, err := this.db.Get([]byte(undoneKey))
	if err != nil && err != leveldb.ErrNotFound {
		return nil, err
	}
	if len(data) == 0 {
		return nil, nil
	}
	err = json.Unmarshal(data, &list)
	if err != nil {
		return nil, err
	}
	return list, nil
}

func (this *TaskDB) SetUploadProgressDone(id, nodeAddr string) error {
	this.lock.Lock()
	defer this.lock.Unlock()
	log.Debugf("SetUploadProgressDone :%s, addr: %s", id, nodeAddr)
	fi, err := this.GetTaskInfo(id)
	if err != nil {
		log.Errorf("get info err %s", err)
		return err
	}
	if fi == nil {
		log.Errorf("file info not found %d", id)
		return errors.New("file info not found")
	}
	// save upload progress info
	progressKey := FileProgressKey(fi.Id, nodeAddr)
	progress, _ := this.getProgressInfo(progressKey)
	if progress == nil {
		progress = &FileProgress{
			TaskId:       fi.Id,
			NodeHostAddr: nodeAddr,
		}
	}
	log.Debugf("save upload progress done before: %v", progress.Progress)
	progress.Progress = fi.TotalBlockCount
	progressBuf, err := json.Marshal(progress)
	if err != nil {
		return err
	}
	// TODO: split save block count for each node
	log.Debugf("save upload progress done after %v", progress.Progress)
	fiBuf, err := json.Marshal(fi)
	if err != nil {
		return err
	}
	batch := this.db.NewBatch()
	this.db.BatchPut(batch, []byte(progressKey), progressBuf)
	this.db.BatchPut(batch, []byte(TaskInfoKey(fi.Id)), fiBuf)
	return this.db.BatchCommit(batch)
}

func (this *TaskDB) UpdateTaskProgress(id, nodeAddr string, prog uint64) error {
	this.lock.Lock()
	defer this.lock.Unlock()
	log.Debugf("UpdateTaskProgress :%s, addr: %s, progress %d", id, nodeAddr, prog)
	// save upload progress info
	progressKey := FileProgressKey(id, nodeAddr)
	progress, _ := this.getProgressInfo(progressKey)
	if progress == nil {
		progress = &FileProgress{
			TaskId:       id,
			NodeHostAddr: nodeAddr,
		}
	}
	log.Debugf("save  progress  before: %v %v", progress.Progress)
	progress.Progress = prog
	progressBuf, err := json.Marshal(progress)
	if err != nil {
		return err
	}
	log.Debugf("save  progress: %v %v", progress.Progress)
	return this.db.Put([]byte(progressKey), progressBuf)
}

func (this *TaskDB) UpdateTaskProgressState(id, nodeAddr string, state TaskState) error {
	this.lock.Lock()
	defer this.lock.Unlock()
	log.Debugf("UpdateTaskProgress :%s, addr: %s, state %d", id, nodeAddr, state)
	// save upload progress info
	progressKey := FileProgressKey(id, nodeAddr)
	progress, _ := this.getProgressInfo(progressKey)
	if progress == nil {
		progress = &FileProgress{
			TaskId:       id,
			NodeHostAddr: nodeAddr,
			CreatedAt:    utils.GetMilliSecTimestamp(),
		}
	}
	log.Debugf("save  progress  before: %v %v", progress.Progress)
	if state == TaskStateDoing {
		progress.TransferCount++
	}
	progress.NextUpdatedAt = utils.GetMilliSecTimestamp() + common.DISPATCH_FILE_DURATION*1000
	progress.State = state
	progress.UpdatedAt = utils.GetMilliSecTimestamp()
	progressBuf, err := json.Marshal(progress)
	if err != nil {
		return err
	}
	log.Debugf("save  progress: %v %v", progress.Progress)
	return this.db.Put([]byte(progressKey), progressBuf)
}

// IsNodeTaskDone. check if a node has done
func (this *TaskDB) IsNodeTaskDoingOrDone(id, nodeAddr string) (bool, error) {
	this.lock.RLock()
	defer this.lock.RUnlock()
	fi, err := this.GetTaskInfo(id)
	if err != nil || fi == nil {
		return false, err
	}
	log.Debugf("IsNodeTaskDone :%s, addr: %s,", id, nodeAddr)
	// save upload progress info
	progressKey := FileProgressKey(id, nodeAddr)
	progress, _ := this.getProgressInfo(progressKey)
	if progress == nil {
		return false, fmt.Errorf("task %s progress of node %s not found", id, nodeAddr)
	}
	return progress.Progress == fi.TotalBlockCount && fi.TotalBlockCount > 0 ||
		progress.State == TaskStateDoing, nil
}

func (this *TaskDB) SaveFileUploaded(id string) error {
	return this.RemoveFromUndoneList(nil, id, TaskTypeUpload)
}

func (this *TaskDB) SaveFileDownloaded(id string) error {
	countKey := FileDownloadedCountKey()
	countBuf, err := this.db.Get([]byte(countKey))
	if err != nil && err != leveldb.ErrNotFound {
		return err
	}
	count := uint32(0)
	if len(countBuf) != 0 {
		result, err := strconv.ParseUint(string(countBuf), 10, 32)
		if err != nil {
			return err
		}
		count = uint32(result)
	}
	err = this.RemoveFromUndoneList(nil, id, TaskTypeDownload)
	if err != nil {
		return err
	}
	fileDownloadedKey := FileDownloadedKey(count)
	batch := this.db.NewBatch()
	this.db.BatchPut(batch, []byte(countKey), []byte(fmt.Sprintf("%d", count+1)))
	this.db.BatchPut(batch, []byte(fileDownloadedKey), []byte(id))
	return this.db.BatchCommit(batch)
}

// AllDownloadFiles. get all download files from db
func (this *TaskDB) AllDownloadFiles() ([]*TaskInfo, []string, error) {
	countKey := FileDownloadedCountKey()
	countBuf, err := this.db.Get([]byte(countKey))
	if err != nil && err != leveldb.ErrNotFound {
		return nil, nil, err
	}
	if len(countBuf) == 0 {
		return nil, nil, nil
	}
	count, err := strconv.ParseUint(string(countBuf), 10, 32)
	if err != nil {
		return nil, nil, err
	}
	log.Debugf("all download files count :%v", count)
	all := make([]string, 0, count)
	existFileHash := make(map[string]struct{}, 0)
	infos := make([]*TaskInfo, 0, count)
	for i := uint32(0); i < uint32(count); i++ {
		downloadedKey := FileDownloadedKey(i)
		idBuf, err := this.db.Get([]byte(downloadedKey))
		if err != nil || len(idBuf) == 0 {
			continue
		}
		fi, err := this.GetTaskInfo(string(idBuf))
		if err != nil || fi == nil {
			continue
		}
		if len(fi.FileHash) == 0 {
			continue
		}
		if _, ok := existFileHash[fi.FileHash]; ok {
			continue
		}
		existFileHash[fi.FileHash] = struct{}{}
		all = append(all, fi.FileHash)
		infos = append(infos, fi)
	}
	log.Debugf("all different download files count %d", len(infos))
	return infos, all, nil
}

func (this *TaskDB) AddShareTo(id, walletAddress string) error {
	shareKey := FileShareToKey(id, walletAddress)
	return this.db.Put([]byte(shareKey), []byte("true"))
}

func (this *TaskDB) GetUnpaidAmount(id, walletAddress string, asset int32) (uint64, error) {
	shareKey := FileShareToKey(id, walletAddress)
	exist, err := this.db.Get([]byte(shareKey))
	if err != nil && err != leveldb.ErrNotFound {
		return 0, err
	}
	if len(exist) == 0 {
		return 0, nil
	}
	unpaid, err := this.getFileUnpaidInfo(FileUnpaidKey(id, walletAddress, asset))
	if err != nil {
		return 0, err
	}
	if unpaid == nil {
		return 0, nil
	}
	unpaidAmount := uint64(0)
	for _, p := range unpaid.Payments {
		unpaidAmount += p.Amount
	}
	return unpaidAmount, nil
}

// getFileUploadInfo. helper function, get file upload info from db. if fileinfo not found, return (nil, nil)
func (this *TaskDB) GetTaskInfo(id string) (*TaskInfo, error) {
	key := []byte(TaskInfoKey(id))
	value, err := this.db.Get(key)
	if err != nil && err != leveldb.ErrNotFound {
		return nil, err
	}
	if len(value) == 0 {
		log.Debugf("get file info value is empty %s", key)
		return nil, nil
	}

	info := &TaskInfo{}
	err = json.Unmarshal(value, info)
	if err != nil {
		return nil, err
	}
	return info, nil
}

func (this *TaskDB) getTaskInfoByKey(key string) (*TaskInfo, error) {
	value, err := this.db.Get([]byte(key))
	if err != nil && err != leveldb.ErrNotFound {
		return nil, err
	}
	if len(value) == 0 {
		log.Debugf("get file info value is empty %s", key)
		return nil, nil
	}

	info := &TaskInfo{}
	err = json.Unmarshal(value, info)
	if err != nil {
		return nil, err
	}
	return info, nil
}

func (this *TaskDB) SetFileUploadOptions(fileInfoId string, options *fs.UploadOption) error {
	buf, err := json.Marshal(options)
	if err != nil {
		return err
	}
	key := FileOptionsKey(fileInfoId)
	return this.db.Put([]byte(key), buf)
}

func (this *TaskDB) GetFileUploadOptions(fileInfoId string) (*fs.UploadOption, error) {
	key := FileOptionsKey(fileInfoId)
	value, err := this.db.Get([]byte(key))
	if err != nil && err != leveldb.ErrNotFound {
		return nil, err
	}
	if len(value) == 0 {
		return nil, nil
	}
	opt := &fs.UploadOption{}
	err = json.Unmarshal(value, opt)
	if err != nil {
		return nil, err
	}
	return opt, nil
}

func (this *TaskDB) SetFileDownloadOptions(fileInfoId string, options *common.DownloadOption) error {
	buf, err := json.Marshal(options)
	if err != nil {
		return err
	}
	key := FileOptionsKey(fileInfoId)
	return this.db.Put([]byte(key), buf)
}

func (this *TaskDB) GetFileDownloadOptions(fileInfoId string) (*common.DownloadOption, error) {
	key := FileOptionsKey(fileInfoId)
	value, err := this.db.Get([]byte(key))
	if err != nil && err != leveldb.ErrNotFound {
		return nil, err
	}
	if len(value) == 0 {
		return nil, nil
	}
	opt := &common.DownloadOption{}
	err = json.Unmarshal(value, opt)
	if err != nil {
		return nil, err
	}
	return opt, nil
}

func (this *TaskDB) AddFileSession(fileInfoId, sessionId, walletAddress, hostAddress string, asset uint32, unitPrice uint64) error {
	countKey := []byte(FileSessionCountKey(fileInfoId))
	data, err := this.db.Get(countKey)
	if err != nil && err != leveldb.ErrNotFound {
		return err
	}
	count := int(0)
	if len(data) > 0 {
		parseCount, parseErr := strconv.ParseInt(string(data), 10, 64)
		if parseErr != nil {
			return parseErr
		}
		count = int(parseCount)
	}
	sessionKey := FileSessionKey(fileInfoId, count)
	session := &Session{
		SessionId:  sessionId,
		WalletAddr: walletAddress,
		HostAddr:   hostAddress,
		Asset:      asset,
		UnitPrice:  unitPrice,
	}
	sessionBuf, err := json.Marshal(session)
	if err != nil {
		return err
	}
	batch := this.db.NewBatch()
	newCount := int(count) + 1
	this.db.BatchPut(batch, countKey, []byte(fmt.Sprintf("%d", newCount)))
	this.db.BatchPut(batch, []byte(sessionKey), sessionBuf)
	return this.db.BatchCommit(batch)
}

func (this *TaskDB) GetFileSessions(fileInfoId string) (map[string]*Session, error) {
	countKey := []byte(FileSessionCountKey(fileInfoId))
	data, err := this.db.Get(countKey)
	if err != nil && err != leveldb.ErrNotFound {
		return nil, err
	}
	if len(data) == 0 {
		return nil, nil
	}
	count, err := strconv.ParseInt(string(data), 10, 64)
	if err != nil {
		return nil, err
	}
	res := make(map[string]*Session)
	for i := 0; i < int(count); i++ {
		sessionKey := FileSessionKey(fileInfoId, i)
		sessionData, err := this.db.Get([]byte(sessionKey))
		if err != nil || len(sessionData) == 0 {
			continue
		}
		var session *Session
		err = json.Unmarshal(sessionData, &session)
		if err != nil {
			continue
		}
		res[session.HostAddr] = session
	}
	return res, nil
}

// saveFileInfo. helper function, put fileinfo to db
func (this *TaskDB) SaveTaskInfo(info *TaskInfo) error {
	return this.batchSaveTaskInfo(nil, info)
}

func (this *TaskDB) GetTaskIdWithPaymentId(paymentId int32) (string, error) {
	buf, err := this.db.Get([]byte(TaskIdOfPaymentIDKey(paymentId)))
	if err != nil && err != leveldb.ErrNotFound {
		return "", err
	}
	return string(buf), nil
}

func (this *TaskDB) RemoveFromUnSalvedList(batch *leveldb.Batch, id string, ft TaskType) error {
	var list []string
	var key string
	switch ft {
	case TaskTypeUpload:
		key = FileUploadUnSalvedKey()
	case TaskTypeDownload:
		return nil
	case TaskTypeShare:
		return nil
	}
	data, err := this.db.Get([]byte(key))
	if err != nil && err != leveldb.ErrNotFound {
		return err
	}
	if len(data) == 0 {
		return nil
	}
	err = json.Unmarshal(data, &list)
	if err != nil {
		return err
	}
	for i, v := range list {
		if id == v {
			list = append(list[:i], list[i+1:]...)
			break
		}
	}
	newData, err := json.Marshal(list)
	if err != nil {
		return err
	}
	if batch == nil {
		return this.db.Put([]byte(key), newData)
	} else {
		this.db.BatchPut(batch, []byte(key), newData)
		return nil
	}
}

func (this *TaskDB) UnSlavedList(ft TaskType) ([]string, error) {
	var list []string
	var key string
	switch ft {
	case TaskTypeUpload:
		key = FileUploadUnSalvedKey()
	case TaskTypeDownload:
		return nil, nil
	case TaskTypeShare:
		return nil, nil
	}
	data, err := this.db.Get([]byte(key))
	if err != nil && err != leveldb.ErrNotFound {
		return nil, err
	}
	if len(data) == 0 {
		return nil, nil
	}
	err = json.Unmarshal(data, &list)
	if err != nil {
		return nil, err
	}
	return list, nil
}

func (this *TaskDB) GetUploadDoneNodeAddr(id string) (string, error) {
	info, err := this.GetTaskInfo(id)
	if err != nil {
		return "", err
	}
	if info == nil {
		return "", fmt.Errorf("upload file info not found")
	}
	progressPrefix := FileProgressKey(info.Id, "")
	keys, err := this.db.QueryStringKeysByPrefix([]byte(progressPrefix))
	if err != nil {
		log.Errorf("query upload progress keys failed %s", err)
		return "", err
	}
	for _, key := range keys {
		progress, _ := this.getProgressInfo(key)
		if progress == nil {
			continue
		}
		if progress.Progress == info.TotalBlockCount {
			return progress.NodeHostAddr, nil
		}
	}

	return "", fmt.Errorf("no done node")
}

func (this *TaskDB) GetUnDispatchTaskInfos(curWalletAddr string) ([]*TaskInfo, error) {
	prefix := TaskInfoKey("")
	keys, err := this.db.QueryStringKeysByPrefix([]byte(prefix))
	if err != nil {
		return nil, err
	}
	taskInfos := make([]*TaskInfo, 0, len(keys))
	for _, key := range keys {
		info, err := this.getTaskInfoByKey(key)
		if err != nil || info == nil {
			continue
		}
		if info.Type != TaskTypeUpload {
			continue
		}
		if info.TaskState == uint32(TaskStateDone) {
			continue
		}
		if info.FileOwner == curWalletAddr {
			continue
		}
		newInfo := this.CopyTask(info)
		if newInfo == nil {
			log.Warnf("copy task %s failed", info.Id)
			continue
		}
		taskInfos = append(taskInfos, newInfo)
	}
	return taskInfos, nil
}

func (this *TaskDB) CopyTask(t *TaskInfo) *TaskInfo {
	data, err := json.Marshal(t)
	if err != nil {
		return nil
	}
	newInfo := &TaskInfo{}
	if err := json.Unmarshal(data, &newInfo); err != nil {
		return nil
	}
	return newInfo
}

// GetFileNameWithPath. Query all task to get file name whit a specific file path
func (this *TaskDB) GetFileNameWithPath(filePath string) string {
	if len(filePath) == 0 {
		return ""
	}
	prefix := TaskInfoKey("")
	keys, err := this.db.QueryStringKeysByPrefix([]byte(prefix))
	if err != nil {
		return ""
	}
	for _, key := range keys {
		info, err := this.getTaskInfoByKey(key)
		if err != nil || info == nil {
			continue
		}
		if info.FilePath != filePath {
			return ""
		}
		return info.FileName
	}
	return ""
}

// GetUploadTaskInfos. get all upload task info, sort by updated at
func (this *TaskDB) GetUploadTaskInfos() ([]*TaskInfo, error) {
	infos := make(TaskInfos, 0)
	prefix := TaskInfoKey("")
	keys, err := this.db.QueryStringKeysByPrefix([]byte(prefix))
	if err != nil {
		return nil, err
	}
	for _, key := range keys {
		info, err := this.getTaskInfoByKey(key)
		if err != nil || info == nil {
			continue
		}
		if info.Type != TaskTypeUpload {
			continue
		}
		infos = append(infos, info)
	}
	sort.Sort(infos)
	return infos, nil
}

func (this *TaskDB) batchAddToUndoneList(batch *leveldb.Batch, id string, ft TaskType) error {
	var list []string
	var undoneKey string
	switch ft {
	case TaskTypeUpload:
		undoneKey = FileUploadUndoneKey()
	case TaskTypeDownload:
		undoneKey = FileDownloadUndoneKey()
	case TaskTypeShare:
		return nil
	}
	data, err := this.db.Get([]byte(undoneKey))
	if err != nil && err != leveldb.ErrNotFound {
		return err
	}
	if len(data) == 0 {
		list = make([]string, 0)
		list = append(list, id)
		data, err := json.Marshal(list)
		if err != nil {
			return err
		}
		this.db.BatchPut(batch, []byte(undoneKey), data)
		return nil
	}
	err = json.Unmarshal(data, &list)
	if err != nil {
		return err
	}
	for _, v := range list {
		if id == v {
			return nil
		}
	}
	list = append(list, id)
	newData, err := json.Marshal(list)
	if err != nil {
		return err
	}
	this.db.BatchPut(batch, []byte(undoneKey), newData)
	return nil
}

func (this *TaskDB) batchAddToUnSlavedList(batch *leveldb.Batch, id string, ft TaskType) error {
	var key string
	switch ft {
	case TaskTypeUpload:
		key = FileUploadUnSalvedKey()
	case TaskTypeDownload:
		return nil
	case TaskTypeShare:
		return nil
	}
	data, err := this.db.Get([]byte(key))
	if err != nil && err != leveldb.ErrNotFound {
		return err
	}
	newData, err := appendToStringSlice(data, id)
	if err != nil {
		return err
	}
	if newData == nil {
		return nil
	}
	this.db.BatchPut(batch, []byte(key), newData)
	return nil
}

func appendToStringSlice(data []byte, value string) ([]byte, error) {
	list := make([]string, 0)
	if len(data) == 0 {
		list = append(list, value)
		newData, err := json.Marshal(list)
		if err != nil {
			return nil, err
		}
		return newData, nil
	}
	err := json.Unmarshal(data, &list)
	if err != nil {
		return nil, err
	}
	for _, v := range list {
		if value == v {
			return nil, nil
		}
	}
	list = append(list, value)
	newData, err := json.Marshal(list)
	if err != nil {
		return nil, err
	}
	return newData, nil
}

func (this *TaskDB) batchSaveTaskInfo(batch *leveldb.Batch, info *TaskInfo) error {
	info.UpdatedAt = utils.GetMilliSecTimestamp()
	key := []byte(TaskInfoKey(info.Id))
	buf, err := json.Marshal(info)
	if err != nil {
		return err
	}
	if batch == nil {
		return this.db.Put(key, buf)
	}
	this.db.BatchPut(batch, key, buf)
	return nil
}

func (this *TaskDB) batchSaveTaskCount(batch *leveldb.Batch, taskCount *TaskCount) error {
	data, err := json.Marshal(taskCount)
	if err != nil {
		return err
	}
	if batch == nil {
		return this.db.Put([]byte(TaskCountKey()), data)
	}
	this.db.BatchPut(batch, []byte(TaskCountKey()), data)
	return nil
}

// getBlockInfo. helper function, get file upload info from db. if fileinfo not found, return (nil, nil)
func (this *TaskDB) getBlockInfo(key string) (*BlockInfo, error) {
	value, err := this.db.Get([]byte(key))
	if err != nil && err != leveldb.ErrNotFound {
		return nil, err
	}
	if len(value) == 0 {
		return nil, nil
	}
	info := &BlockInfo{}
	err = json.Unmarshal(value, info)
	if err != nil {
		return nil, err
	}
	return info, nil
}

func (this *TaskDB) saveBlockInfo(key string, info *BlockInfo) error {
	buf, err := json.Marshal(info)
	if err != nil {
		return err
	}
	return this.db.Put([]byte(key), buf)
}

func (this *TaskDB) getProgressInfo(key string) (*FileProgress, error) {
	value, err := this.db.Get([]byte(key))
	if err != nil && err != leveldb.ErrNotFound {
		return nil, err
	}
	if len(value) == 0 {
		return nil, nil
	}
	info := &FileProgress{}
	err = json.Unmarshal(value, info)
	if err != nil {
		return nil, err
	}
	return info, nil
}

func (this *TaskDB) saveProgress(key []byte, info *FileProgress) error {
	buf, err := json.Marshal(info)
	if err != nil {
		return err
	}
	return this.db.Put(key, buf)
}

func (this *TaskDB) getFileUnpaidInfo(key string) (*FileDownloadUnPaid, error) {
	value, err := this.db.Get([]byte(key))
	if err != nil && err != leveldb.ErrNotFound {
		return nil, err
	}
	if len(value) == 0 {
		// log.Warnf("get file unpaid info len is 0 %s", key)
		return nil, nil
	}
	info := &FileDownloadUnPaid{}
	err = json.Unmarshal(value, info)
	if err != nil {
		return nil, err
	}
	return info, nil
}

func (this *TaskDB) batchDeleteBlocks(batch *leveldb.Batch, fi *TaskInfo) error {
	if fi == nil {
		return nil
	}
	for i := uint64(0); i < fi.TotalBlockCount; i++ {
		prefix := BlockInfoKey(fi.Id, i, "")
		keys, err := this.db.QueryStringKeysByPrefix([]byte(prefix))
		if len(keys) != 1 || err != nil {
			return nil
		}
		str := keys[0]
		if batch != nil {
			this.db.BatchDelete(batch, []byte(str))
			continue
		}
		err = this.db.Delete([]byte(str))
		if err != nil {
			return err
		}
	}
	return nil
}

func (this *TaskDB) batchDeleteProgress(batch *leveldb.Batch, id string) error {
	prefix := FileProgressKey(id, "")
	keys, err := this.db.QueryStringKeysByPrefix([]byte(prefix))
	if err != nil {
		return nil
	}
	for _, key := range keys {
		if batch != nil {
			this.db.BatchDelete(batch, []byte(key))
			continue
		}
		err = this.db.Delete([]byte(key))
		if err != nil {
			return err
		}
	}
	return nil
}
