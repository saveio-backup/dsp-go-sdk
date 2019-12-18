package store

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/saveio/dsp-go-sdk/common"
	"github.com/saveio/dsp-go-sdk/utils"
	"github.com/saveio/themis/common/log"
	"github.com/syndtr/goleveldb/leveldb"

	fs "github.com/saveio/themis/smartcontract/service/native/savefs"
)

// FileDB. implement a db storage for save information of sending/downloading/downloaded files
type FileDB struct {
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
	FileInfoId string            `json:"file_info_id"`
	FileHash   string            `json:"file_hash"`
	Hash       string            `json:"hash"`                  // block  hash
	Index      uint32            `json:"index"`                 // block index of file
	DataOffset uint64            `json:"data_offset"`           // block raw data offset
	DataSize   uint64            `json:"data_size"`             // block data size
	NodeList   []string          `json:"node_list,omitempty"`   // uploaded node list
	ReqTimes   map[string]uint64 `json:"block_req_times"`       // record block request times for peer
	LinkHashes []string          `json:"link_hashes,omitempty"` // child link hashes slice
}

type Payment struct {
	WalletAddress string `json:"wallet_address"`
	Asset         int32  `json:"asset"`
	Amount        uint64 `json:"amount"`
	PaymentId     int32  `json:"paymentId"`
}

const (
	FILEINFO_FIELD_FILENAME int = iota
	FILEINFO_FIELD_FILEHASH
	FILEINFO_FIELD_STORETX
	FILEINFO_FIELD_WHITELISTTX
	FILEINFO_FIELD_PROVE_PRIVATEKEY
	FILEINFO_FIELD_PREFIX
	FILEINFO_FIELD_TOTALCOUNT
	FILEINFO_FIELD_COPYNUM
	FILEINFO_FIELD_URL
	FILEINFO_FIELD_LINK
	FILEINFO_FIELD_FILEPATH
	FILEINFO_FIELD_WALLETADDR
	FILEINFO_FIELD_REGURL_TX
	FILEINFO_FIELD_BIND_TX
	FILEINFO_FIELD_TASKSTATE
	FILEINFO_FIELD_OWNER
)

// fileInfo keep all blocks infomation and the prove private key for generating tags
type TaskInfo struct {
	Id                string            `json:"id"`                     // task id
	Index             uint32            `json:"index"`                  // task index
	FileHash          string            `json:"file_hash"`              // file hash
	FileName          string            `json:"file_name"`              // file name
	FilePath          string            `json:"file_path"`              // file absolute path
	FileOwner         string            `json:"file_owner"`             // file owner wallet address
	SimpleChecksum    string            `json:"simple_checksum"`        // hash of first 128 KB and last 128 KB from file content
	WalletAddress     string            `json:"wallet_address"`         // task belong to
	CopyNum           uint64            `json:"copy_num"`               // copy num
	Type              TaskType          `json:"file_info_type"`         // task type
	StoreTx           string            `json:"store_tx"`               // store tx hash
	RegisterDNSTx     string            `json:"register_dns_tx"`        // register dns tx
	BindDNSTx         string            `json:"bind_dns_tx"`            // bind dns tx
	WhitelistTx       string            `json:"whitelist_tx"`           // first op whitelist tx
	TotalBlockCount   uint64            `json:"total_block_count"`      // total block count
	SaveBlockCountMap map[string]uint64 `json:"save_block_count_map"`   // receivers block count map
	TaskState         uint64            `json:"task_state"`             // task state
	ProvePrivKey      []byte            `json:"prove_private_key"`      // prove private key params
	Prefix            []byte            `json:"prefix"`                 // file prefix
	EncryptHash       string            `json:"encrypt_hash"`           // encrypt hash
	EncryptSalt       string            `json:"encrypt_salt"`           // encrypt salt
	Url               string            `json:"url"`                    // url
	Link              string            `json:"link"`                   // link
	CurrentBlock      string            `json:"current_block_hash"`     // current transferred block
	CurrentIndex      uint64            `json:"current_block_index"`    // current transferred block index
	StoreType         uint64            `json:"store_type"`             // store type
	InOrder           bool              `json:"in_order"`               // is in order
	OnlyBlock         bool              `json:"only_block"`             // send only raw block data
	TranferState      uint64            `json:"transfer_state"`         // transfer state
	CreatedAt         uint64            `json:"createdAt"`              // createAt, unit ms
	CreatedAtHeight   uint64            `json:"createdAt_block_height"` // created at block height
	UpdatedAt         uint64            `json:"updatedAt"`              // updatedAt, unit ms
	UpdatedAtHeight   uint64            `json:"updatedAt_block_height"` // updatedAt block height
	ExpiredHeight     uint64            `json:"expired_block_height"`   // expiredAt block height
	ErrorCode         uint32            `json:"error_code"`             // error code
	ErrorMsg          string            `json:"error_msg"`              // error msg
	Result            interface{}       `json:"result"`                 // task complete result
}

type FileProgress struct {
	FileInfoId     string `json:"file_info_id"`
	NodeHostAddr   string `json:"node_host_addr"`
	NodeWalletAddr string `json:"node_wallet_addr"`
	Progress       uint64 `json:"progress"`
}

type FileDownloadUnPaid struct {
	FileInfoId   string             `json:"file_info_id"`
	NodeHostAddr string             `json:"node_host_addr"`
	Payments     map[int32]*Payment `json:"payments"`
}

type Session struct {
	SessionId  string `json:"session_id"`
	WalletAddr string `json:"wallet_addr"`
	HostAddr   string `json:"host_addr"`
	Asset      uint64 `json:"asset"`
	UnitPrice  uint64 `json:"unit_price"`
}

type TaskCount struct {
	Index         uint32 `json:"index"`
	TotalCount    uint32 `json:"total_count"`
	UploadCount   uint32 `json:"upload_count"`
	DownloadCount uint32 `json:"download_count"`
	ShareCount    uint32 `json:"share_count"`
}

func NewFileDB(db *LevelDBStore) *FileDB {
	return &FileDB{
		db:   db,
		lock: new(sync.RWMutex),
	}
}

func (this *FileDB) Close() error {
	return this.db.Close()
}

func (this *FileDB) NewTaskInfo(id string, ft TaskType) (*TaskInfo, error) {
	taskCount, err := this.GetTaskCount()
	if err != nil {
		return nil, err
	}
	fi := &TaskInfo{
		Id:                id,
		Index:             taskCount.Index,
		Type:              ft,
		CreatedAt:         utils.GetMilliSecTimestamp(),
		SaveBlockCountMap: make(map[string]uint64, 0),
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
	err = this.batchSaveFileInfo(batch, fi)
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

func (this *FileDB) GetTaskCount() (*TaskCount, error) {
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

func (this *FileDB) GetTaskIdByIndex(index uint32) (string, error) {
	idKey := TaskIdIndexKey(index)
	buf, err := this.db.Get([]byte(idKey))
	if err != nil && err != leveldb.ErrNotFound {
		return "", err
	}
	if len(buf) == 0 {
		return "", nil
	}
	return string(buf), nil
}

// GetTaskIdList. Get all task id list with offset, limit, task type
func (this *FileDB) GetTaskIdList(offset, limit uint32, ft TaskType, allType, reverse, includeFailed bool) []string {
	count, err := this.GetTaskCount()
	if err != nil {
		return nil
	}
	if count.TotalCount == 0 {
		return nil
	}
	if limit == 0 || limit > 100 {
		limit = 100
	}
	list := make([]string, 0, limit)
	reach := uint32(0)

	start := func() int32 {
		if reverse {
			if int32(count.Index) == 0 {
				return 0
			}
			return int32(count.Index) - 1
		} else {
			return 0
		}
	}
	cond := func(i int32) bool {
		if reverse {
			return i >= 0
		}
		return i < int32(count.Index)
	}
	next := func(i int32) int32 {
		if reverse {
			return int32(i) - 1
		}
		return int32(i) + 1
	}
	for i := start(); cond(i); i = next(i) {
		id, _ := this.GetTaskIdByIndex(uint32(i))
		if len(id) == 0 {
			continue
		}
		if offset > reach {
			reach++
			continue
		}
		info, err := this.GetFileInfo(id)
		if err != nil || info == nil {
			log.Warnf("get file info of id %s failed", id)
			continue
		}
		if !allType {
			if info.Type != ft {
				continue
			}
			if info.TaskState == uint64(TaskStateDone) {
				continue
			}
		}
		if !includeFailed && info.TaskState == uint64(TaskStateFailed) {
			continue
		}
		list = append(list, id)
		if uint32(len(list)) >= limit {
			break
		}
	}
	return list
}

func (this *FileDB) SaveFileInfoId(key, id string) error {
	return this.db.Put([]byte(TaskInfoIdWithFile(key)), []byte(id))
}

func (this *FileDB) GetFileInfoId(key string) (string, error) {
	id, err := this.db.Get([]byte(TaskInfoIdWithFile(key)))
	if err != nil {
		return "", err
	}
	return string(id), nil
}

func (this *FileDB) DeleteFileInfoId(key string) error {
	return this.db.Delete([]byte(key))
}

func (this *FileDB) SetFileInfoField(id string, field int, value interface{}) error {
	fi, err := this.GetFileInfo(id)
	if err != nil {
		return err
	}
	if fi == nil {
		return fmt.Errorf("fileinfo not found of %s", id)
	}
	switch field {
	case FILEINFO_FIELD_FILENAME:
		fi.FileName = value.(string)
	case FILEINFO_FIELD_STORETX:
		fi.StoreTx = value.(string)
	case FILEINFO_FIELD_WHITELISTTX:
		fi.WhitelistTx = value.(string)
	case FILEINFO_FIELD_PROVE_PRIVATEKEY:
		fi.ProvePrivKey = value.([]byte)
	case FILEINFO_FIELD_PREFIX:
		fi.Prefix = value.([]byte)
	case FILEINFO_FIELD_TOTALCOUNT:
		fi.TotalBlockCount = value.(uint64)
	case FILEINFO_FIELD_COPYNUM:
		fi.CopyNum = value.(uint64)
	case FILEINFO_FIELD_URL:
		fi.Url = value.(string)
	case FILEINFO_FIELD_LINK:
		fi.Link = value.(string)
	case FILEINFO_FIELD_FILEHASH:
		fi.FileHash = value.(string)
	case FILEINFO_FIELD_FILEPATH:
		fi.FilePath = value.(string)
	case FILEINFO_FIELD_WALLETADDR:
		fi.WalletAddress = value.(string)
	case FILEINFO_FIELD_REGURL_TX:
		fi.RegisterDNSTx = value.(string)
	case FILEINFO_FIELD_BIND_TX:
		fi.BindDNSTx = value.(string)
	case FILEINFO_FIELD_TASKSTATE:
		fi.TaskState = value.(uint64)
	case FILEINFO_FIELD_OWNER:
		fi.FileOwner = value.(string)
	}
	return this.batchSaveFileInfo(nil, fi)
}

func (this *FileDB) SetFileName(id string, fileName string) error {

	fi, err := this.GetFileInfo(id)
	if err != nil {
		return err
	}
	if fi == nil {
		return fmt.Errorf("fileinfo not found of %s", id)
	}
	fi.FileName = fileName
	return this.batchSaveFileInfo(nil, fi)
}

func (this *FileDB) SetFileInfoFields(id string, m map[int]interface{}) error {

	fi, err := this.GetFileInfo(id)
	if err != nil {
		return err
	}
	if fi == nil {
		return fmt.Errorf("fileinfo not found of %s", id)
	}
	for field, value := range m {
		switch field {
		case FILEINFO_FIELD_FILENAME:
			fi.FileName = value.(string)
		case FILEINFO_FIELD_STORETX:
			fi.StoreTx = value.(string)
		case FILEINFO_FIELD_WHITELISTTX:
			fi.WhitelistTx = value.(string)
		case FILEINFO_FIELD_PROVE_PRIVATEKEY:
			fi.ProvePrivKey = value.([]byte)
		case FILEINFO_FIELD_PREFIX:
			fi.Prefix = value.([]byte)
		case FILEINFO_FIELD_TOTALCOUNT:
			fi.TotalBlockCount = value.(uint64)
		case FILEINFO_FIELD_COPYNUM:
			fi.CopyNum = value.(uint64)
		case FILEINFO_FIELD_URL:
			fi.Url = value.(string)
		case FILEINFO_FIELD_LINK:
			fi.Link = value.(string)
		case FILEINFO_FIELD_FILEHASH:
			fi.FileHash = value.(string)
		case FILEINFO_FIELD_FILEPATH:
			fi.FilePath = value.(string)
		case FILEINFO_FIELD_WALLETADDR:
			fi.WalletAddress = value.(string)
		case FILEINFO_FIELD_REGURL_TX:
			fi.RegisterDNSTx = value.(string)
		case FILEINFO_FIELD_BIND_TX:
			fi.BindDNSTx = value.(string)
		case FILEINFO_FIELD_TASKSTATE:
			fi.TaskState = value.(uint64)
		case FILEINFO_FIELD_OWNER:
			fi.FileOwner = value.(string)
		}
	}
	return this.batchSaveFileInfo(nil, fi)
}

func (this *FileDB) GetFileInfoStringValue(id string, field int) (string, error) {
	fi, err := this.GetFileInfo(id)
	if err != nil {
		return "", err
	}
	if fi == nil {
		return "", fmt.Errorf("fileinfo not found of %s", id)
	}
	switch field {
	case FILEINFO_FIELD_FILENAME:
		return fi.FileName, nil
	case FILEINFO_FIELD_STORETX:
		return fi.StoreTx, nil
	case FILEINFO_FIELD_WHITELISTTX:
		return fi.WhitelistTx, nil
	case FILEINFO_FIELD_URL:
		return fi.Url, nil
	case FILEINFO_FIELD_LINK:
		return fi.Link, nil
	case FILEINFO_FIELD_FILEPATH:
		return fi.FilePath, nil
	case FILEINFO_FIELD_WALLETADDR:
		return fi.WalletAddress, nil
	case FILEINFO_FIELD_REGURL_TX:
		return fi.RegisterDNSTx, nil
	case FILEINFO_FIELD_BIND_TX:
		return fi.BindDNSTx, nil
	}
	return "", fmt.Errorf("fileinfo field not found %s %d", id, field)
}

func (this *FileDB) GetFileInfoBytesValue(id string, field int) ([]byte, error) {

	fi, err := this.GetFileInfo(id)
	if err != nil {
		return nil, err
	}
	if fi == nil {
		return nil, fmt.Errorf("fileinfo not found of %s", id)
	}
	switch field {
	case FILEINFO_FIELD_PROVE_PRIVATEKEY:
		return fi.ProvePrivKey, nil
	case FILEINFO_FIELD_PREFIX:
		return fi.Prefix, nil
	}
	return nil, fmt.Errorf("fileinfo field not found %s %d", id, field)
}

func (this *FileDB) GetFileInfoUint64Value(id string, field int) (uint64, error) {

	fi, err := this.GetFileInfo(id)
	if err != nil {
		return 0, err
	}
	if fi == nil {
		return 0, fmt.Errorf("fileinfo not found of %s", id)
	}
	switch field {
	case FILEINFO_FIELD_TOTALCOUNT:
		return fi.TotalBlockCount, nil
	case FILEINFO_FIELD_COPYNUM:
		return fi.CopyNum, nil
	case FILEINFO_FIELD_TASKSTATE:
		return fi.TaskState, nil
	}
	return 0, fmt.Errorf("fileinfo field not found %s %d", id, field)
}

func (this *FileDB) DeleteTaskIds(ids []string) error {
	batch := this.db.NewBatch()
	for _, id := range ids {
		taskInfo, err := this.GetFileInfo(id)
		if err != nil || taskInfo == nil {
			continue
		}
		key := TaskIdIndexKey(taskInfo.Index)
		this.db.BatchDelete(batch, []byte(key))
	}
	return this.db.BatchCommit(batch)
}

// DeleteFileInfo. delete file info from db
func (this *FileDB) DeleteFileInfo(id string) error {
	taskCount, err := this.GetTaskCount()
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
	fi, _ := this.GetFileInfo(id)
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
	err = this.RemoveFromUnSalvedList(batch, id, fi.Type)
	if err != nil {
		return err
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
			unpaidKeys, _ := this.db.QueryKeysByPrefix([]byte(FileUnpaidQueryKey(id)))
			for _, unpaidKey := range unpaidKeys {
				this.db.BatchDelete(batch, unpaidKey)
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

// AddUploadedBlock. add a uploaded block into db
func (this *FileDB) AddUploadedBlock(id, blockHashStr, nodeAddr string, index uint32, dataSize, offset uint64) error {
	this.lock.Lock()
	defer this.lock.Unlock()
	fi, err := this.GetFileInfo(id)
	if err != nil {
		log.Errorf("get info err %s", err)
		return err
	}
	if fi == nil {
		log.Errorf("file info not found %d", id)
		return errors.New("file info not found")
	}
	// save block info
	blockKey := BlockInfoKey(id, index, blockHashStr)
	block, _ := this.getBlockInfo(blockKey)
	if block == nil {
		block = &BlockInfo{
			FileInfoId: id,
			FileHash:   fi.FileHash,
			Hash:       blockHashStr,
			Index:      index,
			NodeList:   make([]string, 0),
			ReqTimes:   make(map[string]uint64),
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
		return this.db.Put([]byte(blockKey), blockBuf)
	}
	block.ReqTimes[nodeAddr] = reqTime + 1
	block.NodeList = append(block.NodeList, nodeAddr)
	if block.DataOffset < offset {
		block.DataOffset = offset
		log.Debugf("set offset for %d %d, old %v", index, offset, block.DataOffset)
	}
	if block.DataSize < dataSize {
		block.DataSize = dataSize
	}
	blockBuf, err := json.Marshal(block)
	if err != nil {
		return err
	}
	// save upload progress info
	progressKey := FileProgressKey(fi.Id, nodeAddr)
	progress, _ := this.getProgressInfo(progressKey)
	if progress == nil {
		progress = &FileProgress{
			FileInfoId:   fi.Id,
			NodeHostAddr: nodeAddr,
		}
	}
	if progress.Progress == fi.TotalBlockCount && fi.SaveBlockCountMap[nodeAddr] == fi.TotalBlockCount && fi.TotalBlockCount > 0 {
		// has done
		log.Debugf("block has added: %d, %v, %v, %s", progress.Progress, fi.TotalBlockCount, fi.SaveBlockCountMap, nodeAddr)
		return nil
	}
	progress.Progress++
	progressBuf, err := json.Marshal(progress)
	if err != nil {
		return err
	}
	fi.SaveBlockCountMap[nodeAddr] = fi.SaveBlockCountMap[nodeAddr] + 1
	fi.CurrentBlock = blockHashStr
	fi.CurrentIndex = uint64(index)
	fiBuf, err := json.Marshal(fi)
	if err != nil {
		return err
	}
	batch := this.db.NewBatch()
	this.db.BatchPut(batch, []byte(blockKey), blockBuf)
	this.db.BatchPut(batch, []byte(progressKey), progressBuf)
	this.db.BatchPut(batch, []byte(TaskInfoKey(fi.Id)), fiBuf)
	log.Debugf("%s, nodeAddr %s increase sent %v, reqTime %v", fi.Id, nodeAddr, fi.SaveBlockCountMap, block.ReqTimes[nodeAddr])
	return this.db.BatchCommit(batch)
}

func (this *FileDB) SetBlocksUploaded(id, nodeAddr string, blockInfos []*BlockInfo) error {
	this.lock.Lock()
	defer this.lock.Unlock()
	fi, err := this.GetFileInfo(id)
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
			FileInfoId:   fi.Id,
			NodeHostAddr: nodeAddr,
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
				FileInfoId: id,
				FileHash:   fi.FileHash,
				Hash:       blockHashStr,
				Index:      index,
				NodeList:   make([]string, 0),
				ReqTimes:   make(map[string]uint64),
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
			log.Debugf("set offset for %d %d, old %v", index, offset, block.DataOffset)
		}
		if block.DataSize < bi.DataSize {
			block.DataSize = bi.DataSize
		}
		blockBuf, err := json.Marshal(block)
		if err != nil {
			return err
		}
		if progress.Progress == fi.TotalBlockCount && fi.SaveBlockCountMap[nodeAddr] == fi.TotalBlockCount && fi.TotalBlockCount > 0 {
			// has done
			log.Debugf("block has added: %d, %v, %v, %s", progress.Progress, fi.TotalBlockCount, fi.SaveBlockCountMap, nodeAddr)
			return nil
		}
		progress.Progress++
		fi.SaveBlockCountMap[nodeAddr] = fi.SaveBlockCountMap[nodeAddr] + 1
		fi.CurrentBlock = blockHashStr
		fi.CurrentIndex = uint64(index)
		this.db.BatchPut(batch, []byte(blockKey), blockBuf)
	}
	log.Debugf("%s, nodeAddr %s increase sent %v, progress %v", fi.Id, nodeAddr, fi.SaveBlockCountMap, progress)
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
func (this *FileDB) UpdateTaskPeerProgress(id, nodeAddr string, count uint64) error {
	this.lock.Lock()
	defer this.lock.Unlock()
	batch := this.db.NewBatch()
	// save upload progress info
	progressKey := FileProgressKey(id, nodeAddr)
	progress, _ := this.getProgressInfo(progressKey)
	if progress == nil {
		progress = &FileProgress{
			FileInfoId:   id,
			NodeHostAddr: nodeAddr,
		}
	}
	progress.Progress = count
	log.Debugf("%s, nodeAddr %s progress %v", id, nodeAddr, progress)
	progressBuf, err := json.Marshal(progress)
	if err != nil {
		return err
	}
	this.db.BatchPut(batch, []byte(progressKey), progressBuf)
	return this.db.BatchCommit(batch)
}

// GetTaskPeerProgress. get progress for a peer
func (this *FileDB) GetTaskPeerProgress(id, nodeAddr string) uint64 {
	this.lock.Lock()
	defer this.lock.Unlock()
	// save upload progress info
	progressKey := FileProgressKey(id, nodeAddr)
	progress, _ := this.getProgressInfo(progressKey)
	if progress == nil {
		return 0
	}
	return progress.Progress
}

// GetCurrentSetBlock.
func (this *FileDB) GetCurrentSetBlock(id string) (string, uint64, error) {
	fi, err := this.GetFileInfo(id)
	if err != nil || fi == nil {
		return "", 0, fmt.Errorf("get file info not found: %s", id)
	}
	return fi.CurrentBlock, fi.CurrentIndex, nil
}

func (this *FileDB) GetBlockOffset(id, blockHash string, index uint32) (uint64, error) {
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

func (this *FileDB) IsFileUploaded(id string) bool {
	this.lock.RLock()
	defer this.lock.RUnlock()
	fi, err := this.GetFileInfo(id)
	if err != nil || fi == nil {
		return false
	}
	sum := uint64(0)
	for _, cnt := range fi.SaveBlockCountMap {
		sum += cnt
	}
	uploaded := sum
	log.Debugf("IsFileUploaded %d %d, save: %d, copyNum: %d", fi.TotalBlockCount, uploaded, sum, fi.CopyNum)
	return fi.TotalBlockCount > 0 && fi.TotalBlockCount == uploaded
}

// IsBlockUploaded. check if a block is uploaded
func (this *FileDB) IsBlockUploaded(id, blockHashStr, nodeAddr string, index uint32) bool {
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
func (this *FileDB) GetUploadedBlockNodeList(id, blockHashStr string, index uint32) []string {
	blockKey := BlockInfoKey(id, index, blockHashStr)
	block, err := this.getBlockInfo(blockKey)
	if block == nil || err != nil {
		return nil
	}
	return block.NodeList
}

// UploadedBlockCount
func (this *FileDB) UploadedBlockCount(id string) uint64 {
	this.lock.RLock()
	defer this.lock.RUnlock()
	fi, err := this.GetFileInfo(id)
	if err != nil || fi == nil {
		return 0
	}
	sum := uint64(0)
	for _, cnt := range fi.SaveBlockCountMap {
		sum += cnt
	}
	log.Debugf("get sent %d", sum)
	return sum
}

// AddFileBlockHashes add all blocks' hash, using for detect whether the node has stored the file
func (this *FileDB) AddFileBlockHashes(id string, blocks []string) error {
	// TODO: test performance
	batch := this.db.NewBatch()
	for index, hash := range blocks {
		key := BlockInfoKey(id, uint32(index), hash)
		info := &BlockInfo{
			FileInfoId: id,
			Hash:       hash,
			Index:      uint32(index),
		}
		buf, err := json.Marshal(info)
		if err != nil {
			return err
		}
		this.db.BatchPut(batch, []byte(key), buf)
	}
	return this.db.BatchCommit(batch)
}

func (this *FileDB) AddFileUnpaid(id, recipientWalAddr string, paymentId, asset int32, amount uint64) error {
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
			FileInfoId: id,
			Payments:   make(map[int32]*Payment, 0),
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
func (this *FileDB) GetUnpaidPayments(id, payToAddress string, asset int32) (map[int32]*Payment, error) {
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

func (this *FileDB) DeleteFileUnpaid(id, payToAddress string, paymentId, asset int32, amount uint64) error {
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

// IsInfoExist return a file is exist or not
func (this *FileDB) IsFileInfoExist(id string) bool {
	fi, err := this.GetFileInfo(id)
	if err != nil || fi == nil {
		return false
	}
	return true
}

// FileBlockHashes. return file block hashes
func (this *FileDB) FileBlockHashes(id string) []string {
	fi, err := this.GetFileInfo(id)
	if err != nil || fi == nil {
		return nil
	}
	hashes := make([]string, 0, fi.TotalBlockCount)
	for i := uint32(0); i < uint32(fi.TotalBlockCount); i++ {
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
func (this *FileDB) FileProgress(id string) map[string]uint64 {
	prefix := FileProgressKey(id, "")
	keys, err := this.db.QueryStringKeysByPrefix([]byte(prefix))
	if err != nil {
		return nil
	}
	m := make(map[string]uint64)
	for _, key := range keys {
		progress, err := this.getProgressInfo(key)
		if err != nil {
			continue
		}
		m[progress.NodeHostAddr] = progress.Progress
	}
	return m
}

//  SetBlockStored set the flag of store state
func (this *FileDB) SetBlockDownloaded(id, blockHashStr, nodeAddr string, index uint32, offset int64, links []string) error {
	blockKey := BlockInfoKey(id, index, blockHashStr)
	block, err := this.getBlockInfo(blockKey)
	if block == nil || err != nil {
		block = &BlockInfo{
			NodeList:   make([]string, 0),
			LinkHashes: make([]string, 0),
		}
	}
	block.FileInfoId = id
	block.Hash = blockHashStr
	block.Index = index
	block.DataOffset = uint64(offset)
	block.NodeList = append(block.NodeList, nodeAddr)
	block.LinkHashes = append(block.LinkHashes, links...)

	blockBuf, err := json.Marshal(block)
	if err != nil {
		return err
	}

	progressKey := FileProgressKey(id, nodeAddr)
	progress, err := this.getProgressInfo(progressKey)
	if progress == nil || err != nil {
		progress = &FileProgress{
			FileInfoId:   id,
			NodeHostAddr: nodeAddr,
		}
	}
	progress.Progress++
	progressBuf, err := json.Marshal(progress)
	if err != nil {
		return err
	}

	fi, err := this.GetFileInfo(id)
	if err != nil {
		return err
	}
	fi.SaveBlockCountMap[nodeAddr]++
	fi.CurrentBlock = blockHashStr
	fi.CurrentIndex = uint64(index)
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
func (this *FileDB) IsBlockDownloaded(id, blockHashStr string, index uint32) bool {
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
func (this *FileDB) IsFileDownloaded(id string) bool {
	fi, err := this.GetFileInfo(id)
	if err != nil || fi == nil {
		return false
	}
	sum := uint64(0)
	for _, cnt := range fi.SaveBlockCountMap {
		sum += cnt
	}
	return sum == fi.TotalBlockCount && fi.TotalBlockCount > 0
}

// GetUndownloadedBlockInfo. check undownloaded block in-order
func (this *FileDB) GetUndownloadedBlockInfo(id, rootBlockHash string) ([]string, map[string]uint32, error) {
	fi, err := this.GetFileInfo(id)
	if err != nil || fi == nil {
		return nil, nil, errors.New("file not found")
	}
	hashes := make([]string, 0)
	indexMap := make(map[string]uint32)
	var search func(string, uint32) error
	// TEST: improve performance
	log.Debugf("search %s", rootBlockHash)
	search = func(blockHash string, blockIndex uint32) error {
		blockKey := BlockInfoKey(id, blockIndex, blockHash)
		block, err := this.getBlockInfo(blockKey)
		if err != nil {
			return err
		}
		if block == nil || len(block.NodeList) == 0 {
			hashes = append(hashes, blockHash)
			indexMap[blockHash] = blockIndex
			return nil
		}
		if len(block.LinkHashes) == 0 {
			return nil
		}
		oldIndex := blockIndex
		childUndoneIndex := -1
		for i, hash := range block.LinkHashes {
			blockIndex++
			childBlockKey := BlockInfoKey(id, blockIndex, hash)
			childBlock, err := this.getBlockInfo(childBlockKey)
			if err != nil {
				return err
			}
			if childBlock != nil && len(childBlock.NodeList) > 0 {
				// downloaded block, skip
				continue
			}
			hashes = append(hashes, hash)
			indexMap[hash] = blockIndex
			if childUndoneIndex == -1 {
				childUndoneIndex = i
			}
		}
		for i, hash := range block.LinkHashes {
			if childUndoneIndex != -1 && i == childUndoneIndex {
				break
			}
			oldIndex++
			neighBorBlockKey := BlockInfoKey(id, oldIndex, hash)
			neighBlock, err := this.getBlockInfo(neighBorBlockKey)
			if err != nil {
				return err
			}
			for _, ch := range neighBlock.LinkHashes {
				blockIndex++
				err := search(ch, blockIndex)
				if err != nil {
					return err
				}
			}
		}
		return nil
	}
	err = search(rootBlockHash, 0)
	log.Debugf("search done err %s", err)
	if err != nil {
		return nil, nil, err
	}
	log.Debugf("undownloaded hashes :%v, len:%d", hashes, len(hashes))
	for h, i := range indexMap {
		log.Debugf("undownloaded hashes-index %s-%d", h, i)
	}
	return hashes, indexMap, nil
}

func (this *FileDB) RemoveFromUndoneList(batch *leveldb.Batch, id string, ft TaskType) error {
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

func (this *FileDB) UndoneList(ft TaskType) ([]string, error) {
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

func (this *FileDB) SetUploadProgressDone(id, nodeAddr string) error {
	this.lock.Lock()
	defer this.lock.Unlock()
	log.Debugf("SetUploadProgressDone :%s, addr: %s", id, nodeAddr)
	fi, err := this.GetFileInfo(id)
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
			FileInfoId:   fi.Id,
			NodeHostAddr: nodeAddr,
		}
	}
	log.Debugf("save upload progress done before: %v %v", fi.SaveBlockCountMap, progress.Progress)
	progress.Progress = fi.TotalBlockCount
	progressBuf, err := json.Marshal(progress)
	if err != nil {
		return err
	}
	// TODO: split save block count for each node
	fi.SaveBlockCountMap[nodeAddr] = fi.TotalBlockCount
	log.Debugf("save upload progress done after: %v %v", fi.SaveBlockCountMap, progress.Progress)
	fiBuf, err := json.Marshal(fi)
	if err != nil {
		return err
	}
	batch := this.db.NewBatch()
	this.db.BatchPut(batch, []byte(progressKey), progressBuf)
	this.db.BatchPut(batch, []byte(TaskInfoKey(fi.Id)), fiBuf)
	return this.db.BatchCommit(batch)
}

func (this *FileDB) SaveFileUploaded(id string) error {
	return this.RemoveFromUndoneList(nil, id, TaskTypeUpload)
}

func (this *FileDB) SaveFileDownloaded(id string) error {
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
func (this *FileDB) AllDownloadFiles() ([]*TaskInfo, []string, error) {
	countKey := FileDownloadedCountKey()
	countBuf, err := this.db.Get([]byte(countKey))
	log.Debugf("countkey:%v, countBuf:%v, err %s", countKey, countBuf, err)
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
	log.Debugf("count :%v", count)
	all := make([]string, 0, count)
	infos := make([]*TaskInfo, 0, count)
	for i := uint32(0); i < uint32(count); i++ {
		downloadedKey := FileDownloadedKey(i)
		log.Debugf("download key %v", downloadedKey)
		idBuf, err := this.db.Get([]byte(downloadedKey))
		if err != nil || len(idBuf) == 0 {
			continue
		}
		fi, err := this.GetFileInfo(string(idBuf))
		if err != nil || fi == nil {
			continue
		}
		if len(fi.FileHash) == 0 {
			continue
		}
		all = append(all, fi.FileHash)
		infos = append(infos, fi)
	}
	return infos, all, nil
}

func (this *FileDB) AddShareTo(id, walletAddress string) error {
	shareKey := FileShareToKey(id, walletAddress)
	return this.db.Put([]byte(shareKey), []byte("true"))
}

func (this *FileDB) GetUnpaidAmount(id, walletAddress string, asset int32) (uint64, error) {
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
func (this *FileDB) GetFileInfo(id string) (*TaskInfo, error) {
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

func (this *FileDB) SetFileUploadOptions(fileInfoId string, options *fs.UploadOption) error {
	buf, err := json.Marshal(options)
	if err != nil {
		return err
	}
	key := FileOptionsKey(fileInfoId)
	return this.db.Put([]byte(key), buf)
}

func (this *FileDB) GetFileUploadOptions(fileInfoId string) (*fs.UploadOption, error) {
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

func (this *FileDB) SetFileDownloadOptions(fileInfoId string, options *common.DownloadOption) error {
	buf, err := json.Marshal(options)
	if err != nil {
		return err
	}
	key := FileOptionsKey(fileInfoId)
	return this.db.Put([]byte(key), buf)
}

func (this *FileDB) GetFileDownloadOptions(fileInfoId string) (*common.DownloadOption, error) {
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

func (this *FileDB) AddFileSession(fileInfoId, sessionId, walletAddress, hostAddress string, asset, unitPrice uint64) error {
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

func (this *FileDB) GetFileSessions(fileInfoId string) (map[string]*Session, error) {
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
func (this *FileDB) SaveFileInfo(info *TaskInfo) error {
	return this.batchSaveFileInfo(nil, info)
}

func (this *FileDB) GetTaskIdWithPaymentId(paymentId int32) (string, error) {
	buf, err := this.db.Get([]byte(TaskIdOfPaymentIDKey(paymentId)))
	if err != nil && err != leveldb.ErrNotFound {
		return "", err
	}
	return string(buf), nil
}

func (this *FileDB) RemoveFromUnSalvedList(batch *leveldb.Batch, id string, ft TaskType) error {
	var list []string
	var key string
	switch ft {
	case TaskTypeUpload:
		key = FileUploadUnSalvedKey()
	case TaskTypeDownload:
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

func (this *FileDB) UnSlavedList(ft TaskType) ([]string, error) {
	var list []string
	var key string
	switch ft {
	case TaskTypeUpload:
		key = FileUploadUnSalvedKey()
	case TaskTypeDownload:
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

func (this *FileDB) batchAddToUndoneList(batch *leveldb.Batch, id string, ft TaskType) error {
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

func (this *FileDB) batchAddToUnSlavedList(batch *leveldb.Batch, id string, ft TaskType) error {
	var key string
	switch ft {
	case TaskTypeUpload:
		key = FileUploadUnSalvedKey()
	case TaskTypeDownload:
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

func (this *FileDB) batchSaveFileInfo(batch *leveldb.Batch, info *TaskInfo) error {
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

func (this *FileDB) batchSaveTaskCount(batch *leveldb.Batch, taskCount *TaskCount) error {
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
func (this *FileDB) getBlockInfo(key string) (*BlockInfo, error) {
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

func (this *FileDB) saveBlockInfo(key string, info *BlockInfo) error {
	buf, err := json.Marshal(info)
	if err != nil {
		return err
	}
	return this.db.Put([]byte(key), buf)
}

func (this *FileDB) getProgressInfo(key string) (*FileProgress, error) {
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

func (this *FileDB) saveProgress(key []byte, info *FileProgress) error {
	buf, err := json.Marshal(info)
	if err != nil {
		return err
	}
	return this.db.Put(key, buf)
}

func (this *FileDB) getFileUnpaidInfo(key string) (*FileDownloadUnPaid, error) {
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

func (this *FileDB) batchDeleteBlocks(batch *leveldb.Batch, fi *TaskInfo) error {
	if fi == nil {
		return nil
	}
	for i := uint32(0); i < uint32(fi.TotalBlockCount); i++ {
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

func (this *FileDB) batchDeleteProgress(batch *leveldb.Batch, id string) error {
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
