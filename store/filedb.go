package store

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/saveio/themis/common/log"
	"github.com/syndtr/goleveldb/leveldb"
)

// FileDB. implement a db storage for save information of sending/downloading/downloaded files
type FileDB struct {
	db   *LevelDBStore
	lock sync.RWMutex
}

type FileInfoType int

const (
	FileInfoTypeUpload FileInfoType = iota
	FileInfoTypeDownload
	FileInfoTypeShare
)

// blockInfo record a block infomation of a file
type BlockInfo struct {
	FileInfoId string   `json:"file_info_id"`
	FileHash   string   `json:"file_hash"`
	Hash       string   `json:"hash"`                  // block  hash
	Index      uint32   `json:"index"`                 // block index of file
	DataOffset uint64   `json:"data_offset"`           // block raw data offset
	DataSize   uint64   `json:"data_size"`             // block data size
	NodeList   []string `json:"node_list,omitempty"`   // uploaded node list
	LinkHashes []string `json:"link_hashes,omitempty"` // child link hashes slice
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
	FILEINFO_FIELD_REQUESTID
)

// fileInfo keep all blocks infomation and the prove private key for generating tags
type FileInfo struct {
	Id              string       `json:"id"`
	FileHash        string       `json:"file_hash"`
	FileName        string       `json:"file_name"`
	CopyNum         uint64       `json:"copy_num"`
	InfoType        FileInfoType `json:"file_info_type"`
	StoreTx         string       `json:"store_tx"`
	RegisterDNSTx   string       `json:"register_dns_tx"`
	BindDNSTx       string       `json:"bind_dns_tx"`
	WhitelistTx     string       `json:"whitelist_tx"`
	TotalBlockCount uint64       `json:"total_block_count"`
	SaveBlockCount  uint64       `json:"save_block_count"`
	ProvePrivKey    []byte       `json:"prove_private_key"`
	Prefix          string       `json:"prefix"`
	EncryptHash     string       `json:"encrypt_hash"`
	EncryptSalt     string       `json:"encrypt_salt"`
	Url             string       `json:"url`
	Link            string       `json:"link"`
	RequestId       string       `json:"requestId"`
	CreatedAt       uint64       `json:"createdAt"`
	UpdatedAt       uint64       `json:"updatedAt"`
}

type FileProgress struct {
	FileInfoId     string `json:"file_info_id"`
	NodeHostAddr   string `json:"node_host_addr"`
	NodeWalletAddr string `json:"node_wallet_addr"`
	Progress       uint64 `json:"progress"`
}

type FileDownloadUnPaid struct {
	FileInfoId   string `json:"file_info_id"`
	NodeHostAddr string `json:"node_host_addr"`
	Payment
}

func NewFileDB(db *LevelDBStore) *FileDB {
	return &FileDB{
		db: db,
	}
}

func (this *FileDB) Close() error {
	return this.db.Close()
}

func (this *FileDB) NewFileInfo(id string, ft FileInfoType) error {
	fi := &FileInfo{
		Id:        id,
		InfoType:  ft,
		CreatedAt: uint64(time.Now().Unix()),
	}
	return this.saveFileInfo(fi)
}

func (this *FileDB) SaveFileInfoId(key, id string) error {
	return this.db.Put([]byte(key), []byte(id))
}

func (this *FileDB) GetFileInfoId(key string) (string, error) {
	id, err := this.db.Get([]byte(key))
	if err != nil {
		return "", err
	}
	return string(id), nil
}

func (this *FileDB) DeleteFileInfoId(key string) error {
	return this.db.Delete([]byte(key))
}

func (this *FileDB) SetFileInfoField(id string, field int, value interface{}) error {
	key := []byte(id)
	fi, err := this.GetFileInfo(key)
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
		fi.Prefix = value.(string)
	case FILEINFO_FIELD_TOTALCOUNT:
		fi.TotalBlockCount = value.(uint64)
	case FILEINFO_FIELD_COPYNUM:
		fi.CopyNum = value.(uint64)
	case FILEINFO_FIELD_URL:
		fi.Url = value.(string)
	case FILEINFO_FIELD_LINK:
		fi.Link = value.(string)
	case FILEINFO_FIELD_REQUESTID:
		fi.RequestId = value.(string)
	case FILEINFO_FIELD_FILEHASH:
		fi.FileHash = value.(string)
	}
	return this.saveFileInfo(fi)
}

func (this *FileDB) SetFileInfoFields(id string, m map[int]interface{}) error {
	key := []byte(id)
	fi, err := this.GetFileInfo(key)
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
			fi.Prefix = value.(string)
		case FILEINFO_FIELD_TOTALCOUNT:
			fi.TotalBlockCount = value.(uint64)
		case FILEINFO_FIELD_COPYNUM:
			fi.CopyNum = value.(uint64)
		case FILEINFO_FIELD_URL:
			fi.Url = value.(string)
		case FILEINFO_FIELD_LINK:
			fi.Link = value.(string)
		case FILEINFO_FIELD_REQUESTID:
			fi.RequestId = value.(string)
		case FILEINFO_FIELD_FILEHASH:
			fi.FileHash = value.(string)
		}
	}
	return this.saveFileInfo(fi)
}

func (this *FileDB) GetFileInfoStringValue(id string, field int) (string, error) {
	key := []byte(id)
	fi, err := this.GetFileInfo(key)
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
	case FILEINFO_FIELD_PREFIX:
		return fi.Prefix, nil
	case FILEINFO_FIELD_URL:
		return fi.Url, nil
	case FILEINFO_FIELD_LINK:
		return fi.Link, nil
	case FILEINFO_FIELD_REQUESTID:
		return fi.RequestId, nil
	}
	return "", fmt.Errorf("fileinfo field not found %s %d", id, field)
}

func (this *FileDB) GetFileInfoBytesValue(id string, field int) ([]byte, error) {
	key := []byte(id)
	fi, err := this.GetFileInfo(key)
	if err != nil {
		return nil, err
	}
	if fi == nil {
		return nil, fmt.Errorf("fileinfo not found of %s", id)
	}
	switch field {
	case FILEINFO_FIELD_PROVE_PRIVATEKEY:
		return fi.ProvePrivKey, nil
	}
	return nil, fmt.Errorf("fileinfo field not found %s %d", id, field)
}

func (this *FileDB) GetFileInfoUint64Value(id string, field int) (uint64, error) {
	key := []byte(id)
	fi, err := this.GetFileInfo(key)
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
	}
	return 0, fmt.Errorf("fileinfo field not found %s %d", id, field)
}

// DeleteFileInfo. delete file info from db
func (this *FileDB) DeleteFileInfo(id string) error {
	//TODO: clean up all
	return this.db.Delete([]byte(id))
}

// AddUploadedBlock. add a uploaded block into db
func (this *FileDB) AddUploadedBlock(id, blockHashStr, nodeAddr string, index uint32, dataSize, offset uint64) error {
	this.lock.Lock()
	defer this.lock.Unlock()
	fi, err := this.GetFileInfo([]byte(id))
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
		}
	}
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
	progress.Progress++
	progressBuf, err := json.Marshal(progress)
	if err != nil {
		return err
	}
	fi.SaveBlockCount++
	fiBuf, err := json.Marshal(fi)
	if err != nil {
		return err
	}
	key := FileBlockTailKey(id, index)
	value := fmt.Sprintf("%d", offset+dataSize)

	this.db.NewBatch()
	this.db.BatchPut([]byte(blockKey), blockBuf)
	this.db.BatchPut([]byte(progressKey), progressBuf)
	this.db.BatchPut([]byte(fi.Id), fiBuf)
	this.db.Put([]byte(key), []byte(value))
	log.Debugf("nodeAddr %s increase sent %d, tail %v", nodeAddr, fi.SaveBlockCount, value)
	return this.db.BatchCommit()
}

func (this *FileDB) GetBlockTail(id string, index uint32) (uint64, error) {
	if index == 0 {
		return 0, nil
	}
	key := FileBlockTailKey(id, index)
	value, err := this.db.Get([]byte(key))
	if err != nil {
		return 0, err
	}
	return strconv.ParseUint(string(value), 10, 64)
}

func (this *FileDB) SetBlockTail(id string, index, tail uint32) error {
	key := FileBlockTailKey(id, index)
	value := fmt.Sprintf("%d", tail)
	return this.db.Put([]byte(key), []byte(value))
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
	fi, err := this.GetFileInfo([]byte(id))
	if err != nil || fi == nil {
		return false
	}
	uploaded := fi.SaveBlockCount / (fi.CopyNum + 1)
	log.Debugf("IsFileUploaded %d %d", fi.TotalBlockCount, uploaded)
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
		if nodeAddr == addr {
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
	fi, err := this.GetFileInfo([]byte(id))
	if err != nil || fi == nil {
		return 0
	}
	log.Debugf("get sent %d", fi.SaveBlockCount)
	return fi.SaveBlockCount
}

// AddFileBlockHashes add all blocks' hash, using for detect whether the node has stored the file
func (this *FileDB) AddFileBlockHashes(id string, blocks []string) error {
	// TODO: test performance
	this.db.NewBatch()
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
		this.db.BatchPut([]byte(key), buf)
	}
	return this.db.BatchCommit()
}

func (this *FileDB) AddFileUnpaid(id, walletAddress string, asset int32, amount uint64) error {
	unpaidKey := FileUnpaidKey(id, walletAddress, asset)
	info, err := this.getFileUnpaidInfo(unpaidKey)
	if err != nil {
		log.Errorf("getFileUnpaidInfo err %s", err)
		return err
	}
	if info == nil {
		info = &FileDownloadUnPaid{
			FileInfoId: id,
		}
		info.WalletAddress = walletAddress
		info.Asset = asset
	}
	info.Amount = info.Amount + amount
	return this.saveFileUnpaidInfo(unpaidKey, info)
}

func (this *FileDB) DeleteFileUnpaid(id, walletAddress string, asset int32, amount uint64) error {
	unpaidKey := FileUnpaidKey(id, walletAddress, asset)
	info, err := this.getFileUnpaidInfo(unpaidKey)
	if err != nil {
		log.Debug("getFileUnpaidInfo err %s", err)
		return err
	}
	if info == nil {
		return fmt.Errorf("can't find file info of id %s, unpaidkey %s", id, unpaidKey)
	}
	if info.Amount > amount {
		info.Amount = info.Amount - amount
		return this.saveFileUnpaidInfo(unpaidKey, info)
	}
	return this.db.Delete([]byte(unpaidKey))
}

// IsInfoExist return a file is exist or not
func (this *FileDB) IsFileInfoExist(id string) bool {
	fi, err := this.GetFileInfo([]byte(id))
	if err != nil || fi == nil {
		return false
	}
	return true
}

// FileBlockHashes. return file block hashes
func (this *FileDB) FileBlockHashes(id string) []string {
	fi, err := this.GetFileInfo([]byte(id))
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
		items := strings.Split(str, "-")
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

	fi, err := this.GetFileInfo([]byte(id))
	if err != nil {
		return err
	}
	fi.SaveBlockCount++
	fiBuf, err := json.Marshal(fi)
	if err != nil {
		return err
	}
	this.db.NewBatch()
	log.Debugf("set block %s, len %d", blockKey, len(blockBuf))
	this.db.BatchPut([]byte(blockKey), blockBuf)
	this.db.BatchPut([]byte(progressKey), progressBuf)
	this.db.BatchPut([]byte(id), fiBuf)
	return this.db.BatchCommit()
}

//  IsBlockDownloaded
func (this *FileDB) IsBlockDownloaded(id, blockHashStr string, index uint32) bool {
	blockKey := BlockInfoKey(id, index, blockHashStr)
	block, err := this.getBlockInfo(blockKey)
	if block == nil || err != nil {
		return false
	}
	log.Debugf("is block downloaded block %v", block)
	if len(block.NodeList) == 0 {
		return false
	}
	return true
}

// IsFileDownloaded check if a downloaded file task has finished storing all blocks
func (this *FileDB) IsFileDownloaded(id string) bool {
	fi, err := this.GetFileInfo([]byte(id))
	if err != nil || fi == nil {
		return false
	}
	return fi.SaveBlockCount == fi.TotalBlockCount
}

// GetUndownloadedBlockInfo. check undownloaded block in-order
func (this *FileDB) GetUndownloadedBlockInfo(id, rootBlockHash string) (string, uint32, error) {
	fi, err := this.GetFileInfo([]byte(id))
	if err != nil || fi == nil {
		return "", 0, errors.New("file not found")
	}
	index := uint32(0)
	// TODO: improve performance
	var search func(blockHash string) (string, error)
	search = func(blockHash string) (string, error) {
		blockKey := BlockInfoKey(id, index, blockHash)
		block, err := this.getBlockInfo(blockKey)
		if err != nil {
			return "", err
		}
		if block == nil {
			return blockHash, nil
		}
		if len(block.LinkHashes) == 0 {
			return "", nil
		}
		oldIndex := index
		for _, hash := range block.LinkHashes {
			index++
			childBlockKey := BlockInfoKey(id, index, hash)
			childBlock, err := this.getBlockInfo(childBlockKey)
			if err != nil {
				return "", err
			}
			if childBlock == nil {
				return "", nil
			}
		}
		for _, hash := range block.LinkHashes {
			oldIndex++
			neighBorBlockKey := BlockInfoKey(id, index, hash)
			neighBlock, err := this.getBlockInfo(neighBorBlockKey)
			if err != nil {
				return "", err
			}
			for _, ch := range neighBlock.LinkHashes {
				index++
				ret, err := search(ch)
				if err != nil {
					return "", err
				}
				if len(ret) == 0 {
					continue
				}
				return ret, nil
			}
		}
		return "", nil
	}
	result, err := search(rootBlockHash)
	if err != nil {
		return "", index, err
	}
	return result, index, nil
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
	fileDownloadedKey := FileDownloadedKey(count)
	this.db.NewBatch()
	this.db.BatchPut([]byte(countKey), []byte(fmt.Sprintf("%d", count+1)))
	this.db.BatchPut([]byte(fileDownloadedKey), []byte(id))
	return this.db.BatchCommit()
}

// AllDownloadFiles. get all download files from db
func (this *FileDB) AllDownloadFiles() ([]string, error) {
	countKey := FileDownloadedCountKey()
	countBuf, err := this.db.Get([]byte(countKey))
	if err != nil {
		return nil, err
	}
	if len(countBuf) == 0 {
		return nil, nil
	}
	count, err := strconv.ParseUint(string(countBuf), 10, 32)
	if err != nil {
		return nil, err
	}
	all := make([]string, 0, count)
	for i := uint32(0); i < uint32(count); i++ {
		downloadedKey := FileDownloadedKey(i)
		idBuf, err := this.db.Get([]byte(downloadedKey))
		if err != nil || len(idBuf) == 0 {
			continue
		}
		fi, err := this.GetFileInfo(idBuf)
		if err != nil || fi == nil {
			continue
		}
		if len(fi.FileHash) == 0 {
			continue
		}
		all = append(all, fi.FileHash)
	}
	return all, nil
}

func (this *FileDB) AddShareTo(id, walletAddress string) error {
	shareKey := FileShareToKey(id, walletAddress)
	return this.db.Put([]byte(shareKey), []byte("true"))
}

func (this *FileDB) CanShareTo(id, walletAddress string, asset int32) (bool, error) {
	fi, err := this.GetFileInfo([]byte(id))
	if err != nil || fi == nil {
		return false, errors.New("file info not found")
	}
	shareKey := FileShareToKey(id, walletAddress)
	exist, err := this.db.Get([]byte(shareKey))
	if err != nil || len(exist) == 0 {
		return false, err
	}
	unpaid, err := this.getFileUnpaidInfo(FileUnpaidKey(id, walletAddress, asset))
	if err != nil {
		return false, err
	}
	if unpaid != nil && unpaid.Amount != 0 {
		return false, fmt.Errorf("can't share to: %s, has unpaid amount: %d", walletAddress, unpaid.Amount)
	}
	return true, nil
}

// getFileUploadInfo. helper function, get file upload info from db. if fileinfo not found, return (nil, nil)
func (this *FileDB) GetFileInfo(key []byte) (*FileInfo, error) {
	value, err := this.db.Get(key)
	if err != nil {
		if err != leveldb.ErrNotFound {
			return nil, err
		}
	}
	if len(value) == 0 {
		return nil, nil
	}

	info := &FileInfo{}
	err = json.Unmarshal(value, info)
	if err != nil {
		return nil, err
	}
	return info, nil
}

// saveFileInfo. helper function, put fileinfo to db
func (this *FileDB) saveFileInfo(info *FileInfo) error {
	info.UpdatedAt = uint64(time.Now().Unix())
	key := []byte(info.Id)
	buf, err := json.Marshal(info)
	if err != nil {
		return err
	}
	return this.db.Put(key, buf)
}

// getBlockInfo. helper function, get file upload info from db. if fileinfo not found, return (nil, nil)
func (this *FileDB) getBlockInfo(key string) (*BlockInfo, error) {
	value, err := this.db.Get([]byte(key))
	if err != nil {
		if err != leveldb.ErrNotFound {
			return nil, err
		}
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
	if err != nil {
		if err != leveldb.ErrNotFound {
			return nil, err
		}
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
	if err != nil {
		if err != leveldb.ErrNotFound {
			return nil, err
		}
	}
	if len(value) == 0 {
		return nil, nil
	}
	info := &FileDownloadUnPaid{}
	err = json.Unmarshal(value, info)
	if err != nil {
		return nil, err
	}
	return info, nil
}

func (this *FileDB) saveFileUnpaidInfo(key string, info *FileDownloadUnPaid) error {
	buf, err := json.Marshal(info)
	if err != nil {
		return err
	}
	return this.db.Put([]byte(key), buf)
}
