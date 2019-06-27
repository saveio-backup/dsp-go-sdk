package store

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/saveio/themis/common/log"
	"github.com/syndtr/goleveldb/leveldb"
)

// FileDB. implement a db storage for save information of sending/downloading/downloaded files
type FileDB struct {
	db *LevelDBStore
}

type FileInfoType int

const (
	FileInfoTypeUpload FileInfoType = iota
	FileInfoTypeDownload
	FileInfoTypeShare
)

type linkInfo struct {
	Hash string `json:"hash"`
	Name string `json:"name"`
	Size uint64 `json:"size"`
}

// blockInfo record a block infomation of a file
type blockInfo struct {
	Hash           string               `json:"hash"`                          // block  hash
	Index          uint32               `json:"index"`                         // block index of file
	State          uint8                `json:"state,omitempty"`               // Block state. 0 => unstored, 1 => stored
	DataSize       uint64               `json:"data_size"`                     // block raw data size
	DataOffset     uint64               `json:"data_offset"`                   // block raw data offset
	FileSize       uint64               `json:"file_size"`                     // file size
	NodeWalletAddr string               `json:"node_wallet_address,omitempty"` // block wallet address
	NodeList       []string             `json:"node_list,omitempty"`           // uploaded node list
	LinkHashes     []string             `json:"link_hashes,omitempty"`         // child link hashes slice
	BlockSizes     []uint64             `json:"block_sizes"`                   // child block raw data size slice
	LinkInfos      map[string]*linkInfo `json:"link_info"`                     // child link info
}

type Payment struct {
	WalletAddress string `json:"wallet_address"`
	Asset         int32  `json:"asset"`
	Amount        uint64 `json:"amount"`
	PaymentId     int32  `json:"paymentId"`
}

// fileInfo keep all blocks infomation and the prove private key for generating tags
type FileInfo struct {
	Id           string                `json:"id"`
	Tx           string                `json:"tx,omitempty"`
	FileName     string                `json:"name"`
	Blocks       map[string]*blockInfo `json:"blocks"`
	BlockHashes  []string              `json:"block_hashes,omitempty"`
	Progress     map[string]uint64     `json:"progress"`
	ProvePrivKey []byte                `json:"prove_private_key,omitempty"`
	Prefix       string                `json:"prefix"`
	UnitPrice    map[int32]uint64      `json:"uint_price"`
	Unpaid       map[string]*Payment   `json:"unpaid"`
	Sent         uint64                `json:"sent"`
	ShareTo      map[string]struct{}   `json:"shareto"`
	CreatedAt    uint64                `json:"createdAt"`
	BlockOffset  map[uint32]int64      `json:"block_offset"`
}

func NewFileDB(db *LevelDBStore) *FileDB {
	return &FileDB{
		db: db,
	}
}

func (this *FileDB) Close() error {
	return this.db.Close()
}

func (this *FileDB) NewFileUploadInfo(id string) error {
	fi := &FileInfo{
		Id:        id,
		CreatedAt: uint64(time.Now().Unix()),
	}
	return this.putFileInfo([]byte(id), fi)
}

func (this *FileDB) SetIdIndex(id, key string) error {
	return this.db.Put([]byte(key), []byte(id))
}

func (this *FileDB) GetId(key string) (string, error) {
	id, err := this.db.Get([]byte(key))
	if err != nil {
		return "", err
	}
	return string(id), nil
}

// PutFileUploadInfo. put info when upload file
func (this *FileDB) PutFileUploadInfo(tx, fileInfoKey string, provePrivKey []byte) error {
	key := []byte(fileInfoKey)
	fi, err := this.GetFileInfo(key)
	if err != nil {
		return err
	}
	if fi == nil {
		fi = &FileInfo{
			CreatedAt: uint64(time.Now().Unix()),
		}
	}
	fi.Tx = tx
	fi.ProvePrivKey = provePrivKey
	fi.Blocks = make(map[string]*blockInfo, 0)
	fi.BlockOffset = make(map[uint32]int64, 0)
	return this.putFileInfo(key, fi)
}

// DeleteFileUploadInfo. delete file upload info from db
func (this *FileDB) DeleteFileUploadInfo(fileInfoKey string) error {
	return this.db.Delete([]byte(fileInfoKey))
}

// AddUploadedBlock. add a uploaded block into db
func (this *FileDB) AddUploadedBlock(fileInfoKey, blockHashStr, nodeAddr string, index uint32, offset int64) error {
	fi, err := this.GetFileInfo([]byte(fileInfoKey))
	if err != nil {
		return err
	}
	if fi == nil {
		return errors.New("file info not found")
	}
	blockKey := uploadFileBlockKey(fileInfoKey, blockHashStr, index)
	block := fi.Blocks[blockKey]
	if block == nil {
		block = &blockInfo{
			Hash:     blockHashStr,
			Index:    index,
			NodeList: make([]string, 0),
		}
	}
	block.NodeList = append(block.NodeList, nodeAddr)
	if fi.Blocks == nil {
		fi.Blocks = make(map[string]*blockInfo, 0)
	}
	fi.Blocks[blockKey] = block
	if fi.Progress == nil {
		fi.Progress = make(map[string]uint64, 0)
	}
	fi.Progress[nodeAddr]++
	fi.Sent++
	oldOffset, ok := fi.BlockOffset[uint32(index)]
	if !ok || oldOffset < offset {
		fi.BlockOffset[uint32(index)] = offset
		log.Debugf("set offset for %d %d, ok: %t, old %v", index, offset, ok, oldOffset)
	}
	return this.putFileInfo([]byte(fileInfoKey), fi)
}

func (this *FileDB) GetBlockOffset(fileInfoKey string, index uint32) int64 {
	fi, err := this.GetFileInfo([]byte(fileInfoKey))
	if err != nil {
		return 0
	}
	if fi == nil {
		return 0
	}
	if index == 0 {
		return 0
	}
	return fi.BlockOffset[index-1]
}

// IsBlockUploaded. check if a block is uploaded
func (this *FileDB) IsBlockUploaded(fileinfoKey, blockHashStr, nodeAddr string, index uint32) bool {
	fi, err := this.GetFileInfo([]byte(fileinfoKey))
	if err != nil || fi == nil {
		return false
	}
	blockKey := uploadFileBlockKey(fileinfoKey, blockHashStr, index)
	block := fi.Blocks[blockKey]
	if block == nil {
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
func (this *FileDB) GetUploadedBlockNodeList(fileinfoKey, blockHashStr string, index uint32) []string {
	fi, err := this.GetFileInfo([]byte(fileinfoKey))
	if err != nil || fi == nil {
		return nil
	}
	blockKey := uploadFileBlockKey(fileinfoKey, blockHashStr, index)
	block := fi.Blocks[blockKey]
	if block == nil {
		return nil
	}
	return block.NodeList
}

// GetFileProvePrivKey
func (this *FileDB) GetFileProvePrivKey(fileInfoKey string) []byte {
	fi, err := this.GetFileInfo([]byte(fileInfoKey))
	if err != nil || fi == nil {
		return nil
	}
	return fi.ProvePrivKey
}

// GetStoreFileTx
func (this *FileDB) GetStoreFileTx(fileInfoKey string) string {
	fi, err := this.GetFileInfo([]byte(fileInfoKey))
	if err != nil || fi == nil {
		return ""
	}
	return fi.Tx
}

// UploadedBlockCount
func (this *FileDB) UploadedBlockCount(fileInfoKey string) uint64 {
	fi, err := this.GetFileInfo([]byte(fileInfoKey))
	if err != nil || fi == nil {
		return 0
	}
	return fi.Sent
}

// AddFileBlockHashes add all blocks' hash, using for detect whether the node has stored the file
func (this *FileDB) AddFileBlockHashes(fileInfoKey string, blocks []string) error {
	fi := &FileInfo{
		BlockHashes: blocks,
		Blocks:      make(map[string]*blockInfo, 0),
		CreatedAt:   uint64(time.Now().Unix()),
	}
	return this.putFileInfo([]byte(fileInfoKey), fi)
}

// AddFilePrefix add file prefix
func (this *FileDB) AddFilePrefix(fileInfoKey, prefix string) error {
	fi, err := this.GetFileInfo([]byte(fileInfoKey))
	if err != nil {
		return err
	}
	fi.Prefix = prefix
	return this.putFileInfo([]byte(fileInfoKey), fi)
}

// AddFileName add file name
func (this *FileDB) AddFileName(fileInfoKey, name string) error {
	fi, err := this.GetFileInfo([]byte(fileInfoKey))
	if err != nil {
		return err
	}
	fi.FileName = name
	return this.putFileInfo([]byte(fileInfoKey), fi)
}

func (this *FileDB) AddDownloadFileUnpaid(fileInfoKey, walletAddress string, asset int32, amount uint64) error {
	return this.addFileUnpaid(fileInfoKey, walletAddress, asset, amount)
}

func (this *FileDB) DeleteDownloadFileUnpaid(fileInfoKey, walletAddress string, asset int32, amount uint64) error {
	return this.deleteFileUnpaid(fileInfoKey, walletAddress, asset, amount)
}

// IsDownloadInfoExist return a file is exist or not
func (this *FileDB) IsDownloadInfoExist(fileInfoKey string) bool {
	fi, err := this.GetFileInfo([]byte(fileInfoKey))
	if err != nil || fi == nil {
		return false
	}
	return true
}

// FileBlockHashes. return file block hashes
func (this *FileDB) FileBlockHashes(fileInfoKey string) []string {
	fi, err := this.GetFileInfo([]byte(fileInfoKey))
	if err != nil || fi == nil {
		return nil
	}
	return fi.BlockHashes
}

// FilePrefix. return file prefix
func (this *FileDB) FilePrefix(fileInfoKey string) string {
	fi, err := this.GetFileInfo([]byte(fileInfoKey))
	if err != nil || fi == nil {
		return ""
	}
	return fi.Prefix
}

// FileName. return file name
func (this *FileDB) FileName(fileInfoKey string) string {
	fi, err := this.GetFileInfo([]byte(fileInfoKey))
	if err != nil || fi == nil {
		return ""
	}
	return fi.FileName
}

// FileProgress. return each node count progress
func (this *FileDB) FileProgress(fileInfoKey string) map[string]uint64 {
	fi, err := this.GetFileInfo([]byte(fileInfoKey))
	if err != nil || fi == nil {
		return nil
	}
	return fi.Progress
}

//  SetBlockStored set the flag of store state
func (this *FileDB) SetBlockDownloaded(fileInfoKey, blockHashStr, nodeAddr string, index uint32, offset int64, links []string) error {
	fi, err := this.GetFileInfo([]byte(fileInfoKey))
	if err != nil {
		return err
	}
	if fi == nil {
		return errors.New("file info not found")
	}
	recv := &blockInfo{
		Index:      index,
		Hash:       blockHashStr,
		State:      1,
		LinkHashes: make([]string, 0),
		DataOffset: uint64(offset),
	}
	for _, l := range links {
		recv.LinkHashes = append(recv.LinkHashes, l)
	}
	blockKey := string(downloadFileBlockKey(fileInfoKey, blockHashStr, index))
	if fi.Blocks == nil {
		fi.Blocks = make(map[string]*blockInfo, 0)
	}
	fi.Blocks[blockKey] = recv
	if fi.Progress == nil {
		fi.Progress = make(map[string]uint64, 0)
	}
	fi.Progress[nodeAddr]++
	return this.putFileInfo([]byte(fileInfoKey), fi)
}

//  IsBlockDownloaded
func (this *FileDB) IsBlockDownloaded(fileInfoKey, blockHashStr string, index uint32) bool {
	fi, err := this.GetFileInfo([]byte(fileInfoKey))
	if err != nil || fi == nil {
		return false
	}
	blockKey := string(downloadFileBlockKey(fileInfoKey, blockHashStr, index))
	_, ok := fi.Blocks[blockKey]
	return ok
}

//  BlockOffset
func (this *FileDB) BlockOffset(fileInfoKey, blockHashStr string, index uint32) (uint64, error) {
	fi, err := this.GetFileInfo([]byte(fileInfoKey))
	if err != nil || fi == nil {
		return 0, errors.New("file not found")
	}
	blockKey := string(downloadFileBlockKey(fileInfoKey, blockHashStr, index))
	v, ok := fi.Blocks[blockKey]
	if !ok {
		return 0, errors.New("block not found")
	}
	return v.DataOffset, nil
}

// IsFileDownloaded check if a downloaded file task has finished storing all blocks
func (this *FileDB) IsFileDownloaded(fileInfoKey string) bool {
	fi, err := this.GetFileInfo([]byte(fileInfoKey))
	if err != nil || fi == nil {
		return false
	}
	return len(fi.BlockHashes) == len(fi.Blocks)
}

// GetUndownloadedBlockInfo. check undownloaded block in-order
func (this *FileDB) GetUndownloadedBlockInfo(fileInfoKey, rootBlockHash string) (string, uint32, error) {
	fi, err := this.GetFileInfo([]byte(fileInfoKey))
	if err != nil || fi == nil {
		return "", 0, errors.New("file not found")
	}
	index := uint32(0)
	var search func(bh string) string
	search = func(bh string) string {
		blockKey := string(downloadFileBlockKey(fileInfoKey, bh, index))
		downloaded, ok := fi.Blocks[blockKey]
		if !ok {
			return bh
		}
		if len(downloaded.LinkHashes) == 0 {
			return ""
		}
		oldIndex := index
		for _, hash := range downloaded.LinkHashes {
			index++
			blockKey := string(downloadFileBlockKey(fileInfoKey, hash, index))
			_, ok := fi.Blocks[blockKey]
			if !ok {
				return hash
			}
		}
		for _, hash := range downloaded.LinkHashes {
			oldIndex++
			blockKey := string(downloadFileBlockKey(fileInfoKey, hash, oldIndex))
			downloaded := fi.Blocks[blockKey]
			for _, ch := range downloaded.LinkHashes {
				index++
				ret := search(ch)
				if len(ret) == 0 {
					continue
				}
				return ret
			}
		}
		return ""
	}
	return search(rootBlockHash), index, nil
}

// DeleteFileDownloadInfo. delete file download info from db
func (this *FileDB) DeleteFileDownloadInfo(fileInfoKey string) error {
	return this.db.Delete([]byte(fileInfoKey))
}

// AllDownloadFiles. get all download files from db
func (this *FileDB) AllDownloadFiles() ([]string, error) {
	prefix := fmt.Sprintf("type=%d&hash=", FileInfoTypeDownload)
	keys, err := this.db.QueryStringKeysByPrefix([]byte(prefix))
	if err != nil {
		return nil, err
	}
	files := make([]string, 0)
	for _, k := range keys {
		files = append(files, k[len(prefix):])
	}
	return files, nil
}

func (this *FileDB) NewFileShareInfo(fileInfoKey string) error {
	fi := &FileInfo{
		Unpaid:    make(map[string]*Payment, 0),
		ShareTo:   make(map[string]struct{}, 0),
		CreatedAt: uint64(time.Now().Unix()),
	}
	return this.putFileInfo([]byte(fileInfoKey), fi)
}

func (this *FileDB) IsShareInfoExists(fileInfoKey string) bool {
	fi, err := this.GetFileInfo([]byte(fileInfoKey))
	if err != nil || fi == nil {
		return false
	}
	return true
}

func (this *FileDB) AddShareTo(fileInfoKey, walletAddress string) error {
	fi, err := this.GetFileInfo([]byte(fileInfoKey))
	if err != nil || fi == nil {
		return errors.New("file info not found")
	}
	fi.ShareTo[walletAddress] = struct{}{}
	return this.putFileInfo([]byte(fileInfoKey), fi)
}

func (this *FileDB) AddShareFileUnpaid(fileInfoKey, walletAddress string, asset int32, amount uint64) error {
	return this.addFileUnpaid(fileInfoKey, walletAddress, asset, amount)
}

func (this *FileDB) DeleteShareFileUnpaid(fileInfoKey, walletAddress string, asset int32, amount uint64) error {
	return this.deleteFileUnpaid(fileInfoKey, walletAddress, asset, amount)
}

func (this *FileDB) CanShareTo(fileInfoKey, walletAddress string) (bool, error) {
	fi, err := this.GetFileInfo([]byte(fileInfoKey))
	if err != nil || fi == nil {
		return false, errors.New("file info not found")
	}
	_, exist := fi.ShareTo[walletAddress]
	if !exist {
		return false, errors.New("unknown wallet address")
	}
	info, ok := fi.Unpaid[walletAddress]
	if !ok || info.Amount == 0 {
		return true, nil
	}
	return false, errors.New("has unpaid amount")
}

func (this *FileDB) DeleteFileShareInfo(fileInfoKey string) error {
	return this.db.Delete([]byte(fileInfoKey))
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

// putFileInfo. helper function, put fileinfo to db
func (this *FileDB) putFileInfo(key []byte, info *FileInfo) error {
	buf, err := json.Marshal(info)
	if err != nil {
		return err
	}
	return this.db.Put(key, buf)
}

func (this *FileDB) addFileUnpaid(fileInfoKey, walletAddress string, asset int32, amount uint64) error {
	fi, err := this.GetFileInfo([]byte(fileInfoKey))
	if err != nil || fi == nil {
		return errors.New("file info not found")
	}
	if fi.Unpaid == nil {
		fi.Unpaid = make(map[string]*Payment, 0)
	}
	fi.Unpaid[walletAddress] = &Payment{
		WalletAddress: walletAddress,
		Asset:         asset,
		Amount:        amount,
	}
	return this.putFileInfo([]byte(fileInfoKey), fi)
}

func (this *FileDB) deleteFileUnpaid(fileInfoKey, walletAddress string, asset int32, amount uint64) error {
	fi, err := this.GetFileInfo([]byte(fileInfoKey))
	if err != nil || fi == nil {
		return errors.New("file info not found")
	}
	if fi.Unpaid == nil {
		return nil
	}
	v, ok := fi.Unpaid[walletAddress]
	if !ok {
		return errors.New("unpaid not found")
	}
	if v.Amount <= amount {
		delete(fi.Unpaid, walletAddress)
	} else {
		v.Amount = v.Amount - amount
	}
	return this.putFileInfo([]byte(fileInfoKey), fi)
}

// uploadFileBlockKey. key constructor
func uploadFileBlockKey(fileInfoKey string, blockHashStr string, index uint32) string {
	return fmt.Sprintf("key=%s&block=%s&index=%d", fileInfoKey, blockHashStr, index)
}

// downloadFileBlockKey. key constructor
func downloadFileBlockKey(fileInfoKey string, blockHashStr string, index uint32) string {
	return fmt.Sprintf("key=%s&block=%s&index=%d", fileInfoKey, blockHashStr, index)
}

// shareFileBlockKey. key constructor
func shareFileBlockKey(fileInfoKey string, blockHashStr string, index uint32) string {
	return fmt.Sprintf("key=%s&block=%s&index=%d", fileInfoKey, blockHashStr, index)
}
