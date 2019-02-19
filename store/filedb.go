package store

import (
	"encoding/json"
	"errors"
	"fmt"

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

// fileInfo keep all blocks infomation and the prove private key for generating tags
type fileInfo struct {
	Tx           string                `json:"tx,omitempty"`
	Blocks       map[string]*blockInfo `json:"blocks"`
	BlockHashes  []string              `json:"block_hashes,omitempty"`
	Progress     map[string]uint64     `json:"progress"`
	ProvePrivKey []byte                `json:"prove_private_key,omitempty"`
	Prefix       string                `json:"prefix"`
}

func NewFileDB(dbPath string) *FileDB {
	db, err := NewLevelDBStore(dbPath)
	if err != nil {
		return nil
	}
	if db == nil {
		return nil
	}
	return &FileDB{
		db: db,
	}
}

// PutFileUploadInfo. put info when upload file
func (this *FileDB) PutFileUploadInfo(tx, fileHashStr string, provePrivKey []byte) error {
	fi, err := this.getFileInfo(fileHashStr, FileInfoTypeUpload)
	if err != nil {
		return err
	}
	if fi == nil {
		fi = &fileInfo{}
	}
	fi.Tx = tx
	fi.ProvePrivKey = provePrivKey
	fi.Blocks = make(map[string]*blockInfo, 0)
	return this.putFileInfo(fileHashStr, fi, FileInfoTypeUpload)
}

// DeleteFileUploadInfo. delete file upload info from db
func (this *FileDB) DeleteFileUploadInfo(fileHashStr string) error {
	return this.db.Delete(uploadFileInfoKey(fileHashStr))
}

// AddUploadedBlock. add a uploaded block into db
func (this *FileDB) AddUploadedBlock(fileHashStr, blockHashStr, nodeAddr string, index uint32) error {
	fi, err := this.getFileInfo(fileHashStr, FileInfoTypeUpload)
	if err != nil {
		return err
	}
	if fi == nil {
		return errors.New("file info not found")
	}
	blockKey := uploadFileBlockKey(fileHashStr, blockHashStr, index)
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
	return this.putFileInfo(fileHashStr, fi, FileInfoTypeUpload)
}

// IsBlockUploaded. check if a block is uploaded
func (this *FileDB) IsBlockUploaded(fileHashStr, blockHashStr, nodeAddr string, index uint32) bool {
	fi, err := this.getFileInfo(fileHashStr, FileInfoTypeUpload)
	if err != nil || fi == nil {
		return false
	}
	blockKey := uploadFileBlockKey(fileHashStr, blockHashStr, index)
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
func (this *FileDB) GetUploadedBlockNodeList(fileHashStr, blockHashStr string, index uint32) []string {
	fi, err := this.getFileInfo(fileHashStr, FileInfoTypeUpload)
	if err != nil || fi == nil {
		return nil
	}
	blockKey := uploadFileBlockKey(fileHashStr, blockHashStr, index)
	block := fi.Blocks[blockKey]
	if block == nil {
		return nil
	}
	return block.NodeList
}

// GetFileProvePrivKey
func (this *FileDB) GetFileProvePrivKey(fileHashStr string) []byte {
	fi, err := this.getFileInfo(fileHashStr, FileInfoTypeUpload)
	if err != nil || fi == nil {
		return nil
	}
	return fi.ProvePrivKey
}

// GetStoreFileTx
func (this *FileDB) GetStoreFileTx(fileHashStr string) string {
	fi, err := this.getFileInfo(fileHashStr, FileInfoTypeUpload)
	if err != nil || fi == nil {
		return ""
	}
	return fi.Tx
}

// UploadedBlockCount
func (this *FileDB) UploadedBlockCount(fileHashStr string) int {
	fi, err := this.getFileInfo(fileHashStr, FileInfoTypeUpload)
	if err != nil || fi == nil {
		return 0
	}
	return len(fi.Blocks)
}

// AddFileBlockHashes add all blocks' hash, using for detect whether the node has stored the file
func (this *FileDB) AddFileBlockHashes(fileHashStr string, blocks []string) error {
	fi := &fileInfo{
		BlockHashes: blocks,
		Blocks:      make(map[string]*blockInfo, 0),
	}
	return this.putFileInfo(fileHashStr, fi, FileInfoTypeDownload)
}

// AddFilePrefix add file prefix
func (this *FileDB) AddFilePrefix(fileHashStr, prefix string) error {
	fi, err := this.getFileInfo(fileHashStr, FileInfoTypeDownload)
	if err != nil {
		return err
	}
	fi.Prefix = prefix
	return this.putFileInfo(fileHashStr, fi, FileInfoTypeDownload)
}

// IsDownloadInfoExist return a file is exist or not
func (this *FileDB) IsDownloadInfoExist(fileHashStr string) bool {
	fi, err := this.getFileInfo(fileHashStr, FileInfoTypeDownload)
	if err != nil || fi == nil {
		return false
	}
	return true
}

// FileBlockHashes. return file block hashes
func (this *FileDB) FileBlockHashes(fileHashStr string) []string {
	fi, err := this.getFileInfo(fileHashStr, FileInfoTypeDownload)
	if err != nil || fi == nil {
		return nil
	}
	return fi.BlockHashes
}

// FilePrefix. return file prefix
func (this *FileDB) FilePrefix(fileHashStr string) string {
	fi, err := this.getFileInfo(fileHashStr, FileInfoTypeDownload)
	if err != nil || fi == nil {
		return ""
	}
	return fi.Prefix
}

// FileProgress. return each node count progress
func (this *FileDB) FileProgress(fileHashStr string, fileType FileInfoType) map[string]uint64 {
	fi, err := this.getFileInfo(fileHashStr, fileType)
	if err != nil || fi == nil {
		return nil
	}
	return fi.Progress
}

//  SetBlockStored set the flag of store state
func (this *FileDB) SetBlockDownloaded(fileHashStr, blockHashStr, nodeAddr string, index uint32, offset int64, links []string) error {
	fi, err := this.getFileInfo(fileHashStr, FileInfoTypeDownload)
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
	blockKey := string(downloadFileBlockKey(fileHashStr, blockHashStr, index))
	if fi.Blocks == nil {
		fi.Blocks = make(map[string]*blockInfo, 0)
	}
	fi.Blocks[blockKey] = recv
	if fi.Progress == nil {
		fi.Progress = make(map[string]uint64, 0)
	}
	fi.Progress[nodeAddr]++
	return this.putFileInfo(fileHashStr, fi, FileInfoTypeDownload)
}

//  IsBlockDownloaded
func (this *FileDB) IsBlockDownloaded(fileHashStr, blockHashStr string, index uint32) bool {
	fi, err := this.getFileInfo(fileHashStr, FileInfoTypeDownload)
	if err != nil || fi == nil {
		return false
	}
	blockKey := string(downloadFileBlockKey(fileHashStr, blockHashStr, index))
	_, ok := fi.Blocks[blockKey]
	return ok
}

//  BlockOffset
func (this *FileDB) BlockOffset(fileHashStr, blockHashStr string, index uint32) (uint64, error) {
	fi, err := this.getFileInfo(fileHashStr, FileInfoTypeDownload)
	if err != nil || fi == nil {
		return 0, errors.New("file not found")
	}
	blockKey := string(downloadFileBlockKey(fileHashStr, blockHashStr, index))
	v, ok := fi.Blocks[blockKey]
	if !ok {
		return 0, errors.New("block not found")
	}
	return v.DataOffset, nil
}

// IsFileDownloaded check if a downloaded file task has finished storing all blocks
func (this *FileDB) IsFileDownloaded(fileHashStr string) bool {
	fi, err := this.getFileInfo(fileHashStr, FileInfoTypeDownload)
	if err != nil || fi == nil {
		return false
	}
	return len(fi.BlockHashes) == len(fi.Blocks)
}

// GetUndownloadedBlockInfo. check undownloaded block in-order
func (this *FileDB) GetUndownloadedBlockInfo(fileHashStr, rootBlockHash string) (string, uint32, error) {
	fi, err := this.getFileInfo(fileHashStr, FileInfoTypeDownload)
	if err != nil || fi == nil {
		return "", 0, errors.New("file not found")
	}
	index := uint32(0)
	var search func(bh string) string
	search = func(bh string) string {
		blockKey := string(downloadFileBlockKey(fileHashStr, bh, index))
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
			blockKey := string(downloadFileBlockKey(fileHashStr, hash, index))
			_, ok := fi.Blocks[blockKey]
			if !ok {
				return hash
			}
		}
		for _, hash := range downloaded.LinkHashes {
			oldIndex++
			blockKey := string(downloadFileBlockKey(fileHashStr, hash, oldIndex))
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
func (this *FileDB) DeleteFileDownloadInfo(fileHashStr string) error {
	return this.db.Delete(downloadFileInfoKey(fileHashStr))
}

// getFileUploadInfo. helper function, get file upload info from db. if fileinfo not found, return (nil, nil)
func (this *FileDB) getFileInfo(fileHashStr string, fileType FileInfoType) (*fileInfo, error) {
	var key []byte
	switch fileType {
	case FileInfoTypeUpload:
		key = uploadFileInfoKey(fileHashStr)
	case FileInfoTypeDownload:
		key = downloadFileInfoKey(fileHashStr)
	}
	value, err := this.db.Get(key)
	if err != nil {
		if err != leveldb.ErrNotFound {
			return nil, err
		}
	}
	if len(value) == 0 {
		return nil, nil
	}

	info := &fileInfo{}
	err = json.Unmarshal(value, info)
	if err != nil {
		return nil, err
	}
	return info, nil
}

// putFileInfo. helper function, put fileinfo to db
func (this *FileDB) putFileInfo(fileHashStr string, info *fileInfo, fileType FileInfoType) error {
	var key []byte
	switch fileType {
	case FileInfoTypeUpload:
		key = uploadFileInfoKey(fileHashStr)
	case FileInfoTypeDownload:
		key = downloadFileInfoKey(fileHashStr)
	}
	buf, err := json.Marshal(info)
	if err != nil {
		return err
	}
	return this.db.Put(key, buf)
}

// uploadFileInfoKey. key constructor
func uploadFileInfoKey(fileHashStr string) []byte {
	return []byte(fmt.Sprintf("type=%d&hash=%s", FileInfoTypeUpload, fileHashStr))
}

// uploadFileBlockKey. key constructor
func uploadFileBlockKey(fileHashStr string, blockHashStr string, index uint32) string {
	return fmt.Sprintf("type=%d&file=%s&block=%s&index=%d", FileInfoTypeUpload, fileHashStr, blockHashStr, index)
}

// downloadFileInfoKey. key constructor
func downloadFileInfoKey(fileHashStr string) []byte {
	return []byte(fmt.Sprintf("type=%d&hash=%s", FileInfoTypeDownload, fileHashStr))
}

// downloadFileBlockKey. key constructor
func downloadFileBlockKey(fileHashStr string, blockHashStr string, index uint32) string {
	return fmt.Sprintf("type=%d&file=%s&block=%s&index=%d", FileInfoTypeDownload, fileHashStr, blockHashStr, index)
}
