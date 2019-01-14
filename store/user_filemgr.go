package store

import (
	"encoding/json"
	"errors"
	"io"
	"io/ioutil"
	"os"
	"sync"
	"time"
)

const (
	STORE_FILES_INFO_DIR    = "./filestore-info/" // temp directory, using for keep sending files infomation
	DOWNLOAD_FILES_INFO_DIR = "./download-info/"  // downloads file directory, using for store temp downloaded files infomation
)

type UserFileMgr struct {
	UserStoreFileMgr
	UserReadFileMgr
}

// NewUserFileMgr init a new file manager
func NewUserFileMgr() *UserFileMgr {
	fm := &UserFileMgr{}
	fm.readFileInfos = make(map[string]*fmFileInfo, 0)
	fm.fileInfos = make(map[string]*fmFileInfo, 0)
	return fm
}

// fileBlockInfo record a block infomation of a file
// including block hash string, a timestamp of sending this block to remote peer
// a timestamp of receiving a block from remote peer
type fmBlockInfo struct {
	Hash          string `json:"hash"`
	Index         uint32 `json:"index"`
	SendTimestamp int64  `json:"sendts"`
	RecvTimestamp int64  `json:"recvts"`
	// NodeAddr       string   `json:"node_address"`
	NodeWalletAddr string   `json:"node_wallet_address"`
	NodeList       []string `json:"node_list"`
	ChildHashStrs  []string `json:"childs"`
}

// NewFmBlockInfo new file manager block info
func NewFmBlockInfo(hash string) *fmBlockInfo {
	return &fmBlockInfo{
		Hash: hash,
	}
}

// fmFileInfo keep all blocks infomation and the prove private key for generating tags
type fmFileInfo struct {
	Tx           string         `json:"tx"`
	Blocks       []*fmBlockInfo `json:"blocks"`
	ProvePrivKey []byte         `json:"prove_private_key"`
}

// NewFmFileInfo new fm file info
func NewFmFileInfo(provePrivKey []byte) *fmFileInfo {
	blks := make([]*fmBlockInfo, 0)
	return &fmFileInfo{
		Blocks:       blks,
		ProvePrivKey: provePrivKey,
	}
}

type UserStoreFileMgr struct {
	lock      sync.RWMutex           // Lock
	fileInfos map[string]*fmFileInfo // FileName <=> FileInfo
}

// NewStoreFile init store file info
func (sfm *UserStoreFileMgr) NewStoreFile(tx, fileHashStr string, provePrivKey []byte) error {
	sfm.lock.Lock()
	defer sfm.lock.Unlock()
	if _, err := os.Stat(STORE_FILES_INFO_DIR); os.IsNotExist(err) {
		err = os.MkdirAll(STORE_FILES_INFO_DIR, 0755)
		if err != nil {
			return err
		}
	}

	data, _ := ioutil.ReadFile(storeFilePath(fileHashStr))
	if len(data) == 0 {
		sfm.fileInfos[fileHashStr] = NewFmFileInfo(provePrivKey)
		sfm.fileInfos[fileHashStr].Tx = tx
		buf, err := json.Marshal(sfm.fileInfos[fileHashStr])
		if err != nil {
			return err
		}
		return ioutil.WriteFile(storeFilePath(fileHashStr), buf, 0666)
	}

	fi := &fmFileInfo{}
	err := json.Unmarshal(data, fi)
	if err != nil {
		return err
	}
	sfm.fileInfos[fileHashStr] = fi
	return nil
}

func (sfm *UserStoreFileMgr) DelStoreFileInfo(fileHashStr string) error {
	sfm.lock.Lock()
	defer sfm.lock.Unlock()
	delete(sfm.fileInfos, fileHashStr)
	err := os.Remove(storeFilePath(fileHashStr))
	if err != nil {
		return err
	}
	files, err := ioutil.ReadDir(STORE_FILES_INFO_DIR)
	if err == nil && len(files) == 0 {
		return os.Remove(STORE_FILES_INFO_DIR)
	}
	return nil
}

// AddStoredBlock add a new stored block info to map and local storage
func (sfm *UserStoreFileMgr) AddStoredBlock(fileHashStr, blockHash, nodeAddr string, index uint32) error {
	sfm.lock.Lock()
	defer sfm.lock.Unlock()
	fi, ok := sfm.fileInfos[fileHashStr]
	if !ok {
		return errors.New("file info not found")
	}
	blks := fi.Blocks
	oldBlks := fi.Blocks
	var block *fmBlockInfo
	for _, blk := range blks {
		if blk.Hash == blockHash && blk.Index == index {
			block = blk
		}
	}
	if block == nil {
		block = NewFmBlockInfo(blockHash)
		block.SendTimestamp = time.Now().Unix()
		block.Index = index
	}
	block.NodeList = append(block.NodeList, nodeAddr)
	blks = append(blks, block)
	fi.Blocks = blks
	buf, err := json.Marshal(fi)
	if err != nil {
		fi.Blocks = oldBlks
		return err
	}
	return ioutil.WriteFile(storeFilePath(fileHashStr), buf, 0666)
}

func (sfm *UserStoreFileMgr) GetStoredBlockNodeList(fileHashStr, blockHashStr string) []string {
	sfm.lock.RLock()
	defer sfm.lock.RUnlock()
	fi, ok := sfm.fileInfos[fileHashStr]
	if !ok {
		data, _ := ioutil.ReadFile(storeFilePath(fileHashStr))
		if len(data) == 0 {
			return nil
		}
		err := json.Unmarshal(data, &fi)
		if err != nil || fi == nil {
			return nil
		}
	}
	if len(fi.Blocks) == 0 {
		return nil
	}
	for _, b := range fi.Blocks {
		if b.Hash == blockHashStr {
			return b.NodeList
		}
	}
	return nil
}

// IsBlockStored check if block has stored
func (sfm *UserStoreFileMgr) IsBlockStored(fileHashStr, blockHash, nodeAddr string, index uint32) bool {
	sfm.lock.RLock()
	defer sfm.lock.RUnlock()
	fi, ok := sfm.fileInfos[fileHashStr]
	if !ok || len(fi.Blocks) == 0 {
		return false
	}
	for _, b := range fi.Blocks {
		if b.Hash == blockHash && b.Index == index {
			for _, addr := range b.NodeList {
				if nodeAddr == addr {
					return true
				}
			}
			return false
		}
	}
	return false
}

func (sfm *UserStoreFileMgr) StoredBlockCount(fileHashStr string) int {
	sfm.lock.RLock()
	defer sfm.lock.RUnlock()
	fi := sfm.fileInfos[fileHashStr]
	if fi == nil {
		return 0
	}
	return len(fi.Blocks)
}

func (sfm *UserStoreFileMgr) GetFileProvePrivKey(fileHashStr string) []byte {
	sfm.lock.RLock()
	defer sfm.lock.RUnlock()
	fi := sfm.fileInfos[fileHashStr]
	if fi == nil {
		return nil
	}
	return fi.ProvePrivKey
}

func (sfm *UserStoreFileMgr) GetStoreFileTx(fileHashStr string) string {
	sfm.lock.RLock()
	defer sfm.lock.RUnlock()
	fi := sfm.fileInfos[fileHashStr]
	if fi == nil {
		return ""
	}
	return fi.Tx
}

type UserReadFileMgr struct {
	readFileInfos map[string]*fmFileInfo
	lock          sync.RWMutex
}

func (rfm *UserReadFileMgr) NewReadFile(tx, fileHashStr string) error {
	rfm.lock.Lock()
	defer rfm.lock.Unlock()
	if _, err := os.Stat(DOWNLOAD_FILES_INFO_DIR); os.IsNotExist(err) {
		err = os.MkdirAll(DOWNLOAD_FILES_INFO_DIR, 0755)
		if err != nil {
			return err
		}
	}

	data, _ := ioutil.ReadFile(downloadingFilePath(fileHashStr))
	if len(data) == 0 {
		rfm.readFileInfos[fileHashStr] = NewFmFileInfo([]byte{})
		rfm.readFileInfos[fileHashStr].Tx = tx
		return nil
	}

	fi := &fmFileInfo{}
	err := json.Unmarshal(data, fi)
	if err != nil {
		return err
	}
	rfm.readFileInfos[fileHashStr] = fi
	return nil
}

func (rfm *UserReadFileMgr) GetReadNodeSliceId(fileHashStr, nodeWalletAddr string) int {
	rfm.lock.RLock()
	defer rfm.lock.RUnlock()
	info := rfm.readFileInfos[fileHashStr]
	cnt := 0
	for _, b := range info.Blocks {
		if b.NodeWalletAddr == nodeWalletAddr {
			cnt++
		}
	}
	return cnt
}

func (rfm *UserReadFileMgr) IsBlockRead(fileHashStr, nodeWalletAddr, blockHashStr string) bool {
	rfm.lock.RLock()
	defer rfm.lock.RUnlock()
	info := rfm.readFileInfos[fileHashStr]
	if info == nil {
		return false
	}
	for _, b := range info.Blocks {
		if b.Hash == blockHashStr && b.NodeWalletAddr == nodeWalletAddr {
			return true
		}
	}
	return false
}

func (rfm *UserReadFileMgr) GetBlockChilds(fileHashStr, nodeWalletAddr, blockHashStr string) []string {
	rfm.lock.RLock()
	defer rfm.lock.RUnlock()
	info := rfm.readFileInfos[fileHashStr]
	for _, b := range info.Blocks {
		if b.Hash == blockHashStr && b.NodeWalletAddr == nodeWalletAddr {
			return b.ChildHashStrs
		}
	}
	return nil
}

func (rfm *UserReadFileMgr) ReceivedBlockFromNode(fileHashStr, blockHashStr, nodeAddr, nodeWalletAddr string, childs []string) {
	rfm.lock.Lock()
	defer rfm.lock.Unlock()

	info := rfm.readFileInfos[fileHashStr]
	if info == nil {
		info = NewFmFileInfo([]byte{})
		info.Blocks = make([]*fmBlockInfo, 0)
	}
	info.Blocks = append(info.Blocks, &fmBlockInfo{
		Hash:           blockHashStr,
		RecvTimestamp:  time.Now().Unix(),
		NodeWalletAddr: nodeWalletAddr,
		ChildHashStrs:  childs,
	})
	buf, err := json.Marshal(info)
	if err != nil {
		return
	}
	ioutil.WriteFile(downloadingFilePath(fileHashStr), buf, 0666)
}

func (rfm *UserReadFileMgr) RemoveReadFile(fileHashStr string) {
	rfm.lock.Lock()
	defer rfm.lock.Unlock()
	delete(rfm.readFileInfos, fileHashStr)
	os.Remove(downloadingFilePath(fileHashStr))
	files, err := ioutil.ReadDir(DOWNLOAD_FILES_INFO_DIR)
	if err == nil && len(files) == 0 {
		os.Remove(DOWNLOAD_FILES_INFO_DIR)
	}
}

// GetReadFileNodeWalletAddr get readfile node wallet address
func (rfm *UserReadFileMgr) GetReadFileNodeInfo(fileHashStr string) (string, string) {
	rfm.lock.Lock()
	defer rfm.lock.Unlock()
	fi := rfm.readFileInfos[fileHashStr]
	if fi == nil || len(fi.Blocks) == 0 {
		return "", ""
	}
	// return fi.Blocks[0].NodeAddr, fi.Blocks[0].NodeWalletAddr
	return fi.Blocks[0].NodeWalletAddr, fi.Blocks[0].NodeWalletAddr
}

func storeFilePath(fileHashStr string) string {
	return STORE_FILES_INFO_DIR + fileHashStr
}

func downloadingFilePath(fileHashStr string) string {
	return DOWNLOAD_FILES_INFO_DIR + fileHashStr
}

// cutFileHead remove first n bytes at the beginning of file
// rename the new temp file to origin
func cutFileHead(fileName string, n int64) error {
	in, err := os.Open(fileName)
	if err != nil {
		return err
	}
	defer in.Close()

	out, err := os.OpenFile(fileName+"_temp", os.O_CREATE|os.O_APPEND|os.O_RDWR, 0666)
	if err != nil {
		return err
	}
	defer out.Close()
	_, err = in.Seek(n, io.SeekStart)
	if err != nil {
		return err
	}
	_, err = io.Copy(out, in)
	if err != nil {
		return err
	}
	err = os.Remove(fileName)
	if err != nil {
		return err
	}
	return os.Rename(fileName+"_temp", fileName)
}
