package store

import (
	"encoding/json"
	"errors"
	"io/ioutil"
	"sync"
	"time"
)

const (
	MAX_STORING_FILE_NUM           = 100                 // Maximum number of storing file currently
	MAX_STORING_TIMEOUT            = 60 * 60             // Max storing file, timeout in second
	STORE_FILES_PATH               = "./filestore.files" // Store file path
	READ_FILES_PATH                = "./read.files"      // Store settle slice file path
	COPY_FILES_PATH                = "./copy.files"      // Store copy file path
	MAX_SETTLE_SLICE_KEEP_TIME     = 10 * 60             // A settle slice keep time for publish
	MAX_OUTDATED_SETTLE_SLICE      = 100                 // Maximum number of outdate settle slice for handling
	MAX_PROVE_FAILED_TIMES         = 10                  // Maximum prove failed times
	MAX_COMMIT_COUNT               = 10                  // Maximum commit count of outdated settle slice
	MAX_COMMIT_SETTLE_SLICES_DELAY = 3                   // Max delay of commit settle slices between current block height and expired height
	BT_FILE_PREFIX_LEN             = 34                  // bt file prefix length
)

// deprecated. use filedb instead
type NodeFileMgr struct {
	NodeStoreFileMgr
	NodeReadFileMgr
	CopyFileMgr
}

//storingBlkInfo used for record a storing block information
type storingBlkInfo struct {
	index uint32
	hash  string // Block hash.
	state uint8  // Block state. 0 => unstored, 1 => stored
}

// storingFileInfo record a storing file information, used for accessing properties fast
type storingFileInfo struct {
	blockHashes     []string          // Record all block hashes, used for sorting
	recvBlks        []*storingBlkInfo // Record all blocks information, only init after received the block
	storingPeers    []string          // Peers that received copynum file
	lastRecvBlkTime int64             // Last received block unix timestamp in second, used for timeout handle
}

type NodeStoreFileMgr struct {
	storingFiles         map[string]*storingFileInfo // Storing files. fileHash => file information
	lock                 sync.RWMutex                // Lock
	fileProveFailedTimes map[string]uint32           // FileHash => prove failed times, using for clean file in failure case
	allStoredFileHashes  []string                    // All stored file hash string
}

func NewNodeFileMgr() *NodeFileMgr {
	fm := &NodeFileMgr{}
	// after the file is stored, the value will be deleted
	fm.storingFiles = make(map[string]*storingFileInfo, 0)

	fm.allStoredFileHashes = make([]string, 0)
	data, _ := ioutil.ReadFile(STORE_FILES_PATH)
	if len(data) > 0 {
		files := make([]string, 0)
		err := json.Unmarshal(data, &files)
		if err == nil {
			fm.allStoredFileHashes = files
		}
	}
	fm.fileProveFailedTimes = make(map[string]uint32, 0)
	fm.filePledgeInfos = make(map[string]*readFilePledgeInfo, 0)
	fm.copyFileInfos = make(map[string]*copyFileInfo)

	// init the settle slice map from local
	rssBuf, _ := ioutil.ReadFile(READ_FILES_PATH)
	if len(rssBuf) > 0 {
		json.Unmarshal(rssBuf, &fm.filePledgeInfos)
	}

	// init the copy file info map from local
	copyBuf, _ := ioutil.ReadFile(COPY_FILES_PATH)
	if len(copyBuf) > 0 {
		json.Unmarshal(copyBuf, &fm.copyFileInfos)
	}

	return fm
}

// IsFileStoring return a file is storing but not finish storing all blocks
func (fm *NodeStoreFileMgr) IsFileStoring(file string) bool {
	fm.lock.RLock()
	defer fm.lock.RUnlock()
	_, ok := fm.storingFiles[file]
	return ok
}

// AddFileBlockHashes add all blocks' hash, using for detect whether the node has stored the file
func (fm *NodeStoreFileMgr) AddFileBlockHashes(fileHashStr string, blocks []string) error {
	fm.lock.Lock()
	defer fm.lock.Unlock()
	if len(fm.storingFiles) >= MAX_STORING_FILE_NUM-1 {
		return errors.New("too much file currently")
	}
	fileInfo := &storingFileInfo{
		blockHashes: blocks,
		recvBlks:    make([]*storingBlkInfo, 0),
	}
	fm.storingFiles[fileHashStr] = fileInfo
	return nil
}

func (fm *NodeStoreFileMgr) FileBlockHashes(fileHashStr string) []string {
	fm.lock.RLock()
	defer fm.lock.RUnlock()
	f, ok := fm.storingFiles[fileHashStr]
	if !ok {
		return nil
	}
	return f.blockHashes
}

//  SetBlockStored set the flag of store state
func (fm *NodeStoreFileMgr) SetBlockStored(fileHashStr, blockHashStr string, index uint32) {
	fm.lock.Lock()
	info := fm.storingFiles[fileHashStr]
	info.lastRecvBlkTime = time.Now().Unix()
	recv := &storingBlkInfo{
		index: index,
		hash:  blockHashStr,
		state: 1,
	}
	info.recvBlks = append(info.recvBlks, recv)
	fm.lock.Unlock()

}

//  SetBlockStored set the flag of store state
func (fm *NodeStoreFileMgr) IsBlockStored(fileHashStr, blockHashStr string, index uint32) bool {
	fm.lock.RLock()
	defer fm.lock.RUnlock()
	info := fm.storingFiles[fileHashStr]
	if info == nil {
		return false
	}
	for _, blk := range info.recvBlks {
		if blk.index == index && blk.hash == blockHashStr {
			return true
		}
	}
	return false

}

// IsFileStored check if a storing file task has finished storing all blocks
func (fm *NodeStoreFileMgr) IsFileStored(file string) bool {
	fm.lock.RLock()
	defer fm.lock.RUnlock()
	info, ok := fm.storingFiles[file]
	if !ok {
		return false
	}
	return len(info.blockHashes) == len(info.recvBlks)
}

// SetFilePeers set all storing backup file peers
func (fm *NodeStoreFileMgr) SetFilePeers(file string, peers []string) {
	fm.lock.Lock()
	defer fm.lock.Unlock()
	info, ok := fm.storingFiles[file]
	if !ok {
		return
	}
	info.storingPeers = peers
	fm.storingFiles[file] = info
}

// FilePeers get all storing backup file peers
func (fm *NodeStoreFileMgr) FilePeers(file string) []string {
	fm.lock.RLock()
	defer fm.lock.RUnlock()
	info, ok := fm.storingFiles[file]
	if !ok {
		return nil
	}
	return info.storingPeers
}

// AllOutdatedStoringFiles get all outdated storing file hashes
func (fm *NodeStoreFileMgr) AllOutdatedStoringFiles() []string {
	fm.lock.RLock()
	defer fm.lock.RUnlock()
	outdated := make([]string, 0)
	now := time.Now().Unix()
	for fileHashStr, info := range fm.storingFiles {
		if len(info.recvBlks) > 0 && now > info.lastRecvBlkTime && now-info.lastRecvBlkTime >= MAX_STORING_TIMEOUT {
			outdated = append(outdated, fileHashStr)
		}
	}
	return outdated
}

func (fm *NodeStoreFileMgr) GetAllStoreFiles() []string {
	fm.lock.RLock()
	defer fm.lock.RUnlock()
	return fm.allStoredFileHashes
}

func (fm *NodeStoreFileMgr) AddStoreFiles(fileHashStr string) error {
	fm.lock.Lock()
	defer fm.lock.Unlock()

	data, _ := ioutil.ReadFile(STORE_FILES_PATH)
	files := make([]string, 0)
	if len(data) > 0 {
		err := json.Unmarshal(data, &files)
		if err != nil {
			return err
		}
	}

	exist := false
	for _, f := range files {
		if fileHashStr == f {
			exist = true
			break
		}
	}
	if !exist {
		files = append(files, fileHashStr)
	}

	for _, f := range fm.allStoredFileHashes {
		if fileHashStr == f {
			exist = true
		}
	}
	if !exist {
		fm.allStoredFileHashes = append(fm.allStoredFileHashes, fileHashStr)
	}
	buf, err := json.Marshal(files)
	if err != nil {
		return err
	}
	return ioutil.WriteFile(STORE_FILES_PATH, buf, 0666)
}

func (fm *NodeStoreFileMgr) IncreProveFailedTimes(fileHashStr string) {
	fm.lock.Lock()
	defer fm.lock.Unlock()
	cnt := fm.fileProveFailedTimes[fileHashStr]
	fm.fileProveFailedTimes[fileHashStr] = cnt + 1
}

func (fm *NodeStoreFileMgr) IsFileProveFailedTooMuch(fileHashStr string) bool {
	fm.lock.RLock()
	defer fm.lock.RUnlock()
	cnt := fm.fileProveFailedTimes[fileHashStr]
	return cnt > MAX_PROVE_FAILED_TIMES
}

// RemoveDataOfStoreFile remove map data when store file successful or failed
func (fm *NodeStoreFileMgr) RemoveDataOfStoreFile(fileHashStr string) {
	fm.lock.Lock()
	defer fm.lock.Unlock()
	delete(fm.storingFiles, fileHashStr)
}

// RemoveStoredFileData remove store file map data after the local file will be deleted
func (fm *NodeStoreFileMgr) RemoveStoredFileData(fileHashStr string) error {
	// remove prove failed count
	fm.removeProveFailedCount(fileHashStr)
	// remove store file data
	fm.RemoveDataOfStoreFile(fileHashStr)
	// remove local store files record
	return fm.removeStoreFiles(fileHashStr)
}

func (fm *NodeStoreFileMgr) removeProveFailedCount(fileHashStr string) {
	fm.lock.Lock()
	defer fm.lock.Unlock()
	delete(fm.fileProveFailedTimes, fileHashStr)
}

func (fm *NodeStoreFileMgr) removeStoreFiles(fileHashStr string) error {
	fm.lock.Lock()
	defer fm.lock.Unlock()
	data, _ := ioutil.ReadFile(STORE_FILES_PATH)
	if len(data) == 0 {
		return nil
	}
	files := make([]string, 0)
	err := json.Unmarshal(data, &files)
	if err != nil {
		return err
	}
	for i, h := range files {
		if h == fileHashStr {
			files = append(files[:i], files[i+1:]...)
		}
	}
	for i, h := range fm.allStoredFileHashes {
		if h == fileHashStr {
			fm.allStoredFileHashes = append(fm.allStoredFileHashes[:i], fm.allStoredFileHashes[i+1:]...)
		}
	}
	buf, err := json.Marshal(files)
	if err != nil {
		return err
	}
	return ioutil.WriteFile(STORE_FILES_PATH, buf, 0666)
}

type readFilePledgeInfo struct {
	HaveReadBlockCount uint64 `json:"have_read_count"`
	SettleSlice        []byte `json:"settle_slice"`
	TotalBlockCount    uint64 `json:"total_count"`
	ExpiredHeight      uint64 `json:"expired_height"` // User can cancel pledge only when current height exceeds expired height.
	CommitCount        uint64 `json:"commit_count"`
}

type NodeReadFileMgr struct {
	rMgrLock        sync.RWMutex                   // lock
	filePledgeInfos map[string]*readFilePledgeInfo // fileHashStr => filePledgeInfo
}

func (fm *NodeReadFileMgr) NewFilePledgeInfo(fileHashStr string, expiredHeight uint64, readCount uint64, totalNum uint64) {
	fm.rMgrLock.Lock()
	defer fm.rMgrLock.Unlock()
	info := fm.filePledgeInfos[fileHashStr]
	if info == nil {
		info = &readFilePledgeInfo{}
	}

	info.TotalBlockCount = totalNum
	info.HaveReadBlockCount = readCount
	info.ExpiredHeight = expiredHeight
	fm.filePledgeInfos[fileHashStr] = info
	buf, err := json.Marshal(fm.filePledgeInfos)
	if err != nil {
		return
	}
	ioutil.WriteFile(READ_FILES_PATH, buf, 0666)
}

// AddReadBlock add a read block
func (fm *NodeReadFileMgr) AddReadBlock(fileHashStr, blockHash string) {
	fm.rMgrLock.Lock()
	defer fm.rMgrLock.Unlock()
	info := fm.filePledgeInfos[fileHashStr]
	if info == nil {
		return
	}
	info.HaveReadBlockCount = info.HaveReadBlockCount + 1
	buf, err := json.Marshal(fm.filePledgeInfos)
	if err != nil {
		return
	}
	ioutil.WriteFile(READ_FILES_PATH, buf, 0666)
}

func (fm *NodeReadFileMgr) GetReadBlocksCount(fileHashStr string) uint64 {
	fm.rMgrLock.RLock()
	defer fm.rMgrLock.RUnlock()
	info := fm.filePledgeInfos[fileHashStr]
	if info == nil {
		return 0
	}
	return info.HaveReadBlockCount
}

func (fm *NodeReadFileMgr) SetFileSettleSlice(fileHashStr string, ss []byte) {
	fm.rMgrLock.Lock()
	defer fm.rMgrLock.Unlock()
	info := fm.filePledgeInfos[fileHashStr]
	if info == nil {
		info = &readFilePledgeInfo{}
	}
	info.SettleSlice = ss
	fm.filePledgeInfos[fileHashStr] = info

	rssBuf, err := json.Marshal(fm.filePledgeInfos)
	if err != nil {
		return
	}
	ioutil.WriteFile(READ_FILES_PATH, rssBuf, 0666)
}

func (fm *NodeReadFileMgr) GetFileSettleSlice(fileHashStr string) []byte {
	fm.rMgrLock.RLock()
	defer fm.rMgrLock.RUnlock()
	info := fm.filePledgeInfos[fileHashStr]
	if info == nil {
		return nil
	}
	return info.SettleSlice
}

// GetAllOutDatedSettleSlice get all settle slices which is outdated for a long time
func (fm *NodeReadFileMgr) GetAllOutDatedSettleSlice(currentBlockHeight uint64) map[string][]byte {
	fm.rMgrLock.Lock()
	defer fm.rMgrLock.Unlock()
	outdate := make(map[string][]byte, 0)
	for fileHashStr, info := range fm.filePledgeInfos {
		if info.ExpiredHeight <= currentBlockHeight+MAX_COMMIT_SETTLE_SLICES_DELAY {
			info.CommitCount++
			if len(info.SettleSlice) == 0 {
				continue
			}
			// avoid overflow
			if len(outdate) >= MAX_OUTDATED_SETTLE_SLICE {
				break
			}
			outdate[fileHashStr] = info.SettleSlice
		}
	}

	for fileHashStr, info := range fm.filePledgeInfos {
		if info.CommitCount >= MAX_COMMIT_COUNT {
			delete(fm.filePledgeInfos, fileHashStr)
		}
	}
	buf, err := json.Marshal(fm.filePledgeInfos)
	if err == nil {
		ioutil.WriteFile(READ_FILES_PATH, buf, 0666)
	}

	return outdate
}

func (fm *NodeReadFileMgr) GetFileBlockTotalNums(fileHashStr string) uint64 {
	fm.rMgrLock.RLock()
	defer fm.rMgrLock.RUnlock()
	info := fm.filePledgeInfos[fileHashStr]
	if info == nil {
		return 0
	}
	return info.TotalBlockCount
}

// IsReadMaxBlock if have read block count equal to max block count
func (fm *NodeReadFileMgr) IsReadMaxBlock(fileHashStr string) bool {
	fm.rMgrLock.RLock()
	defer fm.rMgrLock.RUnlock()
	info := fm.filePledgeInfos[fileHashStr]
	if info == nil {
		return false
	}
	return info.HaveReadBlockCount == info.TotalBlockCount
}

// RemoveDataOfUploadSettleSlice remove map data when commit settle slice
func (fm *NodeReadFileMgr) RemoveDataOfCommitSettleSlice(fileHashStr string) {
	fm.rMgrLock.Lock()
	defer fm.rMgrLock.Unlock()
	delete(fm.filePledgeInfos, fileHashStr)
	buf, err := json.Marshal(fm.filePledgeInfos)
	if err != nil {
		return
	}
	ioutil.WriteFile(READ_FILES_PATH, buf, 0666)
}

type copyBlock struct {
	Hash          string   `json:"hash"`
	ChildHashStrs []string `json:"childs"`
}

type copyFileInfo struct {
	Blocks []*copyBlock `json:"blocks"`
}

// CopyFileMgr backup a file from remote ont-ipfs node
// The received data don't store in local
type CopyFileMgr struct {
	lock          sync.RWMutex             // lock
	copyFileInfos map[string]*copyFileInfo // fileHashStr => filePledgeInfo
}

func (cfm *CopyFileMgr) GetReadNodeSliceId(fileHashStr string) int {
	cfm.lock.RLock()
	defer cfm.lock.RUnlock()
	info := cfm.copyFileInfos[fileHashStr]
	if info == nil {
		return 0
	}
	cnt := len(info.Blocks)
	return cnt
}

func (cfm *CopyFileMgr) IsFileCopying(fileHashStr string) bool {
	cfm.lock.RLock()
	defer cfm.lock.RUnlock()
	_, ok := cfm.copyFileInfos[fileHashStr]
	return ok
}

func (cfm *CopyFileMgr) ReceivedBlockFromNode(fileHashStr, blockHashStr string) {
	cfm.lock.Lock()
	defer cfm.lock.Unlock()

	info := cfm.copyFileInfos[fileHashStr]
	if info == nil {
		info = &copyFileInfo{}
	}
	if info.Blocks == nil {
		info.Blocks = make([]*copyBlock, 0)
	}
	info.Blocks = append(info.Blocks, &copyBlock{
		Hash: blockHashStr,
	})
	cfm.copyFileInfos[fileHashStr] = info
	buf, err := json.Marshal(cfm.copyFileInfos)
	if err != nil {
		return
	}
	ioutil.WriteFile(COPY_FILES_PATH, buf, 0666)
}

func (cfm *CopyFileMgr) RemoveDataOfCopyDone(fileHashStr string) {
	cfm.lock.Lock()
	defer cfm.lock.Unlock()
	delete(cfm.copyFileInfos, fileHashStr)
	buf, err := json.Marshal(cfm.copyFileInfos)
	if err != nil {
		return
	}
	ioutil.WriteFile(COPY_FILES_PATH, buf, 0666)
}
