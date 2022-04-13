package fs

import (
	"context"
	"encoding/hex"
	"fmt"
	"os"
	"strings"

	"github.com/saveio/dsp-go-sdk/consts"
	sdkErr "github.com/saveio/dsp-go-sdk/error"
	"github.com/saveio/dsp-go-sdk/types/state"

	ipld "github.com/saveio/max/Godeps/_workspace/src/gx/ipfs/Qme5bWv7wtjUNGsK2BNGVUFPKiuxWrsqrtvYwCLRw8YFES/go-ipld-format"

	blocks "github.com/saveio/max/Godeps/_workspace/src/gx/ipfs/Qmej7nf81hi2x2tvjRBF3mcp74sQyuDH4VMYDGd1YtXjb2/go-block-format"

	cid "github.com/saveio/max/Godeps/_workspace/src/gx/ipfs/QmcZfnkapfECQGcLZaf9B79NRg7cRa9EnZh4LSbkCzwNvY/go-cid"

	queue "github.com/saveio/dsp-go-sdk/types/queue"
	osUtil "github.com/saveio/dsp-go-sdk/utils/os"
	sdk "github.com/saveio/themis-go-sdk"
	"github.com/saveio/themis/common/log"

	"github.com/saveio/max/importer/helpers"
	"github.com/saveio/max/max"
	"github.com/saveio/max/merkledag"
)

type Fs struct {
	fs             *max.MaxService
	fsType         int
	closeCh        chan struct{}
	removeFileList queue.Queue
	state          *state.SyncState
}

// NewFs. new fs instance
func NewFs(chain *sdk.Chain, opts ...FsOption) (*Fs, error) {
	initOpt := &InitFsOption{}
	for _, opt := range opts {
		opt.apply(initOpt)
	}
	fsConfig := &max.FSConfig{
		RepoRoot:   initOpt.RepoRoot,
		FsType:     max.FS_FILESTORE | max.FS_BLOCKSTORE,
		ChunkSize:  consts.CHUNK_SIZE,
		GcPeriod:   initOpt.GcPeriod,
		MaxStorage: initOpt.MaxStorage,
	}
	if len(fsConfig.RepoRoot) > 0 {
		err := osUtil.CreateDirIfNotExist(fsConfig.RepoRoot)
		if err != nil {
			return nil, sdkErr.NewWithError(sdkErr.FS_CREATE_DB_ERROR, err)
		}
	}
	fs, err := max.NewMaxService(fsConfig, chain)
	if err != nil {
		return nil, sdkErr.NewWithError(sdkErr.FS_INIT_SERVICE_ERROR, err)
	}
	service := &Fs{
		fs:      fs,
		fsType:  initOpt.FsType,
		closeCh: make(chan struct{}, 1),
		state:   state.NewSyncState(),
	}
	service.state.Set(state.ModuleStateActive)
	go service.registerRemoveNotify()
	return service, nil
}

// Close. close fs service
func (this *Fs) Close() error {
	close(this.closeCh)
	this.state.Set(state.ModuleStateStopping)
	err := this.fs.Close()
	if err != nil {
		this.state.Set(state.ModuleStateError)
		return sdkErr.NewWithError(sdkErr.FS_CLOSE_ERROR, err)
	}
	this.state.Set(state.ModuleStateStopped)
	log.Infof("dsp core fs stopped")
	return nil
}

// State. get fs module state
func (this *Fs) State() state.ModuleState {
	return this.state.Get()
}

// NodesFromFile. sharding file function. sharding a file with custom prefix. encrypt is optional, if true, password
// need to be designated
func (this *Fs) NodesFromFile(fileName string, filePrefix string, encrypt bool, password string) ([]string, error) {
	hashes, err := this.fs.NodesFromFile(fileName, filePrefix, encrypt, password)
	if err != nil {
		return nil, sdkErr.New(sdkErr.SHARDING_FAIELD, err.Error())
	}
	return hashes, nil
}

func (this *Fs) NodesFromDir(path string, filePrefix string, encrypt bool, password string) ([]string, error) {
	hashes, err := this.fs.NodesFromDir(path, filePrefix, encrypt, password)
	if err != nil {
		return nil, sdkErr.New(sdkErr.SHARDING_FAIELD, err.Error())
	}
	return hashes, nil
}

// GetAllOffsets. get all block offsets of a file with root hash
func (this *Fs) GetAllOffsets(rootHash string) (map[string]uint64, error) {
	rootCid, err := cid.Decode(rootHash)
	if err != nil {
		return nil, sdkErr.NewWithError(sdkErr.FS_DECODE_CID_ERROR, err)
	}
	m := make(map[string]uint64)
	cids, offsets, indexes, err := this.fs.GetFileAllCidsWithOffset(context.Background(), rootCid)
	if err != nil {
		return nil, sdkErr.NewWithError(sdkErr.FS_GET_ALL_OFFSET_ERROR, err)
	}

	if len(cids) != len(offsets) || len(cids) != len(indexes) {
		return nil, sdkErr.NewWithError(sdkErr.FS_GET_ALL_OFFSET_ERROR,
			fmt.Errorf("length of cids, offsets, indexes no matching"))
	}

	for i, cid := range cids {
		key := fmt.Sprintf("%s-%d", cid.String(), indexes[i])
		m[key] = offsets[i]
	}
	return m, nil
}

// GetFileAllHashes. get file all hashes with its root hash
func (this *Fs) GetFileAllHashes(rootHash string) ([]string, error) {
	rootCid, err := cid.Decode(rootHash)
	if err != nil {
		return nil, sdkErr.New(sdkErr.FS_DECODE_CID_ERROR, err.Error())
	}
	ids, err := this.fs.GetFileAllCids(context.Background(), rootCid)
	if err != nil {
		return nil, sdkErr.New(sdkErr.FS_GET_ALL_CID_ERROR, err.Error())
	}
	hashes := make([]string, 0, len(ids))
	for _, id := range ids {
		hashes = append(hashes, id.String())
	}
	return hashes, nil
}

// GetBlockLinks. decode a block and get its links
func (this *Fs) GetBlockLinks(block blocks.Block) ([]string, error) {
	if block.Cid().Type() != cid.DagProtobuf {
		return nil, nil
	}
	dagNode, err := merkledag.DecodeProtobufBlock(block)
	if err != nil {
		return nil, sdkErr.NewWithError(sdkErr.FS_DECODE_BLOCK_ERROR, err)
	}
	links := make([]string, 0, len(dagNode.Links()))
	for _, link := range dagNode.Links() {
		links = append(links, link.Cid.String())
	}
	return links, nil
}

// GetBlockLinksByDAG. decode a block and get its links
func (this *Fs) GetBlockLinksByDAG(block blocks.Block) ([]*ipld.Link, error) {
	if block.Cid().Type() != cid.DagProtobuf {
		return nil, nil
	}
	dagNode, err := merkledag.DecodeProtobufBlock(block)
	if err != nil {
		return nil, sdkErr.NewWithError(sdkErr.FS_DECODE_BLOCK_ERROR, err)
	}
	return dagNode.Links(), nil
}

// BlockData. get block data from blocks.BlockData
func (this *Fs) BlockData(block blocks.Block) []byte {
	return block.RawData()
}

// BlockDataOfAny. get block data from ipld.Node or *helpers.UnixfsNode
func (this *Fs) BlockDataOfAny(node interface{}) []byte {
	ipldN, ok := node.(ipld.Node)
	if ok && ipldN != nil {
		return ipldN.RawData()
	}

	switch n := node.(type) {
	case *helpers.UnixfsNode:
		dagNode, err := n.GetDagNode()
		if err != nil {
			return nil
		}
		return dagNode.RawData()
	case *merkledag.ProtoNode:
		return n.RawData()
	case *blocks.BasicBlock:
		return n.RawData()
	}
	return nil
}

// EncodedToBlockWithCid. encode block data to block with its cid hash string.
func (this *Fs) EncodedToBlockWithCid(data []byte, cid string) blocks.Block {
	if len(cid) < 2 {
		return nil
	}
	if strings.HasPrefix(cid, consts.PROTO_NODE_PREFIX) {
		return blocks.NewBlock(data)
	}
	if strings.HasPrefix(cid, consts.RAW_NODE_PREFIX) {
		return merkledag.NewRawNode(data)
	}
	return nil
}

// AllBlockHashes. get all block hashes with root and list items
func (this *Fs) AllBlockHashes(root ipld.Node, list []*helpers.UnixfsNode) ([]string, error) {
	hashes := make([]string, 0)
	hashes = append(hashes, root.Cid().String())
	for _, node := range list {
		dagNode, err := node.GetDagNode()
		if err != nil {
			return nil, sdkErr.NewWithError(sdkErr.FS_GET_DAG_NODE_ERROR, err)
		}
		if dagNode.Cid().String() != root.Cid().String() {
			hashes = append(hashes, dagNode.Cid().String())
		}
	}
	return hashes, nil
}

// BlocksListToMap. a convertor, make a list collection to map
func (this *Fs) BlocksListToMap(list []*helpers.UnixfsNode) (map[string]*helpers.UnixfsNode, error) {
	m := make(map[string]*helpers.UnixfsNode, 0)
	for i, node := range list {
		dagNode, err := node.GetDagNode()
		if err != nil {
			return nil, sdkErr.NewWithError(sdkErr.FS_GET_DAG_NODE_ERROR, err)
		}
		key := fmt.Sprintf("%s-%d", dagNode.Cid().String(), (i + 1))
		m[key] = node
	}
	return m, nil
}

// PutBlock. put a block to fs, usually used for storage node
func (this *Fs) PutBlock(block blocks.Block) error {
	err := this.fs.PutBlock(block)
	if err != nil {
		return sdkErr.NewWithError(sdkErr.FS_PUT_DATA_ERROR, err)
	}
	return nil
}

// SetFsFilePrefix. set file prefix to fs.
func (this *Fs) SetFsFilePrefix(fileName, prefix string) error {
	log.Debugf("set file prefix %s %v", fileName, hex.EncodeToString([]byte(prefix)))
	err := this.fs.SetFilePrefix(fileName, prefix)
	if err != nil {
		return sdkErr.NewWithError(sdkErr.FS_PUT_DATA_ERROR, err)
	}
	return nil
}

// PutBlockForFileStore. put a block to fs, usually used for client
func (this *Fs) PutBlockForFileStore(fileName string, block blocks.Block, offset uint64) error {
	err := this.fs.PutBlockForFilestore(fileName, block, offset)
	if err != nil {
		return sdkErr.NewWithError(sdkErr.FS_PUT_DATA_ERROR, err)
	}
	return nil
}

// PutTag. put block tag
func (this *Fs) PutTag(blockHash string, fileHash string, index uint64, tag []byte) error {
	err := this.fs.PutTag(blockHash, fileHash, index, tag)
	if err != nil {
		return sdkErr.NewWithError(sdkErr.FS_PUT_DATA_ERROR, err)
	}
	return nil
}

// GetTag. get tag for a block
func (this *Fs) GetTag(blockHash string, fileHash string, index uint64) ([]byte, error) {
	tag, err := this.fs.GetTag(blockHash, fileHash, index)
	if err != nil {
		return nil, sdkErr.NewWithError(sdkErr.FS_GET_DATA_ERROR, err)
	}
	return tag, nil
}

// StartPDPVerify. start pdp verify for a file
func (this *Fs) StartPDPVerify(fileHash string) error {
	err := this.fs.StartPDPVerify(fileHash)
	if err != nil {
		return sdkErr.NewWithError(sdkErr.FS_INTERNAL_ERROR, err)
	}
	return nil
}

// PinRoot. pin root to prevent GC
func (this *Fs) PinRoot(ctx context.Context, fileHash string) error {
	rootCid, err := cid.Decode(fileHash)
	if err != nil {
		return sdkErr.NewWithError(sdkErr.FS_DECODE_CID_ERROR, err)
	}
	err = this.fs.PinRoot(ctx, rootCid)
	if err != nil {
		return sdkErr.NewWithError(sdkErr.FS_INTERNAL_ERROR, err)
	}
	return nil
}

// GetBlock get blocks
func (this *Fs) GetBlock(hash string) blocks.Block {
	cid, err := cid.Decode(hash)
	if err != nil {
		return nil
	}
	block, err := this.fs.GetBlock(cid)
	if err != nil {
		return nil
	}

	return block
}

// DeleteFile. delete file, unpin root block if needed
// If a block is referenced to other file, ignore it.
func (this *Fs) DeleteFile(fileHashStr, filePath string) error {
	if len(filePath) > 0 {
		err := os.RemoveAll(filePath)
		if err != nil {
			log.Errorf("delete local file %s err %s", filePath, err)
		}
	}
	err := this.fs.DeleteFile(fileHashStr)
	if err != nil {
		return sdkErr.NewWithError(sdkErr.FS_DELETE_FILE_ERROR, err)
	}
	return nil
}

// AESDecryptFile. decrpt a file
func (this *Fs) AESDecryptFile(file, prefix, password, outputPath string) error {
	err := max.DecryptFile(file, prefix, password, outputPath)
	if err != nil {
		return sdkErr.NewWithError(sdkErr.FS_DECRYPT_ERROR, err)
	}
	return nil
}

// AESEncryptFile. encrypt a file
func (this *Fs) AESEncryptFile(file, password, outputPath string) error {
	err := max.EncryptFile(file, password, outputPath)
	if err != nil {
		return sdkErr.NewWithError(sdkErr.FS_ENCRYPT_ERROR, err)
	}
	return nil
}

// RemovedExpiredFiles. pop all expired files from the queue
func (this *Fs) RemovedExpiredFiles() []interface{} {
	return this.removeFileList.PopAll()
}

func (this *Fs) RegChainEventNotificationChannel() chan map[string]interface{} {
	return this.fs.RegChainEventNotificationChannel("DSP-FS")
}

// registerRemoveNotify. register fs notify for a removed file
func (this *Fs) registerRemoveNotify() {
	for {
		select {
		case ret, ok := <-this.fs.Notify:
			if !ok {
				return
			}
			if ret == nil {
				continue
			}
			log.Debugf("remove file notify fileHash: %v, reason: %s", ret.FileHash, ret.Reason)
			this.removeFileList.Push(ret.FileHash)
		case <-this.closeCh:
			log.Debugf("stop notify because fs has closed")
			return
		}
	}
}

// SetFsFileBlockHashes. set file block hashes
func (this *Fs) SetFsFileBlockHashes(fileHash string, blockHashes []string) error {
	err := this.fs.SetFileBlockHashes(fileHash, blockHashes)
	if err != nil {
		return sdkErr.NewWithError(sdkErr.FS_PUT_DATA_ERROR, err)
	}
	return nil
}

// ReturnBuffer. a optimize method for memory
func (this *Fs) ReturnBuffer(buffer []byte) error {
	err := max.ReturnBuffer(buffer)
	if err != nil {
		return sdkErr.NewWithError(sdkErr.FS_INTERNAL_ERROR, err)
	}
	return nil
}
