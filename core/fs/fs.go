package fs

import (
	"context"
	"encoding/hex"
	"fmt"
	ipld "gx/ipfs/Qme5bWv7wtjUNGsK2BNGVUFPKiuxWrsqrtvYwCLRw8YFES/go-ipld-format"
	"os"
	"strings"

	dspErr "github.com/saveio/dsp-go-sdk/error"

	cid "gx/ipfs/QmcZfnkapfECQGcLZaf9B79NRg7cRa9EnZh4LSbkCzwNvY/go-cid"
	blocks "gx/ipfs/Qmej7nf81hi2x2tvjRBF3mcp74sQyuDH4VMYDGd1YtXjb2/go-block-format"

	"github.com/gogo/protobuf/proto"
	"github.com/saveio/dsp-go-sdk/common"
	"github.com/saveio/dsp-go-sdk/config"
	"github.com/saveio/dsp-go-sdk/utils"
	sdk "github.com/saveio/themis-go-sdk"
	chainCom "github.com/saveio/themis/common"
	"github.com/saveio/themis/common/log"

	"github.com/saveio/max/importer/helpers"
	max "github.com/saveio/max/max"
	"github.com/saveio/max/merkledag"
	ml "github.com/saveio/max/merkledag"
	ftpb "github.com/saveio/max/unixfs/pb"
)

type Fs struct {
	fs             *max.MaxService
	cfg            *config.DspConfig
	closeCh        chan struct{}
	removeFileList utils.Queue
}

func NewFs(cfg *config.DspConfig, chain *sdk.Chain) (*Fs, error) {
	if cfg == nil {
		cfg = config.DefaultDspConfig()
	}
	fsConfig := &max.FSConfig{
		RepoRoot:   cfg.FsRepoRoot,
		FsType:     max.FSType(cfg.FsType),
		ChunkSize:  common.CHUNK_SIZE,
		GcPeriod:   cfg.FsGcPeriod,
		MaxStorage: cfg.FsMaxStorage,
	}
	if len(cfg.FsFileRoot) > 0 {
		err := common.CreateDirIfNeed(cfg.FsFileRoot)
		if err != nil {
			return nil, dspErr.NewWithError(dspErr.FS_CREATE_DB_ERROR, err)
		}
	}
	fs, err := max.NewMaxService(fsConfig, chain)
	if err != nil {
		return nil, dspErr.NewWithError(dspErr.FS_INIT_SERVICE_ERROR, err)
	}
	service := &Fs{
		fs:      fs,
		cfg:     cfg,
		closeCh: make(chan struct{}, 1),
	}
	go service.registerRemoveNotify()
	return service, nil
}

func (this *Fs) Close() error {
	close(this.closeCh)
	err := this.fs.Close()
	if err != nil {
		return dspErr.NewWithError(dspErr.FS_CLOSE_ERROR, err)
	}
	return nil
}

func (this *Fs) NodesFromFile(fileName string, filePrefix string, encrypt bool, password string) ([]string, error) {
	hashes, err := this.fs.NodesFromFile(fileName, filePrefix, encrypt, password)
	if err != nil {
		return nil, dspErr.New(dspErr.SHARDING_FAIELD, err.Error())
	}
	return hashes, nil
}

func (this *Fs) GetAllOffsets(rootHash string) (map[string]uint64, error) {
	rootCid, err := cid.Decode(rootHash)
	if err != nil {
		return nil, dspErr.NewWithError(dspErr.FS_DECODE_CID_ERROR, err)
	}
	m := make(map[string]uint64)
	cids, offsets, err := this.fs.GetFileAllCidsWithOffset(context.Background(), rootCid)
	if err != nil {
		return nil, dspErr.NewWithError(dspErr.FS_GET_ALL_OFFSET_ERROR, err)
	}
	for i, cid := range cids {
		m[cid.String()] = offsets[i]
	}
	return m, nil
}

func (this *Fs) GetFileAllHashes(rootHash string) ([]string, error) {
	rootCid, err := cid.Decode(rootHash)
	if err != nil {
		return nil, dspErr.New(dspErr.FS_DECODE_CID_ERROR, err.Error())
	}
	ids, err := this.fs.GetFileAllCids(context.Background(), rootCid)
	if err != nil {
		return nil, dspErr.New(dspErr.FS_GET_ALL_CID_ERROR, err.Error())
	}
	hashes := make([]string, 0, len(ids))
	for _, id := range ids {
		hashes = append(hashes, id.String())
	}
	return hashes, nil
}

func (this *Fs) GetBlockLinks(block blocks.Block) ([]string, error) {
	if block.Cid().Type() != cid.DagProtobuf {
		return nil, nil
	}
	dagNode, err := merkledag.DecodeProtobufBlock(block)
	if err != nil {
		return nil, dspErr.NewWithError(dspErr.FS_DECODE_BLOCK_ERROR, err)
	}
	links := make([]string, 0, len(dagNode.Links()))
	for _, link := range dagNode.Links() {
		links = append(links, link.Cid.String())
	}
	return links, nil
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

// BlockToBytes. get block decoded data bytes
func (this *Fs) BlockToBytes(block blocks.Block) ([]byte, error) {
	_, isRawNode := block.(*ml.RawNode)
	if isRawNode {
		return block.RawData(), nil
	}
	dagNode, err := ml.DecodeProtobufBlock(block)
	if err != nil {
		return nil, dspErr.NewWithError(dspErr.FS_DECODE_BLOCK_ERROR, err)
	}
	pb := new(ftpb.Data)
	if err := proto.Unmarshal(dagNode.(*ml.ProtoNode).Data(), pb); err != nil {
		return nil, dspErr.NewWithError(dspErr.FS_DECODE_BLOCK_ERROR, err)
	}
	return pb.Data, nil
}

// EncodedToBlockWithCid. encode block data to block with its cid hash string.
func (this *Fs) EncodedToBlockWithCid(data []byte, cid string) blocks.Block {
	if len(cid) < 2 {
		return nil
	}
	if strings.HasPrefix(cid, common.PROTO_NODE_PREFIX) {
		return blocks.NewBlock(data)
	}
	if strings.HasPrefix(cid, common.RAW_NODE_PREFIX) {
		return ml.NewRawNode(data)
	}
	return nil
}

func (this *Fs) AllBlockHashes(root ipld.Node, list []*helpers.UnixfsNode) ([]string, error) {
	hashes := make([]string, 0)
	hashes = append(hashes, root.Cid().String())
	for _, node := range list {
		dagNode, err := node.GetDagNode()
		if err != nil {
			return nil, dspErr.NewWithError(dspErr.FS_GET_DAG_NODE_ERROR, err)
		}
		if dagNode.Cid().String() != root.Cid().String() {
			hashes = append(hashes, dagNode.Cid().String())
		}
	}
	return hashes, nil
}

func (this *Fs) BlocksListToMap(list []*helpers.UnixfsNode) (map[string]*helpers.UnixfsNode, error) {
	m := make(map[string]*helpers.UnixfsNode, 0)
	for i, node := range list {
		dagNode, err := node.GetDagNode()
		if err != nil {
			return nil, dspErr.NewWithError(dspErr.FS_GET_DAG_NODE_ERROR, err)
		}
		key := fmt.Sprintf("%s-%d", dagNode.Cid().String(), (i + 1))
		m[key] = node
	}
	return m, nil
}

func (this *Fs) PutBlock(block blocks.Block) error {
	err := this.fs.PutBlock(block)
	if err != nil {
		return dspErr.NewWithError(dspErr.FS_PUT_DATA_ERROR, err)
	}
	return nil
}

func (this *Fs) SetFsFilePrefix(fileName, prefix string) error {
	log.Debugf("set file prefix %s %v", fileName, hex.EncodeToString([]byte(prefix)))
	err := this.fs.SetFilePrefix(fileName, prefix)
	if err != nil {
		return dspErr.NewWithError(dspErr.FS_PUT_DATA_ERROR, err)
	}
	return nil
}

func (this *Fs) PutBlockForFileStore(fileName string, block blocks.Block, offset uint64) error {
	err := this.fs.PutBlockForFilestore(fileName, block, offset)
	if err != nil {
		return dspErr.NewWithError(dspErr.FS_PUT_DATA_ERROR, err)
	}
	return nil
}

func (this *Fs) PutTag(blockHash string, fileHash string, index uint64, tag []byte) error {
	err := this.fs.PutTag(blockHash, fileHash, index, tag)
	if err != nil {
		return dspErr.NewWithError(dspErr.FS_PUT_DATA_ERROR, err)
	}
	return nil
}

func (this *Fs) GetTag(blockHash string, fileHash string, index uint64) ([]byte, error) {
	tag, err := this.fs.GetTag(blockHash, fileHash, index)
	if err != nil {
		return nil, dspErr.NewWithError(dspErr.FS_GET_DATA_ERROR, err)
	}
	return tag, nil
}

func (this *Fs) StartPDPVerify(fileHash string, luckyNum uint64, bakHeight uint64, bakNum uint64, borkenWalletAddr chainCom.Address) error {
	err := this.fs.StartPDPVerify(fileHash, luckyNum, bakHeight, bakNum, borkenWalletAddr)
	if err != nil {
		return dspErr.NewWithError(dspErr.FS_INTERNAL_ERROR, err)
	}
	return nil
}

// PinRoot. pin root to prevent GC
func (this *Fs) PinRoot(ctx context.Context, fileHash string) error {
	rootCid, err := cid.Decode(fileHash)
	if err != nil {
		return dspErr.NewWithError(dspErr.FS_DECODE_CID_ERROR, err)
	}
	err = this.fs.PinRoot(ctx, rootCid)
	if err != nil {
		return dspErr.NewWithError(dspErr.FS_INTERNAL_ERROR, err)
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
	if this.fs.IsFileStore() && this.cfg.FsType == config.FS_FILESTORE && len(filePath) > 0 {
		err := os.Remove(filePath)
		if err != nil {
			return err
		}
	}
	err := this.fs.DeleteFile(fileHashStr)
	if err != nil {
		return dspErr.NewWithError(dspErr.FS_DELETE_FILE_ERROR, err)
	}
	return nil
}

// AESDecryptFile. descypt file
func (this *Fs) AESDecryptFile(file, prefix, password, outputPath string) error {
	err := max.DecryptFile(file, prefix, password, outputPath)
	if err != nil {
		return dspErr.NewWithError(dspErr.FS_DECRYPT_ERROR, err)
	}
	return nil
}

// AESEncryptFile. encrypt file
func (this *Fs) AESEncryptFile(file, password, outputPath string) error {
	err := max.EncryptFile(file, password, outputPath)
	if err != nil {
		return dspErr.NewWithError(dspErr.FS_ENCRYPT_ERROR, err)
	}
	return nil
}

func (this *Fs) RemovedExpiredFiles() []interface{} {
	return this.removeFileList.PopAll()
}

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

func (this *Fs) SetFsFileBlockHashes(fileHash string, blockHashes []string) error {
	err := this.fs.SetFileBlockHashes(fileHash, blockHashes)
	if err != nil {
		return dspErr.NewWithError(dspErr.FS_PUT_DATA_ERROR, err)
	}
	return nil
}

func (this *Fs) ReturnBuffer(buffer []byte) error {
	err := max.ReturnBuffer(buffer)
	if err != nil {
		return dspErr.NewWithError(dspErr.FS_INTERNAL_ERROR, err)
	}
	return nil
}
