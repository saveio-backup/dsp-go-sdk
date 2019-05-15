package fs

import (
	"context"
	"errors"
	"fmt"
	ipld "gx/ipfs/Qme5bWv7wtjUNGsK2BNGVUFPKiuxWrsqrtvYwCLRw8YFES/go-ipld-format"
	"os"
	"path/filepath"
	"strings"

	cid "gx/ipfs/QmcZfnkapfECQGcLZaf9B79NRg7cRa9EnZh4LSbkCzwNvY/go-cid"
	"gx/ipfs/Qmej7nf81hi2x2tvjRBF3mcp74sQyuDH4VMYDGd1YtXjb2/go-block-format"

	"github.com/gogo/protobuf/proto"
	"github.com/saveio/dsp-go-sdk/common"
	"github.com/saveio/dsp-go-sdk/config"
	sdk "github.com/saveio/themis-go-sdk"
	chainCom "github.com/saveio/themis/common"

	"github.com/saveio/max/importer/helpers"
	max "github.com/saveio/max/max"
	ml "github.com/saveio/max/merkledag"
	ftpb "github.com/saveio/max/unixfs/pb"
)

type Fs struct {
	fs  *max.MaxService
	cfg *config.DspConfig
}

func NewFs(cfg *config.DspConfig, chain *sdk.Chain) (*Fs, error) {
	if cfg == nil {
		cfg = config.DefaultDspConfig()
	}

	root, err := filepath.Abs("/")
	if err != nil {
		return nil, err
	}
	fsConfig := &max.FSConfig{
		RepoRoot:   cfg.FsRepoRoot,
		FsRoot:     root,
		FsType:     max.FSType(cfg.FsType),
		ChunkSize:  common.CHUNK_SIZE,
		GcPeriod:   cfg.FsGcPeriod,
		MaxStorage: cfg.FsMaxStorage,
	}
	if _, err := os.Stat(cfg.FsFileRoot); os.IsNotExist(err) {
		err = os.MkdirAll(cfg.FsFileRoot, 0755)
		if err != nil {
			return nil, err
		}
	}

	fs, err := max.NewMaxService(fsConfig, chain)
	if err != nil {
		return nil, err
	}
	return &Fs{
		fs:  fs,
		cfg: cfg,
	}, nil
}

func (this *Fs) Close() error {
	return this.fs.Close()
}

func (this *Fs) NodesFromFile(fileName string, filePrefix string, encrypt bool, password string) (ipld.Node, []*helpers.UnixfsNode, error) {
	root, list, err := this.fs.NodesFromFile(fileName, filePrefix, encrypt, password)
	if err != nil {
		return nil, nil, err
	}
	m := make(map[string]*helpers.UnixfsNode, 0)
	for _, node := range list {
		dagNode, err := node.GetDagNode()
		if err != nil {
			return nil, nil, err
		}
		m[dagNode.Cid().String()] = node
	}
	newList := make([]*helpers.UnixfsNode, 0)
	var breadth func(block ipld.Node)
	breadth = func(block ipld.Node) {
		for _, l := range block.Links() {
			n := m[l.Cid.String()]
			if n == nil {
				return
			}
			newList = append(newList, n)
		}
		for _, l := range block.Links() {
			n := m[l.Cid.String()]
			if n == nil {
				return
			}
			dag, err := n.GetDagNode()
			if err != nil {
				return
			}
			if len(dag.Links()) == 0 {
				continue
			}
			breadth(dag)
		}
	}
	breadth(root)
	if len(list) != len(newList) {
		return nil, nil, errors.New("build new list error")
	}
	return root, newList, nil
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
	unixfN, ok := node.(*helpers.UnixfsNode)
	if ok && unixfN != nil {
		dagNode, err := unixfN.GetDagNode()
		if err != nil {
			return nil
		}
		return dagNode.RawData()
	}
	basicBlk, ok := node.(*blocks.BasicBlock)
	if ok && basicBlk != nil {
		return basicBlk.RawData()
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
		return nil, err
	}
	pb := new(ftpb.Data)
	if err := proto.Unmarshal(dagNode.(*ml.ProtoNode).Data(), pb); err != nil {
		return nil, err
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

// BlockLinks. get links from a block
func (this *Fs) BlockLinks(block blocks.Block) ([]string, error) {
	links := make([]string, 0)
	_, ok := block.(*ml.RawNode)
	if ok {
		// for *ml.RawNode, it has no links
		return nil, nil
	}
	// for *ml.ProtoNode, it has links
	dagNode, err := ml.DecodeProtobufBlock(block)
	if err != nil {
		return nil, err
	}
	for _, l := range dagNode.Links() {
		links = append(links, l.Cid.String())
	}
	return links, nil
}

func (this *Fs) AllBlockHashes(root ipld.Node, list []*helpers.UnixfsNode) ([]string, error) {
	hashes := make([]string, 0)
	hashes = append(hashes, root.Cid().String())
	for _, node := range list {
		dagNode, err := node.GetDagNode()
		if err != nil {
			return nil, err
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
			return nil, err
		}
		key := fmt.Sprintf("%s%d", dagNode.Cid().String(), (i + 1))
		m[key] = node
	}
	return m, nil
}

func (this *Fs) PutBlock(block blocks.Block) error {
	return this.fs.PutBlock(block)
}

func (this *Fs) SetFsFilePrefix(fileName, prefix string) error {
	return this.fs.SetFilePrefix(fileName, prefix)
}

func (this *Fs) PutBlockForFileStore(fileName string, block blocks.Block, offset uint64) error {
	return this.fs.PutBlockForFilestore(fileName, block, offset)
}

func (this *Fs) PutTag(blockHash string, fileHash string, index uint64, tag []byte) error {
	return this.fs.PutTag(blockHash, fileHash, index, tag)
}

func (this *Fs) GetTag(blockHash string, fileHash string, index uint64) ([]byte, error) {
	return this.fs.GetTag(blockHash, fileHash, index)
}

func (this *Fs) StartPDPVerify(fileHash string, luckyNum uint64, bakHeight uint64, bakNum uint64, borkenWalletAddr chainCom.Address) error {
	return this.fs.StartPDPVerify(fileHash, luckyNum, bakHeight, bakNum, borkenWalletAddr)
}

// PinRoot. pin root to prevent GC
func (this *Fs) PinRoot(ctx context.Context, fileHash string) error {
	rootCid, err := cid.Decode(fileHash)
	if err != nil {
		return err
	}
	return this.fs.PinRoot(ctx, rootCid)
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
func (this *Fs) DeleteFile(fileHashStr string) error {
	if this.fs.IsFileStore() && this.cfg.FsType == config.FS_FILESTORE {
		return os.Remove(this.cfg.FsFileRoot + "/" + fileHashStr)
	}
	return this.fs.DeleteFile(fileHashStr)
}

// AESDecryptFile. descypt file
func (this *Fs) AESDecryptFile(file, password, outputPath string) error {
	return max.DecryptFile(file, password, outputPath)
}

// AESEncryptFile. encrypt file
func (this *Fs) AESEncryptFile(file, password, outputPath string) error {
	return max.EncryptFile(file, password, outputPath)
}
