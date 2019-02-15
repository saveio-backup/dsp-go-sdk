package fs

import (
	"errors"
	"fmt"
	ipld "gx/ipfs/Qme5bWv7wtjUNGsK2BNGVUFPKiuxWrsqrtvYwCLRw8YFES/go-ipld-format"

	cid "gx/ipfs/QmcZfnkapfECQGcLZaf9B79NRg7cRa9EnZh4LSbkCzwNvY/go-cid"
	"gx/ipfs/Qmej7nf81hi2x2tvjRBF3mcp74sQyuDH4VMYDGd1YtXjb2/go-block-format"

	"github.com/gogo/protobuf/proto"
	"github.com/oniio/dsp-go-sdk/common"
	sdk "github.com/oniio/oniChain-go-sdk"

	"github.com/oniio/oniFS/importer/helpers"
	ml "github.com/oniio/oniFS/merkledag"
	oniFs "github.com/oniio/oniFS/onifs"
	ftpb "github.com/oniio/oniFS/unixfs/pb"
)

type Fs struct {
	fs *oniFs.OniFSService
}

type FsConfig struct {
	repoRoot  string
	fsRoot    string
	fsType    oniFs.FSType
	chunkSize uint64
	chain     *sdk.Chain
}

func NewFs(config *FsConfig) *Fs {
	if config == nil {
		config = defaultFSConfig()
	}

	fs, err := oniFs.NewOniFSService(config.repoRoot, config.fsRoot, config.fsType, config.chunkSize, config.chain)
	if err != nil {
		return nil
	}

	return &Fs{fs: fs}
}

func defaultFSConfig() *FsConfig {
	return &FsConfig{
		repoRoot:  "./",
		fsRoot:    "./",
		fsType:    oniFs.FS_BLOCKSTORE,
		chunkSize: common.CHUNK_SIZE,
	}
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

func (this *Fs) BlockData(block blocks.Block) []byte {
	return block.RawData()
}

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
	return nil
}

func (this *Fs) BlockToBytes(block blocks.Block) ([]byte, error) {
	dagNode, err := ml.DecodeProtobufBlock(block)
	if err != nil {
		return block.RawData(), nil
	}

	pb := new(ftpb.Data)
	if err := proto.Unmarshal(dagNode.(*ml.ProtoNode).Data(), pb); err != nil {
		return nil, err
	}
	return pb.Data, nil
}

func (this *Fs) EncodedToBlock(data []byte) blocks.Block {
	return blocks.NewBlock(data)
}

func (this *Fs) BlockToDagNode(block blocks.Block) (ipld.Node, error) {
	return ml.DecodeProtobufBlock(block)
}

func (this *Fs) DecodeDagNode(dagNode ipld.Node) ([]byte, error) {
	pb := new(ftpb.Data)
	if err := proto.Unmarshal(dagNode.(*ml.ProtoNode).Data(), pb); err != nil {
		return nil, err
	}
	return pb.Data, nil
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

func (this *Fs) PutBlock(key string, block blocks.Block, storeType common.BlockStoreType) error {
	return this.fs.PutBlock(block)
}

func (this *Fs) PutTag(blockHash string, fileHash string, index uint64, tag []byte) error {
	return this.fs.PutTag(blockHash, fileHash, index, tag)
}

func (this *Fs) StartPDPVerify(fileHash string, luckyNum uint64, bakHeight uint64, bakNum uint64) error {
	return this.fs.StartPDPVerify(fileHash, luckyNum, bakHeight, bakNum)
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
	return this.fs.DeleteFile(fileHashStr)
}

type ProtoNode struct {
	Data       []byte
	FileSize   uint64
	Blocksizes []uint64
	LinkHashes []string
	LinkSizes  map[string]uint64
}

func (this *Fs) BlockToProtoNode(block blocks.Block) (*ProtoNode, error) {
	dagNode, err := ml.DecodeProtobufBlock(block)
	if err != nil {
		return nil, err
	}
	node := new(ProtoNode)
	node.LinkHashes = make([]string, 0)
	node.LinkSizes = make(map[string]uint64, 0)
	for _, link := range dagNode.Links() {
		node.LinkHashes = append(node.LinkHashes, link.Cid.String())
		node.LinkSizes[link.Cid.String()] = link.Size
	}
	pb := new(ftpb.Data)
	if err := proto.Unmarshal(dagNode.(*ml.ProtoNode).Data(), pb); err != nil {
		return nil, err
	}
	node.Data = pb.GetData()
	node.FileSize = pb.GetFilesize()
	node.Blocksizes = pb.GetBlocksizes()
	// fmt.Printf("GetType:%v\n", pb.GetType())
	// fmt.Printf("GetData:%v\n", len(pb.GetData()))
	// fmt.Printf("GetFilesize:%v\n", pb.GetFilesize())
	// fmt.Printf("GetBlocksizes:%v\n", pb.GetBlocksizes())
	// fmt.Printf("GetHashType:%v\n", pb.GetHashType())
	// fmt.Printf("GetFanout:%v\n", pb.GetFanout())
	return node, nil
}

func (this *Fs) ProtoNodeToBlock(node *ProtoNode) (blocks.Block, error) {
	pb := new(ftpb.Data)
	pb.Type = ftpb.Data_File.Enum()
	if len(node.Data) > 0 {
		pb.Data = node.Data
	}
	pb.Filesize = new(uint64)
	*pb.Filesize = node.FileSize
	pb.Blocksizes = node.Blocksizes
	data, err := proto.Marshal(pb)
	if err != nil {
		return nil, err
	}
	fmt.Printf("data:%v, len:%d\n", data, len(data))
	root := ml.NodeWithData(data)
	for _, hash := range node.LinkHashes {
		c, err := cid.Decode(hash)
		if err != nil {
			return nil, err
		}
		size := node.LinkSizes[hash]
		l1 := &ipld.Link{
			Name: "",
			Size: size,
			Cid:  c,
		}
		root.AddRawLink("", l1)
	}
	buf, err := root.EncodeProtobuf(false)
	if err != nil {
		return nil, err
	}
	block := this.EncodedToBlock(buf)
	return block, nil
}
