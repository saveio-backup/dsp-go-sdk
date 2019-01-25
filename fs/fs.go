package fs

import (
	"fmt"
	ipld "gx/ipfs/Qme5bWv7wtjUNGsK2BNGVUFPKiuxWrsqrtvYwCLRw8YFES/go-ipld-format"
	"os"

	"gx/ipfs/Qmej7nf81hi2x2tvjRBF3mcp74sQyuDH4VMYDGd1YtXjb2/go-block-format"

	"github.com/gogo/protobuf/proto"
	"github.com/oniio/dsp-go-sdk/common"
	"github.com/oniio/oniChain/common/log"
	oniFs "github.com/ontio/ont-ipfs-go-sdk"
	"github.com/ontio/ont-ipfs-go-sdk/importer/helpers"
	ml "github.com/ontio/ont-ipfs-go-sdk/merkledag"
	ftpb "github.com/ontio/ont-ipfs/unixfs/pb"
)

type Fs struct{}

func (this *Fs) NodesFromFile(fileName string, filePrefix string, encrypt bool, password string) (ipld.Node, []*helpers.UnixfsNode, error) {
	return oniFs.NodesFromFile(fileName, filePrefix, encrypt, password)
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
		return nil, err
	}

	pb := new(ftpb.Data)
	if err := proto.Unmarshal(dagNode.(*ml.ProtoNode).Data(), pb); err != nil {
		return nil, err
	}
	return pb.Data, nil
}

func (this *Fs) BytesToBlock(data []byte) blocks.Block {
	return blocks.NewBlock(data)
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
	dagNode, _ := ml.DecodeProtobufBlock(block)
	if dagNode != nil {
		rawData, err := this.BlockToBytes(block)
		log.Debugf("rawData len:%d, err:%s", len(rawData), err)
		if err == nil {
			bigF, _ := os.OpenFile("bigfile.txt", os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0666)
			defer bigF.Close()
			bigF.Write(rawData)
		}
		log.Debugf("put block cid:%s, links:%v\n", block.Cid().String(), dagNode.Links())
	} else {
		log.Debugf("put block but dagnode is nil\n")
	}
	return nil
}

func (this *Fs) PutTag(key string, tag []byte) error {
	return nil
}

func (this *Fs) StartPDPVerify(fileHashStr string) error {
	return nil
}

// GetBlock get blocks
func (this *Fs) GetBlock(hash string) blocks.Block {
	return nil
}

// DeleteFile. delete file, unpin root block if needed
// If a block is referenced to other file, ignore it.
func (this *Fs) DeleteFile(fileHashStr string) error {
	return nil
}
