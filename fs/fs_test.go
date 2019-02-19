package fs

import (
	"fmt"
	"io/ioutil"
	"os"
	"testing"

	"github.com/oniio/dsp-go-sdk/common"
	"github.com/oniio/dsp-go-sdk/config"
	ml "github.com/oniio/oniFS/merkledag"
	oniFs "github.com/oniio/oniFS/onifs"
)

var testbigFile = "../testdata/testuploadbigfile.txt"
var testsmallFile = "../testdata/testuploadfile.txt"

func TestNodeFromFile(t *testing.T) {
	fs := &Fs{}
	root, list, err := fs.NodesFromFile(testbigFile, "", false, "")
	if err != nil {
		return
	}
	tree, err := fs.AllBlockHashes(root, list)
	if err != nil {
		return
	}
	fmt.Printf("tree:%v\n", tree)
}

func TestBlockToBytes(t *testing.T) {
	fs := &Fs{}
	root, _, err := fs.NodesFromFile(testbigFile, "", false, "")
	if err != nil {
		fmt.Printf("node from file err:%s\n", err)
		return
	}
	data := fs.BlockData(root)
	fmt.Printf("len:%d, blockdata:%v\n", len(data), data)

	blockBytes, err := fs.BlockToBytes(root)
	if err != nil {
		fmt.Printf("block to bytes err:%s\n", err)
		return
	}
	fmt.Printf("len:%d, blockbytes:%v\n", len(blockBytes), blockBytes)
}

func TestEncodedToBlock(t *testing.T) {
	fs := NewFs(&FsConfig{
		RepoRoot:  "../testdata/onifs_test",
		FsRoot:    "../testdata",
		FsType:    config.FS_FILESTORE,
		ChunkSize: common.CHUNK_SIZE,
	})
	root, l, err := fs.NodesFromFile(testbigFile, "AWaE84wqVf1yffjaR6VJ4NptLdqBAm8G9c", false, "")
	if err != nil {
		fmt.Printf("node from file err:%s\n", err)
		return
	}
	rootData := fs.BlockDataOfAny(root)
	fmt.Printf("rootData :%d\n", len(rootData))
	rootBlock := fs.EncodedToBlockWithCid(rootData, root.Cid().String())
	rootBytes, _ := fs.BlockToBytes(root)
	fmt.Printf("rootBlock cid:%s, len:%d, lenbtes :%d \n", rootBlock.Cid(), len(rootBlock.RawData()), len(rootBytes))
	for _, rootC := range root.Links() {
		fmt.Printf("rootC %s\n", rootC.Cid.String())
	}

	fmt.Printf("root:%s, child len:%d\n", root.Cid().String(), len(l))
	for _, item := range l {
		dagNode, _ := item.GetDagNode()
		rawData := fs.BlockDataOfAny(item)
		itemBytes, _ := fs.BlockToBytes(dagNode)
		block := fs.EncodedToBlockWithCid(rawData, dagNode.Cid().String())
		fmt.Printf("block type %s, rawData len:%d, data:%d, bytes:%d\n", block.Cid(), len(rawData), len(block.RawData()), len(itemBytes))
	}
}

func TestReadWithOffset(t *testing.T) {
	fi, err := os.Open(testsmallFile)
	if err != nil {
		return
	}
	defer fi.Close()
	buf := make([]byte, 2)
	fi.ReadAt(buf, 2)
	fmt.Printf("buf :%v\n", buf)
	all, _ := ioutil.ReadFile(testsmallFile)
	fmt.Printf("all:%v\n", all)
}

func TestWriteAtOffset(t *testing.T) {
	fi, _ := os.OpenFile("./temp.txt", os.O_CREATE|os.O_APPEND|os.O_RDWR, 0666)
	buf1 := []byte("hello")
	buf2 := []byte("world")
	fi.WriteAt(buf2, int64(len(buf1)))
	fi.WriteAt(buf1, 0)
	fi.Close()
}

func TestGetBlock(t *testing.T) {
	fs := NewFs(&FsConfig{
		RepoRoot:  "../testdata/onifs_test",
		FsRoot:    "../testdata",
		FsType:    oniFs.FS_BLOCKSTORE,
		ChunkSize: common.CHUNK_SIZE,
	})
	root, _, err := fs.NodesFromFile(testbigFile, "AWaE84wqVf1yffjaR6VJ4NptLdqBAm8G9c", false, "")
	if err != nil {
		fmt.Printf("err:%v\n", err)
		return
	}
	err = fs.PutBlock(root)
	if err != nil {
		fmt.Printf("put block err %s\n", err)
		return
	}
	blk := fs.GetBlock(root.Cid().String())
	if blk == nil {
		fmt.Printf("block is nil\n")
		return
	}
	// for *ml.ProtoNode, it has links
	dagNode, err := ml.DecodeProtobufBlock(blk)
	if err != nil {
		return
	}
	ls, err := fs.BlockLinks(blk)
	if err != nil {
		return
	}
	fmt.Printf("links :%v, ls:%v\n", dagNode.Links(), ls)
	fmt.Printf("cid %s\n", blk.Cid().String())
	blockData := fs.BlockDataOfAny(blk)
	fmt.Printf("raw data :%d\n", len(blockData))

}
