package fs

import (
	"fmt"
	"io/ioutil"
	"os"
	"testing"

	"github.com/oniio/dsp-go-sdk/common"
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
		RepoRoot:  "../testdata/onifs3",
		FsRoot:    "../testdata",
		FsType:    FS_FILESTORE,
		ChunkSize: common.CHUNK_SIZE,
	})
	root, l, err := fs.NodesFromFile(testbigFile, "AWaE84wqVf1yffjaR6VJ4NptLdqBAm8G9c", false, "")
	if err != nil {
		fmt.Printf("node from file err:%s\n", err)
		return
	}
	fmt.Printf("root:%s, child len:%d\n", root.Cid().String(), len(l))
	block, _ := l[3].GetDagNode()
	rawData := fs.BlockDataOfAny(l[3])
	if err != nil {
		fmt.Printf("bytes to block err:%s\n", err)
		return
	}
	fmt.Printf("block content, len:%d\n", len(rawData))
	encodedBlock := fs.EncodedToBlockWithCid(rawData, block.Cid().String())
	fmt.Printf("block cid:%s, encoded %s\n", block.Cid().String(), encodedBlock.Cid().String())
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
		RepoRoot:  "./onifs2",
		FsRoot:    "./onifs2",
		FsType:    oniFs.FS_FILESTORE,
		ChunkSize: common.CHUNK_SIZE,
	})
	root, _, err := fs.NodesFromFile("./fs.go", "123", false, "")
	if err != nil {
		fmt.Printf("err:%v\n", err)
		return
	}
	// fmt.Printf("root:%s\n", root.Cid().String())
	// err = fs.PutBlock("", root, common.BLOCK_STORE_TYPE_NORMAL)
	// if err != nil {
	// 	fmt.Printf("put block err %s\n", err)
	// 	return
	// }
	blk := fs.GetBlock(root.Cid().String())
	if blk == nil {
		fmt.Printf("block is nil\n")
		return
	}
	fmt.Printf("cid %s\n", blk.Cid().String())
	fmt.Printf("raw data :%s\n", blk.RawData())

}
