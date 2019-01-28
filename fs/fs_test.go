package fs

import (
	"fmt"
	"io/ioutil"
	"os"
	"testing"
)

var testbigFile = "../testdata/testuploadbigfile.txt"
var testsmallFile = "../testdata/testuploadfile.txt"

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

func TestBytesToBlock(t *testing.T) {
	fs := &Fs{}
	root, l, err := fs.NodesFromFile(testbigFile, "", false, "")
	if err != nil {
		fmt.Printf("node from file err:%s\n", err)
		return
	}
	fmt.Printf("root:%s, child len:%d\n", root.Cid().String(), len(l))
	// rawData, err := fs.BlockToBytes(root)
	rawData := fs.BlockData(root)
	if err != nil {
		fmt.Printf("bytes to block err:%s\n", err)
		return
	}
	fmt.Printf("block content:%v, len:%d\n", rawData[:], len(rawData))
	block := fs.BytesToBlock(rawData)
	fmt.Printf("block cid:%s\n", block.Cid().String())
}

func TestDecodeProtoNode(t *testing.T) {
	fs := &Fs{}
	root, l, err := fs.NodesFromFile(testbigFile, "", false, "")
	if err != nil {
		fmt.Printf("node from file err:%s\n", err)
		return
	}
	fmt.Printf("root:%s, child len:%d\n", root.Cid().String(), len(l))
	// block, _ := l[0].GetDagNode()
	block := root
	node, err := fs.BlockToProtoNode(block)
	if err != nil {
		fmt.Printf("block to proto node err:%s\n", err)
		return
	}
	fmt.Printf("GetData:%v\n", len(node.Data))
	fmt.Printf("GetFilesize:%v\n", node.FileSize)
	fmt.Printf("GetBlocksizes:%v\n", node.Blocksizes)
}

func TestEncodeProtoNode(t *testing.T) {
	node := new(ProtoNode)
	node.Data = []byte{}
	node.FileSize = 700000
	node.Blocksizes = []uint64{262144, 262144, 175712}
	node.LinkHashes = []string{"QmZgSgH8xjTp6jY95pV5PRKKKNGLSuHddCchQbL1LC45pm", "QmW2WwMVh6BaVgckyCwMEsUG5edgDKcL41aveWKdCycVCx", "QmSvBSQG3G3ct8Nq6MnXXjXvLbHFBTdMa8ErR9q5bhAPWG"}
	node.LinkSizes = map[string]uint64{
		"QmZgSgH8xjTp6jY95pV5PRKKKNGLSuHddCchQbL1LC45pm": 262158,
		"QmW2WwMVh6BaVgckyCwMEsUG5edgDKcL41aveWKdCycVCx": 262158,
		"QmSvBSQG3G3ct8Nq6MnXXjXvLbHFBTdMa8ErR9q5bhAPWG": 175726,
	}
	fs := &Fs{}
	block, err := fs.ProtoNodeToBlock(node)
	if err != nil {
		return
	}
	fmt.Printf("block:%v\n", block.Cid().String())
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
