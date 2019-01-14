package fs

import (
	"fmt"
	"testing"

	ml "github.com/ontio/ont-ipfs-go-sdk/merkledag"
)

var testFile = "../testdata/testuploadfile.txt"

func TestBlockToBytes(t *testing.T) {
	fs := &Fs{}
	root, _, err := fs.NodesFromFile(testFile, "AYMnqA65pJFKAbbpD8hi5gdNDBmeFBy5hS", false, "")
	if err != nil {
		fmt.Printf("node from file err:%s\n", err)
		return
	}
	data := fs.BlockData(root)

	fmt.Printf("root:%s, data:%v\n", root.Cid().String(), data)
}

func TestBytesToBlock(t *testing.T) {
	fs := &Fs{}
	root, l, err := fs.NodesFromFile(testFile, "AYMnqA65pJFKAbbpD8hi5gdNDBmeFBy5hS", false, "")
	if err != nil {
		fmt.Printf("node from file err:%s\n", err)
		return
	}
	fmt.Printf("child len:%d\n", len(l))
	data := fs.BlockData(root)
	// fmt.Printf("root:%s, data:%v\n", root.Cid().String(), data)
	block := fs.BytesToBlock(data)
	// fmt.Printf("root:%s, block:%v\n", root.Cid().String(), block.Cid().String())
	dagNode, err := ml.DecodeProtobufBlock(block)
	if err != nil {
		fmt.Printf("DecodeProtobufBlock err:%s\n", err)
		return
	}
	fmt.Printf("links:%v\n", dagNode.Links()[0].Cid.String())
	rawData, err := fs.BlockToBytes(block)
	if err != nil {
		fmt.Printf("bytes to block err:%s\n", err)
		return
	}
	fmt.Printf("block content: %d\n", len(rawData))
}
