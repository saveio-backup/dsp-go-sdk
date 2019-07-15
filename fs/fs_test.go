package fs

import (
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/saveio/dsp-go-sdk/config"
	chain "github.com/saveio/themis-go-sdk"
	"github.com/saveio/themis-go-sdk/wallet"
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
	fileRoot, err := filepath.Abs("../testdata")
	if err != nil {
		t.Fatal(err)
		return
	}
	dspCfg := &config.DspConfig{
		FsRepoRoot: fileRoot + "/testdata/max_test",
		FsFileRoot: fileRoot,
		FsType:     config.FS_FILESTORE,
	}
	fs, _ := NewFs(dspCfg, nil)
	root, l, err := fs.NodesFromFile(testbigFile, "AWaE84wqVf1yffjaR6VJ4NptLdqBAm8G9c", false, "")
	if err != nil {
		fmt.Printf("node from file err:%s\n", err)
		return
	}
	rootData := fs.BlockDataOfAny(root)
	fmt.Printf("rootData :%d\n", len(rootData))
	rootBlock := fs.EncodedToBlockWithCid(rootData, root.Cid().String())
	fmt.Printf("blockDataLen:%d\n", len(fs.BlockData(rootBlock)))
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
	fileRoot, err := filepath.Abs("../testdata")
	if err != nil {
		t.Fatal(err)
		return
	}
	dspCfg := &config.DspConfig{
		DBPath:       fmt.Sprintf("%s/db%d", fileRoot, 1),
		FsRepoRoot:   fileRoot + "/max1",
		FsFileRoot:   fileRoot,
		FsType:       config.FS_BLOCKSTORE,
		FsGcPeriod:   "1h",
		FsMaxStorage: "10G",
		ChainRpcAddr: "http://localhost:20336",
	}
	c := chain.NewChain()
	c.NewRpcClient().SetAddress(dspCfg.ChainRpcAddr)
	w, err := wallet.OpenWallet(fileRoot + "/wallet.dat")
	if err != nil {
		t.Fatal(err)
	}
	acc, err := w.GetDefaultAccount([]byte("pwd"))
	if err != nil {
		t.Fatal(err)
	}
	c.SetDefaultAccount(acc)
	fs, _ := NewFs(dspCfg, c)
	if fs == nil {
		t.Fatal("fs is nil")
	}
	blk := fs.GetBlock("zb2rhfrCjaF8LnRoBC7VjLhyH34te5hxTKm4w4KUxrrYHFJnE")
	if blk == nil {
		t.Fatal("block is nil")
	}
	fmt.Printf("blk type:%T", blk)
	blockData := fs.BlockDataOfAny(blk)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("raw data len:%d\n", len(blockData))

}

func TestNewFs(t *testing.T) {
	fileRoot, err := filepath.Abs("../testdata")
	if err != nil {
		t.Fatal(err)
		return
	}
	dspCfg := &config.DspConfig{
		FsRepoRoot: fileRoot + "/testdata/max_test",
		FsFileRoot: fileRoot,
		FsType:     config.FS_FILESTORE,
	}
	fs, _ := NewFs(dspCfg, nil)
	if fs == nil {
		t.Fatal(fs)
	}
	fmt.Printf("fs :%v\n", fs)

}

func Test2GetBlockFromFileStore(t *testing.T) {
	prefix := "AWaE84wqVf1yffjaR6VJ4NptLdqBAm8G9c"
	fileRoot, err := filepath.Abs("../testdata")
	if err != nil {
		t.Fatal(err)
		return
	}
	dspCfg := &config.DspConfig{
		FsRepoRoot: fileRoot + "/testdata/max_test",
		FsFileRoot: fileRoot,
		FsType:     config.FS_FILESTORE,
	}
	fs2, _ := NewFs(dspCfg, nil)
	fullFilePath := "../testdata/QmUQTgbTc1y4a8cq1DyA548B71kSrnVm7vHuBsatmnMBib"
	l0data := []byte{}
	for i := 850000; i <= 887443; i++ {
		l0data = append(l0data, []byte(fmt.Sprintf("%d\n", i))...)
	}
	l0data = append(l0data, []byte("88")...)
	l0Blk := fs2.EncodedToBlockWithCid(append([]byte(prefix), l0data...), "zb2rhfrCjaF8LnRoBC7VjLhyH34te5hxTKm4w4KUxrrYHFJnE")
	fs2.SetFsFilePrefix(fullFilePath, prefix)
	// file, err := os.OpenFile(fullFilePath, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0666)
	// if err != nil {
	// 	t.Fatal(err)
	// }
	// defer file.Close()
	// file.Write(l0data)

	rootData, err := hex.DecodeString("122c0a240155122088c71c6403d98c01640fd69eb2e00dc3c7e8e718d722561627874983189990f3120018808010122c0a2401551220b12d156c4bb5e423c993466b9e22d011f2530c8981aa0aea93c771726f1d1471120018808010122c0a2401551220256df15f50add013e1e66475da6e0c91b01da378ac6e9ef898df10dc07d60ef912001882dd0a0a1208021882dd2a20808010208080102082dd0a")
	if err != nil {
		t.Fatal(err)
	}
	root := fs2.EncodedToBlockWithCid(rootData, "QmUQTgbTc1y4a8cq1DyA548B71kSrnVm7vHuBsatmnMBib")
	fs2.PutBlockForFileStore(fullFilePath, root, 0)

	blk1 := fs2.GetBlock("QmUQTgbTc1y4a8cq1DyA548B71kSrnVm7vHuBsatmnMBib")
	fmt.Printf("blk1:%s\n", blk1.Cid())

	l0data = append([]byte(prefix), l0data...)

	fmt.Printf("l0Data :%d, %x, %s\n", len(l0data), sha1.Sum(l0data), l0Blk.Cid())
	fs2.PutBlockForFileStore(fullFilePath, l0Blk, 0)
	blk2 := fs2.GetBlock(l0Blk.Cid().String())
	fmt.Printf("blk2 :%v from:%s\n", blk2, l0Blk.Cid().String())

}

func TestDownloadFile(t *testing.T) {
	cfg := &config.DspConfig{
		FsRepoRoot: "test",
		FsFileRoot: "download",
	}
	fs, err := NewFs(cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	prefix := "AXjQn2mds5Saqka9ZiZke6fpJC4gM1QVm6"
	root, list, err := fs.NodesFromFile("./win-scatter-10.1.2.exe", prefix, false, "")
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("root %s\n", root.Cid().String())
	file, err := os.OpenFile("./result", os.O_CREATE|os.O_APPEND|os.O_RDWR, 0666)
	if err != nil {
		t.Fatal(err)
	}
	for idx, item := range list {
		data, err := item.GetDagNode()
		if err != nil {
			t.Fatal(err)
		}
		rootdata := fs.BlockData(data)
		if len(rootdata) == 0 {
			fmt.Printf("idx is 0 %d", idx)
			continue
		}
		if len(data.Links()) > 0 {
			continue
		}
		if string(rootdata[:len(prefix)]) == prefix {
			fmt.Printf("cut header %v %v %v, len %d\n", rootdata[:len(prefix)], []byte(prefix), rootdata[len(prefix)-2:len(prefix)+20], len(rootdata[len(prefix):]))
			file.Write(rootdata[len(prefix):])
			continue
		}
		fmt.Printf("write block %s-%d, len %d\n", data.Cid(), idx, len(rootdata))
		file.Write(rootdata[:])
	}
}
