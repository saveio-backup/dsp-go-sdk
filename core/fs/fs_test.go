package fs

import (
	"crypto/rand"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/saveio/dsp-go-sdk/config"
	"github.com/saveio/dsp-go-sdk/utils"
	"github.com/saveio/max/merkledag"
	ml "github.com/saveio/max/merkledag"
	chain "github.com/saveio/themis-go-sdk"
	"github.com/saveio/themis-go-sdk/wallet"
	"github.com/saveio/themis/common"
	"github.com/saveio/themis/common/log"
)

var testbigFile = "../testdata/testuploadbigfile.txt"
var testsmallFile = "../testdata/testuploadfile.txt"
var walletAddress = "ANPCCWJbCimJ2fsohgkrziUeQGikRpMuez"
var testFile = "./setup.exe"
var testFileEncrypted = "./setup-encrypted.exe"
var testFileDecrypted = "./setup-decrypted.exe"
var testFileTemp = "./setup-temp.exe"
var encryptPassword = "123456"

func TestNodeFromFile(t *testing.T) {
	log.InitLog(1, log.Stdout, "./Log/")
	repoPath := "./Repo"
	downloadPath := "./Downloads"

	defer os.RemoveAll(repoPath)
	defer os.RemoveAll(downloadPath)
	defer os.RemoveAll("./Log")
	cfg := &config.DspConfig{
		FsRepoRoot: repoPath,
		FsFileRoot: downloadPath,
	}
	fs, err := NewFs(cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	list, err := fs.NodesFromFile("./T7-uploader.zip", "AAAAWg==AQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAADXFrE901BkX/lt3EyP1U4rB8YyzQAAAAAAADQvD1Q3LXVwbG9hZGVyLnppcAAAAABpc06H", false, "")
	if err != nil {
		return
	}
	hashMap := make(map[string]struct{})
	for i, l := range list {
		fmt.Printf("#%v hash = %s\n", i, l)
		_, ok := hashMap[l]
		if ok {
			fmt.Printf("duplicated %s", l)
		}
		hashMap[l] = struct{}{}
	}
}

func TestBlockToBytes(t *testing.T) {
	cfg := &config.DspConfig{
		FsRepoRoot: "./Repo",
		FsFileRoot: "./Downloads",
	}
	fs, err := NewFs(cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	list, err := fs.NodesFromFile(testbigFile, "", false, "")
	if err != nil {
		fmt.Printf("node from file err:%s\n", err)
		return
	}
	root := fs.GetBlock(list[0])
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
	list, err := fs.NodesFromFile(testbigFile, "AWaE84wqVf1yffjaR6VJ4NptLdqBAm8G9c", false, "")
	if err != nil {
		fmt.Printf("node from file err:%s\n", err)
		return
	}
	root := fs.GetBlock(list[0])
	rootData := fs.BlockDataOfAny(root)
	fmt.Printf("rootData :%d\n", len(rootData))
	rootBlock := fs.EncodedToBlockWithCid(rootData, root.Cid().String())
	fmt.Printf("blockDataLen:%d\n", len(fs.BlockData(rootBlock)))
	rootBytes, _ := fs.BlockToBytes(root)
	fmt.Printf("rootBlock cid:%s, len:%d, lenbtes :%d \n", rootBlock.Cid(), len(rootBlock.RawData()), len(rootBytes))
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
		DBPath:        fmt.Sprintf("%s/db%d", fileRoot, 1),
		FsRepoRoot:    fileRoot + "/max1",
		FsFileRoot:    fileRoot,
		FsType:        config.FS_BLOCKSTORE,
		FsGcPeriod:    "1h",
		FsMaxStorage:  "10G",
		ChainRpcAddrs: []string{"http://localhost:20336"},
	}
	c := chain.NewChain()
	c.NewRpcClient().SetAddress(dspCfg.ChainRpcAddrs)
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
		FsRepoRoot: "./Repo",
		FsFileRoot: "./Downloads",
	}
	fs, err := NewFs(cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	list, err := fs.NodesFromFile(testFile, walletAddress, false, "")
	if err != nil {
		t.Fatal(err)
	}
	root := fs.GetBlock(list[0])
	fmt.Printf("root %s\n", root.Cid().String())
	file, err := os.OpenFile("./result", os.O_CREATE|os.O_APPEND|os.O_RDWR, 0666)
	if err != nil {
		t.Fatal(err)
	}

	defer file.Close()
	for idx, hash := range list {
		block := fs.GetBlock(hash)
		blockData := fs.BlockData(block)
		if len(blockData) == 0 {
			fmt.Printf("idx is 0 %d", idx)
			continue
		}
		links, err := fs.GetBlockLinks(block)
		if err != nil {
			t.Fatal(err)
		}
		if len(links) > 0 {
			continue
		}
		if string(blockData[:len(walletAddress)]) == walletAddress {
			file.Write(blockData[len(walletAddress):])
			continue
		}
		file.Write(blockData[:])
	}
}

func TestEncryptFile(t *testing.T) {
	cfg := &config.DspConfig{
		FsRepoRoot: "./Repo",
		FsFileRoot: "./Downloads",
	}
	fs, err := NewFs(cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	err = fs.AESEncryptFile(testFile, encryptPassword, testFile+"-decrypted")
	if err != nil {
		t.Fatal(err)
	}
}

func TestDecryptFile(t *testing.T) {
	cfg := &config.DspConfig{
		FsRepoRoot: "./Repo",
		FsFileRoot: "./Downloads",
	}
	fs, err := NewFs(cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	err = fs.AESDecryptFile(testFile+"-decrypted", "", encryptPassword, testFile+"-origin")
	if err != nil {
		t.Fatal(err)
	}
}

func TestEncryptNodeToFiles(t *testing.T) {
	cfg := &config.DspConfig{
		FsRepoRoot: "./Repo",
		FsFileRoot: "./Downloads",
	}
	fs, err := NewFs(cfg, nil)
	if err != nil {
		t.Fatal(err)
	}

	addr, err := common.AddressFromBase58(walletAddress)
	if err != nil {
		t.Fatal(err)
	}

	prefix := &utils.FilePrefix{
		Version:    1,
		Encrypt:    true,
		EncryptPwd: encryptPassword,
		Owner:      addr,
	}

	prefixBytes := prefix.Serialize()
	prefixStr := string(prefixBytes)
	fmt.Printf("prefix :%v\n", prefixBytes)
	list, err := fs.NodesFromFile(testFile, prefixStr, prefix.Encrypt, prefix.EncryptPwd)
	if err != nil {
		t.Fatal(err)
	}
	newFile, err := os.OpenFile(testFileEncrypted, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0666)
	if err != nil {
		t.Fatal(err)
	}
	defer newFile.Close()

	for _, l := range list {
		block := fs.GetBlock(l)
		links, _ := fs.GetBlockLinks(block)
		if len(links) > 0 {
			continue
		}
		data := fs.BlockDataOfAny(block)
		if !prefix.Encrypt && string(data[:len(prefixBytes)]) == string(prefixBytes) {
			newFile.Write(data[len(prefixBytes):])
			continue
		}
		newFile.Write(data)
	}
}

func TestReadBlock(t *testing.T) {
	cfg := &config.DspConfig{
		FsRepoRoot: "/Users/zhijie/Desktop/onchain/save-test/node5/FS/AFoUr6dKxGCAcx74nKfBBWavRCyNcengbJ",
		FsType:     0,
	}
	fs, err := NewFs(cfg, nil)
	if err != nil {
		t.Fatal(err)
	}

	block := fs.GetBlock("zb2rhZTC99p7TDkwcKLtoKNkzm16WwCfyAQekifTe6v2JLu4G")
	fmt.Printf("block: %v\n", block)
}

func TestGetAllCid(t *testing.T) {
	cfg := &config.DspConfig{
		FsRepoRoot: "./Repo",
		FsFileRoot: "./Downloads",
	}
	fs, err := NewFs(cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	hash, err := fs.NodesFromFile("./setup.exe", "AQGt++s87eSu6qgQvNcaEj7SFqUvDiPZ6c2D70HTVFlyIcsr0t8TbAC0qZqQYyxOEOIHzr/C5t1JGgAAAAAAAP84AAAAADipJRM=", true, "123")
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("len: %d\n", len(hash))
	for i, h := range hash {
		fmt.Printf("i = %d, h = %s\n", i, h)
	}
	cids, err := fs.GetFileAllHashes(hash[0])
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("len: %d\n", len(cids))
	for i, h := range cids {
		fmt.Printf("i = %d, h = %s\n", i, h)
	}
}

func TestPutBlockAndTag(t *testing.T) {
	log.InitLog(1, log.Stdout, "./Log/")
	repoPath := "./Repo"
	downloadPath := "./Downloads"

	defer os.RemoveAll(repoPath)
	defer os.RemoveAll(downloadPath)
	cfg := &config.DspConfig{
		FsRepoRoot: repoPath,
		FsFileRoot: downloadPath,
		FsType:     config.FS_BLOCKSTORE,
	}
	fs, err := NewFs(cfg, nil)
	if err != nil {
		t.Fatal(err)
	}

	blockCount := 10000
	blockPerFile := 200
	CHUNK_SIZE := 256 * 1024
	fileCnt := blockCount / blockPerFile
	buf := make([]byte, blockCount*CHUNK_SIZE)
	rand.Read(buf)
	type BlockData struct {
		Blk   *merkledag.RawNode
		Block []byte
		Hash  string
		Tag   []byte
	}
	blocksDataMap := make(map[int]*BlockData)
	for i := 0; i < blockCount; i++ {
		start := i * CHUNK_SIZE
		end := (i + 1) * CHUNK_SIZE
		block := ml.NewRawNode(buf[start:end])
		blocksDataMap[i] = &BlockData{
			Blk:  block,
			Hash: block.Cid().String(),
			Tag:  buf[start : start+100],
		}
	}
	wg := new(sync.WaitGroup)
	for i := 0; i < fileCnt; i++ {
		blks := make([]*BlockData, 0)
		for j := i * blockPerFile; j < (i+1)*blockPerFile; j++ {
			blks = append(blks, blocksDataMap[j])
		}
		wg.Add(1)
		go func(datas []*BlockData) {
			defer wg.Done()
			fileHashStr := datas[0].Hash
			log.Infof("test %v", fileHashStr)
			for index, value := range datas {
				// blk := fs.EncodedToBlockWithCid(value.Block, value.Hash)
				// if blk.Cid().String() != value.Hash {
				// 	t.Fatalf("receive a wrong block: %s, expected: %s", blk.Cid().String(), value.Hash)
				// }
				blk := value.Blk
				if err := fs.PutBlock(blk); err != nil {
					t.Fatal(err)
				}
				log.Debugf("put block success %v-%s-%d", fileHashStr, value.Hash, index)
				if err := fs.PutTag(value.Hash, fileHashStr, uint64(index), value.Tag); err != nil {
					t.Fatal(err)
				}
				log.Debugf("put tag success %v-%s-%d", fileHashStr, value.Hash, index)
			}
			log.Infof("test %v done", fileHashStr)
		}(blks)
	}
	wg.Wait()
	log.Infof("test done")
}
