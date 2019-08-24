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
	"github.com/saveio/dsp-go-sdk/utils"
	chain "github.com/saveio/themis-go-sdk"
	"github.com/saveio/themis-go-sdk/wallet"
	"github.com/saveio/themis/common"
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
		return
	}
	hashMap := make(map[string]struct{})
	for i, l := range list {
		if i == 0 {
			fmt.Printf("#%v hash = %s\n", i, l)
		}
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

func TestDecryptPrefixedFile(t *testing.T) {
	cfg := &config.DspConfig{
		FsRepoRoot: "./Repo",
		FsFileRoot: "./Downloads",
	}
	fs, err := NewFs(cfg, nil)
	if err != nil {
		t.Fatal(err)
	}

	encryptedFile, err := os.OpenFile(testFileEncrypted, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0666)
	if err != nil {
		t.Fatal(err)
	}
	defer encryptedFile.Close()
	prefix := make([]byte, utils.PREFIX_LEN)
	_, err = encryptedFile.ReadAt(prefix, 0)
	if err != nil {
		t.Fatal(err)
	}

	filePrefix := &utils.FilePrefix{}
	filePrefix.Deserialize(prefix)
	fmt.Printf("new prefix: %v\n", prefix)
	fmt.Printf("version: %d\n", filePrefix.Version)
	fmt.Printf("encrypt: %t\n", filePrefix.Encrypt)
	fmt.Printf("salt: %v\n", filePrefix.EncryptSalt)
	fmt.Printf("hash: %v\n", filePrefix.EncryptHash)
	fmt.Printf("owner: %s\n", filePrefix.Owner.ToBase58())
	fmt.Printf("fileSize: %d\n", filePrefix.FileSize)
	verify := utils.VerifyEncryptPassword(encryptPassword, filePrefix.EncryptSalt, filePrefix.EncryptHash)
	fmt.Printf("verify : %t\n", verify)
	fmt.Printf("len: %d\n", len(string(prefix)))
	err = fs.AESDecryptFile(testFileEncrypted, string(prefix), encryptPassword, testFileDecrypted)
	if err != nil {
		t.Fatal(err)
	}
}
