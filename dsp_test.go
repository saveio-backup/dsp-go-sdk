package dsp

import (
	"fmt"
	"testing"
	"time"

	"github.com/oniio/oniChain-go-sdk/wallet"

	"github.com/oniio/dsp-go-sdk/common"
	"github.com/oniio/dsp-go-sdk/config"
	"github.com/oniio/oniChain/common/log"
)

var rpcAddr = "http://127.0.0.1:20336"
var node1ListAddr = "tcp://127.0.0.1:4001"
var node2ListAddr = "tcp://127.0.0.1:4002"
var node3ListAddr = "tcp://127.0.0.1:4003"
var node4ListAddr = "tcp://127.0.0.1:4004"

var uploadTestFile = "./testdata/testuploadbigfile.txt"

// var uploadTestFile = "./testdata/testuploadfile.txt"
var walletFile = "./testdata/wallet.dat"
var wallet2File = "./testdata/wallet2.dat"
var wallet3File = "./testdata/wallet3.dat"
var wallet4File = "./testdata/wallet4.dat"
var wallet1Addr = "AYMnqA65pJFKAbbpD8hi5gdNDBmeFBy5hS"
var walletPwd = "pwd"

func init() {
	log.InitLog(1, log.PATH, log.Stdout)
}

func TestChainGetBlockHeight(t *testing.T) {
	dspCfg := &config.DspConfig{
		ChainRpcAddr: rpcAddr,
	}
	d := NewDsp(dspCfg)
	height, err := d.Chain.GetCurrentBlockHeight()
	if err != nil {
		fmt.Printf("get block height err: %s", err)
		return
	}
	fmt.Printf("current block height: %d\n", height)
}
func TestGetVersion(t *testing.T) {
	dspCfg := &config.DspConfig{
		ChainRpcAddr: rpcAddr,
	}
	d := NewDsp(dspCfg)
	version := d.GetVersion()
	fmt.Printf("version: %s\n", version)
}

func TestNodeRegister(t *testing.T) {
	dspCfg := &config.DspConfig{
		ChainRpcAddr: rpcAddr,
	}
	d := NewDsp(dspCfg)
	w, err := wallet.OpenWallet(walletFile)
	// w, err := wallet.OpenWallet(wallet4File)
	if err != nil {
		log.Errorf("open wallet err:%s\n", err)
		return
	}
	acc, err := w.GetDefaultAccount([]byte(walletPwd))
	if err != nil {
		log.Errorf("get default acc err:%s\n", err)
		return
	}
	// register 512G for 12 hours
	d.Chain.SetDefaultAccount(acc)
	tx, err := d.RegisterNode(node1ListAddr, 512*1024*1024, 12)
	// tx, err := d.RegisterNode(node4ListAddr, 512*1024*1024, 12)
	if err != nil {
		log.Errorf("register node err:%s", err)
		return
	}
	log.Infof("tx: %s", tx)
}

func TestNodeUnregister(t *testing.T) {
	dspCfg := &config.DspConfig{
		ChainRpcAddr: rpcAddr,
	}
	d := NewDsp(dspCfg)
	w, err := wallet.OpenWallet(walletFile)
	if err != nil {
		log.Errorf("open wallet err:%s\n", err)
		return
	}
	acc, err := w.GetDefaultAccount([]byte(walletPwd))
	if err != nil {
		log.Errorf("get default acc err:%s\n", err)
		return
	}
	d.Chain.SetDefaultAccount(acc)
	tx, err := d.UnregisterNode()
	if err != nil {
		log.Errorf("register node err:%s", err)
		return
	}
	log.Infof("tx: %s", tx)
}

func TestNodeQuery(t *testing.T) {
	dspCfg := &config.DspConfig{
		ChainRpcAddr: rpcAddr,
	}
	d := NewDsp(dspCfg)
	info, err := d.QueryNode(wallet1Addr)
	if err != nil {
		log.Errorf("query node err %s", err)
		return
	}
	log.Infof("node info pledge %d", info.Pledge)
	log.Infof("node info profit %d", info.Profit)
	log.Infof("node info volume %d", info.Volume)
	log.Infof("node info restvol %d", info.RestVol)
	log.Infof("node info service time %d", info.ServiceTime)
	log.Infof("node info wallet address %s", info.WalletAddr.ToBase58())
	log.Infof("node info node address %s", info.NodeAddr)
}

func TestNodeUpdate(t *testing.T) {
	dspCfg := &config.DspConfig{
		ChainRpcAddr: rpcAddr,
	}
	d := NewDsp(dspCfg)
	w, err := wallet.OpenWallet(walletFile)
	if err != nil {
		log.Errorf("open wallet err:%s\n", err)
		return
	}
	acc, err := w.GetDefaultAccount([]byte(walletPwd))
	if err != nil {
		log.Errorf("get default acc err:%s\n", err)
		return
	}
	d.Chain.SetDefaultAccount(acc)
	tx, err := d.UpdateNode(node1ListAddr, 0, 13)
	if err != nil {
		log.Errorf("update node err:%s", err)
		return
	}
	log.Infof("tx: %s", tx)
}

func TestNodeWithdrawProfit(t *testing.T) {
	dspCfg := &config.DspConfig{
		ChainRpcAddr: rpcAddr,
	}
	d := NewDsp(dspCfg)
	w, err := wallet.OpenWallet(walletFile)
	if err != nil {
		log.Errorf("open wallet err:%s\n", err)
		return
	}
	acc, err := w.GetDefaultAccount([]byte(walletPwd))
	if err != nil {
		log.Errorf("get default acc err:%s\n", err)
		return
	}
	d.Chain.SetDefaultAccount(acc)
	tx, err := d.NodeWithdrawProfit()
	if err != nil {
		log.Errorf("register node err:%s", err)
		return
	}
	log.Infof("tx: %s", tx)
}
func TestStartDspNode(t *testing.T) {
	log.InitLog(1, log.PATH, log.Stdout)
	dspCfg := &config.DspConfig{
		DBPath:       "./testdata/db1",
		FsRepoRoot:   "./testdata/onifs1",
		FsFileRoot:   "./testdata",
		FsType:       config.FS_BLOCKSTORE,
		ChainRpcAddr: rpcAddr,
	}
	d := NewDsp(dspCfg)
	w, err := wallet.OpenWallet(walletFile)
	if err != nil {
		log.Errorf("open wallet err:%s\n", err)
		return
	}
	acc, err := w.GetDefaultAccount([]byte(walletPwd))
	if err != nil {
		log.Errorf("get default acc err:%s\n", err)
		return
	}
	log.Infof("wallet address:%s", acc.Address.ToBase58())
	d.Chain.SetDefaultAccount(acc)

	d.Start(node1ListAddr)
	go d.StartShareServices()
	tick := time.NewTicker(time.Second)
	for {
		<-tick.C
	}
}

func TestDspGetBlock(t *testing.T) {
	dspCfg := &config.DspConfig{
		DBPath:       "./testdata/db1",
		FsRepoRoot:   "./testdata/onifs1",
		FsFileRoot:   "./testdata",
		FsType:       config.FS_BLOCKSTORE,
		ChainRpcAddr: rpcAddr,
	}
	d := NewDsp(dspCfg)
	w, err := wallet.OpenWallet(walletFile)
	if err != nil {
		log.Errorf("open wallet err:%s\n", err)
		return
	}
	acc, err := w.GetDefaultAccount([]byte(walletPwd))
	if err != nil {
		log.Errorf("get default acc err:%s\n", err)
		return
	}
	log.Infof("wallet address:%s", acc.Address.ToBase58())
	d.Chain.SetDefaultAccount(acc)
	// blk := d.Fs.GetBlock("QmUQTgbTc1y4a8cq1DyA548B71kSrnVm7vHuBsatmnMBib")
	blk := d.Fs.GetBlock("zb2rhfrCjaF8LnRoBC7VjLhyH34te5hxTKm4w4KUxrrYHFJnE")
	fmt.Printf("block type :%d, strlen:%d, value:%s!\n", len(blk.RawData()), len("AWaE84wqVf1yffjaR6VJ4NptLdqBAm8G9c"), blk.RawData()[:34])
	blockData := d.Fs.BlockDataOfAny(blk)
	fmt.Printf("blk.cid %s, len:%d\n", blk.Cid().String(), len(blockData))
}

func TestStartDspNode4(t *testing.T) {
	log.InitLog(1, log.PATH, log.Stdout)
	dspCfg := &config.DspConfig{
		DBPath:       "./db3",
		FsRepoRoot:   "./onifs",
		FsFileRoot:   "./onifs",
		FsType:       config.FS_BLOCKSTORE,
		ChainRpcAddr: rpcAddr,
	}
	d := NewDsp(dspCfg)
	w, err := wallet.OpenWallet(wallet4File)
	if err != nil {
		log.Errorf("open wallet err:%s\n", err)
		return
	}
	acc, err := w.GetDefaultAccount([]byte(walletPwd))
	if err != nil {
		log.Errorf("get default acc err:%s\n", err)
		return
	}
	log.Infof("wallet address:%s", acc.Address.ToBase58())
	d.Chain.SetDefaultAccount(acc)

	d.Start(node4ListAddr)
	go d.StartShareServices()
	tick := time.NewTicker(time.Second)
	for {
		<-tick.C
	}
}

func TestNodeFromFile(t *testing.T) {
	dspCfg := &config.DspConfig{
		DBPath:       "./testdata/db2",
		FsRepoRoot:   "./testdata/onifs2",
		FsFileRoot:   "./testdata",
		FsType:       config.FS_FILESTORE,
		ChainRpcAddr: rpcAddr,
	}
	d := NewDsp(dspCfg)
	w, err := wallet.OpenWallet(wallet2File)
	if err != nil {
		log.Errorf("open wallet err:%s\n", err)
		return
	}
	acc, err := w.GetDefaultAccount([]byte(walletPwd))
	if err != nil {
		log.Errorf("get default acc err:%s\n", err)
		return
	}

	r, l, err := d.Fs.NodesFromFile(uploadTestFile, acc.Address.ToBase58(), false, "")
	if err != nil {
		fmt.Printf("nodes from file err %s", err)
		return
	}
	for _, li := range l {
		lid, _ := li.GetDagNode()
		fmt.Printf("li %s\n", lid.Cid())
	}
	fmt.Printf("r :%s, l:%d\n", r.Cid().String(), len(l))
}

func TestUploadFile(t *testing.T) {
	dspCfg := &config.DspConfig{
		DBPath:       "./testdata/db2",
		FsRepoRoot:   "./testdata/onifs2",
		FsFileRoot:   "./testdata",
		FsType:       config.FS_FILESTORE,
		ChainRpcAddr: rpcAddr,
	}
	d := NewDsp(dspCfg)
	d.Start(node2ListAddr)
	w, err := wallet.OpenWallet(wallet2File)
	if err != nil {
		log.Errorf("open wallet err:%s\n", err)
		return
	}
	acc, err := w.GetDefaultAccount([]byte(walletPwd))
	if err != nil {
		log.Errorf("get default acc err:%s\n", err)
		return
	}
	d.Chain.SetDefaultAccount(acc)
	log.Infof("wallet address:%s", acc.Address.ToBase58())
	opt := &common.UploadOption{
		FileDesc:        "file",
		ProveInterval:   110,
		ProveTimes:      3,
		Privilege:       1,
		CopyNum:         0,
		Encrypt:         false,
		EncryptPassword: "",
	}
	d.taskMgr.RegProgressCh()
	go func() {
		stop := false
		for {
			v := <-d.taskMgr.ProgressCh()
			for node, cnt := range v.Count {
				log.Infof("file:%s, hash:%s, total:%d, peer:%s, uploaded:%d, progress:%f", v.FileName, v.FileHash, v.Total, node, cnt, float64(cnt)/float64(v.Total))
				stop = (cnt == v.Total)
			}
			if stop {
				break
			}
		}
	}()
	ret, err := d.UploadFile(uploadTestFile, opt)
	if err != nil {
		log.Errorf("upload file failed, err:%s", err)
		return
	}
	log.Infof("upload file success, ret:%v", ret)
}

func TestDeleteFile(t *testing.T) {
	dspCfg := &config.DspConfig{
		DBPath:       "./testdata/db2",
		FsRepoRoot:   "./testdata/onifs2",
		FsFileRoot:   "./testdata",
		FsType:       config.FS_FILESTORE,
		ChainRpcAddr: rpcAddr,
	}
	d := NewDsp(dspCfg)
	d.Start(node2ListAddr)
	w, err := wallet.OpenWallet(wallet2File)
	if err != nil {
		log.Errorf("open wallet err:%s\n", err)
		return
	}
	acc, err := w.GetDefaultAccount([]byte(walletPwd))
	if err != nil {
		log.Errorf("get default acc err:%s\n", err)
		return
	}
	d.Chain.SetDefaultAccount(acc)
	ret, err := d.DeleteUploadedFile("QmUQTgbTc1y4a8cq1DyA548B71kSrnVm7vHuBsatmnMBib")
	if err != nil {
		log.Errorf("delete file failed, err:%s", err)
		return
	}
	log.Infof("delete file success, ret:%v", ret)
}

func TestDownloadFile(t *testing.T) {
	dspCfg := &config.DspConfig{
		DBPath:       "./testdata/db3",
		FsRepoRoot:   "./testdata/onifs3",
		FsFileRoot:   "./testdata",
		FsType:       config.FS_FILESTORE,
		ChainRpcAddr: rpcAddr,
	}
	d := NewDsp(dspCfg)
	d.Start(node3ListAddr)
	w, err := wallet.OpenWallet(wallet3File)
	if err != nil {
		log.Errorf("open wallet err:%s\n", err)
		return
	}
	acc, err := w.GetDefaultAccount([]byte(walletPwd))
	if err != nil {
		log.Errorf("get default acc err:%s\n", err)
		return
	}
	d.Chain.SetDefaultAccount(acc)
	log.Infof("wallet address:%s", acc.Address.ToBase58())
	d.taskMgr.RegProgressCh()
	go func() {
		stop := false
		for {
			v := <-d.taskMgr.ProgressCh()
			for node, cnt := range v.Count {
				log.Infof("file:%s, hash:%s, total:%d, peer:%s, downloaded:%d, progress:%f", v.FileName, v.FileHash, v.Total, node, cnt, float64(cnt)/float64(v.Total))
				stop = (cnt == v.Total)
			}
			if stop {
				break
			}
		}
	}()
	addrs := []string{node1ListAddr}
	err = d.DownloadFile("QmUQTgbTc1y4a8cq1DyA548B71kSrnVm7vHuBsatmnMBib", true, addrs, "")
	if err != nil {
		log.Errorf("download err %s\n", err)
	}
	// use for testing go routines for tasks are released or not
	time.Sleep(time.Duration(5) * time.Second)
}

func TestStartPDPVerify(t *testing.T) {
	log.InitLog(1, log.PATH, log.Stdout)
	dspCfg := &config.DspConfig{
		DBPath:       "./testdata/db1",
		FsRepoRoot:   "./testdata/onifs1",
		FsFileRoot:   "./testdata",
		FsType:       config.FS_BLOCKSTORE,
		ChainRpcAddr: rpcAddr,
	}
	d := NewDsp(dspCfg)
	w, err := wallet.OpenWallet(walletFile)
	if err != nil {
		log.Errorf("open wallet err:%s\n", err)
		return
	}
	acc, err := w.GetDefaultAccount([]byte(walletPwd))
	if err != nil {
		log.Errorf("get default acc err:%s\n", err)
		return
	}
	log.Infof("wallet address:%s", acc.Address.ToBase58())
	d.Chain.SetDefaultAccount(acc)
	d.Fs.StartPDPVerify("QmUQTgbTc1y4a8cq1DyA548B71kSrnVm7vHuBsatmnMBib", 0, 0, 0)
	tick := time.NewTicker(time.Second)
	for {
		<-tick.C
	}
}
