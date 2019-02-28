package dsp

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/oniio/dsp-go-sdk/utils"

	"github.com/oniio/oniChain-go-sdk/wallet"

	"github.com/oniio/dsp-go-sdk/common"
	"github.com/oniio/dsp-go-sdk/config"
	netcom "github.com/oniio/dsp-go-sdk/network/common"
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

var channel1Addr = "127.0.0.1:3001"
var channel2Addr = "127.0.0.1:3002"
var channel3Addr = "127.0.0.1:3003"
var channel4Addr = "127.0.0.1:3004"

func init() {
	log.InitLog(1, log.PATH, log.Stdout)
}

func TestChainGetBlockHeight(t *testing.T) {
	dspCfg := &config.DspConfig{
		ChainRpcAddr: rpcAddr,
	}
	d := NewDsp(dspCfg, nil)
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
	d := NewDsp(dspCfg, nil)
	version := d.GetVersion()
	fmt.Printf("version: %s\n", version)
}

func TestNodeRegister(t *testing.T) {
	dspCfg := &config.DspConfig{
		ChainRpcAddr: rpcAddr,
	}
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
	d := NewDsp(dspCfg, acc)
	// register 512G for 12 hours
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
	d := NewDsp(dspCfg, acc)
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
	d := NewDsp(dspCfg, nil)
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
	d := NewDsp(dspCfg, acc)
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
	d := NewDsp(dspCfg, acc)
	tx, err := d.NodeWithdrawProfit()
	if err != nil {
		log.Errorf("register node err:%s", err)
		return
	}
	log.Infof("tx: %s", tx)
}
func TestStartDspNode(t *testing.T) {
	log.InitLog(1, log.PATH, log.Stdout)
	fileRoot, err := filepath.Abs("./testdata")
	if err != nil {
		t.Fatal(err)
	}
	dspCfg := &config.DspConfig{
		DBPath:               fileRoot + "/db1",
		FsRepoRoot:           fileRoot + "/onifs1",
		FsFileRoot:           fileRoot,
		FsGcPeriod:           "1h",
		FsType:               config.FS_BLOCKSTORE,
		ChainRpcAddr:         rpcAddr,
		ChannelClientType:    "rpc",
		ChannelListenAddr:    channel1Addr,
		ChannelProtocol:      "tcp",
		ChannelRevealTimeout: "1000",

		TrackerUrls:  []string{"udp://127.0.0.1:6369/announce"},
		SeedInterval: 3600, //  1h
	}

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
	d := NewDsp(dspCfg, acc)
	if d == nil {
		t.Fatal("dsp init failed")
	}
	err = d.Start(node1ListAddr)
	if err != nil {
		t.Fatal(err)
	}
	// set price for all file
	d.Channel.SetUnitPrices(netcom.ASSET_ONG, 1)
	go d.StartShareServices()
	tick := time.NewTicker(time.Second)
	for {
		<-tick.C
	}
}

func TestStartDspNode3(t *testing.T) {
	fileRoot, err := filepath.Abs("./testdata")
	if err != nil {
		t.Fatal(err)
		return
	}
	dspCfg := &config.DspConfig{
		DBPath:       "testdata/db3",
		FsRepoRoot:   "testdata/onifs3",
		FsFileRoot:   fileRoot,
		FsType:       config.FS_FILESTORE,
		ChainRpcAddr: rpcAddr,
	}
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
	d := NewDsp(dspCfg, acc)
	d.Start(node3ListAddr)
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
	fileHashStr := "QmUQTgbTc1y4a8cq1DyA548B71kSrnVm7vHuBsatmnMBib"
	err = d.DownloadFile(fileHashStr, netcom.ASSET_ONG, true, "", true, 100)
	if err != nil {
		log.Errorf("download err %s\n", err)
	}
	// use for testing go routines for tasks are released or not
	time.Sleep(time.Duration(5) * time.Second)
	go d.StartShareServices()
	tick := time.NewTicker(time.Second)
	// err = d.DeleteDownloadedFile("QmUQTgbTc1y4a8cq1DyA548B71kSrnVm7vHuBsatmnMBib")
	log.Debugf("delete file result:%s", err)
	for {
		<-tick.C
	}
}

func TestStartDspNode4(t *testing.T) {
	log.InitLog(1, log.PATH, log.Stdout)
	fileRoot, err := filepath.Abs("./testdata2")
	if err != nil {
		t.Fatal(err)
		return
	}
	dspCfg := &config.DspConfig{
		DBPath:       fileRoot + "/db4",
		FsRepoRoot:   fileRoot + "/onifs4",
		FsFileRoot:   fileRoot,
		FsType:       config.FS_FILESTORE,
		ChainRpcAddr: rpcAddr,
	}
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
	d := NewDsp(dspCfg, acc)
	d.Start(node4ListAddr)
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
	fileHashStr := "QmUQTgbTc1y4a8cq1DyA548B71kSrnVm7vHuBsatmnMBib"
	err = d.DownloadFile(fileHashStr, netcom.ASSET_ONG, true, "", true, 100)
	if err != nil {
		log.Errorf("download err %s\n", err)
	}
	// use for testing go routines for tasks are released or not
	time.Sleep(time.Duration(5) * time.Second)
	go d.StartShareServices()
	tick := time.NewTicker(time.Second)
	for {
		<-tick.C
	}
}

func TestUploadFile(t *testing.T) {
	dspCfg := &config.DspConfig{
		DBPath:       "./testdata/db2",
		FsRepoRoot:   "./testdata/onifs2",
		FsFileRoot:   "./testdata",
		FsType:       config.FS_FILESTORE,
		ChainRpcAddr: rpcAddr,
	}

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
	d := NewDsp(dspCfg, acc)
	d.Start(node2ListAddr)
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

func TestDeleteFileFromUploader(t *testing.T) {
	fileRoot, err := filepath.Abs("./testdata")
	if err != nil {
		t.Fatal(err)
		return
	}
	dspCfg := &config.DspConfig{
		DBPath:       "testdata/db2",
		FsRepoRoot:   "testdata/onifs2",
		FsFileRoot:   fileRoot,
		FsType:       config.FS_FILESTORE,
		ChainRpcAddr: rpcAddr,
	}

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
	d := NewDsp(dspCfg, acc)
	d.Start(node2ListAddr)
	ret, err := d.DeleteUploadedFile("QmUQTgbTc1y4a8cq1DyA548B71kSrnVm7vHuBsatmnMBib")
	if err != nil {
		log.Errorf("delete file failed, err:%s", err)
		return
	}
	log.Infof("delete file success, ret:%v", ret)
	// wait for msg sent
	time.Sleep(time.Duration(5) * time.Second)
}
func TestDeleteFileLocally(t *testing.T) {
	fileRoot, err := filepath.Abs("./testdata")
	if err != nil {
		t.Fatal(err)
		return
	}
	dspCfg := &config.DspConfig{
		DBPath:       "testdata/db3",
		FsRepoRoot:   "testdata/onifs3",
		FsFileRoot:   fileRoot,
		FsType:       config.FS_FILESTORE,
		ChainRpcAddr: rpcAddr,
	}
	d := NewDsp(dspCfg, nil)
	d.Start(node3ListAddr)
	err = d.DeleteDownloadedFile("QmUQTgbTc1y4a8cq1DyA548B71kSrnVm7vHuBsatmnMBib")
	if err != nil {
		log.Errorf("delete file failed, err:%s", err)
		return
	}
	log.Infof("delete file success")
}

func TestGetFileProveNode(t *testing.T) {
	dspCfg := &config.DspConfig{
		ChainRpcAddr: rpcAddr,
	}
	d := NewDsp(dspCfg, nil)
	n1, n2 := d.getFileProveNode("QmUQTgbTc1y4a8cq1DyA548B71kSrnVm7vHuBsatmnMBib", 3)
	fmt.Printf("n1:%v, n2:%v\n", n1, n2)
}

func TestDownloadFile(t *testing.T) {
	fileRoot, err := filepath.Abs("./testdata")
	if err != nil {
		t.Fatal(err)
	}
	dspCfg := &config.DspConfig{
		DBPath:               fileRoot + "/db3",
		FsRepoRoot:           fileRoot + "/onifs3",
		FsFileRoot:           fileRoot,
		FsType:               config.FS_FILESTORE,
		ChainRpcAddr:         rpcAddr,
		ChannelClientType:    "rpc",
		ChannelListenAddr:    channel3Addr,
		ChannelProtocol:      "tcp",
		ChannelRevealTimeout: "1000",

		TrackerUrls:  []string{"udp://127.0.0.1:6369/announce"},
		SeedInterval: 3600, //  1h
	}

	w, err := wallet.OpenWallet(wallet3File)
	if err != nil {
		t.Fatal(err)
	}
	acc, err := w.GetDefaultAccount([]byte(walletPwd))
	if err != nil {
		t.Fatal(err)
	}
	d := NewDsp(dspCfg, acc)
	err = d.Start(node3ListAddr)
	if err != nil {
		t.Fatal(err)
	}
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
	fileHashStr := "QmUQTgbTc1y4a8cq1DyA548B71kSrnVm7vHuBsatmnMBib"
	err = d.DownloadFile(fileHashStr, netcom.ASSET_ONG, true, "", false, 100)
	if err != nil {
		log.Errorf("download err %s\n", err)
	}
	// use for testing go routines for tasks are released or not
	time.Sleep(time.Duration(5) * time.Second)
}

func TestDownloadFileWithQuotation(t *testing.T) {
	fileRoot, err := filepath.Abs("./testdata")
	if err != nil {
		t.Fatal(err)
	}
	dspCfg := &config.DspConfig{
		DBPath:               fileRoot + "/db3",
		FsRepoRoot:           fileRoot + "/onifs3",
		FsFileRoot:           fileRoot,
		FsType:               config.FS_FILESTORE,
		ChainRpcAddr:         rpcAddr,
		ChannelClientType:    "rpc",
		ChannelListenAddr:    channel3Addr,
		ChannelProtocol:      "tcp",
		ChannelRevealTimeout: "1000",
	}

	w, err := wallet.OpenWallet(wallet3File)
	if err != nil {
		t.Fatal(err)
	}
	acc, err := w.GetDefaultAccount([]byte(walletPwd))
	if err != nil {
		t.Fatal(err)
	}
	d := NewDsp(dspCfg, acc)
	fmt.Printf("TestDownloadFileWithQuotation d:%v\n", d)
	err = d.Start(node3ListAddr)
	if err != nil {
		t.Fatal(err)
	}
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

	fileHashStr := "QmUQTgbTc1y4a8cq1DyA548B71kSrnVm7vHuBsatmnMBib"
	// set use free peers
	useFree := false
	quotation, err := d.GetDownloadQuotation(fileHashStr, netcom.ASSET_ONG, useFree)
	if len(quotation) == 0 {
		log.Errorf("no peer to download")
		return
	}
	if err != nil {
		t.Fatal(err)
	}
	if !useFree {
		// filter peers
		quotation = utils.SortPeersByPrice(quotation, 100)
	}
	err = d.SetupChannel(fileHashStr, quotation)
	if err != nil {
		t.Fatal(err)
	}
	err = d.DownloadFileWithQuotation(fileHashStr, netcom.ASSET_ONG, true, quotation, "")
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
	d := NewDsp(dspCfg, acc)
	d.Fs.StartPDPVerify("QmUQTgbTc1y4a8cq1DyA548B71kSrnVm7vHuBsatmnMBib", 0, 0, 0)
	tick := time.NewTicker(time.Second)
	for {
		<-tick.C
	}
}
func TestOpenChannel(t *testing.T) {
	w, err := wallet.OpenWallet(walletFile)
	if err != nil {
		t.Fatal(err)
	}
	acc, err := w.GetDefaultAccount([]byte(walletPwd))
	if err != nil {
		t.Fatal(err)
	}
	dspCfg := &config.DspConfig{
		ChainRpcAddr:         rpcAddr,
		ChannelClientType:    "rpc",
		ChannelListenAddr:    channel1Addr,
		ChannelProtocol:      "tcp",
		ChannelRevealTimeout: "1000",
	}
	d := NewDsp(dspCfg, acc)
	id, err := d.Channel.OpenChannel("AWaE84wqVf1yffjaR6VJ4NptLdqBAm8G9c")
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("id = %d\n", id)
}

func TestDepositChannel(t *testing.T) {
	w, err := wallet.OpenWallet(walletFile)
	if err != nil {
		t.Fatal(err)
	}
	acc, err := w.GetDefaultAccount([]byte(walletPwd))
	if err != nil {
		t.Fatal(err)
	}
	dspCfg := &config.DspConfig{
		ChainRpcAddr:         rpcAddr,
		ChannelClientType:    "rpc",
		ChannelListenAddr:    channel1Addr,
		ChannelProtocol:      "tcp",
		ChannelRevealTimeout: "1000",
	}
	d := NewDsp(dspCfg, acc)
	err = d.Channel.SetDeposit("AWaE84wqVf1yffjaR6VJ4NptLdqBAm8G9c", 100)
	if err != nil {
		t.Fatal(err)
	}
}

func TestPushTracker(t *testing.T) {
	fileRoot, err := filepath.Abs("./testdata")
	if err != nil {
		t.Fatal(err)
	}
	dspCfg := &config.DspConfig{
		DBPath:               fileRoot + "/db1",
		FsRepoRoot:           fileRoot + "/onifs1",
		FsFileRoot:           fileRoot,
		FsGcPeriod:           "1h",
		FsType:               config.FS_BLOCKSTORE,
		ChainRpcAddr:         rpcAddr,
		ChannelClientType:    "rpc",
		ChannelListenAddr:    channel1Addr,
		ChannelProtocol:      "tcp",
		ChannelRevealTimeout: "1000",
	}

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
	d := NewDsp(dspCfg, acc)
	if d == nil {
		t.Fatal("dsp init failed")
	}
	err = d.PushToTrackers("QmUQTgbTc1y4a8cq1DyA548B71kSrnVm7vHuBsatmnMBib", []string{"udp://127.0.0.1:6369/announce"}, "tcp://127.0.0.1:6370")
	if err != nil {
		t.Fatal(err)
	}
}

func TestGetPeersFromTracker(t *testing.T) {
	d := NewDsp(nil, nil)
	if d == nil {
		t.Fatal("dsp init failed")
	}
	d.GetPeerFromTracker("QmUQTgbTc1y4a8cq1DyA548B71kSrnVm7vHuBsatmnMBib", []string{"udp://127.0.0.1:6369/announce"})
}

func TestSeedServices(t *testing.T) {
	fileRoot, err := filepath.Abs("./testdata")
	if err != nil {
		t.Fatal(err)
	}
	dspCfg := &config.DspConfig{
		DBPath:               fileRoot + "/db1",
		FsRepoRoot:           fileRoot + "/onifs1",
		FsFileRoot:           fileRoot,
		FsGcPeriod:           "1h",
		FsType:               config.FS_BLOCKSTORE,
		ChainRpcAddr:         rpcAddr,
		ChannelClientType:    "rpc",
		ChannelListenAddr:    channel1Addr,
		ChannelProtocol:      "tcp",
		ChannelRevealTimeout: "1000",

		TrackerUrls:  []string{"udp://127.0.0.1:6369/announce"},
		SeedInterval: 10,
	}

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
	d := NewDsp(dspCfg, acc)
	if d == nil {
		t.Fatal("dsp init failed")
	}
	d.Start(node1ListAddr)
	tick := time.NewTicker(time.Second)
	for {
		<-tick.C
	}
}
