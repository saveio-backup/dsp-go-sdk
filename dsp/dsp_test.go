package dsp

// import (
// 	"crypto/rand"
// 	"flag"
// 	"fmt"
// 	"os"
// 	"path/filepath"
// 	"strconv"
// 	"testing"
// 	"time"

// 	themisSDK "github.com/saveio/themis-go-sdk"

// 	"github.com/saveio/themis-go-sdk/usdt"
// 	"github.com/saveio/themis-go-sdk/wallet"

// 	"github.com/saveio/dsp-go-sdk/common"
// 	"github.com/saveio/dsp-go-sdk/config"
// 	"github.com/saveio/themis/account"
// 	chainCom "github.com/saveio/themis/common"
// 	"github.com/saveio/themis/common/log"
// 	"github.com/saveio/themis/core/types"
// 	"github.com/saveio/themis/crypto/keypair"
// 	"github.com/saveio/themis/crypto/pdp"
// 	fs "github.com/saveio/themis/smartcontract/service/native/savefs"
// )

// var rpcAddr = "http://127.0.0.1:20336"
// var node1ListAddr = "tcp://127.0.0.1:14001"
// var node2ListAddr = "tcp://127.0.0.1:14002"
// var node3ListAddr = "tcp://127.0.0.1:14003"
// var node4ListAddr = "tcp://127.0.0.1:14004"
// var node5ListAddr = "tcp://127.0.0.1:14005"
// var uploadTestFile = "./testdata/testuploadbigfile.txt"

// var walletFile = "./wallet.dat"
// var wallet2File = "./testdata/wallet2.dat"
// var wallet3File = "./testdata/wallet3.dat"
// var wallet4File = "./testdata/wallet4.dat"
// var wallet5File = "./testdata/wallet5.dat"

// var wallet1Addr = "AYMnqA65pJFKAbbpD8hi5gdNDBmeFBy5hS"
// var wallet2Addr = "AWaE84wqVf1yffjaR6VJ4NptLdqBAm8G9c"
// var wallet3Addr = "AGeTrARjozPVLhuzMxZq36THMtvsrZNAHq"
// var wallet4Addr = "ANa3f9jm2FkWu4NrVn6L1FGu7zadKdvPjL"
// var wallet5Addr = "ANy4eS6oQaX15xpGV7dvsinh2aiqPm9HDf"
// var walletPwd = "pwd"

// var channel1Addr = "127.0.0.1:13001"
// var channel2Addr = "127.0.0.1:13002"
// var channel3Addr = "127.0.0.1:13003"
// var channel4Addr = "127.0.0.1:13004"
// var channel5Addr = "127.0.0.1:13005"

// func init() {
// 	log.InitLog(2, log.PATH, log.Stdout)
// 	log.AddIgnore("oniChannel")
// }

// func newLocalDsp(r, w, wp string) *Dsp {
// 	dspCfg := &config.DspConfig{
// 		ChainRpcAddr: r,
// 	}
// 	var acc *account.Account
// 	if len(w) > 0 {
// 		wal, err := wallet.OpenWallet(w)
// 		if err != nil {
// 			log.Errorf("open wallet err:%s\n", err)
// 			return nil
// 		}
// 		acc, err = wal.GetDefaultAccount([]byte(wp))
// 		if err != nil {
// 			log.Errorf("get default acc err:%s\n", err)
// 			return nil
// 		}
// 	}
// 	d := NewDsp(dspCfg, acc, nil)
// 	return d
// }

// func TestChainGetBlockHeight(t *testing.T) {
// 	dspCfg := &config.DspConfig{
// 		ChainRpcAddr: rpcAddr,
// 	}
// 	d := NewDsp(dspCfg, nil, nil)
// 	height, err := d.chain.SDK().GetCurrentBlockHeight()
// 	if err != nil {
// 		fmt.Printf("get block height err: %s", err)
// 		return
// 	}
// 	fmt.Printf("current block height: %d\n", height)
// }
// func TestGetVersion(t *testing.T) {
// 	dspCfg := &config.DspConfig{
// 		ChainRpcAddr: rpcAddr,
// 	}
// 	d := NewDsp(dspCfg, nil, nil)
// 	version := d.GetVersion()
// 	fmt.Printf("version: %s\n", version)
// }

// func TestNodeRegister(t *testing.T) {
// 	dspCfg := &config.DspConfig{
// 		ChainRpcAddr: rpcAddr,
// 	}
// 	w, err := wallet.OpenWallet(walletFile)
// 	// w, err := wallet.OpenWallet(wallet4File)
// 	// w, err := wallet.OpenWallet(wallet5File)
// 	if err != nil {
// 		log.Errorf("open wallet err:%s\n", err)
// 		return
// 	}
// 	acc, err := w.GetDefaultAccount([]byte(walletPwd))
// 	if err != nil {
// 		log.Errorf("get default acc err:%s\n", err)
// 		return
// 	}
// 	d := NewDsp(dspCfg, acc, nil)
// 	// register 512G for 12 hours
// 	tx, err := d.RegisterNode(node1ListAddr, 512*1024*1024, 12)
// 	// tx, err := d.RegisterNode(node4ListAddr, 512*1024*1024, 12)
// 	// tx, err := d.RegisterNode(node5ListAddr, 512*1024*1024, 12)
// 	if err != nil {
// 		log.Errorf("register node err:%s", err)
// 		return
// 	}
// 	log.Infof("tx: %s", tx)
// }

// func TestNodeUnregister(t *testing.T) {
// 	dspCfg := &config.DspConfig{
// 		ChainRpcAddr: rpcAddr,
// 	}
// 	w, err := wallet.OpenWallet(walletFile)
// 	if err != nil {
// 		log.Errorf("open wallet err:%s\n", err)
// 		return
// 	}
// 	acc, err := w.GetDefaultAccount([]byte(walletPwd))
// 	if err != nil {
// 		log.Errorf("get default acc err:%s\n", err)
// 		return
// 	}
// 	d := NewDsp(dspCfg, acc, nil)
// 	tx, err := d.UnregisterNode()
// 	if err != nil {
// 		log.Errorf("register node err:%s", err)
// 		return
// 	}
// 	log.Infof("tx: %s", tx)
// }

// func TestNodeQuery(t *testing.T) {
// 	dspCfg := &config.DspConfig{
// 		ChainRpcAddr: rpcAddr,
// 	}
// 	d := NewDsp(dspCfg, nil, nil)
// 	info, err := d.QueryNode(wallet1Addr)
// 	if err != nil {
// 		log.Errorf("query node err %s", err)
// 		return
// 	}
// 	log.Infof("node info pledge %d", info.Pledge)
// 	log.Infof("node info profit %d", info.Profit)
// 	log.Infof("node info volume %d", info.Volume)
// 	log.Infof("node info restvol %d", info.RestVol)
// 	log.Infof("node info service time %d", info.ServiceTime)
// 	log.Infof("node info wallet address %s", info.WalletAddr.ToBase58())
// 	log.Infof("node info node address %s", info.NodeAddr)
// }

// func TestNodeUpdate(t *testing.T) {
// 	dspCfg := &config.DspConfig{
// 		ChainRpcAddr: rpcAddr,
// 	}
// 	w, err := wallet.OpenWallet(walletFile)
// 	if err != nil {
// 		log.Errorf("open wallet err:%s\n", err)
// 		return
// 	}
// 	acc, err := w.GetDefaultAccount([]byte(walletPwd))
// 	if err != nil {
// 		log.Errorf("get default acc err:%s\n", err)
// 		return
// 	}
// 	d := NewDsp(dspCfg, acc, nil)
// 	tx, err := d.UpdateNode(node1ListAddr, 0, 12)
// 	if err != nil {
// 		log.Errorf("update node err:%s", err)
// 		return
// 	}
// 	log.Infof("tx: %s", tx)
// }

// func TestNodeWithdrawProfit(t *testing.T) {
// 	dspCfg := &config.DspConfig{
// 		ChainRpcAddr: rpcAddr,
// 	}
// 	w, err := wallet.OpenWallet(walletFile)
// 	if err != nil {
// 		log.Errorf("open wallet err:%s\n", err)
// 		return
// 	}
// 	acc, err := w.GetDefaultAccount([]byte(walletPwd))
// 	if err != nil {
// 		log.Errorf("get default acc err:%s\n", err)
// 		return
// 	}
// 	d := NewDsp(dspCfg, acc, nil)
// 	tx, err := d.NodeWithdrawProfit()
// 	if err != nil {
// 		log.Errorf("register node err:%s", err)
// 		return
// 	}
// 	log.Infof("tx: %s", tx)
// }

// // TestStartDspBlockStoreNode test start block store node.
// // cmd: $ go test -run TestStartDspBlockStoreNode -args "1"
// func TestStartDspBlockStoreNode(t *testing.T) {
// 	fileRoot, err := filepath.Abs("./testdata")
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// 	nodeIndex := 1
// 	if len(flag.Args()) > 0 {
// 		index, err := strconv.Atoi(flag.Args()[0])
// 		if err != nil {
// 			t.Fatal(err)
// 		}
// 		nodeIndex = index
// 	}
// 	fmt.Printf("start node %d\n", nodeIndex)
// 	chListenAddrs := []string{"", channel1Addr, channel2Addr, channel3Addr, channel4Addr, channel5Addr}
// 	walletFiles := []string{"", walletFile, wallet2File, wallet3File, wallet4File, wallet5File}
// 	dspCfg := &config.DspConfig{
// 		DBPath:               fmt.Sprintf("%s/db%d", fileRoot, nodeIndex),
// 		FsRepoRoot:           fmt.Sprintf("%s/max%d", fileRoot, nodeIndex),
// 		FsFileRoot:           fileRoot,
// 		FsGcPeriod:           "1h",
// 		FsMaxStorage:         "10G",
// 		FsType:               config.FS_BLOCKSTORE,
// 		ChainRpcAddr:         rpcAddr,
// 		ChannelClientType:    "rpc",
// 		ChannelListenAddr:    chListenAddrs[nodeIndex],
// 		ChannelProtocol:      "tcp",
// 		ChannelRevealTimeout: "1000",
// 		DnsNodeMaxNum:        100,
// 		SeedInterval:         3600, //  1h
// 	}
// 	w, err := wallet.OpenWallet(walletFiles[nodeIndex])
// 	if err != nil {
// 		log.Errorf("open wallet err:%s\n", err)
// 		return
// 	}
// 	acc, err := w.GetDefaultAccount([]byte(walletPwd))
// 	if err != nil {
// 		log.Errorf("get default acc err:%s\n", err)
// 		return
// 	}
// 	log.Infof("wallet address:%s", acc.Address.ToBase58())
// 	d := NewDsp(dspCfg, acc, nil)
// 	if d == nil {
// 		t.Fatal("dsp init failed")
// 	}
// 	err = d.Start()
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// 	// set price for all file
// 	d.channel.SetUnitPrices(common.ASSET_USDT, common.FILE_DOWNLOAD_UNIT_PRICE)

// 	go d.StartShareServices()
// 	tick := time.NewTicker(time.Second)
// 	for {
// 		<-tick.C
// 	}
// }

// func TestUploadFile(t *testing.T) {
// 	dspCfg := &config.DspConfig{
// 		DBPath:        "./testdata/db2",
// 		FsRepoRoot:    "./testdata/max2",
// 		FsFileRoot:    "./testdata",
// 		FsType:        config.FS_FILESTORE,
// 		ChainRpcAddr:  rpcAddr,
// 		DnsNodeMaxNum: 100,
// 	}
// 	w, err := wallet.OpenWallet(wallet2File)
// 	if err != nil {
// 		log.Errorf("open wallet err:%s\n", err)
// 		return
// 	}
// 	acc, err := w.GetDefaultAccount([]byte(walletPwd))
// 	if err != nil {
// 		log.Errorf("get default acc err:%s\n", err)
// 		return
// 	}
// 	d := NewDsp(dspCfg, acc, nil)
// 	d.Start()
// 	log.Infof("wallet address:%s", acc.Address.ToBase58())
// 	opt := &fs.UploadOption{
// 		FileDesc:      []byte("file"),
// 		ProveInterval: 100,
// 		Privilege:     1,
// 		CopyNum:       0,
// 		Encrypt:       false,
// 		DnsURL:        []byte(fmt.Sprintf("dsp://file%d", time.Now().Unix())),
// 	}

// 	ret, err := d.UploadFile(true, "", uploadTestFile, opt)
// 	log.Debugf("upload file ret %v", ret)
// 	if err != nil {
// 		log.Errorf("upload file failed, err:%s", err)
// 		return
// 	}
// 	log.Infof("upload file success, ret:%v", ret)
// }

// func TestDeleteFileFromUploader(t *testing.T) {
// 	fileRoot, err := filepath.Abs("./testdata")
// 	if err != nil {
// 		t.Fatal(err)
// 		return
// 	}
// 	dspCfg := &config.DspConfig{
// 		DBPath:       "testdata/db2",
// 		FsRepoRoot:   "testdata/max2",
// 		FsFileRoot:   fileRoot,
// 		FsType:       config.FS_FILESTORE,
// 		ChainRpcAddr: rpcAddr,
// 	}

// 	w, err := wallet.OpenWallet(wallet2File)
// 	if err != nil {
// 		log.Errorf("open wallet err:%s\n", err)
// 		return
// 	}
// 	acc, err := w.GetDefaultAccount([]byte(walletPwd))
// 	if err != nil {
// 		log.Errorf("get default acc err:%s\n", err)
// 		return
// 	}
// 	d := NewDsp(dspCfg, acc, nil)
// 	d.Start()
// 	ret, err := d.DeleteUploadedFileByIds([]string{""}, 41005)
// 	if err != nil {
// 		log.Errorf("delete file failed, err:%s", err)
// 		return
// 	}
// 	log.Infof("delete file success, ret:%v", ret)
// 	// wait for msg sent
// 	time.Sleep(time.Duration(5) * time.Second)
// }
// func TestDeleteFileLocally(t *testing.T) {
// 	fileRoot, err := filepath.Abs("./testdata")
// 	if err != nil {
// 		t.Fatal(err)
// 		return
// 	}
// 	dspCfg := &config.DspConfig{
// 		DBPath:       "testdata/db3",
// 		FsRepoRoot:   "testdata/max3",
// 		FsFileRoot:   fileRoot,
// 		FsType:       config.FS_FILESTORE,
// 		ChainRpcAddr: rpcAddr,
// 	}
// 	d := NewDsp(dspCfg, nil, nil)
// 	d.Start()
// 	err = d.DeleteDownloadedFile("QmUQTgbTc1y4a8cq1DyA548B71kSrnVm7vHuBsatmnMBib")
// 	if err != nil {
// 		log.Errorf("delete file failed, err:%s", err)
// 		return
// 	}
// 	log.Infof("delete file success")
// }

// func TestGetFileProveNode(t *testing.T) {
// 	dspCfg := &config.DspConfig{
// 		ChainRpcAddr: rpcAddr,
// 	}
// 	d := NewDsp(dspCfg, nil, nil)
// 	n1 := d.getFileProvedNode("zb2rhkaiU6xcVbt1TtJeLDMJGPb94WxxQho1bBLvMH57Rww8b")
// 	fmt.Printf("n1:%v\n", n1)
// }

// func TestGetExpiredTaskList(t *testing.T) {
// 	dspCfg := &config.DspConfig{
// 		ChainRpcAddr: rpcAddr,
// 	}
// 	d := NewDsp(dspCfg, nil, nil)
// 	list, err := d.chain.SDK().Native.Fs.GetExpiredProveList()
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// 	fmt.Printf("tasks %v\n", list)
// }

// func TestDownloadFile(t *testing.T) {
// 	nodeIdx := 3
// 	fileRoot, err := filepath.Abs("./testdata")
// 	if err != nil {
// 		t.Fatal(err)
// 	}

// 	chListenAddrs := []string{"", channel1Addr, channel2Addr, channel3Addr, channel4Addr, channel5Addr}
// 	walletFiles := []string{"", walletFile, wallet2File, wallet3File, wallet4File, wallet5File}

// 	dspCfg := &config.DspConfig{
// 		DBPath:               fmt.Sprintf("%s/db%d", fileRoot, nodeIdx),
// 		FsRepoRoot:           fmt.Sprintf("%s/max%d", fileRoot, nodeIdx),
// 		FsFileRoot:           fileRoot,
// 		FsType:               config.FS_FILESTORE,
// 		ChainRpcAddr:         rpcAddr,
// 		ChannelClientType:    "rpc",
// 		ChannelListenAddr:    chListenAddrs[nodeIdx],
// 		ChannelProtocol:      "tcp",
// 		ChannelRevealTimeout: "1000",

// 		DnsNodeMaxNum: 100,
// 		SeedInterval:  3600, //  1h
// 	}

// 	w, err := wallet.OpenWallet(walletFiles[nodeIdx])
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// 	acc, err := w.GetDefaultAccount([]byte(walletPwd))
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// 	d := NewDsp(dspCfg, acc, nil)
// 	err = d.Start()
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// 	log.Infof("wallet address:%s", acc.Address.ToBase58())
// 	d.taskMgr.RegProgressCh()
// 	fileHashStr := "QmUQTgbTc1y4a8cq1DyA548B71kSrnVm7vHuBsatmnMBib"
// 	err = d.DownloadFile(true, "", fileHashStr, nil)
// 	if err != nil {
// 		log.Errorf("download err %s\n", err)
// 	}
// 	// use for testing go routines for tasks are released or not
// 	time.Sleep(time.Duration(5) * time.Second)
// 	// set price for all file
// 	d.channel.SetUnitPrices(common.ASSET_USDT, common.FILE_DOWNLOAD_UNIT_PRICE)
// 	go d.StartShareServices()
// 	tick := time.NewTicker(time.Second)
// 	for {
// 		<-tick.C
// 	}

// 	// link := "oni://QmUQTgbTc1y4a8cq1DyA548B71kSrnVm7vHuBsatmnMBib&name=123"
// 	// err = d.DownloadFileByLink(link, common.ASSET_USDT, true, "", false, 100)
// 	// if err != nil {
// 	// 	log.Errorf("download err %s\n", err)
// 	// }
// 	// // use for testing go routines for tasks are released or not
// 	// time.Sleep(time.Duration(5) * time.Second)

// 	// url := "dsp://ok.com"
// 	// err = d.DownloadFileByUrl(url, common.ASSET_USDT, true, "", false, 100)
// 	// if err != nil {
// 	// 	log.Errorf("download err %s\n", err)
// 	// }
// 	// // use for testing go routines for tasks are released or not
// 	// time.Sleep(time.Duration(5) * time.Second)
// }

// func TestStartPDPVerify(t *testing.T) {
// 	dspCfg := &config.DspConfig{
// 		DBPath:       "./testdata/db1",
// 		FsRepoRoot:   "./testdata/max1",
// 		FsFileRoot:   "./testdata",
// 		FsType:       config.FS_BLOCKSTORE,
// 		ChainRpcAddr: rpcAddr,
// 	}
// 	w, err := wallet.OpenWallet(walletFile)
// 	if err != nil {
// 		log.Errorf("open wallet err:%s\n", err)
// 		return
// 	}
// 	acc, err := w.GetDefaultAccount([]byte(walletPwd))
// 	if err != nil {
// 		log.Errorf("get default acc err:%s\n", err)
// 		return
// 	}
// 	log.Infof("wallet address:%s", acc.Address.ToBase58())
// 	d := NewDsp(dspCfg, acc, nil)
// 	d.fs.StartPDPVerify("QmUQTgbTc1y4a8cq1DyA548B71kSrnVm7vHuBsatmnMBib", 0, 0, 0, chainCom.ADDRESS_EMPTY)
// 	tick := time.NewTicker(time.Second)
// 	for {
// 		<-tick.C
// 	}
// }
// func TestOpenChannel(t *testing.T) {
// 	w, err := wallet.OpenWallet(walletFile)
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// 	acc, err := w.GetDefaultAccount([]byte(walletPwd))
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// 	dspCfg := &config.DspConfig{
// 		ChainRpcAddr:         rpcAddr,
// 		ChannelClientType:    "rpc",
// 		ChannelListenAddr:    channel1Addr,
// 		ChannelProtocol:      "tcp",
// 		ChannelRevealTimeout: "1000",
// 	}
// 	d := NewDsp(dspCfg, acc, nil)
// 	id, err := d.channel.OpenChannel("AWaE84wqVf1yffjaR6VJ4NptLdqBAm8G9c", 0)
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// 	fmt.Printf("id = %d\n", id)
// }

// func TestDepositChannel(t *testing.T) {
// 	w, err := wallet.OpenWallet(wallet2File)
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// 	acc, err := w.GetDefaultAccount([]byte(walletPwd))
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// 	dspCfg := &config.DspConfig{
// 		ChainRpcAddr:         rpcAddr,
// 		ChannelClientType:    "rpc",
// 		ChannelListenAddr:    channel2Addr,
// 		ChannelProtocol:      "tcp",
// 		ChannelRevealTimeout: "1000",
// 	}
// 	d := NewDsp(dspCfg, acc, nil)
// 	d.Start()
// 	err = d.channel.SetDeposit("AYMnqA65pJFKAbbpD8hi5gdNDBmeFBy5hS", 662144)
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// }

// func TestPushTracker(t *testing.T) {
// 	fileRoot, err := filepath.Abs("./testdata")
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// 	dspCfg := &config.DspConfig{
// 		DBPath:               fileRoot + "/db1",
// 		FsRepoRoot:           fileRoot + "/max1",
// 		FsFileRoot:           fileRoot,
// 		FsGcPeriod:           "1h",
// 		FsMaxStorage:         "10G",
// 		FsType:               config.FS_BLOCKSTORE,
// 		ChainRpcAddr:         rpcAddr,
// 		ChannelClientType:    "rpc",
// 		ChannelListenAddr:    channel1Addr,
// 		ChannelProtocol:      "tcp",
// 		ChannelRevealTimeout: "1000",
// 	}

// 	w, err := wallet.OpenWallet(walletFile)
// 	if err != nil {
// 		log.Errorf("open wallet err:%s\n", err)
// 		return
// 	}
// 	acc, err := w.GetDefaultAccount([]byte(walletPwd))
// 	if err != nil {
// 		log.Errorf("get default acc err:%s\n", err)
// 		return
// 	}
// 	log.Infof("wallet address:%s", acc.Address.ToBase58())
// 	d := NewDsp(dspCfg, acc, nil)
// 	if d == nil {
// 		t.Fatal("dsp init failed")
// 	}
// 	err = d.PushToTrackers("QmUQTgbTc1y4a8cq1DyA548B71kSrnVm7vHuBsatmnMBib", []string{"udp://127.0.0.1:6369/announce"}, "tcp://127.0.0.1:6370")
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// }

// func TestGetPeersFromTracker(t *testing.T) {
// 	d := NewDsp(nil, nil, nil)
// 	if d == nil {
// 		t.Fatal("dsp init failed")
// 	}
// 	peers := d.GetPeerFromTracker("QmNZrZcmMC1tkF8jsp2Ze73HY1aLXcr3uHXUFJZNLmVECG", []string{"udp://127.0.0.1:6369/announce"})
// 	fmt.Printf("peers %v\n", peers)
// }

// func TestSeedServices(t *testing.T) {
// 	fileRoot, err := filepath.Abs("./testdata")
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// 	dspCfg := &config.DspConfig{
// 		DBPath: fileRoot + "/db1",

// 		FsRepoRoot:   fileRoot + "/max1",
// 		FsFileRoot:   fileRoot,
// 		FsGcPeriod:   "1h",
// 		FsMaxStorage: "10G",
// 		FsType:       config.FS_BLOCKSTORE,

// 		ChainRpcAddr:         rpcAddr,
// 		ChannelClientType:    "rpc",
// 		ChannelListenAddr:    channel1Addr,
// 		ChannelProtocol:      "tcp",
// 		ChannelRevealTimeout: "1000",

// 		DnsNodeMaxNum: 100,
// 		SeedInterval:  10,
// 	}

// 	w, err := wallet.OpenWallet(walletFile)
// 	if err != nil {
// 		log.Errorf("open wallet err:%s\n", err)
// 		return
// 	}
// 	acc, err := w.GetDefaultAccount([]byte(walletPwd))
// 	if err != nil {
// 		log.Errorf("get default acc err:%s\n", err)
// 		return
// 	}
// 	log.Infof("wallet address:%s", acc.Address.ToBase58())
// 	d := NewDsp(dspCfg, acc, nil)
// 	if d == nil {
// 		t.Fatal("dsp init failed")
// 	}
// 	d.Start()
// 	tick := time.NewTicker(time.Second)
// 	for {
// 		<-tick.C
// 	}
// }

// func TestInitDnsSC(t *testing.T) {
// 	dspCfg := &config.DspConfig{
// 		ChainRpcAddr: rpcAddr,
// 	}
// 	w, err := wallet.OpenWallet(walletFile)
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// 	acc, err := w.GetDefaultAccount([]byte(walletPwd))
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// 	log.Infof("wallet address:%s", acc.Address.ToBase58())
// 	d := NewDsp(dspCfg, acc, nil)
// 	if d == nil {
// 		t.Fatal("dsp init failed")
// 	}
// 	tx, err := d.chain.SDK().Native.Dns.RegisterHeader(common.FILE_URL_CUSTOM_HEADER, common.FILE_URL_CUSTOM_HEADER, 1)
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// 	fmt.Printf("hash :%v\n", tx)
// }

// func TestRegisterDnsHeader(t *testing.T) {
// 	dspCfg := &config.DspConfig{
// 		ChainRpcAddrs: []string{rpcAddr},
// 	}
// 	w, err := wallet.OpenWallet(walletFile)
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// 	acc, err := w.GetDefaultAccount([]byte(walletPwd))
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// 	log.Infof("wallet address:%s", acc.Address.ToBase58())
// 	d := NewDsp(dspCfg, acc, nil)
// 	if d == nil {
// 		t.Fatal("dsp init failed")
// 	}
// 	hash, err := d.chain.SDK().Native.Dns.RegisterHeader("oni", "oni", 100000)
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// 	fmt.Printf("hash: %v\n", hash)
// 	os.RemoveAll(filepath.Base(".") + "/Log")
// }

// func TestGetSmartEvent(t *testing.T) {
// 	dspCfg := &config.DspConfig{
// 		ChainRpcAddrs: []string{"http://139.219.136.38:20336"},
// 	}
// 	w, err := wallet.OpenWallet(walletFile)
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// 	acc, err := w.GetDefaultAccount([]byte(walletPwd))
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// 	log.Infof("wallet address:%s", acc.Address.ToBase58())
// 	d := NewDsp(dspCfg, acc, nil)
// 	if d == nil {
// 		t.Fatal("dsp init failed")
// 	}

// 	event, err := d.chain.SDK().GetSmartContractEventByBlock(1021445)
// 	if err != nil {
// 		t.Fatal(err)
// 	}

// 	for _, n := range event.Notify {
// 		contractAddr, err := chainCom.AddressFromHexString(n.ContractAddress)
// 		if err != nil {
// 			continue
// 		}
// 		switch contractAddr.ToBase58() {
// 		case usdt.USDT_CONTRACT_ADDRESS.ToBase58():
// 			states, ok := n.States.([]interface{})
// 			if !ok || states[0].(string) != "transfer" {
// 				continue
// 			}
// 			fmt.Printf("from %s to %s len: %d\n", states[1], states[2], len(states))
// 		}
// 		fmt.Printf("contract: %s, id: %d\n", n.ContractAddress, n.EventIdentifier)
// 	}

// }

// func TestRegisterDns(t *testing.T) {
// 	dspCfg := &config.DspConfig{
// 		ChainRpcAddrs: []string{rpcAddr},
// 	}
// 	w, err := wallet.OpenWallet(walletFile)
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// 	acc, err := w.GetDefaultAccount([]byte(walletPwd))
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// 	log.Infof("wallet address:%s", acc.Address.ToBase58())
// 	d := NewDsp(dspCfg, acc, nil)
// 	if d == nil {
// 		t.Fatal("dsp init failed")
// 	}
// 	hash, err := d.RegisterFileUrl("save://share/a93ed0c4", "save-link://QmT6hfgtvkyPpLr7aUNhy1MfPEDbbwzwyX2zmW5on4X3Mq&name=2019-08-22_12.58.49_LOG.log&owner=AY46Kes2ayy8c38hKBqictG9F9ar73mqhD&size=20514&blocknum=82&tr=dWRwOi8vMTY4LjYzLjI1My4yMzE6NjM2OS9hbm5vdW5jZQ==&tr=dWRwOi8vMTY4LjYzLjI1My4yMzE6NjM2OS9hbm5vdW5jZQ==")
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// 	fmt.Printf("hash: %v\n", hash)
// }

// func TestBindDns(t *testing.T) {
// 	dspCfg := &config.DspConfig{
// 		ChainRpcAddrs: []string{rpcAddr},
// 	}
// 	w, err := wallet.OpenWallet(walletFile)
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// 	acc, err := w.GetDefaultAccount([]byte(walletPwd))
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// 	log.Infof("wallet address:%s", acc.Address.ToBase58())
// 	d := NewDsp(dspCfg, acc, nil)
// 	if d == nil {
// 		t.Fatal("dsp init failed")
// 	}
// 	hash, err := d.BindFileUrl("save://share/a93ed0c4", "save-link://QmT6hfgtvkyPpLr7aUNhy1MfPEDbbwzwyX2zmW5on4X3Mq&name=2019-08-22_12.58.49_LOG.log&owner=AY46Kes2ayy8c38hKBqictG9F9ar73mqhD&size=20514&blocknum=82&tr=dWRwOi8vMTY4LjYzLjI1My4yMzE6NjM2OS9hbm5vdW5jZQ==&tr=dWRwOi8vMTY4LjYzLjI1My4yMzE6NjM2OS9hbm5vdW5jZQ==")
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// 	fmt.Printf("hash: %s\n", hash)
// }

// func TestGetHashFromUrl(t *testing.T) {
// 	dspCfg := &config.DspConfig{
// 		ChainRpcAddrs: []string{rpcAddr},
// 	}
// 	w, err := wallet.OpenWallet(walletFile)
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// 	acc, err := w.GetDefaultAccount([]byte(walletPwd))
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// 	log.Infof("wallet address:%s", acc.Address.ToBase58())
// 	d := NewDsp(dspCfg, acc, nil)
// 	if d == nil {
// 		t.Fatal("dsp init failed")
// 	}
// 	hash := d.GetFileHashFromUrl("oni://share/ecb0ef5d")
// 	fmt.Printf("hash: %s\n", hash)
// }
// func TestQueryDns(t *testing.T) {
// 	dspCfg := &config.DspConfig{
// 		ChainRpcAddrs: []string{rpcAddr},
// 	}
// 	w, err := wallet.OpenWallet(walletFile)
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// 	acc, err := w.GetDefaultAccount([]byte(walletPwd))
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// 	log.Infof("wallet address:%s", acc.Address.ToBase58())
// 	d := NewDsp(dspCfg, acc, nil)
// 	if d == nil {
// 		t.Fatal("dsp init failed")
// 	}
// 	link := d.GetLinkFromUrl("save://share/a93ed0c4")
// 	fmt.Printf("link: %s\n", link)
// }

// func TestGetSetupDNSNodes(t *testing.T) {
// 	dspCfg := &config.DspConfig{
// 		ChainRpcAddr:         rpcAddr,
// 		ChannelClientType:    "rpc",
// 		ChannelListenAddr:    "127.0.0.1:3006",
// 		ChannelProtocol:      "tcp",
// 		ChannelRevealTimeout: "1000",
// 	}
// 	w, err := wallet.OpenWallet(walletFile)
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// 	acc, err := w.GetDefaultAccount([]byte(walletPwd))
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// 	log.Infof("wallet address:%s", acc.Address.ToBase58())
// 	d := NewDsp(dspCfg, acc, nil)
// 	d.Start()
// 	if d.channel == nil {
// 		t.Fatal("channel is nil")
// 	}
// 	err = d.channel.StartService()
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// 	err = d.SetupDNSChannels()
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// 	if d.dns.DNSNode == nil {
// 		t.Fatal("dns node can't setup")
// 	}
// 	fmt.Printf("trackers %v, dns %s:%s\n", d.dns.TrackerUrls, d.dns.DNSNode.WalletAddr, d.dns.DNSNode.HostAddr)
// }

// func TestGetAllDNSNodes(t *testing.T) {
// 	dspCfg := &config.DspConfig{
// 		ChainRpcAddr: rpcAddr,
// 	}
// 	d := NewDsp(dspCfg, nil, nil)
// 	nodes, err := d.chain.SDK().Native.Dns.GetAllDnsNodes()
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// 	for k, v := range nodes {
// 		fmt.Printf("k=%v, wallet=%v\n", k, v.WalletAddr.ToBase58())
// 		fmt.Printf("k=%v, ip=%s\n", k, v.IP)
// 		port, _ := strconv.ParseUint(string(v.Port), 10, 64)
// 		fmt.Printf("k=%v, port=%d\n", k, port)
// 	}

// }

// func TestGetPublicIPFromDNS(t *testing.T) {
// 	d := &Dsp{}
// 	dspCfg := &config.DspConfig{
// 		ChannelProtocol: "udp",
// 	}
// 	d.config = dspCfg
// 	d.dns.TrackerUrls = make([]string, 0)
// 	d.dns.TrackerUrls = append(d.dns.TrackerUrls, "udp://40.73.96.40:6369")
// 	publicIP, err := d.GetExternalIP("AZj9LDEP1nhB1PYVtgAaabVKvN1uAKhmHn")
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// 	t.Logf("public ip %s", publicIP)
// }

// func TestCloseChannel(t *testing.T) {
// 	dspCfg := &config.DspConfig{
// 		ChainRpcAddr:         "http://10.0.1.201:10336",
// 		ChannelClientType:    "rpc",
// 		ChannelListenAddr:    "127.0.0.1:3006",
// 		ChannelProtocol:      "tcp",
// 		ChannelRevealTimeout: "1000",
// 	}
// 	w, err := wallet.OpenWallet("./dns_wallet.dat")
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// 	acc, err := w.GetDefaultAccount([]byte(walletPwd))
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// 	log.Infof("wallet address:%s", acc.Address.ToBase58())
// 	d := NewDsp(dspCfg, acc, nil)
// 	err = d.channel.ChannelClose("AdpPG7rjumCogd5cTvpfgZdS2c19cPK335")
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// }

// func TestPubKeyToAdr(t *testing.T) {
// 	w, err := wallet.OpenWallet("./wallet.dat")
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// 	acc, err := w.GetDefaultAccount([]byte(walletPwd))
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// 	pub := keypair.SerializePublicKey(acc.PublicKey)
// 	fmt.Printf("len: %d\n", len(pub))
// 	addr := types.AddressFromPubKey(acc.PublicKey)
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// 	if addr.ToBase58() != acc.Address.ToBase58() {
// 		t.Fatal("no match")
// 	}
// 	fmt.Printf("addr :%s %s\n", addr.ToBase58(), acc.Address.ToBase58())
// }

// func TestPDPVerify(t *testing.T) {
// 	fileRoot, err := filepath.Abs(".")
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// 	dspCfg := &config.DspConfig{
// 		DBPath:        fileRoot + "/db",
// 		FsRepoRoot:    fileRoot + "/max",
// 		FsFileRoot:    fileRoot + "/downloads",
// 		FsType:        config.FS_FILESTORE,
// 		ChainRpcAddrs: []string{rpcAddr},
// 	}
// 	tempFile := filepath.Join(fileRoot, "fs_temp_big_file")
// 	defer func() {
// 		// log.Debugf("remove...%s %s %s", dspCfg.DBPath, dspCfg.FsRepoRoot, dspCfg.FsFileRoot)
// 		os.RemoveAll(filepath.Join(fileRoot + "Log"))
// 		os.RemoveAll(dspCfg.DBPath)
// 		os.RemoveAll(dspCfg.FsRepoRoot)
// 		os.RemoveAll(dspCfg.FsFileRoot)
// 		os.Remove(tempFile)
// 	}()
// 	d := NewDsp(dspCfg, nil, nil)
// 	fi, err := os.OpenFile(tempFile, os.O_CREATE|os.O_RDWR, 0666)
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// 	defer fi.Close()
// 	randBuf := make([]byte, 1*1024*1024)
// 	rand.Read(randBuf)
// 	_, err = fi.Write(randBuf)
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// 	prefix := "123"
// 	blks, err := d.fs.NodesFromFile(tempFile, prefix, false, "")
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// 	fmt.Printf("hash: %d\n", len(blks))
// 	g, g0, pubKey, privKey, fileID := pdp.Init(tempFile)
// 	byteTags := make([]pdp.Element, len(blks))
// 	byteBlocks := make([]pdp.Block, len(blks))
// 	for i := 0; i < 2; i++ {
// 		for index, hash := range blks {
// 			block := d.fs.GetBlock(hash)
// 			blockData := d.fs.BlockDataOfAny(block)
// 			tag, err := pdp.SignGenerate(blockData, fileID, uint32(index+1), g0, privKey)
// 			if err != nil {
// 				t.Fatal(err)
// 			}
// 			byteTags[index] = pdp.Element{
// 				Buffer: tag,
// 			}
// 			byteBlocks[index] = pdp.Block{
// 				Buffer: blockData,
// 			}
// 			log.Debugf("index: %d, tag: %x", index, tag)
// 			if i == 1 && index == 2 {
// 				log.Debugf("break ")
// 				break
// 			}
// 		}
// 	}

// 	for index, tag := range byteTags {
// 		log.Debugf("index: %d, tag: %x", index, tag)
// 	}

// 	var challenges []pdp.Challenge
// 	challenges = append(challenges, pdp.Challenge{
// 		Index: 1,
// 		Rand:  1,
// 	})
// 	challenges = append(challenges, pdp.Challenge{
// 		Index: 2,
// 		Rand:  2,
// 	})
// 	challenges = append(challenges, pdp.Challenge{
// 		Index: 3,
// 		Rand:  3,
// 	})

// 	multiRes, addRes := pdp.ProofGenerate(challenges, byteTags, byteBlocks)
// 	if !pdp.Verify(g, g0, pubKey, multiRes, addRes, fileID, challenges) {
// 		t.Fatal("verify pdp failed")
// 	}
// }

// func TestGetFsBlocks(t *testing.T) {
// 	fileRoot, err := filepath.Abs(".")
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// 	dspCfg := &config.DspConfig{
// 		DBPath:        fileRoot + "/db",
// 		FsRepoRoot:    fileRoot + "/max",
// 		FsFileRoot:    fileRoot + "/downloads",
// 		FsType:        config.FS_FILESTORE,
// 		ChainRpcAddrs: []string{rpcAddr},
// 	}
// 	defer func() {
// 		os.RemoveAll(filepath.Join(fileRoot + "Log"))
// 		os.RemoveAll(dspCfg.DBPath)
// 		os.RemoveAll(dspCfg.FsRepoRoot)
// 		os.RemoveAll(dspCfg.FsFileRoot)
// 	}()
// 	d := NewDsp(dspCfg, nil, nil)
// 	prefix := "AQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAADd1hCkwGLz1wBgG/VJRSj2RMBMOgAAAAAAACd+AAAAAHiE5fA="
// 	blks, err := d.fs.NodesFromFile("/Users/zhijie/Desktop/SeekLogs/filmlabtest1128_01.zip", prefix, false, "")
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// 	for _, hash := range blks {
// 		fmt.Println(hash)
// 	}
// 	fmt.Printf("hash: %d\n", len(blks))
// }

// func TestGetUnproveFiles(t *testing.T) {
// 	dspCfg := &config.DspConfig{}
// 	w, err := wallet.OpenWallet(walletFile)
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// 	acc, err := w.GetDefaultAccount([]byte(walletPwd))
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// 	log.Infof("wallet address:%s", acc.Address.ToBase58())
// 	d := NewDsp(dspCfg, acc, nil)
// 	if d == nil {
// 		t.Fatal("dsp init failed")
// 	}
// 	fileInfos, err := d.chain.GetUnprovePrimaryFileInfos(acc.Address)
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// 	fmt.Printf("len = %d\n", len(fileInfos))

// 	for i := len(fileInfos) - 1; i >= 0; i-- {
// 		fi := fileInfos[i]
// 		fmt.Printf("%d hash %s, height: %d primary %s\n", i, fi.FileHash, fi.BlockHeight, fi.PrimaryNodes.AddrList[0].ToBase58())
// 	}
// 	details, err := d.chain.GetFileProveDetails("QmT8DH1Pbfc5JBXFX5Guff2RmytoFPsvxxjCpXDFVJRmtL")
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// 	fmt.Printf("details %v\n", details.ProveDetailNum)
// 	for _, d := range details.ProveDetails {
// 		fmt.Printf("addr: %s, %s, %d \n", d.NodeAddr, d.WalletAddr.ToBase58(), d.ProveTimes)
// 	}

// 	os.RemoveAll(filepath.Base(".") + "/Log")
// }

// func TestGetFileInfo(t *testing.T) {
// 	dspCfg := &config.DspConfig{
// 		ChainRpcAddrs: []string{"http://139.219.136.38:20336"},
// 	}
// 	w, err := wallet.OpenWallet(walletFile)
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// 	acc, err := w.GetDefaultAccount([]byte(walletPwd))
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// 	log.Infof("wallet address:%s", acc.Address.ToBase58())
// 	d := NewDsp(dspCfg, acc, nil)
// 	if d == nil {
// 		t.Fatal("dsp init failed")
// 	}
// 	fileHash := "QmTPpxU6QuKVs4aoVt7zUMSwqQmqwTheQ8egB19fvdj9Ai"
// 	info, err := d.chain.GetFileInfo(fileHash)
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// 	fmt.Printf("fileHash: %s, copyNum : %d, owner %s\n", fileHash, info.CopyNum, info.FileOwner.ToBase58())
// 	curHeight, err := d.chain.GetCurrentBlockHeight()
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// 	fmt.Printf("filename %s\n", info.FileDesc)
// 	fmt.Printf("curHeight %d block num %d\n", curHeight, info.FileBlockNum)
// 	fmt.Printf("height %d time: %d\n", info.BlockHeight, time.Now().Unix()-(int64(curHeight-uint32(info.BlockHeight)))*5)
// 	fmt.Printf("expiredHeight %d\n", info.ExpiredHeight)
// 	host, err := d.chain.GetNodeHostAddrListByWallets(info.PrimaryNodes.AddrList)
// 	for idx, wa := range info.PrimaryNodes.AddrList {
// 		fmt.Printf("wallet: %s, addr: %s\n", wa.ToBase58(), host[idx])
// 	}
// 	details, err := d.chain.GetFileProveDetails(fileHash)
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// 	fmt.Printf("details %v\n", details.ProveDetailNum)
// 	for _, d := range details.ProveDetails {
// 		fmt.Printf("addr: %s, %s, %d \n", d.NodeAddr, d.WalletAddr.ToBase58(), d.ProveTimes)
// 	}
// 	os.RemoveAll(filepath.Base(".") + "/Log")
// }

// func TestGetChannelIdentifier(t *testing.T) {
// 	dspCfg := &config.DspConfig{
// 		ChainRpcAddrs: []string{"http://139.219.136.38:20336"},
// 	}
// 	w, err := wallet.OpenWallet(walletFile)
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// 	acc, err := w.GetDefaultAccount([]byte(walletPwd))
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// 	log.Infof("wallet address:%s", acc.Address.ToBase58())
// 	d := NewDsp(dspCfg, acc, nil)
// 	if d == nil {
// 		t.Fatal("dsp init failed")
// 	}
// 	addr1, _ := chainCom.AddressFromBase58("AVmZet4LgYATPoR5Zhat3GCd5PccKqJVVY")
// 	addr2, _ := chainCom.AddressFromBase58("AHmUFP3au9odqgfm3FzSXRUyBxMcNTR8uS")
// 	channelID, err := d.chain.SDK().Native.Channel.GetChannelIdentifier(addr1, addr2)
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// 	fmt.Printf("channel id %v\n", channelID)

// 	os.RemoveAll(filepath.Base(".") + "/Log")
// }

// // CheckFilePrivilege. check if the downloader has privilege to download file
// func CheckFilePrivilege(themis *themisSDK.Chain, fileHashStr, walletAddr string) bool {
// 	info, err := themis.Native.Fs.GetFileInfo(fileHashStr)
// 	if err != nil || info == nil {
// 		log.Errorf("file info not exist %s", fileHashStr)
// 		return false
// 	}
// 	// TODO: check sinature
// 	if info.FileOwner.ToBase58() == walletAddr {
// 		return true
// 	}
// 	if info.Privilege == fs.PUBLIC {
// 		return true
// 	}
// 	if info.Privilege == fs.PRIVATE {
// 		return false
// 	}
// 	whitelist, err := themis.Native.Fs.GetWhiteList(fileHashStr)
// 	if err != nil || whitelist == nil {
// 		return true
// 	}
// 	currentHeight, err := themis.GetCurrentBlockHeight()
// 	if err != nil {
// 		return false
// 	}
// 	for _, r := range whitelist.List {
// 		if r.Addr.ToBase58() != walletAddr {
// 			continue
// 		}
// 		if r.BaseHeight <= uint64(currentHeight) && uint64(currentHeight) <= r.ExpireHeight {
// 			return true
// 		}
// 	}
// 	fmt.Println("here")
// 	return false
// }

// func TestGetNodelist(t *testing.T) {
// 	dspCfg := &config.DspConfig{
// 		ChainRpcAddrs: []string{"http://127.0.0.1:20336"},
// 	}
// 	w, err := wallet.OpenWallet(walletFile)
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// 	acc, err := w.GetDefaultAccount([]byte(walletPwd))
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// 	log.Infof("wallet address:%s", acc.Address.ToBase58())
// 	d := NewDsp(dspCfg, acc, nil)
// 	if d == nil {
// 		t.Fatal("dsp init failed")
// 	}
// 	list, err := d.chain.GetNodeList()
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// 	for _, l := range list.NodeInfo {
// 		fmt.Printf("%s, remain: %d\n", l.NodeAddr, l.RestVol)
// 	}

// }

// func TestGetDNSNodeInfo(t *testing.T) {
// 	dspCfg := &config.DspConfig{
// 		ChainRpcAddrs: []string{"http://127.0.0.1:20336"},
// 	}
// 	w, err := wallet.OpenWallet(walletFile)
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// 	acc, err := w.GetDefaultAccount([]byte(walletPwd))
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// 	log.Infof("wallet address:%s", acc.Address.ToBase58())
// 	d := NewDsp(dspCfg, acc, nil)
// 	if d == nil {
// 		t.Fatal("dsp init failed")
// 	}

// 	walletAddr, err := chainCom.AddressFromBase58("AaH1pqFW3YskUDwpg7XqrqxgviBb57JyK1")
// 	if err != nil {

// 	}
// 	info, err := d.chain.GetDnsNodeByAddr(walletAddr)
// 	if err != nil {

// 	}
// 	fmt.Printf("host %s:%s\n", info.IP, info.Port)

// }

// func TestGetNodePubKey(t *testing.T) {
// 	dspCfg := &config.DspConfig{
// 		ChainRpcAddrs: []string{"http://127.0.0.1:20336"},
// 	}
// 	w, err := wallet.OpenWallet(walletFile)
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// 	acc, err := w.GetDefaultAccount([]byte(walletPwd))
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// 	log.Infof("wallet address:%s", acc.Address.ToBase58())
// 	d := NewDsp(dspCfg, acc, nil)
// 	if d == nil {
// 		t.Fatal("dsp init failed")
// 	}

// 	walletAddr, err := chainCom.AddressFromBase58("ALQ6RWJENsELE7ATuzHz4zgHrq573xJsnM")
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// 	pubKey, err := d.chain.SDK().Native.Channel.GetNodePubKey(walletAddr)
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// 	log.Infof("pubkey %x", pubKey)
// }
