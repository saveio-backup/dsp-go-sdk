package dsp

import (
	"fmt"
	"testing"
	"time"

	"github.com/oniio/oniChain-go-sdk/wallet"

	"github.com/oniio/dsp-go-sdk/common"
	"github.com/oniio/dsp-go-sdk/store"
	"github.com/oniio/oniChain-go-sdk"
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
	d := NewDsp()
	d.Chain = chain.NewChain()
	d.Chain.NewRpcClient().SetAddress(rpcAddr)
	height, err := d.Chain.GetCurrentBlockHeight()
	if err != nil {
		fmt.Printf("get block height err: %s", err)
		return
	}
	fmt.Printf("current block height: %d\n", height)
}
func TestGetVersion(t *testing.T) {
	d := NewDsp()
	version := d.GetVersion()
	fmt.Printf("version: %s\n", version)
}

func TestNodeRegister(t *testing.T) {
	d := NewDsp()
	d.Chain = chain.NewChain()
	d.Chain.NewRpcClient().SetAddress(rpcAddr)
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
	d := NewDsp()
	d.Chain = chain.NewChain()
	d.Chain.NewRpcClient().SetAddress(rpcAddr)
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
	d := NewDsp()
	d.Chain = chain.NewChain()
	d.Chain.NewRpcClient().SetAddress(rpcAddr)
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
	d := NewDsp()
	d.Chain = chain.NewChain()
	d.Chain.NewRpcClient().SetAddress(rpcAddr)
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
	d := NewDsp()
	d.Chain = chain.NewChain()
	d.Chain.NewRpcClient().SetAddress(rpcAddr)
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
	d := NewDsp()
	d.Chain = chain.NewChain()
	d.Chain.NewRpcClient().SetAddress(rpcAddr)
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

func TestStartDspNode4(t *testing.T) {
	log.InitLog(1, log.PATH, log.Stdout)
	d := NewDsp()
	d.taskMgr.FileDB = store.NewFileDB("./db3")
	d.Chain = chain.NewChain()
	d.Chain.NewRpcClient().SetAddress(rpcAddr)
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

func TestUploadFile(t *testing.T) {
	d := NewDsp()
	d.taskMgr.FileDB = store.NewFileDB("./db1")
	d.Start(node2ListAddr)
	d.Chain = chain.NewChain()
	d.Chain.NewRpcClient().SetAddress(rpcAddr)
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
		CopyNum:         1,
		Encrypt:         false,
		EncryptPassword: "",
	}
	progress := make(chan *common.UploadingInfo, 1)
	go func() {
		stop := false
		for {
			v := <-progress
			log.Infof("file:%s, hash:%s, total:%d, uploaded:%d, progress:%f", v.FileName, v.FileHash, v.Total, v.Uploaded, float64(v.Uploaded)/float64(v.Total))
			stop = v.Uploaded == v.Total
			if stop {
				break
			}
		}
		log.Infof("progress finished")
	}()
	ret, err := d.UploadFile(uploadTestFile, opt, progress)
	if err != nil {
		log.Errorf("upload file failed, err:%s", err)
		return
	}
	log.Infof("upload file success, ret:%v", ret)
}

func TestDeleteFile(t *testing.T) {
	d := NewDsp()
	d.taskMgr.FileDB = store.NewFileDB("./db1")
	d.Start(node2ListAddr)
	d.Chain = chain.NewChain()
	d.Chain.NewRpcClient().SetAddress(rpcAddr)
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
	ret, err := d.DeleteUploadedFile("QmQgTa5UDCfBBokfvi4UBCPx9FkpWCaqEer9f59hE7EyTr")
	if err != nil {
		log.Errorf("delete file failed, err:%s", err)
		return
	}
	log.Infof("delete file success, ret:%v", ret)
}

func TestDownloadFile(t *testing.T) {
	d := NewDsp()
	d.taskMgr.FileDB = store.NewFileDB("./db2")
	d.Start(node3ListAddr)
	d.Chain = chain.NewChain()
	d.Chain.NewRpcClient().SetAddress(rpcAddr)
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
	addrs := []string{node1ListAddr, node4ListAddr}
	err = d.DownloadFile("QmQgTa5UDCfBBokfvi4UBCPx9FkpWCaqEer9f59hE7EyTr", true, addrs)
	if err != nil {
		log.Errorf("download err %s\n", err)
	}
	// use for testing go routines for tasks are released or not
	time.Sleep(time.Duration(5) * time.Second)
}
