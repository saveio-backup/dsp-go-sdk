package dsp

import (
	"fmt"
	"testing"
	"time"

	"github.com/oniio/oniChain-go-sdk/wallet"

	"github.com/oniio/dsp-go-sdk/common"
	netcom "github.com/oniio/dsp-go-sdk/network/common"
	"github.com/oniio/dsp-go-sdk/network/message"
	"github.com/oniio/oniChain-go-sdk"
	"github.com/oniio/oniChain/common/log"
)

var rpcAddr = "http://127.0.0.1:20336"
var node1ListAddr = "kcp://127.0.0.1:4001"
var node2ListAddr = "kcp://127.0.0.1:4002"

var uploadTestFile = "./testdata/testuploadbigfile.txt"

// var uploadTestFile = "./testdata/testuploadfile.txt"
var walletFile = "./testdata/wallet.dat"
var wallet2File = "./testdata/wallet2.dat"
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

func TestDspReceive(t *testing.T) {
	log.InitLog(1, log.PATH, log.Stdout)
	d := NewDsp()
	d.Chain = chain.NewChain()
	d.Chain.NewRpcClient().SetAddress(rpcAddr)
	d.Start(node1ListAddr)
	tick := time.NewTicker(time.Second)
	for {
		<-tick.C
	}
}
func TestDspSendMsg(t *testing.T) {
	d := NewDsp()
	d.Start(node2ListAddr)
	d.Network.Connect(node1ListAddr)
	tick := time.NewTicker(time.Second)
	for {
		<-tick.C
		msg := &message.Message{}
		msg.Header = &message.Header{
			Version:   netcom.MESSAGE_VERSION,
			Type:      netcom.MSG_TYPE_BLOCK,
			MsgLength: 0,
		}
		d.Network.Send(msg, node1ListAddr)
	}
}

func TestUploadFile(t *testing.T) {
	d := NewDsp()
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
	opt := &common.UploadOption{
		FileDesc:        "file",
		ProveInterval:   110,
		ProveTimes:      3,
		Privilege:       1,
		CopyNum:         0,
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

func TestMsgResponse(t *testing.T) {
	d := NewDsp()
	d.Start(node2ListAddr)
	err := d.Network.Connect(node1ListAddr)
	if err != nil {
		fmt.Printf("connect err:%v\n", err)
		return
	}
	msg := message.NewFileFetchAskMsg("1", []string{"1"}, "")
	err = d.Network.Send(msg, node1ListAddr)
	if err != nil {
		fmt.Printf("send err:%v\n", err)
		return
	}
	tick := time.NewTicker(time.Second)
	for {
		<-tick.C
	}
}
