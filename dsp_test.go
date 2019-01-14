package dsp

import (
	"fmt"
	"testing"
	"time"

	"github.com/oniio/dsp-go-sdk/network/common"
	"github.com/oniio/dsp-go-sdk/network/message"
	"github.com/oniio/oniChain-go-sdk"
	"github.com/oniio/oniChain/common/log"
)

var rpcAddr = "http://127.0.0.1:20336"
var node1ListAddr = "tcp://127.0.0.1:4001"
var node2ListAddr = "tcp://127.0.0.1:4002"

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

func TestDspReceive(t *testing.T) {
	log.InitLog(1, log.PATH, log.Stdout)
	d := NewDsp()
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
			Version: common.MESSAGE_VERSION,
			Type:    common.MSG_TYPE_BLOCK,
			Length:  0,
		}
		d.Network.Send(msg, node1ListAddr)
	}
}
