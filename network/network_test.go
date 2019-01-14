package network

import (
	"fmt"
	"testing"
	"time"

	"github.com/oniio/dsp-go-sdk/network/common"
	"github.com/oniio/dsp-go-sdk/network/message"
)

var node1ListAddr = "kcp://127.0.0.1:4001"
var node2ListAddr = "kcp://127.0.0.1:4002"

func TestNetworkReceiveMsg(t *testing.T) {
	n := NewNetwork(node1ListAddr, nil)
	n.Start()
	tick := time.NewTicker(time.Second)
	for {
		<-tick.C
	}
}

func TestNetworkSendMsg(t *testing.T) {
	n := NewNetwork(node2ListAddr, nil)
	n.Start()
	n.Connect(node1ListAddr)
	tick := time.NewTicker(time.Duration(10) * time.Second)
	for {

		data1 := []byte{0x2}
		for i := 0; i < 256*1024; i++ {
			data1 = append(data1, []byte("0")...)
		}
		data1 = append(data1, []byte{0x4}...)
		fmt.Printf("len:%d\n", len(data1))
		msg := &message.Message{}
		msg.Header = &message.Header{
			Version:   string(data1),
			Type:      common.MSG_TYPE_BLOCK,
			MsgLength: 0,
		}
		// fmt.Printf("msg:%v\n", msg.Header.Version)
		n.Send(msg, node1ListAddr)
		<-tick.C
	}
}
