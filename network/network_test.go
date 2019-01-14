package network

import (
	"testing"
	"time"

	"github.com/oniio/dsp-go-sdk/network/common"
	"github.com/oniio/dsp-go-sdk/network/message"
)

var node1ListAddr = "tcp://127.0.0.1:4001"
var node2ListAddr = "tcp://127.0.0.1:4002"

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
	tick := time.NewTicker(time.Second)
	for {
		<-tick.C
		msg := &message.Message{}
		msg.Header = &message.Header{
			Version: common.MESSAGE_VERSION,
			Type:    common.MSG_TYPE_BLOCK,
			Length:  0,
		}
		n.Send(msg, node1ListAddr)
	}
}
