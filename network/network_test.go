package network

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/saveio/dsp-go-sdk/network/common"
	"github.com/saveio/dsp-go-sdk/network/message"
	"github.com/saveio/carrier/network"
)

var node1ListAddr = "tcp://127.0.0.1:50001"
var node2ListAddr = "tcp://127.0.0.1:50002"

func TestNetworkReceiveMsg(t *testing.T) {
	n := NewNetwork(node1ListAddr, nil)
	n.Start()
	counter := 0
	n.handler = func(ctx *network.ComponentContext) {
		msg := message.ReadMessage(ctx.Message())
		if msg == nil {
			return
		}
		fmt.Printf("receive msg:%v, from address %s， id:%s, pubkey:%s\n", msg, ctx.Client().Address, ctx.Client().ID.String(), ctx.Client().ID.PublicKeyHex())
		counter++
		if counter == 1 {
			return
		}

		fmt.Printf("reply\n")
		err := ctx.Reply(context.Background(), msg.ToProtoMsg())
		if err != nil {
			fmt.Printf("reply err:%v\n", err)
		}
	}
	tick := time.NewTicker(time.Second)
	for {
		<-tick.C
	}
}

func TestNetworkSendMsg(t *testing.T) {
	n := NewNetwork(node2ListAddr, nil)
	n.Start()
	n.Connect(node1ListAddr)
	// tick := time.NewTicker(time.Duration(3) * time.Second)
	// for {
	msg := &message.Message{}
	msg.Header = &message.Header{
		Version:   "0",
		Type:      common.MSG_TYPE_BLOCK,
		MsgLength: 0,
	}
	res, err := n.Request(msg, node1ListAddr, 3)
	fmt.Printf("get response from msg:%v, err:%s\n", res, err)
	// <-tick.C
	// }
}

func TestDialIP(t *testing.T) {
	n := NewNetwork(node2ListAddr, nil)
	n.Start()
	addr := "tcp://127.0.0.1:13004"
	err := n.Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println("connected")

	time.Sleep(time.Duration(3) * time.Second)
	err = n.Disconnect(addr)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println("disconnected")
	time.Sleep(time.Duration(3) * time.Second)
}
