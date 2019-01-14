package network

import (
	"context"
	"errors"
	"fmt"
	"runtime"

	"github.com/oniio/dsp-go-sdk/network/message"
	"github.com/oniio/dsp-go-sdk/network/message/pb"
	"github.com/oniio/oniChain/common/log"
	"github.com/oniio/oniP2p/crypto/ed25519"
	"github.com/oniio/oniP2p/network"
	p2pNet "github.com/oniio/oniP2p/network"
	"github.com/oniio/oniP2p/types/opcode"
)

type Network struct {
	*p2pNet.Component
	listenAddr string
	net        *p2pNet.Network
	handler    func(*message.Message)
}

func NewNetwork(addr string, handler func(*message.Message)) *Network {
	return &Network{
		listenAddr: addr,
		handler:    handler,
	}
}

func (this *Network) Receive(ctx *network.ComponentContext) error {
	msg := message.ReadMessage(ctx.Message())
	if msg == nil {
		return errors.New("message is nil")
	}
	log.Debugf("Got msg from %s, version: %s", ctx.Client().ID.String(), msg.Header.Version)
	if this.handler != nil {
		this.handler(msg)
	}
	return nil
}

func (this *Network) Start() error {
	if this.net != nil {
		return fmt.Errorf("already listening at %s", this.listenAddr)
	}
	runtime.GOMAXPROCS(runtime.NumCPU())
	opcode.RegisterMessageType(opcode.Opcode(1000), &pb.Message{})
	builder := network.NewBuilder()
	builder.SetAddress(this.listenAddr)
	builder.SetKeys(ed25519.RandomKeyPair())
	builder.AddComponent(this)
	net, err := builder.Build()
	if err != nil {
		return err
	}
	this.net = net
	go this.net.Listen()
	return nil
}

func (this *Network) Halt() error {
	if this.net == nil {
		return errors.New("network is down")
	}
	this.net.Close()
	return nil
}

func (this *Network) Connect(addr string) {
	this.net.Bootstrap(addr)
	this.net.BlockUntilListening()
}

func (this *Network) Send(msg *message.Message, addr string) error {
	client, err := this.net.Client(addr)
	if err != nil {
		return err
	}
	return client.Tell(context.Background(), msg.ToProtoMsg())
}
