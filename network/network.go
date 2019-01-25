package network

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"

	"github.com/oniio/dsp-go-sdk/network/common"
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
	handler    func(*network.PeerClient, *message.Message)
}

func NewNetwork(addr string, handler func(*network.PeerClient, *message.Message)) *Network {
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
	if this.handler != nil {
		this.handler(ctx.Client(), msg)
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

func (this *Network) IsConnectionExists(addr string) bool {
	return this.net.ConnectionStateExists(addr)
}

func (this *Network) Connect(addr ...string) error {
	this.net.Bootstrap(addr...)
	for _, a := range addr {
		exist := this.net.ConnectionStateExists(a)
		if !exist {
			return errors.New("connection not exist")
		}
	}
	return nil
}

// Send send msg to peer
// peer can be addr(string) or client(*network.peerClient)
func (this *Network) Send(msg *message.Message, peer interface{}) error {
	addr, ok := peer.(string)
	if ok {
		client, err := this.net.Client(addr)
		if err != nil {
			return err
		}
		if client == nil {
			return errors.New("client is nil")
		}
		return client.Tell(context.Background(), msg.ToProtoMsg())
	}
	client, ok := peer.(*network.PeerClient)
	if !ok || client == nil {
		return errors.New("invalid peer type")
	}
	return client.Tell(context.Background(), msg.ToProtoMsg())
}

// Broadcast. broadcast same msg to peers. Handle action if send msg success.
// If one msg is sent failed, return err. But the previous success msgs can not be recalled.
func (this *Network) Broadcast(addrs []string, msg *message.Message, stop func() bool, action func(addr string)) error {
	wg := sync.WaitGroup{}
	maxRoutines := common.MAX_GOROUTINES_IN_LOOP
	if len(addrs) <= common.MAX_GOROUTINES_IN_LOOP {
		maxRoutines = len(addrs)
	}
	count := 0
	errs := make(map[string]error, 0)
	for _, addr := range addrs {
		wg.Add(1)
		go func(to string) {
			defer wg.Done()
			if !this.IsConnectionExists(to) {
				err := this.Connect(to)
				if err != nil {
					errs[to] = err
					return
				}
			}
			err := this.Send(msg, to)
			if err != nil {
				errs[to] = err
				return
			}
			if action != nil {
				action(to)
			}
		}(addr)
		count++
		if count >= maxRoutines {
			wg.Wait()
			// reset, start new round
			count = 0
		}
		if len(errs) > 0 {
			break
		}
		if stop != nil && stop() {
			break
		}
	}
	// wait again if last round count < maxRoutines
	wg.Wait()
	if len(errs) == 0 {
		return nil
	}
	for to, err := range errs {
		log.Errorf("broadcast msg to %s, err %s", to, err)
	}
	return errors.New("broadcast failed")
}
