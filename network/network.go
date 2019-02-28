package network

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"time"

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
	handler    func(*network.ComponentContext)
}

func NewNetwork(addr string, handler func(*network.ComponentContext)) *Network {
	return &Network{
		listenAddr: addr,
		handler:    handler,
	}
}

func (this *Network) ListenAddr() string {
	return this.listenAddr
}

func (this *Network) Protocol() string {
	idx := strings.Index(this.listenAddr, "://")
	if idx == -1 {
		return "tcp"
	}
	return this.listenAddr[:idx]
}

func (this *Network) Receive(ctx *network.ComponentContext) error {
	if this.handler != nil {
		this.handler(ctx)
	}
	return nil
}

func (this *Network) Start() error {
	if this.net != nil {
		return fmt.Errorf("already listening at %s", this.listenAddr)
	}
	runtime.GOMAXPROCS(runtime.NumCPU())
	opcode.RegisterMessageType(opcode.Opcode(common.MSG_OP_CODE), &pb.Message{})
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
	client, err := this.loadClient(peer)
	if err != nil {
		return err
	}
	return client.Tell(context.Background(), msg.ToProtoMsg())
}

// Request. send msg to peer and wait for response synchronously
func (this *Network) Request(msg *message.Message, peer interface{}) (*message.Message, error) {
	client, err := this.loadClient(peer)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(common.REQUEST_MSG_TIMEOUT)*time.Second)
	defer cancel()
	res, err := client.Request(ctx, msg.ToProtoMsg())
	if err != nil {
		return nil, err
	}
	return message.ReadMessage(res), nil
}

// Broadcast. broadcast same msg to peers. Handle action if send msg success.
// If one msg is sent failed, return err. But the previous success msgs can not be recalled.
// action(responseMsg, responseToAddr).
func (this *Network) Broadcast(addrs []string, msg *message.Message, needReply bool, stop func() bool, action func(*message.Message, string)) error {
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
			var res *message.Message
			var err error
			if !needReply {
				err = this.Send(msg, to)
			} else {
				res, err = this.Request(msg, to)
			}
			if err != nil {
				errs[to] = err
				return
			}
			if action != nil {
				action(res, to)
			}
		}(addr)
		count++
		if count >= maxRoutines {
			wg.Wait()
			// reset, start new round
			count = 0
		}
		if stop != nil && stop() {
			break
		}
		if len(errs) > 0 {
			break
		}
	}
	// wait again if last round count < maxRoutines
	wg.Wait()
	if stop != nil && stop() {
		return nil
	}
	if len(errs) == 0 {
		return nil
	}
	for to, err := range errs {
		log.Errorf("broadcast msg to %s, err %s", to, err)
	}
	return errors.New("broadcast failed")
}

func (this *Network) loadClient(peer interface{}) (*network.PeerClient, error) {
	addr, ok := peer.(string)
	if ok {
		client, err := this.net.Client(addr)
		if err != nil {
			return nil, err
		}
		if client == nil {
			return nil, errors.New("client is nil")
		}
		return client, nil
	}
	client, ok := peer.(*network.PeerClient)
	if !ok || client == nil {
		return nil, errors.New("invalid peer type")
	}
	return client, nil
}
