package client

import (
	"fmt"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/ontio/ontology-eventbus/actor"
	"github.com/saveio/dsp-go-sdk/common"
	"github.com/saveio/themis/common/log"
)

var P2pServerPid *actor.PID

func SetP2pPid(p2pPid *actor.PID) {
	P2pServerPid = p2pPid
}

type P2pResp struct {
	Error error
}

type ConnectReq struct {
	Address  string
	Response chan *P2pResp
}

type CloseReq struct {
	Address  string
	Response chan *P2pResp
}

type SendReq struct {
	Address  interface{}
	Data     proto.Message
	Response chan *P2pResp
}

type RecvMsg struct {
	From    string
	Message proto.Message
}

type BroadcastReq struct {
	Addresses []string
	Data      proto.Message
	NeedReply bool
	Stop      func() bool
	Action    func(proto.Message, string)
	Response  chan *BroadcastResp
}

type BroadcastResp struct {
	Result map[string]error
	Error  error
}

type PeerListeningReq struct {
	Address  string
	Response chan *P2pResp
}

type PublicAddrReq struct {
	Response chan *PublicAddrResp
}
type PublicAddrResp struct {
	Addr  string
	Error error
}

type RequestWithRetryReq struct {
	Address  interface{}
	Data     proto.Message
	Retry    int
	Response chan *RequestWithRetryResp
}

type RequestWithRetryResp struct {
	Data  proto.Message
	Error error
}

func P2pConnect(address string) error {
	chReq := &ConnectReq{
		Address:  address,
		Response: make(chan *P2pResp, 1),
	}
	P2pServerPid.Tell(chReq)
	select {
	case resp := <-chReq.Response:
		if resp != nil && resp.Error != nil {
			return resp.Error
		}
		return nil
	case <-time.After(time.Duration(common.P2P_REQ_TIMEOUT) * time.Second):
		return fmt.Errorf("[P2pConnect] timeout")
	}
}

func P2pClose(address string) error {
	chReq := &CloseReq{
		Address:  address,
		Response: make(chan *P2pResp, 1),
	}
	P2pServerPid.Tell(chReq)
	select {
	case resp := <-chReq.Response:
		if resp != nil && resp.Error != nil {
			return resp.Error
		}
		return nil
	case <-time.After(time.Duration(common.P2P_REQ_TIMEOUT) * time.Second):
		return fmt.Errorf("[P2pClose] timeout")
	}
}

func P2pSend(address interface{}, data proto.Message) error {
	chReq := &SendReq{
		Address:  address,
		Data:     data,
		Response: make(chan *P2pResp, 1),
	}
	P2pServerPid.Tell(chReq)
	select {
	case resp := <-chReq.Response:
		if resp != nil && resp.Error != nil {
			return resp.Error
		}
		return nil
	case <-time.After(time.Duration(common.P2P_REQ_TIMEOUT) * time.Second):
		return fmt.Errorf("[P2pSend] timeout")
	}
}

func P2pBroadcast(addresses []string, data proto.Message, needReply bool, stop func() bool, action func(proto.Message, string)) (map[string]error, error) {
	chReq := &BroadcastReq{
		Addresses: addresses,
		Data:      data,
		NeedReply: needReply,
		Stop:      stop,
		Action:    action,
		Response:  make(chan *BroadcastResp, 1),
	}
	P2pServerPid.Tell(chReq)
	select {
	case resp := <-chReq.Response:
		if resp != nil && resp.Error != nil {
			return nil, resp.Error
		}
		return resp.Result, nil
	case <-time.After(time.Duration(common.P2P_BROADCAST_TIMEOUT*len(addresses)) * time.Second):
		return nil, fmt.Errorf("[P2pBroadcast] timeout")
	}
}

func P2pIsPeerListening(address string) error {
	chReq := &PeerListeningReq{
		Address:  address,
		Response: make(chan *P2pResp, 1),
	}
	P2pServerPid.Tell(chReq)
	select {
	case resp := <-chReq.Response:
		if resp != nil && resp.Error != nil {
			return resp.Error
		}
		return nil
	case <-time.After(time.Duration(common.P2P_REQ_TIMEOUT) * time.Second):
		return fmt.Errorf("[P2pIsPeerListening] timeout")
	}

}

func P2pGetPublicAddr() string {
	chReq := &PublicAddrReq{
		Response: make(chan *PublicAddrResp, 1),
	}
	P2pServerPid.Tell(chReq)
	select {
	case resp := <-chReq.Response:
		if resp != nil && resp.Error != nil {
			log.Errorf("[P2pGetPublicAddr] resp.Error %s", resp.Error)
			return ""
		}
		return resp.Addr
	case <-time.After(time.Duration(common.P2P_REQ_TIMEOUT) * time.Second):
		log.Errorf("[P2pGetPublicAddr] timeout")
		return ""
	}
}

func P2pRequestWithRetry(msg proto.Message, peer interface{}, retry int) (proto.Message, error) {
	chReq := &RequestWithRetryReq{
		Address:  peer,
		Data:     msg,
		Retry:    retry,
		Response: make(chan *RequestWithRetryResp, 1),
	}
	P2pServerPid.Tell(chReq)
	select {
	case resp := <-chReq.Response:
		if resp != nil && resp.Error != nil {
			log.Errorf("[P2pGetPublicAddr] resp.Error %s", resp.Error)
			return nil, resp.Error
		}
		return resp.Data, nil
	case <-time.After(time.Duration(common.P2P_REQ_TIMEOUT) * time.Second):
		return nil, fmt.Errorf("[P2pIsPeerListening] timeout")
	}
}
