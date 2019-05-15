package client

import (
	"errors"
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

type ConnectReq struct {
	Address string
}

type CloseReq struct {
	Address string
}

type SendReq struct {
	Address interface{}
	Data    proto.Message
}
type P2pResp struct {
	Error error
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
}

type PeerListeningReq struct {
	Address string
}

type PublicAddrReq struct{}
type PublicAddrResp struct {
	Addr string
}

type RequestWithRetryReq struct {
	Address interface{}
	Data    proto.Message
	Retry   int
}
type RequestWithRetryResp struct {
	Data  proto.Message
	Error error
}

func P2pConnect(address string) error {
	chReq := &ConnectReq{address}
	future := P2pServerPid.RequestFuture(chReq, common.P2P_REQ_TIMEOUT*time.Second)
	if _, err := future.Result(); err != nil {
		log.Error("[P2pConnect] error: ", err)
		return err
	}
	return nil
}

func P2pClose(address string) error {
	chReq := &CloseReq{address}
	future := P2pServerPid.RequestFuture(chReq, common.P2P_REQ_TIMEOUT*time.Second)
	if _, err := future.Result(); err != nil {
		log.Error("[P2pClose] error: ", err)
		return err
	}
	return nil
}

func P2pSend(address interface{}, data proto.Message) error {
	chReq := &SendReq{address, data}
	future := P2pServerPid.RequestFuture(chReq, common.P2P_REQ_TIMEOUT*time.Second)
	in, err := future.Result()
	log.Debugf("send msg to %v in %v err %v", address, in, err)
	if err != nil {
		log.Error("[P2pSend] error: ", err)
		return err
	}
	return nil
}

func P2pBroadcast(addresses []string, data proto.Message, needReply bool, stop func() bool, action func(proto.Message, string)) error {
	chReq := &BroadcastReq{addresses, data, needReply, stop, action}
	future := P2pServerPid.RequestFuture(chReq, common.P2P_REQ_TIMEOUT*time.Second)
	if _, err := future.Result(); err != nil {
		log.Error("[P2pBroadcast] error: ", err)
		return err
	}
	return nil
}

func P2pIsPeerListening(address string) error {
	chReq := &PeerListeningReq{address}
	future := P2pServerPid.RequestFuture(chReq, common.P2P_REQ_TIMEOUT*time.Second)
	if _, err := future.Result(); err != nil {
		log.Error("[P2pSend] error: ", err)
		return err
	}
	return nil
}

func P2pGetPublicAddr() string {
	chReq := &PublicAddrReq{}
	future := P2pServerPid.RequestFuture(chReq, common.P2P_REQ_TIMEOUT*time.Second)
	in, err := future.Result()
	if err != nil {
		log.Error("[P2pGetPublicAddr] error: ", err)
		return ""
	}
	resp, ok := in.(*PublicAddrResp)
	if !ok {
		return ""
	}
	return resp.Addr
}

func P2pRequestWithRetry(msg proto.Message, peer interface{}, retry int) (proto.Message, error) {
	chReq := &RequestWithRetryReq{peer, msg, retry}
	future := P2pServerPid.RequestFuture(chReq, common.P2P_REQ_TIMEOUT*time.Second)
	in, err := future.Result()
	if err != nil {
		log.Error("[P2pRequestWithRetry] error: ", err)
		return nil, err
	}
	resp, ok := in.(*RequestWithRetryResp)
	if !ok {
		return nil, errors.New("[P2pRequestWithRetry]r equestWithRetryResp type assert failed")
	}
	return resp.Data, nil
}
