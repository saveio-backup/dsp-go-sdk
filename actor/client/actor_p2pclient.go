package client

import (
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/ontio/ontology-eventbus/actor"
	"github.com/saveio/dsp-go-sdk/common"
	dspErr "github.com/saveio/dsp-go-sdk/error"
	chainCom "github.com/saveio/themis/common"
	"github.com/saveio/themis/common/log"
)

var P2pServerPid *actor.PID

func SetP2pPid(p2pPid *actor.PID) {
	P2pServerPid = p2pPid
}

type P2pResp struct {
	Error error
}

type P2pBoolResp struct {
	Value bool
	Error error
}

type P2pStringResp struct {
	Value string
	Error error
}

type P2pStringSliceResp struct {
	Value []string
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
	Address  string
	MsgId    string
	Data     proto.Message
	Response chan *P2pResp
}

type RecvMsg struct {
	From    string
	Message proto.Message
}

type BroadcastReq struct {
	Addresses []string
	MsgId     string
	Data      proto.Message
	Action    func(proto.Message, string) bool
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
	Address  string
	Data     proto.Message
	Retry    int
	Timeout  int
	Response chan *RequestWithRetryResp
}

type SendAndWaitReplyReq struct {
	Address  string
	MsgId    string
	Data     proto.Message
	Response chan *RequestWithRetryResp
}

type RequestWithRetryResp struct {
	Data  proto.Message
	Error error
}

type WaitForConnectedReq struct {
	Address  string
	Timeout  time.Duration
	Response chan *P2pResp
}

type P2pNetType int

const (
	P2pNetTypeDsp P2pNetType = iota
	P2pNetTypeChannel
)

type ReconnectPeerReq struct {
	NetType  P2pNetType
	Address  string
	Response chan *P2pResp
}

type ConnectionExistReq struct {
	NetType  P2pNetType
	Address  string
	Response chan *P2pBoolResp
}

type CompleteTorrentReq struct {
	Address  string
	Hash     []byte
	IP       string
	Port     uint64
	Response chan *P2pResp
}

type TorrentPeersReq struct {
	Address  string
	Hash     []byte
	Response chan *P2pStringSliceResp
}

type EndpointRegistryReq struct {
	Address    string
	WalletAddr chainCom.Address
	IP         string
	Port       uint64
	Response   chan *P2pResp
}

type GetEndpointReq struct {
	Address    string
	WalletAddr chainCom.Address
	Response   chan *P2pStringResp
}

type IsPeerNetQualityBadReq struct {
	NetType  P2pNetType
	Address  string
	Response chan *P2pBoolResp
}

type AppendAddrToHealthCheckReq struct {
	NetType  P2pNetType
	Address  string
	Response chan *P2pResp
}

func P2pConnectionExist(address string, netType P2pNetType) (bool, error) {
	chReq := &ConnectionExistReq{
		Address:  address,
		NetType:  netType,
		Response: make(chan *P2pBoolResp, 1),
	}
	P2pServerPid.Tell(chReq)
	select {
	case resp := <-chReq.Response:
		if resp != nil {
			return resp.Value, resp.Error
		}
		return false, dspErr.New(dspErr.NETWORK_CONNECT_ERROR, "[P2pConnectionExist] no response")
	case <-time.After(time.Duration(common.ACTOR_P2P_REQ_TIMEOUT) * time.Second):
		return false, dspErr.New(dspErr.NETWORK_TIMEOUT, "[P2pConnectionExist] timeout")
	}
}

func P2pWaitForConnected(address string, timeout time.Duration) error {
	chReq := &WaitForConnectedReq{
		Address:  address,
		Timeout:  timeout,
		Response: make(chan *P2pResp, 1),
	}
	P2pServerPid.Tell(chReq)
	select {
	case resp := <-chReq.Response:
		if resp != nil && resp.Error != nil {
			return dspErr.NewWithError(dspErr.NETWORK_CONNECT_ERROR, resp.Error)
		}
		return nil
	case <-time.After(time.Duration(common.ACTOR_P2P_REQ_TIMEOUT) * time.Second):
		return dspErr.New(dspErr.NETWORK_TIMEOUT, "[P2pWaitForConnected] timeout")
	}
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
			return dspErr.NewWithError(dspErr.NETWORK_CONNECT_ERROR, resp.Error)
		}
		return nil
	case <-time.After(time.Duration(common.ACTOR_P2P_REQ_TIMEOUT) * time.Second):
		return dspErr.New(dspErr.NETWORK_TIMEOUT, "[P2pConnect] timeout")
	}
}

func P2pReconnectPeer(address string, netType P2pNetType) error {
	chReq := &ReconnectPeerReq{
		NetType:  netType,
		Address:  address,
		Response: make(chan *P2pResp, 1),
	}
	P2pServerPid.Tell(chReq)
	select {
	case resp := <-chReq.Response:
		if resp != nil && resp.Error != nil {
			return dspErr.NewWithError(dspErr.NETWORK_CONNECT_ERROR, resp.Error)
		}
		return nil
	case <-time.After(time.Duration(common.ACTOR_P2P_REQ_TIMEOUT) * time.Second):
		return dspErr.New(dspErr.NETWORK_TIMEOUT, "[P2pConnect] timeout")
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
			return dspErr.NewWithError(dspErr.NETWORK_CLOSE_ERROR, resp.Error)
		}
		return nil
	case <-time.After(time.Duration(common.ACTOR_P2P_REQ_TIMEOUT) * time.Second):
		return dspErr.New(dspErr.NETWORK_TIMEOUT, "[P2pClose] timeout")
	}
}

func P2pSend(address, msgId string, data proto.Message) error {
	chReq := &SendReq{
		Address:  address,
		MsgId:    msgId,
		Data:     data,
		Response: make(chan *P2pResp, 1),
	}
	P2pServerPid.Tell(chReq)
	select {
	case resp := <-chReq.Response:
		if resp != nil && resp.Error != nil {
			return dspErr.NewWithError(dspErr.NETWORK_SEND_ERROR, resp.Error)
		}
		return nil
	case <-time.After(time.Duration(common.MAX_ACTOR_P2P_REQ_TIMEOUT) * time.Second):
		return dspErr.New(dspErr.NETWORK_TIMEOUT, "[P2pSend] timeout")
	}
}

// P2pBroadcast. broadcast one msg to different addresses
func P2pBroadcast(addresses []string, data proto.Message, msgId string, actions ...func(proto.Message, string) bool) (map[string]error, error) {
	chReq := &BroadcastReq{
		Addresses: addresses,
		MsgId:     msgId,
		Data:      data,
		Response:  make(chan *BroadcastResp, 1),
	}
	if actions != nil && len(actions) > 0 {
		chReq.Action = actions[0]
	}
	P2pServerPid.Tell(chReq)
	select {
	case resp := <-chReq.Response:
		if resp != nil && resp.Error != nil {
			return nil, dspErr.NewWithError(dspErr.NETWORK_BROADCAST_ERROR, resp.Error)
		}
		return resp.Result, nil
	case <-time.After(time.Duration(common.MAX_ACTOR_P2P_REQ_TIMEOUT*len(addresses)) * time.Second):
		return nil, dspErr.New(dspErr.NETWORK_TIMEOUT, "p2p broadcast timeout")
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
	case <-time.After(time.Duration(common.ACTOR_P2P_REQ_TIMEOUT) * time.Second):
		log.Errorf("[P2pGetPublicAddr] timeout")
		return ""
	}
}

// // P2pRequestWithRetry. send p2p msg by request method, with <retry> times. Each retry has timeout of <timeout> sec
// func P2pRequestWithRetry(msg proto.Message, peer string, retry, timeout int) (proto.Message, error) {
// 	chReq := &RequestWithRetryReq{
// 		Address:  peer,
// 		Data:     msg,
// 		Retry:    retry,
// 		Timeout:  timeout,
// 		Response: make(chan *RequestWithRetryResp, 1),
// 	}
// 	P2pServerPid.Tell(chReq)
// 	select {
// 	case resp := <-chReq.Response:
// 		if resp != nil && resp.Error != nil {
// 			log.Errorf("[P2pRequestWithRetry] resp.Error %s", resp.Error)
// 			return nil, dspErr.NewWithError(dspErr.NETWORK_REQ_ERROR, resp.Error)
// 		}
// 		return resp.Data, nil
// 	case <-time.After(time.Duration(common.ACTOR_MAX_P2P_REQ_TIMEOUT+1) * time.Second):
// 		return nil, dspErr.New(dspErr.NETWORK_TIMEOUT, "[P2pRequestWithRetry] send request msg to %s timeout", peer)
// 	}
// }

// P2pSendAndWaitReply.
func P2pSendAndWaitReply(peer, msgId string, msg proto.Message) (proto.Message, error) {
	chReq := &SendAndWaitReplyReq{
		Address:  peer,
		Data:     msg,
		MsgId:    msgId,
		Response: make(chan *RequestWithRetryResp, 1),
	}
	P2pServerPid.Tell(chReq)
	select {
	case resp := <-chReq.Response:
		if resp != nil && resp.Error != nil {
			log.Errorf("[P2pSendAndWaitReply] resp.Error %s", resp.Error)
			return nil, dspErr.NewWithError(dspErr.NETWORK_REQ_ERROR, resp.Error)
		}
		return resp.Data, nil
	case <-time.After(time.Duration(common.MAX_ACTOR_P2P_REQ_TIMEOUT) * time.Second):
		return nil, dspErr.New(dspErr.NETWORK_TIMEOUT, "[P2pSendAndWaitReply] send request msg to %s timeout", peer)
	}
}

func P2pCompleteTorrent(hash []byte, ip string, port uint64, targetDnsAddr string) error {
	chReq := &CompleteTorrentReq{
		Address:  targetDnsAddr,
		Hash:     hash,
		IP:       ip,
		Port:     port,
		Response: make(chan *P2pResp, 1),
	}
	P2pServerPid.Tell(chReq)
	select {
	case resp := <-chReq.Response:
		if resp != nil && resp.Error != nil {
			return dspErr.NewWithError(dspErr.DNS_PUSH_TRACKER_ERROR, resp.Error)
		}
		return nil
	case <-time.After(time.Duration(common.ACTOR_P2P_REQ_TIMEOUT) * time.Second):
		return dspErr.New(dspErr.NETWORK_TIMEOUT, "[P2pCompleteTorrent] timeout")
	}
}

func P2pTorrentPeers(hash []byte, targetDnsAddr string) ([]string, error) {
	chReq := &TorrentPeersReq{
		Address:  targetDnsAddr,
		Hash:     hash,
		Response: make(chan *P2pStringSliceResp, 1),
	}
	P2pServerPid.Tell(chReq)
	select {
	case resp := <-chReq.Response:
		if resp != nil {
			return resp.Value, resp.Error
		}
		return nil, dspErr.New(dspErr.DNS_REQ_TRACKER_ERROR, "response is nil")
	case <-time.After(time.Duration(common.ACTOR_P2P_REQ_TIMEOUT) * time.Second):
		log.Errorf("[P2pTorrentPeers] timeout")
		return nil, dspErr.New(dspErr.NETWORK_TIMEOUT, "[P2pTorrentPeers] timeout")
	}
}

func P2pEndpointRegistry(addr chainCom.Address, ip string, port uint64, targetDnsAddr string) error {
	chReq := &EndpointRegistryReq{
		Address:    targetDnsAddr,
		WalletAddr: addr,
		IP:         ip,
		Port:       port,
		Response:   make(chan *P2pResp, 1),
	}
	P2pServerPid.Tell(chReq)
	select {
	case resp := <-chReq.Response:
		if resp != nil && resp.Error != nil {
			return dspErr.NewWithError(dspErr.DNS_PUSH_TRACKER_ERROR, resp.Error)
		}
		return nil
	case <-time.After(time.Duration(common.ACTOR_P2P_REQ_TIMEOUT) * time.Second):
		return dspErr.New(dspErr.NETWORK_TIMEOUT, "[P2pEndpointRegistry] timeout")
	}
}

func P2pGetEndpointAddr(addr chainCom.Address, targetDnsAddr string) (string, error) {
	chReq := &GetEndpointReq{
		Address:    targetDnsAddr,
		WalletAddr: addr,
		Response:   make(chan *P2pStringResp, 1),
	}
	P2pServerPid.Tell(chReq)
	select {
	case resp := <-chReq.Response:
		if resp != nil {
			return resp.Value, resp.Error
		}
		return "", dspErr.New(dspErr.DNS_REQ_TRACKER_ERROR, "response is nil")
	case <-time.After(time.Duration(common.ACTOR_P2P_REQ_TIMEOUT) * time.Second):
		log.Errorf("[P2pGetEndpointAddr] timeout")
		return "", dspErr.New(dspErr.NETWORK_TIMEOUT, "[P2pGetEndpointAddr] timeout")
	}
}

func P2pIsPeerNetQualityBad(address string, netType P2pNetType) (bool, error) {
	chReq := &IsPeerNetQualityBadReq{
		Address:  address,
		NetType:  netType,
		Response: make(chan *P2pBoolResp, 1),
	}
	P2pServerPid.Tell(chReq)
	select {
	case resp := <-chReq.Response:
		if resp != nil {
			return resp.Value, resp.Error
		}
		return false, dspErr.New(dspErr.NETWORK_REQ_ERROR, "[P2pIsPeerNetQualityBad] no response")
	case <-time.After(time.Duration(common.ACTOR_P2P_REQ_TIMEOUT) * time.Second):
		return false, dspErr.New(dspErr.NETWORK_REQ_ERROR, "[P2pIsPeerNetQualityBad] timeout")
	}
}

func P2pAppendAddrForHealthCheck(address string, netType P2pNetType) error {
	chReq := &AppendAddrToHealthCheckReq{
		Address:  address,
		NetType:  netType,
		Response: make(chan *P2pResp, 1),
	}
	P2pServerPid.Tell(chReq)
	select {
	case resp := <-chReq.Response:
		if resp != nil {
			return resp.Error
		}
		return dspErr.New(dspErr.NETWORK_REQ_ERROR, "[P2pAppendAddrForHealthCheck] no response")
	case <-time.After(time.Duration(common.ACTOR_P2P_REQ_TIMEOUT) * time.Second):
		return dspErr.New(dspErr.NETWORK_REQ_ERROR, "[P2pAppendAddrForHealthCheck] timeout")
	}
}
