package client

import (
	"strings"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/ontio/ontology-eventbus/actor"
	"github.com/saveio/dsp-go-sdk/consts"
	sdkErr "github.com/saveio/dsp-go-sdk/error"
	chainCom "github.com/saveio/themis/common"
	"github.com/saveio/themis/common/log"
)

var P2PServerPid *actor.PID

func SetP2PPid(p2pPid *actor.PID) {
	P2PServerPid = p2pPid
}

type P2PNetType int

const (
	P2PNetTypeDsp P2PNetType = iota
	P2PNetTypeChannel
)

type P2PResp struct {
	Error error
}

type P2PBoolResp struct {
	Value bool
	Error error
}

type P2PStringResp struct {
	Value string
	Error error
}

type P2PStringsResp struct {
	Value []string
	Error error
}

type ConnectReq struct {
	Address  string
	Response chan *P2PResp
}

type SendReq struct {
	Address     string
	SessionId   string
	MsgId       string
	Data        proto.Message
	SendTimeout time.Duration
	Response    chan *P2PResp
}

type RecvMsg struct {
	From    string
	Message proto.Message
}

type BroadcastReq struct {
	Addresses []string
	SessionId string
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
	Response chan *P2PResp
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
	Address     string
	SessionId   string
	MsgId       string
	Data        proto.Message
	SendTimeout time.Duration
	Response    chan *RequestWithRetryResp
}

type ClosePeerSessionReq struct {
	Address   string
	SessionId string
	Response  chan *P2PResp
}

type RequestWithRetryResp struct {
	Data  proto.Message
	Error error
}

type WaitForConnectedReq struct {
	Address  string
	Timeout  time.Duration
	Response chan *P2PResp
}

type ReconnectPeerReq struct {
	NetType  P2PNetType
	Address  string
	Response chan *P2PResp
}

type ConnectionExistReq struct {
	NetType  P2PNetType
	Address  string
	Response chan *P2PBoolResp
}

type CompleteTorrentReq struct {
	Address  string
	Hash     []byte
	IP       string
	Port     uint64
	Response chan *P2PResp
}

type TorrentPeersReq struct {
	Address  string
	Hash     []byte
	Response chan *P2PStringsResp
}

type EndpointRegistryReq struct {
	Address    string
	WalletAddr chainCom.Address
	IP         string
	Port       uint64
	Response   chan *P2PResp
}

type GetEndpointReq struct {
	Address    string
	WalletAddr chainCom.Address
	Response   chan *P2PStringResp
}

type IsPeerNetQualityBadReq struct {
	NetType  P2PNetType
	Address  string
	Response chan *P2PBoolResp
}

type AppendAddrToHealthCheckReq struct {
	NetType  P2PNetType
	Address  string
	Response chan *P2PResp
}

type RemoveAddrFromHealthCheckReq struct {
	NetType  P2PNetType
	Address  string
	Response chan *P2PResp
}

type GetHostAddrReq struct {
	NetType  P2PNetType
	Address  string
	Response chan *P2PStringResp
}

type P2PSpeedResp struct {
	Tx    uint64
	Rx    uint64
	Error error
}
type PeerSessionSpeedReq struct {
	Address   string
	SessionId string
	Response  chan *P2PSpeedResp
}

// P2PGetHostAddrFromWalletAddr. get peer host addr by wallet addr.
// netType: P2P network type, including dsp network, channel network..
func P2PGetHostAddrFromWalletAddr(walletAddr string, netType P2PNetType) (string, error) {
	chReq := &GetHostAddrReq{
		Address:  walletAddr,
		NetType:  netType,
		Response: make(chan *P2PStringResp, 1),
	}
	P2PServerPid.Tell(chReq)
	select {
	case resp := <-chReq.Response:
		if resp != nil {
			return resp.Value, resp.Error
		}
		return "", sdkErr.New(sdkErr.NETWORK_CONNECT_ERROR, "[P2PGetHostAddrFromWalletAddr] no response")
	case <-time.After(time.Duration(consts.ACTOR_P2P_REQ_TIMEOUT) * time.Second):
		return "", sdkErr.New(sdkErr.NETWORK_TIMEOUT, "[P2PGetHostAddrFromWalletAddr] timeout")
	}
}

// P2PConnectionExist. check if connection is exist by wallet addr and network type
func P2PConnectionExist(address string, netType P2PNetType) (bool, error) {
	chReq := &ConnectionExistReq{
		Address:  address,
		NetType:  netType,
		Response: make(chan *P2PBoolResp, 1),
	}
	P2PServerPid.Tell(chReq)
	select {
	case resp := <-chReq.Response:
		if resp != nil {
			return resp.Value, resp.Error
		}
		return false, sdkErr.New(sdkErr.NETWORK_CONNECT_ERROR, "[P2PConnectionExist] no response")
	case <-time.After(time.Duration(consts.ACTOR_P2P_REQ_TIMEOUT) * time.Second):
		return false, sdkErr.New(sdkErr.NETWORK_TIMEOUT, "[P2PConnectionExist] timeout")
	}
}

// P2PWaitForConnected. a polling method to check connection is reachable with timeout
func P2PWaitForConnected(address string, timeout time.Duration) error {
	chReq := &WaitForConnectedReq{
		Address:  address,
		Timeout:  timeout,
		Response: make(chan *P2PResp, 1),
	}
	P2PServerPid.Tell(chReq)
	select {
	case resp := <-chReq.Response:
		if resp != nil && resp.Error != nil {
			return sdkErr.NewWithError(sdkErr.NETWORK_CONNECT_ERROR, resp.Error)
		}
		return nil
	case <-time.After(time.Duration(consts.ACTOR_P2P_REQ_TIMEOUT) * time.Second):
		return sdkErr.New(sdkErr.NETWORK_TIMEOUT, "[P2PWaitForConnected] timeout")
	}
}

// P2PConnect. dsp network connect to peer by host address. e.g: tcp://127.0.0.1:10301
func P2PConnect(address string) error {
	chReq := &ConnectReq{
		Address:  address,
		Response: make(chan *P2PResp, 1),
	}
	P2PServerPid.Tell(chReq)
	select {
	case resp := <-chReq.Response:
		if resp != nil && resp.Error != nil {
			return sdkErr.NewWithError(sdkErr.NETWORK_CONNECT_ERROR, resp.Error)
		}
		return nil
	case <-time.After(time.Duration(consts.ACTOR_P2P_REQ_TIMEOUT) * time.Second):
		return sdkErr.New(sdkErr.NETWORK_TIMEOUT, "[P2PConnect] timeout")
	}
}

// P2PReconnectPeer. network reconnect peer by wallet address.
func P2PReconnectPeer(address string, netType P2PNetType) error {
	chReq := &ReconnectPeerReq{
		NetType:  netType,
		Address:  address,
		Response: make(chan *P2PResp, 1),
	}
	P2PServerPid.Tell(chReq)
	select {
	case resp := <-chReq.Response:
		if resp != nil && resp.Error != nil {
			return sdkErr.NewWithError(sdkErr.NETWORK_CONNECT_ERROR, resp.Error)
		}
		return nil
	case <-time.After(time.Duration(consts.ACTOR_P2P_REQ_TIMEOUT) * time.Second):
		return sdkErr.New(sdkErr.NETWORK_TIMEOUT, "[P2PConnect] timeout")
	}
}

// P2PSendWithTimeout. send msg with timeout. if timeout, the network stream will be interrupted.
func P2PSendWithTimeout(address, msgId string, data proto.Message, sendTimeout time.Duration) error {
	chReq := &SendReq{
		Address:     address,
		MsgId:       msgId,
		Data:        data,
		SendTimeout: sendTimeout,
		Response:    make(chan *P2PResp, 1),
	}
	P2PServerPid.Tell(chReq)
	select {
	case resp := <-chReq.Response:
		if resp != nil && resp.Error != nil {
			return sdkErr.NewWithError(sdkErr.NETWORK_SEND_ERROR, resp.Error)
		}
		return nil
	case <-time.After(time.Duration(consts.MAX_ACTOR_P2P_REQ_TIMEOUT) * time.Second):
		return sdkErr.New(sdkErr.NETWORK_TIMEOUT, "[P2PSend] timeout")
	}
}

// P2PStreamSend.  send msg by stream. if timeout, the network stream will be interrupted.
func P2PStreamSend(address, sessionId, msgId string, data proto.Message, timeout time.Duration) error {
	chReq := &SendReq{
		Address:     address,
		SessionId:   sessionId,
		MsgId:       msgId,
		Data:        data,
		SendTimeout: timeout,
		Response:    make(chan *P2PResp, 1),
	}
	P2PServerPid.Tell(chReq)
	select {
	case resp := <-chReq.Response:
		if resp != nil && resp.Error != nil {
			return sdkErr.NewWithError(sdkErr.NETWORK_SEND_ERROR, resp.Error)
		}
		return nil
	case <-time.After(time.Duration(consts.MAX_ACTOR_P2P_REQ_TIMEOUT) * time.Second):
		return sdkErr.New(sdkErr.NETWORK_TIMEOUT, "[P2PSend] timeout")
	}
}

// P2PSend. send msg.
func P2PSend(address, msgId string, data proto.Message) error {
	chReq := &SendReq{
		Address:  address,
		MsgId:    msgId,
		Data:     data,
		Response: make(chan *P2PResp, 1),
	}
	P2PServerPid.Tell(chReq)
	select {
	case resp := <-chReq.Response:
		if resp != nil && resp.Error != nil {
			return sdkErr.NewWithError(sdkErr.NETWORK_SEND_ERROR, resp.Error)
		}
		return nil
	case <-time.After(time.Duration(consts.MAX_ACTOR_P2P_REQ_TIMEOUT) * time.Second):
		return sdkErr.New(sdkErr.NETWORK_TIMEOUT, "[P2PSend] timeout")
	}
}

// P2PBroadcast. broadcast one msg to different addresses
func P2PBroadcast(addresses []string, data proto.Message, msgId string, actions ...func(proto.Message, string) bool) (map[string]error, error) {
	chReq := &BroadcastReq{
		Addresses: addresses,
		MsgId:     msgId,
		Data:      data,
		Response:  make(chan *BroadcastResp, 1),
	}
	if actions != nil && len(actions) > 0 {
		chReq.Action = actions[0]
	}
	P2PServerPid.Tell(chReq)
	select {
	case resp := <-chReq.Response:
		if resp != nil && resp.Error != nil {
			return nil, sdkErr.NewWithError(sdkErr.NETWORK_BROADCAST_ERROR, resp.Error)
		}
		connectionErrCnt := 0
		for _, broadcastErr := range resp.Result {
			if broadcastErr != nil && strings.Contains(broadcastErr.Error(), "connected timeout") {
				connectionErrCnt++
			}
		}
		if connectionErrCnt == len(resp.Result) && connectionErrCnt != 0 {
			return nil, sdkErr.New(sdkErr.NETWORK_CONNECT_ERROR, "connect to all peers failed")
		}
		return resp.Result, nil
	case <-time.After(time.Duration(consts.MAX_ACTOR_P2P_REQ_TIMEOUT*len(addresses)) * time.Second):
		return nil, sdkErr.New(sdkErr.NETWORK_TIMEOUT, "p2p broadcast timeout")
	}
}

// P2PGetPublicAddr. get dsp network listen public host address
func P2PGetPublicAddr() string {
	chReq := &PublicAddrReq{
		Response: make(chan *PublicAddrResp, 1),
	}
	P2PServerPid.Tell(chReq)
	select {
	case resp := <-chReq.Response:
		if resp != nil && resp.Error != nil {
			log.Errorf("[P2PGetPublicAddr] resp.Error %s", resp.Error)
			return ""
		}
		return resp.Addr
	case <-time.After(time.Duration(consts.ACTOR_P2P_REQ_TIMEOUT) * time.Second):
		log.Errorf("[P2PGetPublicAddr] timeout")
		return ""
	}
}

// P2PSendAndWaitReply. send msg and wait for reply
func P2PSendAndWaitReply(peer, msgId string, msg proto.Message) (proto.Message, error) {
	chReq := &SendAndWaitReplyReq{
		Address:  peer,
		Data:     msg,
		MsgId:    msgId,
		Response: make(chan *RequestWithRetryResp, 1),
	}
	P2PServerPid.Tell(chReq)
	select {
	case resp := <-chReq.Response:
		if resp != nil && resp.Error != nil {
			log.Errorf("[P2PSendAndWaitReply] resp.Error %s", resp.Error)
			return nil, sdkErr.NewWithError(sdkErr.NETWORK_REQ_ERROR, resp.Error)
		}
		return resp.Data, nil
	case <-time.After(time.Duration(consts.MAX_ACTOR_P2P_REQ_TIMEOUT) * time.Second):
		return nil, sdkErr.New(sdkErr.NETWORK_TIMEOUT, "[P2PSendAndWaitReply] send request msg to %s timeout", peer)
	}
}

// P2PSendAndWaitReply. stream send msg and wait for reply
func P2PStreamSendAndWaitReply(peer, sessionId, msgId string, msg proto.Message, timeout time.Duration) (proto.Message, error) {
	chReq := &SendAndWaitReplyReq{
		Address:     peer,
		Data:        msg,
		SessionId:   sessionId,
		MsgId:       msgId,
		SendTimeout: timeout,
		Response:    make(chan *RequestWithRetryResp, 1),
	}
	P2PServerPid.Tell(chReq)
	select {
	case resp := <-chReq.Response:
		if resp != nil && resp.Error != nil {
			log.Errorf("[P2PSendAndWaitReply] resp.Error %s", resp.Error)
			return nil, sdkErr.NewWithError(sdkErr.NETWORK_REQ_ERROR, resp.Error)
		}
		return resp.Data, nil
	case <-time.After(time.Duration(consts.MAX_ACTOR_P2P_REQ_TIMEOUT) * time.Second):
		return nil, sdkErr.New(sdkErr.NETWORK_TIMEOUT, "[P2PSendAndWaitReply] send request msg to %s timeout", peer)
	}
}

// P2PClosePeerSession.
func P2PClosePeerSession(peer, sessionId string) error {
	chReq := &ClosePeerSessionReq{
		Address:   peer,
		SessionId: sessionId,
		Response:  make(chan *P2PResp, 1),
	}
	P2PServerPid.Tell(chReq)
	select {
	case resp := <-chReq.Response:
		if resp != nil && resp.Error != nil {
			log.Errorf("[P2PClosePeerSession] resp.Error %s", resp.Error)
			return sdkErr.NewWithError(sdkErr.NETWORK_REQ_ERROR, resp.Error)
		}
		return nil
	case <-time.After(time.Duration(consts.MAX_ACTOR_P2P_REQ_TIMEOUT) * time.Second):
		return sdkErr.New(sdkErr.NETWORK_TIMEOUT, "[P2PClosePeerSession] send request msg to %s timeout", peer)
	}
}

// P2PCompleteTorrent. send torrent complete msg to tracker
func P2PCompleteTorrent(hash []byte, ip string, port uint64, targetDnsAddr string) error {
	chReq := &CompleteTorrentReq{
		Address:  targetDnsAddr,
		Hash:     hash,
		IP:       ip,
		Port:     port,
		Response: make(chan *P2PResp, 1),
	}
	P2PServerPid.Tell(chReq)
	select {
	case resp := <-chReq.Response:
		if resp != nil && resp.Error != nil {
			return sdkErr.NewWithError(sdkErr.DNS_PUSH_TRACKER_ERROR, resp.Error)
		}
		return nil
	case <-time.After(time.Duration(consts.ACTOR_P2P_REQ_TIMEOUT) * time.Second):
		return sdkErr.New(sdkErr.NETWORK_TIMEOUT, "[P2PCompleteTorrent] timeout")
	}
}

// P2PTorrentPeers. get torrent peers from tracker
func P2PTorrentPeers(hash []byte, targetDnsAddr string) ([]string, error) {
	chReq := &TorrentPeersReq{
		Address:  targetDnsAddr,
		Hash:     hash,
		Response: make(chan *P2PStringsResp, 1),
	}
	P2PServerPid.Tell(chReq)
	select {
	case resp := <-chReq.Response:
		if resp != nil {
			return resp.Value, resp.Error
		}
		return nil, sdkErr.New(sdkErr.DNS_REQ_TRACKER_ERROR, "response is nil")
	case <-time.After(time.Duration(consts.ACTOR_P2P_REQ_TIMEOUT) * time.Second):
		log.Errorf("[P2PTorrentPeers] timeout")
		return nil, sdkErr.New(sdkErr.NETWORK_TIMEOUT, "[P2PTorrentPeers] timeout")
	}
}

// P2PEndpointRegistry. register endpoint information to tracker
func P2PEndpointRegistry(addr chainCom.Address, ip string, port uint64, targetDnsAddr string) error {
	chReq := &EndpointRegistryReq{
		Address:    targetDnsAddr,
		WalletAddr: addr,
		IP:         ip,
		Port:       port,
		Response:   make(chan *P2PResp, 1),
	}
	P2PServerPid.Tell(chReq)
	select {
	case resp := <-chReq.Response:
		if resp != nil && resp.Error != nil {
			return sdkErr.NewWithError(sdkErr.DNS_PUSH_TRACKER_ERROR, resp.Error)
		}
		return nil
	case <-time.After(time.Duration(consts.ACTOR_P2P_REQ_TIMEOUT) * time.Second):
		return sdkErr.New(sdkErr.NETWORK_TIMEOUT, "[P2PEndpointRegistry] timeout")
	}
}

// P2PGetEndpointAddr. get endpoint hostaddr from tracker
func P2PGetEndpointAddr(addr chainCom.Address, targetDnsAddr string) (string, error) {
	chReq := &GetEndpointReq{
		Address:    targetDnsAddr,
		WalletAddr: addr,
		Response:   make(chan *P2PStringResp, 1),
	}
	P2PServerPid.Tell(chReq)
	select {
	case resp := <-chReq.Response:
		if resp != nil {
			return resp.Value, resp.Error
		}
		return "", sdkErr.New(sdkErr.DNS_REQ_TRACKER_ERROR, "response is nil")
	case <-time.After(time.Duration(consts.ACTOR_P2P_REQ_TIMEOUT) * time.Second):
		log.Errorf("[P2PGetEndpointAddr] get %s with dns %s timeout", addr.ToBase58(), targetDnsAddr)
		return "", sdkErr.New(sdkErr.NETWORK_TIMEOUT, "[P2PGetEndpointAddr] timeout")
	}
}

// P2PIsPeerNetQualityBad. check if peer has a bad network quality
func P2PIsPeerNetQualityBad(address string, netType P2PNetType) (bool, error) {
	chReq := &IsPeerNetQualityBadReq{
		Address:  address,
		NetType:  netType,
		Response: make(chan *P2PBoolResp, 1),
	}
	P2PServerPid.Tell(chReq)
	select {
	case resp := <-chReq.Response:
		if resp != nil {
			return resp.Value, resp.Error
		}
		return false, sdkErr.New(sdkErr.NETWORK_REQ_ERROR, "[P2PIsPeerNetQualityBad] no response")
	case <-time.After(time.Duration(consts.ACTOR_P2P_REQ_TIMEOUT) * time.Second):
		return false, sdkErr.New(sdkErr.NETWORK_REQ_ERROR, "[P2PIsPeerNetQualityBad] timeout")
	}
}

// P2PAppendAddrForHealthCheck. append peer to health check
func P2PAppendAddrForHealthCheck(address string, netType P2PNetType) error {
	chReq := &AppendAddrToHealthCheckReq{
		Address:  address,
		NetType:  netType,
		Response: make(chan *P2PResp, 1),
	}
	P2PServerPid.Tell(chReq)
	select {
	case resp := <-chReq.Response:
		if resp != nil {
			return resp.Error
		}
		return sdkErr.New(sdkErr.NETWORK_REQ_ERROR, "[P2PAppendAddrForHealthCheck] no response")
	case <-time.After(time.Duration(consts.ACTOR_P2P_REQ_TIMEOUT) * time.Second):
		return sdkErr.New(sdkErr.NETWORK_REQ_ERROR, "[P2PAppendAddrForHealthCheck] timeout")
	}
}

// P2PRemoveAddrFormHealthCheck. remote peer from health check
func P2PRemoveAddrFormHealthCheck(address string, netType P2PNetType) error {
	chReq := &RemoveAddrFromHealthCheckReq{
		Address:  address,
		NetType:  netType,
		Response: make(chan *P2PResp, 1),
	}
	P2PServerPid.Tell(chReq)
	select {
	case resp := <-chReq.Response:
		if resp != nil {
			return resp.Error
		}
		return sdkErr.New(sdkErr.NETWORK_REQ_ERROR, "[P2PRemoveAddrFormHealthCheck] no response")
	case <-time.After(time.Duration(consts.ACTOR_P2P_REQ_TIMEOUT) * time.Second):
		return sdkErr.New(sdkErr.NETWORK_REQ_ERROR, "[P2PRemoveAddrFormHealthCheck] timeout")
	}
}
