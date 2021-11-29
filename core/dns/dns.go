package dns

import (
	"fmt"
	"strings"
	"sync"
	"time"

	lru "github.com/hashicorp/golang-lru"
	"github.com/saveio/dsp-go-sdk/actor/client"
	"github.com/saveio/dsp-go-sdk/consts"
	"github.com/saveio/dsp-go-sdk/core/chain"
	"github.com/saveio/dsp-go-sdk/core/channel"
	sdkErr "github.com/saveio/dsp-go-sdk/error"
	tAddr "github.com/saveio/dsp-go-sdk/types/address"
	"github.com/saveio/dsp-go-sdk/types/state"
	uAddr "github.com/saveio/dsp-go-sdk/utils/addr"
	"github.com/saveio/dsp-go-sdk/utils/async"
	"github.com/saveio/dsp-go-sdk/utils/format"
	uTime "github.com/saveio/dsp-go-sdk/utils/time"
	chainCom "github.com/saveio/themis/common"
	"github.com/saveio/themis/common/log"
	"github.com/saveio/themis/smartcontract/service/native/dns"
)

type trackerResp struct {
	ret  interface{}
	stop bool
	err  error
}

type DNS struct {
	chain            *chain.Chain       // core chain
	channel          *channel.Channel   // core channel
	trackerProtocol  string             // tracker network protocol
	trackerUrls      []string           // tracker host addrs
	trackerScore     *sync.Map          // tracker failed counter
	reservedTrackers []string           // trackers from global config
	autoBootstrap    bool               // auto bootstrap to DNS and open channel
	dnsAddrInfo      *tAddr.AddressInfo // current select DNS info
	onlineDNS        map[string]string  // online dns map
	maxNodeNum       int                // max connected dns number
	reservedDNS      []string           // specific dns wallet address from global config
	publicAddrCache  *lru.ARCCache      // get public addr cache
	channelProtocol  string             // channel protocol
	state            *state.SyncState   // module state
	lastUsedDNS      string             // last used dns
}

// NewDNS. init a dns service
func NewDNS(chain *chain.Chain, channel *channel.Channel, opts ...DNSOption) *DNS {
	cache, _ := lru.NewARC(consts.MAX_PUBLICADDR_CACHE_LEN)
	d := &DNS{
		chain:           chain,
		channel:         channel,
		trackerProtocol: "tcp",
		trackerUrls:     make([]string, 0, consts.MAX_TRACKERS_NUM),
		trackerScore:    new(sync.Map),
		publicAddrCache: cache,
		onlineDNS:       make(map[string]string),
		channelProtocol: "tcp",
		state:           state.NewSyncState(),
	}
	for _, opt := range opts {
		opt.apply(d)
	}
	return d
}

// State. get current dns state
func (d *DNS) State() state.ModuleState {
	return d.state.Get()
}

func (d *DNS) HasDNS() bool {
	return d.dnsAddrInfo != nil
}

func (d *DNS) IsDNS(walletAddr string) bool {
	if !d.HasDNS() {
		return false
	}
	return d.dnsAddrInfo.WalletAddr == walletAddr
}

func (d *DNS) CurrentDNSWallet() string {
	if !d.HasDNS() {
		return ""
	}
	return d.dnsAddrInfo.WalletAddr
}

func (d *DNS) CurrentDNSHostAddr() string {
	if !d.HasDNS() {
		return ""
	}
	return d.dnsAddrInfo.HostAddr
}

// Discovery. get dns list from chain, connect them.
func (d *DNS) Discovery() {
	if d.channel == nil {
		log.Debugf("stop discovery dns because channel is nil")
		return
	}
	if _, ok := d.state.GetOrStore(state.ModuleStateStarting); ok {
		return
	}
	log.Debugf("start discovery dns...")
	defer func() {
		d.state.Set(state.ModuleStateStarted)
		d.state.Set(state.ModuleStateActive)
	}()
	// TODO: connect dns async
	connetedDNS, err := d.connectDNS(consts.MAX_DNS_NUM)
	if err != nil {
		return
	}
	d.onlineDNS = connetedDNS
	log.Debugf("discovery online dns %v", d.onlineDNS)
	if d.autoBootstrap || len(d.onlineDNS) == 0 {
		return
	}
	channels, err := d.channel.AllChannels()
	if err != nil {
		log.Errorf("get all channel err %s", err)
		return
	}
	if channels == nil || len(channels.Channels) == 0 {
		return
	}
	addr, _ := d.channel.GetLastUsedDNSWalletAddr()
	LastUsedDNS(addr).apply(d)
	log.Debugf("last used dns %s", d.lastUsedDNS)
	for _, channel := range channels.Channels {
		url, ok := d.onlineDNS[channel.Address]
		if !ok {
			continue
		}
		err = d.channel.CanTransfer(channel.Address, 0)
		if err != nil {
			log.Errorf("can't transfer by channel, err %s", err)
			continue
		}
		_, ok = d.onlineDNS[d.lastUsedDNS]
		log.Debugf("choose channel %s %t %d %t",
			channel.Address, ok, len(d.lastUsedDNS), channel.Address != d.lastUsedDNS)
		if ok && len(d.lastUsedDNS) > 0 && channel.Address != d.lastUsedDNS {
			continue
		}
		log.Debugf("setup dns %v", channel.Address)
		d.dnsAddrInfo = &tAddr.AddressInfo{
			WalletAddr: channel.Address,
			HostAddr:   url,
		}
		d.channel.SelectDNSChannel(channel.Address)
		break
	}
	log.Debugf("use this dns %v", d.dnsAddrInfo)
}

func (d *DNS) StartDNSHealthCheckService() {
	for walletAddr, _ := range d.onlineDNS {
		log.Infof("append walletAddr %s to health check", walletAddr)
		client.P2PAppendAddrForHealthCheck(walletAddr, client.P2PNetTypeChannel)
	}
}

func (d *DNS) ResetDNSNode() {
	d.dnsAddrInfo = nil
}

func (d *DNS) GetAllOnlineDNS() map[string]string {
	return d.onlineDNS
}

func (d *DNS) GetOnlineDNSHostAddr(walletAddr string) string {
	addr, _ := d.onlineDNS[walletAddr]
	return addr
}

func (d *DNS) IsDnsOnline(partnerAddr string) bool {
	url, ok := d.onlineDNS[partnerAddr]
	if !ok || url == "" {
		log.Debugf("OnlineDNS %v", d.onlineDNS)
		return false
	}
	return true
}

func (d *DNS) UpdateDNS(walletAddr, hostAddr string, use bool) {
	log.Debugf("reachable %t host %s", d.channel.ChannelReachale(walletAddr), hostAddr)
	if d.channel.ChannelReachale(walletAddr) && len(hostAddr) > 0 {
		d.onlineDNS[walletAddr] = hostAddr
	}
	if !use && (d != nil && d.dnsAddrInfo != nil) {
		return
	}
	d.dnsAddrInfo = &tAddr.AddressInfo{
		WalletAddr: walletAddr,
		HostAddr:   hostAddr,
	}

	if err := d.channel.SelectDNSChannel(walletAddr); err != nil {
		log.Errorf("update selecting new dns channel err %s", err)
	}
}

// GetPeerHostAddrsOfFile. get peer host addr from trackers for whom store the file
// return: ["tcp://127.0.0.1:1234"]
func (d *DNS) GetPeerHostAddrsOfFile(hash string) []string {
	selfAddr := client.P2PGetPublicAddr()
	protocol := uAddr.GetProtocolOfHostAddr(selfAddr)

	request := func(trackerUrl string, resp chan *trackerResp) {
		peers, err := client.P2PTorrentPeers([]byte(hash), trackerUrl)
		if err != nil || len(peers) == 0 {
			resp <- &trackerResp{
				err: fmt.Errorf("can't find any peers of file %s, err: %s", hash, err),
			}
			return
		}
		peerAddrs := make([]string, 0)
		peerMap := make(map[string]struct{}, 0)
		for _, addr := range peers {
			if !strings.Contains(addr, protocol) {
				addr = uAddr.ConvertToFullHostAddr(protocol, addr)
			}
			if addr == selfAddr {
				continue
			}
			if _, ok := peerMap[addr]; ok || len(peerMap) > consts.MAX_PEERS_NUM_GET_FROM_TRACKER {
				continue
			}
			peerMap[addr] = struct{}{}
			peerAddrs = append(peerAddrs, addr)
		}
		if len(peerAddrs) == 0 {
			resp <- &trackerResp{
				err: fmt.Errorf("can't find any peers of file %s", hash),
			}
			return
		}
		resp <- &trackerResp{
			ret:  peerAddrs,
			stop: true,
		}
	}
	result := d.requestTrackers(request)
	if result == nil {
		return nil
	}
	peerAddrs, ok := result.([]string)
	if !ok {
		log.Errorf("convert result %v to peers failed", result)
		return nil
	}
	return peerAddrs
}

// RegNodeEndpoint. register wallet <=> host address to DNS
func (d *DNS) RegNodeEndpoint(walletAddr chainCom.Address, endpointAddr string) error {
	host, port, err := uAddr.ParseHostAddr(endpointAddr)
	log.Debugf("register host %s of wallet %s to tracker", endpointAddr, walletAddr.ToBase58())
	if err != nil {
		return sdkErr.NewWithError(sdkErr.INVALID_PARAMS, err)
	}
	if len(d.trackerUrls) == 0 {
		log.Debugf("set up dns trackers before register channel endpoint")
		if err := d.SetupTrackers(); err != nil {
			return err
		}
	}
	request := func(trackerUrl string, resp chan *trackerResp) {
		log.Debugf("start register endpoint of %s to tracker %v", endpointAddr, trackerUrl)
		err := client.P2PEndpointRegistry(walletAddr, host, uint64(port), trackerUrl)
		if err != nil {
			log.Errorf("req endpoint to tracker failed, err %s", err)
			resp <- &trackerResp{
				err: err,
			}
		} else {
			log.Debugf("register endpoint to tracker success")
			resp <- &trackerResp{
				ret:  trackerUrl,
				stop: true,
			}
		}
	}
	result := d.requestTrackers(request)
	log.Debugf("reg endpoint result %v", result)
	if result == nil {
		return sdkErr.New(sdkErr.DNS_REG_ENDPOINT_ERROR, "register endpoint failed for all dns nodes")
	}
	return nil
}

// GetPublicAddr. get public addr of wallet from dns nodes
// return ["tcp://127.0.0.1:1234"], nil
func (d *DNS) GetPublicAddr(walletAddr string) (string, error) {
	info, ok := d.publicAddrCache.Get(walletAddr)
	var oldHostAddr string
	if ok && info != nil {
		addrInfo, ok := info.(*tAddr.AddressInfo)
		if ok && addrInfo != nil && uTime.MilliSecBefore(uTime.NowMillisecond(), addrInfo.UpdatedAt,
			consts.MAX_PUBLIC_IP_UPDATE_SECOND*consts.MILLISECOND_PER_SECOND) && len(addrInfo.HostAddr) > 0 {
			log.Debugf("get public host from cache, addr of wallet %s is %s", walletAddr, addrInfo.HostAddr)
			return addrInfo.HostAddr, nil
		}
		if addrInfo != nil {
			oldHostAddr = addrInfo.HostAddr
			log.Debugf("wallet %s, host ip %s from cache is expired. start query again",
				walletAddr, addrInfo.HostAddr, addrInfo.UpdatedAt)
		}
	}
	address, err := chainCom.AddressFromBase58(walletAddr)
	if err != nil {
		log.Errorf("parse a base58 addr %s failed %s", walletAddr, err)
		return "", sdkErr.NewWithError(sdkErr.INVALID_ADDRESS, err)
	}
	if len(d.trackerUrls) == 0 {
		log.Warn("no any trackers exist")
		if err := d.SetupTrackers(); err != nil {
			return "", sdkErr.NewWithError(sdkErr.NO_CONNECTED_DNS, err)
		}
	}
	request := func(url string, resp chan *trackerResp) {
		hostAddr, err := client.P2PGetEndpointAddr(address, url)
		log.Debugf("get hostAddr %s of wallet address %s from tracker %s", string(hostAddr), address.ToBase58(), url)
		if err != nil {
			resp <- &trackerResp{
				err: err,
			}
			return
		}
		if len(string(hostAddr)) == 0 || !uAddr.IsHostAddrValid(string(hostAddr)) {
			resp <- &trackerResp{
				err: fmt.Errorf("invalid hostAddr %s", hostAddr),
			}
			return
		}
		hostAddrStr := uAddr.ConvertToFullHostAddr(d.channelProtocol, string(hostAddr))
		log.Debugf("get hostAddr %s of wallet address %s from tracker %s", string(hostAddr), address.ToBase58(), url)
		resp <- &trackerResp{
			ret:  hostAddrStr,
			stop: true,
		}
	}
	result := d.requestTrackers(request)
	log.Debugf("get external ip wallet %s, result %v", walletAddr, result)
	if result == nil {
		if len(oldHostAddr) > 0 {
			return oldHostAddr, nil
		}
		hostAddrFromChain, err := d.GetDNSHostAddrFromChain(walletAddr)
		if err != nil {
			return "", sdkErr.New(sdkErr.DNS_REQ_TRACKER_ERROR, "request tracker result is nil : %v", result)
		}
		log.Debugf("get host addr %s of %s from chain", walletAddr, hostAddrFromChain)
		result = hostAddrFromChain
	}
	hostAddrStr, ok := result.(string)
	if !ok {
		if len(oldHostAddr) > 0 {
			return oldHostAddr, nil
		}
		log.Errorf("convert result to string failed: %v", result)
		return "", sdkErr.New(sdkErr.INTERNAL_ERROR, "convert result to string failed: %v", result)
	}
	d.publicAddrCache.Add(walletAddr, &tAddr.AddressInfo{
		HostAddr:  hostAddrStr,
		UpdatedAt: uTime.NowMillisecond(),
	})
	return hostAddrStr, nil
}

// GetDNSHostAddrFromChain. get dns host addr from chain
func (d *DNS) GetDNSHostAddrFromChain(walletAddr string) (string, error) {
	walletAddress, err := chainCom.AddressFromBase58(walletAddr)
	if err != nil {
		return "", err
	}
	info, err := d.chain.GetDnsNodeByAddr(walletAddress)
	if err != nil {
		return "", err
	}
	return uAddr.ConvertToFullHostAddr(d.channelProtocol, fmt.Sprintf("%s:%s", info.IP, info.Port)), nil
}

// connectDNS. connect to dns with max DNS num
func (d *DNS) connectDNS(maxDNSNum uint32) (map[string]string, error) {
	ns, err := d.chain.GetAllDnsNodes()
	if err != nil {
		return nil, err
	}
	if len(ns) == 0 {
		return nil, sdkErr.New(sdkErr.DNS_NO_REGISTER_DNS, "no dns nodes from chain")
	}

	// construct connect request function
	connectReq := func(nodes []interface{}, resp chan *async.RequestResponse) {
		if len(nodes) != 1 {
			resp <- &async.RequestResponse{
				Error: sdkErr.New(sdkErr.INTERNAL_ERROR, "invalid arguments %v", nodes),
			}
			return
		}
		v, ok := nodes[0].(dns.DNSNodeInfo)
		if !ok {
			resp <- &async.RequestResponse{
				Error: sdkErr.New(sdkErr.INTERNAL_ERROR, "invalid arguments %v", nodes),
			}
			return
		}
		walletAddr := v.WalletAddr.ToBase58()
		dnsUrl, _ := d.GetPublicAddr(walletAddr)
		log.Debugf("connect to %s with dns url %s", walletAddr, dnsUrl)
		if len(dnsUrl) == 0 {
			dnsUrl = uAddr.ConvertToFullHostAddrWithPort(d.channelProtocol, string(v.IP), string(v.Port))
		}
		if !uAddr.IsHostAddrValid(dnsUrl) {
			log.Errorf("dns url %s is invalid", dnsUrl)
			resp <- &async.RequestResponse{
				Error: sdkErr.New(sdkErr.INTERNAL_ERROR, "invalid arguments %v", dnsUrl),
			}
			return
		}
		err = d.channel.WaitForConnected(walletAddr, time.Duration(consts.WAIT_CHANNEL_CONNECT_TIMEOUT)*time.Second)
		if err != nil {
			log.Errorf("wait channel %s connected err %s", walletAddr, err)
			resp <- &async.RequestResponse{
				Error: sdkErr.New(sdkErr.INTERNAL_ERROR, "wait channel connected err %v", err),
			}
			return
		}
		log.Debugf("wait for connect success: %s", walletAddr)
		resp <- &async.RequestResponse{
			Result: &tAddr.AddressInfo{
				WalletAddr: walletAddr,
				HostAddr:   dnsUrl,
			},
		}
	}
	dnsWalletMap := format.StringsToMap(d.reservedDNS)
	connectArgs := make([][]interface{}, 0, len(ns))
	for _, v := range ns {
		_, ok := dnsWalletMap[v.WalletAddr.ToBase58()]
		if len(dnsWalletMap) > 0 && !ok {
			continue
		}
		connectArgs = append(connectArgs, []interface{}{v})
	}

	log.Debugf("connect DNS with args %v", connectArgs)
	// use dispatch model to call request parallel
	connectResps := async.RequestWithArgs(connectReq, connectArgs)
	// handle response
	connectedDNS := make(map[string]string, 0)
	for _, resp := range connectResps {
		if resp == nil || (resp.Error != nil && resp.Error.Code != sdkErr.SUCCESS) {
			continue
		}
		dnsInfo, ok := resp.Result.(*tAddr.AddressInfo)
		if !ok {
			continue
		}
		connectedDNS[dnsInfo.WalletAddr] = dnsInfo.HostAddr
	}
	log.Debugf("connected dns result %v", connectedDNS)
	return connectedDNS, nil
}
