package dns

import (
	"fmt"
	"net"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	lru "github.com/hashicorp/golang-lru"
	"github.com/saveio/dsp-go-sdk/actor/client"
	"github.com/saveio/dsp-go-sdk/common"
	"github.com/saveio/dsp-go-sdk/core/chain"
	"github.com/saveio/dsp-go-sdk/core/channel"
	dspErr "github.com/saveio/dsp-go-sdk/error"
	"github.com/saveio/dsp-go-sdk/utils"
	chaincom "github.com/saveio/themis/common"
	"github.com/saveio/themis/common/log"
	"github.com/saveio/themis/smartcontract/service/native/dns"
)

type DNSNodeInfo struct {
	WalletAddr string
	HostAddr   string
}

type PublicAddrInfo struct {
	HostAddr  string
	UpdatedAt uint64
}

type trackerResp struct {
	ret  interface{}
	stop bool
	err  error
}

type DNS struct {
	Chain                 *chain.Chain      // core chain
	Channel               *channel.Channel  // core channel
	TrackerProtocol       string            // tracker network protocol
	TrackerUrls           []string          // tracker host addrs
	TrackerScore          *sync.Map         // tracker failed counter
	TrackersFromCfg       []string          // trackers from global config
	AutoBootstrap         bool              // auto bootstrap to DNS and open channel
	DNSNode               *DNSNodeInfo      // current select DNS info
	OnlineDNS             map[string]string // online dns map
	MaxNodeNum            int               // max connected dns number
	DNSWalletAddrsFromCfg []string          // specific dns wallet address from global config
	PublicAddrCache       *lru.ARCCache     // get public addr cache
	channelProtocol       string            // channel protocol
}

type DNSOption interface {
	apply(*DNS)
}

type DNSFunc func(*DNS)

func (f DNSFunc) apply(d *DNS) {
	f(d)
}

func MaxDNSNodeNum(num int) DNSOption {
	return DNSFunc(func(d *DNS) {
		d.MaxNodeNum = num
	})
}

func DNSWalletAddrsFromCfg(walletAddrs []string) DNSOption {
	return DNSFunc(func(d *DNS) {
		d.DNSWalletAddrsFromCfg = walletAddrs
	})
}

func TrackersFromCfg(trs []string) DNSOption {
	return DNSFunc(func(d *DNS) {
		d.TrackersFromCfg = trs
	})
}

func TrackerProtocol(p string) DNSOption {
	return DNSFunc(func(d *DNS) {
		d.TrackerProtocol = p
	})
}

func AutoBootstrap(auto bool) DNSOption {
	return DNSFunc(func(d *DNS) {
		d.AutoBootstrap = auto
	})
}

func ChannelProtocol(protocol string) DNSOption {
	return DNSFunc(func(d *DNS) {
		d.channelProtocol = protocol
	})
}

func NewDNS(chain *chain.Chain, channel *channel.Channel, opts ...DNSOption) *DNS {
	cache, _ := lru.NewARC(common.MAX_PUBLICADDR_CACHE_LEN)
	d := &DNS{
		Chain:           chain,
		TrackerProtocol: "tcp",
		TrackerUrls:     make([]string, 0, common.MAX_TRACKERS_NUM),
		TrackerScore:    new(sync.Map),
		PublicAddrCache: cache,
		OnlineDNS:       make(map[string]string),
		channelProtocol: "tcp",
	}
	for _, opt := range opts {
		opt.apply(d)
	}
	return d
}

// SetupTrackers. setup DNS tracker url list
func (d *DNS) SetupTrackers() error {
	ns, err := d.Chain.GetAllDnsNodes()
	if err != nil {
		return dspErr.NewWithError(dspErr.CHAIN_ERROR, err)
	}
	if len(ns) == 0 {
		return dspErr.New(dspErr.DNS_NO_REGISTER_DNS, "no dns nodes from chain")
	}
	if d.TrackerUrls == nil {
		d.TrackerUrls = make([]string, 0)
	} else {
		d.TrackerUrls = d.TrackerUrls[:0]
	}
	existDNSMap := make(map[string]struct{}, 0)
	for _, trackerUrl := range d.TrackerUrls {
		existDNSMap[trackerUrl] = struct{}{}
	}
	dnsCfgMap := make(map[string]struct{}, 0)
	for _, addr := range d.DNSWalletAddrsFromCfg {
		dnsCfgMap[addr] = struct{}{}
	}

	for _, v := range ns {
		_, ok := dnsCfgMap[v.WalletAddr.ToBase58()]
		if len(dnsCfgMap) > 0 && !ok {
			continue
		}
		log.Debugf("DNS %s :%v, port %v", v.WalletAddr.ToBase58(), string(v.IP), string(v.Port))
		if len(d.TrackerUrls) >= d.MaxNodeNum {
			break
		}
		trackerUrl := fmt.Sprintf("%s://%s:%d", d.TrackerProtocol, v.IP, common.TRACKER_SVR_DEFAULT_PORT)
		_, ok = existDNSMap[trackerUrl]
		if ok {
			continue
		}
		d.TrackerUrls = append(d.TrackerUrls, trackerUrl)
		existDNSMap[trackerUrl] = struct{}{}
	}
	d.TrackerUrls = append(d.TrackerUrls, d.TrackersFromCfg...)
	log.Debugf("d.TrackerUrls %v", d.TrackerUrls)
	return nil
}

// BootstrapDNS. bootstrap max 15 DNS from smart contract
func (d *DNS) BootstrapDNS() {
	log.Debugf("start bootstrapDNS...")
	connetedDNS, err := d.connectDNS(common.MAX_DNS_NUM)
	if err != nil {
		return
	}
	d.OnlineDNS = connetedDNS
	log.Debugf("online dns :%v", d.OnlineDNS)
	if d.AutoBootstrap || len(d.OnlineDNS) == 0 {
		return
	}
	log.Debugf("d.channel %v", d.Channel)
	if d.Channel == nil {
		return
	}
	channels, err := d.Channel.AllChannels()
	if err != nil {
		log.Errorf("get all channel err %s", err)
		return
	}
	if channels == nil || len(channels.Channels) == 0 {
		return
	}
	lastUsedDns, _ := d.Channel.GetLastUsedDNSWalletAddr()
	log.Debugf("last used dns %s  %v", lastUsedDns, d.OnlineDNS)
	for _, channel := range channels.Channels {
		url, ok := d.OnlineDNS[channel.Address]
		if !ok {
			continue
		}
		_, err := d.Channel.OpenChannel(channel.Address, 0)
		if err != nil {
			log.Errorf("open channel failed, err %s", err)
			continue
		}
		_, ok = d.OnlineDNS[lastUsedDns]
		log.Debugf("choose channel %s %t %d %t", channel.Address, ok, len(lastUsedDns), channel.Address != lastUsedDns)
		if ok && len(lastUsedDns) > 0 && channel.Address != lastUsedDns {
			continue
		}
		log.Debugf("setup dns %v", channel.Address)
		d.DNSNode = &DNSNodeInfo{
			WalletAddr: channel.Address,
			HostAddr:   url,
		}
		d.Channel.SelectDNSChannel(channel.Address)
		break
	}
	log.Debugf("use this dns %v", d.DNSNode)
}

// SetupDNSChannels. open channel with DNS automatically. [Deprecated]
func (d *DNS) SetupDNSChannels() error {
	log.Debugf("SetupDNSChannels++++")
	if !d.AutoBootstrap {
		return nil
	}
	ns, err := d.Chain.GetAllDnsNodes()
	if err != nil {
		return err
	}
	if len(ns) == 0 {
		return dspErr.New(dspErr.DNS_NO_REGISTER_DNS, "no dns nodes from chain")
	}
	dnsDeposit := uint64(100)
	lastUsedDNS, _ := d.Channel.GetLastUsedDNSWalletAddr()
	setDNSNodeFunc := func(dnsUrl, walletAddr string) error {
		log.Debugf("set dns node func %s %s", dnsUrl, walletAddr)
		if strings.Index(dnsUrl, "0.0.0.0:0") != -1 {
			return dspErr.New(dspErr.INTERNAL_ERROR, "invalid host addr")
		}
		err = d.Channel.WaitForConnected(walletAddr, time.Duration(common.WAIT_CHANNEL_CONNECT_TIMEOUT)*time.Second)
		if err != nil {
			log.Errorf("wait channel connected err %s %s", walletAddr, err)
			return err
		}
		_, err = d.Channel.OpenChannel(walletAddr, 0)
		if err != nil {
			log.Debugf("open channel err %s", walletAddr)
			return err
		}
		log.Infof("connect to dns node :%s, deposit %d", dnsUrl, dnsDeposit)
		if dnsDeposit > 0 {
			bal, _ := d.Channel.GetTotalDepositBalance(walletAddr)
			log.Debugf("channel to %s current balance %d", walletAddr, bal)
			if bal < dnsDeposit {
				err = d.Channel.SetDeposit(walletAddr, dnsDeposit)
				if err != nil && strings.Index(err.Error(), "totalDeposit must big than contractBalance") == -1 {
					log.Debugf("deposit result %s", err)
					// TODO: withdraw and close channel
					return err
				}
			}
		}
		if len(lastUsedDNS) > 0 && walletAddr != lastUsedDNS {
			return nil
		}
		d.DNSNode = &DNSNodeInfo{
			WalletAddr: walletAddr,
			HostAddr:   dnsUrl,
		}
		d.Channel.SelectDNSChannel(walletAddr)
		log.Debugf("DNSNode wallet: %v, addr: %v", d.DNSNode.WalletAddr, d.DNSNode.HostAddr)
		return nil
	}

	dnsCfgMap := make(map[string]struct{}, 0)
	for _, addr := range d.DNSWalletAddrsFromCfg {
		dnsCfgMap[addr] = struct{}{}
	}

	// first init
	for _, v := range ns {
		_, ok := dnsCfgMap[v.WalletAddr.ToBase58()]
		if len(dnsCfgMap) > 0 && !ok {
			continue
		}
		log.Debugf("DNS %s :%v, port %v", v.WalletAddr.ToBase58(), string(v.IP), string(v.Port))
		dnsUrl, _ := d.GetExternalIP(v.WalletAddr.ToBase58())
		if len(dnsUrl) == 0 {
			dnsUrl = fmt.Sprintf("%s://%s:%s", d.Channel.NetworkProtocol(), v.IP, v.Port)
		}
		err = setDNSNodeFunc(dnsUrl, v.WalletAddr.ToBase58())
		log.Debugf("setDNSNodeFunc err %s", err)
		if err != nil {
			continue
		}
		break
	}
	log.Debugf("set dns func return err%s", err)
	return err
}

// PushToTrackers. Push torrent file hash to trackers
// hash: "..."
// trackerUrls: ["tcp://127.0.0.1:10340"]
// listenAddr: "tcp://127.0.0.1:1234"
func (d *DNS) PushToTrackers(hash string, trackerUrls []string, listenAddr string) error {
	index := strings.Index(listenAddr, "://")
	hostPort := listenAddr
	if index != -1 {
		hostPort = listenAddr[index+3:]
	}
	host, port, err := net.SplitHostPort(hostPort)
	if err != nil {
		return dspErr.NewWithError(dspErr.INTERNAL_ERROR, err)
	}
	netPort, err := strconv.Atoi(port)
	if err != nil {
		return dspErr.NewWithError(dspErr.INTERNAL_ERROR, err)
	}

	log.Debugf("will push file of %s", hash)
	request := func(trackerUrl string, resp chan *trackerResp) {
		err := client.P2pCompleteTorrent([]byte(hash), host, uint64(netPort), trackerUrl)
		if err == nil {
			log.Debugf("pushToTrackers trackerUrl %s hash: %s hostAddr: %s success", trackerUrl, hash, hostPort)
		} else {
			log.Errorf("pushToTrackers trackerUrl %s hash: %s hostAddr: %s err: %s", trackerUrl, hash, hostPort, err)
		}
		resp <- &trackerResp{
			ret: trackerUrl,
			err: err,
		}
	}
	d.requestTrackers(request)
	return nil
}

// GetPeerFromTracker. get peer host addr from trackers
// return: ["tcp://127.0.0.1:1234"]
func (d *DNS) GetPeerFromTracker(hash string, trackerUrls []string) []string {
	selfAddr := client.P2pGetPublicAddr()
	protocol := selfAddr[:strings.Index(selfAddr, "://")]

	request := func(trackerUrl string, resp chan *trackerResp) {
		peers, err := client.P2pTorrentPeers([]byte(hash), trackerUrl)
		if err != nil || len(peers) == 0 {
			resp <- &trackerResp{
				err: fmt.Errorf("peers is empty, err: %s", err),
			}
			return
		}
		peerAddrs := make([]string, 0)
		peerMap := make(map[string]struct{}, 0)
		for _, addr := range peers {
			if !strings.Contains(addr, protocol) {
				addr = fmt.Sprintf("%s://%s", protocol, addr)
			}
			if addr == selfAddr {
				continue
			}
			if _, ok := peerMap[addr]; ok || len(peerMap) > common.MAX_PEERS_NUM_GET_FROM_TRACKER {
				continue
			}
			peerMap[addr] = struct{}{}
			peerAddrs = append(peerAddrs, addr)
		}
		if len(peerAddrs) == 0 {
			resp <- &trackerResp{
				err: fmt.Errorf("peers is empty"),
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
		log.Errorf("convert result to peers failed")
		return nil
	}
	return peerAddrs
}

func (d *DNS) PushFilesToTrackers(files []string) {
	log.Debugf("PushLocalFilesToTrackers %v, files: %d", d.TrackerUrls, len(files))
	if len(d.TrackerUrls) == 0 {
		return
	}
	if len(files) == 0 {
		return
	}
	req := func(files []interface{}, resp chan *utils.RequestResponse) {
		if len(files) != 1 {
			resp <- &utils.RequestResponse{
				Code:  dspErr.INTERNAL_ERROR,
				Error: fmt.Sprintf("invalid arguments %v", files),
			}
			return
		}
		fileHashStr, ok := files[0].(string)
		if !ok {
			resp <- &utils.RequestResponse{
				Code:  dspErr.INTERNAL_ERROR,
				Error: fmt.Sprintf("invalid arguments %v", files),
			}
			return
		}
		err := d.PushToTrackers(fileHashStr, d.TrackerUrls, client.P2pGetPublicAddr())
		if err != nil {
			resp <- &utils.RequestResponse{
				Error: err.Error(),
			}
			return
		} else {
			log.Debugf("push localfiles %s to trackers success", fileHashStr)
		}
		resp <- &utils.RequestResponse{}
	}
	reqArgs := make([][]interface{}, 0, len(files))
	for _, fileHashStr := range files {
		reqArgs = append(reqArgs, []interface{}{fileHashStr})
	}
	utils.CallRequestWithArgs(req, reqArgs)
	log.Debugf("PushLocalFilesToTrackers done%v", d.TrackerUrls)
}

func (d *DNS) RegisterFileUrl(url, link string) (string, error) {
	urlPrefix := common.FILE_URL_CUSTOM_HEADER
	if !strings.HasPrefix(url, urlPrefix) && !strings.HasPrefix(url, common.FILE_URL_CUSTOM_HEADER_PROTOCOL) {
		return "", dspErr.New(dspErr.INTERNAL_ERROR, "url should start with %s", urlPrefix)
	}
	if !utils.ValidateDomainName(url[len(urlPrefix):]) {
		return "", dspErr.New(dspErr.INTERNAL_ERROR, "domain name is invalid")
	}
	tx, err := d.Chain.RegisterUrl(url, dns.CUSTOM_URL, link, link, common.FILE_DNS_TTL)
	if err != nil {
		return "", err
	}
	confirmed, err := d.Chain.PollForTxConfirmed(time.Duration(common.TX_CONFIRM_TIMEOUT)*time.Second, tx)
	if err != nil || !confirmed {
		return "", dspErr.New(dspErr.CHAIN_ERROR, "tx confirm err %s", err)
	}
	return tx, nil
}

func (d *DNS) BindFileUrl(url, link string) (string, error) {
	tx, err := d.Chain.BindUrl(url, link, link, common.FILE_DNS_TTL)
	if err != nil {
		return "", err
	}
	confirmed, err := d.Chain.PollForTxConfirmed(time.Duration(common.TX_CONFIRM_TIMEOUT)*time.Second, tx)
	if err != nil || !confirmed {
		return "", dspErr.New(dspErr.CHAIN_ERROR, "tx confirm err %s", err)
	}
	return tx, nil
}

func (d *DNS) GetLinkFromUrl(url string) string {
	info, err := d.Chain.QueryUrl(url, d.Chain.Address())
	log.Debugf("query url %s %s info %s err %s", url, d.Chain.WalletAddress(), info, err)
	if err != nil || info == nil {
		return ""
	}
	return string(info.Name)
}

func (d *DNS) GetFileHashFromUrl(url string) string {
	link := d.GetLinkFromUrl(url)
	log.Debugf("get link from url %s %s", url, link)
	if len(link) == 0 {
		return ""
	}
	return utils.GetFileHashFromLink(link)
}

func (d *DNS) GetLinkValues(link string) map[string]string {
	return utils.GetFilePropertiesFromLink(link)
}

func (d *DNS) RegNodeEndpoint(walletAddr chaincom.Address, endpointAddr string) error {
	index := strings.Index(endpointAddr, "://")
	hostPort := endpointAddr
	if index != -1 {
		hostPort = endpointAddr[index+3:]
	}
	host, port, err := net.SplitHostPort(hostPort)
	log.Debugf("hostPort %v, host %v", hostPort, host)
	if err != nil {
		return dspErr.NewWithError(dspErr.INTERNAL_ERROR, err)
	}
	if len(d.TrackerUrls) == 0 {
		log.Debugf("set up dns trackers before register channel endpoint")
		err = d.SetupTrackers()
		if err != nil {
			return err
		}
	}
	netPort, err := strconv.Atoi(port)
	if err != nil {
		return dspErr.NewWithError(dspErr.INTERNAL_ERROR, err)
	}

	request := func(trackerUrl string, resp chan *trackerResp) {
		log.Debugf("start RegEndPoint %s hostAddr %v", trackerUrl, hostPort)
		err := client.P2pEndpointRegistry(walletAddr, host, uint64(netPort), trackerUrl)
		log.Debugf("start RegEndPoint end")
		if err != nil {
			log.Errorf("req endpoint failed, err %s", err)
			resp <- &trackerResp{
				err: err,
			}
		} else {
			resp <- &trackerResp{
				ret:  trackerUrl,
				stop: true,
			}
		}
	}
	result := d.requestTrackers(request)
	log.Debugf("reg endpoint final result %v", result)
	if result == nil {
		return dspErr.New(dspErr.DNS_REG_ENDPOINT_ERROR, "register endpoint failed for all dns nodes")
	}
	return nil
}

// GetExternalIP. get external ip of wallet from dns nodes
// return ["tcp://127.0.0.1:1234"], nil
func (d *DNS) GetExternalIP(walletAddr string) (string, error) {
	info, ok := d.PublicAddrCache.Get(walletAddr)
	var oldHostAddr string
	if ok && info != nil {
		addrInfo, ok := info.(*PublicAddrInfo)
		now := utils.GetMilliSecTimestamp()
		if ok && addrInfo != nil && uint64(addrInfo.UpdatedAt+common.MAX_PUBLIC_IP_UPDATE_SECOND*common.MILLISECOND_PER_SECOND) > now && len(addrInfo.HostAddr) > 0 {
			log.Debugf("GetExternalIP %s addr %s from cache", walletAddr, addrInfo.HostAddr)
			return addrInfo.HostAddr, nil
		}
		oldHostAddr = addrInfo.HostAddr
		log.Debugf("wallet: %s, old host ip :%s, now :%d", walletAddr, addrInfo.HostAddr, addrInfo.UpdatedAt)
	}
	address, err := chaincom.AddressFromBase58(walletAddr)
	if err != nil {
		log.Errorf("address from b58 failed %s", err)
		return "", dspErr.NewWithError(dspErr.INVALID_ADDRESS, err)
	}
	if len(d.TrackerUrls) == 0 {
		log.Warn("GetExternalIP no trackers")
	}
	request := func(url string, resp chan *trackerResp) {
		hostAddr, err := client.P2pGetEndpointAddr(address, url)
		log.Debugf("ReqEndPoint hostAddr url: %s, address %s, hostaddr:%s", url, address.ToBase58(), string(hostAddr))
		if err != nil {
			resp <- &trackerResp{
				err: err,
			}
			return
		}
		if len(string(hostAddr)) == 0 || !utils.IsHostAddrValid(string(hostAddr)) {
			resp <- &trackerResp{
				err: fmt.Errorf("invalid hostAddr %s", hostAddr),
			}
			return
		}
		hostAddrStr := utils.FullHostAddr(string(hostAddr), d.channelProtocol)
		log.Debugf("GetExternalIP %s :%v from %s", walletAddr, string(hostAddrStr), url)
		if strings.Index(hostAddrStr, "0.0.0.0:0") != -1 {
			resp <- &trackerResp{
				err: fmt.Errorf("invalid hostAddr %s", hostAddr),
			}
			return
		}
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
		return "", dspErr.New(dspErr.DNS_REQ_TRACKER_ERROR, "request tracker result is nil : %v", result)
	}
	hostAddrStr, ok := result.(string)
	if !ok {
		if len(oldHostAddr) > 0 {
			return oldHostAddr, nil
		}
		log.Errorf("convert result to string failed: %v", result)
		return "", dspErr.New(dspErr.INTERNAL_ERROR, "convert result to string failed: %v", result)
	}
	d.PublicAddrCache.Add(walletAddr, &PublicAddrInfo{
		HostAddr:  hostAddrStr,
		UpdatedAt: utils.GetMilliSecTimestamp(),
	})
	return hostAddrStr, nil
}

// requestTrackers. request trackers parallel
func (d *DNS) requestTrackers(request func(string, chan *trackerResp)) interface{} {
	req := func(trackers []interface{}, resp chan *utils.RequestResponse) bool {
		if len(trackers) != 1 {
			resp <- &utils.RequestResponse{
				Code:  dspErr.INTERNAL_ERROR,
				Error: fmt.Sprintf("invalid arguments %v", trackers),
			}
			return false
		}
		trackerUrl, ok := trackers[0].(string)
		if !ok {
			resp <- &utils.RequestResponse{
				Code:  dspErr.INTERNAL_ERROR,
				Error: fmt.Sprintf("invalid arguments %v", trackers),
			}
			return false
		}

		done := make(chan *trackerResp, 1)
		log.Debugf("start request of tracker: %s", trackerUrl)
		go request(trackerUrl, done)
		for {
			select {
			case ret, ok := <-done:
				if !ok || ret == nil || ret.err != nil {
					resp <- &utils.RequestResponse{
						Result: trackerUrl,
						Code:   dspErr.INTERNAL_ERROR,
						Error:  ret.err.Error(),
					}
					return false
				}
				resp <- &utils.RequestResponse{
					Result: ret.ret,
				}
				return ret.stop
			case <-time.After(time.Duration(common.TRACKER_SERVICE_TIMEOUT) * time.Second):
				log.Errorf("tracker: %s request timeout", trackerUrl)
				resp <- &utils.RequestResponse{
					Result: trackerUrl,
					Code:   dspErr.DNS_TRACKER_TIMEOUT,
					Error:  fmt.Sprintf("tracker: %s request timeout", trackerUrl),
				}
				return false
			}
		}
	}
	reqArgs := make([][]interface{}, 0, len(d.TrackerUrls))
	for _, u := range d.TrackerUrls {
		reqArgs = append(reqArgs, []interface{}{u})
	}
	resp := utils.CallRequestOneWithArgs(req, reqArgs)
	results := make([]interface{}, 0, len(reqArgs))
	for _, r := range resp {
		if r.Code == 0 {
			results = append(results, r.Result)
			continue
		}
		trackerUrl, ok := r.Result.(string)
		if !ok {
			continue
		}
		scoreVal, ok := d.TrackerScore.Load(trackerUrl)
		var score *TrackerScore
		if !ok {
			score = &TrackerScore{url: trackerUrl}
		} else {
			score, _ = scoreVal.(*TrackerScore)
		}
		if r.Code == dspErr.DNS_TRACKER_TIMEOUT {
			score.timeout++
		} else {
			score.failed++
		}
		log.Debugf("update tracker %s score is %s", trackerUrl, score)
		d.TrackerScore.Store(trackerUrl, score)
		d.sortTrackers()
	}
	if len(results) > 0 {
		return results[0]
	}
	return nil
}

func (d *DNS) connectDNS(maxDNSNum uint32) (map[string]string, error) {
	ns, err := d.Chain.GetAllDnsNodes()
	if err != nil {
		return nil, err
	}
	if len(ns) == 0 {
		return nil, dspErr.New(dspErr.DNS_NO_REGISTER_DNS, "no dns nodes from chain")
	}

	// construct connect request function
	connectReq := func(nodes []interface{}, resp chan *utils.RequestResponse) {
		if len(nodes) != 1 {
			resp <- &utils.RequestResponse{
				Code:  dspErr.INTERNAL_ERROR,
				Error: fmt.Sprintf("invalid arguments %v", nodes),
			}
			return
		}
		v, ok := nodes[0].(dns.DNSNodeInfo)
		if !ok {
			resp <- &utils.RequestResponse{
				Code:  dspErr.INTERNAL_ERROR,
				Error: fmt.Sprintf("invalid arguments %v", nodes),
			}
			return
		}
		walletAddr := v.WalletAddr.ToBase58()
		log.Debugf("will connect to %s", walletAddr)
		dnsUrl, _ := d.GetExternalIP(walletAddr)
		if len(dnsUrl) == 0 {
			dnsUrl = fmt.Sprintf("%s://%s:%s", d.Channel.NetworkProtocol(), v.IP, v.Port)
		}
		if strings.Index(dnsUrl, "0.0.0.0:0") != -1 {
			log.Warn("it should not happen")
			resp <- &utils.RequestResponse{
				Code:  dspErr.INTERNAL_ERROR,
				Error: fmt.Sprintf("invalid arguments %v", dnsUrl),
			}
			return
		}
		log.Debugf("connectDNS Loop DNS %s dnsUrl %v", v.WalletAddr.ToBase58(), dnsUrl)
		err = d.Channel.WaitForConnected(walletAddr, time.Duration(common.WAIT_CHANNEL_CONNECT_TIMEOUT)*time.Second)
		if err != nil {
			log.Errorf("wait channel connected err %s %s", walletAddr, err)
			resp <- &utils.RequestResponse{
				Code:  dspErr.INTERNAL_ERROR,
				Error: fmt.Sprintf("wait channel connected err %v", err),
			}
			return
		}
		log.Debugf("wait for connect success: %s", walletAddr)
		resp <- &utils.RequestResponse{
			Result: &DNSNodeInfo{
				WalletAddr: walletAddr,
				HostAddr:   dnsUrl,
			},
		}
	}
	dnsWalletMap := make(map[string]struct{}, 0)
	for _, addr := range d.DNSWalletAddrsFromCfg {
		dnsWalletMap[addr] = struct{}{}
	}
	connectArgs := make([][]interface{}, 0, len(ns))
	for _, v := range ns {
		_, ok := dnsWalletMap[v.WalletAddr.ToBase58()]
		if len(dnsWalletMap) > 0 && !ok {
			continue
		}
		connectArgs = append(connectArgs, []interface{}{v})
	}

	log.Debugf("connect DNS %v", connectArgs)
	// use dispatch model to call request parallel
	connectResps := utils.CallRequestWithArgs(connectReq, connectArgs)

	// handle response
	connectedDNS := make(map[string]string, 0)
	for _, resp := range connectResps {
		if resp == nil || resp.Code != 0 {
			continue
		}
		dnsInfo, ok := resp.Result.(*DNSNodeInfo)
		if !ok {
			continue
		}
		connectedDNS[dnsInfo.WalletAddr] = dnsInfo.HostAddr
	}
	log.Debugf("has dispatch all: %v", connectedDNS)
	return connectedDNS, nil
}

func (d *DNS) sortTrackers() {
	scores := make(TrackerScores, 0)
	for _, u := range d.TrackerUrls {
		scoreVal, ok := d.TrackerScore.Load(u)
		score := &TrackerScore{
			url: u,
		}
		if ok {
			score = scoreVal.(*TrackerScore)
		}
		scores = append(scores, *score)
	}
	sort.Sort(sort.Reverse(scores))
	newTrackers := make([]string, 0)
	for _, s := range scores {
		newTrackers = append(newTrackers, s.url)
	}
	d.TrackerUrls = newTrackers
	log.Debugf("after sort, new track urls %v", d.TrackerUrls)
	// d.SetupTrackers()
}
