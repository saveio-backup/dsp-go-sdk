package dsp

import (
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	lru "github.com/hashicorp/golang-lru"
	"github.com/saveio/dsp-go-sdk/actor/client"
	"github.com/saveio/dsp-go-sdk/common"
	serr "github.com/saveio/dsp-go-sdk/error"
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

type DNS struct {
	TrackerUrls      []string
	TrackerFailedCnt *sync.Map
	DNSNode          *DNSNodeInfo
	OnlineDNS        map[string]string
	PublicAddrCache  *lru.ARCCache
}

func NewDNS() *DNS {
	cache, _ := lru.NewARC(common.MAX_PUBLICADDR_CACHE_LEN)
	return &DNS{
		TrackerUrls:      make([]string, 0, common.MAX_TRACKERS_NUM),
		TrackerFailedCnt: new(sync.Map),
		PublicAddrCache:  cache,
		OnlineDNS:        make(map[string]string),
	}
}

// SetupDNSTrackers. setup DNS tracker url list
func (this *Dsp) SetupDNSTrackers() error {
	ns, err := this.Chain.Native.Dns.GetAllDnsNodes()
	if err != nil {
		return err
	}
	if len(ns) == 0 {
		return errors.New("no dns nodes")
	}
	maxTrackerNum := 1
	if this.Config.DnsNodeMaxNum > 1 {
		maxTrackerNum = this.Config.DnsNodeMaxNum
	}
	if this.DNS.TrackerUrls == nil {
		this.DNS.TrackerUrls = make([]string, 0)
	} else {
		this.DNS.TrackerUrls = this.DNS.TrackerUrls[:0]
	}
	existDNSMap := make(map[string]struct{}, 0)
	for _, trackerUrl := range this.DNS.TrackerUrls {
		existDNSMap[trackerUrl] = struct{}{}
	}
	dnsCfgMap := make(map[string]struct{}, 0)
	for _, addr := range this.Config.DNSWalletAddrs {
		dnsCfgMap[addr] = struct{}{}
	}

	for _, v := range ns {
		_, ok := dnsCfgMap[v.WalletAddr.ToBase58()]
		if len(dnsCfgMap) > 0 && !ok {
			continue
		}
		log.Debugf("DNS %s :%v, port %v", v.WalletAddr.ToBase58(), string(v.IP), string(v.Port))
		if len(this.DNS.TrackerUrls) >= maxTrackerNum {
			break
		}
		trackerUrl := fmt.Sprintf("%s://%s:%d", this.Config.TrackerProtocol, v.IP, common.TRACKER_SVR_DEFAULT_PORT)
		_, ok = existDNSMap[trackerUrl]
		if ok {
			continue
		}
		this.DNS.TrackerUrls = append(this.DNS.TrackerUrls, trackerUrl)
	}
	this.DNS.TrackerUrls = append(this.DNS.TrackerUrls, this.Config.Trackers...)
	log.Debugf("this.DNS.TrackerUrls %v", this.DNS.TrackerUrls)
	return nil
}

// BootstrapDNS. bootstrap max 15 DNS from smart contract
func (this *Dsp) BootstrapDNS() {
	log.Debugf("start bootstrapDNS...")
	connetedDNS, err := this.connectDNS(common.MAX_DNS_NUM)
	if err != nil {
		return
	}
	this.DNS.OnlineDNS = connetedDNS
	log.Debugf("online dns :%v", this.DNS.OnlineDNS)
	if this.Config.AutoSetupDNSEnable || len(this.DNS.OnlineDNS) == 0 {
		return
	}
	channels, err := this.Channel.AllChannels()
	if err != nil {
		log.Errorf("get all channel err %s", err)
		return
	}
	if channels == nil || len(channels.Channels) == 0 {
		return
	}
	log.Debugf("will set online dns")
	for _, channel := range channels.Channels {
		url, ok := this.DNS.OnlineDNS[channel.Address]
		if !ok {
			continue
		}
		_, err := this.Channel.OpenChannel(channel.Address, 0)
		if err != nil {
			log.Errorf("open channel failed, err %s", err)
			continue
		}
		this.DNS.DNSNode = &DNSNodeInfo{
			WalletAddr: channel.Address,
			HostAddr:   url,
		}
		break
	}
	log.Debugf("set online DNSDNS %v", this.DNS.DNSNode)
}

// SetupDNSChannels. open channel with DNS automatically
func (this *Dsp) SetupDNSChannels() error {
	log.Debugf("SetupDNSChannels++++")
	if !this.Config.AutoSetupDNSEnable {
		return nil
	}
	ns, err := this.Chain.Native.Dns.GetAllDnsNodes()
	if err != nil {
		return err
	}
	if len(ns) == 0 {
		return errors.New("no dns nodes")
	}
	setDNSNodeFunc := func(dnsUrl, walletAddr string) error {
		log.Debugf("set dns node func %s %s", dnsUrl, walletAddr)
		if strings.Index(dnsUrl, "0.0.0.0:0") != -1 {
			return errors.New("invalid host addr")
		}
		err = this.Channel.WaitForConnected(walletAddr, time.Duration(common.WAIT_CHANNEL_CONNECT_TIMEOUT)*time.Second)
		if err != nil {
			log.Errorf("wait channel connected err %s %s", walletAddr, err)
			return err
		}
		_, err = this.Channel.OpenChannel(walletAddr, 0)
		if err != nil {
			log.Debugf("open channel err %s", walletAddr)
			return err
		}
		log.Infof("connect to dns node :%s, deposit %d", dnsUrl, this.Config.DnsChannelDeposit)
		if this.Config.DnsChannelDeposit > 0 {
			bal, _ := this.Channel.GetTotalDepositBalance(walletAddr)
			log.Debugf("channel to %s current balance %d", walletAddr, bal)
			if bal < this.Config.DnsChannelDeposit {
				err = this.Channel.SetDeposit(walletAddr, this.Config.DnsChannelDeposit)
				if err != nil && strings.Index(err.Error(), "totalDeposit must big than contractBalance") == -1 {
					log.Debugf("deposit result %s", err)
					// TODO: withdraw and close channel
					return err
				}
			}
		}
		this.DNS.DNSNode = &DNSNodeInfo{
			WalletAddr: walletAddr,
			HostAddr:   dnsUrl,
		}
		log.Debugf("DNSNode wallet: %v, addr: %v", this.DNS.DNSNode.WalletAddr, this.DNS.DNSNode.HostAddr)
		return nil
	}

	dnsCfgMap := make(map[string]struct{}, 0)
	for _, addr := range this.Config.DNSWalletAddrs {
		dnsCfgMap[addr] = struct{}{}
	}

	// first init
	for _, v := range ns {
		_, ok := dnsCfgMap[v.WalletAddr.ToBase58()]
		if len(dnsCfgMap) > 0 && !ok {
			continue
		}
		log.Debugf("DNS %s :%v, port %v", v.WalletAddr.ToBase58(), string(v.IP), string(v.Port))
		dnsUrl, _ := this.GetExternalIP(v.WalletAddr.ToBase58())
		if len(dnsUrl) == 0 {
			dnsUrl = fmt.Sprintf("%s://%s:%s", this.Config.ChannelProtocol, v.IP, v.Port)
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
func (this *Dsp) PushToTrackers(hash string, trackerUrls []string, listenAddr string) error {
	index := strings.Index(listenAddr, "://")
	hostPort := listenAddr
	if index != -1 {
		hostPort = listenAddr[index+3:]
	}
	host, port, err := net.SplitHostPort(hostPort)
	if err != nil {
		return err
	}
	netPort, err := strconv.Atoi(port)
	if err != nil {
		return err
	}

	request := func(trackerUrl string, resp chan *trackerResp) {
		err := client.P2pCompleteTorrent([]byte(hash), host, uint64(netPort), trackerUrl)
		if err != nil {
			log.Debugf("pushToTrackers trackerUrl %s hash: %s hostAddr: %s success", trackerUrl, hash, hostPort)
		} else {
			log.Errorf("pushToTrackers trackerUrl %s hash: %s hostAddr: %s err: %s", trackerUrl, hash, hostPort, err)
		}
		resp <- &trackerResp{
			ret: trackerUrl,
			err: err,
		}
	}
	this.requestTrackers(request)
	return nil
}

// GetPeerFromTracker. get peer host addr from trackers
// return: ["tcp://127.0.0.1:1234"]
func (this *Dsp) GetPeerFromTracker(hash string, trackerUrls []string) []string {
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
	result := this.requestTrackers(request)
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

func (this *Dsp) StartSeedService() {
	log.Debugf("start seed service")
	tick := time.NewTicker(time.Duration(this.Config.SeedInterval) * time.Second)
	for {
		select {
		case <-tick.C:
			if this.stop {
				log.Debugf("stop seed service")
				tick.Stop()
				return
			}
			this.PushLocalFilesToTrackers()
		}
	}
}

func (this *Dsp) PushLocalFilesToTrackers() {
	log.Debugf("PushLocalFilesToTrackers %v", this.DNS.TrackerUrls)
	if len(this.DNS.TrackerUrls) == 0 {
		return
	}
	_, files, err := this.taskMgr.AllDownloadFiles()
	log.Debugf("all downloaded files %v, err %s", len(files), err)
	if len(files) == 0 {
		return
	}
	req := func(files []interface{}, resp chan *utils.RequestResponse) {
		if len(files) != 1 {
			resp <- &utils.RequestResponse{
				Code:  serr.INTERNAL_ERROR,
				Error: fmt.Sprintf("invalid arguments %v", files),
			}
			return
		}
		fileHashStr, ok := files[0].(string)
		if !ok {
			resp <- &utils.RequestResponse{
				Code:  serr.INTERNAL_ERROR,
				Error: fmt.Sprintf("invalid arguments %v", files),
			}
			return
		}
		err = this.PushToTrackers(fileHashStr, this.DNS.TrackerUrls, client.P2pGetPublicAddr())
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
}

func (this *Dsp) RegisterFileUrl(url, link string) (string, error) {
	urlPrefix := common.FILE_URL_CUSTOM_HEADER
	if !strings.HasPrefix(url, urlPrefix) && !strings.HasPrefix(url, common.FILE_URL_CUSTOM_HEADER_PROTOCOL) {
		return "", fmt.Errorf("url should start with %s", urlPrefix)
	}
	if !utils.ValidateDomainName(url[len(urlPrefix):]) {
		return "", errors.New("domain name is invalid")
	}
	hash, err := this.Chain.Native.Dns.RegisterUrl(url, dns.CUSTOM_URL, link, link, common.FILE_DNS_TTL)
	if err != nil {
		return "", err
	}
	confirmed, err := this.Chain.PollForTxConfirmed(time.Duration(common.TX_CONFIRM_TIMEOUT)*time.Second, hash[:])
	if err != nil || !confirmed {
		return "", errors.New("tx confirm err")
	}
	return hex.EncodeToString(chaincom.ToArrayReverse(hash[:])), nil
}

func (this *Dsp) BindFileUrl(url, link string) (string, error) {
	hash, err := this.Chain.Native.Dns.Binding(url, link, link, common.FILE_DNS_TTL)
	if err != nil {
		return "", err
	}
	confirmed, err := this.Chain.PollForTxConfirmed(time.Duration(common.TX_CONFIRM_TIMEOUT)*time.Second, hash[:])
	if err != nil || !confirmed {
		return "", errors.New("tx confirm err")
	}
	return hex.EncodeToString(chaincom.ToArrayReverse(hash[:])), nil
}

func (this *Dsp) GetLinkFromUrl(url string) string {
	info, err := this.Chain.Native.Dns.QueryUrl(url, this.Chain.Native.Dns.DefAcc.Address)
	log.Debugf("query url %s %s info %s err %s", url, this.Chain.Native.Dns.DefAcc.Address.ToBase58(), info, err)
	if err != nil || info == nil {
		return ""
	}
	return string(info.Name)
}

func (this *Dsp) GetFileHashFromUrl(url string) string {
	link := this.GetLinkFromUrl(url)
	log.Debugf("get link from url %s %s", url, link)
	if len(link) == 0 {
		return ""
	}
	return utils.GetFileHashFromLink(link)
}

func (this *Dsp) GetLinkValues(link string) map[string]string {
	return utils.GetFilePropertiesFromLink(link)
}

func (this *Dsp) RegNodeEndpoint(walletAddr chaincom.Address, endpointAddr string) error {
	index := strings.Index(endpointAddr, "://")
	hostPort := endpointAddr
	if index != -1 {
		hostPort = endpointAddr[index+3:]
	}
	host, port, err := net.SplitHostPort(hostPort)
	log.Debugf("hostPort %v, host %v", hostPort, host)
	if err != nil {
		return err
	}
	if len(this.DNS.TrackerUrls) == 0 {
		log.Debugf("set up dns trackers before register channel endpoint")
		err = this.SetupDNSTrackers()
		if err != nil {
			return err
		}
	}
	netPort, err := strconv.Atoi(port)
	if err != nil {
		return err
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
	result := this.requestTrackers(request)
	log.Debugf("reg endpoint final result %v", result)
	if result == nil {
		return errors.New("register endpoint failed for all dns nodes")
	}
	return nil
}

// GetExternalIP. get external ip of wallet from dns nodes
// return ["tcp://127.0.0.1:1234"], nil
func (this *Dsp) GetExternalIP(walletAddr string) (string, error) {
	info, ok := this.DNS.PublicAddrCache.Get(walletAddr)
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
		return "", err
	}
	if len(this.DNS.TrackerUrls) == 0 {
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
		hostAddrStr := utils.FullHostAddr(string(hostAddr), this.Config.ChannelProtocol)
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
	result := this.requestTrackers(request)
	log.Debugf("get external ip wallet %s, result %v", walletAddr, result)
	if result == nil {
		if len(oldHostAddr) > 0 {
			return oldHostAddr, nil
		}
		return "", fmt.Errorf("request tracker result is nil : %v", result)
	}
	hostAddrStr, ok := result.(string)
	if !ok {
		if len(oldHostAddr) > 0 {
			return oldHostAddr, nil
		}
		log.Errorf("convert result to string failed: %v", result)
		return "", fmt.Errorf("convert result to string failed: %v", result)
	}
	this.DNS.PublicAddrCache.Add(walletAddr, &PublicAddrInfo{
		HostAddr:  hostAddrStr,
		UpdatedAt: utils.GetMilliSecTimestamp(),
	})
	return hostAddrStr, nil
}

type trackerResp struct {
	ret  interface{}
	stop bool
	err  error
}

// requestTrackers. request trackers parallel
func (this *Dsp) requestTrackers(request func(string, chan *trackerResp)) interface{} {
	req := func(trackers []interface{}, resp chan *utils.RequestResponse) bool {
		if len(trackers) != 1 {
			resp <- &utils.RequestResponse{
				Code:  serr.INTERNAL_ERROR,
				Error: fmt.Sprintf("invalid arguments %v", trackers),
			}
			return false
		}
		trackerUrl, ok := trackers[0].(string)
		if !ok {
			resp <- &utils.RequestResponse{
				Code:  serr.INTERNAL_ERROR,
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
						Code:   serr.INTERNAL_ERROR,
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
					Code:   serr.INTERNAL_ERROR,
					Error:  fmt.Sprintf("tracker: %s request timeout", trackerUrl),
				}
				return false
			}
		}
	}
	reqArgs := make([][]interface{}, 0, len(this.DNS.TrackerUrls))
	for _, u := range this.DNS.TrackerUrls {
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
		errCnt, ok := this.DNS.TrackerFailedCnt.Load(trackerUrl)
		if !ok {
			this.DNS.TrackerFailedCnt.Store(trackerUrl, 1)
		} else {
			errCntVal, _ := errCnt.(uint64)
			this.DNS.TrackerFailedCnt.Store(trackerUrl, errCntVal+1)
		}
		this.removeLowQoSTracker()
	}
	if len(results) > 0 {
		return results[0]
	}
	return nil
}

func (this *Dsp) connectDNS(maxDNSNum uint32) (map[string]string, error) {
	ns, err := this.Chain.Native.Dns.GetAllDnsNodes()
	if err != nil {
		return nil, err
	}
	if len(ns) == 0 {
		return nil, errors.New("no dns nodes")
	}

	// construct connect request function
	connectReq := func(nodes []interface{}, resp chan *utils.RequestResponse) {
		if len(nodes) != 1 {
			resp <- &utils.RequestResponse{
				Code:  serr.INTERNAL_ERROR,
				Error: fmt.Sprintf("invalid arguments %v", nodes),
			}
			return
		}
		v, ok := nodes[0].(dns.DNSNodeInfo)
		if !ok {
			resp <- &utils.RequestResponse{
				Code:  serr.INTERNAL_ERROR,
				Error: fmt.Sprintf("invalid arguments %v", nodes),
			}
			return
		}
		walletAddr := v.WalletAddr.ToBase58()
		log.Debugf("will connect to %s", walletAddr)
		dnsUrl, _ := this.GetExternalIP(walletAddr)
		if len(dnsUrl) == 0 {
			dnsUrl = fmt.Sprintf("%s://%s:%s", this.Config.ChannelProtocol, v.IP, v.Port)
		}
		if strings.Index(dnsUrl, "0.0.0.0:0") != -1 {
			log.Warn("it should not happen")
			resp <- &utils.RequestResponse{
				Code:  serr.INTERNAL_ERROR,
				Error: fmt.Sprintf("invalid arguments %v", dnsUrl),
			}
			return
		}
		log.Debugf("connectDNS Loop DNS %s dnsUrl %v", v.WalletAddr.ToBase58(), dnsUrl)
		err = this.Channel.WaitForConnected(walletAddr, time.Duration(common.WAIT_CHANNEL_CONNECT_TIMEOUT)*time.Second)
		if err != nil {
			log.Errorf("wait channel connected err %s %s", walletAddr, err)
			resp <- &utils.RequestResponse{
				Code:  serr.INTERNAL_ERROR,
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
	for _, addr := range this.Config.DNSWalletAddrs {
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
		if resp.Code != 0 {
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

func (this *Dsp) removeLowQoSTracker() {
	newTrackers := this.DNS.TrackerUrls[:0]
	for _, url := range this.DNS.TrackerUrls {
		errCnt, ok := this.DNS.TrackerFailedCnt.Load(url)
		if !ok {
			continue
		}
		errCntVal, _ := errCnt.(uint64)
		if errCntVal >= common.MAX_TRACKER_REQ_TIMEOUT_NUM {
			this.DNS.TrackerFailedCnt.Delete(url)
			log.Debugf("remove low QoS tracker %s", url)
			continue
		}
		newTrackers = append(newTrackers, url)
	}
	log.Debugf("new tracker cnt: %d", len(newTrackers))
	if len(newTrackers) > 0 {
		this.DNS.TrackerUrls = newTrackers
	} else {
		log.Debugf("set up trackers because of all tracker provide low QoS")
		this.SetupDNSTrackers()
	}
}
