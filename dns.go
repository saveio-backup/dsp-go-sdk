package dsp

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	lru "github.com/hashicorp/golang-lru"
	"github.com/saveio/dsp-go-sdk/actor/client"
	"github.com/saveio/dsp-go-sdk/common"
	"github.com/saveio/dsp-go-sdk/utils"
	"github.com/saveio/scan/tracker"
	dnscom "github.com/saveio/scan/tracker/common"
	chainsdk "github.com/saveio/themis-go-sdk/utils"
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
	UpdatedAt int64
}

type DNS struct {
	TrackerUrls      []string
	TrackerFailedCnt map[string]uint64
	DNSNode          *DNSNodeInfo
	OnlineDNS        map[string]string
	PublicAddrCache  *lru.ARCCache
}

func NewDNS() *DNS {
	cache, _ := lru.NewARC(common.MAX_PUBLICADDR_CACHE_LEN)
	return &DNS{
		TrackerUrls:      make([]string, 0, common.MAX_TRACKERS_NUM),
		TrackerFailedCnt: make(map[string]uint64),
		PublicAddrCache:  cache,
		OnlineDNS:        make(map[string]string),
	}
}

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
	}
	existDNSMap := make(map[string]struct{}, 0)
	for _, trackerUrl := range this.DNS.TrackerUrls {
		existDNSMap[trackerUrl] = struct{}{}
	}
	for _, v := range ns {
		log.Debugf("DNS %s :%v, port %v", v.WalletAddr.ToBase58(), string(v.IP), string(v.Port))
		if len(this.DNS.TrackerUrls) >= maxTrackerNum {
			break
		}
		trackerUrl := fmt.Sprintf("%s://%s:%d/announce", common.TRACKER_NETWORK_PROTOCOL, v.IP, common.TRACKER_PORT)
		_, ok := existDNSMap[trackerUrl]
		if ok {
			continue
		}
		this.DNS.TrackerUrls = append(this.DNS.TrackerUrls, trackerUrl)
	}
	this.DNS.TrackerUrls = append(this.DNS.TrackerUrls, this.Config.Trackers...)
	log.Debugf("this.DNS.TrackerUrls %v", this.DNS.TrackerUrls)
	return nil
}

func (this *Dsp) SetOnlineDNS() {
	log.Debugf("SetOnlineDNS++++")
	ns, err := this.Chain.Native.Dns.GetAllDnsNodes()
	if err != nil {
		return
	}
	if len(ns) == 0 {
		log.Warnf("no dns nodes")
		return
	}
	// first init
	for _, v := range ns {
		log.Debugf("DNS %s :%v, port %v", v.WalletAddr.ToBase58(), string(v.IP), string(v.Port))
		walletAddr := v.WalletAddr.ToBase58()
		dnsUrl, _ := this.GetExternalIP(walletAddr)
		if len(dnsUrl) == 0 {
			dnsUrl = fmt.Sprintf("%s://%s:%s", this.Config.ChannelProtocol, v.IP, v.Port)
		}
		if strings.Index(dnsUrl, "0.0.0.0:0") != -1 {
			log.Warn("it should not happen")
			continue
		}
		err = this.Channel.SetHostAddr(walletAddr, dnsUrl)
		if err != nil {
			continue
		}
		// err = client.ChannelP2pWaitForConnected(dnsUrl, time.Duration(common.WAIT_CHANNEL_CONNECT_TIMEOUT)*time.Second)
		// TODO: wait for channel to refactor
		err = this.Channel.WaitForConnected(walletAddr, time.Duration(common.WAIT_CHANNEL_CONNECT_TIMEOUT)*time.Second)
		if err != nil {
			log.Errorf("wait channel connected err %s %s", walletAddr, err)
			continue
		}
		this.DNS.OnlineDNS[v.WalletAddr.ToBase58()] = dnsUrl
		if len(this.DNS.OnlineDNS) > common.MAX_DNS_NUM {
			break
		}
	}

	if this.Config.AutoSetupDNSEnable {
		return
	}
	channels, err := this.Channel.AllChannels()
	if err != nil {
		log.Errorf("get all channel err %s", err)
		return
	}
	if len(channels.Channels) == 0 {
		return
	}
	for _, channel := range channels.Channels {
		url, ok := this.DNS.OnlineDNS[channel.Address]
		if !ok {
			continue
		}
		this.DNS.DNSNode = &DNSNodeInfo{
			WalletAddr: channel.Address,
			HostAddr:   url,
		}
		break
	}
}

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
		err = this.Channel.SetHostAddr(walletAddr, dnsUrl)
		if err != nil {
			return err
		}
		// err = client.ChannelP2pWaitForConnected(dnsUrl, time.Duration(common.WAIT_CHANNEL_CONNECT_TIMEOUT)*time.Second)
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
	netIp := net.ParseIP(host).To4()
	if netIp == nil {
		netIp = net.ParseIP(host).To16()
	}
	netPort, err := strconv.Atoi(port)
	if err != nil {
		return err
	}
	var hashBytes [46]byte
	copy(hashBytes[:], []byte(hash)[:])
	for _, trackerUrl := range trackerUrls {
		log.Debugf("trackerurl %s hashBytes: %v netIp:%v netPort:%v", trackerUrl, hashBytes, netIp, netPort)
		go tracker.CompleteTorrent(hashBytes, trackerUrl, netIp, uint16(netPort))
	}
	return nil
}

func (this *Dsp) GetPeerFromTracker(hash string, trackerUrls []string) []string {
	var hashBytes [46]byte
	copy(hashBytes[:], []byte(hash)[:])
	peerAddrs := make([]string, 0)
	selfAddr := client.P2pGetPublicAddr()
	protocol := selfAddr[:strings.Index(selfAddr, "://")]
	for _, trackerUrl := range trackerUrls {
		request := func(resp chan *trackerResp) {
			peers := tracker.GetTorrentPeers(hashBytes, trackerUrl, -1, 1)
			resp <- &trackerResp{
				ret: peers,
				err: nil,
			}
		}
		ret, err := this.trackerReq(trackerUrl, request)
		if err != nil {
			continue
		}
		peers := ret.([]tracker.Peer)
		if len(peers) == 0 {
			continue
		}
		for _, p := range peers {
			addr := fmt.Sprintf("%s://%s:%d", protocol, p.IP, p.Port)
			if addr == selfAddr {
				continue
			}
			peerAddrs = append(peerAddrs, addr)
		}
		break
	}
	this.removeLowQoSTracker()
	return peerAddrs
}

func (this *Dsp) StartSeedService() {
	tick := time.NewTicker(time.Duration(this.Config.SeedInterval) * time.Second)
	for {
		<-tick.C
		this.PushLocalFilesToTrackers()
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
	for _, fileHashStr := range files {
		this.PushToTrackers(fileHashStr, this.DNS.TrackerUrls, client.P2pGetPublicAddr())
	}
}

func (this *Dsp) RegisterFileUrl(url, link string) (string, error) {
	urlPrefix := this.Chain.Native.Dns.GetCustomUrlHeader()
	if !strings.HasPrefix(url, urlPrefix) {
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
		return "", errors.New("tx confirme err")
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
		return "", errors.New("tx confirme err")
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
	netIp := net.ParseIP(host).To4()
	if netIp == nil {
		netIp = net.ParseIP(host).To16()
	}
	netPort, err := strconv.ParseUint(port, 10, 16)
	if err != nil {
		return err
	}

	var wallet [20]byte
	copy(wallet[:], walletAddr[:])
	if len(this.DNS.TrackerUrls) == 0 {
		log.Debugf("set up dns trackers before register channel endpoint")
		err = this.SetupDNSTrackers()
		if err != nil {
			return err
		}
	}
	hasRegister := false
	for _, trackerUrl := range this.DNS.TrackerUrls {
		log.Debugf("trackerurl %s walletAddr: %v netIp:%v netPort:%v", trackerUrl, wallet, netIp, netPort)
		params := dnscom.ApiParams{
			TrackerUrl: trackerUrl,
			Wallet:     wallet,
			IP:         netIp,
			Port:       uint16(netPort),
		}
		rawData, err := json.Marshal(params)
		if err != nil {
			continue
		}

		sigData, err := chainsdk.Sign(this.CurrentAccount(), rawData)
		if err != nil {
			continue
		}
		request := func(resp chan *trackerResp) {
			log.Debugf("start RegEndPoint %s ipport %v:%v", trackerUrl, netIp, netPort)
			err := tracker.RegEndPoint(trackerUrl, sigData, this.CurrentAccount().PublicKey, wallet, netIp, uint16(netPort))
			log.Debugf("start RegEndPoint end")
			if err != nil {
				log.Errorf("req endpoint failed, err %s", err)
			}
			resp <- &trackerResp{
				ret: nil,
				err: err,
			}
		}
		_, err = this.trackerReq(trackerUrl, request)
		if err != nil {
			continue
		}
		if err != nil {
			continue
		}
		if !hasRegister {
			hasRegister = true
		}
	}
	this.removeLowQoSTracker()
	if !hasRegister {
		return errors.New("register endpoint failed for all dns nodes")
	}
	return nil
}

// GetExternalIP. get external ip of wallet from dns nodes
func (this *Dsp) GetExternalIP(walletAddr string) (string, error) {
	info, ok := this.DNS.PublicAddrCache.Get(walletAddr)
	if ok && info != nil {
		addrInfo, ok := info.(*PublicAddrInfo)
		if ok && addrInfo != nil && (addrInfo.UpdatedAt+60) > time.Now().Unix() {
			log.Debugf("GetExternalIP %s addr %s from cache", walletAddr, addrInfo.HostAddr)
			return addrInfo.HostAddr, nil
		}
	}
	address, err := chaincom.AddressFromBase58(walletAddr)
	if err != nil {
		log.Errorf("address from b58 failed %s", err)
		return "", err
	}
	if len(this.DNS.TrackerUrls) == 0 {
		log.Warn("GetExternalIP no trackers")
	}
	for _, url := range this.DNS.TrackerUrls {
		request := func(resp chan *trackerResp) {
			hostAddr, err := tracker.ReqEndPoint(url, address)
			log.Debugf("ReqEndPoint hostAddr url: %s, address %s, hostaddr:%s", url, address.ToBase58(), string(hostAddr))
			resp <- &trackerResp{
				ret: hostAddr,
				err: err,
			}
		}
		ret, err := this.trackerReq(url, request)
		if err != nil {
			log.Errorf("address from req failed %s", err)
			continue
		}
		hostAddr := ret.(string)
		log.Debugf("GetExternalIP %s :%v from %s", walletAddr, string(hostAddr), url)
		if len(string(hostAddr)) == 0 || !utils.IsHostAddrValid(string(hostAddr)) {
			continue
		}
		hostAddrStr := utils.FullHostAddr(string(hostAddr), this.Config.ChannelProtocol)
		if strings.Index(hostAddrStr, "0.0.0.0:0") != -1 {
			continue
		}
		this.DNS.PublicAddrCache.Add(walletAddr, &PublicAddrInfo{
			HostAddr:  hostAddrStr,
			UpdatedAt: time.Now().Unix(),
		})
		return hostAddrStr, nil
	}
	this.removeLowQoSTracker()
	return "", errors.New("host addr not found")
}

// SetupPartnerHost. setup host addr for partners
func (this *Dsp) SetupPartnerHost(partners []string) {
	for _, addr := range partners {
		host, err := this.GetExternalIP(addr)
		log.Debugf("[SetupPartnerHost] get external ip %v, %v, err %s", addr, host, err)
		if err != nil || len(host) == 0 {
			continue
		}
		this.Channel.SetHostAddr(addr, host)
	}
}

type trackerResp struct {
	ret interface{}
	err error
}

func (this *Dsp) trackerReq(trackerUrl string, request func(chan *trackerResp)) (interface{}, error) {
	done := make(chan *trackerResp, 1)
	go request(done)
	for {
		select {
		case ret := <-done:
			return ret.ret, ret.err
		case <-time.After(time.Duration(common.TRACKER_SERVICE_TIMEOUT) * time.Second):
			log.Errorf("tracker request timeout")
			errCnt := this.DNS.TrackerFailedCnt[trackerUrl]
			this.DNS.TrackerFailedCnt[trackerUrl] = errCnt + 1
			return nil, errors.New("tracker request timeout")
		}
	}
}

func (this *Dsp) removeLowQoSTracker() {
	newTrackers := this.DNS.TrackerUrls[:0]
	log.Debugf("newTrackers: %v", newTrackers)
	for _, url := range this.DNS.TrackerUrls {
		errCnt := this.DNS.TrackerFailedCnt[url]
		if errCnt >= common.MAX_TRACKER_REQ_TIMEOUT_NUM {
			delete(this.DNS.TrackerFailedCnt, url)
			log.Debugf("remove low QoS tracker %s", url)
			continue
		}
		newTrackers = append(newTrackers, url)
	}
	log.Debugf("new tracker cnt: %d", len(this.DNS.TrackerUrls))
	if len(newTrackers) > 0 {
		this.DNS.TrackerUrls = newTrackers
	} else {
		log.Debugf("set up trackers because of all tracker provide low QoS")
		this.SetupDNSTrackers()
	}
}
