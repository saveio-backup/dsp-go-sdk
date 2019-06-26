package dsp

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/saveio/dsp-go-sdk/actor/client"
	"github.com/saveio/dsp-go-sdk/common"
	"github.com/saveio/dsp-go-sdk/config"
	"github.com/saveio/dsp-go-sdk/utils"
	"github.com/saveio/scan/tracker"
	dnscom "github.com/saveio/scan/tracker/common"
	chainsdk "github.com/saveio/themis-go-sdk/utils"
	chaincom "github.com/saveio/themis/common"
	"github.com/saveio/themis/common/log"
	"github.com/saveio/themis/smartcontract/service/native/dns"
)

type DNSNodeInfo struct {
	WalletAddr  string
	ChannelAddr string
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
	if this.TrackerUrls == nil {
		this.TrackerUrls = make([]string, 0)
	}
	existDNSMap := make(map[string]struct{}, 0)
	for _, trackerUrl := range this.TrackerUrls {
		existDNSMap[trackerUrl] = struct{}{}
	}
	for _, v := range ns {
		log.Debugf("DNS %s :%v, port %v", v.WalletAddr.ToBase58(), string(v.IP), string(v.Port))
		if len(this.TrackerUrls) >= maxTrackerNum {
			break
		}
		trackerUrl := fmt.Sprintf("%s://%s:%d/announce", common.TRACKER_NETWORK_PROTOCOL, v.IP, common.TRACKER_PORT)
		_, ok := existDNSMap[trackerUrl]
		if ok {
			continue
		}
		this.TrackerUrls = append(this.TrackerUrls, trackerUrl)
	}
	log.Debugf("this.TrackerUrls %v", this.TrackerUrls)
	return nil
}

func (this *Dsp) SetupDNSChannels() error {
	log.Debugf("SetupDNSChannels++++")

	ns, err := this.Chain.Native.Dns.GetAllDnsNodes()
	if err != nil {
		return err
	}
	if len(ns) == 0 {
		return errors.New("no dns nodes")
	}
	oldNodes := this.Channel.GetAllPartners()

	setDNSNodeFunc := func(dnsUrl, walletAddr string) error {
		log.Debugf("set dns node func %s %s", dnsUrl, walletAddr)
		if strings.Index(dnsUrl, "0.0.0.0:0") != -1 {
			return errors.New("invalid host addr")
		}
		err = this.Channel.SetHostAddr(walletAddr, dnsUrl)
		if err != nil {
			return err
		}
		err = this.Channel.WaitForConnected(walletAddr, time.Duration(common.WAIT_CHANNEL_CONNECT_TIMEOUT)*time.Second)
		if err != nil {
			log.Errorf("wait channel connected err %s %s", walletAddr, err)
			return err
		}
		_, err = this.Channel.OpenChannel(walletAddr)
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
		this.DNSNode = &DNSNodeInfo{
			WalletAddr:  walletAddr,
			ChannelAddr: dnsUrl,
		}
		log.Debugf("DNSNode wallet: %v, addr: %v", this.DNSNode.WalletAddr, this.DNSNode.ChannelAddr)
		return nil
	}

	// setup old nodes
	// TODO: get more acculatey nodes list from channel
	if !this.Config.AutoSetupDNSEnable && len(oldNodes) > 0 {
		log.Debugf("set up old dns nodes %v, ns:%v", oldNodes, ns)
		for _, walletAddr := range oldNodes {
			address, err := chaincom.AddressFromBase58(walletAddr)
			if err != nil {
				continue
			}
			if _, ok := ns[address.ToHexString()]; !ok {
				log.Debugf("dns not found %s", address.ToHexString())
				continue
			}
			dnsUrl, _ := this.GetExternalIP(walletAddr)
			if len(dnsUrl) == 0 {
				dnsUrl = fmt.Sprintf("%s://%s:%s", this.Config.ChannelProtocol, ns[address.ToHexString()].IP, ns[address.ToHexString()].Port)
			}
			log.Debugf("dns url of oldnodes %s", dnsUrl)
			err = setDNSNodeFunc(dnsUrl, walletAddr)
			if err != nil {
				log.Debugf("set dns node func err %s", err)
				continue
			}
			break
		}
		return err
	}

	// first init
	for _, v := range ns {
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
		ret, err := trackerReq(request)
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
	if len(this.TrackerUrls) == 0 {
		return
	}
	files := make([]string, 0)
	switch this.Config.FsType {
	case config.FS_FILESTORE:
		fileInfos, err := ioutil.ReadDir(this.Config.FsFileRoot)
		if err != nil || len(fileInfos) == 0 {
			return
		}
		for _, info := range fileInfos {
			if info.IsDir() ||
				(!strings.HasPrefix(info.Name(), common.PROTO_NODE_PREFIX) && !strings.HasPrefix(info.Name(), common.RAW_NODE_PREFIX)) {
				return
			}
			files = append(files, info.Name())
		}
	case config.FS_BLOCKSTORE:
		files, _ = this.taskMgr.AllDownloadFiles()
	}
	if len(files) == 0 {
		return
	}
	for _, fileHashStr := range files {
		this.PushToTrackers(fileHashStr, this.TrackerUrls, client.P2pGetPublicAddr())
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
		panic("no url to download")
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
	if len(this.TrackerUrls) == 0 {
		log.Debugf("set up dns trackers before register channel endpoint")
		err = this.SetupDNSTrackers()
		if err != nil {
			return err
		}
	}
	hasRegister := false
	for _, trackerUrl := range this.TrackerUrls {
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
			err := tracker.RegEndPoint(trackerUrl, sigData, this.CurrentAccount().PublicKey, wallet, netIp, uint16(netPort))
			resp <- &trackerResp{
				ret: nil,
				err: err,
			}
		}
		_, err = trackerReq(request)
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
	if !hasRegister {
		return errors.New("register endpoint failed for all dns nodes")
	}
	return nil
}

// GetExternalIP. get external ip of wallet from dns nodes
func (this *Dsp) GetExternalIP(walletAddr string) (string, error) {
	address, err := chaincom.AddressFromBase58(walletAddr)
	if err != nil {
		log.Errorf("address from b58 failed %s", err)
		return "", err
	}
	for _, url := range this.TrackerUrls {
		request := func(resp chan *trackerResp) {
			hostAddr, err := tracker.ReqEndPoint(url, address)
			log.Debugf("hostAddr :%v %s", hostAddr, string(hostAddr))
			resp <- &trackerResp{
				ret: hostAddr,
				err: err,
			}
		}
		ret, err := trackerReq(request)
		if err != nil {
			log.Errorf("address from req failed %s", err)
			continue
		}
		log.Debugf("ret %v. rettype %T, retstr %s", ret, ret, string(ret.([]byte)))
		hostAddr := ret.([]byte)
		log.Debugf("GetExternalIP %s :%v from %s", walletAddr, string(hostAddr), url)
		if len(string(hostAddr)) == 0 || !utils.IsHostAddrValid(string(hostAddr)) {
			continue
		}
		hostAddrStr := utils.FullHostAddr(string(hostAddr), this.Config.ChannelProtocol)
		if strings.Index(hostAddrStr, "0.0.0.0:0") != -1 {
			continue
		}
		return hostAddrStr, nil
	}
	log.Debugf("no request %v", this.TrackerUrls)
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

func trackerReq(request func(chan *trackerResp)) (interface{}, error) {
	done := make(chan *trackerResp, 1)
	go request(done)
	for {
		select {
		case ret := <-done:
			return ret.ret, ret.err
		case <-time.After(time.Duration(common.TRACKER_SERVICE_TIMEOUT) * time.Second):
			log.Errorf("tracker request timeout")
			return nil, errors.New("tracker request timeout")
		}
	}
}
