package dsp

import (
	"encoding/hex"
	"errors"
	"fmt"
	"io/ioutil"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/oniio/dsp-go-sdk/common"
	"github.com/oniio/dsp-go-sdk/config"
	"github.com/oniio/dsp-go-sdk/utils"
	chaincom "github.com/oniio/oniChain/common"
	"github.com/oniio/oniChain/common/log"
	"github.com/oniio/oniChain/smartcontract/service/native/dns"
	"github.com/oniio/oniDNS/tracker"
)

type DNSNodeInfo struct {
	WalletAddr  string
	ChannelAddr string
}

func (this *Dsp) SetupDNSNode() error {
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
	for _, v := range ns {
		log.Debugf("DNS %s :%v, port %v", v.WalletAddr.ToBase58(), string(v.IP), string(v.Port))
		if len(this.TrackerUrls) >= maxTrackerNum {
			break
		}
		if this.DNSNode != nil {
			trackerUrl := fmt.Sprintf("%s://%s:%d/announce", common.TRACKER_NETWORK_PROTOCOL, v.IP, common.TRACKER_PORT)
			this.TrackerUrls = append(this.TrackerUrls, trackerUrl)
			continue
		}
		dnsUrl := fmt.Sprintf("%s://%s:%s", this.Config.ChannelProtocol, v.IP, v.Port)
		if this.Network != nil && !this.Network.IsPeerListenning(dnsUrl) {
			continue
		}
		log.Debugf("open channel set addr %s-%s", v.WalletAddr.ToBase58(), dnsUrl)
		this.Channel.SetHostAddr(v.WalletAddr.ToBase58(), dnsUrl)
		_, err = this.Channel.OpenChannel(v.WalletAddr.ToBase58())
		if err != nil {
			log.Debugf("open channel err ")
			continue
		}
		err = this.Channel.WaitForConnected(v.WalletAddr.ToBase58(), time.Duration(common.WAIT_CHANNEL_CONNECT_TIMEOUT)*time.Second)
		if err != nil {
			log.Debugf("wait channel connected err %s", err)
			// TODO: withdraw and close channel
			continue
		}
		bal, _ := this.Channel.GetCurrentBalance(v.WalletAddr.ToBase58())
		log.Debugf("current balance %d", bal)
		// depositAmount := uint64(0)
		// if this.Config.DnsChannelDeposit > bal {
		// 	depositAmount = this.Config.DnsChannelDeposit - bal
		// }
		log.Infof("connect to dns node :%s, deposit %d", dnsUrl, this.Config.DnsChannelDeposit)
		err = this.Channel.SetDeposit(v.WalletAddr.ToBase58(), this.Config.DnsChannelDeposit)
		if err != nil {
			log.Debugf("deposit result %s", err)
			// TODO: withdraw and close channel
			continue
		}
		this.DNSNode = &DNSNodeInfo{
			WalletAddr:  v.WalletAddr.ToBase58(),
			ChannelAddr: dnsUrl,
		}
		trackerUrl := fmt.Sprintf("%s://%s:%d/announce", common.TRACKER_NETWORK_PROTOCOL, v.IP, common.TRACKER_PORT)
		this.TrackerUrls = append(this.TrackerUrls, trackerUrl)
	}
	log.Debugf("this.TrackerUrls len %d", len(this.TrackerUrls))
	return nil
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
		err := tracker.CompleteTorrent(hashBytes, trackerUrl, netIp, uint16(netPort))
		if err != nil {
			return err
		}
	}
	return nil
}

func (this *Dsp) GetPeerFromTracker(hash string, trackerUrls []string) []string {
	var hashBytes [46]byte
	copy(hashBytes[:], []byte(hash)[:])

	peerAddrs := make([]string, 0)
	networkProtocol := common.DSP_NETWORK_PROTOCOL
	selfAddr := ""
	if this.Network != nil {
		networkProtocol = this.Network.Protocol()
		selfAddr = this.Network.ExternalAddr()
	}
	for _, trackerUrl := range trackerUrls {
		peers := tracker.GetTorrentPeers(hashBytes, trackerUrl, -1, 1)
		if len(peers) == 0 {
			continue
		}
		for _, p := range peers {
			addr := fmt.Sprintf("%s://%s:%d", networkProtocol, p.IP, p.Port)
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
		if len(this.TrackerUrls) == 0 {
			continue
		}
		files := make([]string, 0)
		switch this.Config.FsType {
		case config.FS_FILESTORE:
			fileInfos, err := ioutil.ReadDir(this.Config.FsFileRoot)
			if err != nil || len(fileInfos) == 0 {
				continue
			}
			for _, info := range fileInfos {
				if info.IsDir() ||
					(!strings.HasPrefix(info.Name(), common.PROTO_NODE_PREFIX) && !strings.HasPrefix(info.Name(), common.RAW_NODE_PREFIX)) {
					continue
				}
				files = append(files, info.Name())
			}
		case config.FS_BLOCKSTORE:
			files, _ = this.taskMgr.AllDownloadFiles()
		}
		if len(files) == 0 {
			continue
		}
		for _, fileHashStr := range files {
			this.PushToTrackers(fileHashStr, this.TrackerUrls, this.Network.ListenAddr())
		}
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
	if err != nil || info == nil {
		return ""
	}
	return string(info.Name)
}

func (this *Dsp) GetFileHashFromUrl(url string) string {
	link := this.GetLinkFromUrl(url)
	if len(link) == 0 {
		return ""
	}
	return utils.GetFileHashFromLink(link)
}

// GetExternalIP. get external ip of wallet from dns nodes
func (this *Dsp) GetExternalIP(walletAddr string) string {
	test := make(map[string]string, 0)
	test["AYMnqA65pJFKAbbpD8hi5gdNDBmeFBy5hS"] = "tcp://127.0.0.1:13001"
	test["AWaE84wqVf1yffjaR6VJ4NptLdqBAm8G9c"] = "tcp://127.0.0.1:13002"
	test["AGeTrARjozPVLhuzMxZq36THMtvsrZNAHq"] = "tcp://127.0.0.1:13003"
	test["ANa3f9jm2FkWu4NrVn6L1FGu7zadKdvPjL"] = "tcp://127.0.0.1:13004"
	test["ANy4eS6oQaX15xpGV7dvsinh2aiqPm9HDf"] = "tcp://127.0.0.1:13005"
	test["AJtzEUDLzsRKbHC1Tfc1oNh8a1edpnVAUf"] = "tcp://127.0.0.1:13008"
	test["AMkN2sRQyT3qHZQqwEycHCX2ezdZNpXNdJ"] = "tcp://127.0.0.1:13001"
	test["AWpW2ukMkgkgRKtwWxC3viXEX8ijLio2Ng"] = "tcp://127.0.0.1:13003"
	test["AKTfgYTAEzGG5FXsM8HHc8M3j95N495TBP"] = "tcp://127.0.0.1:13003"
	test["AGGTaoJ8Ygim7zVi5ZZqrXy8EQqgNQJxYx"] = "tcp://127.0.0.1:13001"
	return test[walletAddr]
}

// SetupPartnerHost. setup host addr for partners
func (this *Dsp) SetupPartnerHost(partners []string) {
	log.Debugf("partners %v\n", partners)
	for _, addr := range partners {
		host := this.GetExternalIP(addr)
		this.Channel.SetHostAddr(addr, host)
	}
}
