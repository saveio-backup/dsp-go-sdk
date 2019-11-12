package dsp

import (
	"github.com/saveio/dsp-go-sdk/core/dns"
	chainCom "github.com/saveio/themis/common"
	"github.com/saveio/themis/common/log"
)

/* DNS API */

func (this *Dsp) HasDNS() bool {
	return this.dns != nil && this.dns.DNSNode != nil
}

func (this *Dsp) IsDNS(walletAddr string) bool {
	if !this.HasDNS() {
		return false
	}
	return this.dns.DNSNode.WalletAddr == walletAddr
}

func (this *Dsp) CurrentDNSWallet() string {
	if this.dns == nil || this.dns.DNSNode == nil {
		return ""
	}
	return this.dns.DNSNode.WalletAddr
}

func (this *Dsp) CurrentDNSHostAddr() string {
	if !this.HasDNS() {
		return ""
	}
	return this.dns.DNSNode.HostAddr
}

func (this *Dsp) SetupTrackers() error {
	return this.dns.SetupTrackers()
}
func (this *Dsp) BootstrapDNS() {
	this.dns.BootstrapDNS()
}
func (this *Dsp) SetupDNSChannels() error {
	return this.dns.SetupDNSChannels()
}

func (this *Dsp) PushToTrackers(hash string, trackerUrls []string, listenAddr string) error {
	return this.dns.PushToTrackers(hash, trackerUrls, listenAddr)
}

func (this *Dsp) GetPeerFromTracker(hash string, trackerUrls []string) []string {
	return this.dns.GetPeerFromTracker(hash, trackerUrls)
}

func (this *Dsp) PushFilesToTrackers(files []string) {
	this.dns.PushFilesToTrackers(files)
}

func (this *Dsp) RegisterFileUrl(url, link string) (string, error) {
	return this.dns.RegisterFileUrl(url, link)
}

func (this *Dsp) BindFileUrl(url, link string) (string, error) {
	return this.dns.BindFileUrl(url, link)
}

func (this *Dsp) GetLinkFromUrl(url string) string {
	return this.dns.GetLinkFromUrl(url)
}

func (this *Dsp) GetFileHashFromUrl(url string) string {
	return this.dns.GetFileHashFromUrl(url)
}

func (this *Dsp) GetLinkValues(link string) map[string]string {
	return this.dns.GetLinkValues(link)
}

func (this *Dsp) RegNodeEndpoint(walletAddr chainCom.Address, endpointAddr string) error {
	return this.dns.RegNodeEndpoint(walletAddr, endpointAddr)
}

func (this *Dsp) GetExternalIP(walletAddr string) (string, error) {
	return this.dns.GetExternalIP(walletAddr)
}

func (this *Dsp) IsDnsOnline(partnerAddr string) bool {
	url, ok := this.dns.OnlineDNS[partnerAddr]
	if !ok || url == "" {
		log.Debugf("OnlineDNS %v", this.dns.OnlineDNS)
		return false
	}
	return true
}

func (this *Dsp) GetAllOnlineDNS() map[string]string {
	return this.dns.OnlineDNS
}

func (this *Dsp) GetOnlineDNSHostAddr(walletAddr string) string {
	return this.dns.OnlineDNS[walletAddr]
}

func (this *Dsp) UpdateDNS(walletAddr, hostAddr string) {
	this.dns.DNSNode = &dns.DNSNodeInfo{
		WalletAddr: walletAddr,
		HostAddr:   hostAddr,
	}
}

func (this *Dsp) GetTrackerList() []string {
	return this.dns.TrackerUrls
}
