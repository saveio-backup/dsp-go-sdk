package dsp

import (
	sdkErr "github.com/saveio/dsp-go-sdk/error"
	"github.com/saveio/dsp-go-sdk/types/link"
)

/* DNS API */
func (this *Dsp) HasDNS() bool {
	return this.DNS != nil && this.DNS.HasDNS()
}

func (this *Dsp) IsDNS(walletAddr string) bool {
	return this.DNS.IsDNS(walletAddr)
}

func (this *Dsp) CurrentDNSWallet() string {
	if this.DNS == nil {
		return ""
	}
	return this.DNS.CurrentDNSWallet()
}

func (this *Dsp) CurrentDNSHostAddr() string {
	if !this.HasDNS() {
		return ""
	}
	return this.DNS.CurrentDNSHostAddr()
}

func (this *Dsp) SetupTrackers() error {
	return this.DNS.SetupTrackers()
}
func (this *Dsp) BootstrapDNS() {
	this.DNS.Discovery()
}

// func (this *Dsp) SetupDNSChannels() error {
// 	return this.DNS.SetupDNSChannels()
// }

func (this *Dsp) PushToTrackers(hash string, trackerUrls []string, listenAddr string) error {
	return this.DNS.PushToTrackers(hash, listenAddr)
}

// func (this *Dsp) GetPeerFromTracker(hash string, trackerUrls []string) []string {
// 	return this.DNS.GetPeerFromTracker(hash, trackerUrls)
// }

func (this *Dsp) PushFilesToTrackers(files []string) {
	this.DNS.PushFilesToTrackers(files)
}

func (this *Dsp) RegisterFileUrl(url, link string) (string, error) {
	return this.DNS.RegisterFileUrl(url, link)
}

func (this *Dsp) BindFileUrl(url, link string) (string, error) {
	return this.DNS.BindFileUrl(url, link)
}

func (this *Dsp) DeleteFileUrl(url string) (string, error) {
	return this.DNS.DeleteFileUrl(url)
}

func (this *Dsp) GenLink(fileHashStr, fileName, blocksRoot, fileOwner string, fileSize, totalCount uint64) string {
	return link.GenOniLinkJSONString(&link.URLLink{
		FileHashStr: fileHashStr,
		FileName:    fileName,
		FileOwner:   fileOwner,
		FileSize:    fileSize,
		BlockNum:    totalCount,
		Trackers:    this.DNS.GetTrackerList(),
		BlocksRoot:  blocksRoot,
	})
}

func (this *Dsp) GetLinkFromUrl(url string) string {
	return this.DNS.GetLinkFromUrl(url)
}

func (this *Dsp) UpdatePluginVersion(urlType uint64, url, link string, urlVersion link.URLVERSION) (string, error) {
	return this.DNS.UpdatePluginVersion(urlType, url, link, urlVersion)
}

func (this *Dsp) GetPluginVersionFromUrl(url string) string {
	return this.DNS.GetPluginVersionFromUrl(url)
}

func (this *Dsp) GetFileHashFromUrl(url string) string {
	return this.DNS.GetFileHashFromUrl(url)
}

func (this *Dsp) GetLinkValues(link string) (*link.URLLink, error) {
	return this.DNS.GetLinkValues(link)
}

func (this *Dsp) GetExternalIP(walletAddr string) (string, error) {
	if this.DNS == nil {
		return "", sdkErr.New(sdkErr.NO_CONNECTED_DNS, "no dns")
	}
	return this.DNS.GetExternalIP(walletAddr)
}

func (this *Dsp) IsDnsOnline(partnerAddr string) bool {
	return this.DNS.IsDnsOnline(partnerAddr)
}

func (this *Dsp) GetAllOnlineDNS() map[string]string {
	return this.DNS.GetAllOnlineDNS()
}

func (this *Dsp) GetOnlineDNSHostAddr(walletAddr string) string {
	return this.DNS.GetOnlineDNSHostAddr(walletAddr)
}

func (this *Dsp) UpdateDNS(walletAddr, hostAddr string, use bool) {
	this.DNS.UpdateDNS(walletAddr, hostAddr, use)

	// log.Debugf("reachable %t host %s", this.channel.ChannelReachale(walletAddr), hostAddr)
	// if this.channel.ChannelReachale(walletAddr) && len(hostAddr) > 0 {
	// 	this.DNS.OnlineDNS[walletAddr] = hostAddr
	// }
	// if !use && (this.DNS != nil && this.DNS.DNSNode != nil) {
	// 	return
	// }
	// this.DNS.DNSNode = &dns.DNSNodeInfo{
	// 	WalletAddr: walletAddr,
	// 	HostAddr:   hostAddr,
	// }
	// if err := this.channel.SelectDNSChannel(walletAddr); err != nil {
	// 	log.Errorf("update selecting new dns channel err %s", err)
	// }
}

func (this *Dsp) ResetDNSNode() {
	this.DNS.ResetDNSNode()
}

func (this *Dsp) GetTrackerList() []string {
	return this.DNS.GetTrackerList()
}
