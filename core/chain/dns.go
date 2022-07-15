package chain

import (
	chainCom "github.com/saveio/themis/common"
	"github.com/saveio/themis/smartcontract/service/native/dns"
)

func (c *Chain) GetAllDnsNodes() (map[string]dns.DNSNodeInfo, error) {
	return c.client.GetAllDnsNodes()
}

func (c *Chain) QueryPluginsInfo() (*dns.NameInfoList, error) {
	return c.client.QueryPluginsInfo()
}

func (c *Chain) RegisterHeader(header, desc string, ttl uint64) (string, error) {
	return c.client.RegisterHeader(header, desc, ttl)
}

func (c *Chain) RegisterUrl(url string, rType uint64, name, desc string, ttl uint64) (string, error) {
	return c.client.RegisterUrl(url, rType, name, desc, ttl)
}

func (c *Chain) BindUrl(urlType uint64, url string, name, desc string, ttl uint64) (string, error) {
	return c.client.BindUrl(urlType, url, name, desc, ttl)
}

func (c *Chain) DeleteUrl(url string) (string, error) {
	return c.client.DeleteUrl(url)
}

func (c *Chain) QueryUrl(url string, ownerAddr chainCom.Address) (*dns.NameInfo, error) {
	return c.client.QueryUrl(url, ownerAddr)
}

func (c *Chain) GetDnsNodeByAddr(wallet chainCom.Address) (*dns.DNSNodeInfo, error) {
	return c.client.GetDnsNodeByAddr(wallet)
}

func (c *Chain) DNSNodeReg(ip, port []byte, initPos uint64) (string, error) {
	return c.client.DNSNodeReg(ip, port, initPos)
}

func (c *Chain) UnregisterDNSNode() (string, error) {
	return c.client.UnregisterDNSNode()
}

func (c *Chain) QuitNode() (string, error) {
	return c.client.QuitNode()
}

func (c *Chain) AddInitPos(addPosAmount uint64) (string, error) {
	return c.client.AddInitPos(addPosAmount)
}

func (c *Chain) ReduceInitPos(changePosAmount uint64) (string, error) {
	return c.client.ReduceInitPos(changePosAmount)
}

func (c *Chain) GetPeerPoolMap() (*dns.PeerPoolMap, error) {
	return c.client.GetPeerPoolMap()
}

func (c *Chain) GetPeerPoolItem(pubKey string) (*dns.PeerPoolItem, error) {
	return c.client.GetPeerPoolItem(pubKey)
}
