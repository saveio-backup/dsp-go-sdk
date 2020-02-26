package chain

import (
	"encoding/hex"

	dspErr "github.com/saveio/dsp-go-sdk/error"
	chainCom "github.com/saveio/themis/common"
	"github.com/saveio/themis/smartcontract/service/native/dns"
)

func (this *Chain) GetAllDnsNodes() (map[string]dns.DNSNodeInfo, error) {
	info, err := this.themis.Native.Dns.GetAllDnsNodes()
	if err != nil {
		return nil, dspErr.NewWithError(dspErr.CHAIN_ERROR, err)
	}
	return info, nil
}

func (this *Chain) QueryPluginsInfo() (*dns.NameInfoList, error) {
	info, err := this.themis.Native.Dns.GetPluginList()
	if err != nil {
		return nil, dspErr.NewWithError(dspErr.CHAIN_ERROR, err)
	}
	return info, nil
}

func (this *Chain) RegisterUrl(url string, rType uint64, name, desc string, ttl uint64) (string, error) {
	txHash, err := this.themis.Native.Dns.RegisterUrl(url, rType, name, desc, ttl)
	if err != nil {
		return "", dspErr.NewWithError(dspErr.CHAIN_ERROR, err)
	}
	tx := hex.EncodeToString(chainCom.ToArrayReverse(txHash[:]))
	return tx, nil
}

func (this *Chain) BindUrl(urlType uint64, url string, name, desc string, ttl uint64) (string, error) {
	txHash, err := this.themis.Native.Dns.Binding(urlType, url, name, desc, ttl)
	if err != nil {
		return "", dspErr.NewWithError(dspErr.CHAIN_ERROR, err)
	}
	tx := hex.EncodeToString(chainCom.ToArrayReverse(txHash[:]))
	return tx, nil
}

func (this *Chain) QueryUrl(url string, ownerAddr chainCom.Address) (*dns.NameInfo, error) {
	info, err := this.themis.Native.Dns.QueryUrl(url, ownerAddr)
	if err != nil {
		return nil, dspErr.NewWithError(dspErr.CHAIN_ERROR, err)
	}
	return info, nil
}

func (this *Chain) GetDnsNodeByAddr(wallet chainCom.Address) (*dns.DNSNodeInfo, error) {
	info, err := this.themis.Native.Dns.GetDnsNodeByAddr(wallet)
	if err != nil {
		return nil, dspErr.NewWithError(dspErr.CHAIN_ERROR, err)
	}
	return info, nil
}

func (this *Chain) DNSNodeReg(ip, port []byte, initPos uint64) (string, error) {
	txHash, err := this.themis.Native.Dns.DNSNodeReg(ip, port, initPos)
	if err != nil {
		return "", dspErr.NewWithError(dspErr.CHAIN_ERROR, err)
	}
	tx := hex.EncodeToString(chainCom.ToArrayReverse(txHash[:]))
	return tx, nil
}

func (this *Chain) UnregisterDNSNode() (string, error) {
	txHash, err := this.themis.Native.Dns.UnregisterDNSNode()
	if err != nil {
		return "", dspErr.NewWithError(dspErr.CHAIN_ERROR, err)
	}
	tx := hex.EncodeToString(chainCom.ToArrayReverse(txHash[:]))
	return tx, nil
}

func (this *Chain) QuitNode() (string, error) {
	txHash, err := this.themis.Native.Dns.QuitNode()
	if err != nil {
		return "", dspErr.NewWithError(dspErr.CHAIN_ERROR, err)
	}
	tx := hex.EncodeToString(chainCom.ToArrayReverse(txHash[:]))
	return tx, nil
}

func (this *Chain) AddInitPos(addPosAmount uint64) (string, error) {
	txHash, err := this.themis.Native.Dns.AddInitPos(addPosAmount)
	if err != nil {
		return "", dspErr.NewWithError(dspErr.CHAIN_ERROR, err)
	}
	tx := hex.EncodeToString(chainCom.ToArrayReverse(txHash[:]))
	return tx, nil
}

func (this *Chain) ReduceInitPos(changePosAmount uint64) (string, error) {
	txHash, err := this.themis.Native.Dns.ReduceInitPos(changePosAmount)
	if err != nil {
		return "", dspErr.NewWithError(dspErr.CHAIN_ERROR, err)
	}
	tx := hex.EncodeToString(chainCom.ToArrayReverse(txHash[:]))
	return tx, nil
}

func (this *Chain) GetPeerPoolMap() (*dns.PeerPoolMap, error) {
	m, err := this.themis.Native.Dns.GetPeerPoolMap()
	if err != nil {
		return nil, dspErr.NewWithError(dspErr.CHAIN_ERROR, err)
	}
	return m, nil
}

func (this *Chain) GetPeerPoolItem(pubKey string) (*dns.PeerPoolItem, error) {
	item, err := this.themis.Native.Dns.GetPeerPoolItem(pubKey)
	if err != nil {
		return nil, dspErr.NewWithError(dspErr.CHAIN_ERROR, err)
	}
	return item, nil
}
