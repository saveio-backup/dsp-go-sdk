package chain

import (
	"encoding/hex"
	sdkErr "github.com/saveio/dsp-go-sdk/error"
	chainCom "github.com/saveio/themis/common"
	"github.com/saveio/themis/smartcontract/service/native/dns"
)

func (t *Themis) GetAllDnsNodes() (map[string]dns.DNSNodeInfo, error) {
	info, err := t.sdk.Native.Dns.GetAllDnsNodes()
	if err != nil {
		return nil, sdkErr.NewWithError(sdkErr.CHAIN_ERROR, t.FormatError(err))
	}
	return info, nil
}

func (t *Themis) QueryPluginsInfo() (*dns.NameInfoList, error) {
	info, err := t.sdk.Native.Dns.GetPluginList()
	if err != nil {
		return nil, sdkErr.NewWithError(sdkErr.CHAIN_ERROR, t.FormatError(err))
	}
	return info, nil
}

func (t *Themis) RegisterHeader(header, desc string, ttl uint64) (string, error) {
	txHash, err := t.sdk.Native.Dns.RegisterHeader(header, desc, ttl)
	if err != nil {
		return "", sdkErr.NewWithError(sdkErr.CHAIN_ERROR, t.FormatError(err))
	}
	tx := hex.EncodeToString(chainCom.ToArrayReverse(txHash[:]))
	return tx, nil
}

func (t *Themis) RegisterUrl(url string, rType uint64, name, desc string, ttl uint64) (string, error) {
	txHash, err := t.sdk.Native.Dns.RegisterUrl(url, rType, name, desc, ttl)
	if err != nil {
		return "", sdkErr.NewWithError(sdkErr.CHAIN_ERROR, t.FormatError(err))
	}
	tx := hex.EncodeToString(chainCom.ToArrayReverse(txHash[:]))
	return tx, nil
}

func (t *Themis) BindUrl(urlType uint64, url string, name, desc string, ttl uint64) (string, error) {
	txHash, err := t.sdk.Native.Dns.Binding(urlType, url, name, desc, ttl)
	if err != nil {
		return "", sdkErr.NewWithError(sdkErr.CHAIN_ERROR, t.FormatError(err))
	}
	tx := hex.EncodeToString(chainCom.ToArrayReverse(txHash[:]))
	return tx, nil
}

func (t *Themis) DeleteUrl(url string) (string, error) {
	txHash, err := t.sdk.Native.Dns.DeleteUrl(url)
	if err != nil {
		return "", sdkErr.NewWithError(sdkErr.CHAIN_ERROR, t.FormatError(err))
	}
	tx := hex.EncodeToString(chainCom.ToArrayReverse(txHash[:]))
	return tx, nil
}

func (t *Themis) QueryUrl(url string, ownerAddr chainCom.Address) (*dns.NameInfo, error) {
	info, err := t.sdk.Native.Dns.QueryUrl(url, ownerAddr)
	if err != nil {
		return nil, sdkErr.NewWithError(sdkErr.CHAIN_ERROR, t.FormatError(err))
	}
	return info, nil
}

func (t *Themis) GetDnsNodeByAddr(wallet chainCom.Address) (*dns.DNSNodeInfo, error) {
	info, err := t.sdk.Native.Dns.GetDnsNodeByAddr(wallet)
	if err != nil {
		return nil, sdkErr.NewWithError(sdkErr.CHAIN_ERROR, t.FormatError(err))
	}
	return info, nil
}

func (t *Themis) DNSNodeReg(ip, port []byte, initPos uint64) (string, error) {
	txHash, err := t.sdk.Native.Dns.DNSNodeReg(ip, port, initPos)
	if err != nil {
		return "", sdkErr.NewWithError(sdkErr.CHAIN_ERROR, t.FormatError(err))
	}
	tx := hex.EncodeToString(chainCom.ToArrayReverse(txHash[:]))
	return tx, nil
}

func (t *Themis) UnregisterDNSNode() (string, error) {
	txHash, err := t.sdk.Native.Dns.UnregisterDNSNode()
	if err != nil {
		return "", sdkErr.NewWithError(sdkErr.CHAIN_ERROR, t.FormatError(err))
	}
	tx := hex.EncodeToString(chainCom.ToArrayReverse(txHash[:]))
	return tx, nil
}

func (t *Themis) QuitNode() (string, error) {
	txHash, err := t.sdk.Native.Dns.QuitNode()
	if err != nil {
		return "", sdkErr.NewWithError(sdkErr.CHAIN_ERROR, t.FormatError(err))
	}
	tx := hex.EncodeToString(chainCom.ToArrayReverse(txHash[:]))
	return tx, nil
}

func (t *Themis) AddInitPos(addPosAmount uint64) (string, error) {
	txHash, err := t.sdk.Native.Dns.AddInitPos(addPosAmount)
	if err != nil {
		return "", sdkErr.NewWithError(sdkErr.CHAIN_ERROR, t.FormatError(err))
	}
	tx := hex.EncodeToString(chainCom.ToArrayReverse(txHash[:]))
	return tx, nil
}

func (t *Themis) ReduceInitPos(changePosAmount uint64) (string, error) {
	txHash, err := t.sdk.Native.Dns.ReduceInitPos(changePosAmount)
	if err != nil {
		return "", sdkErr.NewWithError(sdkErr.CHAIN_ERROR, t.FormatError(err))
	}
	tx := hex.EncodeToString(chainCom.ToArrayReverse(txHash[:]))
	return tx, nil
}

func (t *Themis) GetPeerPoolMap() (*dns.PeerPoolMap, error) {
	m, err := t.sdk.Native.Dns.GetPeerPoolMap()
	if err != nil {
		return nil, sdkErr.NewWithError(sdkErr.CHAIN_ERROR, t.FormatError(err))
	}
	return m, nil
}

func (t *Themis) GetPeerPoolItem(pubKey string) (*dns.PeerPoolItem, error) {
	item, err := t.sdk.Native.Dns.GetPeerPoolItem(pubKey)
	if err != nil {
		return nil, sdkErr.NewWithError(sdkErr.CHAIN_ERROR, t.FormatError(err))
	}
	return item, nil
}
