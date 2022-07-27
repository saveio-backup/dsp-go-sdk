package ethereum

import (
	"encoding/hex"
	sdkErr "github.com/saveio/dsp-go-sdk/error"
	chainCom "github.com/saveio/themis/common"
	"github.com/saveio/themis/smartcontract/service/native/dns"
)

func (e *Ethereum) GetAllDnsNodes() (map[string]dns.DNSNodeInfo, error) {
	info, err := e.sdk.EVM.Dns.GetAllDnsNodes()
	if err != nil {
		return nil, sdkErr.NewWithError(sdkErr.CHAIN_ERROR, e.FormatError(err))
	}
	return info, nil
}

func (e *Ethereum) QueryPluginsInfo() (*dns.NameInfoList, error) {
	info, err := e.sdk.EVM.Dns.GetPluginList()
	if err != nil {
		return nil, sdkErr.NewWithError(sdkErr.CHAIN_ERROR, e.FormatError(err))
	}
	return info, nil
}

func (e *Ethereum) RegisterHeader(header, desc string, ttl uint64) (string, error) {
	txHash, err := e.sdk.EVM.Dns.RegisterHeader(header, desc, ttl)
	if err != nil {
		return "", sdkErr.NewWithError(sdkErr.CHAIN_ERROR, e.FormatError(err))
	}
	tx := hex.EncodeToString(chainCom.ToArrayReverse(txHash[:]))
	return tx, nil
}

func (e *Ethereum) RegisterUrl(url string, rType uint64, name, desc string, ttl uint64) (string, error) {
	txHash, err := e.sdk.EVM.Dns.RegisterUrl(url, rType, name, desc, ttl)
	if err != nil {
		return "", sdkErr.NewWithError(sdkErr.CHAIN_ERROR, e.FormatError(err))
	}
	tx := hex.EncodeToString(chainCom.ToArrayReverse(txHash[:]))
	return tx, nil
}

func (e *Ethereum) BindUrl(urlType uint64, url string, name, desc string, ttl uint64) (string, error) {
	txHash, err := e.sdk.EVM.Dns.Binding(urlType, url, name, desc, ttl)
	if err != nil {
		return "", sdkErr.NewWithError(sdkErr.CHAIN_ERROR, e.FormatError(err))
	}
	tx := hex.EncodeToString(chainCom.ToArrayReverse(txHash[:]))
	return tx, nil
}

func (e *Ethereum) DeleteUrl(url string) (string, error) {
	txHash, err := e.sdk.EVM.Dns.DeleteUrl(url)
	if err != nil {
		return "", sdkErr.NewWithError(sdkErr.CHAIN_ERROR, e.FormatError(err))
	}
	tx := hex.EncodeToString(chainCom.ToArrayReverse(txHash[:]))
	return tx, nil
}

func (e *Ethereum) QueryUrl(url string, ownerAddr chainCom.Address) (*dns.NameInfo, error) {
	info, err := e.sdk.EVM.Dns.QueryUrl(url, ownerAddr)
	if err != nil {
		return nil, sdkErr.NewWithError(sdkErr.CHAIN_ERROR, e.FormatError(err))
	}
	return info, nil
}

func (e *Ethereum) GetDnsNodeByAddr(wallet chainCom.Address) (*dns.DNSNodeInfo, error) {
	info, err := e.sdk.EVM.Dns.GetDnsNodeByAddr(wallet)
	if err != nil {
		return nil, sdkErr.NewWithError(sdkErr.CHAIN_ERROR, e.FormatError(err))
	}
	return info, nil
}

func (e *Ethereum) DNSNodeReg(ip, port []byte, initPos uint64) (string, error) {
	txHash, err := e.sdk.EVM.Dns.DNSNodeReg(ip, port, initPos)
	if err != nil {
		return "", sdkErr.NewWithError(sdkErr.CHAIN_ERROR, e.FormatError(err))
	}
	tx := hex.EncodeToString(chainCom.ToArrayReverse(txHash[:]))
	return tx, nil
}

func (e *Ethereum) UnregisterDNSNode() (string, error) {
	txHash, err := e.sdk.EVM.Dns.UnregisterDNSNode()
	if err != nil {
		return "", sdkErr.NewWithError(sdkErr.CHAIN_ERROR, e.FormatError(err))
	}
	tx := hex.EncodeToString(chainCom.ToArrayReverse(txHash[:]))
	return tx, nil
}

func (e *Ethereum) QuitNode() (string, error) {
	txHash, err := e.sdk.EVM.Dns.QuitNode()
	if err != nil {
		return "", sdkErr.NewWithError(sdkErr.CHAIN_ERROR, e.FormatError(err))
	}
	tx := hex.EncodeToString(chainCom.ToArrayReverse(txHash[:]))
	return tx, nil
}

func (e *Ethereum) AddInitPos(addPosAmount uint64) (string, error) {
	txHash, err := e.sdk.EVM.Dns.AddInitPos(addPosAmount)
	if err != nil {
		return "", sdkErr.NewWithError(sdkErr.CHAIN_ERROR, e.FormatError(err))
	}
	tx := hex.EncodeToString(chainCom.ToArrayReverse(txHash[:]))
	return tx, nil
}

func (e *Ethereum) ReduceInitPos(changePosAmount uint64) (string, error) {
	txHash, err := e.sdk.EVM.Dns.ReduceInitPos(changePosAmount)
	if err != nil {
		return "", sdkErr.NewWithError(sdkErr.CHAIN_ERROR, e.FormatError(err))
	}
	tx := hex.EncodeToString(chainCom.ToArrayReverse(txHash[:]))
	return tx, nil
}

func (e *Ethereum) GetPeerPoolMap() (*dns.PeerPoolMap, error) {
	m, err := e.sdk.EVM.Dns.GetPeerPoolMap()
	if err != nil {
		return nil, sdkErr.NewWithError(sdkErr.CHAIN_ERROR, e.FormatError(err))
	}
	return m, nil
}

func (e *Ethereum) GetPeerPoolItem(pubKey string) (*dns.PeerPoolItem, error) {
	item, err := e.sdk.EVM.Dns.GetPeerPoolItem(pubKey)
	if err != nil {
		return nil, sdkErr.NewWithError(sdkErr.CHAIN_ERROR, e.FormatError(err))
	}
	return item, nil
}
