package contract

import (
	"bytes"
	"errors"
	"strings"

	chain "github.com/oniio/dsp-go-sdk/chain"
	"github.com/oniio/oniChain/common"
	dns "github.com/oniio/oniChain/smartcontract/service/native/dns"
	"github.com/oniio/oniChain/smartcontract/service/native/utils"
)

var ChainSdk *chain.ChainSdk
var AccClient *chain.Account
var OntRpcSrvAddr string

const (
	GasPrice        uint64 = 500
	GasLimit        uint64 = 20000
	contractVersion byte   = 0
)

var contractAddr = utils.OntDNSAddress

func InitDNS(client *chain.Account, sdk *chain.ChainSdk, rpcSvrAddr string) {
	ChainSdk = sdk
	ChainSdk.NewRpcClient().SetAddress(rpcSvrAddr)
	AccClient = client
}

func RegisterUrl(url string, rType uint64, name, desc string, ttl uint64) (common.Uint256, error) {
	if AccClient == nil {
		return common.UINT256_EMPTY, errors.New("account is nil")
	}
	var req dns.RequestName
	if rType == dns.SYSTEM {
		req = dns.RequestName{
			Type:      rType,
			Header:    []byte{0},
			URL:       []byte{0},
			Name:      []byte(name),
			NameOwner: AccClient.Address,
			Desc:      []byte(desc),
			DesireTTL: ttl,
		}
	} else {
		strs := strings.Split(url, "://")
		if len(strs) != 2 {
			return common.UINT256_EMPTY, errors.New("QueryUrl input url format invalid")
		}
		req = dns.RequestName{
			Type:      rType,
			Header:    []byte(strs[0]),
			URL:       []byte(strs[1]),
			Name:      []byte(name),
			NameOwner: AccClient.Address,
			Desc:      []byte(desc),
			DesireTTL: ttl,
		}
	}

	ret, err := ChainSdk.Native.InvokeNativeContract(GasPrice, GasLimit, AccClient,
		contractVersion, contractAddr, dns.REGISTER_NAME, []interface{}{req},
	)
	if err != nil {
		return common.UINT256_EMPTY, err
	}
	return ret, nil

}
func RegisterHeader(header, desc string, ttl uint64) (common.Uint256, error) {
	if AccClient == nil {
		return common.UINT256_EMPTY, errors.New("account is nil")
	}
	req := dns.RequestHeader{
		Header:    []byte(header),
		NameOwner: AccClient.Address,
		Desc:      []byte(desc),
		DesireTTL: ttl,
	}
	ret, err := ChainSdk.Native.InvokeNativeContract(GasPrice, GasLimit, AccClient,
		contractVersion, contractAddr, dns.REGISTER_HEADER, []interface{}{req},
	)
	if err != nil {
		return common.UINT256_EMPTY, err
	}
	return ret, nil
}
func Binding(url string, name, desc string, ttl uint64) (common.Uint256, error) {
	if AccClient == nil {
		return common.UINT256_EMPTY, errors.New("account is nil")
	}
	strs := strings.Split(url, "://")
	if len(strs) != 2 {
		return common.UINT256_EMPTY, errors.New("QueryUrl input url format valid")
	}
	req := dns.RequestName{
		Header:    []byte(strs[0]),
		URL:       []byte(strs[1]),
		Name:      []byte(name),
		NameOwner: AccClient.Address,
		Desc:      []byte(desc),
		DesireTTL: ttl,
	}
	ret, err := ChainSdk.Native.InvokeNativeContract(GasPrice, GasLimit, AccClient,
		contractVersion, contractAddr, dns.UPDATE_DNS_NAME, []interface{}{req},
	)
	if err != nil {
		return common.UINT256_EMPTY, err
	}
	return ret, nil
}

func DeleteUrl(url string) (common.Uint256, error) {
	if AccClient == nil {
		return common.UINT256_EMPTY, errors.New("account is nil")
	}
	strs := strings.Split(url, "://")
	if len(strs) != 2 {
		return common.UINT256_EMPTY, errors.New("QueryUrl input url format valid")
	}
	req := dns.ReqInfo{
		Header: []byte(strs[0]),
		URL:    []byte(strs[1]),
		Owner:  AccClient.Address,
	}
	ret, err := ChainSdk.Native.InvokeNativeContract(GasPrice, GasLimit, AccClient,
		contractVersion, contractAddr, dns.DEL_DNS, []interface{}{req},
	)
	if err != nil {
		return common.UINT256_EMPTY, err
	}
	return ret, nil
}
func DeleteHeader(header string) (common.Uint256, error) {
	if AccClient == nil {
		return common.UINT256_EMPTY, errors.New("account is nil")
	}
	req := dns.ReqInfo{
		Header: []byte(header),
		Owner:  AccClient.Address,
	}
	ret, err := ChainSdk.Native.InvokeNativeContract(GasPrice, GasLimit, AccClient,
		contractVersion, contractAddr, dns.DEL_DNS_HEADER, []interface{}{req},
	)
	if err != nil {
		return common.UINT256_EMPTY, err
	}
	return ret, nil
}
func TransferUrl(url string, toAdder string) (common.Uint256, error) {
	if AccClient == nil {
		return common.UINT256_EMPTY, errors.New("account is nil")
	}
	strs := strings.Split(url, "://")
	if len(strs) != 2 {
		return common.UINT256_EMPTY, errors.New("QueryUrl input url format valid")
	}
	to, err := common.AddressFromHexString(toAdder)
	req := dns.TranferInfo{
		Header: []byte(strs[0]),
		URL:    []byte(strs[1]),
		From:   AccClient.Address,
		To:     to,
	}
	ret, err := ChainSdk.Native.InvokeNativeContract(GasPrice, GasLimit, AccClient,
		contractVersion, contractAddr, dns.TRANSFER_NAME, []interface{}{req},
	)
	if err != nil {
		return common.UINT256_EMPTY, err
	}
	return ret, nil
}
func TransferHeader(header, toAdder string) (common.Uint256, error) {
	if AccClient == nil {
		return common.UINT256_EMPTY, errors.New("account is nil")
	}
	to, err := common.AddressFromHexString(toAdder)
	req := dns.TranferInfo{
		Header: []byte(header),
		From:   AccClient.Address,
		To:     to,
	}
	ret, err := ChainSdk.Native.InvokeNativeContract(GasPrice, GasLimit, AccClient,
		contractVersion, contractAddr, dns.TRANSFER_HEADER_NAME, []interface{}{req},
	)
	if err != nil {
		return common.UINT256_EMPTY, err
	}
	return ret, err
}

func QueryUrl(url string) (*dns.NameInfo, error) {
	strs := strings.Split(url, "://")
	if len(strs) != 2 {
		return nil, errors.New("QueryUrl input url format valid")
	}
	req := dns.ReqInfo{
		Header: []byte(strs[0]),
		URL:    []byte(strs[1]),
		Owner:  AccClient.Address,
	}
	ret, err := ChainSdk.Native.PreExecInvokeNativeContract(
		contractAddr, contractVersion, dns.GET_DNS_NAME, []interface{}{req},
	)
	if err != nil {
		return nil, err
	}
	data, err := ret.Result.ToByteArray()
	if err != nil {
		return nil, errors.New("QueryUrl result toByteArray err")
	}

	var name dns.NameInfo
	infoReader := bytes.NewReader(data)
	err = name.Deserialize(infoReader)
	if err != nil {
		return nil, errors.New("NameInfo deserialize error")
	}
	return &name, nil
}

func QueryHeader(header string) (*dns.HeaderInfo, error) {
	req := dns.ReqInfo{
		Header: []byte(header),
		Owner:  AccClient.Address,
	}
	ret, err := ChainSdk.Native.PreExecInvokeNativeContract(
		contractAddr, contractVersion, dns.GET_HEADER_NAME, []interface{}{req},
	)
	if err != nil {
		return nil, err
	}
	data, err := ret.Result.ToByteArray()
	if err != nil {
		return nil, errors.New("QueryHeader result toByteArray err")
	}

	var headerInfo dns.HeaderInfo
	infoReader := bytes.NewReader(data)
	err = headerInfo.Deserialize(infoReader)
	if err != nil {
		return nil, errors.New("HeaderInfo deserialize error")
	}
	return &headerInfo, nil

}
