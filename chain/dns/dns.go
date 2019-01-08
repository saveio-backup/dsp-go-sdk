package dns

import (
	"bytes"
	"errors"
	"strings"

	"github.com/oniio/dsp-go-sdk/chain/account"
	"github.com/oniio/dsp-go-sdk/chain/client"
	sdkcom "github.com/oniio/dsp-go-sdk/chain/common"
	"github.com/oniio/dsp-go-sdk/chain/utils"
	"github.com/oniio/oniChain/common"
	"github.com/oniio/oniChain/smartcontract/service/native/dns"
)

var (
	DNS_CONTRACT_ADDRESS, _ = utils.AddressFromHexString("1200000000000000000000000000000000000000")
	DNS_CONTRACT_VERSION    = byte(0)
)

type Dns struct {
	Client *client.ClientMgr
	DefAcc *account.Account
}

func (this *Dns) InvokeNativeContract(signer *account.Account, method string, params []interface{}) (common.Uint256, error) {
	if signer == nil {
		return common.UINT256_EMPTY, errors.New("signer is nil")
	}
	tx, err := utils.NewNativeInvokeTransaction(sdkcom.GAS_PRICE, sdkcom.GAS_LIMIT, DNS_CONTRACT_VERSION, DNS_CONTRACT_ADDRESS, method, params)
	if err != nil {
		return common.UINT256_EMPTY, err
	}
	err = utils.SignToTransaction(tx, signer)
	if err != nil {
		return common.UINT256_EMPTY, err
	}
	return this.Client.SendTransaction(tx)
}

func (this *Dns) PreExecInvokeNativeContract(method string, params []interface{}) (*sdkcom.PreExecResult, error) {
	tx, err := utils.NewNativeInvokeTransaction(0, 0, DNS_CONTRACT_VERSION, DNS_CONTRACT_ADDRESS, method, params)
	if err != nil {
		return nil, err
	}
	return this.Client.PreExecTransaction(tx)
}

func (this *Dns) RegisterUrl(url string, rType uint64, name, desc string, ttl uint64) (common.Uint256, error) {
	if this.DefAcc == nil {
		return common.UINT256_EMPTY, errors.New("account is nil")
	}
	var req dns.RequestName
	if rType == dns.SYSTEM {
		req = dns.RequestName{
			Type:      rType,
			Header:    []byte{0},
			URL:       []byte{0},
			Name:      []byte(name),
			NameOwner: this.DefAcc.Address,
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
			NameOwner: this.DefAcc.Address,
			Desc:      []byte(desc),
			DesireTTL: ttl,
		}
	}
	ret, err := this.InvokeNativeContract(this.DefAcc, dns.REGISTER_NAME, []interface{}{req})
	if err != nil {
		return common.UINT256_EMPTY, err
	}
	return ret, nil
}

func (this *Dns) RegisterHeader(header, desc string, ttl uint64) (common.Uint256, error) {
	if this.DefAcc == nil {
		return common.UINT256_EMPTY, errors.New("account is nil")
	}
	req := dns.RequestHeader{
		Header:    []byte(header),
		NameOwner: this.DefAcc.Address,
		Desc:      []byte(desc),
		DesireTTL: ttl,
	}
	ret, err := this.InvokeNativeContract(this.DefAcc, dns.REGISTER_HEADER, []interface{}{req})
	if err != nil {
		return common.UINT256_EMPTY, err
	}
	return ret, nil
}
func (this *Dns) Binding(url string, name, desc string, ttl uint64) (common.Uint256, error) {
	if this.DefAcc == nil {
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
		NameOwner: this.DefAcc.Address,
		Desc:      []byte(desc),
		DesireTTL: ttl,
	}
	ret, err := this.InvokeNativeContract(this.DefAcc, dns.UPDATE_DNS_NAME, []interface{}{req})
	if err != nil {
		return common.UINT256_EMPTY, err
	}
	return ret, nil
}

func (this *Dns) DeleteUrl(url string) (common.Uint256, error) {
	if this.DefAcc == nil {
		return common.UINT256_EMPTY, errors.New("account is nil")
	}
	strs := strings.Split(url, "://")
	if len(strs) != 2 {
		return common.UINT256_EMPTY, errors.New("QueryUrl input url format valid")
	}
	req := dns.ReqInfo{
		Header: []byte(strs[0]),
		URL:    []byte(strs[1]),
		Owner:  this.DefAcc.Address,
	}
	ret, err := this.InvokeNativeContract(this.DefAcc, dns.DEL_DNS, []interface{}{req})
	if err != nil {
		return common.UINT256_EMPTY, err
	}
	return ret, nil
}
func (this *Dns) DeleteHeader(header string) (common.Uint256, error) {
	if this.DefAcc == nil {
		return common.UINT256_EMPTY, errors.New("account is nil")
	}
	req := dns.ReqInfo{
		Header: []byte(header),
		Owner:  this.DefAcc.Address,
	}
	ret, err := this.InvokeNativeContract(this.DefAcc, dns.DEL_DNS_HEADER, []interface{}{req})
	if err != nil {
		return common.UINT256_EMPTY, err
	}
	return ret, nil
}
func (this *Dns) TransferUrl(url string, toAdder string) (common.Uint256, error) {
	if this.DefAcc == nil {
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
		From:   this.DefAcc.Address,
		To:     to,
	}
	ret, err := this.InvokeNativeContract(this.DefAcc, dns.TRANSFER_NAME, []interface{}{req})
	if err != nil {
		return common.UINT256_EMPTY, err
	}
	return ret, nil
}
func (this *Dns) TransferHeader(header, toAdder string) (common.Uint256, error) {
	if this.DefAcc == nil {
		return common.UINT256_EMPTY, errors.New("account is nil")
	}
	to, err := common.AddressFromHexString(toAdder)
	req := dns.TranferInfo{
		Header: []byte(header),
		From:   this.DefAcc.Address,
		To:     to,
	}
	ret, err := this.InvokeNativeContract(this.DefAcc, dns.TRANSFER_HEADER_NAME, []interface{}{req})
	if err != nil {
		return common.UINT256_EMPTY, err
	}
	return ret, err
}

func (this *Dns) QueryUrl(url string, ownerAddr common.Address) (*dns.NameInfo, error) {
	strs := strings.Split(url, "://")
	if len(strs) != 2 {
		return nil, errors.New("QueryUrl input url format valid")
	}
	req := dns.ReqInfo{
		Header: []byte(strs[0]),
		URL:    []byte(strs[1]),
		Owner:  ownerAddr,
	}
	ret, err := this.PreExecInvokeNativeContract(
		dns.GET_DNS_NAME, []interface{}{req},
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

func (this *Dns) QueryHeader(header string, ownerAddr common.Address) (*dns.HeaderInfo, error) {
	req := dns.ReqInfo{
		Header: []byte(header),
		Owner:  ownerAddr,
	}

	ret, err := this.PreExecInvokeNativeContract(
		dns.GET_HEADER_NAME, []interface{}{req},
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
