package global_param

import (
	"bytes"
	"errors"

	"github.com/oniio/dsp-go-sdk/chain/account"
	"github.com/oniio/dsp-go-sdk/chain/client"
	sdkcom "github.com/oniio/dsp-go-sdk/chain/common"
	"github.com/oniio/dsp-go-sdk/chain/utils"
	"github.com/oniio/oniChain/common"
	"github.com/oniio/oniChain/smartcontract/service/native/global_params"
)

var (
	GLOABL_PARAMS_CONTRACT_ADDRESS, _ = utils.AddressFromHexString("0400000000000000000000000000000000000000")
	GLOBAL_PARAMS_CONTRACT_VERSION    = byte(0)
)

type GlobalParam struct {
	Client *client.ClientMgr
}

func (this *GlobalParam) InvokeNativeContract(signer *account.Account, method string, params []interface{}) (common.Uint256, error) {
	if signer == nil {
		return common.UINT256_EMPTY, errors.New("signer is nil")
	}
	tx, err := utils.NewNativeInvokeTransaction(sdkcom.GAS_PRICE, sdkcom.GAS_LIMIT, GLOBAL_PARAMS_CONTRACT_VERSION, GLOABL_PARAMS_CONTRACT_ADDRESS, method, params)
	if err != nil {
		return common.UINT256_EMPTY, err
	}
	err = utils.SignToTransaction(tx, signer)
	if err != nil {
		return common.UINT256_EMPTY, err
	}
	return this.Client.SendTransaction(tx)
}

func (this *GlobalParam) PreExecInvokeNativeContract(
	method string,
	params []interface{},
) (*sdkcom.PreExecResult, error) {
	tx, err := utils.NewNativeInvokeTransaction(0, 0, GLOBAL_PARAMS_CONTRACT_VERSION, GLOABL_PARAMS_CONTRACT_ADDRESS, method, params)
	if err != nil {
		return nil, err
	}
	return this.Client.PreExecTransaction(tx)
}

func (this *GlobalParam) GetGlobalParams(params []string) (map[string]string, error) {
	preResult, err := this.PreExecInvokeNativeContract(
		global_params.GET_GLOBAL_PARAM_NAME,
		[]interface{}{params})
	if err != nil {
		return nil, err
	}
	results, err := preResult.Result.ToByteArray()
	if err != nil {
		return nil, err
	}
	queryParams := new(global_params.Params)
	err = queryParams.Deserialize(bytes.NewBuffer(results))
	if err != nil {
		return nil, err
	}
	globalParams := make(map[string]string, len(params))
	for _, param := range params {
		index, values := queryParams.GetParam(param)
		if index < 0 {
			continue
		}
		globalParams[param] = values.Value
	}
	return globalParams, nil
}

func (this *GlobalParam) SetGlobalParams(gasPrice, gasLimit uint64, signer *account.Account, params map[string]string) (common.Uint256, error) {
	var globalParams global_params.Params
	for k, v := range params {
		globalParams.SetParam(global_params.Param{Key: k, Value: v})
	}
	return this.InvokeNativeContract(signer, global_params.SET_GLOBAL_PARAM_NAME, []interface{}{globalParams})
}

func (this *GlobalParam) TransferAdmin(gasPrice, gasLimit uint64, signer *account.Account, newAdmin common.Address) (common.Uint256, error) {
	return this.InvokeNativeContract(signer, global_params.TRANSFER_ADMIN_NAME, []interface{}{newAdmin})
}

func (this *GlobalParam) AcceptAdmin(gasPrice, gasLimit uint64, signer *account.Account) (common.Uint256, error) {
	return this.InvokeNativeContract(signer, global_params.ACCEPT_ADMIN_NAME, []interface{}{signer.Address})
}

func (this *GlobalParam) SetOperator(gasPrice, gasLimit uint64, signer *account.Account, operator common.Address) (common.Uint256, error) {
	return this.InvokeNativeContract(signer, global_params.SET_OPERATOR, []interface{}{operator})
}

func (this *GlobalParam) CreateSnapshot(gasPrice, gasLimit uint64, signer *account.Account) (common.Uint256, error) {
	return this.InvokeNativeContract(signer, global_params.CREATE_SNAPSHOT_NAME, []interface{}{})
}
