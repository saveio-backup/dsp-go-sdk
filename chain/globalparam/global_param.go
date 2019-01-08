package global_param

import (
	"bytes"

	"github.com/oniio/dsp-go-sdk/chain/account"
	"github.com/oniio/dsp-go-sdk/chain/client"
	sdkcom "github.com/oniio/dsp-go-sdk/chain/common"
	"github.com/oniio/dsp-go-sdk/chain/utils"
	"github.com/oniio/oniChain/common"
	"github.com/oniio/oniChain/core/types"
	"github.com/oniio/oniChain/smartcontract/service/native/global_params"
)

var (
	GLOABL_PARAMS_CONTRACT_ADDRESS, _ = utils.AddressFromHexString("0400000000000000000000000000000000000000")
	GLOBAL_PARAMS_CONTRACT_VERSION    = byte(0)
)

type GlobalParam struct {
	Client *client.ClientMgr
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

func (this *GlobalParam) NewSetGlobalParamsTransaction(gasPrice, gasLimit uint64, params map[string]string) (*types.MutableTransaction, error) {
	var globalParams global_params.Params
	for k, v := range params {
		globalParams.SetParam(global_params.Param{Key: k, Value: v})
	}
	return utils.NewNativeInvokeTransaction(
		gasPrice,
		gasLimit,
		GLOBAL_PARAMS_CONTRACT_VERSION,
		GLOABL_PARAMS_CONTRACT_ADDRESS,
		global_params.SET_GLOBAL_PARAM_NAME,
		[]interface{}{globalParams})
}

func (this *GlobalParam) SetGlobalParams(gasPrice, gasLimit uint64, signer *account.Account, params map[string]string) (common.Uint256, error) {
	tx, err := this.NewSetGlobalParamsTransaction(gasPrice, gasLimit, params)
	if err != nil {
		return common.UINT256_EMPTY, err
	}
	err = utils.SignToTransaction(tx, signer)
	if err != nil {
		return common.UINT256_EMPTY, err
	}
	return this.Client.SendTransaction(tx)
}

func (this *GlobalParam) NewTransferAdminTransaction(gasPrice, gasLimit uint64, newAdmin common.Address) (*types.MutableTransaction, error) {
	return utils.NewNativeInvokeTransaction(
		gasPrice,
		gasLimit,
		GLOBAL_PARAMS_CONTRACT_VERSION,
		GLOABL_PARAMS_CONTRACT_ADDRESS,
		global_params.TRANSFER_ADMIN_NAME,
		[]interface{}{newAdmin})
}

func (this *GlobalParam) TransferAdmin(gasPrice, gasLimit uint64, signer *account.Account, newAdmin common.Address) (common.Uint256, error) {
	tx, err := this.NewTransferAdminTransaction(gasPrice, gasLimit, newAdmin)
	if err != nil {
		return common.UINT256_EMPTY, err
	}
	err = utils.SignToTransaction(tx, signer)
	if err != nil {
		return common.UINT256_EMPTY, err
	}
	return this.Client.SendTransaction(tx)
}

func (this *GlobalParam) NewAcceptAdminTransaction(gasPrice, gasLimit uint64, admin common.Address) (*types.MutableTransaction, error) {
	return utils.NewNativeInvokeTransaction(
		gasPrice,
		gasLimit,
		GLOBAL_PARAMS_CONTRACT_VERSION,
		GLOABL_PARAMS_CONTRACT_ADDRESS,
		global_params.ACCEPT_ADMIN_NAME,
		[]interface{}{admin})
}

func (this *GlobalParam) AcceptAdmin(gasPrice, gasLimit uint64, signer *account.Account) (common.Uint256, error) {
	tx, err := this.NewAcceptAdminTransaction(gasPrice, gasLimit, signer.Address)
	if err != nil {
		return common.UINT256_EMPTY, err
	}
	err = utils.SignToTransaction(tx, signer)
	if err != nil {
		return common.UINT256_EMPTY, err
	}
	return this.Client.SendTransaction(tx)
}

func (this *GlobalParam) NewSetOperatorTransaction(gasPrice, gasLimit uint64, operator common.Address) (*types.MutableTransaction, error) {
	return utils.NewNativeInvokeTransaction(
		gasPrice,
		gasLimit,
		GLOBAL_PARAMS_CONTRACT_VERSION,
		GLOABL_PARAMS_CONTRACT_ADDRESS,
		global_params.SET_OPERATOR,
		[]interface{}{operator},
	)
}

func (this *GlobalParam) SetOperator(gasPrice, gasLimit uint64, signer *account.Account, operator common.Address) (common.Uint256, error) {
	tx, err := this.NewSetOperatorTransaction(gasPrice, gasLimit, operator)
	if err != nil {
		return common.UINT256_EMPTY, err
	}
	err = utils.SignToTransaction(tx, signer)
	if err != nil {
		return common.UINT256_EMPTY, err
	}
	return this.Client.SendTransaction(tx)
}

func (this *GlobalParam) NewCreateSnapshotTransaction(gasPrice, gasLimit uint64) (*types.MutableTransaction, error) {
	return utils.NewNativeInvokeTransaction(
		gasPrice,
		gasLimit,
		GLOBAL_PARAMS_CONTRACT_VERSION,
		GLOABL_PARAMS_CONTRACT_ADDRESS,
		global_params.CREATE_SNAPSHOT_NAME,
		[]interface{}{},
	)
}

func (this *GlobalParam) CreateSnapshot(gasPrice, gasLimit uint64, signer *account.Account) (common.Uint256, error) {
	tx, err := this.NewCreateSnapshotTransaction(gasPrice, gasLimit)
	if err != nil {
		return common.UINT256_EMPTY, err
	}
	err = utils.SignToTransaction(tx, signer)
	if err != nil {
		return common.UINT256_EMPTY, err
	}
	return this.Client.SendTransaction(tx)
}
