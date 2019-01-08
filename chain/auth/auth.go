package auth

import (
	"errors"

	"github.com/oniio/dsp-go-sdk/chain/account"
	"github.com/oniio/dsp-go-sdk/chain/client"
	sdkcom "github.com/oniio/dsp-go-sdk/chain/common"
	"github.com/oniio/dsp-go-sdk/chain/utils"
	"github.com/oniio/oniChain/common"
)

var (
	AUTH_CONTRACT_ADDRESS, _ = utils.AddressFromHexString("0600000000000000000000000000000000000000")
	AUTH_CONTRACT_VERSION    = byte(0)
)

type Auth struct {
	Client *client.ClientMgr
}

func (this *Auth) InvokeNativeContract(signer *account.Account, method string, params []interface{}) (common.Uint256, error) {
	if signer == nil {
		return common.UINT256_EMPTY, errors.New("signer is nil")
	}
	tx, err := utils.NewNativeInvokeTransaction(sdkcom.GAS_PRICE, sdkcom.GAS_LIMIT, AUTH_CONTRACT_VERSION, AUTH_CONTRACT_ADDRESS, method, params)
	if err != nil {
		return common.UINT256_EMPTY, err
	}
	err = utils.SignToTransaction(tx, signer)
	if err != nil {
		return common.UINT256_EMPTY, err
	}
	return this.Client.SendTransaction(tx)
}

func (this *Auth) PreExecInvokeNativeContract(method string, params []interface{}) (*sdkcom.PreExecResult, error) {
	tx, err := utils.NewNativeInvokeTransaction(0, 0, AUTH_CONTRACT_VERSION, AUTH_CONTRACT_ADDRESS, method, params)
	if err != nil {
		return nil, err
	}
	return this.Client.PreExecTransaction(tx)
}

func (this *Auth) AssignFuncsToRole(gasPrice, gasLimit uint64, contractAddress common.Address, signer *account.Account, adminId, role []byte, funcNames []string, keyIndex int) (common.Uint256, error) {
	return this.InvokeNativeContract(signer, "assignFuncsToRole", []interface{}{
		contractAddress,
		adminId,
		role,
		funcNames,
		keyIndex,
	})
}

func (this *Auth) Delegate(gasPrice, gasLimit uint64, signer *account.Account, contractAddress common.Address, from, to, role []byte, period, level, keyIndex int) (common.Uint256, error) {
	return this.InvokeNativeContract(signer, "delegate", []interface{}{
		contractAddress,
		from,
		to,
		role,
		period,
		level,
		keyIndex,
	})
}

func (this *Auth) Withdraw(gasPrice, gasLimit uint64, signer *account.Account, contractAddress common.Address, initiator, delegate, role []byte, keyIndex int) (common.Uint256, error) {
	return this.InvokeNativeContract(signer, "withdraw", []interface{}{
		contractAddress,
		initiator,
		delegate,
		role,
		keyIndex,
	})
}

func (this *Auth) AssignOntIDsToRole(gasPrice, gasLimit uint64, signer *account.Account, contractAddress common.Address, admontId, role []byte, persons [][]byte, keyIndex int) (common.Uint256, error) {
	return this.InvokeNativeContract(signer, "assignOntIDsToRole", []interface{}{
		contractAddress,
		admontId,
		role,
		persons,
		keyIndex,
	})
}

func (this *Auth) Transfer(gasPrice, gasLimit uint64, signer *account.Account, contractAddress common.Address, newAdminId []byte, keyIndex int) (common.Uint256, error) {
	return this.InvokeNativeContract(signer, "transfer", []interface{}{
		contractAddress,
		newAdminId,
		keyIndex,
	})
}

func (this *Auth) VerifyToken(gasPrice, gasLimit uint64, signer *account.Account, contractAddress common.Address, caller []byte, funcName string, keyIndex int) (common.Uint256, error) {
	return this.InvokeNativeContract(signer, "verifyToken", []interface{}{
		contractAddress,
		caller,
		funcName,
		keyIndex,
	})
}
