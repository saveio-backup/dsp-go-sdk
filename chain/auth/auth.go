package auth

import (
	"github.com/oniio/dsp-go-sdk/chain/account"
	"github.com/oniio/dsp-go-sdk/chain/client"
	"github.com/oniio/dsp-go-sdk/chain/utils"
	"github.com/oniio/oniChain/common"
	"github.com/oniio/oniChain/core/types"
)

var (
	AUTH_CONTRACT_ADDRESS, _ = utils.AddressFromHexString("0600000000000000000000000000000000000000")
	AUTH_CONTRACT_VERSION    = byte(0)
)

type Auth struct {
	Client *client.ClientMgr
}

func (this *Auth) NewAssignFuncsToRoleTransaction(gasPrice, gasLimit uint64, contractAddress common.Address, adminId, role []byte, funcNames []string, keyIndex int) (*types.MutableTransaction, error) {
	return utils.NewNativeInvokeTransaction(
		gasPrice,
		gasLimit,
		AUTH_CONTRACT_VERSION,
		AUTH_CONTRACT_ADDRESS,
		"assignFuncsToRole",
		[]interface{}{
			contractAddress,
			adminId,
			role,
			funcNames,
			keyIndex,
		})
}

func (this *Auth) AssignFuncsToRole(gasPrice, gasLimit uint64, contractAddress common.Address, signer *account.Account, adminId, role []byte, funcNames []string, keyIndex int) (common.Uint256, error) {
	tx, err := this.NewAssignFuncsToRoleTransaction(gasPrice, gasLimit, contractAddress, adminId, role, funcNames, keyIndex)
	if err != nil {
		return common.UINT256_EMPTY, err
	}
	err = utils.SignToTransaction(tx, signer)
	if err != nil {
		return common.UINT256_EMPTY, err
	}
	return this.Client.SendTransaction(tx)
}

func (this *Auth) NewDelegateTransaction(gasPrice, gasLimit uint64, contractAddress common.Address, from, to, role []byte, period, level, keyIndex int) (*types.MutableTransaction, error) {
	return utils.NewNativeInvokeTransaction(
		gasPrice,
		gasLimit,
		AUTH_CONTRACT_VERSION,
		AUTH_CONTRACT_ADDRESS,
		"delegate",
		[]interface{}{
			contractAddress,
			from,
			to,
			role,
			period,
			level,
			keyIndex,
		})
}

func (this *Auth) Delegate(gasPrice, gasLimit uint64, signer *account.Account, contractAddress common.Address, from, to, role []byte, period, level, keyIndex int) (common.Uint256, error) {
	tx, err := this.NewDelegateTransaction(gasPrice, gasLimit, contractAddress, from, to, role, period, level, keyIndex)
	if err != nil {
		return common.UINT256_EMPTY, err
	}
	err = utils.SignToTransaction(tx, signer)
	if err != nil {
		return common.UINT256_EMPTY, err
	}
	return this.Client.SendTransaction(tx)
}

func (this *Auth) NewWithdrawTransaction(gasPrice, gasLimit uint64, contractAddress common.Address, initiator, delegate, role []byte, keyIndex int) (*types.MutableTransaction, error) {
	return utils.NewNativeInvokeTransaction(
		gasPrice,
		gasLimit,
		AUTH_CONTRACT_VERSION,
		AUTH_CONTRACT_ADDRESS,
		"withdraw",
		[]interface{}{
			contractAddress,
			initiator,
			delegate,
			role,
			keyIndex,
		})
}

func (this *Auth) Withdraw(gasPrice, gasLimit uint64, signer *account.Account, contractAddress common.Address, initiator, delegate, role []byte, keyIndex int) (common.Uint256, error) {
	tx, err := this.NewWithdrawTransaction(gasPrice, gasLimit, contractAddress, initiator, delegate, role, keyIndex)
	if err != nil {
		return common.UINT256_EMPTY, err
	}
	err = utils.SignToTransaction(tx, signer)
	if err != nil {
		return common.UINT256_EMPTY, err
	}
	return this.Client.SendTransaction(tx)
}

func (this *Auth) NewAssignOntIDsToRoleTransaction(gasPrice, gasLimit uint64, contractAddress common.Address, admontId, role []byte, persons [][]byte, keyIndex int) (*types.MutableTransaction, error) {
	return utils.NewNativeInvokeTransaction(
		gasPrice,
		gasLimit,
		AUTH_CONTRACT_VERSION,
		AUTH_CONTRACT_ADDRESS,
		"assignOntIDsToRole",
		[]interface{}{
			contractAddress,
			admontId,
			role,
			persons,
			keyIndex,
		})
}

func (this *Auth) AssignOntIDsToRole(gasPrice, gasLimit uint64, signer *account.Account, contractAddress common.Address, admontId, role []byte, persons [][]byte, keyIndex int) (common.Uint256, error) {
	tx, err := this.NewAssignOntIDsToRoleTransaction(gasPrice, gasLimit, contractAddress, admontId, role, persons, keyIndex)
	if err != nil {
		return common.UINT256_EMPTY, err
	}
	err = utils.SignToTransaction(tx, signer)
	if err != nil {
		return common.UINT256_EMPTY, err
	}
	return this.Client.SendTransaction(tx)
}

func (this *Auth) NewTransferTransaction(gasPrice, gasLimit uint64, contractAddress common.Address, newAdminId []byte, keyIndex int) (*types.MutableTransaction, error) {
	return utils.NewNativeInvokeTransaction(
		gasPrice,
		gasLimit,
		AUTH_CONTRACT_VERSION,
		AUTH_CONTRACT_ADDRESS,
		"transfer",
		[]interface{}{
			contractAddress,
			newAdminId,
			keyIndex,
		})
}

func (this *Auth) Transfer(gasPrice, gasLimit uint64, signer *account.Account, contractAddress common.Address, newAdminId []byte, keyIndex int) (common.Uint256, error) {
	tx, err := this.NewTransferTransaction(gasPrice, gasLimit, contractAddress, newAdminId, keyIndex)
	if err != nil {
		return common.UINT256_EMPTY, err
	}
	err = utils.SignToTransaction(tx, signer)
	if err != nil {
		return common.UINT256_EMPTY, err
	}
	return this.Client.SendTransaction(tx)
}

func (this *Auth) NewVerifyTokenTransaction(gasPrice, gasLimit uint64, contractAddress common.Address, caller []byte, funcName string, keyIndex int) (*types.MutableTransaction, error) {
	return utils.NewNativeInvokeTransaction(
		gasPrice,
		gasLimit,
		AUTH_CONTRACT_VERSION,
		AUTH_CONTRACT_ADDRESS,
		"verifyToken",
		[]interface{}{
			contractAddress,
			caller,
			funcName,
			keyIndex,
		})
}

func (this *Auth) VerifyToken(gasPrice, gasLimit uint64, signer *account.Account, contractAddress common.Address, caller []byte, funcName string, keyIndex int) (common.Uint256, error) {
	tx, err := this.NewVerifyTokenTransaction(gasPrice, gasLimit, contractAddress, caller, funcName, keyIndex)
	if err != nil {
		return common.UINT256_EMPTY, err
	}
	err = utils.SignToTransaction(tx, signer)
	if err != nil {
		return common.UINT256_EMPTY, err
	}
	return this.Client.SendTransaction(tx)
}
