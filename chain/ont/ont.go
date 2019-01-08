package ont

import (
	"github.com/oniio/dsp-go-sdk/chain/client"
	sdkcom "github.com/oniio/dsp-go-sdk/chain/common"
	"github.com/oniio/dsp-go-sdk/chain/utils"
	"github.com/oniio/oniChain/account"
	"github.com/oniio/oniChain/common"
	"github.com/oniio/oniChain/core/types"
	"github.com/oniio/oniChain/smartcontract/service/native/ont"
)

var (
	ONT_CONTRACT_ADDRESS, _ = utils.AddressFromHexString("0100000000000000000000000000000000000000")
	ONT_CONTRACT_VERSION    = byte(0)
)

type Ont struct {
	Client *client.ClientMgr
}

func (this *Ont) PreExecInvokeNativeContract(method string, params []interface{}) (*sdkcom.PreExecResult, error) {
	tx, err := utils.NewNativeInvokeTransaction(0, 0, ONT_CONTRACT_VERSION, ONT_CONTRACT_ADDRESS, method, params)
	if err != nil {
		return nil, err
	}
	return this.Client.PreExecTransaction(tx)
}

func (this *Ont) NewTransferTransaction(gasPrice, gasLimit uint64, from, to common.Address, amount uint64) (*types.MutableTransaction, error) {
	state := &ont.State{
		From:  from,
		To:    to,
		Value: amount,
	}
	return this.NewMultiTransferTransaction(gasPrice, gasLimit, []*ont.State{state})
}

func (this *Ont) Transfer(gasPrice, gasLimit uint64, from *account.Account, to common.Address, amount uint64) (common.Uint256, error) {
	tx, err := this.NewTransferTransaction(gasPrice, gasLimit, from.Address, to, amount)
	if err != nil {
		return common.UINT256_EMPTY, err
	}
	err = utils.SignToTransaction(tx, from)
	if err != nil {
		return common.UINT256_EMPTY, err
	}
	return this.Client.SendTransaction(tx)
}

func (this *Ont) NewMultiTransferTransaction(gasPrice, gasLimit uint64, states []*ont.State) (*types.MutableTransaction, error) {
	return utils.NewNativeInvokeTransaction(
		gasPrice,
		gasLimit,
		ONT_CONTRACT_VERSION,
		ONT_CONTRACT_ADDRESS,
		ont.TRANSFER_NAME,
		[]interface{}{states})
}

func (this *Ont) MultiTransfer(gasPrice, gasLimit uint64, states []*ont.State, signer *account.Account) (common.Uint256, error) {
	tx, err := this.NewMultiTransferTransaction(gasPrice, gasLimit, states)
	if err != nil {
		return common.UINT256_EMPTY, err
	}
	err = utils.SignToTransaction(tx, signer)
	if err != nil {
		return common.UINT256_EMPTY, err
	}
	return this.Client.SendTransaction(tx)
}

func (this *Ont) NewTransferFromTransaction(gasPrice, gasLimit uint64, sender, from, to common.Address, amount uint64) (*types.MutableTransaction, error) {
	state := &ont.TransferFrom{
		Sender: sender,
		From:   from,
		To:     to,
		Value:  amount,
	}
	return utils.NewNativeInvokeTransaction(
		gasPrice,
		gasLimit,
		ONT_CONTRACT_VERSION,
		ONT_CONTRACT_ADDRESS,
		ont.TRANSFERFROM_NAME,
		[]interface{}{state},
	)
}

func (this *Ont) TransferFrom(gasPrice, gasLimit uint64, sender *account.Account, from, to common.Address, amount uint64) (common.Uint256, error) {
	tx, err := this.NewTransferFromTransaction(gasPrice, gasLimit, sender.Address, from, to, amount)
	if err != nil {
		return common.UINT256_EMPTY, err
	}
	err = utils.SignToTransaction(tx, sender)
	if err != nil {
		return common.UINT256_EMPTY, err
	}
	return this.Client.SendTransaction(tx)
}

func (this *Ont) NewApproveTransaction(gasPrice, gasLimit uint64, from, to common.Address, amount uint64) (*types.MutableTransaction, error) {
	state := &ont.State{
		From:  from,
		To:    to,
		Value: amount,
	}
	return utils.NewNativeInvokeTransaction(
		gasPrice,
		gasLimit,
		ONT_CONTRACT_VERSION,
		ONT_CONTRACT_ADDRESS,
		ont.APPROVE_NAME,
		[]interface{}{state},
	)
}

func (this *Ont) Approve(gasPrice, gasLimit uint64, from *account.Account, to common.Address, amount uint64) (common.Uint256, error) {
	tx, err := this.NewApproveTransaction(gasPrice, gasLimit, from.Address, to, amount)
	if err != nil {
		return common.UINT256_EMPTY, err
	}
	err = utils.SignToTransaction(tx, from)
	if err != nil {
		return common.UINT256_EMPTY, err
	}
	return this.Client.SendTransaction(tx)
}

func (this *Ont) Allowance(from, to common.Address) (uint64, error) {
	type allowanceStruct struct {
		From common.Address
		To   common.Address
	}

	preResult, err := this.PreExecInvokeNativeContract(
		ont.ALLOWANCE_NAME,
		[]interface{}{&allowanceStruct{From: from, To: to}},
	)
	if err != nil {
		return 0, err
	}
	balance, err := preResult.Result.ToInteger()
	if err != nil {
		return 0, err
	}
	return balance.Uint64(), nil
}

func (this *Ont) Symbol() (string, error) {
	preResult, err := this.PreExecInvokeNativeContract(
		ont.SYMBOL_NAME,
		[]interface{}{},
	)
	if err != nil {
		return "", err
	}
	return preResult.Result.ToString()
}

func (this *Ont) BalanceOf(address common.Address) (uint64, error) {
	preResult, err := this.PreExecInvokeNativeContract(
		ont.BALANCEOF_NAME,
		[]interface{}{address[:]},
	)
	if err != nil {
		return 0, err
	}
	balance, err := preResult.Result.ToInteger()
	if err != nil {
		return 0, err
	}
	return balance.Uint64(), nil
}

func (this *Ont) Name() (string, error) {
	preResult, err := this.PreExecInvokeNativeContract(
		ont.NAME_NAME,
		[]interface{}{},
	)
	if err != nil {
		return "", err
	}
	return preResult.Result.ToString()
}

func (this *Ont) Decimals() (byte, error) {
	preResult, err := this.PreExecInvokeNativeContract(
		ont.DECIMALS_NAME,
		[]interface{}{},
	)
	if err != nil {
		return 0, err
	}
	decimals, err := preResult.Result.ToInteger()
	if err != nil {
		return 0, err
	}
	return byte(decimals.Uint64()), nil
}

func (this *Ont) TotalSupply() (uint64, error) {
	preResult, err := this.PreExecInvokeNativeContract(
		ont.TOTAL_SUPPLY_NAME,
		[]interface{}{},
	)
	if err != nil {
		return 0, err
	}
	balance, err := preResult.Result.ToInteger()
	if err != nil {
		return 0, err
	}
	return balance.Uint64(), nil
}
