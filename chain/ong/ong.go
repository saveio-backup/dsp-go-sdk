package ong

import (
	"github.com/oniio/dsp-go-sdk/chain/client"
	sdkcom "github.com/oniio/dsp-go-sdk/chain/common"
	cont "github.com/oniio/dsp-go-sdk/chain/ont"
	"github.com/oniio/dsp-go-sdk/chain/utils"
	"github.com/oniio/oniChain/account"
	"github.com/oniio/oniChain/common"
	"github.com/oniio/oniChain/core/types"
	"github.com/oniio/oniChain/smartcontract/service/native/ont"
)

var (
	ONG_CONTRACT_ADDRESS, _ = utils.AddressFromHexString("0200000000000000000000000000000000000000")
	ONG_CONTRACT_VERSION    = byte(0)
)

type Ong struct {
	Client *client.ClientMgr
}

func (this *Ong) PreExecInvokeNativeContract(
	method string,
	params []interface{},
) (*sdkcom.PreExecResult, error) {
	tx, err := utils.NewNativeInvokeTransaction(0, 0, ONG_CONTRACT_VERSION, ONG_CONTRACT_ADDRESS, method, params)
	if err != nil {
		return nil, err
	}
	return this.Client.PreExecTransaction(tx)
}

func (this *Ong) NewTransferTransaction(gasPrice, gasLimit uint64, from, to common.Address, amount uint64) (*types.MutableTransaction, error) {
	state := &ont.State{
		From:  from,
		To:    to,
		Value: amount,
	}
	return this.NewMultiTransferTransaction(gasPrice, gasLimit, []*ont.State{state})
}

func (this *Ong) Transfer(gasPrice, gasLimit uint64, from *account.Account, to common.Address, amount uint64) (common.Uint256, error) {
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

func (this *Ong) NewMultiTransferTransaction(gasPrice, gasLimit uint64, states []*ont.State) (*types.MutableTransaction, error) {
	return utils.NewNativeInvokeTransaction(
		gasPrice,
		gasLimit,
		ONG_CONTRACT_VERSION,
		ONG_CONTRACT_ADDRESS,
		ont.TRANSFER_NAME,
		[]interface{}{states})
}

func (this *Ong) MultiTransfer(gasPrice, gasLimit uint64, states []*ont.State, signer *account.Account) (common.Uint256, error) {
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

func (this *Ong) NewTransferFromTransaction(gasPrice, gasLimit uint64, sender, from, to common.Address, amount uint64) (*types.MutableTransaction, error) {
	state := &ont.TransferFrom{
		Sender: sender,
		From:   from,
		To:     to,
		Value:  amount,
	}
	return utils.NewNativeInvokeTransaction(
		gasPrice,
		gasLimit,
		ONG_CONTRACT_VERSION,
		ONG_CONTRACT_ADDRESS,
		ont.TRANSFERFROM_NAME,
		[]interface{}{state},
	)
}

func (this *Ong) TransferFrom(gasPrice, gasLimit uint64, sender *account.Account, from, to common.Address, amount uint64) (common.Uint256, error) {
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

func (this *Ong) NewWithdrawONGTransaction(gasPrice, gasLimit uint64, address common.Address, amount uint64) (*types.MutableTransaction, error) {
	return this.NewTransferFromTransaction(gasPrice, gasLimit, address, cont.ONT_CONTRACT_ADDRESS, address, amount)
}

func (this *Ong) WithdrawONG(gasPrice, gasLimit uint64, address *account.Account, amount uint64) (common.Uint256, error) {
	tx, err := this.NewWithdrawONGTransaction(gasPrice, gasLimit, address.Address, amount)
	if err != nil {
		return common.UINT256_EMPTY, err
	}
	err = utils.SignToTransaction(tx, address)
	if err != nil {
		return common.UINT256_EMPTY, err
	}
	return this.Client.SendTransaction(tx)
}

func (this *Ong) NewApproveTransaction(gasPrice, gasLimit uint64, from, to common.Address, amount uint64) (*types.MutableTransaction, error) {
	state := &ont.State{
		From:  from,
		To:    to,
		Value: amount,
	}
	return utils.NewNativeInvokeTransaction(
		gasPrice,
		gasLimit,
		ONG_CONTRACT_VERSION,
		ONG_CONTRACT_ADDRESS,
		ont.APPROVE_NAME,
		[]interface{}{state},
	)
}

func (this *Ong) Approve(gasPrice, gasLimit uint64, from *account.Account, to common.Address, amount uint64) (common.Uint256, error) {
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

func (this *Ong) Allowance(from, to common.Address) (uint64, error) {
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

func (this *Ong) UnboundONG(address common.Address) (uint64, error) {
	return this.Allowance(cont.ONT_CONTRACT_ADDRESS, address)
}

func (this *Ong) Symbol() (string, error) {
	preResult, err := this.PreExecInvokeNativeContract(
		ont.SYMBOL_NAME,
		[]interface{}{},
	)
	if err != nil {
		return "", err
	}
	return preResult.Result.ToString()
}

func (this *Ong) BalanceOf(address common.Address) (uint64, error) {
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

func (this *Ong) Name() (string, error) {
	preResult, err := this.PreExecInvokeNativeContract(
		ont.NAME_NAME,
		[]interface{}{},
	)
	if err != nil {
		return "", err
	}
	return preResult.Result.ToString()
}

func (this *Ong) Decimals() (byte, error) {
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

func (this *Ong) TotalSupply() (uint64, error) {
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
