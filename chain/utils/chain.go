package utils

import (
	"fmt"
	"math/rand"

	"github.com/oniio/dsp-go-sdk/chain/account"
	"github.com/oniio/oniChain/common"
	"github.com/oniio/oniChain/core/payload"
	"github.com/oniio/oniChain/core/types"
	cutils "github.com/oniio/oniChain/core/utils"
	"github.com/oniio/oniChain/crypto/keypair"
)

func SignToTransaction(tx *types.MutableTransaction, signer account.Signer) error {
	if tx.Payer == common.ADDRESS_EMPTY {
		account, ok := signer.(*account.Account)
		if ok {
			tx.Payer = account.Address
		}
	}
	for _, sigs := range tx.Sigs {
		if PubKeysEqual([]keypair.PublicKey{signer.GetPublicKey()}, sigs.PubKeys) {
			//have already signed
			return nil
		}
	}
	txHash := tx.Hash()
	sigData, err := signer.Sign(txHash.ToArray())
	if err != nil {
		return fmt.Errorf("sign error:%s", err)
	}
	if tx.Sigs == nil {
		tx.Sigs = make([]types.Sig, 0)
	}
	tx.Sigs = append(tx.Sigs, types.Sig{
		PubKeys: []keypair.PublicKey{signer.GetPublicKey()},
		M:       1,
		SigData: [][]byte{sigData},
	})
	return nil
}

//NewInvokeTransaction return smart contract invoke transaction
func NewInvokeTransaction(gasPrice, gasLimit uint64, invokeCode []byte) *types.MutableTransaction {
	invokePayload := &payload.InvokeCode{
		Code: invokeCode,
	}
	tx := &types.MutableTransaction{
		GasPrice: gasPrice,
		GasLimit: gasLimit,
		TxType:   types.Invoke,
		Nonce:    rand.Uint32(),
		Payload:  invokePayload,
		Sigs:     make([]types.Sig, 0, 0),
	}
	return tx
}

func NewNativeInvokeTransaction(
	gasPrice,
	gasLimit uint64,
	version byte,
	contractAddress common.Address,
	method string,
	params []interface{},
) (*types.MutableTransaction, error) {
	if params == nil {
		params = make([]interface{}, 0, 1)
	}
	//Params cannot empty, if params is empty, fulfil with empty string
	if len(params) == 0 {
		params = append(params, "")
	}
	invokeCode, err := cutils.BuildNativeInvokeCode(contractAddress, version, method, params)
	if err != nil {
		return nil, fmt.Errorf("BuildNativeInvokeCode error:%s", err)
	}
	return NewInvokeTransaction(gasPrice, gasLimit, invokeCode), nil
}
