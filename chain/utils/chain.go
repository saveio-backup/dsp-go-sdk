package utils

import (
	"fmt"
	"math/rand"

	"github.com/oniio/oniChain/account"
	"github.com/oniio/oniChain/common"
	"github.com/oniio/oniChain/core/payload"
	"github.com/oniio/oniChain/core/types"
	cutils "github.com/oniio/oniChain/core/utils"
	"github.com/oniio/oniChain/crypto/keypair"
	s "github.com/oniio/oniChain/crypto/signature"
)

func Sign(acc *account.Account, data []byte) ([]byte, error) {
	sig, err := s.Sign(acc.SigScheme, acc.PrivateKey, data, nil)
	if err != nil {
		return nil, err
	}
	sigData, err := s.Serialize(sig)
	if err != nil {
		return nil, fmt.Errorf("signature.Serialize error:%s", err)
	}
	return sigData, nil
}

func SignToTransaction(tx *types.MutableTransaction, signer *account.Account) error {
	if tx.Payer == common.ADDRESS_EMPTY {
		tx.Payer = signer.Address
	}
	for _, sigs := range tx.Sigs {
		if PubKeysEqual([]keypair.PublicKey{signer.PublicKey}, sigs.PubKeys) {
			//have already signed
			return nil
		}
	}
	txHash := tx.Hash()
	sigData, err := Sign(signer, txHash.ToArray())
	if err != nil {
		return fmt.Errorf("sign error:%s", err)
	}
	if tx.Sigs == nil {
		tx.Sigs = make([]types.Sig, 0)
	}
	tx.Sigs = append(tx.Sigs, types.Sig{
		PubKeys: []keypair.PublicKey{signer.PublicKey},
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
