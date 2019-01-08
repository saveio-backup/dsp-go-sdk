package chain

import (
	"encoding/hex"
	"fmt"
	"math/rand"
	"time"

	"github.com/oniio/dsp-go-sdk/chain/account"
	"github.com/oniio/dsp-go-sdk/chain/client"
	sdkcom "github.com/oniio/dsp-go-sdk/chain/common"
	"github.com/oniio/dsp-go-sdk/chain/utils"
	"github.com/oniio/dsp-go-sdk/chain/wallet"
	"github.com/oniio/oniChain/common"
	sign "github.com/oniio/oniChain/common"
	"github.com/oniio/oniChain/common/constants"
	"github.com/oniio/oniChain/core/payload"
	"github.com/oniio/oniChain/core/types"
	"github.com/oniio/oniChain/crypto/keypair"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

type Chain struct {
	client.ClientMgr
	Native *NativeContract
	NeoVM  *NeoVMContract
}

//NewChain return Chain.
func NewChain() *Chain {
	chain := &Chain{}
	native := newNativeContract(chain.GetClientMgr())
	chain.Native = native
	neoVM := newNeoVMContract(chain)
	chain.NeoVM = neoVM
	return chain
}

//CreateWallet return a new wallet
func (this *Chain) CreateWallet(walletFile string) (*wallet.Wallet, error) {
	if utils.IsFileExist(walletFile) {
		return nil, fmt.Errorf("wallet:%s has already exist", walletFile)
	}
	return wallet.OpenWallet(walletFile)
}

//OpenWallet return a wallet instance
func (this *Chain) OpenWallet(walletFile string) (*wallet.Wallet, error) {
	return wallet.OpenWallet(walletFile)
}

//NewInvokeTransaction return smart contract invoke transaction
func (this *Chain) NewInvokeTransaction(gasPrice, gasLimit uint64, invokeCode []byte) *types.MutableTransaction {
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

func (this *Chain) SignToTransaction(tx *types.MutableTransaction, signer account.Signer) error {
	return utils.SignToTransaction(tx, signer)
}

func (this *Chain) MultiSignToTransaction(tx *types.MutableTransaction, m uint16, pubKeys []keypair.PublicKey, signer account.Signer) error {
	pkSize := len(pubKeys)
	if m == 0 || int(m) > pkSize || pkSize > constants.MULTI_SIG_MAX_PUBKEY_SIZE {
		return fmt.Errorf("both m and number of pub key must larger than 0, and small than %d, and m must smaller than pub key number", constants.MULTI_SIG_MAX_PUBKEY_SIZE)
	}
	validPubKey := false
	for _, pk := range pubKeys {
		if keypair.ComparePublicKey(pk, signer.GetPublicKey()) {
			validPubKey = true
			break
		}
	}
	if !validPubKey {
		return fmt.Errorf("invalid signer")
	}
	if tx.Payer == common.ADDRESS_EMPTY {
		payer, err := types.AddressFromMultiPubKeys(pubKeys, int(m))
		if err != nil {
			return fmt.Errorf("AddressFromMultiPubKeys error:%s", err)
		}
		tx.Payer = payer
	}
	txHash := tx.Hash()
	if len(tx.Sigs) == 0 {
		tx.Sigs = make([]types.Sig, 0)
	}
	sigData, err := signer.Sign(txHash.ToArray())
	if err != nil {
		return fmt.Errorf("sign error:%s", err)
	}
	hasMutilSig := false
	for i, sigs := range tx.Sigs {
		if utils.PubKeysEqual(sigs.PubKeys, pubKeys) {
			hasMutilSig = true
			if utils.HasAlreadySig(txHash.ToArray(), signer.GetPublicKey(), sigs.SigData) {
				break
			}
			sigs.SigData = append(sigs.SigData, sigData)
			tx.Sigs[i] = sigs
			break
		}
	}
	if !hasMutilSig {
		tx.Sigs = append(tx.Sigs, types.Sig{
			PubKeys: pubKeys,
			M:       m,
			SigData: [][]byte{sigData},
		})
	}
	return nil
}

func (this *Chain) InvokeNativeContract(
	gasPrice,
	gasLimit uint64,
	signer *account.Account,
	version byte,
	contractAddress common.Address,
	method string,
	params []interface{},
) (common.Uint256, error) {
	tx, err := utils.NewNativeInvokeTransaction(gasPrice, gasLimit, version, contractAddress, method, params)
	if err != nil {
		return common.UINT256_EMPTY, err
	}
	err = this.SignToTransaction(tx, signer)
	if err != nil {
		return common.UINT256_EMPTY, err
	}
	return this.SendTransaction(tx)
}

func (this *Chain) PreExecInvokeNativeContract(
	contractAddress common.Address,
	version byte,
	method string,
	params []interface{},
) (*sdkcom.PreExecResult, error) {
	tx, err := utils.NewNativeInvokeTransaction(0, 0, version, contractAddress, method, params)
	if err != nil {
		return nil, err
	}
	return this.PreExecTransaction(tx)
}

func (this *Chain) GetTxData(tx *types.MutableTransaction) (string, error) {
	txData, err := tx.IntoImmutable()
	if err != nil {
		return "", fmt.Errorf("IntoImmutable error:%s", err)
	}
	sink := sign.ZeroCopySink{}
	err = txData.Serialization(&sink)
	if err != nil {
		return "", fmt.Errorf("tx serialization error:%s", err)
	}
	rawtx := hex.EncodeToString(sink.Bytes())
	return rawtx, nil
}

func (this *Chain) GetMutableTx(rawTx string) (*types.MutableTransaction, error) {
	txData, err := hex.DecodeString(rawTx)
	if err != nil {
		return nil, fmt.Errorf("RawTx hex decode error:%s", err)
	}
	tx, err := types.TransactionFromRawBytes(txData)
	if err != nil {
		return nil, fmt.Errorf("TransactionFromRawBytes error:%s", err)
	}
	mutTx, err := tx.IntoMutable()
	if err != nil {
		return nil, fmt.Errorf("[ONT]IntoMutable error:%s", err)
	}
	return mutTx, nil
}

func (this *Chain) GetMultiAddr(pubkeys []keypair.PublicKey, m int) (string, error) {
	addr, err := types.AddressFromMultiPubKeys(pubkeys, m)
	if err != nil {
		return "", fmt.Errorf("GetMultiAddrs error:%s", err)
	}
	return addr.ToBase58(), nil
}

func (this *Chain) GetAdddrByPubKey(pubKey keypair.PublicKey) string {
	address := types.AddressFromPubKey(pubKey)
	return address.ToBase58()
}
