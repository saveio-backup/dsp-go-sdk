package chain

import (
	"encoding/hex"
	"time"

	dspErr "github.com/saveio/dsp-go-sdk/error"
	themisSDK "github.com/saveio/themis-go-sdk"
	sdkCom "github.com/saveio/themis-go-sdk/common"
	"github.com/saveio/themis/account"
	chainCom "github.com/saveio/themis/common"
	"github.com/saveio/themis/core/types"
	"github.com/saveio/themis/smartcontract/service/native/micropayment"
)

type Chain struct {
	account  *account.Account // account for chain
	themis   *themisSDK.Chain // chain sdk
	isClient bool             // flag of is client or max node
}

type ChainOption interface {
	apply(*Chain)
}

type ChainOptFunc func(*Chain)

func (f ChainOptFunc) apply(c *Chain) {
	f(c)
}

func IsClient(is bool) ChainOption {
	return ChainOptFunc(func(c *Chain) {
		c.isClient = is
	})
}

func NewChain(acc *account.Account, rpcAddrs []string, opts ...ChainOption) *Chain {
	themis := themisSDK.NewChain()
	themis.NewRpcClient().SetAddress(rpcAddrs)
	if acc != nil {
		themis.SetDefaultAccount(acc)
	}
	ch := &Chain{
		account: acc,
		themis:  themis,
	}
	for _, opt := range opts {
		opt.apply(ch)
	}
	return ch
}

func (this *Chain) SetAccount(acc *account.Account) {
	this.account = acc
	this.themis.Native.SetDefaultAccount(acc)
}

func (this *Chain) CurrentAccount() *account.Account {
	return this.account
}

func (this *Chain) WalletAddress() string {
	return this.account.Address.ToBase58()
}

func (this *Chain) Address() chainCom.Address {
	return this.account.Address
}

func (this *Chain) Themis() *themisSDK.Chain {
	return this.themis
}

func (this *Chain) GetCurrentBlockHeight() (uint32, error) {
	height, err := this.themis.GetCurrentBlockHeight()
	if err != nil {
		return 0, dspErr.NewWithError(dspErr.CHAIN_ERROR, err)
	}
	return height, nil
}

func (this *Chain) PollForTxConfirmed(timeout time.Duration, txHashStr string) (bool, error) {
	reverseTxHash, err := hex.DecodeString(txHashStr)
	if err != nil {
		return false, dspErr.NewWithError(dspErr.CHAIN_ERROR, err)
	}
	txHash := chainCom.ToArrayReverse(reverseTxHash)
	confirmed, err := this.themis.PollForTxConfirmed(timeout, txHash)
	if err != nil {
		return false, dspErr.NewWithError(dspErr.CHAIN_ERROR, err)
	}
	return confirmed, nil
}

func (this *Chain) WaitForGenerateBlock(timeout time.Duration, blockCount ...uint32) (bool, error) {
	confirmed, err := this.themis.WaitForGenerateBlock(timeout, blockCount...)
	if err != nil {
		return false, dspErr.NewWithError(dspErr.CHAIN_ERROR, err)
	}
	return confirmed, nil
}

func (this *Chain) GetBlockHeightByTxHash(txHash string) (uint32, error) {
	height, err := this.themis.GetBlockHeightByTxHash(txHash)
	if err != nil {
		return 0, dspErr.NewWithError(dspErr.CHAIN_ERROR, err)
	}
	return height, nil
}

func (this *Chain) BalanceOf(addr chainCom.Address) (uint64, error) {
	bal, err := this.themis.Native.Usdt.BalanceOf(addr)
	if err != nil {
		return 0, dspErr.NewWithError(dspErr.CHAIN_ERROR, err)
	}
	return bal, nil
}

func (this *Chain) GetChainVersion() (string, error) {
	ver, err := this.themis.GetVersion()
	if err != nil {
		return "", dspErr.NewWithError(dspErr.CHAIN_ERROR, err)
	}
	return ver, nil
}

func (this *Chain) GetBlockHash(height uint32) (string, error) {
	val, err := this.themis.GetBlockHash(height)
	if err != nil {
		return "", dspErr.NewWithError(dspErr.CHAIN_ERROR, err)
	}
	return hex.EncodeToString(chainCom.ToArrayReverse(val[:])), nil
}

func (this *Chain) GetBlockByHash(blockHash string) (*types.Block, error) {
	val, err := this.themis.GetBlockByHash(blockHash)
	if err != nil {
		return nil, dspErr.NewWithError(dspErr.CHAIN_ERROR, err)
	}
	return val, nil
}

func (this *Chain) GetBlockTxHashesByHeight(height uint32) (*sdkCom.BlockTxHashes, error) {
	val, err := this.themis.GetBlockTxHashesByHeight(height)
	if err != nil {
		return nil, dspErr.NewWithError(dspErr.CHAIN_ERROR, err)
	}
	return val, nil
}

func (this *Chain) GetBlockByHeight(height uint32) (*types.Block, error) {
	val, err := this.themis.GetBlockByHeight(height)
	if err != nil {
		return nil, dspErr.NewWithError(dspErr.CHAIN_ERROR, err)
	}
	return val, nil
}

func (this *Chain) GetTransaction(txHash string) (*types.Transaction, error) {
	val, err := this.themis.GetTransaction(txHash)
	if err != nil {
		return nil, dspErr.NewWithError(dspErr.CHAIN_ERROR, err)
	}
	return val, nil
}

func (this *Chain) GetSmartContractEvent(txHash string) (*sdkCom.SmartContactEvent, error) {
	val, err := this.themis.GetSmartContractEvent(txHash)
	if err != nil {
		return nil, dspErr.NewWithError(dspErr.CHAIN_ERROR, err)
	}
	return val, nil
}

func (this *Chain) GetSmartContract(contractAddress string) (*sdkCom.SmartContract, error) {
	val, err := this.themis.GetSmartContract(contractAddress)
	if err != nil {
		return nil, dspErr.NewWithError(dspErr.CHAIN_ERROR, err)
	}
	return val, nil
}
func (this *Chain) GetStorage(contractAddress string, key []byte) ([]byte, error) {
	val, err := this.themis.GetStorage(contractAddress, key)
	if err != nil {
		return nil, dspErr.NewWithError(dspErr.CHAIN_ERROR, err)
	}
	return val, nil
}
func (this *Chain) GetMerkleProof(txHash string) (*sdkCom.MerkleProof, error) {
	val, err := this.themis.GetMerkleProof(txHash)
	if err != nil {
		return nil, dspErr.NewWithError(dspErr.CHAIN_ERROR, err)
	}
	return val, nil
}
func (this *Chain) GetGasPrice() (uint64, error) {
	val, err := this.themis.GetGasPrice()
	if err != nil {
		return 0, dspErr.NewWithError(dspErr.CHAIN_ERROR, err)
	}
	return val, nil
}
func (this *Chain) GetMemPoolTxCount() (*sdkCom.MemPoolTxCount, error) {
	val, err := this.themis.GetMemPoolTxCount()
	if err != nil {
		return nil, dspErr.NewWithError(dspErr.CHAIN_ERROR, err)
	}
	return val, nil
}

func (this *Chain) GetMemPoolTxState(txHash string) (*sdkCom.MemPoolTxState, error) {
	val, err := this.themis.GetMemPoolTxState(txHash)
	if err != nil {
		return nil, dspErr.NewWithError(dspErr.CHAIN_ERROR, err)
	}
	return val, nil
}

func (this *Chain) GetSmartContractEventByEventId(contractAddress string, address string, eventId uint32) ([]*sdkCom.SmartContactEvent, error) {
	val, err := this.themis.GetSmartContractEventByEventId(contractAddress, address, eventId)
	if err != nil {
		return nil, dspErr.NewWithError(dspErr.CHAIN_ERROR, err)
	}
	return val, nil
}
func (this *Chain) GetSmartContractEventByBlock(height uint32) (*sdkCom.SmartContactEvent, error) {
	val, err := this.themis.GetSmartContractEventByBlock(height)
	if err != nil {
		return nil, dspErr.NewWithError(dspErr.CHAIN_ERROR, err)
	}
	return val, nil
}

func (this *Chain) Transfer(gasPrice, gasLimit uint64, from *account.Account, to chainCom.Address, amount uint64) (string, error) {
	val, err := this.themis.Native.Usdt.Transfer(gasPrice, gasLimit, from, to, amount)
	if err != nil {
		return "", dspErr.NewWithError(dspErr.CHAIN_ERROR, err)
	}
	return hex.EncodeToString(chainCom.ToArrayReverse(val[:])), nil
}

func (this *Chain) InvokeNativeContract(gasPrice, gasLimit uint64, signer *account.Account, version byte, contractAddress chainCom.Address, method string, params []interface{}) (string, error) {
	val, err := this.themis.InvokeNativeContract(gasPrice, gasLimit, signer, version, contractAddress, method, params)
	if err != nil {
		return "", dspErr.NewWithError(dspErr.CHAIN_ERROR, err)
	}
	return hex.EncodeToString(chainCom.ToArrayReverse(val[:])), nil
}

func (this *Chain) PreExecInvokeNativeContract(contractAddress chainCom.Address, version byte, method string, params []interface{}) (*sdkCom.PreExecResult, error) {
	val, err := this.themis.PreExecInvokeNativeContract(contractAddress, version, method, params)
	if err != nil {
		return nil, dspErr.NewWithError(dspErr.CHAIN_ERROR, err)
	}
	return val, nil
}

func (this *Chain) GetChannelInfo(channelID uint64, participant1, participant2 chainCom.Address) (*micropayment.ChannelInfo, error) {
	val, err := this.themis.Native.Channel.GetChannelInfo(channelID, participant1, participant2)
	if err != nil {
		return nil, dspErr.NewWithError(dspErr.CHAIN_ERROR, err)
	}
	return val, nil
}
