package chain

import (
	"time"

	"github.com/saveio/dsp-go-sdk/consts"
	ethereum "github.com/saveio/dsp-go-sdk/core/ethereum"
	themis "github.com/saveio/dsp-go-sdk/core/themis"

	"github.com/saveio/dsp-go-sdk/types/state"
	themisSDK "github.com/saveio/themis-go-sdk"
	sdkCom "github.com/saveio/themis-go-sdk/common"
	"github.com/saveio/themis/account"
	chainCom "github.com/saveio/themis/common"
	"github.com/saveio/themis/core/types"
	"github.com/saveio/themis/smartcontract/service/native/micropayment"
)

type Chain struct {
	client Client
}

func NewChain(acc *account.Account, rpcAddrs []string, mode string, opts ...ChainOption) *Chain {
	var chain *Chain
	switch mode {
	case consts.DspModeOp:
		themisChain := ethereum.NewEthereum(acc, rpcAddrs)
		chain = &Chain{
			client: themisChain,
		}
	default:
		themisChain := themis.NewThemis(acc, rpcAddrs)
		chain = &Chain{
			client: themisChain,
		}
	}
	for _, opt := range opts {
		opt.apply(chain)
	}
	return chain
}

func (c *Chain) GetChainType() string {
	switch c.client.(type) {
	case *ethereum.Ethereum:
		return consts.DspModeOp
	default:
		return consts.DspModeThemis
	}
}

func (c *Chain) GetSDK() *themisSDK.Chain {
	return c.client.GetSDK()
}

func (c *Chain) SetAccount(acc *account.Account) {
	c.client.SetAccount(acc)
}

func (c *Chain) CurrentAccount() *account.Account {
	return c.client.CurrentAccount()
}

func (c *Chain) SetIsClient(isClient bool) {
	c.client.SetIsClient(isClient)
}

func (c *Chain) BlockConfirm() uint32 {
	return c.client.BlockConfirm()
}

func (c *Chain) SetBlockConfirm(blockConfirm uint32) {
	c.client.SetBlockConfirm(blockConfirm)
}

func (c *Chain) State() state.ModuleState {
	return c.client.State()
}

func (c *Chain) WalletAddress() string {
	return c.client.WalletAddress()
}

func (c *Chain) Address() chainCom.Address {
	return c.client.Address()
}

func (c *Chain) SDK() *themisSDK.Chain {
	return c.client.SDK()
}

func (c *Chain) GetCurrentBlockHeight() (uint32, error) {
	return c.client.GetCurrentBlockHeight()
}

func (c *Chain) PollForTxConfirmed(timeout time.Duration, txHashStr string) (uint32, error) {
	return c.client.PollForTxConfirmed(timeout, txHashStr)
}

func (c *Chain) WaitForGenerateBlock(timeout time.Duration, blockCount ...uint32) (bool, error) {
	return c.client.WaitForGenerateBlock(timeout, blockCount...)
}

func (c *Chain) WaitForTxConfirmed(blockHeight uint64) error {
	return c.client.WaitForTxConfirmed(blockHeight)
}

func (c *Chain) GetBlockHeightByTxHash(txHash string) (uint32, error) {
	return c.client.GetBlockHeightByTxHash(txHash)
}

func (c *Chain) BalanceOf(addr chainCom.Address) (uint64, error) {
	return c.client.BalanceOf(addr)
}

func (c *Chain) GetChainVersion() (string, error) {
	return c.client.GetChainVersion()
}

func (c *Chain) GetBlockHash(height uint32) (string, error) {
	return c.client.GetBlockHash(height)
}

func (c *Chain) GetBlockByHash(blockHash string) (*types.Block, error) {
	return c.client.GetBlockByHash(blockHash)
}

func (c *Chain) GetBlockTxHashesByHeight(height uint32) (*sdkCom.BlockTxHashes, error) {
	return c.client.GetBlockTxHashesByHeight(height)
}

func (c *Chain) GetBlockByHeight(height uint32) (*types.Block, error) {
	return c.client.GetBlockByHeight(height)
}

func (c *Chain) GetTransaction(txHash string) (*types.Transaction, error) {
	return c.client.GetTransaction(txHash)
}

func (c *Chain) GetRawTransaction(txHash string) ([]byte, error) {
	return c.client.GetRawTransaction(txHash)
}

func (c *Chain) GetSmartContractEvent(txHash string) (*sdkCom.SmartContactEvent, error) {
	return c.client.GetSmartContractEvent(txHash)
}

func (c *Chain) GetSmartContract(contractAddress string) (*sdkCom.SmartContract, error) {
	return c.client.GetSmartContract(contractAddress)
}
func (c *Chain) GetStorage(contractAddress string, key []byte) ([]byte, error) {
	return c.client.GetStorage(contractAddress, key)
}
func (c *Chain) GetMerkleProof(txHash string) (*sdkCom.MerkleProof, error) {
	return c.client.GetMerkleProof(txHash)
}
func (c *Chain) GetGasPrice() (uint64, error) {
	return c.client.GetGasPrice()
}
func (c *Chain) GetMemPoolTxCount() (*sdkCom.MemPoolTxCount, error) {
	return c.client.GetMemPoolTxCount()
}

func (c *Chain) GetMemPoolTxState(txHash string) (*sdkCom.MemPoolTxState, error) {
	return c.client.GetMemPoolTxState(txHash)
}

func (c *Chain) GetSmartContractEventByEventId(contractAddress string, address string, eventId uint32) ([]*sdkCom.SmartContactEvent, error) {
	return c.client.GetSmartContractEventByEventId(contractAddress, address, eventId)
}

func (c *Chain) GetSmartContractEventByEventIdAndHeights(contractAddress string, address string,
	eventId, startHeight, endHeight uint32) ([]*sdkCom.SmartContactEvent, error) {
	return c.client.GetSmartContractEventByEventIdAndHeights(contractAddress, address, eventId, startHeight, endHeight)
}

func (c *Chain) GetSmartContractEventByBlock(height uint32) (*sdkCom.SmartContactEvent, error) {
	return c.client.GetSmartContractEventByBlock(height)
}

func (c *Chain) Transfer(gasPrice, gasLimit uint64, from *account.Account, to chainCom.Address, amount uint64) (string, error) {
	return c.client.Transfer(gasPrice, gasLimit, from, to, amount)
}

func (c *Chain) InvokeNativeContract(gasPrice, gasLimit uint64, signer *account.Account, version byte, contractAddress chainCom.Address, method string, params []interface{}) (string, error) {
	return c.client.InvokeNativeContract(gasPrice, gasLimit, signer, version, contractAddress, method, params)
}

func (c *Chain) PreExecInvokeNativeContract(contractAddress chainCom.Address, version byte, method string, params []interface{}) (*sdkCom.PreExecResult, error) {
	return c.client.PreExecInvokeNativeContract(contractAddress, version, method, params)
}

func (c *Chain) GetChannelInfo(channelID uint64, participant1, participant2 chainCom.Address) (*micropayment.ChannelInfo, error) {
	return c.client.GetChannelInfo(channelID, participant1, participant2)
}

func (c *Chain) FastTransfer(paymentId uint64, from, to chainCom.Address, amount uint64) (string, error) {
	return c.client.FastTransfer(paymentId, from, to, amount)
}
