package ethereum

import (
	ethCom "github.com/ethereum/go-ethereum/common"
	"github.com/saveio/dsp-go-sdk/types/state"
	themisSDK "github.com/saveio/themis-go-sdk"
	sdkCom "github.com/saveio/themis-go-sdk/common"
	"github.com/saveio/themis/account"
	chainCom "github.com/saveio/themis/common"
	"github.com/saveio/themis/common/log"
	"github.com/saveio/themis/core/types"
	"github.com/saveio/themis/smartcontract/service/native/micropayment"
	"math/rand"
	"time"
)

type Ethereum struct {
	account      *account.Account // account for chain
	sdk          *themisSDK.Chain // chain sdk
	isClient     bool             // flag of is client or max node
	blockConfirm uint32           // wait for n block confirm
	r            *rand.Rand
	s            *state.SyncState
}

func NewEthereum(acc *account.Account, rpcAddrs []string) *Ethereum {
	sdkClient := themisSDK.NewChain()
	sdkClient.NewEthClient().SetAddress(rpcAddrs)
	if acc != nil {
		sdkClient.SetDefaultAccount(acc)
	}
	ch := &Ethereum{
		account: acc,
		sdk:     sdkClient,
		r:       rand.New(rand.NewSource(time.Now().UnixNano())),
		s:       state.NewSyncState(),
	}
	ch.s.Set(state.ModuleStateActive)
	return ch
}

func (e Ethereum) SetAccount(acc *account.Account) {
	e.account = acc
	e.sdk.EVM.SetDefaultAccount(acc)
}

func (e Ethereum) CurrentAccount() *account.Account {
	return e.account
}

func (e Ethereum) SetIsClient(isClient bool) {
	e.isClient = isClient
}

func (e Ethereum) BlockConfirm() uint32 {
	//TODO implement me
	panic("implement me")
}

func (e Ethereum) SetBlockConfirm(blockConfirm uint32) {
	e.blockConfirm = blockConfirm
}

func (e Ethereum) State() state.ModuleState {
	return e.s.Get()
}

func (e Ethereum) WalletAddress() string {
	return e.account.EthAddress.Hex()
}

func (e Ethereum) Address() chainCom.Address {
	return chainCom.Address(e.account.EthAddress)
}

func (e Ethereum) SDK() *themisSDK.Chain {
	return e.sdk
}

func (e Ethereum) GetCurrentBlockHeight() (uint32, error) {
	height, err := e.sdk.GetCurrentBlockHeight()
	if err != nil {
		return 0, err
	}
	return height, nil
}

func (e Ethereum) PollForTxConfirmed(timeout time.Duration, txHashStr string) (uint32, error) {
	//TODO implement me
	log.Errorf("PollForTxConfirmed not implemented")
	return 1, nil
}

func (e Ethereum) WaitForGenerateBlock(timeout time.Duration, blockCount ...uint32) (bool, error) {
	//TODO implement me
	panic("implement me")
}

func (e Ethereum) WaitForTxConfirmed(blockHeight uint64) error {
	log.Errorf("WaitForTxConfirmed not implemented")
	return nil
}

func (e Ethereum) GetBlockHeightByTxHash(txHash string) (uint32, error) {
	log.Errorf("GetBlockHeightByTxHash not implemented")
	return 0, nil
}

func (e Ethereum) BalanceOf(addr chainCom.Address) (uint64, error) {
	address := ethCom.BytesToAddress(addr[:])
	of, err := e.sdk.EVM.ERC20.BalanceOf(address)
	if err != nil {
		return 0, err
	}
	return of, nil
}

func (e Ethereum) GetChainVersion() (string, error) {
	return "1.0.0", nil
}

func (e Ethereum) GetBlockHash(height uint32) (string, error) {
	//TODO implement me
	panic("implement me")
}

func (e Ethereum) GetBlockByHash(blockHash string) (*types.Block, error) {
	//TODO implement me
	panic("implement me")
}

func (e Ethereum) GetBlockTxHashesByHeight(height uint32) (*sdkCom.BlockTxHashes, error) {
	//TODO implement me
	panic("implement me")
}

func (e Ethereum) GetBlockByHeight(height uint32) (*types.Block, error) {
	//TODO implement me
	panic("implement me")
}

func (e Ethereum) GetTransaction(txHash string) (*types.Transaction, error) {
	//TODO implement me
	panic("implement me")
}

func (e Ethereum) GetSmartContractEvent(txHash string) (*sdkCom.SmartContactEvent, error) {
	//TODO implement me
	log.Errorf("GetSmartContractEvent not implemented")
	return nil, nil
}

func (e Ethereum) GetSmartContract(contractAddress string) (*sdkCom.SmartContract, error) {
	//TODO implement me
	panic("implement me")
}

func (e Ethereum) GetStorage(contractAddress string, key []byte) ([]byte, error) {
	//TODO implement me
	panic("implement me")
}

func (e Ethereum) GetMerkleProof(txHash string) (*sdkCom.MerkleProof, error) {
	//TODO implement me
	panic("implement me")
}

func (e Ethereum) GetGasPrice() (uint64, error) {
	//TODO implement me
	panic("implement me")
}

func (e Ethereum) GetMemPoolTxCount() (*sdkCom.MemPoolTxCount, error) {
	//TODO implement me
	panic("implement me")
}

func (e Ethereum) GetMemPoolTxState(txHash string) (*sdkCom.MemPoolTxState, error) {
	//TODO implement me
	panic("implement me")
}

func (e Ethereum) GetSmartContractEventByEventId(contractAddress string, address string, eventId uint32) ([]*sdkCom.SmartContactEvent, error) {
	return e.sdk.GetSmartContractEventByEventId(contractAddress, address, eventId)
}

func (e Ethereum) GetSmartContractEventByEventIdAndHeights(contractAddress string, address string, eventId, startHeight, endHeight uint32) ([]*sdkCom.SmartContactEvent, error) {
	return e.sdk.GetSmartContractEventByEventIdAndHeights(contractAddress, address, eventId, startHeight, endHeight)
}

func (e Ethereum) GetSmartContractEventByBlock(height uint32) (*sdkCom.SmartContactEvent, error) {
	return e.sdk.GetSmartContractEventByBlock(height)
}

func (e Ethereum) Transfer(gasPrice, gasLimit uint64, from *account.Account, to chainCom.Address, amount uint64) (string, error) {
	//TODO implement me
	panic("implement me")
}

func (e Ethereum) InvokeNativeContract(gasPrice, gasLimit uint64, signer *account.Account, version byte, contractAddress chainCom.Address, method string, params []interface{}) (string, error) {
	//TODO implement me
	panic("implement me")
}

func (e Ethereum) PreExecInvokeNativeContract(contractAddress chainCom.Address, version byte, method string, params []interface{}) (*sdkCom.PreExecResult, error) {
	//TODO implement me
	panic("implement me")
}

func (e Ethereum) GetChannelInfo(channelID uint64, participant1, participant2 chainCom.Address) (*micropayment.ChannelInfo, error) {
	//TODO implement me
	panic("implement me")
}

func (e Ethereum) FastTransfer(paymentId uint64, from, to chainCom.Address, amount uint64) (string, error) {
	//TODO implement me
	panic("implement me")
}
