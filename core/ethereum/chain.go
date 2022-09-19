package ethereum

import (
	"encoding/hex"
	"fmt"
	ethCom "github.com/ethereum/go-ethereum/common"
	"github.com/saveio/dsp-go-sdk/consts"
	sdkErr "github.com/saveio/dsp-go-sdk/error"
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

func (e Ethereum) GetSDK() *themisSDK.Chain {
	return e.sdk
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
	return e.blockConfirm
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
	hash := ethCom.HexToHash(txHashStr)
	reverse := chainCom.ToArrayReverse(hash.Bytes())
	height, err := e.sdk.PollForTxConfirmedHeight(timeout, reverse)
	if err != nil {
		return 0, sdkErr.NewWithError(sdkErr.CHAIN_ERROR, e.FormatError(err))
	}
	return height, nil
}

func (e Ethereum) WaitForGenerateBlock(timeout time.Duration, blockCount ...uint32) (bool, error) {
	if len(blockCount) == 0 {
		if e.blockConfirm != 0 {
			blockCount = make([]uint32, 0)
			blockCount = append(blockCount, e.blockConfirm)
		} else {
			return true, nil
		}
	} else {
		if blockCount[0] == 0 {
			if e.blockConfirm != 0 {
				blockCount[0] = e.blockConfirm
			} else {
				return true, nil
			}
		}
	}
	confirmed, err := e.sdk.WaitForGenerateBlock(timeout, blockCount...)
	if err != nil {
		return false, sdkErr.NewWithError(sdkErr.CHAIN_ERROR, e.FormatError(err))
	}
	return confirmed, nil
}

func (e Ethereum) WaitForTxConfirmed(blockHeight uint64) error {
	currentBlockHeight, err := e.GetCurrentBlockHeight()
	log.Debugf("wait for tx confirmed height: %d, now: %d", blockHeight, currentBlockHeight)
	if err != nil {
		log.Errorf("get block height err %s", err)
		return err
	}
	if blockHeight <= uint64(currentBlockHeight) {
		return nil
	}

	timeout := consts.WAIT_FOR_GENERATEBLOCK_TIMEOUT * uint32(blockHeight-uint64(currentBlockHeight))
	if timeout > consts.DOWNLOAD_FILE_TIMEOUT {
		timeout = consts.DOWNLOAD_FILE_TIMEOUT
	}
	waitSuccess, err := e.WaitForGenerateBlock(time.Duration(timeout)*time.Second,
		uint32(blockHeight-uint64(currentBlockHeight)))
	if err != nil || !waitSuccess {
		log.Errorf("get block height err %s %d %d", err, currentBlockHeight, blockHeight)
		return fmt.Errorf("get block height err %d %d", currentBlockHeight, blockHeight)
	}
	return nil
}

func (e Ethereum) GetBlockHeightByTxHash(txHash string) (uint32, error) {
	height, err := e.sdk.GetBlockHeightByTxHash(txHash)
	if err != nil {
		return 0, sdkErr.NewWithError(sdkErr.CHAIN_ERROR, e.FormatError(err))
	}
	return height, nil
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
	val, err := e.sdk.GetBlockHash(height)
	if err != nil {
		return "", sdkErr.NewWithError(sdkErr.CHAIN_ERROR, e.FormatError(err))
	}
	return hex.EncodeToString(chainCom.ToArrayReverse(val[:])), nil
}

func (e Ethereum) GetBlockByHash(blockHash string) (*types.Block, error) {
	val, err := e.sdk.GetBlockByHash(blockHash)
	if err != nil {
		return nil, sdkErr.NewWithError(sdkErr.CHAIN_ERROR, e.FormatError(err))
	}
	return val, nil
}

func (e Ethereum) GetBlockTxHashesByHeight(height uint32) (*sdkCom.BlockTxHashes, error) {
	val, err := e.sdk.GetBlockTxHashesByHeight(height)
	if err != nil {
		return nil, sdkErr.NewWithError(sdkErr.CHAIN_ERROR, e.FormatError(err))
	}
	return val, nil
}

func (e Ethereum) GetBlockByHeight(height uint32) (*types.Block, error) {
	val, err := e.sdk.GetBlockByHeight(height)
	if err != nil {
		return nil, sdkErr.NewWithError(sdkErr.CHAIN_ERROR, e.FormatError(err))
	}
	return val, nil
}

func (e Ethereum) GetTransaction(txHash string) (*types.Transaction, error) {
	val, err := e.sdk.GetTransaction(txHash)
	if err != nil {
		return nil, sdkErr.NewWithError(sdkErr.CHAIN_ERROR, e.FormatError(err))
	}
	return val, nil
}

func (e Ethereum) GetSmartContractEvent(txHash string) (*sdkCom.SmartContactEvent, error) {
	val, err := e.sdk.GetSmartContractEvent(txHash)
	if err != nil {
		return nil, sdkErr.NewWithError(sdkErr.CHAIN_ERROR, e.FormatError(err))
	}
	return val, nil
}

func (e Ethereum) GetSmartContract(contractAddress string) (*sdkCom.SmartContract, error) {
	val, err := e.sdk.GetSmartContract(contractAddress)
	if err != nil {
		return nil, sdkErr.NewWithError(sdkErr.CHAIN_ERROR, e.FormatError(err))
	}
	return val, nil
}

func (e Ethereum) GetStorage(contractAddress string, key []byte) ([]byte, error) {
	val, err := e.sdk.GetStorage(contractAddress, key)
	if err != nil {
		return nil, sdkErr.NewWithError(sdkErr.CHAIN_ERROR, e.FormatError(err))
	}
	return val, nil
}

func (e Ethereum) GetMerkleProof(txHash string) (*sdkCom.MerkleProof, error) {
	val, err := e.sdk.GetMerkleProof(txHash)
	if err != nil {
		return nil, sdkErr.NewWithError(sdkErr.CHAIN_ERROR, e.FormatError(err))
	}
	return val, nil
}

func (e Ethereum) GetGasPrice() (uint64, error) {
	val, err := e.sdk.GetGasPrice()
	if err != nil {
		return 0, sdkErr.NewWithError(sdkErr.CHAIN_ERROR, e.FormatError(err))
	}
	return val, nil
}

func (e Ethereum) GetMemPoolTxCount() (*sdkCom.MemPoolTxCount, error) {
	val, err := e.sdk.GetMemPoolTxCount()
	if err != nil {
		return nil, sdkErr.NewWithError(sdkErr.CHAIN_ERROR, e.FormatError(err))
	}
	return val, nil
}

func (e Ethereum) GetMemPoolTxState(txHash string) (*sdkCom.MemPoolTxState, error) {
	val, err := e.sdk.GetMemPoolTxState(txHash)
	if err != nil {
		return nil, sdkErr.NewWithError(sdkErr.CHAIN_ERROR, e.FormatError(err))
	}
	return val, nil
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
	val, err := e.sdk.Native.Usdt.Transfer(gasPrice, gasLimit, from, to, amount)
	if err != nil {
		return "", sdkErr.NewWithError(sdkErr.CHAIN_ERROR, e.FormatError(err))
	}
	return hex.EncodeToString(chainCom.ToArrayReverse(val[:])), nil
}

func (e Ethereum) InvokeNativeContract(gasPrice, gasLimit uint64, signer *account.Account, version byte, contractAddress chainCom.Address, method string, params []interface{}) (string, error) {
	val, err := e.sdk.InvokeNativeContract(gasPrice, gasLimit, signer, version, contractAddress, method, params)
	if err != nil {
		return "", sdkErr.NewWithError(sdkErr.CHAIN_ERROR, e.FormatError(err))
	}
	return hex.EncodeToString(chainCom.ToArrayReverse(val[:])), nil
}

func (e Ethereum) PreExecInvokeNativeContract(contractAddress chainCom.Address, version byte, method string, params []interface{}) (*sdkCom.PreExecResult, error) {
	val, err := e.sdk.PreExecInvokeNativeContract(contractAddress, version, method, params)
	if err != nil {
		return nil, sdkErr.NewWithError(sdkErr.CHAIN_ERROR, e.FormatError(err))
	}
	return val, nil
}

func (e Ethereum) GetChannelInfo(channelID uint64, participant1, participant2 chainCom.Address) (*micropayment.ChannelInfo, error) {
	val, err := e.sdk.Native.Channel.GetChannelInfo(channelID, participant1, participant2)
	if err != nil {
		return nil, sdkErr.NewWithError(sdkErr.CHAIN_ERROR, e.FormatError(err))
	}
	return val, nil
}

func (e Ethereum) FastTransfer(paymentId uint64, from, to chainCom.Address, amount uint64) (string, error) {
	val, err := e.sdk.Native.Channel.FastTransfer(paymentId, from, to, amount)
	if err != nil {
		return "", sdkErr.NewWithError(sdkErr.CHAIN_ERROR, e.FormatError(err))
	}
	return hex.EncodeToString(chainCom.ToArrayReverse(val[:])), nil
}
