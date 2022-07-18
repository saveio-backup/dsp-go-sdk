package chain

import (
	"encoding/hex"
	"fmt"
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

type Themis struct {
	account      *account.Account // account for chain
	sdk          *themisSDK.Chain // chain sdk
	isClient     bool             // flag of is client or max node
	blockConfirm uint32           // wait for n block confirm
	r            *rand.Rand
	s            *state.SyncState
}

func NewThemis(acc *account.Account, rpcAddrs []string) *Themis {
	sdkClient := themisSDK.NewChain()
	sdkClient.NewRpcClient().SetAddress(rpcAddrs)
	if acc != nil {
		sdkClient.SetDefaultAccount(acc)
	}
	ch := &Themis{
		account: acc,
		sdk:     sdkClient,
		r:       rand.New(rand.NewSource(time.Now().UnixNano())),
		s:       state.NewSyncState(),
	}
	ch.s.Set(state.ModuleStateActive)
	return ch
}

func (t *Themis) SetAccount(acc *account.Account) {
	t.account = acc
	t.sdk.Native.SetDefaultAccount(acc)
}

func (t *Themis) CurrentAccount() *account.Account {
	return t.account
}

func (t *Themis) SetIsClient(isClient bool) {
	t.isClient = isClient
}

func (t *Themis) BlockConfirm() uint32 {
	return t.blockConfirm
}

func (t *Themis) SetBlockConfirm(blockConfirm uint32) {
	t.blockConfirm = blockConfirm
}

func (t *Themis) State() state.ModuleState {
	return t.s.Get()
}

func (t *Themis) WalletAddress() string {
	if t.account == nil {
		return ""
	}
	return t.account.Address.ToBase58()
}

func (t *Themis) Address() chainCom.Address {
	if t.account == nil {
		return chainCom.ADDRESS_EMPTY
	}
	return t.account.Address
}

func (t *Themis) SDK() *themisSDK.Chain {
	return t.sdk
}

func (t *Themis) GetCurrentBlockHeight() (uint32, error) {
	height, err := t.sdk.GetCurrentBlockHeight()
	if err != nil {
		return 0, sdkErr.NewWithError(sdkErr.CHAIN_ERROR, t.FormatError(err))
	}
	return height, nil
}

func (t *Themis) PollForTxConfirmed(timeout time.Duration, txHashStr string) (uint32, error) {
	reverseTxHash, err := hex.DecodeString(txHashStr)
	if err != nil {
		return 0, sdkErr.NewWithError(sdkErr.CHAIN_ERROR, t.FormatError(err))
	}
	txHash := chainCom.ToArrayReverse(reverseTxHash)
	height, err := t.sdk.PollForTxConfirmedHeight(timeout, txHash)
	if err != nil {
		return 0, sdkErr.NewWithError(sdkErr.CHAIN_ERROR, t.FormatError(err))
	}
	return height, nil
}

func (t *Themis) WaitForGenerateBlock(timeout time.Duration, blockCount ...uint32) (bool, error) {
	if len(blockCount) == 0 {
		if t.blockConfirm != 0 {
			blockCount = make([]uint32, 0)
			blockCount = append(blockCount, t.blockConfirm)
		} else {
			return true, nil
		}
	} else {
		if blockCount[0] == 0 {
			if t.blockConfirm != 0 {
				blockCount[0] = t.blockConfirm
			} else {
				return true, nil
			}
		}
	}
	confirmed, err := t.sdk.WaitForGenerateBlock(timeout, blockCount...)
	if err != nil {
		return false, sdkErr.NewWithError(sdkErr.CHAIN_ERROR, t.FormatError(err))
	}
	return confirmed, nil
}

func (t *Themis) WaitForTxConfirmed(blockHeight uint64) error {
	currentBlockHeight, err := t.GetCurrentBlockHeight()
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
	waitSuccess, err := t.WaitForGenerateBlock(time.Duration(timeout)*time.Second,
		uint32(blockHeight-uint64(currentBlockHeight)))
	if err != nil || !waitSuccess {
		log.Errorf("get block height err %s %d %d", err, currentBlockHeight, blockHeight)
		return fmt.Errorf("get block height err %d %d", currentBlockHeight, blockHeight)
	}
	return nil
}

func (t *Themis) GetBlockHeightByTxHash(txHash string) (uint32, error) {
	height, err := t.sdk.GetBlockHeightByTxHash(txHash)
	if err != nil {
		return 0, sdkErr.NewWithError(sdkErr.CHAIN_ERROR, t.FormatError(err))
	}
	return height, nil
}

func (t *Themis) BalanceOf(addr chainCom.Address) (uint64, error) {
	bal, err := t.sdk.Native.Usdt.BalanceOf(addr)
	if err != nil {
		return 0, sdkErr.NewWithError(sdkErr.CHAIN_ERROR, t.FormatError(err))
	}
	return bal, nil
}

func (t *Themis) GetChainVersion() (string, error) {
	ver, err := t.sdk.GetVersion()
	if err != nil {
		return "", sdkErr.NewWithError(sdkErr.CHAIN_ERROR, t.FormatError(err))
	}
	return ver, nil
}

func (t *Themis) GetBlockHash(height uint32) (string, error) {
	val, err := t.sdk.GetBlockHash(height)
	if err != nil {
		return "", sdkErr.NewWithError(sdkErr.CHAIN_ERROR, t.FormatError(err))
	}
	return hex.EncodeToString(chainCom.ToArrayReverse(val[:])), nil
}

func (t *Themis) GetBlockByHash(blockHash string) (*types.Block, error) {
	val, err := t.sdk.GetBlockByHash(blockHash)
	if err != nil {
		return nil, sdkErr.NewWithError(sdkErr.CHAIN_ERROR, t.FormatError(err))
	}
	return val, nil
}

func (t *Themis) GetBlockTxHashesByHeight(height uint32) (*sdkCom.BlockTxHashes, error) {
	val, err := t.sdk.GetBlockTxHashesByHeight(height)
	if err != nil {
		return nil, sdkErr.NewWithError(sdkErr.CHAIN_ERROR, t.FormatError(err))
	}
	return val, nil
}

func (t *Themis) GetBlockByHeight(height uint32) (*types.Block, error) {
	val, err := t.sdk.GetBlockByHeight(height)
	if err != nil {
		return nil, sdkErr.NewWithError(sdkErr.CHAIN_ERROR, t.FormatError(err))
	}
	return val, nil
}

func (t *Themis) GetTransaction(txHash string) (*types.Transaction, error) {
	val, err := t.sdk.GetTransaction(txHash)
	if err != nil {
		return nil, sdkErr.NewWithError(sdkErr.CHAIN_ERROR, t.FormatError(err))
	}
	return val, nil
}

func (t *Themis) GetSmartContractEvent(txHash string) (*sdkCom.SmartContactEvent, error) {
	val, err := t.sdk.GetSmartContractEvent(txHash)
	if err != nil {
		return nil, sdkErr.NewWithError(sdkErr.CHAIN_ERROR, t.FormatError(err))
	}
	return val, nil
}

func (t *Themis) GetSmartContract(contractAddress string) (*sdkCom.SmartContract, error) {
	val, err := t.sdk.GetSmartContract(contractAddress)
	if err != nil {
		return nil, sdkErr.NewWithError(sdkErr.CHAIN_ERROR, t.FormatError(err))
	}
	return val, nil
}
func (t *Themis) GetStorage(contractAddress string, key []byte) ([]byte, error) {
	val, err := t.sdk.GetStorage(contractAddress, key)
	if err != nil {
		return nil, sdkErr.NewWithError(sdkErr.CHAIN_ERROR, t.FormatError(err))
	}
	return val, nil
}
func (t *Themis) GetMerkleProof(txHash string) (*sdkCom.MerkleProof, error) {
	val, err := t.sdk.GetMerkleProof(txHash)
	if err != nil {
		return nil, sdkErr.NewWithError(sdkErr.CHAIN_ERROR, t.FormatError(err))
	}
	return val, nil
}
func (t *Themis) GetGasPrice() (uint64, error) {
	val, err := t.sdk.GetGasPrice()
	if err != nil {
		return 0, sdkErr.NewWithError(sdkErr.CHAIN_ERROR, t.FormatError(err))
	}
	return val, nil
}
func (t *Themis) GetMemPoolTxCount() (*sdkCom.MemPoolTxCount, error) {
	val, err := t.sdk.GetMemPoolTxCount()
	if err != nil {
		return nil, sdkErr.NewWithError(sdkErr.CHAIN_ERROR, t.FormatError(err))
	}
	return val, nil
}

func (t *Themis) GetMemPoolTxState(txHash string) (*sdkCom.MemPoolTxState, error) {
	val, err := t.sdk.GetMemPoolTxState(txHash)
	if err != nil {
		return nil, sdkErr.NewWithError(sdkErr.CHAIN_ERROR, t.FormatError(err))
	}
	return val, nil
}

func (t *Themis) GetSmartContractEventByEventId(contractAddress string, address string, eventId uint32) ([]*sdkCom.SmartContactEvent, error) {
	val, err := t.sdk.GetSmartContractEventByEventId(contractAddress, address, eventId)
	if err != nil {
		return nil, sdkErr.NewWithError(sdkErr.CHAIN_ERROR, t.FormatError(err))
	}
	return val, nil
}

func (t *Themis) GetSmartContractEventByEventIdAndHeights(contractAddress string, address string,
	eventId, startHeight, endHeight uint32) ([]*sdkCom.SmartContactEvent, error) {
	val, err := t.sdk.GetSmartContractEventByEventIdAndHeights(
		contractAddress, address, eventId, startHeight, endHeight)
	if err != nil {
		return nil, sdkErr.NewWithError(sdkErr.CHAIN_ERROR, t.FormatError(err))
	}
	return val, nil
}

func (t *Themis) GetSmartContractEventByBlock(height uint32) (*sdkCom.SmartContactEvent, error) {
	val, err := t.sdk.GetSmartContractEventByBlock(height)
	if err != nil {
		return nil, sdkErr.NewWithError(sdkErr.CHAIN_ERROR, t.FormatError(err))
	}
	return val, nil
}

func (t *Themis) Transfer(gasPrice, gasLimit uint64, from *account.Account, to chainCom.Address, amount uint64) (string, error) {
	val, err := t.sdk.Native.Usdt.Transfer(gasPrice, gasLimit, from, to, amount)
	if err != nil {
		return "", sdkErr.NewWithError(sdkErr.CHAIN_ERROR, t.FormatError(err))
	}
	return hex.EncodeToString(chainCom.ToArrayReverse(val[:])), nil
}

func (t *Themis) InvokeNativeContract(gasPrice, gasLimit uint64, signer *account.Account, version byte, contractAddress chainCom.Address, method string, params []interface{}) (string, error) {
	val, err := t.sdk.InvokeNativeContract(gasPrice, gasLimit, signer, version, contractAddress, method, params)
	if err != nil {
		return "", sdkErr.NewWithError(sdkErr.CHAIN_ERROR, t.FormatError(err))
	}
	return hex.EncodeToString(chainCom.ToArrayReverse(val[:])), nil
}

func (t *Themis) PreExecInvokeNativeContract(contractAddress chainCom.Address, version byte, method string, params []interface{}) (*sdkCom.PreExecResult, error) {
	val, err := t.sdk.PreExecInvokeNativeContract(contractAddress, version, method, params)
	if err != nil {
		return nil, sdkErr.NewWithError(sdkErr.CHAIN_ERROR, t.FormatError(err))
	}
	return val, nil
}

func (t *Themis) GetChannelInfo(channelID uint64, participant1, participant2 chainCom.Address) (*micropayment.ChannelInfo, error) {
	val, err := t.sdk.Native.Channel.GetChannelInfo(channelID, participant1, participant2)
	if err != nil {
		return nil, sdkErr.NewWithError(sdkErr.CHAIN_ERROR, t.FormatError(err))
	}
	return val, nil
}

func (t *Themis) FastTransfer(paymentId uint64, from, to chainCom.Address, amount uint64) (string, error) {
	val, err := t.sdk.Native.Channel.FastTransfer(paymentId, from, to, amount)
	if err != nil {
		return "", sdkErr.NewWithError(sdkErr.CHAIN_ERROR, t.FormatError(err))
	}
	return hex.EncodeToString(chainCom.ToArrayReverse(val[:])), nil
}
