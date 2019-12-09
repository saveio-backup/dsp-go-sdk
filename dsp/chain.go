package dsp

import (
	"time"

	dspErr "github.com/saveio/dsp-go-sdk/error"
	sdkCom "github.com/saveio/themis-go-sdk/common"
	"github.com/saveio/themis/account"
	chainCom "github.com/saveio/themis/common"
	"github.com/saveio/themis/core/types"
	"github.com/saveio/themis/smartcontract/service/native/dns"
	"github.com/saveio/themis/smartcontract/service/native/micropayment"
	fs "github.com/saveio/themis/smartcontract/service/native/savefs"
	"github.com/saveio/themis/smartcontract/service/native/usdt"
)

/* Chain API */

func (this *Dsp) SetAccount(acc *account.Account) {
	this.chain.SetAccount(acc)
}

func (this *Dsp) CurrentAccount() *account.Account {
	return this.chain.CurrentAccount()
}

// WalletAddress. get base58 address
func (this *Dsp) Address() chainCom.Address {
	return this.CurrentAccount().Address
}

// WalletAddress. get base58 address
func (this *Dsp) WalletAddress() string {
	return this.CurrentAccount().Address.ToBase58()
}

// RegisterNode. register node to chain
func (this *Dsp) RegisterNode(addr string, volume, serviceTime uint64) (string, error) {
	return this.chain.RegisterNode(addr, volume, serviceTime)
}

// UnregisterNode. cancel register node to chain
func (this *Dsp) UnregisterNode() (string, error) {
	return this.chain.UnregisterNode()
}

// QueryNode. query node information by wallet address
func (this *Dsp) QueryNode(walletAddr string) (*fs.FsNodeInfo, error) {
	return this.chain.QueryNode(walletAddr)
}

// UpdateNode. update node information
func (this *Dsp) UpdateNode(addr string, volume, serviceTime uint64) (string, error) {
	return this.chain.UpdateNode(addr, volume, serviceTime)
}

// RegisterNode. register node to chain
func (this *Dsp) NodeWithdrawProfit() (string, error) {
	return this.chain.NodeWithdrawProfit()
}

// CheckFilePrivilege. check if the downloader has privilege to download file
func (this *Dsp) CheckFilePrivilege(fileHashStr, walletAddr string) bool {
	return this.chain.CheckFilePrivilege(fileHashStr, walletAddr)
}

// GetUserSpace. get user space of client
func (this *Dsp) GetUserSpace(walletAddr string) (*fs.UserSpace, error) {
	return this.chain.GetUserSpace(walletAddr)
}

func (this *Dsp) UpdateUserSpace(walletAddr string, size, sizeOpType, blockCount, countOpType uint64) (string, error) {
	return this.chain.UpdateUserSpace(walletAddr, size, sizeOpType, blockCount, countOpType)
}

func (this *Dsp) GetUpdateUserSpaceCost(walletAddr string, size, sizeOpType, blockCount, countOpType uint64) (*usdt.State, error) {
	return this.chain.GetUpdateUserSpaceCost(walletAddr, size, sizeOpType, blockCount, countOpType)
}

func (this *Dsp) IsFileInfoDeleted(err error) bool {
	return this.chain.IsFileInfoDeleted(err)
}

// CalculateUploadFee. pre-calculate upload cost by its upload options
func (this *Dsp) CalculateUploadFee(opt *fs.UploadOption) (*fs.StorageFee, error) {
	if opt.ProveInterval == 0 {
		return nil, dspErr.New(dspErr.INTERNAL_ERROR, "prove interval too small")
	}
	return this.chain.GetUploadStorageFee(opt)
}

func (this *Dsp) BalanceOf(addr chainCom.Address) (uint64, error) {
	return this.chain.BalanceOf(addr)
}

func (this *Dsp) GetFsSetting() (*fs.FsSetting, error) {
	return this.chain.GetFsSetting()
}

func (this *Dsp) GetCurrentBlockHeight() (uint32, error) {
	return this.chain.GetCurrentBlockHeight()
}

func (this *Dsp) QueryUrl(url string, ownerAddr chainCom.Address) (*dns.NameInfo, error) {
	return this.chain.QueryUrl(url, ownerAddr)
}

func (this *Dsp) PollForTxConfirmed(timeout time.Duration, txHashStr string) (bool, error) {
	return this.chain.PollForTxConfirmed(timeout, txHashStr)
}

func (this *Dsp) WaitForGenerateBlock(timeout time.Duration, blockCount ...uint32) (bool, error) {
	return this.chain.WaitForGenerateBlock(timeout, blockCount...)
}

func (this *Dsp) GetBlockHeightByTxHash(txHash string) (uint32, error) {
	return this.chain.GetBlockHeightByTxHash(txHash)
}
func (this *Dsp) GetChainVersion() (string, error) {
	return this.chain.GetChainVersion()
}
func (this *Dsp) GetBlockHash(height uint32) (string, error) {
	return this.chain.GetBlockHash(height)
}
func (this *Dsp) GetBlockByHash(blockHash string) (*types.Block, error) {
	return this.chain.GetBlockByHash(blockHash)
}
func (this *Dsp) GetBlockTxHashesByHeight(height uint32) (*sdkCom.BlockTxHashes, error) {
	return this.chain.GetBlockTxHashesByHeight(height)
}
func (this *Dsp) GetBlockByHeight(height uint32) (*types.Block, error) {
	return this.chain.GetBlockByHeight(height)
}
func (this *Dsp) GetTransaction(txHash string) (*types.Transaction, error) {
	return this.chain.GetTransaction(txHash)
}
func (this *Dsp) GetSmartContractEvent(txHash string) (*sdkCom.SmartContactEvent, error) {
	return this.chain.GetSmartContractEvent(txHash)
}
func (this *Dsp) GetSmartContract(contractAddress string) (*sdkCom.SmartContract, error) {
	return this.chain.GetSmartContract(contractAddress)
}
func (this *Dsp) GetStorage(contractAddress string, key []byte) ([]byte, error) {
	return this.chain.GetStorage(contractAddress, key)
}
func (this *Dsp) GetMerkleProof(txHash string) (*sdkCom.MerkleProof, error) {
	return this.chain.GetMerkleProof(txHash)
}
func (this *Dsp) GetGasPrice() (uint64, error) {
	return this.chain.GetGasPrice()
}
func (this *Dsp) GetMemPoolTxCount() (*sdkCom.MemPoolTxCount, error) {
	return this.chain.GetMemPoolTxCount()
}
func (this *Dsp) GetMemPoolTxState(txHash string) (*sdkCom.MemPoolTxState, error) {
	return this.chain.GetMemPoolTxState(txHash)
}
func (this *Dsp) GetSmartContractEventByEventId(contractAddress string, address string, eventId uint32) ([]*sdkCom.SmartContactEvent, error) {
	return this.chain.GetSmartContractEventByEventId(contractAddress, address, eventId)
}
func (this *Dsp) GetSmartContractEventByBlock(height uint32) (*sdkCom.SmartContactEvent, error) {
	return this.chain.GetSmartContractEventByBlock(height)
}
func (this *Dsp) Transfer(gasPrice, gasLimit uint64, from *account.Account, to chainCom.Address, amount uint64) (string, error) {
	return this.chain.Transfer(gasPrice, gasLimit, from, to, amount)
}
func (this *Dsp) InvokeNativeContract(gasPrice, gasLimit uint64, signer *account.Account, version byte, contractAddress chainCom.Address, method string, params []interface{}) (string, error) {
	return this.chain.InvokeNativeContract(gasPrice, gasLimit, signer, version, contractAddress, method, params)
}
func (this *Dsp) PreExecInvokeNativeContract(contractAddress chainCom.Address, version byte, method string, params []interface{}) (*sdkCom.PreExecResult, error) {
	return this.chain.PreExecInvokeNativeContract(contractAddress, version, method, params)
}

func (this *Dsp) GetChannelInfoFromChain(channelID uint64, participant1, participant2 chainCom.Address) (*micropayment.ChannelInfo, error) {
	if this.chain == nil {
		return nil, dspErr.New(dspErr.CHAIN_ERROR, "no chain")
	}
	return this.chain.GetChannelInfo(channelID, participant1, participant2)
}

func (this *Dsp) GetDnsNodeByAddr(wallet chainCom.Address) (*dns.DNSNodeInfo, error) {
	if this.chain == nil {
		return nil, dspErr.New(dspErr.CHAIN_ERROR, "no chain")
	}
	return this.chain.GetDnsNodeByAddr(wallet)
}

func (this *Dsp) GetFileInfo(fileHashStr string) (*fs.FileInfo, error) {
	return this.chain.GetFileInfo(fileHashStr)
}

func (this *Dsp) GetFileInfos(fileHashStrs []string) (*fs.FileInfoList, error) {
	return this.chain.GetFileInfos(fileHashStrs)
}

func (this *Dsp) GetFileList(addr chainCom.Address) (*fs.FileList, error) {
	return this.chain.GetFileList(addr)
}

func (this *Dsp) GetWhiteList(fileHashStr string) (*fs.WhiteList, error) {
	return this.chain.GetWhiteList(fileHashStr)
}

func (this *Dsp) WhiteListOp(fileHashStr string, op uint64, whiteList fs.WhiteList) (string, error) {
	return this.chain.WhiteListOp(fileHashStr, op, whiteList)
}

func (this *Dsp) GetNodeList() (*fs.FsNodesInfo, error) {
	return this.chain.GetNodeList()
}

func (this *Dsp) GetFileProveDetails(fileHashStr string) (*fs.FsProveDetails, error) {
	return this.chain.GetFileProveDetails(fileHashStr)
}

func (this *Dsp) DNSNodeReg(ip, port []byte, initPos uint64) (string, error) {
	return this.chain.DNSNodeReg(ip, port, initPos)
}

func (this *Dsp) UnregisterDNSNode() (string, error) {
	return this.chain.UnregisterDNSNode()
}

func (this *Dsp) QuitNode() (string, error) {
	return this.chain.QuitNode()
}

func (this *Dsp) AddInitPos(addPosAmount uint64) (string, error) {
	return this.chain.AddInitPos(addPosAmount)
}

func (this *Dsp) ReduceInitPos(changePosAmount uint64) (string, error) {
	return this.chain.ReduceInitPos(changePosAmount)
}

func (this *Dsp) GetPeerPoolMap() (*dns.PeerPoolMap, error) {
	return this.chain.GetPeerPoolMap()
}

func (this *Dsp) GetPeerPoolItem(pubKey string) (*dns.PeerPoolItem, error) {
	return this.chain.GetPeerPoolItem(pubKey)
}

func (this *Dsp) GetAllDnsNodes() (map[string]dns.DNSNodeInfo, error) {
	return this.chain.GetAllDnsNodes()
}
