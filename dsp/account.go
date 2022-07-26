package dsp

import (
	"github.com/saveio/dsp-go-sdk/consts"
	"time"

	sdkErr "github.com/saveio/dsp-go-sdk/error"
	sdkCom "github.com/saveio/themis-go-sdk/common"
	"github.com/saveio/themis/account"
	chainCom "github.com/saveio/themis/common"
	"github.com/saveio/themis/core/types"
	"github.com/saveio/themis/smartcontract/service/native/dns"
	"github.com/saveio/themis/smartcontract/service/native/micropayment"
	fs "github.com/saveio/themis/smartcontract/service/native/savefs"
	"github.com/saveio/themis/smartcontract/service/native/usdt"
)

func (this *Dsp) SetAccount(acc *account.Account) {
	this.Chain.SetAccount(acc)
}

func (this *Dsp) CurrentAccount() *account.Account {
	if this == nil || this.Chain == nil {
		return nil
	}
	return this.Chain.CurrentAccount()
}

// WalletAddress. get base58 address
func (this *Dsp) Address() chainCom.Address {
	if this == nil {
		return chainCom.ADDRESS_EMPTY
	}
	acc := this.CurrentAccount()
	if acc == nil {
		return chainCom.ADDRESS_EMPTY
	}
	switch this.Mode {
	case consts.DspModeThemis:
		return acc.Address
	case consts.DspModeOp:
		return chainCom.Address(acc.EthAddress)
	}
	return acc.Address
}

// WalletAddress. get base58 address
func (this *Dsp) WalletAddress() string {
	if this == nil {
		return ""
	}
	acc := this.CurrentAccount()
	if acc == nil {
		return ""
	}
	return acc.Address.ToBase58()
}

// RegisterNode. register node to chain
func (this *Dsp) RegisterNode(addr string, volume, serviceTime uint64) (string, error) {
	return this.Chain.RegisterNode(addr, volume, serviceTime)
}

// UnregisterNode. cancel register node to chain
func (this *Dsp) UnregisterNode() (string, error) {
	return this.Chain.NodeExit()
}

// QueryNode. query node information by wallet address
func (this *Dsp) QueryNode(walletAddr string) (*fs.FsNodeInfo, error) {
	return this.Chain.QueryNode(walletAddr)
}

// UpdateNode. update node information
func (this *Dsp) UpdateNode(addr string, volume, serviceTime uint64) (string, error) {
	return this.Chain.UpdateNode(addr, volume, serviceTime)
}

// RegisterNode. register node to chain
func (this *Dsp) NodeWithdrawProfit() (string, error) {
	return this.Chain.NodeWithdrawProfit()
}

func (this *Dsp) RegisterHeader(header, desc string, ttl uint64) (string, error) {
	return this.Chain.RegisterHeader(header, desc, ttl)
}

// CheckFilePrivilege. check if the downloader has privilege to download file
func (this *Dsp) CheckFilePrivilege(info *fs.FileInfo, fileHashStr, walletAddr string) bool {
	return this.Chain.CheckFilePrivilege(info, fileHashStr, walletAddr)
}

// GetUserSpace. get user space of client
func (this *Dsp) GetUserSpace(walletAddr string) (*fs.UserSpace, error) {
	return this.Chain.GetUserSpace(walletAddr)
}

func (this *Dsp) UpdateUserSpace(walletAddr string, size, sizeOpType, blockCount, countOpType uint64) (
	string, error) {
	return this.Chain.UpdateUserSpace(walletAddr, size, sizeOpType, blockCount, countOpType)
}

func (this *Dsp) GetUpdateUserSpaceCost(walletAddr string, size, sizeOpType, blockCount, countOpType uint64) (
	*usdt.State, error) {
	return this.Chain.GetUpdateUserSpaceCost(walletAddr, size, sizeOpType, blockCount, countOpType)
}

func (this *Dsp) IsFileInfoDeleted(err error) bool {
	return this.Chain.IsFileInfoDeleted(err)
}

// CalculateUploadFee. pre-calculate upload cost by its upload options
func (this *Dsp) CalculateUploadFee(opt *fs.UploadOption) (*fs.StorageFee, error) {
	if opt.ProveInterval == 0 {
		return nil, sdkErr.New(sdkErr.INTERNAL_ERROR, "prove interval too small")
	}
	return this.Chain.GetUploadStorageFee(opt)
}

func (this *Dsp) BalanceOf(addr chainCom.Address) (uint64, error) {
	return this.Chain.BalanceOf(addr)
}

func (this *Dsp) GetFsSetting() (*fs.FsSetting, error) {
	return this.Chain.GetFsSetting()
}

func (this *Dsp) GetCurrentBlockHeight() (uint32, error) {
	return this.Chain.GetCurrentBlockHeight()
}

func (this *Dsp) QueryUrl(url string, ownerAddr chainCom.Address) (*dns.NameInfo, error) {
	return this.Chain.QueryUrl(url, ownerAddr)
}

func (this *Dsp) PollForTxConfirmed(timeout time.Duration, txHashStr string) (uint32, error) {
	return this.Chain.PollForTxConfirmed(timeout, txHashStr)
}

func (this *Dsp) WaitForGenerateBlock(timeout time.Duration, blockCount ...uint32) (bool, error) {
	return this.Chain.WaitForGenerateBlock(timeout, blockCount...)
}

func (this *Dsp) GetBlockHeightByTxHash(txHash string) (uint32, error) {
	return this.Chain.GetBlockHeightByTxHash(txHash)
}

func (this *Dsp) GetChainVersion() (string, error) {
	return this.Chain.GetChainVersion()
}

func (this *Dsp) GetBlockHash(height uint32) (string, error) {
	return this.Chain.GetBlockHash(height)
}

func (this *Dsp) GetBlockByHash(blockHash string) (*types.Block, error) {
	return this.Chain.GetBlockByHash(blockHash)
}

func (this *Dsp) GetBlockTxHashesByHeight(height uint32) (*sdkCom.BlockTxHashes, error) {
	return this.Chain.GetBlockTxHashesByHeight(height)
}

func (this *Dsp) GetBlockByHeight(height uint32) (*types.Block, error) {
	return this.Chain.GetBlockByHeight(height)
}

func (this *Dsp) GetTransaction(txHash string) (*types.Transaction, error) {
	return this.Chain.GetTransaction(txHash)
}

func (this *Dsp) GetSmartContractEvent(txHash string) (*sdkCom.SmartContactEvent, error) {
	return this.Chain.GetSmartContractEvent(txHash)
}

func (this *Dsp) GetSmartContract(contractAddress string) (*sdkCom.SmartContract, error) {
	return this.Chain.GetSmartContract(contractAddress)
}

func (this *Dsp) GetStorage(contractAddress string, key []byte) ([]byte, error) {
	return this.Chain.GetStorage(contractAddress, key)
}

func (this *Dsp) GetMerkleProof(txHash string) (*sdkCom.MerkleProof, error) {
	return this.Chain.GetMerkleProof(txHash)
}

func (this *Dsp) GetGasPrice() (uint64, error) {
	return this.Chain.GetGasPrice()
}

func (this *Dsp) GetMemPoolTxCount() (*sdkCom.MemPoolTxCount, error) {
	return this.Chain.GetMemPoolTxCount()
}

func (this *Dsp) GetMemPoolTxState(txHash string) (*sdkCom.MemPoolTxState, error) {
	return this.Chain.GetMemPoolTxState(txHash)
}

func (this *Dsp) GetSmartContractEventByEventId(contractAddress string, address string, eventId uint32) (
	[]*sdkCom.SmartContactEvent, error) {
	return this.Chain.GetSmartContractEventByEventId(contractAddress, address, eventId)
}

func (this *Dsp) GetSmartContractEventByEventIdAndHeights(contractAddress string, address string,
	eventId, startHeight, endHeight uint32) ([]*sdkCom.SmartContactEvent, error) {
	return this.Chain.GetSmartContractEventByEventIdAndHeights(contractAddress, address, eventId,
		startHeight, endHeight)
}

func (this *Dsp) GetSmartContractEventByBlock(height uint32) (*sdkCom.SmartContactEvent, error) {
	return this.Chain.GetSmartContractEventByBlock(height)
}

func (this *Dsp) Transfer(gasPrice, gasLimit uint64, from *account.Account, to chainCom.Address, amount uint64) (
	string, error) {
	return this.Chain.Transfer(gasPrice, gasLimit, from, to, amount)
}

func (this *Dsp) InvokeNativeContract(gasPrice, gasLimit uint64, signer *account.Account, version byte,
	contractAddress chainCom.Address, method string, params []interface{}) (string, error) {
	return this.Chain.InvokeNativeContract(gasPrice, gasLimit, signer, version, contractAddress, method, params)
}

func (this *Dsp) PreExecInvokeNativeContract(contractAddress chainCom.Address, version byte, method string,
	params []interface{}) (*sdkCom.PreExecResult, error) {
	return this.Chain.PreExecInvokeNativeContract(contractAddress, version, method, params)
}

func (this *Dsp) GetChannelInfoFromChain(channelID uint64, participant1, participant2 chainCom.Address) (
	*micropayment.ChannelInfo, error) {
	if this.Chain == nil {
		return nil, sdkErr.New(sdkErr.CHAIN_ERROR, "no chain")
	}
	return this.Chain.GetChannelInfo(channelID, participant1, participant2)
}

func (this *Dsp) GetDnsNodeByAddr(wallet chainCom.Address) (*dns.DNSNodeInfo, error) {
	if this.Chain == nil {
		return nil, sdkErr.New(sdkErr.CHAIN_ERROR, "no chain")
	}
	return this.Chain.GetDnsNodeByAddr(wallet)
}

func (this *Dsp) GetFileInfo(fileHashStr string) (*fs.FileInfo, error) {
	return this.Chain.GetFileInfo(fileHashStr)
}

func (this *Dsp) GetFileInfos(fileHashStrs []string) (*fs.FileInfoList, error) {
	return this.Chain.GetFileInfos(fileHashStrs)
}

func (this *Dsp) GetFileList(addr chainCom.Address) (*fs.FileList, error) {
	return this.Chain.GetFileList(addr)
}

func (this *Dsp) GetWhiteList(fileHashStr string) (*fs.WhiteList, error) {
	return this.Chain.GetWhiteList(fileHashStr)
}

func (this *Dsp) WhiteListOp(fileHashStr string, op uint64, whiteList fs.WhiteList) (string, error) {
	return this.Chain.WhiteListOp(fileHashStr, op, whiteList)
}

func (this *Dsp) GetNodeList() (*fs.FsNodesInfo, error) {
	return this.Chain.GetNodeList()
}

func (this *Dsp) GetFileProveDetails(fileHashStr string) (*fs.FsProveDetails, error) {
	return this.Chain.GetFileProveDetails(fileHashStr)
}

func (this *Dsp) DNSNodeReg(ip, port []byte, initPos uint64) (string, error) {
	return this.Chain.DNSNodeReg(ip, port, initPos)
}

func (this *Dsp) UnregisterDNSNode() (string, error) {
	return this.Chain.UnregisterDNSNode()
}

func (this *Dsp) QuitNode() (string, error) {
	return this.Chain.QuitNode()
}

func (this *Dsp) AddInitPos(addPosAmount uint64) (string, error) {
	return this.Chain.AddInitPos(addPosAmount)
}

func (this *Dsp) ReduceInitPos(changePosAmount uint64) (string, error) {
	return this.Chain.ReduceInitPos(changePosAmount)
}

func (this *Dsp) GetPeerPoolMap() (*dns.PeerPoolMap, error) {
	return this.Chain.GetPeerPoolMap()
}

func (this *Dsp) GetPeerPoolItem(pubKey string) (*dns.PeerPoolItem, error) {
	return this.Chain.GetPeerPoolItem(pubKey)
}

func (this *Dsp) GetAllDnsNodes() (map[string]dns.DNSNodeInfo, error) {
	return this.Chain.GetAllDnsNodes()
}

func (this *Dsp) GetNodeHostAddrListByWallets(wallets []chainCom.Address) ([]string, error) {
	return this.Chain.GetNodeHostAddrListByWallets(wallets)
}

func (this *Dsp) QueryPluginsInfo() (*dns.NameInfoList, error) {
	return this.Chain.QueryPluginsInfo()
}

func (this *Dsp) CreateSector(sectorId uint64, proveLevel uint64, size uint64, isPlots bool) (string, error) {
	return this.Chain.CreateSector(sectorId, proveLevel, size, isPlots)
}

func (this *Dsp) DeleteSector(sectorId uint64) (string, error) {
	return this.Chain.DeleteSector(sectorId)
}

func (this *Dsp) GetSectorInfo(sectorId uint64) (*fs.SectorInfo, error) {
	return this.Chain.GetSectorInfo(sectorId)
}

func (this *Dsp) GetSectorInfosForNode(addr string) (*fs.SectorInfos, error) {
	return this.Chain.GetSectorInfosForNode(addr)
}
