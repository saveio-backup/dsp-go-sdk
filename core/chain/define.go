package chain

import (
	"github.com/saveio/dsp-go-sdk/types/state"
	themisSDK "github.com/saveio/themis-go-sdk"
	sdkCom "github.com/saveio/themis-go-sdk/common"
	"github.com/saveio/themis/account"
	chainCom "github.com/saveio/themis/common"
	"github.com/saveio/themis/core/types"
	"github.com/saveio/themis/smartcontract/service/native/dns"
	"github.com/saveio/themis/smartcontract/service/native/micropayment"
	fs "github.com/saveio/themis/smartcontract/service/native/savefs"
	"github.com/saveio/themis/smartcontract/service/native/savefs/pdp"
	"github.com/saveio/themis/smartcontract/service/native/usdt"
	"time"
)

type Client interface {
	// chain
	SetAccount(acc *account.Account)
	CurrentAccount() *account.Account
	SetIsClient(isClient bool)
	BlockConfirm() uint32
	SetBlockConfirm(blockConfirm uint32)
	State() state.ModuleState
	WalletAddress() string
	Address() chainCom.Address
	SDK() *themisSDK.Chain
	GetCurrentBlockHeight() (uint32, error)
	PollForTxConfirmed(timeout time.Duration, txHashStr string) (uint32, error)
	WaitForGenerateBlock(timeout time.Duration, blockCount ...uint32) (bool, error)
	WaitForTxConfirmed(blockHeight uint64) error
	GetBlockHeightByTxHash(txHash string) (uint32, error)
	BalanceOf(addr chainCom.Address) (uint64, error)
	GetChainVersion() (string, error)
	GetBlockHash(height uint32) (string, error)
	GetBlockByHash(blockHash string) (*types.Block, error)
	GetBlockTxHashesByHeight(height uint32) (*sdkCom.BlockTxHashes, error)
	GetBlockByHeight(height uint32) (*types.Block, error)
	GetTransaction(txHash string) (*types.Transaction, error)
	GetSmartContractEvent(txHash string) (*sdkCom.SmartContactEvent, error)
	GetSmartContract(contractAddress string) (*sdkCom.SmartContract, error)
	GetStorage(contractAddress string, key []byte) ([]byte, error)
	GetMerkleProof(txHash string) (*sdkCom.MerkleProof, error)
	GetGasPrice() (uint64, error)
	GetMemPoolTxCount() (*sdkCom.MemPoolTxCount, error)
	GetMemPoolTxState(txHash string) (*sdkCom.MemPoolTxState, error)
	GetSmartContractEventByEventId(contractAddress string, address string,
		eventId uint32) ([]*sdkCom.SmartContactEvent, error)
	GetSmartContractEventByEventIdAndHeights(contractAddress string, address string,
		eventId, startHeight, endHeight uint32) ([]*sdkCom.SmartContactEvent, error)
	GetSmartContractEventByBlock(height uint32) (*sdkCom.SmartContactEvent, error)
	Transfer(gasPrice, gasLimit uint64, from *account.Account, to chainCom.Address, amount uint64) (string, error)
	InvokeNativeContract(gasPrice, gasLimit uint64, signer *account.Account, version byte,
		contractAddress chainCom.Address, method string, params []interface{}) (string, error)
	PreExecInvokeNativeContract(contractAddress chainCom.Address, version byte, method string,
		params []interface{}) (*sdkCom.PreExecResult, error)
	GetChannelInfo(channelID uint64, participant1, participant2 chainCom.Address) (*micropayment.ChannelInfo, error)
	FastTransfer(paymentId uint64, from, to chainCom.Address, amount uint64) (string, error)
	// dns
	GetAllDnsNodes() (map[string]dns.DNSNodeInfo, error)
	QueryPluginsInfo() (*dns.NameInfoList, error)
	RegisterHeader(header, desc string, ttl uint64) (string, error)
	RegisterUrl(url string, rType uint64, name, desc string, ttl uint64) (string, error)
	BindUrl(urlType uint64, url string, name, desc string, ttl uint64) (string, error)
	DeleteUrl(url string) (string, error)
	QueryUrl(url string, ownerAddr chainCom.Address) (*dns.NameInfo, error)
	GetDnsNodeByAddr(wallet chainCom.Address) (*dns.DNSNodeInfo, error)
	DNSNodeReg(ip, port []byte, initPos uint64) (string, error)
	UnregisterDNSNode() (string, error)
	QuitNode() (string, error)
	AddInitPos(addPosAmount uint64) (string, error)
	ReduceInitPos(changePosAmount uint64) (string, error)
	GetPeerPoolMap() (*dns.PeerPoolMap, error)
	GetPeerPoolItem(pubKey string) (*dns.PeerPoolItem, error)
	// error
	FormatError(err error) error
	// fs
	GetFileInfo(fileHashStr string) (*fs.FileInfo, error)
	GetFileInfos(fileHashStr []string) (*fs.FileInfoList, error)
	GetUploadStorageFee(opt *fs.UploadOption) (*fs.StorageFee, error)
	GetDeleteFilesStorageFee(addr chainCom.Address, fileHashStrs []string) (uint64, error)
	GetNodeList() (*fs.FsNodesInfo, error)
	ProveParamSer(rootHash []byte, fileId pdp.FileID) ([]byte, error)
	ProveParamDes(buf []byte) (*fs.ProveParam, error)
	StoreFile(
		fileHashStr, blocksRoot string,
		blockNum, blockSizeInKB, proveLevel, expiredHeight, copyNum uint64,
		fileDesc []byte, privilege uint64, proveParam []byte,
		storageType, realFileSize uint64,
		primaryNodes, candidateNodes []chainCom.Address, plotInfo *fs.PlotInfo,
	) (string, uint32, error)
	DeleteFiles(files []string, gasLimit uint64) (string, error)
	DeleteUploadedFiles(fileHashStrs []string, gasLimit uint64) (string, uint32, error)
	AddWhiteLists(fileHashStr string, whitelists []fs.Rule) (string, error)
	GetFileProveDetails(fileHashStr string) (*fs.FsProveDetails, error)
	GetFileProveNodes(fileHashStr string) (map[string]uint64, error)
	GetFileList(addr chainCom.Address) (*fs.FileList, error)
	GetFsSetting() (*fs.FsSetting, error)
	GetWhiteList(fileHashStr string) (*fs.WhiteList, error)
	WhiteListOp(fileHashStr string, op uint64, whiteList fs.WhiteList) (string, error)
	GetNodeInfoByWallet(walletAddr chainCom.Address) (*fs.FsNodeInfo, error)
	GetNodeHostAddrListByWallets(nodeWalletAddrs []chainCom.Address) ([]string, error)
	GetNodeListWithoutAddrs(nodeWalletAddrs []chainCom.Address, num int) ([]chainCom.Address, error)
	GetUnprovePrimaryFileInfos(walletAddr chainCom.Address) ([]fs.FileInfo, error)
	GetUnproveCandidateFileInfos(walletAddr chainCom.Address) ([]fs.FileInfo, error)
	CheckHasProveFile(fileHashStr string, walletAddr chainCom.Address) bool
	CreateSector(sectorId uint64, proveLevel uint64, size uint64, isPlots bool) (string, error)
	DeleteSector(sectorId uint64) (string, error)
	GetSectorInfo(sectorId uint64) (*fs.SectorInfo, error)
	GetSectorInfosForNode(walletAddr string) (*fs.SectorInfos, error)
	IsFileInfoDeleted(err error) bool
	CheckFilePrivilege(info *fs.FileInfo, fileHashStr, walletAddr string) bool
	GetUserSpace(walletAddr string) (*fs.UserSpace, error)
	UpdateUserSpace(walletAddr string, size, sizeOpType, blockCount, countOpType uint64) (string, error)
	GetUpdateUserSpaceCost(walletAddr string, size, sizeOpType, blockCount, countOpType uint64) (*usdt.State, error)
	RegisterNode(addr string, volume, serviceTime uint64) (string, error)
	NodeExit() (string, error)
	QueryNode(walletAddr string) (*fs.FsNodeInfo, error)
	UpdateNode(addr string, volume, serviceTime uint64) (string, error)
	NodeWithdrawProfit() (string, error)
}
