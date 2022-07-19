package ethereum

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
	//TODO implement me
	panic("implement me")
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
	//TODO implement me
	panic("implement me")
}

func (e Ethereum) Address() chainCom.Address {
	//TODO implement me
	panic("implement me")
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
	panic("implement me")
}

func (e Ethereum) WaitForGenerateBlock(timeout time.Duration, blockCount ...uint32) (bool, error) {
	//TODO implement me
	panic("implement me")
}

func (e Ethereum) WaitForTxConfirmed(blockHeight uint64) error {
	//TODO implement me
	panic("implement me")
}

func (e Ethereum) GetBlockHeightByTxHash(txHash string) (uint32, error) {
	//TODO implement me
	panic("implement me")
}

func (e Ethereum) BalanceOf(addr chainCom.Address) (uint64, error) {
	of, err := e.sdk.EVM.Usdt.BalanceOf(addr)
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
	panic("implement me")
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
	//TODO implement me
	panic("implement me")
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

func (e Ethereum) GetAllDnsNodes() (map[string]dns.DNSNodeInfo, error) {
	nodes, err := e.sdk.EVM.Dns.GetAllDnsNodes()
	if err != nil {
		return nil, err
	}
	return nodes, nil

}

func (e Ethereum) QueryPluginsInfo() (*dns.NameInfoList, error) {
	//TODO implement me
	panic("implement me")
}

func (e Ethereum) RegisterHeader(header, desc string, ttl uint64) (string, error) {
	//TODO implement me
	panic("implement me")
}

func (e Ethereum) RegisterUrl(url string, rType uint64, name, desc string, ttl uint64) (string, error) {
	//TODO implement me
	panic("implement me")
}

func (e Ethereum) BindUrl(urlType uint64, url string, name, desc string, ttl uint64) (string, error) {
	//TODO implement me
	panic("implement me")
}

func (e Ethereum) DeleteUrl(url string) (string, error) {
	//TODO implement me
	panic("implement me")
}

func (e Ethereum) QueryUrl(url string, ownerAddr chainCom.Address) (*dns.NameInfo, error) {
	//TODO implement me
	panic("implement me")
}

func (e Ethereum) GetDnsNodeByAddr(wallet chainCom.Address) (*dns.DNSNodeInfo, error) {
	//TODO implement me
	panic("implement me")
}

func (e Ethereum) DNSNodeReg(ip, port []byte, initPos uint64) (string, error) {
	//TODO implement me
	panic("implement me")
}

func (e Ethereum) UnregisterDNSNode() (string, error) {
	//TODO implement me
	panic("implement me")
}

func (e Ethereum) QuitNode() (string, error) {
	//TODO implement me
	panic("implement me")
}

func (e Ethereum) AddInitPos(addPosAmount uint64) (string, error) {
	//TODO implement me
	panic("implement me")
}

func (e Ethereum) ReduceInitPos(changePosAmount uint64) (string, error) {
	//TODO implement me
	panic("implement me")
}

func (e Ethereum) GetPeerPoolMap() (*dns.PeerPoolMap, error) {
	//TODO implement me
	panic("implement me")
}

func (e Ethereum) GetPeerPoolItem(pubKey string) (*dns.PeerPoolItem, error) {
	//TODO implement me
	panic("implement me")
}

func (e Ethereum) FormatError(err error) error {
	//TODO implement me
	panic("implement me")
}

func (e Ethereum) GetFileInfo(fileHashStr string) (*fs.FileInfo, error) {
	//TODO implement me
	panic("implement me")
}

func (e Ethereum) GetFileInfos(fileHashStr []string) (*fs.FileInfoList, error) {
	//TODO implement me
	panic("implement me")
}

func (e Ethereum) GetUploadStorageFee(opt *fs.UploadOption) (*fs.StorageFee, error) {
	//TODO implement me
	panic("implement me")
}

func (e Ethereum) GetDeleteFilesStorageFee(addr chainCom.Address, fileHashStrs []string) (uint64, error) {
	//TODO implement me
	panic("implement me")
}

func (e Ethereum) GetNodeList() (*fs.FsNodesInfo, error) {
	//TODO implement me
	panic("implement me")
}

func (e Ethereum) ProveParamSer(rootHash []byte, fileId pdp.FileID) ([]byte, error) {
	//TODO implement me
	panic("implement me")
}

func (e Ethereum) ProveParamDes(buf []byte) (*fs.ProveParam, error) {
	//TODO implement me
	panic("implement me")
}

func (e Ethereum) StoreFile(fileHashStr, blocksRoot string, blockNum, blockSizeInKB, proveLevel, expiredHeight, copyNum uint64, fileDesc []byte, privilege uint64, proveParam []byte, storageType, realFileSize uint64, primaryNodes, candidateNodes []chainCom.Address, plotInfo *fs.PlotInfo) (string, uint32, error) {
	//TODO implement me
	panic("implement me")
}

func (e Ethereum) DeleteFiles(files []string, gasLimit uint64) (string, error) {
	//TODO implement me
	panic("implement me")
}

func (e Ethereum) DeleteUploadedFiles(fileHashStrs []string, gasLimit uint64) (string, uint32, error) {
	//TODO implement me
	panic("implement me")
}

func (e Ethereum) AddWhiteLists(fileHashStr string, whitelists []fs.Rule) (string, error) {
	//TODO implement me
	panic("implement me")
}

func (e Ethereum) GetFileProveDetails(fileHashStr string) (*fs.FsProveDetails, error) {
	//TODO implement me
	panic("implement me")
}

func (e Ethereum) GetFileProveNodes(fileHashStr string) (map[string]uint64, error) {
	//TODO implement me
	panic("implement me")
}

func (e Ethereum) GetFileList(addr chainCom.Address) (*fs.FileList, error) {
	list, err := e.sdk.EVM.Fs.GetFileList(addr)
	if err != nil {
		return nil, err
	}
	return list, nil
}

func (e Ethereum) GetFsSetting() (*fs.FsSetting, error) {
	//TODO implement me
	panic("implement me")
}

func (e Ethereum) GetWhiteList(fileHashStr string) (*fs.WhiteList, error) {
	//TODO implement me
	panic("implement me")
}

func (e Ethereum) WhiteListOp(fileHashStr string, op uint64, whiteList fs.WhiteList) (string, error) {
	//TODO implement me
	panic("implement me")
}

func (e Ethereum) GetNodeInfoByWallet(walletAddr chainCom.Address) (*fs.FsNodeInfo, error) {
	//TODO implement me
	panic("implement me")
}

func (e Ethereum) GetNodeHostAddrListByWallets(nodeWalletAddrs []chainCom.Address) ([]string, error) {
	//TODO implement me
	panic("implement me")
}

func (e Ethereum) GetNodeListWithoutAddrs(nodeWalletAddrs []chainCom.Address, num int) ([]chainCom.Address, error) {
	//TODO implement me
	panic("implement me")
}

func (e Ethereum) GetUnprovePrimaryFileInfos(walletAddr chainCom.Address) ([]fs.FileInfo, error) {
	//TODO implement me
	panic("implement me")
}

func (e Ethereum) GetUnproveCandidateFileInfos(walletAddr chainCom.Address) ([]fs.FileInfo, error) {
	//TODO implement me
	panic("implement me")
}

func (e Ethereum) CheckHasProveFile(fileHashStr string, walletAddr chainCom.Address) bool {
	//TODO implement me
	panic("implement me")
}

func (e Ethereum) CreateSector(sectorId uint64, proveLevel uint64, size uint64, isPlots bool) (string, error) {
	//TODO implement me
	panic("implement me")
}

func (e Ethereum) DeleteSector(sectorId uint64) (string, error) {
	//TODO implement me
	panic("implement me")
}

func (e Ethereum) GetSectorInfo(sectorId uint64) (*fs.SectorInfo, error) {
	//TODO implement me
	panic("implement me")
}

func (e Ethereum) GetSectorInfosForNode(walletAddr string) (*fs.SectorInfos, error) {
	//TODO implement me
	panic("implement me")
}

func (e Ethereum) IsFileInfoDeleted(err error) bool {
	//TODO implement me
	panic("implement me")
}

func (e Ethereum) CheckFilePrivilege(info *fs.FileInfo, fileHashStr, walletAddr string) bool {
	//TODO implement me
	panic("implement me")
}

func (e Ethereum) GetUserSpace(walletAddr string) (*fs.UserSpace, error) {
	base58, err := chainCom.AddressFromBase58(walletAddr)
	if err != nil {
		return nil, err
	}
	space, err := e.sdk.EVM.Fs.GetUserSpace(base58)
	if err != nil {
		return nil, err
	}
	return space, nil
}

func (e Ethereum) UpdateUserSpace(walletAddr string, size, sizeOpType, blockCount, countOpType uint64) (string, error) {
	//TODO implement me
	panic("implement me")
}

func (e Ethereum) GetUpdateUserSpaceCost(walletAddr string, size, sizeOpType, blockCount, countOpType uint64) (*usdt.State, error) {
	//TODO implement me
	panic("implement me")
}

func (e Ethereum) RegisterNode(addr string, volume, serviceTime uint64) (string, error) {
	//TODO implement me
	panic("implement me")
}

func (e Ethereum) NodeExit() (string, error) {
	//TODO implement me
	panic("implement me")
}

func (e Ethereum) QueryNode(walletAddr string) (*fs.FsNodeInfo, error) {
	address, err := chainCom.AddressFromBase58(walletAddr)
	if err != nil {
		return nil, err
	}
	query, err := e.sdk.EVM.Fs.NodeQuery(address)
	if err != nil {
		return nil, err
	}
	return query, nil
}

func (e Ethereum) UpdateNode(addr string, volume, serviceTime uint64) (string, error) {
	//TODO implement me
	panic("implement me")
}

func (e Ethereum) NodeWithdrawProfit() (string, error) {
	//TODO implement me
	panic("implement me")
}
