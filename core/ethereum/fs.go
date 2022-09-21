package ethereum

import (
	"encoding/hex"
	"errors"
	"fmt"
	ethCom "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/saveio/dsp-go-sdk/consts"
	sdkErr "github.com/saveio/dsp-go-sdk/error"
	chainCom "github.com/saveio/themis/common"
	"github.com/saveio/themis/common/log"
	fs "github.com/saveio/themis/smartcontract/service/native/savefs"
	"github.com/saveio/themis/smartcontract/service/native/savefs/pdp"
	"github.com/saveio/themis/smartcontract/service/native/usdt"
	"strings"
	"time"
)

var (
	errFileInfoNotFound = errors.New("FsGetFileInfo getFsFileInfo error")
)

func (e Ethereum) GetFileInfo(fileHashStr string) (*fs.FileInfo, error) {
	info, err := e.sdk.EVM.Fs.GetFileInfo(fileHashStr)
	if err != nil {
		return nil, sdkErr.NewWithError(sdkErr.CHAIN_ERROR, e.FormatError(err))
	}
	return info, nil
}

func (e Ethereum) GetFileInfos(fileHashStr []string) (*fs.FileInfoList, error) {
	list, err := e.sdk.EVM.Fs.GetFileInfos(fileHashStr)
	if err != nil {
		return nil, sdkErr.NewWithError(sdkErr.CHAIN_ERROR, e.FormatError(err))
	}
	return list, nil
}

func (e Ethereum) GetUploadStorageFee(opt *fs.UploadOption) (*fs.StorageFee, error) {
	if opt.ProveInterval == 0 {
		return nil, sdkErr.New(sdkErr.INTERNAL_ERROR, "prove interval too small")
	}

	fee, err := e.sdk.EVM.Fs.GetUploadStorageFee(opt)
	if err != nil {
		return nil, sdkErr.NewWithError(sdkErr.CHAIN_ERROR, e.FormatError(err))
	}
	return fee, nil
}

func (e Ethereum) GetDeleteFilesStorageFee(addr chainCom.Address, fileHashStrs []string) (uint64, error) {
	fee, err := e.sdk.EVM.Fs.GetDeleteFilesStorageFee(fileHashStrs)
	if err != nil {
		return 0, sdkErr.NewWithError(sdkErr.CHAIN_ERROR, e.FormatError(err))
	}
	return fee, nil
}

func (e Ethereum) GetNodeList() (*fs.FsNodesInfo, error) {
	list, err := e.sdk.EVM.Fs.GetNodeList()
	if err != nil {
		return nil, sdkErr.NewWithError(sdkErr.CHAIN_ERROR, e.FormatError(err))
	}
	e.r.Shuffle(len(list.NodeInfo), func(i, j int) {
		list.NodeInfo[i], list.NodeInfo[j] = list.NodeInfo[j], list.NodeInfo[i]
	})
	return list, nil
}

func (e Ethereum) ProveParamSer(rootHash []byte, fileId pdp.FileID) ([]byte, error) {
	paramsBuf, err := e.sdk.EVM.Fs.ProveParamSer(rootHash, fileId)
	if err != nil {
		return nil, sdkErr.NewWithError(sdkErr.CHAIN_ERROR, e.FormatError(err))
	}
	return paramsBuf, nil
}

func (e Ethereum) ProveParamDes(buf []byte) (*fs.ProveParam, error) {
	arg, err := e.sdk.EVM.Fs.ProveParamDes(buf)
	if err != nil {
		return nil, sdkErr.NewWithError(sdkErr.CHAIN_ERROR, e.FormatError(err))
	}
	return arg, nil
}

func (e Ethereum) StoreFile(fileHashStr, blocksRoot string, blockNum, blockSizeInKB, proveLevel, expiredHeight, copyNum uint64, fileDesc []byte, privilege uint64, proveParam []byte, storageType, realFileSize uint64, primaryNodes, candidateNodes []chainCom.Address, plotInfo *fs.PlotInfo, url string) (string, uint32, error) {
	txHash, err := e.sdk.EVM.Fs.StoreFile(fileHashStr, blocksRoot, blockNum, blockSizeInKB, proveLevel,
		expiredHeight, copyNum, fileDesc, privilege, proveParam, storageType, realFileSize, primaryNodes, candidateNodes, plotInfo, url)
	if err != nil {
		return "", 0, sdkErr.NewWithError(sdkErr.CHAIN_ERROR, e.FormatError(err))
	}
	tx := hex.EncodeToString(txHash)
	height, err := e.sdk.EVM.Fs.Client.GetClient().GetBlockHeightByTxHash(tx)
	if err != nil {
		return "", 0, sdkErr.NewWithError(sdkErr.CHAIN_ERROR, e.FormatError(err))
	}
	return tx, height, nil
}

func (e Ethereum) DeleteFiles(files []string, gasLimit uint64) (string, error) {
	deleteFiles, err := e.sdk.EVM.Fs.DeleteFiles(files, gasLimit)
	if err != nil {
		return "", sdkErr.NewWithError(sdkErr.CHAIN_ERROR, e.FormatError(err))
	}
	return string(deleteFiles), nil
}

func (e Ethereum) DeleteUploadedFiles(fileHashStrs []string, gasLimit uint64) (string, uint32, error) {
	if len(fileHashStrs) == 0 {
		return "", 0, nil
	}
	needDeleteFile := false
	for _, fileHashStr := range fileHashStrs {
		info, err := e.GetFileInfo(fileHashStr)
		if err != nil {
			info = nil
		}
		log.Debugf("evm delete file get fileinfo %v, err %v", info, err)
		if info == nil {
			return "", 0, nil
		}
		address := ethCom.BytesToAddress(info.FileOwner[:])
		if info != nil && address.String() != e.WalletAddress() {
			return "", 0, sdkErr.New(sdkErr.DELETE_FILE_ACCESS_DENIED,
				"file %s can't be deleted, you are not the owner", fileHashStr)
		}
		if info != nil && err == nil {
			needDeleteFile = true
		}
	}
	if !needDeleteFile {
		return "", 0, sdkErr.New(sdkErr.NO_FILE_NEED_DELETED, "no file to delete")
	}
	txHashStr, err := e.DeleteFiles(fileHashStrs, gasLimit)
	if err != nil {
		log.Error("delete files error ", err)
		return "", 0, sdkErr.NewWithError(sdkErr.CHAIN_ERROR, e.FormatError(err))
	}
	log.Debugf("delete file txHash %s", txHashStr)
	// TODO wangyu height
	return txHashStr, 0, nil
}

func (e Ethereum) AddWhiteLists(fileHashStr string, whitelists []fs.Rule) (string, error) {
	txHash, err := e.sdk.EVM.Fs.AddWhiteLists(fileHashStr, whitelists)
	if err != nil {
		return "", sdkErr.NewWithError(sdkErr.CHAIN_ERROR, e.FormatError(err))
	}
	tx := hex.EncodeToString(txHash)
	_, err = e.PollForTxConfirmed(time.Duration(consts.TX_CONFIRM_TIMEOUT)*time.Second, tx)
	if err != nil {
		return "", err
	}
	return tx, nil
}

func (e Ethereum) GetFileProveDetails(fileHashStr string) (*fs.FsProveDetails, error) {
	details, err := e.sdk.EVM.Fs.GetFileProveDetails(fileHashStr)
	if err != nil {
		return nil, sdkErr.NewWithError(sdkErr.CHAIN_ERROR, e.FormatError(err))
	}
	return details, nil
}

func (e Ethereum) GetFileProveNodes(fileHashStr string) (map[string]uint64, error) {
	details, err := e.sdk.EVM.Fs.GetFileProveDetails(fileHashStr)
	if err != nil {
		return nil, sdkErr.NewWithError(sdkErr.CHAIN_ERROR, e.FormatError(err))
	}
	if err != nil || details == nil {
		return nil, sdkErr.New(sdkErr.INTERNAL_ERROR, "prove detail not exist %s, err %s", fileHashStr, err)
	}
	provedNodes := make(map[string]uint64, 0)
	for _, d := range details.ProveDetails {
		log.Debugf("wallet %v, node %v, prove times %d", d.WalletAddr.ToBase58(),
			string(d.NodeAddr), d.ProveTimes)
		if d.ProveTimes > 0 {
			provedNodes[d.WalletAddr.ToBase58()] = d.ProveTimes
		}
	}
	return provedNodes, nil
}

func (e Ethereum) GetFileList(addr chainCom.Address) (*fs.FileList, error) {
	list, err := e.sdk.EVM.Fs.GetFileList(addr)
	if err != nil {
		return nil, err
	}
	return list, nil
}

func (e Ethereum) GetFsSetting() (*fs.FsSetting, error) {
	setting, err := e.sdk.EVM.Fs.GetSetting()
	if err != nil {
		return nil, err
	}
	return setting, nil
}

func (e Ethereum) GetWhiteList(fileHashStr string) (*fs.WhiteList, error) {
	list, err := e.sdk.EVM.Fs.GetWhiteList(fileHashStr)
	if err != nil {
		return nil, sdkErr.NewWithError(sdkErr.CHAIN_ERROR, e.FormatError(err))
	}
	return list, nil
}

func (e Ethereum) WhiteListOp(fileHashStr string, op uint64, whiteList fs.WhiteList) (string, error) {
	txHash, err := e.sdk.EVM.Fs.WhiteListOp(fileHashStr, op, whiteList)
	if err != nil {
		return "", sdkErr.NewWithError(sdkErr.CHAIN_ERROR, e.FormatError(err))
	}
	return hex.EncodeToString(txHash), nil
}

func (e Ethereum) GetNodeInfoByWallet(walletAddr chainCom.Address) (*fs.FsNodeInfo, error) {
	infos, err := e.sdk.EVM.Fs.GetNodeListByAddrs([]chainCom.Address{walletAddr})
	if err != nil {
		return nil, sdkErr.NewWithError(sdkErr.CHAIN_ERROR, e.FormatError(err))
	}
	if infos.NodeNum != 1 {
		return nil, sdkErr.NewWithError(sdkErr.CHAIN_ERROR, fmt.Errorf("wrong node num %v", infos.NodeNum))
	}

	nodeInfo := infos.NodeInfo[0]
	p := &nodeInfo
	return p, nil
}

func (e Ethereum) GetNodeHostAddrListByWallets(nodeWalletAddrs []chainCom.Address) ([]string, error) {
	info, err := e.sdk.EVM.Fs.GetNodeListByAddrs(nodeWalletAddrs)
	if err != nil {
		return nil, sdkErr.NewWithError(sdkErr.CHAIN_ERROR, e.FormatError(err))
	}
	if info.NodeNum != uint64(len(nodeWalletAddrs)) {
		return nil, sdkErr.New(sdkErr.CHAIN_ERROR, "node num %d is not equal to request wallets length %d", info.NodeNum, len(nodeWalletAddrs))
	}
	hostAddrs := make([]string, 0, info.NodeNum)
	for _, node := range info.NodeInfo {
		hostAddrs = append(hostAddrs, string(node.NodeAddr))
	}
	return hostAddrs, nil
}

func (e Ethereum) GetNodeListWithoutAddrs(nodeWalletAddrs []chainCom.Address, num int) ([]chainCom.Address, error) {
	list, err := e.sdk.EVM.Fs.GetNodeList()
	if err != nil {
		return nil, sdkErr.NewWithError(sdkErr.CHAIN_ERROR, e.FormatError(err))
	}
	addrs := make([]chainCom.Address, 0, num)
	nodeWalletMap := make(map[chainCom.Address]struct{}, 0)
	for _, addr := range nodeWalletAddrs {
		nodeWalletMap[addr] = struct{}{}
	}
	for _, info := range list.NodeInfo {
		if _, ok := nodeWalletMap[info.WalletAddr]; ok {
			continue
		}
		addrs = append(addrs, info.WalletAddr)
		if len(addrs) >= num {
			break
		}
	}
	return addrs, nil
}

func (e Ethereum) GetUnprovePrimaryFileInfos(walletAddr chainCom.Address) ([]fs.FileInfo, error) {
	list, err := e.sdk.EVM.Fs.GetUnprovePrimaryFileList(walletAddr)
	if err != nil {
		return nil, sdkErr.NewWithError(sdkErr.CHAIN_ERROR, e.FormatError(err))
	}
	if list.FileNum == 0 || len(list.List) == 0 {
		return nil, nil
	}
	hashes := make([]string, 0, list.FileNum)
	for _, item := range list.List {
		hashes = append(hashes, string(item.Hash))
	}
	infoList, err := e.sdk.EVM.Fs.GetFileInfos(hashes)
	if err != nil {
		return nil, sdkErr.NewWithError(sdkErr.CHAIN_ERROR, e.FormatError(err))
	}
	return infoList.List, nil
}

func (e Ethereum) GetUnproveCandidateFileInfos(walletAddr chainCom.Address) ([]fs.FileInfo, error) {
	list, err := e.sdk.EVM.Fs.GetUnProveCandidateFileList(walletAddr)
	if err != nil {
		return nil, sdkErr.NewWithError(sdkErr.CHAIN_ERROR, e.FormatError(err))
	}
	if list.FileNum == 0 || len(list.List) == 0 {
		return nil, nil
	}
	hashes := make([]string, 0, list.FileNum)
	for _, item := range list.List {
		hashes = append(hashes, string(item.Hash))
	}
	infoList, err := e.sdk.EVM.Fs.GetFileInfos(hashes)
	if err != nil {
		return nil, sdkErr.NewWithError(sdkErr.CHAIN_ERROR, e.FormatError(err))
	}
	return infoList.List, nil
}

func (e Ethereum) CheckHasProveFile(fileHashStr string, walletAddr chainCom.Address) bool {
	details, err := e.sdk.EVM.Fs.GetFileProveDetails(fileHashStr)
	if err != nil {
		return false
	}
	for _, d := range details.ProveDetails {
		if d.WalletAddr.ToBase58() == walletAddr.ToBase58() && d.ProveTimes > 0 {
			return true
		}
	}
	return false
}

func (e Ethereum) CreateSector(sectorId uint64, proveLevel uint64, size uint64, isPlots bool) (string, error) {
	txHash, err := e.sdk.EVM.Fs.CreateSector(sectorId, proveLevel, size, isPlots)
	if err != nil {
		return "", sdkErr.NewWithError(sdkErr.CHAIN_ERROR, e.FormatError(err))
	}
	tx := hex.EncodeToString(txHash)
	return tx, nil
}

func (e Ethereum) DeleteSector(sectorId uint64) (string, error) {
	txHash, err := e.sdk.EVM.Fs.DeleteSector(sectorId)
	if err != nil {
		return "", sdkErr.NewWithError(sdkErr.CHAIN_ERROR, e.FormatError(err))
	}
	tx := hex.EncodeToString(txHash)
	return tx, nil
}

func (e Ethereum) GetSectorInfo(sectorId uint64) (*fs.SectorInfo, error) {
	sectorInfo, err := e.sdk.EVM.Fs.GetSectorInfo(sectorId)
	if err != nil {
		return nil, sdkErr.NewWithError(sdkErr.CHAIN_ERROR, e.FormatError(err))
	}
	return sectorInfo, nil
}

func (e Ethereum) GetSectorInfosForNode(walletAddr string) (*fs.SectorInfos, error) {
	ethAddress := ethCom.HexToAddress(walletAddr)
	address, err := chainCom.AddressParseFromBytes(ethAddress.Bytes())
	if err != nil {
		return nil, sdkErr.NewWithError(sdkErr.CHAIN_ERROR, e.FormatError(err))
	}
	if err != nil {
		return nil, sdkErr.NewWithError(sdkErr.CHAIN_ERROR, e.FormatError(err))
	}
	sectorInfos, err := e.sdk.EVM.Fs.GetSectorInfosForNode(address)
	if err != nil {
		return nil, sdkErr.NewWithError(sdkErr.CHAIN_ERROR, e.FormatError(err))
	}
	return sectorInfos, nil
}

func (e Ethereum) IsFileInfoDeleted(err error) bool {
	if err != nil && strings.Contains(
		strings.ToLower(err.Error()), strings.ToLower(errFileInfoNotFound.Error())) {
		return true
	}
	return false
}

func (e Ethereum) CheckFilePrivilege(info *fs.FileInfo, fileHashStr, walletAddr string) bool {
	// TODO: check sinature
	if info.FileOwner.ToBase58() == walletAddr {
		return true
	}
	if info.Privilege == fs.PUBLIC {
		return true
	}
	if info.Privilege == fs.PRIVATE {
		return false
	}
	whitelist, err := e.sdk.EVM.Fs.GetWhiteList(fileHashStr)
	if err != nil || whitelist == nil {
		return true
	}
	currentHeight, err := e.sdk.GetCurrentBlockHeight()
	if err != nil {
		return false
	}
	for _, r := range whitelist.List {
		if r.Addr.ToBase58() != walletAddr {
			continue
		}
		if r.BaseHeight <= uint64(currentHeight) && uint64(currentHeight) <= r.ExpireHeight {
			return true
		}
	}
	return false
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
	address, err := chainCom.AddressFromBase58(walletAddr)
	if err != nil {
		return "", sdkErr.NewWithError(sdkErr.CHAIN_ERROR, e.FormatError(err))
	}
	if size == 0 {
		sizeOpType = uint64(fs.UserSpaceNone)
	}
	if blockCount == 0 {
		countOpType = uint64(fs.UserSpaceNone)
	}
	txHash, err := e.sdk.EVM.Fs.UpdateUserSpace(address, &fs.UserSpaceOperation{
		Type:  sizeOpType,
		Value: size,
	}, &fs.UserSpaceOperation{
		Type:  countOpType,
		Value: blockCount,
	})
	if err != nil {
		return "", sdkErr.NewWithError(sdkErr.CHAIN_ERROR, e.FormatError(err))
	}
	tx := hex.EncodeToString(txHash)
	return tx, nil
}

func (e Ethereum) GetUpdateUserSpaceCost(walletAddr string, size, sizeOpType, blockCount, countOpType uint64) (*usdt.State, error) {
	address, err := chainCom.AddressFromBase58(walletAddr)
	if err != nil {
		return nil, sdkErr.NewWithError(sdkErr.CHAIN_ERROR, e.FormatError(err))
	}
	if size == 0 {
		sizeOpType = uint64(fs.UserSpaceNone)
	}
	if blockCount == 0 {
		countOpType = uint64(fs.UserSpaceNone)
	}
	state, err := e.sdk.EVM.Fs.GetUpdateSpaceCost(address, &fs.UserSpaceOperation{
		Type:  sizeOpType,
		Value: size,
	}, &fs.UserSpaceOperation{
		Type:  countOpType,
		Value: blockCount,
	})
	if err != nil {
		return nil, sdkErr.NewWithError(sdkErr.CHAIN_ERROR, e.FormatError(err))
	}
	return state, nil
}

func (e Ethereum) RegisterNode(addr string, volume, serviceTime uint64) (string, error) {
	register, err := e.sdk.EVM.Fs.NodeRegister(volume, serviceTime, addr)
	if err != nil {
		return "", err
	}
	return hexutil.Encode(register), nil
}

func (e Ethereum) NodeExit() (string, error) {
	cancel, err := e.sdk.EVM.Fs.NodeCancel()
	if err != nil {
		return "", err
	}
	return hexutil.Encode(cancel), nil
}

func (e Ethereum) QueryNode(walletAddr string) (*fs.FsNodeInfo, error) {
	toAddress := ethCom.HexToAddress(walletAddr)
	address, err := chainCom.AddressParseFromBytes(toAddress[:])
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
	base58 := ETHAddressToBase58(e.sdk.EVM.Fs.Client.GetDefaultAccount().EthAddress.Bytes())
	nodeInfo, err := e.QueryNode(base58)
	if err != nil {
		return "", sdkErr.NewWithError(sdkErr.CHAIN_ERROR, e.FormatError(err))
	}
	if volume == 0 {
		volume = nodeInfo.Volume
	}
	if volume < nodeInfo.Volume-nodeInfo.RestVol {
		return "", sdkErr.New(sdkErr.CHAIN_ERROR, "volume %d is less than original volume %d - restvol %d", volume, nodeInfo.Volume, nodeInfo.RestVol)
	}
	if serviceTime == 0 {
		serviceTime = nodeInfo.ServiceTime
	}
	if len(addr) == 0 {
		addr = string(nodeInfo.NodeAddr)
	}
	txHash, err := e.sdk.EVM.Fs.NodeUpdate(volume, serviceTime, addr)
	if err != nil {
		return "", sdkErr.NewWithError(sdkErr.CHAIN_ERROR, e.FormatError(err))
	}
	tx := hex.EncodeToString(txHash)
	return tx, nil
}

func (e Ethereum) NodeWithdrawProfit() (string, error) {
	profit, err := e.sdk.EVM.Fs.NodeWithDrawProfit()
	if err != nil {
		return "", err
	}
	return hexutil.Encode(profit), nil
}
