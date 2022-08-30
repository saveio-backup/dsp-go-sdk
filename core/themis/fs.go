package chain

import (
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/saveio/dsp-go-sdk/consts"
	sdkErr "github.com/saveio/dsp-go-sdk/error"
	chainCom "github.com/saveio/themis/common"
	"github.com/saveio/themis/common/log"
	fs "github.com/saveio/themis/smartcontract/service/native/savefs"
	"github.com/saveio/themis/smartcontract/service/native/savefs/pdp"
	"github.com/saveio/themis/smartcontract/service/native/usdt"
)

var (
	errFileInfoNotFound = errors.New("FsGetFileInfo getFsFileInfo error")
)

func (t *Themis) GetFileInfo(fileHashStr string) (*fs.FileInfo, error) {
	info, err := t.sdk.Native.Fs.GetFileInfo(fileHashStr)
	if err != nil {
		if t.IsFileInfoDeleted(err) {
			return nil, sdkErr.NewWithError(sdkErr.FILE_NOT_FOUND_FROM_CHAIN, err)
		}
		return nil, sdkErr.NewWithError(sdkErr.CHAIN_ERROR, t.FormatError(err))
	}
	return info, nil
}

func (t *Themis) GetFileInfos(fileHashStr []string) (*fs.FileInfoList, error) {
	list, err := t.sdk.Native.Fs.GetFileInfos(fileHashStr)
	if err != nil {
		return nil, sdkErr.NewWithError(sdkErr.CHAIN_ERROR, t.FormatError(err))
	}
	return list, nil
}

func (t *Themis) GetUploadStorageFee(opt *fs.UploadOption) (*fs.StorageFee, error) {
	if opt.ProveInterval == 0 {
		return nil, sdkErr.New(sdkErr.INTERNAL_ERROR, "prove interval too small")
	}

	fee, err := t.sdk.Native.Fs.GetUploadStorageFee(opt)
	if err != nil {
		return nil, sdkErr.NewWithError(sdkErr.CHAIN_ERROR, t.FormatError(err))
	}
	return fee, nil
}

func (t *Themis) GetDeleteFilesStorageFee(addr chainCom.Address, fileHashStrs []string) (uint64, error) {
	list, err := t.GetFileList(addr)
	if err != nil {
		return 0, err
	}

	fileListFound, fileListNotFound := make([]string, 0, 0), make([]string, 0, 0)

	for _, hash := range fileHashStrs {
		found := false
		for _, fh := range list.List {
			if hash == string(fh.Hash) {
				found = true
				break
			}
		}
		if !found {
			fileListNotFound = append(fileListNotFound, hash)
		} else {
			fileListFound = append(fileListFound, hash)
		}
	}

	fee, err := t.sdk.Native.Fs.GetDeleteFilesStorageFee(fileListFound)
	if err != nil {
		return fee, sdkErr.NewWithError(sdkErr.CHAIN_ERROR, t.FormatError(err))
	} else if len(fileListNotFound) != 0 {
		return fee, fmt.Errorf("files not found on chain, hashs: %v", fileListNotFound)
	}

	return fee, nil
}

func (t *Themis) GetNodeList() (*fs.FsNodesInfo, error) {
	list, err := t.sdk.Native.Fs.GetNodeList()
	if err != nil {
		return nil, sdkErr.NewWithError(sdkErr.CHAIN_ERROR, t.FormatError(err))
	}
	t.r.Shuffle(len(list.NodeInfo), func(i, j int) {
		list.NodeInfo[i], list.NodeInfo[j] = list.NodeInfo[j], list.NodeInfo[i]
	})
	return list, nil
}

func (t *Themis) ProveParamSer(rootHash []byte, fileId pdp.FileID) ([]byte, error) {
	paramsBuf, err := t.sdk.Native.Fs.ProveParamSer(rootHash, fileId)
	if err != nil {
		return nil, sdkErr.NewWithError(sdkErr.CHAIN_ERROR, t.FormatError(err))
	}
	return paramsBuf, nil
}

func (t *Themis) ProveParamDes(buf []byte) (*fs.ProveParam, error) {
	arg, err := t.sdk.Native.Fs.ProveParamDes(buf)
	if err != nil {
		return nil, sdkErr.NewWithError(sdkErr.CHAIN_ERROR, t.FormatError(err))
	}
	return arg, nil
}

func (t *Themis) StoreFile(
	fileHashStr, blocksRoot string,
	blockNum, blockSizeInKB, proveLevel, expiredHeight, copyNum uint64,
	fileDesc []byte, privilege uint64, proveParam []byte,
	storageType, realFileSize uint64,
	primaryNodes, candidateNodes []chainCom.Address, plotInfo *fs.PlotInfo,url string,
) (string, uint32, error) {
	txHash, err := t.sdk.Native.Fs.StoreFile(fileHashStr, blocksRoot, blockNum, blockSizeInKB, proveLevel,
		expiredHeight, copyNum, fileDesc, privilege, proveParam, storageType, realFileSize, primaryNodes, candidateNodes, plotInfo,url)
	if err != nil {
		return "", 0, sdkErr.NewWithError(sdkErr.CHAIN_ERROR, t.FormatError(err))
	}
	tx := hex.EncodeToString(chainCom.ToArrayReverse(txHash))
	log.Debugf("store file txhash :%v, tx: %v", txHash, tx)

	txHeight, err := t.PollForTxConfirmed(time.Duration(consts.TX_CONFIRM_TIMEOUT)*time.Second, tx)
	if err != nil || txHeight == 0 {
		log.Errorf("poll tx failed %s", err)
		return "", 0, err
	}
	if t.blockConfirm > 0 {
		_, err = t.WaitForGenerateBlock(time.Duration(consts.TX_CONFIRM_TIMEOUT)*time.Second,
			t.blockConfirm)
		if err != nil {
			log.Errorf("wait for generate err %s", err)
			return "", 0, err
		}
	}

	return tx, txHeight, nil
}

func (t *Themis) DeleteFiles(files []string, gasLimit uint64) (string, error) {
	txHash, err := t.sdk.Native.Fs.DeleteFiles(files, gasLimit)
	if err != nil {
		return "", sdkErr.NewWithError(sdkErr.CHAIN_ERROR, t.FormatError(err))
	}
	tx := hex.EncodeToString(chainCom.ToArrayReverse(txHash))
	return tx, nil
}

func (t *Themis) DeleteUploadedFiles(fileHashStrs []string, gasLimit uint64) (string, uint32, error) {
	if len(fileHashStrs) == 0 {
		return "", 0, nil
	}
	needDeleteFile := false
	for _, fileHashStr := range fileHashStrs {
		info, err := t.GetFileInfo(fileHashStr)
		log.Debugf("delete file get fileinfo %v, err %v", info, err)
		if err != nil {
			if derr, ok := err.(*sdkErr.Error); ok && derr.Code != sdkErr.FILE_NOT_FOUND_FROM_CHAIN {
				log.Debugf("info:%v, other err:%s", info, err)
				return "", 0, sdkErr.New(sdkErr.FILE_NOT_FOUND_FROM_CHAIN,
					"file info not found, %s has deleted", fileHashStr)
			}
		}
		if info != nil && info.FileOwner.ToBase58() != t.WalletAddress() {
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
	txHashStr, err := t.DeleteFiles(fileHashStrs, gasLimit)
	log.Debugf("delete file tx %v, err %v", txHashStr, err)
	if err != nil {
		return "", 0, sdkErr.NewWithError(sdkErr.CHAIN_ERROR, t.FormatError(err))
	}
	log.Debugf("delete file txHash %s", txHashStr)
	txHeight, err := t.PollForTxConfirmed(time.Duration(consts.TX_CONFIRM_TIMEOUT)*time.Second, txHashStr)
	if err != nil || txHeight == 0 {
		return "", 0, sdkErr.New(sdkErr.CHAIN_ERROR, "wait for tx confirmed failed")
	}
	log.Debugf("delete file tx height %d, err %v", txHeight, err)
	if err != nil {
		return "", 0, sdkErr.NewWithError(sdkErr.CHAIN_ERROR, t.FormatError(err))
	}
	return txHashStr, txHeight, nil
}

func (t *Themis) AddWhiteLists(fileHashStr string, whitelists []fs.Rule) (string, error) {
	txHash, err := t.sdk.Native.Fs.AddWhiteLists(fileHashStr, whitelists)
	if err != nil {
		return "", sdkErr.NewWithError(sdkErr.CHAIN_ERROR, t.FormatError(err))
	}
	tx := hex.EncodeToString(chainCom.ToArrayReverse(txHash))
	_, err = t.PollForTxConfirmed(time.Duration(consts.TX_CONFIRM_TIMEOUT)*time.Second, tx)
	if err != nil {
		return "", err
	}
	return tx, nil
}

func (t *Themis) GetFileProveDetails(fileHashStr string) (*fs.FsProveDetails, error) {
	details, err := t.sdk.Native.Fs.GetFileProveDetails(fileHashStr)
	if err != nil {
		return nil, sdkErr.NewWithError(sdkErr.CHAIN_ERROR, t.FormatError(err))
	}
	return details, nil
}

func (t *Themis) GetFileProveNodes(fileHashStr string) (map[string]uint64, error) {
	details, err := t.sdk.Native.Fs.GetFileProveDetails(fileHashStr)
	if err != nil {
		return nil, sdkErr.NewWithError(sdkErr.CHAIN_ERROR, t.FormatError(err))
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

func (t *Themis) GetFileList(addr chainCom.Address) (*fs.FileList, error) {
	list, err := t.sdk.Native.Fs.GetFileList(addr)
	if err != nil {
		return nil, sdkErr.NewWithError(sdkErr.CHAIN_ERROR, t.FormatError(err))
	}
	return list, nil
}

func (t *Themis) GetFsSetting() (*fs.FsSetting, error) {
	set, err := t.sdk.Native.Fs.GetSetting()
	if err != nil {
		return nil, sdkErr.NewWithError(sdkErr.CHAIN_ERROR, t.FormatError(err))
	}
	return set, nil
}

func (t *Themis) GetWhiteList(fileHashStr string) (*fs.WhiteList, error) {
	list, err := t.sdk.Native.Fs.GetWhiteList(fileHashStr)
	if err != nil {
		return nil, sdkErr.NewWithError(sdkErr.CHAIN_ERROR, t.FormatError(err))
	}
	return list, nil
}

func (t *Themis) WhiteListOp(fileHashStr string, op uint64, whiteList fs.WhiteList) (string, error) {
	txHash, err := t.sdk.Native.Fs.WhiteListOp(fileHashStr, op, whiteList)
	if err != nil {
		return "", sdkErr.NewWithError(sdkErr.CHAIN_ERROR, t.FormatError(err))
	}
	return hex.EncodeToString(chainCom.ToArrayReverse(txHash)), nil
}

func (t *Themis) GetNodeInfoByWallet(walletAddr chainCom.Address) (*fs.FsNodeInfo, error) {
	infos, err := t.sdk.Native.Fs.GetNodeListByAddrs([]chainCom.Address{walletAddr})
	if err != nil {
		return nil, sdkErr.NewWithError(sdkErr.CHAIN_ERROR, t.FormatError(err))
	}
	if infos.NodeNum != 1 {
		return nil, sdkErr.NewWithError(sdkErr.CHAIN_ERROR, fmt.Errorf("wrong node num %v", infos.NodeNum))
	}

	nodeInfo := infos.NodeInfo[0]
	p := &nodeInfo
	return p, nil
}

func (t *Themis) GetNodeHostAddrListByWallets(nodeWalletAddrs []chainCom.Address) ([]string, error) {
	info, err := t.sdk.Native.Fs.GetNodeListByAddrs(nodeWalletAddrs)
	if err != nil {
		return nil, sdkErr.NewWithError(sdkErr.CHAIN_ERROR, t.FormatError(err))
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

func (t *Themis) GetNodeListWithoutAddrs(nodeWalletAddrs []chainCom.Address, num int) ([]chainCom.Address, error) {
	list, err := t.sdk.Native.Fs.GetNodeList()
	if err != nil {
		return nil, sdkErr.NewWithError(sdkErr.CHAIN_ERROR, t.FormatError(err))
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

func (t *Themis) GetUnprovePrimaryFileInfos(walletAddr chainCom.Address) ([]fs.FileInfo, error) {
	list, err := t.sdk.Native.Fs.GetUnprovePrimaryFileList(walletAddr)
	if err != nil {
		return nil, sdkErr.NewWithError(sdkErr.CHAIN_ERROR, t.FormatError(err))
	}
	if list.FileNum == 0 || len(list.List) == 0 {
		return nil, nil
	}
	hashes := make([]string, 0, list.FileNum)
	for _, item := range list.List {
		hashes = append(hashes, string(item.Hash))
	}
	infoList, err := t.sdk.Native.Fs.GetFileInfos(hashes)
	if err != nil {
		return nil, sdkErr.NewWithError(sdkErr.CHAIN_ERROR, t.FormatError(err))
	}
	return infoList.List, nil
}

func (t *Themis) GetUnproveCandidateFileInfos(walletAddr chainCom.Address) ([]fs.FileInfo, error) {
	list, err := t.sdk.Native.Fs.GetUnProveCandidateFileList(walletAddr)
	if err != nil {
		return nil, sdkErr.NewWithError(sdkErr.CHAIN_ERROR, t.FormatError(err))
	}
	if list.FileNum == 0 || len(list.List) == 0 {
		return nil, nil
	}
	hashes := make([]string, 0, list.FileNum)
	for _, item := range list.List {
		hashes = append(hashes, string(item.Hash))
	}
	infoList, err := t.sdk.Native.Fs.GetFileInfos(hashes)
	if err != nil {
		return nil, sdkErr.NewWithError(sdkErr.CHAIN_ERROR, t.FormatError(err))
	}
	return infoList.List, nil
}

func (t *Themis) CheckHasProveFile(fileHashStr string, walletAddr chainCom.Address) bool {
	details, err := t.sdk.Native.Fs.GetFileProveDetails(fileHashStr)
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

func (t *Themis) CreateSector(sectorId uint64, proveLevel uint64, size uint64, isPlots bool) (string, error) {
	txHash, err := t.sdk.Native.Fs.CreateSector(sectorId, proveLevel, size, isPlots)
	if err != nil {
		return "", sdkErr.NewWithError(sdkErr.CHAIN_ERROR, t.FormatError(err))
	}
	tx := hex.EncodeToString(chainCom.ToArrayReverse(txHash))
	return tx, nil
}

func (t *Themis) DeleteSector(sectorId uint64) (string, error) {
	txHash, err := t.sdk.Native.Fs.DeleteSector(sectorId)
	if err != nil {
		return "", sdkErr.NewWithError(sdkErr.CHAIN_ERROR, t.FormatError(err))
	}
	tx := hex.EncodeToString(chainCom.ToArrayReverse(txHash))
	return tx, nil
}

func (t *Themis) GetSectorInfo(sectorId uint64) (*fs.SectorInfo, error) {
	sectorInfo, err := t.sdk.Native.Fs.GetSectorInfo(sectorId)
	if err != nil {
		return nil, sdkErr.NewWithError(sdkErr.CHAIN_ERROR, t.FormatError(err))
	}
	return sectorInfo, nil
}

func (t *Themis) GetSectorInfosForNode(walletAddr string) (*fs.SectorInfos, error) {
	address, err := chainCom.AddressFromBase58(walletAddr)
	if err != nil {
		return nil, sdkErr.NewWithError(sdkErr.CHAIN_ERROR, t.FormatError(err))
	}
	sectorInfos, err := t.sdk.Native.Fs.GetSectorInfosForNode(address)
	if err != nil {
		return nil, sdkErr.NewWithError(sdkErr.CHAIN_ERROR, t.FormatError(err))
	}
	return sectorInfos, nil
}

func (t *Themis) IsFileInfoDeleted(err error) bool {
	if err != nil && strings.Contains(
		strings.ToLower(err.Error()), strings.ToLower(errFileInfoNotFound.Error())) {
		return true
	}
	return false
}

// CheckFilePrivilege. check if the downloader has privilege to download file
func (t *Themis) CheckFilePrivilege(info *fs.FileInfo, fileHashStr, walletAddr string) bool {
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
	whitelist, err := t.sdk.Native.Fs.GetWhiteList(fileHashStr)
	if err != nil || whitelist == nil {
		return true
	}
	currentHeight, err := t.sdk.GetCurrentBlockHeight()
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

// GetUserSpace. get user space of client
func (t *Themis) GetUserSpace(walletAddr string) (*fs.UserSpace, error) {
	address, err := chainCom.AddressFromBase58(walletAddr)
	if err != nil {
		return nil, sdkErr.NewWithError(sdkErr.CHAIN_ERROR, t.FormatError(err))
	}
	us, err := t.sdk.Native.Fs.GetUserSpace(address)
	if err != nil {
		return nil, sdkErr.NewWithError(sdkErr.CHAIN_ERROR, t.FormatError(err))
	}
	return us, nil
}

func (t *Themis) UpdateUserSpace(walletAddr string, size, sizeOpType, blockCount, countOpType uint64) (string, error) {
	address, err := chainCom.AddressFromBase58(walletAddr)
	if err != nil {
		return "", sdkErr.NewWithError(sdkErr.CHAIN_ERROR, t.FormatError(err))
	}
	if size == 0 {
		sizeOpType = uint64(fs.UserSpaceNone)
	}
	if blockCount == 0 {
		countOpType = uint64(fs.UserSpaceNone)
	}
	txHash, err := t.sdk.Native.Fs.UpdateUserSpace(address, &fs.UserSpaceOperation{
		Type:  sizeOpType,
		Value: size,
	}, &fs.UserSpaceOperation{
		Type:  countOpType,
		Value: blockCount,
	})
	if err != nil {
		return "", sdkErr.NewWithError(sdkErr.CHAIN_ERROR, t.FormatError(err))
	}
	tx := hex.EncodeToString(chainCom.ToArrayReverse(txHash))
	return tx, nil
}

func (t *Themis) GetUpdateUserSpaceCost(walletAddr string, size, sizeOpType, blockCount, countOpType uint64) (*usdt.State, error) {
	address, err := chainCom.AddressFromBase58(walletAddr)
	if err != nil {
		return nil, sdkErr.NewWithError(sdkErr.CHAIN_ERROR, t.FormatError(err))
	}
	if size == 0 {
		sizeOpType = uint64(fs.UserSpaceNone)
	}
	if blockCount == 0 {
		countOpType = uint64(fs.UserSpaceNone)
	}
	state, err := t.sdk.Native.Fs.GetUpdateSpaceCost(address, &fs.UserSpaceOperation{
		Type:  sizeOpType,
		Value: size,
	}, &fs.UserSpaceOperation{
		Type:  countOpType,
		Value: blockCount,
	})
	if err != nil {
		return nil, sdkErr.NewWithError(sdkErr.CHAIN_ERROR, t.FormatError(err))
	}
	return state, nil
}
