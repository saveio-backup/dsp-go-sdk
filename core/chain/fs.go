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
	errFileInfoNotFound = errors.New("[FS Profit] getFsFileInfo not found")
)

func (this *Chain) GetFileInfo(fileHashStr string) (*fs.FileInfo, error) {
	info, err := this.themis.Native.Fs.GetFileInfo(fileHashStr)
	if err != nil {
		if this.IsFileInfoDeleted(err) {
			return nil, sdkErr.NewWithError(sdkErr.FILE_NOT_FOUND_FROM_CHAIN, err)
		}
		return nil, sdkErr.NewWithError(sdkErr.CHAIN_ERROR, err)
	}
	return info, nil
}

func (this *Chain) GetFileInfos(fileHashStr []string) (*fs.FileInfoList, error) {
	list, err := this.themis.Native.Fs.GetFileInfos(fileHashStr)
	if err != nil {
		return nil, sdkErr.NewWithError(sdkErr.CHAIN_ERROR, err)
	}
	return list, nil
}

func (this *Chain) GetUploadStorageFee(opt *fs.UploadOption) (*fs.StorageFee, error) {
	if opt.ProveInterval == 0 {
		return nil, sdkErr.New(sdkErr.INTERNAL_ERROR, "prove interval too small")
	}

	fee, err := this.themis.Native.Fs.GetUploadStorageFee(opt)
	if err != nil {
		return nil, sdkErr.NewWithError(sdkErr.CHAIN_ERROR, err)
	}
	return fee, nil
}

func (this *Chain) GetDeleteFilesStorageFee(addr chainCom.Address, fileHashStrs []string) (uint64, error) {
	list, err := this.GetFileList(addr)
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

	fee, err := this.themis.Native.Fs.GetDeleteFilesStorageFee(fileListFound)
	if err != nil {
		return fee, sdkErr.NewWithError(sdkErr.CHAIN_ERROR, err)
	} else if len(fileListNotFound) != 0 {
		return fee, fmt.Errorf("files not found on chain, hashs: %v", fileListNotFound)
	}

	return fee, nil
}

func (this *Chain) GetNodeList() (*fs.FsNodesInfo, error) {
	list, err := this.themis.Native.Fs.GetNodeList()
	if err != nil {
		return nil, sdkErr.NewWithError(sdkErr.CHAIN_ERROR, err)
	}
	this.r.Shuffle(len(list.NodeInfo), func(i, j int) {
		list.NodeInfo[i], list.NodeInfo[j] = list.NodeInfo[j], list.NodeInfo[i]
	})
	return list, nil
}

func (this *Chain) ProveParamSer(rootHash []byte, fileId pdp.FileID) ([]byte, error) {
	paramsBuf, err := this.themis.Native.Fs.ProveParamSer(rootHash, fileId)
	if err != nil {
		return nil, sdkErr.NewWithError(sdkErr.CHAIN_ERROR, err)
	}
	return paramsBuf, nil
}

func (this *Chain) ProveParamDes(buf []byte) (*fs.ProveParam, error) {
	arg, err := this.themis.Native.Fs.ProveParamDes(buf)
	if err != nil {
		return nil, sdkErr.NewWithError(sdkErr.CHAIN_ERROR, err)
	}
	return arg, nil
}

func (this *Chain) StoreFile(
	fileHashStr, blocksRoot string,
	blockNum, blockSizeInKB, proveLevel, expiredHeight, copyNum uint64,
	fileDesc []byte, privilege uint64, proveParam []byte,
	storageType, realFileSize uint64,
	primaryNodes, candidateNodes []chainCom.Address,
) (string, uint32, error) {
	txHash, err := this.themis.Native.Fs.StoreFile(fileHashStr, blocksRoot, blockNum, blockSizeInKB, proveLevel,
		expiredHeight, copyNum, fileDesc, privilege, proveParam, storageType, realFileSize, primaryNodes, candidateNodes)
	if err != nil {
		return "", 0, sdkErr.NewWithError(sdkErr.CHAIN_ERROR, err)
	}
	tx := hex.EncodeToString(chainCom.ToArrayReverse(txHash))
	log.Debugf("store file txhash :%v, tx: %v", txHash, tx)

	txHeight, err := this.PollForTxConfirmed(time.Duration(consts.TX_CONFIRM_TIMEOUT)*time.Second, tx)
	if err != nil || txHeight == 0 {
		log.Errorf("poll tx failed %s", err)
		return "", 0, err
	}
	if this.blockConfirm > 0 {
		_, err = this.WaitForGenerateBlock(time.Duration(consts.TX_CONFIRM_TIMEOUT)*time.Second,
			this.blockConfirm)
		if err != nil {
			log.Errorf("wait for generate err %s", err)
			return "", 0, err
		}
	}

	return tx, txHeight, nil
}

func (this *Chain) DeleteFiles(files []string, gasLimit uint64) (string, error) {
	txHash, err := this.themis.Native.Fs.DeleteFiles(files, gasLimit)
	if err != nil {
		return "", sdkErr.NewWithError(sdkErr.CHAIN_ERROR, err)
	}
	tx := hex.EncodeToString(chainCom.ToArrayReverse(txHash))
	return tx, nil
}

func (this *Chain) DeleteUploadedFiles(fileHashStrs []string, gasLimit uint64) (string, uint32, error) {
	if len(fileHashStrs) == 0 {
		return "", 0, nil
	}
	needDeleteFile := false
	for _, fileHashStr := range fileHashStrs {
		info, err := this.GetFileInfo(fileHashStr)
		log.Debugf("delete file get fileinfo %v, err %v", info, err)
		if err != nil {
			if derr, ok := err.(*sdkErr.Error); ok && derr.Code != sdkErr.FILE_NOT_FOUND_FROM_CHAIN {
				log.Debugf("info:%v, other err:%s", info, err)
				return "", 0, sdkErr.New(sdkErr.FILE_NOT_FOUND_FROM_CHAIN,
					"file info not found, %s has deleted", fileHashStr)
			}
		}
		if info != nil && info.FileOwner.ToBase58() != this.WalletAddress() {
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
	txHashStr, err := this.DeleteFiles(fileHashStrs, gasLimit)
	log.Debugf("delete file tx %v, err %v", txHashStr, err)
	if err != nil {
		return "", 0, sdkErr.NewWithError(sdkErr.CHAIN_ERROR, err)
	}
	log.Debugf("delete file txHash %s", txHashStr)
	txHeight, err := this.PollForTxConfirmed(time.Duration(consts.TX_CONFIRM_TIMEOUT)*time.Second, txHashStr)
	if err != nil || txHeight == 0 {
		return "", 0, sdkErr.New(sdkErr.CHAIN_ERROR, "wait for tx confirmed failed")
	}
	log.Debugf("delete file tx height %d, err %v", txHeight, err)
	if err != nil {
		return "", 0, sdkErr.NewWithError(sdkErr.CHAIN_ERROR, err)
	}
	return txHashStr, txHeight, nil
}

func (this *Chain) AddWhiteLists(fileHashStr string, whitelists []fs.Rule) (string, error) {
	txHash, err := this.themis.Native.Fs.AddWhiteLists(fileHashStr, whitelists)
	if err != nil {
		return "", sdkErr.NewWithError(sdkErr.CHAIN_ERROR, err)
	}
	tx := hex.EncodeToString(chainCom.ToArrayReverse(txHash))
	_, err = this.PollForTxConfirmed(time.Duration(consts.TX_CONFIRM_TIMEOUT)*time.Second, tx)
	if err != nil {
		return "", err
	}
	return tx, nil
}

func (this *Chain) GetFileProveDetails(fileHashStr string) (*fs.FsProveDetails, error) {
	details, err := this.themis.Native.Fs.GetFileProveDetails(fileHashStr)
	if err != nil {
		return nil, sdkErr.NewWithError(sdkErr.CHAIN_ERROR, err)
	}
	return details, nil
}

func (this *Chain) GetFileProveNodes(fileHashStr string) (map[string]uint64, error) {
	details, err := this.themis.Native.Fs.GetFileProveDetails(fileHashStr)
	if err != nil {
		return nil, sdkErr.NewWithError(sdkErr.CHAIN_ERROR, err)
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

func (this *Chain) GetFileList(addr chainCom.Address) (*fs.FileList, error) {
	list, err := this.themis.Native.Fs.GetFileList(addr)
	if err != nil {
		return nil, sdkErr.NewWithError(sdkErr.CHAIN_ERROR, err)
	}
	return list, nil
}

func (this *Chain) GetFsSetting() (*fs.FsSetting, error) {
	set, err := this.themis.Native.Fs.GetSetting()
	if err != nil {
		return nil, sdkErr.NewWithError(sdkErr.CHAIN_ERROR, err)
	}
	return set, nil
}

func (this *Chain) GetWhiteList(fileHashStr string) (*fs.WhiteList, error) {
	list, err := this.themis.Native.Fs.GetWhiteList(fileHashStr)
	if err != nil {
		return nil, sdkErr.NewWithError(sdkErr.CHAIN_ERROR, err)
	}
	return list, nil
}

func (this *Chain) WhiteListOp(fileHashStr string, op uint64, whiteList fs.WhiteList) (string, error) {
	txHash, err := this.themis.Native.Fs.WhiteListOp(fileHashStr, op, whiteList)
	if err != nil {
		return "", sdkErr.NewWithError(sdkErr.CHAIN_ERROR, err)
	}
	return hex.EncodeToString(chainCom.ToArrayReverse(txHash)), nil
}

func (this *Chain) GetNodeHostAddrListByWallets(nodeWalletAddrs []chainCom.Address) ([]string, error) {
	info, err := this.themis.Native.Fs.GetNodeListByAddrs(nodeWalletAddrs)
	if err != nil {
		return nil, sdkErr.NewWithError(sdkErr.CHAIN_ERROR, err)
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

func (this *Chain) GetNodeListWithoutAddrs(nodeWalletAddrs []chainCom.Address, num int) ([]chainCom.Address, error) {
	list, err := this.themis.Native.Fs.GetNodeList()
	if err != nil {
		return nil, sdkErr.NewWithError(sdkErr.CHAIN_ERROR, err)
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

func (this *Chain) GetUnprovePrimaryFileInfos(walletAddr chainCom.Address) ([]fs.FileInfo, error) {
	list, err := this.themis.Native.Fs.GetUnprovePrimaryFileList(walletAddr)
	if err != nil {
		return nil, sdkErr.NewWithError(sdkErr.CHAIN_ERROR, err)
	}
	if list.FileNum == 0 || len(list.List) == 0 {
		return nil, nil
	}
	hashes := make([]string, 0, list.FileNum)
	for _, item := range list.List {
		hashes = append(hashes, string(item.Hash))
	}
	infoList, err := this.themis.Native.Fs.GetFileInfos(hashes)
	if err != nil {
		return nil, sdkErr.NewWithError(sdkErr.CHAIN_ERROR, err)
	}
	return infoList.List, nil
}

func (this *Chain) GetUnproveCandidateFileInfos(walletAddr chainCom.Address) ([]fs.FileInfo, error) {
	list, err := this.themis.Native.Fs.GetUnProveCandidateFileList(walletAddr)
	if err != nil {
		return nil, sdkErr.NewWithError(sdkErr.CHAIN_ERROR, err)
	}
	if list.FileNum == 0 || len(list.List) == 0 {
		return nil, nil
	}
	hashes := make([]string, 0, list.FileNum)
	for _, item := range list.List {
		hashes = append(hashes, string(item.Hash))
	}
	infoList, err := this.themis.Native.Fs.GetFileInfos(hashes)
	if err != nil {
		return nil, sdkErr.NewWithError(sdkErr.CHAIN_ERROR, err)
	}
	return infoList.List, nil
}

func (this *Chain) CheckHasProveFile(fileHashStr string, walletAddr chainCom.Address) bool {
	details, err := this.themis.Native.Fs.GetFileProveDetails(fileHashStr)
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

func (this *Chain) CreateSector(sectorId uint64, proveLevel uint64, size uint64) (string, error) {
	txHash, err := this.themis.Native.Fs.CreateSector(sectorId, proveLevel, size)
	if err != nil {
		return "", sdkErr.NewWithError(sdkErr.CHAIN_ERROR, err)
	}
	tx := hex.EncodeToString(chainCom.ToArrayReverse(txHash))
	return tx, nil
}

func (this *Chain) DeleteSector(sectorId uint64) (string, error) {
	txHash, err := this.themis.Native.Fs.DeleteSector(sectorId)
	if err != nil {
		return "", sdkErr.NewWithError(sdkErr.CHAIN_ERROR, err)
	}
	tx := hex.EncodeToString(chainCom.ToArrayReverse(txHash))
	return tx, nil
}

func (this *Chain) GetSectorInfo(sectorId uint64) (*fs.SectorInfo, error) {
	sectorInfo, err := this.themis.Native.Fs.GetSectorInfo(sectorId)
	if err != nil {
		return nil, sdkErr.NewWithError(sdkErr.CHAIN_ERROR, err)
	}
	return sectorInfo, nil
}

func (this *Chain) GetSectorInfosForNode(walletAddr string) (*fs.SectorInfos, error) {
	address, err := chainCom.AddressFromBase58(walletAddr)
	if err != nil {
		return nil, sdkErr.NewWithError(sdkErr.CHAIN_ERROR, err)
	}
	sectorInfos, err := this.themis.Native.Fs.GetSectorInfosForNode(address)
	if err != nil {
		return nil, sdkErr.NewWithError(sdkErr.CHAIN_ERROR, err)
	}
	return sectorInfos, nil
}

func (this *Chain) IsFileInfoDeleted(err error) bool {
	if err != nil && strings.Contains(
		strings.ToLower(err.Error()), strings.ToLower(errFileInfoNotFound.Error())) {
		return true
	}
	return false
}

// CheckFilePrivilege. check if the downloader has privilege to download file
func (this *Chain) CheckFilePrivilege(info *fs.FileInfo, fileHashStr, walletAddr string) bool {
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
	whitelist, err := this.themis.Native.Fs.GetWhiteList(fileHashStr)
	if err != nil || whitelist == nil {
		return true
	}
	currentHeight, err := this.themis.GetCurrentBlockHeight()
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
func (this *Chain) GetUserSpace(walletAddr string) (*fs.UserSpace, error) {
	address, err := chainCom.AddressFromBase58(walletAddr)
	if err != nil {
		return nil, sdkErr.NewWithError(sdkErr.CHAIN_ERROR, err)
	}
	us, err := this.themis.Native.Fs.GetUserSpace(address)
	if err != nil {
		return nil, sdkErr.NewWithError(sdkErr.CHAIN_ERROR, err)
	}
	return us, nil
}

func (this *Chain) UpdateUserSpace(walletAddr string, size, sizeOpType, blockCount, countOpType uint64) (string, error) {
	address, err := chainCom.AddressFromBase58(walletAddr)
	if err != nil {
		return "", sdkErr.NewWithError(sdkErr.CHAIN_ERROR, err)
	}
	if size == 0 {
		sizeOpType = uint64(fs.UserSpaceNone)
	}
	if blockCount == 0 {
		countOpType = uint64(fs.UserSpaceNone)
	}
	txHash, err := this.themis.Native.Fs.UpdateUserSpace(address, &fs.UserSpaceOperation{
		Type:  sizeOpType,
		Value: size,
	}, &fs.UserSpaceOperation{
		Type:  countOpType,
		Value: blockCount,
	})
	if err != nil {
		return "", sdkErr.NewWithError(sdkErr.CHAIN_ERROR, err)
	}
	tx := hex.EncodeToString(chainCom.ToArrayReverse(txHash))
	return tx, nil
}

func (this *Chain) GetUpdateUserSpaceCost(walletAddr string, size, sizeOpType, blockCount, countOpType uint64) (*usdt.State, error) {
	address, err := chainCom.AddressFromBase58(walletAddr)
	if err != nil {
		return nil, sdkErr.NewWithError(sdkErr.CHAIN_ERROR, err)
	}
	if size == 0 {
		sizeOpType = uint64(fs.UserSpaceNone)
	}
	if blockCount == 0 {
		countOpType = uint64(fs.UserSpaceNone)
	}
	state, err := this.themis.Native.Fs.GetUpdateSpaceCost(address, &fs.UserSpaceOperation{
		Type:  sizeOpType,
		Value: size,
	}, &fs.UserSpaceOperation{
		Type:  countOpType,
		Value: blockCount,
	})
	if err != nil {
		return nil, sdkErr.NewWithError(sdkErr.CHAIN_ERROR, err)
	}
	return state, nil
}
