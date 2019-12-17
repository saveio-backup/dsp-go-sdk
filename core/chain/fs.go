package chain

import (
	"encoding/hex"

	dspErr "github.com/saveio/dsp-go-sdk/error"
	chainCom "github.com/saveio/themis/common"
	"github.com/saveio/themis/common/log"
	fs "github.com/saveio/themis/smartcontract/service/native/savefs"
)

func (this *Chain) GetFileInfo(fileHashStr string) (*fs.FileInfo, error) {
	info, err := this.themis.Native.Fs.GetFileInfo(fileHashStr)
	if err != nil {
		return nil, dspErr.NewWithError(dspErr.CHAIN_ERROR, err)
	}
	return info, nil
}

func (this *Chain) GetFileInfos(fileHashStr []string) (*fs.FileInfoList, error) {
	list, err := this.themis.Native.Fs.GetFileInfos(fileHashStr)
	if err != nil {
		return nil, dspErr.NewWithError(dspErr.CHAIN_ERROR, err)
	}
	return list, nil
}

func (this *Chain) GetExpiredProveList() (*fs.BakTasks, error) {
	tasks, err := this.themis.Native.Fs.GetExpiredProveList()
	if err != nil {
		return nil, dspErr.NewWithError(dspErr.CHAIN_ERROR, err)
	}
	return tasks, nil
}

func (this *Chain) GetUploadStorageFee(opt *fs.UploadOption) (*fs.StorageFee, error) {
	fee, err := this.themis.Native.Fs.GetUploadStorageFee(opt)
	if err != nil {
		return nil, dspErr.NewWithError(dspErr.CHAIN_ERROR, err)
	}
	return fee, nil
}

func (this *Chain) GetNodeList() (*fs.FsNodesInfo, error) {
	list, err := this.themis.Native.Fs.GetNodeList()
	if err != nil {
		return nil, dspErr.NewWithError(dspErr.CHAIN_ERROR, err)
	}
	this.r.Shuffle(len(list.NodeInfo), func(i, j int) {
		list.NodeInfo[i], list.NodeInfo[j] = list.NodeInfo[j], list.NodeInfo[i]
	})
	return list, nil
}

func (this *Chain) ProveParamSer(g, g0, pubKey, fileId []byte) ([]byte, error) {
	paramsBuf, err := this.themis.Native.Fs.ProveParamSer(g, g0, pubKey, fileId)
	if err != nil {
		return nil, dspErr.NewWithError(dspErr.CHAIN_ERROR, err)
	}
	return paramsBuf, nil
}

func (this *Chain) ProveParamDes(buf []byte) (*fs.ProveParam, error) {
	arg, err := this.themis.Native.Fs.ProveParamDes(buf)
	if err != nil {
		return nil, dspErr.NewWithError(dspErr.CHAIN_ERROR, err)
	}
	return arg, nil
}

func (this *Chain) StoreFile(fileHashStr string, blockNum, blockSizeInKB, proveInterval, expiredHeight, copyNum uint64,
	fileDesc []byte, privilege uint64, proveParam []byte, storageType, realFileSize uint64, primaryNodes, candidateNodes []chainCom.Address) (string, error) {
	txHash, err := this.themis.Native.Fs.StoreFile(fileHashStr, blockNum, blockSizeInKB, proveInterval,
		expiredHeight, copyNum, fileDesc, privilege, proveParam, storageType, realFileSize, primaryNodes, candidateNodes)
	if err != nil {
		return "", dspErr.NewWithError(dspErr.CHAIN_ERROR, err)
	}
	tx := hex.EncodeToString(chainCom.ToArrayReverse(txHash))
	log.Debugf("store file txhash :%v, tx: %v", txHash, tx)
	return tx, nil
}

func (this *Chain) DeleteFiles(files []string) (string, error) {
	txHash, err := this.themis.Native.Fs.DeleteFiles(files)
	if err != nil {
		return "", dspErr.NewWithError(dspErr.CHAIN_ERROR, err)
	}
	tx := hex.EncodeToString(chainCom.ToArrayReverse(txHash))
	return tx, nil
}

func (this *Chain) AddWhiteLists(fileHashStr string, whitelists []fs.Rule) (string, error) {
	txHash, err := this.themis.Native.Fs.AddWhiteLists(fileHashStr, whitelists)
	if err != nil {
		return "", dspErr.NewWithError(dspErr.CHAIN_ERROR, err)
	}
	tx := hex.EncodeToString(chainCom.ToArrayReverse(txHash))
	return tx, nil
}

func (this *Chain) GetFileProveDetails(fileHashStr string) (*fs.FsProveDetails, error) {
	details, err := this.themis.Native.Fs.GetFileProveDetails(fileHashStr)
	if err != nil {
		return nil, dspErr.NewWithError(dspErr.CHAIN_ERROR, err)
	}
	return details, nil
}

func (this *Chain) GetFileList(addr chainCom.Address) (*fs.FileList, error) {
	list, err := this.themis.Native.Fs.GetFileList(addr)
	if err != nil {
		return nil, dspErr.NewWithError(dspErr.CHAIN_ERROR, err)
	}
	return list, nil
}

func (this *Chain) GetFsSetting() (*fs.FsSetting, error) {
	set, err := this.themis.Native.Fs.GetSetting()
	if err != nil {
		return nil, dspErr.NewWithError(dspErr.CHAIN_ERROR, err)
	}
	return set, nil
}

func (this *Chain) GetWhiteList(fileHashStr string) (*fs.WhiteList, error) {
	list, err := this.themis.Native.Fs.GetWhiteList(fileHashStr)
	if err != nil {
		return nil, dspErr.NewWithError(dspErr.CHAIN_ERROR, err)
	}
	return list, nil
}

func (this *Chain) WhiteListOp(fileHashStr string, op uint64, whiteList fs.WhiteList) (string, error) {
	txHash, err := this.themis.Native.Fs.WhiteListOp(fileHashStr, op, whiteList)
	if err != nil {
		return "", dspErr.NewWithError(dspErr.CHAIN_ERROR, err)
	}
	return hex.EncodeToString(chainCom.ToArrayReverse(txHash)), nil
}

func (this *Chain) GetNodeHostAddrListByWallets(nodeWalletAddrs []chainCom.Address) ([]string, error) {
	info, err := this.themis.Native.Fs.GetNodeListByAddrs(nodeWalletAddrs)
	if err != nil {
		return nil, dspErr.NewWithError(dspErr.CHAIN_ERROR, err)
	}
	if info.NodeNum != uint64(len(nodeWalletAddrs)) {
		return nil, dspErr.New(dspErr.CHAIN_ERROR, "node num %d is not equal to request wallets length %d", info.NodeNum, len(nodeWalletAddrs))
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
		return nil, dspErr.NewWithError(dspErr.CHAIN_ERROR, err)
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
		return nil, dspErr.NewWithError(dspErr.CHAIN_ERROR, err)
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
		return nil, dspErr.NewWithError(dspErr.CHAIN_ERROR, err)
	}
	return infoList.List, nil
}

func (this *Chain) GetUnproveCandidateFileInfos(walletAddr chainCom.Address) ([]fs.FileInfo, error) {
	list, err := this.themis.Native.Fs.GetUnProveCandidateFileList(walletAddr)
	if err != nil {
		return nil, dspErr.NewWithError(dspErr.CHAIN_ERROR, err)
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
		return nil, dspErr.NewWithError(dspErr.CHAIN_ERROR, err)
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
