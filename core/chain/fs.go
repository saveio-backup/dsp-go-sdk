package chain

import (
	chainCom "github.com/saveio/themis/common"
	fs "github.com/saveio/themis/smartcontract/service/native/savefs"
	"github.com/saveio/themis/smartcontract/service/native/savefs/pdp"
	"github.com/saveio/themis/smartcontract/service/native/usdt"
)

func (c *Chain) GetFileInfo(fileHashStr string) (*fs.FileInfo, error) {
	return c.client.GetFileInfo(fileHashStr)
}

func (c *Chain) GetFileInfos(fileHashStr []string) (*fs.FileInfoList, error) {
	return c.client.GetFileInfos(fileHashStr)
}

func (c *Chain) GetUploadStorageFee(opt *fs.UploadOption) (*fs.StorageFee, error) {
	return c.client.GetUploadStorageFee(opt)
}

func (c *Chain) GetDeleteFilesStorageFee(addr chainCom.Address, fileHashStrs []string) (uint64, error) {
	return c.client.GetDeleteFilesStorageFee(addr, fileHashStrs)
}

func (c *Chain) GetNodeList() (*fs.FsNodesInfo, error) {
	return c.client.GetNodeList()
}

func (c *Chain) ProveParamSer(rootHash []byte, fileId pdp.FileID) ([]byte, error) {
	return c.client.ProveParamSer(rootHash, fileId)
}

func (c *Chain) ProveParamDes(buf []byte) (*fs.ProveParam, error) {
	return c.client.ProveParamDes(buf)
}

func (c *Chain) StoreFile(
	fileHashStr, blocksRoot string,
	blockNum, blockSizeInKB, proveLevel, expiredHeight, copyNum uint64,
	fileDesc []byte, privilege uint64, proveParam []byte,
	storageType, realFileSize uint64,
	primaryNodes, candidateNodes []chainCom.Address, plotInfo *fs.PlotInfo,url string,
) (string, uint32, error) {
	return c.client.StoreFile(fileHashStr, blocksRoot, blockNum, blockSizeInKB, proveLevel, expiredHeight,
		copyNum, fileDesc, privilege, proveParam, storageType, realFileSize, primaryNodes, candidateNodes, plotInfo,url)
}

func (c *Chain) DeleteFiles(files []string, gasLimit uint64) (string, error) {
	return c.client.DeleteFiles(files, gasLimit)
}

func (c *Chain) DeleteUploadedFiles(fileHashStrs []string, gasLimit uint64) (string, uint32, error) {
	return c.client.DeleteUploadedFiles(fileHashStrs, gasLimit)
}

func (c *Chain) AddWhiteLists(fileHashStr string, whitelists []fs.Rule) (string, error) {
	return c.client.AddWhiteLists(fileHashStr, whitelists)
}

func (c *Chain) GetFileProveDetails(fileHashStr string) (*fs.FsProveDetails, error) {
	return c.client.GetFileProveDetails(fileHashStr)
}

func (c *Chain) GetFileProveNodes(fileHashStr string) (map[string]uint64, error) {
	return c.client.GetFileProveNodes(fileHashStr)
}

func (c *Chain) GetFileList(addr chainCom.Address) (*fs.FileList, error) {
	return c.client.GetFileList(addr)
}

func (c *Chain) GetFsSetting() (*fs.FsSetting, error) {
	return c.client.GetFsSetting()
}

func (c *Chain) GetWhiteList(fileHashStr string) (*fs.WhiteList, error) {
	return c.client.GetWhiteList(fileHashStr)
}

func (c *Chain) WhiteListOp(fileHashStr string, op uint64, whiteList fs.WhiteList) (string, error) {
	return c.client.WhiteListOp(fileHashStr, op, whiteList)
}

func (c *Chain) GetNodeInfoByWallet(walletAddr chainCom.Address) (*fs.FsNodeInfo, error) {
	return c.client.GetNodeInfoByWallet(walletAddr)
}

func (c *Chain) GetNodeHostAddrListByWallets(nodeWalletAddrs []chainCom.Address) ([]string, error) {
	return c.client.GetNodeHostAddrListByWallets(nodeWalletAddrs)
}

func (c *Chain) GetNodeListWithoutAddrs(nodeWalletAddrs []chainCom.Address, num int) ([]chainCom.Address, error) {
	return c.client.GetNodeListWithoutAddrs(nodeWalletAddrs, num)
}

func (c *Chain) GetUnprovePrimaryFileInfos(walletAddr chainCom.Address) ([]fs.FileInfo, error) {
	return c.client.GetUnprovePrimaryFileInfos(walletAddr)
}

func (c *Chain) GetUnproveCandidateFileInfos(walletAddr chainCom.Address) ([]fs.FileInfo, error) {
	return c.client.GetUnproveCandidateFileInfos(walletAddr)
}

func (c *Chain) CheckHasProveFile(fileHashStr string, walletAddr chainCom.Address) bool {
	return c.client.CheckHasProveFile(fileHashStr, walletAddr)
}

func (c *Chain) CreateSector(sectorId uint64, proveLevel uint64, size uint64, isPlots bool) (string, error) {
	return c.client.CreateSector(sectorId, proveLevel, size, isPlots)
}

func (c *Chain) DeleteSector(sectorId uint64) (string, error) {
	return c.client.DeleteSector(sectorId)
}

func (c *Chain) GetSectorInfo(sectorId uint64) (*fs.SectorInfo, error) {
	return c.client.GetSectorInfo(sectorId)
}

func (c *Chain) GetSectorInfosForNode(walletAddr string) (*fs.SectorInfos, error) {
	return c.client.GetSectorInfosForNode(walletAddr)
}

func (c *Chain) IsFileInfoDeleted(err error) bool {
	return c.client.IsFileInfoDeleted(err)
}

// CheckFilePrivilege. check if the downloader has privilege to download file
func (c *Chain) CheckFilePrivilege(info *fs.FileInfo, fileHashStr, walletAddr string) bool {
	return c.client.CheckFilePrivilege(info, fileHashStr, walletAddr)
}

// GetUserSpace. get user space of client
func (c *Chain) GetUserSpace(walletAddr string) (*fs.UserSpace, error) {
	return c.client.GetUserSpace(walletAddr)
}

func (c *Chain) UpdateUserSpace(walletAddr string, size, sizeOpType, blockCount, countOpType uint64) (string, error) {
	return c.client.UpdateUserSpace(walletAddr, size, sizeOpType, blockCount, countOpType)
}

func (c *Chain) GetUpdateUserSpaceCost(walletAddr string, size, sizeOpType, blockCount, countOpType uint64) (*usdt.State, error) {
	return c.client.GetUpdateUserSpaceCost(walletAddr, size, sizeOpType, blockCount, countOpType)
}
