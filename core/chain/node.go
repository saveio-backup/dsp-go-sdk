package chain

import (
	"encoding/hex"
	"strings"

	dspErr "github.com/saveio/dsp-go-sdk/error"
	chainCom "github.com/saveio/themis/common"
	fs "github.com/saveio/themis/smartcontract/service/native/savefs"
	"github.com/saveio/themis/smartcontract/service/native/usdt"
)

// RegisterNode. register node to chain
func (this *Chain) RegisterNode(addr string, volume, serviceTime uint64) (string, error) {
	txHash, err := this.themis.Native.Fs.NodeRegister(volume, serviceTime, addr)
	if err != nil {
		return "", dspErr.NewWithError(dspErr.CHAIN_ERROR, err)
	}
	tx := hex.EncodeToString(chainCom.ToArrayReverse(txHash))
	return tx, nil
}

// UnregisterNode. unregister node to chain
func (this *Chain) UnregisterNode() (string, error) {
	txHash, err := this.themis.Native.Fs.NodeCancel()
	if err != nil {
		return "", dspErr.NewWithError(dspErr.CHAIN_ERROR, err)
	}
	tx := hex.EncodeToString(chainCom.ToArrayReverse(txHash))
	return tx, nil
}

// QueryNode. query node information by wallet address
func (this *Chain) QueryNode(walletAddr string) (*fs.FsNodeInfo, error) {
	address, err := chainCom.AddressFromBase58(walletAddr)
	if err != nil {
		return nil, dspErr.NewWithError(dspErr.CHAIN_ERROR, err)
	}
	info, err := this.themis.Native.Fs.NodeQuery(address)
	if err != nil {
		return nil, dspErr.NewWithError(dspErr.CHAIN_ERROR, err)
	}
	return info, nil
}

// UpdateNode. update node information
func (this *Chain) UpdateNode(addr string, volume, serviceTime uint64) (string, error) {
	nodeInfo, err := this.QueryNode(this.themis.Native.Fs.DefAcc.Address.ToBase58())
	if err != nil {
		return "", dspErr.NewWithError(dspErr.CHAIN_ERROR, err)
	}
	if volume == 0 {
		volume = nodeInfo.Volume
	}
	if volume < nodeInfo.Volume-nodeInfo.RestVol {
		return "", dspErr.New(dspErr.CHAIN_ERROR, "volume %d is less than original volume %d - restvol %d", volume, nodeInfo.Volume, nodeInfo.RestVol)
	}
	if serviceTime == 0 {
		serviceTime = nodeInfo.ServiceTime
	}
	if len(addr) == 0 {
		addr = string(nodeInfo.NodeAddr)
	}
	txHash, err := this.themis.Native.Fs.NodeUpdate(volume, serviceTime, addr)
	if err != nil {
		return "", dspErr.NewWithError(dspErr.CHAIN_ERROR, err)
	}
	tx := hex.EncodeToString(chainCom.ToArrayReverse(txHash))
	return tx, nil
}

// RegisterNode. register node to chain
func (this *Chain) NodeWithdrawProfit() (string, error) {
	txHash, err := this.themis.Native.Fs.NodeWithDrawProfit()
	if err != nil {
		return "", dspErr.NewWithError(dspErr.CHAIN_ERROR, err)
	}
	tx := hex.EncodeToString(chainCom.ToArrayReverse(txHash))
	return tx, nil
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
		return nil, dspErr.NewWithError(dspErr.CHAIN_ERROR, err)
	}
	us, err := this.themis.Native.Fs.GetUserSpace(address)
	if err != nil {
		return nil, dspErr.NewWithError(dspErr.CHAIN_ERROR, err)
	}
	return us, nil
}

func (this *Chain) UpdateUserSpace(walletAddr string, size, sizeOpType, blockCount, countOpType uint64) (string, error) {
	address, err := chainCom.AddressFromBase58(walletAddr)
	if err != nil {
		return "", dspErr.NewWithError(dspErr.CHAIN_ERROR, err)
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
		return "", dspErr.NewWithError(dspErr.CHAIN_ERROR, err)
	}
	tx := hex.EncodeToString(chainCom.ToArrayReverse(txHash))
	return tx, nil
}

func (this *Chain) GetUpdateUserSpaceCost(walletAddr string, size, sizeOpType, blockCount, countOpType uint64) (*usdt.State, error) {
	address, err := chainCom.AddressFromBase58(walletAddr)
	if err != nil {
		return nil, dspErr.NewWithError(dspErr.CHAIN_ERROR, err)
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
		return nil, dspErr.NewWithError(dspErr.CHAIN_ERROR, err)
	}
	return state, nil
}

func (this *Chain) IsFileInfoDeleted(err error) bool {
	if err != nil && strings.Contains(err.Error(), "[FS Profit] FsGetFileInfo not found") {
		return true
	}
	return false
}
