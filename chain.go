package dsp

import (
	"encoding/hex"
	"fmt"

	"github.com/saveio/dsp-go-sdk/config"
	"github.com/saveio/themis/account"
	chainCom "github.com/saveio/themis/common"
	fs "github.com/saveio/themis/smartcontract/service/native/onifs"
)

func (this *Dsp) CurrentAccount() *account.Account {
	return this.Chain.Native.Fs.DefAcc
}

// WalletAddress. get base58 address
func (this *Dsp) WalletAddress() string {
	return this.CurrentAccount().Address.ToBase58()
}

// RegisterNode. register node to chain
func (this *Dsp) RegisterNode(addr string, volume, serviceTime uint64) (string, error) {
	txHash, err := this.Chain.Native.Fs.NodeRegister(volume, serviceTime, addr)
	if err != nil {
		return "", err
	}
	tx := hex.EncodeToString(chainCom.ToArrayReverse(txHash))
	return tx, nil
}

// UnregisterNode. unregister node to chain
func (this *Dsp) UnregisterNode() (string, error) {
	txHash, err := this.Chain.Native.Fs.NodeCancel()
	if err != nil {
		return "", err
	}
	tx := hex.EncodeToString(chainCom.ToArrayReverse(txHash))
	return tx, nil
}

// QueryNode. query node information by wallet address
func (this *Dsp) QueryNode(walletAddr string) (*fs.FsNodeInfo, error) {
	address, err := chainCom.AddressFromBase58(walletAddr)
	if err != nil {
		return nil, err
	}
	return this.Chain.Native.Fs.NodeQuery(address)
}

// UpdateNode. update node information
func (this *Dsp) UpdateNode(addr string, volume, serviceTime uint64) (string, error) {
	nodeInfo, err := this.QueryNode(this.Chain.Native.Fs.DefAcc.Address.ToBase58())
	if err != nil {
		return "", err
	}
	if volume == 0 {
		volume = nodeInfo.Volume
	}
	if volume < nodeInfo.Volume-nodeInfo.RestVol {
		return "", fmt.Errorf("volume %d is less than original volume %d - restvol %d", volume, nodeInfo.Volume, nodeInfo.RestVol)
	}
	if serviceTime == 0 {
		serviceTime = nodeInfo.ServiceTime
	}
	if len(addr) == 0 {
		addr = string(nodeInfo.NodeAddr)
	}
	txHash, err := this.Chain.Native.Fs.NodeUpdate(volume, serviceTime, addr)
	if err != nil {
		return "", err
	}
	tx := hex.EncodeToString(chainCom.ToArrayReverse(txHash))
	return tx, nil
}

// RegisterNode. register node to chain
func (this *Dsp) NodeWithdrawProfit() (string, error) {
	txHash, err := this.Chain.Native.Fs.NodeWithDrawProfit()
	if err != nil {
		return "", err
	}
	tx := hex.EncodeToString(chainCom.ToArrayReverse(txHash))
	return tx, nil
}

// CheckFilePrivilege. check if the downloader has privilege to download file
func (this *Dsp) CheckFilePrivilege(fileHashStr, walletAddr string) bool {
	if this.Config.FsType == config.FS_FILESTORE {
		return true
	}
	info, err := this.Chain.Native.Fs.GetFileInfo(fileHashStr)
	if err != nil || info == nil {
		return false
	}
	if info.Privilege == fs.PUBLIC {
		return true
	}
	if info.Privilege == fs.PRIVATE {
		return false
	}
	whitelist, err := this.Chain.Native.Fs.GetWhiteList(fileHashStr)
	if err != nil || whitelist == nil {
		return true
	}
	currentHeight, err := this.Chain.GetCurrentBlockHeight()
	if err != nil {
		return false
	}
	for _, r := range whitelist.List {
		if r.Addr.ToBase58() != walletAddr {
			continue
		}
		if r.BaseHeight >= uint64(currentHeight) && uint64(currentHeight) <= r.ExpireHeight {
			return true
		}
	}
	return false
}

// GetUserSpace. get user space of client
func (this *Dsp) GetUserSpace(walletAddr string) (*fs.UserSpace, error) {
	address, err := chainCom.AddressFromBase58(walletAddr)
	if err != nil {
		return nil, err
	}
	return this.Chain.Native.Fs.GetUserSpace(address)
}

func (this *Dsp) UpdateUserSpace(walletAddr string, size, sizeOpType, blockCount, countOpType uint64) (string, error) {
	address, err := chainCom.AddressFromBase58(walletAddr)
	if err != nil {
		return "", err
	}
	if size == 0 {
		sizeOpType = uint64(fs.UserSpaceNone)
	}
	if blockCount == 0 {
		countOpType = uint64(fs.UserSpaceNone)
	}
	txHash, err := this.Chain.Native.Fs.UpdateUserSpace(address, &fs.UserSpaceOperation{
		Type:  sizeOpType,
		Value: size,
	}, &fs.UserSpaceOperation{
		Type:  countOpType,
		Value: blockCount,
	})
	if err != nil {
		return "", err
	}
	tx := hex.EncodeToString(chainCom.ToArrayReverse(txHash))
	return tx, nil
}
