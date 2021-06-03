package chain

import (
	"encoding/hex"

	sdkErr "github.com/saveio/dsp-go-sdk/error"
	chainCom "github.com/saveio/themis/common"
	fs "github.com/saveio/themis/smartcontract/service/native/savefs"
)

var ErrNoFileInfo = "[FS Profit] FsGetFileInfo not found!"

// RegisterNode. register node to chain
func (this *Chain) RegisterNode(addr string, volume, serviceTime uint64) (string, error) {
	txHash, err := this.themis.Native.Fs.NodeRegister(volume, serviceTime, addr)
	if err != nil {
		return "", sdkErr.NewWithError(sdkErr.CHAIN_ERROR, err)
	}
	tx := hex.EncodeToString(chainCom.ToArrayReverse(txHash))
	return tx, nil
}

// NodeExit. exit a fs node submit to chain
func (this *Chain) NodeExit() (string, error) {
	txHash, err := this.themis.Native.Fs.NodeCancel()
	if err != nil {
		return "", sdkErr.NewWithError(sdkErr.CHAIN_ERROR, err)
	}
	tx := hex.EncodeToString(chainCom.ToArrayReverse(txHash))
	return tx, nil
}

// QueryNode. query node information by wallet address
func (this *Chain) QueryNode(walletAddr string) (*fs.FsNodeInfo, error) {
	address, err := chainCom.AddressFromBase58(walletAddr)
	if err != nil {
		return nil, sdkErr.NewWithError(sdkErr.CHAIN_ERROR, err)
	}
	info, err := this.themis.Native.Fs.NodeQuery(address)
	if err != nil {
		return nil, sdkErr.NewWithError(sdkErr.CHAIN_ERROR, err)
	}
	return info, nil
}

// UpdateNode. update node information
func (this *Chain) UpdateNode(addr string, volume, serviceTime uint64) (string, error) {
	nodeInfo, err := this.QueryNode(this.themis.Native.Fs.DefAcc.Address.ToBase58())
	if err != nil {
		return "", sdkErr.NewWithError(sdkErr.CHAIN_ERROR, err)
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
	txHash, err := this.themis.Native.Fs.NodeUpdate(volume, serviceTime, addr)
	if err != nil {
		return "", sdkErr.NewWithError(sdkErr.CHAIN_ERROR, err)
	}
	tx := hex.EncodeToString(chainCom.ToArrayReverse(txHash))
	return tx, nil
}

// RegisterNode. register node to chain
func (this *Chain) NodeWithdrawProfit() (string, error) {
	txHash, err := this.themis.Native.Fs.NodeWithDrawProfit()
	if err != nil {
		return "", sdkErr.NewWithError(sdkErr.CHAIN_ERROR, err)
	}
	tx := hex.EncodeToString(chainCom.ToArrayReverse(txHash))
	return tx, nil
}
