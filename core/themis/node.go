package chain

import (
	"encoding/hex"
	sdkErr "github.com/saveio/dsp-go-sdk/error"
	chainCom "github.com/saveio/themis/common"
	fs "github.com/saveio/themis/smartcontract/service/native/savefs"
)

var ErrNoFileInfo = "[FS Profit] FsGetFileInfo not found!"

// RegisterNode. register node to chain
func (t *Themis) RegisterNode(addr string, volume, serviceTime uint64) (string, error) {
	txHash, err := t.themis.Native.Fs.NodeRegister(volume, serviceTime, addr)
	if err != nil {
		return "", sdkErr.NewWithError(sdkErr.CHAIN_ERROR, t.FormatError(err))
	}
	tx := hex.EncodeToString(chainCom.ToArrayReverse(txHash))
	return tx, nil
}

// NodeExit. exit a fs node submit to chain
func (t *Themis) NodeExit() (string, error) {
	txHash, err := t.themis.Native.Fs.NodeCancel()
	if err != nil {
		return "", sdkErr.NewWithError(sdkErr.CHAIN_ERROR, t.FormatError(err))
	}
	tx := hex.EncodeToString(chainCom.ToArrayReverse(txHash))
	return tx, nil
}

// QueryNode. query node information by wallet address
func (t *Themis) QueryNode(walletAddr string) (*fs.FsNodeInfo, error) {
	address, err := chainCom.AddressFromBase58(walletAddr)
	if err != nil {
		return nil, sdkErr.NewWithError(sdkErr.CHAIN_ERROR, t.FormatError(err))
	}
	info, err := t.themis.Native.Fs.NodeQuery(address)
	if err != nil {
		return nil, sdkErr.NewWithError(sdkErr.CHAIN_ERROR, t.FormatError(err))
	}
	return info, nil
}

// UpdateNode. update node information
func (t *Themis) UpdateNode(addr string, volume, serviceTime uint64) (string, error) {
	nodeInfo, err := t.QueryNode(t.themis.Native.Fs.Client.GetDefaultAccount().Address.ToBase58())
	if err != nil {
		return "", sdkErr.NewWithError(sdkErr.CHAIN_ERROR, t.FormatError(err))
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
	txHash, err := t.themis.Native.Fs.NodeUpdate(volume, serviceTime, addr)
	if err != nil {
		return "", sdkErr.NewWithError(sdkErr.CHAIN_ERROR, t.FormatError(err))
	}
	tx := hex.EncodeToString(chainCom.ToArrayReverse(txHash))
	return tx, nil
}

// RegisterNode. register node to chain
func (t *Themis) NodeWithdrawProfit() (string, error) {
	txHash, err := t.themis.Native.Fs.NodeWithDrawProfit()
	if err != nil {
		return "", sdkErr.NewWithError(sdkErr.CHAIN_ERROR, t.FormatError(err))
	}
	tx := hex.EncodeToString(chainCom.ToArrayReverse(txHash))
	return tx, nil
}
