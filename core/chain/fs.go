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
	fileDesc []byte, privilege uint64, proveParam []byte, storageType, realFileSize uint64) (string, error) {
	txHash, err := this.themis.Native.Fs.StoreFile(fileHashStr, blockNum, blockSizeInKB, proveInterval,
		expiredHeight, copyNum, fileDesc, privilege, proveParam, storageType, realFileSize)
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
