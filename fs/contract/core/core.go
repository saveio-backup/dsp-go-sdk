package core

import (
	"bytes"
	"errors"
	"fmt"
	"time"

	chain "github.com/oniio/dsp-go-sdk/chain"
	"github.com/oniio/oniChain/common"
	"github.com/oniio/oniChain/crypto/PDP"
	"github.com/oniio/oniChain/crypto/keypair"
	"github.com/oniio/oniChain/crypto/signature"
	fs "github.com/oniio/oniChain/smartcontract/service/native/ontfs"
	"github.com/oniio/oniChain/smartcontract/service/native/utils"
)

const contractVersion = byte(0)

var contractAddr common.Address

type OntFs struct {
	WalletPath    string
	Password      []byte
	WalletAddr    common.Address
	GasPrice      uint64
	GasLimit      uint64
	ChainSdk      *chain.ChainSdk
	Wallet        *chain.Wallet
	DefAcc        *chain.Account
	OntRpcSrvAddr string
}

func Init(walletPath string, walletPwd string, ontRpcSrvAddr string) *OntFs {
	contractAddr = utils.OntFSContractAddress
	ontFs := &OntFs{
		WalletPath:    walletPath,
		Password:      []byte(walletPwd),
		GasPrice:      uint64(0),
		GasLimit:      uint64(20000),
		OntRpcSrvAddr: ontRpcSrvAddr,
	}

	ontFs.ChainSdk = chain.NewChainSdk()
	ontFs.ChainSdk.NewRpcClient().SetAddress(ontFs.OntRpcSrvAddr)

	if len(walletPath) != 0 {
		var err error
		ontFs.Wallet, err = ontFs.ChainSdk.OpenWallet(ontFs.WalletPath)
		if err != nil {
			fmt.Printf("Account.Open error:%s\n", err)
			return nil
		}
		ontFs.DefAcc, err = ontFs.Wallet.GetDefaultAccount(ontFs.Password)
		if err != nil {
			fmt.Printf("GetDefaultAccount error:%s\n", err)
			return nil
		}
		ontFs.WalletAddr = ontFs.DefAcc.Address
	} else {
		ontFs.Wallet = nil
		ontFs.DefAcc = nil
	}
	return ontFs
}

func (d *OntFs) OntFsInit(fsGasPrice, gasPerKBPerBlock, gasPerKBForRead, gasForChallenge,
	maxProveBlockNum, minChallengeRate uint64, minVolume uint64) ([]byte, error) {
	if d.DefAcc == nil {
		return nil, errors.New("DefAcc is nil")
	}
	ret, err := d.ChainSdk.Native.InvokeNativeContract(d.GasPrice, d.GasLimit, d.DefAcc,
		contractVersion, contractAddr, fs.FS_INIT,
		[]interface{}{&fs.FsSetting{FsGasPrice: fsGasPrice,
			GasPerKBPerBlock: gasPerKBPerBlock,
			GasPerKBForRead:  gasPerKBForRead,
			GasForChallenge:  gasForChallenge,
			MaxProveBlockNum: maxProveBlockNum,
			MinChallengeRate: minChallengeRate,
			MinVolume:        minVolume}},
	)
	if err != nil {
		return nil, err
	}
	return ret.ToArray(), err
}

func (d *OntFs) GetSetting() (*fs.FsSetting, error) {
	ret, err := d.ChainSdk.Native.PreExecInvokeNativeContract(
		contractAddr, contractVersion, fs.FS_GETSETTING, []interface{}{},
	)
	if err != nil {
		return nil, err
	}
	data, err := ret.Result.ToByteArray()
	if err != nil {
		return nil, fmt.Errorf("GetSetting result toByteArray: %s", err.Error())
	}

	var fsSet fs.FsSetting
	retInfo := fs.DecRet(data)
	if retInfo.Ret {
		fsSetReader := bytes.NewReader(retInfo.Info)
		err = fsSet.Deserialize(fsSetReader)
		if err != nil {
			return nil, fmt.Errorf("FsGetSetting error: %s", err.Error())
		}
		return &fsSet, nil
	} else {
		return nil, errors.New(string(retInfo.Info))
	}
}

func (d *OntFs) GetNodeList() (*fs.FsNodesInfo, error) {
	ret, err := d.ChainSdk.Native.PreExecInvokeNativeContract(
		contractAddr, contractVersion, fs.FS_GET_NODE_LIST, []interface{}{},
	)
	if err != nil {
		return nil, err
	}
	data, err := ret.Result.ToByteArray()
	if err != nil {
		return nil, fmt.Errorf("GetNodeList result toByteArray: %s", err.Error())
	}
	var nodesInfo fs.FsNodesInfo
	retInfo := fs.DecRet(data)
	if retInfo.Ret {
		reader := bytes.NewReader(retInfo.Info)
		if err = nodesInfo.Deserialize(reader); err != nil {
			return nil, fmt.Errorf("GetNodeList json Unmarshal: %s", err.Error())
		}
		return &nodesInfo, nil
	} else {
		return nil, errors.New(string(retInfo.Info))
	}
}

func (d *OntFs) NodeRegister(volume uint64, serviceTime uint64, nodeAddr string) ([]byte, error) {
	if d.DefAcc == nil {
		return nil, errors.New("DefAcc is nil")
	}
	ret, err := d.ChainSdk.Native.InvokeNativeContract(d.GasPrice, d.GasLimit, d.DefAcc,
		contractVersion, contractAddr, fs.FS_NODE_REGISTER,
		[]interface{}{&fs.FsNodeInfo{Pledge: 0, Profit: 0, Volume: volume, RestVol: 0,
			ServiceTime: serviceTime, WalletAddr: d.WalletAddr, NodeAddr: []byte(nodeAddr)}},
	)
	if err != nil {
		return nil, err
	}
	return ret.ToArray(), err
}

func (d *OntFs) NodeQuery(nodeWallet common.Address) (*fs.FsNodeInfo, error) {
	ret, err := d.ChainSdk.Native.PreExecInvokeNativeContract(
		contractAddr, contractVersion, fs.FS_NODE_QUERY, []interface{}{nodeWallet},
	)
	if err != nil {
		return nil, err
	}
	data, err := ret.Result.ToByteArray()
	if err != nil {
		return nil, fmt.Errorf("GetNodeList result toByteArray: %s", err.Error())
	}

	var fsNodeInfo fs.FsNodeInfo
	retInfo := fs.DecRet(data)
	if retInfo.Ret {
		fsNodeInfoReader := bytes.NewReader(retInfo.Info)
		err = fsNodeInfo.Deserialize(fsNodeInfoReader)
		if err != nil {
			return nil, fmt.Errorf("FsNodeQuery error: %s", err.Error())
		}
		return &fsNodeInfo, nil
	} else {
		return nil, errors.New(string(retInfo.Info))
	}
}

func (d *OntFs) NodeUpdate(volume uint64, serviceTime uint64, nodeAddr string) ([]byte, error) {
	if d.DefAcc == nil {
		return nil, errors.New("DefAcc is nil")
	}
	ret, err := d.ChainSdk.Native.InvokeNativeContract(d.GasPrice, d.GasLimit, d.DefAcc,
		contractVersion, contractAddr, fs.FS_NODE_UPDATE,
		[]interface{}{&fs.FsNodeInfo{Pledge: 0, Profit: 0, Volume: volume, RestVol: 0,
			ServiceTime: serviceTime, WalletAddr: d.WalletAddr, NodeAddr: []byte(nodeAddr)}},
	)
	if err != nil {
		return nil, err
	}
	return ret.ToArray(), err
}

func (d *OntFs) NodeCancel() ([]byte, error) {
	if d.DefAcc == nil {
		return nil, errors.New("DefAcc is nil")
	}
	ret, err := d.ChainSdk.Native.InvokeNativeContract(d.GasPrice, d.GasLimit, d.DefAcc,
		contractVersion, contractAddr, fs.FS_NODE_CANCEL,
		[]interface{}{d.WalletAddr},
	)
	if err != nil {
		return nil, err
	}
	return ret.ToArray(), err
}

func (d *OntFs) NodeWithDrawProfit() ([]byte, error) {
	if d.DefAcc == nil {
		return nil, errors.New("DefAcc is nil")
	}
	ret, err := d.ChainSdk.Native.InvokeNativeContract(d.GasPrice, d.GasLimit, d.DefAcc,
		contractVersion, contractAddr, fs.FS_NODE_WITH_DRAW_PROFIT,
		[]interface{}{d.WalletAddr},
	)
	if err != nil {
		return nil, err
	}
	return ret.ToArray(), err
}

func (d *OntFs) GetFileInfo(fileHashStr string) (*fs.FileInfo, error) {
	fileHash := []byte(fileHashStr)
	ret, err := d.ChainSdk.Native.PreExecInvokeNativeContract(contractAddr, contractVersion,
		fs.FS_GET_FILE_INFO, []interface{}{fileHash},
	)
	if err != nil {
		return nil, err
	}
	data, err := ret.Result.ToByteArray()
	if err != nil {
		return nil, fmt.Errorf("GetNodeList result toByteArray: %s", err.Error())
	}

	var fileInfo fs.FileInfo
	retInfo := fs.DecRet(data)
	if retInfo.Ret {
		fsFileInfoReader := bytes.NewReader(retInfo.Info)
		err = fileInfo.Deserialize(fsFileInfoReader)
		if err != nil {
			return nil, fmt.Errorf("GetFileInfo error: %s", err.Error())
		}
		return &fileInfo, err
	} else {
		return nil, errors.New(string(retInfo.Info))
	}
}

func (d *OntFs) FileProve(fileHashStr string, multiRes []byte, addResStr string, blockHeight uint64) ([]byte, error) {
	if d.DefAcc == nil {
		return nil, errors.New("DefAcc is nil")
	}
	fileHash := []byte(fileHashStr)
	addRes := []byte(addResStr)
	ret, err := d.ChainSdk.Native.InvokeNativeContract(d.GasPrice, d.GasLimit,
		d.DefAcc, contractVersion, contractAddr, fs.FS_FILE_PROVE,
		[]interface{}{&fs.FileProve{FileHash: fileHash,
			MultiRes:    multiRes,
			AddRes:      addRes,
			BlockHeight: blockHeight,
			NodeWallet:  d.WalletAddr,
			Profit:      0,
			LuckyNum:    0,
			BakHeight:   0,
			BakNum:      0}},
	)
	if err != nil {
		return nil, err
	}
	return ret.ToArray(), err
}

func (d *OntFs) FileBackProve(fileHashStr string, multiRes []byte, addResStr string, blockHeight,
	luckyNum, bakHeight, bakNum uint64, brokenWallet common.Address) ([]byte, error) {
	if d.DefAcc == nil {
		return nil, errors.New("DefAcc is nil")
	}
	fileHash := []byte(fileHashStr)
	addRes := []byte(addResStr)
	ret, err := d.ChainSdk.Native.InvokeNativeContract(d.GasPrice, d.GasLimit,
		d.DefAcc, contractVersion, contractAddr, fs.FS_FILE_PROVE,
		[]interface{}{&fs.FileProve{FileHash: fileHash,
			MultiRes:     multiRes,
			AddRes:       addRes,
			BlockHeight:  blockHeight,
			NodeWallet:   d.WalletAddr,
			Profit:       0,
			LuckyNum:     luckyNum,
			BakHeight:    bakHeight,
			BakNum:       bakNum,
			BrokenWallet: brokenWallet}},
	)
	if err != nil {
		return nil, err
	}
	return ret.ToArray(), err
}

func (d *OntFs) GetFileReadPledge(fileHashStr string, readFromAddr common.Address) (*fs.FileReadPledge, *fs.ReadPlan, error) {
	fileHash := []byte(fileHashStr)
	getReadPledge := &fs.GetReadPledge{
		FileHash: fileHash,
		FromAddr: readFromAddr,
	}
	ret, err := d.ChainSdk.Native.PreExecInvokeNativeContract(contractAddr, contractVersion,
		fs.FS_GET_FILE_READ_PLEDGE, []interface{}{getReadPledge},
	)
	if err != nil {
		return nil, nil, err
	}

	data, err := ret.Result.ToByteArray()
	if err != nil {
		return nil, nil, fmt.Errorf("GetNodeList result toByteArray: %s", err.Error())
	}
	var frp fs.FileReadPledge
	retInfo := fs.DecRet(data)
	if retInfo.Ret {
		reader := bytes.NewReader(retInfo.Info)
		if err = frp.Deserialize(reader); err != nil {
			return nil, nil, err
		}

		var tmpReadPlan fs.ReadPlan
		reader = bytes.NewReader(frp.ReadRecord)
		if err := tmpReadPlan.Deserialize(reader); err != nil {
			return &frp, nil, err
		}
		return &frp, &tmpReadPlan, nil
	} else {
		return nil, nil, errors.New(string(retInfo.Info))
	}
}

func (d *OntFs) GetExpiredProveList() (*fs.BakTasks, error) {
	ret, err := d.ChainSdk.Native.PreExecInvokeNativeContract(contractAddr, contractVersion,
		fs.FS_GET_EXPIRED_PROVE_LIST, []interface{}{},
	)
	if err != nil {
		return nil, err
	}
	data, err := ret.Result.ToByteArray()
	if err != nil {
		return nil, fmt.Errorf("GetNodeList result toByteArray: %s", err.Error())
	}

	var bakTasks fs.BakTasks
	retInfo := fs.DecRet(data)
	if retInfo.Ret {
		reader := bytes.NewReader(retInfo.Info)
		if err = bakTasks.Deserialize(reader); err != nil {
			return nil, fmt.Errorf("FsGetExpiredProveList BakTasks Deserialize: %s", err.Error())
		}
		return &bakTasks, err
	} else {
		return nil, errors.New(string(retInfo.Info))
	}
}

func (d *OntFs) GetFileProveDetails(fileHashStr string) (*fs.FsProveDetails, error) {
	fileHash := []byte(fileHashStr)
	ret, err := d.ChainSdk.Native.PreExecInvokeNativeContract(contractAddr, contractVersion,
		fs.FS_GET_FILE_PROVE_DETAILS, []interface{}{fileHash},
	)
	if err != nil {
		return nil, err
	}
	data, err := ret.Result.ToByteArray()
	if err != nil {
		return nil, fmt.Errorf("GetProveDetails result toByteArray: %s", err.Error())
	}
	var fileProveDetails fs.FsProveDetails
	retInfo := fs.DecRet(data)
	if retInfo.Ret {
		fileProveDetailReader := bytes.NewReader(retInfo.Info)
		err = fileProveDetails.Deserialize(fileProveDetailReader)
		if err != nil {
			return nil, fmt.Errorf("GetProveDetails deserialize error: %s", err.Error())
		}
		return &fileProveDetails, err
	} else {
		return nil, errors.New(string(retInfo.Info))
	}
}

func (d *OntFs) FileReadPledge(fileHashStr string, readPlans fs.ReadPlan) ([]byte, error) {
	if d.DefAcc == nil {
		return nil, errors.New("DefAcc is nil")
	}
	fileHash := []byte(fileHashStr)
	buf := new(bytes.Buffer)
	if err := readPlans.Serialize(buf); err != nil {
		return nil, fmt.Errorf("FsReadFilePledge serialize error: %s", err.Error())
	}
	fileReadPledge := &fs.FileReadPledge{
		FileHash:     fileHash,
		FromAddr:     d.WalletAddr,
		BlockHeight:  0,
		ExpireHeight: 0,
		RestMoney:    0,
		ReadRecord:   buf.Bytes(),
	}
	ret, err := d.ChainSdk.Native.InvokeNativeContract(d.GasPrice, d.GasLimit, d.DefAcc,
		contractVersion, contractAddr, fs.FS_READ_FILE_PLEDGE, []interface{}{fileReadPledge},
	)
	if err != nil {
		return nil, err
	}
	return ret.ToArray(), err
}

func (d *OntFs) CancelFileRead(fileHashStr string) ([]byte, error) {
	if d.DefAcc == nil {
		return nil, errors.New("DefAcc is nil")
	}
	fileHash := []byte(fileHashStr)
	getReadPledge := &fs.GetReadPledge{
		FileHash: fileHash,
		FromAddr: d.WalletAddr,
	}
	ret, err := d.ChainSdk.Native.InvokeNativeContract(d.GasPrice, d.GasLimit, d.DefAcc,
		contractVersion, contractAddr, fs.FS_CANCLE_FILE_READ, []interface{}{getReadPledge},
	)
	if err != nil {
		return nil, err
	}
	return ret.ToArray(), err
}

func (d *OntFs) GenFileReadSettleSlice(fileHash []byte, payTo common.Address, sliceId uint64) (*fs.FileReadSettleSlice, error) {
	settleSlice := fs.FileReadSettleSlice{
		FileHash: fileHash,
		PayFrom:  d.WalletAddr,
		PayTo:    payTo,
		SliceId:  sliceId,
	}
	bf := new(bytes.Buffer)
	err := settleSlice.Serialize(bf)
	if err != nil {
		return nil, fmt.Errorf("FileReadSettleSlice serialize error: %s", err.Error())
	}
	signData, err := d.DefAcc.Sign(bf.Bytes())
	if err != nil {
		return nil, fmt.Errorf("FileReadSettleSlice Sign error: %s", err.Error())
	}
	settleSlice.Sig = signData
	settleSlice.PubKey = keypair.SerializePublicKey(d.DefAcc.PublicKey)
	return &settleSlice, nil
}

func (d *OntFs) FileReadProfitSettle(fileReadSettleSlice fs.FileReadSettleSlice) ([]byte, error) {
	if d.DefAcc == nil {
		return nil, errors.New("DefAcc is nil")
	}
	ret, err := d.ChainSdk.Native.InvokeNativeContract(d.GasPrice, d.GasLimit,
		d.DefAcc, contractVersion, contractAddr, fs.FS_FILE_READ_PROFIT_SETTLE,
		[]interface{}{&fileReadSettleSlice},
	)
	if err != nil {
		return nil, err
	}
	return ret.ToArray(), err
}

func (d *OntFs) VerifyFileReadSettleSlice(settleSlice fs.FileReadSettleSlice) (bool, error) {
	tmpSettleSlice := fs.FileReadSettleSlice{
		FileHash: settleSlice.FileHash,
		PayFrom:  settleSlice.PayFrom,
		PayTo:    settleSlice.PayTo,
		SliceId:  settleSlice.SliceId,
	}
	bf := new(bytes.Buffer)
	err := tmpSettleSlice.Serialize(bf)
	if err != nil {
		return false, fmt.Errorf("FileReadSettleSlice serialize error: %s", err.Error())
	}

	signValue, err := signature.Deserialize(settleSlice.Sig)
	if err != nil {
		return false, fmt.Errorf("FileReadSettleSlice signature deserialize error: %s", err.Error())
	}
	pubKey, err := keypair.DeserializePublicKey(settleSlice.PubKey)
	if err != nil {
		return false, fmt.Errorf("FileReadSettleSlice deserialize PublicKey( error: %s", err.Error())
	}
	result := signature.Verify(pubKey, bf.Bytes(), signValue)
	return result, nil
}

func (d *OntFs) ProveParamDes(proveParam []byte) (*fs.ProveParam, error) {
	var proveParamSt fs.ProveParam
	reader := bytes.NewReader(proveParam)
	if err := proveParamSt.Deserialize(reader); err != nil {
		return nil, fmt.Errorf("ProveParamDes Deserialize error: %s", err.Error())
	}
	return &proveParamSt, nil
}

func (d *OntFs) FileReadSettleSliceEnc(fileHash []byte, payFrom common.Address,
	payTo common.Address, sliceId uint64, sig []byte, pubKey []byte) ([]byte, error) {
	fileReadSettleSlice := fs.FileReadSettleSlice{
		FileHash: fileHash,
		PayFrom:  payFrom,
		PayTo:    payTo,
		SliceId:  sliceId,
		Sig:      sig,
		PubKey:   pubKey,
	}
	bf := new(bytes.Buffer)
	if err := fileReadSettleSlice.Serialize(bf); err != nil {
		return nil, fmt.Errorf("FileReadSettleSlice Serialize error: %s", err.Error())
	}
	return bf.Bytes(), nil
}

func (d *OntFs) FileReadSettleSliceDec(fileReadSettleSliceSer []byte) (*fs.FileReadSettleSlice, error) {
	reader := bytes.NewReader(fileReadSettleSliceSer)
	var fileReadSettleSlice fs.FileReadSettleSlice
	if err := fileReadSettleSlice.Deserialize(reader); err != nil {
		return nil, fmt.Errorf("FileReadSettleSlice Deserialize error: %s", err.Error())
	}
	return &fileReadSettleSlice, nil
}

func (d *OntFs) GenChallenge(walletAddr common.Address, hash common.Uint256, fileBlockNum, proveNum uint64) []PDP.Challenge {
	return fs.GenChallenge(walletAddr, hash, uint32(fileBlockNum), uint32(proveNum))
}

func (d *OntFs) PollForTxConfirmed(timeout time.Duration, txHash []byte) (bool, error) {
	return d.ChainSdk.PollForTxConfirmed(timeout, txHash)
}
