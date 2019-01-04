package client

import (
	"bytes"
	"errors"
	"fmt"
	"time"

	chain "github.com/oniio/dsp-go-sdk/chain"
	"github.com/oniio/oniChain/common"
	"github.com/oniio/oniChain/crypto/keypair"
	fs "github.com/oniio/oniChain/smartcontract/service/native/ontfs"
	"github.com/oniio/oniChain/smartcontract/service/native/utils"
)

type OntFsClient struct {
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

const contractVersion = byte(0)

var contractAddr common.Address

func Init(walletPath string, walletPwd string, ontRpcSrvAddr string) *OntFsClient {
	contractAddr = utils.OntFSContractAddress
	ontFs := &OntFsClient{
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

func (c *OntFsClient) GetSetting() (*fs.FsSetting, error) {
	ret, err := c.ChainSdk.Native.PreExecInvokeNativeContract(
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

func (c *OntFsClient) GetNodeList() (*fs.FsNodesInfo, error) {
	ret, err := c.ChainSdk.Native.PreExecInvokeNativeContract(
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

func (c *OntFsClient) ProveParamSer(g []byte, g0 []byte, pubKey []byte, fileId []byte) ([]byte, error) {
	var proveParam fs.ProveParam
	proveParam.G = g
	proveParam.G0 = g0
	proveParam.PubKey = pubKey
	proveParam.FileId = fileId
	bf := new(bytes.Buffer)
	if err := proveParam.Serialize(bf); err != nil {
		return nil, fmt.Errorf("ProveParam Serialize error: %s", err.Error())
	}
	return bf.Bytes(), nil
}

func (c *OntFsClient) ProveParamDes(proveParam []byte) (*fs.ProveParam, error) {
	var proveParamSt fs.ProveParam
	reader := bytes.NewReader(proveParam)
	if err := proveParamSt.Deserialize(reader); err != nil {
		return nil, fmt.Errorf("ProveParamDes Deserialize error: %s", err.Error())
	}
	return &proveParamSt, nil
}

func (c *OntFsClient) StoreFile(fileHashStr string, blockNum uint64,
	blockSize uint64, challengeRate uint64, challengeTimes uint64, copyNum uint64,
	fileDesc []byte, privilege uint64, proveParam []byte) ([]byte, error) {
	if c.DefAcc == nil {
		return nil, errors.New("DefAcc is nil")
	}
	fileHash := []byte(fileHashStr)
	fileInfo := &fs.FileInfo{
		FileHash:       fileHash,
		FileOwner:      c.WalletAddr,
		FileDesc:       fileDesc,
		Privilege:      privilege,
		FileBlockNum:   blockNum,
		FileBlockSize:  blockSize,
		ChallengeRate:  challengeRate,
		ChallengeTimes: challengeTimes,
		CopyNum:        copyNum,
		Deposit:        0,
		FileProveParam: proveParam,
		ProveBlockNum:  0,
	}
	ret, err := c.ChainSdk.Native.InvokeNativeContract(c.GasPrice, c.GasLimit, c.DefAcc,
		contractVersion, contractAddr, fs.FS_STORE_FILE, []interface{}{fileInfo},
	)
	if err != nil {
		return nil, err
	}
	return ret.ToArray(), err
}

func (c *OntFsClient) FileRenew(fileHashStr string, renewTimes uint64) ([]byte, error) {
	if c.DefAcc == nil {
		return nil, errors.New("DefAcc is nil")
	}
	fileHash := []byte(fileHashStr)
	fileRenew := &fs.FileReNew{
		FileHash:   fileHash,
		FromAddr:   c.WalletAddr,
		ReNewTimes: renewTimes,
	}
	ret, err := c.ChainSdk.Native.InvokeNativeContract(c.GasPrice, c.GasLimit, c.DefAcc,
		contractVersion, contractAddr, fs.FS_FILE_RENEW, []interface{}{fileRenew},
	)
	if err != nil {
		return nil, err
	}
	return ret.ToArray(), err
}

func (c *OntFsClient) GetFileInfo(fileHashStr string) (*fs.FileInfo, error) {
	fileHash := []byte(fileHashStr)
	ret, err := c.ChainSdk.Native.PreExecInvokeNativeContract(contractAddr, contractVersion,
		fs.FS_GET_FILE_INFO, []interface{}{fileHash},
	)
	if err != nil {
		return nil, err
	}
	data, err := ret.Result.ToByteArray()
	if err != nil {
		return nil, fmt.Errorf("GetFileInfo result toByteArray: %s", err.Error())
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

func (c *OntFsClient) ChangeFileOwner(fileHashStr string, newOwner common.Address) ([]byte, error) {
	if c.DefAcc == nil {
		return nil, errors.New("DefAcc is nil")
	}
	fileHash := []byte(fileHashStr)
	ownerChange := &fs.OwnerChange{
		FileHash: fileHash,
		CurOwner: c.WalletAddr,
		NewOwner: newOwner,
	}
	ret, err := c.ChainSdk.Native.InvokeNativeContract(c.GasPrice, c.GasLimit, c.DefAcc,
		contractVersion, contractAddr, fs.FS_CHANGE_FILE_OWNER, []interface{}{ownerChange},
	)
	if err != nil {
		return nil, err
	}
	return ret.ToArray(), err
}

func (c *OntFsClient) ChangeFilePrivilege(fileHashStr string, newPrivilege uint64) ([]byte, error) {
	if c.DefAcc == nil {
		return nil, errors.New("DefAcc is nil")
	}
	fileHash := []byte(fileHashStr)
	priChange := &fs.PriChange{
		FileHash:  fileHash,
		Privilege: newPrivilege,
	}
	ret, err := c.ChainSdk.Native.InvokeNativeContract(c.GasPrice, c.GasLimit, c.DefAcc,
		contractVersion, contractAddr, fs.FS_CHANGE_FILE_PRIVILEGE, []interface{}{priChange},
	)
	if err != nil {
		return nil, err
	}
	return ret.ToArray(), err
}

func (c *OntFsClient) GetFileList() (*fs.FileList, error) {
	ret, err := c.ChainSdk.Native.PreExecInvokeNativeContract(contractAddr, contractVersion,
		fs.FS_GET_FILE_LIST, []interface{}{c.WalletAddr},
	)
	if err != nil {
		return nil, err
	}
	data, err := ret.Result.ToByteArray()
	if err != nil {
		return nil, fmt.Errorf("GetFileList result toByteArray: %s", err.Error())
	}

	var fileList fs.FileList
	retInfo := fs.DecRet(data)
	if retInfo.Ret {
		fsFileListReader := bytes.NewReader(retInfo.Info)
		err = fileList.Deserialize(fsFileListReader)
		if err != nil {
			return nil, fmt.Errorf("GetFileList error: %s", err.Error())
		}
		return &fileList, err
	} else {
		return nil, errors.New(string(retInfo.Info))
	}
}

func (c *OntFsClient) GetFileProveDetails(fileHashStr string) (*fs.FsProveDetails, error) {
	fileHash := []byte(fileHashStr)
	ret, err := c.ChainSdk.Native.PreExecInvokeNativeContract(contractAddr, contractVersion,
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

func (c *OntFsClient) WhiteListOp(fileHashStr string, op uint64, whiteList fs.WhiteList) ([]byte, error) {
	if c.DefAcc == nil {
		return nil, errors.New("DefAcc is nil")
	}
	if op != fs.ADD && op != fs.ADD_COV && op != fs.DEL && op != fs.DEL_ALL {
		return nil, errors.New("Param [op] error")
	}
	fileHash := []byte(fileHashStr)
	whiteListOp := fs.WhiteListOp{FileHash: fileHash, Op: op, List: whiteList}
	buf := new(bytes.Buffer)
	if err := whiteListOp.Serialize(buf); err != nil {
		return nil, fmt.Errorf("WhiteListOp serialize error: %s", err.Error())
	}
	ret, err := c.ChainSdk.Native.InvokeNativeContract(c.GasPrice, c.GasLimit, c.DefAcc,
		contractVersion, contractAddr, fs.FS_WHITE_LIST_OP, []interface{}{buf.Bytes()},
	)
	if err != nil {
		return nil, err
	}
	return ret.ToArray(), err
}

func (c *OntFsClient) GetWhiteList(fileHashStr string) (*fs.WhiteList, error) {
	fileHash := []byte(fileHashStr)
	ret, err := c.ChainSdk.Native.PreExecInvokeNativeContract(contractAddr, contractVersion,
		fs.FS_GET_WHITE_LIST, []interface{}{fileHash},
	)
	if err != nil {
		return nil, err
	}
	data, err := ret.Result.ToByteArray()
	if err != nil {
		return nil, fmt.Errorf("GetProveDetails result toByteArray: %s", err.Error())
	}
	var whiteList fs.WhiteList
	retInfo := fs.DecRet(data)
	if retInfo.Ret {
		whiteListReader := bytes.NewReader(retInfo.Info)
		err = whiteList.Deserialize(whiteListReader)
		if err != nil {
			return nil, fmt.Errorf("GetWhiteList deserialize error: %s", err.Error())
		}
		return &whiteList, err
	} else {
		return nil, errors.New(string(retInfo.Info))
	}
}

func (c *OntFsClient) FileReadPledge(fileHashStr string, readPlans fs.ReadPlan) ([]byte, error) {
	if c.DefAcc == nil {
		return nil, errors.New("DefAcc is nil")
	}
	fileHash := []byte(fileHashStr)
	buf := new(bytes.Buffer)
	if err := readPlans.Serialize(buf); err != nil {
		return nil, fmt.Errorf("FsReadFilePledge serialize error: %s", err.Error())
	}
	fileReadPledge := &fs.FileReadPledge{
		FileHash:     fileHash,
		FromAddr:     c.WalletAddr,
		BlockHeight:  0,
		ExpireHeight: 0,
		RestMoney:    0,
		ReadRecord:   buf.Bytes(),
	}
	ret, err := c.ChainSdk.Native.InvokeNativeContract(c.GasPrice, c.GasLimit, c.DefAcc,
		contractVersion, contractAddr, fs.FS_READ_FILE_PLEDGE, []interface{}{fileReadPledge},
	)
	if err != nil {
		return nil, err
	}
	return ret.ToArray(), err
}

func (c *OntFsClient) GetFileReadPledge(fileHashStr string) (*fs.FileReadPledge, *fs.ReadPlan, error) {
	fileHash := []byte(fileHashStr)
	getReadPledge := &fs.GetReadPledge{
		FileHash: fileHash,
		FromAddr: c.WalletAddr,
	}
	ret, err := c.ChainSdk.Native.PreExecInvokeNativeContract(contractAddr, contractVersion,
		fs.FS_GET_FILE_READ_PLEDGE, []interface{}{getReadPledge},
	)
	if err != nil {
		return nil, nil, err
	}
	data, err := ret.Result.ToByteArray()
	if err != nil {
		return nil, nil, fmt.Errorf("FsGetFileReadPledge result toByteArray: %s", err.Error())
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

func (c *OntFsClient) CancelFileRead(fileHashStr string) ([]byte, error) {
	if c.DefAcc == nil {
		return nil, errors.New("DefAcc is nil")
	}
	fileHash := []byte(fileHashStr)
	getReadPledge := &fs.GetReadPledge{
		FileHash: fileHash,
		FromAddr: c.WalletAddr,
	}
	ret, err := c.ChainSdk.Native.InvokeNativeContract(c.GasPrice, c.GasLimit, c.DefAcc,
		contractVersion, contractAddr, fs.FS_CANCLE_FILE_READ, []interface{}{getReadPledge},
	)
	if err != nil {
		return nil, err
	}
	return ret.ToArray(), err
}

func (c *OntFsClient) GenFileReadSettleSlice(fileHash []byte, payTo common.Address, sliceId uint64) (*fs.FileReadSettleSlice, error) {
	settleSlice := fs.FileReadSettleSlice{
		FileHash: fileHash,
		PayFrom:  c.WalletAddr,
		PayTo:    payTo,
		SliceId:  sliceId,
	}
	bf := new(bytes.Buffer)
	err := settleSlice.Serialize(bf)
	if err != nil {
		return nil, fmt.Errorf("FileReadSettleSlice serialize error: %s", err.Error())
	}
	signData, err := c.DefAcc.Sign(bf.Bytes())
	if err != nil {
		return nil, fmt.Errorf("FileReadSettleSlice Sign error: %s", err.Error())
	}
	settleSlice.Sig = signData
	settleSlice.PubKey = keypair.SerializePublicKey(c.DefAcc.PublicKey)
	return &settleSlice, nil
}

func (c *OntFsClient) DeleteFile(fileHashStr string) ([]byte, error) {
	if c.DefAcc == nil {
		return nil, errors.New("DefAcc is nil")
	}
	fileHash := []byte(fileHashStr)
	ret, err := c.ChainSdk.Native.InvokeNativeContract(c.GasPrice, c.GasLimit, c.DefAcc,
		contractVersion, contractAddr, fs.FS_DELETE_FILE, []interface{}{fileHash},
	)
	if err != nil {
		return nil, err
	}
	return ret.ToArray(), err
}

func (c *OntFsClient) FileReadSettleSliceEnc(fileHash []byte, payFrom common.Address,
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

func (c *OntFsClient) FileReadRecordDec(readRecord []byte) (*fs.ReadPlan, error) {
	var tmpReadPlan fs.ReadPlan
	reader := bytes.NewReader(readRecord)
	if err := tmpReadPlan.Deserialize(reader); err != nil {
		return nil, err
	}
	return &tmpReadPlan, nil
}

func (c *OntFsClient) PollForTxConfirmed(timeout time.Duration, txHash []byte) (bool, error) {
	return c.ChainSdk.PollForTxConfirmed(timeout, txHash)
}
