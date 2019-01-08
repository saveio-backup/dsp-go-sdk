package fs

import (
	"fmt"
	"testing"

	"github.com/oniio/dsp-go-sdk/chain/client"
	"github.com/oniio/dsp-go-sdk/chain/wallet"
	fs "github.com/oniio/oniChain/smartcontract/service/native/onifs"
)

var testFs *Fs
var walletPath = "./wallet.dat"
var pwd = []byte("pwd")
var rpc_addr = "http://127.0.0.1:20336"
var txt = "QmevhnWdtmz89BMXuuX5pSY2uZtqKLz7frJsrCojT5kmb6"

func TestMain(m *testing.M) {
	var err error
	w, err := wallet.OpenWallet(walletPath)
	if err != nil {
		fmt.Printf("Account.Open error:%s\n", err)
	}
	acc, err := w.GetDefaultAccount(pwd)
	if err != nil {
		fmt.Printf("GetDefaultAccount error:%s\n", err)
	}
	testFs := &Fs{}
	testFs.Client = &client.ClientMgr{}
	testFs.Client.NewRpcClient().SetAddress(rpc_addr)
	testFs.DefAcc = acc
	m.Run()
}

func TestOntFsClient_GetNodeList(t *testing.T) {
	ret, err := testFs.GetNodeList()
	if err != nil {
		t.Errorf(err.Error())
		return
	}
	nodesInfo := (*fs.FsNodesInfo)(ret)

	for _, nodeInfo := range nodesInfo.NodeInfo {
		fmt.Println("FsNodeQuery Success")
		fmt.Println("Pledge: ", nodeInfo.Pledge)
		fmt.Println("Profit: ", nodeInfo.Profit)
		fmt.Println("Volume: ", nodeInfo.Volume)
		fmt.Println("RestVol: ", nodeInfo.RestVol)
		fmt.Println("NodeAddr: ", string(nodeInfo.NodeAddr))
		fmt.Println("WalletAddr: ", nodeInfo.WalletAddr)
		fmt.Println("ServiceTime:", nodeInfo.ServiceTime)
	}
}

func TestOntFsClient_StoreFile(t *testing.T) {
	proveParam := []byte("ProveProveProveProveProveProveProveProveProveProveProveProve")
	ret, err := testFs.StoreFile(txt, 12, 32,
		32, 32, 32, []byte("1.jpg"), fs.PUBLIC, proveParam)
	if err != nil {
		t.Errorf(err.Error())
		return
	}
	fmt.Println(ret)
}

func TestOntFsClient_GetFileInfo(t *testing.T) {
	fileInfo, err := testFs.GetFileInfo(txt)
	if err != nil {
		t.Errorf(err.Error())
		return
	}
	fmt.Println("FileHash:", fileInfo.FileHash)
	fmt.Println("FileOwner:", fileInfo.FileOwner)
	fmt.Println("CopyNum:", fileInfo.CopyNum)
	fmt.Println("ChallengeRate:", fileInfo.ChallengeRate)
	fmt.Println("ChallengeTimes:", fileInfo.ChallengeTimes)
	fmt.Println("FileBlockNum:", fileInfo.FileBlockNum)
	fmt.Println("FIleBlockSize:", fileInfo.FileBlockSize)
	fmt.Println("Deposit:", fileInfo.Deposit)
	fmt.Println("FileProveParam:", string(fileInfo.FileProveParam))
	fmt.Println("ProveBlockNum:", fileInfo.ProveBlockNum)
}

func TestOntFsClient_GetFileProveDetails(t *testing.T) {
	fileProveDetails, err := testFs.GetFileProveDetails(txt)
	if err != nil {
		t.Errorf(err.Error())
		return
	}
	fmt.Println("CopyNum:", fileProveDetails.CopyNum)
	fmt.Println("ProveDetailNum:", fileProveDetails.ProveDetailNum)
	var i uint64
	for i = 0; i < fileProveDetails.ProveDetailNum; i++ {
		fmt.Println("NodeAddr: ", fileProveDetails.ProveDetails[i].NodeAddr)
		fmt.Println("WalletAddr:", fileProveDetails.ProveDetails[i].WalletAddr)
		fmt.Println("ProveTimes:", fileProveDetails.ProveDetails[i].ProveTimes)
	}
}

func TestOntFs_OntFsInit(t *testing.T) {
	testFs.OniFsInit(2000, 2, 1, 1,
		32, 120, 1024*1024)
}

func TestOntFs_GetSetting(t *testing.T) {
	fsSet, err := testFs.GetSetting()
	if err != nil {
		t.Errorf(err.Error())
		return
	}
	fmt.Println("FsGetSetting Success")
	fmt.Println("FsGasPrice:", fsSet.FsGasPrice,
		"GasPerKBForStore:", fsSet.GasPerKBPerBlock,
		"GasPerKBForRead:", fsSet.GasPerKBForRead,
		"GasForChallenge:", fsSet.GasForChallenge,
		"MaxProveBlockNum:", fsSet.MaxProveBlockNum)
}

func TestOntFs_NodeRegister(t *testing.T) {
	_, err := testFs.NodeRegister(1000, 10000, "https://127.0.0.1:5002")
	if err != nil {
		t.Errorf(err.Error())
		return
	}
	fmt.Println("NodeRegister Success")
}

func TestOntFs_NodeQuery(t *testing.T) {
	fsNodeInfo, err := testFs.NodeQuery(testFs.DefAcc.Address)
	if err != nil {
		t.Errorf(err.Error())
		return
	}
	fmt.Println("NodeQuery Success")
	fmt.Println("Pledge: ", fsNodeInfo.Pledge)
	fmt.Println("Profit: ", fsNodeInfo.Profit)
	fmt.Println("Volume: ", fsNodeInfo.Volume)
	fmt.Println("RestVol: ", fsNodeInfo.RestVol)
	fmt.Println("NodeAddr: ", string(fsNodeInfo.NodeAddr))
	fmt.Println("WalletAddr: ", fsNodeInfo.WalletAddr)
	fmt.Println("ServiceTime:", fsNodeInfo.ServiceTime)
}

func TestOntFs_NodeUpdate(t *testing.T) {
	_, err := testFs.NodeUpdate(100000, 100000, "https://127.0.0.1:5001")
	if err != nil {
		t.Errorf(err.Error())
		return
	}
	fmt.Println("NodeUpdate Success")
}

func TestOntFs_NodeCancel(t *testing.T) {
	_, err := testFs.NodeCancel()
	if err != nil {
		t.Errorf(err.Error())
		return
	}
	fmt.Println("NodeCancel Success")
}

func TestOntFs_NodeWithDrawProfit(t *testing.T) {
	_, err := testFs.NodeWithDrawProfit()
	if err != nil {
		t.Errorf(err.Error())
		return
	}
	fmt.Println("NodeWithDrawProfit Success")
}

func TestOntFs_FileProve(t *testing.T) {
	_, err := testFs.FileProve(txt, nil, "", 1)
	if err != nil {
		t.Errorf(err.Error())
		return
	}
	fmt.Println("TestOntFs_FileProve Success")
}
