package client

import (
	"fmt"
	"testing"

	fs "github.com/oniio/oniChain/smartcontract/service/native/ontfs"
)

var ontFs *OntFsClient

var txt = "QmevhnWdtmz89BMXuuX5pSY2uZtqKLz7frJsrCojT5kmb6"

func TestMain(m *testing.M) {
	ontFs = Init("./wallet.dat", "pwd", "http://localhost:20336")
	if ontFs == nil {
		fmt.Println("Init error")
	}
	m.Run()
}

func TestOntFsClient_GetNodeList(t *testing.T) {
	ret, err := ontFs.GetNodeList()
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
	ret, err := ontFs.StoreFile(txt, 12, 32,
		32, 32, 32, []byte("1.jpg"), fs.PUBLIC, proveParam)
	if err != nil {
		t.Errorf(err.Error())
		return
	}
	fmt.Println(ret)
}

func TestOntFsClient_GetFileInfo(t *testing.T) {
	fileInfo, err := ontFs.GetFileInfo(txt)
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
	fileProveDetails, err := ontFs.GetFileProveDetails(txt)
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
