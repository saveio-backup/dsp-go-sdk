package common

import (
	"fmt"

	chainCom "github.com/saveio/themis/common"
)

type UploadResult struct {
	Tx             string
	FileHash       string
	Url            string
	Link           string
	RegisterDnsTx  string
	BindDnsTx      string
	AddWhiteListTx string
}

type PayStoreFileResult struct {
	Tx                   string           // pay for store file tx on chain
	ParamsBuf            []byte           // PDP params buffer
	PrivateKey           []byte           // PDP verify private key
	MasterNodeWalletAddr chainCom.Address // master node wallet addr
	MasterNodeHostAddr   string           // Master node
}

type PayInfo struct {
	WalletAddress string
	Asset         int32
	UnitPrice     uint64
}

type DeleteFileStatus struct {
	HostAddr string
	Code     uint32
	Error    string
}

type DeleteUploadFileResp struct {
	Tx       string
	FileHash string
	FileName string
	Nodes    []DeleteFileStatus
}

type DownloadOption struct {
	FileName    string
	FileOwner   string
	Asset       int32
	InOrder     bool
	DecryptPwd  string
	Free        bool
	SetFileName bool
	MaxPeerCnt  int
	Url         string
}

type WorkerNetMsgPhase int

const (
	WorkNetPhaseNone WorkerNetMsgPhase = iota
	WorkerSendFileAsk
	WorkerRecvFileAsk
	WorkerSendFileAck
	WorkerRecvFileAck
	WorkerSendFileRdy
	WorkerRecvFileRdy
	WorkerSendFilePause
	WorkerRecvFilePause
	WorkerSendFileResume
	WorkerRecvFileResume
	WorkerSendFileCancel
	WorkerRecvFileCancel
	WorkerSendFetchDone
	WorkerRecvFileDone
	WorkerSendDownloadAsk
	WorkerRecvDownloadAsk
	WorkerSendDownloadAck
	WorkerRecvDownloadAck
	WorkerSendDownloadDone
	WorkerRecvDownloadDone
	WorkerSendDownloadCancel
	WorkerRecvDownloadCancel
	WorkerSendFileDelete
	WorkerRecvDownloadDelete
	WorkerSendFileDeleteDone
	WorkerRecvFileDeleteDone
)

type Gas struct {
	GasPrice uint64
	GasLimit uint64
}
type NodeInfo struct {
	HostAddr   string
	WalletAddr string
}

func (n NodeInfo) String() string {
	return fmt.Sprintf("wallet address: %s, host address: %s", n.WalletAddr, n.HostAddr)
}
