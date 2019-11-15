package common

import (
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
