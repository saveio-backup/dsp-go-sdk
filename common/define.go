package common

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
	Tx         string
	ParamsBuf  []byte
	PrivateKey []byte
}

type PayInfo struct {
	WalletAddress string
	Asset         int32
	UnitPrice     uint64
}

type DeleteFileStatus struct {
	HostAddr string
	Code     uint64
	Error    string
}

type DeleteUploadFileResp struct {
	Tx       string
	FileName string
	Nodes    []DeleteFileStatus
}

type DownloadOption struct {
	FileName    string
	Asset       int32
	InOrder     bool
	DecryptPwd  string
	Free        bool
	SetFileName bool
	MaxPeerCnt  int
}
