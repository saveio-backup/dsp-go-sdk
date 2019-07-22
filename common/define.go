package common

type FileStoreType int

const (
	FileStoreTypeNormal FileStoreType = iota
	FileStoreTypeProfessional
)

type UploadOption struct {
	FileDesc        string
	ProveInterval   uint64
	ProveTimes      uint32
	Privilege       uint32
	CopyNum         uint32
	Encrypt         bool
	EncryptPassword string
	RegisterDns     bool
	BindDns         bool
	DnsUrl          string
	WhiteList       []string
	Share           bool
	StorageType     FileStoreType
}

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
	Status   string
	ErrorMsg string
}

type DeleteUploadFileResp struct {
	Tx     string
	Status []DeleteFileStatus
}
