package common

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
}

type UploadResult struct {
	Tx            string
	FileHash      string
	Link          string
	RegisterDnsTx string
	BindDnsTx     string
}

type PayStoreFileReulst struct {
	Tx         string
	ParamsBuf  []byte
	PrivateKey []byte
}

type PayInfo struct {
	WalletAddress string
	Asset         int32
	UnitPrice     uint64
}
