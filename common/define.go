package common

type UploadingInfo struct {
	FileName string
	FileHash string
	Total    uint64
	Uploaded uint64
}

type UploadOption struct {
	FileDesc        string
	ProveInterval   uint64
	ProveTimes      uint32
	Privilege       uint32
	CopyNum         uint32
	Encrypt         bool
	EncryptPassword string
}

type UploadResult struct {
	Tx       string
	FileHash string
	Link     string
}

type PayStoreFileReulst struct {
	Tx         string
	ParamsBuf  []byte
	PrivateKey []byte
}
