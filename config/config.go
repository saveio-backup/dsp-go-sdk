package config

type FSType int

const (
	FS_FILESTORE = iota
	FS_BLOCKSTORE
)

type DspConfig struct {
	DBPath     string // level DB data path
	FsRepoRoot string // fs block store repo root path
	FsFileRoot string // fs file store root path
	FsType     FSType // fs type

	ChainRpcAddr string // chain rpc address
}

func DefaultDspConfig() *DspConfig {
	config := &DspConfig{
		DBPath:       "./db",
		FsRepoRoot:   ".",
		FsFileRoot:   ".",
		FsType:       FS_BLOCKSTORE,
		ChainRpcAddr: "http://localhost:20336",
	}
	return config
}
