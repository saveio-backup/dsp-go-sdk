package config

type FSType int

const (
	FS_FILESTORE = iota
	FS_BLOCKSTORE
)

type DspConfig struct {
	DBPath             string // level DB data path
	ChainRpcAddr       string // chain rpc address
	CheckDepositBlkNum uint64 // check deposit price of block num. if it is 0, no checking

	FsRepoRoot string // fs block store repo root path
	FsFileRoot string // fs file store root path
	FsType     FSType // fs type
	FsGcPeriod string // fs gc period

	ChannelClientType    string // channel client type. e.g: "rpc"
	ChannelListenAddr    string // channel listen address. e.g: "127.0.0.1:3001"
	ChannelProtocol      string // channel network protocol. e.g: "tcp"
	ChannelRevealTimeout string // channel reveal time out. e.g: "1000"
}

func DefaultDspConfig() *DspConfig {
	config := &DspConfig{
		// 		DBPath:       "./db",
		// 		FsRepoRoot:   ".",
		// 		FsFileRoot:   ".",
		// 		FsType:       FS_BLOCKSTORE,
		// FsGcPeriod:   "1h",
		// ChainRpcAddr: "http://localhost:20336",
	}
	return config
}
