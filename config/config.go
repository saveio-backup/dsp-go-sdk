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

	FsRepoRoot   string // fs block store repo root path
	FsFileRoot   string // fs file store root path
	FsType       FSType // fs type
	FsGcPeriod   string // fs gc period
	FsMaxStorage string // fs max storage

	ChannelClientType    string // channel client type. e.g: "rpc"
	ChannelListenAddr    string // channel listen address. e.g: "127.0.0.1:3001"
	ChannelProtocol      string // channel network protocol. e.g: "tcp"
	ChannelRevealTimeout string // channel reveal timeout. e.g: "50"
	ChannelSettleTimeout string // channel settle timeout. e.g: "120"
	ChannelDBPath        string

	AutoSetupDNSEnable bool   // enable auto setup DNS node or not
	DnsNodeMaxNum      int    // dns node max count
	DnsChannelDeposit  uint64 // deposit amount of channel between self and dns node
	SeedInterval       int    // push file to tracker interval in second, if it's 0, no push
}

func DefaultDspConfig() *DspConfig {
	config := &DspConfig{
		DBPath:               "./DB/dsp",
		ChainRpcAddr:         "http://127.0.0.1:20336",
		CheckDepositBlkNum:   0,
		FsRepoRoot:           "./FS",
		FsFileRoot:           "/",
		FsType:               FS_FILESTORE,
		FsGcPeriod:           "1h",
		FsMaxStorage:         "10G",
		ChannelClientType:    "rpc",
		ChannelListenAddr:    "127.0.0.1:3001",
		ChannelProtocol:      "udp",
		ChannelRevealTimeout: "50",
		ChannelSettleTimeout: "120",
		ChannelDBPath:        "./DB/channel",
		AutoSetupDNSEnable:   true,
		DnsNodeMaxNum:        1,
		DnsChannelDeposit:    1000000000,
		SeedInterval:         3600,
	}
	return config
}
