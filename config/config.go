package config

type FSType int

const (
	FS_FILESTORE = iota
	FS_BLOCKSTORE
)

type DspConfig struct {
	DBPath             string   // level DB data path
	ChainRpcAddr       string   // chain rpc address
	ChainRpcAddrs      []string // chain rpc addresses
	CheckDepositBlkNum uint64   // check deposit price of block num. if it is 0, no checking
	BlockConfirm       uint32   // block confirmation of tx

	FsRepoRoot   string // fs block store repo root path
	FsFileRoot   string // fs file store root path
	FsType       FSType // fs type
	FsGcPeriod   string // fs gc period
	FsMaxStorage string // fs max storage
	EnableBackup bool   // enable backup file

	ChannelClientType    string // channel client type. e.g: "rpc"
	ChannelListenAddr    string // channel listen address. e.g: "127.0.0.1:3001"
	ChannelProtocol      string // channel network protocol. e.g: "tcp"
	ChannelRevealTimeout string // channel reveal timeout. e.g: "50"
	ChannelSettleTimeout string // channel settle timeout. e.g: "120"
	ChannelDBPath        string // channel DB path
	BlockDelay           string // block delay for confirmation
	MaxUnpaidPayment     int32  // max unpaid payments for sharing a file

	AutoSetupDNSEnable bool     // enable auto setup DNS node or not
	DnsNodeMaxNum      int      // dns node max count
	DnsChannelDeposit  uint64   // deposit amount of channel between self and dns node
	SeedInterval       int      // push file to tracker interval in second, if it's 0, no push
	TrackerProtocol    string   // tracker protocol
	Trackers           []string // tracker address list
	DNSWalletAddrs     []string // DNS wallet ADDRESS
}

func DefaultDspConfig() *DspConfig {
	config := &DspConfig{
		DBPath:               "./DB/dsp",
		ChainRpcAddrs:        []string{"http://127.0.0.1:20336"},
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
