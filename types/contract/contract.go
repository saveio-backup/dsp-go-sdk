package contract

// type Participant struct {
// 	WalletAddr     chainCom.Address
// 	Deposit        uint64
// 	WithDrawAmount uint64
// 	IP             []byte
// 	Port           []byte
// 	Balance        uint64
// 	BalanceHash    []byte
// 	Nonce          uint64
// 	IsCloser       bool
// 	LocksRoot      []byte
// 	LockedAmount   uint64
// }

// type ChannelInfo struct {
// 	ChannelID         uint64
// 	ChannelState      uint64
// 	Participant1      Participant
// 	Participant2      Participant
// 	SettleBlockHeight uint64
// }

// type DNSNodeInfo struct {
// 	WalletAddr  chainCom.Address
// 	IP          []byte
// 	Port        []byte
// 	InitDeposit uint64
// 	PeerPubKey  string
// }

// type NameInfo struct {
// 	Type        uint64
// 	Header      []byte
// 	URL         []byte
// 	Name        []byte
// 	NameOwner   chainCom.Address
// 	Desc        []byte
// 	BlockHeight uint64
// 	TTL         uint64 // 0: bypass
// }

// type NameInfoList struct {
// 	NameNum uint64
// 	List    []NameInfo
// }

// type Status uint8

// type PeerPoolItem struct {
// 	PeerPubkey    string           //peer pubkey
// 	WalletAddress chainCom.Address //peer owner
// 	Status        Status           //peer status
// 	TotalInitPos  uint64           //total authorize pos this peer received
// }

// type PeerPoolMap struct {
// 	PeerPoolMap map[string]*PeerPoolItem
// }

// const (
// 	PRIVATE   = 0
// 	PUBLIC    = 1
// 	WHITELIST = 2
// )

// const (
// 	FileStorageTypeUseSpace = 0
// 	FileStorageTypeCustom   = 1
// )

// type NodeList struct {
// 	AddrNum  uint64
// 	AddrList []chainCom.Address
// }

// type FileInfo struct {
// 	FileHash       []byte
// 	FileOwner      chainCom.Address
// 	FileDesc       []byte
// 	Privilege      uint64
// 	FileBlockNum   uint64
// 	FileBlockSize  uint64
// 	ProveInterval  uint64
// 	ProveTimes     uint64
// 	ExpiredHeight  uint64
// 	CopyNum        uint64
// 	Deposit        uint64
// 	FileProveParam []byte
// 	ProveBlockNum  uint64
// 	BlockHeight    uint64 // store file info block height
// 	ValidFlag      bool
// 	StorageType    uint64
// 	RealFileSize   uint64
// 	PrimaryNodes   NodeList // Nodes store file
// 	CandidateNodes NodeList // Nodes backup file
// 	BlocksRoot     []byte
// }

// type FileInfoList struct {
// 	FileNum uint64
// 	List    []FileInfo
// }

// type BakTasks struct {
// 	TaskNum uint64
// 	Tasks   []BakTask
// }

// type BakTask struct {
// 	FileHash   []byte
// 	DeadLine   uint64
// 	BakNum     uint64
// 	BakHeight  uint64
// 	LuckyNum   uint64
// 	BakSrvAddr []byte
// 	BackUpAddr chainCom.Address
// 	BrokenAddr chainCom.Address
// 	LuckyAddr  chainCom.Address
// }

// type StorageFee struct {
// 	TxnFee        uint64
// 	SpaceFee      uint64
// 	ValidationFee uint64
// }

// type FsNodesInfo struct {
// 	NodeNum  uint64
// 	NodeInfo []FsNodeInfo
// }

// type FsNodeInfo struct {
// 	Pledge      uint64
// 	Profit      uint64
// 	Volume      uint64
// 	RestVol     uint64
// 	ServiceTime uint64
// 	WalletAddr  chainCom.Address
// 	NodeAddr    []byte
// }

// type ProveParam struct {
// 	G      []byte
// 	G0     []byte
// 	PubKey []byte
// 	FileId []byte
// }

// type Rule struct {
// 	Addr         chainCom.Address
// 	BaseHeight   uint64
// 	ExpireHeight uint64
// }

// type FsProveDetails struct {
// 	CopyNum        uint64
// 	ProveDetailNum uint64
// 	ProveDetails   []ProveDetail
// }

// type ProveDetail struct {
// 	NodeAddr    []byte
// 	WalletAddr  chainCom.Address
// 	ProveTimes  uint64
// 	BlockHeight uint64 // block height for first file prove
// 	Finished    bool
// }

// type FileHash struct {
// 	Hash []byte
// }

// type FileList struct {
// 	FileNum uint64
// 	List    []FileHash
// }

// type FsSetting struct {
// 	FsGasPrice         uint64 // gas price for fs contract
// 	GasPerGBPerBlock   uint64 // gas for store block
// 	GasPerKBForRead    uint64 // gas for read file
// 	GasForChallenge    uint64 // gas for challenge
// 	MaxProveBlockNum   uint64
// 	MinProveInterval   uint64
// 	MinVolume          uint64
// 	DefaultProvePeriod uint64 // default prove interval
// 	DefaultCopyNum     uint64 // default copy number
// }

// type WhiteList struct {
// 	Num  uint64
// 	List []Rule
// }

// type UploadOption struct {
// 	FileDesc        []byte
// 	FileSize        uint64
// 	ProveInterval   uint64
// 	ExpiredHeight   uint64
// 	Privilege       uint64
// 	CopyNum         uint64
// 	Encrypt         bool
// 	EncryptPassword []byte
// 	RegisterDNS     bool
// 	BindDNS         bool
// 	DnsURL          []byte
// 	WhiteList       WhiteList
// 	Share           bool
// 	StorageType     uint64
// }

// // UserSpace. used to stored user space
// type UserSpace struct {
// 	Used         uint64 // used space
// 	Remain       uint64 // remain space, equal to blockNum*blockSize
// 	ExpireHeight uint64 // expired block height
// 	Balance      uint64 // balance of asset
// 	UpdateHeight uint64 // update block height
// }

// // UserSpaceParams. used to update user spaces
// type UserSpaceType uint64

// const (
// 	UserSpaceNone UserSpaceType = iota
// 	UserSpaceAdd
// 	UserSpaceRevoke
// )

// type UserSpaceOperation struct {
// 	Type  uint64
// 	Value uint64
// }

// type TransferState struct {
// 	From  chainCom.Address
// 	To    chainCom.Address
// 	Value uint64
// }

// const (
// 	SYSTEM            uint64 = 0x00
// 	CUSTOM_HEADER     uint64 = 0x01
// 	CUSTOM_URL        uint64 = 0x02
// 	CUSTOM_HEADER_URL uint64 = 0x04
// 	UPDATE            uint64 = 0x08
// )

// type NameType uint64

// const (
// 	NameTypeNormal NameType = iota
// 	NameTypePlugin
// )
