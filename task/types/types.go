package types

import (
	"fmt"

	"github.com/saveio/dsp-go-sdk/network/message/types/block"
	"github.com/saveio/dsp-go-sdk/store"
	chainCom "github.com/saveio/themis/common"
	"github.com/saveio/themis/smartcontract/service/native/savefs"
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
}

type DownloadOption struct {
	FileName    string
	FileOwner   string
	BlocksRoot  string
	Asset       int32
	InOrder     bool
	DecryptPwd  string
	Free        bool
	SetFileName bool
	MaxPeerCnt  int
	Url         string
	BlockNum    uint64
}

type WorkerNetMsgPhase int

const (
	WorkNetPhaseNone WorkerNetMsgPhase = iota
	WorkerSendFileAsk
	WorkerRecvFileAsk
	WorkerSendFileAck
	WorkerRecvFileAck
	WorkerSendFileRdy
	WorkerRecvFileRdy
	WorkerSendFilePause
	WorkerRecvFilePause
	WorkerSendFileResume
	WorkerRecvFileResume
	WorkerSendFileCancel
	WorkerRecvFileCancel
	WorkerSendFetchDone
	WorkerRecvFileDone
	WorkerSendDownloadAsk
	WorkerRecvDownloadAsk
	WorkerSendDownloadAck
	WorkerRecvDownloadAck
	WorkerSendDownloadDone
	WorkerRecvDownloadDone
	WorkerSendDownloadCancel
	WorkerRecvDownloadCancel
	WorkerSendFileDelete
	WorkerRecvDownloadDelete
	WorkerSendFileDeleteDone
	WorkerRecvFileDeleteDone
)

type Gas struct {
	GasPrice uint64
	GasLimit uint64
}
type NodeInfo struct {
	HostAddr   string
	WalletAddr string
	UpdatedAt  uint64
}

func (n NodeInfo) String() string {
	return fmt.Sprintf("wallet address: %s, host address: %s", n.WalletAddr, n.HostAddr)
}

type TaskProgressState int

const (
	None                              TaskProgressState = iota // 0
	TaskPause                                                  // 1
	TaskDoing                                                  // 2
	TaskUploadFileMakeSlice                                    // 3
	TaskUploadFileMakeSliceDone                                // 4
	TaskUploadFilePaying                                       // 5
	TaskUploadFilePayingDone                                   // 6
	TaskUploadFileCommitWhitelist                              // 7
	TaskUploadFileCommitWhitelistDone                          // 8
	TaskUploadFileFindReceivers                                // 9
	TaskUploadFileFindReceiversDone                            // 10
	TaskUploadFileGeneratePDPData                              // 11
	TaskUploadFileTransferBlocks                               // 12
	TaskUploadFileTransferBlocksDone                           // 13
	TaskUploadFileWaitForPDPProve                              // 14
	TaskUploadFileWaitForPDPProveDone                          // 15
	TaskUploadFileRegisterDNS                                  // 16
	TaskUploadFileRegisterDNSDone                              // 17
	TaskDownloadFileStart                                      // 18
	TaskDownloadSearchPeers                                    // 19
	TaskDownloadFileDownloading                                // 20
	TaskDownloadRequestBlocks                                  // 21
	TaskDownloadReceiveBlocks                                  // 22
	TaskDownloadPayForBlocks                                   // 23
	TaskDownloadPayForBlocksDone                               // 24
	TaskDownloadFileMakeSeed                                   // 25
	TaskDownloadPayForBlocksFailed                             // 26
	TaskDownloadCheckingFile                                   // 27
	TaskDownloadCheckingFileFailed                             // 28
	TaskDownloadCheckingFileDone                               // 29
	TaskWaitForBlockConfirmed                                  // 30
	TaskWaitForBlockConfirmedDone                              // 31
	TaskCreate                                                 // 32
)

type ProgressInfo struct {
	TaskId        string
	Type          store.TaskType                // task type
	Url           string                        // task url
	StoreType     uint32                        // store type
	FileName      string                        // file name
	FileHash      string                        // file hash
	FilePath      string                        // file path
	CopyNum       uint32                        // copyNum
	FileSize      uint64                        // file size
	RealFileSize  uint64                        // real file size
	Total         uint64                        // total file's blocks count
	Encrypt       bool                          // file encrypted or not
	Progress      map[string]store.FileProgress // address <=> progress
	SlaveProgress map[string]store.FileProgress // progress for slave nodes
	NodeHostAddrs map[string]string             // node host addrs map
	TaskState     store.TaskState               // task state
	ProgressState TaskProgressState             // TaskProgressState
	Result        interface{}                   // finish result
	ErrorCode     uint32                        // error code
	ErrorMsg      string                        // interrupt error
	CreatedAt     uint64
	UpdatedAt     uint64
}
type ShareState int

const (
	ShareStateBegin ShareState = iota
	ShareStateReceivedPaying
	ShareStateEnd
)

type ShareNotification struct {
	TaskKey       string
	State         ShareState
	FileHash      string
	FileName      string
	FileOwner     string
	ToWalletAddr  string
	PaymentId     uint64
	PaymentAmount uint64
}

type BackupFileOpt struct {
	LuckyNum   uint64
	BakNum     uint64
	BakHeight  uint64
	BackUpAddr string
	BrokenAddr string
}

type GetBlockReq struct {
	TimeStamp     int64
	FileHash      string
	Hash          string
	Index         uint64
	PeerAddr      string
	WalletAddress string
	Asset         int32
	Syn           string
}

type WorkerState struct {
	Working     bool
	Unpaid      bool
	TotalFailed map[string]uint32
}

type BlockResp struct {
	Hash      string
	Index     uint64
	PeerAddr  string
	Block     []byte
	Tag       []byte
	Offset    int64
	PaymentId int32
}

type JobFunc func(arg1 string, arg2 string, arg3 string, blocks []*block.Block) ([]*BlockResp, error)

// BlockMsgData. data to send of block msg
type BlockMsgData struct {
	BlockData []byte
	DataLen   uint64
	Tag       []byte
	Offset    uint64
	RefCnt    int
}

type AddPlotFileResp struct {
	TaskId             string
	UpdateNodeTxHash   string
	CreateSectorTxHash string
}

type PlotFileInfo struct {
	FileName    string
	FileHash    string
	FileOwner   string
	FileSize    uint64
	BlockHeight uint64
	PlotInfo    *savefs.PlotInfo
}

type AllPlotsFileResp struct {
	TotalCount int
	TotalSize  uint64
	FileInfos  []*PlotFileInfo
}

type PocTaskPDPState int

const (
	PocTaskPDPStateNonce = iota
	PocTaskPDPStateGeneratingPdp
	PocTaskPDPStatePdpSaved
	PocTaskPDPStateSubmitted
)

type PocTaskInfo struct {
	TaskId       string
	FileName     string
	FileHash     string
	FileOwner    string
	FileSize     uint64
	RealFileSize uint64
	BlockHeight  uint64
	Progress     float64
	PDPState     PocTaskPDPState
	TaskState    store.TaskState
	PlotInfo     *savefs.PlotInfo
	ProveTimes   uint64
}

type AllPocTaskResp struct {
	TotalCount      int
	TotalSize       uint64
	TotalProvedSize uint64
	PocTaskInfos    []*PocTaskInfo
}
