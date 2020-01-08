package task

type TaskProgressState int

const (
	None TaskProgressState = iota
	TaskPause
	TaskDoing
	TaskUploadFileMakeSlice
	TaskUploadFileMakeSliceDone
	TaskUploadFilePaying     // 5
	TaskUploadFilePayingDone // 6
	TaskUploadFileCommitWhitelist
	TaskUploadFileCommitWhitelistDone
	TaskUploadFileFindReceivers
	TaskUploadFileFindReceiversDone // 10
	TaskUploadFileGeneratePDPData
	TaskUploadFileTransferBlocks
	TaskUploadFileTransferBlocksDone
	TaskUploadFileWaitForPDPProve
	TaskUploadFileWaitForPDPProveDone
	TaskUploadFileRegisterDNS
	TaskUploadFileRegisterDNSDone
	TaskDownloadFileStart
	TaskDownloadSearchPeers
	TaskDownloadFileDownloading
	TaskDownloadRequestBlocks
	TaskDownloadReceiveBlocks
	TaskDownloadPayForBlocks // 23
	TaskDownloadPayForBlocksDone
	TaskDownloadFileMakeSeed
	TaskDownloadPayForBlocksFailed
)
