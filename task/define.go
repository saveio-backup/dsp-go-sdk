package task

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
