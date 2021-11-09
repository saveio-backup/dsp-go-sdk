package dsp

import (
	"github.com/saveio/dsp-go-sdk/consts"
	sdkErr "github.com/saveio/dsp-go-sdk/error"
	"github.com/saveio/dsp-go-sdk/store"
	"github.com/saveio/dsp-go-sdk/task/base"
	"github.com/saveio/dsp-go-sdk/task/types"
	"github.com/saveio/dsp-go-sdk/types/prefix"
	"github.com/saveio/themis/common/log"
)

func (this *Dsp) AESEncryptFile(file, password, outputPath string) error {
	return this.Fs.AESEncryptFile(file, password, outputPath)
}

func (this *Dsp) GetFileNameWithPath(filePath string) string {
	return this.TaskMgr.GetFileNameWithPath(filePath)
}

func (this *Dsp) AESDecryptFile(file, prefix, password, outputPath string) error {
	return this.Fs.AESDecryptFile(file, prefix, password, outputPath)
}

func (this *Dsp) InsertShareRecord(id, fileHash, fileName, fileOwner, toWalletAddr string, profit uint64) error {
	return this.TaskMgr.InsertShareRecord(id, fileHash, fileName, fileOwner, toWalletAddr, profit)
}

func (this *Dsp) IncreaseShareRecordProfit(id string, profit uint64) error {
	return this.TaskMgr.IncreaseShareRecordProfit(id, profit)
}

func (this *Dsp) FindShareRecordById(id string) (*store.ShareRecord, error) {
	return this.TaskMgr.FindShareRecordById(id)
}

func (this *Dsp) FindShareRecordsByCreatedAt(beginedAt, endedAt, offset, limit int64) (
	[]*store.ShareRecord, int, error) {
	return this.TaskMgr.FindShareRecordsByCreatedAt(beginedAt, endedAt, offset, limit)
}

func (this *Dsp) FindLastShareTime(fileHash string) (uint64, error) {
	return this.TaskMgr.FindLastShareTime(fileHash)
}

func (this *Dsp) CountRecordByFileHash(fileHash string) (uint64, error) {
	return this.TaskMgr.CountRecordByFileHash(fileHash)
}

func (this *Dsp) SumRecordsProfit() (uint64, error) {
	return this.TaskMgr.SumRecordsProfit()
}

func (this *Dsp) SumRecordsProfitByFileHash(fileHashStr string) (uint64, error) {
	return this.TaskMgr.SumRecordsProfitByFileHash(fileHashStr)
}

func (this *Dsp) SumRecordsProfitById(id string) (uint64, error) {
	return this.TaskMgr.SumRecordsProfitById(id)
}

// IsFileEncrypted. read prefix of file to check it's encrypted or not
func (this *Dsp) IsFileEncrypted(fullFilePath string) bool {
	filePrefix, _, err := prefix.GetPrefixFromFile(fullFilePath)
	if err != nil {
		return false
	}
	return filePrefix.Encrypt
}

// DownloadFile. download file, piece by piece from addrs.
// inOrder: if true, the file will be downloaded block by block in order
// free: if true, query nodes who can share file free
// maxPeeCnt: download with max number peers who provide file at the less price
func (this *Dsp) DownloadFile(newTask bool, taskId, fileHashStr string, opt *types.DownloadOption) (err error) {
	// start a task
	defer func() {
		sdkErr, _ := err.(*sdkErr.Error)
		if err != nil {
			log.Errorf("download file %s err %s", fileHashStr, err)
		}
		log.Debugf("task %s has end, err %s", taskId, err)
		if sdkErr != nil {
			this.TaskMgr.EmitDownloadResult(taskId, nil, sdkErr)
		}
		// delete task from cache in the end
		if this.TaskMgr.IsFileDownloaded(taskId) && sdkErr == nil {
			this.TaskMgr.EmitDownloadResult(taskId, "", nil)
			this.TaskMgr.DeleteDownloadTask(taskId)
		}
	}()
	if newTask {
		downloadTask, err := this.TaskMgr.NewDownloadTask(taskId)
		if err != nil {
			return err
		}
		taskId = downloadTask.GetId()
	}
	downloadTask := this.TaskMgr.GetDownloadTask(taskId)
	if downloadTask == nil {
		return sdkErr.New(sdkErr.INTERNAL_ERROR, "task %s is nil", taskId)
	}
	if err := downloadTask.SetInfoWithOptions(
		base.FileHash(fileHashStr),
		base.RealFileSize(opt.RealFileSize),
	); err != nil {
		return sdkErr.New(sdkErr.INTERNAL_ERROR, "set file hash %s for task %s err %s",
			fileHashStr, taskId, err)
	}

	log.Debugf("downloadFile %s, url %s task %s is new task %t", fileHashStr, opt.Url, taskId, newTask)

	if len(fileHashStr) == 0 {
		log.Errorf("taskId %s no filehash for download", taskId)
		return sdkErr.New(sdkErr.DOWNLOAD_FILEHASH_NOT_FOUND, "no filehash for download")
	}

	err = downloadTask.Start(opt)
	return err
}

func (this *Dsp) PauseDownload(taskId string) error {
	tsk := this.TaskMgr.GetDownloadTask(taskId)
	if tsk == nil {
		return sdkErr.New(sdkErr.GET_FILEINFO_FROM_DB_ERROR, "task %s not found", taskId)
	}
	if err := tsk.Pause(); err != nil {
		return sdkErr.NewWithError(sdkErr.TASK_PAUSE_ERROR, err)
	}
	return nil
}

func (this *Dsp) ResumeDownload(taskId string) error {
	tsk := this.TaskMgr.GetDownloadTask(taskId)
	if tsk == nil {
		return sdkErr.New(sdkErr.GET_FILEINFO_FROM_DB_ERROR, "task %s not found", taskId)
	}
	if err := tsk.Resume(); err != nil {
		serr, _ := err.(*sdkErr.Error)
		if serr != nil {
			this.TaskMgr.EmitDownloadResult(taskId, nil, serr)
		} else {
			this.TaskMgr.EmitDownloadResult(taskId, nil, sdkErr.New(sdkErr.INTERNAL_ERROR, err.Error()))
		}
		return sdkErr.NewWithError(sdkErr.TASK_RESUME_ERROR, err)
	}
	return nil
}

func (this *Dsp) RetryDownload(taskId string) error {
	tsk := this.TaskMgr.GetDownloadTask(taskId)
	if tsk == nil {
		return sdkErr.New(sdkErr.GET_FILEINFO_FROM_DB_ERROR, "task %s not found", taskId)
	}
	if err := tsk.Retry(); err != nil {
		serr, _ := err.(*sdkErr.Error)
		if serr != nil {
			this.TaskMgr.EmitDownloadResult(taskId, nil, serr)
		} else {
			this.TaskMgr.EmitDownloadResult(taskId, nil, sdkErr.New(sdkErr.INTERNAL_ERROR, err.Error()))
		}
		return sdkErr.NewWithError(sdkErr.TASK_RETRY_ERROR, err)
	}
	return nil
}

func (this *Dsp) CancelDownload(taskId string) error {
	tsk := this.TaskMgr.GetDownloadTask(taskId)
	if tsk == nil {
		return sdkErr.New(sdkErr.GET_FILEINFO_FROM_DB_ERROR, "task %s not found", taskId)
	}
	if err := tsk.Cancel(); err != nil {
		return err
	}
	return this.DeleteDownloadedFile(taskId)
}

// DownloadFileByLink. download file by link, e.g oni://Qm...&name=xxx&tr=xxx
func (this *Dsp) DownloadFileByLink(id, linkStr string, asset int32, inOrder bool, decryptPwd string,
	free, setFileName bool, maxPeerCnt int) error {
	link, err := this.DNS.GetLinkValues(linkStr)
	if err != nil {
		return err
	}
	if maxPeerCnt > consts.MAX_PEERCNT_FOR_DOWNLOAD {
		maxPeerCnt = consts.MAX_PEERCNT_FOR_DOWNLOAD
	}
	opt := &types.DownloadOption{
		FileName:     link.FileName,
		FileOwner:    link.FileOwner,
		BlocksRoot:   link.BlocksRoot,
		Asset:        asset,
		InOrder:      inOrder,
		DecryptPwd:   decryptPwd,
		Free:         free,
		SetFileName:  setFileName,
		MaxPeerCnt:   maxPeerCnt,
		BlockNum:     link.BlockNum,
		RealFileSize: link.FileSize,
	}
	return this.downloadFileWithOpt(id, link.FileHashStr, opt)
}

// DownloadFileByUrl. download file by link, e.g dsp://file1
func (this *Dsp) DownloadFileByUrl(id, url string, asset int32, inOrder bool, decryptPwd string,
	free, setFileName bool, maxPeerCnt int) error {
	fileHashStr := this.DNS.GetFileHashFromUrl(url)
	link, err := this.DNS.GetLinkValues(this.DNS.GetLinkFromUrl(url))
	if err != nil {
		return err
	}
	if maxPeerCnt > consts.MAX_PEERCNT_FOR_DOWNLOAD {
		maxPeerCnt = consts.MAX_PEERCNT_FOR_DOWNLOAD
	}

	opt := &types.DownloadOption{
		FileName:     link.FileName,
		BlocksRoot:   link.BlocksRoot,
		Asset:        asset,
		InOrder:      inOrder,
		DecryptPwd:   decryptPwd,
		Free:         free,
		SetFileName:  setFileName,
		FileOwner:    link.FileOwner,
		MaxPeerCnt:   maxPeerCnt,
		Url:          url,
		BlockNum:     link.BlockNum,
		RealFileSize: link.FileSize,
	}
	return this.downloadFileWithOpt(id, fileHashStr, opt)
}

// DownloadFileByUrl. download file by link, e.g dsp://file1
func (this *Dsp) DownloadFileByHash(id, fileHashStr string, asset int32, inOrder bool, decryptPwd string,
	free, setFileName bool, maxPeerCnt int) error {
	// TODO: get file name, fix url
	info, _ := this.Chain.GetFileInfo(fileHashStr)
	var fileName, fileOwner, blocksRoot string
	var blockNum, realFileSize uint64
	if info != nil {
		fileName = string(info.FileDesc)
		fileOwner = info.FileOwner.ToBase58()
		blocksRoot = string(info.BlocksRoot)
		blockNum = info.FileBlockNum
		realFileSize = info.RealFileSize
	}
	if maxPeerCnt > consts.MAX_PEERCNT_FOR_DOWNLOAD {
		maxPeerCnt = consts.MAX_PEERCNT_FOR_DOWNLOAD
	}
	opt := &types.DownloadOption{
		FileName:     fileName,
		Asset:        asset,
		InOrder:      inOrder,
		BlocksRoot:   blocksRoot,
		DecryptPwd:   decryptPwd,
		Free:         free,
		SetFileName:  setFileName,
		FileOwner:    fileOwner,
		MaxPeerCnt:   maxPeerCnt,
		BlockNum:     blockNum,
		RealFileSize: realFileSize,
	}
	return this.downloadFileWithOpt(id, fileHashStr, opt)
}

// DeleteDownloadedFile. Delete downloaded file in local.
func (this *Dsp) DeleteDownloadedFile(taskId string) error {
	return this.TaskMgr.DeleteDownloadedFile(taskId)
}

// DeleteDownloadedLocalFile. Delete file in local.
func (this *Dsp) DeleteDownloadedLocalFile(fileHash string) error {
	taskId := this.TaskMgr.GetDownloadedTaskId(fileHash, this.Chain.WalletAddress())
	if len(fileHash) == 0 {
		return sdkErr.New(sdkErr.DELETE_FILE_FAILED, "delete filehash is empty")
	}
	return this.TaskMgr.DeleteDownloadedFile(taskId)
}

// AllDownloadFiles. get all downloaded file names
func (this *Dsp) AllDownloadFiles() ([]*store.TaskInfo, []string, error) {
	return this.TaskMgr.AllDownloadFiles()
}

func (this *Dsp) DownloadedFileInfo(fileHashStr string) (*store.TaskInfo, error) {
	// my task. use my wallet address
	return this.TaskMgr.GetDownloadTaskInfoByFileHash(fileHashStr), nil
}

// downloadFileWithOpt. internal helper, download or resume file with hash and options
func (this *Dsp) downloadFileWithOpt(taskId string, fileHashStr string, opt *types.DownloadOption) error {
	if len(opt.FileName) == 0 {
		// TODO: get file name from chain if the file exists on chain
		info, _ := this.Chain.GetFileInfo(fileHashStr)
		if info != nil {
			opt.FileName = string(info.FileDesc)
			opt.BlocksRoot = string(info.BlocksRoot)
			opt.FileOwner = string(info.FileOwner.ToBase58())
		}
	}

	downloadTask := this.TaskMgr.GetDownloadTaskByFileHash(fileHashStr)
	if downloadTask == nil {
		// task is not downloading
		if len(taskId) > 0 {
			log.Debugf("start a new download task of id: %s, file %s",
				taskId, fileHashStr)
		} else {
			log.Debugf("start a new download task of file %s", fileHashStr)
		}
		return this.DownloadFile(true, taskId, fileHashStr, opt)
	}

	// task is downloading
	if downloadTask.IsTaskDone() {
		log.Debugf("download task %s has done, start a new download task of id: %s, file %s",
			downloadTask.GetId(), taskId, fileHashStr)
		return this.DownloadFile(true, taskId, fileHashStr, opt)
	}
	if taskPreparing, taskDoing := downloadTask.IsTaskPreparingOrDoing(); taskPreparing || taskDoing {
		return sdkErr.New(sdkErr.DOWNLOAD_REFUSED, "task exists, and it is preparing or doing")
	}
	log.Debugf("task exists, resume the download task of id: %s, file %s", taskId, fileHashStr)
	return this.DownloadFile(false, taskId, fileHashStr, opt)
}

// // shareUploadedFile. share uploaded file when upload success
// func (this *Dsp) shareUploadedFile(filePath, fileName, prefix string, hashes []string) error {
// 	log.Debugf("shareUploadedFile path: %s filename:%s prefix:%s hashes:%d", filePath, fileName, prefix, len(hashes))
// 	if !this.IsClient() {
// 		return sdkErr.New(sdkErr.INTERNAL_ERROR, "fs type is not file store")
// 	}
// 	if len(hashes) == 0 {
// 		return sdkErr.New(sdkErr.INTERNAL_ERROR, "no block hashes")
// 	}
// 	fileHashStr := hashes[0]
// 	taskId := this.taskMgr.TaskId(fileHashStr, this.chain.WalletAddress(), store.TaskTypeDownload)
// 	if len(taskId) == 0 {
// 		var err error
// 		taskId, err = this.taskMgr.NewTask("", store.TaskTypeDownload)
// 		if err != nil {
// 			return err
// 		}
// 		if err := this.taskMgr.AddFileBlockHashes(taskId, hashes); err != nil {
// 			return err
// 		}
// 		fullFilePath := utils.GetFileFullPath(this.config.FsFileRoot, fileHashStr, fileName,
// 			utils.GetPrefixEncrypted([]byte(prefix)))
// 		if err := this.taskMgr.SetTaskInfoWithOptions(taskId, task.Prefix(prefix),
// 			task.FileName(fullFilePath)); err != nil {
// 			return err
// 		}
// 	}
// 	fullFilePath, err := this.taskMgr.GetFilePath(taskId)
// 	if err != nil {
// 		return err
// 	}
// 	input, err := os.Open(filePath)
// 	if err != nil {
// 		return err
// 	}
// 	output, err := os.OpenFile(fullFilePath, os.O_CREATE|os.O_RDWR, 0666)
// 	if err != nil {
// 		return err
// 	}
// 	defer output.Close()
// 	n, err := io.Copy(output, input)
// 	log.Debugf("copy %d bytes", n)
// 	if err != nil {
// 		return err
// 	}
// 	// TODO: set nil prefix for encrypted file
// 	this.fs.SetFsFilePrefix(fullFilePath, prefix)
// 	offsets, err := this.fs.GetAllOffsets(fileHashStr)
// 	if err != nil {
// 		return err
// 	}
// 	for index, hash := range hashes {
// 		if this.taskMgr.IsBlockDownloaded(taskId, hash, uint64(index)) {
// 			log.Debugf("%s-%s-%d is downloaded", fileHashStr, hash, index)
// 			continue
// 		}
// 		block := this.fs.GetBlock(hash)
// 		links, err := this.fs.GetBlockLinks(block)
// 		if err != nil {
// 			return err
// 		}
// 		offsetKey := fmt.Sprintf("%s-%d", hash, index)
// 		offset := offsets[offsetKey]
// 		log.Debugf("hash: %s-%s-%d , offset: %d, links count %d", fileHashStr, hash, index, offset, len(links))
// 		if err := this.taskMgr.SetBlockDownloaded(taskId, fileHashStr, client.P2pGetPublicAddr(), uint64(index),
// 			int64(offset), links); err != nil {
// 			return err
// 		}
// 	}
// 	go this.dns.PushToTrackers(fileHashStr, this.dns.TrackerUrls, client.P2pGetPublicAddr())
// 	return nil
// }
