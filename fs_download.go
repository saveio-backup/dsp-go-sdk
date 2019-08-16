package dsp

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"sync"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/saveio/dsp-go-sdk/actor/client"
	"github.com/saveio/dsp-go-sdk/common"
	"github.com/saveio/dsp-go-sdk/config"
	serr "github.com/saveio/dsp-go-sdk/error"
	netcom "github.com/saveio/dsp-go-sdk/network/common"
	"github.com/saveio/dsp-go-sdk/network/message"
	"github.com/saveio/dsp-go-sdk/network/message/types/file"
	"github.com/saveio/dsp-go-sdk/store"
	"github.com/saveio/dsp-go-sdk/task"
	"github.com/saveio/dsp-go-sdk/utils"
	chainCom "github.com/saveio/themis/common"
	"github.com/saveio/themis/common/log"
)

// DownloadFile. download file, piece by piece from addrs.
// inOrder: if true, the file will be downloaded block by block in order
// free: if true, query nodes who can share file free
// maxPeeCnt: download with max number peers who provide file at the less price
func (this *Dsp) DownloadFile(taskId, fileHashStr string, opt *common.DownloadOption) error {
	// start a task
	var sdkErr *serr.SDKError
	var err error
	if len(taskId) == 0 {
		taskId, err = this.taskMgr.NewTask(task.TaskTypeDownload)
	} else {
		log.Warnf("download task has exists %s", taskId)
		if this.taskMgr.GetTaskState(taskId) == task.TaskStateDoing || this.taskMgr.GetTaskState(taskId) == task.TaskStatePrepare {
			log.Warnf("download task is doing")
			return nil
		}
	}
	log.Debugf("download file id: %s, fileHash: %s, option: %v", taskId, fileHashStr, opt)
	defer func() {
		if err != nil {
			log.Errorf("download file %s err %s", fileHashStr, err)
		}
		log.Debugf("emit ret %s %s", taskId, err)
		this.taskMgr.EmitResult(taskId, nil, sdkErr)
		// delete task from cache in the end
		if this.taskMgr.IsFileDownloaded(taskId) && sdkErr == nil {
			this.taskMgr.DeleteTask(taskId, false)
		}
	}()
	if err != nil {
		sdkErr = serr.NewDetailError(serr.NEW_TASK_FAILED, err.Error())
		return err
	}
	this.taskMgr.EmitProgress(taskId, task.TaskDownloadFileStart)
	if len(fileHashStr) == 0 {
		log.Errorf("taskId %s no filehash for download", taskId)
		err = errors.New("no filehash for download")
		sdkErr = serr.NewDetailError(serr.DOWNLOAD_FILEHASH_NOT_FOUND, err.Error())
		return err
	}
	if this.DNS.DNSNode == nil {
		err = errors.New("no online dns node")
		sdkErr = serr.NewDetailError(serr.NO_CONNECTED_DNS, err.Error())
		return err
	}
	log.Debugf("download file dns node %s", this.DNS.DNSNode.WalletAddr)
	this.taskMgr.SetTaskInfos(taskId, fileHashStr, "", opt.FileName, this.WalletAddress())
	err = this.taskMgr.SetFileDownloadOptions(taskId, opt)
	err = this.taskMgr.BindTaskId(taskId)
	if err != nil {
		sdkErr = serr.NewDetailError(serr.SET_FILEINFO_DB_ERROR, err.Error())
		return err
	}
	pause, sdkErr := this.checkIfPauseDownload(taskId, fileHashStr)
	if sdkErr != nil {
		return errors.New(sdkErr.Error.Error())
	}
	if pause {
		return nil
	}
	addrs := this.GetPeerFromTracker(fileHashStr, this.DNS.TrackerUrls)
	log.Debugf("get addr from peer %v, hash %s %v", addrs, fileHashStr, this.DNS.TrackerUrls)
	if len(addrs) == 0 {
		err = fmt.Errorf("get 0 peer of %s from %d trackers", fileHashStr, len(this.DNS.TrackerUrls))
		sdkErr = serr.NewDetailError(serr.NO_DOWNLOAD_SEED, err.Error())
		return err
	}
	if opt.MaxPeerCnt > common.MAX_DOWNLOAD_PEERS_NUM {
		opt.MaxPeerCnt = common.MAX_DOWNLOAD_PEERS_NUM
	}
	pause, sdkErr = this.checkIfPauseDownload(taskId, fileHashStr)
	if sdkErr != nil {
		return errors.New(sdkErr.Error.Error())
	}
	if pause {
		return nil
	}
	sdkErr = this.downloadFileFromPeers(taskId, fileHashStr, opt.Asset, opt.InOrder, opt.DecryptPwd, opt.Free, opt.SetFileName, opt.MaxPeerCnt, addrs)
	if sdkErr != nil {
		err = errors.New(sdkErr.Error.Error())
	}
	return err
}

func (this *Dsp) PauseDownload(taskId string) error {
	taskType := this.taskMgr.TaskType(taskId)
	if taskType != task.TaskTypeDownload {
		return fmt.Errorf("task %s is not a download task", taskId)
	}
	canPause, err := this.taskMgr.IsTaskCanPause(taskId)
	if err != nil {
		log.Errorf("pause task err %s", err)
		return err
	}
	if !canPause {
		log.Debugf("task is pausing")
		return nil
	}
	err = this.taskMgr.SetTaskState(taskId, task.TaskStatePause)
	if err != nil {
		return err
	}
	this.taskMgr.EmitProgress(taskId, task.TaskPause)
	return nil
}

func (this *Dsp) ResumeDownload(taskId string) error {
	taskType := this.taskMgr.TaskType(taskId)
	if taskType != task.TaskTypeDownload {
		return fmt.Errorf("task %s is not a download task", taskId)
	}
	canResume, err := this.taskMgr.IsTaskCanResume(taskId)
	if err != nil {
		log.Errorf("resume task err %s", err)
		return err
	}
	if !canResume {
		log.Debugf("task is resuming")
		return nil
	}
	err = this.taskMgr.SetTaskState(taskId, task.TaskStateDoing)
	if err != nil {
		return err
	}
	this.taskMgr.EmitProgress(taskId, task.TaskDoing)
	return this.checkIfResumeDownload(taskId)
}

func (this *Dsp) RetryDownload(taskId string) error {
	taskType := this.taskMgr.TaskType(taskId)
	if taskType != task.TaskTypeDownload {
		return fmt.Errorf("task %s is not a download task", taskId)
	}
	failed, err := this.taskMgr.IsTaskFailed(taskId)
	if err != nil {
		return err
	}
	if !failed {
		return fmt.Errorf("task %s is not failed", taskId)
	}
	err = this.taskMgr.SetTaskState(taskId, task.TaskStateDoing)
	if err != nil {
		return err
	}
	this.taskMgr.EmitProgress(taskId, task.TaskDoing)
	return this.checkIfResumeDownload(taskId)
}

func (this *Dsp) CancelDownload(taskId string) error {
	taskType := this.taskMgr.TaskType(taskId)
	if taskType != task.TaskTypeDownload {
		return fmt.Errorf("task %s is not a download task", taskId)
	}
	cancel, err := this.taskMgr.IsTaskCancel(taskId)
	if err != nil {
		return err
	}
	if cancel {
		return fmt.Errorf("task is cancelling: %s", taskId)
	}
	fileHashStr := this.taskMgr.TaskFileHash(taskId)
	sessions, err := this.taskMgr.GetFileSessions(taskId)
	if err != nil {
		return err
	}
	if len(sessions) == 0 {
		log.Debugf("no sessions to cancel")
		err := this.DeleteDownloadedFile(taskId)
		if err != nil {
			return err
		}
		return nil
	}
	log.Debugf("sessions %v", sessions)
	// broadcast download cancel msg
	wg := sync.WaitGroup{}
	for hostAddr, session := range sessions {
		wg.Add(1)
		go func(a string, ses *store.Session) {
			msg := message.NewFileDownloadCancel(ses.SessionId, fileHashStr, this.WalletAddress(), int32(ses.Asset))
			client.P2pRequestWithRetry(msg.ToProtoMsg(), a, common.MAX_NETWORK_REQUEST_RETRY)
			wg.Done()
		}(hostAddr, session)
	}
	wg.Wait()
	return this.DeleteDownloadedFile(taskId)
}

// DownloadFileByLink. download file by link, e.g oni://Qm...&name=xxx&tr=xxx
func (this *Dsp) DownloadFileByLink(link string, asset int32, inOrder bool, decryptPwd string, free, setFileName bool, maxPeerCnt int) error {
	fileHashStr := utils.GetFileHashFromLink(link)
	linkvalues := this.GetLinkValues(link)
	fileName := linkvalues[common.FILE_LINK_NAME_KEY]
	opt := &common.DownloadOption{
		FileName:    fileName,
		Asset:       asset,
		InOrder:     inOrder,
		DecryptPwd:  decryptPwd,
		Free:        free,
		SetFileName: setFileName,
		MaxPeerCnt:  maxPeerCnt,
	}
	taskId := this.taskMgr.TaskId(fileHashStr, this.WalletAddress(), task.TaskTypeDownload)
	if !this.taskMgr.TaskExist(taskId) {
		log.Debugf("DownloadFileByLink %s, hash %s, opt %v", fileHashStr, fileHashStr, opt)
		return this.DownloadFile("", fileHashStr, opt)
	}
	taskDone, _ := this.taskMgr.IsTaskDone(taskId)
	if taskDone {
		log.Debugf("DownloadFileByLink %s, hash %s, opt %v, download a done task", fileHashStr, fileHashStr, opt)
		return this.DownloadFile("", fileHashStr, opt)
	}
	return fmt.Errorf("task has exist, but not finished %s", taskId)
}

// DownloadFileByUrl. download file by link, e.g dsp://file1
func (this *Dsp) DownloadFileByUrl(url string, asset int32, inOrder bool, decryptPwd string, free, setFileName bool, maxPeerCnt int) error {
	fileHashStr := this.GetFileHashFromUrl(url)
	linkvalues := this.GetLinkValues(this.GetLinkFromUrl(url))
	fileName := linkvalues[common.FILE_LINK_NAME_KEY]
	opt := &common.DownloadOption{
		FileName:    fileName,
		Asset:       asset,
		InOrder:     inOrder,
		DecryptPwd:  decryptPwd,
		Free:        free,
		SetFileName: setFileName,
		MaxPeerCnt:  maxPeerCnt,
	}
	taskId := this.taskMgr.TaskId(fileHashStr, this.WalletAddress(), task.TaskTypeDownload)
	if !this.taskMgr.TaskExist(taskId) {
		log.Debugf("DownloadFileByUrl %s, hash %s, opt %v", url, fileHashStr, opt)
		return this.DownloadFile("", fileHashStr, opt)
	}
	taskDone, _ := this.taskMgr.IsTaskDone(taskId)
	if taskDone {
		log.Debugf("DownloadFileByUrl %s, hash %s, opt %v, download a done task", url, fileHashStr, opt)
		return this.DownloadFile("", fileHashStr, opt)
	}
	return fmt.Errorf("task has exist, but not finished %s", taskId)
}

// DownloadFileByUrl. download file by link, e.g dsp://file1
func (this *Dsp) DownloadFileByHash(fileHashStr string, asset int32, inOrder bool, decryptPwd string, free, setFileName bool, maxPeerCnt int) error {
	// TODO: get file name
	opt := &common.DownloadOption{
		FileName:    "",
		Asset:       asset,
		InOrder:     inOrder,
		DecryptPwd:  decryptPwd,
		Free:        free,
		SetFileName: setFileName,
		MaxPeerCnt:  maxPeerCnt,
	}
	taskId := this.taskMgr.TaskId(fileHashStr, this.WalletAddress(), task.TaskTypeDownload)
	if !this.taskMgr.TaskExist(taskId) {
		log.Debugf("DownloadFileByHash %s, hash %s, opt %v", fileHashStr, fileHashStr, opt)
		return this.DownloadFile("", fileHashStr, opt)
	}
	taskDone, _ := this.taskMgr.IsTaskDone(taskId)
	if taskDone {
		log.Debugf("DownloadFileByHash %s, hash %s, opt %v, download a done task", fileHashStr, fileHashStr, opt)
		return this.DownloadFile("", fileHashStr, opt)
	}
	return fmt.Errorf("task has exist, but not finished %s", taskId)
}

// GetDownloadQuotation. get peers and the download price of the file. if free flag is set, return price-free peers.
func (this *Dsp) GetDownloadQuotation(fileHashStr string, asset int32, free bool, addrs []string) (map[string]*file.Payment, error) {
	if len(addrs) == 0 {
		return nil, errors.New("no peer for download")
	}
	taskId := this.taskMgr.TaskId(fileHashStr, this.WalletAddress(), task.TaskTypeDownload)
	sessions, err := this.taskMgr.GetFileSessions(taskId)
	if err != nil {
		return nil, err
	}
	peerPayInfos := make(map[string]*file.Payment, 0)
	if sessions != nil {
		// use original sessions
		log.Debugf("use original sessions: %v", sessions)
		for hostAddr, session := range sessions {
			peerPayInfos[hostAddr] = &file.Payment{
				WalletAddress: session.WalletAddr,
				Asset:         int32(session.Asset),
				UnitPrice:     session.UnitPrice,
			}
			this.taskMgr.SetSessionId(taskId, session.WalletAddr, session.SessionId)
		}
		log.Debugf("get session from db : %v", peerPayInfos)
		return peerPayInfos, nil
	}
	msg := message.NewFileDownloadAsk(fileHashStr, this.WalletAddress(), asset)
	blockHashes := make([]string, 0)
	prefix := ""
	replyLock := &sync.Mutex{}
	log.Debugf("will broadcast file download ask msg to %v", addrs)
	reply := func(msg proto.Message, addr string) {
		p2pMsg := message.ReadMessage(msg)
		if p2pMsg.Error != nil {
			log.Errorf("get download ask err %s", p2pMsg.Error.Message)
			return
		}
		fileMsg := p2pMsg.Payload.(*file.File)
		if len(fileMsg.BlockHashes) < len(blockHashes) {
			return
		}
		if len(prefix) > 0 && fileMsg.Prefix != prefix {
			return
		}
		if free && (fileMsg.PayInfo != nil && fileMsg.PayInfo.UnitPrice != 0) {
			return
		}
		replyLock.Lock()
		defer replyLock.Unlock()
		blockHashes = fileMsg.BlockHashes
		peerPayInfos[addr] = fileMsg.PayInfo
		prefix = fileMsg.Prefix
		this.taskMgr.AddFileSession(taskId, fileMsg.SessionId, fileMsg.PayInfo.WalletAddress, addr, uint64(fileMsg.PayInfo.Asset), fileMsg.PayInfo.UnitPrice)
	}
	ret, err := client.P2pBroadcast(addrs, msg.ToProtoMsg(), true, nil, reply)
	log.Debugf("broadcast file download msg result %v err %s", ret, err)
	log.Debugf("peer prices:%v", peerPayInfos)
	if len(peerPayInfos) == 0 {
		return nil, errors.New("no peerPayInfos for download")
	}

	totalCount := this.taskMgr.GetFileTotalBlockCount(taskId)
	if uint64(len(blockHashes)) > totalCount {
		log.Debugf("AddFileBlockHashes id %v hashes %s-%s, prefix %s", taskId, blockHashes[0], blockHashes[len(blockHashes)-1], prefix)
		err = this.taskMgr.AddFileBlockHashes(taskId, blockHashes)
		if err != nil {
			return nil, err
		}
		err = this.taskMgr.BatchSetFileInfo(taskId, nil, prefix, this.taskMgr.FileNameFromTask(taskId), uint64(len(blockHashes)))
		if err != nil {
			return nil, err
		}
	}
	return peerPayInfos, nil
}

// DepositChannelForFile. deposit channel. peerPrices: peer network address <=> payment info
func (this *Dsp) DepositChannelForFile(fileHashStr string, peerPrices map[string]*file.Payment) error {
	taskId := this.taskMgr.TaskId(fileHashStr, this.WalletAddress(), task.TaskTypeDownload)
	if !this.taskMgr.IsFileInfoExist(taskId) {
		return errors.New("download info not exist")
	}
	log.Debugf("GetExternalIP for downloaded nodes %v", peerPrices)
	if len(peerPrices) == 0 {
		return errors.New("no peers to deposit channel")
	}
	for _, payInfo := range peerPrices {
		hostAddr, err := this.GetExternalIP(payInfo.WalletAddress)
		log.Debugf("Set host addr after deposit channel %s - %s, err %s", payInfo.WalletAddress, hostAddr, err)
		if len(hostAddr) == 0 || err != nil {
			continue
		}
		// TODO: change to parallel job
		go client.P2pConnect(hostAddr)
	}
	if !this.Config.AutoSetupDNSEnable {
		return nil
	}
	totalCount := this.taskMgr.GetFileTotalBlockCount(taskId)
	log.Debugf("get blockhashes from %s, totalCount %d", taskId, totalCount)
	if totalCount == 0 {
		return errors.New("no blocks")
	}
	totalAmount := common.FILE_DOWNLOAD_UNIT_PRICE * uint64(totalCount) * uint64(common.CHUNK_SIZE)
	log.Debugf("deposit to channel price:%d, cnt:%d, chunksize:%d, total:%d", common.FILE_DOWNLOAD_UNIT_PRICE, totalCount, common.CHUNK_SIZE, totalAmount)
	if totalAmount/common.FILE_DOWNLOAD_UNIT_PRICE != uint64(totalCount)*uint64(common.CHUNK_SIZE) {
		return errors.New("deposit amount overflow")
	}
	curBal, _ := this.Channel.GetAvailableBalance(this.DNS.DNSNode.WalletAddr)
	if curBal < totalAmount {
		log.Debugf("depositing...")
		err := this.Channel.SetDeposit(this.DNS.DNSNode.WalletAddr, totalAmount)
		log.Debugf("deposit result %s", err)
		if err != nil {
			return err
		}
	}
	return nil
}

// PayForData. pay for block
func (this *Dsp) PayForBlock(payInfo *file.Payment, addr, fileHashStr string, blockSize uint64) (int32, error) {
	if payInfo == nil {
		log.Warn("payinfo is nil")
		return 0, nil
	}
	amount := blockSize * payInfo.UnitPrice
	if amount/blockSize != payInfo.UnitPrice {
		return 0, errors.New("total price overflow")
	}
	if amount == 0 {
		log.Warn("pay amount is 0")
		return 0, nil
	}
	taskId := this.taskMgr.TaskId(fileHashStr, this.WalletAddress(), task.TaskTypeDownload)
	err := this.taskMgr.AddFileUnpaid(taskId, payInfo.WalletAddress, payInfo.Asset, amount)
	if err != nil {
		return 0, err
	}
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	paymentId := r.Int31()
	log.Debugf("paying to %s, id %v, err:%s", payInfo.WalletAddress, paymentId, err)
	err = this.Channel.MediaTransfer(paymentId, amount, payInfo.WalletAddress)
	if err != nil {
		log.Debugf("mediaTransfer failed paymentId %d, payTo: %s, err %s", paymentId, payInfo.WalletAddress, err)
		return 0, err
	}
	log.Debugf("paying to %s, id %v success", payInfo.WalletAddress, paymentId)
	log.Debugf("send paymenMsg paymentId %d, for file:%s, size:%d, price:%d success", paymentId, fileHashStr, blockSize, amount)
	// send payment msg
	msg := message.NewPayment(this.WalletAddress(), payInfo.WalletAddress, paymentId,
		payInfo.Asset, amount, fileHashStr, netcom.MSG_ERROR_CODE_NONE)
	// TODO: wait for receiver received notification (need optimized)
	_, err = client.P2pRequestWithRetry(msg.ToProtoMsg(), addr, common.MAX_NETWORK_REQUEST_RETRY)
	log.Debugf("payment msg response :%d, err:%s", paymentId, err)
	if err != nil {
		return 0, err
	}
	// clean unpaid order
	err = this.taskMgr.DeleteFileUnpaid(taskId, payInfo.WalletAddress, payInfo.Asset, amount)
	if err != nil {
		return 0, err
	}
	log.Debugf("delete unpaid %d", amount)
	return paymentId, nil
}

// PayUnpaidFile. pay unpaid order for file
func (this *Dsp) PayUnpaidFile(fileHashStr string) error {
	// TODO: pay unpaid file at first
	return nil
}

// DownloadFileWithQuotation. download file, piece by piece from addrs.
// inOrder: if true, the file will be downloaded block by block in order
func (this *Dsp) DownloadFileWithQuotation(fileHashStr string, asset int32, inOrder, setFileName bool, quotation map[string]*file.Payment, decryptPwd string) *serr.SDKError {
	// task exist at runtime
	taskId := this.taskMgr.TaskId(fileHashStr, this.WalletAddress(), task.TaskTypeDownload)
	if len(quotation) == 0 {
		return serr.NewDetailError(serr.GET_DOWNLOAD_INFO_FAILED_FROM_PEERS, fmt.Sprintf("no peer quotation for download: %s", fileHashStr))
	}
	if !this.taskMgr.IsFileInfoExist(taskId) {
		return serr.NewDetailError(serr.FILEINFO_NOT_EXIST, fmt.Sprintf("download info not exist: %s", fileHashStr))
	}
	// pay unpaid order of the file after last download
	err := this.PayUnpaidFile(fileHashStr)
	if err != nil {
		return serr.NewDetailError(serr.PAY_UNPAID_BLOCK_FAILED, err.Error())
	}
	pause, sdkErr := this.checkIfPauseDownload(taskId, fileHashStr)
	if sdkErr != nil {
		return sdkErr
	}
	if pause {
		return nil
	}
	// new download logic
	peerAddrWallet := make(map[string]string, 0)
	wg := sync.WaitGroup{}
	for addr, payInfo := range quotation {
		wg.Add(1)
		sessionId, err := this.taskMgr.GetSessionId(taskId, payInfo.WalletAddress)
		if err != nil {
			continue
		}
		peerAddrWallet[addr] = payInfo.WalletAddress
		go func(a string) {
			msg := message.NewFileDownload(sessionId, fileHashStr, this.WalletAddress(), asset)
			log.Debugf("broadcast file_download msg to %v", a)
			broadcastRet, err := client.P2pBroadcast([]string{a}, msg.ToProtoMsg(), true, nil, nil)
			log.Debugf("brocast file download msg %v err %v", broadcastRet, err)
			wg.Done()
		}(addr)
	}
	wg.Wait()
	blockHashes := this.taskMgr.FileBlockHashes(taskId)
	prefix, err := this.taskMgr.GetFilePrefix(taskId)
	if err != nil {
		return serr.NewDetailError(serr.GET_FILEINFO_FROM_DB_ERROR, err.Error())
	}
	log.Debugf("filehashstr:%v, blockhashes-len:%v, prefix:%v", fileHashStr, len(blockHashes), prefix)
	fileName, _ := this.taskMgr.GetFileName(taskId)
	fullFilePath, err := this.taskMgr.GetFilePath(taskId)
	if err != nil {
		return serr.NewDetailError(serr.GET_FILEINFO_FROM_DB_ERROR, err.Error())
	}
	if len(fullFilePath) == 0 {
		if setFileName {
			fullFilePath = utils.GetFileNameAtPath(this.Config.FsFileRoot+"/", fileHashStr, fileName)
			log.Debugf("get fullFilePath of id :%s, filehashstr %s, filename %s, taskDone: %t path %s", taskId, fileHashStr, fileName, fullFilePath)
		} else {
			fullFilePath = this.Config.FsFileRoot + "/" + fileHashStr
		}
		err = this.taskMgr.SetFilePath(taskId, fullFilePath)
		if err != nil {
			return serr.NewDetailError(serr.SET_FILEINFO_DB_ERROR, err.Error())
		}
	}
	err = this.taskMgr.BatchSetFileInfo(taskId, nil, nil, nil, uint64(len(blockHashes)))
	if err != nil {
		return serr.NewDetailError(serr.SET_FILEINFO_DB_ERROR, err.Error())
	}
	pause, sdkErr = this.checkIfPauseDownload(taskId, fileHashStr)
	if sdkErr != nil {
		return sdkErr
	}
	if pause {
		return nil
	}
	this.taskMgr.EmitProgress(taskId, task.TaskDownloadFileDownloading)
	// declare job for workers
	job := func(tId, fHash, bHash, pAddr, walletAddr string, index int32) (*task.BlockResp, error) {
		resp, err := this.downloadBlock(tId, fHash, bHash, index, pAddr, walletAddr, 1)
		if err != nil {
			return nil, err
		}
		if resp == nil {
			return nil, fmt.Errorf("download block is nil %s-%s-%d from %s %s, err %s", fHash, bHash, index, pAddr, walletAddr, err)
		}
		log.Debugf("job done: download tId  %s-%s-%d %s-%d from %s %s", fHash, bHash, index, resp.Hash, resp.Index, pAddr, walletAddr)
		payInfo := quotation[pAddr]
		paymentId, err := this.PayForBlock(payInfo, pAddr, fHash, uint64(len(resp.Block)))
		log.Debugf("pay for block: %s-%s-%d to %s, wallet: %s success, paymentId: %d", fHash, bHash, index, pAddr, walletAddr, paymentId)
		if err != nil {
			log.Errorf("pay for block %s err %s", resp.Hash, err)
			return nil, err
		}
		return resp, nil
	}
	this.taskMgr.NewWorkers(taskId, peerAddrWallet, inOrder, job)
	go this.taskMgr.WorkBackground(taskId)
	log.Debugf("start download file")
	if inOrder {
		sdkErr = this.receiveBlockInOrder(taskId, fileHashStr, fullFilePath, prefix, peerAddrWallet, asset)
		if sdkErr != nil {
			return sdkErr
		}
		if len(decryptPwd) == 0 {
			return nil
		}
		sdkErr = this.decryptDownloadedFile(fullFilePath, decryptPwd)
		if sdkErr != nil {
			return sdkErr
		}
		return nil
	}
	// TODO: support out-of-order download
	return nil
}

// DeleteDownloadedFile. Delete downloaded file in local.
func (this *Dsp) DeleteDownloadedFile(taskId string) error {
	if len(taskId) == 0 {
		return errors.New("delete taskId is empty")
	}
	filePath, _ := this.taskMgr.GetFilePath(taskId)
	fileHashStr := this.taskMgr.TaskFileHash(taskId)
	err := this.Fs.DeleteFile(fileHashStr, filePath)
	if err != nil {
		log.Errorf("fs delete file: %s, path: %s, err: %s", fileHashStr, filePath, err)
		// return err
	}
	log.Debugf("delete local file success fileHash:%s, path:%s", fileHashStr, filePath)
	return this.taskMgr.DeleteTask(taskId, true)
}

// StartBackupFileService. start a backup file service to find backup jobs.
func (this *Dsp) StartBackupFileService() {
	backupingCnt := 0
	ticker := time.NewTicker(time.Duration(common.BACKUP_FILE_DURATION) * time.Second)
	backupFailedMap := make(map[string]int)
	for {
		select {
		case <-ticker.C:
			if this.Chain == nil {
				break
			}
			tasks, err := this.Chain.Native.Fs.GetExpiredProveList()
			if err != nil || tasks == nil || len(tasks.Tasks) == 0 {
				break
			}
			if backupingCnt > 0 {
				log.Debugf("doing backup jobs %d", backupingCnt)
				break
			}
			// TODO: optimize with parallel download
			for _, t := range tasks.Tasks {
				addrCheckFailed := (t.LuckyAddr.ToBase58() != this.WalletAddress()) ||
					(t.BackUpAddr.ToBase58() == this.WalletAddress()) ||
					(t.BrokenAddr.ToBase58() == this.WalletAddress()) ||
					(t.BrokenAddr.ToBase58() == t.BackUpAddr.ToBase58())
				if addrCheckFailed {
					// log.Debugf("address check faield file %s, lucky: %s, backup: %s, broken: %s", string(t.FileHash), t.LuckyAddr.ToBase58(), t.BackUpAddr.ToBase58(), t.BrokenAddr.ToBase58())
					continue
				}
				if len(t.FileHash) == 0 || len(t.BakSrvAddr) == 0 || len(t.BackUpAddr.ToBase58()) == 0 {
					// log.Debugf("get expired prove list params invalid:%v", t)
					continue
				}
				if v, ok := backupFailedMap[string(t.FileHash)]; ok && v >= common.MAX_BACKUP_FILE_FAILED {
					log.Debugf("skip backup this file, because has failed")
					continue
				}
				backupingCnt++
				log.Debugf("go backup file:%s, from:%s %s", t.FileHash, t.BakSrvAddr, t.BackUpAddr.ToBase58())
				// TODO: test back up logic
				serr := this.downloadFileFromPeers("", string(t.FileHash), common.ASSET_USDT, true, "", false, false, common.MAX_DOWNLOAD_PEERS_NUM, []string{string(t.BakSrvAddr)})
				if serr != nil {
					log.Errorf("download file err %s", serr)
					backupFailedMap[string(t.FileHash)] = backupFailedMap[string(t.FileHash)] + 1
					continue
				}
				err := this.Fs.PinRoot(context.TODO(), string(t.FileHash))
				if err != nil {
					log.Errorf("pin root file err %s", err)
					continue
				}
				log.Debugf("prove file %s, luckyNum %d, bakHeight:%d, bakNum:%d", string(t.FileHash), t.LuckyNum, t.BakHeight, t.BakNum)
				err = this.Fs.StartPDPVerify(string(t.FileHash), t.LuckyNum, t.BakHeight, t.BakNum, t.BrokenAddr)
				if err != nil {
					log.Errorf("pdp verify error for backup task")
					continue
				}
				log.Debugf("backup file:%s success", t.FileHash)
			}
			// reset
			backupingCnt = 0
		}
	}
}

// AllDownloadFiles. get all downloaded file names
func (this *Dsp) AllDownloadFiles() ([]*store.FileInfo, []string, error) {
	return this.taskMgr.AllDownloadFiles()
}

func (this *Dsp) DownloadedFileInfo(fileHashStr string) (*store.FileInfo, error) {
	// my task. use my wallet address
	fileInfoKey := this.taskMgr.TaskId(fileHashStr, this.WalletAddress(), task.TaskTypeDownload)
	return this.taskMgr.GetFileInfo(fileInfoKey)
}

func (this *Dsp) checkIfResumeDownload(taskId string) error {
	opt, err := this.taskMgr.GetFileDownloadOptions(taskId)
	if err != nil {
		return err
	}
	if opt == nil {
		return errors.New("can't find download options, please retry")
	}
	fileHashStr := this.taskMgr.TaskFileHash(taskId)
	if len(fileHashStr) == 0 {
		return fmt.Errorf("filehash not found %s", taskId)
	}
	log.Debugf("resume download file")
	// TODO: record original workers
	go this.DownloadFile(taskId, fileHashStr, opt)
	return nil
}

func (this *Dsp) receiveBlockInOrder(taskId, fileHashStr, fullFilePath, prefix string, peerAddrWallet map[string]string, asset int32) *serr.SDKError {
	blockIndex, err := this.addUndownloadReq(taskId, fileHashStr)
	if err != nil {
		return serr.NewDetailError(serr.GET_UNDOWNLOAD_BLOCK_FAILED, err.Error())
	}
	hasCutPrefix := false
	var file *os.File
	if this.Config.FsType == config.FS_FILESTORE {
		file, err = createDownloadFile(this.Config.FsFileRoot, fullFilePath)
		if err != nil {
			return serr.NewDetailError(serr.CREATE_DOWNLOAD_FILE_FAILED, err.Error())
		}
		defer func() {
			log.Debugf("close file defer")
			file.Close()
		}()
	}
	timeout := time.NewTimer(time.Duration(common.DOWNLOAD_FILE_TIMEOUT) * time.Second)
	stateChange := this.taskMgr.TaskStateChange(taskId)
	for {
		select {
		case value, ok := <-this.taskMgr.TaskNotify(taskId):
			timeout.Reset(time.Duration(common.DOWNLOAD_FILE_TIMEOUT) * time.Second)
			if !ok {
				return serr.NewDetailError(serr.DOWNLOAD_BLOCK_FAILED, "download internal error")
			}
			pause, sdkErr := this.checkIfPauseDownload(taskId, fileHashStr)
			if sdkErr != nil {
				return sdkErr
			}
			if pause {
				return nil
			}
			log.Debugf("received block %s-%s-%d from %s", fileHashStr, value.Hash, value.Index, value.PeerAddr)
			if this.taskMgr.IsBlockDownloaded(taskId, value.Hash, uint32(value.Index)) {
				log.Debugf("%s-%s-%d is downloaded", fileHashStr, value.Hash, value.Index)
				continue
			}
			block := this.Fs.EncodedToBlockWithCid(value.Block, value.Hash)
			links, err := this.Fs.GetBlockLinks(block)
			if err != nil {
				return serr.NewDetailError(serr.GET_FILEINFO_FROM_DB_ERROR, err.Error())
			}
			if block.Cid().String() == fileHashStr && this.Config.FsType == config.FS_FILESTORE {
				log.Debugf("set file prefix %s %s", fullFilePath, prefix)
				this.Fs.SetFsFilePrefix(fullFilePath, prefix)
			}
			if len(links) == 0 && this.Config.FsType == config.FS_FILESTORE {
				data := this.Fs.BlockData(block)
				// TEST: performance
				fileStat, err := file.Stat()
				if err != nil {
					return serr.NewDetailError(serr.GET_FILE_STATE_ERROR, err.Error())
				}
				// cut prefix
				// TEST: why not use filesize == 0
				if !hasCutPrefix && len(data) >= len(prefix) && string(data[:len(prefix)]) == prefix {
					log.Debugf("cut prefix data-len %d, prefix %s, prefix-len: %d, str %s", len(data), prefix, len(prefix), string(data[:len(prefix)]))
					data = data[len(prefix):]
					hasCutPrefix = true
				}
				// TEST: offset
				writeAtPos := int64(0)
				if value.Offset > 0 {
					writeAtPos = value.Offset - int64(len(prefix))
				}
				log.Debugf("block %s filesize %d, block-len %d, offset %v prefix %v pos %d", block.Cid().String(), fileStat.Size(), len(data), value.Offset, len(prefix), writeAtPos)
				_, err = file.WriteAt(data, writeAtPos)
				fileStat2, _ := file.Stat()
				log.Debugf("after write size %v, file %v", fileStat2.Size(), file)
				if err != nil {
					return serr.NewDetailError(serr.WRITE_FILE_DATA_FAILED, err.Error())
				}
			}
			if this.Config.FsType == config.FS_FILESTORE {
				err = this.Fs.PutBlockForFileStore(fullFilePath, block, uint64(value.Offset))
				log.Debugf("put block for store err %v", err)
			} else {
				err = this.Fs.PutBlock(block)
				if err != nil {
					return serr.NewDetailError(serr.FS_PUT_BLOCK_FAILED, err.Error())
				}
				log.Debugf("block %s value.index %d, value.tag:%d", block.Cid(), value.Index, len(value.Tag))
				err = this.Fs.PutTag(block.Cid().String(), fileHashStr, uint64(value.Index), value.Tag)
			}
			log.Debugf("put block for file %s block: %s, offset:%d", fullFilePath, block.Cid(), value.Offset)
			if err != nil {
				log.Errorf("put block err %s", err)
				return serr.NewDetailError(serr.FS_PUT_BLOCK_FAILED, err.Error())
			}
			err = this.taskMgr.SetBlockDownloaded(taskId, value.Hash, value.PeerAddr, uint32(value.Index), value.Offset, links)
			if err != nil {
				return serr.NewDetailError(serr.SET_FILEINFO_DB_ERROR, err.Error())
			}
			this.taskMgr.EmitProgress(taskId, task.TaskDownloadFileDownloading)
			log.Debugf("%s-%s-%d set downloaded", fileHashStr, value.Hash, value.Index)
			for _, l := range links {
				blockIndex++
				err := this.taskMgr.AddBlockReq(taskId, l, blockIndex)
				if err != nil {
					return serr.NewDetailError(serr.ADD_GET_BLOCK_REQUEST_FAILED, err.Error())
				}
			}
			if len(links) != 0 {
				continue
			}
			// find a more accurate way
			if value.Index != blockIndex || !this.taskMgr.IsFileDownloaded(taskId) {
				log.Debugf("continue value.Index %d, is %t", value.Index, this.taskMgr.IsFileDownloaded(taskId))
				continue
			}
			log.Debugf("IsFileDownloaded last block %t", this.taskMgr.IsFileDownloaded(taskId))
			// last block
			this.taskMgr.SetTaskState(taskId, task.TaskStateDone)
			break
		case newState := <-stateChange:
			switch newState {
			case task.TaskStatePause, task.TaskStateCancel:
				log.Debugf("download break when receive state change of %d", newState)
				return nil
			}
		case <-timeout.C:
			this.taskMgr.SetTaskState(taskId, task.TaskStateFailed)
			workerState := this.taskMgr.GetTaskWorkerState(taskId)
			for addr, state := range workerState {
				log.Debugf("download timeout worker addr: %s, working : %t, unpaid: %t, totalFailed %v", addr, state.Working, state.Unpaid, state.TotalFailed)
			}
			return serr.NewDetailError(serr.DOWNLOAD_FILE_TIMEOUT, "Download file timeout")
		}
		done, err := this.taskMgr.IsTaskDone(taskId)
		if err != nil {
			return serr.NewDetailError(serr.SET_FILEINFO_DB_ERROR, err.Error())
		}
		if done {
			log.Debugf("download task is done")
			break
		}
	}
	this.taskMgr.EmitProgress(taskId, task.TaskDownloadFileMakeSeed)
	go this.PushToTrackers(fileHashStr, this.DNS.TrackerUrls, client.P2pGetPublicAddr())
	for addr, walletAddr := range peerAddrWallet {
		sessionId, err := this.taskMgr.GetSessionId(taskId, walletAddr)
		if err != nil {
			continue
		}
		go func(a, w, sid string) {
			fileDownloadOkMsg := message.NewFileDownloadOk(sid, fileHashStr, this.WalletAddress(), asset)
			client.P2pBroadcast([]string{a}, fileDownloadOkMsg.ToProtoMsg(), true, nil, nil)
		}(addr, walletAddr, sessionId)
	}
	return nil
}

func (this *Dsp) decryptDownloadedFile(fullFilePath, decryptPwd string) *serr.SDKError {
	if len(decryptPwd) == 0 {
		return serr.NewDetailError(serr.DECRYPT_FILE_FAILED, "no decrypt password")
	}
	err := this.Fs.AESDecryptFile(fullFilePath, decryptPwd, fullFilePath+"-decrypted")
	if err != nil {
		return serr.NewDetailError(serr.DECRYPT_FILE_FAILED, err.Error())
	}
	err = os.Rename(fullFilePath+"-decrypted", fullFilePath)
	if err != nil {
		return serr.NewDetailError(serr.RENAME_FILED_FAILED, err.Error())
	}
	return nil
}

func (this *Dsp) addUndownloadReq(taskId, fileHashStr string) (int32, error) {
	hashes, indexMap, err := this.taskMgr.GetUndownloadedBlockInfo(taskId, fileHashStr)
	if err != nil {
		return 0, err
	}
	if len(hashes) == 0 {
		log.Debugf("no undownloaded block %s %v", hashes, indexMap)
		return 0, nil
	}
	// panic("Test")
	log.Debugf("start download at %s-%s-%d", fileHashStr, hashes[0], indexMap[hashes[0]])
	blockIndex := int32(0)
	for _, hash := range hashes {
		blockIndex = int32(indexMap[hash])
		err = this.taskMgr.AddBlockReq(taskId, hash, int32(blockIndex))
		if err != nil {
			return 0, err
		}
	}
	return blockIndex, nil
}

// downloadFileFromPeers. downloadfile base methods. download file from peers.
func (this *Dsp) downloadFileFromPeers(taskId, fileHashStr string, asset int32, inOrder bool, decryptPwd string, free, setFileName bool, maxPeerCnt int, addrs []string) *serr.SDKError {
	quotation, err := this.GetDownloadQuotation(fileHashStr, asset, free, addrs)
	log.Debugf("downloadFileFromPeers :%v", quotation)
	if err != nil {
		return serr.NewDetailError(serr.GET_DOWNLOAD_INFO_FAILED_FROM_PEERS, err.Error())
	}
	if len(quotation) == 0 {
		return serr.NewDetailError(serr.NO_DOWNLOAD_SEED, "no quotation from peers")
	}
	pause, sdkErr := this.checkIfPauseDownload(taskId, fileHashStr)
	if sdkErr != nil {
		return sdkErr
	}
	if pause {
		return nil
	}
	if !free && len(addrs) > maxPeerCnt {
		log.Debugf("filter peers free %t len %d max %d", free, len(addrs), maxPeerCnt)
		// filter peers
		quotation = utils.SortPeersByPrice(quotation, maxPeerCnt)
	}
	err = this.DepositChannelForFile(fileHashStr, quotation)
	if err != nil {
		return serr.NewDetailError(serr.PREPARE_CHANNEL_ERROR, err.Error())
	}
	pause, sdkErr = this.checkIfPauseDownload(taskId, fileHashStr)
	if sdkErr != nil {
		return sdkErr
	}
	if pause {
		return nil
	}
	log.Debugf("set up channel success: %v", quotation)
	sdkErr = this.DownloadFileWithQuotation(fileHashStr, asset, inOrder, setFileName, quotation, decryptPwd)
	log.Debugf("DownloadFileWithQuotation err %v", sdkErr)
	return sdkErr
}

// startFetchBlocks for store node, fetch blocks one by one after receive fetch_rdy msg
func (this *Dsp) startFetchBlocks(fileHashStr string, addr, walletAddr string) error {
	// my task. use my wallet address
	log.Debugf("startFetchBlocks: %s", fileHashStr)
	taskId := this.taskMgr.TaskId(fileHashStr, this.WalletAddress(), task.TaskTypeDownload)
	blockHashes := this.taskMgr.FileBlockHashes(taskId)
	if len(blockHashes) == 0 {
		log.Errorf("block hashes is empty for file :%s", fileHashStr)
		return errors.New("block hashes is empty")
	}
	err := client.P2pConnect(addr)
	if err != nil {
		return err
	}
	pause, cancel := false, false
	defer func() {
		if cancel {
			return
		}
		err := this.Fs.PinRoot(context.TODO(), fileHashStr)
		if err != nil {
			log.Errorf("pin root file err %s", err)
		}
	}()
	// TODO: optimize this with one hash once time
	for index, hash := range blockHashes {
		pause, cancel, err = this.taskMgr.IsTaskPauseOrCancel(taskId)
		if err != nil {
			return err
		}
		if pause || cancel {
			log.Debugf("fetch task break it pause: %t, cancel: %t", pause, cancel)
			break
		}
		if this.taskMgr.IsBlockDownloaded(taskId, hash, uint32(index)) {
			continue
		}
		value, err := this.downloadBlock(taskId, fileHashStr, hash, int32(index), addr, walletAddr, common.MAX_BLOCK_FETCHED_RETRY)
		if err != nil {
			return err
		}
		err = this.Fs.PutBlock(this.Fs.EncodedToBlockWithCid(value.Block, hash))
		if err != nil {
			return err
		}
		err = this.Fs.PutTag(hash, fileHashStr, uint64(value.Index), value.Tag)
		if err != nil {
			return err
		}
		err = this.taskMgr.SetBlockDownloaded(taskId, value.Hash, value.PeerAddr, uint32(index), value.Offset, nil)
		log.Debugf("SetBlockDownloaded taskId:%s, %s-%s-%d-%d, err: %s", taskId, fileHashStr, value.Hash, index, value.Offset, err)
		if err != nil {
			return err
		}
	}
	if !this.taskMgr.IsFileDownloaded(taskId) {
		return errors.New("break here, but file not be stored")
	}
	log.Infof("received all block, start pdp verify")
	// all block is saved, prove it
	err = this.Fs.StartPDPVerify(fileHashStr, 0, 0, 0, chainCom.ADDRESS_EMPTY)
	if err != nil {
		log.Errorf("start pdp verify err %s", err)
		return err
	}
	this.PushToTrackers(fileHashStr, this.DNS.TrackerUrls, client.P2pGetPublicAddr())
	// TODO: remove unused file info fields after prove pdp success
	this.taskMgr.SetTaskState(taskId, task.TaskStateDone)
	this.taskMgr.DeleteTask(taskId, false)
	doneMsg := message.NewFileFetchDone(taskId, fileHashStr)
	client.P2pSend(addr, doneMsg.ToProtoMsg())
	log.Debugf("fetch file done, send done msg to %s", addr)
	return nil
}

// downloadBlock. download block helper function.
func (this *Dsp) downloadBlock(taskId, fileHashStr, hash string, index int32, addr, peerWalletAddr string, retry uint32) (*task.BlockResp, error) {
	sessionId, err := this.taskMgr.GetSessionId(taskId, peerWalletAddr)
	if err != nil {
		return nil, err
	}
	walletAddress := this.WalletAddress()
	// TODO: refactor to request - reply model
	log.Debugf("download block of task: %s %s-%s-%d to %s", taskId, fileHashStr, hash, index, addr)
	ch := this.taskMgr.NewBlockRespCh(taskId, sessionId, hash, index)
	defer func() {
		log.Debugf("drop block resp channel: %s-%s-%s-%d", taskId, sessionId, hash, index)
		this.taskMgr.DropBlockRespCh(taskId, sessionId, hash, index)
	}()
	msg := message.NewBlockReqMsg(sessionId, fileHashStr, hash, index, walletAddress, common.ASSET_USDT)
	var block *task.BlockResp
	for i := uint32(0); i < retry; i++ {
		err = client.P2pSend(addr, msg.ToProtoMsg())
		log.Debugf("send download block msg sessionId %s of %s-%s-%d to %s, err %s, retry: %d", sessionId, fileHashStr, hash, index, addr, err, i)
		if err != nil {
			continue
		}
		received := false
		timeout := false
		select {
		case value, ok := <-ch:
			received = true
			if !ok {
				err = fmt.Errorf("receiving block none from channel")
				continue
			}
			if timeout {
				err = fmt.Errorf("receiving block %s timeout", hash)
				continue
			}
			block = value
			err = nil
			return value, nil
		case <-time.After(time.Duration(common.BLOCK_FETCH_TIMEOUT) * time.Second):
			if received {
				break
			}
			timeout = true
			err = fmt.Errorf("receiving block %s timeout", hash)
			continue
		}
	}
	return block, err
}

func (this *Dsp) checkIfPauseDownload(taskId, fileHashStr string) (bool, *serr.SDKError) {
	pause, err := this.taskMgr.IsTaskPause(taskId)
	if err != nil {
		sdkerr := serr.NewDetailError(serr.TASK_PAUSE_ERROR, err.Error())
		return false, sdkerr
	}
	if !pause {
		return false, nil
	}
	if len(fileHashStr) == 0 {
		return pause, nil
	}
	return pause, nil
}

// shareUploadedFile. share uploaded file when upload success
func (this *Dsp) shareUploadedFile(filePath, fileName, prefix string, hashes []string) error {
	log.Debugf("shareUploadedFile path: %s filename:%s prefix:%s hashes:%d", filePath, fileName, prefix, len(hashes))
	if this.Config.FsType != config.FS_FILESTORE {
		return errors.New("fs type is not file store")
	}
	if len(hashes) == 0 {
		return errors.New("no block hashes")
	}
	fileHashStr := hashes[0]
	taskId := this.taskMgr.TaskId(fileHashStr, this.WalletAddress(), task.TaskTypeDownload)
	if len(taskId) == 0 {
		var err error
		taskId, err = this.taskMgr.NewTask(task.TaskTypeDownload)
		if err != nil {
			return err
		}
		err = this.taskMgr.AddFileBlockHashes(taskId, hashes)
		if err != nil {
			return err
		}
		err = this.taskMgr.BatchSetFileInfo(taskId, nil, prefix, nil, nil)
		if err != nil {
			return err
		}
		fullFilePath := utils.GetFileNameAtPath(this.Config.FsFileRoot+"/", fileHashStr, fileName)
		err = this.taskMgr.SetFilePath(taskId, fullFilePath)
		if err != nil {
			return err
		}
	}
	fullFilePath, err := this.taskMgr.GetFilePath(taskId)
	if err != nil {
		return err
	}
	input, err := os.Open(filePath)
	if err != nil {
		return err
	}
	output, err := os.OpenFile(fullFilePath, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0666)
	if err != nil {
		return err
	}
	defer output.Close()
	n, err := io.Copy(output, input)
	log.Debugf("copy %d bytes", n)
	if err != nil {
		return err
	}
	this.Fs.SetFsFilePrefix(fullFilePath, prefix)
	offsets, err := this.Fs.GetAllOffsets(fileHashStr)
	if err != nil {
		return err
	}
	for index, hash := range hashes {
		if this.taskMgr.IsBlockDownloaded(taskId, hash, uint32(index)) {
			log.Debugf("%s-%s-%d is downloaded", fileHashStr, hash, index)
			continue
		}
		block := this.Fs.GetBlock(hash)
		links, err := this.Fs.GetBlockLinks(block)
		if err != nil {
			return err
		}
		offset := offsets[hash]
		log.Debugf("hash: %s-%s-%d , offset: %d, links count %d", fileHashStr, hash, index, offset, len(links))
		err = this.taskMgr.SetBlockDownloaded(taskId, fileHashStr, client.P2pGetPublicAddr(), uint32(index), int64(offset), links)
		if err != nil {
			return err
		}
	}
	go this.PushToTrackers(fileHashStr, this.DNS.TrackerUrls, client.P2pGetPublicAddr())
	return nil
}

// createDownloadFile. create file handler for write downloading file
func createDownloadFile(dir, filePath string) (*os.File, error) {
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		err = os.MkdirAll(dir, 0755)
		if err != nil {
			return nil, err
		}
	}
	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0666)
	log.Debugf("create download file %s %v %s", filePath, file, err)
	if err != nil {
		return nil, err
	}
	return file, nil
}

// removeTempFile. remove the temp download file
func removeTempFile(fileName string) {
	// os.Remove(common.DOWNLOAD_FILE_TEMP_DIR_PATH + "/" + fileName)
}
