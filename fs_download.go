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
	"github.com/saveio/dsp-go-sdk/network/message/types/block"
	"github.com/saveio/dsp-go-sdk/network/message/types/file"
	"github.com/saveio/dsp-go-sdk/network/message/types/payment"
	"github.com/saveio/dsp-go-sdk/store"
	"github.com/saveio/dsp-go-sdk/task"
	"github.com/saveio/dsp-go-sdk/utils"
	chainCom "github.com/saveio/themis/common"
	"github.com/saveio/themis/common/log"
)

// IsFileEncrypted. read prefix of file to check it's encrypted or not
func (this *Dsp) IsFileEncrypted(fullFilePath string) bool {
	sourceFile, err := os.Open(fullFilePath)
	if err != nil {
		return false
	}
	defer sourceFile.Close()
	prefix := make([]byte, utils.PREFIX_LEN)
	_, err = sourceFile.Read(prefix)
	if err != nil {
		return false
	}
	filePrefix := &utils.FilePrefix{}
	filePrefix.Deserialize([]byte(prefix))
	return filePrefix.Encrypt
}

// DownloadFile. download file, piece by piece from addrs.
// inOrder: if true, the file will be downloaded block by block in order
// free: if true, query nodes who can share file free
// maxPeeCnt: download with max number peers who provide file at the less price
func (this *Dsp) DownloadFile(taskId, fileHashStr string, opt *common.DownloadOption) error {
	// start a task
	var sdkErr *serr.SDKError
	var err error
	if len(taskId) == 0 {
		taskId, err = this.taskMgr.NewTask(store.TaskTypeDownload)
	}
	log.Debugf("download file id: %s, fileHash: %s, option: %v", taskId, fileHashStr, opt)
	defer func() {
		if err != nil {
			log.Errorf("download file %s err %s", fileHashStr, err)
		}
		log.Debugf("emit ret %s %s", taskId, err)
		if sdkErr != nil {
			this.taskMgr.EmitResult(taskId, nil, sdkErr)
		} else {
			this.taskMgr.EmitResult(taskId, "", sdkErr)
		}
		// delete task from cache in the end
		if this.taskMgr.IsFileDownloaded(taskId) && sdkErr == nil {
			this.taskMgr.DeleteTask(taskId)
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
	this.taskMgr.NewBatchSet(taskId)
	this.taskMgr.SetFileHash(taskId, fileHashStr)
	this.taskMgr.SetFileName(taskId, opt.FileName)
	this.taskMgr.SetFileOwner(taskId, opt.FileOwner)
	this.taskMgr.SetUrl(taskId, opt.Url)
	this.taskMgr.SetWalletAddr(taskId, this.WalletAddress())
	err = this.taskMgr.BatchCommit(taskId)
	if err != nil {
		sdkErr = serr.NewDetailError(serr.SET_FILEINFO_DB_ERROR, err.Error())
		return err
	}
	err = this.taskMgr.SetFileDownloadOptions(taskId, opt)
	if err != nil {
		sdkErr = serr.NewDetailError(serr.SET_FILEINFO_DB_ERROR, err.Error())
		return err
	}
	err = this.taskMgr.BindTaskId(taskId)
	if err != nil {
		sdkErr = serr.NewDetailError(serr.SET_FILEINFO_DB_ERROR, err.Error())
		return err
	}
	this.taskMgr.EmitProgress(taskId, task.TaskDownloadFileStart)
	stop, sdkErr := this.stopDownload(taskId)
	if sdkErr != nil {
		return sdkErr.Error
	}
	if stop {
		return nil
	}
	this.taskMgr.EmitProgress(taskId, task.TaskDownloadSearchPeers)
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
	stop, sdkErr = this.stopDownload(taskId)
	if sdkErr != nil {
		return sdkErr.Error
	}
	if stop {
		return nil
	}

	sdkErr = this.downloadFileFromPeers(taskId, fileHashStr, opt.Asset, opt.InOrder, opt.DecryptPwd, opt.Free, opt.SetFileName, opt.MaxPeerCnt, addrs)
	if sdkErr != nil {
		err = errors.New(sdkErr.Error.Error())
	}
	return err
}

func (this *Dsp) PauseDownload(taskId string) error {
	taskType, _ := this.taskMgr.TaskType(taskId)
	if taskType != store.TaskTypeDownload {
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
	taskType, _ := this.taskMgr.TaskType(taskId)
	if taskType != store.TaskTypeDownload {
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
	taskType, _ := this.taskMgr.TaskType(taskId)
	if taskType != store.TaskTypeDownload {
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
	taskType, _ := this.taskMgr.TaskType(taskId)
	if taskType != store.TaskTypeDownload {
		return fmt.Errorf("task %s is not a download task", taskId)
	}
	cancel, err := this.taskMgr.IsTaskCancel(taskId)
	if err != nil {
		return err
	}
	if cancel {
		return fmt.Errorf("task is cancelling: %s", taskId)
	}
	fileHashStr, _ := this.taskMgr.TaskFileHash(taskId)
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
	// TODO: broadcast download cancel msg, need optimize
	wg := sync.WaitGroup{}
	for hostAddr, session := range sessions {
		wg.Add(1)
		go func(a string, ses *store.Session) {
			msg := message.NewFileDownloadCancel(ses.SessionId, fileHashStr, this.WalletAddress(), int32(ses.Asset))
			client.P2pRequestWithRetry(msg.ToProtoMsg(), a, common.MAX_NETWORK_REQUEST_RETRY, common.P2P_REQUEST_TIMEOUT*common.MAX_NETWORK_REQUEST_RETRY)
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
	fileOwner := linkvalues[common.FILE_LINK_OWNER_KEY]
	if maxPeerCnt > common.MAX_PEERCNT_FOR_DOWNLOAD {
		maxPeerCnt = common.MAX_PEERCNT_FOR_DOWNLOAD
	}
	opt := &common.DownloadOption{
		FileName:    fileName,
		Asset:       asset,
		InOrder:     inOrder,
		DecryptPwd:  decryptPwd,
		Free:        free,
		SetFileName: setFileName,
		FileOwner:   fileOwner,
		MaxPeerCnt:  maxPeerCnt,
	}
	taskId := this.taskMgr.TaskId(fileHashStr, this.WalletAddress(), store.TaskTypeDownload)
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
	fileOwner := linkvalues[common.FILE_LINK_OWNER_KEY]
	if maxPeerCnt > common.MAX_PEERCNT_FOR_DOWNLOAD {
		maxPeerCnt = common.MAX_PEERCNT_FOR_DOWNLOAD
	}
	opt := &common.DownloadOption{
		FileName:    fileName,
		Asset:       asset,
		InOrder:     inOrder,
		DecryptPwd:  decryptPwd,
		Free:        free,
		SetFileName: setFileName,
		FileOwner:   fileOwner,
		MaxPeerCnt:  maxPeerCnt,
		Url:         url,
	}
	taskId := this.taskMgr.TaskId(fileHashStr, this.WalletAddress(), store.TaskTypeDownload)
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
	info, _ := this.Chain.Native.Fs.GetFileInfo(fileHashStr)
	var fileName, fileOwner string
	if info != nil {
		fileName = string(info.FileDesc)
		fileOwner = info.FileOwner.ToBase58()
	}
	if maxPeerCnt > common.MAX_PEERCNT_FOR_DOWNLOAD {
		maxPeerCnt = common.MAX_PEERCNT_FOR_DOWNLOAD
	}
	opt := &common.DownloadOption{
		FileName:    fileName,
		Asset:       asset,
		InOrder:     inOrder,
		DecryptPwd:  decryptPwd,
		Free:        free,
		SetFileName: setFileName,
		FileOwner:   fileOwner,
		MaxPeerCnt:  maxPeerCnt,
	}
	taskId := this.taskMgr.TaskId(fileHashStr, this.WalletAddress(), store.TaskTypeDownload)
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
func (this *Dsp) GetDownloadQuotation(fileHashStr, decryptPwd string, asset int32, free bool, addrs []string) (map[string]*file.Payment, error) {
	if len(addrs) == 0 {
		return nil, errors.New("no peer for download")
	}
	taskId := this.taskMgr.TaskId(fileHashStr, this.WalletAddress(), store.TaskTypeDownload)
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
	reply := func(msg proto.Message, addr string) bool {
		replyLock.Lock()
		defer replyLock.Unlock()
		p2pMsg := message.ReadMessage(msg)
		if p2pMsg.Error != nil {
			log.Errorf("get download ask err %s from %s", p2pMsg.Error.Message, addr)
			return false
		}
		fileMsg := p2pMsg.Payload.(*file.File)
		if len(fileMsg.BlockHashes) < len(blockHashes) {
			return false
		}
		if len(prefix) > 0 && string(fileMsg.Prefix) != prefix {
			return false
		}

		filePrefix := &utils.FilePrefix{}
		filePrefix.Deserialize([]byte(prefix))
		if len(decryptPwd) > 0 && !utils.VerifyEncryptPassword(decryptPwd, filePrefix.EncryptSalt, filePrefix.EncryptHash) {
			log.Warnf("encrypt password not match hash")
			return false
		}

		if free && (fileMsg.PayInfo != nil && fileMsg.PayInfo.UnitPrice != 0) {
			return false
		}
		blockHashes = fileMsg.BlockHashes
		peerPayInfos[addr] = fileMsg.PayInfo
		prefix = string(fileMsg.Prefix)
		log.Debugf("prefix hex: %s", fileMsg.Prefix)
		this.taskMgr.AddFileSession(taskId, fileMsg.SessionId, fileMsg.PayInfo.WalletAddress, addr, uint64(fileMsg.PayInfo.Asset), fileMsg.PayInfo.UnitPrice)
		return false
	}
	ret, err := client.P2pBroadcast(addrs, msg.ToProtoMsg(), true, reply)
	log.Debugf("broadcast file download msg result %v err %s", ret, err)
	if err != nil {
		return nil, err
	}
	log.Debugf("peer prices:%v", peerPayInfos)
	if len(peerPayInfos) == 0 {
		return nil, errors.New("remote peer has deleted the file")
	}
	totalCount, _ := this.taskMgr.GetFileTotalBlockCount(taskId)
	log.Debugf("get file total count :%d", totalCount)
	if uint64(len(blockHashes)) == totalCount {
		return peerPayInfos, nil
	}
	log.Debugf("AddFileBlockHashes id %v hashes %s-%s, prefix %s, len: %d", taskId, blockHashes[0], blockHashes[len(blockHashes)-1], prefix, len(prefix))
	err = this.taskMgr.AddFileBlockHashes(taskId, blockHashes)
	if err != nil {
		return nil, err
	}
	this.taskMgr.NewBatchSet(taskId)
	this.taskMgr.SetPrefix(taskId, prefix)
	this.taskMgr.SetTotalBlockCount(taskId, uint64(len(blockHashes)))
	err = this.taskMgr.BatchCommit(taskId)
	if err != nil {
		return nil, err
	}
	return peerPayInfos, nil
}

// DepositChannelForFile. deposit channel. peerPrices: peer network address <=> payment info
func (this *Dsp) DepositChannelForFile(fileHashStr string, peerPrices map[string]*file.Payment) error {
	taskId := this.taskMgr.TaskId(fileHashStr, this.WalletAddress(), store.TaskTypeDownload)
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
	totalCount, _ := this.taskMgr.GetFileTotalBlockCount(taskId)
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
	if payInfo.WalletAddress == this.WalletAddress() {
		return 0, fmt.Errorf("can't pay to self : %s", payInfo.WalletAddress)
	}
	if this.DNS == nil || this.DNS.DNSNode == nil {
		return 0, fmt.Errorf("no dns")
	}
	amount := blockSize * payInfo.UnitPrice
	if amount/blockSize != payInfo.UnitPrice {
		return 0, errors.New("total price overflow")
	}
	if amount == 0 {
		log.Warn("pay amount is 0")
		return 0, nil
	}
	taskId := this.taskMgr.TaskId(fileHashStr, this.WalletAddress(), store.TaskTypeDownload)
	err := this.taskMgr.AddFileUnpaid(taskId, payInfo.WalletAddress, payInfo.Asset, amount)
	if err != nil {
		return 0, err
	}
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	paymentId := r.Int31()
	dnsHostAddr, err := this.GetExternalIP(this.DNS.DNSNode.WalletAddr)
	if err != nil {
		return 0, err
	}
	exist, err := client.P2pConnectionExist(dnsHostAddr, client.P2pNetTypeChannel)
	if err != nil {
		log.Errorf("reconnect channel err %s %s", this.DNS.DNSNode.WalletAddr, err)
		return 0, err
	}
	if !exist {
		log.Debugf("DNS conncetion not exist reconnect...")
		err = client.P2pReconnectPeer(dnsHostAddr, client.P2pNetTypeChannel)
		if err != nil {
			return 0, err
		}
		err = client.P2pWaitForConnected(dnsHostAddr, time.Duration(common.WAIT_CHANNEL_CONNECT_TIMEOUT))
		if err != nil {
			return 0, err
		}
	}
	// targetHostAddr, err := this.GetExternalIP(payInfo.WalletAddress)
	// if err != nil {
	// 	return 0, err
	// }
	// err = client.P2pReconnectPeer(targetHostAddr, client.P2pNetTypeChannel)
	// if err != nil {
	// 	log.Errorf("reconnect channel err %s %s", payInfo.WalletAddress, err)
	// 	return 0, err
	// }
	log.Debugf("paying to %s, id %v, err:%s", payInfo.WalletAddress, paymentId, err)
	err = this.Channel.MediaTransfer(paymentId, amount, this.DNS.DNSNode.WalletAddr, payInfo.WalletAddress)
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
	_, err = client.P2pRequestWithRetry(msg.ToProtoMsg(), addr, common.MAX_NETWORK_REQUEST_RETRY, common.DOWNLOAD_FILE_TIMEOUT)
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
	taskId := this.taskMgr.TaskId(fileHashStr, this.WalletAddress(), store.TaskTypeDownload)
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
	stop, sdkErr := this.stopDownload(taskId)
	if sdkErr != nil {
		return sdkErr
	}
	if stop {
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
			broadcastRet, err := client.P2pBroadcast([]string{a}, msg.ToProtoMsg(), true, nil)
			log.Debugf("brocast file download msg %v err %v", broadcastRet, err)
			wg.Done()
		}(addr)
	}
	wg.Wait()
	blockHashes := this.taskMgr.FileBlockHashes(taskId)
	prefixBuf, err := this.taskMgr.GetFilePrefix(taskId)
	if err != nil {
		return serr.NewDetailError(serr.GET_FILEINFO_FROM_DB_ERROR, err.Error())
	}
	prefix := string(prefixBuf)
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
	err = this.taskMgr.SetTotalBlockCount(taskId, uint64(len(blockHashes)))
	if err != nil {
		return serr.NewDetailError(serr.SET_FILEINFO_DB_ERROR, err.Error())
	}
	stop, sdkErr = this.stopDownload(taskId)
	if sdkErr != nil {
		return sdkErr
	}
	if stop {
		return nil
	}
	this.taskMgr.EmitProgress(taskId, task.TaskDownloadFileDownloading)
	// declare job for workers
	job := func(tId, fHash, pAddr, walletAddr string, blocks []*block.Block) ([]*task.BlockResp, error) {
		this.taskMgr.EmitProgress(taskId, task.TaskDownloadRequestBlocks)
		resp, err := this.downloadBlockFlights(tId, fHash, pAddr, walletAddr, blocks, 1, common.DOWNLOAD_BLOCKFLIGHTS_TIMEOUT)
		if err != nil {
			return nil, err
		}
		if resp == nil {
			return nil, fmt.Errorf("download blocks is nil %s from %s %s, err %s", fHash, pAddr, walletAddr, err)
		}
		this.taskMgr.EmitProgress(taskId, task.TaskDownloadReceiveBlocks)
		payInfo := quotation[pAddr]
		var totalBytes int
		for _, v := range resp {
			totalBytes += len(v.Block)
		}
		if totalBytes == 0 {
			return nil, errors.New("request total bytes count 0")
		}
		this.taskMgr.EmitProgress(taskId, task.TaskDownloadPayForBlocks)
		paymentId, err := this.PayForBlock(payInfo, pAddr, fHash, uint64(totalBytes))
		log.Debugf("pay for block: %s to %s, wallet: %s success, paymentId: %d", fHash, pAddr, walletAddr, paymentId)
		if err != nil {
			log.Errorf("pay for blocks err %s", err)
			return nil, err
		}
		this.taskMgr.EmitProgress(taskId, task.TaskDownloadPayForBlocksDone)
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
	fileHashStr, _ := this.taskMgr.TaskFileHash(taskId)
	err := this.Fs.DeleteFile(fileHashStr, filePath)
	if err != nil {
		log.Errorf("fs delete file: %s, path: %s, err: %s", fileHashStr, filePath, err)
		// return err
	}
	log.Debugf("delete local file success fileHash:%s, path:%s", fileHashStr, filePath)
	return this.taskMgr.CleanTask(taskId)
}

// StartBackupFileService. start a backup file service to find backup jobs.
func (this *Dsp) StartBackupFileService() {
	backupingCnt := 0
	ticker := time.NewTicker(time.Duration(common.BACKUP_FILE_DURATION) * time.Second)
	backupFailedMap := make(map[string]int)
	for {
		select {
		case <-ticker.C:
			if this.stop {
				log.Debugf("stop backup file service")
				return
			}
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

// StartCheckRemoveFiles. check to remove files after prove PDP done
func (this *Dsp) StartCheckRemoveFiles() {
	log.Debugf("StartCheckRemoveFiles ")
	ticker := time.NewTicker(time.Duration(common.REMOVE_FILES_DURATION) * time.Second)
	for {
		select {
		case <-ticker.C:
			if this.stop {
				log.Debugf("stop check remove files service")
				return
			}
			files := this.Fs.RemovedExpiredFiles()
			if len(files) == 0 {
				continue
			}
			for _, f := range files {
				hash, ok := f.(string)
				if !ok {
					continue
				}
				taskId := this.taskMgr.TaskId(hash, this.WalletAddress(), store.TaskTypeDownload)
				log.Debugf("delete removed file %s %s", taskId, hash)
				this.DeleteDownloadedFile(taskId)
			}
		}
	}
}

// AllDownloadFiles. get all downloaded file names
func (this *Dsp) AllDownloadFiles() ([]*store.TaskInfo, []string, error) {
	return this.taskMgr.AllDownloadFiles()
}

func (this *Dsp) DownloadedFileInfo(fileHashStr string) (*store.TaskInfo, error) {
	// my task. use my wallet address
	fileInfoKey := this.taskMgr.TaskId(fileHashStr, this.WalletAddress(), store.TaskTypeDownload)
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
	fileHashStr, _ := this.taskMgr.TaskFileHash(taskId)
	if len(fileHashStr) == 0 {
		return fmt.Errorf("filehash not found %s", taskId)
	}
	log.Debugf("resume download file")
	// TODO: record original workers
	go this.DownloadFile(taskId, fileHashStr, opt)
	return nil
}

// receiveBlockInOrder. receive blocks in order
func (this *Dsp) receiveBlockInOrder(taskId, fileHashStr, fullFilePath, prefix string, peerAddrWallet map[string]string, asset int32) *serr.SDKError {
	blockIndex, err := this.addUndownloadReq(taskId, fileHashStr)
	if err != nil {
		return serr.NewDetailError(serr.GET_UNDOWNLOAD_BLOCK_FAILED, err.Error())
	}
	hasCutPrefix := false
	filePrefix := &utils.FilePrefix{}
	filePrefix.Deserialize([]byte(prefix))
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
	stateCheckTicker := time.NewTicker(time.Duration(common.TASK_STATE_CHECK_DURATION) * time.Second)
	for {
		select {
		case value, ok := <-this.taskMgr.TaskNotify(taskId):
			timeout.Reset(time.Duration(common.DOWNLOAD_FILE_TIMEOUT) * time.Second)
			if !ok {
				return serr.NewDetailError(serr.DOWNLOAD_BLOCK_FAILED, "download internal error")
			}
			stop, sdkErr := this.stopDownload(taskId)
			if sdkErr != nil {
				return sdkErr
			}
			if stop {
				log.Debugf("stop download task %s", taskId)
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
				if !filePrefix.Encrypt {
					this.Fs.SetFsFilePrefix(fullFilePath, prefix)
				} else {
					this.Fs.SetFsFilePrefix(fullFilePath, "")
				}
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
				if !filePrefix.Encrypt && !hasCutPrefix && len(data) >= len(prefix) && string(data[:len(prefix)]) == prefix {
					log.Debugf("cut prefix data-len %d, prefix %s, prefix-len: %d, str %s", len(data), prefix, len(prefix), string(data[:len(prefix)]))
					data = data[len(prefix):]
					hasCutPrefix = true
				}
				// TEST: offset
				writeAtPos := value.Offset
				if value.Offset > 0 && !filePrefix.Encrypt {
					writeAtPos = value.Offset - int64(len(prefix))
				}
				log.Debugf("block %s filesize %d, block-len %d, offset %v prefix %v pos %d", block.Cid().String(), fileStat.Size(), len(data), value.Offset, len(prefix), writeAtPos)
				_, err = file.WriteAt(data, writeAtPos)
				// fileStat2, _ := file.Stat()
				// log.Debugf("after write size %v, file %v", fileStat2.Size(), file)
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
		case <-stateCheckTicker.C:
			stop, sdkErr := this.stopDownload(taskId)
			if sdkErr != nil {
				return sdkErr
			}
			if stop {
				log.Debugf("stop download task %s", taskId)
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
			client.P2pBroadcast([]string{a}, fileDownloadOkMsg.ToProtoMsg(), true, nil)
		}(addr, walletAddr, sessionId)
	}
	return nil
}

func (this *Dsp) decryptDownloadedFile(fullFilePath, decryptPwd string) *serr.SDKError {
	if len(decryptPwd) == 0 {
		return serr.NewDetailError(serr.DECRYPT_FILE_FAILED, "no decrypt password")
	}
	filePrefix := &utils.FilePrefix{}
	sourceFile, err := os.Open(fullFilePath)
	if err != nil {
		return serr.NewDetailError(serr.DECRYPT_FILE_FAILED, err.Error())
	}
	defer sourceFile.Close()
	prefix := make([]byte, utils.PREFIX_LEN)
	_, err = sourceFile.Read(prefix)
	if err != nil {
		return serr.NewDetailError(serr.DECRYPT_FILE_FAILED, err.Error())
	}
	log.Debugf("read first n prefix :%v", prefix)
	filePrefix.Deserialize([]byte(prefix))
	if !utils.VerifyEncryptPassword(decryptPwd, filePrefix.EncryptSalt, filePrefix.EncryptHash) {
		return serr.NewDetailError(serr.DECRYPT_WRONG_PASSWORD, "wrong password")
	}
	err = this.Fs.AESDecryptFile(fullFilePath, string(prefix), decryptPwd, utils.GetDecryptedFilePath(fullFilePath))
	if err != nil {
		return serr.NewDetailError(serr.DECRYPT_FILE_FAILED, err.Error())
	}
	// err = os.Rename(fullFilePath+"-decrypted", fullFilePath)
	// if err != nil {
	// 	return serr.NewDetailError(serr.RENAME_FILED_FAILED, err.Error())
	// }
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
	quotation, err := this.GetDownloadQuotation(fileHashStr, decryptPwd, asset, free, addrs)
	log.Debugf("downloadFileFromPeers :%v", quotation)
	if err != nil {
		return serr.NewDetailError(serr.GET_DOWNLOAD_INFO_FAILED_FROM_PEERS, err.Error())
	}
	if len(quotation) == 0 {
		return serr.NewDetailError(serr.NO_DOWNLOAD_SEED, "no quotation from peers")
	}
	stop, sdkErr := this.stopDownload(taskId)
	if sdkErr != nil {
		return sdkErr
	}
	if stop {
		log.Debugf("stop download task %s", taskId)
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
	stop, sdkErr = this.stopDownload(taskId)
	if sdkErr != nil {
		return sdkErr
	}
	if stop {
		log.Debugf("stop download task %s", taskId)
		return nil
	}
	log.Debugf("set up channel success: %v", quotation)
	sdkErr = this.DownloadFileWithQuotation(fileHashStr, asset, inOrder, setFileName, quotation, decryptPwd)
	log.Debugf("DownloadFileWithQuotation err %v", sdkErr)
	return sdkErr
}

// startFetchBlocks for store node, fetch blocks one by one after receive fetch_rdy msg
func (this *Dsp) startFetchBlocks(fileHashStr string, addr, peerWalletAddr string) error {
	// my task. use my wallet address
	log.Debugf("startFetchBlocks: %s", fileHashStr)
	taskId := this.taskMgr.TaskId(fileHashStr, this.WalletAddress(), store.TaskTypeDownload)
	sessionId, err := this.taskMgr.GetSessionId(taskId, peerWalletAddr)
	if err != nil {
		return err
	}
	blockHashes := this.taskMgr.FileBlockHashes(taskId)
	if len(blockHashes) == 0 {
		log.Errorf("block hashes is empty for file :%s", fileHashStr)
		return errors.New("block hashes is empty")
	}
	err = client.P2pConnect(addr)
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
	downloadQoS := make([]int64, 0)
	blocks := make([]*block.Block, 0, common.MAX_REQ_BLOCK_COUNT)
	downloadBlkCap := common.MIN_REQ_BLOCK_COUNT
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
			log.Debugf("block has downloaded %s %d", hash, index)
			continue
		}
		blocks = append(blocks, &block.Block{
			SessionId: sessionId,
			Index:     int32(index),
			FileHash:  fileHashStr,
			Hash:      hash,
			Operation: netcom.BLOCK_OP_GET,
			Payment: &payment.Payment{
				Sender: this.WalletAddress(),
				Asset:  common.ASSET_USDT,
			},
		})
		if index != len(blockHashes)-1 && len(blocks) < downloadBlkCap {
			continue
		}
		var resps []*task.BlockResp
		startDownload := time.Now().Unix()
		resps, err = this.downloadBlockFlights(taskId, fileHashStr, addr, peerWalletAddr, blocks, common.MAX_BLOCK_FETCHED_RETRY, common.DOWNLOAD_FILE_TIMEOUT)
		if time.Now().Unix() <= startDownload {
			downloadQoS = append(downloadQoS, 0)
		} else {
			downloadQoS = append(downloadQoS, time.Now().Unix()-startDownload)
		}

		if err != nil {
			return err
		}
		if len(downloadQoS) >= common.MIN_DOWNLOAD_QOS_LEN {
			// reduce slice
			downloadQoS = downloadQoS[len(downloadQoS)-common.MIN_DOWNLOAD_QOS_LEN:]
			downloadBlkCap = adjustDownloadCap(downloadBlkCap, downloadQoS)
			log.Debugf("adjust new download cap: %d", downloadBlkCap)
		}
		blocks = blocks[:0]

		for _, value := range resps {
			blk := this.Fs.EncodedToBlockWithCid(value.Block, value.Hash)
			if blk.Cid().String() != value.Hash {
				return fmt.Errorf("receive a wrong block: %s, expected: %s", blk.Cid().String(), value.Hash)
			}
			err = this.Fs.PutBlock(blk)
			if err != nil {
				return err
			}
			err = this.Fs.PutTag(value.Hash, fileHashStr, uint64(value.Index), value.Tag)
			if err != nil {
				return err
			}
			err = this.taskMgr.SetBlockDownloaded(taskId, value.Hash, value.PeerAddr, uint32(value.Index), value.Offset, nil)
			if err != nil {
				log.Errorf("SetBlockDownloaded taskId:%s, %s-%s-%d-%d, err: %s", taskId, fileHashStr, value.Hash, value.Index, value.Offset, err)
				return err
			}
			log.Debugf("SetBlockDownloaded taskId:%s, %s-%s-%d-%d", taskId, fileHashStr, value.Hash, value.Index, value.Offset)
		}
	}
	if !this.taskMgr.IsFileDownloaded(taskId) {
		return errors.New("break here, but file not be stored")
	}

	err = this.Fs.SetFsFileBlockHashes(fileHashStr, blockHashes)
	if err != nil {
		log.Errorf("set file blockhashes err %s", err)
		return err
	}

	// all block is saved, prove it
	for i := 0; i < common.MAX_START_PDP_RETRY; i++ {
		log.Infof("received all block, start pdp verify %s, retry: %d", fileHashStr, i)
		err = this.Fs.StartPDPVerify(fileHashStr, 0, 0, 0, chainCom.ADDRESS_EMPTY)
		if err == nil {
			break
		}
		time.Sleep(time.Duration(common.START_PDP_RETRY_DELAY) * time.Second)
	}
	if err != nil {
		log.Errorf("start pdp verify err %s", err)
		deleteErr := this.DeleteDownloadedFile(taskId)
		if deleteErr != nil {
			log.Errorf("delete download file err of PDP failed file %s", deleteErr)
			return deleteErr
		}
		return err
	}
	this.PushToTrackers(fileHashStr, this.DNS.TrackerUrls, client.P2pGetPublicAddr())
	// TODO: remove unused file info fields after prove pdp success
	this.taskMgr.SetTaskState(taskId, task.TaskStateDone)
	this.taskMgr.DeleteTask(taskId)
	doneMsg := message.NewFileFetchDone(taskId, fileHashStr)
	client.P2pSend(addr, doneMsg.ToProtoMsg())
	log.Debugf("fetch file done, send done msg to %s", addr)
	return nil
}

// downloadBlockUnits. download block helper function for client.
func (this *Dsp) downloadBlockFlights(taskId, fileHashStr, ipAddr, peerWalletAddr string, blocks []*block.Block, retry, timeout uint32) ([]*task.BlockResp, error) {
	sessionId, err := this.taskMgr.GetSessionId(taskId, peerWalletAddr)
	if err != nil {
		return nil, err
	}
	for _, v := range blocks {
		log.Debugf("download block of task: %s %s-%s-%d from %s", taskId, fileHashStr, v.Hash, v.Index, ipAddr)
	}
	timeStamp := time.Now().UnixNano()
	log.Debugf("download block timestamp %d", timeStamp)
	ch := this.taskMgr.NewBlockFlightsRespCh(taskId, sessionId, timeStamp)
	defer func() {
		log.Debugf("drop blockflight resp channel: %s-%s-%d", taskId, sessionId, timeStamp)
		this.taskMgr.DropBlockFlightsRespCh(taskId, sessionId, timeStamp)
	}()
	msg := message.NewBlockFlightsReqMsg(blocks, timeStamp)

	for i := uint32(0); i < retry; i++ {
		log.Debugf("send download blockflights msg sessionId %s of %s from %s,  retry: %d", sessionId, fileHashStr, ipAddr, i)
		// TODO: refactor send msg with request-reply model
		err = client.P2pSend(ipAddr, msg.ToProtoMsg())
		if err != nil {
			log.Errorf("send download blockflights msg err: %s", err)
		}
		select {
		case value, ok := <-ch:
			if !ok {
				err = fmt.Errorf("receiving block channel close")
				continue
			}
			log.Debugf("receive blocks len: %d", len(value))
			return value, nil
		case <-time.After(time.Duration(timeout/retry) * time.Second):
			err = fmt.Errorf("receiving blockflight %s timeout", blocks[0].GetHash())
			continue
		}
	}
	return nil, err
}

func (this *Dsp) stopDownload(taskId string) (bool, *serr.SDKError) {
	stop, err := this.taskMgr.IsTaskStop(taskId)
	if err != nil {
		sdkerr := serr.NewDetailError(serr.TASK_INTERNAL_ERROR, err.Error())
		return false, sdkerr
	}
	return stop, nil
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
	taskId := this.taskMgr.TaskId(fileHashStr, this.WalletAddress(), store.TaskTypeDownload)
	if len(taskId) == 0 {
		var err error
		taskId, err = this.taskMgr.NewTask(store.TaskTypeDownload)
		if err != nil {
			return err
		}
		err = this.taskMgr.AddFileBlockHashes(taskId, hashes)
		if err != nil {
			return err
		}
		err = this.taskMgr.SetPrefix(taskId, prefix)
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
	// TODO: set nil prefix for encrypted file
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
	err := common.CreateDirIfNeed(dir)
	if err != nil {
		return nil, err
	}
	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0666)
	log.Debugf("create download file %s %v %s", filePath, file, err)
	if err != nil {
		return nil, err
	}
	return file, nil
}

func adjustDownloadCap(cap int, qos []int64) int {
	speedUp := canDownloadSpeedUp(qos)
	newCap := cap
	if speedUp {
		newCap = cap + 2
		if newCap > common.MAX_REQ_BLOCK_COUNT {
			return common.MAX_REQ_BLOCK_COUNT
		} else {
			return newCap
		}
	}
	newCap = cap - 2
	if newCap < common.MIN_REQ_BLOCK_COUNT {
		return common.MIN_REQ_BLOCK_COUNT
	}
	return newCap
}

func canDownloadSpeedUp(qos []int64) bool {
	if len(qos) < common.MIN_DOWNLOAD_QOS_LEN {
		return false
	}
	if qos[len(qos)-1] >= common.DOWNLOAD_BLOCKFLIGHTS_TIMEOUT {
		return false
	}
	qosSum := int64(0)
	for i := 0; i < common.MIN_DOWNLOAD_QOS_LEN; i++ {
		qosSum += qos[len(qos)-i-1]
	}
	avg := qosSum / common.MIN_DOWNLOAD_QOS_LEN
	log.Debugf("qosSum :%d, avg : %d", qosSum, avg)
	return avg < common.DOWNLOAD_BLOCKFLIGHTS_TIMEOUT
}
