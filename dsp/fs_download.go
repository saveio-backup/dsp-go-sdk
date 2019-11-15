package dsp

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/saveio/dsp-go-sdk/actor/client"
	"github.com/saveio/dsp-go-sdk/common"
	"github.com/saveio/dsp-go-sdk/config"
	dspErr "github.com/saveio/dsp-go-sdk/error"
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
	fs "github.com/saveio/themis/smartcontract/service/native/savefs"
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
	var err error
	defer func() {
		sdkErr, _ := err.(*dspErr.Error)
		if err != nil {
			log.Errorf("download file %s err %s", fileHashStr, err)
		}
		log.Debugf("emit ret %s %s", taskId, err)
		if sdkErr != nil {
			this.taskMgr.EmitResult(taskId, nil, sdkErr)
		}
		// delete task from cache in the end
		if this.taskMgr.IsFileDownloaded(taskId) && sdkErr == nil {
			this.taskMgr.EmitResult(taskId, "", nil)
			this.taskMgr.DeleteTask(taskId)
		}
	}()
	if len(taskId) == 0 {
		taskId, err = this.taskMgr.NewTask(store.TaskTypeDownload)
		if err != nil {
			return err
		}
	}
	log.Debugf("download file id: %s, fileHash: %s, option: %v", taskId, fileHashStr, opt)
	this.taskMgr.EmitProgress(taskId, task.TaskDownloadFileStart)
	if len(fileHashStr) == 0 {
		log.Errorf("taskId %s no filehash for download", taskId)
		err = errors.New("no filehash for download")
		return dspErr.NewWithError(dspErr.DOWNLOAD_FILEHASH_NOT_FOUND, err)
	}
	if this.dns.DNSNode == nil {
		err = dspErr.New(dspErr.NO_CONNECTED_DNS, "no online dns node")
		return dspErr.NewWithError(dspErr.NO_CONNECTED_DNS, err)
	}
	log.Debugf("download file dns node %s", this.dns.DNSNode.WalletAddr)
	if err = this.taskMgr.SetTaskInfoWithOptions(taskId, task.FileHash(fileHashStr), task.FileName(opt.FileName),
		task.FileOwner(opt.FileOwner), task.Url(opt.Url), task.Walletaddr(this.chain.WalletAddress())); err != nil {
		return err
	}
	if err = this.taskMgr.SetFileDownloadOptions(taskId, opt); err != nil {
		return err
	}
	if err = this.taskMgr.BindTaskId(taskId); err != nil {
		return err
	}
	this.taskMgr.EmitProgress(taskId, task.TaskDownloadFileStart)
	if stop, err := this.taskMgr.IsTaskStop(taskId); err != nil || stop {
		return err
	}
	this.taskMgr.EmitProgress(taskId, task.TaskDownloadSearchPeers)
	addrs, err := this.getPeersForDownload(fileHashStr)
	if err != nil {
		return err
	}
	log.Debugf("get addr from peer %v, hash %s", addrs, fileHashStr)
	if opt.MaxPeerCnt > common.MAX_DOWNLOAD_PEERS_NUM {
		opt.MaxPeerCnt = common.MAX_DOWNLOAD_PEERS_NUM
	}
	if stop, err := this.taskMgr.IsTaskStop(taskId); err != nil || stop {
		return err
	}
	if err = this.downloadFileFromPeers(taskId, fileHashStr, opt.Asset, opt.InOrder, opt.DecryptPwd, opt.Free, opt.SetFileName, opt.MaxPeerCnt, addrs); err != nil {
		return err
	}
	return nil
}

func (this *Dsp) PauseDownload(taskId string) error {
	if taskType, _ := this.taskMgr.GetTaskType(taskId); taskType != store.TaskTypeDownload {
		return dspErr.New(dspErr.WRONG_TASK_TYPE, "task %s is not a download task", taskId)
	}
	if canPause, err := this.taskMgr.IsTaskCanPause(taskId); err != nil || !canPause {
		return err
	}
	if err := this.taskMgr.SetTaskState(taskId, store.TaskStatePause); err != nil {
		return err
	}
	this.taskMgr.EmitProgress(taskId, task.TaskPause)
	return nil
}

func (this *Dsp) ResumeDownload(taskId string) error {
	if taskType, _ := this.taskMgr.GetTaskType(taskId); taskType != store.TaskTypeDownload {
		return dspErr.New(dspErr.WRONG_TASK_TYPE, "task %s is not a download task", taskId)
	}
	if canResume, err := this.taskMgr.IsTaskCanResume(taskId); err != nil || !canResume {
		return err
	}
	if err := this.taskMgr.SetTaskState(taskId, store.TaskStateDoing); err != nil {
		return err
	}
	this.taskMgr.EmitProgress(taskId, task.TaskDoing)
	return this.checkIfResumeDownload(taskId)
}

func (this *Dsp) RetryDownload(taskId string) error {
	if taskType, _ := this.taskMgr.GetTaskType(taskId); taskType != store.TaskTypeDownload {
		return dspErr.New(dspErr.WRONG_TASK_TYPE, "task %s is not a download task", taskId)
	}
	if failed, err := this.taskMgr.IsTaskFailed(taskId); err != nil || !failed {
		return err
	}
	if err := this.taskMgr.SetTaskState(taskId, store.TaskStateDoing); err != nil {
		return err
	}
	this.taskMgr.EmitProgress(taskId, task.TaskDoing)
	return this.checkIfResumeDownload(taskId)
}

func (this *Dsp) CancelDownload(taskId string) error {
	if taskType, _ := this.taskMgr.GetTaskType(taskId); taskType != store.TaskTypeDownload {
		return dspErr.New(dspErr.WRONG_TASK_TYPE, "task %s is not a download task", taskId)
	}
	cancel, err := this.taskMgr.IsTaskCancel(taskId)
	if err != nil {
		return err
	}
	if cancel {
		return fmt.Errorf("task is cancelling: %s", taskId)
	}
	fileHashStr, _ := this.taskMgr.GetTaskFileHash(taskId)
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
			msg := message.NewFileMsg(fileHashStr, netcom.FILE_OP_DOWNLOAD_CANCEL,
				message.WithSessionId(ses.SessionId),
				message.WithWalletAddress(this.chain.WalletAddress()),
				message.WithAsset(int32(ses.Asset)),
				message.WithSign(this.account),
			)
			client.P2pRequestWithRetry(msg.ToProtoMsg(), a, common.MAX_NETWORK_REQUEST_RETRY, common.P2P_REQUEST_WAIT_REPLY_TIMEOUT)
			wg.Done()
		}(hostAddr, session)
	}
	wg.Wait()
	return this.DeleteDownloadedFile(taskId)
}

// DownloadFileByLink. download file by link, e.g oni://Qm...&name=xxx&tr=xxx
func (this *Dsp) DownloadFileByLink(link string, asset int32, inOrder bool, decryptPwd string, free, setFileName bool, maxPeerCnt int) error {
	fileHashStr := utils.GetFileHashFromLink(link)
	linkvalues := this.dns.GetLinkValues(link)
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
	taskId := this.taskMgr.TaskId(fileHashStr, this.chain.WalletAddress(), store.TaskTypeDownload)
	if !this.taskMgr.TaskExist(taskId) {
		log.Debugf("DownloadFileByLink %s, hash %s, opt %v", fileHashStr, fileHashStr, opt)
		return this.DownloadFile("", fileHashStr, opt)
	}
	taskDone, _ := this.taskMgr.IsTaskDone(taskId)
	if taskDone {
		log.Debugf("DownloadFileByLink %s, hash %s, opt %v, download a done task", fileHashStr, fileHashStr, opt)
		return this.DownloadFile("", fileHashStr, opt)
	}
	return dspErr.New(dspErr.DOWNLOAD_TASK_EXIST, "task %s has exist, but not finished", taskId)
}

// DownloadFileByUrl. download file by link, e.g dsp://file1
func (this *Dsp) DownloadFileByUrl(url string, asset int32, inOrder bool, decryptPwd string, free, setFileName bool, maxPeerCnt int) error {
	fileHashStr := this.dns.GetFileHashFromUrl(url)
	linkvalues := this.dns.GetLinkValues(this.dns.GetLinkFromUrl(url))
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
	taskId := this.taskMgr.TaskId(fileHashStr, this.chain.WalletAddress(), store.TaskTypeDownload)
	if !this.taskMgr.TaskExist(taskId) {
		log.Debugf("DownloadFileByUrl %s, hash %s, opt %v", url, fileHashStr, opt)
		return this.DownloadFile("", fileHashStr, opt)
	} else {
		taskDone, _ := this.taskMgr.IsTaskDone(taskId)
		if taskDone {
			log.Debugf("DownloadFileByUrl %s, hash %s, opt %v, download a done task", url, fileHashStr, opt)
			return this.DownloadFile("", fileHashStr, opt)
		}
		log.Debugf("task has exist, but not finished %s", taskId)
		return this.DownloadFile(taskId, fileHashStr, opt)
	}
}

// DownloadFileByUrl. download file by link, e.g dsp://file1
func (this *Dsp) DownloadFileByHash(fileHashStr string, asset int32, inOrder bool, decryptPwd string, free, setFileName bool, maxPeerCnt int) error {
	// TODO: get file name
	info, _ := this.chain.GetFileInfo(fileHashStr)
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
	taskId := this.taskMgr.TaskId(fileHashStr, this.chain.WalletAddress(), store.TaskTypeDownload)
	if !this.taskMgr.TaskExist(taskId) {
		log.Debugf("DownloadFileByHash %s, hash %s, opt %v", fileHashStr, fileHashStr, opt)
		return this.DownloadFile("", fileHashStr, opt)
	}
	taskDone, _ := this.taskMgr.IsTaskDone(taskId)
	if taskDone {
		log.Debugf("DownloadFileByHash %s, hash %s, opt %v, download a done task", fileHashStr, fileHashStr, opt)
		return this.DownloadFile("", fileHashStr, opt)
	}
	return dspErr.New(dspErr.DOWNLOAD_TASK_EXIST, "task %s has exist, but not finished", taskId)
}

// GetDownloadQuotation. get peers and the download price of the file. if free flag is set, return price-free peers.
func (this *Dsp) GetDownloadQuotation(fileHashStr, decryptPwd string, asset int32, free bool, addrs []string) (map[string]*file.Payment, error) {
	if len(addrs) == 0 {
		return nil, dspErr.New(dspErr.NO_DOWNLOAD_SEED, "no peer for download")
	}
	taskId := this.taskMgr.TaskId(fileHashStr, this.chain.WalletAddress(), store.TaskTypeDownload)
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
	msg := message.NewFileMsg(fileHashStr, netcom.FILE_OP_DOWNLOAD_ASK,
		message.WithWalletAddress(this.chain.WalletAddress()),
		message.WithAsset(asset),
		message.WithSign(this.account),
	)
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
	connectionErrCnt := 0
	for _, broadcastErr := range ret {
		if broadcastErr != nil && strings.Contains(broadcastErr.Error(), "connected timeout") {
			connectionErrCnt++
		}
	}
	if connectionErrCnt == len(ret) {
		return nil, dspErr.New(dspErr.NETWORK_CONNECT_ERROR, "connect to all peers failed")
	}
	log.Debugf("peer prices:%v", peerPayInfos)
	if len(peerPayInfos) == 0 {
		return nil, dspErr.New(dspErr.REMOTE_PEER_DELETE_FILE, "remote peer has deleted the file")
	}
	totalCount, _ := this.taskMgr.GetFileTotalBlockCount(taskId)
	log.Debugf("get file total count :%d", totalCount)
	if uint64(len(blockHashes)) == totalCount {
		return peerPayInfos, nil
	}
	log.Debugf("AddFileBlockHashes id %v hashes %s-%s, prefix %s, len: %d", taskId, blockHashes[0], blockHashes[len(blockHashes)-1], prefix, len(prefix))
	if err := this.taskMgr.AddFileBlockHashes(taskId, blockHashes); err != nil {
		return nil, err
	}
	if err := this.taskMgr.SetTaskInfoWithOptions(taskId, task.Prefix(prefix), task.TotalBlockCnt(uint64(len(blockHashes)))); err != nil {
		return nil, err
	}
	return peerPayInfos, nil
}

// DepositChannelForFile. deposit channel. peerPrices: peer network address <=> payment info
func (this *Dsp) DepositChannelForFile(fileHashStr string, peerPrices map[string]*file.Payment) error {
	taskId := this.taskMgr.TaskId(fileHashStr, this.chain.WalletAddress(), store.TaskTypeDownload)
	if !this.taskMgr.IsFileInfoExist(taskId) {
		return dspErr.New(dspErr.GET_FILEINFO_FROM_DB_ERROR, "download info not exist")
	}
	log.Debugf("GetExternalIP for downloaded nodes %v", peerPrices)
	if len(peerPrices) == 0 {
		return dspErr.New(dspErr.NO_DOWNLOAD_SEED, "no peers to deposit channel")
	}
	for _, payInfo := range peerPrices {
		hostAddr, err := this.dns.GetExternalIP(payInfo.WalletAddress)
		log.Debugf("Set host addr after deposit channel %s - %s, err %s", payInfo.WalletAddress, hostAddr, err)
		if len(hostAddr) == 0 || err != nil {
			continue
		}
		// TODO: change to parallel job
		go client.P2pConnect(hostAddr)
	}
	if !this.config.AutoSetupDNSEnable {
		return nil
	}
	totalCount, _ := this.taskMgr.GetFileTotalBlockCount(taskId)
	log.Debugf("get blockhashes from %s, totalCount %d", taskId, totalCount)
	if totalCount == 0 {
		return dspErr.New(dspErr.NO_BLOCK_TO_DOWNLOAD, "no blocks")
	}
	totalAmount := common.FILE_DOWNLOAD_UNIT_PRICE * uint64(totalCount) * uint64(common.CHUNK_SIZE)
	log.Debugf("deposit to channel price:%d, cnt:%d, chunksize:%d, total:%d", common.FILE_DOWNLOAD_UNIT_PRICE, totalCount, common.CHUNK_SIZE, totalAmount)
	if totalAmount/common.FILE_DOWNLOAD_UNIT_PRICE != uint64(totalCount)*uint64(common.CHUNK_SIZE) {
		return dspErr.New(dspErr.INTERNAL_ERROR, "deposit amount overflow")
	}
	curBal, _ := this.channel.GetAvailableBalance(this.dns.DNSNode.WalletAddr)
	if curBal >= totalAmount {
		return nil
	}
	log.Debugf("depositing...")
	if err := this.channel.SetDeposit(this.dns.DNSNode.WalletAddr, totalAmount); err != nil {
		log.Debugf("deposit result %s", err)
		return err
	}
	return nil
}

// PayForBlock. Pay for block with media transfer. PayInfo is some payment info of receiver, addr is a host address of receiver.
// BlockSize is the size to pay in KB. If newpayment flag is set, it is set to a new payment and will accumulate to unpaid amount.
// PaymentId, a unique id for payment, if it's zero. It will be generated by a random integer
func (this *Dsp) PayForBlock(payInfo *file.Payment, addr, fileHashStr string, blockSize uint64, paymentId int32, newPayment bool) (int32, error) {
	if payInfo == nil {
		log.Warn("payinfo is nil")
		return 0, nil
	}
	if payInfo.WalletAddress == this.chain.WalletAddress() {
		return 0, dspErr.New(dspErr.PAY_BLOCK_TO_SELF, "can't pay to self : %s", payInfo.WalletAddress)
	}
	if this.dns == nil || this.dns.DNSNode == nil {
		return 0, dspErr.New(dspErr.NO_CONNECTED_DNS, "no dns")
	}
	amount := blockSize * payInfo.UnitPrice
	if amount/blockSize != payInfo.UnitPrice {
		return 0, dspErr.New(dspErr.INTERNAL_ERROR, "total price overflow")
	}
	if amount == 0 {
		log.Warn("pay amount is 0")
		return 0, nil
	}
	taskId := this.taskMgr.TaskId(fileHashStr, this.chain.WalletAddress(), store.TaskTypeDownload)
	if paymentId == 0 {
		paymentId = this.channel.NewPaymentId()
	}
	if newPayment {
		if err := this.taskMgr.AddFileUnpaid(taskId, payInfo.WalletAddress, paymentId, payInfo.Asset, amount); err != nil {
			return 0, err
		}
	}
	dnsHostAddr, err := this.dns.GetExternalIP(this.dns.DNSNode.WalletAddr)
	if err != nil {
		return 0, err
	}
	exist, err := client.P2pConnectionExist(dnsHostAddr, client.P2pNetTypeChannel)
	if err != nil || !exist {
		log.Debugf("DNS connection not exist reconnect...")
		if err = client.P2pReconnectPeer(dnsHostAddr, client.P2pNetTypeChannel); err != nil {
			return 0, err
		}
		if err = client.P2pWaitForConnected(dnsHostAddr, time.Duration(common.WAIT_CHANNEL_CONNECT_TIMEOUT)); err != nil {
			return 0, err
		}
	}
	log.Debugf("paying to %s, id %v, err:%s", payInfo.WalletAddress, paymentId, err)
	if err := this.channel.MediaTransfer(paymentId, amount, this.dns.DNSNode.WalletAddr, payInfo.WalletAddress); err != nil {
		log.Debugf("mediaTransfer failed paymentId %d, payTo: %s, err %s", paymentId, payInfo.WalletAddress, err)
		return 0, err
	}
	log.Debugf("paying to %s, id %v success", payInfo.WalletAddress, paymentId)
	// clean unpaid order
	if err := this.taskMgr.DeleteFileUnpaid(taskId, payInfo.WalletAddress, paymentId, payInfo.Asset, amount); err != nil {
		return 0, err
	}
	log.Debugf("delete unpaid %d", amount)
	return paymentId, nil
}

// PayUnpaidFile. pay unpaid order for file
func (this *Dsp) PayUnpaidPayments(taskId, fileHashStr string, quotation map[string]*file.Payment) error {
	for hostAddr, payInfo := range quotation {
		payments, _ := this.taskMgr.GetUnpaidPayments(taskId, payInfo.WalletAddress, payInfo.Asset)
		if len(payments) == 0 {
			continue
		}
		for _, payment := range payments {
			log.Debugf("pay to %s of the unpaid amount %d for task %s", payInfo.WalletAddress, payment.Amount, taskId)
			_, err := this.PayForBlock(payInfo, hostAddr, fileHashStr, payment.Amount/payInfo.UnitPrice, payment.PaymentId, false)
			if err != nil {
				log.Errorf("pay unpaid payment err %s", err)
			}
		}
	}
	return nil
}

// DownloadFileWithQuotation. download file, piece by piece from addrs.
// inOrder: if true, the file will be downloaded block by block in order
func (this *Dsp) DownloadFileWithQuotation(fileHashStr string, asset int32, inOrder, setFileName bool, quotation map[string]*file.Payment, decryptPwd string) error {
	// task exist at runtime
	taskId := this.taskMgr.TaskId(fileHashStr, this.chain.WalletAddress(), store.TaskTypeDownload)
	if len(quotation) == 0 {
		return dspErr.New(dspErr.GET_DOWNLOAD_INFO_FAILED_FROM_PEERS, "no peer quotation for download: %s", fileHashStr)
	}
	if !this.taskMgr.IsFileInfoExist(taskId) {
		return dspErr.New(dspErr.FILEINFO_NOT_EXIST, "download info not exist: %s", fileHashStr)
	}
	// pay unpaid order of the file after last download
	if err := this.PayUnpaidPayments(taskId, fileHashStr, quotation); err != nil {
		return dspErr.NewWithError(dspErr.PAY_UNPAID_BLOCK_FAILED, err)
	}
	if stop, err := this.taskMgr.IsTaskStop(taskId); err != nil || stop {
		return err
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
			msg := message.NewFileMsg(fileHashStr, netcom.FILE_OP_DOWNLOAD,
				message.WithSessionId(sessionId),
				message.WithWalletAddress(this.chain.WalletAddress()),
				message.WithAsset(asset),
				message.WithSign(this.account),
			)
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
		return err
	}
	prefix := string(prefixBuf)
	log.Debugf("filehashstr:%v, blockhashes-len:%v, prefix:%v", fileHashStr, len(blockHashes), prefix)
	fileName, _ := this.taskMgr.GetFileName(taskId)
	fullFilePath, err := this.taskMgr.GetFilePath(taskId)
	if err != nil {
		return err
	}
	if len(fullFilePath) == 0 {
		if setFileName {
			fullFilePath = utils.GetFileNameAtPath(this.config.FsFileRoot+"/", fileHashStr, fileName)
			log.Debugf("get fullFilePath of id :%s, filehashstr %s, filename %s, taskDone: %t path %s", taskId, fileHashStr, fileName, fullFilePath)
		} else {
			fullFilePath = this.config.FsFileRoot + "/" + fileHashStr
		}
		if err := this.taskMgr.SetFilePath(taskId, fullFilePath); err != nil {
			return err
		}
	}
	if err := this.taskMgr.SetTotalBlockCount(taskId, uint64(len(blockHashes))); err != nil {
		return err
	}
	if stop, err := this.taskMgr.IsTaskStop(taskId); err != nil || stop {
		return err
	}
	this.taskMgr.EmitProgress(taskId, task.TaskDownloadFileDownloading)
	// declare job for workers
	job := func(tId, fHash, pAddr, walletAddr string, blocks []*block.Block) ([]*task.BlockResp, error) {
		this.taskMgr.EmitProgress(taskId, task.TaskDownloadRequestBlocks)
		resp, err := this.downloadBlockFlights(tId, fHash, pAddr, walletAddr, blocks, 1, common.DOWNLOAD_BLOCKFLIGHTS_TIMEOUT)
		if err != nil {
			return nil, err
		}
		if resp == nil || len(resp) == 0 {
			return nil, dspErr.New(dspErr.DOWNLOAD_BLOCK_FAILED, "download blocks is nil %s from %s %s, err %s", fHash, pAddr, walletAddr, err)
		}
		this.taskMgr.EmitProgress(taskId, task.TaskDownloadReceiveBlocks)
		payInfo := quotation[pAddr]
		var totalBytes int
		for _, v := range resp {
			totalBytes += len(v.Block)
		}
		if totalBytes == 0 {
			return nil, dspErr.New(dspErr.DOWNLOAD_BLOCK_FAILED, "request total bytes count 0")
		}
		this.taskMgr.EmitProgress(taskId, task.TaskDownloadPayForBlocks)
		paymentId, err := this.PayForBlock(payInfo, pAddr, fHash, uint64(totalBytes), resp[0].PaymentId, true)
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
		if err := this.receiveBlockInOrder(taskId, fileHashStr, fullFilePath, prefix, peerAddrWallet, asset); err != nil {
			return err
		}
		if len(decryptPwd) == 0 {
			return nil
		}
		if err := this.decryptDownloadedFile(fullFilePath, decryptPwd); err != nil {
			return err
		}
		return nil
	}
	// TODO: support out-of-order download
	return nil
}

// DeleteDownloadedFile. Delete downloaded file in local.
func (this *Dsp) DeleteDownloadedFile(taskId string) error {
	if len(taskId) == 0 {
		return dspErr.New(dspErr.DELETE_FILE_FAILED, "delete taskId is empty")
	}
	filePath, _ := this.taskMgr.GetFilePath(taskId)
	fileHashStr, _ := this.taskMgr.GetTaskFileHash(taskId)
	err := this.fs.DeleteFile(fileHashStr, filePath)
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
			if this.isStop {
				log.Debugf("stop backup file service")
				ticker.Stop()
				return
			}
			if this.chain == nil {
				break
			}
			tasks, err := this.chain.GetExpiredProveList()
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
					continue
				}
				if len(t.FileHash) == 0 || len(t.BakSrvAddr) == 0 || len(t.BackUpAddr.ToBase58()) == 0 {
					continue
				}
				if v, ok := backupFailedMap[string(t.FileHash)]; ok && v >= common.MAX_BACKUP_FILE_FAILED {
					log.Debugf("skip backup this file, because has failed")
					continue
				}
				backupingCnt++
				log.Debugf("go backup file:%s, from:%s %s", t.FileHash, t.BakSrvAddr, t.BackUpAddr.ToBase58())
				fileInfo, err := this.chain.GetFileInfo(string(t.FileHash))
				if err != nil {
					continue
				}
				if err = this.backupFileFromPeer(fileInfo, string(t.BakSrvAddr), t.LuckyNum, t.BakHeight, t.BakNum, t.BrokenAddr); err != nil {
					backupFailedMap[string(t.FileHash)]++
					continue
				}
			}
			// reset
			backupingCnt = 0
		}
	}
}

// StartFetchFileService. start fetch files from master node when client upload files
func (this *Dsp) StartFetchFileService() {
	ticker := time.NewTicker(time.Duration(common.BACKUP_FILE_DURATION) * time.Second)
	for {
		<-ticker.C
		if this.isStop {
			log.Debugf("stop fetch file service")
			ticker.Stop()
			return
		}
		fileInfos, err := this.chain.GetUnprovePrimaryFileInfos(this.chain.Address())
		if err != nil {
			continue
		}
		log.Debugf("unprove primary files count: %d", len(fileInfos))
		for _, fi := range fileInfos {
			if len(fi.PrimaryNodes.AddrList) < 2 {
				log.Debugf("primary node list too small %v", fi.PrimaryNodes.AddrList)
				continue
			}
			if fi.PrimaryNodes.AddrList[0].ToBase58() == this.WalletAddress() {
				// i am the master node, skip..
				log.Debugf("skip fetch because i am master node")
				continue
			}
			fileHashStr := string(fi.FileHash)
			// find if i can fetch file
			if !this.chain.CheckHasProveFile(fileHashStr, fi.PrimaryNodes.AddrList[0]) {
				log.Debugf("node has not prove %v %v", fileHashStr, fi.PrimaryNodes.AddrList[0])
				continue
			}
			// get host addr from wallet addr
			hostAddrs, err := this.chain.GetNodeHostAddrListByWallets([]chainCom.Address{fi.PrimaryNodes.AddrList[0]})
			if err != nil {
				log.Errorf("get host addr failed of wallet %s", err)
				continue
			}
			if len(hostAddrs) != 1 {
				log.Errorf("get host addr failed of wallet %s", fi.PrimaryNodes.AddrList[0])
				continue
			}

			log.Debugf("go back up file %s from %s", string(fi.FileHash), hostAddrs[0])
			// start download file
			if err := this.backupFileFromPeer(&fi, hostAddrs[0], 0, 0, 0, chainCom.ADDRESS_EMPTY); err != nil {
				log.Errorf("backup file err %s", err)
			}
			log.Infof("download file success %s", string(fi.FileHash))
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
			if this.isStop {
				log.Debugf("stop check remove files service")
				ticker.Stop()
				return
			}
			files := this.fs.RemovedExpiredFiles()
			if len(files) == 0 {
				continue
			}
			for _, f := range files {
				hash, ok := f.(string)
				if !ok {
					continue
				}
				taskId := this.taskMgr.TaskId(hash, this.chain.WalletAddress(), store.TaskTypeDownload)
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
	fileInfoKey := this.taskMgr.TaskId(fileHashStr, this.chain.WalletAddress(), store.TaskTypeDownload)
	return this.taskMgr.GetFileInfo(fileInfoKey)
}

// getPeersForDownload. get peers for download from tracker and fs contract
func (this *Dsp) getPeersForDownload(fileHashStr string) ([]string, error) {
	addrs := this.dns.GetPeerFromTracker(fileHashStr, this.dns.TrackerUrls)
	log.Debugf("get addr from peer %v, hash %s %v", addrs, fileHashStr, this.dns.TrackerUrls)
	if len(addrs) > 0 {
		return addrs, nil
	}
	log.Warnf("get 0 peer from tracker of file %s, tracker num %d", fileHashStr, len(this.dns.TrackerUrls))
	addrs = this.getFileProvedNode(fileHashStr)
	if len(addrs) > 0 {
		return addrs, nil
	}
	return nil, dspErr.New(dspErr.NO_DOWNLOAD_SEED, "No peer for downloading the file %s", fileHashStr)
}

func (this *Dsp) checkIfResumeDownload(taskId string) error {
	opt, err := this.taskMgr.GetFileDownloadOptions(taskId)
	if err != nil {
		return err
	}
	if opt == nil {
		return dspErr.New(dspErr.GET_FILEINFO_FROM_DB_ERROR, "can't find download options, please retry")
	}
	fileHashStr, _ := this.taskMgr.GetTaskFileHash(taskId)
	if len(fileHashStr) == 0 {
		return dspErr.New(dspErr.GET_FILEINFO_FROM_DB_ERROR, "filehash not found %s", taskId)
	}
	log.Debugf("resume download file")
	// TODO: record original workers
	go this.DownloadFile(taskId, fileHashStr, opt)
	return nil
}

// receiveBlockInOrder. receive blocks in order
func (this *Dsp) receiveBlockInOrder(taskId, fileHashStr, fullFilePath, prefix string, peerAddrWallet map[string]string, asset int32) error {
	blockIndex, err := this.addUndownloadedReq(taskId, fileHashStr)
	if err != nil {
		return dspErr.NewWithError(dspErr.GET_UNDOWNLOADED_BLOCK_FAILED, err)
	}
	hasCutPrefix := false
	filePrefix := &utils.FilePrefix{}
	filePrefix.Deserialize([]byte(prefix))
	var file *os.File
	if this.config.FsType == config.FS_FILESTORE {
		file, err = createDownloadFile(this.config.FsFileRoot, fullFilePath)
		if err != nil {
			return dspErr.NewWithError(dspErr.CREATE_DOWNLOAD_FILE_FAILED, err)
		}
		defer file.Close()
	}
	stateCheckTicker := time.NewTicker(time.Duration(common.TASK_STATE_CHECK_DURATION) * time.Second)
	defer stateCheckTicker.Stop()
	for {
		select {
		case value, ok := <-this.taskMgr.TaskNotify(taskId):
			if !ok {
				return dspErr.New(dspErr.DOWNLOAD_BLOCK_FAILED, "download internal error")
			}
			if stop, err := this.taskMgr.IsTaskStop(taskId); err != nil || stop {
				log.Debugf("stop download task %s", taskId)
				return err
			}
			log.Debugf("received block %s-%s-%d from %s", fileHashStr, value.Hash, value.Index, value.PeerAddr)
			if this.taskMgr.IsBlockDownloaded(taskId, value.Hash, uint32(value.Index)) {
				log.Debugf("%s-%s-%d is downloaded", fileHashStr, value.Hash, value.Index)
				continue
			}
			block := this.fs.EncodedToBlockWithCid(value.Block, value.Hash)
			links, err := this.fs.GetBlockLinks(block)
			if err != nil {
				return err
			}
			if block.Cid().String() == fileHashStr && this.config.FsType == config.FS_FILESTORE {
				if !filePrefix.Encrypt {
					this.fs.SetFsFilePrefix(fullFilePath, prefix)
				} else {
					this.fs.SetFsFilePrefix(fullFilePath, "")
				}
			}
			if len(links) == 0 && this.config.FsType == config.FS_FILESTORE {
				data := this.fs.BlockData(block)
				// TEST: performance
				fileStat, err := file.Stat()
				if err != nil {
					return dspErr.NewWithError(dspErr.GET_FILE_STATE_ERROR, err)
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
					return dspErr.NewWithError(dspErr.WRITE_FILE_DATA_FAILED, err)
				}
			}
			if this.config.FsType == config.FS_FILESTORE {
				err = this.fs.PutBlockForFileStore(fullFilePath, block, uint64(value.Offset))
				log.Debugf("put block for store err %v", err)
			} else {
				err = this.fs.PutBlock(block)
				if err != nil {
					return err
				}
				log.Debugf("block %s value.index %d, value.tag:%d", block.Cid(), value.Index, len(value.Tag))
				err = this.fs.PutTag(block.Cid().String(), fileHashStr, uint64(value.Index), value.Tag)
			}
			log.Debugf("put block for file %s block: %s, offset:%d", fullFilePath, block.Cid(), value.Offset)
			if err != nil {
				log.Errorf("put block err %s", err)
				return err
			}
			if err := this.taskMgr.SetBlockDownloaded(taskId, value.Hash, value.PeerAddr, uint32(value.Index), value.Offset, links); err != nil {
				return err
			}
			this.taskMgr.EmitProgress(taskId, task.TaskDownloadFileDownloading)
			log.Debugf("%s-%s-%d set downloaded", fileHashStr, value.Hash, value.Index)
			for _, l := range links {
				blockIndex++
				if err := this.taskMgr.AddBlockReq(taskId, l, blockIndex); err != nil {
					return dspErr.New(dspErr.ADD_GET_BLOCK_REQUEST_FAILED, err.Error())
				}
			}
			if len(links) != 0 {
				continue
			}
			// find a more accurate way
			if value.Index+common.FILE_DOWNLOADED_INDEX_OFFSET < blockIndex || !this.taskMgr.IsFileDownloaded(taskId) {
				continue
			}
			log.Debugf("file has downloaded: %t, last block index: %d", this.taskMgr.IsFileDownloaded(taskId), value.Index)
			// last block
			this.taskMgr.SetTaskState(taskId, store.TaskStateDone)
			break
		case <-stateCheckTicker.C:
			if stop, err := this.taskMgr.IsTaskStop(taskId); err != nil || stop {
				log.Debugf("stop download task %s", taskId)
				return err
			}
			if timeout, _ := this.taskMgr.IsTaskTimeout(taskId); !timeout {
				continue
			}
			this.taskMgr.SetTaskState(taskId, store.TaskStateFailed)
			workerState := this.taskMgr.GetTaskWorkerState(taskId)
			for addr, state := range workerState {
				log.Debugf("download timeout worker addr: %s, working : %t, unpaid: %t, totalFailed %v", addr, state.Working, state.Unpaid, state.TotalFailed)
			}
			return dspErr.New(dspErr.DOWNLOAD_FILE_TIMEOUT, "Download file timeout")
		}
		done, err := this.taskMgr.IsTaskDone(taskId)
		if err != nil {
			return err
		}
		if done {
			log.Debugf("download task is done")
			break
		}
	}
	this.taskMgr.EmitProgress(taskId, task.TaskDownloadFileMakeSeed)
	go this.dns.PushToTrackers(fileHashStr, this.dns.TrackerUrls, client.P2pGetPublicAddr())
	for addr, walletAddr := range peerAddrWallet {
		sessionId, err := this.taskMgr.GetSessionId(taskId, walletAddr)
		if err != nil {
			continue
		}
		go func(a, w, sid string) {
			fileDownloadOkMsg := message.NewFileMsg(fileHashStr, netcom.FILE_OP_DOWNLOAD_OK,
				message.WithSessionId(sid),
				message.WithWalletAddress(this.chain.WalletAddress()),
				message.WithAsset(asset),
				message.WithSign(this.account),
			)
			client.P2pBroadcast([]string{a}, fileDownloadOkMsg.ToProtoMsg(), true, nil)
		}(addr, walletAddr, sessionId)
	}
	return nil
}

func (this *Dsp) decryptDownloadedFile(fullFilePath, decryptPwd string) error {
	if len(decryptPwd) == 0 {
		return dspErr.New(dspErr.DECRYPT_FILE_FAILED, "no decrypt password")
	}
	filePrefix := &utils.FilePrefix{}
	sourceFile, err := os.Open(fullFilePath)
	if err != nil {
		return dspErr.New(dspErr.DECRYPT_FILE_FAILED, err.Error())
	}
	defer sourceFile.Close()
	prefix := make([]byte, utils.PREFIX_LEN)
	_, err = sourceFile.Read(prefix)
	if err != nil {
		return dspErr.New(dspErr.DECRYPT_FILE_FAILED, err.Error())
	}
	log.Debugf("read first n prefix :%v", prefix)
	filePrefix.Deserialize([]byte(prefix))
	if !utils.VerifyEncryptPassword(decryptPwd, filePrefix.EncryptSalt, filePrefix.EncryptHash) {
		return dspErr.New(dspErr.DECRYPT_WRONG_PASSWORD, "wrong password")
	}
	err = this.fs.AESDecryptFile(fullFilePath, string(prefix), decryptPwd, utils.GetDecryptedFilePath(fullFilePath))
	if err != nil {
		return dspErr.New(dspErr.DECRYPT_FILE_FAILED, err.Error())
	}
	return nil
}

func (this *Dsp) addUndownloadedReq(taskId, fileHashStr string) (int32, error) {
	hashes, indexMap, err := this.taskMgr.GetUndownloadedBlockInfo(taskId, fileHashStr)
	if err != nil {
		return 0, err
	}
	if len(hashes) == 0 {
		log.Debugf("no undownloaded block %s %v", hashes, indexMap)
		return 0, nil
	}
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
func (this *Dsp) downloadFileFromPeers(taskId, fileHashStr string, asset int32, inOrder bool, decryptPwd string, free, setFileName bool, maxPeerCnt int, addrs []string) error {
	quotation, err := this.GetDownloadQuotation(fileHashStr, decryptPwd, asset, free, addrs)
	log.Debugf("downloadFileFromPeers :%v", quotation)
	if err != nil {
		return dspErr.New(dspErr.GET_DOWNLOAD_INFO_FAILED_FROM_PEERS, err.Error())
	}
	if len(quotation) == 0 {
		return dspErr.New(dspErr.NO_DOWNLOAD_SEED, "no quotation from peers")
	}
	if stop, err := this.taskMgr.IsTaskStop(taskId); err != nil || stop {
		log.Debugf("stop download task %s", taskId)
		return err
	}
	if !free && len(addrs) > maxPeerCnt {
		log.Debugf("filter peers free %t len %d max %d", free, len(addrs), maxPeerCnt)
		// filter peers
		quotation = utils.SortPeersByPrice(quotation, maxPeerCnt)
	}
	err = this.DepositChannelForFile(fileHashStr, quotation)
	if err != nil {
		return dspErr.New(dspErr.PREPARE_CHANNEL_ERROR, err.Error())
	}
	if stop, err := this.taskMgr.IsTaskStop(taskId); err != nil || stop {
		log.Debugf("stop download task %s", taskId)
		return err
	}
	log.Debugf("set up channel success: %v", quotation)
	return this.DownloadFileWithQuotation(fileHashStr, asset, inOrder, setFileName, quotation, decryptPwd)
}

// startFetchBlocks for store node, fetch blocks one by one after receive fetch_rdy msg
func (this *Dsp) startFetchBlocks(fileHashStr string, addr, peerWalletAddr string) error {
	// my task. use my wallet address
	taskId := this.taskMgr.TaskId(fileHashStr, this.chain.WalletAddress(), store.TaskTypeDownload)
	log.Debugf("startFetchBlocks taskId: %s, fileHash: %s", taskId, fileHashStr)
	if this.taskMgr.IsFileDownloaded(taskId) {
		log.Debugf("file has downloaded: %s", fileHashStr)
		return nil
	}
	sessionId, err := this.taskMgr.GetSessionId(taskId, peerWalletAddr)
	if err != nil {
		return err
	}
	if err = client.P2pConnect(addr); err != nil {
		return err
	}
	this.taskMgr.NewWorkers(taskId, map[string]string{addr: peerWalletAddr}, true, nil)
	pause, cancel := false, false
	defer func() {
		if cancel {
			return
		}
		err := this.fs.PinRoot(context.TODO(), fileHashStr)
		if err != nil {
			log.Errorf("pin root file err %s", err)
		}
	}()
	downloadQoS := make([]int64, 0)
	blocks := make([]*block.Block, 0, common.MAX_REQ_BLOCK_COUNT)
	downloadBlkCap := common.MIN_REQ_BLOCK_COUNT
	totalCount, err := this.taskMgr.GetFileTotalBlockCount(taskId)
	if err != nil {
		return err
	}
	blockHashes := make([]string, 0, totalCount)
	blockHashes = append(blockHashes, fileHashStr)
	for index := 0; index < len(blockHashes); index++ {
		hash := blockHashes[index]
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
				Sender: this.chain.WalletAddress(),
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
			log.Debugf("adjust new download cap: %d for file %s", downloadBlkCap, fileHashStr)
		}
		blocks = blocks[:0]

		for _, value := range resps {
			blk := this.fs.EncodedToBlockWithCid(value.Block, value.Hash)
			if blk.Cid().String() != value.Hash {
				return fmt.Errorf("receive a wrong block: %s, expected: %s", blk.Cid().String(), value.Hash)
			}
			err = this.fs.PutBlock(blk)
			if err != nil {
				return err
			}
			err = this.fs.PutTag(value.Hash, fileHashStr, uint64(value.Index), value.Tag)
			if err != nil {
				return err
			}
			err = this.taskMgr.SetBlockDownloaded(taskId, value.Hash, value.PeerAddr, uint32(value.Index), value.Offset, nil)
			if err != nil {
				log.Errorf("SetBlockDownloaded taskId:%s, %s-%s-%d-%d, err: %s", taskId, fileHashStr, value.Hash, value.Index, value.Offset, err)
				return err
			}
			log.Debugf("SetBlockDownloaded taskId:%s, %s-%s-%d-%d", taskId, fileHashStr, value.Hash, value.Index, value.Offset)
			links, err := this.fs.GetBlockLinks(blk)
			if err != nil {
				log.Errorf("get block links err %s", err)
				return err
			}
			blockHashes = append(blockHashes, links...)
			log.Debugf("blockHashes: %v, len: %d", blockHashes, len(blockHashes))
		}
	}
	if !this.taskMgr.IsFileDownloaded(taskId) {
		return dspErr.New(dspErr.DOWNLOAD_BLOCK_FAILED, "all block has fetched, but file not download completely")
	}

	err = this.fs.SetFsFileBlockHashes(fileHashStr, blockHashes)
	if err != nil {
		log.Errorf("set file blockhashes err %s", err)
		return err
	}

	// all block is saved, prove it
	for i := 0; i < common.MAX_START_PDP_RETRY; i++ {
		log.Infof("received all block, start pdp verify %s, retry: %d", fileHashStr, i)
		err = this.fs.StartPDPVerify(fileHashStr, 0, 0, 0, chainCom.ADDRESS_EMPTY)
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
	this.dns.PushToTrackers(fileHashStr, this.dns.TrackerUrls, client.P2pGetPublicAddr())
	// TODO: remove unused file info fields after prove pdp success
	this.taskMgr.EmitResult(taskId, "", nil)
	this.taskMgr.DeleteTask(taskId)
	doneMsg := message.NewFileMsg(fileHashStr, netcom.FILE_OP_FETCH_DONE,
		message.WithSessionId(taskId),
		message.WithWalletAddress(this.chain.WalletAddress()),
		message.WithSign(this.account),
	)
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
	if len(blocks) > 0 {
		log.Debugf("download block of task: %s %s-%s-%d to %s-%s-%d to %s", taskId, fileHashStr, blocks[0].Hash, blocks[0].Index, fileHashStr, blocks[len(blocks)-1].Hash, blocks[len(blocks)-1].Index, ipAddr)
	}
	timeStamp := time.Now().UnixNano()
	log.Debugf("download block timestamp %d", timeStamp)
	ch := this.taskMgr.NewBlockFlightsRespCh(taskId, sessionId, timeStamp)
	defer func() {
		log.Debugf("drop block flight resp channel: %s-%s-%d", taskId, sessionId, timeStamp)
		this.taskMgr.DropBlockFlightsRespCh(taskId, sessionId, timeStamp)
	}()
	msg := message.NewBlockFlightsReqMsg(blocks, timeStamp)

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for i := uint32(0); i < retry; i++ {
		log.Debugf("send download block flights msg sessionId %s of %s from %s,  retry: %d", sessionId, fileHashStr, ipAddr, i)
		// TODO: refactor send msg with request-reply model
		err = client.P2pSend(ipAddr, msg.ToProtoMsg())
		if err != nil {
			log.Errorf("send download block flights msg err: %s", err)
			continue
		}
		downloadTimeout := false
		logCounter := 0
		for {
			select {
			case value, ok := <-ch:
				if !ok {
					err = dspErr.New(dspErr.INTERNAL_ERROR, "receiving block channel close")
					continue
				}
				log.Debugf("receive blocks len: %d", len(value))
				this.taskMgr.ActiveDownloadTaskPeer(ipAddr)
				return value, nil
			case <-ticker.C:
				duration, err := this.taskMgr.GetTaskWorkerIdleDuration(taskId, ipAddr)
				logCounter++
				if logCounter%10 == 0 {
					log.Debugf("worker %s idle duration %d, timeout %d", ipAddr, duration, timeout/retry)
				}
				if err != nil {
					return nil, err
				}
				if duration < uint64(timeout/retry)*1000 {
					continue
				}
				err = dspErr.New(dspErr.DOWNLOAD_BLOCK_FAILED, "receiving blockflight %s timeout", blocks[0].GetHash())
				// TODO: why err is nil here
				downloadTimeout = true
			}
			if downloadTimeout {
				break
			}
		}
	}
	if err != nil {
		log.Errorf("download block flight err %s", err)
	}
	return nil, err
}

// shareUploadedFile. share uploaded file when upload success
func (this *Dsp) shareUploadedFile(filePath, fileName, prefix string, hashes []string) error {
	log.Debugf("shareUploadedFile path: %s filename:%s prefix:%s hashes:%d", filePath, fileName, prefix, len(hashes))
	if !this.IsClient() {
		return dspErr.New(dspErr.INTERNAL_ERROR, "fs type is not file store")
	}
	if len(hashes) == 0 {
		return dspErr.New(dspErr.INTERNAL_ERROR, "no block hashes")
	}
	fileHashStr := hashes[0]
	taskId := this.taskMgr.TaskId(fileHashStr, this.chain.WalletAddress(), store.TaskTypeDownload)
	if len(taskId) == 0 {
		var err error
		taskId, err = this.taskMgr.NewTask(store.TaskTypeDownload)
		if err != nil {
			return err
		}
		if err := this.taskMgr.AddFileBlockHashes(taskId, hashes); err != nil {
			return err
		}
		fullFilePath := utils.GetFileNameAtPath(this.config.FsFileRoot+"/", fileHashStr, fileName)
		if err := this.taskMgr.SetTaskInfoWithOptions(taskId, task.Prefix(prefix), task.FileName(fullFilePath)); err != nil {
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
	output, err := os.OpenFile(fullFilePath, os.O_CREATE|os.O_RDWR, 0666)
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
	this.fs.SetFsFilePrefix(fullFilePath, prefix)
	offsets, err := this.fs.GetAllOffsets(fileHashStr)
	if err != nil {
		return err
	}
	for index, hash := range hashes {
		if this.taskMgr.IsBlockDownloaded(taskId, hash, uint32(index)) {
			log.Debugf("%s-%s-%d is downloaded", fileHashStr, hash, index)
			continue
		}
		block := this.fs.GetBlock(hash)
		links, err := this.fs.GetBlockLinks(block)
		if err != nil {
			return err
		}
		offset := offsets[hash]
		log.Debugf("hash: %s-%s-%d , offset: %d, links count %d", fileHashStr, hash, index, offset, len(links))
		if err := this.taskMgr.SetBlockDownloaded(taskId, fileHashStr, client.P2pGetPublicAddr(), uint32(index), int64(offset), links); err != nil {
			return err
		}
	}
	go this.dns.PushToTrackers(fileHashStr, this.dns.TrackerUrls, client.P2pGetPublicAddr())
	return nil
}

// backupFileFromPeer. backup a file from peer
func (this *Dsp) backupFileFromPeer(fileInfo *fs.FileInfo, peer string, luckyNum, bakHeight, bakNum uint64, brokenAddr chainCom.Address) error {
	fileHashStr := string(fileInfo.FileHash)
	if fileInfo == nil || len(fileHashStr) == 0 {
		return dspErr.New(dspErr.DOWNLOAD_FILEHASH_NOT_FOUND, "no filehash for download")
	}
	taskId := this.taskMgr.TaskId(fileHashStr, this.chain.WalletAddress(), store.TaskTypeDownload)
	var err error
	defer func() {
		sdkErr, _ := err.(*dspErr.Error)
		if err != nil {
			log.Errorf("download file %s err %s", fileHashStr, err)
		}
		log.Debugf("emit ret %s %s", taskId, err)
		if sdkErr != nil {
			this.taskMgr.EmitResult(taskId, nil, sdkErr)
		}
		// delete task from cache in the end
		if this.taskMgr.IsFileDownloaded(taskId) && sdkErr == nil {
			this.taskMgr.EmitResult(taskId, "", nil)
			this.taskMgr.DeleteTask(taskId)
		}
	}()
	if len(taskId) == 0 || !this.taskMgr.TaskExist(taskId) {
		taskId, err = this.taskMgr.NewTask(store.TaskTypeDownload)
		if err != nil {
			return err
		}
	}
	taskDone, _ := this.taskMgr.IsTaskDone(taskId)
	if taskDone {
		return nil
	}
	if this.dns.DNSNode == nil {
		err = dspErr.New(dspErr.NO_CONNECTED_DNS, "no online dns node")
		return dspErr.NewWithError(dspErr.NO_CONNECTED_DNS, err)
	}
	log.Debugf("download file dns node %s", this.dns.DNSNode.WalletAddr)
	if err = this.taskMgr.SetTaskInfoWithOptions(taskId, task.FileHash(fileHashStr), task.FileName(string(fileInfo.FileDesc)),
		task.FileOwner(fileInfo.FileOwner.ToBase58()), task.Walletaddr(this.chain.WalletAddress())); err != nil {
		return err
	}
	if err = this.taskMgr.BindTaskId(taskId); err != nil {
		return err
	}
	// TODO: test back up logic
	if err = this.downloadFileFromPeers(taskId, fileHashStr, common.ASSET_USDT, true, "", false, false, common.MAX_DOWNLOAD_PEERS_NUM, []string{peer}); err != nil {
		log.Errorf("download file err %s", err)
		return err
	}
	if err = this.fs.PinRoot(context.TODO(), fileHashStr); err != nil {
		log.Errorf("pin root file err %s", err)
		return err
	}
	if err = this.fs.StartPDPVerify(fileHashStr, luckyNum, bakHeight, bakNum, brokenAddr); err != nil {
		log.Errorf("pdp verify error for backup task")
		return err
	}
	log.Debugf("backup file:%s success", fileHashStr)
	return nil
}

// createDownloadFile. create file handler for write downloading file
func createDownloadFile(dir, filePath string) (*os.File, error) {
	err := common.CreateDirIfNeed(dir)
	if err != nil {
		return nil, dspErr.NewWithError(dspErr.INTERNAL_ERROR, err)
	}
	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR, 0666)
	log.Debugf("create download file %s %v %s", filePath, file, err)
	if err != nil {
		return nil, dspErr.NewWithError(dspErr.INTERNAL_ERROR, err)
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
	newCap = cap - 4
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

func (this *Dsp) AESEncryptFile(file, password, outputPath string) error {
	return this.fs.AESEncryptFile(file, password, outputPath)
}

func (this *Dsp) AESDecryptFile(file, prefix, password, outputPath string) error {
	return this.fs.AESDecryptFile(file, prefix, password, outputPath)
}
