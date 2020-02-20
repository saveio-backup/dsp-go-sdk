package dsp

import (
	"context"
	"fmt"
	"io"
	"os"
	"runtime/debug"
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

func (this *Dsp) AESEncryptFile(file, password, outputPath string) error {
	return this.fs.AESEncryptFile(file, password, outputPath)
}

func (this *Dsp) GetFileNameWithPath(filePath string) string {
	return this.taskMgr.GetFileNameWithPath(filePath)
}

func (this *Dsp) AESDecryptFile(file, prefix, password, outputPath string) error {
	return this.fs.AESDecryptFile(file, prefix, password, outputPath)
}

func (this *Dsp) InsertShareRecord(id, fileHash, fileName, fileOwner, toWalletAddr string, profit uint64) error {
	if this.shareRecordDB == nil {
		return nil
	}
	return this.shareRecordDB.InsertShareRecord(id, fileHash, fileName, fileOwner, toWalletAddr, profit)
}

func (this *Dsp) IncreaseShareRecordProfit(id string, profit uint64) error {
	if this.shareRecordDB == nil {
		return nil
	}
	return this.shareRecordDB.IncreaseShareRecordProfit(id, profit)
}

func (this *Dsp) FindShareRecordById(id string) (*store.ShareRecord, error) {
	if this.shareRecordDB == nil {
		return nil, nil
	}
	return this.shareRecordDB.FindShareRecordById(id)
}
func (this *Dsp) FineShareRecordsByCreatedAt(beginedAt, endedAt, offset, limit int64) (
	[]*store.ShareRecord, error) {
	if this.shareRecordDB == nil {
		return nil, nil
	}
	return this.shareRecordDB.FineShareRecordsByCreatedAt(beginedAt, endedAt, offset, limit)
}
func (this *Dsp) FindLastShareTime(fileHash string) (uint64, error) {
	if this.shareRecordDB == nil {
		return 0, nil
	}
	return this.shareRecordDB.FindLastShareTime(fileHash)
}
func (this *Dsp) CountRecordByFileHash(fileHash string) (uint64, error) {
	if this.shareRecordDB == nil {
		return 0, nil
	}
	return this.shareRecordDB.CountRecordByFileHash(fileHash)
}
func (this *Dsp) SumRecordsProfit() (uint64, error) {
	if this.shareRecordDB == nil {
		return 0, nil
	}
	return this.shareRecordDB.SumRecordsProfit()
}
func (this *Dsp) SumRecordsProfitByFileHash(fileHashStr string) (uint64, error) {
	if this.shareRecordDB == nil {
		return 0, nil
	}
	return this.shareRecordDB.SumRecordsProfitByFileHash(fileHashStr)
}
func (this *Dsp) SumRecordsProfitById(id string) (uint64, error) {
	if this.shareRecordDB == nil {
		return 0, nil
	}
	return this.shareRecordDB.SumRecordsProfitById(id)
}

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
func (this *Dsp) DownloadFile(taskId, fileHashStr string, opt *common.DownloadOption) (err error) {
	// start a task
	defer func() {
		sdkErr, _ := err.(*dspErr.Error)
		if err != nil {
			log.Errorf("download file %s err %s", fileHashStr, err)
		}
		log.Debugf("task %s has end, err %s", taskId, err)
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
	if err = this.taskMgr.SetTaskState(taskId, store.TaskStateDoing); err != nil {
		return err
	}
	if len(fileHashStr) == 0 {
		log.Errorf("taskId %s no filehash for download", taskId)
		return dspErr.New(dspErr.DOWNLOAD_FILEHASH_NOT_FOUND, "no filehash for download")
	}
	if this.dns.DNSNode == nil {
		return dspErr.New(dspErr.NO_CONNECTED_DNS, "no online dns node")
	}
	log.Debugf("download file dns node %s %s", this.dns.DNSNode.WalletAddr, opt.Url)
	// store task info
	if err = this.taskMgr.SetTaskInfoWithOptions(taskId,
		task.FileHash(fileHashStr),
		task.BlocksRoot(opt.BlocksRoot),
		task.FileName(opt.FileName),
		task.FileOwner(opt.FileOwner),
		task.Asset(opt.Asset),
		task.DecryptPwd(opt.DecryptPwd),
		task.Free(opt.Free),
		task.SetFileName(opt.SetFileName),
		task.MaxPeerCnt(opt.MaxPeerCnt),
		task.Url(opt.Url),
		task.Inorder(opt.InOrder),
		task.Walletaddr(this.chain.WalletAddress())); err != nil {
		return err
	}
	// bind task id to (file hash, wallet address)
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
	taskInfo, err := this.taskMgr.GetTaskInfoCopy(taskId)
	if err != nil {
		return err
	}
	if err = this.downloadFileFromPeers(taskInfo, addrs); err != nil {
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
			client.P2pSend(a, msg.MessageId, msg.ToProtoMsg())
			wg.Done()
		}(hostAddr, session)
	}
	wg.Wait()
	return this.DeleteDownloadedFile(taskId)
}

// DownloadFileByLink. download file by link, e.g oni://Qm...&name=xxx&tr=xxx
func (this *Dsp) DownloadFileByLink(linkStr string, asset int32, inOrder bool, decryptPwd string,
	free, setFileName bool, maxPeerCnt int) error {
	link, err := this.dns.GetLinkValues(linkStr)
	if err != nil {
		return err
	}
	if maxPeerCnt > common.MAX_PEERCNT_FOR_DOWNLOAD {
		maxPeerCnt = common.MAX_PEERCNT_FOR_DOWNLOAD
	}
	opt := &common.DownloadOption{
		FileName:    link.FileName,
		FileOwner:   link.FileOwner,
		BlocksRoot:  link.BlocksRoot,
		Asset:       asset,
		InOrder:     inOrder,
		DecryptPwd:  decryptPwd,
		Free:        free,
		SetFileName: setFileName,
		MaxPeerCnt:  maxPeerCnt,
	}
	return this.downloadFileWithOpt(link.FileHashStr, opt)
}

// DownloadFileByUrl. download file by link, e.g dsp://file1
func (this *Dsp) DownloadFileByUrl(url string, asset int32, inOrder bool, decryptPwd string,
	free, setFileName bool, maxPeerCnt int) error {
	fileHashStr := this.dns.GetFileHashFromUrl(url)
	link, err := this.dns.GetLinkValues(this.dns.GetLinkFromUrl(url))
	if err != nil {
		return err
	}
	if maxPeerCnt > common.MAX_PEERCNT_FOR_DOWNLOAD {
		maxPeerCnt = common.MAX_PEERCNT_FOR_DOWNLOAD
	}
	opt := &common.DownloadOption{
		FileName:    link.FileName,
		BlocksRoot:  link.BlocksRoot,
		Asset:       asset,
		InOrder:     inOrder,
		DecryptPwd:  decryptPwd,
		Free:        free,
		SetFileName: setFileName,
		FileOwner:   link.FileOwner,
		MaxPeerCnt:  maxPeerCnt,
		Url:         url,
	}
	return this.downloadFileWithOpt(fileHashStr, opt)
}

// DownloadFileByUrl. download file by link, e.g dsp://file1
func (this *Dsp) DownloadFileByHash(fileHashStr string, asset int32, inOrder bool, decryptPwd string,
	free, setFileName bool, maxPeerCnt int) error {
	// TODO: get file name, fix url
	info, _ := this.chain.GetFileInfo(fileHashStr)
	var fileName, fileOwner, blocksRoot string
	if info != nil {
		fileName = string(info.FileDesc)
		fileOwner = info.FileOwner.ToBase58()
		blocksRoot = string(info.BlocksRoot)
	}
	if maxPeerCnt > common.MAX_PEERCNT_FOR_DOWNLOAD {
		maxPeerCnt = common.MAX_PEERCNT_FOR_DOWNLOAD
	}
	opt := &common.DownloadOption{
		FileName:    fileName,
		Asset:       asset,
		InOrder:     inOrder,
		BlocksRoot:  blocksRoot,
		DecryptPwd:  decryptPwd,
		Free:        free,
		SetFileName: setFileName,
		FileOwner:   fileOwner,
		MaxPeerCnt:  maxPeerCnt,
	}
	return this.downloadFileWithOpt(fileHashStr, opt)
}

// GetDownloadQuotation. get peers and the download price of the file. if free flag is set, return price-free peers.
// taskInfo. task info data struct
// opt. download options
func (this *Dsp) GetDownloadQuotation(taskInfo *store.TaskInfo, addrs []string) (
	map[string]*file.Payment, error) {
	if len(addrs) == 0 {
		return nil, dspErr.New(dspErr.NO_DOWNLOAD_SEED, "no peer for download")
	}
	// taskId := this.taskMgr.TaskId(fileHashStr, this.chain.WalletAddress(), store.TaskTypeDownload)
	sessions, err := this.taskMgr.GetFileSessions(taskInfo.Id)
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
			this.taskMgr.SetSessionId(taskInfo.Id, session.WalletAddr, session.SessionId)
		}
		log.Debugf("get session from db : %v", peerPayInfos)
		return peerPayInfos, nil
	}
	msg := message.NewFileMsg(taskInfo.FileHash, netcom.FILE_OP_DOWNLOAD_ASK,
		message.WithWalletAddress(this.chain.WalletAddress()),
		message.WithAsset(taskInfo.Asset),
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
		if len(taskInfo.DecryptPwd) > 0 && !utils.VerifyEncryptPassword(taskInfo.DecryptPwd, filePrefix.EncryptSalt,
			filePrefix.EncryptHash) {
			log.Warnf("encrypt password not match hash")
			return false
		}

		if taskInfo.Free && (fileMsg.PayInfo != nil && fileMsg.PayInfo.UnitPrice != 0) {
			return false
		}
		// check block hashes valid
		if utils.ComputeStringHashRoot(fileMsg.BlockHashes) != taskInfo.BlocksRoot {
			return false
		}
		if len(blockHashes) == 0 {
			blockHashes = fileMsg.BlockHashes
		}
		if len(prefix) == 0 {
			prefix = string(fileMsg.Prefix)
		}
		log.Debugf("prefix hex: %s %d, hash len %d", fileMsg.Prefix, fileMsg.TotalBlockCount, len(blockHashes))
		peerPayInfos[addr] = fileMsg.PayInfo
		this.taskMgr.AddFileSession(taskInfo.Id, fileMsg.SessionId, fileMsg.PayInfo.WalletAddress, addr,
			uint32(fileMsg.PayInfo.Asset), fileMsg.PayInfo.UnitPrice)
		return false
	}
	ret, err := client.P2pBroadcast(addrs, msg.ToProtoMsg(), msg.MessageId, reply)
	log.Debugf("broadcast file download msg result %v err %s", ret, err)
	if err != nil {
		return nil, err
	}
	log.Debugf("peer prices:%v", peerPayInfos)
	if len(peerPayInfos) == 0 {
		return nil, dspErr.New(dspErr.REMOTE_PEER_DELETE_FILE, "remote peer has deleted the file")
	}
	if !this.taskMgr.TaskExist(taskInfo.Id) {
		return nil, dspErr.New(dspErr.TASK_NOT_EXIST, "task not exist")
	}
	log.Debugf("get file total count :%d", taskInfo.TotalBlockCount)
	// check has add file block hashes
	if uint64(len(this.taskMgr.FileBlockHashes(taskInfo.Id))) == taskInfo.TotalBlockCount &&
		taskInfo.TotalBlockCount != 0 {
		return peerPayInfos, nil
	}
	// add file block hashes
	log.Debugf("add file blockHashes task id %v hashes %s-%s, prefix %s, prefix len: %d",
		taskInfo.Id, blockHashes[0], blockHashes[len(blockHashes)-1], prefix, len(prefix))
	if err := this.taskMgr.AddFileBlockHashes(taskInfo.Id, blockHashes); err != nil {
		return nil, err
	}
	totalBlockCount := len(blockHashes)
	// update total block count and prefix
	if err := this.taskMgr.SetTaskInfoWithOptions(taskInfo.Id,
		task.Prefix(prefix),
		task.FileSize(getFileSizeWithBlockCount(uint64(totalBlockCount))),
		task.TotalBlockCnt(uint64(totalBlockCount))); err != nil {
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
	log.Debugf("deposit to channel price:%d, cnt:%d, chunksize:%d, total:%d",
		common.FILE_DOWNLOAD_UNIT_PRICE, totalCount, common.CHUNK_SIZE, totalAmount)
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

// PayForBlock. Pay for block with media transfer. PayInfo is some payment info of receiver,
// addr is a host address of receiver.
// BlockSize is the size to pay in KB. If newpayment flag is set, it is set to a new payment
// and will accumulate to unpaid amount.
// PaymentId, a unique id for payment, if it's zero. It will be generated by a random integer
func (this *Dsp) PayForBlock(payInfo *file.Payment, addr, fileHashStr string, blockSize uint64,
	paymentId int32, newPayment bool) (int32, error) {
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
		if err := this.taskMgr.AddFileUnpaid(taskId, payInfo.WalletAddress, paymentId, payInfo.Asset,
			amount); err != nil {
			return 0, err
		}
	}

	// use default dns to pay
	if err := this.checkDNSState(this.dns.DNSNode.WalletAddr); err == nil {
		log.Debugf("paying for file %s, to %s, id %v, use dns: %s",
			fileHashStr, payInfo.WalletAddress, paymentId, this.dns.DNSNode.WalletAddr)
		if err := this.channel.MediaTransfer(paymentId, amount, this.dns.DNSNode.WalletAddr,
			payInfo.WalletAddress); err == nil {
			// clean unpaid order
			if err := this.taskMgr.DeleteFileUnpaid(taskId, payInfo.WalletAddress, paymentId,
				payInfo.Asset, amount); err != nil {
				return 0, err
			}
			log.Debugf("delete unpaid %d", amount)
			// active peer to prevent pay too long
			this.taskMgr.ActiveDownloadTaskPeer(addr)
			return paymentId, nil
		} else {
			this.taskMgr.ActiveDownloadTaskPeer(addr)
			log.Debugf("mediaTransfer failed paymentId %d, payTo: %s, err %s",
				paymentId, payInfo.WalletAddress, err)
		}
	}

	// use other dns to pay
	allCh, err := this.channel.AllChannels()
	if err != nil {
		return 0, err
	}
	paySuccess := false
	for _, ch := range allCh.Channels {
		if ch.Address == this.dns.DNSNode.WalletAddr {
			continue
		}
		if err := this.checkDNSState(ch.Address); err == nil {
			continue
		}
		log.Debugf("paying for file %s, to %s, id %v, use dns: %s", fileHashStr, payInfo.WalletAddress, paymentId, ch.Address)
		if err := this.channel.MediaTransfer(paymentId, amount, ch.Address, payInfo.WalletAddress); err != nil {
			log.Debugf("mediaTransfer failed paymentId %d, payTo: %s, err %s", paymentId, payInfo.WalletAddress, err)
			this.taskMgr.ActiveDownloadTaskPeer(addr)
			continue
		}
		this.taskMgr.ActiveDownloadTaskPeer(addr)
		paySuccess = true
		log.Debugf("paying for file %s ,to %s, id %v success", fileHashStr, payInfo.WalletAddress, paymentId)
		break
	}
	if !paySuccess {
		return 0, fmt.Errorf("pay %d failed", paymentId)
	}
	// clean unpaid order
	if err := this.taskMgr.DeleteFileUnpaid(taskId, payInfo.WalletAddress, paymentId, payInfo.Asset,
		amount); err != nil {
		return 0, err
	}
	log.Debugf("delete unpaid %d", amount)
	return paymentId, nil
}

// PayUnpaidFile. pay unpaid order for file
func (this *Dsp) PayUnpaidPayments(taskId, fileHashStr string, quotation map[string]*file.Payment) error {
	this.taskMgr.EmitProgress(taskId, task.TaskDownloadPayForBlocks)
	for hostAddr, payInfo := range quotation {
		payments, _ := this.taskMgr.GetUnpaidPayments(taskId, payInfo.WalletAddress, payInfo.Asset)
		if len(payments) == 0 {
			continue
		}
		for _, payment := range payments {
			log.Debugf("pay to %s of the unpaid amount %d for task %s", payInfo.WalletAddress, payment.Amount, taskId)
			_, err := this.PayForBlock(payInfo, hostAddr, fileHashStr, payment.Amount/payInfo.UnitPrice,
				payment.PaymentId, false)
			if err != nil {
				log.Errorf("pay unpaid payment err %s", err)
			}
		}
	}
	this.taskMgr.EmitProgress(taskId, task.TaskDownloadPayForBlocksDone)
	return nil
}

// DownloadFileWithQuotation. download file, piece by piece from addrs.
// inOrder: if true, the file will be downloaded block by block in order
func (this *Dsp) DownloadFileWithQuotation(taskInfo *store.TaskInfo, quotation map[string]*file.Payment) error {
	// task exist at runtime
	if len(quotation) == 0 {
		return dspErr.New(dspErr.GET_DOWNLOAD_INFO_FAILED_FROM_PEERS,
			"no peer quotation for download: %s", taskInfo.FileHash)
	}
	if taskInfo == nil {
		return dspErr.New(dspErr.FILEINFO_NOT_EXIST, "download info not exist")
	}
	if !this.taskMgr.IsFileInfoExist(taskInfo.Id) {
		return dspErr.New(dspErr.FILEINFO_NOT_EXIST, "download info not exist: %s", taskInfo.FileHash)
	}
	// pay unpaid order of the file after last download
	if err := this.PayUnpaidPayments(taskInfo.Id, taskInfo.FileHash, quotation); err != nil {
		return dspErr.NewWithError(dspErr.PAY_UNPAID_BLOCK_FAILED, err)
	}
	if stop, err := this.taskMgr.IsTaskStop(taskInfo.Id); err != nil || stop {
		return err
	}
	// new download logic
	peerAddrWallet := make(map[string]string, 0)
	wg := sync.WaitGroup{}
	for addr, payInfo := range quotation {
		wg.Add(1)
		sessionId, err := this.taskMgr.GetSessionId(taskInfo.Id, payInfo.WalletAddress)
		if err != nil {
			continue
		}
		peerAddrWallet[addr] = payInfo.WalletAddress
		go func(a string) {
			msg := message.NewFileMsg(taskInfo.FileHash, netcom.FILE_OP_DOWNLOAD,
				message.WithSessionId(sessionId),
				message.WithWalletAddress(this.chain.WalletAddress()),
				message.WithAsset(taskInfo.Asset),
				message.WithSign(this.account),
			)
			log.Debugf("broadcast file_download msg to %v", a)
			broadcastRet, err := client.P2pBroadcast([]string{a}, msg.ToProtoMsg(), msg.MessageId)
			log.Debugf("brocast file download msg %v err %v", broadcastRet, err)
			wg.Done()
		}(addr)
	}
	wg.Wait()
	blockHashes := this.taskMgr.FileBlockHashes(taskInfo.Id)
	log.Debugf("filehashstr:%v, blockhashes-len:%v, prefix:%s", taskInfo.FileHash, len(blockHashes), taskInfo.Prefix)
	fullFilePath := taskInfo.FilePath
	if len(fullFilePath) == 0 {
		if taskInfo.SetFileName {
			fullFilePath = utils.GetFileFullPath(this.config.FsFileRoot, taskInfo.FileHash, taskInfo.FileName,
				utils.GetPrefixEncrypted(taskInfo.Prefix))
		} else {
			fullFilePath = utils.GetFileFullPath(this.config.FsFileRoot, taskInfo.FileHash, "",
				utils.GetPrefixEncrypted(taskInfo.Prefix))
		}
		log.Debugf("get fullFilePath of id :%s, filehashstr %s, filename %s,  path %s",
			taskInfo.Id, taskInfo.FileHash, taskInfo.FileName, fullFilePath)
		if err := this.taskMgr.SetFilePath(taskInfo.Id, fullFilePath); err != nil {
			return err
		}
	}
	if stop, err := this.taskMgr.IsTaskStop(taskInfo.Id); err != nil || stop {
		return err
	}
	this.taskMgr.EmitProgress(taskInfo.Id, task.TaskDownloadFileDownloading)
	// declare job for workers
	job := func(tId, fHash, pAddr, walletAddr string, blocks []*block.Block) ([]*task.BlockResp, error) {
		this.taskMgr.EmitProgress(taskInfo.Id, task.TaskDownloadRequestBlocks)
		startedAt := utils.GetMilliSecTimestamp()
		resp, err := this.downloadBlockFlights(tId, fHash, pAddr, walletAddr, blocks, 1,
			common.DOWNLOAD_BLOCKFLIGHTS_TIMEOUT)
		if err != nil {
			this.taskMgr.UpdateTaskPeerSpeed(taskInfo.Id, pAddr, 0)
			return nil, err
		}
		if resp == nil || len(resp) == 0 {
			this.taskMgr.UpdateTaskPeerSpeed(taskInfo.Id, pAddr, 0)
			return nil, dspErr.New(dspErr.DOWNLOAD_BLOCK_FAILED, "download blocks is nil %s from %s %s, err %s",
				fHash, pAddr, walletAddr, err)
		}
		this.taskMgr.EmitProgress(taskInfo.Id, task.TaskDownloadReceiveBlocks)
		payInfo := quotation[pAddr]
		var totalBytes int
		for _, v := range resp {
			totalBytes += len(v.Block)
		}
		if totalBytes == 0 {
			return nil, dspErr.New(dspErr.DOWNLOAD_BLOCK_FAILED, "request total bytes count 0")
		}
		endedAt := utils.GetMilliSecTimestamp()
		speed := uint64(totalBytes)
		if endedAt > startedAt {
			speed = uint64(totalBytes) / (endedAt - startedAt)
		}
		this.taskMgr.UpdateTaskPeerSpeed(taskInfo.Id, pAddr, speed)
		this.taskMgr.EmitProgress(taskInfo.Id, task.TaskDownloadPayForBlocks)
		log.Debugf("download block of file %s-%s-%d to %s-%s-%d from %s success, start paying to it, "+
			"size is %d speed: %d", fHash, resp[0].Hash, resp[0].Index,
			fHash, resp[len(resp)-1].Hash, resp[len(resp)-1].Index, pAddr, totalBytes, speed)
		paymentId, err := this.PayForBlock(payInfo, pAddr, fHash, uint64(totalBytes), resp[0].PaymentId, true)
		if err != nil {
			log.Errorf("pay for blocks err %s", err)
			this.taskMgr.EmitProgress(taskInfo.Id, task.TaskDownloadPayForBlocksFailed)
			return nil, dspErr.New(dspErr.PAY_UNPAID_BLOCK_FAILED, err.Error())
		}
		this.taskMgr.EmitProgress(taskInfo.Id, task.TaskDownloadPayForBlocksDone)
		log.Debugf("pay for block: %s to %s, wallet: %s success, paymentId: %d",
			fHash, pAddr, walletAddr, paymentId)
		return resp, nil
	}
	this.taskMgr.NewWorkers(taskInfo.Id, peerAddrWallet, taskInfo.InOrder, job)
	go this.taskMgr.WorkBackground(taskInfo.Id)
	log.Debugf("start download file in order %t", taskInfo.InOrder)
	if !taskInfo.InOrder {
		if err := this.receiveBlockNoOrder(taskInfo.Id, peerAddrWallet); err != nil {
			log.Debugf("stack %s", debug.Stack())
			return err
		}
		log.Debugf("will check file hash task id %s, file hash %s, downloaded %t",
			taskInfo.Id, taskInfo.FileHash, this.taskMgr.IsFileDownloaded(taskInfo.Id))
		if this.IsClient() && this.taskMgr.IsFileDownloaded(taskInfo.Id) && !utils.GetPrefixEncrypted(taskInfo.Prefix) {
			this.taskMgr.EmitProgress(taskInfo.Id, task.TaskDownloadCheckingFile)
			checkFileList, err := this.fs.NodesFromFile(fullFilePath, string(taskInfo.Prefix), false, "")
			if err != nil {
				return err
			}
			log.Debugf("checking file hash %s", checkFileList[0])
			if len(checkFileList) == 0 || checkFileList[0] != taskInfo.FileHash {
				this.taskMgr.EmitProgress(taskInfo.Id, task.TaskDownloadCheckingFileFailed)
				err = dspErr.New(dspErr.CHECK_FILE_FAILED, "file hash not match")
				return err
			}
			this.taskMgr.EmitProgress(taskInfo.Id, task.TaskDownloadCheckingFileDone)
		}
		if len(taskInfo.DecryptPwd) == 0 {
			return nil
		}
		if err := this.decryptDownloadedFile(taskInfo.Id); err != nil {
			return err
		}
		return nil
	}
	// TODO: support in-order download
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

// DeleteDownloadedLocalFile. Delete file in local.
func (this *Dsp) DeleteDownloadedLocalFile(fileHash string) error {
	taskId, _ := this.taskMgr.GetDownloadedTaskId(fileHash)
	if len(taskId) == 0 {
		return dspErr.New(dspErr.DELETE_FILE_FAILED, "delete taskId is empty")
	}
	filePath, _ := this.taskMgr.GetFilePath(taskId)
	fileHashStr, _ := this.taskMgr.GetTaskFileHash(taskId)
	err := this.fs.DeleteFile(fileHashStr, filePath)
	if err != nil {
		log.Errorf("fs delete file: %s, path: %s, err: %s", fileHashStr, filePath, err)
		return err
	}
	log.Debugf("delete local file success fileHash:%s, path:%s", fileHashStr, filePath)
	return nil
}

// StartBackupFileService. start a backup file service to find backup jobs.
func (this *Dsp) StartBackupFileService() {
	backupingCnt := 0
	ticker := time.NewTicker(time.Duration(common.BACKUP_FILE_DURATION) * time.Second)
	backupFailedMap := make(map[string]int)
	for {
		select {
		case <-ticker.C:
			if !this.Running() {
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
			log.Debugf("get %d task to back up", len(tasks.Tasks))
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
				log.Debugf("go backup file:%s, from backup svr addr:%s, backupWalletAddr: %s,"+
					"brokenAddr: %s, luckyWalletAddr: %s, luckyIndex: %d",
					t.FileHash, t.BakSrvAddr, t.BackUpAddr.ToBase58(), t.BrokenAddr.ToBase58(),
					t.LuckyAddr.ToBase58(), t.LuckyNum)
				fileInfo, err := this.chain.GetFileInfo(string(t.FileHash))
				if err != nil {
					continue
				}
				if err = this.backupFileFromPeer(fileInfo, string(t.BakSrvAddr), t.LuckyNum,
					t.BakHeight, t.BakNum, t.BrokenAddr); err != nil {
					backupFailedMap[string(t.FileHash)]++
					continue
				}
			}
			log.Debugf("finish backup one round")
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
			if !this.Running() {
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

// checkDNSState. check the p2p state of a dns
func (this *Dsp) checkDNSState(dnsWalletAddr string) error {
	dnsHostAddr, err := this.dns.GetExternalIP(dnsWalletAddr)
	if err != nil {
		return err
	}
	exist, err := client.P2pConnectionExist(dnsHostAddr, client.P2pNetTypeChannel)
	if exist && err == nil {
		return nil
	}
	log.Debugf("DNS connection not exist reconnect...")
	if err = client.P2pReconnectPeer(dnsHostAddr, client.P2pNetTypeChannel); err != nil {
		return err
	}
	if err = client.P2pWaitForConnected(dnsHostAddr,
		time.Duration(common.WAIT_CHANNEL_CONNECT_TIMEOUT)); err != nil {
		return err
	}
	return nil
}

// downloadFileWithOpt. internal helper, download or resume file with hash and options
func (this *Dsp) downloadFileWithOpt(fileHashStr string, opt *common.DownloadOption) error {
	if len(opt.FileName) == 0 {
		// TODO: get file name from chain if the file exists on chain
		info, _ := this.chain.GetFileInfo(fileHashStr)
		if info != nil {
			opt.FileName = string(info.FileDesc)
			opt.BlocksRoot = string(info.BlocksRoot)
			opt.FileOwner = string(info.FileOwner.ToBase58())
		}
	}
	taskId := this.taskMgr.TaskId(fileHashStr, this.chain.WalletAddress(), store.TaskTypeDownload)
	if !this.taskMgr.TaskExist(taskId) {
		if len(taskId) > 0 {
			log.Debugf("task not exist in memory, but has downloaded, start a new download task of id: %s, file %s",
				taskId, fileHashStr)
		} else {
			log.Debugf("start a new download task of file %s", fileHashStr)
		}
		return this.DownloadFile("", fileHashStr, opt)
	}
	if taskDone, _ := this.taskMgr.IsTaskDone(taskId); taskDone {
		log.Debugf("task has done, start a new download task of id: %s, file %s", taskId, fileHashStr)
		return this.DownloadFile("", fileHashStr, opt)
	}
	if taskPreparing, taskDoing, _ := this.taskMgr.IsTaskPreparingOrDoing(taskId); taskPreparing || taskDoing {
		return dspErr.New(dspErr.DOWNLOAD_REFUSED, "task exists, and it is preparing or doing")
	}
	log.Debugf("task exists, resume the download task of id: %s, file %s", taskId, fileHashStr)
	return this.DownloadFile(taskId, fileHashStr, opt)
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
	taskInfo, err := this.taskMgr.GetTaskInfoCopy(taskId)
	if err != nil {
		return err
	}
	if taskInfo == nil {
		return dspErr.New(dspErr.GET_FILEINFO_FROM_DB_ERROR, "can't find download options, please retry")
	}
	if len(taskInfo.FileHash) == 0 {
		return dspErr.New(dspErr.GET_FILEINFO_FROM_DB_ERROR, "filehash not found %s", taskId)
	}
	log.Debugf("resume download file %s", taskId)
	opt := &common.DownloadOption{
		FileName:    taskInfo.FileName,
		FileOwner:   taskInfo.FileOwner,
		BlocksRoot:  taskInfo.BlocksRoot,
		Asset:       taskInfo.Asset,
		InOrder:     taskInfo.InOrder,
		DecryptPwd:  taskInfo.DecryptPwd,
		Free:        taskInfo.Free,
		SetFileName: taskInfo.SetFileName,
		MaxPeerCnt:  taskInfo.MaxPeerCnt,
		Url:         taskInfo.Url,
	}
	// TODO: record original workers
	go this.DownloadFile(taskId, taskInfo.FileHash, opt)
	return nil
}

// receiveBlockNoOrder. receive blocks in order
func (this *Dsp) receiveBlockNoOrder(taskId string, peerAddrWallet map[string]string) error {
	taskInfo, err := this.taskMgr.GetTaskInfoCopy(taskId)
	if err != nil || taskInfo == nil {
		return dspErr.New(dspErr.FILEINFO_NOT_EXIST, "task info is nil")
	}
	if this.taskMgr.IsFileDownloaded(taskInfo.Id) {
		log.Debugf("task %s, file %s has downloaded", taskInfo.Id, taskInfo.FileHash)
		return nil
	}
	if err := this.addDownloadBlockReq(taskInfo.Id, taskInfo.FileHash); err != nil {
		return dspErr.NewWithError(dspErr.GET_UNDOWNLOADED_BLOCK_FAILED, err)
	}
	hasCutPrefix := false
	prefix := string(taskInfo.Prefix)
	isFileEncrypted := utils.GetPrefixEncrypted(taskInfo.Prefix)
	var file *os.File
	if this.config.FsType == config.FS_FILESTORE {
		var createFileErr error
		file, createFileErr = createDownloadFile(this.config.FsFileRoot, taskInfo.FilePath)
		if createFileErr != nil {
			return dspErr.NewWithError(dspErr.CREATE_DOWNLOAD_FILE_FAILED, createFileErr)
		}
		defer func() {
			log.Debugf("close file")
			file.Close()
		}()
	}
	if file == nil {
		return dspErr.New(dspErr.CREATE_DOWNLOAD_FILE_FAILED, "create file failed")
	}
	stateCheckTicker := time.NewTicker(time.Duration(common.TASK_STATE_CHECK_DURATION) * time.Second)
	defer stateCheckTicker.Stop()
	for {
		select {
		case value, ok := <-this.taskMgr.TaskNotify(taskInfo.Id):
			if !ok {
				return dspErr.New(dspErr.DOWNLOAD_BLOCK_FAILED, "download internal error")
			}
			if stop, err := this.taskMgr.IsTaskStop(taskInfo.Id); err != nil || stop {
				log.Debugf("stop download task %s", taskInfo.Id)
				return err
			}
			log.Debugf("received block %s-%s-%d from %s", taskInfo.FileHash, value.Hash, value.Index, value.PeerAddr)
			if this.taskMgr.IsBlockDownloaded(taskInfo.Id, value.Hash, uint64(value.Index)) &&
				!this.taskMgr.IsFileDownloaded(taskInfo.Id) {
				log.Debugf("%s-%s-%d is downloaded", taskInfo.FileHash, value.Hash, value.Index)
				continue
			}
			block := this.fs.EncodedToBlockWithCid(value.Block, value.Hash)
			links, err := this.fs.GetBlockLinks(block)
			if err != nil {
				return err
			}
			if block.Cid().String() != value.Hash {
				log.Warnf("receive a unmatched hash block %s %s", block.Cid().String(), value.Hash)
			}
			if block.Cid().String() == taskInfo.FileHash && this.config.FsType == config.FS_FILESTORE {
				if !isFileEncrypted {
					this.fs.SetFsFilePrefix(taskInfo.FilePath, prefix)
				} else {
					this.fs.SetFsFilePrefix(taskInfo.FilePath, "")
				}
			}
			if len(links) == 0 && this.config.FsType == config.FS_FILESTORE {
				data := this.fs.BlockData(block)
				// cut prefix
				// TEST: why not use filesize == 0
				if !isFileEncrypted && !hasCutPrefix && len(data) >= len(prefix) &&
					string(data[:len(prefix)]) == prefix {
					log.Debugf("cut prefix data-len %d, prefix %s, prefix-len: %d, str %s",
						len(data), prefix, len(prefix), string(data[:len(prefix)]))
					data = data[len(prefix):]
					hasCutPrefix = true
				}
				// TEST: offset
				writeAtPos := value.Offset
				if value.Offset > 0 && !isFileEncrypted {
					writeAtPos = value.Offset - int64(len(prefix))
				}
				log.Debugf("block %s block-len %d, offset %v prefix %v pos %d",
					block.Cid().String(), len(data), value.Offset, len(prefix), writeAtPos)
				if _, err := file.WriteAt(data, writeAtPos); err != nil {
					return dspErr.NewWithError(dspErr.WRITE_FILE_DATA_FAILED, err)
				}
			}
			if this.config.FsType == config.FS_FILESTORE {
				err = this.fs.PutBlockForFileStore(taskInfo.FilePath, block, uint64(value.Offset))
				log.Debugf("put block for store err %v", err)
			} else {
				err = this.fs.PutBlock(block)
				if err != nil {
					return err
				}
				log.Debugf("block %s value.index %d, value.tag:%d", block.Cid(), value.Index, len(value.Tag))
				err = this.fs.PutTag(block.Cid().String(), taskInfo.FileHash, uint64(value.Index), value.Tag)
			}
			log.Debugf("put block for file %s block: %s, offset:%d", taskInfo.FilePath, block.Cid(), value.Offset)
			if err != nil {
				log.Errorf("put block err %s", err)
				return err
			}
			if err := this.taskMgr.SetBlockDownloaded(taskInfo.Id, value.Hash, value.PeerAddr, value.Index,
				value.Offset, links); err != nil {
				return err
			}
			this.taskMgr.EmitProgress(taskInfo.Id, task.TaskDownloadFileDownloading)
			log.Debugf("%s-%s-%d set downloaded", taskInfo.FileHash, value.Hash, value.Index)
			poolLen, err := this.taskMgr.GetBlockReqPoolLen(taskInfo.Id)
			if err != nil {
				return err
			}
			if poolLen != 0 {
				continue
			}
			if !this.taskMgr.IsFileDownloaded(taskInfo.Id) {
				continue
			}
			log.Debugf("file has downloaded: %t, last block index: %d",
				this.taskMgr.IsFileDownloaded(taskInfo.Id), value.Index)
			// last block
			this.taskMgr.SetTaskState(taskInfo.Id, store.TaskStateDone)
			break
		case <-stateCheckTicker.C:
			if stop, err := this.taskMgr.IsTaskStop(taskInfo.Id); err != nil || stop {
				log.Debugf("stop download task %s", taskInfo.Id)
				return err
			}
			if timeout, _ := this.taskMgr.IsTaskTimeout(taskInfo.Id); !timeout {
				continue
			}
			this.taskMgr.SetTaskState(taskInfo.Id, store.TaskStateFailed)
			workerState := this.taskMgr.GetTaskWorkerState(taskInfo.Id)
			for addr, state := range workerState {
				log.Debugf("download timeout worker addr: %s, working : %t, unpaid: %t, totalFailed %v",
					addr, state.Working, state.Unpaid, state.TotalFailed)
			}
			if paidFail, _ := this.taskMgr.AllPeerPaidFailed(taskInfo.Id); paidFail {
				log.Errorf("Download file %s failed, pay failed to all peers", taskInfo.Id)
				return dspErr.New(dspErr.DOWNLOAD_FILE_TIMEOUT, "Download file failed, pay failed to all peers")
			}
			return dspErr.New(dspErr.DOWNLOAD_FILE_TIMEOUT, "Download file timeout")
		}
		done, err := this.taskMgr.IsTaskDone(taskInfo.Id)
		if err != nil {
			return err
		}
		if done {
			log.Debugf("download task is done")
			break
		}
	}
	this.taskMgr.EmitProgress(taskInfo.Id, task.TaskDownloadFileMakeSeed)
	go this.dns.PushToTrackers(taskInfo.FileHash, this.dns.TrackerUrls, client.P2pGetPublicAddr())
	for addr, walletAddr := range peerAddrWallet {
		sessionId, err := this.taskMgr.GetSessionId(taskInfo.Id, walletAddr)
		if err != nil {
			continue
		}
		go func(a, w, sid string) {
			fileDownloadOkMsg := message.NewFileMsg(taskInfo.FileHash, netcom.FILE_OP_DOWNLOAD_OK,
				message.WithSessionId(sid),
				message.WithWalletAddress(this.chain.WalletAddress()),
				message.WithAsset(taskInfo.Asset),
				message.WithSign(this.account),
			)
			client.P2pBroadcast([]string{a}, fileDownloadOkMsg.ToProtoMsg(), fileDownloadOkMsg.MessageId)
		}(addr, walletAddr, sessionId)
	}
	return nil
}

func (this *Dsp) decryptDownloadedFile(taskId string) error {
	taskInfo, err := this.taskMgr.GetTaskInfoCopy(taskId)
	if err != nil || taskInfo != nil {
		return dspErr.New(dspErr.FILEINFO_NOT_EXIST, "task %s not exist", taskId)
	}
	if len(taskInfo.DecryptPwd) == 0 {
		return dspErr.New(dspErr.DECRYPT_FILE_FAILED, "no decrypt password")
	}
	filePrefix := &utils.FilePrefix{}
	sourceFile, err := os.Open(taskInfo.FilePath)
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
	if !utils.VerifyEncryptPassword(taskInfo.DecryptPwd, filePrefix.EncryptSalt, filePrefix.EncryptHash) {
		return dspErr.New(dspErr.DECRYPT_WRONG_PASSWORD, "wrong password")
	}
	newFilePath := utils.GetDecryptedFilePath(taskInfo.FilePath, taskInfo.FileName)
	if err := this.fs.AESDecryptFile(taskInfo.FilePath, string(prefix),
		taskInfo.DecryptPwd, newFilePath); err != nil {
		return dspErr.New(dspErr.DECRYPT_FILE_FAILED, err.Error())
	}
	// return this.taskMgr.SetTaskInfoWithOptions(taskId, task.FilePath(newFilePath))
	return nil
}

func (this *Dsp) addDownloadBlockReq(taskId, fileHashStr string) error {
	hashes, indexMap, err := this.taskMgr.GetUndownloadedBlockInfo(taskId, fileHashStr)
	log.Debugf("undownload hashes %v", hashes)
	if err != nil {
		return err
	}
	reqs := make([]*task.GetBlockReq, 0)
	if len(hashes) == 0 {
		// TODO: check bug
		if this.taskMgr.IsFileDownloaded(fileHashStr) {
			log.Debugf("no undownloaded block %s %v", hashes, indexMap)
			return nil
		}
		log.Warnf("all block has downloaded, but file not downloed")
		reqs = append(reqs, &task.GetBlockReq{
			FileHash: fileHashStr,
			Hash:     fileHashStr,
			Index:    0,
		})
		if err := this.taskMgr.AddBlockReq(taskId, reqs); err != nil {
			return err
		}
		return nil
	}
	log.Debugf("start download at %s-%s-%d", fileHashStr, hashes[0], indexMap[hashes[0]])

	blockIndex := uint64(0)
	for _, hash := range hashes {
		blockIndex = indexMap[hash]
		reqs = append(reqs, &task.GetBlockReq{
			FileHash: fileHashStr,
			Hash:     hash,
			Index:    blockIndex,
		})
	}
	return this.taskMgr.AddBlockReq(taskId, reqs)
}

// downloadFileFromPeers. downloadfile base methods. download file from peers.
func (this *Dsp) downloadFileFromPeers(taskInfo *store.TaskInfo, addrs []string) error {
	quotation, err := this.GetDownloadQuotation(taskInfo, addrs)
	log.Debugf("downloadFileFromPeers :%v", quotation)
	if err != nil {
		return dspErr.New(dspErr.GET_DOWNLOAD_INFO_FAILED_FROM_PEERS, err.Error())
	}
	if len(quotation) == 0 {
		return dspErr.New(dspErr.NO_DOWNLOAD_SEED, "no quotation from peers")
	}
	if stop, err := this.taskMgr.IsTaskStop(taskInfo.Id); err != nil || stop {
		log.Debugf("stop download task %s", taskInfo.Id)
		return err
	}
	if !taskInfo.Free && len(addrs) > taskInfo.MaxPeerCnt {
		log.Debugf("filter peers free %t len %d max %d", taskInfo.Free, len(addrs), taskInfo.MaxPeerCnt)
		// filter peers
		quotation = utils.SortPeersByPrice(quotation, taskInfo.MaxPeerCnt)
	}
	if err := this.DepositChannelForFile(taskInfo.FileHash, quotation); err != nil {
		return dspErr.New(dspErr.PREPARE_CHANNEL_ERROR, err.Error())
	}
	if stop, err := this.taskMgr.IsTaskStop(taskInfo.Id); err != nil || stop {
		log.Debugf("stop download task %s", taskInfo.Id)
		return err
	}
	log.Debugf("set up channel success: %v", quotation)
	// get new task info because task info has updated
	newTaskInfo, err := this.taskMgr.GetTaskInfoCopy(taskInfo.Id)
	if err != nil {
		return err
	}
	return this.DownloadFileWithQuotation(newTaskInfo, quotation)
}

// [Deprecated] startFetchBlocks for store node, fetch blocks one by one after receive fetch_rdy msg
func (this *Dsp) startFetchBlocks(fileHashStr string, addr, peerWalletAddr string) error {
	// my task. use my wallet address
	taskId := this.taskMgr.TaskId(fileHashStr, this.chain.WalletAddress(), store.TaskTypeDownload)
	log.Debugf("startFetchBlocks taskId: %s, fileHash: %s", taskId, fileHashStr)
	if this.taskMgr.IsFileDownloaded(taskId) {
		log.Debugf("file has downloaded: %s", fileHashStr)
		return this.fs.StartPDPVerify(fileHashStr, 0, 0, 0, chainCom.ADDRESS_EMPTY)
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
		if this.taskMgr.IsBlockDownloaded(taskId, hash, uint64(index)) {
			log.Debugf("block has downloaded %s %d", hash, index)
			blk := this.fs.GetBlock(hash)
			links, err := this.fs.GetBlockLinks(blk)
			if err != nil {
				log.Errorf("get block links err %s", err)
				return err
			}
			blockHashes = append(blockHashes, links...)
			continue
		}
		blocks = append(blocks, &block.Block{
			SessionId: sessionId,
			Index:     uint64(index),
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
		resps, err = this.downloadBlockFlights(taskId, fileHashStr, addr, peerWalletAddr, blocks,
			common.MAX_BLOCK_FETCHED_RETRY, common.DOWNLOAD_FILE_TIMEOUT)
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
			err = this.taskMgr.SetBlockDownloaded(taskId, value.Hash, value.PeerAddr,
				value.Index, value.Offset, nil)
			if err != nil {
				log.Errorf("SetBlockDownloaded taskId:%s, %s-%s-%d-%d, err: %s",
					taskId, fileHashStr, value.Hash, value.Index, value.Offset, err)
				return err
			}
			log.Debugf("SetBlockDownloaded taskId:%s, %s-%s-%d-%d",
				taskId, fileHashStr, value.Hash, value.Index, value.Offset)
			links, err := this.fs.GetBlockLinks(blk)
			if err != nil {
				log.Errorf("get block links err %s", err)
				return err
			}
			blockHashes = append(blockHashes, links...)
			log.Debugf("blockHashes len: %d", len(blockHashes))
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
	log.Debugf("push to trackers %v", this.dns.TrackerUrls)
	this.dns.PushToTrackers(fileHashStr, this.dns.TrackerUrls, client.P2pGetPublicAddr())
	// TODO: remove unused file info fields after prove pdp success
	this.taskMgr.EmitResult(taskId, "", nil)
	this.taskMgr.DeleteTask(taskId)
	return nil
}

func (this *Dsp) putBlocks(taskId, fileHashStr, peerAddr string, resps []*task.BlockResp) error {
	log.Debugf("putBlocks taskId: %s, fileHash: %s", taskId, fileHashStr)
	if len(fileHashStr) != 0 {
		defer this.fs.PinRoot(context.TODO(), fileHashStr)
	}
	if this.taskMgr.IsFileDownloaded(taskId) {
		log.Debugf("file has downloaded: %s", fileHashStr)
		err := this.fs.StartPDPVerify(fileHashStr, 0, 0, 0, chainCom.ADDRESS_EMPTY)
		if err != nil {
			return err
		}
		// dispatch downloaded blocks to other primary nodes
		dispatchTaskId := this.taskMgr.TaskId(fileHashStr, this.WalletAddress(), store.TaskTypeUpload)
		go this.dispatchBlocks(dispatchTaskId, taskId, fileHashStr)
		return nil
	}

	for _, value := range resps {
		if len(value.Block) == 0 {
			log.Errorf("receive empty block %d of file %s", len(value.Block), fileHashStr)
			return fmt.Errorf("receive empty block %d of file %s", len(value.Block), fileHashStr)
		}
		blk := this.fs.EncodedToBlockWithCid(value.Block, value.Hash)
		if blk.Cid().String() != value.Hash {
			return fmt.Errorf("receive a wrong block: %s, expected: %s", blk.Cid().String(), value.Hash)
		}
		if len(value.Tag) == 0 {
			log.Errorf("receive empty tag %d of file %s and block %s",
				len(value.Tag), fileHashStr, blk.Cid().String())
			return fmt.Errorf("receive empty tag %d of file %s and block %s",
				len(value.Tag), fileHashStr, blk.Cid().String())
		}
		if err := this.fs.PutBlock(blk); err != nil {
			return err
		}
		if err := this.fs.PutTag(value.Hash, fileHashStr, uint64(value.Index), value.Tag); err != nil {
			return err
		}

		if err := this.taskMgr.SetBlockDownloaded(taskId, value.Hash,
			value.PeerAddr, value.Index, value.Offset, nil); err != nil {
			log.Errorf("SetBlockDownloaded taskId:%s, %s-%s-%d-%d, err: %s",
				taskId, fileHashStr, value.Hash, value.Index, value.Offset, err)
			return err
		}
		log.Debugf("SetBlockDownloaded taskId:%s, %s-%s-%d-%d",
			taskId, fileHashStr, value.Hash, value.Index, value.Offset)
	}
	if !this.taskMgr.IsFileDownloaded(taskId) {
		return nil
	}
	blockHashes := this.taskMgr.FileBlockHashes(taskId)
	log.Debugf("file block hashes %d", len(blockHashes))
	if err := this.fs.SetFsFileBlockHashes(fileHashStr, blockHashes); err != nil {
		log.Errorf("set file blockhashes err %s", err)
		return err
	}
	// all block is saved, prove it
	proved := false
	for i := 0; i < common.MAX_START_PDP_RETRY; i++ {
		log.Infof("received all block, start pdp verify %s, retry: %d", fileHashStr, i)
		if err := this.fs.StartPDPVerify(fileHashStr, 0, 0, 0, chainCom.ADDRESS_EMPTY); err == nil {
			proved = true
			break
		} else {
			log.Errorf("start pdp verify err %s", err)
		}
		time.Sleep(time.Duration(common.START_PDP_RETRY_DELAY) * time.Second)
	}
	if !proved {
		if err := this.DeleteDownloadedFile(taskId); err != nil {
			return err
		}
		return nil
	}
	log.Debugf("pdp verify %s success, push the file to trackers %v", fileHashStr, this.dns.TrackerUrls)
	this.dns.PushToTrackers(fileHashStr, this.dns.TrackerUrls, client.P2pGetPublicAddr())
	// TODO: remove unused file info fields after prove pdp success
	this.taskMgr.EmitResult(taskId, "", nil)
	this.taskMgr.DeleteTask(taskId)
	if err := this.fs.PinRoot(context.TODO(), fileHashStr); err != nil {
		log.Errorf("pin root file err %s", err)
		return err
	}
	// dispatch downloaded blocks to other primary nodes
	go this.dispatchBlocks("", taskId, fileHashStr)
	doneMsg := message.NewFileMsg(fileHashStr, netcom.FILE_OP_FETCH_DONE,
		message.WithSessionId(taskId),
		message.WithWalletAddress(this.chain.WalletAddress()),
		message.WithSign(this.account),
	)
	return client.P2pSend(peerAddr, doneMsg.MessageId, doneMsg.ToProtoMsg())
}

// downloadBlockUnits. download block helper function for client.
func (this *Dsp) downloadBlockFlights(taskId, fileHashStr, ipAddr, peerWalletAddr string, blockReqs []*block.Block,
	retry, timeout uint32) ([]*task.BlockResp, error) {
	sessionId, err := this.taskMgr.GetSessionId(taskId, peerWalletAddr)
	if err != nil {
		return nil, err
	}
	if len(blockReqs) > 0 {
		log.Debugf("download block of task: %s %s-%s-%d to %s-%s-%d to %s",
			taskId, fileHashStr, blockReqs[0].Hash, blockReqs[0].Index, fileHashStr,
			blockReqs[len(blockReqs)-1].Hash, blockReqs[len(blockReqs)-1].Index, ipAddr)
	}
	timeStamp := time.Now().UnixNano()
	log.Debugf("download block timestamp %d", timeStamp)
	msg := message.NewBlockFlightsReqMsg(blockReqs, timeStamp)
	blockReqM := make(map[string]struct{}, 0)
	for _, req := range blockReqs {
		blockReqM[keyOfBlockHashAndIndex(req.Hash, req.Index)] = struct{}{}
	}
	for i := uint32(0); i < retry; i++ {
		log.Debugf("send download block flights msg sessionId %s of %s from %s,  retry: %d",
			sessionId, fileHashStr, ipAddr, i)
		resp, err := client.P2pSendAndWaitReply(ipAddr, msg.MessageId, msg.ToProtoMsg())
		if err != nil {
			log.Errorf("send download block flights msg err: %s", err)
			continue
		}
		msg := message.ReadMessage(resp)
		if msg == nil {
			return nil, fmt.Errorf("receive invalid msg from peer %s", ipAddr)
		}
		if msg.Payload == nil {
			log.Error("receive empty block flights of task %s", taskId)
			return nil, fmt.Errorf("receive empty block flights of task %s", taskId)
		}
		blockFlightsMsg := msg.Payload.(*block.BlockFlights)
		if blockFlightsMsg == nil || len(blockFlightsMsg.Blocks) == 0 {
			log.Errorf("receive empty block flights, err %v", msg.Error)
			return nil, fmt.Errorf("receive empty block flights of task %s", taskId)
		}
		log.Debugf("taskId: %s, sessionId: %s receive %d blocks from peer:%s",
			taskId, blockFlightsMsg.Blocks[0].SessionId, len(blockFlightsMsg.Blocks), ipAddr)
		// active worker
		this.taskMgr.ActiveDownloadTaskPeer(ipAddr)
		blockResps := make([]*task.BlockResp, 0)
		for _, blockMsg := range blockFlightsMsg.Blocks {
			if _, ok := blockReqM[keyOfBlockHashAndIndex(blockMsg.Hash, blockMsg.Index)]; !ok {
				log.Warnf("block %s-%d is not my request task", blockMsg.Hash, blockMsg.Index)
				continue
			}
			block := this.fs.EncodedToBlockWithCid(blockMsg.Data, blockMsg.Hash)
			if block.Cid().String() != blockMsg.Hash {
				log.Warnf("receive wrong block %s-%d", blockMsg.Hash, blockMsg.Index)
				continue
			}
			blockResps = append(blockResps, &task.BlockResp{
				Hash:      blockMsg.Hash,
				Index:     blockMsg.Index,
				PeerAddr:  ipAddr,
				Block:     blockMsg.Data,
				Tag:       blockMsg.Tag,
				Offset:    blockMsg.Offset,
				PaymentId: blockFlightsMsg.PaymentId,
			})
		}
		if len(blockResps) == 0 {
			log.Warnf("blockResps is not match request")
		}
		return blockResps, nil
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
		fullFilePath := utils.GetFileFullPath(this.config.FsFileRoot, fileHashStr, fileName,
			utils.GetPrefixEncrypted([]byte(prefix)))
		if err := this.taskMgr.SetTaskInfoWithOptions(taskId, task.Prefix(prefix),
			task.FileName(fullFilePath)); err != nil {
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
		if this.taskMgr.IsBlockDownloaded(taskId, hash, uint64(index)) {
			log.Debugf("%s-%s-%d is downloaded", fileHashStr, hash, index)
			continue
		}
		block := this.fs.GetBlock(hash)
		links, err := this.fs.GetBlockLinks(block)
		if err != nil {
			return err
		}
		offsetKey := fmt.Sprintf("%s-%d", hash, index)
		offset := offsets[offsetKey]
		log.Debugf("hash: %s-%s-%d , offset: %d, links count %d", fileHashStr, hash, index, offset, len(links))
		if err := this.taskMgr.SetBlockDownloaded(taskId, fileHashStr, client.P2pGetPublicAddr(), uint64(index),
			int64(offset), links); err != nil {
			return err
		}
	}
	go this.dns.PushToTrackers(fileHashStr, this.dns.TrackerUrls, client.P2pGetPublicAddr())
	return nil
}

// backupFileFromPeer. backup a file from peer
func (this *Dsp) backupFileFromPeer(fileInfo *fs.FileInfo, peer string, luckyNum, bakHeight, bakNum uint64,
	brokenAddr chainCom.Address) (err error) {
	fileHashStr := string(fileInfo.FileHash)
	if fileInfo == nil || len(fileHashStr) == 0 {
		return dspErr.New(dspErr.DOWNLOAD_FILEHASH_NOT_FOUND, "no filehash for download")
	}
	taskId := this.taskMgr.TaskId(fileHashStr, this.chain.WalletAddress(), store.TaskTypeDownload)
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
	taskDone, _ := this.taskMgr.IsTaskDone(taskId)
	if taskDone {
		return nil
	}
	if this.dns.DNSNode == nil {
		return dspErr.New(dspErr.NO_CONNECTED_DNS, "no online dns node")
	}
	if err = this.taskMgr.SetTaskState(taskId, store.TaskStateDoing); err != nil {
		return err
	}
	log.Debugf("download file dns node %s", this.dns.DNSNode.WalletAddr)
	if err = this.taskMgr.SetTaskInfoWithOptions(taskId,
		task.FileHash(fileHashStr),
		task.Asset(common.ASSET_USDT),
		task.Inorder(false),
		task.MaxPeerCnt(common.MAX_DOWNLOAD_PEERS_NUM),
		task.FileName(string(fileInfo.FileDesc)),
		task.FileOwner(fileInfo.FileOwner.ToBase58()),
		task.Walletaddr(this.chain.WalletAddress())); err != nil {
		return err
	}
	if err = this.taskMgr.BindTaskId(taskId); err != nil {
		return err
	}
	// TODO: test back up logic
	taskInfo, err := this.taskMgr.GetTaskInfoCopy(taskId)
	if err != nil {
		return err
	}
	if err = this.downloadFileFromPeers(taskInfo, []string{peer}); err != nil {
		log.Errorf("download file err %s", err)
		return err
	}
	if err = this.fs.PinRoot(context.TODO(), fileHashStr); err != nil {
		log.Errorf("pin root file err %s", err)
		return err
	}
	if err = this.fs.StartPDPVerify(fileHashStr, luckyNum, bakHeight, bakNum, brokenAddr); err != nil {
		if err := this.DeleteDownloadedFile(taskId); err != nil {
			log.Errorf("delete downloaded file of task failed %v, err %s", taskId, err)
			return err
		}
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
	log.Debugf("create download file dir:%s, path: %s, file: %v %s", dir, filePath, file, err)
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
