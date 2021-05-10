package dsp

import (
	"fmt"
	"strings"
	"time"

	"github.com/saveio/dsp-go-sdk/actor/client"
	"github.com/saveio/dsp-go-sdk/network/message/types/payment"
	"github.com/saveio/dsp-go-sdk/network/message/types/progress"

	"github.com/saveio/carrier/network"
	"github.com/saveio/dsp-go-sdk/common"
	serr "github.com/saveio/dsp-go-sdk/error"
	netcom "github.com/saveio/dsp-go-sdk/network/common"
	"github.com/saveio/dsp-go-sdk/network/message"
	"github.com/saveio/dsp-go-sdk/network/message/types/block"
	"github.com/saveio/dsp-go-sdk/network/message/types/file"
	"github.com/saveio/dsp-go-sdk/store"
	"github.com/saveio/dsp-go-sdk/task"
	chainCom "github.com/saveio/themis/common"
	"github.com/saveio/themis/common/log"
)

// Receive p2p message receive router
func (this *Dsp) Receive(ctx *network.ComponentContext, peerWalletAddr string) {
	msg := message.ReadMessage(ctx.Message())
	peer := ctx.Client()
	if msg == nil || msg.Header == nil {
		log.Debugf("receive nil msg from %s", peerWalletAddr)
		return
	}
	log.Debugf("received msg from peer %s, msgId %s", peer.ClientID(), msg.MessageId)
	if msg.Header.Version != netcom.MESSAGE_VERSION {
		log.Debugf("unrecognized msg version %s", msg.Header.Version)
		return
	}
	log.Debugf("receive msg version:%s, type %s, len: %d",
		msg.Header.Version, msg.Header.Type, msg.Header.MsgLength)
	switch msg.Header.Type {
	case netcom.MSG_TYPE_FILE:
		this.handleFileMsg(ctx, peerWalletAddr, msg)
	case netcom.MSG_TYPE_BLOCK_FLIGHTS:
		this.handleBlockFlightsMsg(ctx, peerWalletAddr, msg)
	case netcom.MSG_TYPE_PROGRESS:
		this.handleProgressMsg(ctx, peerWalletAddr, msg)
	case netcom.MSG_TYPE_PAYMENT:
		this.handlePaymentMsg(ctx, peerWalletAddr, msg)
	default:
		log.Debugf("unrecognized msg type %s", msg.Header.Type)
	}
}

func (this *Dsp) handlePaymentMsg(ctx *network.ComponentContext, peerWalletAddr string, msg *message.Message) {
	paymentMsg := msg.Payload.(*payment.Payment)

	event, err := this.chain.GetSmartContractEvent(paymentMsg.TxHash)
	if err != nil {
		log.Errorf("handle payment msg, get smart contract event err %s for tx %s", err, paymentMsg.TxHash)
		// TODO: reply err
	}

	log.Debugf("event %v", event)
	valid := false
	for _, n := range event.Notify {
		log.Debugf("event states %T", n.States)
		s, ok := n.States.(map[string]interface{})
		if !ok {
			log.Errorf("States id convert err %T", n.States)
			continue
		}
		paymentId, ok := s["paymentId"].(uint64)
		if !ok {
			log.Errorf("payment id convert err %T", s["paymentId"])
			continue
		}
		log.Debugf("get payment id from event %s", paymentId)
		if int32(paymentId) == int32(paymentMsg.PaymentId) {
			valid = true
			break
		}
	}
	if !valid {
		// TODO: reply err

	}

	taskId, err := this.taskMgr.GetTaskIdWithPaymentId(int32(paymentMsg.PaymentId))
	if err != nil {
		log.Errorf("get taskId with payment id failed %s", err)
		// TODO: reply err
	}
	fileHashStr, err := this.taskMgr.GetTaskFileHash(taskId)
	if err != nil {
		log.Errorf("get fileHash with task id failed %s", err)
		// TODO: reply err
	}

	// delete record
	err = this.taskMgr.DeleteFileUnpaid(taskId, paymentMsg.Sender, int32(paymentMsg.PaymentId),
		int32(paymentMsg.Asset), uint64(paymentMsg.Amount))
	if err != nil {
		log.Errorf("delete share file info %s", err)
		// TODO: reply err
	}
	downloadTaskId := this.taskMgr.TaskId(fileHashStr, this.chain.WalletAddress(),
		store.TaskTypeDownload)
	fileName, _ := this.taskMgr.GetFileName(downloadTaskId)
	fileOwner, _ := this.taskMgr.GetFileOwner(downloadTaskId)
	log.Debugf("delete unpaid success taskId: %v, fileHash: %v, fileName: %v, owner: %v, sender: %v, amount: %v",
		taskId, fileHashStr, fileName, fileOwner, paymentMsg.Sender, uint64(paymentMsg.Amount))
	this.shareRecordDB.InsertShareRecord(taskId, fileHashStr, fileName, fileOwner,
		paymentMsg.Sender, uint64(paymentMsg.Amount))

	replyMsg := message.NewEmptyMsg(
		message.WithSign(this.account),
		message.WithSyn(msg.MessageId),
	)
	if err := client.P2pSend(peerWalletAddr, replyMsg.MessageId, replyMsg.ToProtoMsg()); err != nil {
		log.Errorf("reply payment ok msg failed", err)
	} else {
		log.Debugf("reply payment ok msg success")
	}
}

// handleFileMsg handle all file msg
func (this *Dsp) handleFileMsg(ctx *network.ComponentContext, peerWalletAddr string, msg *message.Message) {
	fileMsg := msg.Payload.(*file.File)
	switch fileMsg.Operation {
	case netcom.FILE_OP_FETCH_ASK:
		log.Debugf("handleFileMsg fetchAsk %s file %s from peer:%s, length:%d",
			msg.MessageId, fileMsg.Hash, peerWalletAddr, msg.Header.MsgLength)
		this.handleFileAskMsg(ctx, peerWalletAddr, msg)
	case netcom.FILE_OP_FETCH_RDY:
		log.Debugf("handleFileMsg fetchRdy %s file %s from peer:%s, length:%d",
			msg.MessageId, fileMsg.Hash, peerWalletAddr, msg.Header.MsgLength)
		this.handleFileRdyMsg(ctx, peerWalletAddr, msg)
	case netcom.FILE_OP_FETCH_PAUSE:
		log.Debugf("handleFileMsg fetchPause %s file %s from peer:%s, length:%d",
			msg.MessageId, fileMsg.Hash, peerWalletAddr, msg.Header.MsgLength)
		this.handleFileFetchPauseMsg(ctx, peerWalletAddr, msg)
	case netcom.FILE_OP_FETCH_RESUME:
		log.Debugf("handleFileMsg fetchResume %s file %s from peer:%s, length:%d",
			msg.MessageId, fileMsg.Hash, peerWalletAddr, msg.Header.MsgLength)
		this.handleFileFetchResumeMsg(ctx, peerWalletAddr, msg)
	case netcom.FILE_OP_FETCH_DONE:
		log.Debugf("handleFileMsg fetchDone %s file %s from peer:%s, length:%d",
			msg.MessageId, fileMsg.Hash, peerWalletAddr, msg.Header.MsgLength)
		this.handleFileFetchDoneMsg(ctx, peerWalletAddr, msg)
	case netcom.FILE_OP_FETCH_CANCEL:
		log.Debugf("handleFileMsg fetchCancel %s file %s from peer:%s, length:%d",
			msg.MessageId, fileMsg.Hash, peerWalletAddr, msg.Header.MsgLength)
		this.handleFileFetchCancelMsg(ctx, peerWalletAddr, msg)
	case netcom.FILE_OP_DELETE:
		log.Debugf("handleFileMsg delete %s file %s from peer:%s, length:%d",
			msg.MessageId, fileMsg.Hash, peerWalletAddr, msg.Header.MsgLength)
		this.handleFileDeleteMsg(ctx, peerWalletAddr, msg)
	case netcom.FILE_OP_DOWNLOAD_ASK:
		log.Debugf("handleFileMsg downloadAsk %s file %s from peer:%s, length:%d",
			msg.MessageId, fileMsg.Hash, peerWalletAddr, msg.Header.MsgLength)
		this.handleFileDownloadAskMsg(ctx, peerWalletAddr, msg)
	case netcom.FILE_OP_DOWNLOAD:
		log.Debugf("handleFileMsg download %s  file %s from peer:%s, length:%d",
			msg.MessageId, fileMsg.Hash, peerWalletAddr, msg.Header.MsgLength)
		this.handleFileDownloadMsg(ctx, peerWalletAddr, msg)
	case netcom.FILE_OP_DOWNLOAD_OK:
		log.Debugf("handleFileMsg downloadOk %s file %s from peer:%s, length:%d",
			msg.MessageId, fileMsg.Hash, peerWalletAddr, msg.Header.MsgLength)
		this.handleFileDownloadOkMsg(ctx, peerWalletAddr, msg)
	case netcom.FILE_OP_DOWNLOAD_CANCEL:
		log.Debugf("handleFileMsg downloadCancel %s file %s from peer:%s, length:%d",
			msg.MessageId, fileMsg.Hash, peerWalletAddr, msg.Header.MsgLength)
		this.handleFileDownloadCancelMsg(ctx, peerWalletAddr, msg)
	default:
	}
}

func (this *Dsp) handleProgressMsg(ctx *network.ComponentContext, peerWalletAddr string, msg *message.Message) {
	progressMsg := msg.Payload.(*progress.Progress)
	log.Debugf("handleProgressMsg %d of file %s from peer:%s, length:%d",
		progressMsg.Operation, progressMsg.Hash, peerWalletAddr, msg.Header.MsgLength)
	switch progressMsg.Operation {
	case netcom.FILE_OP_PROGRESS_REQ:
		this.handleReqProgressMsg(ctx, peerWalletAddr, msg)
	}
}

// handleFileAskMsg. client send file ask msg to storage node for handshake and share file metadata
func (this *Dsp) handleFileAskMsg(ctx *network.ComponentContext, peerWalletAddr string, msg *message.Message) {
	// try to find a exist task id
	fileMsg := msg.Payload.(*file.File)
	height, _ := this.chain.GetCurrentBlockHeight()
	existTaskId := this.taskMgr.TaskId(fileMsg.Hash, this.chain.WalletAddress(), store.TaskTypeDownload)
	if len(existTaskId) == 0 {
		var replyMsg *message.Message
		if this.taskMgr.GetDoingTaskNum(store.TaskTypeDownload) >= this.config.MaxDownloadTask {
			log.Warnf("current downloading task num exceed %d, reject new task", this.config.MaxDownloadTask)
			replyMsg = message.NewFileMsgWithError(fileMsg.GetHash(), netcom.FILE_OP_FETCH_ACK,
				serr.TOO_MANY_TASKS,
				"",
				message.WithSessionId(fileMsg.SessionId),
				message.WithWalletAddress(this.chain.WalletAddress()),
				message.WithSign(this.account),
				message.ChainHeight(height),
				message.WithSyn(msg.MessageId),
			)
		} else {
			replyMsg = message.NewFileMsg(fileMsg.GetHash(), netcom.FILE_OP_FETCH_ACK,
				message.WithSessionId(fileMsg.SessionId),
				message.WithWalletAddress(this.chain.WalletAddress()),
				message.WithSign(this.account),
				message.ChainHeight(height),
				message.WithSyn(msg.MessageId),
			)
		}
		if err := client.P2pSend(peerWalletAddr, replyMsg.MessageId, replyMsg.ToProtoMsg()); err != nil {
			log.Errorf("send file_ack msg to %s failed %s", peerWalletAddr, err)
			return
		}
		log.Debugf("send file_ack msg to %s success", peerWalletAddr)
		return
	}
	// handle old task
	state, _ := this.taskMgr.GetTaskState(existTaskId)
	log.Debugf("download task exist %s, file %s, state: %d", existTaskId, fileMsg.Hash, state)
	if state == store.TaskStateCancel || state == store.TaskStateFailed || state == store.TaskStateDone {
		log.Warnf("the task has a wrong state of file_ask %s", state)
	}
	newMsg := message.NewFileMsg(fileMsg.GetHash(), netcom.FILE_OP_FETCH_ACK,
		message.WithSessionId(fileMsg.SessionId),
		message.WithWalletAddress(this.chain.WalletAddress()),
		message.WithSign(this.account),
		message.ChainHeight(height),
		message.WithSyn(msg.MessageId),
	)
	if err := client.P2pSend(peerWalletAddr, newMsg.MessageId, newMsg.ToProtoMsg()); err != nil {
		log.Errorf("send exist task %s file_ack msg to %s failed %s",
			existTaskId, peerWalletAddr, err)
		return
	}
	log.Debugf("send exist task %s file_ack msg to %s success",
		existTaskId, peerWalletAddr)
}

// handleFileRdyMsg. client send ready msg to storage node for telling them it's ready for be fetched
func (this *Dsp) handleFileRdyMsg(ctx *network.ComponentContext, peerWalletAddr string, msg *message.Message) {
	fileMsg := msg.Payload.(*file.File)
	replyErr := func(fileHash, synMsgId string, errorCode uint32, errorMsg string) error {
		replyMsg := message.NewFileMsgWithError(fileHash,
			netcom.FILE_OP_FETCH_RDY_OK, errorCode, errorMsg,
			message.WithWalletAddress(this.chain.WalletAddress()),
			message.WithSign(this.account),
			message.WithSyn(synMsgId),
		)
		err := client.P2pSend(peerWalletAddr, replyMsg.MessageId, replyMsg.ToProtoMsg())
		if err != nil {
			log.Errorf("reply rdy ok msg failed %s", err)
		}
		return err
	}
	// check file tx, and if it is confirmed, and file info is still exist
	if fileMsg.Tx == nil {
		replyErr(fileMsg.Hash, msg.MessageId, serr.MISS_UPLOADED_FILE_TX, "upload file tx is required")
		return
	}
	if err := this.waitForTxConfirmed(fileMsg.Tx.Height); err != nil {
		log.Errorf("get block height err %s", err)
		replyErr(fileMsg.Hash, msg.MessageId, serr.CHECK_UPLOADED_TX_ERROR, "get block height err "+err.Error())
		return
	}
	// TODO: check rpc server avaliable
	info, err := this.chain.GetFileInfo(fileMsg.Hash)
	if info == nil {
		log.Errorf("fetch ask file info is nil %s %s, err %s", fileMsg.Hash, fileMsg.PayInfo.WalletAddress, err)
		replyErr(fileMsg.Hash, msg.MessageId, serr.FILEINFO_NOT_EXIST, "fetch ask file info is nil")
		return
	}
	log.Debugf("get file info %s success", fileMsg.Hash)
	// check upload privilege
	if fileMsg.PayInfo == nil || (info.FileOwner.ToBase58() != fileMsg.PayInfo.WalletAddress &&
		len(info.PrimaryNodes.AddrList) > 0 &&
		info.PrimaryNodes.AddrList[0].ToBase58() != fileMsg.PayInfo.WalletAddress) {
		log.Errorf("receive fetch ask msg from wrong account %s", fileMsg.PayInfo.WalletAddress)
		replyErr(fileMsg.Hash, msg.MessageId, serr.NO_PRIVILEGE_TO_UPLOAD, "fetch owner not match")
		return
	}
	if info.FileBlockNum != fileMsg.TotalBlockCount {
		log.Errorf("block number is unmatched %d, %d", len(fileMsg.BlockHashes), info.FileBlockNum)
		replyErr(fileMsg.Hash, msg.MessageId, serr.NO_PRIVILEGE_TO_UPLOAD, "file block number not match")
		return
	}
	if len(info.BlocksRoot) == 0 {
		log.Warnf("file %s has not blocks root", fileMsg.Hash)
	}
	// my task. use my wallet address
	taskId := this.taskMgr.TaskId(fileMsg.Hash, this.chain.WalletAddress(), store.TaskTypeDownload)
	if len(taskId) == 0 {
		// handle new download task. use my wallet address
		var err error
		taskId, err = this.taskMgr.NewTask("", store.TaskTypeDownload)
		log.Debugf("fetch_ask new task %s of file: %s", taskId, fileMsg.Hash)
		if err != nil {
			log.Errorf("new task failed %s", err)
			replyErr(fileMsg.Hash, msg.MessageId, serr.NEW_TASK_FAILED, "new task failed")
			return
		}
	} else {
		storeTx, err := this.taskMgr.GetStoreTx(taskId)
		if err != nil {
			replyErr(fileMsg.Hash, msg.MessageId, serr.GET_TASK_PROPERTY_ERROR, err.Error())
			return
		}
		if storeTx == fileMsg.Tx.Hash {
			fileHashDone := this.taskMgr.IsFileDownloaded(taskId)
			if fileHashDone && this.IsFs() && !this.chain.CheckHasProveFile(fileMsg.Hash, this.Address()) {
				// file has downloaded but not proved
				if err := this.fs.StartPDPVerify(fileMsg.Hash, 0, 0, 0, chainCom.ADDRESS_EMPTY); err != nil {
					replyErr(fileMsg.Hash, msg.MessageId, serr.SET_FILEINFO_DB_ERROR, err.Error())
					return
				}
			}
			state, _ := this.taskMgr.GetTaskState(taskId)
			if state != store.TaskStateDone {
				if fileHashDone {
					this.taskMgr.SetTaskState(taskId, store.TaskStateDone)
				} else {
					// set a new task state
					err := this.taskMgr.SetTaskState(taskId, store.TaskStateDoing)
					log.Debugf("set task state err: %s", err)
				}
			}
		} else {
			log.Debugf("task %s store tx not match %s-%s", taskId, storeTx, fileMsg.Tx.Hash)
			// receive a new file info tx, delete old task info
			if err := this.taskMgr.CleanTask(taskId); err != nil {
				replyErr(fileMsg.Hash, msg.MessageId, serr.SET_FILEINFO_DB_ERROR, err.Error())
				return
			}
			// handle new download task. use my wallet address
			var err error
			taskId, err = this.taskMgr.NewTask("", store.TaskTypeDownload)
			log.Debugf("fetch_ask new task %s of file: %s", taskId, fileMsg.Hash)
			if err != nil {
				log.Errorf("new task failed %s", err)
				replyErr(fileMsg.Hash, msg.MessageId, serr.NEW_TASK_FAILED, "new task failed")
				return
			}
		}
	}
	if !this.taskMgr.IsFileInfoExist(taskId) {
		log.Warnf("new task is deleted immediately")
		replyErr(fileMsg.Hash, msg.MessageId, serr.NEW_TASK_FAILED, "new task failed")
		return
	}
	if err := this.taskMgr.SetTaskInfoWithOptions(taskId,
		task.FileHash(fileMsg.Hash),
		task.BlocksRoot(fileMsg.BlocksRoot),
		task.Prefix(string(fileMsg.Prefix)),
		task.Walletaddr(this.chain.WalletAddress()),
		task.Privilege(info.Privilege),
		task.FileOwner(info.FileOwner.ToBase58()),
		task.StoreTx(fileMsg.Tx.Hash),
		task.StoreTxHeight(uint32(fileMsg.Tx.Height)),
		task.TotalBlockCnt(fileMsg.TotalBlockCount)); err != nil {
		log.Errorf("batch set file info fetch ask %s", err)
		replyErr(fileMsg.Hash, msg.MessageId, serr.SET_TASK_PROPERTY_ERROR, "new task failed")
		return
	}
	if err := this.taskMgr.AddFileSession(taskId, fileMsg.SessionId, fileMsg.PayInfo.WalletAddress,
		peerWalletAddr, uint32(fileMsg.PayInfo.Asset), fileMsg.PayInfo.UnitPrice); err != nil {
		log.Errorf("add session err in file fetch ask %s", err)
		replyErr(fileMsg.Hash, msg.MessageId, serr.SET_TASK_PROPERTY_ERROR, "new task failed")
		return
	}
	if err := this.taskMgr.BindTaskId(taskId); err != nil {
		log.Errorf("set task info err in file fetch ask %s", err)
		replyErr(fileMsg.Hash, msg.MessageId, serr.SET_TASK_PROPERTY_ERROR, "new task failed")
		return
	}
	currentBlockHash, currentBlockIndex, err := this.taskMgr.GetCurrentSetBlock(taskId)
	if err != nil {
		replyErr(fileMsg.Hash, msg.MessageId, serr.GET_TASK_PROPERTY_ERROR,
			"get current received block hash failed"+err.Error())
		return
	}
	replyMsg := message.NewFileMsgWithError(fileMsg.Hash,
		netcom.FILE_OP_FETCH_RDY_OK, 0, "",
		message.WithWalletAddress(this.chain.WalletAddress()),
		message.WithBreakpointHash(currentBlockHash),
		message.WithBreakpointIndex(uint64(currentBlockIndex)),
		message.WithSign(this.account),
		message.WithSyn(msg.MessageId),
	)
	if err := client.P2pSend(peerWalletAddr, replyMsg.MessageId, replyMsg.ToProtoMsg()); err != nil {
		log.Errorf("reply rdy ok msg failed", err)
	} else {
		log.Debugf("reply rdy ok msg success")
	}
}

// handleFileFetchPauseMsg. client send pause msg to storage nodes for telling them to pause the task
func (this *Dsp) handleFileFetchPauseMsg(ctx *network.ComponentContext,
	peerWalletAddr string, msg *message.Message) {
	fileMsg := msg.Payload.(*file.File)
	// my task. use my wallet address
	taskId := this.taskMgr.TaskId(fileMsg.Hash, this.chain.WalletAddress(), store.TaskTypeDownload)
	log.Debugf("handleFileFetchPauseMsg: of %s, taskId: %s from", fileMsg.Hash, taskId, peerWalletAddr)
	if !this.taskMgr.IsFileInfoExist(taskId) {
		return
	}
	this.taskMgr.SetTaskState(taskId, store.TaskStatePause)
}

// handleFileFetchResumeMsg. handle resume msg from client
func (this *Dsp) handleFileFetchResumeMsg(ctx *network.ComponentContext,
	peerWalletAddr string, msg *message.Message) {
	// my task. use my wallet address
	fileMsg := msg.Payload.(*file.File)
	taskId := this.taskMgr.TaskId(fileMsg.Hash, this.chain.WalletAddress(), store.TaskTypeDownload)
	log.Debugf("handleFileFetchResumeMsg: of %s, taskId: %s from", fileMsg.Hash, taskId, peerWalletAddr)
	if !this.taskMgr.IsFileInfoExist(taskId) {
		return
	}
	this.taskMgr.SetTaskState(taskId, store.TaskStateDoing)
}

func (this *Dsp) handleFileFetchDoneMsg(ctx *network.ComponentContext,
	peerWalletAddr string, msg *message.Message) {
	fileMsg := msg.Payload.(*file.File)
	taskId := this.taskMgr.TaskId(fileMsg.Hash, this.chain.WalletAddress(), store.TaskTypeUpload)
	if !this.taskMgr.IsFileInfoExist(taskId) {
		return
	}
	log.Debugf("receive fetch done msg, save task id:%s fileHash: %s, from %s done",
		taskId, fileMsg.Hash, peerWalletAddr)
	this.taskMgr.SetUploadProgressDone(taskId, peerWalletAddr)
}

func (this *Dsp) handleFileFetchCancelMsg(ctx *network.ComponentContext,
	peerWalletAddr string, msg *message.Message) {
	// my task. use my wallet address
	fileMsg := msg.Payload.(*file.File)
	taskId := this.taskMgr.TaskId(fileMsg.Hash, this.chain.WalletAddress(), store.TaskTypeDownload)
	log.Debugf("handleFileFetchCancelMsg: of %s, taskId: %s from", fileMsg.Hash, taskId, peerWalletAddr)
	if !this.taskMgr.IsFileInfoExist(taskId) {
		log.Debugf("file info not exist of canceling file %s", fileMsg.Hash)
		return
	}
	this.taskMgr.SetTaskState(taskId, store.TaskStateCancel)
}

// handleFileDeleteMsg. client send delete msg to storage nodes for telling them to delete the file and release the resources.
func (this *Dsp) handleFileDeleteMsg(ctx *network.ComponentContext,
	peerWalletAddr string, msg *message.Message) {
	fileMsg := msg.Payload.(*file.File)
	if fileMsg.Tx != nil && fileMsg.Tx.Height > 0 {
		if err := this.waitForTxConfirmed(fileMsg.Tx.Height); err != nil {
			log.Errorf("get block height err %s", err)
			replyMsg := message.NewFileMsgWithError(fileMsg.Hash,
				netcom.FILE_OP_DELETE_ACK, serr.DELETE_FILE_TX_UNCONFIRMED, err.Error(),
				message.WithSessionId(fileMsg.SessionId),
				message.WithWalletAddress(this.chain.WalletAddress()),
				message.WithSign(this.account),
				message.WithSyn(msg.MessageId),
			)
			if err := client.P2pSend(peerWalletAddr, replyMsg.MessageId, replyMsg.ToProtoMsg()); err != nil {
				log.Errorf("reply delete ok msg failed", err)
				return
			}
			log.Debugf("reply delete ack msg success")
			return
		}
	}
	info, err := this.chain.GetFileInfo(fileMsg.Hash)
	if info != nil || (err != nil && strings.Index(err.Error(), "FsGetFileInfo not found") == -1) {
		log.Errorf("delete file info is not nil %s %s", fileMsg.Hash, fileMsg.PayInfo.WalletAddress)
		replyMsg := message.NewFileMsgWithError(fileMsg.Hash, netcom.FILE_OP_DELETE_ACK,
			serr.DELETE_FILE_FILEINFO_EXISTS, "file info hasn't been deleted",
			message.WithSessionId(fileMsg.SessionId),
			message.WithWalletAddress(this.chain.WalletAddress()),
			message.WithSign(this.account),
			message.WithSyn(msg.MessageId),
		)
		if err := client.P2pSend(peerWalletAddr, replyMsg.MessageId, replyMsg.ToProtoMsg()); err != nil {
			log.Errorf("reply delete ok msg failed", err)
			return
		}
		log.Debugf("reply delete ack msg success")
		return
	}
	replyMsg := message.NewFileMsg(fileMsg.Hash, netcom.FILE_OP_DELETE_ACK,
		message.WithSessionId(fileMsg.SessionId),
		message.WithWalletAddress(this.chain.WalletAddress()),
		message.WithSign(this.account),
		message.WithSyn(msg.MessageId),
	)
	if err := client.P2pSend(peerWalletAddr, replyMsg.MessageId, replyMsg.ToProtoMsg()); err != nil {
		log.Errorf("reply delete ok msg failed", err)
	} else {
		log.Debugf("reply delete ack msg success")
	}
	// TODO: check file owner
	taskId := this.taskMgr.TaskId(fileMsg.Hash, this.chain.WalletAddress(), store.TaskTypeDownload)
	err = this.DeleteDownloadedFile(taskId)
	if err != nil {
		log.Errorf("delete downloaded file failed", err)
	}
}

// handleFileDownloadAskMsg. client send download ask msg to peers for download
func (this *Dsp) handleFileDownloadAskMsg(ctx *network.ComponentContext,
	peerWalletAddr string, msg *message.Message) {
	fileMsg := msg.Payload.(*file.File)
	replyErr := func(sessionId, fileHash string, errorCode uint32, errorMsg string, ctx *network.ComponentContext) {
		replyMsg := message.NewFileMsgWithError(fileMsg.Hash, netcom.FILE_OP_DOWNLOAD_ACK, errorCode, errorMsg,
			message.WithSessionId(sessionId),
			message.WithWalletAddress(this.chain.WalletAddress()),
			message.WithUnitPrice(0),
			message.WithAsset(0),
			message.WithSign(this.account),
			message.WithSyn(msg.MessageId),
		)
		if err := client.P2pSend(peerWalletAddr, replyMsg.MessageId, replyMsg.ToProtoMsg()); err != nil {
			log.Errorf("reply download ack err msg failed", err)
		}
	}
	info, err := this.chain.GetFileInfo(fileMsg.Hash)
	if err != nil || info == nil {
		log.Errorf("handle download ask msg, get file info %s not exist from chain", fileMsg.Hash)
		replyErr("", fileMsg.Hash, serr.FILEINFO_NOT_EXIST,
			fmt.Sprintf("file %s is deleted", fileMsg.Hash), ctx)
		return
	}
	if !this.chain.CheckFilePrivilege(info, fileMsg.Hash, fileMsg.PayInfo.WalletAddress) {
		log.Errorf("user %s has no privilege to download this file %s",
			fileMsg.PayInfo.WalletAddress, fileMsg.Hash)
		replyErr("", fileMsg.Hash, serr.NO_PRIVILEGE_TO_DOWNLOAD,
			fmt.Sprintf("user %s has no privilege to download this file", fileMsg.PayInfo.WalletAddress), ctx)
		return
	}
	downloadedId, err := this.taskMgr.GetDownloadedTaskId(fileMsg.Hash)
	if err != nil || len(downloadedId) == 0 {
		replyErr("", fileMsg.Hash, serr.FILEINFO_NOT_EXIST,
			fmt.Sprintf("no downloaded task for file %s", fileMsg.Hash), ctx)
		return
	}
	price, err := this.GetFileUnitPrice(fileMsg.PayInfo.Asset)
	if err != nil {
		replyErr("", fileMsg.Hash, serr.UNITPRICE_ERROR, err.Error(), ctx)
		return
	}
	prefix, err := this.taskMgr.GetFilePrefix(downloadedId)
	log.Debugf("get prefix from local: %s", prefix)
	if err != nil {
		replyErr("", fileMsg.Hash, serr.INTERNAL_ERROR, err.Error(), ctx)
		return
	}
	if this.channel != nil && this.dns != nil && this.dns.DNSNode != nil {
		dnsBalance, err := this.channel.GetAvailableBalance(this.dns.DNSNode.WalletAddr)
		if err != nil || dnsBalance == 0 {
			replyErr("", fileMsg.Hash, serr.INTERNAL_ERROR,
				"no enough balance with dns"+this.dns.DNSNode.WalletAddr, ctx)
			return
		}
	} else {
		log.Errorf("channel is nil %t or dns is nil %t", this.channel == nil, this.dns == nil)
	}

	localId := this.taskMgr.TaskId(fileMsg.Hash, fileMsg.PayInfo.WalletAddress, store.TaskTypeShare)
	if this.taskMgr.TaskExist(localId) {
		// old task
		sessionId, err := this.taskMgr.GetSessionId(localId, "")
		if err != nil {
			replyErr(sessionId, fileMsg.Hash, serr.INTERNAL_ERROR, err.Error(), ctx)
			return
		}
		canShare := this.canShareTo(localId, fileMsg.PayInfo.WalletAddress, fileMsg.PayInfo.Asset)
		if !canShare {
			replyErr(sessionId, fileMsg.Hash, serr.INTERNAL_ERROR,
				fmt.Sprintf("can't share %s to, err: %v", fileMsg.Hash, err), ctx)
			return
		}
		totalCount, _ := this.taskMgr.GetFileTotalBlockCount(downloadedId)
		replyMsg := message.NewFileMsg(fileMsg.Hash, netcom.FILE_OP_DOWNLOAD_ACK,
			message.WithSessionId(sessionId),
			message.WithBlockHashes(this.taskMgr.FileBlockHashes(downloadedId)),
			message.WithTotalBlockCount(totalCount),
			message.WithWalletAddress(this.chain.WalletAddress()),
			message.WithPrefix(prefix),
			message.WithUnitPrice(price),
			message.WithAsset(fileMsg.PayInfo.Asset),
			message.WithSign(this.account),
			message.WithSyn(msg.MessageId),
		)
		if err := client.P2pSend(peerWalletAddr, replyMsg.MessageId, replyMsg.ToProtoMsg()); err != nil {
			log.Errorf("reply download ack  msg failed", err)
		} else {
			this.taskMgr.SetTaskState(localId, store.TaskStateDoing)
			log.Debugf("reply download ack msg success")
		}
		return
	}
	// TODO: check channel balance and router path
	taskId, err := this.taskMgr.NewTask("", store.TaskTypeShare)
	if err != nil {
		replyErr("", fileMsg.Hash, serr.INTERNAL_ERROR, err.Error(), ctx)
		return
	}
	sessionId, err := this.taskMgr.GetSessionId(taskId, "")
	if err != nil {
		replyErr(sessionId, fileMsg.Hash, serr.INTERNAL_ERROR, err.Error(), ctx)
		return
	}
	rootBlk := this.fs.GetBlock(fileMsg.Hash)

	if !this.taskMgr.IsFileInfoExist(downloadedId) {
		replyErr(sessionId, fileMsg.Hash, serr.FILEINFO_NOT_EXIST, "", ctx)
		return
	}
	if rootBlk == nil {
		log.Error("file info exist but root block not found")
		err = this.DeleteDownloadedFile(downloadedId)
		if err != nil {
			log.Errorf("delete downloaded file err %s", err)
		}
		replyErr(sessionId, fileMsg.Hash, serr.FILEINFO_NOT_EXIST, "", ctx)
		return
	}

	if this.taskMgr.GetDoingTaskNum(store.TaskTypeShare) >= this.config.MaxShareTask {
		log.Warnf("current sharing task num exceed %d, reject new task", this.config.MaxShareTask)
		replyErr(sessionId, fileMsg.Hash, serr.TOO_MANY_TASKS, "", ctx)
		return
	}
	totalBlockCount := uint64(len(this.taskMgr.FileBlockHashes(downloadedId)))
	log.Debugf("sessionId %s blockCount %v %s prefix %s", sessionId, totalBlockCount,
		downloadedId, prefix)
	replyMsg := message.NewFileMsg(fileMsg.Hash, netcom.FILE_OP_DOWNLOAD_ACK,
		message.WithSessionId(sessionId),
		message.WithBlockHashes(this.taskMgr.FileBlockHashes(downloadedId)),
		message.WithTotalBlockCount(totalBlockCount),
		message.WithWalletAddress(this.chain.WalletAddress()),
		message.WithPrefix(prefix),
		message.WithUnitPrice(price),
		message.WithAsset(fileMsg.PayInfo.Asset),
		message.WithSign(this.account),
		message.WithSyn(msg.MessageId),
	)
	if err := client.P2pSend(peerWalletAddr, replyMsg.MessageId, replyMsg.ToProtoMsg()); err != nil {
		log.Errorf("reply download ack  msg failed", err)
	} else {
		log.Debugf("reply download ack msg success")
	}
	log.Debugf("reply download ack success new share task %s, file %s", taskId, fileMsg.Hash)
	if err := this.taskMgr.SetTaskInfoWithOptions(taskId,
		task.FileHash(fileMsg.Hash),
		task.ReferId(downloadedId),
		task.Walletaddr(fileMsg.PayInfo.WalletAddress)); err != nil {
		log.Errorf("batch commit file info failed, err: %s", err)
	}
	this.taskMgr.BindTaskId(taskId)
}

// handleFileDownloadMsg. client send to peers and telling them the file will be downloaded soon
func (this *Dsp) handleFileDownloadMsg(ctx *network.ComponentContext, peerWalletAddr string, msg *message.Message) {
	fileMsg := msg.Payload.(*file.File)
	taskKey := this.taskMgr.TaskId(fileMsg.Hash, fileMsg.PayInfo.WalletAddress, store.TaskTypeShare)
	// TODO: check channel balance and router path
	this.taskMgr.AddShareTo(taskKey, fileMsg.PayInfo.WalletAddress)
	hostAddr, _ := this.dns.GetExternalIP(fileMsg.PayInfo.WalletAddress)
	log.Debugf("Set host addr after recv file download %s - %s", fileMsg.PayInfo.WalletAddress, hostAddr)
}

func (this *Dsp) handleFileDownloadCancelMsg(ctx *network.ComponentContext, peerWalletAddr string, msg *message.Message) {
	fileMsg := msg.Payload.(*file.File)
	taskId := this.taskMgr.TaskId(fileMsg.Hash, fileMsg.PayInfo.WalletAddress, store.TaskTypeShare)
	if !this.taskMgr.TaskExist(taskId) {
		log.Warnf("share task not exist %s", fileMsg.Hash)
		return
	}
	// TODO: check unpaid amount
	if err := client.P2pClosePeerSession(peerWalletAddr, taskId); err != nil {
		log.Errorf("close peer failed")
	}
	this.taskMgr.CleanTask(taskId)
	log.Debugf("delete share task of %s", taskId)
}

// handleFileDownloadOkMsg. client send download ok msg to remove peers for telling them the task is finished
func (this *Dsp) handleFileDownloadOkMsg(ctx *network.ComponentContext,
	peerWalletAddr string, msg *message.Message) {
	fileMsg := msg.Payload.(*file.File)
	taskId := this.taskMgr.TaskId(fileMsg.Hash, fileMsg.PayInfo.WalletAddress, store.TaskTypeShare)
	if !this.taskMgr.TaskExist(taskId) {
		log.Errorf("share task not exist %s", fileMsg.Hash)
		return
	}
	// TODO: check unpaid amount
	if err := client.P2pClosePeerSession(peerWalletAddr, taskId); err != nil {
		log.Errorf("close peer failed")
	}
	this.taskMgr.CleanTask(taskId)
	log.Debugf("delete share task of %s", taskId)
}

// handleBlockFlightsMsg handle block flight msg
func (this *Dsp) handleBlockFlightsMsg(ctx *network.ComponentContext,
	peerWalletAddr string, msg *message.Message) {
	if msg.Payload == nil {
		log.Error("receive empty block flights")
		return
	}
	blockFlightsMsg := msg.Payload.(*block.BlockFlights)
	if blockFlightsMsg == nil || len(blockFlightsMsg.Blocks) == 0 {
		log.Errorf("receive empty block flights, err %v", msg.Error)
		return
	}
	switch blockFlightsMsg.Blocks[0].Operation {
	case netcom.BLOCK_OP_NONE:
		taskId := this.taskMgr.TaskId(blockFlightsMsg.Blocks[0].FileHash,
			this.chain.WalletAddress(), store.TaskTypeDownload)
		log.Debugf("taskId: %s, sessionId: %s receive %d blocks from peer:%s",
			taskId, blockFlightsMsg.Blocks[0].SessionId, len(blockFlightsMsg.Blocks), peerWalletAddr)
		exist := this.taskMgr.TaskExist(taskId)
		if !exist {
			log.Debugf("task %s, file: %s not exist", taskId, blockFlightsMsg.Blocks[0].FileHash)
			return
		}
		// active worker
		this.taskMgr.ActiveDownloadTaskPeer(peerWalletAddr)
		blocks := make([]*task.BlockResp, 0)
		for _, blockMsg := range blockFlightsMsg.Blocks {
			isDownloaded := this.taskMgr.IsBlockDownloaded(taskId, blockMsg.Hash, uint64(blockMsg.Index))
			if !isDownloaded {
				b := &task.BlockResp{
					Hash:      blockMsg.Hash,
					Index:     blockMsg.Index,
					PeerAddr:  peerWalletAddr,
					Block:     blockMsg.Data,
					Tag:       blockMsg.Tag,
					Offset:    blockMsg.Offset,
					PaymentId: blockFlightsMsg.PaymentId,
				}
				blocks = append(blocks, b)
			} else {
				log.Debugf("the block %s has downloaded", blockMsg.Hash)
			}
		}
		if len(blocks) == 0 {
			log.Debug("all download blocks have been downloaded")
			return
		}
		log.Debugf("task %s, push blocks from %s-%s-%d to %s-%d",
			taskId, blockFlightsMsg.Blocks[0].FileHash, blocks[0].Hash, blocks[0].Index,
			blocks[len(blocks)-1].Hash, blocks[len(blocks)-1].Index)
		if this.taskMgr.BlockFlightsChannelExists(taskId, blockFlightsMsg.Blocks[0].SessionId,
			blockFlightsMsg.TimeStamp) {
			this.taskMgr.PushGetBlockFlights(taskId, blockFlightsMsg.Blocks[0].SessionId,
				blocks, blockFlightsMsg.TimeStamp)
			return
		}
		// put to receive block logic
		if err := this.putBlocks(taskId, blockFlightsMsg.Blocks[0].FileHash, peerWalletAddr, blocks); err != nil {
			log.Errorf("put blocks failed %s", err)
		}
	case netcom.BLOCK_OP_GET:
		blockMsg := blockFlightsMsg.Blocks[0]
		sessionId := blockMsg.SessionId
		for _, blk := range blockFlightsMsg.Blocks {
			log.Debugf("session: %s handle get block %s-%s-%d from %s",
				sessionId, blk.FileHash, blk.Hash, blk.Index, peerWalletAddr)
			if len(sessionId) == 0 {
				return
			}
			existTsk, _ := this.taskMgr.GetTaskInfoCopy(sessionId)
			if existTsk == nil {
				log.Debugf("task %s, file %s not exist", sessionId, blockMsg.FileHash)
				return
			}
		}
		taskType, _ := this.taskMgr.GetTaskType(sessionId)
		log.Debugf("task key:%s type %d", sessionId, taskType)
		switch taskType {
		case store.TaskTypeDownload, store.TaskTypeShare:
			reqCh := this.taskMgr.BlockReqCh()
			req := make([]*task.GetBlockReq, 0)
			for _, v := range blockFlightsMsg.Blocks {
				log.Debugf("push get block req: %s-%s-%d from %s - %s",
					v.GetSessionId(), v.GetFileHash(), blockFlightsMsg.TimeStamp, v.Payment.Sender, peerWalletAddr)
				r := &task.GetBlockReq{
					TimeStamp:     blockFlightsMsg.TimeStamp,
					FileHash:      v.FileHash,
					Hash:          v.Hash,
					Index:         v.Index,
					PeerAddr:      peerWalletAddr,
					WalletAddress: v.Payment.Sender,
					Asset:         v.Payment.Asset,
					Syn:           msg.MessageId,
				}
				req = append(req, r)
			}

			reqCh <- req
			log.Debugf("push get block flights req done")
		case store.TaskTypeUpload:
			if len(blockFlightsMsg.Blocks) == 0 {
				log.Warnf("receive get blocks msg empty")
				return
			}
			pause, sdkerr := this.checkIfPause(sessionId, blockFlightsMsg.Blocks[0].FileHash)
			if sdkerr != nil {
				log.Debugf("handle get block pause %v %t", sdkerr, pause)
				return
			}
			if pause {
				log.Debugf("handle get block pause %v %t", sdkerr, pause)
				return
			}
			reqCh, err := this.taskMgr.TaskBlockReq(sessionId)
			if err != nil {
				log.Errorf("get task block reqCh err: %s", err)
				return
			}
			req := make([]*task.GetBlockReq, 0)
			for _, v := range blockFlightsMsg.Blocks {
				log.Debugf("push get block req: %s-%s-%d from %s - %s",
					v.GetSessionId(), v.GetFileHash(), blockFlightsMsg.TimeStamp, v.Payment.Sender, peerWalletAddr)
				r := &task.GetBlockReq{
					TimeStamp:     blockFlightsMsg.TimeStamp,
					FileHash:      v.FileHash,
					Hash:          v.Hash,
					Index:         v.Index,
					PeerAddr:      peerWalletAddr,
					WalletAddress: v.Payment.Sender,
					Asset:         v.Payment.Asset,
				}
				req = append(req, r)
			}
			log.Debugf("push get block to request")
			reqCh <- req
		default:
			log.Debugf("handle block flights get msg, tasktype not support")
		}
	default:
	}
}

// handleReqProgressMsg.
func (this *Dsp) handleReqProgressMsg(ctx *network.ComponentContext,
	peerWalletAddr string, msg *message.Message) {
	progressMsg := msg.Payload.(*progress.Progress)
	nodeInfos := make([]*progress.ProgressInfo, 0)
	for _, info := range progressMsg.Infos {
		id := this.taskMgr.TaskId(progressMsg.Hash, this.WalletAddress(), store.TaskTypeUpload)
		var prog *store.FileProgress
		if len(id) != 0 {
			prog = this.taskMgr.GetTaskPeerProgress(id, info.WalletAddr)
		}
		log.Debugf("handle req progress msg, get progress of id %s, file: %s, addr %s, count %d, ",
			id, progressMsg.Hash, info.WalletAddr, prog)
		count := uint64(0)
		if prog != nil {
			count = prog.Progress
		}
		nodeInfos = append(nodeInfos, &progress.ProgressInfo{
			WalletAddr: info.WalletAddr,
			NodeAddr:   info.NodeAddr,
			Count:      int32(count),
		})
	}
	resp := message.NewProgressMsg(this.WalletAddress(), progressMsg.Hash, netcom.FILE_OP_PROGRESS,
		nodeInfos, message.WithSign(this.CurrentAccount()), message.WithSyn(msg.MessageId))
	client.P2pSend(peerWalletAddr, resp.MessageId, resp.ToProtoMsg())
}

func (this *Dsp) waitForTxConfirmed(blockHeight uint64) error {
	currentBlockHeight, err := this.chain.GetCurrentBlockHeight()
	log.Debugf("wait for tx confirmed height: %d, now: %d", blockHeight, currentBlockHeight)
	if err != nil {
		log.Errorf("get block height err %s", err)
		return err
	}
	if blockHeight <= uint64(currentBlockHeight) {
		return nil
	}

	timeout := common.WAIT_FOR_GENERATEBLOCK_TIMEOUT * uint32(blockHeight-uint64(currentBlockHeight))
	if timeout > common.DOWNLOAD_FILE_TIMEOUT {
		timeout = common.DOWNLOAD_FILE_TIMEOUT
	}
	waitSuccess, err := this.chain.WaitForGenerateBlock(time.Duration(timeout)*time.Second,
		uint32(blockHeight-uint64(currentBlockHeight)))
	if err != nil || !waitSuccess {
		log.Errorf("get block height err %s %d %d", err, currentBlockHeight, blockHeight)
		return fmt.Errorf("get block height err %d %d", currentBlockHeight, blockHeight)
	}
	return nil
}
