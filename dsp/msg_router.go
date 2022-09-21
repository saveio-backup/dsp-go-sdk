package dsp

import (
	"fmt"
	"github.com/ethereum/go-ethereum/common"
	"strings"

	"github.com/saveio/dsp-go-sdk/actor/client"
	"github.com/saveio/dsp-go-sdk/consts"
	"github.com/saveio/dsp-go-sdk/network/message/types/payment"
	"github.com/saveio/dsp-go-sdk/network/message/types/progress"
	"github.com/saveio/dsp-go-sdk/task/base"
	"github.com/saveio/dsp-go-sdk/task/download"
	"github.com/saveio/dsp-go-sdk/task/types"

	"github.com/saveio/carrier/network"
	serr "github.com/saveio/dsp-go-sdk/error"
	netcom "github.com/saveio/dsp-go-sdk/network/common"
	"github.com/saveio/dsp-go-sdk/network/message"
	"github.com/saveio/dsp-go-sdk/network/message/types/block"
	"github.com/saveio/dsp-go-sdk/network/message/types/file"
	"github.com/saveio/dsp-go-sdk/store"
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
	log.Debugf("handle payment msg txHash %s %v", paymentMsg.TxHash, paymentMsg)

	event, err := this.Chain.GetSmartContractEvent(paymentMsg.TxHash)
	if err != nil {
		log.Errorf("handle payment msg, get smart contract event err %s for tx %s", err, paymentMsg.TxHash)
		// TODO: reply err
		replyMsg := message.NewEmptyMsg(
			message.WithSign(this.Chain.CurrentAccount(), this.Mode),
			message.WithSyn(msg.MessageId),
		)
		if err := client.P2PSend(peerWalletAddr, replyMsg.MessageId, replyMsg.ToProtoMsg()); err != nil {
			log.Errorf("reply payment error msg failed", err)
		} else {
			log.Debugf("reply payment error msg success")
		}
		return
	}
	if event == nil || event.Notify == nil {
		log.Errorf("get event nil from tx %v", paymentMsg.TxHash)
		// TODO: reply err
		replyMsg := message.NewEmptyMsg(
			message.WithSign(this.Chain.CurrentAccount(), this.Mode),
			message.WithSyn(msg.MessageId),
		)
		if err := client.P2PSend(peerWalletAddr, replyMsg.MessageId, replyMsg.ToProtoMsg()); err != nil {
			log.Errorf("reply payment error msg failed", err)
		} else {
			log.Debugf("reply payment error msg success")
		}
		return
	}

	valid := false
	for _, n := range event.Notify {
		if n == nil || n.States == nil {
			continue
		}
		s, ok := n.States.(map[string]interface{})
		if !ok {
			continue
		}
		paymentId, ok := s["paymentId"].(float64)
		if !ok {
			log.Errorf("payment id convert err %T", s["paymentId"])
			continue
		}
		log.Debugf("get payment id %v %T from event %v, paymentMsg.PaymentId %v",
			s["paymentId"], s["paymentId"], paymentId, paymentMsg.PaymentId)
		if int32(paymentId) == int32(paymentMsg.PaymentId) {
			valid = true
			break
		}
	}
	if !valid {
		// TODO: reply err
		replyMsg := message.NewEmptyMsg(
			message.WithSign(this.Chain.CurrentAccount(), this.Mode),
			message.WithSyn(msg.MessageId),
		)
		if err := client.P2PSend(peerWalletAddr, replyMsg.MessageId, replyMsg.ToProtoMsg()); err != nil {
			log.Errorf("reply payment error msg failed", err)
		} else {
			log.Debugf("reply payment error msg success")
		}
		return
	}

	taskId, err := this.TaskMgr.GetTaskIdWithPaymentId(int32(paymentMsg.PaymentId))
	if err != nil {
		log.Errorf("get taskId with payment id failed %s", err)
		// TODO: reply err
		replyMsg := message.NewEmptyMsg(
			message.WithSign(this.Chain.CurrentAccount(), this.Mode),
			message.WithSyn(msg.MessageId),
		)
		if err := client.P2PSend(peerWalletAddr, replyMsg.MessageId, replyMsg.ToProtoMsg()); err != nil {
			log.Errorf("reply payment error msg failed", err)
		} else {
			log.Debugf("reply payment error msg success")
		}
		return
	}
	shareTask := this.TaskMgr.GetShareTask(taskId)
	if shareTask == nil {
		log.Errorf("get share taskId with id %s not found", taskId)
		// TODO: reply err
		replyMsg := message.NewEmptyMsg(
			message.WithSign(this.Chain.CurrentAccount(), this.Mode),
			message.WithSyn(msg.MessageId),
		)
		if err := client.P2PSend(peerWalletAddr, replyMsg.MessageId, replyMsg.ToProtoMsg()); err != nil {
			log.Errorf("reply payment error msg failed", err)
		} else {
			log.Debugf("reply payment error msg success")
		}
		return
	}

	fileHashStr := shareTask.GetFileHash()
	fileName := shareTask.GetFileName()
	fileOwner := shareTask.GetFileOwner()

	// delete record
	err = shareTask.DeleteFileUnpaid(
		paymentMsg.Sender,
		int32(paymentMsg.PaymentId),
		int32(paymentMsg.Asset),
		uint64(paymentMsg.Amount),
	)
	if err != nil {
		log.Errorf("delete share file info %s", err)
		// TODO: reply err
		replyMsg := message.NewEmptyMsg(
			message.WithSign(this.Chain.CurrentAccount(), this.Mode),
			message.WithSyn(msg.MessageId),
		)
		if err := client.P2PSend(peerWalletAddr, replyMsg.MessageId, replyMsg.ToProtoMsg()); err != nil {
			log.Errorf("reply payment error msg failed", err)
		} else {
			log.Debugf("reply payment error msg success")
		}
		return
	}
	log.Debugf("delete unpaid success taskId: %v, fileHash: %v, fileName: %v, owner: %v, sender: %v, amount: %v",
		taskId, fileHashStr, fileName, fileOwner, paymentMsg.Sender, uint64(paymentMsg.Amount))
	this.TaskMgr.InsertShareRecord(taskId, fileHashStr, fileName, fileOwner,
		paymentMsg.Sender, uint64(paymentMsg.Amount))

	replyMsg := message.NewEmptyMsg(
		message.WithSign(this.Chain.CurrentAccount(), this.Mode),
		message.WithSyn(msg.MessageId),
	)
	if err := client.P2PSend(peerWalletAddr, replyMsg.MessageId, replyMsg.ToProtoMsg()); err != nil {
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
	height, _ := this.Chain.GetCurrentBlockHeight()
	existTaskId := this.TaskMgr.GetDownloadedTaskId(fileMsg.Hash, this.Chain.WalletAddress())
	if len(existTaskId) == 0 {
		var replyMsg *message.Message
		if this.TaskMgr.GetDoingTaskNum(store.TaskTypeDownload) >= this.config.MaxDownloadTask {
			log.Warnf("current downloading task num exceed %d, reject new task", this.config.MaxDownloadTask)
			replyMsg = message.NewFileMsgWithError(fileMsg.GetHash(), netcom.FILE_OP_FETCH_ACK,
				serr.TOO_MANY_TASKS,
				"",
				message.WithSessionId(fileMsg.SessionId),
				message.WithWalletAddress(this.Chain.CurrentAccount().Address.ToBase58()),
				message.WithSign(this.Chain.CurrentAccount(), consts.DspModeThemis),
				message.ChainHeight(height),
				message.WithSyn(msg.MessageId),
			)
		} else {
			replyMsg = message.NewFileMsg(fileMsg.GetHash(), netcom.FILE_OP_FETCH_ACK,
				message.WithSessionId(fileMsg.SessionId),
				message.WithWalletAddress(this.Chain.CurrentAccount().Address.ToBase58()),
				message.WithSign(this.Chain.CurrentAccount(), consts.DspModeThemis),
				message.ChainHeight(height),
				message.WithSyn(msg.MessageId),
			)
		}
		if err := client.P2PSend(peerWalletAddr, replyMsg.MessageId, replyMsg.ToProtoMsg()); err != nil {
			log.Errorf("send file_ack msg to %s failed %s", peerWalletAddr, err)
			return
		}
		log.Debugf("send file_ack msg to %s success", peerWalletAddr)
		return
	}
	// handle old task
	// state, _ := this.taskMgr.GetTaskState(existTaskId)
	// log.Debugf("download task exist %s, file %s, state: %d", existTaskId, fileMsg.Hash, state)
	// if state == store.TaskStateCancel || state == store.TaskStateFailed || state == store.TaskStateDone {
	// 	log.Warnf("the task has a wrong state of file_ask %s", state)
	// }
	// there use themis address reply
	newMsg := message.NewFileMsg(fileMsg.GetHash(), netcom.FILE_OP_FETCH_ACK,
		message.WithSessionId(fileMsg.SessionId),
		message.WithWalletAddress(this.Chain.CurrentAccount().Address.ToBase58()),
		message.WithSign(this.Chain.CurrentAccount(), consts.DspModeThemis),
		message.ChainHeight(height),
		message.WithSyn(msg.MessageId),
	)
	if err := client.P2PSend(peerWalletAddr, newMsg.MessageId, newMsg.ToProtoMsg()); err != nil {
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
			message.WithWalletAddress(this.Chain.WalletAddress()),
			message.WithSign(this.Chain.CurrentAccount(), this.Mode),
			message.WithSyn(synMsgId),
		)
		err := client.P2PSend(peerWalletAddr, replyMsg.MessageId, replyMsg.ToProtoMsg())
		if err != nil {
			log.Errorf("reply rdy ok msg failed %s", err)
		}
		log.Debugf("replyErr: file rdy ok msg to %s success", peerWalletAddr)
		return err
	}
	// check file tx, and if it is confirmed, and file info is still exist
	if fileMsg.Tx == nil {
		log.Errorf("file rdy msg has no tx %s", fileMsg.Hash)
		replyErr(fileMsg.Hash, msg.MessageId, serr.MISS_UPLOADED_FILE_TX, "upload file tx is required")
		return
	}
	if err := this.Chain.WaitForTxConfirmed(fileMsg.Tx.Height); err != nil {
		log.Errorf("get block height err %s", err)
		replyErr(fileMsg.Hash, msg.MessageId, serr.CHECK_UPLOADED_TX_ERROR, "get block height err "+err.Error())
		return
	}
	// TODO: check rpc server avaliable
	info, err := this.Chain.GetFileInfo(fileMsg.Hash)
	if info == nil {
		log.Errorf("fetch ask file info is nil %s %s, err %s", fileMsg.Hash, fileMsg.PayInfo.WalletAddress, err)
		replyErr(fileMsg.Hash, msg.MessageId, serr.FILEINFO_NOT_EXIST, "fetch ask file info is nil")
		return
	}
	log.Debugf("get file info %s success", fileMsg.Hash)
	// check upload privilege
	ethAddress := common.BytesToAddress(info.FileOwner[:])
	if fileMsg.PayInfo == nil ||
		(info.FileOwner.ToBase58() != fileMsg.PayInfo.WalletAddress &&
			ethAddress.String() != fileMsg.PayInfo.WalletAddress &&
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
	var downloadTask *download.DownloadTask
	taskId := this.TaskMgr.GetDownloadedTaskId(fileMsg.Hash, this.Chain.WalletAddress())
	log.Debugf("[handleFileRdyMsg] get downloaded task %s", taskId)
	if len(taskId) == 0 {
		// handle new download task. use my wallet address
		log.Debugf("task not exist, create new download task")
		downloadTask, err = this.TaskMgr.NewDownloadTask("")
		log.Debugf("task not exist, create new download task %v, err %v", downloadTask, err)
		if err != nil || downloadTask == nil {
			log.Errorf("new task failed %s", err)
			replyErr(fileMsg.Hash, msg.MessageId, serr.NEW_TASK_FAILED, "new task failed")
			return
		}
		taskId = downloadTask.GetId()
		log.Debugf("fetch_ask new task %s of file: %s", taskId, fileMsg.Hash)
	} else {
		downloadTask = this.TaskMgr.GetDownloadTask(taskId)
		if downloadTask == nil {
			replyErr(fileMsg.Hash, msg.MessageId, serr.GET_TASK_PROPERTY_ERROR, err.Error())
			return
		}
		if downloadTask.GetStoreTx() == fileMsg.Tx.Hash {
			fileHashDone := this.TaskMgr.IsFileDownloaded(taskId)
			if fileHashDone && this.IsFs() && !this.Chain.CheckHasProveFile(fileMsg.Hash, this.Address()) {
				// file has downloaded but not proved
				if err := this.TaskMgr.StartPDPVerify(fileMsg.Hash); err != nil {
					replyErr(fileMsg.Hash, msg.MessageId, serr.SET_FILEINFO_DB_ERROR, err.Error())
					return
				}
			}
			if downloadTask.State() != store.TaskStateDone {
				if fileHashDone {
					downloadTask.SetTaskState(store.TaskStateDone)
				} else {
					// set a new task state
					downloadTask.SetTaskState(store.TaskStateDoing)
				}
			}
		} else {
			log.Debugf("task %s store tx not match %s-%s",
				taskId, downloadTask.GetStoreTx(), fileMsg.Tx.Hash)
			// receive a new file info tx, delete old task info
			if err := this.TaskMgr.CleanDownloadTask(taskId); err != nil {
				replyErr(fileMsg.Hash, msg.MessageId, serr.SET_FILEINFO_DB_ERROR, err.Error())
				return
			}
			// handle new download task. use my wallet address
			downloadTask, err = this.TaskMgr.NewDownloadTask("")
			if err != nil {
				log.Errorf("new task failed %s", err)
				replyErr(fileMsg.Hash, msg.MessageId, serr.NEW_TASK_FAILED, "new task failed")
				return
			}
			taskId = downloadTask.GetId()
			log.Debugf("fetch_ask new task %s of file: %s", taskId, fileMsg.Hash)
		}
	}

	if !this.TaskMgr.IsFileInfoExist(taskId) {
		log.Warnf("new task is deleted immediately")
		replyErr(fileMsg.Hash, msg.MessageId, serr.NEW_TASK_FAILED, "new task failed")
		return
	}
	if err := downloadTask.SetInfoWithOptions(
		base.FileHash(fileMsg.Hash),
		base.FileName(string(info.FileDesc)),
		base.BlocksRoot(fileMsg.BlocksRoot),
		base.Prefix(string(fileMsg.Prefix)),
		base.Walletaddr(this.Chain.WalletAddress()),
		base.Privilege(info.Privilege),
		base.FileOwner(info.FileOwner.ToBase58()),
		base.StoreTx(fileMsg.Tx.Hash),
		base.CopyNum(uint32(info.CopyNum)),
		base.StoreTxHeight(uint32(fileMsg.Tx.Height)),
		base.TotalBlockCnt(fileMsg.TotalBlockCount),
		base.PeerToSessionIds(map[string]string{fileMsg.PayInfo.WalletAddress: fileMsg.SessionId}),
	); err != nil {
		log.Errorf("batch set file info fetch ask %s", err)
		replyErr(fileMsg.Hash, msg.MessageId, serr.SET_TASK_PROPERTY_ERROR, "new task failed")
		return
	}
	if err := this.TaskMgr.AddFileSession(taskId, fileMsg.SessionId, fileMsg.PayInfo.WalletAddress,
		peerWalletAddr, uint32(fileMsg.PayInfo.Asset), fileMsg.PayInfo.UnitPrice); err != nil {
		log.Errorf("add session err in file fetch ask %s", err)
		replyErr(fileMsg.Hash, msg.MessageId, serr.SET_TASK_PROPERTY_ERROR, "new task failed")
		return
	}
	// deprecated
	// if err := this.taskMgr.BindTaskId(taskId); err != nil {
	// 	log.Errorf("set task info err in file fetch ask %s", err)
	// 	replyErr(fileMsg.Hash, msg.MessageId, serr.SET_TASK_PROPERTY_ERROR, "new task failed")
	// 	return
	// }
	currentBlockHash, currentBlockIndex, err := this.TaskMgr.GetCurrentSetBlock(taskId)
	if err != nil {
		replyErr(fileMsg.Hash, msg.MessageId, serr.GET_TASK_PROPERTY_ERROR,
			"get current received block hash failed"+err.Error())
		return
	}
	replyMsg := message.NewFileMsgWithError(fileMsg.Hash,
		netcom.FILE_OP_FETCH_RDY_OK, 0, "",
		message.WithWalletAddress(this.Chain.WalletAddress()),
		message.WithBreakpointHash(currentBlockHash),
		message.WithBreakpointIndex(uint64(currentBlockIndex)),
		message.WithSign(this.Chain.CurrentAccount(), this.Mode),
		message.WithSyn(msg.MessageId),
	)
	if err := client.P2PSend(peerWalletAddr, replyMsg.MessageId, replyMsg.ToProtoMsg()); err != nil {
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
	taskId := this.TaskMgr.GetDownloadedTaskId(fileMsg.Hash, this.Chain.WalletAddress())

	log.Debugf("handleFileFetchPauseMsg: of %s, taskId: %s from", fileMsg.Hash, taskId, peerWalletAddr)
	if !this.TaskMgr.IsFileInfoExist(taskId) {
		return
	}
	downloadTask := this.TaskMgr.GetDownloadTask(taskId)
	if downloadTask == nil {
		return
	}

	downloadTask.SetTaskState(store.TaskStatePause)
}

// handleFileFetchResumeMsg. handle resume msg from client
func (this *Dsp) handleFileFetchResumeMsg(ctx *network.ComponentContext,
	peerWalletAddr string, msg *message.Message) {
	// my task. use my wallet address
	fileMsg := msg.Payload.(*file.File)
	taskId := this.TaskMgr.GetDownloadedTaskId(fileMsg.Hash, this.Chain.WalletAddress())

	log.Debugf("handleFileFetchResumeMsg: of %s, taskId: %s from", fileMsg.Hash, taskId, peerWalletAddr)
	if !this.TaskMgr.IsFileInfoExist(taskId) {
		return
	}
	downloadTask := this.TaskMgr.GetDownloadTask(taskId)
	if downloadTask == nil {
		return
	}

	downloadTask.SetTaskState(store.TaskStateDoing)
}

func (this *Dsp) handleFileFetchDoneMsg(ctx *network.ComponentContext,
	peerWalletAddr string, msg *message.Message) {
	fileMsg := msg.Payload.(*file.File)

	taskId := this.TaskMgr.GetUploadTaskId(fileMsg.Hash, this.Chain.WalletAddress())
	log.Debugf("handleFileFetchDoneMsg: of %s, taskId: %s from", fileMsg.Hash, taskId, peerWalletAddr)
	if !this.TaskMgr.IsFileInfoExist(taskId) {
		return
	}
	uploadTask := this.TaskMgr.GetUploadTask(taskId)
	if uploadTask == nil {
		return
	}
	log.Debugf("receive fetch done msg, save task id:%s fileHash: %s, from %s done",
		taskId, fileMsg.Hash, peerWalletAddr)
	err := uploadTask.SetUploadProgressDone(taskId, peerWalletAddr)
	if err != nil {
		log.Errorf("set upload progress done failed", err)
	}
}

func (this *Dsp) handleFileFetchCancelMsg(ctx *network.ComponentContext,
	peerWalletAddr string, msg *message.Message) {
	// my task. use my wallet address
	fileMsg := msg.Payload.(*file.File)

	// my task. use my wallet address
	taskId := this.TaskMgr.GetDownloadedTaskId(fileMsg.Hash, this.Chain.WalletAddress())

	log.Debugf("handleFileFetchCancelMsg: of %s, taskId: %s from", fileMsg.Hash, taskId, peerWalletAddr)
	if !this.TaskMgr.IsFileInfoExist(taskId) {
		return
	}
	downloadTask := this.TaskMgr.GetDownloadTask(taskId)
	if downloadTask == nil {
		return
	}

	downloadTask.SetTaskState(store.TaskStateCancel)
}

// handleFileDeleteMsg. client send delete msg to storage nodes for telling them to delete the file and release the resources.
func (this *Dsp) handleFileDeleteMsg(ctx *network.ComponentContext,
	peerWalletAddr string, msg *message.Message) {
	fileMsg := msg.Payload.(*file.File)
	if fileMsg.Tx != nil && fileMsg.Tx.Height > 0 {
		if err := this.Chain.WaitForTxConfirmed(fileMsg.Tx.Height); err != nil {
			log.Errorf("get block height err %s", err)
			replyMsg := message.NewFileMsgWithError(fileMsg.Hash,
				netcom.FILE_OP_DELETE_ACK, serr.DELETE_FILE_TX_UNCONFIRMED, err.Error(),
				message.WithSessionId(fileMsg.SessionId),
				message.WithWalletAddress(this.Chain.WalletAddress()),
				message.WithSign(this.Chain.CurrentAccount(), this.Mode),
				message.WithSyn(msg.MessageId),
			)
			if err := client.P2PSend(peerWalletAddr, replyMsg.MessageId, replyMsg.ToProtoMsg()); err != nil {
				log.Errorf("reply delete ok msg failed", err)
				return
			}
			log.Debugf("reply delete ack msg success")
			return
		}
	}
	info, err := this.Chain.GetFileInfo(fileMsg.Hash)
	if info != nil || (err != nil && strings.Index(err.Error(), "FsGetFileInfo not found") == -1) {
		log.Errorf("delete file info is not nil %s %s", fileMsg.Hash, fileMsg.PayInfo.WalletAddress)
		replyMsg := message.NewFileMsgWithError(fileMsg.Hash, netcom.FILE_OP_DELETE_ACK,
			serr.DELETE_FILE_FILEINFO_EXISTS, "file info hasn't been deleted",
			message.WithSessionId(fileMsg.SessionId),
			message.WithWalletAddress(this.Chain.WalletAddress()),
			message.WithSign(this.Chain.CurrentAccount(), this.Mode),
			message.WithSyn(msg.MessageId),
		)
		if err := client.P2PSend(peerWalletAddr, replyMsg.MessageId, replyMsg.ToProtoMsg()); err != nil {
			log.Errorf("reply delete ok msg failed", err)
			return
		}
		log.Debugf("reply delete ack msg success")
		return
	}
	replyMsg := message.NewFileMsg(fileMsg.Hash, netcom.FILE_OP_DELETE_ACK,
		message.WithSessionId(fileMsg.SessionId),
		message.WithWalletAddress(this.Chain.WalletAddress()),
		message.WithSign(this.Chain.CurrentAccount(), this.Mode),
		message.WithSyn(msg.MessageId),
	)
	if err := client.P2PSend(peerWalletAddr, replyMsg.MessageId, replyMsg.ToProtoMsg()); err != nil {
		log.Errorf("reply delete ok msg failed", err)
	} else {
		log.Debugf("reply delete ack msg success")
	}
	// TODO: check file owner
	taskId := this.TaskMgr.GetDownloadedTaskId(fileMsg.Hash, this.Chain.WalletAddress())
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
			message.WithWalletAddress(this.Chain.WalletAddress()),
			message.WithUnitPrice(0),
			message.WithAsset(0),
			message.WithSign(this.Chain.CurrentAccount(), this.Mode),
			message.WithSyn(msg.MessageId),
		)
		if err := client.P2PSend(peerWalletAddr, replyMsg.MessageId, replyMsg.ToProtoMsg()); err != nil {
			log.Errorf("reply download ack err msg failed", err)
		}
	}
	info, err := this.Chain.GetFileInfo(fileMsg.Hash)
	if err != nil || info == nil {
		log.Errorf("handle download ask msg, get file info %s not exist from chain", fileMsg.Hash)
		replyErr("", fileMsg.Hash, serr.FILEINFO_NOT_EXIST,
			fmt.Sprintf("file %s is deleted", fileMsg.Hash), ctx)
		return
	}
	if !this.Chain.CheckFilePrivilege(info, fileMsg.Hash, fileMsg.PayInfo.WalletAddress) {
		log.Errorf("user %s has no privilege to download this file %s",
			fileMsg.PayInfo.WalletAddress, fileMsg.Hash)
		replyErr("", fileMsg.Hash, serr.NO_PRIVILEGE_TO_DOWNLOAD,
			fmt.Sprintf("user %s has no privilege to download this file", fileMsg.PayInfo.WalletAddress), ctx)
		return
	}
	downloadedId := this.TaskMgr.GetDownloadedTaskId(fileMsg.Hash, this.Chain.WalletAddress())
	if len(downloadedId) == 0 {
		replyErr("", fileMsg.Hash, serr.FILEINFO_NOT_EXIST,
			fmt.Sprintf("no downloaded task for file %s", fileMsg.Hash), ctx)
		return
	}
	downloadedTask := this.TaskMgr.GetDownloadTask(downloadedId)
	if downloadedTask == nil {
		replyErr("", fileMsg.Hash, serr.FILEINFO_NOT_EXIST,
			fmt.Sprintf("no downloaded task for file %s", fileMsg.Hash), ctx)
		return
	}
	prefix := downloadedTask.GetPrefix()
	log.Debugf("get prefix from local: %s", prefix)

	if this.Channel != nil && this.DNS != nil && this.DNS.HasDNS() {
		dnsBalance, err := this.Channel.GetAvailableBalance(this.DNS.CurrentDNSWallet())
		if err != nil || dnsBalance == 0 {
			replyErr("", fileMsg.Hash, serr.INTERNAL_ERROR,
				"no enough balance with dns"+this.DNS.CurrentDNSWallet(), ctx)
			return
		}
	} else {
		log.Errorf("channel is nil %t or dns is nil %t", this.Channel == nil, this.DNS == nil)
	}

	sharedTask := this.TaskMgr.GetShareTaskByFileHash(fileMsg.Hash, fileMsg.PayInfo.WalletAddress)
	if sharedTask != nil {
		// old task
		sessionId := sharedTask.GetId()
		if !sharedTask.CanShareTo(fileMsg.PayInfo.WalletAddress, fileMsg.PayInfo.Asset) {
			replyErr(sessionId, fileMsg.Hash, serr.INTERNAL_ERROR,
				fmt.Sprintf("can't share %s to, err: %v", fileMsg.Hash, err), ctx)
			return
		}
		totalCount := downloadedTask.GetTotalBlockCnt()
		replyMsg := message.NewFileMsg(fileMsg.Hash, netcom.FILE_OP_DOWNLOAD_ACK,
			message.WithSessionId(sessionId),
			message.WithBlockHashes(this.TaskMgr.FileBlockHashes(downloadedId)),
			message.WithTotalBlockCount(totalCount),
			message.WithWalletAddress(this.Chain.WalletAddress()),
			message.WithPrefix(prefix),
			message.WithUnitPrice(consts.DOWNLOAD_BLOCK_PRICE),
			message.WithAsset(fileMsg.PayInfo.Asset),
			message.WithSign(this.Chain.CurrentAccount(), this.Mode),
			message.WithSyn(msg.MessageId),
		)
		if err := client.P2PSend(peerWalletAddr, replyMsg.MessageId, replyMsg.ToProtoMsg()); err != nil {
			log.Errorf("reply download ack msg failed", err)
		} else {
			sharedTask.SetTaskState(store.TaskStateDoing)
			log.Debugf("reply download ack msg success")
		}
		return
	}
	// share task not found, create new share task
	// TODO: check channel balance and router path
	sharedTask, err = this.TaskMgr.NewShareTask("")
	if err != nil {
		replyErr("", fileMsg.Hash, serr.INTERNAL_ERROR, err.Error(), ctx)
		return
	}
	taskId := sharedTask.GetId()
	sessionId := sharedTask.GetId()
	rootBlk := this.Fs.GetBlock(fileMsg.Hash)

	if !this.TaskMgr.IsFileInfoExist(downloadedId) {
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

	if this.TaskMgr.GetDoingTaskNum(store.TaskTypeShare) >= this.config.MaxShareTask {
		log.Warnf("current sharing task num exceed %d, reject new task", this.config.MaxShareTask)
		replyErr(sessionId, fileMsg.Hash, serr.TOO_MANY_TASKS, "", ctx)
		return
	}

	// TODO: there will be large packet if block hashes slice too big
	allBlockHashes := this.TaskMgr.FileBlockHashes(downloadedId)
	totalBlockCount := uint64(len(allBlockHashes))
	log.Debugf("sessionId %s blockCount %v %s prefix %s", sessionId, totalBlockCount,
		downloadedId, prefix)
	replyMsg := message.NewFileMsg(fileMsg.Hash, netcom.FILE_OP_DOWNLOAD_ACK,
		message.WithSessionId(sessionId),
		message.WithBlockHashes(allBlockHashes),
		message.WithTotalBlockCount(totalBlockCount),
		message.WithWalletAddress(this.Chain.WalletAddress()),
		message.WithPrefix(prefix),
		message.WithUnitPrice(consts.DOWNLOAD_BLOCK_PRICE),
		message.WithAsset(fileMsg.PayInfo.Asset),
		message.WithSign(this.Chain.CurrentAccount(), this.Mode),
		message.WithSyn(msg.MessageId),
	)
	if err := client.P2PSend(peerWalletAddr, replyMsg.MessageId, replyMsg.ToProtoMsg()); err != nil {
		log.Errorf("reply download ack  msg failed", err)
	} else {
		log.Debugf("reply download ack msg success")
	}
	log.Debugf("reply download ack success new share task %s, file %s, fileName %s, owner %s",
		taskId, fileMsg.Hash, downloadedTask.GetFileName(), downloadedTask.GetFileOwner())
	if err := sharedTask.SetInfoWithOptions(
		base.FileName(downloadedTask.GetFileName()),
		base.FileOwner(downloadedTask.GetFileOwner()),
		base.FileHash(fileMsg.Hash),
		base.ReferId(downloadedId),
		base.Walletaddr(fileMsg.PayInfo.WalletAddress),
		base.Prefix(string(prefix)),
		base.TotalBlockCnt(totalBlockCount),
	); err != nil {
		log.Errorf("batch commit file info failed, err: %s", err)
	}
}

// handleFileDownloadMsg. client send to peers and telling them the file will be downloaded soon
func (this *Dsp) handleFileDownloadMsg(ctx *network.ComponentContext, peerWalletAddr string, msg *message.Message) {
	fileMsg := msg.Payload.(*file.File)
	sharedTask := this.TaskMgr.GetShareTaskByFileHash(fileMsg.Hash, fileMsg.PayInfo.WalletAddress)
	if sharedTask == nil {
		log.Errorf("shared task not found for file %s, wallet %s", fileMsg.Hash, fileMsg.PayInfo.WalletAddress)
		return
	}
	this.TaskMgr.AddShareTo(sharedTask.GetId(), fileMsg.PayInfo.WalletAddress)
	hostAddr, _ := this.DNS.GetExternalIP(fileMsg.PayInfo.WalletAddress)
	log.Debugf("Set host addr after recv file download %s - %s", fileMsg.PayInfo.WalletAddress, hostAddr)
}

func (this *Dsp) handleFileDownloadCancelMsg(ctx *network.ComponentContext, peerWalletAddr string, msg *message.Message) {
	fileMsg := msg.Payload.(*file.File)
	sharedTask := this.TaskMgr.GetShareTaskByFileHash(fileMsg.Hash, fileMsg.PayInfo.WalletAddress)
	if sharedTask == nil {
		log.Errorf("shared task not found for file %s, wallet %s", fileMsg.Hash, fileMsg.PayInfo.WalletAddress)
		return
	}
	// TODO: check unpaid amount
	taskId := sharedTask.GetId()
	if err := client.P2PClosePeerSession(peerWalletAddr, taskId); err != nil {
		log.Errorf("close peer failed")
	}
	this.TaskMgr.CleanShareTask(taskId)
	log.Debugf("delete share task of %s", taskId)
}

// handleFileDownloadOkMsg. client send download ok msg to remove peers for telling them the task is finished
func (this *Dsp) handleFileDownloadOkMsg(ctx *network.ComponentContext,
	peerWalletAddr string, msg *message.Message) {
	fileMsg := msg.Payload.(*file.File)
	sharedTask := this.TaskMgr.GetShareTaskByFileHash(fileMsg.Hash, fileMsg.PayInfo.WalletAddress)
	if sharedTask == nil {
		log.Errorf("shared task not found for file %s, wallet %s", fileMsg.Hash, fileMsg.PayInfo.WalletAddress)
		return
	}
	// TODO: check unpaid amount
	taskId := sharedTask.GetId()
	if err := client.P2PClosePeerSession(peerWalletAddr, taskId); err != nil {
		log.Errorf("close peer failed")
	}
	this.TaskMgr.CleanShareTask(taskId)
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
		// receive blocks for download task
		taskId := this.TaskMgr.GetDownloadedTaskId(blockFlightsMsg.Blocks[0].FileHash, this.Chain.WalletAddress())
		log.Debugf("download task %s, sessionId %s receive %d blocks from peer:%s",
			taskId, blockFlightsMsg.Blocks[0].SessionId, len(blockFlightsMsg.Blocks), peerWalletAddr)
		downloadTask := this.TaskMgr.GetDownloadTask(taskId)
		if downloadTask == nil {
			log.Debugf("download task %s, file %s not exist", taskId, blockFlightsMsg.Blocks[0].FileHash)
			return
		}
		// active worker
		downloadTask.ActiveWorker(peerWalletAddr)
		blocks := make([]*types.BlockResp, 0)
		for _, blockMsg := range blockFlightsMsg.Blocks {
			isDownloaded := this.TaskMgr.IsBlockDownloaded(taskId, blockMsg.Hash, uint64(blockMsg.Index))
			if !isDownloaded {
				b := &types.BlockResp{
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
		if downloadTask.BlockFlightsChannelExists(blockFlightsMsg.Blocks[0].SessionId,
			blockFlightsMsg.TimeStamp) {
			downloadTask.PushGetBlockFlights(blockFlightsMsg.Blocks[0].SessionId,
				blocks, blockFlightsMsg.TimeStamp)
			return
		}
		// put to receive block logic
		if err := downloadTask.PutBlocks(peerWalletAddr, blocks); err != nil {
			log.Errorf("put blocks failed %s", err)
		}
	case netcom.BLOCK_OP_GET:
		blockMsg := blockFlightsMsg.Blocks[0]
		sessionId := blockMsg.SessionId
		var taskType store.TaskType

		if len(sessionId) == 0 {
			return
		}

		// now the blockFlights get is only processed for file sharing
		shareTask := this.TaskMgr.GetShareTask(sessionId)
		if shareTask == nil {
			log.Errorf("no share task found with session Id%s", sessionId)
			return
		}

		taskType = shareTask.GetTaskType()

		log.Debugf("task key: %s type %d", sessionId, taskType)
		switch taskType {
		case store.TaskTypeShare:
			reqCh := this.TaskMgr.BlockReqCh()
			req := make([]*types.GetBlockReq, 0)
			for _, v := range blockFlightsMsg.Blocks {
				log.Debugf("push get block req: %s-%s-%d from %s - %s",
					v.GetSessionId(), v.GetFileHash(), blockFlightsMsg.TimeStamp, v.Payment.Sender, peerWalletAddr)
				r := &types.GetBlockReq{
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
			// deprecated

			// if len(blockFlightsMsg.Blocks) == 0 {
			// 	log.Warnf("receive get blocks msg empty")
			// 	return
			// }
			// uploadTask := this.TaskMgr.GetUploadTask(sessionId)
			// if uploadTask == nil {
			// 	log.Debugf("handle get block get upload task %s is nil", sessionId)
			// 	return
			// }
			// if uploadTask.IsTaskPaused() {
			// 	log.Debugf("handle get block get upload task %s is paused", sessionId)
			// 	return
			// }
			// reqCh := uploadTask.GetBlockReq()
			// if reqCh == nil {
			// 	log.Errorf("get task block reqCh is nil for task %s", sessionId)
			// 	return
			// }
			// req := make([]*types.GetBlockReq, 0)
			// for _, v := range blockFlightsMsg.Blocks {
			// 	log.Debugf("push get block req: %s-%s-%d from %s - %s",
			// 		v.GetSessionId(), v.GetFileHash(), blockFlightsMsg.TimeStamp, v.Payment.Sender, peerWalletAddr)
			// 	r := &types.GetBlockReq{
			// 		TimeStamp:     blockFlightsMsg.TimeStamp,
			// 		FileHash:      v.FileHash,
			// 		Hash:          v.Hash,
			// 		Index:         v.Index,
			// 		PeerAddr:      peerWalletAddr,
			// 		WalletAddress: v.Payment.Sender,
			// 		Asset:         v.Payment.Asset,
			// 	}
			// 	req = append(req, r)
			// }
			// log.Debugf("push get block to request")
			// reqCh <- req
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
		id := this.TaskMgr.GetDownloadedTaskId(progressMsg.Hash, this.WalletAddress())
		var prog *store.FileProgress
		if len(id) != 0 {
			prog = this.TaskMgr.GetTaskPeerProgress(id, info.WalletAddr)
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
		nodeInfos, message.WithSign(this.CurrentAccount(), this.Mode), message.WithSyn(msg.MessageId))
	client.P2PSend(peerWalletAddr, resp.MessageId, resp.ToProtoMsg())
}
