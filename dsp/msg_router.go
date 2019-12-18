package dsp

import (
	"context"
	"fmt"
	"strings"
	"time"

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
	"github.com/saveio/themis/common/log"
)

// Receive p2p message receive router
func (this *Dsp) Receive(ctx *network.ComponentContext) {
	msg := message.ReadMessage(ctx.Message())
	peer := ctx.Client()
	if msg == nil || msg.Header == nil {
		log.Debugf("receive nil msg from %s", peer.Address)
		return
	}
	log.Debugf("received msg from peer %s, msgId %d", peer.Address, msg.MessageId)
	if msg.Header.Version != netcom.MESSAGE_VERSION {
		log.Debugf("unrecognized msg version %s", msg.Header.Version)
		return
	}
	log.Debugf("receive msg version:%s, type %s, len: %d", msg.Header.Version, msg.Header.Type, msg.Header.MsgLength)
	switch msg.Header.Type {
	case netcom.MSG_TYPE_FILE:
		this.handleFileMsg(ctx, peer, msg)
	case netcom.MSG_TYPE_BLOCK_FLIGHTS:
		this.handleBlockFlightsMsg(ctx, peer, msg)
	case netcom.MSG_TYPE_PROGRESS:
		this.handleProgressMsg(ctx, peer, msg)
	default:
		log.Debugf("unrecognized msg type %s", msg.Header.Type)
	}
}

// handleFileMsg handle all file msg
func (this *Dsp) handleFileMsg(ctx *network.ComponentContext, peer *network.PeerClient, msg *message.Message) {
	fileMsg := msg.Payload.(*file.File)
	log.Debugf("handleFileMsg %d of file %s from peer:%s, length:%d", fileMsg.Operation, fileMsg.Hash, peer.Address, msg.Header.MsgLength)
	switch fileMsg.Operation {
	case netcom.FILE_OP_FETCH_ASK:
		this.handleFileAskMsg(ctx, peer, fileMsg)
	case netcom.FILE_OP_FETCH_RDY:
		this.handleFileRdyMsg(ctx, peer, fileMsg)
	case netcom.FILE_OP_FETCH_PAUSE:
		this.handleFileFetchPauseMsg(ctx, peer, fileMsg)
	case netcom.FILE_OP_FETCH_RESUME:
		this.handleFileFetchResumeMsg(ctx, peer, fileMsg)
	case netcom.FILE_OP_FETCH_DONE:
		this.handleFileFetchDoneMsg(ctx, peer, fileMsg)
	case netcom.FILE_OP_FETCH_CANCEL:
		this.handleFileFetchCancelMsg(ctx, peer, fileMsg)

	case netcom.FILE_OP_DELETE:
		this.handleFileDeleteMsg(ctx, peer, fileMsg)

	case netcom.FILE_OP_DOWNLOAD_ASK:
		this.handleFileDownloadAskMsg(ctx, peer, fileMsg)
	case netcom.FILE_OP_DOWNLOAD:
		this.handleFileDownloadMsg(ctx, peer, fileMsg)
	case netcom.FILE_OP_DOWNLOAD_OK:
		this.handleFileDownloadOkMsg(ctx, peer, fileMsg)
	case netcom.FILE_OP_DOWNLOAD_CANCEL:
		this.handleFileDownloadCancelMsg(ctx, peer, fileMsg)
	default:
	}
}

func (this *Dsp) handleProgressMsg(ctx *network.ComponentContext, peer *network.PeerClient, msg *message.Message) {
	progressMsg := msg.Payload.(*progress.Progress)
	log.Debugf("handleProgressMsg %d of file %s from peer:%s, length:%d", progressMsg.Operation, progressMsg.Hash, peer.Address, msg.Header.MsgLength)
	switch progressMsg.Operation {
	case netcom.FILE_OP_PROGRESS_REQ:
		this.handleReqProgressMsg(ctx, peer, progressMsg)
	}
}

// handleFileAskMsg. client send file ask msg to storage node for handshake and share file metadata
func (this *Dsp) handleFileAskMsg(ctx *network.ComponentContext, peer *network.PeerClient, fileMsg *file.File) {
	// try to find a exist task id
	height, _ := this.chain.GetCurrentBlockHeight()
	existTaskId := this.taskMgr.TaskId(fileMsg.Hash, this.chain.WalletAddress(), store.TaskTypeDownload)
	if len(existTaskId) == 0 {
		newMsg := message.NewFileMsg(fileMsg.GetHash(), netcom.FILE_OP_FETCH_ACK,
			message.WithSessionId(fileMsg.SessionId),
			message.WithBreakpointHash(""),
			message.WithBreakpointIndex(0),
			message.WithWalletAddress(this.chain.WalletAddress()),
			message.WithSign(this.account),
			message.ChainHeight(height),
		)
		log.Debugf("send new task of file_ack msg to %v", peer)
		err := ctx.Reply(context.Background(), newMsg.ToProtoMsg())
		if err != nil {
			log.Errorf("reply file_ack msg failed", err)
			return
		}
		log.Debugf("reply file_ack msg success")
		return
	}
	log.Debugf("handle file ask existTaskId: %s", existTaskId)
	// handle old task
	state, _ := this.taskMgr.GetTaskState(existTaskId)
	log.Debugf("fetch_ask task exist localId %s, %s state: %d", existTaskId, fileMsg.Hash, state)
	if state == store.TaskStateCancel || state == store.TaskStateFailed || state == store.TaskStateDone {
		log.Warnf("the task has a wrong state of file_ask %s", state)
	}
	currentBlockHash, currentBlockIndex, err := this.taskMgr.GetCurrentSetBlock(existTaskId)
	if err != nil {
		log.Errorf("get current set block err %s", err)
		replyMsg := message.NewFileMsgWithError(fileMsg.GetHash(), netcom.FILE_OP_FETCH_ACK, netcom.MSG_ERROR_INTERNAL_ERROR, fmt.Sprintf("get current set block err %s", err),
			message.WithSign(this.account))
		if err := ctx.Reply(context.Background(), replyMsg.ToProtoMsg()); err != nil {
			log.Errorf("reply file_ack msg failed", err)
			return
		}
		return
	}
	newMsg := message.NewFileMsg(fileMsg.GetHash(), netcom.FILE_OP_FETCH_ACK,
		message.WithSessionId(fileMsg.SessionId),
		message.WithBreakpointHash(currentBlockHash),
		message.WithBreakpointIndex(currentBlockIndex),
		message.WithWalletAddress(this.chain.WalletAddress()),
		message.WithSign(this.account),
		message.ChainHeight(height),
	)
	log.Debugf("fetch task is exist send file_ack msg %v", peer)
	if err := ctx.Reply(context.Background(), newMsg.ToProtoMsg()); err != nil {
		log.Errorf("reply file_ack msg failed", err)
		return
	}
	if state != store.TaskStateDone {
		// set a new task state
		err = this.taskMgr.SetTaskState(existTaskId, store.TaskStateDoing)
		log.Debugf("set task state err: %s", err)
	}
	log.Debugf("reply exist task file_ack msg success")
}

// handleFileRdyMsg. client send ready msg to storage node for telling them it's ready for be fetched
func (this *Dsp) handleFileRdyMsg(ctx *network.ComponentContext, peer *network.PeerClient, fileMsg *file.File) {
	if fileMsg.Tx == nil {
		return
	}
	err := this.waitForTxConfirmed(fileMsg.Tx.Height)
	if err != nil {
		log.Errorf("get block height err %s", err)
		return
	}
	info, _ := this.chain.GetFileInfo(fileMsg.Hash)
	if info == nil {
		log.Errorf("fetch ask file info is nil %s %s", fileMsg.Hash, fileMsg.PayInfo.WalletAddress)
		return
	}
	log.Debugf("get file info %s success", fileMsg.Hash)
	if info.FileOwner.ToBase58() != fileMsg.PayInfo.WalletAddress {
		log.Errorf("receive fetch ask msg from wrong account %s", fileMsg.PayInfo.WalletAddress)
		return
	}
	if fileMsg.PayInfo == nil {
		log.Warnf("fetch_rdy msg no contains wallet address")
		return
	}
	// my task. use my wallet address
	taskId := this.taskMgr.TaskId(fileMsg.Hash, this.chain.WalletAddress(), store.TaskTypeDownload)
	if len(taskId) == 0 {
		// handle new download task. use my wallet address
		taskId, err = this.taskMgr.NewTask(store.TaskTypeDownload)
		log.Debugf("fetch_ask new task %s of file: %s", taskId, fileMsg.Hash)
		if err != nil {
			log.Errorf("new task failed %s", err)
			return
		}
	}
	if !this.taskMgr.IsFileInfoExist(taskId) {
		return
	}
	if err := this.taskMgr.SetTaskInfoWithOptions(taskId,
		task.FileHash(fileMsg.Hash),
		task.Prefix(string(fileMsg.Prefix)),
		task.Walletaddr(this.chain.WalletAddress()),
		task.TotalBlockCnt(uint64(fileMsg.TotalBlockCount))); err != nil {
		log.Errorf("batch set file info fetch ask %s", err)
		return
	}
	if err := this.taskMgr.AddFileSession(taskId, fileMsg.SessionId, fileMsg.PayInfo.WalletAddress, peer.Address, uint64(fileMsg.PayInfo.Asset), fileMsg.PayInfo.UnitPrice); err != nil {
		log.Errorf("add session err in file fetch ask %s", err)
		return
	}
	if err := this.taskMgr.BindTaskId(taskId); err != nil {
		log.Errorf("set task info err in file fetch ask %s", err)
		return
	}
	totalCount, _ := this.taskMgr.GetFileTotalBlockCount(taskId)
	if totalCount != uint64(fileMsg.TotalBlockCount) {
		log.Errorf("add fileblockhashes failed:%s", err)
		return
	}
	if totalCount != info.FileBlockNum {
		log.Errorf("block number is unmatched %d, %d", len(fileMsg.BlockHashes), info.FileBlockNum)
		return
	}
	if err := ctx.Reply(context.Background(), message.NewEmptyMsg().ToProtoMsg()); err != nil {
		log.Errorf("reply file_ack msg failed", err)
	}
	log.Debugf("reply file_rdy msg success")
	log.Debugf("start fetching blocks of taskId:%s, file: %s", taskId, fileMsg.Hash)
	this.taskMgr.SetFileOwner(taskId, info.FileOwner.ToBase58())
	if err := this.startFetchBlocks(fileMsg.Hash, peer.Address, fileMsg.PayInfo.WalletAddress); err != nil {
		log.Errorf("start fetch blocks for file %s failed, err:%s", fileMsg.Hash, err)
	}
}

// handleFileFetchPauseMsg. client send pause msg to storage nodes for telling them to pause the task
func (this *Dsp) handleFileFetchPauseMsg(ctx *network.ComponentContext, peer *network.PeerClient, fileMsg *file.File) {
	// my task. use my wallet address
	taskId := this.taskMgr.TaskId(fileMsg.Hash, this.chain.WalletAddress(), store.TaskTypeDownload)
	log.Debugf("handleFileFetchPauseMsg: of %s, taskId: %s from", fileMsg.Hash, taskId, peer.Address)
	if !this.taskMgr.IsFileInfoExist(taskId) {
		return
	}
	replyMsg := message.NewEmptyMsg()
	err := ctx.Reply(context.Background(), replyMsg.ToProtoMsg())
	if err != nil {
		log.Errorf("taskId: %s, reply pause msg err %s", taskId, err)
		return
	}
	log.Debugf("reply pause msg success, pause fetching blocks")
	this.taskMgr.SetTaskState(taskId, store.TaskStatePause)
}

// handleFileFetchResumeMsg. handle resume msg from client
func (this *Dsp) handleFileFetchResumeMsg(ctx *network.ComponentContext, peer *network.PeerClient, fileMsg *file.File) {
	// my task. use my wallet address
	taskId := this.taskMgr.TaskId(fileMsg.Hash, this.chain.WalletAddress(), store.TaskTypeDownload)
	log.Debugf("handleFileFetchResumeMsg: of %s, taskId: %s from", fileMsg.Hash, taskId, peer.Address)
	if !this.taskMgr.IsFileInfoExist(taskId) {
		return
	}
	replyMsg := message.NewEmptyMsg()
	err := ctx.Reply(context.Background(), replyMsg.ToProtoMsg())
	if err != nil {
		log.Errorf("taskId: %s, reply pause msg err %s", taskId, err)
		return
	}
	log.Debugf("reply resume msg success, resume fetching blocks")
	this.taskMgr.SetTaskState(taskId, store.TaskStateDoing)
}

func (this *Dsp) handleFileFetchDoneMsg(ctx *network.ComponentContext, peer *network.PeerClient, fileMsg *file.File) {
	taskId := this.taskMgr.TaskId(fileMsg.Hash, this.chain.WalletAddress(), store.TaskTypeUpload)
	if !this.taskMgr.IsFileInfoExist(taskId) {
		return
	}
	log.Debugf("receive fetch done msg, save task id:%s fileHash: %s, from %s done", taskId, fileMsg.Hash, peer.Address)
	this.taskMgr.SetUploadProgressDone(taskId, peer.Address)
}

func (this *Dsp) handleFileFetchCancelMsg(ctx *network.ComponentContext, peer *network.PeerClient, fileMsg *file.File) {
	// my task. use my wallet address
	taskId := this.taskMgr.TaskId(fileMsg.Hash, this.chain.WalletAddress(), store.TaskTypeDownload)
	log.Debugf("handleFileFetchCancelMsg: of %s, taskId: %s from", fileMsg.Hash, taskId, peer.Address)
	replyMsg := message.NewEmptyMsg()
	err := ctx.Reply(context.Background(), replyMsg.ToProtoMsg())
	if err != nil {
		log.Errorf("taskId: %s, reply cancel msg err %s", taskId, err)
		return
	}
	log.Debugf("reply cancel msg success, cancel fetching blocks")
	if !this.taskMgr.IsFileInfoExist(taskId) {
		log.Debugf("file info not exist of canceling file %s", fileMsg.Hash)
		return
	}
	this.taskMgr.SetTaskState(taskId, store.TaskStateCancel)
}

// handleFileDeleteMsg. client send delete msg to storage nodes for telling them to delete the file and release the resources.
func (this *Dsp) handleFileDeleteMsg(ctx *network.ComponentContext, peer *network.PeerClient, fileMsg *file.File) {
	if fileMsg.Tx != nil && fileMsg.Tx.Height > 0 {
		err := this.waitForTxConfirmed(fileMsg.Tx.Height)
		if err != nil {
			log.Errorf("get block height err %s", err)
			replyMsg := message.NewFileMsgWithError(fileMsg.Hash, netcom.FILE_OP_DELETE_ACK, serr.DELETE_FILE_TX_UNCONFIRMED, err.Error(),
				message.WithSessionId(fileMsg.SessionId),
				message.WithWalletAddress(this.chain.WalletAddress()),
				message.WithSign(this.account),
			)
			err = ctx.Reply(context.Background(), replyMsg.ToProtoMsg())
			if err != nil {
				log.Errorf("reply delete ok msg failed", err)
			} else {
				log.Debugf("reply delete ack msg success")
			}
			return
		}
	}
	info, err := this.chain.GetFileInfo(fileMsg.Hash)
	if info != nil || (err != nil && strings.Index(err.Error(), "FsGetFileInfo not found") == -1) {
		log.Errorf("delete file info is not nil %s %s", fileMsg.Hash, fileMsg.PayInfo.WalletAddress)
		replyMsg := message.NewFileMsgWithError(fileMsg.Hash, netcom.FILE_OP_DELETE_ACK, serr.DELETE_FILE_FILEINFO_EXISTS, "file info hasn't been deleted",
			message.WithSessionId(fileMsg.SessionId),
			message.WithWalletAddress(this.chain.WalletAddress()),
			message.WithSign(this.account),
		)
		err = ctx.Reply(context.Background(), replyMsg.ToProtoMsg())
		if err != nil {
			log.Errorf("reply delete ok msg failed", err)
		} else {
			log.Debugf("reply delete ack msg success")
		}
		return
	}
	replyMsg := message.NewFileMsg(fileMsg.Hash, netcom.FILE_OP_DELETE_ACK,
		message.WithSessionId(fileMsg.SessionId),
		message.WithWalletAddress(this.chain.WalletAddress()),
		message.WithSign(this.account),
	)
	err = ctx.Reply(context.Background(), replyMsg.ToProtoMsg())
	if err != nil {
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
func (this *Dsp) handleFileDownloadAskMsg(ctx *network.ComponentContext, peer *network.PeerClient, fileMsg *file.File) {
	replyErr := func(sessionId, fileHash string, errorCode uint32, errorMsg string, ctx *network.ComponentContext) {
		replyMsg := message.NewFileMsgWithError(fileMsg.Hash, netcom.FILE_OP_DOWNLOAD_ACK, errorCode, errorMsg,
			message.WithSessionId(sessionId),
			message.WithWalletAddress(this.chain.WalletAddress()),
			message.WithUnitPrice(0),
			message.WithAsset(0),
			message.WithSign(this.account),
		)
		err := ctx.Reply(context.Background(), replyMsg.ToProtoMsg())
		log.Debugf("reply download_ack errmsg code %d, err %s", errorCode, err)
		if err != nil {
			log.Errorf("reply download ack err msg failed", err)
		}
	}
	if !this.chain.CheckFilePrivilege(fileMsg.Hash, fileMsg.PayInfo.WalletAddress) {
		log.Errorf("user %s has no privilege to download this file", fileMsg.PayInfo.WalletAddress)
		replyErr("", fileMsg.Hash, serr.NO_PRIVILEGE_TO_DOWNLOAD, fmt.Sprintf("user %s has no privilege to download this file", fileMsg.PayInfo.WalletAddress), ctx)
		return
	}
	downloadInfoId := this.taskMgr.TaskId(fileMsg.Hash, this.chain.WalletAddress(), store.TaskTypeDownload)
	price, err := this.GetFileUnitPrice(fileMsg.PayInfo.Asset)
	if err != nil {
		replyErr("", fileMsg.Hash, serr.UNITPRICE_ERROR, err.Error(), ctx)
		return
	}
	prefix, err := this.taskMgr.GetFilePrefix(downloadInfoId)
	log.Debugf("get prefix from local: %s", prefix)
	if err != nil {
		replyErr("", fileMsg.Hash, serr.INTERNAL_ERROR, err.Error(), ctx)
		return
	}
	dnsBalance, err := this.channel.GetAvailableBalance(this.dns.DNSNode.WalletAddr)
	if err != nil || dnsBalance == 0 {
		replyErr("", fileMsg.Hash, serr.INTERNAL_ERROR, "no enough balance with dns"+this.dns.DNSNode.WalletAddr, ctx)
		return
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
			replyErr(sessionId, fileMsg.Hash, serr.INTERNAL_ERROR, fmt.Sprintf("can't share %s to, err: %v", fileMsg.Hash, err), ctx)
			return
		}
		totalCount, _ := this.taskMgr.GetFileTotalBlockCount(downloadInfoId)
		replyMsg := message.NewFileMsg(fileMsg.Hash, netcom.FILE_OP_DOWNLOAD_ACK,
			message.WithSessionId(sessionId),
			message.WithBlockHashes(this.taskMgr.FileBlockHashes(downloadInfoId)),
			message.WithTotalBlockCount(int32(totalCount)),
			message.WithWalletAddress(this.chain.WalletAddress()),
			message.WithPrefix(prefix),
			message.WithUnitPrice(price),
			message.WithAsset(fileMsg.PayInfo.Asset),
			message.WithSign(this.account),
		)
		err = ctx.Reply(context.Background(), replyMsg.ToProtoMsg())
		if err != nil {
			log.Errorf("reply download ack  msg failed", err)
		} else {
			this.taskMgr.SetTaskState(localId, store.TaskStateDoing)
			log.Debugf("reply download ack msg success")
		}
		return
	}
	// TODO: check channel balance and router path
	taskId, err := this.taskMgr.NewTask(store.TaskTypeShare)
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

	if !this.taskMgr.IsFileInfoExist(downloadInfoId) {
		replyErr(sessionId, fileMsg.Hash, serr.FILEINFO_NOT_EXIST, "", ctx)
		return
	}
	if this.taskMgr.IsFileInfoExist(downloadInfoId) && rootBlk == nil {
		log.Error("file info exist but root block not found")
		err = this.DeleteDownloadedFile(downloadInfoId)
		if err != nil {
			log.Errorf("delete downloaded file err %s", err)
		}
		replyErr(sessionId, fileMsg.Hash, serr.FILEINFO_NOT_EXIST, "", ctx)
		return
	}

	if this.taskMgr.ShareTaskNum() >= common.MAX_TASKS_NUM {
		replyErr(sessionId, fileMsg.Hash, serr.TOO_MANY_TASKS, "", ctx)
		return
	}
	log.Debugf("sessionId %s blockCount %v %s prefix %s", sessionId, len(this.taskMgr.FileBlockHashes(downloadInfoId)), downloadInfoId, prefix)
	replyMsg := message.NewFileMsg(fileMsg.Hash, netcom.FILE_OP_DOWNLOAD_ACK,
		message.WithSessionId(sessionId),
		message.WithBlockHashes(this.taskMgr.FileBlockHashes(downloadInfoId)),
		message.WithTotalBlockCount(int32(len(this.taskMgr.FileBlockHashes(downloadInfoId)))),
		message.WithWalletAddress(this.chain.WalletAddress()),
		message.WithPrefix(prefix),
		message.WithUnitPrice(price),
		message.WithAsset(fileMsg.PayInfo.Asset),
		message.WithSign(this.account),
	)
	err = ctx.Reply(context.Background(), replyMsg.ToProtoMsg())
	if err != nil {
		log.Errorf("reply download ack  msg failed", err)
	}
	log.Debugf("reply download ack success new share task %s, key %s", fileMsg.Hash, taskId)
	if err := this.taskMgr.SetTaskInfoWithOptions(taskId, task.FileHash(fileMsg.Hash), task.Walletaddr(fileMsg.PayInfo.WalletAddress)); err != nil {
		log.Errorf("[DSP handleFileDownloadAskMsg] batch commit file info failed, err: %s", err)
	}
	this.taskMgr.BindTaskId(taskId)
}

// handleFileDownloadMsg. client send to peers and telling them the file will be downloaded soon
func (this *Dsp) handleFileDownloadMsg(ctx *network.ComponentContext, peer *network.PeerClient, fileMsg *file.File) {
	taskKey := this.taskMgr.TaskId(fileMsg.Hash, fileMsg.PayInfo.WalletAddress, store.TaskTypeShare)
	// TODO: check channel balance and router path
	this.taskMgr.AddShareTo(taskKey, fileMsg.PayInfo.WalletAddress)
	hostAddr, _ := this.dns.GetExternalIP(fileMsg.PayInfo.WalletAddress)
	log.Debugf("Set host addr after recv file download %s - %s", fileMsg.PayInfo.WalletAddress, hostAddr)
	err := ctx.Reply(context.Background(), message.NewEmptyMsg().ToProtoMsg())
	if err != nil {
		log.Errorf("reply download msg failed, err %s", err)
	}
	log.Debugf("reply download msg success")
	downloadTaskId := this.taskMgr.TaskId(fileMsg.Hash, this.chain.WalletAddress(), store.TaskTypeDownload)
	fileName, _ := this.taskMgr.GetFileName(downloadTaskId)
	fileOwner, _ := this.taskMgr.GetFileOwner(downloadTaskId)
	this.taskMgr.EmitNotification(taskKey, task.ShareStateBegin, fileMsg.Hash, fileName, fileOwner, fileMsg.PayInfo.WalletAddress, 0, 0)
}

func (this *Dsp) handleFileDownloadCancelMsg(ctx *network.ComponentContext, peer *network.PeerClient, fileMsg *file.File) {
	taskId := this.taskMgr.TaskId(fileMsg.Hash, fileMsg.PayInfo.WalletAddress, store.TaskTypeShare)
	if !this.taskMgr.TaskExist(taskId) {
		log.Warnf("share task not exist %s", fileMsg.Hash)
		err := ctx.Reply(context.Background(), message.NewEmptyMsg().ToProtoMsg())
		if err != nil {
			log.Errorf("reply download msg failed, err %s", err)
		} else {
			log.Debugf("reply download cancel msg success")
		}
		return
	}
	// TODO: check unpaid amount
	this.taskMgr.CleanTask(taskId)
	log.Debugf("delete share task of %s", taskId)
	err := ctx.Reply(context.Background(), message.NewEmptyMsg().ToProtoMsg())
	if err != nil {
		log.Errorf("reply download msg failed, err %s", err)
	} else {
		log.Debugf("reply download cancel msg success")
	}
	downloadTaskId := this.taskMgr.TaskId(fileMsg.Hash, this.chain.WalletAddress(), store.TaskTypeDownload)
	fileName, _ := this.taskMgr.GetFileName(downloadTaskId)
	fileOwner, _ := this.taskMgr.GetFileOwner(downloadTaskId)
	this.taskMgr.EmitNotification(taskId, task.ShareStateEnd, fileMsg.Hash, fileName, fileOwner, fileMsg.PayInfo.WalletAddress, 0, 0)
}

// handleFileDownloadOkMsg. client send download ok msg to remove peers for telling them the task is finished
func (this *Dsp) handleFileDownloadOkMsg(ctx *network.ComponentContext, peer *network.PeerClient, fileMsg *file.File) {
	taskId := this.taskMgr.TaskId(fileMsg.Hash, fileMsg.PayInfo.WalletAddress, store.TaskTypeShare)
	if !this.taskMgr.TaskExist(taskId) {
		log.Errorf("share task not exist %s", fileMsg.Hash)
		return
	}
	// TODO: check unpaid amount
	this.taskMgr.CleanTask(taskId)
	log.Debugf("delete share task of %s", taskId)
	err := ctx.Reply(context.Background(), message.NewEmptyMsg().ToProtoMsg())
	if err != nil {
		log.Errorf("reply download msg failed, err %s", err)
	}
	log.Debugf("reply download ack msg success")
	downloadTaskId := this.taskMgr.TaskId(fileMsg.Hash, this.chain.WalletAddress(), store.TaskTypeDownload)
	fileName, _ := this.taskMgr.GetFileName(downloadTaskId)
	fileOwner, _ := this.taskMgr.GetFileOwner(downloadTaskId)
	this.taskMgr.EmitNotification(taskId, task.ShareStateEnd, fileMsg.Hash, fileName, fileOwner, fileMsg.PayInfo.WalletAddress, 0, 0)
}

// handleBlockFlightsMsg handle block flight msg
func (this *Dsp) handleBlockFlightsMsg(ctx *network.ComponentContext, peer *network.PeerClient, msg *message.Message) {
	blockFlightsMsg := msg.Payload.(*block.BlockFlights)
	if blockFlightsMsg == nil || len(blockFlightsMsg.Blocks) == 0 {
		log.Error("msg Payload is not valid *block.BlockFlights")
		return
	}
	switch blockFlightsMsg.Blocks[0].Operation {
	case netcom.BLOCK_OP_NONE:
		taskId := this.taskMgr.TaskId(blockFlightsMsg.Blocks[0].FileHash, this.chain.WalletAddress(), store.TaskTypeDownload)
		log.Debugf("taskId: %s, sessionId: %s receive %d blocks from peer:%s", taskId, blockFlightsMsg.Blocks[0].SessionId, len(blockFlightsMsg.Blocks), peer.Address)
		exist := this.taskMgr.TaskExist(taskId)
		if !exist {
			log.Debugf("task %s not exist", blockFlightsMsg.Blocks[0].FileHash)
			return
		}
		blocks := make([]*task.BlockResp, 0)
		for _, blockMsg := range blockFlightsMsg.Blocks {
			isDownloaded := this.taskMgr.IsBlockDownloaded(taskId, blockMsg.Hash, uint32(blockMsg.Index))
			if !isDownloaded {
				b := &task.BlockResp{
					Hash:      blockMsg.Hash,
					Index:     blockMsg.Index,
					PeerAddr:  peer.Address,
					Block:     blockMsg.Data,
					Tag:       blockMsg.Tag,
					Offset:    blockMsg.Offset,
					PaymentId: blockFlightsMsg.PaymentId,
				}
				blocks = append(blocks, b)
				log.Debugf("append block %s finished", blockMsg.Hash)
			} else {
				log.Debugf("the block %s has downloaded", blockMsg.Hash)
			}
		}
		if len(blocks) == 0 {
			log.Debug("all download blocks have been downloaded")
		} else {
			log.Debugf("push %d blocks to channel", len(blocks))
			this.taskMgr.PushGetBlockFlights(taskId, blockFlightsMsg.Blocks[0].SessionId, blocks, blockFlightsMsg.TimeStamp)
		}

		emptyMsg := message.NewEmptyMsg()
		err := ctx.Reply(context.Background(), emptyMsg.ToProtoMsg())
		if err != nil {
			log.Errorf("reply block flight msg failed", err)
		} else {
			log.Debugf("reply block flight msg success")
		}
	case netcom.BLOCK_OP_GET:
		blockMsg := blockFlightsMsg.Blocks[0]
		sessionId := blockMsg.SessionId
		for _, blk := range blockFlightsMsg.Blocks {
			log.Debugf("session: %s handle get block %s-%s-%d from %s", sessionId, blk.FileHash, blk.Hash, blk.Index, peer.Address)
			if len(sessionId) == 0 {
				return
			}
			exist := this.taskMgr.TaskExist(sessionId)
			if !exist {
				log.Debugf("task %s not exist", blockMsg.FileHash)
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
				log.Debugf("push get block req: %s-%s-%d from %s - %s", v.GetSessionId(), v.GetFileHash(), blockFlightsMsg.TimeStamp, v.Payment.Sender, peer.Address)
				r := &task.GetBlockReq{
					TimeStamp:     blockFlightsMsg.TimeStamp,
					FileHash:      v.FileHash,
					Hash:          v.Hash,
					Index:         v.Index,
					PeerAddr:      peer.Address,
					WalletAddress: v.Payment.Sender,
					Asset:         v.Payment.Asset,
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
				log.Debugf("push get block req: %s-%s-%d from %s - %s", v.GetSessionId(), v.GetFileHash(), blockFlightsMsg.TimeStamp, v.Payment.Sender, peer.Address)
				r := &task.GetBlockReq{
					TimeStamp:     blockFlightsMsg.TimeStamp,
					FileHash:      v.FileHash,
					Hash:          v.Hash,
					Index:         v.Index,
					PeerAddr:      peer.Address,
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
func (this *Dsp) handleReqProgressMsg(ctx *network.ComponentContext, peer *network.PeerClient, progressMsg *progress.Progress) {
	nodeInfos := make([]*progress.ProgressInfo, 0)
	for _, info := range progressMsg.Infos {
		id := this.taskMgr.TaskId(progressMsg.Hash, info.WalletAddr, store.TaskTypeShare)
		if len(id) == 0 {
			log.Errorf("task id is empty of hash %s, wallet %s, type %d", progressMsg.Hash, info.WalletAddr, store.TaskTypeShare)
		}
		prog := this.taskMgr.GetTaskPeerProgress(id, info.NodeAddr)
		log.Debugf("handle req progress msg, get progress of id %s, addr %s, count %d", id, info.NodeAddr, prog)
		nodeInfos = append(nodeInfos, &progress.ProgressInfo{
			NodeAddr: info.NodeAddr,
			Count:    int32(prog),
		})
	}
	resp := message.NewProgressMsg(this.WalletAddress(), progressMsg.Hash, netcom.FILE_OP_PROGRESS, nodeInfos, message.WithSign(this.CurrentAccount()))
	ctx.Reply(context.Background(), resp.ToProtoMsg())
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
	waitSuccess, err := this.chain.WaitForGenerateBlock(time.Duration(timeout)*time.Second, uint32(blockHeight-uint64(currentBlockHeight)))
	if err != nil || !waitSuccess {
		log.Errorf("get block height err %s %d %d", err, currentBlockHeight, blockHeight)
		return fmt.Errorf("get block height err %d %d", currentBlockHeight, blockHeight)
	}
	return nil
}
