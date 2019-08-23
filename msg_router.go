package dsp

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/saveio/carrier/network"
	"github.com/saveio/dsp-go-sdk/common"
	serr "github.com/saveio/dsp-go-sdk/error"
	netcom "github.com/saveio/dsp-go-sdk/network/common"
	"github.com/saveio/dsp-go-sdk/network/message"
	"github.com/saveio/dsp-go-sdk/network/message/types/block"
	"github.com/saveio/dsp-go-sdk/network/message/types/file"
	"github.com/saveio/dsp-go-sdk/network/message/types/payment"
	"github.com/saveio/dsp-go-sdk/task"
	"github.com/saveio/themis/common/log"
)

// Receive p2p message receive router
func (this *Dsp) Receive(ctx *network.ComponentContext) {
	msg := message.ReadMessage(ctx.Message())
	peer := ctx.Client()
	log.Debugf("received msg from peer %s, msgId %d", peer.Address, msg.MessageId)
	if msg == nil || msg.Header == nil {
		log.Debugf("receive nil msg from %s", peer.Address)
		return
	}
	if msg.Header.Version != netcom.MESSAGE_VERSION {
		log.Debugf("unrecognized msg version %s", msg.Header.Version)
		return
	}
	log.Debugf("receive msg.Header.Type %s", msg.Header.Type)
	switch msg.Header.Type {
	case netcom.MSG_TYPE_FILE:
		this.handleFileMsg(ctx, peer, msg)
	case netcom.MSG_TYPE_BLOCK:
		this.handleBlockMsg(ctx, peer, msg)
	case netcom.MSG_TYPE_PAYMENT:
		this.handlePaymentMsg(ctx, peer, msg)
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

// handleFileAskMsg. client send file ask msg to storage node for handshake and share file metadata
func (this *Dsp) handleFileAskMsg(ctx *network.ComponentContext, peer *network.PeerClient, fileMsg *file.File) {
	//TODO: verify & save info
	// task exist at runtime
	localId := this.taskMgr.TaskId(fileMsg.Hash, this.WalletAddress(), task.TaskTypeDownload)
	if len(localId) > 0 && this.taskMgr.TaskExist(localId) {
		state := this.taskMgr.GetTaskState(localId)
		log.Debugf("fetch_ask task exist localId %s, %s state: %d", localId, fileMsg.Hash, state)
		if state == task.TaskStateCancel || state == task.TaskStateFailed || state == task.TaskStateDone {
			log.Warnf("the task has a wrong state of file_ask %s", state)
		}
		currentBlockHash, currentBlockIndex, err := this.taskMgr.GetCurrentSetBlock(localId)
		if err != nil {
			log.Errorf("get current set block err %s", err)
			return
		}
		err = this.taskMgr.AddFileSession(localId, fileMsg.SessionId, fileMsg.PayInfo.WalletAddress, peer.Address, uint64(fileMsg.PayInfo.Asset), fileMsg.PayInfo.UnitPrice)
		if err != nil {
			log.Errorf("add session err in file fetch ask %s", err)
			return
		}
		newMsg := message.NewFileFetchAck(fileMsg.SessionId, fileMsg.GetHash(), currentBlockHash, currentBlockIndex)
		log.Debugf("fetch task is exist send file_ack msg %v", peer)
		err = ctx.Reply(context.Background(), newMsg.ToProtoMsg())
		if err != nil {
			log.Errorf("reply file_ack msg failed", err)
		} else {
			err = this.taskMgr.SetTaskState(localId, task.TaskStateDoing)
			log.Debugf("set task state err: %s", err)
			log.Debugf("reply file_ack msg success")
		}
		return
	}
	// my task. use my wallet address
	taskId, err := this.taskMgr.NewTask(task.TaskTypeDownload)
	log.Debugf("fetch_ask new task %s", taskId)
	if err != nil {
		log.Errorf("new task failed %s", err)
		return
	}
	this.taskMgr.SetTaskInfos(taskId, fileMsg.Hash, "", "", this.WalletAddress())
	err = this.taskMgr.AddFileSession(taskId, fileMsg.SessionId, fileMsg.PayInfo.WalletAddress, peer.Address, uint64(fileMsg.PayInfo.Asset), fileMsg.PayInfo.UnitPrice)
	if err != nil {
		log.Errorf("add session err in file fetch ask %s", err)
		return
	}
	err = this.taskMgr.BindTaskId(taskId)
	if err != nil {
		log.Errorf("set task info err in file fetch ask %s", err)
		return
	}
	totalCount := this.taskMgr.GetFileTotalBlockCount(taskId)
	hashListLen := uint64(len(fileMsg.BlockHashes))
	if totalCount < hashListLen {
		err := this.taskMgr.AddFileBlockHashes(taskId, fileMsg.BlockHashes)
		if err != nil {
			log.Errorf("add fileblockhashes failed:%s", err)
			return
		}
		err = this.taskMgr.BatchSetFileInfo(taskId, nil, fileMsg.Prefix, nil, hashListLen)
		if err != nil {
			log.Errorf("SetFileInfoFields failed:%s", err)
			return
		}
	}
	newMsg := message.NewFileFetchAck(fileMsg.SessionId, fileMsg.GetHash(), "", 0)
	log.Debugf("send file_ack msg %v %v", peer, newMsg)
	err = ctx.Reply(context.Background(), newMsg.ToProtoMsg())
	if err != nil {
		log.Errorf("reply file_ack msg failed", err)
	}
	log.Debugf("reply file_ack msg success")
}

// handleFileRdyMsg. client send ready msg to storage node for telling them it's ready for be fetched
func (this *Dsp) handleFileRdyMsg(ctx *network.ComponentContext, peer *network.PeerClient, fileMsg *file.File) {
	// my task. use my wallet address
	taskId := this.taskMgr.TaskId(fileMsg.Hash, this.WalletAddress(), task.TaskTypeDownload)
	if fileMsg.Tx == nil {
		return
	}
	err := this.waitForTxConfirmed(fileMsg.Tx.Height)
	if err != nil {
		log.Errorf("get block height err %s", err)
		this.taskMgr.DeleteTask(taskId, false)
		return
	}
	info, _ := this.Chain.Native.Fs.GetFileInfo(fileMsg.Hash)
	if info == nil {
		log.Errorf("fetch ask file info is nil %s %s", fileMsg.Hash, fileMsg.PayInfo.WalletAddress)
		this.taskMgr.DeleteTask(taskId, false)
		return
	}
	log.Debugf("get file info %s success", fileMsg.Hash)
	if info.FileOwner.ToBase58() != fileMsg.PayInfo.WalletAddress {
		log.Errorf("receive fetch ask msg from wrong account %s", fileMsg.PayInfo.WalletAddress)
		this.taskMgr.DeleteTask(taskId, false)
		return
	}
	totalCount := this.taskMgr.GetFileTotalBlockCount(taskId)
	if totalCount != info.FileBlockNum {
		log.Errorf("block number is unmatched %d, %d", len(fileMsg.BlockHashes), info.FileBlockNum)
		this.taskMgr.DeleteTask(taskId, false)
		return
	}
	if !this.taskMgr.IsFileInfoExist(taskId) {
		return
	}
	if fileMsg.PayInfo == nil {
		log.Warnf("fetch_rdy msg no contains wallet address")
		return
	}
	log.Debugf("start fetching blocks of taskId:%s, file: %s", taskId, fileMsg.Hash)
	this.taskMgr.SetFileOwner(taskId, info.FileOwner.ToBase58())
	err = this.startFetchBlocks(fileMsg.Hash, peer.Address, fileMsg.PayInfo.WalletAddress)
	if err != nil {
		log.Errorf("start fetch blocks for file %s failed, err:%s", fileMsg.Hash, err)
	}
}

// handleFileFetchPauseMsg. client send pause msg to storage nodes for telling them to pause the task
func (this *Dsp) handleFileFetchPauseMsg(ctx *network.ComponentContext, peer *network.PeerClient, fileMsg *file.File) {
	// my task. use my wallet address
	taskId := this.taskMgr.TaskId(fileMsg.Hash, this.WalletAddress(), task.TaskTypeDownload)
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
	this.taskMgr.SetTaskState(taskId, task.TaskStatePause)
}

// handleFileFetchResumeMsg. handle resume msg from client
func (this *Dsp) handleFileFetchResumeMsg(ctx *network.ComponentContext, peer *network.PeerClient, fileMsg *file.File) {
	// my task. use my wallet address
	taskId := this.taskMgr.TaskId(fileMsg.Hash, this.WalletAddress(), task.TaskTypeDownload)
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
	this.taskMgr.SetTaskState(taskId, task.TaskStateDoing)
}

func (this *Dsp) handleFileFetchDoneMsg(ctx *network.ComponentContext, peer *network.PeerClient, fileMsg *file.File) {
	taskId := this.taskMgr.TaskId(fileMsg.Hash, this.WalletAddress(), task.TaskTypeUpload)
	if !this.taskMgr.IsFileInfoExist(taskId) {
		return
	}
	log.Debugf("receive fetch done msg, save task id:%s fileHash: %s done", taskId, fileMsg.Hash)
	this.taskMgr.SetUploadProgressDone(taskId, peer.Address)
}

func (this *Dsp) handleFileFetchCancelMsg(ctx *network.ComponentContext, peer *network.PeerClient, fileMsg *file.File) {
	// my task. use my wallet address
	taskId := this.taskMgr.TaskId(fileMsg.Hash, this.WalletAddress(), task.TaskTypeDownload)
	log.Debugf("handleFileFetchCancelMsg: of %s, taskId: %s from", fileMsg.Hash, taskId, peer.Address)
	if !this.taskMgr.IsFileInfoExist(taskId) {
		return
	}
	replyMsg := message.NewEmptyMsg()
	err := ctx.Reply(context.Background(), replyMsg.ToProtoMsg())
	if err != nil {
		log.Errorf("taskId: %s, reply cancel msg err %s", taskId, err)
		return
	}
	log.Debugf("reply cancel msg success, cancel fetching blocks")
	this.taskMgr.SetTaskState(taskId, task.TaskStateCancel)
}

// handleFileDeleteMsg. client send delete msg to storage nodes for telling them to delete the file and release the resources.
func (this *Dsp) handleFileDeleteMsg(ctx *network.ComponentContext, peer *network.PeerClient, fileMsg *file.File) {
	if fileMsg.Tx != nil && fileMsg.Tx.Height > 0 {
		err := this.waitForTxConfirmed(fileMsg.Tx.Height)
		if err != nil {
			log.Errorf("get block height err %s", err)
			replyMsg := message.NewFileDeleteAck(fileMsg.SessionId, fileMsg.Hash, serr.DELETE_FILE_TX_UNCONFIRMED, err.Error())
			err = ctx.Reply(context.Background(), replyMsg.ToProtoMsg())
			if err != nil {
				log.Errorf("reply delete ok msg failed", err)
			} else {
				log.Debugf("reply delete ack msg success")
			}
			return
		}
	}
	info, err := this.Chain.Native.Fs.GetFileInfo(fileMsg.Hash)
	if info != nil || strings.Index(err.Error(), "FsGetFileInfo not found") == -1 {
		log.Errorf("delete file info is not nil %s %s", fileMsg.Hash, fileMsg.PayInfo.WalletAddress)
		replyMsg := message.NewFileDeleteAck(fileMsg.SessionId, fileMsg.Hash, serr.DELETE_FILE_FILEINFO_EXISTS, "file info hasn't been deleted")
		err = ctx.Reply(context.Background(), replyMsg.ToProtoMsg())
		if err != nil {
			log.Errorf("reply delete ok msg failed", err)
		} else {
			log.Debugf("reply delete ack msg success")
		}
		return
	}
	replyMsg := message.NewFileDeleteAck(fileMsg.SessionId, fileMsg.Hash, serr.SUCCESS, "")
	err = ctx.Reply(context.Background(), replyMsg.ToProtoMsg())
	if err != nil {
		log.Errorf("reply delete ok msg failed", err)
	} else {
		log.Debugf("reply delete ack msg success")
	}
	// TODO: check file owner
	taskId := this.taskMgr.TaskId(fileMsg.Hash, this.WalletAddress(), task.TaskTypeDownload)
	err = this.DeleteDownloadedFile(taskId)
	if err != nil {
		log.Errorf("delete downloaded file failed", err)
	}
}

// handleFileDownloadAskMsg. client send download ask msg to peers for download
func (this *Dsp) handleFileDownloadAskMsg(ctx *network.ComponentContext, peer *network.PeerClient, fileMsg *file.File) {
	if !this.CheckFilePrivilege(fileMsg.Hash, fileMsg.PayInfo.WalletAddress) {
		log.Errorf("user %s has no privilege to download this file", fileMsg.PayInfo.WalletAddress)
		return
	}
	replyErr := func(sessionId, fileHash string, errorCode uint32, errorMsg string, ctx *network.ComponentContext) {
		replyMsg := message.NewFileDownloadAck(sessionId, fileMsg.Hash, nil, "", "", 0, 0, errorCode, errorMsg)
		err := ctx.Reply(context.Background(), replyMsg.ToProtoMsg())
		log.Debugf("reply download_ack errmsg code %d, err %s", errorCode, err)
		if err != nil {
			log.Errorf("reply download ack err msg failed", err)
		}
	}
	downloadInfoId := this.taskMgr.TaskId(fileMsg.Hash, this.WalletAddress(), task.TaskTypeDownload)
	price, err := this.GetFileUnitPrice(fileMsg.PayInfo.Asset)
	if err != nil {
		replyErr("", fileMsg.Hash, serr.UNITPRICE_ERROR, err.Error(), ctx)
		return
	}
	prefix, err := this.taskMgr.GetFilePrefix(downloadInfoId)
	if err != nil {
		replyErr("", fileMsg.Hash, serr.INTERNAL_ERROR, err.Error(), ctx)
		return
	}
	localId := this.taskMgr.TaskId(fileMsg.Hash, fileMsg.PayInfo.WalletAddress, task.TaskTypeShare)
	if this.taskMgr.TaskExist(localId) {
		// old task
		sessionId, err := this.taskMgr.GetSessionId(localId, "")
		if err != nil {
			replyErr(sessionId, fileMsg.Hash, serr.INTERNAL_ERROR, err.Error(), ctx)
			return
		}
		replyMsg := message.NewFileDownloadAck(sessionId, fileMsg.Hash, this.taskMgr.FileBlockHashes(downloadInfoId),
			this.WalletAddress(), prefix,
			price, fileMsg.PayInfo.Asset, serr.SUCCESS, "")
		err = ctx.Reply(context.Background(), replyMsg.ToProtoMsg())
		if err != nil {
			log.Errorf("reply download ack  msg failed", err)
		} else {
			this.taskMgr.SetTaskState(localId, task.TaskStateDoing)
			log.Debugf("reply download ack msg success")
		}
		return
	}
	// TODO: check channel balance and router path
	taskId, err := this.taskMgr.NewTask(task.TaskTypeShare)
	if err != nil {
		replyErr("", fileMsg.Hash, serr.INTERNAL_ERROR, err.Error(), ctx)
		return
	}
	sessionId, err := this.taskMgr.GetSessionId(taskId, "")
	if err != nil {
		replyErr(sessionId, fileMsg.Hash, serr.INTERNAL_ERROR, err.Error(), ctx)
		return
	}
	rootBlk := this.Fs.GetBlock(fileMsg.Hash)

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
	replyMsg := message.NewFileDownloadAck(sessionId, fileMsg.Hash, this.taskMgr.FileBlockHashes(downloadInfoId),
		this.WalletAddress(), prefix,
		price, fileMsg.PayInfo.Asset, serr.SUCCESS, "")
	err = ctx.Reply(context.Background(), replyMsg.ToProtoMsg())
	if err != nil {
		log.Errorf("reply download ack  msg failed", err)
	}
	log.Debugf("reply download ack success new share task %s, key %s", fileMsg.Hash, taskId)
	this.taskMgr.SetTaskInfos(taskId, fileMsg.Hash, "", "", fileMsg.PayInfo.WalletAddress)
	this.taskMgr.BindTaskId(taskId)
}

// handleFileDownloadMsg. client send to peers and telling them the file will be downloaded soon
func (this *Dsp) handleFileDownloadMsg(ctx *network.ComponentContext, peer *network.PeerClient, fileMsg *file.File) {
	taskKey := this.taskMgr.TaskId(fileMsg.Hash, fileMsg.PayInfo.WalletAddress, task.TaskTypeShare)
	// TODO: check channel balance and router path
	this.taskMgr.AddShareTo(taskKey, fileMsg.PayInfo.WalletAddress)
	hostAddr, _ := this.GetExternalIP(fileMsg.PayInfo.WalletAddress)
	log.Debugf("Set host addr after recv file download %s - %s", fileMsg.PayInfo.WalletAddress, hostAddr)
	err := ctx.Reply(context.Background(), message.NewEmptyMsg().ToProtoMsg())
	if err != nil {
		log.Errorf("reply download msg failed, err %s", err)
	}
	log.Debugf("reply download msg success")
	downloadTaskId := this.taskMgr.TaskId(fileMsg.Hash, this.WalletAddress(), task.TaskTypeDownload)
	fileName, _ := this.taskMgr.GetFileName(downloadTaskId)
	fileOwner, _ := this.taskMgr.GetFileOwner(downloadTaskId)
	this.taskMgr.EmitNotification(taskKey, task.ShareStateBegin, fileMsg.Hash, fileName, fileOwner, fileMsg.PayInfo.WalletAddress, 0, 0)
}

func (this *Dsp) handleFileDownloadCancelMsg(ctx *network.ComponentContext, peer *network.PeerClient, fileMsg *file.File) {
	taskId := this.taskMgr.TaskId(fileMsg.Hash, fileMsg.PayInfo.WalletAddress, task.TaskTypeShare)
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
	this.taskMgr.DeleteTask(taskId, true)
	log.Debugf("delete share task of %s", taskId)
	err := ctx.Reply(context.Background(), message.NewEmptyMsg().ToProtoMsg())
	if err != nil {
		log.Errorf("reply download msg failed, err %s", err)
	} else {
		log.Debugf("reply download cancel msg success")
	}
	downloadTaskId := this.taskMgr.TaskId(fileMsg.Hash, this.WalletAddress(), task.TaskTypeDownload)
	fileName, _ := this.taskMgr.GetFileName(downloadTaskId)
	fileOwner, _ := this.taskMgr.GetFileOwner(downloadTaskId)
	this.taskMgr.EmitNotification(taskId, task.ShareStateEnd, fileMsg.Hash, fileName, fileOwner, fileMsg.PayInfo.WalletAddress, 0, 0)
}

// handleFileDownloadOkMsg. client send download ok msg to remove peers for telling them the task is finished
func (this *Dsp) handleFileDownloadOkMsg(ctx *network.ComponentContext, peer *network.PeerClient, fileMsg *file.File) {
	taskId := this.taskMgr.TaskId(fileMsg.Hash, fileMsg.PayInfo.WalletAddress, task.TaskTypeShare)
	if !this.taskMgr.TaskExist(taskId) {
		log.Errorf("share task not exist %s", fileMsg.Hash)
		return
	}
	// TODO: check unpaid amount
	this.taskMgr.DeleteTask(taskId, true)
	log.Debugf("delete share task of %s", taskId)
	err := ctx.Reply(context.Background(), message.NewEmptyMsg().ToProtoMsg())
	if err != nil {
		log.Errorf("reply download msg failed, err %s", err)
	}
	log.Debugf("reply download ack msg success")
	downloadTaskId := this.taskMgr.TaskId(fileMsg.Hash, this.WalletAddress(), task.TaskTypeDownload)
	fileName, _ := this.taskMgr.GetFileName(downloadTaskId)
	fileOwner, _ := this.taskMgr.GetFileOwner(downloadTaskId)
	this.taskMgr.EmitNotification(taskId, task.ShareStateEnd, fileMsg.Hash, fileName, fileOwner, fileMsg.PayInfo.WalletAddress, 0, 0)
}

// handleBlockMsg handle all file msg
func (this *Dsp) handleBlockMsg(ctx *network.ComponentContext, peer *network.PeerClient, msg *message.Message) {
	blockMsg := msg.Payload.(*block.Block)

	switch blockMsg.Operation {
	case netcom.BLOCK_OP_NONE:
		taskId := this.taskMgr.TaskId(blockMsg.FileHash, this.WalletAddress(), task.TaskTypeDownload)
		log.Debugf("taskId: %s, sessionId: %s receive block %d %s-%s-%d from peer:%s, length:%d", taskId, blockMsg.SessionId, blockMsg.Operation, blockMsg.FileHash, blockMsg.Hash, blockMsg.Index, peer.Address, msg.Header.MsgLength)
		exist := this.taskMgr.TaskExist(taskId)
		if !exist {
			log.Debugf("task %s not exist", blockMsg.FileHash)
			return
		}
		isDownloaded := this.taskMgr.IsBlockDownloaded(taskId, blockMsg.Hash, uint32(blockMsg.Index))
		if !isDownloaded {
			this.taskMgr.PushGetBlock(taskId, blockMsg.SessionId, &task.BlockResp{
				Hash:     blockMsg.Hash,
				Index:    blockMsg.Index,
				PeerAddr: peer.Address,
				Block:    blockMsg.Data,
				Tag:      blockMsg.Tag,
				Offset:   blockMsg.Offset,
			})
			log.Debugf("push block finished")
		} else {
			log.Debugf("the block has downloaded")
		}
		emptyMsg := message.NewEmptyMsg()
		err := ctx.Reply(context.Background(), emptyMsg.ToProtoMsg())
		if err != nil {
			log.Errorf("reply block msg failed", err)
		} else {
			log.Debugf("reply block msg success")
		}
	case netcom.BLOCK_OP_GET:
		sessionId := blockMsg.SessionId
		log.Debugf("session: %s handle get block %s-%s-%d from %s", sessionId, blockMsg.FileHash, blockMsg.Hash, blockMsg.Index, peer.Address)
		if len(sessionId) == 0 {
			return
		}
		exist := this.taskMgr.TaskExist(sessionId)
		if !exist {
			log.Debugf("task %s not exist", blockMsg.FileHash)
			return
		}
		taskType := this.taskMgr.TaskType(sessionId)
		log.Debugf("task key:%s type %d", sessionId, taskType)
		switch taskType {
		case task.TaskTypeUpload:
			pause, sdkerr := this.checkIfPause(sessionId, blockMsg.FileHash)
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
			log.Debugf("push get block to request")
			reqCh <- &task.GetBlockReq{
				FileHash: blockMsg.FileHash,
				Hash:     blockMsg.Hash,
				Index:    blockMsg.Index,
				PeerAddr: peer.Address,
			}
			return
		case task.TaskTypeDownload, task.TaskTypeShare:
			reqCh := this.taskMgr.BlockReqCh()
			log.Debugf("push get block req: %s-%s-%s-%d-%s", sessionId, blockMsg.FileHash, blockMsg.Hash, blockMsg.Index, peer.Address)
			reqCh <- &task.GetBlockReq{
				FileHash:      blockMsg.FileHash,
				Hash:          blockMsg.Hash,
				Index:         blockMsg.Index,
				PeerAddr:      peer.Address,
				WalletAddress: blockMsg.Payment.Sender,
				Asset:         blockMsg.Payment.Asset,
			}
			log.Debugf("push get block req done")
		default:
			log.Debugf("handle block get msg, tasktype not found %v", taskType)
		}
	default:
	}
}

// handlePaymentMsg. handle payment msg
func (this *Dsp) handlePaymentMsg(ctx *network.ComponentContext, peer *network.PeerClient, msg *message.Message) {
	paymentMsg := msg.Payload.(*payment.Payment)
	log.Debugf("received paymentMsg: %v sender:%v, asset:%d, amount:%d", paymentMsg.PaymentId, paymentMsg.Sender, paymentMsg.Asset, paymentMsg.Amount)
	// check
	pay, err := this.Channel.GetPayment(paymentMsg.PaymentId)
	if err != nil || pay == nil {
		log.Errorf("get payment from db err %s, pay %v", err, pay)
		return
	}
	if pay.WalletAddress != paymentMsg.Sender || pay.Amount != paymentMsg.Amount {
		log.Errorf("payment %v is different from payment msg %v", pay, paymentMsg)
		return
	}
	err = this.Channel.DeletePayment(paymentMsg.PaymentId)
	if err != nil {
		log.Errorf("delete payment from db err %s", err)
		return
	}
	// delete record
	taskKey := this.taskMgr.TaskId(paymentMsg.FileHash, paymentMsg.Sender, task.TaskTypeShare)
	log.Debugf("delete payment success, taskId:%s, paymentId:%s", taskKey, paymentMsg.PaymentId)
	err = this.taskMgr.DeleteFileUnpaid(taskKey, paymentMsg.Sender, paymentMsg.Asset, paymentMsg.Amount)
	if err != nil {
		log.Errorf("delete share file info %s", err)
		return
	}
	log.Debugf("delete unpaid success %v", paymentMsg)
	err = ctx.Reply(context.Background(), message.NewEmptyMsg().ToProtoMsg())
	if err != nil {
		log.Errorf("reply delete ok msg failed", err)
	}
	log.Debugf("reply handle payment msg")
	downloadTaskId := this.taskMgr.TaskId(paymentMsg.FileHash, this.WalletAddress(), task.TaskTypeDownload)
	fileName, _ := this.taskMgr.GetFileName(downloadTaskId)
	fileOwner, _ := this.taskMgr.GetFileOwner(downloadTaskId)
	this.taskMgr.EmitNotification(taskKey, task.ShareStateReceivedPaying, paymentMsg.FileHash, fileName, fileOwner, paymentMsg.Sender, uint64(paymentMsg.PaymentId), uint64(paymentMsg.Amount))
}

func (this *Dsp) waitForTxConfirmed(blockHeight uint64) error {
	currentBlockHeight, err := this.Chain.GetCurrentBlockHeight()
	log.Debugf("wait for tx confirmed height: %d, now: %d", blockHeight, currentBlockHeight)
	if err != nil {
		log.Errorf("get block height err %s", err)
		return err
	}
	if blockHeight <= uint64(currentBlockHeight) {
		return nil
	}

	waitSuccess, err := this.Chain.WaitForGenerateBlock(time.Duration(common.WAIT_FOR_GENERATEBLOCK_TIMEOUT)*time.Second, uint32(blockHeight-uint64(currentBlockHeight)))
	if err != nil || !waitSuccess {
		log.Errorf("get block height err %s %d %d", err, currentBlockHeight, blockHeight)
		return fmt.Errorf("get block height err %d %d", currentBlockHeight, blockHeight)
	}
	return nil
}
