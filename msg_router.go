package dsp

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/saveio/carrier/network"
	"github.com/saveio/dsp-go-sdk/common"
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
	log.Debugf("receive msg.Header.Type %d", msg.Header.Type)
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
		//TODO: verify & save info
		err := this.waitForTxConfirmed(fileMsg.Tx.Height)
		if err != nil {
			log.Errorf("get block height err %s", err)
			return
		}
		info, _ := this.Chain.Native.Fs.GetFileInfo(fileMsg.Hash)
		if info == nil {
			log.Errorf("fetch ask file info is nil %s %s", fileMsg.Hash, fileMsg.PayInfo.WalletAddress)
			return
		}
		log.Debugf("get file info %s success", fileMsg.Hash)
		if fileMsg.PayInfo.WalletAddress != "AKg57FvzHNC6Q4F6Mge6aio6uhfo9cLaQ8" {
			log.Errorf("Test receive fetch ask from %s", fileMsg.PayInfo.WalletAddress)
			return
		}
		if info.FileOwner.ToBase58() != fileMsg.PayInfo.WalletAddress {
			log.Errorf("receive fetch ask msg from wrong account %s", fileMsg.PayInfo.WalletAddress)
			return
		}
		if uint64(len(fileMsg.BlockHashes)) != info.FileBlockNum {
			log.Errorf("block number is unmatched %d, %d", len(fileMsg.BlockHashes), info.FileBlockNum)
			return
		}
		// task exist at runtime
		localId := this.taskMgr.TaskId(fileMsg.Hash, this.WalletAddress(), task.TaskTypeDownload)
		log.Debugf("fetch_ask try find localId %s", localId)
		if len(localId) > 0 && this.taskMgr.TaskExist(localId) {
			log.Errorf("fetch task is exist")
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
		this.taskMgr.SetSessionId(taskId, fileMsg.PayInfo.WalletAddress, fileMsg.SessionId)
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
		newMsg := message.NewFileFetchAck(fileMsg.SessionId, fileMsg.GetHash())
		log.Debugf("send file_ack msg %v %v", peer, newMsg)
		err = ctx.Reply(context.Background(), newMsg.ToProtoMsg())
		if err != nil {
			log.Errorf("reply file_ack msg failed", err)
		}
		log.Debugf("reply file_ack msg success")
	case netcom.FILE_OP_FETCH_RDY:
		// my task. use my wallet address
		taskKey := this.taskMgr.TaskId(fileMsg.Hash, this.WalletAddress(), task.TaskTypeDownload)
		if !this.taskMgr.IsFileInfoExist(taskKey) {
			return
		}
		if fileMsg.PayInfo == nil {
			log.Warnf("fetch_rdy msg no contains wallet address")
			return
		}
		log.Debugf("start fetching blocks")
		err := this.startFetchBlocks(fileMsg.Hash, peer.Address, fileMsg.PayInfo.WalletAddress)
		if err != nil {
			log.Errorf("start fetch blocks for file %s failed, err:%s", fileMsg.Hash, err)
		}
	case netcom.FILE_OP_DELETE:
		err := this.waitForTxConfirmed(fileMsg.Tx.Height)
		if err != nil {
			log.Errorf("get block height err %s", err)
			return
		}
		info, err := this.Chain.Native.Fs.GetFileInfo(fileMsg.Hash)
		if info != nil || strings.Index(err.Error(), "FsGetFileInfo not found") == -1 {
			log.Errorf("delete file info is not nil %s %s", fileMsg.Hash, fileMsg.PayInfo.WalletAddress)
			return
		}
		replyMsg := message.NewFileDeleteAck(fileMsg.SessionId, fileMsg.Hash)
		err = ctx.Reply(context.Background(), replyMsg.ToProtoMsg())
		if err != nil {
			log.Errorf("reply delete ok msg failed", err)
		}
		log.Debugf("reply delete ack msg success")
		// TODO: check file owner
		err = this.DeleteDownloadedFile(fileMsg.Hash)
		if err != nil {
			log.Errorf("delete downloaded file failed", err)
		}
	case netcom.FILE_OP_DOWNLOAD_ASK:
		if !this.CheckFilePrivilege(fileMsg.Hash, fileMsg.PayInfo.WalletAddress) {
			log.Errorf("user %s has no privilege to download this file", fileMsg.PayInfo.WalletAddress)
			return
		}
		// localId := this.taskMgr.TaskId(fileMsg.Hash, fileMsg.PayInfo.WalletAddress, task.TaskTypeShare)
		// if this.taskMgr.TaskExist(localId) {
		// 	replyErr(fileMsg.Hash, netcom.MSG_ERROR_CODE_TASK_EXIST, ctx)
		// 	return
		// }
		// TODO: check channel balance and router path
		taskId, err := this.taskMgr.NewTask(task.TaskTypeShare)
		sessionId, err := this.taskMgr.GetSeesionId(taskId, "")
		// my task. use my wallet address
		replyErr := func(fileHash string, errorCode int32, ctx *network.ComponentContext) {
			replyMsg := message.NewFileDownloadAck(sessionId, fileMsg.Hash, nil, "", "", 0, 0, errorCode)
			err := ctx.Reply(context.Background(), replyMsg.ToProtoMsg())
			log.Debugf("reply download_ack errmsg code %d, err %s", errorCode, err)
			if err != nil {
				log.Errorf("reply download ack err msg failed", err)
			}
		}
		if err != nil {
			replyErr(fileMsg.Hash, netcom.MSG_ERROR_INTERNAL_ERROR, ctx)
			return
		}
		rootBlk := this.Fs.GetBlock(fileMsg.Hash)
		downloadInfoId := this.taskMgr.TaskId(fileMsg.Hash, this.WalletAddress(), task.TaskTypeDownload)
		if !this.taskMgr.IsFileInfoExist(downloadInfoId) || rootBlk == nil {
			log.Errorf("file ask err root block %t", rootBlk == nil)
			replyErr(fileMsg.Hash, netcom.MSG_ERROR_CODE_FILE_NOT_EXIST, ctx)
			return
		}
		if this.taskMgr.TaskNum() >= common.MAX_TASKS_NUM {
			replyErr(fileMsg.Hash, netcom.MSG_ERROR_CODE_TOO_MANY_TASKS, ctx)
			return
		}
		price, err := this.GetFileUnitPrice(fileMsg.PayInfo.Asset)
		if err != nil {
			replyErr(fileMsg.Hash, netcom.MSG_ERROR_CODE_FILE_UNITPRICE_ERROR, ctx)
			return
		}
		prefix, err := this.taskMgr.GetFilePrefix(downloadInfoId)
		if err != nil {
			replyErr(fileMsg.Hash, netcom.MSG_ERROR_INTERNAL_ERROR, ctx)
			return
		}
		log.Debugf("sessionId %s blockCount %v %s prefix %s", sessionId, len(this.taskMgr.FileBlockHashes(downloadInfoId)), downloadInfoId, prefix)
		replyMsg := message.NewFileDownloadAck(sessionId, fileMsg.Hash, this.taskMgr.FileBlockHashes(downloadInfoId),
			this.WalletAddress(), prefix,
			price, fileMsg.PayInfo.Asset, netcom.MSG_ERROR_CODE_NONE)
		err = ctx.Reply(context.Background(), replyMsg.ToProtoMsg())
		if err != nil {
			log.Errorf("reply download ack  msg failed", err)
		}
		log.Debugf("reply download ack success new share task %s, key %s", fileMsg.Hash, taskId)
		this.taskMgr.SetTaskInfos(taskId, fileMsg.Hash, "", "", fileMsg.PayInfo.WalletAddress)
		this.taskMgr.BindTaskId(taskId)
	case netcom.FILE_OP_DOWNLOAD:
		taskKey := this.taskMgr.TaskId(fileMsg.Hash, fileMsg.PayInfo.WalletAddress, task.TaskTypeShare)
		// TODO: check channel balance and router path
		this.taskMgr.AddShareTo(taskKey, fileMsg.PayInfo.WalletAddress)
		hostAddr, err := this.GetExternalIP(fileMsg.PayInfo.WalletAddress)
		log.Debugf("Set host addr after recv file download %s - %s", fileMsg.PayInfo.WalletAddress, hostAddr)
		if len(hostAddr) > 0 || err != nil {
			this.Channel.SetHostAddr(fileMsg.PayInfo.WalletAddress, hostAddr)
		}
		err = ctx.Reply(context.Background(), message.NewEmptyMsg().ToProtoMsg())
		if err != nil {
			log.Errorf("reply download msg failed, err %s", err)
		}
		log.Debugf("reply download msg success")
		this.taskMgr.EmitNotification(taskKey, task.ShareStateBegin, fileMsg.Hash, fileMsg.PayInfo.WalletAddress, 0, 0)
	case netcom.FILE_OP_DOWNLOAD_OK:
		taskId := this.taskMgr.TaskId(fileMsg.Hash, fileMsg.PayInfo.WalletAddress, task.TaskTypeShare)
		if !this.taskMgr.TaskExist(taskId) {
			log.Errorf("share task not exist %s", fileMsg.Hash)
			return
		}
		this.taskMgr.DeleteTask(taskId, true)
		log.Debugf("delete share task of %s", taskId)
		err := ctx.Reply(context.Background(), message.NewEmptyMsg().ToProtoMsg())
		if err != nil {
			log.Errorf("reply download msg failed, err %s", err)
		}
		log.Debugf("reply download ack msg success")
		this.taskMgr.EmitNotification(taskId, task.ShareStateEnd, fileMsg.Hash, fileMsg.PayInfo.WalletAddress, 0, 0)
	default:
	}
}

// handleBlockMsg handle all file msg
func (this *Dsp) handleBlockMsg(ctx *network.ComponentContext, peer *network.PeerClient, msg *message.Message) {
	blockMsg := msg.Payload.(*block.Block)
	log.Debugf("handleBlockMsg %d %s-%s-%d from peer:%s, length:%d", blockMsg.Operation, blockMsg.FileHash, blockMsg.Hash, blockMsg.Index, peer.Address, msg.Header.MsgLength)

	switch blockMsg.Operation {
	case netcom.BLOCK_OP_NONE:
		taskId := this.taskMgr.TaskId(blockMsg.FileHash, this.WalletAddress(), task.TaskTypeDownload)
		exist := this.taskMgr.TaskExist(taskId)
		if !exist {
			log.Debugf("task %s not exist", blockMsg.FileHash)
			return
		}
		this.taskMgr.PushGetBlock(taskId, &task.BlockResp{
			Hash:     blockMsg.Hash,
			Index:    blockMsg.Index,
			PeerAddr: peer.Address,
			Block:    blockMsg.Data,
			Tag:      blockMsg.Tag,
			Offset:   blockMsg.Offset,
		})
	case netcom.BLOCK_OP_GET:
		sessionId := blockMsg.SessionId
		log.Debugf("session: %s handle get block %s-%s-%d", sessionId, blockMsg.FileHash, blockMsg.Hash, blockMsg.Index)
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
			reqCh, err := this.taskMgr.TaskBlockReq(sessionId)
			if err != nil {
				log.Errorf("get task block reqCh err: %s", err)
				return
			}
			reqCh <- &task.GetBlockReq{
				FileHash: blockMsg.FileHash,
				Hash:     blockMsg.Hash,
				Index:    blockMsg.Index,
				PeerAddr: peer.Address,
			}
			return
		case task.TaskTypeDownload, task.TaskTypeShare:
			reqCh := this.taskMgr.BlockReqCh()
			reqCh <- &task.GetBlockReq{
				FileHash:      blockMsg.FileHash,
				Hash:          blockMsg.Hash,
				Index:         blockMsg.Index,
				PeerAddr:      peer.Address,
				WalletAddress: blockMsg.Payment.Sender,
				Asset:         blockMsg.Payment.Asset,
			}
		default:
			log.Debugf("handle block get msg, tasktype not found")
		}
	default:
	}
}

// handlePaymentMsg. handle payment msg
func (this *Dsp) handlePaymentMsg(ctx *network.ComponentContext, peer *network.PeerClient, msg *message.Message) {
	paymentMsg := msg.Payload.(*payment.Payment)
	log.Debugf("received paymentMsg sender:%v, asset:%d, amount:%d", paymentMsg.Sender, paymentMsg.Asset, paymentMsg.Amount)
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
	err = this.taskMgr.DeleteFileUnpaid(taskKey, paymentMsg.Sender, paymentMsg.Asset, paymentMsg.Amount)
	log.Debugf("delete unpaid success %v", paymentMsg)
	if err != nil {
		log.Debugf("delete share file info %s", err)
		return
	}
	err = ctx.Reply(context.Background(), message.NewEmptyMsg().ToProtoMsg())
	if err != nil {
		log.Errorf("reply delete ok msg failed", err)
	}
	log.Debugf("reply handle payment msg")
	this.taskMgr.EmitNotification(taskKey, task.ShareStateReceivedPaying, paymentMsg.FileHash, paymentMsg.Sender, uint64(paymentMsg.PaymentId), uint64(paymentMsg.Amount))
}

func (this *Dsp) waitForTxConfirmed(blockHeight uint64) error {
	currentBlockHeight, err := this.Chain.GetCurrentBlockHeight()
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
