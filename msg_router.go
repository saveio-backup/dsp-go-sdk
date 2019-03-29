package dsp

import (
	"context"
	"strings"

	"github.com/oniio/dsp-go-sdk/common"
	netcom "github.com/oniio/dsp-go-sdk/network/common"
	"github.com/oniio/dsp-go-sdk/network/message"
	"github.com/oniio/dsp-go-sdk/network/message/types/block"
	"github.com/oniio/dsp-go-sdk/network/message/types/file"
	"github.com/oniio/dsp-go-sdk/network/message/types/payment"
	"github.com/oniio/dsp-go-sdk/task"
	"github.com/oniio/oniChain/common/log"
	"github.com/oniio/oniP2p/network"
)

// Receive p2p message receive router
func (this *Dsp) Receive(ctx *network.ComponentContext) {
	msg := message.ReadMessage(ctx.Message())
	peer := ctx.Client()
	if msg == nil || msg.Header == nil {
		log.Debugf("receive nil msg from %s", peer.Address)
		return
	}
	if msg.Header.Version != netcom.MESSAGE_VERSION {
		log.Debugf("unrecongized msg version %s", msg.Header.Version)
		return
	}
	switch msg.Header.Type {
	case netcom.MSG_TYPE_FILE:
		this.handleFileMsg(ctx, peer, msg)
	case netcom.MSG_TYPE_BLOCK:
		this.handleBlockMsg(ctx, peer, msg)
	case netcom.MSG_TYPE_PAYMENT:
		this.handlePaymentMsg(ctx, peer, msg)
	default:
		log.Debugf("unrecongized msg type %s", msg.Header.Type)
	}
}

// handleFileMsg handle all file msg
func (this *Dsp) handleFileMsg(ctx *network.ComponentContext, peer *network.PeerClient, msg *message.Message) {
	fileMsg := msg.Payload.(*file.File)
	log.Debugf("handleFileMsg %s from peer:%s, length:%d", map[int32]string{
		netcom.FILE_OP_FETCH_ASK:    "fetch_ask",
		netcom.FILE_OP_FETCH_ACK:    "fetch_ack",
		netcom.FILE_OP_FETCH_RDY:    "fetch_rdy",
		netcom.FILE_OP_DOWNLOAD_ASK: "download_ask",
		netcom.FILE_OP_DOWNLOAD_ACK: "download_ack",
		netcom.FILE_OP_DOWNLOAD:     "download",
	}[fileMsg.Operation], peer.Address, msg.Header.MsgLength)
	switch fileMsg.Operation {
	case netcom.FILE_OP_FETCH_ASK:
		//TODO: verify & save info
		info, _ := this.Chain.Native.Fs.GetFileInfo(fileMsg.Hash)
		if info == nil {
			log.Errorf("fetch ask file info is nil %s %s", fileMsg.Hash, fileMsg.PayInfo.WalletAddress)
			return
		}
		if info.FileOwner.ToBase58() != fileMsg.PayInfo.WalletAddress {
			log.Errorf("receive fetch ask msg from wrong acc %s", fileMsg.PayInfo.WalletAddress)
			return
		}
		if uint64(len(fileMsg.BlockHashes)) != info.FileBlockNum {
			log.Errorf("block number is unmatched %d, %d", len(fileMsg.BlockHashes), info.FileBlockNum)
			return
		}
		// my task. use my wallet address
		taskKey := this.taskMgr.TaskKey(fileMsg.Hash, this.WalletAddress(), task.TaskTypeDownload)
		if !this.taskMgr.IsDownloadInfoExist(taskKey) {
			err := this.taskMgr.AddFileBlockHashes(taskKey, fileMsg.BlockHashes)
			if err != nil {
				log.Errorf("add fileblockhashes failed:%s", err)
				return
			}
			err = this.taskMgr.AddFilePrefix(taskKey, fileMsg.Prefix)
			if err != nil {
				log.Errorf("add file prefix failed:%s", err)
				return
			}
		}
		newMsg := message.NewFileFetchAck(fileMsg.GetHash())
		this.Network.Send(newMsg, peer)
	case netcom.FILE_OP_FETCH_ACK:
		// my task. use my wallet address
		taskKey := this.taskMgr.TaskKey(fileMsg.Hash, this.WalletAddress(), task.TaskTypeUpload)
		timeout, err := this.taskMgr.TaskTimeout(taskKey)
		if err != nil {
			log.Errorf("get task timeout err:%s", err)
			return
		}
		if timeout {
			log.Debugf("task timeout for hash:%s", fileMsg.Hash)
			return
		}
		this.taskMgr.OnTaskAck(taskKey)
	case netcom.FILE_OP_FETCH_RDY:
		// my task. use my wallet address
		taskKey := this.taskMgr.TaskKey(fileMsg.Hash, this.WalletAddress(), task.TaskTypeDownload)
		if !this.taskMgr.IsDownloadInfoExist(taskKey) {
			return
		}
		if this.taskMgr.TaskExist(taskKey) {
			log.Debugf("task is exist")
			return
		}
		err := this.startFetchBlocks(fileMsg.Hash, peer.Address)
		if err != nil {
			log.Errorf("start fetch blocks for file %s failed, err:%s", fileMsg.Hash, err)
		}
	case netcom.FILE_OP_DELETE:
		info, err := this.Chain.Native.Fs.GetFileInfo(fileMsg.Hash)
		if info != nil || strings.Index(err.Error(), "FsGetFileInfo not found") == -1 {
			log.Errorf("delete file info is not nil %s %s", fileMsg.Hash, fileMsg.PayInfo.WalletAddress)
			return
		}
		replyMsg := message.NewFileDeleteAck(fileMsg.Hash)
		err = ctx.Reply(context.Background(), replyMsg.ToProtoMsg())
		if err != nil {
			log.Errorf("reply delete ok msg failed", err)
		}
		// TODO: check file owner
		this.deleteFile(fileMsg.Hash)
	case netcom.FILE_OP_DOWNLOAD_ASK:
		// my task. use my wallet address
		taskKey := this.taskMgr.TaskKey(fileMsg.Hash, this.WalletAddress(), task.TaskTypeDownload)
		rootBlk := this.Fs.GetBlock(fileMsg.Hash)
		if !this.taskMgr.IsDownloadInfoExist(taskKey) || rootBlk == nil {
			log.Errorf("file ask err root block %t", rootBlk == nil)
			replyMsg := message.NewFileDownloadAck(fileMsg.Hash, nil, "", "", 0, netcom.MSG_ERROR_CODE_FILE_NOT_EXIST)
			err := ctx.Reply(context.Background(), replyMsg.ToProtoMsg())
			if err != nil {
				log.Errorf("reply download ack err msg failed", err)
			}
			return
		}
		if this.taskMgr.TaskNum() >= common.MAX_TASKS_NUM {
			log.Errorf("task not mutch")
			replyMsg := message.NewFileDownloadAck(fileMsg.Hash, nil, "", "", 0, netcom.MSG_ERROR_CODE_TOO_MANY_TASKS)
			err := ctx.Reply(context.Background(), replyMsg.ToProtoMsg())
			if err != nil {
				log.Errorf("reply download ack err msg failed", err)
			}
			return
		}
		price, err := this.GetFileUnitPrice(fileMsg.PayInfo.Asset)
		if err != nil {
			replyMsg := message.NewFileDownloadAck(fileMsg.Hash, nil, "", "", 0, netcom.MSG_ERROR_CODE_FILE_UNITPRICE_ERROR)
			err = ctx.Reply(context.Background(), replyMsg.ToProtoMsg())
			if err != nil {
				log.Errorf("reply download ack err msg failed", err)
			}
			return
		}
		replyMsg := message.NewFileDownloadAck(fileMsg.Hash, this.taskMgr.FileBlockHashes(taskKey),
			this.Chain.Native.Fs.DefAcc.Address.ToBase58(), this.taskMgr.FilePrefix(taskKey),
			price, netcom.MSG_ERROR_CODE_NONE)
		err = ctx.Reply(context.Background(), replyMsg.ToProtoMsg())
		if err != nil {
			log.Errorf("reply download ack  msg failed", err)
		}
	case netcom.FILE_OP_DOWNLOAD:
		if !this.CheckFilePrivilege(fileMsg.Hash, fileMsg.PayInfo.WalletAddress) {
			log.Errorf("user %s has no privilege to download this file", fileMsg.PayInfo.WalletAddress)
			return
		}
		taskKey := this.taskMgr.TaskKey(fileMsg.Hash, fileMsg.PayInfo.WalletAddress, task.TaskTypeShare)
		if this.taskMgr.TaskExist(taskKey) {
			log.Errorf("share task exist %s", fileMsg.Hash)
			return
		}
		// TODO: check channel balance and router path
		log.Debugf("new share task %s", fileMsg.Hash)
		this.taskMgr.NewTask(fileMsg.Hash, fileMsg.PayInfo.WalletAddress, task.TaskTypeShare)
		if !this.taskMgr.IsShareInfoExists(taskKey) {
			err := this.taskMgr.NewFileShareInfo(taskKey)
			if err != nil {
				return
			}
		}
		this.taskMgr.AddShareTo(taskKey, fileMsg.PayInfo.WalletAddress)
		hostAddr := this.GetExternalIP(fileMsg.PayInfo.WalletAddress)
		log.Debugf("Set host addr after recv file download %s - %s", fileMsg.PayInfo.WalletAddress, hostAddr)
		if len(hostAddr) > 0 {
			this.Channel.SetHostAddr(fileMsg.PayInfo.WalletAddress, hostAddr)
		}
		err := ctx.Reply(context.Background(), message.NewEmptyMsg().ToProtoMsg())
		if err != nil {
			log.Errorf("reply download msg failed, err %s", err)
		}
		// TODO: delete tasks finally
	default:
	}
}

// handleBlockMsg handle all file msg
func (this *Dsp) handleBlockMsg(ctx *network.ComponentContext, peer *network.PeerClient, msg *message.Message) {
	blockMsg := msg.Payload.(*block.Block)
	log.Debugf("handleBlockMsg %s from peer:%s, length:%d",
		map[int32]string{
			netcom.BLOCK_OP_NONE: "block_none",
			netcom.BLOCK_OP_GET:  "block_get",
		}[blockMsg.Operation], peer.Address, msg.Header.MsgLength)

	switch blockMsg.Operation {
	case netcom.BLOCK_OP_NONE:
		taskKey := this.taskMgr.TaskKey(blockMsg.FileHash, this.WalletAddress(), task.TaskTypeDownload)
		exist := this.taskMgr.TaskExist(taskKey)
		if !exist {
			log.Debugf("task %s not exist", blockMsg.FileHash)
			return
		}
		respCh, err := this.taskMgr.TaskBlockResp(taskKey)
		if err != nil {
			log.Errorf("get task block respch err: %s", err)
			return
		}
		respCh <- &task.BlockResp{
			Hash:     blockMsg.Hash,
			Index:    blockMsg.Index,
			PeerAddr: peer.Address,
			Block:    blockMsg.Data,
			Tag:      blockMsg.Tag,
			Offset:   blockMsg.Offset,
		}
	case netcom.BLOCK_OP_GET:
		log.Debugf("handle get block %s-%s-%d", blockMsg.FileHash, blockMsg.Hash, blockMsg.Index)
		taskKey, err := this.taskMgr.TryGetTaskKey(blockMsg.FileHash, this.WalletAddress(), blockMsg.Payment.Sender)
		if err != nil {
			log.Debugf("try get task key failed %s", err)
			return
		}
		exist := this.taskMgr.TaskExist(taskKey)
		if !exist {
			log.Debugf("task %s not exist", blockMsg.FileHash)
			return
		}
		taskType := this.taskMgr.TaskType(taskKey)
		log.Debugf("task key:%s type %d", taskKey, taskType)
		switch taskType {
		case task.TaskTypeUpload:
			reqCh, err := this.taskMgr.TaskBlockReq(taskKey)
			if err != nil {
				log.Errorf("get task block reqch err: %s", err)
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
	log.Debugf("received paymentmsg:%v", paymentMsg)
	// check
	pay, err := this.Channel.GetPayment(paymentMsg.PaymentId)
	if err != nil || pay == nil {
		log.Errorf("get payment from db err %s, pay %v", err, pay)
		return
	}
	if pay.WalletAddress != paymentMsg.Sender || pay.Asset != paymentMsg.Asset || pay.Amount != paymentMsg.Amount {
		log.Errorf("payment %v is different from payment msg %v", pay, paymentMsg)
		return
	}
	err = this.Channel.DeletePayment(paymentMsg.PaymentId)
	if err != nil {
		log.Errorf("delete payment from db err %s", err)
		return
	}
	// delete record
	taskKey := this.taskMgr.TaskKey(paymentMsg.FileHash, paymentMsg.Sender, task.TaskTypeShare)
	err = this.taskMgr.DeleteShareFileUnpaid(taskKey, paymentMsg.Sender, paymentMsg.Asset, paymentMsg.Amount)
	log.Debugf("delete unpaid success %v", paymentMsg)
	if err != nil {
		log.Debugf("delete share file info %s", err)
		return
	}
	err = ctx.Reply(context.Background(), message.NewEmptyMsg().ToProtoMsg())
	if err != nil {
		log.Errorf("reply delete ok msg failed", err)
	}
}
