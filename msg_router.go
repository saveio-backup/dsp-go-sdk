package dsp

import (
	"context"
	"strings"
	"time"

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
		log.Debugf("receive nil msg from %s\n", peer.Address)
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
	log.Debugf("handleFileMsg %s from peer:%s, length:%d\n", map[int32]string{
		netcom.FILE_OP_FETCH_ASK: "fetch_ask",
		netcom.FILE_OP_FETCH_ACK: "fetch_ack",
		netcom.FILE_OP_FETCH_RDY: "fetch_rdy",
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
		if !this.taskMgr.IsDownloadInfoExist(fileMsg.Hash) {
			err := this.taskMgr.AddFileBlockHashes(fileMsg.Hash, fileMsg.BlockHashes)
			if err != nil {
				log.Errorf("add fileblockhashes failed:%s", err)
				return
			}
			err = this.taskMgr.AddFilePrefix(fileMsg.Hash, fileMsg.Prefix)
			if err != nil {
				log.Errorf("add file prefix failed:%s", err)
				return
			}
		}
		newMsg := message.NewFileFetchAck(fileMsg.GetHash())
		this.Network.Send(newMsg, peer)
	case netcom.FILE_OP_FETCH_ACK:
		timeout, err := this.taskMgr.TaskTimeout(fileMsg.Hash)
		if err != nil {
			log.Errorf("get task timeout err:%s", err)
			return
		}
		if timeout {
			log.Debugf("task timeout for hash:%s", fileMsg.Hash)
			return
		}
		this.taskMgr.OnTaskAck(fileMsg.Hash)
	case netcom.FILE_OP_FETCH_RDY:
		if !this.taskMgr.IsDownloadInfoExist(fileMsg.Hash) {
			return
		}
		if this.taskMgr.TaskExist(fileMsg.Hash) {
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
		if !this.taskMgr.IsDownloadInfoExist(fileMsg.Hash) {
			replyMsg := message.NewFileDownloadAck(fileMsg.Hash, nil, "", "", 0, netcom.MSG_ERROR_CODE_FILE_NOT_EXIST)
			err := ctx.Reply(context.Background(), replyMsg.ToProtoMsg())
			if err != nil {
				log.Errorf("reply download ack err msg failed", err)
			}
			return
		}
		if this.taskMgr.TaskNum() >= common.MAX_TASKS_NUM {
			replyMsg := message.NewFileDownloadAck(fileMsg.Hash, nil, "", "", 0, netcom.MSG_ERROR_CODE_TOO_MANY_TASKS)
			err := ctx.Reply(context.Background(), replyMsg.ToProtoMsg())
			if err != nil {
				log.Errorf("reply download ack err msg failed", err)
			}
			return
		}
		price, err := this.GetFileUnitPrice(fileMsg.Hash, fileMsg.PayInfo.Asset)
		if err != nil {
			replyMsg := message.NewFileDownloadAck(fileMsg.Hash, nil, "", "", 0, netcom.MSG_ERROR_CODE_FILE_UNITPRICE_ERROR)
			err = ctx.Reply(context.Background(), replyMsg.ToProtoMsg())
			if err != nil {
				log.Errorf("reply download ack err msg failed", err)
			}
			return
		}
		replyMsg := message.NewFileDownloadAck(fileMsg.Hash, this.taskMgr.FileBlockHashes(fileMsg.Hash),
			this.Chain.Native.Fs.DefAcc.Address.ToBase58(), this.taskMgr.FilePrefix(fileMsg.Hash),
			price, netcom.MSG_ERROR_CODE_NONE)
		err = ctx.Reply(context.Background(), replyMsg.ToProtoMsg())
		if err != nil {
			log.Errorf("reply download ack  msg failed", err)
		}
	case netcom.FILE_OP_DOWNLOAD:
		// check deposit price
		err := this.Channel.WaitForConnected(fileMsg.PayInfo.WalletAddress, time.Duration(common.WAIT_CHANNEL_CONNECT_TIMEOUT)*time.Second)
		if err != nil {
			log.Errorf("wait channel connected err %s", err)
			return
		}
		if this.Config.CheckDepositBlkNum > 0 {
			price, err := this.GetFileUnitPrice(fileMsg.Hash, fileMsg.PayInfo.Asset)
			if err != nil {
				log.Errorf("get file unit price err %s", err)
				return
			}
			if price > 0 {
				balance, err := this.Channel.GetTargetBalance(fileMsg.PayInfo.WalletAddress)
				if err != nil {
					log.Errorf("get target balance err %s", err)
					return
				}
				// calulate
				if price*this.Config.CheckDepositBlkNum > balance {
					log.Errorf("insufficient deposit balance")
					return
				}
			}
		}
		this.taskMgr.NewTask(fileMsg.Hash, task.TaskTypeShare)
		log.Debugf("new share task %s", fileMsg.Hash)
		if !this.taskMgr.IsShareInfoExists(fileMsg.Hash) {
			err = this.taskMgr.NewFileShareInfo(fileMsg.Hash)
			if err != nil {
				return
			}
		}
		this.taskMgr.AddShareTo(fileMsg.Hash, fileMsg.PayInfo.WalletAddress)
		// TODO: delete tasks finally
	default:
	}
}

// handleBlockMsg handle all file msg
func (this *Dsp) handleBlockMsg(ctx *network.ComponentContext, peer *network.PeerClient, msg *message.Message) {
	blockMsg := msg.Payload.(*block.Block)
	log.Debugf("handleBlockMsg %s from peer:%s, length:%d\n",
		map[int32]string{
			netcom.BLOCK_OP_NONE: "block_none",
			netcom.BLOCK_OP_GET:  "block_get",
		}[blockMsg.Operation], peer.Address, msg.Header.MsgLength)
	exist := this.taskMgr.TaskExist(blockMsg.FileHash)
	if !exist {
		log.Debugf("task %s not exist", blockMsg.FileHash)
		return
	}
	switch blockMsg.Operation {
	case netcom.BLOCK_OP_NONE:
		respCh, err := this.taskMgr.TaskBlockResp(blockMsg.FileHash)
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
		taskType := this.taskMgr.TaskType(blockMsg.FileHash)
		switch taskType {
		case task.TaskTypeUpload:
			reqCh, err := this.taskMgr.TaskBlockReq(blockMsg.FileHash)
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
	err = this.taskMgr.DeleteShareFileUnpaid(paymentMsg.FileHash, paymentMsg.Sender, paymentMsg.Asset, paymentMsg.Amount)
	if err != nil {
		log.Debugf("delete share file info %s", err)
		return
	}
	err = ctx.Reply(context.Background(), nil)
	if err != nil {
		log.Errorf("reply delete ok msg failed", err)
	}
}
