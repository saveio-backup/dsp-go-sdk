package dsp

import (
	"context"
	"strings"

	"github.com/oniio/dsp-go-sdk/common"
	netcom "github.com/oniio/dsp-go-sdk/network/common"
	"github.com/oniio/dsp-go-sdk/network/message"
	"github.com/oniio/dsp-go-sdk/network/message/types/block"
	"github.com/oniio/dsp-go-sdk/network/message/types/file"
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
	case netcom.FILE_OP_DOWNLOAD:
		if !this.taskMgr.IsDownloadInfoExist(fileMsg.Hash) {
			replyMsg := message.NewFileDownloadAckErr(fileMsg.Hash, netcom.MSG_ERROR_CODE_FILE_NOT_EXIST)
			err := ctx.Reply(context.Background(), replyMsg.ToProtoMsg())
			if err != nil {
				log.Errorf("reply download ack err msg failed", err)
			}
			return
		}
		if this.taskMgr.TaskNum() >= common.MAX_TASKS_NUM {
			replyMsg := message.NewFileDownloadAckErr(fileMsg.Hash, netcom.MSG_ERROR_CODE_FILE_NOT_EXIST)
			err := ctx.Reply(context.Background(), replyMsg.ToProtoMsg())
			if err != nil {
				log.Errorf("reply download ack err msg failed", err)
			}
		}
		// TODO: verify price
		replyMsg := message.NewFileDownloadAck(fileMsg.Hash, this.taskMgr.FileBlockHashes(fileMsg.Hash), this.Chain.Native.Fs.DefAcc.Address.ToBase58())
		err := ctx.Reply(context.Background(), replyMsg.ToProtoMsg())
		if err != nil {
			log.Errorf("reply download ack  msg failed", err)
		}
		this.taskMgr.NewTask(fileMsg.Hash, task.TaskTypeDownload)
		// TODO: delete task finally
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
		case task.TaskTypeDownload:
			reqCh := this.taskMgr.BlockReqCh()
			reqCh <- &task.GetBlockReq{
				FileHash: blockMsg.FileHash,
				Hash:     blockMsg.Hash,
				Index:    blockMsg.Index,
				PeerAddr: peer.Address,
			}
		}
	default:
	}
}
