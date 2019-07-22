package dsp

import (
	"github.com/saveio/dsp-go-sdk/actor/client"
	"github.com/saveio/dsp-go-sdk/network/message"
	"github.com/saveio/dsp-go-sdk/task"
	"github.com/saveio/themis/common/log"
)

func (this *Dsp) StartShareServices() {
	ch := this.taskMgr.BlockReqCh()
	for {
		select {
		case req, ok := <-ch:
			if !ok {
				log.Errorf("block req channel false")
				break
			}
			taskId := this.taskMgr.TaskId(req.FileHash, req.WalletAddress, task.TaskTypeShare)
			log.Debugf("share task taskId %s", taskId)
			// check if has unpaid block request
			canShare, err := this.taskMgr.CanShareTo(taskId, req.WalletAddress, req.Asset)
			if err != nil || !canShare {
				log.Errorf("cant share to %s for file %s, can:%t err %s", req.WalletAddress, req.FileHash, canShare, err)
				break
			}
			// send block if requester has paid all block
			blk := this.Fs.GetBlock(req.Hash)
			blockData := this.Fs.BlockDataOfAny(blk)
			if len(blockData) == 0 {
				log.Errorf("get block data empty %s", req.Hash)
				break
			}
			downloadTaskKey := this.taskMgr.TaskId(req.FileHash, this.WalletAddress(), task.TaskTypeDownload)
			offset, err := this.taskMgr.GetBlockOffset(downloadTaskKey, req.Hash, uint32(req.Index))
			log.Debugf("share block get offset from download info %s,  hash %s, index %v, offset %v, err %s", downloadTaskKey, req.Hash, req.Index, offset, err)
			if err != nil {
				log.Errorf("get block offset, err: %s", err)
				break
			}
			// TODO: only send tag with tagflag enabled
			// TEST: client get tag
			tag, _ := this.Fs.GetTag(req.Hash, req.FileHash, uint64(req.Index))
			sessionId, _ := this.taskMgr.GetSeesionId(taskId, "")
			msg := message.NewBlockMsg(sessionId, req.Index, req.FileHash, req.Hash, blockData, tag, int64(offset))
			client.P2pSend(req.PeerAddr, msg.ToProtoMsg())
			up, err := this.GetFileUnitPrice(req.Asset)
			if err != nil {
				log.Errorf("get file unit price err after send block, err: %s", err)
				break
			}
			// add new unpaid block request to store
			this.taskMgr.AddFileUnpaid(taskId, req.WalletAddress, req.Asset, uint64(len(blockData))*up)
		}
	}
}

// RegShareNotificationChannel. register share channel
func (this *Dsp) RegShareNotificationChannel() {
	if this == nil {
		log.Errorf("this.taskMgr == nil")
	}
	this.taskMgr.RegShareNotification()
}

// ShareNotificationChannel.
func (this *Dsp) ShareNotificationChannel() chan *task.ShareNotification {
	return this.taskMgr.ShareNotification()
}

// CloseShareNotificationChannel.
func (this *Dsp) CloseShareNotificationChannel() {
	this.taskMgr.CloseShareNotification()
}
