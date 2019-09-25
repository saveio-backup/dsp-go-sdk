package dsp

import (
	"github.com/saveio/dsp-go-sdk/actor/client"
	"github.com/saveio/dsp-go-sdk/common"
	"github.com/saveio/dsp-go-sdk/config"
	netcomm "github.com/saveio/dsp-go-sdk/network/common"
	"github.com/saveio/dsp-go-sdk/network/message"
	"github.com/saveio/dsp-go-sdk/network/message/types/block"
	"github.com/saveio/dsp-go-sdk/store"
	"github.com/saveio/dsp-go-sdk/task"
	"github.com/saveio/themis/common/log"
)

func (this *Dsp) StartShareServices() {
	ch := this.taskMgr.BlockReqCh()
	for {
		select {
		case req, ok := <-ch:
			if !ok {
				log.Errorf("block flights req channel false")
				break
			}
			go this.shareBlock(req)
		}
	}
}

func (this *Dsp) shareBlock(req []*task.GetBlockReq) {
	if req == nil || len(req) == 0 {
		log.Debugf("share block request empty")
		return
	}
	blocks := make([]*block.Block, 0)
	totalAmount := uint64(0)
	taskId := ""
	reqWalletAddr := ""
	reqAsset := int32(0)
	log.Debugf("share block task: %s, req from %s-%s-%d to %s-%s-%d of peer wallet: %s, peer addr: %s", taskId, req[0].FileHash, req[0].Hash, req[0].Index, req[0].WalletAddress, req[0].PeerAddr,
		req[len(req)-1].FileHash, req[len(req)-1].Hash, req[len(req)-1].Index, req[len(req)-1].WalletAddress, req[len(req)-1].PeerAddr)
	for _, blockmsg := range req {
		taskId = this.taskMgr.TaskId(blockmsg.FileHash, blockmsg.WalletAddress, store.TaskTypeShare)
		reqWalletAddr = blockmsg.WalletAddress
		reqAsset = blockmsg.Asset
		// check if has unpaid block request
		canShare, err := this.taskMgr.CanShareTo(taskId, blockmsg.WalletAddress, blockmsg.Asset)
		if err != nil || !canShare {
			log.Errorf("cant share to %s for file %s, can:%t err %s", blockmsg.WalletAddress, blockmsg.FileHash, canShare, err)
			return
		}
		// send block if requester has paid all block
		blk := this.Fs.GetBlock(blockmsg.Hash)
		blockData := this.Fs.BlockDataOfAny(blk)
		if len(blockData) == 0 {
			log.Errorf("get block data empty %s", blockmsg.Hash)
			return
		}
		downloadTaskKey := this.taskMgr.TaskId(blockmsg.FileHash, this.WalletAddress(), store.TaskTypeDownload)
		offset, err := this.taskMgr.GetBlockOffset(downloadTaskKey, blockmsg.Hash, uint32(blockmsg.Index))
		if err != nil {
			log.Errorf("share block taskId: %s download info %s,  hash: %s-%s-%v, offset %v to: %s err %s", taskId, downloadTaskKey, blockmsg.FileHash, blockmsg.Hash, blockmsg.Index, offset, blockmsg.PeerAddr, err)
			return
		}
		// TODO: only send tag with tagflag enabled
		// TEST: client get tag
		var tag []byte
		if this.Config.FsType == config.FS_BLOCKSTORE {
			tag, _ = this.Fs.GetTag(blockmsg.Hash, blockmsg.FileHash, uint64(blockmsg.Index))
		}
		sessionId, _ := this.taskMgr.GetSessionId(taskId, "")
		up, err := this.GetFileUnitPrice(blockmsg.Asset)
		if err != nil {
			log.Errorf("get file unit price err after send block, err: %s", err)
			return
		}
		// add new unpaid block request to store
		err = this.taskMgr.AddFileUnpaid(taskId, blockmsg.WalletAddress, blockmsg.Asset, uint64(len(blockData))*up)
		if err != nil {
			log.Errorf("add file unpaid failed err : %s", err)
			return
		}
		totalAmount += uint64(len(blockData)) * up
		log.Debugf("add file unpaid success %d", totalAmount)
		b := &block.Block{
			SessionId: sessionId,
			Index:     blockmsg.Index,
			FileHash:  blockmsg.FileHash,
			Hash:      blockmsg.Hash,
			Data:      blockData,
			Tag:       tag,
			Operation: netcomm.BLOCK_OP_NONE,
			Offset:    int64(offset),
		}
		blocks = append(blocks, b)
	}
	if len(blocks) == 0 {
		log.Warn("no block shared")
		return
	}
	flights := &block.BlockFlights{
		TimeStamp: req[0].TimeStamp,
		Blocks:    blocks,
	}
	msg := message.NewBlockFlightsMsg(flights)
	_, err := client.P2pRequestWithRetry(msg.ToProtoMsg(), req[0].PeerAddr, common.MAX_SEND_BLOCK_RETRY, common.DOWNLOAD_BLOCKFLIGHTS_TIMEOUT)
	if err != nil {
		log.Errorf("share send block, err: %s", err)
		// TODO: delete unpaid msg if need
		if !common.ConntextTimeoutErr(err) {
			this.taskMgr.DeleteFileUnpaid(taskId, reqWalletAddr, reqAsset, totalAmount)
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
