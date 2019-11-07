package dsp

import (
	"bytes"
	"github.com/saveio/dsp-go-sdk/actor/client"
	"github.com/saveio/dsp-go-sdk/common"
	"github.com/saveio/dsp-go-sdk/config"
	netcomm "github.com/saveio/dsp-go-sdk/network/common"
	"github.com/saveio/dsp-go-sdk/network/message"
	"github.com/saveio/dsp-go-sdk/network/message/types/block"
	"github.com/saveio/dsp-go-sdk/store"
	"github.com/saveio/dsp-go-sdk/task"
	chActor "github.com/saveio/pylons/actor/server"
	chainCom "github.com/saveio/themis/common"
	"github.com/saveio/themis/common/log"
	cUtils "github.com/saveio/themis/smartcontract/service/native/utils"
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

// registerReceiveNotification. register receive payment notification
func (this *Dsp) registerReceiveNotification() {
	log.Debugf("registerReceiveNotification")
	receiveChan, err := chActor.RegisterReceiveNotification()
	log.Debugf("receiveChan:%v, err %v", receiveChan, err)
	if err != nil {
		// panic(err)
	}
	go func() {
		for {
			select {
			case event := <-receiveChan:
				addr, err := chainCom.AddressParseFromBytes(event.Initiator[:])
				if err != nil {
					log.Errorf("receive payment with unrecognized address %v", event)
					continue
				}
				log.Debugf("PaymentReceive amount %d from %s with paymentID %d\n",
					event.Amount, addr.ToBase58(), event.Identifier)
				taskId, err := this.taskMgr.GetTaskIdWithPaymentId(int32(event.Identifier))
				if err != nil {
					log.Errorf("get taskId with payment id failed %s", err)
					continue
				}
				fileHashStr, err := this.taskMgr.TaskFileHash(taskId)
				if err != nil {
					log.Errorf("get fileHash with task id failed %s", err)
					continue
				}
				asset := common.ASSET_NONE
				if bytes.Compare(event.TokenNetworkId[:], cUtils.UsdtContractAddress[:]) == 0 {
					asset = common.ASSET_USDT
				}
				// delete record
				err = this.taskMgr.DeleteFileUnpaid(taskId, addr.ToBase58(), int32(event.Identifier), int32(asset), uint64(event.Amount))
				if err != nil {
					log.Errorf("delete share file info %s", err)
					continue
				}
				log.Debugf("delete unpaid success %v", taskId)
				downloadTaskId := this.taskMgr.TaskId(fileHashStr, this.WalletAddress(), store.TaskTypeDownload)
				fileName, _ := this.taskMgr.GetFileName(downloadTaskId)
				fileOwner, _ := this.taskMgr.GetFileOwner(downloadTaskId)
				this.taskMgr.EmitNotification(taskId, task.ShareStateReceivedPaying, fileHashStr, fileName, fileOwner, addr.ToBase58(), uint64(event.Identifier), uint64(event.Amount))
			case <-this.Channel.GetCloseCh():
				return
			}
		}
	}()
}

func (this *Dsp) canShareTo(taskId, walletAddress string, asset int32) bool {
	unpaidAmount, err := this.taskMgr.GetUnpaidAmount(taskId, walletAddress, asset)
	maxUnpaidAmount := uint64(common.CHUNK_SIZE * common.MAX_REQ_BLOCK_COUNT * this.Config.MaxUnpaidPayment)
	if err != nil || unpaidAmount >= maxUnpaidAmount {
		log.Errorf("cant share to %s for file %s, unpaidAmount: %d err %s", walletAddress, taskId, unpaidAmount, err)
		return false
	}
	return true
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

	paymentId := this.Channel.NewPaymentId()
	for _, blockmsg := range req {
		taskId = this.taskMgr.TaskId(blockmsg.FileHash, blockmsg.WalletAddress, store.TaskTypeShare)
		reqWalletAddr = blockmsg.WalletAddress
		reqAsset = blockmsg.Asset
		// check if has unpaid block request
		canShareTo := this.canShareTo(taskId, blockmsg.WalletAddress, blockmsg.Asset)
		if !canShareTo {
			log.Errorf("cant share to %s for file %s", blockmsg.WalletAddress, blockmsg.FileHash)
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
		err = this.taskMgr.AddFileUnpaid(taskId, blockmsg.WalletAddress, paymentId, blockmsg.Asset, uint64(len(blockData))*up)
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
		PaymentId: paymentId,
		TimeStamp: req[0].TimeStamp,
		Blocks:    blocks,
	}
	log.Debugf("share block task: %s, req from %s-%s-%d to %s-%s-%d of peer wallet: %s, peer addr: %s", taskId, req[0].FileHash, req[0].Hash, req[0].Index, req[0].WalletAddress, req[0].PeerAddr,
		req[len(req)-1].FileHash, req[len(req)-1].Hash, req[len(req)-1].Index, req[len(req)-1].WalletAddress, req[len(req)-1].PeerAddr)
	msg := message.NewBlockFlightsMsg(flights)
	_, err := client.P2pRequestWithRetry(msg.ToProtoMsg(), req[0].PeerAddr, common.MAX_SEND_BLOCK_RETRY, common.P2P_REQUEST_WAIT_REPLY_TIMEOUT)
	if err != nil {
		log.Errorf("share send block, err: %s", err)
		// TODO: delete unpaid msg if need
		if !common.ConntextTimeoutErr(err) {
			this.taskMgr.DeleteFileUnpaid(taskId, reqWalletAddr, paymentId, reqAsset, totalAmount)
		}
	}
}
