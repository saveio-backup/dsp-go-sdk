package share

import (
	"math/rand"
	"time"

	"github.com/saveio/dsp-go-sdk/actor/client"
	"github.com/saveio/dsp-go-sdk/consts"
	sdkErr "github.com/saveio/dsp-go-sdk/error"
	netCom "github.com/saveio/dsp-go-sdk/network/common"
	"github.com/saveio/dsp-go-sdk/network/message"
	"github.com/saveio/dsp-go-sdk/network/message/types/block"
	"github.com/saveio/dsp-go-sdk/task/types"
	"github.com/saveio/themis/common/log"
)

func (this *ShareTask) ShareBlock(req []*types.GetBlockReq) {
	if req == nil || len(req) == 0 {
		log.Debugf("share block request empty")
		return
	}
	blocks := make([]*block.Block, 0)
	totalAmount := uint64(0)
	taskId := ""
	reqWalletAddr := ""
	reqAsset := int32(0)
	// TODO wangyu how handle channel is nil condition
	var paymentId int32
	if this.Mgr.Channel() != nil {
		paymentId = this.Mgr.Channel().NewPaymentId()
	} else {
		paymentId = rand.Int31()
	}
	var shareErr *sdkErr.Error

	for _, blockmsg := range req {
		taskId = this.Id
		log.Debugf("share block task id %v, file hash %v, peer wallet address %v, syn %s",
			taskId, blockmsg.FileHash, blockmsg.WalletAddress, blockmsg.Syn)
		reqWalletAddr = blockmsg.WalletAddress
		reqAsset = blockmsg.Asset
		// check if has unpaid block request
		canShareTo := this.CanShareTo(blockmsg.WalletAddress, blockmsg.Asset)
		if !canShareTo {
			shareErr = sdkErr.New(sdkErr.EXIST_UNPAID_BLOCKS,
				"cant share to %s for file %s", blockmsg.WalletAddress, blockmsg.FileHash)
			break
		}
		// send block if requester has paid all block
		blk := this.Mgr.Fs().GetBlock(blockmsg.Hash)
		blockData := this.Mgr.Fs().BlockDataOfAny(blk)
		if len(blockData) == 0 {
			shareErr = sdkErr.New(sdkErr.BLOCK_NOT_FOUND,
				"get block %s failed, its data is empty ", blockmsg.Hash)
			break
		}
		downloadTaskId := this.GetReferId()
		if len(downloadTaskId) == 0 {
			shareErr = sdkErr.New(sdkErr.REFER_ID_NOT_FOUND,
				"get download task id of task %s not found", taskId)
			break
		}
		offset, err := this.DB.GetBlockOffset(downloadTaskId, blockmsg.Hash, blockmsg.Index)
		if err != nil {
			shareErr = sdkErr.New(sdkErr.GET_TASK_PROPERTY_ERROR,
				"share block taskId: %s download info %s,  hash: %s-%s-%v, offset %v to: %s err %s",
				taskId, downloadTaskId, blockmsg.FileHash, blockmsg.Hash,
				blockmsg.Index, offset, blockmsg.PeerAddr, err)
			break
		}
		// TODO: only send tag with tagflag enabled
		// TEST: client get tag
		var tag []byte
		if this.Mgr.Config().FsType == consts.FS_BLOCKSTORE {
			tag, _ = this.Mgr.Fs().GetTag(blockmsg.Hash, blockmsg.FileHash, uint64(blockmsg.Index))
		}

		price := uint64(consts.DOWNLOAD_BLOCK_PRICE)
		// prefix := this.Info.Prefix

		// up, err := this.GetFileUnitPrice(blockmsg.Asset)
		// if err != nil {
		// 	shareErr = sdkErr.New(sdkErr.GET_TASK_PROPERTY_ERROR,
		// 		"get file unit price err after send block, err: %s", err)
		// 	break
		// }
		// add new unpaid block request to store
		err = this.AddFileUnpaid(blockmsg.WalletAddress, paymentId,
			blockmsg.Asset, uint64(len(blockData))*price)
		if err != nil {
			shareErr = sdkErr.New(sdkErr.ADD_FILE_UNPAID_ERROR,
				"add file unpaid failed err : %s", err)
			break
		}
		totalAmount += uint64(len(blockData)) * price
		log.Debugf("share block task id %v, file hash %v, peer wallet address %v, "+
			"downloadId %s, offset %d, total amount %d",
			taskId, blockmsg.FileHash, blockmsg.WalletAddress, downloadTaskId, offset, totalAmount)
		b := &block.Block{
			SessionId: taskId,
			Index:     blockmsg.Index,
			FileHash:  blockmsg.FileHash,
			Hash:      blockmsg.Hash,
			Data:      blockData,
			Tag:       tag,
			Operation: netCom.BLOCK_OP_NONE,
			Offset:    int64(offset),
		}
		blocks = append(blocks, b)
	}
	if shareErr != nil {
		log.Error(shareErr.Error())
		msg := message.NewBlockFlightsMsgWithError(nil, shareErr.Code, shareErr.Error(),
			message.WithSyn(req[0].Syn))
		err := client.P2PSend(req[0].PeerAddr, msg.MessageId, msg.ToProtoMsg())
		if err != nil {
			log.Errorf("send share block err msg failed, err: %s", err)
		}
		return
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

	msg := message.NewBlockFlightsMsg(flights, message.WithSyn(req[0].Syn))
	log.Debugf("share block task: %s, req from %s-%s-%d to %s-%s-%d of peer wallet: %s,"+
		" peer addr: %s, msgId: %s, syn %s, timeout %ds",
		taskId, req[0].FileHash, req[0].Hash, req[0].Index, req[len(req)-1].FileHash, req[len(req)-1].Hash,
		req[len(req)-1].Index, req[len(req)-1].WalletAddress, req[len(req)-1].PeerAddr,
		msg.MessageId, req[0].Syn, consts.SHARE_BLOCKS_TIMEOUT)
	if err := client.P2PStreamSend(req[0].PeerAddr, taskId, msg.MessageId, msg.ToProtoMsg(),
		time.Duration(consts.SHARE_BLOCKS_TIMEOUT)*time.Second); err != nil {
		log.Errorf("share send block, err: %s", err)
		// TODO: delete unpaid msg if need
		this.DeleteFileUnpaid(reqWalletAddr, paymentId, reqAsset, totalAmount)
		return
	}
	// update share progress
	oldProgress := this.DB.GetTaskPeerProgress(taskId, req[0].PeerAddr)
	if oldProgress == nil {
		this.DB.UpdateTaskPeerProgress(taskId, req[0].PeerAddr, uint64(len(req)))
		return
	}
	this.DB.UpdateTaskPeerProgress(taskId, req[0].PeerAddr, oldProgress.Progress+uint64(len(req)))
}

func (this *ShareTask) CanShareTo(walletAddress string, asset int32) bool {
	taskId := this.Id
	unpaidAmount, err := this.DB.GetUnpaidAmount(taskId, walletAddress, asset)
	maxUnpaidAmount := uint64(consts.CHUNK_SIZE * consts.MAX_REQ_BLOCK_COUNT * this.Mgr.Config().MaxUnpaidPayment)
	if err != nil || unpaidAmount >= maxUnpaidAmount {
		log.Errorf("cant share to %s for file %s, unpaidAmount: %d err %s",
			walletAddress, taskId, unpaidAmount, err)
		return false
	}
	return true
}
