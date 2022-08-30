package download

import (
	"fmt"
	"time"

	"github.com/saveio/dsp-go-sdk/actor/client"
	"github.com/saveio/dsp-go-sdk/consts"
	sdkErr "github.com/saveio/dsp-go-sdk/error"
	"github.com/saveio/dsp-go-sdk/network/message"
	"github.com/saveio/dsp-go-sdk/network/message/types/file"
	"github.com/saveio/dsp-go-sdk/task/types"
	chainCom "github.com/saveio/themis/common"
	"github.com/saveio/themis/common/log"
)

// getFileProveNode. get file storing nodes  and stored (expired) nodes
func (this *DownloadTask) getFileProvedNode(fileHashStr string) []string {
	proveDetails, err := this.Mgr.Chain().GetFileProveDetails(fileHashStr)
	if proveDetails == nil {
		return nil
	}
	nodes := make([]string, 0)
	log.Debugf("download task %s file %s prove details length %v, err: %v",
		this.GetId(), fileHashStr, len(proveDetails.ProveDetails), err)
	for _, detail := range proveDetails.ProveDetails {
		log.Debugf("file %s, node: %s, prove times %d", fileHashStr, string(detail.NodeAddr), detail.ProveTimes)
		if detail.ProveTimes == 0 {
			continue
		}
		nodes = append(nodes, string(detail.NodeAddr))
	}
	return nodes
}

// DepositChannelForFile. deposit channel. peerPrices: peer network address <=> payment info
func (this *DownloadTask) depositChannel() error {
	// taskId := this.taskMgr.TaskId(fileHashStr, this.GetCurrentWalletAddr(), store.TaskTypeDownload)
	taskId := this.GetId()
	if !this.DB.IsTaskInfoExist(taskId) {
		return sdkErr.New(sdkErr.GET_FILEINFO_FROM_DB_ERROR, "download info not exist")
	}
	if !this.Mgr.Config().AutoSetupDNSEnable {
		return nil
	}
	totalCount := this.GetTotalBlockCnt()
	log.Debugf("download task %s totalCount %d", taskId, totalCount)
	if totalCount == 0 {
		return sdkErr.New(sdkErr.NO_BLOCK_TO_DOWNLOAD, "no blocks")
	}
	totalAmount := consts.FILE_DOWNLOAD_UNIT_PRICE * uint64(totalCount) * uint64(consts.CHUNK_SIZE)
	log.Debugf("deposit to channel price: %d, totalCount: %d, chunksize: %d, totalAmount: %d",
		consts.FILE_DOWNLOAD_UNIT_PRICE, totalCount, consts.CHUNK_SIZE, totalAmount)
	if totalAmount/consts.FILE_DOWNLOAD_UNIT_PRICE != uint64(totalCount)*uint64(consts.CHUNK_SIZE) {
		return sdkErr.New(sdkErr.INTERNAL_ERROR, "deposit amount overflow")
	}
	curBal, _ := this.Mgr.Channel().GetAvailableBalance(this.Mgr.DNS().CurrentDNSWallet())
	if curBal >= totalAmount {
		return nil
	}
	log.Debugf("download task %s start to deposit", taskId)
	if err := this.Mgr.Channel().SetDeposit(this.Mgr.DNS().CurrentDNSWallet(), totalAmount); err != nil {
		log.Errorf("download task %s deposit dns %s error %s", taskId, this.Mgr.DNS().CurrentDNSWallet(), err)
		return err
	}
	return nil

}

// payForBlock. Pay for block with media transfer. PayInfo is some payment info of receiver,
// addr is a host address of receiver.
// BlockSize is the size to pay in KB. If newpayment flag is set, it is set to a new payment
// and will accumulate to unpaid amount.
func (this *DownloadTask) payForBlock(payInfo *file.Payment, blockSize uint64, paymentId int32, newPayment bool) error {
	if payInfo == nil {
		return nil
	}
	if payInfo.WalletAddress == this.GetCurrentWalletAddr() {
		return sdkErr.New(sdkErr.PAY_BLOCK_TO_SELF, "can't pay to self : %s", payInfo.WalletAddress)
	}
	if this.Mgr.DNS() == nil || !this.Mgr.DNS().HasDNS() {
		return sdkErr.New(sdkErr.NO_CONNECTED_DNS, "no dns")
	}
	amount := blockSize * payInfo.UnitPrice
	// if blockSize%consts.CHUNK_SIZE != 0 {
	// 	amount = (blockSize/consts.CHUNK_SIZE + 1) * consts.CHUNK_SIZE
	// }
	if amount/blockSize != payInfo.UnitPrice {
		return sdkErr.New(sdkErr.INTERNAL_ERROR, "total price overflow")
	}
	taskId := this.GetId()
	if amount == 0 {
		log.Warn("download task %s pay amount is zero", taskId)
		return nil
	}
	if paymentId == 0 {
		log.Warn("download task %s paymentId is zero", taskId)
		return nil
	}

	if newPayment {
		if err := this.DB.AddFileUnpaid(taskId, payInfo.WalletAddress, paymentId, payInfo.Asset,
			amount); err != nil {
			return err
		}
	}

	if this.PayOnLayer1() && !this.Mgr.Config().EnableLayer2 {
		return this.fastTransfer(taskId, payInfo, paymentId, amount)
	}

	fileHashStr := this.GetFileHash()
	// use default dns to pay
	if err := this.checkDNSState(this.Mgr.DNS().CurrentDNSWallet()); err == nil {
		log.Debugf("paying for file %s, to %s, id %v, use dns: %s, amount %v, balance %v",
			fileHashStr, payInfo.WalletAddress, paymentId, this.Mgr.DNS().CurrentDNSWallet(), amount,
			this.GetChannelBalance(this.Mgr.DNS().CurrentDNSWallet()))
		if err := this.Mgr.Channel().MediaTransfer(paymentId, amount, this.Mgr.DNS().CurrentDNSWallet(),
			payInfo.WalletAddress); err == nil {
			// clean unpaid order
			if err := this.DB.DeleteFileUnpaid(taskId, payInfo.WalletAddress, paymentId,
				payInfo.Asset, amount); err != nil {
				return err
			}
			log.Debugf("paying for file %s, to %s, id %v, success amount %v, balance %v",
				fileHashStr, payInfo.WalletAddress, paymentId, amount,
				this.GetChannelBalance(this.Mgr.DNS().CurrentDNSWallet()))
			// active peer to prevent pay too long
			this.ActiveWorker(payInfo.WalletAddress)
			return nil
		} else {
			this.ActiveWorker(payInfo.WalletAddress)
			log.Debugf("mediaTransfer failed paymentId %d, payTo: %s, err %s",
				paymentId, payInfo.WalletAddress, err)
		}
	}

	// use other dns to pay
	allCh, err := this.Mgr.Channel().AllChannels()
	if err != nil {
		return err
	}
	paySuccess := false
	for _, ch := range allCh.Channels {
		if ch.Address == this.Mgr.DNS().CurrentDNSWallet() {
			continue
		}
		if err := this.checkDNSState(ch.Address); err == nil {
			continue
		}

		log.Debugf("paying for file %s, to %s, id %v, use dns: %s, amount %v balance %v", fileHashStr, payInfo.WalletAddress, paymentId, ch.Address, amount, this.GetChannelBalance(ch.Address))
		if err := this.Mgr.Channel().MediaTransfer(paymentId, amount, ch.Address, payInfo.WalletAddress); err != nil {
			log.Debugf("mediaTransfer failed paymentId %d, payTo: %s, err %s", paymentId, payInfo.WalletAddress, err)
			this.ActiveWorker(payInfo.WalletAddress)
			continue
		}
		this.ActiveWorker(payInfo.WalletAddress)
		paySuccess = true
		log.Debugf("paying for file %s ,to %s, id %v success, balance %v", fileHashStr, payInfo.WalletAddress, paymentId, this.GetChannelBalance(ch.Address))
		break
	}
	if !paySuccess {
		if this.Mgr.Config().EnableLayer2 {
			return fmt.Errorf("mediaTransfer failed paymentId %v, payto %v", paymentId, payInfo.WalletAddress)
		}
		err := this.fastTransfer(taskId, payInfo, paymentId, amount)
		if err != nil {
			return fmt.Errorf("fast pay failed for %d", paymentId)
		}
		_ = this.setPayOnLayer1(true)
		return nil
	}
	// clean unpaid order
	if err := this.DB.DeleteFileUnpaid(taskId, payInfo.WalletAddress, paymentId, payInfo.Asset,
		amount); err != nil {
		return err
	}
	log.Debugf("delete unpaid %d", amount)
	return nil
}

func (this *DownloadTask) GetChannelBalance(walletAddr string) uint64 {
	info, err := this.Mgr.Channel().GetChannelInfo(walletAddr)
	if err != nil {
		log.Errorf("get channel info err %s for wallet %s", err, walletAddr)
	}
	return info.Balance
}

// fastTransfer. transfer asset on layer-1 contract, store payment id in contract state
func (this *DownloadTask) fastTransfer(taskId string, payInfo *file.Payment, paymentId int32, amount uint64) error {
	receiverAddr, err := chainCom.AddressFromBase58(payInfo.WalletAddress)
	if err != nil {
		return err
	}
	txHash, err := this.Mgr.Chain().FastTransfer(uint64(paymentId), this.Mgr.Chain().Address(), receiverAddr, amount)
	if err != nil {
		return err
	}
	txHeight, err := this.Mgr.Chain().PollForTxConfirmed(time.Duration(consts.TX_CONFIRM_TIMEOUT)*time.Second, txHash)
	if err != nil {
		return err
	}

	log.Debugf("task %s sending payment msg %v to %s, amount %v, txHash: %s txHeight: %v",
		taskId, paymentId, payInfo.WalletAddress, amount, txHash, txHeight)
	msg := message.NewPaymentMsg(
		this.GetCurrentWalletAddr(),
		payInfo.WalletAddress,
		paymentId,
		consts.ASSET_USDT,
		amount,
		txHash,
		message.WithSign(this.Mgr.Chain().CurrentAccount(), this.Mgr.Chain().GetChainType()),
	)
	_, err = client.P2PSendAndWaitReply(payInfo.WalletAddress, msg.MessageId, msg.ToProtoMsg())
	if err != nil {
		return err
	}
	log.Debugf("task %s sent fast transfer payment msg with paymentId %v", taskId, paymentId)
	if err := this.DB.DeleteFileUnpaid(taskId, payInfo.WalletAddress, paymentId,
		payInfo.Asset, amount); err != nil {
		return err
	}
	log.Debugf("delete unpaid %d", amount)
	// active peer to prevent pay too long
	this.ActiveWorker(payInfo.WalletAddress)
	return nil
}

// payUnpaidPayments. pay unpaid amount for file
func (this *DownloadTask) payUnpaidPayments(peerPrices map[string]*file.Payment) error {
	taskId := this.GetId()
	fileHashStr := this.GetFileHash()
	for _, payInfo := range peerPrices {
		payments, _ := this.DB.GetUnpaidPayments(taskId, payInfo.WalletAddress, payInfo.Asset)
		if len(payments) == 0 {
			continue
		}
		log.Debugf("task %s, file %s ,unpaid amount %v", taskId, fileHashStr, len(payments))
		for _, payment := range payments {
			log.Debugf("pay to %s of the unpaid amount %d for task %s", payInfo.WalletAddress, payment.Amount, taskId)
			this.EmitProgress(types.TaskDownloadPayForBlocks)
			err := this.payForBlock(payInfo, payment.Amount/payInfo.UnitPrice, payment.PaymentId, false)
			if err != nil {
				log.Errorf("pay unpaid payment err %s", err)
				this.EmitProgress(types.TaskDownloadPayForBlocksFailed)
			} else {
				this.EmitProgress(types.TaskDownloadPayForBlocksDone)
			}
		}
	}
	return nil
}
