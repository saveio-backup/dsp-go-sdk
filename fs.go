package dsp

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"math/rand"
	"os"
	"strings"
	"time"

	ipld "gx/ipfs/Qme5bWv7wtjUNGsK2BNGVUFPKiuxWrsqrtvYwCLRw8YFES/go-ipld-format"

	"github.com/gogo/protobuf/proto"
	"github.com/saveio/dsp-go-sdk/actor/client"
	"github.com/saveio/dsp-go-sdk/common"
	"github.com/saveio/dsp-go-sdk/config"
	netcom "github.com/saveio/dsp-go-sdk/network/common"
	"github.com/saveio/dsp-go-sdk/network/message"
	"github.com/saveio/dsp-go-sdk/network/message/types/file"
	"github.com/saveio/dsp-go-sdk/store"
	"github.com/saveio/dsp-go-sdk/task"
	"github.com/saveio/dsp-go-sdk/utils"
	"github.com/saveio/max/importer/helpers"
	chainCom "github.com/saveio/themis/common"
	"github.com/saveio/themis/common/log"
	"github.com/saveio/themis/crypto/pdp"
)

func (this *Dsp) CalculateUploadFee(filePath string, opt *common.UploadOption, whitelistCnt uint64) (uint64, error) {
	fee := uint64(0)
	fi, err := os.Open(filePath)
	if err != nil {
		return 0, err
	}
	fileInfo, err := fi.Stat()
	if err != nil {
		return 0, err
	}
	fsSetting, err := this.Chain.Native.Fs.GetSetting()
	if err != nil {
		return 0, err
	}
	userSpace, _ := this.Chain.Native.Fs.GetUserSpace(this.CurrentAccount().Address)
	currentHeight, err := this.Chain.GetCurrentBlockHeight()
	if err != nil {
		return 0, err
	}
	estimatedTimes := uint64(math.Ceil((float64(userSpace.ExpireHeight) - float64(currentHeight)) / float64(fsSetting.DefaultProvePeriod)))
	defer func() {
		err = fi.Close()
		if err != nil {
			log.Errorf("close file %s err %s", filePath, err)
		}
	}()
	txGas := uint64(10000000)
	log.Debugf("opt.interval:%d", opt.ProveInterval)
	log.Debugf("opt.ProveTimes:%d", opt.ProveTimes)
	log.Debugf("opt.CopyNum:%d", opt.CopyNum)
	useDefalut := (opt.ProveInterval == 0 && opt.ProveTimes == 0 && opt.CopyNum == 0) ||
		(userSpace != nil && opt.ProveInterval == fsSetting.DefaultProvePeriod && uint64(opt.ProveTimes) == estimatedTimes && uint64(opt.CopyNum) == fsSetting.DefaultCopyNum)
	log.Debugf("estimatedTimes: %d, userspace.expired %d, current %d, usedDefault:%t", estimatedTimes, userSpace.ExpireHeight, currentHeight, useDefalut)
	if whitelistCnt > 0 {
		fee = txGas * 4
	} else {
		fee = txGas * 3
	}
	if useDefalut {
		return fee, nil
	}
	fileSize := uint64(fileInfo.Size()) / 1024
	if fileSize < 0 {
		fileSize = 1
	}
	log.Debugf("fileSize :%d", fileSize)
	fee += (opt.ProveInterval*fileSize*fsSetting.GasPerKBPerBlock +
		fsSetting.GasForChallenge) * uint64(opt.ProveTimes) * fsSetting.FsGasPrice * (uint64(opt.CopyNum) + 1)
	return fee, nil
}

// UploadFile upload file logic synchronously
func (this *Dsp) UploadFile(filePath string, opt *common.UploadOption) (*common.UploadResult, error) {
	var uploadRet *common.UploadResult
	var err error
	if err = uploadOptValid(filePath, opt); err != nil {
		return nil, err
	}
	if this.Chain.Native.Fs.DefAcc == nil {
		return nil, errors.New("account is nil")
	}
	var tx, fileHashStr string
	var totalCount uint64
	root, list, err := this.Fs.NodesFromFile(filePath, this.Chain.Native.Fs.DefAcc.Address.ToBase58(), opt.Encrypt, opt.EncryptPassword)
	if err != nil {
		log.Errorf("node from file err: %s", err)
		return nil, err
	}

	fileHashStr = root.Cid().String()
	taskKey := this.taskMgr.TaskKey(fileHashStr, this.WalletAddress(), task.TaskTypeUpload)
	// emit result
	defer func() {
		this.taskMgr.EmitResult(taskKey, uploadRet, err)
		// delete task from cache in the end
		this.taskMgr.DeleteTask(taskKey)
		err = this.taskMgr.DeleteFileUploadInfo(taskKey)
		if err != nil {
			log.Errorf("delete file upload info err: %s", err)
		}
	}()
	if this.taskMgr.TaskExist(taskKey) {
		err = errors.New("upload task is exist")
		return nil, err
	}
	this.taskMgr.NewTask(fileHashStr, this.WalletAddress(), task.TaskTypeUpload)
	this.taskMgr.SetFileName(taskKey, opt.FileDesc)
	// this.taskMgr.AddFileName(taskKey, opt.FileDesc)
	go this.taskMgr.EmitProgress(taskKey)
	log.Debugf("root:%s, list.len:%d", fileHashStr, len(list))
	// get nodeList
	nodeList, err := this.getUploadNodeList(filePath, taskKey, fileHashStr)
	if err != nil {
		return nil, err
	}
	if uint32(len(nodeList)) < opt.CopyNum+1 {
		err = fmt.Errorf("node is not enough %d, copyNum %d", len(nodeList), opt.CopyNum)
		return nil, err
	}
	totalCount = uint64(len(list) + 1)
	// pay file
	payRet, err := this.payForSendFile(filePath, taskKey, fileHashStr, uint64(len(list)+1), opt)
	if err != nil {
		return nil, err
	}
	var addWhiteListTx string
	if len(opt.WhiteList) > 0 {
		addWhiteListTx, err = this.addWhitelistsForFile(fileHashStr, opt.WhiteList, opt.ProveInterval*uint64(opt.ProveTimes))
		if err != nil {
			return nil, err
		}
	}
	tx = payRet.Tx
	p, err := this.Chain.Native.Fs.ProveParamDes(payRet.ParamsBuf)
	if err != nil {
		return nil, err
	}
	g0, fileID := p.G0, p.FileId
	hashes, err := this.Fs.AllBlockHashes(root, list)
	if err != nil {
		return nil, err
	}
	listMap, err := this.Fs.BlocksListToMap(list)
	if err != nil {
		return nil, err
	}
	this.taskMgr.SetFileBlocksTotalCount(taskKey, uint64(len(list)+1))
	if opt != nil && opt.Share {
		go this.shareUploadedFile(filePath, opt.FileDesc, this.WalletAddress(), hashes, root, list)
	}
	receivers, err := this.waitFileReceivers(taskKey, fileHashStr, nodeList, hashes, int(opt.CopyNum)+1)
	if err != nil {
		return nil, err
	}
	log.Debugf("receivers:%v", receivers)
	if len(receivers) < int(opt.CopyNum)+1 {
		err = errors.New("file receivers is not enough")
		return nil, err
	}
	// notify fetch ready to all receivers
	err = this.notifyFetchReady(taskKey, fileHashStr, receivers)
	if err != nil {
		return nil, err
	}
	req, err := this.taskMgr.TaskBlockReq(taskKey)
	if err != nil {
		return nil, err
	}
	go this.taskMgr.EmitProgress(taskKey)
	log.Debugf("wait for fetching block")
	finish := false
	timeout := time.NewTimer(time.Duration(common.BLOCK_FETCH_TIMEOUT) * time.Second)
	// TODO: replace offset calculate by fs api
	offset := int64(0)
	for {
		select {
		case reqInfo := <-req:
			timeout.Reset(time.Duration(common.BLOCK_FETCH_TIMEOUT) * time.Second)
			// send block
			var blockData []byte
			var dataLen int64
			if reqInfo.Hash == fileHashStr && reqInfo.Index == 0 {
				blockData = this.Fs.BlockDataOfAny(root)
				blockDecodedData, err := this.Fs.BlockToBytes(root)
				if err != nil {
					return nil, err
				}
				dataLen = int64(len(blockDecodedData))
			} else {
				unixNode := listMap[fmt.Sprintf("%s%d", reqInfo.Hash, reqInfo.Index)]
				unixBlock, err := unixNode.GetDagNode()
				if err != nil {
					return nil, err
				}
				blockData = this.Fs.BlockDataOfAny(unixNode)
				blockDecodedData, err := this.Fs.BlockToBytes(unixBlock)
				if err != nil {
					return nil, err
				}
				dataLen = int64(len(blockDecodedData))
			}
			offset += dataLen
			if len(blockData) == 0 {
				log.Errorf("block is nil hash %s, peer %s failed, err %s", reqInfo.Hash, reqInfo.PeerAddr, err)
				break
			}
			isStored := this.taskMgr.IsBlockUploaded(taskKey, reqInfo.Hash, reqInfo.PeerAddr, uint32(reqInfo.Index))
			if isStored {
				log.Debugf("block has stored %s", reqInfo.Hash)
				break
			}
			tag, err := pdp.SignGenerate(blockData, fileID, uint32(reqInfo.Index)+1, g0, payRet.PrivateKey)
			if err != nil {
				log.Errorf("generate tag hash %s, peer %s failed, err %s", reqInfo.Hash, reqInfo.PeerAddr, err)
				return nil, err
			}
			msg := message.NewBlockMsg(int32(reqInfo.Index), fileHashStr, reqInfo.Hash, blockData, tag, offset-dataLen)
			log.Debugf("send fetched block msg len:%d, block len:%d, tag len:%d", msg.Header.MsgLength, dataLen, len(tag))
			err = client.P2pSend(reqInfo.PeerAddr, msg.ToProtoMsg())
			if err != nil {
				log.Errorf("send block msg hash %s to peer %s failed, err %s", reqInfo.Hash, reqInfo.PeerAddr, err)
				return nil, err
			}
			log.Debugf("send block success %s, index:%d, taglen:%d, offset:%d add uploaded block to db", reqInfo.Hash, reqInfo.Index, len(tag), offset-dataLen)
			// stored
			this.taskMgr.AddUploadedBlock(taskKey, reqInfo.Hash, reqInfo.PeerAddr, uint32(reqInfo.Index))
			// update progress
			go this.taskMgr.EmitProgress(taskKey)
			if len(this.taskMgr.GetUploadedBlockNodeList(taskKey, reqInfo.Hash, uint32(reqInfo.Index))) < int(opt.CopyNum)+1 {
				break
			}
			sent := uint64(this.taskMgr.UploadedBlockCount(taskKey))
			if totalCount == sent {
				log.Infof("all block has sent")
				finish = true
				break
			}
		case <-timeout.C:
			err = errors.New("wait for fetch block timeout")
			return nil, err
		}
		if finish {
			break
		}
	}
	proved := this.checkFileBeProved(fileHashStr, opt.ProveTimes, opt.CopyNum)
	log.Debugf("checking file proved done %t", proved)
	if !proved {
		err = errors.New("file has sent, but no enought prove is finished")
		return nil, err
	}

	oniLink := utils.GenOniLink(fileHashStr, opt.FileDesc, 0, totalCount, this.TrackerUrls)
	var dnsRegTx, dnsBindTx string
	if opt.RegisterDns && len(opt.DnsUrl) > 0 {
		dnsRegTx, err = this.RegisterFileUrl(opt.DnsUrl, oniLink)
		log.Debugf("reg dns %s, err %s", dnsRegTx, err)
	}
	if opt.BindDns && len(opt.DnsUrl) > 0 {
		dnsBindTx, err = this.BindFileUrl(opt.DnsUrl, oniLink)
		log.Debugf("bind dns %s, err %s", dnsBindTx, err)
	}
	uploadRet = &common.UploadResult{
		Tx:             tx,
		FileHash:       fileHashStr,
		Url:            opt.DnsUrl,
		Link:           oniLink,
		RegisterDnsTx:  dnsRegTx,
		BindDnsTx:      dnsBindTx,
		AddWhiteListTx: addWhiteListTx,
	}
	log.Debug("upload success uploadRet: %v!!", uploadRet)
	return uploadRet, nil
}

// DeleteUploadedFile. Delete uploaded file from remote nodes. it is called by the owner
func (this *Dsp) DeleteUploadedFile(fileHashStr string) (string, error) {
	if len(fileHashStr) == 0 {
		return "", errors.New("delete file hash string is empty")
	}
	info, err := this.Chain.Native.Fs.GetFileInfo(fileHashStr)
	if err != nil || info == nil {
		log.Debugf("info:%v, err:%s", info, err)
		return "", fmt.Errorf("file info not found, %s has deleted", fileHashStr)
	}
	if info.FileOwner.ToBase58() != this.Chain.Native.Fs.DefAcc.Address.ToBase58() {
		return "", fmt.Errorf("file %s can't be deleted, you are not the owner", fileHashStr)
	}
	storingNode, _ := this.getFileProveNode(fileHashStr, info.ChallengeTimes)
	txHash, err := this.Chain.Native.Fs.DeleteFile(fileHashStr)
	if err != nil {
		return "", err
	}
	log.Debugf("delete file txHash %s", hex.EncodeToString(chainCom.ToArrayReverse(txHash)))
	confirmed, err := this.Chain.PollForTxConfirmed(time.Duration(common.TX_CONFIRM_TIMEOUT)*time.Second, txHash)
	if err != nil || !confirmed {
		return "", errors.New("wait for tx confirmed failed")
	}
	taskKey := this.taskMgr.TaskKey(fileHashStr, this.WalletAddress(), task.TaskTypeUpload)
	if len(storingNode) == 0 {
		// find breakpoint keep nodelist
		storingNode = append(storingNode, this.taskMgr.GetUploadedBlockNodeList(taskKey, fileHashStr, 0)...)
	}
	msg := message.NewFileDelete(fileHashStr, this.Chain.Native.Fs.DefAcc.Address.ToBase58())
	err = client.P2pBroadcast(storingNode, msg.ToProtoMsg(), true, nil, nil)
	if err != nil {
		return "", err
	}
	err = this.taskMgr.DeleteFileUploadInfo(taskKey)
	if err != nil {
		log.Errorf("delete upload info from db err: %s", err)
	}
	return hex.EncodeToString(chainCom.ToArrayReverse(txHash)), nil
}

func (this *Dsp) Progress() {
	this.RegProgressChannel()
	go func() {
		stop := false
		for {
			v := <-this.ProgressChannel()
			for node, cnt := range v.Count {
				log.Infof("file:%s, hash:%s, total:%d, peer:%s, uploaded:%d, progress:%f", v.FileName, v.FileHash, v.Total, node, cnt, float64(cnt)/float64(v.Total))
				stop = (cnt == v.Total)
			}
			if stop {
				break
			}
		}
		// TODO: why need close
		this.CloseProgressChannel()
	}()
}

func (this *Dsp) StartShareServices() {
	ch := this.taskMgr.BlockReqCh()
	for {
		select {
		case req, ok := <-ch:
			if !ok {
				log.Errorf("block req channel false")
				break
			}
			taskKey := this.taskMgr.TaskKey(req.FileHash, req.WalletAddress, task.TaskTypeShare)
			// check if has unpaid block request
			canShare, err := this.taskMgr.CanShareTo(taskKey, req.WalletAddress)
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
			downloadTaskKey := this.taskMgr.TaskKey(req.FileHash, this.WalletAddress(), task.TaskTypeDownload)
			offset, err := this.taskMgr.BlockOffset(downloadTaskKey, req.Hash, uint32(req.Index))
			if err != nil {
				log.Errorf("get block offset err%s", err)
				break
			}
			log.Debugf("share block %s, index:%d,  offset %d", req.Hash, req.Index, offset)
			// TODO: only send tag with tagflag enabled
			// TEST: client get tag
			tag, _ := this.Fs.GetTag(req.Hash, req.FileHash, uint64(req.Index))
			msg := message.NewBlockMsg(req.Index, req.FileHash, req.Hash, blockData, tag, int64(offset))
			client.P2pSend(req.PeerAddr, msg.ToProtoMsg())
			up, err := this.GetFileUnitPrice(req.Asset)
			if err != nil {
				log.Errorf("get file unit price err after send block, err: %s", err)
				break
			}
			// add new unpaid block request to store
			this.taskMgr.AddShareFileUnpaid(taskKey, req.WalletAddress, req.Asset, uint64(len(blockData))*up)
		}
	}
}

// DownloadFile. download file, peice by peice from addrs.
// inOrder: if true, the file will be downloaded block by block in order
// free: if true, query nodes who can share file free
// maxPeeCnt: download with max number peers with min price
func (this *Dsp) DownloadFile(fileHashStr, fileName string, asset int32, inOrder bool, decryptPwd string, free bool, maxPeerCnt int) error {
	// start a task
	taskKey := this.taskMgr.NewTask(fileHashStr, this.WalletAddress(), task.TaskTypeDownload)
	go this.taskMgr.EmitProgress(taskKey)
	this.taskMgr.SetFileName(taskKey, fileName)
	addrs := this.GetPeerFromTracker(fileHashStr, this.TrackerUrls)
	if len(addrs) == 0 {
		log.Debugf("get 0 peer of %s from trackers %v", fileHashStr, this.TrackerUrls)
	}
	err := this.downloadFileFromPeers(fileHashStr, asset, inOrder, decryptPwd, free, maxPeerCnt, addrs)
	this.taskMgr.EmitResult(taskKey, nil, err)
	// delete task from cache in the end
	this.taskMgr.DeleteTask(taskKey)
	return err
}

// DownloadFileByLink. download file by link, e.g oni://Qm...&name=xxx&tr=xxx
func (this *Dsp) DownloadFileByLink(link string, asset int32, inOrder bool, decryptPwd string, free bool, maxPeerCnt int) error {
	fileHashStr := utils.GetFileHashFromLink(link)
	linkvalues := this.GetLinkValues(link)
	fileName := linkvalues[common.FILE_LINK_NAME_KEY]
	return this.DownloadFile(fileHashStr, fileName, asset, inOrder, decryptPwd, free, maxPeerCnt)
}

// DownloadFileByUrl. download file by link, e.g dsp://file1
func (this *Dsp) DownloadFileByUrl(url string, asset int32, inOrder bool, decryptPwd string, free bool, maxPeerCnt int) error {
	fileHashStr := this.GetFileHashFromUrl(url)
	linkvalues := this.GetLinkValues(this.GetLinkFromUrl(url))
	fileName := linkvalues[common.FILE_LINK_NAME_KEY]
	return this.DownloadFile(fileHashStr, fileName, asset, inOrder, decryptPwd, free, maxPeerCnt)
}

// GetDownloadQuotation. get peers and the download price of the file. if free flag is set, return price-free peers.
func (this *Dsp) GetDownloadQuotation(fileHashStr string, asset int32, free bool, addrs []string) (map[string]*file.Payment, error) {
	if len(addrs) == 0 {
		return nil, errors.New("no peer for download")
	}
	msg := message.NewFileDownloadAsk(fileHashStr, this.Chain.Native.Fs.DefAcc.Address.ToBase58(), asset)
	peerPayInfos := make(map[string]*file.Payment, 0)
	blockHashes := make([]string, 0)
	prefix := ""
	reply := func(msg proto.Message, addr string) {
		p2pMsg := message.ReadMessage(msg)
		if p2pMsg.Error != nil {
			log.Errorf("get download ask err %s", p2pMsg.Error.Message)
			return
		}
		fileMsg := p2pMsg.Payload.(*file.File)
		if len(fileMsg.BlockHashes) < len(blockHashes) {
			return
		}
		if len(prefix) > 0 && fileMsg.Prefix != prefix {
			return
		}
		if free && (fileMsg.PayInfo != nil && fileMsg.PayInfo.UnitPrice != 0) {
			return
		}
		blockHashes = fileMsg.BlockHashes
		peerPayInfos[addr] = fileMsg.PayInfo
		prefix = fileMsg.Prefix
	}
	err := client.P2pBroadcast(addrs, msg.ToProtoMsg(), true, nil, reply)
	if err != nil {
		log.Errorf("file download err %s", err)
	}
	if len(peerPayInfos) == 0 {
		return nil, errors.New("no peerPayInfos for download")
	}
	log.Debugf("peer prices:%v", peerPayInfos)
	taskKey := this.taskMgr.TaskKey(fileHashStr, this.WalletAddress(), task.TaskTypeDownload)
	if this.taskMgr.IsDownloadInfoExist(taskKey) {
		return peerPayInfos, nil
	}
	err = this.taskMgr.AddFileBlockHashes(taskKey, blockHashes)
	if err != nil {
		return nil, err
	}
	err = this.taskMgr.AddFilePrefix(taskKey, prefix)
	if err != nil {
		return nil, err
	}
	this.taskMgr.AddFileName(taskKey, this.taskMgr.FileNameFromTask(taskKey))
	return peerPayInfos, nil
}

// DepositChannelForFile. deposit channel. peerPrices: peer network address <=> payment info
func (this *Dsp) DepositChannelForFile(fileHashStr string, peerPrices map[string]*file.Payment) error {
	taskKey := this.taskMgr.TaskKey(fileHashStr, this.WalletAddress(), task.TaskTypeDownload)
	if !this.taskMgr.IsDownloadInfoExist(taskKey) {
		return errors.New("download info not exist")
	}
	if len(peerPrices) == 0 {
		return errors.New("no peers to open channel")
	}
	blockHashes := this.taskMgr.FileBlockHashes(taskKey)
	if len(blockHashes) == 0 {
		return errors.New("no blocks")
	}
	totalAmount := common.FILE_DOWNLOAD_UNIT_PRICE * uint64(len(blockHashes)) * uint64(common.CHUNK_SIZE)
	log.Debugf("deposit to channel price:%d, cnt:%d, chunsize:%d, total:%d", common.FILE_DOWNLOAD_UNIT_PRICE, len(blockHashes), common.CHUNK_SIZE, totalAmount)
	if totalAmount/common.FILE_DOWNLOAD_UNIT_PRICE != uint64(len(blockHashes))*uint64(common.CHUNK_SIZE) {
		return errors.New("deposit amount overflow")
	}
	log.Debugf("this %v", this)
	log.Debugf("this.ch %v", this.Channel)
	curBal, _ := this.Channel.GetAvaliableBalance(this.DNSNode.WalletAddr)
	if curBal < totalAmount {
		log.Debugf("depositing...")
		err := this.Channel.SetDeposit(this.DNSNode.WalletAddr, totalAmount)
		log.Debugf("deposit result %s", err)
		if err != nil {
			return err
		}
	}
	for _, payInfo := range peerPrices {
		hostAddr, err := this.GetExternalIP(payInfo.WalletAddress)
		log.Debugf("Set host addr after deposit channel %s - %s", payInfo.WalletAddress, hostAddr)
		if len(hostAddr) == 0 || err != nil {
			continue
		}
		this.Channel.SetHostAddr(payInfo.WalletAddress, hostAddr)
	}
	return nil
}

// PayForData. pay for block
func (this *Dsp) PayForBlock(payInfo *file.Payment, addr, fileHashStr string, blockSize uint64) (int32, error) {
	if payInfo == nil {
		return 0, nil
	}
	amount := blockSize * payInfo.UnitPrice
	if amount/blockSize != payInfo.UnitPrice {
		return 0, errors.New("total price overflow")
	}
	if amount == 0 {
		return 0, nil
	}
	taskKey := this.taskMgr.TaskKey(fileHashStr, this.WalletAddress(), task.TaskTypeDownload)
	err := this.taskMgr.AddDownloadFileUnpaid(taskKey, payInfo.WalletAddress, payInfo.Asset, amount)
	if err != nil {
		return 0, err
	}
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	paymentId := r.Int31()
	log.Debugf("paying to %s, id %v, err:%s", payInfo.WalletAddress, paymentId, err)
	err = this.Channel.MediaTransfer(paymentId, amount, payInfo.WalletAddress)
	if err != nil {
		log.Debugf("payingmentid %d, failed err %s", paymentId, err)
		return 0, err
	}
	log.Debugf("payment id %d, for file:%s, size:%d, price:%d success", paymentId, fileHashStr, blockSize, amount)
	// send payment msg
	msg := message.NewPayment(this.Chain.Native.Fs.DefAcc.Address.ToBase58(), payInfo.WalletAddress, paymentId,
		payInfo.Asset, amount, fileHashStr, netcom.MSG_ERROR_CODE_NONE)
	// TODO: wait for receiver received notification (need optimized)
	time.Sleep(time.Second)
	_, err = client.P2pRequestWithRetry(msg.ToProtoMsg(), addr, common.MAX_NETWORK_REQUEST_RETRY)
	log.Debugf("payment msg response :%d, err:%s", paymentId, err)
	if err != nil {
		return 0, err
	}
	// clean unpaid order
	err = this.taskMgr.DeleteDownloadFileUnpaid(taskKey, payInfo.WalletAddress, payInfo.Asset, amount)
	if err != nil {
		return 0, err
	}
	this.taskMgr.SetWorkerPaid(taskKey, addr)
	log.Debugf("delete unpaid %d", amount)
	return paymentId, nil
}

// PayUnpaidFile. pay unpaid order for file
func (this *Dsp) PayUnpaidFile(fileHashStr string) error {
	// TODO: pay unpaid file at first
	return nil
}

// DownloadFileWithQuotation. download file, peice by peice from addrs.
// inOrder: if true, the file will be downloaded block by block in order
func (this *Dsp) DownloadFileWithQuotation(fileHashStr string, asset int32, inOrder bool, quotation map[string]*file.Payment, decryptPwd string) error {
	// task exist at runtime
	taskKey := this.taskMgr.TaskKey(fileHashStr, this.WalletAddress(), task.TaskTypeDownload)
	// if this.taskMgr.TaskExist(taskKey) {
	// 	return errors.New("download task is exist")
	// }
	if len(quotation) == 0 {
		return errors.New("no peer quotation for download")
	}
	if !this.taskMgr.IsDownloadInfoExist(taskKey) {
		return errors.New("download info not exist")
	}
	// pay unpaid order of the file after last download
	err := this.PayUnpaidFile(fileHashStr)
	if err != nil {
		return err
	}

	// new download logic
	msg := message.NewFileDownload(fileHashStr, this.Chain.Native.Fs.DefAcc.Address.ToBase58(), asset)
	addrs := make([]string, 0)
	for addr, _ := range quotation {
		addrs = append(addrs, addr)
	}
	log.Debugf("broad cast msg to %v", addrs)
	err = client.P2pBroadcast(addrs, msg.ToProtoMsg(), true, nil, nil)
	log.Debugf("brocast file download msg err %v", err)
	if err != nil {
		return err
	}
	blockHashes := this.taskMgr.FileBlockHashes(taskKey)
	prefix := this.taskMgr.FilePrefix(taskKey)
	log.Debugf("filehashstr:%v, blockhashes-len:%v, prefix:%v", fileHashStr, len(blockHashes), prefix)
	var file *os.File
	if this.Config.FsType == config.FS_FILESTORE {
		file, err = createDownloadFile(this.Config.FsFileRoot, fileHashStr)
		if err != nil {
			return err
		}
		defer func() {
			log.Debugf("close file defer")
			file.Close()
		}()
	}
	this.taskMgr.SetFileBlocksTotalCount(taskKey, uint64(len(blockHashes)))
	go this.taskMgr.EmitProgress(taskKey)
	// declare job for workers
	job := func(fHash, bHash, pAddr string, index int32, respCh chan *task.BlockResp) (*task.BlockResp, error) {
		log.Debugf("download %s-%s-%d from %s", fHash, bHash, index, pAddr)
		return this.downloadBlock(fHash, bHash, index, pAddr, respCh)
	}
	this.taskMgr.NewWorkers(taskKey, addrs, inOrder, job)
	go this.taskMgr.WorkBackground(taskKey)
	if inOrder {
		hash, index, err := this.taskMgr.GetUndownloadedBlockInfo(taskKey, fileHashStr)
		if err != nil {
			return err
		}
		if len(hash) == 0 {
			// removeTempFile(fileHashStr)
			return errors.New("no undownloaded block")
		}
		blockIndex := int32(index)
		err = this.taskMgr.AddBlockReq(taskKey, hash, blockIndex)
		if err != nil {
			return err
		}
		fullFilePath := this.Config.FsFileRoot + "/" + fileHashStr
		for {
			value, ok := <-this.taskMgr.TaskNotify(taskKey)
			if !ok {
				return errors.New("download internal error")
			}
			if this.taskMgr.IsBlockDownloaded(taskKey, value.Hash, uint32(value.Index)) {
				log.Debugf("%s-%s-%d is downloaded", fileHashStr, value.Hash, value.Index)
				continue
			}
			block := this.Fs.EncodedToBlockWithCid(value.Block, value.Hash)
			links, err := this.Fs.BlockLinks(block)
			if err != nil {
				return err
			}
			if block.Cid().String() == fileHashStr && this.Config.FsType == config.FS_FILESTORE {
				log.Debugf("set file prefix %s %s", fullFilePath, prefix)
				this.Fs.SetFsFilePrefix(fullFilePath, prefix)
			}
			if len(links) == 0 && this.Config.FsType == config.FS_FILESTORE {
				data := this.Fs.BlockData(block)
				// TEST: performance
				fileStat, err := file.Stat()
				if err != nil {
					return err
				}
				// cut prefix
				if fileStat.Size() == 0 && len(data) >= len(prefix) && string(data[:len(prefix)]) == prefix {
					data = data[len(prefix):]
				}
				// TEST: offset
				writeAtPos := int64(0)
				if value.Offset > 0 {
					writeAtPos = value.Offset - int64(len(prefix))
				}
				_, err = file.WriteAt(data, writeAtPos)
				if err != nil {
					return err
				}
			}
			payInfo := quotation[value.PeerAddr]
			_, err = this.PayForBlock(payInfo, value.PeerAddr, fileHashStr, uint64(len(value.Block)))
			if err != nil {
				return err
			}
			if this.Config.FsType == config.FS_FILESTORE {
				err = this.Fs.PutBlockForFileStore(fullFilePath, block, uint64(value.Offset))
				log.Debugf("put block for store err %v", err)
			} else {
				err = this.Fs.PutBlock(block)
				if err != nil {
					return err
				}
				log.Debugf("block %s value.index %d, value.tag:%d", block.Cid(), value.Index, len(value.Tag))
				err = this.Fs.PutTag(block.Cid().String(), fileHashStr, uint64(value.Index), value.Tag)
			}
			log.Debugf("put block for file %s block: %s, offset:%d", fullFilePath, block.Cid(), value.Offset)
			if err != nil {
				log.Errorf("put block err %s", err)
				return err
			}
			err = this.taskMgr.SetBlockDownloaded(taskKey, value.Hash, value.PeerAddr, uint32(value.Index), value.Offset, links)
			if err != nil {
				return err
			}
			go this.taskMgr.EmitProgress(taskKey)
			log.Debugf("%s-%s-%d set downloaded", fileHashStr, value.Hash, value.Index)
			for _, l := range links {
				blockIndex++
				err := this.taskMgr.AddBlockReq(taskKey, l, blockIndex)
				if err != nil {
					return err
				}
			}
			if len(links) != 0 {
				continue
			}
			// find a more accurate way
			if value.Index != blockIndex {
				continue
			}
			// last block
			this.taskMgr.SetTaskDone(taskKey, true)
			break
		}
		go this.PushToTrackers(fileHashStr, this.TrackerUrls, client.P2pGetPublicAddr())
		fileDonwloadOkMsg := message.NewFileDownloadOk(fileHashStr, this.Chain.Native.Fs.DefAcc.Address.ToBase58(), asset)
		log.Debugf("broad file donwload ok cast msg to %v", addrs)
		client.P2pBroadcast(addrs, fileDonwloadOkMsg.ToProtoMsg(), true, nil, nil)
		if len(decryptPwd) > 0 {
			err := this.Fs.AESDecryptFile(fullFilePath, decryptPwd, fullFilePath+"-decrypted")
			if err != nil {
				return err
			}
			return os.Rename(fullFilePath+"-decrypted", fullFilePath)
		}
		return nil
	}
	// TODO: support out-of-order download
	return nil
}

// DeleteDownloadedFile. Delete downloaded file in local.
func (this *Dsp) DeleteDownloadedFile(fileHashStr string) error {
	if len(fileHashStr) == 0 {
		return errors.New("delete file hash string is empty")
	}
	return this.deleteFile(fileHashStr)
}

// RegProgressChannel. register progress channel
func (this *Dsp) RegProgressChannel() {
	if this == nil {
		log.Errorf("this.taskMgr == nil")
	}
	this.taskMgr.RegProgressCh()
}

// GetProgressChannel.
func (this *Dsp) ProgressChannel() chan *task.ProgressInfo {
	return this.taskMgr.ProgressCh()
}

// CloseProgressChannel.
func (this *Dsp) CloseProgressChannel() {
	this.taskMgr.CloseProgressCh()
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

// StartBackupFileService. start a backup file service to find backup jobs.
func (this *Dsp) StartBackupFileService() {
	backupingCnt := 0
	ticker := time.NewTicker(time.Duration(common.BACKUP_FILE_DURATION) * time.Second)
	for {
		select {
		case <-ticker.C:
			if this.Chain == nil {
				break
			}
			tasks, err := this.Chain.Native.Fs.GetExpiredProveList()
			if err != nil || tasks == nil || len(tasks.Tasks) == 0 {
				break
			}
			if backupingCnt > 0 {
				log.Debugf("doing backup jobs %d", backupingCnt)
				break
			}
			// TODO: optimize with parallel download
			for _, t := range tasks.Tasks {
				addrCheckFailed := (t.LuckyAddr.ToBase58() != this.Chain.Native.Fs.DefAcc.Address.ToBase58()) ||
					(t.BackUpAddr.ToBase58() == this.Chain.Native.Fs.DefAcc.Address.ToBase58()) ||
					(t.BrokenAddr.ToBase58() == this.Chain.Native.Fs.DefAcc.Address.ToBase58()) ||
					(t.BrokenAddr.ToBase58() == t.BackUpAddr.ToBase58())
				if addrCheckFailed {
					log.Debugf("address check faield file %s, lucky: %s, backup: %s, broken: %s", string(t.FileHash), t.LuckyAddr.ToBase58(), t.BackUpAddr.ToBase58(), t.BrokenAddr.ToBase58())
					continue
				}
				if len(t.FileHash) == 0 || len(t.BakSrvAddr) == 0 || len(t.BackUpAddr.ToBase58()) == 0 {
					log.Debugf("get expired prove list params invalid:%v", t)
					continue
				}
				backupingCnt++
				log.Debugf("go backup file:%s, from:%s %s", t.FileHash, t.BakSrvAddr, t.BackUpAddr.ToBase58())
				err := this.downloadFileFromPeers(string(t.FileHash), common.ASSET_USDT, true, "", false, common.MAX_DOWNLOAD_PEERS_NUM, []string{string(t.BakSrvAddr)})
				if err != nil {
					log.Errorf("download file err %s", err)
					continue
				}
				err = this.Fs.PinRoot(context.TODO(), string(t.FileHash))
				if err != nil {
					log.Errorf("pin root file err %s", err)
					continue
				}
				log.Debugf("prove file %s, luckynum %d, bakheight:%d, baknum:%d", string(t.FileHash), t.LuckyNum, t.BakHeight, t.BakNum)
				err = this.Fs.StartPDPVerify(string(t.FileHash), t.LuckyNum, t.BakHeight, t.BakNum, t.BrokenAddr)
				if err != nil {
					log.Errorf("pdp verify error for backup task")
					continue
				}
				log.Debugf("backup file:%s success", t.FileHash)
			}
			// reset
			backupingCnt = 0
		}
	}
}

// AllDownloadFiles. get all downloaded file names
func (this *Dsp) AllDownloadFiles() []string {
	files := make([]string, 0)
	switch this.Config.FsType {
	case config.FS_FILESTORE:
		fileInfos, err := ioutil.ReadDir(this.Config.FsFileRoot)
		if err != nil || len(fileInfos) == 0 {
			return files
		}
		for _, info := range fileInfos {
			if info.IsDir() ||
				(!strings.HasPrefix(info.Name(), common.PROTO_NODE_PREFIX) && !strings.HasPrefix(info.Name(), common.RAW_NODE_PREFIX)) {
				continue
			}
			files = append(files, info.Name())
		}
	case config.FS_BLOCKSTORE:
		files, _ = this.taskMgr.AllDownloadFiles()
	}
	return files
}

func (this *Dsp) DownloadedFileInfo(fileHashStr string) (*store.FileInfo, error) {
	// my task. use my wallet address
	fileInfoKey := this.taskMgr.TaskKey(fileHashStr, this.WalletAddress(), task.TaskTypeDownload)
	return this.taskMgr.GetFileInfo([]byte(fileInfoKey))
}

// downloadFileFromPeers. downloadfile base methods. download file from peers.
func (this *Dsp) downloadFileFromPeers(fileHashStr string, asset int32, inOrder bool, decryptPwd string, free bool, maxPeerCnt int, addrs []string) error {
	quotation, err := this.GetDownloadQuotation(fileHashStr, asset, free, addrs)
	if err != nil {
		return err
	}
	if len(quotation) == 0 {
		return errors.New("no quotation from peers")
	}
	if !free && len(addrs) > maxPeerCnt {
		// filter peers
		quotation = utils.SortPeersByPrice(quotation, maxPeerCnt)
	}
	err = this.DepositChannelForFile(fileHashStr, quotation)
	if err != nil {
		return err
	}
	log.Debugf("set up channel success: %v", quotation)
	return this.DownloadFileWithQuotation(fileHashStr, asset, inOrder, quotation, decryptPwd)
}

// payForSendFile pay before send a file
// return PoR params byte slice, PoR private key of the file or error
func (this *Dsp) payForSendFile(filePath, taskKey, fileHashStr string, blockNum uint64, opt *common.UploadOption) (*common.PayStoreFileReulst, error) {
	fileInfo, _ := this.Chain.Native.Fs.GetFileInfo(fileHashStr)
	var paramsBuf, privateKey []byte
	var tx string
	if fileInfo == nil {
		log.Info("first upload file")
		minInterval := uint64(0)
		setting, _ := this.Chain.Native.Fs.GetSetting()
		if setting != nil {
			minInterval = setting.MinChallengeRate
		}
		if uint64(opt.ProveInterval) < minInterval {
			return nil, fmt.Errorf("challenge rate %d less than min challenge rate %d", opt.ProveInterval, minInterval)
		}
		blockSizeInKB := uint64(math.Ceil(float64(common.CHUNK_SIZE) / 1024.0))
		g, g0, pubKey, privKey, fileID := pdp.Init(filePath)
		var err error
		paramsBuf, err = this.Chain.Native.Fs.ProveParamSer(g, g0, pubKey, fileID)
		privateKey = privKey
		if err != nil {
			log.Errorf("serialzation prove params failed:%s", err)
			return nil, err
		}
		txHash, err := this.Chain.Native.Fs.StoreFile(fileHashStr, blockNum, blockSizeInKB, opt.ProveInterval,
			uint64(opt.ProveTimes), uint64(opt.CopyNum), []byte(opt.FileDesc), uint64(opt.Privilege), paramsBuf)
		if err != nil {
			log.Errorf("pay store file order failed:%s", err)
			return nil, err
		}
		tx = hex.EncodeToString(chainCom.ToArrayReverse(txHash))
		// TODO: check confirmed on receivers
		confirmed, err := this.Chain.PollForTxConfirmed(time.Duration(common.TX_CONFIRM_TIMEOUT)*time.Second, txHash)
		if err != nil || !confirmed {
			return nil, errors.New("tx is not confirmed")
		}
		err = this.taskMgr.PutFileUploadInfo(tx, taskKey, privateKey)
		if err != nil {
			return nil, err
		}
	} else {
		storing, stored := this.getFileProveNode(fileHashStr, fileInfo.ChallengeTimes)
		if len(storing) > 0 {
			return nil, fmt.Errorf("file:%s has stored", fileHashStr)
		}
		if len(stored) > 0 {
			return nil, fmt.Errorf("file:%s has expired, please delete it first", fileHashStr)
		}
		log.Debugf("has paid but not store")
		if uint64(this.taskMgr.UploadedBlockCount(taskKey)) == blockNum {
			return nil, fmt.Errorf("has sent all blocks, waiting for ont-ipfs node commit proves")
		}
		paramsBuf = fileInfo.FileProveParam
		privateKey = this.taskMgr.GetFileProvePrivKey(taskKey)
		tx = this.taskMgr.GetStoreFileTx(taskKey)
	}
	if len(paramsBuf) == 0 || len(privateKey) == 0 {
		return nil, fmt.Errorf("params.length is %d, prove private key length is %d. please delete file and re-send it", len(paramsBuf), len(privateKey))
	}
	return &common.PayStoreFileReulst{
		Tx:         tx,
		ParamsBuf:  paramsBuf,
		PrivateKey: privateKey,
	}, nil
}

// addWhitelistsForFile. add whitelist for file with added blockcount to current height
func (this *Dsp) addWhitelistsForFile(fileHashStr string, walletAddrs []string, blockCount uint64) (string, error) {
	txHash, err := this.Chain.Native.Fs.AddWhiteLists(fileHashStr, walletAddrs, blockCount)
	if err != nil {
		return "", err
	}
	tx := hex.EncodeToString(chainCom.ToArrayReverse(txHash))
	return tx, nil
}

// getFileProveNode. get file storing nodes  and stored (expired) nodes
func (this *Dsp) getFileProveNode(fileHashStr string, challengeTimes uint64) ([]string, []string) {
	proveDetails, err := this.Chain.Native.Fs.GetFileProveDetails(fileHashStr)
	storing, stored := make([]string, 0), make([]string, 0)
	if proveDetails == nil {
		return storing, stored
	}
	log.Debugf("details :%v, err:%s", len(proveDetails.ProveDetails), err)
	for _, detail := range proveDetails.ProveDetails {
		log.Debugf("node:%s, prove times %d, challenge times %d", string(detail.NodeAddr), detail.ProveTimes, challengeTimes)
		if detail.ProveTimes < challengeTimes+1 {
			storing = append(storing, string(detail.NodeAddr))
		} else {
			stored = append(stored, string(detail.NodeAddr))
		}
	}
	return storing, stored
}

// getUploadNodeList get nodelist from cache first. if empty, get nodelist from chain
func (this *Dsp) getUploadNodeList(filePath, taskKey, fileHashStr string) ([]string, error) {
	fileStat, err := os.Stat(filePath)
	if err != nil {
		return nil, err
	}
	nodeList := this.taskMgr.GetUploadedBlockNodeList(taskKey, fileHashStr, 0)
	if len(nodeList) != 0 {
		return nodeList, nil
	}

	infos, err := this.Chain.Native.Fs.GetNodeList()
	if err != nil {
		return nil, err
	}
	for _, info := range infos.NodeInfo {
		fileSizeInKb := uint64(math.Ceil(float64(fileStat.Size()) / 1024.0))
		if info.RestVol < fileSizeInKb {
			continue
		}
		fullAddress := string(info.NodeAddr)
		if fullAddress == client.P2pGetPublicAddr() {
			continue
		}
		nodeList = append(nodeList, fullAddress)
	}
	return nodeList, nil
}

// checkFileBeProved thread-block method. check if the file has been proved for all store nodes.
func (this *Dsp) checkFileBeProved(fileHashStr string, proveTimes, copyNum uint32) bool {
	retry := 0
	timewait := 5
	for {
		if retry > common.CHECK_PROVE_TIMEOUT/timewait {
			return false
		}
		storing, _ := this.getFileProveNode(fileHashStr, uint64(proveTimes))
		if uint32(len(storing)) >= copyNum+1 {
			break
		}
		retry++
		time.Sleep(time.Duration(timewait) * time.Second)
	}
	return true
}

// waitFileReceivers find nodes and send file give msg, waiting for file fetch msg
// client -> fetch_ask -> peer.
// client <- fetch_ack <- peer.
// return receivers
func (this *Dsp) waitFileReceivers(taskKey, fileHashStr string, nodeList, blockHashes []string, receiverCount int) ([]string, error) {
	receivers := make([]string, 0)
	msg := message.NewFileFetchAsk(fileHashStr, blockHashes, this.Chain.Native.Fs.DefAcc.Address.ToBase58(), this.Chain.Native.Fs.DefAcc.Address.ToBase58())
	action := func(res proto.Message, addr string) {
		log.Debugf("send file ask msg success %s", addr)
		// block waiting for ack msg or timeout msg
		isAck := false
		ack, err := this.taskMgr.TaskAck(taskKey)
		if err != nil {
			return
		}
		select {
		case <-ack:
			isAck = true
			log.Debugf("received ack from %s", addr)
			receivers = append(receivers, addr)
		case <-time.After(time.Duration(common.FILE_FETCH_ACK_TIMEOUT) * time.Second):
			if isAck {
				return
			}
			this.taskMgr.SetTaskTimeout(taskKey, true)
			return
		}
	}
	// TODO: make stop func sense in parallel mode
	stop := func() bool {
		return len(receivers) >= receiverCount
	}
	err := client.P2pBroadcast(nodeList, msg.ToProtoMsg(), false, stop, action)
	if err != nil {
		log.Errorf("wait file receivers broadcast err")
		return nil, err
	}
	if len(receivers) >= receiverCount {
		receivers = receivers[:receiverCount]
	}
	log.Debugf("receives :%v", receivers)
	return receivers, nil
}

// notifyFetchReady send fetch_rdy msg to receivers
func (this *Dsp) notifyFetchReady(taskKey, fileHashStr string, receivers []string) error {
	msg := message.NewFileFetchRdy(fileHashStr)
	this.taskMgr.SetTaskReady(taskKey, true)
	err := client.P2pBroadcast(receivers, msg.ToProtoMsg(), false, nil, nil)
	if err != nil {
		log.Errorf("notify err %s", err)
		this.taskMgr.SetTaskReady(taskKey, false)
	}
	return err
}

// startFetchBlocks for store node, fetch blocks one by one after receive fetch_rdy msg
func (this *Dsp) startFetchBlocks(fileHashStr string, addr string) error {
	// my task. use my wallet address
	taskKey := this.taskMgr.TaskKey(fileHashStr, this.WalletAddress(), task.TaskTypeDownload)
	// task exist at runtime
	if this.taskMgr.TaskExist(taskKey) {
		return errors.New("fetch task is exist")
	}
	blockHashes := this.taskMgr.FileBlockHashes(taskKey)
	if len(blockHashes) == 0 {
		log.Errorf("block hashes is empty for file :%s", fileHashStr)
		return errors.New("block hashes is empty")
	}
	err := client.P2pConnect(addr)
	if err != nil {
		return err
	}
	this.taskMgr.NewTask(fileHashStr, this.WalletAddress(), task.TaskTypeDownload)
	defer this.taskMgr.DeleteTask(taskKey)
	resp, err := this.taskMgr.TaskBlockResp(taskKey)
	if err != nil {
		return err
	}
	for index, hash := range blockHashes {
		if this.taskMgr.IsBlockDownloaded(taskKey, hash, uint32(index)) {
			continue
		}
		value, err := this.downloadBlock(fileHashStr, hash, int32(index), addr, resp)
		if err != nil {
			return err
		}
		err = this.Fs.PutBlock(this.Fs.EncodedToBlockWithCid(value.Block, hash))
		if err != nil {
			return err
		}
		err = this.Fs.PutTag(hash, fileHashStr, uint64(value.Index), value.Tag)
		if err != nil {
			return err
		}
		log.Debugf("SetBlockDownloaded %s-%s-%d-%d", fileHashStr, value.Hash, index, value.Offset)
		this.taskMgr.SetBlockDownloaded(taskKey, value.Hash, value.PeerAddr, uint32(index), value.Offset, nil)
	}
	if !this.taskMgr.IsFileDownloaded(taskKey) {
		return errors.New("all blocks have sent but file not be stored")
	}
	err = this.Fs.PinRoot(context.TODO(), fileHashStr)
	if err != nil {
		log.Errorf("pin root file err %s", err)
		return err
	}
	this.PushToTrackers(fileHashStr, this.TrackerUrls, client.P2pGetPublicAddr())
	log.Infof("received all block, start pdp verify")

	// all block is saved, prove it
	err = this.Fs.StartPDPVerify(fileHashStr, 0, 0, 0, chainCom.ADDRESS_EMPTY)
	if err != nil {
		log.Errorf("start pdp verify err %s", err)
		return err
	}
	// TODO: remove unused file info fields after prove pdp success
	return nil
}

// downloadBlock. download block helper function.
func (this *Dsp) downloadBlock(fileHashStr, hash string, index int32, addr interface{}, resp chan *task.BlockResp) (*task.BlockResp, error) {
	var walletAddress string
	if this.Chain.Native.Fs.DefAcc != nil {
		walletAddress = this.Chain.Native.Fs.DefAcc.Address.ToBase58()
	} else {
		walletAddress = this.Chain.Native.Channel.DefAcc.Address.ToBase58()
	}
	msg := message.NewBlockReqMsg(fileHashStr, hash, index, walletAddress, common.ASSET_USDT)
	err := client.P2pSend(addr, msg.ToProtoMsg())
	if err != nil {
		return nil, err
	}
	received := false
	timeout := false
	select {
	case value, ok := <-resp:
		received = true
		if timeout {
			return nil, fmt.Errorf("receiving block %s timeout", hash)
		}
		if !ok {
			return nil, fmt.Errorf("receiving block channel %s error", hash)
		}
		return value, nil
	case <-time.After(time.Duration(common.BLOCK_FETCH_TIMEOUT) * time.Second):
		if received {
			break
		}
		timeout = true
		return nil, fmt.Errorf("receiving block %s timeout", hash)
	}
	return nil, errors.New("no receive channel")
}

// deleteFile. delete file from fs.
func (this *Dsp) deleteFile(fileHashStr string) error {
	err := this.Fs.DeleteFile(fileHashStr)
	if err != nil {
		return err
	}
	log.Debugf("delete file success")
	taskKey := this.taskMgr.TaskKey(fileHashStr, this.WalletAddress(), task.TaskTypeDownload)
	return this.taskMgr.DeleteFileDownloadInfo(taskKey)
}

// shareUploadedFile. share uploaded file when upload success
func (this *Dsp) shareUploadedFile(filePath, fileName, prefix string, blockHashes []string, root ipld.Node, list []*helpers.UnixfsNode) error {
	log.Debugf("shareUploadedFile path: %s filename:%s prefix:%s hashes:%d", filePath, fileName, prefix, blockHashes)
	if this.Config.FsType != config.FS_FILESTORE {
		return errors.New("fs type is not file store")
	}
	fileHashStr := root.Cid().String()
	taskKey := this.taskMgr.TaskKey(fileHashStr, this.WalletAddress(), task.TaskTypeDownload)
	if this.taskMgr.IsDownloadInfoExist(taskKey) {
		return nil
	}
	err := this.taskMgr.AddFileBlockHashes(taskKey, blockHashes)
	if err != nil {
		return err
	}
	err = this.taskMgr.AddFilePrefix(taskKey, prefix)
	if err != nil {
		return err
	}
	input, err := os.Open(filePath)
	if err != nil {
		return err
	}
	fullFilePath := this.Config.FsFileRoot + "/" + fileHashStr
	output, err := os.OpenFile(fullFilePath, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0666)
	if err != nil {
		return err
	}
	defer output.Close()
	n, err := io.Copy(output, input)
	log.Debugf("copy %d bytes", n)
	if err != nil {
		return err
	}

	blockOffset := int64(0)
	this.Fs.SetFsFilePrefix(fullFilePath, prefix)
	if !this.taskMgr.IsBlockDownloaded(taskKey, fileHashStr, uint32(0)) {
		log.Debugf("%s-%s-%d is downloaded", fileHashStr, fileHashStr, 0)
		links, err := this.Fs.BlockLinks(root)
		if err != nil {
			return err
		}
		log.Debugf("links count %d", len(links))
		err = this.Fs.PutBlockForFileStore(fullFilePath, root, uint64(0))
		if err != nil {
			log.Errorf("put block err %s", err)
			return err
		}
		err = this.taskMgr.SetBlockDownloaded(taskKey, fileHashStr, client.P2pGetPublicAddr(), uint32(0), 0, links)
		if err != nil {
			return err
		}
		log.Debugf("%s-%s-%d set downloaded", fileHashStr, 0, 0)
		blockDecodedData, err := this.Fs.BlockToBytes(root)
		if err != nil {
			return err
		}
		dataLen := int64(len(blockDecodedData))
		blockOffset += dataLen
	}
	for index, dagBlock := range list {
		blockIndex := index + 1
		block, err := dagBlock.GetDagNode()
		if err != nil {
			return err
		}
		blockHash := block.Cid().String()
		// send block
		blockDecodedData, err := this.Fs.BlockToBytes(block)
		if err != nil {
			return err
		}
		dataLen := int64(len(blockDecodedData))
		if this.taskMgr.IsBlockDownloaded(taskKey, blockHash, uint32(blockIndex)) {
			log.Debugf("%s-%s-%d is downloaded", fileHashStr, blockHash, blockIndex)
			continue
		}
		links := make([]string, 0, len(block.Links()))
		for _, l := range block.Links() {
			links = append(links, l.Cid.String())
		}
		err = this.Fs.PutBlockForFileStore(fullFilePath, block, uint64(blockOffset))
		log.Debugf("put block for file %s block: %s, offset:%d", fullFilePath, block.Cid(), blockOffset)
		if err != nil {
			log.Errorf("put block err %s", err)
			return err
		}
		err = this.taskMgr.SetBlockDownloaded(taskKey, blockHash, client.P2pGetPublicAddr(), uint32(blockIndex), blockOffset, links)
		if err != nil {
			return err
		}
		log.Debugf("%s-%s-%d set downloaded", fileHashStr, blockHash, blockIndex)
		blockOffset += dataLen
	}
	go this.PushToTrackers(fileHashStr, this.TrackerUrls, client.P2pGetPublicAddr())
	return nil
}

// uploadOptValid check upload opt valid
func uploadOptValid(filePath string, opt *common.UploadOption) error {
	if !common.FileExisted(filePath) {
		return errors.New("file not exist")
	}
	if opt.Encrypt && len(opt.EncryptPassword) == 0 {
		return errors.New("encrypt password is missing")
	}
	return nil
}

// createDownloadFile. create file handler for write downloading file
func createDownloadFile(dir, fileName string) (*os.File, error) {
	// if _, err := os.Stat(common.DOWNLOAD_FILE_TEMP_DIR_PATH); os.IsNotExist(err) {
	// 	err = os.MkdirAll(common.DOWNLOAD_FILE_TEMP_DIR_PATH, 0755)
	// 	if err != nil {
	// 		return nil, err
	// 	}
	// }
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		err = os.MkdirAll(dir, 0755)
		if err != nil {
			return nil, err
		}
	}
	// file, err := os.OpenFile(common.DOWNLOAD_FILE_TEMP_DIR_PATH+"/"+fileName, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0666)
	file, err := os.OpenFile(dir+"/"+fileName, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}
	return file, nil
}

// removeTempFile. remove the temp download file
func removeTempFile(fileName string) {
	// os.Remove(common.DOWNLOAD_FILE_TEMP_DIR_PATH + "/" + fileName)
}
