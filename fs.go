package dsp

import (
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"os"
	"time"

	"github.com/oniio/dsp-go-sdk/common"
	"github.com/oniio/dsp-go-sdk/config"
	netcom "github.com/oniio/dsp-go-sdk/network/common"
	"github.com/oniio/dsp-go-sdk/network/message"
	"github.com/oniio/dsp-go-sdk/network/message/types/file"
	"github.com/oniio/dsp-go-sdk/task"
	"github.com/oniio/dsp-go-sdk/utils"
	chainCom "github.com/oniio/oniChain/common"
	"github.com/oniio/oniChain/common/log"
	"github.com/oniio/oniChain/crypto/pdp"
	fs "github.com/oniio/oniChain/smartcontract/service/native/onifs"
)

// RegisterNode. register node to chain
func (this *Dsp) RegisterNode(addr string, volume, serviceTime uint64) (string, error) {
	txHash, err := this.Chain.Native.Fs.NodeRegister(volume, serviceTime, addr)
	if err != nil {
		return "", err
	}
	tx := hex.EncodeToString(chainCom.ToArrayReverse(txHash))
	return tx, nil
}

// UnregisterNode. unregister node to chain
func (this *Dsp) UnregisterNode() (string, error) {
	txHash, err := this.Chain.Native.Fs.NodeCancel()
	if err != nil {
		return "", err
	}
	tx := hex.EncodeToString(chainCom.ToArrayReverse(txHash))
	return tx, nil
}

// QueryNode. query node information by wallet address
func (this *Dsp) QueryNode(walletAddr string) (*fs.FsNodeInfo, error) {
	address, err := chainCom.AddressFromBase58(walletAddr)
	if err != nil {
		return nil, err
	}
	return this.Chain.Native.Fs.NodeQuery(address)
}

// UpdateNode. update node information
func (this *Dsp) UpdateNode(addr string, volume, serviceTime uint64) (string, error) {
	nodeInfo, err := this.QueryNode(this.Chain.Native.Fs.DefAcc.Address.ToBase58())
	if err != nil {
		return "", err
	}
	if volume == 0 {
		volume = nodeInfo.Volume
	}
	if volume < nodeInfo.Volume-nodeInfo.RestVol {
		return "", fmt.Errorf("volume %d is less than original volume %d - restvol %d", volume, nodeInfo.Volume, nodeInfo.RestVol)
	}
	if serviceTime == 0 {
		serviceTime = nodeInfo.ServiceTime
	}
	if len(addr) == 0 {
		addr = string(nodeInfo.NodeAddr)
	}
	txHash, err := this.Chain.Native.Fs.NodeUpdate(volume, serviceTime, addr)
	if err != nil {
		return "", err
	}
	tx := hex.EncodeToString(chainCom.ToArrayReverse(txHash))
	return tx, nil
}

// RegisterNode. register node to chain
func (this *Dsp) NodeWithdrawProfit() (string, error) {
	txHash, err := this.Chain.Native.Fs.NodeWithDrawProfit()
	if err != nil {
		return "", err
	}
	tx := hex.EncodeToString(chainCom.ToArrayReverse(txHash))
	return tx, nil
}

// UploadFile upload file logic
func (this *Dsp) UploadFile(filePath string, opt *common.UploadOption) (*common.UploadResult, error) {
	if err := uploadOptValid(filePath, opt); err != nil {
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
	if this.taskMgr.TaskExist(fileHashStr) {
		return nil, errors.New("task is exist")
	}
	log.Debugf("root:%s, list.len:%d", fileHashStr, len(list))
	// get nodeList
	nodeList, err := this.getUploadNodeList(filePath, fileHashStr)
	if err != nil {
		return nil, err
	}
	if uint32(len(nodeList)) < opt.CopyNum+1 {
		return nil, fmt.Errorf("node is not enough %d, copyNum %d", len(nodeList), opt.CopyNum)
	}
	totalCount = uint64(len(list) + 1)

	// pay file
	payRet, err := this.payForSendFile(filePath, fileHashStr, uint64(len(list)+1), opt)
	if err != nil {
		return nil, err
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
	this.taskMgr.NewTask(fileHashStr, task.TaskTypeUpload)
	// delete task from cache in the end
	defer this.taskMgr.DeleteTask(fileHashStr)
	this.taskMgr.SetFileName(fileHashStr, opt.FileDesc)
	this.taskMgr.SetFileBlocksTotalCount(fileHashStr, uint64(len(list)+1))
	go this.taskMgr.EmitProgress(fileHashStr)
	receivers, err := this.waitFileReceivers(fileHashStr, nodeList, hashes, int(opt.CopyNum)+1)
	if err != nil {
		return nil, err
	}
	log.Debugf("receivers:%v", receivers)
	if len(receivers) < int(opt.CopyNum)+1 {
		return nil, errors.New("file receivers is not enough")
	}
	// notify fetch ready to all receivers
	err = this.notifyFetchReady(fileHashStr, receivers)
	if err != nil {
		return nil, err
	}

	req, err := this.taskMgr.TaskBlockReq(fileHashStr)
	if err != nil {
		return nil, err
	}
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
			isStored := this.taskMgr.IsBlockUploaded(fileHashStr, reqInfo.Hash, reqInfo.PeerAddr, uint32(reqInfo.Index))
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
			err = this.Network.Send(msg, reqInfo.PeerAddr)
			if err != nil {
				log.Errorf("send block msg hash %s to peer %s failed, err %s", reqInfo.Hash, reqInfo.PeerAddr, err)
			}
			log.Debugf("send block success %s, index:%d, taglen:%d, offset:%d add uploaded block to db", reqInfo.Hash, reqInfo.Index, len(tag), offset-dataLen)
			// stored
			this.taskMgr.AddUploadedBlock(fileHashStr, reqInfo.Hash, reqInfo.PeerAddr, uint32(reqInfo.Index))
			// update progress
			go this.taskMgr.EmitProgress(fileHashStr)
			if len(this.taskMgr.GetUploadedBlockNodeList(fileHashStr, reqInfo.Hash, uint32(reqInfo.Index))) < int(opt.CopyNum)+1 {
				break
			}
			sent := uint64(this.taskMgr.UploadedBlockCount(fileHashStr))
			if totalCount == sent {
				log.Infof("all block has sent")
				finish = true
				break
			}
		case <-timeout.C:
			return nil, errors.New("wait for fetch block timeout")
		}
		if finish {
			break
		}
	}
	log.Debugf("checking file proved")
	proved := this.checkFileBeProved(fileHashStr, opt.ProveTimes, opt.CopyNum)
	log.Debugf("checking file proved done %t", proved)
	if !proved {
		return nil, errors.New("file has sent, but no enought prove is finished")
	}
	err = this.taskMgr.DeleteFileUploadInfo(fileHashStr)
	if err != nil {
		log.Errorf("delete file upload info err: %s", err)
	}

	oniLink := utils.GenOniLink(fileHashStr, opt.FileDesc, 0, totalCount, this.Config.TrackerUrls)
	var dnsRegTx, dnsBindTx string
	if opt.RegisterDns && len(opt.DnsUrl) > 0 {
		dnsRegTx, err = this.RegisterFileUrl(opt.DnsUrl, oniLink)
	}
	if opt.BindDns && len(opt.DnsUrl) > 0 {
		dnsBindTx, _ = this.BindFileUrl(opt.DnsUrl, oniLink)
	}

	return &common.UploadResult{
		Tx:            tx,
		FileHash:      fileHashStr,
		Link:          oniLink,
		RegisterDnsTx: dnsRegTx,
		BindDnsTx:     dnsBindTx,
	}, nil
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
	if len(storingNode) == 0 {
		// find breakpoint keep nodelist
		storingNode = append(storingNode, this.taskMgr.GetUploadedBlockNodeList(fileHashStr, fileHashStr, 0)...)
	}
	msg := message.NewFileDelete(fileHashStr, this.Chain.Native.Fs.DefAcc.Address.ToBase58())
	err = this.Network.Broadcast(storingNode, msg, true, nil, nil)
	if err != nil {
		return "", err
	}
	err = this.taskMgr.DeleteFileUploadInfo(fileHashStr)
	if err != nil {
		log.Errorf("delete upload info from db err: %s", err)
	}
	return hex.EncodeToString(chainCom.ToArrayReverse(txHash)), nil
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
			// check if has unpaid block request
			canShare, err := this.taskMgr.CanShareTo(req.FileHash, req.WalletAddress)
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
			offset, err := this.taskMgr.BlockOffset(req.FileHash, req.Hash, uint32(req.Index))
			if err != nil {
				log.Errorf("get block offset err%s", err)
				break
			}
			log.Debugf("share block %s, index:%d,  offset %d", req.Hash, req.Index, offset)
			// TODO: only send tag with tagflag enabled
			// TEST: client get tag
			tag, _ := this.Fs.GetTag(req.Hash, req.FileHash, uint64(req.Index))
			msg := message.NewBlockMsg(req.Index, req.FileHash, req.Hash, blockData, tag, int64(offset))
			this.Network.Send(msg, req.PeerAddr)
			up, err := this.GetFileUnitPrice(req.FileHash, req.Asset)
			if err != nil {
				log.Errorf("get file unit price err after send block, err: %s", err)
				break
			}
			// add new unpaid block request to store
			this.taskMgr.AddShareFileUnpaid(req.FileHash, req.WalletAddress, req.Asset, uint64(len(blockData))*up)
		}
	}
}

// DownloadFile. download file, peice by peice from addrs.
// inOrder: if true, the file will be downloaded block by block in order
// free: if true, query nodes who can share file free
// maxPeeCnt: download with max number peers with min price
func (this *Dsp) DownloadFile(fileHashStr string, asset int32, inOrder bool, decryptPwd string, free bool, maxPeerCnt int) error {
	addrs := this.GetPeerFromTracker(fileHashStr, this.Config.TrackerUrls)
	return this.downloadFileFromPeers(fileHashStr, asset, inOrder, decryptPwd, free, maxPeerCnt, addrs)
}

// DownloadFileByLink. download file by link, e.g oni://Qm...&name=xxx&tr=xxx
func (this *Dsp) DownloadFileByLink(link string, asset int32, inOrder bool, decryptPwd string, free bool, maxPeerCnt int) error {
	fileHashStr := utils.GetFileHashFromLink(link)
	return this.DownloadFile(fileHashStr, asset, inOrder, decryptPwd, free, maxPeerCnt)
}

// DownloadFileByUrl. download file by link, e.g dsp://file1
func (this *Dsp) DownloadFileByUrl(url string, asset int32, inOrder bool, decryptPwd string, free bool, maxPeerCnt int) error {
	fileHashStr := this.GetFileHashFromUrl(url)
	return this.DownloadFile(fileHashStr, asset, inOrder, decryptPwd, free, maxPeerCnt)
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
	reply := func(msg *message.Message, addr string) {
		if msg.Error != nil {
			return
		}
		fileMsg := msg.Payload.(*file.File)
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
	err := this.Network.Broadcast(addrs, msg, true, nil, reply)
	if err != nil {
		log.Errorf("file download err %s", err)
	}
	if len(peerPayInfos) == 0 {
		return nil, errors.New("no peer for download")
	}
	log.Debugf("peer prices:%v", peerPayInfos)
	if this.taskMgr.IsDownloadInfoExist(fileHashStr) {
		return peerPayInfos, nil
	}
	err = this.taskMgr.AddFileBlockHashes(fileHashStr, blockHashes)
	if err != nil {
		return nil, err
	}
	err = this.taskMgr.AddFilePrefix(fileHashStr, prefix)
	if err != nil {
		return nil, err
	}
	return peerPayInfos, nil
}

// SetupChannel. open and deposit channel. peerPrices: peer network address <=> payment info
func (this *Dsp) SetupChannel(fileHashStr string, peerPrices map[string]*file.Payment) error {
	if !this.taskMgr.IsDownloadInfoExist(fileHashStr) {
		return errors.New("download info not exist")
	}
	if len(peerPrices) == 0 {
		return errors.New("no peers to open channel")
	}
	blockHashes := this.taskMgr.FileBlockHashes(fileHashStr)
	if len(blockHashes) == 0 {
		return errors.New("no blocks")
	}
	// TODO: optimize to parallel operation
	for _, info := range peerPrices {
		id, err := this.Channel.OpenChannel(info.WalletAddress)
		log.Debugf("channel id %d", id)
		if err != nil {
			return err
		}
		if info.UnitPrice == 0 {
			err = this.Channel.WaitForConnected(info.WalletAddress, time.Duration(common.WAIT_CHANNEL_CONNECT_TIMEOUT)*time.Second)
			if err != nil {
				return err
			}
			continue
		}
		totalAmount := info.UnitPrice * uint64(len(blockHashes)) * uint64(common.CHUNK_SIZE)
		log.Debugf("deposit to %s price:%d, cnt:%d, chunsize:%d, total:%d", info.WalletAddress, info.UnitPrice, len(blockHashes), common.CHUNK_SIZE, totalAmount)
		if totalAmount/info.UnitPrice != uint64(len(blockHashes))*uint64(common.CHUNK_SIZE) {
			return errors.New("deposit amount overflow")
		}
		log.Debugf("depositing...")
		err = this.Channel.SetDeposit(info.WalletAddress, totalAmount)
		log.Debugf("deposit result %s", err)
		if err != nil {
			return err
		}
		log.Debugf("waiting for connected")
		err = this.Channel.WaitForConnected(info.WalletAddress, time.Duration(common.WAIT_CHANNEL_CONNECT_TIMEOUT)*time.Second)
		log.Debugf("waiting for connected duccess")
		if err != nil {
			return err
		}
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
	err := this.taskMgr.AddDownloadFileUnpaid(fileHashStr, payInfo.WalletAddress, payInfo.Asset, amount)
	if err != nil {
		return 0, err
	}
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	paymentId := r.Int31()
	err = this.Channel.DirectTransfer(paymentId, amount, payInfo.WalletAddress)
	if err != nil {
		return 0, err
	}
	log.Debugf("payment id %d, for file:%s, size:%d, price:%d", paymentId, fileHashStr, blockSize, amount)
	// send payment msg
	msg := message.NewPayment(this.Chain.Native.Fs.DefAcc.Address.ToBase58(), payInfo.WalletAddress, paymentId,
		payInfo.Asset, amount, fileHashStr, netcom.MSG_ERROR_CODE_NONE)
	_, err = this.Network.Request(msg, addr)
	log.Debugf("payment msg response :%d, err:%s", paymentId, err)
	if err != nil {
		return 0, err
	}
	// clean unpaid order
	err = this.taskMgr.DeleteDownloadFileUnpaid(fileHashStr, payInfo.WalletAddress, payInfo.Asset, amount)
	if err != nil {
		return 0, err
	}
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
	if this.taskMgr.TaskExist(fileHashStr) {
		return errors.New("task is exist")
	}
	if len(quotation) == 0 {
		return errors.New("no peer for download")
	}
	if !this.taskMgr.IsDownloadInfoExist(fileHashStr) {
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
	err = this.Network.Broadcast(addrs, msg, true, nil, nil)
	if err != nil {
		return err
	}
	blockHashes := this.taskMgr.FileBlockHashes(fileHashStr)
	prefix := this.taskMgr.FilePrefix(fileHashStr)
	log.Debugf("filehashstr:%v, blockhashes-len:%v, prefix:%v\n", fileHashStr, len(blockHashes), prefix)
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

	// start a task
	this.taskMgr.NewTask(fileHashStr, task.TaskTypeDownload)
	defer this.taskMgr.DeleteTask(fileHashStr)
	this.taskMgr.SetFileBlocksTotalCount(fileHashStr, uint64(len(blockHashes)))
	// declare job for workers
	job := func(fHash, bHash, pAddr string, index int32, respCh chan *task.BlockResp) (*task.BlockResp, error) {
		log.Debugf("download %s-%s-%d from %s", fHash, bHash, index, pAddr)
		return this.downloadBlock(fHash, bHash, index, pAddr, respCh)
	}
	this.taskMgr.NewWorkers(fileHashStr, addrs, inOrder, job)
	go this.taskMgr.WorkBackground(fileHashStr)
	if inOrder {
		hash, index, err := this.taskMgr.GetUndownloadedBlockInfo(fileHashStr, fileHashStr)
		if err != nil {
			return err
		}
		if len(hash) == 0 {
			removeTempFile(fileHashStr)
			return errors.New("no undownloaded block")
		}
		blockIndex := int32(index)
		err = this.taskMgr.AddBlockReq(fileHashStr, hash, blockIndex)
		if err != nil {
			return err
		}
		fullFilePath := this.Config.FsFileRoot + "/" + fileHashStr
		for {
			value, ok := <-this.taskMgr.TaskNotify(fileHashStr)
			if !ok {
				return errors.New("download internal error")
			}
			if this.taskMgr.IsBlockDownloaded(fileHashStr, value.Hash, uint32(value.Index)) {
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
				// Test: performance
				fileStat, err := file.Stat()
				if err != nil {
					return err
				}
				// cut prefix
				if fileStat.Size() == 0 && len(data) >= len(prefix) && string(data[:len(prefix)]) == prefix {
					data = data[len(prefix):]
				}
				// TODO: write at offset
				_, err = file.Write(data)
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
			err = this.taskMgr.SetBlockDownloaded(fileHashStr, value.Hash, value.PeerAddr, uint32(value.Index), value.Offset, links)
			if err != nil {
				return err
			}
			go this.taskMgr.EmitProgress(fileHashStr)
			log.Debugf("%s-%s-%d set downloaded", fileHashStr, value.Hash, value.Index)
			for _, l := range links {
				blockIndex++
				err := this.taskMgr.AddBlockReq(fileHashStr, l, blockIndex)
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
			this.taskMgr.SetTaskDone(fileHashStr, true)
			break
		}
		this.PushToTrackers(fileHashStr, this.Config.TrackerUrls, this.Network.ListenAddr())
		if len(decryptPwd) > 0 {
			return this.Fs.AESDecryptFile(fullFilePath, decryptPwd, fullFilePath+"-decrypted")
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

// StartBackupFileService. start a backup file service to find backup jobs.
func (this *Dsp) StartBackupFileService() {
	backupingCnt := 0
	ticker := time.NewTicker(time.Duration(common.BACKUP_FILE_DURATION) * time.Second)
	for {
		log.Debugf("backup ticker")
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
					log.Debugf("address check faield, lucky: %s, backup: %s, broken: %s", t.LuckyAddr.ToBase58(), t.BackUpAddr.ToBase58(), t.BrokenAddr.ToBase58())
					continue
				}
				if len(t.FileHash) == 0 || len(t.BakSrvAddr) == 0 || len(t.BackUpAddr.ToBase58()) == 0 {
					log.Debugf("get expired prove list params invalid:%v", t)
					continue
				}
				backupingCnt++
				log.Debugf("go backup file:%s, from:%s %s", t.FileHash, t.BakSrvAddr, t.BackUpAddr.ToBase58())
				err := this.downloadFileFromPeers(string(t.FileHash), common.ASSET_ONG, true, "", false, common.MAX_DOWNLOAD_PEERS_NUM, []string{string(t.BakSrvAddr)})
				if err != nil {
					log.Errorf("download file err %s", err)
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
	err = this.SetupChannel(fileHashStr, quotation)
	if err != nil {
		return err
	}
	log.Debugf("set up channel success: %v\n", quotation)
	return this.DownloadFileWithQuotation(fileHashStr, asset, inOrder, quotation, decryptPwd)
}

// payForSendFile pay before send a file
// return PoR params byte slice, PoR private key of the file or error
func (this *Dsp) payForSendFile(filePath, fileHashStr string, blockNum uint64, opt *common.UploadOption) (*common.PayStoreFileReulst, error) {
	fileInfo, _ := this.Chain.Native.Fs.GetFileInfo(fileHashStr)
	var paramsBuf, privateKey []byte
	var tx string
	if fileInfo == nil {
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
		err = this.taskMgr.PutFileUploadInfo(tx, fileHashStr, privateKey)
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
		if uint64(this.taskMgr.UploadedBlockCount(fileHashStr)) == blockNum {
			return nil, fmt.Errorf("has sent all blocks, waiting for ont-ipfs node commit proves")
		}
		paramsBuf = fileInfo.FileProveParam
		privateKey = this.taskMgr.GetFileProvePrivKey(fileHashStr)
		tx = this.taskMgr.GetStoreFileTx(fileHashStr)
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
func (this *Dsp) getUploadNodeList(filePath, fileHashStr string) ([]string, error) {
	fileStat, err := os.Stat(filePath)
	if err != nil {
		return nil, err
	}
	nodeList := this.taskMgr.GetUploadedBlockNodeList(fileHashStr, fileHashStr, 0)
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
		log.Debugf("time.Duration %d", timewait)
		time.Sleep(time.Duration(timewait) * time.Second)
	}
	return true
}

// waitFileReceivers find nodes and send file give msg, waiting for file fetch msg
// client -> fetch_ask -> peer.
// client <- fetch_ack <- peer.
// return receivers
func (this *Dsp) waitFileReceivers(fileHashStr string, nodeList, blockHashes []string, receiverCount int) ([]string, error) {
	receivers := make([]string, 0)
	msg := message.NewFileFetchAsk(fileHashStr, blockHashes, this.Chain.Native.Fs.DefAcc.Address.ToBase58(), this.Chain.Native.Fs.DefAcc.Address.ToBase58())
	action := func(res *message.Message, addr string) {
		log.Debugf("send file ask msg success %s", addr)
		// block waiting for ack msg or timeout msg
		isAck := false
		ack, err := this.taskMgr.TaskAck(fileHashStr)
		if err != nil {
			return
		}
		select {
		case <-ack:
			isAck = true
			log.Debugf("received ack from %s\n", addr)
			receivers = append(receivers, addr)
		case <-time.After(time.Duration(common.FILE_FETCH_ACK_TIMEOUT) * time.Second):
			if isAck {
				return
			}
			this.taskMgr.SetTaskTimeout(fileHashStr, true)
			return
		}
	}
	stop := func() bool {
		return len(receivers) >= receiverCount
	}
	err := this.Network.Broadcast(nodeList, msg, false, stop, action)
	if err != nil {
		log.Errorf("wait file receivers broadcast err")
		return nil, err
	}
	log.Debugf("receives :%v", receivers)
	return receivers, nil
}

// notifyFetchReady send fetch_rdy msg to receivers
func (this *Dsp) notifyFetchReady(fileHashStr string, receivers []string) error {
	msg := message.NewFileFetchRdy(fileHashStr)
	this.taskMgr.SetTaskReady(fileHashStr, true)
	err := this.Network.Broadcast(receivers, msg, false, nil, nil)
	if err != nil {
		log.Errorf("notify err %s", err)
		this.taskMgr.SetTaskReady(fileHashStr, false)
	}
	return err
}

// startFetchBlocks for store node, fetch blocks one by one after receive fetch_rdy msg
func (this *Dsp) startFetchBlocks(fileHashStr string, addr string) error {
	blockHashes := this.taskMgr.FileBlockHashes(fileHashStr)
	if len(blockHashes) == 0 {
		log.Errorf("block hashes is empty for file :%s", fileHashStr)
		return errors.New("block hashes is empty")
	}
	if !this.Network.IsConnectionExists(addr) {
		err := this.Network.Connect(addr)
		if err != nil {
			return err
		}
	}
	this.taskMgr.NewTask(fileHashStr, task.TaskTypeDownload)
	defer this.taskMgr.DeleteTask(fileHashStr)
	resp, err := this.taskMgr.TaskBlockResp(fileHashStr)
	if err != nil {
		return err
	}
	for index, hash := range blockHashes {
		if this.taskMgr.IsBlockDownloaded(fileHashStr, hash, uint32(index)) {
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
		this.taskMgr.SetBlockDownloaded(fileHashStr, value.Hash, value.PeerAddr, uint32(index), value.Offset, nil)
	}
	if !this.taskMgr.IsFileDownloaded(fileHashStr) {
		return errors.New("all blocks have sent but file not be stored")
	}
	this.PushToTrackers(fileHashStr, this.Config.TrackerUrls, this.Network.ListenAddr())
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
	msg := message.NewBlockReqMsg(fileHashStr, hash, index, walletAddress, common.ASSET_ONG)
	err := this.Network.Send(msg, addr)
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
	return this.taskMgr.DeleteFileDownloadInfo(fileHashStr)
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
	if _, err := os.Stat(common.DOWNLOAD_FILE_TEMP_DIR_PATH); os.IsNotExist(err) {
		err = os.MkdirAll(common.DOWNLOAD_FILE_TEMP_DIR_PATH, 0755)
		if err != nil {
			return nil, err
		}
	}
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
	os.Remove(common.DOWNLOAD_FILE_TEMP_DIR_PATH + "/" + fileName)
}
