package dsp

import (
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"os"
	"time"

	"github.com/oniio/dsp-go-sdk/common"
	"github.com/oniio/dsp-go-sdk/network/message"
	"github.com/oniio/dsp-go-sdk/network/message/types/file"
	"github.com/oniio/dsp-go-sdk/task"
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
			log.Debugf("send block success %s, index:%d, taglen:%d, add uploaded block to db", reqInfo.Hash, reqInfo.Index, len(tag))
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
	return &common.UploadResult{
		Tx:       tx,
		FileHash: fileHashStr,
		Link:     "oni://" + fileHashStr + "&tr=",
	}, nil
}

// DeleteUploadedFile. delete uploaded file from the owner
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
	txHash, err := this.Chain.Native.Fs.DeleteFile(fileHashStr)
	if err != nil {
		return "", err
	}
	log.Debugf("delete file txHash %s", hex.EncodeToString(chainCom.ToArrayReverse(txHash)))
	confirmed, err := this.Chain.PollForTxConfirmed(time.Duration(common.TX_CONFIRM_TIMEOUT)*time.Second, txHash)
	if err != nil || !confirmed {
		return "", errors.New("wait for tx confirmed failed")
	}
	storingNode, _ := this.getFileProveNode(fileHashStr, info.ChallengeTimes)
	if len(storingNode) == 0 {
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
			blk := this.Fs.GetBlock(req.Hash)
			blockData := this.Fs.BlockDataOfAny(blk)
			if len(blockData) == 0 {
				log.Errorf("get block data empty %s", req.Hash)
				continue
			}
			offset, err := this.taskMgr.BlockOffset(req.FileHash, req.Hash, uint32(req.Index))
			if err != nil {
				log.Errorf("get block offset err%s", err)
				continue
			}
			msg := message.NewBlockMsg(req.Index, req.FileHash, req.Hash, blockData, nil, int64(offset))
			this.Network.Send(msg, req.PeerAddr)
		}
	}
}

// DownloadFile. download file, peice by peice from addrs.
// inOrder: if true, the file will be downloaded block by block in order
func (this *Dsp) DownloadFile(fileHashStr string, inOrder bool, addrs []string, decryptPwd string) error {
	msg := message.NewFileDownload(fileHashStr, this.Chain.Native.Fs.DefAcc.Address.ToBase58(), 0, 0)
	peers := make([]string, 0)
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
		blockHashes = fileMsg.BlockHashes
		peers = append(peers, addr)
		prefix = fileMsg.Prefix
	}
	err := this.Network.Broadcast(addrs, msg, true, nil, reply)
	if err != nil {
		return err
	}
	if len(peers) == 0 {
		return errors.New("no peer for download")
	}
	log.Debugf("filehashstr:%v, blockhashes-len:%v, prefix:%v\n", fileHashStr, len(blockHashes), prefix)
	err = this.taskMgr.AddFileBlockHashes(fileHashStr, blockHashes)
	if err != nil {
		return err
	}
	err = this.taskMgr.AddFilePrefix(fileHashStr, prefix)
	if err != nil {
		return err
	}
	err = createTempDir()
	if err != nil {
		return err
	}
	err = createDownloadDir(this.Config.FsFileRoot)
	if err != nil {
		return err
	}

	file, err := os.OpenFile(common.DOWNLOAD_FILE_TEMP_DIR_PATH+"/"+fileHashStr, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0666)
	if err != nil {
		return err
	}
	defer file.Close()
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
			return errors.New("no undownloaded block")
		}
		blockIndex := int32(index)
		err = this.taskMgr.AddBlockReq(fileHashStr, hash, blockIndex)
		if err != nil {
			return err
		}
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
			err = this.taskMgr.SetBlockDownloaded(fileHashStr, value.Hash, value.PeerAddr, uint32(value.Index), value.Offset, links)
			if err != nil {
				return err
			}
			err = this.Fs.PutBlock(block)
			if err != nil {
				log.Errorf("put block err %s", err)
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
			data := this.Fs.BlockData(block)
			// Test: performance
			fileStat, err := file.Stat()
			if err != nil {
				return err
			}
			// cut prefix
			if fileStat.Size() == 0 && len(data) >= len(prefix) && string(data[:len(prefix)]) == prefix {
				data = data[len(prefix):]
				log.Debugf("cut prefix len:%d", len(data))
			}
			_, err = file.Write(data)
			if err != nil {
				return err
			}
			if value.Index != blockIndex {
				continue
			}
			// last block
			this.taskMgr.SetTaskDone(fileHashStr, true)
			break
		}
		if len(decryptPwd) > 0 {
			// TODO: decrypt file
		}
		return os.Rename(common.DOWNLOAD_FILE_TEMP_DIR_PATH+"/"+fileHashStr, this.Config.FsFileRoot+"/"+fileHashStr)
	}
	// TODO: support out-of-order download
	return nil
}

func (this *Dsp) StartBackupFileService() {
	// wait for init contractRequest
	ticker := time.NewTicker(time.Duration(common.BACKUP_FILE_DURATION) * time.Second)
	for {
		select {
		case <-ticker.C:
			if this.Chain == nil {
				break
			}
			tasks, err := this.Chain.Native.Fs.GetExpiredProveList()
			if err != nil {
				break
			}
			if tasks == nil || len(tasks.Tasks) == 0 {
				log.Debugf("expired tasks len:%d", len(tasks.Tasks))
				break
			}
			cnt := 0
			for _, t := range tasks.Tasks {
				if t.LuckyAddr.ToBase58() != this.Chain.Native.Fs.DefAcc.Address.ToBase58() {
					log.Debugf("get expired list err: lucky node is %s not me", t.LuckyAddr.ToBase58())
					continue
				}
				if t.BackUpAddr.ToBase58() == this.Chain.Native.Fs.DefAcc.Address.ToBase58() {
					log.Debug("get expired list err: backup to self")
					continue
				}
				if t.BrokenAddr.ToBase58() == this.Chain.Native.Fs.DefAcc.Address.ToBase58() {
					log.Debug("get expired list err: me is offline")
					continue
				}
				log.Debugf("broken:%s, backupsvr:%s", t.BrokenAddr.ToBase58(), t.BackUpAddr.ToBase58())
				if t.BrokenAddr.ToBase58() == t.BackUpAddr.ToBase58() {
					log.Debug("get expired list err: backup from a bad node")
					continue
				}
				if len(t.FileHash) == 0 || len(t.BakSrvAddr) == 0 || len(t.BackUpAddr.ToBase58()) == 0 {
					log.Debugf("get expired prove list params invalid:%v", t)
					continue
				}
				log.Debugf("go backup file:%s, from:%s %s", t.FileHash, t.BakSrvAddr, t.BackUpAddr.ToBase58())
				cnt++
				if cnt >= common.MAX_EXPIRED_PROVE_TASK_NUM {
					break
				}
				opt := &task.BackupFileOpt{
					LuckyNum:   t.LuckyNum,
					BakNum:     t.BakNum,
					BakHeight:  t.BakHeight,
					BackUpAddr: t.BackUpAddr.ToBase58(),
					BrokenAddr: t.BrokenAddr.ToBase58(),
				}
				this.taskMgr.NewTask(string(t.FileHash), task.TaskTypeBackup)
				this.taskMgr.SetBackupOpt(string(t.FileHash), opt)
				// TODO:
				// err := bs.backupFileFromNode(context.TODO(), string(t.FileHash), string(t.BakSrvAddr), t.LuckyNum, t.BakHeight, t.BakNum, t.BackUpAddr, t.BrokenAddr)
				// if err != nil {
				// 	log.Errorf("back up file:%s failed:%s", t.FileHash, err)
				// 	continue
				// }
				log.Debugf("backup file:%s success", t.FileHash)
			}
		}
	}
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
		log.Debugf("prove times %d, challenge times %d", detail.ProveTimes, challengeTimes)
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
	// TODO: push to tracker
	log.Infof("received all block, start pdp verify")
	// all block is saved, prove it
	err = this.Fs.StartPDPVerify(fileHashStr, 0, 0, 0)
	if err != nil {
		log.Errorf("start pdp verify err %s", err)
		return err
	}
	// TODO: remove unused file info fields after prove pdp success
	return nil
}

// downloadBlock. download block helper function.
func (this *Dsp) downloadBlock(fileHashStr, hash string, index int32, addr interface{}, resp chan *task.BlockResp) (*task.BlockResp, error) {
	msg := message.NewBlockReqMsg(fileHashStr, hash, index)
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

func createTempDir() error {
	if _, err := os.Stat(common.DOWNLOAD_FILE_TEMP_DIR_PATH); os.IsNotExist(err) {
		return os.MkdirAll(common.DOWNLOAD_FILE_TEMP_DIR_PATH, 0755)
	}
	return nil
}

func createDownloadDir(path string) error {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return os.MkdirAll(path, 0755)
	}
	return nil
}
