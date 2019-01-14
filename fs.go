package dsp

import (
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"os"
	"sync"
	"time"

	"github.com/oniio/dsp-go-sdk/common"
	"github.com/oniio/dsp-go-sdk/network/message"
	chainCom "github.com/oniio/oniChain/common"
	"github.com/oniio/oniChain/common/log"
	"github.com/oniio/oniChain/crypto/pdp"
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

// UploadFile upload file logic
func (this *Dsp) UploadFile(filePath string, opt *common.UploadOption, progress chan *common.UploadingInfo) (*common.UploadResult, error) {
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
	go emitProgress(progress, opt.FileDesc, fileHashStr, totalCount, 0)

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
	this.taskMgr.NewTask(fileHashStr)
	// delete task from cache in the end
	defer func() {
		this.taskMgr.DeleteTask(fileHashStr)
	}()
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

	finish := false
	timeout := time.NewTimer(time.Duration(common.BLOCK_FETCH_TIMEOUT) * time.Second)
	for {
		select {
		case reqInfo := <-req:
			isStored := this.UserFileMgr.IsBlockStored(fileHashStr, reqInfo.Hash, reqInfo.PeerAddr, uint32(reqInfo.Index))
			if isStored {
				log.Debugf("block has stored %s", reqInfo.Hash)
				break
			}
			timeout.Reset(time.Duration(common.BLOCK_FETCH_TIMEOUT) * time.Second)
			// send block
			var blockData []byte
			if reqInfo.Hash == fileHashStr && reqInfo.Index == 0 {
				blockData = this.Fs.BlockDataOfAny(root)
			} else {
				unixNode := listMap[fmt.Sprintf("%s%d", reqInfo.Hash, reqInfo.Index)]
				blockData = this.Fs.BlockDataOfAny(unixNode)
			}
			if len(blockData) == 0 {
				log.Errorf("block is nil hash %s, peer %s failed, err %s", reqInfo.Hash, reqInfo.PeerAddr, err)
				break
			}
			tag, err := pdp.SignGenerate(blockData, fileID, uint32(reqInfo.Index)+1, g0, payRet.PrivateKey)
			if err != nil {
				log.Errorf("generate tag hash %s, peer %s failed, err %s", reqInfo.Hash, reqInfo.PeerAddr, err)
				break
			}
			msg := message.NewBlockMsg(int32(reqInfo.Index), fileHashStr, reqInfo.Hash, blockData, tag)
			err = this.Network.Send(msg, reqInfo.PeerAddr)
			if err != nil {
				log.Errorf("send block msg hash %s to peer %s failed, err %s", reqInfo.Hash, reqInfo.PeerAddr, err)
			}
			// stored
			this.UserFileMgr.AddStoredBlock(fileHashStr, reqInfo.Hash, reqInfo.PeerAddr, uint32(reqInfo.Index))
			// update progress
			if len(this.UserFileMgr.GetStoredBlockNodeList(fileHashStr, reqInfo.Hash)) < int(opt.CopyNum)+1 {
				break
			}
			sent := uint64(this.UserFileMgr.StoredBlockCount(fileHashStr))
			go emitProgress(progress, opt.FileDesc, fileHashStr, totalCount, sent)
			if totalCount == sent {
				// TODO check pdp prove
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
	proved := this.checkFileBeProved(fileHashStr, opt.ProveTimes, opt.CopyNum)
	if !proved {
		return nil, errors.New("file has sent, but no enought prove is finished")
	}
	return &common.UploadResult{
		Tx:       tx,
		FileHash: fileHashStr,
		Link:     "oni://" + fileHashStr + "&tr=",
	}, nil
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
		err = this.UserFileMgr.NewStoreFile(tx, fileHashStr, privateKey)
		if err != nil {
			return nil, err
		}
	} else {
		storing, stored := this.getFileStoreNodeCount(fileHashStr, fileInfo.ChallengeTimes)
		if storing > 0 {
			return nil, fmt.Errorf("file:%s has stored", fileHashStr)
		}
		if stored > 0 {
			return nil, fmt.Errorf("file:%s has expired, please delete it first", fileHashStr)
		}
		log.Debugf("has paid but not store")
		err := this.UserFileMgr.NewStoreFile("", fileHashStr, []byte{})
		if err != nil {
			return nil, err
		}
		if uint64(this.UserFileMgr.StoredBlockCount(fileHashStr)) == blockNum {
			return nil, fmt.Errorf("has sent all blocks, waiting for ont-ipfs node commit proves")
		}
		paramsBuf = fileInfo.FileProveParam
		privateKey = this.UserFileMgr.GetFileProvePrivKey(fileHashStr)
		tx = this.UserFileMgr.GetStoreFileTx(fileHashStr)
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

// getFileStoreNodeCount. get file storing nodes count and stored (expired) nodes count
func (this *Dsp) getFileStoreNodeCount(fileHashStr string, challengeTimes uint64) (uint32, uint32) {
	proveDetails, _ := this.Chain.Native.Fs.GetFileProveDetails(fileHashStr)
	storing, stored := uint32(0), uint32(0)
	if proveDetails != nil {
		for _, detail := range proveDetails.ProveDetails {
			if detail.ProveTimes < challengeTimes+1 {
				storing++
			} else {
				stored++
			}
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
	nodeList := this.UserFileMgr.GetStoredBlockNodeList(fileHashStr, fileHashStr)
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
		storing, _ := this.getFileStoreNodeCount(fileHashStr, uint64(proveTimes))
		if storing >= copyNum+1 {
			break
		}
		retry++
		time.Sleep(time.Duration(timewait) * time.Second)
	}
	return true
}

// waitFileReceivers find nodes and send file give msg, waiting for file fetch msg
// peer -> filegive
// peer <- filefetch
// return receivers
func (this *Dsp) waitFileReceivers(fileHashStr string, nodeList, blockHashes []string, receiverCount int) ([]string, error) {
	receivers := make([]string, 0)
	sent := 0
	wg := sync.WaitGroup{}
	// max go routines number in loop
	connsInLoop := common.MAX_GOROUTINES_IN_LOOP
	if receiverCount <= common.MAX_GOROUTINES_IN_LOOP {
		connsInLoop = receiverCount
	}
	//TEST: large amount of go routine case
	for _, addr := range nodeList {
		wg.Add(1)
		go func(to string) {
			if !this.Network.IsConnectionExists(to) {
				err := this.Network.Connect(to)
				if err != nil {
					log.Errorf("connect err:%s", err)
					sent--
					wg.Done()
					return
				}
			}
			msg := message.NewFileFetchAskMsg(fileHashStr, blockHashes, this.Chain.Native.Fs.DefAcc.Address.ToBase58())
			err := this.Network.Send(msg, to)
			log.Debugf("sent fetch ask msg: %v", msg)
			if err != nil {
				sent--
				wg.Done()
				return
			}
			// block waiting for ack msg or timeout msg
			isAck := false
			ack, err := this.taskMgr.TaskAck(fileHashStr)
			if err != nil {
				sent--
				wg.Done()
				return
			}
			select {
			case <-ack:
				isAck = true
				log.Debugf("received ack from %s\n", to)
				receivers = append(receivers, to)
				wg.Done()
			case <-time.After(time.Duration(common.FILE_FETCH_ACK_TIMEOUT) * time.Second):
				if isAck {
					return
				}
				this.taskMgr.SetTaskTimeout(fileHashStr, true)
				sent--
				wg.Done()
				return
			}
		}(addr)
		sent++
		if sent == connsInLoop {
			wg.Wait()
		}
		if len(receivers) >= receiverCount {
			break
		}
		sent = 0
		connsInLoop = common.MAX_GOROUTINES_IN_LOOP
		if receiverCount-len(receivers) <= common.MAX_GOROUTINES_IN_LOOP {
			connsInLoop = receiverCount - len(receivers)
		}
	}
	log.Debugf("receives :%v", receivers)
	return receivers, nil
}

// notifyFetchReady send fetch_rdy msg to receivers
func (this *Dsp) notifyFetchReady(fileHashStr string, receivers []string) error {
	wg := sync.WaitGroup{}
	connsInLoop := common.MAX_GOROUTINES_IN_LOOP
	if len(receivers) <= common.MAX_GOROUTINES_IN_LOOP {
		connsInLoop = len(receivers)
	}
	count := 0
	this.taskMgr.SetTaskReady(fileHashStr, true)
	var err error
	for _, addr := range receivers {
		wg.Add(1)
		go func(to string) {
			if !this.Network.IsConnectionExists(to) {
				err = this.Network.Connect(to)
				if err != nil {
					wg.Done()
					return
				}
			}
			msg := message.NewFileFetchRdyMsg(fileHashStr)
			err = this.Network.Send(msg, to)
			if err != nil {
				count--
				wg.Done()
				return
			}
			wg.Done()
		}(addr)
		count++
		if count >= connsInLoop {
			wg.Wait()
		}
		// reset
		connsInLoop = common.MAX_GOROUTINES_IN_LOOP
		if len(receivers)-count <= common.MAX_GOROUTINES_IN_LOOP {
			connsInLoop = len(receivers) - count
		}
		count = 0
	}
	if err != nil {
		log.Errorf("notify err %s", err)
		this.taskMgr.SetTaskReady(fileHashStr, false)
	}
	return err
}

// startFetchBlocks for store node, fetch blocks one by one after receive fetch_rdy msg
func (this *Dsp) startFetchBlocks(fileHashStr string, addr string) error {
	blockHashes := this.NodeFilemgr.FileBlockHashes(fileHashStr)
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
	this.taskMgr.NewTask(fileHashStr)
	resp, err := this.taskMgr.TaskBlockResp(fileHashStr)
	if err != nil {
		return err
	}
	for index, hash := range blockHashes {
		if this.NodeFilemgr.IsBlockStored(fileHashStr, hash, uint32(index)) {
			continue
		}
		msg := message.NewBlockReqMsg(fileHashStr, hash, int32(index))
		err := this.Network.Send(msg, addr)
		if err != nil {
			return err
		}
		received := false
		timeout := false
		// wait for block msg
		select {
		case value, ok := <-resp:
			if timeout {
				return fmt.Errorf("receiving block %s timeout", hash)
			}
			if !ok {
				return fmt.Errorf("receiving block channel %s error", hash)
			}
			// save block to fs
			err := this.Fs.PutBlock(value.Hash, this.Fs.BytesToBlock(value.Block), common.BLOCK_STORE_TYPE_NORMAL)
			if err != nil {
				return err
			}
			err = this.Fs.PutTag(fmt.Sprintf("%s%d", hash, value.Index), value.Tag)
			if err != nil {
				return err
			}
			this.NodeFilemgr.SetBlockStored(fileHashStr, value.Hash, uint32(index))
		case <-time.After(time.Duration(common.BLOCK_FETCH_TIMEOUT) * time.Second):
			if received {
				break
			}
			timeout = true
			return fmt.Errorf("receiving block %s timeout", hash)
		}
	}
	if !this.NodeFilemgr.IsFileStored(fileHashStr) {
		return errors.New("all blocks have sent but file not be stored")
	}
	// TODO: push to tracker
	log.Infof("received all block, start pdp verify")
	// all block is saved, prove it
	return this.Fs.StartPDPVerify(fileHashStr)
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

func emitProgress(uploading chan *common.UploadingInfo, desc, hash string, total, sent uint64) {
	uploading <- &common.UploadingInfo{
		FileName: desc,
		FileHash: hash,
		Total:    total,
		Uploaded: sent,
	}
}
