package dsp

import (
	"encoding/hex"
	"errors"
	"fmt"
	ipld "gx/ipfs/Qme5bWv7wtjUNGsK2BNGVUFPKiuxWrsqrtvYwCLRw8YFES/go-ipld-format"
	"math"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/saveio/dsp-go-sdk/actor/client"
	"github.com/saveio/dsp-go-sdk/common"
	serr "github.com/saveio/dsp-go-sdk/error"
	"github.com/saveio/dsp-go-sdk/network/message"
	"github.com/saveio/dsp-go-sdk/network/message/types/file"
	"github.com/saveio/dsp-go-sdk/task"
	"github.com/saveio/dsp-go-sdk/utils"
	"github.com/saveio/max/importer/helpers"
	chainCom "github.com/saveio/themis/common"
	"github.com/saveio/themis/common/log"
	"github.com/saveio/themis/crypto/pdp"
)

// fetchedDone. handle fetched request channel data structure
type fetchedDone struct {
	done bool
	err  error
}

// blockMsgData. data to send of block msg
type blockMsgData struct {
	blockData []byte
	dataLen   uint64
	tag       []byte
}

// CalculateUploadFee. pre-calculate upload cost by its upload options
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
	defer func() {
		err = fi.Close()
		if err != nil {
			log.Errorf("close file %s err %s", filePath, err)
		}
	}()
	txGas := uint64(10000000)
	useDefault := opt.StorageType == common.FileStoreTypeNormal
	log.Debugf("filePath %s, opt.interval:%d, opt.ProveTimes:%d, opt.CopyNum:%d, useDefault %v ", filePath, opt.ProveInterval, opt.ProveTimes, opt.CopyNum, useDefault)
	if whitelistCnt > 0 {
		fee = txGas * 4
	} else {
		fee = txGas * 3
	}
	if useDefault {
		return fee, nil
	}
	fileSize := uint64(fileInfo.Size()) / 1024
	if fileSize < 0 {
		fileSize = 1
	}
	fee += (opt.ProveInterval*fileSize*fsSetting.GasPerKBPerBlock +
		fsSetting.GasForChallenge) * uint64(opt.ProveTimes) * fsSetting.FsGasPrice * (uint64(opt.CopyNum) + 1)
	log.Debugf("fileSize :%d, fee", fileSize, fee)
	return fee, nil
}

// UploadTaskExist. check if upload task is existed by filePath hash string
func (this *Dsp) UploadTaskExist(filePath string) bool {
	taskId := this.taskMgr.TaskId(filePath, this.WalletAddress(), task.TaskTypeUpload)
	return len(taskId) > 0
}

// UploadFile upload new file logic synchronously
func (this *Dsp) UploadFile(taskId, filePath string, opt *common.UploadOption) (*common.UploadResult, error) {
	var uploadRet *common.UploadResult
	var sdkerr *serr.SDKError
	var err error
	if len(taskId) == 0 {
		taskId, err = this.taskMgr.NewTask(task.TaskTypeUpload)
	}
	this.taskMgr.EmitProgress(taskId, task.TaskUploadFileMakeSlice)
	// emit result finally
	defer func() {
		log.Debugf("emit result finally id %v, ret %v err %v", taskId, uploadRet, err)
		this.taskMgr.EmitResult(taskId, uploadRet, sdkerr)
		// delete task from cache in the end when success
		if uploadRet != nil && sdkerr == nil {
			this.taskMgr.DeleteTask(taskId, false)
		}
	}()
	if err != nil {
		sdkerr = serr.NewDetailError(serr.NEW_TASK_FAILED, err.Error())
		return nil, err
	}
	// check options valid
	if err = uploadOptValid(filePath, opt); err != nil {
		sdkerr = serr.NewDetailError(serr.INVALID_PARAMS, err.Error())
		return nil, err
	}
	var tx, fileHashStr string
	var totalCount uint64
	log.Debugf("new upload task: %s, will split for file", taskId)
	// split file to pieces
	root, list, err := this.Fs.NodesFromFile(filePath, this.WalletAddress(), opt.Encrypt, opt.EncryptPassword)
	if err != nil {
		sdkerr = serr.NewDetailError(serr.SHARDING_FAIELD, err.Error())
		log.Errorf("node from file err: %s", err)
		return nil, err
	}
	this.taskMgr.EmitProgress(taskId, task.TaskUploadFileMakeSliceDone)
	fileHashStr = root.Cid().String()
	this.taskMgr.SetTaskInfos(taskId, fileHashStr, filePath, opt.FileDesc, this.WalletAddress())
	this.taskMgr.BindTaskId(taskId)
	// check has uploaded this file
	isUploaded := this.taskMgr.IsFileUploaded(taskId)
	if isUploaded {
		sdkerr = serr.NewDetailError(serr.FILE_HAS_UPLOADED, fmt.Sprint("file has uploaded %s", fileHashStr))
		log.Debugf("file:%s has uploaded", fileHashStr)
		return nil, sdkerr.Error
	}
	log.Debugf("root:%s, list.len:%d", fileHashStr, len(list))
	// get nodeList
	nodeList, err := this.getUploadNodeList(filePath, taskId, fileHashStr)
	if err != nil {
		sdkerr = serr.NewDetailError(serr.GET_STORAGE_NODES_FAILED, err.Error())
		return nil, err
	}
	log.Debugf("uploading nodelist %v, opt.CopyNum %d", nodeList, opt.CopyNum)
	if uint32(len(nodeList)) < opt.CopyNum+1 {
		sdkerr = serr.NewDetailError(serr.ONLINE_NODES_NOT_ENOUGH, fmt.Sprintf("node is not enough %d, copyNum %d", len(nodeList), opt.CopyNum))
		return nil, sdkerr.Error
	}
	this.taskMgr.EmitProgress(taskId, task.TaskUploadFilePaying)
	totalCount = uint64(len(list) + 1)
	// pay file
	payRet, err := this.payForSendFile(filePath, taskId, fileHashStr, uint64(len(list)+1), opt)
	if err != nil {
		sdkerr = serr.NewDetailError(serr.PAY_FOR_STORE_FILE_FAILED, err.Error())
		return nil, err
	}
	payTxHeight, err := this.Chain.GetBlockHeightByTxHash(payRet.Tx)
	if err != nil {
		sdkerr = serr.NewDetailError(serr.PAY_FOR_STORE_FILE_FAILED, err.Error())
		return nil, err
	}
	err = this.taskMgr.BatchSetFileInfo(taskId, fileHashStr, nil, nil, totalCount)
	err = this.taskMgr.SetStoreTx(taskId, payRet.Tx)
	err = this.taskMgr.SetPrivateKey(taskId, payRet.PrivateKey)
	if err != nil {
		sdkerr = serr.NewDetailError(serr.SET_FILEINFO_DB_ERROR, err.Error())
		return nil, err
	}
	this.taskMgr.EmitProgress(taskId, task.TaskUploadFilePayingDone)
	var addWhiteListTx string
	if len(opt.WhiteList) > 0 {
		this.taskMgr.EmitProgress(taskId, task.TaskUploadFileCommitWhitelist)
		addWhiteListTx, err = this.addWhitelistsForFile(fileHashStr, opt.WhiteList, opt.ProveInterval*uint64(opt.ProveTimes))
		if err != nil {
			sdkerr = serr.NewDetailError(serr.ADD_WHITELIST_ERROR, err.Error())
			return nil, err
		}
		err = this.taskMgr.SetWhitelistTx(taskId, addWhiteListTx)
		if err != nil {
			sdkerr = serr.NewDetailError(serr.SET_FILEINFO_DB_ERROR, err.Error())
			return nil, err
		}
		this.taskMgr.EmitProgress(taskId, task.TaskUploadFileCommitWhitelistDone)
	}
	tx = payRet.Tx
	p, err := this.Chain.Native.Fs.ProveParamDes(payRet.ParamsBuf)
	if err != nil {
		sdkerr = serr.NewDetailError(serr.GET_PDP_PARAMS_ERROR, err.Error())
		return nil, err
	}
	// log.Debugf("g0:%v, fileID:%v, privateKey:%v", sha1.Sum(g0), sha1.Sum(fileID), sha1.Sum(payRet.PrivateKey))
	hashes, err := this.Fs.AllBlockHashes(root, list)
	if err != nil {
		sdkerr = serr.NewDetailError(serr.GET_ALL_BLOCK_ERROR, err.Error())
		return nil, err
	}
	if opt != nil && opt.Share {
		go this.shareUploadedFile(filePath, opt.FileDesc, this.WalletAddress(), hashes, root, list)
	}
	this.taskMgr.EmitProgress(taskId, task.TaskUploadFileFindReceivers)
	receivers, err := this.waitFileReceivers(taskId, fileHashStr, nodeList, hashes, int(opt.CopyNum)+1, tx, uint64(payTxHeight))
	if err != nil {
		sdkerr = serr.NewDetailError(serr.SEARCH_RECEIVERS_FAILED, err.Error())
		return nil, err
	}
	log.Debugf("receivers:%v", receivers)
	if len(receivers) < int(opt.CopyNum)+1 {
		err = errors.New("file receivers is not enough")
		sdkerr = serr.NewDetailError(serr.RECEIVERS_NOT_ENOUGH, err.Error())
		return nil, err
	}
	// notify fetch ready to all receivers
	err = this.notifyFetchReady(taskId, fileHashStr, receivers)
	if err != nil {
		sdkerr = serr.NewDetailError(serr.RECEIVERS_REJECTED, err.Error())
		return nil, err
	}
	this.taskMgr.EmitProgress(taskId, task.TaskUploadFileFindReceiversDone)

	// TODO: set max count
	maxFetchRoutines := len(receivers)
	if maxFetchRoutines > common.MAX_UPLOAD_ROUTINES {
		maxFetchRoutines = common.MAX_UPLOAD_ROUTINES
	}
	doneCh := make(chan *fetchedDone)
	sdkerr = this.waitForFetchBlock(taskId, root, list, maxFetchRoutines, int(opt.CopyNum), p.G0, p.FileId, payRet.PrivateKey, doneCh)
	if sdkerr != nil {
		return nil, errors.New(sdkerr.Error.Error())
	}

	this.taskMgr.EmitProgress(taskId, task.TaskUploadFileWaitForPDPProve)
	proved := this.checkFileBeProved(fileHashStr, opt.ProveTimes, opt.CopyNum)
	close(doneCh)
	log.Debugf("checking file proved done %t", proved)
	if !proved {
		err = errors.New("file has sent, but no enough prove is finished")
		sdkerr = serr.NewDetailError(serr.FILE_UPLOADED_CHECK_PDP_FAILED, err.Error())
		return nil, err
	}
	this.taskMgr.EmitProgress(taskId, task.TaskUploadFileWaitForPDPProveDone)
	this.taskMgr.EmitProgress(taskId, task.TaskUploadFileRegisterDNS)
	oniLink := utils.GenOniLink(fileHashStr, opt.FileDesc, 0, totalCount, this.DNS.TrackerUrls)
	var dnsRegTx, dnsBindTx string
	if opt.RegisterDns && len(opt.DnsUrl) > 0 {
		dnsRegTx, err = this.RegisterFileUrl(opt.DnsUrl, oniLink)
		log.Debugf("acc %s, reg dns %s, err %s", this.Chain.Native.Dns.DefAcc.Address.ToBase58(), dnsRegTx, err)
	}
	if opt.BindDns && len(opt.DnsUrl) > 0 {
		dnsBindTx, err = this.BindFileUrl(opt.DnsUrl, oniLink)
		log.Debugf("bind dns %s, err %s", dnsBindTx, err)
	}
	this.taskMgr.EmitProgress(taskId, task.TaskUploadFileRegisterDNSDone)
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
func (this *Dsp) DeleteUploadedFile(fileHashStr string) (*common.DeleteUploadFileResp, error) {
	if len(fileHashStr) == 0 {
		return nil, errors.New("delete file hash string is empty")
	}
	info, err := this.Chain.Native.Fs.GetFileInfo(fileHashStr)
	if err != nil || info == nil {
		log.Debugf("info:%v, err:%s", info, err)
		return nil, fmt.Errorf("file info not found, %s has deleted", fileHashStr)
	}
	if info.FileOwner.ToBase58() != this.WalletAddress() {
		return nil, fmt.Errorf("file %s can't be deleted, you are not the owner", fileHashStr)
	}
	storingNode, _ := this.getFileProveNode(fileHashStr, info.ChallengeTimes)
	txHash, err := this.Chain.Native.Fs.DeleteFile(fileHashStr)
	if err != nil {
		return nil, err
	}
	txHashStr := hex.EncodeToString(chainCom.ToArrayReverse(txHash))
	txHeight, err := this.Chain.GetBlockHeightByTxHash(txHashStr)
	if err != nil {
		return nil, err
	}
	log.Debugf("delete file txHash %s", txHashStr)
	confirmed, err := this.Chain.PollForTxConfirmed(time.Duration(common.TX_CONFIRM_TIMEOUT)*time.Second, txHash)
	if err != nil || !confirmed {
		return nil, errors.New("wait for tx confirmed failed")
	}
	taskId := this.taskMgr.TaskId(fileHashStr, this.WalletAddress(), task.TaskTypeUpload)
	if len(storingNode) == 0 {
		// find breakpoint keep nodelist
		storingNode = append(storingNode, this.taskMgr.GetUploadedBlockNodeList(taskId, fileHashStr, 0)...)
	}
	sessionId, err := this.taskMgr.GetSeesionId(taskId, "")
	if err != nil {
		return nil, err
	}
	log.Debugf("will broadcast delete msg to %v", storingNode)
	resp := &common.DeleteUploadFileResp{}
	if len(storingNode) > 0 {
		msg := message.NewFileDelete(sessionId, fileHashStr, this.WalletAddress(), txHashStr, uint64(txHeight))
		m, err := client.P2pBroadcast(storingNode, msg.ToProtoMsg(), true, nil, nil)
		if err != nil {
			return nil, err
		}
		nodeStatus := make([]common.DeleteFileStatus, 0, len(m))
		for addr, deleteErr := range m {
			errMsg := ""
			if deleteErr != nil {
				errMsg = deleteErr.Error()
			}
			nodeStatus = append(nodeStatus, common.DeleteFileStatus{
				HostAddr: addr,
				Status:   "",
				ErrorMsg: errMsg,
			})
		}
		resp.Status = nodeStatus
		log.Debugf("broadcast to delete file msg success")
	}
	// TODO: make transaction commit
	err = this.taskMgr.DeleteTask(taskId, true)
	if err != nil {
		log.Errorf("delete upload info from db err: %s", err)
	}
	resp.Tx = hex.EncodeToString(chainCom.ToArrayReverse(txHash))
	return resp, nil
}

// payForSendFile pay before send a file
// return PoR params byte slice, PoR private key of the file or error
func (this *Dsp) payForSendFile(filePath, taskKey, fileHashStr string, blockNum uint64, opt *common.UploadOption) (*common.PayStoreFileResult, error) {
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
			log.Errorf("serialization prove params failed:%s", err)
			return nil, err
		}
		txHash, err := this.Chain.Native.Fs.StoreFile(fileHashStr, blockNum, blockSizeInKB, opt.ProveInterval,
			uint64(opt.ProveTimes), uint64(opt.CopyNum), []byte(opt.FileDesc), uint64(opt.Privilege), paramsBuf, uint64(opt.StorageType))
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
	} else {
		storing, stored := this.getFileProveNode(fileHashStr, fileInfo.ChallengeTimes)
		if len(storing) > 0 {
			return nil, fmt.Errorf("file:%s has stored", fileHashStr)
		}
		if len(stored) > 0 {
			return nil, fmt.Errorf("file:%s has expired, please delete it first", fileHashStr)
		}
		log.Debugf("has paid but not store")
		if uint64(this.taskMgr.UploadedBlockCount(taskKey)) == blockNum*(fileInfo.CopyNum+1) {
			return nil, fmt.Errorf("has sent all blocks, waiting for fs node commit proves")
		}
		paramsBuf = fileInfo.FileProveParam
		var err error
		privateKey, err = this.taskMgr.GetFilePrivateKey(taskKey)
		if err != nil {
			return nil, err
		}
		tx, err = this.taskMgr.GetStoreTx(taskKey)
		if err != nil {
			return nil, err
		}
	}
	if len(paramsBuf) == 0 || len(privateKey) == 0 {
		return nil, fmt.Errorf("params.length is %d, prove private key length is %d. please delete file and re-send it", len(paramsBuf), len(privateKey))
	}
	return &common.PayStoreFileResult{
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
	_, err = this.Chain.PollForTxConfirmed(time.Duration(common.TX_CONFIRM_TIMEOUT)*time.Second, txHash)
	if err != nil {
		return "", err
	}
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
func (this *Dsp) waitFileReceivers(taskId, fileHashStr string, nodeList, blockHashes []string, receiverCount int, txHash string, txHeight uint64) ([]string, error) {
	var receiverLock sync.Mutex
	receivers := make([]string, 0)
	receiverLen := int32(0)
	sessionId, err := this.taskMgr.GetSeesionId(taskId, "")
	if err != nil {
		return nil, err
	}
	msg := message.NewFileFetchAsk(sessionId, fileHashStr, blockHashes, this.WalletAddress(), this.WalletAddress(), txHash, txHeight)
	action := func(res proto.Message, addr string) {
		log.Debugf("send file ask msg success %s", addr)
		p2pMsg := message.ReadMessage(res)
		if p2pMsg.Error != nil {
			log.Errorf("get file fetch_ack msg err %s", p2pMsg.Error.Message)
			return
		}
		// waiting for ack msg
		fileMsg := p2pMsg.Payload.(*file.File)
		receiverLock.Lock()
		receivers = append(receivers, addr)
		receiverLock.Unlock()
		log.Debugf("send file_ask msg %s success %s, receive file_ack msg", fileMsg.Hash, addr)
		atomic.AddInt32(&receiverLen, 1)
	}
	// TODO: make stop func sense in parallel mode
	stop := func() bool {
		return atomic.LoadInt32(&receiverLen) >= int32(receiverCount)
	}
	log.Debugf("broadcast fetch_ask msg to %v", nodeList)
	ret, err := client.P2pBroadcast(nodeList, msg.ToProtoMsg(), true, stop, action)
	if err != nil {
		log.Errorf("wait file receivers broadcast err")
		return nil, err
	}
	log.Debugf("receives :%v, receiverCount %v, real %v", receivers, receiverCount, atomic.LoadInt32(&receiverLen))
	if len(receivers) >= receiverCount {
		receivers = append([]string{}, receivers[:receiverCount]...)
	}
	log.Debugf("receives :%v, ret %v", receivers, ret)
	return receivers, nil
}

// notifyFetchReady send fetch_rdy msg to receivers
func (this *Dsp) notifyFetchReady(taskId, fileHashStr string, receivers []string) error {
	sessionId, err := this.taskMgr.GetSeesionId(taskId, "")
	if err != nil {
		return err
	}
	msg := message.NewFileFetchRdy(sessionId, fileHashStr, this.WalletAddress())
	this.taskMgr.SetTaskReady(taskId, true)
	ret, err := client.P2pBroadcast(receivers, msg.ToProtoMsg(), false, nil, nil)
	if err != nil {
		log.Errorf("notify err %s", err)
		this.taskMgr.SetTaskReady(taskId, false)
		return err
	}
	for _, e := range ret {
		if e != nil {
			log.Errorf("notify err %s", err)
			this.taskMgr.SetTaskReady(taskId, false)
			return e
		}
	}
	return nil
}

// generateBlockMsgData. generate blockdata, blockEncodedData, tagData with prove params
func (this *Dsp) generateBlockMsgData(list []*helpers.UnixfsNode, fileID, g0, privateKey []byte) (map[string]*blockMsgData, error) {
	m := make(map[string]*blockMsgData, 0)
	for i, node := range list {
		dagNode, err := node.GetDagNode()
		if err != nil {
			return nil, err
		}
		blockDecodedData, err := this.Fs.BlockToBytes(dagNode)
		if err != nil {
			return nil, err
		}
		blockData := this.Fs.BlockDataOfAny(node)
		index := uint32(i + 1)
		key := keyOfUnixNode(dagNode.Cid().String(), index)
		tag, err := pdp.SignGenerate(blockData, fileID, uint32(index+1), g0, privateKey)
		if err != nil {
			return nil, err
		}
		m[key] = &blockMsgData{
			blockData: blockData,
			dataLen:   uint64(len(blockDecodedData)),
			tag:       tag,
		}
	}
	return m, nil
}

// waitForFetchBlock. wait for fetched blocks concurrent
func (this *Dsp) waitForFetchBlock(taskId string, root ipld.Node, list []*helpers.UnixfsNode, maxFetchRoutines, copyNum int, g0, fileID, privateKey []byte, doneCh chan *fetchedDone) *serr.SDKError {
	req, err := this.taskMgr.TaskBlockReq(taskId)
	if err != nil {
		return serr.NewDetailError(serr.PREPARE_UPLOAD_ERROR, err.Error())
	}
	this.taskMgr.EmitProgress(taskId, task.TaskUploadFileGeneratePDPData)
	blockMsgDataMap, err := this.generateBlockMsgData(list, fileID, g0, privateKey)
	if err != nil {
		return serr.NewDetailError(serr.GET_ALL_BLOCK_ERROR, err.Error())
	}
	this.taskMgr.EmitProgress(taskId, task.TaskUploadFileTransferBlocks)
	timeout := time.NewTimer(time.Duration(common.BLOCK_FETCH_TIMEOUT) * time.Second)
	sessionId, err := this.taskMgr.GetSeesionId(taskId, "")
	if err != nil {
		return serr.NewDetailError(serr.GET_SESSION_ID_FAILED, err.Error())
	}
	fileHashStr := this.taskMgr.TaskFileHash(taskId)
	rootBlockData := this.Fs.BlockDataOfAny(root)
	if len(rootBlockData) == 0 {
		return serr.NewDetailError(serr.PREPARE_UPLOAD_ERROR, fmt.Sprintf("root block is nil %s", fileHashStr))
	}
	rootDecodedData, err := this.Fs.BlockToBytes(root)
	if err != nil {
		return serr.NewDetailError(serr.PREPARE_UPLOAD_ERROR, err.Error())
	}
	rootDataLen := uint64(len(rootDecodedData))
	// root tag generate index from 1
	rootTag, err := pdp.SignGenerate(rootBlockData, fileID, 1, g0, privateKey)
	if err != nil {
		return serr.NewDetailError(serr.PREPARE_UPLOAD_ERROR, err.Error())
	}
	totalCount := uint64(len(blockMsgDataMap) + 1)

	for i := 0; i < maxFetchRoutines; i++ {
		go func() {
			for {
				select {
				case reqInfo := <-req:
					timeout.Reset(time.Duration(common.BLOCK_FETCH_TIMEOUT) * time.Second)
					var msgData *blockMsgData
					if reqInfo.Hash == fileHashStr && reqInfo.Index == 0 {
						msgData = &blockMsgData{
							blockData: rootBlockData,
							dataLen:   rootDataLen,
							tag:       rootTag,
						}
					} else {
						key := keyOfUnixNode(reqInfo.Hash, uint32(reqInfo.Index))
						log.Debugf("get msg data by key %s", key)
						msgData = blockMsgDataMap[key]
					}
					// handle fetch request async
					this.handleFetchBlockRequest(taskId, sessionId, fileHashStr, reqInfo, copyNum, totalCount, msgData, doneCh)
				case _, ok := <-doneCh:
					if !ok {
						log.Debugf("stop rotines")
						return
					}
				}
			}
		}()
	}
	select {
	case ret := <-doneCh:
		log.Debugf("receive fetch file done channel done: %v, err: %s", ret.done, ret.err)
		if ret.err != nil {
			return serr.NewDetailError(serr.TASK_INTERNAL_ERROR, err.Error())
		}
		if !ret.done {
			break
		}
		// check all blocks has sent
		log.Infof("all block has sent %s", taskId)
		break
	case <-timeout.C:
		err = errors.New("wait for fetch block timeout")
		return serr.NewDetailError(serr.TASK_WAIT_TIMEOUT, err.Error())
	}
	this.taskMgr.EmitProgress(taskId, task.TaskUploadFileTransferBlocksDone)
	return nil
}

// handleFetchBlockRequest. handle fetch request and send block
func (this *Dsp) handleFetchBlockRequest(taskId, sessionId, fileHashStr string,
	reqInfo *task.GetBlockReq,
	copyNum int,
	totalCount uint64,
	blockMsgData *blockMsgData,
	done chan *fetchedDone) {
	// send block
	log.Debugf("receive fetch block msg of %s-%s-%d from %s", reqInfo.FileHash, reqInfo.Hash, reqInfo.Index, reqInfo.PeerAddr)
	if len(blockMsgData.blockData) == 0 || len(blockMsgData.tag) == 0 {
		err := fmt.Errorf("block len %d, tag len %d hash %s, peer %s failed", len(blockMsgData.blockData), len(blockMsgData.tag), reqInfo.Hash, reqInfo.PeerAddr)
		done <- &fetchedDone{done: false, err: err}
		return
	}
	isStored := this.taskMgr.IsBlockUploaded(taskId, reqInfo.Hash, reqInfo.PeerAddr, uint32(reqInfo.Index))
	if isStored {
		log.Debugf("block has stored %s", reqInfo.Hash)
		// done <- &fetchedDone{done: false, err: nil}
		return
	}
	// TODO: check err? replace offset calculate by fs api
	var offset uint64
	if reqInfo.Index > 0 {
		tail, err := this.taskMgr.GetBlockTail(taskId, uint32(reqInfo.Index-1))
		if err != nil {
			done <- &fetchedDone{done: false, err: err}
			return
		}
		offset = tail
	}
	msg := message.NewBlockMsg(sessionId, int32(reqInfo.Index), fileHashStr, reqInfo.Hash, blockMsgData.blockData, blockMsgData.tag, int64(offset))
	log.Debugf("send fetched block msg len:%d, block len:%d, tag len:%d", msg.Header.MsgLength, blockMsgData.dataLen, len(blockMsgData.tag))
	err := client.P2pSend(reqInfo.PeerAddr, msg.ToProtoMsg())
	if err != nil {
		log.Errorf("send block msg hash %s to peer %s failed, err %s", reqInfo.Hash, reqInfo.PeerAddr, err)
		done <- &fetchedDone{done: false, err: err}
		return
	}
	log.Debugf("send block success %s, index:%d, taglen:%d, offset:%d ", reqInfo.Hash, reqInfo.Index, len(blockMsgData.tag), offset)
	// stored
	this.taskMgr.AddUploadedBlock(taskId, reqInfo.Hash, reqInfo.PeerAddr, uint32(reqInfo.Index), blockMsgData.dataLen, offset)
	// update progress
	this.taskMgr.EmitProgress(taskId, task.TaskUploadFileTransferBlocks)
	log.Debugf("upload node list len %d, taskkey %s, hash %s, index %d", len(this.taskMgr.GetUploadedBlockNodeList(taskId, reqInfo.Hash, uint32(reqInfo.Index))), taskId, reqInfo.Hash, reqInfo.Index)
	// check all copynum node has received the block
	count := len(this.taskMgr.GetUploadedBlockNodeList(taskId, reqInfo.Hash, uint32(reqInfo.Index)))
	if count < copyNum+1 {
		log.Debugf("peer %s getUploadedBlockNodeList hash:%s, index:%d, count %d less than copynum %d", reqInfo.PeerAddr, reqInfo.Hash, reqInfo.Index, count, copyNum)
		return
	}
	sent := uint64(this.taskMgr.UploadedBlockCount(taskId) / (uint64(copyNum) + 1))
	if totalCount != sent {
		log.Debugf("totalCount %d != sent %d %d", totalCount, sent, this.taskMgr.UploadedBlockCount(taskId))
		return
	}
	log.Debugf("fetched job done of id %s hash %s index %d peer %s", taskId, fileHashStr, reqInfo.Index, reqInfo.PeerAddr)
	done <- &fetchedDone{
		done: true,
		err:  nil,
	}
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

func keyOfUnixNode(hash string, index uint32) string {
	return fmt.Sprintf("%s-%d", hash, index)
}
