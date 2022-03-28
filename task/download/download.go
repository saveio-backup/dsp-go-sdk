package download

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/saveio/dsp-go-sdk/actor/client"
	"github.com/saveio/dsp-go-sdk/consts"
	sdkErr "github.com/saveio/dsp-go-sdk/error"
	netCom "github.com/saveio/dsp-go-sdk/network/common"
	"github.com/saveio/dsp-go-sdk/network/message"
	"github.com/saveio/dsp-go-sdk/network/message/types/block"
	"github.com/saveio/dsp-go-sdk/network/message/types/file"
	"github.com/saveio/dsp-go-sdk/network/message/types/payment"
	"github.com/saveio/dsp-go-sdk/store"
	"github.com/saveio/dsp-go-sdk/task/base"
	"github.com/saveio/dsp-go-sdk/task/types"
	uPrefix "github.com/saveio/dsp-go-sdk/types/prefix"
	"github.com/saveio/dsp-go-sdk/utils/crypto"
	uTask "github.com/saveio/dsp-go-sdk/utils/task"
	uTime "github.com/saveio/dsp-go-sdk/utils/time"
	"github.com/saveio/themis/common/log"
)

func (this *DownloadTask) Start(opt *types.DownloadOption) error {
	log.Debugf("download file id: %s, fileHash: %s, option: %v", this.GetId(), this.GetFileHash(), opt)
	this.EmitProgress(types.TaskCreate)
	this.EmitProgress(types.TaskDownloadFileStart)
	if err := this.setTaskState(store.TaskStateDoing); err != nil {
		return err
	}
	if len(this.GetFileHash()) == 0 {
		log.Errorf("taskId %s no filehash for download", this.GetId())
		return sdkErr.New(sdkErr.DOWNLOAD_FILEHASH_NOT_FOUND, "no filehash for download")
	}
	if !this.Mgr.DNS().HasDNS() {
		return sdkErr.New(sdkErr.NO_CONNECTED_DNS, "no online dns node")
	}
	log.Debugf("download file dns node %s, url %s, total block %v", this.Mgr.DNS().CurrentDNSWallet(), opt.Url, opt.BlockNum)

	if opt.MaxPeerCnt > consts.MAX_DOWNLOAD_PEERS_NUM {
		opt.MaxPeerCnt = consts.MAX_DOWNLOAD_PEERS_NUM
	}
	// store task info
	if err := this.SetInfoWithOptions(
		base.FileHash(this.GetFileHash()),
		base.BlocksRoot(opt.BlocksRoot),
		base.FileName(opt.FileName),
		base.FileOwner(opt.FileOwner),
		base.Asset(opt.Asset),
		base.DecryptPwd(opt.DecryptPwd),
		base.Free(opt.Free),
		base.SetFileName(opt.SetFileName),
		base.MaxPeerCnt(opt.MaxPeerCnt),
		base.Url(opt.Url),
		base.InOrder(opt.InOrder),
		base.FileSize(getFileSizeWithBlockCount(opt.BlockNum)),
		base.Walletaddr(this.GetCurrentWalletAddr())); err != nil {
		return err
	}

	this.EmitProgress(types.TaskDownloadFileStart)
	if stop := this.IsTaskStop(); stop {
		return sdkErr.New(sdkErr.DOWNLOAD_BLOCK_FAILED, "download task %s is stop", this.GetId())
	}
	this.EmitProgress(types.TaskDownloadSearchPeers)
	addrs, err := this.getPeersForDownload()
	if err != nil {
		return err
	}

	if stop := this.IsTaskStop(); stop {
		return sdkErr.New(sdkErr.DOWNLOAD_BLOCK_FAILED, "download task %s is stop", this.GetId())
	}
	return this.downloadFileFromPeers(addrs)
}

func (this *DownloadTask) Pause() error {
	if err := this.Task.Pause(); err != nil {
		return err
	}
	this.EmitProgress(types.TaskPause)
	return nil
}

func (this *DownloadTask) Resume() error {
	if err := this.Task.Resume(); err != nil {
		log.Errorf("resume download task err %s", err)
		return err
	}
	this.EmitProgress(types.TaskDoing)

	if len(this.GetFileHash()) == 0 {
		sdkErr := sdkErr.New(sdkErr.GET_FILEINFO_FROM_DB_ERROR, "filehash not found %s", this.GetId())
		log.Errorf("resume download task err empty file hash")
		return sdkErr
	}
	opt := &types.DownloadOption{
		FileName:    this.GetFileName(),
		FileOwner:   this.GetFileOwner(),
		BlocksRoot:  this.GetBlocksRoot(),
		Asset:       this.GetAsset(),
		InOrder:     this.IsInOrder(),
		DecryptPwd:  this.GetDecryptPwd(),
		Free:        this.IsFree(),
		SetFileName: this.IsSetFileName(),
		MaxPeerCnt:  this.GetMaxPeerCnt(),
		Url:         this.GetUrl(),
		BlockNum:    this.GetTotalBlockCnt(),
	}
	// TODO: record original workers
	go this.Start(opt)
	return nil
}

func (this *DownloadTask) Retry() error {
	if err := this.Task.Retry(); err != nil {
		return err
	}
	this.EmitProgress(types.TaskDoing)
	if len(this.GetFileHash()) == 0 {
		sdkErr := sdkErr.New(sdkErr.GET_FILEINFO_FROM_DB_ERROR, "filehash not found %s", this.GetId())
		return sdkErr
	}
	log.Debugf("resume download file %s", this.GetId())
	opt := &types.DownloadOption{
		FileName:    this.GetFileName(),
		FileOwner:   this.GetFileOwner(),
		BlocksRoot:  this.GetBlocksRoot(),
		Asset:       this.GetAsset(),
		InOrder:     this.IsInOrder(),
		DecryptPwd:  this.GetDecryptPwd(),
		Free:        this.IsFree(),
		SetFileName: this.IsSetFileName(),
		MaxPeerCnt:  this.GetMaxPeerCnt(),
		Url:         this.GetUrl(),
		BlockNum:    this.GetTotalBlockCnt(),
	}
	// TODO: record original workers
	go this.Start(opt)
	return nil
}

func (this *DownloadTask) Cancel() error {
	// TODOV2: check is canncelling
	// if cancel {
	// 	return fmt.Errorf("task is cancelling: %s", taskId)
	// }
	if err := this.Task.Cancel(); err != nil {
		return err
	}

	sessions, err := this.DB.GetFileSessions(this.GetId())
	if err != nil {
		return err
	}
	if len(sessions) == 0 {
		log.Debugf("no sessions to cancel")
		return nil
	}

	fileHashStr := this.GetFileHash()
	log.Debugf("cancel download task %v", fileHashStr)
	// TODO: broadcast download cancel msg, need optimize
	wg := sync.WaitGroup{}
	for walletAddr, session := range sessions {
		wg.Add(1)
		go func(a string, ses *store.Session) {
			msg := message.NewFileMsg(fileHashStr, netCom.FILE_OP_DOWNLOAD_CANCEL,
				message.WithSessionId(ses.SessionId),
				message.WithWalletAddress(this.GetCurrentWalletAddr()),
				message.WithAsset(int32(ses.Asset)),
				message.WithSign(this.Mgr.Chain().CurrentAccount()),
			)
			client.P2PSend(a, msg.MessageId, msg.ToProtoMsg())
			wg.Done()
		}(walletAddr, session)
	}
	wg.Wait()

	// Delete downloaded file if it's cancel
	filePath := this.GetFilePath()
	if len(filePath) == 0 {
		log.Warnf("can't delete file %s because its' path is empty", fileHashStr)
		return nil
	}
	if err := this.Mgr.Fs().DeleteFile(fileHashStr, filePath); err != nil {
		log.Errorf("fs delete file: %s, path: %s, err: %s", fileHashStr, filePath, err)
	}
	return nil
}

// getPeersForDownload. get peers for download from tracker and fs contract
func (this *DownloadTask) getPeersForDownload() ([]string, error) {
	fileHashStr := this.GetFileHash()
	addrs := this.Mgr.DNS().GetPeerFromTracker(fileHashStr)
	log.Debugf("get addr from peer %v, hash %s %v", addrs, fileHashStr, this.Mgr.DNS().GetTrackerList())
	if len(addrs) > 0 {
		return addrs, nil
	}
	log.Warnf("get 0 peer from tracker of file %s, tracker num %d",
		fileHashStr, len(this.Mgr.DNS().GetTrackerList()))
	addrs = this.getFileProvedNode(fileHashStr)
	if len(addrs) > 0 {
		log.Debugf("get addr from peer %v, hash %s", addrs, fileHashStr)
		return addrs, nil
	}
	return nil, sdkErr.New(sdkErr.NO_DOWNLOAD_SEED, "No peer for downloading the file %s", fileHashStr)
}

// downloadFileFromPeers. downloadfile base methods. download file from peers.
func (this *DownloadTask) downloadFileFromPeers(addrs []string) error {
	peerPrices, err := this.getDownloadPeerPrices(addrs)
	log.Debugf("download task %s get peer prices :%v", this.GetId(), peerPrices)
	if err != nil {
		return sdkErr.New(sdkErr.GET_DOWNLOAD_INFO_FAILED_FROM_PEERS, err.Error())
	}
	if len(peerPrices) == 0 {
		return sdkErr.New(sdkErr.NO_DOWNLOAD_SEED, "no peerPrices from peers")
	}
	if stop := this.IsTaskStop(); stop {
		return sdkErr.New(sdkErr.DOWNLOAD_BLOCK_FAILED, "download task %s is stop", this.GetId())
	}
	if !this.IsFree() && len(addrs) > this.GetMaxPeerCnt() {
		log.Debugf("filter peers free %t len %d max %d", this.IsFree(), len(addrs), this.GetMaxPeerCnt())
		// filter peers
		peerPrices = uTask.SortPeersByPrice(peerPrices, this.GetMaxPeerCnt())
	}
	this.peerPrices = peerPrices
	if err := this.depositChannel(); err != nil {
		return sdkErr.New(sdkErr.PREPARE_CHANNEL_ERROR, err.Error())
	}
	if stop := this.IsTaskStop(); stop {
		return sdkErr.New(sdkErr.DOWNLOAD_BLOCK_FAILED, "download task %s is stop", this.GetId())
	}
	log.Debugf("set up channel success: %v", peerPrices)
	// get new task info because task info has updated
	return this.internalDownload()
}

// getDownloadPeerPrices. get peers and the download price of the file. if free flag is set, return price-free peers.
// this.Info. task info data struct
// opt. download options
// map[string]*file.Payment ( wallet addr <=> payment )
func (this *DownloadTask) getDownloadPeerPrices(addrs []string) (
	map[string]*file.Payment, error) {
	if len(addrs) == 0 {
		return nil, sdkErr.New(sdkErr.NO_DOWNLOAD_SEED, "no peer for download")
	}

	sessions, err := this.DB.GetFileSessions(this.GetId())
	if err != nil {
		return nil, err
	}
	peerPayInfos := make(map[string]*file.Payment, 0)
	if sessions != nil {
		// use original sessions
		log.Debugf("file %s is a breakpoint transfer task, use original sessions: %v",
			this.GetFileHash(), sessions)
		oldSessionWg := new(sync.WaitGroup)
		for _, session := range sessions {
			peerPayInfos[session.WalletAddr] = &file.Payment{
				WalletAddress: session.WalletAddr,
				Asset:         int32(session.Asset),
				UnitPrice:     session.UnitPrice,
			}
			this.SetSessionId(session.WalletAddr, session.SessionId)
			oldSessionWg.Add(1)
			go func(sesHostAddr string) {
				if err := client.P2PConnect(sesHostAddr); err != nil {
					log.Errorf("connect %s err %s", sesHostAddr, err)
				}
				oldSessionWg.Done()
			}(session.HostAddr)
		}
		oldSessionWg.Wait()
		log.Debugf("get session from db: %v", peerPayInfos)
		return peerPayInfos, nil
	}
	msg := message.NewFileMsg(this.GetFileHash(), netCom.FILE_OP_DOWNLOAD_ASK,
		message.WithWalletAddress(this.GetCurrentWalletAddr()),
		message.WithAsset(this.GetAsset()),
		message.WithSign(this.Mgr.Chain().CurrentAccount()),
	)
	blockHashes := make([]string, 0)
	prefix := ""
	replyLock := &sync.Mutex{}
	nodeHostMap := make(map[string]string)
	sessionIds := make(map[string]string)
	log.Debugf("broadcast file download ask msg %s to %v for file %s",
		msg.MessageId, addrs, this.GetFileHash())
	reply := func(msg proto.Message, hostAddr string) bool {
		replyLock.Lock()
		defer replyLock.Unlock()
		p2pMsg := message.ReadMessage(msg)
		if p2pMsg.Error != nil {
			log.Errorf("get download ask err %s from %s", p2pMsg.Error.Message, hostAddr)
			return false
		}
		fileMsg := p2pMsg.Payload.(*file.File)
		log.Debugf("receive file %s download ack msg from peer %s", fileMsg.Hash, hostAddr)
		if len(fileMsg.BlockHashes) < len(blockHashes) {
			return false
		}
		if len(prefix) > 0 && string(fileMsg.Prefix) != prefix {
			return false
		}

		filePrefix := &uPrefix.FilePrefix{}
		filePrefix.Deserialize(fileMsg.Prefix)
		if len(this.GetDecryptPwd()) > 0 && !uPrefix.VerifyEncryptPassword(this.GetDecryptPwd(), filePrefix.EncryptSalt,
			filePrefix.EncryptHash) {
			log.Warnf("encrypt password not match hash")
			return false
		}

		if this.IsFree() && (fileMsg.PayInfo != nil && fileMsg.PayInfo.UnitPrice != 0) {
			return false
		}
		// check block hashes valid
		if crypto.ComputeStringHashRoot(fileMsg.BlockHashes) != this.GetBlocksRoot() {
			return false
		}
		if len(blockHashes) == 0 {
			blockHashes = fileMsg.BlockHashes
		}
		if len(prefix) == 0 {
			prefix = string(fileMsg.Prefix)
		}
		nodeHostMap[fileMsg.PayInfo.WalletAddress] = hostAddr
		log.Debugf("file %s, prefix %s, total %d, blockCnt %d",
			fileMsg.Hash, fileMsg.Prefix, fileMsg.TotalBlockCount, len(blockHashes))
		sessionIds[fileMsg.PayInfo.WalletAddress] = fileMsg.SessionId
		peerPayInfos[fileMsg.PayInfo.WalletAddress] = fileMsg.PayInfo
		return false
	}
	ret, err := client.P2PBroadcast(addrs, msg.ToProtoMsg(), msg.MessageId, reply)
	log.Debugf("broadcast file download msg result %v err %s", ret, err)
	if err != nil {
		return nil, err
	}

	log.Debugf("peer prices:%v", peerPayInfos)
	if len(peerPayInfos) == 0 {
		return nil, sdkErr.New(sdkErr.REMOTE_PEER_DELETE_FILE, "no source available")
	}
	if !this.Mgr.IsDownloadTaskExist(this.GetId()) {
		return nil, sdkErr.New(sdkErr.TASK_NOT_EXIST, "task not exist")
	}
	log.Debugf("get file total count :%d", this.GetTotalBlockCnt())
	// check has add file block hashes
	if uint64(len(this.DB.FileBlockHashes(this.GetId()))) == this.GetTotalBlockCnt() &&
		this.GetTotalBlockCnt() != 0 {
		return peerPayInfos, nil
	}
	// add file block hashes
	log.Debugf("add file blockHashes task id %v hashes %s-%s, prefix %s, prefix len: %d",
		this.GetId(), blockHashes[0], blockHashes[len(blockHashes)-1], prefix, len(prefix))
	if err := this.DB.AddFileBlockHashes(this.GetId(), blockHashes); err != nil {
		return nil, err
	}
	totalBlockCount := len(blockHashes)
	// update total block count and prefix
	if err := this.SetInfoWithOptions(
		base.Prefix(prefix),
		base.NodeHostAddrs(nodeHostMap),
		base.FileSize(getFileSizeWithBlockCount(uint64(totalBlockCount))),
		base.TotalBlockCnt(uint64(totalBlockCount))); err != nil {
		return nil, err
	}
	for peerWalletAddr, sessionId := range sessionIds {
		sessionPeerPayInfo := peerPayInfos[peerWalletAddr]
		if sessionPeerPayInfo == nil {
			continue
		}
		this.SetSessionId(peerWalletAddr, sessionId)
		if err := this.DB.AddFileSession(this.GetId(), sessionId, peerWalletAddr, nodeHostMap[peerWalletAddr],
			uint32(sessionPeerPayInfo.Asset), sessionPeerPayInfo.UnitPrice); err != nil {
			return nil, err
		}
	}
	return peerPayInfos, nil
}

//  downloadFileWithPeerPrices. download file, piece by piece from addrs.
//  inOrder: if true, the file will be downloaded block by block in order
func (this *DownloadTask) internalDownload() error {
	// task exist at runtime
	peerPrices := this.peerPrices
	if len(peerPrices) == 0 {
		return sdkErr.New(sdkErr.GET_DOWNLOAD_INFO_FAILED_FROM_PEERS,
			"no peerPrices for download: %s", this.GetFileHash())
	}

	if !this.DB.IsTaskInfoExist(this.GetId()) {
		return sdkErr.New(sdkErr.FILEINFO_NOT_EXIST, "download info not exist: %s", this.GetFileHash())
	}
	// pay unpaid order of the file after last download
	if err := this.payUnpaidPayments(peerPrices); err != nil {
		return sdkErr.NewWithError(sdkErr.PAY_UNPAID_BLOCK_FAILED, err)
	}
	if stop := this.IsTaskStop(); stop {
		return sdkErr.New(sdkErr.DOWNLOAD_BLOCK_FAILED, "download task %s is stop", this.GetId())
	}
	// TODO: new download logic
	peerAddrWallet := make([]string, 0)
	wg := sync.WaitGroup{}
	for _, payInfo := range peerPrices {
		wg.Add(1)
		sessionId := this.GetSessionId(payInfo.WalletAddress)
		if len(sessionId) == 0 {
			log.Warnf("download task %s get sessionId nil", this.GetId())
			continue
		}
		peerAddrWallet = append(peerAddrWallet, payInfo.WalletAddress)
		go func(walletAddr string) {
			msg := message.NewFileMsg(this.GetFileHash(), netCom.FILE_OP_DOWNLOAD,
				message.WithSessionId(sessionId),
				message.WithWalletAddress(this.GetCurrentWalletAddr()),
				message.WithAsset(this.GetAsset()),
				message.WithSign(this.Mgr.Chain().CurrentAccount()),
			)
			log.Debugf("broadcast file_download msg to %v", walletAddr)
			hostAddr, err := client.P2PGetHostAddrFromWalletAddr(walletAddr, client.P2PNetTypeDsp)
			if err != nil {
				log.Errorf("get host addr failed %s", err)
			}
			broadcastRet, err := client.P2PBroadcast([]string{hostAddr}, msg.ToProtoMsg(), msg.MessageId)
			log.Debugf("brocast file download msg %v err %v", broadcastRet, err)
			wg.Done()
		}(payInfo.WalletAddress)
	}
	wg.Wait()
	blockHashes := this.DB.FileBlockHashes(this.GetId())
	fullFilePath := this.GetFullFilePath()
	log.Debugf("filehashstr:%v, blockhashes-len:%v, prefix:%s, full filePath %s",
		this.GetFileHash(), len(blockHashes), this.GetPrefix(), fullFilePath)
	this.SetFilePath(fullFilePath)

	if stop := this.IsTaskStop(); stop {
		return sdkErr.New(sdkErr.DOWNLOAD_BLOCK_FAILED, "download task %s is stop", this.GetId())
	}
	this.EmitProgress(types.TaskDownloadFileDownloading)

	this.NewWorkers(peerAddrWallet, this.downloadBlock)
	go this.workBackground()
	log.Debugf("start download file in order %t", this.IsInOrder())
	if !this.IsInOrder() {
		if err := this.receiveBlockNoOrder(peerAddrWallet); err != nil {
			log.Debugf("stack %s", debug.Stack())
			return err
		}
		log.Debugf("will check file hash task id %s, file hash %s, downloaded %t",
			this.GetId(), this.GetFileHash(), this.DB.IsFileDownloaded(this.GetId()))
		if this.Mgr.IsClient() &&
			this.DB.IsFileDownloaded(this.GetId()) && !uPrefix.GetPrefixEncrypted(this.GetPrefix()) {
			this.EmitProgress(types.TaskDownloadCheckingFile)
			stat, err := os.Stat(fullFilePath)
			if err != nil {
				return err
			}
			checkFileList := make([]string, 0)
			if stat.IsDir() {
				checkFileList, err = this.Mgr.Fs().NodesFromDir(fullFilePath, string(this.GetPrefix()), false, "")
				if err != nil {
					return err
				}
			} else {
				checkFileList, err = this.Mgr.Fs().NodesFromFile(fullFilePath, string(this.GetPrefix()), false, "")
				if err != nil {
					return err
				}
			}
			log.Debugf("checking file hash %s", checkFileList[0])
			if (len(checkFileList) == 0 || checkFileList[0] != this.GetFileHash()) && !stat.IsDir() {
				this.EmitProgress(types.TaskDownloadCheckingFileFailed)
				// TODO wangyu dir hash not match
				err = sdkErr.New(sdkErr.CHECK_FILE_FAILED, "file hash not match")
				return err
			}
			this.EmitProgress(types.TaskDownloadCheckingFileDone)
		}
		if len(this.GetDecryptPwd()) == 0 {
			return nil
		}
		log.Debugf("decrypt task %s file with pwd %s", this.GetId(), this.GetDecryptPwd())
		if err := this.decryptDownloadedFile(); err != nil {
			return err
		}
		return nil
	}
	// TODO: support in-order download
	return nil
}

// declare job for workers
func (this *DownloadTask) downloadBlock(taskId, fileHash, walletAddr string, blocks []*block.Block) ([]*types.BlockResp, error) {
	if stop := this.IsTaskStop(); stop {
		log.Debugf("stop download task %s", taskId)
		return nil, sdkErr.New(sdkErr.DOWNLOAD_BLOCK_FAILED, "download task %s is stop", taskId)
	}
	// if state
	this.EmitProgress(types.TaskDownloadRequestBlocks)
	startedAt := uTime.GetMilliSecTimestamp()
	resp, err := this.downloadBlockFlights(taskId, fileHash, walletAddr, blocks, 1,
		consts.DOWNLOAD_BLOCKFLIGHTS_TIMEOUT)
	if err != nil {
		this.DB.UpdateTaskPeerSpeed(taskId, walletAddr, 0)
		return nil, err
	}
	if resp == nil || len(resp) == 0 {
		this.DB.UpdateTaskPeerSpeed(taskId, walletAddr, 0)
		return nil, sdkErr.New(sdkErr.DOWNLOAD_BLOCK_FAILED, "download blocks is nil %s from  %s, err %s",
			fileHash, walletAddr, err)
	}
	this.EmitProgress(types.TaskDownloadReceiveBlocks)
	payInfo := this.peerPrices[walletAddr]
	var totalBytes int
	for _, v := range resp {
		totalBytes += len(v.Block)
	}
	if totalBytes == 0 {
		return nil, sdkErr.New(sdkErr.DOWNLOAD_BLOCK_FAILED, "request total bytes count 0")
	}
	endedAt := uTime.GetMilliSecTimestamp()
	speed := uint64(totalBytes)
	if endedAt > startedAt {
		speed = uint64(totalBytes) / (endedAt - startedAt)
	}
	this.DB.UpdateTaskPeerSpeed(this.GetId(), walletAddr, speed)
	this.EmitProgress(types.TaskDownloadPayForBlocks)
	log.Debugf("download block of file %s-%s-%d to %s-%s-%d from %s success, start paying to it, "+
		"size is %d speed: %d", fileHash, resp[0].Hash, resp[0].Index,
		fileHash, resp[len(resp)-1].Hash, resp[len(resp)-1].Index, walletAddr, totalBytes, speed)
	this.setWorkerUnPaid(walletAddr, true)
	paymentId := resp[0].PaymentId
	if paymentId == 0 {
		paymentId = this.Mgr.Channel().NewPaymentId()
	}
	if err := this.payForBlock(payInfo, uint64(totalBytes), paymentId, true); err != nil {
		log.Errorf("pay for blocks err %s", err)
		this.EmitProgress(types.TaskDownloadPayForBlocksFailed)
		return nil, sdkErr.New(sdkErr.PAY_UNPAID_BLOCK_FAILED, err.Error())
	}
	this.setWorkerUnPaid(walletAddr, false)
	this.EmitProgress(types.TaskDownloadPayForBlocksDone)
	log.Debugf("pay for block: %s to %s, wallet: %s success, paymentId: %d",
		fileHash, walletAddr, paymentId)
	return resp, nil
}

// checkDNSState. check the p2p state of a dns
func (this *DownloadTask) checkDNSState(dnsWalletAddr string) error {
	exist, err := client.P2PConnectionExist(dnsWalletAddr, client.P2PNetTypeChannel)
	if exist && err == nil {
		return nil
	}
	log.Debugf("DNS connection not exist reconnect...")
	if err = client.P2PReconnectPeer(dnsWalletAddr, client.P2PNetTypeChannel); err != nil {
		return err
	}
	if err = client.P2PWaitForConnected(dnsWalletAddr,
		time.Duration(consts.WAIT_CHANNEL_CONNECT_TIMEOUT)); err != nil {
		return err
	}
	return nil
}

// downloadBlockUnits. download block helper function for client.
func (this *DownloadTask) downloadBlockFlights(taskId, fileHashStr, peerWalletAddr string, blockReqs []*block.Block,
	retry, timeout uint32) ([]*types.BlockResp, error) {
	sessionId := this.GetSessionId(peerWalletAddr)
	if len(sessionId) == 0 {
		return nil, sdkErr.New(sdkErr.TASK_INTERNAL_ERROR, "download sessionId not exist: %s", peerWalletAddr)
	}
	if len(blockReqs) > 0 {
		log.Debugf("download block of task: %s %s-%s-%d to %s-%s-%d to %s",
			taskId, fileHashStr, blockReqs[0].Hash, blockReqs[0].Index, fileHashStr,
			blockReqs[len(blockReqs)-1].Hash, blockReqs[len(blockReqs)-1].Index)
	}
	timeStamp := time.Now().UnixNano()
	msg := message.NewBlockFlightsReqMsg(blockReqs, timeStamp)
	blockReqM := make(map[string]struct{}, 0)
	for _, req := range blockReqs {
		blockReqM[keyOfBlockHashAndIndex(req.Hash, req.Index)] = struct{}{}
	}
	for i := uint32(0); i < retry; i++ {
		state := this.State()
		log.Debugf("send download block flights msg sessionId %s of %s retry %d,"+
			" msgId %s, timeStamp %d, state %d",
			sessionId, fileHashStr, i, msg.MessageId, timeStamp, state)
		this.ActiveWorker(peerWalletAddr)
		resp, err := client.P2PSendAndWaitReply(peerWalletAddr, msg.MessageId, msg.ToProtoMsg())
		if err != nil {
			log.Errorf("send download block flights msg err: %s", err)
			continue
		}
		msg := message.ReadMessage(resp)
		if msg == nil {
			return nil, fmt.Errorf("receive invalid msg from peer %s", peerWalletAddr)
		}
		if msg.Payload == nil {
			log.Error("receive empty block flights of task %s", taskId)
			return nil, fmt.Errorf("receive empty block flights of task %s", taskId)
		}
		blockFlightsMsg := msg.Payload.(*block.BlockFlights)
		if blockFlightsMsg == nil || len(blockFlightsMsg.Blocks) == 0 {
			log.Errorf("receive empty block flights, err %v", msg.Error)
			return nil, fmt.Errorf("receive empty block flights of task %s", taskId)
		}
		log.Debugf("taskId: %s, sessionId: %s receive %d blocks from peer:%s",
			taskId, blockFlightsMsg.Blocks[0].SessionId, len(blockFlightsMsg.Blocks), peerWalletAddr)
		// active worker
		this.ActiveWorker(peerWalletAddr)
		blockResps := make([]*types.BlockResp, 0)
		for _, blockMsg := range blockFlightsMsg.Blocks {
			if _, ok := blockReqM[keyOfBlockHashAndIndex(blockMsg.Hash, blockMsg.Index)]; !ok {
				log.Warnf("block %s-%d is not my request task", blockMsg.Hash, blockMsg.Index)
				continue
			}
			block := this.Mgr.Fs().EncodedToBlockWithCid(blockMsg.Data, blockMsg.Hash)
			if block.Cid().String() != blockMsg.Hash {
				log.Warnf("receive wrong block %s-%d", blockMsg.Hash, blockMsg.Index)
				continue
			}
			blockResps = append(blockResps, &types.BlockResp{
				Hash:      blockMsg.Hash,
				Index:     blockMsg.Index,
				PeerAddr:  peerWalletAddr,
				Block:     blockMsg.Data,
				Tag:       blockMsg.Tag,
				Offset:    blockMsg.Offset,
				PaymentId: blockFlightsMsg.PaymentId,
			})
		}
		if len(blockResps) == 0 {
			log.Warnf("blockResps is not match request")
		}
		return blockResps, nil
	}
	// if err != nil {
	// 	log.Errorf("download block flight err %s", err)
	// }
	// return nil, err
	return nil, nil
}

// workBackground. Run n goroutines to check request pool one second a time.
// If there exist a idle request, find the idle worker to do the job
func (this *DownloadTask) workBackground() {
	fileHash := this.GetFileHash()
	tskWalletAddr := this.GetWalletAddr()
	addrs := this.GetWorkerAddrs()
	inOrder := this.IsInOrder()
	notifyIndex := uint64(0)
	indexToBlkKey := &sync.Map{}
	// lock for local go routines variables
	max := len(addrs)
	if max > consts.MAX_GOROUTINES_FOR_WORK_TASK {
		max = consts.MAX_GOROUTINES_FOR_WORK_TASK
	}

	flightMap := &sync.Map{}
	blockCache := &sync.Map{}
	getBlockCacheLen := func() int {
		len := int(0)
		blockCache.Range(func(k, v interface{}) bool {
			len++
			return true
		})
		return len
	}

	blockIndexKey := func(hash string, index uint64) string {
		return fmt.Sprintf("%s-%d", hash, index)
	}

	type job struct {
		req       []*types.GetBlockReq
		worker    *base.Worker
		flightKey []string
	}
	jobCh := make(chan *job, max)

	type getBlocksResp struct {
		worker        *base.Worker
		flightKey     []string
		failedFlights []*types.GetBlockReq
		ret           []*types.BlockResp
		err           error
	}
	dropDoneCh := uint32(0)

	done := make(chan *getBlocksResp, 1)
	doneNotify := make(chan bool)
	maxFlightLen := this.GetMaxFlightLen()
	log.Debugf("task %s, max flight len %d", this.GetId(), maxFlightLen)
	// trigger to start
	go this.notifyBlockReqPoolLen()
	go func() {
		for {
			if this.State() == store.TaskStateDone {
				log.Debugf("task job break because task is done")
				close(jobCh)
				atomic.AddUint32(&dropDoneCh, 1)
				close(doneNotify)
				break
			}
			if this.State() == store.TaskStatePause || this.State() == store.TaskStateFailed || this.State() == store.TaskStateNone {
				log.Debugf("task job break because task is pause, failed or none")
				close(jobCh)
				break
			}
			// check pool has item or no
			log.Debugf("wait for block pool notify")
			select {
			case <-this.blockReqPoolNotify:
			case <-time.After(time.Duration(3) * time.Second):
			}
			reqPoolLen := this.GetBlockReqPoolLen()
			log.Debugf("wait for block pool notify done %d", reqPoolLen)
			if reqPoolLen == 0 {
				log.Debugf("req pool is empty but state is %d", this.State())
				continue
			}
			// get the idle request
			var req []*types.GetBlockReq
			var flightKey string
			var flights []string
			pool := this.GetBlockReqPool()
			for _, r := range pool {
				flightKey = blockIndexKey(r.Hash, r.Index)
				if _, ok := flightMap.Load(flightKey); ok {
					continue
				}
				if _, ok := blockCache.Load(flightKey); ok {
					continue
				}
				req = append(req, r)
				flights = append(flights, flightKey)
				if len(req) == maxFlightLen {
					break
				}
			}
			if req == nil {
				continue
			}
			// get next index idle worker
			worker := this.GetIdleWorker(addrs, fileHash, req[0].Hash)
			if worker == nil {
				// can't find a valid worker
				log.Debugf("no idle workers of flights %s-%s to %s-%s",
					fileHash, flights[0], fileHash, flights[len(flights)-1])
				continue
			}
			for _, v := range flights {
				flightMap.Store(v, struct{}{})
			}
			if len(flights) > 0 {
				log.Debugf("add flight %s-%s to %s-%s, worker %s",
					fileHash, flights[0], fileHash, flights[len(flights)-1], worker.WalletAddr())
			}
			this.DelBlockReqFromPool(req)
			this.SetWorkerWorking(worker.WalletAddr(), true)
			jobCh <- &job{
				req:       req,
				flightKey: flights,
				worker:    worker,
			}
		}
		log.Debugf("outside for loop")
	}()

	go func() {
		for {
			if this.State() == store.TaskStateDone {
				log.Debugf("receive job task is done break")
				break
			}
			select {
			case resp, ok := <-done:
				if !ok {
					log.Debugf("done channel has close")
					break
				}
				// delete failed key
				for _, req := range resp.failedFlights {
					flightMap.Delete(blockIndexKey(req.Hash, req.Index))
				}
				for k, v := range resp.flightKey {
					if resp.err != nil {
						flightMap.Delete(v)
						// remove the request from flight
						log.Errorf("worker %s do job err continue %s", resp.worker.WalletAddr(), resp.err)
						continue
					}
					respBlk := resp.ret[k]
					if respBlk == nil {
						continue
					}
					blockCache.Store(v, respBlk)
					indexToBlkKey.Store(respBlk.Index, v)
					flightMap.Delete(v)
				}
				go func() {
					log.Debugf("notify when block reqs receive response", resp.flightKey)
					this.blockReqPoolNotify <- struct{}{}
					log.Debugf("notify when block reqs  receive response done")
				}()
				if resp.err != nil {
					break
				}
				if len(resp.flightKey) > 0 {
					log.Debugf("store flight from %s to %s from peer %s",
						resp.flightKey[0], resp.flightKey[len(resp.flightKey)-1], resp.worker.WalletAddr())
				}
				// notify outside
				if inOrder {
					for {
						blkKey, ok := indexToBlkKey.Load(notifyIndex)
						if !ok {
							break
						}
						blktemp, ok := blockCache.Load(blkKey)
						if !ok {
							log.Warnf("continue because block cache not has %v!", blkKey)
							break
						}
						blk, ok := blktemp.(*types.BlockResp)
						if !ok {
							break
						}
						this.NotifyBlock(blk)
						blockCache.Delete(blkKey)
						indexToBlkKey.Delete(notifyIndex)
						notifyIndex++
					}
				} else {
					for _, blkKey := range resp.flightKey {
						blktemp, ok := blockCache.Load(blkKey)
						if !ok {
							log.Warnf("continue because block cache not has %v!", blkKey, resp.flightKey)
							continue
						}
						blk := blktemp.(*types.BlockResp)
						this.NotifyBlock(blk)
						blockCache.Delete(blkKey)
						indexToBlkKey.Delete(blk.Index)
					}
				}
				log.Debugf("remain %d response at block cache", getBlockCacheLen())
			}
			if this.State() == store.TaskStatePause || this.State() == store.TaskStateFailed {
				log.Debugf("receive state %d", this.State())
				atomic.AddUint32(&dropDoneCh, 1)
				close(doneNotify)
				break
			}
		}
		log.Debugf("outside receive job task")
	}()

	// open max routine to do jobs
	log.Debugf("open %d routines to work task %s, file %s background", max, this.GetId(), fileHash)
	for i := 0; i < max; i++ {
		go func() {
			for {
				select {
				case <-doneNotify:
					log.Debugf("recv done notify")
					close(done)
					return
				default:
					state := this.State()
					if state == store.TaskStateDone || state == store.TaskStatePause || state == store.TaskStateFailed {
						log.Debugf("task is break, state: %d", state)
						return
					}
					job, ok := <-jobCh
					if !ok {
						log.Debugf("job channel has close")
						return
					}
					flights := make([]*block.Block, 0)
					sessionId := this.GetSessionId(job.worker.WalletAddr())
					if len(sessionId) == 0 {
						log.Warnf("task %s get session id failed of peer %s", this.GetId(), job.worker.WalletAddr())
					}
					for _, v := range job.req {
						b := &block.Block{
							SessionId: sessionId,
							Index:     v.Index,
							FileHash:  v.FileHash,
							Hash:      v.Hash,
							Operation: netCom.BLOCK_OP_GET,
							Payment: &payment.Payment{
								Sender: tskWalletAddr,
								Asset:  consts.ASSET_USDT,
							},
						}
						flights = append(flights, b)
					}
					log.Debugf("request blocks %s-%s-%d to %s-%d to %s, peer wallet: %s, length %d",
						fileHash, job.req[0].Hash, job.req[0].Index, job.req[len(job.req)-1].Hash,
						job.req[len(job.req)-1].Index, job.worker.WalletAddr(), job.worker.WalletAddr(), len(job.req))
					ret, err := job.worker.Do(this.GetId(), fileHash, job.worker.WalletAddr(), flights)
					this.SetWorkerWorking(job.worker.WalletAddr(), false)
					if err != nil {
						if serr, ok := err.(*sdkErr.Error); ok && serr.Code == sdkErr.PAY_UNPAID_BLOCK_FAILED {
							log.Errorf("request blocks paid failed %v from %s, err %s",
								job.req, job.worker.WalletAddr(), err)
						} else {
						}
						if len(job.req) > 0 {
							log.Errorf("request blocks %s of %s to %s from %s, err %s",
								fileHash, job.req[0].Hash, job.req[len(job.req)-1].Hash, job.worker.WalletAddr(), err)
						} else {
							log.Errorf("request blocks %v from %s, err %s", job.req, job.worker.WalletAddr(), err)
						}
					} else {
						log.Debugf("request blocks %s-%s to %s from %s success",
							fileHash, ret[0].Hash, ret[len(ret)-1].Hash, job.worker.WalletAddr())
					}
					stop := atomic.LoadUint32(&dropDoneCh) > 0
					if stop {
						log.Debugf("stop when drop channel is not 0")
						return
					}
					successFlightKeys := make([]string, 0)
					failedFlightReqs := make([]*types.GetBlockReq, 0)
					successFlightMap := make(map[string]struct{}, 0)
					for _, v := range ret {
						key := blockIndexKey(v.Hash, v.Index)
						successFlightMap[key] = struct{}{}
						successFlightKeys = append(successFlightKeys, key)
					}
					if len(successFlightKeys) != len(job.req) {
						for _, r := range job.req {
							if _, ok := successFlightMap[blockIndexKey(r.Hash, r.Index)]; ok {
								continue
							}
							failedFlightReqs = append(failedFlightReqs, r)
						}
					}
					if len(successFlightKeys) > 0 {
						log.Debugf("push success flightskey from %s to %s",
							successFlightKeys[0],
							successFlightKeys[len(successFlightKeys)-1])
					}
					if len(failedFlightReqs) > 0 {
						log.Debugf("push failed flightskey from %s-%d to %s-%d",
							failedFlightReqs[0].Hash, failedFlightReqs[0].Index,
							failedFlightReqs[len(failedFlightReqs)-1].Hash, failedFlightReqs[len(failedFlightReqs)-1].Index)
					}

					resp := &getBlocksResp{
						worker:        job.worker,        // worker info
						flightKey:     successFlightKeys, // success flight keys
						failedFlights: failedFlightReqs,  // failed flight keys
						ret:           ret,               // success flight response
						err:           err,               // job error
					}
					if len(failedFlightReqs) > 0 {
						this.InsertBlockReqToPool(failedFlightReqs)
					}
					stop = atomic.LoadUint32(&dropDoneCh) > 0
					if stop {
						log.Debugf("stop when drop channel is not 0")
						return
					}
					done <- resp
				}
			}
		}()
	}
}

// receiveBlockNoOrder. receive blocks in order
func (this *DownloadTask) receiveBlockNoOrder(peerAddrWallet []string) error {

	if this.DB.IsFileDownloaded(this.GetId()) {
		log.Debugf("task %s, file %s has downloaded", this.GetId(), this.GetFileHash())
		return nil
	}
	if err := this.addDownloadBlockReq(); err != nil {
		return sdkErr.NewWithError(sdkErr.GET_UNDOWNLOADED_BLOCK_FAILED, err)
	}
	hasCutPrefix := false
	prefix := string(this.GetPrefix())
	if len(prefix) == 0 {
		log.Warnf("task %s file prefix is empty", this.GetId())
	}
	isFileEncrypted := uPrefix.GetPrefixEncrypted(this.GetPrefix())
	//var file *os.File
	//if this.Mgr.IsClient() {
	//	var createFileErr error
	//	file, createFileErr = createDownloadFile(this.Mgr.Config().FsFileRoot, this.GetFilePath())
	//	if createFileErr != nil {
	//		return sdkErr.NewWithError(sdkErr.CREATE_DOWNLOAD_FILE_FAILED, createFileErr)
	//	}
	//	defer func() {
	//		if err := file.Close(); err != nil {
	//			log.Errorf("close file err %s", err)
	//		}
	//	}()
	//}
	//if file == nil {
	//	return sdkErr.New(sdkErr.CREATE_DOWNLOAD_FILE_FAILED, "create file failed")
	//}
	stateCheckTicker := time.NewTicker(time.Duration(consts.TASK_STATE_CHECK_DURATION) * time.Second)
	defer stateCheckTicker.Stop()
	hasDir := false
	for {
		select {
		case value, ok := <-this.GetTaskNotify():
			if !ok {
				return sdkErr.New(sdkErr.DOWNLOAD_BLOCK_FAILED, "download internal error")
			}
			if stop := this.IsTaskStop(); stop {
				return sdkErr.New(sdkErr.DOWNLOAD_BLOCK_FAILED, "download task %s is stop", this.GetId())
			}
			log.Debugf("received block %s-%s-%d from %s", this.GetFileHash(), value.Hash, value.Index, value.PeerAddr)
			if this.DB.IsBlockDownloaded(this.GetId(), value.Hash, uint64(value.Index)) &&
				!this.DB.IsFileDownloaded(this.GetId()) {
				log.Debugf("%s-%s-%d is downloaded", this.GetFileHash(), value.Hash, value.Index)
				continue
			}
			block := this.Mgr.Fs().EncodedToBlockWithCid(value.Block, value.Hash)
			links, err := this.Mgr.Fs().GetBlockLinks(block)
			if err != nil {
				return err
			}
			if block.Cid().String() != value.Hash {
				log.Warnf("receive a unmatched hash block %s %s", block.Cid().String(), value.Hash)
			}
			if block.Cid().String() == this.GetFileHash() && this.Mgr.IsClient() {
				if !isFileEncrypted {
					this.Mgr.Fs().SetFsFilePrefix(this.GetFilePath(), prefix)
				} else {
					this.Mgr.Fs().SetFsFilePrefix(this.GetFilePath(), "")
				}
			}
			if len(links) > 0 && this.Mgr.IsClient() {
				hasDir = true
				dirPath := filepath.Join(this.Mgr.Config().FsFileRoot, block.Cid().String())
				this.SetFilePath(dirPath)
			}
			if len(links) == 0 && this.Mgr.IsClient() {
				// TODO wangyu reduce create file times
				var file *os.File
				if this.Mgr.IsClient() {
					var createFileErr error
					if hasDir {
						filePath := filepath.Join(this.GetFilePath(), block.Cid().String())
						file, createFileErr = createDownloadFile(this.GetFilePath(), filePath)
					} else {
						file, createFileErr = createDownloadFile(this.Mgr.Config().FsFileRoot, this.GetFilePath())
					}
					if createFileErr != nil {
						return sdkErr.NewWithError(sdkErr.CREATE_DOWNLOAD_FILE_FAILED, createFileErr)
					}
					defer func() {
						if err := file.Close(); err != nil {
							log.Errorf("close file err %s", err)
						}
					}()
				}

				data := this.Mgr.Fs().BlockData(block)
				// cut prefix
				// TEST: why not use filesize == 0
				if !isFileEncrypted && !hasCutPrefix && len(data) >= len(prefix) &&
					string(data[:len(prefix)]) == prefix {
					log.Debugf("cut prefix data-len %d, prefix %s, prefix-len: %d, str %s",
						len(data), prefix, len(prefix), string(data[:len(prefix)]))
					data = data[len(prefix):]
					hasCutPrefix = true
				}
				// TEST: offset
				writeAtPos := value.Offset
				if value.Offset > 0 && !isFileEncrypted {
					writeAtPos = value.Offset - int64(len(prefix))
				}
				log.Debugf("file %s append block %s index %d, the block data length %d, offset %v, pos %d",
					this.GetFileHash(), value.Index,
					block.Cid().String(), len(data), value.Offset, writeAtPos)
				if _, err := file.WriteAt(data, writeAtPos); err != nil {
					return sdkErr.NewWithError(sdkErr.WRITE_FILE_DATA_FAILED, err)
				}
			}
			if this.Mgr.IsClient() {
				err = this.Mgr.Fs().PutBlockForFileStore(this.GetFilePath(), block, uint64(value.Offset))
				log.Debugf("put block for store err %v", err)
			} else {
				err = this.Mgr.Fs().PutBlock(block)
				if err != nil {
					return err
				}
				log.Debugf("block %s value.index %d, value.tag:%d", block.Cid(), value.Index, len(value.Tag))
				err = this.Mgr.Fs().PutTag(block.Cid().String(), this.GetFileHash(), uint64(value.Index), value.Tag)
			}
			log.Debugf("put block for file %s block: %s, offset:%d", this.GetFilePath(), block.Cid(), value.Offset)
			if err != nil {
				log.Errorf("put block err %s", err)
				return err
			}
			if err := this.SetBlockDownloaded(this.GetId(), value.Hash, value.PeerAddr, value.Index,
				value.Offset, links); err != nil {
				return err
			}
			this.EmitProgress(types.TaskDownloadFileDownloading)
			log.Debugf("%s-%s-%d set downloaded", this.GetFileHash(), value.Hash, value.Index)
			poolLen := this.GetBlockReqPoolLen()

			if poolLen != 0 {
				continue
			}
			if !this.DB.IsFileDownloaded(this.GetId()) {
				continue
			}
			log.Debugf("file has downloaded: %t, last block index: %d",
				this.DB.IsFileDownloaded(this.GetId()), value.Index)
			// last block
			this.SetTaskState(store.TaskStateDone)
			break
		case <-stateCheckTicker.C:
			if this.IsTaskStop() {
				return sdkErr.New(sdkErr.DOWNLOAD_BLOCK_FAILED, "download task %s is stop", this.GetId())
			}
			if !this.IsTimeout() {
				continue
			}
			if this.DB.IsFileDownloaded(this.GetId()) {
				log.Warnf("task %s is timeout but file has downloed", this.GetId())
				break
			}
			// this.taskMgr.SetTaskState(this.GetId(), store.TaskStateFailed)
			workerState := this.GetWorkerState()
			for addr, state := range workerState {
				log.Debugf("download timeout worker addr: %s, working : %t, unpaid: %t, totalFailed %v",
					addr, state.Working, state.Unpaid, state.TotalFailed)
			}
			if paidFail := this.AllPeerPaidFailed(); paidFail {
				log.Errorf("Download file %s failed, pay failed to all peers", this.GetId())
				return sdkErr.New(sdkErr.DOWNLOAD_FILE_TIMEOUT, "Download file failed, pay failed to all peers")
			}
			return sdkErr.New(sdkErr.DOWNLOAD_FILE_TIMEOUT, "Download file timeout")
		}
		done := this.IsTaskDone()

		if done {
			log.Debugf("download task is done")
			break
		}
	}
	this.EmitProgress(types.TaskDownloadFileMakeSeed)
	go this.Mgr.DNS().PushToTrackers(this.GetFileHash(), client.P2PGetPublicAddr())
	for _, walletAddr := range peerAddrWallet {
		sessionId := this.GetSessionId(walletAddr)
		if len(sessionId) == 0 {
			continue
		}
		go func(w, sid string) {
			fileDownloadOkMsg := message.NewFileMsg(this.GetFileHash(), netCom.FILE_OP_DOWNLOAD_OK,
				message.WithSessionId(sid),
				message.WithWalletAddress(this.GetCurrentWalletAddr()),
				message.WithAsset(this.GetAsset()),
				message.WithSign(this.Mgr.Chain().CurrentAccount()),
			)
			hostAddr, err := client.P2PGetHostAddrFromWalletAddr(w, client.P2PNetTypeDsp)
			if err != nil {
				log.Errorf("get host addr failed %s", err)
			}
			client.P2PBroadcast([]string{hostAddr}, fileDownloadOkMsg.ToProtoMsg(), fileDownloadOkMsg.MessageId)
		}(walletAddr, sessionId)
	}
	return nil
}

func (this *DownloadTask) addDownloadBlockReq() error {
	taskId := this.GetId()
	fileHashStr := this.GetFileHash()
	blks, err := this.DB.GetUndownloadedBlockInfo(taskId, fileHashStr)
	if err != nil {
		return err
	}
	log.Debugf("undownloaded hashes len %v", len(blks))
	reqs := make([]*types.GetBlockReq, 0)
	if len(blks) == 0 {
		// TODO: check bug
		if this.DB.IsFileDownloaded(taskId) {
			return nil
		}
		log.Warnf("all block has downloaded, but file not downloed")
		reqs = append(reqs, &types.GetBlockReq{
			FileHash: fileHashStr,
			Hash:     fileHashStr,
			Index:    0,
		})
		this.AddBlockReqToPool(reqs)
		return nil
	}
	log.Debugf("start download at %s-%s-%d", fileHashStr, blks[0].Hash, blks[0].Index)
	for _, blk := range blks {
		reqs = append(reqs, &types.GetBlockReq{
			FileHash: fileHashStr,
			Hash:     blk.Hash,
			Index:    blk.Index,
		})
		log.Debugf("append hash %s index %v", blk.Hash, blk.Index)
	}
	this.AddBlockReqToPool(reqs)
	return nil
}

func (this *DownloadTask) decryptDownloadedFile() error {
	if len(this.GetDecryptPwd()) == 0 {
		return sdkErr.New(sdkErr.DECRYPT_FILE_FAILED, "no decrypt password")
	}
	filePrefix, prefix, err := uPrefix.GetPrefixFromFile(this.GetFilePath())
	if err != nil {
		return sdkErr.New(sdkErr.DECRYPT_FILE_FAILED, err.Error())
	}
	if !uPrefix.VerifyEncryptPassword(this.GetDecryptPwd(), filePrefix.EncryptSalt, filePrefix.EncryptHash) {
		return sdkErr.New(sdkErr.DECRYPT_WRONG_PASSWORD, "wrong password")
	}
	newFilePath := uTask.GetDecryptedFilePath(this.GetFilePath(), this.GetFileName())
	if err := this.Mgr.Fs().AESDecryptFile(this.GetFilePath(), string(prefix),
		this.GetDecryptPwd(), newFilePath); err != nil {
		return sdkErr.New(sdkErr.DECRYPT_FILE_FAILED, err.Error())
	}
	log.Debugf("decrypt file success %s", newFilePath)
	// return this.taskMgr.SetTaskInfoWithOptions(taskId, task.FilePath(newFilePath))
	return nil
}

// // [Deprecated] startFetchBlocks for store node, fetch blocks one by one after receive fetch_rdy msg
// func (this *DownloadTask) startFetchBlocks(fileHashStr string, addr, peerWalletAddr string) error {
// 	// my task. use my wallet address
// 	taskId := this.taskMgr.TaskId(fileHashStr, this.GetCurrentWalletAddr(), store.TaskTypeDownload)
// 	log.Debugf("startFetchBlocks taskId: %s, fileHash: %s", taskId, fileHashStr)
// 	if this.DB.IsFileDownloaded(taskId) {
// 		log.Debugf("file has downloaded: %s", fileHashStr)
// 		return this.Mgr.Fs().StartPDPVerify(fileHashStr, 0, 0, 0, chainCom.ADDRESS_EMPTY)
// 	}
// 	sessionId, err := this.taskMgr.GetSessionId(taskId, peerWalletAddr)
// 	if err != nil {
// 		return err
// 	}
// 	if err = client.P2PConnect(addr); err != nil {
// 		return err
// 	}
// 	this.taskMgr.NewWorkers(taskId, []string{peerWalletAddr}, true, nil)
// 	pause, cancel := false, false
// 	defer func() {
// 		if cancel {
// 			return
// 		}
// 		err := this.Mgr.Fs().PinRoot(context.TODO(), fileHashStr)
// 		if err != nil {
// 			log.Errorf("pin root file err %s", err)
// 		}
// 	}()
// 	downloadQoS := make([]int64, 0)
// 	blocks := make([]*block.Block, 0, consts.MAX_REQ_BLOCK_COUNT)
// 	downloadBlkCap := consts.MIN_REQ_BLOCK_COUNT
// 	totalCount, err := this.taskMgr.GetFileTotalBlockCount(taskId)
// 	if err != nil {
// 		return err
// 	}
// 	blockHashes := make([]string, 0, totalCount)
// 	blockHashes = append(blockHashes, fileHashStr)
// 	for index := 0; index < len(blockHashes); index++ {
// 		hash := blockHashes[index]
// 		pause, cancel, err = this.taskMgr.IsTaskPauseOrCancel(taskId)
// 		if err != nil {
// 			return err
// 		}
// 		if pause || cancel {
// 			log.Debugf("fetch task break it pause: %t, cancel: %t", pause, cancel)
// 			break
// 		}
// 		if this.taskMgr.IsBlockDownloaded(taskId, hash, uint64(index)) {
// 			log.Debugf("block has downloaded %s %d", hash, index)
// 			blk := this.Mgr.Fs().GetBlock(hash)
// 			links, err := this.Mgr.Fs().GetBlockLinks(blk)
// 			if err != nil {
// 				log.Errorf("get block links err %s", err)
// 				return err
// 			}
// 			blockHashes = append(blockHashes, links...)
// 			continue
// 		}
// 		blocks = append(blocks, &block.Block{
// 			SessionId: sessionId,
// 			Index:     uint64(index),
// 			FileHash:  fileHashStr,
// 			Hash:      hash,
// 			Operation: netCom.BLOCK_OP_GET,
// 			Payment: &payment.Payment{
// 				Sender: this.GetCurrentWalletAddr(),
// 				Asset:  consts.ASSET_USDT,
// 			},
// 		})
// 		if index != len(blockHashes)-1 && len(blocks) < downloadBlkCap {
// 			continue
// 		}
// 		var resps []*task.BlockResp
// 		startDownload := time.Now().Unix()
// 		resps, err = this.downloadBlockFlights(taskId, fileHashStr, peerWalletAddr, blocks,
// 			consts.MAX_BLOCK_FETCHED_RETRY, consts.DOWNLOAD_FILE_TIMEOUT)
// 		if time.Now().Unix() <= startDownload {
// 			downloadQoS = append(downloadQoS, 0)
// 		} else {
// 			downloadQoS = append(downloadQoS, time.Now().Unix()-startDownload)
// 		}
// 		if err != nil {
// 			return err
// 		}
// 		if len(downloadQoS) >= consts.MIN_DOWNLOAD_QOS_LEN {
// 			// reduce slice
// 			downloadQoS = downloadQoS[len(downloadQoS)-consts.MIN_DOWNLOAD_QOS_LEN:]
// 			downloadBlkCap = adjustDownloadCap(downloadBlkCap, downloadQoS)
// 			log.Debugf("adjust new download cap: %d for file %s", downloadBlkCap, fileHashStr)
// 		}
// 		blocks = blocks[:0]

// 		for _, value := range resps {
// 			blk := this.Mgr.Fs().EncodedToBlockWithCid(value.Block, value.Hash)
// 			if blk.Cid().String() != value.Hash {
// 				return fmt.Errorf("receive a wrong block: %s, expected: %s", blk.Cid().String(), value.Hash)
// 			}
// 			err = this.Mgr.Fs().PutBlock(blk)
// 			if err != nil {
// 				return err
// 			}
// 			err = this.Mgr.Fs().PutTag(value.Hash, fileHashStr, uint64(value.Index), value.Tag)
// 			if err != nil {
// 				return err
// 			}
// 			err = this.taskMgr.SetBlockDownloaded(taskId, value.Hash, value.PeerAddr,
// 				value.Index, value.Offset, nil)
// 			if err != nil {
// 				log.Errorf("SetBlockDownloaded taskId:%s, %s-%s-%d-%d, err: %s",
// 					taskId, fileHashStr, value.Hash, value.Index, value.Offset, err)
// 				return err
// 			}
// 			log.Debugf("SetBlockDownloaded taskId:%s, %s-%s-%d-%d",
// 				taskId, fileHashStr, value.Hash, value.Index, value.Offset)
// 			links, err := this.Mgr.Fs().GetBlockLinks(blk)
// 			if err != nil {
// 				log.Errorf("get block links err %s", err)
// 				return err
// 			}
// 			blockHashes = append(blockHashes, links...)
// 			log.Debugf("blockHashes len: %d", len(blockHashes))
// 		}
// 	}
// 	if !this.DB.IsFileDownloaded(taskId) {
// 		return sdkErr.New(sdkErr.DOWNLOAD_BLOCK_FAILED, "all block has fetched, but file not download completely")
// 	}

// 	err = this.Mgr.Fs().SetFsFileBlockHashes(fileHashStr, blockHashes)
// 	if err != nil {
// 		log.Errorf("set file blockhashes err %s", err)
// 		return err
// 	}

// 	// all block is saved, prove it
// 	for i := 0; i < consts.MAX_START_PDP_RETRY; i++ {
// 		log.Infof("received all block, start pdp verify %s, retry: %d", fileHashStr, i)
// 		err = this.Mgr.Fs().StartPDPVerify(fileHashStr, 0, 0, 0, chainCom.ADDRESS_EMPTY)
// 		if err == nil {
// 			break
// 		}
// 		time.Sleep(time.Duration(consts.START_PDP_RETRY_DELAY) * time.Second)
// 	}
// 	if err != nil {
// 		log.Errorf("start pdp verify err %s", err)
// 		deleteErr := this.DeleteDownloadedFile(taskId)
// 		if deleteErr != nil {
// 			log.Errorf("delete download file err of PDP failed file %s", deleteErr)
// 			return deleteErr
// 		}
// 		return err
// 	}
// 	log.Debugf("push to trackers %v", this.dns.TrackerUrls)
// 	this.dns.PushToTrackers(fileHashStr, this.dns.TrackerUrls, client.P2PGetPublicAddr())
// 	// TODO: remove unused file info fields after prove pdp success
// 	this.taskMgr.EmitResult(taskId, "", nil)
// 	this.taskMgr.DeleteTask(taskId)
// 	return nil
// }

// func (this *DownloadTask) putBlocks(taskId, fileHashStr, peerAddr string, resps []*task.BlockResp) error {
// 	log.Debugf("putBlocks taskId: %s, fileHash: %s", taskId, fileHashStr)
// 	if len(fileHashStr) != 0 {
// 		defer this.Mgr.Fs().PinRoot(context.TODO(), fileHashStr)
// 	}
// 	if this.DB.IsFileDownloaded(taskId) {
// 		log.Debugf("file has downloaded: %s", fileHashStr)
// 		err := this.Mgr.Fs().StartPDPVerify(fileHashStr, 0, 0, 0, chainCom.ADDRESS_EMPTY)
// 		if err != nil {
// 			return err
// 		}
// 		// dispatch downloaded blocks to other primary nodes
// 		dispatchTaskId := this.taskMgr.TaskId(fileHashStr, this.WalletAddress(), store.TaskTypeUpload)
// 		go this.dispatchBlocks(dispatchTaskId, taskId, fileHashStr)
// 		return nil
// 	}

// 	blkInfos := make([]*store.BlockInfo, 0)
// 	for _, value := range resps {
// 		if len(value.Block) == 0 {
// 			log.Errorf("receive empty block %d of file %s", len(value.Block), fileHashStr)
// 			return fmt.Errorf("receive empty block %d of file %s", len(value.Block), fileHashStr)
// 		}
// 		blk := this.Mgr.Fs().EncodedToBlockWithCid(value.Block, value.Hash)
// 		if blk.Cid().String() != value.Hash {
// 			return fmt.Errorf("receive a wrong block: %s, expected: %s", blk.Cid().String(), value.Hash)
// 		}
// 		if len(value.Tag) == 0 {
// 			log.Errorf("receive empty tag %d of file %s and block %s",
// 				len(value.Tag), fileHashStr, blk.Cid().String())
// 			return fmt.Errorf("receive empty tag %d of file %s and block %s",
// 				len(value.Tag), fileHashStr, blk.Cid().String())
// 		}
// 		if err := this.Mgr.Fs().PutBlock(blk); err != nil {
// 			return err
// 		}
// 		log.Debugf("put block success %v-%s-%d", fileHashStr, value.Hash, value.Index)
// 		if err := this.Mgr.Fs().PutTag(value.Hash, fileHashStr, uint64(value.Index), value.Tag); err != nil {
// 			return err
// 		}
// 		log.Debugf(" put tag or done %s-%s-%d", fileHashStr, value.Hash, value.Index)
// 		blkInfos = append(blkInfos, &store.BlockInfo{
// 			Hash:       value.Hash,
// 			Index:      value.Index,
// 			DataOffset: uint64(value.Offset),
// 			NodeList:   []string{value.PeerAddr},
// 		})

// 	}
// 	if len(blkInfos) > 0 {
// 		if err := this.taskMgr.SetBlocksDownloaded(taskId, blkInfos); err != nil {
// 			log.Errorf("SetBlocksDownloaded taskId:%s, %s-%s-%d-%d to %s-%d-%d, err: %s",
// 				taskId, fileHashStr, blkInfos[0].Hash, blkInfos[0].Index, blkInfos[0].DataOffset,
// 				blkInfos[len(blkInfos)-1].Hash, blkInfos[len(blkInfos)-1].Index, blkInfos[len(blkInfos)-1].DataOffset,
// 				err)
// 			return err
// 		}
// 		log.Debugf("SetBlocksDownloaded taskId:%s, %s-%s-%d-%d to %s-%d-%d",
// 			taskId, fileHashStr, blkInfos[0].Hash, blkInfos[0].Index, blkInfos[0].DataOffset,
// 			blkInfos[len(blkInfos)-1].Hash, blkInfos[len(blkInfos)-1].Index, blkInfos[len(blkInfos)-1].DataOffset)
// 	}

// 	if !this.DB.IsFileDownloaded(taskId) {
// 		return nil
// 	}
// 	blockHashes := this.taskMgr.FileBlockHashes(taskId)
// 	log.Debugf("file block hashes %d", len(blockHashes))
// 	if err := this.Mgr.Fs().SetFsFileBlockHashes(fileHashStr, blockHashes); err != nil {
// 		log.Errorf("set file blockhashes err %s", err)
// 		return err
// 	}
// 	// all block is saved, prove it
// 	proved := false
// 	for i := 0; i < consts.MAX_START_PDP_RETRY; i++ {
// 		log.Infof("received all block, start pdp verify %s, retry: %d", fileHashStr, i)
// 		if err := this.Mgr.Fs().StartPDPVerify(fileHashStr, 0, 0, 0, chainCom.ADDRESS_EMPTY); err == nil {
// 			proved = true
// 			break
// 		} else {
// 			log.Errorf("start pdp verify err %s", err)
// 		}
// 		time.Sleep(time.Duration(consts.START_PDP_RETRY_DELAY) * time.Second)
// 	}
// 	if !proved {
// 		if err := this.DeleteDownloadedFile(taskId); err != nil {
// 			return err
// 		}
// 		return nil
// 	}
// 	log.Debugf("pdp verify %s success, push the file to trackers %v", fileHashStr, this.dns.TrackerUrls)
// 	this.dns.PushToTrackers(fileHashStr, this.dns.TrackerUrls, client.P2PGetPublicAddr())
// 	// TODO: remove unused file info fields after prove pdp success
// 	this.taskMgr.EmitResult(taskId, "", nil)
// 	this.taskMgr.DeleteTask(taskId)
// 	if err := this.Mgr.Fs().PinRoot(context.TODO(), fileHashStr); err != nil {
// 		log.Errorf("pin root file err %s", err)
// 		return err
// 	}
// 	// dispatch downloaded blocks to other primary nodes
// 	go this.dispatchBlocks("", taskId, fileHashStr)
// 	doneMsg := message.NewFileMsg(fileHashStr, netCom.FILE_OP_FETCH_DONE,
// 		message.WithSessionId(taskId),
// 		message.WithWalletAddress(this.GetCurrentWalletAddr()),
// 		message.WithSign(this.Mgr.Chain().CurrentAccount()),
// 	)
// 	return client.P2PSend(peerAddr, doneMsg.MessageId, doneMsg.ToProtoMsg())
// }
