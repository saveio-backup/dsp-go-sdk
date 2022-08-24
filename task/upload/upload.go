package upload

import (
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/saveio/dsp-go-sdk/actor/client"
	"github.com/saveio/dsp-go-sdk/consts"
	sdkErr "github.com/saveio/dsp-go-sdk/error"
	netCom "github.com/saveio/dsp-go-sdk/network/common"
	"github.com/saveio/dsp-go-sdk/network/message"
	"github.com/saveio/dsp-go-sdk/network/message/types/block"
	"github.com/saveio/dsp-go-sdk/network/message/types/file"
	"github.com/saveio/dsp-go-sdk/store"
	"github.com/saveio/dsp-go-sdk/task/base"
	"github.com/saveio/dsp-go-sdk/task/types"
	"github.com/saveio/dsp-go-sdk/types/prefix"
	uAddr "github.com/saveio/dsp-go-sdk/utils/addr"
	"github.com/saveio/dsp-go-sdk/utils/crypto"
	chainCom "github.com/saveio/themis/common"
	"github.com/saveio/themis/common/log"
	"github.com/saveio/themis/crypto/keypair"
	fs "github.com/saveio/themis/smartcontract/service/native/savefs"
	"github.com/saveio/themis/smartcontract/service/native/savefs/pdp"
)

// Start. start upload task
// Phase1: check upload option validation
// Phase2: make block split by using fs module
// Phase3: pay for file if it haven't been paid
// Phase4: find receivers with ask-ack msg
// Phase5: setup primary nodes and candidate nodes
// Phase6: send blocks and wait for pdp prove
// Phase7: continuesly request dispatch progress to master node
func (this *UploadTask) Start(newTask bool, taskId, filePath string, opt *fs.UploadOption) (
	*types.UploadResult, error) {
	file, err := os.Stat(filePath)
	if err != nil {
		return nil, sdkErr.NewWithError(sdkErr.INTERNAL_ERROR, err)
	}
	if newTask {
		if err := this.SetTaskState(store.TaskStateDoing); err != nil {
			return nil, err
		}
		this.EmitProgress(types.TaskCreate)
	}
	this.EmitProgress(types.TaskUploadFileMakeSlice)
	// check options valid
	if err = uploadOptValid(filePath, opt); err != nil {
		return nil, err
	}
	checksum := ""
	var fileType uint8
	if file.IsDir() {
		fileType = prefix.FILETYPE_DIR
		checksum, err = crypto.GetSimpleChecksumOfDir(filePath)
		if err != nil {
			return nil, sdkErr.New(sdkErr.INVALID_PARAMS, err.Error())
		}
	} else {
		fileType = prefix.FILETYPE_FILE
		// calculate check sum for setting to DB
		checksum, err = crypto.GetSimpleChecksumOfFile(filePath)
		if err != nil {
			return nil, sdkErr.New(sdkErr.INVALID_PARAMS, err.Error())
		}
	}
	if err = this.SetInfoWithOptions(
		base.FilePath(filePath),
		base.SimpleCheckSum(checksum),
		base.StoreType(uint32(opt.StorageType)),
		base.RealFileSize(opt.FileSize),
		base.ProveInterval(opt.ProveInterval),
		base.ProveLevel(opt.ProveLevel),
		base.ExpiredHeight(opt.ExpiredHeight),
		base.Privilege(opt.Privilege),
		base.CopyNum(uint32(opt.CopyNum)),
		base.Encrypt(opt.Encrypt),
		base.EncryptPassword(opt.EncryptPassword),
		base.EncryptNodeAddr(opt.EncryptNodeAddr),
		base.RegisterDNS(opt.RegisterDNS),
		base.BindDNS(opt.BindDNS),
		base.WhiteList(fsWhiteListToWhiteList(opt.WhiteList)),
		base.Share(opt.Share),
		base.StoreType(uint32(opt.StorageType)),
		base.Url(string(opt.DnsURL)),
		base.FileName(string(opt.FileDesc)),
		base.Walletaddr(this.Mgr.Chain().WalletAddress()),
		base.CopyNum(uint32(opt.CopyNum))); err != nil {
		return nil, err
	}

	this.EmitProgress(types.TaskUploadFileMakeSlice)
	var tx, fileHashStr, prefixStr, blocksRoot string
	var totalCount uint64
	log.Debugf("upload task %s, will split for file, prove level %v", taskId, opt.ProveLevel)

	// check if task is paused
	if this.IsTaskPaused() {
		return nil, this.sendPauseMsg()
	}

	if this.IsTaskCancel() {
		return nil, sdkErr.New(sdkErr.INTERNAL_ERROR, "task is cancel")
	}

	tx = this.GetStoreTx()
	var hashes []string
	// sharing file or get hashes from DB
	if len(tx) == 0 {
		// file not paid
		eType := prefix.ENCRYPTTYPE_NONE
		if len(opt.EncryptPassword) > 0 {
			eType = prefix.ENCRYPTTYPE_AES
		}
		if len(opt.EncryptNodeAddr) > 0 {
			eType = prefix.ENCRYPTTYPE_ECIES
		}
		filePrefix := &prefix.FilePrefix{
			Version:     prefix.PREFIX_VERSION,
			FileType:    fileType,
			Encrypt:     opt.Encrypt,
			EncryptPwd:  string(opt.EncryptPassword),
			EncryptType: uint8(eType),
			Owner:       this.Mgr.Chain().Address(),
			FileSize:    opt.FileSize,
			FileName:    string(opt.FileDesc),
		}
		_ = filePrefix.MakeSalt()
		prefixStr = filePrefix.String()
		log.Debugf("node from file prefix: %v, len: %d", prefixStr, len(prefixStr))
		var publicKey keypair.PublicKey
		if eType == prefix.ENCRYPTTYPE_ECIES {
			pubKey, err := this.Mgr.DNS().GetNodePubKey(string(opt.EncryptNodeAddr))
			if err != nil {
				log.Errorf("get node pub key failed, %v", err)
				return nil, err
			}
			publicKey, err = keypair.DeserializePublicKey(pubKey)
			if err != nil {
				log.Errorf("deserialize public key failed, %v", err)
				return nil, err
			}
		}
		if !file.IsDir() {
			hashes, err = this.Mgr.Fs().NodesFromFile(filePath, prefixStr, opt.Encrypt, string(opt.EncryptPassword), publicKey)
			if err != nil {
				return nil, err
			}
			blocksRoot = crypto.ComputeStringHashRoot(hashes)
			fileHashStr = hashes[0]
		} else {
			hashes, err = this.Mgr.Fs().NodesFromDir(filePath, prefixStr, opt.Encrypt, string(opt.EncryptPassword), publicKey)
			if err != nil {
				log.Errorf("get nodes from dir failed, %v", err)
				return nil, err
			}
			blocksRoot = crypto.ComputeStringHashRoot(hashes)
			fileHashStr = hashes[0]
		}
	} else {
		// file has paid, get prefix, fileHash etc.
		prefixStr = string(this.GetPrefix())
		fileHashStr = this.GetFileHash()
		log.Debugf("prefix of file %s is %v, its len: %d", fileHashStr, prefixStr, len(prefixStr))
		hashes, err = this.Mgr.Fs().GetFileAllHashes(fileHashStr)
		if err != nil {
			return nil, err
		}
		blocksRoot = this.GetBlocksRoot()
	}
	if len(prefixStr) == 0 {
		return nil, sdkErr.New(sdkErr.SHARDING_FAIELD, "missing file prefix")
	}
	if totalCount = uint64(len(hashes)); totalCount == 0 {
		return nil, sdkErr.New(sdkErr.SHARDING_FAIELD, "no blocks to upload")
	}

	log.Debugf("sharding file finished, taskId:%s, path: %s, fileHash: %s, totalCount:%d, blocksRoot: %x",
		taskId, filePath, hashes[0], totalCount, blocksRoot)
	this.EmitProgress(types.TaskUploadFileMakeSliceDone)

	if err = this.SetInfoWithOptions(
		base.FileHash(fileHashStr),
		base.BlocksRoot(blocksRoot),
		base.TotalBlockCnt(totalCount),
		base.FileSize(getFileSizeWithBlockCount(totalCount)),
		base.Prefix(prefixStr)); err != nil {
		return nil, err
	}
	fileInfo, _ := this.Mgr.Chain().GetFileInfo(fileHashStr)
	log.Debugf("get file info of %s is %v", fileHashStr, fileInfo)
	if newTask && (fileInfo != nil || this.Mgr.PauseDuplicatedUploadTask(taskId, fileHashStr)) {
		log.Errorf("cancel the task???")
		return nil, sdkErr.New(sdkErr.UPLOAD_TASK_EXIST, "file has uploading or uploaded, please cancel the task 1")
	}
	log.Debugf("check if file is uploaded %v", taskId)

	// check has uploaded this file
	if this.DB.IsFileUploaded(taskId, false) {
		log.Debugf("upload task %s already uploaded, publish it", taskId)
		return this.publishFile(opt)
	}

	log.Debugf("check if file is proved %v", taskId)
	if this.isFileProved(fileHashStr) {
		// file hasn't uploaded finish, but it has been proved, maybe something wrong
		log.Warnf("file %s has been proved, but it should not upload finish from db record", fileHashStr)
		return this.publishFile(opt)
	}
	log.Debugf("upload task %s isn't uploaded", taskId)

	fileID := GetFileIDFromFileHash(fileHashStr)
	tags, err := this.GeneratePdpTags(hashes, fileID, nil)
	if err != nil {
		log.Errorf("generate pdp tags failed, %v", err)
		return nil, err
	}
	if this.IsTaskPaused() {
		return nil, this.sendPauseMsg()
	}
	if this.IsTaskCancel() {
		return nil, sdkErr.New(sdkErr.INTERNAL_ERROR, "task is cancel")
	}
	tagsRoot, err := this.GetMerkleRootForTag(fileID, tags)
	if err != nil {
		return nil, err
	}
	if this.IsTaskPaused() {
		return nil, this.sendPauseMsg()
	}
	if this.IsTaskCancel() {
		return nil, sdkErr.New(sdkErr.INTERNAL_ERROR, "task is cancel")
	}
	// pay file
	if fileInfo != nil {
		// file has paid
		walletAddrs, err := this.findReceivers(fileInfo.PrimaryNodes.AddrList)
		if err != nil {
			return nil, sdkErr.NewWithError(sdkErr.SEARCH_RECEIVERS_FAILED, err)
		}
		log.Debugf("file %s find receivers %v", fileHashStr, walletAddrs)
		if err := this.setupTaskFieldByFileInfo(fileInfo); err != nil {
			return nil, err
		}
	} else {
		walletAddrs, err := this.findReceivers(nil)
		if err != nil {
			log.Errorf("find receivers failed, %v", err)
			return nil, sdkErr.NewWithError(sdkErr.SEARCH_RECEIVERS_FAILED, err)
		}
		log.Debugf("file %s find receivers %v", fileHashStr, walletAddrs)
		if err := this.payFile(fileID, tagsRoot, walletAddrs); err != nil {
			// rollback transfer state
			log.Errorf("task %s pay for file %s failed %s", taskId, fileHashStr, err)
			this.EmitProgress(types.TaskUploadFileFindReceivers)
			return nil, sdkErr.NewWithError(sdkErr.PAY_FOR_STORE_FILE_FAILED, err)
		}
	}

	if this.IsTaskPaused() {
		return nil, this.sendPauseMsg()
	}
	if this.IsTaskCancel() {
		return nil, sdkErr.New(sdkErr.INTERNAL_ERROR, "task is cancel")
	}
	this.EmitProgress(types.TaskUploadFilePayingDone)
	if _, err = this.addWhitelist(taskId, fileHashStr, opt); err != nil {
		return nil, err
	}
	if opt != nil && opt.Share {
		// TODOV2: enable share uploaded task
		// go func() {
		// 	if err := this.shareUploadedFile(filePath, string(opt.FileDesc), this.Mgr.Chain().WalletAddress(),
		// 		hashes); err != nil {
		// 		log.Errorf("share upload fle err %s", err)
		// 	}
		// }()
	}
	if !opt.Share && opt.Encrypt {
		// TODO: delete encrypted block store
	}
	if this.IsTaskPaused() {
		return nil, this.sendPauseMsg()
	}
	if this.IsTaskCancel() {
		return nil, sdkErr.New(sdkErr.INTERNAL_ERROR, "task is cancel")
	}
	this.EmitProgress(types.TaskUploadFileFindReceiversDone)
	if err = this.sendBlocks(hashes); err != nil {
		log.Errorf("send blocks err %s", err)
		return nil, err
	}
	log.Debugf("wait for fetch blocks finish id: %s ", taskId)
	if this.IsTaskPaused() {
		return nil, this.sendPauseMsg()
	}
	if this.IsTaskCancel() {
		return nil, sdkErr.New(sdkErr.INTERNAL_ERROR, "task is cancel")
	}

	uploadRet, err := this.publishFile(opt)
	if err != nil {
		return nil, err
	}
	log.Debug("upload success result %v", uploadRet)
	return uploadRet, nil
}

func (this *UploadTask) Pause() error {
	if err := this.Task.Pause(); err != nil {
		return err
	}
	// wait for all routine exit

	// emit new progress state
	this.EmitProgress(types.TaskPause)
	return nil
}

// Resume. resume upload task
func (this *UploadTask) Resume() error {
	taskId := this.GetId()
	log.Debugf("resume upload task %s", taskId)

	// call super.resume()
	if err := this.Task.Resume(); err != nil {
		log.Errorf("resume task %s err %s", taskId, err)
		return err
	}
	this.EmitProgress(types.TaskDoing)

	opt := &fs.UploadOption{
		FileDesc:        []byte(this.GetFileName()),
		FileSize:        this.GetRealFileSize(),
		ProveInterval:   this.GetProveInterval(),
		ProveLevel:      this.GetProveLevel(),
		ExpiredHeight:   this.GetExpiredHeight(),
		Privilege:       this.GetPrivilege(),
		CopyNum:         uint64(this.GetCopyNum()),
		Encrypt:         this.GetEncrypt(),
		EncryptPassword: this.GetEncryptPassword(),
		RegisterDNS:     this.GetRegisterDNS(),
		BindDNS:         this.GetBindDNS(),
		DnsURL:          []byte(this.GetUrl()),
		WhiteList:       whiteListToFsWhiteList(this.GetWhiteList()),
		Share:           this.GetShare(),
		StorageType:     uint64(this.GetStoreType()),
	}

	fileHashStr := this.GetFileHash()
	// send resume msg
	if len(fileHashStr) == 0 {
		go func() {
			uploadResult, err := this.Start(false, this.GetId(), this.GetFilePath(), opt)
			var serr *sdkErr.Error
			if err != nil {
				serr, _ = err.(*sdkErr.Error)
			}
			if uploadResult != nil {
				this.Mgr.EmitUploadResult(taskId, uploadResult, serr)
				// delete task from cache in the end when success
				if serr == nil {
					this.Mgr.DeleteUploadTask(taskId)
				}
			}
		}()
		return nil
	}
	if exist, err := this.DB.ExistSameUploadTaskInfo(taskId, fileHashStr); err != nil || exist {
		log.Debugf("exist same task err %v, exist %v", err, exist)
		return sdkErr.New(sdkErr.UPLOAD_TASK_EXIST, "file has uploading or uploaded, please cancel the task 2")
	}
	fileInfo, _ := this.Mgr.Chain().GetFileInfo(fileHashStr)
	if fileInfo != nil {
		// check if the task is expired or proved
		now, err := this.Mgr.Chain().GetCurrentBlockHeight()
		if err != nil {
			return err
		}
		if fileInfo.ExpiredHeight <= uint64(now) {
			return sdkErr.New(sdkErr.FILE_IS_EXPIRED, "file:%s has expired", fileHashStr)
		}
		hasProvedFile := uint64(len(this.getFileProvedNode(fileHashStr))) == opt.CopyNum+1
		if hasProvedFile || this.DB.IsFileUploaded(taskId, false) {
			log.Debugf("hasProvedFile: %t ", hasProvedFile)
			uploadResult, err := this.publishFile(opt)
			if err != nil {
				return err
			}
			log.Debug("upload success result %v", uploadResult)
			if uploadResult != nil {
				this.Mgr.EmitUploadResult(taskId, uploadResult, nil)
				// delete task from cache in the end when success
				this.Mgr.DeleteUploadTask(taskId)
			}
			return nil
		}
	}

	if len(this.GetPrimaryNodes()) == 0 {
		go func() {
			uploadResult, err := this.Start(false, taskId, this.GetFilePath(), opt)
			var serr *sdkErr.Error
			if err != nil {
				serr, _ = err.(*sdkErr.Error)
			}
			if uploadResult != nil {
				this.Mgr.EmitUploadResult(taskId, uploadResult, serr)
				// delete task from cache in the end when success
				if serr == nil {
					this.Mgr.DeleteUploadTask(taskId)
				}
			}
		}()
		return nil
	}
	hostAddr := this.getMasterNodeHostAddr()
	if len(hostAddr) == 0 {
		return sdkErr.New(sdkErr.GET_HOST_ADDR_ERROR, "host addr is empty for %s", this.GetPrimaryNodes())
	}
	// drop all pending request first
	// deprecated
	// req, err := this.taskMgr.TaskBlockReq(taskId)
	// if err == nil && req != nil {
	// 	log.Warnf("drain request len: %d", len(req))
	// 	for len(req) > 0 {
	// 		<-req
	// 	}
	// 	log.Warnf("drain request len: %d", len(req))
	// }
	msg := message.NewFileMsg(fileHashStr, netCom.FILE_OP_FETCH_RESUME,
		message.WithSessionId(taskId),
		message.WithWalletAddress(this.Mgr.Chain().WalletAddress()),
		message.WithSign(this.Mgr.Chain().CurrentAccount()),
	)
	_, err := client.P2PBroadcast([]string{hostAddr}, msg.ToProtoMsg(), msg.MessageId)
	if err != nil {
		return err
	}
	log.Debugf("resume upload task %s file %s", taskId, fileHashStr)
	go func() {
		uploadResult, err := this.Start(false, taskId, this.GetFilePath(), opt)
		log.Debugf("resume upload task %s file %s, result %s, err %v", taskId, fileHashStr, uploadResult, err)
		var serr *sdkErr.Error
		if err != nil {
			serr, _ = err.(*sdkErr.Error)
		}
		if uploadResult != nil {
			this.Mgr.EmitUploadResult(taskId, uploadResult, serr)
			// delete task from cache in the end when success
			if serr == nil {
				this.Mgr.DeleteUploadTask(taskId)
			}
		} else {
			this.Mgr.EmitUploadResult(taskId, uploadResult, serr)
		}
	}()
	return nil
}

// Cancel. cancel upload task
func (this *UploadTask) Cancel(tx string, txHeight uint32) (*types.DeleteUploadFileResp, error) {
	if err := this.Task.Cancel(); err != nil {
		return nil, err
	}

	fileHashStr := this.GetFileHash()
	taskId := this.GetId()
	oldState := this.State()

	log.Debugf("cancel upload task id: %s,  fileHash: %v, oldState %v",
		taskId, fileHashStr, oldState)

	if len(fileHashStr) == 0 {
		// file hasn't make slice, just delete local task
		err := this.Mgr.CleanUploadTask(taskId)
		if err == nil {
			return nil, nil
		}
		log.Errorf("delete task %s with no fileHash err %s", taskId, err)
		this.SetTaskState(oldState)
		return nil, sdkErr.New(sdkErr.DELETE_FILE_FAILED, "delete task failed %s, err: %v", fileHashStr, err)
	}

	// find uploaded node from DB
	nodeList := this.DB.GetUploadedBlockNodeList(taskId, fileHashStr, 0)
	if len(nodeList) == 0 {
		resp, err := this.sendDeleteMsg(tx, txHeight)
		if err != nil {
			this.SetTaskState(oldState)
			return nil, err
		}
		if resp == nil {
			return nil, sdkErr.New(sdkErr.DELETE_FILE_FAILED, "delete file but not response %s", fileHashStr)
		}

		err = this.Mgr.CleanUploadTask(taskId)
		if err == nil {
			return resp, nil
		}
		log.Errorf("delete task of file %s with no nodes err %s", fileHashStr, err)
		this.SetTaskState(oldState)
		return nil, sdkErr.New(sdkErr.DELETE_FILE_FAILED, "delete task failed %s, err: %v", fileHashStr, err)

	}

	// send fetch-cancel msg
	msg := message.NewFileMsg(fileHashStr, netCom.FILE_OP_FETCH_CANCEL,
		message.WithSessionId(taskId),
		message.WithWalletAddress(this.Mgr.Chain().WalletAddress()),
		message.WithSign(this.Mgr.Chain().CurrentAccount()),
	)
	ret, err := client.P2PBroadcast(nodeList, msg.ToProtoMsg(), msg.MessageId)
	log.Debugf("broadcast cancel file %s msg ret %v, err: %s", fileHashStr, ret, err)
	resp, err := this.sendDeleteMsg(tx, txHeight)
	if err != nil {
		this.SetTaskState(oldState)
		return nil, err
	}
	if resp == nil {
		return nil, sdkErr.New(sdkErr.DELETE_FILE_FAILED, "delete file but not response %s", fileHashStr)
	}

	err = this.Mgr.CleanUploadTask(taskId)
	if err == nil {
		return resp, nil
	}
	log.Errorf("delete task %s err %s", taskId, err)
	this.SetTaskState(oldState)
	return nil, sdkErr.New(sdkErr.DELETE_FILE_FAILED, "delete task failed %s, err: %v", fileHashStr, err)
}

// sendPauseMsg. send fetch pause msg to master storage node.
func (this *UploadTask) sendPauseMsg() error {
	pause := this.IsTaskPaused()
	if !pause {
		return nil
	}
	if len(this.GetFileHash()) == 0 {
		return nil
	}
	if len(this.GetPrimaryNodes()) == 0 {
		return nil
	}
	hostAddr := this.getMasterNodeHostAddr()
	// send pause msg
	msg := message.NewFileMsg(this.GetFileHash(), netCom.FILE_OP_FETCH_PAUSE,
		message.WithSessionId(this.GetId()),
		message.WithWalletAddress(this.Mgr.Chain().WalletAddress()),
		message.WithSign(this.Mgr.Chain().CurrentAccount()),
	)
	ret, err := client.P2PBroadcast([]string{hostAddr}, msg.ToProtoMsg(), msg.MessageId)
	if err != nil {
		return err
	}
	log.Debugf("broadcast pause msg ret %v", ret)
	return nil
}

// findReceivers find the storage nodes and start handshake
// The handshake communication:
// client -> fetch_ask -> peer.
// client <- fetch_ack <- peer.
// primaryNodes: original handshaked peers' wallet address
// return receivers' wallet address
func (this *UploadTask) findReceivers(primaryNodes []chainCom.Address) ([]chainCom.Address, error) {
	log.Debugf("upload task %v start to find receivers with primary node length %v", this.GetId(), len(primaryNodes))

	this.EmitProgress(types.TaskUploadFileFindReceivers)
	var nodeWalletAddrList []chainCom.Address
	var nodeList []string
	receiverCount := int(this.GetCopyNum() + 1)
	if primaryNodes != nil && len(primaryNodes) > 0 {
		// get primary nodelist (primary nodes mean the nodes fetch files) from fileinfo first.
		// get host addr from chain
		nodeWalletAddrList = primaryNodes
		var err error
		nodeList, err = this.Mgr.Chain().GetNodeHostAddrListByWallets(nodeWalletAddrList[:1])
		if err != nil {
			return nil, err
		}
		receiverCount = 1
	} else {
		// get nodes
		var err error
		nodeWalletAddrList, err = this.getNodesFromChain()
		if err != nil {
			return nil, sdkErr.NewWithError(sdkErr.GET_STORAGE_NODES_FAILED, err)
		}
		log.Debugf("uploading nodelist %v, opt.CopyNum %d", nodeWalletAddrList, this.GetCopyNum())
		if len(nodeWalletAddrList) < int(this.GetCopyNum()+1) {
			return nil, sdkErr.New(sdkErr.ONLINE_NODES_NOT_ENOUGH, "node is not enough %d, copyNum %d",
				len(nodeWalletAddrList), this.GetCopyNum())
		}
		// get host addrs
		nodeList, err = this.Mgr.Chain().GetNodeHostAddrListByWallets(nodeWalletAddrList)
		if err != nil {
			return nil, err
		}
		if !this.Mgr.Config().AllowLocalNode {
			nodeList = removeLocalIPNodes(nodeList)
		}
	}
	var msg *message.Message
	chainType := this.Mgr.Chain().GetChainType()
	switch chainType {
	case consts.DspModeOp:
		msg = message.NewFileMsg(this.GetFileHash(), netCom.FILE_OP_FETCH_ASK,
			message.WithSessionId(this.GetId()), // use task id as session id to remote peers
			message.WithWalletAddress(this.Mgr.Chain().WalletAddress()),
			message.WithSignByETH(this.Mgr.Chain().CurrentAccount()),
		)
	default:
		msg = message.NewFileMsg(this.GetFileHash(), netCom.FILE_OP_FETCH_ASK,
			message.WithSessionId(this.GetId()), // use task id as session id to remote peers
			message.WithWalletAddress(this.Mgr.Chain().WalletAddress()),
			message.WithSign(this.Mgr.Chain().CurrentAccount()),
		)
	}
	log.Debugf("broadcast fetch_ask msg of file: %s, to %v", this.GetFileHash(), nodeList)
	this.SetNodeNetPhase(nodeList, int(types.WorkerSendFileAsk))
	walletAddrs, err := this.broadcastAskMsg(msg, nodeList, receiverCount)
	if err != nil {
		return nil, err
	}
	if len(walletAddrs) < receiverCount {
		return nil, sdkErr.New(sdkErr.RECEIVERS_NOT_ENOUGH, "file receivers is not enough")
	}
	for _, walletAddress := range walletAddrs {
		this.SetNodeNetPhase([]string{walletAddress.ToBase58()}, int(types.WorkerRecvFileAck))
	}
	return walletAddrs, nil
}

// broadcastAskMsg. send ask msg to nodelist, and expect min response count of node to response a ack msg
// return the host addr list in order
func (this *UploadTask) broadcastAskMsg(msg *message.Message, nodeList []string, minResponse int) (
	[]chainCom.Address, error) {
	lock := new(sync.Mutex)
	walletAddrs := make([]chainCom.Address, 0)
	backupAddrs := make([]chainCom.Address, 0)
	existWallet := make(map[string]struct{})
	stop := false
	height, _ := this.Mgr.Chain().GetCurrentBlockHeight()
	nodeListLen := len(nodeList)
	action := func(res proto.Message, hostAddr string) bool {
		lock.Lock()
		defer lock.Unlock()
		p2pMsg := message.ReadMessage(res)
		if p2pMsg == nil {
			log.Errorf("read message failed")
			return false
		}
		if p2pMsg.Error != nil && p2pMsg.Error.Code != sdkErr.SUCCESS {
			log.Errorf("get file fetch_ack msg err code %d, msg %s", p2pMsg.Error.Code, p2pMsg.Error.Message)
			return false
		}
		if stop {
			log.Debugf("break here after stop is true")
			return true
		}
		fileMsg := p2pMsg.Payload.(*file.File)
		walletAddr := fileMsg.PayInfo.WalletAddress
		log.Debugf("recv file ack msg from %s file %s", walletAddr, fileMsg.Hash)
		if _, ok := existWallet[fileMsg.PayInfo.WalletAddress]; ok {
			return false
		}
		existWallet[fileMsg.PayInfo.WalletAddress] = struct{}{}
		walletAddress, err := chainCom.AddressFromBase58(fileMsg.PayInfo.WalletAddress)
		if err != nil {
			return false
		}
		// compare chain info
		if fileMsg.ChainInfo.Height > height && fileMsg.ChainInfo.Height > height+consts.MAX_BLOCK_HEIGHT_DIFF {
			log.Debugf("remote node block height is %d, but current block height is %d",
				fileMsg.ChainInfo.Height, height)
			backupAddrs = append(backupAddrs, walletAddress)
			return false
		}
		if height > fileMsg.ChainInfo.Height && height > fileMsg.ChainInfo.Height+consts.MAX_BLOCK_HEIGHT_DIFF {
			log.Debugf("remote node block height is %d, but current block height is %d",
				fileMsg.ChainInfo.Height, height)
			backupAddrs = append(backupAddrs, walletAddress)
			return false
		}

		// lower priority node, put to backup list
		if this.Mgr.IsWorkerBusy(this.GetId(), walletAddr, int(types.WorkerSendFileAsk)) {
			log.Debugf("peer %s is busy, put to backup list", walletAddr)
			backupAddrs = append(backupAddrs, walletAddress)
			return false
		}
		if bad, err := client.P2PIsPeerNetQualityBad(walletAddr, client.P2PNetTypeDsp); bad || err != nil {
			log.Debugf("peer %s network quality is bad, put to backup list", walletAddr)
			backupAddrs = append(backupAddrs, walletAddress)
			return false
		}
		walletAddrs = append(walletAddrs, walletAddress)
		log.Debugf("send file_ask msg success of file: %s, to: %s  receivers: %v", fileMsg.Hash, walletAddr, walletAddrs)
		primaryListLen := len(walletAddrs)
		backupListLen := len(backupAddrs)
		if primaryListLen >= minResponse || primaryListLen >= nodeListLen ||
			backupListLen >= nodeListLen {
			log.Debugf("break receiver goroutine")
			stop = true
			return true
		}
		log.Debugf("continue....")
		return false
	}
	ret, err := client.P2PBroadcast(nodeList, msg.ToProtoMsg(), msg.MessageId, action)
	if err != nil {
		log.Errorf("wait file receivers broadcast err %s", err)
		return nil, err
	}
	log.Debugf("receives %v, backupAddrs: %v, broadcast file ask ret %v, minResponse %d",
		walletAddrs, backupAddrs, ret, minResponse)
	if len(walletAddrs) >= minResponse {
		return walletAddrs, nil
	}
	if len(walletAddrs)+len(backupAddrs) < minResponse {
		log.Errorf("find %d primary nodes and %d back up nodes, but the file need %d nodes",
			len(walletAddrs), len(backupAddrs), minResponse)
		return nil, sdkErr.New(sdkErr.RECEIVERS_NOT_ENOUGH, "no enough nodes, please retry later")
	}
	// append backup nodes to tail with random range
	log.Debugf("backup list %v", backupAddrs)
	for _, walletAddr := range backupAddrs {
		walletAddrs = append(walletAddrs, walletAddr)
		if len(walletAddrs) >= minResponse {
			break
		}
	}
	log.Debugf("receives :%v, ret %v", walletAddrs, ret)
	return walletAddrs, nil
}

// notifyFetchReady send fetch_rdy msg to receivers
func (this *UploadTask) sendFetchReadyMsg(peerWalletAddr string) (*message.Message, error) {

	taskId := this.GetId()
	msg := message.NewFileMsg(this.GetFileHash(), netCom.FILE_OP_FETCH_RDY,
		message.WithSessionId(taskId),
		message.WithWalletAddress(this.Mgr.Chain().WalletAddress()),
		message.WithBlocksRoot(this.GetBlocksRoot()),
		message.WithTxHash(this.GetStoreTx()),
		message.WithTxHeight(uint64(this.GetStoreTxHeight())),
		message.WithPrefix(this.GetPrefix()),
		message.WithTotalBlockCount(this.GetTotalBlockCnt()),
		message.WithSign(this.Mgr.Chain().CurrentAccount()),
	)
	log.Debugf("send ready msg tx %s, height %d, sessionId %s, prefix %s, blocks root %s",
		this.GetStoreTx(), this.GetStoreTxHeight(), taskId, this.GetPrefix(), this.GetBlocksRoot())
	resp, err := client.P2PSendAndWaitReply(peerWalletAddr, msg.MessageId, msg.ToProtoMsg())
	if err != nil {
		return nil, err
	}
	p2pMsg := message.ReadMessage(resp)
	if p2pMsg == nil {
		return nil, sdkErr.New(sdkErr.INTERNAL_ERROR, "read msg failed")
	}
	return p2pMsg, nil
}

// generateBlockMsgData. generate blockdata, blockEncodedData, tagData with prove params
func (this *UploadTask) generateBlockMsgData(hash string, index, offset uint64, fileID pdp.FileID) (*types.BlockMsgData, error) {
	// TODO: use disk fetch rather then memory access
	block := this.Mgr.Fs().GetBlock(hash)
	blockData := this.Mgr.Fs().BlockDataOfAny(block)
	if len(blockData) == 0 {
		log.Warnf("get empty block of %s %d", hash, index)
	}

	p := pdp.NewPdp(0)
	tag, err := p.GenerateTag([]pdp.Block{blockData}, fileID)
	if err != nil {
		return nil, err
	}

	msgData := &types.BlockMsgData{
		BlockData: blockData,
		Tag:       tag[0][:],
		Offset:    offset,
		RefCnt:    1,
	}
	return msgData, nil
}

// sendBlocks. prepare block data and tags, send blocks to the node
func (this *UploadTask) sendBlocks(hashes []string) error {
	p, err := this.Mgr.Chain().ProveParamDes(this.GetProveParams())
	if err != nil {
		return sdkErr.New(sdkErr.GET_PDP_PARAMS_ERROR, err.Error())
	}

	if len(this.GetPrimaryNodes()) == 0 || len(this.GetPrimaryNodes()[0]) == 0 {
		return sdkErr.New(sdkErr.GET_TASK_PROPERTY_ERROR, "master node host addr is nil")
	}
	payTxHeight, err := this.Mgr.Chain().GetBlockHeightByTxHash(this.GetStoreTx())
	if err != nil {
		return sdkErr.NewWithError(sdkErr.PAY_FOR_STORE_FILE_FAILED, err)
	}
	if err := this.SetInfoWithOptions(base.StoreTxHeight(payTxHeight)); err != nil {
		return sdkErr.NewWithError(sdkErr.SET_FILEINFO_DB_ERROR, err)
	}
	log.Debugf("pay for file success at block height %v ", payTxHeight)
	this.EmitProgress(types.TaskUploadFileGeneratePDPData)
	allOffset, err := this.Mgr.Fs().GetAllOffsets(hashes[0])
	if err != nil {
		return sdkErr.New(sdkErr.PREPARE_UPLOAD_ERROR, err.Error())
	}
	this.EmitProgress(types.TaskUploadFileTransferBlocks)
	var getMsgDataLock sync.Mutex
	blockMsgDataMap := make(map[string]*types.BlockMsgData)
	getMsgData := func(hash string, index uint64) *types.BlockMsgData {
		getMsgDataLock.Lock()
		defer getMsgDataLock.Unlock()
		key := keyOfBlockHashAndIndex(hash, index)
		data, ok := blockMsgDataMap[key]
		if ok {
			return data
		}
		offsetKey := fmt.Sprintf("%s-%d", hash, index)
		offset, _ := allOffset[offsetKey]
		var err error
		data, err = this.generateBlockMsgData(hash, index, offset, p.FileID)
		if err != nil {
			return nil
		}
		blockMsgDataMap[key] = data
		return data
	}
	cleanMsgData := func(reqInfos []*block.Block) {
		getMsgDataLock.Lock()
		defer getMsgDataLock.Unlock()
		for _, reqInfo := range reqInfos {
			hash := reqInfo.Hash
			index := uint64(reqInfo.Index)
			key := keyOfBlockHashAndIndex(hash, index)
			data, ok := blockMsgDataMap[key]
			if !ok {
				continue
			}
			data.RefCnt--
			if data.RefCnt > 0 {
				continue
			}
			delete(blockMsgDataMap, key)
			this.Mgr.Fs().ReturnBuffer(data.BlockData)
		}
		if len(reqInfos) > 0 {
			log.Debugf("delete block msg data of key %s-%d to %s-%d", reqInfos[0].Hash, reqInfos[0].Index,
				reqInfos[len(reqInfos)-1].Hash, reqInfos[len(reqInfos)-1].Index)
		}
	}
	return this.SendBlocksToPeer(this.GetPrimaryNodes()[0], hashes, getMsgData, cleanMsgData)
}

// sendBlocksToPeer. send rdy msg, wait its response, and send blocks to the node
func (this *UploadTask) SendBlocksToPeer(
	peerWalletAddr string,
	blockHashes []string,
	getMsgData func(hash string, index uint64) *types.BlockMsgData,
	cleanMsgData func(reqInfos []*block.Block),
) error {

	taskId := this.GetId()
	fileHashStr := this.GetFileHash()
	log.Debugf("sendBlocksToPeer %v, taskId: %s, file: %s", peerWalletAddr, taskId, fileHashStr)
	if err := this.SetInOrder(true); err != nil {
		return err
	}
	this.NewWorkers([]string{peerWalletAddr}, nil)
	// notify fetch ready to all receivers
	resp, err := this.sendFetchReadyMsg(peerWalletAddr)
	if err != nil {
		log.Errorf("notify fetch ready msg failed, err %s", err)
		return err
	}
	if resp.Error != nil && resp.Error.Code != netCom.MSG_ERROR_CODE_NONE {
		log.Errorf("receive rdy msg reply err %d, msg %s", resp.Error.Code, resp.Error.Message)
		return sdkErr.New(sdkErr.RECEIVE_ERROR_MSG, resp.Error.Message)
	}

	respFileMsg := resp.Payload.(*file.File)

	log.Debugf("blockHashes :%d", len(blockHashes))
	startIndex := uint64(0)
	if respFileMsg.Breakpoint != nil && len(respFileMsg.Breakpoint.Hash) != 0 &&
		respFileMsg.Breakpoint.Index != 0 && respFileMsg.Breakpoint.Index < uint64(len(blockHashes)) &&
		blockHashes[respFileMsg.Breakpoint.Index] == respFileMsg.Breakpoint.Hash {
		startIndex = respFileMsg.Breakpoint.Index + 1
	}
	if err := this.UpdateTaskProgress(taskId, peerWalletAddr, startIndex); err != nil {
		return err
	}
	log.Debugf("task %s, start at %d", taskId, startIndex)
	if stop := this.IsTaskStop(); stop {
		log.Debugf("stop handle request because task is stop: %t", stop)
		return nil
	}
	blocks := make([]*block.Block, 0, consts.MAX_SEND_BLOCK_COUNT)
	blockInfos := make([]*store.BlockInfo, 0, consts.MAX_SEND_BLOCK_COUNT)
	sending := false
	sendLock := new(sync.Mutex)

	limitWg := new(sync.WaitGroup)
	stopTickerCh := make(chan struct{})
	defer close(stopTickerCh)
	go func() {
		speedLimitTicker := time.NewTicker(time.Second)
		defer speedLimitTicker.Stop()
		for {
			select {
			case <-speedLimitTicker.C:
				sendLock.Lock()
				if sending {
					limitWg.Done()
					sending = false
					log.Debugf("sending is 1, set 0")
				}
				sendLock.Unlock()
			case <-stopTickerCh:
				log.Debugf("stop check limit ticker")
				return
			}
		}
	}()
	for index, hash := range blockHashes {
		if uint64(index) < startIndex {
			continue
		}
		blockMsgData := getMsgData(hash, uint64(index))
		if blockMsgData == nil {
			return sdkErr.New(sdkErr.DISPATCH_FILE_ERROR, "no block msg data to send")
		}
		isStored := this.DB.IsBlockUploaded(taskId, hash, peerWalletAddr, uint64(index))
		if !isStored {
			bi := &store.BlockInfo{
				Hash:       hash,
				Index:      uint64(index),
				DataSize:   blockMsgData.DataLen,
				DataOffset: blockMsgData.Offset,
			}
			blockInfos = append(blockInfos, bi)
		}
		b := &block.Block{
			SessionId: this.GetId(),
			Index:     uint64(index),
			FileHash:  fileHashStr,
			Hash:      hash,
			Data:      blockMsgData.BlockData,
			Tag:       blockMsgData.Tag,
			Operation: netCom.BLOCK_OP_NONE,
			Offset:    int64(blockMsgData.Offset),
		}
		blocks = append(blocks, b)
		if index != len(blockHashes)-1 && len(blocks) < consts.MAX_SEND_BLOCK_COUNT {
			continue
		}
		log.Debugf("will send blocks %d", len(blocks))
		this.ActiveWorker(peerWalletAddr)
		sendLock.Lock()
		if !sending {
			limitWg.Add(1)
			sending = true
			log.Debugf("sending is 0, add 1")
		}
		sendLock.Unlock()
		if err := this.sendBlockFlightMsg(peerWalletAddr, blocks); err != nil {
			limitWg.Wait()
			return err
		}
		limitWg.Wait()
		if cleanMsgData != nil {
			cleanMsgData(blocks)
		}
		// update progress
		this.SetBlocksUploaded(taskId, peerWalletAddr, blockInfos)
		this.EmitProgress(types.TaskUploadFileTransferBlocks)
		this.ActiveWorker(peerWalletAddr)
		blocks = blocks[:0]
		blockInfos = blockInfos[:0]
		if stop := this.IsTaskStop(); stop {
			log.Debugf("stop handle request because task is stop: %t", stop)
			return nil
		}
		fileOwner := this.GetFileOwner()
		if !this.DB.IsFileUploaded(taskId, fileOwner != this.Mgr.Chain().WalletAddress()) {
			continue
		}
		log.Debugf("task %s send blocks done", taskId)
	}
	return nil
}

// sendBlockFlightMsg. send block flight msg to peer
func (this *UploadTask) sendBlockFlightMsg(peerAddr string, blocks []*block.Block) error {
	// send block
	if len(blocks) == 0 {
		log.Warnf("task %s has no block to send", this.GetId())
		return nil
	}
	flights := &block.BlockFlights{
		TimeStamp: time.Now().UnixNano(),
		Blocks:    blocks,
	}
	msg := message.NewBlockFlightsMsg(flights)
	sendLogMsg := fmt.Sprintf("file: %s, block %s-%s, index:%d-%d to %s with msg id %s",
		this.GetFileHash(), blocks[0].Hash, blocks[len(blocks)-1].Hash,
		blocks[0].Index, blocks[len(blocks)-1].Index, peerAddr, msg.MessageId)
	sendingTime := time.Now().Unix()
	log.Debugf("sending %s", sendLogMsg)
	if err := client.P2PSend(peerAddr, msg.MessageId, msg.ToProtoMsg()); err != nil {
		log.Errorf("%v, err %s", sendLogMsg, err)
		return err
	}
	log.Debugf("sending %s success\n, used %ds", sendLogMsg, time.Now().Unix()-sendingTime)
	return nil
}

// getMasterNodeHostAddr. get master node host address
func (this *UploadTask) getMasterNodeHostAddr() string {
	walletAddr := this.GetPrimaryNodes()[0]
	hostAddr, _ := client.P2PGetHostAddrFromWalletAddr(walletAddr, client.P2PNetTypeDsp)
	if len(hostAddr) != 0 {
		return hostAddr
	}
	hosts, err := this.Mgr.Chain().GetNodeHostAddrListByWallets(uAddr.Base58ToWalletAddrs([]string{walletAddr}))
	if err != nil || len(hosts) == 0 {
		return ""
	}
	return hosts[0]
}

// DeleteUploadedFileByIds. Delete uploaded file from remote nodes. it is called by the owner
func (this *UploadTask) sendDeleteMsg(deleteTaskTx string, deleteTaskTxHeight uint32) (*types.DeleteUploadFileResp, error) {
	fileHashStr := this.GetFileHash()
	taskId := this.GetId()
	storingNode := this.getFileProvedNode(fileHashStr)
	log.Debugf("cancel upload task %s file %v will send file-delete msg", taskId, fileHashStr)
	if len(storingNode) == 0 {
		// find breakpoint keep nodelist
		nodesFromTsk := uAddr.Base58ToWalletAddrs(this.DB.GetUploadedBlockNodeList(taskId, fileHashStr, 0))
		host, err := this.Mgr.Chain().GetNodeHostAddrListByWallets(nodesFromTsk)
		if err != nil {
			log.Warnf("get nodes empty from wallets %v", nodesFromTsk)
		}
		storingNode = append(storingNode, host...)
		log.Debugf("storingNode: %v", storingNode)
	}
	log.Debugf("will broadcast delete msg to %v", storingNode)
	fileName := this.GetFileName()
	resp := &types.DeleteUploadFileResp{
		Tx:       deleteTaskTx,
		FileHash: fileHashStr,
		FileName: fileName,
	}
	if len(storingNode) == 0 {
		log.Debugf("cancel upload task %s file %s with resp %v has no stored nodes",
			taskId, fileHashStr, resp)
		return resp, nil
	}

	go func() {
		log.Debugf("cancel upload task %s send delete msg to nodes :%v", taskId, storingNode)
		msg := message.NewFileMsg(fileHashStr, netCom.FILE_OP_DELETE,
			message.WithSessionId(taskId),
			message.WithWalletAddress(this.Mgr.Chain().WalletAddress()),
			message.WithTxHash(deleteTaskTx),
			message.WithTxHeight(uint64(deleteTaskTxHeight)),
			message.WithSign(this.Mgr.Chain().CurrentAccount()),
		)
		nodeStatusLock := new(sync.Mutex)
		nodeStatus := make([]types.DeleteFileStatus, 0, len(storingNode))
		reply := func(msg proto.Message, hostAddr string) bool {
			ackMsg := message.ReadMessage(msg)
			nodeStatusLock.Lock()
			defer nodeStatusLock.Unlock()
			errCode := uint32(0)
			errMsg := ""
			if ackMsg.Error != nil {
				errCode = ackMsg.Error.Code
				errMsg = ackMsg.Error.Message
			}
			nodeStatus = append(nodeStatus, types.DeleteFileStatus{
				HostAddr: hostAddr,
				Code:     errCode,
				Error:    errMsg,
			})
			return false
		}
		m, err := client.P2PBroadcast(storingNode, msg.ToProtoMsg(), msg.MessageId, reply)
		log.Debugf("send delete msg done ret: %v, nodeStatus: %v, err: %s", m, nodeStatus, err)
	}()

	return resp, nil
}
