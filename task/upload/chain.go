package upload

import (
	"math"
	"os"
	"time"

	"github.com/saveio/dsp-go-sdk/actor/client"
	"github.com/saveio/dsp-go-sdk/consts"
	sdkErr "github.com/saveio/dsp-go-sdk/error"
	"github.com/saveio/dsp-go-sdk/task/base"
	"github.com/saveio/dsp-go-sdk/task/types"
	"github.com/saveio/dsp-go-sdk/types/link"
	uAddr "github.com/saveio/dsp-go-sdk/utils/addr"
	chainCom "github.com/saveio/themis/common"
	"github.com/saveio/themis/common/log"
	fs "github.com/saveio/themis/smartcontract/service/native/savefs"
	"github.com/saveio/themis/smartcontract/service/native/savefs/pdp"
)

// payForSendFile pay before send a file
// return PoR params byte slice, PoR private key of the file or error
func (this *UploadTask) payFile(fileID pdp.FileID, tagsRoot []byte, walletAddrs []chainCom.Address) error {
	if this.IsTaskPaused() {
		return this.sendPauseMsg()
	}

	this.EmitProgress(types.TaskUploadFilePaying)

	log.Info("upload task %s pay the upload file %s on chain with wallet %s", this.GetId(), this.GetFileHash(),
		this.GetWalletAddr())
	blockSizeInKB := uint64(math.Ceil(float64(consts.CHUNK_SIZE) / 1024.0))
	paramsBuf, err := this.Mgr.Chain().ProveParamSer(tagsRoot, fileID)
	if err != nil {
		log.Errorf("serialization prove params failed:%s", err)
		return err
	}
	candidateNodes, err := this.Mgr.Chain().GetNodeListWithoutAddrs(walletAddrs, 2*len(walletAddrs))
	if err != nil {
		return err
	}
	if err = this.SetInfoWithOptions(base.ProveParams(paramsBuf)); err != nil {
		log.Errorf("set task info err %s", err)
		return err
	}
	log.Debugf("primary nodes: %v, candidate nodes: %v proveLevel %v", walletAddrs, candidateNodes, this.GetProveLevel())
	// TODO: check confirmed on receivers
	tx, txHeight, err := this.Mgr.Chain().StoreFile(
		this.GetFileHash(),
		this.GetBlocksRoot(),
		this.GetTotalBlockCnt(),
		blockSizeInKB,
		this.GetProveLevel(),
		this.GetExpiredHeight(),
		uint64(this.GetCopyNum()),
		[]byte(this.GetFileName()),
		uint64(this.GetPrivilege()),
		paramsBuf,
		uint64(this.GetStoreType()),
		this.GetRealFileSize(),
		walletAddrs,
		candidateNodes,
		nil,
	)
	if err != nil {
		log.Debugf("store file params fileHash:%v, blockNum:%v, blockSize:%v, "+
			"proveInterval:%v, expiredHeight:%v, copyNum:%v, fileName:%v, privilege:%v, params:%x,"+
			" storeType:%v, fileSize:%v, primary:%v, candidates:%v",
			this.GetFileHash(), this.GetTotalBlockCnt(), blockSizeInKB, this.GetProveInterval(),
			this.GetExpiredHeight(), uint64(this.GetCopyNum()), []byte(this.GetFileName()),
			uint64(this.GetPrivilege()), paramsBuf, uint64(this.GetStoreType()), this.GetRealFileSize(),
			walletAddrs, candidateNodes)
		log.Errorf("pay store file order failed:%s", err)
		return err
	}
	this.EmitProgress(types.TaskWaitForBlockConfirmed)

	primaryNodeHostAddrs, _ := this.Mgr.Chain().GetNodeHostAddrListByWallets(walletAddrs)
	candidateNodeHostAddrs, _ := this.Mgr.Chain().GetNodeHostAddrListByWallets(candidateNodes)
	primaryNodeMap := uAddr.WalletHostAddressMap(walletAddrs, primaryNodeHostAddrs)
	candidateNodeHostMap := uAddr.WalletHostAddressMap(candidateNodes, candidateNodeHostAddrs)
	this.EmitProgress(types.TaskWaitForBlockConfirmedDone)
	if err = this.SetInfoWithOptions(
		base.StoreTx(tx),
		base.StoreTxHeight(txHeight),
		base.FileOwner(this.Mgr.Chain().WalletAddress()),
		base.PrimaryNodes(uAddr.WalletAddrsToBase58(walletAddrs)),
		base.CandidateNodes(uAddr.WalletAddrsToBase58(candidateNodes)),
		base.NodeHostAddrs(uAddr.MergeTwoAddressMap(primaryNodeMap, candidateNodeHostMap)),
		base.ProveParams(paramsBuf),
	); err != nil {
		log.Errorf("set task info err %s", err)
		return err
	}

	return nil
}

// SetupTaskFieldByFileInfo. setup some task properties from fileInfo from the chain
func (this *UploadTask) setupTaskFieldByFileInfo(fileInfo *fs.FileInfo) error {
	paramsBuf := fileInfo.FileProveParam
	tx := this.GetStoreTx()
	log.Debugf("file %s already paid, tx %s, setup task by fileInfo", fileInfo.FileHash, tx)

	primaryWallets := make([]chainCom.Address, 0)
	primaryWallets = append(primaryWallets, fileInfo.PrimaryNodes.AddrList...)

	candidateWallets := make([]chainCom.Address, 0)
	candidateWallets = append(candidateWallets, fileInfo.CandidateNodes.AddrList...)

	primaryNodeHostAddrs, _ := this.Mgr.Chain().GetNodeHostAddrListByWallets(primaryWallets)
	candidateNodeHostAddrs, _ := this.Mgr.Chain().GetNodeHostAddrListByWallets(candidateWallets)
	primaryNodeMap := uAddr.WalletHostAddressMap(primaryWallets, primaryNodeHostAddrs)
	candidateNodeHostMap := uAddr.WalletHostAddressMap(candidateWallets, candidateNodeHostAddrs)

	err := this.SetInfoWithOptions(
		base.StoreTx(tx),
		base.StoreTxHeight(uint32(fileInfo.BlockHeight)),
		base.FileOwner(this.Mgr.Chain().WalletAddress()),
		base.PrimaryNodes(uAddr.WalletAddrsToBase58(primaryWallets)),
		base.CandidateNodes(uAddr.WalletAddrsToBase58(candidateWallets)),
		base.NodeHostAddrs(uAddr.MergeTwoAddressMap(primaryNodeMap, candidateNodeHostMap)),
		base.ProveParams(paramsBuf))
	if err != nil {
		log.Errorf("set task info err %s", err)
	}
	return err
}

func (this *UploadTask) addWhitelist(taskId, fileHashStr string, opt *fs.UploadOption) (string, error) {
	// Don't add whitelist if resume upload
	if opt.WhiteList.Num == 0 {
		return "", nil
	}
	tx := this.GetWhitelistTx()
	if len(tx) > 0 {
		log.Debugf("upload task %s get whitelist tx %s from db", taskId, tx)
		return tx, nil
	}
	this.EmitProgress(types.TaskUploadFileCommitWhitelist)
	addWhiteListTx, err := this.Mgr.Chain().AddWhiteLists(fileHashStr, opt.WhiteList.List)
	log.Debugf("upload task %s add whitelist tx %s, err %s", taskId, addWhiteListTx, err)
	if err != nil {
		return "", sdkErr.NewWithError(sdkErr.ADD_WHITELIST_ERROR, err)
	}
	err = this.SetWhiteListTx(addWhiteListTx)
	if err != nil {
		return "", sdkErr.NewWithError(sdkErr.SET_FILEINFO_DB_ERROR, err)
	}
	this.EmitProgress(types.TaskUploadFileCommitWhitelistDone)
	return addWhiteListTx, nil
}

func (this *UploadTask) registerUrls(saveLink string, opt *fs.UploadOption) (string, string, error) {

	this.EmitProgress(types.TaskUploadFileRegisterDNS)
	var dnsRegTx, dnsBindTx string
	var err error
	urlString := string(opt.DnsURL)
	if opt.RegisterDNS && len(urlString) > 0 {
		dnsRegTx, err = this.Mgr.DNS().RegisterFileUrl(string(urlString), saveLink)
		if err != nil {
			log.Errorf("register url %s %s err: %s", urlString, saveLink, err)
			return "", "", err
		}
		log.Debugf("acc %s, reg dns %s for file %s", this.Mgr.Chain().WalletAddress(), dnsRegTx, this.GetFileHash())
	}
	if opt.BindDNS && len(urlString) > 0 {
		dnsBindTx, err = this.Mgr.DNS().BindFileUrl(string(urlString), saveLink)
		if err != nil {
			log.Errorf("bind url %s, saveLink %s err: %s", urlString, saveLink, err)
			return "", "", err
		}
		log.Debugf("bind dns %s for file %s", dnsBindTx, this.GetFileHash())
	}

	if len(dnsRegTx) > 0 || len(dnsBindTx) > 0 {
		this.EmitProgress(types.TaskWaitForBlockConfirmed)
		_, err := this.Mgr.Chain().WaitForGenerateBlock(time.Duration(consts.TX_CONFIRM_TIMEOUT) * time.Second)
		this.EmitProgress(types.TaskWaitForBlockConfirmedDone)
		if err != nil {
			log.Errorf("register url %s  wait for generate block failed, err %s", urlString, err)
			return "", "", err
		}
	}
	this.EmitProgress(types.TaskUploadFileRegisterDNSDone)
	return dnsRegTx, dnsBindTx, nil
}

// getNodesFromChain. get nodelist from chain
func (this *UploadTask) getNodesFromChain() ([]chainCom.Address, error) {
	fileStat, err := os.Stat(this.GetFilePath())
	if err != nil {
		return nil, sdkErr.NewWithError(sdkErr.INVALID_PARAMS, err)
	}
	infos, err := this.Mgr.Chain().GetNodeList()
	if err != nil {
		return nil, err
	}
	expectNodeNum := 4 * (this.GetCopyNum() + 1)
	nodeList := make([]chainCom.Address, 0, expectNodeNum)
	for _, info := range infos.NodeInfo {
		fileSizeInKb := uint64(math.Ceil(float64(fileStat.Size()) / 1024.0))
		if info.RestVol < fileSizeInKb {
			continue
		}
		fullAddress := string(info.NodeAddr)
		if fullAddress == client.P2PGetPublicAddr() {
			continue
		}
		nodeList = append(nodeList, info.WalletAddr)
		if uint64(len(nodeList)) >= uint64(expectNodeNum) {
			break
		}
	}
	return nodeList, nil
}

// getFileProveNode. get file storing nodes  and stored (expired) nodes
func (this *UploadTask) getFileProvedNode(fileHashStr string) []string {
	proveDetails, err := this.Mgr.Chain().GetFileProveDetails(fileHashStr)
	if proveDetails == nil {
		return nil
	}
	nodes := make([]string, 0)
	log.Debugf("fileHashStr %s prove details :%v, err:%s", fileHashStr, len(proveDetails.ProveDetails), err)
	for _, detail := range proveDetails.ProveDetails {
		log.Debugf("file %s, node:%s, prove times %d", fileHashStr, string(detail.NodeAddr), detail.ProveTimes)
		if detail.ProveTimes == 0 {
			continue
		}
		nodes = append(nodes, string(detail.NodeAddr))
	}
	return nodes
}

// isFileProved. check if master node has proved the file
func (this *UploadTask) isFileProved(fileHashStr string) bool {
	info, _ := this.Mgr.Chain().GetFileInfo(fileHashStr)
	if info == nil {
		return false
	}
	if len(info.PrimaryNodes.AddrList) < 1 {
		return false
	}
	proveDetails, _ := this.Mgr.Chain().GetFileProveDetails(fileHashStr)
	if proveDetails == nil {
		return false
	}
	for _, detail := range proveDetails.ProveDetails {
		log.Debugf("file %s, node:%s, prove times %d", fileHashStr, string(detail.NodeAddr), detail.ProveTimes)
		if detail.ProveTimes == 0 {
			continue
		}
		if detail.WalletAddr.ToBase58() == info.PrimaryNodes.AddrList[0].ToBase58() {
			return true
		}
	}
	return false
}

// checkFileBeProved thread-block method. check if the file has been proved for all store nodes.
func (this *UploadTask) checkFileBeProved(fileHashStr string) bool {
	this.EmitProgress(types.TaskUploadFileWaitForPDPProve)
	defer this.EmitProgress(types.TaskUploadFileWaitForPDPProveDone)
	retry := 0
	timewait := 5
	for {
		if retry > consts.CHECK_PROVE_TIMEOUT/timewait {
			return false
		}
		nodes := this.getFileProvedNode(fileHashStr)
		if uint64(len(nodes)) > 0 {
			break
		}
		retry++
		time.Sleep(time.Duration(timewait) * time.Second)
	}
	return true
}

// getFileUploadResult. get file upload result
func (this *UploadTask) publishFile(opt *fs.UploadOption) (*types.UploadResult, error) {
	fileLink := &link.URLLink{
		FileHashStr: this.GetFileHash(),
		FileName:    this.GetFileName(),
		FileOwner:   this.GetFileOwner(),
		FileSize:    opt.FileSize,
		BlockNum:    uint64(this.GetTotalBlockCnt()),
		Trackers:    this.Mgr.DNS().GetTrackerList(),
		BlocksRoot:  this.GetBlocksRoot(),
	}
	fileLinkStr := fileLink.String()

	// proved := this.checkFileBeProved(fileHashStr)
	// log.Debugf("checking file: %s proved done %t", fileHashStr, proved)
	// if !proved {
	// 	return nil, sdkErr.New(sdkErr.FILE_UPLOADED_CHECK_PDP_FAILED,
	// 		"file has sent, but no enough prove is finished")
	// }
	// if pause, err := this.checkIfPause(taskId, fileHashStr); err != nil || pause {
	// 	return "", "", err
	// }

	var dnsRegTx, dnsBindTx string
	if len(this.GetRegUrlTx()) == 0 || len(this.GetBindUrlTx()) == 0 {
		var err error
		dnsRegTx, dnsBindTx, err = this.registerUrls(fileLinkStr, opt)
		if err != nil {
			return nil, err
		}
	}
	log.Debugf("publish file %s with reg tx %s, bind tx %s",
		fileLink.FileHashStr, dnsRegTx, dnsBindTx)

	if err := this.SetInfoWithOptions(
		base.Url(string(opt.DnsURL)),
		base.RegUrlTx(dnsRegTx),
		base.BindUrlTx(dnsBindTx)); err != nil {
		return nil, err
	}

	return &types.UploadResult{
		Tx:             this.GetStoreTx(),
		FileHash:       this.GetFileHash(),
		Url:            string(opt.DnsURL),
		Link:           fileLinkStr,
		RegisterDnsTx:  dnsRegTx,
		BindDnsTx:      dnsBindTx,
		AddWhiteListTx: this.GetWhitelistTx(),
	}, nil
}
