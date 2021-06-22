package dispatch

import (
	"github.com/saveio/dsp-go-sdk/actor/client"
	sdkErr "github.com/saveio/dsp-go-sdk/error"
	netCom "github.com/saveio/dsp-go-sdk/network/common"
	"github.com/saveio/dsp-go-sdk/network/message"
	"github.com/saveio/dsp-go-sdk/network/message/types/file"
	"github.com/saveio/dsp-go-sdk/store"
	"github.com/saveio/dsp-go-sdk/task/download"
	"github.com/saveio/dsp-go-sdk/task/types"
	"github.com/saveio/themis/common/log"
)

// dispatchBlocks. dispatch blocks to other primary nodes
func (this *DispatchTask) Start() error {
	fileHashStr := this.GetFileHash()
	referId := this.GetReferId()
	taskId := this.GetId()
	log.Debugf("task %s dispatch file %v, refer to download task %s", taskId, fileHashStr, referId)
	fileInfo, err := this.Mgr.Chain().GetFileInfo(fileHashStr)
	if err != nil {
		return sdkErr.NewWithError(sdkErr.FILEINFO_NOT_EXIST, err)
	}
	if len(fileInfo.PrimaryNodes.AddrList) == 0 {
		return sdkErr.New(sdkErr.INTERNAL_ERROR, "primary node is nil")
	}
	if fileInfo.PrimaryNodes.AddrList[0].ToBase58() != this.GetCurrentWalletAddr() {
		log.Debugf("no need to dispatch file %s, because i am not the master node", fileHashStr)
		this.Mgr.CleanDispatchTask(taskId)
		return nil
	}
	provedNodes, err := this.Mgr.Chain().GetFileProveNodes(fileHashStr)
	if err != nil || provedNodes == nil {
		log.Errorf("dispatch file, but file prove detail not exist %s, err %s", fileHashStr, err)
		return sdkErr.New(sdkErr.INTERNAL_ERROR,
			"dispatch file, but file prove detail not exist %s, err %s", fileHashStr, err)
	}

	if provedTime, ok := provedNodes[this.GetCurrentWalletAddr()]; !ok || provedTime == 0 {
		return sdkErr.New(sdkErr.INTERNAL_ERROR, "master node hasn't proved the file %s", fileHashStr)
	}

	nodesToDispatch := make([]*types.NodeInfo, 0)

	hostAddrs, err := this.Mgr.Chain().GetNodeHostAddrListByWallets(fileInfo.PrimaryNodes.AddrList)
	if err != nil {
		return sdkErr.New(sdkErr.INTERNAL_ERROR,
			"dispatch file, get node host addr failed %s, err %s", fileHashStr, err)
	}
	for idx, n := range fileInfo.PrimaryNodes.AddrList {
		if n.ToBase58() == this.GetCurrentWalletAddr() {
			continue
		}
		if _, ok := provedNodes[n.ToBase58()]; ok {
			continue
		}
		if len(taskId) > 0 {
			// FIXME: state check is error when restart up
			doingOrDone, _ := this.isNodeTaskDoingOrDone(n.ToBase58())
			log.Debugf("upload task %s transfer to %s is doing or done %t",
				taskId, n.ToBase58(), doingOrDone)
			if doingOrDone {
				continue
			}
		}
		nodesToDispatch = append(nodesToDispatch, &types.NodeInfo{
			HostAddr:   hostAddrs[idx],
			WalletAddr: n.ToBase58(),
		})
	}
	log.Debugf("nodes to dispatch %v", nodesToDispatch)
	if len(nodesToDispatch) == 0 {
		log.Debugf("all nodes have dispatched, set %s file %s done", taskId, fileHashStr)
		if len(taskId) > 0 {
			this.SetTaskState(store.TaskStateDone)
		}
		return nil
	}
	refDownloadTaskImpl := this.Mgr.GetDownloadTaskImpl(referId)
	if refDownloadTaskImpl == nil {
		return sdkErr.New(sdkErr.INTERNAL_ERROR,
			"dispatch file, get download task with referId %s nil", referId)
	}
	refDownloadTask, _ := refDownloadTaskImpl.(*download.DownloadTask)
	if refDownloadTask == nil {
		return sdkErr.New(sdkErr.INTERNAL_ERROR,
			"dispatch file, get download task with referId %s nil", referId)
	}

	blockHashes := this.DB.FileBlockHashes(referId)
	totalCount := len(blockHashes)

	if err := this.SetTaskState(store.TaskStateDoing); err != nil {
		log.Errorf("set dispatch task  %s doing err %v", taskId, err)
		return sdkErr.New(sdkErr.INTERNAL_ERROR,
			"set dispatch task  %s doing err %v", taskId, err)
	}
	getMsgData := func(hash string, index uint64) *types.BlockMsgData {
		block := this.Mgr.Fs().GetBlock(hash)
		blockData := this.Mgr.Fs().BlockDataOfAny(block)
		tag, err := this.Mgr.Fs().GetTag(hash, fileHashStr, uint64(index))
		if err != nil {
			log.Errorf("get tag of file %s, block %s, index %d err %v",
				fileHashStr, hash, index, err)
			return nil
		}
		offset, err := this.DB.GetBlockOffset(referId, hash, index)
		if err != nil {
			log.Errorf("get block offset err %v, task id %s, file %s, block %s, index %d",
				err, referId, fileHashStr, hash, index)
			return nil
		}
		return &types.BlockMsgData{
			BlockData: blockData,
			Tag:       tag,
			Offset:    offset,
		}
	}
	for _, node := range nodesToDispatch {
		go func(peerWalletAddr, peerHostAddr string) {
			client.P2PAppendAddrForHealthCheck(peerWalletAddr, client.P2PNetTypeDsp)
			if err := client.P2PConnect(peerHostAddr); err != nil {
				log.Errorf("dispatch task %s connect to peer failed %s", taskId, err)
				return
			}
			msg := message.NewFileMsg(fileHashStr, netCom.FILE_OP_FETCH_ASK,
				message.WithSessionId(this.GetId()),
				message.WithWalletAddress(this.GetCurrentWalletAddr()),
				message.WithSign(this.Mgr.Chain().CurrentAccount()),
			)
			log.Debugf("task %s, file %s send file ask msg to %s", taskId, fileHashStr, peerWalletAddr)
			resp, err := client.P2PSendAndWaitReply(peerWalletAddr, msg.MessageId, msg.ToProtoMsg())
			if err != nil {
				log.Errorf("send file ask msg err %s", err)
				return
			}
			p2pMsg := message.ReadMessage(resp)
			if p2pMsg.Error != nil && p2pMsg.Error.Code != sdkErr.SUCCESS {
				log.Errorf("get file fetch_ack msg err %s", p2pMsg.Error.Message)
				return
			}
			// compare chain info
			fileMsg := p2pMsg.Payload.(*file.File)
			if fileMsg.ChainInfo.Height < refDownloadTask.GetStoreTxHeight() {
				log.Debugf("task %s dispatch interrupt, height %d less than %d",
					taskId, fileMsg.ChainInfo.Height, refDownloadTask.GetStoreTxHeight())
				return
			}
			log.Debugf("dispatch send file taskId %s, fileHashStr %s, peerAddr %s, prefix %s, storeTx %s, "+
				"totalCount %d, storeTxHeight %d", taskId, fileHashStr, peerWalletAddr, string(refDownloadTask.GetPrefix()),
				refDownloadTask.GetStoreTx(), uint32(totalCount), refDownloadTask.GetStoreTxHeight())
			this.updateTaskNodeState(peerWalletAddr, store.TaskStateDoing)
			if err := this.SendBlocksToPeer(
				peerWalletAddr,
				blockHashes,
				getMsgData,
				nil,
			); err != nil {
				log.Errorf("dispatch task %s send block err %s", taskId, err)
				this.Mgr.EmitDispatchResult(taskId, nil, sdkErr.NewWithError(sdkErr.DISPATCH_FILE_ERROR, err))
				this.updateTaskNodeState(peerWalletAddr, store.TaskStateFailed)
				client.P2PRemoveAddrFormHealthCheck(peerWalletAddr, client.P2PNetTypeDsp)
				return
			}
			this.updateTaskNodeState(peerWalletAddr, store.TaskStateDone)
			client.P2PRemoveAddrFormHealthCheck(peerWalletAddr, client.P2PNetTypeDsp)
		}(node.WalletAddr, node.HostAddr)
	}
	return nil
}
