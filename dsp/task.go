package dsp

import (
	"github.com/saveio/dsp-go-sdk/common"
	dspErr "github.com/saveio/dsp-go-sdk/error"
	"github.com/saveio/dsp-go-sdk/network/message/types/progress"

	"github.com/saveio/dsp-go-sdk/actor/client"
	netcomm "github.com/saveio/dsp-go-sdk/network/common"
	"github.com/saveio/dsp-go-sdk/network/message"
	"github.com/saveio/dsp-go-sdk/store"
	"github.com/saveio/dsp-go-sdk/task"
	"github.com/saveio/themis/common/log"
)

func (this *Dsp) RecoverDBLossTask() error {
	if this.chain == nil || this.account == nil {
		return nil
	}
	list, err := this.chain.GetFileList(this.account.Address)
	if err != nil {
		log.Errorf("get file list err %s", err)
	}
	if list == nil {
		return nil
	}
	uploadHashes := make([]string, 0, int(list.FileNum))
	nameMap := make(map[string]string, 0)
	for _, h := range list.List {
		fileHashStr := string(h.Hash)
		id := this.taskMgr.TaskId(fileHashStr, this.chain.WalletAddress(), store.TaskTypeUpload)
		exist := this.taskMgr.TaskExistInDB(id)
		if exist {
			continue
		}
		info, _ := this.chain.GetFileInfo(fileHashStr)
		if info == nil {
			log.Debugf("info is nil : %v", fileHashStr)
			continue
		}
		uploadHashes = append(uploadHashes, string(h.Hash))
		nameMap[fileHashStr] = string(info.FileDesc)
	}
	if len(uploadHashes) == 0 {
		return nil
	}
	log.Debugf("recover task : %v, %v", uploadHashes, nameMap)
	err = this.taskMgr.RecoverDBLossTask(uploadHashes, nameMap, this.chain.WalletAddress())
	if err != nil {
		log.Errorf("recover DB loss task err %s", err)
	}
	return err
}

func (this *Dsp) GetProgressInfo(taskId string) *task.ProgressInfo {
	return this.taskMgr.GetProgressInfo(taskId)
}

func (this *Dsp) GetTaskState(taskId string) (store.TaskState, error) {
	if this.taskMgr == nil {
		return store.TaskStateNone, nil
	}
	return this.taskMgr.GetTaskState(taskId)
}

func (this *Dsp) IsTaskExist(taskId string) bool {
	return this.taskMgr.TaskExist(taskId)
}

func (this *Dsp) Progress() {
	this.RegProgressChannel()
	go func() {
		stop := false
		for {
			v := <-this.ProgressChannel()
			for node, pro := range v.Progress {
				log.Infof("file progress:%s, hash:%s, total:%d, peer:%s, uploaded:%d, progress:%f, speed: %d",
					v.FileName, v.FileHash, v.Total, node, pro.Progress, float64(pro.Progress)/float64(v.Total),
					pro.AvgSpeed())
				stop = (pro.Progress == v.Total)
			}
			if stop {
				break
			}
		}
		// TODO: why need close
		this.CloseProgressChannel()
	}()
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

func (this *Dsp) GetTaskFileName(id string) string {
	fileName, _ := this.taskMgr.GetTaskFileName(id)
	return fileName
}

func (this *Dsp) GetTaskFileHash(id string) string {
	fileHash, _ := this.taskMgr.GetTaskFileHash(id)
	return fileHash
}

func (this *Dsp) GetUploadTaskId(fileHashStr string) string {
	return this.taskMgr.TaskId(fileHashStr, this.chain.WalletAddress(), store.TaskTypeUpload)
}

func (this *Dsp) GetDownloadTaskIdByUrl(url string) string {
	fileHash := this.dns.GetFileHashFromUrl(url)
	return this.taskMgr.TaskId(fileHash, this.chain.WalletAddress(), store.TaskTypeDownload)
}

func (this *Dsp) GetUrlOfUploadedfile(fileHashStr string) string {
	return this.taskMgr.GetUrlOfUploadedfile(fileHashStr, this.chain.WalletAddress())
}

func (this *Dsp) GetTaskIdList(offset, limit uint32, ft store.TaskType, allType, reverse, includeFailed bool) []string {
	if this == nil || this.taskMgr == nil {
		return nil
	}
	return this.taskMgr.GetTaskIdList(offset, limit, ft, allType, reverse, includeFailed)
}

func (this *Dsp) DeleteTaskIds(ids []string) error {
	return this.taskMgr.DeleteTaskIds(ids)
}

// GetFileUploadSize. get file upload size
func (this *Dsp) GetFileUploadSize(fileHashStr, nodeAddr string) (uint64, error) {
	id := this.taskMgr.TaskId(fileHashStr, this.WalletAddress(), store.TaskTypeUpload)
	if len(id) == 0 {
		return 0, dspErr.New(dspErr.TASK_NOT_EXIST, "task id is empty")
	}
	progressInfo := this.taskMgr.GetProgressInfo(id)
	if progressInfo == nil {
		return 0, nil
	}
	progress, ok := progressInfo.Progress[nodeAddr]
	if ok {
		return uint64(progress.Progress) * common.CHUNK_SIZE, nil
	}
	return uint64(progressInfo.SlaveProgress[nodeAddr].Progress) * common.CHUNK_SIZE, nil
}

func (this *Dsp) RunGetProgressTicker() bool {
	ids, err := this.taskMgr.GetUnSlavedTasks()
	if err != nil {
		return false
	}
	log.Debugf("un slaved task %v", ids)
	taskHasDone := 0
	for _, id := range ids {
		taskInfo, _ := this.taskMgr.GetTaskInfoClone(id)
		if taskInfo == nil {
			log.Warnf("task %s not exist, remove it", id)
			this.taskMgr.RemoveUnSlavedTasks(id)
			continue
		}
		nodeAddr, _ := this.taskMgr.GetUploadDoneNodeAddr(id)
		if len(nodeAddr) == 0 {
			continue
		}
		fileHash, err := this.taskMgr.GetTaskFileHash(id)
		if err != nil {
			continue
		}

		// TODO: use mem cache instread of request rpc
		info, err := this.chain.GetFileInfo(fileHash)
		if err != nil || info == nil {
			continue
		}
		if info.PrimaryNodes.AddrNum == 1 {
			taskHasDone++
			this.taskMgr.RemoveUnSlavedTasks(id)
			continue
		}
		proveDetail, _ := this.chain.GetFileProveDetails(fileHash)
		if proveDetail != nil && proveDetail.ProveDetailNum == proveDetail.CopyNum+1 {
			taskHasDone++
			this.taskMgr.RemoveUnSlavedTasks(id)
			continue
		}

		toReqPeers := make([]*progress.ProgressInfo, 0)
		hostAddrs, err := this.chain.GetNodeHostAddrListByWallets(info.PrimaryNodes.AddrList)
		if err != nil || len(hostAddrs) == 0 {
			continue
		}
		for i := 0; i < len(hostAddrs); i++ {
			if i == 0 {
				continue
			}
			toReqPeers = append(toReqPeers, &progress.ProgressInfo{
				WalletAddr: info.PrimaryNodes.AddrList[i].ToBase58(),
				NodeAddr:   hostAddrs[i],
			})
		}
		log.Debugf("send req progress msg to %v for %s", toReqPeers, fileHash)
		msg := message.NewProgressMsg(this.WalletAddress(), fileHash, netcomm.FILE_OP_PROGRESS_REQ, toReqPeers, message.WithSign(this.CurrentAccount()))
		// send req progress msg
		resp, err := client.P2pSendAndWaitReply(nodeAddr, msg.MessageId, msg.ToProtoMsg())
		if err != nil {
			continue
		}
		p2pMsg := message.ReadMessage(resp)
		progress := p2pMsg.Payload.(*progress.Progress)
		if progress.Hash != fileHash {
			continue
		}
		// update progress
		progressSum := uint32(0)
		for _, info := range progress.Infos {
			log.Debugf("receive %s-%s progress wallet: %v, progress: %d", progress.Hash, fileHash, info.NodeAddr, info.Count)
			oldProgress := this.taskMgr.GetTaskPeerProgress(id, info.NodeAddr)
			if oldProgress != nil && oldProgress.Progress > uint32(info.Count) {
				progressSum += oldProgress.Progress
				continue
			}
			if err := this.taskMgr.UpdateTaskPeerProgress(id, info.NodeAddr, uint32(info.Count)); err != nil {
				continue
			}
			progressSum += uint32(info.Count)
		}
		log.Debugf("progressSum: %d, total: %d", progressSum, uint64(len(toReqPeers))*info.FileBlockNum)
		if progressSum != uint32(len(toReqPeers))*uint32(info.FileBlockNum) {
			continue
		}
		taskHasDone++
		log.Debugf("remove unslaved task %s", id)
		this.taskMgr.RemoveUnSlavedTasks(id)
	}
	if taskHasDone == len(ids) {
		return true
	}
	return false
}

func (this *Dsp) GetDownloadTaskRemainSize(taskId string) uint32 {
	progress := this.taskMgr.GetProgressInfo(taskId)
	sum := uint32(0)
	for _, c := range progress.Progress {
		sum += c.Progress
	}
	if progress.Total > sum {
		return progress.Total - sum
	}
	return 0
}

func (this *Dsp) InsertUserspaceRecord(id, walletAddr string, size uint64,
	sizeOp store.UserspaceOperation, second uint64, secondOp store.UserspaceOperation, amount uint64,
	transferType store.UserspaceTransferType) error {
	return this.userspaceRecordDB.InsertUserspaceRecord(id, walletAddr, size,
		sizeOp, second, secondOp, amount,
		transferType)
}

// SelectUserspaceRecordByWalletAddr.
func (this *Dsp) SelectUserspaceRecordByWalletAddr(
	walletAddr string, offset, limit uint64) ([]*store.UserspaceRecord, error) {
	return this.userspaceRecordDB.SelectUserspaceRecordByWalletAddr(walletAddr, offset, limit)
}
