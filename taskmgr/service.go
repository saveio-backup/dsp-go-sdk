package taskmgr

import (
	"bytes"
	sdkErr "github.com/saveio/dsp-go-sdk/error"
	"github.com/saveio/dsp-go-sdk/task/base"
	"github.com/saveio/dsp-go-sdk/task/types"
	"time"

	"github.com/saveio/dsp-go-sdk/actor/client"
	"github.com/saveio/dsp-go-sdk/consts"
	netCom "github.com/saveio/dsp-go-sdk/network/common"
	"github.com/saveio/dsp-go-sdk/network/message"
	"github.com/saveio/dsp-go-sdk/network/message/types/progress"
	"github.com/saveio/dsp-go-sdk/store"
	chActor "github.com/saveio/pylons/actor/server"
	chainCom "github.com/saveio/themis/common"
	"github.com/saveio/themis/common/log"
	cUtils "github.com/saveio/themis/smartcontract/service/native/utils"
)

var (
	eventFilterName        = "eventName"
	eventFilterFileHash    = "fileHash"
	eventFilterBlockHeight = "blockHeight"
	fileProvedEventName    = "filePdpSuccess"
	eventFilterWalletAddr  = "walletAddr"
)

// ReceiveMediaTransferNotify. register receive payment notification
func (this *TaskMgr) ReceiveMediaTransferNotify() {
	log.Debugf("registerReceiveNotification")
	receiveChan, err := chActor.RegisterReceiveNotification()
	if err != nil {
		log.Errorf("register receiveChan:%v, err %v", receiveChan, err)
	}
	go func() {
		for {
			select {
			case event := <-receiveChan:
				addr, err := chainCom.AddressParseFromBytes(event.Initiator[:])
				if err != nil {
					log.Errorf("receive payment with unrecognized address %v", event)
					continue
				}
				log.Debugf("PaymentReceive amount %d from %s with paymentID %d\n",
					event.Amount, addr.ToBase58(), event.Identifier)
				taskId, err := this.db.GetTaskIdWithPaymentId(int32(event.Identifier))
				if err != nil {
					log.Errorf("get taskId with payment id failed %s", err)
					continue
				}
				shareTask := this.GetShareTask(taskId)
				if shareTask == nil {
					log.Errorf("get shareTask nil with taskId %s", taskId)
					continue
				}
				asset := consts.ASSET_NONE
				if bytes.Compare(event.TokenNetworkId[:], cUtils.UsdtContractAddress[:]) == 0 {
					asset = consts.ASSET_USDT
				}
				// delete record
				err = shareTask.DeleteFileUnpaid(addr.ToBase58(), int32(event.Identifier),
					int32(asset), uint64(event.Amount))
				if err != nil {
					log.Errorf("delete share file info %s", err)
					continue
				}

				if len(shareTask.GetFileName()) == 0 {
					// TODOV2: set share task info by downloaded task
					panic("share task is no setup")
				}
				fileHashStr := shareTask.GetFileHash()
				fileName := shareTask.GetFileName()
				fileOwner := shareTask.GetFileOwner()
				log.Debugf("delete unpaid success %v %v %v %v %v %v",
					taskId, fileHashStr, fileName, fileOwner, addr.ToBase58(), uint64(event.Amount))
				this.InsertShareRecord(taskId, fileHashStr, fileName, fileOwner,
					addr.ToBase58(), uint64(event.Amount))
			case <-this.channel.GetCloseCh():
				log.Debugf("taskmgr stop receive mediatransfer notify service")
				return
			}
		}
	}()
}

func (this *TaskMgr) StartService() {

	go this.shareService()
	go this.progressTicker.Run()
	go this.fileProvedService()

	if this.cfg.FsType == consts.FS_FILESTORE {
		return
	}
	go this.dispatchFileService()
	go this.removeFileService()
	if this.cfg.EnableBackup {
		log.Debugf("start backup file service ")
		go this.backupFileService()
	}
}

func (this *TaskMgr) dispatchFileService() {
	log.Debugf("start dispatch file service")
	ticker := time.NewTicker(time.Duration(consts.DISPATCH_FILE_DURATION) * time.Second)
	for {
		select {
		case <-ticker.C:
			tasks, err := this.db.GetUnDispatchTaskInfos(this.chain.WalletAddress())
			if err != nil {
				log.Errorf("get dispatch task failed %s", err)
				continue
			}
			log.Debugf("find %v task need to dispatch", len(tasks))
			for _, t := range tasks {
				if len(t.ReferId) == 0 {
					log.Debugf("skip dispatch task %s because its referId is empty", t.Id)
					continue
				}
				dispatchTask := this.GetDispatchTask(t.Id)
				if dispatchTask == nil {
					log.Errorf("dispatch file service get dispatch nil from taskId %s", t.Id)
					continue
				}
				if this.db.IsFileUploaded(t.Id, true) {
					log.Debugf("find %s task has uploaded, skip dispatch", t.Id)
					// update task state
					dispatchTask.SetTaskState(store.TaskStateDone)
					this.DeleteDispatchTask(t.Id)
					continue
				}
				if prepare, doing := dispatchTask.IsTaskPreparingOrDoing(); prepare || doing {
					log.Debugf("dispatch task %v is prepare %t or doing %t, skip it", t.Id, prepare, doing)
					continue
				}
				log.Debugf("get task %s to dispatch %s", t.Id, t.FileHash)
				go dispatchTask.Start()
			}
		case <-this.closeCh:
			log.Debugf("taskmgr stop dispatch file service")
			ticker.Stop()
			return
		}

	}

}

func (this *TaskMgr) shareService() {
	for {
		select {
		case req, ok := <-this.blockReqCh:
			if !ok {
				log.Errorf("block flights req channel false")
				break
			}
			if len(req) == 0 {
				break
			}

			shareTask := this.GetShareTaskByFileHash(req[0].FileHash, req[0].WalletAddress)

			go shareTask.ShareBlock(req)
		case <-this.closeCh:
			log.Debugf("taskmgr stop share file service")
			return
		}
	}
}

func (this *TaskMgr) runGetProgressTicker() bool {
	unSalve, _ := this.GetUnSlavedTasks()
	if len(unSalve) == 0 {
		return false
	}

	log.Debugf("un slaved task %v", unSalve)
	taskHasDone := 0
	for _, id := range unSalve {
		tsk := this.GetBaseTaskById(id)
		if tsk == nil {
			log.Warnf("task %s not exist, remove it", id)
			this.db.RemoveFromUnSalvedList(nil, id, store.TaskTypeUpload)
			continue
		}
		taskType := tsk.GetTaskType()
		nodeAddr, _ := this.db.GetUploadDoneNodeAddr(id)
		if len(nodeAddr) == 0 {
			continue
		}
		fileHash := tsk.GetFileHash()
		if len(fileHash) == 0 {
			log.Debugf("taskId %s get unslaved task fileHash is empty", id)
			continue
		}

		// TODO: use mem cache instread of request rpc
		info, err := this.chain.GetFileInfo(fileHash)
		if err != nil || info == nil {
			continue
		}
		if info.PrimaryNodes.AddrNum == 1 {
			taskHasDone++
			this.db.RemoveFromUnSalvedList(nil, id, taskType)
			continue
		}
		proveDetail, _ := this.chain.GetFileProveDetails(fileHash)
		if proveDetail != nil && proveDetail.ProveDetailNum == proveDetail.CopyNum+1 {
			taskHasDone++
			this.db.RemoveFromUnSalvedList(nil, id, taskType)
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
		msg := message.NewProgressMsg(
			this.chain.WalletAddress(),
			fileHash,
			netCom.FILE_OP_PROGRESS_REQ,
			toReqPeers,
			message.WithSign(this.chain.CurrentAccount(), this.Chain().GetChainType()),
		)
		// send req progress msg
		resp, err := client.P2PSendAndWaitReply(nodeAddr, msg.MessageId, msg.ToProtoMsg())
		if err != nil {
			continue
		}
		p2pMsg := message.ReadMessage(resp)
		if p2pMsg == nil {
			continue
		}
		progress := p2pMsg.Payload.(*progress.Progress)
		if progress.Hash != fileHash {
			continue
		}
		// update progress
		progressSum := uint64(0)
		for _, info := range progress.Infos {
			log.Debugf("receive %s-%s progress wallet: %v, progress: %d",
				progress.Hash, fileHash, info.NodeAddr, info.Count)
			oldProgress := this.db.GetTaskPeerProgress(id, info.NodeAddr)
			if oldProgress != nil && oldProgress.Progress > uint64(info.Count) {
				progressSum += oldProgress.Progress
				continue
			}
			if err := this.db.UpdateTaskPeerProgress(id, info.NodeAddr, uint64(info.Count)); err != nil {
				continue
			}
			progressSum += uint64(info.Count)
		}
		log.Debugf("progressSum: %d, total: %d", progressSum, uint64(len(toReqPeers))*info.FileBlockNum)
		if progressSum != uint64(len(toReqPeers))*uint64(info.FileBlockNum) {
			continue
		}
		taskHasDone++
		log.Debugf("remove unslaved task %s", id)
		this.db.RemoveFromUnSalvedList(nil, id, taskType)
	}
	if taskHasDone == len(unSalve) {
		return true
	}
	return false
}

func (this *TaskMgr) retryTaskService() bool {
	// if this.state.Get() != state.ModuleStateActive {
	// 	log.Debugf("stop retry task since module is stopped")
	// 	return true
	// }
	uploadIds := this.GetUploadTasksToRetry()
	for _, taskId := range uploadIds {
		uploadTask := this.GetUploadTask(taskId)
		log.Debugf("retry upload task service running, retry %s, is nil %t", taskId, uploadTask == nil)
		if uploadTask == nil {
			continue
		}
		go uploadTask.Resume()
	}
	downloadIds := this.GetDownloadTasksToRetry()
	for _, taskId := range downloadIds {
		downloadTask := this.GetDownloadTask(taskId)
		log.Debugf("retry download task service running, retry %s, is nil %t", taskId, downloadTask == nil)
		if downloadTask == nil {
			continue
		}
		go downloadTask.Resume()
	}
	return false
}

// startCheckRemoveFiles. check to remove files after prove PDP done
func (this *TaskMgr) removeFileService() {
	log.Debugf("StartCheckRemoveFiles ")
	ticker := time.NewTicker(time.Duration(consts.REMOVE_FILES_DURATION) * time.Second)
	for {
		select {
		case <-ticker.C:

			files := this.fs.RemovedExpiredFiles()
			if len(files) == 0 {
				continue
			}
			for _, f := range files {
				hash, ok := f.(string)
				if !ok {
					continue
				}

				taskId := this.GetDownloadedTaskId(hash, this.chain.WalletAddress())
				log.Debugf("delete removed file %s %s", taskId, hash)
				this.DeleteFileFromChain(taskId)
				this.DeleteDownloadedFile(taskId)

				dispatchTask := this.GetDispatchTaskByReferId(taskId)
				if dispatchTask != nil {
					dispatchTask.SetTaskState(store.TaskStateCancel)
					this.DeleteDispatchTask(dispatchTask.GetId())
				}
			}

		case <-this.closeCh:
			log.Debugf("taskmgr stop check removed file service")
			ticker.Stop()
			return
		}
	}
}

func (this *TaskMgr) DeleteFileFromChain(taskId string) error {
	if len(taskId) == 0 {
		return sdkErr.New(sdkErr.DELETE_FILE_FAILED, "delete taskId is empty")
	}
	tsk := this.GetDownloadTask(taskId)
	if tsk == nil {
		return sdkErr.New(sdkErr.DELETE_FILE_FAILED, "task %s is nil", taskId)
	}
	fileHashStr := tsk.GetFileHash()
	_, err := this.Chain().DeleteFiles([]string{fileHashStr}, 0)
	if err != nil {
		log.Errorf("delete file %s from chain err %s", fileHashStr, err)
		return err
	}
	url := tsk.GetUrl()
	if url == "" {
		return nil
	}
	_, err = this.Chain().DeleteUrl(url)
	if err != nil {
		log.Errorf("delete url %s from chain err %s", url, err)
		return err
	}
	return nil
}

// startCheckFileProved. check file has proved service
func (this *TaskMgr) fileProvedService() {
	log.Infof("start fileProvedService")
	notify := this.fs.RegChainEventNotificationChannel()
	for {
		select {
		case event := <-notify:
			log.Infof("fileProvedService event %v", event)
			if event == nil {
				log.Errorf("receive empty event")
				continue
			}
			eventName, _ := event[eventFilterName].(string)
			log.Infof("fileProvedService eventName %v", eventName)
			if len(eventName) == 0 {
				log.Debugf("wrong event name type %v, type %T", event, event[eventFilterName])
				continue
			}
			if eventName != fileProvedEventName {
				log.Debugf("wrong event name %v, expect %v", eventName, fileProvedEventName)
				continue
			}
			fileHash, _ := event[eventFilterFileHash].(string)
			if len(fileHash) == 0 {
				log.Debugf("wrong event name type %v, type %T", event, event[eventFilterFileHash])
				continue
			}
			walletAddr, _ := event[eventFilterWalletAddr].(string)
			if walletAddr != this.chain.WalletAddress() {
				log.Debugf("wrong event name type %v, type %T, walletAddr %s, expect %v",
					event, event[eventFilterWalletAddr], walletAddr, this.chain.WalletAddress())
				continue
			}

			val, ok := this.fileProvedCh.Load(fileHash)
			if !ok {
				log.Debugf("file %s has proved, but notify channel is not found", fileHash)
				continue
			}
			ch, ok := val.(chan uint32)
			if !ok {
				log.Debugf("file %s has proved, but notify channel wrong type", fileHash)
				continue
			}
			log.Debugf("file %s proved success notify height %v", fileHash, event[eventFilterBlockHeight])
			ch <- event[eventFilterBlockHeight].(uint32)

		case <-this.closeCh:
			log.Debugf("taskmgr stop check file proved service")
			return
		}
	}
}

// backupFileService. start a backup file service to find backup jobs.
func (this *TaskMgr) backupFileService() {
	ticker := time.NewTicker(time.Duration(consts.BACKUP_FILE_DURATION) * time.Second)
	for {
		select {
		case <-ticker.C:
			log.Debugf("backupFileService ticked")
			sectors, err := this.chain.GetSectorInfosForNode(this.chain.WalletAddress())
			if err != nil {
				log.Errorf("get sector info err %s", err)
				continue
			}
			for _, sector := range sectors.Sectors {
				for _, file := range sector.FileList.List {
					fileHash := string(file.Hash)
					list, err := this.fs.GetLocalSectorFileList(sector.SectorID)
					if err != nil {
						continue
					}
					for _, v := range list {
						if v == fileHash {
							continue
						}
					}
					info, err := this.chain.GetFileInfo(fileHash)
					if err != nil {
						log.Errorf("get file info err %s", err)
						continue
					}
					height, err := this.chain.GetCurrentBlockHeight()
					if err != nil {
						log.Errorf("get current block height err %s", err)
						continue
					}
					if uint64(height) > info.ExpiredHeight+2*info.ProveInterval {
						this.fs.RemoveFileListPush(fileHash)
						log.Debugf("file %v is expired, expired height %v, current height %v", fileHash, info.ExpiredHeight, height)
						continue
					}
					downloadTask, err := this.NewDownloadTask("")
					if err != nil {
						log.Errorf("new download task err %s", err)
						continue
					}
					downloadTask.SetInfoWithOptions(base.FileHash(fileHash))
					var fileName, fileOwner, blocksRoot string
					var blockNum, realFileSize uint64
					if info != nil {
						fileName = string(info.FileDesc)
						fileOwner = info.FileOwner.ToBase58()
						blocksRoot = string(info.BlocksRoot)
						blockNum = info.FileBlockNum
						realFileSize = info.RealFileSize
					}
					maxPeerCnt := consts.MAX_PEERCNT_FOR_DOWNLOAD
					opt := &types.DownloadOption{
						FileName:     fileName,
						Asset:        1,
						InOrder:      false,
						BlocksRoot:   blocksRoot,
						DecryptPwd:   "",
						Free:         true,
						SetFileName:  true,
						FileOwner:    fileOwner,
						MaxPeerCnt:   maxPeerCnt,
						BlockNum:     blockNum,
						RealFileSize: realFileSize,
					}
					err = downloadTask.Start(opt)
					if err != nil {
						log.Errorf("start download task err %s", err)
						continue
					}
					log.Debugf("backup file %s task start success", fileHash)
				}
			}
		case <-this.closeCh:
			log.Debugf("taskmgr stop check removed file service")
			ticker.Stop()
			return
		}
	}
}
