package dsp

import (
	"github.com/saveio/dsp-go-sdk/store"
	"github.com/saveio/dsp-go-sdk/task"
	"github.com/saveio/themis/common/log"
)

func (this *Dsp) RecoverDBLossTask() error {
	list, err := this.Chain.Native.Fs.GetFileList(this.Account.Address)
	if err != nil {
		log.Errorf("get file list err %s", err)
	}
	uploadHashes := make([]string, 0, int(list.FileNum))
	nameMap := make(map[string]string, 0)
	for _, h := range list.List {
		fileHashStr := string(h.Hash)
		id := this.taskMgr.TaskId(fileHashStr, this.WalletAddress(), store.TaskTypeUpload)
		exist := this.taskMgr.TaskExistInDB(id)
		if exist {
			continue
		}
		info, _ := this.Chain.Native.Fs.GetFileInfo(fileHashStr)
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
	err = this.taskMgr.RecoverDBLossTask(uploadHashes, nameMap, this.WalletAddress())
	if err != nil {
		log.Errorf("recover DB loss task err %s", err)
	}
	return err
}

func (this *Dsp) GetProgressInfo(taskId string) *task.ProgressInfo {
	return this.taskMgr.GetProgressInfo(taskId)
}

func (this *Dsp) GetTaskState(taskId string) (task.TaskState, error) {
	if this.taskMgr == nil {
		return task.TaskStateNone, nil
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
			for node, cnt := range v.Count {
				log.Infof("file:%s, hash:%s, total:%d, peer:%s, uploaded:%d, progress:%f", v.FileName, v.FileHash, v.Total, node, cnt, float64(cnt)/float64(v.Total))
				stop = (cnt == v.Total)
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
	fileName, _ := this.taskMgr.FileNameFromTask(id)
	return fileName
}

func (this *Dsp) GetTaskFileHash(id string) string {
	fileHash, _ := this.taskMgr.TaskFileHash(id)
	return fileHash
}

func (this *Dsp) GetUploadTaskId(fileHashStr string) string {
	return this.taskMgr.TaskId(fileHashStr, this.WalletAddress(), store.TaskTypeUpload)
}

func (this *Dsp) GetDownloadTaskIdByUrl(url string) string {
	fileHash := this.GetFileHashFromUrl(url)
	return this.taskMgr.TaskId(fileHash, this.WalletAddress(), store.TaskTypeDownload)
}

func (this *Dsp) GetUrlOfUploadedfile(fileHashStr string) string {
	return this.taskMgr.GetUrlOfUploadedfile(fileHashStr, this.WalletAddress())
}

func (this *Dsp) GetTaskIdList(offset, limit uint32, ft store.TaskType, allType, reverse bool) []string {
	return this.taskMgr.GetTaskIdList(offset, limit, ft, allType, reverse)
}

func (this *Dsp) DeleteTaskIds(ids []string) error {
	return this.taskMgr.DeleteTaskIds(ids)
}
