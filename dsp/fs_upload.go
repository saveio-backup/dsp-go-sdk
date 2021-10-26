package dsp

import (
	sdkErr "github.com/saveio/dsp-go-sdk/error"
	"github.com/saveio/dsp-go-sdk/task/types"
	chainCom "github.com/saveio/themis/common"
	"github.com/saveio/themis/common/log"

	fs "github.com/saveio/themis/smartcontract/service/native/savefs"
)

// UploadTaskExist. check if upload task is existed by filePath hash string
func (this *Dsp) UploadTaskExist(filePath string) (bool, error) {
	return this.TaskMgr.UploadTaskExist(filePath)
}

// UploadFile upload new file logic synchronously
func (this *Dsp) UploadFile(newTask bool, taskId, filePath string, opt *fs.UploadOption) (
	uploadRet *types.UploadResult, err error) {

	defer func() {
		sdkErr, _ := err.(*sdkErr.Error)
		if uploadRet == nil {
			this.TaskMgr.EmitUploadResult(taskId, nil, sdkErr)
			if sdkErr != nil {
				log.Errorf("++++ task %s upload finish %v, err: %v", err)
			} else {
				log.Debugf("task %s is paused", taskId)
			}
		} else {
			log.Debugf("task %v upload finish, result %v", taskId, uploadRet)
			this.TaskMgr.EmitUploadResult(taskId, uploadRet, sdkErr)
		}
		// delete task from cache in the end when success
		if uploadRet != nil && err == nil {
			this.TaskMgr.DeleteUploadTask(taskId)
		}
	}()
	if newTask {
		// new task because of task id is empty, generate a new task
		uploadTask, err := this.TaskMgr.NewUploadTask(taskId)
		if err != nil {
			return nil, err
		}
		taskId = uploadTask.GetId()
	}

	uploadTask := this.TaskMgr.GetUploadTask(taskId)
	if uploadTask == nil {
		err = sdkErr.New(sdkErr.GET_FILEINFO_FROM_DB_ERROR, "task %v not found", taskId)
		return
	}
	uploadRet, err = uploadTask.Start(newTask, taskId, filePath, opt)

	return
}

// PauseUpload. pause a task
func (this *Dsp) PauseUpload(taskId string) error {
	uploadTask := this.TaskMgr.GetUploadTask(taskId)
	if uploadTask == nil {
		return sdkErr.New(sdkErr.GET_FILEINFO_FROM_DB_ERROR, "task %s not found", taskId)
	}
	if uploadTask.IsTaskPaused() {
		log.Debugf("upload task %s already paused", taskId)
		return nil
	}
	if err := uploadTask.Pause(); err != nil {
		return sdkErr.NewWithError(sdkErr.TASK_PAUSE_ERROR, err)
	}
	return nil
}

func (this *Dsp) ResumeUpload(taskId string) error {
	uploadTask := this.TaskMgr.GetUploadTask(taskId)
	if uploadTask == nil {
		return sdkErr.New(sdkErr.GET_FILEINFO_FROM_DB_ERROR, "task %s not found", taskId)
	}

	if err := uploadTask.Resume(); err != nil {
		serr, _ := err.(*sdkErr.Error)
		if serr != nil {
			this.TaskMgr.EmitUploadResult(taskId, nil, serr)
		} else {
			this.TaskMgr.EmitUploadResult(taskId, nil, sdkErr.New(sdkErr.INTERNAL_ERROR, err.Error()))
		}
		log.Errorf("resume %s failed, err", taskId, err)
		return sdkErr.NewWithError(sdkErr.TASK_RESUME_ERROR, err)
	}

	return nil
}

func (this *Dsp) CancelUpload(taskId string, gasLimit uint64) (*types.DeleteUploadFileResp, error) {

	uploadTask := this.TaskMgr.GetUploadTask(taskId)
	if uploadTask == nil {
		return nil, sdkErr.New(sdkErr.GET_FILEINFO_FROM_DB_ERROR, "task %s not found", taskId)
	}

	fileHashStrs := make([]string, 0)

	if this.TaskMgr.ExistSameUploadFile(taskId, uploadTask.GetFileHash()) {
		log.Debugf("exist same file %s uploading, ignore delete it from chain", fileHashStrs)
	} else {
		fileHashStrs = append(fileHashStrs, uploadTask.GetFileHash())
	}

	if len(fileHashStrs) == 0 {
		log.Debugf("cancel upload task %s, its file info already deleted", taskId)
		return uploadTask.Cancel("", 0)
	}

	if uploadTask.IsTaskPaying() {
		log.Debugf("upload task %s is paying, can't cancel", taskId)
		return nil, sdkErr.New(sdkErr.GET_FILEINFO_FROM_DB_ERROR, "upload task %s is paying, can't cancel", taskId)
	}

	txHashStr, txHeight, err := this.DeleteUploadFilesFromChain(fileHashStrs, gasLimit)
	log.Debugf("delete upload task %s from chain :%s, %d, %v", taskId, txHashStr, txHeight, err)
	if err != nil {
		if serr, ok := err.(*sdkErr.Error); ok && serr.Code != sdkErr.NO_FILE_NEED_DELETED {
			return nil, serr
		}
	}

	resp, err := uploadTask.Cancel(txHashStr, txHeight)
	if err != nil {
		log.Errorf("cancel task %s err %s", taskId, err)
		return nil, err
	}
	return resp, nil
}

func (this *Dsp) RetryUpload(taskId string) error {
	uploadTask := this.TaskMgr.GetUploadTask(taskId)
	if uploadTask == nil {
		return sdkErr.New(sdkErr.GET_FILEINFO_FROM_DB_ERROR, "task %s not found", taskId)
	}
	if err := uploadTask.Retry(); err != nil {
		return sdkErr.NewWithError(sdkErr.TASK_RETRY_ERROR, err)
	}
	return this.ResumeUpload(taskId)
}

func (this *Dsp) DeleteUploadFilesFromChain(fileHashStrs []string, gasLimit uint64) (string, uint32, error) {
	return this.Chain.DeleteUploadedFiles(fileHashStrs, gasLimit)
}

// DeleteUploadedFileByIds. Delete uploaded file from remote nodes. it is called by the owner
func (this *Dsp) DeleteUploadedFileByIds(ids []string, gasLimit uint64) ([]*types.DeleteUploadFileResp, error) {
	if len(ids) == 0 {
		return nil, sdkErr.New(sdkErr.DELETE_FILE_FAILED, "delete file ids is empty")
	}
	log.Debugf("DeleteUploadedFileByIds: %v", ids)
	fileHashStrs := make([]string, 0, len(ids))
	taskIds := make([]string, 0, len(ids))
	taskIdM := make(map[string]struct{}, 0)
	hashM := make(map[string]struct{}, 0)
	for _, id := range ids {
		if _, ok := taskIdM[id]; ok {
			continue
		}
		taskIdM[id] = struct{}{}
		taskIds = append(taskIds, id)
		hash := this.TaskMgr.GetTaskFileHashById(id)
		if len(hash) == 0 {
			continue
		}
		if _, ok := hashM[hash]; ok {
			continue
		}
		hashM[hash] = struct{}{}
		if this.TaskMgr.ExistSameUploadFile(id, hash) {
			log.Debugf("exist same file %s uploading, ignore delete it from chain", hash)
			continue
		}
		fileHashStrs = append(fileHashStrs, hash)
	}

	txHashStr, txHeight, err := this.DeleteUploadFilesFromChain(fileHashStrs, gasLimit)
	log.Debugf("delete upload files from chain :%s, %d, %v", txHashStr, txHeight, err)
	if err != nil {
		if serr, ok := err.(*sdkErr.Error); ok && serr.Code != sdkErr.NO_FILE_NEED_DELETED {
			return nil, serr
		}
	}
	resps := make([]*types.DeleteUploadFileResp, 0, len(fileHashStrs))
	for _, taskId := range taskIds {

		uploadTask := this.TaskMgr.GetUploadTask(taskId)
		if uploadTask == nil {
			log.Errorf("upload task %v, not found", taskId)
			continue
		}

		resp, err := uploadTask.Cancel(txHashStr, txHeight)
		if err != nil {
			log.Errorf("delete upload info from db err: %s", err)
			continue
		}
		log.Debugf("delete task success %v", resp)
		resps = append(resps, resp)
	}
	return resps, nil
}

func (this *Dsp) GetDeleteFilesStorageFee(addr chainCom.Address, fileHashStrs []string) (uint64, error) {
	return this.Chain.GetDeleteFilesStorageFee(addr, fileHashStrs)
}
