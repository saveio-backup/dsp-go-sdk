package task

import (
	"fmt"

	"github.com/saveio/dsp-go-sdk/common"
	"github.com/saveio/dsp-go-sdk/store"
	"github.com/saveio/themis/common/log"
	fs "github.com/saveio/themis/smartcontract/service/native/savefs"
)

// setter list for task info

func (this *TaskMgr) NewBatchSet(taskId string) error {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		return fmt.Errorf("[TaskMgr SetSessionId] task not found: %s", taskId)
	}
	v.NewBatchSet()
	return nil
}

// SetFileHash. set file hash of task
func (this *TaskMgr) SetFileHash(taskId, fileHash string) error {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		return fmt.Errorf("[TaskMgr SetFileHash] task not found: %s", taskId)
	}
	return v.SetFileHash(fileHash)
}

func (this *TaskMgr) SetSessionId(taskId, peerWalletAddr, id string) error {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		return fmt.Errorf("[TaskMgr SetSessionId] task not found: %s", taskId)
	}
	v.SetSessionId(peerWalletAddr, id)
	return nil
}

// SetFileName. set file name of task
func (this *TaskMgr) SetFileName(taskId, fileName string) error {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		return fmt.Errorf("[TaskMgr SetFileName] task not found: %s", taskId)
	}
	return v.SetFileName(fileName)
}
func (this *TaskMgr) SetTotalBlockCount(taskId string, totalBlockCount uint64) error {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		return fmt.Errorf("[TaskMgr SetFileName] task not found: %s", taskId)
	}
	return v.SetTotalBlockCnt(totalBlockCount)
}

// SetPrefix. set file prefix of task
func (this *TaskMgr) SetPrefix(taskId string, prefix string) error {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		return fmt.Errorf("[TaskMgr SetFileName] task not found: %s", taskId)
	}
	return v.SetPrefix(prefix)
}

func (this *TaskMgr) SetWalletAddr(taskId, walletAddr string) error {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		return fmt.Errorf("[TaskMgr SetWalletAddr] task not found: %s", taskId)
	}
	return v.SetWalletaddr(walletAddr)
}

func (this *TaskMgr) SetCopyNum(taskId string, copyNum uint64) error {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		return fmt.Errorf("[TaskMgr SetCopyNum] task not found: %s", taskId)
	}
	return v.SetCopyNum(copyNum)
}

func (this *TaskMgr) SetOnlyBlock(taskId string, only bool) error {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		return fmt.Errorf("[TaskMgr SetFileName] task not found: %s", taskId)
	}
	return v.SetOnlyBlock(only)
}

func (this *TaskMgr) SetTaskState(taskId string, state store.TaskState) error {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		return fmt.Errorf("task: %s, not exist", taskId)
	}
	log.Debugf("set task state: %s %d", taskId, state)
	return v.SetTaskState(state)
}

func (this *TaskMgr) SetWhitelistTx(taskId string, whitelistTx string) error {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		return fmt.Errorf("task: %s, not exist", taskId)
	}
	return v.SetWhiteListTx(whitelistTx)
}

func (this *TaskMgr) SetFileOwner(taskId, owner string) error {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		return fmt.Errorf("task: %s, not exist", taskId)
	}
	return v.SetFileOwner(owner)
}

func (this *TaskMgr) SetPrivateKey(taskId string, value []byte) error {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		return fmt.Errorf("task: %s, not exist", taskId)
	}
	return v.SetPrivateKey(value)
}

func (this *TaskMgr) SetStoreTx(taskId, tx string) error {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		return fmt.Errorf("task: %s, not exist", taskId)
	}
	return v.SetStoreTx(tx)
}

func (this *TaskMgr) SetRegUrlTx(taskId, tx string) error {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		return fmt.Errorf("task: %s, not exist", taskId)
	}
	return v.SetRegUrlTx(tx)
}

func (this *TaskMgr) SetBindUrlTx(taskId, tx string) error {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		return fmt.Errorf("task: %s, not exist", taskId)
	}
	return v.SetBindUrlTx(tx)
}

func (this *TaskMgr) SetUrl(taskId, url string) error {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		return fmt.Errorf("task: %s, not exist", taskId)
	}
	return v.SetUrl(url)
}

func (this *TaskMgr) SetFilePath(taskId, path string) error {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		return fmt.Errorf("task: %s, not exist", taskId)
	}
	return v.SetFilePath(path)
}

func (this *TaskMgr) SetSimpleCheckSum(taskId, checksum string) error {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		return fmt.Errorf("task: %s, not exist", taskId)
	}
	return v.SetSimpleCheckSum(checksum)
}

func (this *TaskMgr) BatchCommit(taskId string) error {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		return fmt.Errorf("task: %s, not exist", taskId)
	}
	return v.BatchCommit()
}

func (this *TaskMgr) AddUploadedBlock(taskId, blockHashStr, nodeAddr string, index uint32, dataSize, offset uint64) error {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		return fmt.Errorf("task: %s, not exist", taskId)
	}
	return v.AddUploadedBlock(taskId, blockHashStr, nodeAddr, index, dataSize, offset)
}

func (this *TaskMgr) SetBlocksUploaded(taskId, nodeAddr string, blockInfos []*store.BlockInfo) error {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		return fmt.Errorf("task: %s, not exist", taskId)
	}
	return v.SetBlocksUploaded(taskId, nodeAddr, blockInfos)
}

func (this *TaskMgr) SetFileUploadOptions(taskId string, opt *fs.UploadOption) error {
	task, ok := this.GetTaskById(taskId)
	if !ok {
		return fmt.Errorf("task: %s, not exist", taskId)
	}
	task.NewBatchSet()
	task.SetStoreType(opt.StorageType)
	task.SetCopyNum(opt.CopyNum)
	task.SetUrl(string(opt.DnsURL))
	err := task.BatchCommit()
	if err != nil {
		return err
	}
	return this.db.SetFileUploadOptions(taskId, opt)
}

func (this *TaskMgr) SetUploadProgressDone(taskId, nodeAddr string) error {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		return fmt.Errorf("task: %s, not exist", taskId)
	}
	if v.State() == store.TaskStateCancel || v.State() == store.TaskStateFailed {
		return nil
	}
	return v.SetUploadProgressDone(taskId, nodeAddr)
}

func (this *TaskMgr) SetBlockDownloaded(taskId, blockHashStr, nodeAddr string, index uint32, offset int64, links []string) error {
	v, ok := this.GetTaskById(taskId)
	if !ok {
		return fmt.Errorf("task: %s, not exist", taskId)
	}
	return v.SetBlockDownloaded(taskId, blockHashStr, nodeAddr, index, offset, links)
}

func (this *TaskMgr) AddShareTo(id, walletAddress string) error {
	return this.db.AddShareTo(id, walletAddress)
}

func (this *TaskMgr) SetFileDownloadOptions(id string, opt *common.DownloadOption) error {
	return this.db.SetFileDownloadOptions(id, opt)
}

func (this *TaskMgr) AddFileSession(fileInfoId string, sessionId, walletAddress, hostAddress string, asset, unitPrice uint64) error {
	this.SetSessionId(fileInfoId, walletAddress, sessionId)
	return this.db.AddFileSession(fileInfoId, sessionId, walletAddress, hostAddress, asset, unitPrice)
}

func (this *TaskMgr) AddFileUnpaid(id, walletAddress string, asset int32, amount uint64) error {
	return this.db.AddFileUnpaid(id, walletAddress, asset, amount)
}

func (this *TaskMgr) AddFileBlockHashes(id string, blocks []string) error {
	return this.db.AddFileBlockHashes(id, blocks)
}

func (this *TaskMgr) DeleteFileUnpaid(id, walletAddress string, asset int32, amount uint64) error {
	return this.db.DeleteFileUnpaid(id, walletAddress, asset, amount)
}

func (this *TaskMgr) DeleteTaskIds(ids []string) error {
	return this.db.DeleteTaskIds(ids)
}
