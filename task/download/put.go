package download

import (
	"context"
	"fmt"
	"time"

	"github.com/saveio/dsp-go-sdk/actor/client"
	"github.com/saveio/dsp-go-sdk/consts"
	netCom "github.com/saveio/dsp-go-sdk/network/common"
	"github.com/saveio/dsp-go-sdk/network/message"
	"github.com/saveio/dsp-go-sdk/store"
	"github.com/saveio/dsp-go-sdk/task/types"
	chainCom "github.com/saveio/themis/common"
	"github.com/saveio/themis/common/log"
)

func (this *DownloadTask) PutBlocks(peerAddr string, resps []*types.BlockResp) error {
	taskId := this.GetId()
	fileHashStr := this.GetFileHash()
	filePath := this.GetFilePath()
	log.Debugf("putBlocks taskId: %s, fileHash: %s, filePath %s",
		taskId, fileHashStr, filePath)
	if len(fileHashStr) != 0 {
		defer this.Mgr.Fs().PinRoot(context.TODO(), fileHashStr)
	}
	if this.DB.IsFileDownloaded(taskId) {
		log.Debugf("file has downloaded: %s", fileHashStr)
		err := this.Mgr.Fs().StartPDPVerify(fileHashStr, 0, 0, 0, chainCom.ADDRESS_EMPTY)
		if err != nil {
			return err
		}
		// dispatch downloaded blocks to other primary nodes
		go this.Mgr.DispatchTask(taskId, fileHashStr)
		return nil
	}

	blkInfos := make([]*store.BlockInfo, 0)
	for _, value := range resps {
		if len(value.Block) == 0 {
			log.Errorf("receive empty block %d of file %s", len(value.Block), fileHashStr)
			return fmt.Errorf("receive empty block %d of file %s", len(value.Block), fileHashStr)
		}
		blk := this.Mgr.Fs().EncodedToBlockWithCid(value.Block, value.Hash)
		if blk.Cid().String() != value.Hash {
			return fmt.Errorf("receive a wrong block: %s, expected: %s", blk.Cid().String(), value.Hash)
		}
		if len(value.Tag) == 0 {
			log.Errorf("receive empty tag %d of file %s and block %s",
				len(value.Tag), fileHashStr, blk.Cid().String())
			return fmt.Errorf("receive empty tag %d of file %s and block %s",
				len(value.Tag), fileHashStr, blk.Cid().String())
		}
		if err := this.Mgr.Fs().PutBlock(blk); err != nil {
			return err
		}
		log.Debugf("put block success %v-%s-%d", fileHashStr, value.Hash, value.Index)
		if err := this.Mgr.Fs().PutTag(value.Hash, fileHashStr, uint64(value.Index), value.Tag); err != nil {
			return err
		}
		log.Debugf(" put tag or done %s-%s-%d", fileHashStr, value.Hash, value.Index)
		blkInfos = append(blkInfos, &store.BlockInfo{
			Hash:       value.Hash,
			Index:      value.Index,
			DataOffset: uint64(value.Offset),
			NodeList:   []string{value.PeerAddr},
		})

	}
	if len(blkInfos) > 0 {
		if err := this.SetBlocksDownloaded(taskId, blkInfos); err != nil {
			log.Errorf("SetBlocksDownloaded taskId:%s, %s-%s-%d-%d to %s-%d-%d, err: %s",
				taskId, fileHashStr, blkInfos[0].Hash, blkInfos[0].Index, blkInfos[0].DataOffset,
				blkInfos[len(blkInfos)-1].Hash, blkInfos[len(blkInfos)-1].Index, blkInfos[len(blkInfos)-1].DataOffset,
				err)
			return err
		}
		log.Debugf("SetBlocksDownloaded taskId:%s, %s-%s-%d-%d to %s-%d-%d",
			taskId, fileHashStr, blkInfos[0].Hash, blkInfos[0].Index, blkInfos[0].DataOffset,
			blkInfos[len(blkInfos)-1].Hash, blkInfos[len(blkInfos)-1].Index, blkInfos[len(blkInfos)-1].DataOffset)
	}

	if !this.DB.IsFileDownloaded(taskId) {
		return nil
	}
	blockHashes := this.DB.FileBlockHashes(taskId)
	log.Debugf("file block hashes %d", len(blockHashes))
	if err := this.Mgr.Fs().SetFsFileBlockHashes(fileHashStr, blockHashes); err != nil {
		log.Errorf("set file blockhashes err %s", err)
		return err
	}
	// all block is saved, prove it
	proved := false
	for i := 0; i < consts.MAX_START_PDP_RETRY; i++ {
		log.Infof("received all block, start pdp verify %s, retry: %d", fileHashStr, i)
		if err := this.Mgr.Fs().StartPDPVerify(fileHashStr, 0, 0, 0, chainCom.ADDRESS_EMPTY); err == nil {
			proved = true
			break
		} else {
			log.Errorf("start pdp verify err %s", err)
		}
		time.Sleep(time.Duration(consts.START_PDP_RETRY_DELAY) * time.Second)
	}
	if !proved {
		if err := this.Mgr.Fs().DeleteFile(fileHashStr, filePath); err != nil {
			log.Errorf("put block but proved failed, delete file %s err %s", fileHashStr, err)
			return err
		}
		return this.Mgr.CleanDownloadTask(taskId)
	}
	log.Debugf("pdp verify %s success, push the file to trackers %v", fileHashStr, this.Mgr.DNS().GetTrackerList())
	this.Mgr.DNS().PushToTrackers(fileHashStr, client.P2PGetPublicAddr())
	// TODO: remove unused file info fields after prove pdp success
	this.Mgr.EmitDownloadResult(taskId, "", nil)
	this.Mgr.DeleteDownloadTask(taskId)
	if err := this.Mgr.Fs().PinRoot(context.TODO(), fileHashStr); err != nil {
		log.Errorf("pin root file err %s", err)
		return err
	}
	// dispatch downloaded blocks to other primary nodes
	go this.Mgr.DispatchTask(taskId, fileHashStr)
	doneMsg := message.NewFileMsg(fileHashStr, netCom.FILE_OP_FETCH_DONE,
		message.WithSessionId(taskId),
		message.WithWalletAddress(this.GetCurrentWalletAddr()),
		message.WithSign(this.Mgr.Chain().CurrentAccount()),
	)
	return client.P2PSend(peerAddr, doneMsg.MessageId, doneMsg.ToProtoMsg())
}
