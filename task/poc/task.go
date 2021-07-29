package poc

import (
	"fmt"
	"os"
	"path"
	"strconv"
	"strings"

	"github.com/saveio/dsp-go-sdk/store"
	"github.com/saveio/dsp-go-sdk/task/upload"
	"github.com/saveio/dsp-go-sdk/types/prefix"
	"github.com/saveio/themis/common/log"
	"github.com/saveio/themis/smartcontract/service/native/savefs"
)

type PocTask struct {
	*upload.UploadTask // base task
}

func NewPocTask(taskId string, taskType store.TaskType, db *store.TaskDB) *PocTask {
	dt := &PocTask{
		UploadTask: upload.NewUploadTask(taskId, taskType, db),
	}
	return dt
}

func (p *PocTask) AddPlotFile(plotCfg *PlotConfig) error {

	fileName := GetPlotFileName(plotCfg)
	fileName = path.Join(plotCfg.Path, fileName)
	log.Infof("add plot file %s", fileName)

	fileStat, err := os.Stat(fileName)
	if err != nil {
		return err
	}

	fileSize := uint64(fileStat.Size())
	enough, err := p.isPocSectorEnough(fileSize)
	if err != nil {
		return err
	}
	if !enough {
		return fmt.Errorf("poc sectors not enough for file size %v, please create one", fileSize)
	}

	filePrefix := &prefix.FilePrefix{
		Version:    prefix.PREFIX_VERSION,
		Encrypt:    false,
		EncryptPwd: "",
		Owner:      p.Mgr.Chain().Address(),
		FileSize:   fileSize,
		FileName:   fileName,
	}
	filePrefix.MakeSalt()
	prefixStr := filePrefix.String()

	blockHashes, err := p.Mgr.Fs().NodesFromFile(fileName, prefixStr, filePrefix.Encrypt, filePrefix.EncryptPwd)
	if err != nil {
		log.Errorf("nodes from file error %s", err)
		return err
	}

	fileHash := blockHashes[0]

	err = p.Mgr.Fs().SetFsFilePrefix(fileName, prefixStr)
	if err != nil {
		return err
	}

	fileID := upload.GetFileIDFromFileHash(fileHash)
	tags, err := p.GeneratePdpTags(blockHashes, fileID)
	if err != nil {
		return err
	}

	for index, tag := range tags {
		err = p.Mgr.Fs().PutTag(blockHashes[index], fileHash, uint64(index), tag[:])
		if err != nil {
			return err
		}
	}
	tagsRoot, err := p.GetMerkleRootForTag(fileID, tags)
	if err != nil {
		return err
	}

	proveParam, err := p.Mgr.Chain().ProveParamSer(tagsRoot, fileID)
	if err != nil {
		return err
	}

	numericId, _ := strconv.Atoi(plotCfg.NumericID)

	sp := &StoreFileParam{
		fileHash:       fileHash,
		blocksRoot:     "",
		blockNum:       plotCfg.Nonces,
		blockSize:      256,
		proveLevel:     savefs.PROVE_LEVEL_HIGH,
		expiredHeight:  1000,
		copyNum:        0,
		fileDesc:       []byte("plot file"),
		privilege:      0,
		proveParam:     proveParam,
		storageType:    savefs.FileStorageTypeCustom,
		realFileSize:   256 * plotCfg.Nonces,
		primaryNodes:   nil,
		candidateNodes: nil,
		plotInfo: &savefs.PlotInfo{
			NumericID:  uint64(numericId),
			StartNonce: plotCfg.StartNonce,
			Nonces:     plotCfg.Nonces,
		},
	}

	_, _, err = p.Mgr.Chain().StoreFile(sp.fileHash,
		sp.blocksRoot,
		sp.blockNum,
		sp.blockSize,
		sp.proveLevel,
		sp.expiredHeight,
		sp.copyNum,
		sp.fileDesc,
		sp.privilege,
		sp.proveParam,
		sp.storageType,
		sp.realFileSize,
		sp.primaryNodes,
		sp.candidateNodes,
		sp.plotInfo,
	)
	if err != nil {
		return err
	}

	fileInfo, err := p.Mgr.Chain().GetFileInfo(fileHash)
	if err != nil {
		return err
	}

	log.Infof("get fileInfo success for file %s, %+v", fileHash, fileInfo)

	err = p.Mgr.Fs().StartPDPVerify(fileHash)
	if err != nil {
		return err
	}
	return nil
}

func (p *PocTask) isPocSectorEnough(fileSize uint64) (bool, error) {

	result, err := p.Mgr.Chain().GetSectorInfosForNode(p.Mgr.Chain().WalletAddress())
	if err != nil {
		log.Errorf("get sector infos for node err %s", err)
		return false, err
	}
	if result.SectorCount == 0 {
		return false, fmt.Errorf("no sector found")
	}
	for _, sec := range result.Sectors {
		if !sec.IsPlots {
			continue
		}
		if sec.Size > sec.Used+fileSize {
			return true, nil
		}
	}
	return false, nil
}

func GetPlotFileName(cfg *PlotConfig) string {
	startStr := strconv.Itoa(int(cfg.StartNonce))
	noncesStr := strconv.Itoa(int(cfg.Nonces))
	return strings.Join([]string{cfg.NumericID, startStr, noncesStr}, "_")
}
