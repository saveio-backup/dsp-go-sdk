package poc

import (
	"errors"
	"fmt"
	"io/ioutil"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/saveio/dsp-go-sdk/consts"
	"github.com/saveio/dsp-go-sdk/task/base"
	"github.com/saveio/dsp-go-sdk/task/upload"
	tskUtils "github.com/saveio/dsp-go-sdk/utils/task"
	uTime "github.com/saveio/dsp-go-sdk/utils/time"
	"github.com/saveio/max/max/sector"
	"github.com/saveio/themis/common/log"
	"github.com/saveio/themis/smartcontract/service/native/savefs"
)

var (
	InsufficientBalanceErr = errors.New("insufficient balance")
)

func (p *PocTask) CreateSectorForPlot(plotCfg *PlotConfig) (string, string, error) {
	fileName := tskUtils.GetPlotFileName(plotCfg.Nonces, plotCfg.StartNonce, plotCfg.NumericID)
	fileName = filepath.Join(plotCfg.Path, fileName)
	log.Infof("add plot file %s", fileName)

	fileStat, err := os.Stat(fileName)
	if err != nil {
		return "", "", err
	}

	fileSize := uint64(fileStat.Size())
	fileSizeInKB := uint64(math.Ceil(float64(fileSize / 1024)))

	info, _ := p.Mgr.Chain().GetNodeInfoByWallet(p.Mgr.Chain().Address())
	registerTx, sectorTx := "", ""
	ipAddr := fmt.Sprintf("%v://%v", p.Mgr.Config().DspProtocol, p.Mgr.Config().DspListenAddr)
	sectorSize := fileSizeInKB
	if sectorSize < sector.MIN_SECTOR_SIZE {
		sectorSize = uint64(sector.MIN_SECTOR_SIZE + 1)
	}
	log.Infof("create sector sectorSize %v", sectorSize)
	if info == nil {
		fsSetting, err := p.Mgr.Chain().GetFsSetting()
		if err != nil {
			return "", "", err
		}
		volume := uint64(sectorSize)
		if volume < fsSetting.MinVolume {
			volume = fsSetting.MinVolume + 1
		}
		log.Infof("register node with volume %v", volume)
		registerTx, err = p.Mgr.Chain().RegisterNode(ipAddr, volume, consts.MAX_SERVICE_TIME)
		if err != nil {
			return "", "", err
		}
		log.Infof("register node tx %v", registerTx)
		_, err = p.Mgr.Chain().PollForTxConfirmed(time.Duration(consts.POLL_FOR_BLOCK_TIMEOUT)*time.Second, registerTx)
		if err != nil {
			return "", "", err
		}
		nodeInfo2, _ := p.Mgr.Chain().GetNodeInfoByWallet(p.Mgr.Chain().Address())
		log.Infof("nodeInfo2 %v", nodeInfo2)
	} else {
		log.Infof("rest %v volume %v", info.RestVol, info.Volume)
		if info.RestVol < sectorSize {
			registerTx, err = p.Mgr.Chain().UpdateNode(ipAddr, uint64(info.Volume+sectorSize), consts.MAX_SERVICE_TIME)
			if err != nil {
				return "", "", err
			}

			_, err := p.Mgr.Chain().PollForTxConfirmed(time.Duration(consts.POLL_FOR_BLOCK_TIMEOUT)*time.Second, registerTx)
			if err != nil {
				return "", "", err
			}

		}
	}

	result, err := p.Mgr.Chain().GetSectorInfosForNode(p.Mgr.Chain().WalletAddress())
	if err != nil {
		log.Errorf("get sector infos for node err %s", err)
		return "", "", err
	}

	enough, _ := p.isPocSectorEnough(fileSizeInKB)
	if enough {
		return registerTx, sectorTx, nil
	}

	sectorTx, err = p.Mgr.Chain().CreateSector(result.SectorCount+1, savefs.PROVE_LEVEL_HIGH, sectorSize, true)
	if err != nil {
		return "", "", err
	}

	_, err = p.Mgr.Chain().PollForTxConfirmed(time.Duration(consts.POLL_FOR_BLOCK_TIMEOUT)*time.Second, sectorTx)
	log.Infof("sectorTx node tx %v, err %s", sectorTx, err)
	if err != nil {
		return "", "", err
	}
	return registerTx, sectorTx, nil
}

func (p *PocTask) mockPlotFile(fileName string) error {
	baseName := filepath.Base(fileName)
	readPath := filepath.Join("./plots/", baseName)
	data, err := ioutil.ReadFile(readPath)
	if err != nil {
		return err
	}
	return ioutil.WriteFile(fileName, data, 0666)
}

// GenPlotData. Sharding file and get all blocks. Generate PDP tags and save to local storage.
func (p *PocTask) GenPlotPDPData(plotCfg *PlotConfig) error {

	fileName := tskUtils.GetPlotFileName(plotCfg.Nonces, plotCfg.StartNonce, plotCfg.NumericID)
	fileName = filepath.Join(plotCfg.Path, fileName)
	log.Infof("GenPlotData add plot file %s", fileName)
	// p.mockPlotFile(fileName)

	_, err := os.Stat(fileName)
	if err != nil {
		log.Errorf("GenPlotData file %s, err %s", fileName, err)
		return err
	}

	prefixStr := ""
	if err := p.SetInfoWithOptions(
		base.TotalBlockCnt(plotCfg.Nonces),
		base.ProveLevel(savefs.PROVE_LEVEL_HIGH),
		base.FileName(fileName),
		base.Privilege(savefs.PRIVATE),
		base.RealFileSize(consts.CHUNK_SIZE_KB*plotCfg.Nonces),
	); err != nil {
		return err
	}

	blockHashes, err := p.Mgr.Fs().NodesFromFile(fileName, prefixStr, false, "", nil)
	if err != nil {
		log.Errorf("nodes from file error %s", err)
		return err
	}

	fileHash := blockHashes[0]

	fileInfo, _ := p.Mgr.Chain().GetFileInfo(fileHash)
	if fileInfo != nil {
		log.Errorf("file %s hash %s already exists on chain", fileName, fileHash)
		return fmt.Errorf("file %s hash %s already exists on chain", fileName, fileHash)
	}

	if err := p.SetInfoWithOptions(
		base.FileHash(fileHash),
	); err != nil {
		return err
	}

	err = p.Mgr.Fs().SetFsFilePrefix(fileName, prefixStr)
	if err != nil {
		return err
	}

	fileID := upload.GetFileIDFromFileHash(fileHash)
	generateProgressCh := make(chan upload.GenearatePdpProgress)
	go func() {
		lastDate := GetMilliSecTimestamp()
		for {
			progress := <-generateProgressCh
			nowTs := GetMilliSecTimestamp()
			spent := nowTs - lastDate
			if spent > 0 {
				estimateSpent := uint64(int(spent) * progress.Total / progress.Generated)
				if estimateSpent > spent && (estimateSpent-spent > 1000) {
					progress.EstimateTime = int((estimateSpent - spent) / 1000)
				}
			} else {
				progress.EstimateTime = 0
			}
			if progress.Total == progress.Generated {
				progress.EstimateTime = 0
			}
			log.Debugf("generate progress total %v, generated %v, second %v, EstimateTime %v", progress.Total, progress.Generated, spent, progress.EstimateTime)
			p.SetGenerateProgress(progress)
			if p.AllTagGenerated() {
				log.Debugf("progress return")
				return
			}
		}
	}()
	tags, err := p.GeneratePdpTags(blockHashes, fileID, generateProgressCh)
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

	return p.SetInfoWithOptions(
		base.ProveParams(proveParam),
	)

}

func (p *PocTask) AddPlotFile(plotCfg *PlotConfig) error {

	fileName := tskUtils.GetPlotFileName(plotCfg.Nonces, plotCfg.StartNonce, plotCfg.NumericID)
	fileName = filepath.Join(plotCfg.Path, fileName)
	log.Infof("AddPlotFile add plot file %s %s", fileName, p.GetFileName())
	if fileName != p.GetFileName() {
		return fmt.Errorf("poc task %v has different fileName %s expect %v", p.GetId(), fileName, p.GetFileName())
	}

	fileStat, err := os.Stat(fileName)
	if err != nil {
		return err
	}
	fileSizeInKB := uint64(math.Ceil(float64(fileStat.Size() / 1024)))
	enough, err := p.isPocSectorEnough(fileSizeInKB)
	if err != nil {
		return err
	}
	if !enough {
		return fmt.Errorf("poc sectors not enough for file size %v, please create one", fileSizeInKB)
	}

	currentHeight, err := p.Mgr.Chain().GetCurrentBlockHeight()
	if err != nil {
		return err
	}

	numericId, _ := strconv.Atoi(plotCfg.NumericID)

	sp := &StoreFileParam{
		fileHash:       p.GetFileHash(),
		blocksRoot:     "",
		blockNum:       p.GetTotalBlockCnt(),
		blockSize:      consts.CHUNK_SIZE_KB,
		proveLevel:     p.GetProveLevel(),
		expiredHeight:  uint64(currentHeight + consts.MAX_PLOT_FILE_EXPIRED_BLOCK),
		copyNum:        0,
		fileDesc:       []byte(p.GetFileName()),
		privilege:      p.GetPrivilege(),
		proveParam:     p.GetProveParams(),
		storageType:    savefs.FileStorageTypeCustom,
		realFileSize:   p.GetRealFileSize(),
		primaryNodes:   nil,
		candidateNodes: nil,
		plotInfo: &savefs.PlotInfo{
			NumericID:  uint64(numericId),
			StartNonce: plotCfg.StartNonce,
			Nonces:     plotCfg.Nonces,
		},
		url: p.GetUrl(),
	}

	tx, height, err := p.Mgr.Chain().StoreFile(sp.fileHash,
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
		sp.url,
	)
	if err != nil {
		return err
	}

	fileHash := sp.fileHash
	fileInfo, err := p.Mgr.Chain().GetFileInfo(fileHash)
	if err != nil {
		return err
	}

	log.Infof("get fileInfo success for file %s, %+v", fileHash, fileInfo)

	if err := p.SetInfoWithOptions(
		base.FileHash(fileHash),
		base.StoreTx(tx),
		base.StoreTxHeight(height),
		base.StoreTxTime(uTime.GetMilliSecTimestamp()),
	); err != nil {
		return err
	}

	err = p.Mgr.StartPDPVerify(fileHash)
	if err != nil {
		log.Errorf("start pdp verify %s err %s", fileHash, err)
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
		log.Infof("sector %v, size %v, used %v, filesize %v", sec.IsPlots, sec.Size, sec.Used, fileSize)
		if !sec.IsPlots {
			continue
		}
		if sec.Size >= sec.Used+fileSize {
			return true, nil
		}
	}
	return false, nil
}

func GetMilliSecTimestamp() uint64 {
	return uint64(time.Now().UnixNano() / int64(time.Millisecond))
}
