package poc

import (
	"errors"
	"fmt"
	"math"
	"os"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/saveio/dsp-go-sdk/consts"
	"github.com/saveio/dsp-go-sdk/task/base"
	"github.com/saveio/dsp-go-sdk/task/upload"
	"github.com/saveio/max/max/sector"
	"github.com/saveio/themis/common/log"
	"github.com/saveio/themis/smartcontract/service/native/savefs"
)

var (
	InsufficientBalanceErr = errors.New("insufficient balance")
)

func (p *PocTask) CreateSectorForPlot(plotCfg *PlotConfig) (string, string, error) {
	fileName := GetPlotFileName(plotCfg)
	fileName = path.Join(plotCfg.Path, fileName)
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

func (p *PocTask) AddPlotFile(plotCfg *PlotConfig) error {

	fileName := GetPlotFileName(plotCfg)
	fileName = path.Join(plotCfg.Path, fileName)
	log.Infof("AddPlotFile add plot file %s", fileName)

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

	prefixStr := ""

	blockHashes, err := p.Mgr.Fs().NodesFromFile(fileName, prefixStr, false, "")
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

	currentHeight, err := p.Mgr.Chain().GetCurrentBlockHeight()
	if err != nil {
		return err
	}

	numericId, _ := strconv.Atoi(plotCfg.NumericID)

	sp := &StoreFileParam{
		fileHash:       fileHash,
		blocksRoot:     "",
		blockNum:       plotCfg.Nonces,
		blockSize:      consts.CHUNK_SIZE_KB,
		proveLevel:     savefs.PROVE_LEVEL_HIGH,
		expiredHeight:  uint64(currentHeight + consts.MAX_PLOT_FILE_EXPIRED_BLOCK),
		copyNum:        0,
		fileDesc:       []byte(fileName),
		privilege:      savefs.PRIVATE,
		proveParam:     proveParam,
		storageType:    savefs.FileStorageTypeCustom,
		realFileSize:   consts.CHUNK_SIZE_KB * plotCfg.Nonces,
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

	if err := p.SetInfoWithOptions(base.FileHash(fileHash)); err != nil {
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

func GetPlotFileName(cfg *PlotConfig) string {
	startStr := strconv.Itoa(int(cfg.StartNonce))
	noncesStr := strconv.Itoa(int(cfg.Nonces))
	return strings.Join([]string{cfg.NumericID, startStr, noncesStr}, "_")
}
