package download

import (
	"fmt"
	"os"

	"github.com/saveio/dsp-go-sdk/consts"
	sdkErr "github.com/saveio/dsp-go-sdk/error"
	uOS "github.com/saveio/dsp-go-sdk/utils/os"
	"github.com/saveio/themis/common/log"
)

func getFileSizeWithBlockCount(cnt uint64) uint64 {
	size := consts.CHUNK_SIZE * cnt / 1024
	if size == 0 {
		return 1
	}
	return size
}

func keyOfBlockHashAndIndex(hash string, index uint64) string {
	return fmt.Sprintf("%s-%d", hash, index)
}

// createDownloadFile. create file handler for write downloading file
func createDownloadFile(dir, filePath string) (*os.File, error) {
	err := uOS.CreateDirIfNeed(dir)
	if err != nil {
		return nil, sdkErr.NewWithError(sdkErr.INTERNAL_ERROR, err)
	}
	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR, 0666)
	log.Debugf("create download file dir:%s, path: %s, file: %v %s", dir, filePath, file, err)
	if err != nil {
		return nil, sdkErr.NewWithError(sdkErr.INTERNAL_ERROR, err)
	}
	return file, nil
}

func adjustDownloadCap(cap int, qos []int64) int {
	speedUp := canDownloadSpeedUp(qos)
	newCap := cap
	if speedUp {
		newCap = cap + 2
		if newCap > consts.MAX_REQ_BLOCK_COUNT {
			return consts.MAX_REQ_BLOCK_COUNT
		} else {
			return newCap
		}
	}
	newCap = cap - 4
	if newCap < consts.MIN_REQ_BLOCK_COUNT {
		return consts.MIN_REQ_BLOCK_COUNT
	}
	return newCap
}

func canDownloadSpeedUp(qos []int64) bool {
	if len(qos) < consts.MIN_DOWNLOAD_QOS_LEN {
		return false
	}
	if qos[len(qos)-1] >= consts.DOWNLOAD_BLOCKFLIGHTS_TIMEOUT {
		return false
	}
	qosSum := int64(0)
	for i := 0; i < consts.MIN_DOWNLOAD_QOS_LEN; i++ {
		qosSum += qos[len(qos)-i-1]
	}
	avg := qosSum / consts.MIN_DOWNLOAD_QOS_LEN
	log.Debugf("qosSum :%d, avg : %d", qosSum, avg)
	return avg < consts.DOWNLOAD_BLOCKFLIGHTS_TIMEOUT
}
