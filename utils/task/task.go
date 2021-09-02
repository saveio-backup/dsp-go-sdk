package task

import (
	"fmt"
	"math/rand"
	"sort"
	"strconv"
	"strings"

	"github.com/saveio/dsp-go-sdk/consts"

	"github.com/saveio/dsp-go-sdk/network/message/types/file"
	chainCom "github.com/saveio/themis/common"
)

// SortPeersByPrice. sort peer paymentInfo by its unitprice with max peer count
func SortPeersByPrice(peerPaymentInfo map[string]*file.Payment, maxPeerCnt int) map[string]*file.Payment {
	infos := make([]*file.Payment, 0)
	keyMap := make(map[string]string, 0)
	for addr, info := range peerPaymentInfo {
		infos = append(infos, info)
		keyMap[fmt.Sprintf("%s%d%d", info.WalletAddress, info.Asset, info.UnitPrice)] = addr
	}
	sort.SliceStable(infos, func(i, j int) bool {
		return infos[i].UnitPrice < infos[j].UnitPrice
	})
	// use max cnt peers
	if maxPeerCnt > len(infos) {
		maxPeerCnt = len(infos)
	}
	newPeerPaymentInfo := make(map[string]*file.Payment, 0)
	for i := 0; i < maxPeerCnt; i++ {
		newPeerPaymentInfo[keyMap[fmt.Sprintf("%s%d%d", infos[i].WalletAddress, infos[i].Asset, infos[i].UnitPrice)]] = infos[i]
	}
	return newPeerPaymentInfo
}

func GetFileFullPath(dirPath, fileHash, fileName string, encrypted bool) string {
	if strings.LastIndex(dirPath, "/") != len(dirPath)-1 {
		dirPath = dirPath + "/"
	}
	if len(fileName) == 0 {
		if encrypted {
			return dirPath + fileHash + consts.ENCRYPTED_FILE_EXTENSION
		}
		return dirPath + fileHash
	}
	i := strings.LastIndex(fileName, ".")
	name := fileName
	ext := ""
	if i > 0 {
		name = fileName[:i]
		ext = fileName[i:]
	}

	for count := 0; count < 1000; count++ {
		fullPath := dirPath + name
		if count > 0 {
			fullPath += fmt.Sprintf(" (%d)", count)
		}
		if encrypted {
			fullPath += consts.ENCRYPTED_FILE_EXTENSION
		} else {
			if len(ext) > 0 {
				fullPath += ext
			}
		}
		exist := chainCom.FileExisted(fullPath)
		if !exist {
			return fullPath
		}
	}
	return dirPath + fileHash
}

// GetDecryptedFilePath. get file path after dectypted file
func GetDecryptedFilePath(filePath, fileName string) string {
	i := strings.LastIndex(filePath, consts.ENCRYPTED_FILE_EXTENSION)
	if i == -1 {
		return filePath + "-decrypted"
	}
	name := filePath[:i]
	if len(fileName) == 0 {
		return name
	}
	orignalExt := ""
	origExtIndex := strings.LastIndex(fileName, ".")
	if origExtIndex == -1 {
		return name
	}
	orignalExt = fileName[origExtIndex:]
	return name + orignalExt
}

// func GenIdByTimestamp(r *rand.Rand) string {
// 	if r == nil {
// 		r = rand.New(rand.NewSource(time.Now().UnixNano()))
// 	}
// 	return fmt.Sprintf("%d%d", time.Now().UnixNano(), 100000+r.Int31n(900000))
// }

// GetJitterDelay. delay go up when attemp times up
// initSec is the first delay in second
// return second
func GetJitterDelay(attempt, initSec int) uint64 {
	jitter := (float64(attempt) + rand.Float64()) * float64(initSec)
	return uint64(jitter)
}

func GetPlotFileName(nonces, startNonce uint64, numericID string) string {
	startStr := strconv.Itoa(int(startNonce))
	noncesStr := strconv.Itoa(int(nonces))
	return strings.Join([]string{numericID, startStr, noncesStr}, "_")
}
