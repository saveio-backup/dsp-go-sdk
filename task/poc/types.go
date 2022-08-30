package poc

import (
	"path/filepath"
	"strconv"
	"strings"

	"github.com/saveio/themis/common"
	"github.com/saveio/themis/common/log"
	"github.com/saveio/themis/smartcontract/service/native/savefs"
)

type StoreFileParam struct {
	fileHash       string
	blocksRoot     string
	blockNum       uint64
	blockSize      uint64
	proveLevel     uint64
	expiredHeight  uint64
	copyNum        uint64
	fileDesc       []byte
	privilege      uint64
	proveParam     []byte
	storageType    uint64
	realFileSize   uint64
	primaryNodes   []common.Address
	candidateNodes []common.Address
	plotInfo       *savefs.PlotInfo
	url string
}

type PlotConfig struct {
	Sys        string // windows or linux
	NumericID  string // numeric ID
	StartNonce uint64 // start nonce
	Nonces     uint64 // num of nonce
	Path       string // path to store plot file
}

func GetNonceFromName(fileName string) (uint64, uint64) {
	fileBaseName := filepath.Base(fileName)
	parts := strings.Split(fileBaseName, "_")
	if len(parts) != 3 {
		return 0, 0
	}

	startNonce, err := strconv.ParseUint(parts[1], 10, 64)
	if err != nil {
		log.Errorf("parse start nonce err %s", err)
		return 0, 0
	}

	nonce, err := strconv.ParseUint(parts[2], 10, 64)

	if err != nil {
		return 0, 0
	}

	return startNonce, nonce
}
