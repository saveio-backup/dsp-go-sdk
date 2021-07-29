package poc

import (
	"github.com/saveio/themis/common"
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
}

type PlotConfig struct {
	Sys        string // windows or linux
	NumericID  string // numeric ID
	StartNonce uint64 // start nonce
	Nonces     uint64 // num of nonce
	Path       string // path to store plot file
}
