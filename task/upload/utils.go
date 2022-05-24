package upload

import (
	"fmt"
	"strings"

	"github.com/saveio/dsp-go-sdk/consts"
	sdkErr "github.com/saveio/dsp-go-sdk/error"
	"github.com/saveio/dsp-go-sdk/store"
	"github.com/saveio/themis/common"
	chainCom "github.com/saveio/themis/common"
	fs "github.com/saveio/themis/smartcontract/service/native/savefs"
)

// uploadOptValid check upload opt valid
func uploadOptValid(filePath string, opt *fs.UploadOption) error {
	if !common.FileExisted(filePath) {
		return sdkErr.New(sdkErr.INVALID_PARAMS, "file %s not exist", filePath)
	}
	if opt.Encrypt {
		if len(opt.EncryptPassword) == 0 && len(opt.EncryptNodeAddr) == 0 {
			return sdkErr.New(sdkErr.INVALID_PARAMS, "encrypt password or encrypt node addr missed")
		}
	}
	return nil
}

func keyOfBlockHashAndIndex(hash string, index uint64) string {
	return fmt.Sprintf("%s-%d", hash, index)
}

func fsWhiteListToWhiteList(whiteList fs.WhiteList) []*store.WhiteList {
	wh := make([]*store.WhiteList, 0)
	for _, r := range whiteList.List {
		wh = append(wh, &store.WhiteList{
			Address:     r.Addr.ToBase58(),
			StartHeight: r.BaseHeight,
			EndHeight:   r.ExpireHeight,
		})
	}
	return wh
}

func whiteListToFsWhiteList(whiteList []*store.WhiteList) fs.WhiteList {
	wh := fs.WhiteList{
		Num: uint64(len(whiteList)),
	}
	wh.List = make([]fs.Rule, 0)
	for _, r := range whiteList {
		addr, _ := chainCom.AddressFromBase58(r.Address)
		wh.List = append(wh.List, fs.Rule{
			Addr:         addr,
			BaseHeight:   r.StartHeight,
			ExpireHeight: r.EndHeight,
		})
	}
	return wh
}

func getFileSizeWithBlockCount(cnt uint64) uint64 {
	size := consts.CHUNK_SIZE * cnt / 1024
	if size == 0 {
		return 1
	}
	return size
}

func removeLocalIPNodes(list []string) []string {
	newList := make([]string, 0)
	for _, item := range list {
		isLocalIP := strings.Contains(item, "127.0.0.1") ||
			strings.Contains(item, "localhost") ||
			strings.Contains(item, "192.168.") ||
			strings.Contains(item, "172.23..") ||
			strings.Contains(item, "10.0.")
		if isLocalIP {
			continue
		}
		newList = append(newList, item)
	}
	return newList
}
