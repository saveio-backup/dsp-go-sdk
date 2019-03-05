package utils

import (
	"fmt"
	"strings"

	"github.com/oniio/dsp-go-sdk/common"
)

func GenOniLink(fileHashStr, fileName string, fileSize, blockNum uint64, trackers []string) string {
	link := fmt.Sprintf("oni://%s", fileHashStr)
	if len(fileName) > 0 {
		link += fmt.Sprintf("&name=%s", fileName)
	}
	if fileSize > 0 {
		link += fmt.Sprintf("&size=%d", fileSize)
	}
	if blockNum > 0 {
		link += fmt.Sprintf("&blocknum=%d", blockNum)
	}
	for _, t := range trackers {
		link += fmt.Sprintf("&tr=%s", t)
	}
	return link
}

func GetFileHashFromLink(link string) string {
	idx := strings.Index(link, fmt.Sprintf("://%s", common.PROTO_NODE_PREFIX))
	if idx != -1 {
		return link[idx+3 : idx+3+common.FILE_HASH_LEN]
	}
	idx = strings.Index(link, fmt.Sprintf("://%s", common.RAW_NODE_PREFIX))
	if idx != -1 {
		return link[idx+3 : idx+3+common.FILE_HASH_LEN]
	}
	return ""

}
