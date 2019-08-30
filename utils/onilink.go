package utils

import (
	"encoding/base64"
	"fmt"
	"strings"

	"github.com/saveio/dsp-go-sdk/common"
)

func GenOniLink(fileHashStr, fileName, owner string, fileSize, blockNum uint64, trackers []string) string {
	link := fmt.Sprintf("%s%s", common.FILE_LINK_PREFIX, fileHashStr)
	if len(fileName) > 0 {
		link += fmt.Sprintf("&%s=%s", common.FILE_LINK_NAME_KEY, fileName)
	}
	if len(owner) > 0 {
		link += fmt.Sprintf("&%s=%s", common.FILE_LINK_OWNER_KEY, owner)
	}
	if fileSize > 0 {
		link += fmt.Sprintf("&%s=%d", common.FILE_LINK_SIZE_KEY, fileSize)
	}
	if blockNum > 0 {
		link += fmt.Sprintf("&%s=%d", common.FILE_LINK_BLOCKNUM_KEY, blockNum)
	}
	for _, t := range trackers {
		trackerUrlEncoded := base64.URLEncoding.EncodeToString([]byte(t))
		link += fmt.Sprintf("&%s=%s", common.FILE_LINK_TRACKERS_KEY, trackerUrlEncoded)
	}
	return link
}

func GetFileHashFromLink(link string) string {
	idx := strings.Index(link, fmt.Sprintf("://%s", common.PROTO_NODE_PREFIX))
	if idx != -1 {
		return link[idx+3 : idx+3+common.PROTO_NODE_FILE_HASH_LEN]
	}
	idx = strings.Index(link, fmt.Sprintf("://%s", common.RAW_NODE_PREFIX))
	if idx != -1 {
		return link[idx+3 : idx+3+common.RAW_NODE_FILE_HASH_LEN]
	}
	return ""
}

func GetFilePropertiesFromLink(link string) map[string]string {
	hash := GetFileHashFromLink(link)
	if len(hash) == 0 {
		return nil
	}
	hashIdx := strings.Index(link, hash)
	if hashIdx == -1 {
		return nil
	}
	properties := make(map[string]string, 0)
	properties[common.FILE_LINK_HASH_KEY] = hash
	remain := link[hashIdx+len(hash)+1:]
	parts := strings.Split(remain, "&")
	for _, p := range parts {
		if strings.Index(p, "=") == -1 {
			continue
		}
		kv := strings.Split(p, "=")
		if len(kv) != 2 {
			continue
		}
		properties[kv[0]] = kv[1]
	}
	return properties
}
