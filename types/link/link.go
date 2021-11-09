package link

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/url"
	"strconv"
	"strings"

	"github.com/saveio/dsp-go-sdk/consts"
)

const PLUGIN_URLVERSION_SPLIT = "#PV#"
const PLUGIN_URLVERSION_CHANGELOG_PREFIX = "#CL#"

type ChangeLog struct {
	ZH string
	EN string
}

func (c *ChangeLog) String() string {
	data, _ := json.Marshal(c)
	return string(data)
}

type URLVERSION struct {
	Type      uint64
	Url       string
	Version   string
	FileHash  string
	Img       string
	Title     string
	Platform  int
	ChangeLog ChangeLog
}

type UrlVersionType int

func (u *URLVERSION) String() string {
	data, _ := json.Marshal(u)
	return string(data)
}

type URLLink struct {
	Version     string   // url link version
	FileHashStr string   // file hash string
	FileName    string   // file name
	FileOwner   string   // file owner base58 address
	FileSize    uint64   // real file size (KiB)
	BlockNum    uint64   // file sharding block count
	Trackers    []string // trackers' url array
	BlocksRoot  string   // block hash merkle root
}

// String. JSON string format. e.g: {"Version":"","FileHashStr":"123","FileName":"456",
// "FileOwner":"789","FileSize":10,"BlockNum":11,"Trackers":["tcp://127.0.0.1:10340","tcp://127.0.0.2:10340"],
// "BlocksRoot":""}
func (u *URLLink) String() string {
	data, _ := json.Marshal(u)
	return string(data)
}

// OniLinkString. URL path format. e.g: oni-link://123&name=456&owner=789&size=10&
// blocknum=11&tr=dGNwOi8vMTI3LjAuMC4xOjEwMzQw&tr=dGNwOi8vMTI3LjAuMC4yOjEwMzQw
func (u *URLLink) OniLinkString() string {
	return GenOniLink(u.FileHashStr, u.FileName, u.FileOwner, u.BlocksRoot, u.FileSize, u.BlockNum, u.Trackers)
}

// [Deprecated, use GenOniLinkJSONString instead] GenOniLink. gen oni link with URL path format
func GenOniLink(fileHashStr, fileName, owner, blocksRoot string,
	fileSize, blockNum uint64, trackers []string) string {
	link := fmt.Sprintf("%s%s", consts.FILE_LINK_PREFIX, fileHashStr)
	if len(fileName) > 0 {
		link += fmt.Sprintf("&%s=%s", consts.FILE_LINK_NAME_KEY, fileName)
	}
	if len(owner) > 0 {
		link += fmt.Sprintf("&%s=%s", consts.FILE_LINK_OWNER_KEY, owner)
	}
	if len(blocksRoot) > 0 {
		link += fmt.Sprintf("&%s=%s", consts.FILE_LINK_BLOCKSROOT_KEY, blocksRoot)
	}
	if fileSize >= 0 {
		link += fmt.Sprintf("&%s=%d", consts.FILE_LINK_SIZE_KEY, fileSize)
	}
	if blockNum >= 0 {
		link += fmt.Sprintf("&%s=%d", consts.FILE_LINK_BLOCKNUM_KEY, blockNum)
	}
	for _, t := range trackers {
		trackerUrlEncoded := base64.URLEncoding.EncodeToString([]byte(t))
		link += fmt.Sprintf("&%s=%s", consts.FILE_LINK_TRACKERS_KEY, trackerUrlEncoded)
	}
	return link
}

// GenOniLinkJSONString. generate oni link with JSON string
func GenOniLinkJSONString(link *URLLink) string {
	return link.String()
}

// DecodeLinkStr. decode link string to URLLink struct
// support url path link string and json format link string
func DecodeLinkStr(linkStr string) (*URLLink, error) {
	if strings.Contains(linkStr, consts.FILE_LINK_PREFIX) {
		linkStr = strings.ReplaceAll(linkStr, consts.FILE_LINK_PREFIX,
			fmt.Sprintf("%s?%s=", consts.FILE_LINK_PREFIX, consts.FILE_LINK_HASH_KEY))
		u, err := url.Parse(linkStr)
		if err != nil {
			return nil, err
		}
		link := &URLLink{}
		for key, val := range u.Query() {
			if len(val) == 0 {
				continue
			}
			switch key {
			case consts.FILE_LINK_HASH_KEY:
				link.FileHashStr = val[0]
			case consts.FILE_LINK_NAME_KEY:
				link.FileName = val[0]
			case consts.FILE_LINK_SIZE_KEY:
				link.FileSize, _ = strconv.ParseUint(val[0], 10, 64)
			case consts.FILE_LINK_OWNER_KEY:
				link.FileOwner = val[0]
			case consts.FILE_LINK_BLOCKNUM_KEY:
				link.BlockNum, _ = strconv.ParseUint(val[0], 10, 64)
			case consts.FILE_LINK_TRACKERS_KEY:
				link.Trackers = make([]string, 0, len(val))
				for _, encodedUrl := range val {
					decoded, _ := base64.URLEncoding.DecodeString(encodedUrl)
					if len(decoded) == 0 {
						continue
					}
					link.Trackers = append(link.Trackers, string(decoded))
				}
			}
		}
		return link, nil
	}
	link := &URLLink{}
	if err := json.Unmarshal([]byte(linkStr), &link); err != nil {
		return nil, err
	}
	return link, nil
}
