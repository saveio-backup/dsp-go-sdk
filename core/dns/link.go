package dns

import (
	"strings"
	"time"

	"github.com/saveio/dsp-go-sdk/consts"
	sdkErr "github.com/saveio/dsp-go-sdk/error"
	"github.com/saveio/dsp-go-sdk/types/link"
	"github.com/saveio/dsp-go-sdk/utils/validator"
	"github.com/saveio/themis/common/log"
	"github.com/saveio/themis/smartcontract/service/native/dns"
)

// RegisterFileUrl. register file url with link
// return tx hash or error
func (d *DNS) RegisterFileUrl(url, link string) (string, error) {
	urlPrefix := consts.FILE_URL_CUSTOM_HEADER
	if !strings.HasPrefix(url, urlPrefix) && !strings.HasPrefix(url, consts.FILE_URL_CUSTOM_HEADER_PROTOCOL) {
		return "", sdkErr.New(sdkErr.INVALID_PARAMS, "url should start with %s", urlPrefix)
	}
	if !validator.ValidateDomainName(url[len(urlPrefix):]) {
		return "", sdkErr.New(sdkErr.INVALID_PARAMS, "domain name is invalid")
	}
	tx, err := d.chain.RegisterUrl(url, dns.CUSTOM_URL, link, "", consts.FILE_DNS_TTL)
	if err != nil {
		return "", err
	}
	height, err := d.chain.PollForTxConfirmed(time.Duration(consts.TX_CONFIRM_TIMEOUT)*time.Second, tx)
	if err != nil || height == 0 {
		return "", sdkErr.New(sdkErr.CHAIN_ERROR, "tx confirm err %s", err)
	}

	d.chain.WaitForGenerateBlock(time.Duration(consts.TX_CONFIRM_TIMEOUT)*time.Second, 1)

	return tx, nil
}

// DeleteFileUrl. unregister url from chain
func (d *DNS) DeleteFileUrl(url string) (string, error) {
	urlPrefix := consts.FILE_URL_CUSTOM_HEADER
	if !strings.HasPrefix(url, urlPrefix) && !strings.HasPrefix(url, consts.FILE_URL_CUSTOM_HEADER_PROTOCOL) {
		return "", sdkErr.New(sdkErr.INVALID_PARAMS, "url should start with %s", urlPrefix)
	}
	tx, err := d.chain.DeleteUrl(url)
	if err != nil {
		return "", err
	}
	height, err := d.chain.PollForTxConfirmed(time.Duration(consts.TX_CONFIRM_TIMEOUT)*time.Second, tx)
	if err != nil || height == 0 {
		return "", sdkErr.New(sdkErr.CHAIN_ERROR, "tx confirm err %s", err)
	}
	return tx, nil
}

// BindFileUrl. bind url with link
func (d *DNS) BindFileUrl(url, link string) (string, error) {
	tx, err := d.chain.BindUrl(uint64(dns.NameTypeNormal), url, link, link, consts.FILE_DNS_TTL)
	if err != nil {
		return "", err
	}
	height, err := d.chain.PollForTxConfirmed(time.Duration(consts.TX_CONFIRM_TIMEOUT)*time.Second, tx)
	if err != nil || height == 0 {
		return "", sdkErr.New(sdkErr.CHAIN_ERROR, "tx confirm err %s", err)
	}
	return tx, nil
}

func (d *DNS) GenLink(fileHashStr, fileName, blocksRoot, fileOwner string, fileSize, totalCount uint64) string {
	return link.GenOniLinkJSONString(&link.URLLink{
		FileHashStr: fileHashStr,
		FileName:    fileName,
		FileOwner:   fileOwner,
		FileSize:    fileSize,
		BlockNum:    totalCount,
		Trackers:    d.trackerUrls,
		BlocksRoot:  blocksRoot,
	})
}

// GetLinkFromUrl. query link of url
func (d *DNS) GetLinkFromUrl(url string) string {
	info, err := d.chain.QueryUrl(url, d.chain.Address())
	log.Debugf("query url %s %s info %s err %s", url, d.chain.WalletAddress(), info, err)
	if err != nil || info == nil {
		return ""
	}
	return string(info.Name)
}

// UpdatePluginVersion. update plugin url with version
func (d *DNS) UpdatePluginVersion(urlType uint64, url, linkStr string, urlVersion link.URLVERSION) (string, error) {
	urlPrefix := consts.FILE_URL_CUSTOM_HEADER_PROTOCOL
	if !strings.HasPrefix(url, urlPrefix) {
		return "", sdkErr.New(sdkErr.INTERNAL_ERROR, "url should start with %s", urlPrefix)
	}
	if !validator.ValidateDomainName(url[len(urlPrefix):]) {
		return "", sdkErr.New(sdkErr.INTERNAL_ERROR, "domain name is invalid")
	}
	nameInfo, err := d.chain.QueryUrl(url, d.chain.Address())
	log.Debugf("query url %s %s info %s err %s", url, d.chain.WalletAddress(), nameInfo, err)
	info := urlVersion.String()
	if nameInfo != nil {
		info = string(nameInfo.Desc) + link.PLUGIN_URLVERSION_SPLIT + info
	}
	tx, err := d.chain.BindUrl(urlType, url, linkStr, info, consts.FILE_DNS_TTL)
	if err != nil {
		return "", err
	}
	height, err := d.chain.PollForTxConfirmed(time.Duration(consts.TX_CONFIRM_TIMEOUT)*time.Second, tx)
	if err != nil || height == 0 {
		return "", sdkErr.New(sdkErr.CHAIN_ERROR, "tx confirm err %s", err)
	}
	return tx, nil
}

// GetPluginVersionFromUrl. get plugin version from url
func (d *DNS) GetPluginVersionFromUrl(url string) string {
	info, err := d.chain.QueryUrl(url, d.chain.Address())
	log.Debugf("query url %s %s info %s err %s", url, d.chain.WalletAddress(), info, err)
	if err != nil || info == nil {
		return ""
	}
	return string(info.Desc)
}

// GetFileHashFromUrl. get file hash of url
func (d *DNS) GetFileHashFromUrl(url string) string {
	linkStr := d.GetLinkFromUrl(url)
	log.Debugf("url %s has link %s", url, linkStr)
	if len(linkStr) == 0 {
		return ""
	}
	l, err := link.DecodeLinkStr(linkStr)
	if err != nil {
		return ""
	}
	return l.FileHashStr
}

// GetLinkValues. Decode url link from string
func (d *DNS) GetLinkValues(linkStr string) (*link.URLLink, error) {
	return link.DecodeLinkStr(linkStr)
}
