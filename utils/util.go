package utils

import (
	"fmt"
	"regexp"
	"sort"
	"strings"

	"github.com/oniio/dsp-go-sdk/network/message/types/file"
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

func ValidateDomainName(domain string) bool {
	if len(domain) == 0 {
		return false
	}
	stringRegex := regexp.MustCompile(`^[A-Za-z0-9]+$`)
	if stringRegex.MatchString(domain) {
		return true
	}
	// Golang does not support Perl syntax ((?
	// will throw out :
	// error parsing regexp: invalid or unsupported Perl syntax: `(?!`
	//patternStr := "^((?!-)[A-Za-z0-9-]{1,63}(?<!-)\\.)+[A-Za-z]{2,6}$"
	// use regular expression without Perl syntax
	regExp := regexp.MustCompile(`^(([a-zA-Z]{1})|([a-zA-Z]{1}[a-zA-Z]{1})|([a-zA-Z]{1}[0-9]{1})|([0-9]{1}[a-zA-Z]{1})|([a-zA-Z0-9][a-zA-Z0-9-_]{1,61}[a-zA-Z0-9]))\.([a-zA-Z]{2,6}|[a-zA-Z0-9-]{2,30}\.[a-zA-Z
]{2,3})$`)
	return regExp.MatchString(domain)
}

func FullHostAddr(hostAddr, protocol string) string {
	if strings.Index(hostAddr, protocol) != -1 {
		return hostAddr
	}
	return fmt.Sprintf("%s://%s", protocol, hostAddr)
}
