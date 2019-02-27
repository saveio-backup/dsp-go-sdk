package utils

import (
	"fmt"
	"sort"

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
