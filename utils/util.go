package utils

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math/rand"
	"net"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/saveio/dsp-go-sdk/common"
	"github.com/saveio/dsp-go-sdk/network/message/types/file"
	chainCom "github.com/saveio/themis/common"
	"github.com/saveio/themis/common/log"
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
	if len(protocol) == 0 {
		log.Warnf("full host addr no protocol: %s", protocol)
		protocol = "tcp"
	}
	if strings.Index(hostAddr, protocol) != -1 {
		return hostAddr
	}
	return fmt.Sprintf("%s://%s", protocol, hostAddr)
}

func IsHostAddrValid(hostAddr string) bool {
	host := hostAddr
	protocolIdx := strings.Index(hostAddr, "://")
	if protocolIdx != -1 {
		host = hostAddr[protocolIdx+3:]
	}
	portIndex := strings.Index(host, ":")
	if portIndex != -1 {
		host = host[:portIndex]
	}
	ip := net.ParseIP(host)
	log.Debugf("hostAddr %s, host %s, ip %v", hostAddr, host, ip)
	if ip != nil {
		return true
	}
	return false
}

func GetFileFullPath(dirPath, fileHash, fileName string, encrypted bool) string {
	if strings.LastIndex(dirPath, "/") != len(dirPath)-1 {
		dirPath = dirPath + "/"
	}
	if len(fileName) == 0 {
		if encrypted {
			return dirPath + fileHash + common.ENCRYPTED_FILE_EXTENSION
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
			fullPath += common.ENCRYPTED_FILE_EXTENSION
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

func StringToSha256Hex(str string) string {
	hash := sha256.Sum256([]byte(str))
	hexStr := hex.EncodeToString(hash[:])
	return hexStr
}

// GetDecryptedFilePath. get file path after dectypted file
func GetDecryptedFilePath(filePath, fileName string) string {
	i := strings.LastIndex(filePath, common.ENCRYPTED_FILE_EXTENSION)
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

func GetMilliSecTimestamp() uint64 {
	return uint64(time.Now().UnixNano() / int64(time.Millisecond))
}

// StringSliceToKeyMap. convert a string slice to map with default value ""
func StringSliceToKeyMap(input []string) map[string]string {
	m := make(map[string]string, len(input))
	for _, key := range input {
		m[key] = ""
	}
	return m
}

func GenIdByTimestamp(r *rand.Rand) string {
	if r == nil {
		r = rand.New(rand.NewSource(time.Now().UnixNano()))
	}
	return fmt.Sprintf("%d%d", time.Now().UnixNano(), 100000+r.Int31n(900000))
}

// WalletAddrsToBase58. convert address to base58 format
func WalletAddrsToBase58(addrs []chainCom.Address) []string {
	addr := make([]string, 0, len(addrs))
	for _, a := range addrs {
		addr = append(addr, a.ToBase58())
	}
	return addr
}

// WalletAddrsToBase58. convert address to base58 format
func Base58ToWalletAddrs(addrs []string) []chainCom.Address {
	addr := make([]chainCom.Address, 0, len(addrs))
	for _, a := range addrs {
		address, err := chainCom.AddressFromBase58(a)
		if err != nil {
			continue
		}
		addr = append(addr, address)
	}
	return addr
}

// WalletHostAddressMap. get wallet address map
func WalletHostAddressMap(addrs []chainCom.Address, hosts []string) map[string]string {
	m := make(map[string]string)
	if len(addrs) != len(hosts) {
		return nil
	}
	for i, a := range addrs {
		m[a.ToBase58()] = hosts[i]
	}
	return m
}

func MergeTwoAddressMap(addr1, addr2 map[string]string) map[string]string {
	m := make(map[string]string)
	for key, value := range addr1 {
		m[key] = value
	}
	for key, value := range addr2 {
		m[key] = value
	}
	return m
}

func RemoveDuplicated(input []string) []string {
	exist := make(map[string]struct{}, 0)
	output := make([]string, 0, len(input))
	for _, url := range input {
		if _, ok := exist[url]; ok {
			continue
		}
		exist[url] = struct{}{}
		output = append(output, url)
	}
	return output
}
