package addr

import (
	chainCom "github.com/saveio/themis/common"
)

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
