package crypto

import (
	"fmt"
	"testing"

	"github.com/saveio/themis-go-sdk/wallet"
)

func TestGenKeypair(t *testing.T) {
	w, err := wallet.OpenWallet("./wallet.dat")
	if err != nil {
		t.Fatal(err)
	}
	acc, err := w.GetDefaultAccount([]byte("pwd"))
	if err != nil {
		t.Fatal(err)
	}
	pair := NewNetworkKeyPairWithAccount(acc)
	addr := AddressFromPubkeyHex(fmt.Sprintf("%x", pair.PublicKey))
	fmt.Printf("pub: %x, priv: %v\n", pair.PublicKey, addr)
}

func TestGetChecksumOfFile(t *testing.T) {
	checkSum, err := GetChecksumOfFile("/Users/zhijie/Downloads/cn_windows_7_professional_x64_dvd_x15-65791.iso")
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("check %v\n", checkSum)
}
