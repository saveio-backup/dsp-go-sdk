package utils

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
