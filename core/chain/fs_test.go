package themis

import (
	"fmt"
	"testing"

	chainCom "github.com/saveio/themis/common"
)

func TestCopyAddress(t *testing.T) {
	addr1, err := chainCom.AddressFromBase58("ATH4DrY9ofModnNNSJMzzpWfzAhDDFFJX6")
	if err != nil {
		t.Fatal(err)
	}
	addr2, err := chainCom.AddressFromBase58("ANe8fD9kxHV4xHP2XMAfdffEQUAS7bW9hi")
	if err != nil {
		t.Fatal(err)
	}

	addrlist1 := make([]chainCom.Address, 0)
	addrlist1 = append(addrlist1, addr1)
	addrlist1 = append(addrlist1, addr2)

	fmt.Printf("addr list 1 %p, len %d\n", addrlist1, len(addrlist1))
	addrlist2 := make([]chainCom.Address, 0)
	addrlist2 = addrlist1
	fmt.Printf("addr list 2 %p, len %d\n", addrlist2, len(addrlist2))
}
