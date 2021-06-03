package link

import (
	"fmt"
	"testing"
)

func TestGenOniLink(t *testing.T) {
	l := GenOniLink("123", "456", "789", "000", 10, 11, []string{"tcp://127.0.0.1:10340", "tcp://127.0.0.2:10340"})
	fmt.Printf("link %s\n", l)
}

func TestDecodeOldLink(t *testing.T) {
	oldLink := GenOniLink("123", "456", "789", "000", 10, 11, []string{"tcp://127.0.0.1:10340", "tcp://127.0.0.2:10340"})
	fmt.Printf("oldLink %v\n", oldLink)
	link, err := DecodeLinkStr(oldLink)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("link: %s\n", link)
	link2, err := DecodeLinkStr(link.String())
	if err != nil {
		t.Fatal(err)
	}
	if link.String() != link2.String() {
		t.Fatal("link1 != link2")
	}
	fmt.Printf("link2: %s\n", link2)
}
