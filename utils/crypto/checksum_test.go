package crypto

import (
	"fmt"
	"testing"
)

func TestMD5All(t *testing.T) {
	path := "/Users/smallyu/work/gogs/edge-deploy/cnode1"
	all, err := MD5All(path)
	if err != nil {
		t.Error(err)
	}
	for k, v := range all {
		fmt.Printf("%s: %x\n", k, v)
	}
}

func TestGetSimpleChecksumOfDir(t *testing.T) {
	path := "/Users/smallyu/work/gogs/edge-deploy/cnode1/testaaa34"
	all, err := GetSimpleChecksumOfDir(path)
	if err != nil {
		t.Error(err)
	}
	fmt.Println(all)
}

func TestGetSimpleChecksumOfFile(t *testing.T) {
	path := "/Users/smallyu/work/gogs/edge-deploy/cnode1/debug.sh"
	all, err := GetSimpleChecksumOfFile(path)
	if err != nil {
		t.Error(err)
	}
	fmt.Println(all)
}
