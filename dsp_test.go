package dsp

import (
	"fmt"
	"testing"

	"github.com/oniio/oniChain-go-sdk"
)

var rpcAddr = "http://127.0.0.1:20336"

func TestChainGetBlockHeight(t *testing.T) {
	d := NewDsp()
	d.Chain = chain.NewChain()
	d.Chain.NewRpcClient().SetAddress(rpcAddr)
	height, err := d.Chain.GetCurrentBlockHeight()
	if err != nil {
		fmt.Printf("get block height err: %s", err)
		return
	}
	fmt.Printf("current block height: %d\n", height)
}
func TestGetVersion(t *testing.T) {
	d := NewDsp()
	version := d.GetVersion()
	fmt.Printf("version: %s\n", version)
}
