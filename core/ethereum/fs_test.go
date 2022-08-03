package ethereum

import (
	ethCom "github.com/ethereum/go-ethereum/common"
	chainCom "github.com/saveio/themis/common"
	"testing"
)

func TestEthereum_QueryNode(t *testing.T) {
	s := "0x5406d22c2add39fa7be8bc0f18a03d3cefbbc0e8"
	address := ethCom.HexToAddress(s)
	t.Log(address)
	bytes, err := chainCom.AddressParseFromBytes(address[:])
	if err != nil {
		t.Error(err)
	}
	t.Log(bytes)
}
