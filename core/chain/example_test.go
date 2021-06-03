package chain

import (
	"fmt"

	"github.com/saveio/themis/account"
)

func ExampleNewChain() {
	var acc *account.Account
	chain := NewChain(acc, []string{"http://127.0.0.1:20336"})
	currentHeight, err := chain.GetCurrentBlockHeight()
	if err != nil {
		fmt.Printf("get current block height err %s\n", err)
		return
	}
	fmt.Printf("current height %d\n", currentHeight)
	// Output:
	// current height 1
}
