package chain

import (
	"math/rand"
	"time"

	"github.com/oniio/dsp-go-sdk/chain/client"
	sdkcom "github.com/oniio/dsp-go-sdk/chain/common"
	"github.com/oniio/dsp-go-sdk/chain/utils"
	"github.com/oniio/oniChain/account"
	"github.com/oniio/oniChain/common"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

type Chain struct {
	client.ClientMgr
	Native *NativeContract
	NeoVM  *NeoVMContract
}

//NewChain return Chain.
func NewChain() *Chain {
	chain := &Chain{}
	native := newNativeContract(chain.GetClientMgr())
	chain.Native = native
	neoVM := newNeoVMContract(chain)
	chain.NeoVM = neoVM
	return chain
}

func (this *Chain) InvokeNativeContract(
	gasPrice,
	gasLimit uint64,
	signer *account.Account,
	version byte,
	contractAddress common.Address,
	method string,
	params []interface{},
) (common.Uint256, error) {
	tx, err := utils.NewNativeInvokeTransaction(gasPrice, gasLimit, version, contractAddress, method, params)
	if err != nil {
		return common.UINT256_EMPTY, err
	}
	err = utils.SignToTransaction(tx, signer)
	if err != nil {
		return common.UINT256_EMPTY, err
	}
	return this.SendTransaction(tx)
}

func (this *Chain) PreExecInvokeNativeContract(
	contractAddress common.Address,
	version byte,
	method string,
	params []interface{},
) (*sdkcom.PreExecResult, error) {
	tx, err := utils.NewNativeInvokeTransaction(0, 0, version, contractAddress, method, params)
	if err != nil {
		return nil, err
	}
	return this.PreExecTransaction(tx)
}
