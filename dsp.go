package dsp

import (
	"fmt"

	"github.com/oniio/dsp-go-sdk/common"
	"github.com/oniio/dsp-go-sdk/network"
	"github.com/oniio/dsp-go-sdk/network/message"
	"github.com/oniio/oniChain-go-sdk"
)

type Dsp struct {
	Chain   *chain.Chain
	Network *network.Network
}

func NewDsp() *Dsp {
	return &Dsp{}
}

func (this *Dsp) GetVersion() string {
	return common.DSP_SDK_VERSION
}

func (this *Dsp) Start(addr string) {
	this.Network = network.NewNetwork(addr, this.Receive)
	this.Network.Start()
}

func (this *Dsp) Receive(msg *message.Message) {
	fmt.Printf("dsp receive msg:%v\n", msg)
}
