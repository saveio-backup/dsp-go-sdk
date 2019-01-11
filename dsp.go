package dsp

import (
	"github.com/oniio/dsp-go-sdk/common"
	"github.com/oniio/oniChain-go-sdk"
)

type Dsp struct {
	Chain *chain.Chain
}

func NewDsp() *Dsp {
	return &Dsp{}
}

func (d *Dsp) GetVersion() string {
	return common.DSP_SDK_VERSION
}
