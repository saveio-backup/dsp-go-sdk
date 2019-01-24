package dsp

import (
	"github.com/oniio/dsp-go-sdk/common"
	"github.com/oniio/dsp-go-sdk/fs"
	"github.com/oniio/dsp-go-sdk/network"
	"github.com/oniio/oniChain-go-sdk"
)

type Dsp struct {
	Chain   *chain.Chain
	Network *network.Network
	Fs      *fs.Fs
	taskMgr *TaskMgr
}

func NewDsp() *Dsp {
	return &Dsp{
		Fs:      &fs.Fs{},
		taskMgr: NewTaskMgr(),
	}
}

func (this *Dsp) GetVersion() string {
	return common.DSP_SDK_VERSION
}

func (this *Dsp) Start(addr string) {
	this.Network = network.NewNetwork(addr, this.Receive)
	this.Network.Start()
}
