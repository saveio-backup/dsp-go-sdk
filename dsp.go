package dsp

import (
	"github.com/oniio/dsp-go-sdk/common"
	"github.com/oniio/dsp-go-sdk/fs"
	"github.com/oniio/dsp-go-sdk/network"
	"github.com/oniio/dsp-go-sdk/store"
	"github.com/oniio/oniChain-go-sdk"
)

type Dsp struct {
	Chain       *chain.Chain
	Network     *network.Network
	Fs          *fs.Fs
	taskMgr     *TaskMgr
	UserFileMgr *store.UserFileMgr
	NodeFilemgr *store.NodeFileMgr
}

func NewDsp() *Dsp {
	return &Dsp{
		Fs:          &fs.Fs{},
		taskMgr:     NewTaskMgr(),
		UserFileMgr: store.NewUserFileMgr(),
		NodeFilemgr: store.NewNodeFileMgr(),
	}
}

func (this *Dsp) GetVersion() string {
	return common.DSP_SDK_VERSION
}

func (this *Dsp) Start(addr string) {
	this.Network = network.NewNetwork(addr, this.Receive)
	this.Network.Start()
}
