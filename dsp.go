package dsp

import (
	"github.com/oniio/dsp-go-sdk/common"
	"github.com/oniio/dsp-go-sdk/config"
	"github.com/oniio/dsp-go-sdk/fs"
	"github.com/oniio/dsp-go-sdk/network"
	"github.com/oniio/dsp-go-sdk/store"
	"github.com/oniio/dsp-go-sdk/task"
	"github.com/oniio/oniChain-go-sdk"
)

type Dsp struct {
	Config  *config.DspConfig
	Chain   *chain.Chain
	Network *network.Network
	Fs      *fs.Fs
	taskMgr *task.TaskMgr
}

func NewDsp(c *config.DspConfig) *Dsp {
	d := &Dsp{
		taskMgr: task.NewTaskMgr(),
	}
	if c == nil {
		return d
	}
	d.Chain = chain.NewChain()
	d.Chain.NewRpcClient().SetAddress(c.ChainRpcAddr)
	if len(c.DBPath) > 0 {
		d.taskMgr.FileDB = store.NewFileDB(c.DBPath)
	}
	if len(c.FsRepoRoot) > 0 {
		d.Fs = fs.NewFs(&fs.FsConfig{
			RepoRoot:  c.FsRepoRoot,
			FsRoot:    c.FsFileRoot,
			FsType:    fs.FSType(c.FsType),
			ChunkSize: common.CHUNK_SIZE,
			Chain:     d.Chain,
		})
	}
	return d
}

func (this *Dsp) GetVersion() string {
	return common.DSP_SDK_VERSION
}

func (this *Dsp) Start(addr string) {
	this.Network = network.NewNetwork(addr, this.Receive)
	this.Network.Start()
}
