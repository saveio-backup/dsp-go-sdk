package dsp

import (
	"github.com/oniio/dsp-go-sdk/channel"
	"github.com/oniio/dsp-go-sdk/common"
	"github.com/oniio/dsp-go-sdk/config"
	"github.com/oniio/dsp-go-sdk/fs"
	"github.com/oniio/dsp-go-sdk/network"
	"github.com/oniio/dsp-go-sdk/store"
	"github.com/oniio/dsp-go-sdk/task"
	"github.com/oniio/oniChain-go-sdk"
	"github.com/oniio/oniChain/account"
)

type Dsp struct {
	Config  *config.DspConfig
	Chain   *chain.Chain
	Network *network.Network
	Fs      *fs.Fs
	Channel *channel.Channel
	taskMgr *task.TaskMgr
}

func NewDsp(c *config.DspConfig, acc *account.Account) *Dsp {
	d := &Dsp{
		taskMgr: task.NewTaskMgr(),
	}
	if c == nil {
		return d
	}
	d.Config = c
	d.Chain = chain.NewChain()
	d.Chain.NewRpcClient().SetAddress(c.ChainRpcAddr)
	if acc != nil {
		d.Chain.SetDefaultAccount(acc)
	}
	var dbstore *store.LevelDBStore
	if len(c.DBPath) > 0 {
		dbstore, err := store.NewLevelDBStore(c.DBPath)
		if err != nil || dbstore == nil {
			return nil
		}
		d.taskMgr.FileDB = store.NewFileDB(dbstore)
	}
	if len(c.FsRepoRoot) > 0 {
		d.Fs = fs.NewFs(c, d.Chain)
	}
	if len(c.ChannelListenAddr) > 0 {
		d.Channel = channel.NewChannelService(c, d.Chain)
		if dbstore != nil {
			paymentDB := store.NewPaymentDB(dbstore)
			d.Channel.SetPaymentDB(paymentDB)
		}
	}
	return d
}

func (this *Dsp) GetVersion() string {
	return common.DSP_SDK_VERSION
}

func (this *Dsp) Start(addr string) {
	this.Network = network.NewNetwork(addr, this.Receive)
	this.Network.Start()
	if this.Channel != nil {
		this.Channel.StartService()
	}
}

func (this *Dsp) Stop() {
	if this.Channel != nil {
		this.Channel.StopService()
	}
}
