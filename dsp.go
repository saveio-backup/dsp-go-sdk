package dsp

import (
	"errors"
	"time"

	"github.com/oniio/dsp-go-sdk/channel"
	"github.com/oniio/dsp-go-sdk/common"
	"github.com/oniio/dsp-go-sdk/config"
	"github.com/oniio/dsp-go-sdk/fs"
	"github.com/oniio/dsp-go-sdk/network"
	"github.com/oniio/dsp-go-sdk/store"
	"github.com/oniio/dsp-go-sdk/task"
	"github.com/oniio/oniChain-go-sdk"
	"github.com/oniio/oniChain/account"
	"github.com/oniio/oniChain/common/log"
)

type Dsp struct {
	Config      *config.DspConfig
	Chain       *chain.Chain
	Network     *network.Network
	Fs          *fs.Fs
	Channel     *channel.Channel
	TrackerUrls []string
	DNSNode     *DNSNodeInfo
	taskMgr     *task.TaskMgr
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
		var err error
		dbstore, err = store.NewLevelDBStore(c.DBPath)
		if err != nil {
			log.Errorf("init db err %s", err)
			return nil
		}
		d.taskMgr.FileDB = store.NewFileDB(dbstore)
	}
	if len(c.FsRepoRoot) > 0 {
		var err error
		d.Fs, err = fs.NewFs(c, d.Chain)
		if err != nil {
			log.Errorf("init fs err %s", err)
			return nil
		}
	}
	if len(c.ChannelListenAddr) > 0 && acc != nil {
		var err error
		d.Channel, err = channel.NewChannelService(c, d.Chain)
		if err != nil {
			log.Errorf("init channel err %s", err)
			return nil
		}
		if dbstore != nil {
			channelDB := store.NewChannelDB(dbstore)
			d.Channel.SetChannelDB(channelDB)
		}
	}
	return d
}

func (this *Dsp) GetVersion() string {
	return common.DSP_SDK_VERSION
}

func (this *Dsp) Start(addr string) error {
	this.Network = network.NewNetwork(addr, this.Receive)
	err := this.Network.Start()
	if err != nil {
		return err
	}
	if this.Config == nil {
		return nil
	}
	if this.Channel != nil {
		err := this.StartChannelService()
		if err != nil {
			return err
		}
		err = this.SetupDNSNode()
		if err != nil {
			return err
		}
	}
	if this.Config.SeedInterval > 0 {
		go this.StartSeedService()
	}
	if this.Config.FsType == config.FS_BLOCKSTORE {
		go this.StartBackupFileService()
	}

	// if addr == "tcp://127.0.0.1:14003" {
	// 	log.Debugf("Test MT")
	// 	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	// 	paymentId := r.Int31()
	// 	target := "AMkN2sRQyT3qHZQqwEycHCX2ezdZNpXNdJ"
	// 	this.Channel.SetHostAddr(target, this.GetExternalIP(target))
	// 	err := this.Channel.MediaTransfer(paymentId, 1, target)
	// 	log.Errorf("Media Transfer Error %s", err)
	// }
	return nil
}

func (this *Dsp) StartChannelService() error {
	if this.Channel == nil {
		return errors.New("channel is nil")
	}
	this.SetupPartnerHost(this.Channel.GetAllPartners())
	err := this.Channel.StartService()
	if err != nil {
		return err
	}
	time.Sleep(time.Second)
	this.Channel.OverridePartners()
	return nil
}

func (this *Dsp) Stop() error {
	if this.taskMgr.FileDB != nil {
		err := this.taskMgr.FileDB.Close()
		if err != nil {
			log.Errorf("close fileDB err %s", err)
			return err
		}
	}
	if this.Channel != nil {
		this.Channel.StopService()
	}
	if this.Fs != nil {
		err := this.Fs.Close()
		if err != nil {
			log.Errorf("close fs err %s", err)
			return err
		}
	}
	if this.Network != nil {
		err := this.Network.Halt()
		if err != nil {
			log.Errorf("close dsp p2p err %s", err)
			return err
		}
	}

	return nil
}
