package dsp

import (
	"errors"
	"time"

	"github.com/ontio/ontology-eventbus/actor"
	dspActorClient "github.com/saveio/dsp-go-sdk/actor/client"
	"github.com/saveio/dsp-go-sdk/channel"
	"github.com/saveio/dsp-go-sdk/common"
	"github.com/saveio/dsp-go-sdk/config"
	"github.com/saveio/dsp-go-sdk/fs"
	"github.com/saveio/dsp-go-sdk/store"
	"github.com/saveio/dsp-go-sdk/task"
	chActorClient "github.com/saveio/pylons/actor/client"
	chain "github.com/saveio/themis-go-sdk"
	"github.com/saveio/themis/account"
	chainCom "github.com/saveio/themis/common"
	"github.com/saveio/themis/common/log"
)

type Dsp struct {
	Account *account.Account
	Config  *config.DspConfig
	Chain   *chain.Chain
	Fs      *fs.Fs
	Channel *channel.Channel
	DNS     *DNS
	taskMgr *task.TaskMgr
}

func NewDsp(c *config.DspConfig, acc *account.Account, p2pActor *actor.PID) *Dsp {
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
		d.Account = acc
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
	dspActorClient.SetP2pPid(p2pActor)
	if len(c.ChannelListenAddr) > 0 && acc != nil {
		var err error
		getHostCallBack := func(addr chainCom.Address) (string, error) {
			return d.GetExternalIP(addr.ToBase58())
		}
		d.Channel, err = channel.NewChannelService(c, d.Chain, getHostCallBack)
		if err != nil {
			log.Errorf("init channel err %s", err)
			return nil
		}
		chActorClient.SetP2pPid(p2pActor)
		if dbstore != nil {
			channelDB := store.NewChannelDB(dbstore)
			d.Channel.SetChannelDB(channelDB)
		}
	}
	d.DNS = NewDNS()
	return d
}

func (this *Dsp) GetVersion() string {
	return common.DSP_SDK_VERSION
}

func (this *Dsp) Start() error {
	if this.Config == nil {
		return nil
	}
	// start dns service
	if this.Channel != nil {
		err := this.StartChannelService()
		if err != nil {
			return err
		}
		if this.Config.AutoSetupDNSEnable {
			err = this.SetupDNSChannels()
			if err != nil {
				return err
			}
		}
	}

	// start seed service
	if this.Config.SeedInterval > 0 {
		go this.StartSeedService()
	}

	// start backup service
	if this.Config.FsType == config.FS_BLOCKSTORE {
		go this.StartBackupFileService()
	}
	return nil
}

func (this *Dsp) StartChannelService() error {
	if this.Channel == nil {
		return errors.New("channel is nil")
	}
	err := this.SetupDNSTrackers()
	if err != nil {
		return err
	}
	this.SetupPartnerHost(this.Channel.GetAllPartners())
	err = this.Channel.StartService()
	if err != nil {
		return err
	}
	time.Sleep(time.Second)
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
	return nil
}

func (this *Dsp) UpdateConfig(field string, value interface{}) error {
	switch field {
	case "FsFileRoot":
		str, ok := value.(string)
		if !ok {
			return errors.New("invalid value type")
		}
		this.Config.FsFileRoot = str
	}
	log.Debugf("update config %s", this.Config.FsFileRoot)
	return nil
}
