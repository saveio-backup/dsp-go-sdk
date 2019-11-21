package dsp

import (
	"time"

	"github.com/ontio/ontology-eventbus/actor"
	dspActorClient "github.com/saveio/dsp-go-sdk/actor/client"
	"github.com/saveio/dsp-go-sdk/common"
	"github.com/saveio/dsp-go-sdk/config"
	"github.com/saveio/dsp-go-sdk/core/chain"
	"github.com/saveio/dsp-go-sdk/core/channel"
	"github.com/saveio/dsp-go-sdk/core/dns"
	"github.com/saveio/dsp-go-sdk/core/fs"
	dspErr "github.com/saveio/dsp-go-sdk/error"
	"github.com/saveio/dsp-go-sdk/store"
	"github.com/saveio/dsp-go-sdk/task"
	chActorClient "github.com/saveio/pylons/actor/client"

	"github.com/saveio/themis/account"
	chainCom "github.com/saveio/themis/common"
	"github.com/saveio/themis/common/log"
)

var Version string

type Dsp struct {
	account *account.Account  // Chain account of current login user
	config  *config.DspConfig // Dsp global config
	chain   *chain.Chain      // Chain component
	fs      *fs.Fs            // FS component
	channel *channel.Channel  // Channel Component
	dns     *dns.DNS          // DNS component
	taskMgr *task.TaskMgr     // Task Mgr
	running bool              // flag of service status
}

func NewDsp(c *config.DspConfig, acc *account.Account, p2pActor *actor.PID) *Dsp {
	d := &Dsp{
		taskMgr: task.NewTaskMgr(),
		dns:     &dns.DNS{},
	}
	if c == nil {
		return d
	}
	d.config = c
	d.chain = chain.NewChain(acc, c.ChainRpcAddrs, chain.IsClient(d.IsClient()))
	d.account = acc
	var dbstore *store.LevelDBStore
	if len(c.DBPath) > 0 {
		var err error
		dbstore, err = store.NewLevelDBStore(c.DBPath)
		if err != nil {
			log.Errorf("init db err %s", err)
			return nil
		}
		d.taskMgr.SetFileDB(dbstore)
		err = d.taskMgr.RecoverUndoneTask()
		if err != nil {
			log.Errorf("recover undone task err %s", err)
		}
		go d.RecoverDBLossTask()
	}
	if len(c.FsRepoRoot) > 0 {
		var err error
		d.fs, err = fs.NewFs(c, d.chain.Themis())
		if err != nil {
			log.Errorf("init fs err %s", err)
			return nil
		}
	}
	dspActorClient.SetP2pPid(p2pActor)
	if len(c.ChannelListenAddr) > 0 && acc != nil {
		var err error
		getHostCallBack := func(addr chainCom.Address) (string, error) {
			return d.dns.GetExternalIP(addr.ToBase58())
		}
		d.channel, err = channel.NewChannelService(c, d.chain.Themis(), getHostCallBack)
		if err != nil {
			log.Errorf("init channel err %s", err)
			return nil
		}
		chActorClient.SetP2pPid(p2pActor)
		if dbstore != nil {
			channelDB := store.NewChannelDB(dbstore)
			d.channel.SetChannelDB(channelDB)
		}
	}
	d.dns = dns.NewDNS(d.chain, d.channel,
		dns.MaxDNSNodeNum(d.config.DnsNodeMaxNum),
		dns.DNSWalletAddrsFromCfg(d.config.DNSWalletAddrs),
		dns.TrackerProtocol(d.config.TrackerProtocol),
		dns.TrackersFromCfg(d.config.Trackers),
		dns.ChannelProtocol(d.config.ChannelProtocol),
		dns.AutoBootstrap(d.config.AutoSetupDNSEnable))
	return d
}

func (this *Dsp) GetVersion() string {
	return common.DSP_SDK_VERSION
}

func (this *Dsp) IsClient() bool {
	return this.config.FsType == config.FS_FILESTORE
}

func (this *Dsp) IsFs() bool {
	return this.config.FsType == config.FS_BLOCKSTORE
}

func (this *Dsp) Start() error {
	if this.config == nil {
		return nil
	}
	// start dns service
	if this.channel != nil {
		err := this.StartChannelService()
		if err != nil {
			return err
		}
		this.dns.Channel = this.channel
		this.dns.BootstrapDNS()
	}
	// start seed service
	if this.config.SeedInterval > 0 {
		go this.StartSeedService()
	}

	// start backup service
	if this.IsFs() {
		if this.config.EnableBackup {
			log.Debugf("start backup file service ")
			go this.StartBackupFileService()
		}
		go this.StartCheckRemoveFiles()
		go this.StartFetchFileService()
	}
	this.running = true
	return nil
}

func (this *Dsp) StartChannelService() error {
	if this.channel == nil {
		return dspErr.New(dspErr.CHANNEL_START_SERVICE_ERROR, "channel is nil")
	}
	this.registerReceiveNotification()
	err := this.channel.StartService()
	if err != nil {
		return err
	}
	time.Sleep(time.Second)
	return nil
}

func (this *Dsp) Stop() error {
	err := this.taskMgr.CloseDB()
	if err != nil {
		log.Errorf("close fileDB err %s", err)
		return err
	}
	if this.channel != nil {
		this.channel.StopService()
	}
	if this.fs != nil {
		err := this.fs.Close()
		if err != nil {
			log.Errorf("close fs err %s", err)
			return err
		}
	}
	log.Debugf("stop dsp success")
	this.running = false
	return nil
}

func (this *Dsp) Running() bool {
	return this.running
}

func (this *Dsp) UpdateConfig(field string, value interface{}) error {
	switch field {
	case "FsFileRoot":
		str, ok := value.(string)
		if !ok {
			return dspErr.New(dspErr.INTERNAL_ERROR, "invalid value type")
		}
		this.config.FsFileRoot = str
	}
	log.Debugf("update config %s", this.config.FsFileRoot)
	return nil
}

func (this *Dsp) StartSeedService() {
	log.Debugf("start seed service")
	tick := time.NewTicker(time.Duration(this.config.SeedInterval) * time.Second)
	defer tick.Stop()
	for {
		select {
		case <-tick.C:
			if !this.Running() {
				log.Debugf("stop seed service")
				return
			}
			_, files, err := this.taskMgr.AllDownloadFiles()
			if err != nil {
				continue
			}
			this.dns.PushFilesToTrackers(files)
		}
	}
}
