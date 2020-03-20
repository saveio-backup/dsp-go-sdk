package dsp

import (
	"time"

	"github.com/ontio/ontology-eventbus/actor"
	"github.com/saveio/dsp-go-sdk/actor/client"
	dspActorClient "github.com/saveio/dsp-go-sdk/actor/client"
	"github.com/saveio/dsp-go-sdk/common"
	"github.com/saveio/dsp-go-sdk/config"
	"github.com/saveio/dsp-go-sdk/core/chain"
	"github.com/saveio/dsp-go-sdk/core/channel"
	"github.com/saveio/dsp-go-sdk/core/dns"
	"github.com/saveio/dsp-go-sdk/core/fs"
	dspErr "github.com/saveio/dsp-go-sdk/error"
	"github.com/saveio/dsp-go-sdk/state"
	"github.com/saveio/dsp-go-sdk/store"
	"github.com/saveio/dsp-go-sdk/task"
	"github.com/saveio/dsp-go-sdk/utils/ticker"
	chActorClient "github.com/saveio/pylons/actor/client"
	"github.com/saveio/themis/account"
	"github.com/saveio/themis/common/log"
	"github.com/saveio/themis/http/base/sys"
)

var Version string

type Dsp struct {
	account           *account.Account         // Chain account of current login user
	config            *config.DspConfig        // Dsp global config
	chain             *chain.Chain             // Chain component
	fs                *fs.Fs                   // FS component
	channel           *channel.Channel         // Channel Component
	dns               *dns.DNS                 // DNS component
	taskMgr           *task.TaskMgr            // Task Mgr
	levelDBStore      *store.LevelDBStore      // Level DB
	shareRecordDB     *store.ShareRecordDB     // share_record db
	userspaceRecordDB *store.UserspaceRecordDB // user space db
	state             *state.SyncState         // dsp state
	closeCh           chan struct{}            // close signal
	retryTaskTicker   *ticker.Ticker           // retry task ticker
}

func NewDsp(c *config.DspConfig, acc *account.Account, p2pActor *actor.PID) *Dsp {
	d := &Dsp{
		dns: &dns.DNS{},
	}
	d.state = state.NewSyncState()
	if c == nil {
		return d
	}
	d.config = c
	if d.IsClient() {
		d.taskMgr = task.NewTaskMgr(ticker.NewTicker(time.Duration(common.TASK_PROGRESS_TICKER_DURATION)*time.Second, d.RunGetProgressTicker))
	} else {
		d.taskMgr = task.NewTaskMgr(nil)
	}
	d.retryTaskTicker = ticker.NewTicker(time.Second, d.retryTaskService)
	d.chain = chain.NewChain(acc, c.ChainRpcAddrs, chain.IsClient(d.IsClient()))
	d.account = acc
	if len(c.DBPath) > 0 {
		var err error
		d.levelDBStore, err = store.NewLevelDBStore(c.DBPath)
		if err != nil {
			log.Errorf("init db err %s", err)
			return nil
		}
		d.shareRecordDB = store.NewShareRecordDB(d.levelDBStore)
		d.userspaceRecordDB = store.NewUserspaceRecordDB(d.levelDBStore)
		d.taskMgr.SetFileDB(d.levelDBStore)
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
		if err := d.initChannelService(); err != nil {
			log.Errorf("init channel err %s", err)
			// return nil
		} else {
			chActorClient.SetP2pPid(p2pActor)
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
		this.state.Set(state.ModuleStateActive)
		return nil
	}
	this.closeCh = make(chan struct{})
	this.state.Set(state.ModuleStateStarting)
	if err := this.dns.SetupTrackers(); err != nil {
		this.state.Set(state.ModuleStateError)
		return err
	}

	if this.channel != nil {
		if err := this.StartChannelService(); err != nil {
			this.state.Set(state.ModuleStateError)
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
		go this.startDispatchFileService()
		go this.startDNSHealthCheckService()
	}
	if this.IsClient() {
		sys.MonitorEnable = false
		log.Debugf("disable monitor")
	}
	if this.IsClient() && this.config.HealthCheckDNS {
		log.Debugf("startDNSHealthCheckService")
		go this.startDNSHealthCheckService()
	}
	unSalve, _ := this.taskMgr.GetUnSlavedTasks()
	if len(unSalve) > 0 {
		this.taskMgr.RunGetProgress()
	}
	this.state.Set(state.ModuleStateStarted)
	this.state.Set(state.ModuleStateActive)
	log.Debugf("runing...")
	return nil
}

func (this *Dsp) StartChannelService() error {
	if this.channel == nil {
		return dspErr.New(dspErr.CHANNEL_START_SERVICE_ERROR, "channel is nil")
	}
	this.registerReceiveNotification()
	if err := this.channel.StartService(); err != nil {
		return err
	}
	time.Sleep(time.Second)
	return nil
}

func (this *Dsp) Stop() error {
	this.state.Set(state.ModuleStateStopping)
	close(this.closeCh)
	err := this.taskMgr.CloseDB()
	if err != nil {
		log.Errorf("close fileDB err %s", err)
		this.state.Set(state.ModuleStateError)
		return err
	}
	if this.channel != nil {
		this.channel.StopService()
		this.channel = nil
	}
	if this.fs != nil {
		err := this.fs.Close()
		if err != nil {
			log.Errorf("close fs err %s", err)
			this.state.Set(state.ModuleStateError)
			return err
		}
	}
	log.Debugf("stop dsp success")
	this.state.Set(state.ModuleStateStopped)
	return nil
}

func (this *Dsp) Running() bool {
	return this.state.Get() == state.ModuleStateActive
}

func (this *Dsp) State() state.ModuleState {
	return this.state.Get()
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
	log.Debugf("start seed service %ds", this.config.SeedInterval)
	tick := time.NewTicker(time.Duration(this.config.SeedInterval) * time.Second)
	defer tick.Stop()
	for {
		_, files, err := this.taskMgr.AllDownloadFiles()
		if err != nil {
			log.Errorf("push seed service err %v", err)
		} else {
			this.dns.PushFilesToTrackers(files)
		}
		select {
		case <-tick.C:
			if !this.Running() {
				log.Debugf("stop seed service")
				return
			}
		case <-this.closeCh:
			log.Debugf("stop seed service")
			return
		}
	}
}

func (this *Dsp) startDNSHealthCheckService() {
	for walletAddr, _ := range this.dns.OnlineDNS {
		client.P2pAppendAddrForHealthCheck(walletAddr, client.P2pNetTypeChannel)
	}
}

func (this *Dsp) initChannelService() error {
	ch, err := channel.NewChannelService(this.config, this.chain.Themis())
	if err != nil {
		log.Errorf("init channel err %s", err)
		return err
	}
	this.channel = ch
	channelDB := store.NewChannelDB(this.levelDBStore)
	this.channel.SetChannelDB(channelDB)

	return nil
}
