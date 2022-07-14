package dsp

import (
	"fmt"
	"github.com/saveio/dsp-go-sdk/core/chain"
	"time"

	"github.com/ontio/ontology-eventbus/actor"
	dspActorClient "github.com/saveio/dsp-go-sdk/actor/client"
	"github.com/saveio/dsp-go-sdk/config"
	"github.com/saveio/dsp-go-sdk/consts"
	"github.com/saveio/dsp-go-sdk/core/channel"
	"github.com/saveio/dsp-go-sdk/core/dns"
	"github.com/saveio/dsp-go-sdk/core/fs"
	dspErr "github.com/saveio/dsp-go-sdk/error"
	"github.com/saveio/dsp-go-sdk/store"
	"github.com/saveio/dsp-go-sdk/taskmgr"
	"github.com/saveio/dsp-go-sdk/types/state"
	chActorClient "github.com/saveio/pylons/actor/client"
	"github.com/saveio/themis/account"
	"github.com/saveio/themis/common/log"
	"github.com/saveio/themis/http/base/sys"
)

var Version string

type Dsp struct {
	Chain             *chain.Chain             // Chain component
	Fs                *fs.Fs                   // FS component
	DNS               *dns.DNS                 // DNS component
	Channel           *channel.Channel         // Channel Component
	TaskMgr           *taskmgr.TaskMgr         // task mgr
	config            *config.DspConfig        // Dsp global config
	levelDBStore      *store.LevelDBStore      // Level DB
	userspaceRecordDB *store.UserspaceRecordDB // user space db
	state             *state.SyncState         // dsp state
	closeCh           chan struct{}            // close signal
}

func NewDsp(c *config.DspConfig, acc *account.Account, p2pActor *actor.PID) *Dsp {
	d := &Dsp{}
	d.state = state.NewSyncState()
	if c == nil {
		return d
	}
	d.config = c

	d.Chain = chain.NewChain(acc, c.ChainRpcAddrs,
		chain.IsClient(d.IsClient()),
		chain.BlockConfirm(c.BlockConfirm),
	)
	// d.account = acc
	if len(c.DBPath) > 0 {
		var err error
		d.levelDBStore, err = store.NewLevelDBStore(c.DBPath)
		if err != nil {
			log.Errorf("init db err %s", err)
			return nil
		}
		d.userspaceRecordDB = store.NewUserspaceRecordDB(d.levelDBStore)

	}
	if len(c.FsRepoRoot) > 0 {
		var err error
		d.Fs, err = fs.NewFs(d.Chain.Themis(),
			fs.RepoRoot(d.config.FsRepoRoot),
			fs.FsType(d.config.FsType),
			fs.ChunkSize(consts.CHUNK_SIZE),
			fs.GcPeriod(d.config.FsGcPeriod),
			fs.MaxStorage(d.config.FsMaxStorage),
		)
		if err != nil {
			log.Errorf("init fs err %s", err)
			return nil
		}
	}
	dspActorClient.SetP2PPid(p2pActor)
	if len(c.ChannelListenAddr) > 0 && acc != nil {
		if err := d.initChannelService(); err != nil {
			log.Errorf("init channel err %s", err)
			// return nil
		} else {
			chActorClient.SetP2pPid(p2pActor)
		}
	}
	log.Infof("start init dns with chain %v, channel %v", d.Chain, d.Channel)
	d.DNS = dns.NewDNS(d.Chain, d.Channel,
		dns.MaxDNSNodeNum(d.config.DnsNodeMaxNum),
		dns.ReservedDNS(d.config.DNSWalletAddrs),
		dns.TrackerProtocol(d.config.TrackerProtocol),
		dns.ReservedTrackers(d.config.Trackers),
		dns.ChannelProtocol(d.config.ChannelProtocol),
		dns.AutoBootstrap(d.config.AutoSetupDNSEnable))

	d.TaskMgr = taskmgr.NewTaskMgr(d.Chain, d.Fs, d.DNS, d.Channel, d.config)
	d.TaskMgr.SetFileDB(d.levelDBStore)

	if err := d.TaskMgr.RecoverUndoneTask(); err != nil {
		log.Errorf("recover undone task err %s", err)
	}
	go d.RecoverDBLossTask()
	return d
}

func (this *Dsp) GetVersion() string {
	return consts.DSP_SDK_VERSION
}

func (this *Dsp) IsClient() bool {
	return this.config.FsType == consts.FS_FILESTORE
}

func (this *Dsp) IsFs() bool {
	return this.config.FsType == consts.FS_BLOCKSTORE
}

func (this *Dsp) Start() error {
	if this.config == nil {
		log.Debugf("dsp config is nil")
		this.state.Set(state.ModuleStateActive)
		return nil
	}
	this.closeCh = make(chan struct{})
	this.state.Set(state.ModuleStateStarting)
	if err := this.DNS.SetupTrackers(); err != nil {
		this.state.Set(state.ModuleStateError)
		log.Errorf("set up dns err %v", err)
	}

	if this.Channel != nil {
		if err := this.StartChannelService(); err != nil {
			this.state.Set(state.ModuleStateError)
			log.Errorf("start channel err %s", err)
		} else {

			this.DNS.Discovery()
		}
	}

	// start seed service
	if this.config.SeedInterval > 0 {
		go this.StartSeedService()
	}
	// start backup service
	if this.IsFs() && this.DNS != nil {
		go this.DNS.StartDNSHealthCheckService()
	}
	if this.IsClient() {
		sys.MonitorEnable = false
		log.Debugf("disable monitor")
	}
	if this.IsClient() && this.config.HealthCheckDNS && this.DNS != nil {
		log.Debugf("startDNSHealthCheckService")
		go this.DNS.StartDNSHealthCheckService()
	}

	this.state.Set(state.ModuleStateStarted)
	this.state.Set(state.ModuleStateActive)
	log.Debugf("dsp is runing...")
	go this.TaskMgr.StartService()
	return nil
}

func (this *Dsp) StartChannelService() error {
	if this.Channel == nil {
		return dspErr.New(dspErr.CHANNEL_START_SERVICE_ERROR, "channel is nil")
	}
	this.TaskMgr.ReceiveMediaTransferNotify()
	if err := this.Channel.StartService(); err != nil {
		return err
	}
	time.Sleep(time.Second)
	return nil
}

func (this *Dsp) Stop() error {
	this.state.Set(state.ModuleStateStopping)
	if this.TaskMgr.HasRunningTask() {
		this.state.Set(state.ModuleStateActive)
		log.Debugf("cant stop dsp module with running task")
		return fmt.Errorf("exist running task")
	}
	log.Debugf("closing dsp close channel")
	close(this.closeCh)
	if err := this.TaskMgr.Stop(); err != nil {
		log.Errorf("close fileDB err %s", err)
		this.state.Set(state.ModuleStateError)
		return err
	}
	log.Debugf("close dsp task mgr success")
	if this.Channel != nil {
		log.Debugf("stop channel service")
		this.Channel.StopService()
		log.Debugf("stop channel service success")
	}
	if this.Fs != nil {
		log.Debugf("closing dsp fs module")
		err := this.Fs.Close()
		if err != nil {
			log.Errorf("close fs err %s", err)
			this.state.Set(state.ModuleStateError)
			return err
		}
		log.Debugf("closing dsp fs module succes")
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
		_, files, err := this.TaskMgr.AllDownloadFiles()
		if err != nil {
			log.Errorf("push seed service err %v", err)
		} else {
			this.DNS.PushFilesToTrackers(files)
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

func (this *Dsp) initChannelService() error {
	ch, err := channel.NewChannelService(
		this.Chain.Themis(),
		channel.ClientType(this.config.ChannelClientType),
		channel.RevealTimeout(this.config.ChannelRevealTimeout),
		channel.DBPath(this.config.ChannelDBPath),
		channel.SettleTimeout(this.config.ChannelSettleTimeout),
		channel.BlockDelay(this.config.BlockDelay),
		channel.IsClient(this.IsClient()),
		channel.ChannelDB(store.NewChannelDB(this.levelDBStore)),
	)
	if err != nil {
		log.Errorf("init channel err %s", err)
		return err
	}
	this.Channel = ch
	log.Infof("init channel service success ch %v", this.Channel)

	return nil
}
