package dsp

import (
	"time"

	"github.com/ontio/ontology-eventbus/actor"
	"github.com/saveio/dsp-go-sdk/actor/client"
	dspErr "github.com/saveio/dsp-go-sdk/error"
	"github.com/saveio/dsp-go-sdk/store"
	ch_actor "github.com/saveio/pylons/actor/server"
	"github.com/saveio/pylons/common"
	chainCom "github.com/saveio/themis/common"
	"github.com/saveio/themis/common/log"
)

// SetUnitPriceForAllFile. set unit price for block sharing for all files
func (this *Dsp) SetUnitPriceForAllFile(asset int32, price uint64) {
	if this.channel == nil {
		return
	}
	this.channel.SetUnitPrices(asset, price)
}

// CleanUnitPriceForAllFile. clean unit price for block sharing for all files
func (this *Dsp) CleanUnitPriceForAllFile(asset int32) {
	if this.channel == nil {
		return
	}
	this.channel.CleanUnitPrices(asset)
}

func (this *Dsp) GetFileUnitPrice(asset int32) (uint64, error) {
	if this.channel == nil {
		return 0, nil
	}
	return this.channel.GetUnitPrices(asset)
}

func (this *Dsp) GetChannelPid() *actor.PID {
	if this.channel == nil {
		return nil
	}
	return this.channel.GetChannelPid()
}

func (this *Dsp) NetworkProtocol() string {
	if this.channel == nil {
		log.Warnf("channel is not instance for service NetworkProtocol")
		return ""
	}
	return this.channel.NetworkProtocol()
}

func (this *Dsp) StartService() error {
	if this.channel == nil {
		log.Warnf("channel is not instance for service StartService")
		return nil
	}
	return this.channel.StartService()
}

func (this *Dsp) HasChannelInstance() bool {
	return this.channel != nil
}

func (this *Dsp) ChannelRunning() bool {
	if this.channel == nil {
		log.Warnf("channel is not instance for service ChannelRunning")
		return false
	}
	return this.channel.Active()
}

func (this *Dsp) ChannelFirstSyncing() bool {
	if this.channel == nil {
		log.Warnf("channel is not instance for service ChannelFirstSyncing")
		return false
	}
	return this.channel.SyncingBlock()
}

func (this *Dsp) GetCurrentFilterBlockHeight() uint32 {
	if this.channel == nil {
		log.Warnf("channel is not instance for service GetCurrentFilterBlockHeight")
		return 0
	}
	return this.channel.GetCurrentFilterBlockHeight()
}
func (this *Dsp) StopService() {
	if this.channel == nil {
		log.Warnf("channel is not instance for service StopService")
		return
	}
	this.channel.StopService()
}
func (this *Dsp) GetCloseCh() chan struct{} {
	if this.channel == nil {
		log.Warnf("channel is not instance for service GetCloseCh")
		return nil
	}
	return this.channel.GetCloseCh()
}
func (this *Dsp) SetChannelDB(db *store.ChannelDB) {
	if this.channel == nil {
		log.Warnf("channel is not instance for service SetChannelDB")
		return
	}
	this.channel.SetChannelDB(db)
}
func (this *Dsp) GetAllPartners() []string {
	if this.channel == nil {
		log.Warnf("channel is not instance for service GetAllPartners")
		return nil
	}
	return this.channel.GetAllPartners()
}
func (this *Dsp) AddChannelInfo(id uint64, partnerAddr string) error {
	if this.channel == nil {
		log.Warnf("channel is not instance for service AddChannelInfo")
		return nil
	}
	return this.channel.AddChannelInfo(id, partnerAddr)
}
func (this *Dsp) GetChannelInfoFromDB(targetAddress string) (*store.ChannelInfo, error) {
	if this.channel == nil {
		return nil, dspErr.New(dspErr.CHANNEL_INTERNAL_ERROR, "no channel")
	}
	return this.channel.GetChannelInfoFromDB(targetAddress)
}
func (this *Dsp) OverridePartners() error {
	if this.channel == nil {
		log.Warnf("channel is not instance for service OverridePartners")
		return nil
	}
	return this.channel.OverridePartners()
}
func (this *Dsp) WaitForConnected(walletAddr string, timeout time.Duration) error {
	if this.channel == nil {
		log.Warnf("channel is not instance for service WaitForConnected")
		return nil
	}
	return this.channel.WaitForConnected(walletAddr, timeout)
}
func (this *Dsp) ChannelReachale(walletAddr string) bool {
	if this.channel == nil {
		log.Warnf("channel is not instance for service ChannelReachale")
		return false
	}
	return this.channel.ChannelReachale(walletAddr)
}
func (this *Dsp) HealthyCheckNodeState(walletAddr string) error {
	if this.channel == nil {
		log.Warnf("channel is not instance for service HealthyCheckNodeState")
		return nil
	}
	return this.channel.HealthyCheckNodeState(walletAddr)
}
func (this *Dsp) OpenChannel(targetAddress string, depositAmount uint64) (common.ChannelID, error) {
	if this.channel == nil {
		log.Warnf("channel is not instance for service OpenChannel")
		return 0, nil
	}
	// add dns to health check list
	if this.IsClient() {
		return this.channel.OpenChannel(targetAddress, depositAmount)
	}
	chId, err := this.channel.OpenChannel(targetAddress, depositAmount)
	if err != nil {
		return chId, err
	}
	addr, err := chainCom.AddressFromBase58(targetAddress)
	if err != nil {
		return chId, err
	}
	info, _ := this.GetDnsNodeByAddr(addr)
	if info == nil {
		return chId, nil
	}
	client.P2pAppendAddrForHealthCheck(targetAddress, client.P2pNetTypeChannel)
	return chId, nil
}

func (this *Dsp) SetChannelIsDNS(targetAddr string, isDNS bool) error {
	if this.channel == nil {
		log.Warnf("channel is not instance for service SetChannelIsDNS")
		return nil
	}
	return this.channel.SetChannelIsDNS(targetAddr, isDNS)
}

func (this *Dsp) ChannelClose(targetAddress string) error {
	if this.channel == nil {
		log.Warnf("channel is not instance for service ChannelClose")
		return nil
	}
	return this.channel.ChannelClose(targetAddress)
}
func (this *Dsp) SetDeposit(targetAddress string, amount uint64) error {
	if this.channel == nil {
		log.Warnf("channel is not instance for service SetDeposit")
		return nil
	}
	return this.channel.SetDeposit(targetAddress, amount)
}
func (this *Dsp) CanTransfer(to string, amount uint64) error {
	if this.channel == nil {
		log.Warnf("channel is not instance for service CanTransfer")
		return nil
	}
	return this.channel.CanTransfer(to, amount)
}
func (this *Dsp) NewPaymentId() int32 {
	if this.channel == nil {
		log.Warnf("channel is not instance for service NewPaymentId")
		return 0
	}
	return this.channel.NewPaymentId()
}
func (this *Dsp) DirectTransfer(paymentId int32, amount uint64, to string) error {
	if this.channel == nil {
		log.Warnf("channel is not instance for service DirectTransfer")
		return nil
	}
	return this.channel.DirectTransfer(paymentId, amount, to)
}
func (this *Dsp) MediaTransfer(paymentId int32, amount uint64, media, to string) error {
	if this.channel == nil {
		log.Warnf("channel is not instance for service MediaTransfer")
		return nil
	}
	return this.channel.MediaTransfer(paymentId, amount, media, to)
}
func (this *Dsp) GetTotalDepositBalance(targetAddress string) (uint64, error) {
	if this.channel == nil {
		log.Warnf("channel is not instance for service GetTotalDepositBalance")
		return 0, nil
	}
	return this.channel.GetTotalDepositBalance(targetAddress)
}
func (this *Dsp) GetAvailableBalance(partnerAddress string) (uint64, error) {
	if this.channel == nil {
		log.Warnf("channel is not instance for service GetAvailableBalance")
		return 0, nil
	}
	return this.channel.GetAvailableBalance(partnerAddress)
}
func (this *Dsp) GetTotalWithdraw(partnerAddress string) (uint64, error) {
	if this.channel == nil {
		log.Warnf("channel is not instance for service GetTotalWithdraw")
		return 0, nil
	}
	return this.channel.GetTotalWithdraw(partnerAddress)
}
func (this *Dsp) GetCurrentBalance(partnerAddress string) (uint64, error) {
	if this.channel == nil {
		log.Warnf("channel is not instance for service GetCurrentBalance")
		return 0, nil
	}
	return this.channel.GetCurrentBalance(partnerAddress)
}
func (this *Dsp) Withdraw(targetAddress string, amount uint64) (bool, error) {
	if this.channel == nil {
		log.Warnf("channel is not instance for service Withdraw")
		return false, nil
	}
	return this.channel.Withdraw(targetAddress, amount)
}
func (this *Dsp) CooperativeSettle(targetAddress string) error {
	if this.channel == nil {
		log.Warnf("channel is not instance for service CooperativeSettle")
		return nil
	}
	return this.channel.CooperativeSettle(targetAddress)
}
func (this *Dsp) SetUnitPrices(asset int32, price uint64) {
	if this.channel == nil {
		log.Warnf("channel is not instance for service SetUnitPrices")
		return
	}
	this.channel.SetUnitPrices(asset, price)
}
func (this *Dsp) GetUnitPrices(asset int32) (uint64, error) {
	if this.channel == nil {
		log.Warnf("channel is not instance for service GetUnitPrices")
		return 0, nil
	}
	return this.channel.GetUnitPrices(asset)
}
func (this *Dsp) CleanUnitPrices(asset int32) {
	if this.channel == nil {
		log.Warnf("channel is not instance for service CleanUnitPrices")
		return
	}
	this.channel.CleanUnitPrices(asset)
}
func (this *Dsp) ChannelExist(walletAddr string) bool {
	if this.channel == nil {
		log.Warnf("channel is not instance for service ChannelExist")
		return false
	}
	return this.channel.ChannelExist(walletAddr)
}
func (this *Dsp) GetChannelInfo(walletAddr string) (*ch_actor.ChannelInfo, error) {
	if this.channel == nil {
		log.Warnf("channel is not instance for service GetChannelInfo")
		return nil, nil
	}
	return this.channel.GetChannelInfo(walletAddr)
}
func (this *Dsp) AllChannels() (*ch_actor.ChannelsInfoResp, error) {
	if this.channel == nil {
		return nil, nil
	}
	return this.channel.AllChannels()
}
