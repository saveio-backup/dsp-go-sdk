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
	return this.channel.NetworkProtocol()
}

func (this *Dsp) GetHostAddr(walletAddr string) (string, error) {
	return this.channel.GetHostAddr(walletAddr)
}

func (this *Dsp) StartService() error {
	return this.channel.StartService()
}

func (this *Dsp) HasChannelInstance() bool {
	return this.channel != nil
}

func (this *Dsp) ChannelRunning() bool {
	return this.channel.Running()
}

func (this *Dsp) ChannelFirstSyncing() bool {
	return this.channel.FirstSyncing()
}

func (this *Dsp) GetCurrentFilterBlockHeight() uint32 {
	return this.channel.GetCurrentFilterBlockHeight()
}
func (this *Dsp) StopService() {
	this.channel.StopService()
}
func (this *Dsp) GetCloseCh() chan struct{} {
	return this.channel.GetCloseCh()
}
func (this *Dsp) SetChannelDB(db *store.ChannelDB) {
	this.channel.SetChannelDB(db)
}
func (this *Dsp) GetAllPartners() []string {
	return this.channel.GetAllPartners()
}
func (this *Dsp) AddChannelInfo(id uint64, partnerAddr string) error {
	return this.channel.AddChannelInfo(id, partnerAddr)
}
func (this *Dsp) GetChannelInfoFromDB(targetAddress string) (*store.ChannelInfo, error) {
	if this.channel == nil {
		return nil, dspErr.New(dspErr.CHANNEL_INTERNAL_ERROR, "no channel")
	}
	return this.channel.GetChannelInfoFromDB(targetAddress)
}
func (this *Dsp) OverridePartners() error {
	return this.channel.OverridePartners()
}
func (this *Dsp) WaitForConnected(walletAddr string, timeout time.Duration) error {
	return this.channel.WaitForConnected(walletAddr, timeout)
}
func (this *Dsp) ChannelReachale(walletAddr string) bool {
	return this.channel.ChannelReachale(walletAddr)
}
func (this *Dsp) HealthyCheckNodeState(walletAddr string) error {
	return this.channel.HealthyCheckNodeState(walletAddr)
}
func (this *Dsp) OpenChannel(targetAddress string, depositAmount uint64) (common.ChannelID, error) {
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
	hostAddr, _ := this.GetExternalIP(targetAddress)
	if len(hostAddr) > 0 {
		client.P2pAppendAddrForHealthCheck(hostAddr, client.P2pNetTypeChannel)
	}
	return chId, nil
}

func (this *Dsp) SetChannelIsDNS(targetAddr string, isDNS bool) error {
	return this.channel.SetChannelIsDNS(targetAddr, isDNS)
}

func (this *Dsp) ChannelClose(targetAddress string) error {
	return this.channel.ChannelClose(targetAddress)
}
func (this *Dsp) SetDeposit(targetAddress string, amount uint64) error {
	return this.channel.SetDeposit(targetAddress, amount)
}
func (this *Dsp) CanTransfer(to string, amount uint64) error {
	return this.channel.CanTransfer(to, amount)
}
func (this *Dsp) NewPaymentId() int32 {
	return this.channel.NewPaymentId()
}
func (this *Dsp) DirectTransfer(paymentId int32, amount uint64, to string) error {
	return this.channel.DirectTransfer(paymentId, amount, to)
}
func (this *Dsp) MediaTransfer(paymentId int32, amount uint64, media, to string) error {
	return this.channel.MediaTransfer(paymentId, amount, media, to)
}
func (this *Dsp) GetTotalDepositBalance(targetAddress string) (uint64, error) {
	return this.channel.GetTotalDepositBalance(targetAddress)
}
func (this *Dsp) GetAvailableBalance(partnerAddress string) (uint64, error) {
	return this.channel.GetAvailableBalance(partnerAddress)
}
func (this *Dsp) GetTotalWithdraw(partnerAddress string) (uint64, error) {
	return this.channel.GetTotalWithdraw(partnerAddress)
}
func (this *Dsp) GetCurrentBalance(partnerAddress string) (uint64, error) {
	return this.channel.GetCurrentBalance(partnerAddress)
}
func (this *Dsp) Withdraw(targetAddress string, amount uint64) (bool, error) {
	return this.channel.Withdraw(targetAddress, amount)
}
func (this *Dsp) CooperativeSettle(targetAddress string) error {
	return this.channel.CooperativeSettle(targetAddress)
}
func (this *Dsp) SetUnitPrices(asset int32, price uint64) {
	this.channel.SetUnitPrices(asset, price)
}
func (this *Dsp) GetUnitPrices(asset int32) (uint64, error) {
	return this.channel.GetUnitPrices(asset)
}
func (this *Dsp) CleanUnitPrices(asset int32) {
	this.channel.CleanUnitPrices(asset)
}
func (this *Dsp) ChannelExist(walletAddr string) bool {
	return this.channel.ChannelExist(walletAddr)
}
func (this *Dsp) GetChannelInfo(walletAddr string) (*ch_actor.ChannelInfo, error) {
	return this.channel.GetChannelInfo(walletAddr)
}
func (this *Dsp) AllChannels() (*ch_actor.ChannelsInfoResp, error) {
	if this.channel == nil {
		return nil, nil
	}
	return this.channel.AllChannels()
}
