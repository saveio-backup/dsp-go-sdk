package dsp

import (
	"time"

	"github.com/ontio/ontology-eventbus/actor"
	ch_actor "github.com/saveio/pylons/actor/server"
	"github.com/saveio/pylons/common"
	"github.com/saveio/themis/common/log"
)

// // SetUnitPriceForAllFile. set unit price for block sharing for all files
// func (this *Dsp) SetUnitPriceForAllFile(asset int32, price uint64) {
// 	if this.Channel == nil {
// 		return
// 	}
// 	this.Channel.SetUnitPrices(asset, price)
// }

// // CleanUnitPriceForAllFile. clean unit price for block sharing for all files
// func (this *Dsp) CleanUnitPriceForAllFile(asset int32) {
// 	if this.Channel == nil {
// 		return
// 	}
// 	this.Channel.CleanUnitPrices(asset)
// }

// func (this *Dsp) GetFileUnitPrice(asset int32) (uint64, error) {
// 	if this.Channel == nil {
// 		return 0, nil
// 	}
// 	return this.Channel.GetUnitPrices(asset)
// }

func (this *Dsp) GetChannelPid() *actor.PID {
	if this.Channel == nil {
		return nil
	}
	return this.Channel.GetChannelPid()
}

func (this *Dsp) NetworkProtocol() string {
	if this.Channel == nil {
		log.Warnf("channel is not instance for service NetworkProtocol")
		return ""
	}
	return this.config.ChannelProtocol
}

// func (this *Dsp) StartChannelService() error {
// 	if this.Channel == nil {
// 		log.Warnf("channel is not instance for service StartService")
// 		return nil
// 	}
// 	return this.Channel.StartService()
// }

func (this *Dsp) HasChannelInstance() bool {
	return this.Channel != nil
}

func (this *Dsp) ChannelRunning() bool {
	if this.Channel == nil {
		log.Warnf("channel is not instance for service ChannelRunning")
		return false
	}
	return this.Channel.Active()
}

func (this *Dsp) ChannelFirstSyncing() bool {
	if this.Channel == nil {
		log.Warnf("channel is not instance for service ChannelFirstSyncing")
		return false
	}
	return this.Channel.SyncingBlock()
}

func (this *Dsp) GetCurrentFilterBlockHeight() uint32 {
	if this.Channel == nil {
		log.Warnf("channel is not instance for service GetCurrentFilterBlockHeight")
		return 0
	}
	return this.Channel.GetCurrentFilterBlockHeight()
}
func (this *Dsp) StopService() {
	if this.Channel == nil {
		log.Warnf("channel is not instance for service StopService")
		return
	}
	this.Channel.StopService()
}
func (this *Dsp) GetCloseCh() chan struct{} {
	if this.Channel == nil {
		log.Warnf("channel is not instance for service GetCloseCh")
		return nil
	}
	return this.Channel.GetCloseCh()
}

// func (this *Dsp) SetChannelDB(db *store.ChannelDB) {
// 	if this.Channel == nil {
// 		log.Warnf("channel is not instance for service SetChannelDB")
// 		return
// 	}
// 	this.Channel.SetChannelDB(db)
// }

func (this *Dsp) GetAllPartners() []string {
	if this.Channel == nil {
		log.Warnf("channel is not instance for service GetAllPartners")
		return nil
	}
	partners, _ := this.Channel.GetAllPartners()
	return partners
}

// func (this *Dsp) AddChannelInfo(id uint64, partnerAddr string) error {
// 	if this.Channel == nil {
// 		log.Warnf("channel is not instance for service AddChannelInfo")
// 		return nil
// 	}
// 	return this.Channel.AddChannelInfo(id, partnerAddr)
// }
// func (this *Dsp) GetChannelInfoFromDB(targetAddress string) (*store.ChannelInfo, error) {
// 	if this.Channel == nil {
// 		return nil, dspErr.New(dspErr.CHANNEL_INTERNAL_ERROR, "no channel")
// 	}
// 	return this.Channel.GetChannelInfoFromDB(targetAddress)
// }
// func (this *Dsp) OverridePartners() error {
// 	if this.Channel == nil {
// 		log.Warnf("channel is not instance for service OverridePartners")
// 		return nil
// 	}
// 	return this.Channel.OverridePartners()
// }

func (this *Dsp) WaitForConnected(walletAddr string, timeout time.Duration) error {
	if this.Channel == nil {
		log.Warnf("channel is not instance for service WaitForConnected")
		return nil
	}
	return this.Channel.WaitForConnected(walletAddr, timeout)
}
func (this *Dsp) ChannelReachale(walletAddr string) bool {
	if this.Channel == nil {
		log.Warnf("channel is not instance for service ChannelReachale")
		return false
	}
	return this.Channel.ChannelReachale(walletAddr)
}
func (this *Dsp) HealthyCheckNodeState(walletAddr string) error {
	if this.Channel == nil {
		log.Warnf("channel is not instance for service HealthyCheckNodeState")
		return nil
	}
	return this.Channel.HealthyCheckNodeState(walletAddr)
}

func (this *Dsp) OpenChannel(targetAddress string, depositAmount uint64) (common.ChannelID, error) {
	if this.Channel == nil {
		log.Warnf("channel is not instance for service OpenChannel")
		return 0, nil
	}
	// add dns to health check list
	return this.Channel.OpenChannel(targetAddress, depositAmount)
}

// func (this *Dsp) SetChannelIsDNS(targetAddr string, isDNS bool) error {
// 	if this.Channel == nil {
// 		log.Warnf("channel is not instance for service SetChannelIsDNS")
// 		return nil
// 	}
// 	return this.Channel.SetChannelIsDNS(targetAddr, isDNS)
// }

func (this *Dsp) CloseChannel(targetAddress string) error {
	if this.Channel == nil {
		log.Warnf("channel is not instance for service CloseChannel")
		return nil
	}
	return this.Channel.CloseChannel(targetAddress)
}

func (this *Dsp) SetDeposit(targetAddress string, amount uint64) error {
	if this.Channel == nil {
		log.Warnf("channel is not instance for service SetDeposit")
		return nil
	}
	return this.Channel.SetDeposit(targetAddress, amount)
}
func (this *Dsp) CanTransfer(to string, amount uint64) error {
	if this.Channel == nil {
		log.Warnf("channel is not instance for service CanTransfer")
		return nil
	}
	return this.Channel.CanTransfer(to, amount)
}

func (this *Dsp) NewPaymentId() int32 {
	if this.Channel == nil {
		log.Warnf("channel is not instance for service NewPaymentId")
		return 0
	}
	return this.Channel.NewPaymentId()
}

func (this *Dsp) DirectTransfer(paymentId int32, amount uint64, to string) error {
	if this.Channel == nil {
		log.Warnf("channel is not instance for service DirectTransfer")
		return nil
	}
	return this.Channel.DirectTransfer(paymentId, amount, to)
}
func (this *Dsp) MediaTransfer(paymentId int32, amount uint64, media, to string) error {
	if this.Channel == nil {
		log.Warnf("channel is not instance for service MediaTransfer")
		return nil
	}
	return this.Channel.MediaTransfer(paymentId, amount, media, to)
}
func (this *Dsp) GetTotalDepositBalance(targetAddress string) (uint64, error) {
	if this.Channel == nil {
		log.Warnf("channel is not instance for service GetTotalDepositBalance")
		return 0, nil
	}
	return this.Channel.GetTotalDepositBalance(targetAddress)
}
func (this *Dsp) GetAvailableBalance(partnerAddress string) (uint64, error) {
	if this.Channel == nil {
		log.Warnf("channel is not instance for service GetAvailableBalance")
		return 0, nil
	}
	return this.Channel.GetAvailableBalance(partnerAddress)
}
func (this *Dsp) GetTotalWithdraw(partnerAddress string) (uint64, error) {
	if this.Channel == nil {
		log.Warnf("channel is not instance for service GetTotalWithdraw")
		return 0, nil
	}
	return this.Channel.GetTotalWithdraw(partnerAddress)
}
func (this *Dsp) GetCurrentBalance(partnerAddress string) (uint64, error) {
	if this.Channel == nil {
		log.Warnf("channel is not instance for service GetCurrentBalance")
		return 0, nil
	}
	return this.Channel.GetCurrentBalance(partnerAddress)
}
func (this *Dsp) Withdraw(targetAddress string, amount uint64) (bool, error) {
	if this.Channel == nil {
		log.Warnf("channel is not instance for service Withdraw")
		return false, nil
	}
	return this.Channel.Withdraw(targetAddress, amount)
}
func (this *Dsp) CooperativeSettle(targetAddress string) error {
	if this.Channel == nil {
		log.Warnf("channel is not instance for service CooperativeSettle")
		return nil
	}
	return this.Channel.CooperativeSettle(targetAddress)
}

// func (this *Dsp) SetUnitPrices(asset int32, price uint64) {
// 	if this.Channel == nil {
// 		log.Warnf("channel is not instance for service SetUnitPrices")
// 		return
// 	}
// 	this.Channel.SetUnitPrices(asset, price)
// }
// func (this *Dsp) GetUnitPrices(asset int32) (uint64, error) {
// 	if this.Channel == nil {
// 		log.Warnf("channel is not instance for service GetUnitPrices")
// 		return 0, nil
// 	}
// 	return this.Channel.GetUnitPrices(asset)
// }
// func (this *Dsp) CleanUnitPrices(asset int32) {
// 	if this.Channel == nil {
// 		log.Warnf("channel is not instance for service CleanUnitPrices")
// 		return
// 	}
// 	this.Channel.CleanUnitPrices(asset)
// }

func (this *Dsp) ChannelExist(walletAddr string) bool {
	if this.Channel == nil {
		log.Warnf("channel is not instance for service ChannelExist")
		return false
	}
	return this.Channel.ChannelExist(walletAddr)
}
func (this *Dsp) GetChannelInfo(walletAddr string) (*ch_actor.ChannelInfo, error) {
	if this.Channel == nil {
		log.Warnf("channel is not instance for service GetChannelInfo")
		return nil, nil
	}
	return this.Channel.GetChannelInfo(walletAddr)
}
func (this *Dsp) AllChannels() (*ch_actor.ChannelsInfoResp, error) {
	if this.Channel == nil {
		return nil, nil
	}
	return this.Channel.AllChannels()
}
