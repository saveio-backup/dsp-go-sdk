package channel

import (
	"errors"
	"time"

	dspcom "github.com/oniio/dsp-go-sdk/common"
	"github.com/oniio/dsp-go-sdk/config"
	"github.com/oniio/dsp-go-sdk/store"
	sdk "github.com/oniio/oniChain-go-sdk"
	"github.com/oniio/oniChain-go-sdk/usdt"
	chaincomm "github.com/oniio/oniChain/common"
	"github.com/oniio/oniChain/common/log"
	"github.com/oniio/oniChain/smartcontract/service/native/utils"
	ch "github.com/oniio/oniChannel"
	"github.com/oniio/oniChannel/common"
	"github.com/oniio/oniChannel/transfer"
)

type Channel struct {
	channel    *ch.Channel
	closeCh    chan struct{}
	unitPrices map[int32]uint64
	channelDB  *store.ChannelDB
	walletAddr string
}

func NewChannelService(cfg *config.DspConfig, chain *sdk.Chain) (*Channel, error) {
	if cfg == nil {
		cfg = config.DefaultDspConfig()
	}
	var channelConfig = &ch.ChannelConfig{
		ClientType:    cfg.ChannelClientType,
		ChainNodeURL:  cfg.ChainRpcAddr,
		ListenAddress: cfg.ChannelListenAddr,
		Protocol:      cfg.ChannelProtocol,
		RevealTimeout: cfg.ChannelRevealTimeout,
	}
	channel, err := ch.NewChannelService(channelConfig, chain.Native.Channel.DefAcc)
	if err != nil {
		return nil, err
	}
	return &Channel{
		channel:    channel,
		closeCh:    make(chan struct{}, 1),
		walletAddr: chain.Native.Channel.DefAcc.Address.ToBase58(),
	}, nil
}

// SetHostAddr. set host address for wallet
func (this *Channel) SetHostAddr(walletAddr, host string) error {
	addr, err := chaincomm.AddressFromBase58(walletAddr)
	if err != nil {
		return err
	}
	this.channel.Service.SetHostAddr(common.Address(addr), host)
	this.channelDB.AddPartner(this.walletAddr, walletAddr)
	return nil
}

// StartService. start channel service
func (this *Channel) StartService() error {
	err := this.channel.StartService()
	if err != nil {
		return err
	}
	go this.registerReceiveNotification()
	return nil
}

func (this *Channel) StopService() {
	close(this.closeCh)
}

func (this *Channel) SetChannelDB(db *store.ChannelDB) {
	this.channelDB = db
}

// GetAllPartners. get all partners from local db
func (this *Channel) GetAllPartners() []string {
	partners, _ := this.channelDB.GetPartners(this.walletAddr)
	return partners
}

// OverridePartners. override local partners with neighbours from channel
func (this *Channel) OverridePartners() error {
	newPartners := make([]string, 0)
	neighbours := transfer.GetNeighbours(this.channel.Service.StateFromChannel())
	for _, v := range neighbours {
		newPartners = append(newPartners, common.ToBase58(v))
	}
	log.Debugf("override new partners %v\n", newPartners)
	return this.channelDB.OverridePartners(this.walletAddr, newPartners)
}

// WaitForConnected. wait for conected for a period.
func (this *Channel) WaitForConnected(walletAddr string, timeout time.Duration) error {
	interval := time.Duration(dspcom.CHECK_CHANNEL_STATE_INTERVAL) * time.Second
	secs := int(timeout / interval)
	if secs <= 0 {
		secs = 1
	}
	for i := 0; i < secs; i++ {
		if this.ChannelReachale(walletAddr) {
			return nil
		}
		<-time.After(interval)
	}
	return errors.New("wait for connected timeout")
}

// ChannelReachale. is channel open and reachable
func (this *Channel) ChannelReachale(walletAddr string) bool {
	target, _ := chaincomm.AddressFromBase58(walletAddr)
	state := transfer.GetNodeNetworkStatus(this.channel.Service.StateFromChannel(), common.Address(target))
	log.Debugf("state %s, target:%s", state, target.ToBase58())
	if state == transfer.NetworkReachable {
		return true
	}
	return false
}

// OpenChannel. open channel for target of token.
func (this *Channel) OpenChannel(targetAddress string) (common.ChannelID, error) {
	token := common.TokenAddress(usdt.USDT_CONTRACT_ADDRESS)
	target, err := chaincomm.AddressFromBase58(targetAddress)
	if err != nil {
		return 0, err
	}
	channelID := this.channel.Service.OpenChannel(token, common.Address(target))
	if channelID == 0 {
		return 0, errors.New("setup channel failed")
	}
	return channelID, nil
}

func (this *Channel) ChannelClose(targetAddress string) error {
	token := common.TokenAddress(usdt.USDT_CONTRACT_ADDRESS)
	target, err := chaincomm.AddressFromBase58(targetAddress)
	if err != nil {
		return err
	}
	this.channel.Service.ChannelClose(token, common.Address(target), common.NetworkTimeout(20))
	this.channelDB.DeletePartner(this.walletAddr, targetAddress)
	return nil
}

// SetDeposit. deposit money to target
func (this *Channel) SetDeposit(targetAddress string, amount uint64) error {
	if amount == 0 {
		return nil
	}
	token := common.TokenAddress(usdt.USDT_CONTRACT_ADDRESS)
	target, err := chaincomm.AddressFromBase58(targetAddress)
	if err != nil {
		return err
	}
	depositAmount := common.TokenAmount(amount)
	err = this.channel.Service.SetTotalChannelDeposit(token, common.Address(target), depositAmount)
	if err != nil {
		return err
	}
	return nil
}

// DirectTransfer. direct transfer to with payment id, and amount
func (this *Channel) DirectTransfer(paymentId int32, amount uint64, to string) error {
	target, err := chaincomm.AddressFromBase58(to)
	if err != nil {
		return err
	}
	status, err := this.channel.Service.DirectTransferAsync(common.TokenAmount(amount), common.Address(target), common.PaymentID(paymentId))
	if err != nil {
		return err
	}
	select {
	case ret := <-status:
		if ret {
			return nil
		}
		return errors.New("direct transfer payment failed")
	case <-time.After(time.Duration(dspcom.CHANNEL_TRANSFER_TIMEOUT) * time.Second):
		return errors.New("direct transfer timeout")
	}
}

func (this *Channel) MediaTransfer(paymentId int32, amount uint64, to string) error {
	registryAddress := common.PaymentNetworkID(utils.MicroPayContractAddress)
	tokenAddress := common.TokenAddress(usdt.USDT_CONTRACT_ADDRESS)
	target, err := chaincomm.AddressFromBase58(to)
	if err != nil {
		return err
	}
	status, err := this.channel.Service.MediaTransfer(registryAddress, tokenAddress, common.TokenAmount(amount), common.Address(target), common.PaymentID(paymentId))
	if err != nil {
		return err
	}
	select {
	case ret := <-status:
		if ret {
			return nil
		}
		return errors.New("media transfer payment failed")
	case <-time.After(time.Duration(dspcom.CHANNEL_TRANSFER_TIMEOUT) * time.Second):
		return errors.New("media transfer timeout")
	}
}

// GetTargetBalance. check target deposit balance
func (this *Channel) GetTargetBalance(targetAddress string) (uint64, error) {
	target, err := chaincomm.AddressFromBase58(targetAddress)
	if err != nil {
		return 0, err
	}
	if this.channel.Service.Wal == nil {
		return 0, errors.New("channel sqlite init failed")
	}
	chainState := this.channel.Service.StateFromChannel()

	channelState := transfer.GetChannelStateFor(chainState, common.PaymentNetworkID(common.Address(utils.MicroPayContractAddress)),
		common.TokenAddress(usdt.USDT_CONTRACT_ADDRESS), common.Address(target))
	if channelState == nil {
		return 0, errors.New("channel state is nil")
	}
	state := channelState.GetChannelEndState(0)
	return uint64(state.GetContractBalance()), nil
}

func (this *Channel) GetCurrentBalance(partnerAddress string) (uint64, error) {
	partner, err := chaincomm.AddressFromBase58(partnerAddress)
	if err != nil {
		return 0, err
	}
	registryAddress := common.PaymentNetworkID(utils.MicroPayContractAddress)
	tokenAddress := common.TokenAddress(usdt.USDT_CONTRACT_ADDRESS)
	chanState := this.channel.Service.GetChannel(registryAddress, tokenAddress, common.Address(partner))
	if chanState == nil {
		return 0, nil
	}
	var ourLocked, parLocked common.TokenAmount
	ourBalance := chanState.OurState.GetGasBalance()
	outCtBal := chanState.OurState.ContractBalance
	parBalance := chanState.PartnerState.GetGasBalance()
	parCtBal := chanState.PartnerState.ContractBalance

	if chanState.OurState.BalanceProof != nil {
		ourLocked = chanState.OurState.BalanceProof.LockedAmount
	}
	if chanState.PartnerState.BalanceProof != nil {
		parLocked = chanState.PartnerState.BalanceProof.LockedAmount
	}

	log.Infof("[Balance] Our[BL: %d CT: %d LK: %d] Par[BL: %d CT: %d LK: %d]",
		ourBalance, outCtBal, ourLocked, parBalance, parCtBal, parLocked)
	return uint64(ourBalance), nil
}

// Withdraw. withdraw balance with target address
func (this *Channel) Withdraw(targetAddress string, amount uint64) (bool, error) {
	token := common.TokenAddress(usdt.USDT_CONTRACT_ADDRESS)
	target, err := chaincomm.AddressFromBase58(targetAddress)
	if err != nil {
		return false, err
	}
	withdrawAmount := common.TokenAmount(amount)
	withdrawCh, err := this.channel.Service.Withdraw(token, common.Address(target), withdrawAmount)
	if err != nil {
		return false, err
	}
	select {
	case ret := <-withdrawCh:
		return ret, nil
	case <-time.After(time.Duration(dspcom.CHANNEL_WITHDRAW_TIMEOUT) * time.Second):
		return false, errors.New("withdraw timeout")
	}
}

// CooperativeSettle. settle channel cooperatively
func (this *Channel) CooperativeSettle(targetAddress string) error {
	token := common.TokenAddress(usdt.USDT_CONTRACT_ADDRESS)
	target, err := chaincomm.AddressFromBase58(targetAddress)
	if err != nil {
		return err
	}
	return this.channel.Service.ChannelCooperativeSettle(token, common.Address(target))
}

// SetUnitPrices
func (this *Channel) SetUnitPrices(asset int32, price uint64) {
	if this.unitPrices == nil {
		this.unitPrices = make(map[int32]uint64, 0)
	}
	this.unitPrices[asset] = price
}

func (this *Channel) GetUnitPrices(asset int32) (uint64, error) {
	if this.unitPrices == nil {
		return 0, errors.New("no unit prices")
	}
	p, ok := this.unitPrices[asset]
	if !ok {
		return 0, errors.New("no unit prices")
	}
	return p, nil
}

func (this *Channel) CleanUninPrices(asset int32) {
	if this.unitPrices == nil {
		return
	}
	delete(this.unitPrices, asset)
}

func (this *Channel) GetPayment(paymentId int32) (*store.Payment, error) {
	return this.channelDB.GetPayment(paymentId)
}

func (this *Channel) DeletePayment(paymentId int32) error {
	return this.channelDB.RemovePayment(paymentId)
}

// registerReceiveNotification. register receive payment notification
func (this *Channel) registerReceiveNotification() {
	receiveChan := make(chan *transfer.EventPaymentReceivedSuccess)
	this.channel.RegisterReceiveNotification(receiveChan)
	for {
		select {
		case event := <-receiveChan:
			addr, err := chaincomm.AddressParseFromBytes(event.Initiator[:])
			if err != nil {
				continue
			}
			log.Debugf("PaymentReceive amount %d from %s with paymentID %d\n",
				event.Amount, addr.ToBase58(), event.Identifier)
			this.channelDB.AddPayment(addr.ToBase58(), int32(event.Identifier), uint64(event.Amount))
		case <-this.closeCh:
			return
		}
	}
}
