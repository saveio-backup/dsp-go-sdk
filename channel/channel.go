package channel

import (
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/ontio/ontology-eventbus/actor"
	dspcom "github.com/saveio/dsp-go-sdk/common"
	"github.com/saveio/dsp-go-sdk/config"
	"github.com/saveio/dsp-go-sdk/store"
	ch "github.com/saveio/pylons"
	ch_actor "github.com/saveio/pylons/actor/server"
	"github.com/saveio/pylons/common"
	"github.com/saveio/pylons/transfer"
	sdk "github.com/saveio/themis-go-sdk"
	"github.com/saveio/themis-go-sdk/usdt"
	chaincomm "github.com/saveio/themis/common"
	"github.com/saveio/themis/common/log"
	"github.com/saveio/themis/smartcontract/service/native/utils"
)

type Channel struct {
	chActor    *ch_actor.ChannelActorServer
	chActorId  *actor.PID
	closeCh    chan struct{}
	unitPrices map[int32]uint64
	channelDB  *store.ChannelDB
	walletAddr string
	isStart    bool
}

type channelInfo struct {
	ChannelId     uint32
	Balance       uint64
	BalanceFormat string
	Address       string
	HostAddr      string
	TokenAddr     string
}

type ChannelInfosResp struct {
	Balance       uint64
	BalanceFormat string
	Channels      []*channelInfo
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
		DBPath:        cfg.ChannelDBPath,
	}
	log.Debugf("channel cfg %v", channelConfig)
	if _, err := os.Stat(channelConfig.DBPath); os.IsNotExist(err) {
		err = os.MkdirAll(channelConfig.DBPath, 0755)
		if err != nil {
			return nil, err
		}
	}
	//start channel and actor
	channelActor, err := ch_actor.NewChannelActor(channelConfig, chain.Native.Channel.DefAcc)
	if err != nil {
		return nil, err
	}
	chnPid := channelActor.GetLocalPID()
	return &Channel{
		chActorId:  chnPid,
		chActor:    channelActor,
		closeCh:    make(chan struct{}, 1),
		walletAddr: chain.Native.Channel.DefAcc.Address.ToBase58(),
	}, nil
}

func (this *Channel) GetChannelPid() *actor.PID {
	return this.chActorId
}

// SetHostAddr. set host address for wallet
func (this *Channel) GetHostAddr(walletAddr string) (string, error) {
	addr, err := chaincomm.AddressFromBase58(walletAddr)
	if err != nil {
		return "", err
	}
	log.Debugf("GetHostAddr %v", walletAddr)
	return ch_actor.GetHostAddr(common.Address(addr))
}

// SetHostAddr. set host address for wallet
func (this *Channel) SetHostAddr(walletAddr, host string) error {
	addr, err := chaincomm.AddressFromBase58(walletAddr)
	if err != nil {
		return err
	}
	log.Debugf("SetHostAddr %v %v", walletAddr, host)
	ch_actor.SetHostAddr(common.Address(addr), host)
	this.channelDB.AddPartner(this.walletAddr, walletAddr)
	return nil
}

// StartService. start channel service
func (this *Channel) StartService() error {
	//start connnect target
	err := this.chActor.Start()
	if err != nil {
		return err
	}
	this.isStart = true
	this.OverridePartners()
	go this.registerReceiveNotification()
	return nil
}

// StartServiceAsync. start channel service Async
func (this *Channel) StartServiceAsync() error {
	//start connnect target
	go func() {
		err := this.chActor.Start()
		if err != nil {
			panic(err)
		}
		this.isStart = true
		this.OverridePartners()
	}()
	go this.registerReceiveNotification()
	return nil
}

func (this *Channel) GetCurrentFilterBlockHeight() uint32 {
	height, err := ch_actor.GetLastFilterBlockHeight()
	if err != nil {
		log.Errorf("request err %s", err)
	}
	return height
}

func (this *Channel) StopService() {
	if this.isStart {
		this.chActor.Stop()
	} else {
		this.chActor.GetChannelService().Service.Wal.Storage.Close()
	}
	this.channelDB.Close()
	this.chActorId.Stop()
	close(this.closeCh)
	this.isStart = false
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
	neighbours := transfer.GetNeighbours(this.chActor.GetChannelService().Service.StateFromChannel())
	for _, v := range neighbours {
		newPartners = append(newPartners, common.ToBase58(v))
	}
	log.Debugf("override new partners %v\n", newPartners)
	return this.channelDB.OverridePartners(this.walletAddr, newPartners)
	return nil
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
		} else {
			log.Warn("connect peer:%s failed", walletAddr)
			this.HealthyCheckNodeState(walletAddr)
		}
		<-time.After(interval)
	}
	return errors.New("wait for connected timeout")
}

// ChannelReachale. is channel open and reachable
func (this *Channel) ChannelReachale(walletAddr string) bool {
	target, _ := chaincomm.AddressFromBase58(walletAddr)
	reachable, _ := ch_actor.ChannelReachable(common.Address(target))
	return reachable
}

func (this *Channel) HealthyCheckNodeState(walletAddr string) error {
	target, err := chaincomm.AddressFromBase58(walletAddr)
	if err != nil {
		return err
	}
	return ch_actor.HealthyCheckNodeState(common.Address(target))
}

// OpenChannel. open channel for target of token.
func (this *Channel) OpenChannel(targetAddress string) (common.ChannelID, error) {
	if !this.isStart {
		return 0, errors.New("channel service is not start")
	}
	token := common.TokenAddress(usdt.USDT_CONTRACT_ADDRESS)
	target, err := chaincomm.AddressFromBase58(targetAddress)
	if err != nil {
		return 0, err
	}
	channelID, err := ch_actor.OpenChannel(token, common.Address(target))
	if err != nil {
		return 0, err
	}
	if channelID == 0 {
		return 0, errors.New("setup channel failed")
	}
	return channelID, nil
}

func (this *Channel) ChannelClose(targetAddress string) error {
	if !this.isStart {
		return errors.New("channel service is not start")
	}
	target, err := chaincomm.AddressFromBase58(targetAddress)
	if err != nil {
		return err
	}
	success, err := ch_actor.CloseChannel(common.Address(target))
	if err == nil && success {
		this.channelDB.DeletePartner(this.walletAddr, targetAddress)
	}
	return err
}

// SetDeposit. deposit money to target
func (this *Channel) SetDeposit(targetAddress string, amount uint64) error {
	if amount == 0 {
		return nil
	}
	if !this.isStart {
		return errors.New("channel service is not start")
	}
	token := common.TokenAddress(usdt.USDT_CONTRACT_ADDRESS)
	target, err := chaincomm.AddressFromBase58(targetAddress)
	if err != nil {
		return err
	}
	depositAmount := common.TokenAmount(amount)
	err = ch_actor.SetTotalChannelDeposit(token, common.Address(target), depositAmount)
	if err != nil {
		return err
	}
	return nil
}

func (this *Channel) CanTransfer(to string, amount uint64) error {
	if !this.isStart {
		return errors.New("channel service is not start")
	}
	target, err := chaincomm.AddressFromBase58(to)
	if err != nil {
		return err
	}
	interval := time.Duration(dspcom.CHECK_CHANNEL_CAN_TRANSFER_INTERVAL) * time.Second
	secs := int(dspcom.CHECK_CHANNEL_CAN_TRANSFER_TIMEOUT / interval)
	if secs <= 0 {
		secs = 1
	}
	for i := 0; i < secs; i++ {
		ret, err := ch_actor.CanTransfer(common.Address(target), common.TokenAmount(amount))
		log.Debugf("CanTransfer ret %t err %s", ret, err)
		if err == nil {
			return nil
		}
		<-time.After(interval)
	}
	return errors.New("check can transfer timeout")
}

// DirectTransfer. direct transfer to with payment id, and amount
func (this *Channel) DirectTransfer(paymentId int32, amount uint64, to string) error {
	err := this.CanTransfer(to, amount)
	if err != nil {
		return err
	}
	target, err := chaincomm.AddressFromBase58(to)
	if err != nil {
		return err
	}
	success, err := ch_actor.DirectTransferAsync(common.TokenAmount(amount), common.Address(target), common.PaymentID(paymentId))
	if err != nil {
		return err
	}
	log.Debugf("direct transfer success: %t", success)
	if success {
		return nil
	}
	return errors.New(fmt.Sprintf("direct transfer failed: %t", success))
}

func (this *Channel) MediaTransfer(paymentId int32, amount uint64, to string) error {
	err := this.CanTransfer(to, amount)
	if err != nil {
		log.Errorf("can't transter id %d, err %s", paymentId, err)
		return err
	}
	registryAddress := common.PaymentNetworkID(utils.MicroPayContractAddress)
	tokenAddress := common.TokenAddress(usdt.USDT_CONTRACT_ADDRESS)
	target, err := chaincomm.AddressFromBase58(to)
	if err != nil {
		return err
	}
	type mediaTransferResp struct {
		success bool
		err     error
	}
	mediaTransferCh := make(chan *mediaTransferResp, 0)
	go func() {
		success, err := ch_actor.MediaTransfer(registryAddress, tokenAddress, common.TokenAmount(amount), common.Address(target), common.PaymentID(paymentId))
		mediaTransferCh <- &mediaTransferResp{
			success: success,
			err:     err,
		}
	}()
	for {
		select {
		case ret := <-mediaTransferCh:
			if ret.err != nil {
				return ret.err
			}
			log.Debugf("media transfer success: %t", ret.success)
			return nil
		case <-time.After(time.Minute):
			return errors.New("media transfer timeout")
		}
	}
}

// GetTargetBalance. check total deposit balance
func (this *Channel) GetTotalDepositBalance(targetAddress string) (uint64, error) {
	partner, err := chaincomm.AddressFromBase58(targetAddress)
	if err != nil {
		return 0, err
	}
	return ch_actor.GetTotalDepositBalance(common.Address(partner))
}

// GetAvaliableBalance. get avaliable balance
func (this *Channel) GetAvaliableBalance(partnerAddress string) (uint64, error) {
	partner, err := chaincomm.AddressFromBase58(partnerAddress)
	if err != nil {
		return 0, err
	}
	return ch_actor.GetAvaliableBalance(common.Address(partner))
}

func (this *Channel) GetTotalWithdraw(partnerAddress string) (uint64, error) {
	partner, err := chaincomm.AddressFromBase58(partnerAddress)
	if err != nil {
		return 0, err
	}
	return ch_actor.GetTotalWithdraw(common.Address(partner))
}

func (this *Channel) GetCurrentBalance(partnerAddress string) (uint64, error) {
	partner, err := chaincomm.AddressFromBase58(partnerAddress)
	if err != nil {
		return 0, err
	}
	return ch_actor.GetCurrentBalance(common.Address(partner))
}

// Withdraw. withdraw balance with target address
func (this *Channel) Withdraw(targetAddress string, amount uint64) (bool, error) {
	if !this.isStart {
		return false, errors.New("channel service is not start")
	}
	token := common.TokenAddress(usdt.USDT_CONTRACT_ADDRESS)
	target, err := chaincomm.AddressFromBase58(targetAddress)
	if err != nil {
		return false, err
	}
	withdrawAmount := common.TokenAmount(amount)
	return ch_actor.WithDraw(token, common.Address(target), withdrawAmount)
}

// CooperativeSettle. settle channel cooperatively
func (this *Channel) CooperativeSettle(targetAddress string) error {
	if !this.isStart {
		return errors.New("channel service is not start")
	}
	target, err := chaincomm.AddressFromBase58(targetAddress)
	if err != nil {
		return err
	}
	return ch_actor.CooperativeSettle(common.Address(target))
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

func (this *Channel) AllChannels() *ChannelInfosResp {
	infos := make([]*channelInfo, 0)
	resp := &ChannelInfosResp{
		Balance:       0,
		BalanceFormat: "0",
		Channels:      infos,
	}
	all := ch_actor.GetAllChannels()
	if all == nil {
		return resp
	}
	resp.Balance = all.Balance
	resp.BalanceFormat = all.BalanceFormat
	for _, ch := range all.Channels {
		resp.Channels = append(resp.Channels, &channelInfo{
			ChannelId:     ch.ChannelId,
			Address:       ch.Address,
			Balance:       ch.Balance,
			BalanceFormat: ch.BalanceFormat,
			HostAddr:      ch.HostAddr,
			TokenAddr:     ch.TokenAddr,
		})
	}
	return resp
}

// registerReceiveNotification. register receive payment notification
func (this *Channel) registerReceiveNotification() {
	receiveChan, err := ch_actor.RegisterReceiveNotification()
	log.Debugf("receiveChan:%v, err %v", receiveChan, err)
	if err != nil {
		panic(err)
	}
	for {
		select {
		case event := <-receiveChan:
			addr, err := chaincomm.AddressParseFromBytes(event.Initiator[:])
			if err != nil {
				continue
			}
			log.Debugf("PaymentReceive2 amount %d from %s with paymentID %d\n",
				event.Amount, addr.ToBase58(), event.Identifier)
			this.channelDB.AddPayment(addr.ToBase58(), int32(event.Identifier), uint64(event.Amount))
		case <-this.closeCh:
			return
		}
	}
}
