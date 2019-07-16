package channel

import (
	"errors"
	"os"
	"strings"
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
	chain      *sdk.Chain
	cfg        *config.DspConfig
}

type channelInfo struct {
	ChannelId         uint32
	Balance           uint64
	BalanceFormat     string
	Address           string
	HostAddr          string
	TokenAddr         string
	Participant1State int
	ParticiPant2State int
}

type ChannelInfosResp struct {
	Balance       uint64
	BalanceFormat string
	Channels      []*channelInfo
}

func NewChannelService(cfg *config.DspConfig, chain *sdk.Chain, getHostAddrCallBack func(chaincomm.Address) (string, error)) (*Channel, error) {
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
	hostAddrCallBack := func(addr common.Address) (string, error) {
		return getHostAddrCallBack(chaincomm.Address(addr))
	}
	err = ch_actor.SetGetHostAddrCallback(hostAddrCallBack)
	if err != nil {
		return nil, err
	}
	chnPid := channelActor.GetLocalPID()
	return &Channel{
		chActorId:  chnPid,
		chActor:    channelActor,
		closeCh:    make(chan struct{}, 1),
		walletAddr: chain.Native.Channel.DefAcc.Address.ToBase58(),
		chain:      chain,
		cfg:        cfg,
	}, nil
}

func (this *Channel) GetChannelPid() *actor.PID {
	return this.chActorId
}

// SetHostAddr. set host address for wallet
func (this *Channel) GetHostAddr(walletAddr string) (string, error) {
	log.Debugf("[dsp-go-sdk-channel] GetHostAddr %s", walletAddr)
	addr, err := chaincomm.AddressFromBase58(walletAddr)
	if err != nil {
		return "", err
	}
	log.Debugf("GetHostAddr %v", walletAddr)
	host, err := ch_actor.GetHostAddr(common.Address(addr))
	if err != nil {
		return "", err
	}
	prefix := this.cfg.ChannelProtocol + "://"
	if strings.Contains(host, prefix) {
		return host, nil
	}
	return prefix + host, nil
}

// SetHostAddr. set host address for wallet
func (this *Channel) SetHostAddr(walletAddr, host string) error {
	index := strings.Index(host, this.cfg.ChannelProtocol)
	realHost := host
	if index != -1 {
		prefix := this.cfg.ChannelProtocol + "://"
		realHost = host[index+len(prefix):]
	}
	log.Debugf("[dsp-go-sdk-channel] SetHostAddr %s %s", walletAddr, realHost)
	addr, err := chaincomm.AddressFromBase58(walletAddr)
	if err != nil {
		return err
	}
	ch_actor.SetHostAddr(common.Address(addr), realHost)
	this.channelDB.AddPartner(this.walletAddr, walletAddr)
	return nil
}

// StartService. start channel service
func (this *Channel) StartService() error {
	//start connnect target
	log.Debugf("[dsp-go-sdk-channel] StartService")
	err := this.chActor.Start()
	if err != nil {
		return err
	}
	log.Debugf("StartService done")
	this.isStart = true
	this.OverridePartners()
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
	log.Debug("[dsp-go-sdk-channel] StopService")
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
	log.Debugf("[dsp-go-sdk-channel] OverridePartners")
	if !this.isStart {
		return errors.New("channel service is not start")
	}
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
	log.Debugf("[dsp-go-sdk-channel] WaitForConnected %s", walletAddr)
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
	log.Debugf("[dsp-go-sdk-channel] ChannelReachale %s", walletAddr)
	target, _ := chaincomm.AddressFromBase58(walletAddr)
	reachable, _ := ch_actor.ChannelReachable(common.Address(target))
	return reachable
}

func (this *Channel) HealthyCheckNodeState(walletAddr string) error {
	log.Debugf("[dsp-go-sdk-channel] HealthyCheckNodeState %s", walletAddr)
	target, err := chaincomm.AddressFromBase58(walletAddr)
	if err != nil {
		return err
	}
	return ch_actor.HealthyCheckNodeState(common.Address(target))
}

// OpenChannel. open channel for target of token.
func (this *Channel) OpenChannel(targetAddress string, depositAmount uint64) (common.ChannelID, error) {
	log.Debugf("[dsp-go-sdk-channel] OpenChannel %s", targetAddress)
	if !this.isStart {
		return 0, errors.New("channel service is not start")
	}
	token := common.TokenAddress(usdt.USDT_CONTRACT_ADDRESS)
	target, err := chaincomm.AddressFromBase58(targetAddress)
	if err != nil {
		return 0, err
	}
	channelID, err := ch_actor.OpenChannel(token, common.Address(target))
	log.Debugf("actor open channel id :%v, %s", channelID, err)
	if err != nil {
		return 0, err
	}
	if channelID == 0 {
		return 0, errors.New("setup channel failed")
	}
	log.Infof("connect to dns node :%s, deposit %d", targetAddress, depositAmount)
	if depositAmount == 0 {
		return channelID, nil
	}
	// err = this.WaitForConnected(targetAddress, time.Duration(dspcom.WAIT_CHANNEL_CONNECT_TIMEOUT)*time.Second)
	// if err != nil {
	// 	log.Errorf("wait channel connected err %s %s", targetAddress, err)
	// 	return channelID, err
	// }
	bal, _ := this.GetTotalDepositBalance(targetAddress)
	log.Debugf("channel to %s current balance %d", targetAddress, bal)
	if bal >= depositAmount {
		return channelID, nil
	}
	err = this.SetDeposit(targetAddress, depositAmount)
	if err != nil && strings.Index(err.Error(), "totalDeposit must big than contractBalance") == -1 {
		log.Debugf("deposit result %s", err)
		// TODO: withdraw and close channel
		return 0, err
	}
	return channelID, nil
}

func (this *Channel) ChannelClose(targetAddress string) error {
	log.Debugf("[dsp-go-sdk-channel] ChannelClose %s", targetAddress)
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
	log.Debugf("[dsp-go-sdk-channel] SetDeposit %s", targetAddress)
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
	log.Debugf("[dsp-go-sdk-channel] CanTransfer %s", to)
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
	log.Debugf("[dsp-go-sdk-channel] DirectTransfer %s", to)
	if !this.isStart {
		return errors.New("channel service is not start")
	}
	err := this.CanTransfer(to, amount)
	if err != nil {
		return err
	}
	target, err := chaincomm.AddressFromBase58(to)
	if err != nil {
		return err
	}
	type transferResp struct {
		success bool
		err     error
	}
	transferRespCh := make(chan *transferResp, 0)
	go func() {
		success, err := ch_actor.DirectTransferAsync(common.TokenAmount(amount), common.Address(target), common.PaymentID(paymentId))
		transferRespCh <- &transferResp{
			success: success,
			err:     err,
		}
		log.Debugf("direct transfer success: %t", success)
	}()
	for {
		select {
		case ret := <-transferRespCh:
			if ret.err != nil {
				return ret.err
			}
			log.Debugf("direct transfer success: %t", ret.success)
			return nil
		case <-time.After(time.Minute):
			return errors.New("direct transfer timeout")
		}
	}
}

func (this *Channel) MediaTransfer(paymentId int32, amount uint64, to string) error {
	log.Debugf("[dsp-go-sdk-channel] MediaTransfer %s", to)
	if !this.isStart {
		return errors.New("channel service is not start")
	}
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
	log.Debugf("[dsp-go-sdk-channel] GetTotalDepositBalance %s", targetAddress)
	partner, err := chaincomm.AddressFromBase58(targetAddress)
	if err != nil {
		return 0, err
	}
	return ch_actor.GetTotalDepositBalance(common.Address(partner))
}

// GetAvaliableBalance. get avaliable balance
func (this *Channel) GetAvailableBalance(partnerAddress string) (uint64, error) {
	log.Debugf("[dsp-go-sdk-channel] GetAvailableBalance %s", partnerAddress)
	partner, err := chaincomm.AddressFromBase58(partnerAddress)
	if err != nil {
		return 0, err
	}
	return ch_actor.GetAvailableBalance(common.Address(partner))
}

func (this *Channel) GetTotalWithdraw(partnerAddress string) (uint64, error) {
	log.Debugf("[dsp-go-sdk-channel] GetTotalWithdraw %s", partnerAddress)
	partner, err := chaincomm.AddressFromBase58(partnerAddress)
	if err != nil {
		return 0, err
	}
	return ch_actor.GetTotalWithdraw(common.Address(partner))
}

func (this *Channel) GetCurrentBalance(partnerAddress string) (uint64, error) {
	log.Debugf("[dsp-go-sdk-channel] GetCurrentBalance %s", partnerAddress)
	partner, err := chaincomm.AddressFromBase58(partnerAddress)
	if err != nil {
		return 0, err
	}
	return ch_actor.GetCurrentBalance(common.Address(partner))
}

// Withdraw. withdraw balance with target address
func (this *Channel) Withdraw(targetAddress string, amount uint64) (bool, error) {
	log.Debugf("[dsp-go-sdk-channel] Withdraw %s", targetAddress)
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
	log.Debugf("[dsp-go-sdk-channel] CooperativeSettle %s", targetAddress)
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

func (this *Channel) CleanUnitPrices(asset int32) {
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

func (this *Channel) ChannelExist(walletAddr string) bool {
	if !this.isStart {
		return false
	}
	all, _ := ch_actor.GetAllChannels()
	if all == nil {
		return true
	}
	for _, ch := range all.Channels {
		if ch.Address != walletAddr {
			continue
		}
		return false
	}
	return true
}

func (this *Channel) AllChannels() *ChannelInfosResp {
	log.Debugf("[dsp-go-sdk-channel] AllChannels")
	if !this.isStart {
		return nil
	}
	infos := make([]*channelInfo, 0)
	resp := &ChannelInfosResp{
		Balance:       0,
		BalanceFormat: "0",
		Channels:      infos,
	}
	all, _ := ch_actor.GetAllChannels()
	if all == nil {
		return resp
	}
	resp.Balance = all.Balance
	resp.BalanceFormat = all.BalanceFormat
	for _, ch := range all.Channels {
		info, err := this.chain.Native.Channel.GetChannelInfo(uint64(ch.ChannelId), chaincomm.ADDRESS_EMPTY, chaincomm.ADDRESS_EMPTY)
		if err != nil {
			log.Errorf("get channel info err %s", err)
		}
		state1 := 1
		if info != nil && info.Participant1.IsCloser {
			state1 = 0
		}

		state2 := 1
		if info != nil && info.Participant2.IsCloser {
			state2 = 0
		}

		resp.Channels = append(resp.Channels, &channelInfo{
			ChannelId:         ch.ChannelId,
			Address:           ch.Address,
			Balance:           ch.Balance,
			BalanceFormat:     ch.BalanceFormat,
			HostAddr:          ch.HostAddr,
			TokenAddr:         ch.TokenAddr,
			Participant1State: state1,
			ParticiPant2State: state2,
		})
	}
	return resp
}

// registerReceiveNotification. register receive payment notification
func (this *Channel) registerReceiveNotification() {
	log.Debugf("[dsp-go-sdk-channel] registerReceiveNotification")
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
