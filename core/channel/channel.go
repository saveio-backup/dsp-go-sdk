package channel

import (
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"time"

	"github.com/ontio/ontology-eventbus/actor"
	dspcom "github.com/saveio/dsp-go-sdk/common"
	"github.com/saveio/dsp-go-sdk/config"
	dspErr "github.com/saveio/dsp-go-sdk/error"
	"github.com/saveio/dsp-go-sdk/state"
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
	chain      *sdk.Chain
	cfg        *config.DspConfig
	r          *rand.Rand
	lock       *sync.Mutex
	state      *state.SyncState
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

func NewChannelService(cfg *config.DspConfig, chain *sdk.Chain) (*Channel, error) {
	if cfg == nil {
		cfg = config.DefaultDspConfig()
	}
	rpcAddrs := cfg.ChainRpcAddrs
	if len(rpcAddrs) == 0 {
		rpcAddrs = []string{cfg.ChainRpcAddr}
	}
	var channelConfig = &ch.ChannelConfig{
		ClientType:    cfg.ChannelClientType,
		ChainNodeURLs: rpcAddrs,
		RevealTimeout: cfg.ChannelRevealTimeout,
		DBPath:        cfg.ChannelDBPath,
		SettleTimeout: cfg.ChannelSettleTimeout,
		BlockDelay:    cfg.BlockDelay,
	}
	log.Debugf("pylons cfg: %v", channelConfig)
	err := dspcom.CreateDirIfNeed(channelConfig.DBPath)
	if err != nil {
		return nil, dspErr.NewWithError(dspErr.CHANNEL_CREATE_DB_ERROR, err)
	}
	//start channel and actor
	channelActor, err := ch_actor.NewChannelActor(channelConfig, chain.Native.Channel.DefAcc)
	if err != nil {
		log.Debugf("channelActor+++ %s", err)
		return nil, dspErr.NewWithError(dspErr.CHANNEL_CREATE_ACTOR_ERROR, err)
	}
	chnPid := channelActor.GetLocalPID()
	log.Debugf("channel actor %v, pid %v", channelActor, chnPid)
	return &Channel{
		chActorId:  chnPid,
		chActor:    channelActor,
		closeCh:    make(chan struct{}, 1),
		walletAddr: chain.Native.Channel.DefAcc.Address.ToBase58(),
		chain:      chain,
		cfg:        cfg,
		r:          rand.New(rand.NewSource(time.Now().UnixNano())),
		lock:       new(sync.Mutex),
		state:      state.NewSyncState(),
	}, nil
}

func (this *Channel) GetChannelPid() *actor.PID {
	return this.chActorId
}

func (this *Channel) NetworkProtocol() string {
	return this.cfg.ChannelProtocol
}

// StartService. start channel service
func (this *Channel) StartService() error {
	//start connect target
	log.Debugf("StartService")
	this.state.Set(state.ModuleStateStarting)
	err := this.chActor.SyncBlockData()
	log.Debugf("sync block data done")
	if err != nil {
		this.state.Set(state.ModuleStateError)
		log.Errorf("channel sync block err %s", err)
		return dspErr.NewWithError(dspErr.CHANNEL_SYNC_BLOCK_ERROR, err)
	}
	defer func() {
		log.Debugf("in defer cunf")
		if e := recover(); e != nil {
			log.Errorf("send panic recover err %v", e)
		}
	}()
	log.Debugf("start pylons")
	err = ch_actor.StartPylons()
	log.Debugf("start pylons done")
	this.state.Set(state.ModuleStateStarted)
	if err != nil {
		this.state.Set(state.ModuleStateError)
		return dspErr.NewWithError(dspErr.CHANNEL_START_INSTANCE_ERROR, err)
	}
	this.state.Set(state.ModuleStateActive)
	this.OverridePartners()
	return nil
}

func (this *Channel) State() state.ModuleState {
	if this.state == nil {
		return state.ModuleStateNone
	}
	return this.state.Get()
}

func (this *Channel) SyncingBlock() bool {
	if this.state == nil {
		return true
	}
	return this.state.Get() == state.ModuleStateNone || this.state.Get() == state.ModuleStateStarting || this.state.Get() == state.ModuleStateStarted
}

func (this *Channel) Active() bool {
	if this.state == nil {
		return false
	}
	return this.state.Get() == state.ModuleStateActive
}

func (this *Channel) GetCurrentFilterBlockHeight() uint32 {
	height, err := ch_actor.GetLastFilterBlockHeight()
	if err != nil {
		log.Errorf("request err %s", err)
	}
	return height
}

func (this *Channel) StopService() {
	if this.channelDB != nil {
		this.channelDB.Close()
	}
	if this.State() != state.ModuleStateActive {
		// if not start, there is no transport to receive for channel service
		this.state.Set(state.ModuleStateStopping)
		if this.chActor.GetChannelService().Service != nil &&
			this.chActor.GetChannelService().Service.Wal != nil &&
			this.chActor.GetChannelService().Service.Wal.Storage != nil {
			// close channel DB if it is opened
			this.chActor.GetChannelService().Service.Wal.Storage.Close()
		}
		this.state.Set(state.ModuleStateStopped)
		return
	}
	this.state.Set(state.ModuleStateStopping)
	log.Debug("[dsp-go-sdk-channel] StopService")
	err := ch_actor.StopPylons()
	log.Debug("[dsp-go-sdk-channel] StopService done")
	if err != nil {
		this.state.Set(state.ModuleStateError)
		log.Errorf("stop pylons err %s", err)
		return
	}
	if this.chActorId != nil {
		this.chActorId.Stop()
	}
	close(this.closeCh)
	this.state.Set(state.ModuleStateStopped)
}

func (this *Channel) GetCloseCh() chan struct{} {
	return this.closeCh
}

func (this *Channel) SetChannelDB(db *store.ChannelDB) {
	this.channelDB = db
}

func (this *Channel) GetChannelDB() *store.ChannelDB {
	return this.channelDB
}

// GetAllPartners. get all partners from local db
func (this *Channel) GetAllPartners() []string {
	partners, _ := this.channelDB.GetPartners()
	return partners
}

func (this *Channel) AddChannelInfo(id uint64, partnerAddr string) error {
	if this.channelDB == nil {
		return dspErr.New(dspErr.CHANNEL_SET_DB_ERROR, "no channel DB")
	}
	err := this.channelDB.AddChannelInfo(id, partnerAddr)
	if err != nil {
		return dspErr.NewWithError(dspErr.CHANNEL_SET_DB_ERROR, err)
	}
	return nil
}

func (this *Channel) GetChannelInfoFromDB(targetAddress string) (*store.ChannelInfo, error) {
	info, err := this.channelDB.GetChannelInfo(targetAddress)
	if err != nil {
		return nil, dspErr.NewWithError(dspErr.CHANNEL_GET_DB_ERROR, err)
	}
	return info, nil
}

// OverridePartners. override local partners with neighbors from channel
func (this *Channel) OverridePartners() error {
	log.Debugf("[dsp-go-sdk-channel] OverridePartners")
	if this.State() != state.ModuleStateActive {
		return dspErr.New(dspErr.CHANNEL_SERVICE_NOT_START, "channel service is not start")
	}
	newPartners := make([]string, 0)
	neighbors := transfer.GetNeighbours(this.chActor.GetChannelService().Service.StateFromChannel())
	for _, v := range neighbors {
		newPartners = append(newPartners, common.ToBase58(v))
	}
	log.Debugf("override new partners %v\n", newPartners)
	err := this.channelDB.OverridePartners(this.walletAddr, newPartners)
	if err != nil {
		return dspErr.NewWithError(dspErr.CHANNEL_SET_DB_ERROR, err)
	}
	return nil
}

// WaitForConnected. wait for connected for a period.
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
			this.HealthyCheckNodeState(walletAddr)
		}
		<-time.After(interval)
	}
	return dspErr.New(dspErr.NETWORK_TIMEOUT, "wait for connected timeout")
}

// ChannelReachale. is channel open and reachable
func (this *Channel) ChannelReachale(walletAddr string) bool {
	if this.State() != state.ModuleStateActive {
		return false
	}
	target, _ := chaincomm.AddressFromBase58(walletAddr)
	reachable, _ := ch_actor.ChannelReachable(common.Address(target))
	log.Debugf("[dsp-go-sdk-channel] ChannelReachale %s, reachable: %t", walletAddr, reachable)
	return reachable
}

func (this *Channel) HealthyCheckNodeState(walletAddr string) error {
	log.Debugf("[dsp-go-sdk-channel] HealthyCheckNodeState %s", walletAddr)
	if this.State() != state.ModuleStateActive {
		return dspErr.New(dspErr.CHANNEL_SERVICE_NOT_START, "channel service is not start")
	}
	target, err := chaincomm.AddressFromBase58(walletAddr)
	if err != nil {
		return dspErr.NewWithError(dspErr.INVALID_ADDRESS, err)
	}
	err = ch_actor.HealthyCheckNodeState(common.Address(target))
	if err != nil {
		return dspErr.NewWithError(dspErr.CHANNEL_INTERNAL_ERROR, err)
	}
	return nil
}

// OpenChannel. open channel for target of token.
func (this *Channel) OpenChannel(targetAddress string, depositAmount uint64) (common.ChannelID, error) {
	log.Debugf("[dsp-go-sdk-channel] OpenChannel %s", targetAddress)
	if this.State() != state.ModuleStateActive {
		return 0, dspErr.New(dspErr.CHANNEL_SERVICE_NOT_START, "channel service is not start")
	}
	token := common.TokenAddress(usdt.USDT_CONTRACT_ADDRESS)
	target, err := chaincomm.AddressFromBase58(targetAddress)
	if err != nil {
		return 0, dspErr.NewWithError(dspErr.INVALID_ADDRESS, err)
	}
	channelID, err := ch_actor.OpenChannel(token, common.Address(target))
	log.Debugf("actor open channel id :%v, %s", channelID, err)
	if err != nil {
		return 0, dspErr.NewWithError(dspErr.CHANNEL_OPEN_FAILED, err)
	}
	if channelID == 0 {
		return 0, dspErr.New(dspErr.CHANNEL_OPEN_FAILED, "setup channel failed")
	}
	log.Infof("connect to dns node :%s, deposit %d", targetAddress, depositAmount)
	if depositAmount == 0 {
		return channelID, nil
	}
	bal, _ := this.GetTotalDepositBalance(targetAddress)
	log.Debugf("channel to %s current balance %d", targetAddress, bal)
	if bal >= depositAmount {
		return channelID, nil
	}
	err = this.SetDeposit(targetAddress, depositAmount)
	if err != nil && strings.Index(err.Error(), "totalDeposit must big than contractBalance") == -1 {
		log.Debugf("deposit result %s", err)
		// TODO: withdraw and close channel
		return 0, dspErr.NewWithError(dspErr.CHANNEL_DEPOSIT_FAILED, err)
	}
	err = this.channelDB.AddChannelInfo(uint64(channelID), targetAddress)
	if err != nil {

	}
	return channelID, nil
}

func (this *Channel) SetChannelIsDNS(targetAddr string, isDNS bool) error {
	err := this.channelDB.SetChannelIsDNS(targetAddr, isDNS)
	if err != nil {
		return dspErr.NewWithError(dspErr.CHANNEL_SET_DB_ERROR, err)
	}
	return nil
}

func (this *Channel) ChannelClose(targetAddress string) error {
	log.Debugf("[dsp-go-sdk-channel] ChannelClose %s", targetAddress)
	if this.State() != state.ModuleStateActive {
		return dspErr.New(dspErr.CHANNEL_SERVICE_NOT_START, "channel service is not start")
	}
	target, err := chaincomm.AddressFromBase58(targetAddress)
	if err != nil {
		return dspErr.NewWithError(dspErr.INVALID_ADDRESS, err)
	}
	success, err := ch_actor.CloseChannel(common.Address(target))
	if err != nil {
		return dspErr.NewWithError(dspErr.CHANNEL_INTERNAL_ERROR, err)
	}
	if !success {
		return dspErr.New(dspErr.CHANNEL_INTERNAL_ERROR, "close channel done but no success")
	}
	if err := this.channelDB.DeleteChannelInfo(targetAddress); err != nil {
		return dspErr.NewWithError(dspErr.CHANNEL_INTERNAL_ERROR, err)
	}
	return nil
}

// SetDeposit. deposit money to target
func (this *Channel) SetDeposit(targetAddress string, amount uint64) error {
	log.Debugf("[dsp-go-sdk-channel] SetDeposit %s", targetAddress)
	if amount == 0 {
		return nil
	}
	if this.State() != state.ModuleStateActive {
		return dspErr.New(dspErr.CHANNEL_SERVICE_NOT_START, "channel service is not start")
	}
	token := common.TokenAddress(usdt.USDT_CONTRACT_ADDRESS)
	target, err := chaincomm.AddressFromBase58(targetAddress)
	if err != nil {
		return dspErr.NewWithError(dspErr.INVALID_ADDRESS, err)
	}
	depositAmount := common.TokenAmount(amount)
	err = ch_actor.SetTotalChannelDeposit(token, common.Address(target), depositAmount)
	if err != nil {
		return dspErr.NewWithError(dspErr.CHANNEL_DEPOSIT_FAILED, err)
	}
	return nil
}

func (this *Channel) CanTransfer(to string, amount uint64) error {
	log.Debugf("[dsp-go-sdk-channel] CanTransfer %s", to)
	if this.State() != state.ModuleStateActive {
		return dspErr.New(dspErr.CHANNEL_SERVICE_NOT_START, "channel service is not start")
	}
	target, err := chaincomm.AddressFromBase58(to)
	if err != nil {
		return dspErr.NewWithError(dspErr.INVALID_ADDRESS, err)
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
	return dspErr.New(dspErr.CHANNEL_CHECK_TIMEOUT, "check can transfer timeout")
}

func (this *Channel) NewPaymentId() int32 {
	return this.r.Int31()
}

// DirectTransfer. direct transfer to with payment id, and amount
func (this *Channel) DirectTransfer(paymentId int32, amount uint64, to string) error {
	log.Debugf("[dsp-go-sdk-channel] DirectTransfer %s", to)
	if this.State() != state.ModuleStateActive {
		return dspErr.New(dspErr.CHANNEL_SERVICE_NOT_START, "channel service is not start")
	}
	err := this.CanTransfer(to, amount)
	if err != nil {
		return err
	}
	target, err := chaincomm.AddressFromBase58(to)
	if err != nil {
		return dspErr.NewWithError(dspErr.INVALID_ADDRESS, err)
	}

	success, err := ch_actor.DirectTransferAsync(common.Address(target), common.TokenAmount(amount), common.PaymentID(paymentId))
	log.Debugf("media transfer success: %t, err: %s", success, err)
	if success && err == nil {
		return nil
	}
	resp, err := ch_actor.GetPaymentResult(common.Address(target), common.PaymentID(paymentId))
	if err != nil {
		if resp != nil {
			return dspErr.New(dspErr.CHANNEL_MEDIA_TRANSFER_TIMEOUT, "media transfer timeout, getPaymentResult reason: %s, result: %t, err: %s", resp.Reason, resp.Result, err)
		}
		return dspErr.New(dspErr.CHANNEL_MEDIA_TRANSFER_TIMEOUT, "media transfer timeout, getPaymentResult err: %s", err)
	}
	if resp == nil {
		return dspErr.New(dspErr.CHANNEL_MEDIA_TRANSFER_TIMEOUT, "media transfer timeout, resp and err is both nil")
	}
	if resp.Result {
		log.Debugf("media transfer check success: %t", resp.Result)
		return nil
	}
	return dspErr.New(dspErr.CHANNEL_MEDIA_TRANSFER_TIMEOUT, "media transfer timeout, getPaymentResult reason: %s, result: %t", resp.Reason, resp.Result)
}

func (this *Channel) MediaTransfer(paymentId int32, amount uint64, media, to string) error {
	log.Debugf("[dsp-go-sdk-channel] MediaTransfer %s", to)
	this.lock.Lock()
	defer this.lock.Unlock()
	if this.State() != state.ModuleStateActive {
		return dspErr.New(dspErr.CHANNEL_SERVICE_NOT_START, "channel service is not start")
	}
	registryAddress := common.PaymentNetworkID(utils.MicroPayContractAddress)
	tokenAddress := common.TokenAddress(usdt.USDT_CONTRACT_ADDRESS)
	target, err := chaincomm.AddressFromBase58(to)
	if err != nil {
		return dspErr.NewWithError(dspErr.INVALID_ADDRESS, err)
	}
	mediaAddr, err := chaincomm.AddressFromBase58(media)
	if err != nil {
		return dspErr.NewWithError(dspErr.INVALID_ADDRESS, err)
	}
	success, err := ch_actor.MediaTransfer(registryAddress, tokenAddress, common.Address(mediaAddr), common.Address(target), common.TokenAmount(amount), common.PaymentID(paymentId))
	log.Debugf("media transfer success: %t, err: %s", success, err)
	if success && err == nil {
		return nil
	}
	resp, err := ch_actor.GetPaymentResult(common.Address(target), common.PaymentID(paymentId))
	if err != nil {
		if resp != nil {
			return fmt.Errorf("media transfer timeout, getPaymentResult reason: %s, result: %t, err: %s", resp.Reason, resp.Result, err)
		}
		return fmt.Errorf("media transfer timeout, getPaymentResult err: %s", err)
	}
	if resp == nil {
		return errors.New("media transfer timeout, resp and err is both nil")
	}
	if resp.Result {
		log.Debugf("media transfer check success: %t", resp.Result)
		return nil
	}
	return fmt.Errorf("media transfer timeout, getPaymentResult reason: %s, result: %t", resp.Reason, resp.Result)

}

// GetTargetBalance. check total deposit balance
func (this *Channel) GetTotalDepositBalance(targetAddress string) (uint64, error) {
	log.Debugf("[dsp-go-sdk-channel] GetTotalDepositBalance %s", targetAddress)
	if this.State() != state.ModuleStateActive {
		return 0, dspErr.New(dspErr.CHANNEL_SERVICE_NOT_START, "channel service is not start")
	}
	partner, err := chaincomm.AddressFromBase58(targetAddress)
	if err != nil {
		return 0, dspErr.NewWithError(dspErr.INVALID_ADDRESS, err)
	}
	bal, err := ch_actor.GetTotalDepositBalance(common.Address(partner))
	if err != nil {
		return 0, dspErr.NewWithError(dspErr.CHANNEL_INTERNAL_ERROR, err)
	}
	return bal, nil
}

// GetAvaliableBalance. get avaliable balance
func (this *Channel) GetAvailableBalance(partnerAddress string) (uint64, error) {
	log.Debugf("[dsp-go-sdk-channel] GetAvailableBalance %s", partnerAddress)
	if this.State() != state.ModuleStateActive {
		return 0, dspErr.New(dspErr.CHANNEL_SERVICE_NOT_START, "channel service is not start")
	}
	partner, err := chaincomm.AddressFromBase58(partnerAddress)
	if err != nil {
		return 0, dspErr.NewWithError(dspErr.INVALID_ADDRESS, err)
	}
	bal, err := ch_actor.GetAvailableBalance(common.Address(partner))
	if err != nil {
		return 0, dspErr.NewWithError(dspErr.CHANNEL_INTERNAL_ERROR, err)
	}
	return bal, nil
}

func (this *Channel) GetNextNonce(partnerAddress string) uint64 {
	if this.State() != state.ModuleStateActive {
		return 0
	}
	partner, err := chaincomm.AddressFromBase58(partnerAddress)
	if err != nil {
		return 0
	}
	if this.chActor == nil || this.chActor.GetChannelService() == nil ||
		this.chActor.GetChannelService().Service == nil {
		return 0
	}
	chainState := this.chActor.GetChannelService().Service.StateFromChannel()
	microAddress := common.Address(utils.MicroPayContractAddress)
	token := common.TokenAddress(usdt.USDT_CONTRACT_ADDRESS)
	paymentNetworkID := common.PaymentNetworkID(microAddress)
	channelState := transfer.GetChannelStateFor(chainState, paymentNetworkID, token, common.Address(partner))
	if channelState == nil {
		log.Warnf("channel state is nil")
		return 0
	}
	senderState := channelState.GetChannelEndState(0)
	if senderState == nil {
		log.Warnf("senderState is nil")
		return 0
	}
	if senderState.BalanceProof == nil {
		log.Warnf("senderState balanceProof is nil")
		return 0
	}
	return uint64(senderState.BalanceProof.Nonce)
}

func (this *Channel) GetTotalWithdraw(partnerAddress string) (uint64, error) {
	if this.State() != state.ModuleStateActive {
		return 0, dspErr.New(dspErr.CHANNEL_SERVICE_NOT_START, "channel service is not start")
	}
	log.Debugf("[dsp-go-sdk-channel] GetTotalWithdraw %s", partnerAddress)
	partner, err := chaincomm.AddressFromBase58(partnerAddress)
	if err != nil {
		return 0, dspErr.NewWithError(dspErr.INVALID_ADDRESS, err)
	}
	bal, err := ch_actor.GetTotalWithdraw(common.Address(partner))
	if err != nil {
		return 0, dspErr.NewWithError(dspErr.CHANNEL_INTERNAL_ERROR, err)
	}
	return bal, nil
}

func (this *Channel) GetCurrentBalance(partnerAddress string) (uint64, error) {
	if this.State() != state.ModuleStateActive {
		return 0, dspErr.New(dspErr.CHANNEL_SERVICE_NOT_START, "channel service is not start")
	}
	log.Debugf("[dsp-go-sdk-channel] GetCurrentBalance %s", partnerAddress)
	partner, err := chaincomm.AddressFromBase58(partnerAddress)
	if err != nil {
		return 0, dspErr.NewWithError(dspErr.INVALID_ADDRESS, err)
	}
	bal, err := ch_actor.GetCurrentBalance(common.Address(partner))
	if err != nil {
		return 0, dspErr.NewWithError(dspErr.CHANNEL_INTERNAL_ERROR, err)
	}
	return bal, nil
}

// Withdraw. withdraw balance with target address
func (this *Channel) Withdraw(targetAddress string, amount uint64) (bool, error) {
	log.Debugf("[dsp-go-sdk-channel] Withdraw %s", targetAddress)
	if this.State() != state.ModuleStateActive {
		return false, dspErr.New(dspErr.CHANNEL_SERVICE_NOT_START, "channel service is not start")
	}
	token := common.TokenAddress(usdt.USDT_CONTRACT_ADDRESS)
	target, err := chaincomm.AddressFromBase58(targetAddress)
	if err != nil {
		return false, dspErr.NewWithError(dspErr.INVALID_ADDRESS, err)
	}
	withdrawAmount := common.TokenAmount(amount)
	success, err := ch_actor.WithDraw(token, common.Address(target), withdrawAmount)
	if err != nil {
		return false, dspErr.NewWithError(dspErr.CHANNEL_INTERNAL_ERROR, err)
	}
	return success, nil
}

// CooperativeSettle. settle channel cooperatively
func (this *Channel) CooperativeSettle(targetAddress string) error {
	log.Debugf("[dsp-go-sdk-channel] CooperativeSettle %s", targetAddress)
	if this.State() != state.ModuleStateActive {
		return dspErr.New(dspErr.CHANNEL_SERVICE_NOT_START, "channel service is not start")
	}
	target, err := chaincomm.AddressFromBase58(targetAddress)
	if err != nil {
		return dspErr.NewWithError(dspErr.INVALID_ADDRESS, err)
	}
	err = ch_actor.CooperativeSettle(common.Address(target))
	if err != nil {
		return dspErr.NewWithError(dspErr.CHANNEL_INTERNAL_ERROR, err)
	}
	return nil
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
		return 0, dspErr.New(dspErr.CHANNEL_INTERNAL_ERROR, "no unit prices")
	}
	p, ok := this.unitPrices[asset]
	if !ok {
		return 0, dspErr.New(dspErr.CHANNEL_INTERNAL_ERROR, "no unit prices")
	}
	return p, nil
}

func (this *Channel) CleanUnitPrices(asset int32) {
	if this.unitPrices == nil {
		return
	}
	delete(this.unitPrices, asset)
}

func (this *Channel) ChannelExist(walletAddr string) bool {
	if this.State() != state.ModuleStateActive {
		return false
	}
	all, _ := ch_actor.GetAllChannels()
	if all == nil {
		return false
	}
	for _, ch := range all.Channels {
		if ch.Address == walletAddr {
			return true
		}
	}
	return false
}

func (this *Channel) GetChannelInfo(walletAddr string) (*ch_actor.ChannelInfo, error) {
	if this.State() != state.ModuleStateActive {
		return nil, nil
	}
	all, _ := ch_actor.GetAllChannels()
	if all == nil {
		return nil, dspErr.New(dspErr.CHANNEL_INTERNAL_ERROR, "channel is nil")
	}

	for _, ch := range all.Channels {
		if ch.Address == walletAddr {
			return ch, nil
		}
	}
	return nil, dspErr.New(dspErr.CHANNEL_NOT_EXIST, "channel not exists")
}

func (this *Channel) AllChannels() (*ch_actor.ChannelsInfoResp, error) {
	log.Debugf("[dsp-go-sdk-channel] AllChannels")
	if this.State() != state.ModuleStateActive {
		return nil, nil
	}
	resp, err := ch_actor.GetAllChannels()
	if err != nil {
		return nil, dspErr.NewWithError(dspErr.CHANNEL_INTERNAL_ERROR, err)
	}
	return resp, nil
}

// SelectDNSChannel. select a dns channel and update it's record timestamp
func (this *Channel) SelectDNSChannel(dnsWalletAddr string) error {
	log.Debugf("select dns wallet %s", dnsWalletAddr)
	err := this.channelDB.SelectChannel(dnsWalletAddr)
	if err != nil {
		return dspErr.NewWithError(dspErr.CHANNEL_INTERNAL_ERROR, err)
	}
	return nil
}

func (this *Channel) GetLastUsedDNSWalletAddr() (string, error) {
	walletAddr, err := this.channelDB.GetLastUsedDNSChannel()
	if err != nil {
		return "", dspErr.NewWithError(dspErr.CHANNEL_INTERNAL_ERROR, err)
	}
	return walletAddr, nil
}
