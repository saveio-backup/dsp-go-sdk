package channel

import (
	"errors"
	"math/rand"
	"strings"
	"sync"
	"time"

	"github.com/saveio/dsp-go-sdk/actor/client"
	"github.com/saveio/dsp-go-sdk/consts"
	"github.com/saveio/dsp-go-sdk/store"

	"github.com/ontio/ontology-eventbus/actor"
	sdkErr "github.com/saveio/dsp-go-sdk/error"
	"github.com/saveio/dsp-go-sdk/types/state"
	osUtils "github.com/saveio/dsp-go-sdk/utils/os"
	ch "github.com/saveio/pylons"
	ch_actor "github.com/saveio/pylons/actor/server"
	"github.com/saveio/pylons/common"
	pylonsCom "github.com/saveio/pylons/common"
	"github.com/saveio/pylons/transfer"
	themisSdk "github.com/saveio/themis-go-sdk"
	"github.com/saveio/themis-go-sdk/usdt"
	chainCom "github.com/saveio/themis/common"
	"github.com/saveio/themis/common/log"
	"github.com/saveio/themis/smartcontract/service/native/utils"
)

type Channel struct {
	opts    *InitChannelOptions
	db      *store.ChannelDB
	chActor *ch_actor.ChannelActorServer // pylons actor server
	closeCh chan struct{}                // channel service close notify
	chain   *themisSdk.Chain             // chain SDK
	random  *rand.Rand                   // random
	lock    *sync.Mutex                  // mutext for operation
	state   *state.SyncState             // channel module state
}

// NewChannelService. init channel service with chain SDK and other options
// options including chain client type, reveal timeout, setting timeout, DBPath, block delay
func NewChannelService(chain *themisSdk.Chain, opts ...ChannelOpt) (*Channel, error) {
	var rpcAddrs []string
	if chain != nil && chain.GetRpcClient() != nil {
		rpcAddrs = chain.GetRpcClient().GetAddress()
	}
	initChannelOpt := DefaultInitChannelOptions()
	for _, o := range opts {
		o.apply(initChannelOpt)
	}
	var channelConfig = &ch.ChannelConfig{
		ClientType:    initChannelOpt.ClientType,
		ChainNodeURLs: rpcAddrs,
		RevealTimeout: initChannelOpt.RevealTimeout,
		DBPath:        initChannelOpt.DBPath,
		SettleTimeout: initChannelOpt.SettleTimeout,
		BlockDelay:    initChannelOpt.BlockDelay,
	}
	log.Debugf("pylons cfg: %v", channelConfig)
	err := osUtils.CreateDirIfNotExist(initChannelOpt.DBPath)
	if err != nil {
		return nil, sdkErr.NewWithError(sdkErr.CHANNEL_CREATE_DB_ERROR, err)
	}
	//start channel and actor
	channelActor, err := ch_actor.NewChannelActor(channelConfig, chain.Native.Channel.DefAcc)
	if err != nil {
		log.Debugf("channelActor+++ %s", err)
		return nil, sdkErr.NewWithError(sdkErr.CHANNEL_CREATE_ACTOR_ERROR, err)
	}
	log.Debugf("channel actor %v, pid %v", channelActor, channelActor.GetLocalPID())
	return &Channel{
		opts:    initChannelOpt,
		db:      initChannelOpt.DB,
		chActor: channelActor,
		closeCh: make(chan struct{}, 1),
		chain:   chain,
		random:  rand.New(rand.NewSource(time.Now().UnixNano())),
		lock:    new(sync.Mutex),
		state:   state.NewSyncState(),
	}, nil
}

// GetChannelPid. get channel actor server PID
func (this *Channel) GetChannelPid() *actor.PID {
	if this.chActor == nil {
		return nil
	}
	return this.chActor.GetLocalPID()
}

// StartService. start channel service
func (this *Channel) StartService() error {
	log.Debugf("start channel service, start syncing block")
	if _, ok := this.state.GetOrStore(state.ModuleStateStarting); ok {
		return sdkErr.New(sdkErr.CHANNEL_START_INSTANCE_ERROR, "channel service is starting")
	}
	err := this.chActor.SyncBlockData()
	log.Debugf("channel service syncing block done")
	if err != nil {
		this.state.Set(state.ModuleStateError)
		log.Errorf("channel sync block err %s", err)
		return sdkErr.NewWithError(sdkErr.CHANNEL_SYNC_BLOCK_ERROR, err)
	}
	defer func() {
		if e := recover(); e != nil {
			log.Errorf("send panic recover err %v", e)
		}
	}()
	log.Debugf("starting pylons service")
	err = ch_actor.StartPylons()
	log.Debugf("start pylons done")
	go func() {
		log.Debugf("starting get fee schedule")
		fee, err := ch_actor.GetFee(0, true)
		if err != nil {
			log.Errorf("get fee schedule error: %s", err)
			return
		}
		err = ch_actor.SetFee(fee, false)
		if err != nil {
			log.Errorf("set fee schedule error: %s", err)
			return
		}
		log.Debugf("get fee schedule done")
	}()
	this.state.Set(state.ModuleStateStarted)
	if err != nil {
		this.state.Set(state.ModuleStateError)
		return sdkErr.NewWithError(sdkErr.CHANNEL_START_INSTANCE_ERROR, err)
	}
	this.state.Set(state.ModuleStateActive)
	return nil
}

// StopService. stop channel service
func (this *Channel) StopService() {
	if this.State() != state.ModuleStateActive {
		// if not start, there is no transport to receive for channel service
		this.state.Set(state.ModuleStateStopping)
		if this.chActor == nil {
			this.state.Set(state.ModuleStateStopped)
			return
		}
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
	log.Debug("dsp core channel StopService")
	err := ch_actor.StopPylons()
	log.Infof("dsp core channel stopped")
	if err != nil {
		this.state.Set(state.ModuleStateError)
		log.Errorf("stop pylons err %s", err)
		return
	}
	if this.chActor != nil && this.chActor.GetLocalPID() != nil {
		this.chActor.GetLocalPID().Stop()
	}
	close(this.closeCh)
	this.state.Set(state.ModuleStateStopped)
}

// State. get channel module state.
func (this *Channel) State() state.ModuleState {
	if this.state == nil {
		return state.ModuleStateNone
	}
	return this.state.Get()
}

// SyncingBlock. return if channel is syncing block right now.
func (this *Channel) SyncingBlock() bool {
	if this.state == nil {
		return true
	}
	return this.state.Get() == state.ModuleStateNone ||
		this.state.Get() == state.ModuleStateStarting ||
		this.state.Get() == state.ModuleStateStarted
}

// Active. return if channel module is active right now.
func (this *Channel) Active() bool {
	if this.state == nil {
		return false
	}
	return this.state.Get() == state.ModuleStateActive
}

// GetCurrentFilterBlockHeight. get current filter block height
func (this *Channel) GetCurrentFilterBlockHeight() uint32 {
	height, err := ch_actor.GetLastFilterBlockHeight()
	if err != nil {
		log.Errorf("get last filter block height err %s", err)
	}
	return height
}

func (this *Channel) GetCloseCh() chan struct{} {
	return this.closeCh
}

// GetAllPartners. get all partners from local db
func (this *Channel) GetAllPartners() ([]string, error) {
	if this.State() != state.ModuleStateActive {
		return nil, sdkErr.New(sdkErr.CHANNEL_SERVICE_NOT_START, "channel service is not start")
	}
	newPartners := make([]string, 0)
	neighbors := transfer.GetNeighbours(this.chActor.GetChannelService().Service.StateFromChannel())
	for _, v := range neighbors {
		newPartners = append(newPartners, pylonsCom.ToBase58(v))
	}
	log.Debugf("get partners from channel service %v", newPartners)
	return newPartners, nil
}

// WaitForConnected. wait for connected for a period.
func (this *Channel) WaitForConnected(walletAddr string, timeout time.Duration) error {
	log.Debugf("check channel connection reachable of wallet %s with timeout %d", walletAddr, timeout)
	interval := time.Duration(consts.CHECK_CHANNEL_STATE_INTERVAL) * time.Second
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
	return sdkErr.New(sdkErr.NETWORK_TIMEOUT, "wait for connected %s timeout", walletAddr)
}

// ChannelReachale. is channel open and reachable
func (this *Channel) ChannelReachale(walletAddr string) bool {
	if this.State() != state.ModuleStateActive {
		return false
	}
	target, _ := chainCom.AddressFromBase58(walletAddr)
	reachable, _ := ch_actor.ChannelReachable(pylonsCom.Address(target))
	log.Debugf("check channel connection reachable of wallet %s, reachable: %t", walletAddr, reachable)
	return reachable
}

// HealthyCheckNodeState. health check node network state with wallet address
func (this *Channel) HealthyCheckNodeState(walletAddr string) error {
	if this.State() != state.ModuleStateActive {
		return sdkErr.New(sdkErr.CHANNEL_SERVICE_NOT_START, "channel service is not start")
	}
	log.Debugf("start healthy check node state of wallet %s", walletAddr)
	target, err := chainCom.AddressFromBase58(walletAddr)
	if err != nil {
		return sdkErr.NewWithError(sdkErr.INVALID_ADDRESS, err)
	}
	err = ch_actor.HealthyCheckNodeState(pylonsCom.Address(target))
	if err != nil {
		return sdkErr.NewWithError(sdkErr.NETWORK_CHECK_CONN_STATE_ERROR, err)
	}
	return nil
}

// OpenChannel. open channel for target of token.
func (this *Channel) OpenChannel(targetAddress string, depositAmount uint64) (pylonsCom.ChannelID, error) {
	log.Infof("open channel with target %s, deposit amount %d", targetAddress, depositAmount)
	if this.State() != state.ModuleStateActive {
		return 0, sdkErr.New(sdkErr.CHANNEL_SERVICE_NOT_START, "channel service is not start")
	}
	token := pylonsCom.TokenAddress(usdt.USDT_CONTRACT_ADDRESS)
	target, err := chainCom.AddressFromBase58(targetAddress)
	if err != nil {
		return 0, sdkErr.NewWithError(sdkErr.INVALID_ADDRESS, err)
	}
	channelID, err := ch_actor.OpenChannel(token, pylonsCom.Address(target))
	if err != nil {
		log.Errorf("open channel with %s err %s", targetAddress, err)
		return 0, sdkErr.NewWithError(sdkErr.CHANNEL_OPEN_FAILED, err)
	}
	if channelID == 0 {
		log.Errorf("open channel with %s but channel id is 0", targetAddress)
		return 0, sdkErr.New(sdkErr.CHANNEL_OPEN_FAILED, "setup channel failed")
	}
	if depositAmount == 0 {
		this.healthCheck(targetAddress)
		return channelID, nil
	}
	bal, _ := this.GetTotalDepositBalance(targetAddress)
	if bal >= depositAmount {
		log.Debugf("open channel to %s balance insufficient, current balance is %d", targetAddress, bal)
		this.healthCheck(targetAddress)
		return channelID, nil
	}
	err = this.SetDeposit(targetAddress, depositAmount)
	if err != nil && strings.Index(err.Error(), "totalDeposit must big than contractBalance") == -1 {
		log.Errorf("deposit channel with target %s failed result %s", targetAddress, err)
		// TODO: withdraw and close channel
		return 0, sdkErr.NewWithError(sdkErr.CHANNEL_DEPOSIT_FAILED, err)
	}
	this.healthCheck(targetAddress)
	return channelID, nil
}

// CloseChannel. close channel with target
func (this *Channel) CloseChannel(targetAddress string) error {
	log.Debugf("close channel with target %s", targetAddress)
	if this.State() != state.ModuleStateActive {
		return sdkErr.New(sdkErr.CHANNEL_SERVICE_NOT_START, "channel service is not start")
	}
	target, err := chainCom.AddressFromBase58(targetAddress)
	if err != nil {
		return sdkErr.NewWithError(sdkErr.INVALID_ADDRESS, err)
	}
	success, err := ch_actor.CloseChannel(pylonsCom.Address(target))
	if err != nil {
		return sdkErr.NewWithError(sdkErr.CHANNEL_CLOSE_FAILED, err)
	}
	if !success {
		return sdkErr.New(sdkErr.CHANNEL_CLOSE_FAILED, "close channel done but no success")
	}
	return nil
}

// SetDeposit. deposit money to target
func (this *Channel) SetDeposit(targetAddress string, amount uint64) error {
	log.Debugf("set channel deposit with target %s, amount %d", targetAddress, amount)
	if amount == 0 {
		return nil
	}
	if this.State() != state.ModuleStateActive {
		return sdkErr.New(sdkErr.CHANNEL_SERVICE_NOT_START, "channel service is not start")
	}
	token := pylonsCom.TokenAddress(usdt.USDT_CONTRACT_ADDRESS)
	target, err := chainCom.AddressFromBase58(targetAddress)
	if err != nil {
		return sdkErr.NewWithError(sdkErr.INVALID_ADDRESS, err)
	}
	depositAmount := pylonsCom.TokenAmount(amount)
	err = ch_actor.SetTotalChannelDeposit(token, pylonsCom.Address(target), depositAmount)
	if err != nil {
		return sdkErr.NewWithError(sdkErr.CHANNEL_DEPOSIT_FAILED, err)
	}
	return nil
}

// CanTransfer. check if can transfer to
func (this *Channel) CanTransfer(to string, amount uint64) error {
	log.Debugf("check if can transfer to %s with amount %d", to, amount)
	if this.State() != state.ModuleStateActive {
		return sdkErr.New(sdkErr.CHANNEL_SERVICE_NOT_START, "channel service is not start")
	}
	target, err := chainCom.AddressFromBase58(to)
	if err != nil {
		return sdkErr.NewWithError(sdkErr.INVALID_ADDRESS, err)
	}
	interval := time.Duration(consts.CHECK_CHANNEL_CAN_TRANSFER_INTERVAL) * time.Second
	secs := int(consts.CHECK_CHANNEL_CAN_TRANSFER_TIMEOUT / interval)
	if secs <= 0 {
		secs = 1
	}
	for i := 0; i < secs; i++ {
		_, err := ch_actor.CanTransfer(pylonsCom.Address(target), pylonsCom.TokenAmount(amount))
		if err == nil {
			return nil
		} else {
			log.Errorf("can't transfer to %s, err %s", to, err)
		}
		<-time.After(interval)
	}
	return sdkErr.New(sdkErr.CHANNEL_CHECK_TIMEOUT, "check can transfer to %s with amount %d timeout", to, amount)
}

// NewPaymentId. generate a new payment id for transfer
func (this *Channel) NewPaymentId() int32 {
	return this.random.Int31()
}

// DirectTransfer. direct transfer to with payment id, and amount
func (this *Channel) DirectTransfer(paymentId int32, amount uint64, to string) error {
	if paymentId == 0 {
		paymentId = this.NewPaymentId()
	}
	log.Debugf("direct transfer to %s payment id %d amount %d", to, paymentId, amount)
	if this.State() != state.ModuleStateActive {
		return sdkErr.New(sdkErr.CHANNEL_SERVICE_NOT_START, "channel service is not start")
	}
	err := this.CanTransfer(to, amount)
	if err != nil {
		return err
	}
	target, err := chainCom.AddressFromBase58(to)
	if err != nil {
		return sdkErr.NewWithError(sdkErr.INVALID_ADDRESS, err)
	}
	success, tErr := ch_actor.DirectTransferAsync(pylonsCom.Address(target), pylonsCom.TokenAmount(amount),
		pylonsCom.PaymentID(paymentId))
	if success {
		log.Debugf("Payment id %d, direct transfer success", paymentId)
		return nil
	} else {
		// avoid null point exception
		if tErr == nil {
			tErr = errors.New("unknown error")
		}
		log.Errorf("payment id %d, direct transfer err: %s", paymentId, err)
		resp, err := ch_actor.GetPaymentResult(pylonsCom.Address(target), pylonsCom.PaymentID(paymentId))
		if err != nil {
			return sdkErr.New(sdkErr.CHANNEL_DIRECT_TRANSFER_TIMEOUT,
				"direct transfer timeout and get payment result timeout: %s", err)
		}
		if resp == nil {
			return sdkErr.New(sdkErr.CHANNEL_DIRECT_TRANSFER_ERROR,
				"direct transfer error: %s, and get payment id %d empty result", tErr.Error(), paymentId)
		}
		return sdkErr.New(sdkErr.CHANNEL_DIRECT_TRANSFER_ERROR,
			"Direct transfer error: %s, and get payment id %d result: %t, reson: %s", tErr.Error(), paymentId, resp.Result, resp.Reason)
	}
	return sdkErr.New(sdkErr.CHANNEL_DIRECT_TRANSFER_ERROR, "unknown error, payment id: %d", paymentId)
}

func (this *Channel) MediaTransfer(paymentId int32, amount uint64, media, to string) error {
	if paymentId == 0 {
		paymentId = this.NewPaymentId()
	}
	log.Debugf("media transfer to %s payment id %s amount %d with media %v", to, paymentId, amount, to)
	if this.State() != state.ModuleStateActive {
		return sdkErr.New(sdkErr.CHANNEL_SERVICE_NOT_START, "channel service is not start")
	}
	registryAddress := common.PaymentNetworkID(utils.MicroPayContractAddress)
	tokenAddress := common.TokenAddress(usdt.USDT_CONTRACT_ADDRESS)
	target, err := chainCom.AddressFromBase58(to)
	if err != nil {
		return sdkErr.NewWithError(sdkErr.INVALID_ADDRESS, err)
	}
	mediaAddr, err := chainCom.AddressFromBase58(media)
	if err != nil {
		return sdkErr.NewWithError(sdkErr.INVALID_ADDRESS, err)
	}
	success, tErr := ch_actor.MediaTransfer(registryAddress, tokenAddress, pylonsCom.Address(mediaAddr),
		pylonsCom.Address(target), pylonsCom.TokenAmount(amount), pylonsCom.PaymentID(paymentId))
	if success {
		log.Debugf("payment %d media transfer success", paymentId)
		return nil
	} else {
		// avoid null point exception
		if tErr == nil {
			tErr = errors.New("unknown error")
		}
		resp, err := ch_actor.GetPaymentResult(pylonsCom.Address(target), pylonsCom.PaymentID(paymentId))
		if err != nil {
			return sdkErr.New(sdkErr.CHANNEL_MEDIA_TRANSFER_TIMEOUT,
				"media transfer timeout and get payment result timeout: %s", err)
		}
		if resp == nil {
			return sdkErr.New(sdkErr.CHANNEL_MEDIA_TRANSFER_ERROR,
				"media transfer error: %s, and get payment id %d empty result", tErr.Error(), paymentId)
		}
		return sdkErr.New(sdkErr.CHANNEL_MEDIA_TRANSFER_ERROR,
			"Media transfer error: %s, and get payment id %d result: %t, reson: %s", tErr.Error(), paymentId, resp.Result, resp.Reason)
	}
	return sdkErr.New(sdkErr.CHANNEL_MEDIA_TRANSFER_ERROR, "unknown error, payment id: %d", paymentId)
}

// GetTargetBalance. check total deposit balance
func (this *Channel) GetTotalDepositBalance(targetAddress string) (uint64, error) {
	log.Debugf("getting total balance of %s", targetAddress)
	if this.State() != state.ModuleStateActive {
		return 0, sdkErr.New(sdkErr.CHANNEL_SERVICE_NOT_START, "channel service is not start")
	}
	partner, err := chainCom.AddressFromBase58(targetAddress)
	if err != nil {
		return 0, sdkErr.NewWithError(sdkErr.INVALID_ADDRESS, err)
	}
	bal, err := ch_actor.GetTotalDepositBalance(pylonsCom.Address(partner))
	if err != nil {
		return 0, sdkErr.NewWithError(sdkErr.CHANNEL_GET_TOTAL_BALANCE_ERROR, err)
	}
	return bal, nil
}

// GetAvaliableBalance. get avaliable balance between target
func (this *Channel) GetAvailableBalance(targetAddress string) (uint64, error) {
	log.Debugf("getting available balance of %s", targetAddress)
	if this.State() != state.ModuleStateActive {
		return 0, sdkErr.New(sdkErr.CHANNEL_SERVICE_NOT_START, "channel service is not start")
	}
	partner, err := chainCom.AddressFromBase58(targetAddress)
	if err != nil {
		return 0, sdkErr.NewWithError(sdkErr.INVALID_ADDRESS, err)
	}
	bal, err := ch_actor.GetAvailableBalance(pylonsCom.Address(partner))
	if err != nil {
		return 0, sdkErr.NewWithError(sdkErr.CHANNEL_GET_AVAILABLE_BALANCE_ERROR, err)
	}
	return bal, nil
}

// GetTotalWithdraw. get total withdraw amount between target
func (this *Channel) GetTotalWithdraw(targetAddress string) (uint64, error) {
	if this.State() != state.ModuleStateActive {
		return 0, sdkErr.New(sdkErr.CHANNEL_SERVICE_NOT_START, "channel service is not start")
	}
	log.Debugf("getting total withdrawal of target %s", targetAddress)
	partner, err := chainCom.AddressFromBase58(targetAddress)
	if err != nil {
		return 0, sdkErr.NewWithError(sdkErr.INVALID_ADDRESS, err)
	}
	bal, err := ch_actor.GetTotalWithdraw(pylonsCom.Address(partner))
	if err != nil {
		return 0, sdkErr.NewWithError(sdkErr.CHANNEL_GET_TOTAL_WITHDRAWL_ERROR, err)
	}
	return bal, nil
}

// GetCurrentBalance. get current deposit balance between target
func (this *Channel) GetCurrentBalance(targetAddress string) (uint64, error) {
	if this.State() != state.ModuleStateActive {
		return 0, sdkErr.New(sdkErr.CHANNEL_SERVICE_NOT_START, "channel service is not start")
	}
	log.Debugf("get current deposit balance between target %s", targetAddress)
	partner, err := chainCom.AddressFromBase58(targetAddress)
	if err != nil {
		return 0, sdkErr.NewWithError(sdkErr.INVALID_ADDRESS, err)
	}
	bal, err := ch_actor.GetCurrentBalance(pylonsCom.Address(partner))
	if err != nil {
		return 0, sdkErr.NewWithError(sdkErr.CHANNEL_GET_CURRENT_BALANCE_ERROR, err)
	}
	return bal, nil
}

// Withdraw. withdraw balance with target address
//
// Args:
//
// targetAddress (string) target wallet base58 format string
//
// amount 		 (uint64) withdraw amount
//
// Error Code:
//
// 90009:   service not started
func (this *Channel) Withdraw(targetAddress string, amount uint64) (bool, error) {
	log.Debugf("withdraw balance with target %s with amount %d", targetAddress, amount)
	if this.State() != state.ModuleStateActive {
		return false, sdkErr.New(sdkErr.CHANNEL_SERVICE_NOT_START, "channel service is not start")
	}
	token := pylonsCom.TokenAddress(usdt.USDT_CONTRACT_ADDRESS)
	target, err := chainCom.AddressFromBase58(targetAddress)
	if err != nil {
		return false, sdkErr.NewWithError(sdkErr.INVALID_ADDRESS, err)
	}
	withdrawAmount := pylonsCom.TokenAmount(amount)
	success, err := ch_actor.WithDraw(token, pylonsCom.Address(target), withdrawAmount)
	if err != nil {
		return false, sdkErr.NewWithError(sdkErr.CHANNEL_WITHDRAW_FAILED, err)
	}
	return success, nil
}

// CooperativeSettle. settle channel cooperatively
func (this *Channel) CooperativeSettle(targetAddress string) error {
	log.Debugf("cooperative settle with target %s", targetAddress)
	if this.State() != state.ModuleStateActive {
		return sdkErr.New(sdkErr.CHANNEL_SERVICE_NOT_START, "channel service is not start")
	}
	target, err := chainCom.AddressFromBase58(targetAddress)
	if err != nil {
		return sdkErr.NewWithError(sdkErr.INVALID_ADDRESS, err)
	}
	err = ch_actor.CooperativeSettle(pylonsCom.Address(target))
	if err != nil {
		return sdkErr.NewWithError(sdkErr.CHANNEL_COOPERATIVE_SETTLE_ERROR, err)
	}
	return nil
}

// ChannelExist. check if channel is exist
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

// GetChannelInfo. get channel detail info
func (this *Channel) GetChannelInfo(walletAddr string) (*ch_actor.ChannelInfo, error) {
	if this.State() != state.ModuleStateActive {
		return nil, nil
	}
	all, _ := ch_actor.GetAllChannels()
	if all == nil {
		return nil, sdkErr.New(sdkErr.CHANNEL_INTERNAL_ERROR, "has no channels")
	}

	for _, ch := range all.Channels {
		if ch.Address == walletAddr {
			return ch, nil
		}
	}
	return nil, sdkErr.New(sdkErr.CHANNEL_NOT_EXIST, "channel between %s not exists", walletAddr)
}

// AllChannels. get all channels
func (this *Channel) AllChannels() (*ch_actor.ChannelsInfoResp, error) {
	log.Debugf("getting all channels")
	if this.State() != state.ModuleStateActive {
		return nil, nil
	}
	resp, err := ch_actor.GetAllChannels()
	if err != nil {
		return nil, sdkErr.NewWithError(sdkErr.CHANNEL_INTERNAL_ERROR, err)
	}
	return resp, nil
}

func (this *Channel) healthCheck(targetAddress string) error {
	if !this.opts.IsClient {
		return nil
	}
	wallet, err := chainCom.AddressFromBase58(targetAddress)
	if err != nil {
		log.Errorf("health check dns parse wallet address err %v", err)
		return err
	}
	info, _ := this.chain.Native.Dns.GetDnsNodeByAddr(wallet)
	if info == nil {
		return nil
	}
	client.P2PAppendAddrForHealthCheck(targetAddress, client.P2PNetTypeChannel)
	return nil
}

func (this *Channel) GetFee(channelID uint64) (*transfer.FeeScheduleState, error) {
	fee, err := ch_actor.GetFee(pylonsCom.ChannelID(channelID), false)
	if err != nil {
		log.Errorf("GetFee err %v", err)
		return nil, err
	}
	return fee, nil
}

func (this *Channel) SetFee(fee *transfer.FeeScheduleState) error {
	err := ch_actor.SetFee(fee, true)
	if err != nil {
		log.Errorf("SetFee err %v", err)
		return err
	}
	return nil
}

func (this *Channel) GetPenalty() (*pylonsCom.RoutePenaltyConfig, error) {
	penalty, err := ch_actor.GetPenalty()
	if err != nil {
		log.Errorf("GetPenalty err %v", err)
		return nil, err
	}
	return penalty, nil
}

func (this *Channel) SetPenalty(penalty *pylonsCom.RoutePenaltyConfig) error {
	err := ch_actor.SetPenalty(penalty)
	if err != nil {
		log.Errorf("SetPenalty err %v", err)
		return err
	}
	return nil
}
