package channel

import (
	"errors"
	"fmt"
	"time"

	dspcom "github.com/oniio/dsp-go-sdk/common"
	"github.com/oniio/dsp-go-sdk/config"
	sdk "github.com/oniio/oniChain-go-sdk"
	"github.com/oniio/oniChain-go-sdk/ong"
	chaincomm "github.com/oniio/oniChain/common"
	"github.com/oniio/oniChain/smartcontract/service/native/utils"
	ch "github.com/oniio/oniChannel"
	"github.com/oniio/oniChannel/common"
	"github.com/oniio/oniChannel/transfer"
)

type Channel struct {
	channel *ch.Channel
	closeCh chan struct{}
}

func NewChannelService(cfg *config.DspConfig, chain *sdk.Chain) *Channel {
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
		return nil
	}
	return &Channel{
		channel: channel,
		closeCh: make(chan struct{}, 1),
	}
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

// WaitForConnected. wait for conected for a period.
func (this *Channel) WaitForConnected(walletAddr string, timeout time.Duration) error {
	target, _ := chaincomm.AddressFromBase58(walletAddr)
	interval := time.Duration(dspcom.CHECK_CHANNEL_STATE_INTERVAL) * time.Second
	secs := int(timeout / interval)
	if secs <= 0 {
		secs = 1
	}
	for i := 0; i < secs; i++ {
		state := transfer.GetNodeNetworkStatus(this.channel.Service.StateFromChannel(), common.Address(target))
		if state == transfer.NetworkReachable {
			return nil
		}
		<-time.After(interval)
	}
	return errors.New("wait for connected timeout")
}

// OpenChannel. open channel for target of token.
func (this *Channel) OpenChannel(targetAddress string) (common.ChannelID, error) {
	token := common.TokenAddress(ong.ONG_CONTRACT_ADDRESS)
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

// SetDeposit. deposit money to target
func (this *Channel) SetDeposit(targetAddress string, amount uint64) error {
	token := common.TokenAddress(ong.ONG_CONTRACT_ADDRESS)
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
func (this *Channel) DirectTransfer(paymentId, amount uint64, to string) error {
	target, err := chaincomm.AddressFromBase58(to)
	if err != nil {
		return err
	}
	status, err := this.channel.Service.DirectTransferAsync(common.TokenAmount(amount), common.Address(target), common.PaymentID(paymentId))
	if err != nil {
		return err
	}
	ret := <-status
	if ret {
		return nil
	}
	return errors.New("payment failed")
}

// GetTargetBalance. check target deposit balance
func (this *Channel) GetTargetBalance(targetAddress string) (uint64, error) {
	target, err := chaincomm.AddressFromBase58(targetAddress)
	if err != nil {
		return 0, err
	}
	chainState := this.channel.Service.StateFromChannel()
	channelState := transfer.GetChannelStateFor(chainState, common.PaymentNetworkID(common.Address(utils.MicroPayContractAddress)),
		common.TokenAddress(ong.ONG_CONTRACT_ADDRESS), common.Address(target))
	if channelState == nil {
		return 0, errors.New("channel state is nil")
	}
	state := channelState.GetChannelEndState(0)
	return uint64(state.GetContractBalance()), nil
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
			fmt.Printf("PaymentReceive amount %d from %s with paymentID %d\n",
				event.Amount, addr.ToBase58(), event.Identifier)
		case <-this.closeCh:
			return
		}
	}
}
