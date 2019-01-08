package channel

import (
	"encoding/hex"
	"fmt"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/oniio/dsp-go-sdk/chain/client"
	"github.com/oniio/dsp-go-sdk/chain/wallet"
	"github.com/oniio/oniChain/common"
	"github.com/stretchr/testify/assert"
)

var walletPath = "./wallet.dat"
var pwd = []byte("pwd")
var rpc_addr = "http://127.0.0.1:20336"
var PARTICIPANT2_WALLETADDR = "AWcxaqe5chY3gSyUcBgdEgQrm7Ntr15qrZ" // Channel participant2 address base58 format
var testChannel *Channel

func init() {
	var err error
	w, err := wallet.OpenWallet(walletPath)
	if err != nil {
		fmt.Printf("Account.Open error:%s\n", err)
	}
	acc, err := w.GetDefaultAccount(pwd)
	if err != nil {
		fmt.Printf("GetDefaultAccount error:%s\n", err)
	}
	testChannel := &Channel{}
	testChannel.Client = &client.ClientMgr{}
	testChannel.Client.NewRpcClient().SetAddress(rpc_addr)
	testChannel.DefAcc = acc
}
func TestRegisterPaymentEndPoint(t *testing.T) {
	ip := net.ParseIP("127.0.0.1")
	port := []byte("8089")
	tx, err := testChannel.RegisterPaymentEndPoint(ip, port, testChannel.DefAcc.Address)
	assert.Nil(t, err)
	fmt.Printf("tx :%s\n", hex.EncodeToString(common.ToArrayReverse(tx)))
}

func TestFindEndpointByAddress(t *testing.T) {
	addr, _ := common.AddressFromBase58("APjuJnmakqdidJxf8mBwGThEUbAFiTU7uj")
	nodeInfo, err := testChannel.GetEndpointByAddress(addr)
	assert.Nil(t, err)
	fmt.Printf("addr:%s endpoint:  %s:%s\n", nodeInfo.WalletAddr.ToBase58(), net.IP(nodeInfo.IP), nodeInfo.Port)
}

func TestOpenChannel(t *testing.T) {
	wallet2Addr, err := common.AddressFromBase58(PARTICIPANT2_WALLETADDR)
	h, err := testChannel.Client.GetCurrentBlockHeight()
	assert.Nil(t, err)
	height, err := strconv.ParseUint(string(h), 10, 64)
	fmt.Printf("height: %d\n", height)
	assert.Nil(t, err)
	tx, err := testChannel.OpenChannel(testChannel.DefAcc.Address, wallet2Addr, height)
	assert.Nil(t, err)
	fmt.Printf("tx :%s\n", hex.EncodeToString(common.ToArrayReverse(tx)))
	confirmed, err := testChannel.Client.PollForTxConfirmed(time.Duration(60)*time.Second, tx)
	assert.Nil(t, err)
	assert.True(t, confirmed)
}

func TestSetTotalDeposit(t *testing.T) {

	wallet2Addr, err := common.AddressFromBase58(PARTICIPANT2_WALLETADDR)
	h, err := testChannel.Client.GetCurrentBlockHeight()
	assert.Nil(t, err)
	height, err := strconv.ParseUint(string(h), 10, 64)
	fmt.Printf("height: %d\n", height)
	assert.Nil(t, err)
	tx, err := testChannel.SetTotalDeposit(104, testChannel.DefAcc.Address, wallet2Addr, 25)
	assert.Nil(t, err)
	fmt.Printf("tx :%s\n", hex.EncodeToString(common.ToArrayReverse(tx)))
	confirmed, err := testChannel.Client.PollForTxConfirmed(time.Duration(60)*time.Second, tx)
	assert.Nil(t, err)
	assert.True(t, confirmed)
}

func TestGetChannelId(t *testing.T) {

	wallet2Addr, err := common.AddressFromBase58(PARTICIPANT2_WALLETADDR)
	assert.Nil(t, err)
	id, err := testChannel.GetChannelIdentifier(wallet2Addr, testChannel.DefAcc.Address)
	assert.Nil(t, err)
	fmt.Printf("id:%d\n", id)
}

func TestGetChannelInfo(t *testing.T) {
	wallet2Addr, err := common.AddressFromBase58(PARTICIPANT2_WALLETADDR)
	assert.Nil(t, err)
	info, err := testChannel.GetChannelInfo(104, testChannel.DefAcc.Address, wallet2Addr)
	assert.Nil(t, err)
	fmt.Printf("SettleBlockHeight :%v\n", info.SettleBlockHeight)
	fmt.Printf("ChannelState :%v\n", info.ChannelState)
}

func TestGetChannelParticipantInfo(t *testing.T) {
	wallet2Addr, err := common.AddressFromBase58(PARTICIPANT2_WALLETADDR)
	assert.Nil(t, err)
	p, err := testChannel.GetChannelParticipantInfo(104, testChannel.DefAcc.Address, wallet2Addr)
	assert.Nil(t, err)
	fmt.Printf("WalletAddr :%v\n", p.WalletAddr.ToBase58())
	fmt.Printf("Deposit :%v\n", p.Deposit)
	fmt.Printf("WithDrawAmount :%v\n", p.WithDrawAmount)
	fmt.Printf("IP :%v\n", p.IP)
	fmt.Printf("Port :%v\n", p.Port)
	fmt.Printf("Balance :%v\n", p.Balance)
	fmt.Printf("BalanceHash :%v\n", p.BalanceHash)
	fmt.Printf("Nonce :%v\n", p.Nonce)
	fmt.Printf("IsCloser :%v\n", p.IsCloser)
}

func TestRegisterSecret(t *testing.T) {
	secret := "hello world117"
	tx, err := testChannel.RegisterSecret([]byte(secret))
	assert.Nil(t, err)
	fmt.Printf("tx :%s\n", hex.EncodeToString(common.ToArrayReverse(tx)))
	confirmed, err := testChannel.Client.PollForTxConfirmed(time.Duration(60)*time.Second, tx)
	fmt.Printf("confirmed:%t\n", confirmed)
	assert.Nil(t, err)
	assert.True(t, confirmed)
}

func TestGetSecretRevealBlockHeight(t *testing.T) {
	secretHash, err := hex.DecodeString("6e13cc69a7ec5fbd77f98f6e2f2c017a58e48aec55ae93a3428b884ab7068c29")
	assert.Nil(t, err)
	height, err := testChannel.GetSecretRevealBlockHeight(secretHash)
	assert.Nil(t, err)
	fmt.Printf("height :%d\n", height)
}

func TestGetCurrentHeight(t *testing.T) {
	h, _ := testChannel.Client.GetCurrentBlockHeight()
	height, _ := strconv.ParseUint(string(h), 10, 64)
	fmt.Printf("h:%d\n", height)
}

func TestFilter(t *testing.T) {
	// client := NewRpcClient("http://127.0.0.1:20336")
	// ret, _ := client.GetFilterArgsForAllEventsFromChannel(0, 7373, 7375)
	// fmt.Printf("ret:%v\n", ret)
}
