package contract

import (
	"encoding/hex"
	"fmt"
	"net"
	"strconv"
	"testing"
	"time"

	chain "github.com/oniio/dsp-go-sdk/chain"
	"github.com/oniio/oniChain/common"
	"github.com/stretchr/testify/assert"
)

const (
	RPC_ADDRESS             = "http://127.0.0.1:20336"             // Ontology rpc address
	WALLET_PATH             = "./wallet.dat"                       // User wallet dat file path
	WALLET_PWD              = "pwd"                                // User wallet password
	PARTICIPANT2_WALLETADDR = "AWcxaqe5chY3gSyUcBgdEgQrm7Ntr15qrZ" // Channel participant2 address base58 format
	// PARTICIPANT2_WALLETADDR = "AN9V8bh2tr36Nrk51vS9kTp7EaZbJn61SH" // Channel participant2 address base58 format
)

func TestRegisterPaymentEndPoint(t *testing.T) {
	chainSdk := chain.NewChainSdk()
	wallet, err := chainSdk.OpenWallet(WALLET_PATH)
	assert.NotNil(t, wallet)
	assert.Nil(t, err)
	account, err := wallet.GetDefaultAccount([]byte(WALLET_PWD))
	assert.Nil(t, err)
	mp := InitMicroPayment(account, RPC_ADDRESS)
	assert.NotNil(t, mp)
	ip := net.ParseIP("127.0.0.1")
	port := []byte("8089")
	tx, err := mp.RegisterPaymentEndPoint(ip, port, account.Address)
	assert.Nil(t, err)
	fmt.Printf("tx :%s\n", hex.EncodeToString(common.ToArrayReverse(tx)))
}

func TestFindEndpointByAddress(t *testing.T) {
	chainSdk := chain.NewChainSdk()
	wallet, err := chainSdk.OpenWallet(WALLET_PATH)
	assert.NotNil(t, wallet)
	assert.Nil(t, err)
	account, err := wallet.GetDefaultAccount([]byte(WALLET_PWD))
	assert.Nil(t, err)
	mp := InitMicroPayment(account, RPC_ADDRESS)
	assert.NotNil(t, mp)
	addr, _ := common.AddressFromBase58("APjuJnmakqdidJxf8mBwGThEUbAFiTU7uj")
	nodeInfo, err := mp.GetEndpointByAddress(addr)
	assert.Nil(t, err)
	fmt.Printf("addr:%s endpoint:  %s:%s\n", nodeInfo.WalletAddr.ToBase58(), net.IP(nodeInfo.IP), nodeInfo.Port)
}

func TestOpenChannel(t *testing.T) {
	chainSdk := chain.NewChainSdk()
	wallet, err := chainSdk.OpenWallet(WALLET_PATH)
	assert.NotNil(t, wallet)
	assert.Nil(t, err)
	account, err := wallet.GetDefaultAccount([]byte(WALLET_PWD))
	assert.Nil(t, err)
	mp := InitMicroPayment(account, RPC_ADDRESS)
	assert.NotNil(t, mp)
	wallet2Addr, err := common.AddressFromBase58(PARTICIPANT2_WALLETADDR)
	h, err := chainSdk.GetCurrentBlockHeight()
	assert.Nil(t, err)
	height, err := strconv.ParseUint(string(h), 10, 64)
	fmt.Printf("height: %d\n", height)
	assert.Nil(t, err)
	tx, err := mp.OpenChannel(account.Address, wallet2Addr, height)
	assert.Nil(t, err)
	fmt.Printf("tx :%s\n", hex.EncodeToString(common.ToArrayReverse(tx)))
	confirmed, err := chainSdk.PollForTxConfirmed(time.Duration(60)*time.Second, tx)
	assert.Nil(t, err)
	assert.True(t, confirmed)
}

func TestSetTotalDeposit(t *testing.T) {
	chainSdk := chain.NewChainSdk()
	wallet, err := chainSdk.OpenWallet(WALLET_PATH)
	assert.NotNil(t, wallet)
	assert.Nil(t, err)
	account, err := wallet.GetDefaultAccount([]byte(WALLET_PWD))
	assert.Nil(t, err)
	mp := InitMicroPayment(account, RPC_ADDRESS)
	assert.NotNil(t, mp)
	wallet2Addr, err := common.AddressFromBase58(PARTICIPANT2_WALLETADDR)
	h, err := chainSdk.GetCurrentBlockHeight()
	assert.Nil(t, err)
	height, err := strconv.ParseUint(string(h), 10, 64)
	fmt.Printf("height: %d\n", height)
	assert.Nil(t, err)
	tx, err := mp.SetTotalDeposit(104, account.Address, wallet2Addr, 25)
	assert.Nil(t, err)
	fmt.Printf("tx :%s\n", hex.EncodeToString(common.ToArrayReverse(tx)))
	confirmed, err := chainSdk.PollForTxConfirmed(time.Duration(60)*time.Second, tx)
	assert.Nil(t, err)
	assert.True(t, confirmed)
}

func TestGetChannelId(t *testing.T) {
	chainSdk := chain.NewChainSdk()
	wallet, err := chainSdk.OpenWallet(WALLET_PATH)
	assert.NotNil(t, wallet)
	assert.Nil(t, err)
	account, err := wallet.GetDefaultAccount([]byte(WALLET_PWD))
	assert.Nil(t, err)
	mp := InitMicroPayment(account, RPC_ADDRESS)
	assert.NotNil(t, mp)
	wallet2Addr, err := common.AddressFromBase58(PARTICIPANT2_WALLETADDR)
	assert.Nil(t, err)
	id, err := mp.GetChannelIdentifier(wallet2Addr, account.Address)
	assert.Nil(t, err)
	fmt.Printf("id:%d\n", id)
}

func TestGetChannelInfo(t *testing.T) {
	chainSdk := chain.NewChainSdk()
	wallet, err := chainSdk.OpenWallet(WALLET_PATH)
	assert.NotNil(t, wallet)
	assert.Nil(t, err)
	account, err := wallet.GetDefaultAccount([]byte(WALLET_PWD))
	assert.Nil(t, err)
	mp := InitMicroPayment(account, RPC_ADDRESS)
	assert.NotNil(t, mp)
	wallet2Addr, err := common.AddressFromBase58(PARTICIPANT2_WALLETADDR)
	assert.Nil(t, err)
	info, err := mp.GetChannelInfo(104, account.Address, wallet2Addr)
	assert.Nil(t, err)
	fmt.Printf("SettleBlockHeight :%v\n", info.SettleBlockHeight)
	fmt.Printf("ChannelState :%v\n", info.ChannelState)
}

func TestGetChannelParticipantInfo(t *testing.T) {
	chainSdk := chain.NewChainSdk()
	wallet, err := chainSdk.OpenWallet(WALLET_PATH)
	assert.NotNil(t, wallet)
	assert.Nil(t, err)
	account, err := wallet.GetDefaultAccount([]byte(WALLET_PWD))
	assert.Nil(t, err)
	mp := InitMicroPayment(account, RPC_ADDRESS)
	assert.NotNil(t, mp)
	wallet2Addr, err := common.AddressFromBase58(PARTICIPANT2_WALLETADDR)
	assert.Nil(t, err)
	p, err := mp.GetChannelParticipantInfo(104, account.Address, wallet2Addr)
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
	chainSdk := chain.NewChainSdk()
	wallet, err := chainSdk.OpenWallet(WALLET_PATH)
	assert.NotNil(t, wallet)
	assert.Nil(t, err)
	account, err := wallet.GetDefaultAccount([]byte(WALLET_PWD))
	assert.Nil(t, err)
	mp := InitMicroPayment(account, RPC_ADDRESS)
	assert.NotNil(t, mp)
	secret := "hello world117"
	tx, err := mp.RegisterSecret([]byte(secret))
	assert.Nil(t, err)
	fmt.Printf("tx :%s\n", hex.EncodeToString(common.ToArrayReverse(tx)))
	confirmed, err := chainSdk.PollForTxConfirmed(time.Duration(60)*time.Second, tx)
	fmt.Printf("confirmed:%t\n", confirmed)
	assert.Nil(t, err)
	assert.True(t, confirmed)
}

func TestGetSecretRevealBlockHeight(t *testing.T) {
	chainSdk := chain.NewChainSdk()
	wallet, err := chainSdk.OpenWallet(WALLET_PATH)
	assert.NotNil(t, wallet)
	assert.Nil(t, err)
	account, err := wallet.GetDefaultAccount([]byte(WALLET_PWD))
	assert.Nil(t, err)
	mp := InitMicroPayment(account, RPC_ADDRESS)
	assert.NotNil(t, mp)
	assert.NotNil(t, mp)
	secretHash, err := hex.DecodeString("6e13cc69a7ec5fbd77f98f6e2f2c017a58e48aec55ae93a3428b884ab7068c29")
	assert.Nil(t, err)
	height, err := mp.GetSecretRevealBlockHeight(secretHash)
	assert.Nil(t, err)
	fmt.Printf("height :%d\n", height)
}

func TestGetCurrentHeight(t *testing.T) {
	chainSdk := chain.NewChainSdk()
	wallet, err := chainSdk.OpenWallet(WALLET_PATH)
	assert.NotNil(t, wallet)
	assert.Nil(t, err)
	account, err := wallet.GetDefaultAccount([]byte(WALLET_PWD))
	assert.Nil(t, err)
	mp := InitMicroPayment(account, RPC_ADDRESS)
	assert.NotNil(t, mp)
	assert.NotNil(t, mp)
	h, _ := chainSdk.GetCurrentBlockHeight()
	height, _ := strconv.ParseUint(string(h), 10, 64)
	fmt.Printf("h:%d\n", height)
}

func TestFilter(t *testing.T) {
	// client := NewRpcClient("http://127.0.0.1:20336")
	// ret, _ := client.GetFilterArgsForAllEventsFromChannel(0, 7373, 7375)
	// fmt.Printf("ret:%v\n", ret)
}
