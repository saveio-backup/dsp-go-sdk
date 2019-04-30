package channel

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/saveio/dsp-go-sdk/config"
	chain "github.com/saveio/themis-go-sdk"
	"github.com/saveio/themis-go-sdk/wallet"
	cliutil "github.com/saveio/themis/cmd/utils"
	"github.com/saveio/themis/common/log"
	"github.com/saveio/themis/crypto/keypair"
	"github.com/saveio/pylons/transfer"
)

var rpcAddr = "http://127.0.0.1:20336"

var walletFile = "../testdata/wallet.dat"
var wallet2File = "../testdata/wallet2.dat"
var wallet3File = "../testdata/wallet3.dat"
var walletPwd = "pwd"

var channel1Addr = "127.0.0.1:13001"
var channel2Addr = "127.0.0.1:13002"
var channel3Addr = "127.0.0.1:13003"
var channel4Addr = "127.0.0.1:13004"
var channel5Addr = "127.0.0.1:13005"

func TestCloseChannel(t *testing.T) {
	w, err := wallet.OpenWallet(walletFile)
	if err != nil {
		t.Fatal(err)
	}
	acc, err := w.GetDefaultAccount([]byte(walletPwd))
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("acc %v\n", acc)

	w2, err := wallet.OpenWallet(wallet2File)
	if err != nil {
		t.Fatal(err)
	}
	acc2, err := w2.GetDefaultAccount([]byte(walletPwd))
	if err != nil {
		t.Fatal(err)
	}
	chain := chain.NewChain()
	chain.NewRpcClient().SetAddress(rpcAddr)
	chain.SetDefaultAccount(acc2)

	target := acc.Address
	id, err := chain.Native.Channel.GetChannelIdentifier(acc2.Address, target)
	if err != nil {
		t.Fatal(err)
	}
	if id == 0 {
		t.Fatal("id is 0")
	}
	fmt.Printf("id = %d, err %s\n", id, err)
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	nonce := r.Uint64()
	sig, err := cliutil.Sign([]byte("123"), acc2)
	if err != nil {
		t.Fatal(err)
	}
	hash, err := chain.Native.Channel.CloseChannel(id, acc2.Address, target, nil, nonce, nil, sig, keypair.SerializePublicKey(acc2.PublicKey))
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("hash:%v\n", hash)
	fmt.Printf("id = %d, err %s\n", id, err)
}

func TestDepositChannel(t *testing.T) {
	w, err := wallet.OpenWallet(walletFile)
	if err != nil {
		t.Fatal(err)
	}
	acc, err := w.GetDefaultAccount([]byte(walletPwd))
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("acc %v\n", acc)
	chain := chain.NewChain()
	chain.NewRpcClient().SetAddress(rpcAddr)
	w2, err := wallet.OpenWallet(wallet2File)
	if err != nil {
		t.Fatal(err)
	}
	acc2, err := w2.GetDefaultAccount([]byte(walletPwd))
	if err != nil {
		t.Fatal(err)
	}
	chain.SetDefaultAccount(acc2)

	cfg := &config.DspConfig{
		ChainRpcAddr:         rpcAddr,
		ChannelClientType:    "rpc",
		ChannelListenAddr:    channel3Addr,
		ChannelProtocol:      "tcp",
		ChannelRevealTimeout: "1000",
	}
	c, _ := NewChannelService(cfg, chain)
	id, err := c.OpenChannel(acc.Address.ToBase58())
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("id = %d\n", id)
	if id == 0 {
		t.Fatal("id is 0")
	}
	err = c.StartService()
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("chain.acc %s, acc %s\n", chain.Native.Channel.DefAcc.Address.ToBase58(), acc.Address.ToBase58())
	err = c.SetDeposit(acc.Address.ToBase58(), 100)
	if err != nil {
		t.Fatal(err)
	}
}

func TestGetTargetBalance(t *testing.T) {
	log.InitLog(1, log.PATH, log.Stdout)
	w, err := wallet.OpenWallet(wallet3File)
	if err != nil {
		t.Fatal(err)
	}
	acc, err := w.GetDefaultAccount([]byte(walletPwd))
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("acc %v\n", acc)

	chain := chain.NewChain()
	chain.NewRpcClient().SetAddress(rpcAddr)
	chain.SetDefaultAccount(acc)
	cfg := &config.DspConfig{
		ChainRpcAddr:         rpcAddr,
		ChannelClientType:    "rpc",
		ChannelListenAddr:    channel3Addr,
		ChannelProtocol:      "tcp",
		ChannelRevealTimeout: "1000",
	}
	target := "ANa3f9jm2FkWu4NrVn6L1FGu7zadKdvPjL"
	if target == "" {
	}
	c, _ := NewChannelService(cfg, chain)
	neighbours := transfer.GetNeighbours(c.channel.Service.StateFromChannel())
	fmt.Printf("len:%d\n", len(neighbours))

	err = c.StartService()
	if err != nil {
		t.Fatal(err)
	}
	bal, err := c.GetCurrentBalance(target)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("balance %v\n", bal)
	c.SetHostAddr(target, "tcp://127.0.0.1:13004")
	_, err = c.OpenChannel(target)
	if err != nil {
		t.Fatal(err)
	}
	err = c.WaitForConnected(target, time.Duration(60)*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("connected\n")
}
