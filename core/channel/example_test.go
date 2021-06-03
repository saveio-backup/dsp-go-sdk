package channel

import (
	"fmt"
	"log"

	chain "github.com/saveio/themis-go-sdk"
	"github.com/saveio/themis-go-sdk/wallet"
)

func ExampleChannel_CloseChannel() {
	w, err := wallet.OpenWallet(walletFile)
	if err != nil {
		log.Fatal(err)
	}
	acc, err := w.GetDefaultAccount([]byte(walletPwd))
	if err != nil {
		log.Fatal(err)
	}

	w2, err := wallet.OpenWallet(wallet2File)
	if err != nil {
		log.Fatal(err)
	}
	acc2, err := w2.GetDefaultAccount([]byte(walletPwd))
	if err != nil {
		log.Fatal(err)
	}
	chain := chain.NewChain()
	chain.NewRpcClient().SetAddress([]string{rpcAddr})
	chain.SetDefaultAccount(acc)
	channel, err := NewChannelService(chain)
	if err != nil {
		log.Fatal(err)
	}
	if err = channel.StartService(); err != nil {
		log.Fatal(err)
	}
	if err := channel.CloseChannel(acc2.Address.ToBase58()); err != nil {
		log.Fatal(err)
	}
}

func ExampleChannel_SetDeposit() {
	w, err := wallet.OpenWallet(walletFile)
	if err != nil {
		log.Fatal(err)
	}
	acc, err := w.GetDefaultAccount([]byte(walletPwd))
	if err != nil {
		log.Fatal(err)
	}

	w2, err := wallet.OpenWallet(wallet2File)
	if err != nil {
		log.Fatal(err)
	}
	acc2, err := w2.GetDefaultAccount([]byte(walletPwd))
	if err != nil {
		log.Fatal(err)
	}
	chain := chain.NewChain()
	chain.NewRpcClient().SetAddress([]string{rpcAddr})
	chain.SetDefaultAccount(acc)
	channel, err := NewChannelService(chain)
	if err != nil {
		log.Fatal(err)
	}
	if err = channel.StartService(); err != nil {
		log.Fatal(err)
	}
	id, err := channel.OpenChannel(acc2.Address.ToBase58(), 0)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("id = %d\n", id)
	if id == 0 {
		log.Fatal("id is 0")
	}
	fmt.Printf("chain.acc %s, acc %s\n", chain.Native.Channel.DefAcc.Address.ToBase58(), acc.Address.ToBase58())
	err = channel.SetDeposit(acc2.Address.ToBase58(), 100)
	if err != nil {
		log.Fatal(err)
	}
}

func ExampleChannel_GetCurrentBalance() {
	w, err := wallet.OpenWallet(wallet3File)
	if err != nil {
		log.Fatal(err)
	}
	acc, err := w.GetDefaultAccount([]byte(walletPwd))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("acc %v\n", acc)

	chain := chain.NewChain()
	chain.NewRpcClient().SetAddress([]string{rpcAddr})
	chain.SetDefaultAccount(acc)
	target := "ANa3f9jm2FkWu4NrVn6L1FGu7zadKdvPjL"
	c, err := NewChannelService(chain)
	if err != nil {
		log.Fatal(err)
	}
	if err := c.StartService(); err != nil {
		log.Fatal(err)
	}
	bal, err := c.GetCurrentBalance(target)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("balance %v\n", bal)
}
