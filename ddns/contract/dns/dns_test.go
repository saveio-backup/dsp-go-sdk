package dns

import (
	"fmt"
	"testing"
	"time"

	chain "github.com/oniio/dsp-go-sdk/chain"
	dns "github.com/oniio/oniChain/smartcontract/service/native/dns"
)

var walletPath = "../wallet.dat"
var pwd = []byte("123456")
var rpc_addr = "http://127.0.0.1:20336"

func init() {
	var err error
	sdk := chain.NewChainSdk()
	w, err := sdk.OpenWallet(walletPath)
	if err != nil {
		fmt.Printf("Account.Open error:%s\n", err)
	}
	acc, err := w.GetDefaultAccount(pwd)
	if err != nil {
		fmt.Printf("GetDefaultAccount error:%s\n", err)
	}
	InitDNS(acc, sdk, rpc_addr)
}
func TestRegister(t *testing.T) {
	fmt.Printf("====register a random default url with dsp header====\n")
	ret1, err := RegisterUrl("", dns.SYSTEM, "path://weqwquhdnskfudyzksdwj", "32123232", 123235)
	if err != nil {
		t.Errorf(err.Error())
		return
	}
	fmt.Printf("Random url item txHash: %v\n", ret1.ToHexString())

	fmt.Printf("====register a header====\n")
	ret2, err := RegisterHeader("ftp", "test", 100000)
	if err != nil {
		t.Errorf(err.Error())
		return
	}
	fmt.Printf("Register header item txHash: %v\n", ret2.ToHexString())
	fmt.Println("Wait For Generate Block......")
	ChainSdk.WaitForGenerateBlock(30*time.Second, 1)
	fmt.Println("====query header dsp====")
	info, err := QueryHeader("ftp")
	if err != nil {
		t.Errorf("QueryHeader error:%s", err)
		return
	}
	fmt.Printf("header dsp: %+v\n", info)

	fmt.Printf("====register a random url with custom header====\n")
	ret3, err := RegisterUrl("ftp://", dns.CUSTOM_HEADER, "path://1234567", "1111111", 1)
	if err != nil {
		t.Errorf(err.Error())
		return
	}
	fmt.Printf("Random url with custom header item txHash: %v\n", ret3.ToHexString())

	fmt.Printf("====register a custom url with dsp header====\n")
	ret4, err := RegisterUrl("dsp://onchain.com", dns.CUSTOM_URL, "path://weqwquhdnskfudyzksdwj", "32123232", 123235)
	if err != nil {
		t.Errorf(err.Error())
		return
	}
	fmt.Printf("Custom url with dsp header item txHash: %v\n", ret4.ToHexString())

	fmt.Printf("====regist a custom url with custom header====\n")
	ret5, err := RegisterUrl("ftp://www.onchain.com", dns.CUSTOM_HEADER_URL, "path://weqwquhdnskfudyzksdwj", "32123232", 123235)
	if err != nil {
		t.Errorf(err.Error())
		return
	}
	fmt.Printf("Custom header url with custom header item txHash: %v\n", ret5.ToHexString())

	fmt.Println("Wait For Generate Block......")
	ChainSdk.WaitForGenerateBlock(30*time.Second, 1)
	event, err := ChainSdk.GetSmartContractEvent(ret1.ToHexString())
	if err != nil {
		t.Errorf("1 GetSmartContractEvent error:%s", err)
		return
	}
	fmt.Printf("regist a random default url with dsp header Event: %+v  %+v\n", event, event.Notify)

	event, err = ChainSdk.GetSmartContractEvent(ret2.ToHexString())
	if err != nil {
		t.Errorf("2 GetSmartContractEvent error:%s\n", err)
		return
	}
	fmt.Printf("regist a header Event: %+v  %+v\n", event, event.Notify)

	event, err = ChainSdk.GetSmartContractEvent(ret3.ToHexString())
	if err != nil {
		t.Errorf("3 GetSmartContractEvent error:%s", err)
		return
	}
	fmt.Printf("Random url with custom header Event: %+v %+v\n", event, event.Notify)

	event, err = ChainSdk.GetSmartContractEvent(ret4.ToHexString())
	if err != nil {
		t.Errorf("4 GetSmartContractEvent error:%s", err)
		return
	}
	fmt.Printf("Custom url with dsp header Event: %+v  %+v\n", event, event.Notify)

	event, err = ChainSdk.GetSmartContractEvent(ret5.ToHexString())
	if err != nil {
		t.Errorf("5 GetSmartContractEvent error:%s", err)
		return
	}
	fmt.Printf("Custom header url with custom header Event: %+v  %+v\n", event, event.Notify)

	nameInfo, err := QueryUrl("dsp://onchain.com")
	if err != nil {
		t.Errorf("QueryUrl error:%s", err)
		return
	}
	fmt.Printf("url: %+v\n", nameInfo)
}

func TestQueryHeader(t *testing.T) {
	headerInfo, err := QueryHeader("ftp")
	if err != nil {
		t.Errorf("QueryHeader error:%s", err)
		return
	}
	fmt.Printf("Header: %s\n", headerInfo.Header)
	fmt.Printf("HeaderOwner: %s\n", headerInfo.HeaderOwner.ToBase58())
	fmt.Printf("Desc: %s\n", headerInfo.Desc)
	fmt.Printf("BlockHeight: %v\n", headerInfo.BlockHeight)
	fmt.Printf("TTL: %v\n", headerInfo.TTL)
}

func TestQueryUrl(t *testing.T) {
	nameInfo, err := QueryUrl("ftp://www.onchain.com")
	if err != nil {
		t.Errorf("QueryUrl error:%s", err)
		return
	}
	fmt.Printf("Header: %s\n", nameInfo.Header)
	fmt.Printf("URL: %s\n", nameInfo.URL)
	fmt.Printf("Name: %s\n", nameInfo.Name)
	fmt.Printf("NameOwner: %s\n", nameInfo.NameOwner.ToBase58())
	fmt.Printf("Desc: %s\n", nameInfo.Desc)
	fmt.Printf("BlockHeight: %v\n", nameInfo.BlockHeight)
	fmt.Printf("TTL: %v\n", nameInfo.TTL)
}

func TestBinding(t *testing.T) {
	nameInfo, err := QueryUrl("ftp://www.onchain.com")
	if err != nil {
		t.Errorf("QueryUrl error:%s", err)
		return
	}
	fmt.Printf("Header: %s\n", nameInfo.Header)
	fmt.Printf("URL: %s\n", nameInfo.URL)
	fmt.Printf("Name: %s\n", nameInfo.Name)
	fmt.Printf("NameOwner: %s\n", nameInfo.NameOwner.ToBase58())
	fmt.Printf("Desc: %s\n", nameInfo.Desc)
	fmt.Printf("BlockHeight: %v\n", nameInfo.BlockHeight)
	fmt.Printf("TTL: %v\n", nameInfo.TTL)
	fmt.Println("bing ftp://www.onchain.com to 127.0.0.1")
	Binding("ftp://www.onchain.com", "127.0.0.1", "should return true", 123456789)
	fmt.Println("Wait For Generate Block......")
	ChainSdk.WaitForGenerateBlock(30*time.Second, 1)
	nameInfo, err = QueryUrl("ftp://www.onchain.com")
	if err != nil {
		t.Errorf("QueryUrl ftp://www.onchain.com failed:%s", err)
	}
	fmt.Printf("Header: %s\n", nameInfo.Header)
	fmt.Printf("URL: %s\n", nameInfo.URL)
	fmt.Printf("Name: %s\n", nameInfo.Name)
	fmt.Printf("NameOwner: %s\n", nameInfo.NameOwner.ToBase58())
	fmt.Printf("Desc: %s\n", nameInfo.Desc)
	fmt.Printf("BlockHeight: %v\n", nameInfo.BlockHeight)
	fmt.Printf("TTL: %v\n", nameInfo.TTL)
}
func TestTransferUrl(t *testing.T) {
	nameInfo, err := QueryUrl("ftp://www.onchain.com")
	if err != nil {
		t.Errorf("QueryUrl error:%s", err)
		return
	}
	fmt.Printf("Header: %s\n", nameInfo.Header)
	fmt.Printf("URL: %s\n", nameInfo.URL)
	fmt.Printf("Name: %s\n", nameInfo.Name)
	fmt.Printf("NameOwner: %s\n", nameInfo.NameOwner.ToBase58())
	fmt.Printf("Desc: %s\n", nameInfo.Desc)
	fmt.Printf("BlockHeight: %v\n", nameInfo.BlockHeight)
	fmt.Printf("TTL: %v\n", nameInfo.TTL)
	fmt.Println("Transfer ftp://www.onchain.com to AFmseVrdL9f9oyCzZefL9tG6UbvhPbdYzM")
	TransferUrl("ftp://www.onchain.com", "AFmseVrdL9f9oyCzZefL9tG6UbvhPbdYzM")
	fmt.Println("Wait For Generate Block......")
	ChainSdk.WaitForGenerateBlock(30*time.Second, 1)
	nameInfo, err = QueryUrl("ftp://www.onchain.com")
	if err != nil {
		t.Errorf("QueryUrl ftp://www.onchain.com failed:%s", err)
		return
	}
	fmt.Printf("Header: %s\n", nameInfo.Header)
	fmt.Printf("URL: %s\n", nameInfo.URL)
	fmt.Printf("Name: %s\n", nameInfo.Name)
	fmt.Printf("NameOwner: %s\n", nameInfo.NameOwner.ToBase58())
	fmt.Printf("Desc: %s\n", nameInfo.Desc)
	fmt.Printf("BlockHeight: %v\n", nameInfo.BlockHeight)
	fmt.Printf("TTL: %v\n", nameInfo.TTL)
}

func TestTransferHeader(t *testing.T) {
	headerInfo, err := QueryHeader("ftp")
	if err != nil {
		t.Errorf("QueryHeader error:%s", err)
		return
	}
	fmt.Printf("Header: %s\n", headerInfo.Header)
	fmt.Printf("HeaderOwner: %s\n", headerInfo.HeaderOwner.ToBase58())
	fmt.Printf("Desc: %s\n", headerInfo.Desc)
	fmt.Printf("BlockHeight: %v\n", headerInfo.BlockHeight)
	fmt.Printf("TTL: %v\n", headerInfo.TTL)
	fmt.Println("Transfer ftp to AFmseVrdL9f9oyCzZefL9tG6UbvhPbdYzM")
	TransferHeader("ftp", "AFmseVrdL9f9oyCzZefL9tG6UbvhPbdYzM")
	fmt.Println("Wait For Generate Block......")
	ChainSdk.WaitForGenerateBlock(30*time.Second, 1)
	headerInfo, err = QueryHeader("ftp")
	if err != nil {
		t.Errorf("QueryHeader ftp: failed:%s", err)
		return
	}
	fmt.Printf("Header: %s\n", headerInfo.Header)
	fmt.Printf("HeaderOwner: %s\n", headerInfo.HeaderOwner.ToBase58())
	fmt.Printf("Desc: %s\n", headerInfo.Desc)
	fmt.Printf("BlockHeight: %v\n", headerInfo.BlockHeight)
	fmt.Printf("TTL: %v\n", headerInfo.TTL)
}

func TestDeleteHeader(t *testing.T) {
	headerInfo, err := QueryHeader("ftp")
	if err != nil {
		t.Errorf("QueryHeader error:%s", err)
		return
	}
	fmt.Printf("Header: %s\n", headerInfo.Header)
	fmt.Printf("HeaderOwner: %s\n", headerInfo.HeaderOwner.ToBase58())
	fmt.Printf("Desc: %s\n", headerInfo.Desc)
	fmt.Printf("BlockHeight: %v\n", headerInfo.BlockHeight)
	fmt.Printf("TTL: %v\n", headerInfo.TTL)
	fmt.Println("delete ftp")
	DeleteHeader("ftp")
	fmt.Println("Wait For Generate Block......")
	ChainSdk.WaitForGenerateBlock(30*time.Second, 1)
	_, err = QueryHeader("ftp")
	if err == nil {
		t.Errorf("delete ftp failed:%s", err)
	}
}
func TestDeleteUrl(t *testing.T) {
	nameInfo, err := QueryUrl("ftp://www.onchain.com")
	if err != nil {
		t.Errorf("QueryUrl error:%s", err)
		return
	}
	fmt.Printf("Header: %s\n", nameInfo.Header)
	fmt.Printf("URL: %s\n", nameInfo.URL)
	fmt.Printf("Name: %s\n", nameInfo.Name)
	fmt.Printf("NameOwner: %s\n", nameInfo.NameOwner.ToBase58())
	fmt.Printf("Desc: %s\n", nameInfo.Desc)
	fmt.Printf("BlockHeight: %v\n", nameInfo.BlockHeight)
	fmt.Printf("TTL: %v\n", nameInfo.TTL)
	fmt.Println("delete ftp://www.onchain.com")
	DeleteUrl("ftp://www.onchain.com")
	fmt.Println("Wait For Generate Block......")
	ChainSdk.WaitForGenerateBlock(30*time.Second, 1)
	_, err = QueryUrl("ftp://www.onchain.com")
	if err == nil {
		t.Errorf("delete ftp://www.onchain.com failed:%s", err)
	}
}
