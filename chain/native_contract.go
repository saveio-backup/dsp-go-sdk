package chain

import (
	"github.com/oniio/dsp-go-sdk/chain/auth"
	"github.com/oniio/dsp-go-sdk/chain/channel"
	"github.com/oniio/dsp-go-sdk/chain/client"
	"github.com/oniio/dsp-go-sdk/chain/dns"
	"github.com/oniio/dsp-go-sdk/chain/fs"
	cgp "github.com/oniio/dsp-go-sdk/chain/globalparam"
	"github.com/oniio/dsp-go-sdk/chain/ong"
	"github.com/oniio/dsp-go-sdk/chain/ont"
	"github.com/oniio/dsp-go-sdk/chain/ontid"
	"github.com/oniio/oniChain/account"
)

type NativeContract struct {
	Ont          *ont.Ont
	Ong          *ong.Ong
	OntId        *ontid.OntId
	GlobalParams *cgp.GlobalParam
	Auth         *auth.Auth
	Dns          *dns.Dns
	Fs           *fs.Fs
	Channel      *channel.Channel
}

func newNativeContract(client *client.ClientMgr) *NativeContract {
	native := &NativeContract{}
	native.Ont = &ont.Ont{Client: client}
	native.Ong = &ong.Ong{Client: client}
	native.OntId = &ontid.OntId{Client: client}
	native.GlobalParams = &cgp.GlobalParam{Client: client}
	native.Auth = &auth.Auth{Client: client}
	native.Dns = &dns.Dns{Client: client}
	native.Fs = &fs.Fs{Client: client}
	native.Channel = &channel.Channel{Client: client}
	return native
}

func (this *NativeContract) SetDefaultAccount(acc *account.Account) {
	this.Channel.DefAcc = acc
	this.Fs.DefAcc = acc
	this.Dns.DefAcc = acc
}
