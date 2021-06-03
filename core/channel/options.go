package channel

import "github.com/saveio/dsp-go-sdk/store"

type InitChannelOptions struct {
	ClientType    string
	RevealTimeout string
	DBPath        string
	SettleTimeout string
	BlockDelay    string
	IsClient      bool
	DB            *store.ChannelDB
}

func DefaultInitChannelOptions() *InitChannelOptions {
	return &InitChannelOptions{
		ClientType:    "rpc",
		RevealTimeout: "20",
		DBPath:        "./ChannelDB",
		SettleTimeout: "50",
		BlockDelay:    "3",
	}
}

type ChannelOpt interface {
	apply(*InitChannelOptions)
}

type ChannelOptFunc func(*InitChannelOptions)

func (f ChannelOptFunc) apply(c *InitChannelOptions) {
	f(c)
}

func ClientType(clientType string) ChannelOpt {
	return ChannelOptFunc(func(o *InitChannelOptions) {
		o.ClientType = clientType
	})
}

func RevealTimeout(RevealTimeout string) ChannelOpt {
	return ChannelOptFunc(func(o *InitChannelOptions) {
		o.RevealTimeout = RevealTimeout
	})
}

func DBPath(DBPath string) ChannelOpt {
	return ChannelOptFunc(func(o *InitChannelOptions) {
		o.DBPath = DBPath
	})
}

func SettleTimeout(SettleTimeout string) ChannelOpt {
	return ChannelOptFunc(func(o *InitChannelOptions) {
		o.SettleTimeout = SettleTimeout
	})
}

func BlockDelay(BlockDelay string) ChannelOpt {
	return ChannelOptFunc(func(o *InitChannelOptions) {
		o.BlockDelay = BlockDelay
	})
}

func IsClient(isClient bool) ChannelOpt {
	return ChannelOptFunc(func(o *InitChannelOptions) {
		o.IsClient = isClient
	})
}

func ChannelDB(db *store.ChannelDB) ChannelOpt {
	return ChannelOptFunc(func(d *InitChannelOptions) {
		d.DB = db
	})
}
