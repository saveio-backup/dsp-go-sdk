package chain

type ChainOption interface {
	apply(*Chain)
}

type ChainOptFunc func(client *Chain)

func (f ChainOptFunc) apply(c *Chain) {
	f(c)
}

func IsClient(is bool) ChainOption {
	return ChainOptFunc(func(c *Chain) {
		c.SetIsClient(is)
	})
}

func BlockConfirm(blockConfirm uint32) ChainOption {
	return ChainOptFunc(func(c *Chain) {
		c.SetBlockConfirm(blockConfirm)
	})
}
