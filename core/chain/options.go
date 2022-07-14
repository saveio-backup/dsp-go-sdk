package themis

type ChainOption interface {
	apply(*Chain)
}

type ChainOptFunc func(*Chain)

func (f ChainOptFunc) apply(c *Chain) {
	f(c)
}

func IsClient(is bool) ChainOption {
	return ChainOptFunc(func(c *Chain) {
		c.isClient = is
	})
}

func BlockConfirm(blockConfirm uint32) ChainOption {
	return ChainOptFunc(func(c *Chain) {
		c.blockConfirm = blockConfirm
	})
}
