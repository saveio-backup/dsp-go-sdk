package dns

type DNSOption interface {
	apply(*DNS)
}

type DNSFunc func(*DNS)

func (f DNSFunc) apply(d *DNS) {
	f(d)
}

func MaxDNSNodeNum(num int) DNSOption {
	return DNSFunc(func(d *DNS) {
		d.maxNodeNum = num
	})
}

func ReservedDNS(walletAddrs []string) DNSOption {
	return DNSFunc(func(d *DNS) {
		d.reservedDNS = walletAddrs
	})
}

func ReservedTrackers(trs []string) DNSOption {
	return DNSFunc(func(d *DNS) {
		d.reservedTrackers = trs
	})
}

func TrackerProtocol(p string) DNSOption {
	return DNSFunc(func(d *DNS) {
		d.trackerProtocol = p
	})
}

func AutoBootstrap(auto bool) DNSOption {
	return DNSFunc(func(d *DNS) {
		d.autoBootstrap = auto
	})
}

func ChannelProtocol(protocol string) DNSOption {
	return DNSFunc(func(d *DNS) {
		d.channelProtocol = protocol
	})
}

func LastUsedDNS(lastUsedDNS string) DNSOption {
	return DNSFunc(func(d *DNS) {
		d.lastUsedDNS = lastUsedDNS
	})
}
