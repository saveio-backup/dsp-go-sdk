package dsp

import (
	"errors"
)

// SetUnitPriceForAllFile. set unit price for block sharing for all files
func (this *Dsp) SetUnitPriceForAllFile(asset int32, price uint64) {
	this.Channel.SetUnitPrices(asset, price)
}

// CleanUnitPriceForAllFile. clean unit price for block sharing for all files
func (this *Dsp) CleanUnitPriceForAllFile(asset int32) {
	this.Channel.CleanUninPrices(asset)
}

func (this *Dsp) GetFileUnitPrice(asset int32) (uint64, error) {
	return this.Channel.GetUnitPrices(asset)
}

func (this *Dsp) setupDNSNodeHost() error {
	if this.DNSNode == nil {
		return errors.New("no dns node to setup")
	}
	err := this.Channel.SetHostAddr(this.DNSNode.WalletAddr, this.DNSNode.ChannelAddr)
	if err != nil {
		return err
	}
	return nil
}
