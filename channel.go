package dsp

// SetUnitPriceForAllFile. set unit price for block sharing for all files
func (this *Dsp) SetUnitPriceForAllFile(asset int32, price uint64) {
	if this.Channel == nil {
		return
	}
	this.Channel.SetUnitPrices(asset, price)
}

// CleanUnitPriceForAllFile. clean unit price for block sharing for all files
func (this *Dsp) CleanUnitPriceForAllFile(asset int32) {
	this.Channel.CleanUnitPrices(asset)
}

func (this *Dsp) GetFileUnitPrice(asset int32) (uint64, error) {
	return this.Channel.GetUnitPrices(asset)
}
