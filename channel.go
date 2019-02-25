package dsp

// SetUnitPriceForAllFile. set unit price for block sharing for all files
func (this *Dsp) SetUnitPriceForAllFile(asset int32, price uint64) {
	this.Channel.SetUnitPrices(asset, price)
}

// CleanUnitPriceForAllFile. clean unit price for block sharing for all files
func (this *Dsp) CleanUnitPriceForAllFile(asset int32) {
	this.Channel.CleanUninPrices(asset)
}

func (this *Dsp) SetUnitPriceForFile(fileHashStr string, asset int32, price uint64) {
	this.taskMgr.SetFileUnitPrice(fileHashStr, asset, price)
}

func (this *Dsp) GetFileUnitPrice(fileHashStr string, asset int32) (uint64, error) {
	// check file db for set file price first
	price, err := this.taskMgr.FileUnitPrice(fileHashStr, asset)
	if err == nil {
		return price, nil
	}
	// check if has  unitprice for all file
	return this.Channel.GetUnitPrices(asset)
}
