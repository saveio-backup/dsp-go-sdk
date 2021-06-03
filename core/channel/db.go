package channel

import (
	sdkErr "github.com/saveio/dsp-go-sdk/error"
	"github.com/saveio/dsp-go-sdk/store"
	"github.com/saveio/themis/common/log"
)

func (this *Channel) SetChannelDB(db *store.ChannelDB) {
	this.db = db
}

// SelectDNSChannel. select a dns channel and update it's record timestamp
func (this *Channel) SelectDNSChannel(dnsWalletAddr string) error {
	log.Debugf("select dns wallet %s", dnsWalletAddr)
	err := this.db.SelectChannel(dnsWalletAddr)
	if err != nil {
		return sdkErr.NewWithError(sdkErr.CHANNEL_INTERNAL_ERROR, err)
	}
	return nil
}

// SelectDNSChannel. select a dns channel and update it's record timestamp
func (this *Channel) SetChannelIsDNS(dnsWalletAddr string, isDNS bool) error {
	log.Debugf("set dns wallet %s isDNS %t", dnsWalletAddr, isDNS)
	err := this.db.SetChannelIsDNS(dnsWalletAddr, isDNS)
	if err != nil {
		return sdkErr.NewWithError(sdkErr.CHANNEL_INTERNAL_ERROR, err)
	}
	return nil
}

func (this *Channel) GetLastUsedDNSWalletAddr() (string, error) {
	walletAddr, err := this.db.GetLastUsedDNSChannel()
	if err != nil {
		return "", sdkErr.NewWithError(sdkErr.CHANNEL_INTERNAL_ERROR, err)
	}
	return walletAddr, nil
}

func (this *Channel) AddChannelInfo(id uint64, partnerAddr string) error {
	if this.db == nil {
		return sdkErr.New(sdkErr.CHANNEL_SET_DB_ERROR, "no channel DB")
	}
	err := this.db.AddChannelInfo(id, partnerAddr)
	if err != nil {
		return sdkErr.NewWithError(sdkErr.CHANNEL_SET_DB_ERROR, err)
	}
	return nil
}

func (this *Channel) GetChannelInfoFromDB(targetAddress string) (*store.ChannelInfo, error) {
	info, err := this.db.GetChannelInfo(targetAddress)
	if err != nil {
		return nil, sdkErr.NewWithError(sdkErr.CHANNEL_GET_DB_ERROR, err)
	}
	return info, nil
}
