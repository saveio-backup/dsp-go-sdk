package store

import (
	"encoding/json"
	"fmt"

	"github.com/saveio/dsp-go-sdk/utils"
	"github.com/syndtr/goleveldb/leveldb"
)

type ChannelDB struct {
	db *LevelDBStore
}

func NewChannelDB(d *LevelDBStore) *ChannelDB {
	p := &ChannelDB{
		db: d,
	}
	return p
}

func (this *ChannelDB) Close() {
	this.db.Close()
}

// AddPayment. put payment info to db
func (this *ChannelDB) AddPayment(walletAddr string, paymentId int32, amount uint64) error {
	key := []byte(PaymentKey(paymentId))
	info := &Payment{
		WalletAddress: walletAddr,
		PaymentId:     paymentId,
		Amount:        amount,
	}
	buf, err := json.Marshal(info)
	if err != nil {
		return err
	}
	return this.db.Put(key, buf)
}

// GetPayment. get payment info from db
func (this *ChannelDB) GetPayment(paymentId int32) (*Payment, error) {
	key := []byte(PaymentKey(paymentId))
	value, err := this.db.Get(key)
	if err != nil {
		if err != leveldb.ErrNotFound {
			return nil, err
		}
	}
	if len(value) == 0 {
		return nil, nil
	}

	info := &Payment{}
	err = json.Unmarshal(value, info)
	if err != nil {
		return nil, err
	}
	return info, nil
}

// RemovePayment. remove payment info from db
func (this *ChannelDB) RemovePayment(paymentId int32) error {
	key := []byte(PaymentKey(paymentId))
	return this.db.Delete(key)
}

type ChannelInfo struct {
	ID          uint64 `json:"id"`
	PartnerAddr string `json:"partner_address"`
	IsDNS       bool   `json:"is_dns"`
	CreatedAt   uint64 `json:"createdAt"`
}

// AddPartner. add partner to localDB.  walletAddr <=> map[partnerAddr]struct{}
func (this *ChannelDB) AddChannelInfo(id uint64, partnerAddr string) error {
	key := []byte(ChannelInfoKey(partnerAddr))
	value, err := this.db.Get(key)
	if err != nil && err != leveldb.ErrNotFound {
		return err
	}
	if len(value) > 0 {
		return nil
	}
	ch := &ChannelInfo{
		ID:          id,
		PartnerAddr: partnerAddr,
		CreatedAt:   utils.GetMilliSecTimestamp(),
	}
	buf, err := json.Marshal(ch)
	if err != nil {
		return err
	}
	return this.db.Put(key, buf)
}

func (this *ChannelDB) SetChannelIsDNS(partnerAddr string, isDNS bool) error {
	key := []byte(ChannelInfoKey(partnerAddr))
	value, err := this.db.Get(key)
	if err != nil && err != leveldb.ErrNotFound {
		return err
	}
	if len(value) == 0 {
		return fmt.Errorf("channel not exist")
	}
	ch := &ChannelInfo{}
	err = json.Unmarshal(value, &ch)
	if err != nil {
		return err
	}
	ch.IsDNS = isDNS
	buf, err := json.Marshal(ch)
	if err != nil {
		return err
	}
	return this.db.Put(key, buf)
}

func (this *ChannelDB) GetChannelInfo(partnerAddr string) (*ChannelInfo, error) {
	key := []byte(ChannelInfoKey(partnerAddr))
	value, err := this.db.Get(key)
	if err != nil && err != leveldb.ErrNotFound {
		return nil, err
	}
	if len(value) == 0 {
		return nil, fmt.Errorf("channel info is empty")
	}
	ch := &ChannelInfo{}
	err = json.Unmarshal(value, &ch)
	if err != nil {
		return nil, err
	}
	return ch, nil
}

func (this *ChannelDB) DeleteChannelInfo(partnerAddr string) error {
	key := []byte(ChannelInfoKey(partnerAddr))
	return this.db.Delete(key)
}

func (this *ChannelDB) GetPartners() ([]string, error) {
	prefix := ChannelInfoKeyPrefix()
	keys, err := this.db.QueryKeysByPrefix([]byte(prefix))
	if err != nil {
		return nil, err
	}
	ps := make([]string, 0, len(keys))
	for _, key := range keys {
		value, err := this.db.Get(key)
		if err != nil || len(value) == 0 {
			continue
		}
		ch := &ChannelInfo{}
		err = json.Unmarshal(value, &ch)
		if err != nil {
			continue
		}
		ps = append(ps, ch.PartnerAddr)
	}
	return ps, nil
}

func (this *ChannelDB) OverridePartners(walletAddr string, partnerAddrs []string) error {
	prefix := ChannelInfoKeyPrefix()
	keys, err := this.db.QueryKeysByPrefix([]byte(prefix))
	if err != nil {
		return err
	}
	newParnerM := make(map[string]struct{}, 0)
	for _, addr := range partnerAddrs {
		newParnerM[addr] = struct{}{}
	}
	deleteChannels := make([]string, 0, len(keys))
	for _, key := range keys {
		value, err := this.db.Get(key)
		if err != nil || len(value) == 0 {
			continue
		}
		ch := &ChannelInfo{}
		err = json.Unmarshal(value, &ch)
		if err != nil {
			continue
		}
		_, ok := newParnerM[ch.PartnerAddr]
		if ok {
			continue
		}
		deleteChannels = append(deleteChannels, ch.PartnerAddr)
	}
	if len(deleteChannels) == 0 {
		return nil
	}

	batch := this.db.NewBatch()
	for _, addr := range deleteChannels {
		this.db.BatchDelete(batch, []byte(ChannelInfoKey(addr)))
	}
	return this.db.BatchCommit(batch)
}
