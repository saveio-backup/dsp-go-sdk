package store

import (
	"encoding/json"
	"fmt"

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
	key := []byte(fmt.Sprintf("payment:%d", paymentId))
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
	key := []byte(fmt.Sprintf("payment:%d", paymentId))
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
	key := []byte(fmt.Sprintf("payment:%d", paymentId))
	return this.db.Delete(key)
}

// AddPartner. add partner to localDB.  walletAddr <=> map[partnerAddr]struct{}
func (this *ChannelDB) AddPartner(walletAddr, partnerAddr string) error {
	key := []byte(fmt.Sprintf("channel:%s", walletAddr))
	value, _ := this.db.Get(key)

	var partners map[string]struct{}
	if value != nil {
		json.Unmarshal(value, &partners)
	} else {
		partners = make(map[string]struct{}, 0)
	}
	_, ok := partners[partnerAddr]
	if ok {
		return nil
	}
	partners[partnerAddr] = struct{}{}
	data, err := json.Marshal(partners)
	if err != nil {
		return err
	}
	return this.db.Put(key, data)
}

func (this *ChannelDB) DeletePartner(walletAddr, partnerAddr string) error {
	key := []byte(fmt.Sprintf("channel:%s", walletAddr))
	value, _ := this.db.Get(key)

	var partners map[string]struct{}
	if value != nil {
		json.Unmarshal(value, &partners)
	} else {
		partners = make(map[string]struct{}, 0)
	}
	_, ok := partners[partnerAddr]
	if !ok {
		return nil
	}
	delete(partners, partnerAddr)
	data, err := json.Marshal(partners)
	if err != nil {
		return err
	}
	return this.db.Put(key, data)
}

func (this *ChannelDB) GetPartners(walletAddr string) ([]string, error) {
	key := []byte(fmt.Sprintf("channel:%s", walletAddr))
	value, _ := this.db.Get(key)
	if value == nil {
		return nil, nil
	}
	var partners map[string]struct{}
	json.Unmarshal(value, &partners)
	ps := make([]string, 0)
	for addr, _ := range partners {
		ps = append(ps, addr)
	}
	return ps, nil
}

func (this *ChannelDB) OverridePartners(walletAddr string, partnerAddrs []string) error {
	key := []byte(fmt.Sprintf("channel:%s", walletAddr))
	partners := make(map[string]struct{}, 0)
	for _, addr := range partnerAddrs {
		partners[addr] = struct{}{}
	}
	data, err := json.Marshal(partners)
	if err != nil {
		return err
	}
	return this.db.Put(key, data)
}
