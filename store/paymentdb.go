package store

import (
	"encoding/json"
	"fmt"

	"github.com/syndtr/goleveldb/leveldb"
)

type PaymentDB struct {
	db *LevelDBStore
}

func NewPaymentDB(d *LevelDBStore) *PaymentDB {
	p := &PaymentDB{
		db: d,
	}
	return p
}

// AddPayment. put payment info to db
func (this *PaymentDB) AddPayment(walletAddr string, paymentId int32, amount uint64) error {
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
func (this *PaymentDB) GetPayment(paymentId int32) (*Payment, error) {
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
func (this *PaymentDB) RemovePayment(paymentId int32) error {
	key := []byte(fmt.Sprintf("payment:%d", paymentId))
	return this.db.Delete(key)
}
