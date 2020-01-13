package store

import (
	"encoding/json"
	"sort"
	"sync"
	"time"
)

type UserspaceOperation uint

const (
	UserspaceOperationNone UserspaceOperation = iota
	UserspaceOperationAdd
	UserspaceOperationRevoke
)

type UserspaceTransferType uint

const (
	TransferTypeNone UserspaceTransferType = iota
	TransferTypeIn
	TransferTypeOut
)

type UserspaceRecord struct {
	Id              string
	WalletAddr      string
	Size            uint64
	SizeOperation   UserspaceOperation
	Second          uint64
	SecondOperation UserspaceOperation
	Amount          uint64
	TransferType    UserspaceTransferType
	TotalSize       uint64
	ExpiredAt       uint64
	CreatedAt       uint64
	UpdatedAt       uint64
}

type UserspaceRecordDB struct {
	db   *LevelDBStore
	lock *sync.Mutex
}

func NewUserspaceRecordDB(d *LevelDBStore) *UserspaceRecordDB {
	p := &UserspaceRecordDB{
		db:   d,
		lock: new(sync.Mutex),
	}
	return p
}

func (this *UserspaceRecordDB) Close() {
	this.db.Close()
}

func (this *UserspaceRecordDB) InsertUserspaceRecord(id, walletAddr string, size uint64,
	sizeOp UserspaceOperation, second uint64, secondOp UserspaceOperation, amount uint64,
	transferType UserspaceTransferType) error {
	totalSize, expiredAt := uint64(0), uint64(0)
	records, _ := this.SelectUserspaceRecordByWalletAddr(walletAddr, 0, 1)
	now := uint64(time.Now().Unix())
	if len(records) > 0 && records[0].ExpiredAt > now {
		totalSize = records[0].TotalSize
		expiredAt = records[0].ExpiredAt
	}

	switch sizeOp {
	case UserspaceOperationAdd:
		totalSize += size
	case UserspaceOperationRevoke:
		if totalSize >= size {
			totalSize -= size
		}
	}

	switch secondOp {
	case UserspaceOperationAdd:
		if expiredAt == 0 {
			expiredAt = now
		}
		expiredAt += second
	case UserspaceOperationRevoke:
		if expiredAt >= second {
			expiredAt -= second
		} else {
			expiredAt = now
		}
	}
	sr := &UserspaceRecord{
		Id:              id,
		WalletAddr:      walletAddr,
		Size:            size,
		SizeOperation:   sizeOp,
		Second:          second,
		SecondOperation: secondOp,
		Amount:          amount,
		TransferType:    transferType,
		TotalSize:       totalSize,
		ExpiredAt:       expiredAt,
		CreatedAt:       uint64(time.Now().Unix()),
		UpdatedAt:       uint64(time.Now().Unix()),
	}
	data, err := json.Marshal(sr)
	if err != nil {
		return err
	}
	key := []byte(UserspaceRecordKey(id))
	return this.db.Put(key, data)
}

type UserspaceRecords []*UserspaceRecord

func (s UserspaceRecords) Len() int {
	return len(s)
}
func (s UserspaceRecords) Less(i, j int) bool {
	return s[i].UpdatedAt < s[j].UpdatedAt
}

func (s UserspaceRecords) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

// SelectUserspaceRecordByWalletAddr.
func (this *UserspaceRecordDB) SelectUserspaceRecordByWalletAddr(walletAddr string, offset, limit uint64) ([]*UserspaceRecord, error) {
	prefix := []byte(UserspaceRecordKey(""))
	keys, err := this.db.QueryStringKeysByPrefix(prefix)
	if err != nil {
		return nil, err
	}
	srs := make(UserspaceRecords, 0, limit)
	for _, k := range keys {
		sr := &UserspaceRecord{}
		data, _ := this.db.Get([]byte(k))
		if len(data) == 0 {
			continue
		}
		err := json.Unmarshal(data, &sr)
		if err != nil {
			continue
		}
		if sr.WalletAddr != walletAddr {
			continue
		}
		srs = append(srs, sr)
	}
	sort.Sort(sort.Reverse(srs))
	if limit == 0 {
		return srs[offset:], nil
	}
	end := offset + limit
	if end > uint64(len(srs)) {
		end = uint64(len(srs))
	}
	return srs[offset:end], nil
}
