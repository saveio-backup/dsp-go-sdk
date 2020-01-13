package store

import (
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"time"
)

type ShareRecord struct {
	Id           string
	FileName     string
	FileOwner    string
	FileHash     string
	ToWalletAddr string
	Profit       uint64
	CreatedAt    uint64
	UpdatedAt    uint64
}

type ShareRecordDB struct {
	db   *LevelDBStore
	lock *sync.Mutex
}

func NewShareRecordDB(d *LevelDBStore) *ShareRecordDB {
	p := &ShareRecordDB{
		db:   d,
		lock: new(sync.Mutex),
	}
	return p
}

func (this *ShareRecordDB) Close() {
	this.db.Close()
}

// InsertShareRecord. insert a new miner_record or replace it
func (this *ShareRecordDB) InsertShareRecord(id, fileHash, fileName, fileOwner, toWalletAddr string, profit uint64) error {
	sr := &ShareRecord{}
	key := []byte(ShareRecordKey(id))
	data, _ := this.db.Get(key)
	if len(data) > 0 {
		err := json.Unmarshal(data, &sr)
		if err != nil {
			return err
		}
		sr.UpdatedAt = uint64(time.Now().Unix())
	} else {
		sr.CreatedAt = uint64(time.Now().Unix())
		sr.UpdatedAt = uint64(time.Now().Unix())
	}
	if len(fileHash) > 0 {
		sr.FileHash = fileHash
	}
	if len(fileName) > 0 {
		sr.FileName = fileName
	}
	if len(fileOwner) > 0 {
		sr.FileOwner = fileOwner
	}
	if len(toWalletAddr) > 0 {
		sr.ToWalletAddr = toWalletAddr
	}
	sr.Profit += profit
	buf, err := json.Marshal(sr)
	if err != nil {
		return err
	}
	return this.db.Put(key, buf)
}

// IncreaseShareRecordProfit. increase miner profit by increment
func (this *ShareRecordDB) IncreaseShareRecordProfit(id string, profit uint64) error {
	sr := &ShareRecord{}
	key := []byte(ShareRecordKey(id))
	data, _ := this.db.Get(key)
	if len(data) > 0 {
		err := json.Unmarshal(data, &sr)
		if err != nil {
			return err
		}
	} else {
		sr.CreatedAt = uint64(time.Now().Unix())
	}
	sr.Profit += profit
	sr.UpdatedAt = uint64(time.Now().Unix())
	buf, err := json.Marshal(sr)
	if err != nil {
		return err
	}
	return this.db.Put(key, buf)
}

// FindShareRecordById. find miner record by id
func (this *ShareRecordDB) FindShareRecordById(id string) (*ShareRecord, error) {
	sr := &ShareRecord{}
	key := []byte(ShareRecordKey(id))
	data, _ := this.db.Get(key)
	if len(data) > 0 {
		err := json.Unmarshal(data, &sr)
		if err != nil {
			return nil, err
		}
		return sr, nil
	}
	return nil, fmt.Errorf("id not found %s", id)
}

type ShareRecords []*ShareRecord

func (s ShareRecords) Len() int {
	return len(s)
}
func (s ShareRecords) Less(i, j int) bool {
	return s[i].UpdatedAt < s[j].UpdatedAt
}

func (s ShareRecords) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

// // FineShareRecordsByCreatedAt. find miner record by createdat interval
func (this *ShareRecordDB) FineShareRecordsByCreatedAt(beginedAt, endedAt, offset, limit int64) ([]*ShareRecord, error) {
	prefix := []byte(ShareRecordKey(""))
	keys, err := this.db.QueryStringKeysByPrefix(prefix)
	if err != nil {
		return nil, err
	}
	srs := make(ShareRecords, 0, limit)
	for _, k := range keys {
		sr := &ShareRecord{}
		data, _ := this.db.Get([]byte(k))
		if len(data) == 0 {
			continue
		}
		err := json.Unmarshal(data, &sr)
		if err != nil {
			continue
		}
		if sr.CreatedAt < uint64(beginedAt) || sr.CreatedAt > uint64(endedAt) {
			continue
		}
		srs = append(srs, sr)
	}
	sort.Sort(sort.Reverse(srs))
	if limit == 0 {
		return srs[offset:], nil
	}
	end := offset + limit
	if end > int64(len(srs)) {
		end = int64(len(srs))
	}
	return srs[offset:end], nil
}

func (this *ShareRecordDB) FindLastShareTime(fileHash string) (uint64, error) {
	prefix := []byte(ShareRecordKey(""))
	keys, err := this.db.QueryStringKeysByPrefix(prefix)
	if err != nil {
		return 0, err
	}
	lastTime := uint64(0)
	for _, k := range keys {
		sr := &ShareRecord{}
		data, _ := this.db.Get([]byte(k))
		if len(data) == 0 {
			continue
		}
		err := json.Unmarshal(data, &sr)
		if err != nil {
			continue
		}
		if sr.FileHash != fileHash {
			continue
		}
		if sr.CreatedAt > lastTime {
			lastTime = sr.CreatedAt
		}
	}
	return lastTime, nil
}

func (this *ShareRecordDB) CountRecordByFileHash(fileHash string) (uint64, error) {
	prefix := []byte(ShareRecordKey(""))
	keys, err := this.db.QueryStringKeysByPrefix(prefix)
	if err != nil {
		return 0, err
	}
	cnt := uint64(0)
	for _, k := range keys {
		sr := &ShareRecord{}
		data, _ := this.db.Get([]byte(k))
		if len(data) == 0 {
			continue
		}
		err := json.Unmarshal(data, &sr)
		if err != nil {
			continue
		}
		if sr.FileHash != fileHash {
			continue
		}
		cnt++
	}
	return cnt, nil
}

// // SumRecordsProfit. sum profit off all files
func (this *ShareRecordDB) SumRecordsProfit() (uint64, error) {
	prefix := []byte(ShareRecordKey(""))
	keys, err := this.db.QueryStringKeysByPrefix(prefix)
	if err != nil {
		return 0, err
	}
	cnt := uint64(0)
	for _, k := range keys {
		sr := &ShareRecord{}
		data, _ := this.db.Get([]byte(k))
		if len(data) == 0 {
			continue
		}
		err := json.Unmarshal(data, &sr)
		if err != nil {
			continue
		}
		cnt += sr.Profit
	}
	return cnt, nil
}

// SumRecordsProfitByFileHash. sum profit by one files
func (this *ShareRecordDB) SumRecordsProfitByFileHash(fileHashStr string) (uint64, error) {
	prefix := []byte(ShareRecordKey(""))
	keys, err := this.db.QueryStringKeysByPrefix(prefix)
	if err != nil {
		return 0, err
	}
	profit := uint64(0)
	for _, k := range keys {
		sr := &ShareRecord{}
		data, _ := this.db.Get([]byte(k))
		if len(data) == 0 {
			continue
		}
		err := json.Unmarshal(data, &sr)
		if err != nil {
			continue
		}
		if sr.FileHash != fileHashStr {
			continue
		}
		profit += sr.Profit
	}
	return profit, nil
}

// // SumRecordsProfitByFileHash. sum profit by one files
func (this *ShareRecordDB) SumRecordsProfitById(id string) (uint64, error) {
	sr := &ShareRecord{}
	key := []byte(ShareRecordKey(id))
	data, _ := this.db.Get(key)
	if len(data) > 0 {
		err := json.Unmarshal(data, &sr)
		if err != nil {
			return 0, err
		}
		return sr.Profit, nil
	}
	return 0, fmt.Errorf("id not found %s", id)
}
