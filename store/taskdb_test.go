package store

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"testing"
)

var localTestDBPath = "./testDB"

func TestGetFileUploadInfo(t *testing.T) {
	dbPath := "../testdata/db1"
	db, err := NewLevelDBStore(dbPath)
	if err != nil || db == nil {
		return
	}
	fileDB := NewTaskDB(db)
	fmt.Printf("DB: %v\n", fileDB)
}

func TestPutFileUploadInfo(t *testing.T) {
	dbPath := "../testdata/db1"
	db, err := NewLevelDBStore(dbPath)
	if err != nil || db == nil {
		return
	}
	fileDB := NewTaskDB(db)
	fmt.Printf("DB: %v\n", fileDB)
}

func TestPutLargeSlice(t *testing.T) {
	err := os.MkdirAll(localTestDBPath, 0755)
	if err != nil {
		t.Fatal(err)
	}
	db, err := NewLevelDBStore(localTestDBPath)
	if err != nil || db == nil {
		t.Fatal(err)
	}
	fileDB := NewTaskDB(db)
	largeDBSliceKey := "largeDBSliceKey"

	sliceStr := "00e43174-dab8-11e9-8736-e470b8115fb3"
	largeSlice := make([]string, 0)
	testLen := 1000000
	for i := 0; i < testLen; i++ {
		largeSlice = append(largeSlice, sliceStr)
	}
	largeSliceBuf, err := json.Marshal(largeSlice)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("largeSliceLen: %d, buf size: %dMB\n", len(largeSlice), len(largeSliceBuf)/1024/1024)
	err = fileDB.db.Put([]byte(largeDBSliceKey), largeSliceBuf)
	if err != nil {
		t.Fatal(err)
	}
	largeSliceBuf2, err := fileDB.db.Get([]byte(largeDBSliceKey))
	if err != nil {
		t.Fatal(err)
	}
	largeSlice2 := make([]string, 0)
	err = json.Unmarshal(largeSliceBuf2, &largeSlice2)
	if err != nil {
		t.Fatal(err)
	}

	if len(largeSlice2) != testLen {
		t.Fatalf("recover test len not equal to %d", testLen)
	}
	err = os.RemoveAll(localTestDBPath)
	if err != nil {
		t.Fatal(err)
	}
}

func TestGetBlockOffset(t *testing.T) {
	dbPath := "../testdata/db1"
	db, err := NewLevelDBStore(dbPath)
	if err != nil || db == nil {
		return
	}
	fileDB := NewTaskDB(db)
	if fileDB == nil {
		t.Fatal("db is nil")
		return
	}
	off, err := fileDB.GetBlockOffset("QmUQTgbTc1y4a8cq1DyA548B71kSrnVm7vHuBsatmnMBib-AYMnqA65pJFKAbbpD8hi5gdNDBmeFBy5hS-2", "zb2rhiZuEBDquFNx7YcDHgRgsHEiyxjnvimq9XZawLeaJKgWc", 2)
	if err != nil {
		fmt.Printf("ERR :%s\n", err)
		return
	}
	fmt.Printf("offset:%d\n", off)
}

func TestGetUploadedBlockNodeList(t *testing.T) {
	dbPath := "../testdata/db1"
	db, err := NewLevelDBStore(dbPath)
	if err != nil || db == nil {
		return
	}
	fileDB := NewTaskDB(db)
	nodes := fileDB.GetUploadedBlockNodeList("QmQgTa5UDCfBBokfvi4UBCPx9FkpWCaqEer9f59hE7EyTr", "QmQgTa5UDCfBBokfvi4UBCPx9FkpWCaqEer9f59hE7EyTr", 0)
	fmt.Printf("nodes:%v\n", nodes)
}

func TestGetPrefix(t *testing.T) {
	dbPath := "../testdata/db1"
	db, err := NewLevelDBStore(dbPath)
	if err != nil || db == nil {
		return
	}
	fileDB := NewTaskDB(db)
	if fileDB == nil {
		fmt.Printf("DB is nil\n")
		return
	}
}

func TestDeleteUnpaid(t *testing.T) {
	dbPath := "../testdata/db1"
	db, err := NewLevelDBStore(dbPath)
	if err != nil || db == nil {
		return
	}
	fileDB := NewTaskDB(db)
	if fileDB == nil {
		fmt.Printf("DB is nil\n")
		return
	}
	amount, err := fileDB.GetUnpaidAmount("", "AGeTrARjozPVLhuzMxZq36THMtvsrZNAHq", 0)
	fmt.Printf("unpaid amount %d, err: %s\n", amount, err)
}

func TestAllDownloadFiles(t *testing.T) {
	dbPath := "../testdata/db1"
	db, err := NewLevelDBStore(dbPath)
	if err != nil || db == nil {
		return
	}
	fileDB := NewTaskDB(db)
	if fileDB == nil {
		fmt.Printf("DB is nil\n")
		return
	}
	files, _, err := fileDB.AllDownloadFiles()
	fmt.Printf("files %v, err: %s\n", files, err)
}

func TestMarshalFileInfo(t *testing.T) {
	testM := make(map[string]uint64, 0)
	testM["123"] = 1
	info := &TaskInfo{
		Id:              "92d25944-cbb9-11e9-8340-acde48001122",
		FileHash:        "QmbQLV1jU5oCrEvbHwvmPR2Dy2yHBa71hQ3MweCYu5ubC4",
		FileName:        "2019-08-23_12.17.42_LOG.log",
		FilePath:        "./2019-08-23_12.17.42_LOG.log",
		FileOwner:       "AY46Kes2ayy8c38hKBqictG9F9ar73mqhD",
		CopyNum:         2,
		Type:            0,
		StoreTx:         "27db5bae0138cb7a42ce161a57c1bdc514eef40a5a4ea1b9893b7fb24af74d8d",
		TotalBlockCount: 82,
		TaskState:       1,
		Prefix:          nil,
		EncryptSalt:     "",
		EncryptHash:     "",
		Url:             "",
		Link:            "",
		CurrentBlock:    "",
		CurrentIndex:    0,
		StoreType:       0,
		InOrder:         false,
		OnlyBlock:       false,
		TranferState:    12,
		CreatedAt:       1567233350,
		CreatedAtHeight: 0,
		UpdatedAt:       1567233367,
		Result:          "123",
	}
	buf, err := json.Marshal(info)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("buf lenL: %d\n", len(buf))
	info2 := &TaskInfo{}
	err = json.Unmarshal(buf, info2)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("Id: %v\n", info2.Id)
	fmt.Printf("FileHash: %v\n", info2.FileHash)
	fmt.Printf("FileName: %v\n", info2.FileName)
	fmt.Printf("FilePath: %v\n", info2.FilePath)
	fmt.Printf("FileOwner: %v\n", info2.FileOwner)
	fmt.Printf("CopyNum: %v\n", info2.CopyNum)
	fmt.Printf("InfoType: %v\n", info2.Type)
	fmt.Printf("StoreTx: %v\n", info2.StoreTx)
	fmt.Printf("TotalBlockCount: %v\n", info2.TotalBlockCount)
	fmt.Printf("TaskState: %v\n", info2.TaskState)
	fmt.Printf("Prefix: %v\n", info2.Prefix)
	fmt.Printf("EncryptSalt: %v\n", info2.EncryptSalt)
	fmt.Printf("EncryptHash: %v\n", info2.EncryptHash)
	fmt.Printf("Url: %v\n", info2.Url)
	fmt.Printf("Link: %v\n", info2.Link)
	fmt.Printf("CurrentBlock: %v\n", info2.CurrentBlock)
	fmt.Printf("CurrentIndex: %v\n", info2.CurrentIndex)
	fmt.Printf("StoreType: %v\n", info2.StoreType)
	fmt.Printf("InOrder: %v\n", info2.InOrder)
	fmt.Printf("OnlyBlock: %v\n", info2.OnlyBlock)
	fmt.Printf("TranferState: %v\n", info2.TranferState)
	fmt.Printf("CreatedAt: %v\n", info2.CreatedAt)
	fmt.Printf("CreatedAtHeight: %v\n", info2.CreatedAtHeight)
	fmt.Printf("UpdatedAt: %v\n", info2.UpdatedAt)
	fmt.Printf("Result: %v\n", info2.Result)
}

func TestSort(t *testing.T) {
	taskInfo := &TaskInfo{
		Id:        "1",
		CreatedAt: 5,
		UpdatedAt: 2,
	}

	taskInfo2 := &TaskInfo{
		Id:        "2",
		CreatedAt: 2,
		UpdatedAt: 3,
	}

	infos := make(TaskInfos, 0)
	infos = append(infos, taskInfo)
	infos = append(infos, taskInfo2)

	sort.Sort(TaskInfosByCreatedAt(infos))
	for _, info := range infos {
		fmt.Printf("info id = %s, created %d, updated %d\n", info.Id, info.CreatedAt, info.UpdatedAt)
	}
}

func TestJsonOmit(t *testing.T) {
	obj := &TaskInfo{
		CopyNum: 1,
	}
	data, err := json.Marshal(obj)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("json str %s\n", string(data))
	obj = &TaskInfo{}
	json.Unmarshal(data, &obj)
	fmt.Printf("desc %v\n", obj.CopyNum)
	obj = &TaskInfo{}
	data, err = json.Marshal(obj)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("json str %s\n", string(data))
}

func TestPrintTaskState(t *testing.T) {
	fmt.Printf("state %v\n", TaskStateNone)
	fmt.Printf("state %v\n", TaskStatePause)
	fmt.Printf("state %v\n", TaskStatePrepare)
	fmt.Printf("state %v\n", TaskStateDoing)
	fmt.Printf("state %v\n", TaskStateDone)
	fmt.Printf("state %v\n", TaskStateFailed)
	fmt.Printf("state %v\n", TaskStateCancel)
	fmt.Printf("state %v\n", TaskStateIdle)
}
