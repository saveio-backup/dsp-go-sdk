package store

import (
	"fmt"
	"testing"
)

func TestGetFileUploadInfo(t *testing.T) {
	dbPath := "../testdata/db1"
	db, err := NewLevelDBStore(dbPath)
	if err != nil || db == nil {
		return
	}
	fileDB := NewFileDB(db)
	info, err := fileDB.getFileInfo([]byte("123"))
	fmt.Printf("info:%v, err:%s\n", info, err)
}

func TestPutFileUploadInfo(t *testing.T) {
	dbPath := "../testdata/db1"
	db, err := NewLevelDBStore(dbPath)
	if err != nil || db == nil {
		return
	}
	fileDB := NewFileDB(db)
	err = fileDB.PutFileUploadInfo("1", "123", []byte("privatekey"))
	fmt.Printf("put err:%s\n", err)
}

func TestGetBlockOffset(t *testing.T) {
	dbPath := "../testdata/db1"
	db, err := NewLevelDBStore(dbPath)
	if err != nil || db == nil {
		return
	}
	fileDB := NewFileDB(db)
	if fileDB == nil {
		t.Fatal("db is nil")
		return
	}
	off, err := fileDB.BlockOffset("QmUQTgbTc1y4a8cq1DyA548B71kSrnVm7vHuBsatmnMBib-AYMnqA65pJFKAbbpD8hi5gdNDBmeFBy5hS-2", "zb2rhiZuEBDquFNx7YcDHgRgsHEiyxjnvimq9XZawLeaJKgWc", 2)
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
	fileDB := NewFileDB(db)
	nodes := fileDB.GetUploadedBlockNodeList("QmQgTa5UDCfBBokfvi4UBCPx9FkpWCaqEer9f59hE7EyTr", "QmQgTa5UDCfBBokfvi4UBCPx9FkpWCaqEer9f59hE7EyTr", 0)
	fmt.Printf("nodes:%v\n", nodes)
}

func TestGetUndownloadedBlockIndex(t *testing.T) {
	dbPath := "../testdata/db3"
	db, err := NewLevelDBStore(dbPath)
	if err != nil || db == nil {
		return
	}
	fileDB := NewFileDB(db)
	if fileDB == nil {
		return
	}
	fileHashStr := "QmUQTgbTc1y4a8cq1DyA548B71kSrnVm7vHuBsatmnMBib"
	hash, index, err := fileDB.GetUndownloadedBlockInfo(fileHashStr, fileHashStr)
	fmt.Printf("undownloaded hash:%s index:%d, err :%s\n", hash, index, err)
}

func TestGetPrefix(t *testing.T) {
	dbPath := "../testdata/db1"
	db, err := NewLevelDBStore(dbPath)
	if err != nil || db == nil {
		return
	}
	fileDB := NewFileDB(db)
	if fileDB == nil {
		fmt.Printf("DB is nil\n")
		return
	}
	pre := fileDB.FilePrefix("QmUQTgbTc1y4a8cq1DyA548B71kSrnVm7vHuBsatmnMBib")
	fmt.Printf("prefix :%s\n", pre)
}

func TestDeleteUnpaid(t *testing.T) {
	dbPath := "../testdata/db1"
	db, err := NewLevelDBStore(dbPath)
	if err != nil || db == nil {

		return
	}
	fileDB := NewFileDB(db)
	if fileDB == nil {
		fmt.Printf("DB is nil\n")
		return
	}
	can, err := fileDB.CanShareTo("QmUQTgbTc1y4a8cq1DyA548B71kSrnVm7vHuBsatmnMBib", "AGeTrARjozPVLhuzMxZq36THMtvsrZNAHq")
	fmt.Printf("can share %t, err: %s\n", can, err)
	// err = fileDB.DeleteShareFileUnpaid("QmUQTgbTc1y4a8cq1DyA548B71kSrnVm7vHuBsatmnMBib", "AGeTrARjozPVLhuzMxZq36THMtvsrZNAHq", 2, 158)
	// if err != nil {
	// 	t.Fatal(err)
	// }
	// can, err = fileDB.CanShareTo("QmUQTgbTc1y4a8cq1DyA548B71kSrnVm7vHuBsatmnMBib", "AGeTrARjozPVLhuzMxZq36THMtvsrZNAHq")
	// fmt.Printf("can share %t, err: %s\n", can, err)
}

func TestAllDownloadFiles(t *testing.T) {
	dbPath := "../testdata/db1"
	db, err := NewLevelDBStore(dbPath)
	if err != nil || db == nil {

		return
	}
	fileDB := NewFileDB(db)
	if fileDB == nil {
		fmt.Printf("DB is nil\n")
		return
	}
	files, err := fileDB.AllDownloadFiles()
	fmt.Printf("files %s, err: %s\n", files, err)
}
