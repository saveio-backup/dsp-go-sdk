package store

import (
	"fmt"
	"testing"
)

func TestGetFileUploadInfo(t *testing.T) {
	fileDB := NewFileDB("./db")
	info, err := fileDB.getFileInfo("123", FileInfoTypeUpload)
	fmt.Printf("info:%v, err:%s\n", info, err)
}

func TestPutFileUploadInfo(t *testing.T) {
	fileDB := NewFileDB("./db")
	err := fileDB.PutFileUploadInfo("1", "123", []byte("privatekey"))
	fmt.Printf("put err:%s\n", err)
}
