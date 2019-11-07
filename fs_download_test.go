package dsp

import (
	"fmt"
	"io/ioutil"
	"os"
	"testing"
	"time"
)

func TestWriteAtWithOffset(t *testing.T) {
	filePath := fmt.Sprintf("./temp_%d", time.Now().Unix())
	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	defer os.Remove(filePath)
	content := "hello world"
	wrote, err := file.WriteAt([]byte(content), 0)
	if err != nil {
		t.Fatal(err)
	}
	if wrote != len(content) {
		t.Fatalf("len not match %d != %d", wrote, len(content))
	}
	wrote, err = file.WriteAt([]byte(content), int64(len(content)))
	if err != nil {
		t.Fatal(err)
	}
	if wrote != len(content) {
		t.Fatalf("len not match %d != %d", wrote, len(content))
	}
	buf, err := ioutil.ReadFile(filePath)
	if err != nil {
		t.Fatal(err)
	}
	if len(buf) != 2*len(content) {
		t.Fatalf("len not match %d != %d", len(buf), 2*len(content))
	}
}
