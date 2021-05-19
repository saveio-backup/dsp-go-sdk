package dsp

import (
	"crypto/md5"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/saveio/themis/common/log"
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

func TestWriteConcurrent(t *testing.T) {
	filePath := fmt.Sprintf("./temp_%d", time.Now().Unix())
	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	defer os.Remove(filePath)
	chunkSize := 256 * 1024
	totalBlock := 50
	routineCnt := 5

	buf := make([]byte, totalBlock*chunkSize)
	datas := make([][]byte, 0)
	for i := 0; i < totalBlock; i++ {
		datas = append(datas, buf[i*chunkSize:(i+1)*chunkSize])
	}
	md5Hash := md5.Sum(buf[:])
	fmt.Printf("md5: %x\n", md5Hash)
	wg := new(sync.WaitGroup)
	rand.Seed(time.Now().UnixNano())

	perCnt := totalBlock / routineCnt
	for i := 0; i < routineCnt; i++ {
		wg.Add(1)
		blks := make([][]byte, 0)

		blks = append(blks, datas[i*perCnt:(i+1)*perCnt]...)
		go func(blocks [][]byte, round int) {
			defer wg.Done()
			rand.Shuffle(len(blocks), func(i, j int) { blocks[i], blocks[j] = blocks[j], blocks[i] })
			for index, b := range blocks {
				pos := int64((round*perCnt + index) * chunkSize)
				file.WriteAt(b, pos)
				log.Infof("write at %d", pos)
			}

		}(blks, i)
	}
	wg.Wait()

	fileData, _ := ioutil.ReadFile(filePath)
	md5Hash = md5.Sum(fileData[:])
	fmt.Printf("md5: %x\n", md5Hash)
}

func TestWgLimiter(t *testing.T) {
	wg := new(sync.WaitGroup)

	go func() {
		<-time.After(time.Duration(2) * time.Second)
		fmt.Println("wg done")
		wg.Done()
	}()
	wg.Add(1)
	wg.Wait()
	fmt.Println("out+++")
	wg.Wait()
	fmt.Println("finish")
}
