package task

import (
	"fmt"
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/saveio/dsp-go-sdk/store"
	"github.com/saveio/themis/common/log"
)

func TestSetManyTaskDownloaded(t *testing.T) {
	tskMgr := NewTaskMgr(nil)
	dbPath := "./testDB"
	defer os.RemoveAll(dbPath)
	log.InitLog(0, log.Stdout)
	levelDBStore, err := store.NewLevelDBStore(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	tskMgr.SetFileDB(levelDBStore)
	randomMaker := rand.New(rand.NewSource(time.Now().UnixNano()))
	tskIdM := make(map[int]string, 0)
	tskCount := 3
	blkCount := 5
	for i := 0; i < tskCount; i++ {
		id, err := tskMgr.NewTask("", store.TaskTypeDownload)
		if err != nil {
			t.Fatal(err)
		}
		tskMgr.SetFileName(id, fmt.Sprintf("%d", i))
		tskIdM[i] = id
	}
	blockCounter := make(map[int]int)
	wg := new(sync.WaitGroup)
	for i := 0; i < tskCount*blkCount; i++ {
		idIdx := int(randomMaker.Int31n(int32(tskCount)))
		taskId := tskIdM[idIdx]
		cnt := blockCounter[idIdx]
		blockCounter[idIdx]++
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			if err := tskMgr.SetBlockDownloaded(taskId, fmt.Sprintf("%v-%v", idIdx, index), "node", uint64(index), 0, nil); err != nil {
				t.Fatal(err)
			}
		}(cnt)
	}
	wg.Wait()
	log.Infof("test done")

}
