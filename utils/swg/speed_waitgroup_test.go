package swg

import (
	"os"
	"testing"
	"time"

	"github.com/saveio/themis/common/log"
)

func TestSWG(t *testing.T) {

	log.InitLog(1, os.Stdout)
	s := NewSWG(time.Second)
	go s.Start()

	s.Add()
	time.Sleep(time.Duration(5) * time.Second)
	s.Wait()

}
