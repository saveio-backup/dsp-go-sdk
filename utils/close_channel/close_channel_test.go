package closedchannel

import (
	"fmt"
	"testing"
	"time"
)

func TestCloseChannel(t *testing.T) {
	closed := New()

	go func() {
		for {
			v1 := <-closed.C()
			fmt.Printf("close1 : %v\n", v1)
		}

	}()
	go func() {
		for {
			v2 := <-closed.C()
			fmt.Printf("close2 : %v\n", v2)
		}
	}()

	<-time.After(time.Duration(3) * time.Second)
	closed.Close()
	<-time.After(time.Duration(3) * time.Second)
	closed.Close()
	closed.Fire()
	closed.Close()
	<-time.After(time.Duration(3) * time.Second)
}
