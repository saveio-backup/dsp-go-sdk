package ticker

import (
	"fmt"
	"math/rand"
	"testing"
	"time"
)

func TestTicker(t *testing.T) {
	r := rand.New(rand.NewSource(time.Now().Unix()))
	f := func() bool {
		v := r.Int31()
		fmt.Printf("v=%d, stop: %t\n", v, v%3 == 0)
		if v%8 == 0 {
			return true
		}
		return false
	}
	ticker := NewTicker(time.Duration(2)*time.Second, f)
	ticker.Run()
	<-time.After(time.Duration(5) * time.Second)
	ticker.Run()
	<-time.After(time.Minute)
}
