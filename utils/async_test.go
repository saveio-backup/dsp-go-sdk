package utils

import (
	"fmt"
	"math/rand"
	"testing"
	"time"
)

func TestCallRequestWithArgs(t *testing.T) {
	delayFunc := func(a []interface{}, resp chan *RequestResponse) {
		r := rand.Uint32()%10 + 1
		fmt.Printf("sleep for %d\n", r)
		time.Sleep(time.Duration(r) * time.Second)
		resp <- &RequestResponse{}
	}
	args := make([][]interface{}, 5)
	fmt.Printf("start %d\n", time.Now().Unix())
	resps := CallRequestWithArgs(delayFunc, args)
	fmt.Printf("end: %d, resps: %v\n", time.Now().Unix(), resps)
	time.Sleep(time.Minute)
}
