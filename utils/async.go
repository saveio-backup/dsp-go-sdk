package utils

import (
	"sync"

	"github.com/saveio/dsp-go-sdk/common"
)

type RequestResponse struct {
	Result interface{}
	Code   uint32
	Error  string
}

// CallRequestWithArgs. Use N gotinues to dispatch a same request with different args. wait response of each request
func CallRequestWithArgs(request func([]interface{}, chan *RequestResponse), argsOfRequests [][]interface{}) []*RequestResponse {
	max := common.MAX_ASYNC_ROUTINES
	lock := new(sync.Mutex)
	result := make([]*RequestResponse, 0, len(argsOfRequests))
	if len(argsOfRequests) <= max {
		// if request len is small, use wait group
		wg := new(sync.WaitGroup)
		for _, args := range argsOfRequests {
			wg.Add(1)
			go func(a []interface{}) {
				done := make(chan *RequestResponse, 1)
				request(a, done)
				resp := <-done
				wg.Done()
				lock.Lock()
				defer lock.Unlock()
				result = append(result, resp)
			}(args)
		}
		wg.Wait()
		return result
	}
	jobCh := make(chan []interface{}, max)
	jobDone := make(chan struct{}, 1)
	// use dispatch job model
	go func() {
		// dispatcher
		for _, args := range argsOfRequests {
			jobCh <- args
		}
		close(jobCh)
		close(jobDone)
	}()
	for i := 0; i < max; i++ {
		go func() {
			args, ok := <-jobCh
			if !ok {
				return
			}
			done := make(chan *RequestResponse, 1)
			request(args, done)
			resp := <-done
			lock.Lock()
			defer lock.Unlock()
			result = append(result, resp)
		}()
	}
	<-jobDone
	return result
}
