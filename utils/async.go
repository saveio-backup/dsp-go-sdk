package utils

import (
	"errors"
	"sync"
	"time"

	"github.com/saveio/dsp-go-sdk/common"
	"github.com/saveio/themis/common/log"
)

type RequestResponse struct {
	Result interface{}
	Code   uint32
	Error  string
}

// CallRequestWithArgs. Use N goroutines to dispatch a same request with different args. wait response of each request
func CallRequestWithArgs(request func([]interface{}, chan *RequestResponse), argsOfRequests [][]interface{}) []*RequestResponse {
	max := common.MAX_ASYNC_ROUTINES
	return RequestAllWithArgsLimited(max, request, argsOfRequests)
}

// CallRequestOneWithArgs. Use N goroutines to dispatch a same request with different args. wait response of each request
func CallRequestOneWithArgs(request func([]interface{}, chan *RequestResponse) bool, argsOfRequests [][]interface{}) []*RequestResponse {
	max := common.MAX_ASYNC_ROUTINES
	return RequestOneWithArgsLimited(max, request, argsOfRequests)
}

// CallRequestWithArgs. Use N goroutines to dispatch a same request with different args. wait response of each request
func RequestAllWithArgsLimited(routinesNum int, request func([]interface{}, chan *RequestResponse), argsOfRequests [][]interface{}) []*RequestResponse {
	max := routinesNum
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
	jobCh := make(chan []interface{}, 1)
	jobDone := make(chan struct{}, 1)
	// use dispatch job model
	go func() {
		// dispatcher
		for _, args := range argsOfRequests {
			jobCh <- args
		}
		close(jobCh)
	}()

	for i := 0; i < max; i++ {
		go func() {
			for {
				args, ok := <-jobCh
				if !ok {
					return
				}
				done := make(chan *RequestResponse, 1)
				request(args, done)
				resp := <-done
				lock.Lock()
				result = append(result, resp)
				if len(result) == len(argsOfRequests) {
					jobDone <- struct{}{}
				}
				lock.Unlock()
			}
		}()
	}
	<-jobDone
	return result
}

// RequestOneWithArgsLimited. Use N goroutines to dispatch a same request with different args. wait response of each request
func RequestOneWithArgsLimited(routinesNum int, request func([]interface{}, chan *RequestResponse) bool, argsOfRequests [][]interface{}) []*RequestResponse {
	max := routinesNum
	lock := new(sync.Mutex)
	result := make([]*RequestResponse, 0, len(argsOfRequests))

	jobCh := make(chan []interface{}, 1)
	jobDone := make(chan struct{}, 1)
	jobBreak := false
	jobClosed := false
	// use dispatch job model
	go func() {
		// dispatcher
		for _, args := range argsOfRequests {
			jobCh <- args
		}
		close(jobCh)
		jobClosed = true
		log.Debugf("dispatch done")
	}()

	for i := 0; i < max; i++ {
		go func() {
			for {
				args, ok := <-jobCh
				if !ok {
					return
				}
				done := make(chan *RequestResponse, 1)
				stop := request(args, done)
				resp := <-done
				lock.Lock()
				if jobBreak {
					log.Debugf("break job when stop")
					lock.Unlock()
					return
				}
				jobBreak = stop
				result = append(result, resp)
				if jobBreak || len(result) == len(argsOfRequests) {
					jobDone <- struct{}{}
				}
				lock.Unlock()
			}
		}()
	}
	<-jobDone
	if !jobClosed {
		// drain all pending jobs.
		// TODO: need a optimized way
		for {
			_, ok := <-jobCh
			if !ok {
				log.Debugf("break job dispatch")
				break
			}
		}
	}
	return result
}

type TimeoutFunc func() error

func DoWithTimeout(f TimeoutFunc, timeout time.Duration) error {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	done := make(chan error)
	go func() {
		err := f()
		done <- err
	}()
	select {
	case err := <-done:
		return err
	case <-timer.C:
		return errors.New("action timeout")
	}
}
