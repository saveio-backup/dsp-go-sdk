package swg

import (
	"math/rand"
	"sync"
	"time"

	"github.com/saveio/themis/common/log"
)

type SWG struct {
	id        int32
	lock      *sync.Mutex
	wg        *sync.WaitGroup
	doing     bool
	close     chan struct{}
	limitTime time.Duration
}

func NewSWG(limitTime time.Duration) *SWG {
	s := &SWG{
		id:        rand.Int31(),
		lock:      new(sync.Mutex),
		wg:        new(sync.WaitGroup),
		doing:     false,
		close:     make(chan struct{}),
		limitTime: limitTime,
	}
	log.Debugf("create swg id: %v", s.id)

	return s
}

func (s *SWG) Close() {
	log.Debugf("swg id: %v closed", s.id)
	close(s.close)
}

func (s *SWG) Start() {

	speedLimitTicker := time.NewTicker(s.limitTime)
	defer speedLimitTicker.Stop()
	for {
		select {
		case <-speedLimitTicker.C:
			s.lock.Lock()
			if s.doing {
				s.wg.Done()
				s.doing = false
				log.Debugf("swg id: %v set done", s.id)
			}
			s.lock.Unlock()
		case <-s.close:
			return
		}
	}

}

func (s *SWG) Add() {
	s.lock.Lock()
	if !s.doing {
		s.wg.Add(1)
		s.doing = true
		log.Debugf("swg id: %v set added", s.id)
	}
	s.lock.Unlock()
}

func (s *SWG) Wait() {
	s.wg.Wait()
}
