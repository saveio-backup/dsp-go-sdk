package ticker

import (
	"fmt"
	"sync"
	"time"
)

type Ticker struct {
	duration time.Duration
	running  bool
	l        *sync.Mutex
	f        func() bool
}

func NewTicker(duration time.Duration, f func() bool) *Ticker {
	return &Ticker{
		duration: duration,
		l:        new(sync.Mutex),
		f:        f,
	}
}

func (t *Ticker) Run() {
	if t.isRunning() {
		return
	}
	fmt.Println("is running is false")
	t.setRunning(true)
	go func() {
		fmt.Printf("gogogo\n")
		tick := time.NewTicker(t.duration)
		defer tick.Stop()
		for {
			select {
			case <-tick.C:
				if !t.isRunning() {
					return
				}
				if t.f() {
					t.setRunning(false)
					return
				}
			}
		}
	}()
}

func (t *Ticker) Stop() {
	t.setRunning(false)
}

func (t *Ticker) isRunning() bool {
	t.l.Lock()
	defer t.l.Unlock()
	return t.running
}

func (t *Ticker) setRunning(r bool) {
	t.l.Lock()
	defer t.l.Unlock()
	t.running = r
}
