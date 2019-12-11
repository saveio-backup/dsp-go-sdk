package ticker

import (
	"context"
	"sync"
	"time"
)

type Ticker struct {
	duration time.Duration
	running  bool
	l        *sync.Mutex
	ctx      context.Context
	f        func() bool
}

func NewTicker(duration time.Duration, f func() bool) *Ticker {
	return &Ticker{
		duration: duration,
		l:        new(sync.Mutex),
		f:        f,
		ctx:      context.Background(),
	}
}

func (t *Ticker) Context() context.Context {
	return t.ctx
}

func (t *Ticker) Run() {
	if t.isRunning() {
		return
	}
	t.setRunning(true)
	go func() {
		tick := time.NewTicker(t.duration)
		defer func() {
			tick.Stop()
		}()
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
