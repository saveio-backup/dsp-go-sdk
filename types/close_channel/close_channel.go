package closedchannel

import (
	"sync/atomic"
)

type ClosedChannel struct {
	ch       chan struct{}
	isClosed uint32
}

func New() *ClosedChannel {
	ch := make(chan struct{}, 0)
	return &ClosedChannel{
		ch: ch,
	}
}

func (this *ClosedChannel) IsClosed() bool {
	isClosed := atomic.LoadUint32(&this.isClosed)
	return isClosed == 1
}

func (this *ClosedChannel) C() <-chan struct{} {
	return this.ch
}

func (this *ClosedChannel) Fire() {
	atomic.StoreUint32(&this.isClosed, 0)
}

func (this *ClosedChannel) Close() {
	atomic.StoreUint32(&this.isClosed, 1)
	close(this.ch)
}
