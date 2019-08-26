package utils

import "sync"

type Queue struct {
	data []interface{}
	lock sync.Mutex
}

func New() *Queue {
	q := &Queue{}
	q.data = make([]interface{}, 0)
	return q
}

func (this *Queue) Push(v interface{}) {
	this.lock.Lock()
	defer this.lock.Unlock()
	this.data = append(this.data, v)
}

func (this *Queue) Pop() interface{} {
	this.lock.Lock()
	defer this.lock.Unlock()
	if len(this.data) == 0 {
		return nil
	}
	first := this.data[0]
	this.data = append(this.data[0:0], this.data[1:]...)
	return first
}

func (this *Queue) Len() int {
	this.lock.Lock()
	defer this.lock.Unlock()
	return len(this.data)
}

func (this *Queue) PopAll() []interface{} {
	this.lock.Lock()
	defer this.lock.Unlock()
	all := this.data
	this.data = this.data[:0]
	return all
}
