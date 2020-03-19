package state

import "sync"

type ModuleState int

const (
	ModuleStateNone ModuleState = iota
	ModuleStateStarting
	ModuleStateStarted
	ModuleStateActive
	ModuleStateStopping
	ModuleStateStopped
	ModuleStateError
)

type SyncState struct {
	s ModuleState
	l *sync.Mutex
}

func NewSyncState() *SyncState {
	return &SyncState{
		s: ModuleStateNone,
		l: new(sync.Mutex),
	}
}

func (this *SyncState) Set(s ModuleState) {
	this.l.Lock()
	defer this.l.Unlock()
	this.s = s
}

func (this *SyncState) Get() ModuleState {
	this.l.Lock()
	defer this.l.Unlock()
	return this.s
}
