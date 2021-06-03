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

// GetOrStore returns the existing value for the key if present.
// Otherwise, it stores and returns the given value.
// The loaded result is true if the value was loaded, false if stored.
func (this *SyncState) GetOrStore(newState ModuleState) (ModuleState, bool) {
	this.l.Lock()
	defer this.l.Unlock()
	if this.s == newState {
		return this.s, true
	}
	this.s = newState
	return newState, false
}
