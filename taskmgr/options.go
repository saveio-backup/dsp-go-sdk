package taskmgr

import "github.com/saveio/dsp-go-sdk/store"

type MgrOption interface {
	apply(*TaskMgr)
}

type MgrOptionFunc func(*TaskMgr)

func (f MgrOptionFunc) apply(d *TaskMgr) {
	f(d)
}

func TaskDB(d *store.LevelDBStore) MgrOptionFunc {
	return MgrOptionFunc(func(t *TaskMgr) {
		t.db = store.NewTaskDB(d)
		t.shareRecordDB = store.NewShareRecordDB(d)
	})
}
