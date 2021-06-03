package fs

type InitFsOption struct {
	RepoRoot   string
	FsType     int
	ChunkSize  uint64
	GcPeriod   string
	MaxStorage string
}

type FsOption interface {
	apply(*InitFsOption)
}

type FsOptFunc func(*InitFsOption)

func (f FsOptFunc) apply(c *InitFsOption) {
	f(c)
}

func RepoRoot(root string) FsOption {
	return FsOptFunc(func(o *InitFsOption) {
		o.RepoRoot = root
	})
}

func FsType(fsType int) FsOption {
	return FsOptFunc(func(o *InitFsOption) {
		o.FsType = fsType
	})
}

func ChunkSize(chunkSize uint64) FsOption {
	return FsOptFunc(func(o *InitFsOption) {
		o.ChunkSize = chunkSize
	})
}

func GcPeriod(period string) FsOption {
	return FsOptFunc(func(o *InitFsOption) {
		o.GcPeriod = period
	})
}

func MaxStorage(max string) FsOption {
	return FsOptFunc(func(o *InitFsOption) {
		o.MaxStorage = max
	})
}
