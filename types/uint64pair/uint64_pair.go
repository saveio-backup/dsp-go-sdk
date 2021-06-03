package uint64pair

type Uint64Pair struct {
	Key   string
	Value uint64
}

type Uint64PairList []Uint64Pair

func (p Uint64PairList) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
func (p Uint64PairList) Len() int           { return len(p) }
func (p Uint64PairList) Less(i, j int) bool { return p[i].Value < p[j].Value }
