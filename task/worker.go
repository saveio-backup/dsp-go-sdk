package task

type jobFunc func(string, string, string, int32, chan *BlockResp) (*BlockResp, error)

type Worker struct {
	remoteAddr string
	working    bool
	job        jobFunc
	failed     map[string]struct{}
}

func NewWorker(addr string, j jobFunc) *Worker {
	w := &Worker{}
	w.remoteAddr = addr
	w.job = j
	w.failed = make(map[string]struct{}, 0)
	return w
}

func (w *Worker) Do(fileHash, blockHash, peerAddr string, index int32, respCh chan *BlockResp) (*BlockResp, error) {
	w.working = true
	resp, err := w.job(fileHash, blockHash, peerAddr, index, respCh)
	if err != nil {
		w.failed[blockHash] = struct{}{}
	}
	w.working = false
	return resp, err
}

func (w *Worker) RemoteAddress() string {
	return w.remoteAddr
}

func (w *Worker) Working() bool {
	return w.working
}

func (w *Worker) WorkFailed(hash string) bool {
	_, ok := w.failed[hash]
	return ok
}
