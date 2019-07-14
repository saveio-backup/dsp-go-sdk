package task

import "github.com/saveio/dsp-go-sdk/common"

type jobFunc func(string, string, string, string, int32) (*BlockResp, error)

type Worker struct {
	remoteAddr string
	working    bool
	job        jobFunc
	failed     map[string]int
	unpaid     bool
}

func NewWorker(addr string, j jobFunc) *Worker {
	w := &Worker{}
	w.remoteAddr = addr
	w.job = j
	w.failed = make(map[string]int, 0)
	return w
}

func (w *Worker) Do(taskId, fileHash, blockHash, peerAddr string, index int32) (*BlockResp, error) {
	w.working = true
	resp, err := w.job(taskId, fileHash, blockHash, peerAddr, index)
	if err != nil {
		cnt := w.failed[blockHash]
		w.failed[blockHash] = cnt + 1
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
	cnt, ok := w.failed[hash]
	if !ok {
		return false
	}
	if cnt >= common.MAX_WORKER_FAILED_NUM {
		return true
	}
	return false
}

func (w *Worker) SetUnpaid(unpaid bool) {
	w.unpaid = unpaid
}

func (w *Worker) Unpaid() bool {
	return w.unpaid
}
