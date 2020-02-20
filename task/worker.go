package task

import (
	"sync"

	"github.com/saveio/dsp-go-sdk/common"
	"github.com/saveio/dsp-go-sdk/network/message/types/block"
	"github.com/saveio/dsp-go-sdk/utils"
)

type jobFunc func(string, string, string, string, []*block.Block) ([]*BlockResp, error)

type Worker struct {
	id          string            // worker network peer id
	activeTime  uint64            // worker active timestamp, millisecond
	lock        *sync.RWMutex     // lock for private variables
	remoteAddr  string            // worker remote host addr
	walletAddr  string            // worker wallet addr
	working     bool              // flag of working or not of worker
	job         jobFunc           // job callback function
	failed      map[string]int    // map nodeAddr <=> request failed count
	unpaid      bool              // flag of unpaid or not of worker
	totalFailed map[string]uint32 // map fileHash <=> all request failed count
}

func NewWorker(addr, walletAddr string, j jobFunc) *Worker {
	w := &Worker{
		remoteAddr:  addr,
		walletAddr:  walletAddr,
		job:         j,
		failed:      make(map[string]int, 0),
		totalFailed: make(map[string]uint32, 0),
		activeTime:  utils.GetMilliSecTimestamp(),
		lock:        new(sync.RWMutex),
	}
	return w
}

func (w *Worker) SetID(id string) {
	w.id = id
}

func (w *Worker) Do(taskId, fileHash, peerAddr, walletAddr string, blocks []*block.Block) ([]*BlockResp, error) {
	w.working = true
	resp, err := w.job(taskId, fileHash, peerAddr, walletAddr, blocks)
	if err != nil {
		if len(blocks) > 0 {
			blockHash := blocks[0].Hash
			cnt := w.failed[blockHash]
			w.failed[blockHash] = cnt + 1
		}

		totalF := w.totalFailed[fileHash]
		w.totalFailed[fileHash] = totalF + 1
	}
	w.working = false
	return resp, err
}

func (w *Worker) RemoteAddress() string {
	return w.remoteAddr
}

func (w *Worker) WalletAddr() string {
	return w.walletAddr
}

func (w *Worker) Working() bool {
	return w.working
}

func (w *Worker) WorkFailed(hash string) bool {
	cnt, ok := w.failed[hash]
	if !ok {
		return false
	}
	if cnt >= common.MAX_WORKER_BLOCK_FAILED_NUM {
		return true
	}
	return false
}

func (w *Worker) FailedTooMuch(fileHashStr string) bool {
	return w.totalFailed[fileHashStr] >= common.MAX_WORKER_FILE_FAILED_NUM
}

func (w *Worker) SetUnpaid(unpaid bool) {
	w.unpaid = unpaid
}

func (w *Worker) SetWorking(working bool) {
	w.working = working
}

func (w *Worker) Unpaid() bool {
	return w.unpaid
}

// Active. make the worker active
func (w *Worker) Active() {
	w.lock.Lock()
	defer w.lock.Unlock()
	w.activeTime = utils.GetMilliSecTimestamp()
}

// ActiveTime. get last active time in millisecond
func (w *Worker) ActiveTime() uint64 {
	w.lock.RLock()
	defer w.lock.RUnlock()
	return w.activeTime
}
