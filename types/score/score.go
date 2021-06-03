// QoS Score Util
//
// The score is identified by a unique id, which can be a URL, a wallet address, a UUID, or a host address.
// The score will record the server's failed times, timeout times, bad times, can use a weighting factor algorithm
// to calculate its score. And the scores slice can be sort descending.
//
package score

import "fmt"

type Score struct {
	id      string
	failed  int
	timeout int
	result  int
}

// NewScore. init a new score
func NewScore(id string) *Score {
	return &Score{
		id: id,
	}
}

// Id. score id
func (s *Score) Id() string {
	return s.id
}

// String. price score with a readable text
func (s *Score) String() string {
	return fmt.Sprintf("{failed: %d, timeout: %d, bad: %d}", s.failed, s.timeout, s.failed+s.timeout*2)
}

// Fail. increase the failed count of the score
func (s *Score) Fail() {
	s.failed++
}

// Timeout. increase the timeout count of the score
func (s *Score) Timeout() {
	s.timeout++
}

// Reset. reset the failed and timeout count
func (s *Score) Reset() {
	s.failed = 0
	s.timeout = 0
}

// Result. calculate the final score result. The highest score is 100, the lowest score is 0.
func (s *Score) Result() float64 {
	if s.failed+s.timeout*2 >= 100 {
		return 0
	}
	return 100.0 - float64(s.failed+s.timeout*2)
}

type Scores []Score

func (s Scores) Len() int {
	return len(s)
}

func (s Scores) Less(i, j int) bool {
	badScoreI := (s[i].failed*1 + s[i].timeout*2)
	badScoreJ := (s[j].failed*1 + s[j].timeout*2)
	return badScoreI > badScoreJ
}

func (s Scores) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}
