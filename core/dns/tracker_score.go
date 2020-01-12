package dns

import (
	"fmt"
)

type TrackerScore struct {
	url     string
	failed  uint32
	timeout uint32
}

func (s *TrackerScore) String() string {
	if s == nil {
		return "{}"
	}
	return fmt.Sprintf("{failed: %d, timeout: %d, bad: %d}", s.failed, s.timeout, s.failed+s.timeout*2)
}

type TrackerScores []TrackerScore

func (s TrackerScores) Len() int {
	return len(s)
}

func (s TrackerScores) Less(i, j int) bool {
	badScoreI := (s[i].failed*1 + s[i].timeout*2)
	badScoreJ := (s[j].failed*1 + s[j].timeout*2)
	return badScoreI > badScoreJ
}

func (s TrackerScores) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}
