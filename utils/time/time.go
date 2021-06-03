package time

import "time"

// NowMillisecond. get current millisecond second timestamp
func NowMillisecond() uint64 {
	return uint64(time.Now().UnixNano() / int64(time.Millisecond))
}

// MilliSecBefore. check if past + cap > now
func MilliSecBefore(now, past, cap uint64) bool {
	return past+cap > now
}

// MilliSecEqual. check if past + cap == now
func MilliSecEqual(now, past, cap uint64) bool {
	return past+cap == now
}

// MilliSecBefore. check if past + cap < now
func MilliSecAfter(now, past, cap uint64) bool {
	return past+cap < now
}

func GetMilliSecTimestamp() uint64 {
	return uint64(time.Now().UnixNano() / int64(time.Millisecond))
}
