package cache

import (
	"sync/atomic"
	"time"
)

// clockOffsetNs shifts the time used for cache entry expiry (memory and disk).
// It is zero in production; tests move it forward to expire entries without
// sleeping. Atomic so background cache goroutines can read it race-free.
var clockOffsetNs atomic.Int64

// clockNow returns the current time as seen by cache expiry.
func clockNow() time.Time {
	return time.Now().Add(time.Duration(clockOffsetNs.Load()))
}

// AdvanceClockForTesting moves the clock used for cache and disk-cache expiry
// forward by d for every cache in the process and returns a function that
// restores it. For tests only.
func AdvanceClockForTesting(d time.Duration) (restore func()) {
	clockOffsetNs.Add(int64(d))
	return func() { clockOffsetNs.Add(-int64(d)) }
}
