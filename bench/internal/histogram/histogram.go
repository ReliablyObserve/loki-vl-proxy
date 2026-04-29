// Package histogram provides a concurrent-safe latency recorder with percentile computation.
package histogram

import (
	"math"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// Histogram records latency samples and computes percentiles.
// Thread-safe via sharded buffers that are merged on Snapshot.
type Histogram struct {
	mu      sync.Mutex
	samples []time.Duration

	count   atomic.Int64
	sum     atomic.Int64 // nanoseconds
	errors  atomic.Int64
	bytes   atomic.Int64 // response bytes
}

func New() *Histogram { return &Histogram{} }

// Record adds one latency sample.
func (h *Histogram) Record(d time.Duration, respBytes int64, err bool) {
	h.count.Add(1)
	h.sum.Add(int64(d))
	h.bytes.Add(respBytes)
	if err {
		h.errors.Add(1)
	}
	h.mu.Lock()
	h.samples = append(h.samples, d)
	h.mu.Unlock()
}

// Snapshot returns computed statistics. Safe to call concurrently.
func (h *Histogram) Snapshot(wallDuration time.Duration) Stats {
	h.mu.Lock()
	cp := make([]time.Duration, len(h.samples))
	copy(cp, h.samples)
	h.mu.Unlock()

	sort.Slice(cp, func(i, j int) bool { return cp[i] < cp[j] })

	n := h.count.Load()
	errs := h.errors.Load()
	bytes := h.bytes.Load()
	sum := h.sum.Load()

	s := Stats{
		Count:     n,
		Errors:    errs,
		ErrorRate: safeDivF(float64(errs), float64(n)),
		TotalBytes: bytes,
	}
	if n > 0 {
		s.Mean = time.Duration(sum / n)
	}
	if wallDuration > 0 {
		s.Throughput = float64(n) / wallDuration.Seconds()
		s.BytesPerSec = float64(bytes) / wallDuration.Seconds()
	}
	if len(cp) > 0 {
		s.Min = cp[0]
		s.Max = cp[len(cp)-1]
		s.P50 = pct(cp, 50)
		s.P75 = pct(cp, 75)
		s.P90 = pct(cp, 90)
		s.P95 = pct(cp, 95)
		s.P99 = pct(cp, 99)
		s.P999 = pct(cp, 99.9)
	}
	return s
}

// Stats holds computed histogram statistics.
type Stats struct {
	Count      int64
	Errors     int64
	ErrorRate  float64
	TotalBytes int64
	BytesPerSec float64
	Throughput float64 // req/s
	Mean       time.Duration
	Min        time.Duration
	Max        time.Duration
	P50        time.Duration
	P75        time.Duration
	P90        time.Duration
	P95        time.Duration
	P99        time.Duration
	P999       time.Duration
}

func pct(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(math.Ceil(p/100.0*float64(len(sorted)))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

func safeDivF(a, b float64) float64 {
	if b == 0 {
		return 0
	}
	return a / b
}
