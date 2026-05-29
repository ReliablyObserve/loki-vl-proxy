package observability

import (
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

// RequestSampler implements adaptive request log sampling:
//
//   - Errors (4xx/5xx) are always counted and always emitted, but at high
//     error rates identical status codes are collapsed into periodic digests
//     ("429 ×312 in last 10s") instead of one line per request.
//   - OK traffic (2xx/3xx) emits no per-request lines at all by default.
//     Instead, a periodic digest is emitted every DigestInterval with
//     aggregate stats (total requests, P50/P99 latency, cache hit rate, etc.).
//   - When total request rate is below QuietThreshold (e.g., 10 req/s),
//     all requests are logged individually for debugging convenience.
//   - The sampler is safe for concurrent use.
type RequestSampler struct {
	// QuietThreshold: below this req/s, log every request individually.
	QuietThreshold int64
	// DigestInterval: how often to emit periodic stats.
	DigestInterval time.Duration
	// ErrorCollapseWindow: within this window, identical error codes are
	// collapsed into a single digest line.
	ErrorCollapseWindow time.Duration

	// Counters for the current digest window.
	okCount    atomic.Int64
	errCount   atomic.Int64
	totalCount atomic.Int64

	// Latency tracking (microseconds).
	latencySumUs atomic.Int64
	latencyMaxUs atomic.Int64
	cacheHits    atomic.Int64
	cacheMisses  atomic.Int64

	// Error bucketing: status code → count in current window.
	errBucketMu sync.Mutex
	errBuckets  map[int]*errorBucket

	// Rate detection.
	windowStart atomic.Int64 // unix millis
	windowCount atomic.Int64 // requests in current 1s window

	// Digest state.
	lastDigest atomic.Int64 // unix millis
}

type errorBucket struct {
	count     int64
	lastQuery string
	lastMs    int64
}

// NewRequestSampler creates a sampler with sensible defaults.
func NewRequestSampler() *RequestSampler {
	now := time.Now().UnixMilli()
	s := &RequestSampler{
		QuietThreshold:      10,
		DigestInterval:      10 * time.Second,
		ErrorCollapseWindow: 10 * time.Second,
		errBuckets:          make(map[int]*errorBucket),
	}
	s.windowStart.Store(now)
	s.lastDigest.Store(now)
	return s
}

// RequestInfo contains the info needed for sampling decisions.
type RequestInfo struct {
	StatusCode int
	LatencyMs  int64
	Query      string
	CacheHit   bool
	Endpoint   string
	Route      string
	Tenant     string
}

// ShouldLog returns whether this individual request should be logged.
// It always updates internal counters regardless of the return value.
func (s *RequestSampler) ShouldLog(info RequestInfo) bool {
	s.totalCount.Add(1)
	s.latencySumUs.Add(info.LatencyMs * 1000)
	for {
		old := s.latencyMaxUs.Load()
		newVal := info.LatencyMs * 1000
		if newVal <= old || s.latencyMaxUs.CompareAndSwap(old, newVal) {
			break
		}
	}
	if info.CacheHit {
		s.cacheHits.Add(1)
	} else {
		s.cacheMisses.Add(1)
	}

	isError := info.StatusCode >= 400

	// Track per-second rate for quiet detection.
	now := time.Now().UnixMilli()
	ws := s.windowStart.Load()
	if now-ws > 1000 {
		s.windowStart.CompareAndSwap(ws, now)
		s.windowCount.Store(1)
	} else {
		s.windowCount.Add(1)
	}
	rate := s.windowCount.Load()

	if isError {
		s.errCount.Add(1)
		s.errBucketMu.Lock()
		b, ok := s.errBuckets[info.StatusCode]
		if !ok {
			b = &errorBucket{}
			s.errBuckets[info.StatusCode] = b
		}
		b.count++
		b.lastQuery = info.Query
		b.lastMs = info.LatencyMs
		s.errBucketMu.Unlock()

		// In quiet mode, log every error individually.
		if rate <= s.QuietThreshold {
			return true
		}
		// In busy mode, log the first error of each code, then collapse.
		return b.count == 1
	}

	// OK traffic.
	s.okCount.Add(1)

	// In quiet mode (low rate), log every request.
	if rate <= s.QuietThreshold {
		return true
	}

	// In busy mode, suppress individual OK logs — digest handles it.
	return false
}

// DigestAttrs returns aggregated stats and resets counters if the digest
// interval has elapsed. Returns nil if it's not time for a digest yet.
func (s *RequestSampler) DigestAttrs() []slog.Attr {
	now := time.Now().UnixMilli()
	last := s.lastDigest.Load()
	if now-last < s.DigestInterval.Milliseconds() {
		return nil
	}
	if !s.lastDigest.CompareAndSwap(last, now) {
		return nil
	}

	total := s.totalCount.Swap(0)
	ok := s.okCount.Swap(0)
	errs := s.errCount.Swap(0)
	sumUs := s.latencySumUs.Swap(0)
	maxUs := s.latencyMaxUs.Swap(0)
	hits := s.cacheHits.Swap(0)
	misses := s.cacheMisses.Swap(0)

	if total == 0 {
		return nil
	}

	elapsedSec := float64(now-last) / 1000.0
	if elapsedSec < 0.001 {
		elapsedSec = 0.001
	}
	avgMs := float64(sumUs) / float64(total) / 1000.0
	cacheRate := float64(0)
	if hits+misses > 0 {
		cacheRate = float64(hits) / float64(hits+misses) * 100
	}

	attrs := []slog.Attr{
		slog.Int64("requests.total", total),
		slog.Int64("requests.ok", ok),
		slog.Int64("requests.errors", errs),
		slog.Float64("requests.rate_per_sec", float64(total)/elapsedSec),
		slog.Float64("latency.avg_ms", avgMs),
		slog.Int64("latency.max_ms", maxUs/1000),
		slog.Float64("cache.hit_pct", cacheRate),
		slog.Float64("digest.interval_sec", elapsedSec),
	}

	// Include error breakdown.
	s.errBucketMu.Lock()
	for code, b := range s.errBuckets {
		if b.count > 0 {
			attrs = append(attrs,
				slog.String(fmt.Sprintf("errors.%d", code),
					fmt.Sprintf("×%d (last: %dms)", b.count, b.lastMs)),
			)
		}
	}
	// Reset buckets.
	s.errBuckets = make(map[int]*errorBucket)
	s.errBucketMu.Unlock()

	return attrs
}
