package observability

import (
	"fmt"
	"log/slog"
	"sync"
	"testing"
	"time"
)

func TestNewRequestSampler_Defaults(t *testing.T) {
	s := NewRequestSampler()
	if s.QuietThreshold != 10 {
		t.Fatalf("QuietThreshold default: want 10, got %d", s.QuietThreshold)
	}
	if s.DigestInterval != 10*time.Second {
		t.Fatalf("DigestInterval default: want 10s, got %s", s.DigestInterval)
	}
	if s.ErrorCollapseWindow != 10*time.Second {
		t.Fatalf("ErrorCollapseWindow default: want 10s, got %s", s.ErrorCollapseWindow)
	}
	if s.errBuckets == nil {
		t.Fatal("errBuckets map must be initialised")
	}
}

func TestRequestSampler_QuietMode_LogsEveryRequest(t *testing.T) {
	s := NewRequestSampler()
	// Below QuietThreshold (=10): every request should log.
	for i := 0; i < 5; i++ {
		if !s.ShouldLog(RequestInfo{StatusCode: 200, LatencyMs: 5}) {
			t.Fatalf("quiet-mode OK request %d should log", i)
		}
	}
	if got := s.totalCount.Load(); got != 5 {
		t.Fatalf("totalCount: want 5, got %d", got)
	}
	if got := s.okCount.Load(); got != 5 {
		t.Fatalf("okCount: want 5, got %d", got)
	}
}

func TestRequestSampler_BusyMode_SuppressesOK(t *testing.T) {
	s := NewRequestSampler()
	s.QuietThreshold = 2 // make it easy to exceed

	// Drive past threshold within one window.
	s.ShouldLog(RequestInfo{StatusCode: 200, LatencyMs: 1})
	s.ShouldLog(RequestInfo{StatusCode: 200, LatencyMs: 1})
	// Third OK request — we're now above QuietThreshold; should be suppressed.
	if s.ShouldLog(RequestInfo{StatusCode: 200, LatencyMs: 1}) {
		t.Fatal("busy-mode OK request must be suppressed")
	}
}

func TestRequestSampler_BusyMode_CollapsesRepeatedErrors(t *testing.T) {
	s := NewRequestSampler()
	s.QuietThreshold = 1

	// First call moves rate above the threshold for the next ones.
	s.ShouldLog(RequestInfo{StatusCode: 200, LatencyMs: 1})

	// First 429 in busy mode → log it.
	if !s.ShouldLog(RequestInfo{StatusCode: 429, LatencyMs: 1, Query: "Q1"}) {
		t.Fatal("first busy-mode error must log")
	}
	// Subsequent identical-code errors → collapse.
	for i := 0; i < 4; i++ {
		if s.ShouldLog(RequestInfo{StatusCode: 429, LatencyMs: 1, Query: fmt.Sprintf("Q%d", i+2)}) {
			t.Fatalf("repeated 429 error #%d must be collapsed (not logged)", i+2)
		}
	}
	if got := s.errCount.Load(); got != 5 {
		t.Fatalf("errCount: want 5, got %d", got)
	}
}

func TestRequestSampler_QuietMode_LogsEveryError(t *testing.T) {
	s := NewRequestSampler()
	// Default QuietThreshold=10, only 3 requests — all errors should log.
	for i := 0; i < 3; i++ {
		if !s.ShouldLog(RequestInfo{StatusCode: 500, LatencyMs: 2}) {
			t.Fatalf("quiet-mode error %d must log", i)
		}
	}
}

func TestRequestSampler_CountersAndLatencyTracking(t *testing.T) {
	s := NewRequestSampler()
	s.ShouldLog(RequestInfo{StatusCode: 200, LatencyMs: 10, CacheHit: true})
	s.ShouldLog(RequestInfo{StatusCode: 200, LatencyMs: 30, CacheHit: false})
	s.ShouldLog(RequestInfo{StatusCode: 500, LatencyMs: 5, CacheHit: false})

	if got := s.cacheHits.Load(); got != 1 {
		t.Fatalf("cacheHits: want 1, got %d", got)
	}
	if got := s.cacheMisses.Load(); got != 2 {
		t.Fatalf("cacheMisses: want 2, got %d", got)
	}
	if got := s.latencyMaxUs.Load(); got != 30*1000 {
		t.Fatalf("latencyMaxUs: want 30000, got %d", got)
	}
	wantSum := int64((10 + 30 + 5) * 1000)
	if got := s.latencySumUs.Load(); got != wantSum {
		t.Fatalf("latencySumUs: want %d, got %d", wantSum, got)
	}
}

func TestRequestSampler_DigestAttrs_TooEarlyReturnsNil(t *testing.T) {
	s := NewRequestSampler()
	s.ShouldLog(RequestInfo{StatusCode: 200, LatencyMs: 1})
	if attrs := s.DigestAttrs(); attrs != nil {
		t.Fatalf("expected nil before DigestInterval elapsed, got %d attrs", len(attrs))
	}
}

func TestRequestSampler_DigestAttrs_ZeroTotalReturnsNil(t *testing.T) {
	s := NewRequestSampler()
	s.DigestInterval = 1 * time.Millisecond
	// No requests observed — digest must skip.
	time.Sleep(3 * time.Millisecond)
	if attrs := s.DigestAttrs(); attrs != nil {
		t.Fatalf("expected nil for zero-total digest, got %d attrs", len(attrs))
	}
}

func TestRequestSampler_DigestAttrs_EmitsAggregatesAndResetsCounters(t *testing.T) {
	s := NewRequestSampler()
	s.DigestInterval = 1 * time.Millisecond
	s.QuietThreshold = 1

	s.ShouldLog(RequestInfo{StatusCode: 200, LatencyMs: 5, CacheHit: true})
	s.ShouldLog(RequestInfo{StatusCode: 200, LatencyMs: 15, CacheHit: false})
	s.ShouldLog(RequestInfo{StatusCode: 429, LatencyMs: 4, Query: "Q"})
	s.ShouldLog(RequestInfo{StatusCode: 500, LatencyMs: 7, Query: "Q500"})

	time.Sleep(5 * time.Millisecond)

	attrs := s.DigestAttrs()
	if attrs == nil {
		t.Fatal("expected non-nil digest attrs")
	}

	asMap := attrsToMap(attrs)
	if v, _ := asMap["requests.total"].(int64); v != 4 {
		t.Fatalf("requests.total: want 4, got %v", asMap["requests.total"])
	}
	if v, _ := asMap["requests.ok"].(int64); v != 2 {
		t.Fatalf("requests.ok: want 2, got %v", asMap["requests.ok"])
	}
	if v, _ := asMap["requests.errors"].(int64); v != 2 {
		t.Fatalf("requests.errors: want 2, got %v", asMap["requests.errors"])
	}
	if v, _ := asMap["latency.max_ms"].(int64); v != 15 {
		t.Fatalf("latency.max_ms: want 15, got %v", asMap["latency.max_ms"])
	}
	if v, _ := asMap["cache.hit_pct"].(float64); v <= 0 || v >= 100 {
		t.Fatalf("cache.hit_pct: want strictly between 0 and 100 (50%%), got %v", v)
	}
	if _, ok := asMap["errors.429"]; !ok {
		t.Fatalf("expected errors.429 breakdown key, got map: %v", asMap)
	}
	if _, ok := asMap["errors.500"]; !ok {
		t.Fatalf("expected errors.500 breakdown key, got map: %v", asMap)
	}

	// Counters reset after a successful digest.
	if got := s.totalCount.Load(); got != 0 {
		t.Fatalf("totalCount must reset to 0, got %d", got)
	}
	if got := s.errCount.Load(); got != 0 {
		t.Fatalf("errCount must reset to 0, got %d", got)
	}
	s.errBucketMu.Lock()
	if got := len(s.errBuckets); got != 0 {
		t.Fatalf("errBuckets must reset, got %d entries", got)
	}
	s.errBucketMu.Unlock()
}

func TestRequestSampler_DigestAttrs_SecondCallReturnsNilUntilNextInterval(t *testing.T) {
	s := NewRequestSampler()
	s.DigestInterval = 5 * time.Millisecond
	s.ShouldLog(RequestInfo{StatusCode: 200, LatencyMs: 1})
	time.Sleep(10 * time.Millisecond)
	if attrs := s.DigestAttrs(); attrs == nil {
		t.Fatal("expected first DigestAttrs after interval to return attrs")
	}
	// Immediately after — too early.
	if attrs := s.DigestAttrs(); attrs != nil {
		t.Fatalf("expected immediate re-call to return nil, got %d attrs", len(attrs))
	}
}

func TestRequestSampler_ConcurrentShouldLogIsRaceFree(t *testing.T) {
	s := NewRequestSampler()
	const workers = 8
	const each = 200
	var wg sync.WaitGroup
	wg.Add(workers)
	for w := 0; w < workers; w++ {
		go func(id int) {
			defer wg.Done()
			for i := 0; i < each; i++ {
				code := 200
				if i%5 == 0 {
					code = 500
				}
				s.ShouldLog(RequestInfo{StatusCode: code, LatencyMs: int64(i % 50), CacheHit: i%2 == 0})
			}
		}(w)
	}
	wg.Wait()
	if got := s.totalCount.Load(); got != int64(workers*each) {
		t.Fatalf("totalCount under concurrency: want %d, got %d", workers*each, got)
	}
}

// attrsToMap collapses an slog.Attr slice into a map for inspection.
// Int/Float/String attrs unwrap to their Go types; everything else falls
// through as the slog.Value.
func attrsToMap(attrs []slog.Attr) map[string]any {
	m := make(map[string]any, len(attrs))
	for _, a := range attrs {
		switch a.Value.Kind() {
		case slog.KindInt64:
			m[a.Key] = a.Value.Int64()
		case slog.KindFloat64:
			m[a.Key] = a.Value.Float64()
		case slog.KindString:
			m[a.Key] = a.Value.String()
		default:
			m[a.Key] = a.Value
		}
	}
	return m
}
