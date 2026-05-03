package proxy

import (
	"context"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"
)

func TestQueryRangeWindowHitEstimate_CachedVariants(t *testing.T) {
	p := newTestProxy(t, "http://unused")
	req := httptest.NewRequest("GET", "/loki/api/v1/query_range", nil)
	req.Header.Set("X-Scope-OrgID", "tenant-a")
	window := queryRangeWindow{startNs: 10, endNs: 20}
	cacheKey := p.queryRangeWindowHasHitsCacheKey(req, "{app=\"api\"}", window)

	p.cache.Set(cacheKey, []byte("42"))
	if got, err := p.queryRangeWindowHitEstimate(context.Background(), req, "{app=\"api\"}", window); err != nil || got != 42 {
		t.Fatalf("expected parsed cached hit estimate, got %d err=%v", got, err)
	}

	p.cache.Set(cacheKey, []byte("1x"))
	if got, err := p.queryRangeWindowHitEstimate(context.Background(), req, "{app=\"api\"}", window); err != nil || got != 1 {
		t.Fatalf("expected legacy truthy cached hit estimate, got %d err=%v", got, err)
	}

	p.cache.Set(cacheKey, []byte("garbage"))
	if got, err := p.queryRangeWindowHitEstimate(context.Background(), req, "{app=\"api\"}", window); err != nil || got != 0 {
		t.Fatalf("expected invalid cached hit estimate to fall back to zero, got %d err=%v", got, err)
	}
}

func TestQueryRangeWindowBatchParallelLimitAndTTL(t *testing.T) {
	p := newTestProxy(t, "http://unused")
	p.queryRangeAdaptiveParallel = false
	p.queryRangeMaxParallel = 8
	p.queryRangeStreamAwareBatching = true
	p.queryRangeExpensiveWindowHitThreshold = 100
	p.queryRangeExpensiveWindowMaxParallel = 2

	window := queryRangeWindow{startNs: 1, endNs: 2}
	estimates := map[string]int64{queryRangeWindowKey(window): 150}
	if got := p.queryRangeWindowBatchParallelLimit(window, estimates); got != 2 {
		t.Fatalf("expected expensive window to degrade batch parallelism, got %d", got)
	}

	p.queryRangeExpensiveWindowMaxParallel = 0
	if got := p.queryRangeWindowBatchParallelLimit(window, estimates); got != 1 {
		t.Fatalf("expected non-positive expensive max parallel to clamp to 1, got %d", got)
	}

	p.queryRangeStreamAwareBatching = false
	if got := p.queryRangeWindowBatchParallelLimit(window, estimates); got != 8 {
		t.Fatalf("expected disabled stream-aware batching to keep default parallelism, got %d", got)
	}

	p.queryRangeHistoryCacheTTL = 0
	p.queryRangeRecentCacheTTL = 0
	if got := p.queryRangeWindowPrefilterTTL(time.Now().UnixNano()); got != queryRangePrefilterFallbackTTL {
		t.Fatalf("expected zero window ttl to fall back to prefilter ttl, got %v", got)
	}
}

func TestSplitQueryRangeWindowsWithOptions_AlignmentAndReverse(t *testing.T) {
	if windows := splitQueryRangeWindowsWithOptions(0, 10, 0, "forward", false); windows != nil {
		t.Fatalf("expected zero interval to produce no windows")
	}
	if windows := splitQueryRangeWindowsWithOptions(10, 0, time.Second, "forward", false); windows != nil {
		t.Fatalf("expected inverted range to produce no windows")
	}

	windows := splitQueryRangeWindowsWithOptions(2, 12, 5*time.Nanosecond, "backward", true)
	if len(windows) != 3 {
		t.Fatalf("expected three aligned windows, got %#v", windows)
	}
	if windows[0] != (queryRangeWindow{startNs: 10, endNs: 12}) ||
		windows[1] != (queryRangeWindow{startNs: 5, endNs: 9}) ||
		windows[2] != (queryRangeWindow{startNs: 2, endNs: 4}) {
		t.Fatalf("unexpected aligned reverse windows: %#v", windows)
	}
}

func TestNormalizeLokiNumericTimeToUnixNano_Thresholds(t *testing.T) {
	tests := []struct {
		name  string
		value float64
		want  int64
	}{
		{name: "seconds", value: 12.5, want: int64(12.5 * float64(time.Second))},
		{name: "milliseconds", value: 1700000000000.5, want: int64(1700000000000.5 * float64(time.Millisecond))},
		{name: "microseconds", value: 1700000000000000.5, want: int64(1700000000000000.5 * float64(time.Microsecond))},
		{name: "nanoseconds", value: 1700000000000000000.0, want: 1700000000000000000},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := normalizeLokiNumericTimeToUnixNano(tt.value); got != tt.want {
				t.Fatalf("normalizeLokiNumericTimeToUnixNano(%v) = %d, want %d", tt.value, got, tt.want)
			}
		})
	}
}

func TestVLLogsToLokiWindowEntries_SkipsInvalidAndDerivesFields(t *testing.T) {
	p := newTestProxy(t, "http://unused")
	body := []byte(
		"not-json\n" +
			`{"_msg":"missing-time","_stream":"{app=\"api\"}"}` + "\n" +
			`{"_time":"2026-04-01T00:00:00Z","_msg":"ok","_stream":"{app=\"api\",level=\"warn\"}","trace_id":"abc"}` + "\n",
	)

	entries := p.vlLogsToLokiWindowEntries(body, `{app="api"}`, true, true)
	if len(entries) != 1 {
		t.Fatalf("expected one valid translated window entry, got %#v", entries)
	}
	if entries[0].Stream["service_name"] != "api" {
		t.Fatalf("expected synthetic service name to be derived, got %#v", entries[0].Stream)
	}
	if entries[0].Stream["detected_level"] != "warn" {
		t.Fatalf("expected detected level to be preserved, got %#v", entries[0].Stream)
	}
	if got := entries[0].Ts; got != strconv.FormatInt(time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC).UnixNano(), 10) {
		t.Fatalf("unexpected translated timestamp %v", got)
	}
}
