package proxy

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

func BenchmarkQueryRangeWindowing_NoPrefilter(b *testing.B) {
	benchmarkQueryRangeWindowingPrefilter(b, false)
}

func BenchmarkQueryRangeWindowing_WithPrefilter(b *testing.B) {
	benchmarkQueryRangeWindowingPrefilter(b, true)
}

func BenchmarkQueryRangeWindowing_StreamAwareBatching_Off(b *testing.B) {
	benchmarkQueryRangeWindowingStreamAware(b, false)
}

func BenchmarkQueryRangeWindowing_StreamAwareBatching_On(b *testing.B) {
	benchmarkQueryRangeWindowingStreamAware(b, true)
}

func benchmarkQueryRangeWindowingPrefilter(b *testing.B, prefilter bool) {
	baseStart := time.Now().Add(-48 * time.Hour).UTC().Truncate(time.Hour).UnixNano()
	baseEnd := baseStart + int64(48*time.Hour) - 1
	stepNs := int64(time.Hour)

	var hitsCalls atomic.Int64
	var queryCalls atomic.Int64
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		switch r.URL.Path {
		case "/select/logsql/hits":
			hitsCalls.Add(1)
			windowStart, _ := strconv.ParseInt(r.Form.Get("start"), 10, 64)
			idx := int((windowStart - baseStart) / stepNs)
			val := 0
			// Sparse history: only every 6th window has data.
			if idx%6 == 0 {
				val = 1
			}
			_, _ = fmt.Fprintf(
				w,
				`{"hits":[{"fields":{"app":"nginx"},"timestamps":["%s"],"values":[%d]}]}`,
				time.Unix(0, windowStart).UTC().Format(time.RFC3339),
				val,
			)
		case "/select/logsql/query":
			queryCalls.Add(1)
			windowStart, _ := strconv.ParseInt(r.Form.Get("start"), 10, 64)
			time.Sleep(1 * time.Millisecond)
			_, _ = fmt.Fprintf(
				w,
				"{\"_time\":%q,\"_msg\":\"ok\",\"_stream\":\"{app=\\\"nginx\\\"}\"}\n",
				time.Unix(0, windowStart).UTC().Format(time.RFC3339Nano),
			)
		default:
			http.Error(w, "unexpected path", http.StatusNotFound)
		}
	}))
	defer backend.Close()

	c := cache.New(60*time.Second, 50000)
	p, err := New(Config{
		BackendURL:                    backend.URL,
		Cache:                         c,
		LogLevel:                      "error",
		QueryRangeWindowingEnabled:    true,
		QueryRangeSplitInterval:       time.Hour,
		QueryRangeMaxParallel:         2,
		QueryRangeAdaptiveParallel:    false,
		QueryRangeFreshness:           10 * time.Minute,
		QueryRangeRecentCacheTTL:      0,
		QueryRangeHistoryCacheTTL:     24 * time.Hour,
		QueryRangePrefilterIndexStats: prefilter,
		QueryRangePrefilterMinWindows: 1,
	})
	if err != nil {
		b.Fatalf("failed to create proxy: %v", err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		query := fmt.Sprintf(`{app="nginx",iter="%d"} | json | logfmt`, i)
		req := httptest.NewRequest(
			http.MethodGet,
			fmt.Sprintf(
				"/loki/api/v1/query_range?query=%s&start=%d&end=%d&limit=5000",
				url.QueryEscape(query),
				baseStart,
				baseEnd,
			),
			nil,
		)
		w := httptest.NewRecorder()
		p.handleQueryRange(w, req)
		if w.Code != http.StatusOK {
			b.Fatalf("unexpected status: %d body=%s", w.Code, strings.TrimSpace(w.Body.String()))
		}
	}
	b.StopTimer()

	b.ReportMetric(float64(queryCalls.Load())/float64(b.N), "query_calls/op")
	b.ReportMetric(float64(hitsCalls.Load())/float64(b.N), "hits_calls/op")
}

func benchmarkQueryRangeWindowingStreamAware(b *testing.B, streamAware bool) {
	baseStart := time.Now().Add(-24 * time.Hour).UTC().Truncate(time.Hour).UnixNano()
	baseEnd := baseStart + int64(24*time.Hour) - 1
	var inFlight atomic.Int64
	var maxInFlight atomic.Int64

	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		switch r.URL.Path {
		case "/select/logsql/hits":
			_, _ = w.Write([]byte(`{"hits":[{"fields":{"app":"nginx"},"timestamps":["2026-01-01T00:00:00Z"],"values":[50000]}]}`))
		case "/select/logsql/query":
			cur := inFlight.Add(1)
			for {
				prev := maxInFlight.Load()
				if cur <= prev || maxInFlight.CompareAndSwap(prev, cur) {
					break
				}
			}
			time.Sleep(2 * time.Millisecond)
			inFlight.Add(-1)
			windowStart, _ := strconv.ParseInt(r.Form.Get("start"), 10, 64)
			_, _ = fmt.Fprintf(
				w,
				"{\"_time\":%q,\"_msg\":\"ok\",\"_stream\":\"{app=\\\"nginx\\\"}\"}\n",
				time.Unix(0, windowStart).UTC().Format(time.RFC3339Nano),
			)
		default:
			http.Error(w, "unexpected path", http.StatusNotFound)
		}
	}))
	defer backend.Close()

	p, err := New(Config{
		BackendURL:                      backend.URL,
		Cache:                           cache.New(60*time.Second, 50000),
		LogLevel:                        "error",
		QueryRangeWindowingEnabled:      true,
		QueryRangeSplitInterval:         time.Hour,
		QueryRangeMaxParallel:           4,
		QueryRangeAdaptiveParallel:      false,
		QueryRangeFreshness:             10 * time.Minute,
		QueryRangeRecentCacheTTL:        0,
		QueryRangeHistoryCacheTTL:       24 * time.Hour,
		QueryRangePrefilterIndexStats:   true,
		QueryRangePrefilterMinWindows:   1,
		QueryRangeStreamAwareBatching:   streamAware,
		QueryRangeExpensiveHitThreshold: 1,
		QueryRangeExpensiveMaxParallel:  1,
	})
	if err != nil {
		b.Fatalf("failed to create proxy: %v", err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		req := httptest.NewRequest(
			http.MethodGet,
			fmt.Sprintf(
				"/loki/api/v1/query_range?query=%s&start=%d&end=%d&limit=5000",
				url.QueryEscape(fmt.Sprintf(`{app="nginx",iter="%d"}`, i)),
				baseStart,
				baseEnd,
			),
			nil,
		)
		w := httptest.NewRecorder()
		p.handleQueryRange(w, req)
		if w.Code != http.StatusOK {
			b.Fatalf("unexpected status: %d body=%s", w.Code, strings.TrimSpace(w.Body.String()))
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(maxInFlight.Load()), "max_inflight")
}
