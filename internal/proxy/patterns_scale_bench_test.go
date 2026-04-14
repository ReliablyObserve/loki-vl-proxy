package proxy

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
)

func buildPatternNDJSON(lines int) []byte {
	if lines < 1 {
		lines = 1
	}
	var b strings.Builder
	b.Grow(lines * 140)
	for i := range lines {
		method := []string{"GET", "POST", "PUT", "DELETE"}[i%4]
		status := []int{200, 201, 400, 500}[i%4]
		msg := fmt.Sprintf(
			"level=info msg=request method=%s route=/api/item/%d status=%d duration_ms=%d",
			method,
			i%2000,
			status,
			10+(i%900),
		)
		_, _ = fmt.Fprintf(
			&b,
			`{"_time":"2026-04-04T10:%02d:%02dZ","_msg":%q,"level":"info"}`+"\n",
			(i/60)%60,
			i%60,
			msg,
		)
	}
	return []byte(b.String())
}

func buildPatternWindowEntries(lines int) []queryRangeWindowEntry {
	if lines < 1 {
		lines = 1
	}
	entries := make([]queryRangeWindowEntry, 0, lines)
	for i := range lines {
		method := []string{"GET", "POST", "PUT", "DELETE"}[i%4]
		status := []int{200, 201, 400, 500}[i%4]
		msg := fmt.Sprintf(
			"level=info msg=request method=%s route=/api/item/%d status=%d duration_ms=%d",
			method,
			i%2000,
			status,
			10+(i%900),
		)
		entries = append(entries, queryRangeWindowEntry{
			Stream: map[string]string{"level": "info"},
			Value: []interface{}{
				strconv.FormatInt(1_775_296_800_000_000_000+int64(i), 10),
				msg,
			},
		})
	}
	return entries
}

func patternBypassRequests(requestCount int) []*http.Request {
	if requestCount < 1 {
		requestCount = 1
	}
	requests := make([]*http.Request, 0, requestCount)
	for i := range requestCount {
		req := benchmarkRequest(
			fmt.Sprintf(
				`/loki/api/v1/patterns?query={app="api"}&start=%d&end=%d&step=1m&limit=50`,
				i+1,
				i+2,
			),
		)
		_ = req.ParseForm()
		requests = append(requests, req)
	}
	return requests
}

func BenchmarkProxy_Patterns_Scale_CacheBypass(b *testing.B) {
	scales := []int{100, 1000, 10000}
	for _, scale := range scales {
		scale := scale
		b.Run("lines_"+strconv.Itoa(scale), func(b *testing.B) {
			body := buildPatternNDJSON(scale)
			mux, _ := newCompatBenchmarkProxy(b, func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/select/logsql/query" {
					http.NotFound(w, r)
					return
				}
				w.Header().Set("Content-Type", "application/x-ndjson")
				_, _ = w.Write(body)
			}, false)

			// Keep a wide unique-request set so cache bypass remains representative
			// under parallel benchmark load.
			requests := patternBypassRequests(8192)
			var idx atomic.Uint64

			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				w := newBenchmarkResponseWriter()
				for pb.Next() {
					r := requests[int(idx.Add(1)-1)%len(requests)]
					w.reset()
					mux.ServeHTTP(w, r)
				}
			})
		})
	}
}

func BenchmarkProxy_Patterns_Scale_CacheHit(b *testing.B) {
	scales := []int{100, 1000, 10000}
	for _, scale := range scales {
		scale := scale
		b.Run("lines_"+strconv.Itoa(scale), func(b *testing.B) {
			body := buildPatternNDJSON(scale)
			mux, _ := newCompatBenchmarkProxy(b, func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/select/logsql/query" {
					http.NotFound(w, r)
					return
				}
				w.Header().Set("Content-Type", "application/x-ndjson")
				_, _ = w.Write(body)
			}, false)

			rawURL := `/loki/api/v1/patterns?query={app="api"}&start=1&end=2&step=1m&limit=50`
			warmRoute(mux, rawURL, nil)
			req := benchmarkRequest(rawURL)
			_ = req.ParseForm()

			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				w := newBenchmarkResponseWriter()
				for pb.Next() {
					w.reset()
					mux.ServeHTTP(w, req)
				}
			})
		})
	}
}

func BenchmarkProxy_PatternsExtract_WindowEntries_Scale(b *testing.B) {
	scales := []int{100, 1000, 10000}
	for _, scale := range scales {
		scale := scale
		b.Run("lines_"+strconv.Itoa(scale), func(b *testing.B) {
			entries := buildPatternWindowEntries(scale)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = extractLogPatternsFromWindowEntries(entries, "1m", 50)
			}
		})
	}
}
