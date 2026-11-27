package proxy

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// BenchmarkRankedParserWindow is the request of issue #1795 through the handler: topk(20) over a [1h] window metric
// with a parser stage, 80 s at Grafana's 53 ms step, over 600 series, read in one pass and in time chunks under a
// small row bound. VictoriaLogs' answers are rendered once and replayed, so the proxy's own work is measured.
func BenchmarkRankedParserWindow(b *testing.B) {
	end := time.Date(2026, 9, 15, 9, 0, 0, 0, time.UTC)
	start := end.Add(-80 * time.Second)
	// 600 series over the hour, 60 of them busy (a line every 0.5 to 3.5 s) through the 80 s.
	rows := chunkRows(start.Add(-time.Hour-time.Minute), end.Add(time.Second), 600)
	for i := 0; i < 60; i++ {
		every := time.Duration(500+50*i) * time.Millisecond
		for ts := start.Add(-time.Hour - time.Minute + time.Duration(i)*time.Millisecond); ts.Before(end.Add(time.Second)); ts = ts.Add(every) {
			rows = append(rows, emulatedRow{ts: ts.UnixNano(), pod: "busy-" + strconv.Itoa(i)})
		}
	}
	emu := &vlStatsRangeEmulator{t: &testing.T{}, rows: rows}
	var mu sync.Mutex
	answers := map[string][]byte{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		key := r.URL.Path + "?" + r.Form.Encode()
		mu.Lock()
		body, ok := answers[key]
		mu.Unlock()
		if !ok {
			rec := httptest.NewRecorder()
			emu.ServeHTTP(rec, r)
			body = rec.Body.Bytes()
			mu.Lock()
			answers[key] = body
			mu.Unlock()
		}
		w.Header().Set("Content-Type", "application/x-ndjson")
		_, _ = w.Write(body)
	}))
	defer srv.Close()
	for _, bc := range []struct {
		name, query string
		rows        int
	}{
		{"parser one read", "topk(20, sum by (pod) (count_over_time({app=\"api\"} | regexp `(?P<pod>\\S+)` [1h])))", 1_000_000},
		{"parser chunked", "topk(20, sum by (pod) (count_over_time({app=\"api\"} | regexp `(?P<pod>\\S+)` [1h])))", 1_500},
		{"no parser one read", `topk(20, sum by (pod) (count_over_time({app="api"} [1h])))`, 1_000_000},
	} {
		params := url.Values{
			"query": {bc.query},
			"start": {strconv.FormatInt(start.UnixNano(), 10)},
			"end":   {strconv.FormatInt(end.UnixNano(), 10)},
			"step":  {"0.053"},
		}
		b.Run(bc.name, func(b *testing.B) {
			p, err := New(Config{BackendURL: srv.URL, Cache: cache.New(0, 0), LogLevel: "error", RangeMetricRowLimit: bc.rows})
			if err != nil {
				b.Fatal(err)
			}
			p.storeBackendVersion("v1.50.0", "v1.50.0")
			b.ReportAllocs()
			for b.Loop() {
				rec := httptest.NewRecorder()
				p.handleQueryRange(rec, httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+params.Encode(), nil))
				if rec.Code != http.StatusOK {
					b.Fatalf("status %d: %.200s", rec.Code, rec.Body.String())
				}
			}
		})
	}
}
