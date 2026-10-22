package proxy

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// BenchmarkOrderedJSONLogsVolume measures Grafana's `{...} | json` logs volume
// query over one hour of lines (one every 200ms) against the fake backend.
func BenchmarkOrderedJSONLogsVolume(b *testing.B) {
	benchmarkOrderedJSONQuery(b, `sum by (level, detected_level) (count_over_time({app="api"} | json | drop __error__[1m]))`)
}

// BenchmarkOrderedJSONRowEvaluator measures the row evaluator (a `without`
// grouping has no stats pushdown) over the same hour of lines.
func BenchmarkOrderedJSONRowEvaluator(b *testing.B) {
	benchmarkOrderedJSONQuery(b, `sum without (n, counter, msg) (count_over_time({app="api"} | json | drop __error__[1m]))`)
}

func benchmarkOrderedJSONQuery(b *testing.B, query string) {
	s0 := time.Unix(1700000400, 0).UTC()
	lines := make([]jsonVolumeLine, 0, 18000)
	for i := 0; i < 18000; i++ {
		lines = append(lines, jsonVolumeFixtureLine(s0.Add(time.Duration(i)*200*time.Millisecond), i))
	}
	srv, _ := newJSONVolumeFakeVL(b, lines)
	p, err := New(Config{BackendURL: srv.URL, Cache: cache.NewDisabled(), LogLevel: "error"})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	p.storeBackendVersion("v1.50.0", "v1.50.0")
	params := url.Values{
		"query": {query},
		"start": {strconv.FormatInt(s0.Add(time.Minute).Unix(), 10)},
		"end":   {strconv.FormatInt(s0.Add(time.Hour).Unix(), 10)},
		"step":  {"60"},
	}
	target := "/loki/api/v1/query_range?" + params.Encode()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rec := httptest.NewRecorder()
		p.handleQueryRange(rec, httptest.NewRequest(http.MethodGet, target, nil))
		if rec.Code != http.StatusOK {
			b.Fatalf("status %d: %s", rec.Code, rec.Body)
		}
	}
}
