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

// stepAlignOverheadTarget is a metric query_range through the full route
// chain, answered from the response cache after the first request: the
// cheapest path, where the per-request cost of step alignment shows most.
func stepAlignOverheadTarget(tb testing.TB) (http.Handler, func(start, end time.Time) string) {
	tb.Helper()
	s0 := time.Unix(1700001000, 0).UTC()
	srv, _ := newSlidingFakeVL(tb, alignFixture(s0, 3*time.Hour))
	tb.Cleanup(srv.Close)
	p, err := New(Config{
		BackendURL:           srv.URL,
		Cache:                cache.NewWithMaxBytes(time.Minute, 100000, 256<<20),
		CompatCache:          cache.New(time.Minute, 1000),
		LogLevel:             "error",
		AlignQueriesWithStep: true,
	})
	if err != nil {
		tb.Fatalf("proxy: %v", err)
	}
	tb.Cleanup(func() {
		_ = p.Shutdown(context.Background())
		p.cache.Close()
		p.compatCache.Close()
	})
	p.storeBackendVersion("v1.50.0", "v1.50.0")
	mux := http.NewServeMux()
	p.RegisterProxyRoutes(mux)
	target := func(start, end time.Time) string {
		return "/loki/api/v1/query_range?" + url.Values{
			"query": {`sum by (app) (count_over_time({app=~".+"}[5m]))`},
			"start": {strconv.FormatInt(start.UnixNano(), 10)},
			"end":   {strconv.FormatInt(end.UnixNano(), 10)},
			"step":  {"60000ms"},
		}.Encode()
	}
	return mux, target
}

// BenchmarkStepAlignOverhead serves the same request repeatedly from the
// response cache: bounds on the step grid, Grafana's shape (start on the
// grid, end off it) and both bounds off the grid.
func BenchmarkStepAlignOverhead(b *testing.B) {
	mux, target := stepAlignOverheadTarget(b)
	grid := time.Unix(1700001000, 0).UTC().Add(2 * time.Hour).Truncate(time.Minute)
	for _, tc := range []struct {
		name       string
		start, end time.Time
	}{
		{"on_grid", grid.Add(-time.Hour), grid},
		{"grafana", grid.Add(-time.Hour), grid.Add(7 * time.Second)},
		{"off_grid", grid.Add(-time.Hour + 13*time.Second), grid.Add(7 * time.Second)},
	} {
		b.Run(tc.name, func(b *testing.B) {
			path := target(tc.start, tc.end)
			serve := func() {
				rec := httptest.NewRecorder()
				mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
				if rec.Code != http.StatusOK {
					b.Fatalf("%d %s", rec.Code, rec.Body.String())
				}
			}
			serve() // fills the response cache
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				serve()
			}
		})
	}
}
