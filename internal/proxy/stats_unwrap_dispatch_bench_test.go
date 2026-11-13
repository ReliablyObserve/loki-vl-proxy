package proxy

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// BenchmarkStatsUnwrapDispatch measures a metric query through the real handler against a stub that answers every
// stats request with a fixed body: the dispatch of the translated query, the step shift and the answer's rewrite.
func BenchmarkStatsUnwrapDispatch(b *testing.B) {
	const (
		rangeBody   = `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"app":"api"},"values":[[1799999700,"3"],[1800000000,"4"],[1800000300,"5"],[1800000600,"6"]]},{"metric":{"app":"web"},"values":[[1800000000,"0.5"],[1800000300,"1"]]}]}}`
		instantBody = `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"app":"api"},"value":[1800000600,"3"]},{"metric":{"app":"web"},"value":[1800000600,"0.5"]}]}}`
	)
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/select/logsql/stats_query" {
			_, _ = w.Write([]byte(instantBody))
			return
		}
		_, _ = w.Write([]byte(rangeBody))
	}))
	defer backend.Close()
	c := cache.New(time.Millisecond, 10)
	defer c.Close()
	p, err := New(Config{BackendURL: backend.URL, Cache: c, LogLevel: "error", BackendMinVersion: "v1.40.0", BackendVersionCheckTimeout: time.Second})
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = p.Shutdown(context.Background()) }()
	mux := http.NewServeMux()
	p.RegisterRoutes(mux)

	for _, tc := range []struct{ name, endpoint, query string }{
		{"sum_over_time_tumbling", "query_range", `sum by (app) (sum_over_time({app="x"} | json | unwrap n [5m]))`},
		{"stddev_over_time_tumbling", "query_range", `stddev_over_time({app="x"} | json | unwrap n [5m]) by (app)`},
		{"stddev_over_time_longer_range", "query_range", `stddev_over_time({app="x"} | json | unwrap n [10m]) by (app)`},
		{"count_over_time_tumbling", "query_range", `sum by (app) (count_over_time({app="x"}[5m]))`},
		{"stddev_over_time_instant", "query", `stddev_over_time({app="x"} | json | unwrap n [5m]) by (app)`},
		{"stddev_over_time_shorter_range", "query_range", `stddev_over_time({app="x"} | json | unwrap n [1m]) by (app)`},
		{"stdvar_over_time_tumbling", "query_range", `stdvar_over_time({app="x"} | json | unwrap n [5m]) by (app)`},
		{"stdvar_over_time_longer_range", "query_range", `stdvar_over_time({app="x"} | json | unwrap n [10m]) by (app)`},
		{"avg_over_time_longer_range", "query_range", `avg_over_time({app="x"} | json | unwrap n [10m]) by (app)`},
	} {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				// A selector of its own per iteration, so no response cache answers it.
				query := strings.Replace(tc.query, `{app="x"}`, `{app="x`+strconv.Itoa(i)+`"}`, 1)
				params := url.Values{"query": {query}, "start": {"1800000000"}, "end": {"1800000600"}, "step": {"300"}, "time": {"1800000600"}}
				req := httptest.NewRequest(http.MethodGet, "/loki/api/v1/"+tc.endpoint+"?"+params.Encode(), nil)
				req.Header.Set("X-Scope-OrgID", "0")
				rec := httptest.NewRecorder()
				mux.ServeHTTP(rec, req)
				if rec.Code != http.StatusOK {
					b.Fatalf("status %d: %s", rec.Code, rec.Body)
				}
			}
		})
	}
}
