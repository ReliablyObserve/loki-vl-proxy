package proxy

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// BenchmarkRequestChain_MetricQueryRange_Hit times a metric query_range cache hit through the whole route chain,
// the request rewrite (lowercaseQueryKeywords) included, in the spelling Grafana sends.
func BenchmarkRequestChain_MetricQueryRange_Hit(b *testing.B) {
	benchmarkRequestChain(b, `/loki/api/v1/query_range?query=`+url.QueryEscape(`sum by (level) (count_over_time({app="nginx"} |= "error" [1m]))`)+`&step=15`)
}

// BenchmarkRequestQueryText times the request rewrite's text (lowercaseQueryKeywords) for queries it keeps, in the
// spellings Grafana and Logs Drilldown send, and for one it respells: memoised (a query seen before) and without the
// memo (handlerText).
func BenchmarkRequestQueryText(b *testing.B) {
	for _, c := range []struct{ name, q string }{
		{"metric", `sum by (level) (count_over_time({service_name="api"} | json | level=~"error|warn" [$__auto]))`},
		{"ranking", `topk(5, sum by (app) (rate({app=~".+"} |= "x" [5m]))) / on(app) group_left sum by (app) (rate({app=~".+"}[5m]))`},
		{"log", `{service_name="api"} | logfmt | line_format "{{.msg}}" |= "a (b)"`},
		{"respelled", `sum by (level) ( count_over_time ({service_name="api"} | json [5m]) )`},
	} {
		b.Run(c.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = requestQueryText(c.q, 0)
			}
		})
		// Without the memo: a query seen for the first time, or once the memo is full.
		b.Run("uncached/"+c.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = handlerText(c.q, 0)
			}
		})
	}
}

// BenchmarkRequestChain_MetricQueryRange_Miss times a metric query_range miss through the whole route chain (the
// request rewrite, validation, the response cache's miss, the translation and the VictoriaLogs call to a stub): each
// request asks a window a minute later than the one before, so no cache answers it.
func BenchmarkRequestChain_MetricQueryRange_Miss(b *testing.B) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"values":[{"value":"app","hits":100}]}`))
	}))
	defer backend.Close()
	p, err := New(Config{BackendURL: backend.URL, Cache: cache.New(time.Minute, 1000), CompatCache: cache.New(time.Minute, 1000), LogLevel: "error"})
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = p.Shutdown(b.Context()) }()
	mux := http.NewServeMux()
	p.RegisterRoutes(mux)
	query := url.QueryEscape(`sum by (level) (count_over_time({app="nginx"} |= "error" [1m]))`)
	end := time.Now().Truncate(time.Minute).Add(-24 * time.Hour)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		e := end.Add(time.Duration(i) * time.Minute)
		target := "/loki/api/v1/query_range?query=" + query + "&step=15&start=" + strconv.FormatInt(e.Add(-time.Hour).UnixNano(), 10) + "&end=" + strconv.FormatInt(e.UnixNano(), 10)
		req := httptest.NewRequest(http.MethodGet, target, nil)
		req.Header.Set("X-Scope-OrgID", "0")
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			b.Fatalf("%d %s", rec.Code, rec.Body)
		}
	}
}
