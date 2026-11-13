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

// benchmarkRequestChain times a happy-path request through the whole route
// chain of the shipped mux (form decode, parameter checks, tenant, limits, the
// response cache) on a cache hit, so that what a request costs before any
// backend work, the part parameter validation adds to, is what is measured.
func benchmarkRequestChain(b *testing.B, path string) {
	b.Helper()
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/select/logsql/query" {
			_, _ = w.Write([]byte(`{"_time":"2026-11-13T10:30:00Z","_msg":"test","_stream":"{app=\"nginx\"}","app":"nginx"}` + "\n"))
			return
		}
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
	end := time.Now().Truncate(time.Minute)
	start := end.Add(-time.Hour)
	target := path + "&start=" + strconv.FormatInt(start.UnixNano(), 10) + "&end=" + strconv.FormatInt(end.UnixNano(), 10)
	req := httptest.NewRequest(http.MethodGet, target, nil)
	req.Header.Set("X-Scope-OrgID", "0")
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req) // fill the cache
	if rec.Code != http.StatusOK {
		b.Fatalf("%s: %d %s", target, rec.Code, rec.Body)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			b.Fatalf("%d %s", rec.Code, rec.Body)
		}
	}
}

func BenchmarkRequestChain_QueryRange_Hit(b *testing.B) {
	benchmarkRequestChain(b, `/loki/api/v1/query_range?query=%7Bapp%3D%22nginx%22%7D&step=15&limit=1000&direction=backward`)
}

func BenchmarkRequestChain_Labels_Hit(b *testing.B) {
	benchmarkRequestChain(b, `/loki/api/v1/labels?query=%7Bapp%3D%22nginx%22%7D`)
}

// BenchmarkRequestParamChecks times the parameter checks of a valid request on
// their own, in the forms Grafana sends.
func BenchmarkRequestParamChecks(b *testing.B) {
	p := paramTestProxy(b)
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	r.Form = url.Values{"query": {`{app="x"}`}, "start": {"1794567075000000000"}, "end": {"1794570675000000000"}, "step": {"15"}, "limit": {"1000"}, "direction": {"backward"}}
	now := time.Unix(1794570675, 0)
	for _, route := range []string{"query_range", "labels", "volume_range", "tail"} {
		checks := requestParamChecks[route]
		b.Run(route, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if err := p.requestParamError(checks, r, now); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
