package proxy

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// BenchmarkRouteChain_LabelsCacheHit sends a cached /labels request through
// the server's path, SecurityHeadersMiddleware and the registered mux, so the
// router checks (path cleaning, method, form decode) are on the measured
// path. It uses only what the base has, to compare base and branch.
func BenchmarkRouteChain_LabelsCacheHit(b *testing.B) {
	benchRouteChain(b, "GET", "/loki/api/v1/labels?start=1794567075&end=1794570675")
}

// BenchmarkRouteChain_QueryRangeCacheHit is the same for a cached query_range.
func BenchmarkRouteChain_QueryRangeCacheHit(b *testing.B) {
	benchRouteChain(b, "GET", "/loki/api/v1/query_range?query=%7Bapp%3D%22a%22%7D&start=1794567075&end=1794570675&limit=10")
}

func benchRouteChain(b *testing.B, method, target string) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"values":[{"value":"app","hits":100}]}`))
	}))
	defer backend.Close()
	c := cache.New(60*time.Second, 10000)
	defer c.Close()
	p, err := New(Config{BackendURL: backend.URL, Cache: c, LogLevel: "error"})
	if err != nil {
		b.Fatal(err)
	}
	mux := http.NewServeMux()
	p.RegisterRoutes(mux)
	h := SecurityHeadersMiddleware(mux)
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(method, target, nil))
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		w := newBenchmarkResponseWriter()
		r := httptest.NewRequest(method, target, nil)
		for pb.Next() {
			w.reset()
			h.ServeHTTP(w, r)
		}
	})
}
