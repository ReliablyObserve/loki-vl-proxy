package proxy

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// The request path through requestLogger: a request that does no cache or VictoriaLogs work (the constant cost
// of the flow's bookkeeping), one answered from the cache, and one that misses and asks VictoriaLogs; each with
// the request log and the slow-query trace off ("plain"), the request log on ("logon"), the trace on at the
// default thresholds ("traceon") and both.

func benchFlowProxy(b *testing.B, logOn, traceOn bool) (*Proxy, *http.ServeMux) {
	b.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, `{"field":"app","hits":1}`+"\n")
	}))
	b.Cleanup(srv.Close)
	p, err := New(Config{BackendURL: srv.URL, Cache: cache.New(60*time.Second, 100000), LogLevel: "error"})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	if logOn {
		p.log = slog.New(slog.NewJSONHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelInfo}))
		p.requestSampler = nil // above -log-rate-threshold the sampler would suppress every line of a benchmark loop
	}
	if traceOn {
		p.slowQueryThreshold, p.slowUpstreamThreshold = DefaultSlowQueryThreshold, DefaultSlowUpstreamThreshold
	}
	mux := http.NewServeMux()
	p.RegisterRoutes(mux)
	return p, mux
}

func BenchmarkRequestPath(b *testing.B) {
	for _, mode := range []struct {
		name           string
		logOn, traceOn bool
	}{{"plain", false, false}, {"logon", true, false}, {"traceon", false, true}, {"both", true, true}} {
		b.Run("NoCacheNoVL/"+mode.name, func(b *testing.B) {
			p, _ := benchFlowProxy(b, mode.logOn, mode.traceOn)
			h := p.requestLogger("query_range", "/loki/api/v1/query_range", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })
			req := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?query=x", nil)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				h.ServeHTTP(httptest.NewRecorder(), req)
			}
		})
		b.Run("LabelsCacheHit/"+mode.name, func(b *testing.B) {
			_, mux := benchFlowProxy(b, mode.logOn, mode.traceOn)
			url := fmt.Sprintf("/loki/api/v1/labels?start=%d&end=%d&query=%%2A", perfBaseTimeNs-int64(time.Hour), perfBaseTimeNs)
			mux.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, url, nil))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				mux.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, url, nil))
			}
		})
		b.Run("LabelsCacheMissVL/"+mode.name, func(b *testing.B) {
			_, mux := benchFlowProxy(b, mode.logOn, mode.traceOn)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				// Windows 48h apart never share a cache key, so each request asks VictoriaLogs.
				end := perfBaseTimeNs - int64(i+1)*int64(48*time.Hour)
				url := fmt.Sprintf("/loki/api/v1/labels?start=%d&end=%d&query=%%2A", end-int64(time.Hour), end)
				mux.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, url, nil))
			}
		})
	}
}
