package proxy

import (
	"context"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// The request path's reads of what the proxy knows of the backend, as the handlers make them on every request: the
// gates, the flight key prefix, the version check of a response's headers and the refusal prediction guard. It calls
// only functions that exist before and after the capability layer moved the gates into one table, so the same file
// measures the base and the branch (benchstat, interleaved).
func benchCapProxy(b *testing.B) *Proxy {
	b.Helper()
	c := cache.New(time.Minute, 100)
	b.Cleanup(c.Close)
	p, err := New(Config{BackendURL: "http://127.0.0.1:1", Cache: c, LogLevel: "error"})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	p.storeBackendVersion("test", "v1.53.0")
	return p
}

func BenchmarkRequestPathCapabilityReads(b *testing.B) {
	p := benchCapProxy(b)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		caps := p.logsqlCapabilities()
		if !caps.PipeCoalesce || !p.supportsDensePatternWindowing() || !p.supportsColumnIndexedFields() || !p.supportsStreamMetadataEndpoints() || p.flightPrefix() == "" {
			b.Fatal("gates")
		}
	}
}

func BenchmarkRequestPathObserveHeaders(b *testing.B) {
	p := benchCapProxy(b)
	h := http.Header{"Server": {"VictoriaLogs/v1.53.0"}, "Content-Type": {"application/json"}}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p.observeBackendVersionFromHeaders(h)
	}
}

func BenchmarkRequestPathObserveHeadersNoVersion(b *testing.B) {
	p := benchCapProxy(b)
	h := http.Header{"Content-Type": {"application/json"}}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p.observeBackendVersionFromHeaders(h)
	}
}

func BenchmarkRequestPathPredictRefusalGuard(b *testing.B) {
	p := benchCapProxy(b)
	params := url.Values{"query": {`{app="api"} | unpack_json | filter status:>=500 | stats by (_time:1m, level) count() as c`}, "start": {"1"}, "end": {"2"}, "step": {"60s"}}
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, _, ok := p.predictVLRefusal(ctx, http.MethodPost, "/select/logsql/stats_query_range", params); ok {
			b.Fatal("predicted")
		}
	}
}
