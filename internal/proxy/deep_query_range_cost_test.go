package proxy

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"runtime"
	"runtime/metrics"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// deepLabelReplace is base wrapped in depth label_replace calls, tag making the query one no memo holds yet.
func deepLabelReplace(depth int, tag string) string {
	const prefix, suffix = `label_replace(`, `, "dst", "$1", "src", "(.*)")`
	return strings.Repeat(prefix, depth) + `sum by (app) (rate({app="` + tag + `"}[1m]))` + strings.Repeat(suffix, depth)
}

// deepLabelReplaceMaxDepth is the deepest such nesting below Loki's input size limit.
const deepLabelReplaceMaxDepth = 3040

// deepQueryRangeProxy is a proxy over a VictoriaLogs stub that answers every call with nothing, and its routes.
func deepQueryRangeProxy(tb testing.TB) http.Handler {
	stub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte("{}\n"))
	}))
	tb.Cleanup(stub.Close)
	p, err := New(Config{BackendURL: stub.URL, Cache: cache.New(60*time.Second, 1000), LogLevel: "error"})
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	mux := http.NewServeMux()
	p.RegisterRoutes(mux)
	return mux
}

// serveDeepQueryRange sends q to /loki/api/v1/query_range and returns the status.
func serveDeepQueryRange(h http.Handler, q string) int {
	target := "/loki/api/v1/query_range?" + url.Values{"query": {q}, "start": {"1700000000"}, "end": {"1700003600"},
		"step": {"60"}}.Encode()
	req := httptest.NewRequest(http.MethodGet, target, nil)
	req.Header.Set("X-Scope-OrgID", "0")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec.Code
}

// TestDeepQueryRangeCostSweep writes the depth and size sweep of a whole query_range of a nested label_replace
// (through every middleware and the handler, over a VictoriaLogs stub) as a Markdown table, to the file
// LVP_DEEP_QUERY_RANGE_SWEEP names: time, allocations, bytes allocated and the peak of the live heap above where the
// request started. Every request sends a query no memo holds.
func TestDeepQueryRangeCostSweep(t *testing.T) {
	out := os.Getenv("LVP_DEEP_QUERY_RANGE_SWEEP")
	if out == "" {
		t.Skip("set LVP_DEEP_QUERY_RANGE_SWEEP=<file> to write the sweep")
	}
	h := deepQueryRangeProxy(t)
	var b strings.Builder
	b.WriteString("| depth | bytes | status | time | ns/level | allocs | allocs/level | B | peak heap |\n")
	b.WriteString("|---:|---:|---:|---:|---:|---:|---:|---:|---:|\n")
	depths := []int{}
	for d := 1; d < deepLabelReplaceMaxDepth; d *= 2 {
		depths = append(depths, d)
	}
	for i, d := range append(depths, deepLabelReplaceMaxDepth) {
		q := deepLabelReplace(d, fmt.Sprintf("sweep-%d", i))
		var status int
		took, allocs, bytes, peak := measureRequest(func() { status = serveDeepQueryRange(h, q) })
		fmt.Fprintf(&b, "| %d | %d | %d | %s | %d | %d | %.1f | %d | %d |\n", d, len(q), status, took.Round(time.Microsecond),
			took.Nanoseconds()/int64(d), allocs, float64(allocs)/float64(d), bytes, peak)
		if took > 30*time.Second {
			fmt.Fprintf(&b, "| >%d | | | over the 30s budget: deeper rows not run | | | | | |\n", d)
			break
		}
	}
	if err := os.WriteFile(out, []byte(b.String()), 0o600); err != nil {
		t.Fatal(err)
	}
}

// measureRequest runs f once: its time, allocations, bytes allocated and the peak of the live heap above where it
// started (sampled every 200µs).
func measureRequest(f func()) (took time.Duration, allocs, bytes, peak uint64) {
	runtime.GC()
	sample := []metrics.Sample{{Name: "/memory/classes/heap/objects:bytes"}}
	read := func() uint64 { metrics.Read(sample); return sample[0].Value.Uint64() }
	base := read()
	top := base
	stop, done := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(done)
		tick := time.NewTicker(200 * time.Microsecond)
		defer tick.Stop()
		for {
			select {
			case <-stop:
				return
			case <-tick.C:
				top = max(top, read())
			}
		}
	}()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	start := time.Now()
	f()
	took = time.Since(start)
	runtime.ReadMemStats(&after)
	close(stop)
	<-done
	return took, after.Mallocs - before.Mallocs, after.TotalAlloc - before.TotalAlloc, max(top, read()) - base
}

// BenchmarkDeepQueryRange times a whole query_range of a nested label_replace, each request with a query no memo
// holds: ns/op divided by the depth is the cost of one level of the request.
func BenchmarkDeepQueryRange(b *testing.B) {
	h := deepQueryRangeProxy(b)
	for _, d := range []int{16, 256} {
		b.Run(fmt.Sprintf("depth=%d", d), func(b *testing.B) {
			b.ReportAllocs()
			i := 0
			for b.Loop() {
				i++
				if status := serveDeepQueryRange(h, deepLabelReplace(d, fmt.Sprintf("bench-%d-%d", d, i))); status != http.StatusOK {
					b.Fatalf("status %d", status)
				}
			}
		})
	}
}
