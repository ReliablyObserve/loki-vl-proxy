package proxy

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
	mw "github.com/ReliablyObserve/Loki-VL-proxy/internal/middleware"
)

// BenchmarkQueryAnswerPaths times whole query and query_range requests
// through the route chain (request log, stats layer, response caches) against
// a VictoriaLogs stub: log, metric and instant answers, each a miss (a new
// window per iteration, so VictoriaLogs is asked) and a hit (the same request
// again), identity and gzip. It is the base-vs-change measure of the stats
// layer (query_stats.go) and runs unchanged on code without it.
func BenchmarkQueryAnswerPaths(b *testing.B) {
	var rows strings.Builder
	base := time.Now().Add(-48 * time.Hour).Truncate(time.Hour)
	for i := 0; i < 1000; i++ {
		fmt.Fprintf(&rows, `{"_time":%q,"_stream":"{app=\"api\"}","_msg":"GET /api/v1/items/%d 200 12ms user=alice region=us-east-1","app":"api"}`+"\n",
			base.Add(time.Duration(i)*time.Millisecond).UTC().Format(time.RFC3339Nano), i)
	}
	rawRows := rows.String()
	vl := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/select/logsql/query":
			_, _ = w.Write([]byte(rawRows))
		case "/select/logsql/stats_query_range":
			_ = r.ParseForm()
			start, _ := parseLokiTimeToUnixNano(r.Form.Get("start"))
			s := start / int64(time.Second)
			_, _ = fmt.Fprintf(w, `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"app":"api"},"values":[[%d,"7"],[%d,"8"],[%d,"9"]]}]}}`, s, s+60, s+120)
		case "/select/logsql/stats_query":
			_, _ = w.Write([]byte(`{"status":"success","data":{"resultType":"vector","result":[{"metric":{"app":"api"},"value":[1,"7"]}]}}`))
		case "/select/logsql/hits":
			_, _ = w.Write([]byte(`{"hits":[]}`))
		default:
			_, _ = w.Write([]byte(`{"values":[]}`))
		}
	}))
	b.Cleanup(vl.Close)
	newHandler := func(b *testing.B, gzip bool) http.Handler {
		mode := ""
		if gzip {
			mode = "gzip"
		}
		p, err := New(Config{BackendURL: vl.URL, Cache: cache.New(time.Hour, 100000), CompatCache: cache.New(time.Hour, 100000),
			LogLevel: "error", ClientResponseCompression: mode, ClientResponseCompressionMinBytes: 1024})
		if err != nil {
			b.Fatal(err)
		}
		b.Cleanup(func() { _ = p.Shutdown(context.Background()) })
		mux := http.NewServeMux()
		p.RegisterRoutes(mux)
		if !gzip {
			return mux
		}
		return mw.CompressionHandlerWithOptions(mux, mw.CompressionOptions{Mode: mode, MinBytes: 1024})
	}
	end := base.Add(time.Hour)
	target := func(kind string, shift int) string {
		e := end.Add(-time.Duration(shift) * 48 * time.Hour)
		switch kind {
		case "log":
			return "/loki/api/v1/query_range?" + url.Values{"query": {`{app="api"}`}, "limit": {"1000"},
				"start": {fmt.Sprint(e.Add(-time.Hour).UnixNano())}, "end": {fmt.Sprint(e.UnixNano())}}.Encode()
		case "metric":
			return "/loki/api/v1/query_range?" + url.Values{"query": {`sum by (app) (count_over_time({app="api"}[1m]))`}, "step": {"60"},
				"start": {fmt.Sprint(e.Add(-3 * time.Minute).Unix())}, "end": {fmt.Sprint(e.Unix())}}.Encode()
		default:
			return "/loki/api/v1/query?" + url.Values{"query": {`sum(count_over_time({app="api"}[5m]))`}, "time": {fmt.Sprint(e.Unix())}}.Encode()
		}
	}
	do := func(b *testing.B, h http.Handler, path string, gzip bool) {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		req.Header.Set("X-Scope-OrgID", "0")
		if gzip {
			req.Header.Set("Accept-Encoding", "gzip")
		}
		// A writer that keeps nothing, as a connection's buffered writer does:
		// a recorder's growing buffer would dominate the measure.
		rec := &benchAnswerWriter{h: http.Header{}, code: http.StatusOK}
		h.ServeHTTP(rec, req)
		if rec.code != http.StatusOK || rec.n == 0 {
			b.Fatalf("%s: %d (%d bytes)", path, rec.code, rec.n)
		}
	}
	// A refresh that moves the window by a second: a new response-cache key
	// whose rows come from the log window cache.
	b.Run("log/shifted/identity", func(b *testing.B) {
		h := newHandler(b, false)
		for i := 0; i < 3; i++ {
			do(b, h, target("log", 0), false)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			e := end.Add(time.Duration(i%600+1) * time.Second)
			do(b, h, "/loki/api/v1/query_range?"+url.Values{"query": {`{app="api"}`}, "limit": {"1000"},
				"start": {fmt.Sprint(e.Add(-time.Hour).UnixNano())}, "end": {fmt.Sprint(e.UnixNano())}}.Encode(), false)
		}
	})
	// A metric query after the log answer of its selector over a wider window:
	// the stats layer finds the log answer's record and prices the metric's
	// bytes from it. Each iteration moves the metric's window by a millisecond
	// (a new response-cache key).
	b.Run("metric/miss/afterlog/identity", func(b *testing.B) {
		h := newHandler(b, false)
		do(b, h, target("log", 0), false)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			e := end.Add(-time.Duration(i+1) * time.Millisecond)
			do(b, h, "/loki/api/v1/query_range?"+url.Values{"query": {`sum by (app) (count_over_time({app="api"}[1m]))`}, "step": {"60"},
				"start": {fmt.Sprint(e.Add(-3 * time.Minute).UnixNano())}, "end": {fmt.Sprint(e.UnixNano())}}.Encode(), false)
		}
	})
	for _, kind := range []string{"log", "metric", "instant"} {
		for _, gzip := range []bool{false, true} {
			enc := "identity"
			if gzip {
				enc = "gzip"
			}
			b.Run(kind+"/miss/"+enc, func(b *testing.B) {
				h := newHandler(b, gzip)
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					do(b, h, target(kind, i+1), gzip)
				}
			})
			b.Run(kind+"/hit/"+enc, func(b *testing.B) {
				h := newHandler(b, gzip)
				path := target(kind, 0)
				do(b, h, path, gzip)
				do(b, h, path, gzip)
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					do(b, h, path, gzip)
				}
			})
		}
	}
}

type benchAnswerWriter struct {
	h    http.Header
	code int
	n    int
}

func (w *benchAnswerWriter) Header() http.Header { return w.h }
func (w *benchAnswerWriter) Write(b []byte) (int, error) {
	w.n += len(b)
	return len(b), nil
}
func (w *benchAnswerWriter) WriteHeader(code int) { w.code = code }
