package proxy

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// BenchmarkSuccessPathFlights measures a successful log query_range through
// every middleware, one request at a time: a response cache hit (the
// repeat), a miss that runs the handler against a stub VictoriaLogs (a new
// limit each time), a form POST (never cached, so it always runs), and a GET
// with -stream-response. The request flights sit on every one of these paths;
// none may get slower.
func BenchmarkSuccessPathFlights(b *testing.B) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasPrefix(r.URL.Path, "/select/") {
			w.WriteHeader(http.StatusOK)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"_time":"2023-11-14T22:13:20Z","_msg":"hello","_stream":"{app=\"api\"}","app":"api"}` + "\n"))
	}))
	defer backend.Close()
	mux := func(stream bool) *http.ServeMux {
		p, err := New(Config{
			BackendURL:     backend.URL,
			Cache:          cache.New(60*time.Second, 100000),
			CompatCache:    cache.New(60*time.Second, 100000),
			LogLevel:       "error",
			StreamResponse: stream,
		})
		if err != nil {
			b.Fatal(err)
		}
		b.Cleanup(func() { _ = p.Shutdown(b.Context()) })
		m := http.NewServeMux()
		p.RegisterRoutes(m)
		return m
	}
	buffered, streamed := mux(false), mux(true)
	const base = "/loki/api/v1/query_range?query=%7Bapp%3D%22api%22%7D&start=1700000000&end=1700003600&limit="
	serve := func(h http.Handler, req *http.Request) {
		req.Header.Set("X-Scope-OrgID", "0")
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			b.Fatalf("status %d: %s", rec.Code, rec.Body.String())
		}
	}
	get := func(h http.Handler, target string) { serve(h, httptest.NewRequest(http.MethodGet, target, nil)) }
	b.Run("hit", func(b *testing.B) {
		get(buffered, base+"100")
		b.ReportAllocs()
		for b.Loop() {
			get(buffered, base+"100")
		}
	})
	b.Run("miss", func(b *testing.B) {
		b.ReportAllocs()
		i := 1000
		for b.Loop() {
			i++
			get(buffered, base+strconv.Itoa(i))
		}
	})
	b.Run("post", func(b *testing.B) {
		form := url.Values{"query": {`{app="api"}`}, "start": {"1700000000"}, "end": {"1700003600"}, "limit": {"100"}}.Encode()
		b.ReportAllocs()
		for b.Loop() {
			req := httptest.NewRequest(http.MethodPost, "/loki/api/v1/query_range", strings.NewReader(form))
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			serve(buffered, req)
		}
	})
	b.Run("stream", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			get(streamed, base+"100")
		}
	})
}
