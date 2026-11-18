package proxy

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// BenchmarkQueryRequestValidation is what a query request costs before its handler reads the query: the request
// rewrite (lowercaseQueryKeywords: the query's text, the planning budget's gate, the operator limits on the query's
// size) and the handler's validateQuery. hit sends one query again and again (the memos hold it, the common case:
// dashboards refresh the same query); miss sends a query it has not seen (parse, semantic pass, tree checks; the
// request text and the validation are stored). Both run with the request's telemetry in the context, as a request
// that went through the request logger does.
func BenchmarkQueryRequestValidation(b *testing.B) {
	c := cache.New(time.Minute, 100)
	defer c.Close()
	p, err := New(Config{BackendURL: "http://127.0.0.1:1", Cache: c, LogLevel: "error"})
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = p.Shutdown(context.Background()) }()
	end := time.Unix(1794570660, 0)
	handler := p.lowercaseQueryKeywords(func(w http.ResponseWriter, r *http.Request) {
		if _, ok := p.validateQuery(w, r.FormValue("query"), "query_range"); !ok {
			b.Fatal("a valid query was refused")
		}
	})
	request := func(query string) *http.Request {
		params := url.Values{"query": {query}, "start": {strconv.FormatInt(end.Add(-time.Hour).UnixNano(), 10)}, "end": {strconv.FormatInt(end.UnixNano(), 10)}, "step": {"60"}}
		r := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+params.Encode(), nil)
		return r.WithContext(context.WithValue(r.Context(), requestTelemetryKey, &requestTelemetry{}))
	}
	const shape = `sum by (level) (rate({app="api-gateway"} | json | status>=500 [5m]))`
	b.Run("hit", func(b *testing.B) {
		r := request(shape)
		w := httptest.NewRecorder()
		handler(w, r) // fill the memos
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			r := request(shape)
			handler(w, r)
		}
	})
	// A query over 4 KiB is not cached anywhere: every request validates it, as before.
	for _, long := range []struct{ name, query string }{
		{"long_log_10KB", `{app="api-gateway"}` + strings.Repeat(` | json`, 1400)},
		{"long_nested_metric_7KB", strings.Repeat("sum(", 800) + `rate({app="api-gateway"}[5m])` + strings.Repeat(")", 800)},
		{"long_wrapped_metric_20KB", strings.Repeat("(", 9000) + `rate({app="api-gateway"}[5m])` + strings.Repeat(")", 9000)},
	} {
		b.Run(long.name, func(b *testing.B) {
			w := httptest.NewRecorder()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				handler(w, request(long.query))
			}
		})
	}
	b.Run("miss", func(b *testing.B) {
		w := httptest.NewRecorder()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			// A new query each time: the memos keep up to a thousand of them, then stop taking more, so the
			// parse is paid on every iteration.
			r := request(`sum by (level) (rate({app="api-gateway", pod="p` + strconv.Itoa(i) + `"} | json | status>=500 [5m]))`)
			handler(w, r)
		}
	})
}

// BenchmarkTranslateQueryMissInRequest is the translation of a query the translation cache has not seen, inside a
// request (its telemetry in the context): a log query the query plan's lowering writes, and a metric query it
// leaves to the string translator.
func BenchmarkTranslateQueryMissInRequest(b *testing.B) {
	c := cache.New(time.Minute, 100)
	defer c.Close()
	p, err := New(Config{BackendURL: "http://127.0.0.1:1", Cache: c, LogLevel: "error", LabelStyle: LabelStyleUnderscores})
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = p.Shutdown(context.Background()) }()
	for _, shape := range []struct{ name, query string }{
		{"lowered_log", `{app="api", namespace="prod"} |= "error %d" != "debug"`},
		{"translator_metric", `sum by (level) (rate({app="api", pod="p%d"} | json | status>=500 [5m]))`},
	} {
		b.Run(shape.name, func(b *testing.B) {
			ctx := context.WithValue(context.Background(), requestTelemetryKey, &requestTelemetry{})
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if _, err := p.translateQueryOpts(ctx, fmt.Sprintf(shape.query, i), false); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkSelectorRouteValidation is a selector route's request before its handler: the request rewrite and the
// route's selector validation (labels with a query), for a selector seen before and for one it has not.
func BenchmarkSelectorRouteValidation(b *testing.B) {
	c := cache.New(time.Minute, 100)
	defer c.Close()
	p, err := New(Config{BackendURL: "http://127.0.0.1:1", Cache: c, LogLevel: "error"})
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = p.Shutdown(context.Background()) }()
	handler := p.lowercaseQueryKeywords(p.lokiQueryParamValidation("labels", func(http.ResponseWriter, *http.Request) {}))
	end := time.Unix(1794570660, 0)
	request := func(selector string) *http.Request {
		params := url.Values{"query": {selector}, "start": {strconv.FormatInt(end.Add(-time.Hour).UnixNano(), 10)}, "end": {strconv.FormatInt(end.UnixNano(), 10)}}
		r := httptest.NewRequest(http.MethodGet, "/loki/api/v1/labels?"+params.Encode(), nil)
		ctx := context.WithValue(r.Context(), requestTelemetryKey, &requestTelemetry{})
		ctx = context.WithValue(ctx, requestRouteMetaKey, requestRouteMeta{endpoint: "labels", route: "/loki/api/v1/labels"})
		return r.WithContext(ctx)
	}
	b.Run("hit", func(b *testing.B) {
		w := httptest.NewRecorder()
		handler(w, request(`{app="api-gateway", namespace="prod"}`))
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			handler(w, request(`{app="api-gateway", namespace="prod"}`))
		}
	})
	b.Run("miss", func(b *testing.B) {
		w := httptest.NewRecorder()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			handler(w, request(`{app="api-gateway", pod="p`+strconv.Itoa(i)+`"}`))
		}
	})
}
