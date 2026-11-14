package proxy

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// BenchmarkTranslateQueryMiss is the translate-on-miss path of every request: each iteration translates a query the
// translation cache has not seen, so it runs the string translator and stores the answer.
func BenchmarkTranslateQueryMiss(b *testing.B) {
	p, err := New(Config{BackendURL: "http://unused", Cache: cache.New(time.Minute, 1000), LogLevel: "error", LabelStyle: LabelStyleUnderscores})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := p.translateQueryOpts(context.Background(), `{app="api", namespace="prod"} |= "error `+strconv.Itoa(i)+`" != "debug"`, false); err != nil {
			b.Fatal(err)
		}
	}
}

// translateMissShapes are the query shapes of BenchmarkTranslateQueryMissShapes: the constructs the typed lowering
// serves (log queries with line, pattern and label filters and no parser) and the ones it does not, each with a
// text that differs per iteration so the translation cache misses.
var translateMissShapes = []struct {
	name  string
	query func(i string) string
}{
	{"native line filters", func(i string) string { return `{app="api", namespace="prod"} |= "error ` + i + `" != "debug"` }},
	{"native pattern filter", func(i string) string { return `{app="api"} |> "<_> GET <path> ` + i + `" |= "200"` }},
	{"native label filters", func(i string) string { return `{app="api"} |= "x" | status >= 500 | path =~ "/api/` + i + `.*"` }},
	{"derived level filter", func(i string) string { return `{service_name="api"} |= "error ` + i + `" | detected_level="error"` }},
	{"parser logfmt", func(i string) string { return `{app="api"} | logfmt | status >= 500 | path = "/api/` + i + `"` }},
	{"parser json", func(i string) string { return `{app="api"} |= "error ` + i + `" | json | status="500"` }},
	{"metric rate", func(i string) string { return `sum by (level) (rate({app="api"} |= "error ` + i + `" [5m]))` }},
	{"metric unwrap", func(i string) string { return `sum(rate({app="api"} | json | unwrap bytes [5m])) + ` + i }},
	{"metric unwrap keep", func(i string) string {
		return `sum(quantile_over_time(0.9, {app="api"} |= "x ` + i + `" | json | keep app, d | unwrap d [5m]))`
	}},
}

// benchTranslateMissShapes runs every shape of translateMissShapes on a proxy built from cfg.
func benchTranslateMissShapes(b *testing.B, cfg Config) {
	cfg.BackendURL, cfg.Cache, cfg.LogLevel, cfg.LabelStyle = "http://unused", cache.New(time.Minute, 1000), "error", LabelStyleUnderscores
	for _, shape := range translateMissShapes {
		b.Run(shape.name, func(b *testing.B) {
			p, err := New(cfg)
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(func() { _ = p.Shutdown(context.Background()) })
			ctx := context.Background()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := p.translateQueryOpts(ctx, shape.query(strconv.Itoa(i)), false); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkTranslateQueryMissShapes is the translate-on-miss path of every request, per query shape: what a
// translation costs the request that finds the translation cache empty. Run on a base commit and a branch with
// benchstat; BenchmarkTranslateQueryMissShapesTranslator is the same code with the typed lowering off.
func BenchmarkTranslateQueryMissShapes(b *testing.B) { benchTranslateMissShapes(b, Config{}) }
