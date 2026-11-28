package proxy

import (
	"context"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// unpackSumWrappedRequestQueries are sums over an unpack under an empty by clause, a binary operation, a label_replace
// call and behind a keep, as the handlers translate them (semantics/ungrouped-sum-skips-unpack), and a grouped sum the
// plan leaves to the string translator.
var unpackSumWrappedRequestQueries = []string{
	`sum by () (count_over_time({app="x"} | unpack [5m]))`,
	`sum(count_over_time({app="x"} | unpack [5m])) * 2`,
	`label_replace(sum(rate({app="x"} | unpack | drop a [5m])), "d", "x", "", "")`,
	`sum(count_over_time({app="x"} | unpack | keep a [5m]))`,
	`sum by (app) (count_over_time({app="x"} | unpack [5m]))`,
}

// BenchmarkTranslateUnpackSumWrapped is the translation of a request's query with no translation cache: the screens,
// the plan and the lowering, or the string translator.
func BenchmarkTranslateUnpackSumWrapped(b *testing.B) {
	p, err := New(Config{BackendURL: "http://unused", Cache: cache.New(time.Minute, 100), LogLevel: "error"})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	p.translationCache = nil
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := p.translateQueryOpts(ctx, unpackSumWrappedRequestQueries[i%len(unpackSumWrappedRequestQueries)], false); err != nil {
			b.Fatal(err)
		}
	}
}

// unpackGroupedControlQueries name a sum and an unpack, so the unpack screen passes them, but the plan leaves them to
// the string translator: the screen's cost on the queries it does not serve.
var unpackGroupedControlQueries = []string{
	`sum by (app) (count_over_time({app="x"} | unpack [5m]))`,
	`sum(max_over_time({app="x"} | unpack | unwrap a [5m]))`,
	`sum_over_time({app="x"} | unpack | unwrap a [5m])`,
	`sum(rate({app="x"} | unpack | keep a [5m]))`,
}

// BenchmarkTranslateUnpackGroupedControl is BenchmarkTranslateUnpackSumWrapped over the control queries.
func BenchmarkTranslateUnpackGroupedControl(b *testing.B) {
	p, err := New(Config{BackendURL: "http://unused", Cache: cache.New(time.Minute, 100), LogLevel: "error"})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	p.translationCache = nil
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := p.translateQueryOpts(ctx, unpackGroupedControlQueries[i%len(unpackGroupedControlQueries)], false); err != nil {
			b.Fatal(err)
		}
	}
}
