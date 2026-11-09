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
