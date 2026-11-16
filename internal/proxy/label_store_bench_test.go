package proxy

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// BenchmarkLabelsCacheStore writes a 40-label listing over a cached one, as a
// refresh of a /labels answer does.
func BenchmarkLabelsCacheStore(b *testing.B) {
	p, err := New(Config{BackendURL: "http://127.0.0.1:1", Cache: cache.New(time.Minute, 1000), LogLevel: "error"})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	labels := make([]string, 40)
	for i := range labels {
		labels[i] = fmt.Sprintf("label_%02d", i)
	}
	p.storeLabelsCache("labels", "bench", labels, 5*time.Minute)
	b.ReportAllocs()
	for b.Loop() {
		p.storeLabelsCache("labels", "bench", labels, 5*time.Minute)
	}
}
