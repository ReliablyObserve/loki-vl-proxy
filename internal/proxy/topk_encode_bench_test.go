package proxy

import (
	"context"
	"fmt"
	"testing"
	"time"

	logqlpkg "github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
)

var benchGroup = &logqlpkg.Grouping{Without: true, Labels: []string{"host"}}

// BenchmarkEncodeTopK ranks 500 series over 2,000 tumbling steps, the shape of
// a heavy topk over a grouped window metric.
func BenchmarkEncodeTopK(b *testing.B) {
	const series, steps = 500, 2000
	stats := &slidingStats{windowGrid: true, ids: map[string]int32{}}
	for i := 0; i < series; i++ {
		stats.keys = append(stats.keys, fmt.Sprintf("app-%04d", i))
		stats.lokiNames = []string{"app"}
		for k := 0; k < steps; k++ {
			stats.records = append(stats.records, slidingRecord{series: int32(i), grid: slidingGridA, index: int32(k), count: float64((i*7+k*13)%97 + 1)})
		}
	}
	start := time.Unix(1700000000, 0)
	for _, tc := range []struct {
		name  string
		group bool
	}{{"ungrouped", false}, {"grouped by app", true}} {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for n := 0; n < b.N; n++ {
				topK := rangeTopK{k: 5, descending: true}
				if tc.group {
					topK.group = benchGroup
				}
				if _, err := stats.encodeTopK(context.Background(), false, 1, steps, 0, topK, start, time.Minute, 1<<30); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
