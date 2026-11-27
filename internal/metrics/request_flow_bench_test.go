package metrics

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

// BenchmarkRequestFlowMetrics is the per-request record of the flow histograms and their share of a /metrics
// answer with 20 routes, which all read a cache (Render) or none (RenderNoReads: no reads series).
func BenchmarkRequestFlowMetrics(b *testing.B) {
	flow := RequestFlow{ParseCount: 1, CacheLookups: 4, CacheHits: 2, VLCalls: 1, Cache: time.Millisecond, VL: 10 * time.Millisecond, CacheReads: [3]int{4, 1, 0}}
	m := NewMetrics()
	for i := range 20 {
		m.RecordRequestFlow("query_range", fmt.Sprintf("/loki/api/v1/route%d", i), flow)
	}
	unread := flow
	unread.CacheReads = [3]int{}
	quiet := NewMetrics()
	for i := range 20 {
		quiet.RecordRequestFlow("query_range", fmt.Sprintf("/loki/api/v1/route%d", i), unread)
	}
	b.Run("RenderNoReads", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			var sb strings.Builder
			quiet.writeRequestFlowMetrics(&sb)
		}
	})
	b.Run("Record", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			m.RecordRequestFlow("query_range", "/loki/api/v1/route1", flow)
		}
	})
	b.Run("Render", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			var sb strings.Builder
			m.writeRequestFlowMetrics(&sb)
		}
	})
}
