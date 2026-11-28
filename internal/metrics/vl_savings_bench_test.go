package metrics

import (
	"testing"
	"time"
)

// BenchmarkRecordServedAndReuse is what one client request adds to the savings families: its served class and one
// reused answer.
func BenchmarkRecordServedAndReuse(b *testing.B) {
	m := NewMetrics()
	w := VLWork{Calls: 2, Duration: time.Millisecond, Bytes: 300, Rows: 7}
	b.ReportAllocs()
	for b.Loop() {
		m.RecordServedRequest("query_range", "/loki/api/v1/query_range", ServedPartial, time.Millisecond, 1000, w)
		m.RecordVLReuse("query_range", "/loki/api/v1/query_range", ReuseWindow, ReuseL1Memory, w, true)
	}
}
