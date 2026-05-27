package metrics

import (
	"testing"
	"time"
)

func BenchmarkHistogramObserve(b *testing.B) {
	h := newHistogramWithBuckets(defaultBuckets)
	b.RunParallel(func(pb *testing.PB) {
		v := 0.001
		for pb.Next() {
			h.observe(v)
			v += 0.001
			if v > 10.0 {
				v = 0.001
			}
		}
	})
}

func BenchmarkQueryTrackerRecord(b *testing.B) {
	qt := NewQueryTracker(10000)
	queries := []string{
		`{app="api"} |= "error"`,
		`{app="web"} | json | line_format "{{.msg}}"`,
		`rate({app="api"}[5m])`,
		`{namespace="prod"} |~ "timeout|deadline"`,
		`sum by (level)(count_over_time({app="api"}[1m]))`,
	}
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			qt.Record("/loki/api/v1/query_range", queries[i%len(queries)], time.Millisecond*50, false)
			i++
		}
	})
}
