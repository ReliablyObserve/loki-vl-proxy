package proxy

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// conformance: limits/metric-sample-cap-is-a-flag-and-memory-bound
func BenchmarkBuildOrderedJSONMetricSamples(b *testing.B) {
	plan, ok := compileOrderedJSONMetric(`count_over_time({app="test"}|json|drop __error__[10s])`)
	if !ok {
		b.Fatal("plan")
	}
	start := time.Unix(1700000000, 0).UTC()
	const seriesCount, points = 1000, 300
	series := make(map[string]manualSeriesSamples, seriesCount)
	for n := 0; n < seriesCount; n++ {
		name := fmt.Sprintf("s%04d", n)
		s := manualSeriesSamples{Metric: map[string]string{"app": name}}
		for i := 0; i < points; i++ {
			s.Samples = append(s.Samples, rangeMetricSample{ts: start.Add(time.Duration(i) * 10 * time.Second).UnixNano(), value: 1})
		}
		series[name] = s
	}
	end := start.Add(points * 10 * time.Second)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := buildOrderedJSONMetric(context.Background(), plan, series, start, end, 10*time.Second, true, defaultOrderedJSONMetricMaxBytes); err != nil {
			b.Fatal(err)
		}
	}
}
