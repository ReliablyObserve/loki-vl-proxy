package proxy

import (
	"testing"
)

func BenchmarkAggregateManualWindowCount(b *testing.B) {
	samples := make([]rangeMetricSample, 100)
	for i := range samples {
		samples[i] = rangeMetricSample{ts: int64(i * 10), value: float64(i)}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		aggregateManualWindow("count_over_time", 0, samples, 0, 990, 99.0)
	}
}

func BenchmarkAggregateManualWindowAvg(b *testing.B) {
	samples := make([]rangeMetricSample, 100)
	for i := range samples {
		samples[i] = rangeMetricSample{ts: int64(i * 10), value: float64(i)}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		aggregateManualWindow("avg", 0, samples, 0, 990, 99.0)
	}
}

func BenchmarkAggregateManualWindowQuantile(b *testing.B) {
	samples := make([]rangeMetricSample, 100)
	for i := range samples {
		samples[i] = rangeMetricSample{ts: int64(i * 10), value: float64(i)}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		aggregateManualWindow("quantile", 0.5, samples, 0, 990, 99.0)
	}
}
