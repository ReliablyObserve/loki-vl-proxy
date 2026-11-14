package proxy

import (
	"fmt"
	"testing"
	"time"
)

// BenchmarkUnwrapWindowMatrix: the window evaluator of an unwrap function over merged buckets (a 5m range at a 60s
// step over 6h, 1m buckets, 40 series): avg and max over time as before, and stddev and stdvar over time from the
// mean, count and deviation of each bucket.
func BenchmarkUnwrapWindowMatrix(b *testing.B) {
	start := time.Unix(1_800_000_000, 0)
	step, window := time.Minute, 5*time.Minute
	end := start.Add(6 * time.Hour)
	series := map[string]manualSeriesSamples{}
	for i := 0; i < 40; i++ {
		entry := manualSeriesSamples{Metric: map[string]string{"app": fmt.Sprintf("app-%02d", i)}}
		for at := start.Add(-window); !at.After(end); at = at.Add(time.Minute) {
			ts := at.UnixNano()
			entry.Samples = append(entry.Samples, rangeMetricSample{ts: ts, value: float64(100 + i%7)})
			entry.Counts = append(entry.Counts, rangeMetricSample{ts: ts, value: float64(1 + i%3)})
			entry.Deviations = append(entry.Deviations, rangeMetricSample{ts: ts, value: float64(i % 4)})
			entry.PresentBuckets = append(entry.PresentBuckets, ts)
		}
		series[canonicalLabelsKey(entry.Metric)] = entry
	}
	for _, fn := range []string{"avg", "max", "stddev", "stdvar"} {
		b.Run(fn, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if _, err := buildHitsRangeMetricMatrix(fn, series, start, end, step, window, 1<<30); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkDeviationPartialsFold: pairing VictoriaLogs' three bucket results of stddev and stdvar over time (40
// series, one group each, 6h of 1m buckets) into the series the window evaluator reads.
func BenchmarkDeviationPartialsFold(b *testing.B) {
	start := time.Unix(1_800_000_000, 0)
	var means, counts, devs []byte
	for _, out := range []*[]byte{&means, &counts, &devs} {
		*out = append(*out, '[')
	}
	for m := 0; m < 6*60; m++ {
		sep := ""
		if m > 0 {
			sep = ","
		}
		ts := start.Add(time.Duration(m) * time.Minute).Unix()
		means = fmt.Appendf(means, `%s[%d,"%d.5"]`, sep, ts, 100+m%9)
		counts = fmt.Appendf(counts, `%s[%d,"%d"]`, sep, ts, 1+m%4)
		devs = fmt.Appendf(devs, `%s[%d,"%d.25"]`, sep, ts, m%3)
	}
	for _, out := range []*[]byte{&means, &counts, &devs} {
		*out = append(*out, ']')
	}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		var d deviationPartials
		for s := 0; s < 40; s++ {
			key := fmt.Sprintf("app=a%02d", s)
			metric := map[string]string{"app": key}
			d.add(key, "app\x00"+key+"\x00", metric, "__lvp_avg", means, start, time.Minute)
			d.add(key, "app\x00"+key+"\x00", metric, "__sample_count", counts, start, time.Minute)
			d.add(key, "app\x00"+key+"\x00", metric, deviationBucketName, devs, start, time.Minute)
		}
		series := make(map[string]manualSeriesSamples, 40)
		d.fold(series)
		if len(series) != 40 {
			b.Fatalf("%d series", len(series))
		}
	}
}
