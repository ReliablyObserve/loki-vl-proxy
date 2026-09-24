package proxy

import (
	"context"
	"testing"
)

// TestCapSeriesByTotalCount_MapVariant covers the map-based helper used by the
// raw-scan path via buildManualRangeMetricMatrix.
func TestCapSeriesByTotalCount_MapVariant(t *testing.T) {
	series := map[string]manualSeriesSamples{
		"{pod=\"noise-a\"}": {Metric: map[string]string{"pod": "noise-a"}, Samples: []rangeMetricSample{{ts: 1, value: 1}}},
		"{pod=\"noise-b\"}": {Metric: map[string]string{"pod": "noise-b"}, Samples: []rangeMetricSample{{ts: 1, value: 1}}},
		"{pod=\"busy-c\"}":  {Metric: map[string]string{"pod": "busy-c"}, Samples: []rangeMetricSample{{ts: 1, value: 40}, {ts: 2, value: 60}}},
		"{pod=\"busy-d\"}":  {Metric: map[string]string{"pod": "busy-d"}, Samples: []rangeMetricSample{{ts: 1, value: 30}, {ts: 2, value: 50}}},
	}
	capped := capSeriesByTotalCount(series, 2)
	if len(capped) != 2 {
		t.Fatalf("expected 2, got %d", len(capped))
	}
	if _, ok := capped["{pod=\"busy-c\"}"]; !ok {
		t.Errorf("busiest series busy-c dropped: %v", mapKeys(capped))
	}
	if _, ok := capped["{pod=\"busy-d\"}"]; !ok {
		t.Errorf("second-busiest series busy-d dropped: %v", mapKeys(capped))
	}
	if _, ok := capped["{pod=\"noise-a\"}"]; ok {
		t.Errorf("noise series survived top-N cap: %v", mapKeys(capped))
	}
	// Disabled / fits-already passthrough.
	if got := capSeriesByTotalCount(series, 0); len(got) != 4 {
		t.Errorf("maxSeries=0 should passthrough, got %d", len(got))
	}
	if got := capSeriesByTotalCount(series, 10); len(got) != 4 {
		t.Errorf("fits-already should passthrough, got %d", len(got))
	}
}

// TestResolvedMaxStatsQuerySeries_Default verifies the 500 default and override.
func TestResolvedMaxStatsQuerySeries_Default(t *testing.T) {
	if got := (&Proxy{maxStatsQuerySeries: 0}).resolvedMaxStatsQuerySeries(context.Background()); got != 500 {
		t.Errorf("default: expected 500, got %d", got)
	}
	if got := (&Proxy{maxStatsQuerySeries: 1200}).resolvedMaxStatsQuerySeries(context.Background()); got != 1200 {
		t.Errorf("override: expected 1200, got %d", got)
	}
	if got := (*Proxy)(nil).resolvedMaxStatsQuerySeries(context.Background()); got != 500 {
		t.Errorf("nil receiver: expected 500, got %d", got)
	}
}

func mapKeys(m map[string]manualSeriesSamples) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
