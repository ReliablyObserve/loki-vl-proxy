package proxy

import (
	"context"
	"math"
	"net/http"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/lokierr"
)

// The sample cap is a flag, bounded by the memory the process may use, and never below the cap this proxy had when it
// was compiled in.
// conformance: limits/metric-sample-cap-is-a-flag-and-memory-bound, limits/every-cap-is-a-flag
func TestEffectiveMetricSampleLimitFollowsTheFlagAndTheMemoryLimit(t *testing.T) {
	const gib = int64(1) << 30
	for _, tc := range []struct {
		name               string
		configured, memory int64
		want               int64
	}{
		{"unset uses the default", 0, math.MaxInt64, 10_000_000},
		{"negative uses the default", -5, math.MaxInt64, 10_000_000},
		{"a configured value stands without a memory limit", 25_000_000, math.MaxInt64, 25_000_000},
		{"a zero memory limit means none", 3_000_000, 0, 3_000_000},
		{"the default is lowered by a 2 GiB limit to a quarter at 96 bytes", 0, 2 * gib, 2 * gib / 4 / 96},
		{"a configured value above the memory bound is lowered too", 50_000_000, 2 * gib, 2 * gib / 4 / 96},
		{"a configured value below the memory bound stands", 3_000_000, 2 * gib, 3_000_000},
		{"a large limit leaves the default alone", 0, 64 * gib, 10_000_000},
		{"a small limit never goes below the old compiled-in cap", 0, 64 << 20, maxMetricEvalSamples},
		{"a configured value below the old cap is raised to it", 500, math.MaxInt64, maxMetricEvalSamples},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := effectiveMetricSampleLimit(tc.configured, tc.memory); got != tc.want {
				t.Fatalf("effectiveMetricSampleLimit(%d, %d) = %d, want %d", tc.configured, tc.memory, got, tc.want)
			}
		})
	}
	if got := (&Proxy{orderedJSONMaxSamples: 3_000_000}).maxMetricSamples(); got < metricSampleFloor || got > 3_000_000 {
		t.Fatalf("maxMetricSamples = %d, want a value in [%d, 3000000]", got, metricSampleFloor)
	}
}

// The request carries its cap to the evaluators that have no Proxy; a context without one gets the default.
// conformance: limits/metric-sample-cap-is-a-flag-and-memory-bound
func TestMetricSampleLimitContext(t *testing.T) {
	if got := metricSampleLimit(context.Background()); got < metricSampleFloor {
		t.Fatalf("default limit %d is below the floor %d", got, metricSampleFloor)
	}
	if got := metricSampleLimit(withMetricSampleLimit(context.Background(), 7)); got != 7 {
		t.Fatalf("carried limit %d, want 7", got)
	}
	if got := metricSampleLimit(withMetricSampleLimit(context.Background(), 0)); got < metricSampleFloor {
		t.Fatalf("a zero carried limit must fall back to the default, got %d", got)
	}
}

// An answer past the cap is Loki's response-too-large answer, and the log line (not the client) names the flag; below
// the cap the same answer is built.
// conformance: limits/metric-sample-cap-is-a-flag-and-memory-bound
func TestOrderedMetricPastTheSampleCapIsATypedLimitError(t *testing.T) {
	plan, ok := compileOrderedJSONMetric(`count_over_time({app="test"}|json|drop __error__[10s])`)
	if !ok {
		t.Fatal("plan")
	}
	start := time.Unix(1700000000, 0).UTC()
	series := map[string]manualSeriesSamples{}
	for _, name := range []string{"a", "b", "c"} {
		s := manualSeriesSamples{Metric: map[string]string{"app": name}}
		for i := 0; i < 4; i++ {
			s.Samples = append(s.Samples, rangeMetricSample{ts: start.Add(time.Duration(i) * 10 * time.Second).UnixNano(), value: 1})
		}
		series[name] = s
	}
	end := start.Add(30 * time.Second)
	if _, err := buildOrderedJSONMetric(withMetricSampleLimit(context.Background(), 1_000_000), plan, series, start, end, 10*time.Second, true, defaultOrderedJSONMetricMaxBytes); err != nil {
		t.Fatalf("below the cap: %v", err)
	}
	_, err := buildOrderedJSONMetric(withMetricSampleLimit(context.Background(), 5), plan, series, start, end, 10*time.Second, true, defaultOrderedJSONMetricMaxBytes)
	if err == nil {
		t.Fatal("an answer of 12 samples was built under a cap of 5")
	}
	typed, ok := err.(*lokierr.Error)
	if !ok {
		t.Fatalf("error %T %v is not a typed limit error", err, err)
	}
	flag, value, _ := typed.Limit()
	if flag != flagOrderedJSONMetricMaxSamples || value != 5 {
		t.Fatalf("limit flag %q value %d, want %q 5", flag, value, flagOrderedJSONMetricMaxSamples)
	}
	if typed.Status() != http.StatusInternalServerError {
		t.Fatalf("status %d, want 500", typed.Status())
	}
}
