package proxy

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"runtime/debug"
	"strconv"
	"strings"
	"time"
)

// Evaluation bounds shared by proxy-side metric evaluation (ordered JSON
// metrics and manual range-metric budgets). The point bound is Loki's
// query_range resolution limit, lokiMaxPointsPerSeries.
//
// maxMetricEvalSamples stays the bound on the evaluation points of one request and the size hint of a result slice.
// The samples a proxy-evaluated answer may hold are a separate, operator-set limit: see effectiveMetricSampleLimit.
const maxMetricEvalSamples = 1000000

const (
	// defaultMetricSampleLimit is -ordered-json-metric-max-samples when it is 0 or unset.
	defaultMetricSampleLimit int64 = 10_000_000
	// metricSampleFloor is the smallest effective cap: the limit this proxy had before it was configurable.
	metricSampleFloor int64 = maxMetricEvalSamples
	// metricSampleMemoryBytes is the memory one sample is budgeted at while a result is held (the point itself, its
	// slice growth and the response text built from it), and metricSampleMemoryShare the share of the runtime
	// memory limit one request may spend on its samples.
	metricSampleMemoryBytes int64 = 96
	metricSampleMemoryShare int64 = 4
)

// effectiveMetricSampleLimit is the cap on the samples of one proxy-evaluated metric answer: the configured limit
// (0 means defaultMetricSampleLimit), lowered so one request cannot spend more than a quarter of the runtime memory
// limit (GOMEMLIMIT) at metricSampleMemoryBytes per sample, and never below metricSampleFloor. Without a runtime
// memory limit the configured value stands.
func effectiveMetricSampleLimit(configured, memoryLimit int64) int64 {
	limit := configured
	if limit <= 0 {
		limit = defaultMetricSampleLimit
	}
	if memoryLimit > 0 && memoryLimit < math.MaxInt64 {
		if byMemory := memoryLimit / metricSampleMemoryShare / metricSampleMemoryBytes; byMemory < limit {
			limit = byMemory
		}
	}
	return max(limit, metricSampleFloor)
}

type metricSampleLimitKey struct{}

// withMetricSampleLimit carries the request's effective sample cap to the evaluators that have no Proxy.
func withMetricSampleLimit(ctx context.Context, limit int64) context.Context {
	return context.WithValue(ctx, metricSampleLimitKey{}, limit)
}

// metricSampleLimit is the request's sample cap, or the default one for a context that carries none.
func metricSampleLimit(ctx context.Context) int64 {
	if limit, ok := ctx.Value(metricSampleLimitKey{}).(int64); ok && limit > 0 {
		return limit
	}
	return effectiveMetricSampleLimit(0, debug.SetMemoryLimit(-1))
}

// maxMetricSamples is the cap on the samples of one proxy-evaluated answer on this proxy.
func (p *Proxy) maxMetricSamples() int64 {
	return effectiveMetricSampleLimit(p.orderedJSONMaxSamples, debug.SetMemoryLimit(-1))
}

var errMetricEvalBudget = errors.New("metric evaluation exceeds its point or sample limit")

// metricEvalPointCount returns the number of evaluation points in [start, end]
// at step, rejecting invalid or unbounded windows before any backend work.
func metricEvalPointCount(start, end time.Time, step time.Duration) (int, error) {
	if step <= 0 || end.Before(start) {
		return 0, fmt.Errorf("invalid metric evaluation bounds or step")
	}
	distance := end.Sub(start)
	if distance == time.Duration(1<<63-1) || distance/step > lokiMaxPointsPerSeries {
		return 0, errMetricEvalBudget
	}
	return int(distance/step) + 1, nil
}

func seriesKeyFromMetric(metric map[string]string) string {
	if len(metric) == 0 {
		return "{}"
	}
	data, _ := json.Marshal(metric)
	return string(data)
}

func parseValueToFloat(v interface{}) float64 {
	switch val := v.(type) {
	case string:
		f, _ := strconv.ParseFloat(val, 64)
		return f
	case float64:
		return val
	case json.Number:
		f, _ := val.Float64()
		return f
	default:
		return 0
	}
}

// parseLokiDuration parses Loki/Prometheus-style duration strings like "5m", "1h", "30s", "1d".
func parseLokiDuration(s string) time.Duration {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0
	}

	// Try Go's time.ParseDuration first (handles "5m", "1h30m", "30s", etc.)
	if d, err := time.ParseDuration(s); err == nil {
		return d
	}

	// Fall back to Prometheus/Loki units (d/w/y) and mixed-unit forms.
	if d, ok := parsePrometheusStyleDuration(s); ok {
		return d
	}

	// Handle "d" suffix (days) — not supported by Go
	if strings.HasSuffix(s, "d") {
		n, err := strconv.Atoi(s[:len(s)-1])
		if err == nil {
			return time.Duration(n) * 24 * time.Hour
		}
	}

	// Try as seconds (numeric string)
	if f, err := strconv.ParseFloat(s, 64); err == nil {
		return time.Duration(f * float64(time.Second))
	}

	return 0
}

// parseTimestamp parses a Loki timestamp (Unix seconds, nanoseconds, or RFC3339).
func parseTimestamp(s string) (time.Time, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return time.Now(), nil
	}
	if nanos, ok := parseFlexibleUnixNanos(s); ok {
		return time.Unix(0, nanos), nil
	}
	return time.Time{}, fmt.Errorf("unparseable timestamp: %q", s)
}
