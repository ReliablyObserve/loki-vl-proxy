package proxy

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Evaluation bounds shared by proxy-side metric evaluation (ordered JSON
// metrics and manual range-metric budgets).
const maxMetricEvalPoints = 10000
const maxMetricEvalSamples = 1000000

var errMetricEvalBudget = errors.New("metric evaluation exceeds its point or sample limit")

// metricEvalPointCount returns the number of evaluation points in [start, end]
// at step, rejecting invalid or unbounded windows before any backend work.
func metricEvalPointCount(start, end time.Time, step time.Duration) (int, error) {
	if step <= 0 || end.Before(start) {
		return 0, fmt.Errorf("invalid metric evaluation bounds or step")
	}
	distance := end.Sub(start)
	if distance == time.Duration(1<<63-1) || distance/step >= maxMetricEvalPoints {
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
