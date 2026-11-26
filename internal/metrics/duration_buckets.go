package metrics

import (
	"fmt"
	"math"
	"strconv"
	"strings"
)

// maxDurationBuckets bounds -metrics.duration-buckets: every bound adds one series to each duration histogram
// series of a replica.
const maxDurationBuckets = 32

// defaultDurationBuckets are the bounds, in seconds, of the proxy's duration histograms. They reach past the
// default -backend-timeout (120s), so a slow answer (12s, 30s) can be told from a request that ran into a
// 60s or 120s timeout.
var defaultDurationBuckets = []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 15, 20, 30, 45, 60, 90, 120}

// DefaultDurationBuckets returns the bounds, in seconds, used when -metrics.duration-buckets is empty.
func DefaultDurationBuckets() []float64 {
	return append([]float64(nil), defaultDurationBuckets...)
}

// ParseDurationBuckets reads a comma-separated list of histogram bounds in seconds (-metrics.duration-buckets).
// An empty value means the defaults. The bounds must be finite numbers above zero in strictly ascending order,
// at most maxDurationBuckets of them; the +Inf bucket is always added and is not listed.
func ParseDurationBuckets(raw string) ([]float64, error) {
	if strings.TrimSpace(raw) == "" {
		return DefaultDurationBuckets(), nil
	}
	if n := strings.Count(raw, ",") + 1; n > maxDurationBuckets {
		return nil, fmt.Errorf("%d bounds, at most %d are allowed", n, maxDurationBuckets)
	}
	parts := strings.Split(raw, ",")
	buckets := make([]float64, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		bound, err := strconv.ParseFloat(part, 64)
		if err != nil {
			return nil, fmt.Errorf("bound %q is not a number of seconds", part)
		}
		if math.IsNaN(bound) || math.IsInf(bound, 0) || bound <= 0 {
			return nil, fmt.Errorf("bound %q must be a finite number above zero", part)
		}
		if len(buckets) > 0 && bound <= buckets[len(buckets)-1] {
			return nil, fmt.Errorf("bound %q is not above the one before it: list the bounds in ascending order, each once", part)
		}
		buckets = append(buckets, bound)
	}
	return buckets, nil
}
