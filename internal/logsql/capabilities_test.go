// internal/logsql/capabilities_test.go
package logsql_test

import (
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
)

func TestCapabilitiesFor(t *testing.T) {
	tests := []struct {
		semver string
		want   logsql.Capabilities
	}{
		// Versions below v1.40 are not supported; return baseline (all false).
		{"v1.30.0", logsql.Capabilities{}},
		{"v1.39.9", logsql.Capabilities{}},
		// v1.40 baseline — all optional features false.
		{"v1.40.0", logsql.Capabilities{}},
		{"v1.43.9", logsql.Capabilities{}},
		// v1.44 adds rate_sum.
		{"v1.44.0", logsql.Capabilities{StatsRateSum: true}},
		// v1.45 adds ipv4_range.
		{"v1.45.0", logsql.Capabilities{StatsRateSum: true, FieldIPv4Range: true}},
		{"v1.48.9", logsql.Capabilities{StatsRateSum: true, FieldIPv4Range: true}},
		// v1.49 adds metadata substring.
		{"v1.49.0", logsql.Capabilities{StatsRateSum: true, FieldIPv4Range: true, MetadataSubstring: true}},
		// v1.50 adds dense pattern windowing.
		{"v1.50.0", logsql.Capabilities{StatsRateSum: true, FieldIPv4Range: true, MetadataSubstring: true, DensePatternWindowing: true}},
		{"v1.99.0", logsql.Capabilities{StatsRateSum: true, FieldIPv4Range: true, MetadataSubstring: true, DensePatternWindowing: true}},
		// Empty string — safe baseline.
		{"", logsql.Capabilities{}},
		// no "v" prefix
		{"1.44.0", logsql.Capabilities{StatsRateSum: true}},
		// two-component version (no patch)
		{"v1.40", logsql.Capabilities{}},
	}
	for _, tc := range tests {
		t.Run(tc.semver, func(t *testing.T) {
			if got := logsql.CapabilitiesFor(tc.semver); got != tc.want {
				t.Errorf("CapabilitiesFor(%q) = %+v, want %+v", tc.semver, got, tc.want)
			}
		})
	}
}
