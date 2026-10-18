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
		// The floor (v1.49.0) is the zero value: every feature at or before it is
		// always on and has no flag.
		{"v1.49.0", logsql.Capabilities{}},
		{"v1.49.9", logsql.Capabilities{}},
		// v1.50 adds dense pattern windowing.
		{"v1.50.0", logsql.Capabilities{DensePatternWindowing: true}},
		{"v1.50.9", logsql.Capabilities{DensePatternWindowing: true}},
		// v1.51 adds the coalesce pipe.
		{"v1.51.0", logsql.Capabilities{DensePatternWindowing: true, PipeCoalesce: true}},
		{"v1.99.0", logsql.Capabilities{DensePatternWindowing: true, PipeCoalesce: true}},
		// An unknown or unparsable version behaves as the floor.
		{"", logsql.Capabilities{}},
		{"not-a-version", logsql.Capabilities{}},
		// Below the floor is unsupported and gets no extra capabilities.
		{"v1.48.9", logsql.Capabilities{}},
		{"v1.40.0", logsql.Capabilities{}},
		// no "v" prefix
		{"1.51.0", logsql.Capabilities{DensePatternWindowing: true, PipeCoalesce: true}},
		// two-component version (no patch)
		{"v1.50", logsql.Capabilities{DensePatternWindowing: true}},
	}
	for _, tc := range tests {
		t.Run(tc.semver, func(t *testing.T) {
			if got := logsql.CapabilitiesFor(tc.semver); got != tc.want {
				t.Errorf("CapabilitiesFor(%q) = %+v, want %+v", tc.semver, got, tc.want)
			}
		})
	}
}
