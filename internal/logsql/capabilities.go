// internal/logsql/capabilities.go
package logsql

import (
	"strconv"
	"strings"
)

// MinSupportedMajor and MinSupportedMinor define the oldest VictoriaLogs
// release that this package targets (v1.49.0). Queries targeting earlier
// versions must fall back to the proxy's string-concatenation translator.
const (
	MinSupportedMajor = 1
	MinSupportedMinor = 49
)

// Capabilities describes which optional LogsQL constructs are available
// in a specific VictoriaLogs deployment.
//
// The package targets v1.49.0 and newer. Everything introduced up to v1.49.0
// (PipeHits, PipeRunning, PipeBlock, PipeUniq, PipeTop, StatsHistogram,
// rate_sum(), ipv4_range(), the metadata substring filter) is assumed always
// available; it is not represented as a flag because every supported version
// includes it. An unknown version gets the zero value, the v1.49.0 floor.
//
// Features that vary within the v1.49–v1.5x range are:
//
//	DensePatternWindowing — dense windowing for patterns.    Added in v1.50.
//	PipeCoalesce          — coalesce pipe.                   Added in v1.51.
//
// When a feature is false, the translator or proxy must substitute a
// compatible but slower alternative (regexp approximation, proxy-side
// post-filtering, etc.).
type Capabilities struct {
	DensePatternWindowing bool // v1.50+
	PipeCoalesce          bool // v1.51+
}

// CapabilitiesFor returns the Capabilities for the given VictoriaLogs semver
// string (e.g. "v1.50.0" or "1.52.1").
//
// Versions earlier than v1.49 are not supported; they return the zero value
// (all features false) rather than panicking, so callers always get a safe
// degraded capability set.
func CapabilitiesFor(semver string) Capabilities {
	maj, min, _, ok := parseSemver(semver)
	if !ok || maj < MinSupportedMajor || (maj == MinSupportedMajor && min < MinSupportedMinor) {
		return Capabilities{}
	}
	return Capabilities{
		DensePatternWindowing: atLeast(maj, min, 1, 50),
		PipeCoalesce:          atLeast(maj, min, 1, 51),
	}
}

func atLeast(maj, min, wantMaj, wantMin int) bool {
	if maj != wantMaj {
		return maj > wantMaj
	}
	return min >= wantMin
}

func parseSemver(s string) (major, minor, patch int, ok bool) {
	s = strings.TrimSpace(strings.TrimPrefix(s, "v"))
	if s == "" {
		return 0, 0, 0, false
	}
	parts := strings.SplitN(s, ".", 3)
	if len(parts) < 2 {
		return 0, 0, 0, false
	}
	maj, err := strconv.Atoi(numericPart(parts[0]))
	if err != nil {
		return 0, 0, 0, false
	}
	min, err := strconv.Atoi(numericPart(parts[1]))
	if err != nil {
		return 0, 0, 0, false
	}
	pt := 0
	if len(parts) == 3 {
		pt, _ = strconv.Atoi(numericPart(parts[2]))
	}
	return maj, min, pt, true
}

func numericPart(s string) string {
	for i := range s {
		if s[i] < '0' || s[i] > '9' {
			return s[:i]
		}
	}
	return s
}
