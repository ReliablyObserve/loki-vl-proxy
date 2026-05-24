// internal/logsql/capabilities.go
package logsql

import (
	"strconv"
	"strings"
)

// MinSupportedMajor and MinSupportedMinor define the oldest VictoriaLogs
// release that this package targets. Queries targeting earlier versions
// must fall back to the proxy's string-concatenation translator.
const (
	MinSupportedMajor = 1
	MinSupportedMinor = 40
)

// Capabilities describes which optional LogsQL constructs are available
// in a specific VictoriaLogs deployment.
//
// The package targets v1.4x and v1.5x families only. All features listed
// in the v1.40 baseline (PipeHits, PipeRunning, PipeBlock, PipeUniq,
// PipeTop, StatsHistogram) are assumed always available; they are not
// represented as flags because every supported version includes them.
//
// Features that vary within the v1.40–v1.5x range are:
//
//	StatsRateSum          — rate_sum() stats function.       Added in v1.44.
//	FieldIPv4Range        — ipv4_range() field filter.       Added in v1.45.
//	MetadataSubstring     — substring in metadata fields.    Added in v1.49.
//	DensePatternWindowing — dense windowing for patterns.    Added in v1.50.
//
// When a feature is false, the translator or proxy must substitute a
// compatible but slower alternative (regexp approximation, proxy-side
// post-filtering, etc.).
type Capabilities struct {
	StatsRateSum          bool // v1.44+
	FieldIPv4Range        bool // v1.45+
	MetadataSubstring     bool // v1.49+
	DensePatternWindowing bool // v1.50+
}

// CapabilitiesFor returns the Capabilities for the given VictoriaLogs semver
// string (e.g. "v1.50.0" or "1.44.2").
//
// Versions earlier than v1.40 are not supported; they return the zero value
// (all features false) rather than panicking, so callers always get a safe
// degraded capability set.
func CapabilitiesFor(semver string) Capabilities {
	maj, min, _, ok := parseSemver(semver)
	if !ok || maj < MinSupportedMajor || (maj == MinSupportedMajor && min < MinSupportedMinor) {
		return Capabilities{}
	}
	return Capabilities{
		StatsRateSum:          atLeast(maj, min, 1, 44),
		FieldIPv4Range:        atLeast(maj, min, 1, 45),
		MetadataSubstring:     atLeast(maj, min, 1, 49),
		DensePatternWindowing: atLeast(maj, min, 1, 50),
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
