// internal/logsql/capabilities.go
package logsql

import "github.com/ReliablyObserve/Loki-VL-proxy/internal/capability"

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

	// NoDetectedLevelBodyScan is no VictoriaLogs feature: the proxy sets it
	// with -detected-level-body-scan=false, so a detected_level filter reads
	// the stored level fields alone, as the proxy derives the label then.
	// CapabilitiesFor leaves it false.
	NoDetectedLevelBodyScan bool
}

// CapabilitiesFor returns the Capabilities for the given VictoriaLogs semver string (e.g. "v1.50.0" or "1.52.1"):
// the derived view of the capability table (internal/capability) for a named release, kept for tests and for the
// translator's callers that hold a version string. It compares no versions itself.
//
// A release the table does not know (unreadable, or below every gate) returns the zero value, so callers always get
// a safe degraded capability set.
func CapabilitiesFor(semver string) Capabilities {
	return CapabilitiesOf(capability.ForRelease(capability.ParseRelease(semver)))
}

// CapabilitiesOf is the LogsQL view of a capability set: the translator and the lowering read it.
func CapabilitiesOf(s capability.Set) Capabilities {
	return Capabilities{
		DensePatternWindowing: s.On(capability.DensePatternWindowing),
		PipeCoalesce:          s.On(capability.PipeCoalesce),
	}
}
