// Package probes holds the LogsQL text of the feature probes of the capability table (internal/capability): one
// tiny, read-only query per probed gate, sent to an empty one-second window with a row limit of one. Each is a shape
// every supported VictoriaLogs release parses (or refuses with a parse error), proven not to crash v1.49.0 and the
// newest release on the VictoriaLogs oracle (internal/capability TestProbesCrashSafe). The math quoted-constant
// shape that crashes v1.49.0 and v1.52.0 can never be one.
//
// The texts live here, beside the LogsQL parser and printers they belong to, so the capability table (a stdlib
// leaf) names them without writing query syntax of its own.
package probes

const (
	// Coalesce holds the coalesce pipe (v1.51.0): older releases refuse it as an unexpected pipe.
	Coalesce = `* | coalesce (a, b) as c`
	// StatsLimitAfterStats holds a limit pipe after the last stats pipe of an instant stats query (v1.51.0): older
	// releases refuse it as a pipe that cannot be put after stats.
	StatsLimitAfterStats = `* | stats count() as c | limit 1`
	// RangeTimeWriter holds a pipe that writes _time ahead of the last stats pipe of a range stats query: v1.53.0
	// and later refuse it, earlier releases answer it.
	RangeTimeWriter = `* | math 1 as _time | stats count() as c`
	// StreamFieldNames is the query of the stream_field_names metadata probe.
	StreamFieldNames = `*`
)
