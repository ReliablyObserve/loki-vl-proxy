// Package probes holds the LogsQL text of the feature probes of the capability table (internal/capability): one
// tiny, read-only query per probed gate, sent to an empty one-second window with a row limit of one. Each is a shape
// every supported VictoriaLogs release parses (or refuses with a parse error), proven not to crash v1.49.0 and the
// newest release on the VictoriaLogs oracle (internal/capability TestProbesCrashSafe). The math quoted-constant
// shape that crashes v1.49.0 and v1.52.0 can never be one.
//
// The texts live here, beside the LogsQL parser and printers they belong to, so the capability table (a stdlib
// leaf) names them without writing query syntax of its own.
package probes

// Window is the one-second window every probe reads: a fixed moment long before any retention, so a probe never scans
// stored rows (a stats query without a bounded range would count every row up to now) and answers from nothing.
const (
	WindowStart = "2000-01-01T00:00:00Z"
	WindowEnd   = "2000-01-01T00:00:01Z"
)

// timeFilter holds the probe's query to Window in its own text, for the endpoints that read the range from the
// query (stats_query reads only its time arg).
const timeFilter = `_time:[2000-01-01T00:00:00Z, 2000-01-01T00:00:01Z) `

const (
	// Coalesce holds the coalesce pipe (v1.51.0): older releases refuse it as an unexpected pipe.
	Coalesce = timeFilter + `| coalesce (a, b) as c`
	// StatsLimitAfterStats holds a limit pipe after the last stats pipe of an instant stats query (v1.51.0): older
	// releases refuse it as a pipe that cannot be put after stats.
	StatsLimitAfterStats = timeFilter + `| stats count() as c | limit 1`
	// RangeTimeWriter holds a pipe that writes _time ahead of the last stats pipe of a range stats query: v1.53.0
	// and later refuse it, earlier releases answer it.
	RangeTimeWriter = timeFilter + `| math 1 as _time | stats count() as c`
	// StreamFieldNames is the query of the stream_field_names metadata probe.
	StreamFieldNames = `*`
)
