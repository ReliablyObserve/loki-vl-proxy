package proxy

import "strconv"

// sortByTimePipe returns the `| sort by (_time …)` stage the raw-row read paths
// append, carrying VictoriaLogs' own sort LIMIT.
//
// `sort` is a blocking pipe. Without a limit VictoriaLogs has to hold every
// matching row before it can emit the first one, so the HTTP `limit` argument —
// which trims the RESULT — cannot bound the work: a 1000-line panel over a busy
// namespace buffers the namespace. `sort by (_time desc) limit N` makes
// VictoriaLogs keep an N-row heap instead, and it is the form its own
// documentation prescribes for exactly this reason.
//
// n <= 0 emits the unbounded form, for a caller that genuinely consumes the
// whole match and must not have its input truncated.
func sortByTimePipe(forward bool, n int) string {
	s := " | sort by (_time desc)"
	if forward {
		s = " | sort by (_time)"
	}
	if n > 0 {
		s += " limit " + strconv.Itoa(n)
	}
	return s
}
