// Package refusalcases is the table of VictoriaLogs refusals the proxy predicts (logsql.PredictRefusal), read by the
// predictor's tests and by the oracle test that sends every row to VictoriaLogs itself. Only tests import it.
package refusalcases

import (
	"strings"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
)

// Case is one row of the refusal table: a call, the release it is sent to, and the rule VictoriaLogs refuses
// it by ("" when VictoriaLogs answers it). The oracle test in internal/proxy sends every row to VictoriaLogs v1.49.0
// and v1.53.0 and holds the prediction to VictoriaLogs' answer.
type Case struct {
	Name    string
	In      logsql.RefusalInput
	Release string
	Rule    string
}

// LongQuery is a query of n bytes or more that VictoriaLogs runs.
func LongQuery(n int) string {
	var b strings.Builder
	b.WriteString(`* | filter level:x00000`)
	for b.Len() < n {
		b.WriteString(" or level:x00000")
	}
	return b.String()
}

// Cases is the refusal table, shared with the oracle test (internal/proxy vl_refusal_oracle_test.go).
func Cases() []Case {
	q, sq, sr := logsql.PathQuery, logsql.PathStatsQuery, logsql.PathStatsQueryRange
	long := LongQuery(16400)
	rows := []Case{
		// The length limit, known from VictoriaLogs' refusal (16384 by default).
		{"query over the length limit", logsql.RefusalInput{Path: q, Query: long, MaxQueryLen: 16384}, "", logsql.RuleQueryTooLong},
		{"stats query over the length limit", logsql.RefusalInput{Path: sq, Query: long + " | stats count() as c", MaxQueryLen: 16384}, "", logsql.RuleQueryTooLong},
		{"query at the length limit", logsql.RefusalInput{Path: q, Query: LongQuery(100), MaxQueryLen: 16384}, "", ""},
		// A pipe the release lacks.
		{"coalesce before v1.51", logsql.RefusalInput{Path: q, Query: `* | coalesce (a, b) as c`}, "v1.49.0", logsql.RulePipeBeforeRelease},
		{"coalesce from v1.51", logsql.RefusalInput{Path: q, Query: `* | coalesce (a, b) as c`}, "v1.53.0", ""},
		{"coalesce on stats before v1.51", logsql.RefusalInput{Path: sr, Query: `* | coalesce (a, b) as c | stats count() as n`}, "v1.49.0", logsql.RulePipeBeforeRelease},
		{"json_array_concat read as a filter by v1.49", logsql.RefusalInput{Path: q, Query: `* | json_array_concat x ","`}, "v1.49.0", ""},
		// Stats shapes.
		{"stats query without stats", logsql.RefusalInput{Path: sr, Query: `* | filter level:info`}, "", logsql.RuleStatsMissingStats},
		{"total_stats between stats", logsql.RefusalInput{Path: sr, Query: `* | stats count() as c | total_stats sum(c) as t | stats count() as d`}, "", logsql.RuleRangePipeBeforeStat},
		{"total_stats ahead of stats", logsql.RefusalInput{Path: sr, Query: `* | total_stats count() as t | stats count() as c`}, "", logsql.RuleRangePipeBeforeStat},
		{"total_stats ahead of instant stats", logsql.RefusalInput{Path: sq, Query: `* | total_stats count() as t | stats count() as c`}, "", ""},
		{"sort ahead of range stats", logsql.RefusalInput{Path: sr, Query: `* | sort by (level) | stats count() as c`}, "", logsql.RuleRangePipeBeforeStat},
		{"fields without _time ahead of range stats", logsql.RefusalInput{Path: sr, Query: `* | fields level | stats count() as c`}, "", logsql.RuleRangePipeBeforeStat},
		{"fields with _time ahead of range stats", logsql.RefusalInput{Path: sr, Query: `* | fields _time, level | stats count() as c`}, "", ""},
		{"copy into _time ahead of range stats", logsql.RefusalInput{Path: sr, Query: `* | copy level as _time | stats count() as c`}, "", logsql.RuleRangePipeBeforeStat},
		{"rename of another field ahead of range stats", logsql.RefusalInput{Path: sr, Query: `* | rename level as lvl | stats count() as c`}, "", ""},
		{"unpack_json ahead of range stats", logsql.RefusalInput{Path: sr, Query: `* | unpack_json | stats by (level) count() as c`}, "", ""},
		{"math into _time on v1.49", logsql.RefusalInput{Path: sr, Query: `* | math 1 as _time | stats count() as c`}, "v1.49.0", ""},
		{"math into _time from v1.53", logsql.RefusalInput{Path: sr, Query: `* | math 1 as _time | stats count() as c`}, "v1.53.0", logsql.RuleRangePipeBeforeStat},
		{"replace at _time on v1.49", logsql.RefusalInput{Path: sr, Query: `* | replace ("a", "b") at _time | stats count() as c`}, "v1.49.0", ""},
		{"replace at _time from v1.53", logsql.RefusalInput{Path: sr, Query: `* | replace ("a", "b") at _time | stats count() as c`}, "v1.53.0", logsql.RuleRangePipeBeforeStat},
		{"unpack_json of _time from v1.53", logsql.RefusalInput{Path: sr, Query: `* | unpack_json fields (_time) | stats count() as c`}, "v1.53.0", logsql.RuleRangePipeBeforeStat},
		{"unroll of _time from v1.53", logsql.RefusalInput{Path: sr, Query: `* | unroll (_time) | stats count() as c`}, "v1.53.0", logsql.RuleRangePipeBeforeStat},
		{"extract of _time from v1.53", logsql.RefusalInput{Path: sr, Query: `* | extract "<_time> " | stats count() as c`}, "v1.53.0", logsql.RuleRangePipeBeforeStat},
		{"extract of _time on v1.49", logsql.RefusalInput{Path: sr, Query: `* | extract "<_time> " | stats count() as c`}, "v1.49.0", ""},
		{"limit after range stats", logsql.RefusalInput{Path: sr, Query: `* | stats count() as c | limit 1`}, "", logsql.RuleStatsPipeAfterStats},
		{"offset after range stats", logsql.RefusalInput{Path: sr, Query: `* | stats count() as c | offset 1`}, "", logsql.RuleStatsPipeAfterStats},
		{"limit after instant stats before v1.51", logsql.RefusalInput{Path: sq, Query: `* | stats count() as c | limit 1`}, "v1.49.0", logsql.RuleStatsPipeAfterStats},
		{"limit after instant stats from v1.51", logsql.RefusalInput{Path: sq, Query: `* | stats count() as c | limit 1`}, "v1.53.0", ""},
		{"unpack_logfmt after stats", logsql.RefusalInput{Path: sq, Query: `* | stats by (level) count() as c | unpack_logfmt`}, "", logsql.RuleStatsPipeAfterStats},
		{"sort and math after stats", logsql.RefusalInput{Path: sr, Query: `* | stats by (level) count() as c | math c*2 as d | sort by (d)`}, "", ""},
		// A query VictoriaLogs clones and cannot read back.
		{"math with a quoted time and a limit before v1.53", logsql.RefusalInput{Path: q, Query: `* | math "2025-01-01T00:00:00Z" as x`, Limit: true}, "v1.49.0", logsql.RuleMathQuotedClone},
		{"math with a quoted plain number and a limit before v1.53", logsql.RefusalInput{Path: q, Query: `* | math "1.5" + 1 as x`, Limit: true}, "v1.49.0", ""},
		{"math with a quoted time in a sum and a limit before v1.53", logsql.RefusalInput{Path: q, Query: `* | math 1 + '2025-01-01T00:00:00Z' as x`, Limit: true}, "v1.49.0", logsql.RuleMathQuotedClone},
		{"math with a quoted field name and a limit", logsql.RefusalInput{Path: q, Query: `* | math "level" as x`, Limit: true}, "v1.49.0", ""},
		{"math with a quoted time without a limit", logsql.RefusalInput{Path: q, Query: `* | math "2025-01-01T00:00:00Z" as x`}, "v1.49.0", ""},
		{"math with a quoted time and a limit from v1.53", logsql.RefusalInput{Path: q, Query: `* | math "2025-01-01T00:00:00Z" as x`, Limit: true}, "v1.53.0", ""},
	}
	var out []Case
	for _, r := range rows {
		if r.Release != "" {
			out = append(out, r)
			continue
		}
		for _, rel := range []string{"v1.49.0", "v1.53.0"} {
			r2 := r
			r2.Release = rel
			out = append(out, r2)
		}
	}
	return out
}
