package translate

import (
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
)

// RangeStatsPipe is the stats pipe that answers a range aggregation over a log query with no unwrapped value and
// no per-second division: the rows of the range counted (count_over_time, and absent_over_time, which the proxy
// answers from the same count), or the bytes of their lines summed (bytes_over_time). It has no grouping: the
// proxy buckets the answer by step. ok is false for every other function.
func RangeStatsPipe(op logql.RangeOp) (pipe logsql.PipeStats, ok bool) {
	var fn logsql.StatsFunc
	switch op {
	case logql.RangeCountOverTime, logql.RangeAbsentOverTime:
		fn = logsql.Count{}
	case logql.RangeBytesOverTime:
		fn = logsql.SumLen{Field: "_msg"}
	default:
		return logsql.PipeStats{}, false
	}
	return logsql.PipeStats{Funcs: []logsql.StatsFuncAlias{{Func: fn}}}, true
}
