package translate

import (
	"strconv"
	"sync"

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

// RateStatsPipes are the stats pipes that answer rate and bytes_rate over a log query with no parser: the rows of
// the range counted (rate), or the bytes of their lines summed (bytes_rate), per Loki series, divided by the
// range's seconds, then summed again per series. The series are the stream, its stored level and the detected_level
// the chain derives (RateDetectedLevelChain); VictoriaLogs groups and the proxy buckets the answer by step. ok is
// false for every other function.
func RateStatsPipes(op logql.RangeOp, seconds float64) (pipes [3]logsql.Pipe, ok bool) {
	var fn logsql.StatsFunc
	switch op {
	case logql.RangeRate:
		fn = logsql.Count{}
	case logql.RangeBytesRate:
		fn = logsql.SumLen{Field: "_msg"}
	default:
		return pipes, false
	}
	by := []logsql.GroupKey{{Field: "_stream"}, {Field: "level"}, {Field: DetectedLevelLabel}}
	pipes[0] = logsql.PipeStats{By: by, Funcs: []logsql.StatsFuncAlias{{Func: fn, Alias: "__lvp_inner"}}}
	pipes[1] = logsql.PipeMath{Alias: "__lvp_rate", Expr: "__lvp_inner/" + strconv.FormatFloat(seconds, 'f', -1, 64)}
	pipes[2] = logsql.PipeStats{By: by, Funcs: []logsql.StatsFuncAlias{{Func: logsql.Sum{Field: "__lvp_rate"}}}}
	return pipes, true
}

// RateDetectedLevelChain is the detected_level chain of a rate: Loki's value for every row, written into
// detected_level (DetectedLevelChainGrouping over every field Loki reads). It takes no argument, so it is built
// once.
var RateDetectedLevelChain = sync.OnceValue(func() string {
	return BuildDetectedLevelChain(DetectedLevelFieldNames, DetectedLevelChainGrouping, nil)
})
