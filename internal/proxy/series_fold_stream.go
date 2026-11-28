package proxy

import (
	"context"
	"slices"
	"strconv"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/lower"
	fj "github.com/valyala/fastjson"
)

// foldsOwnRange reports a query whose plan is vector aggregations over one range aggregation: the matrix fold of the
// request is the fold of that range aggregation's series (a binary operation's operands, ranked or label_replace'd
// queries keep their reads).
func foldsOwnRange(query string) bool {
	plan := resolvedPlan(query)
	if plan == nil {
		return false
	}
	n, seen := plan.Root, false
	for {
		va, ok := n.(*queryplan.VectorAgg)
		if !ok {
			break
		}
		seen, n = true, va.In
	}
	_, ok := n.(*queryplan.RangeAgg)
	return seen && ok
}

// The series fold of the stream identity (-structured-metadata-series-identity).
//
// With Loki's structured-metadata series, a line that carries a trace_id of its own is a series of its own, and a
// window metric's bucket read answered a row per line, which the proxy read one by one before the vector aggregation
// folded them into a few groups. The fold of series_fold_read.go applies to it as to a parser's series hash: in one
// scan of /select/logsql/query, VictoriaLogs groups the read's rows per series (the rebuilt _stream, copied into the
// fold's series field) and bucket, keeps the series that span more than one bucket of the read as they are, and folds
// the series that lie wholly in one bucket into a row per group and bucket holding the outer aggregation's merge of
// their values and their count. The window evaluator merges each kept series' buckets into its windows as before, and
// adds a folded row to the windows that hold its bucket, weighted by its count (addFoldedSeries). A window holds all
// of a folded series' lines in the read or none, so the answer is the per-series one.
//
// It applies to count_over_time and rate (the bucket stats count the rows), under count, sum, avg, max or min by
// labels that are the read's own columns: a level of the fold that keeps a label the read does not group by, or a
// without, reads every series as before.

// streamSeriesCopy is the pipe that writes the row's stream into the fold's series field.
var streamSeriesCopy = logsql.PipeCopy{Pairs: [][2]string{{"_stream", dynamicSeriesHashLabel}}}.String()

// streamSeriesRename names the series field of the folded read's answer _stream again.
var streamSeriesRename = logsql.PipeRename{Pairs: [][2]string{{dynamicSeriesHashLabel, "_stream"}}}.String()

// streamSeriesBy is the read's grouping with the fold's series field in the place of _stream.
func streamSeriesBy(by []string) []string {
	out := make([]string, len(by))
	for i, b := range by {
		if b == "_stream" {
			b = dynamicSeriesHashLabel
		}
		out[i] = b
	}
	return out
}

// streamSeriesFold is the fold of a request's bucket reads over the stream identity, or nil when it does not apply:
// the flag is off, the read's series are not the stream, the function is not one whose bucket stats count the rows,
// or the vector aggregation cannot take folded rows.
func streamSeriesFold(ctx context.Context, query string, spec statsCompatSpec, manualFunc string, fold *matrixFold, window time.Duration) *seriesFoldRead {
	if seriesFoldOff || metadataSeriesOf(ctx) == nil || fold == nil || len(fold.levels) == 0 || !slices.Contains(spec.GroupBy, "_stream") || !foldsOwnRange(query) {
		return nil
	}
	value := "c"
	switch manualFunc {
	case "count_over_time":
	case "rate":
		seconds := window.Seconds()
		if seconds <= 0 {
			return nil
		}
		// A series' rate in a window is its count over the range's seconds, as the window evaluator divides it.
		value = "c / " + strconv.FormatFloat(seconds, 'f', -1, 64)
	default:
		return nil
	}
	columns := map[string]bool{}
	for _, b := range spec.GroupBy {
		if name, err := strconv.Unquote(b); err == nil {
			b = name
		}
		columns[b] = true
	}
	for _, level := range fold.levels {
		if !level.keep {
			return nil
		}
	}
	for label := range fold.levels[0].labels {
		if !columns[label] {
			return nil
		}
	}
	var outer logql.VectorOp
	switch m := fold.levels[0].merge; {
	case m.count:
		outer = logql.VectorCount
	case m.avg:
		outer = logql.VectorAvg
	case m.max:
		outer = logql.VectorMax
	case m.min:
		outer = logql.VectorMin
	case m.sum:
		outer = logql.VectorSum
	default:
		return nil
	}
	return &seriesFoldRead{
		outer:       outer,
		columns:     []string{"c"},
		stats:       []logsql.StatsFuncAlias{{Func: logsql.Count{}, Alias: "c"}},
		value:       value,
		valueColumn: "c",
		countColumn: lower.SeriesFoldCount,
		added:       true,
		stream:      true,
	}
}

// foldedStreamRow reports a row of a folded read of the stream identity that folds series (its stream blank).
func (s *seriesFoldRead) foldedStreamRow(metricObj *fj.Object) bool {
	if s == nil || !s.stream {
		return false
	}
	v := metricObj.Get("_stream")
	return v == nil || len(v.GetStringBytes()) == 0
}

// streamGroupedRow reports a row of a read grouped by the stream whose labels are its stream's: every row of such a
// read but a folded row of the stream identity.
func streamGroupedRow(streamGrouped bool, fold *seriesFoldRead, metricObj *fj.Object) bool {
	return streamGrouped && !fold.foldedStreamRow(metricObj)
}
