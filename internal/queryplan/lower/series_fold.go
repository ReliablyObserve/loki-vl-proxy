package lower

import (
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
)

// The bucket read of a parser's series identity over a range longer than the step, folded in VictoriaLogs.
//
// On such a range the window evaluator takes the range function's partial aggregates per series and per anchored
// bucket (Profile IdentityAhead: the stats group by the series hash) and merges each series' buckets into Loki's
// windows (t-range, t]. With a key of its own on every line (a request id) every line is a series, and the answer
// holds a row per line. Most such series lie wholly in one bucket, and a window holds either all of a series' lines
// in the read or none of them (a window is a run of whole buckets, and every window the read answers lies in it): the
// series' value in every window that holds it is its value in that bucket, so the vector aggregation over the series
// needs only each bucket's sum (or extreme) of those values and their count, not the series one by one.
//
// SeriesFold writes that fold after the bucket stats pipe, two stats levels in one stats_query_range (VictoriaLogs
// adds the bucket to both): each row's value is taken from its bucket partials, the series hash of a row is blanked
// unless the series lies in more than one bucket of the read (a subquery over the same rows, VictoriaLogs scopes it
// to the read's time range and filters), and the second stats pipe adds up, per bucket and group, the values of the
// blanked series (or takes their extreme) into the value column and their count into the count column. A series in
// more than one bucket keeps its hash and its partials, unchanged. A series whose value is not finite, or that has a
// line whose value may read as NaN, keeps its hash too: Loki's aggregation of such values depends on each series.

// SeriesFoldField is the field that holds a row's value in its bucket, which the blanked series add up.
const SeriesFoldField = "__lvp_fold"

// SeriesFoldCount is the count column the fold adds when the bucket stats have none (max and min over time under an
// avg): the number of series a blanked group adds up, one for a series that keeps its hash.
const SeriesFoldCount = "__lvp_n"

// seriesFoldNaN counts, in the subquery, the lines of a series whose unwrapped value may read as NaN.
const seriesFoldNaN = "__lvp_nan"

// SeriesFold describes the fold of one bucket read.
type SeriesFold struct {
	// Outer is the vector aggregation over the series: sum and avg add the values up, max and min take their extreme.
	Outer logql.VectorOp
	// By is the grouping of the bucket stats pipe, the series hash among it (VictoriaLogs names).
	By []string
	// Columns are the bucket stats' results, read again as numbers before the second stats pipe.
	Columns []string
	// Value is the math expression of a series' value in a bucket over Columns (`__lvp_sum / __sample_count`).
	Value string
	// ValueColumn is the column a blanked group's value goes into; CountColumn the one its series count goes into,
	// empty when the bucket stats count nothing (SeriesFoldCount is added when Outer is avg).
	ValueColumn, CountColumn string
	// Field is the unwrapped field as VictoriaLogs names it: a line whose value holds "nan" in any case keeps its
	// series' hash.
	Field string
	// Bucket and Offset are the read's step and offset as sent to stats_query_range: the subquery tells the buckets
	// of a series apart on the same grid.
	Bucket, Offset string
}

// Pipes are the pipes that follow the bucket stats pipe of a read whose query before the unwrap gate (the series
// hash its last pipe) is identity. ok is false for an outer aggregation that does not fold.
func (f SeriesFold) Pipes(identity string) (pipes []logsql.Pipe, ok bool) {
	var outer func(string) logsql.StatsFunc
	switch f.Outer {
	case logql.VectorSum, logql.VectorAvg:
		outer = func(c string) logsql.StatsFunc { return logsql.Sum{Field: c} }
	case logql.VectorMax:
		outer = func(c string) logsql.StatsFunc { return logsql.Max{Field: c} }
	case logql.VectorMin:
		outer = func(c string) logsql.StatsFunc { return logsql.Min{Field: c} }
	default:
		return nil, false
	}
	if identity == "" || f.Value == "" || f.ValueColumn == "" || len(f.By) == 0 || f.Bucket == "" {
		return nil, false
	}
	pipes = make([]logsql.Pipe, 0, 2*len(f.Columns)+8)
	// A stats result is text to the pipes after it, which VictoriaLogs' stats read with a parser that refuses an
	// infinity and long numbers: math gives them numbers again.
	for _, c := range f.Columns {
		pipes = append(pipes, logsql.PipeMath{Expr: c, Alias: c})
	}
	pipes = append(pipes,
		logsql.PipeMath{Expr: f.Value, Alias: SeriesFoldField},
		logsql.PipeFormat{Template: "", ResultField: dynamicSeriesField, If: logsql.AllOf{
			logsql.Exclude{Expr: logsql.InQuery{Field: dynamicSeriesField, Query: f.spanning(identity)}},
			logsql.FieldFilter{Field: SeriesFoldField, Op: logsql.FieldOpLT, Value: "inf"},
			logsql.FieldFilter{Field: SeriesFoldField, Op: logsql.FieldOpGT, Value: "-inf"},
		}},
		logsql.PipeFormat{Template: "<" + SeriesFoldField + ">", ResultField: f.ValueColumn, If: blanked},
		logsql.PipeMath{Expr: f.ValueColumn, Alias: f.ValueColumn},
	)
	if f.CountColumn != "" {
		pipes = append(pipes,
			logsql.PipeFormat{Template: "1", ResultField: f.CountColumn, If: blanked},
			logsql.PipeMath{Expr: f.CountColumn, Alias: f.CountColumn})
	}
	keys := make([]logsql.GroupKey, len(f.By))
	for i, b := range f.By {
		keys[i] = logsql.GroupKey{Field: b}
	}
	funcs := make([]logsql.StatsFuncAlias, 0, len(f.Columns)+1)
	for _, c := range f.Columns {
		fn := logsql.StatsFunc(logsql.Sum{Field: c})
		if c == f.ValueColumn {
			fn = outer(c)
		}
		funcs = append(funcs, logsql.StatsFuncAlias{Func: fn, Alias: c})
	}
	if f.CountColumn == "" && f.Outer == logql.VectorAvg {
		funcs = append(funcs, logsql.StatsFuncAlias{Func: logsql.Count{}, Alias: SeriesFoldCount})
	}
	return append(pipes, logsql.PipeStats{By: keys, Funcs: funcs}), true
}

// blanked selects the rows of the series whose hash the fold blanked.
var blanked = logsql.FieldFilter{Field: dynamicSeriesField, Op: logsql.FieldOpExact, Value: ""}

// spanning is the subquery of the series hashes that keep their partials: the series with lines in more than one
// bucket of the read's grid, and the series with a line whose value may read as NaN.
func (f SeriesFold) spanning(identity string) string {
	nan := logsql.FieldFilter{Field: logsql.QuoteFieldName(f.Field), Op: logsql.FieldOpRegexp, Value: "(?i)nan"}
	pipes := logsql.Query{Filter: logsql.DeferredExpr{Raw: identity}, Pipes: []logsql.Pipe{
		logsql.PipeStats{By: []logsql.GroupKey{{Field: dynamicSeriesField}, {Field: "_time", Bucket: f.Bucket, Offset: f.Offset}},
			Funcs: []logsql.StatsFuncAlias{{Func: logsql.Count{}, Alias: SeriesFoldCount}, {Func: logsql.Count{}, If: nan, Alias: seriesFoldNaN}}},
		logsql.PipeStats{By: []logsql.GroupKey{{Field: dynamicSeriesField}},
			Funcs: []logsql.StatsFuncAlias{{Func: logsql.Count{}, Alias: SeriesFoldCount}, {Func: logsql.Sum{Field: seriesFoldNaN}, Alias: seriesFoldNaN}}},
		logsql.PipeFilter{Expr: logsql.OrExpr{Left: logsql.FieldFilter{Field: SeriesFoldCount, Op: logsql.FieldOpGT, Value: "1"},
			Right: logsql.FieldFilter{Field: seriesFoldNaN, Op: logsql.FieldOpGT, Value: "0"}, Lower: true}},
		logsql.PipeFields{Labels: []string{dynamicSeriesField}},
	}}
	return pipes.String()
}
