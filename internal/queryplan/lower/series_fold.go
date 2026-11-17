package lower

import (
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translate"
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
// SeriesFold writes that fold around the bucket stats pipe, in one stats_query_range (VictoriaLogs adds the bucket to
// every stats pipe). A subquery over the same rows (VictoriaLogs scopes it to the read's time range and filters, and
// runs it once however often the text holds it) classes each series hash: spanning (lines in more than one bucket of
// the read's grid, or a line whose value may read as NaN or an infinity), several lines in one bucket, or neither (no
// entry: one line). Before the bucket stats pipe each row gets its series' class, and the hash of a series of one
// line is blanked where the bucket stats already fold it (Singletons): its value is its line's. After it, each
// row's value is taken from its bucket partials, the hash of a series of several lines in one bucket is blanked
// (unless its value is not finite), and a second stats pipe adds up, per bucket and group, the values of the blanked
// series (or takes their extreme) into the value column and their count into the count column. A spanning series
// keeps its hash and its partials, unchanged.

// SeriesFoldField is the field that holds a row's value in its bucket, which the blanked series add up.
const SeriesFoldField = "__lvp_fold"

// SeriesFoldCount is the count column the fold adds when the bucket stats have none (max and min over time under an
// avg): the number of series a blanked group adds up, one for a series that keeps its hash.
const SeriesFoldCount = "__lvp_n"

// SeriesFoldClass is the field the fold writes a row's series class into, which the bucket stats group by.
const SeriesFoldClass = "__lvp_c"

// The fold's own fields: the subquery's counts and its key, a row's keys, and the mark of a row blanked after the
// bucket stats.
const (
	seriesFoldBuckets = "__lvp_nb"
	seriesFoldOdd     = "__lvp_odd"
	seriesFoldKey     = "__lvp_k"
	seriesFoldSpanKey = "__lvp_ks"
	seriesFoldOneKey  = "__lvp_km"
	seriesFoldBlank   = "__lvp_fb"
)

// SeriesFoldSingletons is how the bucket stats fold the series of one line, blanked before them.
type SeriesFoldSingletons int

const (
	// SingletonsNone keeps every hash through the bucket stats (max and min over time: the bucket's extreme of the
	// blanked lines is not the sum of their values).
	SingletonsNone SeriesFoldSingletons = iota
	// SingletonsKept: the value and count columns of a blanked group already hold the sum of its lines' values and
	// their count (avg over time: a line's average is its value).
	SingletonsKept
	// SingletonsZero: the value of a series of one line is 0 and the count column holds their count (stddev and
	// stdvar over time).
	SingletonsZero
)

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
	// Singletons is how the bucket stats fold the series of one line.
	Singletons SeriesFoldSingletons
	// Field is the unwrapped field as VictoriaLogs names it: a line whose value holds "nan" or "inf" in any case keeps
	// its series' hash.
	Field string
	// Bucket and Offset are the read's step and offset as sent to stats_query_range: the subquery tells the buckets
	// of a series apart on the same grid.
	Bucket, Offset string
}

// SeriesFoldRead is the fold of a read whose query before the unwrap gate (the series hash its last pipe) is
// identity: Before stands between the read's base query and its bucket stats pipe, which groups by By (the fold's
// grouping plus the class), and After follows that pipe. ok is false for an outer aggregation that does not fold.
type SeriesFoldRead struct {
	Before, After []logsql.Pipe
	By            []string
}

// Pipes is the fold of a read over identity.
func (f SeriesFold) Pipes(identity string) (SeriesFoldRead, bool) {
	var outer func(string) logsql.StatsFunc
	switch f.Outer {
	case logql.VectorSum, logql.VectorAvg:
		outer = func(c string) logsql.StatsFunc { return logsql.Sum{Field: c} }
	case logql.VectorMax:
		outer = func(c string) logsql.StatsFunc { return logsql.Max{Field: c} }
	case logql.VectorMin:
		outer = func(c string) logsql.StatsFunc { return logsql.Min{Field: c} }
	default:
		return SeriesFoldRead{}, false
	}
	if identity == "" || f.Value == "" || f.ValueColumn == "" || len(f.By) == 0 || f.Bucket == "" ||
		f.Singletons != SingletonsNone && f.CountColumn == "" {
		return SeriesFoldRead{}, false
	}
	sub := f.classes(identity)
	before := []logsql.Pipe{
		logsql.PipeFormat{Template: "s<" + dynamicSeriesField + ">", ResultField: seriesFoldSpanKey},
		logsql.PipeFormat{Template: "m<" + dynamicSeriesField + ">", ResultField: seriesFoldOneKey},
		logsql.PipeFormat{Template: "s", ResultField: SeriesFoldClass, If: logsql.InQuery{Field: seriesFoldSpanKey, Query: sub}},
		logsql.PipeFormat{Template: "m", ResultField: SeriesFoldClass, If: logsql.InQuery{Field: seriesFoldOneKey, Query: sub}},
	}
	if f.Singletons != SingletonsNone {
		// A series of one line: the bucket stats fold it with the others of its group.
		before = append(before, logsql.PipeFormat{Template: "", ResultField: dynamicSeriesField,
			If: logsql.Exclude{Expr: logsql.FieldFilter{Field: SeriesFoldClass, Op: logsql.FieldOpAny}}})
	}
	by := append(append(make([]string, 0, len(f.By)+1), f.By...), SeriesFoldClass)
	after := make([]logsql.Pipe, 0, 2*len(f.Columns)+10)
	// A stats result is text to the pipes after it, which VictoriaLogs' stats read with a parser that refuses an
	// infinity and long numbers: math gives them numbers again.
	for _, c := range f.Columns {
		after = append(after, logsql.PipeMath{Expr: c, Alias: c})
	}
	if f.Singletons == SingletonsZero {
		after = append(after, logsql.PipeFormat{Template: "0", ResultField: f.ValueColumn, If: blanked},
			logsql.PipeMath{Expr: f.ValueColumn, Alias: f.ValueColumn})
	}
	after = append(after,
		logsql.PipeMath{Expr: f.Value, Alias: SeriesFoldField},
		// A series of several lines in one bucket, whose value is finite: blanked after the bucket stats.
		logsql.PipeFormat{Template: "1", ResultField: seriesFoldBlank, If: logsql.AllOf{
			logsql.Exclude{Expr: blanked},
			logsql.FieldFilter{Field: SeriesFoldClass, Op: logsql.FieldOpExact, Value: "m"},
			logsql.FieldFilter{Field: SeriesFoldField, Op: logsql.FieldOpLT, Value: "inf"},
			logsql.FieldFilter{Field: SeriesFoldField, Op: logsql.FieldOpGT, Value: "-inf"},
		}},
		logsql.PipeFormat{Template: "<" + SeriesFoldField + ">", ResultField: f.ValueColumn, If: blankedAfter},
		logsql.PipeMath{Expr: f.ValueColumn, Alias: f.ValueColumn},
	)
	if f.CountColumn != "" {
		after = append(after,
			logsql.PipeFormat{Template: "1", ResultField: f.CountColumn, If: blankedAfter},
			logsql.PipeMath{Expr: f.CountColumn, Alias: f.CountColumn})
	}
	after = append(after, logsql.PipeFormat{Template: "", ResultField: dynamicSeriesField, If: blankedAfter})
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
	return SeriesFoldRead{Before: before, After: append(after, logsql.PipeStats{By: keys, Funcs: funcs}), By: by}, true
}

// blanked selects the rows of the series whose hash the fold blanked; blankedAfter the ones it blanks after the
// bucket stats.
var (
	blanked      = logsql.FieldFilter{Field: dynamicSeriesField, Op: logsql.FieldOpExact, Value: ""}
	blankedAfter = logsql.FieldFilter{Field: seriesFoldBlank, Op: logsql.FieldOpExact, Value: "1"}
)

// classes is the subquery of the series classes: "s<hash>" for a series with lines in more than one bucket of the
// read's grid or a line whose value may read as NaN or an infinity, "m<hash>" for one of several lines in one bucket.
func (f SeriesFold) classes(identity string) string {
	odd := logsql.FieldFilter{Field: logsql.QuoteFieldName(f.Field), Op: logsql.FieldOpRegexp, Value: "(?i)(nan|inf)"}
	gt := func(field, v string) logsql.FieldFilter {
		return logsql.FieldFilter{Field: field, Op: logsql.FieldOpGT, Value: v}
	}
	lte := func(field, v string) logsql.FieldFilter {
		return logsql.FieldFilter{Field: field, Op: logsql.FieldOpLTE, Value: v}
	}
	spans := logsql.OrExpr{Left: gt(seriesFoldBuckets, "1"), Right: gt(seriesFoldOdd, "0"), Lower: true}
	q := logsql.Query{Filter: logsql.DeferredExpr{Raw: identity}, Pipes: []logsql.Pipe{
		logsql.PipeStats{By: []logsql.GroupKey{{Field: dynamicSeriesField}, {Field: "_time", Bucket: f.Bucket, Offset: f.Offset}},
			Funcs: []logsql.StatsFuncAlias{{Func: logsql.Count{}, Alias: SeriesFoldCount}, {Func: logsql.Count{}, If: odd, Alias: seriesFoldOdd}}},
		logsql.PipeStats{By: []logsql.GroupKey{{Field: dynamicSeriesField}},
			Funcs: []logsql.StatsFuncAlias{{Func: logsql.Count{}, Alias: seriesFoldBuckets}, {Func: logsql.Sum{Field: SeriesFoldCount}, Alias: SeriesFoldCount},
				{Func: logsql.Sum{Field: seriesFoldOdd}, Alias: seriesFoldOdd}}},
		logsql.PipeFilter{Expr: logsql.OrExpr{Left: gt(SeriesFoldCount, "1"), Right: gt(seriesFoldOdd, "0"), Lower: true}},
		logsql.PipeFormat{Template: "s<" + dynamicSeriesField + ">", ResultField: seriesFoldKey, If: spans},
		logsql.PipeFormat{Template: "m<" + dynamicSeriesField + ">", ResultField: seriesFoldKey,
			If: logsql.AllOf{lte(seriesFoldBuckets, "1"), lte(seriesFoldOdd, "0")}},
		logsql.PipeFields{Labels: []string{seriesFoldKey}},
	}}
	return q.String()
}

// SeriesFoldIdentity is the subquery's reading of a read's query before the unwrap gate: ungated, less the pipes
// that derive service_name for a clause (translate.ServiceNameGroupingPipes under caps) when no stage of the query
// reads a label after its parser (the series hash does not read service_name, so the classes are the same, and
// VictoriaLogs runs one pipe per spelling of the service fewer per line). A query with a label filter keeps them.
func SeriesFoldIdentity(p *queryplan.Plan, caps logsql.Capabilities, ungated string) string {
	if p == nil || !DynamicUnwrapIdentity(p) {
		return ungated
	}
	for _, st := range p.Root.(*queryplan.VectorAgg).In.(*queryplan.RangeAgg).In.Stages {
		if _, ok := st.(*queryplan.LabelFilter); ok {
			return ungated
		}
	}
	service := " " + translate.ServiceNameGroupingPipes(caps)
	if service == " " {
		return ungated
	}
	for i := 0; i+len(service) <= len(ungated); i++ {
		if ungated[i:i+len(service)] == service {
			return ungated[:i] + ungated[i+len(service):]
		}
	}
	return ungated
}
