package lower

import (
	"strings"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan"
)

// The bucket read of a parser's series identity over a range longer than the step, folded in VictoriaLogs.
//
// On such a range the window evaluator takes the range function's partial aggregates per series and per anchored
// bucket (Profile IdentityAhead: the stats group by the series hash) and merges each series' buckets into Loki's
// windows (t-range, t]. With a key of its own on every line (a request id) every line is a series, and the answer
// would hold a row per line. Most such series lie wholly in one bucket, and a window holds either all of a series'
// lines in the read or none of them (a window is a run of whole buckets, and every window the read answers lies in
// it): the series' value in every window that holds it is its value in that bucket, so the vector aggregation over
// the series needs only each bucket's sum (or extreme) of those values and their count, not the series one by one.
//
// SeriesFold writes that fold after the read's base query, in one scan of /select/logsql/query (the proxy aligns the
// read's bounds to its grid as stats_query_range does; that endpoint adds the bucket to every stats pipe, so no stats
// pipe of it can group a series across buckets):
//
//   - the bucket stats, grouped by the bucket (_time on the read's grid), the read's grouping and the series hash: the
//     function's partials per series and bucket;
//   - each row's value in its bucket, and whether it is finite (a series holding a NaN line, __lvp_bad 2, never is);
//   - a stats pipe by the read's grouping and the hash: the buckets of each series, its first bucket and value, and
//     its bucket rows as JSON;
//   - a series in one bucket whose value is finite is folded: its hash is blanked and its value and a count of one go
//     into the fold's own columns. Every other series' bucket rows are read back from the JSON (unroll), their partials
//     unchanged;
//   - the last stats pipe, by the bucket, the grouping and the hash: the partials of each series that keeps its hash
//     passed through (any: one row per group), the outer aggregation's merge of the folded values (SeriesFoldField) and
//     their count (SeriesFoldSeriesCount).
//
// The series hash is computed once, in the scan. The unwrap check's counting form adds the conversion flag to the
// bucket stats and the last stats pipe only (SeriesFoldKeepsFlag): the series' rows carry it through the JSON.

// SeriesFoldField is the column of a folded group's merged values (and, on each row, its value in its bucket).
const SeriesFoldField = "__lvp_fold"

// SeriesFoldSeriesCount is the column of a folded group's series count.
const SeriesFoldSeriesCount = "__lvp_fc"

// SeriesFoldCount is the count column the proxy reads a folded group's series count into when the bucket stats have
// none (max and min over time under an avg).
const SeriesFoldCount = "__lvp_n"

// SeriesFoldRows is the field holding a series' bucket rows as JSON.
const SeriesFoldRows = "__lvp_rows"

// The fold's own fields: a series' bucket count and first bucket, a row's finiteness, and the mark of a folded series.
const (
	seriesFoldBuckets = "__lvp_nb"
	seriesFoldFirst   = "__lvp_b"
	seriesFoldFinite  = "__lvp_ok"
	seriesFoldBlank   = "__lvp_fb"
	// seriesFoldFlag is the unwrap check's conversion flag (__lvp_bad, 2 for a NaN line).
	seriesFoldFlag = "__lvp_bad"
	// seriesFoldMaxFloat bounds a finite value: VictoriaLogs' range filters read "+Inf" below inf and "-Inf" above
	// -inf, and NaN within neither bound.
	seriesFoldMaxFloat = "1.7976931348623157e308"
)

// SeriesFold describes the fold of one bucket read.
type SeriesFold struct {
	// Outer is the vector aggregation over the series: sum and avg add the values up, max and min take their extreme.
	Outer logql.VectorOp
	// By is the grouping of the bucket stats pipe, the series hash among it (VictoriaLogs names), and Stats its
	// functions.
	By    []string
	Stats []logsql.StatsFuncAlias
	// Columns are the bucket stats' results.
	Columns []string
	// Value is the math expression of a series' value in a bucket over Columns (`__lvp_sum / __sample_count`).
	Value string
	// Bucket and Offset are the read's step and offset (the grid of stats_query_range's buckets).
	Bucket, Offset string
}

// SeriesFoldRead is the fold of a read: the pipes that follow the read's base query.
type SeriesFoldRead struct {
	Pipes []logsql.Pipe
}

// seriesFoldFinitePipe marks a row whose value in its bucket is finite and not a NaN line's.
var seriesFoldFinitePipe = logsql.PipeFormat{Template: "1", ResultField: seriesFoldFinite, If: logsql.AllOf{
	logsql.Exclude{Expr: logsql.FieldFilter{Field: seriesFoldFlag, Op: logsql.FieldOpExact, Value: "2"}},
	logsql.FieldFilter{Field: SeriesFoldField, Op: logsql.FieldOpLTE, Value: seriesFoldMaxFloat},
	logsql.FieldFilter{Field: SeriesFoldField, Op: logsql.FieldOpGTE, Value: "-" + seriesFoldMaxFloat},
}}

// seriesFoldUnroll reads the bucket rows of a series that keeps its hash back as rows.
var seriesFoldUnroll = logsql.PipeUnroll{Fields: []string{SeriesFoldRows}}

// folded selects the rows of a folded series.
var folded = logsql.FieldFilter{Field: seriesFoldBlank, Op: logsql.FieldOpExact, Value: "1"}

// Pipes is the fold of a read.
func (f SeriesFold) Pipes() (SeriesFoldRead, bool) {
	var outer logsql.StatsFunc
	switch f.Outer {
	case logql.VectorSum, logql.VectorAvg:
		outer = logsql.Sum{Field: SeriesFoldField}
	case logql.VectorMax:
		outer = logsql.Max{Field: SeriesFoldField}
	case logql.VectorMin:
		outer = logsql.Min{Field: SeriesFoldField}
	default:
		return SeriesFoldRead{}, false
	}
	if f.Value == "" || len(f.By) == 0 || len(f.Stats) == 0 || len(f.Columns) == 0 || f.Bucket == "" || !hasSeriesField(f.By) {
		return SeriesFoldRead{}, false
	}
	keys := make([]logsql.GroupKey, 0, len(f.By)+1)
	keys = append(keys, logsql.GroupKey{Field: "_time", Bucket: f.Bucket, Offset: f.Offset})
	for _, b := range f.By {
		keys = append(keys, logsql.GroupKey{Field: b})
	}
	rows := append([]string{"_time", seriesFoldFlag}, f.Columns...)
	pipes := make([]logsql.Pipe, 0, len(f.Columns)+12)
	pipes = append(pipes,
		logsql.PipeStats{By: keys, Funcs: f.Stats},
		logsql.PipeMath{Expr: f.Value, Alias: SeriesFoldField},
		seriesFoldFinitePipe,
		logsql.PipeStats{By: keys[1:], Funcs: []logsql.StatsFuncAlias{
			{Func: logsql.Count{}, Alias: seriesFoldBuckets},
			{Func: logsql.Min{Field: "_time"}, Alias: seriesFoldFirst},
			{Func: logsql.Min{Field: seriesFoldFlag}, Alias: seriesFoldFlag},
			{Func: logsql.Min{Field: seriesFoldFinite}, Alias: seriesFoldFinite},
			{Func: logsql.Min{Field: SeriesFoldField}, Alias: SeriesFoldField},
			{Func: logsql.JSONValues{Field: strings.Join(rows, ", ")}, Alias: SeriesFoldRows},
		}},
		logsql.PipeFormat{Template: "1", ResultField: seriesFoldBlank, If: logsql.AllOf{
			logsql.FieldFilter{Field: seriesFoldBuckets, Op: logsql.FieldOpExact, Value: "1"},
			logsql.FieldFilter{Field: seriesFoldFinite, Op: logsql.FieldOpExact, Value: "1"},
		}},
		logsql.PipeFormat{Template: "", ResultField: SeriesFoldRows, If: folded},
		seriesFoldUnroll,
		logsql.PipeUnpackJSON{From: SeriesFoldRows},
		logsql.PipeFormat{Template: "<" + seriesFoldFirst + ">", ResultField: "_time", If: folded},
		logsql.PipeFormat{Template: "", ResultField: SeriesFoldField, If: logsql.Exclude{Expr: folded}},
	)
	for _, c := range f.Columns {
		pipes = append(pipes, logsql.PipeFormat{Template: "", ResultField: c, If: folded})
	}
	pipes = append(pipes, logsql.PipeFormat{Template: "", ResultField: dynamicSeriesField, If: folded})
	last := make([]logsql.GroupKey, 0, len(keys))
	last = append(append(last, logsql.GroupKey{Field: "_time"}), keys[1:]...)
	funcs := make([]logsql.StatsFuncAlias, 0, len(f.Columns)+2)
	for _, c := range f.Columns {
		funcs = append(funcs, logsql.StatsFuncAlias{Func: logsql.Any{Field: c}, Alias: c})
	}
	funcs = append(funcs, logsql.StatsFuncAlias{Func: outer, Alias: SeriesFoldField}, logsql.StatsFuncAlias{Func: logsql.Count{}, Alias: SeriesFoldSeriesCount})
	return SeriesFoldRead{Pipes: append(pipes, logsql.PipeStats{By: last, Funcs: funcs})}, true
}

// hasSeriesField reports whether by holds the series hash.
func hasSeriesField(by []string) bool {
	for _, b := range by {
		if b == dynamicSeriesField {
			return true
		}
	}
	return false
}

// SeriesFoldKeepsFlag reports a pipe of the fold (as the unwrap check splits a query: no leading "| ") that the
// counting form passes as it is: the series stats (the flag rides in each series' bucket rows, so grouping by it would
// fold the finite buckets of a series with a NaN line), the finiteness mark that reads the flag, and the unroll of the
// bucket rows.
func SeriesFoldKeepsFlag(pipe string) bool {
	if strings.HasSuffix(pipe, seriesFoldRowsSuffix) && strings.Contains(pipe, "json_values(") {
		return true
	}
	return pipe == seriesFoldFiniteText || pipe == seriesFoldUnrollText
}

// The texts SeriesFoldKeepsFlag compares a pipe with (without the leading "| "), written once.
var (
	seriesFoldRowsSuffix = ") as " + SeriesFoldRows
	seriesFoldFiniteText = seriesFoldFinitePipe.String()[2:]
	seriesFoldUnrollText = seriesFoldUnroll.String()[2:]
)

// SeriesFoldBase is the base query of a folded read over base, a read's query up to the bucket stats with the series
// hash ahead (Profile IdentityAhead): the parser's unpack writes its keys under the identity's prefix and copies them
// out (the same fields and values: VictoriaLogs' unpack writes a block per set of keys, and a copy writes the keys a
// block holds), so the identity reads the prefixed keys without unpacking the line again. base is returned as it is
// when it does not hold exactly one plain unpack of the plan's parser and one unpack of the identity after it, or when
// a pipe between them reads the line or writes a prefixed key.
func SeriesFoldBase(p *queryplan.Plan, base string) string {
	if p == nil || !DynamicIdentityAhead(p) {
		return base
	}
	parser, ok := dynamicIdentity(p.Root.(*queryplan.VectorAgg), p.Root.(*queryplan.VectorAgg).In.(*queryplan.RangeAgg))
	if !ok {
		return base
	}
	plain, prefixed, second := logsql.PipeUnpackJSON{}.String(), logsql.PipeUnpackJSON{ResultPrefix: dynamicPrefix}.String(), dynamicUnpackJSON
	if parser.Type == logql.ParserLogfmt {
		plain, prefixed, second = logsql.PipeUnpackLogfmt{}.String(), logsql.PipeUnpackLogfmt{ResultPrefix: dynamicPrefix}.String(), dynamicUnpackLogfmt
	}
	plain, second = " "+plain, " "+second
	at, n := pipeAt(base, plain)
	next, m := pipeAt(base, second)
	if n != 1 || m != 1 || next <= at {
		return base
	}
	between := base[at+len(plain) : next]
	if strings.Contains(between, "_msg") || strings.Contains(between, dynamicPrefix) {
		return base
	}
	copied := " " + prefixed + " " + logsql.PipeCopy{Pairs: [][2]string{{dynamicPrefix + "*", "*"}}}.String()
	return base[:at] + copied + between + base[next+len(second):]
}

// pipeAt is the index of the first pipe of text written as pipe (" | name ..."), followed by another pipe, and the
// number of such pipes.
func pipeAt(text, pipe string) (at, n int) {
	at = -1
	for i := 0; ; i++ {
		j := strings.Index(text[i:], pipe+" |")
		if j < 0 {
			return at, n
		}
		if n == 0 {
			at = i + j
		}
		n++
		i += j
	}
}
