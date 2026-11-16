package lower

import (
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translate"
)

// The series of a range function over an unwrapped label after a json or logfmt parser with no keep: Loki puts
// every sample in the series of the labels its line ends with, the stream's and every key the parser extracts, less
// the unwrapped one (queryplan.SeriesLabels: a dynamic identity). Two lines are one series when they agree on all of
// those, so the identity is a property of the data, not of the query: a line with a key of its own (a request id)
// is a series of its own, and lines that differ only in the unwrapped value are one.
//
// VictoriaLogs groups by named fields only, so the lowering names the identity: it unpacks the line a second time
// under a prefix of its own, deletes the unwrapped key (every spelling the parser can give the label), packs the
// prefixed fields, the stored level fields and the stream into one value and hashes it. The stats then group by that hash, as they group by
// the labels a keep leaves (keptIdentity): a value per series, then the vector aggregation over the series. The
// string translator groups these by the stream and each line instead, so the proxy serves them from the lowering
// (DynamicUnwrapIdentity).

// dynamicSeriesField is the field that holds the hash of a line's parsed labels less the unwrapped one.
const dynamicSeriesField = "__lvp_sid"

// dynamicPrefix is the prefix the second unpack writes the parsed labels under.
const dynamicPrefix = "__lvp_p_"

// dynamicPacked are the fields the series hash reads: the parsed labels, the structured metadata a series holds in
// Loki that VictoriaLogs stores as fields of the row (the level the proxy reads, and detected_level) and the stream,
// so the hash alone names a series.
var dynamicPacked = []string{dynamicPrefix + "*", "level", translate.DetectedLevelLabel, "_stream"}

// dynamicSeries is the series identity of a dynamic identity, as the stats group by it: the hash, which holds the
// stream.
var dynamicSeries = []string{dynamicSeriesField}

// DynamicUnwrapIdentity reports a range function over an unwrapped label under one vector aggregation (sum, max, min
// or avg, by labels or none) whose series are the stream and every label a json or logfmt parser extracts (no keep,
// no drop, nothing that changes the line or the labels by their values): the queries whose series identity the
// lowering writes from the data (the top of this file). The functions are the ones taken per series that the outer
// aggregation then folds: avg, max, min, stddev and stdvar over time (quantile_over_time and rate_counter need each
// series' samples, which VictoriaLogs' stats cannot give without a row per series).
func DynamicUnwrapIdentity(p *queryplan.Plan) bool {
	va, ok := p.Root.(*queryplan.VectorAgg)
	if !ok {
		return false
	}
	ra, ok := va.In.(*queryplan.RangeAgg)
	if !ok {
		return false
	}
	_, ok = dynamicIdentity(va, ra)
	return ok
}

// dynamicIdentity is the parser of a query DynamicUnwrapIdentity names, ok false for any other.
func dynamicIdentity(va *queryplan.VectorAgg, ra *queryplan.RangeAgg) (parser *queryplan.Parse, ok bool) {
	switch va.Op {
	case logql.VectorSum, logql.VectorMax, logql.VectorMin, logql.VectorAvg:
	default:
		return nil, false
	}
	if va.By != nil && va.By.Without || ra.By != nil || ra.In == nil {
		return nil, false
	}
	switch ra.Op {
	case logql.RangeAvgOverTime, logql.RangeMaxOverTime, logql.RangeMinOverTime, logql.RangeStddevOverTime,
		logql.RangeStdvarOverTime:
	default:
		return nil, false
	}
	if seriesSplitFree(va.Op, ra.Op) {
		return nil, false
	}
	var unwrap *queryplan.Unwrap
	for _, st := range ra.In.Stages {
		switch st := st.(type) {
		case *queryplan.LineFilter, *queryplan.LabelFilter:
		case *queryplan.Parse:
			// One parser of every key: the second unpack reads what it read.
			if parser != nil || st.Fields != nil || st.Param != "" || st.Strict || st.KeepEmpty ||
				st.Type != logql.ParserJSON && st.Type != logql.ParserLogfmt {
				return nil, false
			}
			parser = st
		case *queryplan.Unwrap:
			if unwrap != nil || parser == nil {
				return nil, false
			}
			unwrap = st
		default:
			// A keep, a drop, a format, a decolorize: the labels or the line the unpack reads are not the parser's.
			return nil, false
		}
	}
	if unwrap == nil || unwrap.Label == "" {
		return nil, false
	}
	out := queryplan.SeriesLabels(ra.In.Stages)
	if !out.Dynamic || out.Kept || out.Opaque || len(out.Dropped) > 0 {
		return nil, false
	}
	return parser, true
}

// seriesSplitFree reports an aggregation whose answer does not depend on how its samples are split into series: the
// maximum of each series' maximum is the maximum of the group's samples, and the minimum of minimums the minimum,
// whatever the series. The stream's series (the string translator's grouping) answer it as Loki's series do, with
// less work in VictoriaLogs than the series hash, so the dynamic identity is not written for it.
// TestSeriesSplitFreeAggregations holds the pairs to an evaluation over random series splits.
func seriesSplitFree(vec logql.VectorOp, rng logql.RangeOp) bool {
	return vec == logql.VectorMax && rng == logql.RangeMaxOverTime || vec == logql.VectorMin && rng == logql.RangeMinOverTime
}

// dynamicIdentityPipes are the pipes that write the series hash of each row (the top of this file): the line
// unpacked under dynamicPrefix, every spelling of the unwrapped label deleted, the rest packed and hashed. They are
// written as one text from the typed pipes.
func dynamicIdentityPipes(parser *queryplan.Parse, label string) string {
	unpack := dynamicUnpackJSON
	if parser.Type == logql.ParserLogfmt {
		unpack = dynamicUnpackLogfmt
	}
	spellings := translate.ParsedKeyVariants(label)
	deleted := make([]string, len(spellings))
	for i, sp := range spellings {
		deleted[i] = logsql.QuoteFieldName(dynamicPrefix + sp)
	}
	return unpack + " " + logsql.PipeDelete{Labels: deleted}.String() + dynamicPackHash
}

// The fixed pipes of dynamicIdentityPipes: the second unpack of each parser, the pack and hash, and the unwrapped
// value read again as a number. After the unpack VictoriaLogs holds the value math wrote as text, and its stats read
// such text with a parser that refuses an infinity and any number longer than 27 characters (1e25, 1e-30, 1e308):
// avg, sum and stddev then answer NaN. The math pipe gives the stats a number again.
var (
	dynamicUnpackJSON   = logsql.PipeUnpackJSON{From: "_msg", ResultPrefix: dynamicPrefix}.String()
	dynamicUnpackLogfmt = logsql.PipeUnpackLogfmt{From: "_msg", ResultPrefix: dynamicPrefix}.String()
	dynamicPackHash     = " " + logsql.PipePackJSON{Fields: dynamicPacked, ResultField: dynamicSeriesField}.String() + " " +
		logsql.PipeHash{Fields: []string{dynamicSeriesField}, Result: dynamicSeriesField}.String() + " " +
		logsql.PipeMath{Expr: translate.UnwrapValueAlias, Alias: translate.UnwrapValueAlias}.String()
)

// dynamicInnerNumber reads each series' value (the inner stats' text) as a number for the outer aggregation, for the
// reason dynamicPackHash reads the unwrapped value again: an outer sum or avg over "+Inf" or "1e308" written out
// answers NaN, or leaves the series out.
var dynamicInnerNumber = logsql.PipeMath{Expr: "__lvp_inner", Alias: "__lvp_inner"}.String()

// withDynamicIdentity sets the series identity of q to the dynamic identity of the query, when it has one and no
// keep decided it: the identity pipes go first, before any stats.
func (q *unwrapQuery) withDynamicIdentity(va *queryplan.VectorAgg, ra *queryplan.RangeAgg, label string) {
	if q.keptSet {
		return
	}
	parser, ok := dynamicIdentity(va, ra)
	if !ok {
		return
	}
	q.pipes = append(q.pipes, dynamicIdentityPipes(parser, label))
	q.kept, q.keptSet, q.dynamic = dynamicSeries, true, true
}

// WritesDynamicUnwrapIdentity reports a plan with, at any depth, a vector aggregation over a range function whose
// series are the dynamic identity of a parser (DynamicUnwrapIdentity's shape): the lowering writes that identity
// wherever it stands, where the string translator writes the stream (and each line). The proxy serves only the
// shapes DynamicUnwrapIdentity names; the shadows hold every such plan to Loki (the Loki differentials), not to the
// translator.
func WritesDynamicUnwrapIdentity(p *queryplan.Plan) bool {
	var walk func(n queryplan.Node) bool
	walk = func(n queryplan.Node) bool {
		switch n := n.(type) {
		case *queryplan.VectorAgg:
			if ra, ok := n.In.(*queryplan.RangeAgg); ok {
				_, dyn := dynamicIdentity(n, ra)
				return dyn
			}
			return walk(n.In)
		case *queryplan.Rank:
			return walk(n.In)
		case *queryplan.LabelReplace:
			return walk(n.In)
		case *queryplan.BinOp:
			return walk(n.L) || walk(n.R)
		}
		return false
	}
	return walk(p.Root)
}

// withoutDynamicSeries is by less the field of the dynamic identity (the series hash).
func withoutDynamicSeries(by []string) []string {
	out := make([]string, 0, len(by))
	for _, f := range by {
		if f != dynamicSeriesField {
			out = append(out, f)
		}
	}
	return out
}
