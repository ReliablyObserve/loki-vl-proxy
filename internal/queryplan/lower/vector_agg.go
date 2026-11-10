package lower

import (
	"strings"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/physical"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translate"
)

// vectorAgg lowers sum, count, min, max and avg, with or without a by or without clause, over a range aggregation
// that rangeAgg lowers (count_over_time, bytes_over_time, absent_over_time, rate, bytes_rate). The string translator
// writes the log query of the metric row (metricLogQuery), then the stats of the aggregation:
//
//   - sum reads the rows (or the per-series rate) of the whole group, so the stats group by the labels of the
//     clause and nothing else; a rate is first counted per series (the inner labels) and divided by the range's
//     seconds;
//   - count, min, max and avg read one value per Loki series, so an inner stats keeps the series identity (the
//     stream and its stored level, and the detected_level the chain derives) next to the labels of the clause, and
//     the outer stats aggregates those values (a rate divides them first, except under count, which only counts);
//   - a without clause writes no grouping: the proxy folds the series itself, and the lowering returns the Fold
//     the string translator's marker decodes to, over a stats that keeps a row per series (for count, min, max and
//     avg the inner grouping, for a sum of a rate the stream and its level).
//
// The labels of the clause go through the label function (translate.NormalizeByLabels); service_name and
// detected_level are derived per row: a stats that groups by one gets the pipes that derive it before the first
// pipe of the log query (translate.ServiceNameGroupingPipes, and the chain, in front of them), and a stats that
// groups by both the stream and its level also groups by detected_level, as the translator rewrites it.
//
// A vector aggregation over other ones is nestedVectorAgg's. Everything else is left to the string translator under
// a construct name:
//   - stddev and stdvar (the translator writes the operand's stats, or a binary, and the proxy evaluates them);
//   - an operand that is not a range aggregation (a ranking, a binary), one rangeAgg leaves
//     out (unwrap functions, parsers, drop and keep, a range-level grouping), and the cases it names there;
//   - a without clause with no label, a grouping label the label function turns into nothing, and a query whose
//     text the translator would read as a clause or a stats pipe (a quoted `without`, `| stats`).
func vectorAgg(va *queryplan.VectorAgg, prof Profile) (physical.Op, error) {
	if _, nested := va.In.(*queryplan.VectorAgg); nested {
		return nestedVectorAgg(va, prof)
	}
	ra, ok := va.In.(*queryplan.RangeAgg)
	if !ok {
		return nil, &NotLoweredError{Construct: "VectorAgg operand"}
	}
	src := ra.In
	if unwrapStage(ra.In) != nil {
		if ra.Op != logql.RangeAbsentOverTime {
			return unwrapVector(va, ra, prof)
		}
		// absent_over_time reads no unwrapped value: the aggregation is the same over the query without the stage.
		stripped, err := withoutUnwrap(ra, "VectorAgg")
		if err != nil {
			return nil, err
		}
		ra = stripped
	}
	return vectorAggOver(va, ra, src, prof)
}

// vectorAggOver is vectorAgg over the range aggregation ra, the operand of va. src is the log query as the client
// wrote it, which holds the unwrap stage that ra does not (the translator reads the series from the whole text).
func vectorAggOver(va *queryplan.VectorAgg, ra *queryplan.RangeAgg, src *queryplan.LogSelect, prof Profile) (physical.Op, error) {
	b, err := buildVectorAgg(va, ra, src, prof)
	if err != nil {
		return nil, err
	}
	return b.op(string(va.Op), prof), nil
}

// builtVector is a vector aggregation over a range aggregation, lowered up to its stats: the log query of the
// metric row, the stats pipes, and the clause of a without (which the proxy folds).
type builtVector struct {
	log metricLog
	// keys are the pipes that resolve the labels of the first stats pipe, each after a blank.
	keys    string
	stats   []logsql.Pipe
	without bool
	raw     []string
}

// buildVectorAgg is the lowering of va over ra: everything vectorAggOver writes except the final text, which a
// nest of aggregations (nestedVectorAgg) extends with the stats of its outer levels first.
func buildVectorAgg(va *queryplan.VectorAgg, ra *queryplan.RangeAgg, src *queryplan.LogSelect, prof Profile) (builtVector, error) {
	switch va.Op {
	case logql.VectorSum, logql.VectorCount, logql.VectorMin, logql.VectorMax, logql.VectorAvg:
	default:
		return builtVector{}, &NotLoweredError{Construct: "VectorAgg " + string(va.Op)}
	}
	if ra.By != nil {
		return builtVector{}, &NotLoweredError{Construct: "VectorAgg grouping"}
	}
	fn, ok := translate.RangeRowFunc(ra.Op)
	if !ok {
		return builtVector{}, &NotLoweredError{Construct: "VectorAgg unwrap"}
	}
	rate := ra.Op == logql.RangeRate || ra.Op == logql.RangeBytesRate
	seconds := 0.0
	if rate {
		// The translator reads the range's seconds from its text and writes none for a range of no length.
		if seconds = ra.Range.Seconds(); seconds <= 0 {
			return builtVector{}, &NotLoweredError{Construct: "VectorAgg range"}
		}
	}
	var labelBuf [6]string
	labels, raw, without, err := vectorClause(va, prof, labelBuf[:0])
	if err != nil {
		return builtVector{}, err
	}
	log, err := metricLogQuery(ra.In, prof, "VectorAgg")
	if err != nil {
		return builtVector{}, err
	}
	// A fold over a parser, with a label filter or a drop after it, costs more allocations to write than the
	// translator spends on the same text (the stage's own lowering does).
	if without && log.parser && log.afterParser {
		return builtVector{}, &NotLoweredError{Construct: "VectorAgg" + costConstruct}
	}
	// The series identity of count, min, max, avg and rate comes from the parsers the translator finds in the text
	// (a quoted `| json` drops the level from it), and a rate reads the unwrapped label there.
	if readsUnwrapOrParser(ra.In, log, rate, !log.parser) {
		return builtVector{}, errVectorQuotedClause
	}
	identity := seriesIdentity(log.parser)
	seriesLevel := rate || va.Op != logql.VectorSum
	if seriesLevel {
		ident, err := seriesIdent(src, log.parser, without, "VectorAgg", prof)
		if err != nil {
			return builtVector{}, err
		}
		if ident != nil {
			identity = ident
		}
	}
	// The translator reads a clause and a stats pipe out of the text of the query, quoted strings included.
	if without || len(labels) > 0 {
		if textReadAsClause(log) {
			return builtVector{}, &NotLoweredError{Construct: "VectorAgg quoted clause"}
		}
	}
	// Stripping a without clause, the translator also folds every run of blanks in the query into one.
	if without && (textHas(log, "  ") || selectionHas(ra.In, "  ")) {
		return builtVector{}, &NotLoweredError{Construct: "VectorAgg quoted clause"}
	}
	stats := vectorStats(va.Op, fn, rate, seconds, labels, without, identity)
	keys, err := vectorKeyPipes(stats, fn, log, rate && va.Op == logql.VectorSum && len(labels) > 0, seriesLevel || len(labels) > 0)
	if err != nil {
		return builtVector{}, err
	}
	// A rate summed by labels is built through the path that resolves them again, which finds the copy pipes of
	// the log query and of the pipes that resolved them.
	stats = rateVectorPipes(stats, va.Op, labels, log, log.holdsCopy() || keys.copies)
	return builtVector{log: log, keys: keys.text, stats: stats, without: without, raw: raw}, nil
}

// vectorKeyPipes are the pipes that resolve the labels of the first of the stats pipes: the translator does that
// where it builds the pipe with the grouping in hand (a rate, count, min, max and avg), or inserts the clause into
// a sum (addByClause); site says it does. again says the rate's final stats resolves them once more.
func vectorKeyPipes(stats []logsql.Pipe, fn logsql.StatsFunc, log metricLog, again, site bool) (keyed, error) {
	if !site {
		return keyed{}, nil
	}
	var byBuf [8]string
	return log.keyPipes(firstBy(byBuf[:0], stats), fn, statArg(fn), "VectorAgg", again)
}

// op is the physical operator of the stats of b: the query, and for a without clause the fold of the series rows
// by the operator foldOp (the aggregation at the top of the query), which the proxy runs itself.
func (b builtVector) op(foldOp string, prof Profile) physical.Op {
	chain, service := "", ""
	var needChain, needService bool
	for _, p := range b.stats {
		if st, ok := p.(logsql.PipeStats); ok {
			needChain = needChain || groupsBy(st, translate.DetectedLevelLabel)
			needService = needService || groupsBy(st, "service_name")
		}
	}
	if needChain {
		chain = translate.RateDetectedLevelChain()
	}
	if needService {
		service = translate.ServiceNameGroupingPipes(prof.Caps)
	}
	op := physical.Op(physical.VLQuery{Text: b.log.text(chain, service, b.keys, b.stats...)})
	if b.without {
		op = physical.Fold{Levels: []physical.FoldLevel{{Op: foldOp, Labels: b.raw, Without: true}}, In: op}
	}
	return op
}

// vectorStats are the stats pipes of the aggregation over the rows of a range function fn (a rate divides by
// seconds). labels are the grouping labels as VictoriaLogs names them, without whether the clause is a without
// (which groups by none: the proxy folds the series). identity is what tells a series apart (seriesIdentity).
func vectorStats(op logql.VectorOp, fn logsql.StatsFunc, rate bool, seconds float64, labels []string, without bool, identity []string) []logsql.Pipe {
	const inner, rated = "__lvp_inner", "__lvp_rate"
	stats := statsPipe
	outerBy := func(fallback []string) []string {
		switch {
		case len(labels) > 0:
			return labels
		case without:
			return fallback
		}
		return nil
	}
	if op == logql.VectorSum {
		if !rate {
			return []logsql.Pipe{stats(labels, fn, "")}
		}
		by := labels
		if len(by) == 0 {
			by = identity
		}
		pipes := []logsql.Pipe{stats(by, fn, inner), translate.RateDivision(seconds)}
		// A sum of the rate of every series, folded by the proxy under without, keeps the stream and its level.
		outer := labels
		if without {
			outer = identity
		}
		return append(pipes, stats(outer, logsql.Sum{Field: rated}, ""))
	}
	// count, min, max and avg read one value per series.
	by := translate.JoinByLabels(labels, identity)
	pipes := []logsql.Pipe{stats(by, fn, inner)}
	var f logsql.StatsFunc
	switch op {
	case logql.VectorCount:
		f = logsql.Count{}
	case logql.VectorMin:
		f = logsql.Min{Field: inner}
	case logql.VectorMax:
		f = logsql.Max{Field: inner}
	default:
		f = logsql.Avg{Field: inner}
	}
	if rate && op != logql.VectorCount {
		pipes = append(pipes, translate.RateDivision(seconds))
		switch op {
		case logql.VectorMin:
			f = logsql.Min{Field: rated}
		case logql.VectorMax:
			f = logsql.Max{Field: rated}
		default:
			f = logsql.Avg{Field: rated}
		}
	}
	return append(pipes, stats(outerBy(by), f, ""))
}

// statsPipe is a stats pipe of f over by; the stream and its level also group by detected_level, as the translator
// rewrites every stats pipe that names both.
func statsPipe(by []string, f logsql.StatsFunc, alias string) logsql.PipeStats {
	keys := make([]logsql.GroupKey, len(by), len(by)+1)
	for i, field := range by {
		keys[i] = logsql.GroupKey{Field: field}
	}
	if hasField(by, "_stream") && hasField(by, "level") && !hasField(by, translate.DetectedLevelLabel) {
		keys = append(keys, logsql.GroupKey{Field: translate.DetectedLevelLabel})
	}
	return logsql.PipeStats{By: keys, Funcs: []logsql.StatsFuncAlias{{Func: f, Alias: alias}}}
}

// groupsBy reports whether a stats pipe groups by the field.
func groupsBy(st logsql.PipeStats, field string) bool {
	for _, k := range st.By {
		if k.Field == field {
			return true
		}
	}
	return false
}

func hasField(fields []string, f string) bool {
	for _, x := range fields {
		if x == f {
			return true
		}
	}
	return false
}

// textHas reports whether the filters or pipes of the log query contain s.
func textHas(m metricLog, s string) bool {
	return strings.Contains(m.head, s) || strings.Contains(m.restore, s) || strings.Contains(m.pipes, s)
}

// selectionHas reports whether a value the client wrote in the query contains s (anyClientString). The printed
// query may not hold it: a value that is not UTF-8 is written as a filter that matches nothing, a drop of matchers
// writes no pipe, and the translator reads the client's text.
func selectionHas(sel *queryplan.LogSelect, s string) bool {
	return anyClientString(sel, func(v string) bool { return strings.Contains(v, s) })
}

// textReadAsClause reports whether the log query's text holds a stats pipe, which the translator finds by text to
// insert a grouping.
func textReadAsClause(m metricLog) bool { return textHas(m, statsPipePrefix) }

// statsPipePrefix is how a stats pipe starts in the printed query ("| stats "), taken from the typed pipe.
var statsPipePrefix = strings.TrimSuffix(logsql.PipeStats{Funcs: []logsql.StatsFuncAlias{{Func: logsql.Count{}}}}.String(), logsql.Count{}.String())

// seriesIdentity is what the translator takes to tell the series of a metric apart: the stream and its stored level
// (with the detected_level the chain derives), or the stream alone when the log query has a parser, whose labels
// the translator does not carry.
func seriesIdentity(parser bool) []string {
	if parser {
		return []string{"_stream"}
	}
	return []string{"_stream", "level"}
}
