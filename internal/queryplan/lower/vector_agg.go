package lower

import (
	"slices"
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
// Everything else is left to the string translator under a construct name:
//   - stddev and stdvar (the translator writes the operand's stats, or a binary, and the proxy evaluates them);
//   - an operand that is not a range aggregation (a nested aggregation, a ranking, a binary), one rangeAgg leaves
//     out (unwrap functions, parsers, drop and keep, a range-level grouping), and the cases it names there;
//   - a without clause with no label, a grouping label the label function turns into nothing, and a query whose
//     text the translator would read as a clause or a stats pipe (a quoted `without`, `| stats`).
func vectorAgg(va *queryplan.VectorAgg, prof Profile) (physical.Op, error) {
	ra, ok := va.In.(*queryplan.RangeAgg)
	if !ok {
		return nil, &NotLoweredError{Construct: "VectorAgg operand"}
	}
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
	return vectorAggOver(va, ra, prof)
}

// vectorAggOver is vectorAgg over the range aggregation ra, the operand of va.
func vectorAggOver(va *queryplan.VectorAgg, ra *queryplan.RangeAgg, prof Profile) (physical.Op, error) {
	switch va.Op {
	case logql.VectorSum, logql.VectorCount, logql.VectorMin, logql.VectorMax, logql.VectorAvg:
	default:
		return nil, &NotLoweredError{Construct: "VectorAgg " + string(va.Op)}
	}
	if ra.By != nil {
		return nil, &NotLoweredError{Construct: "VectorAgg grouping"}
	}
	fn, ok := translate.RangeRowFunc(ra.Op)
	if !ok {
		return nil, &NotLoweredError{Construct: "VectorAgg unwrap"}
	}
	rate := ra.Op == logql.RangeRate || ra.Op == logql.RangeBytesRate
	seconds := 0.0
	if rate {
		// The translator reads the range's seconds from its text and writes none for a range of no length.
		if seconds = ra.Range.Seconds(); seconds <= 0 {
			return nil, &NotLoweredError{Construct: "VectorAgg range"}
		}
	}
	var labels, raw []string
	without := false
	if g := va.By; g != nil {
		if len(g.Labels) > 0 {
			labels = translate.NormalizeByLabels(g.Labels, prof.Label)
			if len(labels) == 0 {
				return nil, &NotLoweredError{Construct: "VectorAgg grouping label"}
			}
		}
		// A label named bool, with a blank on each side in the text, is a modifier to the translator's regexp.
		if !g.Without && slices.Contains(g.Labels, "bool") {
			return nil, &NotLoweredError{Construct: "VectorAgg quoted clause"}
		}
		if g.Without {
			if len(g.Labels) == 0 {
				return nil, &NotLoweredError{Construct: "VectorAgg without empty"}
			}
			without, raw, labels = true, g.Labels, nil
		}
	}
	log, err := metricLogQuery(ra.In, prof, "VectorAgg")
	if err != nil {
		return nil, err
	}
	// The series identity of count, min, max, avg and rate comes from the parsers the translator finds in the text
	// (a quoted `| json` drops the level from it), and a rate reads the unwrapped label there.
	if err := parserTextError(ra.In, log, rate, labels, without); err != nil {
		return nil, err
	}
	// The translator reads a clause and a stats pipe out of the text of the query, quoted strings included.
	if without || len(labels) > 0 {
		if textReadAsClause(log) {
			return nil, &NotLoweredError{Construct: "VectorAgg quoted clause"}
		}
	}
	// Stripping a without clause, the translator also folds every run of blanks in the query into one.
	if without && (textHas(log, "  ") || selectionHas(ra.In, "  ")) {
		return nil, &NotLoweredError{Construct: "VectorAgg quoted clause"}
	}
	stats := rateVectorPipes(vectorStats(va.Op, fn, rate, seconds, labels, without, seriesIdentity(log.parser)), va.Op, labels, log, false)
	chain, service := "", ""
	var needChain, needService bool
	for _, p := range stats {
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
	op := physical.Op(physical.VLQuery{Text: log.text(chain, service, stats...)})
	if without {
		op = physical.Fold{Levels: []physical.FoldLevel{{Op: string(va.Op), Labels: raw, Without: true}}, In: op}
	}
	return op, nil
}

// vectorStats are the stats pipes of the aggregation over the rows of a range function fn (a rate divides by
// seconds). labels are the grouping labels as VictoriaLogs names them, without whether the clause is a without
// (which groups by none: the proxy folds the series). identity is what tells a series apart (seriesIdentity).
func vectorStats(op logql.VectorOp, fn logsql.StatsFunc, rate bool, seconds float64, labels []string, without bool, identity []string) []logsql.Pipe {
	const inner, rated = "__lvp_inner", "__lvp_rate"
	byKeys := func(fields []string) []logsql.GroupKey {
		keys := make([]logsql.GroupKey, len(fields))
		for i, f := range fields {
			keys[i] = logsql.GroupKey{Field: f}
		}
		return keys
	}
	// stats is a stats pipe over by; the stream and its level also group by detected_level.
	stats := func(by []string, f logsql.StatsFunc, alias string) logsql.PipeStats {
		keys := byKeys(by)
		if hasField(by, "_stream") && hasField(by, "level") && !hasField(by, translate.DetectedLevelLabel) {
			keys = append(keys, logsql.GroupKey{Field: translate.DetectedLevelLabel})
		}
		return logsql.PipeStats{By: keys, Funcs: []logsql.StatsFuncAlias{{Func: f, Alias: alias}}}
	}
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

// selectionHas reports whether a value the client wrote in the selector or a line filter contains s. The printed
// query may not hold it: a value that is not UTF-8 is written as a filter that matches nothing, where the
// translator reads the client's text.
func selectionHas(sel *queryplan.LogSelect, s string) bool {
	for _, m := range sel.Matchers {
		if strings.Contains(m.Value, s) {
			return true
		}
	}
	for _, st := range sel.Stages {
		if lf, ok := st.(*queryplan.LineFilter); ok {
			if strings.Contains(lf.Value, s) {
				return true
			}
			for _, alt := range lf.Or {
				if strings.Contains(alt.Value, s) {
					return true
				}
			}
		}
	}
	return false
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

// parserVectorConstruct names the construct of a vector aggregation over a log query with a parser that the
// lowering leaves to the translator, "" when it writes it. The translator folds a without clause over a parser by
// the labels the parsers extract, which it reads from the text of the query, and resolves a grouping by a label
// that a parser may have stored under another spelling (a name with an underscore).
func parserVectorConstruct(labels []string, without bool) string {
	if without {
		return "VectorAgg parser without"
	}
	for _, l := range labels {
		if resolvableLabel(l) {
			return "VectorAgg parser label"
		}
	}
	return ""
}

// resolvableLabel reports whether the translator looks for a label under the other spellings of a parser key: a
// name with an underscore that is not the first character, which is not a derived label.
func resolvableLabel(label string) bool {
	if label == "service_name" || label == translate.DetectedLevelLabel {
		return false
	}
	if i := strings.IndexByte(label, '_'); i <= 0 {
		return false
	}
	for i := 0; i < len(label); i++ {
		c := label[i]
		if !(c == '_' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' && i > 0) {
			return false
		}
	}
	return true
}

// parserTextError names the construct of a vector aggregation whose series identity the translator reads from the
// parsers in the text of the query (quoted text that spells one, or a parser whose labels it resolves or folds).
func parserTextError(in *queryplan.LogSelect, log metricLog, rate bool, labels []string, without bool) error {
	if readsUnwrapOrParser(in, log, rate, !log.parser) {
		return errVectorQuotedClause
	}
	if log.parser {
		if c := parserVectorConstruct(labels, without); c != "" {
			return &NotLoweredError{Construct: c}
		}
	}
	return nil
}
