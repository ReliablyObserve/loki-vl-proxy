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
	if translatorReadsQuotedText(ra.In, log) {
		return nil, &NotLoweredError{Construct: "VectorAgg quoted clause"}
	}
	stats := vectorStats(va.Op, fn, rate, seconds, labels, without)
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
	op := physical.Op(physical.VLQuery{Text: log.text(chain, service, stats)})
	if without {
		op = physical.Fold{Levels: []physical.FoldLevel{{Op: string(va.Op), Labels: raw, Without: true}}, In: op}
	}
	return op, nil
}

// vectorStats are the stats pipes of the aggregation over the rows of a range function fn (a rate divides by
// seconds). labels are the grouping labels as VictoriaLogs names them, without whether the clause is a without
// (which groups by none: the proxy folds the series).
func vectorStats(op logql.VectorOp, fn logsql.StatsFunc, rate bool, seconds float64, labels []string, without bool) []logsql.Pipe {
	identity := []string{"_stream", "level"}
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

// translatorReadsQuotedText reports whether the log query's text holds what a regexp of the string translator reads
// out of the whole LogQL text, quoted strings included: a `without (...)` clause (stripped from the query, and a
// fold marker appended) and a ` bool ` modifier (removed). The lowering leaves such a query to the translator.
func translatorReadsQuotedText(sel *queryplan.LogSelect, m metricLog) bool {
	return textHas(m, "without") || textHas(m, "bool") || selectionHas(sel, "without") || selectionHas(sel, "bool")
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
