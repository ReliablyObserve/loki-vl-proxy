package translator

import (
	"slices"
	"strings"
)

// withoutSeriesStats is the pipe that keeps a row per series (the stream and
// its level) in the translation of a `<op> without (...)` over a range
// aggregation with no parser: VictoriaLogs cannot name "every label but these",
// so the proxy folds those rows (applyWithoutGrouping).
const withoutSeriesStats = "| stats by (_stream, level) "

// PushDownWithout rewrites the series rows of a `<op> without (l...) (<range
// aggregation>)`, alone or as the innermost level of a nest of sum, avg, min,
// max and count, so that VictoriaLogs folds them itself: the labels a series
// has are the `{name="value",...}` pairs of its `_stream` field, so the pairs of
// the excluded labels are cut out of that text between the stats pipe that
// counts a series and the one that combines the series now sharing a stream:
//
//	stats by (_stream, level) count() as a | replace_regexp(...) at _stream | stats by (_stream, level) avg(a)
//
// The pipes after the first are the nest's own levels (tryTranslateVectorChain),
// each over the rows of the one before. series is the translation without the
// without marker (what the proxy folds). ok is false, and the proxy folds
// instead, unless every step is exact:
//   - the query is the `without` itself or a nest under no ranking, sorting or
//     label transform, and series is the two-pipe translation of a range
//     aggregation with no parser (the series are the streams);
//   - each excluded label is a plain name (letters and digits) that no label
//     translation changes, either way, and that no derived label is built from: the
//     service_name sources, level and detected_level are cut out of the answer
//     after they are derived (ensureSyntheticServiceName), not out of the
//     stream;
//   - the levels above the `without` group `by` labels that are columns of the
//     stats pipes (not a derived label), which every pipe then keeps.
//
// A label the stream lacks is a pair that is not in its text, so it cuts
// nothing, as it removes nothing from the series in Loki; a value is skipped as
// a quoted string, whatever it holds (pkg/logql/evaluator.go VectorAggEvaluator.Next,
// `without` drops the listed labels the series has).
func PushDownWithout(logql, series string, labelFn, toLoki LabelTranslateFunc) (string, bool) {
	levels, ok := withoutPushdownLevels(logql, labelFn, toLoki)
	if !ok {
		return "", false
	}
	innermost := len(levels) - 1
	excluded := levels[innermost].labels
	var firstAbove []string
	if innermost > 0 {
		firstAbove = subtractLabels(levels[innermost-1].labels, excluded)
	}
	by := "_stream, level"
	if len(firstAbove) > 0 {
		by += ", " + normalizeByLabels(strings.Join(firstAbove, ", "), labelFn)
	}
	var cut strings.Builder
	for _, name := range excluded {
		cut.WriteString(streamPairCut(name))
	}
	query, ok := cutSeriesStats(series, levels[innermost].op, by, cut.String())
	if !ok {
		return "", false
	}
	kept := firstAbove
	for i := innermost - 1; i >= 0; i-- {
		lv := levels[i]
		switch {
		case lv.labels == nil:
			kept = nil
		case i == innermost-1:
			kept = subtractLabels(lv.labels, excluded)
		default:
			kept = intersectLabels(lv.labels, kept)
		}
		aliased, ok := applyOuterAggregation(query+" as __lvp_inner", lv.op, "__lvp_inner")
		if !ok {
			return "", false
		}
		query = addByClauseLast(aliased, strings.Join(kept, ", "), labelFn)
	}
	return query, true
}

// WithoutStreamCut returns the pipes that cut the excluded labels out of the
// _stream text for `sum without (l...) (<range aggregation>)` alone, under the
// same conditions as PushDownWithout: a sum of per-stream counts, bytes or
// rates is the sum over the streams that share the cut text, bucket by
// bucket, so a sliding window can read its buckets folded
// (pkg/logql/evaluator.go VectorAggEvaluator.Next).
func WithoutStreamCut(logql string, labelFn, toLoki LabelTranslateFunc) (string, bool) {
	levels, ok := withoutPushdownLevels(logql, labelFn, toLoki)
	if !ok || len(levels) != 1 || levels[0].op != "sum" {
		return "", false
	}
	var cut strings.Builder
	for _, name := range levels[0].labels {
		cut.WriteString(streamPairCut(name))
	}
	return cut.String(), true
}

// withoutPushdownLevels returns the levels of logql, outermost first, when it is
// an innermost `without` of plain names under levels that group `by`, with no
// ranking, sorting or label transform around them.
func withoutPushdownLevels(logql string, labelFn, toLoki LabelTranslateFunc) ([]vectorLevel, bool) {
	logql = strings.TrimSpace(logql)
	if PeelWithoutWrappers(logql) != logql || !strings.Contains(logql, "without") {
		return nil, false
	}
	var levels []vectorLevel
	expr := logql
	for {
		lv, inner, split := splitVectorLevel(expr)
		if !split {
			break
		}
		levels = append(levels, lv)
		expr = strings.TrimSpace(inner)
	}
	if len(levels) == 0 || aggFuncRE.MatchString(expr) || aggByBeforeRE.MatchString(expr) {
		return nil, false
	}
	innermost := levels[len(levels)-1]
	if !innermost.without || len(innermost.labels) == 0 {
		return nil, false
	}
	for _, name := range innermost.labels {
		if !pushDownLabel(name, labelFn, toLoki) {
			return nil, false
		}
	}
	for _, lv := range levels[:len(levels)-1] {
		if lv.without {
			return nil, false
		}
		for _, name := range lv.labels {
			if !aboveLabelName(name) {
				return nil, false
			}
		}
	}
	return levels, true
}

// cutSeriesStats puts the cut between the pipes of the series rows: the
// stream's count (or bytes) and the aggregation over those series, or, for a
// `sum`, the stream's count alone (the sum of the series' counts is the count).
// A stats pipe keeps only the fields it groups by and computes, so the labels
// the levels above read are grouped by from the first pipe on.
func cutSeriesStats(series, op, by, cut string) (string, bool) {
	if strings.Contains(strings.ReplaceAll(series, "sum_len(_msg)", ""), "_msg") || strings.Contains(series, "unpack_") || carriesTranslationMarker(series) {
		return "", false
	}
	statsBy := "| stats by (" + by + ") "
	switch strings.Count(series, "| stats ") {
	case 2:
		last := strings.LastIndex(series, withoutSeriesStats)
		if strings.Count(series, withoutSeriesStats) != 2 || last < 0 {
			return "", false
		}
		head, tail := series[:last], series[last+len(withoutSeriesStats):]
		if tail == "" || strings.Contains(tail, "|") {
			return "", false
		}
		return strings.Replace(head, withoutSeriesStats, statsBy, 1) + cut + statsBy + tail, true
	case 1:
		first := strings.Index(series, withoutSeriesStats)
		if op != "sum" || strings.Count(series, withoutSeriesStats) != 1 || first < 0 {
			return "", false
		}
		head, fn := series[:first], series[first+len(withoutSeriesStats):]
		if fn != "count()" && fn != "sum_len(_msg)" {
			return "", false
		}
		return head + statsBy + fn + " as __lvp_inner " + cut + statsBy + "sum(__lvp_inner)", true
	}
	return "", false
}

// pushDownLabel reports whether cutting name out of the stream text is the
// same as excluding the label from the series the proxy builds.
func pushDownLabel(name string, labelFn, toLoki LabelTranslateFunc) bool {
	if !plainLabelName(name) || derivedLabel(name) {
		return false
	}
	// The stream key is the name itself, and it answers to that name (no custom
	// mapping renames it either way).
	return (labelFn == nil || labelFn(name) == name) && (toLoki == nil || toLoki(name) == name)
}

// plainLabelName: letters and digits only, no underscore or dot: no label style
// maps a second stream key to such a name, and it needs no escaping in a regexp.
func plainLabelName(name string) bool {
	if name == "" || name[0] >= '0' && name[0] <= '9' {
		return false
	}
	for i := 0; i < len(name); i++ {
		c := name[i]
		if (c < 'a' || c > 'z') && (c < 'A' || c > 'Z') && (c < '0' || c > '9') {
			return false
		}
	}
	return true
}

// aboveLabelName: a label a level above groups by is read as a column of the
// stats pipes (translated like any `by` label), so any name is fine but the
// ones the proxy derives per row, which those pipes do not compute.
func aboveLabelName(name string) bool {
	switch name {
	case "level", "detected_level", "service_name", "_stream", "_msg", "_time", "":
		return false
	}
	return !strings.ContainsAny(name, " \t\n\"'`(),|")
}

// derivedLabel: a label the proxy derives or mirrors from others when it reads
// the stream (service_name, level, detected_level, and the service_name sources).
func derivedLabel(name string) bool {
	switch name {
	case "level", "detected_level", "service_name", "_stream", "_msg", "_time":
		return true
	}
	return slices.Contains(syntheticServiceNameFields, name)
}

// streamPairCut returns the two pipes that remove the pair of label name from
// the `{a="x",name="y",b="z"}` text of the _stream field: one for a pair that
// has a pair before it (with that pair's comma) and one for the first pair
// (with the comma after it, if any). The value is a quoted string whose
// backslash escapes are skipped; the expression has no alternation bar, which
// the pipe splitter of the extent cache and of the test emulator read as a pipe.
func streamPairCut(name string) string {
	const value = `"[^"\\]*(?:\\.[^"\\]*)*"`
	return "| replace_regexp(`," + name + "=" + value + "`, \"\") at _stream " +
		"| replace_regexp(`^\\{" + name + "=" + value + ",?`, \"{\") at _stream "
}
