package translator

import (
	"regexp"
	"slices"
	"strings"
)

// vectorLevel is one vector aggregation of a nested query, e.g. the
// `min by (zone)` of `min by (zone) (avg by (pod, zone) (count_over_time(...)))`.
type vectorLevel struct {
	op      string
	labels  []string
	without bool
}

var (
	vectorLevelBeforeRE = regexp.MustCompile(`^(sum|avg|max|min|count)\s+(by|without)\s*\(([^)]*)\)\s*\(`)
	vectorLevelOpenRE   = regexp.MustCompile(`^(sum|avg|max|min|count)\s*\(`)
	vectorLevelAfterRE  = regexp.MustCompile(`^(by|without)\s*\(([^)]*)\)$`)
)

func splitLabelList(list string) []string {
	var out []string
	for _, label := range strings.Split(list, ",") {
		if label = strings.TrimSpace(label); label != "" {
			out = append(out, label)
		}
	}
	return out
}

// splitVectorLevel peels the outermost sum, avg, max, min or count off expr,
// in either spelling of its grouping (`sum by (l) (...)`, `sum(...) by (l)`),
// and returns the expression it aggregates.
func splitVectorLevel(expr string) (vectorLevel, string, bool) {
	var lv vectorLevel
	var rest string
	if m := vectorLevelBeforeRE.FindStringSubmatch(expr); m != nil {
		lv = vectorLevel{op: m[1], labels: splitLabelList(m[3]), without: m[2] == "without"}
		rest = expr[len(m[0]):]
		end := findLastMatchingParen(rest)
		if end < 0 || strings.TrimSpace(rest[end+1:]) != "" {
			return lv, "", false
		}
		return lv, rest[:end], true
	}
	m := vectorLevelOpenRE.FindStringSubmatch(expr)
	if m == nil {
		return lv, "", false
	}
	lv.op = m[1]
	rest = expr[len(m[0]):]
	end := findLastMatchingParen(rest)
	if end < 0 {
		return lv, "", false
	}
	if tail := strings.TrimSpace(rest[end+1:]); tail != "" {
		g := vectorLevelAfterRE.FindStringSubmatch(tail)
		if g == nil {
			return lv, "", false
		}
		lv.labels, lv.without = splitLabelList(g[2]), g[1] == "without"
	}
	return lv, rest[:end], true
}

// carriesTranslationMarker reports whether a translated query ends in, or is
// wrapped by, a marker the proxy post-processes (group, binary, without):
// appending a stats pipe after one would corrupt it.
func carriesTranslationMarker(query string) bool {
	return strings.Contains(query, groupMarker) || strings.Contains(query, WithoutMarkerSuffix) || strings.HasPrefix(query, BinaryMetricPrefix)
}

// tryTranslateVectorChain translates a vector aggregation over another one
// (`min by (zone) (avg by (pod, zone) (count_over_time({...}[5m])))`, two or
// more levels of sum, avg, max, min and count over a range aggregation).
//
// Loki folds the innermost level first and each level keeps exactly its own
// grouping labels, the ones its input series have (pkg/logql/evaluator.go
// VectorAggEvaluator.Next, :467-477). The innermost level translates as it
// does alone; every level above it is a stats pipe of its own over the rows of
// the previous one, which are the series of that level:
//
//	stats by (pod, zone, _stream, level) count() as a | stats by (pod, zone) avg(a) as b | stats by (zone) min(b)
//
// A level above the first can only keep labels the previous level kept, so its
// `by` list is cut to those (a label the series lack is absent in Loki and
// groups as empty in VictoriaLogs; both leave no label) and its `without` list
// is subtracted from them. That needs the previous level to name its labels:
// when the innermost level is a `without`, its labels are the series' own,
// which VictoriaLogs cannot list, so the query keeps the series rows and the
// proxy folds every level over them (withoutLabels, the innermost `without`).
//
// Levels the chain does not model (group, topk, stddev, stdvar, ...) and
// translations that carry a marker are left to the single-level paths.
func tryTranslateVectorChain(logql string, labelFn LabelTranslateFunc) (query string, withoutLabels []string, ok bool) {
	var levels []vectorLevel // outermost first
	var exprs []string
	expr := strings.TrimSpace(logql)
	for {
		lv, inner, split := splitVectorLevel(expr)
		if !split {
			break
		}
		levels = append(levels, lv)
		exprs = append(exprs, expr)
		expr = strings.TrimSpace(inner)
	}
	if len(levels) < 2 || aggFuncRE.MatchString(expr) || aggByBeforeRE.MatchString(expr) {
		return "", nil, false
	}

	innermost := len(levels) - 1
	text := exprs[innermost]
	if levels[innermost].without {
		text, withoutLabels = extractWithoutLabels(text)
	}
	query, translated := tryTranslateMetricQueryWithout(text, labelFn, levels[innermost].without)
	if !translated || carriesTranslationMarker(query) {
		return "", nil, false
	}
	if levels[innermost].without {
		// The proxy folds the levels over the series rows of this query.
		return query, withoutLabels, true
	}

	kept := levels[innermost].labels
	for i := innermost - 1; i >= 0; i-- {
		lv := levels[i]
		switch {
		case lv.without:
			kept = subtractLabels(kept, lv.labels)
		case lv.labels == nil:
			kept = nil
		default:
			kept = intersectLabels(lv.labels, kept)
		}
		aliased, _ := applyOuterAggregation(query+" as __lvp_inner", lv.op, "__lvp_inner")
		query = addByClauseLast(aliased, strings.Join(kept, ", "), labelFn)
	}
	return query, nil, true
}

// intersectLabels returns the labels of want that are in have, in want's order.
func intersectLabels(want, have []string) []string {
	var out []string
	for _, label := range want {
		if slices.Contains(have, label) && !slices.Contains(out, label) {
			out = append(out, label)
		}
	}
	return out
}

// subtractLabels returns the labels of have that are not in drop.
func subtractLabels(have, drop []string) []string {
	var out []string
	for _, label := range have {
		if !slices.Contains(drop, label) {
			out = append(out, label)
		}
	}
	return out
}
