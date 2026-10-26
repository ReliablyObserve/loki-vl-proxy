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

// HasNestedVectorKeywords reports whether a query names two or more of the
// aggregation operators a nest is made of (sum, avg, max, min, count), each as a
// word followed by its grouping or its parenthesis. A vector aggregation over
// another one needs both, so every other query skips the parse that finds the
// levels. It may say yes to a query that is not a nest (a label named like an
// operator), never no to one that is.
func HasNestedVectorKeywords(logql string) bool {
	found := 0
	for i := 0; i < len(logql); i++ {
		c := logql[i]
		if c != 's' && c != 'a' && c != 'm' && c != 'c' {
			continue
		}
		if i > 0 && isIdentByte(logql[i-1]) {
			continue
		}
		for _, kw := range [...]string{"sum", "avg", "max", "min", "count"} {
			if !strings.HasPrefix(logql[i:], kw) {
				continue
			}
			j := i + len(kw)
			for j < len(logql) && (logql[j] == ' ' || logql[j] == '\t' || logql[j] == '\n' || logql[j] == '\r') {
				j++
			}
			if j < len(logql) && (logql[j] == '(' || strings.HasPrefix(logql[j:], "by") || strings.HasPrefix(logql[j:], "without")) {
				if found++; found == 2 {
					return true
				}
			}
			break
		}
	}
	return false
}

func isIdentByte(c byte) bool {
	return c == '_' || c == '.' || c >= '0' && c <= '9' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z'
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
func tryTranslateVectorChain(logql string, labelFn LabelTranslateFunc) (query string, withoutLabels []string, foldOp string, ok bool) {
	if !HasNestedVectorKeywords(logql) {
		return "", nil, "", false
	}
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
		return "", nil, "", false
	}

	innermost := len(levels) - 1
	text := exprs[innermost]
	if levels[innermost].without {
		text, withoutLabels = extractWithoutLabels(text)
	}
	query, translated := tryTranslateMetricQueryWithout(text, labelFn, levels[innermost].without, levels[innermost].without)
	if !translated || carriesTranslationMarker(query) {
		return "", nil, "", false
	}
	if levels[innermost].without {
		// The proxy folds the levels over the series rows of this query; the
		// marker's operator is the outermost level's, as for a single `without`
		// the proxy folds (foldingWithoutOp).
		return query, withoutLabels, levels[0].op, true
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
	return query, nil, "", true
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

// VectorChainRange returns, as the text it was written in, the expression under
// the sum, avg, max, min and count levels of logql (in either spelling of a
// level's grouping) and how many levels it peeled.
func VectorChainRange(logql string) (string, int) {
	expr, levels := strings.TrimSpace(logql), 0
	for {
		_, inner, ok := splitVectorLevel(expr)
		if !ok {
			return expr, levels
		}
		expr, levels = strings.TrimSpace(inner), levels+1
	}
}
