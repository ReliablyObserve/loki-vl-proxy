package translator

import "github.com/ReliablyObserve/Loki-VL-proxy/internal/translate"

// selectorRegexpFieldFilter is the LogsQL filter for a stream-selector
// matcher `label=~pattern`: a Prometheus label matcher, compiled as
// ^(?s:pattern)$ (Prometheus label matcher semantics, as the Loki oracle's
// answers record), so it
// matches the whole value and '.' matches newlines.
func selectorRegexpFieldFilter(label, pattern string, negate bool) string {
	return translate.SelectorRegexpFilter(label, pattern, negate).String()
}

// labelFilterRegexpFieldFilter is the LogsQL filter for a label filter
// `| label =~ value` (see lokiLabelFilterPattern).
func labelFilterRegexpFieldFilter(label, value string, negate bool) string {
	return wholeValueRegexpFilter(label, lokiLabelFilterPattern(value), negate, false)
}

// LabelFilterRegexpFieldFilter is labelFilterRegexpFieldFilter for a field
// name the caller has already quoted.
func LabelFilterRegexpFieldFilter(field, value string, negate bool) string {
	return labelFilterRegexpFieldFilter(field, value, negate)
}

// NewLabelFilterCondition is translate.NewLabelFilterCondition.
func NewLabelFilterCondition(field, op, value string) (DropCondition, error) {
	return translate.NewLabelFilterCondition(field, op, value)
}

// NewIPLabelFilterCondition is translate.NewIPLabelFilterCondition.
func NewIPLabelFilterCondition(field, pattern string, negate bool) (DropCondition, error) {
	return translate.NewIPLabelFilterCondition(field, pattern, negate)
}

// wholeValueRegexpFilter selects a row when the field's whole value matches
// pattern, as ^(?s:pattern)$ when dotNL, else as ^(?:pattern)$. A missing
// field reads as the empty value, as an absent Loki label does.
//
// VictoriaLogs' field:~"re" is a substring match (lib/logstorage
// filter_regexp.go matchRow) in which '.' matches a newline (regexutil parses
// with syntax.DotNL), so the pattern is anchored, with an explicit (?-s:) when
// '.' must not match one, except where an
// equivalent filter keeps VictoriaLogs on a faster path:
//   - a pattern matching everything, or everything non-empty, becomes ".*" or
//     ".+": VictoriaLogs reads exactly these as match-all and as non-empty
//     (parser.go newFilterRegexpOpt);
//   - a plain literal becomes an exact filter (field:="v");
//   - ".*v.*" with a plain literal v becomes the substring regexp v.
func wholeValueRegexpFilter(label, pattern string, negate, dotNL bool) string {
	return translate.WholeValueRegexpFilterExpr(label, pattern, negate, dotNL).String()
}

// lokiLabelFilterPattern is translate.LokiLabelFilterPattern.
func lokiLabelFilterPattern(value string) string { return translate.LokiLabelFilterPattern(value) }
