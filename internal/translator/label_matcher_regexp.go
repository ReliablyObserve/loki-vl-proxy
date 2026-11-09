package translator

import (
	"fmt"
	"regexp"
	"strconv"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translate"
)

// selectorRegexpFieldFilter is the LogsQL filter for a stream-selector
// matcher `label=~pattern`: a Prometheus label matcher, compiled as
// ^(?s:pattern)$ (Loki v3.7.7
// vendor/github.com/prometheus/prometheus/model/labels/regexp.go:76), so it
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

// NewLabelFilterCondition is the DropCondition of a label filter
// `| field op value`: its regexp matches what Loki's label filter matches
// (lokiLabelFilterPattern), where NewDropCondition compiles a drop or keep
// matcher's.
func NewLabelFilterCondition(field, op, value string) (DropCondition, error) {
	dc := DropCondition{Field: field, Op: op, Value: value}
	if op == "=~" || op == "!~" {
		re, err := regexp.Compile(translate.AnchorLabelFilterPattern(lokiLabelFilterPattern(value)))
		if err != nil {
			return dc, fmt.Errorf("invalid regex %q in label filter: %w", value, err)
		}
		dc.re = re
	}
	return dc, nil
}

// NewIPLabelFilterCondition is the DropCondition of a label filter
// `| field = ip("pattern")` (negate: `!=`). Its Value is that LogQL text, so
// code that tells a condition from the empty-value ones keeps working; Matches
// answers what the LogsQL of IPFilter answers on VictoriaLogs for the value, so
// the rows a plan reads itself, its stats pushdown and the log query agree.
func NewIPLabelFilterCondition(field, pattern string, negate bool) (DropCondition, error) {
	op := "="
	if negate {
		op = "!="
	}
	dc := DropCondition{Field: field, Op: op, Value: "ip(" + strconv.Quote(pattern) + ")"}
	ip, ok := newIPCondition(pattern)
	if !ok {
		return dc, fmt.Errorf("invalid ip pattern %q in label filter", pattern)
	}
	dc.ip = ip
	return dc, nil
}

// IPFilter is the LogsQL filter of an ip() label condition on field (the
// label's name, quoted the way LogsQL needs it); ok is false for any other
// condition.
func (dc DropCondition) IPFilter(field string) (filter string, ok bool) {
	if dc.ip == nil {
		return "", false
	}
	return ipLabelFilter(field, dc.ip.raw, dc.Op == "!=")
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
