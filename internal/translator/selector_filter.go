package translator

import (
	"fmt"
	"regexp"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
)

// SelectorMatcher is a stream selector matcher as the LogQL parser reads it: the label, its operator ("=", "!=",
// "=~" or "!~") and its unquoted value.
type SelectorMatcher struct {
	Name, Op, Value string
}

// SelectorFilter is the VictoriaLogs filter for a bare stream selector, built as nodes: the filter the string
// translator writes for the same selector. Matchers on declared stream fields form one stream filter, written
// first; every other matcher becomes a field filter, in the selector's order. A detected_level matcher, when
// detected_level is not a stream field, matches every row or none. A selector that keeps every row is *.
func SelectorFilter(matchers []SelectorMatcher, labelFn LabelTranslateFunc, streamFields map[string]bool) (logsql.FilterExpr, error) {
	var stream []logsql.LabelMatcher
	var parts logsql.AllOf
	for _, m := range matchers {
		negate, isRe, ok := selectorMatchOp(m.Op)
		if !ok {
			return nil, &ParseError{Msg: fmt.Sprintf("unknown operator %q in stream matcher", m.Op), Pos: -1}
		}
		label := sanitizeFieldIdentifier(m.Name)
		if !streamFields[detectedLevelLabel] && label == detectedLevelLabel {
			filter, err := detectedLevelMatcherFilter(m, negate, isRe)
			if err != nil {
				return nil, err
			}
			if filter != nil {
				parts = append(parts, filter)
			}
			continue
		}
		if streamFields != nil && !negate && !isRe && streamFieldLabel(label, labelFn, streamFields) {
			stream = append(stream, logsql.LabelMatcher{Name: m.Name, Op: "=", Value: m.Value})
			continue
		}
		if f := matcherFieldFilter(label, negate, isRe, m.Value, labelFn); f != nil {
			parts = append(parts, f)
		}
	}
	if len(stream) > 0 {
		parts = append(logsql.AllOf{logsql.StreamFilter{Matchers: stream}}, parts...)
	}
	switch len(parts) {
	case 0:
		return logsql.Wildcard{}, nil
	case 1:
		return parts[0], nil
	}
	return parts, nil
}

func selectorMatchOp(op string) (negate, isRe, ok bool) {
	for _, o := range streamMatcherOps {
		if o.logql == op {
			return o.negate, o.isRe, true
		}
	}
	return false, false, false
}

// streamFieldLabel reports whether an exact matcher on label can use the stream filter: the field it maps to is
// a declared stream field. service_name never does, as the proxy derives it (canUseStreamSelector).
func streamFieldLabel(label string, labelFn LabelTranslateFunc, streamFields map[string]bool) bool {
	if label == "" || label == "service_name" {
		return false
	}
	if labelFn != nil {
		label = sanitizeFieldIdentifier(labelFn(label))
		if label == "" {
			return false
		}
	}
	return streamFields[label]
}

// detectedLevelMatcherFilter is detectedLevelSelectorFilter for a parsed matcher: nil when the matcher accepts
// the empty value (every stream has no detected_level label), matchNoRows otherwise.
func detectedLevelMatcherFilter(m SelectorMatcher, negate, isRe bool) (logsql.FilterExpr, error) {
	matchesEmpty := m.Value == ""
	if isRe {
		re, err := regexp.Compile("^(?:" + m.Value + ")$")
		if err != nil {
			return nil, &ParseError{Msg: fmt.Sprintf("invalid regexp in stream matcher %s%s%q: %v", m.Name, m.Op, m.Value, err), Pos: -1}
		}
		matchesEmpty = re.MatchString("")
	}
	if matchesEmpty != negate {
		return nil, nil
	}
	return logsql.FieldFilter{Field: "_stream_id", Op: logsql.FieldOpIn}, nil
}
