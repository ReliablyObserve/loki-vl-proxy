package translate

import (
	"fmt"
	"regexp"
	"regexp/syntax"
	"strconv"
	"strings"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
)

// ParseError is returned when the input is not valid LogQL syntax.
// The proxy HTTP layer maps this to errorType "parse error".
type ParseError struct {
	Msg string
	Pos int // byte offset in input; -1 if unknown
}

// SelectorRegexpFilter is selectorRegexpFieldFilter as a filter node.
func SelectorRegexpFilter(label, pattern string, negate bool) logsql.FilterExpr {
	return WholeValueRegexpFilterExpr(label, pattern, negate, true)
}

// WholeValueRegexpFilterExpr is wholeValueRegexpFilter as a filter node.
func WholeValueRegexpFilterExpr(label, pattern string, negate, dotNL bool) logsql.FilterExpr {
	switch {
	case pattern == ".*" || pattern == KeepAllPattern:
		return BuildFieldFilter(label, logsql.FieldOpRegexp, ".*", negate)
	case pattern == ".+" || pattern == NonEmptyPattern:
		return BuildFieldFilter(label, logsql.FieldOpRegexp, ".+", negate)
	case pattern != "" && regexp.QuoteMeta(pattern) == pattern && exactQuotable(pattern):
		// No metacharacter: the literal itself, without parsing it.
		return BuildFieldFilter(label, logsql.FieldOpExact, pattern, negate)
	}
	flags := syntax.Perl
	if dotNL {
		flags |= syntax.DotNL
	}
	if reg, err := syntax.Parse(pattern, flags); err == nil {
		reg = reg.Simplify()
		switch {
		case isAnyCharRepeat(reg, syntax.OpStar):
			return BuildFieldFilter(label, logsql.FieldOpRegexp, ".*", negate)
		case isAnyCharRepeat(reg, syntax.OpPlus):
			return BuildFieldFilter(label, logsql.FieldOpRegexp, ".+", negate)
		}
		if literal, ok := plainLiteral(reg); ok {
			return BuildFieldFilter(label, logsql.FieldOpExact, literal, negate)
		}
		if reg.Op == syntax.OpConcat && len(reg.Sub) == 3 && isAnyCharRepeat(reg.Sub[0], syntax.OpStar) && isAnyCharRepeat(reg.Sub[2], syntax.OpStar) {
			if literal, ok := plainLiteral(reg.Sub[1]); ok {
				return BuildFieldFilter(label, logsql.FieldOpRegexp, regexp.QuoteMeta(literal), negate)
			}
		}
		if !dotNL && strings.Contains(pattern, "|") {
			// A label filter's literal alternation is Loki's OR of equality
			// checks; VictoriaLogs compares the unpacked values faster than it
			// runs the anchored regexp on each.
			if values, ok := LiteralSet(reg, MaxExactAlternatives); ok {
				return exactAlternativesFilter(label, values, negate)
			}
		}
	}
	anchored := logsql.AnchorLabelMatcherRegex(pattern)
	if !dotNL {
		anchored = AnchorLabelFilterPattern(pattern)
	}
	return BuildFieldFilter(label, logsql.FieldOpRegexp, anchored, negate)
}

// SelectorMatcher is a stream selector matcher as the LogQL parser reads it: the label, its operator ("=", "!=",
// "=~" or "!~") and its unquoted value.
type SelectorMatcher struct {
	Name, Op, Value string
}

// StreamSelectorFilter is SelectorFilter for the parsed matchers of a stream selector.
func StreamSelectorFilter(ms []logql.LabelMatcher, labelFn LabelTranslateFunc, streamFields map[string]bool) (logsql.FilterExpr, error) {
	return StreamSelectorFilterNamed(ms, labelFn, streamFields, nil)
}

// StreamSelectorFilterNamed is StreamSelectorFilter with name applied to the field name of every field filter, after
// the label function and the quoting of a dotted name (nil keeps the names as they are): the query plan quotes the
// names VictoriaLogs reads as keywords this way (logsql.QuoteKeywordField), where the string translator does not.
func StreamSelectorFilterNamed(ms []logql.LabelMatcher, labelFn LabelTranslateFunc, streamFields map[string]bool, name func(string) string) (logsql.FilterExpr, error) {
	matchers := make([]SelectorMatcher, len(ms))
	for i, m := range ms {
		matchers[i] = SelectorMatcher{Name: m.Name, Op: m.Op.String(), Value: m.Value}
	}
	return selectorFilter(matchers, labelFn, streamFields, name)
}

// SelectorFilter is the VictoriaLogs filter for a bare stream selector, built as nodes: the filter the string
// translator writes for the same selector. Matchers on declared stream fields form one stream filter, written
// first; every other matcher becomes a field filter, in the selector's order. A detected_level matcher, when
// detected_level is not a stream field, matches every row or none. A selector that keeps every row is *.
func SelectorFilter(matchers []SelectorMatcher, labelFn LabelTranslateFunc, streamFields map[string]bool) (logsql.FilterExpr, error) {
	return selectorFilter(matchers, labelFn, streamFields, nil)
}

// selectorFilter is SelectorFilter with the field names passed through name (nil: as they are).
func selectorFilter(matchers []SelectorMatcher, labelFn LabelTranslateFunc, streamFields map[string]bool, name func(string) string) (logsql.FilterExpr, error) {
	var stream []logsql.LabelMatcher
	var parts logsql.AllOf
	for _, m := range matchers {
		negate, isRe, ok := selectorMatchOp(m.Op)
		if !ok {
			return nil, &ParseError{Msg: fmt.Sprintf("unknown operator %q in stream matcher", m.Op), Pos: -1}
		}
		label := SanitizeFieldIdentifier(m.Name)
		if !streamFields[DetectedLevelLabel] && label == DetectedLevelLabel {
			filter, err := detectedLevelMatcherFilter(m, negate, isRe)
			if err != nil {
				return nil, err
			}
			if filter != nil {
				parts = append(parts, filter)
			}
			continue
		}
		if inStreamFilter(label, negate, isRe, labelFn, streamFields) {
			stream = append(stream, logsql.LabelMatcher{Name: m.Name, Op: "=", Value: m.Value})
			continue
		}
		if f := matcherFieldFilter(label, negate, isRe, m.Value, labelFn, name); f != nil {
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

// inStreamFilter reports whether an exact matcher on label goes into the stream filter, where the string translator
// copies the matcher's own spelling.
func inStreamFilter(label string, negate, isRe bool, labelFn LabelTranslateFunc, streamFields map[string]bool) bool {
	return streamFields != nil && !negate && !isRe && streamFieldLabel(label, labelFn, streamFields)
}

// StreamFilterMatcher reports whether SelectorFilter writes m into the stream filter ({k="v"}), where the string
// translator copies the matcher's spelling instead of quoting its value.
func StreamFilterMatcher(m logql.LabelMatcher, labelFn LabelTranslateFunc, streamFields map[string]bool) bool {
	label := SanitizeFieldIdentifier(m.Name)
	if !streamFields[DetectedLevelLabel] && label == DetectedLevelLabel {
		return false
	}
	return inStreamFilter(label, m.Op == logql.MatchNeq || m.Op == logql.MatchNotRe, m.Op == logql.MatchRe || m.Op == logql.MatchNotRe, labelFn, streamFields)
}

func selectorMatchOp(op string) (negate, isRe, ok bool) {
	for _, o := range StreamMatcherOps {
		if o.LogQL == op {
			return o.Negate, o.IsRe, true
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
		label = SanitizeFieldIdentifier(labelFn(label))
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
	return matchNoRows, nil
}

// LabelTranslateFunc translates a Loki label name to a VL field name (query direction).
// If nil, no translation is performed.
type LabelTranslateFunc func(lokiLabel string) string

// BuildFieldFilter is a field filter, excluded (-field:...) when negate.
func BuildFieldFilter(field string, op logsql.FieldOp, value string, negate bool) logsql.FilterExpr {
	ff := logsql.FieldFilter{Field: field, Op: op, Value: value}
	if negate {
		return logsql.Exclude{Expr: ff}
	}
	return ff
}

func NormalizeFieldIdentifier(label string) string {
	label = strings.TrimSpace(label)
	if label == "" {
		return ""
	}
	label = strings.ReplaceAll(label, "`", "")
	label = strings.ReplaceAll(label, `"`, "")
	// The regexp drops the white space around a dot; a name without a dot, or with no white space, has none to drop,
	// and the replacement would only copy it.
	if hasDotAndSpace(label) {
		label = fieldDotSpacingRE.ReplaceAllString(label, ".")
	}
	return strings.TrimSpace(label)
}

// hasDotAndSpace reports whether s has both a dot and a character the regexp's \s matches (an ASCII space).
func hasDotAndSpace(s string) bool {
	dot, space := false, false
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '.':
			dot = true
		case ' ', '\t', '\n', '\v', '\f', '\r':
			space = true
		}
	}
	return dot && space
}

func SanitizeFieldIdentifier(label string) string {
	label = NormalizeFieldIdentifier(label)
	if label == "" {
		return ""
	}
	label = strings.Trim(label, ".")
	for strings.Contains(label, "..") {
		label = strings.ReplaceAll(label, "..", ".")
	}
	return strings.TrimSpace(label)
}

var fieldDotSpacingRE = regexp.MustCompile(`\s*\.\s*`)

// StreamMatcherOps maps LogQL stream-selector operators to FieldOp constants.
// Only the four operators valid in stream selectors are listed here.
var StreamMatcherOps = []struct {
	LogQL  string
	VLOp   logsql.FieldOp
	Negate bool
	IsRe   bool
}{
	{"!~", logsql.FieldOpRegexp, true, true},
	{"=~", logsql.FieldOpRegexp, false, true},
	{"!=", logsql.FieldOpExact, true, false},
	{"=", logsql.FieldOpExact, false, false},
}

// MatcherFieldFilter is the field filter for a selector matcher on label, with its value as Loki reads it
// (unquoted), or nil when the label is no field name.
func MatcherFieldFilter(label string, negate, isRe bool, value string, labelFn LabelTranslateFunc) logsql.FilterExpr {
	return matcherFieldFilter(label, negate, isRe, value, labelFn, nil)
}

// matcherFieldFilter is MatcherFieldFilter with the field name passed through name (nil: as it is).
func matcherFieldFilter(label string, negate, isRe bool, value string, labelFn LabelTranslateFunc, name func(string) string) logsql.FilterExpr {
	if label == "" {
		return nil
	}
	// service_name is derived from stream fields on the read path;
	// match exactly the streams the proxy labels with that value.
	if label == "service_name" {
		return ServiceNameMatcherFilterExpr(value, negate, isRe)
	}
	if labelFn != nil {
		label = SanitizeFieldIdentifier(labelFn(label))
		if label == "" {
			return nil
		}
	}

	// VL requires quoting for dotted field names. Quote the field name
	// before building the FieldFilter so it uses it as-is.
	if strings.Contains(label, ".") {
		label = `"` + label + `"`
	} else if name != nil {
		label = name(label)
	}

	if isRe {
		// A selector matcher is anchored to the whole value.
		return SelectorRegexpFilter(label, value, negate)
	}

	if value == "" {
		// level="" matches entries where level is absent or empty.
		// -level:* covers both cases; level:="" would only match
		// explicit empty strings.
		if label == "level" && !negate {
			return logsql.Exclude{Expr: logsql.FieldFilter{Field: "level", Op: logsql.FieldOpAny}}
		}
		if negate {
			// field:!"" means "field exists and is non-empty"; keep literal form.
			return logsql.FieldFilter{Field: label, Op: logsql.FieldOpNotExact}
		}
		return BuildFieldFilter(label, logsql.FieldOpExact, "", false)
	}

	op := logsql.FieldOpExact
	return BuildFieldFilter(label, op, value, negate)
}

var SyntheticServiceNameFields = []string{
	"service_name",
	"service.name",
	"service",
	"app",
	"application",
	"app_name",
	"name",
	"app_kubernetes_io_name",
	"container",
	"container_name",
	"k8s.container.name",
	"k8s_container_name",
	"component",
	"workload",
	"job",
	"k8s.job.name",
	"k8s_job_name",
}

// matchNoStreams is matchNoStreamsFilter as a filter node.
var matchNoStreams = logsql.Exclude{Expr: logsql.Wildcard{}}

// ServiceNameMatcherFilterExpr is serviceNameMatcherFilter for the matcher's value as Loki reads it.
func ServiceNameMatcherFilterExpr(value string, neg, isRegex bool) logsql.FilterExpr {
	if isRegex {
		// Loki selector regexps are fully anchored and '.' matches newlines.
		value = logsql.AnchorLabelMatcherRegex(value)
	}
	return ServiceNameDerivedFilterExpr(value, neg, isRegex)
}

// ServiceNameDerivedFilterExpr matches the rows whose derived service_name the matcher accepts: the first
// non-empty field of SyntheticServiceNameFields, or UnknownServiceNameValue when they are all empty.
func ServiceNameDerivedFilterExpr(value string, neg, isRegex bool) logsql.FilterExpr {
	var re *regexp.Regexp
	if isRegex {
		// An invalid regexp is left for VictoriaLogs to reject.
		re, _ = regexp.Compile(value)
	}
	branches := make(logsql.AnyOf, 0, len(SyntheticServiceNameFields)+1)
	emptyPrefix := make(logsql.AllOf, 0, len(SyntheticServiceNameFields))
	for _, field := range SyntheticServiceNameFields {
		name := field
		if strings.Contains(name, ".") {
			name = `"` + name + `"`
		}
		var terms logsql.AllOf
		switch {
		case isRegex:
			terms = append(terms, logsql.FieldFilter{Field: name, Op: logsql.FieldOpRegexp, Value: value})
			if re == nil || re.MatchString("") {
				// A missing field reads as empty and would satisfy an
				// empty-compatible regexp; derivation needs a non-empty value.
				terms = append(terms, logsql.Exclude{Expr: logsql.FieldFilter{Field: name, Op: logsql.FieldOpExact}})
			}
		case value != "":
			terms = append(terms, logsql.FieldFilter{Field: name, Op: logsql.FieldOpExact, Value: value, GoQuoted: true})
		}
		if len(terms) > 0 {
			terms = append(append(make(logsql.AllOf, 0, len(emptyPrefix)+len(terms)), emptyPrefix...), terms...)
			if len(terms) == 1 {
				branches = append(branches, terms[0])
			} else {
				branches = append(branches, logsql.Group{Expr: terms})
			}
		}
		emptyPrefix = append(emptyPrefix, logsql.FieldFilter{Field: name, Op: logsql.FieldOpExact})
	}
	unknownMatches := value == UnknownServiceNameValue
	if isRegex {
		unknownMatches = re != nil && re.MatchString(UnknownServiceNameValue)
	}
	if unknownMatches {
		branches = append(branches, logsql.Group{Expr: emptyPrefix})
	}
	if len(branches) == 0 {
		// Every row has a non-empty service_name, so `service_name=""`
		// matches nothing and `service_name!=""` matches everything.
		if neg {
			return logsql.Wildcard{}
		}
		return matchNoStreams
	}
	filter := logsql.FilterExpr(logsql.Group{Expr: branches})
	if neg {
		return logsql.Exclude{Expr: filter}
	}
	return filter
}

const (
	// DetectedLevelLabel is the label Loki derives the log level into.
	DetectedLevelLabel = "detected_level"
	// UnknownServiceNameValue is the service_name Loki assigns when no source label is set.
	UnknownServiceNameValue = "unknown_service"
)

// AnchorLabelFilterPattern anchors a lokiLabelFilterPattern result the way
// Loki does, ^(?:pattern)$, spelled so '.' keeps not matching a newline in
// VictoriaLogs too.
func AnchorLabelFilterPattern(pattern string) string {
	if strings.Contains(pattern, ".") {
		return "^(?-s:" + pattern + ")$"
	}
	return "^(?:" + pattern + ")$"
}

func plainLiteral(reg *syntax.Regexp) (string, bool) {
	if reg.Op != syntax.OpLiteral || reg.Flags&syntax.FoldCase != 0 || !exactQuotable(string(reg.Rune)) {
		return "", false
	}
	return string(reg.Rune), true
}

// exactQuotable reports whether an exact filter can carry the literal: its
// quoting escapes only '"' and '\', so a control character stays in the
// regexp form, which is quoted with Go's rules.
func exactQuotable(literal string) bool {
	return logsql.QuoteValue(literal) == strconv.Quote(literal)
}

// exactAlternatives is `(label:="a" OR label:="b")`, negated with '-'.
// exactAlternativesFilter is the exact filters a literal alternation becomes: (label:="a" OR label:="b"),
// excluded when negate.
func exactAlternativesFilter(label string, values []string, negate bool) logsql.FilterExpr {
	exact := make(logsql.AnyOf, len(values))
	for i, v := range values {
		exact[i] = BuildFieldFilter(label, logsql.FieldOpExact, v, false)
	}
	filter := logsql.FilterExpr(logsql.Group{Expr: exact})
	if negate {
		return logsql.Exclude{Expr: filter}
	}
	return filter
}

// MaxExactAlternatives bounds the exact filters a literal alternation becomes.
const MaxExactAlternatives = 16

// LiteralSet returns the values a regexp matching a finite set of non-empty
// literals matches, at most limit of them: literals, alternations, character
// classes and their concatenations (Go's parser factors `api|apx` into
// `ap[ix]`).
func LiteralSet(reg *syntax.Regexp, limit int) ([]string, bool) {
	var values []string
	switch reg.Op {
	case syntax.OpLiteral:
		if v, ok := plainLiteral(reg); ok {
			values = []string{v}
		}
	case syntax.OpCapture:
		return LiteralSet(reg.Sub[0], limit)
	case syntax.OpCharClass:
		if reg.Flags&syntax.FoldCase != 0 {
			return nil, false
		}
		for i := 0; i+1 < len(reg.Rune); i += 2 {
			for r := reg.Rune[i]; r <= reg.Rune[i+1]; r++ {
				if len(values) == limit {
					return nil, false
				}
				values = append(values, string(r))
			}
		}
	case syntax.OpAlternate:
		for _, sub := range reg.Sub {
			vs, ok := LiteralSet(sub, limit-len(values))
			if !ok {
				return nil, false
			}
			values = append(values, vs...)
		}
	case syntax.OpConcat:
		values = []string{""}
		for _, sub := range reg.Sub {
			vs, ok := LiteralSet(sub, limit)
			if !ok || len(values)*len(vs) > limit {
				return nil, false
			}
			product := make([]string, 0, len(values)*len(vs))
			for _, prefix := range values {
				for _, v := range vs {
					product = append(product, prefix+v)
				}
			}
			values = product
		}
	}
	for _, v := range values {
		if v == "" || !exactQuotable(v) {
			return nil, false
		}
	}
	return values, len(values) > 0 && len(values) <= limit
}

// isAnyCharRepeat reports whether reg is (?s:.)* (op OpStar) or (?s:.)+.
func isAnyCharRepeat(reg *syntax.Regexp, op syntax.Op) bool {
	return reg.Op == op && reg.Sub[0].Op == syntax.OpAnyChar
}

// The patterns of the simplifier's filters (see lokiLabelFilterPattern).
const (
	KeepAllPattern  = "(?s:.*)"
	NonEmptyPattern = "(?s:.+)"
)

func (e *ParseError) Error() string { return e.Msg }
