package translator

import (
	"fmt"
	"regexp"
	"regexp/syntax"
	"strconv"
	"strings"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
)

// selectorRegexpFieldFilter is the LogsQL filter for a stream-selector
// matcher `label=~pattern`: a Prometheus label matcher, compiled as
// ^(?s:pattern)$ (Loki v3.7.7
// vendor/github.com/prometheus/prometheus/model/labels/regexp.go:76), so it
// matches the whole value and '.' matches newlines.
func selectorRegexpFieldFilter(label, pattern string, negate bool) string {
	return wholeValueRegexpFilter(label, pattern, negate, true)
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
		re, err := regexp.Compile(anchorLabelFilterPattern(lokiLabelFilterPattern(value)))
		if err != nil {
			return dc, fmt.Errorf("invalid regex %q in label filter: %w", value, err)
		}
		dc.re = re
	}
	return dc, nil
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
	switch {
	case pattern == ".*" || pattern == keepAllPattern:
		return buildFieldFilterStr(label, logsql.FieldOpRegexp, ".*", negate)
	case pattern == ".+" || pattern == nonEmptyPattern:
		return buildFieldFilterStr(label, logsql.FieldOpRegexp, ".+", negate)
	case pattern != "" && regexp.QuoteMeta(pattern) == pattern && exactQuotable(pattern):
		// No metacharacter: the literal itself, without parsing it.
		return buildFieldFilterStr(label, logsql.FieldOpExact, pattern, negate)
	}
	flags := syntax.Perl
	if dotNL {
		flags |= syntax.DotNL
	}
	if reg, err := syntax.Parse(pattern, flags); err == nil {
		reg = reg.Simplify()
		switch {
		case isAnyCharRepeat(reg, syntax.OpStar):
			return buildFieldFilterStr(label, logsql.FieldOpRegexp, ".*", negate)
		case isAnyCharRepeat(reg, syntax.OpPlus):
			return buildFieldFilterStr(label, logsql.FieldOpRegexp, ".+", negate)
		}
		if literal, ok := plainLiteral(reg); ok {
			return buildFieldFilterStr(label, logsql.FieldOpExact, literal, negate)
		}
		if reg.Op == syntax.OpConcat && len(reg.Sub) == 3 && isAnyCharRepeat(reg.Sub[0], syntax.OpStar) && isAnyCharRepeat(reg.Sub[2], syntax.OpStar) {
			if literal, ok := plainLiteral(reg.Sub[1]); ok {
				return buildFieldFilterStr(label, logsql.FieldOpRegexp, regexp.QuoteMeta(literal), negate)
			}
		}
		if !dotNL && strings.Contains(pattern, "|") {
			// A label filter's literal alternation is Loki's OR of equality
			// checks; VictoriaLogs compares the unpacked values faster than it
			// runs the anchored regexp on each.
			if values, ok := literalSet(reg, maxExactAlternatives); ok {
				return exactAlternatives(label, values, negate)
			}
		}
	}
	anchored := logsql.AnchorLabelMatcherRegex(pattern)
	if !dotNL {
		anchored = anchorLabelFilterPattern(pattern)
	}
	return buildFieldFilterStr(label, logsql.FieldOpRegexp, anchored, negate)
}

// anchorLabelFilterPattern anchors a lokiLabelFilterPattern result the way
// Loki does, ^(?:pattern)$, spelled so '.' keeps not matching a newline in
// VictoriaLogs too.
func anchorLabelFilterPattern(pattern string) string {
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
func exactAlternatives(label string, values []string, negate bool) string {
	exact := make([]string, len(values))
	for i, v := range values {
		exact[i] = buildFieldFilterStr(label, logsql.FieldOpExact, v, false)
	}
	filter := "(" + strings.Join(exact, " OR ") + ")"
	if negate {
		return "-" + filter
	}
	return filter
}

// maxExactAlternatives bounds the exact filters a literal alternation becomes.
const maxExactAlternatives = 16

// literalSet returns the values a regexp matching a finite set of non-empty
// literals matches, at most limit of them: literals, alternations, character
// classes and their concatenations (Go's parser factors `api|apx` into
// `ap[ix]`).
func literalSet(reg *syntax.Regexp, limit int) ([]string, bool) {
	var values []string
	switch reg.Op {
	case syntax.OpLiteral:
		if v, ok := plainLiteral(reg); ok {
			values = []string{v}
		}
	case syntax.OpCapture:
		return literalSet(reg.Sub[0], limit)
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
			vs, ok := literalSet(sub, limit-len(values))
			if !ok {
				return nil, false
			}
			values = append(values, vs...)
		}
	case syntax.OpConcat:
		values = []string{""}
		for _, sub := range reg.Sub {
			vs, ok := literalSet(sub, limit)
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

// lokiLabelFilterPattern returns the pattern whose whole-value match,
// ^(?:pattern)$, selects what Loki's label filter `| label =~ "value"`
// selects. Unlike a stream-selector matcher, Loki does not anchor a label
// filter's regexp as written: parseRegexpFilter (pkg/logql/log/filter.go:645,
// reached from NewStringLabelFilter in label_filter.go:360) first tries
// RegexSimplifier.Simplify (filter.go:696) and only anchors, as ^(?:re)$ and
// without the s flag, what it cannot simplify. The simplifier turns
//   - a literal into an equality check, case-insensitively under (?i);
//   - `.*` and the empty pattern into a filter that keeps everything;
//   - `.+` into a non-empty check;
//   - a concatenation of one literal and `.*` parts (api.*, .*api) into a
//     substring check;
//   - a literal followed by an alternation (what Go's parser makes of a common
//     prefix, so `prod-eu|prod` and `api|api-gateway`) into one substring
//     check per branch;
//   - an alternation into the OR of its branches, where a branch that keeps
//     everything is dropped (newOrFilter, filter.go:309): `GET|` is an equality
//     check on GET.
//
// The cases were checked against Loki v3.7.7's pipeline.
func lokiLabelFilterPattern(value string) string {
	if value != "" && regexp.QuoteMeta(value) == value {
		return value // a literal: an equality check
	}
	reg, err := syntax.Parse(value, syntax.Perl)
	if err != nil {
		// Left for VictoriaLogs to reject; Loki rejects it at parse time.
		return value
	}
	if pattern, ok := simplifiedLabelFilter(reg.Simplify()); ok {
		return pattern
	}
	// Loki's fallback: the regexp itself, anchored as ^(?:re)$.
	return value
}

// The patterns of the simplifier's filters (see lokiLabelFilterPattern).
const (
	keepAllPattern  = "(?s:.*)"
	nonEmptyPattern = "(?s:.+)"
)

func containsPattern(literal string, foldCase bool) string {
	if literal == "" {
		return keepAllPattern // newContainsFilter (filter.go:556)
	}
	return keepAllPattern + quoteLiteralPattern(literal, foldCase) + keepAllPattern
}

func quoteLiteralPattern(literal string, foldCase bool) string {
	if foldCase {
		return "(?i:" + regexp.QuoteMeta(literal) + ")"
	}
	return regexp.QuoteMeta(literal)
}

// orPatterns joins branches the way newOrFilter chains them: a branch that
// keeps everything is dropped unless it is the only one left.
func orPatterns(branches []string) string {
	kept := branches[:0:0]
	for _, b := range branches {
		if b != keepAllPattern {
			kept = append(kept, b)
		}
	}
	if len(kept) == 0 {
		return keepAllPattern
	}
	return strings.Join(kept, "|")
}

func isFoldCase(reg *syntax.Regexp) bool { return reg.Flags&syntax.FoldCase != 0 }

func isAnyCharNotNLStar(reg *syntax.Regexp) bool {
	return reg.Op == syntax.OpStar && reg.Sub[0].Op == syntax.OpAnyCharNotNL
}

// simplifiedLabelFilter mirrors RegexSimplifier.Simplify (filter.go:696).
func simplifiedLabelFilter(reg *syntax.Regexp) (string, bool) {
	switch reg.Op {
	case syntax.OpAlternate:
		branches := make([]string, 0, len(reg.Sub))
		for _, sub := range reg.Sub {
			b, ok := simplifiedLabelFilter(sub)
			if !ok {
				return "", false
			}
			branches = append(branches, b)
		}
		return orPatterns(branches), true
	case syntax.OpConcat:
		return simplifiedLabelFilterConcat(reg, "", false)
	case syntax.OpCapture:
		return simplifiedLabelFilter(reg.Sub[0])
	case syntax.OpLiteral:
		return quoteLiteralPattern(string(reg.Rune), isFoldCase(reg)), true
	case syntax.OpStar:
		if reg.Sub[0].Op == syntax.OpAnyCharNotNL {
			return keepAllPattern, true
		}
	case syntax.OpPlus:
		if len(reg.Sub) == 1 && reg.Sub[0].Op == syntax.OpAnyCharNotNL {
			return nonEmptyPattern, true
		}
	case syntax.OpEmptyMatch:
		return keepAllPattern, true
	}
	return "", false
}

// simplifiedLabelFilterConcat mirrors simplifyConcat (filter.go:754): at most
// three parts, one literal, `.*` parts, and an alternation after the literal.
func simplifiedLabelFilterConcat(reg *syntax.Regexp, base string, haveBase bool) (string, bool) {
	subs := make([]*syntax.Regexp, 0, len(reg.Sub))
	for _, sub := range reg.Sub {
		if sub.Op == syntax.OpCapture {
			sub = sub.Sub[0] // util.ClearCapture
		}
		if sub.Op != syntax.OpEmptyMatch {
			subs = append(subs, sub)
		}
	}
	if len(subs) > 3 {
		return "", false
	}
	var branches []string
	literals := 0
	foldCase := false
	for _, sub := range subs {
		switch {
		case sub.Op == syntax.OpLiteral:
			if literals != 0 {
				return "", false
			}
			literals++
			base += string(sub.Rune)
			haveBase = true
			foldCase = isFoldCase(sub)
		case sub.Op == syntax.OpAlternate && haveBase:
			alt, ok := simplifiedLabelFilterConcatAlternate(sub, base, foldCase)
			if !ok {
				return "", false
			}
			branches = append(branches, alt...)
		case isAnyCharNotNLStar(sub):
		default:
			return "", false
		}
	}
	if branches != nil {
		return orPatterns(branches), true
	}
	if haveBase {
		return containsPattern(base, foldCase), true
	}
	return "", false
}

// simplifiedLabelFilterConcatAlternate mirrors simplifyConcatAlternate
// (filter.go:812): one substring check per branch of the alternation.
func simplifiedLabelFilterConcatAlternate(reg *syntax.Regexp, literal string, foldCase bool) ([]string, bool) {
	branches := make([]string, 0, len(reg.Sub))
	for _, alt := range reg.Sub {
		if !foldCase && isFoldCase(alt) {
			return nil, false
		}
		switch {
		case alt.Op == syntax.OpEmptyMatch, isAnyCharNotNLStar(alt):
			branches = append(branches, containsPattern(literal, foldCase))
		case alt.Op == syntax.OpLiteral:
			branches = append(branches, containsPattern(literal+string(alt.Rune), foldCase))
		case alt.Op == syntax.OpConcat:
			b, ok := simplifiedLabelFilterConcat(alt, literal, true)
			if !ok {
				return nil, false
			}
			branches = append(branches, b)
		default:
			return nil, false
		}
	}
	return branches, true
}
