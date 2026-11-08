package translator

import (
	"fmt"
	"regexp"
	"regexp/syntax"
	"strconv"
	"strings"

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

func containsPattern(literal string, foldCase bool) string {
	if literal == "" {
		return translate.KeepAllPattern // newContainsFilter (filter.go:556)
	}
	return translate.KeepAllPattern + quoteLiteralPattern(literal, foldCase) + translate.KeepAllPattern
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
		if b != translate.KeepAllPattern {
			kept = append(kept, b)
		}
	}
	if len(kept) == 0 {
		return translate.KeepAllPattern
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
			return translate.KeepAllPattern, true
		}
	case syntax.OpPlus:
		if len(reg.Sub) == 1 && reg.Sub[0].Op == syntax.OpAnyCharNotNL {
			return translate.NonEmptyPattern, true
		}
	case syntax.OpEmptyMatch:
		return translate.KeepAllPattern, true
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
