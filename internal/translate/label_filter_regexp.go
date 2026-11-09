package translate

import (
	"regexp"
	"regexp/syntax"
	"strings"
)

// LokiLabelFilterPattern returns the pattern whose whole-value match,
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
func LokiLabelFilterPattern(value string) string {
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
		return KeepAllPattern // newContainsFilter (filter.go:556)
	}
	return KeepAllPattern + quoteLiteralPattern(literal, foldCase) + KeepAllPattern
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
		if b != KeepAllPattern {
			kept = append(kept, b)
		}
	}
	if len(kept) == 0 {
		return KeepAllPattern
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
			return KeepAllPattern, true
		}
	case syntax.OpPlus:
		if len(reg.Sub) == 1 && reg.Sub[0].Op == syntax.OpAnyCharNotNL {
			return NonEmptyPattern, true
		}
	case syntax.OpEmptyMatch:
		return KeepAllPattern, true
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
