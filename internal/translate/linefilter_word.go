package translate

import (
	"regexp/syntax"
	"strconv"
	"strings"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
)

// WordBoundaryFilter is the VictoriaLogs phrase filter of a regexp line filter that is a literal between word
// boundaries, `\bfailed\b` or `\bfoo bar\b` (optionally non-capturing alternatives of such literals,
// `\b(?:error|failed)\b`): the text of each literal as a phrase filter, which VictoriaLogs answers from its bloom
// filters where the regexp scans every line. ok is false for every other regexp, which stays a regexp filter.
//
// The two match the same lines when the literal starts and ends with an ASCII word character: Go's \b is a boundary
// between an ASCII word character ([0-9A-Za-z_]) and anything else, and VictoriaLogs' phrase filter requires the
// character before a phrase that starts with a token character, and the one after a phrase that ends with one, to be
// a non-token character (Unicode letters and digits and _ are token characters). They differ only on a neighbour that
// is a non-ASCII letter or digit (`éfailed`): a boundary for Go, a token continuation for VictoriaLogs. A literal
// whose edge is not an ASCII word character (`\b:failed\b`, `\bé\b`) means the opposite under the two rules and is
// left as a regexp. A case-insensitive literal ((?i)) is left as a regexp: VictoriaLogs' i("...") filter does not use
// the bloom filters and folds case by its own rules.
func WordBoundaryFilter(pattern string) (f logsql.FilterExpr, ok bool) {
	if !strings.Contains(pattern, `\b`) {
		return nil, false // the common regexp costs a substring scan, not a parse
	}
	re, err := syntax.Parse(pattern, syntax.Perl)
	if err != nil {
		return nil, false
	}
	re = re.Simplify()
	for re.Op == syntax.OpCapture {
		re = re.Sub[0]
	}
	if re.Op != syntax.OpConcat || len(re.Sub) != 3 || re.Sub[0].Op != syntax.OpWordBoundary || re.Sub[2].Op != syntax.OpWordBoundary {
		return nil, false
	}
	body := re.Sub[1]
	for body.Op == syntax.OpCapture {
		body = body.Sub[0]
	}
	phrases := []string{}
	switch body.Op {
	case syntax.OpLiteral:
		p, ok := boundedLiteral(body)
		if !ok {
			return nil, false
		}
		phrases = append(phrases, p)
	case syntax.OpAlternate:
		for _, sub := range body.Sub {
			for sub.Op == syntax.OpCapture {
				sub = sub.Sub[0]
			}
			p, ok := boundedLiteral(sub)
			if !ok {
				return nil, false
			}
			phrases = append(phrases, p)
		}
	default:
		return nil, false
	}
	if len(phrases) == 1 {
		return logsql.Phrase{Value: phrases[0]}, true
	}
	alts := make(logsql.AnyOf, len(phrases))
	for i, p := range phrases {
		alts[i] = logsql.Phrase{Value: p}
	}
	return logsql.Group{Expr: alts}, true
}

// boundedLiteral is the text of a case-sensitive literal whose first and last characters are ASCII word characters.
func boundedLiteral(re *syntax.Regexp) (string, bool) {
	if re.Op != syntax.OpLiteral || re.Flags&syntax.FoldCase != 0 || len(re.Rune) == 0 {
		return "", false
	}
	if !asciiWord(re.Rune[0]) || !asciiWord(re.Rune[len(re.Rune)-1]) {
		return "", false
	}
	return string(re.Rune), true
}

func asciiWord(r rune) bool {
	return r == '_' || r >= '0' && r <= '9' || r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z'
}

// WordBoundaryFilterText is WordBoundaryFilter for the string translator: the LogsQL text of the filter of a
// double-quoted regexp literal as the client wrote it, negated for !~, and ok false when the regexp is not a bounded
// literal (or the literal is not a plain double-quoted string).
func WordBoundaryFilterText(literal string, negate bool) (string, bool) {
	if len(literal) == 0 || literal[0] != '"' || !strings.Contains(literal, `\\b`) {
		return "", false // the spelling of a \b in a double-quoted literal; no unquote for the other regexps
	}
	pattern, err := strconv.Unquote(literal)
	if err != nil {
		return "", false
	}
	f, ok := WordBoundaryFilter(pattern)
	if !ok {
		return "", false
	}
	if negate {
		return logsql.NotExpr{Expr: f}.String(), true
	}
	return f.String(), true
}
