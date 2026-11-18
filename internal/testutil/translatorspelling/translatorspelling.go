// Package translatorspelling is test support: it recognises, by content, the spellings of a query the string
// translator reads otherwise than the query plan, which reads the parsed tree, and names the registry case of each.
// The proxy serves the plan's text for them (the lowering has no text screen), so the corpus and grid shadows count
// such a row as the translator's known divergence instead of a mismatch of the lowering, and each case's Loki
// differential holds what the proxy answers. Nothing in the proxy imports it; it goes with the string translator.
package translatorspelling

import (
	"regexp"
	"strconv"
	"strings"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/physical"
)

// The registry cases of the spellings Case recognises.
const (
	// QuotedText is quoted text of a log query under a binary operation that the translator's scans of the whole
	// binary text read as syntax: a parenthesis, a run of two blanks, bool, without or the bytes of its markers.
	QuotedText = "semantics/translator-reads-quoted-text-as-syntax"
	// UnwrapConversionBlank is an unwrap conversion written with a blank before or inside its parenthesis
	// (`bytes (n)`, `bytes( n )`): the translator reads the whole call as the label's name.
	UnwrapConversionBlank = "semantics/blank-before-call-parenthesis-read-unlike-loki"
	// QuantileLiteral is a quantile_over_time parameter written otherwise than the shortest form of its value
	// (`0.50`, `5e-1`, `.5`): the translator copies the literal into the stats function.
	QuantileLiteral = "semantics/translator-writes-leading-dot-quantile"
	// BinaryText is a binary operation whose operator, number or vector matching the translator reads from the text
	// otherwise than the tree: an operator without a blank on each side, a number written otherwise than its shortest
	// form or in parentheses, a vector matching modifier spelled otherwise than `on(a, b)`.
	BinaryText = "semantics/translator-reads-binary-operation-text"
)

// Case is the registry case of the spelling of q (with its plan p) that the string translator reads otherwise than the
// plan, and "" when q holds none. A caller asks it only for a row where the two texts differ: it names the spelling,
// it does not prove that the spelling is the difference.
func Case(q string, p *queryplan.Plan) string {
	binary := holdsBinOp(p.Root)
	switch {
	case binary && quotedLogText(q):
		return QuotedText
	case unwrapConversionBlankRE.MatchString(withoutStrings(q)):
		return UnwrapConversionBlank
	case quantileLiteral(q):
		return QuantileLiteral
	case binary && binaryText(unquoted(q)):
		return BinaryText
	}
	return ""
}

// holdsBinOp reports a binary operation at the root of a plan or under its label_replace calls, rankings and vector
// aggregations.
func holdsBinOp(n queryplan.Node) bool {
	for {
		switch v := n.(type) {
		case *queryplan.BinOp:
			return true
		case *queryplan.LabelReplace:
			n = v.In
		case *queryplan.Rank:
			n = v.In
		case *queryplan.VectorAgg:
			n = v.In
		default:
			return false
		}
	}
}

// quotedLogText reports a log query of q whose quoted text holds what the translator reads as syntax in a binary
// text. The log queries are read from the parsed tree, so a label_replace call's own strings are not.
func quotedLogText(q string) bool {
	expr, err := logql.Parse(logql.CanonicalizeLogRanges(q))
	if err != nil {
		return false
	}
	var logs []string
	collectLogQueries(expr, &logs)
	for _, text := range logs {
		for _, s := range quotedStrings(text) {
			if strings.ContainsAny(s, "()") || strings.Contains(s, "  ") {
				return true
			}
			for _, w := range [...]string{"bool", "without", "|||", physical.MatchingPrefix, physical.BinaryPrefix, physical.LabelReplacePrefix} {
				if strings.Contains(s, w) {
					return true
				}
			}
		}
	}
	return false
}

func collectLogQueries(e logql.Expr, out *[]string) {
	switch v := e.(type) {
	case *logql.LogQuery:
		*out = append(*out, v.String())
	case *logql.RangeAggregation:
		collectLogQueries(v.Inner, out)
	case *logql.VectorAggregation:
		collectLogQueries(v.Inner, out)
	case *logql.BinOpExpr:
		collectLogQueries(v.Left, out)
		collectLogQueries(v.Right, out)
	case *logql.OpaqueMetricExpr:
		if v.Inner != nil {
			collectLogQueries(v.Inner, out)
		}
	}
}

// quotedStrings are the contents of the double-quoted and backquoted strings of text.
func quotedStrings(text string) []string {
	var out []string
	for i := 0; i < len(text); i++ {
		c := text[i]
		if c != '"' && c != '`' {
			continue
		}
		end := closing(text, i)
		out = append(out, text[i+1:min(end, len(text))])
		i = end
	}
	return out
}

// closing is the index of the quote that closes the string opening at text[i] (len(text) when it is not closed).
func closing(text string, i int) int {
	c := text[i]
	for j := i + 1; j < len(text); j++ {
		if text[j] == '\\' && c == '"' {
			j++
		} else if text[j] == c {
			return j
		}
	}
	return len(text)
}

// withoutStrings is q with each of its strings written as `"`.
func withoutStrings(q string) string {
	var b strings.Builder
	for i := 0; i < len(q); i++ {
		if c := q[i]; c == '"' || c == '`' {
			i = closing(q, i)
			b.WriteByte('"')
			continue
		}
		b.WriteByte(q[i])
	}
	return b.String()
}

// unquoted is q with its strings written as `"` and each log query (a stream selector and its pipeline, up to its
// range or the parenthesis that closes it) and range written as `{` and `[`: the text of the metric operations around
// the log queries.
func unquoted(q string) string {
	var b strings.Builder
	for i := 0; i < len(q); i++ {
		switch c := q[i]; c {
		case '"', '`':
			i = closing(q, i)
			b.WriteByte('"')
		case '{':
			// The log query: up to its range, or up to the parenthesis that closes the call it is in.
			depth := 0
			for i++; i < len(q); i++ {
				if q[i] == '"' || q[i] == '`' {
					i = closing(q, i)
					continue
				}
				if q[i] == '(' {
					depth++
				} else if q[i] == ')' {
					if depth == 0 {
						break
					}
					depth--
				} else if q[i] == '[' && depth == 0 {
					break
				}
			}
			b.WriteByte('{')
			i--
		case '[':
			for i < len(q) && q[i] != ']' {
				i++
			}
			b.WriteByte('[')
		default:
			b.WriteByte(c)
		}
	}
	return b.String()
}

var (
	unwrapConversionBlankRE = regexp.MustCompile(`unwrap\s+(?i:bytes|duration_seconds|duration)(?:\s+\(|\(\s|\(\S*\s+\))`)
	quantileLiteralRE       = regexp.MustCompile(`quantile_over_time\(\s*([^,\s]+)\s*,`)
	binaryOperatorRE        = regexp.MustCompile(`[-+*/%^<>=!|~]+`)
	binaryNumberRE          = regexp.MustCompile(`(?:^|[^\w.])(\.?\d[\d.]*(?:[eE][-+]?\d+)?)(?:[^\w.]|$)`)
	parenthesizedNumberRE   = regexp.MustCompile(`\(\s*-?\.?\d[\d.eE+-]*\s*\)`)
	matchingSpellingRE      = regexp.MustCompile(`\b(?:on|ignoring|group_left|group_right)\s+\(|\)(?:group_left|group_right)|\b(?:group_left|group_right)(?:\s|$)`)
	matchingLabelsRE        = regexp.MustCompile(`\b(?:on|ignoring|group_left|group_right)\(([^()]*)\)`)
)

// quantileLiteral reports a quantile_over_time parameter written otherwise than the shortest form of its value.
func quantileLiteral(q string) bool {
	for _, m := range quantileLiteralRE.FindAllStringSubmatch(withoutStrings(q), -1) {
		v, err := strconv.ParseFloat(m[1], 64)
		if err == nil && m[1] != strconv.FormatFloat(v, 'g', -1, 64) {
			return true
		}
	}
	return false
}

// binaryText reports a binary operation text (strings, selectors and ranges taken out) the translator reads otherwise
// than the tree: a blank other than one space, an operator without a space on each side, a number written otherwise
// than its shortest form or in parentheses, or a vector matching modifier spelled otherwise than `on(a, b)`.
func binaryText(u string) bool {
	if strings.ContainsAny(u, "\t\n\r") || strings.Contains(u, "  ") || parenthesizedNumberRE.MatchString(u) || matchingSpellingRE.MatchString(u) {
		return true
	}
	for _, m := range matchingLabelsRE.FindAllStringSubmatch(u, -1) {
		if labels := m[1]; labels != strings.TrimSpace(labels) || strings.Contains(strings.ReplaceAll(labels, ", ", ""), ",") || strings.Contains(labels, " ,") {
			return true
		}
	}
	for _, loc := range binaryOperatorRE.FindAllStringIndex(u, -1) {
		if loc[0] == 0 || u[loc[0]-1] != ' ' || loc[1] == len(u) || u[loc[1]] != ' ' {
			return true
		}
	}
	for _, m := range binaryNumberRE.FindAllStringSubmatch(u, -1) {
		v, err := strconv.ParseFloat(m[1], 64)
		if err == nil && m[1] != strconv.FormatFloat(v, 'f', -1, 64) {
			return true
		}
	}
	return false
}
