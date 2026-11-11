package lower

import (
	"strings"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
)

// QuoteKeywordFieldText returns text, a query the string translator wrote, with each field name VictoriaLogs cannot
// read bare (logsql.KeywordFieldName: not, in, and, or, options, contains_all, ...) quoted where a filter names a
// field, the way the lowering writes it. The translator writes such a name bare (`not:="x"`) and VictoriaLogs
// rejects the query, though Loki accepts a label called not or in; the lowering quotes it. The shadows hold the
// lowering to this text, so a query with such a name is held to the translator's text made right, not to its bytes
// (semantics/translator-writes-keyword-field-names-bare). A text with no such name comes back as it is, with no
// allocation.
func QuoteKeywordFieldText(text string) string {
	var b *strings.Builder
	last := 0
	for i := 0; i < len(text); i++ {
		switch c := text[i]; {
		case c == '"' || c == '`':
			i = endOfQuote(text, i)
		case isFieldNameStart(c) && (i == 0 || text[i-1] == ' ' || text[i-1] == '(' || text[i-1] == '-'):
			j := i
			for j < len(text) && (text[j] == '_' || text[j] >= 'a' && text[j] <= 'z' || text[j] >= 'A' && text[j] <= 'Z') {
				j++
			}
			if j < len(text) && text[j] == ':' && logsql.KeywordFieldName(text[i:j]) {
				if b == nil {
					b = &strings.Builder{}
					b.Grow(len(text) + 8)
				}
				b.WriteString(text[last:i])
				b.WriteString(logsql.QuoteKeywordField(text[i:j]))
				last = j
			}
			i = j - 1
		}
	}
	if b == nil {
		return text
	}
	b.WriteString(text[last:])
	return b.String()
}

// isFieldNameStart reports a byte a bare field name can begin with.
func isFieldNameStart(c byte) bool {
	return c == '_' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z'
}

// endOfQuote is the index of the byte that closes the double-quoted or backquoted string that opens at text[i]
// (the last byte when it is not closed); a backslash escapes the next byte of a double-quoted string.
func endOfQuote(text string, i int) int {
	q := text[i]
	for j := i + 1; j < len(text); j++ {
		switch {
		case text[j] == '\\' && q == '"':
			j++
		case text[j] == q:
			return j
		}
	}
	return len(text) - 1
}
