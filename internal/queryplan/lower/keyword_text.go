package lower

import (
	"strings"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
)

// QuoteKeywordFieldText returns text, a query the string translator wrote, with each field name VictoriaLogs cannot
// read bare quoted where a filter names a field, the way the lowering writes it: a keyword or filter function
// (logsql.KeywordFieldName: not, in, and, or, options, contains_all, ...), and a name with a rune VictoriaLogs'
// lexer does not join into a word (logsql.BareFieldName: punctuation, a symbol, a combining mark, a zero-width
// space, an emoji; a dot and a hyphen the translator already quotes). The translator writes such a name bare
// (`not:="x"`, `;:="x"`) and VictoriaLogs rejects the query or reads it as something else, though Loki accepts the
// label; the lowering quotes it. The shadows hold the lowering to this text, so a query with such a name is held to
// the translator's text made right, not to its bytes (semantics/translator-writes-keyword-field-names-bare,
// semantics/translator-writes-exotic-field-names-bare). A text with no such name comes back as it is, with no
// allocation.
func QuoteKeywordFieldText(text string) string {
	var b *strings.Builder
	last := 0
	for i := 0; i < len(text); i++ {
		c := text[i]
		switch {
		case c == '"' || c == '`':
			i = endOfQuote(text, i)
		case c == '-' || c == '!':
			// A negation: the name is read from the next byte.
		case startsFieldName(text, i):
			j := i
			for j < len(text) && !isNameDelimiter(text[j]) {
				j++
			}
			if j > i && j < len(text) && text[j] == ':' && quotesAsFieldName(text[i:j]) && opensFilter(text[j+1:]) {
				if b == nil {
					b = &strings.Builder{}
					b.Grow(len(text) + 8)
				}
				b.WriteString(text[last:i])
				b.WriteString(logsql.QuoteFieldName(text[i:j]))
				last = j
			}
			if j > i {
				i = j - 1
			}
		}
	}
	if b == nil {
		return text
	}
	b.WriteString(text[last:])
	return b.String()
}

// startsFieldName reports whether a field name can begin at text[i]: at the start of the text, after a blank or an
// opening parenthesis, or after a negation (- or !) that begins there itself.
func startsFieldName(text string, i int) bool {
	if i > 0 && (text[i-1] == '-' || text[i-1] == '!') {
		i--
	}
	return i == 0 || text[i-1] == ' ' || text[i-1] == '('
}

// isNameDelimiter reports a byte that ends the word a field name is read from: a blank, a parenthesis, a quote, a
// pipe, a comma, a colon or a brace.
func isNameDelimiter(c byte) bool {
	switch c {
	case ' ', '\t', '\n', '(', ')', '"', '`', '|', ',', ':', '{', '}':
		return true
	}
	return false
}

// quotesAsFieldName reports a bare word the translator wrote as a field name that the lowering writes quoted: a
// keyword, or a name with a rune that is neither a word rune, a dot nor a hyphen (the translator quotes the
// names with a dot or a hyphen itself).
func quotesAsFieldName(word string) bool {
	if logsql.KeywordFieldName(word) {
		return true
	}
	if logsql.BareFieldName(word) {
		return false
	}
	for _, r := range word {
		if r != '.' && r != '-' && !logsql.BareFieldName(string(r)) {
			return true
		}
	}
	return false
}

// opensFilter reports the text after the colon of a field filter: an operator, a quoted value or a function.
func opensFilter(rest string) bool {
	if rest == "" {
		return false
	}
	switch c := rest[0]; c {
	case '=', '~', '*', '>', '<', '!', '"':
		return true
	default:
		return c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z'
	}
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
