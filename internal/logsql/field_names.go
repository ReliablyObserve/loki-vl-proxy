package logsql

import (
	"unicode"
	"unicode/utf8"
)

// BareFieldName reports whether VictoriaLogs reads name, written bare, as one word: every rune of it is a letter, a
// digit or an underscore, the runes its lexer joins into a token (lib/logstorage isTokenRune: unicode.IsLetter,
// unicode.IsDigit and _). Any other rune (a dot, a hyphen, punctuation, a symbol, a combining mark, a superscript or
// a roman numeral, a zero-width space, an emoji) ends the token, so a filter or a pipe that names the field bare is
// rejected or read as something else (`;:="z"`, `copy ; as x`); quoted, every name is read as the field it is. The
// empty name is no field (the message), so it is bare. Keywords and filter functions (KeywordFieldName) are words
// too and are left to that test.
func BareFieldName(name string) bool {
	for i := 0; i < len(name); i++ {
		c := name[i]
		if c >= utf8.RuneSelf {
			return bareRunes(name[i:])
		}
		if c != '_' && (c < 'a' || c > 'z') && (c < 'A' || c > 'Z') && (c < '0' || c > '9') {
			return false
		}
	}
	return true
}

// bareRunes is BareFieldName for the tail of a name that holds a rune above ASCII: invalid UTF-8 is no word.
func bareRunes(s string) bool {
	for _, r := range s {
		if r == utf8.RuneError || r != '_' && !unicode.IsLetter(r) && !unicode.IsDigit(r) {
			return false
		}
	}
	return true
}

// QuoteFieldName is name as a filter or a pipe writes it where it names a field: quoted when VictoriaLogs cannot
// read it bare (not BareFieldName) or reads it as a keyword or a filter function (KeywordFieldName), else name as it
// is, with no allocation. A name that is already quoted (it begins with a double quote or a backquote, as a name with
// a dot or a hyphen is when the selector builders have written it) is returned as it is. A label name never begins
// with a quote (translate.SanitizeFieldIdentifier drops them), so the test cannot read a field as a quoted string.
func QuoteFieldName(name string) string {
	if name == "" || name[0] == '"' || name[0] == '`' {
		return name
	}
	if BareFieldName(name) && !KeywordFieldName(name) {
		return name
	}
	return quoteLogsQL(name)
}
