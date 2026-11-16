package proxy

import (
	"strconv"
	"strings"
	"unicode"
	"unicode/utf16"
	"unicode/utf8"
)

// Loki v3.7.7's non-strict `| logfmt` (pkg/logql/log/parser.go:379-438 over the
// decoder of pkg/logql/log/logfmt, itself go-logfmt's), as the unwrap
// conversion check needs it to re-derive the value Loki unwraps:
//
//   - pairs are separated by bytes <= ' ';
//   - a key ends at '=' or at a byte <= ' ' (a bare key has no value); a key
//     holding '"' or invalid UTF-8, an empty key before '=', and a value
//     holding '=' or '"' are syntax errors: the pair is skipped up to the next
//     separator and parsing goes on (non-strict);
//   - a quoted value is unquoted (\" \\ \/ \' \b \f \n \r \t \uXXXX with
//     surrogate pairs); an unterminated or invalid one is skipped;
//   - an empty value is not a label (keepEmpty false), so a later duplicate can
//     set it; otherwise the first value of a key wins (Hints.Extracted);
//   - U+FFFD and invalid UTF-8 in a value become a space (removeInvalidUtf).
//
// It yields every pair the decoder accepts, in order; the caller applies the
// first-wins and empty-value rules with its label set.
func lokiLogfmtPairs(line string, yield func(key, value string)) {
	pos := 0
	for pos < len(line) {
		// garbage before the key
		for pos < len(line) && line[pos] <= ' ' {
			pos++
		}
		if pos >= len(line) {
			return
		}
		start, multibyte := pos, false
		key, value := "", ""
		ok := true
	scanKey:
		for {
			if pos >= len(line) {
				key = line[start:pos]
				if multibyte && strings.ContainsRune(key, utf8.RuneError) {
					ok = false
				}
				break
			}
			c := line[pos]
			switch {
			case c == '=':
				key = line[start:pos]
				if key == "" || (multibyte && strings.ContainsRune(key, utf8.RuneError)) {
					pos = lokiLogfmtSkip(line, pos)
					ok = false
					break scanKey
				}
				pos++
				value, pos, ok = lokiLogfmtValue(line, pos)
				break scanKey
			case c == '"':
				pos = lokiLogfmtSkip(line, pos)
				ok = false
				break scanKey
			case c <= ' ':
				key = line[start:pos]
				if multibyte && strings.ContainsRune(key, utf8.RuneError) {
					ok = false
				}
				break scanKey
			case c >= utf8.RuneSelf:
				multibyte = true
			}
			pos++
		}
		if ok && key != "" {
			yield(key, value)
		}
	}
}

// lokiLogfmtValue reads the value after '=' at pos.
func lokiLogfmtValue(line string, pos int) (string, int, bool) {
	if pos >= len(line) || line[pos] <= ' ' {
		return "", pos, true
	}
	if line[pos] == '"' {
		escaped, esc := false, false
		for i := pos + 1; i < len(line); i++ {
			c := line[i]
			switch {
			case esc:
				esc = false
			case c == '\\':
				escaped, esc = true, true
			case c == '"':
				raw := line[pos : i+1]
				if !escaped {
					return raw[1 : len(raw)-1], i + 1, true
				}
				v, ok := lokiLogfmtUnquote(raw)
				return v, i + 1, ok
			}
		}
		return "", len(line), false // unterminated
	}
	start := pos
	for ; pos < len(line); pos++ {
		c := line[pos]
		if c == '=' || c == '"' {
			return "", lokiLogfmtSkip(line, pos), false
		}
		if c <= ' ' {
			break
		}
	}
	return line[start:pos], pos, true
}

// lokiLogfmtSkip skips to the next separator after a syntax error.
func lokiLogfmtSkip(line string, pos int) int {
	for pos < len(line) && line[pos] > ' ' {
		pos++
	}
	return pos
}

// lokiLogfmtUnquote unquotes a quoted logfmt value holding escapes, as the
// decoder's unquoteBytes does.
func lokiLogfmtUnquote(quoted string) (string, bool) {
	s := quoted[1 : len(quoted)-1]
	var b strings.Builder
	for r := 0; r < len(s); {
		c := s[r]
		switch {
		case c == '\\':
			r++
			if r >= len(s) {
				return "", false
			}
			switch s[r] {
			case '"', '\\', '/', '\'':
				b.WriteByte(s[r])
				r++
			case 'b':
				b.WriteByte('\b')
				r++
			case 'f':
				b.WriteByte('\f')
				r++
			case 'n':
				b.WriteByte('\n')
				r++
			case 'r':
				b.WriteByte('\r')
				r++
			case 't':
				b.WriteByte('\t')
				r++
			case 'u':
				rr := lokiLogfmtU4(s[r-1:])
				if rr < 0 {
					return "", false
				}
				r += 5
				if utf16.IsSurrogate(rr) {
					if dec := utf16.DecodeRune(rr, lokiLogfmtU4(s[r:])); dec != unicode.ReplacementChar {
						r += 6
						b.WriteRune(dec)
						break
					}
					rr = unicode.ReplacementChar
				}
				b.WriteRune(rr)
			default:
				return "", false
			}
		case c == '"':
			return "", false
		case c < utf8.RuneSelf:
			b.WriteByte(c)
			r++
		default:
			rr, size := utf8.DecodeRuneInString(s[r:])
			r += size
			b.WriteRune(rr)
		}
	}
	return b.String(), true
}

func lokiLogfmtU4(s string) rune {
	if len(s) < 6 || s[0] != '\\' || s[1] != 'u' {
		return -1
	}
	v, err := strconv.ParseUint(s[2:6], 16, 16) // four hex digits: at most 0xFFFF
	if err != nil {
		return -1
	}
	return rune(v)
}

// lokiLabelValue is a parsed value as Loki stores it: U+FFFD and invalid
// UTF-8 become a space (parser.go removeInvalidUtf).
func lokiLabelValue(v string) string {
	if !strings.ContainsRune(v, utf8.RuneError) {
		return v
	}
	return strings.Map(func(r rune) rune {
		if r == utf8.RuneError {
			return ' '
		}
		return r
	}, v)
}
