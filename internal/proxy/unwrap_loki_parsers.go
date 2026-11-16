package proxy

import (
	"strings"
	"unicode/utf16"
	"unicode/utf8"
)

// The logfmt reading of `| logfmt`, written from the logfmt format (key=value pairs separated by white space, a
// value bare or double-quoted with JSON escapes) and held to Loki's own answers for the same lines
// (TestLogfmtPairsMatchRecordedLoki: the pipeline oracle's labels after `| logfmt`). VictoriaLogs' unpack_logfmt
// reads some lines differently, so the conversion-error check re-reads a stored line with this decoder.
//
// The scanner, per pair:
//   - bytes up to and including the space (any byte <= ' ') separate pairs;
//   - a key runs until '=', a space or the end; a key holding a double quote, an invalid UTF-8 sequence or U+FFFD,
//     or an empty key before '=', is malformed;
//   - a key followed by a space or the end has the empty value;
//   - after '=', a double quote opens a quoted value that ends at the next unescaped quote (the next pair may start
//     right after it); a quote that never closes is malformed and ends the line;
//   - otherwise the value runs until a space or the end, and one holding '=' or '"' is malformed.
//
// A malformed pair yields nothing and the scan resumes after the next space.

// lokiLogfmtPairs calls yield with every key and value of line, in line order, duplicates included. Values are
// unquoted; keys and values are not sanitized (lokiLabelValue, the parsers' key sanitizer).
func lokiLogfmtPairs(line string, yield func(key, value string)) {
	pos := 0
	for pos < len(line) {
		if line[pos] <= ' ' {
			pos++
			continue
		}
		start, ascii := pos, true
		for pos < len(line) {
			c := line[pos]
			if c <= ' ' || c == '=' || c == '"' {
				break
			}
			ascii = ascii && c < utf8.RuneSelf
			pos++
		}
		key := line[start:pos]
		if pos < len(line) && line[pos] == '"' || key == "" || !ascii && !logfmtKeyValid(key) {
			pos = lokiLogfmtSkip(line, pos)
			continue
		}
		if pos == len(line) || line[pos] != '=' {
			yield(key, "")
			continue
		}
		value, next, ok := lokiLogfmtValue(line, pos+1)
		pos = next
		if ok {
			yield(key, value)
		}
	}
}

// logfmtKeyValid reports a key the scanner accepts: not empty, valid UTF-8, without U+FFFD.
func logfmtKeyValid(key string) bool {
	if key == "" {
		return false
	}
	for i := 0; i < len(key); i++ {
		if key[i] >= utf8.RuneSelf {
			return utf8.ValidString(key[i:]) && !strings.ContainsRune(key[i:], utf8.RuneError)
		}
	}
	return true
}

// lokiLogfmtValue reads the value that starts at pos (just after '='). It returns the value, the position the scan
// resumes at and whether the value is well formed.
func lokiLogfmtValue(line string, pos int) (string, int, bool) {
	if pos == len(line) || line[pos] <= ' ' {
		return "", pos, true
	}
	if line[pos] == '"' {
		end := pos + 1
		for end < len(line) && line[end] != '"' {
			if line[end] == '\\' {
				end++
			}
			end++
		}
		if end >= len(line) {
			return "", len(line), false
		}
		value, ok := lokiLogfmtUnquote(line[pos : end+1])
		if !ok {
			return "", lokiLogfmtSkip(line, end+1), false
		}
		return value, end + 1, true
	}
	start := pos
	for pos < len(line) {
		c := line[pos]
		if c <= ' ' {
			break
		}
		if c == '=' || c == '"' {
			return "", lokiLogfmtSkip(line, pos), false
		}
		pos++
	}
	return line[start:pos], pos, true
}

// lokiLogfmtSkip returns the position after the space that ends the malformed text at pos.
func lokiLogfmtSkip(line string, pos int) int {
	for pos < len(line) && line[pos] > ' ' {
		pos++
	}
	return pos
}

// lokiLogfmtUnquote decodes a quoted value, quotes included (the caller guarantees both), with JSON's string
// escapes: \" \\ \/ \b \f \n \r \t and \uXXXX (a surrogate pair is one rune, a lone surrogate U+FFFD). An
// invalid UTF-8 byte becomes U+FFFD; control characters are kept. ok is false for an unknown or short escape, a
// backslash at the end or an unescaped quote inside.
func lokiLogfmtUnquote(quoted string) (string, bool) {
	s := quoted[1 : len(quoted)-1]
	plain := true
	for i := 0; i < len(s) && plain; i++ {
		plain = s[i] != '\\' && s[i] != '"' && s[i] < utf8.RuneSelf
	}
	if plain {
		return s, true
	}
	var b strings.Builder
	b.Grow(len(s) + 2)
	for i := 0; i < len(s); {
		c := s[i]
		switch {
		case c == '"':
			return "", false
		case c == '\\':
			if i+1 == len(s) {
				return "", false
			}
			switch s[i+1] {
			case '"', '\\', '/':
				b.WriteByte(s[i+1])
			case 'b':
				b.WriteByte('\b')
			case 'f':
				b.WriteByte('\f')
			case 'n':
				b.WriteByte('\n')
			case 'r':
				b.WriteByte('\r')
			case 't':
				b.WriteByte('\t')
			case 'u':
				r := lokiLogfmtU4(s[i:])
				if r < 0 {
					return "", false
				}
				i += 6
				if utf16.IsSurrogate(r) {
					if r2 := lokiLogfmtU4(s[i:]); r2 >= 0 {
						if pair := utf16.DecodeRune(r, r2); pair != utf8.RuneError {
							b.WriteRune(pair)
							i += 6
							continue
						}
					}
					r = utf8.RuneError
				}
				b.WriteRune(r)
				continue
			default:
				return "", false
			}
			i += 2
		case c < utf8.RuneSelf:
			b.WriteByte(c)
			i++
		default:
			r, size := utf8.DecodeRuneInString(s[i:])
			b.WriteRune(r)
			i += size
		}
	}
	return b.String(), true
}

// lokiLogfmtU4 reads the escape \uXXXX at the start of s: its code unit, or -1 when s does not start with one.
func lokiLogfmtU4(s string) rune {
	if len(s) < 6 || s[0] != '\\' || s[1] != 'u' {
		return -1
	}
	var r rune
	for _, c := range []byte(s[2:6]) {
		switch {
		case c >= '0' && c <= '9':
			c -= '0'
		case c >= 'a' && c <= 'f':
			c = c - 'a' + 10
		case c >= 'A' && c <= 'F':
			c = c - 'A' + 10
		default:
			return -1
		}
		r = r*16 + rune(c)
	}
	return r
}

// lokiLabelValue is a value as Loki keeps it as a label: every invalid UTF-8 byte and every U+FFFD becomes a space
// (recorded: TestLogfmtPairsMatchRecordedLoki).
func lokiLabelValue(v string) string {
	clean := true
	for i := 0; i < len(v) && clean; i++ {
		clean = v[i] < utf8.RuneSelf
	}
	if clean || (utf8.ValidString(v) && !strings.ContainsRune(v, utf8.RuneError)) {
		return v
	}
	var b strings.Builder
	b.Grow(len(v))
	for i := 0; i < len(v); {
		r, size := utf8.DecodeRuneInString(v[i:])
		if r == utf8.RuneError {
			b.WriteByte(' ')
		} else {
			b.WriteString(v[i : i+size])
		}
		i += size
	}
	return b.String()
}
