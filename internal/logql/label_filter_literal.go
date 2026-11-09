package logql

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"
)

// This file reads the number, duration and bytes literals of a label filter the way Loki's lexer does, which
// scans a number with the rules of Go's text/scanner (prefixes 0x 0o 0b and a leading 0, '_' separators, decimal
// and hexadecimal floats, with that scanner's error messages), then lets the characters that follow turn it into
// a duration or a bytes size when the whole text is one. Every rule here is checked against Loki's own answers
// (label_filter_number_literal_loki_test.go).

// numberLiteral is a literal found at a position of a label filter.
type numberLiteral struct {
	end  int    // the byte after the literal
	name string // NUMBER, DURATION or BYTES
	// err is the lexer error Loki reports for the literal ("hexadecimal literal has no digits"), empty when it is
	// a valid token.
	err string
}

func isDecimalDigit(c int) bool { return '0' <= c && c <= '9' }

func isHexDigit(c int) bool {
	return isDecimalDigit(c) || 'a' <= (c|0x20) && (c|0x20) <= 'f'
}

// at is the byte of src at i, -1 past its end.
func at(src string, i int) int {
	if i < len(src) {
		return int(src[i])
	}
	return -1
}

func lowerByte(c int) int { return c | 0x20 }

// literalName is the name of a number literal in the scanner's messages.
func literalName(prefix byte) string {
	switch prefix {
	case 'x':
		return "hexadecimal literal"
	case 'o', '0':
		return "octal literal"
	case 'b':
		return "binary literal"
	}
	return "decimal literal"
}

// noDigitsMessage is "<literal name> has no digits".
func noDigitsMessage(prefix byte) string {
	switch prefix {
	case 'x':
		return "hexadecimal literal has no digits"
	case 'o', '0':
		return "octal literal has no digits"
	case 'b':
		return "binary literal has no digits"
	}
	return "decimal literal has no digits"
}

// scanDigits reads the digits and '_' separators of a number part in base from i. It returns the end, whether a
// digit (bit 0) and a '_' (bit 1) were read, and the first digit that does not belong to the base.
func scanDigits(src string, i, base int, invalid *int) (end, digsep int) {
	for {
		c := at(src, i)
		switch {
		case c == '_':
			digsep |= 2
		case base <= 10 && isDecimalDigit(c):
			if c >= '0'+base && *invalid == 0 {
				*invalid = c
			}
			digsep |= 1
		case base > 10 && isHexDigit(c):
			digsep |= 1
		default:
			return i, digsep
		}
		i++
	}
}

// invalidSeparator reports whether the '_' of a number literal text do not all sit between two digits (or between
// a base prefix and a digit).
func invalidSeparator(x string) bool {
	x1 := byte(' ')
	d := byte('.') // the previous character: '_', '0' for a digit, '.' for anything else
	i := 0
	if len(x) >= 2 && x[0] == '0' {
		if x1 = x[1] | 0x20; x1 == 'x' || x1 == 'o' || x1 == 'b' {
			d, i = '0', 2
		}
	}
	for ; i < len(x); i++ {
		p := d
		d = x[i]
		switch {
		case d == '_':
			if p != '0' {
				return true
			}
		case isDecimalDigit(int(d)) || x1 == 'x' && isHexDigit(int(d)):
			d = '0'
		default:
			if p == '_' {
				return true
			}
			d = '.'
		}
	}
	return d == '_'
}

// scanNumber reads the number that starts at src[i], a digit or a '.' followed by a digit, and returns its end and
// the first error of the scan.
func scanNumber(src string, i int) (end int, msg string) {
	fail := func(m string) {
		if msg == "" {
			msg = m
		}
	}
	pos, base, prefix := i, 10, byte(0)
	digsep, invalid := 0, 0
	seenDot, float := false, false
	if src[pos] == '.' {
		pos++
		seenDot = true
	} else {
		if src[pos] == '0' {
			pos++
			switch lowerByte(at(src, pos)) {
			case 'x':
				pos, base, prefix = pos+1, 16, 'x'
			case 'o':
				pos, base, prefix = pos+1, 8, 'o'
			case 'b':
				pos, base, prefix = pos+1, 2, 'b'
			default:
				base, prefix, digsep = 8, '0', 1
			}
		}
		var ds int
		pos, ds = scanDigits(src, pos, base, &invalid)
		digsep |= ds
		if at(src, pos) == '.' {
			pos++
			seenDot = true
		}
	}
	if seenDot {
		float = true
		if prefix == 'o' || prefix == 'b' {
			fail("invalid radix point in " + literalName(prefix))
		}
		var ds int
		pos, ds = scanDigits(src, pos, base, &invalid)
		digsep |= ds
	}
	if digsep&1 == 0 {
		fail(noDigitsMessage(prefix))
	}
	if c := at(src, pos); lowerByte(c) == 'e' || lowerByte(c) == 'p' {
		switch {
		case lowerByte(c) == 'e' && prefix != 0 && prefix != '0':
			fail(fmt.Sprintf("%q exponent requires decimal mantissa", rune(c)))
		case lowerByte(c) == 'p' && prefix != 'x':
			fail(fmt.Sprintf("%q exponent requires hexadecimal mantissa", rune(c)))
		}
		pos++
		float = true
		if c = at(src, pos); c == '+' || c == '-' {
			pos++
		}
		var ds int
		pos, ds = scanDigits(src, pos, 10, nil)
		digsep |= ds
		if ds&1 == 0 {
			fail("exponent has no digits")
		}
	} else if prefix == 'x' && float {
		fail("hexadecimal mantissa requires a 'p' exponent")
	}
	if !float && invalid != 0 {
		fail(fmt.Sprintf("invalid digit %q in %s", rune(invalid), literalName(prefix)))
	}
	if digsep&2 != 0 && invalidSeparator(src[i:pos]) {
		fail("'_' must separate successive digits")
	}
	return pos, msg
}

// durationRunEnd is the end of the run of digits, '.' and duration letters (n u s m h d w y, and the micro sign
// µ, not the Greek mu) that starts at i.
func durationRunEnd(src string, i int) int {
	for i < len(src) {
		if c := src[i]; c < utf8.RuneSelf {
			if (c < '0' || c > '9') && c != '.' && strings.IndexByte("nusmhdwy", c) < 0 {
				return i
			}
			i++
			continue
		}
		r, size := utf8.DecodeRuneInString(src[i:])
		if !unicode.IsNumber(r) && r != 'µ' {
			return i
		}
		i += size
	}
	return i
}

// promUnits are the units of the calendar durations Loki adds to Go's: "1y2w3d" (integers, in this order).
var promUnits = []struct {
	name string
	mult time.Duration
}{{"y", 365 * 24 * time.Hour}, {"w", 7 * 24 * time.Hour}, {"d", 24 * time.Hour}, {"h", time.Hour}, {"m", time.Minute}, {"s", time.Second}, {"ms", time.Millisecond}}

// parsePromDuration reads a duration written as whole numbers of y, w, d, h, m, s and ms, each unit once and
// from the largest to the smallest.
func parsePromDuration(s string) bool {
	if s == "" {
		return false
	}
	var total time.Duration
	last := -1
	for s != "" {
		n := 0
		for n < len(s) && isDecimalDigit(int(s[n])) {
			n++
		}
		if n == 0 {
			return false
		}
		v, err := strconv.ParseUint(s[:n], 10, 63)
		if err != nil {
			return false
		}
		s = s[n:]
		u := 0
		for u < len(s) && !isDecimalDigit(int(s[u])) {
			u++
		}
		unit := -1
		for k, pu := range promUnits {
			if pu.name == s[:u] {
				unit = k
			}
		}
		if unit <= last {
			return false
		}
		last, s = unit, s[u:]
		mult := promUnits[unit].mult
		if v > uint64(math.MaxInt64)/uint64(mult) {
			return false
		}
		total += time.Duration(v) * mult
		if total < 0 {
			return false
		}
	}
	return true
}

// isLokiDuration reports whether Loki reads text as a duration: Go's syntax, or whole calendar units.
func isLokiDuration(text string) bool {
	if _, err := time.ParseDuration(text); err == nil {
		return true
	}
	return parsePromDuration(text)
}

// bytesUnits are the units of a bytes size by their lower case name, as Loki's size parser takes them.
var bytesUnits = map[string]float64{
	"": 1, "b": 1,
	"k": 1e3, "kb": 1e3, "m": 1e6, "mb": 1e6, "g": 1e9, "gb": 1e9, "t": 1e12, "tb": 1e12, "p": 1e15, "pb": 1e15, "e": 1e18, "eb": 1e18,
	"ki": 1 << 10, "kib": 1 << 10, "mi": 1 << 20, "mib": 1 << 20, "gi": 1 << 30, "gib": 1 << 30, "ti": 1 << 40, "tib": 1 << 40,
	"pi": 1 << 50, "pib": 1 << 50, "ei": 1 << 60, "eib": 1 << 60,
}

// bytesRunEnd is the end of the run of numbers, '.' and the letters B i k K M G T P that starts at i: the
// characters Loki's lexer reads after a number to see whether they make a bytes size.
func bytesRunEnd(src string, i int) int {
	for i < len(src) {
		if c := src[i]; c < utf8.RuneSelf {
			if (c < '0' || c > '9') && c != '.' && strings.IndexByte("BikKMGTP", c) < 0 {
				break
			}
			i++
			continue
		}
		r, size := utf8.DecodeRuneInString(src[i:])
		if !unicode.IsNumber(r) {
			break
		}
		i += size
	}
	return i
}

// isLokiBytes reports whether text is a bytes size the way Loki's size parser reads it: a decimal number and a
// unit of bytesUnits in any case, with a size that fits 64 bits.
func isLokiBytes(text string) bool {
	last := 0
	for last < len(text) {
		r, size := utf8.DecodeRuneInString(text[last:])
		if !unicode.IsDigit(r) && r != '.' && r != ',' {
			break
		}
		last += size
	}
	unit := strings.TrimSpace(text[last:])
	if len(unit) > 3 {
		return false
	}
	var lower [3]byte
	for i := 0; i < len(unit); i++ {
		c := unit[i]
		if 'A' <= c && c <= 'Z' {
			c += 'a' - 'A'
		}
		lower[i] = c
	}
	mult, ok := bytesUnits[string(lower[:len(unit)])]
	if !ok {
		return false
	}
	v, err := strconv.ParseFloat(strings.ReplaceAll(text[:last], ",", ""), 64)
	return err == nil && v*mult < math.MaxUint64
}

// scanNumberLiteral reads the literal that starts at src[i], a digit or a '.' followed by a digit: the number, or
// the duration or bytes size it begins.
func scanNumberLiteral(src string, i int) numberLiteral {
	// The common case: decimal digits, no leading zero, and nothing that could continue a literal after them.
	if j := i; src[i] > '0' && src[i] <= '9' {
		for j++; j < len(src) && src[j] >= '0' && src[j] <= '9'; j++ {
		}
		if j == len(src) {
			return numberLiteral{end: j, name: "NUMBER"}
		}
		c := src[j]
		if letter := c|0x20 >= 'a' && c|0x20 <= 'z'; !letter && c != '.' && c != '_' && c != 0 && c < utf8.RuneSelf {
			return numberLiteral{end: j, name: "NUMBER"}
		}
	}
	numEnd, msg := scanNumber(src, i)
	lit := numberLiteral{end: numEnd, name: "NUMBER", err: msg}
	if msg != "" {
		return lit
	}
	if end := durationRunEnd(src, numEnd); end > numEnd && isLokiDuration(src[i:end]) {
		lit.end, lit.name = end, "DURATION"
	} else {
		if end = bytesRunEnd(src, numEnd); end > numEnd && isLokiBytes(src[i:end]) {
			lit.end, lit.name = end, "BYTES"
		}
	}
	return lit
}

// scanNegativeDuration reads a duration with a minus sign ("-5m") that starts at src[i], a '-': Loki's lexer
// makes one token of it when the characters that follow are a duration, and a minus sign otherwise. It returns
// the end, or 0.
func scanNegativeDuration(src string, i int) int {
	end := durationRunEnd(src, i+1)
	if end > i+1 {
		if _, err := time.ParseDuration(src[i:end]); err == nil {
			return end
		}
	}
	return 0
}
