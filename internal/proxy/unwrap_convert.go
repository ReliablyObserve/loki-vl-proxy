package proxy

import (
	"fmt"
	"math"
	"math/bits"
	"strconv"
	"strings"
	"time"
	"unicode"
)

// convertUnwrap converts an unwrapped label value the way an unwrap expression names it (LogQL docs, "Unwrapped
// range aggregations"): a plain unwrap reads a float (strconv.ParseFloat), duration() and duration_seconds() read
// a Go duration and answer its seconds (time.ParseDuration), bytes() reads a human byte size (parseHumanBytes).
// The value is read as it is, with no trimming. The error text is the one Loki writes into __error_details__ when
// the conversion rejects the value (recorded: TestUnwrapConversionMatchesRecordedLoki).
func convertUnwrap(value, conv string) (float64, error) {
	switch conv {
	case "duration", "duration_seconds":
		d, err := time.ParseDuration(value)
		if err != nil {
			return 0, err
		}
		return d.Seconds(), nil
	case "bytes":
		n, err := parseHumanBytes(value)
		if err != nil {
			return 0, err
		}
		return float64(n), nil
	}
	return strconv.ParseFloat(value, 64)
}

// convertUnwrapValue is convertUnwrap for the evaluators that only need to know whether the value makes a sample.
func convertUnwrapValue(value, conv string) (float64, bool) {
	v, err := convertUnwrap(value, conv)
	return v, err == nil
}

// humanBytesTable maps a lower-cased size suffix to its multiplier: SI units (k, kb, ... eb) are powers of 1000,
// IEC units (ki, kib, ... eib) powers of 1024, and a bare number or "b" is bytes.
var humanBytesTable = func() map[string]uint64 {
	t := map[string]uint64{"": 1, "b": 1}
	si, iec := uint64(1), uint64(1)
	for _, p := range []string{"k", "m", "g", "t", "p", "e"} {
		si *= 1000
		iec *= 1024
		t[p], t[p+"b"] = si, si
		t[p+"i"], t[p+"ib"] = iec, iec
	}
	return t
}()

// parseHumanBytes reads a human byte size as go-humanize v1.1.0's ParseBytes does, the version Loki 3.7.10 resolves
// for bytes() (test/oracle/loki/go.mod): the leading run of digits (any Unicode decimal digit), dots and commas
// is the number, commas removed, read with strconv.ParseFloat; the rest, trimmed and lower-cased, names the unit.
// A whole number (no dot) that strconv.ParseUint reads is multiplied exactly in uint64 and is "too large" when the
// product overflows; any other size is computed in float64 and truncated, and is "too large" from 2^64 up. Loki
// 3.7.9 resolved v1.0.1, which had only the float64 path: a whole size within float64's rounding of 2^64 (such as
// 18446744073709551615) was "too large" there. humanize counts the number's runes and cuts the string at that many
// bytes; that is kept, a non-ASCII digit shortens the number read. Checked on every input of
// TestUnwrapBytesPatternAgreesWithParseBytesOnGeneratedInputs against ParseBytes itself.
func parseHumanBytes(s string) (uint64, error) {
	runes, comma, dot := 0, false, false
scan:
	for _, r := range s {
		switch {
		case r == ',':
			comma = true
		case r == '.':
			dot = true
		case !unicode.IsDigit(r):
			break scan
		}
		runes++
	}
	num := s[:runes]
	if comma {
		num = strings.ReplaceAll(num, ",", "")
	}
	// A whole number uint64 holds is read exactly (and is also one ParseFloat reads, so the error order is
	// humanize's); any other number goes through ParseFloat.
	whole, wholeErr := uint64(0), error(nil)
	if !dot {
		whole, wholeErr = strconv.ParseUint(num, 10, 64)
	}
	var f float64
	if dot || wholeErr != nil {
		var err error
		if f, err = strconv.ParseFloat(num, 64); err != nil {
			return 0, err
		}
	}
	unit := strings.ToLower(strings.TrimSpace(s[runes:]))
	m, ok := humanBytesTable[unit]
	if !ok {
		return 0, fmt.Errorf("unhandled size name: %v", unit)
	}
	if !dot && wholeErr == nil {
		if hi, lo := bits.Mul64(whole, m); hi == 0 {
			return lo, nil
		}
		return 0, fmt.Errorf("too large: %v", s)
	}
	f *= float64(m)
	if f >= math.MaxUint64 {
		return 0, fmt.Errorf("too large: %v", s)
	}
	return uint64(f), nil
}
