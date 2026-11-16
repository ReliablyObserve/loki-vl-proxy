package proxy

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
	"unicode"
)

// Loki v3.7.7 converts the unwrapped label with pkg/logql/log/metrics_extraction.go
// convertFloat, convertDuration (time.ParseDuration, in seconds) and
// convertBytes (humanize.ParseBytes). None of them trims the value: a value
// the conversion rejects makes a sample carrying __error__="SampleExtractionErr"
// (see unwrapConversionError), and a label that is absent or empty makes no
// sample at all. These functions are the same conversions, so every raw-sample
// path accepts exactly the inputs Loki does.

// convertUnwrap converts a raw label value with the LogQL unwrap conversion
// function conv ("" plain number, "duration", "bytes"). The error text is the
// one Loki puts in __error_details__.
func convertUnwrap(value, conv string) (float64, error) {
	switch conv {
	case "duration":
		d, err := time.ParseDuration(value)
		if err != nil {
			return 0, err
		}
		return d.Seconds(), nil
	case "bytes":
		b, err := parseHumanBytes(value)
		if err != nil {
			return 0, err
		}
		return float64(b), nil
	default:
		return strconv.ParseFloat(value, 64)
	}
}

// convertUnwrapValue reports whether value converts, for the callers that skip
// a sample Loki would mark with an error.
func convertUnwrapValue(value, conv string) (float64, bool) {
	f, err := convertUnwrap(value, conv)
	return f, err == nil
}

// humanBytesTable is go-humanize's bytesSizeTable, the multipliers
// humanize.ParseBytes accepts (case-insensitive unit names).
var humanBytesTable = map[string]uint64{
	"b": 1, "kib": 1 << 10, "kb": 1000, "mib": 1 << 20, "mb": 1000 * 1000,
	"gib": 1 << 30, "gb": 1000 * 1000 * 1000, "tib": 1 << 40, "tb": 1000 * 1000 * 1000 * 1000,
	"pib": 1 << 50, "pb": 1000 * 1000 * 1000 * 1000 * 1000,
	"eib": 1 << 60, "eb": 1000 * 1000 * 1000 * 1000 * 1000 * 1000,
	"": 1, "ki": 1 << 10, "k": 1000, "mi": 1 << 20, "m": 1000 * 1000,
	"gi": 1 << 30, "g": 1000 * 1000 * 1000, "ti": 1 << 40, "t": 1000 * 1000 * 1000 * 1000,
	"pi": 1 << 50, "p": 1000 * 1000 * 1000 * 1000 * 1000,
	"ei": 1 << 60, "e": 1000 * 1000 * 1000 * 1000 * 1000 * 1000,
}

// parseHumanBytes is humanize.ParseBytes (github.com/dustin/go-humanize
// v1.0.1, the function Loki's convertBytes calls), including its error text.
func parseHumanBytes(s string) (uint64, error) {
	lastDigit := 0
	hasComma := false
	for _, r := range s {
		if !unicode.IsDigit(r) && r != '.' && r != ',' {
			break
		}
		if r == ',' {
			hasComma = true
		}
		lastDigit++
	}
	num := s[:lastDigit]
	if hasComma {
		num = strings.ReplaceAll(num, ",", "")
	}
	f, err := strconv.ParseFloat(num, 64)
	if err != nil {
		return 0, err
	}
	extra := strings.ToLower(strings.TrimSpace(s[lastDigit:]))
	if m, ok := humanBytesTable[extra]; ok {
		f *= float64(m)
		if f >= math.MaxUint64 {
			return 0, fmt.Errorf("too large: %v", s)
		}
		return uint64(f), nil
	}
	return 0, fmt.Errorf("unhandled size name: %v", extra)
}
