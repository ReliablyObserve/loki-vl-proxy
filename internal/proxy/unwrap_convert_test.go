package proxy

import (
	"math"
	"strings"
	"testing"
)

// TestConvertUnwrapMatchesLoki pins the conversions to Loki v3.7.7's
// convertFloat / convertDuration / convertBytes: no trimming, Go duration
// syntax (no "d", a bare number only as "0"), humanize byte units, and the
// error text Loki puts in __error_details__.
func TestConvertUnwrapMatchesLoki(t *testing.T) {
	tests := []struct {
		conv, input string
		want        float64
		errText     string // "" when the value converts
	}{
		{"", "15", 15, ""},
		{"", "-1.5", -1.5, ""},
		{"", "1e3", 1000, ""},
		{"", "+5", 5, ""},
		{"", ".5", 0.5, ""},
		{"", " 5", 0, `strconv.ParseFloat: parsing " 5": invalid syntax`},
		{"", "86282s", 0, `strconv.ParseFloat: parsing "86282s": invalid syntax`},
		{"", "abc", 0, `strconv.ParseFloat: parsing "abc": invalid syntax`},
		{"duration", "100ms", 0.1, ""},
		{"duration", "1h30m", 5400, ""},
		{"duration", "86282s", 86282, ""},
		{"duration", "100us", 0.0001, ""},
		{"duration", "0", 0, ""},
		{"duration", "42", 0, `time: missing unit in duration "42"`},
		{"duration", "1d", 0, `time: unknown unit "d" in duration "1d"`},
		{"duration", "", 0, `time: invalid duration ""`},
		{"duration", "abc", 0, `time: invalid duration "abc"`},
		{"bytes", "1024", 1024, ""},
		{"bytes", "1KB", 1000, ""},
		{"bytes", "1kib", 1024, ""},
		{"bytes", "1.5KiB", 1536, ""},
		{"bytes", "2048B", 2048, ""},
		{"bytes", "1,000", 1000, ""},
		{"bytes", "10 MB", 1e7, ""},
		{"bytes", "1.5B", 1, ""},
		{"bytes", "86282s", 0, "unhandled size name: s"},
		{"bytes", "abc", 0, `strconv.ParseFloat: parsing "": invalid syntax`},
		{"bytes", "", 0, `strconv.ParseFloat: parsing "": invalid syntax`},
	}
	for _, tt := range tests {
		t.Run(tt.conv+"/"+tt.input, func(t *testing.T) {
			got, err := convertUnwrap(tt.input, tt.conv)
			if tt.errText != "" {
				if err == nil || err.Error() != tt.errText {
					t.Fatalf("convertUnwrap(%q, %q) error = %v, want %q", tt.input, tt.conv, err, tt.errText)
				}
				if _, ok := convertUnwrapValue(tt.input, tt.conv); ok {
					t.Fatalf("convertUnwrapValue(%q, %q) accepted a value Loki rejects", tt.input, tt.conv)
				}
				return
			}
			if err != nil {
				t.Fatalf("convertUnwrap(%q, %q) error = %v", tt.input, tt.conv, err)
			}
			if math.Abs(got-tt.want) > 1e-9*math.Max(1, math.Abs(tt.want)) {
				t.Errorf("convertUnwrap(%q, %q) = %v, want %v", tt.input, tt.conv, got, tt.want)
			}
		})
	}
	if _, err := convertUnwrap("1"+strings.Repeat("0", 30), "bytes"); err == nil || !strings.HasPrefix(err.Error(), "too large:") {
		t.Errorf("bytes beyond uint64 must fail with humanize's text, got %v", err)
	}
}
