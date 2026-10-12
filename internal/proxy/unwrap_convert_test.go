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

// Loki converts duration_seconds() with convertDuration, as duration()
// (pkg/logql/syntax/extractor.go:63-71): one unwrap expression, parsed by every
// route of the proxy, selects the "duration" conversion, and the conversion
// accepts and rejects the values time.ParseDuration does.
// conformance: semantics/unwrap-duration-seconds, loki_api_v1_query_range, loki_api_v1_query
func TestUnwrapDurationSecondsIsConvertedAsDuration(t *testing.T) {
	for _, tc := range []struct{ expr, field, conv string }{
		{"duration_seconds(okdur)", "okdur", "duration"},
		{"duration(okdur)", "okdur", "duration"},
		{"bytes(okdur)", "okdur", "bytes"},
		{"okdur", "okdur", ""},
		{"duration_seconds( okdur )", "okdur", "duration"},
		{"duration_seconds(`okdur`)", "okdur", "duration"},
	} {
		field, conv := parseUnwrapExpression(tc.expr)
		if field != tc.field || conv != tc.conv {
			t.Errorf("parseUnwrapExpression(%q) = (%q, %q), want (%q, %q)", tc.expr, field, conv, tc.field, tc.conv)
		}
	}
	for _, tc := range []struct{ query, field, conv string }{
		{`{a="b"} | logfmt | unwrap duration_seconds(okdur) [5m]`, "okdur", "duration"},
		{`{a="b"} | logfmt | unwrap duration(okdur) [5m]`, "okdur", "duration"},
		{`{a="b"} | logfmt | unwrap bytes(okdur) [5m]`, "okdur", "bytes"},
		{`{a="b"} | logfmt | unwrap okdur [5m]`, "okdur", ""},
	} {
		field, conv := extractBareParserUnwrapExpr(tc.query)
		if field != tc.field || conv != tc.conv {
			t.Errorf("extractBareParserUnwrapExpr(%q) = (%q, %q), want (%q, %q)", tc.query, field, conv, tc.field, tc.conv)
		}
	}

	// The edge table of a duration value: the same answer through both names,
	// and Go's time.ParseDuration text for a rejected one, as Loki puts it in
	// __error_details__.
	for _, tc := range []struct {
		input   string
		want    float64
		errText string
	}{
		{"250ms", 0.25, ""},
		{"1.5s", 1.5, ""},
		{"1h", 3600, ""},
		{"-1s", -1, ""},
		{"5.s", 5, ""},
		{".5s", 0.5, ""},
		{"+2m", 120, ""},
		{"0", 0, ""},
		{"abc", 0, `time: invalid duration "abc"`},
		{"1d", 0, `time: unknown unit "d" in duration "1d"`},
		{"42", 0, `time: missing unit in duration "42"`},
		{"", 0, `time: invalid duration ""`},
	} {
		_, conv := parseUnwrapExpression("duration_seconds(v)")
		got, err := convertUnwrap(tc.input, conv)
		if tc.errText != "" {
			if err == nil || err.Error() != tc.errText {
				t.Errorf("duration_seconds %q: error = %v, want %q", tc.input, err, tc.errText)
			}
			continue
		}
		if err != nil || math.Abs(got-tc.want) > 1e-9 {
			t.Errorf("duration_seconds %q = (%v, %v), want %v", tc.input, got, err, tc.want)
		}
	}
}
