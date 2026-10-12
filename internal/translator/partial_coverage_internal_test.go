package translator

import (
	"math"
	"testing"
)

func TestDurationSeconds(t *testing.T) {
	tests := []struct {
		in   string
		want float64
	}{
		{"", 0},
		{"5s", 5},
		{"1m", 60},
		{"1h", 3600},
		{"24h", 86400},
		{"100ms", 0.1},
		{"500us", 0.0005},
		{"100ns", 1e-7},
		{"1h30m", 5400},
		{"2h30m45s", 2*3600 + 30*60 + 45},
		{"3d", 3 * 86400}, // 'd' supported via fallback regex
		{"1w", 7 * 86400}, // 'w' supported via fallback regex
		{"garbage", 0},
		{"  5s  ", 5},
	}
	for _, tc := range tests {
		got := durationSeconds(tc.in)
		// Allow tiny float drift on fractions.
		if math.Abs(got-tc.want) > 1e-6 {
			t.Errorf("durationSeconds(%q) = %v, want %v", tc.in, got, tc.want)
		}
	}
}

func TestLooksLikeBareLabelMatcher(t *testing.T) {
	tests := []struct {
		in   string
		want bool
	}{
		{"", false},
		{`app="api-gateway"`, true},
		{`level!="info"`, true},
		{`app=~"^api"`, true},
		{`app=`, false},             // no quoted value
		{"app=api-gateway", false},  // unquoted
		{`{app="x"}`, false},        // wrapped in braces, not bare
		{`("app=x")`, false},        // starts with `(`
		{`123app="x"`, true},        // starts with digit, still an identifier
		{`  app="x"  `, true},       // whitespace tolerance
		{`http.method="GET"`, true}, // dotted name allowed
		{`http_method="GET"`, true}, // underscore allowed
		{`http-method="GET"`, true}, // dash allowed
	}
	for _, tc := range tests {
		got := looksLikeBareLabelMatcher(tc.in)
		if got != tc.want {
			t.Errorf("looksLikeBareLabelMatcher(%q) = %v, want %v", tc.in, got, tc.want)
		}
	}
}
