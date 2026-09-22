package logsql_test

import (
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
)

func TestQuoteValue(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{"empty", "", `""`},
		{"simple", "simple", `"simple"`},
		{"has_quotes", `has "quotes"`, `"has \"quotes\""`},
		{"has_backslash", `has\backslash`, `"has\\backslash"`},
		{"has_both", `has " and \`, `"has \" and \\"`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := logsql.QuoteValue(tc.input)
			if got != tc.want {
				t.Errorf("QuoteValue(%q) = %q, want %q", tc.input, got, tc.want)
			}
		})
	}
}

func TestQuotePattern(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{"empty", "", `""`},
		{"simple", "simple", `"simple"`},
		// Backslash preserved (regex escape semantics), only " is escaped.
		{"regex_backslash", `\d+`, `"\\d+"`},
		{"regex_word", `\w+\s*`, `"\\w+\\s*"`},
		// Quotes are still escaped.
		{"has_quote", `say "hi"`, `"say \"hi\""`},
		// VictoriaLogs unquotes with Go's rules, so a pattern's backslashes must
		// survive as escapes: it rejects "a\b" and accepts "a\\b" (verified on
		// v1.52.0).
		{"backslash_escaped_for_go_unquoting", `a\b`, `"a\\b"`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := logsql.QuotePattern(tc.input)
			if got != tc.want {
				t.Errorf("QuotePattern(%q) = %q, want %q", tc.input, got, tc.want)
			}
		})
	}
}
