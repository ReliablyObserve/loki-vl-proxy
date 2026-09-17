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
		// A regexp escape is DOUBLED on the way into the literal: VictoriaLogs
		// unquotes before it compiles, so `\\d` is what reaches RE2 as `\d`.
		// A single backslash is a LogsQL parse error, not an escape.
		{"regex_backslash", `\d+`, `"\\d+"`},
		{"regex_word", `\w+\s*`, `"\\w+\\s*"`},
		// Quotes are still escaped.
		{"has_quote", `say "hi"`, `"say \"hi\""`},
		{"backslash_doubled", `a\b`, `"a\\b"`},
		// A character class carrying an escape, the shape a detected_level
		// filter produces.
		{"bracket_class", `[ \t]`, `"[ \\t]"`},
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
