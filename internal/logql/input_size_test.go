package logql

import (
	"strings"
	"testing"
)

// Parse rejects a query of MaxInputSize bytes or more with Loki's error before lexing it, as Loki's ParseExpr does,
// so no parse recurses deeper than a query under the limit can nest; the deepest such queries still parse.
// conformance: architecture-syntax-package-layering
func TestParseRejectsInputAtLokisMaxSize(t *testing.T) {
	wrap := func(open, close string, n int) string {
		return strings.Repeat(open, n) + `count_over_time({a="b"}[1m])` + strings.Repeat(close, n)
	}
	inner := len(`count_over_time({a="b"}[1m])`)
	under := (MaxInputSize - 1 - inner) / 2
	for _, tc := range []struct {
		name  string
		query string
		ok    bool
	}{
		{"parentheses just under the limit", wrap("(", ")", under), true},
		{"parentheses at the limit", wrap("(", ")", under) + strings.Repeat(" ", MaxInputSize-len(wrap("(", ")", under))), false},
		{"nested sums just under the limit", wrap("sum(", ")", (MaxInputSize-1-inner)/5), true},
		{"a million nested sums", wrap("sum(", ")", 1_000_000), false},
	} {
		_, err := Parse(tc.query)
		if tc.ok && err != nil {
			t.Errorf("%s (%d bytes): %v", tc.name, len(tc.query), err)
		}
		if !tc.ok {
			want := InputSizeError(tc.query)
			if want == "" {
				t.Fatalf("%s: %d bytes is under the limit", tc.name, len(tc.query))
			}
			if err == nil || err.Error() != want {
				t.Errorf("%s: got %v, want %q", tc.name, err, want)
			}
		}
	}
	if got := CanonicalizeLogRanges(strings.Repeat("(", MaxInputSize)); len(got) != MaxInputSize {
		t.Errorf("an oversized query is not returned unchanged by CanonicalizeLogRanges")
	}
}
