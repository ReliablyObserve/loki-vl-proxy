package logql

import (
	"strings"
	"testing"
)

// TestParse_UnbalancedOpaqueParens guards against a parser panic on opaque
// metric functions (label_replace, label_join, or any unknown function name)
// whose argument parentheses never close. consumeBalancedParens returned 0 when
// it hit EOF with parens still open, and the caller then sliced p.input[start:0]
// — panicking with "slice bounds out of range". Since ValidateLogQL runs on
// every proxy query, such an input must yield a parse error, never a panic.
func TestParse_UnbalancedOpaqueParens(t *testing.T) {
	inputs := []string{
		`(A00000(`,
		`A00000(`,
		`label_replace(`,
		`label_join(rate({app="x"}[5m])`,
		`sum(unknownfn(`,
		`(((`,
		`foo(bar(baz(`,
	}
	for _, in := range inputs {
		t.Run(in, func(t *testing.T) {
			// Parse must not panic and must report an error.
			_, err := Parse(in)
			if err == nil {
				t.Errorf("Parse(%q) = nil error, want a parse error", in)
			}
			// ValidateLogQL must not panic and must return a non-empty message.
			if msg := ValidateLogQL(in); msg == "" {
				t.Errorf("ValidateLogQL(%q) = %q, want a parse error", in, msg)
			} else if !strings.HasPrefix(msg, "parse error") {
				t.Errorf("ValidateLogQL(%q) = %q, want a Loki-style \"parse error ...\"", in, msg)
			}
		})
	}
}

// TestParse_BalancedOpaqueParens confirms the fix does not regress the
// balanced-parens opaque-function path, which must still capture the raw text.
func TestParse_BalancedOpaqueParens(t *testing.T) {
	valid := []string{
		`label_replace(rate({app="x"}[5m]), "dst", "$1", "src", "(.*)")`,
		`label_join(rate({app="x"}[5m]), "dst", ",", "a", "b")`,
	}
	for _, in := range valid {
		t.Run(in, func(t *testing.T) {
			if msg := ValidateLogQL(in); msg != "" {
				t.Errorf("ValidateLogQL(%q) = %q, want valid", in, msg)
			}
		})
	}
}
