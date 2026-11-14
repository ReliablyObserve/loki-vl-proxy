package config

import "testing"

// A duration default is resolved however the Go source spaces the product, so the
// configuration reference prints 5m0s, never the Go expression.
func TestResolveDefaultDurationSpacing(t *testing.T) {
	for expr, want := range map[string]string{
		"5*time.Minute":        "5m0s",
		"5 * time.Minute":      "5m0s",
		" 6 * time.Hour ":      "6h0m0s",
		"20 *time.Second":      "20s",
		"3* time.Second":       "3s",
		"time.Second":          "1s",
		"2 * 60 * time.Second": "2m0s",
	} {
		if got := ResolveDefault(expr, nil); got != want {
			t.Errorf("ResolveDefault(%q) = %q, want %q", expr, got, want)
		}
	}
	for _, expr := range []string{"5 * time.Fortnight", "x * time.Minute", "* time.Minute"} {
		if got := ResolveDefault(expr, nil); got != expr {
			t.Errorf("ResolveDefault(%q) = %q, want the expression unchanged", expr, got)
		}
	}
}
