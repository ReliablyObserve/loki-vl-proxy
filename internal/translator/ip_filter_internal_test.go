package translator

import (
	"strings"
	"testing"
)

func TestExtractIPFilterArg(t *testing.T) {
	tests := []struct {
		in      string
		wantArg string
		wantOK  bool
	}{
		{`ip("10.0.0.0/8")`, "10.0.0.0/8", true},
		{`ip("192.168.1.1")`, "192.168.1.1", true},
		{`ip("2001:db8::/32")`, "2001:db8::/32", true},
		{`  ip( "10.0.0.0" )`, "10.0.0.0", true},
		{`notip("10.0.0.0")`, "", false},
		{`ip(10.0.0.0)`, "", false}, // missing quotes
		{`ip("unclosed`, "", false}, // missing closing quote
		{`ip("")`, "", true},        // empty arg is valid
	}
	for _, tc := range tests {
		t.Run(tc.in, func(t *testing.T) {
			arg, _, ok := extractIPFilterArg(tc.in)
			if ok != tc.wantOK {
				t.Errorf("extractIPFilterArg(%q): ok=%v, want %v", tc.in, ok, tc.wantOK)
			}
			if ok && arg != tc.wantArg {
				t.Errorf("extractIPFilterArg(%q): arg=%q, want %q", tc.in, arg, tc.wantArg)
			}
		})
	}
}

func TestExtractIPFilterArg_RestIsParsed(t *testing.T) {
	// After ip("X") VL queries often continue. Verify the rest is returned.
	_, rest, ok := extractIPFilterArg(`ip("10.0.0.1") |~ ".*"`)
	if !ok {
		t.Fatal("expected ok=true")
	}
	if !strings.Contains(rest, `|~`) {
		t.Fatalf("expected rest to contain remaining query, got %q", rest)
	}
}
