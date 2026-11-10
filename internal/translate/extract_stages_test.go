package translate

import (
	"reflect"
	"testing"
)

// conformance: architecture-syntax-package-layering
func TestExtractStageBuilders(t *testing.T) {
	if got := ExtractRegexp(`(?P<a>\d+)`).String(); got != `| extract_regexp "(?P<a>\\d+)"` {
		t.Errorf("ExtractRegexp = %s", got)
	}
	if got := ExtractRegexp("a`b").String(); got != `| extract_regexp "a`+"`"+`b"` {
		t.Errorf("ExtractRegexp with a backquote = %s", got)
	}
	if p, ok := ExtractPattern(`<a> "q" <b>`); !ok || p.String() != `| extract "<a> \"q\" <b>"` {
		t.Errorf("ExtractPattern = %v, %v", p, ok)
	}
	for _, noop := range []string{"(.*)", ".*", "^.*$", "(?s).*", "(?s:.*)", " (.*) "} {
		if !NoopPattern(noop) {
			t.Errorf("%q is a wildcard-only pattern", noop)
		}
		if p, ok := ExtractPattern(noop); ok || p != nil {
			t.Errorf("ExtractPattern(%q) = %v, %v; want no pipe", noop, p, ok)
		}
	}
	for _, real := range []string{"", "<a>", "(.+)", "x.*"} {
		if NoopPattern(real) {
			t.Errorf("%q is not a wildcard-only pattern", real)
		}
	}
	if got := Decolorize().String(); got != "| decolorize" {
		t.Errorf("Decolorize = %s", got)
	}
}

// conformance: architecture-syntax-package-layering
func TestExtractStageNames(t *testing.T) {
	if got := RegexpCaptureNames(`(?P<a>\d+) (\w+) (?P<b>x)`); !reflect.DeepEqual(got, []string{"a", "b"}) {
		t.Errorf("RegexpCaptureNames = %v", got)
	}
	if got := RegexpCaptureNames(`(?P<a>`); got != nil {
		t.Errorf("a regexp Go does not compile defines no names, got %v", got)
	}
	if got := PatternCaptureNames(`<a> <_> <b1_c> <_x> <1a> <a.b> <A>`); !reflect.DeepEqual(got, []string{"a", "b1_c", "A"}) {
		t.Errorf("PatternCaptureNames = %v", got)
	}
}
