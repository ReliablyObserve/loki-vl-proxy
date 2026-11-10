package translate

import (
	"reflect"
	"regexp"
	"strings"
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

// RegexpCaptureNames parses the regexp with regexp/syntax where the translator compiles it: the names and the
// verdict are the same for every regexp, valid or not.
//
// conformance: architecture-syntax-package-layering
func TestRegexpCaptureNamesMatchCompile(t *testing.T) {
	for _, pattern := range []string{
		``, `a`, `(a)`, `(?P<a>x)`, `(?P<a>x)(?P<b>y)`, `(?P<a>x)|(?P<b>y)`, `(?P<a>(?P<b>x))`, `(?:(?P<a>x))`, `(?P<a>x)(?P<a>y)`,
		`(?P<a_b>\d+)(\w+)`, `(?<a>x)`, `(?P<a>`, `(?P<a>x`, `(?P<1a>x)`, `(?P<a-b>x)`, `(?P<>x)`, `[`, `a{1001}`, `a{2,1}`, `(a{1000}){1000}`,
		`(?i)(?P<é>x)`, `(?s:.)(?P<z>.)`, `\p{Foo}`, `(?P<a>x)\1`, `(?=x)`, `x**`, `(((((((((((((((((((((((((((((((((((((((((((((((((((a)))))))))))))))))))))))))))))))))))))))))))))))))))`,
		"(?P<a>x)\n(?P<b>y)", `(?U)(?P<l>.*)`, `^(?P<ip>\S+) \S+ (?P<user>\S+) \[(?P<ts>[^\]]+)\]`, strings.Repeat(`(?P<a>x)`, 3), `(?P<` + strings.Repeat("n", 300) + `>x)`,
	} {
		var want []string
		if re, err := regexp.Compile(pattern); err == nil {
			for _, n := range re.SubexpNames() {
				if n != "" {
					want = append(want, n)
				}
			}
		}
		if got := RegexpCaptureNames(pattern); !reflect.DeepEqual(got, want) {
			t.Errorf("RegexpCaptureNames(%q) = %v, regexp.Compile gives %v", pattern, got, want)
		}
	}
}
