package translate

import (
	"regexp"
	"strings"
	"testing"
)

func TestPatternLineFilterHelpers(t *testing.T) {
	if got := planPatternLineFilterAlt(`foo<_>`, 0); got.re != `(?s)foo.+` || got.extract != "" {
		t.Fatalf("unexpected exact pattern translation %+v", got)
	}
	if got := planPatternLineFilterAlt(`literal.*value`, 0); got.re != `literal\.\*value$` || got.extract != "" || got.exact.String() != `_msg:~"literal\\.\\*value" _msg:~"(?s)literal\\.\\*value$" NOT _msg:~"(?s)literal\\.\\*value."` {
		t.Fatalf("unexpected literal pattern translation %+v", got)
	}
	if got := planPatternLineFilterAlt(``, 0); got.re != `^$` || got.extract != "" {
		t.Fatalf("unexpected empty pattern translation %+v", got)
	}
	seq := 0
	if got := strings.Join(PatternLineFilterParts([]string{`foo<_>bar`, `baz<_>`}, false, &seq), " "); got != `((_msg:~"foo" _msg:~"bar") or (_msg:~"baz")) ~"(?:(?s)foo.+bar$)|(?:(?s)baz.+)" | extract "foo<plain:__lvp_pf0_1>bar<plain:__lvp_pf0_r>" from _msg | filter (__lvp_pf0_1:* __lvp_pf0_r:"") or (_msg:~"(?s)baz.+") | delete __lvp_pf*` || seq != 1 {
		t.Fatalf("unexpected translated pattern line filter %q (seq %d)", got, seq)
	}
	if got := strings.Join(PatternLineFilterParts([]string{`foo<_>`}, true, &seq), " "); got != `NOT ~"(?s)foo.+"` || seq != 1 {
		t.Fatalf("unexpected translated negative pattern line filter %q (seq %d)", got, seq)
	}
}

// The regexp VictoriaLogs evaluates keeps Loki's verdict per line: the empty
// pattern accepts only the empty line (pattern.Matcher.Test).
// conformance: semantics/pattern-line-filter
func TestEmptyPatternRegexpMatchesOnlyEmptyLine(t *testing.T) {
	re := regexp.MustCompile(planPatternLineFilterAlt("", 0).re)
	for line, want := range map[string]bool{
		"":                 true,
		" ":                false,
		"a":                false,
		"GET /x 200":       false,
		"first\nsecond":    false,
		"\n":               false,
		"level=info msg=x": false,
	} {
		if got := re.MatchString(line); got != want {
			t.Errorf("empty pattern vs %q: matched=%v, Loki accepts=%v", line, got, want)
		}
	}
}
