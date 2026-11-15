package logql

import (
	"reflect"
	"strings"
	"testing"
)

// stagesAhead counts the pipes and the ! line filters of the text after a selector, outside quoted strings, up to a
// range; a count that misses or adds a stage changes only the room the pipeline starts with.
//
// conformance: architecture-syntax-package-layering
func TestStagesAhead(t *testing.T) {
	for _, tc := range []struct {
		text string
		want int
	}{
		{``, 0},
		{` |= "x"`, 1},
		{` |= "x" != "y" !~ "z" !> "<_>"`, 4},
		{` | json | status="500"`, 2},
		{` |= "a|b" |~ "c|d"`, 2},
		{" |= `a|b` | regexp `(?P<a>x|y)`", 2},
		{` |= "a\"|b" | json`, 2},
		{` |= "x" [5m]) | unwrap y`, 1},
		{` |~ "[a-z]" | json`, 2},
		{` | a != "x"`, 2}, // a label filter's != counts once more: room for one more stage
		{` |= "unterminated`, 1},
		{strings.Repeat(` | json`, 40), maxStagesAhead},
	} {
		if got := stagesAhead(tc.text); got != tc.want {
			t.Errorf("stagesAhead(%q) = %d, want %d", tc.text, got, tc.want)
		}
	}
}

// A log query's head (the query, its selector and its first matcher) is one allocation, its pipeline is sized once,
// and a double-quoted string written the way strconv.Quote writes it is kept as a part of the query in a label
// filter's text: the parse of the usual log query allocates its nodes and little else. The parse answers the same
// tree as it did with the room, for any number of matchers and stages (FuzzPrintReparse holds the text).
//
// conformance: architecture-syntax-package-layering
func TestParseLogQueryAllocations(t *testing.T) {
	for _, tc := range []struct {
		query string
		max   float64
	}{
		{`{app="api"}`, 2},
		{`{app="api", namespace="prod"} |= "error" != "debug"`, 5},
		{`{app="api"} |= "error 17" | json | status="500"`, 7},
		{`{app="api"} | logfmt | status >= 500 | path = "/api/17"`, 9},
	} {
		if _, err := Parse(tc.query); err != nil {
			t.Fatalf("%q: %v", tc.query, err)
		}
		if got := testing.AllocsPerRun(50, func() { _, _ = Parse(tc.query) }); got > tc.max {
			t.Errorf("%q: %v allocations, want at most %v", tc.query, got, tc.max)
		}
	}
	// The room changes no tree: a selector of several matchers, a query of more stages than the room, strings
	// spelled as strconv.Quote spells them.
	for _, q := range []string{
		`{a="1", b="2", c="3", d="4", e="5"} |= "x"`, `{a="1"}` + strings.Repeat(` | json`, 20),
		`{a="b"} | json | x="a\"b"`, `{a="b"} | json | x="a|b" | y!="c"`,
	} {
		expr, err := Parse(q)
		if err != nil {
			t.Fatalf("%q: %v", q, err)
		}
		again, err := Parse(expr.String())
		if err != nil || !reflect.DeepEqual(expr, again) {
			t.Errorf("%q: printed %q parses to another tree (%v)", q, expr.String(), err)
		}
	}
	lq, err := ParseLogQuery(`{a="b"} | json | x="plain"`)
	if err != nil {
		t.Fatal(err)
	}
	if raw := lq.Pipeline[1].(*LabelFilterStage).Raw; raw != `x="plain"` {
		t.Errorf("label filter text %q", raw)
	}
	if lq, err := ParseLogQuery(`{a="b"} | json | x="a\x41"`); err != nil || lq.Pipeline[1].(*LabelFilterStage).Raw != `x="aA"` {
		t.Errorf("an escaped string is written the way strconv.Quote writes its value: %v %v", lq, err)
	}
}
