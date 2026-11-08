package logsql

import (
	"strings"
	"testing"
	"unicode/utf8"
)

// The `expr as alias` form of a math pipe is found by bytes, so a chunk with invalid UTF-8 (which strings.ToLower
// lengthens) neither panics nor reads the wrong alias, and every valid chunk reads as before.
// conformance: architecture-syntax-package-layering
func TestPipeMathAsAliasByBytes(t *testing.T) {
	for _, tc := range []struct {
		query, alias, expr string
		ok                 bool
	}{
		{"* | math a+1 as b", "b", "a+1", true},
		{"* | math a+1 AS b", "b", "a+1", true},
		{"* | math a+1 As b", "b", "a+1", true},
		{"* | math (x as y)+1 as z", "z", "(x as y)+1", true},
		{"0|mAth\xb5 As 0", "0", "\xb5", true},
		{"* | math \xff\xfe as \xb5", "\xb5", "\xff\xfe", true},
		{"* | math a+1 as", "", "", false},
		{"* | math as b", "", "", false},
	} {
		q, err := Parse(tc.query)
		if !tc.ok {
			if err == nil {
				if m, isMath := q.Pipes[len(q.Pipes)-1].(PipeMath); isMath && m.Alias != "" {
					t.Errorf("%q: read %+v, want an error", tc.query, m)
				}
			}
			continue
		}
		if err != nil {
			t.Fatalf("%q: %v", tc.query, err)
		}
		m, isMath := q.Pipes[len(q.Pipes)-1].(PipeMath)
		if !isMath || m.Alias != tc.alias || m.Expr != tc.expr {
			t.Errorf("%q: got %#v, want alias %q expr %q", tc.query, q.Pipes[len(q.Pipes)-1], tc.alias, tc.expr)
		}
	}
}

// On text strings.ToLower keeps the length of, the byte search finds what the lower-cased search found.
// conformance: architecture-syntax-package-layering
func TestLastSpacedAsMatchesLowerCasedSearch(t *testing.T) {
	parts := []string{" as ", " AS ", " aS ", "as", " a s ", "x", " ", "1+2", "é", "\t", "(", ")"}
	var walk func(prefix string, depth int)
	walk = func(prefix string, depth int) {
		if depth == 0 {
			return
		}
		for _, p := range parts {
			s := prefix + p
			if lower := strings.ToLower(s); utf8.ValidString(s) && len(lower) == len(s) {
				if got, want := lastSpacedAs(s), strings.LastIndex(lower, " as "); got != want {
					t.Fatalf("lastSpacedAs(%q) = %d, the lower-cased search %d", s, got, want)
				}
			}
			walk(s, depth-1)
		}
	}
	walk("", 4)
}

// A function filter's arguments are read without their `#` comments, as the scanner reads them, so the printed
// query parses back to the same filter; a # inside a quoted argument stays.
// conformance: architecture-syntax-package-layering
func TestFilterFuncArgsWithoutComments(t *testing.T) {
	for _, tc := range []struct{ query, args string }{
		{"re(0#\n)", "0"},
		{"re(0 # a comment\n)", "0"},
		{"re(\"a#b\")", "\"a#b\""},
		{"re(`a#b`)", "`a#b`"},
		{"re('a#b' # c\n)", "'a#b'"},
		{"re(\"a\\\"#b\")", "\"a\\\"#b\""},
		{"re(a#b\n)", "a"},
	} {
		q, err := Parse(tc.query)
		if err != nil {
			t.Fatalf("%q: %v", tc.query, err)
		}
		f, ok := q.Filter.(FilterFunc)
		if !ok || f.Args != tc.args {
			t.Errorf("%q: got %#v, want args %q", tc.query, q.Filter, tc.args)
			continue
		}
		again, err := Parse(q.String())
		if err != nil || again.String() != q.String() {
			t.Errorf("%q printed %q, which reads back as %v %v", tc.query, q.String(), again, err)
		}
	}
}

// values, uniq_values and json_values_topk keep a quoted field name as written, like the other stats functions, so
// a name that needs quotes prints back to the same query.
// conformance: architecture-syntax-package-layering
func TestStatsValuesKeepQuotedField(t *testing.T) {
	for _, tc := range []struct{ query, want string }{
		{`* | stats values("a b", 3)`, `* | stats values("a b", 3)`},
		{`* | stats uniq_values("x-y", 5)`, `* | stats uniq_values("x-y", 5)`},
		{`* | stats values("", 3)`, `* | stats values("", 3)`},
		{"0|vAlues(\"\"0)", `0 | stats values("", 0)`},
		{`* | stats values(level, 10)`, `* | stats values(level, 10)`},
	} {
		q, err := Parse(tc.query)
		if err != nil {
			t.Fatalf("%q: %v", tc.query, err)
		}
		if got := q.String(); got != tc.want {
			t.Errorf("%q printed %q, want %q", tc.query, got, tc.want)
		}
		if again, err := Parse(q.String()); err != nil || again.String() != q.String() {
			t.Errorf("%q printed %q, which reads back as %v %v", tc.query, q.String(), again, err)
		}
	}
}
