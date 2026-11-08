package logql

import (
	"reflect"
	"strings"
	"testing"
)

// The LogQL surface Loki v3.7.9 accepts for comments and offsets, with Loki's own answers: "" where Loki accepts the
// query, its error text where it rejects it, and the offset of each range aggregation Loki's tree holds (recorded
// with the test oracle under test/oracle/loki, `facts`; no Loki code). The proxy answers each one the same.
// conformance: semantics/comments-signed-compound-offsets, semantics/proxy-rejects-valid-logql
func TestCommentsAndOffsetsLikeLoki(t *testing.T) {
	cases := []struct {
		query   string
		lokiErr string
		offsets []int64
	}{
		{"{a=\"x\"} # a comment", "", nil},
		{"{a=\"x\"} |= \"e\" # trailing\n| json", "", nil},
		{"# leading\n{a=\"x\"}", "", nil},
		{"count_over_time({a=\"x\"}[3s] offset -1s)", "", []int64{-1000000000}},
		{"count_over_time({a=\"x\"}[3s] offset 1h2m20s)", "", []int64{3740000000000}},
		{"count_over_time({a=\"x\"}[3s] offset -1h30m)", "", []int64{-5400000000000}},
		{"sum(rate({a=\"x\"}[5m] offset -5m)) / sum(rate({a=\"x\"}[5m]))", "", []int64{-300000000000, 0}},
		{"{a=\"x\"} |= \"a#b\"", "", nil},
		{"count_over_time({a=\"x\"}[3s] offset - 1s)", "parse error at line 1, col 36: syntax error: unexpected -, expecting DURATION", nil},
		{"count_over_time({a=\"x\"}[3s] offset +1s)", "parse error at line 1, col 36: syntax error: unexpected +, expecting DURATION", nil},
		{"count_over_time({a=\"x\"}[3s] offset -0s)", "", []int64{0}},
		{"count_over_time({a=\"x\"}[3s] offset --1s)", "parse error at line 1, col 36: syntax error: unexpected -, expecting DURATION", nil},
		{"{a=\"x\" # c\n}", "", nil},
		{"# only a comment", "parse error at line 1, col 17: syntax error: unexpected $end", nil},
		{"{a=\"x\"} | line_format \"#x\"", "", nil},
		{"count_over_time({a=\"x\"}[1h2m])", "", []int64{0}},
		{"count_over_time({a=\"x\"}[5m30s])", "", []int64{0}},
		{"count_over_time({a=\"x\"}[3s] offset 1h-2m)", "parse error at line 0, col 38: syntax error: unexpected DURATION, expecting )", nil},
		{"count_over_time({a=\"x\"}[3s] offset 1h 2m)", "parse error at line 0, col 39: syntax error: unexpected DURATION, expecting )", nil},
		{"count_over_time({a=\"x\"}[3s] offset 1.5h)", "", []int64{5400000000000}},
		{"count_over_time({a=\"x\"}[3s] offset 1h2x)", "parse error at line 1, col 36: syntax error: unexpected NUMBER, expecting DURATION", nil},
		{"count_over_time({a=\"x\"}[3s] offset 2m1h)", "", []int64{3720000000000}},
		{"{a=\"x\"} |= \"x\" #c", "", nil},
		{"{a=\"x\"}#c", "", nil},
		{"sum(rate({a=\"x\"}[5m])) # by (a)\n by (b)", "", []int64{0}},
		{"{a=\"x\"} | status >= 4e2", "", nil},
		{"count_over_time({a=\"x\"}[3s] offset -1d)", "parse error at line 1, col 36: syntax error: unexpected -, expecting DURATION", nil},
		{"count_over_time({a=\"x\"}[1h2m3ms])", "", []int64{0}},
		{"count_over_time({a=\"x\"} [3s]offset -1s)", "", []int64{-1000000000}},
	}
	for _, c := range cases {
		expr, msg := ValidateLogQLExpr(c.query)
		if msg != c.lokiErr {
			t.Errorf("%q: %q, Loki %q", c.query, msg, c.lokiErr)
			continue
		}
		if msg != "" || c.offsets == nil {
			continue
		}
		var got []int64
		var walk func(Expr)
		walk = func(e Expr) {
			switch x := e.(type) {
			case *RangeAggregation:
				got = append(got, int64(x.OffsetDuration))
			case *VectorAggregation:
				walk(x.Inner)
			case *BinOpExpr:
				walk(x.Left)
				walk(x.Right)
			}
		}
		walk(expr)
		if !reflect.DeepEqual(got, c.offsets) {
			t.Errorf("%q: offsets %v, Loki %v", c.query, got, c.offsets)
		}
	}
}

// StripComments removes exactly the comments: the rest parses to the same tree, and a `#` inside a string stays.
func TestStripComments(t *testing.T) {
	cases := map[string]string{
		`{a="x"} # c`:                           `{a="x"} `,
		"{a=\"x\"} |= \"e\" # t\n| json":        "{a=\"x\"} |= \"e\" \n| json",
		"# lead\n{a=\"x\"}":                     "\n{a=\"x\"}",
		`{a="x"} |= "a#b"`:                      `{a="x"} |= "a#b"`,
		"{a=\"x\"} |~ `a#b` # c":                "{a=\"x\"} |~ `a#b` ",
		`{a="x"} | line_format "{{.a}}#{{.b}}"`: `{a="x"} | line_format "{{.a}}#{{.b}}"`,
		`{a="x" # c` + "\n}":                    "{a=\"x\" \n}",
		`{a="x"}`:                               `{a="x"}`,
		`{a="x} # unterminated`:                 `{a="x} # unterminated`,
	}
	for in, want := range cases {
		if got := StripComments(in); got != want {
			t.Errorf("StripComments(%q) = %q, want %q", in, got, want)
		}
	}
}

// StripComments is idempotent, only removes text that starts at a `#`, and never changes what a query means: the
// query is accepted exactly when the stripped one is, and both print the same tree.
func FuzzStripComments(f *testing.F) {
	for _, s := range []string{
		`{a="x"} # c`, "{a=\"x\"} |= \"e\" # t\n| json", "# lead\n{a=\"x\"}", `{a="x"} |= "a#b"`,
		"{a=\"x\"} |~ `a#b` # c", `{a="x"} | line_format "{{.a}}#{{.b}}"`, "{a=\"x\" # c\n}", `{a="x"} # "unterminated`,
		"sum(rate({a=\"x\"}[5m])) # by (a)\n by (b)", `count_over_time({a="x"}[3s] offset -1s) # x`, "#\n#\n{a=\"x\"}#",
	} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, q string) {
		if len(q) > 4096 {
			return
		}
		s := StripComments(q)
		if StripComments(s) != s {
			t.Fatalf("%q: stripping twice changes %q", q, s)
		}
		if len(s) > len(q) {
			t.Fatalf("%q: stripped text %q is longer", q, s)
		}
		// Every removed span is a comment: walking both texts, what q has more starts at a '#'.
		i, j := 0, 0
		for i < len(q) {
			if j < len(s) && q[i] == s[j] {
				i, j = i+1, j+1
				continue
			}
			if q[i] != '#' {
				t.Fatalf("%q: %q removed text that is not a comment at byte %d", q, s, i)
			}
			for i < len(q) && q[i] != '\n' {
				i++
			}
		}
		if strings.TrimSpace(s) == "*" {
			// The bare wildcard is a text tolerance for some clients (validateLogQL), read on the text the
			// handlers get, which the middleware strips first.
			return
		}
		e1, m1 := ValidateLogQLExpr(q)
		e2, m2 := ValidateLogQLExpr(s)
		if (m1 == "") != (m2 == "") {
			t.Fatalf("%q accepted %v, stripped %q accepted %v (%s / %s)", q, m1 == "", s, m2 == "", m1, m2)
		}
		if m1 == "" && e1 != nil && e2 != nil && e1.String() != e2.String() {
			t.Fatalf("%q: stripping changes the tree:\n%s\n%s", q, e1, e2)
		}
	})
}
