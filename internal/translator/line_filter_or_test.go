package translator

import "testing"

// conformance: semantics/line-filter-or-alternatives
func TestTranslateLineFilterOrAlternatives(t *testing.T) {
	cases := []struct{ logql, want string }{
		// |= and != are substring matches; one regexp is one pass over the line.
		{`{app="x"} |= "a" or "b"`, `app:="x" ~"a|b"`},
		{`{app="x"} |= "a" or "b" or "c.d"`, `app:="x" ~"a|b|c\\.d"`},
		{"{app=\"x\"} |= `a` or `b`", `app:="x" ~"a|b"`},
		{`{app="x"} != "a" or "b"`, `app:="x" NOT ~"a|b"`},
		// Regex and pattern alternatives stay grouped so each keeps its own anchors and flags.
		{`{app="x"} |~ "^a" or "(?i)b"`, `app:="x" ~"(?:^a)|(?:(?i)b)"`},
		{`{app="x"} !~ "a" or "b" or "c"`, `app:="x" NOT ~"(?:a)|(?:b)|(?:c)"`},
		{`{app="x"} |> "<_>a" or "<_>b"`, `app:="x" ~"(?:.*a)|(?:.*b)"`},
		{`{app="x"} !> "<_>a" or "<_>b"`, `app:="x" NOT ~"(?:.*a)|(?:.*b)"`},
		// ip(): plain text under |=, |~ and |> (Loki's or chain ignores the Op), an ip match under !=.
		{`{app="x"} |= "a" or ip("1.2.3.4")`, `app:="x" ~"a|1\\.2\\.3\\.4"`},
		{`{app="x"} |= ip("1.2.3.4") or "a"`, `app:="x" ~"1\\.2\\.3\\.4|a"`},
		{`{app="x"} != "a" or ip("1.2.3.4")`, `app:="x" NOT ~"(?:a)|(?:1\\.2\\.3\\.4)"`},
		{`{app="x"} != ip("1.2.3.4") or "a"`, `app:="x" NOT ~"(?:1\\.2\\.3\\.4)|(?:a)"`},
		// Stages around the chain, and chains in a metric query.
		{`{app="x"} |= "k" |= "a" or "b" != "z" or "y" |~ "q"`, `app:="x" ~"k" ~"a|b" NOT ~"z|y" ~"q"`},
		{`{app="x"} | json |= "a" or "b" | x="1"`, `app:="x" | unpack_json | filter ~"a|b" | filter x:="1"`},
		{`{app="x"} | json != "a" or "b"`, `app:="x" | unpack_json | filter NOT ~"a|b"`},
		{`{app="x"} | logfmt !~ "a" or "b"`, `app:="x" | unpack_logfmt | filter NOT ~"(?:a)|(?:b)"`},
		{`count_over_time({app="x"} |~ "a" or "b" | json [5m])`, `app:="x" ~"(?:a)|(?:b)" | unpack_json | stats count()`},
		// A single operand is unchanged.
		{`{app="x"} |= "a"`, `app:="x" ~"a"`},
		{`{app="x"} != "a"`, `app:="x" NOT ~"a"`},
		{`{app="x"} |~ "a|b"`, `app:="x" ~"a|b"`},
		{`{app="x"} | json != "a"`, `app:="x" | unpack_json | filter NOT ~"a"`},
	}
	for _, c := range cases {
		got, err := TranslateLogQL(c.logql)
		if err != nil {
			t.Errorf("%s: %v", c.logql, err)
			continue
		}
		if got != c.want {
			t.Errorf("%s:\n got %s\nwant %s", c.logql, got, c.want)
		}
	}
}

// Loki drops an alternative that matches every line from a positive chain
// (`|= "" or "b"` is `|= "b"`) and keeps it in a negated one.
// conformance: semantics/line-filter-or-alternatives
func TestTranslateLineFilterOrDropsMatchAllAlternative(t *testing.T) {
	cases := []struct{ logql, want string }{
		{`{app="x"} |= "" or "b"`, `app:="x" ~"b"`},
		{`{app="x"} |~ ".*" or "b|c"`, `app:="x" ~"b|c"`},
		{`{app="x"} |= "" or ""`, `app:="x" ~""`},
		{`{app="x"} != "" or "b"`, `app:="x" NOT ~"|b"`},
		// Loki's simplifier turns these into the match-all filter as well.
		{`{app="x"} |~ "red" or "(.*)"`, `app:="x" ~"red"`},
		{`{app="x"} |~ "red" or "()"`, `app:="x" ~"red"`},
		{`{app="x"} |~ "red" or "(?:.*)"`, `app:="x" ~"red"`},
		{`{app="x"} |~ "red" or ".*?"`, `app:="x" ~"red"`},
		{`{app="x"} |~ "red" or "(?s).*"`, `app:="x" ~"(?:red)|(?:(?s).*)"`},
	}
	for _, c := range cases {
		got, err := TranslateLogQL(c.logql)
		if err != nil || got != c.want {
			t.Errorf("%s: got %s (%v), want %s", c.logql, got, err, c.want)
		}
	}
}

// An ip(...) alternative ends Loki's orFilter, and the next `or` re-attaches to
// the head, dropping what came before (newOrLineFilterExpr: left.Or = right); a
// negated chain is a conjunction and keeps everything.
// conformance: semantics/line-filter-or-alternatives
func TestTranslateLineFilterOrAfterIPAlternative(t *testing.T) {
	cases := []struct{ logql, want string }{
		{`{app="x"} |= "a" or ip("1.2.3.4") or "b"`, `app:="x" ~"a|b"`},
		{`{app="x"} |= "a" or "b" or ip("1.2.3.4") or "c"`, `app:="x" ~"a|c"`},
		{`{app="x"} |= "a" or ip("1.2.3.4") or ip("5.6.7.8")`, `app:="x" ~"a|5\\.6\\.7\\.8"`},
		{`{app="x"} |= "a" or "b" or ip("1.2.3.4")`, `app:="x" ~"a|b|1\\.2\\.3\\.4"`},
		{`{app="x"} |= ip("1.2.3.4") or "b" or "c"`, `app:="x" ~"1\\.2\\.3\\.4|b|c"`},
		{`{app="x"} != "a" or ip("1.2.3.4") or "b"`, `app:="x" NOT ~"(?:a)|(?:1\\.2\\.3\\.4)|(?:b)"`},
		// Any white space after `or`, a raw string in ip(), and uppercase OR.
		{"{app=\"x\"} |= \"a\" or\t\"b\"", `app:="x" ~"a|b"`},
		{"{app=\"x\"} |= \"a\" or\n\"b\"", `app:="x" ~"a|b"`},
		{"{app=\"x\"} != \"a\" or ip(`1.2.3.4`)", `app:="x" NOT ~"(?:a)|(?:1\\.2\\.3\\.4)"`},
		{"{app=\"x\"} != ip(`1.2.3.4`) or \"a\"", `app:="x" NOT ~"(?:1\\.2\\.3\\.4)|(?:a)"`},
		{`{app="x"} |= "a" OR "b"`, `app:="x" ~"a|b"`},
	}
	for _, c := range cases {
		got, err := TranslateLogQL(c.logql)
		if err != nil || got != c.want {
			t.Errorf("%q: got %s (%v), want %s", c.logql, got, err, c.want)
		}
	}
}
