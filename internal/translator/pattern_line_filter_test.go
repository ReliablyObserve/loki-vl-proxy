package translator

import (
	"regexp"
	"testing"
)

// Loki's pattern line filter with an empty pattern is a matcher with no
// expression (pkg/logql/log/pattern/pattern.go ParseLineFilter returns an empty
// Matcher) and Matcher.Test accepts only an empty line then, so `|> ""` keeps
// the empty lines and `!> ""` drops them; a non-empty line is kept by `!> ""`
// and dropped by `|> ""`.
// conformance: semantics/pattern-line-filter
func TestTranslateEmptyPatternLineFilter(t *testing.T) {
	cases := []struct{ logql, want string }{
		{`{namespace="prod"} |> ""`, `namespace:="prod" ~"^$"`},
		{`{namespace="prod"} !> ""`, `namespace:="prod" NOT ~"^$"`},
		{"{namespace=\"prod\"} |> ``", `namespace:="prod" ~"^$"`},
		{"{namespace=\"prod\"} !> ``", `namespace:="prod" NOT ~"^$"`},
		// Combined with other filters and stages.
		{`{namespace="prod"} |= "GET" |> "" |= "x"`, `namespace:="prod" ~"GET" ~"^$" ~"x"`},
		// A line filter written right after the pattern (no pipe) is a stage of its own.
		{`{namespace="prod"} |> "" != "x"`, `namespace:="prod" ~"^$" NOT ~"x"`},
		{`{namespace="prod"} |> "" !~ "x"`, `namespace:="prod" ~"^$" NOT ~"x"`},
		{`{namespace="prod"} !> "" != "x"`, `namespace:="prod" NOT ~"^$" NOT ~"x"`},
		{`{namespace="prod"} !> "" !~ "x" !> "a <_>"`, `namespace:="prod" NOT ~"^$" NOT ~"x" NOT ~"a .*"`},
		{`{namespace="prod"} |> "a <_>" or "" != "x" or "y"`, `namespace:="prod" ~"(?:a .*)|(?:^$)" NOT ~"x|y"`},
		{`{namespace="prod"} | json |> "" != "x"`, `namespace:="prod" | unpack_json | filter ~"^$" NOT ~"x"`},
		{`{namespace="prod"} |> "a <_>" != "x"`, `namespace:="prod" ~"a .*" NOT ~"x"`},
		{`{namespace="prod"} |> "a" OR "b"`, `namespace:="prod" ~"(?:a)|(?:b)"`},
		{`{namespace="prod"} !> "" |> "GET <_>"`, `namespace:="prod" NOT ~"^$" ~"GET .*"`},
		{`{namespace="prod"} | json !> ""`, `namespace:="prod" | unpack_json | filter NOT ~"^$"`},
		{`{namespace="prod"} | json |> "" | level="info"`, `namespace:="prod" | unpack_json | filter ~"^$" | filter level:="info"`},
		// An empty alternative is an ordinary pattern: it never matches a non-empty line, and
		// (unlike an empty |= operand) it is not dropped from the chain.
		{`{namespace="prod"} |> "" or "a <_>"`, `namespace:="prod" ~"(?:^$)|(?:a .*)"`},
		{`{namespace="prod"} !> "a <_>" or ""`, `namespace:="prod" NOT ~"(?:a .*)|(?:^$)"`},
		// Metric queries push the same filter down to the stats.
		{`count_over_time({namespace="prod"} |> "" [5m])`, `namespace:="prod" ~"^$" | stats count()`},
		{`count_over_time({namespace="prod"} |> "" != "x" [5m])`, `namespace:="prod" ~"^$" NOT ~"x" | stats count()`},
		{`count_over_time({namespace="prod"} !> "" [5m])`, `namespace:="prod" NOT ~"^$" | stats count()`},
		// Non-empty patterns are unchanged.
		{`{namespace="prod"} |> "<_> GET <_>"`, `namespace:="prod" ~".* GET .*"`},
		{`{namespace="prod"} !> "foo <_>"`, `namespace:="prod" NOT ~"foo .*"`},
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

// The regexp VictoriaLogs evaluates keeps Loki's verdict per line: the empty
// pattern accepts only the empty line (pattern.Matcher.Test).
// conformance: semantics/pattern-line-filter
func TestEmptyPatternRegexpMatchesOnlyEmptyLine(t *testing.T) {
	re := regexp.MustCompile(patternFilterValueToRegex(""))
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
