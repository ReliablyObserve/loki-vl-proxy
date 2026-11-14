package translate

import (
	"regexp"
	"strings"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
)

// lineKept evaluates the line filter node the way VictoriaLogs does for the regexp and negation nodes the builders
// write, over one line.
func lineKept(t *testing.T, f logsql.FilterExpr, line string) bool {
	t.Helper()
	switch n := f.(type) {
	case nil:
		return true
	case logsql.Regexp:
		return regexp.MustCompile(n.Pattern).MatchString(line)
	case logsql.NotExpr:
		return !lineKept(t, n.Expr, line)
	case logsql.AllOf:
		for _, e := range n {
			if !lineKept(t, e, line) {
				return false
			}
		}
		return true
	}
	t.Fatalf("unexpected filter node %T (%s)", f, f)
	return false
}

// Loki matches a line that holds any alternative for |=, |~ and |>, and none of them for != and !~ (the negated chain
// is a conjunction). A text that is not UTF-8 is in no stored line.
// conformance: semantics/line-filter-or-alternatives
func TestLineFilterStage(t *testing.T) {
	alt := func(v string, ip bool) logql.LineFilterAlt { return logql.LineFilterAlt{Value: v, IP: ip} }
	for _, c := range []struct {
		name  string
		stage logql.LineFilterStage
		want  string // "" for a filter that keeps every line
		ok    bool
		keep  []string
		drop  []string
	}{
		{"contains", logql.LineFilterStage{Op: logql.LineFilterContains, Value: "a.b"}, `~"a\\.b"`, true, []string{"xa.by"}, []string{"axb"}},
		{"excludes", logql.LineFilterStage{Op: logql.LineFilterExcludes, Value: "a.b"}, `NOT ~"a\\.b"`, true, []string{"axb"}, []string{"a.b"}},
		{"regexp", logql.LineFilterStage{Op: logql.LineFilterMatchRe, Value: "a.b"}, `~"a.b"`, true, []string{"axb"}, []string{"ab"}},
		{"exclude regexp", logql.LineFilterStage{Op: logql.LineFilterExcludeRe, Value: "a.b"}, `NOT ~"a.b"`, true, []string{"ab"}, []string{"axb"}},
		{"contains chain", logql.LineFilterStage{Op: logql.LineFilterContains, Value: "x", Or: []logql.LineFilterAlt{alt("y", false)}}, `~"x|y"`, true, []string{"xx", "yy"}, []string{"zz"}},
		{"excludes chain is a conjunction", logql.LineFilterStage{Op: logql.LineFilterExcludes, Value: "x", Or: []logql.LineFilterAlt{alt("y", false)}}, `NOT ~"x|y"`, true, []string{"zz"}, []string{"xx", "yy"}},
		{"regexp chain", logql.LineFilterStage{Op: logql.LineFilterMatchRe, Value: "x", Or: []logql.LineFilterAlt{alt("y+", false)}}, `~"(?:x)|(?:y+)"`, true, []string{"x", "yy"}, []string{"z"}},
		{"an ip() alternative of a positive chain ends it: the next or drops the ones before", logql.LineFilterStage{Op: logql.LineFilterContains, Value: "x", Or: []logql.LineFilterAlt{alt("10.0.0.1", true), alt("z", false)}}, `~"x|z"`, true, []string{"x", "z"}, []string{"10.0.0.1"}},
		{"a positive ip() head is matched as text in a chain", logql.LineFilterStage{Op: logql.LineFilterContains, Value: "10.0.0.1", IP: true, Or: []logql.LineFilterAlt{alt("z", false)}}, `~"10\\.0\\.0\\.1|z"`, true, []string{"10.0.0.1", "z"}, []string{"10x0y0w1"}},
		{"a negated chain keeps ip() semantics", logql.LineFilterStage{Op: logql.LineFilterExcludes, Value: "x", Or: []logql.LineFilterAlt{alt("10.0.0.1", true)}}, "", true, []string{"y", "10.0.0.12", "a 10.0.0.2 b"}, []string{"x", "a 10.0.0.1 b"}},
		{"an alternative that matches every line is dropped from a positive chain", logql.LineFilterStage{Op: logql.LineFilterContains, Value: "x", Or: []logql.LineFilterAlt{alt("", false)}}, `~"x"`, true, []string{"x"}, []string{"y"}},
		{"a != of the empty text keeps no line", logql.LineFilterStage{Op: logql.LineFilterExcludes, Value: "x", Or: []logql.LineFilterAlt{alt("", false)}}, `NOT ~"x|"`, true, nil, []string{"x", "y", ""}},
		{"a text that is not UTF-8 is held by no line", logql.LineFilterStage{Op: logql.LineFilterContains, Value: "\xff"}, `_stream_id:in()`, true, nil, nil},
		{"!= of a text no line holds keeps every line", logql.LineFilterStage{Op: logql.LineFilterExcludes, Value: "\xff"}, "", true, nil, nil},
		{"an unheld alternative is dropped from the chain", logql.LineFilterStage{Op: logql.LineFilterContains, Value: "ok", Or: []logql.LineFilterAlt{alt("\xff", false)}}, `~"ok"`, true, []string{"ok"}, []string{"no"}},
		{"a pattern filter is the translator's", logql.LineFilterStage{Op: logql.LineFilterContainsPat, Value: "x"}, "", false, nil, nil},
		{"an excluded pattern filter is the translator's", logql.LineFilterStage{Op: logql.LineFilterExcludePat, Value: "x"}, "", false, nil, nil},
	} {
		t.Run(c.name, func(t *testing.T) {
			st := c.stage
			f, ok := LineFilter(&st)
			if ok != c.ok {
				t.Fatalf("ok = %v, want %v", ok, c.ok)
			}
			if !ok {
				return
			}
			got := ""
			if f != nil {
				got = f.String()
			}
			if c.want != "" && got != c.want {
				t.Errorf("filter = %s, want %s", got, c.want)
			}
			if c.want == "" && c.keep == nil && c.drop == nil && f != nil {
				t.Errorf("filter = %s, want none (every line kept)", got)
			}
			if f != nil && !strings.Contains(got, "_stream_id") {
				mustParseLogsQL(t, "* "+got)
			}
			for _, line := range c.keep {
				if !lineKept(t, f, line) {
					t.Errorf("line %q must be kept by %s", line, got)
				}
			}
			for _, line := range c.drop {
				if f != nil && !strings.Contains(got, "_stream_id") && lineKept(t, f, line) {
					t.Errorf("line %q must be dropped by %s", line, got)
				}
			}
		})
	}
}

// conformance: semantics/line-filter-or-alternatives
func TestOrLineFilterPattern(t *testing.T) {
	text := func(s string) OrAlternative { return OrAlternative{Text: s} }
	ip := func(s string) OrAlternative { return OrAlternative{Text: s, IP: true} }
	for _, c := range []struct {
		name          string
		negate, regex bool
		alts          []OrAlternative
		want          string
	}{
		{"plain texts are quoted", false, false, []OrAlternative{text("a.b"), text("c")}, `a\.b|c`},
		{"regexps are grouped", false, true, []OrAlternative{text("a"), text("b|c")}, `(?:a)|(?:b|c)`},
		{"one regexp stays as it is", false, true, []OrAlternative{text("a|b")}, `a|b`},
		{"a regexp that keeps every line is dropped", false, true, []OrAlternative{text("a"), text(".*")}, `a`},
		{"alone it is the only one left", false, true, []OrAlternative{text(".*")}, ``},
		{"ip() ends the orFilter of a positive chain", false, false, []OrAlternative{text("a"), ip("10.0.0.1"), text("b")}, `a|b`},
		{"an ip() last alternative stays", false, false, []OrAlternative{text("a"), ip("10.0.0.1")}, `a|10\.0\.0\.1`},
		{"a negated chain keeps ip() semantics", true, false, []OrAlternative{text("c"), ip("10.0.0.1")}, ``},
	} {
		got := OrLineFilterPattern(c.negate, c.regex, c.alts)
		if c.want != "" && got != c.want {
			t.Errorf("%s: %q, want %q", c.name, got, c.want)
		}
		if c.negate {
			re := regexp.MustCompile(got)
			if !re.MatchString("c") || !re.MatchString("a 10.0.0.1 b") || re.MatchString("a 10.0.0.12 b") {
				t.Errorf("%s: negated chain %q must hold the text c and the address 10.0.0.1 only", c.name, got)
			}
		}
	}
	if got, want := OrLineFilter(false, false, []OrAlternative{text("a.b"), text("c")}), `~"a\\.b|c"`; got != want {
		t.Errorf("OrLineFilter = %s, want %s", got, want)
	}
	if got, want := OrLineFilter(true, true, []OrAlternative{text("a"), text("c|d")}), `NOT ~"(?:a)|(?:c|d)"`; got != want {
		t.Errorf("negated OrLineFilter = %s, want %s", got, want)
	}
}

// Loki turns an empty text, and a regexp that simplifies to .* or the empty match, into its match-all line filter.
// conformance: semantics/line-filter-or-alternatives
func TestLokiMatchesEverything(t *testing.T) {
	for _, c := range []struct {
		text  string
		regex bool
		want  bool
	}{
		{"", false, true}, {"x", false, false}, {".*", false, false},
		{".*", true, true}, {"", true, true}, {"(.*)", true, true},
		{"a", true, false}, {".+", true, false}, {"(", true, false},
	} {
		if got := LokiMatchesEverything(c.text, c.regex); got != c.want {
			t.Errorf("LokiMatchesEverything(%q, %v) = %v, want %v", c.text, c.regex, got, c.want)
		}
	}
}

// A line holds an IPv4 address of the pattern when the address stands alone or between non-address characters. The
// IPv6 lines of the grid are a known difference from Loki (semantics/ip-filter-precision) and are not asserted here.
// conformance: semantics/ip-line-filter-ipv4-like-loki
func TestIPLineFilter(t *testing.T) {
	for _, c := range []struct {
		pattern string
		keep    []string
		drop    []string
	}{
		{"10.0.0.0/24", []string{"x 10.0.0.12 y", "10.0.0.255", "client=10.0.0.1:443"}, []string{"x 110.0.0.12 y", "10.0.1.1", "10.0.0.256", "no address"}},
		{"10.0.0.1-10.0.0.5", []string{"x 10.0.0.3 y"}, []string{"10.0.0.9", "10.0.0.31"}},
		{"10.0.0.1", []string{"a 10.0.0.1 b"}, []string{"a 10.0.0.12 b", "a 10.0.0.10 b"}},
	} {
		in := IPLineFilter(c.pattern, false)
		out := IPLineFilter(c.pattern, true)
		mustParseLogsQL(t, "* "+in.String())
		mustParseLogsQL(t, "* "+out.String())
		if _, ok := out.(logsql.NotExpr); !ok {
			t.Errorf("%s: != must negate the filter, got %T", c.pattern, out)
		}
		for _, line := range c.keep {
			if !lineKept(t, in, line) || lineKept(t, out, line) {
				t.Errorf("%s: line %q must be kept by |= and dropped by !=", c.pattern, line)
			}
		}
		for _, line := range c.drop {
			if lineKept(t, in, line) || !lineKept(t, out, line) {
				t.Errorf("%s: line %q must be dropped by |= and kept by !=", c.pattern, line)
			}
		}
	}
	// An IPv4 pattern is checked on its fixed octets first, which VictoriaLogs looks up in its bloom filters.
	if all, ok := IPLineFilter("10.0.0.0/24", false).(logsql.AllOf); !ok || len(all) != 2 || all[0].String() != `~"10\\.0\\.0\\."` {
		t.Errorf("IPv4 prefilter: %v", IPLineFilter("10.0.0.0/24", false))
	}
}

// conformance: semantics/line-filter-invalid-utf8
func TestTextLineFilter(t *testing.T) {
	bad := OrAlternative{Text: "\xff"}
	good := OrAlternative{Text: "a"}
	if f := TextLineFilter(false, []OrAlternative{bad}); f != MatchNoRows() || f.String() != "_stream_id:in()" {
		t.Errorf("|= of an unheld text must match no row, got %v", f)
	}
	if f := TextLineFilter(true, []OrAlternative{bad}); f != nil {
		t.Errorf("!= of an unheld text keeps every line, got %v", f)
	}
	if f := TextLineFilter(false, []OrAlternative{bad, good}); f.String() != `~"a"` {
		t.Errorf("the held alternatives remain: %v", f)
	}
	if f := TextLineFilter(true, []OrAlternative{good}); f.String() != `NOT ~"a"` {
		t.Errorf("negated: %v", f)
	}
	if TextsHeld([]OrAlternative{good, bad}) || !TextsHeld([]OrAlternative{good, {Text: "\xff", IP: true}}) || !TextsHeld(nil) {
		t.Error("TextsHeld: an ip() alternative is held, a non-UTF-8 text is not")
	}
}
