package lower

import (
	"errors"
	"strconv"
	"strings"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan"
)

// A range or vector aggregation over drop, keep, decolorize, line_format, label_format and the regexp and pattern
// parsers lowers to the query the string translator writes, under every profile: a metric row's drop deletes every
// bare name it lists, a keep that names detected_level without level deletes level, and the stages the log query
// lowering writes are written as they are.
//
// conformance: architecture-syntax-package-layering
func TestPlanLowersMetricStages(t *testing.T) {
	logs := []string{
		`{app="a"} | drop x`, `{app="a"} | drop x, level`, `{app="a"} | drop level, detected_level`, `{app="a"} | drop x="y", z`,
		`{app="a"} | json | drop a, b | status>=500`, `{app="a"} | logfmt | keep a, b`, `{app="a"} | keep a | json`, `{app="a"} | keep detected_level`,
		`{app="a"} | json | keep detected_level, a`, `{app="a"} | json | keep detected_level, level`, `{app="a"} | keep level`,
		`{app="a"} | decolorize`, `{app="a"} | decolorize | json`, `{app="a"} | line_format "{{.a}} x"`, `{app="a"} | json | line_format "{{.a}}"`,
		`{app="a"} | label_format b=a`, `{app="a"} | json | label_format b="{{.a}}-x"`, `{app="a"} | json | label_format b=a | drop a`,
		"{app=\"a\"} | regexp `(?P<a>\\d+) (?P<b>\\w+)`", "{app=\"a\"} | regexp `(?P<a>\\d+)` | a=\"1\"", `{app="a"} | pattern "<a> <b>"`,
		`{app="a"} | pattern "<a>:<b>" | b="x" | drop a`, `{app="a"} | pattern "<_> <a>" | line_format "{{.a}}"`,
		`{app="a", env="e"} |= "x" | regexp "(?P<a>x)" |~ "y"`, `{service_name="s"} | json | drop service_name`,
	}
	funcs := []string{"count_over_time", "bytes_over_time", "absent_over_time", "rate", "bytes_rate"}
	clauses := []string{``, `by (a)`, `without (a)`, `by (level)`, `by (detected_level)`, `by (service_name)`, `by (a_b)`}
	lowered := 0
	for _, prof := range rangeProfiles() {
		check := func(q string) {
			t.Helper()
			got, legacy, same, delegated, ok := rangeAggLowering(t, q, prof)
			if !ok || delegated {
				return // the cases the lowering leaves out are named in TestPlanDelegatesMetricStages
			}
			lowered++
			if !same {
				t.Errorf("%s: %s:\n lowered %s\n  string %s", prof.name, q, got, legacy)
			}
		}
		for _, in := range logs {
			for _, f := range funcs {
				check(f + "(" + in + " [5m])")
				for _, op := range []string{"sum", "count", "min", "max", "avg"} {
					for _, c := range clauses {
						check(op + " " + c + " (" + f + "(" + in + " [5m]))")
					}
				}
			}
			check("sum_over_time(" + in + " | unwrap n [5m])")
			check("sum by (a) (avg_over_time(" + in + " | unwrap n [5m]))")
			check("rate(" + in + " | unwrap n [5m])")
		}
	}
	if lowered < 5000 {
		t.Fatalf("only %d queries lowered", lowered)
	}
}

// The part of a metric row the lowering leaves to the string translator, named: the series a rate groups by and the
// one value per series of count, min, max and avg when the translator reads a trailing keep, and the quoted text it
// reads as syntax in a stage whose value is not in the printed query.
//
// conformance: architecture-syntax-package-layering
func TestPlanDelegatesMetricStages(t *testing.T) {
	for q, construct := range map[string]string{
		// a trailing keep decides the series; one followed by a parser, label_format, drop or unwrap does not
		`count_over_time({app="a"} | line_format "{{upper .a}}" | keep a [5m])`:              "RangeAgg LogSelect line format function",
		`sum(count_over_time({app="a"} | label_format a="x <y>" [5m]))`:                      "VectorAgg LogSelect label format angle bracket",
		`rate({app="a"} | drop a="x", b | keep k="v" | drop c="| json" [5m])`:                "RangeAgg quoted clause",
		`rate({app="a"} | keep a="| logfmt" [5m])`:                                           "RangeAgg quoted clause",
		`rate({app="a"} | label_format b="| pattern x" [5m])`:                                "RangeAgg quoted clause",
		`bytes_rate({app="a"} | line_format "| regexp x" [5m])`:                              "RangeAgg quoted clause",
		`rate({app="a"} | x="| unpack" [5m])`:                                                "RangeAgg quoted clause",
		`count_over_time({app="a"} | keep a="x without (y)" [5m])`:                           "RangeAgg quoted clause",
		`sum(count_over_time({app="a"} | drop a="a bool b" [5m]))`:                           "VectorAgg quoted clause",
		`sum by (a) (rate({app="a"} | label_format b="x ||| y" [5m]))`:                       "VectorAgg quoted clause",
		`sum without (a) (count_over_time({app="a"} | drop a="  " [5m]))`:                    "VectorAgg quoted clause",
		`sum(rate({app="a"} | regexp "(?P<a>x)" |= "| unpack_json" [5m]))`:                   "VectorAgg quoted clause",
		`count_over_time({app="a"} | pattern "<a>" | label_format b="| unpack_logfmt" [5m])`: "RangeAgg quoted clause",
		`sum without (a) (count_over_time({app="a"} | regexp "(?P<a>x)" [5m]))`:              "",
		`sum(count_over_time({app="a"} | json | drop a | regexp "(?P<b>x)" [5m]))`:           "",
	} {
		p := rangeAggPlan(q)
		if p == nil {
			t.Fatalf("%s: does not parse", q)
		}
		_, err := Plan(p, Profile{})
		var nl *NotLoweredError
		switch {
		case construct == "" && err != nil:
			t.Errorf("%s: got %v, want lowered", q, err)
		case construct != "" && (!errors.As(err, &nl) || nl.Construct != construct):
			t.Errorf("%s: got %v, want not lowered: %s", q, err, construct)
		}
	}
}

// Behind a regexp or pattern parser alone the string translator resolves no label it groups by or unwraps: it
// does that when its text of the query holds a json or logfmt unpack, which the stages here write none of.
//
// conformance: architecture-syntax-package-layering
func TestPlanRegexpParserResolvesNoLabels(t *testing.T) {
	for _, q := range []string{
		"sum by (a_b) (count_over_time({app=\"a\"} | regexp `(?P<a_b>\\d+)` [5m]))",
		`sum by (a_b) (rate({app="a"} | pattern "<a_b> <_>" [5m]))`,
		`sum_over_time({app="a"} | pattern "<a_b>" | unwrap a_b [5m])`,
		`sum(rate({app="a"} | pattern "<n>" | unwrap n [5m]))`,
		`rate({app="a"} | regexp "(?P<n>.+)" | unwrap n [5m])`,
	} {
		for _, prof := range rangeProfiles() {
			got, legacy, same, delegated, ok := rangeAggLowering(t, q, prof)
			if !ok || delegated {
				t.Errorf("%s: %s: not lowered (ok %v)", prof.name, q, ok)
			} else if !same {
				t.Errorf("%s: %s:\n lowered %s\n  string %s", prof.name, q, got, legacy)
			}
		}
	}
}

func TestMetricDropAndKeepPipes(t *testing.T) {
	drop := func(labels []string, matchers ...logql.DropMatcher) *logql.DropStage {
		return &logql.DropStage{Labels: labels, Matchers: matchers}
	}
	keep := func(labels []string, matchers ...logql.DropMatcher) *logql.KeepStage {
		return &logql.KeepStage{Labels: labels, Matchers: matchers}
	}
	text := func(ps []logsql.Pipe) string {
		parts := make([]string, len(ps))
		for i, p := range ps {
			parts[i] = p.String()
		}
		return strings.Join(parts, " ")
	}
	for _, c := range []struct {
		name string
		got  func() ([]logsql.Pipe, error)
		want string
	}{
		{"drop names", func() ([]logsql.Pipe, error) { return metricDropPipes(drop([]string{"a", "b"})) }, "| delete a, b"},
		{"drop keeps no level source", func() ([]logsql.Pipe, error) { return metricDropPipes(drop([]string{"level", "detected_level"})) }, "| delete level, detected_level"},
		{"drop matchers", func() ([]logsql.Pipe, error) {
			return metricDropPipes(drop(nil, logql.DropMatcher{Name: "a", Op: "=", Value: "x"}))
		}, ""},
		{"drop names and matchers", func() ([]logsql.Pipe, error) {
			return metricDropPipes(drop([]string{"a"}, logql.DropMatcher{Name: "b", Op: "=", Value: "x"}))
		}, "| delete a"},
		{"keep names", func() ([]logsql.Pipe, error) { return metricKeepPipes(keep([]string{"a", "b"}), nil, Profile{}) }, "| fields _time, _msg, _stream, a, b"},
		{"keep detected_level", func() ([]logsql.Pipe, error) {
			return metricKeepPipes(keep([]string{"detected_level"}), nil, Profile{})
		}, "| fields _time, _msg, _stream, detected_level, level | delete level"},
		{"keep detected_level and level", func() ([]logsql.Pipe, error) {
			return metricKeepPipes(keep([]string{"detected_level", "level"}), nil, Profile{})
		}, "| fields _time, _msg, _stream, detected_level, level"},
		{"keep detected_level matcher", func() ([]logsql.Pipe, error) {
			return metricKeepPipes(keep(nil, logql.DropMatcher{Name: "detected_level", Op: "=", Value: "x"}), nil, Profile{})
		}, "| fields _time, _msg, _stream, detected_level, level | delete level"},
	} {
		ps, err := c.got()
		if err != nil {
			t.Errorf("%s: %v", c.name, err)
			continue
		}
		if got := text(ps); got != c.want {
			t.Errorf("%s: got %q, want %q", c.name, got, c.want)
		}
	}
	// A matcher value the translator's splitting reads wrongly stays with it, as in a log query.
	if _, err := metricDropPipes(drop(nil, logql.DropMatcher{Name: "a", Op: "=", Value: `x"y`})); err == nil {
		t.Error("drop matcher with a quote lowered")
	}
	if _, err := metricKeepPipes(keep(nil, logql.DropMatcher{Name: "a", Op: "=", Value: "x=y"}), nil, Profile{}); err == nil {
		t.Error("keep matcher with an operator character lowered")
	}
}

// selectOf is the log query of count_over_time(q [5m]).
func selectOf(t *testing.T, q string) *queryplan.LogSelect {
	t.Helper()
	p := rangeAggPlan(`count_over_time(` + q + ` [5m])`)
	if p == nil {
		t.Fatalf("%s: does not parse", q)
	}
	return p.Root.(*queryplan.RangeAgg).In
}

// The string translator takes a keep to decide the series when no parser, label_format, drop or unwrap follows the
// last one, and it has no matcher and only plain names (trailingKeepGrouping): the labels it leaves, less the error
// labels. "-" says the keep decides nothing.
func TestTrailingKeepLabels(t *testing.T) {
	for q, want := range map[string]string{
		`{a="b"}`:                                         "-",
		`{a="b"} | keep x`:                                "x",
		`{a="b"} | keep x="y"`:                            "-",
		`{a="b"} | keep x, y="z"`:                         "-",
		`{a="b"} | keep x="y" | json | keep z`:            "-",
		`{a="b"} | keep x | keep y`:                       "y",
		`{a="b"} | keep x |= "z" | y="1"`:                 "x",
		`{a="b"} | keep x | line_format "q" | decolorize`: "x",
		`{a="b"} | json | keep x, y`:                      "x,y",
		`{a="b"} | keep x | json`:                         "-",
		`{a="b"} | keep x | label_format y=x`:             "-",
		`{a="b"} | keep x | drop y`:                       "-",
		`{a="b"} | keep x | unwrap n`:                     "-",
		`{a="b"} | keep x | regexp "(?P<q>.)"`:            "-",
		`{a="b"} | keep x | pattern "<q>"`:                "-",
		`{a="b"} | keep x | json | keep y`:                "y",
		`{a="b"} | drop x | keep y`:                       "y",
		`{a="b"} | keep x | unwrap n | keep y`:            "y",
		`{a="b"} | keep __error__, x, __error_details__`:  "x",
		`{a="b"} | keep __error__`:                        "",
		`{a="b"} | line_format "x"`:                       "-",
	} {
		kept, ok := trailingKeepLabels(selectOf(t, q))
		got := "-"
		if ok {
			got = strings.Join(kept, ",")
		}
		if got != want {
			t.Errorf("%s: got %q, want %q", q, got, want)
		}
	}
}

// Every text the client wrote in the log query is read, since the translator reads the query text: the stages that
// print no pipe (a drop of matchers) and the values the printed query rewrites hold it too.
func TestAnyClientString(t *testing.T) {
	for _, q := range []string{
		`{a="NEEDLE"}`, `{NEEDLE="b"}`, `{a="b"} |= "NEEDLE"`, `{a="b"} |= "x" or "NEEDLE"`, `{a="b"} | json NEEDLE="x"`, `{a="b"} | json x="NEEDLE"`,
		`{a="b"} | regexp "(?P<NEEDLE>x)"`, `{a="b"} | pattern "<NEEDLE>"`, `{a="b"} | x="NEEDLE"`, `{a="b"} | NEEDLE="x" or y="z"`,
		`{a="b"} | drop NEEDLE`, `{a="b"} | drop x="NEEDLE"`, `{a="b"} | keep NEEDLE`, `{a="b"} | keep x="NEEDLE"`, `{a="b"} | line_format "NEEDLE"`,
		`{a="b"} | label_format x="NEEDLE"`, `{a="b"} | label_format NEEDLE=y`, `{a="b"} | label_format x=NEEDLE`,
	} {
		if !anyClientString(selectOf(t, q), func(s string) bool { return strings.Contains(s, "NEEDLE") }) {
			t.Errorf("%s: text not read", q)
		}
	}
	if anyClientString(selectOf(t, `{a="b"} | json | drop x | line_format "y"`), func(s string) bool { return strings.Contains(s, "NEEDLE") }) {
		t.Error("text read that is not there")
	}
}

// FuzzMetricStagesLowering holds the lowering of range and vector aggregations over drop, keep, decolorize,
// line_format, label_format, regexp and pattern to the string translator on any selector values, line filters,
// label filters and stage values, functions, clauses, ranges and offsets under every profile: both write the same
// query, or the lowering leaves the query to the translator. Values that spell a parser, an unwrap, a without clause
// or a boolean modifier are kept in (the translator reads them as syntax and the lowering must leave those queries
// to it); unbalanced parentheses and the words of a range or a vector matching stay out.
//
// conformance: architecture-syntax-package-layering
func FuzzMetricStagesLowering(f *testing.F) {
	f.Add("a", "x", "level", "error", uint8(0), uint8(0), uint8(0), uint8(0))
	f.Add("service_name", "é\n", "status", ".*", uint8(7), uint8(3), uint8(5), uint8(9))
	f.Add("k8s.pod", "{{.x}}", "a-b", "| json", uint8(203), uint8(9), uint8(11), uint8(70))
	f.Add("pod", "z", "detected_level", "without (x)", uint8(4), uint8(14), uint8(2), uint8(33))
	f.Add("a", "| unwrap n", "k", "a bool b", uint8(3), uint8(21), uint8(1), uint8(130))
	f.Fuzz(func(t *testing.T, sel, line, name, val string, fn, shape, rng, wrap uint8) {
		for _, s := range []string{sel, line, name, val} {
			if fuzzValueOutOfScope(s) {
				return
			}
		}
		ident := func(s string) string {
			var b strings.Builder
			for _, r := range s {
				if r == '_' || r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' && b.Len() > 0 {
					b.WriteRune(r)
				}
			}
			if b.Len() == 0 {
				return "x"
			}
			return b.String()
		}
		label, capture := ident(name), ident(name)
		selector := `{` + ident(sel) + []string{`="`, `!="`, `=~"`, `!~"`}[shape%4] + escapeValue(sel) + `"}`
		q := strconv.Quote(val)
		stages := []string{
			` | drop ` + label, ` | drop ` + label + `, level`, ` | drop ` + label + `=` + q, ` | keep ` + label, ` | keep detected_level, ` + label,
			` | keep ` + label + `=` + q, ` | decolorize`, ` | line_format ` + strconv.Quote(line), ` | label_format ` + capture + `=` + ident(sel),
			` | label_format ` + capture + `=` + strconv.Quote(line), ` | regexp "(?P<` + capture + `>x)" | ` + capture + `=` + q,
			` | pattern "<` + capture + `> <_>" | ` + capture + `!=` + q + ` | drop ` + capture, ` | json | drop ` + label + ` | keep ` + capture,
			` | logfmt | ` + label + `=~` + q + ` | drop ` + label, ` |= ` + strconv.Quote(line) + ` | decolorize | drop ` + label,
			` | ` + label + `=` + q, ` | drop ` + label + ` | line_format ` + strconv.Quote(line) + ` | keep ` + label,
		}
		stage := stages[int(shape>>2)%len(stages)]
		function := []string{"count_over_time", "bytes_over_time", "absent_over_time", "rate", "bytes_rate"}[fn%5]
		span := []string{`[5m]`, `[1ms]`, `[1d]`, `[1h30m]`, `[5m] offset 1h`, `[1m] offset 0s`}[rng%6]
		body := function + `(` + selector + stage + span + `)`
		clauses := []string{``, `by (` + ident(val) + `)`, `without (` + ident(val) + `)`, `by (level)`, `by (` + ident(val) + `, ` + ident(sel) + `)`}
		clause := clauses[int(wrap>>3)%len(clauses)]
		if op := []string{"", "sum", "count", "min", "max", "avg"}[int(wrap)%6]; op != "" {
			if clause == "" {
				body = op + `(` + body + `)`
			} else {
				body = op + ` ` + clause + ` (` + body + `)`
			}
		}
		if logql.ParseAndValidate(logql.CanonicalizeLogRanges(body)) != nil {
			return // every route answers Loki's rejection before it translates anything
		}
		for _, prof := range rangeProfiles() {
			if lowered, legacy, same, delegated, ok := rangeAggLowering(t, body, prof); ok && !delegated && !same {
				t.Fatalf("%s: %s:\n lowered %s\n  string %s", prof.name, body, lowered, legacy)
			}
		}
	})
}

// fuzzValueOutOfScope reports a value the string translator misreads in ways the lowering does not follow (the
// corpus records them as known divergences): parentheses that do not balance, square brackets, and the words of an
// offset or a vector matching clause. A long value is out as well: each exec translates the query nine times, and
// a long value only slows the fuzzer down.
func fuzzValueOutOfScope(s string) bool {
	if len(s) > 96 {
		return true // each exec translates the query nine times: a long value only slows the fuzzer down
	}
	depth := 0
	for _, r := range s {
		switch r {
		case '(':
			depth++
		case ')':
			if depth--; depth < 0 {
				return true
			}
		case '[', ']':
			return true
		}
	}
	if depth != 0 {
		return true
	}
	for _, kw := range []string{"offset", "by", "on", "ignoring", "group_"} {
		if strings.Contains(s, kw) {
			return true
		}
	}
	return false
}

// clientReadsAsClause finds what readsAsClause finds.
func TestClientReadsAsClause(t *testing.T) {
	for _, s := range []string{"", "a", "without", "x without y", "withou", "bool", "a bool b", "boo", "|||", "a|||b", "||", "| |", "wibool", "bbool", "wwithout", "|||x", "xxbo", "w", "b", "|"} {
		if got, want := clientReadsAsClause(s), readsAsClause(s); got != want {
			t.Errorf("clientReadsAsClause(%q) = %v, readsAsClause = %v", s, got, want)
		}
	}
}
