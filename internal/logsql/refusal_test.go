package logsql

import (
	"net/http"
	"strings"
	"testing"
)

// Nothing is predicted for an unknown release, a path no rule covers, a query the proxy's parser does not read, or
// a pipe no rule knows.
//
// conformance: vl-query-too-long, vl-stats-range-pipe-before-stats
func TestPredictRefusalAbstains(t *testing.T) {
	v53 := ParseVersion("v1.53.0")
	cases := []struct {
		name string
		in   RefusalInput
		v    Version
	}{
		{"unknown release", RefusalInput{Path: PathQuery, Query: strings.Repeat("x", 20000), MaxQueryLen: 16384}, Version{}},
		{"unknown release, a stats shape", RefusalInput{Path: PathStatsQueryRange, Query: `* | sort by (a) | stats count()`}, Version{}},
		{"unread path", RefusalInput{Path: "/select/logsql/hits", Query: strings.Repeat("x", 20000), MaxQueryLen: 16384}, v53},
		{"unknown limit", RefusalInput{Path: PathQuery, Query: strings.Repeat("x", 20000)}, v53},
		{"length on tail", RefusalInput{Path: PathTail, Query: strings.Repeat("x", 20000), MaxQueryLen: 16384}, v53},
		{"filter pipe without its keyword", RefusalInput{Path: PathStatsQueryRange, Query: `* | level:info | sort by (x) | stats count()`}, v53},
		{"wildcard delete", RefusalInput{Path: PathStatsQueryRange, Query: `* | delete _t* | stats count()`}, v53},
		{"wildcard fields", RefusalInput{Path: PathStatsQueryRange, Query: `* | fields lev* | stats count()`}, v53},
		{"stats only", RefusalInput{Path: PathStatsQueryRange, Query: `{app="a"} | stats by (level) count() as c`}, v53},
		{"no query", RefusalInput{Path: PathQuery}, v53},
	}
	for _, c := range cases {
		if r, ok := PredictRefusal(c.in, c.v); ok {
			t.Errorf("%s: predicted %+v", c.name, r)
		}
	}
}

// A release line gate reads a later patch of an older line apart from the line's first release.
//
// conformance: vl-math-quoted-constant-clone-crash
func TestRefusalGatesByReleaseLine(t *testing.T) {
	g := GateMathQuotedConstantClone
	for v, want := range map[string]bool{
		"v1.49.0": false, "v1.50.2": false, "v1.51.1": false, "v1.52.0": false, "v1.52.1": false, "v1.52.2": false,
		"v1.53.0": true, "v1.54.0": true, "v2.0.0": true, "garbage": false,
	} {
		if got := g.In(ParseVersion(v)); got != want {
			t.Errorf("%s: %v, want %v", v, got, want)
		}
	}
	if lts := (Gate{From: [3]int{1, 53, 0}, Lines: map[[2]int]int{{1, 52}: 1}}); lts.In(ParseVersion("v1.52.0")) || !lts.In(ParseVersion("v1.52.1")) || lts.In(ParseVersion("v1.51.9")) {
		t.Fatal("a line entry gates the line from its patch on")
	}
	if GateExtraFiltersLen.In(ParseVersion("v1.53.0")) || !GateExtraFiltersLen.In(ParseVersion("v1.54.0")) || GateExtraFiltersLen.In(ParseVersion("v1.52.2")) {
		t.Fatal("extra filters length gate")
	}
	if v := ParseVersion("1.52.1"); !v.Known || v.String() != "v1.52.1" || (Version{}).String() != "unknown" {
		t.Fatalf("version %+v", v)
	}
}

// An unknown release guards the clone crash (VictoriaLogs may lack the fix) and predicts nothing else.
//
// conformance: vl-math-quoted-constant-clone-crash
func TestPredictRefusalUnknownReleaseGuardsCrash(t *testing.T) {
	in := RefusalInput{Path: PathQuery, Query: `* | math "2025-01-01T00:00:00Z" as x`, Limit: true}
	if r, ok := PredictRefusal(in, Version{}); !ok || !r.Crash {
		t.Fatalf("unknown release: %+v %v", r, ok)
	}
	in.Query = `* | math "2025-01-01T00:00:00Z" as x | ("`
	if _, ok := PredictRefusal(in, Version{}); ok {
		t.Fatal("a text the parser does not read predicts nothing")
	}
}

// From v1.54 an extra_filters or extra_stream_filters arg over the length limit is refused; before, it is not.
//
// conformance: vl-extra-filters-too-long
func TestPredictRefusalExtraFilters(t *testing.T) {
	in := RefusalInput{Path: PathStatsQueryRange, Query: `* | stats count() as c`, MaxQueryLen: 100, ExtraStreamFilters: []string{`{tenant="` + strings.Repeat("a", 120) + `"}`}}
	if _, ok := PredictRefusal(in, ParseVersion("v1.53.0")); ok {
		t.Fatal("v1.53 does not bound extra_stream_filters")
	}
	r, ok := PredictRefusal(in, ParseVersion("v1.54.0"))
	if !ok || r.Rule != RuleExtraFiltersTooLong || r.Status != http.StatusUnprocessableEntity || !strings.Contains(r.Message, "`extra_stream_filters` arg length cannot exceed -search.maxQueryLen=100") {
		t.Fatalf("got %+v %v", r, ok)
	}
	in.ExtraStreamFilters, in.ExtraFilters = nil, []string{strings.Repeat("x", 101)}
	if r, ok := PredictRefusal(in, ParseVersion("v1.54.0")); !ok || !strings.Contains(r.Message, "`extra_filters`") {
		t.Fatalf("got %+v %v", r, ok)
	}
	in.Query = `* | stats count() as c | limit 1` // the query's own refusal comes first
	if r, _ := PredictRefusal(in, ParseVersion("v1.54.0")); r.Rule != RuleExtraFiltersTooLong {
		t.Fatalf("extra filters are read before the stats shape, got %s", r.Rule)
	}
}

// The screen never skips a query the parser-based rules refuse: over pipelines built from every pipe the rules
// read, PredictRefusal (screened) and the rules applied to every parsed query agree.
//
// conformance: vl-stats-range-pipe-before-stats, vl-stats-pipe-after-stats
func TestRefusalScreenAgreesWithRules(t *testing.T) {
	pipes := []string{
		"filter level:x", "sort by (a)", "limit 2", "offset 1", "fields a", "fields _time, a", "delete _time", "delete a",
		"copy a as _time", "copy a as b", "rename a as _time", "rename a as b", "format \"<a>\" as _time", "format \"<a>\" as b",
		"math 1 as _time", "math 1 as b", "replace (\"a\", \"b\") at _time", "unpack_json fields (_time)", "unpack_json",
		"unpack_logfmt", "unroll (_time)", "extract \"<_time> \"", "total_stats count() as t", "running_stats count() as r",
		"top 2 by (a)", "uniq by (a)", "first 1 by (a)", "last 1 by (a)", "stats count() as c", "stats by (a) count() as c",
		"hash(a) as _time", "len(a) as _time", "coalesce (a, b) as _time", "drop_empty_fields", "decolorize",
	}
	for _, rel := range []string{"v1.49.0", "v1.52.1", "v1.53.0", "v1.54.0"} {
		v := ParseVersion(rel)
		for _, path := range []string{PathStatsQuery, PathStatsQueryRange} {
			for _, a := range pipes {
				for _, b := range pipes {
					for _, c := range []string{"stats count() as n", "stats by (a) count() as n | limit 1", "stats count() as n | math n*2 as m"} {
						text := "* | " + a + " | " + b + " | " + c
						got, ok := PredictRefusal(RefusalInput{Path: path, Query: text}, v)
						q, err := Parse(text)
						if err != nil {
							continue
						}
						want, wok := predictStatsRefusal(q, path == PathStatsQueryRange, v)
						if !GatePipeCoalesce.In(v) && strings.Contains(text, "coalesce") {
							continue // refused earlier as an unknown pipe
						}
						if ok != wok || got.Rule != want.Rule {
							t.Fatalf("%s %s %q: screened %v %q, rules %v %q", rel, path, text, ok, got.Rule, wok, want.Rule)
						}
					}
				}
			}
		}
	}
}

// The screen reads pipe names outside quotes and brackets, and gives up beyond the pipes it keeps.
func TestRefusalPipeWords(t *testing.T) {
	var out [4]pipeWord
	n, ok := pipeWords(`_msg:"a|b" | filter x:~"(a|b)" | stats by (a) count()`, out[:])
	if !ok || n != 2 || out[0].name != "filter" || out[1].name != "stats" {
		t.Fatalf("%d %v %+v", n, ok, out[:n])
	}
	if _, ok := pipeWords(strings.Repeat("| filter x ", 5), out[:]); ok {
		t.Fatal("five pipes must not fit four")
	}
	if statsShapeMayRefuse(strings.Repeat("| filter x ", 70)+"| stats count()", true, ParseVersion("v1.53.0")) {
		t.Fatal("more pipes than the screen keeps must leave the decision to the parser")
	}
	if !mathHasQuotedNumber(`'2025-01-01T00:00:00Z' + a`) || mathHasQuotedNumber(`'1s' + a`) || mathHasQuotedNumber(`"unterminated`) || mathHasQuotedNumber(`"a\"b" + 1`) || quotedNumber("0x1f") {
		t.Fatal("quoted number")
	}
}

func BenchmarkPredictRefusalSuccessPath(b *testing.B) {
	v := ParseVersion("v1.53.0")
	in := RefusalInput{Path: PathStatsQueryRange, MaxQueryLen: 16384,
		Query: `{app="api"} _time:[1700000000, 1700003600) | unpack_json fields (level, status) | filter status:>=500 | stats by (_time:1m, level) count() as c`}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, ok := PredictRefusal(in, v); ok {
			b.Fatal("predicted")
		}
	}
}
