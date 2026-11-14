package vlemu

import (
	"encoding/json"
	"net/http"
	"net/url"
	"reflect"
	"strings"
	"testing"
	"time"
)

func instantForm(query string) url.Values {
	return url.Values{"query": {query}, "start": {"0"}, "end": {"4000000000"}, "time": {"1700000000"}}
}

// stats_query answers one value per group at the request's time, as a vector.
func TestEmulatorStatsInstant(t *testing.T) {
	e := newEmu(t,
		Row{TS: sec(0), Stream: `{app="a"}`, Value: 1},
		Row{TS: sec(10), Stream: `{app="a"}`, Value: 2},
		Row{TS: sec(20), Stream: `{app="b"}`, Value: 4},
	)
	kind, out := decodeStats(t, serve(e.Handler(), "/select/logsql/stats_query", instantForm("* | stats by (app) sum(__lvp_v) as s")))
	if kind != "vector" || len(out) != 2 {
		t.Fatalf("%s %v", kind, out)
	}
	got := map[string]string{}
	for _, s := range out {
		got[s.metric["app"]] = s.values[0][1] + "@" + s.values[0][0]
	}
	if !reflect.DeepEqual(got, map[string]string{"a": "3@1700000000", "b": "4@1700000000"}) {
		t.Errorf("instant sums: %v", got)
	}
	if e.Widest != 2 {
		t.Errorf("Widest = %d, want the 2 series of the answer", e.Widest)
	}
}

// The pipes in front of and between the stats pipes: a delete, unpack_json, replace_regexp and a copy of a number.
func TestEmulatorStatsFieldPipes(t *testing.T) {
	e := newEmu(t,
		Row{TS: sec(0), Stream: `{app="a"}`, Value: 3, Labels: map[string]string{"zone": "z1", "k": "v1"}},
		Row{TS: sec(10), Stream: `{app="a"}`, Value: 5, Labels: map[string]string{"zone": "z2", "k": "v2"}},
	)
	h := e.Handler()
	query := func(q string) map[string][][2]string {
		_, out := decodeStats(t, serve(h, "/select/logsql/stats_query_range", rangeForm(q, "1m")))
		return relSeries(out)
	}
	// delete removes the fields it names (not a pattern ending in *) before the stats pipe groups by them.
	if got := query("* | delete zone | stats by (zone) count() as n"); !reflect.DeepEqual(got, map[string][][2]string{"n|zone=": {{"0", "2"}}}) {
		t.Errorf("delete: %v", got)
	}
	if got := query("* | delete zone* | stats by (zone) count() as n"); len(got) != 2 {
		t.Errorf("a delete of a pattern keeps the fields: %v", got)
	}
	// replace_regexp rewrites a field of the grouped rows.
	if got := query("* | stats by (zone) count() as n | replace_regexp(`z([0-9])`, \"zone-${1}\") at zone | stats by (zone) sum(n) as total"); !reflect.DeepEqual(got, map[string][][2]string{
		"total|zone=zone-1": {{"0", "1"}}, "total|zone=zone-2": {{"0", "1"}},
	}) {
		t.Errorf("replace_regexp: %v", got)
	}
	// math copies a number to another field; unpack_json reads one key of a JSON field.
	if got := query("* | stats row_min(_time, __lvp_v) as first, count() as n | unpack_json from first fields (__lvp_v) | math __lvp_v as v | stats sum(v) as s"); !reflect.DeepEqual(got, map[string][][2]string{"s": {{"0", "3"}}}) {
		t.Errorf("unpack_json and math: %v", got)
	}
	if msg := fatal(func() {
		serve(h, "/select/logsql/stats_query_range", rangeForm("* | stats row_min(_time, __lvp_v) as first | math first as v | stats sum(v) as s", "1m"))
	}); !strings.Contains(msg, "not a number") {
		t.Errorf("math of a JSON result: %q", msg)
	}
	if msg := fatal(func() {
		serve(h, "/select/logsql/stats_query_range", rangeForm("* | stats count() as n | unpack_json from n fields (x)", "1m"))
	}); !strings.Contains(msg, "unpack_json of") {
		t.Errorf("unpack_json of a number: %q", msg)
	}
	if msg := fatal(func() {
		serve(h, "/select/logsql/stats_query_range", rangeForm("* | stats count() as n | replace_regexp(`(`, \"x\") at n", "1m"))
	}); !strings.Contains(msg, "replace_regexp") {
		t.Errorf("replace_regexp of a bad expression: %q", msg)
	}
}

// A result with row_min or row_max alone has no metric field: VictoriaLogs answers 400.
func TestEmulatorRowFunctionsAlone(t *testing.T) {
	e := newEmu(t, Row{TS: sec(0), Stream: `{app="a"}`, Value: 1})
	rec := serve(e.Handler(), "/select/logsql/stats_query_range", rangeForm("* | stats row_min(_time, __lvp_v) as first", "1m"))
	if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), "missing metric fields in the results of query [* | stats row_min(_time, __lvp_v) as first]") {
		t.Errorf("status %d: %s", rec.Code, rec.Body)
	}
	if msg := fatal(func() {
		serve(e.Handler(), "/select/logsql/stats_query_range", rangeForm("* | stats row_min(_other, __lvp_v) as first, count() as n", "1m"))
	}); !strings.Contains(msg, "row_min(_other, __lvp_v)") {
		t.Errorf("row_min of another field: %q", msg)
	}
}

// The conversion gate of an unwrapped label: a row whose value the conversion rejects is flagged 1 and kept, with the
// value 0, only by a query that counts the flag; the plain gate drops it.
func TestEmulatorUnwrapGateFlags(t *testing.T) {
	e := newEmu(t,
		Row{TS: sec(0), Stream: `{app="a"}`, Value: 5},
		Row{TS: sec(10), Stream: `{app="a"}`, Raw: "abc"},
		Row{TS: sec(20), Stream: `{app="a"}`, Raw: "1e999"},
		Row{TS: sec(30), Stream: `{app="a"}`, Raw: "7"},
	)
	h := e.Handler()
	count := func(q string) map[string][][2]string {
		_, out := decodeStats(t, serve(h, "/select/logsql/stats_query_range", rangeForm(q, "1m")))
		return relSeries(out)
	}
	if got := count("* | stats count() as n"); !reflect.DeepEqual(got, map[string][][2]string{"n": {{"0", "2"}}}) {
		t.Errorf("the plain gate drops a word and a value out of range: %v", got)
	}
	counting := `* | format "1" as __lvp_bad | stats by (__lvp_bad) count() as n`
	got := count(counting)
	want := map[string][][2]string{"n|__lvp_bad=0": {{"0", "2"}}, "n|__lvp_bad=1": {{"0", "2"}}}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("the counting gate classifies the rows: %v", got)
	}
}

// A window shorter than the step is kept by its phase filter: the rows in (lower, upper] of each step.
func TestEmulatorWindowPhase(t *testing.T) {
	e := newEmu(t,
		Row{TS: sec(5), Stream: `{app="a"}`, Value: 1},
		Row{TS: sec(15), Stream: `{app="a"}`, Value: 2},
		Row{TS: sec(25), Stream: `{app="a"}`, Value: 3},
		Row{TS: sec(70), Stream: `{app="a"}`, Value: 4},
	)
	anchor, step := sec(0), int64(60*time.Second)
	// The bounds carry the 128 ns of tolerance the filter adds: the window is the rows 10 s to 20 s into each step.
	phase := "* | math ((_time - " + itoa(anchor) + ") % " + itoa(step) + ") as __lvp_window_phase | filter __lvp_window_phase:>" + itoa(int64(10*time.Second)+128) + " __lvp_window_phase:<=" + itoa(int64(20*time.Second)+128) + " | stats count() as n"
	_, out := decodeStats(t, serve(e.Handler(), "/select/logsql/stats_query_range", rangeForm(phase, "1m")))
	if got := relSeries(out); !reflect.DeepEqual(got, map[string][][2]string{"n": {{"0", "1"}}}) {
		t.Errorf("the row 15 s into the step is the only one in (10 s, 20 s]: %v", got)
	}
}

func itoa(n int64) string { b, _ := json.Marshal(n); return string(b) }

// A stats request the emulator cannot read is a test bug, reported at once.
func TestEmulatorRejectsWhatItCannotRun(t *testing.T) {
	e := newEmu(t, Row{TS: sec(0), Stream: `{app="a"}`, Value: 1})
	h := e.Handler()
	for _, c := range []struct {
		name  string
		path  string
		form  url.Values
		fatal string
	}{
		{"a step that is no duration", "/select/logsql/stats_query_range", rangeForm("* | stats count()", "soon"), `step "soon"`},
		{"an offset that is no duration", "/select/logsql/stats_query_range", func() url.Values { f := rangeForm("* | stats count()", "1m"); f.Set("offset", "later"); return f }(), `offset "later"`},
		{"a query without a stats pipe", "/select/logsql/stats_query", instantForm("* | filter x:*"), "no stats pipe"},
		{"an unknown pipe", "/select/logsql/stats_query", instantForm("* | stats count() | sort by (x)"), `unknown pipe "sort by (x)"`},
		{"an unknown stats function", "/select/logsql/stats_query", instantForm("* | stats !!"), `unknown stats function "!!"`},
		{"a stats function the emulator lacks", "/select/logsql/stats_query", instantForm("* | stats median(__lvp_v) as m"), "stats function median"},
	} {
		if msg := fatal(func() { serve(h, c.path, c.form) }); !strings.Contains(msg, c.fatal) {
			t.Errorf("%s: emulator said %q, want one with %q", c.name, msg, c.fatal)
		}
	}
}

// The window phase of a row is its time past the anchor, modulo the step; the float form reads the time as
// VictoriaLogs' math pipe does.
func TestWindowPhaseKeeps(t *testing.T) {
	for _, c := range []struct {
		ts, anchor, step, lower, upper int64
		float                          bool
		want                           bool
	}{
		{15, 0, 60, 10, 20, true, true},
		{10, 0, 60, 10, 20, true, false}, // lower is exclusive
		{20, 0, 60, 10, 20, true, true},  // upper is inclusive
		{75, 0, 60, 10, 20, true, true},  // the phase repeats every step
		{5, 0, 60, 10, 20, true, false},
		// The ideal form takes the 128 ns of tolerance and the anchor remainder out again.
		{15, 0, 60, 128 + 10, 128 + 20, false, true},
		{10, 0, 60, 128 + 10, 128 + 20, false, false},
		{-45, 0, 60, 128 + 10, 128 + 20, false, true}, // a time before the anchor wraps into the step: -45 is 15 past it
		{20, 0, 60, 128 + 10, 128 + 20, false, true},
		{21, 0, 60, 128 + 10, 128 + 20, false, false},
		{75, 0, 60, 128 + 10, 128 + 20, false, true},
	} {
		if got := WindowPhaseKeeps(c.ts, c.anchor, c.step, c.lower, c.upper, c.float); got != c.want {
			t.Errorf("WindowPhaseKeeps(%+v) = %v, want %v", c, got, c.want)
		}
	}
}

func TestFloorDivAndSplit(t *testing.T) {
	for _, c := range [][3]int64{{7, 2, 3}, {-7, 2, -4}, {-8, 2, -4}, {7, -2, -4}, {-7, -2, 3}, {0, 5, 0}, {5, 5, 1}, {-1, 60, -1}} {
		if got := FloorDiv(c[0], c[1]); got != c[2] {
			t.Errorf("FloorDiv(%d, %d) = %d, want %d", c[0], c[1], got, c[2])
		}
	}
	got := splitStatsFuncs("count() as n, row_min(_time, f) as first,  sum(x)")
	if want := []string{"count() as n", "row_min(_time, f) as first", "sum(x)"}; !reflect.DeepEqual(got, want) {
		t.Errorf("splitStatsFuncs = %q, want %q", got, want)
	}
	if got := lastStatsPipe([]string{"*", "stats a", "math x", "stats b", "filter y"}); got != "stats b" {
		t.Errorf("lastStatsPipe = %q", got)
	}
	if got := lastStatsPipe([]string{"*", "filter y"}); got != "" {
		t.Errorf("lastStatsPipe without one = %q", got)
	}
	row := Row{Value: 2.50}
	if row.Text() != "2.5" {
		t.Errorf("Text of a number = %q", row.Text())
	}
	if (Row{Value: 1, Raw: "nan"}).Text() != "nan" {
		t.Error("Raw is the text when it is set")
	}
	if got := (&Emulator{Field: "f"}).Line(Row{Value: 3}); got != "f=3" {
		t.Errorf("Line = %q", got)
	}
	if got := (&Emulator{}).LevelOf("", "x"); got != "unknown" {
		t.Errorf("LevelOf without a stored level = %q", got)
	}
}
