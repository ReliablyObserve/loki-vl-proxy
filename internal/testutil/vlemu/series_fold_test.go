package vlemu

import (
	"strings"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/lower"
)

// The fold of a parser's series that lie in one bucket: the emulator classes the series over the read's rows (one
// line, several lines in one bucket, spanning buckets or holding a NaN or an infinity), folds the series of one line
// before the bucket stats and the others after them into one row per bucket and group with their value and count,
// and keeps the spanning series' partials; a list of values in in() is no subquery.
func TestEmulatorSeriesFold(t *testing.T) {
	e := newEmu(t,
		Row{TS: sec(1), Stream: `{app="a"}`, Value: 2, Msg: `{"v":2,"k":"one"}`},
		Row{TS: sec(2), Stream: `{app="a"}`, Value: 4, Msg: `{"v":4,"k":"twice"}`},
		Row{TS: sec(3), Stream: `{app="a"}`, Value: 6, Msg: `{"v":6,"k":"twice"}`},
		Row{TS: sec(5), Stream: `{app="a"}`, Value: 1, Msg: `{"v":1,"k":"spans"}`},
		Row{TS: sec(65), Stream: `{app="a"}`, Value: 3, Msg: `{"v":3,"k":"spans"}`},
		Row{TS: sec(7), Stream: `{app="a"}`, Raw: "NaN", Msg: `{"v":"NaN","k":"odd"}`},
	)
	const identity = `app:="a" | hash(__lvp_sid) as __lvp_sid`
	read, ok := lower.SeriesFold{Outer: logql.VectorSum, By: []string{"__lvp_sid"},
		Stats:   []logsql.StatsFuncAlias{{Func: logsql.Sum{Field: "__lvp_v"}, Alias: "__lvp_sum"}, {Func: logsql.Count{}, Alias: "__sample_count"}},
		Columns: []string{"__lvp_sum", "__sample_count"}, Value: "__lvp_sum / __sample_count", ValueColumn: "__lvp_sum",
		CountColumn: "__sample_count", Singletons: lower.SingletonsKept, Field: "v", Bucket: "60s", Offset: "0s"}.Pipes(identity)
	if !ok {
		t.Fatal("no fold")
	}
	q := identity
	for _, p := range read.Pipes {
		q += " " + p.String()
	}
	_, out := decodeStats(t, serve(e.Handler(), "/select/logsql/stats_query_range", rangeForm(q, "60s")))
	got := relSeries(out)
	blank := map[string]string{}
	kept := 0
	for key, points := range got {
		switch {
		case strings.HasPrefix(key, "__lvp_sum|__lvp_sid=") && !strings.HasSuffix(key, "__lvp_sid="):
			kept += len(points)
		case strings.HasSuffix(key, "__lvp_sid="):
			for _, p := range points {
				blank[strings.SplitN(key, "|", 2)[0]+"@"+p[0]] = p[1]
			}
		}
	}
	// Bucket 0: the series of one line (2) and the series of two lines in one bucket (mean 5) fold: 7 over 2 series.
	if blank["__lvp_sum@0"] != "7" || blank["__sample_count@0"] != "2" {
		t.Errorf("folded bucket 0: %v (all %v)", blank, got)
	}
	// The spanning series keeps its two buckets, and the NaN series its one.
	if kept != 3 {
		t.Errorf("%d kept points, want 3: %v", kept, got)
	}
	// A list of values in in() is no subquery: the query is left as it is.
	if q, fold := cutSeriesFold(e, `app:in("a","b") | stats count() as n`); fold != nil || q != `app:in("a","b") | stats count() as n` {
		t.Errorf("a value list was cut: %q %v", q, fold)
	}
	// The fold's conditions and math, term by term.
	fold := &emuSeriesFold{classes: map[string]string{"x": "s"}}
	fields := func() map[string]string {
		return map[string]string{"a": "6", "b": "3", "__lvp_ks": "sx", "__lvp_fold": "1.5", "__lvp_sid": ""}
	}
	for pipe, want := range map[string][2]string{
		`math a / b as q`:             {"q", "2"},
		`math a * b as q`:             {"q", "18"},
		`format "<a>-<b>" as __lvp_c`: {"__lvp_c", "6-3"},
		`format if (__lvp_ks:in(__lvp_span)) "s" as __lvp_fb`: {"__lvp_fb", "s"},
		`format if (__lvp_sid:="") "<__lvp_fold>" as b`:       {"b", "1.5"},
	} {
		f := fields()
		if !fold.foldPipe(pipe, []emuRec{{f: f}}) || f[want[0]] != want[1] {
			t.Errorf("%s: %s = %q, want %q", pipe, want[0], f[want[0]], want[1])
		}
	}
	f := fields()
	recs := []emuRec{{f: f}}
	for cond, want := range map[string]bool{
		`a:*`: true, `-a:*`: false, `__lvp_fold:<inf __lvp_fold:>-inf`: true, `-__lvp_ks:in(__lvp_span)`: false, `a:="6"`: true,
	} {
		if got := fold.holds(cond, f, fold.keys()); got != want {
			t.Errorf("%s: %v, want %v", cond, got, want)
		}
	}
	if fold.foldPipe(`format "x" as app`, recs) {
		t.Error("a format of another field is the fold's")
	}
}
