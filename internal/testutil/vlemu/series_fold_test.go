package vlemu

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/lower"
)

// The fold of a parser's series that lie in one bucket: the emulator answers a folded read on /select/logsql/query
// with a row per bucket and group. The series of one line and the series of two lines in one bucket fold into the
// bucket's row (the sum of their values, their count); a series spanning buckets keeps its bucket rows, its partials
// as they are; a NaN line's series keeps its hash; a field without a value is left out of a row.
func TestEmulatorSeriesFold(t *testing.T) {
	e := newEmu(t,
		Row{TS: sec(1), Stream: `{app="a"}`, Value: 2, Msg: `{"v":2,"k":"one"}`},
		Row{TS: sec(2), Stream: `{app="a"}`, Value: 4, Msg: `{"v":4,"k":"twice"}`},
		Row{TS: sec(3), Stream: `{app="a"}`, Value: 6, Msg: `{"v":6,"k":"twice"}`},
		Row{TS: sec(5), Stream: `{app="a"}`, Value: 1, Msg: `{"v":1,"k":"spans"}`},
		Row{TS: sec(65), Stream: `{app="a"}`, Value: 3, Msg: `{"v":3,"k":"spans"}`},
		Row{TS: sec(7), Stream: `{app="a"}`, Raw: "NaN", Msg: `{"v":"NaN","k":"odd"}`},
	)
	const base = `app:="a" | hash(__lvp_sid) as __lvp_sid | format "2" as __lvp_bad`
	for _, outer := range []logql.VectorOp{logql.VectorSum, logql.VectorMax} {
		read, ok := lower.SeriesFold{Outer: outer, By: []string{"__lvp_sid"},
			Stats:   []logsql.StatsFuncAlias{{Func: logsql.Sum{Field: "__lvp_v"}, Alias: "__lvp_sum"}, {Func: logsql.Count{}, Alias: "__sample_count"}},
			Columns: []string{"__lvp_sum", "__sample_count"}, Value: "__lvp_sum / __sample_count", Bucket: "60s", Offset: "0s"}.Pipes()
		if !ok {
			t.Fatal("no fold")
		}
		q := base
		for _, p := range read.Pipes {
			q += " " + p.String()
		}
		// The unwrap check's counting form groups the bucket stats and the last stats pipe by the flag.
		q = strings.Replace(q, "stats by (_time:60s offset 0s, __lvp_sid)", "stats by (_time:60s offset 0s, __lvp_sid, __lvp_bad)", 1)
		q = strings.Replace(q, "stats by (_time, __lvp_sid)", "stats by (_time, __lvp_sid, __lvp_bad)", 1)
		rec := serve(e.Handler(), "/select/logsql/query", url.Values{"query": {q}, "start": {fmt.Sprint(sec(-600) / 1e9)}, "end": {fmt.Sprint(sec(600) / 1e9)}})
		if rec.Code != 200 {
			t.Fatalf("status %d: %s", rec.Code, rec.Body)
		}
		folded, kept := map[string]string{}, 0
		sc := bufio.NewScanner(rec.Body)
		for sc.Scan() {
			var row map[string]string
			if err := json.Unmarshal(sc.Bytes(), &row); err != nil {
				t.Fatalf("%v: %s", err, sc.Text())
			}
			if _, ok := row["__lvp_sid"]; ok {
				kept++
				if row["__lvp_sum"] == "" || row["__sample_count"] == "" {
					t.Errorf("a kept row without its partials: %v", row)
				}
				continue
			}
			folded[row["_time"]+" "+row["__lvp_bad"]] = row["__lvp_fold"] + "/" + row["__lvp_fc"]
		}
		// Bucket 0: the series of one line (2) and the series of two lines in one bucket (mean 5) fold.
		want := map[logql.VectorOp]string{logql.VectorSum: "7/2", logql.VectorMax: "5/2"}[outer]
		if got := folded["2023-11-14T22:00:00Z 0"]; got != want || len(folded) != 1 {
			t.Errorf("%v: folded %v, want %s", outer, folded, want)
		}
		// The spanning series keeps its two buckets, and the NaN series its one.
		if kept != 3 {
			t.Errorf("%v: %d kept rows, want 3", outer, kept)
		}
	}
	if isSeriesFold(`app:="a" | stats count() as n`) {
		t.Error("a stats read is a fold")
	}
}

// TestSeriesHashKeyOrder: the series hash of a parser's identity reads the line's keys sorted after set_stream_fields,
// and in the line's order after pack_json (VictoriaLogs' unpack starts a block per sequence of keys and pack_json
// follows the block's columns), so the emulator splits the series the old text split, json and logfmt alike.
//
// conformance: semantics/unwrap-identity-key-order
func TestSeriesHashKeyOrder(t *testing.T) {
	e := &Emulator{T: t, Field: "v"}
	for _, tc := range []struct{ a, b string }{
		{`{"route":"/x","v":3,"user":"u"}`, `{"user":"u","v":4,"route":"/x"}`},
		{`route=/x v=3 user=u`, `user=u v=4 route=/x`},
	} {
		row := func(msg string) map[string]string {
			return map[string]string{"_stream": `{app="a"}`, "detected_level": "info", "_msg": msg}
		}
		if e.seriesHash(row(tc.a), false) != e.seriesHash(row(tc.b), false) {
			t.Errorf("set_stream_fields: %s and %s are two series", tc.a, tc.b)
		}
		if e.seriesHash(row(tc.a), true) == e.seriesHash(row(tc.b), true) {
			t.Errorf("pack_json: %s and %s are one series, VictoriaLogs splits them", tc.a, tc.b)
		}
	}
	// The unwrapped key and a value-only difference never split a series.
	if e.seriesHash(map[string]string{"_msg": `{"v":1,"k":"x"}`}, false) != e.seriesHash(map[string]string{"_msg": `{"k":"x","v":2}`}, false) {
		t.Error("the unwrapped value split a series")
	}
}
