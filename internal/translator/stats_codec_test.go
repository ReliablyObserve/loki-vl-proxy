package translator

import (
	"reflect"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/physical"
)

// A stats query is one with a stats pipe outside quoted strings, as the typed parser reads it, with the
// quote-aware scan for the shorthand pipes the parser does not model (`| rate(`, `| count(`).
func TestDecodeStatsTellsAStatsQuery(t *testing.T) {
	for _, tc := range []struct {
		query string
		stats bool
	}{
		{`path:="quote\"slash\\" | stats count()`, true},
		{`path:="quote\" | stats count()"`, false},
		{`path:="quote\" | stats count()" | stats count()`, true},
		{`app:="a" | rate()`, true},
		{`app:="a"`, false},
	} {
		if got := isStatsTextHeuristic(tc.query); got != tc.stats {
			t.Fatalf("heuristic %s: %v", tc.query, got)
		}
		if _, got := DecodeStats(tc.query); got != tc.stats {
			t.Fatalf("DecodeStats(%s) stats %v", tc.query, got)
		}
	}
}

// The shape of the first stats pipe: the query before it, its grouping (a bucket key stays one entry; `by ()` is
// ByExplicit), its function and field.
func TestDecodeStatsShape(t *testing.T) {
	for _, tc := range []struct {
		query string
		want  physical.VLStats
	}{
		{`app:="nginx" | unpack_json | stats by (namespace, app) first(duration)`, physical.VLStats{Shaped: true, Base: `app:="nginx" | unpack_json`, By: []string{"namespace", "app"}, Func: "first", Field: "duration"}},
		{`app:="nginx" | stats by () avg(confidence)`, physical.VLStats{Shaped: true, Base: `app:="nginx"`, ByExplicit: true, Func: "avg", Field: "confidence"}},
		{`app:="a" | stats by (_time:60000000000ns offset 1ns, pod) count()`, physical.VLStats{Shaped: true, Base: `app:="a"`, By: []string{"_time:60000000000ns offset 1ns", "pod"}, Func: "count"}},
		{`app:="a" | stats count() as c | math c/60 as v`, physical.VLStats{}},
		{`app:="a"`, physical.VLStats{}},
		{`app:="a"` + UnwrapGateFor("latency", "") + ` | stats by (pod) sum(` + UnwrapValueAlias + `)`, physical.VLStats{Shaped: true,
			Base: `app:="a"` + UnwrapGateFor("latency", ""), By: []string{"pod"}, Func: "sum", Field: UnwrapValueAlias,
			Gate: UnwrapGateFor("latency", ""), UnwrapField: "latency",
			UngatedText: `app:="a" | stats by (pod) sum(` + UnwrapValueAlias + `)`}},
	} {
		got, _ := DecodeStats(tc.query)
		tc.want.Text = tc.query
		if tc.want.UngatedText == "" {
			tc.want.UngatedText = tc.query
		}
		if !reflect.DeepEqual(got, tc.want) {
			t.Fatalf("DecodeStats(%s) = %+v, want %+v", tc.query, got, tc.want)
		}
	}
}
