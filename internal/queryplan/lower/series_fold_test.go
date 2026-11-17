package lower

import (
	"strings"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
)

// seriesFoldText is the text of f's pipes after a bucket stats pipe.
func seriesFoldText(t *testing.T, f SeriesFold, identity string) (string, bool) {
	t.Helper()
	pipes, ok := f.Pipes(identity)
	var b strings.Builder
	for _, p := range pipes {
		b.WriteString(" " + p.String())
	}
	return b.String(), ok
}

// The fold of a read: the bucket results read again as numbers, each row's value, the hash blanked unless the series
// spans buckets of the read's grid (the subquery, over the identity, on the grid sent), holds a NaN or has a value
// that is not finite, the blanked rows' value and count written into their columns, and the second stats pipe by the
// read's grouping: the outer aggregation's merge for the value column, a sum for every other. The text parses.
//
// conformance: semantics/unwrap-per-series-identity-raw-rows
func TestSeriesFoldPipes(t *testing.T) {
	const identity = `app:="a" | unpack_json | filter n:* | unpack_json from _msg result_prefix "__lvp_p_" | delete __lvp_p_n | pack_json fields (__lvp_p_*, level, detected_level, _stream) as __lvp_sid | hash(__lvp_sid) as __lvp_sid`
	avg := SeriesFold{Outer: logql.VectorSum, By: []string{"app", "__lvp_sid"}, Columns: []string{"__lvp_sum", "__sample_count"},
		Value: "__lvp_sum / __sample_count", ValueColumn: "__lvp_sum", CountColumn: "__sample_count", Field: "n", Bucket: "20s", Offset: "-17000000001ns"}
	got, ok := seriesFoldText(t, avg, identity)
	want := ` | math __lvp_sum as __lvp_sum | math __sample_count as __sample_count | math __lvp_sum / __sample_count as __lvp_fold` +
		` | format if (-__lvp_sid:in(` + identity + ` | stats by (__lvp_sid, _time:20s offset -17000000001ns) count() as __lvp_n, count() if (n:~"(?i)nan") as __lvp_nan` +
		` | stats by (__lvp_sid) count() as __lvp_n, sum(__lvp_nan) as __lvp_nan | filter __lvp_n:>1 or __lvp_nan:>0 | fields __lvp_sid) __lvp_fold:<inf __lvp_fold:>-inf) "" as __lvp_sid` +
		` | format if (__lvp_sid:="") "<__lvp_fold>" as __lvp_sum | math __lvp_sum as __lvp_sum | format if (__lvp_sid:="") "1" as __sample_count | math __sample_count as __sample_count` +
		` | stats by (app, __lvp_sid) sum(__lvp_sum) as __lvp_sum, sum(__sample_count) as __sample_count`
	if !ok || got != want {
		t.Fatalf("got %v\n%s\nwant\n%s", ok, got, want)
	}
	if _, err := logsql.Parse(identity + " | stats by (app, __lvp_sid) sum(__lvp_v) as __lvp_sum, count() as __sample_count" + got); err != nil {
		t.Fatalf("the folded read does not parse: %v", err)
	}
	// The outer aggregation's merge of the value column; an avg over a function without a count adds one.
	extreme := SeriesFold{By: []string{"__lvp_sid"}, Columns: []string{"c"}, Value: "c", ValueColumn: "c", Field: "a.b", Bucket: "1s", Offset: "-1ns"}
	for op, tail := range map[logql.VectorOp]string{
		logql.VectorMax: `| stats by (__lvp_sid) max(c) as c`,
		logql.VectorMin: `| stats by (__lvp_sid) min(c) as c`,
		logql.VectorSum: `| stats by (__lvp_sid) sum(c) as c`,
		logql.VectorAvg: `| stats by (__lvp_sid) sum(c) as c, count() as __lvp_n`,
	} {
		extreme.Outer = op
		got, ok := seriesFoldText(t, extreme, identity)
		if !ok || !strings.HasSuffix(got, tail) || strings.Contains(got, `"1" as`) || !strings.Contains(got, `count() if ("a.b":~"(?i)nan")`) {
			t.Errorf("%v: %v %s", op, ok, got)
		}
	}
	// No fold: an aggregation that does not fold, or a read without an identity, a value or a grid.
	for name, f := range map[string]SeriesFold{
		"count":    {Outer: logql.VectorCount, By: avg.By, Columns: avg.Columns, Value: avg.Value, ValueColumn: avg.ValueColumn, Bucket: "1s"},
		"no by":    {Outer: logql.VectorSum, Columns: avg.Columns, Value: avg.Value, ValueColumn: avg.ValueColumn, Bucket: "1s"},
		"no value": {Outer: logql.VectorSum, By: avg.By, Columns: avg.Columns, ValueColumn: avg.ValueColumn, Bucket: "1s"},
		"no grid":  {Outer: logql.VectorSum, By: avg.By, Columns: avg.Columns, Value: avg.Value, ValueColumn: avg.ValueColumn},
	} {
		if _, ok := f.Pipes(identity); ok {
			t.Errorf("%s: folded", name)
		}
	}
	if _, ok := avg.Pipes(""); ok {
		t.Error("no identity: folded")
	}
}
