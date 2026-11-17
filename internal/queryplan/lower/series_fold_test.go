package lower

import (
	"strings"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translate"
)

// avgStats are the bucket stats of avg over time.
var avgStats = []logsql.StatsFuncAlias{{Func: logsql.Sum{Field: "__lvp_v"}, Alias: "__lvp_sum"}, {Func: logsql.Count{}, Alias: "__sample_count"}}

// seriesFoldText is the folded read of f over identity (around the bucket stats of avg over time when f has none).
func seriesFoldText(t *testing.T, f SeriesFold, identity string) (string, bool) {
	t.Helper()
	if f.Stats == nil {
		f.Stats = avgStats
	}
	read, ok := f.Pipes(identity)
	if !ok {
		return "", false
	}
	var b strings.Builder
	for _, p := range read.Pipes {
		b.WriteString(" " + p.String())
	}
	return b.String(), true
}

// The fold of a read: each row's series class from the subquery (over the identity, on the grid sent, written twice
// so VictoriaLogs runs it once), the hash of a series of one line blanked before the bucket stats (grouped by the
// class too), the bucket results read again as numbers, each row's value, the hash of a series of several lines in
// one bucket blanked after them unless its value is not finite, the blanked rows' value and count written into their
// columns, and the second stats pipe by the read's grouping: the outer aggregation's merge for the value column, a sum
// for every other. The text parses.
//
// conformance: semantics/unwrap-per-series-identity-raw-rows
func TestSeriesFoldPipes(t *testing.T) {
	const identity = `app:="a" | unpack_json | filter n:* | unpack_json from _msg result_prefix "__lvp_p_" | delete __lvp_p_n | pack_json fields (__lvp_p_*, level, detected_level, _stream) as __lvp_sid | hash(__lvp_sid) as __lvp_sid`
	avg := SeriesFold{Outer: logql.VectorSum, By: []string{"app", "__lvp_sid"}, Stats: avgStats, Columns: []string{"__lvp_sum", "__sample_count"},
		Value: "__lvp_sum / __sample_count", ValueColumn: "__lvp_sum", CountColumn: "__sample_count", Singletons: SingletonsKept,
		Field: "n", Bucket: "20s", Offset: "-17000000001ns"}
	got, ok := seriesFoldText(t, avg, identity)
	sub := identity + ` | stats by (__lvp_sid, _time:20s offset -17000000001ns) count() as __lvp_n, count() if (n:~"(?i)(nan|inf)") as __lvp_odd` +
		` | stats by (__lvp_sid) count() as __lvp_nb, sum(__lvp_n) as __lvp_n, sum(__lvp_odd) as __lvp_odd | filter __lvp_n:>1 or __lvp_odd:>0` +
		` | format if (__lvp_nb:>1 or __lvp_odd:>0) "s<__lvp_sid>" as __lvp_k | format if (__lvp_nb:<=1 __lvp_odd:<=0) "m<__lvp_sid>" as __lvp_k | fields __lvp_k`
	want := ` | format "s<__lvp_sid>" as __lvp_ks | format "m<__lvp_sid>" as __lvp_km | format if (__lvp_ks:in(` + sub + `)) "s" as __lvp_c` +
		` | format if (__lvp_km:in(` + sub + `)) "m" as __lvp_c | format if (-__lvp_c:*) "" as __lvp_sid` +
		` | stats by (app, __lvp_sid, __lvp_c) sum(__lvp_v) as __lvp_sum, count() as __sample_count` +
		` | math __lvp_sum as __lvp_sum | math __sample_count as __sample_count | math __lvp_sum / __sample_count as __lvp_fold` +
		` | format if (-__lvp_sid:="" -__lvp_c:="s" __lvp_fold:<inf __lvp_fold:>-inf) "1" as __lvp_fb` +
		` | format if (__lvp_fb:="1") "<__lvp_fold>" as __lvp_sum | math __lvp_sum as __lvp_sum | format if (__lvp_fb:="1") "1" as __sample_count` +
		` | math __sample_count as __sample_count | format if (__lvp_fb:="1") "" as __lvp_sid` +
		` | stats by (app, __lvp_sid) sum(__lvp_sum) as __lvp_sum, sum(__sample_count) as __sample_count`
	if !ok || got != want {
		t.Fatalf("got %v\n%s\nwant\n%s", ok, got, want)
	}
	if _, err := logsql.Parse(identity + got); err != nil {
		t.Fatalf("the folded read does not parse: %v", err)
	}
	// The outer aggregation's merge of the value column; an avg over a function without a count adds one; a function
	// whose bucket stats do not hold the series of one line blanks no hash before them.
	extreme := SeriesFold{By: []string{"__lvp_sid"}, Stats: []logsql.StatsFuncAlias{{Func: logsql.Max{Field: "__lvp_v"}, Alias: "c"}}, Columns: []string{"c"}, Value: "c", ValueColumn: "c", Field: "a.b", Bucket: "1s", Offset: "-1ns"}
	for op, tail := range map[logql.VectorOp]string{
		logql.VectorMax: `| stats by (__lvp_sid) max(c) as c`,
		logql.VectorMin: `| stats by (__lvp_sid) min(c) as c`,
		logql.VectorSum: `| stats by (__lvp_sid) sum(c) as c`,
		logql.VectorAvg: `| stats by (__lvp_sid) sum(c) as c, count() as __lvp_n`,
	} {
		extreme.Outer = op
		got, ok := seriesFoldText(t, extreme, identity)
		if !ok || !strings.HasSuffix(got, tail) || !strings.Contains(got, `| stats by (__lvp_sid, __lvp_c) max(__lvp_v) as c |`) ||
			strings.Contains(got, `"1" as __sample_count`) || strings.Contains(got, `(-__lvp_c:*)`) ||
			!strings.Contains(got, `count() if ("a.b":~"(?i)(nan|inf)")`) {
			t.Errorf("%v: %v %s", op, ok, got)
		}
	}
	// stddev and stdvar: a series of one line is 0.
	dev := SeriesFold{Outer: logql.VectorSum, By: []string{"__lvp_sid"}, Stats: []logsql.StatsFuncAlias{{Func: logsql.Stddev{Field: "__lvp_v"}, Alias: "__lvp_sd"}}, Columns: []string{"__lvp_avg", "__sample_count", "__lvp_sd"},
		Value: "__lvp_sd * __lvp_sd", ValueColumn: "__lvp_sd", CountColumn: "__sample_count", Singletons: SingletonsZero, Field: "n", Bucket: "1s", Offset: "-1ns"}
	if got, ok := seriesFoldText(t, dev, identity); !ok || !strings.Contains(got, `| format if (__lvp_sid:="") "0" as __lvp_sd | math __lvp_sd as __lvp_sd | math __lvp_sd * __lvp_sd as __lvp_fold`) {
		t.Errorf("stdvar: %v %s", ok, got)
	}
	// No fold: an aggregation that does not fold, or a read without an identity, a value, a grid, or a count for the
	// series of one line.
	for name, f := range map[string]SeriesFold{
		"count":     {Outer: logql.VectorCount, Stats: avgStats, By: avg.By, Columns: avg.Columns, Value: avg.Value, ValueColumn: avg.ValueColumn, Bucket: "1s"},
		"no by":     {Outer: logql.VectorSum, Stats: avgStats, Columns: avg.Columns, Value: avg.Value, ValueColumn: avg.ValueColumn, Bucket: "1s"},
		"no value":  {Outer: logql.VectorSum, Stats: avgStats, By: avg.By, Columns: avg.Columns, ValueColumn: avg.ValueColumn, Bucket: "1s"},
		"no grid":   {Outer: logql.VectorSum, Stats: avgStats, By: avg.By, Columns: avg.Columns, Value: avg.Value, ValueColumn: avg.ValueColumn},
		"no counts": {Outer: logql.VectorSum, Stats: avgStats, By: avg.By, Columns: avg.Columns, Value: avg.Value, ValueColumn: avg.ValueColumn, Bucket: "1s", Singletons: SingletonsKept},
		"no stats":  {Outer: logql.VectorSum, By: avg.By, Columns: avg.Columns, Value: avg.Value, ValueColumn: avg.ValueColumn, Bucket: "1s"},
	} {
		if _, ok := f.Pipes(identity); ok {
			t.Errorf("%s: folded", name)
		}
	}
	if _, ok := avg.Pipes(""); ok {
		t.Error("no identity: folded")
	}
}

// The subquery reads the identity without the pipes that derive service_name for a clause (the hash does not read
// it), unless a stage of the query reads a label after its parser; a query without the identity, or a text without
// those pipes, is left as it is.
//
// conformance: semantics/unwrap-per-series-identity-raw-rows
func TestSeriesFoldIdentity(t *testing.T) {
	caps := logsql.Capabilities{}
	service := translate.ServiceNameGroupingPipes(caps)
	ungated := `app:="a" ` + service + ` | unpack_json | hash(__lvp_sid) as __lvp_sid`
	for q, stripped := range map[string]bool{
		`sum by (service_name) (avg_over_time({app="a"} | json | unwrap n [5m]))`:          true,
		`sum by (service_name) (avg_over_time({app="a"} | json | x="1" | unwrap n [5m]))`:  false,
		`sum by (service_name) (sum_over_time({app="a"} | json | unwrap n [5m]))`:          false,
		`sum by (service_name) (avg_over_time({app="a"} |= "x" | logfmt | unwrap n [5m]))`: true,
	} {
		got := SeriesFoldIdentity(rangeAggPlan(q), caps, ungated)
		if want := `app:="a" | unpack_json | hash(__lvp_sid) as __lvp_sid`; stripped && got != want || !stripped && got != ungated {
			t.Errorf("%s: %s", q, got)
		}
	}
	if got := SeriesFoldIdentity(nil, caps, ungated); got != ungated {
		t.Errorf("no plan: %s", got)
	}
	if plain := `app:="a" | unpack_json | hash(__lvp_sid) as __lvp_sid`; SeriesFoldIdentity(rangeAggPlan(`sum(avg_over_time({app="a"} | json | unwrap n [5m]))`), caps, plain) != plain {
		t.Error("a text without the service pipes changed")
	}
}
