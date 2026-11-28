package lower

import (
	"strings"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan"
)

// avgStats are the bucket stats of avg over time.
var avgStats = []logsql.StatsFuncAlias{{Func: logsql.Sum{Field: "__lvp_v"}, Alias: "__lvp_sum"}, {Func: logsql.Count{}, Alias: "__sample_count"}}

// seriesFoldText is the folded read of f (around the bucket stats of avg over time when f has none).
func seriesFoldText(t *testing.T, f SeriesFold) (string, bool) {
	t.Helper()
	if f.Stats == nil {
		f.Stats = avgStats
	}
	read, ok := f.Pipes()
	if !ok {
		return "", false
	}
	var b strings.Builder
	for _, p := range read.Pipes {
		b.WriteString(" " + p.String())
	}
	return b.String(), true
}

// The fold of a read: the bucket stats by the bucket on the read's grid, the grouping and the series hash; each row's
// value and its finiteness; the series stats (their bucket count, first bucket, flag, finiteness and value, and their
// bucket rows as JSON); a series in one bucket whose value is finite folded (its hash, rows and partials blanked, its
// value into the fold's column), every other series' rows read back; the last stats pipe by the bucket, the grouping
// and the hash: each kept series' partials passed through, the outer aggregation's merge of the folded values and
// their count. The text parses.
//
// conformance: semantics/unwrap-per-series-identity-raw-rows
func TestSeriesFoldPipes(t *testing.T) {
	const base = `app:="a" | unpack_json | filter n:* | unpack_json from _msg result_prefix "__lvp_p_" | delete __lvp_p_n | pack_json fields (__lvp_p_*, level, detected_level, _stream) as __lvp_sid | hash(__lvp_sid) as __lvp_sid`
	avg := SeriesFold{Outer: logql.VectorSum, By: []string{"app", "__lvp_sid"}, Stats: avgStats, Columns: []string{"__lvp_sum", "__sample_count"},
		Value: "__lvp_sum / __sample_count", Bucket: "20s", Offset: "-17000000001ns"}
	got, ok := seriesFoldText(t, avg)
	want := ` | stats by (_time:20s offset -17000000001ns, app, __lvp_sid) sum(__lvp_v) as __lvp_sum, count() as __sample_count` +
		` | math __lvp_sum / __sample_count as __lvp_fold` +
		` | format if (-__lvp_bad:="2" __lvp_fold:<=1.7976931348623157e308 __lvp_fold:>=-1.7976931348623157e308) "1" as __lvp_ok` +
		` | stats by (app, __lvp_sid) count() as __lvp_nb, min(_time) as __lvp_b, min(__lvp_bad) as __lvp_bad, min(__lvp_ok) as __lvp_ok,` +
		` min(__lvp_fold) as __lvp_fold, json_values(_time, __lvp_bad, __lvp_sum, __sample_count) as __lvp_rows` +
		` | format if (__lvp_nb:="1" __lvp_ok:="1") "1" as __lvp_fb | format if (__lvp_fb:="1") "" as __lvp_rows | unroll (__lvp_rows)` +
		` | unpack_json from __lvp_rows | format if (__lvp_fb:="1") "<__lvp_b>" as _time | format if (-__lvp_fb:="1") "" as __lvp_fold` +
		` | format if (__lvp_fb:="1") "" as __lvp_sum | format if (__lvp_fb:="1") "" as __sample_count | format if (__lvp_fb:="1") "" as __lvp_sid` +
		` | stats by (_time, app, __lvp_sid) any(__lvp_sum) as __lvp_sum, any(__sample_count) as __sample_count, sum(__lvp_fold) as __lvp_fold, count() as __lvp_fc`
	if !ok || got != want {
		t.Fatalf("got %v\n%s\nwant\n%s", ok, got, want)
	}
	if _, err := logsql.Parse(base + got); err != nil {
		t.Fatalf("the folded read does not parse: %v", err)
	}
	// The outer aggregation's merge of the folded values: a sum for sum and avg (and for count, which reads the series
	// count), the extreme for max and min.
	extreme := SeriesFold{By: []string{"__lvp_sid"}, Stats: []logsql.StatsFuncAlias{{Func: logsql.Max{Field: "__lvp_v"}, Alias: "c"}}, Columns: []string{"c"}, Value: "c", Bucket: "1s", Offset: "-1ns"}
	for op, tail := range map[logql.VectorOp]string{
		logql.VectorMax:   `| stats by (_time, __lvp_sid) any(c) as c, max(__lvp_fold) as __lvp_fold, count() as __lvp_fc`,
		logql.VectorMin:   `| stats by (_time, __lvp_sid) any(c) as c, min(__lvp_fold) as __lvp_fold, count() as __lvp_fc`,
		logql.VectorSum:   `| stats by (_time, __lvp_sid) any(c) as c, sum(__lvp_fold) as __lvp_fold, count() as __lvp_fc`,
		logql.VectorAvg:   `| stats by (_time, __lvp_sid) any(c) as c, sum(__lvp_fold) as __lvp_fold, count() as __lvp_fc`,
		logql.VectorCount: `| stats by (_time, __lvp_sid) any(c) as c, sum(__lvp_fold) as __lvp_fold, count() as __lvp_fc`,
	} {
		extreme.Outer = op
		got, ok := seriesFoldText(t, extreme)
		if !ok || !strings.HasSuffix(got, tail) || !strings.Contains(got, `| stats by (_time:1s offset -1ns, __lvp_sid) max(__lvp_v) as c |`) ||
			!strings.Contains(got, `json_values(_time, __lvp_bad, c) as __lvp_rows`) {
			t.Errorf("%v: %v %s", op, ok, got)
		}
	}
	// stdvar: a series' value is its bucket's variance.
	dev := SeriesFold{Outer: logql.VectorSum, By: []string{"__lvp_sid"}, Stats: []logsql.StatsFuncAlias{{Func: logsql.Stddev{Field: "__lvp_v"}, Alias: "__lvp_sd"}}, Columns: []string{"__lvp_avg", "__sample_count", "__lvp_sd"},
		Value: "__lvp_sd * __lvp_sd", Bucket: "1s", Offset: "-1ns"}
	if got, ok := seriesFoldText(t, dev); !ok || !strings.Contains(got, `| math __lvp_sd * __lvp_sd as __lvp_fold |`) || !strings.Contains(got, `any(__lvp_avg) as __lvp_avg, any(__sample_count) as __sample_count, any(__lvp_sd) as __lvp_sd`) {
		t.Errorf("stdvar: %v %s", ok, got)
	}
	// No fold: an aggregation that does not fold, or a read without the series hash, a value, a grid, columns or
	// stats.
	for name, f := range map[string]SeriesFold{
		"stddev":     {Outer: logql.VectorStddev, Stats: avgStats, By: avg.By, Columns: avg.Columns, Value: avg.Value, Bucket: "1s"},
		"no by":      {Outer: logql.VectorSum, Stats: avgStats, Columns: avg.Columns, Value: avg.Value, Bucket: "1s"},
		"no hash":    {Outer: logql.VectorSum, Stats: avgStats, By: []string{"app"}, Columns: avg.Columns, Value: avg.Value, Bucket: "1s"},
		"no value":   {Outer: logql.VectorSum, Stats: avgStats, By: avg.By, Columns: avg.Columns, Bucket: "1s"},
		"no grid":    {Outer: logql.VectorSum, Stats: avgStats, By: avg.By, Columns: avg.Columns, Value: avg.Value},
		"no columns": {Outer: logql.VectorSum, Stats: avgStats, By: avg.By, Value: avg.Value, Bucket: "1s"},
		"no stats":   {Outer: logql.VectorSum, By: avg.By, Columns: avg.Columns, Value: avg.Value, Bucket: "1s"},
	} {
		if _, ok := f.Pipes(); ok {
			t.Errorf("%s: folded", name)
		}
	}
}

// The unwrap check's counting form passes the series stats, the finiteness mark and the unroll as they are; every
// other pipe of the fold (and any other pipe) is its to read.
//
// conformance: semantics/unwrap-per-series-identity-raw-rows, semantics/unwrap-conversion-error
func TestSeriesFoldKeepsFlag(t *testing.T) {
	read, ok := SeriesFold{Outer: logql.VectorSum, By: []string{"app", "__lvp_sid"}, Stats: avgStats, Columns: []string{"__lvp_sum", "__sample_count"},
		Value: "__lvp_sum / __sample_count", Bucket: "20s", Offset: "-1ns"}.Pipes()
	if !ok {
		t.Fatal("no fold")
	}
	var kept []string
	for _, p := range read.Pipes {
		if text := strings.TrimPrefix(p.String(), "| "); SeriesFoldKeepsFlag(text) {
			kept = append(kept, text)
		}
	}
	if len(kept) != 3 || !strings.HasPrefix(kept[1], "stats by (app, __lvp_sid) count() as __lvp_nb") || kept[2] != "unroll (__lvp_rows)" ||
		!strings.HasSuffix(kept[0], `"1" as __lvp_ok`) {
		t.Errorf("kept %q", kept)
	}
	for _, other := range []string{`stats by (app) count() as n`, `stats by (app) json_values(x) as y`, `unroll (x)`, `format "1" as __lvp_ok`, `math x as y`} {
		if SeriesFoldKeepsFlag(other) {
			t.Errorf("%s: kept", other)
		}
	}
}

// The folded read's base unpacks the line once: the parser's plain unpack writes its keys under the identity's prefix
// and copies them out, and the identity's own unpack goes, for json and logfmt alike. A base without exactly one of
// each, with the identity's unpack before the parser's, or with a pipe between them that reads the line or writes a
// prefixed key, and a plan that is not a parser's identity ahead, stay as they are.
//
// conformance: semantics/unwrap-per-series-identity-raw-rows
func TestSeriesFoldBase(t *testing.T) {
	const tail = ` | delete __lvp_p_n | pack_json fields (__lvp_p_*, level, detected_level, _stream) as __lvp_sid | hash(__lvp_sid) as __lvp_sid`
	jsonPlan := rangeAggPlan(`sum by (app) (avg_over_time({app="a"} | json | unwrap n [5m]))`)
	logfmtPlan := rangeAggPlan(`sum by (app) (avg_over_time({app="a"} | logfmt | unwrap n [5m]))`)
	for name, c := range map[string]struct {
		base, want string
		logfmt     bool
	}{
		"json": {base: `app:="a" | unpack_json | filter n:* | unpack_json from _msg result_prefix "__lvp_p_"` + tail,
			want: `app:="a" | unpack_json result_prefix "__lvp_p_" | copy __lvp_p_* as * | filter n:*` + tail},
		"adjacent": {base: `app:="a" | unpack_json | unpack_json from _msg result_prefix "__lvp_p_"` + tail,
			want: `app:="a" | unpack_json result_prefix "__lvp_p_" | copy __lvp_p_* as *` + tail},
		"logfmt": {base: `app:="a" | unpack_logfmt | filter n:* | unpack_logfmt from _msg result_prefix "__lvp_p_"` + tail, logfmt: true,
			want: `app:="a" | unpack_logfmt result_prefix "__lvp_p_" | copy __lvp_p_* as * | filter n:*` + tail},
		"reads the line": {base: `app:="a" | unpack_json | filter _msg:x | unpack_json from _msg result_prefix "__lvp_p_"` + tail},
		"prefixed key":   {base: `app:="a" | unpack_json | format "1" as __lvp_p_x | unpack_json from _msg result_prefix "__lvp_p_"` + tail},
		"two parsers":    {base: `app:="a" | unpack_json | unpack_json | unpack_json from _msg result_prefix "__lvp_p_"` + tail},
		"no identity":    {base: `app:="a" | unpack_json | filter n:*` + tail},
		"order":          {base: `app:="a" | unpack_json from _msg result_prefix "__lvp_p_" | unpack_json | filter n:*` + tail},
	} {
		p := jsonPlan
		if c.logfmt {
			p = logfmtPlan
		}
		want := c.want
		if want == "" {
			want = c.base
		}
		if got := SeriesFoldBase(p, c.base); got != want {
			t.Errorf("%s:\n%s\nwant\n%s", name, got, want)
		}
	}
	base := `app:="a" | unpack_json | unpack_json from _msg result_prefix "__lvp_p_"` + tail
	for name, p := range map[string]*queryplan.Plan{
		"nil":     nil,
		"listed":  rangeAggPlan(`sum by (app) (avg_over_time({app="a"} | json n, m | unwrap n [5m]))`),
		"no fold": rangeAggPlan(`sum by (app) (sum_over_time({app="a"} | json | unwrap n [5m]))`),
	} {
		if got := SeriesFoldBase(p, base); got != base {
			t.Errorf("%s: %s", name, got)
		}
	}
}
