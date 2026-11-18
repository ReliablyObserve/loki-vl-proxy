package proxy

import (
	"encoding/json"
	"errors"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/lower"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/physical"
)

// avgFoldRead is the fold of sum by (app) over avg over time.
func avgFoldRead(t *testing.T, q string) *seriesFoldRead {
	t.Helper()
	return &seriesFoldRead{outer: logql.VectorSum, plan: resolvedPlan(q), columns: []string{"__lvp_sum", "__sample_count"},
		stats: []logsql.StatsFuncAlias{{Func: logsql.Sum{Field: "__lvp_v"}, Alias: "__lvp_sum"}, {Func: logsql.Count{}, Alias: "__sample_count"}},
		value: "__lvp_sum / __sample_count", valueColumn: "__lvp_sum", countColumn: "__sample_count"}
}

// The unwrap check's counting form of a folded read groups the bucket stats and the last stats pipe by the
// conversion flag, and passes the series stats, the finiteness mark and the unroll as they are: a series whose lines
// are flagged in one bucket and not in another still has both buckets, so it keeps its hash. The read unpacks the line
// once and reads in the bucket stats the rows the counted gate flags.
//
// conformance: semantics/unwrap-per-series-identity-raw-rows, semantics/unwrap-conversion-error
func TestSeriesFoldCountingForm(t *testing.T) {
	const q = `sum by (app) (avg_over_time({app="a"} | json | unwrap n [5m]))`
	op, err := lower.PlanQuery(q, resolvedPlan(q), lower.Profile{IdentityAhead: true})
	if err != nil {
		t.Fatal(err)
	}
	text := physical.Encode(op)
	first := strings.Index(text, " | stats by (")
	if first < 0 {
		t.Fatalf("no bucket stats: %s", text)
	}
	folded, ok := avgFoldRead(t, q).query(text[:first], []string{"app", dynamicSeriesHashLabel}, "sum(__lvp_v) as __lvp_sum, count() as __sample_count",
		time.Unix(1_800_000_000, 0), time.Minute)
	if !ok {
		t.Fatal("no fold")
	}
	if strings.Contains(folded, `from _msg result_prefix "__lvp_p_"`) || !strings.Contains(folded, `| unpack_json result_prefix "__lvp_p_" | copy __lvp_p_* as * |`) {
		t.Errorf("the read unpacks the line twice: %s", folded)
	}
	counted, _, field, _, _, ok := unwrapCountingRewrite(folded, func(string, string) (bool, bool) { return true, true })
	if !ok || field != "n" {
		t.Fatalf("the folded read is not counted (%v, %q): %s", ok, field, folded)
	}
	for _, want := range []string{
		"| stats by (_time:60s offset -1ns, app, __lvp_sid, __lvp_bad) sum(__lvp_v) as __lvp_sum",
		"| stats by (app, __lvp_sid) count() as __lvp_nb",
		`| format if (-__lvp_bad:="2" __lvp_fold:<=1.7976931348623157e308 __lvp_fold:>=-1.7976931348623157e308) "1" as __lvp_ok |`,
		"| unroll (__lvp_rows) |",
		"| stats by (_time, app, __lvp_sid, __lvp_bad) any(__lvp_sum) as __lvp_sum",
	} {
		if !strings.Contains(counted, want) {
			t.Errorf("counted form without %q:\n%s", want, counted)
		}
	}
	if nan := unwrapNaNFor(counted); !nan.poison || !nan.counts["__sample_count"] {
		t.Errorf("the NaN rule of the bucket stats: %+v", nan)
	}
}

// The bounds a folded read sends are the ones stats_query_range reads for its parameters: VictoriaLogs takes the end
// as exclusive, moves the start down and the end up to the edges of the buckets of the step shifted by the offset,
// and /select/logsql/query takes the end as exclusive again. The values below are the ones real VictoriaLogs answers
// the same buckets for (internal/querycorpus TestSeriesFoldBoundsMatchStatsQueryRange); a step or a time that does
// not parse fails the read.
//
// conformance: semantics/unwrap-per-series-identity-raw-rows
func TestSeriesFoldQueryParams(t *testing.T) {
	for _, c := range []struct {
		start, end, step, offset, wantStart, wantEnd string
	}{
		{"2026-01-01T00:00:00Z", "2026-01-01T00:06:00.000000001Z", "60s", "-1ns", "2025-12-31T23:59:00.000000001Z", "2026-01-01T00:06:00.000000001Z"},
		{"2026-01-01T00:00:17Z", "2026-01-01T00:06:17.000000001Z", "60s", "-17000000001ns", "2025-12-31T23:59:17.000000001Z", "2026-01-01T00:06:17.000000001Z"},
		{"2026-01-01T00:00:17.000000001Z", "2026-01-01T00:03:00.000000001Z", "20s", "-17000000002ns", "2025-12-31T23:59:57.000000002Z", "2026-01-01T00:03:17.000000002Z"},
		{"2026-01-01T00:00:00Z", "2026-01-01T00:05:30.000000001Z", "120s", "-1ns", "2025-12-31T23:58:00.000000001Z", "2026-01-01T00:06:00.000000001Z"},
		// Unix seconds, and no offset.
		{"1767225600", "1767225960", "60s", "", "2026-01-01T00:00:00Z", "2026-01-01T00:06:00Z"},
	} {
		in := url.Values{"query": {"q"}, "start": {c.start}, "end": {c.end}, "step": {c.step}, "timeout": {"1s"}}
		if c.offset != "" {
			in.Set("offset", c.offset)
		}
		out, err := SeriesFoldQueryParams(in)
		if err != nil {
			t.Fatalf("%v: %v", in, err)
		}
		if out.Get("start") != c.wantStart || out.Get("end") != c.wantEnd || out.Has("step") || out.Has("offset") || out.Get("query") != "q" || out.Get("timeout") != "1s" {
			t.Errorf("%v: %v, want %s .. %s", in, out, c.wantStart, c.wantEnd)
		}
	}
	for name, in := range map[string]url.Values{
		"start":  {"start": {"x"}, "end": {"1767225960"}, "step": {"60s"}},
		"end":    {"start": {"1767225600"}, "end": {"x"}, "step": {"60s"}},
		"step":   {"start": {"1767225600"}, "end": {"1767225960"}, "step": {"0s"}},
		"offset": {"start": {"1767225600"}, "end": {"1767225960"}, "step": {"60s"}, "offset": {"x"}},
	} {
		if _, err := SeriesFoldQueryParams(in); err == nil {
			t.Errorf("%s: no error", name)
		}
	}
}

// A folded answer is read back into stats_query_range's shape: a result per column and group, its labels the last
// stats pipe's grouping less the bucket (a label without a value written empty), its points the buckets in order, a
// time in seconds, and the unwrap check's flag when the rows name it. A folded group (no series hash) answers its merged value and series count in the value and count
// columns; a group that keeps its hash answers the bucket stats' results, and a count of one when the fold adds the
// count column. The results are ordered by column, name and labels. A row that is not an object, a row without its
// bucket, a read without its grouping or its series hash, and an answer past the stats body bound fail the read.
//
// conformance: semantics/unwrap-per-series-identity-raw-rows
func TestSeriesFoldMatrix(t *testing.T) {
	rows := strings.Join([]string{
		`{"_time":"2026-01-01T00:01:00.000000001Z","app":"a","__lvp_bad":"0","__lvp_fold":"7","__lvp_fc":"2"}`,
		`{"_time":"2026-01-01T00:00:00.000000001Z","app":"a","__lvp_bad":"0","__lvp_fold":"3.5","__lvp_fc":"1"}`,
		`{"_time":"2026-01-01T00:00:00.000000001Z","app":"a","__lvp_sid":"9","__lvp_bad":"2","__lvp_sum":"NaN","__sample_count":"1","__lvp_fold":"NaN","__lvp_fc":"1"}`,
		``,
	}, "\n")
	read := avgFoldRead(t, "")
	read.by = []string{"app", dynamicSeriesHashLabel}
	body, err := read.matrix([]byte(rows))
	if err != nil {
		t.Fatal(err)
	}
	const want = `{"status":"success","data":{"resultType":"matrix","result":[` +
		`{"metric":{"__name__":"__lvp_sum","app":"a","__lvp_sid":"","__lvp_bad":"0"},"values":[[1767225600,"3.5"],[1767225660,"7"]]},` +
		`{"metric":{"__name__":"__lvp_sum","app":"a","__lvp_sid":"9","__lvp_bad":"2"},"values":[[1767225600,"NaN"]]},` +
		`{"metric":{"__name__":"__sample_count","app":"a","__lvp_sid":"","__lvp_bad":"0"},"values":[[1767225600,"1"],[1767225660,"2"]]},` +
		`{"metric":{"__name__":"__sample_count","app":"a","__lvp_sid":"9","__lvp_bad":"2"},"values":[[1767225600,"1"]]}]}}`
	if string(body) != want {
		t.Errorf("got\n%s\nwant\n%s", body, want)
	}
	if !json.Valid(body) {
		t.Error("not JSON")
	}
	// max over time under an avg: the fold adds the count column, one for a series that keeps its hash.
	extreme := &seriesFoldRead{outer: logql.VectorAvg, columns: []string{"c"}, valueColumn: "c", countColumn: lower.SeriesFoldCount, added: true, by: []string{dynamicSeriesHashLabel}}
	body, err = extreme.matrix([]byte(`{"_time":"2026-01-01T00:00:00Z","__lvp_sid":"9","c":"4"}` + "\n" + `{"_time":"2026-01-01T00:00:00Z","c":"","__lvp_fold":"5","__lvp_fc":"3"}`))
	if err != nil || !strings.Contains(string(body), `{"metric":{"__name__":"__lvp_n","__lvp_sid":""},"values":[[1767225600,"3"]]}`) ||
		!strings.Contains(string(body), `{"metric":{"__name__":"__lvp_n","__lvp_sid":"9"},"values":[[1767225600,"1"]]}`) ||
		!strings.Contains(string(body), `{"metric":{"__name__":"c","__lvp_sid":""},"values":[[1767225600,"5"]]}`) {
		t.Errorf("%v %s", err, body)
	}
	var unreadable *statsBucketsUnreadableError
	noHash := &seriesFoldRead{columns: read.columns, valueColumn: read.valueColumn, countColumn: read.countColumn, by: []string{"app"}}
	for name, c := range map[string]struct {
		read *seriesFoldRead
		rows string
	}{
		"not an object": {read, `[1,2]`},
		"not JSON":      {read, `{"_time":`},
		"no bucket":     {read, `{"app":"a"}`},
		"no read":       {nil, `{"_time":"2026-01-01T00:00:00Z"}`},
		"no hash":       {noHash, `{"_time":"2026-01-01T00:00:00Z"}`},
	} {
		if _, err := c.read.matrix([]byte(c.rows)); !errors.As(err, &unreadable) {
			t.Errorf("%s: %v", name, err)
		}
	}
	// A quoted grouping name is read unquoted.
	quoted := avgFoldRead(t, "")
	quoted.by = []string{`"k8s.pod"`, dynamicSeriesHashLabel}
	if body, err := quoted.matrix([]byte(`{"_time":"2026-01-01T00:00:00Z","k8s.pod":"p","__lvp_fold":"1","__lvp_fc":"1"}`)); err != nil || !strings.Contains(string(body), `"k8s.pod":"p","__lvp_sid":""}`) {
		t.Errorf("quoted: %v %s", err, body)
	}
	old := maxStatsBucketResponseBytes
	maxStatsBucketResponseBytes = 64
	t.Cleanup(func() { maxStatsBucketResponseBytes = old })
	if _, err := read.matrix([]byte(rows)); !errors.Is(err, errBodyTooLarge) {
		t.Errorf("past the bound: %v", err)
	}
}
