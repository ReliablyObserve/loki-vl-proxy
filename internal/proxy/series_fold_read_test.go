package proxy

import (
	"encoding/json"
	"errors"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/lower"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/physical"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/testutil/vlemu"
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
// once and reads in the bucket stats the rows the counted gate flags. No nan mark is written before the last stats
// pipe: the folded values are finite and the kept partials pass through any.
//
// conformance: semantics/unwrap-per-series-identity-raw-rows, semantics/unwrap-conversion-error, semantics/unwrap-identity-nan-partials
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
	// The folded values are finite and a kept series' partials pass through any (a NaN stays NaN): no nan mark before
	// the last stats pipe, which would flag every kept series' rows (their folded value is blank).
	if strings.Contains(counted, `"2" as __lvp_bad | stats by (_time, `) {
		t.Errorf("a nan mark before the fold's last stats pipe:\n%s", counted)
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

// FuzzSeriesFoldMatrix holds the reader of a folded answer on any bytes: it never panics, answers JSON within the
// stats body bound, and otherwise fails closed (an unreadable answer, or past the bound).
//
// conformance: semantics/unwrap-per-series-identity-raw-rows
func FuzzSeriesFoldMatrix(f *testing.F) {
	for _, seed := range []string{
		`{"_time":"2026-01-01T00:01:00.000000001Z","app":"a","__lvp_bad":"0","__lvp_fold":"7","__lvp_fc":"2"}` + "\n" +
			`{"_time":"2026-01-01T00:00:00.000000001Z","app":"a","__lvp_sid":"9","__lvp_bad":"2","__lvp_sum":"NaN","__sample_count":"1","__lvp_fold":"NaN","__lvp_fc":"1"}`,
		`{"_time":"1767225600","__lvp_sid":"","__lvp_fold":"+Inf"}`,
		"\n\n",
		`[1,2]`,
		`{"_time":`,
		`{"app":"a"}`,
		`{"_time":"2026-01-01T00:00:00Z","app":{"x":1},"__lvp_sid":"\u0000","__lvp_sum":"\"<>&"}`,
	} {
		f.Add([]byte(seed), "app")
	}
	old := maxStatsBucketResponseBytes
	maxStatsBucketResponseBytes = 4096
	f.Cleanup(func() { maxStatsBucketResponseBytes = old })
	f.Fuzz(func(t *testing.T, raw []byte, group string) {
		if len(raw) > 1<<14 {
			return
		}
		read := &seriesFoldRead{outer: logql.VectorSum, columns: []string{"__lvp_sum", "__sample_count"}, valueColumn: "__lvp_sum",
			countColumn: "__sample_count", by: []string{group, dynamicSeriesHashLabel}}
		body, err := read.matrix(raw)
		var unreadable *statsBucketsUnreadableError
		switch {
		case err == nil:
			if !json.Valid(body) || int64(len(body)) > maxStatsBucketResponseBytes+3 {
				t.Fatalf("%d bytes, valid %v", len(body), json.Valid(body))
			}
		case !errors.As(err, &unreadable) && !errors.Is(err, errBodyTooLarge):
			t.Fatalf("an error that is neither unreadable nor too large: %v", err)
		}
	})
}

// A folded read VictoriaLogs fails (its timeout: a 503 with its own text) answers through the error layer, never with
// VictoriaLogs' text; an answer that is not rows fails the read the same way, with no raw-row read.
//
// conformance: semantics/unwrap-per-series-identity-raw-rows, semantics/error-layer-silent-success
func TestSeriesFoldReadFailure(t *testing.T) {
	s := generateDynWindowScenario(rand.New(rand.NewSource(3)), "sum", "pod", "avg_over_time", "json")
	for name, answer := range map[string]func(w http.ResponseWriter){
		"timeout": func(w http.ResponseWriter) {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte("the request couldn't be executed in 0.002 seconds; possible solutions: to increase -search.maxQueryDuration=10m0s"))
		},
		"not rows": func(w http.ResponseWriter) {
			_, _ = w.Write([]byte(`{"status":"success","data":{"resultType":"matrix","result":[]}}trailing`))
		},
	} {
		t.Run(name, func(t *testing.T) {
			emu := &vlemu.Emulator{T: t, Rows: s.rows, Field: "amount", Version: "v1.54.0"}
			inner := emu.Handler()
			folded := 0
			handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_ = r.ParseForm()
				if r.URL.Path == "/select/logsql/query" && strings.Contains(r.Form.Get("query"), ") as "+lower.SeriesFoldRows) {
					folded++
					answer(w)
					return
				}
				inner.ServeHTTP(w, r)
			})
			backend := httptest.NewServer(handler)
			defer backend.Close()
			c := cache.New(time.Minute, 100)
			defer c.Close()
			p, err := New(Config{BackendURL: backend.URL, Cache: c, LogLevel: "error"})
			if err != nil {
				t.Fatal(err)
			}
			defer closeTestProxy(p)
			params := url.Values{"query": {s.query()}, "start": {strconv.FormatInt(s.start/1e9, 10)}, "end": {strconv.FormatInt(s.end/1e9, 10)},
				"step": {strconv.FormatInt(int64(s.step/time.Second), 10)}}
			res := doCompatProxyRequest(p, "/loki/api/v1/query_range?"+params.Encode(), map[string]string{"X-Scope-OrgID": "0"})
			if res.Code < 500 || folded == 0 || strings.Contains(res.Body.String(), "couldn't be executed") || strings.Contains(res.Body.String(), "trailing") {
				t.Fatalf("%d after %d folded reads: %.300s", res.Code, folded, res.Body)
			}
			if raw := emu.EndpointCalls("/select/logsql/query"); len(raw) > 0 {
				t.Fatalf("read raw rows: %s", raw[0])
			}
		})
	}
}
