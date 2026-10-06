package proxy

import (
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
)

// unwrapFakeVL records every VictoriaLogs call and answers raw log queries with
// fixed rows and stats queries with an empty matrix.
type unwrapFakeVL struct {
	mu    sync.Mutex
	calls []unwrapFakeCall
	rows  []string // NDJSON rows of /select/logsql/query
}

type unwrapFakeCall struct{ path, query string }

func newUnwrapFakeVL(t *testing.T, rows []string) (*httptest.Server, *unwrapFakeVL) {
	t.Helper()
	fake := &unwrapFakeVL{rows: rows}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		fake.mu.Lock()
		fake.calls = append(fake.calls, unwrapFakeCall{r.URL.Path, r.Form.Get("query")})
		fake.mu.Unlock()
		switch r.URL.Path {
		case "/select/logsql/query":
			w.Header().Set("Content-Type", "application/x-ndjson")
			_, _ = w.Write([]byte(strings.Join(fake.rows, "\n")))
		case "/select/logsql/stats_query_range", "/select/logsql/stats_query":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"status":"success","data":{"resultType":"matrix","result":[]}}`))
		default:
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{}`))
		}
	}))
	t.Cleanup(srv.Close)
	return srv, fake
}

// query returns the last query sent to path ("" when none was).
func (f *unwrapFakeVL) query(path string) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := ""
	for _, c := range f.calls {
		if c.path == path {
			out = c.query
		}
	}
	return out
}

func unwrapRow(ts time.Time, fields map[string]string) string {
	row := map[string]string{"_time": ts.UTC().Format(time.RFC3339Nano), "_stream": `{app="x"}`, "_msg": "line"}
	for k, v := range fields {
		row[k] = v
	}
	b, _ := json.Marshal(row)
	return string(b)
}

// unwrapRangeValues runs a range query and returns the values of its only series.
func unwrapRangeValues(t *testing.T, p *Proxy, query string, start, end time.Time, step time.Duration) []string {
	t.Helper()
	params := url.Values{"query": {query}, "start": {strconv.FormatInt(start.UnixNano(), 10)}, "end": {strconv.FormatInt(end.UnixNano(), 10)}, "step": {strconv.Itoa(int(step.Seconds()))}}
	rec := httptest.NewRecorder()
	p.handleQueryRange(rec, httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+params.Encode(), nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("%s: %d %s", query, rec.Code, rec.Body.String())
	}
	var resp struct {
		Data struct {
			Result []struct {
				Values [][]any `json:"values"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("%s: %v: %s", query, err, rec.Body.String())
	}
	if len(resp.Data.Result) != 1 {
		t.Fatalf("%s: expected one series, got %s", query, rec.Body.String())
	}
	var values []string
	for _, v := range resp.Data.Result[0].Values {
		values = append(values, fmt.Sprint(v[1]))
	}
	return values
}

// duration() and bytes() are converted by VictoriaLogs in Loki's syntax (Go
// durations in seconds, humanize sizes in bytes), so the stats pipe reads the
// rows that convert and nothing is fetched row by row.
func TestUnwrapConversionIsConvertedInTheStatsQuery(t *testing.T) {
	t0 := time.Unix(1700000400, 0).UTC()
	for _, tc := range []struct{ name, query, conv, want string }{
		{"duration", `sum by (app) (max_over_time({app="x"} | unwrap duration(v) [1m]))`, "duration", " | stats by (app) max(__lvp_v)"},
		{"bytes", `sum by (app) (max_over_time({app="x"} | unwrap bytes(v) [1m]))`, "bytes", " | stats by (app) max(__lvp_v)"},
		{"duration rate, sliding", `sum by (app) (rate({app="x"} | unwrap duration(v) [2m]))`, "duration", " | stats by (app) sum(__lvp_v) as c, count() as __sample_count"},
		{"bytes rate, tumbling", `sum by (app) (rate({app="x"} | unwrap bytes(v) [1m]))`, "bytes", " | stats by (app) sum(__lvp_v) as __lvp_inner"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv, fake := newUnwrapFakeVL(t, nil)
			p := newSlidingTestProxy(t, srv.URL)
			params := url.Values{"query": {tc.query}, "start": {strconv.FormatInt(t0.UnixNano(), 10)}, "end": {strconv.FormatInt(t0.Add(10*time.Minute).UnixNano(), 10)}, "step": {"60"}}
			rec := httptest.NewRecorder()
			p.handleQueryRange(rec, httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+params.Encode(), nil))
			if rec.Code != http.StatusOK {
				t.Fatalf("%d %s", rec.Code, rec.Body.String())
			}
			want := translator.UnwrapGateFor("v", tc.conv) + tc.want
			if q := fake.query("/select/logsql/stats_query_range"); !strings.Contains(q, want) {
				t.Fatalf("expected %q in the VictoriaLogs query, got %q", want, q)
			}
			if q := fake.query("/select/logsql/query"); q != "" {
				t.Fatalf("a conversion must not be fetched row by row: %q", q)
			}
		})
	}
}

// A plain unwrap's stats pipe reads only the rows that make a sample: the
// translated query carries the gate, whichever route sends it.
func TestUnwrapStatsQueriesCarryTheSampleGate(t *testing.T) {
	t0 := time.Unix(1700000400, 0).UTC()
	gate := translator.UnwrapGate("b")
	for _, tc := range []struct {
		name, query, path, want string
	}{
		{"native range", `sum by (app) (max_over_time({app="x"} | unwrap b [1m]))`, "/select/logsql/stats_query_range", gate + " | stats by (app) max(__lvp_v)"},
		{"bare parser buckets", `max_over_time({app="x"} | logfmt | unwrap b [2m])`, "/select/logsql/stats_query_range", translator.UnwrapGate("b") + " | stats by (_stream) max(__lvp_v) as c"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv, fake := newUnwrapFakeVL(t, nil)
			p := newSlidingTestProxy(t, srv.URL)
			params := url.Values{"query": {tc.query}, "start": {strconv.FormatInt(t0.UnixNano(), 10)}, "end": {strconv.FormatInt(t0.Add(10*time.Minute).UnixNano(), 10)}, "step": {"60"}}
			rec := httptest.NewRecorder()
			p.handleQueryRange(rec, httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+params.Encode(), nil))
			if rec.Code != http.StatusOK {
				t.Fatalf("%d %s", rec.Code, rec.Body.String())
			}
			if q := fake.query(tc.path); !strings.Contains(q, tc.want) {
				t.Fatalf("expected %q in the VictoriaLogs query, got %q", tc.want, q)
			}
		})
	}
}

// A tumbling window (range == step) of a function VictoriaLogs has no exact
// stats for (rate_counter, quantile) is answered by the raw evaluator, which
// converts duration() and bytes() with Go's own functions,, as it is for a sliding one: the gate's own math pipe must not make
// the query look like a rate pipeline the native stats route can run.
func TestUnwrapTumblingWindowsKeepTheRawEvaluator(t *testing.T) {
	t0 := time.Unix(1700000400, 0).UTC()
	rows := []string{
		unwrapRow(t0.Add(10*time.Second), map[string]string{"n": "10", "d": "1m30s"}),
		unwrapRow(t0.Add(20*time.Second), map[string]string{"n": "20", "d": "30s"}),
		unwrapRow(t0.Add(30*time.Second), map[string]string{"n": "30", "d": "x"}),
	}
	for _, tc := range []struct{ name, query, want string }{
		{"quantile", `quantile_over_time(0.5, {app="x"} | unwrap n [1m])`, "20"},
		{"quantile grouped", `sum by (app) (quantile_over_time(0.5, {app="x"} | unwrap n [1m]))`, "20"},
		{"rate_counter", `rate_counter({app="x"} | unwrap n [1m])`, "0.3333333333333333"},
		{"quantile of a conversion", `quantile_over_time(0.5, {app="x"} | unwrap duration(d) [1m])`, "60"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv, fake := newUnwrapFakeVL(t, rows)
			p := newSlidingTestProxy(t, srv.URL)
			got := unwrapRangeValues(t, p, tc.query, t0.Add(time.Minute), t0.Add(time.Minute), time.Minute)
			if len(got) != 1 || got[0] != tc.want {
				t.Fatalf("got %v, want [%s]", got, tc.want)
			}
			if q := fake.query("/select/logsql/stats_query_range"); q != "" {
				t.Fatalf("expected the raw evaluator, VictoriaLogs was asked for stats: %q", q)
			}
			if q := fake.query("/select/logsql/query"); q == "" || strings.Contains(q, translator.UnwrapValueAlias) {
				t.Fatalf("expected the raw evaluator on the ungated query, got %q", q)
			}
		})
	}
}

// quantile reads the unwrapped field, not the gate's alias: the raw evaluator
// sees the ungated query and converts the value itself.
func TestUnwrapQuantileReadsTheUnwrappedValues(t *testing.T) {
	t0 := time.Unix(1700000400, 0).UTC()
	rows := []string{
		unwrapRow(t0.Add(10*time.Second), map[string]string{"n": "10"}),
		unwrapRow(t0.Add(20*time.Second), map[string]string{"n": "20"}),
		unwrapRow(t0.Add(30*time.Second), map[string]string{"n": "30"}),
		unwrapRow(t0.Add(35*time.Second), map[string]string{"other": "no n here"}),
	}
	srv, fake := newUnwrapFakeVL(t, rows)
	p := newSlidingTestProxy(t, srv.URL)
	// A 2m window at a 1m step is a sliding window: the raw evaluator.
	got := unwrapRangeValues(t, p, `quantile_over_time(0.5, {app="x"} | unwrap n [2m])`, t0.Add(time.Minute), t0.Add(time.Minute), time.Minute)
	if len(got) != 1 || got[0] != "20" {
		t.Fatalf("got %v, want [20]", got)
	}
	if q := fake.query("/select/logsql/query"); q == "" || strings.Contains(q, translator.UnwrapValueAlias) {
		t.Fatalf("expected the raw evaluator on the ungated query, got %q", q)
	}
}

// rate over an unwrapped label is the sum of its values per second
// (pkg/logql/range_vector.go rateLogs with computeValues), not a line rate: a
// sliding window asks VictoriaLogs for buckets of the converted sum, with the
// sample count as presence, and divides the window's sum by its seconds.
func TestUnwrapRateSlidingWindowSumsBuckets(t *testing.T) {
	t0 := time.Unix(1700000400, 0).UTC()
	srv, fake := newUnwrapFakeVL(t, nil)
	p := newSlidingTestProxy(t, srv.URL)
	params := url.Values{"query": {`sum by (app) (rate({app="x"} | unwrap n [2m]))`}, "start": {strconv.FormatInt(t0.UnixNano(), 10)}, "end": {strconv.FormatInt(t0.Add(10*time.Minute).UnixNano(), 10)}, "step": {"60"}}
	rec := httptest.NewRecorder()
	p.handleQueryRange(rec, httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+params.Encode(), nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("%d %s", rec.Code, rec.Body.String())
	}
	want := translator.UnwrapGate("n") + " | stats by (app) sum(__lvp_v) as c, count() as __sample_count"
	if q := fake.query("/select/logsql/stats_query_range"); !strings.Contains(q, want) {
		t.Fatalf("expected %q in the VictoriaLogs query, got %q", want, q)
	}
	if q := fake.query("/select/logsql/query"); q != "" {
		t.Fatalf("a sliding unwrap rate is answered from buckets, not rows: %q", q)
	}
}

func TestUnwrapRateWindowIsSumPerSecond(t *testing.T) {
	t0 := time.Unix(1700000400, 0).UTC()
	series := map[string]manualSeriesSamples{"a": {
		Metric: map[string]string{"app": "x"},
		// Buckets (edge, edge+60s] labelled by their left edge: 30 in the first
		// minute, 90 in the third, none in the second.
		Samples:        []rangeMetricSample{{ts: t0.UnixNano(), value: 30}, {ts: t0.Add(2 * time.Minute).UnixNano(), value: 90}},
		PresentBuckets: []int64{t0.UnixNano(), t0.Add(2 * time.Minute).UnixNano()},
	}}
	body, err := buildHitsRangeMetricMatrix("unwrap_rate", series, t0.Add(2*time.Minute), t0.Add(4*time.Minute), time.Minute, 2*time.Minute, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	var resp struct {
		Data struct {
			Result []struct {
				Values [][]any `json:"values"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, v := range resp.Data.Result[0].Values {
		got = append(got, fmt.Sprintf("%v=%v", v[0], v[1]))
	}
	// t0+2m: window (t0, t0+2m] holds the buckets at t0 (30) and t0+1m (none): 30/120.
	// t0+3m: (t0+1m, t0+3m] holds the buckets at t0+1m (none) and t0+2m (90): 90/120.
	// t0+4m: (t0+2m, t0+4m] holds the buckets at t0+2m (90) and t0+3m (none): 90/120.
	want := []string{
		fmt.Sprintf("%v=0.25", float64(t0.Add(2*time.Minute).Unix())),
		fmt.Sprintf("%v=0.75", float64(t0.Add(3*time.Minute).Unix())),
		fmt.Sprintf("%v=0.75", float64(t0.Add(4*time.Minute).Unix())),
	}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("got %v, want %v", got, want)
	}
}

// One infinite sample must not turn every later window into NaN, as a prefix sum
// would: each window is summed on its own.
func TestUnwrapRateWindowWithInfinityStaysLocal(t *testing.T) {
	t0 := time.Unix(1700000400, 0).UTC()
	step := time.Minute
	series := map[string]manualSeriesSamples{"a": {
		Metric:         map[string]string{"app": "x"},
		Samples:        []rangeMetricSample{{ts: t0.UnixNano(), value: math.Inf(1)}, {ts: t0.Add(2 * step).UnixNano(), value: 60}},
		PresentBuckets: []int64{t0.UnixNano(), t0.Add(2 * step).UnixNano()},
	}}
	body, err := buildHitsRangeMetricMatrix("unwrap_rate", series, t0.Add(step), t0.Add(3*step), step, step, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(body), `"+Inf"`) {
		t.Fatalf("the window holding the infinity must be +Inf: %s", body)
	}
	if !strings.Contains(string(body), `"1"`) {
		t.Fatalf("the later window holds 60 over 60s = 1, not NaN: %s", body)
	}
}

// The bytes() gate reads its value with an extract_regexp pipe; that must not make
// the query one with parser stages, which would send it to the raw evaluator.
func TestUnwrapGateIsNotAParserStage(t *testing.T) {
	base := `{app="a"} | unpack_logfmt | filter __error__:=""`
	for _, conv := range []string{"", "duration", "bytes"} {
		if queryUsesParserStages(base + translator.UnwrapGateFor("f", conv)) {
			t.Errorf("conv %q: the gate counts as a parser stage", conv)
		}
	}
	if !queryUsesParserStages(base + ` | extract_regexp "x" from y` + translator.UnwrapGateFor("f", "bytes")) {
		t.Error("a real extract_regexp before the gate must still count")
	}
}
