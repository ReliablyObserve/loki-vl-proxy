package proxy

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"
)

// The settled steps cache keeps the answers of the window evaluator per
// evaluation time and evaluates only the steps a moved window does not hold,
// the way Loki's results cache keeps extents of its query_range answers
// (pkg/storage/chunk/cache/resultscache/cache.go Do, handleHit). Every answer
// must be byte for byte the uncached proxy's, Loki's 400 for a rejected value
// included.

// queryStepsRows: two apps and two levels on a new pod every 20 seconds (a
// stream per batch, as Kubernetes pods rotate), four lines each, over span;
// raw, when set, gives the unwrapped text of a line in place of its number.
func queryStepsRows(s0 time.Time, span time.Duration, raw func(at time.Duration, app string) string) []emuRow {
	var rows []emuRow
	n := 0
	for at := time.Duration(0); at < span; at += 20 * time.Second {
		for _, app := range []string{"pay", "auth"} {
			for _, level := range []string{"info", "error"} {
				n++
				stream := fmt.Sprintf(`{app=%q,env="production",level=%q,pod="%s-%04d"}`, app, level, app, n)
				for i := 0; i < 4; i++ {
					row := emuRow{ts: s0.Add(at + time.Duration(i)*3*time.Second + 500*time.Millisecond).UnixNano(), stream: stream, value: float64((n*7+i*3)%41) + 0.25}
					if raw != nil && i == 0 {
						row.raw = raw(at, app)
					}
					rows = append(rows, row)
				}
			}
		}
	}
	return rows
}

// queryStepsWindows: a refresh moving forward a step, two steps and a minute,
// windows moved back, a re-run, a window widened back, another step, a grid
// off whole seconds and its refresh, and a minute step.
func queryStepsWindows(end time.Time) []extentWindow {
	start := end.Add(-90 * time.Minute)
	w := func(from time.Time, span, step time.Duration) extentWindow {
		return extentWindow{from, from.Add(span), step}
	}
	ms := start.Add(250 * time.Millisecond)
	return []extentWindow{
		w(start, 90*time.Minute, 10*time.Second),
		w(start.Add(10*time.Second), 90*time.Minute, 10*time.Second),
		w(start.Add(30*time.Second), 90*time.Minute, 10*time.Second),
		w(start.Add(90*time.Second), 90*time.Minute, 10*time.Second),
		w(start.Add(-time.Minute), 90*time.Minute, 10*time.Second),
		w(start.Add(-2*time.Minute), 90*time.Minute, 10*time.Second),
		w(start.Add(30*time.Second), 90*time.Minute, 10*time.Second),
		w(start.Add(-30*time.Minute), 2*time.Hour, 10*time.Second),
		w(start, 90*time.Minute, 15*time.Second),
		w(start.Add(45*time.Second), 90*time.Minute, 15*time.Second),
		w(ms, 60*time.Minute, 10*time.Second),
		w(ms.Add(time.Minute), 60*time.Minute, 10*time.Second),
		w(start, 90*time.Minute, time.Minute),
		w(start.Add(time.Minute), 90*time.Minute, time.Minute),
		w(start.Add(3*time.Minute), 90*time.Minute, time.Minute),
	}
}

var queryStepsQueries = []string{
	`sum without (pod) (max_over_time({env="production"} | logfmt amount | unwrap amount [5m]))`,
	`sum without (pod) (sum_over_time({env="production"} | logfmt amount | unwrap amount [5m]))`,
	`sum without (pod) (rate({env="production"} | logfmt amount | unwrap amount [5m]))`,
	`max without (pod) (max_over_time({env="production"} | logfmt amount | unwrap amount [5m]))`,
	`topk(2, sum without (pod) (max_over_time({env="production"} | logfmt amount | unwrap amount [5m])))`,
	`sum without (pod) (count_over_time({env="production"}[1m]))`,
	`quantile_over_time(0.95, {env="production"} | logfmt | unwrap amount [1m]) by (app)`,
	`sum(rate_counter({env="production"} | logfmt | unwrap amount [5m]))`,
	`sum by (app) (sum_over_time({env="production"} | logfmt | unwrap amount [5m]))`,
	`max by (app) (max_over_time({env="production"} | logfmt | unwrap amount | __error__="" [1m]))`,
	`sum by (app) (avg_over_time({env="production"} | logfmt | unwrap amount [2m]))`,
	`sum by (app, level) (sum_over_time({env="production"} | logfmt | unwrap amount [30s]))`,
	`sum by (app) (count_over_time({env="production"}[2m]))`,
	`sum by (app) (rate({env="production"}[2m])) / sum by (app) (count_over_time({env="production"}[1m]))`,
}

// runQueryStepsDifferential sends every window of every query to a proxy
// with the caches and one without, each over its own emulated VictoriaLogs,
// and requires the same status and bytes. It returns the outcomes of the
// settled steps cache and the answers with data and with Loki's 400.
func runQueryStepsDifferential(t *testing.T, rows []emuRow, queries []string, windows []extentWindow) (outcomes map[string]int, withData, nans, rejected int) {
	t.Helper()
	onSrv, _ := newExtentShim(t, (&vlEmulator{t: t, rows: rows, field: "amount", version: "v1.50.0"}).handler())
	offSrv, _ := newExtentShim(t, (&vlEmulator{t: t, rows: rows, field: "amount", version: "v1.50.0"}).handler())
	on := newExtentTestProxy(t, onSrv.URL, true, 256<<20)
	off := newExtentTestProxy(t, offSrv.URL, false, 256<<20)
	for _, query := range queries {
		for _, w := range windows {
			wantCode, want, wantHeader := queryStepsGet(t, off, query, w)
			gotCode, got, gotHeader := queryStepsGet(t, on, query, w)
			if gotCode != wantCode || !sameQueryStepsAnswer(got, want) {
				t.Fatalf("%s [%s, %s] step %s: the settled steps changed the answer\n with: %d %s\nwithout: %d %s", query, w.start.Format(time.RFC3339Nano), w.end.Format(time.RFC3339Nano), w.step, gotCode, got, wantCode, want)
			}
			for _, name := range []string{"Content-Type", "X-Content-Type-Options", "Warning"} {
				if gotHeader.Get(name) != wantHeader.Get(name) {
					t.Fatalf("%s [%s, %s] step %s: header %s %q, without the steps %q", query, w.start.Format(time.RFC3339Nano), w.end.Format(time.RFC3339Nano), w.step, name, gotHeader.Get(name), wantHeader.Get(name))
				}
			}
			switch {
			case wantCode == http.StatusOK && bytes.Contains(want, []byte(`"values":[[`)):
				withData++
				if bytes.Contains(want, []byte(`"NaN"`)) {
					nans++
				}
			case wantCode == http.StatusBadRequest && bytes.Contains(want, []byte("SampleExtractionErr")):
				rejected++
			case wantCode != http.StatusOK:
				t.Fatalf("%s: %d %s", query, wantCode, want)
			}
		}
	}
	return queryStepsOutcomes(t, on), withData, nans, rejected
}

// sameQueryStepsAnswer compares two answers byte for byte, or, where the
// uncached proxy writes a metric's labels in no fixed order (the native
// tumbling route), series by series in order with their labels as a set.
func sameQueryStepsAnswer(a, b []byte) bool {
	if bytes.Equal(a, b) {
		return true
	}
	type answer struct {
		Status string `json:"status"`
		Data   struct {
			ResultType string `json:"resultType"`
			Result     []struct {
				Metric map[string]string `json:"metric"`
				Values [][2]any          `json:"values"`
			} `json:"result"`
		} `json:"data"`
	}
	var x, y answer
	if json.Unmarshal(a, &x) != nil || json.Unmarshal(b, &y) != nil {
		return false
	}
	return reflect.DeepEqual(x, y)
}

// queryStepsGet sends one window through the handler and returns the answer
// with its headers, once the stores it started are done.
func queryStepsGet(t testing.TB, p *Proxy, query string, w extentWindow) (int, []byte, http.Header) {
	t.Helper()
	params := url.Values{
		"query": {query},
		"start": {strconv.FormatInt(w.start.UnixNano(), 10)},
		"end":   {strconv.FormatInt(w.end.UnixNano(), 10)},
		"step":  {strconv.FormatFloat(w.step.Seconds(), 'f', -1, 64)},
	}
	rec := httptest.NewRecorder()
	p.handleQueryRange(rec, httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+params.Encode(), nil))
	p.statsExtentStores.wait()
	return rec.Code, rec.Body.Bytes(), rec.Header()
}

func queryStepsOutcomes(t testing.TB, p *Proxy) map[string]int {
	t.Helper()
	rec := httptest.NewRecorder()
	p.metrics.Handler(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	out := map[string]int{}
	for _, line := range strings.Split(rec.Body.String(), "\n") {
		const prefix = `loki_vl_proxy_internal_operation_total{operation="query_steps_cache",outcome="`
		if rest, ok := strings.CutPrefix(line, prefix); ok {
			outcome, value, _ := strings.Cut(rest, `"} `)
			n, _ := strconv.Atoi(value)
			out[outcome] += n
		}
	}
	return out
}

// TestQueryRangeStepsMatchUncached: the window evaluator's shapes (folds
// without a label over unwrap functions and counts, topk over them, quantile,
// rate_counter, grouped sums, max, avg, a range shorter than the step) over
// refreshes, re-runs, widened windows, other steps and a grid off whole
// seconds give the uncached proxy's answer byte for byte; with a value Loki's
// conversion rejects in a stored window, in the fresh part only, and a nan.
//
// conformance: quality/query-range-settled-steps, quality/unwrap-query-range-extent-cache, semantics/unwrap-over-time-range-windows, semantics/unwrap-conversion-error, loki_api_v1_query_range
func TestQueryRangeStepsMatchUncached(t *testing.T) {
	s0 := time.Unix(1_700_001_000, 0).UTC()
	span := 4 * time.Hour
	end := s0.Add(3 * time.Hour)
	for _, tc := range []struct {
		name     string
		raw      func(at time.Duration, app string) string
		want400  bool
		wantNaN  bool
		minCache int
	}{
		{"clean", nil, false, false, 100},
		{"nan", func(at time.Duration, app string) string {
			if app == "pay" && at >= 100*time.Minute && at < 101*time.Minute {
				return "NaN"
			}
			return ""
		}, false, true, 50},
		// The first window ends at 3h: a rejected value at 3h01m is read only in
		// the fresh part of a moved window, after its steps were stored.
		{"rejected value only in the fresh part", func(at time.Duration, app string) string {
			if app == "pay" && at >= 181*time.Minute && at < 182*time.Minute {
				return "x7"
			}
			return ""
		}, true, false, 20},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rows := queryStepsRows(s0, span, tc.raw)
			outcomes, withData, nans, rejected := runQueryStepsDifferential(t, rows, queryStepsQueries, queryStepsWindows(end))
			t.Logf("outcomes %v; %d answers with data (%d with NaN), %d with Loki's 400", outcomes, withData, nans, rejected)
			if withData < len(queryStepsQueries)*5 || tc.want400 != (rejected > 0) || tc.wantNaN != (nans > 0) {
				t.Fatalf("%d answers with data (%d with NaN), %d with Loki's 400: the run proves little", withData, nans, rejected)
			}
			if outcomes["hit"]+outcomes["partial"] < tc.minCache || outcomes["unreadable"] != 0 {
				t.Fatalf("the moved windows must be answered from settled steps: %v", outcomes)
			}
		})
	}
}

// TestQueryRangeStepsLokiWindows: answered from settled steps, the window
// sums folded without the pod are Loki's (t-range, t] sums per step
// (pkg/logql/range_vector.go:102-189 and 371-420; the fold,
// pkg/logql/evaluator.go VectorAggEvaluator.Next), computed apart from the
// proxy over the same rows, for every window of a refresh.
//
// conformance: quality/query-range-settled-steps, semantics/unwrap-over-time-range-windows, loki_api_v1_query_range
func TestQueryRangeStepsLokiWindows(t *testing.T) {
	s0 := time.Unix(1_700_001_000, 0).UTC()
	rows := queryStepsRows(s0, 4*time.Hour, nil)
	srv, _ := newExtentShim(t, (&vlEmulator{t: t, rows: rows, field: "amount", version: "v1.50.0"}).handler())
	p := newExtentTestProxy(t, srv.URL, true, 256<<20)
	podRE := regexp.MustCompile(`,pod="[^"]*"`)
	group := func(stream string) string { return podRE.ReplaceAllString(stream, "") }
	for _, tc := range []struct{ fn, outer, query string }{
		{"max_over_time", "sum", `sum without (pod) (max_over_time({env="production"} | logfmt amount | unwrap amount [5m]))`},
		{"sum_over_time", "max", `max without (pod) (sum_over_time({env="production"} | logfmt amount | unwrap amount [5m]))`},
	} {
		start := s0.Add(90 * time.Minute)
		for i := 0; i < 6; i++ {
			from := start.Add(time.Duration(i) * 10 * time.Second)
			to := from.Add(time.Hour)
			code, body := extentQueryRange(t, p, tc.query, from, to, 10*time.Second, nil)
			if code != http.StatusOK {
				t.Fatalf("%s: %d %s", tc.query, code, body)
			}
			var times []int64
			for at := from; !at.After(to); at = at.Add(10 * time.Second) {
				times = append(times, at.UnixNano())
			}
			want := lokiOverTimeGroups(rows, tc.fn, tc.outer, int64(5*time.Minute), 0, times, group)
			var resp struct {
				Data struct {
					Result []struct {
						Metric map[string]string `json:"metric"`
						Values [][2]any          `json:"values"`
					} `json:"result"`
				} `json:"data"`
			}
			if err := json.Unmarshal(body, &resp); err != nil {
				t.Fatal(err)
			}
			if len(resp.Data.Result) != len(want) {
				t.Fatalf("%s window %d: %d series, Loki has %d", tc.query, i, len(resp.Data.Result), len(want))
			}
			for _, series := range resp.Data.Result {
				key := fmt.Sprintf(`{app=%q,env="production",level=%q}`, series.Metric["app"], series.Metric["level"])
				points := want[key]
				if len(series.Values) != len(points) {
					t.Fatalf("%s window %d %s: %d points, Loki has %d", tc.query, i, key, len(series.Values), len(points))
				}
				for _, pt := range series.Values {
					ts := int64(pt[0].(float64)) * int64(time.Second)
					got, _ := strconv.ParseFloat(pt[1].(string), 64)
					if w, ok := points[ts]; !ok || math.Abs(got-w) > 1e-9*math.Max(1, math.Abs(w)) {
						t.Fatalf("%s window %d %s at %d: %v, Loki %v (present %v)", tc.query, i, key, ts, got, w, ok)
					}
				}
			}
		}
	}
	if o := queryStepsOutcomes(t, p); o["partial"] < 8 {
		t.Fatalf("the moved windows must be answered from settled steps: %v", o)
	}
}

// queryRangeRequests reads the proxy's count of query_range answers with
// status code.
func queryRangeRequests(t testing.TB, p *Proxy, code int) int {
	t.Helper()
	rec := httptest.NewRecorder()
	p.metrics.Handler(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	prefix := `loki_vl_proxy_requests_total{system="loki",direction="downstream",endpoint="query_range",route="/loki/api/v1/query_range",status="` + strconv.Itoa(code) + `"} `
	for _, line := range strings.Split(rec.Body.String(), "\n") {
		if rest, ok := strings.CutPrefix(line, prefix); ok {
			n, _ := strconv.Atoi(rest)
			return n
		}
	}
	return 0
}

// TestQueryRangeStepsOnlyPerStepAnswers: an answer that is not a function of
// each step's window alone is never kept or merged: sort_desc orders series
// by their last value over the whole window, a Logs Drilldown request may
// keep the busiest series of the whole window, and a query holding both
// parsers is translated after a probe of the window. Every
// answer is the uncached one, and each request is counted once.
//
// conformance: quality/query-range-settled-steps, loki_api_v1_query_range
func TestQueryRangeStepsOnlyPerStepAnswers(t *testing.T) {
	s0 := time.Unix(1_700_001_000, 0).UTC()
	rows := queryStepsRows(s0, 4*time.Hour, nil)
	drilldown := http.Header{"X-Query-Tags": {"Source=grafana-lokiexplore-app"}}
	for _, tc := range []struct {
		name   string
		query  string
		header http.Header
	}{
		{"sort_desc", `sort_desc(sum by (app) (count_over_time({env="production"}[2m])))`, nil},
		{"drilldown", `sum by (app) (count_over_time({env="production"}[2m]))`, drilldown},
		{"both parsers", `sum by (app) (sum_over_time({env="production"} | json | logfmt | unwrap amount [2m]))`, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			onSrv, _ := newExtentShim(t, (&vlEmulator{t: t, rows: rows, field: "amount", version: "v1.50.0"}).handler())
			offSrv, _ := newExtentShim(t, (&vlEmulator{t: t, rows: rows, field: "amount", version: "v1.50.0"}).handler())
			on := newExtentTestProxy(t, onSrv.URL, true, 256<<20)
			off := newExtentTestProxy(t, offSrv.URL, false, 256<<20)
			start := s0.Add(90 * time.Minute)
			for i := 0; i < 4; i++ {
				from := start.Add(time.Duration(i) * 10 * time.Second)
				wantCode, want := extentQueryRange(t, off, tc.query, from, from.Add(time.Hour), 10*time.Second, tc.header)
				gotCode, got := extentQueryRange(t, on, tc.query, from, from.Add(time.Hour), 10*time.Second, tc.header)
				if gotCode != wantCode || !sameQueryStepsAnswer(got, want) {
					t.Fatalf("window %d: %d %s\nwithout: %d %s", i, gotCode, got, wantCode, want)
				}
			}
			if o := queryStepsOutcomes(t, on); o["hit"]+o["partial"]+o["stored"] != 0 {
				t.Fatalf("steps kept or merged: %v", o)
			}
			if n := queryRangeRequests(t, on, http.StatusOK) + queryRangeRequests(t, on, http.StatusBadRequest); n != 4 {
				t.Fatalf("%d requests counted, 4 sent", n)
			}
		})
	}
}

// TestQueryRangeStepsScopeAndAccounting: settled steps are kept per tenant
// (one tenant's steps never answer another's request), a Grafana refresh
// whose start stays on the step grid while its end moves less than a step
// is answered from them, the steps newer than -query-range-freshness are
// evaluated again (a line written there shows on the next refresh), and
// every request is counted once, the ones answered from steps too.
//
// conformance: quality/query-range-settled-steps, tenant-numeric-routing, loki_api_v1_query_range
func TestQueryRangeStepsScopeAndAccounting(t *testing.T) {
	s0 := time.Unix(1_700_001_000, 0).UTC()
	rows := queryStepsRows(s0, 4*time.Hour, nil)
	emuOn := &vlEmulator{t: t, rows: rows, field: "amount", version: "v1.50.0"}
	emuOff := &vlEmulator{t: t, rows: rows, field: "amount", version: "v1.50.0"}
	onSrv, _ := newExtentShim(t, emuOn.handler())
	offSrv, _ := newExtentShim(t, emuOff.handler())
	on := newExtentTestProxy(t, onSrv.URL, true, 256<<20)
	off := newExtentTestProxy(t, offSrv.URL, false, 256<<20)
	query := `sum without (pod) (max_over_time({env="production"} | logfmt amount | unwrap amount [5m]))`
	tenant := func(id string) http.Header { return http.Header{"X-Scope-Orgid": {id}} }
	check := func(label string, from, to time.Time, h http.Header) {
		t.Helper()
		wantCode, want := extentQueryRange(t, off, query, from, to, 10*time.Second, h)
		gotCode, got := extentQueryRange(t, on, query, from, to, 10*time.Second, h)
		if gotCode != http.StatusOK || gotCode != wantCode || !bytes.Equal(got, want) {
			t.Fatalf("%s: %d %s\nwithout: %d %s", label, gotCode, got, wantCode, want)
		}
	}
	start := s0.Add(90 * time.Minute)
	// Steps up to the horizon are kept, the newer ones read again.
	horizon := start.Add(50 * time.Minute)
	on.queryRangeFreshness = time.Since(horizon)
	check("tenant a", start, start.Add(time.Hour), tenant("a"))
	before := queryStepsOutcomes(t, on)
	check("tenant b, moved", start.Add(10*time.Second), start.Add(time.Hour+10*time.Second), tenant("b"))
	if o := queryStepsOutcomes(t, on); o["partial"]+o["hit"] != before["partial"]+before["hit"] {
		t.Fatalf("tenant b was answered from tenant a's steps: %v", o)
	}
	// Grafana refreshes: the start on the step grid, the end a few seconds on.
	for i, d := range []time.Duration{3 * time.Second, 7 * time.Second, 12 * time.Second, 19 * time.Second} {
		check(fmt.Sprintf("refresh %d", i), start, start.Add(time.Hour+d), tenant("a"))
	}
	if o := queryStepsOutcomes(t, on); o["partial"]+o["hit"] < before["partial"]+before["hit"]+4 {
		t.Fatalf("the refreshes inside a step were not answered from steps: %v", o)
	}
	// A line written after the horizon shows on the next request.
	late := emuRow{ts: start.Add(55 * time.Minute).UnixNano(), stream: `{app="pay",env="production",level="info",pod="late"}`, value: 1e6}
	emuOn.rows = append(append([]emuRow(nil), rows...), late)
	emuOff.rows = emuOn.rows
	check("after a line past the horizon", start.Add(10*time.Second), start.Add(time.Hour+10*time.Second), tenant("a"))
	check("again", start.Add(20*time.Second), start.Add(time.Hour+20*time.Second), tenant("a"))
	if got := queryRangeRequests(t, on, http.StatusOK); got != 8 {
		t.Fatalf("%d requests counted, 8 sent", got)
	}
}

// TestQueryRangeStepsSeriesLimit: Loki's series limit applies to the whole
// window. A series in the newest part only (the older series stopped before
// it), merged with the stored steps, makes an answer with more series than
// the limit: the proxy evaluates the whole window and answers as without
// steps.
//
// conformance: quality/query-range-settled-steps, loki_api_v1_query_range
func TestQueryRangeStepsSeriesLimit(t *testing.T) {
	s0 := time.Unix(1_700_001_000, 0).UTC()
	var rows []emuRow
	for at := time.Duration(0); at < 3*time.Hour; at += 30 * time.Second {
		if at < 120*time.Minute {
			rows = append(rows, emuRow{ts: s0.Add(at).UnixNano(), stream: `{app="pay",env="production",level="info",pod="a"}`, value: 1})
		}
		if at >= 150*time.Minute {
			rows = append(rows, emuRow{ts: s0.Add(at).UnixNano(), stream: `{app="auth",env="production",level="info",pod="b"}`, value: 2})
		}
	}
	onSrv, _ := newExtentShim(t, (&vlEmulator{t: t, rows: rows, field: "amount", version: "v1.50.0"}).handler())
	offSrv, _ := newExtentShim(t, (&vlEmulator{t: t, rows: rows, field: "amount", version: "v1.50.0"}).handler())
	on := newExtentTestProxy(t, onSrv.URL, true, 256<<20)
	off := newExtentTestProxy(t, offSrv.URL, false, 256<<20)
	on.maxStatsQuerySeries, off.maxStatsQuerySeries = 1, 1
	query := `sum by (app) (sum_over_time({env="production"} | logfmt | unwrap amount [5m]))`
	for i, from := range []time.Time{s0.Add(60 * time.Minute), s0.Add(80 * time.Minute), s0.Add(100 * time.Minute)} {
		wantCode, want := extentQueryRange(t, off, query, from, from.Add(time.Hour), 10*time.Second, nil)
		gotCode, got := extentQueryRange(t, on, query, from, from.Add(time.Hour), 10*time.Second, nil)
		if gotCode != wantCode || !bytes.Equal(got, want) {
			t.Fatalf("window %d: %d %s\nwithout: %d %s", i, gotCode, got, wantCode, want)
		}
		if i == 2 && wantCode != http.StatusBadRequest {
			t.Fatalf("the last window must exceed the limit: %d %s", wantCode, want)
		}
	}
	if o := queryStepsOutcomes(t, on); o["partial"] < 1 || o["fallback"] < 1 {
		t.Fatalf("a window answered from steps, then the whole window evaluated: %v", o)
	}
}

// queryStepsTestGrid is a grid of whole seconds, queryStepsTestWhole one off them by 250 ms.
var (
	queryStepsTestGrid  = queryStepsGrid{step: int64(10 * time.Second), phase: 0}
	queryStepsTestWhole = queryStepsGrid{step: int64(10 * time.Second), phase: int64(250 * time.Millisecond)}
)

// queryStepsRoundTripCases: the answers the round trip is pinned on, also the
// seeds of FuzzQueryStepsRoundTrip.
var queryStepsRoundTripCases = []struct {
	name string
	g    queryStepsGrid
	from int64
	to   int64
	body string
	ok   bool
}{
	{"status first", queryStepsTestGrid, 170000000, 170000002, `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"app":"a"},"values":[[1700000000,"1"],[1700000020,"2.5"]]},{"metric":{"app":"b"},"values":[[1700000010,"NaN"]]}]}}`, true},
	{"data first", queryStepsTestGrid, 170000000, 170000002, `{"data":{"result":[{"metric":{},"values":[[1700000000,"1"]]}],"resultType":"matrix"},"status":"success"}`, true},
	{"empty", queryStepsTestGrid, 170000000, 170000002, `{"status":"success","data":{"resultType":"matrix","result":[]}}`, true},
	{"fractional", queryStepsTestWhole, 170000000, 170000002, `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"a":"x"},"values":[[1700000000.25,"1"]]}]}}`, true},
	{"cut to seconds", queryStepsTestWhole, 170000000, 170000002, `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"a":"x"},"values":[[1700000010,"1"]]}]}}`, true},
	{"stats field", queryStepsTestGrid, 170000000, 170000002, `{"status":"success","data":{"resultType":"matrix","result":[],"stats":{}}}`, true},
	{"off the grid", queryStepsTestGrid, 170000000, 170000002, `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{},"values":[[1700000005,"1"]]}]}}`, false},
	{"outside the range", queryStepsTestGrid, 170000000, 170000001, `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{},"values":[[1700000020,"1"]]}]}}`, false},
	{"series twice", queryStepsTestGrid, 170000000, 170000002, `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"a":"x"},"values":[[1700000000,"1"]]},{"metric":{"a":"x"},"values":[[1700000010,"1"]]}]}}`, false},
	{"vector", queryStepsTestGrid, 170000000, 170000002, `{"status":"success","data":{"resultType":"vector","result":[]}}`, false},
	{"error", queryStepsTestGrid, 170000000, 170000002, `{"status":"error","data":{"resultType":"matrix","result":[]}}`, false},
}

// TestQueryStepsAnswerRoundTrip: an answer is taken apart only when it can be
// written back exactly: both envelopes the evaluator writes, whole and
// fractional second timestamps and a grid off whole seconds; anything else
// (a field it does not know, a point off the grid or outside the range, a
// series twice) is left alone.
//
// conformance: quality/query-range-settled-steps
func TestQueryStepsAnswerRoundTrip(t *testing.T) {
	for _, tc := range queryStepsRoundTripCases {
		f, ok := parseQueryStepsAnswer([]byte(tc.body), tc.g, tc.from, tc.to)
		if ok != tc.ok {
			t.Fatalf("%s: parsed %v, want %v", tc.name, ok, tc.ok)
		}
		if !ok {
			continue
		}
		out := []byte(f.names[0])
		for i, s := range f.series {
			out = appendQueryStepsSeries(out, i, s)
		}
		if got := string(append(out, f.names[1]...)); got != tc.body {
			t.Fatalf("%s: written back as %s", tc.name, got)
		}
	}
}

// TestQueryRangeStepsRouteBoundary: the window evaluator answers a window of
// more than Loki's 11,000 points of buckets (a range the step does not
// divide, here [5m] at step 86.4s: 2.4s buckets) from raw rows, and a short
// missing range of the same query from buckets. Their float sums may differ
// in the last bits, so steps of one route are never merged with the other's:
// the moved window is evaluated whole, and every answer is the uncached one.
//
// conformance: quality/query-range-settled-steps, semantics/unwrap-over-time-range-windows
func TestQueryRangeStepsRouteBoundary(t *testing.T) {
	s0 := time.Unix(1_700_001_000, 0).UTC()
	rows := queryStepsRows(s0, 4*time.Hour, nil)
	end := s0.Add(4 * time.Hour)
	step := 86400 * time.Millisecond
	start := end.Add(-24 * time.Hour)
	windows := []extentWindow{
		{start, end, step},
		{start.Add(step), end.Add(step), step},
		{end.Add(-2 * time.Hour), end, step},
		{end.Add(-2*time.Hour + step), end.Add(step), step},
	}
	outcomes, withData, _, _ := runQueryStepsDifferential(t, rows, []string{
		`sum by (app) (sum_over_time({env="production"} | logfmt | unwrap amount [5m]))`,
	}, windows)
	t.Logf("outcomes %v, %d answers with data", outcomes, withData)
	if withData != len(windows) || outcomes["fallback"] < 1 || outcomes["partial"] < 1 {
		t.Fatalf("the long window must fall back to a whole evaluation and the short one be merged: %v", outcomes)
	}
}
