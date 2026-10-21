package proxy

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// instantVolumeResult is one decoded vector: the value of each series by its
// canonical label key, and the sample time they all carry.
type instantVolumeResult struct {
	values map[string]string
	stamps map[float64]bool
}

func decodeInstantVolume(t testing.TB, query string, body []byte) instantVolumeResult {
	t.Helper()
	var resp struct {
		Data struct {
			ResultType string `json:"resultType"`
			Result     []struct {
				Metric map[string]string `json:"metric"`
				Value  []any             `json:"value"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &resp); err != nil || resp.Data.ResultType != "vector" {
		t.Fatalf("%s: invalid vector (%v): %s", query, err, body)
	}
	out := instantVolumeResult{values: map[string]string{}, stamps: map[float64]bool{}}
	for _, series := range resp.Data.Result {
		key := canonicalLabelsKey(series.Metric)
		if _, dup := out.values[key]; dup {
			t.Fatalf("%s: duplicate series %s in %s", query, key, body)
		}
		stamp, _ := series.Value[0].(float64)
		out.stamps[stamp] = true
		out.values[key], _ = series.Value[1].(string)
	}
	return out
}

func runJSONVolumeQueryInstant(t testing.TB, p *Proxy, query string, at time.Time) (instantVolumeResult, *httptest.ResponseRecorder) {
	t.Helper()
	params := url.Values{"query": {query}, "time": {strconv.FormatInt(at.UnixNano(), 10)}}
	rec := httptest.NewRecorder()
	p.handleQuery(rec, httptest.NewRequest(http.MethodGet, "/loki/api/v1/query?"+params.Encode(), nil))
	if rec.Code != http.StatusOK {
		return instantVolumeResult{}, rec
	}
	return decodeInstantVolume(t, query, rec.Body.Bytes()), rec
}

// rawInstantVolume is the raw-row evaluator's answer, the reference the stats
// pushdown must equal.
func rawInstantVolume(t testing.TB, p *Proxy, query string, at time.Time) instantVolumeResult {
	t.Helper()
	plan, ok := compileOrderedJSONMetric(query)
	if !ok {
		t.Fatalf("%s: no ordered JSON plan", query)
	}
	series, err := p.collectOrderedJSONMetric(t.Context(), plan, at, at, time.Second)
	if err != nil {
		t.Fatalf("%s: raw evaluator: %v", query, err)
	}
	body, err := buildOrderedJSONMetric(t.Context(), plan, series, at, at, time.Second, false, p.orderedJSONMetricMaxBytes())
	if err != nil {
		t.Fatalf("%s: raw vector: %v", query, err)
	}
	return decodeInstantVolume(t, query, body)
}

// recordedVLCall is one request the proxy sent to the backend.
type recordedVLCall struct {
	path   string
	params url.Values
}

type vlCallRecorder struct {
	mu    sync.Mutex
	calls []recordedVLCall
}

func (r *vlCallRecorder) statsCalls() []recordedVLCall {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []recordedVLCall
	for _, call := range r.calls {
		if call.path == "/select/logsql/stats_query_range" {
			out = append(out, call)
		}
	}
	return out
}

// recordVLCalls fronts the backend at target with a proxy that records the
// parameters of every request.
func recordVLCalls(t testing.TB, target string) (string, *vlCallRecorder) {
	t.Helper()
	targetURL, err := url.Parse(target)
	if err != nil {
		t.Fatal(err)
	}
	rec := &vlCallRecorder{}
	reverse := httputil.NewSingleHostReverseProxy(targetURL)
	front := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		r.Body = io.NopCloser(bytes.NewReader(body))
		params, _ := url.ParseQuery(string(body))
		for k, v := range r.URL.Query() {
			params[k] = v
		}
		rec.mu.Lock()
		rec.calls = append(rec.calls, recordedVLCall{r.URL.Path, params})
		rec.mu.Unlock()
		reverse.ServeHTTP(w, r)
	}))
	t.Cleanup(front.Close)
	return front.URL, rec
}

// instantVolumeFixture is the rotating-shape fixture plus older lines out to
// more than a day before it, so every window length cuts through data.
func instantVolumeFixture(s0 time.Time) []jsonVolumeLine {
	lines := jsonVolumeFixture(s0)
	for i, off := range []time.Duration{-30 * time.Minute, -3 * time.Hour, -20 * time.Hour, -26 * time.Hour} {
		for j := 0; j < 6; j++ {
			lines = append(lines, jsonVolumeFixtureLine(s0.Add(off+time.Duration(j)*7*time.Second), i+j))
		}
	}
	return lines
}

func metricsBody(p *Proxy) string {
	rec := httptest.NewRecorder()
	p.metrics.Handler(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	return rec.Body.String()
}

// An instant `| json | drop __error__` volume is answered from one
// stats_query_range bucket (t-window, t] and never reads raw rows. The answer
// equals the Loki model and the raw-row evaluator for every window length,
// whole-second evaluation time, function and grouping, with parser-error lines
// kept by the dropped __error__ (a sub-millisecond time is covered by
// TestOrderedJSONInstantVolumeSubMillisecondSampleTime).
//
// conformance: semantics/json-instant-volume-stats
func TestOrderedJSONInstantVolumeUsesStatsBucket(t *testing.T) {
	s0 := time.Unix(1700000400, 0).UTC()
	lines := instantVolumeFixture(s0)
	for _, tc := range []struct {
		query   string
		fn      string
		groupBy []string
		window  time.Duration
	}{
		{`sum by (level, detected_level) (count_over_time({app="api"} | json | drop __error__ [5m]))`, "count_over_time", []string{"level", "detected_level"}, 5 * time.Minute},
		{`sum by (level) (rate({app="api"} | json | drop __error__, __error_details__ [90s]))`, "rate", []string{"level"}, 90 * time.Second},
		{`sum by (detected_level) (bytes_over_time({app="api"} | json | drop __error__ [1h]))`, "bytes_over_time", []string{"detected_level"}, time.Hour},
		{`sum by (level) (count_over_time({app="api"} | json | drop __error__ [24h]))`, "count_over_time", []string{"level"}, 24 * time.Hour},
	} {
		for _, at := range []time.Time{s0.Add(2 * time.Minute), s0.Add(5*time.Minute + 7*time.Second), s0.Add(10 * time.Minute), s0.Add(27 * time.Hour)} {
			t.Run(tc.query+"@"+strconv.FormatInt(at.Unix()-s0.Unix(), 10), func(t *testing.T) {
				srv, fake := newJSONVolumeFakeVL(t, lines)
				front, recorded := recordVLCalls(t, srv.URL)
				p := newSlidingTestProxy(t, front)

				ref := lokiJSONVolumeReference(lines, tc.fn, tc.groupBy, at, at, time.Second, tc.window)
				want := map[string]string{}
				for key, byTime := range ref {
					want[key] = byTime[at.Unix()]
				}
				got, rec := runJSONVolumeQueryInstant(t, p, tc.query, at)
				if rec.Code != http.StatusOK {
					t.Fatalf("status %d: %s", rec.Code, rec.Body)
				}
				if !reflect.DeepEqual(got.values, want) {
					t.Fatalf("result differs from the Loki reference\n got: %v\nwant: %v", got.values, want)
				}
				if len(got.values) > 0 && !reflect.DeepEqual(got.stamps, map[float64]bool{float64(at.Unix()): true}) {
					t.Fatalf("sample times %v, want the evaluation time %d", got.stamps, at.Unix())
				}
				if raw := rawInstantVolume(t, p, tc.query, at); !reflect.DeepEqual(got, raw) {
					t.Fatalf("stats answer differs from the raw evaluator\n got: %v\nraw: %v", got, raw)
				}

				fake.mu.Lock()
				// The raw evaluator above reads rows; the request itself must not.
				rawReads := fake.rawCalls
				fake.mu.Unlock()
				if rawReads != 1 {
					t.Fatalf("raw row reads %d: the request must read none and the reference evaluator one", rawReads)
				}
				stats := recorded.statsCalls()
				if len(stats) == 0 {
					t.Fatal("no stats_query_range call")
				}
				edge := at.Add(-tc.window)
				for _, call := range stats {
					if call.params.Get("start") != edge.UTC().Format(time.RFC3339Nano) ||
						call.params.Get("end") != at.Add(time.Nanosecond).UTC().Format(time.RFC3339Nano) ||
						call.params.Get("step") != strconv.FormatInt(int64(tc.window/time.Second), 10)+"s" {
						t.Fatalf("stats window %v, want one bucket (%s, %s]", call.params, edge, at)
					}
				}
				if !strings.Contains(metricsBody(p), `evaluator="vl_stats_buckets",reason="pushdown"} 1`) {
					t.Fatalf("pushdown not recorded:\n%s", metricsBody(p))
				}
			})
		}
	}
}

// Loki's instant window is (t-range, t]: a line exactly at t-range is outside,
// one at t is inside, and so are the lines a nanosecond to either side that
// the same rule decides. A window holding only an empty line still counts for
// bytes_over_time (value 0), as on the range path.
//
// conformance: semantics/json-instant-volume-stats
func TestOrderedJSONInstantVolumeWindowEdges(t *testing.T) {
	at := time.Unix(1700001000, 0).UTC()
	window := time.Minute
	lines := []jsonVolumeLine{
		{at.Add(-window - time.Nanosecond), "trace", `{"a":1}`},
		{at.Add(-window), "warn", `{"a":1}`},
		{at.Add(-window + time.Nanosecond), "info", `{"a":1}`},
		{at, "error", `{"a":1}`},
		{at.Add(time.Nanosecond), "debug", `{"a":1}`},
	}
	for _, tc := range []struct {
		name, query string
		lines       []jsonVolumeLine
		want        map[string]string
	}{
		{"count", `sum by (level) (count_over_time({app="api"} | json | drop __error__ [1m]))`, lines, map[string]string{
			canonicalLabelsKey(map[string]string{"level": "info"}):  "1",
			canonicalLabelsKey(map[string]string{"level": "error"}): "1",
		}},
		{"rate", `sum by (level) (rate({app="api"} | json | drop __error__ [1m]))`, lines, map[string]string{
			canonicalLabelsKey(map[string]string{"level": "info"}):  strconv.FormatFloat(1.0/60, 'f', -1, 64),
			canonicalLabelsKey(map[string]string{"level": "error"}): strconv.FormatFloat(1.0/60, 'f', -1, 64),
		}},
		{"only the excluded edge", `sum by (level) (count_over_time({app="api"} | json | drop __error__ [1m]))`, lines[:2], map[string]string{}},
		{"only the included end", `sum by (level) (count_over_time({app="api"} | json | drop __error__ [1m]))`, lines[3:4], map[string]string{
			canonicalLabelsKey(map[string]string{"level": "error"}): "1",
		}},
		{"empty line", `sum by (level) (bytes_over_time({app="api"} | json | drop __error__ [1m]))`, []jsonVolumeLine{{at, "error", ""}, {at.Add(-window), "warn", "{}"}}, map[string]string{
			canonicalLabelsKey(map[string]string{"level": "error"}): "0",
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv, fake := newJSONVolumeFakeVL(t, tc.lines)
			p := newSlidingTestProxy(t, srv.URL)
			got, rec := runJSONVolumeQueryInstant(t, p, tc.query, at)
			if rec.Code != http.StatusOK {
				t.Fatalf("status %d: %s", rec.Code, rec.Body)
			}
			if !reflect.DeepEqual(got.values, tc.want) {
				t.Fatalf("window edges\n got: %v\nwant: %v", got.values, tc.want)
			}
			fake.mu.Lock()
			rawReads, statsReads := fake.rawCalls, len(fake.statsCalls)
			fake.mu.Unlock()
			if rawReads != 0 || statsReads == 0 {
				t.Fatalf("answered from raw rows: raw=%d stats=%d", rawReads, statsReads)
			}
			if raw := rawInstantVolume(t, p, tc.query, at); !reflect.DeepEqual(got, raw) {
				t.Fatalf("stats answer differs from the raw evaluator\n got: %v\nraw: %v", got, raw)
			}
		})
	}
}

// The sample time is the evaluation time in seconds with millisecond
// precision, as Loki prints a vector sample.
//
// conformance: semantics/json-instant-volume-stats
func TestOrderedJSONInstantVolumeSampleTime(t *testing.T) {
	at := time.Unix(1700001000, 123_000_000).UTC()
	srv, _ := newJSONVolumeFakeVL(t, []jsonVolumeLine{{at, "info", `{"a":1}`}})
	p := newSlidingTestProxy(t, srv.URL)
	got, rec := runJSONVolumeQueryInstant(t, p, `sum by (level) (count_over_time({app="api"} | json | drop __error__ [1m]))`, at)
	if rec.Code != http.StatusOK || !reflect.DeepEqual(got.stamps, map[float64]bool{1700001000.123: true}) {
		t.Fatalf("status %d, sample times %v: %s", rec.Code, got.stamps, rec.Body)
	}
}

// Loki prints a vector sample time in seconds with millisecond precision, so a
// sub-millisecond evaluation time is cut to the millisecond on the stats route.
// The raw-row evaluator prints the full nanoseconds (a pre-existing difference
// from Loki on that route), so the two routes are compared on value, not on
// timestamp, for this case; the stats route's timestamp is asserted against
// Loki's own formatting.
//
// conformance: semantics/json-instant-volume-stats
func TestOrderedJSONInstantVolumeSubMillisecondSampleTime(t *testing.T) {
	at := time.Unix(1700001000, 123_456_789).UTC()
	query := `sum by (level) (count_over_time({app="api"} | json | drop __error__ [1m]))`
	srv, fake := newJSONVolumeFakeVL(t, []jsonVolumeLine{{at.Add(-10 * time.Second), "info", `{"a":1}`}, {at, "warn", `{"a":1}`}})
	p := newSlidingTestProxy(t, srv.URL)
	got, rec := runJSONVolumeQueryInstant(t, p, query, at)
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rec.Code, rec.Body)
	}
	wantStamp := float64(at.UnixMilli()) / 1e3
	if !reflect.DeepEqual(got.stamps, map[float64]bool{wantStamp: true}) || wantStamp != 1700001000.123 {
		t.Fatalf("sample times %v, want Loki's millisecond %v", got.stamps, wantStamp)
	}
	if !strings.Contains(rec.Body.String(), `[1700001000.123,"1"]`) {
		t.Fatalf("sample time is not printed with millisecond precision: %s", rec.Body)
	}
	raw := rawInstantVolume(t, p, query, at)
	if !reflect.DeepEqual(got.values, raw.values) {
		t.Fatalf("stats values differ from the raw evaluator\n got: %v\nraw: %v", got.values, raw.values)
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if len(fake.statsCalls) == 0 {
		t.Fatal("not answered from a stats bucket")
	}
}

// An ungrouped sum answers one series without labels. The pushdown covers it
// when a label filter after the parser keeps unparsed lines out (the stats
// query unpacks only the filter keys); the same query answers from one stats
// bucket and equals the Loki model and the raw evaluator. (An ungrouped sum
// with no filter on a parsed label never reaches this route: the plan proves
// the JSON stage cannot change the count and the native instant route answers.)
//
// conformance: semantics/json-instant-volume-stats
func TestOrderedJSONInstantVolumeUngroupedSum(t *testing.T) {
	if plan, ok := compileOrderedJSONMetric(`sum(count_over_time({app="api"} | json [1h]))`); !ok || plan.withoutJSON == "" {
		t.Fatal("an ungrouped sum without a parsed-label filter is expected to stay on the native route")
	}
	s0 := time.Unix(1700000400, 0).UTC()
	rows := filterPushdownFixture(s0)
	for _, tc := range []struct {
		name, query string
	}{
		{"count", `sum(count_over_time({app="api"} | json | pipeline="logs/loki" [1h]))`},
		{"count short window", `sum(count_over_time({app="api"} | json | pipeline="logs/loki" [2m]))`},
		{"rate", `sum(rate({app="api"} | json | pipeline=~"logs/.*" [5m]))`},
	} {
		for _, at := range []time.Time{s0.Add(4 * time.Minute), s0.Add(10 * time.Minute)} {
			t.Run(tc.name+"@"+strconv.FormatInt(at.Unix()-s0.Unix(), 10), func(t *testing.T) {
				srv, fake := newPushdownFakeVL(t, rows, nil)
				p := newFilterPushdownProxy(t, srv.URL)
				fake.stored = p.labelTranslator.ToVL
				plan, ok := compileOrderedJSONMetric(tc.query)
				if !ok || !plan.pushdown || plan.grouping != nil {
					t.Fatalf("%s: want an ungrouped pushdown plan", tc.query)
				}
				want := map[string]string{}
				for key, byTime := range lokiPushdownReference(t, plan, rows, at, at, time.Second) {
					want[key] = byTime[at.Unix()]
				}
				if len(want) != 1 {
					t.Fatalf("reference holds %d series, want the one ungrouped series: %v", len(want), want)
				}
				got, rec := runJSONVolumeQueryInstant(t, p, tc.query, at)
				if rec.Code != http.StatusOK {
					t.Fatalf("status %d: %s", rec.Code, rec.Body)
				}
				if !reflect.DeepEqual(got.values, want) {
					t.Fatalf("result differs from the Loki reference\n got: %v\nwant: %v", got.values, want)
				}
				if !reflect.DeepEqual(got.stamps, map[float64]bool{float64(at.Unix()): true}) {
					t.Fatalf("sample times %v, want the evaluation time %d", got.stamps, at.Unix())
				}
				fake.mu.Lock()
				stats, rawReads := len(fake.stats), fake.raw
				fake.mu.Unlock()
				if stats != 1 || rawReads != 0 {
					t.Fatalf("expected one stats call and no raw read: stats=%d raw=%d", stats, rawReads)
				}
				if raw := rawInstantVolume(t, p, tc.query, at); !reflect.DeepEqual(got, raw) {
					t.Fatalf("stats answer differs from the raw evaluator\n got: %v\nraw: %v", got, raw)
				}
			})
		}
	}
}

// A backend error on the stats bucket reaches the client as it does on the
// range route for the same failure: the same status and body, never a partial
// vector and never a silent fall back to counting raw rows.
//
// conformance: semantics/json-instant-volume-stats
func TestOrderedJSONInstantVolumeBackendErrorMatchesRangeRoute(t *testing.T) {
	at := time.Unix(1700001000, 0).UTC()
	query := `sum by (level) (count_over_time({app="api"} | json | drop __error__ [1m]))`
	for _, status := range []int{http.StatusInternalServerError, http.StatusBadGateway, http.StatusServiceUnavailable} {
		t.Run(strconv.Itoa(status), func(t *testing.T) {
			var mu sync.Mutex
			rawReads := 0
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_ = r.ParseForm()
				switch {
				case r.URL.Path == "/select/logsql/stats_query_range":
					http.Error(w, "backend stats failure", status)
				case strings.HasSuffix(r.Form.Get("query"), " | limit 1"): // the parse-risk probes find nothing
				default:
					mu.Lock()
					rawReads++
					mu.Unlock()
				}
			}))
			defer srv.Close()
			p := newSlidingTestProxy(t, srv.URL)
			_, instant := runJSONVolumeQueryInstant(t, p, query, at)

			rangeRec := httptest.NewRecorder()
			rangeParams := url.Values{"query": {query}, "start": {strconv.FormatInt(at.UnixNano(), 10)}, "end": {strconv.FormatInt(at.UnixNano(), 10)}, "step": {"60"}}
			p.handleQueryRange(rangeRec, httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+rangeParams.Encode(), nil))

			if instant.Code < 500 || strings.Contains(instant.Body.String(), `"result"`) {
				t.Fatalf("instant: status %d: %s", instant.Code, instant.Body)
			}
			if instant.Code != rangeRec.Code || instant.Body.String() != rangeRec.Body.String() {
				t.Fatalf("backend error differs\ninstant: %d %s\n range: %d %s", instant.Code, instant.Body, rangeRec.Code, rangeRec.Body)
			}
			mu.Lock()
			defer mu.Unlock()
			if rawReads != 0 {
				t.Fatalf("a stats error fell back to %d raw row reads", rawReads)
			}
		})
	}
}

// A line the two parsers read differently keeps the exact raw evaluator, and a
// plan the pushdown does not cover never asks for buckets.
//
// conformance: semantics/json-instant-volume-stats
func TestOrderedJSONInstantVolumeFallbacksReadRawRows(t *testing.T) {
	s0 := time.Unix(1700000400, 0).UTC()
	at := s0.Add(time.Minute)
	t.Run("parse disagreement", func(t *testing.T) {
		lines := []jsonVolumeLine{{s0.Add(10 * time.Second), "info", `{"msg":"ok"}`}, {s0.Add(20 * time.Second), "", `{"level":"error","msg": truncated`}}
		srv, fake := newJSONVolumeFakeVL(t, lines)
		p := newSlidingTestProxy(t, srv.URL)
		got, rec := runJSONVolumeQueryInstant(t, p, `sum by (level) (count_over_time({app="api"} | json | drop __error__[1m]))`, at)
		want := map[string]string{
			canonicalLabelsKey(map[string]string{"level": "info"}):  "1",
			canonicalLabelsKey(map[string]string{"level": "error"}): "1",
		}
		if rec.Code != http.StatusOK || !reflect.DeepEqual(got.values, want) {
			t.Fatalf("status %d\n got: %v\nwant: %v", rec.Code, got.values, want)
		}
		fake.mu.Lock()
		defer fake.mu.Unlock()
		if fake.rawCalls != 1 || len(fake.guardCalls) != 1 {
			t.Fatalf("expected the guard to keep the raw evaluator: raw=%d guard=%d", fake.rawCalls, len(fake.guardCalls))
		}
		if !strings.Contains(metricsBody(p), `evaluator="raw_rows",reason="probe"} 1`) {
			t.Fatalf("probe fallback not recorded:\n%s", metricsBody(p))
		}
	})
	t.Run("ineligible plan", func(t *testing.T) {
		query := `sum by (level) (count_over_time({app="api"} | json [1m]))` // parser errors are kept: no pushdown
		if plan, ok := compileOrderedJSONMetric(query); !ok || plan.pushdown {
			t.Fatalf("%s: want an ordered JSON plan without pushdown", query)
		}
		srv, fake := newJSONVolumeFakeVL(t, []jsonVolumeLine{{s0.Add(10 * time.Second), "info", `{"a":1}`}, {s0.Add(20 * time.Second), "warn", `{"a":2}`}})
		p := newSlidingTestProxy(t, srv.URL)
		got, rec := runJSONVolumeQueryInstant(t, p, query, at)
		if rec.Code != http.StatusOK || len(got.values) != 2 {
			t.Fatalf("status %d: %s", rec.Code, rec.Body)
		}
		fake.mu.Lock()
		defer fake.mu.Unlock()
		if fake.rawCalls != 1 || len(fake.statsCalls) != 0 {
			t.Fatalf("expected one raw read and no stats call: raw=%d stats=%q", fake.rawCalls, fake.statsCalls)
		}
		if !strings.Contains(metricsBody(p), `evaluator="raw_rows",reason="ineligible"} 1`) {
			t.Fatalf("ineligible fallback not recorded:\n%s", metricsBody(p))
		}
	})
}

// The series limit and the response-size limit apply to the instant answer as
// they do to the range one: above the limit the request fails with the limit's
// error instead of a partial vector, and exactly at it the answer passes.
//
// conformance: semantics/json-instant-volume-stats
func TestOrderedJSONInstantVolumeLimits(t *testing.T) {
	s0 := time.Unix(1700000400, 0).UTC()
	lines := jsonVolumeFixture(s0)
	at := s0.Add(10 * time.Minute)
	query := `sum by (level) (count_over_time({app="api"} | json | drop __error__ [10m]))`
	srv, fake := newJSONVolumeFakeVL(t, lines)

	p := newSlidingTestProxy(t, srv.URL)
	full, rec := runJSONVolumeQueryInstant(t, p, query, at)
	if rec.Code != http.StatusOK || len(full.values) < 3 {
		t.Fatalf("status %d, series %d: %s", rec.Code, len(full.values), rec.Body)
	}

	p = newSlidingTestProxy(t, srv.URL)
	p.maxStatsQuerySeries = len(full.values) - 1
	if _, rec = runJSONVolumeQueryInstant(t, p, query, at); rec.Code < 400 || strings.Contains(rec.Body.String(), `"result"`) ||
		!strings.Contains(rec.Body.String(), "maximum number of series ("+strconv.Itoa(len(full.values)-1)+") reached") {
		t.Fatalf("series overflow: status %d: %s", rec.Code, rec.Body)
	}
	p = newSlidingTestProxy(t, srv.URL)
	p.maxStatsQuerySeries = len(full.values)
	if got, rec := runJSONVolumeQueryInstant(t, p, query, at); rec.Code != http.StatusOK || !reflect.DeepEqual(got, full) {
		t.Fatalf("exactly the limit's series: status %d: %s", rec.Code, rec.Body)
	}

	// The same byte limit fails the range and the instant answer alike, naming
	// the flag that raises it.
	p = newSlidingTestProxy(t, srv.URL)
	p.execLimits.BufferedBackendBodyBytes = 64
	_, instant := runJSONVolumeQueryInstant(t, p, query, at)
	rangeRec := httptest.NewRecorder()
	rangeParams := url.Values{"query": {query}, "start": {strconv.FormatInt(at.UnixNano(), 10)}, "end": {strconv.FormatInt(at.UnixNano(), 10)}, "step": {"60"}}
	p.handleQueryRange(rangeRec, httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+rangeParams.Encode(), nil))
	for name, rec := range map[string]*httptest.ResponseRecorder{"instant": instant, "range": rangeRec} {
		if rec.Code < 400 || strings.Contains(rec.Body.String(), `"result"`) || !strings.Contains(rec.Body.String(), "-backend-max-buffered-response-bytes") {
			t.Fatalf("%s response size limit: status %d: %s", name, rec.Code, rec.Body)
		}
	}
	if instant.Code != rangeRec.Code || instant.Body.String() != rangeRec.Body.String() {
		t.Fatalf("response size limit differs\ninstant: %d %s\n range: %d %s", instant.Code, instant.Body, rangeRec.Code, rangeRec.Body)
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if fake.rawCalls != 0 {
		t.Fatalf("limits are enforced on the stats answer, not by a raw read: raw=%d", fake.rawCalls)
	}
}

// BenchmarkOrderedJSONInstantVolume compares what the proxy spends answering
// an instant `| json | drop __error__` volume over a 1h window from one stats
// bucket and from the raw rows the same corpus holds (18000 JSON lines, three
// levels). The backend is canned, so only the proxy's own cost is measured;
// compare runs with benchstat.
func BenchmarkOrderedJSONInstantVolume(b *testing.B) {
	at := time.Unix(1700004000, 0).UTC()
	levels := []string{"info", "warn", "error"}
	var rows bytes.Buffer
	for i := 0; i < 18000; i++ {
		level := levels[i%3]
		row, _ := json.Marshal(map[string]string{
			"_time": at.Add(-time.Hour + time.Duration(i)*200*time.Millisecond).Format(time.RFC3339Nano), "_stream": `{app="api",level="` + level + `"}`,
			"app": "api", "level": level, "_msg": `{"level":"` + level + `","n":` + strconv.Itoa(i) + `,"msg":"benchmark corpus line"}`,
		})
		rows.Write(row)
		rows.WriteByte('\n')
	}
	var stats strings.Builder
	stats.WriteString(`{"status":"success","data":{"resultType":"matrix","result":[`)
	for i, level := range levels {
		if i > 0 {
			stats.WriteByte(',')
		}
		stats.WriteString(`{"metric":{"__name__":"c","level":"` + level + `"},"values":[[` + strconv.FormatInt(at.Add(-time.Hour).Unix(), 10) + `,"6000"]]}`)
	}
	stats.WriteString(`]}}`)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		switch {
		case r.URL.Path == "/select/logsql/stats_query_range":
			_, _ = w.Write([]byte(stats.String()))
		case strings.HasSuffix(r.Form.Get("query"), " | limit 1"): // the parse-risk probe finds nothing
		default:
			_, _ = w.Write(rows.Bytes())
		}
	}))
	defer srv.Close()
	p, err := New(Config{BackendURL: srv.URL, Cache: cache.New(60*time.Second, 1000), LogLevel: "error"})
	if err != nil {
		b.Fatal(err)
	}
	p.storeBackendVersion("v1.50.0", "v1.50.0")
	plan, ok := compileOrderedJSONMetric(`sum by (level) (count_over_time({app="api"} | json | drop __error__ [1h]))`)
	if !ok || !plan.pushdown {
		b.Fatal("no pushdown plan")
	}
	b.Run("stats", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			body, served, _, err := p.orderedJSONStatsBucketsWithReason(b.Context(), plan, at, at, time.Second, true)
			if err != nil || !served || len(body) == 0 {
				b.Fatalf("served=%v err=%v", served, err)
			}
		}
	})
	b.Run("raw", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			series, err := p.collectOrderedJSONMetric(b.Context(), plan, at, at, time.Second)
			if err != nil {
				b.Fatal(err)
			}
			if _, err = buildOrderedJSONMetric(b.Context(), plan, series, at, at, time.Second, false, p.orderedJSONMetricMaxBytes()); err != nil {
				b.Fatal(err)
			}
		}
	})
}
