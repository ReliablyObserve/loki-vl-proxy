package proxy

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// conformance: loki_api_v1_query_range, window-bounds-and-step-alignment, quality/query-range-settled-steps
func TestOrderedJSONSettledStepsReuse(t *testing.T) {
	base := time.Unix(1700000400, 0).UTC()
	for _, shape := range append(slices.Clone(dlvBaselineShapes), dlvBaselineShape{name: "mixed", pipeline: "| logfmt | json | drop __error__, __error_details__ | keep detected_level", tagged: true}) {
		t.Run(shape.name, func(t *testing.T) {
			backend, fixture := newDLVBaselineBackend(t, shape, base)
			var mu sync.Mutex
			type span struct{ from, to time.Time }
			var spans []span
			observed := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_ = r.ParseForm()
				from, e1 := time.Parse(time.RFC3339Nano, r.Form.Get("start"))
				to, e2 := time.Parse(time.RFC3339Nano, r.Form.Get("end"))
				if e1 == nil && e2 == nil {
					mu.Lock()
					spans = append(spans, span{from, to})
					mu.Unlock()
				}
				backend.Config.Handler.ServeHTTP(w, r)
			}))
			defer observed.Close()
			p := newDLVBaselineProxy(t, observed.URL, false)
			p.queryRangeExtentCache, p.queryRangeExtentCacheTTL = true, time.Hour
			for i, shift := range []time.Duration{0, time.Minute, -time.Minute, 0} {
				from, to := base.Add(time.Minute+shift), base.Add(4*time.Minute+shift)
				target, headers := dlvBaselineRequest(shape, from, to)
				got := dlvBaselineReadMatrix(t, doCompatProxyRequest(p, target, headers))
				want := dlvBaselineWant(fixture.rows, shape, from, to, "10", "alpha")
				if !reflect.DeepEqual(got, want) {
					t.Fatalf("window %d: got %v want %v", i, got, want)
				}
				p.statsExtentStores.wait()
				mu.Lock()
				for _, s := range spans {
					// A read of this window lies within its range, the minute before from (the [1m] range) to
					// end; a call the strict split cancelled in the window before can still arrive now, reading
					// that window (consecutive windows differ by a minute or two at both ends).
					ofThisWindow := !s.from.Before(from.Add(-time.Minute)) && !s.to.After(to.Add(time.Nanosecond))
					if i > 0 && ofThisWindow && s.to.Sub(s.from) > 2*time.Minute+time.Nanosecond {
						t.Errorf("moved window reread settled history: %s to %s", s.from.Sub(base), s.to.Sub(base))
					}
				}
				spans = nil
				mu.Unlock()
			}
			// A different evaluation phase must not borrow the old points.
			target, headers := dlvBaselineRequest(shape, base.Add(time.Minute), base.Add(4*time.Minute))
			u, _ := url.Parse(target)
			q := u.Query()
			q.Set("start", "1700000461000000000")
			q.Set("end", "1700000641000000000")
			u.RawQuery = q.Encode()
			before := fixture.calls.Load()
			if r := doCompatProxyRequest(p, u.String(), headers); r.Code != http.StatusOK || fixture.calls.Load() <= before {
				t.Fatal("different phase did not evaluate its own window")
			}
		})
	}
}

// A mixed range orders labels by name/value pairs. Groups that occur in
// separate windows must keep that order when previously settled points join.
// conformance: semantics/two-parsers-keep-one, quality/query-range-settled-steps
func TestOrderedMixedSettledStepsSeriesOrder(t *testing.T) {
	at := time.Unix(1700000400, 0).UTC()
	var rows []pushdownRow
	for i, group := range []map[string]string{{}, {"x": "a"}, {"x": "z"}, {"x": "a!"}, {"x": "aa", "y": "1"}} {
		ts := at
		if i >= 3 {
			ts = at.Add(2 * time.Minute)
		}
		stream := map[string]string{"app": "api"}
		for name, value := range group {
			stream[name] = value
		}
		rows = append(rows, pushdownRow{ts: ts, stream: stream, msg: "{}"})
	}
	backend, fake := newPushdownFakeVL(t, rows, func(key string) string { return key })
	on, off := newDLVBaselineProxy(t, backend.URL, false), newDLVBaselineProxy(t, backend.URL, false)
	on.queryRangeExtentCache, on.queryRangeExtentCacheTTL = true, time.Hour
	get := func(p *Proxy, end time.Time) *httptest.ResponseRecorder {
		params := url.Values{"query": {`sum by (x,y) (count_over_time({app="api"} | logfmt | json | drop __error__ [1m]))`},
			"start": {at.Format(time.RFC3339Nano)}, "end": {end.Format(time.RFC3339Nano)}, "step": {"60"}}
		return doCompatProxyRequest(p, "/loki/api/v1/query_range?"+params.Encode(), nil)
	}
	end := at.Add(2 * time.Minute)
	want := get(off, end)
	if want.Code != http.StatusOK || !strings.Contains(want.Body.String(), `"x":"aa","y":"1"`) {
		t.Fatal("cold range", want.Body)
	}
	if first := get(on, at.Add(time.Minute)); first.Code != http.StatusOK {
		t.Fatal(first.Body)
	}
	on.statsExtentStores.wait()
	fake.mu.Lock()
	before := fake.raw
	fake.mu.Unlock()
	got := get(on, end)
	on.statsExtentStores.wait()
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if got.Code != want.Code || !bytes.Equal(got.Body.Bytes(), want.Body.Bytes()) {
		t.Errorf("merged range %d %s, whole %d %s", got.Code, got.Body, want.Code, want.Body)
	}
	gaps := fake.rawQueries[before:]
	if len(gaps) != 1 || gaps[0].start < at.Add(time.Minute).UnixNano() {
		t.Errorf("expected only the final evaluation gap, got %+v", gaps)
	}
}

func TestOrderedJSONSettledStepsLimitsAndWarnings(t *testing.T) {
	base := time.Unix(1700000400, 0).UTC()
	for _, limit := range []int{1, 2} {
		t.Run(strconv.Itoa(limit), func(t *testing.T) {
			shape := dlvBaselineShapes[0] // Drilldown chooses busiest series over the full window
			backend, _ := newDLVBaselineBackend(t, shape, base)
			on := newDLVBaselineProxy(t, backend.URL, false)
			off := newDLVBaselineProxy(t, backend.URL, false)
			on.queryRangeExtentCache, on.queryRangeExtentCacheTTL = true, time.Hour
			on.maxStatsQuerySeries, off.maxStatsQuerySeries = limit, limit
			for _, shift := range []time.Duration{0, time.Minute, -time.Minute} {
				target, headers := dlvBaselineRequest(shape, base.Add(time.Minute+shift), base.Add(4*time.Minute+shift))
				got, want := doCompatProxyRequest(on, target, headers), doCompatProxyRequest(off, target, headers)
				if got.Code != want.Code || !bytes.Equal(got.Body.Bytes(), want.Body.Bytes()) {
					t.Fatalf("limit %d changed ranking/warnings: with %d %s without %d %s", limit, got.Code, got.Body, want.Code, want.Body)
				}
				on.statsExtentStores.wait()
			}
		})
	}
}

func TestOrderedJSONSettledStepsUnionLimitFallsBack(t *testing.T) {
	base := time.Unix(1700000400, 0).UTC()
	shape := dlvBaselineShapes[0]
	backend, fixture := newDLVBaselineBackend(t, shape, base)
	for i := range fixture.rows {
		fixture.rows[i].level = "info"
		if fixture.rows[i].ts.After(base.Add(2 * time.Minute)) {
			fixture.rows[i].level = "warn"
		}
	}
	on, off := newDLVBaselineProxy(t, backend.URL, false), newDLVBaselineProxy(t, backend.URL, false)
	on.queryRangeExtentCache, on.queryRangeExtentCacheTTL = true, time.Hour
	on.maxStatsQuerySeries, off.maxStatsQuerySeries = 1, 1
	for _, shift := range []time.Duration{0, time.Minute, 2 * time.Minute} {
		target, headers := dlvBaselineRequest(shape, base.Add(time.Minute+shift), base.Add(2*time.Minute+shift))
		got, want := doCompatProxyRequest(on, target, headers), doCompatProxyRequest(off, target, headers)
		if got.Code != want.Code || !bytes.Equal(got.Body.Bytes(), want.Body.Bytes()) {
			t.Fatalf("merged ranking/limit changed: with %d %s without %d %s", got.Code, got.Body, want.Code, want.Body)
		}
		on.statsExtentStores.wait()
	}
}

func TestOrderedJSONSettledStepsCancellationNotStored(t *testing.T) {
	base := time.Unix(1700000400, 0).UTC()
	shape := dlvBaselineShapes[1]
	backend, _ := newDLVBaselineBackend(t, shape, base)
	p := newDLVBaselineProxy(t, backend.URL, false)
	p.queryRangeExtentCache, p.queryRangeExtentCacheTTL = true, time.Hour
	target, headers := dlvBaselineRequest(shape, base.Add(time.Minute), base.Add(4*time.Minute))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r := httptest.NewRequest(http.MethodGet, target, nil).WithContext(ctx)
	for key, value := range headers {
		r.Header.Set(key, value)
	}
	w := &orderedJSONCancelWriter{httptest.NewRecorder(), cancel}
	mux := http.NewServeMux()
	p.RegisterRoutes(mux)
	mux.ServeHTTP(w, r)
	p.statsExtentStores.wait()
	if ctx.Err() == nil || queryStepsOutcomes(t, p)["stored"] != 0 {
		t.Fatal("canceled request retained settled points")
	}
}

func TestOrderedJSONSettledStepsRateAndSlidingRange(t *testing.T) {
	base := time.Unix(1700000400, 0).UTC()
	shape := dlvBaselineShapes[1]
	for _, function := range []string{"count_over_time", "rate"} {
		t.Run(function, func(t *testing.T) {
			backend, fixture := newDLVBaselineBackend(t, shape, base)
			on, off := newDLVBaselineProxy(t, backend.URL, false), newDLVBaselineProxy(t, backend.URL, false)
			on.queryRangeExtentCache, on.queryRangeExtentCacheTTL = true, time.Hour
			for _, shift := range []time.Duration{0, time.Minute, -time.Minute} {
				target, headers := dlvBaselineRequest(shape, base.Add(time.Minute+shift), base.Add(4*time.Minute+shift))
				u, _ := url.Parse(target)
				q := u.Query()
				q.Set("query", strings.ReplaceAll(strings.ReplaceAll(q.Get("query"), "count_over_time", function), "[1m]", "[2m]"))
				u.RawQuery = q.Encode()
				want := doCompatProxyRequest(off, u.String(), headers)
				before := fixture.calls.Load()
				got := doCompatProxyRequest(on, u.String(), headers)
				if got.Code != want.Code || !bytes.Equal(got.Body.Bytes(), want.Body.Bytes()) {
					t.Fatalf("sliding %s differs: %s vs %s", function, got.Body, want.Body)
				}
				on.statsExtentStores.wait()
				if shift == time.Minute && queryStepsOutcomes(t, on)["partial"] == 0 {
					t.Fatalf("sliding %s did not reuse points (backend calls %d)", function, fixture.calls.Load()-before)
				}
			}
		})
	}
}

func TestOrderedJSONSettledStepsFreshTailRevalidated(t *testing.T) {
	base := cache.Now().UTC().Truncate(time.Minute).Add(-4 * time.Minute)
	shape := dlvBaselineShapes[1]
	backend, fixture := newDLVBaselineBackend(t, shape, base)
	p := newDLVBaselineProxy(t, backend.URL, false)
	p.queryRangeExtentCache, p.queryRangeExtentCacheTTL, p.queryRangeFreshness = true, time.Hour, 2*time.Minute
	target, headers := dlvBaselineRequest(shape, base.Add(time.Minute), base.Add(4*time.Minute))
	_ = dlvBaselineReadMatrix(t, doCompatProxyRequest(p, target, headers))
	p.statsExtentStores.wait()
	// New rows in the unsettled tail must be visible on a moving request.
	fixture.appendRows(jsonVolumeLine{base.Add(3*time.Minute + 45*time.Second), "warn", "bad JSON"}, jsonVolumeLine{base.Add(3*time.Minute + 46*time.Second), "warn", "bad JSON"})
	target, headers = dlvBaselineRequest(shape, base.Add(2*time.Minute), base.Add(5*time.Minute))
	got := dlvBaselineReadMatrix(t, doCompatProxyRequest(p, target, headers))
	want := dlvBaselineWant(fixture.snapshot(), shape, base.Add(2*time.Minute), base.Add(5*time.Minute), "10", "alpha")
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("fresh-tail backfill hidden: got%v want%v", got, want)
	}
}

func TestOrderedJSONSettledStepsProductionDiagnostics(t *testing.T) {
	backend, _ := newDLVBaselineBackend(t, dlvBaselineShapes[1], time.Unix(1700000400, 0))
	p := newDLVBaselineProxy(t, backend.URL, false)
	r := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range", nil)
	_, _, _ = p.queryStepsFor(r, `sum(count_over_time({app="api"}[1m]))`)
	w := httptest.NewRecorder()
	p.metrics.Handler(w, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if !strings.Contains(w.Body.String(), `loki_vl_proxy_internal_operation_total{operation="query_steps_eligibility",outcome="disabled"} 1`) {
		t.Fatal("production cannot distinguish disabled steps cache from a lookup miss")
	}
	var log bytes.Buffer
	p.log = slog.New(slog.NewJSONHandler(&log, &slog.HandlerOptions{Level: slog.LevelDebug}))
	p.logQueryStepsDecision(context.Background(), "partial", queryStepsGrid{first: 1, last: 100}, [][2]int64{{99, 100}}, 1234)
	var event map[string]any
	if err := json.Unmarshal(log.Bytes(), &event); err != nil {
		t.Fatal(err)
	}
	if event["msg"] != "query_steps_cache" || event["total_steps"] != float64(100) || event["missing_steps"] != float64(2) || event["missing_ranges"] != float64(1) || event["response_bytes"] != float64(1234) {
		t.Fatalf("unusable decision log: %s", log.String())
	}
	for _, field := range []string{"query", "tenant", "cache_key", "authorization"} {
		if _, ok := event[field]; ok {
			t.Fatalf("sensitive/high-cardinality field %s", field)
		}
	}
}

func TestOrderedJSONSettledStepsExcludedFunctions(t *testing.T) {
	base := time.Unix(1700000400, 0).UTC()
	shape := dlvBaselineShapes[1]
	for _, function := range []string{"bytes_over_time", "bytes_rate"} {
		t.Run(function, func(t *testing.T) {
			backend, _ := newDLVBaselineBackend(t, shape, base)
			p := newDLVBaselineProxy(t, backend.URL, false)
			p.queryRangeExtentCache, p.queryRangeExtentCacheTTL = true, time.Hour
			target, headers := dlvBaselineRequest(shape, base.Add(time.Minute), base.Add(4*time.Minute))
			u, _ := url.Parse(target)
			q := u.Query()
			q.Set("query", strings.ReplaceAll(strings.ReplaceAll(q.Get("query"), "count_over_time", function), "[1m]", "[2m]"))
			u.RawQuery = q.Encode()
			r := doCompatProxyRequest(p, u.String(), headers)
			p.statsExtentStores.wait()
			if r.Code != http.StatusOK {
				t.Fatal(r.Body)
			}
			if queryStepsOutcomes(t, p)["stored"] != 0 {
				t.Fatal("excluded byte metric published raw fragments into the old namespace")
			}
			w := httptest.NewRecorder()
			p.metrics.Handler(w, httptest.NewRequest(http.MethodGet, "/metrics", nil))
			if !strings.Contains(w.Body.String(), `loki_vl_proxy_internal_operation_total{operation="query_steps_admission",outcome="evaluator"} 1`) {
				t.Fatal("uncertified evaluator misreported as byte overflow")
			}
		})
	}
}

func TestOrderedJSONSettledStepsExpiryAndFailure(t *testing.T) {
	base := time.Unix(1700000400, 0).UTC()
	shape := dlvBaselineShapes[1]
	backend, fixture := newDLVBaselineBackend(t, shape, base)
	p := newDLVBaselineProxy(t, backend.URL, false)
	p.queryRangeExtentCache, p.queryRangeExtentCacheTTL = true, time.Hour
	get := func(shift time.Duration) *httptest.ResponseRecorder {
		target, headers := dlvBaselineRequest(shape, base.Add(time.Minute+shift), base.Add(4*time.Minute+shift))
		return doCompatProxyRequest(p, target, headers)
	}
	if r := get(0); r.Code != http.StatusOK {
		t.Fatal(r.Body)
	}
	p.statsExtentStores.wait()
	fixture.fail.Store(true)
	if r := get(time.Minute); r.Code == http.StatusOK {
		t.Fatal("missing-edge backend failure hidden")
	}
	p.statsExtentStores.wait()
	fixture.fail.Store(false)
	if r := get(time.Minute); r.Code != http.StatusOK {
		t.Fatal("failed gap poisoned recovery", r.Body)
	}
	p.statsExtentStores.wait()
	before := fixture.calls.Load()
	restore := cache.AdvanceClockForTesting(2 * time.Hour)
	defer restore()
	if r := get(0); r.Code != http.StatusOK || fixture.calls.Load() <= before {
		t.Fatal("expired points reused")
	}
}

func TestOrderedJSONSettledStepsMergedBounds(t *testing.T) {
	backend, _ := newDLVBaselineBackend(t, dlvBaselineShapes[1], time.Unix(1700000400, 0))
	p := newDLVBaselineProxy(t, backend.URL, false)
	g := queryStepsGrid{step: int64(time.Minute), first: 1, last: 2}
	body := []byte(`{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"detected_level":"info"},"values":[[60,"1"],[120,"2"]]}]}}`)
	f, ok := parseQueryStepsAnswer(body, g, 1, 2)
	if !ok {
		t.Fatal("bad fixture")
	}
	f.names = append(f.names, "ordered-json-raw", "")
	if _, ok := p.renderQuerySteps(context.Background(), "bounds", []*statsExtentFragment{f}, []*statsExtentFragment{f}); !ok {
		t.Fatal("valid answer rejected")
	}
	p.orderedJSONMaxBytes = 20
	if _, ok := p.renderQuerySteps(context.Background(), "bounds", []*statsExtentFragment{f}, []*statsExtentFragment{f}); ok {
		t.Fatal("merged bytes bypass raw output limit")
	}
	p.orderedJSONMaxBytes = 1 << 30
	f.series[0].points = make([]statsExtentPoint, maxMetricEvalSamples+1)
	if _, ok := p.renderQuerySteps(context.Background(), "bounds", []*statsExtentFragment{f}, []*statsExtentFragment{f}); ok {
		t.Fatal("merged samples bypass raw output limit")
	}
}
