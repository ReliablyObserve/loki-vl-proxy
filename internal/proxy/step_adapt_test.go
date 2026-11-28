package proxy

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/lokierr"
)

var stepTestEnd = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

// stepInput is the request of the issue: 80 s at a fine step over a [1h] window.
func stepInput(step time.Duration, onGrid bool, windows ...time.Duration) stepCandidateInput {
	end := stepTestEnd.UnixNano() / int64(step) * int64(step)
	start := (stepTestEnd.Add(-80*time.Second).UnixNano() + int64(step) - 1) / int64(step) * int64(step)
	if len(windows) == 0 {
		windows = []time.Duration{time.Hour}
	}
	return stepCandidateInput{step: step, startNs: start, endNs: end, onGrid: onGrid, windows: windows, share: 0.5}
}

// TestEffectiveStep pins the effective steps: the smallest multiple of the step that fits when it is at most twice
// the finest step that fits, that finest step otherwise.
//
// conformance: limits/range-metric-step-adapt
func TestEffectiveStep(t *testing.T) {
	ms := time.Millisecond
	cases := []struct {
		name   string
		in     stepCandidateInput
		want   time.Duration
		wantOK bool
	}{
		// (80 s + 1 h) / 400 ms = 9,200 buckets: the first multiple of 50 ms whose gcd with the hour holds the bound;
		// the finest step that fits is 360 ms, and 400 ms is within twice it.
		{"50ms over [1h], raw rows", stepInput(50*ms, true), 400 * ms, true},
		// 53 ms shares no factor with the hour: the smallest multiple that fits is 360 x 53 ms = 19.08 s, more than
		// twice the finest step that fits, 360 ms.
		{"53ms over [1h], raw rows", stepInput(53*ms, true), 360 * ms, true},
		{"53ms over [1h], align off", stepInput(53*ms, false), 360 * ms, true},
		{"200ms over [1h]", stepInput(200*ms, true), 400 * ms, true},
		{"1s over [1h] fits", stepInput(time.Second, true), 0, false},
		// Off the grid the bounds keep the start: same model, same multiple.
		{"50ms over [1h], align off", stepInput(50*ms, false), 400 * ms, true},
		// A [5m] window at 50 ms: (80 s + 5 m) / 50 ms = 7,600 buckets fit already. A bound on the answer halves the
		// points; a read bound with no grid that shrinks is not retried.
		{"50ms over [5m], answer bound", func() stepCandidateInput {
			in := stepInput(50*ms, true, 5*time.Minute)
			in.halve = true
			return in
		}(), 100 * ms, true},
		{"50ms over [5m], read bound", stepInput(50*ms, true, 5*time.Minute), 0, false},
		{"one step's window", func() stepCandidateInput {
			in := stepInput(50*ms, true)
			in.failed = []readGrid{{route: readRouteStepIndependent, buckets: 73600}}
			return in
		}(), 0, false},
		// Two windows: both must fit.
		{"binary [1h] and [5m]", stepInput(50*ms, true, time.Hour, 5*time.Minute), 400 * ms, true},
		// A ceiling below every step that fits: the limit error stays.
		{"ceiling", func() stepCandidateInput { in := stepInput(50*ms, true); in.ceiling = 300 * ms; return in }(), 0, false},
		// A floor above the step that fits.
		{"floor", func() stepCandidateInput { in := stepInput(50*ms, true); in.floor = time.Second; return in }(), time.Second, true},
		// The window grids of a recurrence that failed within the bound: half of their buckets.
		{"window grids at 1s", func() stepCandidateInput {
			in := stepInput(time.Second, true)
			in.failed = []readGrid{{route: "window-grids", step: time.Second, window: time.Hour, bucket: time.Second, buckets: 81}}
			return in
		}(), 2 * time.Second, true},
		// A bucket read within the bound at 1 s: (80 s + 1 h) / 1 s = 3,680 buckets, half of them is 1,840: 2 s.
		{"bucket read at 1s", func() stepCandidateInput {
			in := stepInput(time.Second, true)
			in.failed = []readGrid{{route: "buckets", step: time.Second, window: time.Hour, bucket: time.Second, buckets: 3680}}
			return in
		}(), 2 * time.Second, true},
		// Raw rows read within the bucket bound (a counter proof): no step changes them.
		{"raw rows within the bound", func() stepCandidateInput {
			in := stepInput(time.Minute, true, 5*time.Minute)
			in.failed = []readGrid{{route: "raw", step: time.Minute, window: 5 * time.Minute, bucket: time.Minute, buckets: 6}}
			return in
		}(), 0, false},
		// Raw rows read past it: the step that brings the window within it.
		{"raw rows past the bound", func() stepCandidateInput {
			in := stepInput(50*ms, true)
			in.failed = []readGrid{{route: "raw", step: 50 * ms, window: time.Hour, bucket: 50 * ms, buckets: 73600}}
			return in
		}(), 400 * ms, true},
		// A retry after 360 ms failed within the bound: the multiples of 360 ms.
		{"retry off the grid", func() stepCandidateInput {
			in := stepInput(53*ms, true)
			in.prev = 360 * ms
			in.failed = []readGrid{{route: "buckets", step: 360 * ms, window: time.Hour, bucket: 360 * ms, buckets: 10222}}
			return in
		}(), 720 * ms, true},
		// A range of one step holds no coarser grid of two points.
		{"no room", stepCandidateInput{step: time.Second, startNs: 0, endNs: int64(time.Second), onGrid: true, windows: []time.Duration{time.Hour}}, 0, false},
		// No window: not a metric query.
		{"no window", stepCandidateInput{step: time.Second, startNs: 0, endNs: int64(time.Hour), onGrid: true}, 0, false},
	}
	for _, tc := range cases {
		eff, ok := effectiveStep(tc.in)
		if ok != tc.wantOK || ok && eff != tc.want {
			t.Errorf("%s: %s ok=%v, want %s ok=%v", tc.name, eff, ok, tc.want, tc.wantOK)
		}
	}
}

// checkEffectiveStep is the property of every effective step: above the last one, every evaluation time anchored as
// Loki anchors a request at that step inside the requested range (and one of Loki's times at the requested step when
// the step is a multiple of it), two points at least, within the floor and the ceiling, every window's bucket read
// within the bound.
func checkEffectiveStep(t *testing.T, in stepCandidateInput, eff time.Duration) {
	t.Helper()
	if eff <= max(in.prev, in.step) {
		t.Fatalf("%s not above %s", eff, max(in.prev, in.step))
	}
	startNs, endNs, ok := effectiveBounds(in, eff)
	if !ok || startNs < in.startNs || endNs > in.endNs || endNs-startNs < int64(eff) {
		t.Fatalf("bounds %d..%d of %d..%d at %s", startNs, endNs, in.startNs, in.endNs, eff)
	}
	for ts := startNs; ts <= endNs; ts += int64(eff) {
		if in.onGrid && ts%int64(eff) != 0 || !in.onGrid && (ts-in.startNs)%int64(eff) != 0 {
			t.Fatalf("time %d is not anchored as Loki anchors a request at %s", ts, eff)
		}
		if eff%in.step == 0 && (in.onGrid && ts%int64(in.step) != 0 || !in.onGrid && (ts-in.startNs)%int64(in.step) != 0) {
			t.Fatalf("time %d of the multiple %s is off Loki's grid of %s", ts, eff, in.step)
		}
	}
	if in.ceiling > 0 && eff > in.ceiling || in.floor > 0 && eff < in.floor {
		t.Fatalf("%s outside the floor and ceiling", eff)
	}
	for _, w := range in.windows {
		if b := bucketsAt(in, eff, w); b > heavyQueryBucketThreshold {
			t.Fatalf("window %s holds %d buckets at %s", w, b, eff)
		}
	}
}

// FuzzEffectiveStep holds every effective step to checkEffectiveStep and, past the bucket bound at the requested
// step, to the rule: no smaller multiple fits; a multiple is at most twice the finest fitting step, a step off the
// grid is that finest step and no multiple within twice it fits.
//
// conformance: limits/range-metric-step-adapt
func FuzzEffectiveStep(f *testing.F) {
	f.Add(int64(50), int64(80_000), int64(3_600_000), int64(0), true, int64(0), int64(0))
	f.Add(int64(53), int64(80_000), int64(3_600_000), int64(300_000), true, int64(0), int64(0))
	f.Add(int64(1000), int64(3_600_000), int64(60_000), int64(0), false, int64(2000), int64(600_000))
	f.Fuzz(func(t *testing.T, stepMs, spanMs, windowMs, window2Ms int64, onGrid bool, floorMs, ceilMs int64) {
		if stepMs < 1 || stepMs > 3_600_000 || spanMs < 1 || spanMs > 86_400_000 || windowMs < 1 || windowMs > 30*86_400_000 ||
			window2Ms < 0 || window2Ms > 30*86_400_000 || floorMs < 0 || ceilMs < 0 || spanMs/stepMs > 11_000 || spanMs > 200_000 && stepMs < 20 {
			t.Skip()
		}
		step := time.Duration(stepMs) * time.Millisecond
		end := stepTestEnd.UnixNano() / int64(step) * int64(step)
		in := stepCandidateInput{step: step, startNs: end - spanMs*int64(time.Millisecond)/int64(step)*int64(step), endNs: end, onGrid: onGrid,
			windows: []time.Duration{time.Duration(windowMs) * time.Millisecond}, floor: time.Duration(floorMs) * time.Millisecond,
			ceiling: time.Duration(ceilMs) * time.Millisecond, share: 0.5}
		if !onGrid {
			in.startNs += 7 * int64(time.Millisecond)
		}
		if window2Ms > 0 {
			in.windows = append(in.windows, time.Duration(window2Ms)*time.Millisecond)
		}
		if in.endNs <= in.startNs {
			t.Skip()
		}
		eff, ok := effectiveStep(in)
		if !ok {
			return
		}
		checkEffectiveStep(t, in, eff)
		fitsAt := func(e time.Duration) bool {
			if _, _, ok := effectiveBounds(in, e); !ok || in.floor > 0 && e < in.floor || in.ceiling > 0 && e > in.ceiling {
				return false
			}
			for _, w := range in.windows {
				if b := bucketsAt(in, e, w); b == 0 || b > heavyQueryBucketThreshold {
					return false
				}
			}
			return true
		}
		if fitsAt(step) {
			return // not past the bucket bound: the halving rules
		}
		finest := time.Duration(0)
		for e := step + time.Millisecond; e <= eff; e += time.Millisecond {
			if fitsAt(e) {
				finest = e
				break
			}
		}
		for e := 2 * step; e < eff; e += step {
			if fitsAt(e) {
				t.Fatalf("%s chosen, but the multiple %s fits", eff, e)
			}
		}
		if eff%step == 0 && eff > stepMultipleDensity*finest {
			t.Fatalf("multiple %s chosen, more than twice the finest fitting step %s", eff, finest)
		}
		if eff%step != 0 && eff != finest {
			t.Fatalf("%s off the grid chosen, the finest fitting step is %s", eff, finest)
		}
	})
}

// stepTestProxy is a proxy for the layer tests, with the given config changes.
func stepTestProxy(t *testing.T, change func(*Config)) *Proxy {
	t.Helper()
	cfg := Config{BackendURL: "http://127.0.0.1:1", Cache: cache.New(time.Minute, 100), LogLevel: "error", AlignQueriesWithStep: true}
	if change != nil {
		change(&cfg)
	}
	p, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	return p
}

// stepRequestFor is the issue's request at step, with a request's telemetry as the request logger gives it.
func stepRequestFor(query string, step time.Duration, org string) *http.Request {
	end := stepTestEnd.UnixNano() / int64(step) * int64(step)
	start := (stepTestEnd.Add(-80*time.Second).UnixNano() + int64(step) - 1) / int64(step) * int64(step)
	form := url.Values{"query": {query}, "start": {strconv.FormatInt(start, 10)}, "end": {strconv.FormatInt(end, 10)},
		"step": {strconv.FormatInt(step.Milliseconds(), 10) + "ms"}}
	r := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+form.Encode(), nil)
	r.Header.Set("X-Scope-OrgID", org)
	return r.WithContext(context.WithValue(r.Context(), requestTelemetryKey, newRequestTelemetry()))
}

// stepStub is a handler that fails a read bound below fits and answers a matrix at or above it.
type stepStub struct {
	p        *Proxy
	fits     time.Duration
	failWith func(limit int) error
	answer   func(step time.Duration) string
	mu       sync.Mutex
	steps    []string
	header   string
}

func (s *stepStub) serve(w http.ResponseWriter, r *http.Request) {
	step, _ := stepWithoutAllocating(r.FormValue("step"))
	s.mu.Lock()
	s.steps = append(s.steps, r.FormValue("step"))
	s.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	if s.header != "" {
		w.Header().Set("X-Proxy-Drilldown-Path", s.header)
	}
	if step < s.fits {
		fail := s.failWith
		if fail == nil {
			fail = func(limit int) error { return newBackendBodyTooLarge(int64(limit)) }
		}
		s.p.fail(w, r, fail(1<<20))
		return
	}
	body := `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"peer_ip":"10.0.0.1"},"values":[[` + r.FormValue("start") + `,"1"]]}]}}`
	if s.answer != nil {
		body = s.answer(step)
	}
	_, _ = w.Write([]byte(body))
}

func (s *stepStub) seen() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.steps...)
}

const stepTestQuery = `sum by (pod) (count_over_time({app="api"} | json [1h]))`

// TestStepLayer_FittingRequestIsServedAsSent: a request that fits is the handler's answer byte for byte, with no
// header of the layer and no effective request, in every mode.
//
// conformance: limits/range-metric-step-adapt
func TestStepLayer_FittingRequestIsServedAsSent(t *testing.T) {
	for _, mode := range []string{"", RangeMetricStepModeAdaptive, RangeMetricStepModeFloor, RangeMetricStepModeOff} {
		p := stepTestProxy(t, func(c *Config) { c.RangeMetricStepMode = mode })
		stub := &stepStub{p: p, fits: time.Millisecond}
		cachedCalls := 0
		h := p.rangeStepLayer(stub.serve, func(w http.ResponseWriter, r *http.Request) { cachedCalls++ })
		direct := httptest.NewRecorder()
		stub.serve(direct, stepRequestFor(stepTestQuery, 50*time.Millisecond, "t"))
		rec := httptest.NewRecorder()
		h(rec, stepRequestFor(stepTestQuery, 50*time.Millisecond, "t"))
		if rec.Code != direct.Code || rec.Body.String() != direct.Body.String() || rec.Header().Get(stepAdjustedHeader) != "" || cachedCalls != 0 {
			t.Fatalf("mode %q: %d %q (header %q, %d effective requests), want the handler's %d %q", mode, rec.Code, rec.Body.String(), rec.Header().Get(stepAdjustedHeader), cachedCalls, direct.Code, direct.Body.String())
		}
	}
}

// TestStepLayer_RetriesAtTheSmallestMultipleThatFits: a read past the buffered bound at 50 ms over [1h] is answered
// at 400 ms with Loki's warning, the warning answer marked for the cache, and a repeat goes straight to 400 ms.
//
// conformance: limits/range-metric-step-adapt
func TestStepLayer_RetriesAtTheSmallestMultipleThatFits(t *testing.T) {
	p := stepTestProxy(t, nil)
	stub := &stepStub{p: p, fits: 400 * time.Millisecond, header: "raw"}
	effective := &stepStub{p: p, fits: 400 * time.Millisecond}
	h := p.rangeStepLayer(stub.serve, effective.serve)
	for i := 0; i < 2; i++ {
		rec := httptest.NewRecorder()
		h(rec, stepRequestFor(stepTestQuery, 50*time.Millisecond, "t"))
		if rec.Code != http.StatusOK {
			t.Fatalf("run %d: %d %s", i, rec.Code, rec.Body.String())
		}
		var resp struct {
			Status   string   `json:"status"`
			Warnings []string `json:"warnings"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil || resp.Status != "success" ||
			len(resp.Warnings) != 1 || resp.Warnings[0] != "step raised from 50ms to 400ms to fit the query limits" {
			t.Fatalf("run %d: %s (%v)", i, rec.Body.String(), err)
		}
		if got := rec.Header().Get(stepAdjustedHeader); got != "50ms;400ms" {
			t.Fatalf("run %d: header %q", i, got)
		}
		if rec.Header().Get("X-Proxy-Drilldown-Path") != "" {
			t.Fatalf("run %d: the failed attempt's header was kept", i)
		}
		if compatCacheCaptureAllowed(rec.Code, false, rec.Header()) {
			t.Fatalf("run %d: the warning answer is cacheable", i)
		}
	}
	if got := strings.Join(stub.seen(), ","); got != "50ms" {
		t.Fatalf("requests as sent: %s, want one (the repeat goes straight to the learned step)", got)
	}
	if got := strings.Join(effective.seen(), ","); got != "400ms,400ms" {
		t.Fatalf("effective requests: %s", got)
	}
	// The effective bounds are multiples of the effective step inside the requested range.
	r := stepRequestFor(stepTestQuery, 50*time.Millisecond, "t")
	sr, ok := p.stepRequestOf(r, p.stepLimitsFor("t"))
	if !ok {
		t.Fatal("not a request the layer reads")
	}
	child := effectiveStepRequest(r, sr, 400*time.Millisecond)
	start, _ := strconv.ParseInt(child.FormValue("start"), 10, 64)
	end, _ := strconv.ParseInt(child.FormValue("end"), 10, 64)
	if start%int64(400*time.Millisecond) != 0 || end%int64(400*time.Millisecond) != 0 || start < sr.in.startNs || end > sr.in.endNs || child.FormValue("query") != stepTestQuery {
		t.Fatalf("effective request %s", child.URL.RawQuery)
	}
}

// TestStepLayer_LokiErrorsAreAnsweredAsBefore: Loki's own limits, and a read bound at a step whose windows already
// fit the bucket bound, keep their error; no effective request answers them.
//
// conformance: limits/range-metric-step-adapt
func TestStepLayer_LokiErrorsAreAnsweredAsBefore(t *testing.T) {
	for name, fail := range map[string]func(int) error{
		"series limit":    func(int) error { return lokierr.SeriesLimitOf(flagMaxStatsQuerySeries, 500, nil) },
		"too many points": func(int) error { return lokierr.TooManyPoints() },
		"label values":    func(limit int) error { return newLabelValuesTooLarge(int64(limit)+1, int64(limit)) },
	} {
		p := stepTestProxy(t, nil)
		stub := &stepStub{p: p, fits: time.Hour, failWith: fail}
		direct := httptest.NewRecorder()
		stub.serve(direct, stepRequestFor(stepTestQuery, 50*time.Millisecond, "t"))
		cachedCalls := 0
		h := p.rangeStepLayer(stub.serve, func(w http.ResponseWriter, r *http.Request) { cachedCalls++ })
		rec := httptest.NewRecorder()
		h(rec, stepRequestFor(stepTestQuery, 50*time.Millisecond, "t"))
		if rec.Code != direct.Code || rec.Body.String() != direct.Body.String() || cachedCalls != 0 {
			t.Fatalf("%s: %d %s (%d effective requests), want %d %s", name, rec.Code, rec.Body.String(), cachedCalls, direct.Code, direct.Body.String())
		}
	}
}

// TestStepLayer_FloorAndCeiling: the floor raises a step before any read; a ceiling below the step that fits keeps
// the error; per tenant, off keeps the error and floor applies only the floor.
//
// conformance: limits/range-metric-step-adapt
func TestStepLayer_FloorAndCeiling(t *testing.T) {
	p := stepTestProxy(t, func(c *Config) {
		c.RangeMetricStepMode = RangeMetricStepModeFloor
		c.RangeMetricMinStep = time.Second
	})
	stub := &stepStub{p: p, fits: time.Millisecond}
	rec := httptest.NewRecorder()
	p.rangeStepLayer(stub.serve, stub.serve)(rec, stepRequestFor(stepTestQuery, 50*time.Millisecond, "t"))
	if got := strings.Join(stub.seen(), ","); got != "1000ms" || !strings.Contains(rec.Body.String(), `"step raised from 50ms to 1s, the minimum step of range metric queries"`) {
		t.Fatalf("floor: requests %s, answer %s", got, rec.Body.String())
	}
	p = stepTestProxy(t, func(c *Config) {
		c.RangeMetricMaxStep = 300 * time.Millisecond
		c.TenantLimits = map[string]map[string]any{"off": {limitRangeMetricStepMode: "off"}, "floor": {limitRangeMetricStepMode: "floor"}}
	})
	for _, org := range []string{"t", "off", "floor"} {
		stub = &stepStub{p: p, fits: 400 * time.Millisecond}
		rec = httptest.NewRecorder()
		p.rangeStepLayer(stub.serve, stub.serve)(rec, stepRequestFor(stepTestQuery, 50*time.Millisecond, org))
		if rec.Code != http.StatusInternalServerError || strings.Join(stub.seen(), ",") != "50ms" {
			t.Fatalf("tenant %s: %d %s, requests %v", org, rec.Code, rec.Body.String(), stub.seen())
		}
	}
}

// TestStepLayer_WarningJoinsTheAnswersWarnings: an effective answer with Loki's warnings keeps them and adds the
// step's.
//
// conformance: limits/range-metric-step-adapt
func TestStepLayer_WarningJoinsTheAnswersWarnings(t *testing.T) {
	body := `{"status":"success","warnings":["maximum number of series (500) reached for a single query; returning partial results"],"data":{"resultType":"matrix","result":[]}}`
	got := withLokiWarning([]byte(body), "step raised from 50ms to 400ms to fit the query limits")
	var resp map[string]json.RawMessage
	var warnings []string
	if err := json.Unmarshal(got, &resp); err != nil || json.Unmarshal(resp["warnings"], &warnings) != nil || len(warnings) != 2 || string(resp["data"]) != `{"resultType":"matrix","result":[]}` {
		t.Fatalf("%s", got)
	}
	if got := string(withLokiWarning([]byte(`{"status":"success","data":{}}`), "w")); got != `{"warnings":["w"],"status":"success","data":{}}` {
		t.Fatalf("%s", got)
	}
}

// TestStepLayer_HeldFailureIsCountedOnce: the failed attempt is not counted or logged as an error; a request the
// layer answers with its error after all counts it once.
//
// conformance: limits/range-metric-step-adapt
func TestStepLayer_HeldFailureIsCountedOnce(t *testing.T) {
	p := stepTestProxy(t, func(c *Config) { c.RangeMetricMaxStep = 300 * time.Millisecond })
	stub := &stepStub{p: p, fits: time.Hour}
	h := p.rangeStepLayer(func(w http.ResponseWriter, r *http.Request) {
		stub.serve(w, r)
		p.recordRangeRequest(r.Context(), "query_range", http.StatusInternalServerError, 0)
	}, stub.serve)
	h(httptest.NewRecorder(), stepRequestFor(stepTestQuery, 50*time.Millisecond, "t"))
	rec := httptest.NewRecorder()
	p.metrics.Handler(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	n := 0
	for _, line := range strings.Split(rec.Body.String(), "\n") {
		if strings.HasPrefix(line, "loki_vl_proxy_requests_total{") && strings.Contains(line, `endpoint="query_range"`) && strings.Contains(line, `status="500"`) {
			v, _ := strconv.Atoi(line[strings.LastIndexByte(line, ' ')+1:])
			n += v
		}
	}
	if n != 1 {
		t.Fatalf("query_range 500 counted %d times", n)
	}
}

// TestStepLayer_Metrics: an adjusted answer is counted by mode and reason with its factor.
//
// conformance: limits/range-metric-step-adapt
func TestStepLayer_Metrics(t *testing.T) {
	p := stepTestProxy(t, nil)
	stub := &stepStub{p: p, fits: 400 * time.Millisecond}
	p.rangeStepLayer(stub.serve, stub.serve)(httptest.NewRecorder(), stepRequestFor(stepTestQuery, 50*time.Millisecond, "t"))
	rec := httptest.NewRecorder()
	p.metrics.Handler(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	for _, want := range []string{
		`loki_vl_proxy_step_adjusted_total{endpoint="query_range",mode="adaptive",reason="backend-max-buffered-response-bytes"} 1`,
		`loki_vl_proxy_step_adjust_factor_bucket{endpoint="query_range",le="8"} 1`,
	} {
		if !strings.Contains(rec.Body.String(), want) {
			t.Errorf("metrics lack %s", want)
		}
	}
}

// TestRangeWindows: the windows of a metric query; a query that is not one, or a window the step sets, has none.
//
// conformance: limits/range-metric-step-adapt
func TestRangeWindows(t *testing.T) {
	for q, want := range map[string]string{
		`sum by (a) (count_over_time({a="b"}[1h]))`:                "1h0m0s",
		`sum(rate({a="b"}[5m])) / sum(rate({a="b"} | json [1h]))`:  "5m0s,1h0m0s",
		`label_replace(rate({a="b"}[1m]), "x", "$1", "a", "(.*)")`: "1m0s",
		`topk(3, sum by (a) (rate({a="b"}[10m]))) * 2`:             "10m0s",
		`{a="b"} |= "x"`: "",
		`vector(1)`:      "",
	} {
		expr, err := parseLogQLExpr(context.Background(), q)
		if err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		windows, ok := rangeWindows(expr)
		var got []string
		for _, w := range windows {
			got = append(got, w.String())
		}
		if strings.Join(got, ",") != want || ok != (want != "") {
			t.Errorf("%s: %v %v, want %q", q, got, ok, want)
		}
	}
}

// TestStepLayer_ThroughTheRouteChain: through the shipped mux (compatibility cache, flights, query stats), a per-stream
// count over [1h] at 50 ms whose raw rows pass the buffered bound answers at 400 ms with one warning and one stats
// object at most; a repeat goes straight to the learned step and reads the effective request's cached answer (no VictoriaLogs
// call), the warning answer itself is never cached (the repeat is adjusted again), and the effective request asked
// directly is the warning-free answer from the cache.
//
// conformance: limits/range-metric-step-adapt
func TestStepLayer_ThroughTheRouteChain(t *testing.T) {
	backend, calls := stepFineRawBackend(t)
	p, get := stepRouteProxy(t, backend.URL, calls)
	end := stepTestEnd.UnixNano()
	start := stepTestEnd.Add(-80 * time.Second).UnixNano()
	for i := 0; i < 2; i++ {
		rec, n := get(50*time.Millisecond, start, end)
		if rec.Code != http.StatusOK {
			t.Fatalf("run %d: %d %.300s", i, rec.Code, rec.Body.String())
		}
		var resp struct {
			Warnings []string `json:"warnings"`
			Data     struct {
				Stats json.RawMessage `json:"stats"`
			} `json:"data"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil || len(resp.Warnings) != 1 || resp.Warnings[0] != "step raised from 50ms to 400ms to fit the query limits" ||
			strings.Count(rec.Body.String(), `"stats"`) > 1 {
			t.Fatalf("run %d: %.400s (%v)", i, rec.Body.String(), err)
		}
		if i == 1 && n != 0 {
			t.Fatalf("the repeat made %d VictoriaLogs calls, want 0", n)
		}
	}
	metrics := httptest.NewRecorder()
	p.metrics.Handler(metrics, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if !strings.Contains(metrics.Body.String(), `loki_vl_proxy_step_adjusted_total{endpoint="query_range",mode="adaptive",reason="backend-max-buffered-response-bytes"} 2`) {
		t.Fatalf("want both requests adjusted (the warning answer is not cached as such)")
	}
	rec, n := get(400*time.Millisecond, start, end)
	if rec.Code != http.StatusOK || n != 0 || strings.Contains(rec.Body.String(), "warnings") || rec.Header().Get(stepAdjustedHeader) != "" {
		t.Fatalf("the effective request asked directly: %d, %d calls, %.300s", rec.Code, n, rec.Body.String())
	}
}

// TestStepLayer_StepIndependentFailureReadsOnce: a read bound at a step whose windows already fit the bucket bound
// (raw rows read for another reason) is answered as main answers it, after one read.
//
// conformance: limits/range-metric-step-adapt
func TestStepLayer_StepIndependentFailureReadsOnce(t *testing.T) {
	var mu sync.Mutex
	calls := 0
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		mu.Lock()
		calls++
		mu.Unlock()
		// Every read past the buffered bound.
		_, _ = w.Write([]byte(strings.Repeat(`{"_time":"2025-12-31T23:30:00Z","_stream":"{app=\"api\"}","_msg":"line","app":"api"}`+"\n", 4096)))
	}))
	defer backend.Close()
	run := func(mode string) (int, string, int) {
		p, err := New(Config{BackendURL: backend.URL, Cache: cache.New(time.Minute, 1000), LogLevel: "error", RangeMetricStepMode: mode,
			AlignQueriesWithStep: true, ExecutionLimits: ExecutionLimitsConfig{BackendMaxBufferedResponseBytes: 64 << 10}})
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = p.Shutdown(context.Background()) }()
		p.storeBackendVersion("v1.54.0", "v1.54.0")
		mux := http.NewServeMux()
		p.RegisterRoutes(mux)
		end := stepTestEnd.UnixNano()
		form := url.Values{"query": {`sum(rate_counter({app="api"} | json | unwrap bytes [5m]))`}, "start": {strconv.FormatInt(end-int64(time.Hour), 10)},
			"end": {strconv.FormatInt(end, 10)}, "step": {"60"}}
		req := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+form.Encode(), nil)
		req.Header.Set("X-Scope-OrgID", "0")
		mu.Lock()
		before := calls
		mu.Unlock()
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		mu.Lock()
		defer mu.Unlock()
		return rec.Code, rec.Body.String(), calls - before
	}
	offCode, offBody, offCalls := run(RangeMetricStepModeOff)
	code, body, n := run(RangeMetricStepModeAdaptive)
	if offCode == http.StatusOK {
		t.Fatalf("the read fits: %.200s", offBody)
	}
	if code != offCode || body != offBody || n != offCalls {
		t.Fatalf("adaptive: %d %q after %d calls, off: %d %q after %d calls", code, body, n, offCode, offBody, offCalls)
	}
}

// stepFineRawBackend is a VictoriaLogs stub whose raw rows of the hour pass a 64 KiB buffered bound and whose
// stats_query_range answers one sample at the start; calls counts its requests.
func stepFineRawBackend(t *testing.T) (*httptest.Server, *stepCallCount) {
	t.Helper()
	calls := &stepCallCount{}
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		calls.add()
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.HasSuffix(r.URL.Path, "/stats_query_range"):
			_, _ = w.Write([]byte(`{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"peer_ip":"10.0.0.1"},"values":[[` +
				r.Form.Get("start") + `,"7"]]}]}}`))
		case r.URL.Path == "/select/logsql/query" && !strings.Contains(r.Form.Get("query"), "stats"):
			// The first raw read (the request at its own step) passes the bound; the rows a later read of the
			// effective request asks for fit.
			n := 4096
			if calls.rawOnce() {
				n = 3
			}
			_, _ = w.Write([]byte(strings.Repeat(`{"_time":"2025-12-31T23:30:00Z","_stream":"{app=\"api\"}","_msg":"line","app":"api"}`+"\n", n)))
		default:
			_, _ = w.Write([]byte(`{"values":[]}`))
		}
	}))
	t.Cleanup(backend.Close)
	return backend, calls
}

type stepCallCount struct {
	mu  sync.Mutex
	n   int
	raw bool
}

// rawOnce reports whether a raw read was answered before, and marks one.
func (c *stepCallCount) rawOnce() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	seen := c.raw
	c.raw = true
	return seen
}

func (c *stepCallCount) add() { c.mu.Lock(); c.n++; c.mu.Unlock() }
func (c *stepCallCount) get() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.n
}

// stepRouteProxy is a proxy with the shipped mux on backend, and a getter of the per-stream count over [1h] at a step
// that returns the answer and the VictoriaLogs calls it made.
func stepRouteProxy(t *testing.T, backend string, calls *stepCallCount) (*Proxy, func(time.Duration, int64, int64) (*httptest.ResponseRecorder, int)) {
	t.Helper()
	p, err := New(Config{BackendURL: backend, Cache: cache.New(time.Minute, 1000), CompatCache: cache.New(time.Minute, 1000), LogLevel: "error",
		AlignQueriesWithStep: true, ExecutionLimits: ExecutionLimitsConfig{BackendMaxBufferedResponseBytes: 64 << 10}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	p.storeBackendVersion("v1.54.0", "v1.54.0")
	mux := http.NewServeMux()
	p.RegisterRoutes(mux)
	return p, func(step time.Duration, startNs, endNs int64) (*httptest.ResponseRecorder, int) {
		before := calls.get()
		form := url.Values{"query": {`sum by (peer_ip) (sum_over_time({app="api"} | json | unwrap bytes [1h]))`}, "start": {strconv.FormatInt(startNs, 10)}, "end": {strconv.FormatInt(endNs, 10)},
			"step": {strconv.FormatInt(step.Milliseconds(), 10) + "ms"}}
		req := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+form.Encode(), nil)
		req.Header.Set("X-Scope-OrgID", "0")
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		return rec, calls.get() - before
	}
}

// TestStepLayer_EachReplicaLearnsOnItsOwn: the learned step is replica memory. A second replica that never served
// the request pays its own read at the requested step once (each replica has a VictoriaLogs stub of its own here, so
// the calls are counted apart), then answers the same bytes; its repeat makes no call.
//
// conformance: limits/range-metric-step-adapt
func TestStepLayer_EachReplicaLearnsOnItsOwn(t *testing.T) {
	backendA, callsA := stepFineRawBackend(t)
	backendB, callsB := stepFineRawBackend(t)
	_, getA := stepRouteProxy(t, backendA.URL, callsA)
	_, getB := stepRouteProxy(t, backendB.URL, callsB)
	end := stepTestEnd.UnixNano()
	start := stepTestEnd.Add(-80 * time.Second).UnixNano()
	a, aCalls := getA(50*time.Millisecond, start, end)
	b, bCalls := getB(50*time.Millisecond, start, end)
	b2, b2Calls := getB(50*time.Millisecond, start, end)
	if a.Code != http.StatusOK || b.Code != http.StatusOK || aCalls == 0 || bCalls != aCalls || b2Calls != 0 {
		t.Fatalf("replica A %d after %d calls, B %d after %d calls, B's repeat %d calls", a.Code, aCalls, b.Code, bCalls, b2Calls)
	}
	strip := func(s string) string {
		if i := strings.Index(s, `"stats"`); i >= 0 {
			return s[:i]
		}
		return s
	}
	if strip(a.Body.String()) != strip(b.Body.String()) || strip(b.Body.String()) != strip(b2.Body.String()) {
		t.Fatalf("replicas answer differently:\n%.300s\n%.300s", a.Body.String(), b.Body.String())
	}
}
