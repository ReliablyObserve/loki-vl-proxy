package proxy

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/lokierr"
)

// countBudget is a queryplan.Budget that runs out at its tripAt-th question.
type countBudget struct{ calls, tripAt int }

func (b *countBudget) Exhausted() bool { b.calls++; return b.calls >= b.tripAt }

// steppingClock is a clock that moves by step at every reading, so a test decides which check sees the budget spent.
func steppingClock(step time.Duration) func() time.Time {
	var mu sync.Mutex
	now := time.Unix(1_700_000_000, 0)
	return func() time.Time {
		mu.Lock()
		defer mu.Unlock()
		now = now.Add(step)
		return now
	}
}

// requestContext is a context that belongs to a request: it carries the telemetry the budget is kept in.
func requestContext() (context.Context, *requestTelemetry) {
	rt := &requestTelemetry{}
	return context.WithValue(context.Background(), requestTelemetryKey, rt), rt
}

func planTimeError(t *testing.T, err error) *lokierr.Error {
	t.Helper()
	e, ok := lokierr.As(err)
	if !ok || e.Msg() != lokierr.MsgPlanTime || e.Kind() != lokierr.KindLimit || e.Status() != http.StatusBadRequest {
		t.Fatalf("error %#v, want the planning budget's typed 400", err)
	}
	if e.Text() != "query planning took longer than the limit" {
		t.Fatalf("client text %q", e.Text())
	}
	if flag, _, ok := e.Limit(); !ok || flag != flagMaxQueryPlanTime {
		t.Fatalf("limit flag %q %v", flag, ok)
	}
	return e
}

// The budget stops the validation of a query in the walk of its tree and once the validation is complete, and a
// stopped validation stores nothing: not in the validation cache, not in the plan memo.
//
// conformance: query-complexity-limits
func TestPlanBudgetStopsValidationAndStoresNothing(t *testing.T) {
	emptyValidationCaches(t)
	deep := depthQuery(600) // more than budgetCheckEvery nodes: the walk asks the budget more than once
	for _, tc := range []struct {
		name    string
		query   string
		tripAt  int
		calls   int
		tripped bool
	}{
		{"in the walk, at the first node", `sum(rate({a="b"}[1m])) # walk`, 1, 1, true},
		{"in the walk, deep in the tree", deep, 2, 2, true},
		{"after the validation", `sum(rate({a="b"}[1m])) # after`, 2, 2, true},
		{"never", `sum(rate({a="b"}[1m])) # never`, 1000, 2, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := &countBudget{tripAt: tc.tripAt}
			v, tripped := cachedQueryValidationWithin(tc.query, b)
			if tripped != tc.tripped || b.calls != tc.calls {
				t.Fatalf("tripped %v after %d questions, want %v after %d", tripped, b.calls, tc.tripped, tc.calls)
			}
			_, cached := validationCache.Load(tc.query)
			_, memoised := planMemoLoad(tc.query)
			if tc.tripped && (cached || memoised || v != (queryValidation{})) {
				t.Errorf("a stopped validation left %+v, cached %v, plan memo %v", v, cached, memoised)
			}
			if !tc.tripped && (!cached || v.msg != "") {
				t.Errorf("a completed validation is cached: %v, msg %q", cached, v.msg)
			}
		})
	}
}

// The budget stops the lowering at a node, answers the typed 400 instead of falling back to the string translator,
// and leaves nothing: the translation is not cached, and the same query translates once the budget is off.
//
// conformance: query-complexity-limits
func TestPlanBudgetStopsTheLowering(t *testing.T) {
	rig := newComplexityRig(t, ExecutionLimitsConfig{MaxQueryPlanTime: 30 * time.Minute}, func(p *Proxy) { p.planClock = steppingClock(time.Hour) })
	p := rig.p
	ctx, rt := requestContext()
	const query = `{a="b"} |= "budget lowering"`
	_, err := p.translateQueryOpts(ctx, query, false)
	e := planTimeError(t, err)
	if !strings.Contains(e.Cause().Error(), "lowering") {
		t.Errorf("the cause does not name the check: %v", e.Cause())
	}
	if rt.plan == nil || rt.plan.err == nil {
		t.Errorf("trip state %+v", rt.plan)
	}
	if got := p.metrics.QueryComplexityRejectionCount("plan_time"); got != 1 {
		t.Errorf("counted %d", got)
	}
	if !strings.Contains(rig.metricsText(), "loki_vl_proxy_translation_errors_total 0\n") {
		t.Error("a budget refusal is counted as a translation error")
	}
	if _, cached := p.translationCache.Get(query); cached {
		t.Error("a refused translation is cached")
	}
	// Nothing was kept: the same query translates, to what a proxy without a budget writes.
	off := newComplexityRig(t, ExecutionLimitsConfig{MaxQueryPlanTime: -1}, nil).p
	want, err := off.translateQueryOpts(context.Background(), query, false)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := off.translateQueryOpts(ctx, query, false); err != nil || got != want {
		t.Fatalf("translation %q %v, want %q", got, err, want)
	}
}

// The check before the request's first VictoriaLogs call: the planning time spent so far against the limit, once per
// request, the same answer for every later call, and no call leaves the proxy.
//
// conformance: query-complexity-limits
func TestPlanBudgetBeforeTheFirstBackendCall(t *testing.T) {
	rig := newComplexityRig(t, ExecutionLimitsConfig{MaxQueryPlanTime: time.Second}, nil)
	p := rig.p
	vlParams := url.Values{"query": {"*"}}

	ctx, rt := requestContext()
	rt.plan, rt.flow.translateDur = &planBudget{validateDur: 600 * time.Millisecond}, 600*time.Millisecond
	hits := rig.backend.hits.Load()
	if _, err := p.vlGet(ctx, "/select/logsql/query", vlParams); err == nil {
		t.Fatal("a request over its planning budget reached VictoriaLogs")
	} else {
		planTimeError(t, err)
	}
	first := rt.plan.err
	if _, err := p.vlPost(ctx, "/select/logsql/query", vlParams); err != first {
		t.Errorf("a later call answered %v, want the first answer", err)
	}
	if rig.backend.hits.Load() != hits {
		t.Error("a refused request called VictoriaLogs")
	}
	if got := p.metrics.QueryComplexityRejectionCount("plan_time"); got != 1 {
		t.Errorf("counted %d, want 1 for the request", got)
	}

	// Within the budget at the first call: the request goes on, and later time is not checked again.
	ctx, rt = requestContext()
	rt.plan = &planBudget{validateDur: 100 * time.Millisecond}
	resp, err := p.vlGet(ctx, "/select/logsql/query", vlParams)
	if err != nil {
		t.Fatalf("a request within its budget: %v", err)
	}
	_ = resp.Body.Close()
	rt.flow.translateDur = time.Hour
	if err := p.planBudgetBeforeBackend(ctx); err != nil {
		t.Errorf("a later call of the same request: %v", err)
	}

	// The budget off checks nothing, and a context outside any request has no budget.
	p = newComplexityRig(t, ExecutionLimitsConfig{MaxQueryPlanTime: -1}, nil).p
	ctx, rt = requestContext()
	rt.flow.translateDur = time.Hour
	if err := p.planBudgetBeforeBackend(ctx); err != nil {
		t.Errorf("budget off: %v", err)
	}
	if err := p.planBudgetBeforeBackend(context.Background()); err != nil {
		t.Errorf("no request: %v", err)
	}
	if seg := p.beginPlan(context.Background(), p.limits()); seg != nil {
		t.Error("a budget for a context outside any request")
	}
}

// A request over its budget at the gate answers the neutral 400, the flag only in the log line, counted once, with
// nothing stored; the request after it plans again and answers the same.
//
// conformance: query-complexity-limits
func TestPlanBudgetGateAnswersNeutral400AndKeepsNothing(t *testing.T) {
	emptyValidationCaches(t)
	rig := newComplexityRig(t, ExecutionLimitsConfig{MaxQueryPlanTime: time.Nanosecond}, nil)
	query := `sum by (a) (rate({a="gate"} |= "budget gate" [1m]))`
	hits := rig.backend.hits.Load()
	for i := 1; i <= 2; i++ {
		status, body := rig.get(t, "/loki/api/v1/query_range", rangeParams(query))
		if status != http.StatusBadRequest || !strings.Contains(body, "query planning took longer than the limit") {
			t.Fatalf("request %d answered %d %q", i, status, body)
		}
		if strings.Contains(body, "max-query-plan-time") {
			t.Errorf("the body names the flag: %q", body)
		}
		if !strings.Contains(rig.metricsText(), `loki_vl_proxy_query_complexity_rejections_total{limit="plan_time"} `+string(rune('0'+i))) {
			t.Errorf("request %d: refusals not counted:\n%s", i, rig.metricsText())
		}
	}
	if rig.backend.hits.Load() != hits {
		t.Error("a refused request called VictoriaLogs")
	}
	logs := rig.logs.String()
	for _, want := range []string{"limit_flag=-max-query-plan-time", "limit_value=1", "validation check"} {
		if !strings.Contains(logs, want) {
			t.Errorf("the request error log lacks %q:\n%s", want, logs)
		}
	}
	lower := query
	if _, ok := validationCache.Load(lower); ok {
		t.Error("the validation of a refused request is cached")
	}
	if _, ok := requestTextCache.Load(query); ok {
		t.Error("the request text of a refused request is memoised")
	}
	if _, ok := planMemoLoad(lower); ok {
		t.Error("the plan memo holds a refused request's parse")
	}
	// The same query is answered once the budget is not exceeded.
	ok := newComplexityRig(t, ExecutionLimitsConfig{}, nil)
	if status, body := ok.get(t, "/loki/api/v1/query_range", rangeParams(query)); status != http.StatusOK {
		t.Errorf("the default budget answered %d %q", status, body)
	}
}

// Identical requests that arrive while one validates a long query share that validation: one run, the same 400 for
// every one of them. A request after the flight validates again: the verdict is not kept.
//
// conformance: query-complexity-limits
func TestPlanBudgetTripIsNotHeldAfterTheFlight(t *testing.T) {
	emptyValidationCaches(t)
	// A long query: 1 KiB or more of nested sums.
	query := depthQuery(400)
	if len(query) < planFlightMinBytes {
		t.Fatalf("the query is %d bytes, under the flight threshold %d", len(query), planFlightMinBytes)
	}
	const requests = 8
	var begun atomic.Int64 // every request reads the clock once when it opens its budget segment
	release := make(chan struct{})
	var started sync.Once
	leaderUp := make(chan struct{})
	rig := newComplexityRig(t, ExecutionLimitsConfig{MaxQueryPlanTime: time.Nanosecond}, func(p *Proxy) {
		p.planClock = func() time.Time { begun.Add(1); return time.Now() }
		p.planValidationHook = func() {
			started.Do(func() { close(leaderUp) })
			<-release
		}
	})
	type answer struct {
		status int
		body   string
	}
	answers := make(chan answer, requests)
	var wg sync.WaitGroup
	for i := 0; i < requests; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			status, body := rig.get(t, "/loki/api/v1/query_range", rangeParams(query))
			answers <- answer{status, body}
		}()
	}
	select {
	case <-leaderUp:
	case <-time.After(30 * time.Second):
		close(release)
		t.Fatal("no request reached the budgeted validation of the long query")
	}
	for deadline := time.Now().Add(30 * time.Second); begun.Load() < requests; runtime.Gosched() { // every request has opened its segment: the rest of the way to the flight is nanoseconds
		if time.Now().After(deadline) {
			close(release)
			t.Fatalf("%d of %d requests reached the gate", begun.Load(), requests)
		}
	}
	time.Sleep(20 * time.Millisecond)
	close(release)
	wg.Wait()
	close(answers)
	for a := range answers {
		if a.status != http.StatusBadRequest || !strings.Contains(a.body, "query planning took longer than the limit") {
			t.Errorf("answered %d %q", a.status, a.body)
		}
	}
	if got := rig.p.planValidations.Load(); got != 1 {
		t.Fatalf("%d identical requests ran %d validations, want 1", requests, got)
	}
	// After the flight the query validates again: nothing was held.
	rig.p.planValidationHook = nil
	if status, _ := rig.get(t, "/loki/api/v1/query_range", rangeParams(query)); status != http.StatusBadRequest {
		t.Errorf("a later request answered %d", status)
	}
	if got := rig.p.planValidations.Load(); got != 2 {
		t.Errorf("a request after the flight ran %d validations in all, want 2", got)
	}
}

// A budget refusal is a request-class failure: shared with the identical requests in flight and held for no time,
// so the next request runs the work again (and is refused again).
//
// conformance: query-complexity-limits
func TestPlanBudgetFailuresAreRequestClassAndNeverHeld(t *testing.T) {
	p := newTestProxy(t, "http://unused")
	for _, err := range []*lokierr.Error{
		lokierr.PlanTimeExceeded(flagMaxQueryPlanTime, 1, errors.New("cause")),
		lokierr.QueryTooComplex(false, 2, flagMaxQueryASTNodes, errors.New("cause")),
	} {
		if class, need := failureClassOf(context.Background(), err, err); class != cache.ErrorRequest || need != 0 {
			t.Errorf("%v: class %v need %v, want the request class", err.Msg().ID(), class, need)
		}
	}
	pol, _ := p.errorPolicy("query_range", requestOnRoute("query_range", "/loki/api/v1/query_range?query=1", context.Background()))
	if ttl := pol.TTL(cache.ErrorRequest); ttl != 0 {
		t.Errorf("the request class is held for %s, want 0", ttl)
	}
	if !pol.Shares(cache.ErrorRequest) {
		t.Error("followers in flight are not handed the request-class failure")
	}
}

func TestPlanBudgetMetricsAndLimitsAreBounded(t *testing.T) {
	p := newTestProxy(t, "http://unused")
	p.metrics.RecordQueryComplexityRejection("a-limit-nobody-declared")
	for _, name := range []string{"ast_nodes", "nesting_depth", "plan_time"} {
		if got := p.metrics.QueryComplexityRejectionCount(name); got != 0 {
			t.Errorf("%s counted %d", name, got)
		}
	}
	if limits := resolveExecutionLimits(ExecutionLimitsConfig{}); limits.QueryPlanTime != DefaultMaxQueryPlanTime {
		t.Errorf("default budget %s", limits.QueryPlanTime)
	}
	if limits := resolveExecutionLimits(ExecutionLimitsConfig{MaxQueryPlanTime: -1}); limits.QueryPlanTime != 0 {
		t.Errorf("budget off is %s", limits.QueryPlanTime)
	}
	if limits := resolveExecutionLimits(ExecutionLimitsConfig{MaxQueryPlanTime: time.Second}); limits.QueryPlanTime != time.Second {
		t.Errorf("budget %s", limits.QueryPlanTime)
	}
}

// A request that is over its planning budget after the gate, in the lowering or before its first VictoriaLogs call,
// answers exactly the neutral 400 on the route's own chain: no flag or value in the body, no VictoriaLogs call, no
// retry, nothing held by the error flights, and not read as a backend failure (a 500 held for a second) nor as a
// translation error.
//
// conformance: query-complexity-limits
func TestPlanBudgetRefusalsAfterTheGateAreTheNeutral400(t *testing.T) {
	const limit = time.Hour
	var jumping atomic.Bool // the clock jumps two hours at every reading while a handler for the lowering stage runs
	var offset atomic.Int64
	rig := newComplexityRigRoutes(t, ExecutionLimitsConfig{MaxQueryPlanTime: limit}, func(p *Proxy) {
		base := time.Unix(1_700_000_000, 0)
		p.planClock = func() time.Time {
			if jumping.Load() {
				offset.Add(int64(2 * time.Hour))
			}
			return base.Add(time.Duration(offset.Load()))
		}
	}, func(p *Proxy, mux *http.ServeMux) {
		// The wrapper runs behind the gate, so the budget is met by the stage under test only.
		wrap := func(next http.HandlerFunc) http.HandlerFunc {
			return func(w http.ResponseWriter, r *http.Request) {
				switch r.Header.Get("X-Test-Stage") {
				case "lowering":
					jumping.Store(true)
					defer jumping.Store(false)
				case "backend":
					if rt := getRequestTelemetry(r.Context()); rt != nil {
						rt.mu.Lock()
						rt.plan = &planBudget{validateDur: 2 * limit}
						rt.mu.Unlock()
					}
				}
				next(w, r)
			}
		}
		mux.Handle("/loki/api/v1/query_range", p.routeHandler("query_range", "/loki/api/v1/query_range", wrap(p.handleQueryRange)))
		mux.Handle("/loki/api/v1/labels", p.routeHandler("labels", "/loki/api/v1/labels", wrap(p.handleLabels)))
	})
	get := func(path, stage string, params url.Values) (int, string) {
		req, _ := http.NewRequest(http.MethodGet, rig.base+path+"?"+params.Encode(), nil)
		req.Header.Set("X-Test-Stage", stage)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = resp.Body.Close() }()
		body, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, string(body)
	}
	now := time.Now().Unix()
	window := url.Values{"start": {strconv.FormatInt(now-300, 10)}, "end": {strconv.FormatInt(now, 10)}}
	cases := []struct {
		name, path, stage string
		params            url.Values
	}{
		{"log query, lowering", "/loki/api/v1/query_range", "lowering", rangeParams(`{a="b"} |= "after the gate"`)},
		{"metric query, before the backend", "/loki/api/v1/query_range", "backend", rangeParams(`sum by (a) (rate({a="b"} |= "after the gate" [1m]))`)},
		{"log query, before the backend", "/loki/api/v1/query_range", "backend", rangeParams(`{a="b"} |= "after the gate, again"`)},
		{"labels, before the backend", "/loki/api/v1/labels", "backend", window},
	}
	hits := rig.backend.hits.Load()
	for i, tc := range cases {
		status, body := get(tc.path, tc.stage, tc.params)
		if status != http.StatusBadRequest || !strings.Contains(body, "query planning took longer than the limit") {
			t.Errorf("%s: answered %d %q", tc.name, status, body)
		}
		if strings.Contains(body, "max-query-plan-time") || strings.Contains(body, "3600000000000") {
			t.Errorf("%s: the body names the flag or its value: %q", tc.name, body)
		}
		if got := rig.p.metrics.QueryComplexityRejectionCount("plan_time"); got != int64(i+1) {
			t.Errorf("%s: %d refusals counted, want %d (one per request, no retry)", tc.name, got, i+1)
		}
	}
	if got := rig.backend.hits.Load(); got != hits {
		t.Errorf("%d VictoriaLogs calls for refused requests", got-hits)
	}
	if entries, _ := rig.p.errorFlights.Len(); entries != 0 {
		t.Errorf("the error flights hold %d refusals", entries)
	}
	if logs := rig.logs.String(); !strings.Contains(logs, "limit_flag=-max-query-plan-time") || strings.Contains(logs, "backend") && strings.Contains(logs, "kind=backend") {
		t.Errorf("request error log:\n%s", logs)
	}
	if !strings.Contains(rig.metricsText(), "loki_vl_proxy_translation_errors_total 0\n") {
		t.Error("a budget refusal is counted as a translation error")
	}
	// The same requests are answered when nothing is over the budget.
	for _, tc := range cases {
		if status, body := get(tc.path, "", tc.params); status != http.StatusOK {
			t.Errorf("%s: without the stage answered %d %q", tc.name, status, body)
		}
	}
}

// The edges of the budget: a trip is made once per request, a budget that is not open is never exhausted, a query
// the validation cache cannot hold is validated for the request with its time counted, a full validation cache
// leaves a short query to its readers, and a query over 4 KiB is validated under the budget where the rewrite
// validates it anyway.
//
// conformance: query-complexity-limits
func TestPlanBudgetEdges(t *testing.T) {
	emptyValidationCaches(t)
	rig := newComplexityRig(t, ExecutionLimitsConfig{MaxQueryPlanTime: time.Second}, nil)
	p := rig.p

	if err := p.planTrip(context.Background(), "x", nil); err != nil {
		t.Errorf("a trip outside any request: %v", err)
	}
	ctx, rt := requestContext()
	first := p.planTrip(ctx, "first", nil)
	if second := p.planTrip(ctx, "second", nil); second != first || !strings.Contains(planTimeError(t, first).Cause().Error(), "first") {
		t.Errorf("a request trips once: %v then %v", first, second)
	}
	if got := p.metrics.QueryComplexityRejectionCount("plan_time"); got != 1 {
		t.Errorf("counted %d", got)
	}
	var none *planSegment
	if none.Exhausted() || budgetOf(none) != nil {
		t.Error("no segment is never exhausted")
	}
	none.end(true)
	_ = rt

	// A query too long for the cache is validated for the request, its time counted toward the budget.
	long := strings.Repeat("(", 3000) + "1" + strings.Repeat(")", 3000)
	if len(long) <= validationCacheMaxQueryBytes {
		t.Fatalf("the query is %d bytes, it must be too long to cache", len(long))
	}
	ctx3, rt3 := requestContext()
	rt3.plan = &planBudget{validateDur: 2 * time.Second}
	rec := httptest.NewRecorder()
	if _, ok := p.validateQueryCtx(ctx3, rec, long, "query_range"); ok || rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), "query planning took longer than the limit") {
		t.Errorf("a request over its budget after validating a long query: ok %v, %d %q", ok, rec.Code, rec.Body.String())
	}
	ctx4, rt4 := requestContext()
	rec = httptest.NewRecorder()
	if _, ok := p.validateQueryCtx(ctx4, rec, long, "query_range"); !ok || rec.Code != http.StatusOK {
		t.Errorf("a request within its budget: ok %v, %d %q", ok, rec.Code, rec.Body.String())
	}
	if rt4.plan == nil || rt4.plan.validateDur <= 0 {
		t.Error("the validation of a long query was not counted")
	}

	// Through the chain: a long query is validated under the budget at the gate where the rewrite validates it, and a
	// full validation cache leaves a short query to the readers after it.
	tiny := newComplexityRig(t, ExecutionLimitsConfig{MaxQueryPlanTime: time.Nanosecond}, nil)
	if status, body := tiny.get(t, "/loki/api/v1/query_range", rangeParams(long)); status != http.StatusBadRequest || !strings.Contains(body, "query planning took longer than the limit") {
		t.Errorf("a long query over the budget answered %d %q", status, body)
	}
	validationCacheSize.Add(validationCacheMaxSize)
	defer validationCacheSize.Add(-validationCacheMaxSize)
	full := `sum by (a) (rate({a="saturated"} |= "full cache" [1m]))`
	// The gate leaves the short query to its validator, which counts its time: over a budget of a nanosecond it is
	// refused there, within a second it is answered, and neither is stored.
	if status, body := tiny.get(t, "/loki/api/v1/query_range", rangeParams(full)); status != http.StatusBadRequest || !strings.Contains(body, "query planning took longer than the limit") {
		t.Errorf("a short query over the budget, the validation cache full, answered %d %q", status, body)
	}
	if !strings.Contains(tiny.logs.String(), "by the validation check") {
		t.Errorf("the refusal was not made by the validator:\n%s", tiny.logs.String())
	}
	if status, body := rig.get(t, "/loki/api/v1/query_range", rangeParams(full)); status != http.StatusOK {
		t.Errorf("a short query within the budget, the validation cache full, answered %d %q", status, body)
	}
	if _, ok := validationCache.Load(full); ok {
		t.Error("a full validation cache took an entry")
	}
}

// Segments of one request that overlap (detected_fields translates in parallel) each keep their own clock: one that
// ends does not switch the budget off for the other, and each counts its own time on top of what the request has
// already spent.
//
// conformance: query-complexity-limits
func TestPlanSegmentsOfOneRequestAreIndependent(t *testing.T) {
	var mu sync.Mutex
	now := time.Unix(1_700_000_000, 0)
	advance := func(d time.Duration) { mu.Lock(); now = now.Add(d); mu.Unlock() }
	rig := newComplexityRig(t, ExecutionLimitsConfig{MaxQueryPlanTime: time.Second}, func(p *Proxy) {
		p.planClock = func() time.Time { mu.Lock(); defer mu.Unlock(); return now }
	})
	p := rig.p
	ctx, rt := requestContext()
	a, b := p.beginPlan(ctx, p.limits()), p.beginPlan(ctx, p.limits())
	if a == nil || b == nil || a == b {
		t.Fatal("each begin opens its own segment")
	}
	advance(600 * time.Millisecond)
	if a.Exhausted() || b.Exhausted() {
		t.Fatal("exhausted inside the budget")
	}
	done := make(chan struct{})
	go func() { // the first segment ends in another goroutine, as a parallel translation does
		defer close(done)
		a.end(true)
	}()
	<-done
	advance(500 * time.Millisecond)
	if !b.Exhausted() {
		t.Fatal("the second segment is not exhausted after the first one ended: its own 1.1 s is over the second")
	}
	if a.Exhausted() {
		t.Error("a closed segment is exhausted")
	}
	b.end(true)
	if rt.plan.validateDur != 600*time.Millisecond+1100*time.Millisecond {
		t.Errorf("validation time %s, want both segments' own time", rt.plan.validateDur)
	}
}

// A rejection Loki makes is final even when the budget runs out as the validation ends: the query is answered with
// Loki's error (and the rejection stored), and the budget refuses only an accepted query.
//
// conformance: query-complexity-limits
func TestPlanBudgetNeverReplacesLokisRejection(t *testing.T) {
	emptyValidationCaches(t)
	const bad = `sum(rate({a="b"}[1m])) by (`
	v, tripped := cachedQueryValidationWithin(bad, &countBudget{tripAt: 1})
	if tripped || v.msg == "" {
		t.Fatalf("a rejected query tripped the budget: %+v %v", v, tripped)
	}
	if _, stored := validationCache.Load(bad); !stored {
		t.Error("Loki's rejection is not stored")
	}
	rig := newComplexityRig(t, ExecutionLimitsConfig{MaxQueryPlanTime: time.Nanosecond}, nil)
	if status, body := rig.get(t, "/loki/api/v1/query_range", rangeParams(`{a="b"} | json |= `)); status != http.StatusBadRequest || strings.Contains(body, "planning took longer") {
		t.Errorf("a query Loki rejects answered %d %q", status, body)
	}
}

// The budget is asked while a query is parsed, not only in the walk of the tree checks: a query long enough to reach
// the first question (1,024 steps) is asked about during its validation, and a trip there (on a query that parses)
// leaves nothing behind, as one in the walk does. A query of 1,500 json stages is one node to the walk, which asks
// once.
//
// conformance: query-complexity-limits
func TestPlanBudgetAskedWhileParsing(t *testing.T) {
	emptyValidationCaches(t)
	q := `{a="parse-budget"}` + strings.Repeat(" | json", 1500)
	never := &countBudget{tripAt: 1 << 30}
	if _, tripped := cachedQueryValidationWithin(q, never); tripped {
		t.Fatal("a budget that never runs out tripped")
	}
	if never.calls <= 2 {
		t.Fatalf("the validation asked the budget %d times: the parse did not ask it", never.calls)
	}
	emptyValidationCaches(t)
	q2 := `{a="parse-budget-trip"}` + strings.Repeat(" | json", 1500)
	b := &countBudget{tripAt: 1}
	v, tripped := cachedQueryValidationWithin(q2, b)
	if !tripped || b.calls != 1 || v != (queryValidation{}) {
		t.Fatalf("tripped %v after %d questions with %+v, want a trip at the parser's first question and nothing else", tripped, b.calls, v)
	}
	_, cached := validationCache.Load(q2)
	_, memoised := planMemoLoad(q2)
	if cached || memoised {
		t.Errorf("a validation the budget stopped left a result (cached %v, plan memo %v)", cached, memoised)
	}
}

// On a request, the route's own validator (validateTimed) hands the request's budget to the parser too: a query too
// long for the validation cache and with no call to respell (not validated at the gate) is asked about while it is
// parsed. A clock that counts its readings sees the parse's questions on top of the ones a short query costs.
//
// conformance: query-complexity-limits
func TestPlanBudgetReachesTheRouteValidatorsParse(t *testing.T) {
	emptyValidationCaches(t)
	var reads atomic.Int64
	rig := newComplexityRig(t, ExecutionLimitsConfig{MaxQueryPlanTime: time.Hour}, func(p *Proxy) {
		p.planClock = func() time.Time { reads.Add(1); return time.Now() }
	})
	count := func(query string) int64 {
		before := reads.Load()
		if status, body := rig.get(t, "/loki/api/v1/query_range", rangeParams(query)); status != http.StatusOK {
			t.Fatalf("%.60q: %d %s", query, status, body)
		}
		return reads.Load() - before
	}
	short := count(`{a="route-parse-short"} | json`)
	long := `{a="route-parse-long"}` + strings.Repeat(" | json", 1500)
	if len(long) <= validationCacheMaxQueryBytes || strings.IndexByte(long, '(') >= 0 {
		t.Fatal("the long query must skip the gate's validation")
	}
	if got := count(long); got < short+3 {
		t.Fatalf("the long query read the clock %d times, the short one %d: its parse did not ask the budget", got, short)
	}
}

// Through the whole handler chain, a 2 ms budget answers the deep shapes with its 400, makes no VictoriaLogs call, and
// stops before the handlers' readers of the translated text: the trip is noticed by the validation, the lowering or
// the check right after the string translator (which it is not interrupted in: nested vector aggregations), never
// only by the check before the first VictoriaLogs call, which the readers would have reached first. The time guard
// is loose (a CI runner under -race is many times slower than a laptop): the checks are what is pinned.
//
// conformance: query-complexity-limits
func TestPlanBudgetAnswersDeepShapesSoon(t *testing.T) {
	emptyValidationCaches(t)
	rig := newComplexityRig(t, ExecutionLimitsConfig{MaxQueryPlanTime: 2 * time.Millisecond}, nil)
	for _, tc := range []struct{ name, query string }{
		{"parentheses", strings.Repeat("(", 60000) + "1" + strings.Repeat(")", 60000)},
		{"binary", strings.Repeat("1+", 60000) + "1"},
		{"json stages", `{a="b"}` + strings.Repeat(" | json", 18000)},
		{"label_replace", strings.Repeat("label_replace(", 2500) + `rate({a="b"}[1m])` + strings.Repeat(`, "d", "$1", "s", "(.*)")`, 2500)},
		{"nested sums", strings.Repeat("sum(", 6000) + `rate({a="b"}[1m])` + strings.Repeat(")", 6000)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			before, logged := rig.backend.hits.Load(), len(rig.logs.String())
			start := time.Now()
			status, body := rig.get(t, "/loki/api/v1/query_range", rangeParams(tc.query))
			if took := time.Since(start); took > 10*time.Second {
				t.Errorf("answered in %v", took)
			}
			if status != http.StatusBadRequest || !strings.Contains(body, "query planning took longer than the limit") {
				t.Fatalf("answered %d %.200s", status, body)
			}
			if calls := rig.backend.hits.Load() - before; calls != 0 {
				t.Errorf("%d VictoriaLogs calls", calls)
			}
			logs := rig.logs.String()[logged:]
			if strings.Contains(logs, "before the first VictoriaLogs call check") {
				t.Error("the trip was noticed only before the first VictoriaLogs call: the handlers' readers ran")
			}
			if !strings.Contains(logs, "by the validation check") && !strings.Contains(logs, "by the lowering check") && !strings.Contains(logs, "by the translation check") {
				t.Errorf("no trip line names the validation, lowering or translation check: %.300s", logs)
			}
		})
	}
}

// A selector route's query is left to its validators at the gate (no parse and no cache slot added to them); the
// budget still bounds the route before its first VictoriaLogs call.
//
// conformance: query-complexity-limits
func TestPlanGateLeavesSelectorRoutesToTheirValidators(t *testing.T) {
	emptyValidationCaches(t)
	p := newComplexityRig(t, ExecutionLimitsConfig{MaxQueryPlanTime: time.Hour}, nil).p
	at := func(endpoint, query string) {
		ctx, _ := requestContext()
		r := requestOnRoute(endpoint, "/loki/api/v1/"+endpoint, ctx)
		if _, err := p.requestTextUnderBudget(r, query, p.limits()); err != nil {
			t.Fatal(err)
		}
	}
	at("labels", `{a="selector-gate"}`)
	if _, stored := validationCache.Load(`{a="selector-gate"}`); stored {
		t.Error("the gate validated the query of a selector route: that adds a parse and a cache entry to its validators")
	}
	at("query_range", `{a="expression-gate"} |= "x"`)
	if _, stored := validationCache.Load(`{a="expression-gate"} |= "x"`); !stored {
		t.Error("the gate left the query of an expression route unvalidated, so its validation would not be counted")
	}
	// A call the rewrite may respell is validated on any route: the rewrite does it anyway.
	const spelled = `sum (rate({a="candidate-gate"}[1m]))`
	at("labels", spelled)
	if _, stored := validationCache.Load(spelled); !stored {
		t.Error("a query the rewrite validates anyway was not validated under the budget")
	}
}

// A query refused by a planning budget on a drilldown route is a plain 400 with no VictoriaLogs call, and no
// response is cached: those routes swallow errors and fall back elsewhere, and none may turn a refusal into a cached
// partial 200.
//
// conformance: query-complexity-limits
func TestPlanBudgetRefusesDrilldownRoutesWithoutCaching(t *testing.T) {
	const limit = time.Hour
	rig := newComplexityRigRoutes(t, ExecutionLimitsConfig{MaxQueryPlanTime: limit}, nil, func(p *Proxy, mux *http.ServeMux) {
		wrap := func(next http.HandlerFunc) http.HandlerFunc {
			return func(w http.ResponseWriter, r *http.Request) {
				if rt := getRequestTelemetry(r.Context()); rt != nil { // the request has spent its budget before any call
					rt.mu.Lock()
					rt.plan = &planBudget{validateDur: 2 * limit}
					rt.mu.Unlock()
				}
				next(w, r)
			}
		}
		for _, route := range []struct {
			endpoint, path string
			h              http.HandlerFunc
		}{
			{"detected_fields", "/loki/api/v1/detected_fields", p.handleDetectedFields},
			{"detected_labels", "/loki/api/v1/detected_labels", p.handleDetectedLabels},
			{"patterns", "/loki/api/v1/patterns", p.handlePatterns},
			{"volume", "/loki/api/v1/index/volume", p.handleVolume},
			{"volume_range", "/loki/api/v1/index/volume_range", p.handleVolumeRange},
			{"series", "/loki/api/v1/series", p.handleSeries},
		} {
			mux.Handle(route.path, p.routeHandler(route.endpoint, route.path, wrap(route.h)))
		}
	})
	now := time.Now().Unix()
	for _, route := range []string{"detected_fields", "detected_labels", "patterns", "index/volume", "index/volume_range", "series"} {
		key := "query"
		if route == "series" {
			key = "match[]"
		}
		params := url.Values{key: {`{a="drilldown"}`}, "start": {strconv.FormatInt(now-300, 10)}, "end": {strconv.FormatInt(now, 10)}, "step": {"60"}}
		hits := rig.backend.hits.Load()
		status, body := rig.get(t, "/loki/api/v1/"+route, params)
		if status != http.StatusBadRequest || !strings.Contains(body, "query planning took longer than the limit") {
			t.Errorf("%s: answered %d %q", route, status, body)
		}
		if got := rig.backend.hits.Load(); got != hits {
			t.Errorf("%s: %d VictoriaLogs calls for a refused request", route, got-hits)
		}
	}
	if entries, _ := rig.p.cache.Size(); entries != 0 {
		t.Errorf("the response cache holds %d entries after refusals", entries)
	}
	if rig.p.compatCache != nil {
		if entries, _ := rig.p.compatCache.Size(); entries != 0 {
			t.Errorf("the compat cache holds %d entries after refusals", entries)
		}
	}
}

// The route validator's budgeted validation of a cacheable query stores nothing when the request is over its budget
// (the walk is cut or the finished validation is refused), and stores the validation when it is not.
//
// conformance: query-complexity-limits
func TestValidateTimedStoresNothingForARefusedRequest(t *testing.T) {
	emptyValidationCaches(t)
	p := newComplexityRig(t, ExecutionLimitsConfig{MaxQueryPlanTime: time.Second}, nil).p
	const refused, accepted = `sum by (a) (rate({a="timed-refused"}[1m]))`, `sum by (a) (rate({a="timed-accepted"}[1m]))`
	ctx, rt := requestContext()
	rt.plan = &planBudget{validateDur: 2 * time.Second}
	if _, err := p.validateTimed(ctx, refused); err == nil {
		t.Fatal("a request over its budget was not refused")
	} else {
		planTimeError(t, err)
	}
	if _, stored := validationCache.Load(refused); stored {
		t.Error("the validation of a refused request is stored")
	}
	if _, memoised := planMemoLoad(refused); memoised {
		t.Error("the parse of a refused request is in the plan memo")
	}
	ctx, _ = requestContext()
	if v, err := p.validateTimed(ctx, accepted); err != nil || v.msg != "" {
		t.Fatalf("a request within its budget: %v %q", err, v.msg)
	}
	if _, stored := validationCache.Load(accepted); !stored {
		t.Error("the validation of an accepted request is not stored")
	}
}
