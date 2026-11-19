package proxy

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/lokierr"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan"
)

// The planning budget (-max-query-plan-time) bounds the proxy's own work on a request before it asks VictoriaLogs:
// the validation misses of its query (parse, semantic pass, tree checks) and its translation (the query plan's
// lowering and, where it falls back, the string translator). Waits for admission, the cache tiers and every
// VictoriaLogs call are not counted. The time is kept in the request's telemetry (the validation time here and the
// flow's translateDur), so a request whose queries are all answered from the caches spends none.
//
// It is checked while a query is validated: in the parser, every parser it makes for a call it reads again and its
// passes over the tree, every 256 tokens or nodes after the first 1,024 (logql.ValidateLogQLPhasesWithin: a shorter
// query's verdict is always made), and in the walk of its tree checks every 256 nodes (queryplan.ValidateWithin); then
// after the validation, at every node of the lowering (lower.Profile.Budget), and before the request's first
// VictoriaLogs call (native tail's own stream call, made after its websocket upgrade, is not one of them). The string
// translator is not interrupted where the lowering leaves a query to it, nor are the request's spelling rewrites around
// the validation: their time counts once they return, at the next check. A request over the budget answers a neutral 400
// through the error layer, the flag in the log line only, and leaves nothing behind: its validation is not stored
// (validationCache, plan memo), its translation is not stored, and the answer is classified like any request limit,
// never held.

// planBudget is a request's budget state, inside its telemetry and guarded by its mutex.
type planBudget struct {
	// validateDur is the time the request's validation misses took (a query's parse, semantic pass and tree checks;
	// a cache hit costs none). With the request's translation time (the flow's translateDur) it is its own
	// planning time, which the budget bounds; neither is logged.
	validateDur time.Duration
	// err is the request's trip, reported again by every later check of the request.
	err error
}

// planSegment is one stretch of the request's own planning work. Segments of one request may overlap (detected_fields
// translates in parallel), so each carries its own start and ends alone. It is the queryplan.Budget the walks ask: its
// own elapsed time and the request's closed validation and translation time against the limit.
type planSegment struct {
	rt     *requestTelemetry
	limit  time.Duration
	clock  func() time.Time
	start  time.Time
	closed bool // guarded by rt.mu
}

func (s *planSegment) now() time.Time {
	if s.clock != nil {
		return s.clock()
	}
	return time.Now()
}

// beginPlan opens a segment of the request's own planning work; nil when the budget is off or ctx belongs to no
// request.
func (p *Proxy) beginPlan(ctx context.Context, lim executionLimits) *planSegment {
	if lim.QueryPlanTime <= 0 {
		return nil
	}
	rt := getRequestTelemetry(ctx)
	if rt == nil {
		return nil
	}
	s := &planSegment{rt: rt, limit: lim.QueryPlanTime, clock: p.planClock}
	s.start = s.now()
	rt.mu.Lock()
	if rt.plan == nil {
		rt.plan = &planBudget{}
	}
	rt.mu.Unlock()
	return s
}

// end closes the segment; validated adds its time to the request's validation time (a translation segment's time is
// counted by the translation itself).
func (s *planSegment) end(validated bool) {
	if s == nil {
		return
	}
	s.rt.mu.Lock()
	if !s.closed {
		s.closed = true
		if validated {
			s.rt.plan.validateDur += s.now().Sub(s.start)
		}
	}
	s.rt.mu.Unlock()
}

// Exhausted reports that the request's planning time, its closed validation and translation time plus this open
// segment, is over the limit (queryplan.Budget).
func (s *planSegment) Exhausted() bool {
	if s == nil {
		return false
	}
	s.rt.mu.Lock()
	defer s.rt.mu.Unlock()
	if s.closed {
		return false
	}
	return s.rt.planSpent()+s.now().Sub(s.start) > s.limit
}

// planSpent is the request's closed planning time; the caller holds rt.mu.
func (rt *requestTelemetry) planSpent() time.Duration {
	spent := rt.flow.translateDur
	if rt.plan != nil {
		spent += rt.plan.validateDur
	}
	return spent
}

// budgetOf is s as a queryplan.Budget, nil (not a typed nil) for no budget.
func budgetOf(s *planSegment) queryplan.Budget {
	if s == nil {
		return nil
	}
	return s
}

// planTrip is the request's planning-budget error, made once: the typed 400 with the flag and limit for the log
// line, the checkpoint and the time spent as its cause, and the rejection counted. seg is the segment that noticed it
// (nil for a check outside any).
func (p *Proxy) planTrip(ctx context.Context, checkpoint string, seg *planSegment) error {
	rt := getRequestTelemetry(ctx)
	if rt == nil {
		return nil
	}
	rt.mu.Lock()
	defer rt.mu.Unlock()
	if rt.plan == nil {
		rt.plan = &planBudget{}
	}
	b := rt.plan
	if b.err != nil {
		return b.err
	}
	limit := p.limits().QueryPlanTime
	spent := rt.planSpent()
	if seg != nil && !seg.closed {
		spent += seg.now().Sub(seg.start)
	}
	b.err = lokierr.PlanTimeExceeded(flagMaxQueryPlanTime, int64(limit),
		fmt.Errorf("planning took %s by the %s check, over the limit of %s", spent.Round(time.Millisecond), checkpoint, limit))
	p.metrics.RecordQueryComplexityRejection("plan_time")
	return b.err
}

// planBudgetBeforeBackend is the check before the request's first VictoriaLogs call: the planning time spent so far
// against the limit. A request that tripped earlier answers the same error again. Later calls of the request read
// the first answer.
func (p *Proxy) planBudgetBeforeBackend(ctx context.Context) error {
	limit := p.limits().QueryPlanTime
	if limit <= 0 {
		return nil
	}
	rt := getRequestTelemetry(ctx)
	if rt == nil {
		return nil
	}
	rt.mu.Lock()
	if rt.planChecked {
		var err error
		if rt.plan != nil {
			err = rt.plan.err
		}
		rt.mu.Unlock()
		return err
	}
	rt.planChecked = true
	spent := rt.planSpent()
	rt.mu.Unlock()
	if spent > limit {
		return p.planTrip(ctx, "before the first VictoriaLogs call", nil)
	}
	return nil
}

// planFlightMinBytes is the shortest query whose budgeted validation identical requests in flight share: below it
// a validation costs microseconds, and an allocation to share one is not worth it.
const planFlightMinBytes = 1024

// validateUnderBudget is cachedQueryValidationWithin for the request's budget. A long query that misses the cache
// is validated once for the requests that send it at the same time (validationGroup, per proxy so every follower
// reads the limit of its own proxy): the leader's budget decides, and every follower gets the same verdict. Nothing
// is stored on a trip, so a request that comes after the flight validates again.
func (p *Proxy) validateUnderBudget(query string, seg *planSegment) (queryValidation, bool) {
	budget := budgetOf(seg)
	if len(query) < planFlightMinBytes {
		return cachedQueryValidationWithin(query, budget)
	}
	if len(query) <= validationCacheMaxQueryBytes {
		if v, ok := validationCache.Load(query); ok {
			if cached, ok := v.(queryValidation); ok {
				return cached, false
			}
		}
	}
	type outcome struct {
		v         queryValidation
		exhausted bool
	}
	res, _, _ := p.validationGroup.Do(query, func() (any, error) {
		p.planValidations.Add(1)
		if p.planValidationHook != nil {
			p.planValidationHook() // tests hold the leader here until the followers have joined
		}
		v, exhausted := cachedQueryValidationWithin(query, budget)
		return outcome{v, exhausted}, nil
	})
	o := res.(outcome)
	return o.v, o.exhausted
}

// requestTextUnderBudget is the request's query text as the handlers read it (requestQueryText) with the first
// sight of the query validated under the planning budget: the keyword-lower-cased text, which the request rewrite and
// every validator after it read from the cache, so the budget covers the work they would have done and the cache
// holds exactly what it held before. A query the request text memo knows costs one lookup. The error is the typed
// 400 of a trip; a query over the length limit is left to the length check, and a query the validation rejects to
// its error.
func (p *Proxy) requestTextUnderBudget(r *http.Request, query string, lim executionLimits) (string, error) {
	if lim.QueryPlanTime <= 0 {
		return requestQueryText(query, lim.QueryLengthBytes), nil
	}
	if len(query) <= validationCacheMaxQueryBytes {
		if v, ok := requestTextCache.Load(query); ok {
			if text, ok := v.(string); ok {
				return text, nil
			}
		}
	}
	if len(query) <= validationCacheMaxQueryBytes && validationCacheSize.Load() >= validationCacheMaxSize {
		// The validation cache takes no more entries: the readers after this one would validate the query again, so a
		// validation made here would only be added to theirs. A query this short validates in well under a
		// millisecond, which is no work for a budget.
		return requestQueryText(query, lim.QueryLengthBytes), nil
	}
	ctx := r.Context()
	seg := p.beginPlan(ctx, lim)
	if seg == nil {
		return requestQueryText(query, lim.QueryLengthBytes), nil
	}
	text, tripped := requestQueryTextWithin(query, lim.QueryLengthBytes, p, seg, gateAll(r))
	seg.end(true)
	if tripped {
		return "", p.planTrip(ctx, "validation", seg)
	}
	return text, nil
}

// validateTimed is cachedQueryValidation for a request: a query the validation cache holds costs the lookup, and one
// it does not hold (too long to cache, or the cache is full) is validated under the request's planning budget, with
// nothing stored for a trip; the error is the typed 400 of a request that is over it.
func (p *Proxy) validateTimed(ctx context.Context, query string) (queryValidation, error) {
	if len(query) <= validationCacheMaxQueryBytes {
		if v, ok := validationCache.Load(query); ok {
			if cached, ok := v.(queryValidation); ok {
				return cached, nil
			}
		}
	}
	lim := p.limits()
	seg := p.beginPlan(ctx, lim)
	if seg == nil {
		return cachedQueryValidation(query), nil
	}
	v, exhausted := cachedQueryValidationWithin(query, budgetOf(seg))
	seg.end(true)
	if exhausted {
		return queryValidation{}, p.planTrip(ctx, "validation", seg)
	}
	return v, nil
}

// isPlanTimeError reports the planning budget's typed refusal.
func isPlanTimeError(err error) bool {
	e, ok := lokierr.As(err)
	return ok && e.Msg() == lokierr.MsgPlanTime
}

// gateAll says the request's route reads a whole expression (query, query_range, format_query): its first sight is
// validated under the budget whatever its spelling. The selector routes validate a stream selector, which costs a
// parse of a few matchers, and are validated at the gate only where the rewrite validates the query anyway.
func gateAll(r *http.Request) bool {
	switch requestRouteMetaFromContext(r.Context()).endpoint {
	case "query", "query_range", "format_query":
		return true
	}
	return false
}

// uncountPlanWait takes the time a request waited for another request's translation out of its own planning time:
// the translation's duration is recorded for the operation, but it is the leader's work, not this request's.
func uncountPlanWait(ctx context.Context, d time.Duration) {
	if rt := getRequestTelemetry(ctx); rt != nil {
		rt.mu.Lock()
		rt.flow.translateDur = max(rt.flow.translateDur-d, 0)
		rt.mu.Unlock()
	}
}
