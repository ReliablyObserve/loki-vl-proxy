package proxy

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	logqlpkg "github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/lower"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
)

// The handlers' side of the dynamic identity of a parser (lower.DynamicUnwrapIdentity): sum, max, min or avg over a
// range function taken per series of an unwrapped label after a json or logfmt parser with no keep, whose series are
// the stream and every parsed label less the unwrapped one. The lowered query writes them as a hash of the stream and
// each line's parsed labels (__lvp_sid), and VictoriaLogs answers the function per series and the aggregation over
// them, on an instant query and on a range up to the step. On a range longer than the step the window evaluator
// takes the function per series hash and per anchored bucket from VictoriaLogs and merges each series' windows, and
// the outer aggregation folds the series (dynamicIdentityWindows, lower.Profile IdentityAhead).

// dynamicQuery is the text the handlers, the fence and the translation read a query as: trimmed, with the keywords in
// lower case (logql.LowercaseKeywords, which the translation applies before it asks the lowering), so `SUM by (a)
// (AVG_OVER_TIME(... | JSON | UNWRAP x [5m]))` is decided as the lower-case spelling is, by all three.
func dynamicQuery(query string) string {
	return logqlpkg.LowercaseKeywords(strings.TrimSpace(query))
}

// dynamicUnwrapIdentity reports a query whose series are the dynamic identity of a parser
// (lower.DynamicUnwrapIdentity) and which the typed lowering serves (dynamicIdentityConstruct): its stats group by the
// stream and the hash of each line's parsed labels, and the handlers read those groups as the series. With
// -query-plan-lowering=false, or for a query the lowering turns away, the string translator serves it and no handler
// reads the identity, so the handlers and the translation decide alike. The verdict is remembered per proxy and query
// (dynamicIdentityVerdicts): one request asks several times.
func (p *Proxy) dynamicUnwrapIdentity(query string) bool {
	query = dynamicQuery(query)
	if !p.queryPlanLowering || !dynamicIdentityScreen(query) {
		return false
	}
	if len(query) > validationCacheMaxQueryBytes {
		// A long query is not remembered: the memory of the verdicts stays bounded.
		return p.lowersDynamicIdentity(query)
	}
	key := dynamicIdentityVerdictKey(p, query)
	if v, ok := dynamicIdentityVerdicts.get(key); ok {
		return v
	}
	v := p.lowersDynamicIdentity(query)
	dynamicIdentityVerdicts.put(key, v)
	return v
}

// lowersDynamicIdentity is dynamicUnwrapIdentity's verdict: the plan has the identity and the lowering writes it.
func (p *Proxy) lowersDynamicIdentity(query string) bool {
	plan := resolvedPlan(query)
	if plan == nil || !lower.DynamicUnwrapIdentity(plan) {
		return false
	}
	p.configMu.RLock()
	streamFields := p.streamFieldsMap
	p.configMu.RUnlock()
	prof := loweringProfile(translator.WithStreamLabels(p.labelTranslator.ToVL, nil), streamFields, p.logsqlCapabilities(), false)
	_, err := lower.PlanQuery(logqlpkg.CanonicalizeLogRanges(query), plan, prof)
	return err == nil
}

// dynamicIdentityVerdictKey names a proxy's verdict on a query.
func dynamicIdentityVerdictKey(p *Proxy, query string) string {
	return fmt.Sprintf("%p\x00%s", p, query)
}

// dynamicIdentityVerdicts remembers dynamicUnwrapIdentity's verdicts, bounded: a full map starts over.
var dynamicIdentityVerdicts = &boundedVerdicts{entries: map[string]bool{}, max: 4096}

// boundedVerdicts is a map of verdicts by key with at most max entries.
type boundedVerdicts struct {
	mu      sync.Mutex
	entries map[string]bool
	max     int
}

func (c *boundedVerdicts) get(key string) (bool, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	v, ok := c.entries[key]
	return v, ok
}

func (c *boundedVerdicts) put(key string, v bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.entries) >= c.max {
		clear(c.entries)
	}
	c.entries[key] = v
}

// streamIdentityKey marks a request whose dynamic identity the handlers do not serve (dynamicIdentityWindows): the
// string translator translates it and every handler reads the series as before.
type streamIdentityKey struct{}

// streamIdentityOnly reports a request marked with streamIdentityKey.
func streamIdentityOnly(ctx context.Context) bool {
	marked, _ := ctx.Value(streamIdentityKey{}).(bool)
	return marked
}

// dynamicSeriesHashLabel is the field the lowering writes the series hash of a parser's identity into
// (lower.DynamicUnwrapIdentity): the window evaluator's buckets group by it, and its series carry it until
// foldDynamicIdentity folds them.
const dynamicSeriesHashLabel = "__lvp_sid"

// identityAheadKey marks a range request of a root dynamic identity whose range is longer than the step
// (dynamicIdentityWindows): it is translated with the series hash before the unwrap gate (lower.Profile
// IdentityAhead), so the window evaluator reads the function per series of each anchored bucket.
type identityAheadKey struct{}

// identityAhead reports a request marked with identityAheadKey.
func identityAhead(ctx context.Context) bool {
	marked, _ := ctx.Value(identityAheadKey{}).(bool)
	return marked
}

// dynamicIdentityFor is dynamicUnwrapIdentity for a request: false for one marked streamIdentityKey.
func (p *Proxy) dynamicIdentityFor(ctx context.Context, query string) bool {
	return !streamIdentityOnly(ctx) && p.dynamicUnwrapIdentity(query)
}

// dynamicIdentityWindows marks a range query of the dynamic identity of a parser whose range is longer than the step,
// before it is translated. At its root it is marked identityAheadKey: it is translated with the series hash before
// the unwrap gate, VictoriaLogs takes the function per series and per anchored bucket, the window evaluator merges
// each series' buckets into Loki's windows (t-range, t] and the outer aggregation folds the series. Under a ranking
// or a label_replace it is marked streamIdentityKey: it is translated by the string translator and answered with the
// stream's series, as before. An operand of a binary operation is evaluated as a request of its own
// (proxyBinaryLogQL), which decides for itself, so each operand reads the series it reads alone whatever the range of
// the other one. out is the request to go on with.
func (p *Proxy) dynamicIdentityWindows(r *http.Request, query string) (out *http.Request) {
	step, ok := parsePositiveStepDuration(r.FormValue("step"))
	if !ok || streamIdentityOnly(r.Context()) || identityAhead(r.Context()) {
		return r
	}
	window, windowed := dynamicWindowRange(query)
	if windowed && window <= step {
		// A range up to the step is served as it is whatever the verdict: the lowering is not asked twice.
		return r
	}
	if p.dynamicUnwrapIdentity(query) {
		if !windowed {
			return r
		}
		if extremeOfExtremes(query) {
			// max by (L) of max_over_time and min by (L) of min_over_time are the extreme of the group's samples in the
			// window, whatever the series: VictoriaLogs takes it per bucket and group (unwrapGroupPushdown), with no
			// hash to write.
			return r.WithContext(context.WithValue(r.Context(), streamIdentityKey{}, true))
		}
		return r.WithContext(context.WithValue(r.Context(), identityAheadKey{}, true))
	}
	// A dynamic identity under a ranking or a label_replace is marked here; a binary operand decides in its own request.
	if !p.dynamicIdentityAtDepth(query, step) {
		return r
	}
	return r.WithContext(context.WithValue(r.Context(), streamIdentityKey{}, true))
}

// binaryOperandSide is how a range request translates one operand of its binary operation: the context, with the
// mark the operand's own request gets (dynamicIdentityWindows), and the operand's printed text when it was printed to
// decide that mark (empty otherwise).
type binaryOperandSide struct {
	ctx  context.Context
	text string
}

// binaryOperandSides are the sides of a range request's binary operation, so the translation each operand's own
// request reads is the one this request has cached. A number is not marked, and a request whose query holds no
// dynamic identity's words keeps its context for both after one text screen.
func (p *Proxy) binaryOperandSides(r *http.Request, binOp *logqlpkg.BinOpExpr) (left, right binaryOperandSide) {
	ctx := r.Context()
	// The lower case reading (other false) the handlers read the identity with; the fence reads every spelling.
	if !dynamicIdentityFenceScreen(dynamicQuery(r.FormValue("query")), false) {
		return binaryOperandSide{ctx: ctx}, binaryOperandSide{ctx: ctx}
	}
	side := func(operand logqlpkg.Expr) binaryOperandSide {
		if _, ok := operand.(*logqlpkg.LiteralExpr); ok {
			return binaryOperandSide{ctx: ctx}
		}
		text := operand.String()
		return binaryOperandSide{ctx: p.dynamicIdentityWindows(r, text).Context(), text: text}
	}
	return side(binOp.Left), side(binOp.Right)
}

// dynamicWindowRange is the range of a query whose function the window evaluator merges from step buckets (avg, max,
// min, stddev and stdvar over time) under a dynamic identity's shape; ok is false for any other query.
func dynamicWindowRange(query string) (time.Duration, bool) {
	query = dynamicQuery(query)
	if !dynamicIdentityScreen(query) {
		return 0, false
	}
	expr, ok := sharedLogQLExpr(query)
	if !ok {
		return 0, false
	}
	va, ok := expr.(*logqlpkg.VectorAggregation)
	if !ok {
		return 0, false
	}
	ra, ok := va.Inner.(*logqlpkg.RangeAggregation)
	if !ok {
		return 0, false
	}
	switch ra.Op {
	case logqlpkg.RangeAvgOverTime, logqlpkg.RangeMaxOverTime, logqlpkg.RangeMinOverTime, logqlpkg.RangeStddevOverTime, logqlpkg.RangeStdvarOverTime:
		return ra.RangeDuration, ra.RangeDuration > 0
	}
	return 0, false
}

// extremeOfExtremes reports a root dynamic identity whose outer aggregation is max over max_over_time or min over
// min_over_time (dynamicWindowRange's shape).
func extremeOfExtremes(query string) bool {
	expr, ok := sharedLogQLExpr(dynamicQuery(query))
	if !ok {
		return false
	}
	va, ok := expr.(*logqlpkg.VectorAggregation)
	if !ok {
		return false
	}
	ra, ok := va.Inner.(*logqlpkg.RangeAggregation)
	return ok && (va.Op == logqlpkg.VectorMax && ra.Op == logqlpkg.RangeMaxOverTime || va.Op == logqlpkg.VectorMin && ra.Op == logqlpkg.RangeMinOverTime)
}

// foldDynamicIdentity serves a range request marked identityAheadKey and folds its series by the query's outer
// aggregation: the evaluator answers a series per hash of a parser's identity (the stream and the parsed labels less
// the unwrapped one), each with its own windows, and the sum, max, min or avg by the clause's labels or none then
// takes Loki's value of each group at each step, which drops the hash. Loki counts max_query_series on the folded
// answer, so the reads of the series are bounded by the raw-row budget of a request that folds its series
// (-manual-range-metric-row-limit, recordFoldBudget), and the folded answer by the tenant's series limit.
func (p *Proxy) foldDynamicIdentity(w http.ResponseWriter, r *http.Request, serve func(http.ResponseWriter, *http.Request) bool) bool {
	expr, ok := sharedLogQLExpr(dynamicQuery(r.FormValue("query")))
	va, isAgg := expr.(*logqlpkg.VectorAggregation)
	if !ok || !isAgg {
		return serve(w, r)
	}
	p.recordFoldBudget(r.Context())
	return foldVectorChain(p, w, r, []*logqlpkg.VectorAggregation{va}, serve)
}

// dynamicIdentityAtDepth reports a query that holds, below its root and outside a binary operation, a range function
// of a dynamic identity's shape over a range longer than step (lower.WritesDynamicUnwrapIdentity).
func (p *Proxy) dynamicIdentityAtDepth(query string, step time.Duration) bool {
	query = dynamicQuery(query)
	if !p.queryPlanLowering || !strings.Contains(query, "unwrap") {
		return false
	}
	plan := resolvedPlan(query)
	if plan == nil || !lower.WritesDynamicUnwrapIdentity(plan) {
		return false
	}
	expr, ok := sharedLogQLExpr(query)
	if !ok {
		return false
	}
	return rangeLongerThan(expr, step)
}

// rangeLongerThan reports a range aggregation over a range longer than step in expr, outside a binary operation: the
// binary route evaluates each operand as a request of its own, which dynamicIdentityWindows marks on its own range,
// so `max_over_time(... [1m]) - count_over_time(...[5m])` at a step of 1m keeps the hash on its first operand.
func rangeLongerThan(expr logqlpkg.Expr, step time.Duration) bool {
	switch e := expr.(type) {
	case *logqlpkg.RangeAggregation:
		return e.RangeDuration > step
	case *logqlpkg.VectorAggregation:
		return rangeLongerThan(e.Inner, step)
	case *logqlpkg.OpaqueMetricExpr:
		return e.Inner != nil && rangeLongerThan(e.Inner, step)
	}
	return false
}
