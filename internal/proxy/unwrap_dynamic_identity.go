package proxy

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	logqlpkg "github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/lower"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
)

// The handlers' side of the dynamic identity of a parser (lower.DynamicUnwrapIdentity): sum, max, min or avg over a
// range function taken per series of an unwrapped label after a json or logfmt parser with no keep, whose series are
// the stream and every parsed label less the unwrapped one. The lowered query writes them as the stream and a hash
// of each line's parsed labels (__lvp_sid), and VictoriaLogs answers the function per series and the aggregation over
// them for an instant query and a range up to the step.
//
// quantile_over_time and a range longer than the step need each series' samples, which VictoriaLogs'
// stats cannot merge across buckets. When every line is a series of its own (a request id) that is one series per
// line, so the proxy first proves it with one stats read over the fetch span: the rows and the distinct series
// hashes. When they are equal each window holds at most one sample of a series, and the function has a closed form:
// the value itself (quantile, avg, max, min over time) or 0 (stddev, stdvar over time), so the outer
// aggregation is the range function of the same name over the rows grouped by its clause (sum by (C) over
// sum_over_time for a sum by C, max_over_time ... by (C) for a max by C, and so on, times 0 for the zero forms), which
// the stats routes answer. Otherwise the series are
// read one by one, bounded by the series limit (-max-stats-query-series): a hash collision only lowers the distinct
// count, so it can fail the proof, never fake it. The per-series evaluators are the quantile value counts and the
// window evaluator over step buckets of each series, both grouped by the series hash.

// dynamicSeriesField is the field of the series hash, as lower writes it.
const dynamicSeriesField = "__lvp_sid"

// dynamicSeriesGroupBy are the fields the quantile counts of a dynamic identity group a series by: the hash of the
// line's parsed labels less the unwrapped one (lower.DynamicUnwrapIdentity), and the stream, whose labels the outer
// aggregation reads.
var dynamicSeriesGroupBy = []string{"_stream", dynamicSeriesField}

// dynamicUnwrapIdentity reports a query whose series are the dynamic identity of a parser
// (lower.DynamicUnwrapIdentity) and which the typed lowering serves (dynamicIdentityConstruct): its stats group by the
// stream and the hash of each line's parsed labels, and the handlers read those groups as the series. With
// -query-plan-lowering=false, or for a query the lowering turns away, the string translator serves it and no handler
// reads the identity, so the handlers and the translation decide alike. The verdict is remembered per proxy and query
// (dynamicIdentityVerdicts): one request asks several times.
func (p *Proxy) dynamicUnwrapIdentity(query string) bool {
	query = strings.TrimSpace(query)
	if !p.queryPlanLowering || !dynamicIdentityScreen(query) {
		return false
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

// dynamicIdentityProves reports the functions the stats handlers prove one-sample series for before reading each
// series' samples: quantile_over_time (the others over a range longer than the step are decided before the
// translation, dynamicIdentityWindows).
func dynamicIdentityProves(manualFunc string, origSpec originalRangeMetricSpec, step time.Duration, instant bool) bool {
	return manualFunc == "quantile"
}

// serveDynamicIdentity answers a query of the dynamic identity of a parser whose function reads each series' samples
// (dynamicIdentityProves) in closed form when every series holds one sample in the fetch span, and fails it with the
// series limit when the series outnumber it; false leaves the request to the per-series evaluators. base is the
// lowered log query up to its first stats pipe (the series hash included), from and to the fetch span.
func (p *Proxy) serveDynamicIdentity(w http.ResponseWriter, r *http.Request, query, base string, from, to time.Time, instant bool) bool {
	if !p.dynamicIdentityFor(r.Context(), query) {
		return false
	}
	closed, ok := dynamicClosedForm(query)
	if !ok {
		return false
	}
	rows, series, ok := p.dynamicIdentitySeries(r.Context(), base, from, to)
	if !ok {
		// No verdict: the per-series evaluators read VictoriaLogs themselves and report its failure.
		return false
	}
	if rows == series {
		markQueryStepsRoute(r.Context(), "parser-identity-one-sample")
		child := cloneMetricQueryRequest(r, closed)
		if instant {
			p.handleQuery(w, child)
		} else {
			p.handleQueryRange(w, child)
		}
		return true
	}
	if limit := p.resolvedMaxStatsQuerySeries(r.Context()); limit > 0 && series > uint64(limit) {
		p.writeErr(w, r.Context(), newSeriesLimitError(r.Context(), limit))
		return true
	}
	return false
}

// dynamicIdentitySeries counts the rows of base over [from, to] and the distinct series hashes among them.
func (p *Proxy) dynamicIdentitySeries(ctx context.Context, base string, from, to time.Time) (rows, series uint64, ok bool) {
	stats := logsql.PipeStats{Funcs: []logsql.StatsFuncAlias{
		{Func: logsql.Count{}, Alias: "n"},
		{Func: logsql.CountUniqHash{Field: dynamicSeriesField}, Alias: "u"},
	}}
	row, ok := p.rateCounterStatsRow(ctx, base+" "+stats.String(), from, to)
	if !ok {
		return 0, 0, false
	}
	n, errN := strconv.ParseUint(row["n"], 10, 64)
	u, errU := strconv.ParseUint(row["u"], 10, 64)
	return n, u, errN == nil && errU == nil
}

// dynamicClosedForm is the query that answers a dynamic identity whose series each hold one sample: the range function
// of the outer aggregation's name over the same log query and range, grouped by the outer clause (none is one
// series), times 0 for the functions whose value over one sample is 0. ok is false for a query with an offset (the
// proof reads the request's own span) or another shape.
func dynamicClosedForm(query string) (string, bool) {
	expr, ok := sharedLogQLExpr(strings.TrimSpace(query))
	if !ok {
		return "", false
	}
	va, ok := expr.(*logqlpkg.VectorAggregation)
	if !ok || va.Grouping != nil && va.Grouping.Without {
		return "", false
	}
	ra, ok := va.Inner.(*logqlpkg.RangeAggregation)
	if !ok || ra.Offset != "" || ra.Grouping != nil {
		return "", false
	}
	op, ok := closedFormOps[va.Op]
	if !ok {
		return "", false
	}
	zero := false
	switch ra.Op {
	case logqlpkg.RangeStddevOverTime, logqlpkg.RangeStdvarOverTime:
		zero, op = true, logqlpkg.RangeSumOverTime
	case logqlpkg.RangeAvgOverTime, logqlpkg.RangeMaxOverTime, logqlpkg.RangeMinOverTime, logqlpkg.RangeQuantileOverTime:
	default:
		return "", false
	}
	labels := []string{}
	if va.Grouping != nil {
		labels = va.Grouping.Labels
	}
	var cf logqlpkg.Expr = &logqlpkg.RangeAggregation{Op: op, Inner: ra.Inner, Range: ra.Range, RangeDuration: ra.RangeDuration, Grouping: &logqlpkg.Grouping{Labels: labels}}
	if op == logqlpkg.RangeSumOverTime {
		// Loki takes no grouping on sum_over_time: the sum by the clause over it adds the rows of each group.
		cf = &logqlpkg.VectorAggregation{Op: logqlpkg.VectorSum, Grouping: va.Grouping,
			Inner: &logqlpkg.RangeAggregation{Op: op, Inner: ra.Inner, Range: ra.Range, RangeDuration: ra.RangeDuration}}
	}
	if zero {
		return (&logqlpkg.BinOpExpr{Left: cf, Op: "*", Right: &logqlpkg.LiteralExpr{Value: 0}}).String(), true
	}
	return cf.String(), true
}

// closedFormOps are the range functions that aggregate one-sample series as the vector aggregations do.
var closedFormOps = map[logqlpkg.VectorOp]logqlpkg.RangeOp{
	logqlpkg.VectorSum: logqlpkg.RangeSumOverTime,
	logqlpkg.VectorMax: logqlpkg.RangeMaxOverTime,
	logqlpkg.VectorMin: logqlpkg.RangeMinOverTime,
	logqlpkg.VectorAvg: logqlpkg.RangeAvgOverTime,
}

// dynamicIdentityInstant is serveDynamicIdentity for an instant query: the fetch span is the window before the
// evaluation time.
func (p *Proxy) dynamicIdentityInstant(w http.ResponseWriter, r *http.Request, query string, spec statsCompatSpec, origSpec originalRangeMetricSpec, manualFunc string) bool {
	if !dynamicIdentityProves(manualFunc, origSpec, 0, true) {
		return false
	}
	evalTS, err := parseTimestamp(r.FormValue("time"))
	if err != nil {
		evalTS = time.Now()
	}
	return p.serveDynamicIdentity(w, r, query, spec.BaseQuery, evalTS.Add(-origSpec.Window), evalTS, true)
}

// dynamicIdentityRange is serveDynamicIdentity for a range query: the fetch span is the window before the start to the
// end.
func (p *Proxy) dynamicIdentityRange(w http.ResponseWriter, r *http.Request, query string, spec statsCompatSpec, origSpec originalRangeMetricSpec, manualFunc string, step time.Duration) bool {
	if !dynamicIdentityProves(manualFunc, origSpec, step, false) {
		return false
	}
	startTS, errS := parseTimestamp(r.FormValue("start"))
	endTS, errE := parseTimestamp(r.FormValue("end"))
	if errS != nil || errE != nil {
		return false
	}
	return p.serveDynamicIdentity(w, r, query, spec.BaseQuery, startTS.Add(-origSpec.Window), endTS, false)
}

// streamIdentityKey marks a request whose dynamic identity the handlers do not serve (dynamicIdentityWindows): the
// string translator translates it and every handler reads the series as before.
type streamIdentityKey struct{}

// streamIdentityOnly reports a request marked with streamIdentityKey.
func streamIdentityOnly(ctx context.Context) bool {
	marked, _ := ctx.Value(streamIdentityKey{}).(bool)
	return marked
}

// dynamicIdentityFor is dynamicUnwrapIdentity for a request: false for one marked streamIdentityKey.
func (p *Proxy) dynamicIdentityFor(ctx context.Context, query string) bool {
	return !streamIdentityOnly(ctx) && p.dynamicUnwrapIdentity(query)
}

// dynamicIdentityWindows decides a range query of the dynamic identity of a parser over avg, max, min, stddev or
// stdvar over time whose range is longer than the step, before it is translated: the window evaluator merges step
// buckets of the stream's series, not of a series hash. When every series holds one sample in the fetch span (the
// proof of serveDynamicIdentity) the query is answered in closed form; otherwise it is marked streamIdentityKey and
// translated and answered as before. served reports an answered request; r is the request to go on with.
func (p *Proxy) dynamicIdentityWindows(w http.ResponseWriter, r *http.Request, query string) (served bool, out *http.Request) {
	window, ok := dynamicWindowRange(query)
	if !ok || !p.dynamicIdentityFor(r.Context(), query) {
		return false, r
	}
	step, ok := parsePositiveStepDuration(r.FormValue("step"))
	if !ok || window <= step {
		return false, r
	}
	marked := r.WithContext(context.WithValue(r.Context(), streamIdentityKey{}, true))
	closed, ok := dynamicClosedForm(query)
	startTS, errS := parseTimestamp(r.FormValue("start"))
	endTS, errE := parseTimestamp(r.FormValue("end"))
	if !ok || errS != nil || errE != nil {
		return false, marked
	}
	text, err := p.translateQueryWithContext(r.Context(), query)
	if err != nil {
		return false, marked
	}
	spec, ok := statsSpecOf(statsQueryOf(text))
	if !ok {
		return false, marked
	}
	rows, series, ok := p.dynamicIdentitySeries(r.Context(), spec.BaseQuery, startTS.Add(-window), endTS)
	if !ok || rows != series {
		return false, marked
	}
	markQueryStepsRoute(r.Context(), "parser-identity-one-sample")
	p.handleQueryRange(w, cloneMetricQueryRequest(r, closed))
	return true, r
}

// dynamicWindowRange is the range of a query whose function the window evaluator merges from step buckets (avg, max,
// min, stddev and stdvar over time) under a dynamic identity's shape; ok is false for any other query.
func dynamicWindowRange(query string) (time.Duration, bool) {
	query = strings.TrimSpace(query)
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
