package proxy

import (
	"bytes"
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/url"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
	logqlpkg "github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
)

// Settled steps: the answers of the window evaluator (proxyManualRangeMetricRange)
// are kept per evaluation time, and a query_range request whose window moved
// (a dashboard refresh, a re-run moved back) evaluates only the steps it does
// not hold. Loki's results cache does the same with its query_range answers:
// extents of the response per split, the missing part evaluated and merged by
// series labels, nothing newer than max_cache_freshness_per_query
// (pkg/storage/chunk/cache/resultscache/cache.go Do, handleHit,
// filterRecentExtents; pkg/querier/queryrange/queryrangebase/query_range.go:310-350
// matrixMerge).
//
// It is exact because a step's sample is a function of the lines in its
// window (t-range, t] alone (pkg/logql/range_vector.go:102-189), and every
// aggregation the evaluator and handleQueryRange apply after the window works
// per step (topk, the folds, label_replace). A step is kept only once its
// window is older than -query-range-freshness, in the response cache tiers
// (memory, disk, peers) under the tenant's scope, for
// -query-range-extent-cache-ttl counted from its oldest evaluation, the same
// rules as the VictoriaLogs buckets of the stats extent cache. A merged answer
// is written exactly as the evaluator writes the whole window: the envelope
// and the series order of its own answers (checked on every answer that is
// kept), Loki's series limit and the response size limit on the merged
// answer. Anything else (an error, a header, an answer of another route, an
// order the merge cannot reproduce) evaluates the whole window as before.
// A kept step expires with the oldest VictoriaLogs bucket it was evaluated
// from, so a late line is not hidden longer than the buckets' own TTL.

// queryStepsKeyPrefix versions the stored steps.
const queryStepsKeyPrefix = "query-steps:v1:"

// queryStepsRunKey carries the queryStepsRun of a request in its context.
type queryStepsRunKey struct{}

// queryStepsRun follows one evaluation of the window: armed while
// handleQueryRange dispatches it to the stats route, perStep once the window
// evaluator answered it. query, start and end name the request it belongs
// to, so a request it evaluates in turn (an operand) is not taken for it.
type queryStepsRun struct {
	query, start, end string
	inner             bool // a missing range evaluated for the steps cache
	mu                sync.Mutex
	armed, perStep    bool
	// route names the evaluator's route (a window of another length may take
	// another, whose float sums can differ in their last bits): steps of
	// different routes are never merged. oldestRead is the oldest read of the
	// VictoriaLogs buckets the answer was evaluated from, unix ns.
	route      string
	oldestRead int64
}

func (run *queryStepsRun) owns(r *http.Request) bool {
	return run != nil && run.query == r.FormValue("query") && run.start == r.FormValue("start") && run.end == r.FormValue("end")
}

func (run *queryStepsRun) arm() {
	if run != nil {
		run.mu.Lock()
		run.armed = true
		run.mu.Unlock()
	}
}

// markQueryStepsEvaluated records that the window evaluator answers the
// request being dispatched.
func markQueryStepsEvaluated(ctx context.Context) {
	if run, _ := ctx.Value(queryStepsRunKey{}).(*queryStepsRun); run != nil {
		run.mu.Lock()
		if run.armed {
			run.perStep = true
		}
		run.mu.Unlock()
	}
}

// markQueryStepsRoute records the route the window evaluator answers by.
func markQueryStepsRoute(ctx context.Context, route string) {
	if run, _ := ctx.Value(queryStepsRunKey{}).(*queryStepsRun); run != nil {
		run.mu.Lock()
		if run.armed {
			if run.route != "" && run.route != route {
				route = "mixed"
			}
			run.route = route
		}
		run.mu.Unlock()
	}
}

// noteQueryStepsRead records that the request's answer reads VictoriaLogs
// buckets first read at readAt.
func noteQueryStepsRead(ctx context.Context, readAt int64) {
	if run, _ := ctx.Value(queryStepsRunKey{}).(*queryStepsRun); run != nil {
		run.mu.Lock()
		if run.oldestRead == 0 || readAt < run.oldestRead {
			run.oldestRead = readAt
		}
		run.mu.Unlock()
	}
}

func (run *queryStepsRun) evaluatedPerStep() bool {
	run.mu.Lock()
	defer run.mu.Unlock()
	return run.perStep && run.route != "" && run.route != "mixed"
}

// stamp names f's route and headers next to its envelope, and dates it by the
// oldest bucket it was evaluated from.
func (run *queryStepsRun) stamp(f *statsExtentFragment, h http.Header) {
	run.mu.Lock()
	defer run.mu.Unlock()
	nosniff := ""
	if h.Get("X-Content-Type-Options") == "nosniff" {
		nosniff = "nosniff"
	}
	f.names = append(f.names, run.route, nosniff)
	if run.oldestRead > 0 && run.oldestRead < f.readAt {
		f.readAt = run.oldestRead
	}
}

// queryStepsSilentKey marks a request whose evaluation is recorded by the
// settled steps cache, not by the handler.
type queryStepsSilentKey struct{}

func withQueryStepsSilent(r *http.Request, run *queryStepsRun) *http.Request {
	if run == nil || !run.inner {
		return r
	}
	return r.WithContext(context.WithValue(r.Context(), queryStepsSilentKey{}, true))
}

func queryStepsSilent(ctx context.Context) bool {
	silent, _ := ctx.Value(queryStepsSilentKey{}).(bool)
	return silent
}

// queryStepsRunFor returns the run r is the evaluation of, nil for a request
// the steps cache did not start.
func queryStepsRunFor(r *http.Request) *queryStepsRun {
	run, _ := r.Context().Value(queryStepsRunKey{}).(*queryStepsRun)
	if run.owns(r) {
		return run
	}
	return nil
}

// queryStepsGrid is a request's evaluation grid: t_k = phase + k*step for k in
// [first, last].
type queryStepsGrid struct {
	step, phase int64
	first, last int64
}

func (g queryStepsGrid) at(k int64) int64 { return g.phase + k*g.step }

// queryStepsFor returns the grid and the key of a request whose settled steps
// can be kept, or false.
func (p *Proxy) queryStepsFor(r *http.Request, logqlQuery string) (queryStepsGrid, string, bool) {
	skip := func(reason string) (queryStepsGrid, string, bool) {
		p.observeInternalOperation(r.Context(), "query_steps_eligibility", reason, 0)
		return queryStepsGrid{}, "", false
	}
	if !p.queryRangeExtentCache || p.cache == nil || p.queryRangeExtentCacheTTL <= 0 || p.cache.MaxEntrySizeBytes() <= 0 {
		return skip("disabled")
	}
	// Native field histograms rank/cap over the whole request. Only the exact
	// ordered JSON evaluator can prove a complete per-step answer below.
	ordered, orderedOK := queryStepsOrderedJSON(logqlQuery)
	if isGrafanaDrilldownRequest(r) && (!orderedOK || ordered.isNativeDrilldownHistogram(r)) {
		return skip("drilldown_profile")
	}
	// A query holding both parsers is translated after a probe of its window's
	// lines (preferWorkingParser): its steps depend on the window.
	if hasParserStage(logqlQuery, "json") && hasParserStage(logqlQuery, "logfmt") {
		return skip("mixed_parsers")
	}
	step, ok := parsePositiveStepDuration(r.FormValue("step"))
	if !ok || !queryStepsShape(logqlQuery, step) {
		return skip("shape")
	}
	startNs, okS := parseLokiTimeToUnixNano(r.FormValue("start"))
	endNs, okE := parseLokiTimeToUnixNano(r.FormValue("end"))
	if !okS || !okE || endNs < startNs {
		return skip("bounds")
	}
	g := queryStepsGrid{step: step.Nanoseconds()}
	g.phase = ((startNs % g.step) + g.step) % g.step
	// Points carry the evaluation time in seconds, maybe cut to whole seconds:
	// a step of four seconds or more, or a grid of whole seconds, names its
	// step unambiguously.
	if g.step < 4*int64(time.Second) && (g.step%int64(time.Second) != 0 || g.phase%int64(time.Second) != 0) {
		return skip("phase")
	}
	g.first = (startNs - g.phase) / g.step
	g.last = g.first + (endNs-startNs)/g.step
	sum := sha256.Sum256([]byte(p.queryStepsBaseKey(r, logqlQuery)))
	prefix := queryStepsKeyPrefix
	if orderedOK {
		// Older binaries do not enforce raw JSON's merged byte/sample bounds.
		// Keep these fragments out of their otherwise identical step keys.
		prefix += "ordered-json:v1:"
	}
	return g, prefix + hex.EncodeToString(sum[:]) + ":" + strconv.FormatInt(g.phase, 10), true
}

// queryStepsShape reports whether the window evaluator can answer query at
// step: a range aggregation, under vector aggregations, whose range is not the
// step (a tumbling window is VictoriaLogs' own bucket) and, for sum, avg, max
// and min over time with unwrap, longer than it; or a quantile or
// rate_counter, which the proxy evaluates at every range. Other queries skip
// the steps cache before any lookup.
func queryStepsShape(query string, step time.Duration) bool {
	if _, ok := queryStepsOrderedJSON(query); ok {
		return true // raw JSON evaluation also handles tumbling windows locally
	}
	if _, _, nested := orderedJSONVectorPlan(query); nested {
		// Outer vector folds depend on the complete inner answer. Keep their
		// results out of generic fragments produced before ordered evaluation.
		return false
	}
	expr, err := logqlpkg.Parse(query)
	if err != nil {
		return false
	}
	for {
		agg, ok := expr.(*logqlpkg.VectorAggregation)
		if !ok {
			break
		}
		expr = agg.Inner
	}
	ra, ok := expr.(*logqlpkg.RangeAggregation)
	if !ok {
		return false
	}
	if ra.Op == logqlpkg.RangeQuantileOverTime || ra.Op == logqlpkg.RangeRateCounter {
		return true
	}
	window := parseLokiDuration(ra.Range)
	if window > 0 && window < step {
		// sum, avg, max and min over time with unwrap and a shorter range are
		// read from VictoriaLogs' step buckets (the phase filter and the
		// relabel, not the window evaluator).
		switch ra.Op {
		case logqlpkg.RangeSumOverTime, logqlpkg.RangeAvgOverTime, logqlpkg.RangeMaxOverTime, logqlpkg.RangeMinOverTime:
			if lq, ok := ra.Inner.(*logqlpkg.LogQuery); ok && hasUnwrapStage(lq) {
				return false
			}
		}
	}
	return window > 0 && window != step
}

// Keep the first extension to direct count/rate plans. Transforms, native
// reroutes and other evaluators retain their existing admission rules.
func queryStepsOrderedJSON(query string) (*orderedJSONMetricPlan, bool) {
	plan, ok := compileOrderedJSONMetric(query)
	return plan, ok && plan.parser == "json" && plan.withoutJSON == "" &&
		(plan.function == "count_over_time" || plan.function == "rate")
}

// queryStepsBaseKey is the response cache key of a request with its window
// left out: the query, the step and everything else that shapes the answer.
func (p *Proxy) queryStepsBaseKey(r *http.Request, logqlQuery string) string {
	clone := r.Clone(r.Context())
	_ = clone.ParseForm()
	form := url.Values{}
	for k, v := range clone.Form {
		form[k] = v
	}
	form.Del("start")
	form.Del("end")
	clone.Form = form
	return p.queryRangeCacheKey(clone, logqlQuery)
}

// queryStepsExtent is the stats extent request the steps of a grid are stored
// under: one entry per split, as the stats extent cache splits its buckets.
func queryStepsExtent(g queryStepsGrid, key string) *statsExtentRequest {
	return &statsExtentRequest{step: g.step, offset: -g.phase, first: g.first, last: g.last,
		chunk: max(int64(statsExtentSplit)/g.step, statsExtentMinChunkBuckets), key: key}
}

// serveQueryRangeSteps answers a request from settled steps and the
// evaluation of the steps missing, and reports true; or it reports false and
// returns the writer and request the evaluation of the whole window goes
// through, with after to call once it is written: the window's settled steps
// are then kept.
func (p *Proxy) serveQueryRangeSteps(w http.ResponseWriter, r *http.Request, logqlQuery, responseKey string, began time.Time) (served bool, outW http.ResponseWriter, outR *http.Request, after func()) {
	grid, key, ok := p.queryStepsFor(r, logqlQuery)
	if !ok {
		return false, w, r, nil
	}
	req := queryStepsExtent(grid, key)
	horizon := cache.Now().Add(-p.queryRangeFreshness).UnixNano()
	lastStored := statsExtentFloorDiv(horizon-grid.phase, grid.step)
	var chunks map[int64]*statsExtentChunk
	if lastStored >= grid.first {
		chunks = p.loadStatsExtentChunks(req, lastStored)
	}
	var covered [][2]int64
	for _, c := range chunks {
		if from, to := max(c.from, grid.first), statsExtentMin(c.to, grid.last); from <= to {
			covered = append(covered, [2]int64{from, to})
		}
	}
	gaps := statsExtentGaps(grid.first, grid.last, covered)
	if len(gaps) > statsExtentMaxGaps {
		gaps = [][2]int64{{gaps[0][0], gaps[len(gaps)-1][1]}}
	}
	whole := func(outcome string) (bool, http.ResponseWriter, *http.Request, func()) {
		w, r, after := p.queryStepsWhole(w, r, outcome, began, grid, req, lastStored, chunks)
		return false, w, r, after
	}
	if len(gaps) == 1 && gaps[0] == [2]int64{grid.first, grid.last} {
		p.logQueryStepsDecision(r.Context(), "miss", grid, gaps, 0)
		return whole("miss")
	}
	fragments, failed, complete := p.evaluateQueryStepsGaps(r, grid, gaps, w.Header())
	if failed != nil {
		// The missing steps fail: their answer is the request's.
		for k, v := range failed.header {
			w.Header()[k] = v
		}
		w.WriteHeader(failed.code)
		_, _ = w.Write(failed.body)
		p.recordQueryStepsRequest(r, logqlQuery, failed.code, began, "error")
		return true, w, r, nil
	}
	if !complete {
		p.logQueryStepsDecision(r.Context(), "gap_not_mergeable", grid, gaps, 0)
		return whole("fallback")
	}
	pieces := make([]*statsExtentFragment, 0, len(chunks)+len(fragments))
	for _, c := range chunks {
		for _, rg := range statsExtentGaps(max(c.from, grid.first), statsExtentMin(c.to, grid.last), slices.Clone(gaps)) {
			pieces = append(pieces, c.slice(rg[0], rg[1]))
		}
	}
	pieces = append(pieces, fragments...)
	body, ok := p.renderQuerySteps(r.Context(), key, pieces, fragments)
	if !ok {
		p.logQueryStepsDecision(r.Context(), "merge_rejected", grid, gaps, 0)
		return whole("fallback")
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	_, _ = w.Write(body)
	if _, ranked := parseInstantMetricPostAggQuery(logqlQuery); responseKey != "" && !ranked {
		// As the evaluation of the whole window would (handleQueryRange keeps
		// the answers of its main dispatch, not of the ranked one).
		p.setLocalReadCacheWithTTL(responseKey, append([]byte(nil), body...), 5*time.Minute)
	}
	p.storeOffRequestPath(func() { p.storeQuerySteps(req, key, lastStored, chunks, fragments) })
	outcome := "partial"
	if len(fragments) == 0 {
		outcome = "hit"
	}
	p.logQueryStepsDecision(r.Context(), outcome, grid, gaps, len(body))
	p.recordQueryStepsRequest(r, logqlQuery, http.StatusOK, began, outcome)
	return true, w, r, nil
}

// queryStepsWhole returns the writer and request the evaluation of the whole
// window goes through, and the function that keeps its settled steps once it
// is written.
func (p *Proxy) queryStepsWhole(w http.ResponseWriter, r *http.Request, outcome string, began time.Time, grid queryStepsGrid, req *statsExtentRequest, lastStored int64, chunks map[int64]*statsExtentChunk) (http.ResponseWriter, *http.Request, func()) {
	run := &queryStepsRun{query: r.FormValue("query"), start: r.FormValue("start"), end: r.FormValue("end")}
	tee := &queryStepsTee{ResponseWriter: w, limit: p.limits().BufferedBackendBodyBytes, before: headerNames(w.Header()), run: run}
	r = r.WithContext(context.WithValue(r.Context(), queryStepsRunKey{}, run))
	return tee, r, func() {
		code := tee.code
		if code == 0 {
			code = http.StatusOK
		}
		if r.Context().Err() != nil || code != http.StatusOK || tee.over || !run.evaluatedPerStep() || !tee.onlyJSONHeader() {
			reason := "evaluator"
			switch {
			case r.Context().Err() != nil:
				reason = "canceled"
			case code != http.StatusOK:
				reason = "error"
			case !run.evaluatedPerStep():
				reason = "evaluator"
			case tee.over:
				reason = "oversize"
			case !tee.onlyJSONHeader():
				reason = "headers"
			}
			p.observeInternalOperation(r.Context(), "query_steps_admission", reason, 0)
			p.observeInternalOperation(r.Context(), "query_steps_cache", outcome, time.Since(began))
			return
		}
		body, header := tee.buf.Bytes(), tee.sent.Clone()
		p.storeOffRequestPath(func() {
			stored := "unreadable"
			if f, ok := parseQueryStepsAnswer(body, grid, grid.first, grid.last); ok {
				run.stamp(f, header)
				p.storeQuerySteps(req, req.key, lastStored, chunks, []*statsExtentFragment{f})
				stored = "stored"
			}
			p.observeInternalOperation(context.Background(), "query_steps_cache", stored, 0)
		})
		p.observeInternalOperation(r.Context(), "query_steps_cache", outcome, time.Since(began))
	}
}

// Numeric work bounds and fixed outcomes are sufficient to correlate a debug
// request with backend reads. Never log query text, labels, auth or cache keys.
func (p *Proxy) logQueryStepsDecision(ctx context.Context, outcome string, grid queryStepsGrid, gaps [][2]int64, bytes int) {
	missing := int64(0)
	for _, gap := range gaps {
		missing += gap[1] - gap[0] + 1
	}
	p.log.DebugContext(ctx, "query_steps_cache", "outcome", outcome, "total_steps", grid.last-grid.first+1, "missing_steps", missing, "missing_ranges", len(gaps), "response_bytes", bytes)
}

// evaluateQueryStepsGaps evaluates the missing ranges (each is a request of
// its own, and together they take no more of the backend than the whole window
// would). A moved window misses two (its old edge and the fresh steps), so
// ranges shorter than -backend-heavy-query-min-range are evaluated at the same
// time; a heavy one goes one after another, so the request never holds more
// than the one heavy slot its whole window would. It returns their steps, or
// the first answer that failed, or complete false when one cannot be merged.
func (p *Proxy) evaluateQueryStepsGaps(r *http.Request, grid queryStepsGrid, gaps [][2]int64, base http.Header) (fragments []*statsExtentFragment, failed *bufferedResponseWriter, complete bool) {
	fragments = make([]*statsExtentFragment, 0, len(gaps))
	answers := make([]*bufferedResponseWriter, len(gaps))
	runs := make([]*queryStepsRun, len(gaps))
	if len(gaps) > 1 && p.queryStepsGapsLight(r, grid, gaps) {
		var wg sync.WaitGroup
		for i, gap := range gaps {
			wg.Add(1)
			go func() {
				defer wg.Done()
				answers[i], runs[i] = p.evaluateQuerySteps(r, grid, gap, base)
			}()
		}
		wg.Wait()
	}
	for i, gap := range gaps {
		answer, run := answers[i], runs[i]
		if answer == nil {
			answer, run = p.evaluateQuerySteps(r, grid, gap, base)
		}
		if answer.code != http.StatusOK {
			return nil, answer, false
		}
		if !run.evaluatedPerStep() || !onlyJSONHeader(answer.header, headerNames(base)) {
			return nil, nil, false
		}
		f, ok := parseQueryStepsAnswer(answer.body, grid, gap[0], gap[1])
		if !ok {
			return nil, nil, false
		}
		run.stamp(f, answer.header)
		fragments = append(fragments, f)
	}
	return fragments, nil, true
}

// queryStepsGapsLight reports whether every missing range reads less than
// -backend-heavy-query-min-range from VictoriaLogs (its steps plus the range
// of the query's window), so none takes a heavy-query slot.
func (p *Proxy) queryStepsGapsLight(r *http.Request, grid queryStepsGrid, gaps [][2]int64) bool {
	if p.heavyQueryLimiter == nil {
		return true
	}
	window, ok := queryStepsWindow(r.FormValue("query"))
	if !ok {
		return false
	}
	for _, gap := range gaps {
		if time.Duration(grid.at(gap[1])-grid.at(gap[0]))+window >= p.backendHeavyQueryMinRange {
			return false
		}
	}
	return true
}

// queryStepsWindow is the range of the range aggregation of a query the
// steps cache keeps (queryStepsShape).
func queryStepsWindow(query string) (time.Duration, bool) {
	expr, err := logqlpkg.Parse(query)
	if err != nil {
		return 0, false
	}
	for {
		agg, ok := expr.(*logqlpkg.VectorAggregation)
		if !ok {
			break
		}
		expr = agg.Inner
	}
	ra, ok := expr.(*logqlpkg.RangeAggregation)
	if !ok {
		return 0, false
	}
	window := parseLokiDuration(ra.Range)
	return window, window > 0
}

func (p *Proxy) recordQueryStepsRequest(r *http.Request, logqlQuery string, code int, began time.Time, outcome string) {
	elapsed := time.Since(began)
	p.observeInternalOperation(r.Context(), "query_steps_cache", outcome, elapsed)
	p.metrics.RecordRequest("query_range", code, elapsed)
	p.queryTracker.Record("query_range", logqlQuery, elapsed, code >= 400)
}

// evaluateQuerySteps evaluates the steps [gap[0], gap[1]] of r through the
// handler, with the run that followed the evaluation. The answer starts with
// the headers base holds (the real writer's, set by the middleware before the
// handler ran), so a step carries the same headers as a window evaluated whole.
func (p *Proxy) evaluateQuerySteps(r *http.Request, grid queryStepsGrid, gap [2]int64, base http.Header) (*bufferedResponseWriter, *queryStepsRun) {
	child := r.Clone(r.Context())
	_ = child.ParseForm()
	form := url.Values{}
	for k, v := range child.Form {
		form[k] = slices.Clone(v)
	}
	start, end := strconv.FormatInt(grid.at(gap[0]), 10), strconv.FormatInt(grid.at(gap[1]), 10)
	form.Set("start", start)
	form.Set("end", end)
	child.Form = form
	child.URL.RawQuery = form.Encode()
	run := &queryStepsRun{query: form.Get("query"), start: start, end: end, inner: true}
	child = child.WithContext(context.WithValue(child.Context(), queryStepsRunKey{}, run))
	out := &bufferedResponseWriter{header: base.Clone()}
	if out.header == nil {
		out.header = make(http.Header)
	}
	p.handleQueryRange(out, child)
	if out.code == 0 {
		out.code = http.StatusOK
	}
	return out, run
}

// storeOffRequestPath runs store in the background when a store slot is free
// and at once otherwise: a kept answer is never dropped under load.
func (p *Proxy) storeOffRequestPath(store func()) {
	if !p.statsExtentStores.start() {
		store()
		return
	}
	go func() {
		defer p.statsExtentStores.done()
		store()
	}()
}

// storeQuerySteps keeps the steps of fragments up to lastStored, merged with
// the entries they extend.
func (p *Proxy) storeQuerySteps(req *statsExtentRequest, key string, lastStored int64, chunks map[int64]*statsExtentChunk, fragments []*statsExtentFragment) {
	if len(fragments) == 0 {
		return
	}
	if chunks == nil {
		chunks = map[int64]*statsExtentChunk{}
	}
	p.storeStatsExtentChunks(req, lastStored, chunks, fragments)
	// The order of the series, learned from an answer that has several.
	for _, f := range fragments {
		if rules := queryStepsOrders(f); len(f.series) > 1 {
			p.cache.SetWithTTL(key+":order", []byte(strconv.Itoa(rules)), p.queryRangeExtentCacheTTL)
		}
	}
}

// Series orders the evaluator writes: by canonicalLabelsKey (the series map
// key of the window builders) and by the key of a typed fold (typedBody.fold).
const (
	queryStepsOrderLabels = 1 << iota
	queryStepsOrderFold
)

type queryStepsSeriesKeys struct {
	labels, fold string
}

func queryStepsKeysOf(metric []byte) (queryStepsSeriesKeys, bool) {
	var m map[string]string
	if err := json.Unmarshal(metric, &m); err != nil {
		return queryStepsSeriesKeys{}, false
	}
	names := make([]string, 0, len(m))
	for name := range m {
		names = append(names, name)
	}
	sort.Strings(names)
	var fold strings.Builder
	for _, name := range names {
		fold.WriteString(strconv.Itoa(len(name)))
		fold.WriteByte(':')
		fold.WriteString(name)
		fold.WriteByte('=')
		fold.WriteString(strconv.Itoa(len(m[name])))
		fold.WriteByte(':')
		fold.WriteString(m[name])
		fold.WriteByte(';')
	}
	return queryStepsSeriesKeys{labels: canonicalLabelsKey(m), fold: fold.String()}, true
}

// queryStepsOrders returns the orders the series of f are written in.
func queryStepsOrders(f *statsExtentFragment) int {
	rules := queryStepsOrderLabels | queryStepsOrderFold
	var prev queryStepsSeriesKeys
	for i, series := range f.series {
		keys, ok := queryStepsKeysOf(series.metric)
		if !ok {
			return 0
		}
		if i > 0 {
			if prev.labels >= keys.labels {
				rules &^= queryStepsOrderLabels
			}
			if prev.fold >= keys.fold {
				rules &^= queryStepsOrderFold
			}
		}
		prev = keys
	}
	return rules
}

// renderQuerySteps writes the merged answer of pieces in the envelope and the
// series order of the evaluator's own answers, or reports false.
func (p *Proxy) renderQuerySteps(ctx context.Context, key string, pieces, fresh []*statsExtentFragment) ([]byte, bool) {
	names, order, ok := mergeQuerySteps(pieces)
	if !ok || len(order) == 0 || len(names) != 4 || names[2] == "" {
		return nil, false // the evaluator answers an empty window itself
	}
	if limit := p.requestQueryLimits(ctx).MaxQuerySeries; limit > 0 && len(order) > limit {
		return nil, false // Loki's series limit applies to the whole window
	}
	if order, ok = p.orderQuerySteps(key, order, fresh); !ok {
		return nil, false
	}
	limit := p.limits().BufferedBackendBodyBytes
	if limit <= 0 {
		limit = int(^uint(0) >> 1)
	}
	if names[2] == "ordered-json-raw" && p.orderedJSONMetricMaxBytes() < int64(limit) {
		limit = int(p.orderedJSONMetricMaxBytes())
	}
	size, ok := queryStepsAddSize(0, len(names[0]), limit)
	if !ok {
		return nil, false
	}
	size, ok = queryStepsAddSize(size, len(names[1]), limit)
	if !ok {
		return nil, false
	}
	points := 0
	for _, s := range order {
		if names[2] == "ordered-json-raw" {
			points += len(s.points)
			if points > maxMetricEvalSamples {
				return nil, false
			}
		}
		sort.Slice(s.points, func(i, j int) bool { return s.points[i].k < s.points[j].k })
		size, ok = queryStepsAddSize(size, len(s.metric), limit)
		if !ok {
			return nil, false
		}
		size, ok = queryStepsAddSize(size, len(`{"metric":,"values":[]},`), limit)
		if !ok {
			return nil, false
		}
		for i, point := range s.points {
			if i > 0 && s.points[i-1].k == point.k {
				return nil, false // pieces that overlap
			}
			size, ok = queryStepsAddSize(size, len(point.raw), limit)
			if !ok {
				return nil, false
			}
			size, ok = queryStepsAddSize(size, 1, limit)
			if !ok {
				return nil, false
			}
		}
	}
	out := make([]byte, 0, size)
	out = append(out, names[0]...)
	for i, s := range order {
		out = appendQueryStepsSeries(out, i, s)
	}
	return append(out, names[1]...), true
}

// queryStepsAddSize bounds every part before adding it, so a large response
// cannot wrap the preallocation size or evade the buffered-body limit.
func queryStepsAddSize(size, part, limit int) (int, bool) {
	if size < 0 || part < 0 || limit < 0 || size > limit || part > limit-size {
		return 0, false
	}
	return size + part, true
}

// mergeQuerySteps joins the series of pieces by their labels, in the order
// they come, or reports false when the pieces are answers of different
// routes or write a series two ways.
func mergeQuerySteps(pieces []*statsExtentFragment) ([]string, []*statsExtentSeries, bool) {
	var names []string
	merged := map[string]*statsExtentSeries{}
	var order []*statsExtentSeries
	pieces = slices.Clone(pieces)
	slices.SortFunc(pieces, func(a, b *statsExtentFragment) int { return cmp.Compare(a.from, b.from) })
	for _, piece := range pieces {
		if names == nil {
			names = piece.names
		} else if !slices.Equal(names, piece.names) {
			return nil, nil, false // answers of different routes
		}
		for _, series := range piece.series {
			keys, ok := queryStepsKeysOf(series.metric)
			if !ok {
				return nil, nil, false
			}
			m := merged[keys.labels]
			if m == nil {
				m = &statsExtentSeries{name: series.name, metric: series.metric}
				merged[keys.labels] = m
				order = append(order, m)
			} else if !bytes.Equal(m.metric, series.metric) {
				return nil, nil, false // one series written two ways
			}
			m.points = append(m.points, series.points...)
		}
	}
	return names, order, true
}

// orderQuerySteps puts the merged series in the order the evaluator writes
// them: the orders its own answers with several series were written in (one
// kept per key, those of the fresh answers), all giving the same order here.
func (p *Proxy) orderQuerySteps(key string, order []*statsExtentSeries, fresh []*statsExtentFragment) ([]*statsExtentSeries, bool) {
	if len(order) < 2 {
		return order, true
	}
	rules := queryStepsOrderLabels | queryStepsOrderFold
	known := false
	if raw, ok := p.cache.Get(key + ":order"); ok {
		if n, err := strconv.Atoi(string(raw)); err == nil {
			rules, known = n, true
		}
	}
	for _, f := range fresh {
		if len(f.series) > 1 {
			rules &= queryStepsOrders(f)
			known = true
		}
	}
	if !known || rules == 0 {
		return nil, false
	}
	keys := make(map[*statsExtentSeries]queryStepsSeriesKeys, len(order))
	for _, s := range order {
		k, ok := queryStepsKeysOf(s.metric)
		if !ok {
			return nil, false
		}
		keys[s] = k
	}
	var sorted []*statsExtentSeries
	for _, rule := range []int{queryStepsOrderLabels, queryStepsOrderFold} {
		if rules&rule == 0 {
			continue
		}
		candidate := slices.Clone(order)
		sort.Slice(candidate, func(i, j int) bool {
			if rule == queryStepsOrderLabels {
				return keys[candidate[i]].labels < keys[candidate[j]].labels
			}
			return keys[candidate[i]].fold < keys[candidate[j]].fold
		})
		if sorted != nil && !slices.Equal(sorted, candidate) {
			return nil, false // the orders the answers allow disagree here
		}
		sorted = candidate
	}
	return sorted, true
}

func appendQueryStepsSeries(out []byte, i int, s *statsExtentSeries) []byte {
	if i > 0 {
		out = append(out, ',')
	}
	out = append(out, `{"metric":`...)
	out = append(out, s.metric...)
	out = append(out, `,"values":[`...)
	for k, point := range s.points {
		if k > 0 {
			out = append(out, ',')
		}
		out = append(out, point.raw...)
	}
	return append(out, "]}"...)
}

// parseQueryStepsAnswer takes a matrix answer apart into the steps [from, to]
// of grid: its envelope (names: the text before and after the series), its
// series as written and their points by step. It reports false when the
// answer is not exactly one it can write back.
func parseQueryStepsAnswer(body []byte, grid queryStepsGrid, from, to int64) (*statsExtentFragment, bool) {
	const head = `"result":[`
	at := bytes.Index(body, []byte(head))
	if at < 0 || !bytes.Contains(body[:at], []byte(`"status":"success"`)) && !bytes.Contains(body[at:], []byte(`"status":"success"`)) {
		return nil, false
	}
	s := &statsExtentScanner{b: body, i: at + len(head)}
	f := &statsExtentFragment{from: from, to: to, readAt: cache.Now().UnixNano()}
	prefix := string(body[:s.i])
	seen := map[string]bool{}
	for !s.at(']') {
		if len(f.series) > 0 && !s.eat(",") {
			return nil, false
		}
		series, ok := scanQueryStepsSeries(s, grid, from, to)
		if !ok || seen[string(series.metric)] {
			return nil, false
		}
		seen[string(series.metric)] = true
		series.name = prefix
		f.series = append(f.series, series)
	}
	f.names = []string{prefix, string(body[s.i:])}
	if !bytes.Contains([]byte(f.names[1]), []byte(`"matrix"`)) && !bytes.Contains([]byte(prefix), []byte(`"matrix"`)) {
		return nil, false
	}
	out := []byte(prefix)
	for i, series := range f.series {
		out = appendQueryStepsSeries(out, i, series)
	}
	return f, bytes.Equal(append(out, f.names[1]...), body)
}

func scanQueryStepsSeries(s *statsExtentScanner, grid queryStepsGrid, from, to int64) (*statsExtentSeries, bool) {
	if !s.eat(`{"metric":`) || !s.at('{') {
		return nil, false
	}
	metricStart := s.i
	s.i++
	for !s.at('}') {
		if s.i > metricStart+1 && !s.eat(",") {
			return nil, false
		}
		if _, ok := s.str(); !ok || !s.eat(":") {
			return nil, false
		}
		if _, ok := s.str(); !ok {
			return nil, false
		}
	}
	s.i++
	series := &statsExtentSeries{metric: s.b[metricStart:s.i]}
	if !s.eat(`,"values":[`) {
		return nil, false
	}
	for !s.at(']') {
		if len(series.points) > 0 && !s.eat(",") {
			return nil, false
		}
		pointStart := s.i
		if !s.eat("[") {
			return nil, false
		}
		numStart := s.i
		for s.i < len(s.b) && strings.IndexByte("0123456789.", s.b[s.i]) >= 0 {
			s.i++
		}
		ns, ok := queryStepsSecondsToNanos(s.b[numStart:s.i])
		if !ok || !s.eat(",") {
			return nil, false
		}
		if _, ok := s.str(); !ok || !s.eat("]") {
			return nil, false
		}
		k := statsExtentFloorDiv(ns-grid.phase+grid.step/2, grid.step)
		if d := ns - grid.at(k); d <= -int64(time.Second) || d >= int64(time.Second) || k < from || k > to {
			return nil, false
		}
		if n := len(series.points); n > 0 && series.points[n-1].k >= k {
			return nil, false
		}
		series.points = append(series.points, statsExtentPoint{k: k, raw: s.b[pointStart:s.i]})
	}
	if !s.eat("]}") || len(series.points) == 0 {
		return nil, false
	}
	return series, true
}

// queryStepsSecondsToNanos reads a timestamp written in seconds, with up to
// nine decimals, exactly.
func queryStepsSecondsToNanos(text []byte) (int64, bool) {
	whole, frac, _ := bytes.Cut(text, []byte("."))
	if len(whole) == 0 || len(whole) > 11 || len(frac) > 9 {
		return 0, false
	}
	sec, err := strconv.ParseInt(string(whole), 10, 64)
	if err != nil {
		return 0, false
	}
	var nanos int64
	for i := 0; i < 9; i++ {
		nanos *= 10
		if i < len(frac) {
			if frac[i] < '0' || frac[i] > '9' {
				return 0, false
			}
			nanos += int64(frac[i] - '0')
		}
	}
	return sec*int64(time.Second) + nanos, true
}

// queryStepsTee passes an answer through and keeps a copy of it, up to limit
// bytes, with its status.
type queryStepsTee struct {
	http.ResponseWriter
	limit  int
	buf    bytes.Buffer
	over   bool
	code   int
	before map[string]bool
	run    *queryStepsRun // nothing is copied of an answer the evaluator did not give
	// sent is the handler's headers at the moment it sent them: a writer
	// below the tee adds its own when the answer goes out (Content-Encoding,
	// Vary and Content-Length of a gzip client's response), and those are not
	// the evaluator's.
	sent http.Header
}

func (t *queryStepsTee) WriteHeader(code int) {
	if code < http.StatusBadRequest {
		t.Header().Set("Content-Type", "application/json")
	} else if t.Header().Get("Content-Type") == "" {
		t.Header().Set("Content-Type", "text/plain; charset=utf-8")
	}
	t.Header().Set("X-Content-Type-Options", "nosniff")
	if t.code == 0 {
		t.code = code
	}
	t.snapshot()
	t.ResponseWriter.WriteHeader(code)
}

func (t *queryStepsTee) snapshot() {
	if t.sent == nil {
		t.sent = t.ResponseWriter.Header().Clone()
	}
}

func (t *queryStepsTee) Write(b []byte) (int, error) {
	if t.code == 0 {
		t.Header().Set("Content-Type", "application/json")
	}
	t.Header().Set("X-Content-Type-Options", "nosniff")
	if t.code == 0 {
		t.code = http.StatusOK
	}
	t.snapshot()
	if t.buf.Len() == 0 && !t.over && (t.code != http.StatusOK || !t.run.evaluatedPerStep()) {
		t.over = true
	}
	if !t.over {
		if t.limit > 0 && t.buf.Len()+len(b) > t.limit {
			t.over, t.buf = true, bytes.Buffer{}
		} else {
			t.buf.Write(b)
		}
	}
	return t.ResponseWriter.Write(b)
}

func (t *queryStepsTee) onlyJSONHeader() bool {
	return onlyJSONHeader(t.sent, t.before)
}

func headerNames(h http.Header) map[string]bool {
	names := make(map[string]bool, len(h))
	for name := range h {
		names[name] = true
	}
	return names
}

// onlyJSONHeader reports whether the handler set no header but the JSON
// Content-Type and nosniff (the ones in before were there already): a warning or a
// partial-answer header is the whole window's.
func onlyJSONHeader(h http.Header, before map[string]bool) bool {
	for name, values := range h {
		if before[name] {
			continue
		}
		switch {
		case name == "Content-Type" && len(values) == 1 && values[0] == "application/json":
		case name == "X-Content-Type-Options" && len(values) == 1 && values[0] == "nosniff":
		default:
			return false
		}
	}
	return true
}
