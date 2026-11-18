package proxy

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	logqlpkg "github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/lokierr"
	"github.com/grafana/jsonparser"
	fj "github.com/valyala/fastjson"
)

// precheckRawMetricRows counts the rows a long-range raw metric fetch would
// read before fetching them. The count runs as a stats pipe over the same
// filter, so VictoriaLogs ships one row instead of the log lines. Only
// pipe-free queries are checked: their count is exact, whereas filtering pipes
// could shrink the result below the limit.
func (p *Proxy) precheckRawMetricRows(ctx context.Context, baseQuery string, fetchParams url.Values, span time.Duration, rowLimit int) error {
	if strings.Contains(baseQuery, "|") || span < p.backendHeavyQueryMinRange {
		return nil
	}
	params := url.Values{
		"query": {baseQuery + " | stats count() as rows"},
		"start": {fetchParams.Get("start")},
		"end":   {fetchParams.Get("end")},
	}
	resp, err := p.vlPost(ctx, "/select/logsql/query", params)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, err := readBodyLimited(resp.Body, maxUpstreamErrorBodyBytes)
	if err != nil || resp.StatusCode >= http.StatusBadRequest {
		// The fetch itself reports backend errors; the estimate is best effort.
		return nil
	}
	var row struct {
		Rows string `json:"rows"`
	}
	if json.Unmarshal(bytes.TrimSpace(body), &row) != nil {
		return nil
	}
	if rows, convErr := strconv.ParseInt(row.Rows, 10, 64); convErr == nil && rows > int64(rowLimit) {
		return newRowLimitError(rowLimit, "", rows)
	}
	return nil
}

// rowLimitError is a metric evaluation that passed -manual-range-metric-row-limit.
// The error layer answers it as Loki answers a query over one of its own limits
// (recorded answers: internal/lokierr/testdata/loki_error_answers.jsonl): 400.
// Loki has no equivalent flag, so the text names the proxy's own.
type rowLimitError struct {
	limit   int
	unit    string // what the limit counted, when not raw log rows
	matches int64  // the rows the query matches, when a count said so; 0 = unknown
}

// manualRowLimitError is the error of an evaluator that read more rows than
// -manual-range-metric-row-limit allows.
func manualRowLimitError(limit int) error { return newRowLimitError(limit, "", 0) }

func (e *rowLimitError) Error() string {
	var b strings.Builder
	fmt.Fprintf(&b, "manual range metric row limit exceeded (%d", e.limit)
	if e.unit != "" {
		b.WriteString(" " + e.unit)
	}
	b.WriteByte(')')
	if e.matches > 0 {
		fmt.Fprintf(&b, "; the query matches %d log lines", e.matches)
	}
	b.WriteString(proxyLimitMarker + "manual-range-metric-row-limit")
	return b.String()
}

func isRowLimitError(err error) bool {
	var limitErr *rowLimitError
	return errors.As(err, &limitErr)
}

// seriesLimitMessagePrefix opens Loki's series limit message and its Drilldown
// partial-result warning; seriesLimitWarningLimit reads the warning by it.
const seriesLimitMessagePrefix = "maximum number of series ("

// seriesLimitError is the cause of Loki's answer to a metric query over its
// series limit (HTTP 400), word for word:
// Grafana shows the message to the user, so it stays Loki's. It is the typed
// error's cause (newSeriesLimitError builds it), which tells the series
// limit apart for the callers that degrade on it; the proxy's limit is set by
// -max-stats-query-series, which the log line names.
//
// The cause is one of two shared values, so a rejection allocates none: the
// limit's setting is the typed error's own, in the log line's limit_value.
type seriesLimitError struct {
	// rowBudget marks the raw-row budget of a request that folds its series
	// afterwards, not a series limit: it answers the row-limit message.
	rowBudget bool
}

var (
	seriesLimitCause       = &seriesLimitError{}
	seriesLimitBudgetCause = &seriesLimitError{rowBudget: true}
)

// newSeriesLimitError is the error of a metric result over limit series. While
// a request folds its series afterwards and reads them under the raw-row budget
// (withWithoutFold records it in the scope), the budget is the limit that
// request hits before the fold, whatever its value: it answers the row-limit
// message. A caller that applies the tenant's own limit to such a request clears
// the fold from its context first (see tenantSeriesLimitContext).
func newSeriesLimitError(ctx context.Context, limit int) error {
	if folding, _ := ctx.Value(foldedSumKey{}).(bool); folding {
		if scope, _ := ctx.Value(seriesLimitScopeKey{}).(*seriesLimitScope); scope != nil && scope.foldBudget > 0 {
			return lokierr.LimitReached(flagManualRangeMetricRowLimit, int64(limit), seriesLimitBudgetCause)
		}
	}
	return lokierr.SeriesLimitOf(flagMaxStatsQuerySeries, limit, seriesLimitCause)
}

// tenantSeriesLimitContext is ctx for a check against the tenant's own series
// limit: the request is not read under the raw-row budget there.
func tenantSeriesLimitContext(ctx context.Context) context.Context {
	return context.WithValue(ctx, foldedSumKey{}, false)
}

func (e *seriesLimitError) Error() string {
	if e.rowBudget {
		return "metric result over the raw-row budget of a request that folds its series; narrow the query or increase -manual-range-metric-row-limit"
	}
	return "metric result over the series limit; narrow the query or increase -max-stats-query-series"
}

func isSeriesLimitError(err error) bool {
	var limitErr *seriesLimitError
	return errors.As(err, &limitErr)
}

// seriesLimitScope records how a request answers a metric result over the series
// limit. Loki gives Grafana Logs Drilldown the first series with a warning
// (registry limits/drilldown-partial-with-warning) and every other client an
// error (registry limits/series-limit-error).
type seriesLimitScope struct {
	drilldown  bool
	foldBudget int          // the raw-row budget a request that folds its series afterwards reads under, unless the tenant's limit is lower; 0 = none
	truncated  atomic.Int64 // the limit that cut a Drilldown result; 0 = none
	// lokiCut is -drilldown-series-limit-mode=loki for a Drilldown range request: the answer is cut as Loki's query
	// frontend cuts it (cutLokiSplits): in each split of the request (-drilldown-series-limit-split-interval) the
	// series met first, ending at the step where the series met so far pass the limit.
	lokiCut bool
	// The request's start, end and step (ns), the range of its range functions when they share one (0 otherwise),
	// and the split interval: they place a time on the answer's grid and in a split.
	startNs, endNs, stepNs, rangeNs, splitNs int64
	// noWarning is a Drilldown instant request Loki's query frontend splits: its answer over the limit keeps the
	// busiest series without Loki's warning, as Loki's split instant answer does.
	noWarning bool
	// innerTripped is set when an answer this request combines (a binary operand's, a tenant's) was itself cut by
	// the limit: the combined answer carries Loki's warning even where its own splits stay within the limit.
	innerTripped atomic.Bool
}

type seriesLimitScopeKey struct{}

// Series limit modes of a Logs Drilldown answer over the limit (-drilldown-series-limit-mode).
const (
	// drilldownSeriesLimitLoki is Loki's answer: the first series met, the range cut at the step where the first
	// series past the limit appears (Loki's engine; recorded in TestDrilldownSeriesLimitMatchesLoki).
	drilldownSeriesLimitLoki = "loki"
	// drilldownSeriesLimitBusiest keeps the busiest series over the whole range.
	drilldownSeriesLimitBusiest = "busiest"
)

// withSeriesLimitScope attaches a series limit scope for r to its context.
func (p *Proxy) withSeriesLimitScope(r *http.Request) (*http.Request, *seriesLimitScope) {
	scope := &seriesLimitScope{drilldown: isGrafanaDrilldownRequest(r)}
	if scope.drilldown && p.drilldownSeriesLimitMode != drilldownSeriesLimitBusiest {
		limits := p.queryLimitsFor(r.Header.Get("X-Scope-OrgID"))
		window, sameWindow := allRangeWindowsEqual(r.FormValue("query"))
		step, okStep := parsePositiveStepDuration(r.FormValue("step")) // a POST form too
		start, okStart := parseLokiTimeToUnixNano(r.FormValue("start"))
		end, okEnd := parseLokiTimeToUnixNano(r.FormValue("end"))
		switch {
		case okStep && okStart && okEnd && end >= start:
			scope.lokiCut = true
			scope.startNs, scope.endNs, scope.stepNs, scope.splitNs = start, end, int64(step), int64(limits.SplitQueriesByInterval)
			if sameWindow {
				scope.rangeNs = int64(window)
			}
		case !okStep && sameWindow && limits.SplitInstantMetricQueriesByInterval > 0 && window > limits.SplitInstantMetricQueriesByInterval:
			// Deployed Loki's query frontend splits an instant metric query whose range is longer than
			// split_instant_metric_queries_by_interval, and its answer over the limit carries no partial-result
			// warning (recorded on Loki 3.7.10; why is not documented).
			scope.noWarning = true
		}
	}
	return r.WithContext(context.WithValue(r.Context(), seriesLimitScopeKey{}, scope)), scope
}

func seriesLimitScopeFrom(ctx context.Context) *seriesLimitScope {
	scope, _ := ctx.Value(seriesLimitScopeKey{}).(*seriesLimitScope)
	return scope
}

// splitOf is the index of the split of the request that holds the answer's step t: Loki's query frontend splits a
// range request at the multiples of the split interval since the epoch strictly inside (start, end), and a step on
// a boundary starts the later split. 0 without a split interval.
func (s *seriesLimitScope) splitOf(t int64) int {
	if s.splitNs <= 0 {
		return 0
	}
	first := s.startNs - s.startNs%s.splitNs + s.splitNs // the first multiple after start
	if s.startNs < 0 {
		first = s.startNs - s.startNs%s.splitNs
	}
	if first >= s.endNs || t < first {
		return 0
	}
	last := (s.endNs - 1 - first) / s.splitNs // the index of the last boundary before end
	if k := (t - first) / s.splitNs; k < last {
		return int(k) + 1
	}
	return int(last) + 1
}

// stepOf is the first step of the request's grid that meets a line (or a sample) at t: Loki's sample at step T
// covers (T-range, T].
func (s *seriesLimitScope) stepOf(t int64) int64 {
	if s.stepNs <= 0 || t <= s.startNs {
		return s.startNs
	}
	return s.startNs + (t-s.startNs+s.stepNs-1)/s.stepNs*s.stepNs
}

// firstStepsInSplits yields, for a run of steps from first to last (the answer's grid), its first step in every
// split it reaches.
func (s *seriesLimitScope) firstStepsInSplits(first, last int64, yield func(int64)) {
	yield(first)
	for split := s.splitOf(first) + 1; split <= s.splitOf(last); split++ {
		boundary := s.startNs - s.startNs%s.splitNs + int64(split)*s.splitNs
		if s.startNs < 0 {
			boundary -= s.splitNs
		}
		yield(s.stepOf(boundary))
	}
}

// lineMet yields the first step, in every split, that meets a line (or a VictoriaLogs bucket labelled by its start,
// passed one nanosecond later) at t: the steps T with T - range < t <= T. A line no step reads yields nothing: one
// after end, or at or before start - max(range, step) (VictoriaLogs aligns a bucket read's start down to its grid,
// so a read can hold the bucket before the request's lines).
func (s *seriesLimitScope) lineMet(t int64, yield func(int64)) {
	if t > s.endNs || s.rangeNs > 0 && t <= s.startNs-max(s.rangeNs, s.stepNs) {
		return
	}
	first := s.stepOf(t)
	last := first
	if s.rangeNs > s.stepNs && s.stepNs > 0 {
		last = s.stepOf(t+s.rangeNs) - s.stepNs
	}
	if last > s.endNs {
		last = s.endNs
	}
	if last < first {
		last = first
	}
	s.firstStepsInSplits(first, last, yield)
}

// rankingGrid is the time grid a first-met ranking keeps its values per, for reads that begin lookback before each
// step (max(range, step); a range shorter than the step keeps only its windows' lines): VictoriaLogs' buckets
// `_time:<width> offset <offset>` each hold the lines (e, e+width] for the grid's edges e, and buckets is how many
// of them the read (start-lookback, end] reaches. ok is false for a request in one split, ranked over its read whole.
//
// Every split k after the first begins at its first step s_k; the lines its steps read are (s_k - lookback,
// last step of k]. The edges include every s_k - lookback, so the buckets that begin inside split k's lines hold
// only lines split k reads, and the values met first in each bucket include the values split k meets first: a
// value v that split k meets through its earliest line t (in a bucket beginning at e >= s_k - lookback) is behind,
// in that bucket, only values with a line in (e, t], which split k meets no later than v. The edges are the split
// starts when the split interval is a multiple of the step (then every s_k lies the same time past its boundary),
// else every step (a step longer than the split puts at most one in each split; a shorter one that does not divide
// it keeps exactness at one bucket per step).
func (s *seriesLimitScope) rankingGrid(lookback int64) (width, offset int64, buckets int, ok bool) {
	if s == nil || s.splitNs <= 0 || s.stepNs <= 0 || lookback <= 0 || s.splitOf(s.endNs) == 0 {
		return 0, 0, 0, false
	}
	first := s.startNs - s.startNs%s.splitNs + s.splitNs // the first boundary after start
	if s.startNs < 0 {
		first = s.startNs - s.startNs%s.splitNs
	}
	width = s.stepNs
	if s.splitNs%s.stepNs == 0 {
		width = s.splitNs
	}
	edge := s.stepOf(first) - lookback // a grid edge
	// VictoriaLogs truncates t+offset to the width and takes the offset back: a bucket begins at edge+1ns.
	offset = ((lookback-1-s.stepOf(first))%width + width) % width
	index := func(t int64) int64 { return floorDiv(t-edge-1, width) } // the bucket of a line at t
	return width, offset, int(index(s.endNs)-index(s.startNs-lookback+1)) + 1, true
}

// rankingFallsBack reports a request whose ranking grid has far more buckets than splits: the split interval is not
// a multiple of a shorter step (a 7 s step against 1h; no Grafana $__auto step), so the grid has a bucket per step,
// more than 2x(splits+1) of them. Such a request is not ranked: the range == step answer reads its whole answer (an
// answer past the byte bounds is Loki's limit error naming the flag), and a top-value read, which only runs once the
// whole answer passed those bounds, answers that error.
func (s *seriesLimitScope) rankingFallsBack(lookback int64) bool {
	width, _, buckets, ok := s.rankingGrid(lookback)
	return ok && width < s.splitNs && int64(buckets) > 2*int64(s.splitOf(s.endNs)+2)
}

// rankedBound is the most series a VictoriaLogs ranking of limit+1 values returns for the request: limit+1, or in
// the Loki mode limit+1 per bucket of its ranking grid (rankingGrid, the reads beginning a step before each step:
// the ranked in() read of a range == step answer).
func (s *seriesLimitScope) rankedBound(limit int) int {
	if s == nil || !s.lokiCut {
		return limit + 1
	}
	if _, _, buckets, ok := s.rankingGrid(s.stepNs); ok {
		return (limit + 1) * buckets
	}
	return limit + 1
}

// keepsSuperset reports a request whose series caps keep, in the Loki mode, more than the limit (every split's
// first limit+1, seriesKeptForLimit) for cutLokiSplits to cut on the finished answer.
func keepsSuperset(ctx context.Context) bool {
	scope := seriesLimitScopeFrom(ctx)
	return scope != nil && scope.lokiCut
}

// seriesKeptForLimit returns the keys a Drilldown partial answer keeps. The busiest mode keeps limit keys (total,
// ties by key). The Loki mode keeps, for every split of the request, the limit+1 keys met first in it (the first
// step of their times, ties by key), all splits together: a bounded superset from which cutLokiSplits, on the
// finished answer, keeps the series and steps Loki keeps. times yields a key's sample (or line) times.
func seriesKeptForLimit(ctx context.Context, keys []string, total func(string) float64, times func(string, func(int64)), limit int) []string {
	if limit <= 0 || len(keys) <= limit {
		return keys
	}
	scope := seriesLimitScopeFrom(ctx)
	if scope != nil && scope.lokiCut {
		return firstMetPerSplit(scope, keys, times, limit+1)
	}
	totals := make(map[string]float64, len(keys))
	for _, k := range keys {
		totals[k] = total(k)
	}
	sorted := append([]string(nil), keys...)
	sort.Slice(sorted, func(i, j int) bool {
		a, b := sorted[i], sorted[j]
		if totals[a] != totals[b] {
			return totals[a] > totals[b]
		}
		return a < b
	})
	return sorted[:limit]
}

// firstMetPerSplit is the union over the request's splits of the n keys met first in each.
func firstMetPerSplit(scope *seriesLimitScope, keys []string, times func(string, func(int64)), n int) []string {
	type met struct {
		first int64
		key   string
	}
	bySplit := map[int][]met{}
	for _, k := range keys {
		firsts := map[int]int64{}
		times(k, func(t int64) {
			step := scope.stepOf(t)
			split := scope.splitOf(step)
			if cur, ok := firsts[split]; !ok || step < cur {
				firsts[split] = step
			}
		})
		for split, first := range firsts {
			bySplit[split] = append(bySplit[split], met{first, k})
		}
	}
	keep := map[string]bool{}
	for _, list := range bySplit {
		sort.Slice(list, func(i, j int) bool {
			if list[i].first != list[j].first {
				return list[i].first < list[j].first
			}
			return list[i].key < list[j].key
		})
		for _, m := range list[:min(n, len(list))] {
			keep[m.key] = true
		}
	}
	out := make([]string, 0, len(keep))
	for _, k := range keys {
		if keep[k] {
			out = append(out, k)
		}
	}
	return out
}

// outerFoldedSeriesKey is separate from foldedSumKey: that existing scope can carry a
// without fold's row budget or a grouped variance fold's series ceiling.
type outerFoldedSeriesKey struct{}

// withFoldedSeries marks a request whose stats answer feeds aggregations
// written around it: Loki counts max_query_series on the answer of the whole
// query (registry limits/series-limit-error), so the handler of the outer
// layers applies the limit and the answer of the layer below is not capped.
func withFoldedSeries(r *http.Request) *http.Request {
	return r.Clone(context.WithValue(r.Context(), outerFoldedSeriesKey{}, true))
}

func outerSeriesFolded(ctx context.Context) bool {
	folded, _ := ctx.Value(outerFoldedSeriesKey{}).(bool)
	return folded
}

// seriesLimitReached is called when a metric result holds more than limit
// series. It returns Loki's error, or nil when the request keeps a partial
// result (Drilldown); the caller then keeps limit series.
func seriesLimitReached(ctx context.Context, limit int) error {
	if scope, _ := ctx.Value(seriesLimitScopeKey{}).(*seriesLimitScope); scope != nil && scope.drilldown {
		scope.truncated.Store(int64(limit))
		return nil
	}
	return newSeriesLimitError(ctx, limit)
}

// logQLProducesSamples reports whether logql evaluates to samples (a matrix or
// vector) rather than log lines. Only those responses carry sample timestamps to
// shift or series to limit; a log query must stream straight through.
func logQLProducesSamples(logql string) bool {
	parsed, ok := sharedLogQLExpr(logql)
	if !ok {
		return false
	}
	switch parsed.(type) {
	case *logqlpkg.RangeAggregation, *logqlpkg.VectorAggregation, *logqlpkg.BinOpExpr, *logqlpkg.OpaqueMetricExpr, *logqlpkg.LiteralExpr:
		return true
	}
	return false
}

// metricResponseRewriter buffers a metric response and, before sending it, moves
// every sample timestamp forward by shift (a LogQL offset evaluates the shifted
// range but stamps samples at the requested times) and adds a Drilldown series
// limit warning.
type metricResponseRewriter struct {
	http.ResponseWriter
	buf    bytes.Buffer
	status int
	shift  time.Duration
	scope  *seriesLimitScope
	// partial, when set, counts a Drilldown answer cut by the series limit, by mode
	// (loki_vl_proxy_internal_operation_total{operation="drilldown_series_limit"}).
	partial func(mode string)
	// store, when set, keeps the answer finish sends (a 200): a response cache written from the body below the
	// rewriter would keep it before the Loki cut, so a repeat served from it would differ from the cold answer.
	store func(body []byte)
}

// storeAnswer keeps body under the request's response cache: through the rewriter (the answer it sends, after the
// Loki cut) when there is one, otherwise with Loki's warning added as the rewriter would.
func (m *metricResponseRewriter) storeAnswer(body []byte, scope *seriesLimitScope, set func([]byte)) {
	if m != nil {
		m.store = set
		return
	}
	set(addSeriesLimitWarning(append([]byte(nil), body...), scope))
}

func (m *metricResponseRewriter) WriteHeader(code int) { m.status = code }

func (m *metricResponseRewriter) Write(b []byte) (int, error) { return m.buf.Write(b) }

func (m *metricResponseRewriter) finish() {
	body := m.buf.Bytes()
	if m.status == 0 || m.status == http.StatusOK {
		if m.scope != nil && m.scope.truncated.Load() > 0 {
			mode := drilldownSeriesLimitBusiest
			if m.scope.lokiCut {
				mode = drilldownSeriesLimitLoki
				body = m.scope.cutAnswer(body)
			}
			if m.partial != nil && m.scope.truncated.Load() > 0 {
				m.partial(mode)
			}
		}
		body = shiftLokiResultTimestamps(body, m.shift)
		body = addSeriesLimitWarning(body, m.scope)
		if m.store != nil {
			m.store(append([]byte(nil), body...))
		}
	}
	if m.status != 0 {
		m.ResponseWriter.WriteHeader(m.status)
	}
	_, _ = m.ResponseWriter.Write(body)
}

// cutAnswer is the Loki mode's answer over the limit (cutLokiSplits on the request's splits). When no split passes
// the limit, and no answer it combines was cut, the answer is whole: no warning.
func (s *seriesLimitScope) cutAnswer(body []byte) []byte {
	out, tripped := cutLokiSplits(body, s, int(s.truncated.Load()))
	if !tripped && !s.innerTripped.Load() {
		s.truncated.Store(0)
	}
	return out
}

// cutLokiSplits cuts a Loki matrix answer as Loki's query frontend and engine cut a Logs Drilldown range answer over
// the limit: the frontend splits the request (scope.splitOf) and Loki's engine evaluates each split on its own,
// stopping at the step where the series met so far in the split pass the limit. That step keeps the series met
// before it and, of those new at it, enough to fill the limit (Loki's order among them is not fixed; here by
// labels). The splits' answers are concatenated and not limited again. It reports whether any split was cut. A
// vector, a limit of 0 or an unparsable body is unchanged.
//
// It reads the answer in place and copies its bytes: everything outside data.result, every series it keeps whole
// and every point it keeps are written as they came, so only the output buffer is allocated.
//
//nolint:gocyclo // one pass over the series, one over the splits and one that writes: the cut is one function.
func cutLokiSplits(body []byte, scope *seriesLimitScope, limit int) ([]byte, bool) {
	if limit <= 0 || scope == nil {
		return body, false
	}
	if resultType, err := jsonparser.GetString(body, "data", "resultType"); err != nil || resultType != "matrix" {
		return body, false
	}
	result, typ, resultEnd, err := jsonparser.Get(body, "data", "result")
	if err != nil || typ != jsonparser.Array {
		return body, false
	}
	resultStart := resultEnd - len(result)
	// One read of the series: each one's bytes, its labels, its values array and its points' times.
	type seriesBytes struct {
		raw                    []byte
		key                    string
		valuesStart, valuesEnd int
		times                  []int64 // -1: a point without a readable time
	}
	var all []seriesBytes
	malformed := false
	if _, err := jsonparser.ArrayEach(result, func(series []byte, _ jsonparser.ValueType, _ int, _ error) {
		values, typ, valuesEnd, err := jsonparser.Get(series, "values")
		if err != nil || typ != jsonparser.Array {
			malformed = true
			return
		}
		metric, _, _, _ := jsonparser.Get(series, "metric")
		sb := seriesBytes{raw: series, key: string(metric), valuesStart: valuesEnd - len(values), valuesEnd: valuesEnd}
		if _, err := jsonparser.ArrayEach(values, func(point []byte, _ jsonparser.ValueType, _ int, _ error) {
			ns, ok := lokiPointTimeNs(point)
			if !ok {
				ns = -1
			}
			sb.times = append(sb.times, ns)
		}); err != nil {
			malformed = true
			return
		}
		all = append(all, sb)
	}); err != nil || malformed {
		return body, false
	}
	// Per split: the series by the first step that meets them in it; the kept ones reach up to the trip step.
	type met struct {
		first int64
		idx   int
	}
	bySplit := map[int][]met{}
	for i, sb := range all {
		firsts := map[int]int64{}
		for _, t := range sb.times {
			if t < 0 {
				continue
			}
			split := scope.splitOf(t)
			if cur, ok := firsts[split]; !ok || t < cur {
				firsts[split] = t
			}
		}
		for split, first := range firsts {
			bySplit[split] = append(bySplit[split], met{first, i})
		}
	}
	// reach[i][split] is the last time series i keeps in split; a split it is not kept in is missing.
	reach := make([]map[int]int64, len(all))
	tripped := false
	for split, list := range bySplit {
		sort.Slice(list, func(a, b int) bool {
			if list[a].first != list[b].first {
				return list[a].first < list[b].first
			}
			return all[list[a].idx].key < all[list[b].idx].key
		})
		until := int64(math.MaxInt64)
		kept := list
		if len(list) > limit {
			until, kept, tripped = list[limit].first, list[:limit], true
		}
		for _, m := range kept {
			if reach[m.idx] == nil {
				reach[m.idx] = map[int]int64{}
			}
			reach[m.idx][split] = until
		}
	}
	if !tripped {
		return body, false
	}
	out := make([]byte, 0, len(body))
	out = append(out, body[:resultStart]...)
	out = append(out, '[')
	kept := 0
	for i, sb := range all {
		keep := func(t int64) bool {
			if t < 0 {
				return false
			}
			until, ok := reach[i][scope.splitOf(t)]
			return ok && t <= until
		}
		points, whole := 0, true
		for _, t := range sb.times {
			if keep(t) {
				points++
			} else {
				whole = false
			}
		}
		if points == 0 {
			continue
		}
		if kept > 0 {
			out = append(out, ',')
		}
		kept++
		if whole {
			out = append(out, sb.raw...)
			continue
		}
		out = append(out, sb.raw[:sb.valuesStart]...)
		out = append(out, '[')
		n, j := 0, 0
		_, _ = jsonparser.ArrayEach(sb.raw[sb.valuesStart:sb.valuesEnd], func(point []byte, _ jsonparser.ValueType, _ int, _ error) {
			if keep(sb.times[j]) {
				if n > 0 {
					out = append(out, ',')
				}
				out = append(out, point...)
				n++
			}
			j++
		})
		out = append(out, ']')
		out = append(out, sb.raw[sb.valuesEnd:]...)
	}
	out = append(out, ']')
	return append(out, body[resultEnd:]...), true
}

// lokiPointTimeNs is the time of a Loki matrix point ([<seconds>, "<value>"]) in ns, rounded to the millisecond
// as Loki's times are. A number of whole seconds with at most three decimals is read exactly without a float; any
// other number goes through ParseFloat.
func lokiPointTimeNs(point []byte) (int64, bool) {
	num := lokiPointTimeText(point)
	if ns, ok := wholeMillisecondSecondsNs(num); ok {
		return ns, true
	}
	if len(num) == 0 || (num[0] != '-' && (num[0] < '0' || num[0] > '9')) {
		return 0, false // a string or another JSON value
	}
	sec, err := jsonparser.ParseFloat(num)
	if err != nil || math.IsNaN(sec) || math.IsInf(sec, 0) {
		return 0, false
	}
	return int64(math.Round(sec*1e3)) * int64(time.Millisecond), true
}

// lokiPointTimeText is the text of a point's first element, nil when the point is not an array.
func lokiPointTimeText(point []byte) []byte {
	if len(point) == 0 || point[0] != '[' {
		return nil
	}
	space := func(c byte) bool { return c == ' ' || c == '\t' || c == '\n' || c == '\r' }
	i := 1
	for i < len(point) && space(point[i]) {
		i++
	}
	j := i
	for j < len(point) && point[j] != ',' && point[j] != ']' && !space(point[j]) {
		j++
	}
	return point[i:j]
}

// wholeMillisecondSecondsNs reads seconds with at most three decimals (up to 10 digits of seconds, so the
// nanoseconds fit an int64) exactly, without a float.
func wholeMillisecondSecondsNs(num []byte) (int64, bool) {
	var sec, ms int64
	digits, frac := 0, -1
	for _, c := range num {
		switch {
		case c >= '0' && c <= '9' && frac < 0:
			sec = sec*10 + int64(c-'0')
			digits++
		case c >= '0' && c <= '9' && frac < 3:
			ms = ms*10 + int64(c-'0')
			frac++
		case c == '.' && frac < 0 && digits > 0:
			frac = 0
		default:
			return 0, false
		}
	}
	if digits == 0 || digits > 10 || frac == 0 {
		return 0, false
	}
	for k := max(frac, 0); k < 3; k++ {
		ms *= 10
	}
	return sec*int64(time.Second) + ms*int64(time.Millisecond), true
}

// shiftLokiResultTimestamps adds shift to the sample timestamps of a Loki
// matrix or vector response.
func shiftLokiResultTimestamps(body []byte, shift time.Duration) []byte {
	if shift == 0 {
		return body
	}
	var resp map[string]json.RawMessage
	if json.Unmarshal(body, &resp) != nil {
		return body
	}
	var data map[string]json.RawMessage
	if json.Unmarshal(resp["data"], &data) != nil {
		return body
	}
	var result []json.RawMessage
	if _, ok := data["result"]; !ok || json.Unmarshal(data["result"], &result) != nil {
		return body
	}
	moveTS := func(point []json.RawMessage) {
		if len(point) == 0 {
			return
		}
		if ts, err := strconv.ParseFloat(string(point[0]), 64); err == nil {
			// The point time is whole milliseconds; take the nearest one before
			// the shift, since ts*1e9 is only within 256 ns of it.
			ns := int64(math.Round(ts*1e3))*int64(time.Millisecond) + int64(shift)
			point[0] = json.RawMessage(strconv.FormatFloat(lokiSampleSeconds(ns), 'f', -1, 64))
		}
	}
	for i, raw := range result {
		var series map[string]json.RawMessage
		if json.Unmarshal(raw, &series) != nil {
			return body
		}
		if values, ok := series["values"]; ok {
			var points [][]json.RawMessage
			if json.Unmarshal(values, &points) != nil {
				return body
			}
			for _, point := range points {
				moveTS(point)
			}
			series["values"], _ = json.Marshal(points)
		}
		if value, ok := series["value"]; ok {
			var point []json.RawMessage
			if json.Unmarshal(value, &point) != nil {
				return body
			}
			moveTS(point)
			series["value"], _ = json.Marshal(point)
		}
		result[i], _ = json.Marshal(series)
	}
	data["result"], _ = json.Marshal(result)
	rewritten, err := marshalOrderedJSONObject(data, []string{"resultType", "result", "stats"})
	if err != nil {
		return body
	}
	resp["data"] = rewritten
	// Loki answers status, warnings, data in that order; a plain map marshal
	// would sort the keys and move data before status.
	out, err := marshalOrderedJSONObject(resp, []string{"status", "warnings", "data"})
	if err != nil {
		return body
	}
	return out
}

// marshalOrderedJSONObject marshals fields in the given key order first, then
// any remaining keys in sorted order, so a rewritten body keeps Loki's shape
// instead of Go's map ordering.
func marshalOrderedJSONObject(fields map[string]json.RawMessage, order []string) ([]byte, error) {
	rest := make([]string, 0, len(fields))
	ordered := make(map[string]bool, len(order))
	for _, key := range order {
		ordered[key] = true
	}
	for key := range fields {
		if !ordered[key] {
			rest = append(rest, key)
		}
	}
	sort.Strings(rest)
	var buf bytes.Buffer
	buf.WriteByte('{')
	write := func(key string) error {
		value, ok := fields[key]
		if !ok || len(value) == 0 {
			return nil
		}
		if buf.Len() > 1 {
			buf.WriteByte(',')
		}
		name, err := json.Marshal(key)
		if err != nil {
			return err
		}
		buf.Write(name)
		buf.WriteByte(':')
		buf.Write(value)
		return nil
	}
	for _, key := range append(append([]string{}, order...), rest...) {
		if err := write(key); err != nil {
			return nil, err
		}
	}
	buf.WriteByte('}')
	return buf.Bytes(), nil
}

// addSeriesLimitWarning adds Loki's partial-result warning to a JSON response
// when the request's Drilldown result was truncated.
func addSeriesLimitWarning(body []byte, scope *seriesLimitScope) []byte {
	limit := int64(0)
	if scope != nil && !scope.noWarning {
		limit = scope.truncated.Load()
	}
	if limit == 0 || len(body) == 0 || body[0] != '{' || hasTopLevelWarnings(body) {
		return body
	}
	warning, _ := json.Marshal([]string{fmt.Sprintf("maximum number of series (%d) reached for a single query; returning partial results", limit)})
	separator := []byte{}
	if len(body) > 2 {
		separator = []byte{','}
	}
	// bytes.Join sizes the result once and checks the sum for overflow, so the
	// response is not copied twice and no length arithmetic happens here.
	return bytes.Join([][]byte{[]byte(`{"warnings":`), warning, separator, body[1:]}, nil)
}

// seriesLimitWarningLimit returns N when a response carries Loki's Drilldown
// partial-result warning "maximum number of series (N) reached for a single
// query; returning partial results", and 0 otherwise. addSeriesLimitWarning
// puts the warnings first, so a body that does not open with them is not
// parsed.
func seriesLimitWarningLimit(body []byte) int {
	if !bytes.HasPrefix(body, []byte(`{"warnings":`)) {
		return 0
	}
	v, err := fj.ParseBytes(body)
	if err != nil {
		return 0
	}
	for _, warning := range v.GetArray("warnings") {
		var limit int
		if _, err := fmt.Sscanf(string(warning.GetStringBytes()), seriesLimitMessagePrefix+"%d) reached for a single query; returning partial results", &limit); err == nil && limit > 0 {
			return limit
		}
	}
	return 0
}

// hasTopLevelWarnings reports whether the response object already carries a
// top-level "warnings" key. A log line inside data can hold the same bytes, so
// the check parses instead of scanning.
func hasTopLevelWarnings(body []byte) bool {
	v, err := fj.ParseBytes(body)
	if err != nil {
		return false
	}
	obj, err := v.Object()
	if err != nil {
		return false
	}
	return obj.Get("warnings") != nil
}

// capSeriesToLimit enforces the series limit on a finished Loki response: Loki's
// error for a plain client, the busiest limit series for Logs Drilldown (Loki
// keeps the first series it encounters; the proxy keeps the busiest, which is
// what a Drilldown panel renders). The returned body is unchanged when the
// result is within the limit.
func capSeriesToLimit(ctx context.Context, body []byte, limit int) ([]byte, error) {
	if limit <= 0 || lokiResultSeriesCount(body) <= limit {
		return body, nil
	}
	if err := seriesLimitReached(ctx, limit); err != nil {
		return nil, err
	}
	return limitLokiResultSeriesFor(ctx, body, limit), nil
}

// capSeriesForRequest is capSeriesToLimit for a result still held as series:
// Loki's error for a plain client, the busiest limit series with Loki's
// warning for Logs Drilldown.
func capSeriesForRequest(ctx context.Context, series map[string]manualSeriesSamples, limit int) (map[string]manualSeriesSamples, error) {
	return capSeriesForRequestAt(ctx, series, limit, 0)
}

// capBucketSeriesForRequest is capSeriesForRequest for series whose samples are VictoriaLogs buckets labelled by
// their start: a bucket's lines lie after its label, so the first step that meets them is the next one.
func capBucketSeriesForRequest(ctx context.Context, series map[string]manualSeriesSamples, limit int) (map[string]manualSeriesSamples, error) {
	return capSeriesForRequestAt(ctx, series, limit, 1)
}

func capSeriesForRequestAt(ctx context.Context, series map[string]manualSeriesSamples, limit int, firstOffset int64) (map[string]manualSeriesSamples, error) {
	if limit <= 0 || len(series) <= limit {
		return series, nil
	}
	if err := seriesLimitReached(ctx, limit); err != nil {
		return nil, err
	}
	keys := make([]string, 0, len(series))
	for k := range series {
		keys = append(keys, k)
	}
	kept := seriesKeptForLimit(ctx, keys, func(k string) float64 {
		var total float64
		for _, smp := range series[k].Samples {
			total += smp.value
		}
		return total
	}, func(k string, yield func(int64)) {
		scope := seriesLimitScopeFrom(ctx)
		for _, smp := range series[k].Samples {
			if firstOffset > 0 {
				scope.lineMet(smp.ts+firstOffset, yield) // a bucket: its lines are read by every step its range reaches
			} else {
				yield(smp.ts)
			}
		}
	}, limit)
	out := make(map[string]manualSeriesSamples, len(kept))
	for _, k := range kept {
		out[k] = series[k]
	}
	return out, nil
}

// seriesLimitCollecting reports whether a collector that has seen n series
// must stop with Loki's error: a plain client fails as soon as the limit is
// passed, while a Drilldown request collects every series and keeps the
// busiest at the end (capSeriesForRequest).
func seriesLimitCollecting(ctx context.Context, n, limit int) error {
	if limit <= 0 || n < limit || requestKeepsPartialSeries(ctx) {
		return nil
	}
	return newSeriesLimitError(ctx, limit)
}

func (p *Proxy) manualMetricRowBudget() (int, error) {
	limit := p.rangeMetricRowLimit
	if limit <= 0 {
		limit = 1_000_000
	}
	if limit == math.MaxInt {
		return 0, fmt.Errorf("manual range metric row limit is too large")
	}
	return limit, nil
}

func checkManualMetricRead(ctx context.Context, limited *io.LimitedReader) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return checkManualMetricBudget(ctx, limited)
}

// checkManualMetricReadDone is checkManualMetricRead for a per-row loop: done
// is ctx.Done(), taken once, so a row costs one channel poll instead of a walk
// down the request's context chain (a checked request adds layers to it).
func checkManualMetricReadDone(ctx context.Context, done <-chan struct{}, limited *io.LimitedReader) error {
	select {
	case <-done:
		return ctx.Err()
	default:
	}
	return checkManualMetricBudget(ctx, limited)
}

func checkManualMetricBudget(ctx context.Context, limited *io.LimitedReader) error {
	if limited.N <= 0 {
		return newManualMetricResponseTooLarge(int64(executionLimitsFrom(ctx).BufferedBackendBodyBytes))
	}
	return nil
}

func bareParserRawSampleWeight(entry map[string]interface{}, spec bareParserMetricCompatSpec) (float64, bool) {
	if spec.unwrapField != "" {
		value, ok := stringifyEntryValue(entry[spec.unwrapField])
		if !ok {
			return 0, false
		}
		// unwrap duration(f) / bytes(f) carry unit strings such as "15ms" or
		// "1024B"; a plain float parse would drop every sample and return no
		// series where Loki returns the converted values.
		return convertUnwrapValue(value, spec.unwrapConv)
	}
	if spec.funcName == "bytes_over_time" || spec.funcName == "bytes_rate" {
		msg, _ := stringifyEntryValue(entry["_msg"])
		return float64(len(msg)), true
	}
	return 1, true
}

// Use the bounded metric encoder for raw parser results too. Enforce the
// evaluation and sample limits before allocating each point, rather than
// building an arbitrarily large document and checking its serialized size.
func buildBoundedBareParserMetric(ctx context.Context, series []bareParserMetricSeries, start, end, step int64, spec bareParserMetricCompatSpec, isRange bool) ([]byte, error) {
	distance := time.Unix(0, end).Sub(time.Unix(0, start))
	if step <= 0 || end < start || distance == time.Duration(math.MaxInt64) || distance/time.Duration(step) >= maxMetricEvalSamples {
		return nil, fmt.Errorf("invalid or excessive manual metric evaluation points")
	}
	ctx = binaryEvaluationContext(ctx)
	result := make(map[string]*binaryMatchedSeries, len(series))
	for index, entry := range series {
		if err := checkBinaryOutputLabels(ctx, entry.metric); err != nil {
			return nil, err
		}
		out := &binaryMatchedSeries{labels: entry.metric}
		left, right := 0, 0
		for evaluation := start; evaluation <= end; {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			for right < len(entry.samples) && entry.samples[right].tsNanos <= evaluation {
				right++
			}
			for left < right && entry.samples[left].tsNanos <= evaluation-int64(spec.rangeWindow) {
				left++
			}
			if right > left {
				if err := checkBinaryOutputSample(ctx); err != nil {
					return nil, err
				}
				value := bareParserMetricWindowValue(spec.funcName, entry.samples[left:right], spec)
				out.points = append(out.points, []any{lokiSampleSeconds(evaluation), strconv.FormatFloat(value, 'f', -1, 64)})
			}
			if end-evaluation < step {
				break
			}
			evaluation += step
		}
		if len(out.points) > 0 {
			result[strconv.Itoa(index)] = out
		}
	}
	resultType := "vector"
	if isRange {
		resultType = "matrix"
	}
	return encodeBinarySeriesContext(ctx, result, resultType, executionLimitsFrom(ctx).BufferedBackendBodyBytes)
}

func (p *Proxy) writeBoundedBareParserMetric(w http.ResponseWriter, r *http.Request, requestStart time.Time, query string, series []bareParserMetricSeries, start, end, step int64, spec bareParserMetricCompatSpec, isRange bool) {
	body, err := buildBoundedBareParserMetric(r.Context(), series, start, end, step, spec, isRange)
	status := http.StatusOK
	if err != nil {
		status = p.writeErr(w, r.Context(), err)
	} else {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(body)
	}
	endpoint := "query"
	if isRange {
		endpoint = "query_range"
	}
	elapsed := time.Since(requestStart)
	p.metrics.RecordRequest(endpoint, status, elapsed)
	p.queryTracker.Record(endpoint, query, elapsed, err != nil)
}
