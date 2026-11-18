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
	// lokiCut is -drilldown-series-limit-mode=loki for a Drilldown range request: the partial answer keeps the first
	// series met and ends at the step where the first series past the limit appears, as Loki's does.
	lokiCut bool
	stepNs  int64        // the request's step, which puts a recorded cut on the answer's grid
	cutNs   atomic.Int64 // the earliest first time (ns) of a series dropped past the limit; 0 = none
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
		if step, ok := parsePositiveStepDuration(r.FormValue("step")); ok { // a POST form too
			scope.lokiCut, scope.stepNs = true, int64(step)
		}
	}
	return r.WithContext(context.WithValue(r.Context(), seriesLimitScopeKey{}, scope)), scope
}

func seriesLimitScopeFrom(ctx context.Context) *seriesLimitScope {
	scope, _ := ctx.Value(seriesLimitScopeKey{}).(*seriesLimitScope)
	return scope
}

// recordCut keeps the earliest first time of a series dropped past the limit (several reads of one request may
// each drop series).
func (s *seriesLimitScope) recordCut(ns int64) {
	if s == nil || ns <= 0 {
		return
	}
	for {
		cur := s.cutNs.Load()
		if cur != 0 && cur <= ns {
			return
		}
		if s.cutNs.CompareAndSwap(cur, ns) {
			return
		}
	}
}

// seriesKeptForLimit returns the limit keys a Drilldown partial answer keeps: the busiest (total, ties by key), or
// in the Loki mode the first met (first time, ties by key), recording the first time of the first key it drops.
func seriesKeptForLimit(ctx context.Context, keys []string, total func(string) float64, first func(string) int64, limit int) []string {
	if limit <= 0 || len(keys) <= limit {
		return keys
	}
	scope := seriesLimitScopeFrom(ctx)
	lokiCut := scope != nil && scope.lokiCut
	totals := make(map[string]float64, len(keys))
	firsts := make(map[string]int64, len(keys))
	for _, k := range keys {
		if lokiCut {
			firsts[k] = first(k)
		} else {
			totals[k] = total(k)
		}
	}
	sorted := append([]string(nil), keys...)
	sort.Slice(sorted, func(i, j int) bool {
		a, b := sorted[i], sorted[j]
		if lokiCut && firsts[a] != firsts[b] {
			return firsts[a] < firsts[b]
		}
		if !lokiCut && totals[a] != totals[b] {
			return totals[a] > totals[b]
		}
		return a < b
	})
	if lokiCut && firsts[sorted[limit]] != math.MaxInt64 {
		scope.recordCut(firsts[sorted[limit]])
	}
	return sorted[:limit]
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
				body = cutLokiResultAfter(body, m.scope.cutNs.Load(), m.scope.stepNs)
			}
			if m.partial != nil {
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

// cutLokiResultAfter ends a Loki matrix answer where Loki ends a Drilldown partial answer: at the first step of
// the answer's grid at or after cutNs (the first time of the first series past the limit), every later point
// dropped, and a series left without points dropped. A vector, a cut of 0 or an unparsable body is unchanged.
func cutLokiResultAfter(body []byte, cutNs, stepNs int64) []byte {
	if cutNs <= 0 || stepNs <= 0 {
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
	var resultType string
	if json.Unmarshal(data["resultType"], &resultType) != nil || resultType != "matrix" {
		return body
	}
	var result []struct {
		Metric json.RawMessage     `json:"metric"`
		Values [][]json.RawMessage `json:"values"`
	}
	if json.Unmarshal(data["result"], &result) != nil {
		return body
	}
	pointNs := func(p []json.RawMessage) (int64, bool) {
		if len(p) == 0 {
			return 0, false
		}
		ts, err := strconv.ParseFloat(string(p[0]), 64)
		return int64(math.Round(ts*1e3)) * int64(time.Millisecond), err == nil
	}
	// The grid: every point lies on origin + k*step; the cut moves up to the first grid time at or after it.
	origin := int64(math.MaxInt64)
	for _, s := range result {
		if len(s.Values) > 0 {
			if ns, ok := pointNs(s.Values[0]); ok && ns < origin {
				origin = ns
			}
		}
	}
	if origin == math.MaxInt64 {
		return body
	}
	end := origin
	if cutNs > origin {
		end = origin + (cutNs-origin+stepNs-1)/stepNs*stepNs
	}
	kept := result[:0]
	for _, s := range result {
		n := 0
		for _, p := range s.Values {
			if ns, ok := pointNs(p); ok && ns <= end {
				s.Values[n] = p
				n++
			}
		}
		if n > 0 {
			s.Values = s.Values[:n]
			kept = append(kept, s)
		}
	}
	encoded, err := json.Marshal(kept)
	if err != nil {
		return body
	}
	data["result"] = encoded
	rewritten, err := marshalOrderedJSONObject(data, []string{"resultType", "result", "stats"})
	if err != nil {
		return body
	}
	resp["data"] = rewritten
	out, err := marshalOrderedJSONObject(resp, []string{"status", "warnings", "data"})
	if err != nil {
		return body
	}
	return out
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
	if scope != nil {
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
	}, func(k string) int64 {
		if first := firstSampleTime(series[k].Samples); first != math.MaxInt64 {
			return first + firstOffset
		}
		return math.MaxInt64
	}, limit)
	out := make(map[string]manualSeriesSamples, len(kept))
	for _, k := range kept {
		out[k] = series[k]
	}
	return out, nil
}

// firstSampleTime is the earliest sample time (ns) of a series, MaxInt64 for none.
func firstSampleTime(samples []rangeMetricSample) int64 {
	first := int64(math.MaxInt64)
	for _, s := range samples {
		if s.ts < first {
			first = s.ts
		}
	}
	return first
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
