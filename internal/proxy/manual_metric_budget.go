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
		return fmt.Errorf("manual range metric row limit exceeded (%d); the query matches %d log lines; narrow the query or increase -manual-range-metric-row-limit", rowLimit, rows)
	}
	return nil
}

// seriesLimitMessagePrefix opens Loki's series limit message; writeError uses it
// to name the proxy flag in the log without changing what the client reads.
const seriesLimitMessagePrefix = "maximum number of series ("

// seriesLimitError is Loki's answer to a metric query over its series limit
// (logqlmodel.NewSeriesLimitError, HTTP 400), word for word: Grafana shows the
// message to the user, so it stays Loki's. The proxy's limit is set by
// -max-stats-query-series, which the log line names.
type seriesLimitError struct{ limit int }

func (e *seriesLimitError) Error() string {
	return fmt.Sprintf("%s%d) reached for a single query; consider reducing query cardinality by adding more specific stream selectors, reducing the time range, or aggregating results with functions like sum(), count() or topk()", seriesLimitMessagePrefix, e.limit)
}

func isSeriesLimitError(err error) bool {
	var limitErr *seriesLimitError
	return errors.As(err, &limitErr)
}

// seriesLimitScope records how a request answers a metric result over the series
// limit. Loki gives Grafana Logs Drilldown the first series with a warning
// (pkg/logql/engine.go JoinSampleVector) and every other client an error.
type seriesLimitScope struct {
	drilldown bool
	truncated atomic.Int64 // the limit that cut a Drilldown result; 0 = none
}

type seriesLimitScopeKey struct{}

// withSeriesLimitScope attaches a series limit scope for r to its context.
func withSeriesLimitScope(r *http.Request) (*http.Request, *seriesLimitScope) {
	scope := &seriesLimitScope{drilldown: isGrafanaDrilldownRequest(r)}
	return r.WithContext(context.WithValue(r.Context(), seriesLimitScopeKey{}, scope)), scope
}

// seriesLimitReached is called when a metric result holds more than limit
// series. It returns Loki's error, or nil when the request keeps a partial
// result (Drilldown); the caller then keeps limit series.
func seriesLimitReached(ctx context.Context, limit int) error {
	if scope, _ := ctx.Value(seriesLimitScopeKey{}).(*seriesLimitScope); scope != nil && scope.drilldown {
		scope.truncated.Store(int64(limit))
		return nil
	}
	return &seriesLimitError{limit: limit}
}

// logQLProducesSamples reports whether logql evaluates to samples (a matrix or
// vector) rather than log lines. Only those responses carry sample timestamps to
// shift or series to limit; a log query must stream straight through.
func logQLProducesSamples(logql string) bool {
	parsed, err := logqlpkg.Parse(logql)
	if err != nil {
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
}

func (m *metricResponseRewriter) WriteHeader(code int) { m.status = code }

func (m *metricResponseRewriter) Write(b []byte) (int, error) { return m.buf.Write(b) }

func (m *metricResponseRewriter) finish() {
	body := m.buf.Bytes()
	if m.status == 0 || m.status == http.StatusOK {
		body = shiftLokiResultTimestamps(body, m.shift)
		body = addSeriesLimitWarning(body, m.scope)
	}
	if m.status != 0 {
		m.ResponseWriter.WriteHeader(m.status)
	}
	_, _ = m.ResponseWriter.Write(body)
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
			ns := int64(math.Round(ts*1e9)) + int64(shift)
			point[0] = json.RawMessage(strconv.FormatFloat(float64(ns)/1e9, 'f', -1, 64))
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
	return limitLokiResultSeries(body, limit), nil
}

// capSeriesForRequest is capSeriesToLimit for a result still held as series:
// Loki's error for a plain client, the busiest limit series with Loki's
// warning for Logs Drilldown.
func capSeriesForRequest(ctx context.Context, series map[string]manualSeriesSamples, limit int) (map[string]manualSeriesSamples, error) {
	if limit <= 0 || len(series) <= limit {
		return series, nil
	}
	if err := seriesLimitReached(ctx, limit); err != nil {
		return nil, err
	}
	return capSeriesByTotalCount(series, limit), nil
}

// seriesLimitCollecting reports whether a collector that has seen n series
// must stop with Loki's error: a plain client fails as soon as the limit is
// passed, while a Drilldown request collects every series and keeps the
// busiest at the end (capSeriesForRequest).
func seriesLimitCollecting(ctx context.Context, n, limit int) error {
	if limit <= 0 || n < limit || requestKeepsPartialSeries(ctx) {
		return nil
	}
	return &seriesLimitError{limit: limit}
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
	if limited.N <= 0 {
		return fmt.Errorf("manual metric response exceeds %d bytes; narrow the query or increase -backend-max-buffered-response-bytes", executionLimitsFrom(ctx).BufferedBackendBodyBytes)
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
				out.points = append(out.points, []any{float64(evaluation) / float64(time.Second), strconv.FormatFloat(value, 'f', -1, 64)})
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
		status = statusFromUpstreamErr(err)
		p.writeError(w, status, err.Error())
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
