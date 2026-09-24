package proxy

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	fj "github.com/valyala/fastjson"

	logqlpkg "github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
)

// --- Stats query proxying ---

func (p *Proxy) proxyStatsQueryRange(w http.ResponseWriter, r *http.Request, logsqlQuery string) {
	// Suppress the Grafana querySplitting RESIDUAL chunk (see isQuerySplitResidual).
	// proxyStatsQueryRange is the entry for every drilldown high-cardinality metric
	// query — pod LABEL and *_id FIELD alike — so this single guard covers them all.
	// A sub-step (range < step) by() residual yields a single-bucket, multi-series
	// frame that Grafana's mergeFrames collapses onto ONE edge of the merged chart
	// (a right-edge spike, or a left-edge "all data at the beginning" cluster). Axis
	// trimming can't fix it (the bucket is legitimately within [start,end]); only
	// suppression does. Safe here because this entry only serves metric (matrix)
	// stats queries, so a log query is never blanked.
	if isQuerySplitResidual(r) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Proxy-Drilldown-Path", "hits-leftover-suppressed")
		_, _ = w.Write(emptyLokiMatrix)
		return
	}

	originalLogql := resolveGrafanaRangeTemplateTokens(r.FormValue("query"), r.FormValue("start"), r.FormValue("end"), r.FormValue("step"))

	topK, topKDesc, hasTopK := parseTopKWrapper(originalLogql)

	out := http.ResponseWriter(w)
	var topKBuf *bufferedResponseWriter
	if hasTopK {
		topKBuf = &bufferedResponseWriter{}
		out = topKBuf
	}

	if p.handleStatsCompatRange(out, r, originalLogql, logsqlQuery) {
		if hasTopK {
			writeTopKFiltered(w, topKBuf, topK, topKDesc, "matrix")
		}
		return
	}

	// Strip | delete __error__, __error_details__ for any stats query — it removes
	// a field per row but never filters logs; counts are identical without it.
	effectiveQuery := logsqlQuery
	if spec, ok := parseStatsCompatSpec(logsqlQuery); ok {
		noDelete := strings.TrimSpace(drilldownDeletePipeRE.ReplaceAllString(spec.BaseQuery, ""))
		if noDelete != spec.BaseQuery {
			effectiveQuery = noDelete + logsqlQuery[len(spec.BaseQuery):]
		}
	}

	// Range == step: Loki's sample at T covers (T-W, T] while VL's bucket
	// labelled T covers [T, T+W). Fetch from start-W on a grid whose buckets are
	// (edge, edge+W] and relabel every bucket to its Loki evaluation timestamp
	// (relabelSnappedTumblingStatsQueryRange). A backend known to be without the
	// offset arg has only epoch-aligned buckets, which match no window of an
	// unaligned start; tumblingBucketsAligned sends those requests to the window
	// evaluator instead.
	if origSpec, origStartNs, ok := statsRateRangeEqualsStepShift(originalLogql, r); ok && p.tumblingBucketsAligned(r, origSpec.Window) {
		buf := &bufferedResponseWriter{}
		shiftedR := r.Clone(r.Context())
		_ = shiftedR.ParseForm()
		shiftedR.Form.Set("start", strconv.FormatInt(origStartNs-origSpec.Window.Nanoseconds(), 10))
		// Lines after Loki's last evaluation timestamp belong to no sample.
		if endNs, ok := parseLokiTimeToUnixNano(r.FormValue("end")); ok && endNs > origStartNs {
			window := origSpec.Window.Nanoseconds()
			shiftedR.Form.Set("end", strconv.FormatInt(origStartNs+(endNs-origStartNs)/window*window, 10))
		}
		p.proxyStatsQueryRangeDirectAnchored(buf, shiftedR, effectiveQuery, origSpec.Window)
		code := buf.code
		if code == 0 {
			code = http.StatusOK
		}
		body := buf.body
		if code == http.StatusOK {
			endNs, _ := parseLokiTimeToUnixNano(r.FormValue("end"))
			body = relabelSnappedTumblingStatsQueryRange(body, origStartNs, endNs, origSpec.Window.Nanoseconds())
			if hasTopK {
				body = applyTopKToMatrix(body, topK, topKDesc)
			}
		}
		// Keep the headers the stats path set, such as the partial-result
		// Warning and X-Proxy-Upstream-* of a Grafana-sourced backend failure.
		for k, v := range buf.header {
			w.Header()[k] = v
		}
		w.Header().Set("Content-Type", "application/json")
		if code != http.StatusOK {
			w.WriteHeader(code)
		}
		_, _ = w.Write(body)
		return
	}

	// Logs Drilldown label and field breakdowns take the same exact path as any
	// other client, as they do in Loki: every series up to the tenant's
	// max_query_series from VictoriaLogs buckets on the request grid.
	p.proxyStatsQueryRangeDirect(out, r, effectiveQuery)

	if hasTopK {
		writeTopKFiltered(w, topKBuf, topK, topKDesc, "matrix")
	}
}

// emptyLokiMatrix is an empty Loki matrix response.
var emptyLokiMatrix = []byte(`{"status":"success","data":{"resultType":"matrix","result":[]}}`)

// proxyStatsQueryRangeDirect issues the VL stats_query_range request directly,
// bypassing the compat layer and the rate-shift gate. Call this when the caller
// has already applied any necessary start shift (e.g. the range == step relabel in proxyStatsQueryRange).
func (p *Proxy) proxyStatsQueryRangeDirect(w http.ResponseWriter, r *http.Request, logsqlQuery string) {
	p.proxyStatsQueryRangeDirectAnchored(w, r, logsqlQuery, 0)
}

// proxyStatsQueryRangeDirectAnchored is proxyStatsQueryRangeDirect with an
// optional tumbling window. When tumblingWindow > 0 and the backend honours the
// offset arg, buckets of that width are anchored at the request start with
// Loki's (edge, edge+window] boundaries: the start is exclusive and the end
// inclusive.
//
// The answer follows Loki's series limit (the tenant's max_query_series). A
// client other than Logs Drilldown gets Loki's 400 as soon as the response
// holds more series than the limit. Logs Drilldown gets the busiest series
// with Loki's partial-result warning: a single-field breakdown over the limit
// is asked again with VictoriaLogs ranking the values (rankedSingleFieldQuery),
// so the second response holds at most limit+1 series; any other shape is read
// whole and capped.
func (p *Proxy) proxyStatsQueryRangeDirectAnchored(w http.ResponseWriter, r *http.Request, logsqlQuery string, tumblingWindow time.Duration) {
	// For the underscore proxy, expand dotted by() labels (e.g. "service.name") to
	// also include their underscore equivalents (e.g. "service_name"). Loki-push data
	// stores these as stream labels under the underscore name; OTel data uses the dotted
	// field. VL groups by whichever exists; the response translation coalesces the two
	// fields into a single Loki label, preferring the non-empty value.
	origGroupBy := parseOriginalByLabels(r.FormValue("query"))
	logsqlQuery = p.addUnderscorefallbackByLabels(logsqlQuery, origGroupBy)

	ctx := r.Context()
	limit := p.resolvedMaxStatsQuerySeries(ctx)
	drilldown := isGrafanaDrilldownRequest(r)
	ranked, rankable := "", false
	overLimitKey := ""
	maxSeries := limit
	if drilldown {
		ranked, rankable = rankedSingleFieldQuery(logsqlQuery, limit)
		if rankable {
			// A breakdown seen over the limit is ranked straight away for a while.
			// The ranked query returns the same answer under the limit, so a stale
			// entry costs only the ranking.
			overLimitKey = "drilldown-over-limit:" + getOrgID(ctx) + ":" + strconv.Itoa(limit) + ":" + logsqlQuery
			if p.cache != nil {
				if _, seen := p.cache.Get(overLimitKey); seen {
					// At most limit+1 series: the limit+1st says the limit was passed.
					logsqlQuery, rankable, maxSeries = ranked, false, limit+1
				}
			}
		}
		// Logs Drilldown fires one breakdown per label or field at once; they share
		// the stats_query_range slots (-stats-query-range-concurrency), and a slot
		// is free again only after the pause (-stats-query-range-inter-query-delay-ms),
		// which does not hold back this response.
		if sem := p.statsQueryRangeSem; sem != nil {
			select {
			case <-sem:
				delay := p.statsQueryRangeInterQueryDelay
				defer func() {
					if delay <= 0 {
						sem <- struct{}{}
						return
					}
					time.AfterFunc(delay, func() { sem <- struct{}{} })
				}()
			case <-ctx.Done():
				p.writeError(w, http.StatusServiceUnavailable, "request cancelled waiting for stats_query_range slot")
				return
			}
		}
	}

	body, err := p.fetchStatsQueryRangeBody(r, logsqlQuery, tumblingWindow, maxSeries)
	if errors.Is(err, errSeriesCountExceeded) && drilldown && rankable {
		if p.cache != nil {
			p.setLocalReadCacheWithTTL(overLimitKey, []byte("1"), drilldownOverLimitTTL)
		}
		// At most limit+1 series: the limit+1st says the limit was passed.
		body, err = p.fetchStatsQueryRangeBody(r, ranked, tumblingWindow, limit+1)
		logsqlQuery = ranked
	}
	if errors.Is(err, errSeriesCountExceeded) && drilldown {
		// Not rankable (or the ranking returned more than it should): read the
		// whole response and keep the busiest series below.
		body, err = p.fetchStatsQueryRangeBody(r, logsqlQuery, tumblingWindow, 0)
	}
	var upstream *statsUpstreamError
	switch {
	case err == nil:
	case errors.As(err, &upstream):
		// Grafana-sourced traffic gets the same partial-results carve-out
		// Loki applies (IsLogsDrilldownRequest in pkg/querier/queryrange/limits.go).
		// Non-Grafana clients (curl, scripts, /ready probes) still see the
		// upstream status so they can react meaningfully.
		if isGrafanaSourcedRequest(r) {
			if upstream.status > 0 {
				p.writeGrafanaStatsFailure(w, p.redactedBackendStatusError("", upstream.status, upstream.body))
			} else {
				p.writeGrafanaStatsFailure(w, upstream.err)
			}
			return
		}
		if upstream.status > 0 {
			p.writeBackendError(w, upstream.status, upstream.body)
		} else {
			p.writeError(w, statusFromUpstreamErr(upstream.err), upstream.err.Error())
		}
		return
	case errors.Is(err, errSeriesCountExceeded):
		p.writeError(w, http.StatusBadRequest, (&seriesLimitError{limit: limit}).Error())
		return
	case errors.Is(err, errBodyTooLarge):
		p.writeError(w, http.StatusBadGateway, fmt.Sprintf("manual metric response exceeds %d bytes; narrow the query or increase -backend-max-buffered-response-bytes", p.limits().BufferedBackendBodyBytes))
		return
	default:
		p.writeError(w, statusFromUpstreamErr(err), err.Error())
		return
	}

	// Single parse pass: filter points to the requested end time AND translate
	// metric labels. Replaces two sequential fastjson parses (trim then translate).
	var keepFn func(int64) bool
	if endNs, ok := parseLokiTimeToUnixNano(r.FormValue("end")); ok {
		keepFn = func(tsNs int64) bool { return tsNs <= endNs }
	}
	body = p.trimAndTranslateStatsQRFJ(ctx, body, keepFn, r.FormValue("query"))
	// Over the limit, Logs Drilldown keeps the busiest series (by total count,
	// not VictoriaLogs' label order, which would keep the count==1 noise floor
	// of a churning field) with Loki's warning; every other client has already
	// stopped reading with Loki's error.
	out := wrapAsLokiResponse(body, "matrix")
	if lokiResultSeriesCount(out) > limit {
		if err := seriesLimitReached(ctx, limit); err != nil {
			p.writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		out = limitLokiResultSeries(out, limit)
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(out)
}

// drilldownOverLimitTTL is how long a Logs Drilldown breakdown seen over the
// series limit is ranked by VictoriaLogs straight away.
const drilldownOverLimitTTL = 5 * time.Minute

// statsUpstreamError is a stats_query_range call VictoriaLogs failed: a
// transport error (status 0) or an error status with its body.
type statsUpstreamError struct {
	status int
	body   []byte
	err    error
}

func (e *statsUpstreamError) Error() string {
	if e.err != nil {
		return e.err.Error()
	}
	return fmt.Sprintf("stats_query_range backend status %d", e.status)
}

// fetchStatsQueryRangeBody runs one stats_query_range call for the request's
// range and reads its body, bounded by -backend-max-buffered-response-bytes
// (errBodyTooLarge) and, when maxSeries > 0, by that many series
// (errSeriesCountExceeded).
func (p *Proxy) fetchStatsQueryRangeBody(r *http.Request, logsqlQuery string, tumblingWindow time.Duration, maxSeries int) ([]byte, error) {
	// Keep metric query_range as a single backend request. Window splitting and
	// window-level cache reuse are for raw log queries only.
	params := buildStatsQueryRangeParams(logsqlQuery, r.FormValue("start"), r.FormValue("end"), r.FormValue("step"))
	if tumblingWindow > 0 && p.supportsStatsRangeOffset() {
		startNs, startOK := parseLokiTimeToUnixNano(r.FormValue("start"))
		endNs, endOK := parseLokiTimeToUnixNano(r.FormValue("end"))
		if startOK && endOK {
			p.setSlidingStatsRangeParams(params, time.Unix(0, startNs), time.Unix(0, endNs), tumblingWindow)
			// The first window is (start, start+window]: a line exactly at start
			// belongs to the window before, which no sample covers.
			params.Set("start", time.Unix(0, startNs+1).UTC().Format(time.RFC3339Nano))
		}
	}

	// Use vlPost directly (not coalesced) so readBodyLimited can bound the response
	// before the full body is allocated. The coalescer's 256 MB cap is too generous
	// when many concurrent field queries each produce a large stats response.
	resp, err := p.vlPost(r.Context(), "/select/logsql/stats_query_range", params)
	if err != nil {
		return nil, &statsUpstreamError{err: err}
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		errBody, _ := readBodyLimited(resp.Body, maxUpstreamErrorBodyBytes)
		return nil, &statsUpstreamError{status: resp.StatusCode, body: errBody}
	}
	counter := &seriesCountingReader{r: resp.Body, limit: maxSeries}
	return readBodyLimited(counter, int64(p.limits().BufferedBackendBodyBytes))
}

// rankedSingleFieldQuery restricts a single-field grouped count to the limit+1
// field values with the most lines in the request's time range, ranked by
// VictoriaLogs in an in() subquery (which inherits the start and end
// arguments). The response then holds every series when there are at most
// limit of them and limit+1 otherwise, whatever the field's cardinality:
// VictoriaLogs still reads the lines twice (the ranking and the buckets), but
// the response, and what the proxy buffers, stay bounded.
func rankedSingleFieldQuery(logsqlQuery string, limit int) (string, bool) {
	spec, ok := parseSingleFieldCountSpec(logsqlQuery)
	if !ok || limit <= 0 || spec.GroupBy[0] == "_stream" {
		return "", false
	}
	field := spec.GroupBy[0] // as the translator wrote it, quoted where needed
	ranking := spec.BaseQuery + " | stats by (" + field + ") count() as __lvp_rank | sort by (__lvp_rank desc, " + field + ") | limit " + strconv.Itoa(limit+1) + " | fields " + field
	return spec.BaseQuery + " | filter " + field + ":in(" + ranking + ")" + logsqlQuery[len(spec.BaseQuery):], true
}

// lokiResultSeriesCount returns the number of series in a Loki JSON response.
func lokiResultSeriesCount(body []byte) int {
	v, err := fj.ParseBytes(body)
	if err != nil {
		return 0
	}
	return len(v.GetArray("data", "result"))
}

// fitTopValueFilter returns the in() filter for the busiest values (ranked
// order) that keeps prefix+filter+suffix within VictoriaLogs' default query
// length, and the values it holds. It never returns an empty in().
func fitTopValueFilter(field string, values []string, prefix, suffix string) (string, []string, error) {
	budget := vlDefaultMaxQueryLen - len(prefix) - len(suffix)
	filter := buildVLInFilter(field, values)
	for len(filter) > budget && len(values) > 1 {
		values = values[:len(values)*budget/len(filter)]
		filter = buildVLInFilter(field, values)
	}
	if len(values) == 0 || len(filter) > budget {
		return "", nil, fmt.Errorf("top-value filter does not fit VictoriaLogs' query length (%d bytes)", vlDefaultMaxQueryLen)
	}
	return filter, values, nil
}

// isGrafanaDrilldownRequest reports whether r originates from Grafana Logs Drilldown.
// Grafana Logs Drilldown sets supportingQueryType="grafana-lokiexplore-app" on every
// Loki data query; the datasource Go backend translates this to the HTTP header
// X-Query-Tags: Source=grafana-lokiexplore-app. As in Loki (IsLogsDrilldownRequest),
// this header turns the series limit error into a partial result with a warning.
func isGrafanaDrilldownRequest(r *http.Request) bool {
	tag := strings.ToLower(parseGrafanaSourceTag(r.Header.Values("X-Query-Tags")))
	return strings.Contains(tag, "lokiexplore") || strings.Contains(tag, "drilldown")
}

// isGrafanaSourcedRequest reports whether r comes from any Grafana UI client
// (Explore, Drilldown, dashboard panels). Used to gate fixes that target
// Grafana's querySplitting behavior — applies to ANY Grafana client because
// querySplitting fires unconditionally for metric range queries in the Loki
// datasource, regardless of which Grafana app issued them.
//
// Detection signals (any one is sufficient):
//   - X-Query-Tags carries a `Source=grafana-…` tag (Drilldown / Explore set this).
//   - User-Agent starts with "Grafana/" (every Grafana backend HTTP client).
//   - X-Grafana-* header present (org id, user id, request id — sent by the
//     Grafana backend on dashboard/explore queries).
//
// Returning a false negative is acceptable (a raw curl/scripts query is served
// normally).
func isGrafanaSourcedRequest(r *http.Request) bool {
	if isGrafanaDrilldownRequest(r) {
		return true
	}
	if ua := r.Header.Get("User-Agent"); strings.HasPrefix(ua, "Grafana/") {
		return true
	}
	for k := range r.Header {
		if strings.HasPrefix(strings.ToLower(k), "x-grafana-") {
			return true
		}
	}
	return false
}

// isQuerySplitResidual reports whether r is the tiny trailing RESIDUAL chunk of
// Grafana's 24h+ querySplitting: a Grafana-sourced request whose span is shorter
// than one step. For a metric (matrix) query such a chunk yields a single-bucket,
// multi-series response that Grafana's mergeFrames/closestIdx collapses onto ONE
// edge of the merged chart — a right-edge spike, or (when the residual is the
// oldest chunk) a left-edge "all data groups at the beginning" cluster. It must be
// suppressed on every metric path; the caller is responsible for confirming the
// query is a metric (matrix) query so a log query is never blanked. A real metric
// query never spans < one step except as this residual — Grafana aligns standalone
// ranges to the step. See [[grafana-loki-querysplitting-24h]].
func isQuerySplitResidual(r *http.Request) bool {
	// Read start/end/step from the URL query directly. r.FormValue depends on
	// r.Form, which upstream handlers may have parsed before r.URL was rewritten
	// for downstream VL calls — leaving an empty cached r.Form that makes the
	// residual check silently no-op. r.URL.Query() reparses RawQuery deterministically;
	// fall back to r.FormValue only if a value is missing there (e.g. POST form).
	q := r.URL.Query()
	get := func(k string) string {
		if v := q.Get(k); v != "" {
			return v
		}
		return r.FormValue(k)
	}
	return isQuerySplitResidualParams(r, get("start"), get("end"), get("step"))
}

// isQuerySplitResidualParams is isQuerySplitResidual with explicit start/end/step.
// Deep handlers must pass their captured raw values: by the time
// a request reaches them the proxy may have rewritten r.URL for downstream VL
// calls, so r.FormValue("start"/"end"/"step") can be empty. Only the Drilldown-source
// check reads r (it uses headers, which persist).
func isQuerySplitResidualParams(r *http.Request, startRaw, endRaw, stepRaw string) bool {
	if !isGrafanaDrilldownRequest(r) {
		return false
	}
	sNs, sok := parseLokiTimeToUnixNano(startRaw)
	eNs, eok := parseLokiTimeToUnixNano(endRaw)
	if !sok || !eok || eNs <= sNs {
		return false
	}
	stepD, stepOK := parsePositiveStepDuration(stepRaw)
	if !stepOK || stepD <= 0 {
		return false
	}
	return eNs-sNs < stepD.Nanoseconds()
}

// renameStatsBodyMetricKey renames a JSON metric key in a stats_query_range
// response body, where the VL field name (e.g. "level") differs from the Loki
// label name (e.g. "detected_level").
func renameStatsBodyMetricKey(body []byte, from, to string) []byte {
	if from == to || len(body) == 0 {
		return body
	}
	return bytes.ReplaceAll(body, []byte(`"`+from+`":`), []byte(`"`+to+`":`))
}

// drilldownTopValuesFromMatrix extracts the label values for field from a Loki
// matrix JSON response (one bucket: the values ranked by line count).
func drilldownTopValuesFromMatrix(body []byte, field string) []string {
	v, err := fj.ParseBytes(body)
	if err != nil {
		return nil
	}
	result := v.GetArray("data", "result")
	if len(result) == 0 {
		return nil
	}
	out := make([]string, 0, len(result))
	for _, entry := range result {
		val := string(entry.GetStringBytes("metric", field))
		if val != "" {
			out = append(out, val)
		}
	}
	return out
}

// drilldownTopValuesHaveEmpty reports whether a Phase 1 matrix ranks the group
// of rows without the field (an empty value).
func drilldownTopValuesHaveEmpty(body []byte, field string) bool {
	v, err := fj.ParseBytes(body)
	if err != nil {
		return false
	}
	for _, entry := range v.GetArray("data", "result") {
		if len(entry.GetStringBytes("metric", field)) == 0 {
			return true
		}
	}
	return false
}

// buildVLInFilter builds a LogsQL field:in("v1","v2",...) existence filter.
// VL evaluates in() filters against pre-indexed columns without a parser stage,
// making them fast even for high-cardinality fields.
func buildVLInFilter(field string, values []string) string {
	var sb strings.Builder
	sb.WriteString(quoteLogsQLIdent(field))
	sb.WriteString(`:in(`)
	for i, v := range values {
		if i > 0 {
			sb.WriteByte(',')
		}
		sb.WriteByte('"')
		sb.WriteString(strings.ReplaceAll(v, `"`, `\"`))
		sb.WriteByte('"')
	}
	sb.WriteByte(')')
	return sb.String()
}

// limitLokiResultSeries truncates the result array of a Loki matrix or vector
// response to the top maxSeries entries by total sample value. Loki keeps the
// first series it encounters; keeping the busiest ones is what a Drilldown panel
// renders, and it stops an alphabetical VictoriaLogs ordering from returning the
// noise floor when tens of thousands of values exist.
// Returns body unchanged if parsing fails or len(result) <= maxSeries.
func limitLokiResultSeries(body []byte, maxSeries int) []byte {
	if maxSeries <= 0 {
		return body
	}
	v, err := fj.ParseBytes(body)
	if err != nil {
		return body
	}
	result := v.GetArray("data", "result")
	if len(result) <= maxSeries {
		return body
	}
	resultType := string(v.GetStringBytes("data", "resultType"))
	if resultType == "" {
		resultType = "matrix"
	}

	// Compute total count per series and sort descending so the most active
	// field values survive the maxSeries cut, not an arbitrary VL ordering.
	type ranked struct {
		idx   int
		total float64
		key   string
	}
	sampleValue := func(pair *fj.Value) float64 {
		arr := pair.GetArray()
		if len(arr) < 2 {
			return 0
		}
		f, e := strconv.ParseFloat(string(arr[1].GetStringBytes()), 64)
		if e != nil {
			return 0
		}
		return f
	}
	ranks := make([]ranked, len(result))
	for i, entry := range result {
		var total float64
		for _, pair := range entry.GetArray("values") {
			total += sampleValue(pair)
		}
		if value := entry.Get("value"); value != nil { // instant vector sample
			total += sampleValue(value)
		}
		ranks[i] = ranked{idx: i, total: total}
	}
	// Ties break on the labels, so equal-volume series (a field whose values
	// appear once each) are kept the same way on every request.
	for i := range ranks {
		if m := result[ranks[i].idx].Get("metric"); m != nil {
			ranks[i].key = string(m.MarshalTo(nil))
		}
	}
	sort.Slice(ranks, func(i, j int) bool {
		if ranks[i].total != ranks[j].total {
			return ranks[i].total > ranks[j].total
		}
		return ranks[i].key < ranks[j].key
	})

	buf := make([]byte, 0, maxSeries*256)
	buf = append(buf, `{"status":"success","data":{"resultType":"`...)
	buf = append(buf, resultType...)
	buf = append(buf, `","result":[`...)
	for i := 0; i < maxSeries; i++ {
		if i > 0 {
			buf = append(buf, ',')
		}
		buf = result[ranks[i].idx].MarshalTo(buf)
	}
	buf = append(buf, `]}}`...)
	return buf
}

// addUnderscorefallbackByLabels augments every by() clause of a translated LogsQL
// stats query with the underscore fallback of each dotted OTel field it groups by
// (e.g. service.name gains service_name). Loki-push data stores these as stream
// labels under the underscore name; OTel data uses the dotted field. VL groups by
// whichever exists; translateStatsResponseLabelsWithContext then coalesces the two
// fields into a single Loki label by preferring the non-empty value. Every clause
// is augmented, so multi-stage pipelines (rate's inner count and outer sum) keep
// both fields through to the final stats pipe. origGroupBy, when given, limits
// the fallbacks to labels the Loki query grouped by.
func (p *Proxy) addUnderscorefallbackByLabels(logsqlQuery string, origGroupBy []string) string {
	if p.labelTranslator == nil || p.labelTranslator.IsPassthrough() ||
		p.labelTranslator.style != LabelStyleUnderscores {
		return logsqlQuery
	}
	const marker = "| stats by ("
	var b strings.Builder
	rest := logsqlQuery
	changed := false
	for {
		idx := strings.Index(rest, marker)
		if idx < 0 {
			break
		}
		open := idx + len(marker)
		closeIdx := strings.Index(rest[open:], ")")
		if closeIdx < 0 {
			break
		}
		list := rest[open : open+closeIdx]
		present := map[string]bool{}
		for _, item := range strings.Split(list, ",") {
			present[strings.Trim(strings.TrimSpace(item), "\"`")] = true
		}
		var extras []string
		for _, item := range strings.Split(list, ",") {
			vlField := strings.Trim(strings.TrimSpace(item), "\"`")
			if !strings.Contains(vlField, ".") {
				continue
			}
			lokiLabel := p.labelTranslator.ToLoki(vlField)
			if lokiLabel == vlField || strings.Contains(lokiLabel, ".") || present[lokiLabel] ||
				p.labelTranslator.ToVL(lokiLabel) != vlField {
				continue
			}
			if len(origGroupBy) > 0 && !containsString(origGroupBy, lokiLabel) {
				continue
			}
			present[lokiLabel] = true
			extras = append(extras, lokiLabel)
		}
		b.WriteString(rest[:open+closeIdx])
		if len(extras) > 0 {
			b.WriteString(", " + strings.Join(extras, ", "))
			changed = true
		}
		rest = rest[open+closeIdx:]
	}
	if !changed {
		return logsqlQuery
	}
	b.WriteString(rest)
	return b.String()
}

// allRangeWindowsEqual returns (window, true) when every range vector in logql
// uses the same window duration. A query like rate({a}[1m]) / rate({b}[5m])
// returns (0, false) because the windows differ. Used to guard binary-expression
// shift logic against applying a single shift to operands with different windows.
func allRangeWindowsEqual(logql string) (time.Duration, bool) {
	var common time.Duration
	inBracket := false
	start := 0
	for i, ch := range logql {
		switch ch {
		case '[':
			inBracket = true
			start = i + 1
		case ']':
			if inBracket {
				inBracket = false
				d := parseLokiDuration(strings.TrimSpace(logql[start:i]))
				if d <= 0 {
					continue
				}
				if common == 0 {
					common = d
				} else if d != common {
					return 0, false
				}
			}
		}
	}
	return common, common > 0
}

// statsRateRangeEqualsStepShift detects whether every range aggregation of the
// query is a log range function (rate, bytes_rate, count_over_time,
// bytes_over_time) with range==step so that the caller can fetch from start-W
// and relabel buckets onto Loki's evaluation timestamps. The parsed expression
// is walked, so outer aggregations like sum by(x)(count_over_time(...)) are
// detected, grouped or not, and function names inside string literals are not.
// Logs Drilldown requests are relabelled like any other: Loki answers them on
// its evaluation timestamps.
// NOTE: binary metric expressions are evaluated per operand through the normal
// handlers; the legacy proxyBinaryMetric paths apply the shift independently.
// Returns (spec, origStartNs, true) when shifting is needed.
func statsRateRangeEqualsStepShift(originalLogql string, r *http.Request) (origSpec originalRangeMetricSpec, origStartNs int64, ok bool) {
	spec, hasSpec := parseOriginalRangeMetricSpec(originalLogql)
	if !hasSpec || spec.Window <= 0 {
		return
	}
	stripped := stripOuterLabelReplace(originalLogql)
	expr, err := logqlpkg.Parse(stripped)
	if err != nil {
		return
	}
	found, _, logRangeOnly := logRangeFunctions(expr)
	if !found || !logRangeOnly {
		return
	}
	step, stepOk := parsePositiveStepDuration(r.FormValue("step"))
	if !stepOk || spec.Window != step {
		return
	}
	startNs, hasStart := parseLokiTimeToUnixNano(r.FormValue("start"))
	if !hasStart {
		return
	}
	return spec, startNs, true
}

// logRangeFunctions walks a metric expression. found reports at least one range
// aggregation, hasRate a rate or bytes_rate, and logRangeOnly that every range
// aggregation is rate, bytes_rate, count_over_time or bytes_over_time over a
// log query and every other node is an aggregation, binary operation or
// literal.
func logRangeFunctions(expr logqlpkg.Expr) (found, hasRate, logRangeOnly bool) {
	switch e := expr.(type) {
	case *logqlpkg.RangeAggregation:
		if _, isLog := e.Inner.(*logqlpkg.LogQuery); !isLog {
			return true, false, false
		}
		switch e.Op {
		case logqlpkg.RangeRate, logqlpkg.RangeBytesRate:
			return true, true, true
		case logqlpkg.RangeCountOverTime, logqlpkg.RangeBytesOverTime:
			return true, false, true
		}
		return true, false, false
	case *logqlpkg.VectorAggregation:
		return logRangeFunctions(e.Inner)
	case *logqlpkg.BinOpExpr:
		lf, lr, lo := logRangeFunctions(e.Left)
		rf, rr, ro := logRangeFunctions(e.Right)
		return lf || rf, lr || rr, lo && ro
	case *logqlpkg.LiteralExpr:
		return false, false, true
	}
	return false, false, false
}

// statsQRFJPool pools fastjson.Parser instances for trimStatsQueryRange* hot paths.
var statsQRFJPool fj.ParserPool

func buildStatsQueryRangeParams(logsqlQuery, startRaw, endRaw, stepRaw string) url.Values {
	return buildStatsQueryRangeParamsShifted(logsqlQuery, startRaw, endRaw, stepRaw, 0)
}

// buildStatsQueryRangeParamsShifted builds VL stats params, optionally shifting
// start back by shiftStart nanoseconds. Used by bare-parser metric fast path to
// include the pre-start bucket required by Loki's first rate() evaluation point.
func buildStatsQueryRangeParamsShifted(logsqlQuery, startRaw, endRaw, stepRaw string, shiftStart int64) url.Values {
	params := url.Values{}
	params.Set("query", logsqlQuery)
	if s := strings.TrimSpace(startRaw); s != "" {
		if shiftStart > 0 {
			if ns, ok := parseLokiTimeToUnixNano(s); ok {
				params.Set("start", nanosToVLTimestamp(ns-shiftStart))
			} else {
				params.Set("start", formatVLStatsTimestamp(s))
			}
		} else {
			params.Set("start", formatVLStatsTimestamp(s))
		}
	}
	if e := strings.TrimSpace(endRaw); e != "" {
		if extendedEnd, ok := extendStatsQueryRangeEnd(e, stepRaw); ok {
			params.Set("end", extendedEnd)
		} else {
			params.Set("end", formatVLStatsTimestamp(e))
		}
	}
	if step := strings.TrimSpace(stepRaw); step != "" {
		params.Set("step", formatVLStep(step))
	}
	return params
}

func extendStatsQueryRangeEnd(endRaw, stepRaw string) (string, bool) {
	endNs, ok := parseLokiTimeToUnixNano(endRaw)
	if !ok {
		return "", false
	}
	stepDur, ok := parsePositiveStepDuration(stepRaw)
	if !ok || stepDur <= 0 {
		return "", false
	}
	return nanosToVLTimestamp(endNs + stepDur.Nanoseconds()), true
}

// fjMarshalPool pools scratch []byte slices for fastjson MarshalTo calls.
// Reusing a pre-allocated slice avoids per-call allocation when marshaling
// individual JSON values back to bytes (metrics, points, etc.).
var fjMarshalPool = &sync.Pool{New: func() interface{} { b := make([]byte, 0, 4096); return &b }}

// marshalFJ marshals v into scratch (resizing as needed) and writes to buf.
// scratch must come from fjMarshalPool.
func marshalFJ(buf *bytes.Buffer, v *fj.Value, scratch *[]byte) {
	*scratch = v.MarshalTo((*scratch)[:0])
	buf.Write(*scratch)
}

// relabelTumblingStatsQueryRange maps VictoriaLogs stats_query_range buckets
// onto Loki's evaluation timestamps for a range aggregation whose window equals
// the step. VictoriaLogs labels each bucket by its start and the bucket covers
// [T, T+step); Loki's sample at T covers (T-W, T]. The caller fetches from
// start-W, every bucket label moves forward by W, and only points inside
// [startNs, endNs] are kept. That also drops the partial buckets the shifted
// fetch produces at either edge. endNs <= 0 disables the upper bound.
func relabelTumblingStatsQueryRange(body []byte, startNs, endNs, windowNs int64) []byte {
	return relabelStatsQueryRange(body, startNs, endNs, func(tsNs int64) int64 { return tsNs + windowNs })
}

// relabelSnappedTumblingStatsQueryRange is relabelTumblingStatsQueryRange for
// buckets fetched from startNs-windowNs: every label first snaps to the nearest
// edge of that grid. VictoriaLogs reports bucket labels as float seconds, which
// cannot carry an anchored edge exactly (the 1ns edge shift, or a millisecond
// start at nanosecond precision), so an unsnapped label can fall just outside
// [startNs, endNs] and drop the first or last sample.
func relabelSnappedTumblingStatsQueryRange(body []byte, startNs, endNs, windowNs int64) []byte {
	anchor := time.Unix(0, startNs-windowNs)
	return relabelStatsQueryRange(body, startNs, endNs, func(tsNs int64) int64 {
		return snapSlidingBucketNanos(tsNs, anchor, time.Duration(windowNs)) + windowNs
	})
}

// relabelStatsQueryRange maps every point timestamp through relabel and keeps
// only points whose new timestamp lies in [startNs, endNs]. endNs <= 0
// disables the upper bound.
func relabelStatsQueryRange(body []byte, startNs, endNs int64, relabel func(int64) int64) []byte {
	return trimStatsQRByTimeFJShifted(body, func(tsNs int64) bool {
		ts := relabel(tsNs)
		return ts >= startNs && (endNs <= 0 || ts <= endNs)
	}, relabel)
}

// trimStatsQRByTimeFJ filters stats_query_range point arrays using fastjson,
// eliminating json.Unmarshal struct allocations and json.Marshal reflection.
// trimStatsQRByTimeFJShifted filters stats_query_range points and rewrites every
// kept point's timestamp with relabel. keep receives the original timestamp.
func trimStatsQRByTimeFJShifted(body []byte, keep func(int64) bool, relabel func(int64) int64) []byte {
	p := statsQRFJPool.Get()
	defer statsQRFJPool.Put(p)

	v, err := p.ParseBytes(body)
	if err != nil {
		return body
	}

	// Locate result series: top-level "results" or nested "data"."result".
	var seriesArr []*fj.Value
	var dataVal *fj.Value

	if r := v.Get("results"); r != nil && r.Type() == fj.TypeArray {
		seriesArr, _ = r.Array()
	}
	if len(seriesArr) == 0 {
		if d := v.Get("data"); d != nil {
			if r := d.Get("result"); r != nil && r.Type() == fj.TypeArray {
				seriesArr, _ = r.Array()
				dataVal = d
			}
		}
	}
	if len(seriesArr) == 0 {
		return body
	}

	// Quick scan: any point falls outside the keep range?
	needsTrim := false
scanLoop:
	for _, series := range seriesArr {
		valObj := series.Get("values")
		if valObj == nil {
			continue
		}
		points, _ := valObj.Array()
		for _, point := range points {
			pts, _ := point.Array()
			if len(pts) == 0 {
				continue
			}
			if !keep(statsQRFJPointNano(pts[0])) {
				needsTrim = true
				break scanLoop
			}
		}
	}
	if !needsTrim && relabel == nil {
		return body
	}

	// Rebuild the JSON response with filtered values arrays.
	buf := jsonBufPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer jsonBufPool.Put(buf)
	buf.Grow(len(body))

	scratch := fjMarshalPool.Get().(*[]byte)
	defer fjMarshalPool.Put(scratch)

	buf.WriteByte('{')
	needsComma := false

	if status := v.Get("status"); status != nil {
		buf.WriteString(`"status":`)
		marshalFJ(buf, status, scratch)
		needsComma = true
	}

	if dataVal != nil {
		if needsComma {
			buf.WriteByte(',')
		}
		buf.WriteString(`"data":{`)
		if rt := dataVal.Get("resultType"); rt != nil {
			buf.WriteString(`"resultType":`)
			marshalFJ(buf, rt, scratch)
			buf.WriteByte(',')
		}
		buf.WriteString(`"result":`)
		writeFilteredStatsQRSeriesFJ(buf, seriesArr, keep, relabel, scratch)
		if stats := dataVal.Get("stats"); stats != nil {
			buf.WriteString(`,"stats":`)
			marshalFJ(buf, stats, scratch)
		}
		buf.WriteByte('}')
	} else {
		if needsComma {
			buf.WriteByte(',')
		}
		buf.WriteString(`"results":`)
		writeFilteredStatsQRSeriesFJ(buf, seriesArr, keep, relabel, scratch)
	}

	buf.WriteByte('}')

	result := make([]byte, buf.Len())
	copy(result, buf.Bytes())
	return result
}

// statsQRSeriesKeepsPoint reports whether keep accepts any point of values.
func statsQRSeriesKeepsPoint(values *fj.Value, keep func(int64) bool) bool {
	points, _ := values.Array()
	for _, point := range points {
		if pts, _ := point.Array(); len(pts) > 0 && keep(statsQRFJPointNano(pts[0])) {
			return true
		}
	}
	return false
}

func writeFilteredStatsQRSeriesFJ(buf *bytes.Buffer, seriesArr []*fj.Value, keep func(int64) bool, relabel func(int64) int64, scratch *[]byte) {
	buf.WriteByte('[')
	written := 0
	for _, series := range seriesArr {
		// A series with no point left in the range is not part of the answer:
		// Loki never returns a matrix series without samples.
		if values := series.Get("values"); values != nil && !statsQRSeriesKeepsPoint(values, keep) {
			continue
		}
		if written > 0 {
			buf.WriteByte(',')
		}
		written++
		buf.WriteByte('{')
		fieldWritten := false
		if metric := series.Get("metric"); metric != nil {
			buf.WriteString(`"metric":`)
			marshalFJ(buf, metric, scratch)
			fieldWritten = true
		}
		if values := series.Get("values"); values != nil {
			if fieldWritten {
				buf.WriteByte(',')
			}
			buf.WriteString(`"values":[`)
			points, _ := values.Array()
			firstPoint := true
			for _, point := range points {
				pts, _ := point.Array()
				if len(pts) == 0 {
					continue
				}
				tsNs := statsQRFJPointNano(pts[0])
				if !keep(tsNs) {
					continue
				}
				if !firstPoint {
					buf.WriteByte(',')
				}
				firstPoint = false
				if relabel == nil || len(pts) < 2 {
					marshalFJ(buf, point, scratch)
					continue
				}
				buf.WriteByte('[')
				buf.WriteString(strconv.FormatFloat(float64(relabel(tsNs))/float64(time.Second), 'f', -1, 64))
				buf.WriteByte(',')
				marshalFJ(buf, pts[1], scratch)
				buf.WriteByte(']')
			}
			buf.WriteByte(']')
		}
		buf.WriteByte('}')
	}
	buf.WriteByte(']')
}

// trimTranslateResult holds per-item output of trimAndTranslateStatsQRFJ's analysis pass.
type trimTranslateResult struct {
	metric   map[string]string // nil = metric unchanged
	valsTrim bool              // at least one values point filtered by keep
}

// trimAndTranslateStatsQRFJ performs time-window filtering and metric-label
// translation in a single fastjson parse, replacing the two-parse sequence of
// trimStatsQRByTimeFJ followed by translateStatsResponseLabelsWithContext.
//
// keep may be nil (no time filtering). If neither filtering nor label translation
// is needed, the original body is returned unchanged with no allocation.
//
//nolint:gocyclo // combines two existing functions; branching is inherent to the schema variants.
func (p *Proxy) trimAndTranslateStatsQRFJ(ctx context.Context, body []byte, keep func(int64) bool, originalQuery string) []byte {
	start := time.Now()

	parser := statsTranslateFJPool.Get()
	defer statsTranslateFJPool.Put(parser)

	v, err := parser.ParseBytes(body)
	if err != nil {
		return body
	}

	// Locate result series across all three JSON shapes the VL stats endpoints emit:
	//   {"data":{"resultType":"…","result":[…]}}  ← Prometheus-compatible (stats_query_range)
	//   {"result":[…]}                             ← bare result
	//   {"results":[…]}                            ← bare results
	type resultSlot struct {
		items  []*fj.Value
		key    string
		inData bool
	}
	var slots []resultSlot
	var dataVal *fj.Value

	if data := v.Get("data"); data != nil {
		if r := data.Get("result"); r != nil && r.Type() == fj.TypeArray {
			if arr, _ := r.Array(); len(arr) > 0 {
				slots = append(slots, resultSlot{items: arr, key: "result", inData: true})
				dataVal = data
			}
		}
	}
	if r := v.Get("result"); r != nil && r.Type() == fj.TypeArray {
		if arr, _ := r.Array(); len(arr) > 0 {
			slots = append(slots, resultSlot{items: arr, key: "result", inData: false})
		}
	}
	if r := v.Get("results"); r != nil && r.Type() == fj.TypeArray {
		if arr, _ := r.Array(); len(arr) > 0 {
			slots = append(slots, resultSlot{items: arr, key: "results", inData: false})
		}
	}

	if len(slots) == 0 {
		return body
	}

	// Allocate per-item result state.
	slotResults := make([][]trimTranslateResult, len(slots))
	for i, s := range slots {
		slotResults[i] = make([]trimTranslateResult, len(s.items))
	}

	// Workspace maps reused across items (same pattern as translateStatsResponseLabelsWithContext).
	levelGrouping := requestedLevelGrouping(originalQuery)
	translated := make(map[string]string, 8)
	syntheticLabels := make(map[string]string, 8)

	needsRebuild := false
	translatedCount := 0

	for si, slot := range slots {
		for ii, item := range slot.items {
			res := &slotResults[si][ii]

			// Pass 1: check whether any values points fall outside keep.
			if keep != nil {
				if values := item.Get("values"); values != nil {
					pts, _ := values.Array()
					for _, pt := range pts {
						ptArr, _ := pt.Array()
						if len(ptArr) > 0 && !keep(statsQRFJPointNano(ptArr[0])) {
							res.valsTrim = true
							needsRebuild = true
							break
						}
					}
				}
			}

			// Pass 2: compute translated metric labels (identical logic to translateStatsResponseLabelsWithContext).
			metricVal := item.Get("metric")
			if metricVal == nil || metricVal.Type() != fj.TypeObject {
				continue
			}

			for k := range translated {
				delete(translated, k)
			}
			changed := false
			hadStream := false

			metricVal.GetObject().Visit(func(k []byte, vv *fj.Value) {
				key := string(k)
				val := string(vv.GetStringBytes())
				switch key {
				case "__name__":
					changed = true
				case "_stream":
					hadStream = true
					for streamKey, streamValue := range parseStreamLabels(val) {
						lokiKey := streamKey
						if !p.labelTranslator.IsPassthrough() {
							lokiKey = p.labelTranslator.ToLoki(streamKey)
						}
						if streamValue != "" || translated[lokiKey] == "" {
							translated[lokiKey] = streamValue
						}
					}
					changed = true
				default:
					lokiKey := key
					if !p.labelTranslator.IsPassthrough() {
						lokiKey = p.labelTranslator.ToLoki(key)
					}
					if lokiKey != key {
						changed = true
					}
					if val != "" || translated[lokiKey] == "" {
						translated[lokiKey] = val
					}
				}
			})

			for k := range syntheticLabels {
				delete(syntheticLabels, k)
			}
			for k, val := range translated {
				syntheticLabels[k] = val
			}

			serviceSignal := hasServiceSignal(syntheticLabels)
			beforeSyntheticCount := len(syntheticLabels)
			levelGrouping.apply(syntheticLabels, translated, hadStream)
			if hadStream {
				ensureSyntheticServiceName(syntheticLabels)
				if !serviceSignal && strings.TrimSpace(syntheticLabels["service_name"]) == unknownServiceName {
					delete(syntheticLabels, "service_name")
				}
			}
			if dropEmptyLabelValues(syntheticLabels) {
				changed = true
			}
			if len(syntheticLabels) != beforeSyntheticCount {
				changed = true
			}
			for key, value := range syntheticLabels {
				if existing, ok := translated[key]; ok && existing == value {
					continue
				}
				translated[key] = value
				changed = true
			}

			if changed {
				translatedCount++
				needsRebuild = true
				res.metric = cloneStringMap(syntheticLabels)
			}
		}
	}

	if !needsRebuild {
		p.observeInternalOperation(ctx, "trim_translate_stats_qr", "noop", time.Since(start))
		return body
	}

	// Rebuild the JSON response once, applying both filtering and translation.
	// Always emit "status":"success" first so wrapAsLokiResponse fast-path A matches
	// and returns the buffer zero-alloc instead of splicing a new []byte.
	buf := jsonBufPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer jsonBufPool.Put(buf)
	buf.Grow(len(body) + len(`{"status":"success",`))

	scratch := fjMarshalPool.Get().(*[]byte)
	defer fjMarshalPool.Put(scratch)
	// Fix 2: pre-grow scratch to body length so values.MarshalTo doesn't
	// chain-reallocate when serialising a large values array in one shot.
	if cap(*scratch) < len(body) {
		*scratch = make([]byte, 0, len(body))
	}

	buf.WriteString(`{"status":"success"`)
	needsComma := true

	if dataVal != nil {
		si := -1
		for i, s := range slots {
			if s.inData {
				si = i
				break
			}
		}
		buf.WriteString(`,"data":{`)
		if rt := dataVal.Get("resultType"); rt != nil {
			buf.WriteString(`"resultType":`)
			marshalFJ(buf, rt, scratch)
			buf.WriteByte(',')
		}
		buf.WriteString(`"result":`)
		if si >= 0 {
			writeTrimmedTranslatedStatsFJ(buf, slots[si].items, slotResults[si], keep, scratch)
		} else {
			if r := dataVal.Get("result"); r != nil {
				marshalFJ(buf, r, scratch)
			} else {
				buf.WriteString(`[]`)
			}
		}
		if statsF := dataVal.Get("stats"); statsF != nil {
			buf.WriteString(`,"stats":`)
			marshalFJ(buf, statsF, scratch)
		}
		buf.WriteByte('}')
	}

	for si, slot := range slots {
		if slot.inData {
			continue
		}
		if needsComma {
			buf.WriteByte(',')
		}
		buf.WriteByte('"')
		buf.WriteString(slot.key)
		buf.WriteString(`":`)
		writeTrimmedTranslatedStatsFJ(buf, slot.items, slotResults[si], keep, scratch)
		needsComma = true
	}

	if errVal := v.Get("error"); errVal != nil {
		if needsComma {
			buf.WriteByte(',')
		}
		buf.WriteString(`"error":`)
		marshalFJ(buf, errVal, scratch)
	}

	buf.WriteByte('}')

	result := make([]byte, buf.Len())
	copy(result, buf.Bytes())
	p.observeInternalOperation(ctx, "trim_translate_stats_qr", "ok", time.Since(start))
	_ = translatedCount
	return result
}

// writeTrimmedTranslatedStatsFJ writes a JSON array of stats items, applying
// time-window filtering (keep != nil) and metric label translation (res.metric != nil)
// in a single write pass.
func writeTrimmedTranslatedStatsFJ(buf *bytes.Buffer, items []*fj.Value, results []trimTranslateResult, keep func(int64) bool, scratch *[]byte) {
	buf.WriteByte('[')
	written := 0
	for i, item := range items {
		res := results[i]
		// A range series with no point left before the end is not part of the
		// answer: Loki never returns a matrix series without samples.
		if values := item.Get("values"); res.valsTrim && keep != nil && values != nil && !statsQRSeriesKeepsPoint(values, keep) {
			continue
		}
		if written > 0 {
			buf.WriteByte(',')
		}
		written++
		if res.metric != nil || res.valsTrim {
			buf.WriteString(`{"metric":`)
			if res.metric != nil {
				marshalStringMapJSONTo(buf, res.metric)
			} else if m := item.Get("metric"); m != nil {
				marshalFJ(buf, m, scratch)
			} else {
				buf.WriteString(`{}`)
			}
			// Instant value — no time filtering (single point, not an array).
			if val := item.Get("value"); val != nil {
				buf.WriteString(`,"value":`)
				marshalFJ(buf, val, scratch)
			}
			// Range values: when filtering is needed, serialise the whole array
			// once via MarshalTo and scan raw bytes — avoids N fastjson
			// re-serialisations for N points (Fix 3).
			if values := item.Get("values"); values != nil {
				buf.WriteString(`,"values":`)
				if res.valsTrim && keep != nil {
					rawVals := values.MarshalTo((*scratch)[:0])
					*scratch = rawVals
					if !writeFilteredValuesRaw(buf, rawVals, keep) {
						// Malformed values array — fall back to fastjson typed nodes.
						buf.WriteByte('[')
						pts, _ := values.Array()
						first := true
						for _, pt := range pts {
							ptArr, _ := pt.Array()
							if len(ptArr) > 0 && !keep(statsQRFJPointNano(ptArr[0])) {
								continue
							}
							if !first {
								buf.WriteByte(',')
							}
							first = false
							marshalFJ(buf, pt, scratch)
						}
						buf.WriteByte(']')
					}
				} else {
					// No time filtering needed: copy the whole array in one shot.
					marshalFJ(buf, values, scratch)
				}
			}
			buf.WriteByte('}')
		} else {
			marshalFJ(buf, item, scratch)
		}
	}
	buf.WriteByte(']')
}

// writeFilteredValuesRaw scans the raw JSON bytes of a stats_query_range
// values array — [[ts,"val"],[ts,"val"],...] — and writes to buf only those
// points where keep(tsNano) is true.
//
// The raw bytes are obtained from fastjson's MarshalTo in one call per series,
// avoiding N per-point MarshalTo+Write cycles for N values (Fix 3). Returns
// false if the bytes are not a recognisable values array; the caller falls back
// to fastjson typed-node iteration.
//
//nolint:gocyclo // byte-scanner state machine; branching is inherent to the [[ts,"val"],...] format.
func writeFilteredValuesRaw(buf *bytes.Buffer, raw []byte, keep func(int64) bool) bool {
	i := 0
	for i < len(raw) && raw[i] <= ' ' {
		i++
	}
	if i >= len(raw) || raw[i] != '[' {
		return false
	}
	i++

	buf.WriteByte('[')
	first := true

	for {
		// Skip whitespace and commas between elements.
		for i < len(raw) && (raw[i] <= ' ' || raw[i] == ',') {
			i++
		}
		if i >= len(raw) {
			return false
		}
		if raw[i] == ']' {
			break
		}
		if raw[i] != '[' {
			return false // unexpected token in outer array
		}

		pointStart := i
		i++ // consume inner '['

		for i < len(raw) && raw[i] <= ' ' {
			i++
		}

		// Parse the timestamp number (first element of the inner array).
		numStart := i
		if i < len(raw) && (raw[i] == '-' || raw[i] == '+') {
			i++
		}
		digitStart := i
		for i < len(raw) && raw[i] >= '0' && raw[i] <= '9' {
			i++
		}
		if i == digitStart {
			return false // no digits found
		}
		hasDot := false
		if i < len(raw) && raw[i] == '.' {
			hasDot = true
			i++
			for i < len(raw) && raw[i] >= '0' && raw[i] <= '9' {
				i++
			}
		}
		if i < len(raw) && (raw[i] == 'e' || raw[i] == 'E') {
			hasDot = true // treat scientific notation as float
			i++
			if i < len(raw) && (raw[i] == '+' || raw[i] == '-') {
				i++
			}
			for i < len(raw) && raw[i] >= '0' && raw[i] <= '9' {
				i++
			}
		}

		var tsNano int64
		if hasDot {
			f, err := strconv.ParseFloat(string(raw[numStart:i]), 64)
			if err != nil {
				return false
			}
			tsNano = normalizeLokiNumericTimeToUnixNano(f)
		} else {
			tsNano = normalizeLokiIntTimeToUnixNano(rawBytesToInt64(raw[numStart:i]))
		}

		// Advance past the rest of this point to its closing ']', handling
		// nested strings (escaped quotes) and nested arrays.
		depth := 1
		for i < len(raw) && depth > 0 {
			switch raw[i] {
			case '[':
				depth++
				i++
			case ']':
				depth--
				i++
			case '"':
				i++ // skip opening '"'
				for i < len(raw) && raw[i] != '"' {
					if raw[i] == '\\' {
						i++ // skip escaped character
					}
					i++
				}
				if i < len(raw) {
					i++ // skip closing '"'
				}
			default:
				i++
			}
		}

		if keep == nil || keep(tsNano) {
			if !first {
				buf.WriteByte(',')
			}
			first = false
			buf.Write(raw[pointStart:i])
		}
	}

	buf.WriteByte(']')
	return true
}

// rawBytesToInt64 parses a decimal integer from a byte slice without allocating
// a string. The caller must ensure b contains only ASCII digits with an optional
// leading '-'. Overflow is not checked — Loki/VL timestamps fit in int64.
func rawBytesToInt64(b []byte) int64 {
	if len(b) == 0 {
		return 0
	}
	neg := false
	i := 0
	if b[0] == '-' {
		neg = true
		i++
	}
	var n int64
	for ; i < len(b); i++ {
		n = n*10 + int64(b[i]-'0')
	}
	if neg {
		return -n
	}
	return n
}

// statsQRFJPointNano extracts the unix-nano timestamp from the first element
// of a stats_query_range point array [ts, "value"].
func statsQRFJPointNano(v *fj.Value) int64 {
	switch v.Type() {
	case fj.TypeNumber:
		return normalizeLokiNumericTimeToUnixNano(v.GetFloat64())
	case fj.TypeString:
		if ns, ok := parseLokiTimeToUnixNano(string(v.GetStringBytes())); ok {
			return ns
		}
	}
	return 0
}

func statsQueryRangePointUnixNano(point []interface{}) int64 {
	if len(point) == 0 {
		return 0
	}
	switch ts := point[0].(type) {
	case float64:
		return normalizeLokiNumericTimeToUnixNano(ts)
	case float32:
		return normalizeLokiNumericTimeToUnixNano(float64(ts))
	case int:
		return normalizeLokiIntTimeToUnixNano(int64(ts))
	case int64:
		return normalizeLokiIntTimeToUnixNano(ts)
	case int32:
		return normalizeLokiIntTimeToUnixNano(int64(ts))
	case json.Number:
		if value, err := ts.Float64(); err == nil {
			return normalizeLokiNumericTimeToUnixNano(value)
		}
	case string:
		if value, ok := parseLokiTimeToUnixNano(ts); ok {
			return value
		}
	}
	return 0
}

func (p *Proxy) proxyStatsQuery(w http.ResponseWriter, r *http.Request, logsqlQuery string) {
	originalLogql := resolveGrafanaRangeTemplateTokens(r.FormValue("query"), r.FormValue("start"), r.FormValue("end"), r.FormValue("step"))
	if p.handleStatsCompatInstant(w, r, originalLogql, logsqlQuery) {
		return
	}

	// Group Loki-push rows (service_name) and OTel rows (service.name) under
	// one Loki label, as proxyStatsQueryRangeDirect does.
	logsqlQuery = p.addUnderscorefallbackByLabels(logsqlQuery, parseOriginalByLabels(r.FormValue("query")))
	logsqlQuery, guarded := withEmptyInputGuard(logsqlQuery)
	params := url.Values{}
	params.Set("query", logsqlQuery)
	evalTime := r.FormValue("time")
	if evalTime == "" {
		evalTime = strconv.FormatInt(time.Now().UnixNano(), 10)
	}
	params.Set("time", formatVLStatsTimestamp(evalTime))

	// Constrain VL to the original LogQL range window so stats_query scans only
	// [time-window, time] instead of ALL historical data. Without start/end,
	// VL's stats_query returns every stream ever seen (O(all_time)) rather than
	// just streams active in the window (O(window)).
	if origSpec, ok := parseOriginalRangeMetricSpec(originalLogql); ok && origSpec.Window > 0 {
		if evalNanos, ok2 := parseFlexibleUnixNanos(evalTime); ok2 {
			// VictoriaLogs filters [start, end); Loki's window is (time-range, time].
			startNanos := evalNanos - int64(origSpec.Window)
			params.Set("start", time.Unix(0, startNanos+1).UTC().Format(time.RFC3339Nano))
			params.Set("end", time.Unix(0, evalNanos+1).UTC().Format(time.RFC3339Nano))
		}
	}

	// Coalesce concurrent identical requests to avoid thundering herd when the
	// compat cache expires under high concurrency. All 50 concurrent clients
	// asking for the same instant metric query share one VL round-trip.
	key := "stats_query:" + getOrgID(r.Context()) + ":" + params.Encode()
	status, body, err := p.vlPostCoalesced(r.Context(), key, "/select/logsql/stats_query", params)
	if err != nil {
		p.writeError(w, statusFromUpstreamErr(err), err.Error())
		return
	}

	// Propagate VL error status
	if status >= 400 {
		p.writeBackendError(w, status, body)
		return
	}

	if guarded {
		body = dropEmptyInputGuard(body)
	}
	body = p.translateStatsResponseLabelsWithContext(r.Context(), body, r.FormValue("query"))
	body = wrapAsLokiResponse(body, "vector")
	if topK, topKDesc, hasTopK := parseTopKWrapper(r.FormValue("query")); hasTopK {
		body = applyTopKToVector(body, topK, topKDesc)
	}
	// Loki applies max_query_series to instant queries as well: an error for a
	// plain client, a partial vector with a warning for Logs Drilldown.
	capped, capErr := capSeriesToLimit(r.Context(), body, p.resolvedMaxStatsQuerySeries(r.Context()))
	if capErr != nil {
		p.writeError(w, badRequestStatusOr(capErr, http.StatusBadRequest), capErr.Error())
		return
	}
	body = capped
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(body)
}

// emptyInputGuardAlias names the row count appended to an ungrouped final stats
// pipe. VictoriaLogs answers such a pipe with one row even when no rows reach it
// (count()=0, sum()=NaN, max()=""), while Loki aggregates an empty input vector
// into an empty vector, so the count is the only reliable empty-input signal.
const emptyInputGuardAlias = "__lvp_n"

var (
	statsGroupByRE      = regexp.MustCompile(`^by\s*\(`)
	statsEmptyGroupByRE = regexp.MustCompile(`^by\s*\(\s*\)`)
)

// withEmptyInputGuard appends count() as __lvp_n to the final stats pipe when
// that pipe is the last one and groups by nothing. It reports whether it did.
func withEmptyInputGuard(query string) (string, bool) {
	idx := strings.LastIndex(query, "| stats ")
	if idx < 0 {
		return query, false
	}
	tail := strings.TrimSpace(query[idx+len("| stats "):])
	if tail == "" || strings.Contains(tail, "|") || (statsGroupByRE.MatchString(tail) && !statsEmptyGroupByRE.MatchString(tail)) {
		return query, false
	}
	return strings.TrimRight(query, " \t\r\n") + ", count() as " + emptyInputGuardAlias, true
}

// dropEmptyInputGuard removes the guard row from a stats_query response and
// clears the result when the guard counted no input rows. Responses without a
// guard row are returned unchanged.
func dropEmptyInputGuard(body []byte) []byte {
	var resp map[string]json.RawMessage
	var data map[string]json.RawMessage
	var result []json.RawMessage
	if json.Unmarshal(body, &resp) != nil || json.Unmarshal(resp["data"], &data) != nil || json.Unmarshal(data["result"], &result) != nil {
		return body
	}
	kept := make([]json.RawMessage, 0, len(result))
	guarded, empty := false, false
	for _, raw := range result {
		var sample struct {
			Metric map[string]string `json:"metric"`
			Value  []json.RawMessage `json:"value"`
		}
		if json.Unmarshal(raw, &sample) != nil || len(sample.Metric) != 1 || sample.Metric["__name__"] != emptyInputGuardAlias {
			kept = append(kept, raw)
			continue
		}
		guarded = true
		var count string
		if len(sample.Value) == 2 && json.Unmarshal(sample.Value[1], &count) == nil {
			n, err := strconv.ParseFloat(count, 64)
			empty = err == nil && n == 0
		}
	}
	if !guarded {
		return body
	}
	if empty {
		kept = kept[:0]
	}
	encoded, err := json.Marshal(kept)
	if err != nil {
		return body
	}
	data["result"] = encoded
	if resp["data"], err = json.Marshal(data); err != nil {
		return body
	}
	if out, err := json.Marshal(resp); err == nil {
		return out
	}
	return body
}

// proxyBinaryMetricQueryRangeVM evaluates with vector matching (on/ignoring/group_left/group_right).
func (p *Proxy) proxyBinaryMetricQueryRangeVM(w http.ResponseWriter, r *http.Request, op, leftQL, rightQL string, vm *translator.VectorMatchInfo) {
	p.proxyBinaryMetricVM(w, r, op, leftQL, rightQL, "stats_query_range", "matrix", vm)
}

func (p *Proxy) proxyBinaryMetricQueryVM(w http.ResponseWriter, r *http.Request, op, leftQL, rightQL string, vm *translator.VectorMatchInfo) {
	p.proxyBinaryMetricVM(w, r, op, leftQL, rightQL, "stats_query", "vector", vm)
}

func (p *Proxy) proxyBinaryMetricVM(w http.ResponseWriter, r *http.Request, op, leftQL, rightQL, vlEndpoint, resultType string, vm *translator.VectorMatchInfo) {
	if expr := binaryExprForRequest(r); expr != nil {
		p.proxyBinaryLogQL(w, r, expr, resultType)
		return
	}
	// If no vector matching, fall back to default behavior
	if vm == nil || (!vm.MatchOn && vm.GroupSide == "" && len(vm.On) == 0 && len(vm.Ignoring) == 0 && len(vm.GroupLeft) == 0 && len(vm.GroupRight) == 0) {
		p.proxyBinaryMetric(w, r, op, leftQL, rightQL, vlEndpoint, resultType)
		return
	}

	isRange := vlEndpoint == "stats_query_range"

	// Apply first-bucket shift if the original LogQL contains rate/bytes_rate with range==step.
	// Guard: only shift when all range windows in the binary expression are equal — mixed
	// windows (e.g. rate({a}[1m]) / rate({b}[5m])) cannot share a single shift value.
	var origStartNs, shiftNs int64
	if isRange {
		if _, uniformOk := allRangeWindowsEqual(r.FormValue("query")); uniformOk {
			if origSpec, startNs, ok := statsRateRangeEqualsStepShift(r.FormValue("query"), r); ok {
				origStartNs = startNs
				shiftNs = origSpec.Window.Nanoseconds()
			}
		}
	}

	buildParams := func(query string) url.Values {
		params := url.Values{"query": {query}}
		if isRange {
			if s := r.FormValue("start"); s != "" {
				if shiftNs > 0 {
					if ns, ok2 := parseLokiTimeToUnixNano(s); ok2 {
						params.Set("start", nanosToVLTimestamp(ns-shiftNs))
					} else {
						params.Set("start", formatVLStatsTimestamp(s))
					}
				} else {
					params.Set("start", formatVLStatsTimestamp(s))
				}
			}
			if e := r.FormValue("end"); e != "" {
				params.Set("end", formatVLStatsTimestamp(e))
			}
			if step := r.FormValue("step"); step != "" {
				params.Set("step", formatVLStep(step))
			}
		} else {
			if t := r.FormValue("time"); t != "" {
				params.Set("time", formatVLStatsTimestamp(t))
			}
		}
		return params
	}

	leftIsScalar := translator.IsScalar(leftQL)
	rightIsScalar := translator.IsScalar(rightQL)

	var leftBody, rightBody []byte
	var leftErr, rightErr error
	// Nested marker operands share the request's bounded work budget.
	leftBody, _, leftErr = p.resolveBinOpBody(r, leftQL, vlEndpoint, resultType, buildParams)
	if leftErr == nil {
		rightBody, _, rightErr = p.resolveBinOpBody(r, rightQL, vlEndpoint, resultType, buildParams)
	}
	if leftErr != nil {
		p.writeError(w, statusFromUpstreamErr(leftErr), "left query: "+leftErr.Error())
		return
	}
	if rightErr != nil {
		p.writeError(w, statusFromUpstreamErr(rightErr), "right query: "+rightErr.Error())
		return
	}

	var result []byte
	if leftIsScalar || rightIsScalar {
		result = combineBinaryMetricResults(leftBody, rightBody, op, resultType, leftIsScalar, rightIsScalar, leftQL, rightQL)
	} else {
		var err error
		result, err = matchBinaryMetricResultsContext(r.Context(), leftBody, rightBody, op, resultType, vm, false)
		if err != nil {
			p.writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
	}

	if origStartNs > 0 {
		endNs, _ := parseLokiTimeToUnixNano(r.FormValue("end"))
		result = relabelTumblingStatsQueryRange(result, origStartNs, endNs, shiftNs)
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write(result)
}

func (p *Proxy) proxyBinaryMetric(w http.ResponseWriter, r *http.Request, op, leftQL, rightQL, vlEndpoint, resultType string) {
	isRange := vlEndpoint == "stats_query_range"

	// Apply first-bucket shift if the original LogQL contains rate/bytes_rate with range==step.
	// Guard: only shift when all range windows in the binary expression are equal.
	var origStartNs, shiftNs int64
	if isRange {
		if _, uniformOk := allRangeWindowsEqual(r.FormValue("query")); uniformOk {
			if origSpec, startNs, ok := statsRateRangeEqualsStepShift(r.FormValue("query"), r); ok {
				origStartNs = startNs
				shiftNs = origSpec.Window.Nanoseconds()
			}
		}
	}

	buildParams := func(query string) url.Values {
		params := url.Values{"query": {query}}
		if isRange {
			if s := r.FormValue("start"); s != "" {
				if shiftNs > 0 {
					if ns, ok2 := parseLokiTimeToUnixNano(s); ok2 {
						params.Set("start", nanosToVLTimestamp(ns-shiftNs))
					} else {
						params.Set("start", formatVLStatsTimestamp(s))
					}
				} else {
					params.Set("start", formatVLStatsTimestamp(s))
				}
			}
			if e := r.FormValue("end"); e != "" {
				params.Set("end", formatVLStatsTimestamp(e))
			}
			if step := r.FormValue("step"); step != "" {
				params.Set("step", formatVLStep(step))
			}
		} else {
			if t := r.FormValue("time"); t != "" {
				params.Set("time", formatVLStatsTimestamp(t))
			}
		}
		return params
	}

	// Check if either side is a scalar or a nested binary marker.
	leftIsScalar := translator.IsScalar(leftQL)
	rightIsScalar := translator.IsScalar(rightQL)
	leftIsMarker := strings.HasPrefix(leftQL, translator.BinaryMetricPrefix)
	rightIsMarker := strings.HasPrefix(rightQL, translator.BinaryMetricPrefix)

	var leftBody, rightBody []byte
	var leftErr, rightErr error

	// When either side is a nested binary marker, resolve it recursively.
	// Otherwise fall through to the plain VL fetch paths.
	if leftIsMarker || rightIsMarker {
		leftBody, leftIsScalar, leftErr = p.resolveBinOpBody(r, leftQL, vlEndpoint, resultType, buildParams)
		if leftErr == nil {
			rightBody, rightIsScalar, rightErr = p.resolveBinOpBody(r, rightQL, vlEndpoint, resultType, buildParams)
		}
	} else if !leftIsScalar && !rightIsScalar {
		// Run both non-scalar VL fetches concurrently.
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			resp, e := p.vlPost(r.Context(), "/select/logsql/"+vlEndpoint, buildParams(leftQL))
			if e != nil {
				leftErr = e
				return
			}
			defer resp.Body.Close()
			leftBody, _ = readBodyLimited(resp.Body, int64(p.limits().BufferedBackendBodyBytes))
		}()
		go func() {
			defer wg.Done()
			resp, e := p.vlPost(r.Context(), "/select/logsql/"+vlEndpoint, buildParams(rightQL))
			if e != nil {
				rightErr = e
				return
			}
			defer resp.Body.Close()
			rightBody, _ = readBodyLimited(resp.Body, int64(p.limits().BufferedBackendBodyBytes))
		}()
		wg.Wait()
	} else {
		if leftIsScalar {
			leftBody = []byte(`{"status":"success","data":{"resultType":"scalar","result":[0,"` + leftQL + `"]}}`)
		} else {
			resp, e := p.vlPost(r.Context(), "/select/logsql/"+vlEndpoint, buildParams(leftQL))
			if e != nil {
				p.writeError(w, statusFromUpstreamErr(e), "left query: "+e.Error())
				return
			}
			defer resp.Body.Close()
			leftBody, _ = readBodyLimited(resp.Body, int64(p.limits().BufferedBackendBodyBytes))
		}

		if rightIsScalar {
			rightBody = []byte(`{"status":"success","data":{"resultType":"scalar","result":[0,"` + rightQL + `"]}}`)
		} else {
			resp, e := p.vlPost(r.Context(), "/select/logsql/"+vlEndpoint, buildParams(rightQL))
			if e != nil {
				p.writeError(w, statusFromUpstreamErr(e), "right query: "+e.Error())
				return
			}
			defer resp.Body.Close()
			rightBody, _ = readBodyLimited(resp.Body, int64(p.limits().BufferedBackendBodyBytes))
		}
	}

	if leftErr != nil {
		p.writeError(w, statusFromUpstreamErr(leftErr), "left query: "+leftErr.Error())
		return
	}
	if rightErr != nil {
		p.writeError(w, statusFromUpstreamErr(rightErr), "right query: "+rightErr.Error())
		return
	}

	// Combine results with arithmetic at proxy level
	result := combineBinaryMetricResults(leftBody, rightBody, op, resultType, leftIsScalar, rightIsScalar, leftQL, rightQL)
	if origStartNs > 0 {
		endNs, _ := parseLokiTimeToUnixNano(r.FormValue("end"))
		result = relabelTumblingStatsQueryRange(result, origStartNs, endNs, shiftNs)
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write(result)
}

// resolveBinOpBody returns the result body for one side of a binary expression.
// Handles scalar strings, nested binary markers, and plain VL queries.
func (p *Proxy) resolveBinOpBody(r *http.Request, query, vlEndpoint, resultType string, buildParams func(string) url.Values) (body []byte, isScalar bool, err error) {
	if translator.IsScalar(query) {
		return []byte(`{"status":"success","data":{"resultType":"scalar","result":[0,"` + query + `"]}}`), true, nil
	}
	if strings.HasPrefix(query, translator.BinaryMetricPrefix) {
		body, err = p.evalBinaryMarker(r, query, vlEndpoint, resultType, buildParams)
		return body, false, err
	}
	resp, e := p.vlPost(r.Context(), "/select/logsql/"+vlEndpoint, buildParams(query))
	if e != nil {
		return nil, false, e
	}
	defer resp.Body.Close()
	body, err = readBodyLimited(resp.Body, int64(p.limits().BufferedBackendBodyBytes))
	if err != nil {
		return nil, false, err
	}
	if resp.StatusCode >= http.StatusBadRequest {
		return nil, false, p.redactedBackendStatusError("binary operand backend returned status", resp.StatusCode, body)
	}
	return body, false, nil
}

// evalBinaryMarker recursively evaluates a __binary__: expression marker.
func (p *Proxy) evalBinaryMarker(r *http.Request, marker, vlEndpoint, resultType string, buildParams func(string) url.Values) ([]byte, error) {
	var err error
	r, err = nextBinaryEvaluation(r)
	if err != nil {
		return nil, err
	}
	op, left, right, vm, ok := translator.ParseBinaryMetricExprFull(marker)
	if !ok {
		return nil, fmt.Errorf("invalid binary expression marker")
	}

	leftBody, leftScalar, err := p.resolveBinOpBody(r, left, vlEndpoint, resultType, buildParams)
	if err != nil {
		return nil, err
	}
	rightBody, rightScalar, err := p.resolveBinOpBody(r, right, vlEndpoint, resultType, buildParams)
	if err != nil {
		return nil, err
	}

	if !leftScalar && !rightScalar {
		return matchBinaryMetricResultsContext(r.Context(), leftBody, rightBody, op, resultType, vm, false)
	}
	return combineBinaryMetricResults(leftBody, rightBody, op, resultType, leftScalar, rightScalar, left, right), nil
}

// combineBinaryMetricResults applies arithmetic op to two VL stats results.
func combineBinaryMetricResults(leftBody, rightBody []byte, op, resultType string, leftScalar, rightScalar bool, leftQL, rightQL string) []byte {
	// For scalar operations (e.g., rate(...) * 100), apply to each value
	if rightScalar {
		scalar := parseScalar(rightQL)
		return applyScalarOp(leftBody, op, scalar, resultType)
	}
	if leftScalar {
		scalar := parseScalar(leftQL)
		return applyScalarOpReverse(rightBody, op, scalar, resultType)
	}

	// Both sides are metric results — combine point-by-point
	// This is a simplified implementation that handles the common case
	// of matching time series (same labels, same timestamps)
	return combineMetricResults(leftBody, rightBody, op, resultType)
}

func parseScalar(s string) float64 {
	f, _ := strconv.ParseFloat(strings.TrimSpace(s), 64)
	return f
}

func applyScalarOp(body []byte, op string, scalar float64, resultType string) []byte {
	var vlResp map[string]interface{}
	if err := json.Unmarshal(body, &vlResp); err != nil {
		return wrapAsLokiResponse(body, resultType)
	}

	results, _ := extractMetricResults(vlResp)
	for _, r := range results {
		rm, _ := r.(map[string]interface{})
		applyScalarToSample(rm, scalar, op, false)
	}

	result, _ := json.Marshal(vlResp)
	return wrapAsLokiResponse(result, resultType)
}

func applyScalarOpReverse(body []byte, op string, scalar float64, resultType string) []byte {
	var vlResp map[string]interface{}
	if err := json.Unmarshal(body, &vlResp); err != nil {
		return wrapAsLokiResponse(body, resultType)
	}

	results, _ := extractMetricResults(vlResp)
	for _, r := range results {
		rm, _ := r.(map[string]interface{})
		applyScalarToSample(rm, scalar, op, true)
	}

	result, _ := json.Marshal(vlResp)
	return wrapAsLokiResponse(result, resultType)
}

func parsePointValue(raw interface{}) float64 {
	switch v := raw.(type) {
	case float64:
		return v
	case string:
		parsed, _ := strconv.ParseFloat(v, 64)
		return parsed
	default:
		return 0
	}
}

func combineMetricResults(leftBody, rightBody []byte, op, resultType string) []byte {
	// Parse both results
	var leftResp, rightResp map[string]interface{}
	json.Unmarshal(leftBody, &leftResp)
	json.Unmarshal(rightBody, &rightResp)

	leftResults, _ := extractMetricResults(leftResp)
	rightResults, _ := extractMetricResults(rightResp)

	// Build a map of right results by metric labels for joining
	rightMap := make(map[string]map[string]float64)
	for _, r := range rightResults {
		rm, _ := r.(map[string]interface{})
		metric, _ := rm["metric"].(map[string]interface{})
		key := metricKey(metric)
		rightMap[key] = samplePointIndex(rm)
	}

	// Combine: for each left result, find matching right result and apply op
	for _, r := range leftResults {
		rm, _ := r.(map[string]interface{})
		metric, _ := rm["metric"].(map[string]interface{})
		key := metricKey(metric)
		rightIdx := rightMap[key]
		if len(rightIdx) > 0 {
			applyBinaryToSample(rm, rightIdx, op)
		}
	}

	result, _ := json.Marshal(leftResp)
	return wrapAsLokiResponse(result, resultType)
}

func extractMetricResults(payload map[string]interface{}) ([]interface{}, bool) {
	if results, ok := payload["results"].([]interface{}); ok {
		return results, true
	}
	if data, ok := payload["data"].(map[string]interface{}); ok {
		if result, ok := data["result"].([]interface{}); ok {
			return result, true
		}
	}
	if result, ok := payload["result"].([]interface{}); ok {
		return result, true
	}
	return nil, false
}

func applyScalarToSample(sample map[string]interface{}, scalar float64, op string, reverse bool) {
	values, _ := sample["values"].([]interface{})
	for i, raw := range values {
		point, _ := raw.([]interface{})
		if len(point) < 2 {
			continue
		}
		val := parsePointValue(point[1])
		newVal := applyOp(val, scalar, op)
		if reverse {
			newVal = applyOp(scalar, val, op)
		}
		point[1] = strconv.FormatFloat(newVal, 'f', -1, 64)
		values[i] = point
	}
	if len(values) > 0 {
		sample["values"] = values
	}

	if value, ok := sample["value"].([]interface{}); ok && len(value) >= 2 {
		val := parsePointValue(value[1])
		newVal := applyOp(val, scalar, op)
		if reverse {
			newVal = applyOp(scalar, val, op)
		}
		value[1] = strconv.FormatFloat(newVal, 'f', -1, 64)
		sample["value"] = value
	}
}

func samplePointIndex(sample map[string]interface{}) map[string]float64 {
	index := map[string]float64{}

	values, _ := sample["values"].([]interface{})
	for _, raw := range values {
		point, _ := raw.([]interface{})
		if len(point) < 2 {
			continue
		}
		index[fmt.Sprintf("%v", point[0])] = parsePointValue(point[1])
	}

	if value, ok := sample["value"].([]interface{}); ok && len(value) >= 2 {
		index[fmt.Sprintf("%v", value[0])] = parsePointValue(value[1])
	}

	return index
}

func applyBinaryToSample(sample map[string]interface{}, rightIndex map[string]float64, op string) {
	values, _ := sample["values"].([]interface{})
	for i, raw := range values {
		point, _ := raw.([]interface{})
		if len(point) < 2 {
			continue
		}
		ts := fmt.Sprintf("%v", point[0])
		rightVal, ok := rightIndex[ts]
		if !ok {
			continue
		}
		leftVal := parsePointValue(point[1])
		point[1] = strconv.FormatFloat(applyOp(leftVal, rightVal, op), 'f', -1, 64)
		values[i] = point
	}
	if len(values) > 0 {
		sample["values"] = values
	}

	if value, ok := sample["value"].([]interface{}); ok && len(value) >= 2 {
		ts := fmt.Sprintf("%v", value[0])
		if rightVal, ok := rightIndex[ts]; ok {
			leftVal := parsePointValue(value[1])
			value[1] = strconv.FormatFloat(applyOp(leftVal, rightVal, op), 'f', -1, 64)
			sample["value"] = value
		}
	}
}

func applyOp(a, b float64, op string) float64 {
	switch op {
	case "/":
		if b == 0 {
			return 0 // avoid division by zero, return 0 like Prometheus
		}
		return a / b
	case "*":
		return a * b
	case "+":
		return a + b
	case "-":
		return a - b
	case "%":
		if b == 0 {
			return 0
		}
		return math.Mod(a, b)
	case "^":
		return math.Pow(a, b)
	case "==":
		if a == b {
			return 1
		}
		return 0
	case "!=":
		if a != b {
			return 1
		}
		return 0
	case ">":
		if a > b {
			return 1
		}
		return 0
	case "<":
		if a < b {
			return 1
		}
		return 0
	case ">=":
		if a >= b {
			return 1
		}
		return 0
	case "<=":
		if a <= b {
			return 1
		}
		return 0
	}
	return a
}

func metricKey(metric map[string]interface{}) string {
	if metric == nil {
		return "{}"
	}
	parts := make([]string, 0, len(metric))
	for k, v := range metric {
		parts = append(parts, fmt.Sprintf("%s=%v", k, v))
	}
	sort.Strings(parts)
	return strings.Join(parts, ",")
}

// applyTopKToMatrix ranks independently at each evaluation timestamp, as
// Loki/PromQL range queries require. Winners can change, so the result may
// contain more than k series, with only their winning samples retained.
func applyTopKToMatrix(body []byte, k int, descending bool) []byte {
	v, err := fj.ParseBytes(body)
	if err != nil {
		return body
	}
	if string(v.GetStringBytes("status")) != "success" || string(v.GetStringBytes("data", "resultType")) != "matrix" {
		return body
	}
	result := v.GetArray("data", "result")
	if len(result) <= k {
		return body
	}
	data := v.Get("data")
	if data == nil {
		return body
	}
	type ranked struct {
		point  *fj.Value
		value  float64
		series int
	}
	steps := make(map[float64][]ranked)
	for i, series := range result {
		for _, point := range series.GetArray("values") {
			pair := point.GetArray()
			if len(pair) != 2 {
				return body
			}
			ts, err := pair[0].Float64()
			if err != nil {
				return body
			}
			value, err := strconv.ParseFloat(string(pair[1].GetStringBytes()), 64)
			if err != nil {
				return body
			}
			steps[ts] = append(steps[ts], ranked{point, value, i})
		}
	}
	selected := make(map[*fj.Value]bool)
	for _, ranks := range steps {
		sort.Slice(ranks, func(a, b int) bool {
			av, bv := ranks[a].value, ranks[b].value
			if math.IsNaN(av) {
				return false
			}
			if math.IsNaN(bv) {
				return true
			}
			if av == bv {
				return ranks[a].series < ranks[b].series
			}
			if descending {
				return av > bv
			}
			return av < bv
		})
		for i := 0; i < min(max(k, 0), len(ranks)); i++ {
			selected[ranks[i].point] = true
		}
	}
	var arena fj.Arena
	filtered := arena.NewArray()
	count := 0
	for _, series := range result {
		values := arena.NewArray()
		n := 0
		for _, point := range series.GetArray("values") {
			if selected[point] {
				values.SetArrayItem(n, point)
				n++
			}
		}
		if n == 0 {
			continue
		}
		series.Set("values", values)
		filtered.SetArrayItem(count, series)
		count++
	}
	data.Set("result", filtered)
	return v.MarshalTo(nil)
}

// applyTopKToVector filters a Loki vector response to the top or bottom k samples.
func applyTopKToVector(body []byte, k int, descending bool) []byte {
	v, err := fj.ParseBytes(body)
	if err != nil {
		return body
	}
	result := v.GetArray("data", "result")
	if len(result) <= k {
		return body
	}

	type ranked struct {
		idx int
		val float64
	}
	ranks := make([]ranked, len(result))
	for i, s := range result {
		arr := s.GetArray("value")
		var f float64
		if len(arr) >= 2 {
			f = parseFloat64Bytes(arr[1].GetStringBytes())
		}
		ranks[i] = ranked{i, f}
	}
	sort.Slice(ranks, func(a, b int) bool {
		if descending {
			return ranks[a].val > ranks[b].val
		}
		return ranks[a].val < ranks[b].val
	})

	kept := make([]int, k)
	for i := range kept {
		kept[i] = ranks[i].idx
	}
	sort.Ints(kept)

	return rebuildMatrixOrVector(body, "vector", result, kept)
}

// rebuildMatrixOrVector rebuilds the Loki response JSON keeping only the series at
// indices `keep` from `result`.
func rebuildMatrixOrVector(body []byte, resultType string, result []*fj.Value, keep []int) []byte {
	var buf bytes.Buffer
	buf.WriteString(`{"status":"success","data":{"resultType":"`)
	buf.WriteString(resultType)
	buf.WriteString(`","result":[`)
	for i, idx := range keep {
		if i > 0 {
			buf.WriteByte(',')
		}
		buf.Write(result[idx].MarshalTo(nil))
	}
	buf.WriteString(`]}}`)
	return buf.Bytes()
}

// writeTopKFiltered applies topk/bottomk post-processing to a buffered response
// and writes the filtered result to dst.
func writeTopKFiltered(dst http.ResponseWriter, buf *bufferedResponseWriter, k int, descending bool, resultType string) {
	body := buf.body
	if len(body) > 0 {
		if resultType == "matrix" {
			body = applyTopKToMatrix(body, k, descending)
		} else {
			body = applyTopKToVector(body, k, descending)
		}
	}
	code := buf.code
	if code == 0 {
		code = http.StatusOK
	}
	dst.Header().Set("Content-Type", "application/json")
	if code != http.StatusOK {
		dst.WriteHeader(code)
	}
	_, _ = dst.Write(body)
}

func parseFloat64Bytes(b []byte) float64 {
	if len(b) == 0 {
		return 0
	}
	f, _ := strconv.ParseFloat(string(b), 64)
	return f
}

func abs64(f float64) float64 {
	if f < 0 {
		return -f
	}
	return f
}
