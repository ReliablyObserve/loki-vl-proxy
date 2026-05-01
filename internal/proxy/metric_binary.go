package proxy

import (
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
)

// --- Stats query proxying ---

func (p *Proxy) proxyStatsQueryRange(w http.ResponseWriter, r *http.Request, logsqlQuery string) {
	originalLogql := resolveGrafanaRangeTemplateTokens(r.FormValue("query"), r.FormValue("start"), r.FormValue("end"), r.FormValue("step"))
	if p.handleStatsCompatRange(w, r, originalLogql, logsqlQuery) {
		return
	}

	// For rate() with range==step: VL tumbling windows miss the pre-window data
	// for the first evaluation point (Loki uses a sliding window [T0-W, T0];
	// VL gives [T0, T0+W)). Shift start back by W, call the direct path, then
	// trim the extra leading bucket from the response.
	if origSpec, origStartNs, ok := statsRateRangeEqualsStepShift(originalLogql, r); ok {
		buf := &bufferedResponseWriter{}
		shiftedR := r.Clone(r.Context())
		_ = shiftedR.ParseForm()
		shiftedR.Form.Set("start", nanosToVLTimestamp(origStartNs-origSpec.Window.Nanoseconds()))
		p.proxyStatsQueryRangeDirect(buf, shiftedR, logsqlQuery)
		body := trimStatsQueryRangeResponseFromStart(buf.body, origStartNs)
		code := buf.code
		if code == 0 {
			code = http.StatusOK
		}
		w.Header().Set("Content-Type", "application/json")
		if code != http.StatusOK {
			w.WriteHeader(code)
		}
		_, _ = w.Write(body)
		return
	}

	p.proxyStatsQueryRangeDirect(w, r, logsqlQuery)
}

// proxyStatsQueryRangeDirect issues the VL stats_query_range request directly,
// bypassing the compat layer and the rate-shift gate. Call this when the caller
// has already applied any necessary start shift (e.g. proxyBareParserMetricViaStats).
func (p *Proxy) proxyStatsQueryRangeDirect(w http.ResponseWriter, r *http.Request, logsqlQuery string) {
	// Keep metric query_range as a single backend request. Window splitting and
	// window-level cache reuse are for raw log queries only.
	params := buildStatsQueryRangeParams(logsqlQuery, r.FormValue("start"), r.FormValue("end"), r.FormValue("step"))

	// Coalesce concurrent identical range metric queries.
	key := "stats_query_range:" + getOrgID(r.Context()) + ":" + params.Encode()
	status, body, err := p.vlPostCoalesced(r.Context(), key, "/select/logsql/stats_query_range", params)
	if err != nil {
		p.writeError(w, statusFromUpstreamErr(err), err.Error())
		return
	}

	// Propagate VL error status
	if status >= 400 {
		p.writeError(w, status, string(body))
		return
	}

	body = trimStatsQueryRangeResponseToEnd(body, r.FormValue("end"))

	// VL stats_query_range returns Prometheus-compatible format.
	// Just wrap it in Loki's envelope.
	// Translate label names (e.g., dots → underscores) in metric labels.
	body = p.translateStatsResponseLabelsWithContext(r.Context(), body, r.FormValue("query"))
	w.Header().Set("Content-Type", "application/json")
	w.Write(wrapAsLokiResponse(body, "matrix"))
}

// statsRateRangeEqualsStepShift detects whether the query contains a rate() or
// bytes_rate() with range==step so that proxyStatsQueryRange can apply the
// first-bucket shift. The check scans the full expression (not just the
// top-level function) so outer aggregations like sum by(x)(rate(...)) and
// binary expressions are also detected.
// Returns (spec, origStartNs, true) when shifting is needed.
func statsRateRangeEqualsStepShift(originalLogql string, r *http.Request) (origSpec originalRangeMetricSpec, origStartNs int64, ok bool) {
	spec, hasSpec := parseOriginalRangeMetricSpec(originalLogql)
	if !hasSpec || spec.Window <= 0 {
		return
	}
	// The tumbling-window first-bucket drift only affects rate() and bytes_rate().
	// Search the full expression for these function calls — "rate(" is also present
	// in "rate_counter(" and "rate_sum(", so exclude those explicitly.
	lq := strings.ToLower(strings.TrimSpace(originalLogql))
	hasBytesRate := strings.Contains(lq, "bytes_rate(")
	hasBareRate := strings.Contains(lq, "rate(") &&
		!strings.Contains(lq, "rate_counter(") &&
		!strings.Contains(lq, "rate_sum(")
	if !hasBareRate && !hasBytesRate {
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

type statsQueryRangeSeries struct {
	Metric json.RawMessage `json:"metric"`
	Values [][]interface{} `json:"values"`
}

type statsQueryRangeResponse struct {
	Data struct {
		ResultType string                  `json:"resultType,omitempty"`
		Result     []statsQueryRangeSeries `json:"result"`
	} `json:"data"`
	Results []statsQueryRangeSeries `json:"results,omitempty"`
}

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

// trimStatsQueryRangeResponseFromStart removes points with timestamp < startNs.
// Used when start was shifted back to include the pre-start bucket for rate().
func trimStatsQueryRangeResponseFromStart(body []byte, startNs int64) []byte {
	var resp statsQueryRangeResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return body
	}
	results := resp.Results
	target := &resp.Results
	if len(results) == 0 {
		results = resp.Data.Result
		target = &resp.Data.Result
	}
	if len(results) == 0 {
		return body
	}
	changed := false
	trimmed := make([]statsQueryRangeSeries, 0, len(results))
	for _, series := range results {
		filtered := make([][]interface{}, 0, len(series.Values))
		for _, point := range series.Values {
			if statsQueryRangePointUnixNano(point) >= startNs {
				filtered = append(filtered, point)
				continue
			}
			changed = true
		}
		series.Values = filtered
		trimmed = append(trimmed, series)
	}
	if !changed {
		return body
	}
	*target = trimmed
	encoded, err := json.Marshal(resp)
	if err != nil {
		return body
	}
	return encoded
}

func trimStatsQueryRangeResponseToEnd(body []byte, endRaw string) []byte {
	endNs, ok := parseLokiTimeToUnixNano(endRaw)
	if !ok {
		return body
	}

	var resp statsQueryRangeResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return body
	}

	results := resp.Results
	target := &resp.Results
	if len(results) == 0 {
		results = resp.Data.Result
		target = &resp.Data.Result
	}
	if len(results) == 0 {
		return body
	}

	changed := false
	trimmed := make([]statsQueryRangeSeries, 0, len(results))
	for _, series := range results {
		filtered := make([][]interface{}, 0, len(series.Values))
		for _, point := range series.Values {
			if statsQueryRangePointUnixNano(point) <= endNs {
				filtered = append(filtered, point)
				continue
			}
			changed = true
		}
		series.Values = filtered
		trimmed = append(trimmed, series)
	}
	if !changed {
		return body
	}
	*target = trimmed
	encoded, err := json.Marshal(resp)
	if err != nil {
		return body
	}
	return encoded
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
			startNanos := evalNanos - int64(origSpec.Window)
			params.Set("start", nanosToVLTimestamp(startNanos))
			params.Set("end", nanosToVLTimestamp(evalNanos))
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
		p.writeError(w, status, string(body))
		return
	}

	body = p.translateStatsResponseLabelsWithContext(r.Context(), body, r.FormValue("query"))
	w.Header().Set("Content-Type", "application/json")
	w.Write(wrapAsLokiResponse(body, "vector"))
}

// proxyBinaryMetricQueryRangeVM evaluates with vector matching (on/ignoring/group_left/group_right).
func (p *Proxy) proxyBinaryMetricQueryRangeVM(w http.ResponseWriter, r *http.Request, op, leftQL, rightQL string, vm *translator.VectorMatchInfo) {
	p.proxyBinaryMetricVM(w, r, op, leftQL, rightQL, "stats_query_range", "matrix", vm)
}

func (p *Proxy) proxyBinaryMetricQueryVM(w http.ResponseWriter, r *http.Request, op, leftQL, rightQL string, vm *translator.VectorMatchInfo) {
	p.proxyBinaryMetricVM(w, r, op, leftQL, rightQL, "stats_query", "vector", vm)
}

func (p *Proxy) proxyBinaryMetricVM(w http.ResponseWriter, r *http.Request, op, leftQL, rightQL, vlEndpoint, resultType string, vm *translator.VectorMatchInfo) {
	// If no vector matching, fall back to default behavior
	if vm == nil || (len(vm.On) == 0 && len(vm.Ignoring) == 0 && len(vm.GroupLeft) == 0 && len(vm.GroupRight) == 0) {
		p.proxyBinaryMetric(w, r, op, leftQL, rightQL, vlEndpoint, resultType)
		return
	}

	isRange := vlEndpoint == "stats_query_range"
	buildParams := func(query string) url.Values {
		params := url.Values{"query": {query}}
		if isRange {
			if s := r.FormValue("start"); s != "" {
				params.Set("start", formatVLStatsTimestamp(s))
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

	// Run both non-scalar VL fetches concurrently.
	if !leftIsScalar && !rightIsScalar {
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
			leftBody, _ = readBodyLimited(resp.Body, maxBufferedBackendBodyBytes)
		}()
		go func() {
			defer wg.Done()
			resp, e := p.vlPost(r.Context(), "/select/logsql/"+vlEndpoint, buildParams(rightQL))
			if e != nil {
				rightErr = e
				return
			}
			defer resp.Body.Close()
			rightBody, _ = readBodyLimited(resp.Body, maxBufferedBackendBodyBytes)
		}()
		wg.Wait()
		if leftErr != nil {
			p.writeError(w, statusFromUpstreamErr(leftErr), "left query: "+leftErr.Error())
			return
		}
		if rightErr != nil {
			p.writeError(w, statusFromUpstreamErr(rightErr), "right query: "+rightErr.Error())
			return
		}
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
			leftBody, _ = readBodyLimited(resp.Body, maxBufferedBackendBodyBytes)
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
			rightBody, _ = readBodyLimited(resp.Body, maxBufferedBackendBodyBytes)
		}
	}

	// Apply vector matching: on(), ignoring(), group_left(), group_right()
	var result []byte
	if len(vm.On) > 0 {
		result = applyOnMatching(leftBody, rightBody, op, vm.On, resultType)
	} else if len(vm.Ignoring) > 0 {
		if err := validateVectorMatchCardinality(leftBody, rightBody, nil, vm.Ignoring, len(vm.GroupLeft) > 0, len(vm.GroupRight) > 0); err != nil {
			p.writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		result = applyIgnoringMatching(leftBody, rightBody, op, vm.Ignoring, resultType)
	} else {
		// group_left/group_right without on/ignoring — use default matching
		result = combineBinaryMetricResults(leftBody, rightBody, op, resultType, leftIsScalar, rightIsScalar, leftQL, rightQL)
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write(result)
}

func (p *Proxy) proxyBinaryMetric(w http.ResponseWriter, r *http.Request, op, leftQL, rightQL, vlEndpoint, resultType string) {
	isRange := vlEndpoint == "stats_query_range"

	buildParams := func(query string) url.Values {
		params := url.Values{"query": {query}}
		if isRange {
			if s := r.FormValue("start"); s != "" {
				params.Set("start", formatVLStatsTimestamp(s))
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

	// Check if either side is a scalar (number)
	leftIsScalar := translator.IsScalar(leftQL)
	rightIsScalar := translator.IsScalar(rightQL)

	var leftBody, rightBody []byte
	var leftErr, rightErr error

	// Run both non-scalar VL fetches concurrently.
	if !leftIsScalar && !rightIsScalar {
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
			leftBody, _ = readBodyLimited(resp.Body, maxBufferedBackendBodyBytes)
		}()
		go func() {
			defer wg.Done()
			resp, e := p.vlPost(r.Context(), "/select/logsql/"+vlEndpoint, buildParams(rightQL))
			if e != nil {
				rightErr = e
				return
			}
			defer resp.Body.Close()
			rightBody, _ = readBodyLimited(resp.Body, maxBufferedBackendBodyBytes)
		}()
		wg.Wait()
		if leftErr != nil {
			p.writeError(w, statusFromUpstreamErr(leftErr), "left query: "+leftErr.Error())
			return
		}
		if rightErr != nil {
			p.writeError(w, statusFromUpstreamErr(rightErr), "right query: "+rightErr.Error())
			return
		}
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
			leftBody, _ = readBodyLimited(resp.Body, maxBufferedBackendBodyBytes)
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
			rightBody, _ = readBodyLimited(resp.Body, maxBufferedBackendBodyBytes)
		}
	}

	// Combine results with arithmetic at proxy level
	result := combineBinaryMetricResults(leftBody, rightBody, op, resultType, leftIsScalar, rightIsScalar, leftQL, rightQL)

	w.Header().Set("Content-Type", "application/json")
	w.Write(result)
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

