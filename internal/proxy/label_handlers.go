package proxy

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"
)

// handleLabels returns label names.
// Loki: GET /loki/api/v1/labels?start=...&end=...
// VL:   GET /select/logsql/stream_field_names?query=*&start=...&end=...
//
//	fallback /select/logsql/field_names for older backends
func (p *Proxy) handleLabels(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	if p.handleMultiTenantFanout(w, r, "labels", p.handleLabels) {
		return
	}
	orgID := r.Header.Get("X-Scope-OrgID")
	cacheKey := p.canonicalReadCacheKey("labels", orgID, r)

	if cached, remaining, _, ok := p.endpointReadCacheEntry("labels", cacheKey); ok {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(cached)
		p.metrics.RecordRequest("labels", http.StatusOK, time.Since(start))
		p.metrics.RecordCacheHit()
		if p.shouldRefreshLabelsInBackground(remaining, CacheTTLs["labels"]) {
			search := strings.TrimSpace(r.FormValue("search"))
			if search == "" {
				search = strings.TrimSpace(r.FormValue("q"))
			}
			p.refreshLabelsCacheAsync(orgID, cacheKey, r.FormValue("query"), r.FormValue("start"), r.FormValue("end"), search, p.snapshotForwardedAuth(r))
		}
		return
	}
	p.metrics.RecordCacheMiss()
	r = withOrgID(r)

	search := strings.TrimSpace(r.FormValue("search"))
	if search == "" {
		search = strings.TrimSpace(r.FormValue("q"))
	}

	labels, err := p.fetchScopedLabelNames(r.Context(), r.FormValue("query"), r.FormValue("start"), r.FormValue("end"), search, true)
	if err != nil {
		status := statusFromUpstreamErr(err)
		p.writeError(w, status, err.Error())
		p.metrics.RecordRequest("labels", status, time.Since(start))
		return
	}

	filtered := make([]string, 0, len(labels))
	for _, v := range labels {
		// Filter VL internal fields only (before translation)
		if isVLInternalField(v) || v == "detected_level" {
			continue
		}
		filtered = append(filtered, v)
	}

	// Apply label name translation (e.g., dots → underscores)
	labels = p.labelTranslator.TranslateLabelsList(filtered)
	labels = appendSyntheticLabels(labels)

	result := lokiLabelsResponse(labels)
	p.setEndpointReadCacheWithTTL("labels", cacheKey, result, CacheTTLs["labels"])
	w.Header().Set("Content-Type", "application/json")
	w.Write(result)
	p.metrics.RecordRequest("labels", http.StatusOK, time.Since(start))
}

// handleLabelValues returns values for a specific label.
// Loki: GET /loki/api/v1/label/{name}/values?start=...&end=...
// VL:   GET /select/logsql/stream_field_values?query=*&field={name}&start=...&end=...
//
//	fallback /select/logsql/field_values for older backends
func (p *Proxy) handleLabelValues(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	// Extract label name from URL: /loki/api/v1/label/{name}/values
	path := r.URL.Path
	parts := strings.Split(path, "/")
	if len(parts) < 7 || parts[6] != "values" {
		p.writeError(w, http.StatusBadRequest, "invalid label values URL")
		return
	}
	labelName := parts[5]
	if strings.Contains(r.Header.Get("X-Scope-OrgID"), "|") && labelName == "__tenant_id__" {
		values := splitMultiTenantOrgIDs(r.Header.Get("X-Scope-OrgID"))
		result := lokiLabelsResponse(values)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(result)
		p.metrics.RecordRequest("label_values", http.StatusOK, time.Since(start))
		return
	}
	if p.handleMultiTenantFanout(w, r, "label_values", p.handleLabelValues) {
		return
	}
	orgID := r.Header.Get("X-Scope-OrgID")
	cacheKey := p.canonicalReadCacheKey("label_values", orgID, r, labelName)
	rawQuery := r.FormValue("query")
	rawLimit := r.FormValue("limit")
	rawOffset := r.FormValue("offset")
	search := r.FormValue("search")
	if strings.TrimSpace(search) == "" {
		search = r.FormValue("q")
	}
	offset := parseNonNegativeInt(rawOffset, 0)
	limit := p.defaultLabelValuesLimit(rawLimit)
	if rawLimit == "" && !p.labelValuesBrowseMode(rawQuery) {
		limit = maxLimitValue
	}

	if cached, remaining, _, ok := p.endpointReadCacheEntry("label_values", cacheKey); ok {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(cached)
		p.metrics.RecordRequest("label_values", http.StatusOK, time.Since(start))
		p.metrics.RecordCacheHit()
		if p.shouldRefreshLabelsInBackground(remaining, CacheTTLs["label_values"]) {
			p.refreshLabelValuesCacheAsync(
				orgID,
				cacheKey,
				labelName,
				r.FormValue("query"),
				r.FormValue("start"),
				r.FormValue("end"),
				r.FormValue("limit"),
				search,
				p.snapshotForwardedAuth(r),
			)
		}
		return
	}
	p.metrics.RecordCacheMiss()
	r = withOrgID(r)

	if p.labelValuesBrowseMode(rawQuery) {
		if indexedValues, ok := p.selectLabelValuesFromIndex(orgID, labelName, search, offset, limit); ok {
			result := lokiLabelsResponse(indexedValues)
			p.setEndpointReadCacheWithTTL("label_values", cacheKey, result, CacheTTLs["label_values"])
			w.Header().Set("Content-Type", "application/json")
			w.Write(result)
			p.metrics.RecordRequest("label_values", http.StatusOK, time.Since(start))
			return
		}
	}

	if labelName == "service_name" {
		values, err := p.serviceNameValues(r.Context(), r.FormValue("query"), r.FormValue("start"), r.FormValue("end"))
		if err != nil {
			status := statusFromUpstreamErr(err)
			p.writeError(w, status, err.Error())
			p.metrics.RecordRequest("label_values", status, time.Since(start))
			return
		}
		p.updateLabelValuesIndex(orgID, labelName, values)
		if p.labelValuesBrowseMode(rawQuery) {
			if indexedValues, ok := p.selectLabelValuesFromIndex(orgID, labelName, search, offset, limit); ok {
				values = indexedValues
			} else {
				values = selectLabelValuesWindow(values, search, offset, limit)
			}
		}
		result := lokiLabelsResponse(values)
		p.setEndpointReadCacheWithTTL("label_values", cacheKey, result, CacheTTLs["label_values"])
		w.Header().Set("Content-Type", "application/json")
		w.Write(result)
		p.metrics.RecordRequest("label_values", http.StatusOK, time.Since(start))
		return
	}

	values, err := p.fetchScopedLabelValues(r.Context(), labelName, rawQuery, r.FormValue("start"), r.FormValue("end"), r.FormValue("limit"), search)
	if err != nil {
		status := statusFromUpstreamErr(err)
		p.writeError(w, status, err.Error())
		p.metrics.RecordRequest("label_values", status, time.Since(start))
		return
	}

	p.updateLabelValuesIndex(orgID, labelName, values)
	if p.labelValuesBrowseMode(rawQuery) {
		if indexedValues, ok := p.selectLabelValuesFromIndex(orgID, labelName, search, offset, limit); ok {
			values = indexedValues
		} else {
			values = selectLabelValuesWindow(values, search, offset, limit)
		}
	} else if search != "" || offset > 0 || rawLimit != "" {
		values = selectLabelValuesWindow(values, search, offset, limit)
	}

	result := lokiLabelsResponse(values)
	p.setEndpointReadCacheWithTTL("label_values", cacheKey, result, CacheTTLs["label_values"])
	w.Header().Set("Content-Type", "application/json")
	w.Write(result)
	p.metrics.RecordRequest("label_values", http.StatusOK, time.Since(start))
}

// handleSeries returns stream/series metadata.
// Loki: GET /loki/api/v1/series?match[]={...}&start=...&end=...
// VL:   GET /select/logsql/streams?query={...}&start=...&end=...
func (p *Proxy) handleSeries(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	if p.handleMultiTenantFanout(w, r, "series", p.handleSeries) {
		return
	}
	r = withOrgID(r)
	matchQueries := r.Form["match[]"]
	query := "*"
	if len(matchQueries) > 0 {
		translated, err := p.translateQueryWithContext(r.Context(), matchQueries[0])
		if err == nil {
			query = translated
		}
	}

	params := url.Values{}
	params.Set("query", query)
	if s := r.FormValue("start"); s != "" {
		params.Set("start", s)
	}
	if e := r.FormValue("end"); e != "" {
		params.Set("end", e)
	}

	coalKey := p.nativeCoalescerKey("series", r.Context(), params)
	status, body, err := p.vlGetCoalescedWithStatus(r.Context(), coalKey, "/select/logsql/streams", params)
	if err != nil {
		httpStatus := statusFromUpstreamErr(err)
		p.writeError(w, httpStatus, err.Error())
		p.metrics.RecordRequest("series", httpStatus, time.Since(start))
		return
	}

	// Propagate VL error status
	if status >= 400 {
		p.writeError(w, status, string(body))
		p.metrics.RecordRequest("series", status, time.Since(start))
		return
	}

	// VL returns: {"values": [{"value": "{stream}", "hits": N}, ...]}
	// Loki expects: {"status": "success", "data": [{"label": "value", ...}, ...]}
	var vlResp struct {
		Values []struct {
			Value string `json:"value"`
			Hits  int64  `json:"hits"`
		} `json:"values"`
	}
	json.Unmarshal(body, &vlResp)

	series := make([]map[string]string, 0, len(vlResp.Values))
	for _, v := range vlResp.Values {
		stream := parseStreamLabels(v.Value)
		if len(stream) == 0 {
			continue
		}
		// Copy before translation/mutation — parseStreamLabels returns a cached map.
		labels := make(map[string]string, len(stream))
		for k, val := range stream {
			labels[k] = val
		}
		labels = p.labelTranslator.TranslateLabelsMap(labels)
		ensureSyntheticServiceName(labels)
		series = append(series, labels)
	}

	type seriesResponse struct {
		Status string              `json:"status"`
		Data   []map[string]string `json:"data"`
	}
	result, _ := json.Marshal(seriesResponse{Status: "success", Data: series})
	w.Header().Set("Content-Type", "application/json")
	w.Write(result)
	p.metrics.RecordRequest("series", http.StatusOK, time.Since(start))
}

// handleIndexStats returns index statistics via VL /select/logsql/hits.
// Loki: GET /loki/api/v1/index/stats?query={...}&start=...&end=...
// Response: {"streams":N, "chunks":N, "entries":N, "bytes":N}
func (p *Proxy) handleIndexStats(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	if p.handleMultiTenantFanout(w, r, "index_stats", p.handleIndexStats) {
		return
	}
	orgID := r.Header.Get("X-Scope-OrgID")
	cacheKey := p.canonicalReadCacheKey("index_stats", orgID, r)
	if cached, _, _, ok := p.endpointReadCacheEntry("index_stats", cacheKey); ok {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(cached)
		p.metrics.RecordRequest("index_stats", http.StatusOK, time.Since(start))
		p.metrics.RecordCacheHit()
		return
	}
	p.metrics.RecordCacheMiss()

	r = withOrgID(r)
	result, err := p.computeIndexStatsResult(r.Context(), r.FormValue("query"), r.FormValue("start"), r.FormValue("end"))
	if err != nil {
		if p.serveStaleReadCacheOnError(w, "index_stats", cacheKey, start, err) {
			return
		}
		status := statusFromUpstreamErr(err)
		p.writeError(w, status, err.Error())
		p.metrics.RecordRequest("index_stats", status, time.Since(start))
		return
	}
	p.setEndpointReadCacheWithTTL("index_stats", cacheKey, result, CacheTTLs["index_stats"])
	w.Header().Set("Content-Type", "application/json")
	w.Write(result)
	p.metrics.RecordRequest("index_stats", http.StatusOK, time.Since(start))
}

func (p *Proxy) computeIndexStatsResult(ctx context.Context, query, start, end string) ([]byte, error) {
	if query == "" {
		query = "*"
	}
	logsqlQuery, _ := p.translateQueryWithContext(ctx, query)

	params := url.Values{}
	params.Set("query", logsqlQuery)
	if s := start; s != "" {
		params.Set("start", formatVLTimestamp(s))
	}
	if e := end; e != "" {
		params.Set("end", formatVLTimestamp(e))
	}
	if params.Get("step") == "" {
		params.Set("step", "1h")
	}

	resp, err := p.vlGet(ctx, "/select/logsql/hits", params)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, _ := readBodyLimited(resp.Body, maxBufferedBackendBodyBytes)
	if resp.StatusCode >= http.StatusBadRequest {
		msg := strings.TrimSpace(string(body))
		if msg == "" {
			msg = fmt.Sprintf("VL backend returned %d", resp.StatusCode)
		}
		return nil, fmt.Errorf("%s", msg)
	}

	entries := sumHitsValues(body)
	hits := parseHits(body)
	streams := len(hits.Hits)
	if streams == 0 && entries > 0 {
		streams = 1
	}
	result, _ := json.Marshal(map[string]interface{}{
		"streams": streams,
		"chunks":  streams,
		"bytes":   entries * 100,
		"entries": entries,
	})
	return result, nil
}

func (p *Proxy) resolveTargetLabelFields(ctx context.Context, targetLabels string, params url.Values) []string {
	fields := splitTargetLabels(targetLabels)
	if len(fields) == 0 {
		return nil
	}

	var (
		available []string
		fetchErr  error
	)

	needsInventory := false
	if p.labelTranslator != nil && p.labelTranslator.style == LabelStyleUnderscores {
		for _, field := range fields {
			translated := p.labelTranslator.ToVL(field)
			if translated == strings.TrimSpace(field) && strings.Contains(field, "_") {
				needsInventory = true
				break
			}
		}
	}

	if needsInventory {
		lookup := url.Values{}
		for _, key := range []string{"query", "start", "end"} {
			if value := strings.TrimSpace(params.Get(key)); value != "" {
				lookup.Set(key, value)
			}
		}
		available, fetchErr = p.fetchPreferredLabelNamesCached(ctx, lookup)
		if fetchErr != nil {
			p.log.Debug("target label inventory lookup failed; falling back to direct mapping", "error", fetchErr)
		}
	}

	if len(available) == 0 {
		available = p.snapshotDeclaredLabelFields()
	}
	available = appendUniqueStrings(available, p.snapshotDeclaredLabelFields()...)

	resolved := make([]string, 0, len(fields))
	for _, field := range fields {
		name := strings.TrimSpace(field)
		if name == "" {
			continue
		}
		mapped := p.labelTranslator.ToVL(name)
		if len(available) > 0 {
			candidates := p.labelTranslator.ResolveLabelCandidates(name, available)
			if len(candidates.candidates) > 0 {
				if candidates.ambiguous {
					p.log.Debug("ambiguous label alias for targetLabels; using first candidate",
						"label", name,
						"candidate_count", len(candidates.candidates),
					)
				}
				mapped = candidates.candidates[0]
			}
		}
		resolved = appendUniqueStrings(resolved, mapped)
	}
	return resolved
}

// handleDetectedFields returns detected field names.
func (p *Proxy) handleDetectedFields(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	if p.handleMultiTenantFanout(w, r, "detected_fields", p.handleDetectedFields) {
		return
	}
	orgID := r.Header.Get("X-Scope-OrgID")
	cacheKey := p.canonicalReadCacheKey("detected_fields", orgID, r)
	if cached, remaining, _, ok := p.endpointReadCacheEntry("detected_fields", cacheKey); ok {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(cached)
		p.metrics.RecordRequest("detected_fields", http.StatusOK, time.Since(start))
		p.metrics.RecordCacheHit()
		if p.shouldRefreshLabelsInBackground(remaining, CacheTTLs["detected_fields"]) {
			p.refreshDetectedFieldsCacheAsync(orgID, cacheKey, r.FormValue("query"), r.FormValue("start"), r.FormValue("end"), parseDetectedLineLimit(r), p.snapshotForwardedAuth(r))
		}
		return
	}
	p.metrics.RecordCacheMiss()

	r = withOrgID(r)
	lineLimit := parseDetectedLineLimit(r)
	fields, _, err := p.detectFields(r.Context(), r.FormValue("query"), r.FormValue("start"), r.FormValue("end"), lineLimit)
	if err != nil {
		if p.serveStaleReadCacheOnError(w, "detected_fields", cacheKey, start, err) {
			return
		}
		status := statusFromUpstreamErr(err)
		p.writeError(w, status, err.Error())
		p.metrics.RecordRequest("detected_fields", status, time.Since(start))
		return
	}
	payload := map[string]interface{}{
		"status": "success",
		"data":   fields,
		"fields": fields,
		"limit":  lineLimit,
	}
	p.setEndpointJSONCacheWithTTL("detected_fields", cacheKey, CacheTTLs["detected_fields"], payload)
	p.writeJSON(w, payload)
	p.metrics.RecordRequest("detected_fields", http.StatusOK, time.Since(start))
}

// handleDetectedFieldValues returns values for a detected field.
// Loki: GET /loki/api/v1/detected_field/{name}/values?query=...
// Response: {"values":["debug","info","warn","error"],"limit":1000}
func (p *Proxy) handleDetectedFieldValues(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	if p.handleMultiTenantFanout(w, r, "detected_field_values", p.handleDetectedFieldValues) {
		return
	}
	orgID := r.Header.Get("X-Scope-OrgID")
	r = withOrgID(r)
	// Extract field name from URL: /loki/api/v1/detected_field/{name}/values
	path := r.URL.Path
	parts := strings.Split(path, "/")
	fieldName := ""
	for i, part := range parts {
		if part == "detected_field" && i+1 < len(parts) {
			fieldName = parts[i+1]
			break
		}
	}
	if fieldName == "" {
		p.writeError(w, http.StatusBadRequest, "missing field name in URL")
		return
	}

	lineLimit := parseDetectedLineLimit(r)
	cacheKey := p.canonicalReadCacheKey("detected_field_values", orgID, r, fieldName)
	if cached, remaining, _, ok := p.endpointReadCacheEntry("detected_field_values", cacheKey); ok {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(cached)
		p.metrics.RecordRequest("detected_field_values", http.StatusOK, time.Since(start))
		p.metrics.RecordCacheHit()
		if p.shouldRefreshLabelsInBackground(remaining, CacheTTLs["detected_field_values"]) {
			p.refreshDetectedFieldValuesCacheAsync(orgID, cacheKey, fieldName, r.FormValue("query"), r.FormValue("start"), r.FormValue("end"), lineLimit, p.snapshotForwardedAuth(r))
		}
		return
	}
	p.metrics.RecordCacheMiss()

	if fieldName == "service_name" {
		if values, err := p.serviceNameValues(r.Context(), r.FormValue("query"), r.FormValue("start"), r.FormValue("end")); err == nil && len(values) > 0 {
			payload := map[string]interface{}{
				"status": "success",
				"data":   values,
				"values": values,
				"limit":  lineLimit,
			}
			p.setEndpointJSONCacheWithTTL("detected_field_values", cacheKey, CacheTTLs["detected_field_values"], payload)
			p.writeJSON(w, payload)
			p.metrics.RecordRequest("detected_field_values", http.StatusOK, time.Since(start))
			return
		}
	}

	values, err := p.resolveDetectedFieldValues(r.Context(), fieldName, r.FormValue("query"), r.FormValue("start"), r.FormValue("end"), lineLimit, true)
	if err != nil {
		if p.serveStaleReadCacheOnError(w, "detected_field_values", cacheKey, start, err) {
			return
		}
		status := statusFromUpstreamErr(err)
		p.writeError(w, status, err.Error())
		p.metrics.RecordRequest("detected_field_values", status, time.Since(start))
		return
	}

	payload := map[string]interface{}{
		"status": "success",
		"data":   values,
		"values": values,
		"limit":  lineLimit,
	}
	p.setEndpointJSONCacheWithTTL("detected_field_values", cacheKey, CacheTTLs["detected_field_values"], payload)
	p.writeJSON(w, payload)
	p.metrics.RecordRequest("detected_field_values", http.StatusOK, time.Since(start))
}

func (p *Proxy) detectedLabelValuesForField(ctx context.Context, fieldName, query, start, end string, lineLimit int) []string {
	_, summaries, err := p.detectLabels(ctx, query, start, end, lineLimit)
	if err != nil || summaries == nil {
		return nil
	}

	available := make([]string, 0, len(summaries))
	for name := range summaries {
		available = append(available, name)
	}
	resolution := p.labelTranslator.ResolveLabelCandidates(fieldName, available)
	if len(resolution.candidates) == 0 {
		resolution = fieldResolution{candidates: []string{fieldName}}
	}

	valueSet := make(map[string]struct{})
	for _, candidate := range resolution.candidates {
		summary := summaries[candidate]
		if summary == nil {
			continue
		}
		for value := range summary.values {
			if strings.TrimSpace(value) == "" {
				continue
			}
			valueSet[value] = struct{}{}
		}
	}
	if len(valueSet) == 0 {
		return nil
	}

	values := make([]string, 0, len(valueSet))
	for value := range valueSet {
		values = append(values, value)
	}
	sort.Strings(values)
	return values
}

