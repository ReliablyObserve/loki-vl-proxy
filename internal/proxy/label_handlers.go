package proxy

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	logqlpkg "github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
)

// handleLabels returns label names.
// Loki: GET /loki/api/v1/labels?start=...&end=...
// VL:   GET /select/logsql/stream_field_names?query=*&start=...&end=...
//
//	fallback /select/logsql/field_names for older backends
func (p *Proxy) handleLabels(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	if p.handleMultiTenantFanout(w, r, "labels") {
		return
	}
	orgID := r.Header.Get("X-Scope-OrgID")
	cacheKey := p.canonicalReadCacheKey("labels", orgID, r)

	labelsTTL := metadataWindowTTL(r.FormValue("start"), r.FormValue("end"), p.cacheTTLLabels)
	if cached, remaining, _, ok := p.endpointReadCacheEntry("labels", cacheKey); ok {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(cached)
		p.metrics.RecordRequest("labels", http.StatusOK, time.Since(start))
		p.metrics.RecordCacheHit()
		if !metadataListPayloadEmpty(cached) && p.shouldRefreshLabelsInBackground(remaining, labelsTTL) {
			search := strings.TrimSpace(r.FormValue("search"))
			if search == "" {
				search = strings.TrimSpace(r.FormValue("q"))
			}
			p.refreshLabelsCacheAsync(orgID, cacheKey, r.FormValue("query"), r.FormValue("start"), r.FormValue("end"), search, p.snapshotForwardedAuth(r))
		}
		return
	}
	p.metrics.RecordCacheMiss()
	r = p.withRequestScope(r)

	search := strings.TrimSpace(r.FormValue("search"))
	if search == "" {
		search = strings.TrimSpace(r.FormValue("q"))
	}

	labels, err := p.fetchScopedLabelNames(r.Context(), r.FormValue("query"), r.FormValue("start"), r.FormValue("end"), search, true)
	if err != nil {
		// Last known-good full-range answer, if any; otherwise the error, never a
		// capped partial list.
		if p.serveStaleReadCacheOnError(w, "labels", cacheKey, start, err) {
			return
		}
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

	// The fetch above covers the full requested [start, end] range, so this first
	// response is already complete; no follow-up refresh is needed.
	result := lokiLabelsResponse(labels)
	if len(labels) == 0 {
		p.setMetadataListCache("labels", cacheKey, result, 0, labelsTTL)
	} else {
		p.mergeLabelsIntoCache("labels", cacheKey, labels, labelsTTL)
	}
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
	if p.handleMultiTenantFanout(w, r, "label_values") {
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
	// Loki answers /label/{name}/values with every value it knows. The browse
	// window (limit, offset, search) is a proxy extension, so only a request
	// that asks for one gets one: -label-values-hot-limit sizes the indexed
	// browse cache, it never truncates a plain Loki request.
	browseWindow := strings.TrimSpace(rawLimit) != "" || strings.TrimSpace(search) != "" || offset > 0
	limit := p.limits().EntriesPerQuery
	if browseWindow {
		limit = p.defaultLabelValuesLimit(rawLimit)
	}

	labelValuesTTL := metadataWindowTTL(r.FormValue("start"), r.FormValue("end"), p.cacheTTLLabelValues)
	if cached, remaining, _, ok := p.endpointReadCacheEntry("label_values", cacheKey); ok {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(cached)
		p.metrics.RecordRequest("label_values", http.StatusOK, time.Since(start))
		p.metrics.RecordCacheHit()
		if !metadataListPayloadEmpty(cached) && p.shouldRefreshLabelsInBackground(remaining, labelValuesTTL) {
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
	r = p.withRequestScope(r)

	if browseWindow && p.labelValuesBrowseMode(rawQuery) {
		if indexedValues, ok := p.selectLabelValuesFromIndex(p.scopedIndexOrg(r, orgID), labelName, search, offset, limit); ok {
			result := lokiLabelsResponse(indexedValues)
			p.setMetadataListCache("label_values", cacheKey, result, len(indexedValues), labelValuesTTL)
			w.Header().Set("Content-Type", "application/json")
			w.Write(result)
			p.metrics.RecordRequest("label_values", http.StatusOK, time.Since(start))
			return
		}
	}

	if labelName == "service_name" {
		values, err := p.serviceNameValues(r.Context(), r.FormValue("query"), r.FormValue("start"), r.FormValue("end"))
		if err != nil {
			if p.serveStaleReadCacheOnError(w, "label_values", cacheKey, start, err) {
				return
			}
			status := statusFromUpstreamErr(err)
			p.writeError(w, status, err.Error())
			p.metrics.RecordRequest("label_values", status, time.Since(start))
			return
		}
		p.updateLabelValuesIndex(p.scopedIndexOrg(r, orgID), labelName, values)
		if browseWindow && p.labelValuesBrowseMode(rawQuery) {
			if indexedValues, ok := p.selectLabelValuesFromIndex(p.scopedIndexOrg(r, orgID), labelName, search, offset, limit); ok {
				values = indexedValues
			} else {
				values = selectLabelValuesWindow(values, search, offset, limit, p.limits().EntriesPerQuery)
			}
		}
		result := lokiLabelsResponse(values)
		p.setMetadataListCache("label_values", cacheKey, result, len(values), labelValuesTTL)
		w.Header().Set("Content-Type", "application/json")
		w.Write(result)
		p.metrics.RecordRequest("label_values", http.StatusOK, time.Since(start))
		return
	}

	values, err := p.fetchScopedLabelValues(r.Context(), labelName, rawQuery, r.FormValue("start"), r.FormValue("end"), r.FormValue("limit"), search)
	if err != nil {
		// Last known-good full-range answer, if any; otherwise the error.
		if p.serveStaleReadCacheOnError(w, "label_values", cacheKey, start, err) {
			return
		}
		status := statusFromUpstreamErr(err)
		p.writeError(w, status, err.Error())
		p.metrics.RecordRequest("label_values", status, time.Since(start))
		return
	}

	p.updateLabelValuesIndex(p.scopedIndexOrg(r, orgID), labelName, values)
	if browseWindow {
		if indexedValues, ok := p.selectLabelValuesFromIndex(p.scopedIndexOrg(r, orgID), labelName, search, offset, limit); p.labelValuesBrowseMode(rawQuery) && ok {
			values = indexedValues
		} else {
			values = selectLabelValuesWindow(values, search, offset, limit, p.limits().EntriesPerQuery)
		}
	}

	result := lokiLabelsResponse(values)
	p.setMetadataListCache("label_values", cacheKey, result, len(values), labelValuesTTL)
	w.Header().Set("Content-Type", "application/json")
	w.Write(result)
	p.metrics.RecordRequest("label_values", http.StatusOK, time.Since(start))
}

// metadataNegativeCacheTTL is the floor of the TTL for an empty /labels or
// /label/{name}/values answer (and an empty VictoriaLogs field-name list). It
// absorbs repeated polling of idle tenants or selectors without data (Grafana
// autocomplete) without full-range backend scans on every request, while data
// that arrives later becomes visible within this TTL. The TTL actually used is
// effectiveMetadataNegativeTTL.
const metadataNegativeCacheTTL = 30 * time.Second

// effectiveMetadataNegativeTTL returns max(metadataNegativeCacheTTL, minimums).
// Callers pass the disk minimum write TTL (-disk-cache-min-ttl) and the peer
// write-through minimum TTL (-peer-write-through-min-ttl). Both gates are
// inclusive, so an empty answer with this TTL goes through the same write paths
// as a non-empty one and overwrites an earlier non-empty copy of the key in
// memory, on disk and on the owner peer. A shorter TTL would be skipped by those
// tiers, and their older non-empty copy would be served again once the empty
// entry expired.
func effectiveMetadataNegativeTTL(minimums ...time.Duration) time.Duration {
	ttl := metadataNegativeCacheTTL
	for _, minimum := range minimums {
		if minimum > ttl {
			ttl = minimum
		}
	}
	return ttl
}

// metadataNegativeTTL returns the TTL for empty label lists derived at
// construction, or the floor for a Proxy built without New.
func (p *Proxy) metadataNegativeTTL() time.Duration {
	if p == nil || p.metadataNegativeCacheTTL <= 0 {
		return metadataNegativeCacheTTL
	}
	return p.metadataNegativeCacheTTL
}

// setMetadataListCache caches a /labels or /label/{name}/values response body
// holding n items through the endpoint's normal write path: for ttl when
// non-empty, for the negative TTL (metadataNegativeTTL) when empty.
func (p *Proxy) setMetadataListCache(endpoint, cacheKey string, body []byte, n int, ttl time.Duration) {
	if n == 0 {
		ttl = p.metadataNegativeTTL()
	}
	p.setEndpointReadCacheWithTTL(endpoint, cacheKey, body, ttl)
}

// metadataListPayloadEmpty reports whether a Loki label-list body carries no
// items ({"data":[]} or {"data":null}). Such negative entries are short-lived
// and are never refreshed in the background.
func metadataListPayloadEmpty(body []byte) bool {
	if len(body) > 64 {
		return false
	}
	return bytes.Contains(body, []byte(`"data":[]`)) || bytes.Contains(body, []byte(`"data":null`))
}

// handleSeries returns stream/series metadata.
// Loki: GET /loki/api/v1/series?match[]={...}&start=...&end=...
// VL:   GET /select/logsql/streams?query={...}&start=...&end=...
func (p *Proxy) handleSeries(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	if p.handleMultiTenantFanout(w, r, "series") {
		return
	}
	r = p.withRequestScope(r)
	// Loki reads both match and match[] (loghttp.ParseSeriesQuery).
	matchQueries := append(append([]string(nil), r.Form["match[]"]...), r.Form["match"]...)
	query := "*"
	if len(matchQueries) > 0 {
		translated, err := p.translateQueryWithContext(r.Context(), matchQueries[0])
		if err != nil {
			p.writeError(w, http.StatusBadRequest, err.Error())
			p.metrics.RecordRequest("series", http.StatusBadRequest, time.Since(start))
			return
		}
		query = translated
	}

	params := url.Values{}
	params.Set("query", query)
	startRaw := r.FormValue("start")
	endRaw := r.FormValue("end")
	// Apply the default-lookback guard via the shared helper so /series stays
	// in lockstep with /labels and /label/{name}/values. See
	// applyDefaultMetadataLookback for semantics.
	startRaw, endRaw = applyDefaultMetadataLookback(startRaw, endRaw, p.metadataDefaultLookback)
	if startRaw != "" {
		params.Set("start", startRaw)
	}
	if endRaw != "" {
		params.Set("end", endRaw)
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
		code := p.writeBackendError(w, status, body)
		p.metrics.RecordRequest("series", code, time.Since(start))
		return
	}

	// VL returns: {"values": [{"value": "{stream}", "hits": N}, ...]}
	// Loki expects: {"status": "success", "data": [{"label": "value", ...}, ...]}
	//
	// Use fastjson to parse the VL response and write the Loki response directly
	// to avoid encoding/json reflection overhead (mapEncoder + sorting per entry).
	// fjRoot and every value read from it live in fjp's buffers, so the parser
	// goes back to the pool only after the response has been built.
	fjp := vlFJParserPool.Get()
	fjRoot, err := fjp.ParseBytes(body)

	sb := jsonBuilderPool.Get().(*strings.Builder)
	sb.Reset()
	sb.WriteString(`{"status":"success","data":[`)
	first := true

	if err == nil {
		arr := fjRoot.GetArray("values")
		// Pre-grow: 32 (header+footer) + ~80 bytes per stream (labels JSON).
		if need := 32 + len(arr)*80; sb.Cap() < need {
			sb.Grow(need)
		}
		// Get a pooled keys slice (outside the loop to reuse across all stream entries)
		kp := seriesKeysPool.Get().(*[]string)
		keys := (*kp)[:0]
		for _, item := range arr {
			streamStr := string(item.GetStringBytes("value"))
			stream := parseStreamLabels(streamStr)
			if len(stream) == 0 {
				continue
			}
			labels := p.labelTranslator.TranslateLabelsMap(stream)
			ensureSyntheticServiceName(labels)

			// Reuse the keys slice (reset length, keep capacity)
			keys = keys[:0]
			for k := range labels {
				keys = append(keys, k)
			}
			sort.Strings(keys)

			if !first {
				sb.WriteByte(',')
			}
			first = false
			sb.WriteByte('{')
			for i, k := range keys {
				if i > 0 {
					sb.WriteByte(',')
				}
				appendJSONStringToBuilder(sb, k)
				sb.WriteByte(':')
				appendJSONStringToBuilder(sb, labels[k])
			}
			sb.WriteByte('}')
		}
		// Save potentially-grown slice back to pool (after the loop)
		*kp = keys
		seriesKeysPool.Put(kp)
	}
	vlFJParserPool.Put(fjp)
	sb.WriteString(`]}`)
	result := sb.String()
	jsonBuilderPool.Put(sb)
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(result))
	p.metrics.RecordRequest("series", http.StatusOK, time.Since(start))
}

// handleIndexStats returns index statistics via VL /select/logsql/hits.
// Loki: GET /loki/api/v1/index/stats?query={...}&start=...&end=...
// Response: {"streams":N, "chunks":N, "entries":N, "bytes":N}
func (p *Proxy) handleIndexStats(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	if p.handleMultiTenantFanout(w, r, "index_stats") {
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

	r = p.withRequestScope(r)
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
	logsqlQuery, err := p.translateQueryWithContext(ctx, query)
	if err != nil {
		return nil, err
	}

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
	body, _ := readBodyLimited(resp.Body, int64(p.limits().BufferedBackendBodyBytes))
	if resp.StatusCode >= http.StatusBadRequest {
		return nil, p.redactedBackendStatusError("", resp.StatusCode, body)
	}

	entries := sumHitsValues(body)
	hits := parseHits(body)
	streams := len(hits.Hits)
	if streams == 0 && entries > 0 {
		streams = 1
	}
	result := []byte(fmt.Sprintf(`{"streams":%d,"chunks":%d,"bytes":%d,"entries":%d}`, streams, streams, entries*100, entries))
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
		// For underscore proxy: when a label is a known OTel semantic convention
		// (e.g. service_name → service.name via knownUnderscoreToDot), also query
		// the underscore form as a fallback. Loki-push data stores stream labels
		// under underscore names; OTel data uses dotted names. VL returns hits for
		// whichever field exists; TranslateLabelsMap coalesces by preferring the
		// non-empty value. Applies only to knownUnderscoreToDot entries — not to
		// custom fields resolved via inventory (those are stored as dotted OTel).
		if p.labelTranslator != nil && p.labelTranslator.style == LabelStyleUnderscores {
			if _, isKnownOTel := knownUnderscoreToDot[name]; isKnownOTel {
				// When OTel translation is enabled, the primary form is dotted;
				// add the underscore form as a fallback for Loki-push data.
				// When disabled, ToVL already returns the underscore form — skip.
				if p.labelTranslator.translateOTel {
					resolved = appendUniqueStrings(resolved, name)
				}
			}
		}
	}
	return resolved
}

// handleDetectedFields returns detected field names.
func (p *Proxy) handleDetectedFields(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	if p.handleMultiTenantFanout(w, r, "detected_fields") {
		return
	}
	orgID := r.Header.Get("X-Scope-OrgID")
	cacheKey := p.canonicalReadCacheKey("detected_fields", orgID, r)
	detectedFieldsTTL := metadataWindowTTL(r.FormValue("start"), r.FormValue("end"), CacheTTLs["detected_fields"])
	if cached, remaining, _, ok := p.endpointReadCacheEntry("detected_fields", cacheKey); ok {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(cached)
		p.metrics.RecordRequest("detected_fields", http.StatusOK, time.Since(start))
		p.metrics.RecordCacheHit()
		if p.shouldRefreshLabelsInBackground(remaining, detectedFieldsTTL) {
			p.refreshDetectedFieldsCacheAsync(orgID, cacheKey, r.FormValue("query"), r.FormValue("start"), r.FormValue("end"), parseDetectedLineLimit(r), p.snapshotForwardedAuth(r))
		}
		return
	}
	p.metrics.RecordCacheMiss()

	r = p.withRequestScope(r)
	lineLimit := parseDetectedLineLimit(r)
	query := r.FormValue("query")
	// Bound the slow log-scan path so a single Drilldown panel never blocks
	// past drilldownScanTimeout. On timeout we return an empty fields list
	// — Drilldown shows "no fields" rather than spinning forever, and the
	// empty-cache guard prevents the empty result from sticking.
	scanCtx, scanCancel := p.withDrilldownScanTimeout(r.Context())
	defer scanCancel()
	fields, _, err := p.detectFields(scanCtx, query, r.FormValue("start"), r.FormValue("end"), lineLimit)
	if err != nil {
		if isContextDeadlineErr(err) {
			p.observeInternalOperation(r.Context(), "drilldown_scan_timeout", "detected_fields_handler", 0)
			fields = nil
		} else if p.serveStaleReadCacheOnError(w, "detected_fields", cacheKey, start, err) {
			return
		} else {
			status := statusFromUpstreamErr(err)
			p.writeError(w, status, err.Error())
			p.metrics.RecordRequest("detected_fields", status, time.Since(start))
			return
		}
	}
	// When the strict query (full LogQL including parser stages and field filters)
	// returns zero fields, fall back to a native-only index lookup on the bare stream
	// selector. This keeps the Drilldown fields panel populated when a specific field
	// value filter narrows the log sample below the scan threshold.
	// We use native-only (no log-line scan) to avoid returning every field from a
	// broad relaxed scan, which would produce an overwhelming list.
	if len(fields) == 0 {
		if relaxed := relaxedFieldDetectionQuery(query); relaxed != "" && relaxed != query {
			if relaxedFields, _ := p.detectFieldsNativeOnly(r.Context(), relaxed, r.FormValue("start"), r.FormValue("end")); len(relaxedFields) > 0 {
				fields = relaxedFields
			}
		}
	}
	payload := map[string]interface{}{
		"status": "success",
		"data":   fields,
		"fields": fields,
		// Loki always returns 1000 here regardless of the requested line_limit.
		// Drilldown reads this value and displays it as "Fields N" in the UI.
		// Echoing the requested line_limit causes "Fields 5,951" etc. to appear.
		"limit": 1000,
	}
	p.setEndpointJSONCacheWithTTL("detected_fields", cacheKey, detectedFieldsTTL, payload)
	p.writeJSON(w, payload)
	p.metrics.RecordRequest("detected_fields", http.StatusOK, time.Since(start))
}

// handleDetectedFieldValues returns values for a detected field.
// Loki: GET /loki/api/v1/detected_field/{name}/values?query=...
// Response: {"values":["debug","info","warn","error"],"limit":1000}
func (p *Proxy) handleDetectedFieldValues(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	if p.handleMultiTenantFanout(w, r, "detected_field_values") {
		return
	}
	orgID := r.Header.Get("X-Scope-OrgID")
	r = p.withRequestScope(r)
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
	detectedFieldValuesTTL := metadataWindowTTL(r.FormValue("start"), r.FormValue("end"), CacheTTLs["detected_field_values"])
	if cached, remaining, _, ok := p.endpointReadCacheEntry("detected_field_values", cacheKey); ok {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(cached)
		p.metrics.RecordRequest("detected_field_values", http.StatusOK, time.Since(start))
		p.metrics.RecordCacheHit()
		if p.shouldRefreshLabelsInBackground(remaining, detectedFieldValuesTTL) {
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
			p.setEndpointJSONCacheWithTTL("detected_field_values", cacheKey, detectedFieldValuesTTL, payload)
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
	// Skip caching empty value lists — see setCachedDetectedFields rationale.
	// Drilldown otherwise shows "No values" for the full TTL even after data appears.
	if len(values) > 0 {
		p.setEndpointJSONCacheWithTTL("detected_field_values", cacheKey, detectedFieldValuesTTL, payload)
	}
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

// lokiQueryParamRule describes how Loki v3.7 parses the selector parameter of a
// read endpoint before doing any work.
type lokiQueryParamRule int

const (
	// syntax.ParseMatchers(query, true) when query is set (/labels,
	// /label/<name>/values, /detected_labels).
	lokiMatchersOptional lokiQueryParamRule = iota + 1
	// syntax.ParseMatchers(query, true); a missing query is a parse error
	// (/index/stats, /patterns).
	lokiMatchersRequired
	// Like lokiMatchersRequired, but the literal "{}" means every stream
	// (seriesvolume.MatchAny on /index/volume and /index/volume_range).
	lokiVolumeMatchers
	// Every match[] value is a bare selector; {} selects all series (/series).
	lokiSeriesMatchers
	// syntax.ParseLogSelector(query, true) plus pipeline construction; a missing
	// query is a parse error (/detected_fields, /detected_field/<name>/values).
	lokiLogSelectorRequired
	// syntax.ParseExpr(query) before the websocket upgrade (/tail).
	lokiTailQuery
)

var lokiQueryParamRules = map[string]lokiQueryParamRule{
	"labels":                lokiMatchersOptional,
	"label_values":          lokiMatchersOptional,
	"detected_labels":       lokiMatchersOptional,
	"index_stats":           lokiMatchersRequired,
	"patterns":              lokiMatchersRequired,
	"volume":                lokiVolumeMatchers,
	"volume_range":          lokiVolumeMatchers,
	"series":                lokiSeriesMatchers,
	"detected_fields":       lokiLogSelectorRequired,
	"detected_field_values": lokiLogSelectorRequired,
	"tail":                  lokiTailQuery,
}

// lokiEmptyQueryError is what Loki's parser reports for a missing query.
const lokiEmptyQueryError = "parse error : syntax error: unexpected $end"

// cachedSelectorValidation memoizes a deterministic selector validation in the
// bounded validationCache shared with validateLogQLSyntax. The NUL-delimited
// kind prefix keeps each validator's results apart from raw query keys. Only
// short queries are cached (validationCacheMaxQueryBytes), so the cache holds
// at most validationCacheMaxSize small entries.
func cachedSelectorValidation(kind, query string, validate func(string) string) string {
	if len(query) > validationCacheMaxQueryBytes {
		return validate(query)
	}
	key := "\x00" + kind + "\x00" + query
	if v, ok := validationCache.Load(key); ok {
		return v.(string)
	}
	result := validate(query)
	storeValidationResult(key, result)
	return result
}

// queryLengthError rejects an oversized query before it is parsed or cached:
// Loki's "input size too long" at or above syntax.maxInputSize, the proxy's
// maxQueryLength message for anything longer than that limit.
func queryLengthError(query string, maxBytes int) string {
	if msg := logqlpkg.InputSizeError(query); msg != "" {
		return msg
	}
	if maxBytes > 0 && len(query) > maxBytes {
		return fmt.Sprintf("query exceeds max length (%d > %d); raise -max-query-length-bytes", len(query), maxBytes)
	}
	return ""
}

// maxEchoedErrorBytes bounds how much of a rejected query a validation error
// echoes into the response body and the request log.
const maxEchoedErrorBytes = 2048

// truncateQueryError shortens an error message that embeds query text to
// maxEchoedErrorBytes, cutting on a UTF-8 boundary.
func truncateQueryError(msg string) string {
	if len(msg) <= maxEchoedErrorBytes {
		return msg
	}
	cut := maxEchoedErrorBytes
	for cut > 0 && !utf8.RuneStart(msg[cut]) {
		cut--
	}
	return msg[:cut] + "... (truncated)"
}

// lokiQueryParamError returns Loki's 400 message for an invalid selector
// parameter on endpoint, or "" when Loki would accept the request.
func lokiQueryParamError(rule lokiQueryParamRule, r *http.Request, maxQueryBytes int) string {
	query := r.FormValue("query")
	if rule == lokiSeriesMatchers {
		// Loki reads both match and match[] (loghttp.ParseSeriesQuery).
		groups := append(append([]string(nil), r.Form["match"]...), r.Form["match[]"]...)
		for _, group := range groups {
			if msg := queryLengthError(group, maxQueryBytes); msg != "" {
				return msg
			}
		}
		return logqlpkg.ValidateSeriesMatchers(groups)
	}
	if msg := queryLengthError(query, maxQueryBytes); msg != "" {
		return msg
	}
	switch rule {
	case lokiMatchersOptional:
		if query == "" {
			return ""
		}
		return cachedSelectorValidation("matchers", query, logqlpkg.ValidateMatchersQuery)
	case lokiMatchersRequired, lokiVolumeMatchers:
		if query == "" {
			return lokiEmptyQueryError
		}
		if rule == lokiVolumeMatchers && query == "{}" {
			return ""
		}
		return cachedSelectorValidation("matchers", query, logqlpkg.ValidateMatchersQuery)
	case lokiLogSelectorRequired:
		return cachedSelectorValidation("log_selector", query, logqlpkg.ValidateLogSelectorQuery)
	case lokiTailQuery:
		return validateLogQLSyntax(query)
	}
	return ""
}

// lokiQueryParamValidation rejects requests whose selector parameter Loki
// answers with 400 bad_data (a non-selector expression, an empty-compatible
// selector such as {app=""}, an oversized input or a parse error) before cache
// lookup, tenant fan-out or any VictoriaLogs call. Endpoints without a rule
// pass through.
func (p *Proxy) lokiQueryParamValidation(endpoint string, next http.HandlerFunc) http.HandlerFunc {
	rule, ok := lokiQueryParamRules[endpoint]
	if !ok {
		return next
	}
	return func(w http.ResponseWriter, r *http.Request) {
		if msg := lokiQueryParamError(rule, r, p.limits().QueryLengthBytes); msg != "" {
			p.writeError(w, http.StatusBadRequest, truncateQueryError(msg))
			p.metrics.RecordRequest(endpoint, http.StatusBadRequest, 0)
			return
		}
		next(w, r)
	}
}
