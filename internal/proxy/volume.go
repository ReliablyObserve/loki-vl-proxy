package proxy

import (
	"context"
	stdjson "encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	fj "github.com/valyala/fastjson"
)

const defaultVolumeSeriesLimit = 1000

func sumHitsValues(body []byte) int {
	hits := parseHits(body)
	total := 0
	for _, h := range hits.Hits {
		for _, v := range h.Values {
			total += v
		}
	}
	return total
}

func (p *Proxy) translateVolumeMetric(fields map[string]string) map[string]string {
	if fields == nil {
		return nil
	}
	translated := fields
	if p != nil && p.labelTranslator != nil && !p.labelTranslator.IsPassthrough() {
		translated = p.labelTranslator.TranslateLabelsMap(fields)
	}
	if translated == nil {
		return nil
	}
	serviceSignal := hasServiceSignal(translated)
	ensureSyntheticServiceName(translated)
	if !serviceSignal && strings.TrimSpace(translated["service_name"]) == unknownServiceName {
		delete(translated, "service_name")
	}
	return translated
}

func (p *Proxy) hitsToVolumeVector(body []byte, targetLabels string) map[string]interface{} {
	hits := parseHits(body)
	targets := splitTargetLabels(targetLabels)
	result := make([]map[string]interface{}, 0, len(hits.Hits))
	for _, h := range hits.Hits {
		total := 0
		var lastTS float64
		for i, v := range h.Values {
			total += v
			if i < len(h.Timestamps) {
				lastTS = parseTimestampToUnix(string(h.Timestamps[i]))
			}
		}
		translated := p.translateVolumeMetric(h.Fields)
		// Filter metric to only the requested target labels so that extra stream
		// labels (e.g. service_name returned alongside container by VL) don't leak
		// into the response. Drilldown include/exclude breaks when the metric has
		// more keys than the label being explored.
		if len(targets) > 0 {
			translated = buildVolumeMetric(translated, targets)
		}
		result = append(result, map[string]interface{}{
			"metric": translated,
			"value":  []interface{}{lastTS, strconv.Itoa(total)},
		})
	}
	return map[string]interface{}{
		"status": "success",
		"data": map[string]interface{}{
			"resultType": "vector",
			"result":     result,
		},
	}
}

func (p *Proxy) hitsToVolumeMatrix(body []byte, targetLabels, start, end, step string, limit int) map[string]interface{} {
	hits := parseHits(body)
	targets := splitTargetLabels(targetLabels)

	// Apply series limit: sort by total count descending, take top limit.
	// Matches Loki volume_range behavior where limit (default 1000) caps results.
	if limit <= 0 {
		limit = defaultVolumeSeriesLimit
	}
	if len(hits.Hits) > limit {
		sort.Slice(hits.Hits, func(i, j int) bool {
			return hits.Hits[i].Total > hits.Hits[j].Total
		})
		hits.Hits = hits.Hits[:limit]
	}
	bucketRange, fillMissing := parseRequestedBucketRange(start, end, step)
	result := make([]map[string]interface{}, 0, len(hits.Hits))
	for _, h := range hits.Hits {
		seriesFill := fillMissing
		if seriesFill {
			counts := make(map[int64]int, len(h.Timestamps))
			mappedSamples := 0
			for i, ts := range h.Timestamps {
				if i >= len(h.Values) {
					continue
				}
				parsedTS, ok := parseFlexibleUnixSeconds(string(ts))
				if !ok {
					continue
				}
				bucket, ok := bucketRange.bucketFor(parsedTS)
				if !ok {
					continue
				}
				counts[bucket] += h.Values[i]
				mappedSamples++
			}
			if mappedSamples == 0 && len(h.Timestamps) > 0 {
				seriesFill = false
			}
			if seriesFill {
				values := make([][]interface{}, 0, bucketRange.count)
				for ts := bucketRange.start; ts <= bucketRange.end; ts += bucketRange.step {
					values = append(values, []interface{}{float64(ts), strconv.Itoa(counts[ts])})
				}
				translated := p.translateVolumeMetric(h.Fields)
				if len(targets) > 0 {
					translated = buildVolumeMetric(translated, targets)
				}
				result = append(result, map[string]interface{}{
					"metric": translated,
					"values": values,
				})
				continue
			}
		}

		values := make([][]interface{}, 0, len(h.Timestamps))
		for i, ts := range h.Timestamps {
			val := 0
			if i < len(h.Values) {
				val = h.Values[i]
			}
			values = append(values, []interface{}{parseTimestampToUnix(string(ts)), strconv.Itoa(val)})
		}
		translated := p.translateVolumeMetric(h.Fields)
		if len(targets) > 0 {
			translated = buildVolumeMetric(translated, targets)
		}
		result = append(result, map[string]interface{}{
			"metric": translated,
			"values": values,
		})
	}
	return map[string]interface{}{
		"status": "success",
		"data": map[string]interface{}{
			"resultType": "matrix",
			"result":     result,
		},
	}
}

func normalizeDrilldownGroupingLabel(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	switch strings.ToLower(raw) {
	case "$__all", "__all":
		return ""
	default:
		return raw
	}
}

func requestedVolumeTargetLabels(r *http.Request) string {
	if r == nil {
		return ""
	}
	if direct := strings.TrimSpace(r.FormValue("targetLabels")); direct != "" {
		return direct
	}
	for _, key := range []string{"drillDownLabel", "fieldBy", "labelBy", "var-fieldBy", "var-labelBy"} {
		if candidate := normalizeDrilldownGroupingLabel(r.FormValue(key)); candidate != "" {
			return candidate
		}
	}
	return ""
}

// handleVolume returns volume data via VL /select/logsql/hits with field grouping.
// Loki: GET /loki/api/v1/index/volume?query={...}&start=...&end=...
// Response: {"status":"success","data":{"resultType":"vector","result":[{"metric":{...},"value":[ts,"count"]}]}}
func (p *Proxy) handleVolume(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	if p.handleMultiTenantFanout(w, r, "volume") {
		return
	}
	r = withOrgID(r)
	orgID := r.Header.Get("X-Scope-OrgID")
	query := r.FormValue("query")
	startParam := strings.TrimSpace(firstNonEmpty(r.FormValue("start"), r.FormValue("from")))
	endParam := strings.TrimSpace(firstNonEmpty(r.FormValue("end"), r.FormValue("to")))
	targetLabels := requestedVolumeTargetLabels(r)
	cacheKey := p.canonicalReadCacheKey("volume", orgID, r)
	if cached, remaining, _, ok := p.endpointReadCacheEntry("volume", cacheKey); ok {
		if !p.shouldBypassRecentTailCache("volume", remaining, r) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write(cached)
			p.metrics.RecordRequest("volume", http.StatusOK, time.Since(start))
			p.metrics.RecordCacheHit()
			if p.shouldRefreshLabelsInBackground(remaining, CacheTTLs["volume"]) {
				p.refreshVolumeCacheAsync(orgID, cacheKey, query, startParam, endParam, targetLabels, p.snapshotForwardedAuth(r))
			}
			return
		}
	}
	p.metrics.RecordCacheMiss()

	result, err := p.computeVolumeResult(r.Context(), query, startParam, endParam, targetLabels)
	if err != nil {
		if p.serveStaleReadCacheOnError(w, "volume", cacheKey, start, err) {
			return
		}
		status := statusFromUpstreamErr(err)
		p.writeError(w, status, err.Error())
		p.metrics.RecordRequest("volume", status, time.Since(start))
		return
	}
	p.setEndpointJSONCacheWithTTL("volume", cacheKey, CacheTTLs["volume"], result)
	p.writeJSON(w, result)
	p.metrics.RecordRequest("volume", http.StatusOK, time.Since(start))
}

func (p *Proxy) computeVolumeResult(ctx context.Context, query, start, end, targetLabels string) (map[string]interface{}, error) {
	if query == "" {
		query = "*"
	}
	if targetLabels == "" {
		targetLabels = inferPrimaryTargetLabel(query)
	}
	if usesDerivedVolumeLabels(targetLabels) {
		result, err := p.volumeByDerivedLabels(ctx, query, start, end, targetLabels, "")
		if err == nil {
			return result, nil
		}
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
	// VL v1.49+ requires step for hits
	if params.Get("step") == "" {
		params.Set("step", "1h")
	}
	// Request field-level grouping
	if targetLabels != "" {
		mappedFields := p.resolveTargetLabelFields(ctx, targetLabels, params)
		if len(mappedFields) > 0 {
			for _, field := range mappedFields {
				params.Add("field", field)
			}
		}
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

	return p.hitsToVolumeVector(body, targetLabels), nil
}

func (p *Proxy) refreshVolumeCacheAsync(orgID, cacheKey, rawQuery, start, end, targetLabels string, savedReq *http.Request) {
	refreshKey := "refresh:volume:" + cacheKey
	go func() {
		_, err, _ := p.labelRefreshGroup.Do(refreshKey, func() (interface{}, error) {
			ctx, cancel := context.WithTimeout(context.Background(), p.labelBackgroundTimeout())
			defer cancel()
			if orgID != "" {
				ctx = context.WithValue(ctx, orgIDKey, orgID)
			}
			if savedReq != nil {
				ctx = context.WithValue(ctx, origRequestKey, savedReq)
			}
			result, err := p.computeVolumeResult(ctx, rawQuery, start, end, targetLabels)
			if err == nil {
				p.setEndpointJSONCacheWithTTL("volume", cacheKey, CacheTTLs["volume"], result)
			}
			return nil, err
		})
		if err != nil {
			p.log.Debug("background volume refresh failed", "cache_key", cacheKey, "error", err)
		}
	}()
}

// handleVolumeRange returns volume range data via VL /select/logsql/hits with step.
// Loki: GET /loki/api/v1/index/volume_range?query={...}&start=...&end=...&step=60
// Response: {"status":"success","data":{"resultType":"matrix","result":[{"metric":{...},"values":[[ts,"count"],...]}]}}
func (p *Proxy) handleVolumeRange(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	if p.handleMultiTenantFanout(w, r, "volume_range") {
		return
	}
	r = withOrgID(r)
	orgID := r.Header.Get("X-Scope-OrgID")
	query := r.FormValue("query")
	startParam := strings.TrimSpace(firstNonEmpty(r.FormValue("start"), r.FormValue("from")))
	endParam := strings.TrimSpace(firstNonEmpty(r.FormValue("end"), r.FormValue("to")))
	stepParam := r.FormValue("step")
	targetLabels := requestedVolumeTargetLabels(r)
	seriesLimit := defaultVolumeSeriesLimit
	if lv := r.FormValue("limit"); lv != "" {
		if n, err := strconv.Atoi(lv); err == nil && n > 0 {
			seriesLimit = n
		}
	}
	cacheKey := p.canonicalReadCacheKey("volume_range", orgID, r)
	if cached, remaining, _, ok := p.endpointReadCacheEntry("volume_range", cacheKey); ok {
		if !p.shouldBypassRecentTailCache("volume_range", remaining, r) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write(cached)
			p.metrics.RecordRequest("volume_range", http.StatusOK, time.Since(start))
			p.metrics.RecordCacheHit()
			if p.shouldRefreshLabelsInBackground(remaining, CacheTTLs["volume_range"]) {
				p.refreshVolumeRangeCacheAsync(orgID, cacheKey, query, startParam, endParam, stepParam, targetLabels, seriesLimit, p.snapshotForwardedAuth(r))
			}
			return
		}
	}
	p.metrics.RecordCacheMiss()

	result, err := p.computeVolumeRangeResult(r.Context(), query, startParam, endParam, stepParam, targetLabels, seriesLimit)
	if err != nil {
		if p.serveStaleReadCacheOnError(w, "volume_range", cacheKey, start, err) {
			return
		}
		status := statusFromUpstreamErr(err)
		p.writeError(w, status, err.Error())
		p.metrics.RecordRequest("volume_range", status, time.Since(start))
		return
	}
	p.setEndpointJSONCacheWithTTL("volume_range", cacheKey, CacheTTLs["volume_range"], result)
	p.writeJSON(w, result)
	p.metrics.RecordRequest("volume_range", http.StatusOK, time.Since(start))
}

func (p *Proxy) computeVolumeRangeResult(ctx context.Context, query, start, end, step, targetLabels string, limit int) (map[string]interface{}, error) {
	if query == "" {
		query = "*"
	}
	if targetLabels == "" {
		targetLabels = inferPrimaryTargetLabel(query)
	}
	if usesDerivedVolumeLabels(targetLabels) {
		result, err := p.volumeByDerivedLabels(ctx, query, start, end, targetLabels, step)
		if err == nil {
			return result, nil
		}
	}
	logsqlQuery, _ := p.translateQueryWithContext(ctx, query)

	// For single-label volume_range with a limit, use stats_query_range so VL applies
	// the limit natively — the hits endpoint returns ALL unique label values (up to
	// 64 MB cap) which causes truncation for high-cardinality labels like pod.
	// Skip synthetic __ labels (e.g. __tenant_id__): they are not native VL fields
	// and the stats pipe cannot group by them; the hits path handles them correctly.
	if targetLabels != "" && !strings.Contains(targetLabels, ",") && !strings.HasPrefix(targetLabels, "__") {
		if result, err := p.computeVolumeRangeViaStats(ctx, logsqlQuery, targetLabels, start, end, step, limit); err == nil {
			return result, nil
		}
		// Fall through to hits on error.
	}

	params := url.Values{}
	params.Set("query", logsqlQuery)
	if s := start; s != "" {
		params.Set("start", formatVLTimestamp(s))
	}
	if e := end; e != "" {
		params.Set("end", formatVLTimestamp(e))
	}
	if step != "" {
		params.Set("step", formatVLStep(step))
	}
	if targetLabels != "" {
		mappedFields := p.resolveTargetLabelFields(ctx, targetLabels, params)
		if len(mappedFields) > 0 {
			for _, field := range mappedFields {
				params.Add("field", field)
			}
		}
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

	return p.hitsToVolumeMatrix(body, targetLabels, start, end, step, limit), nil
}

// computeVolumeRangeViaStats uses stats_query_range to avoid the hits endpoint's
// unbounded response for high-cardinality labels (e.g. pod with thousands of unique
// values). VL's stats_query_range returns per-bucket flat entries (one value per entry);
// consolidateSingleLabelStats groups them into proper time series and applies the global
// top-N limit server-side.
func (p *Proxy) computeVolumeRangeViaStats(ctx context.Context, logsqlQuery, targetLabel, start, end, step string, limit int) (map[string]interface{}, error) {
	// Resolve VL field name via the same alias resolution as the hits path so that
	// underscore-to-dotted mappings (e.g. host_id → host.id) are honoured.
	vlField := targetLabel
	resolutionParams := url.Values{}
	resolutionParams.Set("query", logsqlQuery)
	if start != "" {
		resolutionParams.Set("start", formatVLTimestamp(start))
	}
	if end != "" {
		resolutionParams.Set("end", formatVLTimestamp(end))
	}
	if resolved := p.resolveTargetLabelFields(ctx, targetLabel, resolutionParams); len(resolved) > 0 {
		vlField = resolved[0]
	}
	// Apply | sort | limit N per bucket to bound the VL response:
	// N × buckets × ~90 bytes stays well under the body cap even for long ranges.
	// consolidateSingleLabelStats then re-ranks globally across all buckets.
	statsQuery := logsqlQuery + " | stats by (" + quoteLogsQLIdent(vlField) + ") count() as _c" +
		" | sort by (_c desc) | limit " + strconv.Itoa(limit)
	params := buildStatsQueryRangeParams(statsQuery, start, end, step)
	resp, err := p.vlPost(ctx, "/select/logsql/stats_query_range", params)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := readBodyLimited(resp.Body, maxBufferedBackendBodyBytes)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode >= http.StatusBadRequest {
		msg := strings.TrimSpace(string(body))
		if msg == "" {
			msg = fmt.Sprintf("VL backend returned %d", resp.StatusCode)
		}
		return nil, fmt.Errorf("%s", msg)
	}
	consolidated := consolidateSingleLabelStats(body, vlField, targetLabel, limit)
	var result map[string]interface{}
	if err := stdjson.Unmarshal(consolidated, &result); err != nil {
		return nil, err
	}
	return result, nil
}

// consolidateSingleLabelStats groups VL stats_query_range per-bucket flat entries by
// vlField value, accumulates counts across all time buckets, ranks by total descending,
// limits to the top-limit series, and emits a Loki matrix with targetLabel as the
// metric key (handles the underscore→dotted rename when vlField != targetLabel).
func consolidateSingleLabelStats(body []byte, vlField, targetLabel string, limit int) []byte {
	var p fj.Parser
	v, err := p.ParseBytes(body)
	if err != nil || v == nil {
		return body
	}
	resultArr := v.GetArray("data", "result")
	if len(resultArr) == 0 {
		return body
	}

	// labelValue → timestamp → accumulated count
	type tsMap = map[int64]int64
	counts := make(map[string]tsMap, 512)

	for _, item := range resultArr {
		labelVal := string(item.GetStringBytes("metric", vlField))
		if labelVal == "" {
			continue
		}
		if counts[labelVal] == nil {
			counts[labelVal] = make(tsMap, 16)
		}
		for _, pair := range item.GetArray("values") {
			arr := pair.GetArray()
			if len(arr) < 2 {
				continue
			}
			ts := arr[0].GetInt64()
			cnt, _ := strconv.ParseInt(string(arr[1].GetStringBytes()), 10, 64)
			counts[labelVal][ts] += cnt
		}
	}

	// Rank by total count descending, apply limit.
	type valCount struct {
		val   string
		total int64
	}
	ranked := make([]valCount, 0, len(counts))
	for val, tsCounts := range counts {
		var total int64
		for _, c := range tsCounts {
			total += c
		}
		ranked = append(ranked, valCount{val, total})
	}
	sort.Slice(ranked, func(i, j int) bool {
		if ranked[i].total != ranked[j].total {
			return ranked[i].total > ranked[j].total
		}
		return ranked[i].val < ranked[j].val
	})
	if limit > 0 && len(ranked) > limit {
		ranked = ranked[:limit]
	}

	// Build Loki matrix: one entry per unique label value with all time-bucket values.
	var buf []byte
	buf = append(buf, `{"status":"success","data":{"resultType":"matrix","result":[`...)
	for i, r := range ranked {
		if i > 0 {
			buf = append(buf, ',')
		}
		tsCounts := counts[r.val]
		tsList := make([]int64, 0, len(tsCounts))
		for ts := range tsCounts {
			tsList = append(tsList, ts)
		}
		sort.Slice(tsList, func(i, j int) bool { return tsList[i] < tsList[j] })

		buf = append(buf, `{"metric":{`...)
		buf = appendJSONQuoted(buf, targetLabel)
		buf = append(buf, ':')
		buf = appendJSONQuoted(buf, r.val)
		buf = append(buf, `},"values":[`...)
		for j, ts := range tsList {
			if j > 0 {
				buf = append(buf, ',')
			}
			buf = append(buf, '[')
			buf = strconv.AppendInt(buf, ts, 10)
			buf = append(buf, `,"`...)
			buf = strconv.AppendInt(buf, tsCounts[ts], 10)
			buf = append(buf, '"', ']')
		}
		buf = append(buf, `]}`...)
	}
	buf = append(buf, `]}}`...)
	return buf
}

func (p *Proxy) refreshVolumeRangeCacheAsync(orgID, cacheKey, rawQuery, start, end, step, targetLabels string, limit int, savedReq *http.Request) {
	refreshKey := "refresh:volume_range:" + cacheKey
	go func() {
		_, err, _ := p.labelRefreshGroup.Do(refreshKey, func() (interface{}, error) {
			ctx, cancel := context.WithTimeout(context.Background(), p.labelBackgroundTimeout())
			defer cancel()
			if orgID != "" {
				ctx = context.WithValue(ctx, orgIDKey, orgID)
			}
			if savedReq != nil {
				ctx = context.WithValue(ctx, origRequestKey, savedReq)
			}
			result, err := p.computeVolumeRangeResult(ctx, rawQuery, start, end, step, targetLabels, limit)
			if err == nil {
				p.setEndpointJSONCacheWithTTL("volume_range", cacheKey, CacheTTLs["volume_range"], result)
			}
			return nil, err
		})
		if err != nil {
			p.log.Debug("background volume_range refresh failed", "cache_key", cacheKey, "error", err)
		}
	}()
}
