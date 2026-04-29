package proxy

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

func endpointForReadCacheKey(cacheKey string) string {
	switch {
	case strings.HasPrefix(cacheKey, "labels:"):
		return "labels"
	case strings.HasPrefix(cacheKey, "label_values:"):
		return "label_values"
	case strings.HasPrefix(cacheKey, "index_stats:"):
		return "index_stats"
	case strings.HasPrefix(cacheKey, "volume_range:"):
		return "volume_range"
	case strings.HasPrefix(cacheKey, "volume:"):
		return "volume"
	case strings.HasPrefix(cacheKey, "detected_fields:"):
		return "detected_fields"
	case strings.HasPrefix(cacheKey, "detected_field_values:"):
		return "detected_field_values"
	case strings.HasPrefix(cacheKey, "detected_labels:"):
		return "detected_labels"
	default:
		return ""
	}
}

func (p *Proxy) canonicalReadCacheKey(endpoint, orgID string, r *http.Request, extraParts ...string) string {
	// Include the per-user auth fingerprint so requests with different forwarded
	// credentials land in different cache namespaces.
	if r != nil {
		if fp := p.forwardedAuthFingerprint(r); fp != "" {
			extraParts = append(extraParts, "auth:"+fp)
		}
	}
	if memoKey, ok := buildCanonicalReadCacheMemoKey(endpoint, orgID, r, extraParts); ok && p != nil {
		p.readCacheKeyMemoMu.RLock()
		if cached, hit := p.readCacheKeyMemo[memoKey]; hit {
			p.readCacheKeyMemoMu.RUnlock()
			return cached
		}
		p.readCacheKeyMemoMu.RUnlock()

		computed := computeCanonicalReadCacheKey(endpoint, orgID, r, extraParts...)
		p.readCacheKeyMemoMu.Lock()
		if p.readCacheKeyMemo == nil || len(p.readCacheKeyMemo) >= maxReadCacheKeyMemoEntries {
			p.readCacheKeyMemo = make(map[canonicalReadCacheMemoKey]string, 2048)
		}
		p.readCacheKeyMemo[memoKey] = computed
		p.readCacheKeyMemoMu.Unlock()
		return computed
	}
	return computeCanonicalReadCacheKey(endpoint, orgID, r, extraParts...)
}

func buildCanonicalReadCacheMemoKey(endpoint, orgID string, r *http.Request, extraParts []string) (canonicalReadCacheMemoKey, bool) {
	if r == nil || len(extraParts) > 1 {
		return canonicalReadCacheMemoKey{}, false
	}
	key := canonicalReadCacheMemoKey{
		endpoint: endpoint,
		orgID:    orgID,
		rawQuery: r.URL.RawQuery,
	}
	if len(extraParts) == 1 {
		key.extra = strings.TrimSpace(extraParts[0])
	}
	return key, true
}

func computeCanonicalReadCacheKey(endpoint, orgID string, r *http.Request, extraParts ...string) string {
	params := r.URL.Query()
	normalizeReadCacheParams(endpoint, params)
	switch endpoint {
	case "detected_fields", "detected_field_values", "detected_labels":
		params.Set("limit", strconv.Itoa(parseDetectedLineLimit(r)))
	}
	if endpoint == "volume" || endpoint == "volume_range" {
		query := strings.TrimSpace(params.Get("query"))
		if query == "" {
			query = "*"
		}
		if strings.TrimSpace(params.Get("targetLabels")) == "" {
			if inferred := inferPrimaryTargetLabel(query); inferred != "" {
				params.Set("targetLabels", inferred)
			}
		}
		if endpoint == "volume_range" {
			if step := strings.TrimSpace(params.Get("step")); step != "" {
				params.Set("step", formatVLStep(step))
			}
		}
	}

	parts := make([]string, 0, 3+len(extraParts))
	parts = append(parts, endpoint, orgID)
	for _, part := range extraParts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		parts = append(parts, part)
	}
	parts = append(parts, params.Encode())
	return strings.Join(parts, ":")
}

func normalizeReadCacheParams(endpoint string, params url.Values) {
	if params == nil {
		return
	}
	if start := strings.TrimSpace(firstNonEmpty(params.Get("start"), params.Get("from"))); start != "" {
		params.Set("start", start)
	}
	params.Del("from")
	if end := strings.TrimSpace(firstNonEmpty(params.Get("end"), params.Get("to"))); end != "" {
		params.Set("end", end)
	}
	params.Del("to")

	if search := strings.TrimSpace(firstNonEmpty(params.Get("search"), params.Get("q"))); search != "" {
		params.Set("search", search)
	}
	params.Del("q")
	params.Del("line_limit")

	switch endpoint {
	case "labels", "label_values", "index_stats", "volume", "volume_range", "detected_fields", "detected_field_values", "detected_labels":
		if query := strings.TrimSpace(params.Get("query")); query == "" {
			params.Set("query", "*")
		} else {
			params.Set("query", query)
		}
	}
}

func (p *Proxy) setJSONCacheWithTTL(cacheKey string, ttl time.Duration, value interface{}) {
	p.setEndpointJSONCacheWithTTL(endpointForReadCacheKey(cacheKey), cacheKey, ttl, value)
}

func (p *Proxy) setEndpointJSONCacheWithTTL(endpoint, cacheKey string, ttl time.Duration, value interface{}) {
	if p == nil || p.cache == nil {
		return
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return
	}
	p.setEndpointReadCacheWithTTL(endpoint, cacheKey, encoded, ttl)
}

// setLocalReadCacheWithTTL stores response bodies for handlers that only read
// cache entries via GetWithTTL, which is intentionally L1-only. Writing those
// entries to disk or peer write-through adds churn without creating a fallback
// path the handlers actually use.
func (p *Proxy) setLocalReadCacheWithTTL(cacheKey string, value []byte, ttl time.Duration) {
	if p == nil || p.cache == nil {
		return
	}
	p.cache.SetLocalOnlyWithTTL(cacheKey, value, ttl)
}

func (p *Proxy) setEndpointReadCacheWithTTL(endpoint, cacheKey string, value []byte, ttl time.Duration) {
	if p == nil || p.cache == nil {
		return
	}
	if p.endpointUsesSharedReadCache(endpoint) {
		p.cache.SetLocalAndDiskWithTTL(cacheKey, value, ttl)
		return
	}
	p.cache.SetLocalOnlyWithTTL(cacheKey, value, ttl)
}

func (p *Proxy) endpointReadCacheEntry(endpoint, cacheKey string) ([]byte, time.Duration, string, bool) {
	if p == nil || p.cache == nil || strings.TrimSpace(cacheKey) == "" {
		return nil, 0, "", false
	}
	if p.endpointUsesSharedReadCache(endpoint) {
		return p.cache.GetSharedWithTTL(cacheKey)
	}
	body, ttl, ok := p.cache.GetWithTTL(cacheKey)
	if !ok {
		return nil, 0, "", false
	}
	return body, ttl, "l1_memory", true
}

func (p *Proxy) staleEndpointCacheEntry(endpoint, cacheKey string) ([]byte, time.Duration, string, bool) {
	if p == nil || p.cache == nil || strings.TrimSpace(cacheKey) == "" {
		return nil, 0, "", false
	}
	if p.endpointUsesSharedReadCache(endpoint) {
		return p.cache.GetRecoverableStaleWithTTL(cacheKey)
	}
	body, ttl, ok := p.cache.GetStaleWithTTL(cacheKey)
	if !ok {
		return nil, 0, "", false
	}
	return body, ttl, "l1_memory", true
}

func (p *Proxy) serveStaleReadCacheOnError(w http.ResponseWriter, endpoint, cacheKey string, started time.Time, err error) bool {
	body, remaining, tier, ok := p.staleEndpointCacheEntry(endpoint, cacheKey)
	if !ok || len(body) == 0 {
		return false
	}
	if strings.TrimSpace(w.Header().Get("Content-Type")) == "" {
		w.Header().Set("Content-Type", "application/json")
	}
	_, _ = w.Write(body)
	if p.metrics != nil {
		p.metrics.RecordRequest(endpoint, http.StatusOK, time.Since(started))
	}
	staleFor := time.Duration(0)
	if remaining < 0 {
		staleFor = -remaining
	}
	p.log.Warn(
		"serving stale cached response after backend failure",
		"endpoint", endpoint,
		"cache_key", cacheKey,
		"cache_tier", tier,
		"stale_for", staleFor.String(),
		"error", err,
	)
	return true
}

func (p *Proxy) refreshDetectedFieldsCacheAsync(orgID, cacheKey, query, start, end string, lineLimit int, savedReq *http.Request) {
	refreshKey := "refresh:detected_fields:" + cacheKey
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
			fields, _, detectErr := p.detectFields(ctx, query, start, end, lineLimit)
			if detectErr != nil {
				return nil, detectErr
			}
			payload := map[string]interface{}{
				"status": "success",
				"data":   fields,
				"fields": fields,
				"limit":  lineLimit,
			}
			p.setEndpointJSONCacheWithTTL("detected_fields", cacheKey, CacheTTLs["detected_fields"], payload)
			return nil, nil
		})
		if err != nil {
			p.log.Debug("background detected_fields refresh failed", "cache_key", cacheKey, "error", err)
		}
	}()
}

func (p *Proxy) refreshDetectedLabelsCacheAsync(orgID, cacheKey, query, start, end string, lineLimit int, savedReq *http.Request) {
	refreshKey := "refresh:detected_labels:" + cacheKey
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
			labels, _, detectErr := p.detectLabels(ctx, query, start, end, lineLimit)
			if detectErr != nil {
				return nil, detectErr
			}
			payload := map[string]interface{}{
				"status":         "success",
				"data":           labels,
				"detectedLabels": labels,
				"limit":          lineLimit,
			}
			p.setEndpointJSONCacheWithTTL("detected_labels", cacheKey, CacheTTLs["detected_labels"], payload)
			return nil, nil
		})
		if err != nil {
			p.log.Debug("background detected_labels refresh failed", "cache_key", cacheKey, "error", err)
		}
	}()
}

func (p *Proxy) refreshDetectedFieldValuesCacheAsync(orgID, cacheKey, fieldName, query, start, end string, lineLimit int, savedReq *http.Request) {
	refreshKey := "refresh:detected_field_values:" + cacheKey
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
			values, err := p.resolveDetectedFieldValues(ctx, fieldName, query, start, end, lineLimit, true)
			if err != nil {
				return nil, err
			}
			if values == nil {
				values = []string{}
			}

			payload := map[string]interface{}{
				"status": "success",
				"data":   values,
				"values": values,
				"limit":  lineLimit,
			}
			p.setEndpointJSONCacheWithTTL("detected_field_values", cacheKey, CacheTTLs["detected_field_values"], payload)
			return nil, nil
		})
		if err != nil {
			p.log.Debug("background detected_field_values refresh failed", "cache_key", cacheKey, "field", fieldName, "error", err)
		}
	}()
}

func (p *Proxy) resolveDetectedFieldValues(ctx context.Context, fieldName, query, start, end string, lineLimit int, relaxOnEmpty bool) ([]string, error) {
	var (
		values  []string
		errVals error
	)
	if nativeField, ok, resolveErr := p.resolveNativeDetectedField(ctx, query, start, end, fieldName); resolveErr == nil && ok {
		values, errVals = p.fetchNativeFieldValues(ctx, query, start, end, nativeField, lineLimit)
		if errVals == nil && len(values) == 0 {
			// Keep Drilldown UX non-empty for synthetic/derived labels when native values are empty.
			values = nil
		}
	}
	if values == nil && errVals == nil {
		_, fieldValues, detectErr := p.detectFields(ctx, query, start, end, lineLimit)
		if detectErr != nil {
			return nil, detectErr
		}
		values = fieldValues[fieldName]
		if values == nil && fieldName == "level" {
			values = fieldValues["detected_level"]
		}
	}
	if len(values) == 0 {
		values = p.detectedLabelValuesForField(ctx, fieldName, query, start, end, lineLimit)
	}
	if errVals != nil {
		return nil, errVals
	}
	if relaxOnEmpty && len(values) == 0 {
		if relaxed := relaxedFieldDetectionQuery(query); relaxed != "" && relaxed != query {
			p.observeInternalOperation(ctx, "discovery_fallback", "detected_field_values_relaxed_after_empty", 0)
			return p.resolveDetectedFieldValues(ctx, fieldName, relaxed, start, end, lineLimit, false)
		}
	}
	// Last resort: field is inside JSON or logfmt _msg (not VL-indexed) and was
	// not found by scan or relaxed-query. Only reached when no relaxed query is
	// available (relaxOnEmpty=false or query already relaxed).
	if len(values) == 0 {
		values, _ = p.fetchUnpackedFieldValues(ctx, query, start, end, fieldName, lineLimit)
	}
	if values == nil {
		values = []string{}
	}
	return values, nil
}

