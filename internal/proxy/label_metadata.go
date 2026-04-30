package proxy

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"

	mw "github.com/ReliablyObserve/Loki-VL-proxy/internal/middleware"
)

type vlAPIError struct {
	status int
	body   string
}

func (e *vlAPIError) Error() string {
	if strings.TrimSpace(e.body) == "" {
		return fmt.Sprintf("victorialogs api error: status %d", e.status)
	}
	return strings.TrimSpace(e.body)
}

func shouldFallbackToGenericMetadata(err error) bool {
	apiErr, ok := err.(*vlAPIError)
	if !ok {
		return false
	}
	return apiErr.status >= 400 && apiErr.status < 500
}

func decodeVLFieldHits(body []byte) ([]string, error) {
	var resp struct {
		Values []struct {
			Value string `json:"value"`
			Hits  int64  `json:"hits"`
		} `json:"values"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, err
	}
	values := make([]string, 0, len(resp.Values))
	for _, item := range resp.Values {
		values = append(values, item.Value)
	}
	return values, nil
}

func appendUniqueStrings(dst []string, values ...string) []string {
	if len(values) == 0 {
		return dst
	}
	seen := make(map[string]struct{}, safeAddCap(len(dst), len(values)))
	for _, existing := range dst {
		seen[existing] = struct{}{}
	}
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		dst = append(dst, value)
	}
	return dst
}

func (p *Proxy) metadataQueryParams(ctx context.Context, candidate, start, end, limit, search string) (url.Values, error) {
	params := url.Values{}
	translated, err := p.translateQueryWithContext(ctx, candidate)
	if err != nil {
		return nil, err
	}
	params.Set("query", translated)
	if strings.TrimSpace(start) != "" {
		params.Set("start", start)
	}
	if strings.TrimSpace(end) != "" {
		params.Set("end", end)
	}
	if strings.TrimSpace(limit) != "" {
		params.Set("limit", limit)
	}
	if search = strings.TrimSpace(search); search != "" && p.supportsMetadataSubstringFilter() {
		params.Set("q", search)
		params.Set("filter", "substring")
	}
	return params, nil
}

func (p *Proxy) fetchScopedLabelNames(ctx context.Context, rawQuery, start, end, search string, useInventoryCache bool) ([]string, error) {
	candidates := metadataQueryCandidates(rawQuery)
	var lastErr error
	for i, candidate := range candidates {
		params, err := p.metadataQueryParams(ctx, candidate, start, end, "", search)
		if err != nil {
			lastErr = err
			if i+1 < len(candidates) {
				p.observeInternalOperation(ctx, "discovery_fallback", "label_names_relaxed_after_error", 0)
			}
			continue
		}
		var labels []string
		if useInventoryCache {
			labels, err = p.fetchPreferredLabelNamesCached(ctx, params)
		} else {
			labels, err = p.fetchPreferredLabelNames(ctx, params)
		}
		if err != nil {
			lastErr = err
			if i+1 < len(candidates) {
				p.observeInternalOperation(ctx, "discovery_fallback", "label_names_relaxed_after_error", 0)
			}
			continue
		}
		if len(labels) == 0 && i+1 < len(candidates) {
			p.observeInternalOperation(ctx, "discovery_fallback", "label_names_empty_primary", 0)
		}
		return labels, nil
	}
	return nil, lastErr
}

func (p *Proxy) fetchScopedLabelValues(ctx context.Context, labelName, rawQuery, start, end, limit, search string) ([]string, error) {
	candidates := metadataQueryCandidates(rawQuery)
	var lastErr error
	for i, candidate := range candidates {
		params, err := p.metadataQueryParams(ctx, candidate, start, end, limit, search)
		if err != nil {
			lastErr = err
			if i+1 < len(candidates) {
				p.observeInternalOperation(ctx, "discovery_fallback", "label_values_relaxed_after_error", 0)
			}
			continue
		}
		values, err := p.fetchPreferredLabelValues(ctx, labelName, params)
		if err != nil {
			lastErr = err
			if i+1 < len(candidates) {
				p.observeInternalOperation(ctx, "discovery_fallback", "label_values_relaxed_after_error", 0)
			}
			continue
		}
		if len(values) == 0 && i+1 < len(candidates) {
			p.observeInternalOperation(ctx, "discovery_fallback", "label_values_empty_primary", 0)
		}
		return values, nil
	}
	return nil, lastErr
}

func (p *Proxy) snapshotDeclaredLabelFields() []string {
	if p == nil || len(p.declaredLabelFields) == 0 {
		return nil
	}
	out := make([]string, len(p.declaredLabelFields))
	copy(out, p.declaredLabelFields)
	return out
}

// nativeCoalescerKey builds a coalescer key for raw VL backend calls (field_names, streams).
// It scopes by orgID and per-user auth fingerprint so concurrent requests from different tenants
// or users (when downstream ACLs depend on forwarded credentials) do not share one backend response.
func (p *Proxy) nativeCoalescerKey(prefix string, ctx context.Context, params url.Values) string {
	key := prefix + ":" + getOrgID(ctx) + ":" + params.Encode()
	if origReq, ok := ctx.Value(origRequestKey).(*http.Request); ok && origReq != nil {
		if fp := p.forwardedAuthFingerprint(origReq); fp != "" {
			key += ":auth:" + fp
		}
	}
	return key
}

func (p *Proxy) vlGetMetadataCoalesced(ctx context.Context, path string, params url.Values) (int, []byte, error) {
	key := "vlmeta:get:" + getOrgID(ctx) + ":" + path + "?" + params.Encode()
	// Include per-user auth fingerprint to prevent cross-user coalescing when
	// forwarded auth headers/cookies are configured.
	if origReq, ok := ctx.Value(origRequestKey).(*http.Request); ok && origReq != nil {
		if fp := p.forwardedAuthFingerprint(origReq); fp != "" {
			key += ":auth:" + fp
		}
	}
	status, _, body, err := p.coalescer.DoWithGuard(key, p.breaker.Allow, func() (*http.Response, error) {
		return p.vlGetInner(ctx, path, params)
	})
	if errors.Is(err, mw.ErrGuardRejected) {
		return 0, nil, fmt.Errorf("circuit breaker open — backend unavailable")
	}
	if err != nil {
		return 0, nil, err
	}
	return status, body, nil
}

func (p *Proxy) fetchVLFieldNames(ctx context.Context, path string, params url.Values) ([]string, error) {
	status, body, err := p.vlGetMetadataCoalesced(ctx, path, params)
	if err != nil {
		return nil, err
	}
	if status >= 400 {
		return nil, &vlAPIError{status: status, body: string(body)}
	}
	fields, err := decodeVLFieldHits(body)
	if err != nil {
		return nil, err
	}
	p.labelTranslator.LearnFieldAliases(fields)
	return fields, nil
}

// fetchStreamFieldNamesCached wraps the stream_field_names VL call with a short-lived
// internal cache (15s TTL, always-on regardless of -cache-disabled). All label_values and
// label_names requests for the same query+timerange share one backend call, eliminating
// the first of two sequential VL RTTs per label_values request.
func (p *Proxy) fetchStreamFieldNamesCached(ctx context.Context, params url.Values) ([]string, error) {
	cacheKey := "sfn:" + getOrgID(ctx) + ":" + params.Encode()
	if origReq, ok := ctx.Value(origRequestKey).(*http.Request); ok && origReq != nil {
		if fp := p.forwardedAuthFingerprint(origReq); fp != "" {
			cacheKey += ":auth:" + fp
		}
	}
	if cached, ok := p.streamFieldNamesCache.Get(cacheKey); ok {
		var fields []string
		if err := json.Unmarshal(cached, &fields); err == nil {
			return fields, nil
		}
	}
	fields, err := p.fetchVLFieldNames(ctx, "/select/logsql/stream_field_names", params)
	if err != nil {
		return nil, err
	}
	if encoded, encErr := json.Marshal(fields); encErr == nil {
		p.streamFieldNamesCache.Set(cacheKey, encoded)
	}
	return fields, nil
}

func (p *Proxy) fetchVLFieldValues(ctx context.Context, path string, params url.Values) ([]string, error) {
	status, body, err := p.vlGetMetadataCoalesced(ctx, path, params)
	if err != nil {
		return nil, err
	}
	if status >= 400 {
		return nil, &vlAPIError{status: status, body: string(body)}
	}
	return decodeVLFieldHits(body)
}

func (p *Proxy) fetchPreferredLabelNames(ctx context.Context, params url.Values) ([]string, error) {
	if p.supportsStreamMetadataEndpoints() {
		labels, err := p.fetchStreamFieldNamesCached(ctx, params)
		if err == nil {
			labels = appendUniqueStrings(labels, p.snapshotDeclaredLabelFields()...)
			return labels, nil
		}
		if !shouldFallbackToGenericMetadata(err) {
			return nil, err
		}
	}
	fallback, fallbackErr := p.fetchVLFieldNames(ctx, "/select/logsql/field_names", params)
	if fallbackErr != nil {
		return nil, fallbackErr
	}
	fallback = appendUniqueStrings(fallback, p.snapshotDeclaredLabelFields()...)
	return fallback, nil
}

func (p *Proxy) fetchPreferredLabelNamesCached(ctx context.Context, params url.Values) ([]string, error) {
	if p == nil || p.cache == nil {
		return p.fetchPreferredLabelNames(ctx, params)
	}

	cacheKey := "label_inventory:" + getOrgID(ctx) + ":" + params.Encode()
	if origReq, ok := ctx.Value(origRequestKey).(*http.Request); ok && origReq != nil {
		if fp := p.forwardedAuthFingerprint(origReq); fp != "" {
			cacheKey += ":auth:" + fp
		}
	}
	if cached, ok := p.cache.Get(cacheKey); ok {
		var labels []string
		if err := json.Unmarshal(cached, &labels); err == nil {
			return labels, nil
		}
	}

	labels, err := p.fetchPreferredLabelNames(ctx, params)
	if err != nil {
		return nil, err
	}
	if encoded, err := json.Marshal(labels); err == nil {
		p.cache.SetWithTTL(cacheKey, encoded, CacheTTLs["label_inventory"])
	}
	return labels, nil
}

func (p *Proxy) fetchPreferredLabelValues(ctx context.Context, labelName string, params url.Values) ([]string, error) {
	useStreamEndpoint := p.supportsStreamMetadataEndpoints()
	streamFields := []string{}
	if useStreamEndpoint {
		var err error
		streamFields, err = p.fetchStreamFieldNamesCached(ctx, params)
		useStreamEndpoint = err == nil
		if err != nil && !shouldFallbackToGenericMetadata(err) {
			return nil, err
		}
	}
	streamFields = appendUniqueStrings(streamFields, p.snapshotDeclaredLabelFields()...)

	resolution := p.labelTranslator.ResolveLabelCandidates(labelName, streamFields)
	if len(resolution.candidates) == 0 && useStreamEndpoint {
		useStreamEndpoint = false
	}
	if len(resolution.candidates) == 0 && !useStreamEndpoint {
		resolution = p.labelTranslator.ResolveLabelCandidates(labelName, p.snapshotDeclaredLabelFields())
	}
	if len(resolution.candidates) == 0 {
		resolution = fieldResolution{candidates: []string{p.labelTranslator.ToVL(labelName)}}
	}
	if len(resolution.candidates) == 0 {
		return []string{}, nil
	}

	endpoint := "/select/logsql/field_values"
	if useStreamEndpoint {
		endpoint = "/select/logsql/stream_field_values"
	}

	seen := make(map[string]struct{}, 16)
	values := make([]string, 0, 16)
	for _, candidate := range resolution.candidates {
		queryParams := url.Values{}
		for key, items := range params {
			for _, item := range items {
				queryParams.Add(key, item)
			}
		}
		queryParams.Set("field", candidate)

		fieldValues, fieldErr := p.fetchVLFieldValues(ctx, endpoint, queryParams)
		if fieldErr != nil && endpoint == "/select/logsql/stream_field_values" && shouldFallbackToGenericMetadata(fieldErr) {
			endpoint = "/select/logsql/field_values"
			fieldValues, fieldErr = p.fetchVLFieldValues(ctx, endpoint, queryParams)
		}
		if fieldErr != nil {
			return nil, fieldErr
		}
		for _, value := range fieldValues {
			if _, ok := seen[value]; ok {
				continue
			}
			seen[value] = struct{}{}
			values = append(values, value)
		}
	}
	sort.Strings(values)
	return values, nil
}

func (p *Proxy) shouldRefreshLabelsInBackground(remaining, ttl time.Duration) bool {
	if remaining <= 0 || ttl <= 0 {
		return false
	}
	threshold := (ttl * 4) / 5
	if threshold <= 0 {
		threshold = ttl / 2
	}
	return remaining <= threshold
}

func (p *Proxy) requestEndsNearNow(r *http.Request) bool {
	if p == nil || r == nil || p.recentTailRefreshWindow <= 0 {
		return false
	}
	endRaw := strings.TrimSpace(firstNonEmpty(r.FormValue("end"), r.FormValue("to")))
	if endRaw == "" {
		// Loki defaults to "now" when end is omitted.
		return true
	}
	endNs, ok := parseLokiTimeToUnixNano(endRaw)
	if !ok {
		return false
	}
	nowNs := time.Now().UnixNano()
	if endNs > nowNs {
		endNs = nowNs
	}
	return nowNs-endNs <= p.recentTailRefreshWindow.Nanoseconds()
}

func (p *Proxy) shouldBypassRecentTailCache(endpoint string, remaining time.Duration, r *http.Request) bool {
	if p == nil || !p.recentTailRefreshEnabled {
		return false
	}
	ttl := CacheTTLs[endpoint]
	if ttl <= 0 || remaining <= 0 {
		return false
	}
	cacheAge := ttl - remaining
	if cacheAge < p.recentTailRefreshMaxStaleness {
		return false
	}
	return p.requestEndsNearNow(r)
}

// snapshotForwardedAuth captures the configured forward headers and cookies from r into
// a minimal synthetic request that is safe to use from background goroutines after the
// original request has been closed. Returns nil when no forwarding is configured.
func (p *Proxy) snapshotForwardedAuth(r *http.Request) *http.Request {
	if r == nil || (len(p.forwardHeaders) == 0 && len(p.forwardCookies) == 0) {
		return nil
	}
	snap := &http.Request{Header: make(http.Header)}
	for _, hdr := range p.forwardHeaders {
		if val := r.Header.Get(hdr); val != "" {
			snap.Header.Set(hdr, val)
		}
	}
	for _, cookie := range r.Cookies() {
		if p.forwardCookies["*"] || p.forwardCookies[cookie.Name] {
			snap.AddCookie(cookie)
		}
	}
	return snap
}

func (p *Proxy) labelBackgroundTimeout() time.Duration {
	if p.client != nil && p.client.Timeout > 0 && p.client.Timeout < 10*time.Second {
		return p.client.Timeout
	}
	return 10 * time.Second
}

func (p *Proxy) refreshLabelsCacheAsync(orgID, cacheKey, rawQuery, start, end, search string, savedReq *http.Request) {
	refreshKey := "refresh:labels:" + cacheKey
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

			labels, fetchErr := p.fetchScopedLabelNames(ctx, rawQuery, start, end, search, false)
			if fetchErr != nil {
				return nil, fetchErr
			}

			filtered := make([]string, 0, len(labels))
			for _, v := range labels {
				// Filter internal VL fields only (before translation)
				if isVLInternalField(v) || v == "detected_level" {
					continue
				}
				filtered = append(filtered, v)
			}
			// Translate dots to underscores
			labels = p.labelTranslator.TranslateLabelsList(filtered)
			labels = appendSyntheticLabels(labels)
			p.setEndpointReadCacheWithTTL("labels", cacheKey, lokiLabelsResponse(labels), CacheTTLs["labels"])
			return nil, nil
		})
		if err != nil {
			p.log.Debug("background labels refresh failed", "cache_key", cacheKey, "error", err)
		}
	}()
}

func (p *Proxy) refreshLabelValuesCacheAsync(orgID, cacheKey, labelName, rawQuery, start, end, limit, search string, savedReq *http.Request) {
	refreshKey := "refresh:label_values:" + cacheKey
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

			var (
				values   []string
				fetchErr error
			)

			if labelName == "service_name" {
				values, fetchErr = p.serviceNameValues(ctx, rawQuery, start, end)
			} else {
				values, fetchErr = p.fetchScopedLabelValues(ctx, labelName, rawQuery, start, end, limit, search)
			}
			if fetchErr != nil {
				return nil, fetchErr
			}

			p.updateLabelValuesIndex(orgID, labelName, values)
			if p.labelValuesBrowseMode(rawQuery) {
				if indexedValues, ok := p.selectLabelValuesFromIndex(orgID, labelName, "", 0, p.defaultLabelValuesLimit(limit)); ok {
					values = indexedValues
				}
			}
			p.setEndpointReadCacheWithTTL("label_values", cacheKey, lokiLabelsResponse(values), CacheTTLs["label_values"])
			return nil, nil
		})
		if err != nil {
			p.log.Debug("background label values refresh failed", "cache_key", cacheKey, "label", labelName, "error", err)
		}
	}()
}

