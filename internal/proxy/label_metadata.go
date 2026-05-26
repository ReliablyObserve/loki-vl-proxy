package proxy

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
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
		if fp := p.fingerprintFromCtx(ctx, origReq); fp != "" {
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
		if fp := p.fingerprintFromCtx(ctx, origReq); fp != "" {
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

// labelFullRangeFetchKey is a context key that instructs fetchStreamFieldNamesCached
// and fetchAllFieldNamesCached to bypass both their in-process short-TTL cache and
// the 1h backend cap. Background refresh goroutines set this key so they can fetch
// the full user-requested range (e.g., 7d) rather than only the most recent 1h.
// Synchronous (on-request) paths MUST NOT set this key — they rely on the cap for
// bounded latency.
type labelFullRangeFetchKey struct{}

// fetchStreamFieldNamesCached wraps the stream_field_names VL call with a short-lived
// internal cache (15s TTL, always-on regardless of -cache-disabled). All label_values and
// label_names requests for the same query+timerange share one backend call, eliminating
// the first of two sequential VL RTTs per label_values request.
//
// Progressive strategy: the synchronous path caps the VL query to 1h (fast initial
// response), then a background goroutine fetches the full requested range via
// refreshLabelsCacheAsync so subsequent requests return complete historical data.
// Set labelFullRangeFetchKey in context to bypass the cap for background fetches.
func (p *Proxy) fetchStreamFieldNamesCached(ctx context.Context, params url.Values) ([]string, error) {
	cacheKey := "sfn:" + getOrgID(ctx) + ":" + params.Encode()
	if origReq, ok := ctx.Value(origRequestKey).(*http.Request); ok && origReq != nil {
		if fp := p.fingerprintFromCtx(ctx, origReq); fp != "" {
			cacheKey += ":auth:" + fp
		}
	}
	fullRange := ctx.Value(labelFullRangeFetchKey{}) != nil
	if !fullRange {
		if cached, ok := p.streamFieldNamesCache.Get(cacheKey); ok {
			var fields []string
			if err := json.Unmarshal(cached, &fields); err == nil {
				return fields, nil
			}
		}
	}
	backendParams := params
	if !fullRange {
		// Cap to 1h for synchronous path: fast initial response.
		// The background refresh (labelFullRangeFetchKey) uses the full range.
		backendParams = capMetadataTimeRange(params, metadataMaxFieldNamesWindow)
	}
	fields, err := p.fetchVLFieldNames(ctx, "/select/logsql/stream_field_names", backendParams)
	if err != nil {
		return nil, err
	}
	if encoded, encErr := json.Marshal(fields); encErr == nil {
		p.streamFieldNamesCache.Set(cacheKey, encoded)
	}
	return fields, nil
}

// metadataMaxFieldNamesWindow is the cap applied to VL backend calls on the synchronous
// (on-request) path. Background refresh goroutines bypass this cap via labelFullRangeFetchKey.
const metadataMaxFieldNamesWindow = time.Hour

// rangeExceedsWindow returns true when the start/end pair spans more than threshold.
// Returns false if either timestamp cannot be parsed.
func rangeExceedsWindow(start, end string, threshold time.Duration) bool {
	if start == "" || end == "" {
		return false
	}
	startNs, ok1 := parseLokiTimeToUnixNano(start)
	endNs, ok2 := parseLokiTimeToUnixNano(end)
	return ok1 && ok2 && endNs-startNs > int64(threshold)
}

// metadataMaxFieldValuesWindow caps the time range for field_values backend calls.
// A 6h window is sufficient to discover all active label values while avoiding the
// O(days) scan cost that makes the first request for a wide range feel slow.
const metadataMaxFieldValuesWindow = 6 * time.Hour

// capMetadataTimeRange returns params with start/end capped to the most recent maxWindow
// anchored at the provided end time, then bucket-aligned so VL sees consistent params
// regardless of small Grafana sliding-window drift. This mirrors the proxy response cache
// key bucketing applied by normalizeReadCacheParams: by sending the same bucketed params
// to VL on every equivalent query, VL's own internal response path can reuse identical
// requests across dashboards. If the interval already fits within maxWindow, or if
// start/end cannot be parsed, the original params are returned unchanged.
func capMetadataTimeRange(params url.Values, maxWindow time.Duration) url.Values {
	startRaw := firstNonEmpty(params.Get("start"), params.Get("from"))
	endRaw := firstNonEmpty(params.Get("end"), params.Get("to"))
	if startRaw == "" || endRaw == "" {
		return params
	}
	startNs, ok1 := parseLokiTimeToUnixNano(startRaw)
	endNs, ok2 := parseLokiTimeToUnixNano(endRaw)
	if !ok1 || !ok2 || endNs <= startNs {
		return params
	}
	capped := url.Values{}
	for k, vs := range params {
		capped[k] = vs
	}
	maxNs := maxWindow.Nanoseconds()
	cappedStart := startNs
	if endNs-startNs > maxNs {
		cappedStart = endNs - maxNs
	}
	// Bucket the capped range so VL sees identical timestamps from all equivalent queries.
	bucketedStart, bucketedEnd := bucketMetadataTime(cappedStart, endNs)
	capped.Set("start", fmt.Sprintf("%d", bucketedStart))
	capped.Set("end", fmt.Sprintf("%d", bucketedEnd))
	capped.Del("from")
	capped.Del("to")
	return capped
}

// fetchAllFieldNamesCached wraps field_names (all fields, not just stream index) with
// a 30s internal cache. Used for label-value candidate resolution: field_names (0.25s)
// is ~30x faster than stream_field_names (7-8s) and contains a superset of the same
// alias information needed to resolve Loki label names to VL field names.
//
// Same progressive strategy as fetchStreamFieldNamesCached: synchronous path caps to
// 1h; background refresh (labelFullRangeFetchKey in context) uses the full range.
func (p *Proxy) fetchAllFieldNamesCached(ctx context.Context, params url.Values) ([]string, error) {
	cacheKey := "afn:" + getOrgID(ctx) + ":" + params.Encode()
	if origReq, ok := ctx.Value(origRequestKey).(*http.Request); ok && origReq != nil {
		if fp := p.fingerprintFromCtx(ctx, origReq); fp != "" {
			cacheKey += ":auth:" + fp
		}
	}
	fullRange := ctx.Value(labelFullRangeFetchKey{}) != nil
	if !fullRange {
		if cached, ok := p.streamFieldNamesCache.Get(cacheKey); ok {
			var fields []string
			if err := json.Unmarshal(cached, &fields); err == nil {
				return fields, nil
			}
		}
	}
	backendParams := params
	if !fullRange {
		backendParams = capMetadataTimeRange(params, metadataMaxFieldNamesWindow)
	}
	fields, err := p.fetchVLFieldNames(ctx, "/select/logsql/field_names", backendParams)
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
		if fp := p.fingerprintFromCtx(ctx, origReq); fp != "" {
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
		p.cache.SetLocalAndDiskWithTTL(cacheKey, encoded, CacheTTLs["label_inventory"])
	}
	return labels, nil
}

func (p *Proxy) fetchPreferredLabelValues(ctx context.Context, labelName string, params url.Values) ([]string, error) {
	// Use cached field_names for candidate resolution (fast: 0.25s cold, instant cached).
	// Candidate resolution only needs to discover VL field aliases (e.g. k8s_namespace_name →
	// k8s.namespace.name); field_names provides this at much lower cost than stream_field_names.
	//
	// For the actual value fetch, use stream_field_values when available: stream-indexed labels
	// (cluster, namespace, app, env, …) are stored only in VL's stream index and field_values
	// returns empty for them. stream_field_values covers both stream-indexed and columnar fields.
	streamFields := []string{}
	if p.supportsStreamMetadataEndpoints() {
		fields, err := p.fetchAllFieldNamesCached(ctx, params)
		if err != nil && !shouldFallbackToGenericMetadata(err) {
			return nil, err
		}
		streamFields = fields
	}
	streamFields = appendUniqueStrings(streamFields, p.snapshotDeclaredLabelFields()...)

	resolution := p.labelTranslator.ResolveLabelCandidates(labelName, streamFields)
	if len(resolution.candidates) == 0 {
		resolution = p.labelTranslator.ResolveLabelCandidates(labelName, p.snapshotDeclaredLabelFields())
	}
	if len(resolution.candidates) == 0 {
		resolution = fieldResolution{candidates: []string{p.labelTranslator.ToVL(labelName)}}
	}
	if len(resolution.candidates) == 0 {
		return []string{}, nil
	}

	// Prefer stream_field_values so stream-indexed labels (e.g. cluster, namespace)
	// return their values. VL's stream_field_values works with both stream-index and
	// column field filters, so it's safe to use unconditionally when the backend
	// supports it. Fall back to field_values on error or for older backends.
	endpoint := "/select/logsql/field_values"
	if p.supportsStreamMetadataEndpoints() {
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
			// Background label fetches fetch the full user-requested range rather than
			// the synchronous 1h cap, so users with wide time windows (2d, 7d) see
			// complete historical label sets on subsequent requests. Use a longer timeout
			// since VL may need to scan more data for wide ranges.
			timeout := 60 * time.Second
			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			defer cancel()
			if orgID != "" {
				ctx = context.WithValue(ctx, orgIDKey, orgID)
			}
			if savedReq != nil {
				ctx = context.WithValue(ctx, origRequestKey, savedReq)
			}
			// Bypass the 1h cap and in-process streamFieldNamesCache so the full
			// range is actually queried against VL.
			ctx = context.WithValue(ctx, labelFullRangeFetchKey{}, true)

			labels, fetchErr := p.fetchScopedLabelNames(ctx, rawQuery, start, end, search, false)
			if fetchErr != nil {
				return nil, fetchErr
			}

			filtered := make([]string, 0, len(labels))
			for _, v := range labels {
				if isVLInternalField(v) || v == "detected_level" {
					continue
				}
				filtered = append(filtered, v)
			}
			labels = p.labelTranslator.TranslateLabelsList(filtered)
			labels = appendSyntheticLabels(labels)
			p.mergeLabelsIntoCache("labels", cacheKey, labels, CacheTTLs["labels"])
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

// labelWarmupWindows are the standard Grafana time-picker presets that the proxy
// pre-populates on startup and keeps warm via the periodic keep-warm loop.
var labelWarmupWindows = []time.Duration{
	time.Hour,
	6 * time.Hour,
	24 * time.Hour,
	7 * 24 * time.Hour,
}

// warmMetadataCacheOnStartup pre-populates the label cache for common Grafana time
// presets (Last 1h, 6h, 24h, 7d). On startup any entries loaded from disk are served
// immediately (fast start); this goroutine refreshes entries that are expired or have
// less than warmupStaleThreshold remaining TTL so users always see fresh data on first
// request. Runs in a background goroutine; does not block startup.
func (p *Proxy) warmMetadataCacheOnStartup() {
	const warmupStaleThreshold = 30 * time.Second
	go func() {
		// Wait until VL is reachable before warming. Exponential backoff up to 10s
		// per retry; total deadline 60s aligns with Kubernetes readiness probe timeout.
		warmCtx, warmCancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer warmCancel()

		backoff := 2 * time.Second
		for {
			status, _, err := p.vlGetMetadataCoalesced(warmCtx, "/health", url.Values{})
			if err == nil && status < 500 {
				break
			}
			select {
			case <-warmCtx.Done():
				p.log.Debug("label cache warmup aborted: VL not ready within deadline")
				return
			case <-time.After(backoff):
				if backoff < 10*time.Second {
					backoff *= 2
				}
			}
		}

		// Jitter: spread startup warmup across a random window to prevent
		// all fleet instances from hammering VL with wide-range queries at once.
		if p.warmupMaxJitter > 0 {
			jitter := time.Duration(rand.Int64N(int64(p.warmupMaxJitter)))
			select {
			case <-warmCtx.Done():
				return
			case <-time.After(jitter):
			}
		}

		// Use a very short TTL for startup warmup so stale pre-ingestion cache
		// entries expire quickly. The keep-warm loop re-populates with the
		// full CacheTTLs["labels"] TTL once actual label data is available.
		const startupWarmupTTL = 10 * time.Second
		p.warmLabelWindows(warmCtx, warmupStaleThreshold, startupWarmupTTL)
	}()
}

// queryPeerHas asks a single peer which of the given cache keys it has, and their
// remaining TTLs. Uses /_cache/has — no value data is transferred.
// Returns nil on error (treated as "peer has nothing").
func (p *Proxy) queryPeerHas(ctx context.Context, peerAddr string, keys []string) map[string]cache.PeerKeyPresence {
	joined := url.QueryEscape(strings.Join(keys, ","))
	endpoint := fmt.Sprintf("http://%s/_cache/has?keys=%s", peerAddr, joined)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil
	}
	if p.peerAuthToken != "" {
		req.Header.Set("X-Peer-Token", p.peerAuthToken)
	}
	req.Header.Set("Accept-Encoding", "zstd, gzip")
	resp, err := p.client.Do(req)
	if err != nil {
		return nil
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil
	}
	if err := decodeCompressedHTTPResponse(resp); err != nil {
		return nil
	}
	body, err := readBodyLimited(resp.Body, 64<<10) // presence index is tiny
	if err != nil {
		return nil
	}
	var presence map[string]cache.PeerKeyPresence
	if err := json.Unmarshal(body, &presence); err != nil {
		return nil
	}
	return presence
}

// fetchCacheKeysFromPeers performs a two-phase batch peer warmup for a set of cache keys.
//
//  1. Discovery (tiny) — ask each peer "which of these N keys do you have?" via a
//     single /_cache/has request per peer. No value data transferred.
//  2. Fetch (targeted) — for each key, pull the value from the peer with the highest
//     remaining TTL (freshest copy) via /_cache/get.
//
// cacheEndpoint is the namespace used when storing entries locally (e.g. "labels",
// "query_range"). For a fleet of P peers and K keys this costs P+K' network round-trips
// instead of the naive P×K approach (K' ≤ K = keys covered by peers).
// Returns the set of keys that were successfully warmed from peers.
func (p *Proxy) fetchCacheKeysFromPeers(ctx context.Context, cacheEndpoint string, cacheKeys []string, ttl time.Duration) map[string]bool {
	if p.peerCache == nil || len(cacheKeys) == 0 {
		return nil
	}
	peers := p.peerCache.Peers()
	if len(peers) == 0 {
		return nil
	}

	type peerResult struct {
		addr     string
		presence map[string]cache.PeerKeyPresence
	}
	ch := make(chan peerResult, len(peers))
	for _, peerAddr := range peers {
		addr := strings.TrimSpace(peerAddr)
		if addr == "" {
			ch <- peerResult{}
			continue
		}
		go func(a string) {
			hasCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
			defer cancel()
			ch <- peerResult{addr: a, presence: p.queryPeerHas(hasCtx, a, cacheKeys)}
		}(addr)
	}

	// bestPeer[key] = addr with the highest remaining TTL for that key.
	// bestAZPeer tracks the same-AZ peer with the highest TTL when selfAZ is set.
	selfAZ := p.peerCache.SelfAZ()
	bestTTL := make(map[string]int64, len(cacheKeys))
	bestPeer := make(map[string]string, len(cacheKeys))
	bestAZTTL := make(map[string]int64, len(cacheKeys))
	bestAZPeer := make(map[string]string, len(cacheKeys))
	for range peers {
		pr := <-ch
		if pr.presence == nil {
			continue
		}
		for key, info := range pr.presence {
			if !info.OK {
				continue
			}
			if info.TTLMs > bestTTL[key] {
				bestTTL[key] = info.TTLMs
				bestPeer[key] = pr.addr
			}
			if selfAZ != "" && p.peerCache.PeerAZ(pr.addr) == selfAZ && info.TTLMs > bestAZTTL[key] {
				bestAZTTL[key] = info.TTLMs
				bestAZPeer[key] = pr.addr
			}
		}
	}

	// Phase 2: fetch values only for keys we need.
	// Prefer same-AZ peers to reduce cross-AZ traffic; fall back to any peer with fresh data.
	warmed := make(map[string]bool, len(cacheKeys))
	for _, key := range cacheKeys {
		addr, ok := bestAZPeer[key]
		if !ok {
			addr, ok = bestPeer[key]
		}
		if !ok {
			continue // no peer has this key
		}
		getCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		endpoint := fmt.Sprintf("http://%s/_cache/get?key=%s", addr, url.QueryEscape(key))
		req, err := http.NewRequestWithContext(getCtx, http.MethodGet, endpoint, nil)
		if err != nil {
			cancel()
			continue
		}
		if p.peerAuthToken != "" {
			req.Header.Set("X-Peer-Token", p.peerAuthToken)
		}
		req.Header.Set("Accept-Encoding", "zstd, gzip")
		resp, err := p.client.Do(req)
		if err != nil {
			cancel()
			continue
		}
		var body []byte
		if resp.StatusCode == http.StatusOK {
			if decodeCompressedHTTPResponse(resp) == nil {
				body, _ = readBodyLimited(resp.Body, 4<<20)
			}
		}
		resp.Body.Close()
		cancel()
		if len(body) == 0 {
			continue
		}
		p.setEndpointReadCacheWithTTL(cacheEndpoint, key, body, ttl)
		warmed[key] = true
	}
	return warmed
}

// labelWarmupWindow carries the metadata needed to warm one Grafana time-window preset.
type labelWarmupWindow struct {
	window   time.Duration
	startStr string
	endStr   string
	cacheKey string
}

// warmLabelWindows pre-populates the label cache for the standard Grafana time presets.
// Windows whose cache entry still has more than minRemaining TTL are skipped.
//
// VL queries are capped to the same 1h window as synchronous user requests (via the
// normal capMetadataTimeRange path). This keeps warmup cheap and avoids wide-range scans
// that would compete with user query_range requests. When users actually request wider
// ranges (6h, 24h, 7d) the background refresh goroutines in handleLabels fetch the full
// range from VL so subsequent requests see complete historical labels.
//
// Peer-first strategy (batch, two-phase):
//  1. Discovery — send one /_cache/has request per peer with all stale window keys.
//     This is metadata-only (no value transfer) and costs P requests for P peers.
//  2. Fetch — pull values from the best (freshest) peer for each window that a peer
//     has cached, costing at most W requests for W windows.
//
// Only windows not covered by any peer fall through to VL queries.
func (p *Proxy) warmLabelWindows(ctx context.Context, minRemaining, ttl time.Duration) {
	nowNs := time.Now().UnixNano()

	// Build metadata for all windows and filter out locally-fresh entries.
	stale := make([]labelWarmupWindow, 0, len(labelWarmupWindows))
	for _, window := range labelWarmupWindows {
		bs, be := bucketMetadataTime(nowNs-int64(window), nowNs)
		startStr := strconv.FormatInt(bs, 10)
		endStr := strconv.FormatInt(be, 10)
		rawQ := "start=" + startStr + "&end=" + endStr + "&query=%2A"
		req, err := http.NewRequest("GET", "/loki/api/v1/labels?"+rawQ, nil)
		if err != nil {
			continue
		}
		cacheKey := p.canonicalReadCacheKey("labels", "", req)
		if _, remaining, _, ok := p.endpointReadCacheEntry("labels", cacheKey); ok && remaining > minRemaining {
			continue // still sufficiently fresh
		}
		stale = append(stale, labelWarmupWindow{window: window, startStr: startStr, endStr: endStr, cacheKey: cacheKey})
	}
	if len(stale) == 0 {
		return
	}

	// Phase 1+2: batch peer discovery + targeted fetch.
	// One /_cache/has request per peer covers all stale keys; then one /_cache/get
	// per covered key from the best peer. Total: P + W' requests (W' ≤ W).
	staleKeys := make([]string, len(stale))
	for i, w := range stale {
		staleKeys[i] = w.cacheKey
	}
	warmedFromPeer := p.fetchCacheKeysFromPeers(ctx, "labels", staleKeys, ttl)

	// Phase 3: fetch remaining stale windows from VL (capped to 1h, same as user requests).
	for _, w := range stale {
		if warmedFromPeer[w.cacheKey] {
			p.log.Debug("label cache window warmed from peer", "window", w.window)
			continue
		}

		fetchCtx, fetchCancel := context.WithTimeout(ctx, 10*time.Second)
		labels, fetchErr := p.fetchScopedLabelNames(fetchCtx, "*", w.startStr, w.endStr, "", false)
		fetchCancel()
		if fetchErr != nil {
			p.log.Debug("label cache warmup failed", "window", w.window, "err", fetchErr)
			continue
		}
		filtered := make([]string, 0, len(labels))
		for _, v := range labels {
			if isVLInternalField(v) || v == "detected_level" {
				continue
			}
			filtered = append(filtered, v)
		}
		labels = p.labelTranslator.TranslateLabelsList(filtered)
		labels = appendSyntheticLabels(labels)
		p.mergeLabelsIntoCache("labels", w.cacheKey, labels, ttl)
		p.log.Debug("label cache warmed from VL", "window", w.window)
	}
}

// startLabelCacheKeepWarmLoop runs warmLabelWindows periodically so that label
// cache entries for standard Grafana presets never go cold when there are no user
// queries. The interval and skip threshold are derived from CacheTTLs["labels"]:
// refresh at 75% of TTL, skip if >40% of TTL remains.
// The goroutine exits when p.keepWarmStop is closed (via Shutdown).
func (p *Proxy) startLabelCacheKeepWarmLoop() {
	ttl := CacheTTLs["labels"]
	interval := ttl * 3 / 4
	skipIfRemaining := ttl * 2 / 5
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-p.keepWarmStop:
				return
			case <-ticker.C:
				ctx, cancel := context.WithTimeout(context.Background(), interval)
				p.warmLabelWindows(ctx, skipIfRemaining, ttl)
				cancel()
			}
		}
	}()
}

// mergeLabelsIntoCache unions newLabels with the current cached label set for
// cacheKey and writes the result back with a fresh TTL. This ensures that new
// labels discovered by user queries or background refreshes are additive:
// labels never disappear from the cache within a TTL window.
func (p *Proxy) mergeLabelsIntoCache(endpoint, cacheKey string, newLabels []string, ttl time.Duration) {
	merged := make(map[string]struct{}, len(newLabels))
	for _, l := range newLabels {
		merged[l] = struct{}{}
	}
	if cached, _, _, ok := p.endpointReadCacheEntry(endpoint, cacheKey); ok {
		var existing struct {
			Data []string `json:"data"`
		}
		if json.Unmarshal(cached, &existing) == nil {
			for _, l := range existing.Data {
				merged[l] = struct{}{}
			}
		}
	}
	all := make([]string, 0, len(merged))
	for l := range merged {
		all = append(all, l)
	}
	sort.Strings(all)
	p.setEndpointReadCacheWithTTL(endpoint, cacheKey, lokiLabelsResponse(all), ttl)
}
