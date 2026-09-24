package proxy

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// labelMetadataCacheKeyVersion versions the cache keys of /labels,
// /label/{name}/values and the label-name inventory. The same key addresses the
// memory, disk (L2) and peer (L3) tiers and the stale-on-error lookup, so
// bumping it makes entries written by older binaries unreachable everywhere.
// "@full-range-v2": older binaries cached answers covering only the most recent
// minutes (names) or hours (values) of the requested range. '@' cannot appear
// in a Loki label name or in an encoded query string, so versioned keys never
// collide with unversioned ones.
const labelMetadataCacheKeyVersion = "@full-range-v2"

// volumeCacheKeyVersion: older binaries cached volumes as line counts stamped
// at bucket starts; volumes are now bytes stamped like Loki.
const volumeCacheKeyVersion = "@bytes-v1"

// logLineCacheKeyVersion versions the cache keys of log query responses and
// query_range windows, which the disk (L2) and peer (L3) tiers keep for up to
// a day. "line-v3": older binaries wrapped _msg and the row's other fields into
// a {"_msg":...} JSON line instead of returning the stored line, or returned
// VictoriaLogs' missing-message placeholder as the line.
const logLineCacheKeyVersion = "line-v3"

// detectedMetadataCacheKeyVersion versions the cache keys of /detected_fields,
// /detected_field/{name}/values and /detected_labels. "@detected-level-v1":
// older binaries listed detected_level with parsers and the stored level
// values, and hid a detected_level stream label.
const detectedMetadataCacheKeyVersion = "@detected-level-v1"

// readCacheKeyVersion returns the key version segment for endpoint, if any.
func readCacheKeyVersion(endpoint string) string {
	switch endpoint {
	case "labels", "label_values", "label_inventory":
		return labelMetadataCacheKeyVersion
	case "volume", "volume_range":
		return volumeCacheKeyVersion
	case "detected_fields", "detected_field_values", "detected_labels":
		return detectedMetadataCacheKeyVersion
	default:
		return ""
	}
}

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

	var authScope string
	if r != nil {
		authScope = p.fingerprintFromCtx(r.Context(), r)
	}
	if memoKey, ok := buildCanonicalReadCacheMemoKey(endpoint, orgID, r, extraParts); ok && p != nil {
		memoKey.authScope = authScope
		p.readCacheKeyMemoMu.RLock()
		if cached, hit := p.readCacheKeyMemo[memoKey]; hit {
			p.readCacheKeyMemoMu.RUnlock()
			return cached
		}
		p.readCacheKeyMemoMu.RUnlock()

		if authScope != "" {
			extraParts = append(extraParts, "auth:"+authScope)
		}
		computed := computeCanonicalReadCacheKey(endpoint, orgID, r, extraParts...)
		p.readCacheKeyMemoMu.Lock()
		if p.readCacheKeyMemo == nil || len(p.readCacheKeyMemo) >= maxReadCacheKeyMemoEntries {
			p.readCacheKeyMemo = make(map[canonicalReadCacheMemoKey]string, 2048)
		}
		p.readCacheKeyMemo[memoKey] = computed
		p.readCacheKeyMemoMu.Unlock()
		return computed
	}
	if authScope != "" {
		extraParts = append(extraParts, "auth:"+authScope)
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
	if endpoint == "volume_range" {
		if step := strings.TrimSpace(params.Get("step")); step != "" {
			params.Set("step", formatVLStep(step))
		}
	}

	parts := make([]string, 0, 4+len(extraParts))
	parts = append(parts, endpoint, orgID)
	if version := readCacheKeyVersion(endpoint); version != "" {
		parts = append(parts, version)
	}
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

// bucketMetadataTime rounds a nanosecond timestamp down to the nearest bucket
// boundary, matching the split-interval cache-key strategy used by Loki's
// queryrange middleware. This prevents every dashboard reload at a slightly
// different time from generating a new cache key: Grafana already rounds to
// 5-minute intervals at the browser level; we mirror that for label/value
// metadata so the proxy response cache achieves a high hit rate.
//
// Bucket sizes (anchored to the interval between start and end):
//   - interval ≤  6h → 5-minute buckets  (matches Grafana's browser rounding)
//   - interval ≤ 48h → 1-hour  buckets
//   - interval  > 48h → 6-hour  buckets
func bucketMetadataTime(startNs, endNs int64) (bucketedStartNs, bucketedEndNs int64) {
	const (
		ns5m  = int64(5 * time.Minute)
		ns1h  = int64(time.Hour)
		ns6h  = int64(6 * time.Hour)
		ns48h = int64(48 * time.Hour)
	)
	interval := endNs - startNs
	var bucket int64
	switch {
	case interval <= ns6h*1:
		bucket = ns5m
	case interval <= ns48h:
		bucket = ns1h
	default:
		bucket = ns6h
	}
	bucketedStartNs = (startNs / bucket) * bucket
	// Round end UP, not down. Rounding both ends down loses the most recent
	// (bucket-1) of data — for a 12h window with a 1h bucket that's up to
	// 59 minutes of recent logs the user expected to see. Drilldown panels
	// then render "No data" while a sibling 6h or 24h tab works because the
	// 6h bucket size is 5m (small loss) and 24h still covers older data
	// outside the gap. Rounding end UP guarantees the recent window is
	// always covered; VL caps any "future" end to wall-clock now so we
	// never fetch beyond the present.
	bucketedEndNs = ((endNs + bucket - 1) / bucket) * bucket
	return bucketedStartNs, bucketedEndNs
}

// fieldNamesCacheBucket is the granularity used to bucket raw timestamps in
// detected_fields / detected_labels cache keys. A 5-minute bucket matches
// Grafana's browser-level time rounding so repeated reloads within the same
// window produce identical cache keys.
const fieldNamesCacheBucket = 5 * time.Minute

// bucketTimestampString rounds the raw Loki timestamp string ts down to the
// nearest bucket boundary and returns it as a decimal nanosecond string.
// ts may be a Unix nanosecond integer or an RFC3339Nano string. Returns ts
// unchanged if it cannot be parsed.
func bucketTimestampString(ts string, bucket time.Duration) string {
	if ts == "" || bucket <= 0 {
		return ts
	}
	ns, ok := parseLokiTimeToUnixNano(ts)
	if !ok {
		return ts
	}
	b := bucket.Nanoseconds()
	return strconv.FormatInt((ns/b)*b, 10)
}

func normalizeReadCacheParams(endpoint string, params url.Values) {
	if params == nil {
		return
	}
	startRaw := strings.TrimSpace(firstNonEmpty(params.Get("start"), params.Get("from")))
	endRaw := strings.TrimSpace(firstNonEmpty(params.Get("end"), params.Get("to")))

	// For label/value metadata endpoints, bucket start/end to reduce cache misses
	// from the sliding dashboard time window. Raw query and instant endpoints are
	// left unmodified — only metadata responses benefit from time-bucketed keys.
	switch endpoint {
	case "labels", "label_values", "detected_fields", "detected_field_values", "detected_labels":
		if startNs, ok1 := parseLokiTimeToUnixNano(startRaw); ok1 {
			if endNs, ok2 := parseLokiTimeToUnixNano(endRaw); ok2 && endNs > startNs {
				bs, be := bucketMetadataTime(startNs, endNs)
				startRaw = strconv.FormatInt(bs, 10)
				endRaw = strconv.FormatInt(be, 10)
			}
		}
	}

	if startRaw != "" {
		params.Set("start", startRaw)
	}
	params.Del("from")
	if endRaw != "" {
		params.Set("end", endRaw)
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
	// An expired empty label list is a negative entry, not a last known-good
	// answer: skip it so disk (or the error) answers during a backend outage.
	var usable func([]byte) bool
	if endpoint == "labels" || endpoint == "label_values" {
		usable = func(body []byte) bool { return !metadataListPayloadEmpty(body) }
	}
	if p.endpointUsesSharedReadCache(endpoint) {
		return p.cache.GetRecoverableStaleWithTTLMatching(cacheKey, usable)
	}
	body, ttl, ok := p.cache.GetStaleWithTTL(cacheKey)
	if !ok || (usable != nil && !usable(body)) {
		return nil, 0, "", false
	}
	return body, ttl, "l1_memory", true
}

// staleResponseHeader marks a response served from an expired cache entry after
// a backend failure. The compatibility-edge cache and the multi-tenant merge
// cache never store such responses, so the stale answer is not re-served as
// fresh once the backend recovers.
const staleResponseHeader = "X-Proxy-Stale-Response"

// markStaleResponse sets staleResponseHeader and, like the Drilldown partial
// response path, Cache-Control: no-store so HTTP caches in front of the proxy do
// not keep the stale body either.
func markStaleResponse(h http.Header) {
	h.Set(staleResponseHeader, "true")
	h.Set("Cache-Control", "no-store")
}

func (p *Proxy) serveStaleReadCacheOnError(w http.ResponseWriter, endpoint, cacheKey string, started time.Time, err error) bool {
	// A rejected query is not a backend outage: answer it, never mask it.
	if isUpstreamQueryRejected(err) {
		return false
	}
	body, remaining, tier, ok := p.staleEndpointCacheEntry(endpoint, cacheKey)
	if !ok || len(body) == 0 {
		return false
	}
	if strings.TrimSpace(w.Header().Get("Content-Type")) == "" {
		w.Header().Set("Content-Type", "application/json")
	}
	markStaleResponse(w.Header())
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
			// Bypass inner detected-fields cache so this goroutine fetches fresh data
			// from VL rather than re-using the stale result from the original request.
			ctx = context.WithValue(ctx, detectedFieldsRefreshKey{}, true)
			// Its field_names call over the requested range is background
			// inventory work: skipped, not queued, when the scan slots are busy.
			ctx = withBackgroundInventory(ctx)
			fields, _, detectErr := p.detectFields(ctx, query, start, end, lineLimit)
			if detectErr != nil {
				return nil, detectErr
			}
			payload := map[string]interface{}{
				"status": "success",
				"data":   fields,
				"fields": fields,
				"limit":  1000, // same constant as handleDetectedFields (Loki always returns 1000)
			}
			// Window-scaled TTL, matching the handler's staleness comparison.
			p.setEndpointJSONCacheWithTTL("detected_fields", cacheKey, metadataWindowTTL(start, end, CacheTTLs["detected_fields"]), payload)
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
			p.setEndpointJSONCacheWithTTL("detected_labels", cacheKey, metadataWindowTTL(start, end, CacheTTLs["detected_labels"]), payload)
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
			p.setEndpointJSONCacheWithTTL("detected_field_values", cacheKey, metadataWindowTTL(start, end, CacheTTLs["detected_field_values"]), payload)
			return nil, nil
		})
		if err != nil {
			p.log.Debug("background detected_field_values refresh failed", "cache_key", cacheKey, "field", fieldName, "error", err)
		}
	}()
}

func (p *Proxy) resolveDetectedFieldValues(ctx context.Context, fieldName, query, start, end string, lineLimit int, relaxOnEmpty bool) ([]string, error) {
	// Bound the ENTIRE resolve under a single budget. VL has no internal
	// timeout, and even the "native" paths can take 10s+ when a parser filter
	// in the query forces full-line evaluation. Single Drilldown panel must
	// never block past drilldownScanTimeout (default 5s) — on budget
	// exhaustion we return empty and rely on the empty-cache guard so the
	// next request retries with a fresh budget.
	scanCtx, scanCancel := p.withDrilldownScanTimeout(ctx)
	defer scanCancel()
	var (
		values  []string
		errVals error
	)
	// detected_level is derived per sampled line; no stored column holds it.
	derivedLevel := fieldName == detectedLevelLabel
	nativeField, nativeOK := "", false
	if !derivedLevel {
		var resolveErr error
		nativeField, nativeOK, resolveErr = p.resolveNativeDetectedField(scanCtx, query, start, end, fieldName)
		nativeOK = nativeOK && resolveErr == nil
	}
	if nativeOK {
		values, errVals = p.fetchNativeFieldValues(scanCtx, query, start, end, nativeField, lineLimit)
		if errVals == nil && len(values) == 0 {
			// Keep Drilldown UX non-empty for synthetic/derived labels when native values are empty.
			values = nil
		}
		if isContextDeadlineErr(errVals) {
			p.observeInternalOperation(ctx, "drilldown_scan_timeout", "detected_field_values_native", 0)
			return []string{}, nil
		}
	}
	if values == nil && errVals == nil {
		_, fieldValues, detectErr := p.detectFields(scanCtx, query, start, end, lineLimit)
		if detectErr != nil {
			if isContextDeadlineErr(detectErr) {
				p.observeInternalOperation(ctx, "drilldown_scan_timeout", "detected_field_values_detect_fields", 0)
				return []string{}, nil
			}
			return nil, detectErr
		}
		values = fieldValues[fieldName]
	}
	if !derivedLevel && len(values) == 0 && scanCtx.Err() == nil {
		values = p.detectedLabelValuesForField(scanCtx, fieldName, query, start, end, lineLimit)
	}
	if errVals != nil {
		return nil, errVals
	}
	if relaxOnEmpty && len(values) == 0 && scanCtx.Err() == nil {
		if relaxed := relaxedFieldDetectionQuery(query); relaxed != "" && relaxed != query {
			p.observeInternalOperation(ctx, "discovery_fallback", "detected_field_values_relaxed_after_empty", 0)
			return p.resolveDetectedFieldValues(ctx, fieldName, relaxed, start, end, lineLimit, false)
		}
	}
	// Last resort: field is inside JSON or logfmt _msg (not VL-indexed) and was
	// not found by scan or relaxed-query. Only reached when no relaxed query is
	// available (relaxOnEmpty=false or query already relaxed).
	if len(values) == 0 && scanCtx.Err() == nil {
		unpacked, _ := p.fetchUnpackedFieldValues(scanCtx, query, start, end, fieldName, lineLimit)
		values = unpacked
	}
	if scanCtx.Err() != nil && len(values) == 0 {
		p.observeInternalOperation(ctx, "drilldown_scan_timeout", "detected_field_values_overall", 0)
	}
	if values == nil {
		values = []string{}
	}
	return values, nil
}

// withDrilldownScanTimeout returns a context bounded by drilldownScanTimeout
// (or the original context if 0 / unset / already deadlined sooner).
func (p *Proxy) withDrilldownScanTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	if p == nil || p.drilldownScanTimeout <= 0 {
		return ctx, func() {}
	}
	deadline, ok := ctx.Deadline()
	if ok && time.Until(deadline) <= p.drilldownScanTimeout {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, p.drilldownScanTimeout)
}

func isContextDeadlineErr(err error) bool {
	if err == nil {
		return false
	}
	return errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled)
}
