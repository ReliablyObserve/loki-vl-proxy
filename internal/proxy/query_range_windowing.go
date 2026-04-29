package proxy

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"
)

const (
	maxQueryRangeWindows           = 4096
	queryRangeCollectedInitialCap  = 1024
	queryRangeWindowFetchAttempts  = 5
	queryRangeRetryMinBackoff      = 100 * time.Millisecond
	queryRangeRetryMaxBackoff      = 5 * time.Second
	queryRangeBatchRetryAttempts   = 3
	queryRangePrefilterAttempts    = 2
	queryRangePrefilterFallbackTTL = 30 * time.Second
)

type queryRangeWindow struct {
	startNs int64
	endNs   int64
}

type queryRangeWindowEntry struct {
	Stream map[string]string `json:"stream"`
	Value  []interface{}     `json:"value"`
}

type queryRangeWindowCacheEntry struct {
	Entries []queryRangeWindowEntry `json:"entries"`
}

func (p *Proxy) proxyLogQueryWindowed(w http.ResponseWriter, r *http.Request, logsqlQuery string) bool {
	if !p.queryRangeWindowing || p.streamResponse {
		return false
	}

	startNs, endNs, ok := parseLokiTimeRangeToUnixNano(r.FormValue("start"), r.FormValue("end"))
	if !ok {
		return false
	}
	windows := splitQueryRangeWindowsWithOptions(
		startNs,
		endNs,
		p.queryRangeSplitInterval,
		r.FormValue("direction"),
		p.queryRangeAlignWindows,
	)
	if len(windows) <= 1 {
		return false
	}
	originalWindowCount := len(windows)

	queryLimit := r.FormValue("limit")
	if queryLimit == "" {
		queryLimit = strconv.Itoa(p.maxLines)
	}
	queryLimit = sanitizeLimit(queryLimit)
	limitValue, err := strconv.Atoi(queryLimit)
	if err != nil || limitValue <= 0 {
		limitValue = 1000
	}

	categorizedLabels := requestWantsCategorizedLabels(r)
	emitStructuredMetadata := p.shouldEmitStructuredMetadata(r)
	p.metrics.RecordTupleMode(tupleModeForRequest(categorizedLabels, emitStructuredMetadata))

	filteredWindows, windowHitEstimate, prefilterErr := p.prefilterQueryRangeWindowsByHits(r.Context(), r, logsqlQuery, windows)
	if prefilterErr != nil {
		p.log.Debug(
			"query_range window prefilter unavailable; using full window set",
			"error", prefilterErr,
			"window_count", originalWindowCount,
		)
	} else {
		windows = filteredWindows
	}

	p.metrics.RecordQueryRangeWindowCount(len(windows))
	if len(windows) == 0 {
		result, _ := json.Marshal(map[string]interface{}{
			"status": "success",
			"data": func() map[string]interface{} {
				data := map[string]interface{}{
					"resultType": "streams",
					"result":     []map[string]interface{}{},
					"stats":      map[string]interface{}{},
				}
				if categorizedLabels {
					data["encodingFlags"] = []string{"categorize-labels"}
				}
				return data
			}(),
		})
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(result) // nosemgrep: go.lang.security.audit.xss.no-direct-write-to-responsewriter -- Content-Type set above; proxy returns pre-built JSON
		return true
	}

	remaining := limitValue
	// Keep preallocation constant-sized to avoid any user-influenced allocation growth.
	collected := make([]queryRangeWindowEntry, 0, queryRangeCollectedInitialCap)
	for i := 0; i < len(windows) && remaining > 0; {
		parallel := p.queryRangeWindowBatchParallelLimit(windows[i], windowHitEstimate)
		batchSize := min(parallel, len(windows)-i)
		if batchSize <= 0 {
			batchSize = 1
		}
		retryAttempt := 0
		var results []queryRangeWindowCacheEntry
		var err error
		for {
			batch := windows[i : i+batchSize]
			limitForBatch := remaining
			results, err = p.fetchQueryRangeWindowBatch(r, logsqlQuery, queryLimit, batch, limitForBatch, batchSize, categorizedLabels, emitStructuredMetadata)
			if err == nil {
				break
			}

			retryable := shouldRetryQueryRangeWindow(err)
			if retryable && batchSize > 1 {
				nextBatchSize := max(1, batchSize/2)
				p.metrics.RecordQueryRangeWindowDegradedBatch()
				p.metrics.RecordQueryRangeWindowRetry()
				p.log.Warn("query_range windowed batch backoff",
					"error", err,
					"window_count", len(windows),
					"batch_start", i,
					"batch_size", batchSize,
					"next_batch_size", nextBatchSize,
				)
				p.forceQueryRangeParallelBackoff()
				retryAttempt++
				backoff := queryRangeWindowRetryBackoff(retryAttempt)
				select {
				case <-r.Context().Done():
					p.writeError(w, http.StatusRequestTimeout, r.Context().Err().Error())
					return true
				case <-time.After(backoff):
				}
				batchSize = nextBatchSize
				continue
			}

			if retryable && batchSize == 1 && retryAttempt < queryRangeBatchRetryAttempts {
				retryAttempt++
				p.metrics.RecordQueryRangeWindowRetry()
				backoff := queryRangeWindowRetryBackoff(retryAttempt)
				p.log.Warn("query_range single-window retrying after batch failure",
					"error", err,
					"window_count", len(windows),
					"window_index", i,
					"attempt", retryAttempt+1,
					"max_attempts", queryRangeBatchRetryAttempts+1,
					"backoff", backoff.String(),
				)
				select {
				case <-r.Context().Done():
					p.writeError(w, http.StatusRequestTimeout, r.Context().Err().Error())
					return true
				case <-time.After(backoff):
				}
				continue
			}
			if retryable && p.queryRangePartialResponses {
				p.metrics.RecordQueryRangeWindowPartialResponse()
				w.Header().Set("X-Loki-VL-Partial-Response", "true")
				if p.queryRangeBackgroundWarm {
					p.warmQueryRangeWindowsAsync(r.Clone(context.Background()), logsqlQuery, queryLimit, windows[i:], categorizedLabels, emitStructuredMetadata)
				}
				p.log.Warn("query_range returning partial response after retryable batch failure",
					"error", err,
					"remaining_windows", len(windows)-i,
				)
				break
			}
			status := statusFromQueryRangeWindowErr(err)
			p.log.Warn("query_range windowed fetch failed",
				"error", err,
				"window_count", len(windows),
				"batch_size", batchSize,
				"status", status,
			)
			p.writeError(w, status, err.Error())
			return true
		}

		for _, result := range results {
			if len(result.Entries) == 0 {
				continue
			}
			collected = append(collected, result.Entries...)
			if len(collected) >= limitValue {
				collected = collected[:limitValue]
				remaining = 0
				break
			}
			remaining = limitValue - len(collected)
		}
		i += batchSize
	}

	mergeStart := time.Now()
	p.maybeAutodetectPatternsFromWindowEntries(
		r.Header.Get("X-Scope-OrgID"),
		r.FormValue("query"),
		r.FormValue("start"),
		r.FormValue("end"),
		r.FormValue("step"),
		collected,
	)
	streams := groupQueryRangeWindowEntries(collected, r.FormValue("direction"))
	p.metrics.RecordQueryRangeWindowMergeDuration(time.Since(mergeStart))

	if len(p.derivedFields) > 0 {
		p.applyDerivedFields(streams)
	}
	originalQuery := r.FormValue("query")
	if strings.Contains(originalQuery, "decolorize") {
		decolorizeStreams(streams)
	}
	if label, cidr, matched := parseIPFilter(originalQuery); matched {
		streams = ipFilterStreams(streams, label, cidr)
	}
	if tmpl := extractLineFormatTemplate(originalQuery); tmpl != "" {
		applyLineFormatTemplate(streams, tmpl)
	}

	result, _ := json.Marshal(map[string]interface{}{
		"status": "success",
		"data": func() map[string]interface{} {
			data := map[string]interface{}{
				"resultType": "streams",
				"result":     streams,
				"stats":      map[string]interface{}{},
			}
			if categorizedLabels {
				data["encodingFlags"] = []string{"categorize-labels"}
			}
			return data
		}(),
	})
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(result) // nosemgrep: go.lang.security.audit.xss.no-direct-write-to-responsewriter -- Content-Type set above; proxy returns pre-built JSON
	return true
}

func (p *Proxy) warmQueryRangeWindowsAsync(
	r *http.Request,
	logsqlQuery string,
	queryLimit string,
	windows []queryRangeWindow,
	categorizedLabels bool,
	emitStructuredMetadata bool,
) {
	if len(windows) == 0 || p == nil {
		return
	}
	maxWarm := p.queryRangeBackgroundWarmMaxWindows
	if maxWarm <= 0 || maxWarm > len(windows) {
		maxWarm = len(windows)
	}
	toWarm := append([]queryRangeWindow(nil), windows[:maxWarm]...)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		for _, window := range toWarm {
			select {
			case <-ctx.Done():
				return
			default:
			}
			_, _ = p.fetchQueryRangeWindow(ctx, r, logsqlQuery, queryLimit, 1000, window, categorizedLabels, emitStructuredMetadata)
		}
	}()
}

func (p *Proxy) fetchQueryRangeWindowBatch(
	r *http.Request,
	logsqlQuery string,
	queryLimit string,
	windows []queryRangeWindow,
	limitForBatch int,
	maxParallel int,
	categorizedLabels bool,
	emitStructuredMetadata bool,
) ([]queryRangeWindowCacheEntry, error) {
	results := make([]queryRangeWindowCacheEntry, len(windows))
	var mu sync.Mutex
	g, ctx := errgroup.WithContext(r.Context())
	if maxParallel <= 0 {
		maxParallel = 1
	}
	g.SetLimit(min(maxParallel, len(windows)))
	if limitForBatch <= 0 {
		limitForBatch = 1
	}
	for i := range windows {
		i := i
		window := windows[i]
		g.Go(func() error {
			entry, err := p.fetchQueryRangeWindow(
				ctx,
				r,
				logsqlQuery,
				queryLimit,
				limitForBatch,
				window,
				categorizedLabels,
				emitStructuredMetadata,
			)
			if err != nil {
				return err
			}
			mu.Lock()
			results[i] = entry
			mu.Unlock()
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}
	return results, nil
}

func (p *Proxy) fetchQueryRangeWindow(
	ctx context.Context,
	r *http.Request,
	logsqlQuery string,
	queryLimit string,
	windowLimit int,
	window queryRangeWindow,
	categorizedLabels bool,
	emitStructuredMetadata bool,
) (queryRangeWindowCacheEntry, error) {
	fetchCtx := ctx
	cancel := func() {}
	if p.queryRangeWindowTimeout > 0 {
		fetchCtx, cancel = context.WithTimeout(ctx, p.queryRangeWindowTimeout)
	}
	defer cancel()

	cacheKey := p.queryRangeWindowCacheKey(r, logsqlQuery, queryLimit, window, categorizedLabels, emitStructuredMetadata)
	if cached, ok := p.cache.Get(cacheKey); ok {
		var entry queryRangeWindowCacheEntry
		if err := json.Unmarshal(cached, &entry); err == nil {
			p.metrics.RecordQueryRangeWindowCacheHit()
			return entry, nil
		}
	}
	p.metrics.RecordQueryRangeWindowCacheMiss()

	windowQuery := withQueryDirectionSort(logsqlQuery, r.FormValue("direction"))
	params := url.Values{}
	params.Set("query", windowQuery)
	params.Set("start", strconv.FormatInt(window.startNs, 10))
	params.Set("end", strconv.FormatInt(window.endNs, 10))
	params.Set("limit", strconv.Itoa(windowLimit))

	coalesceKey := "query_range_window:" + cacheKey
	for attempt := 1; attempt <= queryRangeWindowFetchAttempts; attempt++ {
		fetchStart := time.Now()
		status, body, err := p.vlPostCoalesced(fetchCtx, coalesceKey, "/select/logsql/query", params)
		fetchDuration := time.Since(fetchStart)
		p.metrics.RecordQueryRangeWindowFetchDuration(fetchDuration)

		if err == nil && status < 400 {
			p.observeQueryRangeWindowFetch(fetchDuration, false)
			entries := p.vlLogsToLokiWindowEntries(body, r.FormValue("query"), categorizedLabels, emitStructuredMetadata)
			entry := queryRangeWindowCacheEntry{Entries: entries}
			if ttl := p.queryRangeWindowTTL(window.endNs); ttl > 0 {
				if encoded, err := json.Marshal(entry); err == nil {
					// Window fragments are short-lived and high churn. Keeping them
					// local avoids concentrating peer-cache write-through on a single
					// ring owner when Drilldown or Explore fans a read into many windows.
					p.cache.SetLocalOnlyWithTTL(cacheKey, encoded, ttl)
				}
			}
			return entry, nil
		}

		var fetchErr error
		if err != nil {
			fetchErr = err
		} else {
			msg := strings.TrimSpace(string(body))
			if msg == "" {
				msg = fmt.Sprintf("VL backend returned %d", status)
			}
			fetchErr = &queryRangeWindowHTTPError{status: status, msg: msg}
		}

		p.observeQueryRangeWindowFetch(fetchDuration, true)
		if attempt >= queryRangeWindowFetchAttempts || !shouldRetryQueryRangeWindow(fetchErr) {
			return queryRangeWindowCacheEntry{}, fetchErr
		}

		p.metrics.RecordQueryRangeWindowRetry()
		backoff := queryRangeWindowRetryBackoff(attempt)
		p.log.Warn(
			"query_range window fetch retrying",
			"attempt", attempt+1,
			"max_attempts", queryRangeWindowFetchAttempts,
			"backoff", backoff.String(),
			"error", fetchErr,
		)
		select {
		case <-fetchCtx.Done():
			return queryRangeWindowCacheEntry{}, fetchCtx.Err()
		case <-time.After(backoff):
		}
	}

	return queryRangeWindowCacheEntry{}, errors.New("query_range window fetch failed")
}

func (p *Proxy) prefilterQueryRangeWindowsByHits(
	ctx context.Context,
	r *http.Request,
	logsqlQuery string,
	windows []queryRangeWindow,
) ([]queryRangeWindow, map[string]int64, error) {
	if !p.queryRangePrefilterIndexStats || len(windows) < p.queryRangePrefilterMinWindows {
		return windows, nil, nil
	}
	p.metrics.RecordQueryRangeWindowPrefilterAttempt()
	start := time.Now()
	defer func() {
		p.metrics.RecordQueryRangeWindowPrefilterDuration(time.Since(start))
	}()

	prefilterQuery := queryRangePrefilterQuery(logsqlQuery)
	if prefilterQuery == "" {
		prefilterQuery = "*"
	}

	keep := make([]bool, len(windows))
	estimates := make([]int64, len(windows))
	maxParallel := min(4, max(1, p.queryRangeWindowParallelLimit()))
	maxParallel = min(maxParallel, len(windows))
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(maxParallel)
	for i := range windows {
		i := i
		window := windows[i]
		g.Go(func() error {
			hitEstimate, err := p.queryRangeWindowHitEstimate(gctx, r, prefilterQuery, window)
			if err != nil {
				return err
			}
			estimates[i] = hitEstimate
			keep[i] = hitEstimate > 0
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		p.metrics.RecordQueryRangeWindowPrefilterError()
		return windows, nil, err
	}

	filtered := make([]queryRangeWindow, 0, len(windows))
	filteredEstimates := make(map[string]int64, len(windows))
	for i, hasHits := range keep {
		if hasHits {
			filtered = append(filtered, windows[i])
			filteredEstimates[queryRangeWindowKey(windows[i])] = estimates[i]
		}
	}
	p.metrics.RecordQueryRangeWindowPrefilterOutcome(len(filtered), len(windows)-len(filtered))
	return filtered, filteredEstimates, nil
}

func (p *Proxy) queryRangeWindowHitEstimate(
	ctx context.Context,
	r *http.Request,
	logsqlQuery string,
	window queryRangeWindow,
) (int64, error) {
	cacheKey := p.queryRangeWindowHasHitsCacheKey(r, logsqlQuery, window)
	if cached, ok := p.cache.Get(cacheKey); ok && len(cached) > 0 {
		if est, err := strconv.ParseInt(strings.TrimSpace(string(cached)), 10, 64); err == nil && est >= 0 {
			return est, nil
		}
		if cached[0] == '1' {
			return 1, nil
		}
		return 0, nil
	}

	params := url.Values{}
	params.Set("query", logsqlQuery)
	params.Set("start", strconv.FormatInt(window.startNs, 10))
	params.Set("end", strconv.FormatInt(window.endNs, 10))
	windowSeconds := max(int64(1), (window.endNs-window.startNs+1)/int64(time.Second))
	params.Set("step", formatVLStep(strconv.FormatInt(windowSeconds, 10)))

	coalesceKey := "query_range_window_prefilter:" + cacheKey
	for attempt := 1; attempt <= queryRangePrefilterAttempts; attempt++ {
		status, body, err := p.vlGetCoalescedWithStatus(ctx, coalesceKey, "/select/logsql/hits", params)
		if err == nil && status < http.StatusBadRequest {
			hitEstimate := int64(sumHitsValues(body))
			if hitEstimate < 0 {
				hitEstimate = 0
			}
			if ttl := p.queryRangeWindowPrefilterTTL(window.endNs); ttl > 0 {
				// Prefilter hit estimates are window-scoped scratch state. They are
				// cheap to recompute and do not justify peer write-through churn.
				p.cache.SetLocalOnlyWithTTL(cacheKey, []byte(strconv.FormatInt(hitEstimate, 10)), ttl)
			}
			return hitEstimate, nil
		}
		if err == nil {
			msg := strings.TrimSpace(string(body))
			if msg == "" {
				msg = fmt.Sprintf("VL backend returned %d", status)
			}
			err = &queryRangeWindowHTTPError{status: status, msg: msg}
		}
		if attempt >= queryRangePrefilterAttempts || !shouldRetryQueryRangeWindow(err) {
			return 0, err
		}
		p.metrics.RecordQueryRangeWindowRetry()
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-time.After(queryRangeWindowRetryBackoff(attempt)):
		}
	}
	return 0, errors.New("query_range window prefilter failed")
}

func queryRangeWindowKey(window queryRangeWindow) string {
	return strconv.FormatInt(window.startNs, 10) + ":" + strconv.FormatInt(window.endNs, 10)
}

func (p *Proxy) queryRangeWindowBatchParallelLimit(window queryRangeWindow, estimates map[string]int64) int {
	parallel := p.queryRangeWindowParallelLimit()
	if !p.queryRangeStreamAwareBatching || parallel <= 1 {
		return parallel
	}
	threshold := p.queryRangeExpensiveWindowHitThreshold
	if threshold <= 0 {
		return parallel
	}
	if estimates == nil {
		return parallel
	}
	hitEstimate, ok := estimates[queryRangeWindowKey(window)]
	if !ok || hitEstimate < threshold {
		return parallel
	}
	capParallel := p.queryRangeExpensiveWindowMaxParallel
	if capParallel <= 0 {
		capParallel = 1
	}
	if capParallel < parallel {
		p.metrics.RecordQueryRangeWindowDegradedBatch()
		return capParallel
	}
	return parallel
}

func (p *Proxy) queryRangeWindowHasHitsCacheKey(
	r *http.Request,
	logsqlQuery string,
	window queryRangeWindow,
) string {
	return strings.Join([]string{
		"query_range_window_has_hits",
		r.Header.Get("X-Scope-OrgID"),
		logsqlQuery,
		strconv.FormatInt(window.startNs, 10),
		strconv.FormatInt(window.endNs, 10),
	}, ":")
}

func (p *Proxy) queryRangeWindowPrefilterTTL(windowEndNs int64) time.Duration {
	ttl := p.queryRangeWindowTTL(windowEndNs)
	if ttl <= 0 {
		return queryRangePrefilterFallbackTTL
	}
	return ttl
}

func queryRangePrefilterQuery(logsqlQuery string) string {
	query := strings.TrimSpace(logsqlQuery)
	if query == "" {
		return ""
	}
	if idx := strings.Index(query, "|"); idx > 0 {
		if selector := strings.TrimSpace(query[:idx]); selector != "" {
			return selector
		}
	}
	return query
}

type queryRangeWindowHTTPError struct {
	status int
	msg    string
}

func (e *queryRangeWindowHTTPError) Error() string {
	return e.msg
}

func (e *queryRangeWindowHTTPError) StatusCode() int {
	return e.status
}

func shouldRetryQueryRangeWindow(err error) bool {
	if err == nil {
		return false
	}
	if isCanceledErr(err) {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return true
	}
	var httpErr interface{ StatusCode() int }
	if errors.As(err, &httpErr) {
		code := httpErr.StatusCode()
		if code == http.StatusTooManyRequests || code == http.StatusInternalServerError || code == http.StatusBadGateway || code == http.StatusServiceUnavailable || code == http.StatusGatewayTimeout {
			return true
		}
	}
	lower := strings.ToLower(err.Error())
	return strings.Contains(lower, "backend unavailable") ||
		strings.Contains(lower, "all the 1 backends") ||
		strings.Contains(lower, "connection refused") ||
		strings.Contains(lower, "connection reset") ||
		strings.Contains(lower, "timeout") ||
		strings.Contains(lower, "temporarily unavailable")
}

func statusFromQueryRangeWindowErr(err error) int {
	var httpErr interface{ StatusCode() int }
	if errors.As(err, &httpErr) {
		return httpErr.StatusCode()
	}
	return statusFromUpstreamErr(err)
}

func queryRangeWindowRetryBackoff(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	backoff := queryRangeRetryMinBackoff << (attempt - 1)
	if backoff > queryRangeRetryMaxBackoff {
		return queryRangeRetryMaxBackoff
	}
	return backoff
}

func (p *Proxy) queryRangeWindowTTL(windowEndNs int64) time.Duration {
	cutoff := time.Now().Add(-p.queryRangeFreshness).UnixNano()
	if windowEndNs <= cutoff {
		return p.queryRangeHistoryCacheTTL
	}
	return p.queryRangeRecentCacheTTL
}

func (p *Proxy) queryRangeWindowCacheKey(
	r *http.Request,
	logsqlQuery string,
	queryLimit string,
	window queryRangeWindow,
	categorizedLabels bool,
	emitStructuredMetadata bool,
) string {
	return strings.Join([]string{
		"query_range_window",
		r.Header.Get("X-Scope-OrgID"),
		logsqlQuery,
		r.FormValue("direction"),
		queryLimit,
		strconv.FormatInt(window.startNs, 10),
		strconv.FormatInt(window.endNs, 10),
		strconv.FormatBool(categorizedLabels),
		strconv.FormatBool(emitStructuredMetadata),
	}, ":")
}

func (p *Proxy) vlLogsToLokiWindowEntries(body []byte, originalQuery string, categorizedLabels bool, emitStructuredMetadata bool) []queryRangeWindowEntry {
	entries := make([]queryRangeWindowEntry, 0, len(body)/256+1)
	start := 0
	for start < len(body) {
		end := start
		for end < len(body) && body[end] != '\n' {
			end++
		}
		line := body[start:end]
		if end < len(body) {
			start = end + 1
		} else {
			start = len(body)
		}
		for len(line) > 0 && (line[0] == ' ' || line[0] == '\t' || line[0] == '\r') {
			line = line[1:]
		}
		for len(line) > 0 && (line[len(line)-1] == ' ' || line[len(line)-1] == '\t' || line[len(line)-1] == '\r') {
			line = line[:len(line)-1]
		}
		if len(line) == 0 {
			continue
		}

		entry := vlEntryPool.Get().(map[string]interface{})
		for k := range entry {
			delete(entry, k)
		}
		if err := json.Unmarshal(line, &entry); err != nil {
			vlEntryPool.Put(entry)
			continue
		}

		timeStr, ok := stringifyEntryValue(entry["_time"])
		if !ok || timeStr == "" {
			vlEntryPool.Put(entry)
			continue
		}
		tsNanos, ok := formatEntryTimestamp(timeStr)
		if !ok {
			vlEntryPool.Put(entry)
			continue
		}
		msg, _ := stringifyEntryValue(entry["_msg"])

		labels, structuredMetadata, parsedFields := p.classifyEntryFields(entry, originalQuery)
		translatedLabels := labels
		if !p.labelTranslator.IsPassthrough() {
			translatedLabels = p.labelTranslator.TranslateLabelsMap(labels)
		}
		ensureDetectedLevel(translatedLabels)
		ensureSyntheticServiceName(translatedLabels)

		value := buildStreamValue(tsNanos, msg, structuredMetadata, parsedFields, emitStructuredMetadata, categorizedLabels)
		valueTuple, ok := value.([]interface{})
		if !ok {
			vlEntryPool.Put(entry)
			continue
		}
		entries = append(entries, queryRangeWindowEntry{
			Stream: translatedLabels,
			Value:  valueTuple,
		})
		vlEntryPool.Put(entry)
	}
	return entries
}

func groupQueryRangeWindowEntries(entries []queryRangeWindowEntry, direction string) []map[string]interface{} {
	type streamEntry struct {
		labels map[string]string
		values []interface{}
	}
	streams := make(map[string]*streamEntry, len(entries)/2+1)
	for _, entry := range entries {
		key := canonicalLabelsKey(entry.Stream)
		stream, ok := streams[key]
		if !ok {
			stream = &streamEntry{
				labels: entry.Stream,
				values: make([]interface{}, 0, 8),
			}
			streams[key] = stream
		}
		stream.values = append(stream.values, entry.Value)
	}
	// Sort stream keys for deterministic output order. Go map iteration is
	// non-deterministic; without this, stream order fluctuates across requests
	// whenever the same query spans multiple windows.
	keys := make([]string, 0, len(streams))
	for k := range streams {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	out := make([]map[string]interface{}, 0, len(streams))
	for _, k := range keys {
		stream := streams[k]
		// Sort values within each stream by timestamp to stabilise window
		// boundaries from parallel fetches. Forward = ascending; backward
		// (default) = descending to match Loki's newest-first contract.
		forward := direction == "forward"
		sort.SliceStable(stream.values, func(i, j int) bool {
			vi, _ := stream.values[i].([]interface{})
			vj, _ := stream.values[j].([]interface{})
			if len(vi) == 0 || len(vj) == 0 {
				return false
			}
			ti, _ := vi[0].(string)
			tj, _ := vj[0].(string)
			if forward {
				return ti < tj
			}
			return ti > tj
		})
		out = append(out, map[string]interface{}{
			"stream": stream.labels,
			"values": stream.values,
		})
	}
	return out
}

func withQueryDirectionSort(logsqlQuery, direction string) string {
	if direction == "forward" {
		return logsqlQuery + " | sort by (_time)"
	}
	return logsqlQuery + " | sort by (_time desc)"
}

func splitQueryRangeWindows(startNs, endNs int64, interval time.Duration, direction string) []queryRangeWindow {
	return splitQueryRangeWindowsWithOptions(startNs, endNs, interval, direction, false)
}

func splitQueryRangeWindowsWithOptions(startNs, endNs int64, interval time.Duration, direction string, align bool) []queryRangeWindow {
	if interval <= 0 || endNs < startNs {
		return nil
	}
	step := interval.Nanoseconds()
	if step <= 0 {
		return nil
	}
	windows := make([]queryRangeWindow, 0, min(int((endNs-startNs)/step)+1, maxQueryRangeWindows))
	cursor := startNs
	if align {
		if rem := cursor % step; rem != 0 {
			if rem < 0 {
				rem += step
			}
			firstEnd := cursor + (step - rem) - 1
			if firstEnd >= cursor {
				if firstEnd > endNs {
					firstEnd = endNs
				}
				windows = append(windows, queryRangeWindow{startNs: cursor, endNs: firstEnd})
				if firstEnd == endNs || len(windows) >= maxQueryRangeWindows {
					if direction != "forward" {
						for i, j := 0, len(windows)-1; i < j; i, j = i+1, j-1 {
							windows[i], windows[j] = windows[j], windows[i]
						}
					}
					return windows
				}
				cursor = firstEnd + 1
			}
		}
	}
	for cursor <= endNs && len(windows) < maxQueryRangeWindows {
		windowEnd := cursor + step - 1
		if windowEnd < cursor || windowEnd > endNs {
			windowEnd = endNs
		}
		windows = append(windows, queryRangeWindow{startNs: cursor, endNs: windowEnd})
		if windowEnd == math.MaxInt64 {
			break
		}
		cursor = windowEnd + 1
	}
	if direction != "forward" {
		for i, j := 0, len(windows)-1; i < j; i, j = i+1, j-1 {
			windows[i], windows[j] = windows[j], windows[i]
		}
	}
	return windows
}

func parseLokiTimeRangeToUnixNano(startRaw, endRaw string) (int64, int64, bool) {
	startNs, ok := parseLokiTimeToUnixNano(startRaw)
	if !ok {
		return 0, 0, false
	}
	endNs, ok := parseLokiTimeToUnixNano(endRaw)
	if !ok || endNs < startNs {
		return 0, 0, false
	}
	return startNs, endNs, true
}

func parseLokiTimeToUnixNano(raw string) (int64, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, false
	}
	if parsed, err := time.Parse(time.RFC3339Nano, raw); err == nil {
		return parsed.UnixNano(), true
	}
	if parsed, err := time.Parse(time.RFC3339, raw); err == nil {
		return parsed.UnixNano(), true
	}
	if raw == "now" {
		return time.Now().UnixNano(), true
	}
	if strings.HasPrefix(raw, "now-") || strings.HasPrefix(raw, "now+") {
		sign := raw[3:4]
		d, err := time.ParseDuration(raw[4:])
		if err == nil {
			if sign == "-" {
				return time.Now().Add(-d).UnixNano(), true
			}
			return time.Now().Add(d).UnixNano(), true
		}
	}
	if nInt, err := strconv.ParseInt(raw, 10, 64); err == nil {
		return normalizeLokiIntTimeToUnixNano(nInt), true
	}
	n, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		return 0, false
	}
	return normalizeLokiNumericTimeToUnixNano(n), true
}

func normalizeLokiIntTimeToUnixNano(v int64) int64 {
	abs := v
	if abs < 0 {
		abs = -abs
	}
	switch {
	case abs < 1e11:
		return v * int64(time.Second)
	case abs < 1e14:
		return v * int64(time.Millisecond)
	case abs < 1e17:
		return v * int64(time.Microsecond)
	default:
		return v
	}
}

func normalizeLokiNumericTimeToUnixNano(v float64) int64 {
	abs := math.Abs(v)
	switch {
	case abs < 1e11:
		return int64(v * float64(time.Second))
	case abs < 1e14:
		return int64(v * float64(time.Millisecond))
	case abs < 1e17:
		return int64(v * float64(time.Microsecond))
	default:
		return int64(v)
	}
}

func (p *Proxy) queryRangeWindowParallelLimit() int {
	if !p.queryRangeAdaptiveParallel {
		if p.queryRangeMaxParallel <= 0 {
			return 1
		}
		return p.queryRangeMaxParallel
	}
	p.queryRangeAdaptiveMu.Lock()
	defer p.queryRangeAdaptiveMu.Unlock()
	if p.queryRangeParallelCurrent < p.queryRangeParallelMin || p.queryRangeParallelCurrent > p.queryRangeParallelMax {
		p.queryRangeParallelCurrent = p.queryRangeParallelMin
	}
	if p.queryRangeParallelCurrent <= 0 {
		p.queryRangeParallelCurrent = 1
	}
	return p.queryRangeParallelCurrent
}

func (p *Proxy) observeQueryRangeWindowFetch(latency time.Duration, failed bool) {
	if !p.queryRangeAdaptiveParallel {
		return
	}

	p.queryRangeAdaptiveMu.Lock()
	defer p.queryRangeAdaptiveMu.Unlock()

	if latency < 0 {
		latency = 0
	}
	const alpha = 0.2
	if p.queryRangeLatencyEWMA <= 0 {
		p.queryRangeLatencyEWMA = latency
	} else {
		p.queryRangeLatencyEWMA = time.Duration((1-alpha)*float64(p.queryRangeLatencyEWMA) + alpha*float64(latency))
	}
	errSample := 0.0
	if failed {
		errSample = 1.0
	}
	p.queryRangeErrorEWMA = (1-alpha)*p.queryRangeErrorEWMA + alpha*errSample

	now := time.Now()
	if !p.queryRangeAdaptiveLastAdjust.IsZero() && now.Sub(p.queryRangeAdaptiveLastAdjust) < p.queryRangeAdaptiveCooldown {
		p.metrics.RecordQueryRangeAdaptiveState(p.queryRangeParallelCurrent, p.queryRangeLatencyEWMA, p.queryRangeErrorEWMA)
		return
	}

	oldCurrent := p.queryRangeParallelCurrent
	shouldBackoff := p.queryRangeErrorEWMA >= p.queryRangeErrorBackoffThreshold ||
		(p.queryRangeLatencyEWMA > 0 && p.queryRangeLatencyEWMA >= p.queryRangeLatencyBackoff)
	shouldIncrease := p.queryRangeErrorEWMA <= p.queryRangeErrorBackoffThreshold/2 &&
		p.queryRangeLatencyEWMA > 0 &&
		p.queryRangeLatencyEWMA <= p.queryRangeLatencyTarget

	if shouldBackoff && p.queryRangeParallelCurrent > p.queryRangeParallelMin {
		p.queryRangeParallelCurrent = max(p.queryRangeParallelMin, p.queryRangeParallelCurrent/2)
	} else if shouldIncrease && p.queryRangeParallelCurrent < p.queryRangeParallelMax {
		p.queryRangeParallelCurrent++
	}

	if p.queryRangeParallelCurrent != oldCurrent {
		p.queryRangeAdaptiveLastAdjust = now
	}
	p.metrics.RecordQueryRangeAdaptiveState(p.queryRangeParallelCurrent, p.queryRangeLatencyEWMA, p.queryRangeErrorEWMA)
}

func (p *Proxy) forceQueryRangeParallelBackoff() {
	if !p.queryRangeAdaptiveParallel {
		return
	}

	p.queryRangeAdaptiveMu.Lock()
	defer p.queryRangeAdaptiveMu.Unlock()

	if p.queryRangeParallelCurrent <= p.queryRangeParallelMin {
		return
	}
	p.queryRangeParallelCurrent = max(p.queryRangeParallelMin, p.queryRangeParallelCurrent/2)
	p.queryRangeAdaptiveLastAdjust = time.Now()
	p.metrics.RecordQueryRangeAdaptiveState(p.queryRangeParallelCurrent, p.queryRangeLatencyEWMA, p.queryRangeErrorEWMA)
}

// DownstreamConnectionPressure reports whether this proxy instance is currently
// experiencing enough query_range backpressure to justify nudging sticky
// downstream HTTP/1.x clients to reconnect elsewhere.
func (p *Proxy) DownstreamConnectionPressure() bool {
	if !p.queryRangeAdaptiveParallel {
		return false
	}

	p.queryRangeAdaptiveMu.Lock()
	defer p.queryRangeAdaptiveMu.Unlock()

	if p.queryRangeErrorEWMA > 0 && p.queryRangeErrorEWMA >= p.queryRangeErrorBackoffThreshold {
		return true
	}
	if p.queryRangeLatencyEWMA > 0 && p.queryRangeLatencyEWMA >= p.queryRangeLatencyBackoff {
		return true
	}
	return p.queryRangeParallelCurrent <= p.queryRangeParallelMin &&
		p.queryRangeLatencyEWMA > 0 &&
		p.queryRangeLatencyEWMA >= p.queryRangeLatencyTarget
}
