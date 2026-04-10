package proxy

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"
)

const (
	maxQueryRangeWindows          = 4096
	queryRangeCollectedInitialCap = 1024
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
	windows := splitQueryRangeWindows(startNs, endNs, p.queryRangeSplitInterval, r.FormValue("direction"))
	if len(windows) <= 1 {
		return false
	}

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

	p.metrics.RecordQueryRangeWindowCount(len(windows))

	remaining := limitValue
	// Keep preallocation constant-sized to avoid any user-influenced allocation growth.
	collected := make([]queryRangeWindowEntry, 0, queryRangeCollectedInitialCap)
	for i := 0; i < len(windows) && remaining > 0; {
		parallel := p.queryRangeWindowParallelLimit()
		batchSize := min(parallel, len(windows)-i)
		if batchSize <= 0 {
			batchSize = 1
		}
		batch := windows[i : i+batchSize]
		limitForBatch := remaining
		results, err := p.fetchQueryRangeWindowBatch(r, logsqlQuery, queryLimit, batch, limitForBatch, batchSize, categorizedLabels, emitStructuredMetadata)
		if err != nil {
			p.writeError(w, statusFromUpstreamErr(err), err.Error())
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
	streams := groupQueryRangeWindowEntries(collected)
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
		"data": map[string]interface{}{
			"resultType": "streams",
			"result":     streams,
			"stats":      map[string]interface{}{},
		},
	})
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(result)
	return true
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
	fetchStart := time.Now()
	status, body, err := p.vlPostCoalesced(ctx, coalesceKey, "/select/logsql/query", params)
	fetchDuration := time.Since(fetchStart)
	p.metrics.RecordQueryRangeWindowFetchDuration(fetchDuration)
	if err != nil {
		p.observeQueryRangeWindowFetch(fetchDuration, true)
		return queryRangeWindowCacheEntry{}, err
	}
	if status >= 400 {
		p.observeQueryRangeWindowFetch(fetchDuration, true)
		msg := strings.TrimSpace(string(body))
		if msg == "" {
			msg = fmt.Sprintf("VL backend returned %d", status)
		}
		return queryRangeWindowCacheEntry{}, errors.New(msg)
	}
	p.observeQueryRangeWindowFetch(fetchDuration, false)

	entries := p.vlLogsToLokiWindowEntries(body, r.FormValue("query"), categorizedLabels, emitStructuredMetadata)
	entry := queryRangeWindowCacheEntry{Entries: entries}
	if ttl := p.queryRangeWindowTTL(window.endNs); ttl > 0 {
		if encoded, err := json.Marshal(entry); err == nil {
			p.cache.SetWithTTL(cacheKey, encoded, ttl)
		}
	}
	return entry, nil
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
	for i := 0; i <= len(body); i++ {
		if i < len(body) && body[i] != '\n' {
			continue
		}
		line := body[start:i]
		start = i + 1
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

func groupQueryRangeWindowEntries(entries []queryRangeWindowEntry) []map[string]interface{} {
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
	out := make([]map[string]interface{}, 0, len(streams))
	for _, stream := range streams {
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
	if interval <= 0 || endNs < startNs {
		return nil
	}
	step := interval.Nanoseconds()
	if step <= 0 {
		return nil
	}
	windows := make([]queryRangeWindow, 0, min(int((endNs-startNs)/step)+1, maxQueryRangeWindows))
	cursor := startNs
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
