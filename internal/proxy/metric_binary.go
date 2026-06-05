package proxy

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	fj "github.com/valyala/fastjson"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
)

// --- Stats query proxying ---

func (p *Proxy) proxyStatsQueryRange(w http.ResponseWriter, r *http.Request, logsqlQuery string) {
	originalLogql := resolveGrafanaRangeTemplateTokens(r.FormValue("query"), r.FormValue("start"), r.FormValue("end"), r.FormValue("step"))

	topK, topKDesc, hasTopK := parseTopKWrapper(originalLogql)

	out := http.ResponseWriter(w)
	var topKBuf *bufferedResponseWriter
	if hasTopK {
		topKBuf = &bufferedResponseWriter{}
		out = topKBuf
	}

	if p.handleStatsCompatRange(out, r, originalLogql, logsqlQuery) {
		if hasTopK {
			writeTopKFiltered(w, topKBuf, topK, topKDesc, "matrix")
		}
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
		_ = p.proxyStatsQueryRangeDirect(buf, shiftedR, logsqlQuery)
		body := trimStatsQueryRangeResponseFromStart(buf.body, origStartNs)
		if hasTopK {
			body = applyTopKToMatrix(body, topK, topKDesc)
		}
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

	// Strip | delete __error__, __error_details__ for any stats query — it removes
	// a field per row but never filters logs; counts are identical without it.
	effectiveQuery := logsqlQuery
	if spec, ok := parseStatsCompatSpec(logsqlQuery); ok {
		noDelete := strings.TrimSpace(drilldownDeletePipeRE.ReplaceAllString(spec.BaseQuery, ""))
		if noDelete != spec.BaseQuery {
			effectiveQuery = noDelete + logsqlQuery[len(spec.BaseQuery):]
		}
	}

	// Route Drilldown single-field count queries to the appropriate fast path.
	// Gate behind isGrafanaDrilldownRequest: Explore/API clients with the same
	// LogQL pattern must not be subject to the 500-series cap or two-phase fallback.
	//
	// Two sub-cases:
	//  1. No parser stages (column-indexed fields: stream labels, OTel attrs) →
	//     proxyStatsQueryRangeDrilldown (batcher / two-phase / hybrid).
	//  2. With parser stages (body-embedded fields: trace_id, span_id, level from
	//     logfmt) → proxyStatsQueryRangeDrilldownParserDirect (one direct VL call
	//     with parser preserved and VL-side | limit 500).
	if cleanBase, field, ok := detectDrilldownSingleField(effectiveQuery); ok && isGrafanaDrilldownRequest(r) {
		p.proxyStatsQueryRangeDrilldown(out, r, effectiveQuery, cleanBase, field)
	} else if isGrafanaDrilldownRequest(r) {
		if cleanBase, field, ok := detectDrilldownSingleFieldWithParser(effectiveQuery); ok {
			p.proxyStatsQueryRangeDrilldownParserDirect(out, r, effectiveQuery, cleanBase, field)
		} else {
			p.proxyStatsQueryRangeDirect(out, r, effectiveQuery)
		}
	} else {
		p.proxyStatsQueryRangeDirect(out, r, effectiveQuery)
	}

	if hasTopK {
		writeTopKFiltered(w, topKBuf, topK, topKDesc, "matrix")
	}
}

// proxyStatsQueryRangeDirect issues the VL stats_query_range request directly,
// bypassing the compat layer and the rate-shift gate. Call this when the caller
// has already applied any necessary start shift (e.g. proxyBareParserMetricViaStats).
// maxStatsQueryRangeBytes caps the VL stats_query_range response size in the
// direct (non-coalesced) path. High-cardinality by() clauses (e.g. by(extracted_float_field))
// on broad selectors can return tens of megabytes per query; the Drilldown Fields
// page issues ~20 such queries concurrently, causing OOM. When exceeded, an empty
// Loki matrix is returned rather than buffering an unbounded response.
const maxStatsQueryRangeBytes = 16 << 20 // 16 MB

// emptyLokiMatrix is returned when a VL stats_query_range response exceeds
// maxStatsQueryRangeBytes. Grafana renders it as a blank (no-data) series.
var emptyLokiMatrix = []byte(`{"status":"success","data":{"resultType":"matrix","result":[]}}`)

func (p *Proxy) proxyStatsQueryRangeDirect(w http.ResponseWriter, r *http.Request, logsqlQuery string) (capExceeded bool) {
	// For the underscore proxy, expand dotted by() labels (e.g. "service.name") to
	// also include their underscore equivalents (e.g. "service_name"). Loki-push data
	// stores these as stream labels under the underscore name; OTel data uses the dotted
	// field. VL groups by whichever exists; the response translation coalesces the two
	// fields into a single Loki label, preferring the non-empty value.
	origGroupBy := parseOriginalByLabels(r.FormValue("query"))
	logsqlQuery = p.addUnderscorefallbackByLabels(logsqlQuery, origGroupBy)

	// Keep metric query_range as a single backend request. Window splitting and
	// window-level cache reuse are for raw log queries only.
	params := buildStatsQueryRangeParams(logsqlQuery, r.FormValue("start"), r.FormValue("end"), r.FormValue("step"))

	// Use vlPost directly (not coalesced) so readBodyLimited can bound the response
	// before the full body is allocated. The coalescer's 256 MB cap is too generous
	// when many concurrent field queries each produce a large stats response.
	resp, err := p.vlPost(r.Context(), "/select/logsql/stats_query_range", params)
	if err != nil {
		p.writeError(w, statusFromUpstreamErr(err), err.Error())
		return false
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		errBody, _ := readBodyLimited(resp.Body, maxUpstreamErrorBodyBytes)
		p.writeError(w, resp.StatusCode, extractVLErrorMsg(errBody))
		return false
	}

	body, bodyErr := readBodyLimited(resp.Body, maxStatsQueryRangeBytes)
	if bodyErr != nil {
		// Response exceeds the per-request cap — return empty matrix rather than OOMing.
		// This protects against high-cardinality by() clauses (e.g. by(float_field)) on
		// broad stream selectors producing tens of megabytes per concurrent field query.
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(emptyLokiMatrix)
		return true
	}

	// Single parse pass: filter points to the requested end time AND translate
	// metric labels. Replaces two sequential fastjson parses (trim then translate).
	var keepFn func(int64) bool
	if endNs, ok := parseLokiTimeToUnixNano(r.FormValue("end")); ok {
		keepFn = func(tsNs int64) bool { return tsNs <= endNs }
	}
	body = p.trimAndTranslateStatsQRFJ(r.Context(), body, keepFn, r.FormValue("query"))
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(wrapAsLokiResponse(body, "matrix"))
	return false
}

// maxDrilldownResponseBytes is the per-request body cap for Drilldown single-field
// count queries on the DIRECT path (when | limit 500 per-bucket is effective).
//
// VL applies | limit per-bucket, not globally. For high-cardinality short-lived
// fields (trace_id, span_id, session_id): each bucket has different unique values, so
// 720 buckets × 500/bucket = 360k unique series (~32.8 MB) even with | limit 500.
// When the direct-path response exceeds this cap, drilldownTwoPhase takes over:
//
//	Phase 1 — single-bucket query (step = entire range) → global top-N values, tiny
//	Phase 2 — range query filtered via field:in(...) → ≤500 series with original step
//
// Do NOT inflate the step sent to VL: changing the step breaks Grafana's timestamp
// alignment expectations and causes "no data" for all fields.
const maxDrilldownResponseBytes = 32 << 20 // 32 MB — triggers two-phase fallback if exceeded

// maxDrilldownSeries is the maximum series returned for Drilldown single-field
// count queries. Matches Loki's default max_query_series (500) for Drilldown
// requests — Loki returns partial results with a series-limit notice at this
// threshold rather than an error, allowing Grafana Drilldown to display real
// per-value histogram data.
const maxDrilldownSeries = 500

// maxDrilldownPhase2Values caps the number of field values passed to the Phase 2
// in() filter. VL's default -search.maxQueryLen is 16384 bytes; 200 UUID-length
// values (≈38 bytes each) consume ~7.6 KB, leaving ample room for the base query
// and stats clause. Drilldown's field-values breakdown UI caps at 100 visible series,
// so 200 is already 2× what the UI can display.
const maxDrilldownPhase2Values = 200

// isGrafanaDrilldownRequest reports whether r originates from Grafana Logs Drilldown.
// Grafana Logs Drilldown sets supportingQueryType="grafana-lokiexplore-app" on every
// Loki data query; the datasource Go backend translates this to the HTTP header
// X-Query-Tags: Source=grafana-lokiexplore-app. This header gates Drilldown-specific
// optimizations (500-series cap, two-phase fallback) so that identical query patterns
// from Explore or direct API clients are not subject to the series limit.
func isGrafanaDrilldownRequest(r *http.Request) bool {
	tag := strings.ToLower(parseGrafanaSourceTag(r.Header.Values("X-Query-Tags")))
	return strings.Contains(tag, "lokiexplore") || strings.Contains(tag, "drilldown")
}

// isLikelyHighCardinalityField returns true for field names whose values are
// typically unique per log entry — trace/span IDs, session tokens, request keys.
// For these fields the direct VL path (| limit 500 per bucket) still produces
// numBuckets×500 unique series (e.g. 720×500=360k for a 12h/60s range), which
// overflows maxDrilldownResponseBytes and forces two-phase fallback every time.
// Detecting them eagerly skips the wasted 32 MB read-and-reject round trip.
func isLikelyHighCardinalityField(name string) bool {
	lower := strings.ToLower(name)
	// Exact common names first (fast path, no allocation).
	switch lower {
	case "trace_id", "span_id", "parent_id", "session_id",
		"request_id", "correlation_id", "transaction_id",
		"traceid", "spanid", "parentid",
		"trace.id", "span.id":
		return true
	}
	// Suffix patterns — anything ending in _id/.id/_uuid/_token/_hash/_key is
	// likely to carry a unique-per-request value.
	return strings.HasSuffix(lower, "_id") ||
		strings.HasSuffix(lower, ".id") ||
		strings.HasSuffix(lower, "_uuid") ||
		strings.HasSuffix(lower, ".uuid") ||
		strings.HasSuffix(lower, "_token") ||
		strings.HasSuffix(lower, "_hash") ||
		strings.HasSuffix(lower, "_key")
}

// drilldownCardinalityCache remembers which {orgID, base, field} triples have
// already overflowed maxDrilldownResponseBytes on the direct path. On subsequent
// requests (e.g. after a Grafana filter change re-fires all N field queries), the
// cache lets proxyStatsQueryRangeDrilldown skip the direct attempt and go straight
// to the cheaper two-phase path.
type drilldownCardinalityCache struct {
	mu      sync.RWMutex
	entries map[string]time.Time // key → expiry
}

const drilldownCardinalityCacheTTL = 5 * time.Minute

func newDrilldownCardinalityCache() *drilldownCardinalityCache {
	return &drilldownCardinalityCache{entries: make(map[string]time.Time)}
}

func (c *drilldownCardinalityCache) key(orgID, base, field string) string {
	return orgID + "\x00" + base + "\x00" + field
}

func (c *drilldownCardinalityCache) isHigh(orgID, base, field string) bool {
	k := c.key(orgID, base, field)
	c.mu.RLock()
	exp, ok := c.entries[k]
	c.mu.RUnlock()
	return ok && time.Now().Before(exp)
}

func (c *drilldownCardinalityCache) markHigh(orgID, base, field string) {
	k := c.key(orgID, base, field)
	c.mu.Lock()
	c.entries[k] = time.Now().Add(drilldownCardinalityCacheTTL)
	c.mu.Unlock()
}

// extractStatsGroupByFields returns the field names from a LogsQL "| stats by (...)"
// clause. Used by the field batcher to collect the VL-side field names (including
// underscore fallbacks added by addUnderscorefallbackByLabels) for the batched query.
func extractStatsGroupByFields(logsqlQuery string) []string {
	spec, ok := parseStatsCompatSpec(logsqlQuery)
	if !ok {
		return nil
	}
	return spec.GroupBy
}

// isHighCardinalityFieldName returns true for fields whose values are nearly
// unique per log entry (trace_id, span_id, request_id, …). Including these in
// a batch stats query multiplies the combination space by the number of unique
// values (potentially millions) and saturates VL CPU. Beyond 6 h the proxy
// returns empty rather than issuing the query.
func isHighCardinalityFieldName(name string) bool {
	return strings.HasSuffix(name, "_id") || strings.HasSuffix(name, "_uid")
}

// appendDrilldownSeriesLimit rewrites a VL stats count() query to sort by count
// descending and limit to N unique groups. This pushes the cardinality cap into
// VL so only the top-N series are transmitted to the proxy — critical for
// high-cardinality fields (trace_id, span_id) where unbound VL responses are
// 50 MB+ for 300k+ unique values over long time ranges.
// Returns query unchanged if it does not end with the bare count() token.
func appendDrilldownSeriesLimit(query string, limit int) string {
	q := strings.TrimSpace(query)
	if !strings.HasSuffix(q, "count()") {
		return query
	}
	return q + " as _c | sort by (_c desc) | limit " + strconv.Itoa(limit)
}

// drilldownShouldEagerTwoPhase returns true when the proxy can predict that the
// direct VL path (| limit 500 per bucket → readBodyLimited) will overflow
// maxDrilldownResponseBytes, making a straight two-phase call cheaper. Two
// independent signals trigger eager two-phase:
//
//  1. Known high-cardinality field name (trace_id, span_id, *_id, *_token, …) —
//     these fields carry unique-per-request values, so each time bucket produces a
//     different set of values; | limit 500 per bucket ≠ global top 500.
//
//  2. Cardinality cache: a previous request for {orgID, cleanBase, field} already
//     overflowed the cap, so subsequent re-renders (triggered by Grafana filter/time
//     changes) skip the direct attempt entirely.
func (p *Proxy) drilldownShouldEagerTwoPhase(r *http.Request, cleanBase, field string) bool {
	if isLikelyHighCardinalityField(field) {
		return true
	}
	orgID := r.Header.Get("X-Scope-OrgID")
	return p.drilldownCardCache != nil && p.drilldownCardCache.isHigh(orgID, cleanBase, field)
}

// drilldownStatsCacheTTL is the response cache TTL for Drilldown single-field
// count queries. Drilldown fires 20–100 concurrent query_range calls on every
// tab open or re-render; a 5-minute cache window eliminates all VL calls for
// re-renders and periodic Grafana auto-refreshes within that window. The cache
// is checked before the concurrency semaphore so cached responses bypass the
// semaphore entirely. With coarsened steps and bucketed timestamps the key is
// stable across minor Grafana panel-width changes so the window is effectively
// used.
const drilldownStatsCacheTTL = 5 * time.Minute

// maxDrilldownStatsBuckets caps the number of time buckets sent to VL in a
// Drilldown stats_query_range call for ranges > drilldownHybridThreshold.
// maxDrilldownStatsBucketsShort raises the cap for ranges ≤ drilldownHybridThreshold:
// more buckets reduce the expansion factor (coarseStep/fineStep) so sub-bars reflect
// real VL variation and the precision slider in Grafana Drilldown has a visible effect.
//
// Both caps are aligned at 120 so the transition at drilldownHybridThreshold (12h)
// is seamless — a 13h query produces ~120 bars at ~390s step instead of the previous
// ~30 bars at ~1560s step. With high-card groupings already capped at maxDrilldownSeries
// (500 series), the worst-case stats matrix is 120 × 500 = 60k cells ≈ 3 MB on the wire,
// well within maxDrilldownResponseBytes (32 MB).
const (
	maxDrilldownStatsBuckets      = 120
	maxDrilldownStatsBucketsShort = 120
)

// drilldownLowCardThreshold is the cardinality (distinct grouped-field values)
// at or below which the hybrid path skips step coarsening — for stream labels
// like app/env/cluster/level (1–10 values) the result set stays small even at
// native resolution, so we match Loki's per-bucket point count instead of the
// 30-bucket cap.
const drilldownLowCardThreshold = 50

// drilldownLowCardStatsBuckets caps buckets for the low-cardinality hybrid path.
// 1000 buckets × 50-value cardinality = 50k row response, well within the 32 MB
// drilldown cap even with JSON serialization overhead. Crucially, this is large
// enough that Grafana's native Drilldown steps (worst case 2d→300s = 576 buckets,
// 7d→1200s = 504 buckets) are always preserved at the client's requested resolution.
// The floor only kicks in when a client sends an absurdly fine step (e.g. step=1s at 7d).
const drilldownLowCardStatsBuckets = 1000

// drilldownHighCardStatsBuckets is the tighter bucket cap used when the by-field
// is detected as truly high-cardinality (trace_id, span_id, *_id, *_uid). VL's
// stats pipe materializes one entry per (bucket × distinct-value) pair before the
// top-N heap can trim, so the memory cost scales with cardinality × buckets.
// 30 buckets × ~500k unique trace_ids ≈ 15M map entries ≈ 750 MB — within VL's 40%
// stats memory budget but tight enough that 120 buckets (3 GB) is risky.
const drilldownHighCardStatsBuckets = 30

// drilldownHybridThreshold is the range above which proxyStatsQueryRangeDrilldown
// switches to the hybrid path: field_values (O(column-index), ~30ms at any range)
// for value discovery over the full range, plus a capped stats_query_range for the
// recent histogram window only. Below this threshold the direct/two-phase path is used.
// Set to 12h so that 6h ranges (which Grafana's sliding "now-6h" window occasionally
// exceeds by ~30s due to time rounding) stay on the full-range non-hybrid path and
// show complete coverage rather than the ~1h adaptive histogram window.
const drilldownHybridThreshold = 12 * time.Hour

// drilldownFVEntry is a field value returned by the VL field_values endpoint,
// with its hit count for use in single-point matrix synthesis.
type drilldownFVEntry struct {
	Value string
	Hits  int64
}

// vlStatsNameKeyRE matches VL's internal __name__ column marker that appears in
// stats_query_range metric objects (e.g. "__name__":"_c",). VL adds it for
// Prometheus compatibility; Loki/Grafana clients don't expect it.
var vlStatsNameKeyRE = regexp.MustCompile(`"__name__":"[^"]*",?`)

// stripVLStatsNameKey removes VL's __name__ column marker from a stats_query_range
// body. Called in the hybrid path where label translation is skipped — we need to
// clean __name__ without invoking ensureDetectedLevel which would rename level→detected_level.
func stripVLStatsNameKey(body []byte) []byte {
	if !bytes.Contains(body, []byte(`"__name__"`)) {
		return body
	}
	return vlStatsNameKeyRE.ReplaceAll(body, nil)
}

// renameStatsBodyMetricKey renames a JSON metric key in a stats_query_range
// response body. Used in the hybrid drilldown path when the VL field name
// (e.g. "level") differs from the Loki label name (e.g. "detected_level").
func renameStatsBodyMetricKey(body []byte, from, to string) []byte {
	if from == to || len(body) == 0 {
		return body
	}
	return bytes.ReplaceAll(body, []byte(`"`+from+`":`), []byte(`"`+to+`":`))
}

// synthesizeDrilldownMatrix builds a Loki matrix response with one data point per
// field value at endSec. Used when stats_query_range is skipped (high-cardinality)
// or as the fallback when VL returns an error.
// drilldownSynthesizeBuckets is the number of stub buckets per series for the
// synthesize-only path. Spreading the total hits across this many points produces
// a visible "presence" band in Grafana even when field_values cannot give us a
// real histogram (high-cardinality fields where the limit truncated hits to 0).
// Matches maxDrilldownStatsBuckets so the high-card synthesize path keeps the
// same horizontal resolution as the stats path at wide ranges.
const drilldownSynthesizeBuckets = maxDrilldownStatsBuckets

func synthesizeDrilldownMatrix(lokiField string, entries []drilldownFVEntry, endSec int64) []byte {
	return synthesizeDrilldownMatrixSpread(lokiField, entries, endSec, endSec, 0)
}

// synthesizeDrilldownMatrixSpread builds a Loki matrix response and, when
// (startSec, stepSec) describe a non-degenerate range, evenly distributes each
// entry's total hits across drilldownSynthesizeBuckets points spanning that range.
// Used by the hybrid path's high-cardinality and fallback branches so the chart
// renders as a populated band rather than a single dot at the right edge.
//
// stepSec=0 (or startSec==endSec) falls back to a single point at endSec for
// backwards compatibility with callers that have no range context.
func synthesizeDrilldownMatrixSpread(lokiField string, entries []drilldownFVEntry, startSec, endSec, stepSec int64) []byte {
	if len(entries) == 0 {
		return emptyLokiMatrix
	}
	// Number of stub points per series and the step between them. When the caller
	// supplies a usable range, spread across drilldownSynthesizeBuckets points
	// anchored so the LAST point lands exactly at endSec.
	nPoints := int64(1)
	bucketStep := int64(0)
	firstTs := endSec
	if stepSec > 0 && endSec > startSec {
		rangeSec := endSec - startSec
		bucketStep = rangeSec / drilldownSynthesizeBuckets
		if bucketStep < stepSec {
			bucketStep = stepSec
		}
		// Snap bucketStep up to a step-aligned value so timestamps stay clean.
		if rem := bucketStep % stepSec; rem != 0 {
			bucketStep += stepSec - rem
		}
		nPoints = rangeSec/bucketStep + 1
		if nPoints < 1 {
			nPoints = 1
		}
		firstTs = endSec - (nPoints-1)*bucketStep
	}

	var b bytes.Buffer
	b.Grow(len(entries)*int(nPoints)*24 + 64)
	b.WriteString(`{"status":"success","data":{"resultType":"matrix","result":[`)
	for i, e := range entries {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(`{"metric":{"`)
		writeJSONEscaped(&b, lokiField)
		b.WriteString(`":"`)
		writeJSONEscaped(&b, e.Value)
		b.WriteString(`"},"values":[`)
		// Divide total hits across the stub buckets so the sum still matches
		// the total. Floor to 1 when hits>0 so the band remains visible.
		perBucket := e.Hits / nPoints
		if perBucket < 1 && e.Hits > 0 {
			perBucket = 1
		}
		perStr := strconv.FormatInt(perBucket, 10)
		for j := int64(0); j < nPoints; j++ {
			if j > 0 {
				b.WriteByte(',')
			}
			b.WriteByte('[')
			b.WriteString(strconv.FormatInt(firstTs+j*bucketStep, 10))
			b.WriteString(`,"`)
			b.WriteString(perStr)
			b.WriteString(`"]`)
		}
		b.WriteString(`]}`)
	}
	b.WriteString(`]}}`)
	return b.Bytes()
}

// writeJSONEscaped writes s to b, escaping only the characters that must be escaped
// in a JSON string value (quotes and backslashes). Field names/values from VL do not
// contain control characters so this fast path is sufficient.
func writeJSONEscaped(b *bytes.Buffer, s string) {
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c == '"' || c == '\\' {
			b.WriteByte('\\')
		}
		b.WriteByte(c)
	}
}

// mergeDrilldownWithFieldValues merges a stats_query_range Loki matrix (statsBody,
// covering only the recent histogram window) with field_values entries (covering the
// full requested range). Series present in statsBody keep their per-bucket histogram;
// series present only in field_values get evenly-distributed stub bars spanning the
// full range (not just a single spike at endSec) so Grafana renders a complete
// time-series chart rather than an empty-looking chart with one point at the right edge.
//
// For stats-covered series the stats bars already cover the histogram window; this
// function additionally prefixes averaged stubs from startSec to the first stats
// timestamp so the full range appears populated.
func mergeDrilldownWithFieldValues(statsBody []byte, lokiField string, entries []drilldownFVEntry, endSec, fullRangeNs, histStepNs int64) []byte {
	v, err := fj.ParseBytes(statsBody)
	if err != nil || v == nil {
		startSec := endSec - fullRangeNs/int64(time.Second)
		return synthesizeDrilldownMatrixSpread(lokiField, entries, startSec, endSec, histStepNs/int64(time.Second))
	}
	result := v.GetArray("data", "result")

	// Build value → total-hits lookup (field_values covers the full range).
	fvHits := make(map[string]int64, len(entries))
	for _, e := range entries {
		fvHits[e.Value] = e.Hits
	}

	// Track which values already have histogram data and find the stats window start.
	inStats := make(map[string]struct{}, len(result))
	histStartSec := endSec // sentinel; updated below
	for _, item := range result {
		val := string(item.GetStringBytes("metric", lokiField))
		if val != "" {
			inStats[val] = struct{}{}
		}
		// Find minimum timestamp across all stats series — that is the histogram window start.
		if values := item.GetArray("values"); len(values) > 0 {
			if arr := values[0].GetArray(); len(arr) >= 1 {
				if ts := arr[0].GetInt64(); ts > 0 && ts < histStartSec {
					histStartSec = ts
				}
			}
		}
	}

	// histStepNs here is the full-range stub step (stubStepNs from the caller).
	// The hybrid path no longer calls expandDrilldownStep after merge, so stubs
	// and stats may have different resolutions — this is intentional.
	startSec := endSec - fullRangeNs/int64(time.Second)
	var nFullStub, stubStepSec int64
	if histStepNs > 0 {
		stubStepSec = histStepNs / int64(time.Second)
		if stubStepSec < 1 {
			stubStepSec = 1
		}
		nFullStub = fullRangeNs / histStepNs // e.g. 24h/1h = 24 stubs for a 24h range with 1h histStep
	}

	// stubCount returns the per-bucket hit count averaged across the full range.
	stubCount := func(totalHits int64) int64 {
		if nFullStub < 1 {
			return totalHits
		}
		c := totalHits / nFullStub
		if c < 1 {
			c = 1
		}
		return c
	}

	// writeStubs emits nBuckets time-value pairs starting at tsSec with stepSec.
	writeStubs := func(buf *bytes.Buffer, tsSec, stepSec, nBuckets, hitsPerBucket int64, firstPoint *bool) {
		hitsStr := strconv.FormatInt(hitsPerBucket, 10)
		for i := int64(0); i < nBuckets; i++ {
			if !*firstPoint {
				buf.WriteByte(',')
			}
			*firstPoint = false
			buf.WriteByte('[')
			buf.WriteString(strconv.FormatInt(tsSec+i*stepSec, 10))
			buf.WriteString(`,"`)
			buf.WriteString(hitsStr)
			buf.WriteString(`"]`)
		}
	}

	endStr := strconv.FormatInt(endSec, 10)
	var b bytes.Buffer
	b.Grow(len(statsBody) + len(entries)*int(nFullStub+1)*24 + 64)
	b.WriteString(`{"status":"success","data":{"resultType":"matrix","result":[`)

	first := true
	for _, item := range result {
		if !first {
			b.WriteByte(',')
		}
		first = false

		// Prefix pre-stats window with averaged stubs so the full range is populated.
		totalHits := fvHits[string(item.GetStringBytes("metric", lokiField))]
		preStatsGapSec := histStartSec - startSec

		if preStatsGapSec > 0 && stubStepSec > 0 && totalHits > 0 {
			preNBuckets := preStatsGapSec / stubStepSec
			if preNBuckets > 0 {
				b.WriteString(`{"metric":`)
				if m := item.Get("metric"); m != nil {
					b.Write(m.MarshalTo(nil))
				} else {
					b.WriteString(`{}`)
				}
				b.WriteString(`,"values":[`)
				firstPoint := true
				hitsPerBucket := stubCount(totalHits)
				// Anchor stubs to histStartSec grid: last stub lands exactly at
				// histStartSec-stubStepSec, eliminating the 1-2 step gap that
				// would otherwise appear at the transition to the stats region.
				alignedPreStart := histStartSec - preNBuckets*stubStepSec
				writeStubs(&b, alignedPreStart, stubStepSec, preNBuckets, hitsPerBucket, &firstPoint)
				for _, vv := range item.GetArray("values") {
					if !firstPoint {
						b.WriteByte(',')
					}
					firstPoint = false
					b.Write(vv.MarshalTo(nil))
				}
				b.WriteString(`]}`)
				continue
			}
		}
		// No pre-stats gap or no fv data: emit stats item verbatim.
		b.Write(item.MarshalTo(nil))
	}

	// Emit field_values-only entries with full-range stub bars — BUT ONLY when
	// stats returned no real data. With real stats coverage, adding 500 stub-only
	// series (each value=1 across N buckets) creates a synthetic "flat baseline"
	// equal to ~500 in every time bucket. Grafana's stacked-bar Y-axis then
	// auto-scales to (500 baseline + real data spikes) — and the real per-bucket
	// variation looks like a thin band on top of a large flat floor. The user-
	// visible symptom is "24h chart shows only a few minutes of data" because
	// only the recent spikes stand out against the baseline.
	//
	// When stats has data, that data alone IS the useful signal. Skip stub-fill
	// so the chart's Y-axis reflects real per-bucket counts (typically 1-30 per
	// pod per coarse window) and bars span the actual range with visible variation.
	if len(inStats) == 0 {
		for _, e := range entries {
			if !first {
				b.WriteByte(',')
			}
			first = false
			b.WriteString(`{"metric":{"`)
			writeJSONEscaped(&b, lokiField)
			b.WriteString(`":"`)
			writeJSONEscaped(&b, e.Value)
			b.WriteString(`"},"values":[`)

			if nFullStub > 0 && stubStepSec > 0 {
				firstPoint := true
				// Align stub start UP to step boundary so the stub grid matches VL's
				// step-aligned stats grid. Without alignment, stats and stub timestamps
				// land at different offsets, doubling the time-axis tick count and
				// scattering data across cells Grafana cannot reconcile.
				alignedStart := startSec
				if rem := alignedStart % stubStepSec; rem != 0 {
					alignedStart += stubStepSec - rem
				}
				writeStubs(&b, alignedStart, stubStepSec, nFullStub, stubCount(e.Hits), &firstPoint)
			} else {
				// Fallback when we can't compute a grid: single point at endSec.
				b.WriteByte('[')
				b.WriteString(endStr)
				b.WriteString(`,"`)
				b.WriteString(strconv.FormatInt(e.Hits, 10))
				b.WriteString(`"]`)
			}
			b.WriteString(`]}`)
		}
	}
	b.WriteString(`]}}`)
	return b.Bytes()
}

// drilldownNiceSteps are the candidate step values to snap to after coarsening.
var drilldownNiceSteps = []time.Duration{
	30 * time.Second,
	60 * time.Second,
	2 * time.Minute,
	3 * time.Minute,
	5 * time.Minute,
	10 * time.Minute,
	15 * time.Minute,
	30 * time.Minute,
	time.Hour,
	2 * time.Hour,
	6 * time.Hour,
	12 * time.Hour,
	24 * time.Hour,
}

// highCardStepFloor enforces a tighter step floor on the hybrid path when the
// by-field is detected as truly high-cardinality (trace_id, span_id, *_id, *_uid).
// VL's stats pipe materializes one map entry per (bucket × distinct-value) pair
// before the top-N heap can trim, so memory cost scales with cardinality × buckets.
// Capping the number of buckets at drilldownHighCardStatsBuckets keeps the worst-
// case stats map within VL's 40% memory budget even for 500k+ unique values.
//
// Returns the original effectiveStepRaw unchanged if its parsed step already
// satisfies the floor. The returned step is snapped UP to the next "nice" duration
// for clean VL timestamps. Unparseable input passes through.
func highCardStepFloor(effectiveStepRaw string, rangeNs int64) string {
	d, ok := parsePositiveStepDuration(effectiveStepRaw)
	if !ok || rangeNs <= 0 {
		return effectiveStepRaw
	}
	hcMinStep := time.Duration(rangeNs) / drilldownHighCardStatsBuckets
	if hcMinStep <= d {
		return effectiveStepRaw
	}
	snapped := hcMinStep
	for _, nice := range drilldownNiceSteps {
		if hcMinStep <= nice {
			snapped = nice
			break
		}
	}
	return strconv.FormatInt(int64(snapped/time.Second), 10) + "s"
}

// relaxStepForLowCardinality returns a finer effective step when the by-field
// has low cardinality and can therefore be aggregated cheaply at native step.
// originalStep is the step the client requested (before coarsening); cardinality
// is the number of distinct values returned by the field_values fast tier;
// rangeNs is the requested time range in nanoseconds.
//
// Returns (newStep, true) only when:
//   - cardinality is small (≤ drilldownLowCardThreshold)
//   - cardinality is known to be the full distinct set (< maxDrilldownSeries, i.e. not truncated by the limit param)
//   - the original step parses
//
// The returned step floor is rangeNs/drilldownLowCardStatsBuckets so VL still
// stays bounded for very wide ranges.
func relaxStepForLowCardinality(originalStepRaw string, cardinality int, rangeNs int64) (string, bool) {
	if cardinality <= 0 || cardinality > drilldownLowCardThreshold {
		return "", false
	}
	if cardinality >= maxDrilldownSeries {
		// field_values was truncated by limit=maxDrilldownSeries; we don't know
		// the true cardinality so assume high-card and keep the coarsened step.
		return "", false
	}
	origStep, ok := parsePositiveStepDuration(originalStepRaw)
	if !ok || origStep <= 0 || rangeNs <= 0 {
		return "", false
	}
	minStep := time.Duration(rangeNs) / drilldownLowCardStatsBuckets
	finerStep := origStep
	if finerStep < minStep {
		finerStep = minStep
	}
	return strconv.FormatInt(int64(finerStep/time.Second), 10) + "s", true
}

// coarsenDrilldownStep returns an effective step that is at least step but
// ensures the range [startRaw, endRaw] contains at most maxDrilldownStatsBuckets
// buckets (or maxDrilldownStatsBucketsShort for ranges ≤6h). The result is
// snapped up to the next "nice" step so VL timestamps are round numbers.
// If the requested step already satisfies the budget, it is returned unchanged.
func coarsenDrilldownStep(startRaw, endRaw string, step time.Duration) time.Duration {
	if step <= 0 {
		return step
	}
	startNs, ok1 := parseLokiTimeToUnixNano(startRaw)
	endNs, ok2 := parseLokiTimeToUnixNano(endRaw)
	if !ok1 || !ok2 || endNs <= startNs {
		return step
	}
	rangeNs := endNs - startNs
	cap := int64(maxDrilldownStatsBuckets)
	if rangeNs <= int64(drilldownHybridThreshold) {
		cap = int64(maxDrilldownStatsBucketsShort)
	}
	minStep := time.Duration(rangeNs) / time.Duration(cap)
	if minStep <= step {
		return step
	}
	for _, nice := range drilldownNiceSteps {
		if minStep <= nice {
			return nice
		}
	}
	return minStep
}

// drilldownStatsCacheKey returns a cache key for a Drilldown single-field
// count response. Both start and end are bucketed to 5-minute (or effective-step)
// granularity to absorb Grafana's sliding "last N h" window: for a "last 6h"
// panel, both start and end drift by ~30 s on every Grafana refresh, producing
// a unique key every tick and defeating the cache. Bucketing both timestamps to
// the same granularity keeps the key stable within a 5-minute window.
//
// The step is coarsened via coarsenDrilldownStep before being written to the
// key. Grafana derives the step from the panel's pixel width, so minor window
// resizes produce slightly different step values (e.g. 59s vs 60s). Including
// the raw step would bust the cache on every resize; the coarsened step is
// stable for the entire range/bucket combination, eliminating spurious misses.
func (p *Proxy) drilldownStatsCacheKey(r *http.Request) string {
	startRaw, endRaw, stepRaw := r.FormValue("start"), r.FormValue("end"), r.FormValue("step")
	effectiveStep := time.Duration(0)
	if d, ok := parsePositiveStepDuration(stepRaw); ok {
		effectiveStep = coarsenDrilldownStep(startRaw, endRaw, d)
	}
	bucket := 5 * time.Minute
	if effectiveStep > bucket {
		bucket = effectiveStep
	}
	startBucketed := bucketTimestampString(startRaw, bucket)
	endBucketed := bucketTimestampString(endRaw, bucket)
	var b strings.Builder
	b.WriteString("drilldown_stats:")
	b.WriteString(r.Header.Get("X-Scope-OrgID"))
	b.WriteByte(':')
	b.WriteString(r.FormValue("query"))
	b.WriteByte(':')
	b.WriteString(startBucketed)
	b.WriteByte(':')
	b.WriteString(endBucketed)
	b.WriteByte(':')
	if effectiveStep > 0 {
		b.WriteString(strconv.FormatInt(int64(effectiveStep.Seconds()), 10))
		b.WriteByte('s')
	} else {
		b.WriteString(stepRaw)
	}
	if fp := p.fingerprintFromCtx(r.Context(), r); fp != "" {
		b.WriteString(":auth:")
		b.WriteString(fp)
	}
	return b.String()
}

// maxDrilldownExpansionFactor caps how many fine sub-buckets one coarse bucket
// may be expanded into. When the Grafana-requested step is very fine (e.g. 20 s)
// but the VL coarse step is large (e.g. 6 h for a 7-day range), dividing the
// per-bucket count by 1080 makes every bar effectively invisible. Above this
// threshold we return the coarse data as-is so bar heights remain readable.
const maxDrilldownExpansionFactor = 120

// expandDrilldownStep expands a Loki matrix JSON response produced at coarseStepRaw
// into fineStepRaw resolution by replicating each coarse bucket value across
// (coarseStep/fineStep) fine sub-buckets, each carrying the full coarseValue.
//
// This converts e.g. 25 sparse bars (3600s step, 24h) into 288 dense bars (300s
// step), matching Loki's visual resolution without additional VL queries. The full
// coarse value is replicated (not divided) into each sub-bucket so that sparse
// high-cardinality fields (trace_id, span_id) remain visible. Relative bar heights
// across series within the same field are preserved since all series use the same
// factor. Sub-buckets whose timestamps exceed endRaw are omitted (edge bucket handling).
//
// Returns body unchanged when fineStep >= coarseStep, factor > maxDrilldownExpansionFactor
// (to preserve readable bar heights for long ranges), or either step cannot be parsed.
func expandDrilldownStep(body []byte, fineStepRaw, coarseStepRaw, endRaw string) []byte {
	fineStep, fineOK := parsePositiveStepDuration(fineStepRaw)
	coarseStep, coarseOK := parsePositiveStepDuration(coarseStepRaw)
	if !fineOK || !coarseOK || fineStep <= 0 || coarseStep <= fineStep {
		return body
	}
	factor := int64(coarseStep / fineStep)
	if factor <= 1 || factor > maxDrilldownExpansionFactor {
		return body
	}
	fineStepSec := int64(fineStep / time.Second)

	var endSec int64
	if endNs, ok := parseLokiTimeToUnixNano(endRaw); ok {
		endSec = endNs / int64(time.Second)
	}

	v, parseErr := fj.ParseBytes(body)
	if parseErr != nil {
		return body
	}
	resultArr := v.GetArray("data", "result")
	if len(resultArr) == 0 {
		return body
	}

	var buf bytes.Buffer
	buf.Grow(len(body) * int(factor))
	buf.WriteString(`{"status":"success","data":{"resultType":"matrix","result":[`)

	scratch := make([]byte, 0, 512)
	for si, series := range resultArr {
		if si > 0 {
			buf.WriteByte(',')
		}
		buf.WriteString(`{"metric":`)
		if metric := series.Get("metric"); metric != nil {
			scratch = metric.MarshalTo(scratch[:0])
			buf.Write(scratch)
		} else {
			buf.WriteString(`{}`)
		}
		buf.WriteString(`,"values":[`)

		firstPoint := true
		for _, pair := range series.GetArray("values") {
			arr := pair.GetArray()
			if len(arr) < 2 {
				continue
			}
			tsCoarse := arr[0].GetInt64()
			coarseVal, parseFloatErr := strconv.ParseFloat(string(arr[1].GetStringBytes()), 64)
			if parseFloatErr != nil {
				continue
			}
			// Replicate the full coarse count into every fine sub-bucket rather than
			// dividing by factor. Dividing makes sparse fields (trace_id, span_id with
			// 1-3 occurrences per coarse window) produce sub-1 values that are nearly
			// invisible in Grafana's bar chart. Relative proportions between series are
			// preserved since all series in the same field chart use the same factor.
			//
			// Format with precision -1 (shortest representation) to match Loki's
			// integer-string output ("78" not "78.00") for whole counts, while still
			// rendering floats correctly when fractional values come up from rate-style
			// aggregations.
			subValStr := strconv.FormatFloat(coarseVal, 'f', -1, 64)

			for i := int64(0); i < factor; i++ {
				tsFine := tsCoarse + i*fineStepSec
				if endSec > 0 && tsFine > endSec {
					break
				}
				if !firstPoint {
					buf.WriteByte(',')
				}
				firstPoint = false
				buf.WriteByte('[')
				scratch = strconv.AppendInt(scratch[:0], tsFine, 10)
				buf.Write(scratch)
				buf.WriteString(`,"`)
				buf.WriteString(subValStr)
				buf.WriteString(`"]`)
			}
		}

		buf.WriteString(`]}`)
	}

	buf.WriteString(`]}}`)
	return buf.Bytes()
}

// proxyStatsQueryRangeDrilldown handles Drilldown single-field count queries.
//
// Response cache path: checks a 60-second Drilldown-specific cache before
// acquiring the concurrency semaphore. Re-renders (filter interactions, Grafana
// ticks, second tab) are served from cache with zero VL calls.
//
// Fast path (low-cardinality fields, short ranges): appends VL-side sort+limit
// so only the top-maxDrilldownSeries unique values are transmitted per time
// bucket, then proxy-side caps to maxDrilldownSeries series.
//
// Eager two-phase path: detected via drilldownShouldEagerTwoPhase (HC field
// name, range > 60 buckets, or cached overflow). Bypasses the direct VL attempt
// and goes straight to drilldownTwoPhase — one global sort (Phase 1) + filtered
// scan (Phase 2) instead of per-bucket sorts.
//
// Fallback two-phase path: when the direct response still overflows
// maxDrilldownResponseBytes (unknown HC field, first request for a new selector),
// records the decision in drilldownCardCache and falls through to two-phase.
//
// cleanBase is the stream-selector base without the field existence filter.
// field is the grouped field name. Both come from detectDrilldownSingleField.
func (p *Proxy) proxyStatsQueryRangeDrilldown(w http.ResponseWriter, r *http.Request, logsqlQuery, cleanBase, field string) {
	// Check the 5-minute Drilldown response cache before any VL work.
	drillCacheKey := p.drilldownStatsCacheKey(r)
	if cached, _, ok := p.cache.GetWithTTL(drillCacheKey); ok {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(cached)
		return
	}

	// Apply transforms up-front (before both batcher and semaphore paths) so they
	// run exactly once regardless of which path handles the request.
	origGroupBy := parseOriginalByLabels(r.FormValue("query"))
	logsqlQuery = p.addUnderscorefallbackByLabels(logsqlQuery, origGroupBy)
	// Keep pre-limit query for two-phase fallback (Phase 1 needs it without the
	// sort+limit suffix so it can set step = entire range for a single bucket).
	effectiveForFallback := logsqlQuery

	// Coarsen the step to at most range/maxDrilldownStatsBuckets.
	startRaw, endRaw, stepRaw := r.FormValue("start"), r.FormValue("end"), r.FormValue("step")
	effectiveStepRaw := stepRaw
	if d, ok := parsePositiveStepDuration(stepRaw); ok {
		if coarsened := coarsenDrilldownStep(startRaw, endRaw, d); coarsened != d {
			effectiveStepRaw = strconv.FormatInt(int64(coarsened.Seconds()), 10) + "s"
		}
	}

	// Hybrid path for wide ranges (> drilldownHybridThreshold = 6h) — checked BEFORE
	// the batcher because the batcher issues stats_query_range over the full range,
	// defeating the hybrid's purpose of capping the scan to a recent window.
	// - Fast tier: field_values over the full range (O(column-index), ~30ms) for value discovery.
	// - Slow tier: stats_query_range over a recent adaptive window only (not the full range).
	// - Merge: histogram series from stats + single-point stubs from field_values.
	// - High-cardinality fields: field_values synthesis only, no stats_query_range.
	// This replaces the "return emptyLokiMatrix" for _id/_uid fields over 6h and eliminates
	// the O(log-volume) full-range scan that made 2d+ fields slow.
	{
		startNs, sok := parseLokiTimeToUnixNano(startRaw)
		endNs, eok := parseLokiTimeToUnixNano(endRaw)
		if sok && eok && endNs-startNs > int64(drilldownHybridThreshold) {
			p.proxyStatsQueryRangeDrilldownHybrid(w, r, logsqlQuery, cleanBase, field, effectiveStepRaw, startNs, endNs, drillCacheKey, origGroupBy)
			return
		}
	}

	// Field batcher (short ranges ≤ drilldownHybridThreshold only): folds N concurrent
	// single-field queries into one multi-field VL call. Wide ranges are handled by
	// the hybrid path above which caps the scan window, so batcher only runs here for
	// ranges where a full stats_query_range is cheap (≤6h).
	if batcher := p.drilldownFieldBatcher; batcher != nil && !p.drilldownShouldEagerTwoPhase(r, cleanBase, field) {
		vlFields := extractStatsGroupByFields(logsqlQuery)
		if len(vlFields) == 0 {
			vlFields = []string{field}
		}
		lokiField := field
		if len(origGroupBy) == 1 {
			lokiField = origGroupBy[0]
		} else if lt := p.labelTranslator; lt != nil && !lt.IsPassthrough() {
			lokiField = lt.ToLoki(field)
		}
		orgID := r.Header.Get("X-Scope-OrgID")
		if body := batcher.submit(r.Context(), orgID, cleanBase, lokiField, field, vlFields, startRaw, endRaw, effectiveStepRaw); body != nil {
			body = expandDrilldownStep(body, stepRaw, effectiveStepRaw, endRaw)
			p.setLocalReadCacheWithTTL(drillCacheKey, append([]byte(nil), body...), drilldownStatsCacheTTL)
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write(body)
			return
		}
	}

	// Individual path: semaphore limits concurrent VL stats_query_range calls.
	// Covers both direct and two-phase for non-batched fields.
	if sem := p.statsQueryRangeSem; sem != nil {
		select {
		case <-sem:
			delay := p.statsQueryRangeInterQueryDelay
			defer func() {
				if delay > 0 {
					time.Sleep(delay)
				}
				sem <- struct{}{}
			}()
		case <-r.Context().Done():
			p.writeError(w, http.StatusServiceUnavailable, "request cancelled waiting for stats_query_range slot")
			return
		}
	}

	if !p.drilldownShouldEagerTwoPhase(r, cleanBase, field) {
		// Direct path: VL-side | limit 500 per bucket bounds the per-bucket result;
		// proxy-side limitLokiMatrixSeries caps the global matrix to maxDrilldownSeries.
		limitedQuery := appendDrilldownSeriesLimit(logsqlQuery, maxDrilldownSeries)
		params := buildStatsQueryRangeParams(limitedQuery, startRaw, endRaw, effectiveStepRaw)
		resp, err := p.vlPost(r.Context(), "/select/logsql/stats_query_range", params)
		if err != nil {
			p.writeError(w, statusFromUpstreamErr(err), err.Error())
			return
		}
		defer resp.Body.Close()

		if resp.StatusCode >= 400 {
			errBody, _ := readBodyLimited(resp.Body, maxUpstreamErrorBodyBytes)
			p.writeError(w, resp.StatusCode, extractVLErrorMsg(errBody))
			return
		}

		body, bodyErr := readBodyLimited(resp.Body, maxDrilldownResponseBytes)
		if bodyErr == nil {
			// Direct path succeeded — serve the trimmed, translated matrix.
			var keepFn func(int64) bool
			if endNs, ok := parseLokiTimeToUnixNano(r.FormValue("end")); ok {
				keepFn = func(tsNs int64) bool { return tsNs <= endNs }
			}
			body = p.trimAndTranslateStatsQRFJ(r.Context(), body, keepFn, r.FormValue("query"))
			body = limitLokiMatrixSeries(body, maxDrilldownSeries)
			final := wrapAsLokiResponse(body, "matrix")
			final = expandDrilldownStep(final, stepRaw, effectiveStepRaw, endRaw)
			p.setLocalReadCacheWithTTL(drillCacheKey, append([]byte(nil), final...), drilldownStatsCacheTTL)
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write(final)
			return
		}

		// Response overflowed maxDrilldownResponseBytes — remember for next re-render
		// so subsequent requests for the same {org, base, field} skip the direct path.
		orgID := r.Header.Get("X-Scope-OrgID")
		if p.drilldownCardCache != nil {
			p.drilldownCardCache.markHigh(orgID, cleanBase, field)
		}
	}

	// Two-phase path (either eager or after direct-path overflow).
	body := p.drilldownTwoPhase(r, effectiveForFallback, cleanBase, field, effectiveStepRaw)
	if body == nil {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(emptyLokiMatrix)
		return
	}
	body = expandDrilldownStep(body, stepRaw, effectiveStepRaw, endRaw)
	p.setLocalReadCacheWithTTL(drillCacheKey, append([]byte(nil), body...), drilldownStatsCacheTTL)
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(body)
}

// proxyStatsQueryRangeDrilldownParserDirect handles Drilldown single-field count
// queries that require parser stages (| unpack_json, | unpack_logfmt) to access
// fields embedded in the log body rather than VL's column index (e.g. trace_id,
// span_id, level from logfmt). Unlike proxyStatsQueryRangeDrilldown — which strips
// parser stages and uses column-index fast paths (batcher, two-phase, hybrid) —
// this path issues one direct stats_query_range call with the parser preserved and
// a VL-side | limit 500 to bound the per-bucket cardinality.
//
// Applies the same step coarsening, 5-minute response caching, trim-and-translate,
// and proxy-side series cap as the direct subpath within proxyStatsQueryRangeDrilldown.
//
// cleanBase and field come from detectDrilldownSingleFieldWithParser and are used
// to drive the /hits fast path. cleanBase still has parser stages (e.g. "{...} | json
// | logfmt"); /hits with ignore_pipes=1 strips them and queries VL's column index
// directly — VL automatically indexes parsed JSON fields, so this works for
// trace_id, span_id, request_id, etc., without needing the parser to run server-side.
func (p *Proxy) proxyStatsQueryRangeDrilldownParserDirect(
	w http.ResponseWriter, r *http.Request,
	logsqlQuery, cleanBase, field string,
) {
	drillCacheKey := p.drilldownStatsCacheKey(r)
	if cached, _, ok := p.cache.GetWithTTL(drillCacheKey); ok {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(cached)
		return
	}

	origGroupBy := parseOriginalByLabels(r.FormValue("query"))
	logsqlQuery = p.addUnderscorefallbackByLabels(logsqlQuery, origGroupBy)

	startRaw, endRaw, stepRaw := r.FormValue("start"), r.FormValue("end"), r.FormValue("step")
	effectiveStepRaw := stepRaw
	if d, ok := parsePositiveStepDuration(stepRaw); ok {
		if coarsened := coarsenDrilldownStep(startRaw, endRaw, d); coarsened != d {
			effectiveStepRaw = strconv.FormatInt(int64(coarsened.Seconds()), 10) + "s"
		}
	}

	// /select/logsql/hits fast path — same logic as the hybrid path. /hits gives
	// us top-N values + remainder bucket in one call, with native per-bucket
	// counts. For high-cardinality FIELD queries (trace_id, span_id, request_id,
	// session_id, etc.) this dramatically improves chart shape because the
	// remainder series spans the full window. Without /hits, the legacy
	// stats+merge pipeline below produces a sparse chart dominated by unique
	// values that each appear in only one or two buckets.
	//
	// Pass CLIENT step (stepRaw) — NOT effectiveStepRaw which has been coarsened.
	// /hits scales internally; coarsening would produce a sparse axis with visible
	// empty spaces between buckets.
	if cleanBase != "" && field != "" {
		lokiField := field
		if len(origGroupBy) == 1 {
			lokiField = origGroupBy[0]
		} else if lt := p.labelTranslator; lt != nil && !lt.IsPassthrough() {
			lokiField = lt.ToLoki(field)
		}
		// /hits accepts only stream selector + field — strip parser/drop/bare
		// filter pipes that VL's query parser rejects before ignore_pipes=1
		// can run. The parsed JSON field is already indexed as a VL column,
		// so we don't need `| json` to query it.
		hitsQuery := extractStreamSelectorOnly(cleanBase)
		if hitsQuery != "" {
			if p.proxyStatsQueryRangeDrilldownHits(w, r, hitsQuery, field, lokiField, startRaw, endRaw, stepRaw, drillCacheKey) {
				return
			}
			w.Header().Set("X-Proxy-Drilldown-Hits-Fallback", "1")
		}
	}

	if sem := p.statsQueryRangeSem; sem != nil {
		select {
		case <-sem:
			delay := p.statsQueryRangeInterQueryDelay
			defer func() {
				if delay > 0 {
					time.Sleep(delay)
				}
				sem <- struct{}{}
			}()
		case <-r.Context().Done():
			p.writeError(w, http.StatusServiceUnavailable, "request cancelled waiting for stats_query_range slot")
			return
		}
	}

	limitedQuery := appendDrilldownSeriesLimit(logsqlQuery, maxDrilldownSeries)
	params := buildStatsQueryRangeParams(limitedQuery, startRaw, endRaw, effectiveStepRaw)
	resp, err := p.vlPost(r.Context(), "/select/logsql/stats_query_range", params)
	if err != nil {
		p.writeError(w, statusFromUpstreamErr(err), err.Error())
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		errBody, _ := readBodyLimited(resp.Body, maxUpstreamErrorBodyBytes)
		p.writeError(w, resp.StatusCode, extractVLErrorMsg(errBody))
		return
	}

	body, bodyErr := readBodyLimited(resp.Body, maxDrilldownResponseBytes)
	if bodyErr != nil {
		// VL-side limit should prevent this, but if it overflows serve empty.
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(emptyLokiMatrix)
		return
	}

	var keepFn func(int64) bool
	if endNs, ok := parseLokiTimeToUnixNano(r.FormValue("end")); ok {
		keepFn = func(tsNs int64) bool { return tsNs <= endNs }
	}
	body = p.trimAndTranslateStatsQRFJ(r.Context(), body, keepFn, r.FormValue("query"))
	body = limitLokiMatrixSeries(body, maxDrilldownSeries)
	// Zero-fill missing time buckets so Grafana renders a continuous histogram
	// rather than disconnected spikes — VL stats_query_range omits zero-count
	// buckets while Loki count_over_time always emits every step. The hybrid path
	// already does this; without it here, parser-stage fields (most Drilldown
	// queries — they include "| json field=..." stages) get gappy charts.
	if stepDur, ok := parsePositiveStepDuration(effectiveStepRaw); ok && stepDur > 0 {
		startNs, sok := parseLokiTimeToUnixNano(startRaw)
		endNs, eok := parseLokiTimeToUnixNano(endRaw)
		if sok && eok {
			body = zerofillStatsMatrix(body,
				startNs/int64(time.Second),
				endNs/int64(time.Second),
				int64(stepDur/time.Second))
		}
	}
	final := wrapAsLokiResponse(body, "matrix")
	final = expandDrilldownStep(final, stepRaw, effectiveStepRaw, endRaw)
	p.setLocalReadCacheWithTTL(drillCacheKey, append([]byte(nil), final...), drilldownStatsCacheTTL)
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(final)
}

// drilldownHitsFieldsLimit caps the number of top-N values returned per
// /select/logsql/hits call. 20 matches Grafana Drilldown's default top-N
// rendering in `VizPanelSeriesLimit` — sending more series than the plugin
// will render wastes bandwidth and slows the browser. With dozens of field
// panels on the page, even 100 series per field becomes thousands of series
// total and Grafana noticeably lags.
const drilldownHitsFieldsLimit = 20

// hitsWindowSampleThreshold: time range above which the proxy splits the
// /hits call into multiple windowed queries. VL's /hits picks top-N by
// frequency across the whole range; for high-cardinality fields where every
// value appears once, the top-N ends up clustered at whichever times VL
// happened to scan (typically recent), producing a chart with all activity
// "grouped at the latest part". Splitting the range and querying per-window
// guarantees values are sampled from each time window — chart shows
// distributed activity across the full range.
const hitsWindowSampleThreshold = 6 * time.Hour

// hitsWindowCount is the number of sub-windows the range is split into when
// the windowed sampling kicks in. 4 windows × 5 values per window = 20 series
// total, evenly distributed across the timeline. Higher counts add latency
// (each window is a separate VL call, even when parallelised) without
// proportionally improving the visual.
const hitsWindowCount = 4


// proxyStatsQueryRangeDrilldownHits handles a Drilldown single-field histogram
// using VL's /select/logsql/hits endpoint. Unlike the stats+merge pipeline,
// /hits natively returns top-N series with REAL per-bucket counts PLUS a
// remainder bucket aggregating all other values. The remainder series spans
// the full requested range and is what makes Grafana's default top-N rendering
// useful for high-cardinality fields (where every value would otherwise appear
// in only one or two buckets, making the chart look concentrated).
//
// Response shape from VL:
//
//	{"hits": [
//	  {"fields": {"pod": "..."}, "timestamps": ["..."], "values": [...], "total": N},
//	  ...,
//	  {"fields": {}, "timestamps": [...], "values": [...], "total": LARGE}  // remainder
//	]}
//
// Translation to Loki matrix: each hit becomes one series; ISO timestamps
// parsed to Unix seconds. The remainder bucket (VL emits it with empty fields{})
// is dropped — it aggregates everything outside top-N and dominates Grafana's
// Y-axis for high-cardinality fields, hiding individual top-N values. Loki
// Drilldown doesn't emit a catchall; matching that behaviour keeps the chart
// readable.
// queryHits runs a single VL /select/logsql/hits call and returns the parsed
// response. Returns ok=false (with no error) when the call fails, returns
// HTTP ≥400, or the body overflows the response cap — callers fall back to
// the legacy stats+merge pipeline in those cases.
//
// stepRaw must already be a valid VL duration (e.g. "120s"); bare integer
// formats from Grafana ("120") need normalisation by the caller.
func (p *Proxy) queryHits(
	ctx context.Context,
	cleanBase, field string,
	startRaw, endRaw, stepRaw string,
	fieldsLimit int,
) (vlHitsResponse, bool) {
	params := url.Values{}
	params.Set("query", cleanBase)
	params.Set("field", field)
	params.Set("fields_limit", strconv.Itoa(fieldsLimit))
	params.Set("start", startRaw)
	params.Set("end", endRaw)
	params.Set("step", stepRaw)
	// ignore_pipes=1 strips any pipe stages from the source query so /hits
	// runs against the raw selector. The proxy has already extracted cleanBase
	// from the full LogQL; we don't need VL to re-apply any pipes.
	params.Set("ignore_pipes", "1")

	resp, err := p.vlPost(ctx, "/select/logsql/hits", params)
	if err != nil {
		return vlHitsResponse{}, false
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		return vlHitsResponse{}, false
	}
	body, err := readBodyLimited(resp.Body, maxDrilldownResponseBytes)
	if err != nil {
		return vlHitsResponse{}, false
	}
	return parseHits(body), true
}

// gatherWindowedHits splits [startSec,endSec) into hitsWindowCount equal sub-
// windows, runs queryHits in parallel for each window with fields_limit=
// drilldownHitsFieldsLimit/hitsWindowCount, and returns the merged hits.
//
// Why: VL /hits picks top-N by frequency across the whole range. For
// high-cardinality fields where every value appears once, "top" is broken by
// scan order — typically the most recent values. The full-range chart then
// shows all selected values clustered at the latest time. Per-window
// sampling guarantees each window contributes its own top-K, so the merged
// set spans the timeline visually.
//
// Field values are deduplicated by value string: if the same value appears
// in two windows, its timestamps+values arrays are concatenated.
func (p *Proxy) gatherWindowedHits(
	ctx context.Context,
	cleanBase, field string,
	startSec, endSec int64,
	stepRaw string,
) (vlHitsResponse, bool) {
	perWindow := drilldownHitsFieldsLimit / hitsWindowCount
	if perWindow < 1 {
		perWindow = 1
	}
	windowDur := (endSec - startSec) / hitsWindowCount

	type windowResult struct {
		hits vlHitsResponse
		ok   bool
	}
	results := make([]windowResult, hitsWindowCount)
	var wg sync.WaitGroup
	for i := 0; i < hitsWindowCount; i++ {
		i := i
		wStart := startSec + int64(i)*windowDur
		wEnd := wStart + windowDur
		if i == hitsWindowCount-1 {
			wEnd = endSec // absorb rounding remainder into last window
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			h, ok := p.queryHits(ctx, cleanBase, field,
				strconv.FormatInt(wStart, 10),
				strconv.FormatInt(wEnd, 10),
				stepRaw, perWindow)
			results[i] = windowResult{hits: h, ok: ok}
		}()
	}
	wg.Wait()

	merged := vlHitsResponse{}
	// Use index map so first-seen order is preserved (windows iterate oldest→newest,
	// so older values appear first in the legend — looks chronological).
	idx := make(map[string]int)
	anyOK := false
	for _, res := range results {
		if !res.ok {
			continue
		}
		anyOK = true
		for _, h := range res.hits.Hits {
			fv := h.Fields[field]
			if fv == "" {
				// Drop remainder buckets — see proxyStatsQueryRangeDrilldownHits.
				continue
			}
			if pos, exists := idx[fv]; exists {
				existing := merged.Hits[pos]
				existing.Timestamps = append(existing.Timestamps, h.Timestamps...)
				existing.Values = append(existing.Values, h.Values...)
				existing.Total += h.Total
				merged.Hits[pos] = existing
			} else {
				idx[fv] = len(merged.Hits)
				merged.Hits = append(merged.Hits, h)
			}
		}
	}
	return merged, anyOK
}

func (p *Proxy) proxyStatsQueryRangeDrilldownHits(
	w http.ResponseWriter, r *http.Request,
	cleanBase, field, lokiField string,
	startRaw, endRaw, stepRaw string,
	drillCacheKey string,
) bool {
	// Check 5-min response cache before any VL work.
	if cached, _, ok := p.cache.GetWithTTL(drillCacheKey); ok {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(cached)
		return true
	}

	// VL /hits requires step with a duration suffix (e.g. "120s"). Grafana
	// sends bare integers ("120") to /loki/api/v1/query_range; append "s"
	// when the string parses cleanly as a number.
	stepForHits := stepRaw
	if _, err := strconv.ParseFloat(stepRaw, 64); err == nil {
		stepForHits = stepRaw + "s"
	}

	// For long ranges, sample windows in parallel so the top-N spans the
	// timeline instead of clustering at recent times (VL's natural bias).
	startNs, sOK := parseLokiTimeToUnixNano(startRaw)
	endNs, eOK := parseLokiTimeToUnixNano(endRaw)
	useWindowed := sOK && eOK && time.Duration(endNs-startNs) >= hitsWindowSampleThreshold

	var hits vlHitsResponse
	var ok bool
	if useWindowed {
		hits, ok = p.gatherWindowedHits(r.Context(), cleanBase, field,
			startNs/int64(time.Second), endNs/int64(time.Second), stepForHits)
		if ok {
			w.Header().Set("X-Proxy-Drilldown-Hits-Sampling", "windowed")
		}
	}
	if !useWindowed || !ok {
		hits, ok = p.queryHits(r.Context(), cleanBase, field,
			startRaw, endRaw, stepForHits, drilldownHitsFieldsLimit)
	}
	if !ok {
		return false
	}
	if len(hits.Hits) == 0 {
		return false
	}
	// Fall back when VL returned only the remainder bucket (fields:{}). This
	// happens for ultra-high-cardinality numeric fields (latency_ms, duration_ms)
	// where every value is unique — VL has no meaningful top-N to compute, so it
	// puts everything in remainder. Since we drop the remainder, accepting this
	// response would emit zero series. The legacy stats+merge path produces a
	// usable per-bucket histogram for these fields.
	hasNamed := false
	for _, h := range hits.Hits {
		if h.Total > 0 && h.Fields[field] != "" {
			hasNamed = true
			break
		}
	}
	if !hasNamed {
		return false
	}

	// Per-value rendering: emit one Loki series per top-N value. Drop the
	// remainder bucket (VL's fields:{} catchall) — including it dominates the
	// Y-axis and hides individual values for high-cardinality fields. This
	// matches Loki Drilldown's behaviour: for ephemeral-value fields (trace_id
	// with random IDs, pods with random suffixes) the chart shows scattered
	// dots, which is a faithful representation of the underlying data.
	// Pre-size buffer: ~50 bytes per (timestamp, value) pair across all hits.
	estPairs := 0
	for _, h := range hits.Hits {
		estPairs += len(h.Timestamps)
	}
	var buf bytes.Buffer
	buf.Grow(64 + estPairs*50)
	buf.WriteString(`{"status":"success","data":{"resultType":"matrix","result":[`)
	first := true
	for _, h := range hits.Hits {
		if h.Total == 0 {
			continue
		}
		labelValue := h.Fields[field]
		if labelValue == "" {
			continue
		}
		if !first {
			buf.WriteByte(',')
		}
		first = false
		buf.WriteString(`{"metric":{"`)
		writeJSONEscaped(&buf, lokiField)
		buf.WriteString(`":"`)
		writeJSONEscaped(&buf, labelValue)
		buf.WriteString(`"},"values":[`)
		n := len(h.Timestamps)
		if len(h.Values) < n {
			n = len(h.Values)
		}
		firstPt := true
		for i := 0; i < n; i++ {
			t, terr := time.Parse(time.RFC3339, string(h.Timestamps[i]))
			if terr != nil {
				continue
			}
			if !firstPt {
				buf.WriteByte(',')
			}
			firstPt = false
			buf.WriteByte('[')
			buf.WriteString(strconv.FormatInt(t.Unix(), 10))
			buf.WriteString(`,"`)
			buf.WriteString(strconv.Itoa(h.Values[i]))
			buf.WriteString(`"]`)
		}
		buf.WriteString(`]}`)
	}
	buf.WriteString(`]}}`)

	final := buf.Bytes()
	p.setLocalReadCacheWithTTL(drillCacheKey, append([]byte(nil), final...), drilldownStatsCacheTTL)
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Proxy-Drilldown-Path", "hits")
	_, _ = w.Write(final)
	return true
}

// proxyStatsQueryRangeDrilldownHybrid handles Drilldown fields queries for ranges
// wider than drilldownHybridThreshold (6h). Instead of scanning the full range
// with stats_query_range (O(log-volume), slow), it:
//
//  1. Issues field_values over the full range (~30ms, O(column-index)) to discover
//     all unique values and their total hit counts.
//  2. Computes an adaptive histogram window based on range size and data volume.
//  3. Issues stats_query_range over only the recent adaptive window for sparkline data.
//  4. Merges: values in stats get real histogram bars; values only in field_values get
//     a single data point at endSec with their total count.
//
// High-cardinality fields (trace_id, *_id, *_uid, etc.) skip stats_query_range entirely
// and return a pure field_values synthesis.
//
// X-Proxy-Drilldown-Path response header signals which path was taken for debugging.
func (p *Proxy) proxyStatsQueryRangeDrilldownHybrid(
	w http.ResponseWriter, r *http.Request,
	logsqlQuery, cleanBase, field, effectiveStepRaw string,
	startNs, endNs int64,
	drillCacheKey string, origGroupBy []string,
) {
	ctx := r.Context()
	endRaw := r.FormValue("end")
	startRaw := r.FormValue("start")

	// Loki field name for response metric keys (what Grafana expects).
	lokiField := field
	if len(origGroupBy) == 1 {
		lokiField = origGroupBy[0]
	} else if lt := p.labelTranslator; lt != nil && !lt.IsPassthrough() {
		lokiField = lt.ToLoki(field)
	}

	// /select/logsql/hits fast path — returns top-N + remainder bucket natively.
	// Try this first; it produces a far better chart shape than the stats+merge
	// pipeline because the remainder series naturally spans the full time range,
	// guaranteeing Grafana's default top-N rendering shows something meaningful
	// across the whole window (instead of a few concentrated tall spikes).
	// Falls through to the legacy stats+merge path on any failure (404 on older
	// VL, parse error, empty response, etc.).
	//
	// Pass the CLIENT step (r.FormValue("step")) — NOT effectiveStepRaw which
	// has been coarsened to maxDrilldownStatsBuckets. /hits handles native step
	// efficiently because it computes top-N internally per bucket; passing the
	// coarsened step would produce a sparse axis (e.g. 96 timestamps at 15min
	// instead of 720 at 120s for 24h), which Grafana renders as a chart with
	// visible empty spaces between coarse buckets.
	clientStep := r.FormValue("step")
	if clientStep == "" {
		clientStep = effectiveStepRaw
	}
	// /hits accepts only the stream selector — strip any trailing pipes
	// defensively. For the hybrid path cleanBase is usually already pipe-free
	// (detectDrilldownSingleField rejects parser stages), but extracting the
	// selector explicitly keeps the call path uniform with parser-direct.
	hitsQuery := extractStreamSelectorOnly(cleanBase)
	if hitsQuery == "" {
		hitsQuery = cleanBase
	}
	if p.proxyStatsQueryRangeDrilldownHits(w, r, hitsQuery, field, lokiField, startRaw, endRaw, clientStep, drillCacheKey) {
		return
	}
	// Mark response so callers / load tests know the fallback fired.
	w.Header().Set("X-Proxy-Drilldown-Hits-Fallback", "1")

	endSec := endNs / int64(time.Second)

	// Fast tier: field_values over the full range (O(column-index)).
	fvParams := url.Values{}
	fvParams.Set("query", cleanBase)
	fvParams.Set("field", field)
	fvParams.Set("start", nanosToVLTimestamp(startNs))
	fvParams.Set("end", nanosToVLTimestamp(endNs))
	fvParams.Set("limit", strconv.Itoa(maxDrilldownSeries))

	var fvEntries []drilldownFVEntry
	if resp, err := p.vlGet(ctx, "/select/logsql/field_values", fvParams); err == nil {
		defer resp.Body.Close()
		if resp.StatusCode < 400 {
			fvBody, _ := readBodyLimited(resp.Body, 1<<20)
			var parsed vlFieldValuesResponse
			if json.Unmarshal(fvBody, &parsed) == nil {
				for _, item := range parsed.Values {
					if strings.TrimSpace(item.Value) == "" || item.Hits < 0 {
						continue
					}
					fvEntries = append(fvEntries, drilldownFVEntry{Value: item.Value, Hits: item.Hits})
				}
			}
		}
	}

	rangeNs := endNs - startNs

	// High-cardinality fields (trace_id, span_id, *_id, *_uid, *_token, …): route
	// through stats_query_range so per-bucket counts are real, not synthesized.
	// field_values is unreliable here — when the limit truncates, hits are zeroed
	// and a synthesized matrix shows all-zero values that Grafana renders blank.
	// Tighten the step cap to drilldownHighCardStatsBuckets so VL's stats pipe
	// doesn't OOM on 500k-cardinality grouping over many buckets.
	isHighCard := isLikelyHighCardinalityField(field) || isHighCardinalityFieldName(field)
	if isHighCard {
		effectiveStepRaw = highCardStepFloor(effectiveStepRaw, rangeNs)
		// Fall through to the regular stats tier below. The synthesize-from-
		// field_values fallback inside that tier still acts as the last-resort
		// safety net if VL returns no usable rows.
	}

	// Low-cardinality groupings (≤ drilldownLowCardThreshold distinct values):
	// the 30-bucket cap from coarsenDrilldownStep is pure quality loss for stream
	// labels like app/env/cluster/level — VL handles native step trivially when
	// the result set is small. Restore the original requested step (capped to
	// drilldownLowCardStatsBuckets buckets as a safety floor) so the proxy
	// matches Loki's per-bucket resolution at 2d/7d.
	if relaxed, ok := relaxStepForLowCardinality(r.FormValue("step"), len(fvEntries), rangeNs); ok {
		effectiveStepRaw = relaxed
	}

	// Stats tier: stats_query_range over the full requested range at coarsened step.
	// VL column-index stats are fast even for wide ranges (< 1s for 7d), so scanning
	// only a recent partial window and filling the rest with flat stub bars is
	// unnecessary — it makes different time ranges look identical (same flat bars).
	// effectiveStepRaw is already coarsened to ≤ maxDrilldownStatsBuckets buckets
	// (or relaxed to native step for low-cardinality groupings, see above).
	histStartRaw := nanosToVLTimestamp(startNs)

	// stubStepNs drives stub placement in mergeDrilldownWithFieldValues for values
	// that appear in field_values but not in the top-N stats result (values beyond
	// the maxDrilldownSeries limit). Use effectiveStepRaw resolution so stubs
	// align with the stats bars.
	var stubStepNs int64
	if d, ok := parsePositiveStepDuration(effectiveStepRaw); ok {
		stubStepNs = int64(d)
	}

	// Build a clean stats query grouping by only the target field. logsqlQuery has
	// underscore-fallback variants (e.g. detected_level) added by addUnderscorefallbackByLabels,
	// which would produce duplicate series (level + detected_level) confusing Grafana.
	hybridStatsQuery := cleanBase + " | filter " + field + `:!"" | stats by (` + field + `) count()`
	limitedQuery := appendDrilldownSeriesLimit(hybridStatsQuery, maxDrilldownSeries)
	statsParams := buildStatsQueryRangeParams(limitedQuery, histStartRaw, endRaw, effectiveStepRaw)

	var statsBody []byte
	pathLabel := "hybrid/fv+full-range-stats"

	// Acquire semaphore slot before issuing the stats_query_range.
	if sem := p.statsQueryRangeSem; sem != nil {
		select {
		case <-sem:
			delay := p.statsQueryRangeInterQueryDelay
			defer func() {
				if delay > 0 {
					time.Sleep(delay)
				}
				sem <- struct{}{}
			}()
		case <-ctx.Done():
			// Context cancelled — serve field_values synthesis immediately.
			body := synthesizeDrilldownMatrixSpread(lokiField, fvEntries, startNs/int64(time.Second), endSec, int64(time.Duration(rangeNs)/drilldownSynthesizeBuckets/time.Second))
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-Proxy-Drilldown-Path", "field_values_sem_timeout")
			_, _ = w.Write(body)
			return
		}
	}

	resp, err := p.vlPost(ctx, "/select/logsql/stats_query_range", statsParams)
	if err == nil {
		defer resp.Body.Close()
		if resp.StatusCode < 400 {
			rawBody, bodyErr := readBodyLimited(resp.Body, maxDrilldownResponseBytes)
			if bodyErr == nil {
				// Skip trimAndTranslateStatsQRFJ: the hybrid stats query groups by
				// `field` only (no underscore variants), so VL returns metric keys that
				// match `field` (the VL name). Strip VL's internal __name__ marker, then
				// rename metric keys from `field` to `lokiField` when they differ (e.g.
				// VL "level" → Loki "detected_level") so mergeDrilldownWithFieldValues
				// can look up the correct key. The generic translator is intentionally
				// skipped: its ensureDetectedLevel would break when the query explicitly
				// groups by "level".
				rawBody = stripVLStatsNameKey(rawBody)
				if field != lokiField {
					rawBody = renameStatsBodyMetricKey(rawBody, field, lokiField)
				}
				rawBody = limitLokiMatrixSeries(rawBody, maxDrilldownSeries)
				// Zero-fill missing time steps so Grafana draws a continuous line rather
				// than disconnected spikes. VL stats_query_range omits zero-count buckets;
				// Loki always emits every step in the query window.
				if stepDur, ok := parsePositiveStepDuration(effectiveStepRaw); ok && stepDur > 0 {
					stepSec := int64(stepDur / time.Second)
					rawBody = zerofillStatsMatrix(rawBody,
						startNs/int64(time.Second),
						endNs/int64(time.Second),
						stepSec)
				}
				statsBody = rawBody
			}
		}
	}

	// Merge: stats histogram + field_values stubs for values missing from recent window.
	//
	// Critical ordering: when stats returned real per-bucket data, those series MUST
	// survive the maxDrilldownSeries cap. The stub-fill emits drilldownSynthesizeBuckets
	// (or coarse-step) cells per series, all of value=1 (floored from hits/N when hits
	// is small). For unique-per-request fields like pod, real stats series have 1
	// nonzero bucket with count ~20-30 (sum~30), while stub-only series have 96+
	// cells of value=1 (sum~96+). A naive post-merge limitLokiMatrixSeries ranks by
	// sum and DROPS the real stats series in favour of the synthetic stubs, producing
	// the "every bar is uniform value=1" symptom users see in Drilldown.
	//
	// Skip the post-merge limit entirely. statsBody is already capped to
	// maxDrilldownSeries by the earlier limitLokiMatrixSeries call, and the merge
	// only adds fvEntries that are NOT already in stats. We cap the fvEntries used
	// for stub-fill so the combined output stays within maxDrilldownSeries × ~2 at
	// worst — Grafana handles that fine and the real data is preserved.
	var final []byte
	switch {
	case statsBody != nil && len(fvEntries) > 0:
		final = mergeDrilldownWithFieldValues(statsBody, lokiField, fvEntries, endSec, rangeNs, stubStepNs)
		// No post-merge limitLokiMatrixSeries — see comment above. The merge function
		// internally bounds output by stats_count + fv_count, both already capped at
		// maxDrilldownSeries.
	case statsBody != nil:
		final = statsBody
		pathLabel += "/no_fv"
	case len(fvEntries) > 0:
		final = synthesizeDrilldownMatrixSpread(lokiField, fvEntries, startNs/int64(time.Second), endSec, int64(time.Duration(rangeNs)/drilldownSynthesizeBuckets/time.Second))
		pathLabel = "field_values_stats_err"
	default:
		final = emptyLokiMatrix
		pathLabel = "empty"
	}

	// Expansion is skipped for the hybrid path: stubs (at stubStepNs, coarsened
	// for the full range) and stats (at histStepRaw, coarsened for histWin) may
	// differ in resolution. Applying a single coarseStep via expandDrilldownStep
	// would corrupt the mixed-resolution output. The non-hybrid path still expands.
	p.setLocalReadCacheWithTTL(drillCacheKey, append([]byte(nil), final...), drilldownStatsCacheTTL)
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Proxy-Drilldown-Path", pathLabel)
	_, _ = w.Write(final)
}

// drilldownTwoPhase is the fallback for proxyStatsQueryRangeDrilldown when the
// direct VL response exceeds maxDrilldownResponseBytes. It runs two VL calls:
//
//  1. Phase 1 — a single-bucket stats_query_range (step = entire range) to get
//     the global top-maxDrilldownSeries field values. One bucket means VL's
//     per-bucket | limit N is equivalent to a true global limit.
//
//  2. Phase 2 — a range query filtered to the Phase 1 values via field:in(...),
//     returning ≤maxDrilldownSeries unique series with the effective step.
//
// effectiveStep is the coarsened step (from coarsenDrilldownStep); Phase 2 uses
// it instead of the raw Grafana step to cap bucket count and reduce VL CPU.
//
// Returns the translated Loki matrix response, or nil on any VL error.
func (p *Proxy) drilldownTwoPhase(r *http.Request, effectiveQuery, cleanBase, field, effectiveStep string) []byte {
	ctx := r.Context()
	start, end := r.FormValue("start"), r.FormValue("end")
	step := effectiveStep
	if step == "" {
		step = r.FormValue("step")
	}

	startNs, ok1 := parseLokiTimeToUnixNano(start)
	endNs, ok2 := parseLokiTimeToUnixNano(end)
	if !ok1 || !ok2 || endNs <= startNs {
		return nil
	}
	rangeSec := (endNs - startNs) / int64(time.Second)
	if rangeSec < 1 {
		rangeSec = 1
	}

	p1Params := url.Values{}
	p1Params.Set("query", appendDrilldownSeriesLimit(effectiveQuery, maxDrilldownSeries))
	p1Params.Set("start", nanosToVLTimestamp(startNs))
	p1Params.Set("end", nanosToVLTimestamp(endNs))
	p1Params.Set("step", strconv.FormatInt(rangeSec, 10)+"s")

	resp1, err := p.vlPost(ctx, "/select/logsql/stats_query_range", p1Params)
	if err != nil {
		return nil
	}
	defer resp1.Body.Close()
	if resp1.StatusCode >= 400 {
		return nil
	}

	// 1 MB is generous for ≤maxDrilldownSeries × ~256 bytes per UUID entry.
	p1Body, p1Err := readBodyLimited(resp1.Body, 1<<20)
	if p1Err != nil {
		return nil
	}

	topValues := drilldownTopValuesFromMatrix(p1Body, field)
	if len(topValues) == 0 {
		return emptyLokiMatrix
	}
	if len(topValues) > maxDrilldownPhase2Values {
		topValues = topValues[:maxDrilldownPhase2Values]
	}

	// Phase 2: range query restricted to the global top-N values.
	inFilter := buildVLInFilter(field, topValues)
	p2Query := cleanBase + " | filter " + inFilter + " | stats by (" + quoteLogsQLIdent(field) + ") count()"
	origGroupBy := parseOriginalByLabels(r.FormValue("query"))
	p2Query = p.addUnderscorefallbackByLabels(p2Query, origGroupBy)

	p2Params := buildStatsQueryRangeParams(p2Query, start, end, step)
	resp2, err := p.vlPost(ctx, "/select/logsql/stats_query_range", p2Params)
	if err != nil {
		return nil
	}
	defer resp2.Body.Close()
	if resp2.StatusCode >= 400 {
		return nil
	}

	p2Body, p2Err := readBodyLimited(resp2.Body, maxDrilldownResponseBytes)
	if p2Err != nil {
		return nil
	}

	var keepFn func(int64) bool
	if ok2 {
		keepFn = func(tsNs int64) bool { return tsNs <= endNs }
	}
	p2Body = p.trimAndTranslateStatsQRFJ(ctx, p2Body, keepFn, r.FormValue("query"))
	p2Body = limitLokiMatrixSeries(p2Body, maxDrilldownSeries)
	return wrapAsLokiResponse(p2Body, "matrix")
}

// drilldownTopValuesFromMatrix extracts the label values for field from a Loki
// matrix JSON response. Used by drilldownTwoPhase to parse the Phase 1 result
// (one bucket → global top-N values from a single-bucket stats_query_range call).
func drilldownTopValuesFromMatrix(body []byte, field string) []string {
	v, err := fj.ParseBytes(body)
	if err != nil {
		return nil
	}
	result := v.GetArray("data", "result")
	if len(result) == 0 {
		return nil
	}
	out := make([]string, 0, len(result))
	for _, entry := range result {
		val := string(entry.GetStringBytes("metric", field))
		if val != "" {
			out = append(out, val)
		}
	}
	return out
}

// buildVLInFilter builds a LogsQL field:in("v1","v2",...) existence filter.
// VL evaluates in() filters against pre-indexed columns without a parser stage,
// making them fast even for high-cardinality fields. Used by drilldownTwoPhase
// to restrict Phase 2 queries to the values returned by Phase 1.
func buildVLInFilter(field string, values []string) string {
	var sb strings.Builder
	sb.WriteString(quoteLogsQLIdent(field))
	sb.WriteString(`:in(`)
	for i, v := range values {
		if i > 0 {
			sb.WriteByte(',')
		}
		sb.WriteByte('"')
		sb.WriteString(strings.ReplaceAll(v, `"`, `\"`))
		sb.WriteByte('"')
	}
	sb.WriteByte(')')
	return sb.String()
}

// limitLokiMatrixSeries truncates the result array in a Loki matrix response to
// the top maxSeries entries by total count (sum of all sample values), matching
// Loki's max_series_per_query behaviour. Grafana Drilldown then picks the top 100
// from those series for display — sorting here ensures the most active field values
// survive the cut when VL returns tens of thousands of unique series.
// Returns body unchanged if parsing fails or len(result) <= maxSeries.
func limitLokiMatrixSeries(body []byte, maxSeries int) []byte {
	if maxSeries <= 0 {
		return body
	}
	v, err := fj.ParseBytes(body)
	if err != nil {
		return body
	}
	result := v.GetArray("data", "result")
	if len(result) <= maxSeries {
		return body
	}

	// Compute total count per series and sort descending so the most active
	// field values survive the maxSeries cut, not an arbitrary VL ordering.
	type ranked struct {
		idx   int
		total float64
	}
	ranks := make([]ranked, len(result))
	for i, entry := range result {
		var total float64
		for _, pair := range entry.GetArray("values") {
			arr := pair.GetArray()
			if len(arr) >= 2 {
				if f, e := strconv.ParseFloat(string(arr[1].GetStringBytes()), 64); e == nil {
					total += f
				}
			}
		}
		ranks[i] = ranked{idx: i, total: total}
	}
	sort.Slice(ranks, func(i, j int) bool { return ranks[i].total > ranks[j].total })

	buf := make([]byte, 0, maxSeries*256)
	buf = append(buf, `{"status":"success","data":{"resultType":"matrix","result":[`...)
	for i := 0; i < maxSeries; i++ {
		if i > 0 {
			buf = append(buf, ',')
		}
		buf = result[ranks[i].idx].MarshalTo(buf)
	}
	buf = append(buf, `]}}`...)
	return buf
}

// addUnderscorefallbackByLabels augments a translated LogsQL stats query's by()
// clause with underscore fallbacks for any label that was translated to a dotted
// OTel form (e.g. service_name → service.name). VL returns whichever field exists
// in the log data; translateStatsResponseLabelsWithContext then coalesces the two
// fields into a single Loki label by preferring the non-empty value. This ensures
// correct grouping for Loki-push data (stream labels use underscore names) and OTel
// data (fields use dotted names) without requiring two separate backend calls.
func (p *Proxy) addUnderscorefallbackByLabels(logsqlQuery string, origGroupBy []string) string {
	if p.labelTranslator == nil || p.labelTranslator.IsPassthrough() ||
		p.labelTranslator.style != LabelStyleUnderscores || len(origGroupBy) == 0 {
		return logsqlQuery
	}
	var extras []string
	for _, orig := range origGroupBy {
		vlLabel := p.labelTranslator.ToVL(orig)
		if vlLabel != orig && strings.Contains(vlLabel, ".") {
			extras = append(extras, orig)
		}
	}
	if len(extras) == 0 {
		return logsqlQuery
	}
	byIdx := strings.Index(logsqlQuery, "| stats by (")
	if byIdx < 0 {
		return logsqlQuery
	}
	closeIdx := strings.Index(logsqlQuery[byIdx:], ")")
	if closeIdx < 0 {
		return logsqlQuery
	}
	insertAt := byIdx + closeIdx
	return logsqlQuery[:insertAt] + ", " + strings.Join(extras, ", ") + logsqlQuery[insertAt:]
}

// allRangeWindowsEqual returns (window, true) when every range vector in logql
// uses the same window duration. A query like rate({a}[1m]) / rate({b}[5m])
// returns (0, false) because the windows differ. Used to guard binary-expression
// shift logic against applying a single shift to operands with different windows.
func allRangeWindowsEqual(logql string) (time.Duration, bool) {
	var common time.Duration
	inBracket := false
	start := 0
	for i, ch := range logql {
		switch ch {
		case '[':
			inBracket = true
			start = i + 1
		case ']':
			if inBracket {
				inBracket = false
				d := parseLokiDuration(strings.TrimSpace(logql[start:i]))
				if d <= 0 {
					continue
				}
				if common == 0 {
					common = d
				} else if d != common {
					return 0, false
				}
			}
		}
	}
	return common, common > 0
}

// statsRateRangeEqualsStepShift detects whether the query contains a rate() or
// bytes_rate() with range==step so that the caller can apply the first-bucket
// start shift. The check scans the full expression (not just the top-level
// function) so outer aggregations like sum by(x)(rate(...)) are detected.
// NOTE: binary metric expressions (e.g. rate({a}[1m]) / rate({b}[1m])) are
// routed through proxyBinaryMetric before reaching this function; apply the
// shift there independently (see proxyBinaryMetric / proxyBinaryMetricVM).
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

// statsQRFJPool pools fastjson.Parser instances for trimStatsQueryRange* hot paths.
var statsQRFJPool fj.ParserPool

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

// fjMarshalPool pools scratch []byte slices for fastjson MarshalTo calls.
// Reusing a pre-allocated slice avoids per-call allocation when marshaling
// individual JSON values back to bytes (metrics, points, etc.).
var fjMarshalPool = &sync.Pool{New: func() interface{} { b := make([]byte, 0, 4096); return &b }}

// marshalFJ marshals v into scratch (resizing as needed) and writes to buf.
// scratch must come from fjMarshalPool.
func marshalFJ(buf *bytes.Buffer, v *fj.Value, scratch *[]byte) {
	*scratch = v.MarshalTo((*scratch)[:0])
	buf.Write(*scratch)
}

// trimStatsQueryRangeResponseFromStart removes points with timestamp < startNs.
// Used when start was shifted back to include the pre-start bucket for rate().
func trimStatsQueryRangeResponseFromStart(body []byte, startNs int64) []byte {
	return trimStatsQRByTimeFJ(body, func(tsNs int64) bool { return tsNs >= startNs })
}

// trimStatsQRByTimeFJ filters stats_query_range point arrays using fastjson,
// eliminating json.Unmarshal struct allocations and json.Marshal reflection.
func trimStatsQRByTimeFJ(body []byte, keep func(int64) bool) []byte {
	p := statsQRFJPool.Get()
	defer statsQRFJPool.Put(p)

	v, err := p.ParseBytes(body)
	if err != nil {
		return body
	}

	// Locate result series: top-level "results" or nested "data"."result".
	var seriesArr []*fj.Value
	var dataVal *fj.Value

	if r := v.Get("results"); r != nil && r.Type() == fj.TypeArray {
		seriesArr, _ = r.Array()
	}
	if len(seriesArr) == 0 {
		if d := v.Get("data"); d != nil {
			if r := d.Get("result"); r != nil && r.Type() == fj.TypeArray {
				seriesArr, _ = r.Array()
				dataVal = d
			}
		}
	}
	if len(seriesArr) == 0 {
		return body
	}

	// Quick scan: any point falls outside the keep range?
	needsTrim := false
scanLoop:
	for _, series := range seriesArr {
		valObj := series.Get("values")
		if valObj == nil {
			continue
		}
		points, _ := valObj.Array()
		for _, point := range points {
			pts, _ := point.Array()
			if len(pts) == 0 {
				continue
			}
			if !keep(statsQRFJPointNano(pts[0])) {
				needsTrim = true
				break scanLoop
			}
		}
	}
	if !needsTrim {
		return body
	}

	// Rebuild the JSON response with filtered values arrays.
	buf := jsonBufPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer jsonBufPool.Put(buf)
	buf.Grow(len(body))

	scratch := fjMarshalPool.Get().(*[]byte)
	defer fjMarshalPool.Put(scratch)

	buf.WriteByte('{')
	needsComma := false

	if status := v.Get("status"); status != nil {
		buf.WriteString(`"status":`)
		marshalFJ(buf, status, scratch)
		needsComma = true
	}

	if dataVal != nil {
		if needsComma {
			buf.WriteByte(',')
		}
		buf.WriteString(`"data":{`)
		if rt := dataVal.Get("resultType"); rt != nil {
			buf.WriteString(`"resultType":`)
			marshalFJ(buf, rt, scratch)
			buf.WriteByte(',')
		}
		buf.WriteString(`"result":`)
		writeFilteredStatsQRSeriesFJ(buf, seriesArr, keep, scratch)
		if stats := dataVal.Get("stats"); stats != nil {
			buf.WriteString(`,"stats":`)
			marshalFJ(buf, stats, scratch)
		}
		buf.WriteByte('}')
	} else {
		if needsComma {
			buf.WriteByte(',')
		}
		buf.WriteString(`"results":`)
		writeFilteredStatsQRSeriesFJ(buf, seriesArr, keep, scratch)
	}

	buf.WriteByte('}')

	result := make([]byte, buf.Len())
	copy(result, buf.Bytes())
	return result
}

func writeFilteredStatsQRSeriesFJ(buf *bytes.Buffer, seriesArr []*fj.Value, keep func(int64) bool, scratch *[]byte) {
	buf.WriteByte('[')
	for si, series := range seriesArr {
		if si > 0 {
			buf.WriteByte(',')
		}
		buf.WriteByte('{')
		fieldWritten := false
		if metric := series.Get("metric"); metric != nil {
			buf.WriteString(`"metric":`)
			marshalFJ(buf, metric, scratch)
			fieldWritten = true
		}
		if values := series.Get("values"); values != nil {
			if fieldWritten {
				buf.WriteByte(',')
			}
			buf.WriteString(`"values":[`)
			points, _ := values.Array()
			firstPoint := true
			for _, point := range points {
				pts, _ := point.Array()
				if len(pts) == 0 {
					continue
				}
				if !keep(statsQRFJPointNano(pts[0])) {
					continue
				}
				if !firstPoint {
					buf.WriteByte(',')
				}
				firstPoint = false
				marshalFJ(buf, point, scratch)
			}
			buf.WriteByte(']')
		}
		buf.WriteByte('}')
	}
	buf.WriteByte(']')
}

// trimTranslateResult holds per-item output of trimAndTranslateStatsQRFJ's analysis pass.
type trimTranslateResult struct {
	metric   map[string]string // nil = metric unchanged
	valsTrim bool              // at least one values point filtered by keep
}

// trimAndTranslateStatsQRFJ performs time-window filtering and metric-label
// translation in a single fastjson parse, replacing the two-parse sequence of
// trimStatsQRByTimeFJ followed by translateStatsResponseLabelsWithContext.
//
// keep may be nil (no time filtering). If neither filtering nor label translation
// is needed, the original body is returned unchanged with no allocation.
//
//nolint:gocyclo // combines two existing functions; branching is inherent to the schema variants.
func (p *Proxy) trimAndTranslateStatsQRFJ(ctx context.Context, body []byte, keep func(int64) bool, originalQuery string) []byte {
	start := time.Now()

	parser := statsTranslateFJPool.Get()
	defer statsTranslateFJPool.Put(parser)

	v, err := parser.ParseBytes(body)
	if err != nil {
		return body
	}

	// Locate result series across all three JSON shapes the VL stats endpoints emit:
	//   {"data":{"resultType":"…","result":[…]}}  ← Prometheus-compatible (stats_query_range)
	//   {"result":[…]}                             ← bare result
	//   {"results":[…]}                            ← bare results
	type resultSlot struct {
		items  []*fj.Value
		key    string
		inData bool
	}
	var slots []resultSlot
	var dataVal *fj.Value

	if data := v.Get("data"); data != nil {
		if r := data.Get("result"); r != nil && r.Type() == fj.TypeArray {
			if arr, _ := r.Array(); len(arr) > 0 {
				slots = append(slots, resultSlot{items: arr, key: "result", inData: true})
				dataVal = data
			}
		}
	}
	if r := v.Get("result"); r != nil && r.Type() == fj.TypeArray {
		if arr, _ := r.Array(); len(arr) > 0 {
			slots = append(slots, resultSlot{items: arr, key: "result", inData: false})
		}
	}
	if r := v.Get("results"); r != nil && r.Type() == fj.TypeArray {
		if arr, _ := r.Array(); len(arr) > 0 {
			slots = append(slots, resultSlot{items: arr, key: "results", inData: false})
		}
	}

	if len(slots) == 0 {
		return body
	}

	// Allocate per-item result state.
	slotResults := make([][]trimTranslateResult, len(slots))
	for i, s := range slots {
		slotResults[i] = make([]trimTranslateResult, len(s.items))
	}

	// Workspace maps reused across items (same pattern as translateStatsResponseLabelsWithContext).
	translated := make(map[string]string, 8)
	syntheticLabels := make(map[string]string, 8)

	needsRebuild := false
	translatedCount := 0

	for si, slot := range slots {
		for ii, item := range slot.items {
			res := &slotResults[si][ii]

			// Pass 1: check whether any values points fall outside keep.
			if keep != nil {
				if values := item.Get("values"); values != nil {
					pts, _ := values.Array()
					for _, pt := range pts {
						ptArr, _ := pt.Array()
						if len(ptArr) > 0 && !keep(statsQRFJPointNano(ptArr[0])) {
							res.valsTrim = true
							needsRebuild = true
							break
						}
					}
				}
			}

			// Pass 2: compute translated metric labels (identical logic to translateStatsResponseLabelsWithContext).
			metricVal := item.Get("metric")
			if metricVal == nil || metricVal.Type() != fj.TypeObject {
				continue
			}

			for k := range translated {
				delete(translated, k)
			}
			changed := false
			hadStream := false

			metricVal.GetObject().Visit(func(k []byte, vv *fj.Value) {
				key := string(k)
				val := string(vv.GetStringBytes())
				switch key {
				case "__name__":
					changed = true
				case "_stream":
					hadStream = true
					for streamKey, streamValue := range parseStreamLabels(val) {
						lokiKey := streamKey
						if !p.labelTranslator.IsPassthrough() {
							lokiKey = p.labelTranslator.ToLoki(streamKey)
						}
						if streamValue != "" || translated[lokiKey] == "" {
							translated[lokiKey] = streamValue
						}
					}
					changed = true
				default:
					lokiKey := key
					if !p.labelTranslator.IsPassthrough() {
						lokiKey = p.labelTranslator.ToLoki(key)
					}
					if lokiKey != key {
						changed = true
					}
					if val != "" || translated[lokiKey] == "" {
						translated[lokiKey] = val
					}
				}
			})

			for k := range syntheticLabels {
				delete(syntheticLabels, k)
			}
			for k, val := range translated {
				syntheticLabels[k] = val
			}

			serviceSignal := hasServiceSignal(syntheticLabels)
			beforeSyntheticCount := len(syntheticLabels)
			hadLevel := syntheticLabels["level"] != ""
			ensureDetectedLevel(syntheticLabels)
			if hadLevel && !hadStream && syntheticLabels["detected_level"] != "" {
				delete(syntheticLabels, "level")
				delete(translated, "level")
			}
			if hadStream {
				ensureSyntheticServiceName(syntheticLabels)
				if !serviceSignal && strings.TrimSpace(syntheticLabels["service_name"]) == unknownServiceName {
					delete(syntheticLabels, "service_name")
				}
			}
			if len(syntheticLabels) != beforeSyntheticCount {
				changed = true
			}
			for key, value := range syntheticLabels {
				if existing, ok := translated[key]; ok && existing == value {
					continue
				}
				translated[key] = value
				changed = true
			}

			if changed {
				translatedCount++
				needsRebuild = true
				res.metric = cloneStringMap(syntheticLabels)
			}
		}
	}

	if !needsRebuild {
		p.observeInternalOperation(ctx, "trim_translate_stats_qr", "noop", time.Since(start))
		return body
	}

	// Rebuild the JSON response once, applying both filtering and translation.
	// Always emit "status":"success" first so wrapAsLokiResponse fast-path A matches
	// and returns the buffer zero-alloc instead of splicing a new []byte.
	buf := jsonBufPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer jsonBufPool.Put(buf)
	buf.Grow(len(body) + len(`{"status":"success",`))

	scratch := fjMarshalPool.Get().(*[]byte)
	defer fjMarshalPool.Put(scratch)
	// Fix 2: pre-grow scratch to body length so values.MarshalTo doesn't
	// chain-reallocate when serialising a large values array in one shot.
	if cap(*scratch) < len(body) {
		*scratch = make([]byte, 0, len(body))
	}

	buf.WriteString(`{"status":"success"`)
	needsComma := true

	if dataVal != nil {
		si := -1
		for i, s := range slots {
			if s.inData {
				si = i
				break
			}
		}
		buf.WriteString(`,"data":{`)
		if rt := dataVal.Get("resultType"); rt != nil {
			buf.WriteString(`"resultType":`)
			marshalFJ(buf, rt, scratch)
			buf.WriteByte(',')
		}
		buf.WriteString(`"result":`)
		if si >= 0 {
			writeTrimmedTranslatedStatsFJ(buf, slots[si].items, slotResults[si], keep, scratch)
		} else {
			if r := dataVal.Get("result"); r != nil {
				marshalFJ(buf, r, scratch)
			} else {
				buf.WriteString(`[]`)
			}
		}
		if statsF := dataVal.Get("stats"); statsF != nil {
			buf.WriteString(`,"stats":`)
			marshalFJ(buf, statsF, scratch)
		}
		buf.WriteByte('}')
	}

	for si, slot := range slots {
		if slot.inData {
			continue
		}
		if needsComma {
			buf.WriteByte(',')
		}
		buf.WriteByte('"')
		buf.WriteString(slot.key)
		buf.WriteString(`":`)
		writeTrimmedTranslatedStatsFJ(buf, slot.items, slotResults[si], keep, scratch)
		needsComma = true
	}

	if errVal := v.Get("error"); errVal != nil {
		if needsComma {
			buf.WriteByte(',')
		}
		buf.WriteString(`"error":`)
		marshalFJ(buf, errVal, scratch)
	}

	buf.WriteByte('}')

	result := make([]byte, buf.Len())
	copy(result, buf.Bytes())
	p.observeInternalOperation(ctx, "trim_translate_stats_qr", "ok", time.Since(start))
	_ = translatedCount
	return result
}

// writeTrimmedTranslatedStatsFJ writes a JSON array of stats items, applying
// time-window filtering (keep != nil) and metric label translation (res.metric != nil)
// in a single write pass.
func writeTrimmedTranslatedStatsFJ(buf *bytes.Buffer, items []*fj.Value, results []trimTranslateResult, keep func(int64) bool, scratch *[]byte) {
	buf.WriteByte('[')
	for i, item := range items {
		if i > 0 {
			buf.WriteByte(',')
		}
		res := results[i]
		if res.metric != nil || res.valsTrim {
			buf.WriteString(`{"metric":`)
			if res.metric != nil {
				marshalStringMapJSONTo(buf, res.metric)
			} else if m := item.Get("metric"); m != nil {
				marshalFJ(buf, m, scratch)
			} else {
				buf.WriteString(`{}`)
			}
			// Instant value — no time filtering (single point, not an array).
			if val := item.Get("value"); val != nil {
				buf.WriteString(`,"value":`)
				marshalFJ(buf, val, scratch)
			}
			// Range values: when filtering is needed, serialise the whole array
			// once via MarshalTo and scan raw bytes — avoids N fastjson
			// re-serialisations for N points (Fix 3).
			if values := item.Get("values"); values != nil {
				buf.WriteString(`,"values":`)
				if res.valsTrim && keep != nil {
					rawVals := values.MarshalTo((*scratch)[:0])
					*scratch = rawVals
					if !writeFilteredValuesRaw(buf, rawVals, keep) {
						// Malformed values array — fall back to fastjson typed nodes.
						buf.WriteByte('[')
						pts, _ := values.Array()
						first := true
						for _, pt := range pts {
							ptArr, _ := pt.Array()
							if len(ptArr) > 0 && !keep(statsQRFJPointNano(ptArr[0])) {
								continue
							}
							if !first {
								buf.WriteByte(',')
							}
							first = false
							marshalFJ(buf, pt, scratch)
						}
						buf.WriteByte(']')
					}
				} else {
					// No time filtering needed: copy the whole array in one shot.
					marshalFJ(buf, values, scratch)
				}
			}
			buf.WriteByte('}')
		} else {
			marshalFJ(buf, item, scratch)
		}
	}
	buf.WriteByte(']')
}

// writeFilteredValuesRaw scans the raw JSON bytes of a stats_query_range
// values array — [[ts,"val"],[ts,"val"],...] — and writes to buf only those
// points where keep(tsNano) is true.
//
// The raw bytes are obtained from fastjson's MarshalTo in one call per series,
// avoiding N per-point MarshalTo+Write cycles for N values (Fix 3). Returns
// false if the bytes are not a recognisable values array; the caller falls back
// to fastjson typed-node iteration.
//
//nolint:gocyclo // byte-scanner state machine; branching is inherent to the [[ts,"val"],...] format.
func writeFilteredValuesRaw(buf *bytes.Buffer, raw []byte, keep func(int64) bool) bool {
	i := 0
	for i < len(raw) && raw[i] <= ' ' {
		i++
	}
	if i >= len(raw) || raw[i] != '[' {
		return false
	}
	i++

	buf.WriteByte('[')
	first := true

	for {
		// Skip whitespace and commas between elements.
		for i < len(raw) && (raw[i] <= ' ' || raw[i] == ',') {
			i++
		}
		if i >= len(raw) {
			return false
		}
		if raw[i] == ']' {
			break
		}
		if raw[i] != '[' {
			return false // unexpected token in outer array
		}

		pointStart := i
		i++ // consume inner '['

		for i < len(raw) && raw[i] <= ' ' {
			i++
		}

		// Parse the timestamp number (first element of the inner array).
		numStart := i
		if i < len(raw) && (raw[i] == '-' || raw[i] == '+') {
			i++
		}
		digitStart := i
		for i < len(raw) && raw[i] >= '0' && raw[i] <= '9' {
			i++
		}
		if i == digitStart {
			return false // no digits found
		}
		hasDot := false
		if i < len(raw) && raw[i] == '.' {
			hasDot = true
			i++
			for i < len(raw) && raw[i] >= '0' && raw[i] <= '9' {
				i++
			}
		}
		if i < len(raw) && (raw[i] == 'e' || raw[i] == 'E') {
			hasDot = true // treat scientific notation as float
			i++
			if i < len(raw) && (raw[i] == '+' || raw[i] == '-') {
				i++
			}
			for i < len(raw) && raw[i] >= '0' && raw[i] <= '9' {
				i++
			}
		}

		var tsNano int64
		if hasDot {
			f, err := strconv.ParseFloat(string(raw[numStart:i]), 64)
			if err != nil {
				return false
			}
			tsNano = normalizeLokiNumericTimeToUnixNano(f)
		} else {
			tsNano = normalizeLokiIntTimeToUnixNano(rawBytesToInt64(raw[numStart:i]))
		}

		// Advance past the rest of this point to its closing ']', handling
		// nested strings (escaped quotes) and nested arrays.
		depth := 1
		for i < len(raw) && depth > 0 {
			switch raw[i] {
			case '[':
				depth++
				i++
			case ']':
				depth--
				i++
			case '"':
				i++ // skip opening '"'
				for i < len(raw) && raw[i] != '"' {
					if raw[i] == '\\' {
						i++ // skip escaped character
					}
					i++
				}
				if i < len(raw) {
					i++ // skip closing '"'
				}
			default:
				i++
			}
		}

		if keep == nil || keep(tsNano) {
			if !first {
				buf.WriteByte(',')
			}
			first = false
			buf.Write(raw[pointStart:i])
		}
	}

	buf.WriteByte(']')
	return true
}

// rawBytesToInt64 parses a decimal integer from a byte slice without allocating
// a string. The caller must ensure b contains only ASCII digits with an optional
// leading '-'. Overflow is not checked — Loki/VL timestamps fit in int64.
func rawBytesToInt64(b []byte) int64 {
	if len(b) == 0 {
		return 0
	}
	neg := false
	i := 0
	if b[0] == '-' {
		neg = true
		i++
	}
	var n int64
	for ; i < len(b); i++ {
		n = n*10 + int64(b[i]-'0')
	}
	if neg {
		return -n
	}
	return n
}

// statsQRFJPointNano extracts the unix-nano timestamp from the first element
// of a stats_query_range point array [ts, "value"].
func statsQRFJPointNano(v *fj.Value) int64 {
	switch v.Type() {
	case fj.TypeNumber:
		return normalizeLokiNumericTimeToUnixNano(v.GetFloat64())
	case fj.TypeString:
		if ns, ok := parseLokiTimeToUnixNano(string(v.GetStringBytes())); ok {
			return ns
		}
	}
	return 0
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
	body = wrapAsLokiResponse(body, "vector")
	if topK, topKDesc, hasTopK := parseTopKWrapper(r.FormValue("query")); hasTopK {
		body = applyTopKToVector(body, topK, topKDesc)
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(body)
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

	// Apply first-bucket shift if the original LogQL contains rate/bytes_rate with range==step.
	// Guard: only shift when all range windows in the binary expression are equal — mixed
	// windows (e.g. rate({a}[1m]) / rate({b}[5m])) cannot share a single shift value.
	var origStartNs, shiftNs int64
	if isRange {
		if _, uniformOk := allRangeWindowsEqual(r.FormValue("query")); uniformOk {
			if origSpec, startNs, ok := statsRateRangeEqualsStepShift(r.FormValue("query"), r); ok {
				origStartNs = startNs
				shiftNs = origSpec.Window.Nanoseconds()
			}
		}
	}

	buildParams := func(query string) url.Values {
		params := url.Values{"query": {query}}
		if isRange {
			if s := r.FormValue("start"); s != "" {
				if shiftNs > 0 {
					if ns, ok2 := parseLokiTimeToUnixNano(s); ok2 {
						params.Set("start", nanosToVLTimestamp(ns-shiftNs))
					} else {
						params.Set("start", formatVLStatsTimestamp(s))
					}
				} else {
					params.Set("start", formatVLStatsTimestamp(s))
				}
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

	if origStartNs > 0 {
		result = trimStatsQueryRangeResponseFromStart(result, origStartNs)
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write(result)
}

func (p *Proxy) proxyBinaryMetric(w http.ResponseWriter, r *http.Request, op, leftQL, rightQL, vlEndpoint, resultType string) {
	isRange := vlEndpoint == "stats_query_range"

	// Apply first-bucket shift if the original LogQL contains rate/bytes_rate with range==step.
	// Guard: only shift when all range windows in the binary expression are equal.
	var origStartNs, shiftNs int64
	if isRange {
		if _, uniformOk := allRangeWindowsEqual(r.FormValue("query")); uniformOk {
			if origSpec, startNs, ok := statsRateRangeEqualsStepShift(r.FormValue("query"), r); ok {
				origStartNs = startNs
				shiftNs = origSpec.Window.Nanoseconds()
			}
		}
	}

	buildParams := func(query string) url.Values {
		params := url.Values{"query": {query}}
		if isRange {
			if s := r.FormValue("start"); s != "" {
				if shiftNs > 0 {
					if ns, ok2 := parseLokiTimeToUnixNano(s); ok2 {
						params.Set("start", nanosToVLTimestamp(ns-shiftNs))
					} else {
						params.Set("start", formatVLStatsTimestamp(s))
					}
				} else {
					params.Set("start", formatVLStatsTimestamp(s))
				}
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

	// Check if either side is a scalar or a nested binary marker.
	leftIsScalar := translator.IsScalar(leftQL)
	rightIsScalar := translator.IsScalar(rightQL)
	leftIsMarker := strings.HasPrefix(leftQL, translator.BinaryMetricPrefix)
	rightIsMarker := strings.HasPrefix(rightQL, translator.BinaryMetricPrefix)

	var leftBody, rightBody []byte
	var leftErr, rightErr error

	// When either side is a nested binary marker, resolve it recursively.
	// Otherwise fall through to the plain VL fetch paths.
	if leftIsMarker || rightIsMarker {
		leftBody, leftIsScalar, leftErr = p.resolveBinOpBody(r, leftQL, vlEndpoint, resultType, buildParams)
		if leftErr == nil {
			rightBody, rightIsScalar, rightErr = p.resolveBinOpBody(r, rightQL, vlEndpoint, resultType, buildParams)
		}
	} else if !leftIsScalar && !rightIsScalar {
		// Run both non-scalar VL fetches concurrently.
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

	if leftErr != nil {
		p.writeError(w, statusFromUpstreamErr(leftErr), "left query: "+leftErr.Error())
		return
	}
	if rightErr != nil {
		p.writeError(w, statusFromUpstreamErr(rightErr), "right query: "+rightErr.Error())
		return
	}

	// Combine results with arithmetic at proxy level
	result := combineBinaryMetricResults(leftBody, rightBody, op, resultType, leftIsScalar, rightIsScalar, leftQL, rightQL)
	if origStartNs > 0 {
		result = trimStatsQueryRangeResponseFromStart(result, origStartNs)
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write(result)
}

// resolveBinOpBody returns the result body for one side of a binary expression.
// Handles scalar strings, nested binary markers, and plain VL queries.
func (p *Proxy) resolveBinOpBody(r *http.Request, query, vlEndpoint, resultType string, buildParams func(string) url.Values) (body []byte, isScalar bool, err error) {
	if translator.IsScalar(query) {
		return []byte(`{"status":"success","data":{"resultType":"scalar","result":[0,"` + query + `"]}}`), true, nil
	}
	if strings.HasPrefix(query, translator.BinaryMetricPrefix) {
		body, err = p.evalBinaryMarker(r, query, vlEndpoint, resultType, buildParams)
		return body, false, err
	}
	resp, e := p.vlPost(r.Context(), "/select/logsql/"+vlEndpoint, buildParams(query))
	if e != nil {
		return nil, false, e
	}
	defer resp.Body.Close()
	body, _ = readBodyLimited(resp.Body, maxBufferedBackendBodyBytes)
	return body, false, nil
}

// evalBinaryMarker recursively evaluates a __binary__: expression marker.
func (p *Proxy) evalBinaryMarker(r *http.Request, marker, vlEndpoint, resultType string, buildParams func(string) url.Values) ([]byte, error) {
	op, left, right, vm, ok := translator.ParseBinaryMetricExprFull(marker)
	if !ok {
		return nil, fmt.Errorf("invalid binary expression marker")
	}

	leftBody, leftScalar, err := p.resolveBinOpBody(r, left, vlEndpoint, resultType, buildParams)
	if err != nil {
		return nil, err
	}
	rightBody, rightScalar, err := p.resolveBinOpBody(r, right, vlEndpoint, resultType, buildParams)
	if err != nil {
		return nil, err
	}

	if vm != nil && len(vm.On) > 0 {
		return applyOnMatching(leftBody, rightBody, op, vm.On, resultType), nil
	}
	if vm != nil && len(vm.Ignoring) > 0 {
		return applyIgnoringMatching(leftBody, rightBody, op, vm.Ignoring, resultType), nil
	}
	return combineBinaryMetricResults(leftBody, rightBody, op, resultType, leftScalar, rightScalar, left, right), nil
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

// applyTopKToMatrix filters a Loki matrix response to the top or bottom k series
// by maximum absolute value across all time steps. descending=true keeps the highest
// values (topk), descending=false keeps the lowest (bottomk). On parse error, returns
// the original body unchanged.
func applyTopKToMatrix(body []byte, k int, descending bool) []byte {
	v, err := fj.ParseBytes(body)
	if err != nil {
		return body
	}
	result := v.GetArray("data", "result")
	if len(result) <= k {
		return body
	}

	type ranked struct {
		idx      int
		maxValue float64
	}
	ranks := make([]ranked, len(result))
	for i, s := range result {
		var maxV float64
		for _, val := range s.GetArray("values") {
			arr := val.GetArray()
			if len(arr) < 2 {
				continue
			}
			vf := arr[1].GetStringBytes()
			f := parseFloat64Bytes(vf)
			if abs64(f) > abs64(maxV) {
				maxV = f
			}
		}
		ranks[i] = ranked{i, maxV}
	}

	sort.Slice(ranks, func(a, b int) bool {
		if descending {
			return ranks[a].maxValue > ranks[b].maxValue
		}
		return ranks[a].maxValue < ranks[b].maxValue
	})

	kept := make([]int, k)
	for i := range kept {
		kept[i] = ranks[i].idx
	}
	sort.Ints(kept)

	return rebuildMatrixOrVector(body, "matrix", result, kept)
}

// applyTopKToVector filters a Loki vector response to the top or bottom k samples.
func applyTopKToVector(body []byte, k int, descending bool) []byte {
	v, err := fj.ParseBytes(body)
	if err != nil {
		return body
	}
	result := v.GetArray("data", "result")
	if len(result) <= k {
		return body
	}

	type ranked struct {
		idx int
		val float64
	}
	ranks := make([]ranked, len(result))
	for i, s := range result {
		arr := s.GetArray("value")
		var f float64
		if len(arr) >= 2 {
			f = parseFloat64Bytes(arr[1].GetStringBytes())
		}
		ranks[i] = ranked{i, f}
	}
	sort.Slice(ranks, func(a, b int) bool {
		if descending {
			return ranks[a].val > ranks[b].val
		}
		return ranks[a].val < ranks[b].val
	})

	kept := make([]int, k)
	for i := range kept {
		kept[i] = ranks[i].idx
	}
	sort.Ints(kept)

	return rebuildMatrixOrVector(body, "vector", result, kept)
}

// rebuildMatrixOrVector rebuilds the Loki response JSON keeping only the series at
// indices `keep` from `result`.
func rebuildMatrixOrVector(body []byte, resultType string, result []*fj.Value, keep []int) []byte {
	var buf bytes.Buffer
	buf.WriteString(`{"status":"success","data":{"resultType":"`)
	buf.WriteString(resultType)
	buf.WriteString(`","result":[`)
	for i, idx := range keep {
		if i > 0 {
			buf.WriteByte(',')
		}
		buf.Write(result[idx].MarshalTo(nil))
	}
	buf.WriteString(`]}}`)
	return buf.Bytes()
}

// writeTopKFiltered applies topk/bottomk post-processing to a buffered response
// and writes the filtered result to dst.
func writeTopKFiltered(dst http.ResponseWriter, buf *bufferedResponseWriter, k int, descending bool, resultType string) {
	body := buf.body
	if len(body) > 0 {
		if resultType == "matrix" {
			body = applyTopKToMatrix(body, k, descending)
		} else {
			body = applyTopKToVector(body, k, descending)
		}
	}
	code := buf.code
	if code == 0 {
		code = http.StatusOK
	}
	dst.Header().Set("Content-Type", "application/json")
	if code != http.StatusOK {
		dst.WriteHeader(code)
	}
	_, _ = dst.Write(body)
}

func parseFloat64Bytes(b []byte) float64 {
	if len(b) == 0 {
		return 0
	}
	f, _ := strconv.ParseFloat(string(b), 64)
	return f
}

func abs64(f float64) float64 {
	if f < 0 {
		return -f
	}
	return f
}
