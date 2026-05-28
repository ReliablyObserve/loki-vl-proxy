package proxy

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/url"
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
		p.proxyStatsQueryRangeDirect(buf, shiftedR, logsqlQuery)
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

	// For Drilldown field-presence queries that opted into VL's count-all semantics via
	// | drop __error__: strip | unpack_json / | unpack_logfmt from the VL query before
	// forwarding. Drilldown only surfaces column-indexed fields via detected_fields, so
	// VL can evaluate existence filters (field:!"") and stats directly from the column
	// index — no JSON parsing needed. This avoids O(logs × fields) JSON parsing overhead
	// that makes 12h Drilldown field histogram queries take 2-8 seconds each.
	effectiveQuery := logsqlQuery
	if spec, ok := parseStatsCompatSpec(logsqlQuery); ok &&
		queryUsesParserStages(spec.BaseQuery) &&
		strings.Contains(spec.BaseQuery, "| delete __error__") {
		stripped := strings.TrimSpace(drilldownParserPipeRE.ReplaceAllString(spec.BaseQuery, ""))
		if stripped != spec.BaseQuery && allFiltersAreExistenceChecks(stripped) {
			effectiveQuery = stripped + logsqlQuery[len(spec.BaseQuery):]
		}
	}
	p.proxyStatsQueryRangeDirect(out, r, effectiveQuery)
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

func (p *Proxy) proxyStatsQueryRangeDirect(w http.ResponseWriter, r *http.Request, logsqlQuery string) {
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
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		errBody, _ := readBodyLimited(resp.Body, maxUpstreamErrorBodyBytes)
		p.writeError(w, resp.StatusCode, extractVLErrorMsg(errBody))
		return
	}

	body, bodyErr := readBodyLimited(resp.Body, maxStatsQueryRangeBytes)
	if bodyErr != nil {
		// Response exceeds the per-request cap — return empty matrix rather than OOMing.
		// This protects against high-cardinality by() clauses (e.g. by(float_field)) on
		// broad stream selectors producing tens of megabytes per concurrent field query.
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(emptyLokiMatrix)
		return
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
