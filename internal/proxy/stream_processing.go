package proxy

import (
	"bufio"
	stdjson "encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	fj "github.com/valyala/fastjson"
)

// proxyLogQuery fetches log lines from VictoriaLogs.
func (p *Proxy) proxyLogQuery(w http.ResponseWriter, r *http.Request, logsqlQuery string) {
	// Loki direction param: "forward" = oldest first, "backward" (default) = newest first
	direction := r.FormValue("direction")
	if direction == "forward" {
		logsqlQuery += " | sort by (_time)"
	} else {
		logsqlQuery += " | sort by (_time desc)"
	}

	params := url.Values{}
	params.Set("query", logsqlQuery)
	if s := r.FormValue("start"); s != "" {
		params.Set("start", formatVLTimestamp(s))
	}
	if e := r.FormValue("end"); e != "" {
		params.Set("end", formatVLTimestamp(e))
	}
	limit := r.FormValue("limit")
	if limit == "" {
		limit = strconv.Itoa(p.maxLines)
	}
	params.Set("limit", sanitizeLimit(limit))

	resp, err := p.vlPost(r.Context(), "/select/logsql/query", params)
	if err != nil {
		p.writeError(w, statusFromUpstreamErr(err), err.Error())
		return
	}
	defer resp.Body.Close()

	// Propagate VL error status to the client
	if resp.StatusCode >= 400 {
		body, _ := readBodyLimited(resp.Body, maxUpstreamErrorBodyBytes)
		errMsg := string(body)
		if errMsg == "" {
			errMsg = fmt.Sprintf("VL backend returned %d", resp.StatusCode)
		}
		p.writeError(w, resp.StatusCode, errMsg)
		return
	}

	// Chunked streaming: flush partial results as they arrive from VL
	categorizedLabels := requestWantsCategorizedLabels(r)
	emitStructuredMetadata := p.shouldEmitStructuredMetadata(r)
	p.metrics.RecordTupleMode(tupleModeForRequest(categorizedLabels, emitStructuredMetadata))
	if p.streamResponse {
		p.streamLogQuery(w, resp, r.FormValue("query"), categorizedLabels, emitStructuredMetadata)
		return
	}

	collectPatterns := p.patternsEnabled && p.patternsAutodetectFromQueries
	streams, patterns, err := p.vlReaderToLokiStreams(
		resp.Body,
		r.FormValue("query"),
		r.FormValue("step"),
		categorizedLabels,
		emitStructuredMetadata,
		collectPatterns,
	)
	if err != nil {
		p.writeError(w, http.StatusBadGateway, err.Error())
		return
	}
	p.storeAutodetectedPatterns(
		r.Header.Get("X-Scope-OrgID"),
		p.fingerprintFromCtx(r.Context(), r),
		r.FormValue("query"),
		r.FormValue("start"),
		r.FormValue("end"),
		r.FormValue("step"),
		patterns,
	)

	// Apply derived fields (extract trace_id etc. from log lines)
	if len(p.derivedFields) > 0 {
		p.applyDerivedFields(streams)
	}

	// Apply proxy-side post-processing for Loki features VL doesn't natively support.
	// These are applied after VL returns results, implementing Loki behavior at the proxy.
	// TODO: Remove each when VL adds native equivalents.
	logqlQuery := r.FormValue("query")
	if strings.Contains(logqlQuery, "decolorize") {
		decolorizeStreams(streams)
	}
	if label, cidr, ok := parseIPFilter(logqlQuery); ok {
		streams = ipFilterStreams(streams, label, cidr)
	}
	if tmpl := extractLineFormatTemplate(logqlQuery); tmpl != "" {
		applyLineFormatTemplate(streams, tmpl)
	}

	p.writeJSON(w, map[string]interface{}{
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
}

// streamLogQuery streams VL NDJSON response as chunked Loki-compatible JSON.
func (p *Proxy) streamLogQuery(w http.ResponseWriter, resp *http.Response, originalQuery string, categorizedLabels bool, emitStructuredMetadata bool) {
	flusher, canFlush := w.(http.Flusher)

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Transfer-Encoding", "chunked")

	// Write opening envelope
	openEnvelope := `{"status":"success","data":{"resultType":"streams"`
	if categorizedLabels {
		openEnvelope += `,"encodingFlags":["categorize-labels"]`
	}
	openEnvelope += `,"result":[`
	w.Write([]byte(openEnvelope))
	if canFlush {
		flusher.Flush()
	}

	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 1024*1024), 1024*1024)

	exposureCache := make(map[string][]metadataFieldExposure, 16)
	smBuf2 := metadataMapPool.Get().(map[string]string)
	pfBuf2 := metadataMapPool.Get().(map[string]string)
	defer func() {
		for k := range smBuf2 {
			delete(smBuf2, k)
		}
		for k := range pfBuf2 {
			delete(pfBuf2, k)
		}
		metadataMapPool.Put(smBuf2)
		metadataMapPool.Put(pfBuf2)
	}()

	first := true
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}

		var entry map[string]interface{}
		if err := stdjson.Unmarshal(line, &entry); err != nil {
			continue
		}

		timeStr, ok := stringifyEntryValue(entry["_time"])
		if !ok || timeStr == "" {
			continue
		}
		msg, _ := stringifyEntryValue(entry["_msg"])
		streamLabels := parseStreamLabels(asString(entry["_stream"]))
		msg = reconstructLogLine(msg, entry, streamLabels, originalQuery)

		tsNanos, ok := formatEntryTimestamp(timeStr)
		if !ok {
			continue
		}

		labels, structuredMetadata, parsedFields := p.classifyEntryFields(entry, originalQuery, exposureCache, smBuf2, pfBuf2)
		translatedLabels := labels
		if !p.labelTranslator.IsPassthrough() {
			translatedLabels = p.labelTranslator.TranslateLabelsMap(labels)
		}
		ensureDetectedLevel(translatedLabels)
		ensureSyntheticServiceName(translatedLabels)

		stream := map[string]interface{}{
			"stream": translatedLabels,
			"values": buildStreamValues(tsNanos, msg, structuredMetadata, parsedFields, emitStructuredMetadata, categorizedLabels),
		}

		chunk, _ := stdjson.Marshal(stream)
		if !first {
			w.Write([]byte(","))
		}
		w.Write(chunk)
		first = false

		if canFlush {
			flusher.Flush()
		}
	}

	// Close envelope
	w.Write([]byte(`],"stats":{}}}`))
	if canFlush {
		flusher.Flush()
	}
}

// applyDerivedFields extracts values from log lines using regex and adds them as labels.
// This enables Grafana's "Derived fields" feature for trace linking.
func (p *Proxy) applyDerivedFields(streams []map[string]interface{}) {
	// Pre-compile regexes
	type compiledDF struct {
		name string
		re   *regexp.Regexp
		url  string
	}
	compiled := make([]compiledDF, 0, len(p.derivedFields))
	for _, df := range p.derivedFields {
		re, err := regexp.Compile(df.MatcherRegex)
		if err != nil {
			p.log.Warn("invalid derived field regex", "name", df.Name, "error", err)
			continue
		}
		compiled = append(compiled, compiledDF{name: df.Name, re: re, url: df.URL})
	}

	for _, stream := range streams {
		values, ok := stream["values"].([]interface{})
		if !ok {
			continue
		}
		labels := streamStringMap(stream["stream"])
		if labels == nil {
			continue
		}

		for _, val := range values {
			pair, ok := val.([]interface{})
			if !ok || len(pair) < 2 {
				continue
			}
			line, ok := pair[1].(string)
			if !ok {
				continue
			}
			for _, cdf := range compiled {
				matches := cdf.re.FindStringSubmatch(line)
				if len(matches) > 1 {
					// Use first capture group as the value
					labels[cdf.name] = matches[1]
				} else if len(matches) == 1 {
					// Full match, no capture group
					labels[cdf.name] = matches[0]
				}
			}
		}
	}
}

func streamStringMap(value interface{}) map[string]string {
	switch labels := value.(type) {
	case map[string]string:
		return labels
	case map[string]interface{}:
		result := make(map[string]string, len(labels))
		for k, v := range labels {
			if s, ok := v.(string); ok {
				result[k] = s
			}
		}
		return result
	default:
		return nil
	}
}

// --- Response converters ---

func lokiLabelsResponse(labels []string) []byte {
	result, _ := stdjson.Marshal(map[string]interface{}{
		"status": "success",
		"data":   labels,
	})
	return result
}

// vlEntryPool pools map[string]interface{} to reduce GC pressure in NDJSON parsing.
var vlEntryPool = sync.Pool{
	New: func() interface{} {
		return make(map[string]interface{}, 8)
	},
}

// vlFJParserPool pools fastjson.Parser instances for the vlReaderToLokiStreams hot path.
// fastjson.Parser holds an internal buffer that is reused across ParseBytes calls,
// giving zero heap allocations per entry vs gojson's 33 allocs/op.
var vlFJParserPool fj.ParserPool

// metadataMapPool pools map[string]string used for per-entry structured metadata
// and parsed fields in vlReaderToLokiStreams. The maps are reused across log
// entries within a single response; metadataFieldMap copies content before the
// map is cleared, so reuse is safe.
var metadataMapPool = sync.Pool{
	New: func() interface{} {
		return make(map[string]string, 8)
	},
}

// vlLogsToLokiStreams converts VL newline-delimited JSON logs to Loki streams format.
// Optimized: byte scanning, pooled maps, pre-allocated slices.
func vlLogsToLokiStreams(body []byte) []map[string]interface{} {
	type streamEntry struct {
		Labels map[string]string
		Values [][]string // [[timestamp_ns, line], ...]
	}
	// Estimate line count from body size (~200 bytes/line average)
	estimatedLines := len(body)/200 + 1
	streamMap := make(map[string]*streamEntry, estimatedLines/10+1)
	streamOrder := make([]string, 0, estimatedLines/10+1)

	// Scan lines without copying the entire body to a string.
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

		// Trim whitespace (avoid bytes.TrimSpace allocation)
		for len(line) > 0 && (line[0] == ' ' || line[0] == '\t' || line[0] == '\r') {
			line = line[1:]
		}
		for len(line) > 0 && (line[len(line)-1] == ' ' || line[len(line)-1] == '\t' || line[len(line)-1] == '\r') {
			line = line[:len(line)-1]
		}
		if len(line) == 0 {
			continue
		}

		// Use pooled map to reduce allocations
		entry := vlEntryPool.Get().(map[string]interface{})
		for k := range entry {
			delete(entry, k) // clear reused map
		}
		if err := stdjson.Unmarshal(line, &entry); err != nil {
			vlEntryPool.Put(entry)
			continue
		}

		// Extract _time, _msg, _stream, and remaining fields as labels
		timeStr, _ := entry["_time"].(string)
		msg, _ := stringifyEntryValue(entry["_msg"])
		if timeStr == "" {
			vlEntryPool.Put(entry)
			continue
		}

		// Parse time to nanoseconds
		ts, err := time.Parse(time.RFC3339Nano, timeStr)
		if err != nil {
			vlEntryPool.Put(entry)
			continue
		}
		tsNanos := strconv.FormatInt(ts.UnixNano(), 10)

		// Reconstruct JSON log line from VL's extracted fields. No originalQuery
		// context is available here — pass empty string so only auto-ingestion
		// fields are reconstructed (no text-extraction parser stages present).
		tailStreamLabels := parseStreamLabels(asString(entry["_stream"]))
		if len(entry) > len(tailStreamLabels)+5 {
			msg = reconstructLogLine(msg, entry, tailStreamLabels, "")
		}

		labels := buildEntryLabelsWithStream(entry, tailStreamLabels)
		streamKey := canonicalLabelsKey(labels)

		se, ok := streamMap[streamKey]
		if !ok {
			se = &streamEntry{
				Labels: labels,
				Values: make([][]string, 0),
			}
			streamMap[streamKey] = se
			streamOrder = append(streamOrder, streamKey)
		}

		// Return pooled entry after extracting all needed data
		vlEntryPool.Put(entry)

		se.Values = append(se.Values, []string{tsNanos, msg})
	}

	// Sort streams by key for stable cross-stream ordering. VL returns entries
	// in non-deterministic stream order; without this, same-timestamp entries
	// from different streams reorder between requests.
	sort.Strings(streamOrder)
	result := make([]map[string]interface{}, 0, len(streamMap))
	for _, key := range streamOrder {
		se := streamMap[key]
		// Only tie-break entries with identical nanosecond timestamps by message
		// content. Different-timestamp entries are left in VL's returned order
		// (which already reflects the requested direction: ascending/descending).
		sort.SliceStable(se.Values, func(i, j int) bool {
			ti, tj := se.Values[i][0], se.Values[j][0]
			if ti != tj {
				return false
			}
			return se.Values[i][1] < se.Values[j][1]
		})
		result = append(result, map[string]interface{}{
			"stream": se.Labels,
			"values": se.Values,
		})
	}
	return result
}

type cachedLogQueryStreamDescriptor struct {
	key              string
	rawLabels        map[string]string
	translatedLabels map[string]string
}

func (p *Proxy) vlReaderToLokiStreams(r io.Reader, originalQuery, step string, categorizedLabels bool, emitStructuredMetadata bool, collectPatterns bool) ([]map[string]interface{}, []map[string]interface{}, error) {
	type streamEntry struct {
		Labels map[string]string
		Values []interface{}
	}

	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 0, 64*1024), 8*1024*1024)

	streamMap := make(map[string]*streamEntry, 32)
	streamOrder := make([]string, 0, 32)
	streamDescriptorCache := make(map[string]cachedLogQueryStreamDescriptor, 16)
	streamLabelCache := make(map[string]map[string]string, 16)
	exposureCache := make(map[string][]metadataFieldExposure, 16)
	classifyAsParsed := hasParserStage(originalQuery, "json") || hasParserStage(originalQuery, "logfmt")
	skipLogLineReconstruction := hasTextExtractionParser(originalQuery)
	needsClassification := emitStructuredMetadata || categorizedLabels

	var (
		miner        *patternMiner
		stepSeconds  int64
		patternCount int
	)
	if collectPatterns {
		miner = newPatternMiner()
		stepSeconds = parsePatternStepSeconds(step)
	}

	smBuf := metadataMapPool.Get().(map[string]string)
	pfBuf := metadataMapPool.Get().(map[string]string)
	defer func() {
		for k := range smBuf {
			delete(smBuf, k)
		}
		for k := range pfBuf {
			delete(pfBuf, k)
		}
		metadataMapPool.Put(smBuf)
		metadataMapPool.Put(pfBuf)
	}()

	for scanner.Scan() {
		line := scanner.Bytes()
		for len(line) > 0 && (line[0] == ' ' || line[0] == '\t' || line[0] == '\r') {
			line = line[1:]
		}
		for len(line) > 0 && (line[len(line)-1] == ' ' || line[len(line)-1] == '\t' || line[len(line)-1] == '\r') {
			line = line[:len(line)-1]
		}
		if len(line) == 0 {
			continue
		}

		fjParser := vlFJParserPool.Get()
		fjVal, err := fjParser.ParseBytes(line)
		if err != nil {
			vlFJParserPool.Put(fjParser)
			continue
		}

		timeBytes := fjVal.GetStringBytes("_time")
		if len(timeBytes) == 0 {
			vlFJParserPool.Put(fjParser)
			continue
		}
		timeStr := string(timeBytes)
		tsNanos, ok := formatEntryTimestamp(timeStr)
		if !ok {
			vlFJParserPool.Put(fjParser)
			continue
		}
		msg := string(fjVal.GetStringBytes("_msg"))
		rawStream := string(fjVal.GetStringBytes("_stream"))
		level := string(fjVal.GetStringBytes("level"))

		// Compute stream descriptor first so reconstructLogLine can reuse the
		// already-parsed stream labels instead of re-parsing _stream itself.
		desc := p.logQueryStreamDescriptor(rawStream, level, streamLabelCache, streamDescriptorCache)

		fjObj, fjErr := fjVal.Object()
		if fjErr != nil {
			vlFJParserPool.Put(fjParser)
			continue
		}
		msg = reconstructLogLineWithFlagFJ(msg, fjObj, desc.rawLabels, skipLogLineReconstruction)
		// classifyEntryMetadataFieldsFJ returns smBuf/pfBuf directly (no copy).
		// buildStreamValue → metadataFieldMap copies them before the next iteration
		// clears the buffers. Do not move buildStreamValue below another classify call.
		// Standard Grafana requests have emitStructuredMetadata=false and categorizedLabels=false,
		// so skip the per-field visit entirely — buildStreamValue discards these maps anyway.
		var structuredMetadata, parsedFields map[string]string
		if needsClassification {
			structuredMetadata, parsedFields = p.classifyEntryMetadataFieldsFJ(fjObj, desc.rawLabels, classifyAsParsed, exposureCache, smBuf, pfBuf)
		}
		se, ok := streamMap[desc.key]
		if !ok {
			se = &streamEntry{
				Labels: desc.translatedLabels,
				Values: make([]interface{}, 0, 8),
			}
			streamMap[desc.key] = se
			streamOrder = append(streamOrder, desc.key)
		}
		se.Values = append(se.Values, buildStreamValue(tsNanos, msg, structuredMetadata, parsedFields, emitStructuredMetadata, categorizedLabels))

		if miner != nil {
			levelValue := strings.TrimSpace(desc.rawLabels["detected_level"])
			if levelValue == "" {
				levelValue = strings.TrimSpace(desc.rawLabels["level"])
			}
			if unixSeconds, ok := parseFlexibleUnixSeconds(timeStr); ok {
				bucket := unixSeconds
				if stepSeconds > 0 {
					bucket = (bucket / stepSeconds) * stepSeconds
				}
				miner.Observe(levelValue, msg, bucket)
				patternCount++
			}
		}

		vlFJParserPool.Put(fjParser)
	}
	if err := scanner.Err(); err != nil {
		return nil, nil, err
	}

	sort.Strings(streamOrder)
	result := make([]map[string]interface{}, 0, len(streamMap))
	for _, key := range streamOrder {
		se := streamMap[key]
		// Only tie-break entries with identical nanosecond timestamps by message
		// content. Different-timestamp entries are left in VL's returned order
		// (which already reflects the requested direction: ascending/descending).
		sort.SliceStable(se.Values, func(i, j int) bool {
			vi, _ := se.Values[i].([]interface{})
			vj, _ := se.Values[j].([]interface{})
			if len(vi) < 2 || len(vj) < 2 {
				return false
			}
			ti, _ := vi[0].(string)
			tj, _ := vj[0].(string)
			if ti != tj {
				return false
			}
			msgi, _ := vi[1].(string)
			msgj, _ := vj[1].(string)
			return msgi < msgj
		})
		result = append(result, map[string]interface{}{
			"stream": se.Labels,
			"values": se.Values,
		})
	}

	var patterns []map[string]interface{}
	if miner != nil && patternCount > 0 {
		patterns = buildPatternResponse(miner, maxPatternResponseLimit)
	}
	return result, patterns, nil
}

func (p *Proxy) logQueryStreamDescriptor(rawStream, level string, streamLabelCache map[string]map[string]string, descriptorCache map[string]cachedLogQueryStreamDescriptor) cachedLogQueryStreamDescriptor {
	cacheKey := rawStream + "\x00" + strings.TrimSpace(level)
	if desc, ok := descriptorCache[cacheKey]; ok {
		return desc
	}

	baseLabels, ok := streamLabelCache[rawStream]
	if !ok {
		baseLabels = parseStreamLabels(rawStream)
		streamLabelCache[rawStream] = baseLabels
	}

	rawLabels := cloneStringMap(baseLabels)
	if trimmed := strings.TrimSpace(level); trimmed != "" {
		rawLabels["level"] = trimmed
	}
	ensureDetectedLevel(rawLabels)
	ensureSyntheticServiceName(rawLabels)

	translatedLabels := rawLabels
	if p != nil && p.labelTranslator != nil && !p.labelTranslator.IsPassthrough() {
		translatedLabels = p.labelTranslator.TranslateLabelsMap(rawLabels)
		ensureDetectedLevel(translatedLabels)
		ensureSyntheticServiceName(translatedLabels)
	}

	desc := cachedLogQueryStreamDescriptor{
		key:              canonicalLabelsKey(rawLabels),
		rawLabels:        rawLabels,
		translatedLabels: translatedLabels,
	}
	descriptorCache[cacheKey] = desc
	return desc
}

// classifyEntryMetadataFields fills smBuf and pfBuf with structured metadata
// and parsed fields for the given log entry. Both buffers must be pre-allocated
// and are cleared before use; callers must copy or consume content before the
// next call (metadataFieldMap makes the required copy inside buildStreamValue).
func (p *Proxy) classifyEntryMetadataFields(entry map[string]interface{}, streamLabels map[string]string, classifyAsParsed bool, exposureCache map[string][]metadataFieldExposure, smBuf, pfBuf map[string]string) (map[string]string, map[string]string) {
	for k := range smBuf {
		delete(smBuf, k)
	}
	for k := range pfBuf {
		delete(pfBuf, k)
	}

	for key, value := range entry {
		if isVLInternalField(key) || key == "_stream_id" || key == "level" {
			continue
		}
		if _, exists := streamLabels[key]; exists {
			continue
		}
		stringValue, ok := stringifyEntryValue(value)
		if !ok || strings.TrimSpace(stringValue) == "" {
			continue
		}
		exposures := p.metadataFieldExposuresCached(key, exposureCache)
		for _, exposure := range exposures {
			if _, exists := streamLabels[exposure.name]; exists && !exposure.isAlias {
				continue
			}
			if classifyAsParsed {
				pfBuf[exposure.name] = stringValue
				continue
			}
			smBuf[exposure.name] = stringValue
		}
	}

	var sm, pf map[string]string
	if len(smBuf) > 0 {
		sm = smBuf
	}
	if len(pfBuf) > 0 {
		pf = pfBuf
	}
	return sm, pf
}

// classifyEntryMetadataFieldsFJ is the fastjson variant of classifyEntryMetadataFields.
// It uses fj.Object.Visit to iterate fields without allocating a map[string]interface{}.
func (p *Proxy) classifyEntryMetadataFieldsFJ(obj *fj.Object, streamLabels map[string]string, classifyAsParsed bool, exposureCache map[string][]metadataFieldExposure, smBuf, pfBuf map[string]string) (map[string]string, map[string]string) {
	for k := range smBuf {
		delete(smBuf, k)
	}
	for k := range pfBuf {
		delete(pfBuf, k)
	}
	obj.Visit(func(k []byte, val *fj.Value) {
		key := string(k)
		if isVLInternalField(key) || key == "_stream_id" || key == "level" {
			return
		}
		if _, exists := streamLabels[key]; exists {
			return
		}
		sv, ok := stringifyFJValue(val)
		if !ok || strings.TrimSpace(sv) == "" {
			return
		}
		for _, exposure := range p.metadataFieldExposuresCached(key, exposureCache) {
			if _, exists := streamLabels[exposure.name]; exists && !exposure.isAlias {
				continue
			}
			if classifyAsParsed {
				pfBuf[exposure.name] = sv
				continue
			}
			smBuf[exposure.name] = sv
		}
	})
	var sm, pf map[string]string
	if len(smBuf) > 0 {
		sm = smBuf
	}
	if len(pfBuf) > 0 {
		pf = pfBuf
	}
	return sm, pf
}

func (p *Proxy) metadataFieldExposuresCached(vlField string, exposureCache map[string][]metadataFieldExposure) []metadataFieldExposure {
	if exposureCache == nil {
		return p.metadataFieldExposures(vlField)
	}
	if exposures, ok := exposureCache[vlField]; ok {
		return exposures
	}
	exposures := p.metadataFieldExposures(vlField)
	exposureCache[vlField] = exposures
	return exposures
}

func cloneStringMap(src map[string]string) map[string]string {
	if len(src) == 0 {
		return map[string]string{}
	}
	out := make(map[string]string, len(src))
	for key, value := range src {
		out[key] = value
	}
	return out
}

func stringifyEntryValue(value interface{}) (string, bool) {
	if value == nil {
		return "", false
	}
	switch v := value.(type) {
	case string:
		return v, true
	case map[string]interface{}, []interface{}:
		// VL may parse JSON-looking _msg fields into objects; re-serialize to
		// preserve valid JSON so Grafana can pretty-print the log line.
		b, err := stdjson.Marshal(v)
		if err == nil {
			return string(b), true
		}
		return fmt.Sprintf("%v", v), true
	default:
		return fmt.Sprintf("%v", v), true
	}
}

// stringifyFJValue is the fastjson equivalent of stringifyEntryValue.
// It converts a *fj.Value to a string without heap allocations for the
// common string case. Objects and arrays are re-serialised as JSON to
// preserve the same behaviour as stringifyEntryValue.
func stringifyFJValue(v *fj.Value) (string, bool) {
	if v == nil {
		return "", false
	}
	switch v.Type() {
	case fj.TypeNull:
		return "", false
	case fj.TypeString:
		return string(v.GetStringBytes()), true
	case fj.TypeObject, fj.TypeArray:
		// Re-serialize with encoding/json to get HTML-safe output (<, >, & escaped),
		// matching stringifyEntryValue's stdjson.Marshal behaviour for nested objects.
		b, err := stdjson.Marshal(stdjson.RawMessage(v.MarshalTo(nil)))
		if err == nil {
			return string(b), true
		}
		return v.String(), true
	default:
		// Numbers and bools: v.String() produces "200", "true" — identical to fmt.Sprintf("%v").
		return v.String(), true
	}
}

func formatEntryTimestamp(timeStr string) (string, bool) {
	if parsed, err := time.Parse(time.RFC3339Nano, timeStr); err == nil {
		return strconv.FormatInt(parsed.UnixNano(), 10), true
	}
	if parsed, err := time.Parse(time.RFC3339, timeStr); err == nil {
		return strconv.FormatInt(parsed.UnixNano(), 10), true
	}
	if _, err := strconv.ParseInt(timeStr, 10, 64); err == nil {
		return timeStr, true
	}
	return "", false
}

func buildStreamValues(ts, msg string, structuredMetadata map[string]string, parsedFields map[string]string, emitStructuredMetadata bool, categorizedLabels bool) []interface{} {
	return []interface{}{buildStreamValue(ts, msg, structuredMetadata, parsedFields, emitStructuredMetadata, categorizedLabels)}
}

var emptyCategorizedMetadata = map[string]interface{}{}

func buildStreamValue(ts, msg string, structuredMetadata map[string]string, parsedFields map[string]string, emitStructuredMetadata bool, categorizedLabels bool) interface{} {
	if !categorizedLabels {
		return []interface{}{ts, msg}
	}

	if emitStructuredMetadata {
		metadata := make(map[string]interface{}, 2)
		if len(structuredMetadata) > 0 {
			metadata["structuredMetadata"] = metadataFieldMap(structuredMetadata)
		}
		if len(parsedFields) > 0 {
			metadata["parsed"] = metadataFieldMap(parsedFields)
		}
		if len(metadata) > 0 {
			return []interface{}{ts, msg, metadata}
		}
	}
	return []interface{}{ts, msg, emptyCategorizedMetadata}
}

func metadataFieldMap(fields map[string]string) map[string]string {
	if len(fields) == 0 {
		return nil
	}
	pairs := make(map[string]string, len(fields))
	for key, value := range fields {
		pairs[key] = value
	}
	return pairs
}

// classifyEntryFields classifies log entry fields into stream labels, structured
// metadata, and parsed fields. smBuf and pfBuf are pre-allocated reusable maps
// that are cleared and filled; callers must copy content before the next call
// (buildStreamValue → metadataFieldMap makes the required copy). exposureCache
// is a per-batch cache keyed by VL field name to avoid repeated slice allocs.
// For tight per-entry loops use classifyEntryFieldsWithFlags instead to avoid
// recomputing parser flags and stream labels on every call.
func (p *Proxy) classifyEntryFields(entry map[string]interface{}, originalQuery string, exposureCache map[string][]metadataFieldExposure, smBuf, pfBuf map[string]string) (map[string]string, map[string]string, map[string]string) {
	classifyAsParsed := hasParserStage(originalQuery, "json") || hasParserStage(originalQuery, "logfmt")
	streamLabels := parseStreamLabels(asString(entry["_stream"]))
	return p.classifyEntryFieldsWithFlags(entry, streamLabels, classifyAsParsed, exposureCache, smBuf, pfBuf)
}

// classifyEntryFieldsWithFlags is the hot-path variant of classifyEntryFields
// for tight per-entry loops where originalQuery and stream labels are constant.
// classifyAsParsed and streamLabels must be pre-computed once before the loop.
func (p *Proxy) classifyEntryFieldsWithFlags(entry map[string]interface{}, streamLabels map[string]string, classifyAsParsed bool, exposureCache map[string][]metadataFieldExposure, smBuf, pfBuf map[string]string) (map[string]string, map[string]string, map[string]string) {
	labels := make(map[string]string, len(streamLabels))
	for k, v := range streamLabels {
		labels[k] = v
	}
	if value, ok := stringifyEntryValue(entry["level"]); ok && strings.TrimSpace(value) != "" {
		labels["level"] = value
		// Explicit level field always wins over VL's auto-detected level.
		// VL may set detected_level="info" from _msg when the message body has no
		// level keyword, even if the logfmt key level=warn was parsed separately.
		delete(labels, "detected_level")
	}
	// Mirror Loki's ingest-time level detection: if VL did not surface level as
	// a top-level field (native field or OTel severity), try to extract it from
	// the raw _msg string (JSON or logfmt) so detected_level matches Loki.
	if labels["level"] == "" && labels["detected_level"] == "" {
		if msgStr, ok := entry["_msg"].(string); ok {
			if lvl, ok := extractLevelFromMsg(msgStr); ok {
				labels["level"] = lvl
			}
		}
	}
	ensureDetectedLevel(labels)
	ensureSyntheticServiceName(labels)

	for k := range smBuf {
		delete(smBuf, k)
	}
	for k := range pfBuf {
		delete(pfBuf, k)
	}

	for key, value := range entry {
		if isVLInternalField(key) || key == "_stream_id" || key == "level" {
			continue
		}
		if _, exists := labels[key]; exists {
			continue
		}
		stringValue, ok := stringifyEntryValue(value)
		if !ok || strings.TrimSpace(stringValue) == "" {
			continue
		}
		for _, exposure := range p.metadataFieldExposuresCached(key, exposureCache) {
			if _, exists := labels[exposure.name]; exists && !exposure.isAlias {
				continue
			}
			if classifyAsParsed {
				pfBuf[exposure.name] = stringValue
				continue
			}
			smBuf[exposure.name] = stringValue
		}
	}

	var sm, pf map[string]string
	if len(smBuf) > 0 {
		sm = smBuf
	}
	if len(pfBuf) > 0 {
		pf = pfBuf
	}
	return labels, sm, pf
}

// streamLabelsCacheMu guards streamLabelsCache to prevent concurrent map writes.
// The cache avoids re-parsing the same _stream value (identical for all log lines
// in a stream), which was the top allocation hot path in pprof (10-15% of totals).
var (
	streamLabelsCacheMu sync.RWMutex
	streamLabelsCache   = make(map[string]map[string]string, 256)
)

// parseStreamLabels parses {key="value",key2="value2"} into a map.
// Results are cached by the raw string — _stream values repeat heavily across
// a result set, so the cache eliminates redundant allocations.
// Callers must not mutate the returned map.
func parseStreamLabels(s string) map[string]string {
	streamLabelsCacheMu.RLock()
	if m, ok := streamLabelsCache[s]; ok {
		streamLabelsCacheMu.RUnlock()
		return m
	}
	streamLabelsCacheMu.RUnlock()

	m := parseStreamLabelsUncached(s)

	streamLabelsCacheMu.Lock()
	// Bound cache size to avoid unbounded growth.
	if len(streamLabelsCache) < 4096 {
		streamLabelsCache[s] = m
	}
	streamLabelsCacheMu.Unlock()
	return m
}

// parseStreamLabelsUncached parses without cache lookup. Inlines the split loop
// to avoid allocating an intermediate []string of pairs.
func parseStreamLabelsUncached(s string) map[string]string {
	s = strings.Trim(s, "{}")
	if s == "" {
		return map[string]string{}
	}
	labels := make(map[string]string, 4)
	inQuote := false
	start := 0
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c == '"' {
			inQuote = !inQuote
		}
		if c == ',' && !inQuote {
			appendLabelPair(s[start:i], labels)
			start = i + 1
		}
	}
	appendLabelPair(s[start:], labels)
	return labels
}

// appendLabelPair parses a single key="value" token into dst.
func appendLabelPair(pair string, dst map[string]string) {
	pair = strings.TrimSpace(pair)
	eq := strings.IndexByte(pair, '=')
	if eq <= 0 {
		return
	}
	k := strings.TrimSpace(pair[:eq])
	v := strings.TrimSpace(pair[eq+1:])
	v = strings.Trim(v, `"`)
	dst[k] = v
}

func wrapAsLokiResponse(vlBody []byte, resultType string) []byte {
	// Fast path: VL stats_query_range already returns {"data":{"resultType":"matrix","result":[...]}}
	// normalizeLokiResultDataShape is a no-op when both "result" and "resultType" are present,
	// so we can skip the full parse+marshal and just prepend the status field as a byte splice.
	if isVLDataResultTypeResponse(vlBody) {
		// Prepend status field without any size arithmetic — avoids overflow in capacity calc.
		return append([]byte(`{"status":"success",`), vlBody[1:]...)
	}

	// VL stats endpoints return Prometheus-compatible format already.
	// Try to parse and re-wrap in Loki envelope.
	var promResp map[string]interface{}
	if err := stdjson.Unmarshal(vlBody, &promResp); err != nil {
		// If we can't parse, return as-is with wrapper
		result, _ := stdjson.Marshal(map[string]interface{}{
			"status": "success",
			"data": map[string]interface{}{
				"resultType": resultType,
				"result":     []interface{}{},
			},
		})
		return result
	}

	// Check for VL error responses and translate to Loki error format.
	// VL may return: {"error":"message"} or {"status":"error","msg":"message"}
	if errMsg, ok := promResp["error"].(string); ok {
		result, _ := stdjson.Marshal(map[string]interface{}{
			"status":    "error",
			"errorType": "bad_request",
			"error":     errMsg,
		})
		return result
	}
	if promResp["status"] == "error" {
		errMsg := ""
		if msg, ok := promResp["msg"].(string); ok {
			errMsg = msg
		} else if msg, ok := promResp["message"].(string); ok {
			errMsg = msg
		} else if msg, ok := promResp["error"].(string); ok {
			errMsg = msg
		}
		result, _ := stdjson.Marshal(map[string]interface{}{
			"status":    "error",
			"errorType": "bad_request",
			"error":     errMsg,
		})
		return result
	}

	// If VL already returned status/data format, pass through
	if rawData, ok := promResp["data"]; ok {
		if dataMap, ok := rawData.(map[string]interface{}); ok {
			normalizeLokiResultDataShape(dataMap, resultType)
			result, _ := stdjson.Marshal(map[string]interface{}{
				"status": "success",
				"data":   dataMap,
			})
			return result
		}
		result, _ := stdjson.Marshal(map[string]interface{}{
			"status": "success",
			"data": map[string]interface{}{
				"resultType": resultType,
				"result":     []interface{}{},
			},
		})
		return result
	}

	normalizeLokiResultDataShape(promResp, resultType)

	result, _ := stdjson.Marshal(map[string]interface{}{
		"status": "success",
		"data":   promResp,
	})
	return result
}

// isVLDataResultTypeResponse returns true when vlBody is the compact VL format
// {"data":{"resultType":"...","result":[...]}} — the fast path for wrapAsLokiResponse.
// normalizeLokiResultDataShape is a no-op for this format since both "result" and
// "resultType" are already present, so we can skip parse+marshal entirely.
func isVLDataResultTypeResponse(vlBody []byte) bool {
	const needle = `{"data":{"resultType":`
	if len(vlBody) < len(needle) {
		return false
	}
	for i := range needle {
		if vlBody[i] != needle[i] {
			return false
		}
	}
	return true
}

func normalizeLokiResultDataShape(data map[string]interface{}, defaultResultType string) {
	if data == nil {
		return
	}

	if _, hasResult := data["result"]; !hasResult {
		if rawResults, ok := data["results"]; ok {
			data["result"] = rawResults
		} else {
			data["result"] = []interface{}{}
		}
	}

	currentResultType, _ := data["resultType"].(string)
	if strings.TrimSpace(currentResultType) == "" && strings.TrimSpace(defaultResultType) != "" {
		data["resultType"] = defaultResultType
	}
}

// --- VL hits response conversion helpers ---

type vlHitsResponse struct {
	Hits []struct {
		Fields     map[string]string `json:"fields"`
		Timestamps []vlTimestamp     `json:"timestamps"`
		Values     []int             `json:"values"`
	} `json:"hits"`
}

type vlTimestamp string

func (t *vlTimestamp) UnmarshalJSON(data []byte) error {
	if len(data) == 0 || string(data) == "null" {
		*t = ""
		return nil
	}
	if data[0] == '"' {
		var s string
		if err := stdjson.Unmarshal(data, &s); err != nil {
			return err
		}
		*t = vlTimestamp(s)
		return nil
	}
	var n stdjson.Number
	if err := stdjson.Unmarshal(data, &n); err != nil {
		return err
	}
	*t = vlTimestamp(n.String())
	return nil
}

// parseTimestampToUnix converts a VL timestamp (RFC3339 string or numeric) to Unix seconds.
func parseTimestampToUnix(ts string) float64 {
	if parsed, ok := parseFlexibleUnixSeconds(ts); ok {
		return float64(parsed)
	}
	return float64(time.Now().Unix())
}

func parseHits(body []byte) vlHitsResponse {
	var resp vlHitsResponse
	if err := stdjson.Unmarshal(body, &resp); err != nil {
		return vlHitsResponse{}
	}
	return resp
}

type requestedBucketRange struct {
	start int64
	end   int64
	step  int64
	count int
}

type bareParserMetricCompatSpec struct {
	funcName        string
	baseQuery       string
	rangeWindow     time.Duration
	rangeWindowExpr string
	unwrapField     string
	quantile        float64
}

type bareParserMetricSample struct {
	tsNanos int64
	value   float64
}

type bareParserMetricSeries struct {
	metric  map[string]string
	samples []bareParserMetricSample
}

// isStatsQuery returns true if the LogsQL query contains a stats pipe.
// It only matches top-level pipes, not strings inside quoted filter values
// (e.g., ~"stats query" must NOT trigger this).
func isStatsQuery(logsqlQuery string) bool {
	// Walk the query, skipping quoted regions
	inQuote := false
	for i := 0; i < len(logsqlQuery); i++ {
		if logsqlQuery[i] == '"' {
			inQuote = !inQuote
			continue
		}
		if inQuote {
			continue
		}
		rest := logsqlQuery[i:]
		if strings.HasPrefix(rest, "| stats ") ||
			strings.HasPrefix(rest, "| rate(") ||
			strings.HasPrefix(rest, "| count(") {
			return true
		}
	}
	return false
}

// parseDeleteTimestamp parses a timestamp string used in delete requests and
// returns Unix nanoseconds. It accepts float seconds, float nanoseconds (>1e15),
// RFC3339Nano, and RFC3339. Returns an error for unrecognized formats so callers
// can reject them rather than forwarding an unbounded time range.
func parseDeleteTimestamp(ts string) (int64, error) {
	if f, err := strconv.ParseFloat(ts, 64); err == nil {
		if f > 1e15 {
			// Already nanoseconds — keep as-is (truncated to int64).
			return int64(f), nil
		}
		// Seconds — multiply to nanoseconds.
		return int64(f * 1e9), nil
	}
	if parsed, err := time.Parse(time.RFC3339Nano, ts); err == nil {
		return parsed.UnixNano(), nil
	}
	if parsed, err := time.Parse(time.RFC3339, ts); err == nil {
		return parsed.UnixNano(), nil
	}
	return 0, fmt.Errorf("unrecognized timestamp format: %q", ts)
}

func formatVLTimestamp(ts string) string {
	// Loki sends Unix timestamps (seconds or nanoseconds).
	// Grafana drilldown resource endpoints send RFC3339 timestamps, while
	// query endpoints usually send numeric Unix values. Normalize RFC3339 to
	// Unix nanoseconds so every VL endpoint sees the same time format.
	if _, err := strconv.ParseFloat(ts, 64); err == nil {
		// Already numeric — preserve caller precision.
		return ts
	}
	if parsed, err := time.Parse(time.RFC3339Nano, ts); err == nil {
		return strconv.FormatInt(parsed.UnixNano(), 10)
	}
	if parsed, err := time.Parse(time.RFC3339, ts); err == nil {
		return strconv.FormatInt(parsed.UnixNano(), 10)
	}
	return ts
}

// formatVLStep converts Loki's step parameter to VL duration format.
// Loki/Prometheus sends step as seconds (numeric string like "60") or duration ("1m").
// VL requires duration strings (e.g., "60s", "1m", "1h").
func formatVLStep(step string) string {
	step = strings.TrimSpace(step)
	if step == "" {
		return step
	}
	// If it's already a duration string (contains letter), pass through
	for _, ch := range step {
		if (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') {
			return step
		}
	}
	// Numeric-only: treat as seconds, append "s"
	if seconds, err := strconv.ParseFloat(step, 64); err == nil && seconds > 0 {
		return strconv.FormatFloat(seconds, 'f', -1, 64) + "s"
	}
	return step
}

func requestWantsCategorizedLabels(r *http.Request) bool {
	if r == nil {
		return false
	}
	for _, raw := range r.Header.Values("X-Loki-Response-Encoding-Flags") {
		for _, part := range strings.Split(raw, ",") {
			if strings.EqualFold(strings.TrimSpace(part), "categorize-labels") {
				return true
			}
		}
	}
	return false
}

// buildSlidingWindowSumsFromHits converts pre-aggregated VL hits buckets into
// bareParserMetricSeries using sliding-window sums.
//
// buckets:   canonicalLabelKey → bucketStartNanos → count
// labelSets: canonicalLabelKey → map[string]string label pairs
// evalStart, evalEnd, stepNs, rangeNs: all in nanoseconds
// isRate: if true, divide count by rangeNs/1e9 to get per-second rate
//
// Absent evaluation points (sum == 0) are omitted (matches Loki absent-point behaviour).
func buildSlidingWindowSumsFromHits(
	buckets map[string]map[int64]int64,
	labelSets map[string]map[string]string,
	evalStart, evalEnd, stepNs, rangeNs int64,
	isRate bool,
) []bareParserMetricSeries {
	if len(buckets) == 0 {
		return nil
	}
	result := make([]bareParserMetricSeries, 0, len(buckets))
	for labelKey, tsBuckets := range buckets {
		labels := labelSets[labelKey]
		samples := make([]bareParserMetricSample, 0, (evalEnd-evalStart)/stepNs+2)
		for evalT := evalStart; evalT <= evalEnd; evalT += stepNs {
			windowStart := evalT - rangeNs
			var sum int64
			for bucketTs, cnt := range tsBuckets {
				// Include bucket if it overlaps [windowStart, evalT).
				// A VL hit bucket at bucketTs covers [bucketTs, bucketTs+stepNs).
				if bucketTs >= windowStart && bucketTs < evalT {
					sum += cnt
				}
			}
			if sum == 0 {
				continue // absent point — matches Loki behaviour
			}
			value := float64(sum)
			if isRate {
				value = float64(sum) / (float64(rangeNs) / 1e9)
			}
			samples = append(samples, bareParserMetricSample{tsNanos: evalT, value: value})
		}
		if len(samples) == 0 {
			continue
		}
		metric := make(map[string]string, len(labels))
		for k, v := range labels {
			metric[k] = v
		}
		result = append(result, bareParserMetricSeries{metric: metric, samples: samples})
	}
	sort.Slice(result, func(i, j int) bool {
		return canonicalLabelsKey(result[i].metric) < canonicalLabelsKey(result[j].metric)
	})
	return result
}

func tupleModeForRequest(categorizedLabels bool, emitStructuredMetadata bool) string {
	if emitStructuredMetadata && categorizedLabels {
		return "categorize_labels_3tuple"
	}
	return "default_2tuple"
}

func (p *Proxy) shouldEmitStructuredMetadata(r *http.Request) bool {
	return p.emitStructuredMetadata && requestWantsCategorizedLabels(r)
}

