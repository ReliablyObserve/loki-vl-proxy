package proxy

import (
	"bytes"
	"context"
	"encoding/json"
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
)

const unknownServiceName = "unknown_service"
const detectedFieldsSampleLimit = 500

// vlStreamEntry unmarshals only the _stream field from NDJSON log entries,
// avoiding the cost of allocating a full map[string]interface{} when only
// the stream label string is needed.
type vlStreamEntry struct {
	Stream string `json:"_stream"`
}

var suppressedDetectedFieldNames = map[string]struct{}{
	"timestamp_end":          {},
	"observed_timestamp_end": {},
	"app":                    {},
	"cluster":                {},
	"namespace":              {},
	"service_name":           {},
}

// OTel semantic convention patterns (dotted form) - priority 1 signals
var otelSemanticFields = map[string]struct{}{
	"service.name":            {},
	"service.namespace":       {},
	"k8s.pod.name":            {},
	"k8s.namespace.name":      {},
	"k8s.node.name":           {},
	"k8s.container.name":      {},
	"deployment.environment":  {},
	"deployment.name":         {},
	"deployment.version":      {},
	"host.name":               {},
	"host.id":                 {},
	"host.arch":               {},
	"telemetry.sdk.name":      {},
	"telemetry.sdk.language":  {},
	"telemetry.sdk.version":   {},
}

// OTel underscore-form prefixes (priority 2 signals)
var otelUnderscorePrefixes = []string{
	"k8s_",
	"deployment_",
	"telemetry_",
	"host_",
	"service_",
}

var serviceNameSourceFields = []string{
	"service_name",
	"service.name",
	"service",
	"app",
	"application",
	"app_name",
	"name",
	"app_kubernetes_io_name",
	"container",
	"container_name",
	"k8s.container.name",
	"k8s_container_name",
	"component",
	"workload",
	"job",
	"k8s.job.name",
	"k8s_job_name",
}

type detectedFieldSummary struct {
	label       string
	typ         string
	parsers     map[string]struct{}
	values      map[string]struct{}
	jsonPath    []string
	cardinality int
}

type detectedLabelSummary struct {
	label  string
	values map[string]struct{}
}

func shouldSuppressDetectedField(label string) bool {
	normalized := strings.ToLower(strings.TrimSpace(label))
	if normalized == "" {
		return false
	}
	_, suppressed := suppressedDetectedFieldNames[normalized]
	return suppressed
}

// isOTelData detects whether a log entry is from OTel instrumentation using hierarchical signals.
// IMPORTANT: Checks ONLY stream labels (structured metadata), not message-parsed fields.
// A service.name in JSON message content is NOT an OTel indicator - it must be a stream label.
// Priority 1: Dotted semantic conventions in stream labels (k8s.pod.name, deployment.*, etc.)
// Priority 2: Underscore OTel prefixes in stream labels (k8s_, deployment_, telemetry_, etc.)
// Priority 3: Message field indicators (trace_id, span_id in structured content)
func isOTelData(entry map[string]interface{}) bool {
	if entry == nil {
		return false
	}

	// Extract stream labels from VL response (only real labels, not message-parsed fields)
	streamStr := ""
	if s, ok := entry["_stream"].(string); ok {
		streamStr = s
	}
	streamLabels := parseStreamLabels(streamStr)

	// Priority 1: Check for dotted OTel semantic convention fields in stream labels ONLY
	for key := range streamLabels {
		if _, isOTelField := otelSemanticFields[key]; isOTelField {
			return true // Found a Priority 1 indicator in stream labels
		}
	}

	// Priority 2: Check for OTel underscore-form prefixes in stream labels ONLY
	for key := range streamLabels {
		// Special case: service_name is only an OTel indicator if paired with service.name in stream
		if key == "service_name" {
			if _, hasRealServiceName := streamLabels["service.name"]; hasRealServiceName {
				return true // Real OTel alias pair in stream labels
			}
			continue // Synthetic service_name, skip it
		}

		// Check other OTel underscore prefixes in stream labels
		for _, prefix := range otelUnderscorePrefixes {
			if strings.HasPrefix(key, prefix) {
				return true // Found a Priority 2 indicator in stream labels
			}
		}
	}

	// Priority 3: Check message field indicators (trace_id, span_id)
	// This is a backup signal but not definitive - prefer stream label signals
	if msg, ok := entry["_msg"].(string); ok && msg != "" {
		// Only treat as OTel if we also have other OTel signals in stream
		// Don't rely on message content alone to avoid false positives
		if strings.Contains(msg, "trace_id") && strings.Contains(msg, "span_id") {
			// Check if stream has any k8s or deployment fields as confirmation
			for key := range streamLabels {
				if strings.HasPrefix(key, "k8s.") || strings.HasPrefix(key, "deployment.") {
					return true // Confirmed by message + stream signals
				}
			}
		}
	}

	return false // No OTel indicators found
}

func hasServiceSignal(labels map[string]string) bool {
	if len(labels) == 0 {
		return false
	}
	for _, key := range serviceNameSourceFields {
		if strings.TrimSpace(labels[key]) != "" {
			return true
		}
	}
	return false
}

func deriveServiceName(labels map[string]string) string {
	for _, key := range serviceNameSourceFields {
		if value := strings.TrimSpace(labels[key]); value != "" {
			return value
		}
	}
	return unknownServiceName
}

func ensureSyntheticServiceName(labels map[string]string) {
	if labels == nil {
		return
	}
	if value := strings.TrimSpace(labels["service_name"]); value != "" {
		return
	}
	labels["service_name"] = deriveServiceName(labels)
}

func appendSyntheticLabels(labels []string) []string {
	seen := make(map[string]struct{}, len(labels)+1)
	out := make([]string, 0, len(labels)+1)
	for _, label := range labels {
		if _, ok := seen[label]; ok {
			continue
		}
		seen[label] = struct{}{}
		out = append(out, label)
	}
	if _, ok := seen["service_name"]; !ok {
		out = append(out, "service_name")
	}
	return out
}

func ensureDetectedLevel(labels map[string]string) {
	if labels == nil {
		return
	}
	if value := strings.TrimSpace(labels["detected_level"]); value != "" {
		return
	}
	if value := strings.TrimSpace(labels["level"]); value != "" {
		labels["detected_level"] = value
	}
}

// extractLevelFromMsg attempts to detect a log level from a raw log line,
// matching Loki's ingest-time automatic level detection behavior.
// Handles JSON ({"level":"error"}) and logfmt (level=error key=val).
// Returns ("", false) when no level can be detected.
func extractLevelFromMsg(s string) (string, bool) {
	s = strings.TrimSpace(s)
	if len(s) == 0 {
		return "", false
	}
	if s[0] == '{' {
		return extractLevelFromJSONMsg(s)
	}
	return extractLevelFromLogfmtMsg(s)
}

// levelJSONKeys is the ordered list of JSON keys the proxy checks for level,
// matching Loki's automatic level detection priority.
var levelJSONKeys = []string{"level", "severity", "lvl", "loglevel", "LEVEL", "SEVERITY"}

// levelLogfmtKeys is the ordered list of logfmt key prefixes checked for level.
var levelLogfmtKeys = []string{"level=", "severity=", "lvl=", "loglevel="}

func extractLevelFromJSONMsg(s string) (string, bool) {
	var parsed map[string]json.RawMessage
	if json.Unmarshal([]byte(s), &parsed) != nil {
		return "", false
	}
	for _, key := range levelJSONKeys {
		raw, ok := parsed[key]
		if !ok {
			continue
		}
		var level string
		if json.Unmarshal(raw, &level) != nil {
			continue
		}
		level = strings.TrimSpace(level)
		if level != "" {
			return level, true
		}
	}
	return "", false
}

// extractLevelFromLogfmtMsg scans a logfmt line for level=<value> (and
// aliases). Only reads the first matching key — does not allocate a full map.
func extractLevelFromLogfmtMsg(s string) (string, bool) {
	for _, key := range levelLogfmtKeys {
		idx := strings.Index(s, key)
		if idx < 0 {
			continue
		}
		// Require word boundary: start-of-line or preceded by whitespace.
		if idx > 0 && s[idx-1] != ' ' && s[idx-1] != '\t' {
			continue
		}
		val := s[idx+len(key):]
		if len(val) == 0 {
			continue
		}
		var level string
		if val[0] == '"' {
			end := strings.IndexByte(val[1:], '"')
			if end < 0 {
				continue
			}
			level = strings.TrimSpace(val[1 : end+1])
		} else {
			end := strings.IndexAny(val, " \t")
			if end < 0 {
				end = len(val)
			}
			level = strings.TrimSpace(val[:end])
		}
		if level != "" {
			return level, true
		}
	}
	return "", false
}

func buildEntryLabels(entry map[string]interface{}) map[string]string {
	// parseStreamLabels returns a cached read-only map — copy into a fresh map
	// before adding entry fields so the cache is not mutated.
	stream := parseStreamLabels(asString(entry["_stream"]))
	labels := make(map[string]string, len(stream))
	for k, v := range stream {
		labels[k] = v
	}
	for key, value := range entry {
		if isVLInternalField(key) || key == "_stream_id" {
			continue
		}
		if s, ok := value.(string); ok && strings.TrimSpace(s) != "" {
			labels[key] = s
		}
	}
	// VL may surface detected_level="info" from _msg even when the logfmt key
	// level=warn was also parsed. Explicit level= always wins.
	if labels["level"] != "" {
		delete(labels, "detected_level")
	}
	// Loki detects level at ingest and adds detected_level as a stream label
	// even without a parser (JSON, logfmt). Replicate on the read path: if
	// level is not yet known from VL fields or OTel, try to extract it from
	// the raw _msg string so detected_level is populated identically to Loki.
	if labels["level"] == "" && labels["detected_level"] == "" {
		if msgStr, ok := entry["_msg"].(string); ok {
			if lvl, ok := extractLevelFromMsg(msgStr); ok {
				labels["level"] = lvl
			}
		}
	}
	ensureDetectedLevel(labels)
	ensureSyntheticServiceName(labels)
	return labels
}

// detectedLabelsBufPool pools map[string]string for fillDetectedLabels callers
// that iterate the result immediately and discard it within the same loop tick.
var detectedLabelsBufPool = sync.Pool{
	New: func() interface{} {
		return make(map[string]string, 16)
	},
}

// fillDetectedLabels fills buf with stream labels for entry. Buf is cleared
// before filling; callers must not retain the map past the next fillDetectedLabels call on the same buf.
func fillDetectedLabels(entry map[string]interface{}, buf map[string]string) {
	for k := range buf {
		delete(buf, k)
	}
	stream := parseStreamLabels(asString(entry["_stream"]))
	for k, v := range stream {
		buf[k] = v
	}
	for _, key := range serviceNameSourceFields {
		if _, ok := buf[key]; ok {
			continue
		}
		if value, ok := stringifyEntryValue(entry[key]); ok && strings.TrimSpace(value) != "" {
			buf[key] = value
		}
	}
	if value, ok := stringifyEntryValue(entry["level"]); ok && strings.TrimSpace(value) != "" {
		buf["level"] = value
	}
	ensureDetectedLevel(buf)
	ensureSyntheticServiceName(buf)
}

func shouldExposeStructuredField(key string, streamLabels map[string]string, lt *LabelTranslator) bool {
	if isVLInternalField(key) || key == "_stream_id" || key == "level" {
		return false
	}
	if _, ok := streamLabels[key]; !ok {
		return true
	}
	if strings.ContainsAny(key, ".-") {
		return true
	}
	if lt == nil {
		return false
	}
	return lt.ToLoki(key) != key
}

func scanNativeStreamLabelSet(body []byte) map[string]string {
	labels := make(map[string]string)
	start := 0
	for start < len(body) {
		end := start
		for end < len(body) && body[end] != '\n' {
			end++
		}
		line := bytes.TrimSpace(body[start:end])
		if end < len(body) {
			start = end + 1
		} else {
			start = len(body)
		}
		if len(line) == 0 {
			continue
		}
		var entry vlStreamEntry
		if err := json.Unmarshal(line, &entry); err != nil {
			continue
		}
		for key, value := range parseStreamLabels(entry.Stream) {
			if strings.TrimSpace(value) == "" {
				continue
			}
			labels[key] = value
		}
	}
	return labels
}

func (p *Proxy) metadataFieldExposures(vlField string) []metadataFieldExposure {
	return p.labelTranslator.metadataFieldExposures(vlField, p.metadataFieldMode)
}

func addDetectedField(fields map[string]*detectedFieldSummary, label, parser, typ string, jsonPath []string, value string) {
	if label == "" || strings.TrimSpace(value) == "" {
		return
	}
	if shouldSuppressDetectedField(label) {
		return
	}
	summary := fields[label]
	if summary == nil {
		summary = &detectedFieldSummary{
			label:  label,
			values: map[string]struct{}{},
		}
		if len(jsonPath) > 0 {
			summary.jsonPath = append([]string(nil), jsonPath...)
		}
		fields[label] = summary
	}
	if parser != "" {
		if summary.parsers == nil {
			summary.parsers = map[string]struct{}{}
		}
		summary.parsers[parser] = struct{}{}
	}
	summary.typ = unifyDetectedType(summary.typ, typ)
	summary.values[value] = struct{}{}
	if len(summary.jsonPath) == 0 && len(jsonPath) > 0 {
		summary.jsonPath = append([]string(nil), jsonPath...)
	}
}

func canonicalLabelsKey(labels map[string]string) string {
	if len(labels) == 0 {
		return "{}"
	}
	keys := make([]string, 0, len(labels))
	for key := range labels {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	var b strings.Builder
	b.WriteByte('{')
	for i, key := range keys {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(key)
		b.WriteString(`="`)
		b.WriteString(strings.ReplaceAll(labels[key], `"`, `\"`))
		b.WriteByte('"')
	}
	b.WriteByte('}')
	return b.String()
}

func asString(value interface{}) string {
	if value == nil {
		return ""
	}
	switch v := value.(type) {
	case string:
		return v
	default:
		return strings.TrimSpace(strings.ReplaceAll(strings.TrimSpace(string(mustJSON(v))), "\n", " "))
	}
}

// scanStreamLabelNames returns the set of label names found in _stream fields
// across all NDJSON entries. It does not include serviceNameSourceFields,
// so it accurately represents what VL indexed as stream labels vs.
// auto-extracted body fields.
func scanStreamLabelNames(body []byte, lt *LabelTranslator) map[string]struct{} {
	names := map[string]struct{}{}
	start := 0
	for start < len(body) {
		end := start
		for end < len(body) && body[end] != '\n' {
			end++
		}
		line := bytes.TrimSpace(body[start:end])
		if end < len(body) {
			start = end + 1
		} else {
			start = len(body)
		}
		if len(line) == 0 {
			continue
		}
		var entry vlStreamEntry
		if err := json.Unmarshal(line, &entry); err != nil {
			continue
		}
		for key := range parseStreamLabels(entry.Stream) {
			lokiLabel := key
			if lt != nil {
				if t := lt.ToLoki(key); t != "" {
					lokiLabel = t
				}
			}
			names[lokiLabel] = struct{}{}
		}
	}
	return names
}

func scanDetectedLabelSummaries(body []byte, lt *LabelTranslator) map[string]*detectedLabelSummary {
	summaries := map[string]*detectedLabelSummary{}

	labelBuf := detectedLabelsBufPool.Get().(map[string]string)
	defer func() {
		for k := range labelBuf {
			delete(labelBuf, k)
		}
		detectedLabelsBufPool.Put(labelBuf)
	}()

	startIdx := 0
	for startIdx < len(body) {
		endIdx := startIdx
		for endIdx < len(body) && body[endIdx] != '\n' {
			endIdx++
		}
		line := bytes.TrimSpace(body[startIdx:endIdx])
		if endIdx < len(body) {
			startIdx = endIdx + 1
		} else {
			startIdx = len(body)
		}
		if len(line) == 0 {
			continue
		}

		var entry map[string]interface{}
		if err := json.Unmarshal(line, &entry); err != nil {
			continue
		}

		fillDetectedLabels(entry, labelBuf)
		labels := labelBuf
		delete(labels, "detected_level")

		for key, value := range labels {
			lokiLabel := lt.ToLoki(key)
			if lokiLabel == "" {
				continue
			}
			summary := summaries[lokiLabel]
			if summary == nil {
				summary = &detectedLabelSummary{
					label:  lokiLabel,
					values: map[string]struct{}{},
				}
				summaries[lokiLabel] = summary
			}
			summary.values[value] = struct{}{}
		}
	}
	return summaries
}

func formatDetectedLabelSummaries(summaries map[string]*detectedLabelSummary) []map[string]interface{} {
	result := make([]map[string]interface{}, 0, len(summaries))
	if summary := summaries["service_name"]; summary != nil {
		result = append(result, map[string]interface{}{
			"label":       summary.label,
			"cardinality": len(summary.values),
		})
	}

	names := make([]string, 0, len(summaries))
	for label := range summaries {
		if label == "service_name" {
			continue
		}
		names = append(names, label)
	}
	sort.Strings(names)

	for _, label := range names {
		summary := summaries[label]
		result = append(result, map[string]interface{}{
			"label":       summary.label,
			"cardinality": len(summary.values),
		})
	}
	return result
}

func (p *Proxy) serviceNameValues(ctx context.Context, query, start, end string) ([]string, error) {
	values, err := p.serviceNameValuesFromNativeFields(ctx, query, start, end)
	if err == nil && len(values) > 0 {
		return values, nil
	}

	selectorQuery := streamSelectorPrefix(query)
	if selectorQuery == "" {
		selectorQuery = defaultFieldDetectionQuery(query)
	}
	detectionQuery := defaultQuery(selectorQuery)
	logsqlQuery, err := p.translateQuery(detectionQuery)
	if err != nil {
		return nil, err
	}

	params := url.Values{}
	params.Set("query", logsqlQuery+" | sort by (_time desc)")
	if start != "" {
		params.Set("start", start)
	}
	if end != "" {
		params.Set("end", end)
	}

	resp, err := p.vlGet(ctx, "/select/logsql/streams", params)
	if err != nil {
		return p.serviceNameValuesFromDetectedLabels(ctx, detectionQuery, start, end)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= http.StatusBadRequest {
		return p.serviceNameValuesFromDetectedLabels(ctx, detectionQuery, start, end)
	}
	var vlResp struct {
		Values []struct {
			Value string `json:"value"`
			Hits  int64  `json:"hits"`
		} `json:"values"`
	}
	if err := json.Unmarshal(body, &vlResp); err != nil {
		return nil, err
	}

	seen := make(map[string]struct{}, len(vlResp.Values))
	fallbackValues := make([]string, 0, len(vlResp.Values))
	for _, item := range vlResp.Values {
		labels := parseStreamLabels(item.Value)
		serviceName := deriveServiceName(labels)
		if _, ok := seen[serviceName]; ok {
			continue
		}
		seen[serviceName] = struct{}{}
		fallbackValues = append(fallbackValues, serviceName)
	}
	sort.Strings(fallbackValues)
	if len(fallbackValues) == 0 {
		return p.serviceNameValuesFromDetectedLabels(ctx, detectionQuery, start, end)
	}
	return fallbackValues, nil
}

func (p *Proxy) serviceNameValuesFromNativeFields(ctx context.Context, query, start, end string) ([]string, error) {
	values := make([]string, 0, 16)
	seen := make(map[string]struct{}, 16)
	var lastErr error
	params, err := p.metadataQueryParams(ctx, relaxedFieldDetectionQuery(query), start, end, "", "")
	if err != nil {
		return nil, err
	}
	appendFieldValues := func(fieldValues []string) {
		for _, value := range fieldValues {
			value = strings.TrimSpace(value)
			if value == "" {
				continue
			}
			if _, ok := seen[value]; ok {
				continue
			}
			seen[value] = struct{}{}
			values = append(values, value)
		}
	}

	phase1Succeeded := false
	if p.supportsStreamMetadataEndpoints() {
		streamFields, err := p.fetchVLFieldNames(ctx, "/select/logsql/stream_field_names", params)
		if err == nil {
			phase1Succeeded = true
			streamFields = appendUniqueStrings(streamFields, p.snapshotDeclaredLabelFields()...)
			available := make(map[string]struct{}, len(streamFields))
			for _, field := range streamFields {
				available[field] = struct{}{}
			}
			for _, field := range serviceNameSourceFields {
				if _, ok := available[field]; !ok {
					continue
				}
				queryParams := cloneURLValues(params)
				queryParams.Set("field", field)
				fieldValues, fieldErr := p.fetchVLFieldValues(ctx, "/select/logsql/stream_field_values", queryParams)
				if fieldErr != nil && shouldFallbackToGenericMetadata(fieldErr) {
					fieldValues, fieldErr = p.fetchVLFieldValues(ctx, "/select/logsql/field_values", queryParams)
				}
				if fieldErr != nil {
					lastErr = fieldErr
					continue
				}
				appendFieldValues(fieldValues)
			}
		} else if !shouldFallbackToGenericMetadata(err) {
			lastErr = err
		}
	}

	// Phase 2: supplement with document-level fields (e.g. OTel service.name).
	// When Phase 1 succeeded via stream labels, restrict Phase 2 to dotted-name
	// fields only: stream labels cannot contain dots, so dotted names must come
	// from structured document fields. Skipping plain underscore names prevents
	// logfmt-parsed document fields like "job=sync-users" from appearing as
	// service names when stream labels already provide the correct service list.
	fieldNames, err := p.fetchVLFieldNames(ctx, "/select/logsql/field_names", params)
	if err == nil {
		available := make(map[string]struct{}, len(fieldNames))
		for _, field := range fieldNames {
			available[field] = struct{}{}
		}
		for _, field := range serviceNameSourceFields {
			if phase1Succeeded && !strings.ContainsAny(field, ".-") {
				continue
			}
			if _, ok := available[field]; !ok {
				continue
			}
			queryParams := cloneURLValues(params)
			queryParams.Set("field", field)
			fieldValues, fieldErr := p.fetchVLFieldValues(ctx, "/select/logsql/field_values", queryParams)
			if fieldErr != nil {
				lastErr = fieldErr
				continue
			}
			appendFieldValues(fieldValues)
		}
	} else if lastErr == nil {
		lastErr = err
	}

	values = uniqueSortedNonEmptyStrings(values)
	if len(values) > 0 {
		return values, nil
	}
	return nil, lastErr
}

func uniqueSortedNonEmptyStrings(values []string) []string {
	out := make([]string, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}

func streamSelectorPrefix(query string) string {
	query = strings.TrimSpace(query)
	if query == "" {
		return ""
	}
	inQuote := byte(0)
	escape := false
	for i := 0; i < len(query); i++ {
		ch := query[i]
		if escape {
			escape = false
			continue
		}
		if inQuote != 0 {
			if ch == '\\' && inQuote == '"' {
				escape = true
				continue
			}
			if ch == inQuote {
				inQuote = 0
			}
			continue
		}
		if ch == '"' || ch == '`' {
			inQuote = ch
			continue
		}
		if ch == '|' {
			return strings.TrimSpace(query[:i])
		}
	}
	return query
}

func (p *Proxy) serviceNameValuesFromDetectedLabels(ctx context.Context, query, start, end string) ([]string, error) {
	_, summaries, err := p.detectLabels(ctx, query, start, end, detectedFieldsSampleLimit)
	if err != nil {
		return nil, err
	}
	summary := summaries["service_name"]
	if summary == nil {
		return []string{}, nil
	}
	values := make([]string, 0, len(summary.values))
	for value := range summary.values {
		if strings.TrimSpace(value) == "" {
			continue
		}
		values = append(values, value)
	}
	sort.Strings(values)
	return values, nil
}

func splitTargetLabels(targetLabels string) []string {
	parts := strings.Split(targetLabels, ",")
	labels := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		labels = append(labels, part)
	}
	return labels
}

func inferPrimaryTargetLabel(query string) string {
	query = strings.TrimSpace(query)
	if !strings.HasPrefix(query, "{") {
		return ""
	}

	inQuote := byte(0)
	escape := false
	braceDepth := 0
	end := -1
	for i := 0; i < len(query); i++ {
		ch := query[i]
		if escape {
			escape = false
			continue
		}
		if inQuote != 0 {
			if ch == '\\' && inQuote == '"' {
				escape = true
				continue
			}
			if ch == inQuote {
				inQuote = 0
			}
			continue
		}
		switch ch {
		case '"', '`':
			inQuote = ch
		case '{':
			braceDepth++
		case '}':
			braceDepth--
			if braceDepth == 0 {
				end = i
				i = len(query)
			}
		}
	}
	if end <= 1 {
		return ""
	}

	content := query[1:end]
	inQuote = 0
	escape = false
	start := 0
	firstMatcher := ""
	for i := 0; i < len(content); i++ {
		ch := content[i]
		if escape {
			escape = false
			continue
		}
		if inQuote != 0 {
			if ch == '\\' && inQuote == '"' {
				escape = true
				continue
			}
			if ch == inQuote {
				inQuote = 0
			}
			continue
		}
		if ch == '"' || ch == '`' {
			inQuote = ch
			continue
		}
		if ch != ',' {
			continue
		}
		part := strings.TrimSpace(content[start:i])
		if part != "" {
			firstMatcher = part
			break
		}
		start = i + 1
	}
	if firstMatcher == "" {
		firstMatcher = strings.TrimSpace(content[start:])
	}
	if firstMatcher == "" {
		return ""
	}

	for _, op := range []string{"!~", "=~", "!=", "="} {
		if idx := strings.Index(firstMatcher, op); idx > 0 {
			return strings.TrimSpace(firstMatcher[:idx])
		}
	}
	return ""
}

func usesDerivedVolumeLabels(targetLabels string) bool {
	for _, label := range splitTargetLabels(targetLabels) {
		if label == "service_name" || label == "detected_level" {
			return true
		}
	}
	return false
}

func parseVolumeBoundary(ts string) (time.Time, bool) {
	ts = strings.TrimSpace(ts)
	if ts == "" {
		return time.Time{}, false
	}
	if ts == "now" {
		return time.Now().UTC(), true
	}
	if strings.HasPrefix(ts, "now-") || strings.HasPrefix(ts, "now+") {
		sign := ts[3:4]
		d, err := time.ParseDuration(ts[4:])
		if err == nil {
			if sign == "-" {
				return time.Now().UTC().Add(-d), true
			}
			return time.Now().UTC().Add(d), true
		}
	}
	normalized := formatVLTimestamp(ts)
	if n, err := strconv.ParseInt(normalized, 10, 64); err == nil {
		return time.Unix(0, n).UTC(), true
	}
	return time.Time{}, false
}

func parseVolumeStep(step string) (time.Duration, bool) {
	step = strings.TrimSpace(step)
	if step == "" {
		return 0, false
	}
	d, err := time.ParseDuration(formatVLStep(step))
	if err != nil || d <= 0 {
		return 0, false
	}
	return d, true
}

func parseEntryTime(value interface{}) (time.Time, bool) {
	switch v := value.(type) {
	case string:
		if parsed, err := time.Parse(time.RFC3339Nano, v); err == nil {
			return parsed.UTC(), true
		}
		if parsed, err := time.Parse(time.RFC3339, v); err == nil {
			return parsed.UTC(), true
		}
		if n, err := strconv.ParseInt(strings.TrimSpace(v), 10, 64); err == nil {
			return time.Unix(0, n).UTC(), true
		}
	case float64:
		return time.Unix(0, int64(v)).UTC(), true
	}
	return time.Time{}, false
}

func buildVolumeMetric(labels map[string]string, targetLabels []string) map[string]string {
	metric := make(map[string]string, len(targetLabels))
	for _, target := range targetLabels {
		metric[target] = labels[target]
	}
	return metric
}

func volumeMetricKey(targetLabels []string, metric map[string]string) string {
	if len(targetLabels) == 0 {
		return "{}"
	}
	var b strings.Builder
	b.WriteByte('{')
	for i, target := range targetLabels {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(target)
		b.WriteString(`="`)
		b.WriteString(strings.ReplaceAll(metric[target], `"`, `\"`))
		b.WriteByte('"')
	}
	b.WriteByte('}')
	return b.String()
}

func (p *Proxy) volumeByDerivedLabels(ctx context.Context, query, start, end, targetLabels, step string) (map[string]interface{}, error) {
	logsqlQuery, err := p.translateQuery(defaultQuery(query))
	if err != nil {
		return nil, err
	}

	// When detected_level is a target, VL must extract level from _msg because
	// level is not a VL stream field for JSON/logfmt logs — it lives inside _msg.
	// Chaining unpack_json then unpack_logfmt covers both formats: unpack_json
	// extracts level from JSON objects; unpack_logfmt handles key=value lines.
	// Logs that are neither JSON nor logfmt leave level unset (graceful no-op).
	targets := splitTargetLabels(targetLabels)
	needsLevelUnpack := false
	for _, t := range targets {
		if t == "detected_level" {
			needsLevelUnpack = true
			break
		}
	}
	if needsLevelUnpack && !strings.Contains(logsqlQuery, "unpack_json") && !strings.Contains(logsqlQuery, "unpack_logfmt") {
		logsqlQuery = logsqlQuery + " | unpack_json from _msg | unpack_logfmt from _msg"
	}

	params := url.Values{}
	params.Set("query", logsqlQuery)
	if start != "" {
		params.Set("start", formatVLTimestamp(start))
	}
	if end != "" {
		params.Set("end", formatVLTimestamp(end))
	}
	if strings.TrimSpace(step) != "" {
		params.Set("step", formatVLStep(step))
	} else {
		// VictoriaLogs hits endpoint requires step on v1.49+.
		params.Set("step", "1h")
	}
	sourceFields := p.derivedVolumeSourceFields(targets)
	if len(sourceFields) > 0 {
		for _, field := range sourceFields {
			params.Add("field", field)
		}
	}

	resp, err := p.vlGet(ctx, "/select/logsql/hits", params)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= http.StatusBadRequest {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("derived volume hits request failed: status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	body, _ := io.ReadAll(resp.Body)
	hits := parseHits(body)
	matrixMode := strings.TrimSpace(step) != ""

	type seriesData struct {
		metric map[string]string
		total  int64
		bucket map[int64]int64
	}

	startTime, hasStart := parseVolumeBoundary(start)
	endTime, hasEnd := parseVolumeBoundary(end)
	stepDur, hasStep := parseVolumeStep(step)
	series := make(map[string]*seriesData)
	order := make([]string, 0)

	for _, hit := range hits.Hits {
		translated := p.translateVolumeMetric(hit.Fields)
		ensureDetectedLevel(translated)
		ensureSyntheticServiceName(translated)
		metric := buildVolumeMetric(translated, targets)
		key := volumeMetricKey(targets, metric)

		item := series[key]
		if item == nil {
			item = &seriesData{
				metric: metric,
				bucket: map[int64]int64{},
			}
			series[key] = item
			order = append(order, key)
		}

		if !matrixMode {
			for _, v := range hit.Values {
				item.total += int64(v)
			}
			continue
		}

		for i, ts := range hit.Timestamps {
			value := 0
			if i < len(hit.Values) {
				value = hit.Values[i]
			}
			if value < 0 {
				continue
			}
			entryNs, ok := parseLokiTimeToUnixNano(strings.TrimSpace(string(ts)))
			if !ok {
				continue
			}
			entryTime := time.Unix(0, entryNs).UTC()
			if hasStart && entryTime.Before(startTime) {
				continue
			}
			if hasEnd && entryTime.After(endTime) {
				continue
			}

			bucketTime := entryTime.Unix()
			if hasStep {
				stepSeconds := int64(stepDur.Seconds())
				if stepSeconds <= 0 {
					continue
				}
				if hasStart {
					offset := entryTime.Sub(startTime)
					if offset < 0 {
						continue
					}
					bucketTime = startTime.Add((offset / stepDur) * stepDur).Unix()
				} else {
					bucketTime = (bucketTime / stepSeconds) * stepSeconds
				}
			}
			item.bucket[bucketTime] += int64(value)
		}
	}

	sort.Slice(order, func(i, j int) bool {
		return order[i] < order[j]
	})

	if !matrixMode {
		nowTS := float64(time.Now().Unix())
		result := make([]map[string]interface{}, 0, len(order))
		for _, key := range order {
			item := series[key]
			result = append(result, map[string]interface{}{
				"metric": item.metric,
				"value":  []interface{}{nowTS, strconv.FormatInt(item.total, 10)},
			})
		}
		return map[string]interface{}{
			"status": "success",
			"data": map[string]interface{}{
				"resultType": "vector",
				"result":     result,
			},
		}, nil
	}

	var bucketKeys []int64
	if hasStart && hasEnd && hasStep {
		for ts := startTime.Unix(); ts <= endTime.Unix(); ts += int64(stepDur.Seconds()) {
			bucketKeys = append(bucketKeys, ts)
		}
	}

	result := make([]map[string]interface{}, 0, len(order))
	for _, key := range order {
		item := series[key]
		values := make([][]interface{}, 0)
		if len(bucketKeys) > 0 {
			values = make([][]interface{}, 0, len(bucketKeys))
			for _, ts := range bucketKeys {
				values = append(values, []interface{}{float64(ts), strconv.FormatInt(item.bucket[ts], 10)})
			}
		} else {
			timestamps := make([]int64, 0, len(item.bucket))
			for ts := range item.bucket {
				timestamps = append(timestamps, ts)
			}
			sort.Slice(timestamps, func(i, j int) bool { return timestamps[i] < timestamps[j] })
			for _, ts := range timestamps {
				values = append(values, []interface{}{float64(ts), strconv.FormatInt(item.bucket[ts], 10)})
			}
		}
		result = append(result, map[string]interface{}{
			"metric": item.metric,
			"values": values,
		})
	}

	return map[string]interface{}{
		"status": "success",
		"data": map[string]interface{}{
			"resultType": "matrix",
			"result":     result,
		},
	}, nil
}

func (p *Proxy) derivedVolumeSourceFields(targets []string) []string {
	fields := make([]string, 0, len(targets)+8)
	addField := func(name string) {
		name = strings.TrimSpace(name)
		if name == "" {
			return
		}
		fields = appendUniqueStrings(fields, name)
		if p != nil && p.labelTranslator != nil {
			fields = appendUniqueStrings(fields, strings.TrimSpace(p.labelTranslator.ToVL(name)))
		}
	}

	for _, target := range targets {
		switch strings.TrimSpace(target) {
		case "service_name":
			for _, source := range serviceNameSourceFields {
				addField(source)
			}
		case "detected_level":
			addField("detected_level")
			addField("level")
		default:
			addField(target)
		}
	}

	return fields
}

func defaultQuery(query string) string {
	query = strings.TrimSpace(query)
	if query == "" {
		return "*"
	}
	return normalizeBareSelectorQuery(query)
}

func defaultFieldDetectionQuery(query string) string {
	return defaultQuery(stripFieldDetectionStages(query))
}

func relaxedFieldDetectionQuery(query string) string {
	return defaultQuery(stripFieldComparisonStages(stripFieldDetectionStages(query)))
}

func fieldDetectionQueryCandidates(query string) []string {
	primary := defaultFieldDetectionQuery(query)
	relaxed := relaxedFieldDetectionQuery(query)
	if relaxed == "" || relaxed == primary {
		return []string{primary}
	}
	return []string{primary, relaxed}
}

func metadataQueryCandidates(query string) []string {
	return []string{relaxedFieldDetectionQuery(query)}
}

func normalizeBareSelectorQuery(query string) string {
	query = strings.TrimSpace(query)
	if query == "" || query == "*" || strings.HasPrefix(query, "{") {
		return query
	}
	if strings.Contains(query, "|") || strings.Contains(query, "(") || strings.Contains(query, ")") || strings.Contains(query, "[") || strings.Contains(query, "]") {
		return query
	}
	if strings.Contains(query, "=~") || strings.Contains(query, "!~") || strings.Contains(query, "!=") || strings.Contains(query, "=") {
		return "{" + query + "}"
	}
	return query
}

func stripFieldDetectionStages(query string) string {
	query = strings.TrimSpace(query)
	if query == "" {
		return query
	}

	dropStage := regexp.MustCompile(`\|\s*drop\s+__error__(\s*,\s*__error_details__)?\s*`)
	parserStage := regexp.MustCompile(`\|\s*(json|logfmt|unpack)(\s+[^|]+)?`)
	unwrapStage := regexp.MustCompile(`\|\s*unwrap(?:\s+[^|]+)?`)

	query = dropStage.ReplaceAllString(query, " ")
	query = parserStage.ReplaceAllString(query, " ")
	query = unwrapStage.ReplaceAllString(query, " ")
	for strings.Contains(query, "  ") {
		query = strings.ReplaceAll(query, "  ", " ")
	}
	return strings.TrimSpace(query)
}

func stripFieldComparisonStages(query string) string {
	query = strings.TrimSpace(query)
	if query == "" {
		return query
	}

	fieldComparisonStage := regexp.MustCompile(`\|\s*[A-Za-z_][A-Za-z0-9_.-]*\s*(?:=~|!~|!=|=|>=|<=|>|<)\s*[^|]+`)
	query = fieldComparisonStage.ReplaceAllString(query, " ")
	for strings.Contains(query, "  ") {
		query = strings.ReplaceAll(query, "  ", " ")
	}
	return strings.TrimSpace(query)
}

func inferDetectedType(value interface{}) string {
	switch v := value.(type) {
	case bool:
		return "boolean"
	case float64:
		if v == float64(int64(v)) {
			return "int"
		}
		return "float"
	case string:
		if _, err := time.ParseDuration(v); err == nil {
			return "duration"
		}
		if _, err := strconv.ParseInt(v, 10, 64); err == nil {
			return "int"
		}
		if _, err := strconv.ParseFloat(v, 64); err == nil {
			return "float"
		}
		return "string"
	default:
		return "string"
	}
}

func unifyDetectedType(current, next string) string {
	if current == "" || current == next {
		return next
	}
	if current == "float" || next == "float" {
		return "float"
	}
	return "string"
}

func parseLogfmtFields(line string) map[string]string {
	fields := make(map[string]string)
	var token strings.Builder
	tokens := make([]string, 0, 8)
	inQuote := false

	for _, r := range line {
		switch {
		case r == '"':
			inQuote = !inQuote
			token.WriteRune(r)
		case r == ' ' && !inQuote:
			if token.Len() > 0 {
				tokens = append(tokens, token.String())
				token.Reset()
			}
		default:
			token.WriteRune(r)
		}
	}
	if token.Len() > 0 {
		tokens = append(tokens, token.String())
	}

	for _, token := range tokens {
		parts := strings.SplitN(token, "=", 2)
		if len(parts) != 2 {
			continue
		}
		key := strings.TrimSpace(parts[0])
		value := strings.Trim(strings.TrimSpace(parts[1]), `"`)
		if key == "" {
			continue
		}
		fields[key] = value
	}
	return fields
}

type vlFieldNamesResponse struct {
	Values []struct {
		Value string `json:"value"`
		Hits  int64  `json:"hits"`
	} `json:"values"`
}

type vlFieldValuesResponse struct {
	Values []struct {
		Value string `json:"value"`
		Hits  int64  `json:"hits"`
	} `json:"values"`
}

type vlStreamsResponse struct {
	Values []struct {
		Value string `json:"value"`
		Hits  int64  `json:"hits"`
	} `json:"values"`
}

func (p *Proxy) detectFields(ctx context.Context, query, start, end string, lineLimit int) ([]map[string]interface{}, map[string][]string, error) {
	if lineLimit > maxDetectedScanLines {
		lineLimit = maxDetectedScanLines
	}
	if cachedFields, cachedValues, ok := p.getCachedDetectedFields(ctx, query, start, end, lineLimit); ok {
		return cachedFields, cachedValues, nil
	}
	nativeFields, err := p.detectNativeFields(ctx, query, start, end)
	if err != nil {
		nativeFields = nil
	}
	scanLimit := lineLimit
	if len(nativeFields) > 0 && scanLimit > detectedFieldsSampleLimit {
		scanLimit = detectedFieldsSampleLimit
	}
	candidates := fieldDetectionQueryCandidates(query)
	hadScanFailure := false
	var lastErr error
	for _, candidate := range candidates {
		logsqlQuery, err := p.translateQuery(candidate)
		if err != nil {
			lastErr = err
			hadScanFailure = true
			continue
		}

		params := url.Values{}
		params.Set("query", logsqlQuery+" | sort by (_time desc)")
		params.Set("limit", strconv.Itoa(scanLimit))
		if start != "" {
			params.Set("start", formatVLTimestamp(start))
		}
		if end != "" {
			params.Set("end", formatVLTimestamp(end))
		}

		resp, err := p.vlPost(ctx, "/select/logsql/query", params)
		if err != nil {
			lastErr = err
			hadScanFailure = true
			continue
		}

		body, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if resp.StatusCode >= http.StatusInternalServerError {
			msg := strings.TrimSpace(string(body))
			if msg == "" {
				msg = fmt.Sprintf("VL backend returned %d", resp.StatusCode)
			}
			lastErr = fmt.Errorf("%s", msg)
			hadScanFailure = true
			continue
		}
		if resp.StatusCode >= http.StatusBadRequest {
			msg := strings.TrimSpace(string(body))
			if msg == "" {
				msg = fmt.Sprintf("VL backend returned %d", resp.StatusCode)
			}
			return nil, nil, fmt.Errorf("%s", msg)
		}

		fieldList, fieldValues := p.detectFieldsFromBody(body)
		if len(fieldList) > 0 {
			fieldList, fieldValues = mergeNativeDetectedFields(fieldList, fieldValues, filterNativeDetectedFields(nativeFields, scanNativeStreamLabelSet(body), p.labelTranslator))
			p.setCachedDetectedFields(ctx, query, start, end, lineLimit, fieldList, fieldValues)
			return fieldList, fieldValues, nil
		}
		if len(candidates) > 1 && !hadScanFailure {
			emptyFields := []map[string]interface{}{}
			emptyValues := map[string][]string{}
			p.setCachedDetectedFields(ctx, query, start, end, lineLimit, emptyFields, emptyValues)
			return emptyFields, emptyValues, nil
		}
		break
	}

	if len(nativeFields) > 0 {
		if nativeFieldFilterNeedsStreamLabels(nativeFields, p.labelTranslator) {
			streamLabels, err := p.fetchNativeStreamLabelSet(ctx, query, start, end)
			if err == nil {
				nativeFields = filterNativeDetectedFields(nativeFields, streamLabels, p.labelTranslator)
			}
		}
		fieldList, fieldValues := mergeNativeDetectedFields(nil, nil, nativeFields)
		p.setCachedDetectedFields(ctx, query, start, end, lineLimit, fieldList, fieldValues)
		return fieldList, fieldValues, nil
	}
	return nil, nil, lastErr
}

func (p *Proxy) detectFieldsFromBody(body []byte) ([]map[string]interface{}, map[string][]string) {
	fieldList, fieldValues, _ := p.detectFieldSummaries(body)
	return fieldList, fieldValues
}

func (p *Proxy) detectFieldSummaries(body []byte) ([]map[string]interface{}, map[string][]string, map[string]*detectedFieldSummary) {
	// Use stream-label-only set so that VL auto-extracted body fields sharing
	// a name with a serviceNameSourceField (e.g. "job") are not suppressed.
	labelNames := scanStreamLabelNames(body, p.labelTranslator)
	fields := make(map[string]*detectedFieldSummary)

	// Track OTel presence across ALL entries in the batch.
	// service_name suppression is decided AFTER all entries are processed,
	// because the fields map accumulates across entries and a per-entry
	// delete would incorrectly remove aliases added by earlier OTel entries.
	anyOTelWithServiceName := false

	streamLabelBuf := detectedLabelsBufPool.Get().(map[string]string)
	defer func() {
		for k := range streamLabelBuf {
			delete(streamLabelBuf, k)
		}
		detectedLabelsBufPool.Put(streamLabelBuf)
	}()

	startIdx := 0
	for startIdx < len(body) {
		endIdx := startIdx
		for endIdx < len(body) && body[endIdx] != '\n' {
			endIdx++
		}
		line := bytes.TrimSpace(body[startIdx:endIdx])
		if endIdx < len(body) {
			startIdx = endIdx + 1
		} else {
			startIdx = len(body)
		}
		if len(line) == 0 {
			continue
		}

		var entry map[string]interface{}
		if err := json.Unmarshal(line, &entry); err != nil {
			continue
		}
		fillDetectedLabels(entry, streamLabelBuf)
		streamLabels := streamLabelBuf
		if detectedLevel := strings.TrimSpace(streamLabels["detected_level"]); detectedLevel != "" {
			addDetectedField(fields, "detected_level", "", "string", nil, detectedLevel)
		}

		// Detect whether this entry is OTel data
		if isOTelData(entry) {
			if _, ok := streamLabels["service.name"]; ok {
				anyOTelWithServiceName = true
			}
		}

		// Use only the actual _stream labels for field-exposure checks so that
		// VL auto-extracted body fields (e.g. "job", "container") that share a
		// name with a serviceNameSourceField are not incorrectly suppressed.
		rawStreamLabels := parseStreamLabels(asString(entry["_stream"]))

		for key, value := range entry {
			// Skip indexed labels that should not appear as detected fields
			if key == "app" || key == "cluster" || key == "namespace" {
				continue
			}

			if !shouldExposeStructuredField(key, rawStreamLabels, p.labelTranslator) {
				continue
			}
			stringValue, ok := stringifyEntryValue(value)
			if !ok {
				continue
			}
			// For stream labels (structured metadata), apply label translation for OTel compatibility.
			// This converts dotted names like "service.name" to "service_name" for Loki API compatibility.
			for _, exposure := range p.metadataFieldExposures(key) {
				if _, conflict := labelNames[exposure.name]; conflict && !exposure.isAlias {
					continue
				}
				addDetectedField(fields, exposure.name, "", inferDetectedType(value), nil, stringValue)
			}
		}

		msg, _ := entry["_msg"].(string)
		if msg == "" {
			continue
		}

		var parsedJSON map[string]interface{}
		if json.Unmarshal([]byte(msg), &parsedJSON) == nil {
			for key, value := range parsedJSON {
				if key == "" {
					continue
				}
				// Skip nested objects and arrays — they are not filterable scalar fields
				// and break Grafana Drilldown's field breakdown view (e.g. service={name:...}).
				switch value.(type) {
				case map[string]interface{}, []interface{}:
					continue
				}
				// For parsed message fields, use field name as-is without applying label translation.
				// Label translation is only for stream labels (indexed labels), not message content fields.
				// Skip indexed label names that should never appear in detected_fields.
				if shouldSuppressDetectedField(key) {
					continue
				}
				if _, conflict := labelNames[key]; conflict {
					continue
				}
				addDetectedField(fields, key, "json", inferDetectedType(value), []string{key}, formatDetectedValue(value))
			}
		}

		for key, value := range parseLogfmtFields(msg) {
			if key == "msg" {
				continue
			}
			if key == "level" {
				addDetectedField(fields, "detected_level", "", "string", nil, value)
				continue
			}
			// For parsed message fields, use field name as-is without applying label translation.
			// Label translation is only for stream labels (indexed labels), not message content fields.
			// Skip indexed label names that should never appear in detected_fields.
			if shouldSuppressDetectedField(key) {
				continue
			}
			if _, conflict := labelNames[key]; conflict {
				continue
			}
			addDetectedField(fields, key, "logfmt", inferDetectedType(value), nil, value)
		}
	}

	// Post-scan OTel alias exposure: service_name is unconditionally suppressed
	// by addDetectedField via suppressedDetectedFieldNames. For OTel data with
	// real service.name in stream labels, explicitly add service_name so
	// Drilldown and Explore can use both dotted and underscore forms.
	//
	// In hybrid mode: both service.name and service_name are exposed.
	// In translated mode: only service_name is exposed (no service.name in fields).
	// In native mode: only service.name is exposed (no service_name needed).
	if anyOTelWithServiceName {
		// Find a source for the alias values: prefer service.name, fall back to
		// any field that contributed OTel service name data.
		var source *detectedFieldSummary
		if serviceDot, ok := fields["service.name"]; ok {
			source = serviceDot
		} else {
			// In translated-only mode, service.name is not in fields. Use the
			// values that were collected from stream labels during scan.
			source = &detectedFieldSummary{
				label: "service_name",
				typ:   "string",
			}
		}
		fields["service_name"] = &detectedFieldSummary{
			label:       "service_name",
			typ:         source.typ,
			values:      source.values,
			cardinality: source.cardinality,
		}
	}

	// Post-process: fields detected only via VL top-level auto-extracted path
	// (no parser assigned) and NOT true stream labels get parsers:["json"].
	//
	// Why: VictoriaLogs auto-extracts JSON body fields to top-level indexed
	// fields and replaces _msg with the inner message string. The proxy scans
	// VL NDJSON output and sees plain-text _msg, so JSON-origin fields are not
	// detected via _msg parsing. Loki sees the original JSON body and marks them
	// parsers:["json"]. We match that so Grafana Drilldown uses the correct
	// detected_field/values endpoint rather than label_values (stream-label only).
	//
	// Dotted-name fields (e.g. service.name, k8s.pod.name) are excluded: those
	// are OTel structured metadata, not JSON body fields, and must keep parsers:null.
	for _, summary := range fields {
		if len(summary.parsers) == 0 && !strings.ContainsAny(summary.label, ".") {
			if _, isStreamLabel := labelNames[summary.label]; !isStreamLabel {
				if summary.parsers == nil {
					summary.parsers = map[string]struct{}{}
				}
				summary.parsers["json"] = struct{}{}
				if len(summary.jsonPath) == 0 {
					summary.jsonPath = []string{summary.label}
				}
			}
		}
	}

	names := make([]string, 0, len(fields))
	for label, summary := range fields {
		summary.cardinality = len(summary.values)
		names = append(names, label)
	}
	sort.Strings(names)

	fieldList := make([]map[string]interface{}, 0, len(names))
	fieldValues := make(map[string][]string, len(names))
	for _, label := range names {
		summary := fields[label]
		var parsers []string
		if len(summary.parsers) > 0 {
			parsers = make([]string, 0, len(summary.parsers))
			for parser := range summary.parsers {
				parsers = append(parsers, parser)
			}
			sort.Strings(parsers)
		}

		values := make([]string, 0, len(summary.values))
		for value := range summary.values {
			values = append(values, value)
		}
		sort.Strings(values)
		fieldValues[label] = values

		field := map[string]interface{}{
			"label":       label,
			"type":        summary.typ,
			"cardinality": summary.cardinality,
			"parsers":     parsers,
		}
		if len(summary.jsonPath) > 0 {
			field["jsonPath"] = summary.jsonPath
		}
		fieldList = append(fieldList, field)
	}

	return fieldList, fieldValues, fields
}

func (p *Proxy) detectLabels(ctx context.Context, query, start, end string, lineLimit int) ([]map[string]interface{}, map[string]*detectedLabelSummary, error) {
	if lineLimit > maxDetectedScanLines {
		lineLimit = maxDetectedScanLines
	}
	if cachedLabels, cachedSummaries, ok := p.getCachedDetectedLabels(ctx, query, start, end, lineLimit); ok {
		return cachedLabels, cachedSummaries, nil
	}
	summaries, err := p.detectNativeLabels(ctx, query, start, end)
	if err != nil {
		summaries, err = p.detectScannedLabels(ctx, query, start, end, lineLimit)
		if err != nil {
			return nil, nil, err
		}
	} else if needsDetectedLabelScanSupplement(summaries) {
		scanned, scanErr := p.detectScannedLabels(ctx, query, start, end, lineLimit)
		if scanErr == nil {
			mergeDetectedLabelSupplements(summaries, scanned)
		}
	}
	labels := formatDetectedLabelSummaries(summaries)
	p.setCachedDetectedLabels(ctx, query, start, end, lineLimit, labels, summaries)
	return labels, summaries, nil
}

func needsDetectedLabelScanSupplement(summaries map[string]*detectedLabelSummary) bool {
	if len(summaries) == 0 {
		return true
	}
	for _, label := range []string{"level", "service_name"} {
		summary := summaries[label]
		if summary == nil {
			return true
		}
		if label == "service_name" && detectedServiceNameSummaryNeedsReplacement(summary) {
			return true
		}
	}
	return false
}

func mergeDetectedLabelSupplements(dst, scanned map[string]*detectedLabelSummary) {
	for _, label := range []string{"level", "service_name"} {
		summary := scanned[label]
		if summary == nil {
			continue
		}
		existing := dst[label]
		if existing == nil {
			dst[label] = summary
			continue
		}
		if label == "service_name" && detectedServiceNameSummaryNeedsReplacement(existing) {
			dst[label] = summary
		}
	}
}

func detectedServiceNameSummaryNeedsReplacement(summary *detectedLabelSummary) bool {
	if summary == nil || len(summary.values) == 0 {
		return true
	}
	if len(summary.values) == 1 {
		_, onlyUnknown := summary.values[unknownServiceName]
		return onlyUnknown
	}
	return false
}

func (p *Proxy) detectNativeFields(ctx context.Context, query, start, end string) (map[string]*detectedFieldSummary, error) {
	fieldNames, err := p.fetchNativeFieldNames(ctx, query, start, end)
	if err != nil {
		return nil, err
	}
	out := make(map[string]*detectedFieldSummary, len(fieldNames))
	for _, field := range fieldNames {
		for _, exposure := range p.metadataFieldExposures(field) {
			if shouldSuppressDetectedField(exposure.name) {
				continue
			}
			out[exposure.name] = &detectedFieldSummary{
				label:       exposure.name,
				typ:         "string",
				values:      map[string]struct{}{},
				cardinality: 1,
			}
		}
	}
	return out, nil
}

func mergeNativeDetectedFields(scanned []map[string]interface{}, scannedValues map[string][]string, native map[string]*detectedFieldSummary) ([]map[string]interface{}, map[string][]string) {
	if len(native) == 0 {
		return scanned, scannedValues
	}
	outValues := make(map[string][]string, len(scannedValues))
	for k, v := range scannedValues {
		outValues[k] = append([]string(nil), v...)
	}
	merged := make(map[string]map[string]interface{}, len(scanned)+len(native))
	for _, item := range scanned {
		label, _ := item["label"].(string)
		if label == "" {
			continue
		}
		copied := make(map[string]interface{}, len(item))
		for k, v := range item {
			copied[k] = v
		}
		merged[label] = copied
	}
	for label, summary := range native {
		if shouldSuppressDetectedField(label) {
			continue
		}
		if existing, ok := merged[label]; ok {
			if card, ok := existing["cardinality"].(int); ok && card > 0 {
				continue
			}
			if card, ok := existing["cardinality"].(float64); ok && card > 0 {
				continue
			}
		}
		merged[label] = map[string]interface{}{
			"label":       summary.label,
			"type":        summary.typ,
			"cardinality": summary.cardinality,
			"parsers":     []string{},
		}
		if _, ok := outValues[label]; !ok {
			outValues[label] = []string{}
		}
	}
	labels := make([]string, 0, len(merged))
	for label := range merged {
		labels = append(labels, label)
	}
	sort.Strings(labels)
	out := make([]map[string]interface{}, 0, len(labels))
	for _, label := range labels {
		out = append(out, merged[label])
	}
	return out, outValues
}

func (p *Proxy) fetchNativeFieldNamesForCandidate(ctx context.Context, candidate, start, end string) ([]string, error) {
	logsqlQuery, err := p.translateQuery(candidate)
	if err != nil {
		return nil, err
	}
	params := url.Values{}
	params.Set("query", logsqlQuery)
	if start != "" {
		params.Set("start", start)
	}
	if end != "" {
		params.Set("end", end)
	}
	body, err := p.vlGetCoalesced(ctx, p.nativeCoalescerKey("native_fields", ctx, params), "/select/logsql/field_names", params)
	if err != nil {
		return nil, err
	}
	var resp vlFieldNamesResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, err
	}
	out := make([]string, 0, len(resp.Values))
	for _, item := range resp.Values {
		out = append(out, item.Value)
	}
	return out, nil
}

func (p *Proxy) fetchNativeFieldNames(ctx context.Context, query, start, end string) ([]string, error) {
	var lastErr error
	for _, candidate := range fieldDetectionQueryCandidates(query) {
		fields, err := p.fetchNativeFieldNamesForCandidate(ctx, candidate, start, end)
		if err != nil {
			lastErr = err
			continue
		}
		return fields, nil
	}
	return nil, lastErr
}

func (p *Proxy) fetchNativeStreamLabelSet(ctx context.Context, query, start, end string) (map[string]string, error) {
	parsed, err := p.fetchNativeStreams(ctx, query, start, end)
	if err != nil {
		return nil, err
	}
	labels := make(map[string]string)
	for _, item := range parsed.Values {
		for key, value := range parseStreamLabels(item.Value) {
			if strings.TrimSpace(value) == "" {
				continue
			}
			labels[key] = value
		}
	}
	return labels, nil
}

func (p *Proxy) fetchNativeFieldValues(ctx context.Context, query, start, end, field string, limit int) ([]string, error) {
	var lastErr error
	candidates := fieldDetectionQueryCandidates(query)
	for i, candidate := range candidates {
		logsqlQuery, err := p.translateQuery(candidate)
		if err != nil {
			lastErr = err
			if i+1 < len(candidates) {
				p.observeInternalOperation(ctx, "discovery_fallback", "native_field_values_relaxed_after_error", 0)
			}
			continue
		}
		params := url.Values{}
		params.Set("query", logsqlQuery)
		params.Set("field", field)
		if start != "" {
			params.Set("start", start)
		}
		if end != "" {
			params.Set("end", end)
		}
		if limit > 0 {
			params.Set("limit", strconv.Itoa(limit))
		}
		resp, err := p.vlGet(ctx, "/select/logsql/field_values", params)
		if err != nil {
			lastErr = err
			if i+1 < len(candidates) {
				p.observeInternalOperation(ctx, "discovery_fallback", "native_field_values_relaxed_after_error", 0)
			}
			continue
		}
		body, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if resp.StatusCode >= http.StatusBadRequest {
			msg := strings.TrimSpace(string(body))
			if msg == "" {
				msg = fmt.Sprintf("VL backend returned %d", resp.StatusCode)
			}
			lastErr = fmt.Errorf("%s", msg)
			if i+1 < len(candidates) {
				p.observeInternalOperation(ctx, "discovery_fallback", "native_field_values_relaxed_after_error", 0)
			}
			continue
		}
		var parsed vlFieldValuesResponse
		if err := json.Unmarshal(body, &parsed); err != nil {
			lastErr = err
			continue
		}
		// VL returns hits=0 for valid non-indexed fields (e.g. trace_id, amount)
		// where it has the values but does not count occurrences. Only exclude
		// explicitly negative hits (sentinel/error) and blank values.
		allZero := true
		for _, item := range parsed.Values {
			if item.Hits > 0 {
				allZero = false
				break
			}
		}
		values := make([]string, 0, len(parsed.Values))
		for _, item := range parsed.Values {
			if item.Hits < 0 || strings.TrimSpace(item.Value) == "" {
				continue
			}
			// When a mix of zero and positive hits exists, zero-hit entries are
			// stale indexed names with no matching lines — skip them.
			if !allZero && item.Hits == 0 {
				continue
			}
			values = append(values, item.Value)
		}
		sort.Strings(values)
		if len(values) == 0 && i+1 < len(candidates) {
			p.observeInternalOperation(ctx, "discovery_fallback", "native_field_values_empty_primary", 0)
			continue
		}
		if len(values) > 0 {
			return values, nil
		}
		// No values from native index — field may be inside JSON or logfmt _msg.
		// Retry once with unpack pipes so VL extracts the field from _msg content.
		if !strings.Contains(logsqlQuery, "unpack_json") && !strings.Contains(logsqlQuery, "unpack_logfmt") {
			unpackQuery := logsqlQuery + " | unpack_json from _msg | unpack_logfmt from _msg"
			params.Set("query", unpackQuery)
			resp2, err2 := p.vlGet(ctx, "/select/logsql/field_values", params)
			if err2 == nil {
				body2, _ := io.ReadAll(resp2.Body)
				_ = resp2.Body.Close()
				if resp2.StatusCode < http.StatusBadRequest {
					var parsed2 vlFieldValuesResponse
					if json.Unmarshal(body2, &parsed2) == nil {
						for _, item := range parsed2.Values {
							if item.Hits >= 0 && strings.TrimSpace(item.Value) != "" {
								values = append(values, item.Value)
							}
						}
						sort.Strings(values)
					}
				}
			}
		}
		return values, nil
	}
	return nil, lastErr
}

// fetchUnpackedFieldValues queries VL with | unpack_json from _msg |
// unpack_logfmt from _msg to extract values for fields that are not
// VL-indexed (inside JSON or logfmt _msg). Used as a last-resort fallback
// when neither native field_values nor log-line scanning produced values.
func (p *Proxy) fetchUnpackedFieldValues(ctx context.Context, query, start, end, field string, limit int) ([]string, error) {
	logsqlQuery, err := p.translateQuery(defaultQuery(query))
	if err != nil {
		return nil, err
	}
	if strings.Contains(logsqlQuery, "unpack_json") || strings.Contains(logsqlQuery, "unpack_logfmt") {
		return nil, nil
	}
	unpackQuery := logsqlQuery + " | unpack_json from _msg | unpack_logfmt from _msg"
	params := url.Values{}
	params.Set("query", unpackQuery)
	params.Set("field", field)
	if start != "" {
		params.Set("start", formatVLTimestamp(start))
	}
	if end != "" {
		params.Set("end", formatVLTimestamp(end))
	}
	if limit > 0 {
		params.Set("limit", strconv.Itoa(limit))
	}
	resp, err := p.vlGet(ctx, "/select/logsql/field_values", params)
	if err != nil {
		return nil, err
	}
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode >= http.StatusBadRequest {
		return nil, nil
	}
	var parsed vlFieldValuesResponse
	if json.Unmarshal(body, &parsed) != nil {
		return nil, nil
	}
	values := make([]string, 0, len(parsed.Values))
	for _, item := range parsed.Values {
		if item.Hits >= 0 && strings.TrimSpace(item.Value) != "" {
			values = append(values, item.Value)
		}
	}
	sort.Strings(values)
	return values, nil
}

func (p *Proxy) resolveNativeDetectedField(ctx context.Context, query, start, end, fieldName string) (string, bool, error) {
	var lastErr error
	queryCandidates := fieldDetectionQueryCandidates(query)
	for i, candidate := range queryCandidates {
		fieldNames, err := p.fetchNativeFieldNamesForCandidate(ctx, candidate, start, end)
		if err != nil {
			lastErr = err
			continue
		}
		fieldCandidates := make([]string, 0, len(fieldNames))
		fieldCandidates = append(fieldCandidates, fieldNames...)
		resolution := p.labelTranslator.ResolveMetadataCandidates(fieldName, fieldCandidates, p.metadataFieldMode)
		if len(resolution.candidates) == 1 {
			return resolution.candidates[0], true, nil
		}
		if i+1 < len(queryCandidates) {
			p.observeInternalOperation(ctx, "discovery_fallback", "native_detected_field_empty_primary", 0)
		}
		return "", false, nil
	}
	if lastErr != nil {
		return "", false, lastErr
	}
	return "", false, nil
}

func (p *Proxy) detectNativeLabels(ctx context.Context, query, start, end string) (map[string]*detectedLabelSummary, error) {
	parsed, err := p.fetchNativeStreams(ctx, query, start, end)
	if err != nil {
		return nil, err
	}
	summaries := map[string]*detectedLabelSummary{}
	for _, item := range parsed.Values {
		labels := parseStreamLabels(item.Value)
		ensureSyntheticServiceName(labels)
		for key, value := range labels {
			if key == "detected_level" {
				continue
			}
			lokiLabel := p.labelTranslator.ToLoki(key)
			if lokiLabel == "" {
				continue
			}
			summary := summaries[lokiLabel]
			if summary == nil {
				summary = &detectedLabelSummary{label: lokiLabel, values: map[string]struct{}{}}
				summaries[lokiLabel] = summary
			}
			summary.values[value] = struct{}{}
		}
	}
	return summaries, nil
}

func (p *Proxy) fetchNativeStreams(ctx context.Context, query, start, end string) (*vlStreamsResponse, error) {
	var lastErr error
	for _, candidate := range fieldDetectionQueryCandidates(query) {
		logsqlQuery, err := p.translateQuery(candidate)
		if err != nil {
			lastErr = err
			continue
		}
		params := url.Values{}
		params.Set("query", logsqlQuery+" | sort by (_time desc)")
		if start != "" {
			params.Set("start", start)
		}
		if end != "" {
			params.Set("end", end)
		}
		body, err := p.vlGetCoalesced(ctx, p.nativeCoalescerKey("native_streams", ctx, params), "/select/logsql/streams", params)
		if err != nil {
			lastErr = err
			continue
		}
		var parsed vlStreamsResponse
		if err := json.Unmarshal(body, &parsed); err != nil {
			lastErr = err
			continue
		}
		return &parsed, nil
	}
	return &vlStreamsResponse{}, lastErr
}

func filterNativeDetectedFields(native map[string]*detectedFieldSummary, streamLabels map[string]string, lt *LabelTranslator) map[string]*detectedFieldSummary {
	if len(native) == 0 || len(streamLabels) == 0 {
		return native
	}
	filtered := make(map[string]*detectedFieldSummary, len(native))
	for label, summary := range native {
		if shouldExposeStructuredField(label, streamLabels, lt) {
			filtered[label] = summary
		}
	}
	return filtered
}

func nativeFieldFilterNeedsStreamLabels(native map[string]*detectedFieldSummary, lt *LabelTranslator) bool {
	for label := range native {
		if !shouldExposeStructuredField(label, map[string]string{label: "1"}, lt) {
			return true
		}
	}
	return false
}

func (p *Proxy) detectScannedLabels(ctx context.Context, query, start, end string, lineLimit int) (map[string]*detectedLabelSummary, error) {
	candidates := fieldDetectionQueryCandidates(query)
	var lastErr error
	for i, candidate := range candidates {
		logsqlQuery, err := p.translateQuery(candidate)
		if err != nil {
			lastErr = err
			if i+1 < len(candidates) {
				p.observeInternalOperation(ctx, "discovery_fallback", "detected_labels_relaxed_after_error", 0)
			}
			continue
		}

		params := url.Values{}
		params.Set("query", logsqlQuery+" | sort by (_time desc)")
		params.Set("limit", strconv.Itoa(lineLimit))
		if start != "" {
			params.Set("start", formatVLTimestamp(start))
		}
		if end != "" {
			params.Set("end", formatVLTimestamp(end))
		}

		resp, err := p.vlPost(ctx, "/select/logsql/query", params)
		if err != nil {
			lastErr = err
			if i+1 < len(candidates) {
				p.observeInternalOperation(ctx, "discovery_fallback", "detected_labels_relaxed_after_error", 0)
			}
			continue
		}

		body, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if resp.StatusCode >= http.StatusBadRequest {
			msg := strings.TrimSpace(string(body))
			if msg == "" {
				msg = fmt.Sprintf("VL backend returned %d", resp.StatusCode)
			}
			lastErr = fmt.Errorf("%s", msg)
			if i+1 < len(candidates) {
				p.observeInternalOperation(ctx, "discovery_fallback", "detected_labels_relaxed_after_error", 0)
			}
			continue
		}

		summaries := scanDetectedLabelSummaries(body, p.labelTranslator)
		if len(summaries) == 0 && i+1 < len(candidates) {
			p.observeInternalOperation(ctx, "discovery_fallback", "detected_labels_empty_primary", 0)
		}
		return summaries, nil
	}
	return nil, lastErr
}

type detectedFieldsCachePayload struct {
	Fields []map[string]interface{} `json:"fields"`
	Values map[string][]string      `json:"values"`
}

type detectedLabelsCachePayload struct {
	Labels []map[string]interface{} `json:"labels"`
	Values map[string][]string      `json:"values"`
}

func (p *Proxy) detectedFieldsCacheKey(ctx context.Context, query, start, end string, lineLimit int) string {
	key := "detect_fields:" + getOrgID(ctx) + ":" + defaultFieldDetectionQuery(query) + ":" + start + ":" + end + ":" + strconv.Itoa(lineLimit)
	if origReq, ok := ctx.Value(origRequestKey).(*http.Request); ok && origReq != nil {
		if fp := p.forwardedAuthFingerprint(origReq); fp != "" {
			key += ":auth:" + fp
		}
	}
	return key
}

func (p *Proxy) detectedLabelsCacheKey(ctx context.Context, query, start, end string, lineLimit int) string {
	key := "detect_labels:" + getOrgID(ctx) + ":" + defaultFieldDetectionQuery(query) + ":" + start + ":" + end + ":" + strconv.Itoa(lineLimit)
	if origReq, ok := ctx.Value(origRequestKey).(*http.Request); ok && origReq != nil {
		if fp := p.forwardedAuthFingerprint(origReq); fp != "" {
			key += ":auth:" + fp
		}
	}
	return key
}

func (p *Proxy) getCachedDetectedFields(ctx context.Context, query, start, end string, lineLimit int) ([]map[string]interface{}, map[string][]string, bool) {
	if p.cache == nil {
		return nil, nil, false
	}
	raw, ok := p.cache.Get(p.detectedFieldsCacheKey(ctx, query, start, end, lineLimit))
	if !ok {
		return nil, nil, false
	}
	var payload detectedFieldsCachePayload
	if err := json.Unmarshal(raw, &payload); err != nil {
		return nil, nil, false
	}
	return payload.Fields, payload.Values, true
}

func (p *Proxy) setCachedDetectedFields(ctx context.Context, query, start, end string, lineLimit int, fields []map[string]interface{}, values map[string][]string) {
	if p.cache == nil {
		return
	}
	body, err := json.Marshal(detectedFieldsCachePayload{Fields: fields, Values: values})
	if err != nil {
		return
	}
	p.cache.SetWithTTL(p.detectedFieldsCacheKey(ctx, query, start, end, lineLimit), body, CacheTTLs["detected_fields"])
}

func (p *Proxy) getCachedDetectedLabels(ctx context.Context, query, start, end string, lineLimit int) ([]map[string]interface{}, map[string]*detectedLabelSummary, bool) {
	if p.cache == nil {
		return nil, nil, false
	}
	raw, ok := p.cache.Get(p.detectedLabelsCacheKey(ctx, query, start, end, lineLimit))
	if !ok {
		return nil, nil, false
	}
	var payload detectedLabelsCachePayload
	if err := json.Unmarshal(raw, &payload); err != nil {
		return nil, nil, false
	}
	summaries := make(map[string]*detectedLabelSummary, len(payload.Values))
	for label, values := range payload.Values {
		summary := &detectedLabelSummary{label: label, values: map[string]struct{}{}}
		for _, value := range values {
			summary.values[value] = struct{}{}
		}
		summaries[label] = summary
	}
	return payload.Labels, summaries, true
}

func (p *Proxy) setCachedDetectedLabels(ctx context.Context, query, start, end string, lineLimit int, labels []map[string]interface{}, summaries map[string]*detectedLabelSummary) {
	if p.cache == nil {
		return
	}
	values := make(map[string][]string, len(summaries))
	for label, summary := range summaries {
		sorted := make([]string, 0, len(summary.values))
		for value := range summary.values {
			sorted = append(sorted, value)
		}
		sort.Strings(sorted)
		values[label] = sorted
	}
	body, err := json.Marshal(detectedLabelsCachePayload{Labels: labels, Values: values})
	if err != nil {
		return
	}
	p.cache.SetWithTTL(p.detectedLabelsCacheKey(ctx, query, start, end, lineLimit), body, 30*time.Second)
}

func formatDetectedValue(value interface{}) string {
	switch v := value.(type) {
	case string:
		return v
	case float64:
		if v == float64(int64(v)) {
			return strconv.FormatInt(int64(v), 10)
		}
		return strconv.FormatFloat(v, 'f', -1, 64)
	case bool:
		if v {
			return "true"
		}
		return "false"
	default:
		return strings.TrimSpace(strings.ReplaceAll(strings.TrimPrefix(strings.TrimSuffix(strings.TrimSpace(string(mustJSON(value))), "\""), "\""), "\n", " "))
	}
}

func mustJSON(value interface{}) []byte {
	data, _ := json.Marshal(value)
	return data
}
