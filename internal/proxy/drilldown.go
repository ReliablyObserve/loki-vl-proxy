package proxy

import (
	"bufio"
	"bytes"
	"context"
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

const unknownServiceName = "unknown_service"
const detectedFieldsSampleLimit = 500

// detectedFieldsScannerLineBytes is the maximum NDJSON line length the streaming
// scanner will accept. VL log entries rarely exceed a few KB; 2 MB is a safe cap
// that avoids bufio.Scanner token-too-large errors without over-allocating.
const detectedFieldsScannerLineBytes = 2 * 1024 * 1024

// detectedFieldsScanBufPool pools the initial 64 KB scanner read buffers used by
// detectFieldSummariesStream. Each call would otherwise allocate a fresh slice;
// pooling amortises that cost across concurrent requests.
var detectedFieldsScanBufPool = sync.Pool{
	New: func() interface{} { b := make([]byte, 64*1024); return &b },
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
	var parsed map[string]stdjson.RawMessage
	if stdjson.Unmarshal([]byte(s), &parsed) != nil {
		return "", false
	}
	for _, key := range levelJSONKeys {
		raw, ok := parsed[key]
		if !ok {
			continue
		}
		var level string
		if stdjson.Unmarshal(raw, &level) != nil {
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

// buildEntryLabelsWithStream is like buildEntryLabels but accepts an already-parsed
// stream map to avoid a redundant parseStreamLabels call in hot paths.
func buildEntryLabelsWithStream(entry map[string]interface{}, stream map[string]string) map[string]string {
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
	if labels["level"] != "" {
		delete(labels, "detected_level")
	}
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

// detectedLabelsBufPool pools map[string]string for detected-label scan callers
// that iterate the result immediately and discard it within the same loop tick.
var detectedLabelsBufPool = sync.Pool{
	New: func() interface{} {
		return make(map[string]string, 16)
	},
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
	if err := stdjson.Unmarshal(body, &vlResp); err != nil {
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
		streamFields, err := p.fetchStreamFieldNamesCached(ctx, params)
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
	if sec, ok := parseFlexibleUnixSeconds(ts); ok {
		return time.Unix(sec, 0).UTC(), true
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
	// Always strip | unwrap and | drop __error__ (they break log scanning for field detection).
	// For parser stages:
	//   | json   → always strip. VL v1.50+ auto-indexes JSON from _msg, so VL can evaluate
	//              downstream field filters without the explicit | json stage. Keeping | json
	//              causes VL to expand every _msg JSON key into top-level NDJSON fields in the
	//              query response, which the scanner then detects as thousands of garbage names.
	//   | logfmt / | unpack → keep when there are downstream field comparisons. VL does NOT
	//              auto-parse logfmt/unpack content, so stripping these stages leaves the field
	//              filter referencing fields that VL cannot evaluate, returning zero results.
	noUnwrap := stripUnwrapAndDropStages(query)
	// Strip only | json stages; keep | logfmt / | unpack.
	noJSONParser := collapseSpaces(reJSONParserStage.ReplaceAllString(noUnwrap, " "))
	noParser := stripParserOnlyStages(noUnwrap)
	// If stripping all parsers removes something that | json didn't (i.e. logfmt/unpack present)
	// AND there are downstream field comparisons that depend on those parsers, keep them.
	if noParser != noJSONParser && hasFieldComparisonStages(noParser) {
		return defaultQuery(noJSONParser)
	}
	return defaultQuery(noParser)
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

var (
	reDropStage          = regexp.MustCompile(`\|\s*drop\s+__error__(\s*,\s*__error_details__)?\s*`)
	reParserStage        = regexp.MustCompile(`\|\s*(json|logfmt|unpack)(\s+[^|]+)?`)
	reJSONParserStage    = regexp.MustCompile(`\|\s*json(\s+[^|]+)?`)
	reUnwrapStage        = regexp.MustCompile(`\|\s*unwrap(?:\s+[^|]+)?`)
)

func collapseSpaces(s string) string {
	for strings.Contains(s, "  ") {
		s = strings.ReplaceAll(s, "  ", " ")
	}
	return strings.TrimSpace(s)
}

func stripUnwrapAndDropStages(query string) string {
	query = strings.TrimSpace(query)
	if query == "" {
		return query
	}
	query = reDropStage.ReplaceAllString(query, " ")
	query = reUnwrapStage.ReplaceAllString(query, " ")
	return collapseSpaces(query)
}

func stripParserOnlyStages(query string) string {
	query = strings.TrimSpace(query)
	if query == "" {
		return query
	}
	return collapseSpaces(reParserStage.ReplaceAllString(query, " "))
}

func stripFieldDetectionStages(query string) string {
	return stripParserOnlyStages(stripUnwrapAndDropStages(query))
}

func hasFieldComparisonStages(query string) bool {
	return regexp.MustCompile(`\|\s*[A-Za-z_][A-Za-z0-9_.-]*\s*(?:=~|!~|!=|=|>=|<=|>|<)`).MatchString(query)
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

type detectNativeResult struct {
	fields map[string]*detectedFieldSummary
	err    error
}

func (p *Proxy) detectFields(ctx context.Context, query, start, end string, lineLimit int) ([]map[string]interface{}, map[string][]string, error) {
	if lineLimit > maxDetectedScanLines {
		lineLimit = maxDetectedScanLines
	}
	if cachedFields, cachedValues, ok := p.getCachedDetectedFields(ctx, query, start, end, lineLimit); ok {
		return cachedFields, cachedValues, nil
	}

	// Start native field detection in parallel with the log scan to eliminate
	// sequential round-trip overhead. Previously: 2× VL RTT. Now: 1× VL RTT.
	// Native fields provide OTel/stream-label metadata; the log scan samples
	// actual log lines to infer field names and types.
	nativeCh := make(chan detectNativeResult, 1)
	go func() {
		fields, err := p.detectNativeFields(ctx, query, start, end)
		nativeCh <- detectNativeResult{fields: fields, err: err}
	}()

	// Use detectedFieldsSampleLimit as a safe conservative scan size.
	// When native fields exist they would cap the scan at this limit anyway,
	// and without native fields it is still enough for reliable field detection.
	scanLimit := lineLimit
	if scanLimit > detectedFieldsSampleLimit {
		scanLimit = detectedFieldsSampleLimit
	}

	candidates := fieldDetectionQueryCandidates(query)
	var lastErr error
	var hadScanFailure bool
	var (
		scanFieldList   []map[string]interface{}
		scanFieldValues map[string][]string
	)

	for _, candidate := range candidates {
		logsqlQuery, err := p.translateQuery(candidate)
		if err != nil {
			lastErr = err
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
			continue
		}

		// Check status from headers before reading the body.
		// Error bodies are small; success bodies are streamed without buffering.
		if resp.StatusCode >= http.StatusInternalServerError {
			errBody, _ := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			msg := strings.TrimSpace(string(errBody))
			if msg == "" {
				msg = fmt.Sprintf("VL backend returned %d", resp.StatusCode)
			}
			lastErr = fmt.Errorf("%s", msg)
			hadScanFailure = true
			continue
		}
		if resp.StatusCode >= http.StatusBadRequest {
			errBody, _ := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			msg := strings.TrimSpace(string(errBody))
			if msg == "" {
				msg = fmt.Sprintf("VL backend returned %d", resp.StatusCode)
			}
			// Collect native result before returning so the goroutine doesn't leak.
			<-nativeCh
			return nil, nil, fmt.Errorf("%s", msg)
		}
		// Stream the NDJSON response line-by-line without buffering the full body.
		scanFieldList, scanFieldValues, _ = p.detectFieldSummariesStream(resp.Body)
		_ = resp.Body.Close()
		break
	}

	// Collect native fields result (always, to avoid goroutine leak).
	nativeResult := <-nativeCh
	nativeFields := nativeResult.fields
	if nativeResult.err != nil {
		nativeFields = nil
	}

	// When the primary (strict) candidate returned 2xx with zero log lines — not a
	// VL error, just no matching data — and a relaxed candidate exists, return empty
	// immediately. Callers that own a relaxation loop (resolveDetectedFieldValues)
	// will retry with the relaxed query. handleDetectedFields has its own fallback
	// (see label_handlers.go) that also handles this case.
	if len(scanFieldList) == 0 && len(candidates) > 1 && !hadScanFailure {
		emptyFields := []map[string]interface{}{}
		emptyValues := map[string][]string{}
		p.setCachedDetectedFields(ctx, query, start, end, lineLimit, emptyFields, emptyValues)
		return emptyFields, emptyValues, nil
	}

	if len(scanFieldList) > 0 {
		// When the log-line scan produced results, use them as-is.
		// The scan's outer obj.Visit already captures native VL metadata fields
		// (OTel service.name, trace_id, etc.) for entries whose _msg is not JSON,
		// and the _msg JSON parsing pass finds all JSON fields with parser="json".
		// Merging the native field_names index here would flood the response:
		// VL auto-indexes every JSON key across the entire time range, so the index
		// may contain thousands of field names accumulated from rare log variants
		// that the 500-line scan sample correctly excludes.
		fieldList, fieldValues := mergeNativeDetectedFields(scanFieldList, scanFieldValues, nil)
		p.setCachedDetectedFields(ctx, query, start, end, lineLimit, fieldList, fieldValues)
		return fieldList, fieldValues, nil
	}

	if len(nativeFields) > 0 {
		if nativeFieldFilterNeedsStreamLabels(nativeFields, p.labelTranslator) {
			streamLabels, err := p.fetchNativeStreamLabelSet(ctx, query, start, end)
			if err != nil {
				// Cannot determine stream labels → skip native fields to avoid flooding
				// the detected fields response with every VL-indexed field name.
				nativeFields = nil
			} else {
				nativeFields = filterNativeDetectedFields(nativeFields, streamLabels, p.labelTranslator)
			}
		}
		if len(nativeFields) == 0 {
			return nil, nil, lastErr
		}
		nativeFieldList, nativeFieldValues := mergeNativeDetectedFields(nil, nil, nativeFields)
		p.setCachedDetectedFields(ctx, query, start, end, lineLimit, nativeFieldList, nativeFieldValues)
		return nativeFieldList, nativeFieldValues, nil
	}
	return nil, nil, lastErr
}

// detectFieldsNativeOnly returns the field list that VL's native field_names index
// knows about for query, without doing a log-line scan. Used as a lightweight fallback
// when the strict query returns zero scan results but VL still has index entries for
// the stream. Avoids the "too many fields" problem that would arise from scanning the
// entire bare-selector stream.
func (p *Proxy) detectFieldsNativeOnly(ctx context.Context, query, start, end string) ([]map[string]interface{}, map[string][]string) {
	nativeFields, err := p.detectNativeFields(ctx, query, start, end)
	if err != nil || len(nativeFields) == 0 {
		return nil, nil
	}
	if nativeFieldFilterNeedsStreamLabels(nativeFields, p.labelTranslator) {
		streamLabels, serr := p.fetchNativeStreamLabelSet(ctx, query, start, end)
		if serr != nil {
			return nil, nil
		}
		nativeFields = filterNativeDetectedFields(nativeFields, streamLabels, p.labelTranslator)
	}
	return mergeNativeDetectedFields(nil, nil, nativeFields)
}

// isOTelDataFJ is the fastjson-aware equivalent of isOTelData.
// It inspects the same fields as isOTelData but via fastjson getters,
// avoiding the map[string]interface{} allocation.
func isOTelDataFJ(fjVal *fj.Value) bool {
	if fjVal == nil {
		return false
	}
	streamStr := string(fjVal.GetStringBytes("_stream"))
	streamLabels := parseStreamLabels(streamStr)

	for key := range streamLabels {
		if _, isOTelField := otelSemanticFields[key]; isOTelField {
			return true
		}
	}
	for key := range streamLabels {
		if key == "service_name" {
			if _, hasRealServiceName := streamLabels["service.name"]; hasRealServiceName {
				return true
			}
			continue
		}
		for _, prefix := range otelUnderscorePrefixes {
			if strings.HasPrefix(key, prefix) {
				return true
			}
		}
	}
	msg := string(fjVal.GetStringBytes("_msg"))
	if msg != "" {
		if strings.Contains(msg, "trace_id") && strings.Contains(msg, "span_id") {
			for key := range streamLabels {
				if strings.HasPrefix(key, "k8s.") || strings.HasPrefix(key, "deployment.") {
					return true
				}
			}
		}
	}
	return false
}

// fillDetectedLabelsFJ is the fastjson-aware equivalent of fillDetectedLabels.
// It populates buf with stream labels + service-name fields + level, without
// the map[string]interface{} intermediate.
func fillDetectedLabelsFJ(fjVal *fj.Value, buf map[string]string) {
	for k := range buf {
		delete(buf, k)
	}
	streamStr := string(fjVal.GetStringBytes("_stream"))
	stream := parseStreamLabels(streamStr)
	for k, v := range stream {
		buf[k] = v
	}
	for _, key := range serviceNameSourceFields {
		if _, ok := buf[key]; ok {
			continue
		}
		v := fjVal.Get(key)
		if v == nil || v.Type() == fj.TypeNull {
			continue
		}
		if s, ok := stringifyFJValue(v); ok && strings.TrimSpace(s) != "" {
			buf[key] = s
		}
	}
	if raw := fjVal.GetStringBytes("level"); len(raw) != 0 {
		if s := strings.TrimSpace(string(raw)); s != "" {
			buf["level"] = s
		}
	}
	ensureDetectedLevel(buf)
	ensureSyntheticServiceName(buf)
}

// inferDetectedTypeFJ is the fastjson-aware equivalent of inferDetectedType.
// It determines the Loki field type from a *fj.Value without interface{} boxing.
func inferDetectedTypeFJ(v *fj.Value) string {
	if v == nil {
		return "string"
	}
	switch v.Type() {
	case fj.TypeTrue, fj.TypeFalse:
		return "boolean"
	case fj.TypeNumber:
		// Use Float64() to match encoding/json semantics: JSON numbers unmarshal
		// to float64, and a value with no fractional part (e.g. 1e10) is "int".
		f, err := v.Float64()
		if err != nil {
			return "float"
		}
		if f == float64(int64(f)) {
			return "int"
		}
		return "float"
	case fj.TypeString:
		s := string(v.GetStringBytes())
		if _, err := time.ParseDuration(s); err == nil {
			return "duration"
		}
		if _, err := strconv.ParseInt(s, 10, 64); err == nil {
			return "int"
		}
		if _, err := strconv.ParseFloat(s, 64); err == nil {
			return "float"
		}
		return "string"
	default:
		return "string"
	}
}

// formatDetectedValueFJ is the fastjson-aware equivalent of formatDetectedValue.
func formatDetectedValueFJ(v *fj.Value) string {
	if v == nil {
		return ""
	}
	switch v.Type() {
	case fj.TypeString:
		return string(v.GetStringBytes())
	case fj.TypeNumber:
		raw := v.String()
		// Preserve integer formatting (no decimal point) matching formatDetectedValue.
		if f, err := strconv.ParseFloat(raw, 64); err == nil {
			if f == float64(int64(f)) {
				return strconv.FormatInt(int64(f), 10)
			}
			return strconv.FormatFloat(f, 'f', -1, 64)
		}
		return raw
	case fj.TypeTrue:
		return "true"
	case fj.TypeFalse:
		return "false"
	default:
		s := strings.TrimSpace(v.String())
		s = strings.TrimPrefix(s, "\"")
		s = strings.TrimSuffix(s, "\"")
		return strings.TrimSpace(strings.ReplaceAll(s, "\n", " "))
	}
}

// detectFieldSummariesStream is the streaming equivalent of detectFieldSummaries.
// It reads NDJSON from r line-by-line using bufio.Scanner, eliminating the
// 0.5–2.5 MB per-request io.ReadAll buffer that causes memory pressure under load.
//
// Stream label names are accumulated incrementally (one-pass). For multi-stream
// queries a body field in an early entry is not suppressed by a stream label
// seen in a later entry — an acceptable minor semantic difference; stream labels
// are uniform within a stream, so the case only arises when field names collide
// across streams, which is rare in practice.
//
// The third return value is the accumulated stream label set (key→value),
// passed to filterNativeDetectedFields.
func (p *Proxy) detectFieldSummariesStream(r io.Reader) ([]map[string]interface{}, map[string][]string, map[string]string) {
	scanner := bufio.NewScanner(r)
	bufPtr := detectedFieldsScanBufPool.Get().(*[]byte)
	scanner.Buffer(*bufPtr, detectedFieldsScannerLineBytes)
	defer detectedFieldsScanBufPool.Put(bufPtr)

	fields := make(map[string]*detectedFieldSummary)
	labelNames := make(map[string]struct{})
	streamLabelSet := make(map[string]string)
	exposureCache := make(map[string][]metadataFieldExposure, 16)
	anyOTelWithServiceName := false

	streamLabelBuf := detectedLabelsBufPool.Get().(map[string]string)
	defer func() {
		for k := range streamLabelBuf {
			delete(streamLabelBuf, k)
		}
		detectedLabelsBufPool.Put(streamLabelBuf)
	}()

	for scanner.Scan() {
		line := bytes.TrimSpace(scanner.Bytes())
		if len(line) == 0 {
			continue
		}

		// Use pooled fastjson parser instead of stdjson.Unmarshal + map[string]interface{}.
		fjParser := vlFJParserPool.Get()
		fjVal, err := fjParser.ParseBytes(line)
		if err != nil {
			vlFJParserPool.Put(fjParser)
			continue
		}

		fillDetectedLabelsFJ(fjVal, streamLabelBuf)
		if detectedLevel := strings.TrimSpace(streamLabelBuf["detected_level"]); detectedLevel != "" {
			addDetectedField(fields, "detected_level", "", "string", nil, detectedLevel)
		}

		if isOTelDataFJ(fjVal) {
			if _, ok := streamLabelBuf["service.name"]; ok {
				anyOTelWithServiceName = true
			}
		}

		rawStreamLabels := parseStreamLabels(string(fjVal.GetStringBytes("_stream")))
		for key, value := range rawStreamLabels {
			lokiLabel := key
			if p.labelTranslator != nil {
				if t := p.labelTranslator.ToLoki(key); t != "" {
					lokiLabel = t
				}
			}
			labelNames[lokiLabel] = struct{}{}
			if strings.TrimSpace(value) != "" {
				streamLabelSet[key] = value
			}
		}

		// Get _msg early so we can pre-collect its JSON keys before the outer native
		// field visit. VL auto-indexes all JSON keys from _msg as native NDJSON fields;
		// without this guard those keys appear in detected_fields twice (once without a
		// parser tag from obj.Visit, once with parser="json") and accumulate across
		// thousands of log entries, causing the "Fields 5,965" flood.
		msgBytes := fjVal.GetStringBytes("_msg")

		// Parse _msg JSON once: populate msgJSONKeys for the skip-guard below AND add
		// detected fields with parser="json" in the same pass.
		var msgJSONKeys map[string]struct{}
		if len(msgBytes) >= 2 && msgBytes[0] == '{' {
			fjParser2 := vlFJParserPool.Get()
			if fjVal2, err2 := fjParser2.ParseBytes(msgBytes); err2 == nil {
				if obj2, objErr2 := fjVal2.Object(); objErr2 == nil {
					msgJSONKeys = make(map[string]struct{})
					obj2.Visit(func(keyBytes []byte, v *fj.Value) {
						key := string(keyBytes)
						if key == "" {
							return
						}
						// Skip nested objects and arrays.
						vt := v.Type()
						if vt == fj.TypeObject || vt == fj.TypeArray {
							return
						}
						msgJSONKeys[key] = struct{}{}
						if shouldSuppressDetectedField(key) {
							return
						}
						if _, conflict := labelNames[key]; conflict {
							return
						}
						addDetectedField(fields, key, "json", inferDetectedTypeFJ(v), []string{key}, formatDetectedValueFJ(v))
					})
				}
			}
			vlFJParserPool.Put(fjParser2)
		}

		obj, objErr := fjVal.Object()
		if objErr == nil {
			obj.Visit(func(keyBytes []byte, v *fj.Value) {
				key := string(keyBytes)
				if key == "app" || key == "cluster" || key == "namespace" {
					return
				}
				// Skip keys already covered by _msg JSON parsing above.
				if _, inMsg := msgJSONKeys[key]; inMsg {
					return
				}
				if !shouldExposeStructuredField(key, rawStreamLabels, p.labelTranslator) {
					return
				}
				stringValue, ok := stringifyFJValue(v)
				if !ok {
					return
				}
				for _, exposure := range p.metadataFieldExposuresCached(key, exposureCache) {
					if _, conflict := labelNames[exposure.name]; conflict && !exposure.isAlias {
						continue
					}
					addDetectedField(fields, exposure.name, "", inferDetectedTypeFJ(v), nil, stringValue)
				}
			})
		}

		vlFJParserPool.Put(fjParser)

		if len(msgBytes) == 0 {
			continue
		}
		msg := string(msgBytes)

		for key, value := range parseLogfmtFields(msg) {
			if key == "msg" {
				continue
			}
			if key == "level" {
				addDetectedField(fields, "detected_level", "", "string", nil, value)
				continue
			}
			if shouldSuppressDetectedField(key) {
				continue
			}
			if _, conflict := labelNames[key]; conflict {
				continue
			}
			addDetectedField(fields, key, "logfmt", inferDetectedType(value), nil, value)
		}
	}

	// OTel stream labels (service.name, k8s.pod.name, deployment.environment, etc.)
	// appear only in the _stream string — not as top-level VL NDJSON keys — so
	// obj.Visit above never processes them. Expose them here from the accumulated
	// streamLabelSet so Grafana's structured metadata panel can show OTel conventions.
	for key, value := range streamLabelSet {
		isOTelSemantic := false
		if _, ok := otelSemanticFields[key]; ok {
			isOTelSemantic = true
		} else {
			for _, prefix := range otelUnderscorePrefixes {
				if strings.HasPrefix(key, prefix) {
					isOTelSemantic = true
					break
				}
			}
		}
		if !isOTelSemantic {
			continue
		}
		for _, exposure := range p.metadataFieldExposures(key) {
			if _, conflict := labelNames[exposure.name]; conflict && !exposure.isAlias {
				continue
			}
			addDetectedField(fields, exposure.name, "", "string", nil, value)
		}
	}

	if anyOTelWithServiceName {
		// Only expose service_name as an alias when the raw VL stream does NOT already
		// carry "service_name" (underscore) as a literal stream-label key.  OTel data
		// stores it as "service.name" (dot) which the label translator maps to the Loki
		// label "service_name" — that translation should remain visible.  But when a
		// stream is indexed with a literal service_name key (e.g. plain Loki pushes
		// labelled service_name=…), surfacing it again as a detected field would
		// duplicate the stream selector in the Drilldown fields panel, contrary to how
		// Loki treats stream labels.
		if _, rawIsServiceName := streamLabelSet["service_name"]; !rawIsServiceName {
			var source *detectedFieldSummary
			if serviceDot, ok := fields["service.name"]; ok {
				source = serviceDot
			} else {
				source = &detectedFieldSummary{label: "service_name", typ: "string"}
			}
			fields["service_name"] = &detectedFieldSummary{
				label:       "service_name",
				typ:         source.typ,
				values:      source.values,
				cardinality: source.cardinality,
			}
		}
	}

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

	return fieldList, fieldValues, streamLabelSet
}

// scanDetectedLabelSummariesStream is the streaming equivalent of scanDetectedLabelSummaries.
func scanDetectedLabelSummariesStream(r io.Reader, lt *LabelTranslator) map[string]*detectedLabelSummary {
	summaries := map[string]*detectedLabelSummary{}

	labelBuf := detectedLabelsBufPool.Get().(map[string]string)
	defer func() {
		for k := range labelBuf {
			delete(labelBuf, k)
		}
		detectedLabelsBufPool.Put(labelBuf)
	}()

	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 64*1024), detectedFieldsScannerLineBytes)

	for scanner.Scan() {
		line := bytes.TrimSpace(scanner.Bytes())
		if len(line) == 0 {
			continue
		}

		fjParser := vlFJParserPool.Get()
		fjVal, err := fjParser.ParseBytes(line)
		if err != nil {
			vlFJParserPool.Put(fjParser)
			continue
		}

		fillDetectedLabelsFJ(fjVal, labelBuf)
		vlFJParserPool.Put(fjParser)
		delete(labelBuf, "detected_level")

		for key, value := range labelBuf {
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
	if err := stdjson.Unmarshal(body, &resp); err != nil {
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
		if err := stdjson.Unmarshal(body, &parsed); err != nil {
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
					if stdjson.Unmarshal(body2, &parsed2) == nil {
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
	if stdjson.Unmarshal(body, &parsed) != nil {
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
		if err := stdjson.Unmarshal(body, &parsed); err != nil {
			lastErr = err
			continue
		}
		return &parsed, nil
	}
	return &vlStreamsResponse{}, lastErr
}

func filterNativeDetectedFields(native map[string]*detectedFieldSummary, streamLabels map[string]string, lt *LabelTranslator) map[string]*detectedFieldSummary {
	if len(native) == 0 {
		return native
	}
	if len(streamLabels) == 0 {
		// Without stream labels we cannot distinguish label keys from structured fields;
		// returning native unfiltered would flood the UI with all VL-indexed field names.
		return make(map[string]*detectedFieldSummary)
	}
	filtered := make(map[string]*detectedFieldSummary, len(native))
	for label, summary := range native {
		if shouldExposeStructuredField(label, streamLabels, lt) && !shouldSuppressDetectedField(label) {
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

		if resp.StatusCode >= http.StatusBadRequest {
			errBody, _ := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			msg := strings.TrimSpace(string(errBody))
			if msg == "" {
				msg = fmt.Sprintf("VL backend returned %d", resp.StatusCode)
			}
			lastErr = fmt.Errorf("%s", msg)
			if i+1 < len(candidates) {
				p.observeInternalOperation(ctx, "discovery_fallback", "detected_labels_relaxed_after_error", 0)
			}
			continue
		}

		summaries := scanDetectedLabelSummariesStream(resp.Body, p.labelTranslator)
		_ = resp.Body.Close()
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
		if fp := p.fingerprintFromCtx(ctx, origReq); fp != "" {
			key += ":auth:" + fp
		}
	}
	return key
}

func (p *Proxy) detectedLabelsCacheKey(ctx context.Context, query, start, end string, lineLimit int) string {
	key := "detect_labels:" + getOrgID(ctx) + ":" + defaultFieldDetectionQuery(query) + ":" + start + ":" + end + ":" + strconv.Itoa(lineLimit)
	if origReq, ok := ctx.Value(origRequestKey).(*http.Request); ok && origReq != nil {
		if fp := p.fingerprintFromCtx(ctx, origReq); fp != "" {
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
	if err := stdjson.Unmarshal(raw, &payload); err != nil {
		return nil, nil, false
	}
	return payload.Fields, payload.Values, true
}

func (p *Proxy) setCachedDetectedFields(ctx context.Context, query, start, end string, lineLimit int, fields []map[string]interface{}, values map[string][]string) {
	if p.cache == nil {
		return
	}
	body, err := stdjson.Marshal(detectedFieldsCachePayload{Fields: fields, Values: values})
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
	if err := stdjson.Unmarshal(raw, &payload); err != nil {
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
	body, err := stdjson.Marshal(detectedLabelsCachePayload{Labels: labels, Values: values})
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
	data, _ := stdjson.Marshal(value)
	return data
}
