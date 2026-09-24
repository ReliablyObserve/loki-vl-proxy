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

	logqlpkg "github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
)

const unknownServiceName = "unknown_service"

// detectedFieldsSampleLimit caps the number of distinct VALUES sampled per
// detected field during scan. Matches Loki's default (LIMIT 100) so the
// "cardinality" Grafana Drilldown shows is identical across backends and so
// high-cardinality fields (trace_id, span_id, *_id, *_uid) report the same
// capped 100 instead of 500. Drilldown's plugin uses this number to decide
// when to render a chart vs a placeholder, so divergence here made the proxy
// produce blank-looking field-card charts for ID-style fields where Loki
// would render the same placeholder.
const detectedFieldsSampleLimit = 100

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
	"service.name":           {},
	"service.namespace":      {},
	"k8s.pod.name":           {},
	"k8s.namespace.name":     {},
	"k8s.node.name":          {},
	"k8s.container.name":     {},
	"deployment.environment": {},
	"deployment.name":        {},
	"deployment.version":     {},
	"host.name":              {},
	"host.id":                {},
	"host.arch":              {},
	"telemetry.sdk.name":     {},
	"telemetry.sdk.language": {},
	"telemetry.sdk.version":  {},
}

// OTel underscore-form prefixes (priority 2 signals)
var otelUnderscorePrefixes = []string{
	"k8s_",
	"deployment_",
	"telemetry_",
	"host_",
	"service_",
}

// serviceNameSourceFields is the derivation's field order, shared with the
// translator so the LogsQL and the response side stay in step.
var serviceNameSourceFields = translator.ServiceNameSourceFields()

type detectedFieldSummary struct {
	label       string
	typ         string
	parsers     map[string]struct{}
	values      map[string]struct{}
	jsonPath    []string
	cardinality int
	nativeHits  int64 // hits from VL field_names index; 0 for scan-found fields
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
		// Loki tests the label value for emptiness only, so a value that is
		// only whitespace is a service name like any other.
		if labels[key] != "" {
			return true
		}
	}
	return false
}

func deriveServiceName(labels map[string]string) string {
	for _, key := range serviceNameSourceFields {
		if value := labels[key]; value != "" {
			return value
		}
	}
	return unknownServiceName
}

func ensureSyntheticServiceName(labels map[string]string) {
	if labels == nil {
		return
	}
	if labels["service_name"] != "" {
		return
	}
	labels["service_name"] = deriveServiceName(labels)
}

// appendSyntheticLabels de-duplicates labels and adds the synthetic service_name
// label. A window without data stays empty: Loki returns no label names for it.
func appendSyntheticLabels(labels []string) []string {
	if len(labels) == 0 {
		return []string{}
	}
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

// ensureDetectedLevel mirrors a level label into detected_level. Only the
// metric and volume grouping paths use it; log responses, tail, patterns and
// detected_fields derive detected_level from the stored row.
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

// buildEntryLabels returns the Loki stream labels of a decoded VL row with
// Loki's default encoding: stream fields, every other stored string field and
// the derived detected_level.
func buildEntryLabels(entry map[string]interface{}) map[string]string {
	// parseStreamLabels returns a cached read-only map; the labels are a copy.
	return buildEntryLabelsWithStream(entry, parseStreamLabels(asString(entry["_stream"])))
}

// buildEntryLabelsWithStream is like buildEntryLabels but accepts an already-parsed
// stream map to avoid a redundant parseStreamLabels call in hot paths.
func buildEntryLabelsWithStream(entry map[string]interface{}, stream map[string]string) map[string]string {
	msg, _ := entry["_msg"].(string)
	rows := defaultLogRowLevels()
	detected := rows.mapRow(entry, msg, logRowStream{labels: stream, levels: levelFieldsFromLabels(stream)})
	return entryLabelsWithDetectedLevel(entry, stream, detected)
}

func entryLabelsWithDetectedLevel(entry map[string]interface{}, stream map[string]string, detected detectedLevel) map[string]string {
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
	labels[detectedLevelName(stream)] = detected.String()
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
	// A stored detected_level is replaced by the derived value.
	if isVLInternalField(key) || key == "_stream_id" || key == detectedLevelLabel {
		return false
	}
	if _, ok := streamLabels[key]; !ok {
		// Field not a stream label. For dotted/dashed names check whether the
		// sanitized (underscore) form IS a stream label — if so, this field was
		// injected by emit-structured-metadata and is a pure alias for a stream
		// label that is already exposed in the Labels tab; skip it to avoid
		// duplicate entries in the Fields tab.
		if strings.ContainsAny(key, ".-") {
			sanitized := SanitizeLabelName(key)
			if _, sanitizedOk := streamLabels[sanitized]; sanitizedOk {
				return false
			}
		}
		return true
	}
	// Key IS a stream label. Dotted/dashed stream labels (OTel-style) are always
	// exposed so they appear in the Fields tab alongside their underscore alias.
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
	nativeValues, nativeErr := p.serviceNameValuesFromNativeFields(ctx, query, start, end)
	if nativeErr == nil && len(nativeValues) > 0 {
		return nativeValues, nil
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
	// No | sort pipe: the /streams endpoint uses column-index lookups to enumerate
	// unique stream label sets. Adding | sort by (_time desc) forces VL to load and
	// sort ALL matching log entries before grouping into streams — an O(N_logs) heap
	// sort that OOMs on wide time windows. Stream order is irrelevant for service
	// name detection; we only need to know which streams exist in the window.
	params.Set("query", logsqlQuery)
	if start != "" {
		params.Set("start", start)
	}
	if end != "" {
		params.Set("end", end)
	}
	// Full requested range: /label/service_name/values must list every value with
	// data in [start, end], like Loki.

	resp, err := p.vlGet(ctx, "/select/logsql/streams", params)
	if err != nil {
		// A failed full-range call is returned as an error (callers serve a stale
		// answer or the error), never answered from the recent-data detection
		// sample below, which would be a partial list.
		return nil, err
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= http.StatusInternalServerError {
		return nil, p.redactedBackendStatusError("", resp.StatusCode, body)
	}
	if resp.StatusCode >= http.StatusBadRequest {
		if isVLUnsupportedPath(body) {
			// A backend without /select/logsql/streams: the full-range native
			// field lookup above is the complete answer (or its error).
			if nativeErr != nil {
				return nil, nativeErr
			}
			return []string{}, nil
		}
		// Invalid query or rejected request: return the error, as Loki does,
		// instead of answering from a recent-data sample.
		return nil, p.redactedBackendStatusError("", resp.StatusCode, body)
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

// isVLUnsupportedPath reports whether a VictoriaLogs error body says the
// requested endpoint does not exist on this version (HTTP 400,
// `unsupported path requested: "..."`), as opposed to a rejected query.
func isVLUnsupportedPath(body []byte) bool {
	return bytes.Contains(body, []byte("unsupported path requested"))
}

func (p *Proxy) serviceNameValuesFromNativeFields(ctx context.Context, query, start, end string) ([]string, error) {
	// Loki lists the service names it assigned at ingest, one per stream. The
	// proxy derives that value per row (the same derivation the selector and
	// metric grouping use) and asks VictoriaLogs for its distinct values, so a
	// listed value always selects rows: listing the values of every source
	// field would offer values that `{service_name="..."}` does not match.
	// No time-range cap: /label/service_name/values must list every service with
	// data in the requested [start, end] range, like Loki.
	params, err := p.metadataQueryParams(ctx, relaxedFieldDetectionQuery(query), start, end, "", "")
	if err != nil {
		return nil, err
	}
	params.Set("query", translator.WithDerivedServiceName(params.Get("query"), p.logsqlCapabilities()))
	params.Set("field", "service_name")
	values, err := p.fetchVLFieldValues(ctx, "/select/logsql/field_values", params)
	if err != nil {
		return nil, err
	}
	values = uniqueSortedNonEmptyStrings(values)
	if len(values) > 0 {
		return values, nil
	}
	return nil, nil
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
	lq, err := logqlpkg.ParseLogQuery(query)
	if err != nil {
		// Fall back to brace-scanner on parse failure.
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
	return lq.Selector.String()
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
			// The derivation pipes write the Loki value into service_name, so
			// one group field replaces every source field.
			addField("service_name")
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
	// Strip any metric wrapper (e.g. sum_over_time(...)) so that the inner log
	// stream query can be processed normally. Grafana's metric builder passes
	// the full metric query to detected_fields when populating the unwrap field picker.
	query = stripMetricWrapper(query)

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
	return defaultQuery(stripFieldComparisonStages(stripFieldDetectionStages(stripMetricWrapper(query))))
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
	reDropStage       = regexp.MustCompile(`\|\s*drop\s+__error__(\s*,\s*__error_details__)?\s*`)
	reParserStage     = regexp.MustCompile(`\|\s*(json|logfmt|unpack)(\s+[^|]+)?`)
	reJSONParserStage = regexp.MustCompile(`\|\s*json(\s+[^|]+)?`)
	reUnwrapStage     = regexp.MustCompile(`\|\s*unwrap(?:\s+[^|]+)?`)
	// reMetricWrapper matches a LogQL range-vector metric function prefix.
	// Used to strip the wrapper before field detection so that the inner log
	// stream query can be processed normally.
	reMetricWrapper = regexp.MustCompile(`(?i)^(sum_over_time|count_over_time|rate|bytes_rate|bytes_over_time|avg_over_time|max_over_time|min_over_time|stddev_over_time|stdvar_over_time|first_over_time|last_over_time|absent_over_time|rate_counter)\s*\(`)
)

// stripMetricWrapper extracts the inner log stream query from a metric range-vector
// expression. Grafana's metric builder passes the full query (including the
// sum_over_time() wrapper) to detected_fields — the wrapper must be removed before
// field detection so the inner log stream can be scanned normally.
// Non-metric queries are returned unchanged.
func stripMetricWrapper(query string) string {
	query = strings.TrimSpace(query)
	if !reMetricWrapper.MatchString(query) {
		return query
	}
	// Find the opening paren and extract everything inside the outermost call.
	open := strings.IndexByte(query, '(')
	if open < 0 {
		return query
	}
	inner := query[open+1:]
	depth := 1
	for i, ch := range inner {
		switch ch {
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				return strings.TrimSpace(inner[:i])
			}
		}
	}
	return query
}

func collapseSpaces(s string) string {
	for strings.Contains(s, "  ") {
		s = strings.ReplaceAll(s, "  ", " ")
	}
	return strings.TrimSpace(s)
}

// isErrorDropStage reports whether ds is the `| drop __error__[, __error_details__]`
// pattern that field-detection queries append. Only those specific drops are removed;
// user-authored `| drop` stages are left intact.
func isErrorDropStage(ds *logqlpkg.DropStage) bool {
	if len(ds.Matchers) != 0 {
		return false
	}
	labels := ds.Labels
	if len(labels) == 0 || labels[0] != "__error__" {
		return false
	}
	if len(labels) == 1 {
		return true
	}
	return len(labels) == 2 && labels[1] == "__error_details__"
}

// isParserOnlyStage reports whether a stage is a json/logfmt/unpack parser (no regexp/pattern).
func isParserOnlyStage(stage logqlpkg.Stage) bool {
	ps, ok := stage.(*logqlpkg.ParserStage)
	if !ok {
		return false
	}
	return ps.Type == logqlpkg.ParserJSON ||
		ps.Type == logqlpkg.ParserLogfmt ||
		ps.Type == logqlpkg.ParserUnpack
}

// filterPipeline returns a new LogQuery with stages removed wherever keep returns false.
func filterPipeline(lq *logqlpkg.LogQuery, keep func(logqlpkg.Stage) bool) *logqlpkg.LogQuery {
	out := &logqlpkg.LogQuery{Selector: lq.Selector}
	for _, s := range lq.Pipeline {
		if keep(s) {
			out.Pipeline = append(out.Pipeline, s)
		}
	}
	return out
}

func stripUnwrapAndDropStages(query string) string {
	query = strings.TrimSpace(query)
	if query == "" {
		return query
	}
	lq, err := logqlpkg.ParseLogQuery(query)
	if err != nil {
		// Fall back to regex on parse failure.
		query = reDropStage.ReplaceAllString(query, " ")
		query = reUnwrapStage.ReplaceAllString(query, " ")
		return collapseSpaces(query)
	}
	out := filterPipeline(lq, func(s logqlpkg.Stage) bool {
		if ds, ok := s.(*logqlpkg.DropStage); ok && isErrorDropStage(ds) {
			return false
		}
		if _, ok := s.(*logqlpkg.UnwrapStage); ok {
			return false
		}
		return true
	})
	return out.String()
}

// stripIncompleteUnwrapStubs removes | unwrap pipeline stages that have no field name.
// Grafana's metric builder emits these while the unwrap field picker is open and no
// field has been selected yet; the stage is syntactically invalid and causes a 400.
// Stripping it lets query_range return sample log lines so the picker can populate.
// Uses the AST parser instead of regex to robustly handle whitespace, comments,
// and arbitrary surrounding pipeline stages.
func stripIncompleteUnwrapStubs(query string) string {
	// Fast path: incomplete unwrap stubs only appear in queries containing "unwrap".
	// Skip the expensive AST parse for the common case.
	if !strings.Contains(query, "unwrap") {
		return query
	}
	expr, err := logqlpkg.Parse(query)
	if err != nil {
		return query
	}
	lq, ok := expr.(*logqlpkg.LogQuery)
	if !ok {
		return query
	}
	stripped := logqlpkg.StripIncompleteUnwrapStubs(lq)
	if stripped == lq {
		return query
	}
	return stripped.String()
}

func stripParserOnlyStages(query string) string {
	query = strings.TrimSpace(query)
	if query == "" {
		return query
	}
	lq, err := logqlpkg.ParseLogQuery(query)
	if err != nil {
		return collapseSpaces(reParserStage.ReplaceAllString(query, " "))
	}
	out := filterPipeline(lq, func(s logqlpkg.Stage) bool {
		return !isParserOnlyStage(s)
	})
	return out.String()
}

func stripFieldDetectionStages(query string) string {
	return stripParserOnlyStages(stripUnwrapAndDropStages(query))
}

// labelFilterHasComparison reports whether a LabelFilterStage.Raw string contains a
// field-comparison operator (=, !=, =~, !~, >=, <=, >, <).
var (
	labelFilterCompareRE      = regexp.MustCompile(`[A-Za-z_][A-Za-z0-9_.-]*\s*(?:=~|!~|!=|=|>=|<=|>|<)`)
	hasFieldComparisonRE      = regexp.MustCompile(`\|\s*[A-Za-z_][A-Za-z0-9_.-]*\s*(?:=~|!~|!=|=|>=|<=|>|<)`)
	stripFieldComparisonStgRE = regexp.MustCompile(`\|\s*[A-Za-z_][A-Za-z0-9_.-]*\s*(?:=~|!~|!=|=|>=|<=|>|<)\s*[^|]+`)
)

func hasFieldComparisonStages(query string) bool {
	lq, err := logqlpkg.ParseLogQuery(query)
	if err != nil {
		return hasFieldComparisonRE.MatchString(query)
	}
	for _, stage := range lq.Pipeline {
		lfs, ok := stage.(*logqlpkg.LabelFilterStage)
		if ok && labelFilterCompareRE.MatchString(lfs.Raw) {
			return true
		}
	}
	return false
}

func stripFieldComparisonStages(query string) string {
	query = strings.TrimSpace(query)
	if query == "" {
		return query
	}
	lq, err := logqlpkg.ParseLogQuery(query)
	if err != nil {
		result := stripFieldComparisonStgRE.ReplaceAllString(query, " ")
		for strings.Contains(result, "  ") {
			result = strings.ReplaceAll(result, "  ", " ")
		}
		return strings.TrimSpace(result)
	}
	out := filterPipeline(lq, func(s logqlpkg.Stage) bool {
		lfs, ok := s.(*logqlpkg.LabelFilterStage)
		if ok && labelFilterCompareRE.MatchString(lfs.Raw) {
			return false
		}
		return true
	})
	return out.String()
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
	// string is the widest type: once any value is non-numeric the field is string.
	if current == "string" || next == "string" {
		return "string"
	}
	// float is wider than int: int+float → float.
	if current == "float" || next == "float" {
		return "float"
	}
	return "string"
}

func parseLogfmtFields(line string) map[string]string {
	fields := make(map[string]string)
	start := 0
	inQuote := false
	for i := 0; i < len(line); i++ {
		c := line[i]
		if c == '"' {
			inQuote = !inQuote
			continue
		}
		// Loki's logfmt decoder ends a token at any whitespace byte, not only a
		// space: `n=1\tlevel=warn` yields both pairs.
		if c > ' ' || inQuote {
			continue
		}
		if i > start {
			parseLogfmtToken(line[start:i], fields)
		}
		start = i + 1
	}
	if start < len(line) {
		parseLogfmtToken(line[start:], fields)
	}
	return fields
}

func parseLogfmtToken(tok string, fields map[string]string) {
	eq := strings.IndexByte(tok, '=')
	if eq > 0 {
		key := strings.TrimSpace(tok[:eq])
		val := strings.Trim(strings.TrimSpace(tok[eq+1:]), `"`)
		if key != "" {
			fields[key] = val
		}
	}
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

// source paths (line scan, JSON/logfmt parse, type inference, sentinel
// stripping). Each branch handles a distinct parser-output edge case
// (numeric vs string vs hybrid values; missing/extra parsers; capped scan).
// Extracting would force shared mutable state through extra arguments.
//
//nolint:gocyclo // Scans + aggregates parsed-field samples across multiple
func (p *Proxy) detectFields(ctx context.Context, query, start, end string, lineLimit int) ([]map[string]interface{}, map[string][]string, error) {
	if lineLimit > p.limits().DetectedScanLines {
		lineLimit = p.limits().DetectedScanLines
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
		scanFieldList    []map[string]interface{}
		scanFieldValues  map[string][]string
		scanStreamLabels map[string]string
	)

	for _, candidate := range candidates {
		logsqlQuery, err := p.translateQuery(candidate)
		if err != nil {
			lastErr = err
			continue
		}

		params := url.Values{}
		// Detection only needs a sample of any matching lines for field
		// extraction — order is irrelevant. The previous explicit
		// `| sort by (_time desc)` forced VL to scan and order every matching
		// line before returning the top N, which on heavy parser-filter
		// queries (e.g. `| logfmt | level="error"`) blew past 10s.
		// Without the sort VL can early-exit once it has N matches.
		params.Set("query", logsqlQuery)
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
			lastErr = p.redactedBackendStatusError("", resp.StatusCode, errBody)
			hadScanFailure = true
			continue
		}
		if resp.StatusCode >= http.StatusBadRequest {
			errBody, _ := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			// Collect native result before returning so the goroutine doesn't leak.
			<-nativeCh
			return nil, nil, p.redactedBackendStatusError("", resp.StatusCode, errBody)
		}
		// Stream the NDJSON response line-by-line without buffering the full body.
		scanFieldList, scanFieldValues, scanStreamLabels = p.detectFieldSummariesStream(resp.Body)
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
		// Merge scan results with native index fields that passed the hit threshold in
		// fetchNativeFieldNamesForCandidate (hits ≥ minNativeFieldHits, top-N by hits).
		// The hit filter prevents VL's accumulated one-off JSON keys from flooding the
		// response — only fields with meaningful presence (e.g. trace_id, session_id)
		// get promoted. service.name / service_name are handled separately below so the
		// querySelectsServiceName guard is applied correctly.
		//
		// Guard: skip service.name / service_name from the general merge when the query
		// already selects on them — stream label selectors must not reappear as fields
		// (e.g. {service_name="api-gateway"} must not expose service_name as a field).
		querySelectsServiceName := strings.Contains(query, "service_name") || strings.Contains(query, "service.name")
		if nativeFields != nil && !querySelectsServiceName {
			if _, nativeHasServiceDot := nativeFields["service.name"]; nativeHasServiceDot {
				scanHas := func(label string) bool {
					for _, f := range scanFieldList {
						if l, _ := f["label"].(string); l == label {
							return true
						}
					}
					return false
				}
				if !scanHas("service.name") {
					scanFieldList = append(scanFieldList, map[string]interface{}{
						"label": "service.name", "type": "string",
						"cardinality": 1, "parsers": []string{},
					})
				}
				if !scanHas("service_name") {
					scanFieldList = append(scanFieldList, map[string]interface{}{
						"label": "service_name", "type": "string",
						"cardinality": 1, "parsers": []string{},
					})
				}
			}
		}
		// Build a filtered native map that excludes service.name/service_name (handled
		// above) so the general merge does not bypass the querySelectsServiceName guard.
		// Also apply the minNativeFieldHits threshold here (scan-success path only):
		// native fields with low hit counts are noise from VL's accumulated field index
		// and must not inflate the response when the log-line scan has results.
		// Fields with nativeHits==0 are VL non-indexed fields (valid, not noise) and pass.
		var nativeSparse map[string]*detectedFieldSummary
		if nativeFields != nil {
			nativeSparse = make(map[string]*detectedFieldSummary, len(nativeFields))
			for k, v := range nativeFields {
				if k == "service.name" || k == "service_name" {
					continue
				}
				if v.nativeHits > 0 && v.nativeHits < minNativeFieldHits {
					continue
				}
				// Suppress plain stream labels (e.g. container, env, pod, version, level)
				// that VL's field_names index returns alongside JSON body fields.
				// Stream labels belong in detected_labels, not detected_fields.
				// We only apply this filter when the scan returned a stream label set;
				// OTel dotted/dashed labels (service.name) are handled by the guard above.
				if len(scanStreamLabels) > 0 && !shouldExposeStructuredField(k, scanStreamLabels, p.labelTranslator) {
					continue
				}
				nativeSparse[k] = v
			}
		}
		fieldList, fieldValues := mergeNativeDetectedFields(scanFieldList, scanFieldValues, nativeSparse)
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

// isOTelLabels checks OTel heuristics given pre-parsed stream labels and raw
// _msg bytes. Callers that already have both avoid redundant parseStreamLabels.
func isOTelLabels(streamLabels map[string]string, msgBytes []byte) bool {
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
	if len(msgBytes) > 0 {
		if bytes.Contains(msgBytes, []byte("trace_id")) && bytes.Contains(msgBytes, []byte("span_id")) {
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
	fillDetectedLabelsFJWithStream(fjVal, stream, buf)
}

// fillDetectedLabelsFJWithStream is like fillDetectedLabelsFJ but accepts
// pre-parsed stream labels to avoid a redundant parseStreamLabels call.
func fillDetectedLabelsFJWithStream(fjVal *fj.Value, stream map[string]string, buf map[string]string) {
	for k := range buf {
		delete(buf, k)
	}
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
//
//nolint:gocyclo // streaming JSON scanner with per-line OTel/non-OTel detection, exposure caching, and label/field accumulation; branching is inherent to detected_fields parity.
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
	rowLevels := p.newLogRowLevels()

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

		rowStream := rowLevels.stream(fjVal.GetStringBytes("_stream"))
		rawStreamLabels := rowStream.labels
		msgBytes := fjVal.GetStringBytes("_msg")

		// Loki lists detected_level as a string field without parsers, with the
		// distinct derived values of the sampled lines.
		if rowObj, rowErr := fjVal.Object(); rowErr == nil {
			addDetectedField(fields, detectedLevelName(rawStreamLabels), "", "string", nil, rowLevels.fjRow(fjVal, rowObj, rowStream).String())
		}

		if isOTelLabels(rawStreamLabels, msgBytes) {
			if _, ok := rawStreamLabels["service.name"]; ok {
				anyOTelWithServiceName = true
			}
		}
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

		// Parse _msg JSON once: populate msgJSONKeys for the skip-guard below AND add
		// detected fields with parser="json" in the same pass.
		var msgJSONKeys map[string]struct{}
		if len(msgBytes) >= 2 && msgBytes[0] == '{' {
			fjParser2 := vlFJParserPool.Get()
			if fjVal2, err2 := fjParser2.ParseBytes(msgBytes); err2 == nil {
				if obj2, objErr2 := fjVal2.Object(); objErr2 == nil {
					msgJSONKeys = make(map[string]struct{})
					obj2.Visit(func(keyBytes []byte, v *fj.Value) {
						if len(keyBytes) == 0 {
							return
						}
						// Skip nested objects and arrays before allocating key string.
						vt := v.Type()
						if vt == fj.TypeObject || vt == fj.TypeArray {
							return
						}
						key := string(keyBytes)
						msgJSONKeys[key] = struct{}{}
						if shouldSuppressDetectedField(key) {
							return
						}
						// Loki names a parsed key by its sanitized label and
						// keeps the key itself in jsonPath (-logql-dotted-names).
						label := p.detectedFieldName(key)
						if _, conflict := labelNames[label]; conflict {
							return
						}
						addDetectedField(fields, label, "json", inferDetectedTypeFJ(v), []string{key}, formatDetectedValueFJ(v))
					})
				}
			}
			vlFJParserPool.Put(fjParser2)
		}

		obj, objErr := fjVal.Object()
		if objErr == nil {
			obj.Visit(func(keyBytes []byte, v *fj.Value) {
				// Use string(keyBytes) directly in map lookups — compiler avoids
				// allocation when the converted string is used only as a map key.
				if _, inMsg := msgJSONKeys[string(keyBytes)]; inMsg {
					return
				}
				key := string(keyBytes)
				if key == "app" || key == "cluster" || key == "namespace" {
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

		// msgBytes points into fjParser's buffer, which the next Parse by any
		// goroutine that takes this parser from the pool overwrites, so copy the
		// message before returning the parser.
		// Skip logfmt parsing when _msg is JSON — we already extracted fields
		// via the fastjson pass above and a logfmt scan would misparse JSON tokens.
		var msg string
		if len(msgBytes) > 0 && msgBytes[0] != '{' {
			msg = string(msgBytes)
		}
		vlFJParserPool.Put(fjParser)

		if msg == "" {
			continue
		}

		for key, value := range parseLogfmtFields(msg) {
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
		// Expose service_name as an alias for service.name when we observed OTel entries
		// that carry service.name as a stream key (the dotted OTel form).  We gate on the
		// presence of service.name in streamLabelSet rather than the absence of service_name,
		// because in hybrid datasets both keys may appear: OTel streams use service.name
		// while pre-normalised (Vector/FluentBit) streams use the underscore form.  Checking
		// service.name directly ensures the alias is exposed for the OTel entries even when
		// other streams in the same query carry a literal service_name stream label.
		// The only case where we skip the alias is when there are NO dotted service.name
		// entries at all — i.e. only non-OTel service_name stream labels, which already
		// appear in the labels panel and must not be duplicated in detected_fields.
		if _, hasServiceDot := streamLabelSet["service.name"]; hasServiceDot {
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
		// detected_level is not parsed from the line: Loki lists it with
		// parsers null.
		if len(summary.parsers) == 0 && !strings.ContainsAny(summary.label, ".") && summary.label != detectedLevelLabel && summary.label != detectedLevelExtractedLabel {
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
	if lineLimit > p.limits().DetectedScanLines {
		lineLimit = p.limits().DetectedScanLines
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
	for field, hits := range fieldNames {
		for _, exposure := range p.metadataFieldExposures(field) {
			if shouldSuppressDetectedField(exposure.name) {
				continue
			}
			out[exposure.name] = &detectedFieldSummary{
				label:       exposure.name,
				typ:         "string",
				values:      map[string]struct{}{},
				cardinality: 1,
				nativeHits:  hits,
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

// minNativeFieldHits is the minimum hit count a field must have in VL's column index
// to be promoted into detected_fields when the log-line scan missed it.
// Fields appearing fewer than 10 times are treated as ephemeral noise and excluded.
// This threshold prevents the VL field_names index (which accumulates every JSON key
// ever seen, including one-off debug keys) from flooding the detected_fields response.
// The test fixture uses hits=5 for artificial extra fields — they are intentionally
// below this threshold and will not be promoted (regression guard preserved).
const minNativeFieldHits = 10

// maxNativeFieldsToPromote caps the number of native-index fields added to the
// detected_fields response above what the log-line scan found. In environments
// with highly diverse log schemas the VL column index may contain many entries;
// promoting only the top-N by hit count keeps the field list manageable.
const maxNativeFieldsToPromote = 50

func (p *Proxy) fetchNativeFieldNamesForCandidate(ctx context.Context, candidate, start, end string) (map[string]int64, error) {
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
	// Use the full requested time range (no 5-min cap): field_names uses VL's column
	// index and is fast even on multi-day ranges. A wider window yields meaningful
	// hit counts so sparse fields (trace_id, session_id, span_id, etc.) accumulate
	// enough hits to pass the minNativeFieldHits threshold even when they don't appear
	// in the most-recent N log lines that the log-scan samples. Cap at 7d for safety.
	params = capMetadataStartOnly(params, 7*24*time.Hour)
	body, err := p.vlGetCoalesced(ctx, p.nativeCoalescerKey("native_fields", ctx, params), "/select/logsql/field_names", params)
	if err != nil {
		return nil, err
	}
	var resp vlFieldNamesResponse
	if err := stdjson.Unmarshal(body, &resp); err != nil {
		return nil, err
	}
	// Sort by hits descending so the most consistently-present fields are promoted first.
	sort.Slice(resp.Values, func(i, j int) bool {
		return resp.Values[i].Hits > resp.Values[j].Hits
	})
	// Return all fields (up to maxNativeFieldsToPromote) with their hit counts.
	// Callers that merge with scan results apply minNativeFieldHits; callers that use
	// native fields as a fallback (scan-empty, field resolution) use all fields.
	out := make(map[string]int64, min(len(resp.Values), maxNativeFieldsToPromote))
	for i, item := range resp.Values {
		if i >= maxNativeFieldsToPromote {
			break
		}
		out[item.Value] = item.Hits
	}
	return out, nil
}

func (p *Proxy) fetchNativeFieldNames(ctx context.Context, query, start, end string) (map[string]int64, error) {
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

//nolint:gocyclo // tries multiple candidate queries with relaxation fallbacks, dedup, and limit handling; branching is inherent to native field-values discovery.
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
			lastErr = p.redactedBackendStatusError("", resp.StatusCode, body)
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
			unpackQuery := logsqlQuery + " " + logsql.PipeUnpackJSON{From: "_msg"}.String() + " " + logsql.PipeUnpackLogfmt{From: "_msg"}.String()
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
	unpackQuery := logsqlQuery + " " + logsql.PipeUnpackJSON{From: "_msg"}.String() + " " + logsql.PipeUnpackLogfmt{From: "_msg"}.String()
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
		fieldNamesMap, err := p.fetchNativeFieldNamesForCandidate(ctx, candidate, start, end)
		if err != nil {
			lastErr = err
			continue
		}
		fieldCandidates := make([]string, 0, len(fieldNamesMap))
		for name := range fieldNamesMap {
			fieldCandidates = append(fieldCandidates, name)
		}
		sort.Strings(fieldCandidates)
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
	// VL v1.50+: stream label values are in the column index.
	// Replace O(stream-scan) streams call (~1s) with stream_field_names (~300ms)
	// + parallel field_values (~40ms each) for a ~3x speedup on detected_labels.
	if p.supportsColumnIndexedFields() {
		if summaries, err := p.detectNativeLabelsViaFieldValues(ctx, query, start, end); err == nil {
			return summaries, nil
		}
	}
	parsed, err := p.fetchNativeStreams(ctx, query, start, end)
	if err != nil {
		return nil, err
	}
	summaries := map[string]*detectedLabelSummary{}
	for _, item := range parsed.Values {
		labels := parseStreamLabels(item.Value)
		ensureSyntheticServiceName(labels)
		for key, value := range labels {
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

// detectNativeLabelsViaFieldValues fetches stream label names via stream_field_names
// then their values in parallel via field_values. Returns only stream labels (not all
// indexed fields) to match Loki's detected_labels semantics.
func (p *Proxy) detectNativeLabelsViaFieldValues(ctx context.Context, query, start, end string) (map[string]*detectedLabelSummary, error) {
	var labelNames []string
	var baseParams url.Values
	for _, candidate := range fieldDetectionQueryCandidates(query) {
		logsqlQuery, err := p.translateQuery(candidate)
		if err != nil {
			continue
		}
		params := url.Values{}
		params.Set("query", logsqlQuery)
		if start != "" {
			params.Set("start", start)
		}
		if end != "" {
			params.Set("end", end)
		}
		// detected_labels samples recent data: discover names over the same capped
		// window its values are fetched from.
		capped := capMetadataStartOnly(params, metadataMaxFieldNamesWindow)
		names, err := p.fetchStreamFieldNamesCached(ctx, capped)
		if err != nil || len(names) == 0 {
			continue
		}
		labelNames = names
		baseParams = capped
		break
	}
	if len(labelNames) == 0 {
		return nil, fmt.Errorf("no stream label names found")
	}

	type labelResult struct {
		lokiLabel string
		values    []string
	}
	resultsCh := make(chan labelResult, len(labelNames))

	var wg sync.WaitGroup
	sem := make(chan struct{}, 8)
	for _, name := range labelNames {
		lokiLabel := p.labelTranslator.ToLoki(name)
		if lokiLabel == "" {
			continue
		}
		wg.Add(1)
		go func(vlName, lokiName string) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			queryParams := cloneURLValues(baseParams)
			queryParams.Set("field", vlName)
			vals, err := p.fetchVLFieldValues(ctx, "/select/logsql/field_values", queryParams)
			if err != nil || len(vals) == 0 {
				return
			}
			resultsCh <- labelResult{lokiLabel: lokiName, values: vals}
		}(name, lokiLabel)
	}
	wg.Wait()
	close(resultsCh)

	summaries := make(map[string]*detectedLabelSummary, len(labelNames))
	for r := range resultsCh {
		summary := &detectedLabelSummary{
			label:  r.lokiLabel,
			values: make(map[string]struct{}, len(r.values)),
		}
		for _, v := range r.values {
			summary.values[v] = struct{}{}
		}
		summaries[r.lokiLabel] = summary
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
		// No | sort pipe: the /streams endpoint uses column-index lookups. Adding
		// | sort by (_time desc) forces VL to sort all matching log entries before
		// grouping into streams — an O(N_logs) heap sort that OOMs at wide ranges.
		params.Set("query", logsqlQuery)
		if start != "" {
			params.Set("start", start)
		}
		if end != "" {
			params.Set("end", end)
		}
		params = capMetadataStartOnly(params, metadataMaxFieldNamesWindow)
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
			lastErr = p.redactedBackendStatusError("", resp.StatusCode, errBody)
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

// detectedFieldsRefreshKey is a context key set by refreshDetectedFieldsCacheAsync
// to bypass the inner detected-fields cache (getCachedDetectedFields). Without this
// bypass, background refresh re-uses the stale cached result from the original request
// (same raw start/end params) and never queries VL for fresh data.
type detectedFieldsRefreshKey struct{}

func (p *Proxy) getCachedDetectedFields(ctx context.Context, query, start, end string, lineLimit int) ([]map[string]interface{}, map[string][]string, bool) {
	if p.cache == nil {
		return nil, nil, false
	}
	// Background refresh must bypass the inner cache so it fetches fresh data from VL.
	if ctx.Value(detectedFieldsRefreshKey{}) != nil {
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
	// Never cache an empty result. VL may legitimately return no data when
	// ingestion is just starting, when the time window pre-dates any logs,
	// or during a transient backend hiccup; caching that response makes
	// Drilldown show "No data" for the whole TTL even after data arrives.
	if len(fields) == 0 && len(values) == 0 {
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
	// Same negative-cache guard as setCachedDetectedFields: an empty
	// labels/summaries response shouldn't be persisted, or Drilldown will
	// keep showing "No data" for the full 30s TTL even after data ingests.
	if len(labels) == 0 && len(summaries) == 0 {
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
