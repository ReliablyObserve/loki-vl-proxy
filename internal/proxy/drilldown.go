package proxy

import (
	"context"
	"encoding/json"
	"io"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

const unknownServiceName = "unknown_service"
const detectedFieldsSampleLimit = 500

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

func buildEntryLabels(entry map[string]interface{}) map[string]string {
	labels := parseStreamLabels(asString(entry["_stream"]))
	for key, value := range entry {
		if isVLInternalField(key) || key == "_stream_id" {
			continue
		}
		if s, ok := value.(string); ok && strings.TrimSpace(s) != "" {
			labels[key] = s
		}
	}
	ensureDetectedLevel(labels)
	ensureSyntheticServiceName(labels)
	return labels
}

func buildDetectedLabels(entry map[string]interface{}) map[string]string {
	labels := parseStreamLabels(asString(entry["_stream"]))
	if value, ok := stringifyEntryValue(entry["level"]); ok && strings.TrimSpace(value) != "" {
		labels["level"] = value
	}
	ensureDetectedLevel(labels)
	ensureSyntheticServiceName(labels)
	return labels
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

func scanDetectedLabels(body []byte, lt *LabelTranslator) ([]map[string]interface{}, map[string]struct{}) {
	summaries := scanDetectedLabelSummaries(body, lt)

	labelSet := make(map[string]struct{}, len(summaries))
	result := formatDetectedLabelSummaries(summaries)
	for _, item := range result {
		if label, _ := item["label"].(string); label != "" {
			labelSet[label] = struct{}{}
		}
	}
	return result, labelSet
}

func scanDetectedLabelSummaries(body []byte, lt *LabelTranslator) map[string]*detectedLabelSummary {
	summaries := map[string]*detectedLabelSummary{}

	startIdx := 0
	for i := 0; i <= len(body); i++ {
		if i < len(body) && body[i] != '\n' {
			continue
		}
		line := strings.TrimSpace(string(body[startIdx:i]))
		startIdx = i + 1
		if line == "" {
			continue
		}

		var entry map[string]interface{}
		if err := json.Unmarshal([]byte(line), &entry); err != nil {
			continue
		}

		labels := buildDetectedLabels(entry)
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
		delete(summaries, "service_name")
	}

	names := make([]string, 0, len(summaries))
	for label := range summaries {
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
	logsqlQuery, err := p.translateQuery(defaultQuery(query))
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
		return nil, err
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
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
	values := make([]string, 0, len(vlResp.Values))
	for _, item := range vlResp.Values {
		labels := parseStreamLabels(item.Value)
		serviceName := deriveServiceName(labels)
		if _, ok := seen[serviceName]; ok {
			continue
		}
		seen[serviceName] = struct{}{}
		values = append(values, serviceName)
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
	for i := 0; i <= len(content); i++ {
		if i < len(content) {
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
		}
		part := strings.TrimSpace(content[start:i])
		if part != "" {
			firstMatcher = part
			break
		}
		start = i + 1
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

	targets := splitTargetLabels(targetLabels)
	params := url.Values{}
	params.Set("query", logsqlQuery+" | sort by (_time desc)")
	params.Set("limit", strconv.Itoa(maxLimitValue))
	if start != "" {
		params.Set("start", formatVLTimestamp(start))
	}
	if end != "" {
		params.Set("end", formatVLTimestamp(end))
	}

	resp, err := p.vlPost(ctx, "/select/logsql/query", params)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
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

	startIdx := 0
	for i := 0; i <= len(body); i++ {
		if i < len(body) && body[i] != '\n' {
			continue
		}
		line := strings.TrimSpace(string(body[startIdx:i]))
		startIdx = i + 1
		if line == "" {
			continue
		}

		var entry map[string]interface{}
		if err := json.Unmarshal([]byte(line), &entry); err != nil {
			continue
		}

		entryLabels := buildEntryLabels(entry)
		translated := p.labelTranslator.TranslateLabelsMap(entryLabels)
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
		item.total++

		if matrixMode {
			entryTime, ok := parseEntryTime(entry["_time"])
			if !ok {
				continue
			}
			if hasStart && entryTime.Before(startTime) {
				continue
			}
			if hasEnd && entryTime.After(endTime) {
				continue
			}
			if hasStep {
				offset := entryTime.Sub(startTime)
				if offset < 0 {
					continue
				}
				bucketTime := startTime.Add((offset / stepDur) * stepDur).Unix()
				item.bucket[bucketTime]++
			}
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

	query = dropStage.ReplaceAllString(query, " ")
	query = parserStage.ReplaceAllString(query, " ")
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
	logsqlQuery, err := p.translateQuery(defaultFieldDetectionQuery(query))
	if err != nil {
		return nil, nil, err
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
		return nil, nil, err
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	fieldList, fieldValues := p.detectFieldsFromBody(body)
	fieldList, fieldValues = mergeNativeDetectedFields(fieldList, fieldValues, nativeFields)
	p.setCachedDetectedFields(ctx, query, start, end, lineLimit, fieldList, fieldValues)
	return fieldList, fieldValues, nil
}

func (p *Proxy) detectFieldsFromBody(body []byte) ([]map[string]interface{}, map[string][]string) {
	fieldList, fieldValues, _ := p.detectFieldSummaries(body)
	return fieldList, fieldValues
}

func (p *Proxy) detectFieldSummaries(body []byte) ([]map[string]interface{}, map[string][]string, map[string]*detectedFieldSummary) {
	_, labelNames := scanDetectedLabels(body, p.labelTranslator)
	fields := make(map[string]*detectedFieldSummary)

	startIdx := 0
	for i := 0; i <= len(body); i++ {
		if i < len(body) && body[i] != '\n' {
			continue
		}
		line := strings.TrimSpace(string(body[startIdx:i]))
		startIdx = i + 1
		if line == "" {
			continue
		}

		var entry map[string]interface{}
		if err := json.Unmarshal([]byte(line), &entry); err != nil {
			continue
		}
		streamLabels := buildDetectedLabels(entry)
		if detectedLevel := strings.TrimSpace(streamLabels["detected_level"]); detectedLevel != "" {
			addDetectedField(fields, "detected_level", "", "string", nil, detectedLevel)
		}

		for key, value := range entry {
			if !shouldExposeStructuredField(key, streamLabels, p.labelTranslator) {
				continue
			}
			stringValue, ok := stringifyEntryValue(value)
			if !ok {
				continue
			}
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
				for _, exposure := range p.metadataFieldExposures(key) {
					if _, conflict := labelNames[exposure.name]; conflict && !exposure.isAlias {
						continue
					}
					addDetectedField(fields, exposure.name, "json", inferDetectedType(value), []string{key}, formatDetectedValue(value))
				}
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
			for _, exposure := range p.metadataFieldExposures(key) {
				if _, conflict := labelNames[exposure.name]; conflict && !exposure.isAlias {
					continue
				}
				addDetectedField(fields, exposure.name, "logfmt", inferDetectedType(value), nil, value)
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
	_, ok := summaries["level"]
	return !ok
}

func mergeDetectedLabelSupplements(dst, scanned map[string]*detectedLabelSummary) {
	for _, label := range []string{"level"} {
		if _, ok := dst[label]; ok {
			continue
		}
		if summary := scanned[label]; summary != nil {
			dst[label] = summary
		}
	}
}

func (p *Proxy) detectNativeFields(ctx context.Context, query, start, end string) (map[string]*detectedFieldSummary, error) {
	fieldNames, err := p.fetchNativeFieldNames(ctx, query, start, end)
	if err != nil {
		return nil, err
	}
	out := make(map[string]*detectedFieldSummary, len(fieldNames))
	for _, field := range fieldNames {
		streamLabels := map[string]string{field: "1"}
		if !shouldExposeStructuredField(field, streamLabels, p.labelTranslator) {
			continue
		}
		for _, exposure := range p.metadataFieldExposures(field) {
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

func (p *Proxy) fetchNativeFieldNames(ctx context.Context, query, start, end string) ([]string, error) {
	logsqlQuery, err := p.translateQuery(defaultFieldDetectionQuery(query))
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
	body, err := p.vlGetCoalesced(ctx, "native_fields:"+params.Encode(), "/select/logsql/field_names", params)
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

func (p *Proxy) fetchNativeFieldValues(ctx context.Context, query, start, end, field string, limit int) ([]string, error) {
	logsqlQuery, err := p.translateQuery(defaultFieldDetectionQuery(query))
	if err != nil {
		return nil, err
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
		return nil, err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	var parsed vlFieldValuesResponse
	if err := json.Unmarshal(body, &parsed); err != nil {
		return nil, err
	}
	values := make([]string, 0, len(parsed.Values))
	for _, item := range parsed.Values {
		values = append(values, item.Value)
	}
	sort.Strings(values)
	return values, nil
}

func (p *Proxy) resolveNativeDetectedField(ctx context.Context, query, start, end, fieldName string) (string, bool, error) {
	fieldNames, err := p.fetchNativeFieldNames(ctx, query, start, end)
	if err != nil {
		return "", false, err
	}
	candidates := make([]string, 0, len(fieldNames))
	for _, field := range fieldNames {
		streamLabels := map[string]string{field: "1"}
		if shouldExposeStructuredField(field, streamLabels, p.labelTranslator) {
			candidates = append(candidates, field)
		}
	}
	resolution := p.labelTranslator.ResolveMetadataCandidates(fieldName, candidates, p.metadataFieldMode)
	if len(resolution.candidates) == 1 {
		return resolution.candidates[0], true, nil
	}
	return "", false, nil
}

func (p *Proxy) detectNativeLabels(ctx context.Context, query, start, end string) (map[string]*detectedLabelSummary, error) {
	logsqlQuery, err := p.translateQuery(defaultFieldDetectionQuery(query))
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
		return nil, err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	var parsed vlStreamsResponse
	if err := json.Unmarshal(body, &parsed); err != nil {
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

func (p *Proxy) detectScannedLabels(ctx context.Context, query, start, end string, lineLimit int) (map[string]*detectedLabelSummary, error) {
	logsqlQuery, err := p.translateQuery(defaultQuery(query))
	if err != nil {
		return nil, err
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
		return nil, err
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	return scanDetectedLabelSummaries(body, p.labelTranslator), nil
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
	return "detect_fields:" + getOrgID(ctx) + ":" + query + ":" + start + ":" + end + ":" + strconv.Itoa(lineLimit)
}

func (p *Proxy) detectedLabelsCacheKey(ctx context.Context, query, start, end string, lineLimit int) string {
	return "detect_labels:" + getOrgID(ctx) + ":" + query + ":" + start + ":" + end + ":" + strconv.Itoa(lineLimit)
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
