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

	labelSet := make(map[string]struct{}, len(summaries))
	result := make([]map[string]interface{}, 0, len(summaries))
	if summary := summaries["service_name"]; summary != nil {
		result = append(result, map[string]interface{}{
			"label":       summary.label,
			"cardinality": len(summary.values),
		})
		labelSet[summary.label] = struct{}{}
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
		labelSet[summary.label] = struct{}{}
	}

	return result, labelSet
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

func (p *Proxy) volumeByServiceName(ctx context.Context, query, start, end string) (map[string]interface{}, error) {
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

	grouped := make(map[string]int64, len(vlResp.Values))
	for _, item := range vlResp.Values {
		labels := parseStreamLabels(item.Value)
		grouped[deriveServiceName(labels)] += item.Hits
	}

	names := make([]string, 0, len(grouped))
	for name := range grouped {
		names = append(names, name)
	}
	sort.Strings(names)

	nowTS := float64(time.Now().Unix())
	result := make([]map[string]interface{}, 0, len(names))
	for _, name := range names {
		result = append(result, map[string]interface{}{
			"metric": map[string]string{"service_name": name},
			"value":  []interface{}{nowTS, strconv.FormatInt(grouped[name], 10)},
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
	if strings.TrimSpace(query) == "" {
		return "*"
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

func (p *Proxy) detectFields(ctx context.Context, query, start, end string, lineLimit int) ([]map[string]interface{}, map[string][]string, error) {
	logsqlQuery, err := p.translateQuery(stripFieldDetectionStages(defaultQuery(query)))
	if err != nil {
		return nil, nil, err
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
		return nil, nil, err
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
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
			addDetectedField(fields, key, "", inferDetectedType(value), nil, stringValue)
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
				label := p.labelTranslator.ToLoki(key)
				if _, conflict := labelNames[label]; conflict {
					label += "_extracted"
				}
				addDetectedField(fields, label, "json", inferDetectedType(value), []string{key}, formatDetectedValue(value))
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
			label := p.labelTranslator.ToLoki(key)
			if _, conflict := labelNames[label]; conflict {
				label += "_extracted"
			}
			addDetectedField(fields, label, "logfmt", inferDetectedType(value), nil, value)
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

	return fieldList, fieldValues, nil
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
