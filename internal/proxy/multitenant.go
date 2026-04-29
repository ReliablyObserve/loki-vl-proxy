package proxy

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

func (p *Proxy) handleMultiTenantFanout(w http.ResponseWriter, r *http.Request, endpoint string, single func(http.ResponseWriter, *http.Request)) bool {
	orgID := strings.TrimSpace(r.Header.Get("X-Scope-OrgID"))
	if !hasMultiTenantOrgID(orgID) {
		return false
	}
	tenantIDs := splitMultiTenantOrgIDs(orgID)
	if len(tenantIDs) < 2 {
		return false
	}
	if len(tenantIDs) > maxMultiTenantFanout {
		p.writeError(w, http.StatusBadRequest, fmt.Sprintf("multi-tenant fanout exceeds limit of %d tenants", maxMultiTenantFanout))
		return true
	}
	filteredReq, filteredTenants, err := p.applyTenantSelectorFilter(r, tenantIDs)
	if err != nil {
		p.writeError(w, http.StatusBadRequest, err.Error())
		return true
	}
	if len(filteredTenants) == 0 {
		p.writeJSON(w, emptyMultiTenantResponse(endpoint))
		return true
	}

	if cacheKey, cacheable := p.multiTenantCacheKey(filteredReq, endpoint); cacheable {
		if cached, ok := p.cache.Get(cacheKey); ok {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write(cached)
			return true
		}
	}

	switch endpoint {
	case "detected_fields":
		body, contentType, err := p.multiTenantDetectedFieldsResponse(filteredReq, filteredTenants)
		if err != nil {
			p.writeError(w, http.StatusInternalServerError, "failed to merge multi-tenant response: "+err.Error())
			return true
		}
		w.Header().Set("Content-Type", contentType)
		_, _ = w.Write(body)
		if cacheKey, cacheable := p.multiTenantCacheKey(filteredReq, endpoint); cacheable {
			p.cache.SetWithTTL(cacheKey, body, CacheTTLs[endpoint])
		}
		return true
	case "detected_labels":
		body, contentType, err := p.multiTenantDetectedLabelsResponse(filteredReq, filteredTenants)
		if err != nil {
			p.writeError(w, http.StatusInternalServerError, "failed to merge multi-tenant response: "+err.Error())
			return true
		}
		w.Header().Set("Content-Type", contentType)
		_, _ = w.Write(body)
		if cacheKey, cacheable := p.multiTenantCacheKey(filteredReq, endpoint); cacheable {
			p.cache.SetWithTTL(cacheKey, body, CacheTTLs[endpoint])
		}
		return true
	}

	recorders := make([]*httptest.ResponseRecorder, 0, len(filteredTenants))
	for _, tenantID := range filteredTenants {
		subReq := filteredReq.Clone(filteredReq.Context())
		subReq.Header = filteredReq.Header.Clone()
		subReq.Header.Set("X-Scope-OrgID", tenantID)

		rec := httptest.NewRecorder()
		single(rec, subReq)
		if rec.Code >= 400 {
			copyHeaders(w.Header(), rec.Header())
			if w.Header().Get("Content-Type") == "" {
				w.Header().Set("Content-Type", "application/json")
			}
			w.WriteHeader(rec.Code)
			_, _ = w.Write(rec.Body.Bytes())
			return true
		}
		recorders = append(recorders, rec)
	}

	body, contentType, err := mergeMultiTenantResponses(endpoint, filteredTenants, recorders)
	if err != nil {
		p.writeError(w, http.StatusInternalServerError, "failed to merge multi-tenant response: "+err.Error())
		return true
	}
	if len(body) > maxMultiTenantMergedResponseBytes {
		p.writeError(w, http.StatusRequestEntityTooLarge, "multi-tenant merged response exceeds configured safety limit")
		return true
	}
	if contentType == "" {
		contentType = "application/json"
	}
	w.Header().Set("Content-Type", contentType)
	_, _ = w.Write(body)
	if cacheKey, cacheable := p.multiTenantCacheKey(filteredReq, endpoint); cacheable {
		p.cache.SetWithTTL(cacheKey, body, CacheTTLs[endpoint])
	}
	return true
}

type lokiStringListResponse struct {
	Status string   `json:"status"`
	Data   []string `json:"data"`
}

type lokiSeriesResponse struct {
	Status string              `json:"status"`
	Data   []map[string]string `json:"data"`
}

type lokiQueryResponse struct {
	Status string `json:"status"`
	Data   struct {
		ResultType    string          `json:"resultType"`
		Result        json.RawMessage `json:"result"`
		Stats         json.RawMessage `json:"stats,omitempty"`
		EncodingFlags []string        `json:"encodingFlags,omitempty"`
	} `json:"data"`
}

type lokiStreamResult struct {
	Stream map[string]string `json:"stream"`
	Values [][]interface{}   `json:"values"`
}

type lokiVectorResult struct {
	Metric map[string]string `json:"metric"`
	Value  []interface{}     `json:"value"`
}

type lokiMatrixResult struct {
	Metric map[string]string `json:"metric"`
	Values [][]interface{}   `json:"values"`
}

type detectedFieldResponse struct {
	Status string                `json:"status"`
	Data   []detectedFieldRecord `json:"data"`
	Fields []detectedFieldRecord `json:"fields"`
}

type detectedFieldRecord struct {
	Label       string        `json:"label"`
	Type        string        `json:"type"`
	Cardinality int           `json:"cardinality"`
	Parsers     []string      `json:"parsers"`
	JSONPath    []interface{} `json:"jsonPath,omitempty"`
}

type detectedFieldValuesResponse struct {
	Status string   `json:"status"`
	Data   []string `json:"data"`
	Values []string `json:"values"`
}

type detectedLabelResponse struct {
	Status         string                `json:"status"`
	Data           []detectedLabelRecord `json:"data"`
	DetectedLabels []detectedLabelRecord `json:"detectedLabels"`
}

type detectedLabelRecord struct {
	Label       string `json:"label"`
	Cardinality int    `json:"cardinality"`
}

type patternsResponse struct {
	Status string               `json:"status"`
	Data   []patternResultEntry `json:"data"`
}

type patternResultEntry struct {
	Pattern string          `json:"pattern"`
	Level   string          `json:"level,omitempty"`
	Samples [][]interface{} `json:"samples"`
}

func parseDetectedLineLimit(r *http.Request) int {
	lineLimit := 1000
	if value := strings.TrimSpace(r.FormValue("line_limit")); value != "" {
		if n, err := strconv.Atoi(value); err == nil && n > 0 {
			lineLimit = n
		}
	}
	if value := strings.TrimSpace(r.FormValue("limit")); value != "" {
		if n, err := strconv.Atoi(value); err == nil && n > 0 {
			lineLimit = n
		}
	}
	return lineLimit
}

func (p *Proxy) multiTenantCacheKey(r *http.Request, endpoint string) (string, bool) {
	if r.Method != http.MethodGet {
		return "", false
	}
	if endpoint == "patterns" || endpoint == "index_stats" || endpoint == "volume" || endpoint == "volume_range" || endpoint == "detected_labels" || endpoint == "detected_fields" || endpoint == "detected_field_values" || endpoint == "series" || endpoint == "labels" || endpoint == "label_values" || endpoint == "query" || endpoint == "query_range" {
		key := "mt:" + endpoint + ":" + r.Header.Get("X-Scope-OrgID") + ":" + r.URL.RawQuery
		switch endpoint {
		case "labels", "index_stats", "volume", "volume_range", "detected_labels", "detected_fields":
			key = "mt:" + p.canonicalReadCacheKey(endpoint, r.Header.Get("X-Scope-OrgID"), r)
		case "label_values":
			parts := strings.Split(r.URL.Path, "/")
			if len(parts) >= 7 {
				key = "mt:" + p.canonicalReadCacheKey(endpoint, r.Header.Get("X-Scope-OrgID"), r, parts[5])
			}
		case "detected_field_values":
			parts := strings.Split(r.URL.Path, "/")
			for i, part := range parts {
				if part == "detected_field" && i+1 < len(parts) {
					key = "mt:" + p.canonicalReadCacheKey(endpoint, r.Header.Get("X-Scope-OrgID"), r, parts[i+1])
					break
				}
			}
		case "patterns", "series":
			key = "mt:" + p.canonicalReadCacheKey(endpoint, r.Header.Get("X-Scope-OrgID"), r)
		}
		if endpoint == "query" || endpoint == "query_range" {
			key = "mt:" + p.canonicalReadCacheKey(endpoint, r.Header.Get("X-Scope-OrgID"), r, p.tupleModeCacheKey(r))
		}
		return key, true
	}
	return "", false
}

func (p *Proxy) tupleModeCacheKey(r *http.Request) string {
	categorizedLabels := requestWantsCategorizedLabels(r)
	if p.emitStructuredMetadata && categorizedLabels {
		return "categorize_labels_3tuple"
	}
	return "default_2tuple"
}

func emptyMultiTenantResponse(endpoint string) map[string]interface{} {
	switch endpoint {
	case "labels", "label_values":
		return map[string]interface{}{"status": "success", "data": []string{}}
	case "series":
		return map[string]interface{}{"status": "success", "data": []interface{}{}}
	case "query":
		return map[string]interface{}{"status": "success", "data": map[string]interface{}{"resultType": "streams", "result": []interface{}{}, "stats": map[string]interface{}{}}}
	case "query_range":
		return map[string]interface{}{"status": "success", "data": map[string]interface{}{"resultType": "streams", "result": []interface{}{}, "stats": map[string]interface{}{}}}
	case "index_stats":
		return map[string]interface{}{"streams": 0, "chunks": 0, "bytes": 0, "entries": 0}
	case "volume":
		return map[string]interface{}{"status": "success", "data": map[string]interface{}{"resultType": "vector", "result": []interface{}{}}}
	case "volume_range":
		return map[string]interface{}{"status": "success", "data": map[string]interface{}{"resultType": "matrix", "result": []interface{}{}}}
	case "detected_fields":
		return map[string]interface{}{"status": "success", "data": []interface{}{}, "fields": []interface{}{}}
	case "detected_field_values":
		return map[string]interface{}{"status": "success", "data": []string{}, "values": []string{}}
	case "detected_labels":
		return map[string]interface{}{"status": "success", "data": []interface{}{}, "detectedLabels": []interface{}{}}
	case "patterns":
		return map[string]interface{}{"status": "success", "data": []interface{}{}}
	default:
		return map[string]interface{}{"status": "success"}
	}
}

func (p *Proxy) applyTenantSelectorFilter(r *http.Request, tenantIDs []string) (*http.Request, []string, error) {
	filteredReq := r.Clone(r.Context())
	filteredReq.Header = r.Header.Clone()
	queryValues := filteredReq.URL.Query()

	switch {
	case queryValues.Get("query") != "":
		query, filtered, err := filterTenantMatchers(queryValues.Get("query"), tenantIDs)
		if err != nil {
			return nil, nil, err
		}
		queryValues.Set("query", query)
		filteredReq.URL.RawQuery = queryValues.Encode()
		filteredReq.Form = queryValues
		filteredReq.PostForm = queryValues
		filteredReq.Header.Set("X-Scope-OrgID", strings.Join(filtered, "|"))
		return filteredReq, filtered, nil
	case len(queryValues["match[]"]) > 0:
		filtered := tenantIDs
		matchers := queryValues["match[]"]
		for i, expr := range matchers {
			updated, narrowed, err := filterTenantMatchers(expr, filtered)
			if err != nil {
				return nil, nil, err
			}
			matchers[i] = updated
			filtered = narrowed
		}
		queryValues["match[]"] = matchers
		filteredReq.URL.RawQuery = queryValues.Encode()
		filteredReq.Form = queryValues
		filteredReq.PostForm = queryValues
		filteredReq.Header.Set("X-Scope-OrgID", strings.Join(filtered, "|"))
		return filteredReq, filtered, nil
	default:
		return filteredReq, tenantIDs, nil
	}
}

func filterTenantMatchers(query string, tenantIDs []string) (string, []string, error) {
	query = strings.TrimSpace(query)
	if query == "" {
		return query, tenantIDs, nil
	}
	selector, rest, ok := splitLeadingSelector(query)
	if !ok {
		return query, tenantIDs, nil
	}
	matchers := splitSelectorMatchers(selector[1 : len(selector)-1])
	filteredMatchers := make([]string, 0, len(matchers))
	filteredTenants := append([]string(nil), tenantIDs...)
	for _, matcher := range matchers {
		nextTenants, handled, err := filterTenantsByMatcher(filteredTenants, matcher)
		if err != nil {
			return "", nil, err
		}
		if handled {
			filteredTenants = nextTenants
			continue
		}
		filteredMatchers = append(filteredMatchers, matcher)
	}
	var rebuilt string
	switch {
	case len(filteredMatchers) == 0 && strings.TrimSpace(rest) == "":
		rebuilt = "*"
	case len(filteredMatchers) == 0:
		rebuilt = "* " + strings.TrimSpace(rest)
	case strings.TrimSpace(rest) == "":
		rebuilt = "{" + strings.Join(filteredMatchers, ",") + "}"
	default:
		rebuilt = "{" + strings.Join(filteredMatchers, ",") + "} " + strings.TrimSpace(rest)
	}
	return strings.TrimSpace(rebuilt), filteredTenants, nil
}

func splitLeadingSelector(query string) (selector, rest string, ok bool) {
	query = strings.TrimSpace(query)
	if query == "" || query[0] != '{' {
		return "", "", false
	}
	end := findMatchingBraceLocal(query)
	if end < 0 {
		return "", "", false
	}
	return query[:end+1], strings.TrimSpace(query[end+1:]), true
}

func findMatchingBraceLocal(s string) int {
	depth := 0
	inQuote := false
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '\\':
			if inQuote && i+1 < len(s) {
				i++
			}
		case '"':
			inQuote = !inQuote
		case '{':
			if !inQuote {
				depth++
			}
		case '}':
			if !inQuote {
				depth--
				if depth == 0 {
					return i
				}
			}
		}
	}
	return -1
}

func splitSelectorMatchers(s string) []string {
	var matchers []string
	inQuote := false
	start := 0
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '\\':
			if inQuote && i+1 < len(s) {
				i++
			}
		case '"':
			inQuote = !inQuote
		case ',':
			if !inQuote {
				part := strings.TrimSpace(s[start:i])
				if part != "" {
					matchers = append(matchers, part)
				}
				start = i + 1
			}
		}
	}
	part := strings.TrimSpace(s[start:])
	if part != "" {
		matchers = append(matchers, part)
	}
	return matchers
}

func filterTenantsByMatcher(tenantIDs []string, matcher string) ([]string, bool, error) {
	for _, op := range []string{"!~", "=~", "!=", "="} {
		if idx := strings.Index(matcher, op); idx > 0 {
			label := strings.TrimSpace(matcher[:idx])
			if label != "__tenant_id__" {
				return tenantIDs, false, nil
			}
			raw := strings.TrimSpace(matcher[idx+len(op):])
			raw = strings.Trim(raw, "\"`")
			return applyTenantMatch(tenantIDs, op, raw)
		}
	}
	return tenantIDs, false, nil
}

func applyTenantMatch(tenantIDs []string, op, raw string) ([]string, bool, error) {
	out := make([]string, 0, len(tenantIDs))
	switch op {
	case "=":
		for _, tenantID := range tenantIDs {
			if tenantID == raw {
				out = append(out, tenantID)
			}
		}
	case "!=":
		for _, tenantID := range tenantIDs {
			if tenantID != raw {
				out = append(out, tenantID)
			}
		}
	case "=~":
		re, err := regexp.Compile(raw)
		if err != nil {
			return nil, true, fmt.Errorf("invalid __tenant_id__ regex: %w", err)
		}
		for _, tenantID := range tenantIDs {
			if re.MatchString(tenantID) {
				out = append(out, tenantID)
			}
		}
	case "!~":
		re, err := regexp.Compile(raw)
		if err != nil {
			return nil, true, fmt.Errorf("invalid __tenant_id__ regex: %w", err)
		}
		for _, tenantID := range tenantIDs {
			if !re.MatchString(tenantID) {
				out = append(out, tenantID)
			}
		}
	default:
		return tenantIDs, false, nil
	}
	return out, true, nil
}

func mergeMultiTenantResponses(endpoint string, tenantIDs []string, recorders []*httptest.ResponseRecorder) ([]byte, string, error) {
	switch endpoint {
	case "labels":
		values := make([]string, 0)
		for _, rec := range recorders {
			var resp lokiStringListResponse
			if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
				return nil, "", err
			}
			values = append(values, resp.Data...)
		}
		values = append(values, "__tenant_id__")
		return lokiLabelsResponse(uniqueSortedStrings(values)), "application/json", nil
	case "label_values":
		values := make([]string, 0)
		for _, rec := range recorders {
			var resp lokiStringListResponse
			if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
				return nil, "", err
			}
			values = append(values, resp.Data...)
		}
		return lokiLabelsResponse(uniqueSortedStrings(values)), "application/json", nil
	case "series":
		return mergeSeriesResponses(tenantIDs, recorders)
	case "query", "query_range", "volume", "volume_range":
		return mergeLokiQueryResponses(tenantIDs, recorders)
	case "index_stats":
		return mergeIndexStatsResponses(recorders)
	case "detected_fields":
		return mergeDetectedFieldsResponses(recorders)
	case "detected_field_values":
		return mergeDetectedFieldValuesResponses(recorders)
	case "detected_labels":
		return mergeDetectedLabelsResponses(tenantIDs, recorders)
	case "patterns":
		return mergePatternsResponses(recorders)
	default:
		return nil, "", fmt.Errorf("unsupported multi-tenant endpoint %q", endpoint)
	}
}

func uniqueSortedStrings(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, value := range values {
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

func injectTenantLabel(labels map[string]string, tenantID string) map[string]string {
	if labels == nil {
		labels = map[string]string{}
	}
	if existing, ok := labels["__tenant_id__"]; ok {
		if _, exists := labels["original___tenant_id__"]; !exists {
			labels["original___tenant_id__"] = existing
		}
	}
	labels["__tenant_id__"] = tenantID
	return labels
}

func interfaceStringMap(v interface{}) map[string]string {
	switch m := v.(type) {
	case map[string]string:
		out := make(map[string]string, len(m))
		for k, val := range m {
			out[k] = val
		}
		return out
	case map[string]interface{}:
		out := make(map[string]string, len(m))
		for k, val := range m {
			out[k] = fmt.Sprintf("%v", val)
		}
		return out
	default:
		return map[string]string{}
	}
}

func mergeSeriesResponses(tenantIDs []string, recorders []*httptest.ResponseRecorder) ([]byte, string, error) {
	seen := map[string]map[string]string{}
	for i, rec := range recorders {
		var resp lokiSeriesResponse
		if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
			return nil, "", err
		}
		for _, item := range resp.Data {
			labels := injectTenantLabel(item, tenantIDs[i])
			seen[canonicalLabelsKey(labels)] = labels
		}
	}
	keys := make([]string, 0, len(seen))
	for key := range seen {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	result := make([]map[string]string, 0, len(keys))
	for _, key := range keys {
		result = append(result, seen[key])
	}
	body, err := json.Marshal(map[string]interface{}{"status": "success", "data": result})
	return body, "application/json", err
}

func mergeLokiQueryResponses(tenantIDs []string, recorders []*httptest.ResponseRecorder) ([]byte, string, error) {
	var (
		resultType string
		stats      json.RawMessage
		streams    []lokiStreamResult
		vectors    []lokiVectorResult
		matrixes   []lokiMatrixResult
	)
	encodingFlagsSet := make(map[string]struct{})
	for i, rec := range recorders {
		var resp lokiQueryResponse
		if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
			return nil, "", err
		}
		for _, flag := range resp.Data.EncodingFlags {
			flag = strings.TrimSpace(flag)
			if flag == "" {
				continue
			}
			encodingFlagsSet[flag] = struct{}{}
		}
		if resp.Data.ResultType != "" && resultType == "" {
			resultType = resp.Data.ResultType
		}
		if len(resp.Data.Stats) > 0 {
			stats = resp.Data.Stats
		}
		switch resp.Data.ResultType {
		case "streams":
			var items []lokiStreamResult
			if err := json.Unmarshal(resp.Data.Result, &items); err != nil {
				return nil, "", err
			}
			for _, item := range items {
				item.Stream = injectTenantLabel(item.Stream, tenantIDs[i])
				normalizeMetadataPairTuples(item.Values)
				if streamValuesHaveCategorizedMetadata(item.Values) {
					encodingFlagsSet["categorize-labels"] = struct{}{}
				}
				streams = append(streams, item)
			}
		case "vector":
			var items []lokiVectorResult
			if err := json.Unmarshal(resp.Data.Result, &items); err != nil {
				return nil, "", err
			}
			for _, item := range items {
				item.Metric = injectTenantLabel(item.Metric, tenantIDs[i])
				vectors = append(vectors, item)
			}
		case "matrix":
			var items []lokiMatrixResult
			if err := json.Unmarshal(resp.Data.Result, &items); err != nil {
				return nil, "", err
			}
			for _, item := range items {
				item.Metric = injectTenantLabel(item.Metric, tenantIDs[i])
				matrixes = append(matrixes, item)
			}
		}
	}
	if resultType == "streams" {
		sort.SliceStable(streams, func(i, j int) bool {
			return latestStreamTimestampStrings(streams[i].Values) > latestStreamTimestampStrings(streams[j].Values)
		})
	}
	data := map[string]interface{}{"resultType": resultType}
	switch resultType {
	case "streams":
		data["result"] = streams
	case "vector":
		data["result"] = vectors
	case "matrix":
		data["result"] = matrixes
	default:
		data["result"] = []interface{}{}
	}
	if len(stats) > 0 {
		data["stats"] = json.RawMessage(stats)
	} else {
		data["stats"] = map[string]interface{}{}
	}
	if len(encodingFlagsSet) > 0 {
		encodingFlags := make([]string, 0, len(encodingFlagsSet))
		for flag := range encodingFlagsSet {
			encodingFlags = append(encodingFlags, flag)
		}
		sort.Strings(encodingFlags)
		data["encodingFlags"] = encodingFlags
	}
	body, err := json.Marshal(map[string]interface{}{"status": "success", "data": data})
	return body, "application/json", err
}

func latestStreamTimestampStrings(values [][]interface{}) string {
	if len(values) == 0 {
		return ""
	}
	if len(values[0]) == 0 {
		return ""
	}
	return fmt.Sprintf("%v", values[0][0])
}

// normalizeMetadataPairTuples rewrites tuple metadata to Loki categorized-label object maps.
// It normalizes legacy pair-object arrays ({name,value}) and pair tuples ([[k,v], ...]) into {"k":"v", ...}.
func normalizeMetadataPairTuples(values [][]interface{}) {
	for i := range values {
		if len(values[i]) < 3 {
			continue
		}
		meta, ok := values[i][2].(map[string]interface{})
		if !ok || len(meta) == 0 {
			continue
		}
		if raw, ok := meta["structuredMetadata"]; ok {
			if normalized := normalizeMetadataPairs(raw); len(normalized) > 0 {
				meta["structuredMetadata"] = normalized
			} else {
				delete(meta, "structuredMetadata")
			}
		}
		if raw, ok := meta["parsed"]; ok {
			if normalized := normalizeMetadataPairs(raw); len(normalized) > 0 {
				meta["parsed"] = normalized
			} else {
				delete(meta, "parsed")
			}
		}
		values[i][2] = meta
	}
}

func normalizeMetadataPairs(raw interface{}) map[string]string {
	switch typed := raw.(type) {
	case nil:
		return nil
	case map[string]string:
		out := make(map[string]string, len(typed))
		for key, value := range typed {
			trimmed := strings.TrimSpace(key)
			if trimmed == "" {
				continue
			}
			out[trimmed] = value
		}
		return out
	case []interface{}:
		out := make(map[string]string, len(typed))
		for _, item := range typed {
			switch pair := item.(type) {
			case []interface{}:
				if len(pair) < 2 {
					continue
				}
				key := fmt.Sprintf("%v", pair[0])
				if strings.TrimSpace(key) == "" {
					continue
				}
				out[key] = fmt.Sprintf("%v", pair[1])
			case []string:
				if len(pair) < 2 {
					continue
				}
				key := strings.TrimSpace(pair[0])
				if key == "" {
					continue
				}
				out[key] = pair[1]
			case map[string]interface{}:
				name := strings.TrimSpace(fmt.Sprintf("%v", pair["name"]))
				if name == "" {
					continue
				}
				out[name] = normalizeMetadataStringValue(pair["value"])
			}
		}
		return out
	case map[string]interface{}:
		keys := make([]string, 0, len(typed))
		for key := range typed {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		out := make(map[string]string, len(keys))
		for _, key := range keys {
			trimmed := strings.TrimSpace(key)
			if trimmed == "" {
				continue
			}
			out[trimmed] = normalizeMetadataStringValue(typed[key])
		}
		return out
	default:
		return nil
	}
}

func normalizeMetadataStringValue(raw interface{}) string {
	if raw == nil {
		return ""
	}
	return fmt.Sprintf("%v", raw)
}

func streamValuesHaveCategorizedMetadata(values [][]interface{}) bool {
	for _, tuple := range values {
		if len(tuple) < 3 {
			continue
		}
		if _, ok := tuple[2].(map[string]interface{}); ok {
			return true
		}
	}
	return false
}

func mergeIndexStatsResponses(recorders []*httptest.ResponseRecorder) ([]byte, string, error) {
	totals := map[string]int{"streams": 0, "chunks": 0, "bytes": 0, "entries": 0}
	for _, rec := range recorders {
		var resp map[string]interface{}
		if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
			return nil, "", err
		}
		for key := range totals {
			switch v := resp[key].(type) {
			case float64:
				totals[key] += int(v)
			case int:
				totals[key] += v
			}
		}
	}
	body, err := json.Marshal(totals)
	return body, "application/json", err
}

func mergeDetectedFieldsResponses(recorders []*httptest.ResponseRecorder) ([]byte, string, error) {
	type mergedField struct {
		Label       string
		Type        string
		Cardinality int
		Parsers     map[string]struct{}
		JSONPath    []interface{}
	}
	merged := map[string]*mergedField{}
	for _, rec := range recorders {
		var resp detectedFieldResponse
		if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
			return nil, "", err
		}
		for _, item := range resp.Fields {
			label := item.Label
			if label == "" {
				continue
			}
			mf := merged[label]
			if mf == nil {
				mf = &mergedField{Label: label, Type: item.Type, Parsers: map[string]struct{}{}, JSONPath: item.JSONPath}
				merged[label] = mf
			}
			mf.Cardinality += item.Cardinality
			for _, parser := range item.Parsers {
				mf.Parsers[parser] = struct{}{}
			}
		}
	}
	labels := make([]string, 0, len(merged))
	for label := range merged {
		labels = append(labels, label)
	}
	sort.Strings(labels)
	out := make([]detectedFieldRecord, 0, len(labels))
	for _, label := range labels {
		mf := merged[label]
		parsers := make([]string, 0, len(mf.Parsers))
		for parser := range mf.Parsers {
			parsers = append(parsers, parser)
		}
		sort.Strings(parsers)
		item := detectedFieldRecord{
			Label:       mf.Label,
			Type:        mf.Type,
			Cardinality: mf.Cardinality,
			Parsers:     parsers,
		}
		if len(mf.JSONPath) > 0 {
			item.JSONPath = mf.JSONPath
		}
		out = append(out, item)
	}
	body, err := json.Marshal(detectedFieldResponse{Status: "success", Data: out, Fields: out})
	return body, "application/json", err
}

func (p *Proxy) multiTenantDetectedFieldsResponse(r *http.Request, tenantIDs []string) ([]byte, string, error) {
	lineLimit := parseDetectedLineLimit(r)
	type mergedField struct {
		Label            string
		Type             string
		Parsers          map[string]struct{}
		JSONPath         []interface{}
		Values           map[string]struct{}
		CardinalityFloor int
	}
	merged := map[string]*mergedField{}
	for _, tenantID := range tenantIDs {
		subReq := r.Clone(r.Context())
		subReq.Header = r.Header.Clone()
		subReq.Header.Set("X-Scope-OrgID", tenantID)
		subReq = withOrgID(subReq)

		fields, fieldValues, err := p.detectFields(subReq.Context(), subReq.FormValue("query"), subReq.FormValue("start"), subReq.FormValue("end"), lineLimit)
		if err != nil {
			return nil, "", err
		}
		for _, item := range fields {
			label, _ := item["label"].(string)
			if label == "" {
				continue
			}
			mf := merged[label]
			if mf == nil {
				mf = &mergedField{
					Label:   label,
					Type:    fmt.Sprintf("%v", item["type"]),
					Parsers: map[string]struct{}{},
					Values:  map[string]struct{}{},
				}
				if jp, ok := item["jsonPath"].([]interface{}); ok {
					mf.JSONPath = jp
				}
				merged[label] = mf
			}
			if card, ok := numberToInt(item["cardinality"]); ok && card > mf.CardinalityFloor {
				mf.CardinalityFloor = card
			}
			if parsers, ok := item["parsers"].([]interface{}); ok {
				for _, parser := range parsers {
					mf.Parsers[fmt.Sprintf("%v", parser)] = struct{}{}
				}
			}
			for _, value := range fieldValues[label] {
				mf.Values[value] = struct{}{}
			}
		}
	}

	labels := make([]string, 0, len(merged))
	for label := range merged {
		labels = append(labels, label)
	}
	sort.Strings(labels)

	out := make([]map[string]interface{}, 0, len(labels))
	for _, label := range labels {
		mf := merged[label]
		parsers := make([]string, 0, len(mf.Parsers))
		for parser := range mf.Parsers {
			parsers = append(parsers, parser)
		}
		sort.Strings(parsers)
		cardinality := len(mf.Values)
		if mf.CardinalityFloor > cardinality {
			cardinality = mf.CardinalityFloor
		}
		item := map[string]interface{}{
			"label":       mf.Label,
			"type":        mf.Type,
			"cardinality": cardinality,
			"parsers":     parsers,
		}
		if len(mf.JSONPath) > 0 {
			item["jsonPath"] = mf.JSONPath
		}
		out = append(out, item)
	}
	body, err := json.Marshal(map[string]interface{}{"status": "success", "data": out, "fields": out, "limit": lineLimit})
	return body, "application/json", err
}

func mergeDetectedFieldValuesResponses(recorders []*httptest.ResponseRecorder) ([]byte, string, error) {
	values := make([]string, 0)
	for _, rec := range recorders {
		var resp detectedFieldValuesResponse
		if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
			return nil, "", err
		}
		values = append(values, resp.Values...)
	}
	values = uniqueSortedStrings(values)
	body, err := json.Marshal(detectedFieldValuesResponse{Status: "success", Data: values, Values: values})
	return body, "application/json", err
}

func (p *Proxy) multiTenantDetectedLabelsResponse(r *http.Request, tenantIDs []string) ([]byte, string, error) {
	lineLimit := parseDetectedLineLimit(r)
	merged := map[string]*detectedLabelSummary{
		"__tenant_id__": {
			label:  "__tenant_id__",
			values: map[string]struct{}{},
		},
	}
	for _, tenantID := range tenantIDs {
		merged["__tenant_id__"].values[tenantID] = struct{}{}

		subReq := r.Clone(r.Context())
		subReq.Header = r.Header.Clone()
		subReq.Header.Set("X-Scope-OrgID", tenantID)
		subReq = withOrgID(subReq)

		_, summaries, err := p.detectLabels(subReq.Context(), subReq.FormValue("query"), subReq.FormValue("start"), subReq.FormValue("end"), lineLimit)
		if err != nil {
			return nil, "", err
		}
		for label, summary := range summaries {
			existing := merged[label]
			if existing == nil {
				existing = &detectedLabelSummary{
					label:  summary.label,
					values: map[string]struct{}{},
				}
				merged[label] = existing
			}
			for value := range summary.values {
				existing.values[value] = struct{}{}
			}
		}
	}

	out := formatDetectedLabelSummaries(merged)
	body, err := json.Marshal(map[string]interface{}{"status": "success", "data": out, "detectedLabels": out, "limit": lineLimit})
	return body, "application/json", err
}

func mergeDetectedLabelsResponses(tenantIDs []string, recorders []*httptest.ResponseRecorder) ([]byte, string, error) {
	cardinality := map[string]int{"__tenant_id__": len(tenantIDs)}
	for _, rec := range recorders {
		var resp detectedLabelResponse
		if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
			return nil, "", err
		}
		for _, item := range resp.DetectedLabels {
			label := item.Label
			if label == "" {
				continue
			}
			cardinality[label] += item.Cardinality
		}
	}
	labels := make([]string, 0, len(cardinality))
	for label := range cardinality {
		labels = append(labels, label)
	}
	sort.Slice(labels, func(i, j int) bool {
		if labels[i] == "service_name" {
			return true
		}
		if labels[j] == "service_name" {
			return false
		}
		return labels[i] < labels[j]
	})
	out := make([]detectedLabelRecord, 0, len(labels))
	for _, label := range labels {
		out = append(out, detectedLabelRecord{Label: label, Cardinality: cardinality[label]})
	}
	body, err := json.Marshal(detectedLabelResponse{Status: "success", Data: out, DetectedLabels: out})
	return body, "application/json", err
}

func mergePatternsResponses(recorders []*httptest.ResponseRecorder) ([]byte, string, error) {
	type bucket struct {
		level   string
		pattern string
		samples map[int64]int
		total   int
	}
	merged := map[string]*bucket{}
	for _, rec := range recorders {
		var resp patternsResponse
		if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
			return nil, "", err
		}
		for _, item := range resp.Data {
			pattern := item.Pattern
			level := item.Level
			key := level + "\x00" + pattern
			b := merged[key]
			if b == nil {
				b = &bucket{level: level, pattern: pattern, samples: map[int64]int{}}
				merged[key] = b
			}
			for _, pair := range item.Samples {
				if len(pair) < 2 {
					continue
				}
				ts, ok := numberToInt64(pair[0])
				if !ok {
					continue
				}
				count, ok := numberToInt(pair[1])
				if !ok {
					continue
				}
				b.samples[ts] += count
				b.total += count
			}
		}
	}
	items := make([]*bucket, 0, len(merged))
	for _, item := range merged {
		items = append(items, item)
	}
	sort.Slice(items, func(i, j int) bool { return items[i].total > items[j].total })
	out := make([]patternResultEntry, 0, len(items))
	for _, item := range items {
		timestamps := make([]int64, 0, len(item.samples))
		for ts := range item.samples {
			timestamps = append(timestamps, ts)
		}
		sort.Slice(timestamps, func(i, j int) bool { return timestamps[i] < timestamps[j] })
		samples := make([][]interface{}, 0, len(timestamps))
		for _, ts := range timestamps {
			samples = append(samples, []interface{}{ts, item.samples[ts]})
		}
		respItem := patternResultEntry{Pattern: item.pattern, Samples: samples}
		if item.level != "" {
			respItem.Level = item.level
		}
		out = append(out, respItem)
	}
	body, err := json.Marshal(patternsResponse{Status: "success", Data: out})
	return body, "application/json", err
}

func numberToInt64(v interface{}) (int64, bool) {
	switch n := v.(type) {
	case float64:
		return int64(n), true
	case int64:
		return n, true
	case int:
		return int64(n), true
	case json.Number:
		out, err := n.Int64()
		return out, err == nil
	default:
		return 0, false
	}
}

func numberToInt(v interface{}) (int, bool) {
	switch n := v.(type) {
	case float64:
		return int(n), true
	case int:
		return n, true
	case int64:
		return int(n), true
	case json.Number:
		out, err := n.Int64()
		return int(out), err == nil
	default:
		return 0, false
	}
}

// --- Multitenancy ---

// forwardTenantHeaders maps Loki's X-Scope-OrgID to VL's AccountID/ProjectID.
// Reads orgID from the request context (set by withOrgID).
// tenantMap is protected by configMu (written by ReloadTenantMap on SIGHUP).
func (p *Proxy) forwardTenantHeaders(req *http.Request) {
	orgID := getOrgID(req.Context())
	if orgID == "" {
		// No tenant header → default VL tenant (0:0), serves all data
		return
	}

	// Check tenant map first for string→int mapping (read-lock for SIGHUP safety)
	p.configMu.RLock()
	tm := p.tenantMap
	p.configMu.RUnlock()

	if tm != nil {
		if mapping, ok := tm[orgID]; ok {
			req.Header.Set("AccountID", mapping.AccountID)
			req.Header.Set("ProjectID", mapping.ProjectID)
			return
		}
	}

	// Default-tenant aliases keep Loki single-tenant compatibility while still
	// targeting VictoriaLogs' built-in 0:0 tenant.
	if isDefaultTenantAlias(orgID) {
		return
	}

	// Wildcard bypass is proxy-specific and remains opt-in.
	if orgID == "*" {
		if p.globalTenantAllowed() {
			return
		}
		return
	}

	// Try numeric passthrough: "42" → AccountID: 42
	if _, err := strconv.Atoi(orgID); err == nil {
		req.Header.Set("AccountID", orgID)
		req.Header.Set("ProjectID", "0")
	}
}

