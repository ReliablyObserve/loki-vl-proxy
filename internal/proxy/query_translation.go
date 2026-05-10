package proxy

import (
	"bufio"
	"bytes"
	"context"
	stdjson "encoding/json"
	"fmt"
	"log/slog"
	"math"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/metrics"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
)

func (p *Proxy) requestLogger(endpoint, route string, next http.HandlerFunc) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		tenant := r.Header.Get("X-Scope-OrgID")
		query := r.FormValue("query")
		clientID, clientSource := metrics.ResolveClientContext(r, p.metricsTrustProxyHeaders)
		p.metrics.RecordClientInflight(clientID, 1)
		defer p.metrics.RecordClientInflight(clientID, -1)

		rt := newRequestTelemetry()
		ctx := context.WithValue(r.Context(), requestTelemetryKey, rt)
		ctx = context.WithValue(ctx, requestRouteMetaKey, requestRouteMeta{endpoint: endpoint, route: route})
		grafanaProfile := detectGrafanaClientProfile(r, endpoint, route)
		ctx = context.WithValue(ctx, requestGrafanaClientKey, grafanaProfile)
		reqWithTelemetry := r.WithContext(ctx)
		sc := &statusCapture{ResponseWriter: w, code: 200}
		next.ServeHTTP(sc, reqWithTelemetry)

		elapsed := time.Since(start)
		telemetry := snapshotTelemetry(reqWithTelemetry.Context())
		proxyOverhead := elapsed - telemetry.upstreamDuration
		if proxyOverhead < 0 {
			proxyOverhead = 0
		}
		p.metrics.RecordUpstreamCallsPerRequestWithRoute(endpoint, route, telemetry.upstreamCalls)

		// Per-tenant metrics
		p.metrics.RecordTenantRequestWithRoute(tenant, endpoint, route, sc.code, elapsed)

		// Per-client identity metrics (Grafana user > tenant > IP)
		p.metrics.RecordClientIdentityWithRoute(clientID, endpoint, route, elapsed, int64(sc.bytesWritten))
		p.metrics.RecordClientStatusWithRoute(clientID, endpoint, route, sc.code)
		p.metrics.RecordClientQueryLengthWithRoute(clientID, endpoint, route, len(query))

		// Client error categorization
		if sc.code >= 400 && sc.code < 500 {
			reason := "bad_request"
			switch sc.code {
			case 400:
				reason = "bad_request"
			case 429:
				reason = "rate_limited"
			case 404:
				reason = "not_found"
			case 413:
				reason = "body_too_large"
			}
			p.metrics.RecordClientErrorWithRoute(endpoint, route, reason)
		}

		// Structured request log — includes tenant, query, status, latency, cache info
		logLevel := slog.LevelInfo
		if sc.code >= 500 {
			logLevel = slog.LevelError
		} else if sc.code >= 400 {
			logLevel = slog.LevelWarn
		}
		reqCtx := reqWithTelemetry.Context()
		if !p.log.Enabled(reqCtx, logLevel) {
			return
		}
		// Sample successful requests: skip log assembly for N-1 out of every N 2xx
		// responses to avoid slice allocation and slog formatting overhead.
		if p.logSampleN > 1 && sc.code < 400 {
			if p.logSampleCount.Add(1)%p.logSampleN != 0 {
				return
			}
		}

		authUser, authSource := metrics.ResolveAuthContext(r)
		clientAddr := forwardedClientAddress(r, p.metricsTrustProxyHeaders)
		peerAddr, _ := splitHostPortValue(r.RemoteAddr)
		grafanaSurface := grafanaProfile.surface
		grafanaSourceTag := grafanaProfile.sourceTag
		grafanaVersion := grafanaProfile.version
		upstreamDurationByTypeMs := make(map[string]int64, len(telemetry.upstreamDurationByType))
		for key, value := range telemetry.upstreamDurationByType {
			upstreamDurationByTypeMs[key] = value.Milliseconds()
		}
		internalDurationByTypeMs := make(map[string]int64, len(telemetry.internalDurationByType))
		for key, value := range telemetry.internalDurationByType {
			internalDurationByTypeMs[key] = value.Milliseconds()
		}
		logAttrs := []interface{}{
			"http.route", route,
			"url.path", r.URL.Path,
			"http.request.method", r.Method,
			"http.response.status_code", sc.code,
			"loki.request.type", endpoint,
			"loki.api.system", "loki",
			"proxy.direction", "downstream",
			"event.duration", elapsed.Nanoseconds(),
			"loki.tenant.id", tenant,
			"loki.query", truncateQuery(query, 200),
			"enduser.id", clientID,
			"enduser.source", clientSource,
			"cache.result", telemetry.cacheResult,
			"proxy.duration_ms", elapsed.Milliseconds(),
			"proxy.overhead_ms", proxyOverhead.Milliseconds(),
			"upstream.calls", telemetry.upstreamCalls,
			"upstream.duration_ms", telemetry.upstreamDuration.Milliseconds(),
			"upstream.status_code", telemetry.upstreamLastCode,
			"upstream.error", telemetry.upstreamErrorSeen,
		}
		if len(telemetry.upstreamCallsByType) > 0 {
			logAttrs = append(logAttrs, "upstream.call_types", len(telemetry.upstreamCallsByType))
		}
		if len(upstreamDurationByTypeMs) > 0 {
			logAttrs = append(logAttrs, "upstream.duration_types", len(upstreamDurationByTypeMs))
		}
		if len(telemetry.internalOpsByType) > 0 {
			logAttrs = append(logAttrs, "proxy.operation_types", len(telemetry.internalOpsByType))
		}
		if len(internalDurationByTypeMs) > 0 {
			logAttrs = append(logAttrs, "proxy.operation_duration_types", len(internalDurationByTypeMs))
		}
		if p.log.Enabled(reqCtx, slog.LevelDebug) {
			if len(telemetry.upstreamCallsByType) > 0 {
				logAttrs = append(logAttrs, "upstream.calls_by_type", telemetry.upstreamCallsByType)
			}
			if len(upstreamDurationByTypeMs) > 0 {
				logAttrs = append(logAttrs, "upstream.duration_ms_by_type", upstreamDurationByTypeMs)
			}
			if len(telemetry.internalOpsByType) > 0 {
				logAttrs = append(logAttrs, "proxy.operations_by_type", telemetry.internalOpsByType)
			}
			if len(internalDurationByTypeMs) > 0 {
				logAttrs = append(logAttrs, "proxy.operation_duration_ms_by_type", internalDurationByTypeMs)
			}
		}
		if clientAddr != "" {
			logAttrs = append(logAttrs, "client.address", clientAddr)
		}
		if peerAddr != "" {
			logAttrs = append(logAttrs, "network.peer.address", peerAddr)
		}
		if userAgent := strings.TrimSpace(r.Header.Get("User-Agent")); userAgent != "" {
			logAttrs = append(logAttrs, "user_agent.original", userAgent)
		}
		if grafanaVersion != "" {
			logAttrs = append(logAttrs, "grafana.version", grafanaVersion)
		}
		if grafanaSourceTag != "" {
			logAttrs = append(logAttrs, "grafana.client.source_tag", grafanaSourceTag)
		}
		if grafanaSurface != "unknown" {
			logAttrs = append(logAttrs, "grafana.client.surface", grafanaSurface)
		}
		if grafanaProfile.runtimeFamily != "" {
			logAttrs = append(logAttrs, "grafana.runtime.family", grafanaProfile.runtimeFamily)
		}
		if grafanaProfile.drilldownProfile != "" {
			logAttrs = append(logAttrs, "grafana.drilldown.profile", grafanaProfile.drilldownProfile)
		}
		if grafanaProfile.datasourceProfile != "" {
			logAttrs = append(logAttrs, "grafana.datasource.profile", grafanaProfile.datasourceProfile)
		}
		if enduserName := deriveEnduserName(clientID, clientSource); enduserName != "" {
			logAttrs = append(logAttrs, "enduser.name", enduserName)
		}
		if authUser != "" {
			logAttrs = append(logAttrs,
				"auth.principal", authUser,
				"auth.source", authSource,
			)
		}
		p.log.Log(reqCtx, logLevel, "request", logAttrs...)
	})
}

func truncateQuery(q string, maxLen int) string {
	if len(q) <= maxLen {
		return q
	}
	return q[:maxLen] + "..."
}

func detectGrafanaClientProfile(r *http.Request, endpoint, route string) grafanaClientProfile {
	version := parseGrafanaVersionFromUserAgent(r.Header.Get("User-Agent"))
	sourceTag := parseGrafanaSourceTag(r.Header.Values("X-Query-Tags"))
	surface := "unknown"

	sourceLower := strings.ToLower(sourceTag)
	switch {
	case strings.Contains(sourceLower, "lokiexplore"), strings.Contains(sourceLower, "drilldown"):
		surface = "grafana_drilldown"
	case strings.Contains(sourceLower, "loki"):
		surface = "grafana_loki_datasource"
	}

	// Fallback: infer surface from endpoint family when request is known to come from Grafana.
	if surface == "unknown" && version != "" {
		switch endpoint {
		case "patterns", "detected_fields", "detected_labels", "volume", "volume_range", "drilldown_limits":
			surface = "grafana_drilldown"
		default:
			if strings.HasPrefix(route, "/loki/api/") {
				surface = "grafana_loki_datasource"
			}
		}
	}

	major := parseGrafanaRuntimeMajor(version)
	runtimeFamily := ""
	switch {
	case major >= 12:
		runtimeFamily = "12.x+"
	case major == 11:
		runtimeFamily = "11.x"
	case major > 0:
		runtimeFamily = strconv.Itoa(major) + ".x"
	}

	drilldownProfile := ""
	if surface == "grafana_drilldown" {
		switch {
		case major >= 12:
			drilldownProfile = "drilldown-v2"
		case major == 11:
			drilldownProfile = "drilldown-v1"
		}
	}

	datasourceProfile := ""
	if surface == "grafana_loki_datasource" {
		switch {
		case major >= 12:
			datasourceProfile = "grafana-datasource-v12"
		case major == 11:
			datasourceProfile = "grafana-datasource-v11"
		}
	}

	return grafanaClientProfile{
		surface:           surface,
		sourceTag:         sourceTag,
		version:           version,
		runtimeMajor:      major,
		runtimeFamily:     runtimeFamily,
		drilldownProfile:  drilldownProfile,
		datasourceProfile: datasourceProfile,
	}
}

func parseGrafanaSourceTag(values []string) string {
	for _, raw := range values {
		for _, part := range strings.Split(raw, ",") {
			part = strings.TrimSpace(part)
			if part == "" {
				continue
			}
			key, val, ok := strings.Cut(part, "=")
			if !ok {
				continue
			}
			if !strings.EqualFold(strings.TrimSpace(key), "source") {
				continue
			}
			clean := strings.Trim(strings.TrimSpace(val), `"`)
			if clean != "" {
				return clean
			}
		}
	}
	return ""
}

func parseGrafanaVersionFromUserAgent(userAgent string) string {
	userAgent = strings.TrimSpace(userAgent)
	if userAgent == "" {
		return ""
	}
	lower := strings.ToLower(userAgent)
	idx := strings.Index(lower, "grafana/")
	if idx < 0 {
		return ""
	}
	rest := userAgent[idx+len("grafana/"):]
	end := 0
	for end < len(rest) {
		ch := rest[end]
		if (ch >= '0' && ch <= '9') || (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || ch == '.' || ch == '-' {
			end++
			continue
		}
		break
	}
	version := strings.Trim(rest[:end], ".-")
	return version
}

func parseGrafanaRuntimeMajor(version string) int {
	if version == "" {
		return 0
	}
	majorPart := version
	if idx := strings.IndexByte(majorPart, '.'); idx >= 0 {
		majorPart = majorPart[:idx]
	}
	major, err := strconv.Atoi(majorPart)
	if err != nil {
		return 0
	}
	return major
}

func deriveEnduserName(clientID, clientSource string) string {
	switch clientSource {
	case "grafana_user", "forwarded_user", "webauth_user", "auth_request_user":
		return clientID
	default:
		return ""
	}
}

// translateQuery translates a LogQL query to LogsQL, applying label name translation.
func (p *Proxy) translateQuery(logql string) (string, error) {
	return p.translateQueryWithContext(context.Background(), logql)
}

func (p *Proxy) translateQueryWithContext(ctx context.Context, logql string) (string, error) {
	start := time.Now()
	normalized := strings.TrimSpace(logql)
	switch normalized {
	case "", "*", `"*"`, "`*`":
		p.observeInternalOperation(ctx, "translate_query", "passthrough", time.Since(start))
		return "*", nil
	}
	if p.translationCache != nil {
		if cached, ok := p.translationCache.Get(normalized); ok {
			p.observeInternalOperation(ctx, "translate_query", "cache_hit", time.Since(start))
			return string(cached), nil
		}
	}

	p.configMu.RLock()
	labelFn := p.labelTranslator.ToVL
	streamFieldsMap := p.streamFieldsMap
	p.configMu.RUnlock()
	var (
		translated string
		err        error
	)
	if streamFieldsMap != nil {
		translated, err = translator.TranslateLogQLWithStreamFields(logql, labelFn, streamFieldsMap)
	} else {
		translated, err = translator.TranslateLogQLWithLabels(logql, labelFn)
	}
	if err != nil {
		if p.metrics != nil {
			p.metrics.RecordTranslationError()
		}
		p.observeInternalOperation(ctx, "translate_query", "error", time.Since(start))
		return "", err
	}
	trimmed := strings.TrimSpace(translated)
	if strings.HasPrefix(trimmed, "|") {
		translated = "* " + trimmed
	}
	if p.translationCache != nil {
		p.translationCache.SetWithTTL(normalized, []byte(translated), 5*time.Minute)
	}
	if p.metrics != nil {
		p.metrics.RecordTranslation()
	}
	p.observeInternalOperation(ctx, "translate_query", "translated", time.Since(start))
	return translated, nil
}

var (
	jsonParserStageRE    = regexp.MustCompile(`\|\s*json(?:\s+[^|]+)?`)
	logfmtParserStageRE  = regexp.MustCompile(`\|\s*logfmt(?:\s+[^|]+)?`)
	regexpParserStageRE  = regexp.MustCompile(`\|\s*regexp\b`)
	patternParserStageRE = regexp.MustCompile(`\|\s*pattern\b`)
)

// hasTextExtractionParser returns true when the LogQL query contains any
// parser stage that makes log-line reconstruction unnecessary.  When true,
// the original _msg value is returned verbatim (matching Loki behaviour);
// when false, reconstructLogLineWithFlagFJ wraps _msg + extracted fields
// into a new JSON object, which diverges from Loki for | json queries.
//
// | json is included here: Loki returns the original JSON string unchanged;
// wrapping it in a new JSON envelope is incorrect and adds per-entry CPU cost.
func hasTextExtractionParser(logql string) bool {
	return logfmtParserStageRE.MatchString(logql) ||
		regexpParserStageRE.MatchString(logql) ||
		patternParserStageRE.MatchString(logql) ||
		jsonParserStageRE.MatchString(logql)
}

func hasParserStage(logql, parser string) bool {
	re := jsonParserStageRE
	if parser == "logfmt" {
		re = logfmtParserStageRE
	}
	return re.MatchString(logql)
}

func removeParserStage(logql, parser string) string {
	re := jsonParserStageRE
	if parser == "logfmt" {
		re = logfmtParserStageRE
	}
	logql = re.ReplaceAllString(logql, "")
	for strings.Contains(logql, "  ") {
		logql = strings.ReplaceAll(logql, "  ", " ")
	}
	return strings.TrimSpace(logql)
}

func (p *Proxy) preferWorkingParser(ctx context.Context, logql, start, end string) string {
	opStart := time.Now()
	if !hasParserStage(logql, "json") || !hasParserStage(logql, "logfmt") {
		p.observeInternalOperation(ctx, "prefer_working_parser", "bypass", time.Since(opStart))
		return logql
	}

	baseQuery := extractParserProbeQuery(logql)
	if baseQuery == "" {
		baseQuery = logql
	}
	baseQuery = defaultFieldDetectionQuery(baseQuery)
	logsqlQuery, err := p.translateQueryWithContext(ctx, baseQuery)
	if err != nil {
		p.observeInternalOperation(ctx, "prefer_working_parser", "probe_translate_error", time.Since(opStart))
		return logql
	}

	params := url.Values{}
	params.Set("query", logsqlQuery+" | sort by (_time desc)")
	params.Set("limit", "25")
	if start != "" {
		params.Set("start", formatVLTimestamp(start))
	}
	if end != "" {
		params.Set("end", formatVLTimestamp(end))
	}

	resp, err := p.vlPost(ctx, "/select/logsql/query", params)
	if err != nil {
		p.observeInternalOperation(ctx, "prefer_working_parser", "probe_upstream_error", time.Since(opStart))
		return logql
	}
	defer resp.Body.Close()

	body, err := readBodyLimited(resp.Body, maxBufferedBackendBodyBytes)
	if err != nil || len(body) == 0 {
		p.observeInternalOperation(ctx, "prefer_working_parser", "probe_empty", time.Since(opStart))
		return logql
	}

	jsonHits := 0
	logfmtHits := 0
	startIdx := 0
	for startIdx < len(body) {
		endIdx := startIdx
		for endIdx < len(body) && body[endIdx] != '\n' {
			endIdx++
		}
		line := strings.TrimSpace(string(body[startIdx:endIdx]))
		if endIdx < len(body) {
			startIdx = endIdx + 1
		} else {
			startIdx = len(body)
		}
		if line == "" {
			continue
		}

		var entry map[string]interface{}
		if err := stdjson.Unmarshal([]byte(line), &entry); err != nil {
			continue
		}
		msg, _ := entry["_msg"].(string)
		if msg == "" {
			continue
		}

		var parsedJSON map[string]interface{}
		if stdjson.Unmarshal([]byte(msg), &parsedJSON) == nil && len(parsedJSON) > 0 {
			jsonHits++
		}
		if fields := parseLogfmtFields(msg); len(fields) > 0 {
			logfmtHits++
		}
	}

	switch {
	case jsonHits == 0 && logfmtHits == 0:
		p.observeInternalOperation(ctx, "prefer_working_parser", "no_parser_signal", time.Since(opStart))
		return logql
	case jsonHits >= logfmtHits:
		p.observeInternalOperation(ctx, "prefer_working_parser", "prefer_json", time.Since(opStart))
		return removeParserStage(logql, "logfmt")
	default:
		p.observeInternalOperation(ctx, "prefer_working_parser", "prefer_logfmt", time.Since(opStart))
		return removeParserStage(logql, "json")
	}
}

var metricParserProbeRE = regexp.MustCompile(`(?s)(?:count_over_time|bytes_over_time|rate|bytes_rate|rate_counter|sum_over_time|avg_over_time|max_over_time|min_over_time|first_over_time|last_over_time|stddev_over_time|stdvar_over_time|quantile_over_time)\((.*?)\[[^][]+\]\)`)

var (
	absentOverTimeCompatRE         = regexp.MustCompile(`(?s)^\s*absent_over_time\(\s*(.*)\[([^][]+)\]\s*\)\s*$`)
	bareParserMetricCompatRE       = regexp.MustCompile(`(?s)^\s*(count_over_time|bytes_over_time|rate|bytes_rate|rate_counter|sum_over_time|avg_over_time|max_over_time|min_over_time|first_over_time|last_over_time|stddev_over_time|stdvar_over_time)\((.*)\[([^][]+)\]\)\s*$`)
	bareParserQuantileCompatRE     = regexp.MustCompile(`(?s)^\s*quantile_over_time\(\s*([0-9.]+)\s*,\s*(.*)\[([^][]+)\]\)\s*$`)
	bareParserUnwrapFieldRE        = regexp.MustCompile(`\|\s*unwrap\s+(?:(?:duration|bytes)\(([^)]+)\)|([A-Za-z0-9_.-]+))`)
	regexpExtractingParserStageRE  = regexp.MustCompile(`\|\s*regexp(?:\s+[^|]+)?`)
	patternExtractingParserStageRE = regexp.MustCompile(`\|\s*pattern(?:\s+[^|]+)?`)
	otherExtractingParserStageRE   = regexp.MustCompile(`\|\s*(?:unpack|extract|extract_regexp)(?:\s+[^|]+)?`)
)

func extractParserProbeQuery(logql string) string {
	unquote := func(s string) string {
		s = strings.TrimSpace(s)
		if len(s) < 2 {
			return s
		}
		if (s[0] == '"' && s[len(s)-1] == '"') || (s[0] == '\'' && s[len(s)-1] == '\'') {
			if unquoted, err := strconv.Unquote(s); err == nil {
				return strings.TrimSpace(unquoted)
			}
		}
		return s
	}

	matches := metricParserProbeRE.FindStringSubmatch(logql)
	if len(matches) == 2 {
		return unquote(matches[1])
	}
	return unquote(logql)
}

func hasExtractingParserStage(logql string) bool {
	for _, re := range []*regexp.Regexp{
		jsonParserStageRE,
		logfmtParserStageRE,
		regexpExtractingParserStageRE,
		patternExtractingParserStageRE,
		otherExtractingParserStageRE,
	} {
		if re.MatchString(logql) {
			return true
		}
	}
	return false
}

func parseBareParserMetricCompatSpec(logql string) (bareParserMetricCompatSpec, bool) {
	if matches := bareParserQuantileCompatRE.FindStringSubmatch(strings.TrimSpace(logql)); len(matches) == 4 {
		baseQuery := strings.TrimSpace(matches[2])
		if baseQuery == "" || !hasExtractingParserStage(baseQuery) {
			return bareParserMetricCompatSpec{}, false
		}
		windowRaw := strings.TrimSpace(matches[3])
		window, ok := parsePositiveStepDuration(windowRaw)
		if !ok && !isGrafanaRangeTemplateSelector(windowRaw) {
			return bareParserMetricCompatSpec{}, false
		}
		phi, err := strconv.ParseFloat(matches[1], 64)
		if err != nil || phi < 0 || phi > 1 {
			return bareParserMetricCompatSpec{}, false
		}
		unwrapField := extractBareParserUnwrapField(baseQuery)
		if unwrapField == "" {
			return bareParserMetricCompatSpec{}, false
		}
		return bareParserMetricCompatSpec{
			funcName:        "quantile_over_time",
			baseQuery:       baseQuery,
			rangeWindow:     window,
			rangeWindowExpr: windowRaw,
			unwrapField:     unwrapField,
			quantile:        phi,
		}, true
	}

	matches := bareParserMetricCompatRE.FindStringSubmatch(strings.TrimSpace(logql))
	if len(matches) != 4 {
		return bareParserMetricCompatSpec{}, false
	}
	baseQuery := strings.TrimSpace(matches[2])
	if baseQuery == "" || !hasExtractingParserStage(baseQuery) {
		return bareParserMetricCompatSpec{}, false
	}
	windowRaw := strings.TrimSpace(matches[3])
	window, ok := parsePositiveStepDuration(windowRaw)
	if !ok && !isGrafanaRangeTemplateSelector(windowRaw) {
		return bareParserMetricCompatSpec{}, false
	}
	unwrapField := ""
	switch matches[1] {
	case "rate_counter", "sum_over_time", "avg_over_time", "max_over_time", "min_over_time", "first_over_time", "last_over_time", "stddev_over_time", "stdvar_over_time":
		unwrapField = extractBareParserUnwrapField(baseQuery)
		if unwrapField == "" {
			return bareParserMetricCompatSpec{}, false
		}
	}
	return bareParserMetricCompatSpec{
		funcName:        matches[1],
		baseQuery:       baseQuery,
		rangeWindow:     window,
		rangeWindowExpr: windowRaw,
		unwrapField:     unwrapField,
	}, true
}

func isGrafanaRangeTemplateSelector(window string) bool {
	_, ok := canonicalGrafanaRangeToken(window)
	return ok
}

func resolveBareParserMetricRangeWindow(spec bareParserMetricCompatSpec, start, end, step string) (bareParserMetricCompatSpec, bool) {
	if spec.rangeWindow > 0 {
		return spec, true
	}
	if strings.TrimSpace(spec.rangeWindowExpr) == "" {
		return spec, false
	}
	duration, ok := resolveGrafanaTemplateTokenDuration(spec.rangeWindowExpr, start, end, step)
	if !ok || duration <= 0 {
		return spec, false
	}
	spec.rangeWindow = duration
	return spec, true
}

func extractBareParserUnwrapField(query string) string {
	matches := bareParserUnwrapFieldRE.FindStringSubmatch(query)
	if len(matches) != 3 {
		return ""
	}
	if field := strings.TrimSpace(matches[1]); field != "" {
		return field
	}
	return strings.TrimSpace(matches[2])
}

func formatMetricSampleValue(v float64) string {
	if math.IsNaN(v) {
		return "NaN"
	}
	if math.IsInf(v, 1) {
		return "+Inf"
	}
	if math.IsInf(v, -1) {
		return "-Inf"
	}
	if math.Mod(v, 1) == 0 {
		return strconv.FormatInt(int64(v), 10)
	}
	return strconv.FormatFloat(v, 'f', -1, 64)
}

func metricWindowValue(funcName string, total float64, rangeWindow time.Duration) float64 {
	switch funcName {
	case "rate", "bytes_rate":
		if rangeWindow <= 0 {
			return 0
		}
		return total / rangeWindow.Seconds()
	default:
		return total
	}
}

func (p *Proxy) fetchBareParserMetricSeries(ctx context.Context, originalQuery string, spec bareParserMetricCompatSpec, start, end string) ([]bareParserMetricSeries, error) {
	logsqlQuery, err := p.translateQueryWithContext(ctx, spec.baseQuery)
	if err != nil {
		return nil, err
	}

	params := url.Values{}
	params.Set("query", logsqlQuery+" | sort by (_time)")
	if start != "" {
		params.Set("start", formatVLTimestamp(start))
	}
	if end != "" {
		params.Set("end", formatVLTimestamp(end))
	}
	// Keep consistent with collectRangeMetricSamples to avoid unbounded reads.
	params.Set("limit", "1000000")

	resp, err := p.vlPost(ctx, "/select/logsql/query", params)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= http.StatusBadRequest {
		body, _ := readBodyLimited(resp.Body, maxUpstreamErrorBodyBytes)
		msg := strings.TrimSpace(string(body))
		if msg == "" {
			msg = fmt.Sprintf("VL backend returned %d", resp.StatusCode)
		}
		return nil, &vlAPIError{status: resp.StatusCode, body: msg}
	}

	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 0, 64*1024), 8*1024*1024)
	seriesByKey := make(map[string]*bareParserMetricSeries, 16)
	streamLabelCache := make(map[string]map[string]string, 16)
	streamDescriptorCache := make(map[string]cachedLogQueryStreamDescriptor, 16)
	exposureCache := make(map[string][]metadataFieldExposure, 16)

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
		line := bytes.TrimSpace(scanner.Bytes())
		if len(line) == 0 {
			continue
		}

		entry := vlEntryPool.Get().(map[string]interface{})
		for key := range entry {
			delete(entry, key)
		}
		if err := stdjson.Unmarshal(line, &entry); err != nil {
			vlEntryPool.Put(entry)
			continue
		}

		tsNanos, ok := parseFlexibleUnixNanos(asString(entry["_time"]))
		if !ok {
			vlEntryPool.Put(entry)
			continue
		}
		msg, _ := stringifyEntryValue(entry["_msg"])
		desc := p.logQueryStreamDescriptor(asString(entry["_stream"]), asString(entry["level"]), streamLabelCache, streamDescriptorCache)
		_, parsedFields := p.classifyEntryMetadataFields(entry, desc.rawLabels, true, exposureCache, smBuf, pfBuf)
		metric := cloneStringMap(desc.translatedLabels)
		for key, value := range parsedFields {
			if spec.unwrapField != "" && key == spec.unwrapField {
				continue
			}
			metric[key] = value
		}
		seriesKey := canonicalLabelsKey(metric)
		series, ok := seriesByKey[seriesKey]
		if !ok {
			series = &bareParserMetricSeries{
				metric:  metric,
				samples: make([]bareParserMetricSample, 0, 8),
			}
			seriesByKey[seriesKey] = series
		}
		weight := 1.0
		if spec.unwrapField != "" {
			rawValue, ok := stringifyEntryValue(entry[spec.unwrapField])
			if !ok {
				vlEntryPool.Put(entry)
				continue
			}
			parsedValue, err := strconv.ParseFloat(rawValue, 64)
			if err != nil {
				vlEntryPool.Put(entry)
				continue
			}
			weight = parsedValue
		} else if spec.funcName == "bytes_over_time" || spec.funcName == "bytes_rate" {
			weight = float64(len(msg))
		}
		series.samples = append(series.samples, bareParserMetricSample{tsNanos: tsNanos, value: weight})
		vlEntryPool.Put(entry)
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}

	result := make([]bareParserMetricSeries, 0, len(seriesByKey))
	for _, series := range seriesByKey {
		result = append(result, *series)
	}
	sort.Slice(result, func(i, j int) bool {
		return canonicalLabelsKey(result[i].metric) < canonicalLabelsKey(result[j].metric)
	})
	return result, nil
}

// fetchBareParserMetricSeriesViaHits is the fast path for count_over_time / rate
// sliding windows. Instead of fetching raw log entries, it calls VL's /select/logsql/hits
// endpoint which returns pre-aggregated counts per bucket — no log body transfer.
//
// evalStart, evalEnd, stepNs are in nanoseconds.
func (p *Proxy) fetchBareParserMetricSeriesViaHits(
	ctx context.Context,
	spec bareParserMetricCompatSpec,
	evalStart, evalEnd, stepNs int64,
) ([]bareParserMetricSeries, error) {
	logsqlQuery, err := p.translateQueryWithContext(ctx, spec.baseQuery)
	if err != nil {
		return nil, err
	}

	// Fetch one extra range-window of history before evalStart so the first
	// sliding window evaluation has enough bucketed data.
	fetchStart := evalStart - spec.rangeWindow.Nanoseconds()

	stepSecs := stepNs / int64(time.Second)
	if stepSecs < 1 {
		stepSecs = 1
	}

	params := url.Values{}
	params.Set("query", logsqlQuery)
	params.Set("start", nanosToVLTimestamp(fetchStart))
	params.Set("end", nanosToVLTimestamp(evalEnd))
	params.Set("step", strconv.FormatInt(stepSecs, 10)+"s")

	// Group by declared stream label fields so we get per-stream series.
	p.configMu.RLock()
	declared := p.declaredLabelFields
	lt := p.labelTranslator
	p.configMu.RUnlock()
	for _, f := range declared {
		params.Add("field", f)
	}

	resp, err := p.vlGet(ctx, "/select/logsql/hits", params)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= http.StatusBadRequest {
		body, _ := readBodyLimited(resp.Body, maxUpstreamErrorBodyBytes)
		return nil, fmt.Errorf("hits: status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	body, err := readBodyLimited(resp.Body, maxBufferedBackendBodyBytes)
	if err != nil {
		return nil, err
	}
	hits := parseHits(body)

	buckets := make(map[string]map[int64]int64, len(hits.Hits))
	labelSets := make(map[string]map[string]string, len(hits.Hits))
	for _, hit := range hits.Hits {
		// Translate VL field names to Loki label names.
		translated := make(map[string]string, len(hit.Fields))
		for k, v := range hit.Fields {
			translated[lt.ToLoki(k)] = v
		}
		key := canonicalLabelsKey(translated)
		if _, ok := buckets[key]; !ok {
			buckets[key] = make(map[int64]int64, len(hit.Timestamps))
			labelSets[key] = translated
		}
		for i, ts := range hit.Timestamps {
			tsNanos, ok := parseFlexibleUnixNanos(string(ts))
			if !ok || i >= len(hit.Values) {
				continue
			}
			buckets[key][tsNanos] += int64(hit.Values[i])
		}
	}

	isRate := spec.funcName == "rate"
	return buildSlidingWindowSumsFromHits(buckets, labelSets, evalStart, evalEnd, stepNs, spec.rangeWindow.Nanoseconds(), isRate), nil
}

func bareParserMetricWindowValue(funcName string, window []bareParserMetricSample, spec bareParserMetricCompatSpec) float64 {
	if len(window) == 0 {
		return 0
	}
	switch funcName {
	case "count_over_time", "rate", "bytes_over_time", "bytes_rate", "sum_over_time":
		total := 0.0
		for _, sample := range window {
			total += sample.value
		}
		return metricWindowValue(funcName, total, spec.rangeWindow)
	case "rate_counter":
		if len(window) < 2 || spec.rangeWindow <= 0 {
			return 0
		}
		increase := 0.0
		prev := window[0].value
		for _, sample := range window[1:] {
			if sample.value >= prev {
				increase += sample.value - prev
			} else {
				// Counter reset: treat current value as the post-reset increase.
				increase += sample.value
			}
			prev = sample.value
		}
		return increase / spec.rangeWindow.Seconds()
	case "avg_over_time":
		total := 0.0
		for _, sample := range window {
			total += sample.value
		}
		return total / float64(len(window))
	case "max_over_time":
		maxValue := window[0].value
		for _, sample := range window[1:] {
			if sample.value > maxValue {
				maxValue = sample.value
			}
		}
		return maxValue
	case "min_over_time":
		minValue := window[0].value
		for _, sample := range window[1:] {
			if sample.value < minValue {
				minValue = sample.value
			}
		}
		return minValue
	case "first_over_time":
		return window[0].value
	case "last_over_time":
		return window[len(window)-1].value
	case "stddev_over_time", "stdvar_over_time":
		mean := 0.0
		for _, sample := range window {
			mean += sample.value
		}
		mean /= float64(len(window))
		variance := 0.0
		for _, sample := range window {
			delta := sample.value - mean
			variance += delta * delta
		}
		variance /= float64(len(window))
		if funcName == "stddev_over_time" {
			return math.Sqrt(variance)
		}
		return variance
	case "quantile_over_time":
		values := make([]float64, 0, len(window))
		for _, sample := range window {
			values = append(values, sample.value)
		}
		sort.Float64s(values)
		if len(values) == 1 {
			return values[0]
		}
		pos := spec.quantile * float64(len(values)-1)
		lower := int(math.Floor(pos))
		upper := int(math.Ceil(pos))
		if lower == upper {
			return values[lower]
		}
		weight := pos - float64(lower)
		return values[lower] + ((values[upper] - values[lower]) * weight)
	default:
		return 0
	}
}

func buildBareParserMetricMatrix(series []bareParserMetricSeries, startNanos, endNanos, stepNanos int64, spec bareParserMetricCompatSpec) map[string]interface{} {
	result := make([]lokiMatrixResult, 0, len(series))
	windowNanos := int64(spec.rangeWindow)
	for _, seriesItem := range series {
		values := make([][]interface{}, 0, int(((endNanos-startNanos)/stepNanos)+1))
		samples := seriesItem.samples
		left := 0
		right := 0
		for eval := startNanos; eval <= endNanos; eval += stepNanos {
			lower := eval - windowNanos
			for right < len(samples) && samples[right].tsNanos <= eval {
				right++
			}
			for left < right && samples[left].tsNanos < lower {
				left++
			}
			window := samples[left:right]
			if len(window) == 0 {
				continue // no samples in window — match Loki's absent-point behaviour
			}
			values = append(values, []interface{}{float64(eval) / float64(time.Second), formatMetricSampleValue(bareParserMetricWindowValue(spec.funcName, window, spec))})
		}
		result = append(result, lokiMatrixResult{Metric: seriesItem.metric, Values: values})
	}
	return map[string]interface{}{
		"status": "success",
		"data": map[string]interface{}{
			"resultType": "matrix",
			"result":     result,
		},
	}
}

func buildBareParserMetricVector(series []bareParserMetricSeries, evalNanos int64, spec bareParserMetricCompatSpec) map[string]interface{} {
	result := make([]lokiVectorResult, 0, len(series))
	windowNanos := int64(spec.rangeWindow)
	for _, seriesItem := range series {
		lower := evalNanos - windowNanos
		window := make([]bareParserMetricSample, 0, len(seriesItem.samples))
		for _, sample := range seriesItem.samples {
			if sample.tsNanos >= lower && sample.tsNanos <= evalNanos {
				window = append(window, sample)
			}
		}
		result = append(result, lokiVectorResult{
			Metric: seriesItem.metric,
			Value:  []interface{}{float64(evalNanos) / float64(time.Second), formatMetricSampleValue(bareParserMetricWindowValue(spec.funcName, window, spec))},
		})
	}
	return map[string]interface{}{
		"status": "success",
		"data": map[string]interface{}{
			"resultType": "vector",
			"result":     result,
		},
	}
}

type absentOverTimeCompatSpec struct {
	baseQuery   string
	rangeWindow time.Duration
}

func parseAbsentOverTimeCompatSpec(logql string) (absentOverTimeCompatSpec, bool) {
	matches := absentOverTimeCompatRE.FindStringSubmatch(strings.TrimSpace(logql))
	if len(matches) != 3 {
		return absentOverTimeCompatSpec{}, false
	}
	window, ok := parsePositiveStepDuration(matches[2])
	if !ok {
		return absentOverTimeCompatSpec{}, false
	}
	baseQuery := strings.TrimSpace(matches[1])
	if baseQuery == "" {
		return absentOverTimeCompatSpec{}, false
	}
	return absentOverTimeCompatSpec{baseQuery: baseQuery, rangeWindow: window}, true
}

func extractAbsentMetricLabels(query string) map[string]string {
	selector, _, ok := splitLeadingSelector(strings.TrimSpace(query))
	if !ok || len(selector) < 2 {
		return map[string]string{}
	}
	matchers := splitSelectorMatchers(selector[1 : len(selector)-1])
	labels := make(map[string]string, len(matchers))
	for _, matcher := range matchers {
		matcher = strings.TrimSpace(matcher)
		if strings.Contains(matcher, "!=") || strings.Contains(matcher, "=~") || strings.Contains(matcher, "!~") {
			continue
		}
		idx := strings.Index(matcher, "=")
		if idx <= 0 {
			continue
		}
		label := strings.TrimSpace(matcher[:idx])
		value := strings.TrimSpace(matcher[idx+1:])
		value = strings.Trim(value, "\"`")
		if label == "" || value == "" {
			continue
		}
		labels[label] = value
	}
	return labels
}

func statsResponseIsEmpty(body []byte) bool {
	var resp struct {
		Data struct {
			Result []lokiVectorResult `json:"result"`
		} `json:"data"`
		Results []lokiVectorResult `json:"results"`
	}
	if err := stdjson.Unmarshal(body, &resp); err != nil {
		return false
	}
	results := resp.Results
	if len(results) == 0 {
		results = resp.Data.Result
	}
	if len(results) == 0 {
		return true
	}
	for _, item := range results {
		if len(item.Value) < 2 {
			continue
		}
		raw := fmt.Sprint(item.Value[1])
		raw = strings.Trim(raw, "\"")
		value, err := strconv.ParseFloat(raw, 64)
		if err != nil {
			return false
		}
		if value != 0 {
			return false
		}
	}
	return true
}

func buildAbsentInstantVector(evalRaw string, metric map[string]string) map[string]interface{} {
	evalNs, ok := parseFlexibleUnixNanos(evalRaw)
	if !ok {
		evalNs = time.Now().UnixNano()
	}
	return map[string]interface{}{
		"status": "success",
		"data": map[string]interface{}{
			"resultType": "vector",
			"result": []lokiVectorResult{{
				Metric: metric,
				Value:  []interface{}{float64(evalNs) / float64(time.Second), "1"},
			}},
		},
	}
}

func (p *Proxy) proxyAbsentOverTimeQuery(w http.ResponseWriter, r *http.Request, start time.Time, originalQuery string, spec absentOverTimeCompatSpec) {
	logsqlQuery, err := p.translateQueryWithContext(r.Context(), originalQuery)
	if err != nil {
		p.writeError(w, http.StatusBadRequest, err.Error())
		p.metrics.RecordRequest("query", http.StatusBadRequest, time.Since(start))
		return
	}

	params := url.Values{}
	params.Set("query", logsqlQuery)
	if t := r.FormValue("time"); t != "" {
		params.Set("time", formatVLTimestamp(t))
	}

	resp, err := p.vlPost(r.Context(), "/select/logsql/stats_query", params)
	if err != nil {
		status := statusFromUpstreamErr(err)
		p.writeError(w, status, err.Error())
		p.metrics.RecordRequest("query", status, time.Since(start))
		p.queryTracker.Record("query", originalQuery, time.Since(start), true)
		return
	}
	defer resp.Body.Close()

	body, _ := readBodyLimited(resp.Body, maxBufferedBackendBodyBytes)
	if resp.StatusCode >= http.StatusBadRequest {
		p.writeError(w, resp.StatusCode, string(body))
		p.metrics.RecordRequest("query", resp.StatusCode, time.Since(start))
		p.queryTracker.Record("query", originalQuery, time.Since(start), true)
		return
	}

	body = p.translateStatsResponseLabelsWithContext(r.Context(), body, originalQuery)
	var out []byte
	if statsResponseIsEmpty(body) {
		out, _ = stdjson.Marshal(buildAbsentInstantVector(r.FormValue("time"), extractAbsentMetricLabels(spec.baseQuery)))
	} else {
		out = wrapAsLokiResponse([]byte(`{"result":[]}`), "vector")
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(out)
	elapsed := time.Since(start)
	p.metrics.RecordRequest("query", http.StatusOK, elapsed)
	p.queryTracker.Record("query", originalQuery, elapsed, false)
}

// lastParserStageEnd returns the index in baseQuery just after the last
// extracting parser stage, or -1 if no parser stage is found.
func lastParserStageEnd(baseQuery string) int {
	lastEnd := -1
	for _, re := range []*regexp.Regexp{
		jsonParserStageRE,
		logfmtParserStageRE,
		regexpExtractingParserStageRE,
		patternExtractingParserStageRE,
		otherExtractingParserStageRE,
	} {
		for _, loc := range re.FindAllStringIndex(baseQuery, -1) {
			if loc[1] > lastEnd {
				lastEnd = loc[1]
			}
		}
	}
	return lastEnd
}

// hasPostParserPipeStage reports true when the base query has any pipe stage
// after the last extracting parser (e.g. "| json | status >= 400"). When false,
// the parser doesn't filter log lines and can be stripped for native VL stats.
func hasPostParserPipeStage(baseQuery string) bool {
	end := lastParserStageEnd(baseQuery)
	if end < 0 {
		return false
	}
	return strings.HasPrefix(strings.TrimSpace(baseQuery[end:]), "|")
}

// dropErrorOnlyRE matches a pipe stage that drops only __error__ and/or
// __error_details__ labels — e.g. "| drop __error__, __error_details__" or
// "| drop __error__". Such stages do not filter log lines; they merely strip
// parser-error metadata and signal that the caller has opted in to VL's
// count-all semantics for native stats.
var dropErrorOnlyRE = regexp.MustCompile(`^\|\s*drop\s+(?:__error__(?:\s*,\s*__error_details__)?|__error_details__(?:\s*,\s*__error__)?)\s*$`)

// hasFilteringPostParserPipeStage is like hasPostParserPipeStage but returns
// false when the only post-parser stage is a "| drop __error__[, __error_details__]"
// clause. That clause does not filter log lines — it only removes parse-error
// metadata — so it is safe to skip for the native VL stats fast path.
func hasFilteringPostParserPipeStage(baseQuery string) bool {
	end := lastParserStageEnd(baseQuery)
	if end < 0 {
		return false
	}
	tail := strings.TrimSpace(baseQuery[end:])
	if !strings.HasPrefix(tail, "|") {
		return false
	}
	// Allow a sole "| drop __error__[, __error_details__]" stage to pass through.
	if dropErrorOnlyRE.MatchString(tail) {
		return false
	}
	return true
}

// stripParserStages removes all extracting parser stages from a LogQL base query.
func stripParserStages(baseQuery string) string {
	result := baseQuery
	for _, re := range []*regexp.Regexp{
		jsonParserStageRE,
		logfmtParserStageRE,
		regexpExtractingParserStageRE,
		patternExtractingParserStageRE,
		otherExtractingParserStageRE,
	} {
		result = re.ReplaceAllString(result, "")
	}
	return strings.TrimSpace(result)
}

// proxyBareParserMetricViaStats routes bare parser metric queries (e.g.
// rate({app} | json [5m])) to native VL stats_query_range for the tumbling-window
// case (range==step). Loki groups these by stream labels only (not parsed fields),
// and VL native stats matches that behaviour. Returns true when the fast path handled
// the request.
func (p *Proxy) proxyBareParserMetricViaStats(w http.ResponseWriter, r *http.Request, reqStart time.Time, originalQuery string, spec bareParserMetricCompatSpec) bool {
	window := "[" + spec.rangeWindowExpr + "]"
	switch spec.funcName {
	case "rate", "count_over_time", "bytes_over_time", "bytes_rate":
	default:
		return false
	}
	reconstructed := spec.funcName + "(" + spec.baseQuery + " " + window + ")"

	logsqlQuery, err := p.translateQueryWithContext(r.Context(), reconstructed)
	if err != nil || !isStatsQuery(logsqlQuery) {
		return false
	}
	// Shift start back by the range window so VL includes the extra initial bucket
	// that covers the data Loki uses for the first rate() evaluation point at T0
	// (Loki reads [T0-window, T0]; VL tumbling window without shift gives [T0, T0+step)).
	origStartNs, hasStart := parseLokiTimeToUnixNano(r.FormValue("start"))
	var effectiveR *http.Request
	if hasStart && spec.rangeWindow > 0 {
		shiftedR := r.Clone(r.Context())
		_ = shiftedR.ParseForm()
		shiftedR.Form.Set("start", nanosToVLTimestamp(origStartNs-spec.rangeWindow.Nanoseconds()))
		effectiveR = shiftedR
	} else {
		effectiveR = r
	}

	buf := &bufferedResponseWriter{}
	p.proxyStatsQueryRangeDirect(buf, effectiveR, logsqlQuery)

	body := buf.body
	if hasStart && spec.rangeWindow > 0 {
		body = trimStatsQueryRangeResponseFromStart(body, origStartNs)
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

	elapsed := time.Since(reqStart)
	p.metrics.RecordRequest("query_range", code, elapsed)
	p.queryTracker.Record("query_range", originalQuery, elapsed, code >= 400)
	return true
}

func (p *Proxy) proxyBareParserMetricQueryRange(w http.ResponseWriter, r *http.Request, start time.Time, originalQuery string, spec bareParserMetricCompatSpec) {
	// Tumbling-window fast path: for count-like functions with no post-parser pipeline
	// stages, no unwrap, range==step, and explicit __error__ handling, route to native
	// VL stats_query_range.
	//
	// The __error__ guard is required: per Loki's error model a log line that fails
	// parsing (e.g. invalid JSON for | json) is excluded from metric aggregation — it
	// contributes to __error__, not to rate/count_over_time/bytes_*. VL native stats
	// counts all lines and does not replicate this exclusion. Queries that explicitly
	// handle __error__ (e.g. | drop __error__, __error_details__) have opted in to
	// VL's count-all semantics; all others must use the slow log-fetch path so that
	// only successfully parsed lines are counted.
	stepDurFast, stepOk := parsePositiveStepDuration(r.FormValue("step"))
	rangeEqualsStep := stepOk && spec.rangeWindow > 0 && spec.rangeWindow == stepDurFast
	if spec.unwrapField == "" && rangeEqualsStep && !hasFilteringPostParserPipeStage(spec.baseQuery) &&
		strings.Contains(originalQuery, "__error__") {
		switch spec.funcName {
		case "rate", "count_over_time", "bytes_over_time", "bytes_rate":
			if p.proxyBareParserMetricViaStats(w, r, start, originalQuery, spec) {
				return
			}
		}
	}

	startNanos, ok := parseFlexibleUnixNanos(r.FormValue("start"))
	if !ok {
		p.writeError(w, http.StatusBadRequest, "invalid start timestamp")
		p.metrics.RecordRequest("query_range", http.StatusBadRequest, time.Since(start))
		return
	}
	endNanos, ok := parseFlexibleUnixNanos(r.FormValue("end"))
	if !ok || endNanos < startNanos {
		p.writeError(w, http.StatusBadRequest, "invalid end timestamp")
		p.metrics.RecordRequest("query_range", http.StatusBadRequest, time.Since(start))
		return
	}
	stepNanos, ok := parseStepToNanos(r.FormValue("step"))
	if !ok {
		p.writeError(w, http.StatusBadRequest, "invalid step")
		p.metrics.RecordRequest("query_range", http.StatusBadRequest, time.Since(start))
		return
	}

	// Sliding-window fast path: for count_over_time / rate without post-parser stages
	// and with configured stream label fields, use the hits endpoint (pre-aggregated
	// counts, no log body transfer). Falls back to slow path on any error.
	p.configMu.RLock()
	hasDeclaredFields := len(p.declaredLabelFields) > 0
	p.configMu.RUnlock()
	if spec.unwrapField == "" && !hasPostParserPipeStage(spec.baseQuery) && hasDeclaredFields {
		switch spec.funcName {
		case "rate", "count_over_time":
			if spec.rangeWindow > 0 && spec.rangeWindow.Nanoseconds() > stepNanos {
				series, hitsErr := p.fetchBareParserMetricSeriesViaHits(
					r.Context(), spec,
					startNanos, endNanos, stepNanos,
				)
				if hitsErr == nil {
					w.Header().Set("Content-Type", "application/json")
					marshalJSON(w, buildBareParserMetricMatrix(series, startNanos, endNanos, stepNanos, spec))
					elapsed := time.Since(start)
					p.metrics.RecordRequest("query_range", http.StatusOK, elapsed)
					p.queryTracker.Record("query_range", originalQuery, elapsed, false)
					return
				}
				slog.WarnContext(r.Context(), "hits-based metric path failed, falling back to full-fetch",
					"err", hitsErr, "query", originalQuery)
			}
		}
	}

	series, err := p.fetchBareParserMetricSeries(r.Context(), originalQuery, spec, r.FormValue("start"), r.FormValue("end"))
	if err != nil {
		status := statusFromUpstreamErr(err)
		p.writeError(w, status, err.Error())
		p.metrics.RecordRequest("query_range", status, time.Since(start))
		p.queryTracker.Record("query_range", originalQuery, time.Since(start), true)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	marshalJSON(w, buildBareParserMetricMatrix(series, startNanos, endNanos, stepNanos, spec))
	elapsed := time.Since(start)
	p.metrics.RecordRequest("query_range", http.StatusOK, elapsed)
	p.queryTracker.Record("query_range", originalQuery, elapsed, false)
}

func (p *Proxy) proxyBareParserMetricQuery(w http.ResponseWriter, r *http.Request, start time.Time, originalQuery string, spec bareParserMetricCompatSpec) {
	evalNanos, ok := parseFlexibleUnixNanos(r.FormValue("time"))
	if !ok {
		evalNanos = time.Now().UnixNano()
	}
	startWindow := strconv.FormatInt(evalNanos-int64(spec.rangeWindow), 10)
	endWindow := strconv.FormatInt(evalNanos, 10)
	series, err := p.fetchBareParserMetricSeries(r.Context(), originalQuery, spec, startWindow, endWindow)
	if err != nil {
		status := statusFromUpstreamErr(err)
		p.writeError(w, status, err.Error())
		p.metrics.RecordRequest("query", status, time.Since(start))
		p.queryTracker.Record("query", originalQuery, time.Since(start), true)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	marshalJSON(w, buildBareParserMetricVector(series, evalNanos, spec))
	elapsed := time.Since(start)
	p.metrics.RecordRequest("query", http.StatusOK, elapsed)
	p.queryTracker.Record("query", originalQuery, elapsed, false)
}

// translateStatsItem holds one element from the VL stats result array.
// value/values are kept as stdjson.RawMessage to avoid parsing time-series data
// (timestamps + floats) that we never inspect — only the metric labels change.
type translateStatsItem struct {
	Metric map[string]string `json:"metric"`
	Value  stdjson.RawMessage   `json:"value,omitempty"`
	Values stdjson.RawMessage   `json:"values,omitempty"`
}

type translateStatsData struct {
	ResultType string               `json:"resultType,omitempty"`
	Result     []translateStatsItem `json:"result,omitempty"`
	Stats      stdjson.RawMessage      `json:"stats,omitempty"`
}

type translateStatsResponseBody struct {
	Status  string               `json:"status,omitempty"`
	Data    *translateStatsData  `json:"data,omitempty"`
	Result  []translateStatsItem `json:"result,omitempty"`
	Results []translateStatsItem `json:"results,omitempty"`
	Error   stdjson.RawMessage      `json:"error,omitempty"`
}

func (p *Proxy) translateStatsResponseLabelsWithContext(ctx context.Context, body []byte, originalQuery string) []byte {
	start := time.Now()
	var resp translateStatsResponseBody
	if err := stdjson.Unmarshal(body, &resp); err != nil {
		p.observeInternalOperation(ctx, "translate_stats_response_labels", "decode_error", time.Since(start))
		return body
	}

	// Collect pointers to all result slices (handles data.result, result, results).
	var allResults []*[]translateStatsItem
	if resp.Data != nil && len(resp.Data.Result) > 0 {
		allResults = append(allResults, &resp.Data.Result)
	}
	if len(resp.Result) > 0 {
		allResults = append(allResults, &resp.Result)
	}
	if len(resp.Results) > 0 {
		allResults = append(allResults, &resp.Results)
	}

	if len(allResults) == 0 {
		p.observeInternalOperation(ctx, "translate_stats_response_labels", "no_results", time.Since(start))
		return body
	}

	// Reuse maps across iterations to reduce allocations.
	translated := make(map[string]string, 8)
	syntheticLabels := make(map[string]string, 8)

	translatedMetrics := 0
	for _, resultsPtr := range allResults {
		for i := range *resultsPtr {
			item := &(*resultsPtr)[i]
			metric := item.Metric
			if len(metric) == 0 {
				continue
			}

			// Clear reused maps instead of allocating new ones each iteration.
			for k := range translated {
				delete(translated, k)
			}
			changed := false
			hadStream := false
			for k, v := range metric {
				if k == "__name__" {
					changed = true
					continue
				}
				if k == "_stream" {
					hadStream = true
					for streamKey, streamValue := range parseStreamLabels(v) {
						lokiKey := streamKey
						if !p.labelTranslator.IsPassthrough() {
							lokiKey = p.labelTranslator.ToLoki(streamKey)
						}
						translated[lokiKey] = streamValue
					}
					changed = true
					continue
				}
				lokiKey := k
				if !p.labelTranslator.IsPassthrough() {
					lokiKey = p.labelTranslator.ToLoki(k)
				}
				if lokiKey != k {
					changed = true
				}
				translated[lokiKey] = v
			}
			for k := range syntheticLabels {
				delete(syntheticLabels, k)
			}
			for key, value := range translated {
				syntheticLabels[key] = value
			}
			serviceSignal := hasServiceSignal(syntheticLabels)
			beforeSyntheticCount := len(syntheticLabels)
			hadLevel := syntheticLabels["level"] != ""
			ensureDetectedLevel(syntheticLabels)
			// Remove the raw level label only when it came from an explicit VL grouping
			// dimension (no _stream in the response), i.e. "sum by (detected_level)"
			// translates to VL's "sum by (level)" and back. In that case level must be
			// replaced by detected_level. When _stream IS present, level is a genuine
			// stream label that Loki also returns alongside detected_level — keep both.
			if hadLevel && !hadStream && syntheticLabels["detected_level"] != "" {
				delete(syntheticLabels, "level")
				delete(translated, "level")
			}
			// Only synthesize service_name for raw stream metrics (hadStream=true).
			// For aggregated results like "sum by (container)", the metric should only
			// contain the by() labels — adding service_name derived from container would
			// cause Drilldown include/exclude to fail because the extra label is unexpected.
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
				translatedMetrics++
				// Copy translated map to item — the reused map is mutated next iteration.
				newMetric := make(map[string]string, len(translated))
				for k, v := range translated {
					newMetric[k] = v
				}
				item.Metric = newMetric
			}
		}
	}

	if translatedMetrics == 0 {
		// Nothing changed — skip the re-marshal entirely.
		p.observeInternalOperation(ctx, "translate_stats_response_labels", "noop", time.Since(start))
		return body
	}
	// Clear Status so the output matches VL's format ({"data":{...}} without status).
	// wrapAsLokiResponse adds the {"status":"success"} envelope — its fast-path byte-splice
	// only triggers when the body starts with {"data":{"resultType":, not {"status":...}.
	resp.Status = ""
	result, err := stdjson.Marshal(resp)
	if err != nil {
		p.observeInternalOperation(ctx, "translate_stats_response_labels", "encode_error", time.Since(start))
		return body
	}
	p.observeInternalOperation(ctx, "translate_stats_response_labels", "translated", time.Since(start))
	return result
}
