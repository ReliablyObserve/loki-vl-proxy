package proxy

import (
	"bufio"
	"bytes"
	"context"
	stdjson "encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	fj "github.com/valyala/fastjson"

	logqlpkg "github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/metrics"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/observability"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
)

//nolint:gocyclo // middleware wraps every request with telemetry, client/tenant attribution, panic guards and structured logging; complexity is inherent to instrumentation.
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

		// Adaptive request log sampling:
		// - At low rate (<QuietThreshold req/s): log every request individually
		// - At high rate: suppress OK traffic, emit periodic digest; errors always counted,
		//   collapsed into digest at high error rates
		logLevel := slog.LevelInfo
		if sc.code >= 500 {
			logLevel = slog.LevelError
		} else if sc.code >= 400 {
			logLevel = slog.LevelWarn
		}
		reqCtx := reqWithTelemetry.Context()
		if !p.log.Enabled(reqCtx, logLevel) {
			// Still feed the sampler for digest stats even when level is disabled.
			if p.requestSampler != nil {
				p.requestSampler.ShouldLog(observability.RequestInfo{
					StatusCode: sc.code,
					LatencyMs:  elapsed.Milliseconds(),
					CacheHit:   telemetry.cacheResult == "hit",
				})
			}
			return
		}

		// Emit periodic digest if interval elapsed.
		if p.requestSampler != nil {
			if attrs := p.requestSampler.DigestAttrs(); attrs != nil {
				dr := slog.NewRecord(time.Now(), slog.LevelInfo, "request_digest", 0)
				dr.AddAttrs(attrs...)
				_ = p.log.Handler().Handle(reqCtx, dr)
			}
		}

		// Adaptive sampling decision.
		shouldLog := true
		if p.requestSampler != nil {
			shouldLog = p.requestSampler.ShouldLog(observability.RequestInfo{
				StatusCode: sc.code,
				LatencyMs:  elapsed.Milliseconds(),
				Query:      r.FormValue("query"),
				CacheHit:   telemetry.cacheResult == "hit",
				Endpoint:   endpoint,
				Route:      route,
				Tenant:     tenant,
			})
		} else if p.logSampleN > 1 && sc.code < 400 {
			// Legacy fallback: static sampling when sampler not initialized.
			if p.logSampleCount.Add(1)%p.logSampleN != 0 {
				shouldLog = false
			}
		}
		if !shouldLog {
			return
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
		logAttrs := make([]interface{}, 0, 40)
		logAttrs = append(logAttrs,
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
		)
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
			// authUser is the Basic-Auth username, read from the Authorization
			// header — credential material. We never emit the value (or anything
			// derived from it) to the log, only the auth *mechanism* (a constant
			// like "basic_auth"). Identity for audit is carried by the
			// trusted-proxy enduser.* fields instead. This keeps credential data
			// out of logs entirely (CodeQL go/clear-text-logging).
			logAttrs = append(logAttrs, "auth.source", authSource)
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

// translateBinOpSide translates one side of a binary expression.
// Scalar literal expressions (e.g. "100") are returned as-is without translation,
// since the translator cannot handle bare numeric literals and proxyBinaryMetric
// already detects them via translator.IsScalar.
func (p *Proxy) translateBinOpSide(ctx context.Context, expr logqlpkg.Expr) (string, error) {
	if lit, ok := expr.(*logqlpkg.LiteralExpr); ok {
		return lit.String(), nil
	}
	return p.translateQueryWithContext(ctx, expr.String())
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

	type translationResult struct {
		query string
		err   error
	}
	v, _, _ := p.translationGroup.Do(normalized, func() (interface{}, error) {
		if p.translationCache != nil {
			if cached, ok := p.translationCache.Get(normalized); ok {
				return translationResult{query: string(cached)}, nil
			}
		}

		p.configMu.RLock()
		labelFn := p.labelTranslator.ToVL
		streamFieldsMap := p.streamFieldsMap
		p.configMu.RUnlock()

		p.backendVersionMu.RLock()
		semver := p.backendVersionSemver
		p.backendVersionMu.RUnlock()
		caps := logsql.CapabilitiesFor(semver)

		translated, err := translator.TranslateLogQLWithCapabilities(normalized, labelFn, streamFieldsMap, caps)
		if err != nil {
			return translationResult{err: err}, nil
		}
		trimmed := strings.TrimSpace(translated)
		if strings.HasPrefix(trimmed, "|") {
			translated = "* " + trimmed
		}
		if p.translationCache != nil {
			p.translationCache.SetWithTTL(normalized, []byte(translated), 5*time.Minute)
		}
		return translationResult{query: translated}, nil
	})
	res := v.(translationResult)
	if res.err != nil {
		if p.metrics != nil {
			p.metrics.RecordTranslationError()
		}
		p.observeInternalOperation(ctx, "translate_query", "error", time.Since(start))
		return "", res.err
	}
	if p.metrics != nil {
		p.metrics.RecordTranslation()
	}
	p.observeInternalOperation(ctx, "translate_query", "translated", time.Since(start))
	return res.query, nil
}

var (
	jsonParserStageRE   = regexp.MustCompile(`\|\s*json(?:\s+[^|]+)?`)
	logfmtParserStageRE = regexp.MustCompile(`\|\s*logfmt(?:\s+[^|]+)?`)
	regexpParserStageRE = regexp.MustCompile(`\|\s*regexp\b`)

	// logqlOffsetRE matches the "offset <duration>" clause that appears after a
	// range window bracket, e.g. "[5m] offset 1h". Capture group 1 is the
	// duration string. Supports negative offsets: "[5m] offset -30m".
	logqlOffsetRE = regexp.MustCompile(`\]\s*offset\s+(-?[\w.]+)`)

	// rangeVectorRE matches LogQL range-vector duration brackets such as [5m], [1h],
	// [500ms], [1.5m]. Leading \d anchors the match to numeric durations, avoiding
	// false positives from regex character classes in filter expressions (e.g. [a-z]).
	rangeVectorRE = regexp.MustCompile(`\[\d[\d.]*[smhdwy]\w*\]`)
)

func hasParserStage(query, parser string) bool {
	if parser == "logfmt" {
		return hasParserStageOf(query, false, true)
	}
	return hasParserStageOf(query, true, false)
}

// hasLabelParserStage reports whether query has a | json or | logfmt stage,
// the parsers whose extracted labels join the stream label set. It parses the
// query once instead of once per parser.
func hasLabelParserStage(query string) bool {
	return hasParserStageOf(query, true, true)
}

func hasParserStageOf(query string, jsonStage, logfmtStage bool) bool {
	lq, err := logqlpkg.ParseLogQuery(query)
	if err != nil {
		return (jsonStage && jsonParserStageRE.MatchString(query)) ||
			(logfmtStage && logfmtParserStageRE.MatchString(query))
	}
	return pipelineHasParserStageOf(lq.Pipeline, jsonStage, logfmtStage)
}

func pipelineHasParserStageOf(pipeline []logqlpkg.Stage, jsonStage, logfmtStage bool) bool {
	for _, stage := range pipeline {
		ps, ok := stage.(*logqlpkg.ParserStage)
		if ok && ((jsonStage && ps.Type == logqlpkg.ParserJSON) || (logfmtStage && ps.Type == logqlpkg.ParserLogfmt)) {
			return true
		}
	}
	return false
}

func removeParserStage(query, parser string) string {
	lq, err := logqlpkg.ParseLogQuery(query)
	if err != nil {
		// Fall back to regex on parse failure.
		re := jsonParserStageRE
		if parser == "logfmt" {
			re = logfmtParserStageRE
		}
		result := re.ReplaceAllString(query, "")
		for strings.Contains(result, "  ") {
			result = strings.ReplaceAll(result, "  ", " ")
		}
		return strings.TrimSpace(result)
	}
	remove := logqlpkg.ParserJSON
	if parser == "logfmt" {
		remove = logqlpkg.ParserLogfmt
	}
	filtered := lq.Pipeline[:0]
	for _, stage := range lq.Pipeline {
		if ps, ok := stage.(*logqlpkg.ParserStage); ok && ps.Type == remove {
			continue
		}
		filtered = append(filtered, stage)
	}
	lq.Pipeline = filtered
	return lq.String()
}

// extractLogQLOffset finds a LogQL offset modifier (e.g. "[5m] offset 1h"),
// strips all occurrences from the query, and returns the offset duration.
//
// Errors when:
//   - multiple *different* offset values are present (Loki rejects such queries), or
//   - some range vectors carry an offset and others do not ("mixed" expression).
//     Mixed expressions cannot be handled by a global time shift: only the
//     offset vectors should look at historical data, while the unshifted ones
//     must evaluate at the original window — impossible with a single start/end
//     adjustment. Example: rate(a[5m] offset 1h) + rate(b[5m]) is rejected.
//
// Returns zero duration + unchanged query when no offset is found.
func extractLogQLOffset(logql string) (time.Duration, string, error) {
	matches := logqlOffsetRE.FindAllStringSubmatch(logql, -1)
	if len(matches) == 0 {
		return 0, logql, nil
	}

	// Detect mixed expressions: some range vectors have an offset, others do not.
	// rangeVectorRE counts all duration brackets ([5m], [1h], …); logqlOffsetRE
	// counts only those followed by an offset clause. If the counts differ, the
	// expression mixes offset and non-offset vectors.
	allVectors := rangeVectorRE.FindAllString(logql, -1)
	if len(allVectors) > len(matches) {
		return 0, logql, fmt.Errorf(
			"offset applied to %d of %d range vectors; loki-vl-proxy requires a uniform offset across all range vectors since it applies a global time shift — use a consistent offset or split into separate queries",
			len(matches), len(allVectors),
		)
	}

	seen := map[string]time.Duration{}
	for _, m := range matches {
		durStr := m[1]
		if _, already := seen[durStr]; !already {
			seen[durStr] = parseLokiDuration(durStr)
		}
	}
	if len(seen) > 1 {
		return 0, logql, fmt.Errorf("found %d distinct offsets while expecting at most 1", len(seen))
	}

	var offset time.Duration
	for _, d := range seen {
		offset = d
	}
	stripped := logqlOffsetRE.ReplaceAllString(logql, "]")
	return offset, strings.TrimSpace(stripped), nil
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

	probeKey := baseQuery + "\x00" + start + "\x00" + end
	v, _, _ := p.parserProbeGroup.Do(probeKey, func() (interface{}, error) {
		return p.probePreferredParser(ctx, baseQuery, start, end), nil
	})
	preferred := v.(string)

	switch preferred {
	case "json":
		p.observeInternalOperation(ctx, "prefer_working_parser", "prefer_json", time.Since(opStart))
		return removeParserStage(logql, "logfmt")
	case "logfmt":
		p.observeInternalOperation(ctx, "prefer_working_parser", "prefer_logfmt", time.Since(opStart))
		return removeParserStage(logql, "json")
	default:
		p.observeInternalOperation(ctx, "prefer_working_parser", "no_parser_signal", time.Since(opStart))
		return logql
	}
}

var metricParserProbeRE = regexp.MustCompile(`(?s)(?:count_over_time|bytes_over_time|rate|bytes_rate|rate_counter|sum_over_time|avg_over_time|max_over_time|min_over_time|first_over_time|last_over_time|stddev_over_time|stdvar_over_time|quantile_over_time)\((.*?)\[[^][]+\]\)`)

func (p *Proxy) probePreferredParser(ctx context.Context, baseQuery, start, end string) string {
	logsqlQuery, err := p.translateQueryWithContext(ctx, baseQuery)
	if err != nil {
		return ""
	}

	params := url.Values{}
	params.Set("query", logsqlQuery+" "+logsql.PipeSort{By: []logsql.SortField{{Field: "_time", Desc: true}}}.String())
	params.Set("limit", "25")
	if start != "" {
		params.Set("start", formatVLTimestamp(start))
	}
	if end != "" {
		params.Set("end", formatVLTimestamp(end))
	}

	resp, err := p.vlPost(ctx, "/select/logsql/query", params)
	if err != nil {
		return ""
	}
	defer resp.Body.Close()

	body, err := readBodyLimited(resp.Body, maxBufferedBackendBodyBytes)
	if err != nil || len(body) == 0 {
		return ""
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
		return ""
	case jsonHits >= logfmtHits:
		return "json"
	default:
		return "logfmt"
	}
}

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

// parseBareParserMetricCompatSpec parses a bare (non-aggregated) LogQL metric
// expression that wraps a log pipeline containing at least one extracting parser
// stage (json, logfmt, regexp, pattern, etc.). When the logql AST can parse the
// query (no Grafana template windows like $__interval), it uses typed nodes for
// extraction. Template-window queries that the parser rejects fall back to the
// regex-based path so runtime behaviour is identical.
func parseBareParserMetricCompatSpec(query string) (bareParserMetricCompatSpec, bool) {
	if spec, ok := parseBareParserMetricCompatSpecAST(strings.TrimSpace(query)); ok {
		return spec, true
	}
	return parseBareParserMetricCompatSpecRegex(strings.TrimSpace(query))
}

// parseBareParserMetricCompatSpecAST uses the logql typed AST. It returns
// (spec, false) on any parse or validation failure, signalling the caller to
// fall back to the regex implementation.
func parseBareParserMetricCompatSpecAST(query string) (bareParserMetricCompatSpec, bool) {
	expr, err := logqlpkg.Parse(query)
	if err != nil {
		return bareParserMetricCompatSpec{}, false
	}
	// Reject VectorAggregation wrappers (e.g. sum(count_over_time(...))) — the
	// outer aggregation means this is not a "bare" metric expression.
	if _, isVec := expr.(*logqlpkg.VectorAggregation); isVec {
		return bareParserMetricCompatSpec{}, false
	}
	ra, ok := expr.(*logqlpkg.RangeAggregation)
	if !ok {
		return bareParserMetricCompatSpec{}, false
	}
	// Reject expressions with an outer grouping clause such as `avg_over_time(...) by ()`.
	// Those are handled via the stats translation path (handleStatsCompatRange), not the
	// bare-parser-metric path. The regex implementation rejects them implicitly because
	// bareParserMetricCompatRE matches only up to the closing ')' before any trailing text.
	if ra.Grouping != nil {
		return bareParserMetricCompatSpec{}, false
	}
	lq, ok := ra.Inner.(*logqlpkg.LogQuery)
	if !ok {
		return bareParserMetricCompatSpec{}, false
	}

	// Require at least one extracting parser stage in the pipeline.
	hasParser := false
	for _, s := range lq.Pipeline {
		if _, isParser := s.(*logqlpkg.ParserStage); isParser {
			hasParser = true
			break
		}
	}
	if !hasParser {
		return bareParserMetricCompatSpec{}, false
	}

	windowRaw := ra.Range
	window, windowOK := parsePositiveStepDuration(windowRaw)
	if !windowOK {
		// A non-duration window would have caused a parse error already; guard anyway.
		return bareParserMetricCompatSpec{}, false
	}

	funcName := string(ra.Op)

	// Functions that require an unwrap stage.
	var unwrapField, unwrapConv string
	switch ra.Op {
	case logqlpkg.RangeRateCounter,
		logqlpkg.RangeSumOverTime,
		logqlpkg.RangeAvgOverTime,
		logqlpkg.RangeMaxOverTime,
		logqlpkg.RangeMinOverTime,
		logqlpkg.RangeFirstOverTime,
		logqlpkg.RangeLastOverTime,
		logqlpkg.RangeStddevOverTime,
		logqlpkg.RangeStdvarOverTime:
		// Find the UnwrapStage in the pipeline.
		for _, s := range lq.Pipeline {
			if uw, isUW := s.(*logqlpkg.UnwrapStage); isUW {
				unwrapField = uw.Label
				unwrapConv = uw.Converter
				break
			}
		}
		if unwrapField == "" {
			return bareParserMetricCompatSpec{}, false
		}
	case logqlpkg.RangeQuantileOverTime:
		if !ra.HasParam || ra.Param < 0 || ra.Param > 1 {
			return bareParserMetricCompatSpec{}, false
		}
		for _, s := range lq.Pipeline {
			if uw, isUW := s.(*logqlpkg.UnwrapStage); isUW {
				unwrapField = uw.Label
				unwrapConv = uw.Converter
				break
			}
		}
		if unwrapField == "" {
			return bareParserMetricCompatSpec{}, false
		}
		return bareParserMetricCompatSpec{
			funcName:        funcName,
			baseQuery:       lq.String(),
			rangeWindow:     window,
			rangeWindowExpr: windowRaw,
			unwrapField:     unwrapField,
			unwrapConv:      unwrapConv,
			quantile:        ra.Param,
		}, true
	}

	return bareParserMetricCompatSpec{
		funcName:        funcName,
		baseQuery:       lq.String(),
		rangeWindow:     window,
		rangeWindowExpr: windowRaw,
		unwrapField:     unwrapField,
		unwrapConv:      unwrapConv,
	}, true
}

// parseBareParserMetricCompatSpecRegex is the original regex-based fallback,
// used when the logql parser cannot handle the query (e.g. Grafana template
// windows like [$__interval] that are not valid duration literals).
func parseBareParserMetricCompatSpecRegex(query string) (bareParserMetricCompatSpec, bool) {
	if matches := bareParserQuantileCompatRE.FindStringSubmatch(query); len(matches) == 4 {
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
		unwrapField, unwrapConv := extractBareParserUnwrapExpr(baseQuery)
		if unwrapField == "" {
			return bareParserMetricCompatSpec{}, false
		}
		return bareParserMetricCompatSpec{
			funcName:        "quantile_over_time",
			baseQuery:       baseQuery,
			rangeWindow:     window,
			rangeWindowExpr: windowRaw,
			unwrapField:     unwrapField,
			unwrapConv:      unwrapConv,
			quantile:        phi,
		}, true
	}

	matches := bareParserMetricCompatRE.FindStringSubmatch(query)
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
	unwrapField, unwrapConv := "", ""
	switch matches[1] {
	case "rate_counter", "sum_over_time", "avg_over_time", "max_over_time", "min_over_time", "first_over_time", "last_over_time", "stddev_over_time", "stdvar_over_time":
		unwrapField, unwrapConv = extractBareParserUnwrapExpr(baseQuery)
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
		unwrapConv:      unwrapConv,
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
	field, _ := extractBareParserUnwrapExpr(query)
	return field
}

// extractBareParserUnwrapExpr returns (field, conv) from a | unwrap expression.
// conv is "duration" or "bytes" when a unit-conversion wrapper is present;
// empty when the unwrap references a raw numeric field.
func extractBareParserUnwrapExpr(query string) (field, conv string) {
	matches := bareParserUnwrapFieldRE.FindStringSubmatch(query)
	if len(matches) != 3 {
		return "", ""
	}
	if f := strings.TrimSpace(matches[1]); f != "" {
		// matches[1] is non-empty only when the duration()/bytes() wrapper is present.
		return f, inferUnwrapConv(query)
	}
	return strings.TrimSpace(matches[2]), ""
}

func inferUnwrapConv(query string) string {
	if strings.Contains(query, "duration(") {
		return "duration"
	}
	if strings.Contains(query, "bytes(") {
		return "bytes"
	}
	return ""
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

// fetchBareParserMetricSeries fetches log series for bare-parser metric queries.
// Caching keyed by (query, start, end) would reduce redundant VL fetches within a single
// Grafana render cycle but requires a request-scoped cache — tracked as a separate concern.
func (p *Proxy) fetchBareParserMetricSeries(ctx context.Context, originalQuery string, spec bareParserMetricCompatSpec, start, end string) ([]bareParserMetricSeries, error) {
	logsqlQuery, err := p.translateQueryWithContext(ctx, spec.baseQuery)
	if err != nil {
		return nil, err
	}

	params := url.Values{}
	rowLimit, err := p.manualMetricRowBudget()
	if err != nil {
		return nil, err
	}
	// Stream complete bounded input; sorting at VL retains every candidate row.
	params.Set("query", logsqlQuery+" | limit "+strconv.Itoa(rowLimit+1))
	if start != "" {
		params.Set("start", formatVLTimestamp(start))
	}
	if end != "" {
		if endNanos, ok := parseFlexibleUnixNanos(end); ok {
			params.Set("end", time.Unix(0, endNanos).Add(time.Nanosecond).UTC().Format(time.RFC3339Nano))
		} else {
			params.Set("end", formatVLTimestamp(end))
		}
	}

	resp, err := p.vlPost(ctx, "/select/logsql/query", params)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= http.StatusBadRequest {
		body, _ := readBodyLimited(resp.Body, maxUpstreamErrorBodyBytes)
		return nil, p.redactedBackendStatusError("", resp.StatusCode, body)
	}

	limited := &io.LimitedReader{R: resp.Body, N: maxBufferedBackendBodyBytes + 1}
	scanner := bufio.NewScanner(limited)
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

	// Include parsed fields in metric labels only when the base query has a
	// post-parser pipe stage (e.g. "| json | status >= 500"). Without such a
	// stage, grouping is by stream labels only — matching the native VL stats
	// behaviour that bare rate(| json) used before the __error__ slow-path guard
	// was added. With a post-parser filter, the extracted label values are part
	// of the series identity (as in Loki).
	includeParsedInMetric := hasPostParserPipeStage(spec.baseQuery)

	rows := 0
	for scanner.Scan() {
		if err := checkManualMetricRead(ctx, limited); err != nil {
			return nil, err
		}
		line := bytes.TrimSpace(scanner.Bytes())
		if len(line) == 0 {
			continue
		}
		rows++
		if rows > rowLimit {
			return nil, fmt.Errorf("manual range metric row limit exceeded (%d)", rowLimit)
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
		weight, ok := bareParserRawSampleWeight(entry, spec)
		if !ok {
			vlEntryPool.Put(entry)
			continue
		}
		desc := p.logQueryStreamDescriptor(asString(entry["_stream"]), asString(entry["level"]), streamLabelCache, streamDescriptorCache)
		metric := cloneStringMap(desc.translatedLabels)
		if includeParsedInMetric {
			_, parsedFields := p.classifyEntryMetadataFields(entry, desc.rawLabels, true, exposureCache, smBuf, pfBuf)
			for key, value := range parsedFields {
				if spec.unwrapField != "" && key == spec.unwrapField {
					continue
				}
				metric[key] = value
			}
		}
		seriesKey := canonicalLabelsKey(metric)
		series, ok := seriesByKey[seriesKey]
		if !ok {
			if len(seriesByKey) >= p.resolvedMaxStatsQuerySeries() {
				vlEntryPool.Put(entry)
				return nil, fmt.Errorf("maximum metric series exceeded (%d)", p.resolvedMaxStatsQuerySeries())
			}
			series = &bareParserMetricSeries{
				metric:  metric,
				samples: make([]bareParserMetricSample, 0, 8),
			}
			seriesByKey[seriesKey] = series
		}
		series.samples = append(series.samples, bareParserMetricSample{tsNanos: tsNanos, value: weight})
		vlEntryPool.Put(entry)
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	if err := checkManualMetricRead(ctx, limited); err != nil {
		return nil, err
	}

	result := make([]bareParserMetricSeries, 0, len(seriesByKey))
	for _, series := range seriesByKey {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		sort.Slice(series.samples, func(i, j int) bool { return series.samples[i].tsNanos < series.samples[j].tsNanos })
		result = append(result, *series)
	}
	sort.Slice(result, func(i, j int) bool {
		return canonicalLabelsKey(result[i].metric) < canonicalLabelsKey(result[j].metric)
	})
	return result, nil
}

// fetchBareParserMetricSeriesViaHits is the fast path for count_over_time / rate
// sliding windows when stream label fields are declared. Instead of fetching raw
// log entries, it calls VL's /select/logsql/hits endpoint, which returns line
// counts per bucket grouped by the declared fields — no log body transfer.
// Buckets use the anchored grid of slidingStatsBucket (hits accepts the same
// start, end, step and offset args as stats_query_range) and are labelled by
// their left edge, ready for buildHitsRangeMetricMatrix. ok is false when no
// bucket grid holds every evaluation window edge.
//
// evalStart, evalEnd, stepNs are in nanoseconds.
func (p *Proxy) fetchBareParserMetricSeriesViaHits(
	ctx context.Context,
	spec bareParserMetricCompatSpec,
	evalStart, evalEnd, stepNs int64,
) (series map[string]manualSeriesSamples, ok bool, err error) {
	bucket, ok := p.slidingStatsBucket(time.Unix(0, evalStart), time.Duration(stepNs), spec.rangeWindow)
	if !ok {
		return nil, false, nil
	}
	logsqlQuery, err := p.translateQueryWithContext(ctx, spec.baseQuery)
	if err != nil {
		return nil, true, err
	}

	// The first evaluation window starts one range before evalStart.
	fetchStart := time.Unix(0, evalStart).Add(-spec.rangeWindow)
	params := url.Values{}
	params.Set("query", logsqlQuery)
	p.setSlidingStatsRangeParams(params, fetchStart, time.Unix(0, evalEnd), bucket)

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
		return nil, true, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= http.StatusBadRequest {
		body, _ := readBodyLimited(resp.Body, maxUpstreamErrorBodyBytes)
		return nil, true, p.redactedBackendStatusError("hits: status", resp.StatusCode, body)
	}

	body, err := readBodyLimited(resp.Body, maxBufferedBackendBodyBytes)
	if err != nil {
		return nil, true, err
	}
	hits := parseHits(body)

	series = make(map[string]manualSeriesSamples, len(hits.Hits))
	for _, hit := range hits.Hits {
		// Translate VL field names to Loki label names.
		translated := make(map[string]string, len(hit.Fields))
		for k, v := range hit.Fields {
			translated[lt.ToLoki(k)] = v
		}
		key := canonicalLabelsKey(translated)
		entry, seen := series[key]
		if !seen {
			entry = manualSeriesSamples{Metric: translated, Samples: make([]rangeMetricSample, 0, len(hit.Timestamps))}
		}
		for i, ts := range hit.Timestamps {
			tsNanos, tsOK := parseFlexibleUnixNanos(string(ts))
			if !tsOK || i >= len(hit.Values) || hit.Values[i] == 0 {
				continue
			}
			entry.Samples = append(entry.Samples, rangeMetricSample{ts: snapSlidingBucketNanos(tsNanos, fetchStart, bucket), value: float64(hit.Values[i])})
		}
		series[key] = entry
	}
	// Keep the busiest series, as the stats bucket path does.
	return capSeriesByTotalCount(series, p.resolvedMaxStatsQuerySeries()), true, nil
}

// fetchBareParserStatsBuckets fetches per-stream stats_query_range buckets for a
// bare parser range metric, avoiding the raw log fetch and its row limit on long
// ranges. statsAggFunc is the stats clause appended after "| stats by (_stream)",
// e.g. "count() as c", "sum_len(_msg) as c, count() as __sample_count" (byte
// sums with line presence) or "max(duration) as c". Buckets use the anchored grid
// of slidingStatsBucket and are labelled by their left edge, so the Loki window
// (t-range, t] is the union of the buckets labelled in [t-range, t). Series carry
// the stream labels. ok is false when no bucket grid holds every evaluation
// window edge; callers then use the raw evaluator.
//
// evalStart, evalEnd, stepNs are in nanoseconds.
func (p *Proxy) fetchBareParserStatsBuckets(
	ctx context.Context,
	spec bareParserMetricCompatSpec,
	statsAggFunc string,
	evalStart, evalEnd, stepNs int64,
) (seriesMap map[string]manualSeriesSamples, ok bool, err error) {
	bucket, ok := p.slidingStatsBucket(time.Unix(0, evalStart), time.Duration(stepNs), spec.rangeWindow)
	if !ok {
		return nil, false, nil
	}
	logsqlQuery, err := p.translateQueryWithContext(ctx, spec.baseQuery)
	if err != nil {
		return nil, true, err
	}

	// The first evaluation window starts one range before evalStart.
	fetchStart := time.Unix(0, evalStart).Add(-spec.rangeWindow)
	params := url.Values{}
	params.Set("query", logsqlQuery+" | stats by (_stream) "+statsAggFunc)
	p.setSlidingStatsRangeParams(params, fetchStart, time.Unix(0, evalEnd), bucket)

	resp, err := p.vlPost(ctx, "/select/logsql/stats_query_range", params)
	if err != nil {
		return nil, true, err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= http.StatusBadRequest {
		body, _ := readBodyLimited(resp.Body, maxUpstreamErrorBodyBytes)
		return nil, true, p.redactedBackendStatusError("stats_query_range", resp.StatusCode, body)
	}

	const maxBytes = 64 << 20
	body, err := readBodyLimited(resp.Body, maxBytes)
	if err != nil {
		return nil, true, err
	}

	v, err := fj.ParseBytes(body)
	if err != nil {
		return nil, true, fmt.Errorf("parse stats_query_range: %w", err)
	}
	if status := string(v.GetStringBytes("status")); status != "success" {
		return nil, true, fmt.Errorf("stats_query_range non-success status: %s", status)
	}

	p.configMu.RLock()
	lt := p.labelTranslator
	p.configMu.RUnlock()

	results := v.GetArray("data", "result")
	// Cap to the busiest maxStatsQuerySeries by total value to bound response
	// size for high-cardinality streams (churn-heavy pod names), mirroring the
	// clamp in collectRangeMetricHits. Ranking by value (not VL's alphabetical
	// order) keeps the signal, not the noise floor. Default 500 matches
	// maxDrilldownSeries and Loki's default max_query_series. Byte sums with
	// line presence are capped as complete logical series below.
	withPresence := strings.Contains(statsAggFunc, "as __sample_count")
	if !withPresence {
		results = capStatsResultsByTotalCount(results, p.resolvedMaxStatsQuerySeries())
	}
	seriesMap = make(map[string]manualSeriesSamples, len(results))
	streamLabelCache := make(map[string]map[string]string, len(results))

	for _, res := range results {
		metricObj := res.GetObject("metric")
		streamStr := string(metricObj.Get("_stream").GetStringBytes())

		baseLabels, cached := streamLabelCache[streamStr]
		if !cached {
			baseLabels = parseStreamLabels(streamStr)
			streamLabelCache[streamStr] = baseLabels
		}

		metric := make(map[string]string, len(baseLabels))
		for k, lv := range baseLabels {
			if lt != nil {
				metric[lt.ToLoki(k)] = lv
			} else {
				metric[k] = lv
			}
		}
		ensureDetectedLevel(metric)
		ensureSyntheticServiceName(metric)

		seriesKey := canonicalLabelsKey(metric)
		entry := seriesMap[seriesKey]
		entry.Metric = metric
		merged := len(entry.Samples) > 0
		values := res.GetArray("values")
		if withPresence && string(metricObj.Get("__name__").GetStringBytes()) == "__sample_count" {
			addPresentBuckets(&entry, values, fetchStart, bucket)
			seriesMap[seriesKey] = entry
			continue
		}
		for _, pair := range values {
			arr := pair.GetArray()
			if len(arr) < 2 {
				continue
			}
			ts, tsOK := snapSlidingBucketTimestamp(arr[0], fetchStart, bucket)
			val, valErr := strconv.ParseFloat(string(arr[1].GetStringBytes()), 64)
			if !tsOK || valErr != nil || math.IsNaN(val) || math.IsInf(val, 0) {
				continue
			}
			entry.Samples = append(entry.Samples, rangeMetricSample{ts: ts, value: val})
		}
		if merged {
			// Streams that translate to one label set: keep time order for first/last.
			sort.Slice(entry.Samples, func(i, j int) bool { return entry.Samples[i].ts < entry.Samples[j].ts })
		}
		seriesMap[seriesKey] = entry
	}
	if withPresence {
		seriesMap = capSeriesByTotalCount(seriesMap, p.resolvedMaxStatsQuerySeries())
	}
	return seriesMap, true, nil
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
	baseQuery      string
	rangeWindow    time.Duration
	rangeWindowStr string
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
	return absentOverTimeCompatSpec{baseQuery: baseQuery, rangeWindow: window, rangeWindowStr: matches[2]}, true
}

func extractAbsentMetricLabels(query string) map[string]string {
	expr, err := logqlpkg.Parse(strings.TrimSpace(query))
	if err != nil {
		return map[string]string{}
	}
	lq, ok := expr.(*logqlpkg.LogQuery)
	if !ok {
		return map[string]string{}
	}
	labels := make(map[string]string, len(lq.Selector.Matchers))
	for _, m := range lq.Selector.Matchers {
		if m.Op == logqlpkg.MatchEq {
			labels[m.Name] = m.Value
		}
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
	// Translate the inner count_over_time query — absent_over_time itself has no VL equivalent.
	if spec.rangeWindowStr == "" {
		p.writeError(w, http.StatusBadRequest, "absent_over_time: missing range window")
		p.metrics.RecordRequest("query", http.StatusBadRequest, time.Since(start))
		return
	}
	innerCountQuery := fmt.Sprintf("count_over_time(%s[%s])", spec.baseQuery, spec.rangeWindowStr)
	logsqlQuery, err := p.translateQueryWithContext(r.Context(), innerCountQuery)
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
		code := p.writeBackendError(w, resp.StatusCode, body)
		p.metrics.RecordRequest("query", code, time.Since(start))
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

// proxyAbsentOverTimeQueryRange handles absent_over_time at /query_range by
// running the underlying count query and inverting: steps with count > 0 are
// omitted (stream present); steps with count == 0 or absent emit value "1".
// When the stream does not exist at all VL returns an empty matrix, so all
// steps in [start, end] get value "1" (matching Loki semantics).
func (p *Proxy) proxyAbsentOverTimeQueryRange(w http.ResponseWriter, r *http.Request, start time.Time, originalQuery string, spec absentOverTimeCompatSpec) {
	// Translate the inner count_over_time query — absent_over_time itself has no VL equivalent.
	if spec.rangeWindowStr == "" {
		p.writeError(w, http.StatusBadRequest, "absent_over_time: missing range window")
		p.metrics.RecordRequest("query_range", http.StatusBadRequest, time.Since(start))
		return
	}
	innerCountQuery := fmt.Sprintf("count_over_time(%s[%s])", spec.baseQuery, spec.rangeWindowStr)
	translatedInner, err := p.translateQueryWithContext(r.Context(), innerCountQuery)
	if err != nil {
		p.writeError(w, http.StatusBadRequest, err.Error())
		p.metrics.RecordRequest("query_range", http.StatusBadRequest, time.Since(start))
		return
	}

	bw := &bufferedResponseWriter{header: make(http.Header)}
	sc := &statusCapture{ResponseWriter: bw, code: 200}
	innerR := r.Clone(r.Context())
	p.proxyStatsQueryRange(sc, innerR, translatedInner)

	if sc.code >= http.StatusBadRequest {
		copyHeaders(w.Header(), bw.Header())
		w.WriteHeader(sc.code)
		_, _ = w.Write(bw.body)
		elapsed := time.Since(start)
		p.metrics.RecordRequest("query_range", sc.code, elapsed)
		p.queryTracker.Record("query_range", originalQuery, elapsed, true)
		return
	}

	metric := extractAbsentMetricLabels(spec.baseQuery)
	out := buildAbsentOverTimeMatrix(bw.body, r.FormValue("start"), r.FormValue("end"), r.FormValue("step"), metric)

	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(out)
	elapsed := time.Since(start)
	p.metrics.RecordRequest("query_range", http.StatusOK, elapsed)
	p.queryTracker.Record("query_range", originalQuery, elapsed, false)
}

// buildAbsentOverTimeMatrix inverts a count matrix into an absent matrix.
// Steps where count > 0 are omitted; steps where count == 0 or missing emit "1".
// If the count matrix is empty (stream does not exist), all steps emit "1".
func buildAbsentOverTimeMatrix(countBody []byte, startRaw, endRaw, stepRaw string, metric map[string]string) []byte {
	type matrixResult struct {
		Metric map[string]string `json:"metric"`
		Values [][]interface{}   `json:"values"`
	}
	var resp struct {
		Status string `json:"status"`
		Data   struct {
			ResultType string         `json:"resultType"`
			Result     []matrixResult `json:"result"`
		} `json:"data"`
	}
	_ = stdjson.Unmarshal(countBody, &resp)

	// Build the full step list from [start, end] at the requested step interval.
	startNs, okS := parseLokiTimeToUnixNano(startRaw)
	endNs, okE := parseLokiTimeToUnixNano(endRaw)
	stepDur, okStep := parsePositiveStepDuration(stepRaw)
	if !okS || !okE || !okStep || stepDur == 0 {
		// Cannot compute steps; return empty.
		return wrapAsLokiResponse([]byte(`{"result":[]}`), "matrix")
	}

	stepNs := stepDur.Nanoseconds()
	// Collect the timestamps of steps where the stream HAS data (count > 0).
	presentSteps := make(map[int64]bool)
	for _, series := range resp.Data.Result {
		for _, v := range series.Values {
			if len(v) < 2 {
				continue
			}
			var tsSeconds float64
			switch t := v[0].(type) {
			case float64:
				tsSeconds = t
			case string:
				f, _ := strconv.ParseFloat(t, 64)
				tsSeconds = f
			}
			val := fmt.Sprint(v[1])
			val = strings.Trim(val, "\"")
			count, _ := strconv.ParseFloat(val, 64)
			if count > 0 {
				presentSteps[int64(tsSeconds*1e9)] = true
			}
		}
	}

	// Emit "1" for every step that is NOT in presentSteps.
	// Align to epoch-multiple steps (same anchor VL and Loki use):
	//   alignedStart = ceil(startNs / stepNs) * stepNs
	alignedStart := ((startNs + stepNs - 1) / stepNs) * stepNs
	var absentValues [][]interface{}
	for ts := alignedStart; ts <= endNs; ts += stepNs {
		if !presentSteps[ts] {
			absentValues = append(absentValues, []interface{}{float64(ts) / 1e9, "1"})
		}
	}

	if len(absentValues) == 0 {
		return wrapAsLokiResponse([]byte(`{"result":[]}`), "matrix")
	}

	result := []matrixResult{{Metric: metric, Values: absentValues}}
	out, err := stdjson.Marshal(map[string]interface{}{
		"status": "success",
		"data": map[string]interface{}{
			"resultType": "matrix",
			"result":     result,
		},
	})
	if err != nil {
		return wrapAsLokiResponse([]byte(`{"result":[]}`), "matrix")
	}
	return out
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

// hasDropErrorOnlyPostParserStage returns true when the base query's only
// post-parser stage is exactly a "| drop __error__[, __error_details__]" clause.
// This indicates an explicit opt-in to VL's count-all semantics (count every
// log line, including parse failures) rather than Loki's default of excluding
// parse-failed lines from metric aggregation.
func hasDropErrorOnlyPostParserStage(baseQuery string) bool {
	end := lastParserStageEnd(baseQuery)
	if end < 0 {
		return false
	}
	tail := strings.TrimSpace(baseQuery[end:])
	if !strings.HasPrefix(tail, "|") {
		return false
	}
	return dropErrorOnlyRE.MatchString(tail)
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

func (p *Proxy) proxyBareParserMetricQueryRange(w http.ResponseWriter, r *http.Request, start time.Time, originalQuery string, spec bareParserMetricCompatSpec) {
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

	// Bucket fast path for count_over_time, rate, bytes_over_time and bytes_rate
	// with range >= step and no post-parser filter stages (range == step also
	// accepts a lone "| drop __error__" stage). Buckets avoid the 1M-row raw log
	// fetch that exhausts memory on long ranges (e.g. 24h with $__auto range).
	//
	// VL counts all log lines including those that fail parsing (e.g. non-JSON for
	// | json), while Loki excludes such lines from metric aggregation. In practice
	// the difference is negligible and far preferable to the OOM failures of the
	// raw fetch. Queries with post-parser filter stages (e.g. | status >= 400)
	// bypass this fast path because their filter semantics cannot be replicated by
	// VL stats alone.
	rangeNanos := spec.rangeWindow.Nanoseconds()
	if spec.unwrapField == "" && isLogRangeWindowFunc(spec.funcName) && rangeNanos >= stepNanos &&
		(!hasPostParserPipeStage(spec.baseQuery) || (rangeNanos == stepNanos && hasDropErrorOnlyPostParserStage(spec.baseQuery))) {
		if p.tryBareParserLogRangeBuckets(w, r, start, originalQuery, spec, startNanos, endNanos, stepNanos) {
			return
		}
	}

	// Stats fast path for unwrap aggregations that compose correctly from
	// buckets: sum, max, min. Skip when unwrapConv is set
	// (duration()/bytes()): VL operates on raw strings, not converted floats.
	if p.tryUnwrapViaStatsFastPath(w, r, start, originalQuery, spec, startNanos, endNanos, stepNanos) {
		return
	}

	// Loki's first evaluation includes the complete preceding range window.
	fetchStart := time.Unix(0, startNanos).Add(-spec.rangeWindow).UTC().Format(time.RFC3339Nano)
	series, err := p.fetchBareParserMetricSeries(r.Context(), originalQuery, spec, fetchStart, r.FormValue("end"))
	if err != nil {
		status := statusFromUpstreamErr(err)
		p.writeError(w, status, err.Error())
		p.metrics.RecordRequest("query_range", status, time.Since(start))
		p.queryTracker.Record("query_range", originalQuery, time.Since(start), true)
		return
	}
	p.writeBoundedBareParserMetric(w, r, start, originalQuery, series, startNanos, endNanos, stepNanos, spec, true)
}

// tryBareParserLogRangeBuckets answers a bare parser log-line range metric from
// anchored buckets: /hits for sliding count_over_time and rate when stream label
// fields are declared, otherwise stats_query_range by stream. Returns false,
// without writing, when no bucket grid holds every window edge or VL fails, so
// the caller falls through to the raw evaluator. A VL rejection counts as a
// failure too: these bucket queries are proxy rewrites, so only the raw
// evaluator decides whether the user's query is invalid.
func (p *Proxy) tryBareParserLogRangeBuckets(w http.ResponseWriter, r *http.Request, start time.Time, originalQuery string, spec bareParserMetricCompatSpec, startNanos, endNanos, stepNanos int64) bool {
	var (
		series map[string]manualSeriesSamples
		ok     bool
		err    error
	)
	p.configMu.RLock()
	hasDeclaredFields := len(p.declaredLabelFields) > 0
	p.configMu.RUnlock()
	if hasDeclaredFields && (spec.funcName == "rate" || spec.funcName == "count_over_time") &&
		spec.rangeWindow.Nanoseconds() > stepNanos && !hasPostParserPipeStage(spec.baseQuery) {
		series, ok, err = p.fetchBareParserMetricSeriesViaHits(r.Context(), spec, startNanos, endNanos, stepNanos)
		if err != nil {
			slog.WarnContext(r.Context(), "hits-based metric path failed, falling back to stats",
				"err", err, "query", redactQuery(originalQuery, p.debugLogRawQueries))
		}
	}
	if !ok || err != nil {
		statsAggFunc := "count() as c"
		if spec.funcName == "bytes_over_time" || spec.funcName == "bytes_rate" {
			// A byte sum of zero cannot tell an absent bucket from empty lines.
			statsAggFunc = "sum_len(_msg) as c, count() as __sample_count"
		}
		series, ok, err = p.fetchBareParserStatsBuckets(r.Context(), spec, statsAggFunc, startNanos, endNanos, stepNanos)
		if !ok {
			return false
		}
		if err != nil {
			slog.WarnContext(r.Context(), "stats bucket path failed, falling back to full-fetch",
				"err", err, "query", redactQuery(originalQuery, p.debugLogRawQueries))
			return false
		}
	}
	status := p.writeHitsRangeMetricMatrix(w, spec.funcName, series, time.Unix(0, startNanos), time.Unix(0, endNanos), time.Duration(stepNanos), spec.rangeWindow)
	elapsed := time.Since(start)
	p.metrics.RecordRequest("query_range", status, elapsed)
	p.queryTracker.Record("query_range", originalQuery, elapsed, status != http.StatusOK)
	return true
}

// tryUnwrapViaStatsFastPath attempts to satisfy an unwrap range aggregation using
// the VL stats endpoint (O(buckets) instead of O(log-entries)). Returns true if
// the response was written, false if the caller should fall through to the slow path.
func (p *Proxy) tryUnwrapViaStatsFastPath(w http.ResponseWriter, r *http.Request, start time.Time, originalQuery string, spec bareParserMetricCompatSpec, startNanos, endNanos, stepNanos int64) bool {
	if spec.unwrapField == "" || spec.unwrapConv != "" {
		return false
	}
	vlField := p.labelTranslator.ToVL(spec.unwrapField)
	var statsAggFunc, aggFunc string
	switch spec.funcName {
	case "sum_over_time":
		statsAggFunc, aggFunc = "sum("+vlField+") as c", "sum"
	case "max_over_time":
		statsAggFunc, aggFunc = "max("+vlField+") as c", "max"
	case "min_over_time":
		statsAggFunc, aggFunc = "min("+vlField+") as c", "min"
	}
	// first_over_time and last_over_time have no VictoriaLogs stats function
	// (lib/logstorage/stats_*.go has no first/last); VictoriaLogs rejects them as
	// a query parse error, so they go straight to the exact raw evaluator.
	if statsAggFunc == "" {
		return false
	}
	uwSeries, ok, uwErr := p.fetchBareParserStatsBuckets(r.Context(), spec, statsAggFunc, startNanos, endNanos, stepNanos)
	if !ok {
		return false
	}
	if uwErr != nil {
		slog.WarnContext(r.Context(), "unwrap stats fast path failed, falling back to full-fetch",
			"err", uwErr, "query", redactQuery(originalQuery, p.debugLogRawQueries))
		return false
	}
	// The bucket (L, L+bucket] is labelled by its left edge L. Moving the label to
	// L+1ns puts it inside the inclusive evaluation interval [t-range, t] exactly
	// when the bucket lies inside Loki's window (t-range, t].
	for _, entry := range uwSeries {
		for i := range entry.Samples {
			entry.Samples[i].ts++
		}
	}
	startT := time.Unix(0, startNanos)
	endT := time.Unix(0, endNanos)
	stepD := time.Duration(stepNanos)
	// Retain the established top-N behavior for pre-aggregated stats, while
	// propagating byte/sample budget failures instead of returning empty success.
	uwSeries = capSeriesByTotalCount(uwSeries, p.resolvedMaxStatsQuerySeries())
	result, err := buildManualRangeMetricMatrixContext(r.Context(), aggFunc, 0, uwSeries, startT, endT, stepD, spec.rangeWindow, p.resolvedMaxStatsQuerySeries())
	if err != nil {
		p.writeError(w, statusFromUpstreamErr(err), err.Error())
		return true
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(result) // nosemgrep: go.lang.security.audit.xss.no-direct-write-to-responsewriter
	elapsed := time.Since(start)
	p.metrics.RecordRequest("query_range", http.StatusOK, elapsed)
	p.queryTracker.Record("query_range", originalQuery, elapsed, false)
	return true
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
	p.writeBoundedBareParserMetric(w, r, start, originalQuery, series, evalNanos, evalNanos, int64(time.Second), spec, false)
}

// statsTranslateFJPool pools fastjson.Parser for translateStatsResponseLabels.
var statsTranslateFJPool fj.ParserPool

// translateStatsResponseLabelsWithContext remaps VL stats response label names to Loki conventions.
// Uses fastjson for in-place manipulation — lower allocation than encoding/json with typed structs.
//
//nolint:gocyclo // walks VL stats JSON shape variants and remaps field/label names to Loki conventions across many edge cases; branching is inherent to schema translation.
func (p *Proxy) translateStatsResponseLabelsWithContext(ctx context.Context, body []byte, originalQuery string) []byte {
	start := time.Now()

	parser := statsTranslateFJPool.Get()
	defer statsTranslateFJPool.Put(parser)

	v, err := parser.ParseBytes(body)
	if err != nil {
		p.observeInternalOperation(ctx, "translate_stats_response_labels", "decode_error", time.Since(start))
		return body
	}

	// Locate all result arrays: data.result, result, results.
	type resultSlot struct {
		items  []*fj.Value
		key    string // "result" or "results"
		inData bool
	}
	var slots []resultSlot

	if data := v.Get("data"); data != nil {
		if r := data.Get("result"); r != nil && r.Type() == fj.TypeArray {
			if arr, _ := r.Array(); len(arr) > 0 {
				slots = append(slots, resultSlot{items: arr, key: "result", inData: true})
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
		p.observeInternalOperation(ctx, "translate_stats_response_labels", "no_results", time.Since(start))
		return body
	}

	// Reuse maps across iterations (same pattern as original).
	translated := make(map[string]string, 8)
	syntheticLabels := make(map[string]string, 8)

	// changedMetrics[si][ii] holds the new translated label map for changed items; nil = unchanged.
	// Storing map[string]string instead of pre-serialized []byte avoids the appendJSONString
	// allocations in the first pass; the write pass serialises directly to the output buffer.
	changedMetrics := make([][]map[string]string, len(slots))
	for i, slot := range slots {
		changedMetrics[i] = make([]map[string]string, len(slot.items))
	}

	translatedCount := 0

	for si, slot := range slots {
		for ii, item := range slot.items {
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
						// Coalesce: prefer non-empty when multiple source fields map
						// to the same Loki key (e.g. "service.name" and "service_name").
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
					// Coalesce: prefer non-empty when multiple source fields map
					// to the same Loki key (e.g. "service.name" and "service_name").
					if val != "" || translated[lokiKey] == "" {
						translated[lokiKey] = val
					}
				}
			})

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
				translatedCount++
				changedMetrics[si][ii] = cloneStringMap(syntheticLabels)
			}
		}
	}

	if translatedCount == 0 {
		p.observeInternalOperation(ctx, "translate_stats_response_labels", "noop", time.Since(start))
		return body
	}

	// Rebuild the response JSON, substituting changed metric objects.
	// Always write "status":"success" first so wrapAsLokiResponse fast path A matches
	// and returns the buffer zero-alloc instead of splicing a new []byte.
	buf := jsonBufPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer jsonBufPool.Put(buf)
	buf.Grow(len(body) + len(`{"status":"success",`))

	scratch := fjMarshalPool.Get().(*[]byte)
	defer fjMarshalPool.Put(scratch)

	buf.WriteString(`{"status":"success"`)
	needsComma := true

	if data := v.Get("data"); data != nil {
		si := -1
		for i, s := range slots {
			if s.inData {
				si = i
				break
			}
		}
		buf.WriteString(`,"data":{`)
		if rt := data.Get("resultType"); rt != nil {
			buf.WriteString(`"resultType":`)
			marshalFJ(buf, rt, scratch)
			buf.WriteByte(',')
		}
		buf.WriteString(`"result":`)
		if si >= 0 {
			writeTranslatedStatsItemsFJ(buf, slots[si].items, changedMetrics[si], scratch)
		} else {
			if r := data.Get("result"); r != nil {
				marshalFJ(buf, r, scratch)
			} else {
				buf.WriteString(`[]`)
			}
		}
		if statsF := data.Get("stats"); statsF != nil {
			buf.WriteString(`,"stats":`)
			marshalFJ(buf, statsF, scratch)
		}
		buf.WriteByte('}')
		needsComma = true
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
		writeTranslatedStatsItemsFJ(buf, slot.items, changedMetrics[si], scratch)
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
	p.observeInternalOperation(ctx, "translate_stats_response_labels", "translated", time.Since(start))
	return result
}

// writeTranslatedStatsItemsFJ writes a JSON array of stats items, substituting
// changedMetrics[i] (translated label map) where non-nil; copies original bytes otherwise.
func writeTranslatedStatsItemsFJ(buf *bytes.Buffer, items []*fj.Value, changedMetrics []map[string]string, scratch *[]byte) {
	buf.WriteByte('[')
	for i, item := range items {
		if i > 0 {
			buf.WriteByte(',')
		}
		if changedMetrics[i] != nil {
			buf.WriteString(`{"metric":`)
			marshalStringMapJSONTo(buf, changedMetrics[i])
			if val := item.Get("value"); val != nil {
				buf.WriteString(`,"value":`)
				marshalFJ(buf, val, scratch)
			}
			if vals := item.Get("values"); vals != nil {
				buf.WriteString(`,"values":`)
				marshalFJ(buf, vals, scratch)
			}
			buf.WriteByte('}')
		} else {
			marshalFJ(buf, item, scratch)
		}
	}
	buf.WriteByte(']')
}
