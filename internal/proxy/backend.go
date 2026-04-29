package proxy

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	mw "github.com/ReliablyObserve/Loki-VL-proxy/internal/middleware"
)

func deriveRequestType(endpoint, route string) string {
	endpoint = strings.TrimSpace(endpoint)
	if endpoint != "" {
		return endpoint
	}
	route = strings.TrimSpace(route)
	if route == "" {
		return "unknown"
	}
	if requestType, ok := upstreamRequestTypeByRoute[route]; ok {
		return requestType
	}
	trimmed := strings.Trim(route, "/")
	if trimmed == "" {
		return "unknown"
	}
	trimmed = strings.ReplaceAll(trimmed, "/", "_")
	return trimmed
}

func deriveUpstreamRequestType(route string) string {
	return deriveRequestType("", route)
}

func normalizeObservedRoute(route string) string {
	route = strings.TrimSpace(route)
	if route == "" {
		return "/unknown"
	}
	return route
}

func (p *Proxy) recordUpstreamObservation(ctx context.Context, system, method, route, serverAddress string, serverPort int, statusCode int, duration time.Duration, err error) {
	meta := requestRouteMetaFromContext(ctx)
	requestType := deriveUpstreamRequestType(route)
	observedRoute := normalizeObservedRoute(route)
	parentRequestType := deriveRequestType(meta.endpoint, meta.route)

	recordUpstreamCall(ctx, system, requestType, statusCode, duration, err != nil)

	p.metrics.RecordUpstreamRequest(system, requestType, observedRoute, statusCode, duration)
	if system == "vl" {
		p.metrics.RecordBackendDurationWithRoute(requestType, observedRoute, duration)
	}

	level := slog.LevelInfo
	if err != nil || statusCode >= http.StatusInternalServerError {
		level = slog.LevelError
	} else if statusCode >= http.StatusBadRequest {
		level = slog.LevelWarn
	}
	if !p.log.Enabled(ctx, level) {
		return
	}

	logAttrs := []interface{}{
		"http.route", observedRoute,
		"url.path", observedRoute,
		"http.request.method", method,
		"http.response.status_code", statusCode,
		"loki.request.type", requestType,
		"loki.api.system", system,
		"proxy.direction", "upstream",
		"event.duration", duration.Nanoseconds(),
		"loki.tenant.id", getOrgID(ctx),
	}
	if parentRequestType != "" && parentRequestType != "unknown" {
		logAttrs = append(logAttrs, "loki.parent_request.type", parentRequestType)
	}
	if parentRoute := strings.TrimSpace(meta.route); parentRoute != "" {
		logAttrs = append(logAttrs, "http.parent_route", parentRoute)
	}
	if serverAddress != "" {
		logAttrs = append(logAttrs, "server.address", serverAddress)
	}
	if serverPort > 0 {
		logAttrs = append(logAttrs, "server.port", serverPort)
	}
	if err != nil {
		logAttrs = append(logAttrs,
			"error.type", "transport",
			"error.message", err.Error(),
		)
	}
	p.log.Log(ctx, level, "upstream_request", logAttrs...)
}

func (p *Proxy) observeInternalOperation(ctx context.Context, operation, outcome string, duration time.Duration) {
	if duration < 0 {
		duration = 0
	}
	recordInternalOperation(ctx, operation, outcome, duration)
	if p != nil && p.metrics != nil {
		p.metrics.RecordInternalOperation(operation, outcome, duration)
	}
}

// --- Backend version detection ---

func (p *Proxy) observeBackendVersionFromHeaders(headers http.Header) {
	if headers == nil {
		return
	}
	candidates := []string{
		headers.Get("Server"),
		headers.Get("X-App-Version"),
		headers.Get("X-Backend-Version"),
		headers.Get("X-Victoriametrics-Build"),
	}
	for _, candidate := range candidates {
		candidate = strings.TrimSpace(candidate)
		if candidate == "" {
			continue
		}
		semver := extractBackendSemver(candidate)
		if semver == "" {
			continue
		}
		p.storeBackendVersion(candidate, semver)
		return
	}
}

func extractBackendSemver(raw string) string {
	return backendSemverPattern.FindString(raw)
}

func extractBackendSemverFromMetrics(metricsText string) string {
	metricsText = strings.TrimSpace(metricsText)
	if metricsText == "" {
		return ""
	}
	for _, line := range strings.Split(metricsText, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if !strings.Contains(line, "vm_app_version{") && !strings.Contains(line, "victorialogs_build_info{") {
			continue
		}
		if match := backendMetricsQuotedVersionPattern.FindStringSubmatch(line); len(match) == 2 {
			return match[1]
		}
		if fallback := extractBackendSemver(line); fallback != "" {
			return fallback
		}
	}
	return ""
}

func parseSemverTriplet(version string) (int, int, int, bool) {
	version = strings.TrimSpace(strings.TrimPrefix(version, "v"))
	if version == "" {
		return 0, 0, 0, false
	}
	parts := strings.Split(version, ".")
	if len(parts) < 3 {
		return 0, 0, 0, false
	}
	major, err := strconv.Atoi(numericPrefix(parts[0]))
	if err != nil {
		return 0, 0, 0, false
	}
	minor, err := strconv.Atoi(numericPrefix(parts[1]))
	if err != nil {
		return 0, 0, 0, false
	}
	patch, err := strconv.Atoi(numericPrefix(parts[2]))
	if err != nil {
		return 0, 0, 0, false
	}
	return major, minor, patch, true
}

func normalizeSemverString(version string) string {
	version = strings.TrimSpace(version)
	if version == "" {
		return ""
	}
	if version[0] >= '0' && version[0] <= '9' {
		return "v" + version
	}
	return version
}

func numericPrefix(in string) string {
	in = strings.TrimSpace(in)
	if in == "" {
		return ""
	}
	var b strings.Builder
	for _, r := range in {
		if r < '0' || r > '9' {
			break
		}
		b.WriteRune(r)
	}
	return b.String()
}

func semverAtLeastSemver(version, minVersion string) bool {
	vMaj, vMin, vPatch, vok := parseSemverTriplet(version)
	mMaj, mMin, mPatch, mok := parseSemverTriplet(minVersion)
	if !vok || !mok {
		return false
	}
	if vMaj != mMaj {
		return vMaj > mMaj
	}
	if vMin != mMin {
		return vMin > mMin
	}
	return vPatch >= mPatch
}

func semverAtLeast(semver string, major, minor, patch int) bool {
	mj, mn, pt, ok := parseSemverTriplet(semver)
	if !ok {
		return false
	}
	if mj != major {
		return mj > major
	}
	if mn != minor {
		return mn > minor
	}
	return pt >= patch
}

func deriveBackendCapabilities(semver string) (profile string, supportsStreamMetadata bool, supportsDensePatternWindowing bool, supportsMetadataSubstring bool) {
	switch {
	case semverAtLeast(semver, 1, 50, 0):
		return "vl-v1.50-plus", true, true, true
	case semverAtLeast(semver, 1, 49, 0):
		return "vl-v1.49-plus", true, false, true
	case semverAtLeast(semver, 1, 30, 0):
		return "vl-v1.30-plus", true, false, false
	default:
		return "legacy-pre-v1.30", false, false, false
	}
}

func (p *Proxy) storeBackendVersion(raw, semver string) {
	if semver == "" {
		return
	}
	p.backendVersionMu.Lock()
	defer p.backendVersionMu.Unlock()
	// Keep first detected version to avoid churn from intermediaries rewriting headers.
	if p.backendVersionSemver != "" {
		return
	}
	profile, supportsStreamMetadata, supportsDensePatternWindowing, supportsMetadataSubstring := deriveBackendCapabilities(semver)
	p.backendVersionRaw = raw
	p.backendVersionSemver = semver
	p.backendCapabilityProfile = profile
	p.backendSupportsStreamMetadata = supportsStreamMetadata
	p.backendSupportsDensePatternWindowing = supportsDensePatternWindowing
	p.backendSupportsMetadataSubstring = supportsMetadataSubstring
	if !p.backendVersionLogged {
		p.backendVersionLogged = true
		p.log.Info(
			"backend version detected",
			"backend.version.raw", raw,
			"backend.version.semver", semver,
			"backend.capability_profile", p.backendCapabilityProfile,
			"metadata.stream_endpoints", p.backendSupportsStreamMetadata,
			"metadata.substring_filter", p.backendSupportsMetadataSubstring,
			"patterns.dense_windowing", p.backendSupportsDensePatternWindowing,
		)
	}
}

func (p *Proxy) supportsStreamMetadataEndpoints() bool {
	p.backendVersionMu.RLock()
	defer p.backendVersionMu.RUnlock()
	// Before first backend version observation, optimistically keep stream-first
	// behavior so we don't regress newer deployments.
	if p.backendVersionSemver == "" {
		return true
	}
	return p.backendSupportsStreamMetadata
}

func (p *Proxy) supportsDensePatternWindowing() bool {
	p.backendVersionMu.RLock()
	defer p.backendVersionMu.RUnlock()
	return p.backendSupportsDensePatternWindowing
}

func (p *Proxy) supportsDensePatternWindowingForRequest(r *http.Request) bool {
	if !p.supportsDensePatternWindowing() {
		return false
	}
	if r == nil {
		return true
	}
	profile := grafanaClientProfileFromContext(r.Context())
	// Keep legacy Drilldown runtime family on conservative windowing even when backend
	// can do denser extraction. This mirrors 1.x behavior and avoids over-rendering
	// surprises on older Grafana runtimes.
	if profile.surface == "grafana_drilldown" && profile.runtimeMajor > 0 && profile.runtimeMajor < 12 {
		return false
	}
	return true
}

func (p *Proxy) supportsMetadataSubstringFilter() bool {
	p.backendVersionMu.RLock()
	defer p.backendVersionMu.RUnlock()
	return p.backendSupportsMetadataSubstring
}

func (p *Proxy) backendVersionState() (raw, semver, profile string) {
	p.backendVersionMu.RLock()
	defer p.backendVersionMu.RUnlock()
	return p.backendVersionRaw, p.backendVersionSemver, p.backendCapabilityProfile
}

func (p *Proxy) storeBackendCapabilityProbe(source string, supportsStreamMetadata, supportsMetadataSubstring bool) {
	if p == nil {
		return
	}
	p.backendVersionMu.Lock()
	defer p.backendVersionMu.Unlock()
	if p.backendVersionSemver != "" {
		return
	}
	p.backendVersionRaw = source
	switch {
	case supportsStreamMetadata && supportsMetadataSubstring:
		p.backendCapabilityProfile = "vl-probed-v1.49-plus"
	case supportsStreamMetadata:
		p.backendCapabilityProfile = "vl-probed-v1.30-plus"
	default:
		p.backendCapabilityProfile = "legacy-probed-pre-v1.30"
	}
	p.backendSupportsStreamMetadata = supportsStreamMetadata
	p.backendSupportsMetadataSubstring = supportsMetadataSubstring
	// Keep dense-windowing conservative without explicit semver evidence.
	p.backendSupportsDensePatternWindowing = false
	if !p.backendVersionLogged {
		p.backendVersionLogged = true
		p.log.Info(
			"backend capability profile inferred",
			"backend.version.source", source,
			"backend.capability_profile", p.backendCapabilityProfile,
			"metadata.stream_endpoints", p.backendSupportsStreamMetadata,
			"metadata.substring_filter", p.backendSupportsMetadataSubstring,
			"patterns.dense_windowing", p.backendSupportsDensePatternWindowing,
		)
	}
}

func (p *Proxy) probeBackendVersionFromMetrics(ctx context.Context) {
	if p == nil {
		return
	}
	resp, err := p.vlGet(ctx, "/metrics", nil)
	if err != nil {
		p.log.Debug("backend version metrics probe failed", "error", err)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode >= http.StatusBadRequest {
		p.log.Debug("backend version metrics probe returned non-success status", "http.response.status_code", resp.StatusCode)
		return
	}

	const maxMetricsProbeBytes = 2 << 20 // 2 MiB
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxMetricsProbeBytes))
	if err != nil {
		p.log.Debug("backend version metrics probe read failed", "error", err)
		return
	}
	semver := extractBackendSemverFromMetrics(string(body))
	if semver == "" {
		p.log.Debug("backend version metrics probe found no semver")
		return
	}
	p.storeBackendVersion("metrics:"+semver, semver)
}

func (p *Proxy) probeBackendEndpointSupport(ctx context.Context, path string, params url.Values) bool {
	if p == nil {
		return false
	}
	resp, err := p.vlGet(ctx, path, params)
	if err != nil {
		p.log.Debug("backend endpoint probe failed", "path", path, "error", err)
		return false
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
	return resp.StatusCode < http.StatusBadRequest
}

func (p *Proxy) probeBackendCapabilitiesFromEndpoints(ctx context.Context) {
	if p == nil {
		return
	}
	q := url.Values{}
	q.Set("query", "*")
	supportsStreamMetadata := p.probeBackendEndpointSupport(ctx, "/select/logsql/stream_field_names", q)

	sub := url.Values{}
	sub.Set("query", "*")
	sub.Set("q", "a")
	sub.Set("filter", "substring")
	supportsMetadataSubstring := p.probeBackendEndpointSupport(ctx, "/select/logsql/field_names", sub)

	p.storeBackendCapabilityProbe("endpoint-probe", supportsStreamMetadata, supportsMetadataSubstring)
}

// ValidateBackendVersionCompatibility checks backend version support at startup.
// It blocks startup only when a detected backend version is below the configured
// minimum and the unsafe override is not enabled.
func (p *Proxy) ValidateBackendVersionCompatibility(ctx context.Context) error {
	if p == nil {
		return nil
	}
	timeout := p.backendVersionCheckTimeout
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	checkCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	resp, err := p.vlGet(checkCtx, "/health", nil)
	if err != nil {
		p.log.Warn(
			"backend version compatibility check skipped",
			"reason", "health probe failed",
			"error", err,
			"minimum_supported_version", p.backendMinVersion,
		)
		return nil
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode >= http.StatusBadRequest {
		p.log.Warn(
			"backend version compatibility check skipped",
			"reason", "health probe returned non-success status",
			"http.response.status_code", resp.StatusCode,
			"minimum_supported_version", p.backendMinVersion,
		)
		return nil
	}

	raw, semver, profile := p.backendVersionState()
	if semver == "" {
		p.probeBackendVersionFromMetrics(checkCtx)
		raw, semver, profile = p.backendVersionState()
	}
	if semver == "" {
		p.probeBackendCapabilitiesFromEndpoints(checkCtx)
		raw, semver, profile = p.backendVersionState()
	}
	if semver == "" && profile != "" {
		p.log.Warn(
			"backend version compatibility check partial",
			"reason", "explicit backend semver unavailable; inferred capability profile from runtime endpoint probes",
			"backend.version.source", raw,
			"backend.capability_profile", profile,
			"minimum_supported_version", p.backendMinVersion,
		)
		return nil
	}
	if semver == "" {
		p.log.Warn(
			"backend version compatibility check unavailable",
			"reason", "backend did not expose version in response headers or /metrics payload",
			"minimum_supported_version", p.backendMinVersion,
		)
		return nil
	}
	if !semverAtLeastSemver(semver, p.backendMinVersion) {
		if p.backendAllowUnsupportedVersion {
			p.log.Warn(
				"unsupported backend version allowed by override",
				"backend.version.raw", raw,
				"backend.version.semver", semver,
				"backend.capability_profile", profile,
				"minimum_supported_version", p.backendMinVersion,
				"flag", "backend-allow-unsupported-version",
			)
			return nil
		}
		return fmt.Errorf(
			"detected backend version %s (%s) is below minimum supported %s; compatibility may be incomplete. Set -backend-allow-unsupported-version=true to bypass at your own risk",
			semver,
			raw,
			p.backendMinVersion,
		)
	}

	p.log.Info(
		"backend version compatibility check passed",
		"backend.version.raw", raw,
		"backend.version.semver", semver,
		"backend.capability_profile", profile,
		"minimum_supported_version", p.backendMinVersion,
	)
	return nil
}

// --- VL HTTP client ---

// vlGetInner executes a GET against VL without checking the circuit breaker.
// Callers must either hold a breaker.Allow() token or use DoWithGuard (which
// enforces the guard before fn is called).
func (p *Proxy) vlGetInner(ctx context.Context, path string, params url.Values) (*http.Response, error) {
	u := *p.backend
	u.Path = path
	u.RawQuery = params.Encode()

	p.log.Debug("VL request", "method", "GET", "url", u.String())
	req, err := http.NewRequestWithContext(ctx, "GET", u.String(), nil)
	if err != nil {
		return nil, err
	}
	p.forwardTenantHeaders(req)
	p.applyBackendHeaders(req)
	start := time.Now()
	resp, err := p.client.Do(req)
	duration := time.Since(start)
	serverPort, _ := strconv.Atoi(u.Port())
	if err != nil {
		mappedStatus := statusFromUpstreamErr(err)
		p.recordUpstreamObservation(ctx, "vl", http.MethodGet, path, u.Hostname(), serverPort, mappedStatus, duration, err)
		if shouldRecordBreakerFailure(err) {
			p.breaker.RecordFailure()
		}
		return nil, err
	}
	p.observeBackendVersionFromHeaders(resp.Header)
	if err := decodeCompressedHTTPResponse(resp); err != nil {
		_ = resp.Body.Close()
		p.recordUpstreamObservation(ctx, "vl", http.MethodGet, path, u.Hostname(), serverPort, http.StatusBadGateway, duration, err)
		return nil, fmt.Errorf("decode backend response: %w", err)
	}
	p.recordUpstreamObservation(ctx, "vl", http.MethodGet, path, u.Hostname(), serverPort, resp.StatusCode, duration, nil)
	// Any completed HTTP response proves backend reachability; keep breaker for transport failures only.
	p.breaker.RecordSuccess()
	return resp, nil
}

func (p *Proxy) vlGet(ctx context.Context, path string, params url.Values) (*http.Response, error) {
	if !p.breaker.Allow() {
		return nil, fmt.Errorf("circuit breaker open — backend unavailable")
	}
	return p.vlGetInner(ctx, path, params)
}

// vlPostInner executes a POST against VL without checking the circuit breaker.
// Callers must either hold a breaker.Allow() token or use DoWithGuard.
func (p *Proxy) vlPostInner(ctx context.Context, path string, params url.Values) (*http.Response, error) {
	u := *p.backend
	u.Path = path

	p.log.Debug("VL request", "method", "POST", "url", u.String(), "params", params.Encode())
	req, err := http.NewRequestWithContext(ctx, "POST", u.String(), strings.NewReader(params.Encode()))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	p.forwardTenantHeaders(req)
	p.applyBackendHeaders(req)
	start := time.Now()
	resp, err := p.client.Do(req)
	duration := time.Since(start)
	serverPort, _ := strconv.Atoi(u.Port())
	if err != nil {
		mappedStatus := statusFromUpstreamErr(err)
		p.recordUpstreamObservation(ctx, "vl", http.MethodPost, path, u.Hostname(), serverPort, mappedStatus, duration, err)
		if shouldRecordBreakerFailure(err) {
			p.breaker.RecordFailure()
		}
		return nil, err
	}
	p.observeBackendVersionFromHeaders(resp.Header)
	if err := decodeCompressedHTTPResponse(resp); err != nil {
		_ = resp.Body.Close()
		p.recordUpstreamObservation(ctx, "vl", http.MethodPost, path, u.Hostname(), serverPort, http.StatusBadGateway, duration, err)
		return nil, fmt.Errorf("decode backend response: %w", err)
	}
	p.recordUpstreamObservation(ctx, "vl", http.MethodPost, path, u.Hostname(), serverPort, resp.StatusCode, duration, nil)
	// Any completed HTTP response proves backend reachability; keep breaker for transport failures only.
	p.breaker.RecordSuccess()
	return resp, nil
}

func (p *Proxy) vlPost(ctx context.Context, path string, params url.Values) (*http.Response, error) {
	if !p.breaker.Allow() {
		return nil, fmt.Errorf("circuit breaker open — backend unavailable")
	}
	return p.vlPostInner(ctx, path, params)
}

// vlGetCoalesced wraps vlGet with request coalescing.
func (p *Proxy) vlGetCoalesced(ctx context.Context, key, path string, params url.Values) ([]byte, error) {
	status, body, err := p.vlGetCoalescedWithStatus(ctx, key, path, params)
	if err != nil {
		return nil, err
	}
	if status >= http.StatusBadRequest {
		msg := strings.TrimSpace(string(body))
		if msg == "" {
			msg = fmt.Sprintf("VL backend returned %d", status)
		}
		return nil, errors.New(msg)
	}
	return body, nil
}

// vlGetCoalescedWithStatus wraps vlGetInner with request coalescing and a CB guard.
// When the circuit breaker is open and a request for the same key is already
// in-flight, this call joins the in-flight rather than failing immediately.
func (p *Proxy) vlGetCoalescedWithStatus(ctx context.Context, key, path string, params url.Values) (int, []byte, error) {
	status, _, body, err := p.coalescer.DoWithGuard(key, p.breaker.Allow, func() (*http.Response, error) {
		return p.vlGetInner(ctx, path, params)
	})
	if errors.Is(err, mw.ErrGuardRejected) {
		return 0, nil, fmt.Errorf("circuit breaker open — backend unavailable")
	}
	if err != nil {
		return 0, nil, err
	}
	return status, body, nil
}

// vlPostCoalesced wraps vlPostInner with request coalescing and a CB guard.
func (p *Proxy) vlPostCoalesced(ctx context.Context, key, path string, params url.Values) (int, []byte, error) {
	status, _, body, err := p.coalescer.DoWithGuard(key, p.breaker.Allow, func() (*http.Response, error) {
		return p.vlPostInner(ctx, path, params)
	})
	if errors.Is(err, mw.ErrGuardRejected) {
		return 0, nil, fmt.Errorf("circuit breaker open — backend unavailable")
	}
	if err != nil {
		return 0, nil, err
	}
	return status, body, nil
}
