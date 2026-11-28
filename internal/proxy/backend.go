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

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/capability"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"

	mw "github.com/ReliablyObserve/Loki-VL-proxy/internal/middleware"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/querycorpus"
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
	if level > slog.LevelInfo && ctx.Value(quietUpstreamFailuresKey{}) != nil {
		level = slog.LevelDebug // the caller logs one line for the whole operation
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
	logAttrs = append(logAttrs, p.callerLogAttrs(ctx)...)
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
		errorType := "transport"
		if statusCode == 499 {
			errorType = "canceled"
		}
		logAttrs = append(logAttrs,
			"error.type", errorType,
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
//
// What the proxy knows about the VictoriaLogs release(s) behind it, and every gate derived from it, lives in the
// capability layer (internal/capability, p.caps); this file feeds it (response headers, the /metrics endpoint,
// feature probes) and reads gates out of it.

// backendKey names the backend a release was seen on (the URL host: never credentials).
func (p *Proxy) backendKey() string {
	if p.backend == nil {
		return ""
	}
	return p.backend.Host
}

// observeBackendVersionFromHeaders reads a VictoriaLogs release out of a response's headers. Only a value that names
// VictoriaLogs counts (capability.ParseBuild): a bare semver, or the version of vmauth, vlagent or VictoriaMetrics
// that answers in front of it, is not a VictoriaLogs release. A repeated value costs one string compare.
func (p *Proxy) observeBackendVersionFromHeaders(headers http.Header) {
	if headers == nil {
		return
	}
	for _, name := range [...]string{"Server", "X-App-Version", "X-Backend-Version", "X-Victoriametrics-Build"} {
		candidate := headers.Get(name)
		if candidate == "" {
			continue
		}
		if p.caps.ObserveRaw(p.backendKey(), candidate, capability.SourceHeader) {
			return
		}
	}
	// No VictoriaLogs release in the response: while none has been read (a failed or pending startup probe; a release
	// only declared with -backend-version is not read) retry the metrics probe in the background. An unknown release
	// behaves as the v1.49.0 floor.
	if !p.caps.Backend().Observed() {
		p.maybeReprobeBackendVersion(time.Now())
	}
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

// logsqlCapabilities returns the LogsQL capabilities of the observed backend. Before the first release is known it
// returns the floor baseline.
func (p *Proxy) logsqlCapabilities() logsql.Capabilities {
	return logsql.CapabilitiesOf(p.caps.Backend())
}

func (p *Proxy) supportsStreamMetadataEndpoints() bool {
	// Before the first release is known the stream endpoints are used (the gate holds its default, on), so a newer
	// deployment does not regress; the release, or an endpoint probe that finds them absent, turns it off.
	return p.caps.Backend().On(capability.StreamMetadataEndpoints)
}

func (p *Proxy) supportsDensePatternWindowing() bool {
	return p.caps.Backend().On(capability.DensePatternWindowing)
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

// supportsColumnIndexedFields returns true on VL v1.50+ where stream fields are
// stored in the column index. On these backends field_values is equivalent to
// stream_field_values but ~20x faster, so we prefer it for label value lookups.
func (p *Proxy) supportsColumnIndexedFields() bool {
	return p.caps.Backend().On(capability.ColumnFieldValues)
}

// backendVersionState is what the drilldown-limits answer reports of the backend: the raw text of the effective
// (lowest in the window) release, its canonical name and the capability profile.
func (p *Proxy) backendVersionState() (raw, semver, profile string) {
	set := p.caps.Backend()
	if r := set.Release(); r.Known {
		raw, semver = r.Raw, r.String()
	}
	return raw, semver, set.Profile()
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
		if p.backendVersionStrict {
			return fmt.Errorf("backend version check failed (strict mode): /health probe unreachable: %w", err)
		}
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
		if p.backendVersionStrict {
			return fmt.Errorf("backend version check failed (strict mode): /health returned non-success status %d", resp.StatusCode)
		}
		return nil
	}

	reason := ""
	if p.versionSource == nil && !p.caps.Backend().Observed() {
		reason = p.probeBackendVersionFromMetrics(checkCtx)
	}
	// Feature probes decide the gates that have one when no release is readable, and cross-check the release when it
	// is (-backend-feature-probes=false turns them off).
	p.probeBackendFeatures(checkCtx)
	if p.versionSource != nil {
		// Read whatever the response headers said (the lowest release read decides), after the probes and within its
		// own timeout: a slow version URL does not use up the time they need.
		reason = p.probeBackendVersionFromMetrics(ctx)
	}
	p.noteVersionReadReason(reason) // logged below; a later round logs it only when it changes
	rejected := p.warnNamedRelease()
	raw, semver, profile := p.backendVersionState()
	why := reason
	switch {
	case why != "":
	case len(rejected) > 0:
		why = "a feature probe contradicts the named release " + strings.Join(rejected, ", ")
	default:
		why = "backend did not expose version in response headers or /metrics payload"
	}
	if raw == "" {
		raw = "feature-probe"
	}
	if semver == "" && profile != "" {
		p.log.Warn(
			"backend version compatibility check partial",
			"reason", why,
			"note", "explicit backend semver unavailable; inferred capability profile from runtime endpoint probes. Gates that need the release stay off: see -backend-version-url and -backend-version",
			"backend.version.source", raw,
			"backend.capability_profile", profile,
			"minimum_supported_version", p.backendMinVersion,
		)
		if p.backendVersionStrict {
			return fmt.Errorf("backend version check failed (strict mode): explicit backend semver unavailable (inferred profile %q; %s); minimum required %s", profile, why, p.backendMinVersion)
		}
		return nil
	}
	if semver == "" {
		p.log.Warn(
			"backend version compatibility check unavailable",
			"reason", why,
			"minimum_supported_version", p.backendMinVersion,
		)
		if p.backendVersionStrict {
			return fmt.Errorf("backend version check failed (strict mode): %s; minimum required %s", why, p.backendMinVersion)
		}
		return nil
	}
	if reason != "" && !p.caps.Backend().Observed() {
		// Only -backend-version names a release: say that none was read, and why.
		p.log.Warn(
			"backend version compatibility check partial",
			"reason", reason,
			"note", "no release read from the backend; using -backend-version, which enables only the gates the feature probes confirm",
			"backend.version.semver", semver,
			"minimum_supported_version", p.backendMinVersion,
		)
	}
	if !semverAtLeastSemver(semver, p.backendMinVersion) {
		if p.backendAllowUnsupportedVersion && !p.backendVersionStrict {
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
		if p.backendVersionStrict {
			return fmt.Errorf(
				"backend version check failed (strict mode): detected backend version %s (%s) is below minimum supported %s",
				semver,
				raw,
				p.backendMinVersion,
			)
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
	if st := serviceNameRouteOf(ctx); st != nil {
		// A read of a log query the service_name stream filter narrows (service_name_route.go).
		return p.sendServiceNameRouted(ctx, st, path, params, func(c context.Context, ps url.Values) (*http.Response, error) {
			return p.vlGetInner(c, path, ps)
		})
	}
	params = guardOutboundQuery(p.withBackendTimeoutArg(ctx, path, withExclusionParams(ctx, path, p.scopedTenantParams(ctx, p.metadataSeriesParams(ctx, path, params)))))
	u := backendRequestURL(p.backend, path)
	u.RawQuery = params.Encode()
	querycorpus.RecordBackend(path, params.Get("query"))
	if resp, err, ok := p.predictVLRefusal(ctx, http.MethodGet, path, params); ok {
		return resp, err // VictoriaLogs refuses it on this release: not sent (vl_refusal.go)
	}

	p.log.Debug("VL request", "method", "GET", "url", u.Path, "params", redactQuery(params.Encode(), p.debugLogRawQueries))
	req, err := http.NewRequestWithContext(ctx, "GET", u.String(), nil)
	if err != nil {
		return nil, err
	}
	p.forwardTenantHeaders(req)
	p.applyBackendHeaders(req)
	serverPort, _ := strconv.Atoi(u.Port())
	admission, err := p.admitBackendRequest(ctx, path, params)
	if err != nil {
		// No request left the proxy: the admission outcome is recorded as an
		// internal operation, not as a VictoriaLogs response.
		return nil, err
	}
	req, release := p.sheddable(req, admission)
	release = p.trackSelectCall(path, release)
	start := time.Now()
	resp, err := p.doBackendRead(req, p.client)
	p.metadataScanLimiter.detachCancel(admission.scan)
	admission.observe(ctx, resp, err)
	resp = attachRelease(resp, release)
	duration := time.Since(start)
	if p.metadataScanLimiter.wasShed(admission.scan) {
		if resp != nil {
			// Stopped after VictoriaLogs answered: the body is unreadable.
			_ = resp.Body.Close()
		}
		// The limiter stopped the scan to keep VictoriaLogs' memory inside
		// the headroom: Loki's 429, not a backend failure.
		p.observeInternalOperation(ctx, "backend_metadata_scan_admission", "shed", time.Since(start))
		return nil, p.metadataScanLimiter.shedError()
	}
	if err != nil {
		err = p.sanitizeUpstreamError(err)
		mappedStatus := upstreamErrorStatus(ctx, err)
		p.recordUpstreamObservation(ctx, "vl", http.MethodGet, path, u.Hostname(), serverPort, mappedStatus, duration, err)
		p.observeUpstreamDone(ctx, path, params, mappedStatus, "error", 0, duration)
		if shouldRecordBreakerFailure(ctx, err) {
			p.breaker.RecordFailure()
		}
		return nil, err
	}
	p.observeBackendVersionFromHeaders(resp.Header)
	if err := decodeCompressedHTTPResponse(resp, p.decodedBodyLimit()); err != nil {
		_ = resp.Body.Close()
		p.recordUpstreamObservation(ctx, "vl", http.MethodGet, path, u.Hostname(), serverPort, http.StatusBadGateway, duration, err)
		p.observeUpstreamDone(ctx, path, params, http.StatusBadGateway, "error", 0, duration)
		return nil, fmt.Errorf("decode backend response: %w", err)
	}
	p.recordUpstreamObservation(ctx, "vl", http.MethodGet, path, u.Hostname(), serverPort, resp.StatusCode, duration, nil)
	if isVLRefusalStatus(resp.StatusCode) {
		recordVLRefusalCall(ctx, path, params.Get("query"))
	}
	// Any completed HTTP response proves backend reachability; keep breaker for transport failures only.
	p.breaker.RecordSuccess()
	resp.Body = p.guardBackendBody(ctx, resp.Body, path, params, start)
	resp.Body = p.observeBackendBody(ctx, resp.Body, path, params, resp.StatusCode, start)
	return resp, nil
}

// sanitizeUpstreamError redacts query parameters from a transport error before
// it is logged or returned to a handler. Go's http.Client surfaces dial / TLS /
// timeout failures as a *url.Error whose Error() embeds the full request URL —
// including the LogQL / LogsQL query in RawQuery — so without this the query
// content (which may carry sensitive log selectors) leaks into error logs and
// handler error responses, even though the debug request log already redacts it.
// The wrapped underlying error is preserved so statusFromUpstreamErr and breaker
// classification still see the real cause. No-op under -debug-log-raw-queries.
func (p *Proxy) sanitizeUpstreamError(err error) error {
	if err == nil || p.debugLogRawQueries {
		return err
	}
	var ue *url.Error
	if errors.As(err, &ue) {
		redactedURL := ue.URL
		if u, perr := url.Parse(ue.URL); perr == nil && u.RawQuery != "" {
			u.RawQuery = "redacted"
			redactedURL = u.String()
		}
		return &url.Error{Op: ue.Op, URL: redactedURL, Err: ue.Err}
	}
	return err
}

func (p *Proxy) vlGet(ctx context.Context, path string, params url.Values) (*http.Response, error) {
	if !p.breaker.Allow() {
		return nil, fmt.Errorf("circuit breaker open — backend unavailable")
	}
	return p.vlGetInner(ctx, path, params)
}

// vlPostHTTP executes the raw POST to VL: injects headers, runs the HTTP request, and
// decodes compression. It does NOT interact with the circuit breaker — callers are
// responsible for Allow() checks and RecordFailure/RecordSuccess calls.
func (p *Proxy) vlPostHTTP(ctx context.Context, path string, params url.Values) (*http.Response, error) {
	// A metric checked for unwrap conversion errors counts them in its own stats.
	// The rewrite goes ahead of the stats extent cache, which keys and stores the
	// counting query's buckets, the counted groups included; the counter is read
	// off the answer the cache assembles, stored buckets and fresh ones alike. A
	// read the cache sends itself carries the rewritten query already.
	sentParams := params
	var stripUnwrapCounter func([]byte) ([]byte, error)
	// A metric checked for JSON parser errors marks the lines VictoriaLogs
	// cannot vouch for in the same stats pass (json_parser_error_check.go).
	var stripJSONMarks func([]byte) ([]byte, []jsonErrorHit, error)
	if fetch, _ := ctx.Value(statsExtentFetchKey{}).(bool); !fetch {
		params, stripUnwrapCounter, _ = unwrapCheckFrom(ctx).countingStatsQuery(path, params)
		params, stripJSONMarks, _ = jsonErrorCheckFrom(ctx).jsonCountingStatsQuery(ctx, path, params)
		// Loki's structured-metadata series (metadata_series.go), ahead of the extent cache, which keys the rebuilt query.
		params = p.metadataSeriesParams(ctx, path, params)
	}
	if resp, ok, err := p.slicedSeriesMetadataRead(ctx, path, params); ok {
		// A rebuilt read over more rows than VictoriaLogs should group at once, read in slices (metadata_series.go).
		if err == nil && stripUnwrapCounter != nil {
			if err := p.unwrapStripResponse(resp, stripUnwrapCounter); err != nil {
				return nil, err
			}
		}
		if err == nil && stripJSONMarks != nil {
			resp, _, err = p.jsonStripResponse(ctx, resp, stripJSONMarks, path, sentParams)
		}
		return resp, err
	}
	if resp, ok, err := p.statsExtentPost(ctx, path, params); ok {
		if err == nil && stripUnwrapCounter != nil {
			if err := p.unwrapStripResponse(resp, stripUnwrapCounter); err != nil {
				return nil, err
			}
		}
		if err == nil && stripJSONMarks != nil {
			resp, _, err = p.jsonStripResponse(ctx, resp, stripJSONMarks, path, sentParams)
		}
		return resp, err
	}
	if seriesFoldFrom(ctx) != nil && path == statsQueryRangePath {
		// A folded read of a parser's series identity (series_fold_read.go): identical reads in flight share one call.
		resp, err := p.seriesFoldPost(ctx, path, params, sentParams)
		if err == nil && stripUnwrapCounter != nil {
			if err := p.unwrapStripResponse(resp, stripUnwrapCounter); err != nil {
				return nil, err
			}
		}
		if err == nil && stripJSONMarks != nil {
			resp, _, err = p.jsonStripResponse(ctx, resp, stripJSONMarks, path, sentParams)
		}
		return resp, err
	}
	return p.vlPostSend(ctx, path, params, sentParams, stripUnwrapCounter, stripJSONMarks, false)
}

// vlPostSend sends a POST of params (rewritten for the unwrap and JSON parser error checks already; sentParams as the
// caller sent them) to VictoriaLogs and reads its answer through the request's stripUnwrapCounter and stripJSONMarks,
// when it has them. A folded read (series_fold_read.go) is a stats_query_range read to every step before the send,
// and goes to /select/logsql/query with the bounds that endpoint would read (SeriesFoldQueryParams).
func (p *Proxy) vlPostSend(ctx context.Context, path string, params, sentParams url.Values, stripUnwrapCounter func([]byte) ([]byte, error),
	stripJSONMarks func([]byte) ([]byte, []jsonErrorHit, error), folded bool) (*http.Response, error) {
	if st := serviceNameRouteOf(ctx); st != nil {
		// A read of a log query the service_name stream filter narrows (service_name_route.go).
		return p.sendServiceNameRouted(ctx, st, path, params, func(c context.Context, ps url.Values) (*http.Response, error) {
			return p.vlPostSend(c, path, ps, sentParams, stripUnwrapCounter, stripJSONMarks, folded)
		})
	}
	params, witness := p.pruneLevelChain(ctx, path, params)
	params = guardOutboundQuery(p.withBackendTimeoutArg(ctx, path, withExclusionParams(ctx, path, p.scopedTenantParams(ctx, params))))
	sendPath, sendParams := path, params
	if folded {
		var err error
		if sendParams, err = SeriesFoldQueryParams(params); err != nil {
			return nil, err
		}
		sendPath = seriesFoldQueryPath
	}
	u := backendRequestURL(p.backend, sendPath)
	querycorpus.RecordBackend(sendPath, sendParams.Get("query"))
	if resp, err, ok := p.predictVLRefusal(ctx, http.MethodPost, sendPath, sendParams); ok {
		return resp, err // VictoriaLogs refuses it on this release: not sent (vl_refusal.go)
	}
	body := sendParams.Encode()
	if p.log.Enabled(ctx, slog.LevelDebug) {
		// Built only when written: the encoded form, its hash and the URL cost a request's worth of bytes.
		p.log.Debug("VL request", "method", "POST", "url", u.String(), "params", redactQuery(body, p.debugLogRawQueries))
	}
	req, err := http.NewRequestWithContext(ctx, "POST", u.String(), strings.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	p.forwardTenantHeaders(req)
	p.applyBackendHeaders(req)
	serverPort, _ := strconv.Atoi(u.Port())
	admission, err := p.admitBackendRequest(ctx, path, params)
	if err != nil {
		return nil, err
	}
	release := p.trackSelectCall(path, admission.Release)
	start := time.Now()
	resp, err := p.doBackendRead(req, p.client)
	admission.observe(ctx, resp, err)
	resp = attachRelease(resp, release)
	duration := time.Since(start)
	if err != nil {
		err = p.sanitizeUpstreamError(err)
		mappedStatus := upstreamErrorStatus(ctx, err)
		p.recordUpstreamObservation(ctx, "vl", http.MethodPost, path, u.Hostname(), serverPort, mappedStatus, duration, err)
		p.observeUpstreamDone(ctx, path, params, mappedStatus, "error", 0, duration)
		return nil, err
	}
	p.observeBackendVersionFromHeaders(resp.Header)
	if err := decodeCompressedHTTPResponse(resp, p.decodedBodyLimit()); err != nil {
		_ = resp.Body.Close()
		p.recordUpstreamObservation(ctx, "vl", http.MethodPost, path, u.Hostname(), serverPort, http.StatusBadGateway, duration, err)
		p.observeUpstreamDone(ctx, path, params, http.StatusBadGateway, "error", 0, duration)
		return nil, fmt.Errorf("decode backend response: %w", err)
	}
	p.recordUpstreamObservation(ctx, "vl", http.MethodPost, path, u.Hostname(), serverPort, resp.StatusCode, duration, nil)
	if isVLRefusalStatus(resp.StatusCode) {
		recordVLRefusalCall(ctx, path, params.Get("query"))
	}
	resp.Body = p.observeBackendBody(ctx, resp.Body, path, params, resp.StatusCode, start)
	if stripUnwrapCounter != nil {
		if err := p.unwrapStripResponse(resp, stripUnwrapCounter); err != nil {
			return nil, err
		}
	}
	if stripJSONMarks != nil {
		var refetched bool
		if resp, refetched, err = p.jsonStripResponse(ctx, resp, stripJSONMarks, path, sentParams); err != nil || refetched {
			return resp, err
		}
	}
	if witness != nil {
		flagged, err := p.levelWitnessResponse(resp)
		if err != nil {
			return nil, err
		}
		if flagged {
			// A row holds what the pruned chain does not read: the mask is stale.
			p.dropLevelPresence(ctx, witness.base)
			return p.vlPostHTTP(context.WithValue(ctx, levelFullChainKey{}, true), path, sentParams)
		}
	}
	return resp, nil
}

// vlPostInner executes a POST against VL without checking the circuit breaker.
// Callers must either hold a breaker.Allow() token or use DoWithGuard.
func (p *Proxy) vlPostInner(ctx context.Context, path string, params url.Values) (*http.Response, error) {
	resp, err := p.vlPostHTTP(ctx, path, params)
	if predictedVLAnswer(resp, err) {
		return resp, err // no call reached VictoriaLogs: the breaker learns nothing (vl_refusal.go)
	}
	if err != nil {
		if shouldRecordBreakerFailure(ctx, err) {
			p.breaker.RecordFailure()
		}
		return nil, err
	}
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
		return nil, p.redactedBackendStatusError("", status, body)
	}
	return body, nil
}

// vlGetCoalescedWithStatus wraps vlGetInner with request coalescing and a CB guard.
// When the circuit breaker is open and a request for the same key is already
// in-flight, this call joins the in-flight rather than failing immediately.
func (p *Proxy) vlGetCoalescedWithStatus(ctx context.Context, key, path string, params url.Values) (int, []byte, error) {
	key += ":scope:" + p.contextScopeFingerprint(ctx)
	status, _, body, err := p.coalescer.DoWithGuardCtx(ctx, key, p.breaker.Allow, func(callCtx context.Context) (*http.Response, error) {
		return p.vlGetInner(callCtx, path, params)
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
	key += ":scope:" + p.contextScopeFingerprint(ctx)
	if c := unwrapCheckFrom(ctx); c != nil && (len(c.probes) > 0 || c.json != nil) && !c.sharedNumericRawRead(path) {
		// The answer is read by this request's unwrap conversion check; another
		// request's call does not report its hits here. A check without a probe
		// (it only counts the nan values) records nothing: the rewrite and the
		// answer are the same for every request, which may share the call.
		key += fmt.Sprintf(":unwrap-check:%p", c)
	}
	status, _, body, err := p.coalescer.DoWithGuardCtx(ctx, key, p.breaker.Allow, func(callCtx context.Context) (*http.Response, error) {
		return p.vlPostInner(callCtx, path, params)
	})
	if errors.Is(err, mw.ErrGuardRejected) {
		return 0, nil, fmt.Errorf("circuit breaker open — backend unavailable")
	}
	if err != nil {
		return 0, nil, err
	}
	// The coalescer reads a shared body up to its own bound; a body above the
	// bytes the proxy buffers fails as every buffered read of that size does.
	if limit := int64(p.limits().BufferedBackendBodyBytes); limit > 0 && int64(len(body)) > limit {
		return 0, nil, newBackendBodyTooLarge(limit)
	}
	return status, body, nil
}
