package proxy

import (
	"context"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

type ctxKey int

const (
	orgIDKey ctxKey = iota
	origRequestKey
	requestTelemetryKey
	requestRouteMetaKey
	requestGrafanaClientKey
	authFingerprintKey // memoized forwardedAuthFingerprint result; set by injectAuthFingerprint
)

type requestTelemetry struct {
	mu sync.Mutex

	cacheResult            string
	upstreamCalls          int
	upstreamDuration       time.Duration
	upstreamLastCode       int
	upstreamErrorSeen      bool
	upstreamCallsByType    map[string]int
	upstreamDurationByType map[string]time.Duration
	internalOpsByType      map[string]int
	internalDurationByType map[string]time.Duration
}

type requestTelemetrySnapshot struct {
	cacheResult            string
	upstreamCalls          int
	upstreamDuration       time.Duration
	upstreamLastCode       int
	upstreamErrorSeen      bool
	upstreamCallsByType    map[string]int
	upstreamDurationByType map[string]time.Duration
	internalOpsByType      map[string]int
	internalDurationByType map[string]time.Duration
}

type requestRouteMeta struct {
	endpoint string
	route    string
}

type grafanaClientProfile struct {
	surface           string
	sourceTag         string
	version           string
	runtimeMajor      int
	runtimeFamily     string
	drilldownProfile  string
	datasourceProfile string
}

var upstreamRequestTypeByRoute = map[string]string{
	"/select/logsql/query":               "select_logsql_query",
	"/select/logsql/stats_query":         "select_logsql_stats_query",
	"/select/logsql/stats_query_range":   "select_logsql_stats_query_range",
	"/select/logsql/streams":             "select_logsql_streams",
	"/select/logsql/hits":                "select_logsql_hits",
	"/select/logsql/field_names":         "select_logsql_field_names",
	"/select/logsql/stream_field_names":  "select_logsql_stream_field_names",
	"/select/logsql/field_values":        "select_logsql_field_values",
	"/select/logsql/stream_field_values": "select_logsql_stream_field_values",
	"/select/logsql/delete":              "select_logsql_delete",
	"/select/logsql/tail":                "select_logsql_tail",
}

func newRequestTelemetry() *requestTelemetry {
	return &requestTelemetry{cacheResult: "bypass"}
}

func getRequestTelemetry(ctx context.Context) *requestTelemetry {
	rt, _ := ctx.Value(requestTelemetryKey).(*requestTelemetry)
	return rt
}

func setCacheResult(ctx context.Context, result string) {
	rt := getRequestTelemetry(ctx)
	if rt == nil {
		return
	}
	rt.mu.Lock()
	rt.cacheResult = result
	rt.mu.Unlock()
}

func splitHostPortValue(addr string) (string, int) {
	addr = strings.TrimSpace(addr)
	if addr == "" {
		return "", 0
	}
	host, portStr, err := net.SplitHostPort(addr)
	if err == nil {
		port, _ := strconv.Atoi(portStr)
		return strings.TrimSpace(host), port
	}
	return addr, 0
}

func forwardedClientAddress(r *http.Request, trustProxyHeaders bool) string {
	if trustProxyHeaders {
		if fwd := strings.TrimSpace(r.Header.Get("X-Forwarded-For")); fwd != "" {
			if idx := strings.IndexByte(fwd, ','); idx > 0 {
				return strings.TrimSpace(fwd[:idx])
			}
			return fwd
		}
	}
	host, _ := splitHostPortValue(r.RemoteAddr)
	return host
}

func upstreamBreakdownKey(system, requestType string) string {
	system = strings.TrimSpace(system)
	requestType = strings.TrimSpace(requestType)
	if requestType == "" {
		requestType = "unknown"
	}
	if system == "" {
		return requestType
	}
	return system + ":" + requestType
}

func internalOperationBreakdownKey(operation, outcome string) string {
	operation = strings.TrimSpace(operation)
	outcome = strings.TrimSpace(outcome)
	if operation == "" {
		operation = "unknown"
	}
	if outcome == "" {
		outcome = "unknown"
	}
	return operation + ":" + outcome
}

func recordUpstreamCall(ctx context.Context, system, requestType string, statusCode int, duration time.Duration, hadError bool) {
	rt := getRequestTelemetry(ctx)
	if rt == nil {
		return
	}
	rt.mu.Lock()
	rt.upstreamCalls++
	rt.upstreamDuration += duration
	rt.upstreamLastCode = statusCode
	rt.upstreamErrorSeen = rt.upstreamErrorSeen || hadError
	if requestType != "" {
		key := upstreamBreakdownKey(system, requestType)
		if rt.upstreamCallsByType == nil {
			rt.upstreamCallsByType = make(map[string]int)
		}
		if rt.upstreamDurationByType == nil {
			rt.upstreamDurationByType = make(map[string]time.Duration)
		}
		rt.upstreamCallsByType[key]++
		rt.upstreamDurationByType[key] += duration
	}
	rt.mu.Unlock()
}

func recordInternalOperation(ctx context.Context, operation, outcome string, duration time.Duration) {
	rt := getRequestTelemetry(ctx)
	if rt == nil {
		return
	}
	if duration < 0 {
		duration = 0
	}
	rt.mu.Lock()
	key := internalOperationBreakdownKey(operation, outcome)
	if rt.internalOpsByType == nil {
		rt.internalOpsByType = make(map[string]int)
	}
	if rt.internalDurationByType == nil {
		rt.internalDurationByType = make(map[string]time.Duration)
	}
	rt.internalOpsByType[key]++
	rt.internalDurationByType[key] += duration
	rt.mu.Unlock()
}

func snapshotTelemetry(ctx context.Context) requestTelemetrySnapshot {
	rt := getRequestTelemetry(ctx)
	if rt == nil {
		return requestTelemetrySnapshot{cacheResult: "bypass"}
	}
	rt.mu.Lock()
	defer rt.mu.Unlock()
	upstreamCallsByType := make(map[string]int, len(rt.upstreamCallsByType))
	for key, value := range rt.upstreamCallsByType {
		upstreamCallsByType[key] = value
	}
	upstreamDurationByType := make(map[string]time.Duration, len(rt.upstreamDurationByType))
	for key, value := range rt.upstreamDurationByType {
		upstreamDurationByType[key] = value
	}
	internalOpsByType := make(map[string]int, len(rt.internalOpsByType))
	for key, value := range rt.internalOpsByType {
		internalOpsByType[key] = value
	}
	internalDurationByType := make(map[string]time.Duration, len(rt.internalDurationByType))
	for key, value := range rt.internalDurationByType {
		internalDurationByType[key] = value
	}
	return requestTelemetrySnapshot{
		cacheResult:            rt.cacheResult,
		upstreamCalls:          rt.upstreamCalls,
		upstreamDuration:       rt.upstreamDuration,
		upstreamLastCode:       rt.upstreamLastCode,
		upstreamErrorSeen:      rt.upstreamErrorSeen,
		upstreamCallsByType:    upstreamCallsByType,
		upstreamDurationByType: upstreamDurationByType,
		internalOpsByType:      internalOpsByType,
		internalDurationByType: internalDurationByType,
	}
}

func requestRouteMetaFromContext(ctx context.Context) requestRouteMeta {
	meta, _ := ctx.Value(requestRouteMetaKey).(requestRouteMeta)
	return meta
}

func grafanaClientProfileFromContext(ctx context.Context) grafanaClientProfile {
	profile, _ := ctx.Value(requestGrafanaClientKey).(grafanaClientProfile)
	return profile
}

// withOrgID stores the X-Scope-OrgID and original request in the request context (request-scoped, no shared state).
func withOrgID(r *http.Request) *http.Request {
	ctx := r.Context()
	// Store original request for header forwarding in vlGet/vlPost
	ctx = context.WithValue(ctx, origRequestKey, r)
	orgID := r.Header.Get("X-Scope-OrgID")
	if orgID != "" {
		ctx = context.WithValue(ctx, orgIDKey, orgID)
	}
	return r.WithContext(ctx)
}

// getOrgID retrieves the org ID from context.
func getOrgID(ctx context.Context) string {
	if v, ok := ctx.Value(orgIDKey).(string); ok {
		return v
	}
	return ""
}
