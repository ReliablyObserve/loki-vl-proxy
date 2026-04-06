package proxy

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net"
	"net/http"
	"net/http/httptest"
	_ "net/http/pprof"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/szibis/Loki-VL-proxy/internal/cache"
	"github.com/szibis/Loki-VL-proxy/internal/metrics"
	mw "github.com/szibis/Loki-VL-proxy/internal/middleware"
	"github.com/szibis/Loki-VL-proxy/internal/translator"
	"gopkg.in/yaml.v3"
)

// jsonBufPool pools bytes.Buffer for JSON encoding to reduce allocations.
// Capped at 64KB before returning to prevent pool bloat from large responses.
const maxPooledBufSize = 64 * 1024

var jsonBufPool = sync.Pool{
	New: func() interface{} {
		return bytes.NewBuffer(make([]byte, 0, 4096))
	},
}

// marshalJSON encodes v to JSON using a pooled buffer, then writes to w.
// Reduces allocations vs json.NewEncoder(w).Encode() by reusing buffers.
func marshalJSON(w http.ResponseWriter, v interface{}) {
	buf := jsonBufPool.Get().(*bytes.Buffer)
	buf.Reset()

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(buf).Encode(v); err != nil {
		// Fallback: encode directly (shouldn't happen for our types)
		json.NewEncoder(w).Encode(v)
	} else {
		w.Write(buf.Bytes())
	}

	if buf.Cap() <= maxPooledBufSize {
		jsonBufPool.Put(buf)
	}
	// Oversized buffers are left for GC (prevents pool bloat)
}

type ctxKey int

const (
	orgIDKey ctxKey = iota
	origRequestKey
)

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

// TenantMapping maps a string org ID to VL's numeric AccountID and ProjectID.
type TenantMapping struct {
	AccountID string `json:"account_id" yaml:"account_id"`
	ProjectID string `json:"project_id" yaml:"project_id"`
}

type TailMode string

const (
	TailModeAuto      TailMode = "auto"
	TailModeNative    TailMode = "native"
	TailModeSynthetic TailMode = "synthetic"
)

type Config struct {
	BackendURL        string
	RulerBackendURL   string
	AlertsBackendURL  string
	Cache             *cache.Cache
	LogLevel          string
	MaxConcurrent     int                      // max concurrent backend queries (0=unlimited)
	RatePerSecond     float64                  // per-client rate limit (0=unlimited)
	RateBurst         int                      // per-client burst size
	CBFailThreshold   int                      // circuit breaker failure threshold
	CBOpenDuration    time.Duration            // circuit breaker open duration
	TenantMap         map[string]TenantMapping // string org ID → VL account/project
	AuthEnabled       bool
	AllowGlobalTenant bool

	// Grafana datasource compatibility
	MaxLines         int               // default max lines per query (0=1000)
	ForwardHeaders   []string          // HTTP headers to forward from client to VL backend
	ForwardCookies   []string          // Cookie names to forward from client to VL backend
	BackendHeaders   map[string]string // static headers to add to all VL requests
	BackendBasicAuth string            // "user:password" for VL backend basic auth
	BackendTimeout   time.Duration     // bounded timeout for non-streaming backend requests
	BackendTLSSkip   bool              // skip TLS verification for VL backend
	DerivedFields    []DerivedField    // derived fields for trace/link extraction
	StreamResponse   bool              // stream responses via chunked transfer (default: false)

	// Label translation
	LabelStyle        LabelStyle        // how to translate VL field names to Loki labels
	MetadataFieldMode MetadataFieldMode // how to expose non-label VL fields through field-oriented APIs
	FieldMappings     []FieldMapping    // custom VL↔Loki field name mappings

	// Stream optimization
	StreamFields []string // VL _stream_fields labels — use native stream selectors for these (faster)

	// Peer cache (fleet distribution)
	PeerCache     *cache.PeerCache // optional peer cache for distributed fleet
	PeerAuthToken string

	// Admin/debug endpoints
	RegisterInstrumentation *bool
	EnablePprof             bool
	EnableQueryAnalytics    bool
	AdminAuthToken          string

	// Tail/WebSocket hardening
	TailAllowedOrigins []string
	TailMode           TailMode

	// Metrics/export hardening
	MetricsMaxTenants        int
	MetricsMaxClients        int
	MetricsTrustProxyHeaders bool
}

// DerivedField extracts a value from log lines and creates a link (e.g., to a trace backend).
// Matches Grafana Loki datasource "Derived fields" config.
type DerivedField struct {
	Name            string `json:"name" yaml:"name"`                       // field name (e.g., "traceID")
	MatcherRegex    string `json:"matcherRegex" yaml:"matcherRegex"`       // regex to extract value from log line
	URL             string `json:"url" yaml:"url"`                         // link template (e.g., "http://tempo:3200/trace/${__value.raw}")
	URLDisplayLabel string `json:"urlDisplayLabel" yaml:"urlDisplayLabel"` // display text for the link
	DatasourceUID   string `json:"datasourceUid" yaml:"datasourceUid"`     // Grafana datasource UID for internal link
}

const (
	// maxQueryLength limits the LogQL query string length to prevent abuse.
	maxQueryLength = 65536 // 64KB
	// maxLimitValue caps the number of results per query.
	maxLimitValue    = 10000
	tailWriteTimeout = 2 * time.Second
)

// CacheTTLs defines per-endpoint cache TTLs.
var CacheTTLs = map[string]time.Duration{
	"labels":          60 * time.Second,
	"label_values":    60 * time.Second,
	"series":          30 * time.Second,
	"detected_fields": 30 * time.Second,
	"query_range":     10 * time.Second,
	"query":           10 * time.Second,
}

type Proxy struct {
	backend                  *url.URL
	rulerBackend             *url.URL
	alertsBackend            *url.URL
	client                   *http.Client
	tailClient               *http.Client
	cache                    *cache.Cache
	log                      *slog.Logger
	metrics                  *metrics.Metrics
	queryTracker             *metrics.QueryTracker
	coalescer                *mw.Coalescer
	limiter                  *mw.RateLimiter
	breaker                  *mw.CircuitBreaker
	configMu                 sync.RWMutex // protects tenantMap and labelTranslator
	tenantMap                map[string]TenantMapping
	authEnabled              bool
	allowGlobalTenant        bool
	maxLines                 int
	forwardHeaders           []string          // headers to copy from client request to VL
	forwardCookies           map[string]bool   // cookie names to copy from client request to VL
	backendHeaders           map[string]string // static headers on all VL requests
	derivedFields            []DerivedField
	streamResponse           bool
	labelTranslator          *LabelTranslator
	metadataFieldMode        MetadataFieldMode
	streamFieldsMap          map[string]bool  // known _stream_fields for VL stream selector optimization
	peerCache                *cache.PeerCache // L3 fleet peer cache
	peerAuthToken            string
	registerInstrumentation  bool
	enablePprof              bool
	enableQueryAnalytics     bool
	adminAuthToken           string
	tailAllowedOrigins       map[string]struct{}
	tailMode                 TailMode
	metricsTrustProxyHeaders bool
}

type tailConn interface {
	SetWriteDeadline(time.Time) error
	WriteMessage(int, []byte) error
	WriteControl(int, []byte, time.Time) error
}

func New(cfg Config) (*Proxy, error) {
	u, err := url.Parse(cfg.BackendURL)
	if err != nil {
		return nil, fmt.Errorf("invalid backend URL: %w", err)
	}
	var rulerURL *url.URL
	if strings.TrimSpace(cfg.RulerBackendURL) != "" {
		rulerURL, err = url.Parse(cfg.RulerBackendURL)
		if err != nil {
			return nil, fmt.Errorf("invalid ruler backend URL: %w", err)
		}
	}
	var alertsURL *url.URL
	if strings.TrimSpace(cfg.AlertsBackendURL) != "" {
		alertsURL, err = url.Parse(cfg.AlertsBackendURL)
		if err != nil {
			return nil, fmt.Errorf("invalid alerts backend URL: %w", err)
		}
	}

	level := slog.LevelInfo
	switch strings.ToLower(cfg.LogLevel) {
	case "debug":
		level = slog.LevelDebug
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	}

	baseLogger := slog.Default()
	if baseLogger == nil {
		baseLogger = slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level}))
	}
	logger := slog.New(NewRedactingHandler(baseLogger.Handler())).With("component", "proxy")

	maxConcurrent := cfg.MaxConcurrent
	if maxConcurrent == 0 {
		maxConcurrent = 100 // sensible default
	}
	ratePerSec := cfg.RatePerSecond
	if ratePerSec == 0 {
		ratePerSec = 50 // 50 req/s per client default
	}
	rateBurst := cfg.RateBurst
	if rateBurst == 0 {
		rateBurst = 100
	}
	cbFail := cfg.CBFailThreshold
	if cbFail == 0 {
		cbFail = 5
	}
	cbOpen := cfg.CBOpenDuration
	if cbOpen == 0 {
		cbOpen = 10 * time.Second
	}

	// Build HTTP client optimized for high-concurrency single-backend proxying.
	// Go defaults (MaxIdleConnsPerHost=2) cause ephemeral port exhaustion under load.
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxIdleConns = 256                 // total idle connections across all hosts
	transport.MaxIdleConnsPerHost = 256          // VL is the only backend — all slots for it
	transport.MaxConnsPerHost = 0                // unlimited concurrent connections to VL
	transport.IdleConnTimeout = 90 * time.Second // reuse connections for 90s
	backendTimeout := cfg.BackendTimeout
	if backendTimeout <= 0 {
		backendTimeout = 120 * time.Second
	}
	transport.ResponseHeaderTimeout = backendTimeout
	transport.DisableCompression = false // accept gzip from VL if available
	if cfg.BackendTLSSkip {
		transport.TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
	}
	tailTransport := transport.Clone()
	tailTransport.ResponseHeaderTimeout = 30 * time.Second

	maxLines := cfg.MaxLines
	if maxLines <= 0 {
		maxLines = 1000
	}

	backendHeaders := cfg.BackendHeaders
	if backendHeaders == nil {
		backendHeaders = make(map[string]string)
	}
	if cfg.BackendBasicAuth != "" {
		encoded := base64Encode(cfg.BackendBasicAuth)
		backendHeaders["Authorization"] = "Basic " + encoded
	}
	registerInstrumentation := true
	if cfg.RegisterInstrumentation != nil {
		registerInstrumentation = *cfg.RegisterInstrumentation
	}
	forwardCookies := make(map[string]bool, len(cfg.ForwardCookies))
	for _, cookieName := range cfg.ForwardCookies {
		cookieName = strings.TrimSpace(cookieName)
		if cookieName == "" {
			continue
		}
		forwardCookies[cookieName] = true
	}
	tailAllowedOrigins := make(map[string]struct{}, len(cfg.TailAllowedOrigins))
	for _, origin := range cfg.TailAllowedOrigins {
		origin = strings.TrimSpace(origin)
		if origin == "" {
			continue
		}
		tailAllowedOrigins[origin] = struct{}{}
	}
	metadataFieldMode := normalizeMetadataFieldMode(cfg.MetadataFieldMode)
	tailMode := cfg.TailMode
	if tailMode == "" {
		tailMode = TailModeAuto
	}
	switch tailMode {
	case TailModeAuto, TailModeNative, TailModeSynthetic:
	default:
		return nil, fmt.Errorf("invalid tail mode %q", tailMode)
	}

	return &Proxy{
		backend:       u,
		rulerBackend:  rulerURL,
		alertsBackend: alertsURL,
		client: &http.Client{
			Timeout:   backendTimeout,
			Transport: transport,
		},
		tailClient: &http.Client{
			Transport: tailTransport,
		},
		cache:                    cfg.Cache,
		log:                      logger,
		metrics:                  metrics.NewMetricsWithLimits(cfg.MetricsMaxTenants, cfg.MetricsMaxClients),
		queryTracker:             metrics.NewQueryTracker(10000),
		coalescer:                mw.NewCoalescer(),
		limiter:                  mw.NewRateLimiter(maxConcurrent, ratePerSec, rateBurst),
		breaker:                  mw.NewCircuitBreaker(cbFail, 3, cbOpen),
		tenantMap:                cfg.TenantMap,
		authEnabled:              cfg.AuthEnabled,
		allowGlobalTenant:        cfg.AllowGlobalTenant,
		maxLines:                 maxLines,
		forwardHeaders:           cfg.ForwardHeaders,
		forwardCookies:           forwardCookies,
		backendHeaders:           backendHeaders,
		derivedFields:            cfg.DerivedFields,
		streamResponse:           cfg.StreamResponse,
		labelTranslator:          NewLabelTranslator(cfg.LabelStyle, cfg.FieldMappings),
		metadataFieldMode:        metadataFieldMode,
		streamFieldsMap:          buildStreamFieldsMap(cfg.StreamFields),
		peerCache:                cfg.PeerCache,
		peerAuthToken:            cfg.PeerAuthToken,
		registerInstrumentation:  registerInstrumentation,
		enablePprof:              cfg.EnablePprof,
		enableQueryAnalytics:     cfg.EnableQueryAnalytics,
		adminAuthToken:           cfg.AdminAuthToken,
		tailAllowedOrigins:       tailAllowedOrigins,
		tailMode:                 tailMode,
		metricsTrustProxyHeaders: cfg.MetricsTrustProxyHeaders,
	}, nil
}

func buildStreamFieldsMap(fields []string) map[string]bool {
	if len(fields) == 0 {
		return nil
	}
	m := make(map[string]bool, len(fields))
	for _, f := range fields {
		m[f] = true
	}
	return m
}

// Init wires cross-component dependencies after construction.
func (p *Proxy) Init() {
	p.metrics.SetCircuitBreakerFunc(p.breaker.State)
}

// ReloadTenantMap hot-reloads tenant mappings (called on SIGHUP).
func (p *Proxy) ReloadTenantMap(m map[string]TenantMapping) {
	p.configMu.Lock()
	p.tenantMap = m
	p.configMu.Unlock()
}

// ReloadFieldMappings hot-reloads field mappings and rebuilds the label translator.
func (p *Proxy) ReloadFieldMappings(mappings []FieldMapping) {
	p.configMu.Lock()
	p.labelTranslator = NewLabelTranslator(p.labelTranslator.style, mappings)
	p.configMu.Unlock()
}

// GetMetrics returns the proxy's metrics instance for external telemetry exporters.
func (p *Proxy) GetMetrics() *metrics.Metrics { return p.metrics }

// GetQueryTracker returns the query analytics tracker.
func (p *Proxy) GetQueryTracker() *metrics.QueryTracker { return p.queryTracker }

// securityHeaders wraps a handler with security response headers.
func securityHeaders(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("X-Frame-Options", "DENY")
		w.Header().Set("Cache-Control", "no-store")
		h.ServeHTTP(w, r)
	})
}

type requestPolicyError struct {
	status int
	msg    string
}

func (e *requestPolicyError) Error() string { return e.msg }

// validateTenantHeader applies Loki-style tenant presence checks and proxy-specific
// mapping validation before any backend call is made.
func (p *Proxy) validateTenantHeader(r *http.Request) error {
	orgID := strings.TrimSpace(r.Header.Get("X-Scope-OrgID"))
	if orgID == "" {
		if p.authEnabled {
			return &requestPolicyError{
				status: http.StatusUnauthorized,
				msg:    "missing X-Scope-OrgID header",
			}
		}
		return nil
	}
	if strings.Contains(orgID, "|") {
		if !isMultiTenantQueryPath(r.URL.Path) {
			return &requestPolicyError{
				status: http.StatusBadRequest,
				msg:    "multi-tenant X-Scope-OrgID values are only supported on query endpoints",
			}
		}
		tenantIDs := splitMultiTenantOrgIDs(orgID)
		if len(tenantIDs) < 2 {
			return &requestPolicyError{
				status: http.StatusBadRequest,
				msg:    "multi-tenant X-Scope-OrgID must include at least two tenant IDs",
			}
		}
		for _, tenantID := range tenantIDs {
			if tenantID == "*" {
				return &requestPolicyError{
					status: http.StatusBadRequest,
					msg:    `wildcard "*" is not supported inside multi-tenant X-Scope-OrgID values`,
				}
			}
			if err := p.validateSingleTenantOrgID(tenantID); err != nil {
				return err
			}
		}
		return nil
	}
	return p.validateSingleTenantOrgID(orgID)
}

func (p *Proxy) validateSingleTenantOrgID(orgID string) error {
	p.configMu.RLock()
	_, ok := p.tenantMap[orgID]
	p.configMu.RUnlock()
	if ok {
		return nil
	}

	if isDefaultTenantAlias(orgID) {
		return nil
	}
	if orgID == "*" {
		if p.globalTenantAllowed() {
			return nil
		}
		return &requestPolicyError{
			status: http.StatusForbidden,
			msg:    `global tenant bypass ("*") is disabled`,
		}
	}
	if _, err := strconv.Atoi(orgID); err == nil {
		return nil
	}

	return &requestPolicyError{
		status: http.StatusForbidden,
		msg:    fmt.Sprintf("unknown tenant %q", orgID),
	}
}

func splitMultiTenantOrgIDs(orgID string) []string {
	parts := strings.Split(orgID, "|")
	out := make([]string, 0, len(parts))
	seen := make(map[string]struct{}, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		if _, ok := seen[part]; ok {
			continue
		}
		seen[part] = struct{}{}
		out = append(out, part)
	}
	return out
}

func hasMultiTenantOrgID(orgID string) bool {
	return strings.IndexByte(orgID, '|') >= 0
}

func isMultiTenantQueryPath(path string) bool {
	switch {
	case path == "/loki/api/v1/query":
		return true
	case path == "/loki/api/v1/query_range":
		return true
	case path == "/loki/api/v1/series":
		return true
	case path == "/loki/api/v1/labels":
		return true
	case strings.HasPrefix(path, "/loki/api/v1/label/"):
		return true
	case path == "/loki/api/v1/index/stats":
		return true
	case path == "/loki/api/v1/index/volume":
		return true
	case path == "/loki/api/v1/index/volume_range":
		return true
	case path == "/loki/api/v1/detected_fields":
		return true
	case strings.HasPrefix(path, "/loki/api/v1/detected_field/"):
		return true
	case path == "/loki/api/v1/detected_labels":
		return true
	case path == "/loki/api/v1/patterns":
		return true
	default:
		return false
	}
}

func isDefaultTenantAlias(orgID string) bool {
	switch orgID {
	case "0", "fake", "default":
		return true
	default:
		return false
	}
}

func (p *Proxy) globalTenantAllowed() bool {
	if p.allowGlobalTenant {
		return true
	}
	p.configMu.RLock()
	defer p.configMu.RUnlock()
	return len(p.tenantMap) == 0
}

func (p *Proxy) tenantMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := p.validateTenantHeader(r); err != nil {
			if rpe, ok := err.(*requestPolicyError); ok {
				p.writeError(w, rpe.status, rpe.msg)
				return
			}
			p.writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (p *Proxy) adminMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if p.adminAuthToken != "" {
			got := strings.TrimSpace(r.Header.Get("X-Admin-Token"))
			if got == "" {
				auth := strings.TrimSpace(r.Header.Get("Authorization"))
				if strings.HasPrefix(auth, "Bearer ") {
					got = strings.TrimSpace(strings.TrimPrefix(auth, "Bearer "))
				}
			}
			if got != p.adminAuthToken {
				p.writeError(w, http.StatusUnauthorized, "admin authentication required")
				return
			}
		}
		next.ServeHTTP(w, r)
	})
}

func hostOnly(addr string) string {
	if host, _, err := net.SplitHostPort(addr); err == nil {
		return host
	}
	return addr
}

func (p *Proxy) isKnownPeerHost(host string) bool {
	if p.peerCache == nil {
		return false
	}
	for _, peer := range p.peerCache.Peers() {
		if hostOnly(peer) == host {
			return true
		}
	}
	return false
}

func (p *Proxy) peerCacheMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if p.peerAuthToken != "" {
			if r.Header.Get("X-Peer-Token") != p.peerAuthToken {
				p.writeError(w, http.StatusUnauthorized, "peer authentication required")
				return
			}
			next.ServeHTTP(w, r)
			return
		}

		if !p.isKnownPeerHost(hostOnly(r.RemoteAddr)) {
			p.writeError(w, http.StatusForbidden, "peer cache endpoint is restricted to configured peers")
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (p *Proxy) isAllowedTailOrigin(origin string) bool {
	origin = strings.TrimSpace(origin)
	if origin == "" {
		return true
	}
	if _, ok := p.tailAllowedOrigins["*"]; ok {
		return true
	}
	_, ok := p.tailAllowedOrigins[origin]
	return ok
}

func (p *Proxy) tailUpgrader() websocket.Upgrader {
	return websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool {
			return p.isAllowedTailOrigin(r.Header.Get("Origin"))
		},
	}
}

func (p *Proxy) RegisterRoutes(mux *http.ServeMux) {
	// Rate-limited endpoints with security headers + request logging
	rl := func(endpoint string, h http.HandlerFunc) http.Handler {
		return securityHeaders(p.tenantMiddleware(p.limiter.Middleware(p.requestLogger(endpoint, h))))
	}
	rlNoTenant := func(endpoint string, h http.HandlerFunc) http.Handler {
		return securityHeaders(p.limiter.Middleware(p.requestLogger(endpoint, h)))
	}

	// Loki API endpoints — data queries are rate-limited
	mux.Handle("/loki/api/v1/query_range", rl("query_range", p.handleQueryRange))
	mux.Handle("/loki/api/v1/query", rl("query", p.handleQuery))
	mux.Handle("/loki/api/v1/series", rl("series", p.handleSeries))

	// Metadata endpoints — rate-limited but cached
	mux.Handle("/loki/api/v1/labels", rl("labels", p.handleLabels))
	mux.Handle("/loki/api/v1/label/", rl("label_values", p.handleLabelValues))
	mux.Handle("/loki/api/v1/detected_fields", rl("detected_fields", p.handleDetectedFields))
	mux.Handle("/loki/api/v1/detected_field/", rl("detected_field_values", p.handleDetectedFieldValues))

	// Lighter endpoints — still rate-limited
	mux.Handle("/loki/api/v1/index/stats", rl("index_stats", p.handleIndexStats))
	mux.Handle("/loki/api/v1/index/volume", rl("volume", p.handleVolume))
	mux.Handle("/loki/api/v1/index/volume_range", rl("volume_range", p.handleVolumeRange))
	mux.Handle("/loki/api/v1/patterns", rl("patterns", p.handlePatterns))
	mux.Handle("/loki/api/v1/tail", rl("tail", p.handleTail))

	// Read-only API additions
	mux.Handle("/loki/api/v1/format_query", rl("format_query", p.handleFormatQuery))
	mux.Handle("/loki/api/v1/detected_labels", rl("detected_labels", p.handleDetectedLabels))
	mux.Handle("/loki/api/v1/drilldown-limits", rlNoTenant("drilldown_limits", p.handleDrilldownLimits))

	// Write endpoints — blocked (this is a read-only proxy)
	mux.HandleFunc("/loki/api/v1/push", p.handleWriteBlocked)

	// Delete endpoint — exception to read-only with strict safeguards
	mux.Handle("/loki/api/v1/delete", rl("delete", p.handleDelete))

	// Alerting / ruler read endpoints
	alertRead := func(endpoint string, h http.HandlerFunc) http.Handler {
		return securityHeaders(p.tenantMiddleware(p.requestLogger(endpoint, h)))
	}
	mux.Handle("/loki/api/v1/rules", alertRead("rules", p.handleRules))
	mux.Handle("/loki/api/v1/rules/", alertRead("rules_nested", p.handleRules))
	mux.Handle("/api/prom/rules", alertRead("rules_prom", p.handleRules))
	mux.Handle("/api/prom/rules/", alertRead("rules_prom_nested", p.handleRules))
	mux.Handle("/prometheus/api/v1/rules", alertRead("rules_prometheus", p.handleRules))
	mux.Handle("/loki/api/v1/alerts", alertRead("alerts", p.handleAlerts))
	mux.Handle("/api/prom/alerts", alertRead("alerts_prom", p.handleAlerts))
	mux.Handle("/prometheus/api/v1/alerts", alertRead("alerts_prometheus", p.handleAlerts))
	mux.HandleFunc("/config", p.handleConfigStub)

	// Health / readiness — NOT rate-limited
	mux.HandleFunc("/ready", p.handleReady)
	mux.HandleFunc("/loki/api/v1/status/buildinfo", p.handleBuildInfo)

	if p.registerInstrumentation {
		// Prometheus metrics endpoint — NOT rate-limited
		mux.HandleFunc("/metrics", p.handleMetrics)
		if p.enablePprof {
			mux.Handle("/debug/pprof/cmdline", p.adminMiddleware(http.NotFoundHandler()))
			mux.Handle("/debug/pprof/", p.adminMiddleware(http.DefaultServeMux))
		}
	}

	if p.enableQueryAnalytics {
		mux.Handle("/debug/queries", securityHeaders(p.adminMiddleware(http.HandlerFunc(p.queryTracker.Handler))))
	}

	// Peer cache endpoint — internal, for sharded fleet cache
	if p.peerCache != nil {
		mux.Handle("/_cache/get", securityHeaders(p.peerCacheMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			p.peerCache.ServeHTTP(w, r, p.cache)
		}))))
	}
}

func (p *Proxy) handleMetrics(w http.ResponseWriter, r *http.Request) {
	rec := httptest.NewRecorder()
	p.metrics.Handler(rec, r)

	for k, vals := range rec.Header() {
		for _, v := range vals {
			w.Header().Add(k, v)
		}
	}
	if p.peerCache != nil {
		_, _ = rec.Body.WriteString(p.peerCacheMetrics())
	}

	w.WriteHeader(rec.Code)
	_, _ = w.Write(rec.Body.Bytes())
}

func (p *Proxy) peerCacheMetrics() string {
	stats := p.peerCache.Stats()
	remotePeers, _ := stats["peers"].(int)
	hits, _ := stats["peer_hits"].(int64)
	misses, _ := stats["peer_misses"].(int64)
	errors, _ := stats["peer_errors"].(int64)
	clusterMembers := len(p.peerCache.Peers())

	return fmt.Sprintf(
		"# HELP loki_vl_proxy_peer_cache_peers Number of remote peers currently in the fleet cache ring.\n"+
			"# TYPE loki_vl_proxy_peer_cache_peers gauge\n"+
			"loki_vl_proxy_peer_cache_peers %d\n"+
			"# HELP loki_vl_proxy_peer_cache_cluster_members Number of total cache ring members including this proxy instance.\n"+
			"# TYPE loki_vl_proxy_peer_cache_cluster_members gauge\n"+
			"loki_vl_proxy_peer_cache_cluster_members %d\n"+
			"# HELP loki_vl_proxy_peer_cache_hits_total Successful peer-cache fetches.\n"+
			"# TYPE loki_vl_proxy_peer_cache_hits_total counter\n"+
			"loki_vl_proxy_peer_cache_hits_total %d\n"+
			"# HELP loki_vl_proxy_peer_cache_misses_total Peer-cache lookups that missed on the owner.\n"+
			"# TYPE loki_vl_proxy_peer_cache_misses_total counter\n"+
			"loki_vl_proxy_peer_cache_misses_total %d\n"+
			"# HELP loki_vl_proxy_peer_cache_errors_total Peer-cache fetch errors.\n"+
			"# TYPE loki_vl_proxy_peer_cache_errors_total counter\n"+
			"loki_vl_proxy_peer_cache_errors_total %d\n",
		remotePeers, clusterMembers, hits, misses, errors,
	)
}

// handleQueryRange translates Loki range queries.
// Loki: GET /loki/api/v1/query_range?query={...}&start=...&end=...&limit=...&step=...
// VL stats: POST /select/logsql/stats_query_range with query, start, end, step
// VL logs:  POST /select/logsql/query with query, start, end, limit
func (p *Proxy) handleQueryRange(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	logqlQuery := r.FormValue("query")
	if _, ok := p.validateQuery(w, logqlQuery, "query_range"); !ok {
		return
	}
	if p.handleMultiTenantFanout(w, r, "query_range", p.handleQueryRange) {
		return
	}
	cacheKey := ""
	cacheable := !p.streamResponse
	if cacheable {
		cacheKey = p.queryRangeCacheKey(r, logqlQuery)
		if cached, ok := p.cache.Get(cacheKey); ok {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write(cached)
			elapsed := time.Since(start)
			p.metrics.RecordRequest("query_range", http.StatusOK, elapsed)
			p.metrics.RecordCacheHit()
			p.queryTracker.Record("query_range", logqlQuery, elapsed, false)
			return
		}
		p.metrics.RecordCacheMiss()
	}
	p.log.Debug("query_range request", "logql", logqlQuery)

	logqlQuery = p.preferWorkingParser(r.Context(), logqlQuery, r.FormValue("start"), r.FormValue("end"))

	logsqlQuery, err := p.translateQuery(logqlQuery)
	if err != nil {
		p.writeError(w, http.StatusBadRequest, err.Error())
		p.metrics.RecordRequest("query_range", http.StatusBadRequest, time.Since(start))
		return
	}
	// Extract without() labels for post-processing
	logsqlQuery, withoutLabels := translator.ParseWithoutMarker(logsqlQuery)
	p.log.Debug("translated query", "logsql", logsqlQuery, "without", withoutLabels)

	r = withOrgID(r)

	var (
		sc       = &statusCapture{ResponseWriter: w, code: 200}
		capture  *bufferedResponseWriter
		cacheOut []byte
	)
	if len(withoutLabels) > 0 || cacheable {
		capture = &bufferedResponseWriter{header: make(http.Header)}
		sc = &statusCapture{ResponseWriter: capture, code: 200}
	}

	// Check for subquery expression (e.g., max_over_time(rate(...)[1h:5m]))
	if outerFunc, innerQL, rng, step, ok := translator.ParseSubqueryExpr(logsqlQuery); ok {
		p.proxySubqueryRange(sc, r, outerFunc, innerQL, rng, step)
	} else if op, left, right, vm, ok := translator.ParseBinaryMetricExprFull(logsqlQuery); ok {
		// Binary metric expression (e.g., sum(rate(...)) / sum(rate(...)))
		p.proxyBinaryMetricQueryRangeVM(sc, r, op, left, right, vm)
	} else if isStatsQuery(logsqlQuery) {
		p.proxyStatsQueryRange(sc, r, logsqlQuery)
	} else {
		p.proxyLogQuery(sc, r, logsqlQuery)
	}

	if capture != nil {
		cacheOut = capture.body
		if len(withoutLabels) > 0 {
			cacheOut = applyWithoutGrouping(cacheOut, withoutLabels)
		}
		copyHeaders(w.Header(), capture.Header())
		if w.Header().Get("Content-Type") == "" {
			w.Header().Set("Content-Type", "application/json")
		}
		if sc.code != http.StatusOK {
			w.WriteHeader(sc.code)
		}
		_, _ = w.Write(cacheOut)
		if cacheable && sc.code == http.StatusOK {
			p.cache.SetWithTTL(cacheKey, cacheOut, CacheTTLs["query_range"])
		}
	}

	elapsed := time.Since(start)
	p.metrics.RecordRequest("query_range", sc.code, elapsed)
	p.queryTracker.Record("query_range", logqlQuery, elapsed, sc.code >= 400)
}

func (p *Proxy) queryRangeCacheKey(r *http.Request, logqlQuery string) string {
	rawQuery := r.URL.RawQuery
	if rawQuery == "" {
		var b strings.Builder
		b.Grow(len(logqlQuery) + 64)
		b.WriteString("query=")
		b.WriteString(url.QueryEscape(logqlQuery))
		for _, key := range []string{"start", "end", "step", "limit", "direction"} {
			if value := r.FormValue(key); value != "" {
				b.WriteByte('&')
				b.WriteString(key)
				b.WriteByte('=')
				b.WriteString(url.QueryEscape(value))
			}
		}
		rawQuery = b.String()
	}
	return "query_range:" + r.Header.Get("X-Scope-OrgID") + ":" + rawQuery
}

// handleQuery translates Loki instant queries.
func (p *Proxy) handleQuery(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	logqlQuery := r.FormValue("query")
	if _, ok := p.validateQuery(w, logqlQuery, "query"); !ok {
		return
	}
	p.log.Debug("query request", "logql", logqlQuery)

	if body, ok := evaluateConstantInstantVectorQuery(logqlQuery, r.FormValue("time")); ok {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(body)
		elapsed := time.Since(start)
		p.metrics.RecordRequest("query", http.StatusOK, elapsed)
		p.queryTracker.Record("query", logqlQuery, elapsed, false)
		return
	}
	if p.handleMultiTenantFanout(w, r, "query", p.handleQuery) {
		return
	}

	logqlQuery = p.preferWorkingParser(r.Context(), logqlQuery, r.FormValue("start"), r.FormValue("end"))

	logsqlQuery, err := p.translateQuery(logqlQuery)
	if err != nil {
		p.writeError(w, http.StatusBadRequest, err.Error())
		p.metrics.RecordRequest("query", http.StatusBadRequest, time.Since(start))
		return
	}

	// Extract without() labels for post-processing
	logsqlQuery, withoutLabels := translator.ParseWithoutMarker(logsqlQuery)

	r = withOrgID(r)

	// Wrap writer to capture actual status code for metrics
	sc := &statusCapture{ResponseWriter: w, code: 200}

	var bw *bufferedResponseWriter
	if len(withoutLabels) > 0 {
		bw = &bufferedResponseWriter{header: w.Header()}
		sc = &statusCapture{ResponseWriter: bw, code: 200}
	}

	if outerFunc, innerQL, rng, step, ok := translator.ParseSubqueryExpr(logsqlQuery); ok {
		p.proxySubquery(sc, r, outerFunc, innerQL, rng, step)
	} else if op, left, right, vm, ok := translator.ParseBinaryMetricExprFull(logsqlQuery); ok {
		p.proxyBinaryMetricQueryVM(sc, r, op, left, right, vm)
	} else if isStatsQuery(logsqlQuery) {
		p.proxyStatsQuery(sc, r, logsqlQuery)
	} else {
		p.proxyLogQuery(sc, r, logsqlQuery)
	}

	if bw != nil && len(withoutLabels) > 0 {
		result := applyWithoutGrouping(bw.body, withoutLabels)
		w.Header().Set("Content-Type", "application/json")
		w.Write(result)
	}

	elapsed := time.Since(start)
	p.metrics.RecordRequest("query", sc.code, elapsed)
	p.queryTracker.Record("query", logqlQuery, elapsed, sc.code >= 400)
}

var (
	vectorLiteralRE = regexp.MustCompile(`^\s*vector\(\s*([^)]+?)\s*\)\s*$`)
	vectorBinaryRE  = regexp.MustCompile(`^\s*vector\(\s*([^)]+?)\s*\)\s*([+\-*/])\s*vector\(\s*([^)]+?)\s*\)\s*$`)
)

func evaluateConstantInstantVectorQuery(expr, timeParam string) ([]byte, bool) {
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return nil, false
	}

	if matches := vectorLiteralRE.FindStringSubmatch(expr); len(matches) == 2 {
		value, err := strconv.ParseFloat(strings.TrimSpace(matches[1]), 64)
		if err != nil {
			return nil, false
		}
		return buildConstantVectorResponse(timeParam, value), true
	}

	if matches := vectorBinaryRE.FindStringSubmatch(expr); len(matches) == 4 {
		left, err := strconv.ParseFloat(strings.TrimSpace(matches[1]), 64)
		if err != nil {
			return nil, false
		}
		right, err := strconv.ParseFloat(strings.TrimSpace(matches[3]), 64)
		if err != nil {
			return nil, false
		}
		value, ok := applyConstantBinaryOp(left, right, matches[2])
		if !ok {
			return nil, false
		}
		return buildConstantVectorResponse(timeParam, value), true
	}

	return nil, false
}

func buildConstantVectorResponse(timeParam string, value float64) []byte {
	ts := parseInstantVectorTime(timeParam)
	result, _ := json.Marshal(map[string]interface{}{
		"status": "success",
		"data": map[string]interface{}{
			"resultType": "vector",
			"result": []map[string]interface{}{
				{
					"metric": map[string]string{},
					"value":  []interface{}{ts, strconv.FormatFloat(value, 'f', -1, 64)},
				},
			},
			"stats": map[string]interface{}{},
		},
	})
	return result
}

func parseInstantVectorTime(timeParam string) int64 {
	if timeParam == "" {
		return time.Now().UnixNano()
	}
	if t, err := time.Parse(time.RFC3339Nano, timeParam); err == nil {
		return t.UnixNano()
	}
	if i, err := strconv.ParseInt(timeParam, 10, 64); err == nil {
		if i < 1_000_000_000_000 {
			return i * int64(time.Second)
		}
		return i
	}
	if f, err := strconv.ParseFloat(timeParam, 64); err == nil {
		if f < 1_000_000_000_000 {
			return int64(f * float64(time.Second))
		}
		return int64(f)
	}
	return time.Now().UnixNano()
}

func applyConstantBinaryOp(left, right float64, op string) (float64, bool) {
	switch op {
	case "+":
		return left + right, true
	case "-":
		return left - right, true
	case "*":
		return left * right, true
	case "/":
		if right == 0 {
			return 0, false
		}
		return left / right, true
	default:
		return 0, false
	}
}

// handleLabels returns label names.
// Loki: GET /loki/api/v1/labels?start=...&end=...
// VL:   GET /select/logsql/field_names?query=*&start=...&end=...
func (p *Proxy) handleLabels(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	if p.handleMultiTenantFanout(w, r, "labels", p.handleLabels) {
		return
	}
	orgID := r.Header.Get("X-Scope-OrgID")
	cacheKey := "labels:" + orgID + ":" + r.URL.RawQuery

	if cached, ok := p.cache.Get(cacheKey); ok {
		p.log.Debug("labels cache hit")
		w.Header().Set("Content-Type", "application/json")
		w.Write(cached)
		p.metrics.RecordRequest("labels", http.StatusOK, time.Since(start))
		p.metrics.RecordCacheHit()
		return
	}
	p.metrics.RecordCacheMiss()
	r = withOrgID(r)

	params := url.Values{}
	// Forward the query param if provided (Loki uses it to scope label suggestions)
	if q := r.FormValue("query"); q != "" {
		translated, terr := p.translateQuery(defaultQuery(q))
		if terr == nil {
			params.Set("query", translated)
		} else {
			params.Set("query", "*")
		}
	} else {
		params.Set("query", "*")
	}
	if s := r.FormValue("start"); s != "" {
		params.Set("start", s)
	}
	if e := r.FormValue("end"); e != "" {
		params.Set("end", e)
	}

	body, err := p.vlGetCoalesced(r.Context(), "labels:"+params.Encode(), "/select/logsql/field_names", params)
	if err != nil {
		p.writeError(w, http.StatusBadGateway, err.Error())
		p.metrics.RecordRequest("labels", http.StatusBadGateway, time.Since(start))
		return
	}

	// VL returns: {"values": [{"value": "name", "hits": N}, ...]}
	// Loki expects: {"status": "success", "data": ["name1", "name2", ...]}
	var vlResp struct {
		Values []struct {
			Value string `json:"value"`
			Hits  int64  `json:"hits"`
		} `json:"values"`
	}
	if err := json.Unmarshal(body, &vlResp); err != nil {
		p.writeError(w, http.StatusInternalServerError, "failed to parse VL response: "+err.Error())
		p.metrics.RecordRequest("labels", http.StatusInternalServerError, time.Since(start))
		return
	}

	labels := make([]string, 0, len(vlResp.Values))
	for _, v := range vlResp.Values {
		// Filter out VL internal fields — Loki doesn't expose these
		if isVLInternalField(v.Value) {
			continue
		}
		labels = append(labels, v.Value)
	}

	// Apply label name translation (e.g., dots → underscores)
	labels = p.labelTranslator.TranslateLabelsList(labels)
	labels = appendSyntheticLabels(labels)

	result := lokiLabelsResponse(labels)
	p.cache.SetWithTTL(cacheKey, result, CacheTTLs["labels"])
	w.Header().Set("Content-Type", "application/json")
	w.Write(result)
	p.metrics.RecordRequest("labels", http.StatusOK, time.Since(start))
}

// handleLabelValues returns values for a specific label.
// Loki: GET /loki/api/v1/label/{name}/values?start=...&end=...
// VL:   GET /select/logsql/field_values?query=*&field={name}&start=...&end=...
func (p *Proxy) handleLabelValues(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	// Extract label name from URL: /loki/api/v1/label/{name}/values
	path := r.URL.Path
	parts := strings.Split(path, "/")
	if len(parts) < 7 || parts[6] != "values" {
		p.writeError(w, http.StatusBadRequest, "invalid label values URL")
		return
	}
	labelName := parts[5]
	if strings.Contains(r.Header.Get("X-Scope-OrgID"), "|") && labelName == "__tenant_id__" {
		values := splitMultiTenantOrgIDs(r.Header.Get("X-Scope-OrgID"))
		result := lokiLabelsResponse(values)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(result)
		p.metrics.RecordRequest("label_values", http.StatusOK, time.Since(start))
		return
	}
	if p.handleMultiTenantFanout(w, r, "label_values", p.handleLabelValues) {
		return
	}
	orgID := r.Header.Get("X-Scope-OrgID")
	cacheKey := "label_values:" + orgID + ":" + labelName + ":" + r.URL.RawQuery
	if labelName == "service_name" {
		values, err := p.serviceNameValues(r.Context(), r.FormValue("query"), r.FormValue("start"), r.FormValue("end"))
		if err != nil {
			p.writeError(w, http.StatusBadGateway, err.Error())
			p.metrics.RecordRequest("label_values", http.StatusBadGateway, time.Since(start))
			return
		}
		result := lokiLabelsResponse(values)
		p.cache.SetWithTTL(cacheKey, result, CacheTTLs["label_values"])
		w.Header().Set("Content-Type", "application/json")
		w.Write(result)
		p.metrics.RecordRequest("label_values", http.StatusOK, time.Since(start))
		return
	}
	// Translate Loki label name to VL field name for the backend query
	vlFieldName := p.labelTranslator.ToVL(labelName)

	if cached, ok := p.cache.Get(cacheKey); ok {
		p.log.Debug("label values cache hit", "label", labelName)
		w.Header().Set("Content-Type", "application/json")
		w.Write(cached)
		p.metrics.RecordRequest("label_values", http.StatusOK, time.Since(start))
		p.metrics.RecordCacheHit()
		return
	}
	p.metrics.RecordCacheMiss()

	params := url.Values{}
	if q := r.FormValue("query"); q != "" {
		translated, terr := p.translateQuery(defaultQuery(q))
		if terr == nil {
			params.Set("query", translated)
		} else {
			params.Set("query", "*")
		}
	} else {
		params.Set("query", "*")
	}
	params.Set("field", vlFieldName)
	if s := r.FormValue("start"); s != "" {
		params.Set("start", s)
	}
	if e := r.FormValue("end"); e != "" {
		params.Set("end", e)
	}
	if l := r.FormValue("limit"); l != "" {
		params.Set("limit", l)
	}

	resp, err := p.vlGet(r.Context(), "/select/logsql/field_values", params)
	if err != nil {
		p.writeError(w, http.StatusBadGateway, err.Error())
		p.metrics.RecordRequest("label_values", http.StatusBadGateway, time.Since(start))
		return
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
		p.writeError(w, http.StatusInternalServerError, "failed to parse VL response")
		return
	}

	values := make([]string, 0, len(vlResp.Values))
	for _, v := range vlResp.Values {
		values = append(values, v.Value)
	}

	result := lokiLabelsResponse(values)
	p.cache.SetWithTTL(cacheKey, result, CacheTTLs["label_values"])
	w.Header().Set("Content-Type", "application/json")
	w.Write(result)
	p.metrics.RecordRequest("label_values", http.StatusOK, time.Since(start))
}

// handleSeries returns stream/series metadata.
// Loki: GET /loki/api/v1/series?match[]={...}&start=...&end=...
// VL:   GET /select/logsql/streams?query={...}&start=...&end=...
func (p *Proxy) handleSeries(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	if p.handleMultiTenantFanout(w, r, "series", p.handleSeries) {
		return
	}
	r = withOrgID(r)
	matchQueries := r.Form["match[]"]
	query := "*"
	if len(matchQueries) > 0 {
		translated, err := p.translateQuery(matchQueries[0])
		if err == nil {
			query = translated
		}
	}

	params := url.Values{}
	params.Set("query", query)
	if s := r.FormValue("start"); s != "" {
		params.Set("start", s)
	}
	if e := r.FormValue("end"); e != "" {
		params.Set("end", e)
	}

	resp, err := p.vlGet(r.Context(), "/select/logsql/streams", params)
	if err != nil {
		p.writeError(w, http.StatusBadGateway, err.Error())
		p.metrics.RecordRequest("series", http.StatusBadGateway, time.Since(start))
		return
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)

	// Propagate VL error status
	if resp.StatusCode >= 400 {
		p.writeError(w, resp.StatusCode, string(body))
		p.metrics.RecordRequest("series", resp.StatusCode, time.Since(start))
		return
	}

	// VL returns: {"values": [{"value": "{stream}", "hits": N}, ...]}
	// Loki expects: {"status": "success", "data": [{"label": "value", ...}, ...]}
	var vlResp struct {
		Values []struct {
			Value string `json:"value"`
			Hits  int64  `json:"hits"`
		} `json:"values"`
	}
	json.Unmarshal(body, &vlResp)

	series := make([]map[string]string, 0, len(vlResp.Values))
	for _, v := range vlResp.Values {
		labels := parseStreamLabels(v.Value)
		if len(labels) > 0 {
			labels = p.labelTranslator.TranslateLabelsMap(labels)
			ensureSyntheticServiceName(labels)
			series = append(series, labels)
		}
	}

	result, _ := json.Marshal(map[string]interface{}{
		"status": "success",
		"data":   series,
	})
	w.Header().Set("Content-Type", "application/json")
	w.Write(result)
	p.metrics.RecordRequest("series", http.StatusOK, time.Since(start))
}

// handleIndexStats returns index statistics via VL /select/logsql/hits.
// Loki: GET /loki/api/v1/index/stats?query={...}&start=...&end=...
// Response: {"streams":N, "chunks":N, "entries":N, "bytes":N}
func (p *Proxy) handleIndexStats(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	if p.handleMultiTenantFanout(w, r, "index_stats", p.handleIndexStats) {
		return
	}
	r = withOrgID(r)
	query := r.FormValue("query")
	if query == "" {
		query = "*"
	}
	logsqlQuery, _ := p.translateQuery(query)

	params := url.Values{}
	params.Set("query", logsqlQuery+" | sort by (_time desc)")
	if s := r.FormValue("start"); s != "" {
		params.Set("start", formatVLTimestamp(s))
	}
	if e := r.FormValue("end"); e != "" {
		params.Set("end", formatVLTimestamp(e))
	}
	// VL v1.49+ requires step for hits — use large step to get one bucket (total count)
	if params.Get("step") == "" {
		params.Set("step", "1h")
	}

	resp, err := p.vlGet(r.Context(), "/select/logsql/hits", params)
	if err != nil {
		// Fallback to zeros on error
		p.writeJSON(w, map[string]interface{}{"streams": 0, "chunks": 0, "bytes": 0, "entries": 0})
		p.metrics.RecordRequest("index_stats", http.StatusOK, time.Since(start))
		return
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)

	entries := sumHitsValues(body)
	hits := parseHits(body)
	streams := len(hits.Hits)
	if streams == 0 && entries > 0 {
		streams = 1
	}
	result, _ := json.Marshal(map[string]interface{}{
		"streams": streams,
		"chunks":  streams,
		"bytes":   entries * 100, // approximate — VL doesn't expose bytes
		"entries": entries,
	})
	w.Header().Set("Content-Type", "application/json")
	w.Write(result)
	p.metrics.RecordRequest("index_stats", http.StatusOK, time.Since(start))
}

// handleVolume returns volume data via VL /select/logsql/hits with field grouping.
// Loki: GET /loki/api/v1/index/volume?query={...}&start=...&end=...
// Response: {"status":"success","data":{"resultType":"vector","result":[{"metric":{...},"value":[ts,"count"]}]}}
func (p *Proxy) handleVolume(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	if p.handleMultiTenantFanout(w, r, "volume", p.handleVolume) {
		return
	}
	r = withOrgID(r)
	query := r.FormValue("query")
	if query == "" {
		query = "*"
	}
	targetLabels := r.FormValue("targetLabels")
	if targetLabels == "" {
		targetLabels = inferPrimaryTargetLabel(query)
	}
	if usesDerivedVolumeLabels(targetLabels) {
		result, err := p.volumeByDerivedLabels(r.Context(), query, r.FormValue("start"), r.FormValue("end"), targetLabels, "")
		if err == nil {
			p.writeJSON(w, result)
			p.metrics.RecordRequest("volume", http.StatusOK, time.Since(start))
			return
		}
	}
	logsqlQuery, _ := p.translateQuery(query)

	params := url.Values{}
	params.Set("query", logsqlQuery+" | sort by (_time desc)")
	if s := r.FormValue("start"); s != "" {
		params.Set("start", formatVLTimestamp(s))
	}
	if e := r.FormValue("end"); e != "" {
		params.Set("end", formatVLTimestamp(e))
	}
	// VL v1.49+ requires step for hits
	if params.Get("step") == "" {
		params.Set("step", "1h")
	}
	// Request field-level grouping
	if targetLabels != "" {
		mappedFields := make([]string, 0, len(splitTargetLabels(targetLabels)))
		for _, field := range splitTargetLabels(targetLabels) {
			mappedFields = append(mappedFields, p.labelTranslator.ToVL(field))
		}
		params.Set("field", strings.Join(mappedFields, ","))
	}

	resp, err := p.vlGet(r.Context(), "/select/logsql/hits", params)
	if err != nil {
		p.writeJSON(w, map[string]interface{}{
			"status": "success",
			"data":   map[string]interface{}{"resultType": "vector", "result": []interface{}{}},
		})
		p.metrics.RecordRequest("volume", http.StatusOK, time.Since(start))
		return
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)

	result := p.hitsToVolumeVector(body)
	p.writeJSON(w, result)
	p.metrics.RecordRequest("volume", http.StatusOK, time.Since(start))
}

// handleVolumeRange returns volume range data via VL /select/logsql/hits with step.
// Loki: GET /loki/api/v1/index/volume_range?query={...}&start=...&end=...&step=60
// Response: {"status":"success","data":{"resultType":"matrix","result":[{"metric":{...},"values":[[ts,"count"],...]}]}}
func (p *Proxy) handleVolumeRange(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	if p.handleMultiTenantFanout(w, r, "volume_range", p.handleVolumeRange) {
		return
	}
	r = withOrgID(r)
	query := r.FormValue("query")
	if query == "" {
		query = "*"
	}
	targetLabels := r.FormValue("targetLabels")
	if targetLabels == "" {
		targetLabels = inferPrimaryTargetLabel(query)
	}
	if usesDerivedVolumeLabels(targetLabels) {
		result, err := p.volumeByDerivedLabels(r.Context(), query, r.FormValue("start"), r.FormValue("end"), targetLabels, r.FormValue("step"))
		if err == nil {
			p.writeJSON(w, result)
			p.metrics.RecordRequest("volume_range", http.StatusOK, time.Since(start))
			return
		}
	}
	logsqlQuery, _ := p.translateQuery(query)

	params := url.Values{}
	params.Set("query", logsqlQuery+" | sort by (_time desc)")
	if s := r.FormValue("start"); s != "" {
		params.Set("start", formatVLTimestamp(s))
	}
	if e := r.FormValue("end"); e != "" {
		params.Set("end", formatVLTimestamp(e))
	}
	if step := r.FormValue("step"); step != "" {
		params.Set("step", formatVLStep(step))
	}
	// Forward targetLabels for field-level grouping (same as /volume)
	if targetLabels != "" {
		mappedFields := make([]string, 0, len(splitTargetLabels(targetLabels)))
		for _, field := range splitTargetLabels(targetLabels) {
			mappedFields = append(mappedFields, p.labelTranslator.ToVL(field))
		}
		params.Set("field", strings.Join(mappedFields, ","))
	}

	resp, err := p.vlGet(r.Context(), "/select/logsql/hits", params)
	if err != nil {
		p.writeJSON(w, map[string]interface{}{
			"status": "success",
			"data":   map[string]interface{}{"resultType": "matrix", "result": []interface{}{}},
		})
		p.metrics.RecordRequest("volume_range", http.StatusOK, time.Since(start))
		return
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)

	result := p.hitsToVolumeMatrix(body)
	p.writeJSON(w, result)
	p.metrics.RecordRequest("volume_range", http.StatusOK, time.Since(start))
}

// handleDetectedFields returns detected field names.
func (p *Proxy) handleDetectedFields(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	if p.handleMultiTenantFanout(w, r, "detected_fields", p.handleDetectedFields) {
		return
	}
	r = withOrgID(r)
	lineLimit := parseDetectedLineLimit(r)
	fields, _, err := p.detectFields(r.Context(), r.FormValue("query"), r.FormValue("start"), r.FormValue("end"), lineLimit)
	if err != nil {
		p.writeJSON(w, map[string]interface{}{
			"status": "success",
			"data":   []interface{}{},
			"fields": []interface{}{},
			"limit":  lineLimit,
		})
		p.metrics.RecordRequest("detected_fields", http.StatusOK, time.Since(start))
		return
	}
	p.writeJSON(w, map[string]interface{}{
		"status": "success",
		"data":   fields,
		"fields": fields,
		"limit":  lineLimit,
	})
	p.metrics.RecordRequest("detected_fields", http.StatusOK, time.Since(start))
}

// handleDetectedFieldValues returns values for a detected field.
// Loki: GET /loki/api/v1/detected_field/{name}/values?query=...
// Response: {"values":["debug","info","warn","error"],"limit":1000}
func (p *Proxy) handleDetectedFieldValues(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	if p.handleMultiTenantFanout(w, r, "detected_field_values", p.handleDetectedFieldValues) {
		return
	}
	r = withOrgID(r)
	// Extract field name from URL: /loki/api/v1/detected_field/{name}/values
	path := r.URL.Path
	parts := strings.Split(path, "/")
	fieldName := ""
	for i, part := range parts {
		if part == "detected_field" && i+1 < len(parts) {
			fieldName = parts[i+1]
			break
		}
	}
	if fieldName == "" {
		p.writeError(w, http.StatusBadRequest, "missing field name in URL")
		return
	}

	lineLimit := parseDetectedLineLimit(r)

	_, fieldValues, err := p.detectFields(r.Context(), r.FormValue("query"), r.FormValue("start"), r.FormValue("end"), lineLimit)
	if err != nil {
		p.writeJSON(w, map[string]interface{}{
			"status": "success",
			"data":   []string{},
			"values": []string{},
			"limit":  lineLimit,
		})
		p.metrics.RecordRequest("detected_field_values", http.StatusOK, time.Since(start))
		return
	}
	values := fieldValues[fieldName]
	if values == nil && fieldName == "level" {
		values = fieldValues["detected_level"]
	}

	p.writeJSON(w, map[string]interface{}{
		"status": "success",
		"data":   values,
		"values": values,
		"limit":  lineLimit,
	})
	p.metrics.RecordRequest("detected_field_values", http.StatusOK, time.Since(start))
}

// handlePatterns returns log patterns for Grafana Logs Drilldown.
func (p *Proxy) handlePatterns(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	if p.handleMultiTenantFanout(w, r, "patterns", p.handlePatterns) {
		return
	}
	r = withOrgID(r)
	query := r.FormValue("query")
	if strings.TrimSpace(query) == "" {
		query = "*"
	}

	logsqlQuery, err := p.translateQuery(query)
	if err != nil {
		p.writeJSON(w, map[string]interface{}{
			"status": "success",
			"data":   []interface{}{},
		})
		p.metrics.RecordRequest("patterns", http.StatusOK, time.Since(start))
		return
	}

	params := url.Values{}
	params.Set("query", logsqlQuery+" | sort by (_time desc)")
	if s := r.FormValue("start"); s != "" {
		params.Set("start", formatVLTimestamp(s))
	}
	if e := r.FormValue("end"); e != "" {
		params.Set("end", formatVLTimestamp(e))
	}
	params.Set("limit", "1000")

	patternLimit := 50
	if raw := strings.TrimSpace(r.FormValue("limit")); raw != "" {
		if n, err := strconv.Atoi(raw); err == nil && n > 0 {
			patternLimit = n
		}
	}

	resp, err := p.vlPost(r.Context(), "/select/logsql/query", params)
	if err != nil {
		p.writeJSON(w, map[string]interface{}{
			"status": "success",
			"data":   []interface{}{},
		})
		p.metrics.RecordRequest("patterns", http.StatusOK, time.Since(start))
		return
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		p.writeJSON(w, map[string]interface{}{
			"status": "success",
			"data":   []interface{}{},
		})
		p.metrics.RecordRequest("patterns", http.StatusOK, time.Since(start))
		return
	}

	p.writeJSON(w, map[string]interface{}{
		"status": "success",
		"data":   extractLogPatterns(body, r.FormValue("step"), patternLimit),
	})
	p.metrics.RecordRequest("patterns", http.StatusOK, time.Since(start))
}

// handleFormatQuery returns the query as-is (pretty-printing is client-side for LogQL).
func (p *Proxy) handleFormatQuery(w http.ResponseWriter, r *http.Request) {
	query := r.FormValue("query")
	p.writeJSON(w, map[string]interface{}{
		"status": "success",
		"data":   query,
	})
}

// handleDrilldownLimits returns Grafana Logs Drilldown limits metadata.
// Grafana uses this as a lightweight capability/bootstrap probe.
func (p *Proxy) handleDrilldownLimits(w http.ResponseWriter, r *http.Request) {
	p.writeJSON(w, map[string]interface{}{
		"limits": map[string]interface{}{
			"discover_log_levels":         true,
			"discover_service_name":       []string{"service", "app", "application", "app_name", "name", "app_kubernetes_io_name", "container", "container_name", "k8s_container_name", "component", "workload", "job", "k8s_job_name"},
			"log_level_fields":            []string{"level", "LEVEL", "Level", "log.level", "severity", "SEVERITY", "Severity", "SeverityText", "lvl", "LVL", "Lvl", "severity_text", "Severity_Text", "SEVERITY_TEXT"},
			"max_entries_limit_per_query": maxLimitValue,
			"max_line_size_truncate":      false,
			"max_query_bytes_read":        "0B",
			"max_query_length":            "30d1h",
			"max_query_lookback":          "0s",
			"max_query_range":             "0s",
			"max_query_series":            500,
			"metric_aggregation_enabled":  false,
			"otlp_config": map[string]interface{}{
				"resource_attributes": map[string]interface{}{
					"attributes_config": []map[string]interface{}{
						{
							"action":     "index_label",
							"attributes": []string{"service.name", "service.namespace", "service.instance.id", "deployment.environment", "deployment.environment.name", "cloud.region", "cloud.availability_zone", "k8s.cluster.name", "k8s.namespace.name", "k8s.pod.name", "k8s.container.name", "container.name", "k8s.replicaset.name", "k8s.deployment.name", "k8s.statefulset.name", "k8s.daemonset.name", "k8s.cronjob.name", "k8s.job.name"},
						},
					},
				},
			},
			"pattern_persistence_enabled": false,
			"query_timeout":               "1m",
			"retention_period":            "0s",
			"volume_enabled":              true,
			"volume_max_series":           1000,
		},
		"pattern_ingester_enabled": false,
		"version":                  "unknown",
		"maxDetectedFields":        1000,
		"maxDetectedValues":        1000,
		"maxLabelValues":           1000,
		"maxLines":                 p.maxLines,
	})
}

// handleDetectedLabels returns stream-level labels (similar to detected_fields but for stream labels).
func (p *Proxy) handleDetectedLabels(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	if p.handleMultiTenantFanout(w, r, "detected_labels", p.handleDetectedLabels) {
		return
	}
	r = withOrgID(r)
	lineLimit := parseDetectedLineLimit(r)
	detectedLabels, _, err := p.detectLabels(r.Context(), r.FormValue("query"), r.FormValue("start"), r.FormValue("end"), lineLimit)
	if err != nil {
		p.writeJSON(w, map[string]interface{}{
			"status":         "success",
			"data":           []interface{}{},
			"detectedLabels": []interface{}{},
			"limit":          lineLimit,
		})
		p.metrics.RecordRequest("detected_labels", http.StatusOK, time.Since(start))
		return
	}

	p.writeJSON(w, map[string]interface{}{
		"status":         "success",
		"data":           detectedLabels,
		"detectedLabels": detectedLabels,
		"limit":          lineLimit,
	})
	p.metrics.RecordRequest("detected_labels", http.StatusOK, time.Since(start))
}

// handleWriteBlocked rejects write requests — this is a read-only proxy.
func (p *Proxy) handleWriteBlocked(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusMethodNotAllowed)
	json.NewEncoder(w).Encode(map[string]string{
		"error": "write operations are not supported — this is a read-only proxy. Send logs directly to VictoriaLogs.",
	})
}

const (
	// maxDeleteTimeRange limits delete operations to 30 days for safety.
	maxDeleteTimeRange = 30 * 24 * time.Hour
)

// handleDelete is the sole write exception — proxies Loki delete requests to VL
// with strict safeguards: confirmation header, query validation, time range limits,
// tenant scoping, and audit logging.
//
// Loki: POST /loki/api/v1/delete?query={...}&start=...&end=...
// VL:   POST /select/logsql/delete?query=...&start=...&end=...
func (p *Proxy) handleDelete(w http.ResponseWriter, r *http.Request) {
	start := time.Now()

	// Only POST allowed
	if r.Method != http.MethodPost {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusMethodNotAllowed)
		json.NewEncoder(w).Encode(map[string]string{
			"error": "DELETE endpoint requires POST method",
		})
		return
	}

	// Safeguard 1: Require explicit confirmation header
	if r.Header.Get("X-Delete-Confirmation") != "true" {
		p.writeError(w, http.StatusBadRequest,
			"delete requires X-Delete-Confirmation: true header for safety")
		p.metrics.RecordRequest("delete", http.StatusBadRequest, time.Since(start))
		return
	}

	// Safeguard 2: Require non-empty, non-wildcard query
	query := r.FormValue("query")
	if query == "" {
		p.writeError(w, http.StatusBadRequest, "query parameter required for delete")
		p.metrics.RecordRequest("delete", http.StatusBadRequest, time.Since(start))
		return
	}
	trimmed := strings.TrimSpace(query)
	if trimmed == "{}" || trimmed == "*" || trimmed == "" {
		p.writeError(w, http.StatusBadRequest,
			"wildcard delete rejected — query must target specific streams (e.g., {app=\"nginx\"})")
		p.metrics.RecordRequest("delete", http.StatusBadRequest, time.Since(start))
		return
	}

	// Safeguard 3: Require time range
	startTS := r.FormValue("start")
	endTS := r.FormValue("end")
	if startTS == "" || endTS == "" {
		p.writeError(w, http.StatusBadRequest,
			"both start and end parameters required for delete")
		p.metrics.RecordRequest("delete", http.StatusBadRequest, time.Since(start))
		return
	}

	// Safeguard 4: Limit time range to maxDeleteTimeRange (30 days)
	startSec, err1 := strconv.ParseFloat(startTS, 64)
	endSec, err2 := strconv.ParseFloat(endTS, 64)
	if err1 == nil && err2 == nil {
		// Handle nanosecond timestamps (>1e15 is clearly nanoseconds)
		if startSec > 1e15 {
			startSec = startSec / 1e9
		}
		if endSec > 1e15 {
			endSec = endSec / 1e9
		}
		rangeDur := time.Duration(int64(endSec-startSec)) * time.Second
		if rangeDur > maxDeleteTimeRange {
			p.writeError(w, http.StatusBadRequest,
				fmt.Sprintf("delete time range too wide: %s exceeds maximum %s",
					rangeDur.Round(time.Hour), maxDeleteTimeRange))
			p.metrics.RecordRequest("delete", http.StatusBadRequest, time.Since(start))
			return
		}
		if rangeDur < 0 {
			p.writeError(w, http.StatusBadRequest, "end must be after start")
			p.metrics.RecordRequest("delete", http.StatusBadRequest, time.Since(start))
			return
		}
	}

	// Translate query
	logsqlQuery, err := p.translateQuery(query)
	if err != nil {
		p.writeError(w, http.StatusBadRequest, "failed to translate query: "+err.Error())
		p.metrics.RecordRequest("delete", http.StatusBadRequest, time.Since(start))
		return
	}

	// Safeguard 5: Tenant scoping
	r = withOrgID(r)
	tenant := r.Header.Get("X-Scope-OrgID")

	// Audit log BEFORE executing delete
	p.log.Warn("DELETE request",
		"tenant", tenant,
		"query", query,
		"logsql", logsqlQuery,
		"start", startTS,
		"end", endTS,
		"client", r.RemoteAddr,
	)

	// Forward to VL delete endpoint
	params := url.Values{}
	params.Set("query", logsqlQuery)
	params.Set("start", formatVLTimestamp(startTS))
	params.Set("end", formatVLTimestamp(endTS))

	resp, err := p.vlPost(r.Context(), "/select/logsql/delete", params)
	if err != nil {
		p.writeError(w, http.StatusBadGateway, "VL delete failed: "+err.Error())
		p.metrics.RecordRequest("delete", http.StatusBadGateway, time.Since(start))
		return
	}
	defer resp.Body.Close()

	// Propagate VL response
	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		p.writeError(w, resp.StatusCode, string(body))
		p.metrics.RecordRequest("delete", resp.StatusCode, time.Since(start))
		return
	}

	// Audit log AFTER successful delete
	p.log.Warn("DELETE completed",
		"tenant", tenant,
		"query", query,
		"start", startTS,
		"end", endTS,
		"vl_status", resp.StatusCode,
		"duration_ms", time.Since(start).Milliseconds(),
	)

	w.WriteHeader(http.StatusNoContent)
	p.metrics.RecordRequest("delete", http.StatusNoContent, time.Since(start))
}

// handleTail bridges Loki's WebSocket tail to VL's NDJSON streaming tail.
// Loki: ws:///loki/api/v1/tail?query={...}&start=...&limit=...
// VL:   GET /select/logsql/tail?query=...
func (p *Proxy) handleTail(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	logqlQuery := r.FormValue("query")
	if logqlQuery == "" {
		p.writeError(w, http.StatusBadRequest, "query parameter required")
		p.metrics.RecordRequest("tail", http.StatusBadRequest, time.Since(start))
		return
	}

	logsqlQuery, err := p.translateQuery(logqlQuery)
	if err != nil {
		p.writeError(w, http.StatusBadRequest, err.Error())
		p.metrics.RecordRequest("tail", http.StatusBadRequest, time.Since(start))
		return
	}

	if origin := strings.TrimSpace(r.Header.Get("Origin")); origin != "" && !p.isAllowedTailOrigin(origin) {
		p.writeError(w, http.StatusForbidden, "tail origin not allowed")
		p.metrics.RecordRequest("tail", http.StatusForbidden, time.Since(start))
		return
	}

	r = withOrgID(r)

	tailCtx, tailCancel := context.WithCancel(r.Context())
	defer tailCancel()

	if statusCode, msg, ok := p.preflightTailAccess(tailCtx, logsqlQuery, r.FormValue("start")); ok {
		p.writeError(w, statusCode, msg)
		p.metrics.RecordRequest("tail", statusCode, time.Since(start))
		return
	}

	// Upgrade immediately after local validation so slow or blocking native tail
	// headers do not break the client handshake. Native tail remains a best-effort
	// path; if it stalls or isn't available, synthetic polling takes over.
	upgrader := p.tailUpgrader()
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		p.log.Error("websocket upgrade failed", "error", err)
		p.metrics.RecordRequest("tail", http.StatusBadRequest, time.Since(start))
		return
	}
	defer func() { _ = conn.Close() }()
	p.metrics.RecordRequest("tail", http.StatusOK, time.Since(start))

	// Start a read loop to detect client disconnect (WebSocket protocol requires it).
	// When client closes, this goroutine exits and wsCtx is canceled.
	wsCtx, wsCancel := context.WithCancel(tailCtx)
	defer wsCancel()
	go func() {
		defer tailCancel()
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}()

	pingTicker := time.NewTicker(time.Second)
	defer pingTicker.Stop()

	if p.tailMode == TailModeSynthetic {
		p.log.Debug("tail connected", "logql", logqlQuery, "logsql", logsqlQuery, "native", false, "fallback", "forced synthetic tail mode")
		p.streamSyntheticTail(wsCtx, conn, logsqlQuery, r.FormValue("start"))
		return
	}

	resp, nativeTail, fallbackReason := p.openNativeTailStream(wsCtx, logsqlQuery)
	p.log.Debug("tail connected", "logql", logqlQuery, "logsql", logsqlQuery, "native", nativeTail, "fallback", fallbackReason)
	if !nativeTail {
		if p.tailMode == TailModeNative {
			_ = p.writeTailControl(conn, websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseInternalServerErr, fallbackReason))
			return
		}
		p.streamSyntheticTail(wsCtx, conn, logsqlQuery, r.FormValue("start"))
		return
	}
	defer resp.Body.Close()

	// Read VL NDJSON stream and forward as Loki WebSocket frames
	lineCh := make(chan []byte)
	errCh := make(chan error, 1)
	go func() {
		scanner := bufio.NewScanner(resp.Body)
		scanner.Buffer(make([]byte, 1024*1024), 1024*1024) // 1MB max line
		for scanner.Scan() {
			line := append([]byte(nil), scanner.Bytes()...)
			select {
			case lineCh <- line:
			case <-wsCtx.Done():
				return
			}
		}
		errCh <- scanner.Err()
	}()

	for {
		select {
		case <-wsCtx.Done():
			return
		case <-pingTicker.C:
			if err := p.writeTailMessage(conn, websocket.PingMessage, nil); err != nil {
				p.log.Debug("websocket ping failed, client disconnected", "error", err)
				return
			}
		case err := <-errCh:
			if err != nil && wsCtx.Err() == nil {
				p.log.Debug("tail stream ended with error", "error", err)
			}
			return
		case line := <-lineCh:
			if len(line) == 0 {
				continue
			}

			// Parse VL NDJSON line
			var vlLine map[string]interface{}
			if err := json.Unmarshal(line, &vlLine); err != nil {
				continue
			}

			// Convert to Loki tail frame
			frame := p.vlLineToTailFrame(vlLine)
			frameJSON, err := json.Marshal(frame)
			if err != nil {
				continue
			}

			if err := p.writeTailMessage(conn, websocket.TextMessage, frameJSON); err != nil {
				p.log.Debug("websocket write failed, client disconnected", "error", err)
				return
			}
		}
	}
}

func (p *Proxy) preflightTailAccess(parent context.Context, logsqlQuery, startHint string) (int, string, bool) {
	ctx, cancel := context.WithTimeout(parent, 2*time.Second)
	defer cancel()

	windowStart := time.Now().Add(-5 * time.Second)
	if parsed, ok := parseEntryTime(startHint); ok {
		windowStart = parsed
	}

	params := url.Values{}
	params.Set("query", logsqlQuery+" | sort by (_time desc)")
	params.Set("start", formatVLTimestamp(windowStart.UTC().Format(time.RFC3339Nano)))
	params.Set("end", formatVLTimestamp(time.Now().UTC().Format(time.RFC3339Nano)))
	params.Set("limit", "1")

	resp, err := p.vlGet(ctx, "/select/logsql/query", params)
	if err != nil {
		p.log.Debug("tail preflight skipped", "error", err)
		return 0, "", false
	}
	defer resp.Body.Close()

	if resp.StatusCode < 400 {
		_, _ = io.Copy(io.Discard, resp.Body)
		return 0, "", false
	}

	body, _ := io.ReadAll(resp.Body)
	msg := strings.TrimSpace(string(body))
	if msg == "" {
		msg = http.StatusText(resp.StatusCode)
	}
	return resp.StatusCode, msg, true
}

func (p *Proxy) openNativeTailStream(parent context.Context, logsqlQuery string) (*http.Response, bool, string) {
	ctx, cancel := context.WithTimeout(parent, 1500*time.Millisecond)
	defer cancel()

	vlURL := fmt.Sprintf("%s/select/logsql/tail?query=%s",
		p.backend.String(), url.QueryEscape(logsqlQuery))
	req, err := http.NewRequestWithContext(ctx, "GET", vlURL, nil)
	if err != nil {
		return nil, false, "failed to create native tail request"
	}
	p.applyBackendHeaders(req)
	p.forwardTenantHeaders(req)

	resp, err := p.tailClient.Do(req)
	if err != nil {
		return nil, false, err.Error()
	}
	if resp.StatusCode == http.StatusOK {
		return resp, true, ""
	}

	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	msg := strings.TrimSpace(string(body))
	if msg == "" {
		msg = http.StatusText(resp.StatusCode)
	}
	return nil, false, fmt.Sprintf("backend tail unavailable: %s", msg)
}

func (p *Proxy) streamSyntheticTail(ctx context.Context, conn tailConn, logsqlQuery, startHint string) {
	lastSeen := make(map[string]struct{}, 128)
	windowStart := time.Now().Add(-5 * time.Second)
	if parsed, ok := parseEntryTime(startHint); ok {
		windowStart = parsed
	}

	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		if err := p.writeSyntheticTailBatch(ctx, conn, logsqlQuery, &windowStart, lastSeen); err != nil {
			p.log.Debug("synthetic tail batch failed", "error", err)
		}

		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (p *Proxy) writeSyntheticTailBatch(ctx context.Context, conn tailConn, logsqlQuery string, windowStart *time.Time, lastSeen map[string]struct{}) error {
	params := url.Values{}
	params.Set("query", logsqlQuery+" | sort by (_time)")
	params.Set("start", formatVLTimestamp(windowStart.UTC().Format(time.RFC3339Nano)))
	params.Set("end", formatVLTimestamp(time.Now().UTC().Format(time.RFC3339Nano)))
	params.Set("limit", "200")

	resp, err := p.vlGet(ctx, "/select/logsql/query", params)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("synthetic tail query failed: status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 1024*1024), 1024*1024)
	newest := *windowStart
	for scanner.Scan() {
		line := append([]byte(nil), scanner.Bytes()...)
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}

		var vlLine map[string]interface{}
		if err := json.Unmarshal(line, &vlLine); err != nil {
			continue
		}
		timeStr, _ := stringifyEntryValue(vlLine["_time"])
		msgStr, _ := stringifyEntryValue(vlLine["_msg"])
		streamStr, _ := stringifyEntryValue(vlLine["_stream"])
		seenKey := timeStr + "\x00" + streamStr + "\x00" + msgStr
		if _, ok := lastSeen[seenKey]; ok {
			continue
		}
		lastSeen[seenKey] = struct{}{}

		if entryTime, ok := parseEntryTime(timeStr); ok && entryTime.After(newest) {
			newest = entryTime
		}

		frameJSON, err := json.Marshal(p.vlLineToTailFrame(vlLine))
		if err != nil {
			continue
		}
		if err := p.writeTailMessage(conn, websocket.TextMessage, frameJSON); err != nil {
			return err
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}

	*windowStart = newest.Add(time.Nanosecond)
	if len(lastSeen) > 1024 {
		clear(lastSeen)
	}
	return nil
}

func (p *Proxy) writeTailMessage(conn tailConn, messageType int, data []byte) error {
	if err := conn.SetWriteDeadline(time.Now().Add(tailWriteTimeout)); err != nil {
		return err
	}
	return conn.WriteMessage(messageType, data)
}

func (p *Proxy) writeTailControl(conn tailConn, messageType int, data []byte) error {
	deadline := time.Now().Add(tailWriteTimeout)
	if err := conn.SetWriteDeadline(deadline); err != nil {
		return err
	}
	return conn.WriteControl(messageType, data, deadline)
}

// vlLineToTailFrame converts a single VL NDJSON log line to a Loki tail WebSocket frame.
func (p *Proxy) vlLineToTailFrame(vlLine map[string]interface{}) map[string]interface{} {
	ts := ""
	msg := ""

	for k, v := range vlLine {
		sv := fmt.Sprintf("%v", v)
		switch k {
		case "_time":
			if t, err := time.Parse(time.RFC3339Nano, sv); err == nil {
				ts = fmt.Sprintf("%d", t.UnixNano())
			} else {
				ts = sv
			}
		case "_msg":
			msg = sv
		case "_stream":
			// Skip internal VL stream ID
		}
	}
	if ts == "" {
		ts = fmt.Sprintf("%d", time.Now().UnixNano())
	}

	labels := buildEntryLabels(vlLine)
	translatedLabels := labels
	if !p.labelTranslator.IsPassthrough() {
		translatedLabels = p.labelTranslator.TranslateLabelsMap(labels)
	}
	ensureDetectedLevel(translatedLabels)
	ensureSyntheticServiceName(translatedLabels)

	return map[string]interface{}{
		"streams": []map[string]interface{}{
			{
				"stream": translatedLabels,
				"values": [][]string{{ts, msg}},
			},
		},
	}
}

// handleReady returns readiness status.
func (p *Proxy) handleReady(w http.ResponseWriter, r *http.Request) {
	// Probe VL backend health
	readyReq := withOrgID(r)
	resp, err := p.vlGet(readyReq.Context(), "/health", nil)
	if err != nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		w.Write([]byte("backend not ready"))
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		w.WriteHeader(http.StatusServiceUnavailable)
		w.Write([]byte("backend not ready"))
		return
	}

	// Circuit breaker check
	if !p.breaker.Allow() {
		w.WriteHeader(http.StatusServiceUnavailable)
		w.Write([]byte("circuit breaker open"))
		return
	}

	w.Write([]byte("ready"))
}

func (p *Proxy) writeEmptyRules(w http.ResponseWriter) {
	p.writeJSON(w, map[string]interface{}{
		"status": "success",
		"data": map[string]interface{}{
			"groups": []interface{}{},
		},
	})
}

func (p *Proxy) writeEmptyLegacyRules(w http.ResponseWriter) {
	data, err := yaml.Marshal(map[string][]legacyRuleGroup{})
	if err != nil {
		p.writeError(w, http.StatusInternalServerError, fmt.Sprintf("failed to marshal rules response: %v", err))
		return
	}
	w.Header().Set("Content-Type", "application/yaml")
	_, _ = w.Write(data)
}

func (p *Proxy) writeEmptyAlerts(w http.ResponseWriter) {
	p.writeJSON(w, map[string]interface{}{
		"status": "success",
		"data": map[string]interface{}{
			"alerts": []interface{}{},
		},
	})
}

func (p *Proxy) handleRules(w http.ResponseWriter, r *http.Request) {
	if isLegacyRulesPath(r.URL.Path) {
		if p.rulerBackend != nil {
			p.handleLegacyRules(w, r)
			return
		}
		p.writeEmptyLegacyRules(w)
		return
	}
	if p.rulerBackend == nil {
		p.writeEmptyRules(w)
		return
	}
	p.proxyAlertingRead(w, r, p.rulerBackend, "/api/v1/rules")
}

func (p *Proxy) handleAlerts(w http.ResponseWriter, r *http.Request) {
	if p.alertsBackend == nil {
		p.writeEmptyAlerts(w)
		return
	}
	p.proxyAlertingRead(w, r, p.alertsBackend, "/api/v1/alerts")
}

// handleConfigStub returns a minimal config response.
func (p *Proxy) handleConfigStub(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/yaml")
	w.Write([]byte("# loki-vl-proxy\n"))
}

// handleBuildInfo returns fake build info for Grafana datasource detection.
func (p *Proxy) handleBuildInfo(w http.ResponseWriter, r *http.Request) {
	p.writeJSON(w, map[string]interface{}{
		"status": "success",
		"data": map[string]interface{}{
			"version":   "2.9.0",
			"revision":  "loki-vl-proxy",
			"branch":    "main",
			"goVersion": "go1.26.1",
		},
	})
}

// --- Backend request helpers ---

func (p *Proxy) proxyAlertingRead(w http.ResponseWriter, r *http.Request, backend *url.URL, path string) {
	resp, err := p.alertingBackendGet(withOrgID(r), backend, path)
	if err != nil {
		p.writeError(w, http.StatusBadGateway, err.Error())
		return
	}
	defer resp.Body.Close()

	copyHeaders(w.Header(), resp.Header)
	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, resp.Body)
}

func (p *Proxy) alertingBackendGet(r *http.Request, backend *url.URL, path string) (*http.Response, error) {
	return p.alertingBackendGetWithParams(r, backend, path, r.URL.Query())
}

func (p *Proxy) alertingBackendGetWithParams(r *http.Request, backend *url.URL, path string, params url.Values) (*http.Response, error) {
	if backend == nil {
		return nil, fmt.Errorf("alerting backend not configured")
	}

	u := *backend
	u.Path = path
	u.RawQuery = params.Encode()

	req, err := http.NewRequestWithContext(r.Context(), http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, err
	}
	p.forwardTenantHeaders(req)
	p.applyBackendHeaders(req)
	return p.client.Do(req)
}

type alertingRulesResponse struct {
	Status string `json:"status"`
	Data   struct {
		Groups []alertingRuleGroup `json:"groups"`
	} `json:"data"`
}

type alertingRuleGroup struct {
	Name     string                `json:"name"`
	File     string                `json:"file"`
	Interval float64               `json:"interval"`
	Rules    []alertingBackendRule `json:"rules"`
}

type alertingBackendRule struct {
	Name        string            `json:"name"`
	Query       string            `json:"query"`
	Type        string            `json:"type"`
	Duration    float64           `json:"duration"`
	Labels      map[string]string `json:"labels,omitempty"`
	Annotations map[string]string `json:"annotations,omitempty"`
}

type legacyRuleGroup struct {
	Name     string            `yaml:"name"`
	Interval string            `yaml:"interval,omitempty"`
	Rules    []legacyRuleEntry `yaml:"rules"`
}

type legacyRuleEntry struct {
	Alert       string            `yaml:"alert,omitempty"`
	Record      string            `yaml:"record,omitempty"`
	Expr        string            `yaml:"expr"`
	For         string            `yaml:"for,omitempty"`
	Labels      map[string]string `yaml:"labels,omitempty"`
	Annotations map[string]string `yaml:"annotations,omitempty"`
}

func isLegacyRulesPath(path string) bool {
	return path == "/loki/api/v1/rules" ||
		strings.HasPrefix(path, "/loki/api/v1/rules/") ||
		path == "/api/prom/rules" ||
		strings.HasPrefix(path, "/api/prom/rules/")
}

func parseLegacyRulesPath(path string) (namespace, group string, err error) {
	var suffix string
	switch {
	case strings.HasPrefix(path, "/loki/api/v1/rules/"):
		suffix = strings.TrimPrefix(path, "/loki/api/v1/rules/")
	case strings.HasPrefix(path, "/api/prom/rules/"):
		suffix = strings.TrimPrefix(path, "/api/prom/rules/")
	default:
		return "", "", nil
	}
	if suffix == "" {
		return "", "", nil
	}
	parts := strings.Split(suffix, "/")
	if len(parts) > 2 {
		return "", "", fmt.Errorf("invalid legacy rules path")
	}
	namespace, err = url.PathUnescape(parts[0])
	if err != nil {
		return "", "", fmt.Errorf("invalid namespace: %w", err)
	}
	if !filepath.IsLocal(namespace) {
		return "", "", fmt.Errorf("invalid namespace: path traversal not allowed")
	}
	if len(parts) == 2 {
		group, err = url.PathUnescape(parts[1])
		if err != nil {
			return "", "", fmt.Errorf("invalid group name: %w", err)
		}
		if !filepath.IsLocal(group) {
			return "", "", fmt.Errorf("invalid group name: path traversal not allowed")
		}
	}
	return namespace, group, nil
}

func (p *Proxy) handleLegacyRules(w http.ResponseWriter, r *http.Request) {
	namespace, group, err := parseLegacyRulesPath(r.URL.Path)
	if err != nil {
		p.writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	params := r.URL.Query()
	if namespace != "" {
		params["file[]"] = []string{namespace}
	}
	if group != "" {
		params["rule_group[]"] = []string{group}
	}

	resp, err := p.alertingBackendGetWithParams(withOrgID(r), p.rulerBackend, "/api/v1/rules", params)
	if err != nil {
		p.writeError(w, http.StatusBadGateway, err.Error())
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		copyHeaders(w.Header(), resp.Header)
		w.WriteHeader(resp.StatusCode)
		_, _ = io.Copy(w, resp.Body)
		return
	}

	var decoded alertingRulesResponse
	if err := json.NewDecoder(resp.Body).Decode(&decoded); err != nil {
		p.writeError(w, http.StatusBadGateway, fmt.Sprintf("invalid ruler backend response: %v", err))
		return
	}

	if group != "" {
		if len(decoded.Data.Groups) == 0 {
			p.writeError(w, http.StatusNotFound, "no rule groups found")
			return
		}
		out := legacyRuleGroupFromBackend(decoded.Data.Groups[0])
		data, err := yaml.Marshal(out)
		if err != nil {
			p.writeError(w, http.StatusInternalServerError, fmt.Sprintf("failed to marshal rules response: %v", err))
			return
		}
		w.Header().Set("Content-Type", "application/yaml")
		_, _ = w.Write(data)
		return
	}

	formatted := make(map[string][]legacyRuleGroup)
	for _, g := range decoded.Data.Groups {
		formatted[g.File] = append(formatted[g.File], legacyRuleGroupFromBackend(g))
	}
	data, err := yaml.Marshal(formatted)
	if err != nil {
		p.writeError(w, http.StatusInternalServerError, fmt.Sprintf("failed to marshal rules response: %v", err))
		return
	}
	w.Header().Set("Content-Type", "application/yaml")
	_, _ = w.Write(data)
}

func legacyRuleGroupFromBackend(g alertingRuleGroup) legacyRuleGroup {
	out := legacyRuleGroup{
		Name:  g.Name,
		Rules: make([]legacyRuleEntry, 0, len(g.Rules)),
	}
	if g.Interval > 0 {
		out.Interval = (time.Duration(g.Interval * float64(time.Second))).String()
	}
	for _, r := range g.Rules {
		entry := legacyRuleEntry{
			Expr:        r.Query,
			Labels:      r.Labels,
			Annotations: r.Annotations,
		}
		switch r.Type {
		case "alerting":
			entry.Alert = r.Name
		default:
			entry.Record = r.Name
		}
		if r.Duration > 0 {
			entry.For = (time.Duration(r.Duration * float64(time.Second))).String()
		}
		out.Rules = append(out.Rules, entry)
	}
	return out
}

func (p *Proxy) vlGet(ctx context.Context, path string, params url.Values) (*http.Response, error) {
	if !p.breaker.Allow() {
		return nil, fmt.Errorf("circuit breaker open — backend unavailable")
	}

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
	resp, err := p.client.Do(req)
	if err != nil {
		p.breaker.RecordFailure()
		return nil, err
	}
	if resp.StatusCode >= 500 {
		p.breaker.RecordFailure()
	} else {
		p.breaker.RecordSuccess()
	}
	return resp, nil
}

func (p *Proxy) vlPost(ctx context.Context, path string, params url.Values) (*http.Response, error) {
	if !p.breaker.Allow() {
		return nil, fmt.Errorf("circuit breaker open — backend unavailable")
	}

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
	resp, err := p.client.Do(req)
	if err != nil {
		p.breaker.RecordFailure()
		return nil, err
	}
	if resp.StatusCode >= 500 {
		p.breaker.RecordFailure()
	} else {
		p.breaker.RecordSuccess()
	}
	return resp, nil
}

// vlGetCoalesced wraps vlGet with request coalescing.
func (p *Proxy) vlGetCoalesced(ctx context.Context, key, path string, params url.Values) ([]byte, error) {
	_, _, body, err := p.coalescer.Do(key, func() (*http.Response, error) {
		return p.vlGet(ctx, path, params)
	})
	return body, err
}

// --- Stats query proxying ---

func (p *Proxy) proxyStatsQueryRange(w http.ResponseWriter, r *http.Request, logsqlQuery string) {
	params := url.Values{}
	params.Set("query", logsqlQuery)
	if s := r.FormValue("start"); s != "" {
		params.Set("start", formatVLTimestamp(s))
	}
	if e := r.FormValue("end"); e != "" {
		params.Set("end", formatVLTimestamp(e))
	}
	if step := r.FormValue("step"); step != "" {
		params.Set("step", formatVLStep(step))
	}

	resp, err := p.vlPost(r.Context(), "/select/logsql/stats_query_range", params)
	if err != nil {
		p.writeError(w, http.StatusBadGateway, err.Error())
		return
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)

	// Propagate VL error status
	if resp.StatusCode >= 400 {
		p.writeError(w, resp.StatusCode, string(body))
		return
	}

	// VL stats_query_range returns Prometheus-compatible format.
	// Just wrap it in Loki's envelope.
	// Translate label names (e.g., dots → underscores) in metric labels.
	body = p.translateStatsResponseLabels(body, r.FormValue("query"))
	w.Header().Set("Content-Type", "application/json")
	w.Write(wrapAsLokiResponse(body, "matrix"))
}

func (p *Proxy) proxyStatsQuery(w http.ResponseWriter, r *http.Request, logsqlQuery string) {
	params := url.Values{}
	params.Set("query", logsqlQuery)
	if t := r.FormValue("time"); t != "" {
		params.Set("time", formatVLTimestamp(t))
	}

	resp, err := p.vlPost(r.Context(), "/select/logsql/stats_query", params)
	if err != nil {
		p.writeError(w, http.StatusBadGateway, err.Error())
		return
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)

	// Propagate VL error status
	if resp.StatusCode >= 400 {
		p.writeError(w, resp.StatusCode, string(body))
		return
	}

	body = p.translateStatsResponseLabels(body, r.FormValue("query"))
	w.Header().Set("Content-Type", "application/json")
	w.Write(wrapAsLokiResponse(body, "vector"))
}

// proxyBinaryMetricQueryRangeVM evaluates with vector matching (on/ignoring/group_left/group_right).
func (p *Proxy) proxyBinaryMetricQueryRangeVM(w http.ResponseWriter, r *http.Request, op, leftQL, rightQL string, vm *translator.VectorMatchInfo) {
	p.proxyBinaryMetricVM(w, r, op, leftQL, rightQL, "stats_query_range", "matrix", vm)
}

func (p *Proxy) proxyBinaryMetricQueryVM(w http.ResponseWriter, r *http.Request, op, leftQL, rightQL string, vm *translator.VectorMatchInfo) {
	p.proxyBinaryMetricVM(w, r, op, leftQL, rightQL, "stats_query", "vector", vm)
}

func (p *Proxy) proxyBinaryMetricVM(w http.ResponseWriter, r *http.Request, op, leftQL, rightQL, vlEndpoint, resultType string, vm *translator.VectorMatchInfo) {
	// If no vector matching, fall back to default behavior
	if vm == nil || (len(vm.On) == 0 && len(vm.Ignoring) == 0 && len(vm.GroupLeft) == 0 && len(vm.GroupRight) == 0) {
		p.proxyBinaryMetric(w, r, op, leftQL, rightQL, vlEndpoint, resultType)
		return
	}

	isRange := vlEndpoint == "stats_query_range"
	buildParams := func(query string) url.Values {
		params := url.Values{"query": {query}}
		if isRange {
			if s := r.FormValue("start"); s != "" {
				params.Set("start", formatVLTimestamp(s))
			}
			if e := r.FormValue("end"); e != "" {
				params.Set("end", formatVLTimestamp(e))
			}
			if step := r.FormValue("step"); step != "" {
				params.Set("step", formatVLStep(step))
			}
		} else {
			if t := r.FormValue("time"); t != "" {
				params.Set("time", formatVLTimestamp(t))
			}
		}
		return params
	}

	leftIsScalar := translator.IsScalar(leftQL)
	rightIsScalar := translator.IsScalar(rightQL)

	var leftBody, rightBody []byte
	if leftIsScalar {
		leftBody = []byte(`{"status":"success","data":{"resultType":"scalar","result":[0,"` + leftQL + `"]}}`)
	} else {
		resp, e := p.vlPost(r.Context(), "/select/logsql/"+vlEndpoint, buildParams(leftQL))
		if e != nil {
			p.writeError(w, http.StatusBadGateway, "left query: "+e.Error())
			return
		}
		defer resp.Body.Close()
		leftBody, _ = io.ReadAll(resp.Body)
	}

	if rightIsScalar {
		rightBody = []byte(`{"status":"success","data":{"resultType":"scalar","result":[0,"` + rightQL + `"]}}`)
	} else {
		resp, e := p.vlPost(r.Context(), "/select/logsql/"+vlEndpoint, buildParams(rightQL))
		if e != nil {
			p.writeError(w, http.StatusBadGateway, "right query: "+e.Error())
			return
		}
		defer resp.Body.Close()
		rightBody, _ = io.ReadAll(resp.Body)
	}

	// Apply vector matching: on(), ignoring(), group_left(), group_right()
	var result []byte
	if len(vm.On) > 0 {
		result = applyOnMatching(leftBody, rightBody, op, vm.On, resultType)
	} else if len(vm.Ignoring) > 0 {
		result = applyIgnoringMatching(leftBody, rightBody, op, vm.Ignoring, resultType)
	} else {
		// group_left/group_right without on/ignoring — use default matching
		result = combineBinaryMetricResults(leftBody, rightBody, op, resultType, leftIsScalar, rightIsScalar, leftQL, rightQL)
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write(result)
}

func (p *Proxy) proxyBinaryMetric(w http.ResponseWriter, r *http.Request, op, leftQL, rightQL, vlEndpoint, resultType string) {
	isRange := vlEndpoint == "stats_query_range"

	buildParams := func(query string) url.Values {
		params := url.Values{"query": {query}}
		if isRange {
			if s := r.FormValue("start"); s != "" {
				params.Set("start", formatVLTimestamp(s))
			}
			if e := r.FormValue("end"); e != "" {
				params.Set("end", formatVLTimestamp(e))
			}
			if step := r.FormValue("step"); step != "" {
				params.Set("step", formatVLStep(step))
			}
		} else {
			if t := r.FormValue("time"); t != "" {
				params.Set("time", formatVLTimestamp(t))
			}
		}
		return params
	}

	// Check if either side is a scalar (number)
	leftIsScalar := translator.IsScalar(leftQL)
	rightIsScalar := translator.IsScalar(rightQL)

	var leftBody, rightBody []byte

	if leftIsScalar {
		leftBody = []byte(`{"status":"success","data":{"resultType":"scalar","result":[0,"` + leftQL + `"]}}`)
	} else {
		resp, e := p.vlPost(r.Context(), "/select/logsql/"+vlEndpoint, buildParams(leftQL))
		if e != nil {
			p.writeError(w, http.StatusBadGateway, "left query: "+e.Error())
			return
		}
		defer resp.Body.Close()
		leftBody, _ = io.ReadAll(resp.Body)
	}

	if rightIsScalar {
		rightBody = []byte(`{"status":"success","data":{"resultType":"scalar","result":[0,"` + rightQL + `"]}}`)
	} else {
		resp, e := p.vlPost(r.Context(), "/select/logsql/"+vlEndpoint, buildParams(rightQL))
		if e != nil {
			p.writeError(w, http.StatusBadGateway, "right query: "+e.Error())
			return
		}
		defer resp.Body.Close()
		rightBody, _ = io.ReadAll(resp.Body)
	}

	// Combine results with arithmetic at proxy level
	result := combineBinaryMetricResults(leftBody, rightBody, op, resultType, leftIsScalar, rightIsScalar, leftQL, rightQL)

	w.Header().Set("Content-Type", "application/json")
	w.Write(result)
}

// combineBinaryMetricResults applies arithmetic op to two VL stats results.
func combineBinaryMetricResults(leftBody, rightBody []byte, op, resultType string, leftScalar, rightScalar bool, leftQL, rightQL string) []byte {
	// For scalar operations (e.g., rate(...) * 100), apply to each value
	if rightScalar {
		scalar := parseScalar(rightQL)
		return applyScalarOp(leftBody, op, scalar, resultType)
	}
	if leftScalar {
		scalar := parseScalar(leftQL)
		return applyScalarOpReverse(rightBody, op, scalar, resultType)
	}

	// Both sides are metric results — combine point-by-point
	// This is a simplified implementation that handles the common case
	// of matching time series (same labels, same timestamps)
	return combineMetricResults(leftBody, rightBody, op, resultType)
}

func parseScalar(s string) float64 {
	f, _ := strconv.ParseFloat(strings.TrimSpace(s), 64)
	return f
}

func applyScalarOp(body []byte, op string, scalar float64, resultType string) []byte {
	var vlResp map[string]interface{}
	if err := json.Unmarshal(body, &vlResp); err != nil {
		return wrapAsLokiResponse(body, resultType)
	}

	// VL stats_query_range returns {"results": [{"metric":{}, "values": [[ts, val]...]}...]}
	results, _ := vlResp["results"].([]interface{})
	for _, r := range results {
		rm, _ := r.(map[string]interface{})
		values, _ := rm["values"].([]interface{})
		for i, v := range values {
			point, _ := v.([]interface{})
			if len(point) >= 2 {
				valStr, _ := point[1].(string)
				val, _ := strconv.ParseFloat(valStr, 64)
				newVal := applyOp(val, scalar, op)
				point[1] = strconv.FormatFloat(newVal, 'f', -1, 64)
				values[i] = point
			}
		}
	}

	result, _ := json.Marshal(vlResp)
	return wrapAsLokiResponse(result, resultType)
}

func applyScalarOpReverse(body []byte, op string, scalar float64, resultType string) []byte {
	var vlResp map[string]interface{}
	if err := json.Unmarshal(body, &vlResp); err != nil {
		return wrapAsLokiResponse(body, resultType)
	}

	results, _ := vlResp["results"].([]interface{})
	for _, r := range results {
		rm, _ := r.(map[string]interface{})
		values, _ := rm["values"].([]interface{})
		for i, v := range values {
			point, _ := v.([]interface{})
			if len(point) >= 2 {
				valStr, _ := point[1].(string)
				val, _ := strconv.ParseFloat(valStr, 64)
				newVal := applyOp(scalar, val, op)
				point[1] = strconv.FormatFloat(newVal, 'f', -1, 64)
				values[i] = point
			}
		}
	}

	result, _ := json.Marshal(vlResp)
	return wrapAsLokiResponse(result, resultType)
}

func combineMetricResults(leftBody, rightBody []byte, op, resultType string) []byte {
	// Parse both results
	var leftResp, rightResp map[string]interface{}
	json.Unmarshal(leftBody, &leftResp)
	json.Unmarshal(rightBody, &rightResp)

	leftResults, _ := leftResp["results"].([]interface{})
	rightResults, _ := rightResp["results"].([]interface{})

	// Build a map of right results by metric labels for joining
	rightMap := make(map[string][]interface{})
	for _, r := range rightResults {
		rm, _ := r.(map[string]interface{})
		metric, _ := rm["metric"].(map[string]interface{})
		key := metricKey(metric)
		values, _ := rm["values"].([]interface{})
		rightMap[key] = values
	}

	// Combine: for each left result, find matching right result and apply op
	for _, r := range leftResults {
		rm, _ := r.(map[string]interface{})
		metric, _ := rm["metric"].(map[string]interface{})
		key := metricKey(metric)
		leftValues, _ := rm["values"].([]interface{})
		rightValues := rightMap[key]

		if len(rightValues) > 0 {
			// Build timestamp→value index for right side
			rightIdx := make(map[string]float64)
			for _, v := range rightValues {
				point, _ := v.([]interface{})
				if len(point) >= 2 {
					ts := fmt.Sprintf("%v", point[0])
					valStr, _ := point[1].(string)
					val, _ := strconv.ParseFloat(valStr, 64)
					rightIdx[ts] = val
				}
			}

			// Apply op point-by-point
			for i, v := range leftValues {
				point, _ := v.([]interface{})
				if len(point) >= 2 {
					ts := fmt.Sprintf("%v", point[0])
					leftValStr, _ := point[1].(string)
					leftVal, _ := strconv.ParseFloat(leftValStr, 64)
					if rightVal, ok := rightIdx[ts]; ok {
						newVal := applyOp(leftVal, rightVal, op)
						point[1] = strconv.FormatFloat(newVal, 'f', -1, 64)
					}
					leftValues[i] = point
				}
			}
		}
	}

	result, _ := json.Marshal(leftResp)
	return wrapAsLokiResponse(result, resultType)
}

func applyOp(a, b float64, op string) float64 {
	switch op {
	case "/":
		if b == 0 {
			return 0 // avoid division by zero, return 0 like Prometheus
		}
		return a / b
	case "*":
		return a * b
	case "+":
		return a + b
	case "-":
		return a - b
	case "%":
		if b == 0 {
			return 0
		}
		return math.Mod(a, b)
	case "^":
		return math.Pow(a, b)
	case "==":
		if a == b {
			return 1
		}
		return 0
	case "!=":
		if a != b {
			return 1
		}
		return 0
	case ">":
		if a > b {
			return 1
		}
		return 0
	case "<":
		if a < b {
			return 1
		}
		return 0
	case ">=":
		if a >= b {
			return 1
		}
		return 0
	case "<=":
		if a <= b {
			return 1
		}
		return 0
	}
	return a
}

func metricKey(metric map[string]interface{}) string {
	if metric == nil {
		return "{}"
	}
	parts := make([]string, 0, len(metric))
	for k, v := range metric {
		parts = append(parts, fmt.Sprintf("%s=%v", k, v))
	}
	sort.Strings(parts)
	return strings.Join(parts, ",")
}

// proxyLogQuery fetches log lines from VictoriaLogs.
func (p *Proxy) proxyLogQuery(w http.ResponseWriter, r *http.Request, logsqlQuery string) {
	// Loki direction param: "forward" = oldest first, "backward" (default) = newest first
	direction := r.FormValue("direction")
	if direction == "forward" {
		logsqlQuery += " | sort by (_time)"
	} else {
		logsqlQuery += " | sort by (_time desc)"
	}

	params := url.Values{}
	params.Set("query", logsqlQuery)
	if s := r.FormValue("start"); s != "" {
		params.Set("start", formatVLTimestamp(s))
	}
	if e := r.FormValue("end"); e != "" {
		params.Set("end", formatVLTimestamp(e))
	}
	limit := r.FormValue("limit")
	if limit == "" {
		limit = strconv.Itoa(p.maxLines)
	}
	params.Set("limit", sanitizeLimit(limit))

	resp, err := p.vlPost(r.Context(), "/select/logsql/query", params)
	if err != nil {
		p.writeError(w, http.StatusBadGateway, err.Error())
		return
	}
	defer resp.Body.Close()

	// Propagate VL error status to the client
	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		errMsg := string(body)
		if errMsg == "" {
			errMsg = fmt.Sprintf("VL backend returned %d", resp.StatusCode)
		}
		p.writeError(w, resp.StatusCode, errMsg)
		return
	}

	// Chunked streaming: flush partial results as they arrive from VL
	if p.streamResponse {
		p.streamLogQuery(w, resp)
		return
	}

	body, _ := io.ReadAll(resp.Body)

	// VL returns newline-delimited JSON, each line is a log entry.
	// Loki expects: {"status":"success","data":{"resultType":"streams","result":[...]}}
	streams := p.vlLogsToLokiStreams(body, r.FormValue("query"))

	// Apply derived fields (extract trace_id etc. from log lines)
	if len(p.derivedFields) > 0 {
		p.applyDerivedFields(streams)
	}

	// Apply proxy-side post-processing for Loki features VL doesn't natively support.
	// These are applied after VL returns results, implementing Loki behavior at the proxy.
	// TODO: Remove each when VL adds native equivalents.
	logqlQuery := r.FormValue("query")
	if strings.Contains(logqlQuery, "decolorize") {
		decolorizeStreams(streams)
	}
	if label, cidr, ok := parseIPFilter(logqlQuery); ok {
		streams = ipFilterStreams(streams, label, cidr)
	}
	if tmpl := extractLineFormatTemplate(logqlQuery); tmpl != "" {
		applyLineFormatTemplate(streams, tmpl)
	}

	result, _ := json.Marshal(map[string]interface{}{
		"status": "success",
		"data": map[string]interface{}{
			"resultType": "streams",
			"result":     streams,
			"stats":      map[string]interface{}{},
		},
	})
	w.Header().Set("Content-Type", "application/json")
	w.Write(result)
}

// streamLogQuery streams VL NDJSON response as chunked Loki-compatible JSON.
func (p *Proxy) streamLogQuery(w http.ResponseWriter, resp *http.Response) {
	flusher, canFlush := w.(http.Flusher)

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Transfer-Encoding", "chunked")

	// Write opening envelope
	w.Write([]byte(`{"status":"success","data":{"resultType":"streams","result":[`))
	if canFlush {
		flusher.Flush()
	}

	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 1024*1024), 1024*1024)

	first := true
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}

		var entry map[string]interface{}
		if err := json.Unmarshal(line, &entry); err != nil {
			continue
		}

		timeStr, ok := stringifyEntryValue(entry["_time"])
		if !ok || timeStr == "" {
			continue
		}
		msg, _ := stringifyEntryValue(entry["_msg"])

		tsNanos, ok := formatEntryTimestamp(timeStr)
		if !ok {
			continue
		}

		labels, metadata := p.classifyEntryFields(entry, "")
		translatedLabels := labels
		if !p.labelTranslator.IsPassthrough() {
			translatedLabels = p.labelTranslator.TranslateLabelsMap(labels)
		}
		ensureDetectedLevel(translatedLabels)
		ensureSyntheticServiceName(translatedLabels)

		stream := map[string]interface{}{
			"stream": translatedLabels,
			"values": buildStreamValues(tsNanos, msg, metadata),
		}

		chunk, _ := json.Marshal(stream)
		if !first {
			w.Write([]byte(","))
		}
		w.Write(chunk)
		first = false

		if canFlush {
			flusher.Flush()
		}
	}

	// Close envelope
	w.Write([]byte(`],"stats":{}}}`))
	if canFlush {
		flusher.Flush()
	}
}

// applyDerivedFields extracts values from log lines using regex and adds them as labels.
// This enables Grafana's "Derived fields" feature for trace linking.
func (p *Proxy) applyDerivedFields(streams []map[string]interface{}) {
	// Pre-compile regexes
	type compiledDF struct {
		name string
		re   *regexp.Regexp
		url  string
	}
	compiled := make([]compiledDF, 0, len(p.derivedFields))
	for _, df := range p.derivedFields {
		re, err := regexp.Compile(df.MatcherRegex)
		if err != nil {
			p.log.Warn("invalid derived field regex", "name", df.Name, "error", err)
			continue
		}
		compiled = append(compiled, compiledDF{name: df.Name, re: re, url: df.URL})
	}

	for _, stream := range streams {
		values, ok := stream["values"].([]interface{})
		if !ok {
			continue
		}
		labels := streamStringMap(stream["stream"])
		if labels == nil {
			continue
		}

		for _, val := range values {
			pair, ok := val.([]interface{})
			if !ok || len(pair) < 2 {
				continue
			}
			line, ok := pair[1].(string)
			if !ok {
				continue
			}
			for _, cdf := range compiled {
				matches := cdf.re.FindStringSubmatch(line)
				if len(matches) > 1 {
					// Use first capture group as the value
					labels[cdf.name] = matches[1]
				} else if len(matches) == 1 {
					// Full match, no capture group
					labels[cdf.name] = matches[0]
				}
			}
		}
	}
}

func streamStringMap(value interface{}) map[string]string {
	switch labels := value.(type) {
	case map[string]string:
		return labels
	case map[string]interface{}:
		result := make(map[string]string, len(labels))
		for k, v := range labels {
			if s, ok := v.(string); ok {
				result[k] = s
			}
		}
		return result
	default:
		return nil
	}
}

// --- Response converters ---

func lokiLabelsResponse(labels []string) []byte {
	result, _ := json.Marshal(map[string]interface{}{
		"status": "success",
		"data":   labels,
	})
	return result
}

// vlEntryPool pools map[string]interface{} to reduce GC pressure in NDJSON parsing.
var vlEntryPool = sync.Pool{
	New: func() interface{} {
		return make(map[string]interface{}, 8)
	},
}

// vlLogsToLokiStreams converts VL newline-delimited JSON logs to Loki streams format.
// Optimized: byte scanning, pooled maps, pre-allocated slices.
func vlLogsToLokiStreams(body []byte) []map[string]interface{} {
	type streamEntry struct {
		Labels map[string]string
		Values [][]string // [[timestamp_ns, line], ...]
	}
	// Estimate line count from body size (~200 bytes/line average)
	estimatedLines := len(body)/200 + 1
	streamMap := make(map[string]*streamEntry, estimatedLines/10+1)

	// Scan lines without copying the entire body to a string
	start := 0
	for i := 0; i <= len(body); i++ {
		if i < len(body) && body[i] != '\n' {
			continue
		}
		line := body[start:i]
		start = i + 1

		// Trim whitespace (avoid bytes.TrimSpace allocation)
		for len(line) > 0 && (line[0] == ' ' || line[0] == '\t' || line[0] == '\r') {
			line = line[1:]
		}
		for len(line) > 0 && (line[len(line)-1] == ' ' || line[len(line)-1] == '\t' || line[len(line)-1] == '\r') {
			line = line[:len(line)-1]
		}
		if len(line) == 0 {
			continue
		}

		// Use pooled map to reduce allocations
		entry := vlEntryPool.Get().(map[string]interface{})
		for k := range entry {
			delete(entry, k) // clear reused map
		}
		if err := json.Unmarshal(line, &entry); err != nil {
			vlEntryPool.Put(entry)
			continue
		}

		// Extract _time, _msg, _stream, and remaining fields as labels
		timeStr, _ := entry["_time"].(string)
		msg, _ := entry["_msg"].(string)
		if timeStr == "" {
			vlEntryPool.Put(entry)
			continue
		}

		// Parse time to nanoseconds
		ts, err := time.Parse(time.RFC3339Nano, timeStr)
		if err != nil {
			vlEntryPool.Put(entry)
			continue
		}
		tsNanos := strconv.FormatInt(ts.UnixNano(), 10)

		labels := buildEntryLabels(entry)
		streamKey := canonicalLabelsKey(labels)

		se, ok := streamMap[streamKey]
		if !ok {
			se = &streamEntry{
				Labels: labels,
				Values: make([][]string, 0),
			}
			streamMap[streamKey] = se
		}

		// Return pooled entry after extracting all needed data
		vlEntryPool.Put(entry)

		se.Values = append(se.Values, []string{tsNanos, msg})
	}

	result := make([]map[string]interface{}, 0, len(streamMap))
	for _, se := range streamMap {
		result = append(result, map[string]interface{}{
			"stream": se.Labels,
			"values": se.Values,
		})
	}
	return result
}

func (p *Proxy) vlLogsToLokiStreams(body []byte, originalQuery string) []map[string]interface{} {
	type streamEntry struct {
		Labels map[string]string
		Values []interface{}
	}

	streamMap := make(map[string]*streamEntry, len(body)/256+1)
	start := 0
	for i := 0; i <= len(body); i++ {
		if i < len(body) && body[i] != '\n' {
			continue
		}
		line := body[start:i]
		start = i + 1

		for len(line) > 0 && (line[0] == ' ' || line[0] == '\t' || line[0] == '\r') {
			line = line[1:]
		}
		for len(line) > 0 && (line[len(line)-1] == ' ' || line[len(line)-1] == '\t' || line[len(line)-1] == '\r') {
			line = line[:len(line)-1]
		}
		if len(line) == 0 {
			continue
		}

		entry := vlEntryPool.Get().(map[string]interface{})
		for k := range entry {
			delete(entry, k)
		}
		if err := json.Unmarshal(line, &entry); err != nil {
			vlEntryPool.Put(entry)
			continue
		}

		timeStr, ok := stringifyEntryValue(entry["_time"])
		if !ok || timeStr == "" {
			vlEntryPool.Put(entry)
			continue
		}
		tsNanos, ok := formatEntryTimestamp(timeStr)
		if !ok {
			vlEntryPool.Put(entry)
			continue
		}
		msg, _ := stringifyEntryValue(entry["_msg"])

		labels, metadata := p.classifyEntryFields(entry, originalQuery)
		streamKey := canonicalLabelsKey(labels)
		se, ok := streamMap[streamKey]
		if !ok {
			translatedLabels := labels
			if !p.labelTranslator.IsPassthrough() {
				translatedLabels = p.labelTranslator.TranslateLabelsMap(labels)
			}
			ensureDetectedLevel(translatedLabels)
			ensureSyntheticServiceName(translatedLabels)

			se = &streamEntry{
				Labels: translatedLabels,
				Values: make([]interface{}, 0, 8),
			}
			streamMap[streamKey] = se
		}
		se.Values = append(se.Values, buildStreamValue(tsNanos, msg, metadata))
		vlEntryPool.Put(entry)
	}

	result := make([]map[string]interface{}, 0, len(streamMap))
	for _, se := range streamMap {
		result = append(result, map[string]interface{}{
			"stream": se.Labels,
			"values": se.Values,
		})
	}
	return result
}

func stringifyEntryValue(value interface{}) (string, bool) {
	if value == nil {
		return "", false
	}
	switch v := value.(type) {
	case string:
		return v, true
	default:
		return fmt.Sprintf("%v", v), true
	}
}

func formatEntryTimestamp(timeStr string) (string, bool) {
	if parsed, err := time.Parse(time.RFC3339Nano, timeStr); err == nil {
		return strconv.FormatInt(parsed.UnixNano(), 10), true
	}
	if parsed, err := time.Parse(time.RFC3339, timeStr); err == nil {
		return strconv.FormatInt(parsed.UnixNano(), 10), true
	}
	if _, err := strconv.ParseInt(timeStr, 10, 64); err == nil {
		return timeStr, true
	}
	return "", false
}

func buildStreamValues(ts, msg string, metadata map[string]interface{}) []interface{} {
	return []interface{}{buildStreamValue(ts, msg, metadata)}
}

func buildStreamValue(ts, msg string, metadata map[string]interface{}) interface{} {
	// Current Grafana Loki datasource / Drilldown query paths still reject
	// 3-tuple log values with metadata objects. Keep canonical 2-tuple values
	// here and expose parsed fields through dedicated Drilldown resources.
	_ = metadata
	return []interface{}{ts, msg}
}

func (p *Proxy) classifyEntryFields(entry map[string]interface{}, originalQuery string) (map[string]string, map[string]interface{}) {
	labels := parseStreamLabels(asString(entry["_stream"]))
	if value, ok := stringifyEntryValue(entry["level"]); ok && strings.TrimSpace(value) != "" {
		labels["level"] = value
	}
	ensureDetectedLevel(labels)
	ensureSyntheticServiceName(labels)

	var (
		parsedFields             map[string]string
		structuredMetadataFields map[string]string
	)
	classifyAsParsed := hasParserStage(originalQuery, "json") || hasParserStage(originalQuery, "logfmt")

	for key, value := range entry {
		if isVLInternalField(key) || key == "_stream_id" || key == "level" {
			continue
		}
		if _, exists := labels[key]; exists {
			continue
		}
		stringValue, ok := stringifyEntryValue(value)
		if !ok || strings.TrimSpace(stringValue) == "" {
			continue
		}
		for _, exposure := range p.metadataFieldExposures(key) {
			if _, exists := labels[exposure.name]; exists && !exposure.isAlias {
				continue
			}
			if classifyAsParsed {
				if parsedFields == nil {
					parsedFields = make(map[string]string, 4)
				}
				parsedFields[exposure.name] = stringValue
				continue
			}
			if structuredMetadataFields == nil {
				structuredMetadataFields = make(map[string]string, 4)
			}
			structuredMetadataFields[exposure.name] = stringValue
		}
	}

	metadata := map[string]interface{}{}
	if len(structuredMetadataFields) > 0 {
		metadata["structuredMetadata"] = structuredMetadataFields
	}
	if len(parsedFields) > 0 {
		metadata["parsed"] = parsedFields
	}
	return labels, metadata
}

// parseStreamLabels parses {key="value",key2="value2"} into a map.
func parseStreamLabels(s string) map[string]string {
	labels := make(map[string]string)
	s = strings.Trim(s, "{}")
	if s == "" {
		return labels
	}

	// Simple parser for key="value" pairs
	for _, pair := range splitLabelPairs(s) {
		pair = strings.TrimSpace(pair)
		eqIdx := strings.Index(pair, "=")
		if eqIdx <= 0 {
			continue
		}
		key := strings.TrimSpace(pair[:eqIdx])
		val := strings.TrimSpace(pair[eqIdx+1:])
		val = strings.Trim(val, `"`)
		labels[key] = val
	}
	return labels
}

func splitLabelPairs(s string) []string {
	var pairs []string
	inQuote := false
	start := 0
	for i, c := range s {
		if c == '"' {
			inQuote = !inQuote
		}
		if c == ',' && !inQuote {
			pairs = append(pairs, s[start:i])
			start = i + 1
		}
	}
	if start < len(s) {
		pairs = append(pairs, s[start:])
	}
	return pairs
}

func wrapAsLokiResponse(vlBody []byte, resultType string) []byte {
	// VL stats endpoints return Prometheus-compatible format already.
	// Try to parse and re-wrap in Loki envelope.
	var promResp map[string]interface{}
	if err := json.Unmarshal(vlBody, &promResp); err != nil {
		// If we can't parse, return as-is with wrapper
		result, _ := json.Marshal(map[string]interface{}{
			"status": "success",
			"data": map[string]interface{}{
				"resultType": resultType,
				"result":     []interface{}{},
			},
		})
		return result
	}

	// Check for VL error responses and translate to Loki error format.
	// VL may return: {"error":"message"} or {"status":"error","msg":"message"}
	if errMsg, ok := promResp["error"].(string); ok {
		result, _ := json.Marshal(map[string]interface{}{
			"status":    "error",
			"errorType": "bad_request",
			"error":     errMsg,
		})
		return result
	}
	if promResp["status"] == "error" {
		errMsg := ""
		if msg, ok := promResp["msg"].(string); ok {
			errMsg = msg
		} else if msg, ok := promResp["message"].(string); ok {
			errMsg = msg
		} else if msg, ok := promResp["error"].(string); ok {
			errMsg = msg
		}
		result, _ := json.Marshal(map[string]interface{}{
			"status":    "error",
			"errorType": "bad_request",
			"error":     errMsg,
		})
		return result
	}

	// If VL already returned status/data format, pass through
	if _, ok := promResp["data"]; ok {
		result, _ := json.Marshal(map[string]interface{}{
			"status": "success",
			"data":   promResp["data"],
		})
		return result
	}

	result, _ := json.Marshal(map[string]interface{}{
		"status": "success",
		"data":   promResp,
	})
	return result
}

// --- VL hits response conversion helpers ---

type vlHitsResponse struct {
	Hits []struct {
		Fields     map[string]string `json:"fields"`
		Timestamps []string          `json:"timestamps"` // VL v1.49+: RFC3339 strings
		Values     []int             `json:"values"`
	} `json:"hits"`
}

// parseTimestampToUnix converts a VL timestamp (RFC3339 string or numeric) to Unix seconds.
func parseTimestampToUnix(ts string) float64 {
	t, err := time.Parse(time.RFC3339, ts)
	if err == nil {
		return float64(t.Unix())
	}
	// Try numeric fallback
	if f, err := strconv.ParseFloat(ts, 64); err == nil {
		return f
	}
	return float64(time.Now().Unix())
}

func parseHits(body []byte) vlHitsResponse {
	var resp vlHitsResponse
	json.Unmarshal(body, &resp)
	return resp
}

func sumHitsValues(body []byte) int {
	hits := parseHits(body)
	total := 0
	for _, h := range hits.Hits {
		for _, v := range h.Values {
			total += v
		}
	}
	return total
}

func (p *Proxy) translateVolumeMetric(fields map[string]string) map[string]string {
	if fields == nil {
		return nil
	}
	translated := fields
	if p != nil && p.labelTranslator != nil && !p.labelTranslator.IsPassthrough() {
		translated = p.labelTranslator.TranslateLabelsMap(fields)
	}
	if translated == nil {
		return nil
	}
	ensureSyntheticServiceName(translated)
	return translated
}

func (p *Proxy) hitsToVolumeVector(body []byte) map[string]interface{} {
	hits := parseHits(body)
	result := make([]map[string]interface{}, 0, len(hits.Hits))
	for _, h := range hits.Hits {
		total := 0
		var lastTS float64
		for i, v := range h.Values {
			total += v
			if i < len(h.Timestamps) {
				lastTS = parseTimestampToUnix(h.Timestamps[i])
			}
		}
		result = append(result, map[string]interface{}{
			"metric": p.translateVolumeMetric(h.Fields),
			"value":  []interface{}{lastTS, strconv.Itoa(total)},
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

func (p *Proxy) hitsToVolumeMatrix(body []byte) map[string]interface{} {
	hits := parseHits(body)
	result := make([]map[string]interface{}, 0, len(hits.Hits))
	for _, h := range hits.Hits {
		values := make([][]interface{}, 0, len(h.Timestamps))
		for i, ts := range h.Timestamps {
			val := 0
			if i < len(h.Values) {
				val = h.Values[i]
			}
			values = append(values, []interface{}{parseTimestampToUnix(ts), strconv.Itoa(val)})
		}
		result = append(result, map[string]interface{}{
			"metric": p.translateVolumeMetric(h.Fields),
			"values": values,
		})
	}
	return map[string]interface{}{
		"status": "success",
		"data": map[string]interface{}{
			"resultType": "matrix",
			"result":     result,
		},
	}
}

// isStatsQuery returns true if the LogsQL query contains a stats pipe.
// It only matches top-level pipes, not strings inside quoted filter values
// (e.g., ~"stats query" must NOT trigger this).
func isStatsQuery(logsqlQuery string) bool {
	// Walk the query, skipping quoted regions
	inQuote := false
	for i := 0; i < len(logsqlQuery); i++ {
		if logsqlQuery[i] == '"' {
			inQuote = !inQuote
			continue
		}
		if inQuote {
			continue
		}
		rest := logsqlQuery[i:]
		if strings.HasPrefix(rest, "| stats ") ||
			strings.HasPrefix(rest, "| rate(") ||
			strings.HasPrefix(rest, "| count(") {
			return true
		}
	}
	return false
}

func formatVLTimestamp(ts string) string {
	// Loki sends Unix timestamps (seconds or nanoseconds).
	// Grafana drilldown resource endpoints send RFC3339 timestamps, while
	// query endpoints usually send numeric Unix values. Normalize RFC3339 to
	// Unix nanoseconds so every VL endpoint sees the same time format.
	if _, err := strconv.ParseFloat(ts, 64); err == nil {
		// Already numeric — preserve caller precision.
		return ts
	}
	if parsed, err := time.Parse(time.RFC3339Nano, ts); err == nil {
		return strconv.FormatInt(parsed.UnixNano(), 10)
	}
	if parsed, err := time.Parse(time.RFC3339, ts); err == nil {
		return strconv.FormatInt(parsed.UnixNano(), 10)
	}
	return ts
}

// formatVLStep converts Loki's step parameter to VL duration format.
// Loki/Prometheus sends step as seconds (numeric string like "60") or duration ("1m").
// VL requires duration strings (e.g., "60s", "1m", "1h").
func formatVLStep(step string) string {
	step = strings.TrimSpace(step)
	if step == "" {
		return step
	}
	// If it's already a duration string (contains letter), pass through
	for _, ch := range step {
		if (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') {
			return step
		}
	}
	// Numeric-only: treat as seconds, append "s"
	if _, err := strconv.ParseFloat(step, 64); err == nil {
		return step + "s"
	}
	return step
}

func (p *Proxy) handleMultiTenantFanout(w http.ResponseWriter, r *http.Request, endpoint string, single func(http.ResponseWriter, *http.Request)) bool {
	orgID := strings.TrimSpace(r.Header.Get("X-Scope-OrgID"))
	if !hasMultiTenantOrgID(orgID) {
		return false
	}
	tenantIDs := splitMultiTenantOrgIDs(orgID)
	if len(tenantIDs) < 2 {
		return false
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
		return "mt:" + endpoint + ":" + r.Header.Get("X-Scope-OrgID") + ":" + r.URL.RawQuery, true
	}
	return "", false
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
			var resp struct {
				Data []string `json:"data"`
			}
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
			var resp struct {
				Data []string `json:"data"`
			}
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
		var resp struct {
			Status string                   `json:"status"`
			Data   []map[string]interface{} `json:"data"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
			return nil, "", err
		}
		for _, item := range resp.Data {
			labels := injectTenantLabel(interfaceStringMap(item), tenantIDs[i])
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
		stats      interface{} = map[string]interface{}{}
		merged     []interface{}
	)
	for i, rec := range recorders {
		var resp map[string]interface{}
		if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
			return nil, "", err
		}
		data, _ := resp["data"].(map[string]interface{})
		if rt, _ := data["resultType"].(string); rt != "" {
			if resultType == "" {
				resultType = rt
			}
		}
		if st, ok := data["stats"]; ok {
			stats = st
		}
		items, _ := data["result"].([]interface{})
		for _, item := range items {
			obj, _ := item.(map[string]interface{})
			switch resultType {
			case "streams":
				obj["stream"] = injectTenantLabel(interfaceStringMap(obj["stream"]), tenantIDs[i])
			case "vector", "matrix":
				obj["metric"] = injectTenantLabel(interfaceStringMap(obj["metric"]), tenantIDs[i])
			}
			merged = append(merged, obj)
		}
	}
	if resultType == "streams" {
		sort.SliceStable(merged, func(i, j int) bool {
			left, _ := merged[i].(map[string]interface{})
			right, _ := merged[j].(map[string]interface{})
			leftValues, _ := left["values"].([]interface{})
			rightValues, _ := right["values"].([]interface{})
			return latestStreamTimestamp(leftValues) > latestStreamTimestamp(rightValues)
		})
	}
	body, err := json.Marshal(map[string]interface{}{
		"status": "success",
		"data": map[string]interface{}{
			"resultType": resultType,
			"result":     merged,
			"stats":      stats,
		},
	})
	return body, "application/json", err
}

func latestStreamTimestamp(values []interface{}) string {
	if len(values) == 0 {
		return ""
	}
	pair, _ := values[0].([]interface{})
	if len(pair) == 0 {
		return ""
	}
	return fmt.Sprintf("%v", pair[0])
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
		var resp map[string]interface{}
		if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
			return nil, "", err
		}
		items, _ := resp["fields"].([]interface{})
		for _, item := range items {
			obj, _ := item.(map[string]interface{})
			label, _ := obj["label"].(string)
			if label == "" {
				continue
			}
			mf := merged[label]
			if mf == nil {
				mf = &mergedField{Label: label, Type: fmt.Sprintf("%v", obj["type"]), Parsers: map[string]struct{}{}}
				if jp, ok := obj["jsonPath"].([]interface{}); ok {
					mf.JSONPath = jp
				}
				merged[label] = mf
			}
			if card, ok := obj["cardinality"].(float64); ok {
				mf.Cardinality += int(card)
			}
			if parsers, ok := obj["parsers"].([]interface{}); ok {
				for _, parser := range parsers {
					mf.Parsers[fmt.Sprintf("%v", parser)] = struct{}{}
				}
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
		item := map[string]interface{}{
			"label":       mf.Label,
			"type":        mf.Type,
			"cardinality": mf.Cardinality,
			"parsers":     parsers,
		}
		if len(mf.JSONPath) > 0 {
			item["jsonPath"] = mf.JSONPath
		}
		out = append(out, item)
	}
	body, err := json.Marshal(map[string]interface{}{"status": "success", "data": out, "fields": out})
	return body, "application/json", err
}

func (p *Proxy) multiTenantDetectedFieldsResponse(r *http.Request, tenantIDs []string) ([]byte, string, error) {
	lineLimit := parseDetectedLineLimit(r)
	type mergedField struct {
		Label    string
		Type     string
		Parsers  map[string]struct{}
		JSONPath []interface{}
		Values   map[string]struct{}
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
		item := map[string]interface{}{
			"label":       mf.Label,
			"type":        mf.Type,
			"cardinality": len(mf.Values),
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
		var resp struct {
			Values []string `json:"values"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
			return nil, "", err
		}
		values = append(values, resp.Values...)
	}
	values = uniqueSortedStrings(values)
	body, err := json.Marshal(map[string]interface{}{"status": "success", "data": values, "values": values})
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
		var resp map[string]interface{}
		if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
			return nil, "", err
		}
		items, _ := resp["detectedLabels"].([]interface{})
		for _, item := range items {
			obj, _ := item.(map[string]interface{})
			label, _ := obj["label"].(string)
			if label == "" {
				continue
			}
			if card, ok := obj["cardinality"].(float64); ok {
				cardinality[label] += int(card)
			}
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
	out := make([]map[string]interface{}, 0, len(labels))
	for _, label := range labels {
		out = append(out, map[string]interface{}{"label": label, "cardinality": cardinality[label]})
	}
	body, err := json.Marshal(map[string]interface{}{"status": "success", "data": out, "detectedLabels": out})
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
		var resp map[string]interface{}
		if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
			return nil, "", err
		}
		items, _ := resp["data"].([]interface{})
		for _, item := range items {
			obj, _ := item.(map[string]interface{})
			pattern, _ := obj["pattern"].(string)
			level, _ := obj["level"].(string)
			key := level + "\x00" + pattern
			b := merged[key]
			if b == nil {
				b = &bucket{level: level, pattern: pattern, samples: map[int64]int{}}
				merged[key] = b
			}
			samples, _ := obj["samples"].([]interface{})
			for _, sample := range samples {
				pair, _ := sample.([]interface{})
				if len(pair) < 2 {
					continue
				}
				ts, _ := pair[0].(float64)
				count, _ := pair[1].(float64)
				b.samples[int64(ts)] += int(count)
				b.total += int(count)
			}
		}
	}
	items := make([]*bucket, 0, len(merged))
	for _, item := range merged {
		items = append(items, item)
	}
	sort.Slice(items, func(i, j int) bool { return items[i].total > items[j].total })
	out := make([]map[string]interface{}, 0, len(items))
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
		respItem := map[string]interface{}{"pattern": item.pattern, "samples": samples}
		if item.level != "" {
			respItem["level"] = item.level
		}
		out = append(out, respItem)
	}
	body, err := json.Marshal(map[string]interface{}{"status": "success", "data": out})
	return body, "application/json", err
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

// --- Error / JSON helpers ---

// validateQuery checks query string length and returns a sanitized version.
func (p *Proxy) validateQuery(w http.ResponseWriter, query string, endpoint string) (string, bool) {
	if len(query) > maxQueryLength {
		p.writeError(w, http.StatusBadRequest, fmt.Sprintf("query exceeds max length (%d > %d)", len(query), maxQueryLength))
		p.metrics.RecordRequest(endpoint, http.StatusBadRequest, 0)
		return "", false
	}
	return query, true
}

// sanitizeLimit caps and validates the limit parameter.
func sanitizeLimit(limitStr string) string {
	if limitStr == "" {
		return "1000"
	}
	n, err := strconv.Atoi(limitStr)
	if err != nil || n <= 0 {
		return "1000"
	}
	if n > maxLimitValue {
		return strconv.Itoa(maxLimitValue)
	}
	return limitStr
}

func (p *Proxy) writeError(w http.ResponseWriter, code int, msg string) {
	p.log.Error("request error", "code", code, "error", msg)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":    "error",
		"errorType": lokiErrorType(code),
		"error":     msg,
	})
}

// lokiErrorType returns the Loki/Prometheus-style errorType for an HTTP status code.
// Matches the exact errorType strings from Loki's Prometheus API handler:
// vendor/github.com/prometheus/prometheus/web/api/v1/api.go
func lokiErrorType(code int) string {
	switch code {
	case 400:
		return "bad_data"
	case 404:
		return "not_found"
	case 406:
		return "not_acceptable"
	case 422:
		return "execution"
	case 499:
		return "canceled"
	case 500:
		return "internal"
	case 502:
		return "unavailable"
	case 503:
		return "timeout" // Loki maps 503 to ErrQueryTimeout
	case 504:
		return "timeout"
	default:
		if code >= 400 && code < 500 {
			return "bad_data"
		}
		return "internal"
	}
}

func (p *Proxy) writeJSON(w http.ResponseWriter, data interface{}) {
	marshalJSON(w, data)
}

func base64Encode(s string) string {
	return base64.StdEncoding.EncodeToString([]byte(s))
}

func copyHeaders(dst, src http.Header) {
	for key, values := range src {
		dst.Del(key)
		for _, value := range values {
			dst.Add(key, value)
		}
	}
}

// isVLInternalField returns true for VictoriaLogs internal field names
// that should not be exposed in Loki-compatible label responses.
func isVLInternalField(name string) bool {
	return name == "_time" || name == "_msg" || name == "_stream" || name == "_stream_id"
}

// applyBackendHeaders adds static backend headers and forwarded client headers to a VL request.
func (p *Proxy) applyBackendHeaders(vlReq *http.Request) {
	for k, v := range p.backendHeaders {
		vlReq.Header.Set(k, v)
	}
	if origReq, ok := vlReq.Context().Value(origRequestKey).(*http.Request); ok && origReq != nil {
		clientID, clientSource := metrics.ResolveClientContext(origReq, p.metricsTrustProxyHeaders)
		vlReq.Header.Set("X-Loki-VL-Client-ID", clientID)
		vlReq.Header.Set("X-Loki-VL-Client-Source", clientSource)
		if p.metricsTrustProxyHeaders {
			if grafanaUser := strings.TrimSpace(origReq.Header.Get("X-Grafana-User")); grafanaUser != "" {
				vlReq.Header.Set("X-Grafana-User", grafanaUser)
			}
		}
		// Forward configured client headers from the original request
		if len(p.forwardHeaders) > 0 {
			for _, hdr := range p.forwardHeaders {
				if val := origReq.Header.Get(hdr); val != "" {
					vlReq.Header.Set(hdr, val)
				}
			}
		}
		for _, cookie := range origReq.Cookies() {
			if p.forwardCookies["*"] || p.forwardCookies[cookie.Name] {
				vlReq.AddCookie(cookie)
			}
		}
	}
}

// statusCapture wraps ResponseWriter to capture the status code and bytes written.
type statusCapture struct {
	http.ResponseWriter
	code         int
	bytesWritten int
}

func (sc *statusCapture) WriteHeader(code int) {
	sc.code = code
	sc.ResponseWriter.WriteHeader(code)
}

func (sc *statusCapture) Write(b []byte) (int, error) {
	n, err := sc.ResponseWriter.Write(b)
	sc.bytesWritten += n
	return n, err
}

// Flush implements http.Flusher for chunked streaming support.
func (sc *statusCapture) Flush() {
	if f, ok := sc.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// Hijack implements http.Hijacker for WebSocket upgrade support.
func (sc *statusCapture) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	if h, ok := sc.ResponseWriter.(http.Hijacker); ok {
		return h.Hijack()
	}
	return nil, nil, fmt.Errorf("hijack not supported")
}

// requestLogger wraps a handler with structured logging and per-tenant metrics.
func (p *Proxy) requestLogger(endpoint string, next http.HandlerFunc) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		tenant := r.Header.Get("X-Scope-OrgID")
		query := r.FormValue("query")
		clientID, clientSource := metrics.ResolveClientContext(r, p.metricsTrustProxyHeaders)
		p.metrics.RecordClientInflight(clientID, 1)
		defer p.metrics.RecordClientInflight(clientID, -1)

		sc := &statusCapture{ResponseWriter: w, code: 200}
		next.ServeHTTP(sc, r)

		elapsed := time.Since(start)

		// Per-tenant metrics
		p.metrics.RecordTenantRequest(tenant, endpoint, sc.code, elapsed)

		// Per-client identity metrics (Grafana user > tenant > IP)
		p.metrics.RecordClientIdentity(clientID, endpoint, elapsed, int64(sc.bytesWritten))
		p.metrics.RecordClientStatus(clientID, endpoint, sc.code)
		p.metrics.RecordClientQueryLength(clientID, endpoint, len(query))

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
			p.metrics.RecordClientError(endpoint, reason)
		}

		// Structured request log — includes tenant, query, status, latency, cache info
		logLevel := p.log.Info
		if sc.code >= 500 {
			logLevel = p.log.Error
		} else if sc.code >= 400 {
			logLevel = p.log.Warn
		}
		logLevel("request",
			"http.route", endpoint,
			"http.request.method", r.Method,
			"http.response.status_code", sc.code,
			"event.duration_ms", elapsed.Milliseconds(),
			"loki.tenant.id", tenant,
			"loki.query", truncateQuery(query, 200),
			"client.address", r.RemoteAddr,
			"enduser.id", clientID,
			"loki.client.source", clientSource,
		)
	})
}

func truncateQuery(q string, maxLen int) string {
	if len(q) <= maxLen {
		return q
	}
	return q[:maxLen] + "..."
}

// translateQuery translates a LogQL query to LogsQL, applying label name translation.
func (p *Proxy) translateQuery(logql string) (string, error) {
	normalized := strings.TrimSpace(logql)
	switch normalized {
	case "", "*", `"*"`, "`*`":
		return "*", nil
	}

	labelFn := p.labelTranslator.ToVL
	var (
		translated string
		err        error
	)
	if p.streamFieldsMap != nil {
		translated, err = translator.TranslateLogQLWithStreamFields(logql, labelFn, p.streamFieldsMap)
	} else {
		translated, err = translator.TranslateLogQLWithLabels(logql, labelFn)
	}
	if err != nil {
		return "", err
	}
	trimmed := strings.TrimSpace(translated)
	if strings.HasPrefix(trimmed, "|") {
		return "* " + trimmed, nil
	}
	return translated, nil
}

var (
	jsonParserStageRE   = regexp.MustCompile(`\|\s*json(?:\s+[^|]+)?`)
	logfmtParserStageRE = regexp.MustCompile(`\|\s*logfmt(?:\s+[^|]+)?`)
)

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
	if !hasParserStage(logql, "json") || !hasParserStage(logql, "logfmt") {
		return logql
	}

	baseQuery := extractParserProbeQuery(logql)
	if baseQuery == "" {
		baseQuery = logql
	}
	baseQuery = defaultFieldDetectionQuery(baseQuery)
	logsqlQuery, err := p.translateQuery(baseQuery)
	if err != nil {
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
		return logql
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil || len(body) == 0 {
		return logql
	}

	jsonHits := 0
	logfmtHits := 0
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
		msg, _ := entry["_msg"].(string)
		if msg == "" {
			continue
		}

		var parsedJSON map[string]interface{}
		if json.Unmarshal([]byte(msg), &parsedJSON) == nil && len(parsedJSON) > 0 {
			jsonHits++
		}
		if fields := parseLogfmtFields(msg); len(fields) > 0 {
			logfmtHits++
		}
	}

	switch {
	case jsonHits == 0 && logfmtHits == 0:
		return logql
	case jsonHits >= logfmtHits:
		return removeParserStage(logql, "logfmt")
	default:
		return removeParserStage(logql, "json")
	}
}

var metricParserProbeRE = regexp.MustCompile(`(?s)(?:count_over_time|bytes_over_time|rate|bytes_rate|sum_over_time|avg_over_time|max_over_time|min_over_time|first_over_time|last_over_time|stddev_over_time|stdvar_over_time|quantile_over_time)\((.*?)\[[^][]+\]\)`)

func extractParserProbeQuery(logql string) string {
	matches := metricParserProbeRE.FindStringSubmatch(logql)
	if len(matches) == 2 {
		return strings.TrimSpace(matches[1])
	}
	return strings.TrimSpace(logql)
}

// translateStatsResponseLabels translates label names in VL stats responses
// (both vector and matrix result types) from VL field names (dots) to
// Loki-compatible label names (underscores).
func (p *Proxy) translateStatsResponseLabels(body []byte, originalQuery string) []byte {
	var resp map[string]interface{}
	if err := json.Unmarshal(body, &resp); err != nil {
		return body
	}

	// Handle both direct results and nested data.result
	var results []interface{}
	if data, ok := resp["data"].(map[string]interface{}); ok {
		if r, ok := data["result"].([]interface{}); ok {
			results = r
		}
	}
	if r, ok := resp["result"].([]interface{}); ok {
		results = r
	}
	if r, ok := resp["results"].([]interface{}); ok {
		results = r
	}

	if len(results) == 0 {
		return body
	}

	for _, r := range results {
		entry, ok := r.(map[string]interface{})
		if !ok {
			continue
		}
		// Translate "metric" labels map
		if metricRaw, ok := entry["metric"]; ok {
			if metric, ok := metricRaw.(map[string]interface{}); ok {
				translated := make(map[string]interface{}, len(metric))
				for k, v := range metric {
					if k == "__name__" {
						continue
					}
					lokiKey := k
					if !p.labelTranslator.IsPassthrough() {
						lokiKey = p.labelTranslator.ToLoki(k)
					}
					translated[lokiKey] = v
				}
				if strings.Contains(originalQuery, "detected_level") {
					if _, ok := translated["detected_level"]; !ok {
						if value, ok := translated["level"]; ok {
							translated["detected_level"] = value
							delete(translated, "level")
						}
					}
				}
				entry["metric"] = translated
			}
		}
	}

	result, err := json.Marshal(resp)
	if err != nil {
		return body
	}
	return result
}
