package proxy

import (
	"bufio"
	"context"
	"crypto/tls"
	"sync"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"regexp"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/websocket"
	"github.com/szibis/Loki-VL-proxy/internal/cache"
	"github.com/szibis/Loki-VL-proxy/internal/metrics"
	mw "github.com/szibis/Loki-VL-proxy/internal/middleware"
	"github.com/szibis/Loki-VL-proxy/internal/translator"
)

type ctxKey int

const orgIDKey ctxKey = iota

// withOrgID stores the X-Scope-OrgID in the request context (request-scoped, no shared state).
func withOrgID(r *http.Request) *http.Request {
	orgID := r.Header.Get("X-Scope-OrgID")
	if orgID == "" {
		return r
	}
	return r.WithContext(context.WithValue(r.Context(), orgIDKey, orgID))
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

type Config struct {
	BackendURL       string
	Cache            *cache.Cache
	LogLevel         string
	MaxConcurrent    int     // max concurrent backend queries (0=unlimited)
	RatePerSecond    float64 // per-client rate limit (0=unlimited)
	RateBurst        int     // per-client burst size
	CBFailThreshold  int     // circuit breaker failure threshold
	CBOpenDuration   time.Duration // circuit breaker open duration
	TenantMap        map[string]TenantMapping // string org ID → VL account/project

	// Grafana datasource compatibility
	MaxLines         int               // default max lines per query (0=1000)
	ForwardHeaders   []string          // HTTP headers to forward from client to VL backend
	BackendHeaders   map[string]string // static headers to add to all VL requests
	BackendBasicAuth string            // "user:password" for VL backend basic auth
	BackendTLSSkip   bool              // skip TLS verification for VL backend
	DerivedFields    []DerivedField    // derived fields for trace/link extraction
	StreamResponse   bool              // stream responses via chunked transfer (default: false)

	// Label translation
	LabelStyle    LabelStyle     // how to translate VL field names to Loki labels
	FieldMappings []FieldMapping // custom VL↔Loki field name mappings
}

// DerivedField extracts a value from log lines and creates a link (e.g., to a trace backend).
// Matches Grafana Loki datasource "Derived fields" config.
type DerivedField struct {
	Name          string `json:"name" yaml:"name"`                       // field name (e.g., "traceID")
	MatcherRegex  string `json:"matcherRegex" yaml:"matcherRegex"`       // regex to extract value from log line
	URL           string `json:"url" yaml:"url"`                         // link template (e.g., "http://tempo:3200/trace/${__value.raw}")
	URLDisplayLabel string `json:"urlDisplayLabel" yaml:"urlDisplayLabel"` // display text for the link
	DatasourceUID string `json:"datasourceUid" yaml:"datasourceUid"`     // Grafana datasource UID for internal link
}

const (
	// maxQueryLength limits the LogQL query string length to prevent abuse.
	maxQueryLength = 65536 // 64KB
	// maxLimitValue caps the number of results per query.
	maxLimitValue = 10000
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
	backend        *url.URL
	client         *http.Client
	cache          *cache.Cache
	log            *slog.Logger
	metrics        *metrics.Metrics
	queryTracker   *metrics.QueryTracker
	coalescer      *mw.Coalescer
	limiter        *mw.RateLimiter
	breaker        *mw.CircuitBreaker
	configMu       sync.RWMutex          // protects tenantMap and labelTranslator
	tenantMap      map[string]TenantMapping
	maxLines       int
	forwardHeaders []string          // headers to copy from client request to VL
	backendHeaders map[string]string // static headers on all VL requests
	derivedFields    []DerivedField
	streamResponse   bool
	labelTranslator  *LabelTranslator
}

func New(cfg Config) (*Proxy, error) {
	u, err := url.Parse(cfg.BackendURL)
	if err != nil {
		return nil, fmt.Errorf("invalid backend URL: %w", err)
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

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level}))

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

	// Build HTTP client with optional TLS skip for VL backend
	transport := http.DefaultTransport.(*http.Transport).Clone()
	if cfg.BackendTLSSkip {
		transport.TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
	}

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

	return &Proxy{
		backend: u,
		client: &http.Client{
			Timeout:   120 * time.Second,
			Transport: transport,
		},
		cache:          cfg.Cache,
		log:            logger,
		metrics:        metrics.NewMetrics(),
		queryTracker:   metrics.NewQueryTracker(10000),
		coalescer:      mw.NewCoalescer(),
		limiter:        mw.NewRateLimiter(maxConcurrent, ratePerSec, rateBurst),
		breaker:        mw.NewCircuitBreaker(cbFail, 3, cbOpen),
		tenantMap:      cfg.TenantMap,
		maxLines:       maxLines,
		forwardHeaders: cfg.ForwardHeaders,
		backendHeaders: backendHeaders,
		derivedFields:    cfg.DerivedFields,
		streamResponse:   cfg.StreamResponse,
		labelTranslator:  NewLabelTranslator(cfg.LabelStyle, cfg.FieldMappings),
	}, nil
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

func (p *Proxy) RegisterRoutes(mux *http.ServeMux) {
	// Rate-limited endpoints with security headers + request logging
	rl := func(endpoint string, h http.HandlerFunc) http.Handler {
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

	// Write endpoints — blocked (this is a read-only proxy)
	mux.HandleFunc("/loki/api/v1/push", p.handleWriteBlocked)

	// Health / readiness — NOT rate-limited
	mux.HandleFunc("/ready", p.handleReady)
	mux.HandleFunc("/loki/api/v1/status/buildinfo", p.handleBuildInfo)

	// Prometheus metrics endpoint — NOT rate-limited
	mux.HandleFunc("/metrics", p.metrics.Handler)
	mux.HandleFunc("/debug/queries", p.queryTracker.Handler)
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
	p.log.Debug("query_range request", "logql", logqlQuery)

	logsqlQuery, err := p.translateQuery(logqlQuery)
	if err != nil {
		p.writeError(w, http.StatusBadRequest, err.Error())
		p.metrics.RecordRequest("query_range", http.StatusBadRequest, time.Since(start))
		return
	}
	p.log.Debug("translated query", "logsql", logsqlQuery)

	r = withOrgID(r)

	// Check for binary metric expression (e.g., sum(rate(...)) / sum(rate(...)))
	if op, left, right, ok := translator.ParseBinaryMetricExpr(logsqlQuery); ok {
		p.proxyBinaryMetricQueryRange(w, r, op, left, right)
	} else if isStatsQuery(logsqlQuery) {
		p.proxyStatsQueryRange(w, r, logsqlQuery)
	} else {
		p.proxyLogQuery(w, r, logsqlQuery)
	}
	elapsed := time.Since(start)
	p.metrics.RecordRequest("query_range", http.StatusOK, elapsed)
	p.queryTracker.Record("query_range", logqlQuery, elapsed, false)
}

// handleQuery translates Loki instant queries.
func (p *Proxy) handleQuery(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	logqlQuery := r.FormValue("query")
	if _, ok := p.validateQuery(w, logqlQuery, "query"); !ok {
		return
	}
	p.log.Debug("query request", "logql", logqlQuery)

	logsqlQuery, err := p.translateQuery(logqlQuery)
	if err != nil {
		p.writeError(w, http.StatusBadRequest, err.Error())
		p.metrics.RecordRequest("query", http.StatusBadRequest, time.Since(start))
		return
	}

	r = withOrgID(r)
	if op, left, right, ok := translator.ParseBinaryMetricExpr(logsqlQuery); ok {
		p.proxyBinaryMetricQuery(w, r, op, left, right)
	} else if isStatsQuery(logsqlQuery) {
		p.proxyStatsQuery(w, r, logsqlQuery)
	} else {
		p.proxyLogQuery(w, r, logsqlQuery)
	}
	elapsed := time.Since(start)
	p.metrics.RecordRequest("query", http.StatusOK, elapsed)
	p.queryTracker.Record("query", logqlQuery, elapsed, false)
}

// handleLabels returns label names.
// Loki: GET /loki/api/v1/labels?start=...&end=...
// VL:   GET /select/logsql/field_names?query=*&start=...&end=...
func (p *Proxy) handleLabels(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	r = withOrgID(r)
	cacheKey := "labels:" + r.URL.RawQuery

	if cached, ok := p.cache.Get(cacheKey); ok {
		p.log.Debug("labels cache hit")
		w.Header().Set("Content-Type", "application/json")
		w.Write(cached)
		p.metrics.RecordRequest("labels", http.StatusOK, time.Since(start))
		p.metrics.RecordCacheHit()
		return
	}
	p.metrics.RecordCacheMiss()

	params := url.Values{}
	params.Set("query", "*")
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
	// Translate Loki label name to VL field name for the backend query
	vlFieldName := p.labelTranslator.ToVL(labelName)

	cacheKey := "label_values:" + labelName + ":" + r.URL.RawQuery
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
	params.Set("query", "*")
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
		return
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)

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
	query := r.FormValue("query")
	if query == "" {
		query = "*"
	}
	logsqlQuery, _ := p.translateQuery(query)

	params := url.Values{}
	params.Set("query", logsqlQuery)
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
	query := r.FormValue("query")
	if query == "" {
		query = "*"
	}
	logsqlQuery, _ := p.translateQuery(query)

	params := url.Values{}
	params.Set("query", logsqlQuery)
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
	if fields := r.FormValue("targetLabels"); fields != "" {
		params.Set("field", fields)
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

	result := hitsToVolumeVector(body)
	p.writeJSON(w, result)
	p.metrics.RecordRequest("volume", http.StatusOK, time.Since(start))
}

// handleVolumeRange returns volume range data via VL /select/logsql/hits with step.
// Loki: GET /loki/api/v1/index/volume_range?query={...}&start=...&end=...&step=60
// Response: {"status":"success","data":{"resultType":"matrix","result":[{"metric":{...},"values":[[ts,"count"],...]}]}}
func (p *Proxy) handleVolumeRange(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	query := r.FormValue("query")
	if query == "" {
		query = "*"
	}
	logsqlQuery, _ := p.translateQuery(query)

	params := url.Values{}
	params.Set("query", logsqlQuery)
	if s := r.FormValue("start"); s != "" {
		params.Set("start", formatVLTimestamp(s))
	}
	if e := r.FormValue("end"); e != "" {
		params.Set("end", formatVLTimestamp(e))
	}
	if step := r.FormValue("step"); step != "" {
		params.Set("step", step)
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

	result := hitsToVolumeMatrix(body)
	p.writeJSON(w, result)
	p.metrics.RecordRequest("volume_range", http.StatusOK, time.Since(start))
}

// handleDetectedFields returns detected field names.
func (p *Proxy) handleDetectedFields(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	query := r.FormValue("query")
	if query == "" {
		query = "*"
	}

	logsqlQuery, _ := p.translateQuery(query)

	params := url.Values{}
	params.Set("query", logsqlQuery)
	if s := r.FormValue("start"); s != "" {
		params.Set("start", s)
	}
	if e := r.FormValue("end"); e != "" {
		params.Set("end", e)
	}

	resp, err := p.vlGet(r.Context(), "/select/logsql/field_names", params)
	if err != nil || resp.StatusCode >= 400 {
		// VL field_names may not support complex queries — fallback to wildcard
		if resp != nil {
			resp.Body.Close()
		}
		fallbackParams := url.Values{"query": {"*"}}
		if s := params.Get("start"); s != "" {
			fallbackParams.Set("start", s)
		}
		if e := params.Get("end"); e != "" {
			fallbackParams.Set("end", e)
		}
		resp, err = p.vlGet(r.Context(), "/select/logsql/field_names", fallbackParams)
		if err != nil {
			p.writeJSON(w, map[string]interface{}{"status": "success", "fields": []interface{}{}})
			p.metrics.RecordRequest("detected_fields", http.StatusOK, time.Since(start))
			return
		}
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)

	var vlResp struct {
		Values []struct {
			Value string `json:"value"`
			Hits  int64  `json:"hits"`
		} `json:"values"`
	}
	json.Unmarshal(body, &vlResp)

	fields := make([]map[string]interface{}, 0, len(vlResp.Values))
	seen := make(map[string]bool)
	for _, v := range vlResp.Values {
		if isVLInternalField(v.Value) {
			continue
		}
		translated := p.labelTranslator.ToLoki(v.Value)
		if seen[translated] {
			continue
		}
		seen[translated] = true
		fields = append(fields, map[string]interface{}{
			"label":       translated,
			"type":        "string",
			"cardinality": v.Hits,
		})
	}

	result, _ := json.Marshal(map[string]interface{}{
		"status": "success",
		"fields": fields,
	})
	w.Header().Set("Content-Type", "application/json")
	w.Write(result)
	p.metrics.RecordRequest("detected_fields", http.StatusOK, time.Since(start))
}

// handleDetectedFieldValues returns values for a detected field.
// Loki: GET /loki/api/v1/detected_field/{name}/values?query=...
// Response: {"values":["debug","info","warn","error"],"limit":1000}
func (p *Proxy) handleDetectedFieldValues(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
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

	query := r.FormValue("query")
	if query == "" {
		query = "*"
	}
	logsqlQuery, _ := p.translateQuery(query)

	params := url.Values{}
	// Translate Loki field name to VL field name for the backend query
	vlFieldName := p.labelTranslator.ToVL(fieldName)
	params.Set("query", logsqlQuery)
	params.Set("field", vlFieldName)
	if s := r.FormValue("start"); s != "" {
		params.Set("start", s)
	}
	if e := r.FormValue("end"); e != "" {
		params.Set("end", e)
	}

	resp, err := p.vlGet(r.Context(), "/select/logsql/field_values", params)
	if err != nil {
		p.writeJSON(w, map[string]interface{}{"values": []string{}, "limit": 1000})
		p.metrics.RecordRequest("detected_field_values", http.StatusOK, time.Since(start))
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
	json.Unmarshal(body, &vlResp)

	values := make([]string, 0, len(vlResp.Values))
	for _, v := range vlResp.Values {
		values = append(values, v.Value)
	}

	p.writeJSON(w, map[string]interface{}{
		"values": values,
		"limit":  1000,
	})
	p.metrics.RecordRequest("detected_field_values", http.StatusOK, time.Since(start))
}

// handlePatterns returns log patterns (stub).
func (p *Proxy) handlePatterns(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	query := r.FormValue("query")
	if query == "" {
		query = `{}`
	}

	logsqlQuery, err := p.translateQuery(query)
	if err != nil {
		p.writeJSON(w, map[string]interface{}{"status": "success", "data": []interface{}{}})
		p.metrics.RecordRequest("patterns", http.StatusOK, time.Since(start))
		return
	}

	params := url.Values{"query": {logsqlQuery}, "limit": {"1000"}}
	if s := r.FormValue("start"); s != "" {
		params.Set("start", formatVLTimestamp(s))
	}
	if e := r.FormValue("end"); e != "" {
		params.Set("end", formatVLTimestamp(e))
	}

	resp, err := p.vlGet(r.Context(), "/select/logsql/query", params)
	if err != nil {
		p.writeJSON(w, map[string]interface{}{"status": "success", "data": []interface{}{}})
		p.metrics.RecordRequest("patterns", http.StatusOK, time.Since(start))
		return
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)

	patterns := extractLogPatterns(body)

	p.writeJSON(w, map[string]interface{}{
		"status": "success",
		"data":   patterns,
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

// handleDetectedLabels returns stream-level labels (similar to detected_fields but for stream labels).
func (p *Proxy) handleDetectedLabels(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	query := r.FormValue("query")
	if query == "" {
		query = "*"
	}

	logsqlQuery, _ := p.translateQuery(query)

	params := url.Values{}
	params.Set("query", logsqlQuery)
	if s := r.FormValue("start"); s != "" {
		params.Set("start", s)
	}
	if e := r.FormValue("end"); e != "" {
		params.Set("end", e)
	}

	body, err := p.vlGetCoalesced(r.Context(), "detected_labels:"+params.Encode(), "/select/logsql/field_names", params)
	if err != nil {
		p.writeJSON(w, map[string]interface{}{"status": "success", "data": []interface{}{}})
		p.metrics.RecordRequest("detected_labels", http.StatusOK, time.Since(start))
		return
	}

	var vlResp struct {
		Values []struct {
			Value string `json:"value"`
			Hits  int64  `json:"hits"`
		} `json:"values"`
	}
	json.Unmarshal(body, &vlResp)

	labels := make([]string, 0, len(vlResp.Values))
	for _, v := range vlResp.Values {
		if isVLInternalField(v.Value) {
			continue
		}
		labels = append(labels, p.labelTranslator.ToLoki(v.Value))
	}

	p.writeJSON(w, map[string]interface{}{
		"status": "success",
		"data":   labels,
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

// wsUpgrader is the WebSocket upgrader for tail connections.
var wsUpgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
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

	r = withOrgID(r)

	// Upgrade to WebSocket
	conn, err := wsUpgrader.Upgrade(w, r, nil)
	if err != nil {
		p.log.Error("websocket upgrade failed", "error", err)
		p.metrics.RecordRequest("tail", http.StatusBadRequest, time.Since(start))
		return
	}
	defer conn.Close()

	// Connect to VL tail endpoint (streaming NDJSON)
	vlURL := fmt.Sprintf("%s/select/logsql/tail?query=%s",
		p.backend.String(), url.QueryEscape(logsqlQuery))
	req, err := http.NewRequestWithContext(r.Context(), "GET", vlURL, nil)
	if err != nil {
		p.sendWSError(conn, "failed to create VL request")
		return
	}
	p.forwardTenantHeaders(req)

	resp, err := p.client.Do(req)
	if err != nil {
		p.sendWSError(conn, "VL tail connection failed: "+err.Error())
		return
	}
	defer resp.Body.Close()

	p.log.Debug("tail connected", "logql", logqlQuery, "logsql", logsqlQuery)
	p.metrics.RecordRequest("tail", http.StatusOK, time.Since(start))

	// Start a read loop to detect client disconnect (WebSocket protocol requires it).
	// When client closes, this goroutine exits and wsCtx is canceled.
	wsCtx, wsCancel := context.WithCancel(r.Context())
	defer wsCancel()
	go func() {
		defer wsCancel()
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}()

	// Read VL NDJSON stream and forward as Loki WebSocket frames
	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 1024*1024), 1024*1024) // 1MB max line

	for scanner.Scan() {
		// Check if client disconnected
		select {
		case <-wsCtx.Done():
			return
		default:
		}
		line := scanner.Bytes()
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

		if err := conn.WriteMessage(websocket.TextMessage, frameJSON); err != nil {
			p.log.Debug("websocket write failed, client disconnected", "error", err)
			return
		}
	}
}

// vlLineToTailFrame converts a single VL NDJSON log line to a Loki tail WebSocket frame.
func (p *Proxy) vlLineToTailFrame(vlLine map[string]interface{}) map[string]interface{} {
	ts := ""
	msg := ""
	labels := map[string]string{}

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
		default:
			labels[k] = sv
		}
	}
	if ts == "" {
		ts = fmt.Sprintf("%d", time.Now().UnixNano())
	}

	// Apply label name translation
	translatedLabels := p.labelTranslator.TranslateLabelsMap(labels)

	return map[string]interface{}{
		"streams": []map[string]interface{}{
			{
				"stream": translatedLabels,
				"values": [][]string{{ts, msg}},
			},
		},
	}
}

// sendWSError sends an error message over the WebSocket before closing.
func (p *Proxy) sendWSError(conn *websocket.Conn, msg string) {
	errFrame, _ := json.Marshal(map[string]string{"error": msg})
	conn.WriteMessage(websocket.TextMessage, errFrame)
}

// handleReady returns readiness status.
func (p *Proxy) handleReady(w http.ResponseWriter, r *http.Request) {
	// Probe VL backend health
	resp, err := p.client.Get(p.backend.String() + "/health")
	if err != nil || resp.StatusCode != http.StatusOK {
		w.WriteHeader(http.StatusServiceUnavailable)
		w.Write([]byte("backend not ready"))
		return
	}
	resp.Body.Close()

	// Circuit breaker check
	if !p.breaker.Allow() {
		w.WriteHeader(http.StatusServiceUnavailable)
		w.Write([]byte("circuit breaker open"))
		return
	}

	w.Write([]byte("ready"))
}

// handleBuildInfo returns fake build info for Grafana datasource detection.
func (p *Proxy) handleBuildInfo(w http.ResponseWriter, r *http.Request) {
	p.writeJSON(w, map[string]interface{}{
		"status": "success",
		"data": map[string]interface{}{
			"version":  "2.9.0",
			"revision": "loki-vl-proxy",
			"branch":   "main",
			"goVersion": "go1.23",
		},
	})
}

// --- Backend request helpers ---

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
		params.Set("step", step)
	}

	resp, err := p.vlPost(r.Context(), "/select/logsql/stats_query_range", params)
	if err != nil {
		p.writeError(w, http.StatusBadGateway, err.Error())
		return
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)

	// VL stats_query_range returns Prometheus-compatible format.
	// Just wrap it in Loki's envelope.
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
	w.Header().Set("Content-Type", "application/json")
	w.Write(wrapAsLokiResponse(body, "vector"))
}

// proxyBinaryMetricQueryRange evaluates binary metric expressions for query_range.
// Runs both sides against VL independently, then combines with arithmetic.
func (p *Proxy) proxyBinaryMetricQueryRange(w http.ResponseWriter, r *http.Request, op, leftQL, rightQL string) {
	p.proxyBinaryMetric(w, r, op, leftQL, rightQL, "stats_query_range", "matrix")
}

// proxyBinaryMetricQuery evaluates binary metric expressions for instant queries.
func (p *Proxy) proxyBinaryMetricQuery(w http.ResponseWriter, r *http.Request, op, leftQL, rightQL string) {
	p.proxyBinaryMetric(w, r, op, leftQL, rightQL, "stats_query", "vector")
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
				params.Set("step", step)
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

	// Chunked streaming: flush partial results as they arrive from VL
	if p.streamResponse {
		p.streamLogQuery(w, resp)
		return
	}

	body, _ := io.ReadAll(resp.Body)

	// VL returns newline-delimited JSON, each line is a log entry.
	// Loki expects: {"status":"success","data":{"resultType":"streams","result":[...]}}
	streams := vlLogsToLokiStreams(body)

	// Apply label name translation (e.g., dots → underscores)
	p.translateStreamLabels(streams)

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

		timeStr, _ := entry["_time"].(string)
		msg, _ := entry["_msg"].(string)
		if timeStr == "" {
			continue
		}

		ts, err := time.Parse(time.RFC3339Nano, timeStr)
		if err != nil {
			continue
		}

		labels := make(map[string]string)
		for k, v := range entry {
			if k == "_time" || k == "_msg" || k == "_stream" || k == "_stream_id" {
				continue
			}
			if s, ok := v.(string); ok {
				labels[p.labelTranslator.ToLoki(k)] = s
			}
		}

		stream := map[string]interface{}{
			"stream": labels,
			"values": [][]string{{strconv.FormatInt(ts.UnixNano(), 10), msg}},
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
		name  string
		re    *regexp.Regexp
		url   string
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
		values, ok := stream["values"].([][]string)
		if !ok {
			continue
		}
		labels, _ := stream["stream"].(map[string]string)
		if labels == nil {
			continue
		}

		for _, val := range values {
			if len(val) < 2 {
				continue
			}
			line := val[1]
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

// --- Response converters ---

func lokiLabelsResponse(labels []string) []byte {
	result, _ := json.Marshal(map[string]interface{}{
		"status": "success",
		"data":   labels,
	})
	return result
}

// vlLogsToLokiStreams converts VL newline-delimited JSON logs to Loki streams format.
func vlLogsToLokiStreams(body []byte) []map[string]interface{} {
	type streamEntry struct {
		Labels map[string]string
		Values [][]string // [[timestamp_ns, line], ...]
	}
	streamMap := make(map[string]*streamEntry)

	lines := strings.Split(string(body), "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		var entry map[string]interface{}
		if err := json.Unmarshal([]byte(line), &entry); err != nil {
			continue
		}

		// Extract _time, _msg, _stream, and remaining fields as labels
		timeStr, _ := entry["_time"].(string)
		msg, _ := entry["_msg"].(string)
		streamStr, _ := entry["_stream"].(string)

		if timeStr == "" {
			continue
		}

		// Parse time to nanoseconds
		ts, err := time.Parse(time.RFC3339Nano, timeStr)
		if err != nil {
			continue
		}
		tsNanos := strconv.FormatInt(ts.UnixNano(), 10)

		// Use _stream as the stream key, or build from labels
		streamKey := streamStr
		if streamKey == "" {
			streamKey = "{}"
		}

		se, ok := streamMap[streamKey]
		if !ok {
			labels := parseStreamLabels(streamKey)
			// Add non-stream fields as labels too
			for k, v := range entry {
				if k == "_time" || k == "_msg" || k == "_stream" || k == "_stream_id" {
					continue
				}
				if s, ok := v.(string); ok {
					labels[k] = s
				}
			}
			se = &streamEntry{
				Labels: labels,
				Values: make([][]string, 0),
			}
			streamMap[streamKey] = se
		}

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
		Timestamps []string         `json:"timestamps"` // VL v1.49+: RFC3339 strings
		Values     []int            `json:"values"`
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

func hitsToVolumeVector(body []byte) map[string]interface{} {
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
			"metric": h.Fields,
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

func hitsToVolumeMatrix(body []byte) map[string]interface{} {
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
			"metric": h.Fields,
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

func isStatsQuery(logsqlQuery string) bool {
	return strings.Contains(logsqlQuery, "| stats ") ||
		strings.Contains(logsqlQuery, "| rate(") ||
		strings.Contains(logsqlQuery, "| count(")
}

func formatVLTimestamp(ts string) string {
	// Loki sends Unix timestamps (seconds or nanoseconds).
	// VL accepts RFC3339 or relative timestamps.
	// If it's a number, convert; otherwise pass through.
	if _, err := strconv.ParseFloat(ts, 64); err == nil {
		// Already a number — VL accepts Unix timestamps in seconds
		return ts
	}
	return ts
}

// --- Multitenancy ---

// forwardTenantHeaders maps Loki's X-Scope-OrgID to VL's AccountID/ProjectID.
// Reads orgID from the request context (set by withOrgID) — no shared mutable state.
func (p *Proxy) forwardTenantHeaders(req *http.Request) {
	orgID := getOrgID(req.Context())
	if orgID == "" {
		// No tenant header → default VL tenant (0:0), serves all data
		return
	}

	// Wildcard: "*" or "0" → skip tenant headers, let VL serve all data
	if orgID == "*" || orgID == "0" {
		return
	}

	// Check tenant map first for string→int mapping
	if p.tenantMap != nil {
		if mapping, ok := p.tenantMap[orgID]; ok {
			req.Header.Set("AccountID", mapping.AccountID)
			req.Header.Set("ProjectID", mapping.ProjectID)
			return
		}
	}

	// Try numeric passthrough: "42" → AccountID: 42
	if _, err := strconv.Atoi(orgID); err == nil {
		req.Header.Set("AccountID", orgID)
		req.Header.Set("ProjectID", "0")
	} else {
		// Unmapped non-numeric org ID → default tenant
		req.Header.Set("AccountID", "0")
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
		"errorType": "bad_request",
		"error":     msg,
	})
}

func (p *Proxy) writeJSON(w http.ResponseWriter, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(data)
}

func base64Encode(s string) string {
	return base64.StdEncoding.EncodeToString([]byte(s))
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
	// Forward client headers if configured (requires original request in context)
	// Forwarded headers are set by the middleware before vlGet/vlPost
}

// statusCapture wraps ResponseWriter to capture the status code.
type statusCapture struct {
	http.ResponseWriter
	code int
}

func (sc *statusCapture) WriteHeader(code int) {
	sc.code = code
	sc.ResponseWriter.WriteHeader(code)
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

		sc := &statusCapture{ResponseWriter: w, code: 200}
		next.ServeHTTP(sc, r)

		elapsed := time.Since(start)

		// Per-tenant metrics
		p.metrics.RecordTenantRequest(tenant, endpoint, sc.code, elapsed)

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
			"endpoint", endpoint,
			"method", r.Method,
			"status", sc.code,
			"duration_ms", elapsed.Milliseconds(),
			"tenant", tenant,
			"query", truncateQuery(query, 200),
			"client", r.RemoteAddr,
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
	if p.labelTranslator.IsPassthrough() {
		return translator.TranslateLogQL(logql)
	}
	return translator.TranslateLogQLWithLabels(logql, p.labelTranslator.ToVL)
}

// translateStreamLabels applies label name translation to all streams in a Loki-format result.
func (p *Proxy) translateStreamLabels(streams []map[string]interface{}) {
	if p.labelTranslator.IsPassthrough() {
		return
	}
	for _, stream := range streams {
		if labels, ok := stream["stream"].(map[string]string); ok {
			stream["stream"] = p.labelTranslator.TranslateLabelsMap(labels)
		}
	}
}
