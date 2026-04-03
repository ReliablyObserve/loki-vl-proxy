package proxy

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/szibis/Loki-VL-proxy/internal/cache"
	"github.com/szibis/Loki-VL-proxy/internal/metrics"
	mw "github.com/szibis/Loki-VL-proxy/internal/middleware"
	"github.com/szibis/Loki-VL-proxy/internal/translator"
)

type Config struct {
	BackendURL       string
	Cache            *cache.Cache
	LogLevel         string
	MaxConcurrent    int     // max concurrent backend queries (0=unlimited)
	RatePerSecond    float64 // per-client rate limit (0=unlimited)
	RateBurst        int     // per-client burst size
	CBFailThreshold  int     // circuit breaker failure threshold
	CBOpenDuration   time.Duration // circuit breaker open duration
}

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
	backend   *url.URL
	client    *http.Client
	cache     *cache.Cache
	log       *slog.Logger
	metrics   *metrics.Metrics
	coalescer *mw.Coalescer
	limiter   *mw.RateLimiter
	breaker   *mw.CircuitBreaker
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

	return &Proxy{
		backend: u,
		client: &http.Client{
			Timeout: 120 * time.Second,
		},
		cache:     cfg.Cache,
		log:       logger,
		metrics:   metrics.NewMetrics(),
		coalescer: mw.NewCoalescer(),
		limiter:   mw.NewRateLimiter(maxConcurrent, ratePerSec, rateBurst),
		breaker:   mw.NewCircuitBreaker(cbFail, 3, cbOpen),
	}, nil
}

func (p *Proxy) RegisterRoutes(mux *http.ServeMux) {
	// Rate-limited endpoints (data queries)
	rl := func(h http.HandlerFunc) http.Handler {
		return p.limiter.Middleware(http.HandlerFunc(h))
	}

	// Loki API endpoints — data queries are rate-limited
	mux.Handle("/loki/api/v1/query_range", rl(p.handleQueryRange))
	mux.Handle("/loki/api/v1/query", rl(p.handleQuery))
	mux.Handle("/loki/api/v1/series", rl(p.handleSeries))

	// Metadata endpoints — rate-limited but cached
	mux.Handle("/loki/api/v1/labels", rl(p.handleLabels))
	mux.Handle("/loki/api/v1/label/", rl(p.handleLabelValues))
	mux.Handle("/loki/api/v1/detected_fields", rl(p.handleDetectedFields))

	// Lighter endpoints — still rate-limited
	mux.Handle("/loki/api/v1/index/stats", rl(p.handleIndexStats))
	mux.Handle("/loki/api/v1/index/volume", rl(p.handleVolume))
	mux.Handle("/loki/api/v1/index/volume_range", rl(p.handleVolumeRange))
	mux.Handle("/loki/api/v1/patterns", rl(p.handlePatterns))
	mux.Handle("/loki/api/v1/tail", rl(p.handleTail))

	// Health / readiness — NOT rate-limited
	mux.HandleFunc("/ready", p.handleReady)
	mux.HandleFunc("/loki/api/v1/status/buildinfo", p.handleBuildInfo)

	// Prometheus metrics endpoint — NOT rate-limited
	mux.HandleFunc("/metrics", p.metrics.Handler)
}

// handleQueryRange translates Loki range queries.
// Loki: GET /loki/api/v1/query_range?query={...}&start=...&end=...&limit=...&step=...
// VL stats: POST /select/logsql/stats_query_range with query, start, end, step
// VL logs:  POST /select/logsql/query with query, start, end, limit
func (p *Proxy) handleQueryRange(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	logqlQuery := r.FormValue("query")
	p.log.Debug("query_range request", "logql", logqlQuery)

	logsqlQuery, err := translator.TranslateLogQL(logqlQuery)
	if err != nil {
		p.writeError(w, http.StatusBadRequest, err.Error())
		p.metrics.RecordRequest("query_range", http.StatusBadRequest, time.Since(start))
		return
	}
	p.log.Debug("translated query", "logsql", logsqlQuery)

	if isStatsQuery(logsqlQuery) {
		p.proxyStatsQueryRange(w, r, logsqlQuery)
	} else {
		p.proxyLogQuery(w, r, logsqlQuery)
	}
	p.metrics.RecordRequest("query_range", http.StatusOK, time.Since(start))
}

// handleQuery translates Loki instant queries.
func (p *Proxy) handleQuery(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	logqlQuery := r.FormValue("query")
	p.log.Debug("query request", "logql", logqlQuery)

	logsqlQuery, err := translator.TranslateLogQL(logqlQuery)
	if err != nil {
		p.writeError(w, http.StatusBadRequest, err.Error())
		p.metrics.RecordRequest("query", http.StatusBadRequest, time.Since(start))
		return
	}

	if isStatsQuery(logsqlQuery) {
		p.proxyStatsQuery(w, r, logsqlQuery)
	} else {
		p.proxyLogQuery(w, r, logsqlQuery)
	}
	p.metrics.RecordRequest("query", http.StatusOK, time.Since(start))
}

// handleLabels returns label names.
// Loki: GET /loki/api/v1/labels?start=...&end=...
// VL:   GET /select/logsql/field_names?query=*&start=...&end=...
func (p *Proxy) handleLabels(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
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

	body, err := p.vlGetCoalesced("labels:"+params.Encode(), "/select/logsql/field_names", params)
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
		labels = append(labels, v.Value)
	}

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
	params.Set("field", labelName)
	if s := r.FormValue("start"); s != "" {
		params.Set("start", s)
	}
	if e := r.FormValue("end"); e != "" {
		params.Set("end", e)
	}
	if l := r.FormValue("limit"); l != "" {
		params.Set("limit", l)
	}

	resp, err := p.vlGet("/select/logsql/field_values", params)
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
		translated, err := translator.TranslateLogQL(matchQueries[0])
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

	resp, err := p.vlGet("/select/logsql/streams", params)
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

// handleIndexStats returns basic index statistics.
func (p *Proxy) handleIndexStats(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	// Return a minimal response — VL doesn't have a direct equivalent
	result, _ := json.Marshal(map[string]interface{}{
		"streams": 0,
		"chunks":  0,
		"bytes":   0,
		"entries": 0,
	})
	w.Header().Set("Content-Type", "application/json")
	w.Write(result)
	p.metrics.RecordRequest("index_stats", http.StatusOK, time.Since(start))
}

// handleVolume returns volume data for labels.
func (p *Proxy) handleVolume(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	p.writeJSON(w, map[string]interface{}{
		"status": "success",
		"data": map[string]interface{}{
			"resultType": "vector",
			"result":     []interface{}{},
		},
	})
	p.metrics.RecordRequest("volume", http.StatusOK, time.Since(start))
}

// handleVolumeRange returns volume range data.
func (p *Proxy) handleVolumeRange(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	p.writeJSON(w, map[string]interface{}{
		"status": "success",
		"data": map[string]interface{}{
			"resultType": "matrix",
			"result":     []interface{}{},
		},
	})
	p.metrics.RecordRequest("volume_range", http.StatusOK, time.Since(start))
}

// handleDetectedFields returns detected field names.
func (p *Proxy) handleDetectedFields(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	query := r.FormValue("query")
	if query == "" {
		query = "*"
	}

	logsqlQuery, _ := translator.TranslateLogQL(query)

	params := url.Values{}
	params.Set("query", logsqlQuery)
	if s := r.FormValue("start"); s != "" {
		params.Set("start", s)
	}
	if e := r.FormValue("end"); e != "" {
		params.Set("end", e)
	}

	resp, err := p.vlGet("/select/logsql/field_names", params)
	if err != nil {
		p.writeError(w, http.StatusBadGateway, err.Error())
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

	fields := make([]map[string]interface{}, 0, len(vlResp.Values))
	for _, v := range vlResp.Values {
		fields = append(fields, map[string]interface{}{
			"label":       v.Value,
			"type":        "string",
			"cardinality": v.Hits,
		})
	}

	result, _ := json.Marshal(map[string]interface{}{
		"fields": fields,
	})
	w.Header().Set("Content-Type", "application/json")
	w.Write(result)
	p.metrics.RecordRequest("detected_fields", http.StatusOK, time.Since(start))
}

// handlePatterns returns log patterns (stub).
func (p *Proxy) handlePatterns(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	p.writeJSON(w, map[string]interface{}{
		"status": "success",
		"data":   []interface{}{},
	})
	p.metrics.RecordRequest("patterns", http.StatusOK, time.Since(start))
}

// handleTail handles live tailing (WebSocket → SSE bridge).
func (p *Proxy) handleTail(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	// TODO: Implement WebSocket-to-SSE bridge for /select/logsql/tail
	p.writeError(w, http.StatusNotImplemented, "tail not yet implemented")
	p.metrics.RecordRequest("tail", http.StatusNotImplemented, time.Since(start))
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

func (p *Proxy) vlGet(path string, params url.Values) (*http.Response, error) {
	if !p.breaker.Allow() {
		return nil, fmt.Errorf("circuit breaker open — backend unavailable")
	}

	u := *p.backend
	u.Path = path
	u.RawQuery = params.Encode()

	p.log.Debug("VL request", "method", "GET", "url", u.String())
	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return nil, err
	}
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

func (p *Proxy) vlPost(path string, params url.Values) (*http.Response, error) {
	if !p.breaker.Allow() {
		return nil, fmt.Errorf("circuit breaker open — backend unavailable")
	}

	u := *p.backend
	u.Path = path

	p.log.Debug("VL request", "method", "POST", "url", u.String(), "params", params.Encode())
	req, err := http.NewRequest("POST", u.String(), strings.NewReader(params.Encode()))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
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
func (p *Proxy) vlGetCoalesced(key, path string, params url.Values) ([]byte, error) {
	_, _, body, err := p.coalescer.Do(key, func() (*http.Response, error) {
		return p.vlGet(path, params)
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

	resp, err := p.vlPost("/select/logsql/stats_query_range", params)
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

	resp, err := p.vlPost("/select/logsql/stats_query", params)
	if err != nil {
		p.writeError(w, http.StatusBadGateway, err.Error())
		return
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	w.Header().Set("Content-Type", "application/json")
	w.Write(wrapAsLokiResponse(body, "vector"))
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
		limit = "1000"
	}
	params.Set("limit", limit)

	resp, err := p.vlPost("/select/logsql/query", params)
	if err != nil {
		p.writeError(w, http.StatusBadGateway, err.Error())
		return
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)

	// VL returns newline-delimited JSON, each line is a log entry.
	// Loki expects: {"status":"success","data":{"resultType":"streams","result":[...]}}
	streams := vlLogsToLokiStreams(body)

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

// --- Error / JSON helpers ---

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
