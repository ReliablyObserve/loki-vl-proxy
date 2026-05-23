package proxy

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// handleReady returns readiness status.
func (p *Proxy) handleReady(w http.ResponseWriter, r *http.Request) {
	if p.labelValuesIndexedCache && !p.labelValuesIndexWarmReady.Load() {
		w.WriteHeader(http.StatusServiceUnavailable)
		w.Write([]byte("label values index warming"))
		return
	}
	if p.patternsEnabled && strings.TrimSpace(p.patternsPersistPath) != "" && !p.patternsWarmReady.Load() {
		w.WriteHeader(http.StatusServiceUnavailable)
		w.Write([]byte("patterns snapshot warming"))
		return
	}

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

// handleHealth returns process health status without backend dependency checks.
func (p *Proxy) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte("ok"))
}

// handleAlive returns process liveness status without backend dependency checks.
func (p *Proxy) handleAlive(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte("alive"))
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

	copyBackendHeaders(w.Header(), resp.Header)
	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, resp.Body)
}

func (p *Proxy) alertingBackendGet(r *http.Request, backend *url.URL, path string) (*http.Response, error) {
	return p.alertingBackendGetWithParams(r, backend, path, r.URL.Query())
}

// applyAlertingBackendHeaders sets only the Accept-Encoding header on requests
// to alerting/ruler backends. It intentionally does NOT apply p.backendHeaders
// (which carry VL credentials) and does NOT forward client auth headers or
// cookies, so VictoriaLogs credentials and per-user tokens are never sent to a
// different backend service.
func (p *Proxy) applyAlertingBackendHeaders(req *http.Request) {
	if req.Header.Get("Accept-Encoding") == "" {
		switch p.backendCompression {
		case "none":
			req.Header.Set("Accept-Encoding", "identity")
		case "gzip":
			req.Header.Set("Accept-Encoding", "gzip")
		case "zstd":
			req.Header.Set("Accept-Encoding", "zstd")
		default:
			req.Header.Set("Accept-Encoding", "zstd, gzip")
		}
	}
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
	p.applyAlertingBackendHeaders(req)
	start := time.Now()
	resp, err := p.client.Do(req)
	duration := time.Since(start)
	serverPort, _ := strconv.Atoi(u.Port())
	if err != nil {
		mappedStatus := statusFromUpstreamErr(err)
		p.recordUpstreamObservation(r.Context(), "loki", http.MethodGet, path, u.Hostname(), serverPort, mappedStatus, duration, err)
		return nil, err
	}
	if err := decodeCompressedHTTPResponse(resp); err != nil {
		_ = resp.Body.Close()
		p.recordUpstreamObservation(r.Context(), "loki", http.MethodGet, path, u.Hostname(), serverPort, http.StatusBadGateway, duration, err)
		return nil, fmt.Errorf("decode backend response: %w", err)
	}
	p.recordUpstreamObservation(r.Context(), "loki", http.MethodGet, path, u.Hostname(), serverPort, resp.StatusCode, duration, nil)
	return resp, nil
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
