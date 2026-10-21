package proxy

import (
	"bytes"
	"context"
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

	// Probe VL backend health via the circuit breaker.
	// vlGet calls breaker.Allow() internally; if the breaker is open or the
	// probe fails, we return 503. A successful vlGet also calls RecordSuccess(),
	// counting toward the half-open successThreshold and closing the breaker
	// once enough consecutive probes pass. A second explicit Allow() call here
	// would consume an extra probe slot without recording a success, permanently
	// deadlocking the breaker in half-open state when halfOpenProbes ≥ successThreshold.
	readyReq := p.withRequestScope(r)
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

// handleBuildInfo returns build info for Grafana datasource detection.
// Version >= 3.0.0 is required for Grafana to send X-Loki-Response-Encoding-Flags:categorize-labels,
// which enables structured_metadata in query_range responses.
func (p *Proxy) handleBuildInfo(w http.ResponseWriter, r *http.Request) {
	p.writeJSON(w, map[string]interface{}{
		"status": "success",
		"data": map[string]interface{}{
			"version":   "3.7.1",
			"revision":  "loki-vl-proxy",
			"branch":    "main",
			"goVersion": "go1.26.1",
		},
	})
}

// --- Backend request helpers ---

func (p *Proxy) proxyAlertingRead(w http.ResponseWriter, r *http.Request, backend *url.URL, path string) {
	resp, err := p.alertingBackendGet(p.withRequestScope(r), backend, path)
	if err != nil {
		p.writeError(w, http.StatusBadGateway, err.Error())
		return
	}
	defer resp.Body.Close()

	label, org, confined := p.alertingTenantScope(r.Context())
	if confined {
		if resp.StatusCode/100 != 2 {
			// A backend error body is not attributable to the org; keep the
			// status and say nothing else.
			p.writeError(w, resp.StatusCode, "alerting backend error")
			return
		}
		body, err := io.ReadAll(io.LimitReader(resp.Body, maxAlertingResponseBytes+1))
		if err == nil && len(body) <= maxAlertingResponseBytes {
			body, err = filterAlertingJSONForTenant(body, label, org)
		}
		if err != nil || len(body) > maxAlertingResponseBytes {
			p.writeError(w, http.StatusBadGateway, "invalid alerting backend response")
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(body)
		return
	}

	copyBackendHeaders(w.Header(), resp.Header)
	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, resp.Body)
}

const maxAlertingResponseBytes = 64 << 20

// alertingTenantScope reports whether a rules/alerts read must be confined to the
// rules and alerts of the request's org. The ruler and alerts backends know
// nothing of the tenant label, so in tenant-label mode they answer for every
// tenant. A mapped org gets its mapping's tenant headers instead, and allow-global
// keeps the global view; every other org sees only the rules and alerts that carry
// <tenant-label>=<org> in their labels. A rule without that label belongs to no
// tenant and is shown to none (fail closed). The rule's query is not used for
// attribution: a stream selector cannot be matched reliably.
func (p *Proxy) alertingTenantScope(ctx context.Context) (label, org string, confined bool) {
	routing := p.routingForContext(ctx)
	org = getOrgID(ctx)
	if routing.label == "" {
		return "", "", false
	}
	if _, mapped := routing.tenants[org]; mapped && org != "" {
		return "", "", false
	}
	if org == "" || isDefaultTenantAlias(org) || org == "*" {
		if p.globalTenantAllowed() {
			return "", "", false
		}
		org = tenantFilterNeverMatches
	}
	return routing.label, org, true
}

// alertingTenantMatches is the one attribution rule for rules and alerts: the
// tenant-label value is the org. A value holding a template ("{{") is expanded
// by the ruler per alert, so what the rule shows is not what its alerts carry:
// it is attributable to no org, and never matches.
func alertingTenantMatches(value, org string) bool {
	return value == org && !strings.Contains(value, "{{")
}

func alertingLabelsMatch(labels any, label, org string) bool {
	m, _ := labels.(map[string]any)
	v, _ := m[label].(string)
	return alertingTenantMatches(v, org)
}

// alertingGroupKeep is what a kept rule group retains: its identity, schedule
// and evaluation times, and its rules. The ruler's other group fields (query
// parameters, headers, group labels) can describe rules of other tenants in a
// group that mixes them, so they are dropped. Use one tenant per rule group.
var alertingGroupKeep = map[string]bool{
	"name": true, "file": true, "interval": true, "limit": true, "rules": true,
	"evaluationTime": true, "lastEvaluation": true,
}

// alertingList reads an array that the ruler may answer as null, which is empty.
func alertingList(v any) ([]any, bool) {
	if v == nil {
		return nil, true
	}
	list, ok := v.([]any)
	return list, ok
}

// filterAlertingJSONForTenant keeps, in a Prometheus-style rules or alerts answer,
// the rules and alerts labelled for org (including the alerts nested in a kept
// rule), and drops rule groups left without a rule. The answer keeps its shape
// (an empty groups or alerts list), so Grafana reads it as it reads an org
// without rules. Warnings, infos and the group paging token describe the whole
// backend, so they are dropped. An answer of any other shape, or one holding both
// groups and alerts, is an error rather than passed on unfiltered.
func filterAlertingJSONForTenant(body []byte, label, org string) ([]byte, error) {
	var doc map[string]any
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.UseNumber()
	if err := dec.Decode(&doc); err != nil {
		return nil, err
	}
	data, ok := doc["data"].(map[string]any)
	if !ok {
		return nil, fmt.Errorf("unexpected alerting answer shape")
	}
	_, hasGroups := data["groups"]
	_, hasAlerts := data["alerts"]
	if hasGroups == hasAlerts {
		return nil, fmt.Errorf("unexpected alerting answer shape")
	}
	delete(doc, "warnings")
	delete(doc, "infos")
	delete(data, "groupNextToken")
	if hasGroups {
		groups, ok := alertingList(data["groups"])
		if !ok {
			return nil, fmt.Errorf("unexpected alerting answer shape")
		}
		kept := []any{}
		for _, g := range groups {
			group, _ := g.(map[string]any)
			rules, ok := alertingList(group["rules"])
			if !ok {
				return nil, fmt.Errorf("unexpected alerting answer shape")
			}
			keptRules := []any{}
			for _, rule := range rules {
				r, _ := rule.(map[string]any)
				if !alertingLabelsMatch(r["labels"], label, org) {
					continue
				}
				nested, ok := alertingList(r["alerts"])
				if !ok {
					return nil, fmt.Errorf("unexpected alerting answer shape")
				}
				if _, present := r["alerts"]; present {
					keptAlerts := []any{}
					for _, a := range nested {
						if alert, _ := a.(map[string]any); alertingLabelsMatch(alert["labels"], label, org) {
							keptAlerts = append(keptAlerts, a)
						}
					}
					r["alerts"] = keptAlerts
				}
				keptRules = append(keptRules, rule)
			}
			if len(keptRules) == 0 {
				continue
			}
			for k := range group {
				if !alertingGroupKeep[k] {
					delete(group, k)
				}
			}
			group["rules"] = keptRules
			kept = append(kept, group)
		}
		data["groups"] = kept
	} else {
		alerts, ok := alertingList(data["alerts"])
		if !ok {
			return nil, fmt.Errorf("unexpected alerting answer shape")
		}
		kept := []any{}
		for _, a := range alerts {
			if alert, _ := a.(map[string]any); alertingLabelsMatch(alert["labels"], label, org) {
				kept = append(kept, a)
			}
		}
		data["alerts"] = kept
	}
	// Rule text can hold "<", ">" and "&"; the default encoding would escape them.
	var out bytes.Buffer
	enc := json.NewEncoder(&out)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(doc); err != nil {
		return nil, err
	}
	return out.Bytes(), nil
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
	resp, err := p.doBackendRequest(req, p.client)
	duration := time.Since(start)
	serverPort, _ := strconv.Atoi(u.Port())
	if err != nil {
		mappedStatus := upstreamErrorStatus(r.Context(), err)
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

	resp, err := p.alertingBackendGetWithParams(p.withRequestScope(r), p.rulerBackend, "/api/v1/rules", params)
	if err != nil {
		p.writeError(w, http.StatusBadGateway, err.Error())
		return
	}
	defer resp.Body.Close()
	label, org, confined := p.alertingTenantScope(r.Context())
	if confined && resp.StatusCode/100 != 2 {
		p.writeError(w, resp.StatusCode, "alerting backend error")
		return
	}
	if !confined && resp.StatusCode != http.StatusOK {
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

	// Tenant-label mode: the legacy YAML views are built from the same rules, so
	// the same attribution applies; a group left empty is "not found".
	if confined {
		kept := decoded.Data.Groups[:0]
		for _, g := range decoded.Data.Groups {
			rules := g.Rules[:0]
			for _, rule := range g.Rules {
				if alertingTenantMatches(rule.Labels[label], org) {
					rules = append(rules, rule)
				}
			}
			g.Rules = rules
			if len(rules) > 0 {
				kept = append(kept, g)
			}
		}
		decoded.Data.Groups = kept
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
