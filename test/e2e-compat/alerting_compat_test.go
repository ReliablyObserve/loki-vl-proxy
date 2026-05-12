//go:build e2e

package e2e_compat

import (
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"
)

const vmalertURL = "http://localhost:18880"

func TestAlertingCompat_PrometheusRulesAndAlerts(t *testing.T) {
	ensureDataIngested(t)
	waitForReady(t, vmalertURL+"/api/v1/rules?datasource_type=vlogs", 30*time.Second)
	waitForAlertingData(t)

	rulesDirect := getJSON(t, vmalertURL+"/api/v1/rules?datasource_type=vlogs")
	rulesProxy := getJSON(t, proxyURL+"/prometheus/api/v1/rules?datasource_type=vlogs")

	directGroups := extractArray(extractMap(rulesDirect, "data"), "groups")
	proxyGroups := extractArray(extractMap(rulesProxy, "data"), "groups")
	if len(directGroups) != 1 || len(proxyGroups) != 1 {
		t.Fatalf("expected exactly one alerting group via direct=%d proxy=%d", len(directGroups), len(proxyGroups))
	}
	assertGroupPresent(t, proxyGroups, "loki-vl-e2e-alerts")
	assertRulePresent(t, proxyGroups, "ApiGatewayErrorLogLines", "recording", "vlogs")
	assertRulePresent(t, proxyGroups, "ApiGatewayErrorsPresent", "alerting", "vlogs")
	assertRulePresent(t, proxyGroups, "PaymentServiceErrorsPresent", "alerting", "vlogs")
	assertRuleQueryParity(t, directGroups, proxyGroups, "ApiGatewayErrorLogLines")
	assertRuleStateParity(t, directGroups, proxyGroups, "ApiGatewayErrorsPresent")
	assertRuleStateParity(t, directGroups, proxyGroups, "PaymentServiceErrorsPresent")

	alertsDirect := getJSON(t, vmalertURL+"/api/v1/alerts?datasource_type=vlogs")
	alertsProxy := getJSON(t, proxyURL+"/prometheus/api/v1/alerts?datasource_type=vlogs")
	directAlerts := extractArray(extractMap(alertsDirect, "data"), "alerts")
	proxyAlerts := extractArray(extractMap(alertsProxy, "data"), "alerts")
	if len(directAlerts) != len(proxyAlerts) {
		t.Fatalf("expected alert parity via direct=%d proxy=%d", len(directAlerts), len(proxyAlerts))
	}
	assertAlertSetParity(t, directAlerts, proxyAlerts)
}

func TestAlertingCompat_LegacyLokiRulesYAML(t *testing.T) {
	ensureDataIngested(t)
	waitForReady(t, vmalertURL+"/api/v1/rules?datasource_type=vlogs", 30*time.Second)
	waitForAlertingData(t)

	resp, err := http.Get(proxyURL + "/loki/api/v1/rules/compat.rules/loki-vl-e2e-alerts")
	if err != nil {
		t.Fatalf("legacy loki rules request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected 200, got %d body=%s", resp.StatusCode, string(body))
	}
	if ct := resp.Header.Get("Content-Type"); !strings.Contains(ct, "application/yaml") {
		t.Fatalf("expected YAML content-type, got %q", ct)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("failed to read body: %v", err)
	}
	text := string(body)
	for _, want := range []string{
		"name: loki-vl-e2e-alerts",
		"record: ApiGatewayErrorLogLines",
		"alert: ApiGatewayErrorsPresent",
		"alert: PaymentServiceErrorsPresent",
		"severity: page",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("expected YAML response to contain %q, got %s", want, text)
		}
	}
}

func TestAlertingCompat_GrafanaDatasourceRulesAndAlertsParity(t *testing.T) {
	ensureDataIngested(t)
	waitForReady(t, vmalertURL+"/api/v1/rules?datasource_type=vlogs", 30*time.Second)
	waitForAlertingData(t)

	dsUID := grafanaDatasourceUID(t, "Loki (via VL proxy)")
	rulesPath := grafanaURL + "/api/datasources/proxy/uid/" + url.PathEscape(dsUID) + "/prometheus/api/v1/rules?datasource_type=vlogs"
	alertsPath := grafanaURL + "/api/datasources/proxy/uid/" + url.PathEscape(dsUID) + "/prometheus/api/v1/alerts?datasource_type=vlogs"

	rulesViaGrafana := getJSON(t, rulesPath)
	rulesViaProxy := getJSON(t, proxyURL+"/prometheus/api/v1/rules?datasource_type=vlogs")
	grafanaGroups := extractArray(extractMap(rulesViaGrafana, "data"), "groups")
	proxyGroups := extractArray(extractMap(rulesViaProxy, "data"), "groups")

	if len(grafanaGroups) != len(proxyGroups) {
		t.Fatalf("expected grafana datasource rules parity via proxy=%d grafana=%d", len(proxyGroups), len(grafanaGroups))
	}
	assertGroupPresent(t, grafanaGroups, "loki-vl-e2e-alerts")
	assertRulePresent(t, grafanaGroups, "ApiGatewayErrorLogLines", "recording", "vlogs")
	assertRulePresent(t, grafanaGroups, "ApiGatewayErrorsPresent", "alerting", "vlogs")
	assertRulePresent(t, grafanaGroups, "PaymentServiceErrorsPresent", "alerting", "vlogs")

	alertsViaGrafana := getJSON(t, alertsPath)
	alertsViaProxy := getJSON(t, proxyURL+"/prometheus/api/v1/alerts?datasource_type=vlogs")
	grafanaAlerts := extractArray(extractMap(alertsViaGrafana, "data"), "alerts")
	proxyAlerts := extractArray(extractMap(alertsViaProxy, "data"), "alerts")
	if len(grafanaAlerts) != len(proxyAlerts) {
		t.Fatalf("expected grafana datasource alerts parity via proxy=%d grafana=%d", len(proxyAlerts), len(grafanaAlerts))
	}
}

func waitForAlertingData(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(45 * time.Second)
	params := url.Values{}
	params.Set("datasource_type", "vlogs")
	for time.Now().Before(deadline) {
		rules := getJSON(t, proxyURL+"/prometheus/api/v1/rules?"+params.Encode())
		groups := extractArray(extractMap(rules, "data"), "groups")
		if len(groups) >= 1 {
			return
		}
		time.Sleep(2 * time.Second)
	}
	t.Fatal("timed out waiting for vmalert rules to become visible through proxy")
}

func assertGroupPresent(t *testing.T, groups []interface{}, want string) {
	t.Helper()
	for _, g := range groups {
		group, _ := g.(map[string]interface{})
		if group["name"] == want {
			return
		}
	}
	t.Fatalf("expected group %q in %+v", want, groups)
}

func assertRulePresent(t *testing.T, groups []interface{}, ruleName, ruleType, datasourceType string) {
	t.Helper()
	for _, g := range groups {
		group, _ := g.(map[string]interface{})
		rules, _ := group["rules"].([]interface{})
		for _, r := range rules {
			rule, _ := r.(map[string]interface{})
			if rule["name"] == ruleName {
				if rule["type"] != ruleType {
					t.Fatalf("expected rule %q type %q, got %v", ruleName, ruleType, rule["type"])
				}
				if rule["datasourceType"] != datasourceType {
					t.Fatalf("expected rule %q datasourceType %q, got %v", ruleName, datasourceType, rule["datasourceType"])
				}
				return
			}
		}
	}
	t.Fatalf("expected rule %q in %+v", ruleName, groups)
}

func assertRuleStateParity(t *testing.T, directGroups, proxyGroups []interface{}, ruleName string) {
	t.Helper()

	directState, ok := findRuleState(directGroups, ruleName)
	if !ok {
		t.Fatalf("expected direct rule %q in %+v", ruleName, directGroups)
	}
	proxyState, ok := findRuleState(proxyGroups, ruleName)
	if !ok {
		t.Fatalf("expected proxy rule %q in %+v", ruleName, proxyGroups)
	}
	if directState != proxyState {
		t.Fatalf("expected rule %q state parity direct=%q proxy=%q", ruleName, directState, proxyState)
	}
}

func assertRuleQueryParity(t *testing.T, directGroups, proxyGroups []interface{}, ruleName string) {
	t.Helper()

	directRule, ok := findRule(directGroups, ruleName)
	if !ok {
		t.Fatalf("expected direct rule %q in %+v", ruleName, directGroups)
	}
	proxyRule, ok := findRule(proxyGroups, ruleName)
	if !ok {
		t.Fatalf("expected proxy rule %q in %+v", ruleName, proxyGroups)
	}
	directQuery, _ := directRule["query"].(string)
	proxyQuery, _ := proxyRule["query"].(string)
	if directQuery != proxyQuery {
		t.Fatalf("expected rule %q query parity direct=%q proxy=%q", ruleName, directQuery, proxyQuery)
	}
}

func findRule(groups []interface{}, ruleName string) (map[string]interface{}, bool) {
	for _, g := range groups {
		group, _ := g.(map[string]interface{})
		rules, _ := group["rules"].([]interface{})
		for _, r := range rules {
			rule, _ := r.(map[string]interface{})
			if rule["name"] == ruleName {
				return rule, true
			}
		}
	}
	return nil, false
}

func findRuleState(groups []interface{}, ruleName string) (string, bool) {
	for _, g := range groups {
		group, _ := g.(map[string]interface{})
		rules, _ := group["rules"].([]interface{})
		for _, r := range rules {
			rule, _ := r.(map[string]interface{})
			if rule["name"] == ruleName {
				state, _ := rule["state"].(string)
				return state, true
			}
		}
	}
	return "", false
}

func assertAlertSetParity(t *testing.T, directAlerts, proxyAlerts []interface{}) {
	t.Helper()
	directByName := make(map[string]string, len(directAlerts))
	for _, a := range directAlerts {
		alert, _ := a.(map[string]interface{})
		name, _ := alert["name"].(string)
		state, _ := alert["state"].(string)
		directByName[name] = state
	}
	for _, a := range proxyAlerts {
		alert, _ := a.(map[string]interface{})
		name, _ := alert["name"].(string)
		state, _ := alert["state"].(string)
		want, ok := directByName[name]
		if !ok {
			t.Fatalf("unexpected proxy alert %q in %+v", name, proxyAlerts)
		}
		if want != state {
			t.Fatalf("expected alert %q state parity direct=%q proxy=%q", name, want, state)
		}
		delete(directByName, name)
	}
	if len(directByName) > 0 {
		t.Fatalf("proxy missing direct alerts: %+v", directByName)
	}
}

func TestAlertingCompat_PromAliasRoutes(t *testing.T) {
	ensureDataIngested(t)
	waitForReady(t, vmalertURL+"/api/v1/rules?datasource_type=vlogs", 30*time.Second)
	waitForAlertingData(t)

	for _, path := range []string{
		"/api/prom/rules/compat.rules/loki-vl-e2e-alerts",
		"/loki/api/v1/rules/compat.rules/loki-vl-e2e-alerts",
	} {
		resp, err := http.Get(proxyURL + path)
		if err != nil {
			t.Fatalf("legacy rules alias request failed for %s: %v", path, err)
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("expected 200 from %s, got %d body=%s", path, resp.StatusCode, string(body))
		}
		if !strings.Contains(string(body), "alert: ApiGatewayErrorsPresent") {
			t.Fatalf("expected YAML alert payload from %s, got %s", path, string(body))
		}
	}

	alertsProm := getJSON(t, proxyURL+"/api/prom/alerts?datasource_type=vlogs")
	alertsPrometheus := getJSON(t, proxyURL+"/prometheus/api/v1/alerts?datasource_type=vlogs")
	directAlerts := extractArray(extractMap(alertsPrometheus, "data"), "alerts")
	aliasAlerts := extractArray(extractMap(alertsProm, "data"), "alerts")
	if len(directAlerts) != len(aliasAlerts) {
		t.Fatalf("expected alerts alias parity direct=%d alias=%d", len(directAlerts), len(aliasAlerts))
	}
}

func TestAlertingCompat_LegacyNamespaceAndGroupFiltering(t *testing.T) {
	ensureDataIngested(t)
	waitForReady(t, vmalertURL+"/api/v1/rules?datasource_type=vlogs", 30*time.Second)
	waitForAlertingData(t)

	namespaceResp, err := http.Get(proxyURL + "/loki/api/v1/rules/compat.rules")
	if err != nil {
		t.Fatalf("legacy namespace rules request failed: %v", err)
	}
	defer namespaceResp.Body.Close()
	if namespaceResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(namespaceResp.Body)
		t.Fatalf("expected 200, got %d body=%s", namespaceResp.StatusCode, string(body))
	}
	namespaceBody, _ := io.ReadAll(namespaceResp.Body)
	namespaceText := string(namespaceBody)
	if !strings.Contains(namespaceText, "name: loki-vl-e2e-alerts") {
		t.Fatalf("expected namespace filtered YAML to contain the alerting group, got %s", namespaceText)
	}

	groupResp, err := http.Get(proxyURL + "/loki/api/v1/rules/compat.rules/loki-vl-e2e-alerts")
	if err != nil {
		t.Fatalf("legacy group rules request failed: %v", err)
	}
	defer groupResp.Body.Close()
	if groupResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(groupResp.Body)
		t.Fatalf("expected 200, got %d body=%s", groupResp.StatusCode, string(body))
	}
	groupBody, _ := io.ReadAll(groupResp.Body)
	groupText := string(groupBody)
	if !strings.Contains(groupText, "alert: ApiGatewayErrorsPresent") || strings.Contains(groupText, "compat.rules:") {
		t.Fatalf("expected single-group YAML payload, got %s", groupText)
	}
}

func TestAlertingCompat_LegacyRulesRejectTraversal(t *testing.T) {
	resp, err := http.Get(proxyURL + "/loki/api/v1/rules/%2e%2e/escape")
	if err != nil {
		t.Fatalf("legacy traversal request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected 400 for traversal attempt, got %d body=%s", resp.StatusCode, string(body))
	}
}

func TestAlertingCompat_LegacyMissingRuleGroupReturnsNotFound(t *testing.T) {
	ensureDataIngested(t)
	waitForReady(t, vmalertURL+"/api/v1/rules?datasource_type=vlogs", 30*time.Second)
	waitForAlertingData(t)

	resp, err := http.Get(proxyURL + "/loki/api/v1/rules/compat.rules/does-not-exist")
	if err != nil {
		t.Fatalf("legacy missing rule-group request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected 404 for missing rule group, got %d body=%s", resp.StatusCode, string(body))
	}
}

func TestAlertingCompat_LegacyMissingNamespaceReturnsEmptyYAML(t *testing.T) {
	ensureDataIngested(t)
	waitForReady(t, vmalertURL+"/api/v1/rules?datasource_type=vlogs", 30*time.Second)
	waitForAlertingData(t)

	resp, err := http.Get(proxyURL + "/loki/api/v1/rules/does-not-exist")
	if err != nil {
		t.Fatalf("legacy missing namespace request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected 200 for missing namespace, got %d body=%s", resp.StatusCode, string(body))
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("failed to read missing namespace body: %v", err)
	}
	text := strings.TrimSpace(string(body))
	if text != "{}" {
		t.Fatalf("expected empty YAML map for missing namespace, got %q", text)
	}
}
