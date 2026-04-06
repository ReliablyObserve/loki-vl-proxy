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

const vmalertURL = "http://localhost:8880"

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
	assertRulePresent(t, proxyGroups, "ApiGatewayErrorsPresent", "alerting", "vlogs")
	assertRulePresent(t, proxyGroups, "PaymentServiceErrorsPresent", "alerting", "vlogs")
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
		"alert: ApiGatewayErrorsPresent",
		"alert: PaymentServiceErrorsPresent",
		"severity: page",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("expected YAML response to contain %q, got %s", want, text)
		}
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
