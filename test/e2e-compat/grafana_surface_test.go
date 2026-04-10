//go:build e2e

package e2e_compat

import (
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"
)

func TestGrafanaDatasourceCatalogAndHealth(t *testing.T) {
	ensureDataIngested(t)

	type datasourceExpectation struct {
		name string
		kind string
	}

	expected := []datasourceExpectation{
		{name: "Loki (direct)", kind: "loki"},
		{name: "Loki (via VL proxy)", kind: "loki"},
		{name: "Loki (via VL proxy multi-tenant)", kind: "loki"},
		{name: "Loki (via VL proxy native metadata)", kind: "loki"},
		{name: "Loki (via VL proxy live tail)", kind: "loki"},
		{name: "Loki (via ingress tail)", kind: "loki"},
		{name: "Loki (via VL proxy live tail native)", kind: "loki"},
		{name: "VictoriaLogs (direct)", kind: "victoriametrics-logs-datasource"},
	}

	for _, tc := range expected {
		t.Run(tc.name, func(t *testing.T) {
			resp := getJSON(t, grafanaURL+"/api/datasources/name/"+url.PathEscape(tc.name))
			if resp["uid"] == "" {
				t.Fatalf("expected datasource uid for %q, got %v", tc.name, resp)
			}
			if got, _ := resp["type"].(string); got != tc.kind {
				t.Fatalf("expected datasource %q type=%q, got %q", tc.name, tc.kind, got)
			}
		})
	}

	directVLUID := grafanaDatasourceUID(t, "VictoriaLogs (direct)")
	health := getJSON(t, grafanaURL+"/api/datasources/uid/"+url.PathEscape(directVLUID)+"/health")
	if got, _ := health["status"].(string); got != "OK" {
		t.Fatalf("expected VictoriaLogs direct datasource health OK, got %v", health)
	}
}

func TestProxyCompatibilitySurface(t *testing.T) {
	ensureDataIngested(t)

	t.Run("ready", func(t *testing.T) {
		resp, err := http.Get(proxyURL + "/ready")
		if err != nil {
			t.Fatalf("ready request failed: %v", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("expected /ready to return 200, got %d", resp.StatusCode)
		}
	})

	t.Run("buildinfo", func(t *testing.T) {
		resp := getJSON(t, proxyURL+"/loki/api/v1/status/buildinfo")
		if got, _ := resp["status"].(string); got != "success" {
			t.Fatalf("expected buildinfo status=success, got %v", resp)
		}
		data := extractMap(resp, "data")
		if data == nil || data["version"] == nil || data["version"] == "" {
			t.Fatalf("expected buildinfo version, got %v", resp)
		}
	})

	t.Run("rules", func(t *testing.T) {
		resp, err := http.Get(proxyURL + "/loki/api/v1/rules")
		if err != nil {
			t.Fatalf("rules request failed: %v", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("expected rules status 200, got %d", resp.StatusCode)
		}
		if !strings.Contains(resp.Header.Get("Content-Type"), "application/yaml") {
			t.Fatalf("expected yaml content type, got %q", resp.Header.Get("Content-Type"))
		}
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatalf("failed to read rules body: %v", err)
		}
		if !strings.Contains(string(body), "loki-vl-e2e-alerts") {
			t.Fatalf("expected rules body to include loki-vl-e2e-alerts, got %s", string(body))
		}
	})

	t.Run("alerts", func(t *testing.T) {
		resp := getJSON(t, proxyURL+"/loki/api/v1/alerts")
		if got, _ := resp["status"].(string); got != "success" {
			t.Fatalf("expected alerts status=success, got %v", resp)
		}
		data := extractMap(resp, "data")
		if data == nil || data["alerts"] == nil {
			t.Fatalf("expected alerts payload, got %v", resp)
		}
	})

	t.Run("direct_loki_drilldown_limits", func(t *testing.T) {
		req, err := http.NewRequest(http.MethodGet, lokiURL+"/loki/api/v1/drilldown-limits", nil)
		if err != nil {
			t.Fatalf("failed to create drilldown-limits request: %v", err)
		}
		req.Header.Set("X-Scope-OrgID", "0")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("drilldown-limits request failed: %v", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("expected direct Loki drilldown-limits status 200, got %d", resp.StatusCode)
		}
	})
}
