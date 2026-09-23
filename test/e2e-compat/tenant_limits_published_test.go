//go:build e2e

package e2e_compat

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"gopkg.in/yaml.v3"
)

// tenantLimitsDeploymentKeys are the published limits that describe a
// deployment rather than a query limit: they differ between this stack's Loki
// and a proxy by configuration, not by behaviour.
var tenantLimitsDeploymentKeys = map[string]bool{
	"pattern_persistence_enabled": true, // the patterns proxies persist patterns; this Loki does not
}

// fetchPublishedLimits returns the limits base publishes for tenant 0 on
// /loki/api/v1/drilldown-limits and /config/tenant/v1/limits.
func fetchPublishedLimits(t *testing.T, base string) (map[string]interface{}, map[string]interface{}) {
	t.Helper()
	get := func(path string) []byte {
		req, err := http.NewRequest(http.MethodGet, base+path, nil)
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("X-Scope-OrgID", "0")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("GET %s%s: %v", base, path, err)
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("GET %s%s: %d %s", base, path, resp.StatusCode, body)
		}
		return body
	}
	var drilldown struct {
		Limits map[string]interface{} `json:"limits"`
	}
	if err := json.Unmarshal(get("/loki/api/v1/drilldown-limits"), &drilldown); err != nil {
		t.Fatalf("%s drilldown-limits: %v", base, err)
	}
	var config map[string]interface{}
	if err := yaml.Unmarshal(get("/config/tenant/v1/limits"), &config); err != nil {
		t.Fatalf("%s tenant limits: %v", base, err)
	}
	return drilldown.Limits, config
}

// sameLimit compares two published values after YAML/JSON number decoding
// (1e+06 and 1000000 are the same limit).
func sameLimit(a, b interface{}) bool {
	toFloat := func(v interface{}) (float64, bool) {
		switch n := v.(type) {
		case float64:
			return n, true
		case int:
			return float64(n), true
		case string:
			f, err := strconv.ParseFloat(n, 64)
			return f, err == nil
		}
		return 0, false
	}
	if fa, ok := toFloat(a); ok {
		if fb, ok := toFloat(b); ok {
			return fa == fb
		}
	}
	return fmt.Sprint(a) == fmt.Sprint(b)
}

// The proxies of this stack that mirror its Loki's limits_config publish, on
// both limits endpoints, exactly what Loki publishes; the proxy kept at the
// built-in defaults publishes those defaults. Each published max_entries
// limit is then checked against what the proxy does with a log query just
// past it.
//
// conformance: tenant-query-limits, limits/published-equals-enforced, limits/max-entries-limit-rejects, loki_api_v1_drilldown_limits
func TestDrilldown_TenantLimitsPublishedAsEnforced(t *testing.T) {
	lokiDrilldown, lokiConfig := fetchPublishedLimits(t, lokiURL)
	for name, base := range map[string]string{"patterns-autodetect": patternsAutodetectProxyURL, "primary": proxyURL} {
		t.Run(name+" publishes Loki's limits", func(t *testing.T) {
			drilldown, config := fetchPublishedLimits(t, base)
			for key, want := range lokiDrilldown {
				if tenantLimitsDeploymentKeys[key] {
					continue
				}
				if got, ok := drilldown[key]; !ok || !sameLimit(got, want) {
					t.Errorf("drilldown-limits %s: proxy %v, Loki %v", key, got, want)
				}
			}
			for key := range drilldown {
				if _, ok := lokiDrilldown[key]; !ok {
					t.Errorf("drilldown-limits %s: published by the proxy, not by Loki", key)
				}
			}
			for key, want := range lokiConfig {
				got, ok := config[key]
				if !ok {
					continue // Loki's YAML lists every limits_config field; the proxy publishes its allowlist.
				}
				if !tenantLimitsDeploymentKeys[key] && !sameLimit(got, want) {
					t.Errorf("/config/tenant/v1/limits %s: proxy %v, Loki %v", key, got, want)
				}
			}
		})
	}

	t.Run("default-limit proxy publishes the defaults", func(t *testing.T) {
		drilldown, config := fetchPublishedLimits(t, proxyVmauthURL)
		for key, want := range map[string]interface{}{"max_query_series": 500.0, "max_entries_limit_per_query": 10000.0, "max_query_length": "0s", "query_timeout": "2m"} {
			if !sameLimit(drilldown[key], want) || !sameLimit(config[key], want) {
				t.Errorf("%s: drilldown-limits %v, /config/tenant/v1/limits %v, want %v", key, drilldown[key], config[key], want)
			}
		}
	})

	// A log query asking for one line more than the published
	// max_entries_limit_per_query gets Loki's 400; 0 is unlimited.
	now := time.Now()
	for name, base := range map[string]string{"default-limit proxy": proxyVmauthURL, "patterns-autodetect": patternsAutodetectProxyURL} {
		t.Run(name+" enforces max_entries_limit_per_query as published", func(t *testing.T) {
			drilldown, _ := fetchPublishedLimits(t, base)
			published, ok := drilldown["max_entries_limit_per_query"].(float64)
			if !ok {
				t.Fatalf("max_entries_limit_per_query: %v", drilldown["max_entries_limit_per_query"])
			}
			asked := int(published) + 1
			if published == 0 {
				asked = 20000
			}
			params := url.Values{
				"query": {`{app="api-gateway"}`},
				"start": {strconv.FormatInt(now.Add(-5*time.Minute).UnixNano(), 10)},
				"end":   {strconv.FormatInt(now.UnixNano(), 10)},
				"limit": {strconv.Itoa(asked)},
			}
			req, _ := http.NewRequest(http.MethodGet, base+"/loki/api/v1/query_range?"+params.Encode(), nil)
			req.Header.Set("X-Scope-OrgID", "0")
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			body, _ := io.ReadAll(resp.Body)
			if published == 0 {
				if resp.StatusCode != http.StatusOK {
					t.Fatalf("unlimited max_entries_limit_per_query: limit=%d must run, got %d %s", asked, resp.StatusCode, body)
				}
				return
			}
			want := fmt.Sprintf("max entries limit per query exceeded, limit > max_entries_limit_per_query (%d > %d)", asked, int(published))
			var answer struct {
				Error string `json:"error"`
			}
			_ = json.Unmarshal(body, &answer)
			if resp.StatusCode != http.StatusBadRequest || answer.Error != want {
				t.Fatalf("limit=%d: want 400 %q, got %d %s", asked, want, resp.StatusCode, strings.TrimSpace(string(body)))
			}
		})
	}
}
