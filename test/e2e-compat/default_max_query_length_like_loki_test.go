//go:build e2e

package e2e_compat

import (
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"
)

// TestDefaultMaxQueryLengthLikeLoki compares the stack's proxy kept at the
// built-in limits (the vmauth variant: no -default-max-query-length and no
// -tenant-default-limits) with the stack's Loki, which sets no
// max_query_length either, so both run at their defaults. A range one
// millisecond over 721h gets the same 400 and body on every route Loki's limit
// covers; a range of exactly 721h runs on both; both publish the same
// max_query_length.
//
// conformance: limits/max-query-length-default, tenant-query-limits
func TestDefaultMaxQueryLengthLikeLoki(t *testing.T) {
	ensureDataIngested(t)
	waitForReady(t, proxyVmauthURL+"/ready", 30*time.Second)
	end := time.Now().Truncate(time.Second)
	sel := `{app="api-gateway"}`
	routes := []struct {
		name, path string
		params     url.Values
	}{
		{"query_range", "/loki/api/v1/query_range", url.Values{"query": {sel}, "limit": {"10"}, "step": {"3600"}}},
		{"labels", "/loki/api/v1/labels", url.Values{}},
		{"label_values", "/loki/api/v1/label/app/values", url.Values{}},
		{"series", "/loki/api/v1/series", url.Values{"match[]": {sel}}},
		{"detected_labels", "/loki/api/v1/detected_labels", url.Values{"query": {sel}}},
		{"detected_fields", "/loki/api/v1/detected_fields", url.Values{"query": {sel}}},
		{"index_stats", "/loki/api/v1/index/stats", url.Values{"query": {sel}}},
		{"index_volume", "/loki/api/v1/index/volume", url.Values{"query": {sel}}},
	}
	window := func(params url.Values, length time.Duration) url.Values {
		out := url.Values{}
		for k, v := range params {
			out[k] = v
		}
		out.Set("start", strconv.FormatInt(end.Add(-length).UnixNano(), 10))
		out.Set("end", strconv.FormatInt(end.UnixNano(), 10))
		return out
	}
	const want = "the query time range exceeds the limit (query length: 721h0m0.001s, limit: 30d1h)"
	for _, r := range routes {
		t.Run(r.name+" over the default", func(t *testing.T) {
			params := window(r.params, 721*time.Hour+time.Millisecond)
			lokiStatus, lokiBody := rejectedQueryGet(t, lokiURL, r.path, params, "0", nil)
			if lokiStatus != http.StatusBadRequest || strings.TrimSpace(string(lokiBody)) != want {
				t.Fatalf("Loki fixture drifted: want 400 %q, got %d %.300s", want, lokiStatus, lokiBody)
			}
			proxyStatus, proxyBody := rejectedQueryGet(t, proxyVmauthURL, r.path, params, "0", nil)
			if proxyStatus != lokiStatus || string(proxyBody) != string(lokiBody) {
				t.Fatalf("proxy %d %q, Loki %d %q", proxyStatus, proxyBody, lokiStatus, lokiBody)
			}
		})
	}
	t.Run("labels at exactly the default", func(t *testing.T) {
		params := window(url.Values{}, 721*time.Hour)
		lokiStatus, lokiBody := rejectedQueryGet(t, lokiURL, "/loki/api/v1/labels", params, "0", nil)
		proxyStatus, proxyBody := rejectedQueryGet(t, proxyVmauthURL, "/loki/api/v1/labels", params, "0", nil)
		if lokiStatus != http.StatusOK || proxyStatus != http.StatusOK {
			t.Fatalf("a range of exactly 721h: Loki %d %.200s, proxy %d %.200s; want 200 on both", lokiStatus, lokiBody, proxyStatus, proxyBody)
		}
	})
	t.Run("published max_query_length", func(t *testing.T) {
		lokiDrilldown, lokiConfig := fetchPublishedLimits(t, lokiURL)
		drilldown, config := fetchPublishedLimits(t, proxyVmauthURL)
		if !sameLimit(drilldown["max_query_length"], lokiDrilldown["max_query_length"]) || !sameLimit(config["max_query_length"], lokiConfig["max_query_length"]) {
			t.Fatalf("max_query_length: proxy %v / %v, Loki %v / %v", drilldown["max_query_length"], config["max_query_length"], lokiDrilldown["max_query_length"], lokiConfig["max_query_length"])
		}
	})
}
