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
// covers (the text the route reports: index/volume rounds its start and end
// down to whole seconds first, so it reports 721h0m1s where the others report
// 721h0m0.001s); a range of exactly 721h runs on both; index/volume_range is
// not length-limited on either side; both publish the same max_query_length.
//
// conformance: limits/max-query-length-default, limits/max-query-length-volume-whole-seconds, limits/max-query-length-volume-range-exempt, tenant-query-limits
func TestDefaultMaxQueryLengthLikeLoki(t *testing.T) {
	ensureDataIngested(t)
	waitForReady(t, proxyVmauthURL+"/ready", 30*time.Second)
	end := time.Now().Truncate(time.Second)
	sel := `{app="api-gateway"}`
	const ms = "721h0m0.001s"
	routes := []struct {
		name, path string
		params     url.Values
		want       string // the length Loki reports for a range 1 ms over 721h
	}{
		{"query_range", "/loki/api/v1/query_range", url.Values{"query": {sel}, "limit": {"10"}, "step": {"3600"}}, ms},
		{"labels", "/loki/api/v1/labels", url.Values{}, ms},
		{"label_values", "/loki/api/v1/label/app/values", url.Values{}, ms},
		{"series", "/loki/api/v1/series", url.Values{"match[]": {sel}}, ms},
		{"detected_labels", "/loki/api/v1/detected_labels", url.Values{"query": {sel}}, ms},
		{"detected_fields", "/loki/api/v1/detected_fields", url.Values{"query": {sel}}, ms},
		{"index_stats", "/loki/api/v1/index/stats", url.Values{"query": {sel}}, ms},
		{"index_volume", "/loki/api/v1/index/volume", url.Values{"query": {sel}}, "721h0m1s"},
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
	for _, r := range routes {
		t.Run(r.name+" over the default", func(t *testing.T) {
			want := "the query time range exceeds the limit (query length: " + r.want + ", limit: 30d1h)"
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
	// Loki rounds the start and end of index/volume down to whole seconds before
	// the length check: with the end at .5 s a start 0.4 s over 721h before it
	// is exactly 721h once rounded, and runs.
	t.Run("index_volume 200 boundary at a fractional end", func(t *testing.T) {
		fractionalEnd := end.Add(500 * time.Millisecond)
		for _, over := range []time.Duration{0, 400 * time.Millisecond} {
			params := url.Values{
				"query": {sel},
				"start": {strconv.FormatInt(fractionalEnd.Add(-721*time.Hour-over).UnixNano(), 10)},
				"end":   {strconv.FormatInt(fractionalEnd.UnixNano(), 10)},
			}
			lokiStatus, lokiBody := rejectedQueryGet(t, lokiURL, "/loki/api/v1/index/volume", params, "0", nil)
			proxyStatus, proxyBody := rejectedQueryGet(t, proxyVmauthURL, "/loki/api/v1/index/volume", params, "0", nil)
			if lokiStatus != http.StatusOK || proxyStatus != http.StatusOK {
				t.Fatalf("721h+%s ending at .5 s: Loki %d %.200s, proxy %d %.200s; want 200 on both", over, lokiStatus, lokiBody, proxyStatus, proxyBody)
			}
		}
		// 0.6 s over rounds to a whole second over: both reject with the same whole-second text.
		params := url.Values{
			"query": {sel},
			"start": {strconv.FormatInt(fractionalEnd.Add(-721*time.Hour-600*time.Millisecond).UnixNano(), 10)},
			"end":   {strconv.FormatInt(fractionalEnd.UnixNano(), 10)},
		}
		lokiStatus, lokiBody := rejectedQueryGet(t, lokiURL, "/loki/api/v1/index/volume", params, "0", nil)
		proxyStatus, proxyBody := rejectedQueryGet(t, proxyVmauthURL, "/loki/api/v1/index/volume", params, "0", nil)
		if lokiStatus != http.StatusBadRequest || proxyStatus != lokiStatus || string(proxyBody) != string(lokiBody) {
			t.Fatalf("721h+0.6s ending at .5 s: proxy %d %.200q, Loki %d %.200q; want the same 400", proxyStatus, proxyBody, lokiStatus, lokiBody)
		}
	})
	// Loki applies no max_query_length to index/volume_range: 722h to 10000h at
	// a one hour step are answered; only its resolution check (11,000 points)
	// rejects, at 20000h, with the same text on both sides.
	t.Run("index_volume_range is not length-limited", func(t *testing.T) {
		params := url.Values{"query": {sel}, "step": {"3600"}}
		for _, length := range []time.Duration{722 * time.Hour, 1000 * time.Hour, 5000 * time.Hour, 10000 * time.Hour, 20000 * time.Hour} {
			p := window(params, length)
			lokiStatus, lokiBody := rejectedQueryGet(t, lokiURL, "/loki/api/v1/index/volume_range", p, "0", nil)
			proxyStatus, proxyBody := rejectedQueryGet(t, proxyVmauthURL, "/loki/api/v1/index/volume_range", p, "0", nil)
			wantStatus := http.StatusOK
			if length >= 20000*time.Hour {
				wantStatus = http.StatusBadRequest
			}
			if lokiStatus != wantStatus {
				t.Fatalf("Loki fixture drifted at %s: want %d, got %d %.300s", length, wantStatus, lokiStatus, lokiBody)
			}
			if proxyStatus != lokiStatus || (lokiStatus != http.StatusOK && string(proxyBody) != string(lokiBody)) {
				t.Fatalf("index/volume_range at %s: proxy %d %.300q, Loki %d %.300q", length, proxyStatus, proxyBody, lokiStatus, lokiBody)
			}
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
