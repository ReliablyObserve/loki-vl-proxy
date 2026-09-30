//go:build e2e

package e2e_compat

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
)

// Proxies in the Loki-compatible profile the suites compare with Loki: the
// parity proxy and the two Grafana-facing ones (Explore and the Logs
// Drilldown default datasource).
func lokiProfileProxies() map[string]string {
	return map[string]string{
		"parity":            proxyURL,
		"grafana-explore":   proxyUnderscoreURL,
		"grafana-drilldown": patternsAutodetectProxyURL,
	}
}

// A dotted name anywhere in LogQL is Loki's 400 parse error, with the dot's
// column and the expected-token list of that parser state. Every
// Loki-compatible proxy must answer the same bytes, on every endpoint that
// parses LogQL; the OTel hybrid proxy keeps its documented extension and
// runs the query.
//
// conformance: profiles/dotted-name-parse-error, profiles/dotted-names-accepted-outside-loki-profile
func TestCompat_DottedNameParseErrorsMatchLoki(t *testing.T) {
	ensureDataIngested(t)
	end := time.Now()
	start := end.Add(-time.Hour)
	window := func(params url.Values) url.Values {
		out := cloneQueryValues(params)
		out.Set("start", strconv.FormatInt(start.UnixNano(), 10))
		out.Set("end", strconv.FormatInt(end.UnixNano(), 10))
		return out
	}
	for name, base := range map[string]string{"loki": lokiURL, "proxy": proxyURL} {
		status, body := rejectedQueryGet(t, base, "/loki/api/v1/labels", window(url.Values{"query": {`{app=~".+"}`}}), "0", nil)
		var labels struct {
			Data []string `json:"data"`
		}
		if status != http.StatusOK || json.Unmarshal(body, &labels) != nil || len(labels.Data) == 0 {
			t.Fatalf("%s is not serving data for a valid labels query: %d %s", name, status, body)
		}
	}

	owner := "{env=\"production\"} | json | pipeline=`metrics/prometheus` | k8s.namespace.name=`monitoring`"
	cases := []struct {
		name   string
		path   string
		params url.Values
	}{
		{"owner_label_filter", "/loki/api/v1/query_range", url.Values{"query": {owner}, "limit": {"10"}}},
		{"owner_log_volume", "/loki/api/v1/query_range", url.Values{"query": {`sum by (level) (count_over_time(` + owner + ` [1m]))`}, "step": {"60"}}},
		{"label_filter_after_selector", "/loki/api/v1/query_range", url.Values{"query": {`{app=~".+"} | k8s.pod.name="x"`}, "limit": {"10"}}},
		{"stream_matcher", "/loki/api/v1/query_range", url.Values{"query": {`{service.name="api"}`}, "limit": {"10"}}},
		{"by_list", "/loki/api/v1/query", url.Values{"query": {`sum by (k8s.namespace.name) (count_over_time({app=~".+"}[5m]))`}}},
		{"without_list", "/loki/api/v1/query", url.Values{"query": {`sum without (k8s.namespace.name) (count_over_time({app=~".+"}[5m]))`}}},
		{"on_list", "/loki/api/v1/query", url.Values{"query": {`sum by (app) (count_over_time({app=~".+"}[5m])) / on (a.b) sum by (app) (count_over_time({app=~".+"}[5m]))`}}},
		{"keep", "/loki/api/v1/query_range", url.Values{"query": {`{app=~".+"} | keep k8s.pod.name`}, "limit": {"10"}}},
		{"drop_matcher", "/loki/api/v1/query_range", url.Values{"query": {`{app=~".+"} | drop k8s.pod.name="x"`}, "limit": {"10"}}},
		{"label_format_destination", "/loki/api/v1/query_range", url.Values{"query": {`{app=~".+"} | label_format k8s.x=app`}, "limit": {"10"}}},
		{"json_parameter", "/loki/api/v1/query_range", url.Values{"query": {`{app=~".+"} | json k8s.pod.name`}, "limit": {"10"}}},
		{"unwrap", "/loki/api/v1/query", url.Values{"query": {`sum(sum_over_time({app=~".+"} | json | unwrap http.latency [5m]))`}}},
		{"unwrap_conversion", "/loki/api/v1/query", url.Values{"query": {`sum(sum_over_time({app=~".+"} | json | unwrap duration(http.latency) [5m]))`}}},
		{"name_then_number", "/loki/api/v1/query_range", url.Values{"query": {`{app=~".+"} | http.2xx="1"`}, "limit": {"10"}}},
		{"multiline", "/loki/api/v1/query_range", url.Values{"query": {"{app=~\".+\"}\n| json\n|   k8s.pod.name=\"x\""}, "limit": {"10"}}},
		{"labels_selector", "/loki/api/v1/labels", url.Values{"query": {`{k8s.namespace.name="monitoring"}`}}},
		{"label_values_selector", "/loki/api/v1/label/app/values", url.Values{"query": {`{k8s.namespace.name="monitoring"}`}}},
		{"series_match", "/loki/api/v1/series", url.Values{"match[]": {`{k8s.namespace.name="monitoring"}`}}},
		{"index_volume", "/loki/api/v1/index/volume", url.Values{"query": {`{k8s.namespace.name="monitoring"}`}}},
		{"detected_fields", "/loki/api/v1/detected_fields", url.Values{"query": {`{app=~".+"} | k8s.pod.name="x"`}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			lokiStatus, lokiBody := rejectedQueryGet(t, lokiURL, tc.path, window(tc.params), "0", nil)
			lokiMsg := strings.TrimSpace(string(lokiBody))
			if lokiStatus != http.StatusBadRequest || !strings.HasPrefix(lokiMsg, "parse error at line ") {
				t.Fatalf("Loki fixture drifted: want a 400 parse error, got %d %s", lokiStatus, lokiBody)
			}
			for name, base := range lokiProfileProxies() {
				status, body := rejectedQueryGet(t, base, tc.path, window(tc.params), "0", nil)
				var envelope struct {
					Status, ErrorType, Error string
				}
				if status != http.StatusBadRequest || json.Unmarshal(body, &envelope) != nil ||
					envelope.Status != "error" || envelope.ErrorType != "bad_data" || envelope.Error != lokiMsg {
					t.Errorf("%s proxy: %d %s, want 400 bad_data %q", name, status, body, lokiMsg)
				}
			}
		})
	}

	// The OTel hybrid profile exposes dotted field names, so it runs the
	// dotted spelling of a filter (a documented, profile-scoped extension).
	status, body := rejectedQueryGet(t, proxyOTelHybridURL, "/loki/api/v1/query_range",
		window(url.Values{"query": {`{app=~".+"} | k8s.pod.name="x"`}, "limit": {"10"}}), "0", nil)
	if status != http.StatusOK {
		t.Fatalf("OTel hybrid proxy rejected a dotted label filter: %d %s", status, body)
	}
}

// Loki reads only start, end and query on the label endpoints, so limit,
// offset and search change nothing there. The Grafana-facing
// Loki-compatible proxies (no -label-values-indexed-cache opt-in) must
// ignore them the same way.
//
// conformance: profiles/label-browse-params-ignored
func TestCompat_LabelBrowseParamsIgnoredLikeLoki(t *testing.T) {
	ensureDataIngested(t)
	end := time.Now()
	window := url.Values{
		"start": {strconv.FormatInt(end.Add(-time.Hour).UnixNano(), 10)},
		"end":   {strconv.FormatInt(end.UnixNano(), 10)},
	}
	get := func(base, path string, extra url.Values) []string {
		params := cloneQueryValues(window)
		for k, v := range extra {
			params[k] = v
		}
		status, body := rejectedQueryGet(t, base, path, params, "0", nil)
		var resp struct {
			Data []string `json:"data"`
		}
		if status != http.StatusOK || json.Unmarshal(body, &resp) != nil {
			t.Fatalf("GET %s%s?%s: %d %s", base, path, params.Encode(), status, body)
		}
		sort.Strings(resp.Data)
		return resp.Data
	}
	browse := url.Values{"limit": {"1"}, "offset": {"1"}, "search": {"zzz-no-match"}}
	for _, target := range []struct{ name, base string }{
		{"loki", lokiURL},
		{"grafana-explore", proxyUnderscoreURL},
		{"grafana-drilldown", patternsAutodetectProxyURL},
	} {
		for _, path := range []string{"/loki/api/v1/labels", "/loki/api/v1/label/app/values"} {
			plain := get(target.base, path, nil)
			if len(plain) < 2 {
				t.Fatalf("%s %s: need at least two entries to prove limit/offset are ignored, got %v", target.name, path, plain)
			}
			if withBrowse := get(target.base, path, browse); fmt.Sprint(withBrowse) != fmt.Sprint(plain) {
				t.Errorf("%s %s: limit/offset/search changed the answer: %d entries vs %d without", target.name, path, len(withBrowse), len(plain))
			}
		}
	}
}
