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

// TestCompat_RejectedQueryStatusParity pins Loki's status for queries that are
// invalid: 400 on every read endpoint, never a 5xx. Loki rejects the invalid
// regex selectors while parsing, and so does the proxy's selector validation
// on the metadata endpoints (VictoriaLogs would reject them too); the truncated
// selector is a parse error on the same endpoints. The metric
// query_range cases carry the headers Grafana sends: VictoriaLogs rejects their
// line-filter regex or pattern on stats_query_range (422), and Loki still
// answers Grafana with 400 rather than a partial-results reply.
// conformance: loki-compatible-profile
func TestCompat_RejectedQueryStatusParity(t *testing.T) {
	end := time.Now()
	start := end.Add(-time.Hour)
	window := func(params url.Values) url.Values {
		params.Set("start", strconv.FormatInt(start.UnixNano(), 10))
		params.Set("end", strconv.FormatInt(end.UnixNano(), 10))
		return params
	}

	// Both sides must answer a valid query before an error-status comparison
	// means anything.
	for name, base := range map[string]string{"loki": lokiURL, "proxy": proxyURL} {
		status, body := rejectedQueryGet(t, base, "/loki/api/v1/labels", window(url.Values{"query": {`{app="api-gateway"}`}}), "0", nil)
		if status != http.StatusOK {
			t.Fatalf("%s is not healthy for a valid labels query: %d %s", name, status, body)
		}
	}
	// The regex cases must be rejected by VictoriaLogs natively, otherwise they
	// would not exercise the backend-400 path.
	for _, logsql := range []string{`app:~"(a"`, `app:~"a{2,1}"`} {
		vlStatus, vlBody := rejectedQueryGet(t, vlURL, "/select/logsql/field_names", window(url.Values{"query": {logsql}}), "", nil)
		if vlStatus != http.StatusBadRequest {
			t.Fatalf("VictoriaLogs no longer rejects the regex fixture %s: %d %s", logsql, vlStatus, vlBody)
		}
	}
	for _, logsql := range []string{`_msg:~"a{2,1}" | stats count() c`, `* | extract "<_>" | stats count() c`} {
		vlStatus, vlBody := rejectedQueryGet(t, vlURL, "/select/logsql/stats_query_range", window(url.Values{"query": {logsql}, "step": {"60s"}}), "", nil)
		// The wording differs by version ("cannot parse `query` arg [...]" from
		// v1.48, "cannot parse query [...]" on v1.47); the test checks that the
		// proxy answers Loki's 400, not VictoriaLogs' text.
		if vlStatus != http.StatusUnprocessableEntity || !(strings.Contains(string(vlBody), "cannot parse `query` arg") || strings.Contains(string(vlBody), "cannot parse query [")) {
			t.Fatalf("VictoriaLogs no longer rejects the stats fixture %s with a parse error: %d %s", logsql, vlStatus, vlBody)
		}
	}
	// Headers the Grafana Loki datasource sends; the proxy detects Grafana by any of them.
	grafanaExplore := map[string]string{"User-Agent": "Grafana/13.0.1", "X-Grafana-Org-Id": "1", "X-Query-Tags": "Source=grafana"}
	grafanaDrilldown := map[string]string{"User-Agent": "Grafana/13.0.1", "X-Grafana-Org-Id": "1", "X-Query-Tags": "Source=grafana-lokiexplore-app"}
	const badLineRegexSum = `sum(count_over_time({app="api-gateway"} |~ "a{2,1}" [1m]))`
	const badPatternByApp = `sum by (app) (count_over_time({app="api-gateway"} | pattern "<_>" [1m]))`
	const badLineRegexRange = `count_over_time({app="api-gateway"} |~ "a{2,1}" [1m])`

	const badRegex = `{app=~"(a"}`
	const badRepeat = `{app=~"a{2,1}"}`
	const truncated = `{app=`
	cases := []struct {
		name    string
		path    string
		params  url.Values
		orgID   string
		headers map[string]string
	}{
		{"labels_backend_rejects_regex", "/loki/api/v1/labels", url.Values{"query": {badRegex}}, "0", nil},
		{"labels_backend_rejects_repeat", "/loki/api/v1/labels", url.Values{"query": {badRepeat}}, "0", nil},
		{"label_values_backend_rejects_regex", "/loki/api/v1/label/app/values", url.Values{"query": {badRegex}}, "0", nil},
		{"service_name_values_backend_rejects_regex", "/loki/api/v1/label/service_name/values", url.Values{"query": {badRegex}}, "0", nil},
		{"series_backend_rejects_regex", "/loki/api/v1/series", url.Values{"match[]": {badRegex}}, "0", nil},
		{"index_stats_backend_rejects_regex", "/loki/api/v1/index/stats", url.Values{"query": {badRegex}}, "0", nil},
		{"volume_backend_rejects_regex", "/loki/api/v1/index/volume", url.Values{"query": {badRegex}}, "0", nil},
		{"volume_range_backend_rejects_regex", "/loki/api/v1/index/volume_range", url.Values{"query": {badRegex}, "step": {"60"}}, "0", nil},
		{"detected_fields_backend_rejects_regex", "/loki/api/v1/detected_fields", url.Values{"query": {badRegex}}, "0", nil},
		{"detected_labels_backend_rejects_regex", "/loki/api/v1/detected_labels", url.Values{"query": {badRegex}}, "0", nil},
		{"detected_field_values_backend_rejects_regex", "/loki/api/v1/detected_field/level/values", url.Values{"query": {badRegex}}, "0", nil},
		{"patterns_backend_rejects_regex", "/loki/api/v1/patterns", url.Values{"query": {badRegex}, "step": {"60"}}, "0", nil},
		{"multi_tenant_labels_backend_rejects_regex", "/loki/api/v1/labels", url.Values{"query": {badRegex}}, "0|fake", nil},
		{"multi_tenant_series_backend_rejects_regex", "/loki/api/v1/series", url.Values{"match[]": {badRegex}}, "0|fake", nil},
		{"labels_truncated_selector", "/loki/api/v1/labels", url.Values{"query": {truncated}}, "0", nil},
		{"series_truncated_selector", "/loki/api/v1/series", url.Values{"match[]": {truncated}}, "0", nil},
		{"index_stats_truncated_selector", "/loki/api/v1/index/stats", url.Values{"query": {truncated}}, "0", nil},
		{"detected_fields_truncated_selector", "/loki/api/v1/detected_fields", url.Values{"query": {truncated}}, "0", nil},
		{"patterns_truncated_selector", "/loki/api/v1/patterns", url.Values{"query": {truncated}, "step": {"60"}}, "0", nil},
		{"query_range_api_backend_rejects_line_regex", "/loki/api/v1/query_range", url.Values{"query": {badLineRegexSum}, "step": {"60"}}, "0", nil},
		{"query_range_grafana_explore_backend_rejects_line_regex", "/loki/api/v1/query_range", url.Values{"query": {badLineRegexSum}, "step": {"60"}}, "0", grafanaExplore},
		{"query_range_grafana_drilldown_backend_rejects_line_regex", "/loki/api/v1/query_range", url.Values{"query": {badLineRegexSum}, "step": {"60"}}, "0", grafanaDrilldown},
		{"query_range_grafana_explore_backend_rejects_pattern", "/loki/api/v1/query_range", url.Values{"query": {badPatternByApp}, "step": {"60"}}, "0", grafanaExplore},
		{"query_range_grafana_drilldown_backend_rejects_pattern", "/loki/api/v1/query_range", url.Values{"query": {badPatternByApp}, "step": {"60"}}, "0", grafanaDrilldown},
		{"query_range_grafana_drilldown_backend_rejects_ungrouped_regex", "/loki/api/v1/query_range", url.Values{"query": {badLineRegexRange}, "step": {"60"}}, "0", grafanaDrilldown},
		// Queries Loki's parser rejects and the proxy's parser used to accept (semantics/proxy-accepts-invalid-logql):
		// the proxy answers Loki's own parse error text.
		{"parse_at_modifier", "/loki/api/v1/query_range", url.Values{"query": {`rate({app="api-gateway"}[5m] @ start())`}, "step": {"60"}}, "0", nil},
		{"parse_unknown_unit", "/loki/api/v1/query_range", url.Values{"query": {`rate({app="api-gateway"}[5minutes])`}, "step": {"60"}}, "0", nil},
		{"parse_range_grouping", "/loki/api/v1/query_range", url.Values{"query": {`count_over_time({app="api-gateway"}[5m]) by (level)`}, "step": {"60"}}, "0", nil},
		{"parse_missing_unwrap", "/loki/api/v1/query_range", url.Values{"query": {`sum_over_time({app="api-gateway"}[5m])`}, "step": {"60"}}, "0", nil},
		{"parse_refused_unwrap", "/loki/api/v1/query_range", url.Values{"query": {`count_over_time({app="api-gateway"} | logfmt | unwrap latency [5m])`}, "step": {"60"}}, "0", nil},
		{"parse_set_operator_literal", "/loki/api/v1/query", url.Values{"query": {`1 unless vector(2)`}}, "0", nil},
		{"parse_label_replace_regex", "/loki/api/v1/query_range", url.Values{"query": {`label_replace(rate({app="api-gateway"}[5m]), "d", "$1", "app", "(x")`}, "step": {"60"}}, "0", nil},
		{"parse_bytes_zero_b", "/loki/api/v1/query_range", url.Values{"query": {`{app="api-gateway"} | bytes < 0B`}}, "0", nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Loki is asked as a single tenant: whether the compose Loki accepts
			// multi-tenant org IDs must not decide the expected status. The body
			// must be Loki's query parse error, not an org-ID or other rejection.
			lokiOrgID := tc.orgID
			if strings.Contains(lokiOrgID, "|") {
				lokiOrgID = "0"
			}
			lokiStatus, lokiBody := rejectedQueryGet(t, lokiURL, tc.path, window(cloneQueryValues(tc.params)), lokiOrgID, tc.headers)
			if lokiStatus != http.StatusBadRequest || !strings.Contains(string(lokiBody), "parse error") {
				t.Fatalf("Loki fixture drifted: want 400 parse error, got %d %s", lokiStatus, lokiBody)
			}
			proxyStatus, proxyBody := rejectedQueryGet(t, proxyURL, tc.path, window(cloneQueryValues(tc.params)), tc.orgID, tc.headers)
			if proxyStatus != lokiStatus {
				t.Fatalf("status: proxy %d, Loki %d; proxy body %s", proxyStatus, lokiStatus, proxyBody)
			}
			// Loki writes the message alone as text/plain, and so does the proxy.
			text := string(proxyBody)
			if text == "" || strings.HasPrefix(text, "{") {
				t.Fatalf("proxy error body %q, want Loki's message as text", proxyBody)
			}
			if strings.HasPrefix(tc.name, "parse_") && strings.TrimSpace(text) != strings.TrimSpace(string(lokiBody)) {
				t.Fatalf("proxy error %q, Loki %q", text, lokiBody)
			}
			// A multi-tenant 400 must be the query's rejection, not a tenant or
			// fanout error: the single-tenant error, with no VictoriaLogs text.
			if strings.Contains(tc.orgID, "|") {
				_, singleBody := rejectedQueryGet(t, proxyURL, tc.path, window(cloneQueryValues(tc.params)), "0", tc.headers)
				if text != string(singleBody) || strings.Contains(text, "cannot parse") {
					t.Fatalf("multi-tenant error %q is not the single-tenant error %q, or carries VictoriaLogs text", text, singleBody)
				}
			}
		})
	}
}

func cloneQueryValues(values url.Values) url.Values {
	out := make(url.Values, len(values))
	for key, items := range values {
		out[key] = append([]string(nil), items...)
	}
	return out
}

func rejectedQueryGet(t *testing.T, base, path string, params url.Values, orgID string, extra map[string]string) (int, []byte) {
	t.Helper()
	headers := map[string]string{}
	for key, value := range extra {
		headers[key] = value
	}
	if orgID != "" {
		headers["X-Scope-OrgID"] = orgID
	}
	return hardeningRequest(t, http.MethodGet, base+path+"?"+params.Encode(), "", headers)
}

// TestCompat_VectorAggregationOverLogSelector: Loki's grammar takes only a
// metric expression inside a vector aggregation (pkg/logql/syntax/syntax.y:176-185),
// so topk, bottomk, sort, sort_desc, stddev and sum over a log selector (with
// or without a pipeline) fail with "parse error at line 1, col N: syntax
// error: unexpected )". The proxy answers the same status and bytes on
// query_range and query; it answered query_range with a 500.
// conformance: semantics/vector-aggregation-over-log-selector-status, status-400, loki_api_v1_query_range, loki_api_v1_query
func TestCompat_VectorAggregationOverLogSelector(t *testing.T) {
	end := time.Now()
	start := end.Add(-time.Hour)
	for _, query := range []string{
		`topk(2, {app="api-gateway",env="production"})`,
		`bottomk(1, {app="api-gateway",env="production"})`,
		`sort({app="api-gateway"})`,
		`sort_desc({app="api-gateway",env="production"})`,
		`stddev({namespace="prod"})`,
		`sum by (level) ({app="api-gateway"} | json)`,
		"sum(\n  {app=\"api-gateway\"}\n)",
	} {
		for _, route := range []string{"query_range", "query"} {
			t.Run(route+" "+query, func(t *testing.T) {
				params := url.Values{"query": {query}}
				if route == "query" {
					params.Set("time", strconv.FormatInt(end.UnixNano(), 10))
				} else {
					params.Set("start", strconv.FormatInt(start.UnixNano(), 10))
					params.Set("end", strconv.FormatInt(end.UnixNano(), 10))
					params.Set("step", "60")
				}
				lokiStatus, lokiBody := rejectedQueryGet(t, lokiURL, "/loki/api/v1/"+route, cloneQueryValues(params), "0", nil)
				if lokiStatus != http.StatusBadRequest || !strings.HasPrefix(string(lokiBody), "parse error at line ") {
					t.Fatalf("Loki fixture drifted: want a 400 parse error, got %d %s", lokiStatus, lokiBody)
				}
				proxyStatus, proxyBody := rejectedQueryGet(t, proxyURL, "/loki/api/v1/"+route, cloneQueryValues(params), "0", nil)
				if proxyStatus != lokiStatus || strings.TrimSpace(string(proxyBody)) != strings.TrimSpace(string(lokiBody)) {
					t.Fatalf("proxy %d %q, Loki %d %q", proxyStatus, proxyBody, lokiStatus, lokiBody)
				}
			})
		}
	}
}
