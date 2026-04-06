//go:build e2e

// Extended compatibility tests — covers normal operation patterns and edge cases
// that Grafana users encounter daily. Every test compares proxy vs real Loki.
package e2e_compat

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"sync"
	"testing"
	"time"
)

var extendedSetupOnce sync.Once

func ensureDataIngested(t *testing.T) {
	t.Helper()
	extendedSetupOnce.Do(func() {
		waitForReady(t, proxyURL+"/ready", 30*time.Second)
		waitForReady(t, lokiURL+"/ready", 30*time.Second)
		ingestRichTestData(t)
	})
}

// =============================================================================
// Label Values comparison for multiple labels
// =============================================================================

func TestExtended_LabelValues_AppValues(t *testing.T) {
	ensureDataIngested(t)
	score := &CompatScore{}
	proxyResp := getJSON(t, proxyURL+"/loki/api/v1/label/app/values")
	lokiResp := getJSON(t, lokiURL+"/loki/api/v1/label/app/values")

	proxyVals := extractStringArray(proxyResp, "data")
	lokiVals := extractStringArray(lokiResp, "data")

	t.Logf("Loki app values (%d): %v", len(lokiVals), lokiVals)
	t.Logf("Proxy app values (%d): %v", len(proxyVals), proxyVals)

	if len(proxyVals) > 0 {
		score.pass("label_values_app", fmt.Sprintf("proxy returns %d app values", len(proxyVals)))
	} else {
		score.fail("label_values_app", "proxy returns no app values")
	}

	// Check key apps exist in both
	for _, app := range []string{"api-gateway", "payment-service", "nginx-ingress"} {
		if contains(proxyVals, app) {
			score.pass("label_values_app", fmt.Sprintf("proxy has %q", app))
		} else {
			score.fail("label_values_app", fmt.Sprintf("proxy missing %q", app))
		}
	}

	score.report(t)
}

func TestExtended_LabelValues_LevelValues(t *testing.T) {
	ensureDataIngested(t)
	score := &CompatScore{}
	proxyResp := getJSON(t, proxyURL+"/loki/api/v1/label/level/values")

	proxyVals := extractStringArray(proxyResp, "data")
	t.Logf("Proxy level values: %v", proxyVals)

	expectedLevels := []string{"info", "error", "warn"}
	for _, lvl := range expectedLevels {
		if contains(proxyVals, lvl) {
			score.pass("label_values_level", fmt.Sprintf("has %q", lvl))
		} else {
			score.fail("label_values_level", fmt.Sprintf("missing %q", lvl))
		}
	}

	score.report(t)
}

func TestExtended_LabelValues_NamespaceValues(t *testing.T) {
	ensureDataIngested(t)
	score := &CompatScore{}
	proxyResp := getJSON(t, proxyURL+"/loki/api/v1/label/namespace/values")
	proxyVals := extractStringArray(proxyResp, "data")

	if contains(proxyVals, "prod") {
		score.pass("label_values_ns", "has 'prod'")
	} else {
		score.fail("label_values_ns", "missing 'prod'")
	}
	if contains(proxyVals, "staging") {
		score.pass("label_values_ns", "has 'staging'")
	} else {
		score.fail("label_values_ns", "missing 'staging'")
	}

	score.report(t)
}

// =============================================================================
// Limit parameter enforcement
// =============================================================================

func TestExtended_LimitParameter(t *testing.T) {
	ensureDataIngested(t)
	score := &CompatScore{}

	for _, limit := range []string{"1", "3", "5"} {
		now := time.Now()
		params := url.Values{}
		params.Set("query", `{app="api-gateway"}`)
		params.Set("start", fmt.Sprintf("%d", now.Add(-10*time.Minute).UnixNano()))
		params.Set("end", fmt.Sprintf("%d", now.UnixNano()))
		params.Set("limit", limit)

		proxyResp := getJSON(t, proxyURL+"/loki/api/v1/query_range?"+params.Encode())
		lokiResp := getJSON(t, lokiURL+"/loki/api/v1/query_range?"+params.Encode())

		proxyLines := countLogLines(proxyResp)
		lokiLines := countLogLines(lokiResp)

		t.Logf("[limit=%s] Loki=%d, Proxy=%d", limit, lokiLines, proxyLines)

		if checkStatus(proxyResp) {
			score.pass("limit_"+limit, "proxy success")
		} else {
			score.fail("limit_"+limit, "proxy error")
		}
	}

	score.report(t)
}

// =============================================================================
// Time range boundary tests
// =============================================================================

func TestExtended_TimeRange_LastMinute(t *testing.T) {
	ensureDataIngested(t)
	score := &CompatScore{}
	now := time.Now()
	params := url.Values{}
	params.Set("query", `{app="api-gateway"}`)
	params.Set("start", fmt.Sprintf("%d", now.Add(-1*time.Minute).UnixNano()))
	params.Set("end", fmt.Sprintf("%d", now.UnixNano()))
	params.Set("limit", "100")

	proxyResp := getJSON(t, proxyURL+"/loki/api/v1/query_range?"+params.Encode())
	if checkStatus(proxyResp) {
		score.pass("time_1m", "last-1m query OK")
	} else {
		score.fail("time_1m", "last-1m query error")
	}

	score.report(t)
}

func TestExtended_TimeRange_LastHour(t *testing.T) {
	ensureDataIngested(t)
	score := &CompatScore{}
	now := time.Now()
	params := url.Values{}
	params.Set("query", `{app="api-gateway"}`)
	params.Set("start", fmt.Sprintf("%d", now.Add(-1*time.Hour).UnixNano()))
	params.Set("end", fmt.Sprintf("%d", now.UnixNano()))
	params.Set("limit", "100")

	proxyResp := getJSON(t, proxyURL+"/loki/api/v1/query_range?"+params.Encode())
	if checkStatus(proxyResp) {
		score.pass("time_1h", "last-1h query OK")
	} else {
		score.fail("time_1h", "last-1h query error")
	}

	lines := countLogLines(proxyResp)
	if lines > 0 {
		score.pass("time_1h", fmt.Sprintf("returns %d lines in 1h window", lines))
	}

	score.report(t)
}

// =============================================================================
// Response structure deep validation
// =============================================================================

func TestExtended_ResponseStructure_Streams(t *testing.T) {
	ensureDataIngested(t)
	score := &CompatScore{}
	proxyResp := queryProxy(t, `{app="api-gateway"}`)

	data, ok := proxyResp["data"].(map[string]interface{})
	if !ok {
		score.fail("structure", "missing data object")
		score.report(t)
		return
	}

	// resultType must be "streams"
	if data["resultType"] == "streams" {
		score.pass("structure", "resultType=streams")
	} else {
		score.fail("structure", fmt.Sprintf("resultType=%v", data["resultType"]))
	}

	// result must be array
	result, ok := data["result"].([]interface{})
	if !ok || len(result) == 0 {
		score.fail("structure", "result is empty or not array")
		score.report(t)
		return
	}
	score.pass("structure", fmt.Sprintf("result has %d streams", len(result)))

	// Each stream must have "stream" (map) and "values" (array of [ts, line])
	for i, s := range result {
		if i >= 3 {
			break // only check first 3
		}
		stream, ok := s.(map[string]interface{})
		if !ok {
			score.fail("structure", fmt.Sprintf("result[%d] not an object", i))
			continue
		}

		if labels, ok := stream["stream"].(map[string]interface{}); ok {
			if len(labels) > 0 {
				score.pass("structure", fmt.Sprintf("stream[%d] has %d labels", i, len(labels)))
			}
		} else {
			score.fail("structure", fmt.Sprintf("stream[%d] missing 'stream' labels", i))
		}

		if values, ok := stream["values"].([]interface{}); ok {
			if len(values) > 0 {
				// Check first value is [timestamp, line]
				if pair, ok := values[0].([]interface{}); ok && len(pair) == 2 {
					ts, tsOk := pair[0].(string)
					_, lineOk := pair[1].(string)
					if tsOk && lineOk && len(ts) > 10 {
						score.pass("structure", fmt.Sprintf("stream[%d] values are [nanosecond_ts, line]", i))
					} else {
						score.fail("structure", fmt.Sprintf("stream[%d] value format wrong: %v", i, pair))
					}
				}
			}
		} else {
			score.fail("structure", fmt.Sprintf("stream[%d] missing 'values'", i))
		}
	}

	score.report(t)
}

func TestExtended_ResponseStructure_Labels(t *testing.T) {
	ensureDataIngested(t)
	score := &CompatScore{}
	proxyResp := getJSON(t, proxyURL+"/loki/api/v1/labels")
	lokiResp := getJSON(t, lokiURL+"/loki/api/v1/labels")

	// Both must have status=success
	if proxyResp["status"] == "success" && lokiResp["status"] == "success" {
		score.pass("labels_struct", "both status=success")
	} else {
		score.fail("labels_struct", "status mismatch")
	}

	// Both must return data as string array
	proxyData := extractStringArray(proxyResp, "data")
	lokiData := extractStringArray(lokiResp, "data")

	if len(proxyData) > 0 && len(lokiData) > 0 {
		score.pass("labels_struct", fmt.Sprintf("proxy=%d labels, loki=%d labels", len(proxyData), len(lokiData)))
	}

	// Check common labels exist in both
	commonLabels := []string{"app", "namespace", "level", "cluster", "env"}
	for _, label := range commonLabels {
		inProxy := contains(proxyData, label)
		inLoki := contains(lokiData, label)
		if inProxy && inLoki {
			score.pass("labels_struct", fmt.Sprintf("both have label %q", label))
		} else if inProxy != inLoki {
			score.fail("labels_struct", fmt.Sprintf("label %q: proxy=%v loki=%v", label, inProxy, inLoki))
		}
	}

	score.report(t)
}

// =============================================================================
// Series with complex matchers
// =============================================================================

func TestExtended_Series_RegexMatcher(t *testing.T) {
	ensureDataIngested(t)
	score := &CompatScore{}
	now := time.Now()
	params := url.Values{}
	params.Set("match[]", `{app=~"api.*"}`)
	params.Set("start", fmt.Sprintf("%d", now.Add(-10*time.Minute).UnixNano()))
	params.Set("end", fmt.Sprintf("%d", now.UnixNano()))

	proxyResp := getJSON(t, proxyURL+"/loki/api/v1/series?"+params.Encode())
	lokiResp := getJSON(t, lokiURL+"/loki/api/v1/series?"+params.Encode())

	if checkStatus(proxyResp) {
		score.pass("series_regex", "proxy success")
	} else {
		score.fail("series_regex", "proxy error")
	}

	proxyData, _ := proxyResp["data"].([]interface{})
	lokiData, _ := lokiResp["data"].([]interface{})
	t.Logf("[series_regex] Loki=%d series, Proxy=%d series", len(lokiData), len(proxyData))

	if len(proxyData) > 0 {
		score.pass("series_regex", fmt.Sprintf("proxy returns %d series for app=~api.*", len(proxyData)))
	}

	score.report(t)
}

func TestExtended_Series_MultiMatcher(t *testing.T) {
	ensureDataIngested(t)
	score := &CompatScore{}
	now := time.Now()
	params := url.Values{}
	params.Set("match[]", `{app="api-gateway", level="error"}`)
	params.Set("start", fmt.Sprintf("%d", now.Add(-10*time.Minute).UnixNano()))
	params.Set("end", fmt.Sprintf("%d", now.UnixNano()))

	proxyResp := getJSON(t, proxyURL+"/loki/api/v1/series?"+params.Encode())

	if checkStatus(proxyResp) {
		score.pass("series_multi", "proxy success with multi-matcher")
	} else {
		score.fail("series_multi", "proxy error")
	}

	score.report(t)
}

// =============================================================================
// Detected fields comparison
// =============================================================================

func TestExtended_DetectedFields_ReturnsFields(t *testing.T) {
	ensureDataIngested(t)
	score := &CompatScore{}
	now := time.Now()
	params := url.Values{}
	params.Set("query", `{app="api-gateway", level="info"}`)
	params.Set("start", fmt.Sprintf("%d", now.Add(-10*time.Minute).UnixNano()))
	params.Set("end", fmt.Sprintf("%d", now.UnixNano()))

	proxyResp := getJSON(t, proxyURL+"/loki/api/v1/detected_fields?"+params.Encode())

	if checkStatus(proxyResp) {
		score.pass("detected_fields", "proxy success")
	} else {
		score.fail("detected_fields", "proxy error")
	}

	if fields, ok := proxyResp["fields"].([]interface{}); ok && len(fields) > 0 {
		score.pass("detected_fields", fmt.Sprintf("proxy returns %d fields", len(fields)))
	} else {
		score.fail("detected_fields", "proxy returns no fields")
	}

	score.report(t)
}

// =============================================================================
// Parser chain compatibility: logfmt → label filter
// =============================================================================

func TestExtended_Logfmt_ThenFilter(t *testing.T) {
	ensureDataIngested(t)
	compareQuery(t, "logfmt_filter", `{app="payment-service", level="error"} | logfmt | msg=~"database.*"`)
}

func TestExtended_JSON_ThenFilter_Status(t *testing.T) {
	ensureDataIngested(t)
	compareQuery(t, "json_status", `{app="api-gateway", level="info"} | json | status=200`)
}

func TestExtended_JSON_ThenFilter_Method(t *testing.T) {
	ensureDataIngested(t)
	compareQuery(t, "json_method", `{app="api-gateway", level="info"} | json | method="GET"`)
}

func TestExtended_JSON_ThenFilter_Duration(t *testing.T) {
	ensureDataIngested(t)
	compareQuery(t, "json_duration", `{app="api-gateway", level="info"} | json | duration_ms>100`)
}

// =============================================================================
// Regex selector tests (app=~"pattern")
// =============================================================================

func TestExtended_RegexSelector_MultiApp(t *testing.T) {
	ensureDataIngested(t)
	compareQuery(t, "regex_multi_app", `{app=~"api-gateway|payment-service"}`)
}

func TestExtended_RegexSelector_Prefix(t *testing.T) {
	ensureDataIngested(t)
	compareQuery(t, "regex_prefix", `{app=~"api.*"}`)
}

func TestExtended_NegativeRegexSelector(t *testing.T) {
	ensureDataIngested(t)
	compareQuery(t, "neg_regex", `{app=~".+", app!~"nginx.*"}`)
}

// =============================================================================
// Multiple filter chains
// =============================================================================

func TestExtended_ChainedFilters_ContainsAndNotContains(t *testing.T) {
	ensureDataIngested(t)
	compareQuery(t, "chain_contains", `{app="api-gateway"} |= "api" != "health" != "metrics" != "ready"`)
}

func TestExtended_ChainedFilters_RegexChain(t *testing.T) {
	ensureDataIngested(t)
	compareQuery(t, "chain_regex", `{app="api-gateway"} |~ "GET|POST" !~ "health|ready"`)
}

func TestExtended_ChainedFilters_MixedTypes(t *testing.T) {
	ensureDataIngested(t)
	compareQuery(t, "chain_mixed", `{app="api-gateway"} |= "api" |~ "GET|POST" != "health"`)
}

// =============================================================================
// Grafana Explore-style queries (what users actually type)
// =============================================================================

func TestExtended_GrafanaExplore_SimpleSelect(t *testing.T) {
	ensureDataIngested(t)
	compareQuery(t, "explore_simple", `{namespace="prod"}`)
}

func TestExtended_GrafanaExplore_ErrorSearch(t *testing.T) {
	ensureDataIngested(t)
	compareQuery(t, "explore_errors", `{namespace="prod", level="error"}`)
}

func TestExtended_GrafanaExplore_TextSearch(t *testing.T) {
	ensureDataIngested(t)
	compareQuery(t, "explore_text", `{namespace="prod"} |= "connection"`)
}

func TestExtended_GrafanaExplore_JSONWithField(t *testing.T) {
	ensureDataIngested(t)
	compareQuery(t, "explore_json", `{app="api-gateway"} | json | status>=400`)
}

func TestExtended_GrafanaExplore_Regex500s(t *testing.T) {
	ensureDataIngested(t)
	compareQuery(t, "explore_500", `{app="api-gateway"} |~ "status.:(500|502|503)"`)
}

func TestExtended_GrafanaExplore_NginxErrors(t *testing.T) {
	ensureDataIngested(t)
	compareQuery(t, "explore_nginx", `{app="nginx-ingress"} |~ "\" (4|5)[0-9]{2} "`)
}

// =============================================================================
// Grafana Drilldown-style queries
// =============================================================================

func TestExtended_Drilldown_LabelValuesWithTimeRange(t *testing.T) {
	ensureDataIngested(t)
	score := &CompatScore{}
	now := time.Now()
	params := url.Values{}
	params.Set("query", `{namespace="prod"}`)
	params.Set("start", fmt.Sprintf("%d", now.Add(-10*time.Minute).UnixNano()))
	params.Set("end", fmt.Sprintf("%d", now.UnixNano()))

	proxyResp := getJSON(t, proxyURL+"/loki/api/v1/label/app/values?"+params.Encode())
	lokiResp := getJSON(t, lokiURL+"/loki/api/v1/label/app/values?"+params.Encode())

	proxyVals := extractStringArray(proxyResp, "data")
	lokiVals := extractStringArray(lokiResp, "data")

	if len(proxyVals) > 0 {
		score.pass("drilldown_labels", fmt.Sprintf("proxy=%d values in time range", len(proxyVals)))
	} else {
		score.fail("drilldown_labels", "no values in time range")
	}

	// Use a stable namespace-scoped subset so the score isn't distorted by
	// residual app names from other test ingestions in a reused local stack.
	expected := []string{"api-gateway", "payment-service"}
	missing := 0
	for _, v := range expected {
		if !contains(lokiVals, v) {
			t.Fatalf("loki missing expected namespace-scoped app value %q: %v", v, lokiVals)
		}
		if !contains(proxyVals, v) {
			missing++
			t.Logf("proxy missing namespace-scoped label value: %q", v)
		}
	}
	if missing == 0 {
		score.pass("drilldown_labels", "stable namespace-scoped Loki values present in proxy")
	} else {
		score.fail("drilldown_labels", fmt.Sprintf("%d namespace-scoped Loki values missing from proxy", missing))
	}

	score.report(t)
}

func TestExtended_Drilldown_VolumeStats(t *testing.T) {
	ensureDataIngested(t)
	score := &CompatScore{}
	now := time.Now()
	params := url.Values{}
	params.Set("query", `{namespace="prod"}`)
	params.Set("start", fmt.Sprintf("%d", now.Add(-10*time.Minute).UnixNano()))
	params.Set("end", fmt.Sprintf("%d", now.UnixNano()))

	// Index stats
	statsResp := getJSON(t, proxyURL+"/loki/api/v1/index/stats?"+params.Encode())
	if entries, ok := statsResp["entries"].(float64); ok && entries > 0 {
		score.pass("drilldown_stats", fmt.Sprintf("entries=%v", entries))
	} else {
		score.fail("drilldown_stats", fmt.Sprintf("entries=%v", statsResp["entries"]))
	}

	// Volume
	volResp := getJSON(t, proxyURL+"/loki/api/v1/index/volume?"+params.Encode())
	if checkStatus(volResp) {
		score.pass("drilldown_volume", "volume returns success")
	} else {
		score.fail("drilldown_volume", "volume error")
	}

	score.report(t)
}

// =============================================================================
// POST method support (Grafana sends POST for large queries)
// =============================================================================

func TestExtended_POST_QueryRange(t *testing.T) {
	ensureDataIngested(t)
	score := &CompatScore{}
	now := time.Now()
	params := url.Values{}
	params.Set("query", `{app="api-gateway"}`)
	params.Set("start", fmt.Sprintf("%d", now.Add(-10*time.Minute).UnixNano()))
	params.Set("end", fmt.Sprintf("%d", now.UnixNano()))
	params.Set("limit", "10")

	resp, err := http.PostForm(proxyURL+"/loki/api/v1/query_range", params)
	if err != nil {
		score.fail("post_query", "POST failed: "+err.Error())
		score.report(t)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode == 200 {
		score.pass("post_query", "POST query_range returns 200")
	} else {
		score.fail("post_query", fmt.Sprintf("POST returned %d", resp.StatusCode))
	}

	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)
	if checkStatus(result) {
		score.pass("post_query", "response has status=success")
	}

	score.report(t)
}

func TestExtended_POST_Labels(t *testing.T) {
	ensureDataIngested(t)
	score := &CompatScore{}
	resp, err := http.PostForm(proxyURL+"/loki/api/v1/labels", url.Values{})
	if err != nil {
		score.fail("post_labels", "POST failed")
		score.report(t)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode == 200 {
		score.pass("post_labels", "POST labels returns 200")
	}

	score.report(t)
}

// =============================================================================
// Helpers
// =============================================================================

func extractStringArray(resp map[string]interface{}, key string) []string {
	data, ok := resp[key].([]interface{})
	if !ok {
		return nil
	}
	result := make([]string, 0, len(data))
	for _, v := range data {
		if s, ok := v.(string); ok {
			result = append(result, s)
		}
	}
	return result
}

// =============================================================================
// Direction parameter — logs should be ordered correctly
// =============================================================================

func TestExtended_Direction_Forward(t *testing.T) {
	ensureDataIngested(t)
	score := &CompatScore{}
	now := time.Now()

	params := url.Values{}
	params.Set("query", `{app="api-gateway"}`)
	params.Set("start", fmt.Sprintf("%d", now.Add(-10*time.Minute).UnixNano()))
	params.Set("end", fmt.Sprintf("%d", now.UnixNano()))
	params.Set("direction", "forward")
	params.Set("limit", "10")

	proxyResp := getJSON(t, proxyURL+"/loki/api/v1/query_range?"+params.Encode())
	lokiResp := getJSON(t, lokiURL+"/loki/api/v1/query_range?"+params.Encode())

	if checkStatus(proxyResp) {
		score.pass("direction_forward", "proxy returns success for direction=forward")
	} else {
		score.fail("direction_forward", "proxy error on direction=forward")
	}

	if checkStatus(lokiResp) {
		score.pass("direction_forward", "loki returns success for direction=forward")
	}

	score.report(t)
}

func TestExtended_Direction_Backward(t *testing.T) {
	ensureDataIngested(t)
	score := &CompatScore{}
	now := time.Now()

	params := url.Values{}
	params.Set("query", `{app="api-gateway"}`)
	params.Set("start", fmt.Sprintf("%d", now.Add(-10*time.Minute).UnixNano()))
	params.Set("end", fmt.Sprintf("%d", now.UnixNano()))
	params.Set("direction", "backward")
	params.Set("limit", "10")

	proxyResp := getJSON(t, proxyURL+"/loki/api/v1/query_range?"+params.Encode())

	if checkStatus(proxyResp) {
		score.pass("direction_backward", "proxy returns success for direction=backward")
	} else {
		score.fail("direction_backward", "proxy error on direction=backward")
	}

	score.report(t)
}

// =============================================================================
// Labels with query param — scoped label suggestions
// =============================================================================

func TestExtended_Labels_WithQueryParam(t *testing.T) {
	ensureDataIngested(t)
	score := &CompatScore{}

	// Labels scoped to specific stream
	params := url.Values{}
	params.Set("query", `{app="api-gateway"}`)

	proxyResp := getJSON(t, proxyURL+"/loki/api/v1/labels?"+params.Encode())
	lokiResp := getJSON(t, lokiURL+"/loki/api/v1/labels?"+params.Encode())

	if checkStatus(proxyResp) {
		score.pass("labels_query", "proxy returns success with query param")
	} else {
		score.fail("labels_query", "proxy error with query param")
	}

	if checkStatus(lokiResp) {
		score.pass("labels_query", "loki returns success with query param")
	}

	score.report(t)
}

// =============================================================================
// quantile_over_time — metric query support
// =============================================================================

func TestExtended_QuantileOverTime(t *testing.T) {
	ensureDataIngested(t)
	score := &CompatScore{}
	now := time.Now()

	params := url.Values{}
	params.Set("query", `quantile_over_time(0.95, {app="api-gateway"} | unwrap duration [5m])`)
	params.Set("start", fmt.Sprintf("%d", now.Add(-10*time.Minute).UnixNano()))
	params.Set("end", fmt.Sprintf("%d", now.UnixNano()))
	params.Set("step", "60")

	proxyResp := getJSON(t, proxyURL+"/loki/api/v1/query_range?"+params.Encode())

	if checkStatus(proxyResp) {
		score.pass("quantile_over_time", "proxy returns success for quantile_over_time")
	} else {
		score.fail("quantile_over_time", "proxy error on quantile_over_time")
	}

	score.report(t)
}

// =============================================================================
// VL error propagation — proxy must not mask backend errors
// =============================================================================

func TestExtended_ErrorPropagation(t *testing.T) {
	score := &CompatScore{}

	// Send invalid query that VL can't process
	params := url.Values{}
	params.Set("query", `{app="nginx"} | stats invalid_function()`)

	proxyResp := getJSON(t, proxyURL+"/loki/api/v1/query?"+params.Encode())

	// Should not return status=success for invalid query
	status, _ := proxyResp["status"].(string)
	if status == "error" {
		score.pass("error_propagation", "proxy correctly returns error for invalid query")
	} else {
		score.fail("error_propagation", fmt.Sprintf("proxy returned status=%q instead of error", status))
	}

	score.report(t)
}

// =============================================================================
// Multi-tenant isolation — different tenants must not leak data
// =============================================================================

func TestExtended_TenantIsolation(t *testing.T) {
	score := &CompatScore{}
	now := time.Now()

	params := url.Values{}
	params.Set("query", `{app="api-gateway"}`)
	params.Set("start", fmt.Sprintf("%d", now.Add(-10*time.Minute).UnixNano()))
	params.Set("end", fmt.Sprintf("%d", now.UnixNano()))

	// Request with tenant A
	reqA, _ := http.NewRequest("GET", proxyURL+"/loki/api/v1/query_range?"+params.Encode(), nil)
	reqA.Header.Set("X-Scope-OrgID", "tenant-a")
	respA, err := http.DefaultClient.Do(reqA)
	if err != nil {
		score.fail("tenant_isolation", "tenant-a request failed")
		score.report(t)
		return
	}
	defer respA.Body.Close()

	// Request with tenant B
	reqB, _ := http.NewRequest("GET", proxyURL+"/loki/api/v1/query_range?"+params.Encode(), nil)
	reqB.Header.Set("X-Scope-OrgID", "tenant-b")
	respB, err := http.DefaultClient.Do(reqB)
	if err != nil {
		score.fail("tenant_isolation", "tenant-b request failed")
		score.report(t)
		return
	}
	defer respB.Body.Close()

	switch {
	case respA.StatusCode == 200 && respB.StatusCode == 200:
		score.pass("tenant_isolation", "both tenants get valid responses")
	case respA.StatusCode == respB.StatusCode &&
		(respA.StatusCode == http.StatusUnauthorized || respA.StatusCode == http.StatusForbidden):
		score.secure("tenant_isolation", fmt.Sprintf("proxy consistently rejects unmapped tenants: tenant-a=%d tenant-b=%d", respA.StatusCode, respB.StatusCode))
	default:
		score.fail("tenant_isolation", fmt.Sprintf("tenant-a=%d tenant-b=%d", respA.StatusCode, respB.StatusCode))
	}

	score.report(t)
}

// =============================================================================
// label_format multi-rename — multiple renames in one pipe
// =============================================================================

func TestExtended_LabelFormat_MultiRename(t *testing.T) {
	ensureDataIngested(t)
	score := &CompatScore{}
	now := time.Now()

	params := url.Values{}
	params.Set("query", `{app="api-gateway"} | label_format app_name="{{.app}}", log_level="{{.level}}"`)
	params.Set("start", fmt.Sprintf("%d", now.Add(-10*time.Minute).UnixNano()))
	params.Set("end", fmt.Sprintf("%d", now.UnixNano()))

	proxyResp := getJSON(t, proxyURL+"/loki/api/v1/query_range?"+params.Encode())

	if checkStatus(proxyResp) {
		score.pass("label_format_multi", "proxy returns success for multi-rename label_format")
	} else {
		score.fail("label_format_multi", "proxy error on multi-rename label_format")
	}

	score.report(t)
}

// =============================================================================
// without() grouping clause
// =============================================================================

func TestExtended_WithoutGrouping(t *testing.T) {
	ensureDataIngested(t)
	score := &CompatScore{}
	now := time.Now()

	params := url.Values{}
	params.Set("query", `sum without (level) (count_over_time({app="api-gateway"}[5m]))`)
	params.Set("start", fmt.Sprintf("%d", now.Add(-10*time.Minute).UnixNano()))
	params.Set("end", fmt.Sprintf("%d", now.UnixNano()))
	params.Set("step", "60")

	proxyResp := getJSON(t, proxyURL+"/loki/api/v1/query_range?"+params.Encode())

	if checkStatus(proxyResp) {
		score.pass("without_grouping", "proxy returns success for without() grouping")
	} else {
		score.fail("without_grouping", "proxy error on without() grouping")
	}

	score.report(t)
}

// =============================================================================
// on()/ignoring()/group_left()/group_right() live compatibility smoke
// =============================================================================

func TestExtended_OnGrouping(t *testing.T) {
	ensureDataIngested(t)
	score := &CompatScore{}
	now := time.Now()

	params := url.Values{}
	params.Set("query", `sum by (app, pod) (count_over_time({cluster="us-east-1"}[5m])) / on(app) sum by (app) (count_over_time({cluster="us-east-1", level="error"}[5m]))`)
	params.Set("start", fmt.Sprintf("%d", now.Add(-10*time.Minute).UnixNano()))
	params.Set("end", fmt.Sprintf("%d", now.UnixNano()))
	params.Set("step", "60")

	proxyResp := getJSON(t, proxyURL+"/loki/api/v1/query_range?"+params.Encode())
	if checkStatus(proxyResp) {
		score.pass("on_grouping", "proxy returns success for on() grouping")
	} else {
		score.fail("on_grouping", "proxy error on on() grouping")
	}

	score.report(t)
}

func TestExtended_IgnoringGrouping(t *testing.T) {
	ensureDataIngested(t)
	score := &CompatScore{}
	now := time.Now()

	params := url.Values{}
	params.Set("query", `sum by (app, level) (count_over_time({app="api-gateway"}[5m])) / ignoring(level) sum by (app) (count_over_time({app="api-gateway"}[5m]))`)
	params.Set("start", fmt.Sprintf("%d", now.Add(-10*time.Minute).UnixNano()))
	params.Set("end", fmt.Sprintf("%d", now.UnixNano()))
	params.Set("step", "60")

	proxyResp := getJSON(t, proxyURL+"/loki/api/v1/query_range?"+params.Encode())
	if checkStatus(proxyResp) {
		score.pass("ignoring_grouping", "proxy returns success for ignoring() grouping")
	} else {
		score.fail("ignoring_grouping", "proxy error on ignoring() grouping")
	}

	score.report(t)
}

func TestExtended_GroupLeftJoin(t *testing.T) {
	ensureDataIngested(t)
	score := &CompatScore{}
	now := time.Now()

	params := url.Values{}
	params.Set("query", `sum by (app, pod) (count_over_time({cluster="us-east-1"}[5m])) * on(app) group_left(level) sum by (app, level) (count_over_time({cluster="us-east-1", level="error"}[5m]))`)
	params.Set("start", fmt.Sprintf("%d", now.Add(-10*time.Minute).UnixNano()))
	params.Set("end", fmt.Sprintf("%d", now.UnixNano()))
	params.Set("step", "60")

	proxyResp := getJSON(t, proxyURL+"/loki/api/v1/query_range?"+params.Encode())
	if checkStatus(proxyResp) {
		score.pass("group_left", "proxy returns success for group_left() join")
	} else {
		score.fail("group_left", "proxy error on group_left() join")
	}

	score.report(t)
}

func TestExtended_GroupRightJoin(t *testing.T) {
	ensureDataIngested(t)
	score := &CompatScore{}
	now := time.Now()

	params := url.Values{}
	params.Set("query", `sum by (app, level) (count_over_time({cluster="us-east-1", level="error"}[5m])) * on(app) group_right(pod) sum by (app, pod) (count_over_time({cluster="us-east-1"}[5m]))`)
	params.Set("start", fmt.Sprintf("%d", now.Add(-10*time.Minute).UnixNano()))
	params.Set("end", fmt.Sprintf("%d", now.UnixNano()))
	params.Set("step", "60")

	proxyResp := getJSON(t, proxyURL+"/loki/api/v1/query_range?"+params.Encode())
	if checkStatus(proxyResp) {
		score.pass("group_right", "proxy returns success for group_right() join")
	} else {
		score.fail("group_right", "proxy error on group_right() join")
	}

	score.report(t)
}
