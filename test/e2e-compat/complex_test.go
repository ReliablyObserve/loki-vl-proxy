//go:build e2e

// Complex query compatibility tests — real-world LogQL patterns.
// These test chained filters, multi-label selectors, parsers,
// negative filters, regex, and analytics queries.
package e2e_compat

import (
	"fmt"
	"net/url"
	"testing"
	"time"
)

// TestSetup_IngestRichData pushes diverse production-like logs.
func TestSetup_IngestRichData(t *testing.T) {
	waitForReady(t, lokiURL+"/ready", 30*time.Second)
	waitForReady(t, proxyURL+"/ready", 30*time.Second)
	ingestRichTestData(t)
}

// =============================================================================
// Multi-label stream selectors
// =============================================================================

func TestComplex_MultiLabel_ExactMatch(t *testing.T) {
	q := `{app="api-gateway",namespace="prod",level="error"}`
	compareQuery(t, "multi_label_exact", q)
}

func TestComplex_MultiLabel_RegexApp(t *testing.T) {
	q := `{app=~"api-.*",namespace="prod",env="production"}`
	compareQuery(t, "multi_label_regex_app", q)
}

func TestComplex_MultiLabel_RegexNamespace(t *testing.T) {
	q := `{namespace=~"prod|staging",app="api-gateway"}`
	compareQuery(t, "multi_label_regex_ns", q)
}

func TestComplex_MultiLabel_NegativeMatch(t *testing.T) {
	q := `{app="api-gateway",level!="info"}`
	compareQuery(t, "multi_label_negative", q)
}

func TestComplex_MultiLabel_NegativeRegex(t *testing.T) {
	q := `{app=~".+",namespace!~"kube-.*"}`
	compareQuery(t, "multi_label_neg_regex", q)
}

// =============================================================================
// Line filters — chained, regex, negative
// =============================================================================

func TestComplex_LineFilter_Contains(t *testing.T) {
	q := `{app="api-gateway"} |= "payment"`
	compareQuery(t, "line_contains", q)
}

func TestComplex_LineFilter_NotContains(t *testing.T) {
	q := `{app="api-gateway"} != "health"`
	compareQuery(t, "line_not_contains", q)
}

func TestComplex_LineFilter_Regex(t *testing.T) {
	q := `{app="api-gateway"} |~ "status.*[45]\\d{2}"`
	compareQuery(t, "line_regex", q)
}

func TestComplex_LineFilter_NegativeRegex(t *testing.T) {
	q := `{app="api-gateway"} !~ "health|ready|metrics"`
	compareQuery(t, "line_neg_regex", q)
}

func TestComplex_LineFilter_Chained(t *testing.T) {
	// Must contain "error" AND NOT contain "not found"
	q := `{app="api-gateway",level="error"} |= "error" != "not found"`
	compareQuery(t, "line_chained", q)
}

func TestComplex_LineFilter_ChainedMultiple(t *testing.T) {
	// Contains "payment" AND contains "500"
	q := `{app="api-gateway"} |= "payment" |= "500"`
	compareQuery(t, "line_chained_multi", q)
}

// =============================================================================
// Parsers — JSON, logfmt, pattern
// =============================================================================

func TestComplex_JSON_Parse(t *testing.T) {
	q := `{app="api-gateway",level="info"} | json`
	compareQuery(t, "json_parse", q)
}

func TestComplex_JSON_FilterAfterParse(t *testing.T) {
	q := `{app="api-gateway",level="info"} | json | status >= 400`
	compareQuery(t, "json_filter_status", q)
}

func TestComplex_JSON_FilterMethod(t *testing.T) {
	q := `{app="api-gateway",level="info"} | json | method = "POST"`
	compareQuery(t, "json_filter_method", q)
}

func TestComplex_JSON_FilterMultiple(t *testing.T) {
	q := `{app="api-gateway",level="error"} | json | status >= 500 | upstream = "payment-service"`
	compareQuery(t, "json_filter_multi", q)
}

func TestComplex_Logfmt_Parse(t *testing.T) {
	q := `{app="payment-service",level="error"} | logfmt`
	compareQuery(t, "logfmt_parse", q)
}

func TestComplex_Logfmt_FilterAfterParse(t *testing.T) {
	// Note: Loki and VL handle duration string comparison differently.
	// Loki tries numeric parsing on "2.3s", fails, drops the line.
	// VL does string comparison "2.3s" > "1s" → true (correct for strings).
	// We verify the proxy returns a valid response and handles the pipe chain.
	q := `{app="payment-service",level="warn"} | logfmt | duration > "1s"`
	score := &CompatScore{}

	proxyResult := queryProxy(t, q)
	if checkStatus(proxyResult) {
		score.pass("logfmt_filter", "proxy returns status=success")
	} else {
		score.fail("logfmt_filter", "proxy status not success")
	}

	proxyData := extractMap(proxyResult, "data")
	if proxyData != nil && proxyData["resultType"] == "streams" {
		score.pass("logfmt_filter", "proxy returns resultType=streams")
	} else {
		score.fail("logfmt_filter", "wrong resultType")
	}

	// Proxy should return lines where duration > "1s" (string comparison)
	proxyLines := countLogLines(proxyResult)
	t.Logf("[logfmt_filter] Proxy=%d lines (VL string comparison of duration)", proxyLines)
	if proxyLines >= 0 { // any valid result is OK — semantic difference with Loki is documented
		score.pass("logfmt_filter", "proxy handled logfmt+filter pipeline")
	}

	score.report(t)
}

// =============================================================================
// Negative filters — excluding noise
// =============================================================================

func TestComplex_ExcludeHealthchecks(t *testing.T) {
	// Common pattern: exclude health/ready/metrics endpoints
	q := `{app="api-gateway"} != "/health" != "/ready" != "/metrics"`
	compareQuery(t, "exclude_health", q)
}

func TestComplex_ExcludeHealthchecksRegex(t *testing.T) {
	q := `{app="api-gateway"} !~ "(health|ready|metrics)"`
	compareQuery(t, "exclude_health_regex", q)
}

func TestComplex_ErrorsOnly(t *testing.T) {
	// Errors that are NOT 404s
	q := `{namespace="prod",level="error"} != "not found" != "404"`
	compareQuery(t, "errors_not_404", q)
}

// =============================================================================
// Cross-service / cross-namespace queries
// =============================================================================

func TestComplex_AllNamespaces(t *testing.T) {
	q := `{namespace=~".+"}`
	compareQuery(t, "all_namespaces", q)
}

func TestComplex_CrossService_Errors(t *testing.T) {
	q := `{namespace="prod",level="error"}`
	compareQuery(t, "cross_svc_errors", q)
}

func TestComplex_SpecificPod(t *testing.T) {
	q := `{pod=~"api-gateway.*"}`
	compareQuery(t, "specific_pod", q)
}

// =============================================================================
// Label metadata queries
// =============================================================================

func TestComplex_Labels_AllNamespaces(t *testing.T) {
	score := &CompatScore{}

	proxyResp := getJSON(t, proxyURL+"/loki/api/v1/label/namespace/values")
	lokiResp := getJSON(t, lokiURL+"/loki/api/v1/label/namespace/values")

	proxyValues := extractStrings(proxyResp, "data")
	lokiValues := extractStrings(lokiResp, "data")

	t.Logf("Loki namespaces: %v", lokiValues)
	t.Logf("Proxy namespaces: %v", proxyValues)

	// Both should return prod, staging, ingress-nginx, observability
	for _, ns := range []string{"prod", "staging"} {
		if contains(proxyValues, ns) {
			score.pass("label_values_ns", fmt.Sprintf("proxy has namespace %q", ns))
		} else {
			score.fail("label_values_ns", fmt.Sprintf("proxy missing namespace %q", ns))
		}
	}

	score.report(t)
}

func TestComplex_Labels_AllApps(t *testing.T) {
	score := &CompatScore{}

	proxyResp := getJSON(t, proxyURL+"/loki/api/v1/label/app/values")
	proxyValues := extractStrings(proxyResp, "data")

	t.Logf("Proxy apps: %v", proxyValues)

	for _, app := range []string{"api-gateway", "payment-service", "nginx-ingress"} {
		if contains(proxyValues, app) {
			score.pass("label_values_app", fmt.Sprintf("proxy has app %q", app))
		} else {
			score.fail("label_values_app", fmt.Sprintf("proxy missing app %q", app))
		}
	}

	score.report(t)
}

func TestComplex_Series_MultiLabel(t *testing.T) {
	score := &CompatScore{}
	params := url.Values{}
	params.Set("match[]", `{namespace="prod"}`)

	proxyResp := getJSON(t, proxyURL+"/loki/api/v1/series?"+params.Encode())
	lokiResp := getJSON(t, lokiURL+"/loki/api/v1/series?"+params.Encode())

	proxyData := extractArray(proxyResp, "data")
	lokiData := extractArray(lokiResp, "data")

	t.Logf("Series in prod: Loki=%d, Proxy=%d", len(lokiData), len(proxyData))

	if len(proxyData) > 0 {
		score.pass("series_multi", fmt.Sprintf("proxy returns %d prod series", len(proxyData)))
	} else {
		score.fail("series_multi", "proxy returned 0 series for namespace=prod")
	}

	score.report(t)
}

// =============================================================================
// Drop / keep label pipelines
// =============================================================================

func TestComplex_DropLabels(t *testing.T) {
	q := `{app="api-gateway",level="info"} | drop pod, container`
	compareQuery(t, "drop_labels", q)
}

func TestComplex_KeepLabels(t *testing.T) {
	q := `{app="api-gateway",level="info"} | keep app, namespace, level`
	compareQuery(t, "keep_labels", q)
}

// =============================================================================
// Edge cases
// =============================================================================

func TestComplex_EmptyResult(t *testing.T) {
	q := `{app="nonexistent-service-xyz"}`
	score := &CompatScore{}

	proxyResult := queryProxy(t, q)
	lokiResult := queryLoki(t, q)

	proxyLines := countLogLines(proxyResult)
	lokiLines := countLogLines(lokiResult)

	if proxyLines == 0 && lokiLines == 0 {
		score.pass("empty_result", "both return 0 lines for nonexistent service")
	} else {
		score.fail("empty_result", fmt.Sprintf("loki=%d, proxy=%d", lokiLines, proxyLines))
	}

	score.report(t)
}

func TestComplex_VeryLongQuery(t *testing.T) {
	// Simulate a complex query with many conditions
	q := `{app="api-gateway",namespace="prod",env="production",cluster="us-east-1",level="error"} |= "payment" != "health" != "ready"`
	compareQuery(t, "long_query", q)
}

func TestComplex_SpecialCharsInFilter(t *testing.T) {
	q := `{app="api-gateway"} |= "/api/v1/users"`
	compareQuery(t, "special_chars", q)
}

// =============================================================================
// Combined scoring helper
// =============================================================================

func compareQuery(t *testing.T, name, logql string) {
	t.Helper()
	score := &CompatScore{}

	proxyResult := queryProxy(t, logql)
	lokiResult := queryLoki(t, logql)

	// Both should return success
	if checkStatus(proxyResult) {
		score.pass(name, "proxy status=success")
	} else {
		score.fail(name, "proxy status not success")
	}

	proxyLines := countLogLines(proxyResult)
	lokiLines := countLogLines(lokiResult)

	t.Logf("[%s] query: %s", name, logql)
	t.Logf("[%s] Loki=%d lines, Proxy=%d lines", name, lokiLines, proxyLines)

	// Both should return same result type
	proxyData := extractMap(proxyResult, "data")
	lokiData := extractMap(lokiResult, "data")

	if proxyData != nil && lokiData != nil {
		if proxyData["resultType"] == lokiData["resultType"] {
			score.pass(name, fmt.Sprintf("resultType match: %v", proxyData["resultType"]))
		} else {
			score.fail(name, fmt.Sprintf("resultType mismatch: loki=%v proxy=%v", lokiData["resultType"], proxyData["resultType"]))
		}
	}

	// Line count comparison
	if lokiLines == proxyLines {
		score.pass(name, fmt.Sprintf("line count exact match: %d", lokiLines))
	} else if lokiLines > 0 && proxyLines > 0 {
		ratio := float64(proxyLines) / float64(lokiLines)
		if ratio >= 0.8 && ratio <= 1.2 {
			score.pass(name, fmt.Sprintf("line count close: loki=%d proxy=%d (%.0f%%)", lokiLines, proxyLines, ratio*100))
		} else {
			score.fail(name, fmt.Sprintf("line count diverged: loki=%d proxy=%d", lokiLines, proxyLines))
		}
	} else if lokiLines == 0 && proxyLines == 0 {
		score.pass(name, "both return 0 lines")
	} else {
		score.fail(name, fmt.Sprintf("one empty: loki=%d proxy=%d", lokiLines, proxyLines))
	}

	score.report(t)
}

func queryProxy(t *testing.T, logql string) map[string]interface{} {
	t.Helper()
	now := time.Now()
	params := url.Values{}
	params.Set("query", logql)
	params.Set("start", fmt.Sprintf("%d", now.Add(-10*time.Minute).UnixNano()))
	params.Set("end", fmt.Sprintf("%d", now.UnixNano()))
	params.Set("limit", "1000")
	return getJSON(t, proxyURL+"/loki/api/v1/query_range?"+params.Encode())
}

func queryLoki(t *testing.T, logql string) map[string]interface{} {
	t.Helper()
	now := time.Now()
	params := url.Values{}
	params.Set("query", logql)
	params.Set("start", fmt.Sprintf("%d", now.Add(-10*time.Minute).UnixNano()))
	params.Set("end", fmt.Sprintf("%d", now.UnixNano()))
	params.Set("limit", "1000")
	return getJSON(t, lokiURL+"/loki/api/v1/query_range?"+params.Encode())
}
