//go:build e2e

package e2e_compat

import (
	"fmt"
	"math"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

var (
	operationsMatrixOnce sync.Once
	operationsMatrixApp  string
)

func TestOperationsMatrix_BinaryScalarOperatorsParity(t *testing.T) {
	app := ensureOperationsMatrixData(t)
	base := fmt.Sprintf(`sum(count_over_time({app="%s"}[10m]))`, app)
	cases := []struct {
		name    string
		query   string
		op      string
		reverse bool
	}{
		{name: "add_rhs", query: base + ` + 2`, op: "+"},
		{name: "sub_rhs", query: base + ` - 2`, op: "-"},
		{name: "mul_rhs", query: base + ` * 2`, op: "*"},
		{name: "div_rhs", query: base + ` / 2`, op: "/"},
		{name: "mod_rhs", query: base + ` % 2`, op: "%"},
		{name: "pow_rhs", query: base + ` ^ 2`, op: "^"},
		{name: "add_lhs", query: `2 + ` + base, op: "+", reverse: true},
		{name: "sub_lhs", query: `2 - ` + base, op: "-", reverse: true},
		{name: "mul_lhs", query: `2 * ` + base, op: "*", reverse: true},
		{name: "div_lhs", query: `2 / ` + base, op: "/", reverse: true},
		{name: "mod_lhs", query: `2 % ` + base, op: "%", reverse: true},
		{name: "pow_lhs", query: `2 ^ ` + base, op: "^", reverse: true},
		{name: "gt_bool_rhs", query: base + ` > bool 2`, op: ">"},
		{name: "lt_bool_rhs", query: base + ` < bool 2`, op: "<"},
		{name: "gte_bool_rhs", query: base + ` >= bool 2`, op: ">="},
		{name: "lte_bool_rhs", query: base + ` <= bool 2`, op: "<="},
		{name: "eq_bool_rhs", query: base + ` == bool 2`, op: "=="},
		{name: "neq_bool_rhs", query: base + ` != bool 2`, op: "!="},
		{name: "gt_bool_lhs", query: `2 > bool ` + base, op: ">", reverse: true},
		{name: "lt_bool_lhs", query: `2 < bool ` + base, op: "<", reverse: true},
		{name: "gte_bool_lhs", query: `2 >= bool ` + base, op: ">=", reverse: true},
		{name: "lte_bool_lhs", query: `2 <= bool ` + base, op: "<=", reverse: true},
		{name: "eq_bool_lhs", query: `2 == bool ` + base, op: "==", reverse: true},
		{name: "neq_bool_lhs", query: `2 != bool ` + base, op: "!=", reverse: true},
	}

	at := time.Now()
	proxyBase := querySingleInstantValue(t, proxyURL, base, at)
	lokiBase := querySingleInstantValue(t, lokiURL, base, at)

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			proxyStatus, proxyBody, proxyResp := queryInstantGET(t, proxyURL, tc.query, at)
			lokiStatus, lokiBody, lokiResp := queryInstantGET(t, lokiURL, tc.query, at)
			if proxyStatus != http.StatusOK || lokiStatus != http.StatusOK {
				t.Fatalf("expected 200 query responses, proxy=%d loki=%d query=%q proxyBody=%s lokiBody=%s", proxyStatus, lokiStatus, tc.query, proxyBody, lokiBody)
			}
			if !checkStatus(proxyResp) || !checkStatus(lokiResp) {
				t.Fatalf("expected successful responses for %q, proxy=%v loki=%v", tc.query, proxyResp, lokiResp)
			}

			proxyData := extractMap(proxyResp, "data")
			lokiData := extractMap(lokiResp, "data")
			if proxyData["resultType"] != "vector" || lokiData["resultType"] != "vector" {
				t.Fatalf("expected vector result type for %q, proxy=%v loki=%v", tc.query, proxyData["resultType"], lokiData["resultType"])
			}

			proxyVal := singleVectorValue(t, extractInstantVector(t, proxyResp), tc.query)
			lokiVal := singleVectorValue(t, extractInstantVector(t, lokiResp), tc.query)

			expProxy := applyExpectedOp(proxyBase, 2, tc.op, tc.reverse)
			expLoki := applyExpectedOp(lokiBase, 2, tc.op, tc.reverse)
			if !nearlyEqual(proxyVal, expProxy) {
				t.Fatalf("proxy operation mismatch for %q: got=%v want=%v", tc.query, proxyVal, expProxy)
			}
			if !nearlyEqual(lokiVal, expLoki) {
				t.Fatalf("loki operation mismatch for %q: got=%v want=%v", tc.query, lokiVal, expLoki)
			}
		})
	}
}

func TestOperationsMatrix_FilterOperatorsParity(t *testing.T) {
	app := ensureOperationsMatrixData(t)

	queries := []struct {
		name  string
		query string
	}{
		{name: "line_contains", query: fmt.Sprintf(`{app="%s"} |= "GET"`, app)},
		{name: "line_not_contains", query: fmt.Sprintf(`{app="%s"} != "health"`, app)},
		{name: "line_regex", query: fmt.Sprintf(`{app="%s"} |~ "POST|DELETE"`, app)},
		{name: "line_not_regex", query: fmt.Sprintf(`{app="%s"} !~ "ready|metrics"`, app)},
		{name: "json_filter_gte", query: fmt.Sprintf(`{app="%s"} | json | duration_ms >= 10`, app)},
		{name: "json_filter_lt", query: fmt.Sprintf(`{app="%s"} | json | duration_ms < 100`, app)},
		{name: "logical_and", query: fmt.Sprintf(`{app="%s"} | json | status >= 200 and status < 500`, app)},
		{name: "logical_or", query: fmt.Sprintf(`{app="%s"} | json | status = 500 or status = 502`, app)},
	}

	for _, tc := range queries {
		t.Run(tc.name, func(t *testing.T) {
			proxyResp := queryProxy(t, tc.query)
			lokiResp := queryLoki(t, tc.query)
			if !checkStatus(proxyResp) || !checkStatus(lokiResp) {
				t.Fatalf("expected successful responses for %q, proxy=%v loki=%v", tc.query, proxyResp, lokiResp)
			}

			proxyLines := countLogLines(proxyResp)
			lokiLines := countLogLines(lokiResp)
			if proxyLines != lokiLines {
				t.Fatalf("filter parity mismatch for %q: proxy=%d loki=%d", tc.query, proxyLines, lokiLines)
			}
		})
	}
}

func TestOperationsMatrix_LokiMetricFunctionsParity(t *testing.T) {
	app := ensureOperationsMatrixData(t)
	at := time.Now()

	// Cross-engine parity for scalar-compatible expressions.
	parityCases := []struct {
		name  string
		query string
	}{
		{name: "count_over_time", query: fmt.Sprintf(`sum(count_over_time({app="%s"}[10m]))`, app)},
		{name: "bytes_over_time", query: fmt.Sprintf(`sum(bytes_over_time({app="%s"}[10m]))`, app)},
		{name: "bytes_rate", query: fmt.Sprintf(`sum(bytes_rate({app="%s"}[10m]))`, app)},
		{name: "rate", query: fmt.Sprintf(`sum(rate({app="%s"}[10m]))`, app)},
		{name: "sum_over_time", query: fmt.Sprintf(`sum(sum_over_time({app="%s"} | json | unwrap duration_ms [10m]))`, app)},
		{name: "avg_over_time", query: fmt.Sprintf(`sum(avg_over_time({app="%s"} | json | unwrap duration_ms [10m]))`, app)},
		{name: "max_over_time", query: fmt.Sprintf(`sum(max_over_time({app="%s"} | json | unwrap duration_ms [10m]))`, app)},
		{name: "min_over_time", query: fmt.Sprintf(`sum(min_over_time({app="%s"} | json | unwrap duration_ms [10m]))`, app)},
		{name: "stddev_over_time", query: fmt.Sprintf(`sum(stddev_over_time({app="%s"} | json | unwrap duration_ms [10m]))`, app)},
		{name: "stdvar_over_time", query: fmt.Sprintf(`sum(stdvar_over_time({app="%s"} | json | unwrap duration_ms [10m]))`, app)},
		{name: "quantile_over_time", query: fmt.Sprintf(`sum(quantile_over_time(0.95, {app="%s"} | json | unwrap duration_ms [10m]))`, app)},
	}
	for _, tc := range parityCases {
		t.Run("parity_"+tc.name, func(t *testing.T) {
			proxyVal := querySingleInstantValue(t, proxyURL, tc.query, at)
			lokiVal := querySingleInstantValue(t, lokiURL, tc.query, at)
			if !nearlyEqual(proxyVal, lokiVal) {
				t.Fatalf("function parity mismatch for %q: proxy=%v loki=%v", tc.query, proxyVal, lokiVal)
			}
		})
	}

	// Vector parity for unwrap queries that keep per-label/per-line cardinality.
	vectorCases := []struct {
		name  string
		query string
	}{
		{name: "sum_over_time_vector", query: fmt.Sprintf(`sum_over_time({app="%s"} | json | unwrap duration_ms [10m])`, app)},
		{name: "avg_over_time_vector", query: fmt.Sprintf(`avg_over_time({app="%s"} | json | unwrap duration_ms [10m])`, app)},
		{name: "max_over_time_vector", query: fmt.Sprintf(`max_over_time({app="%s"} | json | unwrap duration_ms [10m])`, app)},
		{name: "min_over_time_vector", query: fmt.Sprintf(`min_over_time({app="%s"} | json | unwrap duration_ms [10m])`, app)},
		{name: "stddev_over_time_vector", query: fmt.Sprintf(`stddev_over_time({app="%s"} | json | unwrap duration_ms [10m])`, app)},
		{name: "stdvar_over_time_vector", query: fmt.Sprintf(`stdvar_over_time({app="%s"} | json | unwrap duration_ms [10m])`, app)},
		{name: "quantile_over_time_vector", query: fmt.Sprintf(`quantile_over_time(0.95, {app="%s"} | json | unwrap duration_ms [10m])`, app)},
	}
	for _, tc := range vectorCases {
		t.Run("vector_"+tc.name, func(t *testing.T) {
			proxyStatus, proxyBody, proxyResp := queryInstantGET(t, proxyURL, tc.query, at)
			lokiStatus, lokiBody, lokiResp := queryInstantGET(t, lokiURL, tc.query, at)
			if proxyStatus != http.StatusOK || lokiStatus != http.StatusOK {
				t.Fatalf("expected 200 query responses, proxy=%d loki=%d query=%q proxyBody=%s lokiBody=%s", proxyStatus, lokiStatus, tc.query, proxyBody, lokiBody)
			}
			if !checkStatus(proxyResp) || !checkStatus(lokiResp) {
				t.Fatalf("expected successful responses for %q, proxy=%v loki=%v", tc.query, proxyResp, lokiResp)
			}

			proxyData := extractMap(proxyResp, "data")
			lokiData := extractMap(lokiResp, "data")
			if proxyData["resultType"] != "vector" || lokiData["resultType"] != "vector" {
				t.Fatalf("expected vector result type for %q, proxy=%v loki=%v", tc.query, proxyData["resultType"], lokiData["resultType"])
			}
			assertVectorParity(t, tc.query, extractInstantVector(t, proxyResp), extractInstantVector(t, lokiResp))
		})
	}
}

func queryInstantGET(t *testing.T, baseURL, expr string, at time.Time) (int, string, map[string]interface{}) {
	t.Helper()
	params := url.Values{}
	params.Set("query", expr)
	params.Set("time", at.Format(time.RFC3339Nano))
	return doJSONGET(t, baseURL+"/loki/api/v1/query?"+params.Encode(), nil)
}

func extractInstantVector(t *testing.T, payload map[string]interface{}) map[string]float64 {
	t.Helper()
	data := extractMap(payload, "data")
	if data == nil {
		t.Fatalf("missing data block: %#v", payload)
	}
	result := extractArray(data, "result")
	out := make(map[string]float64, len(result))

	for _, item := range result {
		sample, ok := item.(map[string]interface{})
		if !ok {
			t.Fatalf("unexpected sample shape: %#v", item)
		}
		metric, _ := sample["metric"].(map[string]interface{})
		key := instantMetricKey(metric)

		valuePair, ok := sample["value"].([]interface{})
		if !ok || len(valuePair) < 2 {
			t.Fatalf("unexpected instant value payload: %#v", sample["value"])
		}

		raw := valuePair[1]
		switch v := raw.(type) {
		case string:
			f, err := strconv.ParseFloat(v, 64)
			if err != nil {
				t.Fatalf("parse instant value %q: %v", v, err)
			}
			out[key] = f
		case float64:
			out[key] = v
		default:
			t.Fatalf("unsupported instant value type %T (%v)", raw, raw)
		}
	}

	return out
}

func instantMetricKey(metric map[string]interface{}) string {
	if len(metric) == 0 {
		return "{}"
	}
	parts := make([]string, 0, len(metric))
	for k, v := range metric {
		parts = append(parts, fmt.Sprintf("%s=%v", k, v))
	}
	sort.Strings(parts)
	return strings.Join(parts, ",")
}

func assertVectorParity(t *testing.T, query string, proxyVec, lokiVec map[string]float64) {
	t.Helper()

	if len(proxyVec) != len(lokiVec) {
		t.Fatalf("series count mismatch for %q: proxy=%d loki=%d proxy=%v loki=%v", query, len(proxyVec), len(lokiVec), proxyVec, lokiVec)
	}

	proxyKeys := make([]string, 0, len(proxyVec))
	for key := range proxyVec {
		proxyKeys = append(proxyKeys, key)
	}
	sort.Strings(proxyKeys)

	lokiKeys := make([]string, 0, len(lokiVec))
	for key := range lokiVec {
		lokiKeys = append(lokiKeys, key)
	}
	sort.Strings(lokiKeys)

	if strings.Join(proxyKeys, ",") == strings.Join(lokiKeys, ",") {
		for key, lokiVal := range lokiVec {
			proxyVal := proxyVec[key]
			if !nearlyEqual(proxyVal, lokiVal) {
				t.Fatalf("value mismatch for %q key=%q proxy=%v loki=%v", query, key, proxyVal, lokiVal)
			}
		}
		return
	}

	proxyVals := make([]float64, 0, len(proxyVec))
	for _, key := range proxyKeys {
		proxyVals = append(proxyVals, proxyVec[key])
	}
	lokiVals := make([]float64, 0, len(lokiVec))
	for _, key := range lokiKeys {
		lokiVals = append(lokiVals, lokiVec[key])
	}
	sort.Float64s(proxyVals)
	sort.Float64s(lokiVals)
	for i := range proxyVals {
		if !nearlyEqual(proxyVals[i], lokiVals[i]) {
			t.Fatalf("value mismatch for %q (label-shape fallback compare) proxy=%v loki=%v", query, proxyVals, lokiVals)
		}
	}
}

func nearlyEqual(a, b float64) bool {
	diff := math.Abs(a - b)
	scale := math.Max(1, math.Max(math.Abs(a), math.Abs(b)))
	return diff <= 1e-6*scale
}

func querySingleInstantValue(t *testing.T, baseURL, expr string, at time.Time) float64 {
	t.Helper()
	status, body, payload := queryInstantGET(t, baseURL, expr, at)
	if status != http.StatusOK {
		t.Fatalf("expected 200 query response for %q from %s, got=%d body=%s", expr, baseURL, status, body)
	}
	if !checkStatus(payload) {
		t.Fatalf("expected successful response for %q from %s, got=%v", expr, baseURL, payload)
	}

	data := extractMap(payload, "data")
	if data["resultType"] != "vector" {
		t.Fatalf("expected vector resultType for %q from %s, got=%v", expr, baseURL, data["resultType"])
	}
	return singleVectorValue(t, extractInstantVector(t, payload), expr)
}

func singleVectorValue(t *testing.T, vec map[string]float64, query string) float64 {
	t.Helper()
	if len(vec) != 1 {
		t.Fatalf("expected single-series result for %q, got=%v", query, vec)
	}
	for _, v := range vec {
		return v
	}
	return 0
}

func applyExpectedOp(metric, scalar float64, op string, reverse bool) float64 {
	a, b := metric, scalar
	if reverse {
		a, b = scalar, metric
	}
	switch op {
	case "+":
		return a + b
	case "-":
		return a - b
	case "*":
		return a * b
	case "/":
		if b == 0 {
			return 0
		}
		return a / b
	case "%":
		if b == 0 {
			return 0
		}
		return math.Mod(a, b)
	case "^":
		return math.Pow(a, b)
	case "==":
		if a == b {
			return 1
		}
		return 0
	case "!=":
		if a != b {
			return 1
		}
		return 0
	case ">":
		if a > b {
			return 1
		}
		return 0
	case "<":
		if a < b {
			return 1
		}
		return 0
	case ">=":
		if a >= b {
			return 1
		}
		return 0
	case "<=":
		if a <= b {
			return 1
		}
		return 0
	default:
		return metric
	}
}

func ensureOperationsMatrixData(t *testing.T) string {
	t.Helper()

	operationsMatrixOnce.Do(func() {
		waitForReady(t, proxyURL+"/ready", 30*time.Second)
		waitForReady(t, lokiURL+"/ready", 30*time.Second)

		operationsMatrixApp = fmt.Sprintf("ops-matrix-%d", time.Now().UnixNano())
		now := time.Now().Add(-2 * time.Minute)

		lines := []logLine{
			{Msg: `{"method":"GET","path":"/v1/users","status":200,"duration_ms":10}`, Level: "info"},
			{Msg: `{"method":"POST","path":"/v1/orders","status":201,"duration_ms":20}`, Level: "info"},
			{Msg: `{"method":"DELETE","path":"/v1/orders/42","status":500,"duration_ms":100}`, Level: "info"},
			{Msg: `{"method":"GET","path":"/health","status":200,"duration_ms":1}`, Level: "info"},
			{Msg: `{"method":"GET","path":"/metrics","status":200,"duration_ms":2}`, Level: "info"},
		}
		stream := map[string]string{"app": operationsMatrixApp, "env": "matrix", "level": "info"}
		pushCustomToLoki(t, now, stream, lines)
		pushCustomToVL(t, now, stream, lines, []string{"app", "env"})

		time.Sleep(3 * time.Second)
	})

	return operationsMatrixApp
}
