//go:build e2e

package e2e_compat

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

type grafanaDSQueryResult struct {
	Status int
	Error  string
	Body   string
}

func containsMissingUnwrapError(errText string) bool {
	errText = strings.ToLower(strings.TrimSpace(errText))
	return strings.Contains(errText, "without unwrap") ||
		strings.Contains(errText, "requires `| unwrap") ||
		strings.Contains(errText, "requires | unwrap")
}

func queryGrafanaLokiDatasource(t *testing.T, datasourceUID, expr string) grafanaDSQueryResult {
	t.Helper()

	payload := map[string]interface{}{
		"from": "now-6h",
		"to":   "now",
		"queries": []map[string]interface{}{
			{
				"refId":      "A",
				"datasource": map[string]interface{}{"type": "loki", "uid": datasourceUID},
				"expr":       expr,
				"queryType":  "range",
				"editorMode": "code",
				"maxLines":   1000,
			},
		},
	}

	raw, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal grafana ds query payload: %v", err)
	}

	req, err := http.NewRequest(http.MethodPost, grafanaURL+"/api/ds/query", bytes.NewReader(raw))
	if err != nil {
		t.Fatalf("build grafana ds query request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("execute grafana ds query request: %v", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read grafana ds query response: %v", err)
	}

	var decoded map[string]interface{}
	if err := json.Unmarshal(body, &decoded); err != nil {
		t.Fatalf("decode grafana ds query response: %v body=%s", err, string(body))
	}

	resultsRaw, ok := decoded["results"]
	if !ok {
		t.Fatalf("grafana ds query missing results: %v", decoded)
	}
	results, ok := resultsRaw.(map[string]interface{})
	if !ok {
		t.Fatalf("grafana ds query results not an object: %T", resultsRaw)
	}
	queryRaw, ok := results["A"]
	if !ok {
		t.Fatalf("grafana ds query missing refId A: %v", results)
	}
	query, ok := queryRaw.(map[string]interface{})
	if !ok {
		t.Fatalf("grafana ds query A not an object: %T", queryRaw)
	}

	status := asInt(query["status"])
	errText := strings.TrimSpace(fmt.Sprintf("%v", query["error"]))
	if errText == "<nil>" {
		errText = ""
	}

	return grafanaDSQueryResult{
		Status: status,
		Error:  errText,
		Body:   string(body),
	}
}

func asInt(value interface{}) int {
	switch v := value.(type) {
	case int:
		return v
	case int64:
		return int(v)
	case float64:
		return int(v)
	case json.Number:
		i, _ := v.Int64()
		return int(i)
	case string:
		if v == "" {
			return 0
		}
		var i int
		_, _ = fmt.Sscanf(v, "%d", &i)
		return i
	default:
		return 0
	}
}

func TestGrafanaClickout_RangeMetricMissingUnwrapParity(t *testing.T) {
	ensureRangeMetricCompatData(t)
	waitForReady(t, grafanaURL+"/api/health", 30*time.Second)

	proxyUID := grafanaDatasourceUID(t, "Loki (via VL proxy)")
	directUID := grafanaDatasourceUID(t, "Loki (direct)")

	cases := []struct {
		name string
		expr string
	}{
		{name: "sum_over_time", expr: `sum_over_time({app="api-gateway"}[$__auto])`},
		{name: "avg_over_time", expr: `avg_over_time({app="api-gateway"}[$__auto])`},
		{name: "max_over_time", expr: `max_over_time({app="api-gateway"}[$__auto])`},
		{name: "min_over_time", expr: `min_over_time({app="api-gateway"}[$__auto])`},
		{name: "first_over_time", expr: `first_over_time({app="api-gateway"}[$__auto])`},
		{name: "last_over_time", expr: `last_over_time({app="api-gateway"}[$__auto])`},
		{name: "stddev_over_time", expr: `stddev_over_time({app="api-gateway"}[$__auto])`},
		{name: "stdvar_over_time", expr: `stdvar_over_time({app="api-gateway"}[$__auto])`},
		{name: "quantile_over_time", expr: `quantile_over_time(0.95, {app="api-gateway"}[$__auto])`},
		{name: "rate_counter", expr: `rate_counter({app="range-metric-counter"}[$__auto])`},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			proxyRes := queryGrafanaLokiDatasource(t, proxyUID, tc.expr)
			directRes := queryGrafanaLokiDatasource(t, directUID, tc.expr)

			if proxyRes.Status != directRes.Status {
				t.Fatalf("status mismatch proxy=%d direct=%d expr=%s proxyBody=%s directBody=%s", proxyRes.Status, directRes.Status, tc.expr, proxyRes.Body, directRes.Body)
			}
			if proxyRes.Status != http.StatusBadRequest {
				t.Fatalf("expected 400 for missing unwrap expr=%s proxy=%s direct=%s", tc.expr, proxyRes.Body, directRes.Body)
			}

			proxyErr := strings.ToLower(proxyRes.Error)
			directErr := strings.ToLower(directRes.Error)
			if !containsMissingUnwrapError(proxyErr) {
				t.Fatalf("expected proxy missing-unwrap error shape expr=%s got=%s", tc.expr, proxyRes.Error)
			}
			if !containsMissingUnwrapError(directErr) {
				t.Fatalf("expected direct missing-unwrap error shape expr=%s got=%s", tc.expr, directRes.Error)
			}
		})
	}
}

func TestGrafanaClickout_RangeMetricAutoWindowUnwrapSuccess(t *testing.T) {
	ensureRangeMetricCompatData(t)
	waitForReady(t, grafanaURL+"/api/health", 30*time.Second)

	proxyUID := grafanaDatasourceUID(t, "Loki (via VL proxy)")
	directUID := grafanaDatasourceUID(t, "Loki (direct)")

	cases := []struct {
		name string
		expr string
	}{
		{name: "sum_over_time_unwrap", expr: `sum_over_time({app="api-gateway"} | json | unwrap duration [$__auto])`},
		{name: "avg_over_time_unwrap", expr: `avg_over_time({app="api-gateway"} | json | unwrap duration [$__auto])`},
		{name: "max_over_time_unwrap", expr: `max_over_time({app="api-gateway"} | json | unwrap duration [$__auto])`},
		{name: "min_over_time_unwrap", expr: `min_over_time({app="api-gateway"} | json | unwrap duration [$__auto])`},
		{name: "first_over_time_unwrap", expr: `first_over_time({app="api-gateway"} | json | unwrap duration [$__auto])`},
		{name: "last_over_time_unwrap", expr: `last_over_time({app="api-gateway"} | json | unwrap duration [$__auto])`},
		{name: "stddev_over_time_unwrap", expr: `stddev_over_time({app="api-gateway"} | json | unwrap duration [$__auto])`},
		{name: "stdvar_over_time_unwrap", expr: `stdvar_over_time({app="api-gateway"} | json | unwrap duration [$__auto])`},
		{name: "quantile_over_time_unwrap", expr: `quantile_over_time(0.95, {app="api-gateway"} | json | unwrap duration [$__auto])`},
		{name: "rate_counter_unwrap", expr: `rate_counter({app="range-metric-counter"} | json | unwrap counter [$__auto])`},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			proxyRes := queryGrafanaLokiDatasource(t, proxyUID, tc.expr)
			directRes := queryGrafanaLokiDatasource(t, directUID, tc.expr)

			if proxyRes.Status != directRes.Status {
				t.Fatalf("status mismatch proxy=%d direct=%d expr=%s proxyBody=%s directBody=%s", proxyRes.Status, directRes.Status, tc.expr, proxyRes.Body, directRes.Body)
			}
			if proxyRes.Status != http.StatusOK {
				t.Fatalf("expected 200 for unwrap query expr=%s proxy=%s direct=%s", tc.expr, proxyRes.Body, directRes.Body)
			}
			if strings.TrimSpace(proxyRes.Error) != "" {
				t.Fatalf("proxy returned unexpected error expr=%s err=%s body=%s", tc.expr, proxyRes.Error, proxyRes.Body)
			}
			if strings.TrimSpace(directRes.Error) != "" {
				t.Fatalf("direct returned unexpected error expr=%s err=%s body=%s", tc.expr, directRes.Error, directRes.Body)
			}
		})
	}
}

func TestGrafanaClickout_RangeMetricSelectorOptionsParity(t *testing.T) {
	ensureRangeMetricCompatData(t)
	waitForReady(t, grafanaURL+"/api/health", 30*time.Second)

	proxyUID := grafanaDatasourceUID(t, "Loki (via VL proxy)")
	directUID := grafanaDatasourceUID(t, "Loki (direct)")

	windows := []string{
		"$__auto",
		"$__interval",
		"${__interval}",
		"5m",
		"1h",
		"1d",
	}

	for _, window := range windows {
		t.Run(window, func(t *testing.T) {
			expr := fmt.Sprintf(`sum_over_time({app="api-gateway"} | json | unwrap duration [%s])`, window)

			proxyRes := queryGrafanaLokiDatasource(t, proxyUID, expr)
			directRes := queryGrafanaLokiDatasource(t, directUID, expr)

			if proxyRes.Status != directRes.Status {
				t.Fatalf("status mismatch proxy=%d direct=%d window=%s expr=%s proxyBody=%s directBody=%s", proxyRes.Status, directRes.Status, window, expr, proxyRes.Body, directRes.Body)
			}
			if proxyRes.Status != http.StatusOK {
				t.Fatalf("expected 200 for selector option window=%s expr=%s proxy=%s direct=%s", window, expr, proxyRes.Body, directRes.Body)
			}
			if strings.TrimSpace(proxyRes.Error) != "" {
				t.Fatalf("proxy returned unexpected error window=%s expr=%s err=%s body=%s", window, expr, proxyRes.Error, proxyRes.Body)
			}
			if strings.TrimSpace(directRes.Error) != "" {
				t.Fatalf("direct returned unexpected error window=%s expr=%s err=%s body=%s", window, expr, directRes.Error, directRes.Body)
			}
		})
	}
}
