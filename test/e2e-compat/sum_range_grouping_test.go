//go:build e2e

package e2e_compat

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"testing"
	"time"
)

func TestHardeningLive_SumRangeAggregateAll(t *testing.T) {
	service := fmt.Sprintf("hardening-sum-range-%d", time.Now().UnixNano())
	evaluation := time.Now().Add(-4 * time.Minute).Truncate(time.Minute)
	stamp := evaluation.Add(-30 * time.Second)
	for i, message := range []string{"a", "bbb"} {
		labels := map[string]string{"service_name": service, "kind": strconv.Itoa(i)}
		row, _ := json.Marshal(map[string]string{"_time": stamp.UTC().Format(time.RFC3339Nano), "_msg": message, "service_name": service, "kind": labels["kind"]})
		status, body := hardeningRequest(t, http.MethodPost, vlURL+"/insert/jsonline?_stream_fields=service_name,kind", string(row)+"\n", map[string]string{"Content-Type": "application/stream+json"})
		if status != http.StatusOK {
			t.Fatalf("VL ingest: %d %s", status, body)
		}
		payload, _ := json.Marshal(map[string]any{"streams": []any{map[string]any{"stream": labels, "values": [][]string{{strconv.FormatInt(stamp.UnixNano(), 10), message}}}}})
		status, body = hardeningRequest(t, http.MethodPost, lokiURL+"/loki/api/v1/push", string(payload), map[string]string{"Content-Type": "application/json"})
		if status != http.StatusNoContent {
			t.Fatalf("Loki ingest: %d %s", status, body)
		}
	}
	status, body := hardeningRequest(t, http.MethodPost, vlURL+"/internal/force_flush", "", nil)
	if status != http.StatusOK {
		t.Fatalf("VL flush: %d %s", status, body)
	}
	selector := fmt.Sprintf(`{service_name=%q}`, service)
	// The shared two-row readiness gate requires both exact metric values.
	waitForParserErrorCanaryMetrics(t, selector, evaluation)
	for _, tc := range []struct {
		function string
		want     float64
	}{{"rate", 2.0 / 300}, {"bytes_rate", 4.0 / 300}, {"count_over_time", 2}, {"bytes_over_time", 4}} {
		for _, outer := range []string{"sum(%s)", "sum by () (%s)", "topk(1, sum(%s))"} {
			for _, mode := range []string{"query", "query_range"} {
				t.Run(tc.function+"/"+outer+"/"+mode, func(t *testing.T) {
					query := fmt.Sprintf(outer, tc.function+"("+selector+"[5m])")
					params := parserErrorQueryParams(query, evaluation, mode)
					for repeat := 0; repeat < 2; repeat++ {
						for _, base := range []string{lokiURL, proxyURL} {
							status, body := hardeningRequest(t, http.MethodGet, base+"/loki/api/v1/"+mode+"?"+params.Encode(), "", nil)
							if status != http.StatusOK {
								t.Fatalf("%s: status %d %s", base, status, body)
							}
							var response parserErrorMetricResponse
							if err := json.Unmarshal(body, &response); err != nil {
								t.Fatal(err)
							}
							if response.Status != "success" || len(response.Data.Result) != 1 || len(response.Data.Result[0].Metric) != 0 {
								t.Fatalf("%s: expected one series with no labels: %s", base, body)
							}
							points, kind, count := response.Data.Result[0].Values, "matrix", 2
							if mode == "query" {
								points, kind, count = [][]json.RawMessage{response.Data.Result[0].Value}, "vector", 1
							}
							if response.Data.ResultType != kind || len(points) != count {
								t.Fatalf("%s: wrong result shape: %s", base, body)
							}
							for i, point := range points {
								assertParserErrorMetricPoint(t, point, evaluation.Add(time.Duration(i)*time.Minute).Unix(), tc.want)
							}
						}
					}
				})
			}
		}
	}
}
