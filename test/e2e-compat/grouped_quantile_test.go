//go:build e2e

package e2e_compat

import (
	"encoding/json"
	"fmt"
	"math"
	"net/url"
	"strconv"
	"testing"
	"time"
)

func TestHardeningLive_GroupedQuantile(t *testing.T) {
	service := fmt.Sprintf("hardening-quantile-%d", time.Now().UnixNano())
	// Align both tested steps: Loki's frontend rounds metric range boundaries
	// to the step grid before evaluating the query.
	stamp := time.Now().Add(-6 * time.Minute).Truncate(5 * time.Minute).UTC()
	for i, row := range []struct {
		seconds, value int
		level          string
	}{
		{-300, 1000, "info"}, // exactly on the excluded lower window boundary
		{-240, 10, "info"}, {-60, 30, "info"}, {0, 50, "info"}, {60, 90, "info"},
		{-60, 100, "error"}, {0, 200, "error"}, {60, 400, "error"},
	} {
		ts := stamp.Add(time.Duration(row.seconds) * time.Second)
		message := fmt.Sprintf(`{"latency":%d}`, row.value)
		labels := map[string]string{"service_name": service, "level": row.level, "pod": fmt.Sprintf("pod-%d", i)}
		vlRow, _ := json.Marshal(map[string]string{
			"_time": ts.Format(time.RFC3339Nano), "_msg": message,
			"service_name": service, "level": row.level, "pod": labels["pod"],
		})
		status, body := hardeningRequest(t, "POST", vlURL+"/insert/jsonline?_stream_fields=service_name,level,pod", string(vlRow)+"\n", map[string]string{"Content-Type": "application/stream+json"})
		if status != 200 {
			t.Fatalf("VL ingest: %d %s", status, body)
		}
		payload, _ := json.Marshal(map[string]any{"streams": []any{map[string]any{"stream": labels, "values": [][]string{{strconv.FormatInt(ts.UnixNano(), 10), message}}}}})
		status, body = hardeningRequest(t, "POST", lokiURL+"/loki/api/v1/push", string(payload), map[string]string{"Content-Type": "application/json"})
		if status != 204 {
			t.Fatalf("Loki ingest: %d %s", status, body)
		}
	}
	status, body := hardeningRequest(t, "POST", vlURL+"/internal/force_flush", "", nil)
	if status != 200 {
		t.Fatalf("VL flush: %d %s", status, body)
	}
	for _, level := range []string{"info", "error"} {
		waitForLokiMetricDataSelector(t, fmt.Sprintf(`{service_name=%q,level=%q}`, service, level))
	}
	for _, tc := range []struct {
		phi, grouping string
		want          map[string][]float64
	}{
		{"0.5", "by (level)", map[string][]float64{"info": {30, 50, 50, 50, 70, 90}, "error": {150, 200, 200, 200, 300, 400}}},
		{"0.5", "by ()", map[string][]float64{"": {50, 95, 95, 95, 145, 245}}},
		{"0.95", "by (level)", map[string][]float64{"info": {48, 86, 86, 86, 88, 90}, "error": {195, 380, 380, 380, 390, 400}}},
		{"0.95", "by ()", map[string][]float64{"": {180, 350, 350, 350, 370, 384.5}}},
	} {
		t.Run(tc.phi+"/"+tc.grouping, func(t *testing.T) {
			query := fmt.Sprintf(`quantile_over_time(%s, {service_name=%q} | json | unwrap latency [5m]) %s`, tc.phi, service, tc.grouping)
			for _, step := range []int{60, 300} {
				params := url.Values{"query": {query}, "start": {stamp.Format(time.RFC3339Nano)}, "end": {stamp.Add(10 * time.Minute).Format(time.RFC3339Nano)}, "step": {strconv.Itoa(step)}}
				for repeat := 0; repeat < 2; repeat++ {
					for _, base := range []string{lokiURL, proxyURL} {
						assertHardeningQuantile(t, base+"/loki/api/v1/query_range?"+params.Encode(), stamp, step, tc.want, false)
					}
				}
			}
			params := url.Values{"query": {query}, "time": {stamp.Format(time.RFC3339Nano)}}
			for repeat := 0; repeat < 2; repeat++ {
				for _, base := range []string{lokiURL, proxyURL} {
					assertHardeningQuantile(t, base+"/loki/api/v1/query?"+params.Encode(), stamp, 60, tc.want, true)
				}
			}
		})
	}
}

func assertHardeningQuantile(t *testing.T, target string, stamp time.Time, step int, want map[string][]float64, instant bool) {
	t.Helper()
	status, body := hardeningRequest(t, "GET", target, "", nil)
	if status != 200 {
		t.Fatalf("%s: status %d %s", target, status, body)
	}
	var response struct {
		Data struct {
			ResultType string
			Result     []struct {
				Metric map[string]string
				Values [][]json.RawMessage
				Value  []json.RawMessage
			}
		}
	}
	if err := json.Unmarshal(body, &response); err != nil {
		t.Fatal(err)
	}
	kind := "matrix"
	if instant {
		kind = "vector"
	}
	if response.Data.ResultType != kind || len(response.Data.Result) != len(want) {
		t.Fatalf("%s: wrong result shape/groups: %s", target, body)
	}
	seen := make(map[string]bool)
	for _, series := range response.Data.Result {
		level := series.Metric["level"]
		expected, ok := want[level]
		labels := 0
		if level != "" {
			labels = 1
		}
		if !ok || seen[level] || len(series.Metric) != labels {
			t.Fatalf("%s: unexpected/duplicate labels: %s", target, body)
		}
		seen[level] = true
		points, count := series.Values, (360+step-1)/step
		if instant {
			points, count = [][]json.RawMessage{series.Value}, 1
		}
		if len(points) != count {
			t.Fatalf("%s: wrong points or nonempty trailing window: %s", target, body)
		}
		for i, point := range points {
			if len(point) != 2 {
				t.Fatalf("invalid point: %s", body)
			}
			var ts float64
			var raw string
			if json.Unmarshal(point[0], &ts) != nil || json.Unmarshal(point[1], &raw) != nil {
				t.Fatalf("invalid point: %s", body)
			}
			value, err := strconv.ParseFloat(raw, 64)
			if err != nil || math.IsNaN(value) || ts != float64(stamp.Unix()+int64(i*step)) || math.Abs(value-expected[i*step/60]) > 1e-9 {
				t.Fatalf("%s: group %q point %d = [%v %s], want [%d %v]", target, level, i, ts, raw, stamp.Unix()+int64(i*step), expected[i*step/60])
			}
		}
	}
}
