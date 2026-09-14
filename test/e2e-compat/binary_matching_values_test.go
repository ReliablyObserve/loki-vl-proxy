//go:build e2e

package e2e_compat

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/proxy"
)

func TestHardeningLive_BinaryMatchingValues(t *testing.T) {
	service := fmt.Sprintf("hardening-binary-values-%d", time.Now().UnixNano())
	evaluation := time.Now().Add(-4 * time.Minute).Truncate(time.Minute)
	stamp := evaluation.Add(-30 * time.Second)
	for level, count := range map[string]int{"info": 1, "error": 2} {
		for i := 0; i < count; i++ {
			ts := stamp.Add(time.Duration(i) * time.Millisecond)
			labels := map[string]string{"service_name": service, "level": level}
			line := fmt.Sprintf("binary-value-%s-%d", level, i)
			// Exercise both a cold instance and the production mapping from
			// the Loki label service_name to the stored OTLP service.name.
			row, _ := json.Marshal(map[string]string{"_time": ts.UTC().Format(time.RFC3339Nano), "_msg": line, "service_name": service, "service.name": service, "level": level})
			status, body := hardeningRequest(t, "POST", vlURL+"/insert/jsonline?_stream_fields=service_name,service.name,level", string(row)+"\n", map[string]string{"Content-Type": "application/stream+json"})
			if status != 200 {
				t.Fatalf("VL ingest %d %s", status, body)
			}
			payload, _ := json.Marshal(map[string]any{"streams": []any{map[string]any{"stream": labels, "values": [][]string{{strconv.FormatInt(ts.UnixNano(), 10), line}}}}})
			status, body = hardeningRequest(t, "POST", lokiURL+"/loki/api/v1/push", string(payload), map[string]string{"Content-Type": "application/json"})
			if status != 204 {
				t.Fatalf("Loki ingest %d %s", status, body)
			}
		}
	}
	hardeningRequest(t, "POST", vlURL+"/internal/force_flush", "", nil)
	p, err := proxy.New(proxy.Config{BackendURL: vlURL, Cache: cache.NewDisabled(), LogLevel: "error"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { p.Shutdown(context.Background()) })
	mux := http.NewServeMux()
	p.RegisterProxyRoutes(mux)
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	selector := `{service_name="` + service + `"}`
	all := `sum by(service_name,level)(count_over_time(` + selector + `[5m]))`
	total := `sum by(service_name)(count_over_time(` + selector + `[5m]))`
	info := `sum by(service_name,level)(count_over_time({service_name="` + service + `",level="info"}[5m]))`
	for _, tc := range []struct {
		name, query string
		want        map[string]float64
		withService bool
	}{
		{"named_on", total + ` / on(service_name) ` + total, map[string]float64{"": 1}, true},
		{"ignoring", info + ` / ignoring(level) ` + total, map[string]float64{"": 1.0 / 3}, true},
		{"group_left", all + ` / on(service_name) group_left() ` + total, map[string]float64{"info": 1.0 / 3, "error": 2.0 / 3}, true},
		{"group_right", total + ` / on(service_name) group_right() ` + all, map[string]float64{"info": 3, "error": 1.5}, true},
		{"empty_on", total + ` / on() sum(count_over_time(` + selector + `[5m]))`, map[string]float64{"": 1}, false},
		{"comparison_filter", all + ` > ignoring(level) group_left() sum by(service_name)(count_over_time({service_name="` + service + `",level="info"}[5m]))`, map[string]float64{"error": 2}, true},
		{"comparison_bool", all + ` > bool ignoring(level) group_left() sum by(service_name)(count_over_time({service_name="` + service + `",level="info"}[5m]))`, map[string]float64{"info": 0, "error": 1}, true},
		{"set_and", all + ` and on(service_name) ` + all, map[string]float64{"info": 1, "error": 2}, true},
		{"set_or", info + ` or on(service_name) ` + all, map[string]float64{"info": 1}, true},
		{"set_unless", all + ` unless on(level) ` + info, map[string]float64{"error": 2}, true},
		{"nested", `(` + total + ` + ` + total + `) / on(service_name) ` + total, map[string]float64{"": 2}, true},
		{"precedence", total + ` + on(service_name) ` + total + ` * ` + total, map[string]float64{"": 12}, true},
		{"precedence_parentheses", `(` + total + ` + ` + total + `) * ` + total, map[string]float64{"": 18}, true},
		{"power_right_associativity", total + ` ^ ` + total + ` ^ 2`, map[string]float64{"": 19683}, true},
		{"scalar_subtree", total + ` * (1 + 2)`, map[string]float64{"": 9}, true},
		{"constant_vector", total + ` + on() vector(1)`, map[string]float64{"": 4}, false},
	} {
		for _, endpoint := range []string{"query", "query_range"} {
			for _, base := range []string{lokiURL, server.URL, proxyURL} {
				t.Run(tc.name+"/"+endpoint+"/"+base, func(t *testing.T) {
					q := url.Values{"query": {tc.query}, "time": {evaluation.UTC().Format(time.RFC3339Nano)}, "start": {evaluation.UTC().Format(time.RFC3339Nano)}, "end": {evaluation.Add(time.Minute).UTC().Format(time.RFC3339Nano)}, "step": {"60"}}
					var response struct {
						Data struct {
							Result []struct {
								Metric map[string]string
								Value  []any
								Values [][]any
							}
						}
					}
					deadline := time.Now().Add(90 * time.Second)
					var body []byte
					for {
						status, data := hardeningRequest(t, "GET", base+"/loki/api/v1/"+endpoint+"?"+q.Encode(), "", nil)
						body = data
						if status != 200 || json.Unmarshal(body, &response) != nil {
							t.Fatalf("query %d %s", status, body)
						}
						if len(response.Data.Result) == len(tc.want) || base != lokiURL || time.Now().After(deadline) {
							break
						}
						time.Sleep(250 * time.Millisecond)
					}
					if len(response.Data.Result) != len(tc.want) {
						t.Fatalf("series count: %s", body)
					}
					for _, series := range response.Data.Result {
						want, ok := tc.want[series.Metric["level"]]
						if !ok {
							t.Fatalf("unexpected labels: %s", body)
						}
						if tc.withService && series.Metric["service_name"] != service {
							t.Fatalf("lost service label: %s", body)
						}
						if !tc.withService && len(series.Metric) != 0 {
							t.Fatalf("unexpected labels: %s", body)
						}
						points := series.Values
						wantCount := 2
						if endpoint == "query" {
							points = [][]any{series.Value}
							wantCount = 1
						}
						if len(points) != wantCount {
							t.Fatalf("sample count: %s", body)
						}
						for i, point := range points {
							if len(point) != 2 || point[0] != float64(evaluation.Add(time.Duration(i)*time.Minute).Unix()) {
								t.Fatalf("timestamp: %s", body)
							}
							value, err := strconv.ParseFloat(fmt.Sprint(point[1]), 64)
							if err != nil || math.Abs(value-want) > 1e-10 {
								t.Fatalf("want %g: %s", want, body)
							}
						}
					}
				})
			}
		}
	}
}
