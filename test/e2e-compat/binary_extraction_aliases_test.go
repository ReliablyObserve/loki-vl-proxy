//go:build e2e

package e2e_compat

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/proxy"
)

func TestHardeningLive_BinaryExtractionAliases(t *testing.T) {
	p, err := proxy.New(proxy.Config{BackendURL: vlURL, Cache: cache.NewDisabled(), LogLevel: "error"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { p.Shutdown(context.Background()) })
	mux := http.NewServeMux()
	p.RegisterProxyRoutes(mux)
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	evaluation := time.Now().Add(-4 * time.Minute).Truncate(time.Minute)
	for _, extraction := range []struct{ name, stage string }{
		{"flat", `json chosen="status"`},
		{"nested", `json chosen="http.status"`},
		{"multiple", `json chosen="status", unused="other"`},
	} {
		service := fmt.Sprintf("hardening-binary-alias-%s-%d", extraction.name, time.Now().UnixNano())
		for i, status := range []string{"error", "error", "ok"} {
			stamp := evaluation.Add(-30 * time.Second).Add(time.Duration(i) * time.Millisecond)
			line := `{"status":"` + status + `","http":{"status":"` + status + `"},"other":"retained"}`
			row, _ := json.Marshal(map[string]string{"_time": stamp.UTC().Format(time.RFC3339Nano), "_msg": line, "service_name": service, "service.name": service})
			code, body := hardeningRequest(t, "POST", vlURL+"/insert/jsonline?_stream_fields=service_name,service.name", string(row)+"\n", map[string]string{"Content-Type": "application/stream+json"})
			if code != 200 {
				t.Fatalf("VL ingest %d %s", code, body)
			}
			payload, _ := json.Marshal(map[string]any{"streams": []any{map[string]any{"stream": map[string]string{"service_name": service}, "values": [][]string{{strconv.FormatInt(stamp.UnixNano(), 10), line}}}}})
			code, body = hardeningRequest(t, "POST", lokiURL+"/loki/api/v1/push", string(payload), map[string]string{"Content-Type": "application/json"})
			if code != 204 {
				t.Fatalf("Loki ingest %d %s", code, body)
			}
		}
		hardeningRequest(t, "POST", vlURL+"/internal/force_flush", "", nil)
		operand := `sum by(chosen)(count_over_time({service_name="` + service + `"} | ` + extraction.stage + ` | chosen="error" [5m]))`
		query := `(` + operand + ` + on() vector(1)) * 2`
		for _, endpoint := range []string{"query", "query_range"} {
			for _, base := range []string{lokiURL, server.URL, proxyURL} {
				t.Run(extraction.name+"/"+endpoint+"/"+base, func(t *testing.T) {
					params := url.Values{"query": {query}, "time": {evaluation.UTC().Format(time.RFC3339Nano)}, "start": {evaluation.UTC().Format(time.RFC3339Nano)}, "end": {evaluation.Add(time.Minute).UTC().Format(time.RFC3339Nano)}, "step": {"60"}}
					code, body := hardeningRequest(t, "GET", base+"/loki/api/v1/"+endpoint+"?"+params.Encode(), "", nil)
					var response struct {
						Data struct {
							Result []struct {
								Metric map[string]string
								Value  []any
								Values [][]any
							}
						}
					}
					if code != 200 || json.Unmarshal(body, &response) != nil || len(response.Data.Result) != 1 {
						t.Fatalf("expected one result: %d %s", code, body)
					}
					series := response.Data.Result[0]
					points := series.Values
					wantPoints := 2
					if endpoint == "query" {
						points = [][]any{series.Value}
						wantPoints = 1
					}
					if len(series.Metric) != 0 || len(points) != wantPoints {
						t.Fatalf("labels or shape differ: %s", body)
					}
					for i, point := range points {
						if len(point) != 2 || point[0] != float64(evaluation.Unix()+int64(i*60)) || point[1] != "6" {
							t.Fatalf("wrong alias value/timestamp: %s", body)
						}
					}
				})
			}
		}
	}
}
