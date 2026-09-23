//go:build e2e

package e2e_compat

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/proxy"
)

func TestHardeningLive_RawMetricLimits(t *testing.T) {
	app := fmt.Sprintf("raw-metric-limits-%d", time.Now().UnixNano())
	evaluation := time.Now().Add(-6 * time.Minute).Truncate(5 * time.Minute)
	fixtures := []struct {
		kind          string
		offset, value int
	}{{"0", -30, 1}, {"1", -30, 2}, {"2", -30, 3}, {"boundary", -300, 999}, {"boundary", -120, 1}, {"boundary", 0, 3}}
	for _, fixture := range fixtures {
		stamp := evaluation.Add(time.Duration(fixture.offset) * time.Second)
		labels := map[string]string{"app": app, "kind": fixture.kind}
		line := fmt.Sprintf(`{"value":%d}`, fixture.value)
		row, _ := json.Marshal(map[string]string{"_time": stamp.Format(time.RFC3339Nano), "_msg": line, "app": app, "kind": fixture.kind, "detected_level": "unknown"})
		status, body := hardeningRequest(t, http.MethodPost, vlURL+"/insert/jsonline?_stream_fields=app,kind", string(row)+"\n", map[string]string{"Content-Type": "application/stream+json"})
		if status != 200 {
			t.Fatalf("VL fixture ingest: %d %s", status, body)
		}
		payload, _ := json.Marshal(map[string]any{"streams": []any{map[string]any{"stream": labels, "values": [][]any{{strconv.FormatInt(stamp.UnixNano(), 10), line, map[string]string{"detected_level": "unknown"}}}}}})
		status, body = hardeningRequest(t, http.MethodPost, lokiURL+"/loki/api/v1/push", string(payload), map[string]string{"Content-Type": "application/json"})
		if status != 204 {
			t.Fatalf("Loki fixture ingest: %d %s", status, body)
		}
	}
	hardeningRequest(t, http.MethodPost, vlURL+"/internal/force_flush", "", nil)
	p, err := proxy.New(proxy.Config{BackendURL: vlURL, Cache: cache.NewDisabled(), MaxStatsQuerySeries: 2, LogLevel: "error"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { p.Shutdown(context.Background()) })
	mux := http.NewServeMux()
	p.RegisterProxyRoutes(mux)
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	for _, mode := range []string{"query", "query_range"} {
		t.Run(mode, func(t *testing.T) {
			params := url.Values{"query": {`avg_over_time({app="` + app + `"}|json|unwrap value[5m])`}, "time": {evaluation.Format(time.RFC3339Nano)}, "start": {evaluation.Add(-time.Minute).Format(time.RFC3339Nano)}, "end": {evaluation.Format(time.RFC3339Nano)}, "step": {"60"}}
			status, body := hardeningRequest(t, http.MethodGet, server.URL+"/loki/api/v1/"+mode+"?"+params.Encode(), "", nil)
			if status < 400 || !strings.Contains(string(body), "maximum number of series (2) reached") || strings.Contains(string(body), `"result"`) {
				t.Fatalf("overflow must be visible without partial data: %d %s", status, body)
			}
			params.Set("query", `avg_over_time({app="`+app+`",kind="0"}|json|unwrap value[5m])`)
			for _, base := range []string{server.URL, lokiURL} {
				deadline := time.Now().Add(time.Minute)
				var response parserErrorMetricResponse
				for {
					status, body = hardeningRequest(t, http.MethodGet, base+"/loki/api/v1/"+mode+"?"+params.Encode(), "", nil)
					if status == 200 && json.Unmarshal([]byte(body), &response) == nil && len(response.Data.Result) == 1 {
						break
					}
					if base != lokiURL || time.Now().After(deadline) {
						t.Fatalf("next narrow query must succeed: %s %d %s", base, status, body)
					}
					time.Sleep(time.Second)
				}
				result := response.Data.Result[0]
				wantLabels := map[string]string{"app": app, "kind": "0", "detected_level": "unknown", "service_name": app}
				if !reflect.DeepEqual(result.Metric, wantLabels) {
					t.Fatalf("narrow result labels: %s got=%v want=%v", base, result.Metric, wantLabels)
				}
				point := result.Value
				if mode == "query_range" {
					if len(result.Values) != 1 {
						t.Fatalf("narrow range values: %s %v", base, result.Values)
					}
					point = result.Values[0]
				}
				assertParserErrorMetricPoint(t, point, evaluation.Unix(), 1)
			}
		})
	}
	for _, mode := range []string{"query", "query_range"} {
		t.Run("boundaries/"+mode, func(t *testing.T) {
			params := url.Values{"query": {`avg_over_time({app="` + app + `",kind="boundary"}|json|unwrap value[5m])`}, "time": {evaluation.Format(time.RFC3339Nano)}, "start": {evaluation.Format(time.RFC3339Nano)}, "end": {evaluation.Add(5 * time.Minute).Format(time.RFC3339Nano)}, "step": {"300"}}
			for _, base := range []string{server.URL, lokiURL} {
				status, body := hardeningRequest(t, http.MethodGet, base+"/loki/api/v1/"+mode+"?"+params.Encode(), "", nil)
				var response parserErrorMetricResponse
				if status != 200 || json.Unmarshal(body, &response) != nil || len(response.Data.Result) != 1 {
					t.Fatalf("boundary query: %s %d %s", base, status, body)
				}
				point := response.Data.Result[0].Value
				if mode == "query_range" {
					if len(response.Data.Result[0].Values) != 1 {
						t.Fatalf("empty trailing window must be absent: %s", body)
					}
					point = response.Data.Result[0].Values[0]
				}
				assertParserErrorMetricPoint(t, point, evaluation.Unix(), 2)
			}
			params.Set("time", evaluation.Add(5*time.Minute).Format(time.RFC3339Nano))
			for _, base := range []string{server.URL, lokiURL} {
				status, body := hardeningRequest(t, http.MethodGet, base+"/loki/api/v1/query?"+params.Encode(), "", nil)
				var response parserErrorMetricResponse
				if status != 200 || json.Unmarshal(body, &response) != nil || len(response.Data.Result) != 0 {
					t.Fatalf("empty instant window must be absent: %s %d %s", base, status, body)
				}
			}
		})
	}
}
