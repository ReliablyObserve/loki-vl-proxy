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

func TestHardeningLive_RegexpCaptureAliases(t *testing.T) {
	service := fmt.Sprintf("hardening-regexp-%d", time.Now().UnixNano())
	stamp := time.Now().Add(-4 * time.Minute).Truncate(time.Second)
	for _, method := range []string{"GET", "POST"} {
		line := method + " /capture-marker"
		labels := map[string]string{"service_name": service}
		row, _ := json.Marshal(map[string]string{"_time": stamp.UTC().Format(time.RFC3339Nano), "_msg": line, "service_name": service})
		status, body := hardeningRequest(t, "POST", vlURL+"/insert/jsonline?_stream_fields=service_name", string(row)+"\n", map[string]string{"Content-Type": "application/stream+json"})
		if status != 200 {
			t.Fatalf("VL ingest: %d %s", status, body)
		}
		payload, _ := json.Marshal(map[string]any{"streams": []any{map[string]any{"stream": labels, "values": [][]string{{strconv.FormatInt(stamp.UnixNano(), 10), line}}}}})
		status, body = hardeningRequest(t, "POST", lokiURL+"/loki/api/v1/push", string(payload), map[string]string{"Content-Type": "application/json"})
		if status != 204 {
			t.Fatalf("Loki ingest: %d %s", status, body)
		}
	}
	status, body := hardeningRequest(t, "POST", vlURL+"/internal/force_flush", "", nil)
	if status != 200 {
		t.Fatalf("VL flush: %d %s", status, body)
	}
	// Explicit mapping reproduces a warmed backend field inventory regardless
	// of test order. The capture does not collide with fixture labels or metadata.
	p, err := proxy.New(proxy.Config{
		BackendURL: vlURL, Cache: cache.NewDisabled(), LogLevel: "error",
		LabelStyle:    proxy.LabelStyleUnderscores,
		FieldMappings: []proxy.FieldMapping{{LokiLabel: "http_method", VLField: "http.method"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { p.Shutdown(context.Background()) })
	mux := http.NewServeMux()
	p.RegisterProxyRoutes(mux)
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	for _, base := range []string{lokiURL, server.URL, proxyURL} {
		for _, method := range []string{"GET", "POST"} {
			t.Run(base+"/"+method, func(t *testing.T) {
				q := url.Values{
					"query": {`{service_name="` + service + `"} | regexp "(?P<http_method>[A-Z]+)" | http_method="` + method + `"`},
					"start": {stamp.Add(-time.Second).UTC().Format(time.RFC3339Nano)},
					"end":   {stamp.Add(time.Second).UTC().Format(time.RFC3339Nano)},
				}
				status, body := hardeningRequest(t, "GET", base+"/loki/api/v1/query_range?"+q.Encode(), "", nil)
				if status != 200 {
					t.Fatalf("query: %d %s", status, body)
				}
				var response struct {
					Data struct {
						Result []struct {
							Stream map[string]string `json:"stream"`
							Values [][]any           `json:"values"`
						} `json:"result"`
					} `json:"data"`
				}
				if err := json.Unmarshal(body, &response); err != nil {
					t.Fatal(err)
				}
				if len(response.Data.Result) != 1 {
					t.Fatalf("want exactly one stream: %s", body)
				}
				stream := response.Data.Result[0]
				if len(stream.Values) != 1 || len(stream.Values[0]) < 2 || stream.Values[0][1] != method+" /capture-marker" {
					t.Fatalf("wrong selected log: %s", body)
				}
				if stream.Stream["http_method"] != method {
					t.Fatalf("missing or wrong capture label: %s", body)
				}
			})
		}
	}
}
