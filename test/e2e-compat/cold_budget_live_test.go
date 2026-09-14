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

func TestHardeningLive_ColdMergeWithOneBackendPermit(t *testing.T) {
	service := fmt.Sprintf("hardening-cold-budget-%d", time.Now().UnixNano())
	now := time.Now().UTC()
	hardeningIngest(t, service, "default", "0", "cold-permit-marker", now.Add(-2*time.Minute))
	hardeningIngest(t, service, "default", "0", "hot-permit-marker", now.Add(-10*time.Second))
	status, body := hardeningRequest(t, "POST", vlURL+"/internal/force_flush", "", nil)
	if status != 200 {
		t.Fatalf("flush %d: %s", status, body)
	}
	p, err := proxy.New(proxy.Config{BackendURL: vlURL, Cache: cache.NewDisabled(), LogLevel: "error", MaxConcurrent: 1, ColdBackend: proxy.ColdBackendConfig{Enabled: true, URL: vlURL, Boundary: time.Minute, Overlap: time.Nanosecond}})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Shutdown(context.Background())
	mux := http.NewServeMux()
	p.RegisterProxyRoutes(mux)
	server := httptest.NewServer(mux)
	defer server.Close()
	for _, direction := range []string{"forward", "backward"} {
		q := url.Values{"query": {`{service_name="` + service + `"}`}, "start": {strconv.FormatInt(now.Add(-3*time.Minute).UnixNano(), 10)}, "end": {strconv.FormatInt(now.UnixNano(), 10)}, "direction": {direction}, "limit": {"2"}}
		status, body := hardeningRequest(t, "GET", server.URL+"/loki/api/v1/query_range?"+q.Encode(), "", nil)
		var response struct {
			Data struct {
				Result []struct{ Values [][]json.RawMessage }
			}
		}
		if status != 200 || json.Unmarshal(body, &response) != nil {
			t.Fatalf("%s merge: %d %s", direction, status, body)
		}
		seen := map[string]int{}
		for _, stream := range response.Data.Result {
			for _, point := range stream.Values {
				if len(point) < 2 {
					t.Fatalf("invalid tuple %s", body)
				}
				var line string
				if json.Unmarshal(point[1], &line) != nil {
					t.Fatalf("invalid line %s", body)
				}
				seen[line]++
			}
		}
		if len(seen) != 2 || seen["hot-permit-marker"] != 1 || seen["cold-permit-marker"] != 1 {
			t.Fatalf("%s missing or duplicated merged rows: %s", direction, body)
		}
	}
}
