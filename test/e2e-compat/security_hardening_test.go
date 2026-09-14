//go:build e2e

package e2e_compat

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
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

func hardeningRequest(t *testing.T, method, target, body string, headers map[string]string) (int, []byte) {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), method, target, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	for key, value := range headers {
		req.Header.Set(key, value)
	}
	client := &http.Client{Timeout: 15 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode, data
}

func TestHardeningLive_TopKChangingWinners(t *testing.T) {
	service := fmt.Sprintf("hardening-rank-%d", time.Now().UnixNano())
	stamp := time.Now().Add(-5 * time.Minute).Truncate(time.Minute)
	for step := 0; step < 2; step++ {
		for level := 0; level < 2; level++ {
			count := 1
			if level == step {
				count = 9
			}
			for i := 0; i < count; i++ {
				ts := stamp.Add(time.Duration(step*60-30)*time.Second + time.Duration(i)*time.Millisecond)
				labels := map[string]string{"service_name": service, "rank": strconv.Itoa(level)}
				row, _ := json.Marshal(map[string]string{"_time": ts.UTC().Format(time.RFC3339Nano), "_msg": "rank-marker", "service_name": service, "rank": labels["rank"]})
				status, body := hardeningRequest(t, "POST", vlURL+"/insert/jsonline?_stream_fields=service_name,rank", string(row)+"\n", map[string]string{"Content-Type": "application/stream+json"})
				if status != 200 {
					t.Fatalf("VL ingest: %d %s", status, body)
				}
				payload, _ := json.Marshal(map[string]any{"streams": []any{map[string]any{"stream": labels, "values": [][]string{{strconv.FormatInt(ts.UnixNano(), 10), "rank-marker"}}}}})
				status, body = hardeningRequest(t, "POST", lokiURL+"/loki/api/v1/push", string(payload), map[string]string{"Content-Type": "application/json"})
				if status != 204 {
					t.Fatalf("Loki ingest: %d %s", status, body)
				}
			}
		}
	}
	status, body := hardeningRequest(t, "POST", vlURL+"/internal/force_flush", "", nil)
	if status != 200 {
		t.Fatalf("flush: %d %s", status, body)
	}
	for _, fn := range []string{"rate", "count_over_time", "bytes_rate", "bytes_over_time"} {
		for _, op := range []string{"topk", "bottomk"} {
			// Empty leading/trailing windows must not create zero-valued winners.
			q := url.Values{"query": {op + `(1, sum by (rank) (` + fn + `({service_name="` + service + `"}[1m])))`}, "start": {strconv.FormatInt(stamp.Add(-2*time.Minute).Unix(), 10)}, "end": {strconv.FormatInt(stamp.Add(3*time.Minute).Unix(), 10)}, "step": {"60"}}
			want := map[int64]string{stamp.Unix(): "0", stamp.Add(time.Minute).Unix(): "1"}
			if op == "bottomk" {
				want = map[int64]string{stamp.Unix(): "1", stamp.Add(time.Minute).Unix(): "0"}
			}
			for _, base := range []string{lokiURL, proxyURL} {
				status, body := hardeningRequest(t, "GET", base+"/loki/api/v1/query_range?"+q.Encode(), "", nil)
				if status != 200 {
					t.Fatalf("%s %s: %d %s", base, op, status, body)
				}
				var response struct {
					Data struct {
						Result []struct {
							Metric map[string]string
							Values [][]any
						}
					}
				}
				if err := json.Unmarshal(body, &response); err != nil {
					t.Fatal(err)
				}
				got := map[int64]string{}
				for _, series := range response.Data.Result {
					for _, point := range series.Values {
						ts := int64(point[0].(float64))
						if _, exists := got[ts]; exists {
							t.Fatalf("multiple winners at %d: %s", ts, body)
						}
						got[ts] = series.Metric["rank"]
					}
				}
				if !reflect.DeepEqual(got, want) {
					t.Fatalf("%s %s winners: got=%v want=%v body=%s", base, op, got, want, body)
				}
			}
		}
	}
}

func hardeningIngest(t *testing.T, service, org, account, marker string, timestamp time.Time) {
	t.Helper()
	row, _ := json.Marshal(map[string]string{"_time": timestamp.UTC().Format(time.RFC3339Nano), "_msg": marker, "service_name": service, "org_id": org, "level": "info"})
	status, body := hardeningRequest(t, "POST", vlURL+"/insert/jsonline?_stream_fields=service_name,org_id,level", string(row)+"\n", map[string]string{"AccountID": account, "ProjectID": "0", "Content-Type": "application/stream+json"})
	if status < 200 || status >= 300 {
		t.Fatalf("VL ingest %d: %s", status, body)
	}
}

func TestHardeningLive_TenantIsolationAcrossNativeLabelAndColdReads(t *testing.T) {
	service := fmt.Sprintf("hardening-%d", time.Now().UnixNano())
	stamp := time.Now().Add(-2 * time.Minute).Truncate(time.Second)
	for _, account := range []string{"0", "540001", "540002"} {
		for _, org := range []string{"hardening-a", "hardening-b", "default"} {
			hardeningIngest(t, service, org, account, account+"-"+org, stamp)
		}
	}
	status, body := hardeningRequest(t, "POST", vlURL+"/internal/force_flush", "", nil)
	if status != 200 {
		t.Fatalf("flush %d: %s", status, body)
	}
	for _, labelMode := range []bool{false, true} {
		for _, cold := range []bool{false, true} {
			for _, cached := range []bool{false, true} {
				t.Run(fmt.Sprintf("label=%v/cold=%v/cache=%v", labelMode, cold, cached), func(t *testing.T) {
					cfg := proxy.Config{BackendURL: vlURL, Cache: cache.NewDisabled(), LogLevel: "error", RequireTenantHeader: true}
					if labelMode {
						cfg.TenantLabel = "org_id"
					} else {
						cfg.TenantMap = map[string]proxy.TenantMapping{"hardening-a": {AccountID: "540001", ProjectID: "0"}, "hardening-b": {AccountID: "540002", ProjectID: "0"}}
					}
					if cached {
						cfg.Cache = cache.New(time.Minute, 100)
						cfg.CompatCache = cache.New(time.Minute, 100)
					}
					if cold {
						cfg.ColdBackend = proxy.ColdBackendConfig{Enabled: true, URL: vlURL, Boundary: time.Second, Overlap: time.Nanosecond}
					}
					p, err := proxy.New(cfg)
					if err != nil {
						t.Fatal(err)
					}
					defer p.Shutdown(context.Background())
					mux := http.NewServeMux()
					p.RegisterProxyRoutes(mux)
					server := httptest.NewServer(mux)
					defer server.Close()
					q := url.Values{"query": {`{service_name="` + service + `"}`}, "start": {strconv.FormatInt(stamp.Add(-time.Second).UnixNano(), 10)}, "end": {strconv.FormatInt(stamp.Add(time.Second).UnixNano(), 10)}, "direction": {"forward"}}
					for repeat := 0; repeat < 2; repeat++ {
						for _, org := range []string{"hardening-a", "hardening-b"} {
							status, body := hardeningRequest(t, "GET", server.URL+"/loki/api/v1/query_range?"+q.Encode(), "", map[string]string{"X-Scope-OrgID": org})
							if status != 200 {
								t.Fatalf("query %s: %d %s", org, status, body)
							}
							wantPrefix := "540001-"
							foreignPrefix := "540002-"
							if org == "hardening-b" {
								wantPrefix, foreignPrefix = foreignPrefix, wantPrefix
							}
							if labelMode {
								wantPrefix = "0-" + org
								foreignPrefix = "0-hardening-b"
								if org == "hardening-b" {
									foreignPrefix = "0-hardening-a"
								}
							}
							if !bytes.Contains(body, []byte(wantPrefix)) || bytes.Contains(body, []byte(foreignPrefix)) || bytes.Contains(body, []byte("0-default")) {
								t.Fatalf("tenant %s leaked/missed data: %s", org, body)
							}
						}
					}
					if labelMode {
						status, _ := hardeningRequest(t, "GET", server.URL+"/loki/api/v1/query_range?"+q.Encode(), "", map[string]string{"X-Scope-OrgID": "*"})
						if status != 403 {
							t.Fatalf("wildcard denial: %d", status)
						}
					}
				})
			}
		}
	}
}

func TestHardeningLive_ExactWindowsAndFormattingMatchLoki(t *testing.T) {
	service := fmt.Sprintf("hardening-parity-%d", time.Now().UnixNano())
	stamp := time.Now().Add(-3 * time.Minute).Truncate(5 * time.Minute).Add(time.Second)
	for i, marker := range []string{"first-marker", "second-marker"} {
		ts := stamp.Add(time.Duration(i*2) * time.Second)
		hardeningIngest(t, service, "default", "0", marker, ts)
		payload, _ := json.Marshal(map[string]any{"streams": []any{map[string]any{"stream": map[string]string{"service_name": service, "org_id": "default", "level": "info"}, "values": [][]string{{strconv.FormatInt(ts.UnixNano(), 10), marker}}}}})
		status, body := hardeningRequest(t, "POST", lokiURL+"/loki/api/v1/push", string(payload), map[string]string{"Content-Type": "application/json"})
		if status != 204 {
			t.Fatalf("Loki ingest %d: %s", status, body)
		}
	}
	hardeningRequest(t, "POST", vlURL+"/internal/force_flush", "", nil)
	// Check malformed templates against actual data, not an accidentally empty
	// epoch window: Loki can skip stage evaluation when no streams are selected.
	invalid := url.Values{"query": {`{service_name="` + service + `"} | line_format "{{.service_name"`}, "start": {strconv.FormatInt(stamp.UnixNano(), 10)}, "end": {strconv.FormatInt(stamp.Add(time.Second).UnixNano(), 10)}}
	for _, base := range []string{lokiURL, proxyURL} {
		status, body := hardeningRequest(t, "GET", base+"/loki/api/v1/query_range?"+invalid.Encode(), "", nil)
		if status != http.StatusBadRequest {
			t.Fatalf("%s malformed formatting: %d %s", base, status, body)
		}
	}
	for _, format := range []string{"", " | line_format `{{.service_name}}`", " | line_format \"{{printf \\\"%s\\\" .service_name}}\""} {
		for i := 0; i < 2; i++ {
			start := stamp.Add(time.Duration(2*i) * time.Second)
			q := url.Values{"query": {`{service_name="` + service + `"}` + format}, "start": {strconv.FormatInt(start.UnixNano(), 10)}, "end": {strconv.FormatInt(start.Add(time.Second).UnixNano(), 10)}, "direction": {"forward"}, "limit": {"10"}}
			for repeat := 0; repeat < 2; repeat++ {
				var lines [][]string
				for _, base := range []string{lokiURL, proxyURL} {
					status, body := hardeningRequest(t, "GET", base+"/loki/api/v1/query_range?"+q.Encode(), "", nil)
					if status != 200 {
						t.Fatalf("%s query %s: %d %s", base, q.Get("query"), status, body)
					}
					var response struct {
						Data struct {
							Result []struct{ Values [][]json.RawMessage }
						}
					}
					if err := json.Unmarshal(body, &response); err != nil {
						t.Fatal(err)
					}
					var values []string
					for _, stream := range response.Data.Result {
						for _, tuple := range stream.Values {
							var line string
							if len(tuple) < 2 || json.Unmarshal(tuple[1], &line) != nil {
								t.Fatal("invalid tuple")
							}
							values = append(values, line)
						}
					}
					lines = append(lines, values)
				}
				if len(lines[0]) != 1 || len(lines[1]) != 1 || lines[0][0] != lines[1][0] {
					t.Fatalf("Loki/proxy visible lines differ: %v", lines)
				}
			}
		}
	}
}
