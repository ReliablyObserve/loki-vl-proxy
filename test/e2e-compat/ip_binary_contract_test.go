//go:build e2e

package e2e_compat

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestHardeningLive_IPAndBinaryCardinality(t *testing.T) {
	service := fmt.Sprintf("hardening-ip-binary-%d", time.Now().UnixNano())
	stamp := time.Now().Add(-4 * time.Minute).Truncate(time.Second)
	for _, level := range []string{"info", "error"} {
		labels := map[string]string{"service_name": service, "level": level}
		line := "ip(bad) " + level
		row, _ := json.Marshal(map[string]string{"_time": stamp.UTC().Format(time.RFC3339Nano), "_msg": line, "service_name": service, "level": level})
		status, body := hardeningRequest(t, "POST", vlURL+"/insert/jsonline?_stream_fields=service_name,level", string(row)+"\n", map[string]string{"Content-Type": "application/stream+json"})
		if status != 200 {
			t.Fatalf("VL ingest %d: %s", status, body)
		}
		payload, _ := json.Marshal(map[string]any{"streams": []any{map[string]any{"stream": labels, "values": [][]string{{strconv.FormatInt(stamp.UnixNano(), 10), line}}}}})
		status, body = hardeningRequest(t, "POST", lokiURL+"/loki/api/v1/push", string(payload), map[string]string{"Content-Type": "application/json"})
		if status != 204 {
			t.Fatalf("Loki ingest %d: %s", status, body)
		}
	}
	hardeningRequest(t, "POST", vlURL+"/internal/force_flush", "", nil)
	selector := `{service_name="` + service + `"}`
	q := url.Values{"query": {selector + ` |= "ip(bad)"`}, "start": {stamp.Add(-time.Second).UTC().Format(time.RFC3339Nano)}, "end": {stamp.Add(time.Second).UTC().Format(time.RFC3339Nano)}}
	// A nonempty literal canary verifies both timestamp units and preservation
	// of ordinary strings which happen to look like an ip() expression.
	for _, base := range []string{lokiURL, proxyURL} {
		status, body := hardeningRequest(t, "GET", base+"/loki/api/v1/query_range?"+q.Encode(), "", nil)
		if status != 200 || !strings.Contains(string(body), "ip(bad) info") || !strings.Contains(string(body), "ip(bad) error") {
			t.Fatalf("%s literal canary: %d %s", base, status, body)
		}
	}
	for _, pattern := range []string{"999.999.999.999", "not-an-ip", "10.0.0.0/33", "::gggg", "10.0.0.2-10.0.0.1"} {
		q.Set("query", selector+` |= ip("`+pattern+`")`)
		for _, base := range []string{lokiURL, proxyURL} {
			status, body := hardeningRequest(t, "GET", base+"/loki/api/v1/query_range?"+q.Encode(), "", nil)
			if status != http.StatusBadRequest {
				t.Errorf("%s invalid %s: %d %s", base, pattern, status, body)
			}
		}
	}
	q.Set("query", `sum by(service_name,level)(count_over_time(`+selector+`[5m])) / ignoring(level) sum by(service_name)(count_over_time(`+selector+`[5m]))`)
	evaluation := stamp.Truncate(time.Minute).Add(time.Minute)
	q.Set("start", evaluation.UTC().Format(time.RFC3339Nano))
	q.Set("end", evaluation.Add(time.Minute).UTC().Format(time.RFC3339Nano))
	q.Set("step", "60")
	q.Set("time", evaluation.UTC().Format(time.RFC3339Nano))
	for _, endpoint := range []string{"query", "query_range"} {
		for _, base := range []string{lokiURL, proxyURL} {
			deadline := time.Now().Add(90 * time.Second)
			for {
				status, body := hardeningRequest(t, "GET", base+"/loki/api/v1/"+endpoint+"?"+q.Encode(), "", nil)
				if status == 500 && strings.Contains(string(body), "many-to-one matching must be explicit") {
					break
				}
				if base != lokiURL || time.Now().After(deadline) || status != 200 {
					t.Fatalf("%s %s cardinality: %d %s", base, endpoint, status, body)
				}
				time.Sleep(250 * time.Millisecond)
			}
		}
	}
	invalid := q.Get("query")
	for _, endpoint := range []string{"query", "query_range"} {
		for _, base := range []string{lokiURL, proxyURL} {
			q.Set("query", `sum by(service_name)(count_over_time(`+selector+`[5m])) / on() sum(count_over_time(`+selector+`[5m]))`)
			status, body := hardeningRequest(t, "GET", base+"/loki/api/v1/"+endpoint+"?"+q.Encode(), "", nil)
			var response struct {
				Data struct {
					Result []struct {
						Metric map[string]string `json:"metric"`
						Value  []interface{}     `json:"value"`
						Values [][]interface{}   `json:"values"`
					} `json:"result"`
				} `json:"data"`
			}
			if status != 200 || json.Unmarshal(body, &response) != nil || len(response.Data.Result) != 1 {
				t.Fatalf("%s %s empty on(): %d %s", base, endpoint, status, body)
			}
			series := response.Data.Result[0]
			if len(series.Metric) != 0 {
				t.Fatalf("empty on() output labels: %s", body)
			}
			points := series.Values
			if endpoint == "query" {
				points = [][]interface{}{series.Value}
			}
			wantPoints := 2
			if endpoint == "query" {
				wantPoints = 1
			}
			if len(points) != wantPoints {
				t.Fatalf("empty on() sample count: %s", body)
			}
			for index, point := range points {
				if len(point) != 2 || point[0] != float64(evaluation.Add(time.Duration(index)*time.Minute).Unix()) || point[1] != "1" {
					t.Fatalf("empty on() value: %s", body)
				}
			}
			q.Set("query", `(`+invalid+`) + on(service_name) sum by(service_name)(count_over_time(`+selector+`[5m]))`)
			status, body = hardeningRequest(t, "GET", base+"/loki/api/v1/"+endpoint+"?"+q.Encode(), "", nil)
			if status != 500 || !strings.Contains(string(body), "many-to-one matching must be explicit") {
				t.Fatalf("%s nested cardinality: %d %s", base, status, body)
			}
		}
	}
}
