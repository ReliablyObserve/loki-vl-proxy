//go:build e2e

package e2e_compat

import (
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"
)

type parserErrorMetricResponse struct {
	Status string `json:"status"`
	Data   struct {
		ResultType string `json:"resultType"`
		Result     []struct {
			Metric map[string]string   `json:"metric"`
			Value  []json.RawMessage   `json:"value"`
			Values [][]json.RawMessage `json:"values"`
		} `json:"result"`
	} `json:"data"`
}

// A pipeline error is runtime state, not a property of the query text. These
// canaries distinguish filtering and dropping that state in pipeline order,
// including Loki's parser hints for aggregations that discard or retain errors.
func TestHardeningLive_ParserErrorPipelineParity(t *testing.T) {
	service := fmt.Sprintf("hardening-parser-errors-%d", time.Now().UnixNano())
	evaluation := time.Now().Add(-4 * time.Minute).Truncate(time.Minute)
	stamp := evaluation.Add(-30 * time.Second)
	for kind, line := range map[string]string{"valid": `{"message":"valid"}`, "invalid": "plain parser-error-canary"} {
		labels := map[string]string{"service_name": service, "kind": kind}
		row, _ := json.Marshal(map[string]string{"service_name": service, "kind": kind, "_time": stamp.UTC().Format(time.RFC3339Nano), "_msg": line})
		status, body := hardeningRequest(t, http.MethodPost, vlURL+"/insert/jsonline?_stream_fields=service_name,kind", string(row)+"\n", map[string]string{"Content-Type": "application/stream+json"})
		if status != http.StatusOK {
			t.Fatalf("VL ingest: %d %s", status, body)
		}
		payload, _ := json.Marshal(map[string]any{"streams": []any{map[string]any{"stream": labels, "values": [][]string{{strconv.FormatInt(stamp.UnixNano(), 10), line}}}}})
		status, body = hardeningRequest(t, http.MethodPost, lokiURL+"/loki/api/v1/push", string(payload), map[string]string{"Content-Type": "application/json"})
		if status != http.StatusNoContent {
			t.Fatalf("Loki ingest: %d %s", status, body)
		}
	}
	status, body := hardeningRequest(t, http.MethodPost, vlURL+"/internal/force_flush", "", nil)
	if status != http.StatusOK {
		t.Fatalf("VL flush: %d %s", status, body)
	}
	selector := `{service_name="` + service + `"}`
	// A fresh Loki ingester may expose logs before range metrics. Gate on the
	// exact two canary rows instead of a fixed sleep or unrelated shared fixtures.
	waitForParserErrorCanaryMetrics(t, selector, evaluation)
	rate := func(pipeline string) string { return "rate(" + selector + pipeline + " [5m])" }
	one := 1.0 / 300
	cases := []struct {
		name    string
		query   string
		status  int
		key     string
		want    map[string]float64
		grouped bool
	}{
		{name: "bare_parser", query: rate(" | json"), status: 400},
		{name: "select_errors", query: rate(` | json | __error__!=""`), status: 400},
		{name: "filter_errors", query: rate(` | json | __error__=""`), status: 200, key: "kind", want: map[string]float64{"valid": one}},
		{name: "drop_errors", query: rate(" | json | drop __error__"), status: 200, key: "kind", want: map[string]float64{"valid": one, "invalid": one}},
		{name: "select_then_drop", query: rate(` | json | __error__!="" | drop __error__`), status: 200, key: "kind", want: map[string]float64{"invalid": one}},
		{name: "drop_then_select", query: rate(` | json | drop __error__ | __error__!=""`), status: 200},
		{name: "drop_details_only", query: rate(" | json | drop __error_details__"), status: 400},
		{name: "empty_selector", query: `rate({service_name="` + service + `-missing"} | json | __error__!="" [5m])`, status: 200},
		{name: "group_by_error", query: "sum by(__error__)(" + rate(" | json") + ")", status: 200, key: "__error__", want: map[string]float64{"": one, "JSONParserErr": one}, grouped: true},
		{name: "group_keeps_error_marker", query: "sum by(__error__)(" + rate(" | json | keep __error__") + ")", status: 200, key: "__error__", want: map[string]float64{"": one, "JSONParserErr": one}, grouped: true},
		{name: "drop_preserve_marker", query: "sum by(__error__)(" + rate(" | json | drop __preserve_error__") + ")", status: 400},
		{name: "group_preserve_marker", query: "sum by(__preserve_error__)(" + rate(` | json | __error__!=""`) + ")", status: 200, key: "__preserve_error__", want: map[string]float64{"true": one}, grouped: true},
		{name: "discard_all_labels", query: "sum(" + rate(" | json") + ")", status: 200, want: map[string]float64{"": 2 * one}, grouped: true},
		{name: "group_original_label_drop_errors", query: "sum by(service_name)(" + rate(" | json | drop __error__") + ")", status: 200, key: "service_name", want: map[string]float64{service: 2 * one}, grouped: true},
		{name: "group_by_kind", query: "sum by(kind)(" + rate(" | json") + ")", status: 400},
		{name: "keep_kind_preserves_errors", query: rate(" | json | keep kind"), status: 400},
	}
	for _, mode := range []string{"query", "query_range"} {
		for _, tc := range cases {
			for _, backend := range []struct{ name, base string }{{"loki", lokiURL}, {"proxy", proxyURL}} {
				t.Run(mode+"/"+tc.name+"/"+backend.name, func(t *testing.T) {
					params := parserErrorQueryParams(tc.query, evaluation, mode)
					status, body := hardeningRequest(t, http.MethodGet, backend.base+"/loki/api/v1/"+mode+"?"+params.Encode(), "", nil)
					t.Logf("params=%s status=%d body=%s", params.Encode(), status, body)
					if status != tc.status {
						t.Fatalf("status=%d want=%d: %s", status, tc.status, body)
					}
					if status == http.StatusBadRequest {
						if !strings.Contains(string(body), "JSONParserErr") {
							t.Fatalf("missing pipeline error category: %s", body)
						}
						return
					}
					var response parserErrorMetricResponse
					if err := json.Unmarshal(body, &response); err != nil {
						t.Fatal(err)
					}
					wantType := "vector"
					if mode == "query_range" {
						wantType = "matrix"
					}
					if response.Status != "success" || response.Data.ResultType != wantType || len(response.Data.Result) != len(tc.want) {
						t.Fatalf("want successful %s with %d series: %s", wantType, len(tc.want), body)
					}
					seen := make(map[string]bool)
					for _, series := range response.Data.Result {
						key := series.Metric[tc.key]
						want, exists := tc.want[key]
						if !exists || seen[key] {
							t.Fatalf("unexpected or duplicate series %q: %s", key, body)
						}
						seen[key] = true
						if tc.grouped {
							for label := range series.Metric {
								if label != tc.key {
									t.Fatalf("ungrouped label %q leaked into aggregation: %s", label, body)
								}
							}
						}
						points := series.Values
						wantPoints := 2
						if mode == "query" {
							points, wantPoints = [][]json.RawMessage{series.Value}, 1
						}
						if len(points) != wantPoints {
							t.Fatalf("samples=%d want=%d: %s", len(points), wantPoints, body)
						}
						for i, point := range points {
							assertParserErrorMetricPoint(t, point, evaluation.Add(time.Duration(i)*time.Minute).Unix(), want)
						}
					}
				})
			}
		}
	}
}

func parserErrorQueryParams(query string, evaluation time.Time, mode string) url.Values {
	params := url.Values{"query": {query}}
	if mode == "query" {
		params.Set("time", evaluation.UTC().Format(time.RFC3339Nano))
	} else {
		params.Set("start", evaluation.UTC().Format(time.RFC3339Nano))
		params.Set("end", evaluation.Add(time.Minute).UTC().Format(time.RFC3339Nano))
		params.Set("step", "60")
	}
	return params
}

func assertParserErrorMetricPoint(t *testing.T, point []json.RawMessage, timestamp int64, want float64) {
	t.Helper()
	if len(point) != 2 {
		t.Fatalf("invalid sample: %s", point)
	}
	var gotTimestamp int64
	var raw string
	if json.Unmarshal(point[0], &gotTimestamp) != nil || json.Unmarshal(point[1], &raw) != nil {
		t.Fatalf("invalid sample tuple: %s", point)
	}
	got, err := strconv.ParseFloat(raw, 64)
	if err != nil || math.IsNaN(got) || math.Abs(got-want) > 1e-12 || gotTimestamp != timestamp {
		t.Fatalf("sample=%s want=[%d,%g]", point, timestamp, want)
	}
}

func waitForParserErrorCanaryMetrics(t *testing.T, selector string, evaluation time.Time) {
	t.Helper()
	params := parserErrorQueryParams("sum(count_over_time("+selector+"[5m]))", evaluation, "query_range")
	timer := time.NewTimer(3 * time.Minute)
	defer timer.Stop()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		status, body := hardeningRequest(t, http.MethodGet, lokiURL+"/loki/api/v1/query_range?"+params.Encode(), "", nil)
		var response parserErrorMetricResponse
		if status == http.StatusOK && json.Unmarshal(body, &response) == nil && len(response.Data.Result) == 1 && len(response.Data.Result[0].Values) == 2 {
			ready := true
			for _, point := range response.Data.Result[0].Values {
				if len(point) != 2 || string(point[1]) != `"2"` {
					ready = false
				}
			}
			if ready {
				return
			}
		}
		select {
		case <-t.Context().Done():
			t.Fatal(t.Context().Err())
		case <-timer.C:
			t.Fatalf("Loki never exposed both parser canary rows: %d %s", status, body)
		case <-ticker.C:
		}
	}
}

func TestHardeningLive_ParserErrorWindowBoundaries(t *testing.T) {
	service := fmt.Sprintf("hardening-parser-boundary-%d", time.Now().UnixNano())
	evaluation := time.Now().Add(-4 * time.Minute).Truncate(time.Minute)
	for _, fixture := range []struct {
		kind      string
		timestamp time.Time
		line      string
	}{
		{"valid-window", evaluation.Add(-5 * time.Minute), "invalid JSON at excluded lower boundary"},
		{"valid-window", evaluation, `{"message":"valid"}`},
		{"error-window", evaluation, "invalid JSON at included upper boundary"},
	} {
		labels := map[string]string{"service_name": service, "boundary": fixture.kind}
		row, _ := json.Marshal(map[string]string{"service_name": service, "boundary": fixture.kind, "_time": fixture.timestamp.UTC().Format(time.RFC3339Nano), "_msg": fixture.line})
		status, body := hardeningRequest(t, http.MethodPost, vlURL+"/insert/jsonline?_stream_fields=service_name,boundary", string(row)+"\n", map[string]string{"Content-Type": "application/stream+json"})
		if status != http.StatusOK {
			t.Fatalf("VL ingest: %d %s", status, body)
		}
		payload, _ := json.Marshal(map[string]any{"streams": []any{map[string]any{"stream": labels, "values": [][]string{{strconv.FormatInt(fixture.timestamp.UnixNano(), 10), fixture.line}}}}})
		status, body = hardeningRequest(t, http.MethodPost, lokiURL+"/loki/api/v1/push", string(payload), map[string]string{"Content-Type": "application/json"})
		if status != http.StatusNoContent {
			t.Fatalf("Loki ingest: %d %s", status, body)
		}
	}
	status, body := hardeningRequest(t, http.MethodPost, vlURL+"/internal/force_flush", "", nil)
	if status != http.StatusOK {
		t.Fatalf("VL flush: %d %s", status, body)
	}
	// The two upper-boundary rows must be visible; the lower-boundary row
	// must be excluded from count_over_time's five-minute trailing window.
	waitForParserErrorCanaryMetrics(t, `{service_name="`+service+`"}`, evaluation)
	for _, mode := range []string{"query", "query_range"} {
		for _, kind := range []string{"valid-window", "error-window"} {
			for _, backend := range []struct{ name, base string }{{"loki", lokiURL}, {"proxy", proxyURL}} {
				t.Run(mode+"/"+kind+"/"+backend.name, func(t *testing.T) {
					query := `rate({service_name="` + service + `",boundary="` + kind + `"}|json[5m])`
					params := parserErrorQueryParams(query, evaluation, mode)
					if mode == "query_range" {
						params.Set("end", params.Get("start"))
					}
					status, body := hardeningRequest(t, http.MethodGet, backend.base+"/loki/api/v1/"+mode+"?"+params.Encode(), "", nil)
					t.Logf("params=%s status=%d body=%s", params.Encode(), status, body)
					if kind == "error-window" {
						if status != 400 || !strings.Contains(string(body), "JSONParserErr") {
							t.Fatalf("missing upper-boundary pipeline error: %d %s", status, body)
						}
						return
					}
					var response parserErrorMetricResponse
					if status != 200 || json.Unmarshal(body, &response) != nil || len(response.Data.Result) != 1 {
						t.Fatalf("lost valid upper row or retained lower error: %d %s", status, body)
					}
					point := response.Data.Result[0].Value
					if mode == "query_range" {
						if len(response.Data.Result[0].Values) != 1 {
							t.Fatalf("unexpected sample count: %s", body)
						}
						point = response.Data.Result[0].Values[0]
					}
					assertParserErrorMetricPoint(t, point, evaluation.Unix(), 1.0/300)
				})
			}
		}
	}
}

func TestHardeningLive_ParserErrorStructuredMetadata(t *testing.T) {
	service := fmt.Sprintf("hardening-parser-metadata-%d", time.Now().UnixNano())
	evaluation := time.Now().Add(-4 * time.Minute).Truncate(time.Minute)
	line := `{"trace_id":"json-value","same":"shared","message":"valid"}`
	metadata := map[string]string{"trace_id": "metadata", "span_id": "span-value", "same": "shared"}
	for i := 0; i < 2; i++ {
		ts := evaluation.Add(time.Duration(-30+i) * time.Second)
		row, _ := json.Marshal(map[string]string{"service_name": service, "_time": ts.UTC().Format(time.RFC3339Nano), "_msg": line, "trace_id": "metadata", "span_id": "span-value", "same": "shared"})
		status, body := hardeningRequest(t, http.MethodPost, vlURL+"/insert/jsonline?_stream_fields=service_name", string(row)+"\n", map[string]string{"Content-Type": "application/stream+json"})
		if status != 200 {
			t.Fatalf("VL ingest: %d %s", status, body)
		}
		payload, _ := json.Marshal(map[string]any{"streams": []any{map[string]any{"stream": map[string]string{"service_name": service}, "values": [][]any{{strconv.FormatInt(ts.UnixNano(), 10), line, metadata}}}}})
		status, body = hardeningRequest(t, http.MethodPost, lokiURL+"/loki/api/v1/push", string(payload), map[string]string{"Content-Type": "application/json"})
		if status != 204 {
			t.Fatalf("Loki ingest: %d %s", status, body)
		}
	}
	status, body := hardeningRequest(t, http.MethodPost, vlURL+"/internal/force_flush", "", nil)
	if status != 200 {
		t.Fatalf("VL flush: %d %s", status, body)
	}
	selector := `{service_name="` + service + `"}`
	waitForParserErrorCanaryMetrics(t, selector, evaluation)
	for _, mode := range []string{"query", "query_range"} {
		for name, pipeline := range map[string]string{
			"unfiltered":                `|json`,
			"metadata_filter":           `|json|span_id="span-value"`,
			"different_value_collision": `|json|trace_id="metadata"|trace_id_extracted="json-value"`,
			"same_value_collision":      `|json|same="shared"|same_extracted="shared"`,
		} {
			for _, backend := range []struct{ name, base string }{{"loki", lokiURL}, {"proxy", proxyURL}} {
				t.Run(mode+"/"+name+"/"+backend.name, func(t *testing.T) {
					params := parserErrorQueryParams("rate("+selector+pipeline+"[5m])", evaluation, mode)
					status, body := hardeningRequest(t, http.MethodGet, backend.base+"/loki/api/v1/"+mode+"?"+params.Encode(), "", nil)
					t.Logf("params=%s status=%d body=%s", params.Encode(), status, body)
					var response parserErrorMetricResponse
					if status != 200 || json.Unmarshal(body, &response) != nil || len(response.Data.Result) != 1 {
						t.Fatalf("missing metadata-selected result: %d %s", status, body)
					}
					series := response.Data.Result[0]
					for label, want := range map[string]string{"service_name": service, "trace_id": "metadata", "trace_id_extracted": "json-value", "span_id": "span-value", "same": "shared", "same_extracted": "shared", "message": "valid"} {
						if series.Metric[label] != want {
							t.Fatalf("label %s=%q want=%q: %s", label, series.Metric[label], want, body)
						}
					}
					points := series.Values
					wantPoints := 2
					if mode == "query" {
						points = [][]json.RawMessage{series.Value}
						wantPoints = 1
					}
					if len(points) != wantPoints {
						t.Fatalf("samples=%d want=%d: %s", len(points), wantPoints, body)
					}
					for i, point := range points {
						assertParserErrorMetricPoint(t, point, evaluation.Add(time.Duration(i)*time.Minute).Unix(), 2.0/300)
					}
				})
			}
		}
	}
}

func TestHardeningLive_ParserErrorExtractionHints(t *testing.T) {
	service := fmt.Sprintf("hardening-parser-hints-%d", time.Now().UnixNano())
	evaluation := time.Now().Add(-4 * time.Minute).Truncate(time.Minute)
	line := `{"value":"bad","meta":"parsed","unicode":"a\ufffdb","invalid":"bad\q"," ":{"child":"nested"},"parent":{" ":"nested"}}`
	for i := 0; i < 2; i++ {
		stamp := evaluation.Add(time.Duration(-30+i) * time.Second)
		row, _ := json.Marshal(map[string]string{"service_name": service, "_time": stamp.UTC().Format(time.RFC3339Nano), "_msg": line, "meta": "stored", "detected_level": "unknown"})
		status, body := hardeningRequest(t, http.MethodPost, vlURL+"/insert/jsonline?_stream_fields=service_name", string(row)+"\n", map[string]string{"Content-Type": "application/stream+json"})
		if status != 200 {
			t.Fatalf("VL ingest: %d %s", status, body)
		}
		payload, _ := json.Marshal(map[string]any{"streams": []any{map[string]any{"stream": map[string]string{"service_name": service}, "values": [][]any{{strconv.FormatInt(stamp.UnixNano(), 10), line, map[string]string{"meta": "stored", "detected_level": "unknown"}}}}}})
		status, body = hardeningRequest(t, http.MethodPost, lokiURL+"/loki/api/v1/push", string(payload), map[string]string{"Content-Type": "application/json"})
		if status != 204 {
			t.Fatalf("Loki ingest: %d %s", status, body)
		}
	}
	status, body := hardeningRequest(t, http.MethodPost, vlURL+"/internal/force_flush", "", nil)
	if status != 200 {
		t.Fatalf("VL flush: %d %s", status, body)
	}
	selector := `{service_name="` + service + `"}`
	waitForParserErrorCanaryMetrics(t, selector, evaluation)
	for _, function := range []string{"count_over_time", "rate"} {
		inner := function + "(" + selector + `|json|drop value|value=""[5m])`
		for _, tc := range []struct {
			name, query string
			empty       bool
			labels      map[string]string
		}{
			{name: "bare_early_filter", query: inner, empty: true},
			{name: "without_early_filter", query: "sum without(service_name)(" + inner + ")", empty: true},
			{name: "singleton_ordered_filter", query: "sum(" + inner + ")"},
			{name: "empty_by_ordered_filter", query: "sum by()(" + inner + ")"},
			{name: "grouped_ordered_filter", query: "sum by(service_name)(" + inner + ")", labels: map[string]string{"service_name": service}},
			{name: "first_filter_hint", query: function + "(" + selector + `|json|value="bad"|drop value|value=""[5m])`, labels: map[string]string{"service_name": service, "meta": "stored", "meta_extracted": "parsed", "unicode": "a b", "detected_level": "unknown"}},
			{name: "dropped_metadata_collision", query: function + "(" + selector + `|drop meta|json[5m])`, labels: map[string]string{"service_name": service, "value": "bad", "meta": "parsed", "unicode": "a b", "detected_level": "unknown"}},
			{name: "unicode_replacement", query: function + "(" + selector + `|json[5m])`, labels: map[string]string{"service_name": service, "value": "bad", "meta": "stored", "meta_extracted": "parsed", "unicode": "a b", "detected_level": "unknown"}},
		} {
			// These fields are retained by the ungrouped cases. Loki accepts the
			// invalid escape as an empty scalar and skips empty JSON path parts.
			if _, ungrouped := tc.labels["unicode"]; ungrouped {
				tc.labels["child"], tc.labels["parent"] = "nested", "nested"
				if tc.name == "unicode_replacement" {
					tc.labels["invalid"] = ""
				}
			}
			for _, mode := range []string{"query", "query_range"} {
				for _, backend := range []struct{ name, base string }{{"loki", lokiURL}, {"proxy", proxyURL}} {
					t.Run(function+"/"+tc.name+"/"+mode+"/"+backend.name, func(t *testing.T) {
						params := parserErrorQueryParams(tc.query, evaluation, mode)
						status, body := hardeningRequest(t, http.MethodGet, backend.base+"/loki/api/v1/"+mode+"?"+params.Encode(), "", nil)
						t.Logf("params=%s status=%d body=%s", params.Encode(), status, body)
						var response parserErrorMetricResponse
						wantSeries := 1
						if tc.empty {
							wantSeries = 0
						}
						if status != 200 || json.Unmarshal(body, &response) != nil || response.Status != "success" || len(response.Data.Result) != wantSeries {
							t.Fatalf("expected %d successful series: %d %s", wantSeries, status, body)
						}
						if tc.empty {
							return
						}
						series := response.Data.Result[0]
						if len(series.Metric) != len(tc.labels) {
							t.Fatalf("unexpected labels: %s", body)
						}
						for name, want := range tc.labels {
							if series.Metric[name] != want {
								t.Fatalf("label %s=%q want=%q: %s", name, series.Metric[name], want, body)
							}
						}
						points, wantPoints := series.Values, 2
						if mode == "query" {
							points, wantPoints = [][]json.RawMessage{series.Value}, 1
						}
						if len(points) != wantPoints {
							t.Fatalf("unexpected sample count: %s", body)
						}
						want := 2.0
						if function == "rate" {
							want /= 300
						}
						for i, point := range points {
							assertParserErrorMetricPoint(t, point, evaluation.Add(time.Duration(i)*time.Minute).Unix(), want)
						}
					})
				}
			}
		}
	}
}
