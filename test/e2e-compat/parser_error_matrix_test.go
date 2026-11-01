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

// TestChaining_ParserErrorNestedAggregations keeps the parser's runtime error
// contract under grouped and nested vector functions in the core CI shard.
// Loki's aggregation hints can skip parsing entirely for aggregate-all queries;
// therefore an unfiltered parser is not universally expected to fail.
// conformance: semantics/pipeline-error-text-parity, semantics/nested-aggregation-outer-by-native-routes, semantics/non-sum-vector-aggregation-unreduced, semantics/json-error-filter-metric-split-pushdown
func TestChaining_ParserErrorNestedAggregations(t *testing.T) {
	service := fmt.Sprintf("parser-matrix-%d", time.Now().UnixNano())
	evaluation := time.Now().Add(-4 * time.Minute).Truncate(time.Minute)
	for index, fixture := range []struct{ cluster, kind, line string }{
		{"east", "valid", `{"message":"east-one"}`},
		{"east", "valid", `{"message":"east-two"}`},
		{"west", "valid", `{"message":"west"}`},
		{"east", "truncated", `{"message":"truncated"`},
		{"west", "plain", "plain parser-matrix-canary"},
	} {
		stamp := evaluation.Add(-30*time.Second + time.Duration(index)*time.Millisecond)
		labels := map[string]string{"service_name": service, "cluster": fixture.cluster, "kind": fixture.kind}
		row := map[string]string{"service_name": service, "cluster": fixture.cluster, "kind": fixture.kind, "_time": stamp.UTC().Format(time.RFC3339Nano), "_msg": fixture.line}
		encoded, err := json.Marshal(row)
		if err != nil {
			t.Fatal(err)
		}
		status, body := hardeningRequest(t, http.MethodPost, vlURL+"/insert/jsonline?_stream_fields=service_name,cluster,kind", string(encoded)+"\n", map[string]string{"Content-Type": "application/stream+json"})
		if status != http.StatusOK {
			t.Fatalf("VL ingest: %d %s", status, body)
		}
		payload, err := json.Marshal(map[string]any{"streams": []any{map[string]any{"stream": labels, "values": [][]string{{strconv.FormatInt(stamp.UnixNano(), 10), fixture.line}}}}})
		if err != nil {
			t.Fatal(err)
		}
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
	// Reuse the exact readiness gate with the two malformed rows. This waits for
	// range metrics, not merely successful ingestion or log visibility.
	waitForParserErrorCanaryMetrics(t, `{service_name="`+service+`",kind!="valid"}`, evaluation)
	for _, function := range []string{"rate", "count_over_time"} {
		factor := 1.0
		if function == "rate" {
			factor /= 300
		}
		for _, filtered := range []bool{false, true} {
			pipeline := " | json"
			filterName := "unfiltered"
			if filtered {
				pipeline += ` | __error__=""`
				filterName = "filter_errors"
			}
			inner := function + "(" + selector + pipeline + " [5m])"
			for _, shape := range []struct {
				name, query, key string
				want             map[string]float64
				unfilteredOK     bool
			}{
				{"sum_by_cluster", "sum by(cluster)(" + inner + ")", "cluster", map[string]float64{"east": 2 * factor, "west": factor}, false},
				{"max_of_sum", "max(sum by(cluster)(" + inner + "))", "", map[string]float64{"": 2 * factor}, false},
				{"avg_of_sum", "avg(sum by(cluster)(" + inner + "))", "", map[string]float64{"": 1.5 * factor}, false},
				{"topk_of_sum", "topk(1,sum by(cluster)(" + inner + "))", "cluster", map[string]float64{"east": 2 * factor}, false},
				{"min_of_sum", "min(sum by(cluster)(" + inner + "))", "", map[string]float64{"": factor}, false},
				{"count_of_sum", "count(sum by(cluster)(" + inner + "))", "", map[string]float64{"": 2}, false},
				{"bottomk_of_sum", "bottomk(1,sum by(cluster)(" + inner + "))", "cluster", map[string]float64{"west": factor}, false},
				{"without_of_sum", "sum without(cluster)(sum by(cluster)(" + inner + "))", "", map[string]float64{"": 3 * factor}, false},
				{"three_levels", "max(avg by(cluster)(sum by(cluster,kind)(" + inner + ")))", "", map[string]float64{"": 2 * factor}, false},
				{"topk_zero", "topk(0,sum by(cluster)(" + inner + "))", "cluster", map[string]float64{}, false},
				{"topk_negative", "topk(-1,sum by(cluster)(" + inner + "))", "cluster", map[string]float64{}, false},
				{"bottomk_zero", "bottomk(0,sum by(cluster)(" + inner + "))", "cluster", map[string]float64{}, false},
				{"bottomk_negative", "bottomk(-1,sum by(cluster)(" + inner + "))", "cluster", map[string]float64{}, false},
				{"sum_all", "sum(" + inner + ")", "", map[string]float64{"": 3 * factor}, true},
				{"max_of_sum_all", "max(sum(" + inner + "))", "", map[string]float64{"": 3 * factor}, true},
			} {
				wantStatus := http.StatusOK
				if !filtered {
					if shape.unfilteredOK {
						shape.want = map[string]float64{"": 5 * factor}
					} else {
						wantStatus = http.StatusBadRequest
					}
				}
				wantError := "JSONParserErr"
				if strings.HasSuffix(shape.name, "_zero") || strings.HasSuffix(shape.name, "_negative") {
					wantStatus, wantError = http.StatusBadRequest, "parse error"
				}
				for _, mode := range []string{"query", "query_range"} {
					t.Run(function+"/"+filterName+"/"+shape.name+"/"+mode, func(t *testing.T) {
						for _, backend := range []struct{ name, base string }{{"loki", lokiURL}, {"proxy", proxyURL}} {
							t.Run(backend.name, func(t *testing.T) {
								params := parserErrorQueryParams(shape.query, evaluation, mode)
								status, body := hardeningRequest(t, http.MethodGet, backend.base+"/loki/api/v1/"+mode+"?"+params.Encode(), "", nil)
								recordParserMatrixResponse(t, params, status, body)
								assertParserMatrixResult(t, status, body, wantStatus, mode, shape.key, shape.want, evaluation, wantError)
							})
						}
					})
				}
			}
		}
		for _, kind := range []string{"valid", "truncated", "plain"} {
			query := "sum by(cluster)(" + function + `({service_name="` + service + `",kind="` + kind + `"}|json[5m]))`
			for _, mode := range []string{"query", "query_range"} {
				t.Run(function+"/input_"+kind+"/"+mode, func(t *testing.T) {
					wantStatus := http.StatusBadRequest
					if kind == "valid" {
						wantStatus = http.StatusOK
					}
					for _, backend := range []struct{ name, base string }{{"loki", lokiURL}, {"proxy", proxyURL}} {
						t.Run(backend.name, func(t *testing.T) {
							params := parserErrorQueryParams(query, evaluation, mode)
							status, body := hardeningRequest(t, http.MethodGet, backend.base+"/loki/api/v1/"+mode+"?"+params.Encode(), "", nil)
							recordParserMatrixResponse(t, params, status, body)
							assertParserMatrixResult(t, status, body, wantStatus, mode, "cluster", map[string]float64{"east": 2 * factor, "west": factor}, evaluation, "JSONParserErr")
						})
					}
				})
			}
		}
	}
}

// Keep each request axis and complete metric answer in the CI test log while
// omitting Loki's large, timing-dependent execution stats. Error bodies retain
// their category and text, including whether parsing or evaluation failed.
func recordParserMatrixResponse(t *testing.T, params url.Values, status int, body []byte) {
	t.Helper()
	var responseBody any = string(body)
	if status == http.StatusOK {
		var response parserErrorMetricResponse
		if json.Unmarshal(body, &response) == nil {
			responseBody = response
		}
	}
	record, err := json.Marshal(map[string]any{"params": params, "status": status, "response": responseBody})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("matrix_response %s", record)
}

func assertParserMatrixResult(t *testing.T, status int, body []byte, wantStatus int, mode, key string, want map[string]float64, evaluation time.Time, wantError string) {
	t.Helper()
	if status != wantStatus {
		t.Fatalf("status=%d want=%d: %s", status, wantStatus, body)
	}
	if status == http.StatusBadRequest {
		if !strings.Contains(string(body), wantError) {
			t.Fatalf("missing %s: %s", wantError, body)
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
	if response.Status != "success" || response.Data.ResultType != wantType || len(response.Data.Result) != len(want) {
		t.Fatalf("unexpected response: %s", body)
	}
	seen := map[string]bool{}
	for _, series := range response.Data.Result {
		label := series.Metric[key]
		value, ok := want[label]
		if !ok || seen[label] {
			t.Fatalf("unexpected or duplicate series: %s", body)
		}
		seen[label] = true
		for name := range series.Metric {
			if name != key {
				t.Fatalf("unexpected grouped label %s: %s", name, body)
			}
		}
		points := series.Values
		wantPoints := 2
		if mode == "query" {
			points = [][]json.RawMessage{series.Value}
			wantPoints = 1
		}
		if len(points) != wantPoints {
			t.Fatalf("unexpected points: %s", body)
		}
		for index, point := range points {
			assertParserErrorMetricPoint(t, point, evaluation.Add(time.Duration(index)*time.Minute).Unix(), value)
		}
	}
}
