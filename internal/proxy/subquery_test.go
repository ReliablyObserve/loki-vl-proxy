package proxy

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/szibis/Loki-VL-proxy/internal/translator"
)

// =============================================================================
// Subquery evaluation — proxy executes inner query at sub-steps
// =============================================================================

func TestSubquery_TranslatorReturnsPrefix(t *testing.T) {
	logql := `max_over_time(rate({app="nginx"}[5m])[1h:5m])`
	result, err := translator.TranslateLogQL(logql)
	if err != nil {
		t.Fatalf("subquery should not error: %v", err)
	}
	if !strings.HasPrefix(result, translator.SubqueryPrefix) {
		t.Fatalf("expected __subquery__ prefix, got %q", result)
	}
}

func TestSubquery_QueryRange_ProxyEvaluates(t *testing.T) {
	// Mock VL backend that returns stats_query results
	callCount := 0
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount++
		w.Header().Set("Content-Type", "application/json")
		// Return a simple stats result with one data point
		fmt.Fprintf(w, `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"app":"nginx"},"value":[1609459200,"%d"]}]}}`, callCount)
	}))
	defer vlBackend.Close()

	// Use a subquery: max_over_time(rate({app="nginx"}[5m])[30m:10m])
	// 30m range / 10m step = ~3 sub-queries
	resp := doGet(t, vlBackend.URL, `/loki/api/v1/query_range?query=max_over_time(rate({app="nginx"}[5m])[30m:10m])&start=1609459200&end=1609462800&step=60`)

	status, _ := resp["status"].(string)
	if status != "success" {
		t.Errorf("expected status=success, got %q (resp: %v)", status, resp)
	}
	data, _ := resp["data"].(map[string]interface{})
	if data == nil {
		t.Fatal("expected data field in response")
	}
	resultType, _ := data["resultType"].(string)
	if resultType != "matrix" {
		t.Errorf("expected resultType=matrix, got %q", resultType)
	}
	// The proxy should have called VL multiple times (at least 3 sub-steps)
	if callCount < 3 {
		t.Errorf("expected at least 3 VL calls for 30m/10m subquery, got %d", callCount)
	}
}

func TestSubquery_InstantQuery_ProxyEvaluates(t *testing.T) {
	callCount := 0
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount++
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"app":"nginx"},"value":[1609459200,"%d"]}]}}`, callCount)
	}))
	defer vlBackend.Close()

	resp := doGet(t, vlBackend.URL, `/loki/api/v1/query?query=max_over_time(rate({app="nginx"}[5m])[30m:10m])&time=1609462800`)

	status, _ := resp["status"].(string)
	if status != "success" {
		t.Errorf("expected status=success, got %q", status)
	}
	if callCount < 3 {
		t.Errorf("expected at least 3 VL calls, got %d", callCount)
	}
}

func TestSubquery_OuterFunctions(t *testing.T) {
	outerFuncs := []string{
		"max_over_time", "min_over_time", "avg_over_time",
		"sum_over_time", "stddev_over_time", "stdvar_over_time",
		"count_over_time",
	}
	for _, fn := range outerFuncs {
		t.Run(fn, func(t *testing.T) {
			logql := fmt.Sprintf(`%s(rate({app="nginx"}[5m])[1h:5m])`, fn)
			result, err := translator.TranslateLogQL(logql)
			if err != nil {
				t.Fatalf("subquery with %s should not error: %v", fn, err)
			}
			if !strings.HasPrefix(result, translator.SubqueryPrefix) {
				t.Errorf("expected __subquery__ prefix for %s, got %q", fn, result)
			}
		})
	}
}

func TestSubqueryAggregation(t *testing.T) {
	tests := []struct {
		name   string
		fn     string
		values []float64
		want   float64
	}{
		{"max of 3 values", "max_over_time", []float64{1, 5, 3}, 5},
		{"min of 3 values", "min_over_time", []float64{1, 5, 3}, 1},
		{"sum of 3 values", "sum_over_time", []float64{2, 4, 6}, 12},
		{"avg of 3 values", "avg_over_time", []float64{2, 4, 6}, 4},
		{"count of 3 values", "count_over_time", []float64{2, 4, 6}, 3},
		{"single value max", "max_over_time", []float64{42}, 42},
		{"empty values", "max_over_time", []float64{}, 0},
		{"first_over_time", "first_over_time", []float64{10, 20, 30}, 10},
		{"last_over_time", "last_over_time", []float64{10, 20, 30}, 30},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := subqueryAggregate(tt.fn, tt.values)
			if got != tt.want {
				t.Errorf("subqueryAggregate(%q, %v) = %v, want %v", tt.fn, tt.values, got, tt.want)
			}
		})
	}
}

func TestParseLokiDuration(t *testing.T) {
	tests := []struct {
		input string
		want  time.Duration
	}{
		{"5m", 5 * time.Minute},
		{"1h", time.Hour},
		{"30s", 30 * time.Second},
		{"1d", 24 * time.Hour},
		{"2h", 2 * time.Hour},
		{"10m", 10 * time.Minute},
		{"1h30m", 90 * time.Minute},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := parseLokiDuration(tt.input)
			if got != tt.want {
				t.Errorf("parseLokiDuration(%q) = %v, want %v", tt.input, got, tt.want)
			}
		})
	}
}

func TestParseTimestamp(t *testing.T) {
	tests := []struct {
		input  string
		wantOk bool
	}{
		{"1609459200", true},
		{"1609459200.123", true},
		{"2021-01-01T00:00:00Z", true},
		{"", true}, // defaults to now
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			_, err := parseTimestamp(tt.input)
			if tt.wantOk && err != nil {
				t.Errorf("parseTimestamp(%q) error: %v", tt.input, err)
			}
		})
	}
}
