//go:build e2e

// Package e2e_pipeline tests the full LogQL→proxy→VictoriaLogs pipeline under
// realistic data volume (~10k log lines). Unlike the e2e-compat suite (which
// compares Loki vs proxy responses for format parity), this suite verifies that
// the proxy correctly routes, translates, and returns non-empty results for the
// query patterns that Grafana Logs Drilldown fires in production.
//
// Prerequisites:
//
//	cd test/e2e-pipeline && docker compose up -d
//	../../scripts/ci/wait_pipeline_stack.sh 120
//
// Run:
//
//	go test -v -tags=e2e -timeout=180s ./test/e2e-pipeline/
package e2e_pipeline

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

var (
	proxyURL = envOr("PROXY_URL", "http://localhost:13200")
	vlURL    = envOr("VL_URL", "http://localhost:19528")
)

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// TestMain ingests data once for the whole suite.
func TestMain(m *testing.M) {
	// Wait for proxy readiness (up to 60s) before ingesting.
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := http.Get(proxyURL + "/ready")
		if err == nil && resp.StatusCode == 200 {
			resp.Body.Close()
			break
		}
		if resp != nil {
			resp.Body.Close()
		}
		time.Sleep(2 * time.Second)
	}
	os.Exit(m.Run())
}

// TestPipelineSetup ingests ~10k lines and is depended on by all query tests.
func TestPipelineSetup(t *testing.T) {
	ingestPipelineData(t)
}

// --- Query battery ---

// TestQueryRange_CountOverTime verifies the stats fast path used by Drilldown
// volume graphs: count_over_time routed to stats_query_range.
func TestQueryRange_CountOverTime(t *testing.T) {
	result := queryRange(t, `count_over_time({namespace="prod"}[5m])`, "matrix")
	requireNonEmpty(t, "count_over_time", result)
}

// TestQueryRange_Rate verifies rate() translation and aggregation pipeline.
func TestQueryRange_Rate(t *testing.T) {
	result := queryRange(t, `rate({namespace="prod"}[1m])`, "matrix")
	requireNonEmpty(t, "rate", result)
}

// TestQueryRange_SumByService verifies cross-service aggregation with by() grouping.
func TestQueryRange_SumByService(t *testing.T) {
	result := queryRange(t, `sum by (service_name) (count_over_time({namespace="prod"}[5m]))`, "matrix")
	requireNonEmpty(t, "sum_by_service", result)
	// Must have one series per service (api-gw, auth, worker, cache, db).
	if len(result) < 5 {
		t.Errorf("sum_by_service: expected ≥5 series (one per service), got %d", len(result))
	}
}

// TestQueryRange_LogfmtUnwrap verifies the unwrap path for logfmt-parsed fields,
// routing through the manual range metric compat layer.
func TestQueryRange_LogfmtUnwrap(t *testing.T) {
	// worker and cache services produce logfmt logs with duration_ms field.
	result := queryRange(t, `avg_over_time({service_name=~"worker|cache"}|logfmt|unwrap duration_ms[5m])`, "matrix")
	requireNonEmpty(t, "logfmt_unwrap", result)
}

// TestQueryRange_JSONUnwrap verifies the unwrap path for json-parsed fields.
func TestQueryRange_JSONUnwrap(t *testing.T) {
	// api-gw, auth, db services produce JSON logs with duration_ms field.
	result := queryRange(t, `max_over_time({service_name="api-gw"}|json|unwrap duration_ms[5m])`, "matrix")
	requireNonEmpty(t, "json_unwrap", result)
}

// TestSeries verifies the /series endpoint returns stream descriptors.
func TestSeries(t *testing.T) {
	now := time.Now()
	start := now.Add(-35 * time.Minute)

	params := url.Values{
		"match[]": []string{`{namespace="prod"}`},
		"start":   []string{fmt.Sprintf("%d", start.UnixNano())},
		"end":     []string{fmt.Sprintf("%d", now.UnixNano())},
	}

	body := getEndpoint(t, proxyURL+"/loki/api/v1/series?"+params.Encode())

	var resp struct {
		Status string              `json:"status"`
		Data   []map[string]string `json:"data"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatalf("series: unmarshal: %v", err)
	}
	if resp.Status != "success" {
		t.Fatalf("series: status=%q, want success", resp.Status)
	}
	if len(resp.Data) == 0 {
		t.Error("series: got 0 streams, want ≥1")
	}
	t.Logf("series: %d streams returned", len(resp.Data))
}

// TestDetectedFields verifies the Drilldown detected_fields endpoint returns
// the fields embedded in log lines (level, status, duration_ms, etc.).
func TestDetectedFields(t *testing.T) {
	now := time.Now()
	start := now.Add(-35 * time.Minute)

	params := url.Values{
		"query": []string{`{service_name="api-gw"}`},
		"start": []string{fmt.Sprintf("%d", start.UnixNano())},
		"end":   []string{fmt.Sprintf("%d", now.UnixNano())},
	}

	body := getEndpoint(t, proxyURL+"/loki/api/v1/detected_fields?"+params.Encode())

	var resp struct {
		Status string `json:"status"`
		Fields []struct {
			Label string `json:"label"`
		} `json:"fields"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatalf("detected_fields: unmarshal: %v", err)
	}
	if resp.Status != "success" {
		t.Fatalf("detected_fields: status=%q, want success", resp.Status)
	}
	if len(resp.Fields) == 0 {
		t.Error("detected_fields: got 0 fields, want ≥1")
	}

	labels := make(map[string]bool)
	for _, f := range resp.Fields {
		labels[f.Label] = true
	}
	t.Logf("detected_fields: %v", keys(labels))

	// Fields embedded in JSON lines must appear.
	for _, want := range []string{"level", "status", "duration_ms"} {
		if !labels[want] {
			t.Errorf("detected_fields: missing expected field %q", want)
		}
	}
}

// TestPatterns verifies the patterns endpoint returns cluster patterns.
func TestPatterns(t *testing.T) {
	now := time.Now()
	start := now.Add(-35 * time.Minute)

	params := url.Values{
		"query": []string{`{namespace="prod"}`},
		"start": []string{fmt.Sprintf("%d", start.UnixNano())},
		"end":   []string{fmt.Sprintf("%d", now.UnixNano())},
	}

	body := getEndpoint(t, proxyURL+"/loki/api/v1/patterns?"+params.Encode())

	var resp struct {
		Status string        `json:"status"`
		Data   []interface{} `json:"data"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatalf("patterns: unmarshal: %v", err)
	}
	if resp.Status != "success" {
		t.Fatalf("patterns: status=%q, want success", resp.Status)
	}
	t.Logf("patterns: %d patterns returned", len(resp.Data))
	// Patterns may be empty for small datasets — log but don't fail.
	if len(resp.Data) == 0 {
		t.Log("patterns: 0 patterns (acceptable for dataset size)")
	}
}

// TestConcurrentQueryRange fires 20 concurrent query_range requests to catch
// races and verify the coalescer handles concurrent identical keys correctly.
func TestConcurrentQueryRange(t *testing.T) {
	const concurrency = 20
	query := `count_over_time({namespace="prod"}[5m])`

	var wg sync.WaitGroup
	errs := make([]error, concurrency)
	results := make([][]interface{}, concurrency)

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					errs[idx] = fmt.Errorf("panic: %v", r)
				}
			}()
			results[idx] = queryRange(t, query, "matrix")
		}(i)
	}
	wg.Wait()

	var failed int
	for i, err := range errs {
		if err != nil {
			t.Errorf("goroutine %d: %v", i, err)
			failed++
		}
	}
	if failed > 0 {
		t.Fatalf("%d/%d goroutines failed", failed, concurrency)
	}

	// All goroutines must get non-empty results.
	for i, res := range results {
		if len(res) == 0 {
			t.Errorf("goroutine %d: empty result from concurrent query", i)
		}
	}
	t.Logf("concurrent query_range: %d/20 goroutines returned results", concurrency-failed)
}

// --- Helpers ---

func queryRange(t *testing.T, query, wantResultType string) []interface{} {
	t.Helper()

	now := time.Now()
	start := now.Add(-35 * time.Minute)
	step := "5m"

	params := url.Values{
		"query": []string{query},
		"start": []string{fmt.Sprintf("%d", start.UnixNano())},
		"end":   []string{fmt.Sprintf("%d", now.UnixNano())},
		"step":  []string{step},
	}

	body := getEndpoint(t, proxyURL+"/loki/api/v1/query_range?"+params.Encode())

	var resp struct {
		Status string `json:"status"`
		Data   struct {
			ResultType string        `json:"resultType"`
			Result     []interface{} `json:"result"`
		} `json:"data"`
		Error string `json:"error"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatalf("query_range %q: unmarshal: %v\nbody: %s", query, err, body)
	}
	if resp.Error != "" {
		t.Fatalf("query_range %q: error=%q", query, resp.Error)
	}
	if resp.Status != "success" {
		t.Fatalf("query_range %q: status=%q, want success", query, resp.Status)
	}
	if resp.Data.ResultType != wantResultType {
		t.Errorf("query_range %q: resultType=%q, want %q", query, resp.Data.ResultType, wantResultType)
	}
	return resp.Data.Result
}

func requireNonEmpty(t *testing.T, name string, result []interface{}) {
	t.Helper()
	if len(result) == 0 {
		t.Errorf("%s: got 0 result series, want ≥1 (data may not have been ingested correctly)", name)
	} else {
		t.Logf("%s: %d series returned", name, len(result))
	}
}

func getEndpoint(t *testing.T, rawURL string) []byte {
	t.Helper()
	resp, err := http.Get(rawURL) //nolint:noctx
	if err != nil {
		t.Fatalf("GET %s: %v", rawURL, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body from %s: %v", rawURL, err)
	}
	if resp.StatusCode != 200 {
		t.Fatalf("GET %s: status %d\nbody: %s", rawURL, resp.StatusCode, body)
	}
	return body
}

func keys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

// Compile-time assertion: strings package used.
var _ = strings.TrimSpace
