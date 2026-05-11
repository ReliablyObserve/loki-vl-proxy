//go:build e2e

// Latency benchmarks for the exhaustive LogQL parity machine.
// Measures proxy vs Loki response time for every valid query category and
// reports the cases with the largest proxy/Loki latency ratio.
//
// Run with:
//
//	go test -v -tags=e2e -run TestParityLatency ./test/e2e-compat/
//
// Go benchmarks (proxy only) for micro-regression detection:
//
//	go test -bench=BenchmarkParity -benchtime=5s -tags=e2e ./test/e2e-compat/
package e2e_compat

import (
	"fmt"
	"net/url"
	"sort"
	"testing"
	"time"
)

// parityCase is a single query entry for latency measurement.
type parityCase struct {
	name     string
	query    string
	category string
}

// validParityCases returns all valid LogQL cases from the exhaustive machine.
var validParityCases = []parityCase{
	// Selectors
	{"exact_match", `{app="api-gateway",env="production"}`, "selector"},
	{"regex_match", `{app=~"api-.*",env="production"}`, "selector"},
	{"not_equal", `{app!="payment-service",env="production"}`, "selector"},
	{"regex_not_match", `{app!~"nginx.*",env="production"}`, "selector"},

	// Line filters
	{"line_contains", `{app="api-gateway",env="production"} |= "GET"`, "line_filter"},
	{"line_not_contains", `{app="api-gateway",env="production"} != "health"`, "line_filter"},
	{"line_regex", `{app="api-gateway",env="production"} |~ "GET|POST"`, "line_filter"},
	{"line_not_regex", `{app="api-gateway",env="production"} !~ "health|ready"`, "line_filter"},

	// Parsers
	{"json_parser", `{app="api-gateway",env="production"} | json`, "parser"},
	{"logfmt_parser", `{app="payment-service",env="production"} | logfmt`, "parser"},
	{"regexp_parser", `{app="api-gateway",env="production"} | regexp "\"method\":\"(?P<http_method>[A-Z]+)\""`, "parser"},
	{"pattern_parser", `{app="api-gateway",env="production"} | json | line_format "{{.method}} {{.path}} {{.status}}" | pattern "<method> <path> <code>"`, "parser"},

	// Field filters
	{"json_field_eq", `{app="api-gateway",env="production"} | json | method="GET"`, "field_filter"},
	{"json_field_ne", `{app="api-gateway",env="production"} | json | method!="DELETE"`, "field_filter"},
	{"json_field_regex", `{app="api-gateway",env="production"} | json | path=~"/api/.*"`, "field_filter"},
	{"json_field_gt", `{app="api-gateway",env="production"} | json | status>=400`, "field_filter"},
	{"logfmt_field_eq", `{app="payment-service",env="production"} | logfmt | level="error"`, "field_filter"},

	// Format stages
	{"line_format", `{app="api-gateway",env="production"} | json | line_format "{{.method}} {{.path}}"`, "format"},
	{"label_format", `{app="api-gateway",env="production"} | json | label_format method_up="{{.method}}"`, "format"},
	{"label_format_multiple", `{app="api-gateway",env="production"} | json | label_format m="{{.method}}", s="{{.status}}"`, "format"},

	// Drop / Keep
	{"drop_single", `{app="api-gateway",env="production"} | json | drop trace_id`, "drop_keep"},
	{"keep_multiple", `{app="api-gateway",env="production"} | json | keep method, status`, "drop_keep"},
	{"drop_error", `{app="api-gateway",env="production"} | json | drop __error__, __error_details__`, "drop_keep"},

	// Multi-stage pipelines
	{"json_filter_format", `{app="api-gateway",env="production"} | json | method="GET" | line_format "{{.status}}"`, "multi_stage"},
	{"chained_line_filters", `{app="api-gateway",env="production"} |= "api" |~ "GET|POST" != "health"`, "multi_stage"},
	{"json_two_field_filters", `{app="api-gateway",env="production"} | json | method="GET" | path=~"/api/.*"`, "multi_stage"},
	{"line_filter_then_json", `{app="api-gateway",env="production"} |= "GET" | json`, "multi_stage"},

	// Metric range queries
	{"count_over_time", `count_over_time({app="api-gateway",env="production"}[5m])`, "metric_range"},
	{"rate", `rate({app="api-gateway",env="production"}[5m])`, "metric_range"},
	{"bytes_over_time", `bytes_over_time({app="api-gateway",env="production"}[5m])`, "metric_range"},
	{"rate_with_line_filter", `rate({app="api-gateway",env="production"} |= "GET"[5m])`, "metric_range"},
	{"rate_with_json_filter", `rate({app="api-gateway",env="production"} | json | method="GET"[5m])`, "metric_range"},
	{"count_with_logfmt", `count_over_time({app="payment-service",env="production"} | logfmt | level="error"[5m])`, "metric_range"},

	// Metric aggregations
	{"sum_by", `sum by (level) (count_over_time({app="api-gateway",env="production"}[5m]))`, "metric_agg"},
	{"sum_rate_by", `sum by (app) (rate({env="production"}[5m]))`, "metric_agg"},
	{"avg_by", `avg by (level) (count_over_time({app="api-gateway",env="production"}[5m]))`, "metric_agg"},
	{"topk", `topk(3, sum by (app) (count_over_time({env="production"}[5m])))`, "metric_agg"},
	{"bottomk", `bottomk(3, sum by (app) (count_over_time({env="production"}[5m])))`, "metric_agg"},
	{"sort_metric", `sort(sum by (app) (count_over_time({env="production"}[5m])))`, "metric_agg"},

	// Unwrap metrics
	{"sum_over_time_unwrap", `sum_over_time({app="api-gateway",env="production"} | json | unwrap duration_ms [5m])`, "unwrap"},
	{"avg_over_time_unwrap", `avg_over_time({app="api-gateway",env="production"} | json | unwrap duration_ms [5m])`, "unwrap"},
	{"max_over_time_unwrap", `max_over_time({app="api-gateway",env="production"} | json | unwrap duration_ms [5m])`, "unwrap"},
	{"quantile_over_time_unwrap", `quantile_over_time(0.99, {app="api-gateway",env="production"} | json | unwrap duration_ms [5m])`, "unwrap"},

	// Comparison and vector ops
	{"metric_gt_zero", `sum by (level) (count_over_time({app="api-gateway",env="production"}[5m])) > 0`, "metric_compare"},
	{"metric_bool_gt", `sum by (level) (count_over_time({app="api-gateway",env="production"}[5m])) > bool 0`, "metric_compare"},
	{"metric_scalar_mul", `sum by (level) (count_over_time({app="api-gateway",env="production"}[5m])) * 100`, "metric_compare"},
	{"vector_or", `sum by (level) (count_over_time({app="api-gateway",env="production"}[5m])) or sum by (level) (count_over_time({app="api-gateway",env="production",level="debug"}[5m]))`, "vector_op"},
	{"vector_unless", `sum by (level) (count_over_time({app="api-gateway",env="production"}[5m])) unless sum by (level) (count_over_time({app="api-gateway",env="production",level="debug"}[5m]))`, "vector_op"},
	{"vector_div", `sum by (level) (count_over_time({app="api-gateway",env="production"}[5m])) / sum by (level) (count_over_time({app="api-gateway",env="production"}[5m]))`, "vector_op"},

	// Absent
	{"absent_over_time", `absent_over_time({app="nonexistent_service_xyz"}[5m])`, "absent"},
}

type parityLatencyResult struct {
	name       string
	category   string
	proxyMs    float64
	lokiMs     float64
	ratioProxy float64 // proxy/loki; >1 = proxy slower
}

// TestParityLatency measures proxy vs Loki latency for every valid parity case.
// Prints a sorted table of the slowest proxy/Loki ratios to help identify regressions.
func TestParityLatency(t *testing.T) {
	ensureDataIngested(t)

	now := time.Now()
	baseParams := func(query string) url.Values {
		p := url.Values{}
		p.Set("query", query)
		p.Set("start", fmt.Sprintf("%d", now.Add(-2*time.Hour).UnixNano()))
		p.Set("end", fmt.Sprintf("%d", now.UnixNano()))
		p.Set("limit", "10")
		p.Set("step", "60")
		return p
	}

	results := make([]parityLatencyResult, 0, len(validParityCases))

	for _, tc := range validParityCases {
		p := baseParams(tc.query)

		proxyLatency, err := measureQueryLatency(proxyURL, p)
		if err != nil {
			t.Logf("SKIP %s: proxy error: %v", tc.name, err)
			continue
		}
		lokiLatency, err := measureQueryLatency(lokiURL, p)
		if err != nil {
			t.Logf("SKIP %s: loki error: %v", tc.name, err)
			continue
		}

		ratio := 0.0
		if lokiLatency > 0 {
			ratio = float64(proxyLatency) / float64(lokiLatency)
		}
		results = append(results, parityLatencyResult{
			name:       tc.name,
			category:   tc.category,
			proxyMs:    float64(proxyLatency.Milliseconds()),
			lokiMs:     float64(lokiLatency.Milliseconds()),
			ratioProxy: ratio,
		})
	}

	// Sort by proxy/loki ratio descending (slowest proxy overhead first).
	sort.Slice(results, func(i, j int) bool {
		return results[i].ratioProxy > results[j].ratioProxy
	})

	t.Logf("\n%-40s %-14s %8s %8s %8s", "Case", "Category", "Proxy(ms)", "Loki(ms)", "Ratio")
	t.Logf("%s", fmt.Sprintf("%-40s %-14s %8s %8s %8s", "----", "--------", "---------", "--------", "-----"))
	for _, r := range results {
		marker := ""
		if r.ratioProxy > 3.0 {
			marker = " ⚠ SLOW"
		} else if r.ratioProxy > 1.5 {
			marker = " △"
		}
		t.Logf("%-40s %-14s %8.1f %8.1f %8.2fx%s",
			r.name, r.category, r.proxyMs, r.lokiMs, r.ratioProxy, marker)
	}

	// Hard limit: no case should be more than 10x slower than Loki.
	for _, r := range results {
		if r.ratioProxy > 10.0 {
			t.Errorf("case %q (category=%s) is %.1fx slower than Loki (proxy=%.0fms loki=%.0fms)",
				r.name, r.category, r.ratioProxy, r.proxyMs, r.lokiMs)
		}
	}
}

// BenchmarkParity_Selectors benchmarks raw selector queries through the proxy.
func BenchmarkParity_Selectors(b *testing.B) {
	if proxyURL == "" {
		b.Skip("proxyURL not set")
	}
	now := time.Now()
	p := url.Values{}
	p.Set("query", `{app="api-gateway",env="production"}`)
	p.Set("start", fmt.Sprintf("%d", now.Add(-5*time.Minute).UnixNano()))
	p.Set("end", fmt.Sprintf("%d", now.UnixNano()))
	p.Set("limit", "10")
	p.Set("step", "60")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := measureQueryLatency(proxyURL, p); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkParity_MetricAgg benchmarks aggregated metric queries through the proxy.
func BenchmarkParity_MetricAgg(b *testing.B) {
	if proxyURL == "" {
		b.Skip("proxyURL not set")
	}
	now := time.Now()
	p := url.Values{}
	p.Set("query", `sum by (level) (count_over_time({app="api-gateway",env="production"}[5m]))`)
	p.Set("start", fmt.Sprintf("%d", now.Add(-10*time.Minute).UnixNano()))
	p.Set("end", fmt.Sprintf("%d", now.UnixNano()))
	p.Set("step", "60")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := measureQueryLatency(proxyURL, p); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkParity_UnwrapMetrics benchmarks unwrap-based metric queries through the proxy.
func BenchmarkParity_UnwrapMetrics(b *testing.B) {
	if proxyURL == "" {
		b.Skip("proxyURL not set")
	}
	now := time.Now()
	p := url.Values{}
	p.Set("query", `avg_over_time({app="api-gateway",env="production"} | json | unwrap duration_ms [5m])`)
	p.Set("start", fmt.Sprintf("%d", now.Add(-10*time.Minute).UnixNano()))
	p.Set("end", fmt.Sprintf("%d", now.UnixNano()))
	p.Set("step", "60")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := measureQueryLatency(proxyURL, p); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkParity_VectorOps benchmarks vector binary operations through the proxy.
func BenchmarkParity_VectorOps(b *testing.B) {
	if proxyURL == "" {
		b.Skip("proxyURL not set")
	}
	now := time.Now()
	p := url.Values{}
	p.Set("query", `sum by (level) (count_over_time({app="api-gateway",env="production"}[5m])) / sum by (level) (count_over_time({app="api-gateway",env="production"}[5m]))`)
	p.Set("start", fmt.Sprintf("%d", now.Add(-10*time.Minute).UnixNano()))
	p.Set("end", fmt.Sprintf("%d", now.UnixNano()))
	p.Set("step", "60")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := measureQueryLatency(proxyURL, p); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkParity_MultiStagePipeline benchmarks complex multi-stage pipelines.
func BenchmarkParity_MultiStagePipeline(b *testing.B) {
	if proxyURL == "" {
		b.Skip("proxyURL not set")
	}
	now := time.Now()
	p := url.Values{}
	p.Set("query", `{app="api-gateway",env="production"} | json | method="GET" | path=~"/api/.*"`)
	p.Set("start", fmt.Sprintf("%d", now.Add(-5*time.Minute).UnixNano()))
	p.Set("end", fmt.Sprintf("%d", now.UnixNano()))
	p.Set("limit", "10")
	p.Set("step", "60")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := measureQueryLatency(proxyURL, p); err != nil {
			b.Fatal(err)
		}
	}
}
