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

// validParityCases resolves the performance catalog from the shared quality
// matrix. A performance case with case_id uses the exact quality query.
func validParityCases(t testing.TB) []parityCase {
	t.Helper()
	matrix := loadQuerySemanticsMatrix(t)
	quality := make(map[string]string, len(matrix.Cases))
	for _, tc := range matrix.Cases {
		quality[tc.ID] = tc.Query
	}
	cases := make([]parityCase, 0, len(matrix.PerformanceCases))
	for _, tc := range matrix.PerformanceCases {
		query := tc.Query
		if tc.CaseID != "" {
			query = quality[tc.CaseID]
		}
		cases = append(cases, parityCase{name: tc.ID, query: query, category: tc.Category})
	}
	return cases
}

func performanceQuery(t testing.TB, id string) string {
	t.Helper()
	for _, tc := range validParityCases(t) {
		if tc.name == id {
			return tc.query
		}
	}
	t.Fatalf("unknown performance case %q", id)
	return ""
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
	cases := validParityCases(t)

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

	results := make([]parityLatencyResult, 0, len(cases))

	for _, tc := range cases {
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
	p.Set("query", performanceQuery(b, "exact_match"))
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
	p.Set("query", performanceQuery(b, "sum_by"))
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
	p.Set("query", performanceQuery(b, "avg_over_time_unwrap"))
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
	p.Set("query", performanceQuery(b, "vector_div"))
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
	p.Set("query", performanceQuery(b, "json_two_field_filters"))
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
