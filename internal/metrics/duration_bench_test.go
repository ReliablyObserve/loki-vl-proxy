package metrics

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// The benchmarks below use only the exported record calls and the /metrics handler, so the same file compiles
// against an earlier build for a base-vs-change comparison.

// BenchmarkRecordRequestDurations is what one fast client request records: its request histogram, one backend call
// and one internal operation. A fast observation counts into every bucket, the worst case for the bucket count.
func BenchmarkRecordRequestDurations(b *testing.B) {
	for _, tc := range []struct {
		name string
		d    time.Duration
	}{{"fast", 500 * time.Microsecond}, {"slow", 25 * time.Second}} {
		b.Run(tc.name, func(b *testing.B) {
			m := NewMetricsWithOptions(256, 256, false)
			b.ReportAllocs()
			for b.Loop() {
				m.RecordRequestWithRoute("query_range", "", 200, tc.d)
				m.RecordBackendDurationWithRoute("select_logsql_query", "/select/logsql/query", tc.d)
				m.RecordInternalOperation("translate_query", "translated", tc.d)
			}
		})
	}
}

// BenchmarkMetricsRender renders /metrics for a replica that served the seeded routes, a few VictoriaLogs routes
// and, in the sensitive variant, 32 tenants and 32 clients.
func BenchmarkMetricsRender(b *testing.B) {
	for _, sensitive := range []bool{false, true} {
		name := "default"
		if sensitive {
			name = "sensitive-labels"
		}
		b.Run(name, func(b *testing.B) {
			m := populatedMetrics(sensitive)
			req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
			b.ReportAllocs()
			var size int
			for b.Loop() {
				rec := httptest.NewRecorder()
				m.Handler(rec, req)
				size = rec.Body.Len()
			}
			b.ReportMetric(float64(size), "body-bytes")
		})
	}
}

func populatedMetrics(sensitive bool) *Metrics {
	m := NewMetricsWithOptions(256, 256, sensitive)
	for _, route := range []string{"query", "hits", "stats_query", "stats_query_range", "field_names", "field_values", "streams", "stream_field_names", "stream_field_values", "facets"} {
		m.RecordBackendDurationWithRoute("select_logsql_"+route, "/select/logsql/"+route, 20*time.Millisecond)
		m.RecordRequestFlow("query_range", "/loki/api/v1/query_range", RequestFlow{ParseCount: 1, VLCalls: 1, VL: time.Millisecond})
	}
	for _, op := range []string{"translate_query", "prefer_working_parser", "translate_stats_response_labels", "patterns_window_cache"} {
		m.RecordInternalOperation(op, "ok", time.Millisecond)
	}
	m.RecordQueryRangeWindowFetchDuration(time.Second)
	if sensitive {
		for i := range 32 {
			id := "id-" + string(rune('a'+i%26)) + string(rune('a'+i/26))
			m.RecordTenantRequestWithRoute(id, "query_range", "", 200, time.Second)
			m.RecordClientIdentityWithRoute(id, "query_range", "", time.Second, 100)
			m.RecordClientQueryLengthWithRoute(id, "query_range", "", 40)
		}
	}
	return m
}
