package proxy

// Benchmarks for the stats query response translation hot path.
// These guard against regressions in translateStatsResponseLabelsWithContext
// and addUnderscorefallbackByLabels, which run on every metric query_range
// response for the underscore proxy.
//
// Run:
//   go test -bench=BenchmarkStatsTranslation -benchmem -count=5 ./internal/proxy/

import (
	"fmt"
	"strings"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// buildStatsMatrixBody builds a synthetic stats_query_range JSON body with
// n series, each containing m data points. Simulates the typical Drilldown
// log-count response shape: {"metric":{"service.name":"svcN"},"values":[...]}.
func buildStatsMatrixBody(n, m int) []byte {
	var sb strings.Builder
	sb.WriteString(`{"status":"success","data":{"resultType":"matrix","result":[`)
	for i := 0; i < n; i++ {
		if i > 0 {
			sb.WriteByte(',')
		}
		sb.WriteString(`{"metric":{"service.name":"service-`)
		sb.WriteString(fmt.Sprintf("%d", i))
		sb.WriteString(`"},"values":[`)
		for j := 0; j < m; j++ {
			if j > 0 {
				sb.WriteByte(',')
			}
			sb.WriteString(fmt.Sprintf("[%d,\"%d\"]", 1700000000+j*60, (i+1)*100+j))
		}
		sb.WriteString(`]}`)
	}
	sb.WriteString(`]}}`)
	return []byte(sb.String())
}

// buildStatsMatrixBodyCoalesced simulates a response from the expanded by()
// clause (both "service.name" and "service_name" in the metric) — the shape
// produced after the underscore-fallback fix.
func buildStatsMatrixBodyCoalesced(n, m int) []byte {
	var sb strings.Builder
	sb.WriteString(`{"status":"success","data":{"resultType":"matrix","result":[`)
	for i := 0; i < n; i++ {
		if i > 0 {
			sb.WriteByte(',')
		}
		// Loki-push shape: service.name="" (not found), service_name=<value>
		sb.WriteString(`{"metric":{"service.name":"","service_name":"service-`)
		sb.WriteString(fmt.Sprintf("%d", i))
		sb.WriteString(`"},"values":[`)
		for j := 0; j < m; j++ {
			if j > 0 {
				sb.WriteByte(',')
			}
			sb.WriteString(fmt.Sprintf("[%d,\"%d\"]", 1700000000+j*60, (i+1)*100+j))
		}
		sb.WriteString(`]}`)
	}
	sb.WriteString(`]}}`)
	return []byte(sb.String())
}

func newUnderscokeProxy(b *testing.B) *Proxy {
	b.Helper()
	p, err := New(Config{
		BackendURL: "http://127.0.0.1:9999",
		Cache:      cache.New(60e9, 100),
		LogLevel:   "error",
		LabelStyle: LabelStyleUnderscores,
	})
	if err != nil {
		b.Fatalf("create proxy: %v", err)
	}
	return p
}

// BenchmarkStatsTranslation_translateStatsResponseLabels_Small benchmarks
// translation of a small metric response (10 series × 60 points).
func BenchmarkStatsTranslation_translateStatsResponseLabels_Small(b *testing.B) {
	p := newUnderscokeProxy(b)
	body := buildStatsMatrixBody(10, 60)
	q := `sum by (service_name) (count_over_time({app="test"}[60s]))`
	b.ResetTimer()
	b.SetBytes(int64(len(body)))
	for i := 0; i < b.N; i++ {
		_ = p.translateStatsResponseLabelsWithContext(b.Context(), body, q)
	}
}

// BenchmarkStatsTranslation_translateStatsResponseLabels_Large benchmarks
// translation of a larger metric response (100 series × 60 points).
func BenchmarkStatsTranslation_translateStatsResponseLabels_Large(b *testing.B) {
	p := newUnderscokeProxy(b)
	body := buildStatsMatrixBody(100, 60)
	q := `sum by (service_name) (count_over_time({app="test"}[60s]))`
	b.ResetTimer()
	b.SetBytes(int64(len(body)))
	for i := 0; i < b.N; i++ {
		_ = p.translateStatsResponseLabelsWithContext(b.Context(), body, q)
	}
}

// BenchmarkStatsTranslation_Coalesced_Small benchmarks translation of the
// expanded by() response (both service.name and service_name present, the
// typical shape after the underscore-fallback fix for Loki-push data).
func BenchmarkStatsTranslation_Coalesced_Small(b *testing.B) {
	p := newUnderscokeProxy(b)
	body := buildStatsMatrixBodyCoalesced(10, 60)
	q := `sum by (service_name) (count_over_time({app="test"}[60s]))`
	b.ResetTimer()
	b.SetBytes(int64(len(body)))
	for i := 0; i < b.N; i++ {
		_ = p.translateStatsResponseLabelsWithContext(b.Context(), body, q)
	}
}

// BenchmarkStatsTranslation_Coalesced_Large benchmarks translation with 100
// series and coalesced metric fields.
func BenchmarkStatsTranslation_Coalesced_Large(b *testing.B) {
	p := newUnderscokeProxy(b)
	body := buildStatsMatrixBodyCoalesced(100, 60)
	q := `sum by (service_name) (count_over_time({app="test"}[60s]))`
	b.ResetTimer()
	b.SetBytes(int64(len(body)))
	for i := 0; i < b.N; i++ {
		_ = p.translateStatsResponseLabelsWithContext(b.Context(), body, q)
	}
}

// BenchmarkStatsTranslation_addUnderscorefallbackByLabels benchmarks the
// by() clause expansion for the underscore proxy (called per metric query).
func BenchmarkStatsTranslation_addUnderscorefallbackByLabels(b *testing.B) {
	p := newUnderscokeProxy(b)
	logsqlQuery := `{app:="payment-service"} | stats by (service.name, level) count() as c`
	origGroupBy := []string{"service_name", "detected_level"}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = p.addUnderscorefallbackByLabels(logsqlQuery, origGroupBy)
	}
}
