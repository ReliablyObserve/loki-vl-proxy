package proxy

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/testutil/vlemu"
)

// BenchmarkSeriesLevelAggregationShapes runs the query shapes of bench/ab sets
// aggregation-over-binary (B1, B2, B4), nested-aggregation (N14, N15) and
// label-replace (L3), plus a bare count by, through the real handler against the
// emulated VictoriaLogs: 120 streams in 4 services, a line every 70s for 4h, a
// 120s step. It reports the VictoriaLogs calls and the response bytes per
// request next to ns/op and allocs/op, for benchstat between builds.
func BenchmarkSeriesLevelAggregationShapes(b *testing.B) {
	const step = 120 * time.Second
	end := binaryAggregationAt
	start := end - int64(4*time.Hour)
	var rows []vlemu.Row
	for svc := 0; svc < 4; svc++ {
		for pod := 0; pod < 30; pod++ {
			stream := fmt.Sprintf(`{env="production",service_name="s%d",app="a%d",namespace="n%d",pod="p%d"}`, svc, svc, pod%3, pod)
			for at := start - int64(10*time.Minute); at <= end; at += 70e9 {
				rows = append(rows, vlemu.Row{TS: at, Stream: stream, Value: 1})
			}
		}
	}
	for _, shape := range []struct{ name, query string }{
		{"B1", `sum by (service_name) (rate({env="production"}[5m]) / 2)`},
		{"B2", `sum by (service_name) (rate({env="production"}[5m]) / count_over_time({env="production"}[5m]))`},
		{"B4", `sum by (app) (sum by (service_name, app) (rate({env="production"}[5m])) + ignoring(service_name) sum by (app) (rate({env="production"}[5m])))`},
		{"N14", `sum by (app) (max by (app, pod) (count_over_time({env="production"}[5m])))`},
		{"N15", `sum by (namespace) (sum by (namespace, app) (rate({env="production"}[5m])))`},
		{"L3", `topk(3, label_replace(sum by (service_name) (count_over_time({env="production"}[5m])), "svc_short", "$1", "service_name", "(.*)-.*"))`},
		{"countBy", `count by (service_name) (count_over_time({env="production"}[5m]))`},
		{"avgBy", `avg by (service_name) (rate({env="production"}[5m]))`},
	} {
		b.Run(shape.name, func(b *testing.B) {
			emu := &vlemu.Emulator{T: b, Rows: rows, Version: "v1.52.0"}
			backend := httptest.NewServer(emu.Handler())
			defer backend.Close()
			p, err := New(Config{BackendURL: backend.URL, LogLevel: "error", BackendMinVersion: "v1.40.0", BackendVersionCheckTimeout: time.Second})
			if err != nil {
				b.Fatal(err)
			}
			defer func() { _ = p.Shutdown(context.Background()) }()
			target := fmt.Sprintf("/loki/api/v1/query_range?query=%s&start=%d&end=%d&step=%d", url.QueryEscape(shape.query), start/1e9, end/1e9, int64(step/time.Second))
			headers := map[string]string{"X-Scope-OrgID": "0"}
			bytes := 0
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				res := doCompatProxyRequest(p, target, headers)
				if res.Code != http.StatusOK {
					b.Fatalf("%s: %d %s", shape.query, res.Code, res.Body)
				}
				bytes = res.Body.Len()
			}
			b.StopTimer()
			b.ReportMetric(float64(len(emu.EndpointCalls("/select/logsql/stats_query_range"))+len(emu.EndpointCalls("/select/logsql/query")))/float64(b.N), "vl-calls/op")
			b.ReportMetric(float64(bytes), "resp-B")
		})
	}
}

// BenchmarkMatrixFoldBuilder: 300 streams in 6 groups, 24h at a 120s step, a 5m
// window; the count by (service_name) of the bucket matrix, as the encoded matrix
// folded afterwards and as the builder folds it.
func BenchmarkMatrixFoldBuilder(b *testing.B) {
	start := time.Unix(1_800_000_000, 0)
	step, window := 120*time.Second, 5*time.Minute
	end := start.Add(24 * time.Hour)
	series := map[string]manualSeriesSamples{}
	for i := 0; i < 300; i++ {
		entry := manualSeriesSamples{Metric: map[string]string{"service_name": fmt.Sprintf("svc-%d", i%6), "pod": fmt.Sprintf("pod-%04d", i), "level": "info", "detected_level": "info"}}
		for at := start.Add(-window); !at.After(end); at = at.Add(time.Minute) {
			entry.Samples = append(entry.Samples, rangeMetricSample{ts: at.UnixNano(), value: float64(1 + i%5)})
		}
		series[canonicalLabelsKey(entry.Metric)] = entry
	}
	levels := []foldLevel{byFoldLevel([]string{"service_name"}, "count")}
	b.Run("matrix-then-fold", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			matrix, err := buildHitsRangeMetricMatrix("count_over_time", series, start, end, step, window, 1<<30)
			if err != nil {
				b.Fatal(err)
			}
			if _, ok := foldLevels(matrix, levels...); !ok {
				b.Fatal("fold")
			}
		}
	})
	b.Run("fold-in-builder", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if _, err := buildHitsRangeMetricMatrixFolded("count_over_time", series, start, end, step, window, 1<<30, &matrixFold{levels: levels}); err != nil {
				b.Fatal(err)
			}
		}
	})
}
