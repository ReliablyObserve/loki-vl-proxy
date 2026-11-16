package proxy

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	fj "github.com/valyala/fastjson"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

const (
	benchLevelStream    = `{app="payment-service",cluster="us-east-1",container="payment",env="production",level="error",namespace="prod",pod="payment-service-391a2-9208",service_name="payment-service"}`
	benchSeverityStream = `{app="payment-service",cluster="us-east-1",container="payment",env="production",namespace="prod",pod="payment-service-391a2-9208",service_name="payment-service",severity="ERROR"}`
)

// BenchmarkLogQueryStreamDescriptorMissStreamLevel times the descriptor a raw-row
// metric read builds once per distinct (stream, level) pair.
func BenchmarkLogQueryStreamDescriptorMissStreamLevel(b *testing.B) {
	p, err := New(Config{BackendURL: "http://unused", Cache: cache.New(60*time.Second, 10), LogLevel: "error"})
	if err != nil {
		b.Fatal(err)
	}
	for _, c := range []struct{ name, stream, level string }{{"level", benchLevelStream, "error"}, {"severity", benchSeverityStream, ""}, {"nolevel", `{app="a",namespace="prod",pod="a-1"}`, ""}, {"field level", `{app="a",namespace="prod",pod="a-1"}`, "warn"}} {
		b.Run(c.name, func(b *testing.B) {
			streams := map[string]map[string]string{c.stream: parseStreamLabels(c.stream)}
			b.ReportAllocs()
			for b.Loop() {
				_ = p.logQueryStreamDescriptorMiss(c.stream, c.level, c.stream+"\x00"+c.level, streams, map[string]cachedLogQueryStreamDescriptor{})
			}
		})
	}
}

// BenchmarkTranslateStatsQRMetricStream times the relabelling of one stats
// answer series read by stream (an unwrap at a step the range divides).
func BenchmarkTranslateStatsQRMetricStream(b *testing.B) {
	p, err := New(Config{BackendURL: "http://unused", Cache: cache.New(60*time.Second, 10), LogLevel: "error"})
	if err != nil {
		b.Fatal(err)
	}
	for _, c := range []struct{ name, stream string }{{"level", benchLevelStream}, {"severity", benchSeverityStream}} {
		b.Run(c.name, func(b *testing.B) {
			v := fj.MustParse(`{"_stream":` + fj.MustParse(`"`+escapeBenchJSON(c.stream)+`"`).String() + `,"__lvp_bad":"0"}`)
			translated, synthetic := map[string]string{}, map[string]string{}
			b.ReportAllocs()
			for b.Loop() {
				_, _ = p.translateStatsQRMetricFJ(v, levelGroupingRequest{}, translated, synthetic)
			}
		})
	}
}

func escapeBenchJSON(s string) string {
	out := make([]byte, 0, len(s)+8)
	for i := 0; i < len(s); i++ {
		if s[i] == '"' || s[i] == '\\' {
			out = append(out, '\\')
		}
		out = append(out, s[i])
	}
	return string(out)
}

// BenchmarkResponseKeyFencesParser times the fence chain of a metric query
// with a parser stage, which the stream level fence now covers.
func BenchmarkResponseKeyFencesParser(b *testing.B) {
	const query = `sum by (service_name) (count_over_time({service_name="payment-service"} | logfmt | status >= 500 [5m]))`
	r := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+url.Values{"query": {query}, "start": {"1"}, "end": {"3600"}, "step": {"60"}}.Encode(), nil)
	b.ReportAllocs()
	for b.Loop() {
		_ = responseKeyFences(r, query, responseKeyInnerQueryRange)
	}
}

// BenchmarkBuildMetricSeriesEntryRawRow times the series a raw-row metric read builds once per distinct
// (stream, level) pair: VictoriaLogs answers the row with its stream fields as fields, so a stream labelled level
// gives the row the same level.
func BenchmarkBuildMetricSeriesEntryRawRow(b *testing.B) {
	p, err := New(Config{BackendURL: "http://unused", Cache: cache.New(60*time.Second, 10), LogLevel: "error"})
	if err != nil {
		b.Fatal(err)
	}
	for _, c := range []struct{ name, stream, level string }{{"level", benchLevelStream, "error"}, {"severity", benchSeverityStream, ""}, {"severity and row level", benchSeverityStream, "debug"}, {"nolevel", `{app="a",namespace="prod",pod="a-1"}`, ""}, {"field level", `{app="a",namespace="prod",pod="a-1"}`, "warn"}} {
		b.Run(c.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				_ = p.buildMetricSeriesEntry(c.stream, c.level, nil, nil, false, nil)
			}
		})
	}
}

// BenchmarkResponseKeyFencesPlainMetric times the fence chain of a metric query with no parser stage, which the
// detected_level chain fence now covers.
func BenchmarkResponseKeyFencesPlainMetric(b *testing.B) {
	const query = `sum by (detected_level) (count_over_time({service_name="payment-service"} [5m]))`
	r := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+url.Values{"query": {query}, "start": {"1"}, "end": {"3600"}, "step": {"60"}}.Encode(), nil)
	b.ReportAllocs()
	for b.Loop() {
		_ = responseKeyFences(r, query, responseKeyInnerQueryRange)
	}
}
