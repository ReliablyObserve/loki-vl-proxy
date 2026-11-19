package proxy

import (
	"bytes"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// logDropKeepBenchProxy is a proxy in the Loki-compatible profile, which reads
// the line-field exposure of every log response.
func logDropKeepBenchProxy(b *testing.B) *Proxy {
	b.Helper()
	p, err := New(Config{BackendURL: "http://127.0.0.1:1", Cache: cache.NewDisabled(), LogLevel: "error", EmitStructuredMetadata: true,
		LabelStyle: LabelStyleUnderscores, MetadataFieldMode: MetadataFieldModeTranslated})
	if err != nil {
		b.Fatal(err)
	}
	if !p.lokiCompatibleProfile() {
		b.Fatal("not the Loki-compatible profile")
	}
	return p
}

// logDropKeepBenchQueries are log queries whose responses the proxy
// post-processes: conditional and bare drop and keep stages, after a parser,
// and a line_format the Loki-compatible profile reads for the line-field
// exposure.
var logDropKeepBenchQueries = map[string]string{
	"drop":        `{app="checkout"} | drop level="info", trace_id`,
	"keep":        `{app="checkout"} | keep level=~"warn|error", app`,
	"json-drop":   `{app="checkout"} | json | drop order_id | keep level!="info"`,
	"line-format": `{app="checkout"} | logfmt | line_format "{{.msg}}" | drop caller`,
}

// BenchmarkLogDropKeepResponse measures the conversion of a VictoriaLogs log
// response of 500 rows for queries with drop and keep stages: the
// post-processing reads its conditions once per response.
func BenchmarkLogDropKeepResponse(b *testing.B) {
	for _, name := range []string{"drop", "keep", "json-drop", "line-format"} {
		query := logDropKeepBenchQueries[name]
		body := detectedLevelBenchBody("logfmt")
		b.Run(name, func(b *testing.B) {
			p := logDropKeepBenchProxy(b)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, _, err := p.vlReaderToLokiStreams(bytes.NewReader(body), query, "", false, false, false); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkLogDropKeepConditions measures reading a log query's drop and keep
// conditions and its line-field exposure, as each log response does.
func BenchmarkLogDropKeepConditions(b *testing.B) {
	p := logDropKeepBenchProxy(b)
	for _, name := range []string{"drop", "keep", "json-drop", "line-format"} {
		query := logDropKeepBenchQueries[name]
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_, _, _, _ = extractDropKeepFromAST(query)
				_ = p.lineFieldExposure(query)
				_, _ = p.backendLogQuery(query)
			}
		})
	}
}
