package proxy

import (
	"bytes"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// detectedLevelBenchBody returns 500 VictoriaLogs rows of one kind for the
// log-response conversion benchmarks.
func detectedLevelBenchBody(kind string) []byte {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	var body []byte
	for i := 0; i < 500; i++ {
		row := map[string]string{
			"_time":   start.Add(time.Duration(i) * time.Millisecond).Format(time.RFC3339Nano),
			"_stream": fmt.Sprintf(`{app="checkout",pod="checkout-%d"}`, i%4),
			"app":     "checkout",
			"pod":     fmt.Sprintf("checkout-%d", i%4),
		}
		switch kind {
		case "stored":
			// Level stored as a field (Loki push structured metadata, OTel, jsonline).
			row["_msg"] = fmt.Sprintf("GET /api/v1/orders/%d 200 %dms", i, i%97)
			row["level"] = []string{"info", "warn", "error"}[i%3]
			row["trace_id"] = fmt.Sprintf("%032x", i)
		case "logfmt":
			row["_msg"] = fmt.Sprintf(`ts=2026-01-01T00:00:00Z caller=orders.go:%d level=%s msg="order processed" order_id=%d duration=%dms`, i%400, []string{"info", "warn", "error"}[i%3], i, i%97)
		case "json":
			row["_msg"] = fmt.Sprintf(`{"ts":"2026-01-01T00:00:00Z","level":"%s","msg":"order processed","order_id":%d}`, []string{"info", "warn", "error"}[i%3], i)
		default:
			// Plain text without any level: the full keyword scan runs.
			row["_msg"] = fmt.Sprintf("GET /api/v1/orders/%d 200 %dms upstream=orders-%d cache=miss", i, i%97, i%7)
		}
		encoded, _ := json.Marshal(row)
		body = append(append(body, encoded...), '\n')
	}
	return body
}

func benchmarkDetectedLevelProxy(b *testing.B) *Proxy {
	b.Helper()
	p, err := New(Config{BackendURL: "http://127.0.0.1:1", Cache: cache.NewDisabled(), LogLevel: "error", EmitStructuredMetadata: true})
	if err != nil {
		b.Fatal(err)
	}
	return p
}

// BenchmarkVLReaderToLokiStreams_Levels measures the single-request log
// conversion per row kind, in the default and categorize-labels encodings.
func BenchmarkVLReaderToLokiStreams_Levels(b *testing.B) {
	for _, kind := range []string{"plain", "stored", "logfmt", "json"} {
		body := detectedLevelBenchBody(kind)
		for _, categorized := range []bool{false, true} {
			name := kind
			if categorized {
				name += "/categorized"
			}
			b.Run(name, func(b *testing.B) {
				p := benchmarkDetectedLevelProxy(b)
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if _, _, err := p.vlReaderToLokiStreams(bytes.NewReader(body), `{app="checkout"}`, "", categorized, categorized, false); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}

// BenchmarkQueryRangeWindowEntries_Levels is the windowed counterpart.
func BenchmarkQueryRangeWindowEntries_Levels(b *testing.B) {
	for _, kind := range []string{"plain", "stored", "logfmt", "json"} {
		body := detectedLevelBenchBody(kind)
		for _, categorized := range []bool{false, true} {
			name := kind
			if categorized {
				name += "/categorized"
			}
			b.Run(name, func(b *testing.B) {
				p := benchmarkDetectedLevelProxy(b)
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					entries := p.vlLogsToLokiWindowEntries(body, `{app="checkout"}`, categorized, categorized)
					_ = groupQueryRangeWindowEntries(entries, "backward", categorized, categorized)
				}
			})
		}
	}
}
