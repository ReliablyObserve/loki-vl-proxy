package proxy

import (
	"bytes"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// BenchmarkJSONLineKeysRows converts 500 rows (placeholder rows, as VictoriaLogs stores a JSON line pushed without
// disable_message_parsing, and rows that kept their line) of a categorized log query in the Loki-compatible
// profile, with -json-line-fields-as-line-keys off (the path every deployment runs) and on.
func BenchmarkJSONLineKeysRows(b *testing.B) {
	for _, mode := range []struct {
		name string
		on   bool
	}{{"off", false}, {"on", true}} {
		p, err := New(Config{BackendURL: "http://unused", Cache: cache.New(time.Minute, 10), LogLevel: "error", EmitStructuredMetadata: true,
			LabelStyle: LabelStyleUnderscores, MetadataFieldMode: MetadataFieldModeTranslated, JSONLineFieldsAsLineKeys: mode.on})
		if err != nil {
			b.Fatal(err)
		}
		for _, rows := range []struct {
			name        string
			placeholder bool
		}{{"stored", false}, {"placeholder", true}} {
			body := logLineBenchBody(rows.placeholder)
			for _, query := range []string{`{app="api"}`, `{app="api"} | json`} {
				b.Run(mode.name+"/"+rows.name+"/"+query, func(b *testing.B) {
					b.ReportAllocs()
					for i := 0; i < b.N; i++ {
						if _, _, err := p.vlReaderToLokiStreams(bytes.NewReader(body), query, "", true, true, false); err != nil {
							b.Fatal(err)
						}
					}
				})
			}
		}
		p.cache.Close()
	}
}
