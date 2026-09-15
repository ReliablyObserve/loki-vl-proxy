package proxy

import (
	"bytes"
	"fmt"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// logLineBenchBody returns 500 VictoriaLogs rows from 10 streams. placeholder
// rows are what VictoriaLogs stores for a Loki push of a JSON line without
// _msg; the other rows keep the line in _msg next to the extracted fields.
func logLineBenchBody(placeholder bool) []byte {
	var body bytes.Buffer
	base := time.Date(2026, 9, 15, 7, 0, 0, 0, time.UTC)
	for i := 0; i < 500; i++ {
		msg := fmt.Sprintf(`{\"method\":\"GET\",\"path\":\"/api/v1/users/%d\",\"status\":200,\"trace_id\":\"t%08d\",\"level\":\"info\"}`, i, i)
		if placeholder {
			msg = "missing _msg field; see https://docs.victoriametrics.com/victorialogs/keyconcepts/#message-field"
		}
		fmt.Fprintf(&body, `{"_msg":"%s","_stream":"{app=\"api\",pod=\"api-%d\"}","_stream_id":"000000000000000%d","_time":"%s","app":"api","pod":"api-%d","level":"info","method":"GET","path":"/api/v1/users/%d","status":"200","trace_id":"t%08d"}`+"\n",
			msg, i%10, i%10, base.Add(time.Duration(i)*time.Millisecond).Format(time.RFC3339Nano), i%10, i, i)
	}
	return body.Bytes()
}

func BenchmarkLogLineRows(b *testing.B) {
	p, err := New(Config{BackendURL: "http://unused", Cache: cache.New(time.Minute, 10), LogLevel: "error", EmitStructuredMetadata: true})
	if err != nil {
		b.Fatal(err)
	}
	defer p.cache.Close()
	query := `{app="api"}`
	for _, rows := range []struct {
		name        string
		placeholder bool
	}{{"stored", false}, {"placeholder", true}} {
		body := logLineBenchBody(rows.placeholder)
		b.Run("Reader/"+rows.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if _, _, err := p.vlReaderToLokiStreams(bytes.NewReader(body), query, "", false, false, false); err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run("Window/"+rows.name, func(b *testing.B) {
			shape := newLogQueryShape(query)
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = p.vlLogsToLokiWindowEntriesStream(bytes.NewReader(body), shape, false, false)
			}
		})
	}
}
