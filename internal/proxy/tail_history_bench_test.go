package proxy

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// tailBenchVL answers every /select/logsql/query with the same rows, as the
// read before the upgrade and a poll of the polling tail do.
func tailBenchVL(b *testing.B, rows []byte) (*Proxy, func()) {
	b.Helper()
	vl := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/x-ndjson")
		_, _ = w.Write(rows)
	}))
	c := cache.New(time.Minute, 100)
	p, err := New(Config{BackendURL: vl.URL, Cache: c, LogLevel: "error"})
	if err != nil {
		b.Fatal(err)
	}
	return p, func() { _ = p.Shutdown(context.Background()); c.Close(); vl.Close() }
}

func tailBenchHistoryRows(n int) []byte {
	var rows bytes.Buffer
	ts := time.Now().Add(-time.Minute)
	for i := n - 1; i >= 0; i-- {
		fmt.Fprintf(&rows, `{"_time":"%s","_stream":"{app=\"api\",env=\"prod\"}","app":"api","env":"prod","level":"info","_msg":"request %d done in 12ms"}`+"\n",
			ts.Add(time.Duration(i)*time.Millisecond).UTC().Format(time.RFC3339Nano), i)
	}
	return rows.Bytes()
}

// BenchmarkTail_SyntheticBatch is one poll of the polling tail: 200 rows read,
// noted as seen and written as frames.
func BenchmarkTail_SyntheticBatch(b *testing.B) {
	p, done := tailBenchVL(b, tailBenchHistoryRows(200))
	defer done()
	tp := p.newTailPipeline(`{app="api"}`, false)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		conn := &countingTailConn{}
		start := time.Now().Add(-time.Hour)
		if err := p.writeSyntheticTailBatch(context.Background(), conn, `app:="api"`, tp, &start, newSyntheticTailSeen(maxSyntheticTailSeenEntries)); err != nil {
			b.Fatal(err)
		}
		if conn.messages == 0 {
			b.Fatal("no frames")
		}
	}
}

// BenchmarkTail_History is the history of a tail with Loki's default limit:
// the read before the upgrade (100 rows) and its frames.
func BenchmarkTail_History(b *testing.B) {
	p, done := tailBenchVL(b, tailBenchHistoryRows(100))
	defer done()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tp := p.newTailPipeline(`{app="api"}`, false) // one per connection
		h, err := p.fetchTailHistory(context.Background(), `app:="api"`, "", "", tp)
		if err != nil || len(h.rows) == 0 {
			b.Fatalf("history: %d bytes, %v", len(h.rows), err)
		}
		conn := &countingTailConn{}
		if err := p.sendTailHistory(context.Background(), conn, tp, h); err != nil || conn.messages != 1 {
			b.Fatalf("history frames %d, %v", conn.messages, err)
		}
	}
}
