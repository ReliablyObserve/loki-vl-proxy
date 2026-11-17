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

// countingTailConn counts what a tail writes and keeps nothing.
type countingTailConn struct{ messages, bytes int }

func (c *countingTailConn) SetWriteDeadline(time.Time) error { return nil }
func (c *countingTailConn) WriteMessage(_ int, data []byte) error {
	c.messages++
	c.bytes += len(data)
	return nil
}
func (c *countingTailConn) WriteControl(_ int, data []byte, _ time.Time) error {
	c.messages++
	c.bytes += len(data)
	return nil
}

// BenchmarkTail_NativeForward is the tail's happy path: a VictoriaLogs stream of
// 500 rows converted to Loki frames and written to the client until the stream
// ends.
func BenchmarkTail_NativeForward(b *testing.B) {
	var rows bytes.Buffer
	ts := time.Now().Add(-time.Hour)
	for i := 0; i < 500; i++ {
		fmt.Fprintf(&rows, `{"_time":"%s","_stream":"{app=\"api\",env=\"prod\"}","app":"api","env":"prod","level":"info","_msg":"request %d done in 12ms"}`+"\n",
			ts.Add(time.Duration(i)*time.Millisecond).UTC().Format(time.RFC3339Nano), i)
	}
	c := cache.New(time.Minute, 100)
	defer c.Close()
	p, err := New(Config{BackendURL: "http://127.0.0.1:1", Cache: c, LogLevel: "error"})
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = p.Shutdown(context.Background()) }()
	tp := p.newTailPipeline(`{app="api"}`, false)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		conn := &countingTailConn{}
		p.forwardNativeTail(context.Background(), conn, tp, bytes.NewReader(rows.Bytes()), nil)
		if conn.messages == 0 {
			b.Fatal("no frames")
		}
	}
}

// BenchmarkTail_SyntheticPoll is one poll of the polled tail: 200 new rows
// read from VictoriaLogs, converted to Loki frames and written to the client.
func BenchmarkTail_SyntheticPoll(b *testing.B) {
	var rows bytes.Buffer
	ts := time.Now().Add(-time.Minute)
	for i := 0; i < 200; i++ {
		fmt.Fprintf(&rows, `{"_time":"%s","_stream":"{app=\"api\",env=\"prod\"}","app":"api","env":"prod","level":"info","_msg":"request %d done in 12ms"}`+"\n",
			ts.Add(time.Duration(i)*time.Millisecond).UTC().Format(time.RFC3339Nano), i)
	}
	body := rows.Bytes()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/x-ndjson")
		_, _ = w.Write(body)
	}))
	defer srv.Close()
	c := cache.New(time.Minute, 100)
	defer c.Close()
	p, err := New(Config{BackendURL: srv.URL, Cache: c, LogLevel: "error"})
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = p.Shutdown(context.Background()) }()
	tp := p.newTailPipeline(`{app="api"}`, false)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		conn := &countingTailConn{}
		start := ts.Add(-time.Second)
		if err := p.writeSyntheticTailBatch(context.Background(), conn, `{app="api"}`, tp, &start, newSyntheticTailSeen(maxSyntheticTailSeenEntries)); err != nil || conn.messages == 0 {
			b.Fatalf("poll: %v, %d frames", err, conn.messages)
		}
	}
}
