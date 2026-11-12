package proxy

import (
	"bytes"
	"context"
	"fmt"
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
