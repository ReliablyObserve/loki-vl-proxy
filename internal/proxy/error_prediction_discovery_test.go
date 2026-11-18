package proxy

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// Every request Loki refuses with a 4xx in the recorded answers (the error
// rows, the pipeline build phase and the check order) is refused before any
// VictoriaLogs call: what Loki decides from the request alone, the proxy
// decides from the request and its plan. A row that reaches VictoriaLogs fails
// here; rows recorded under a configuration the proxy cannot match are not
// compared.
//
// conformance: semantics/error-request-validation-parity, semantics/error-plan-time-refusals
func TestLokiRefusalsSendNoVictoriaLogsCall(t *testing.T) {
	var calls atomic.Int64
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/select/") {
			calls.Add(1)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"values":[]}`))
	}))
	t.Cleanup(backend.Close)
	c := cache.New(time.Minute, 1000)
	p, err := New(Config{BackendURL: backend.URL, Cache: c, LogLevel: "error", BackendTimeout: 5 * time.Second, DefaultMaxQueryLength: DefaultMaxQueryLength})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	mux := http.NewServeMux()
	p.RegisterRoutes(mux)
	front := httptest.NewServer(shippedChain(mux))
	t.Cleanup(front.Close)
	addr := strings.TrimPrefix(front.URL, "http://")
	rows := readLokiErrorAnswers(t)
	for _, extra := range []string{"loki_build_phase.jsonl", "loki_check_order.jsonl"} {
		saved := lokiErrorAnswersPath
		lokiErrorAnswersPath = strings.Replace(saved, "loki_error_answers.jsonl", extra, 1)
		rows = append(rows, readLokiErrorAnswers(t)...)
		lokiErrorAnswersPath = saved
	}
	checked := 0
	for _, row := range rows {
		if row.Loki.Status < 400 || row.Loki.Status >= 500 || row.NotComparable != "" {
			continue
		}
		checked++
		before := calls.Load()
		row.replay(t, addr)
		if d := calls.Load() - before; d > 0 {
			t.Errorf("%s (%s): Loki answers %d %q from the request; the proxy asked VictoriaLogs %d times first", row.ID, row.Route, row.Loki.Status, row.Loki.Body, d)
		}
	}
	if checked < 100 {
		t.Fatalf("only %d recorded refusals checked", checked)
	}
}

// BenchmarkTailHandshakeComplete is the check a tail now makes before its
// history read.
func BenchmarkTailHandshakeComplete(b *testing.B) {
	req := httptest.NewRequest(http.MethodGet, "/loki/api/v1/tail?query=%7Bapp%3D%22api%22%7D", nil)
	req.Header.Set("Connection", "Upgrade")
	req.Header.Set("Upgrade", "websocket")
	req.Header.Set("Sec-WebSocket-Version", "13")
	req.Header.Set("Sec-WebSocket-Key", "dGhlIHNhbXBsZSBub25jZQ==")
	b.ReportAllocs()
	for b.Loop() {
		if !tailHandshakeComplete(req) {
			b.Fatal("complete handshake refused")
		}
	}
}
