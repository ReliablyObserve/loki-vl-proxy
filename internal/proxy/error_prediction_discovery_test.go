package proxy

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// TestLokiErrorRowsReachingVictoriaLogs lists the recorded Loki error rows
// (a 4xx Loki decides from the request) that the proxy answers only after
// asking VictoriaLogs: the candidates for an answer from the plan.
func TestLokiErrorRowsReachingVictoriaLogs(t *testing.T) {
	if os.Getenv("LVP_ERROR_DISCOVERY") == "" {
		t.Skip("set LVP_ERROR_DISCOVERY=1")
	}
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
	n := 0
	rows := readLokiErrorAnswers(t)
	for _, extra := range []string{"loki_build_phase.jsonl", "loki_check_order.jsonl"} {
		saved := lokiErrorAnswersPath
		lokiErrorAnswersPath = strings.Replace(saved, "loki_error_answers.jsonl", extra, 1)
		rows = append(rows, readLokiErrorAnswers(t)...)
		lokiErrorAnswersPath = saved
	}
	for _, row := range rows {
		if row.Loki.Status < 400 || row.Loki.Status >= 500 {
			continue
		}
		before := calls.Load()
		reply := row.replay(t, addr)
		if d := calls.Load() - before; d > 0 {
			n++
			fmt.Printf("%s %s calls=%d loki=%d proxy=%d %q target=%.160s\n", row.ID, row.Route, d, row.Loki.Status, reply.Status, row.Loki.Body, row.Target)
		}
	}
	fmt.Printf("rows reaching VictoriaLogs: %d\n", n)
}
