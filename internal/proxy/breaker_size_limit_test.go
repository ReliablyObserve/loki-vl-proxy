package proxy

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"syscall"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/testutil/vlemu"
)

// A request whose answer is above what the proxy buffers is that request's own outcome: VictoriaLogs answered. It
// never counts as a backend failure, so the circuit breaker stays closed and the next query reaches VictoriaLogs, with
// the stats extent cache on (as deployments run it, where an oversized bucket read surfaced as a transport error).
//
// conformance: backend-deadlines-and-cancellation, semantics/unwrap-per-series-identity-raw-rows
func TestOversizedReadsKeepTheBreakerClosed(t *testing.T) {
	var rows []vlemu.Row
	// Old enough for the stats extent cache to keep its buckets.
	end := time.Now().Add(-2 * time.Hour).Truncate(time.Minute)
	for i := 0; i < 3000; i++ {
		ts := end.Add(-time.Duration(i) * 700 * time.Millisecond)
		rows = append(rows, dynWindowRow(ts.UnixNano(), []string{"a", "b"}[i%2], "logfmt", map[string]string{"req": "r" + strconv.Itoa(i)}, float64(i%9)))
	}
	emu := &vlemu.Emulator{T: t, Rows: rows, Field: "amount", Version: "v1.53.0"}
	backend := httptest.NewServer(emu.Handler())
	defer backend.Close()
	c := cache.NewWithMaxBytes(time.Minute, 100000, 256<<20)
	defer c.Close()
	cfg := Config{BackendURL: backend.URL, Cache: c, LogLevel: "error", QueryRangeExtentCache: true, QueryRangeExtentCacheTTL: time.Hour,
		BackendMinVersion: "v1.49.0", BackendVersionCheckTimeout: time.Second}
	// The bytes the proxy buffers from one VictoriaLogs answer, below the identity's bucket answer.
	cfg.ExecutionLimits.BackendMaxBufferedResponseBytes = 16 << 10
	p, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer closeTestProxy(p)
	_ = p.ValidateBackendVersionCompatibility(context.Background())
	get := func(q string, i int) int {
		params := url.Values{"query": {q}, "start": {strconv.FormatInt(end.Add(-30*time.Minute).Unix()+int64(i), 10)},
			"end": {strconv.FormatInt(end.Unix()+int64(i), 10)}, "step": {"30"}}
		return doCompatProxyRequest(p, "/loki/api/v1/query_range?"+params.Encode(), map[string]string{"X-Scope-OrgID": "0"}).Code
	}
	for i := 0; i < 8; i++ { // past the breaker's default threshold of 5
		if code := get(`sum by (pod) (avg_over_time({app="dw"} | logfmt | unwrap amount [2m]))`, i); code == http.StatusOK {
			t.Logf("read %d answered 200 (chunked)", i)
		}
	}
	if !p.breaker.Allow() {
		t.Fatal("oversized reads opened the circuit breaker")
	}
	emu.Mu.Lock()
	emu.Calls = nil
	emu.Mu.Unlock()
	if code := get(`sum by (pod) (count_over_time({app="dw"}[2m]))`, 100); code != http.StatusOK {
		t.Fatalf("a plain query after the oversized reads answered %d", code)
	}
	if len(emu.EndpointCalls("/select/logsql/stats_query_range")) == 0 {
		t.Fatal("the plain query did not reach VictoriaLogs")
	}
}

// shouldRecordBreakerFailure counts transport failures only: a proxy-side bound on what one request reads (a response
// too large to buffer, a series or row budget, a group guard), bare or wrapped, is the request's outcome.
//
// conformance: backend-deadlines-and-cancellation
func TestBreakerIgnoresProxySideLimits(t *testing.T) {
	refused := &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED}
	ctx := context.Background()
	for _, tc := range []struct {
		name   string
		err    error
		counts bool
	}{
		{"connection refused", refused, true},
		{"wrapped connection refused", fmt.Errorf("post: %w", refused), true},
		{"body too large", errBodyTooLarge, false},
		{"wrapped body too large", fmt.Errorf("read: %w", errBodyTooLarge), false},
		{"backend body too large", newBackendBodyTooLarge(64 << 20), false},
		{"manual metric too large", newManualMetricResponseTooLarge(64 << 20), false},
		{"series count exceeded", errSeriesCountExceeded, false},
		{"stats group guard", fmt.Errorf("x: %w", errStatsGroupGuard), false},
		{"series limit", newSeriesLimitError(ctx, 500), false},
		{"row limit", newRowLimitError(10, "", 20), false},
		{"other", errors.New("EOF"), true},
	} {
		if got := shouldRecordBreakerFailure(ctx, tc.err); got != tc.counts {
			t.Errorf("%s: counts %v, want %v", tc.name, got, tc.counts)
		}
	}
}
