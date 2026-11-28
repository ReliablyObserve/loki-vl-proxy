package proxy

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// blockingVLCall is one VictoriaLogs request the blocking backend received: when it started, and whether it ended
// by its context (the proxy cancelled it) or ran to its end.
type blockingVLCall struct {
	path      string
	cancelled bool
	ended     time.Time
}

// blockingVLBackend holds every read for hold unless the proxy cancels it first.
type blockingVLBackend struct {
	mu    sync.Mutex
	hold  time.Duration
	calls []*blockingVLCall
}

func (b *blockingVLBackend) handler(w http.ResponseWriter, r *http.Request) {
	_ = r.ParseForm()
	switch r.URL.Path {
	case "/health":
		_, _ = w.Write([]byte("ok"))
		return
	case "/metrics":
		_, _ = fmt.Fprintf(w, "vm_app_version{short_version=%q,version=%q} 1\n", "v1.54.0", "victoria-logs-20260101-000000-tags-v1.54.0-0-gabcdef0123")
		return
	}
	if !strings.Contains(r.FormValue("query"), `app:="x"`) {
		// The proxy's capability probes and background label reads: not the request's.
		w.Header().Set("Content-Type", "application/x-ndjson")
		return
	}
	c := &blockingVLCall{path: r.URL.Path + " " + r.FormValue("query")}
	b.mu.Lock()
	b.calls = append(b.calls, c)
	b.mu.Unlock()
	select {
	case <-r.Context().Done():
		b.mu.Lock()
		c.cancelled, c.ended = true, time.Now()
		b.mu.Unlock()
		return
	case <-time.After(b.hold):
	}
	b.mu.Lock()
	c.ended = time.Now()
	b.mu.Unlock()
	switch r.URL.Path {
	case "/select/logsql/stats_query", "/select/logsql/stats_query_range":
		_, _ = w.Write([]byte(`{"status":"success","data":{"resultType":"vector","result":[]}}`))
	default:
		w.Header().Set("Content-Type", "application/x-ndjson")
		_, _ = w.Write([]byte(`{"_time":"2026-11-27T10:00:00Z","_stream":"{app=\"x\"}","_stream_id":"s","_msg":"level=info msg=hi"}` + "\n"))
	}
}

func (b *blockingVLBackend) snapshot() []blockingVLCall {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]blockingVLCall, 0, len(b.calls))
	for _, c := range b.calls {
		out = append(out, *c)
	}
	return out
}

// startCancellable sends target to the proxy's routes on a request it can cancel; the result channel gets the
// status once the handler returns.
func startCancellable(mux *http.ServeMux, target string) (context.CancelFunc, <-chan int) {
	ctx, cancel := context.WithCancel(context.Background())
	req := httptest.NewRequest(http.MethodGet, target, nil).WithContext(ctx)
	req.Header.Set("X-Scope-OrgID", "0")
	done := make(chan int, 1)
	go func() {
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		done <- rec.Code
	}()
	return cancel, done
}

// Identical requests in flight share one run (serveFlight). The request that started it led the run on its own
// context, so when its client went away (Grafana cancels the requests of the view the user left, #1821) the
// VictoriaLogs read was cancelled and every request still waiting on it started it again: the read twice, and the
// waiter's answer late by the leader's time. The run is now the flight's: it ends when the last request waiting
// on it went away, and a request alone still cancels its read at once.
//
// conformance: semantics/client-cancellation-propagates, semantics/error-coalescing-no-inherited-cancel, loki_api_v1_index_stats, loki_api_v1_query
func TestFlightRunIsCancelledByItsLastRequest(t *testing.T) {
	now := time.Now()
	targets := map[string]string{
		"index/stats": "/loki/api/v1/index/stats?" + url.Values{"query": {`{app="x"}`}, "start": {fmt.Sprint(now.Add(-3 * time.Hour).Unix())}, "end": {fmt.Sprint(now.Unix())}}.Encode(),
		"query":       "/loki/api/v1/query?" + url.Values{"query": {`sum(count_over_time({app="x"}[3h]))`}, "time": {fmt.Sprint(now.Unix())}}.Encode(),
	}
	for name, target := range targets {
		for _, tc := range []struct {
			scenario       string
			cancelLeader   bool
			cancelFollower bool
			followers      int
			wantCalls      int
			wantCancelled  bool
			wantLeaderCode int
			wantFollowerOK bool
		}{
			{"leader leaves, a follower waits", true, false, 1, 1, false, 0, true},
			{"a follower leaves", false, true, 1, 1, false, http.StatusOK, false},
			{"every request leaves", true, true, 1, 1, true, 499, false},
			{"a request alone leaves", true, false, 0, 1, true, 499, false},
		} {
			t.Run(name+"/"+tc.scenario, func(t *testing.T) {
				b := &blockingVLBackend{hold: 600 * time.Millisecond}
				backend := httptest.NewServer(http.HandlerFunc(b.handler))
				defer backend.Close()
				p, err := New(Config{BackendURL: backend.URL, Cache: cache.New(time.Minute, 1000), LogLevel: "error", BackendMinVersion: "v1.40.0", BackendVersionCheckTimeout: time.Second})
				if err != nil {
					t.Fatal(err)
				}
				defer func() { _ = p.Shutdown(context.Background()) }()
				if err := p.ValidateBackendVersionCompatibility(context.Background()); err != nil {
					t.Fatal(err)
				}
				mux := http.NewServeMux()
				p.RegisterRoutes(mux)
				cancelLeader, leader := startCancellable(mux, target)
				defer cancelLeader()
				waitFor(t, func() bool { return len(b.snapshot()) == 1 })
				var cancelFollower context.CancelFunc = func() {}
				var follower <-chan int
				if tc.followers > 0 {
					cancelFollower, follower = startCancellable(mux, target)
					defer cancelFollower()
					time.Sleep(50 * time.Millisecond) // the follower joins the flight
				}
				cancelledAt := time.Now()
				if tc.cancelLeader {
					cancelLeader()
				}
				if tc.cancelFollower {
					cancelFollower()
				}
				leaderCode := <-leader
				followerCode := 0
				if follower != nil {
					followerCode = <-follower
				}
				time.Sleep(700 * time.Millisecond) // a read that ran on would end by now
				calls := b.snapshot()
				if len(calls) != tc.wantCalls {
					t.Fatalf("%d VictoriaLogs reads, want %d: %+v", len(calls), tc.wantCalls, calls)
				}
				if calls[0].cancelled != tc.wantCancelled {
					t.Fatalf("read cancelled %v, want %v", calls[0].cancelled, tc.wantCancelled)
				}
				if tc.wantCancelled && calls[0].ended.Sub(cancelledAt) > 200*time.Millisecond {
					t.Fatalf("read cancelled %v after the last request left", calls[0].ended.Sub(cancelledAt))
				}
				if tc.wantLeaderCode != 0 && leaderCode != tc.wantLeaderCode {
					t.Fatalf("leader answered %d, want %d", leaderCode, tc.wantLeaderCode)
				}
				if tc.wantFollowerOK && followerCode != http.StatusOK {
					t.Fatalf("follower answered %d", followerCode)
				}
			})
		}
	}
}

// Requests that need the same parser probe at the same moment share one read, run under the context of the request
// that started it. When that request went away, the read was cancelled and a request that only followed it got no
// verdict: its query kept both parser stages where it would have kept one. A follower still alive now probes again.
//
// conformance: semantics/client-cancellation-propagates, loki_api_v1_query
func TestParserProbeFollowerOutlivesItsLeader(t *testing.T) {
	var mu sync.Mutex
	probes, cancelled := 0, 0
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		if r.URL.Path == "/health" {
			_, _ = w.Write([]byte("ok"))
			return
		}
		if r.URL.Path != "/select/logsql/query" || r.FormValue("limit") != "25" {
			w.Header().Set("Content-Type", "application/x-ndjson")
			return
		}
		mu.Lock()
		probes++
		mu.Unlock()
		select {
		case <-r.Context().Done():
			mu.Lock()
			cancelled++
			mu.Unlock()
			return
		case <-time.After(300 * time.Millisecond):
		}
		w.Header().Set("Content-Type", "application/x-ndjson")
		_, _ = w.Write([]byte(`{"_time":"2026-11-27T10:00:00Z","_stream":"{app=\"x\"}","_msg":"level=info route=/a"}` + "\n"))
	}))
	defer backend.Close()
	p, err := New(Config{BackendURL: backend.URL, Cache: cache.New(time.Minute, 100), LogLevel: "error"})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = p.Shutdown(context.Background()) }()
	const query = `sum(count_over_time({app="x"} | json | logfmt | drop __error__, __error_details__ [1h]))`
	leaderCtx, cancelLeader := context.WithCancel(context.Background())
	leader := make(chan string, 1)
	go func() {
		leader <- p.preferWorkingParser(leaderCtx, query, "1795777200000000001", "1795780800000000001")
	}()
	waitFor(t, func() bool { mu.Lock(); defer mu.Unlock(); return probes == 1 })
	follower := make(chan string, 1)
	go func() {
		follower <- p.preferWorkingParser(context.Background(), query, "1795777200000000001", "1795780800000000001")
	}()
	time.Sleep(50 * time.Millisecond) // the follower joins the probe
	cancelLeader()
	<-leader
	got := <-follower
	if strings.Contains(got, "json") || !strings.Contains(got, "logfmt") {
		t.Fatalf("follower's query %q: want the logfmt stage the lines need, json dropped", got)
	}
	mu.Lock()
	defer mu.Unlock()
	if probes != 2 || cancelled != 1 {
		t.Fatalf("%d probes, %d cancelled: want the leader's cancelled and one of the follower's", probes, cancelled)
	}
}
