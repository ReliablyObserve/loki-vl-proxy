package proxy

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// TestSanitizeUpstreamError_RedactsQuery covers finding 1: transport errors are
// *url.Error whose message embeds the full backend URL including the LogQL/LogsQL
// query in RawQuery, leaking it into logs and handler error responses.
func TestSanitizeUpstreamError_RedactsQuery(t *testing.T) {
	underlying := errors.New("dial tcp 127.0.0.1:9428: connect: connection refused")
	raw := &url.Error{
		Op:  "Get",
		URL: `http://victorialogs:9428/select/logsql/query?query=%7Bapp%3D%22secret%22%7D+%7C%3D+%22password%3Dhunter2%22&start=1`,
		Err: underlying,
	}

	// Redaction on by default.
	p := &Proxy{}
	got := p.sanitizeUpstreamError(raw)
	msg := got.Error()
	for _, leak := range []string{"secret", "password", "hunter2", "query="} {
		if strings.Contains(msg, leak) {
			t.Errorf("sanitized error still leaks %q: %s", leak, msg)
		}
	}
	if !strings.Contains(msg, "redacted") {
		t.Errorf("expected redacted marker, got: %s", msg)
	}
	// Underlying cause preserved so statusFromUpstreamErr / breaker still classify.
	if !errors.Is(got, underlying) {
		t.Errorf("sanitized error lost the underlying cause: %v", got)
	}
	// Path/host kept for diagnostics.
	if !strings.Contains(msg, "victorialogs:9428") || !strings.Contains(msg, "/select/logsql/query") {
		t.Errorf("expected host+path retained, got: %s", msg)
	}

	// Raw-query debug mode opts out of redaction.
	pRaw := &Proxy{debugLogRawQueries: true}
	if pRaw.sanitizeUpstreamError(raw).Error() != raw.Error() {
		t.Error("debugLogRawQueries=true should not redact")
	}
	// Non-url errors and nil pass through.
	plain := errors.New("boom")
	if p.sanitizeUpstreamError(plain) != plain {
		t.Error("non-url error should pass through unchanged")
	}
	if p.sanitizeUpstreamError(nil) != nil {
		t.Error("nil should pass through")
	}
}

// TestColdRouter_StopTerminatesLoop covers finding 6: the cold-router refresh
// loop must be tied to the proxy lifecycle (Stop cancels it; Shutdown calls Stop)
// so it doesn't leak goroutines across proxy lifecycles.
func TestColdRouter_StopTerminatesLoop(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	cr, err := NewColdRouter(ColdBackendConfig{
		Enabled: true, URL: "http://127.0.0.1:1", ManifestRefresh: time.Hour,
	}, logger)
	if err != nil || cr == nil {
		t.Fatalf("new cold router: %v", err)
	}
	cr.Start(context.Background())

	done := make(chan struct{})
	go func() { cr.Stop(context.Background()); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Stop did not return within 5s — refresh loop leaked")
	}
	// Idempotent: a second Stop must not panic (double close guard).
	cr.Stop(context.Background())

	// Stop without Start must not block (started guard returns before any wait).
	cr2, _ := NewColdRouter(ColdBackendConfig{Enabled: true, URL: "http://127.0.0.1:1"}, logger)
	stopped := make(chan struct{})
	go func() { cr2.Stop(context.Background()); close(stopped) }()
	select {
	case <-stopped:
	case <-time.After(2 * time.Second):
		t.Fatal("Stop without Start blocked")
	}
}

// TestPurgePeerCaches_ConcurrencyCapped covers finding 7: the cache-purge fanout
// must bound concurrency so a large ring doesn't burst to every peer at once.
func TestPurgePeerCaches_ConcurrencyCapped(t *testing.T) {
	const numPeers = 40
	var inflight, maxInflight atomic.Int32
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := inflight.Add(1)
		for {
			m := maxInflight.Load()
			if n <= m || maxInflight.CompareAndSwap(m, n) {
				break
			}
		}
		time.Sleep(20 * time.Millisecond)
		inflight.Add(-1)
		w.WriteHeader(http.StatusOK)
	})
	addrs := make([]string, 0, numPeers)
	for i := 0; i < numPeers; i++ {
		srv := httptest.NewServer(h)
		t.Cleanup(srv.Close)
		u, _ := url.Parse(srv.URL)
		addrs = append(addrs, u.Host)
	}
	p, _, _ := newPeerProxy(t, "tok", strings.Join(addrs, ","))

	results := p.purgePeerCaches(context.Background())
	purged := 0
	for _, st := range results {
		if st == "purged" {
			purged++
		}
	}
	if purged != numPeers {
		t.Errorf("want %d peers purged, got %d (results=%v)", numPeers, purged, results)
	}
	if got := maxInflight.Load(); got > int32(peerPurgeMaxConcurrency) {
		t.Errorf("concurrency cap exceeded: max in-flight %d > cap %d", got, peerPurgeMaxConcurrency)
	}
	if maxInflight.Load() < 2 {
		t.Errorf("expected real concurrency (>=2); got %d — fanout not exercised", maxInflight.Load())
	}
}

// TestRedactBackendError covers finding 1 (round 2): VictoriaLogs error bodies
// can echo the LogQL/LogsQL query (selectors, filter values), which would leak
// into error logs/responses when handlers pass the raw body to writeError /
// writeDrilldownPartialFromUpstream.
func TestRedactBackendError(t *testing.T) {
	p := &Proxy{}
	body := []byte(`{"error":"cannot parse query {namespace=\"prod\",app=\"billing\"} |= \"password=hunter2sekret\": unexpected token at offset deadbeefcafe1234"}`)

	got := p.redactBackendError(body)
	for _, leak := range []string{"prod", "billing", "password=hunter2sekret", "deadbeefcafe1234"} {
		if strings.Contains(got, leak) {
			t.Errorf("redacted backend error still leaks %q: %s", leak, got)
		}
	}
	// Diagnostic skeleton is preserved.
	if !strings.Contains(got, "cannot parse query") {
		t.Errorf("expected the error skeleton retained, got: %s", got)
	}

	// Short quoted keywords are NOT redacted (kept readable).
	short := p.redactBackendError([]byte(`{"error":"unknown field \"id\""}`))
	if !strings.Contains(short, `"id"`) {
		t.Errorf("short quoted field should be readable, got: %s", short)
	}

	// debugLogRawQueries opts out.
	praw := &Proxy{debugLogRawQueries: true}
	if !strings.Contains(praw.redactBackendError(body), "password=hunter2sekret") {
		t.Error("debugLogRawQueries=true should not redact the backend error")
	}

	// Non-JSON body + empty pass through extractVLErrorMsg semantics.
	if p.redactBackendError(nil) != "" {
		t.Error("nil body should redact to empty")
	}
}

// TestWindowedHits_GatedToDrilldownOrHighCard covers finding 3 (round 2): the
// window-sampled /hits rewrite (lossy per-window top-N sampling) must apply ONLY
// to Drilldown requests OR high-cardinality fields. A normal Grafana dashboard
// panel over a low-cardinality field, an Explore low-card query, and a direct API
// client must all fall through to exact stats_query_range.
func TestWindowedHits_GatedToDrilldownOrHighCard(t *testing.T) {
	p := &Proxy{}
	// 24h range so the >=2h gate would otherwise pass.
	mk := func(drilldown bool, grafanaUA bool) *http.Request {
		r := httptest.NewRequest(http.MethodGet,
			"/loki/api/v1/query_range?start=1700000000000000000&end=1700086400000000000&step=120s", nil)
		if drilldown {
			r.Header.Set("X-Query-Tags", "Source=grafana-lokiexplore-app")
		}
		if grafanaUA {
			r.Header.Set("User-Agent", "Grafana/11.0.0")
		}
		return r
	}
	lowCard := `status!="" | stats by (status) count()`
	highCard := `trace_id!="" | stats by (trace_id) count()`

	rejects := func(name string, r *http.Request, logsql string) {
		t.Helper()
		w := httptest.NewRecorder()
		if p.tryHighCardCountByWindowedHits(w, r, logsql) {
			t.Errorf("%s: windowed-hits ran but should fall through to exact stats", name)
		}
		if w.Body.Len() != 0 {
			t.Errorf("%s: rejected path wrote a body: %s", name, w.Body.String())
		}
	}

	// Non-Grafana, low-card → exact. (Also covers finding-3 round 1.)
	rejects("non-grafana low-card", mk(false, false), lowCard)
	// Grafana dashboard/Explore (UA only, NOT Drilldown), low-card → exact.
	rejects("grafana dashboard low-card", mk(false, true), lowCard)

	// The gate ITSELF admits Drilldown (any field) and high-card fields. We assert
	// the gate predicates rather than the full path (which needs a backend).
	if !isGrafanaDrilldownRequest(mk(true, true)) {
		t.Error("drilldown-tagged request must be recognized as Drilldown")
	}
	if isGrafanaDrilldownRequest(mk(false, true)) {
		t.Error("a plain Grafana-UA request must NOT be treated as Drilldown")
	}
	if !isLikelyHighCardinalityField("trace_id") {
		t.Error("trace_id must be recognized as high-cardinality")
	}
	if isLikelyHighCardinalityField("status") {
		t.Error("status must NOT be treated as high-cardinality")
	}
	_ = highCard
}
