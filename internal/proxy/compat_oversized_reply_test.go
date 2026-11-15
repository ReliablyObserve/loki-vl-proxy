package proxy

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/testutil/vlemu"
)

// A reply larger than the edge cache's entry bound (a tenth of its budget) was
// cached nowhere on the Loki side when it answered a range ending within
// -query-range-freshness of now or an instant query: the exact range cache
// keeps no near-now answer, instant answers have no exact cache, and the shared
// copy took replies up to 128 KiB. Every repeat of a large Drilldown breakdown,
// a parser metric or an instant over many series asked VictoriaLogs again. Such
// a reply is now kept in the primary cache's memory with the edge cache's TTL
// and freshness rule, so a repeat is a hit that asks VictoriaLogs nothing and
// answers the same bytes.

// oversizedReplyRows are 650 JSON lines over the last 65 minutes, one every six
// seconds, on 300 streams, so a grouped answer has hundreds of series.
func oversizedReplyRows(now time.Time) []vlemu.Row {
	var rows []vlemu.Row
	for i := 0; i < 650; i++ {
		at := now.Add(-65*time.Minute + time.Duration(i)*6*time.Second)
		n := i % 300
		dur, status := 100+i%900, 200+(i%4)*100
		rows = append(rows, vlemu.Row{
			TS:     at.UnixNano(),
			Stream: fmt.Sprintf(`{app="api-gateway",env="production",pod="api-%04d",service_name="api-gateway"}`, n),
			Msg:    fmt.Sprintf(`{"app":"api-gateway","method":%q,"duration_ms":%d,"status":%d,"trace_id":"t%06d","service":{"name":"api-gateway"}}`, []string{"GET", "POST", "PUT", "DELETE"}[i%4], dur, status, n),
			Value:  float64(dur),
			Labels: map[string]string{"app": "api-gateway", "duration_ms": strconv.Itoa(dur), "status": strconv.Itoa(status), "trace_id": fmt.Sprintf("t%06d", n)},
		})
	}
	return rows
}

// oversizedReplyRequests are the requests of the registry cases whose repeats
// asked VictoriaLogs again on the live stack, over windows ending five minutes
// before now as the verifier's do. An instant without time is evaluated at now;
// the pinned spelling of each also compares with the uncached proxy.
func oversizedReplyRequests(now time.Time) []struct{ name, target string } {
	end := now.Add(-5 * time.Minute)
	ns := func(t time.Time) string { return strconv.FormatInt(t.UnixNano(), 10) }
	rangeQuery := func(query, step string) string {
		return "/loki/api/v1/query_range?" + url.Values{"query": {query}, "start": {ns(end.Add(-time.Hour))}, "end": {ns(end)}, "step": {step}}.Encode()
	}
	instant := func(query string, at time.Time) string {
		v := url.Values{"query": {query}}
		if !at.IsZero() {
			v.Set("time", ns(at))
		}
		return "/loki/api/v1/query?" + v.Encode()
	}
	const (
		bareParser = `count_over_time({service_name="api-gateway"} | json [3m])`
		quantile   = `quantile_over_time(2.0, {app="api-gateway"} | json | unwrap duration_ms [5m])`
	)
	return []struct{ name, target string }{
		{"limits/drilldown-breakdown-keeps-busiest", rangeQuery(`sum by (pod) (count_over_time({env="production"}[5m]))`, "60")},
		{"parsed-identity/bare-parser-metric-duplicates-extracted", instant(bareParser, time.Time{})},
		{"parsed-identity/bare-parser-metric-duplicates-extracted/pinned", instant(bareParser, now.Add(-30*time.Second))},
		{"semantics/keep-before-unwrap-identity-in-operands", instant(`label_replace(first_over_time({app="api-gateway"} | json | keep app, duration_ms | unwrap duration_ms [5m]), "svc", "$1", "app", "(.*)")`, end.Add(30*time.Second))},
		{"semantics/label-filter-before-parser-reads-ingest-json-key", rangeQuery(`sum by (trace_id) (count_over_time({env="production"} | trace_id!="" [20s]))`, "20s")},
		{"semantics/quantile-over-time-out-of-range", instant(quantile, time.Time{})},
		{"semantics/quantile-over-time-out-of-range/pinned", instant(quantile, now.Add(-30*time.Second))},
		{"semantics/unpack-non-string-values", rangeQuery(`count_over_time({service_name="api-gateway"} | unpack | status >= 400 [5m])`, "60")},
		{"parsed-identity/regexp-partial-match-series-count", "/loki/api/v1/query_range?" + url.Values{"query": {`count_over_time({app="api-gateway"} | regexp "(?P<mth>GET|POST|PUT)" [5m])`}, "start": {ns(end.Add(-15 * time.Minute))}, "end": {ns(end)}, "step": {"15s"}}.Encode()},
	}
}

// newOversizedReplyProxy is the stack's proxy shape: an edge cache, a primary
// cache with the stack's 5 s default TTL, the extent cache, no series cap below
// the answers, the near-now refresh rule on (with an hour of staleness, so a
// slow run cannot turn a repeat into a refresh: the tests are about entry
// size), and the per-request call header. edgeBytes and primaryBytes set
// the caches' budgets (an entry may hold a tenth); 0 runs without that cache.
func newOversizedReplyProxy(t *testing.T, backend string, edgeBytes, primaryBytes int) *Proxy {
	t.Helper()
	cfg := Config{BackendURL: backend, LogLevel: "error", MaxStatsQuerySeries: 1_000_000, DebugVLCallsHeader: true,
		RecentTailRefreshEnabled: true, RecentTailRefreshWindow: 2 * time.Minute, RecentTailRefreshMaxStaleness: time.Hour}
	if primaryBytes > 0 {
		c := cache.NewWithMaxBytes(5*time.Second, 100000, primaryBytes)
		t.Cleanup(c.Close)
		cfg.Cache, cfg.QueryRangeExtentCache, cfg.QueryRangeExtentCacheTTL = c, true, time.Hour
	}
	if edgeBytes > 0 {
		edge := cache.NewWithMaxBytes(time.Hour, 100000, edgeBytes)
		t.Cleanup(edge.Close)
		cfg.CompatCache = edge
	}
	p, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	p.storeBackendVersion("v1.50.0", "v1.50.0")
	return p
}

// vlCallsOf is the request's VictoriaLogs calls and cache result, as the
// -debug-vl-calls-header headers report them.
func vlCallsOf(t *testing.T, rec *httptest.ResponseRecorder) (int, string) {
	t.Helper()
	calls, err := strconv.Atoi(rec.Header().Get(vlBackendCallsHeader))
	if err != nil {
		t.Fatalf("no %s header: %q", vlBackendCallsHeader, rec.Header().Get(vlBackendCallsHeader))
	}
	return calls, rec.Header().Get(vlCacheResultHeader)
}

// conformance: limits/drilldown-breakdown-keeps-busiest, parsed-identity/bare-parser-metric-duplicates-extracted, semantics/keep-before-unwrap-identity-in-operands, semantics/label-filter-before-parser-reads-ingest-json-key, semantics/quantile-over-time-out-of-range, semantics/unpack-non-string-values, parsed-identity/regexp-partial-match-series-count, quality/edge-cache-oversized-reply
func TestOversizedReplyRepeatAsksVictoriaLogsNothing(t *testing.T) {
	now := time.Now().Truncate(time.Second)
	rows := oversizedReplyRows(now)
	headers := map[string]string{"X-Scope-OrgID": "0"}
	for _, tc := range oversizedReplyRequests(now) {
		t.Run(tc.name, func(t *testing.T) {
			traffic := &vlTraffic{inner: (&vlemu.Emulator{T: t, Rows: rows, Field: "duration_ms", Version: "v1.50.0"}).Handler()}
			srv := httptest.NewServer(traffic)
			t.Cleanup(srv.Close)
			// An edge entry holds 6.5 KB: every answer here is larger.
			p := newOversizedReplyProxy(t, srv.URL, 64<<10, 256<<20)
			first := doCompatProxyRequest(p, tc.target, headers)
			if first.Code != http.StatusOK || !bytes.Contains(first.Body.Bytes(), []byte(`"metric":{`)) {
				t.Fatalf("cold: status %d, no series, the run proves nothing: %.300s", first.Code, first.Body)
			}
			if limit := p.compatCache.MaxEntrySizeBytes(); first.Body.Len() <= limit {
				t.Fatalf("the answer (%d bytes) fits the edge cache (%d): the case does not exercise the oversized path", first.Body.Len(), limit)
			}
			if calls, result := vlCallsOf(t, first); calls == 0 || result != "miss" {
				t.Fatalf("cold: %d VictoriaLogs calls, cache %q; want a miss that asks VictoriaLogs", calls, result)
			}
			repeat := doCompatProxyRequest(p, tc.target, headers)
			if calls, result := vlCallsOf(t, repeat); calls != 0 || result != "hit" {
				t.Fatalf("repeat: %d VictoriaLogs calls, cache %q; want a hit that asks VictoriaLogs nothing", calls, result)
			}
			if repeat.Code != first.Code || !bytes.Equal(repeat.Body.Bytes(), first.Body.Bytes()) {
				t.Fatalf("repeat answered differently:\nfirst:  %.300s\nrepeat: %.300s", first.Body, repeat.Body)
			}
			if strings.Contains(tc.target, "/query_range?") || strings.Contains(tc.target, "time=") {
				// With its time fixed the request has one answer: the cached
				// reply is the uncached proxy's, byte for byte.
				plain := newOversizedReplyProxy(t, srv.URL, 0, 0)
				want := doCompatProxyRequest(plain, tc.target, headers)
				if want.Code != first.Code || !bytes.Equal(want.Body.Bytes(), first.Body.Bytes()) {
					t.Fatalf("the caches changed the answer:\nwith:    %.300s\nwithout: %.300s", first.Body, want.Body)
				}
			}
		})
	}
}

// A reply over the primary cache's entry bound too is not kept: the bound on
// what one reply may hold in memory stays the cache's own.
// conformance: quality/edge-cache-oversized-reply
func TestOversizedReplyAbovePrimaryEntryIsNotKept(t *testing.T) {
	now := time.Now().Truncate(time.Second)
	traffic := &vlTraffic{inner: (&vlemu.Emulator{T: t, Rows: oversizedReplyRows(now), Field: "duration_ms", Version: "v1.50.0"}).Handler()}
	srv := httptest.NewServer(traffic)
	t.Cleanup(srv.Close)
	// Entries of 6.5 KB in the edge cache and 25.6 KB in the primary cache,
	// for a breakdown answer of about 60 KB.
	p := newOversizedReplyProxy(t, srv.URL, 64<<10, 256<<10)
	target := oversizedReplyRequests(now)[0].target
	headers := map[string]string{"X-Scope-OrgID": "0"}
	first := doCompatProxyRequest(p, target, headers)
	if first.Code != http.StatusOK || first.Body.Len() <= p.cache.MaxEntrySizeBytes() {
		t.Fatalf("cold: status %d, %d bytes; want an answer over the primary entry bound %d", first.Code, first.Body.Len(), p.cache.MaxEntrySizeBytes())
	}
	repeat := doCompatProxyRequest(p, target, headers)
	if _, result := vlCallsOf(t, repeat); result != "miss" || !bytes.Equal(repeat.Body.Bytes(), first.Body.Bytes()) {
		t.Fatalf("repeat: cache %q; want a miss with the same answer", result)
	}
}

// The edge cache keeps a reply it can hold itself, and only there: the
// oversized copy is for what it cannot hold.
// conformance: quality/edge-cache-oversized-reply
func TestStoreCompatOversizedReplyOnlyBeyondEdgeEntry(t *testing.T) {
	primary := cache.NewWithMaxBytes(time.Minute, 1000, 1<<20)
	edge := cache.NewWithMaxBytes(time.Minute, 1000, 10<<10)
	t.Cleanup(primary.Close)
	t.Cleanup(edge.Close)
	p := &Proxy{cache: primary, compatCache: edge}
	for _, tc := range []struct {
		name      string
		sharedKey string
		size      int
		kept      bool
	}{
		{"fits the edge entry", "shared-small", 512, false},
		{"edge entry bound exactly", "shared-bound", edge.MaxEntrySizeBytes() - len("edge"), false},
		{"one byte over the edge entry", "shared-over", edge.MaxEntrySizeBytes() - len("edge") + 1, true},
		{"no shared key (not a query reply)", "", 4 << 10, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p.storeCompatOversizedReply(tc.sharedKey, "edge", bytes.Repeat([]byte("x"), tc.size), time.Minute)
			if tc.sharedKey == "" {
				return
			}
			if _, _, ok := primary.GetWithTTL(tc.sharedKey); ok != tc.kept {
				t.Fatalf("kept=%v, want %v", ok, tc.kept)
			}
		})
	}
	if got := p.compatCaptureLimit(""); got != edge.MaxEntrySizeBytes() {
		t.Fatalf("capture limit without a shared key = %d, want the edge entry %d", got, edge.MaxEntrySizeBytes())
	}
	if got := p.compatCaptureLimit("shared"); got != primary.MaxEntrySizeBytes() {
		t.Fatalf("capture limit with a shared key = %d, want the primary entry %d", got, primary.MaxEntrySizeBytes())
	}
	none := &Proxy{compatCache: edge}
	if got := none.compatCaptureLimit("shared"); got != edge.MaxEntrySizeBytes() {
		t.Fatalf("capture limit without a primary cache = %d", got)
	}
	none.storeCompatOversizedReply("shared", "edge", make([]byte, 4<<10), time.Minute) // no primary cache: nothing to keep, no panic
}

// The kept copy lives for the edge cache's TTL (5 minutes), not the primary
// cache's default (5 s on the e2e stack): a repeat 6 s later is still a hit.
// conformance: quality/edge-cache-oversized-reply
func TestOversizedReplyOutlivesThePrimaryDefaultTTL(t *testing.T) {
	now := time.Now().Truncate(time.Second)
	srv := httptest.NewServer((&vlemu.Emulator{T: t, Rows: oversizedReplyRows(now), Field: "duration_ms", Version: "v1.50.0"}).Handler())
	t.Cleanup(srv.Close)
	p := newOversizedReplyProxy(t, srv.URL, 64<<10, 256<<20)
	target := oversizedReplyRequests(now)[0].target
	headers := map[string]string{"X-Scope-OrgID": "0"}
	first := doCompatProxyRequest(p, target, headers)
	if first.Code != http.StatusOK {
		t.Fatalf("cold: status %d", first.Code)
	}
	restore := cache.AdvanceClockForTesting(6 * time.Second)
	defer restore()
	repeat := doCompatProxyRequest(p, target, headers)
	if calls, result := vlCallsOf(t, repeat); calls != 0 || result != "hit" || !bytes.Equal(repeat.Body.Bytes(), first.Body.Bytes()) {
		t.Fatalf("repeat 6s later: %d VictoriaLogs calls, cache %q", calls, result)
	}
}
