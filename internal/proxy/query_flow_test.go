package proxy

import (
	"bytes"
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
	logqlpkg "github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
)

func flowCtx() (context.Context, *requestTelemetry) {
	rt := newRequestTelemetry()
	return context.WithValue(context.Background(), requestTelemetryKey, rt), rt
}

func flowOf(ctx context.Context) flowCounters { return snapshotTelemetry(ctx).flow }

// Flow counters are summed under the request's lock: calls from parallel goroutines (a fan-out) add up exactly.
//
// conformance: observability-query-flow
func TestRequestFlowAccumulatesFromParallelCalls(t *testing.T) {
	p := newTestProxy(t, "http://unused")
	p.cache.SetWithTTL("k", []byte("12345"), time.Minute)
	ctx, _ := flowCtx()
	const workers, each = 8, 50
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < each; i++ {
				if _, err := parseLogQLExpr(ctx, `rate({app="api"}[1m])`); err != nil {
					t.Error(err)
				}
				p.cacheGet(ctx, p.cache, "k")
				p.cacheGet(ctx, p.cache, "missing")
				recordFlowVL(ctx, time.Millisecond, 10)
				p.observeInternalOperation(ctx, "translate_query", "translated", time.Millisecond)
			}
		}()
	}
	wg.Wait()
	f := flowOf(ctx)
	n := workers * each
	if f.parseCount != n || f.translateCount != n || f.vlCalls != n || f.vlBytes != int64(10*n) || f.vlDur != time.Duration(n)*time.Millisecond {
		t.Fatalf("flow %+v", f)
	}
	if f.cacheLookups() != 2*n || f.cacheHits() != n || f.cacheMisses() != n || f.cacheStale() != 0 || f.cacheBytes != int64(5*n) {
		t.Fatalf("cache flow %+v", f)
	}
	if f.cache[flowTierL1].hits != n || f.cache[flowTierUnknown].misses != n {
		t.Fatalf("by tier %+v", f.cache)
	}
	// The request's own time is what VictoriaLogs and the cache do not account for, never negative.
	if got := f.proxyDur(time.Millisecond); got != 0 {
		t.Fatalf("proxyDur with more call time than request time = %v", got)
	}
	if got := f.proxyDur(time.Hour); got <= 0 || got >= time.Hour {
		t.Fatalf("proxyDur = %v", got)
	}
}

// Each cache read returns what the cache returns and records its tier, result and bytes: the full-stack read
// names the tier that answered (unknown on a miss), a memory read is l1_memory, a stale read counts as stale.
//
// conformance: observability-query-flow
func TestCacheReadsKeepTheirResultsAndRecordTierResultAndBytes(t *testing.T) {
	p := newTestProxy(t, "http://unused")
	p.cache.SetWithTTL("fresh", []byte("abc"), time.Minute)
	p.cache.SetWithTTL("expired", []byte("abcdef"), time.Millisecond)
	time.Sleep(5 * time.Millisecond)
	ctx, _ := flowCtx()

	for _, c := range []struct {
		name string
		read func() (got, want string, ok, wantOK bool)
	}{
		{"Get", func() (string, string, bool, bool) {
			v, ok := p.cacheGet(ctx, p.cache, "fresh")
			w, wok := p.cache.Get("fresh")
			return string(v), string(w), ok, wok
		}},
		{"GetWithTTL", func() (string, string, bool, bool) {
			v, _, ok := p.cacheGetWithTTL(ctx, p.cache, "fresh")
			w, _, wok := p.cache.GetWithTTL("fresh")
			return string(v), string(w), ok, wok
		}},
		{"GetWithAge", func() (string, string, bool, bool) {
			v, _, _, ok := p.cacheGetWithAge(ctx, p.cache, "fresh")
			w, _, _, wok := p.cache.GetWithAge("fresh")
			return string(v), string(w), ok, wok
		}},
		{"GetShared", func() (string, string, bool, bool) {
			v, _, _, ok := p.cacheGetShared(ctx, p.cache, "fresh")
			w, _, _, wok := p.cache.GetSharedWithTTL("fresh")
			return string(v), string(w), ok, wok
		}},
		{"GetSharedWithAge", func() (string, string, bool, bool) {
			v, _, _, _, ok := p.cacheGetSharedWithAge(ctx, p.cache, "fresh")
			w, _, _, _, wok := p.cache.GetSharedWithAge("fresh")
			return string(v), string(w), ok, wok
		}},
	} {
		got, want, ok, wantOK := c.read()
		if got != want || ok != wantOK || !ok {
			t.Fatalf("%s: got %q/%v, cache returns %q/%v", c.name, got, ok, want, wantOK)
		}
	}
	f := flowOf(ctx)
	if f.cacheHits() != 5 || f.cacheBytes != 15 || f.cache[flowTierL1].hits != 5 {
		t.Fatalf("five memory hits of 3 bytes: %+v", f)
	}

	if _, ok := p.cacheGet(ctx, p.cache, "absent"); ok {
		t.Fatal("absent key found")
	}
	if _, _, ok := p.cacheGetWithTTL(ctx, p.cache, "absent"); ok {
		t.Fatal("absent key found")
	}
	v, _, ok := p.cacheGetStale(ctx, p.cache, "expired")
	rv, _, rtier, rok := p.cacheGetRecoverableStale(ctx, p.cache, "expired", nil)
	if !ok || string(v) != "abcdef" || !rok || rtier != "l1_memory" || string(rv) != "abcdef" {
		t.Fatalf("stale reads %q %v / %q %q %v", v, ok, rv, rtier, rok)
	}
	f = flowOf(ctx)
	if f.cacheMisses() != 2 || f.cache[flowTierUnknown].misses != 1 || f.cache[flowTierL1].misses != 1 || f.cacheStale() != 2 || f.cacheBytes != 15+12 {
		t.Fatalf("misses, stale and bytes: %+v", f)
	}
	body := metricsBody(p)
	for _, want := range []string{
		`loki_vl_proxy_cache_lookup_seconds_count{tier="l1_memory",result="hit"} 5`,
		`loki_vl_proxy_cache_lookup_seconds_count{tier="unknown",result="miss"} 1`,
		`loki_vl_proxy_cache_lookup_seconds_count{tier="l1_memory",result="stale"} 2`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("metrics lack %s\n%s", want, body)
		}
	}
	// A lookup without a request (a background build) is still a metric and breaks nothing.
	p.cacheGet(context.Background(), p.cache, "fresh")
	var nilCache *cache.Cache
	if _, ok := p.cacheGet(ctx, nilCache, "fresh"); ok {
		t.Fatal("a nil cache has no entries")
	}
}

// The parse helpers return the parser's own result and error; a parse made with a request's context is counted
// and timed in its flow, one made without is not.
//
// conformance: observability-query-flow
func TestParseHelpersMatchTheParserAndCountInTheRequest(t *testing.T) {
	ctx, _ := flowCtx()
	for _, q := range []string{`{app="api"} |= "x"`, `sum by (a) (rate({app="api"}[5m]))`, `{app=`, `rate(`, ``} {
		wantExpr, wantErr := logqlpkg.Parse(q)
		gotExpr, gotErr := parseLogQLExpr(ctx, q)
		if (wantErr == nil) != (gotErr == nil) || (wantErr != nil && wantErr.Error() != gotErr.Error()) || (wantErr == nil && wantExpr.String() != gotExpr.String()) {
			t.Fatalf("Parse(%q): %v %v vs %v %v", q, gotExpr, gotErr, wantExpr, wantErr)
		}
		_, wantQErr := logqlpkg.ParseLogQuery(q)
		_, gotQErr := parseLogQLQuery(ctx, q)
		if (wantQErr == nil) != (gotQErr == nil) || (wantQErr != nil && wantQErr.Error() != gotQErr.Error()) {
			t.Fatalf("ParseLogQuery(%q): %v vs %v", q, gotQErr, wantQErr)
		}
		wantVErr := logqlpkg.ParseAndValidate(q)
		gotVErr := validateLogQL(ctx, q)
		if (wantVErr == nil) != (gotVErr == nil) || (wantVErr != nil && wantVErr.Error() != gotVErr.Error()) {
			t.Fatalf("ParseAndValidate(%q): %v vs %v", q, gotVErr, wantVErr)
		}
	}
	if f := flowOf(ctx); f.parseCount != 15 || f.parseDur <= 0 {
		t.Fatalf("15 parses in the request, got %+v", f)
	}
	if _, err := parseLogQLExpr(noFlowCtx, `{app="api"}`); err != nil {
		t.Fatal(err)
	}
	if f := flowOf(ctx); f.parseCount != 15 {
		t.Fatalf("a parse without the request's context was counted: %+v", f)
	}
}

func flowLogProxy(t *testing.T, vlURL string) (*Proxy, *http.ServeMux, *bytes.Buffer) {
	t.Helper()
	p, mux := newBehaviorProxy(t, vlURL, 0)
	var buf bytes.Buffer
	p.log = slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo}))
	return p, mux, &buf
}

// The request line carries what the request caused: a first labels read misses the cache and asks VictoriaLogs;
// the repeat is answered from the cache with no VictoriaLogs call. Both feed the per-route phase and fan-out
// histograms.
//
// conformance: observability-query-flow, observability-slow-query-log
func TestRequestLogFlowOnACacheMissAndACacheHit(t *testing.T) {
	srv, calls := fieldNamesServer(t, []string{"app", "env"})
	p, mux, buf := flowLogProxy(t, srv.URL)
	getLabels(t, mux, time.Hour)
	getLabels(t, mux, time.Hour)
	reqs := slowLogLines(t, buf, "request")
	if len(reqs) != 2 {
		t.Fatalf("request lines %d\n%s", len(reqs), buf)
	}
	miss, hit := reqs[0], reqs[1]
	num := func(m map[string]any, k string) float64 {
		v, ok := m[k].(float64)
		if !ok {
			t.Fatalf("%s missing or not a number in %v", k, m)
		}
		return v
	}
	if num(miss, "flow.vl.calls") < 1 || num(miss, "flow.vl.bytes") <= 0 || num(miss, "flow.cache.misses") < 1 || num(miss, "flow.cache.lookups") < 1 {
		t.Fatalf("a cache miss asks VictoriaLogs: %v", miss)
	}
	if num(hit, "flow.vl.calls") != 0 || num(hit, "flow.vl.bytes") != 0 || num(hit, "flow.cache.hits") < 1 || num(hit, "flow.cache.bytes") <= 0 || num(hit, "flow.cache.misses") != 0 {
		t.Fatalf("a cache hit sends no VictoriaLogs call: %v", hit)
	}
	byTier, _ := hit["flow.cache.by_tier"].(map[string]any)
	if l1, _ := byTier["l1_memory"].(map[string]any); l1 == nil || l1["hits"].(float64) < 1 {
		t.Fatalf("by_tier %v", byTier)
	}
	for _, k := range []string{"flow.parse.count", "flow.parse.ms", "flow.translate.count", "flow.translate.ms", "flow.cache.stale", "flow.cache.ms", "flow.vl.ms", "flow.proxy.ms"} {
		num(hit, k)
	}
	// The existing fields stay.
	for _, k := range []string{"upstream.calls", "upstream.duration_ms", "cache.result", "proxy.overhead_ms", "proxy.duration_ms"} {
		if _, ok := hit[k]; !ok {
			t.Fatalf("existing field %s lost: %v", k, hit)
		}
	}
	if calls.Load() < 1 {
		t.Fatal("VictoriaLogs never asked")
	}
	body := metricsBody(p)
	for _, want := range []string{
		`loki_vl_proxy_request_fanout_count{route="/loki/api/v1/labels",kind="vl_call"} 2`,
		`loki_vl_proxy_request_fanout_bucket{route="/loki/api/v1/labels",kind="vl_call",le="0"} 1`,
		`loki_vl_proxy_request_fanout_count{route="/loki/api/v1/labels",kind="cache_hit"} 2`,
		`loki_vl_proxy_request_phase_seconds_count{route="/loki/api/v1/labels",phase="proxy"} 2`,
		`loki_vl_proxy_request_phase_seconds_count{route="/loki/api/v1/labels",phase="vl"} 1`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("metrics lack %s", want)
		}
	}
}

// A slow request's line carries the flow and its cache lookups as steps (tier, result, bytes, time) beside the
// VictoriaLogs calls; the cache steps are bounded so they leave room for the calls.
//
// conformance: observability-query-flow, observability-slow-query-log
func TestSlowRequestCarriesTheFlowAndCacheSteps(t *testing.T) {
	p, buf := slowLogProxy(t, 40*time.Millisecond)
	p.cache.SetWithTTL("k", []byte("hello"), time.Minute)
	serveSlowLog(p, "query_range", "/loki/api/v1/query_range", func(w http.ResponseWriter, r *http.Request) {
		p.cacheGet(r.Context(), p.cache, "k")
		p.cacheGet(r.Context(), p.cache, "absent")
		slowLogHandler(p)(w, r)
	})
	slow := slowLogLines(t, buf, "slow_request")
	if len(slow) != 1 {
		t.Fatalf("slow_request lines %d\n%s", len(slow), buf)
	}
	line := slow[0]
	if line["flow.cache.lookups"] != float64(2) || line["flow.cache.hits"] != float64(1) || line["flow.cache.bytes"] != float64(5) || line["flow.vl.calls"] != float64(1) || line["flow.vl.ms"].(float64) < 40 {
		t.Fatalf("slow_request flow %v", line)
	}
	var hit, miss, upstream bool
	for _, s := range line["proxy.steps"].([]any) {
		step := s.(map[string]any)
		switch {
		case step["kind"] == "cache" && step["result"] == "hit":
			hit = step["tier"] == "l1_memory" && step["bytes"] == float64(5) && step["duration_ms"] != nil
		case step["kind"] == "cache" && step["result"] == "miss":
			miss = step["tier"] == "unknown" && step["bytes"] == float64(0)
		case step["kind"] == "upstream":
			upstream = true
		}
	}
	if !hit || !miss || !upstream {
		t.Fatalf("steps lack the cache hit (%v), miss (%v) or the VictoriaLogs call (%v): %v", hit, miss, upstream, line["proxy.steps"])
	}

	// Many lookups (an inventory scan) keep the VictoriaLogs call in the trace: only slowTraceMaxCacheSteps
	// cache steps are kept, the rest are counted as dropped.
	p, buf = slowLogProxy(t, 40*time.Millisecond)
	serveSlowLog(p, "query_range", "/loki/api/v1/query_range", func(w http.ResponseWriter, r *http.Request) {
		for i := 0; i < 3*slowTraceMaxSteps; i++ {
			p.cacheGet(r.Context(), p.cache, "absent")
		}
		slowLogHandler(p)(w, r)
	})
	slow = slowLogLines(t, buf, "slow_request")
	if len(slow) != 1 {
		t.Fatalf("slow_request lines %d", len(slow))
	}
	caches, ups := 0, 0
	for _, s := range slow[0]["proxy.steps"].([]any) {
		switch s.(map[string]any)["kind"] {
		case "cache":
			caches++
		case "upstream":
			ups++
		}
	}
	if caches != slowTraceMaxCacheSteps || ups != 1 || slow[0]["proxy.steps_dropped"] != float64(3*slowTraceMaxSteps-slowTraceMaxCacheSteps) || slow[0]["flow.cache.lookups"] != float64(3*slowTraceMaxSteps) {
		t.Fatalf("cache steps %d, upstream steps %d, dropped %v", caches, ups, slow[0]["proxy.steps_dropped"])
	}
}

// A sampled-out or disabled request log costs the flow nothing but its counters: the histograms still see every
// request, and a request with no VictoriaLogs work records zero calls.
//
// conformance: observability-query-flow
func TestRequestFlowIsMeasuredWhateverTheRequestLogSampling(t *testing.T) {
	p, _ := slowLogProxy(t, 0)
	p.log = slog.New(slog.NewJSONHandler(&bytes.Buffer{}, &slog.HandlerOptions{Level: slog.LevelError}))
	req := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?query=x", nil)
	p.requestLogger("query_range", "/loki/api/v1/query_range", func(w http.ResponseWriter, r *http.Request) {
		p.cacheGet(r.Context(), p.cache, "absent")
		w.WriteHeader(http.StatusOK)
	}).ServeHTTP(httptest.NewRecorder(), req)
	body := metricsBody(p)
	for _, want := range []string{
		`loki_vl_proxy_request_fanout_count{route="/loki/api/v1/query_range",kind="cache_lookup"} 1`,
		`loki_vl_proxy_request_fanout_bucket{route="/loki/api/v1/query_range",kind="vl_call",le="0"} 1`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("metrics lack %s", want)
		}
	}
}
