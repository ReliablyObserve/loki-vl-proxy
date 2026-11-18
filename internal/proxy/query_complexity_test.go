package proxy

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/lokierr"
)

// syncBuffer is a log sink that handlers on several goroutines may write.
type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

// emptyValidationCaches starts a test from empty process-wide validation memos. They stop taking entries when they are
// full, which a long run of other tests leaves them, and the tests that read what a request stores (or that the gate
// runs at all) need room.
func emptyValidationCaches(t *testing.T) {
	t.Helper()
	validationCache.Range(func(k, _ any) bool { validationCache.Delete(k); return true })
	validationCacheSize.Store(0)
	requestTextCache.Range(func(k, _ any) bool { requestTextCache.Delete(k); return true })
	requestTextCacheSize.Store(0)
}

// complexityRig is a proxy behind the shipped middleware chain in front of a VictoriaLogs stand-in that answers every
// call empty and counts them; its request error log lines go to logs.
type complexityRig struct {
	p       *Proxy
	base    string
	backend *errLayerBackend
	logs    *syncBuffer
}

func newComplexityRig(t *testing.T, limits ExecutionLimitsConfig, tune func(*Proxy)) *complexityRig {
	return newComplexityRigRoutes(t, limits, tune, nil)
}

// newComplexityRigRoutes is newComplexityRig with the routes of routes (nil: every route of the proxy) on the
// proxy's own middleware chain.
func newComplexityRigRoutes(t *testing.T, limits ExecutionLimitsConfig, tune func(*Proxy), routes func(*Proxy, *http.ServeMux)) *complexityRig {
	t.Helper()
	backend := &errLayerBackend{failSig: "\x00none", okBody: "empty"}
	vl := httptest.NewServer(backend)
	t.Cleanup(vl.Close)
	c := cache.New(time.Minute, 1000)
	t.Cleanup(c.Close)
	p, err := New(Config{BackendURL: vl.URL, Cache: c, LogLevel: "error", BackendTimeout: 5 * time.Second, ExecutionLimits: limits})
	if err != nil {
		t.Fatalf("new proxy: %v", err)
	}
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	logs := &syncBuffer{}
	p.log = slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	if tune != nil {
		tune(p)
	}
	mux := http.NewServeMux()
	if routes != nil {
		routes(p, mux)
	} else {
		p.RegisterRoutes(mux)
	}
	front := httptest.NewServer(shippedChain(mux))
	t.Cleanup(front.Close)
	return &complexityRig{p: p, base: front.URL, backend: backend, logs: logs}
}

func (r *complexityRig) get(t *testing.T, path string, params url.Values) (int, string) {
	t.Helper()
	resp, err := http.Get(r.base + path + "?" + params.Encode())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(body)
}

// rangeQuery is a query_range request of query over the last five minutes.
func rangeParams(query string) url.Values {
	now := time.Now().Unix()
	return url.Values{"query": {query}, "start": {strconv.FormatInt(now-300, 10)}, "end": {strconv.FormatInt(now, 10)}, "step": {"15"}, "limit": {"10"}}
}

func (r *complexityRig) metricsText() string {
	rec := httptest.NewRecorder()
	r.p.metrics.Handler(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	return rec.Body.String()
}

// stagesQuery is a log query of k json stages: k+2 nodes and one level.
func stagesQuery(k int) string { return `{a="b"}` + strings.Repeat(" | json", k) }

// depthQuery is a metric query n levels deep with n+1 nodes.
func depthQuery(n int) string {
	return strings.Repeat("sum(", n-2) + `rate({a="b"}[1m])` + strings.Repeat(")", n-2)
}

// A query over an operator's node or depth limit is refused with a neutral 400 before any VictoriaLogs call, the flag
// and value in the log line only and the refusal counted; a query at the limit and under it is answered as before,
// on every route that reads a query.
//
// conformance: query-complexity-limits
func TestQueryComplexityLimitsRejectOnlyWhenLowered(t *testing.T) {
	rig := newComplexityRig(t, ExecutionLimitsConfig{MaxQueryASTNodes: 10, MaxQueryNestingDepth: 3}, nil)
	for _, tc := range []struct {
		name, query string
		want        string // "" = answered normally
	}{
		{"nodes under", stagesQuery(7), ""},
		{"nodes at the limit", stagesQuery(8), ""},
		{"nodes over", stagesQuery(9), "query too complex: too many nodes"},
		{"depth at the limit", depthQuery(3), ""},
		{"depth over", depthQuery(4), "query too complex: nesting too deep"},
		{"parentheses add no level", "((((" + depthQuery(3) + "))))", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			hits := rig.backend.hits.Load()
			status, body := rig.get(t, "/loki/api/v1/query_range", rangeParams(tc.query))
			if tc.want == "" {
				if status != http.StatusOK {
					t.Fatalf("answered %d %q, want 200", status, body)
				}
				return
			}
			if status != http.StatusBadRequest || !strings.Contains(body, tc.want) {
				t.Fatalf("answered %d %q, want 400 %q", status, body, tc.want)
			}
			if strings.ContainsAny(body, "0123456789") {
				t.Errorf("the body carries a number: %q", body)
			}
			if strings.Contains(body, "-max-query") || strings.Contains(body, "max-query") {
				t.Errorf("the body names the flag: %q", body)
			}
			if got := rig.backend.hits.Load(); got != hits {
				t.Errorf("%d VictoriaLogs calls for a refused query", got-hits)
			}
		})
	}
	logs := rig.logs.String()
	for _, want := range []string{"limit_flag=-max-query-ast-nodes", "limit_value=10", "limit_flag=-max-query-nesting-depth", "limit_value=3"} {
		if !strings.Contains(logs, want) {
			t.Errorf("the request error log lacks %q:\n%s", want, logs)
		}
	}
	m := rig.metricsText()
	for _, want := range []string{`loki_vl_proxy_query_complexity_rejections_total{limit="ast_nodes"} 1`, `loki_vl_proxy_query_complexity_rejections_total{limit="nesting_depth"} 1`, `loki_vl_proxy_query_complexity_rejections_total{limit="plan_time"} 0`} {
		if !strings.Contains(m, want) {
			t.Errorf("/metrics lacks %q", want)
		}
	}

	// The other routes that read a query or a selector hold the same limits (three matchers are four nodes).
	routes := []struct {
		path string
		key  string
	}{
		{"/loki/api/v1/labels", "query"},
		{"/loki/api/v1/series", "match[]"},
		{"/loki/api/v1/index/stats", "query"},
		{"/loki/api/v1/detected_fields", "query"},
		{"/loki/api/v1/format_query", "query"},
	}
	tight := newComplexityRig(t, ExecutionLimitsConfig{MaxQueryASTNodes: 3}, nil)
	for _, rt := range routes {
		now := time.Now().Unix()
		window := url.Values{"start": {strconv.FormatInt(now-300, 10)}, "end": {strconv.FormatInt(now, 10)}}
		window.Set(rt.key, `{a="b",c="d"}`)
		if status, body := tight.get(t, rt.path, window); status != http.StatusOK {
			t.Errorf("%s: two matchers (three nodes) answered %d %q", rt.path, status, body)
		}
		window.Set(rt.key, `{a="b",c="d",e="f"}`)
		if status, body := tight.get(t, rt.path, window); status != http.StatusBadRequest || !strings.Contains(body, "query too complex: too many nodes") || strings.ContainsAny(body, "0123456789") {
			t.Errorf("%s: three matchers answered %d %q", rt.path, status, body)
		}
	}
	// A query the validation rejects is answered by that rejection, never by a limit.
	if status, body := tight.get(t, "/loki/api/v1/query_range", rangeParams(stagesQuery(5)+" |= ")); status != http.StatusBadRequest || strings.Contains(body, "too complex") {
		t.Errorf("an invalid query answered %d %q", status, body)
	}
}

// The derived defaults are the query length limit in effect, which no query that passed it can exceed: a query is
// never refused by them, whatever the length limit is, and they are walked for nothing.
//
// conformance: query-complexity-limits
func TestQueryComplexityDerivedDefaultsNeverTrip(t *testing.T) {
	limits := resolveExecutionLimits(ExecutionLimitsConfig{})
	if limits.QueryASTNodes != DefaultMaxQueryLengthBytes || limits.QueryNestingDepth != DefaultMaxQueryLengthBytes {
		t.Fatalf("defaults %d nodes %d levels, want the query length limit %d", limits.QueryASTNodes, limits.QueryNestingDepth, DefaultMaxQueryLengthBytes)
	}
	if got := resolveExecutionLimits(ExecutionLimitsConfig{MaxQueryLengthBytes: 500}); got.QueryASTNodes != 500 || got.QueryNestingDepth != 500 {
		t.Errorf("a lowered length limit derives %d and %d, want 500", got.QueryASTNodes, got.QueryNestingDepth)
	}
	if got := resolveExecutionLimits(ExecutionLimitsConfig{MaxQueryLengthBytes: 500, MaxQueryASTNodes: 20, MaxQueryNestingDepth: 4}); got.QueryASTNodes != 20 || got.QueryNestingDepth != 4 {
		t.Errorf("explicit limits are %d and %d, want 20 and 4", got.QueryASTNodes, got.QueryNestingDepth)
	}
	shapes := map[string]string{
		"parentheses": strings.Repeat("(", 6000) + "1" + strings.Repeat(")", 6000),
		"sums":        depthQuery(2000),
		"binary":      strings.Repeat("1+", 6000) + "1",
		"stages":      stagesQuery(3000),
	}
	for name, q := range shapes {
		// The length limit set to the query's own length is the tightest the defaults can be derived from.
		rig := newComplexityRig(t, ExecutionLimitsConfig{MaxQueryLengthBytes: len(q)}, nil)
		if err := rig.p.validQueryComplexityError(q); err != nil {
			t.Errorf("%s: refused with the limits derived from its own length: %v", name, err)
		}
		if f, ok := queryComplexityFacts(q); !ok || f.nodes > len(q) || f.depth > len(q) {
			t.Errorf("%s: facts %+v for %d bytes", name, f, len(q))
		}
		// And a query shorter than both limits is not walked at all: no facts are made for it.
		short := newTestProxy(t, "http://unused")
		before := validationCacheSize.Load()
		if err := short.queryComplexityError(q + " "); err != nil {
			t.Errorf("%s: refused at the default limits: %v", name, err)
		}
		if validationCacheSize.Load() != before {
			t.Errorf("%s: the default limits walked the query", name)
		}
	}
}

// Two proxies in one process hold their own limits: the same query is answered differently and independently, in
// either order, and the shared validation cache holds only facts about the query.
//
// conformance: query-complexity-limits
func TestQueryComplexityLimitsArePerProxy(t *testing.T) {
	emptyValidationCaches(t)
	tight := newComplexityRig(t, ExecutionLimitsConfig{MaxQueryASTNodes: 5}, nil)
	loose := newComplexityRig(t, ExecutionLimitsConfig{}, nil)
	q := stagesQuery(6)
	for round := 0; round < 2; round++ {
		for _, step := range []struct {
			name string
			rig  *complexityRig
			want int
		}{{"loose first", loose, http.StatusOK}, {"tight", tight, http.StatusBadRequest}, {"loose again", loose, http.StatusOK}} {
			if status, body := step.rig.get(t, "/loki/api/v1/query_range", rangeParams(q)); status != step.want {
				t.Fatalf("round %d %s: answered %d %q, want %d", round, step.name, status, body, step.want)
			}
		}
	}
	if _, stored := validationCache.Load(complexityKey(q)); !stored {
		t.Fatal("the facts of the query are not cached")
	}
	validationCache.Range(func(k, v any) bool {
		if _, isErr := v.(error); isErr {
			t.Errorf("%v: a verdict is cached: %v", k, v)
		}
		return true
	})
}

func TestQueryComplexityErrorsAreTypedLimits(t *testing.T) {
	p := newComplexityRig(t, ExecutionLimitsConfig{MaxQueryASTNodes: 2, MaxQueryNestingDepth: 1}, nil).p
	err := p.queryComplexityError(depthQuery(3))
	if err == nil || err.Msg() != lokierr.MsgQueryComplexity || err.Kind() != lokierr.KindLimit || err.Status() != http.StatusBadRequest {
		t.Fatalf("error %#v", err)
	}
	if flag, value, ok := err.Limit(); !ok || flag != flagMaxQueryASTNodes || value != 2 {
		t.Errorf("limit %q %d %v", flag, value, ok)
	}
	if err.Text() != "query too complex: too many nodes" || !strings.Contains(err.Cause().Error(), "4 nodes, over the limit of 2") {
		t.Errorf("text %q", err.Text())
	}
	p = newComplexityRig(t, ExecutionLimitsConfig{MaxQueryNestingDepth: 1}, nil).p
	err = p.queryComplexityError(depthQuery(3))
	if err == nil || err.Text() != "query too complex: nesting too deep" || !strings.Contains(err.Cause().Error(), "3 levels deep, over the limit of 1") {
		t.Fatalf("depth error %v", err)
	}
	if p.queryComplexityError(`not logql {`) != nil || p.validQueryComplexityError(`not logql {`) != nil {
		t.Error("a query that does not parse is answered by the validation, not by a limit")
	}
}

// The operator's size limits answer ahead of the response cache on the expression routes: a response cached under
// other limits (a peer at the defaults, an entry written before a reload) never replaces the refusal.
//
// conformance: query-complexity-limits
func TestQueryComplexityLimitsAnswerAheadOfTheResponseCache(t *testing.T) {
	c := cache.New(time.Minute, 1000)
	t.Cleanup(c.Close)
	build := func(limits ExecutionLimitsConfig) *complexityRig {
		backend := &errLayerBackend{failSig: "\x00none", okBody: "empty"}
		vl := httptest.NewServer(backend)
		t.Cleanup(vl.Close)
		p, err := New(Config{BackendURL: vl.URL, Cache: c, CompatCache: c, LogLevel: "error", BackendTimeout: 5 * time.Second, ExecutionLimits: limits})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
		mux := http.NewServeMux()
		p.RegisterRoutes(mux)
		front := httptest.NewServer(shippedChain(mux))
		t.Cleanup(front.Close)
		return &complexityRig{p: p, base: front.URL, backend: backend, logs: &syncBuffer{}}
	}
	loose, tight := build(ExecutionLimitsConfig{}), build(ExecutionLimitsConfig{MaxQueryASTNodes: 5})
	for _, route := range []string{"/loki/api/v1/query_range", "/loki/api/v1/query"} {
		query := stagesQuery(6)
		params := rangeParams(query)
		if route == "/loki/api/v1/query" {
			query = `sum(count_over_time({a="b"} | json | json | json | json [1m]))`
			params = url.Values{"query": {query}, "time": {strconv.FormatInt(time.Now().Unix(), 10)}}
		}
		if status, body := loose.get(t, route, params); status != http.StatusOK {
			t.Fatalf("%s: the default-limit proxy answered %d %q", route, status, body)
		}
		hits := loose.backend.hits.Load()
		if status, _ := loose.get(t, route, params); status != http.StatusOK || loose.backend.hits.Load() != hits {
			t.Fatalf("%s: the repeat was not answered from the cache (status %d, %d new calls)", route, status, loose.backend.hits.Load()-hits)
		}
		if status, body := tight.get(t, route, params); status != http.StatusBadRequest || !strings.Contains(body, "query too complex: too many nodes") {
			t.Errorf("%s: a proxy with a lowered limit answered %d %q from the cache the other one filled", route, status, body)
		}
	}
}
