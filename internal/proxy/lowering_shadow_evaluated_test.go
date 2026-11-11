package proxy

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"runtime"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/lower"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/physical"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/testutil/vlemu"
)

// evaluatedShadowBackend is an emulated VictoriaLogs with a few series, recording every request it gets.
func evaluatedShadowBackend(t *testing.T) (*httptest.Server, func() []string) {
	t.Helper()
	var rows []vlemu.Row
	for i := 0; i < 24; i++ {
		rows = append(rows, vlemu.Row{
			TS:     1_705_312_200_000_000_000 + int64(i)*10e9,
			Stream: fmt.Sprintf(`{app=%q,pod="p%d"}`, []string{"api", "web", "db"}[i%3], i%4),
			Value:  float64(i % 5),
			Msg:    fmt.Sprintf("level=info route=/r%d n=%d", i%2, i),
		})
	}
	emu := &vlemu.Emulator{T: t, Rows: rows, Field: "n"}
	var mu sync.Mutex
	var reqs []string
	h := emu.Handler()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		mu.Lock()
		reqs = append(reqs, r.Method+" "+r.URL.Path+" "+r.Form.Encode())
		mu.Unlock()
		h.ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)
	return srv, func() []string {
		mu.Lock()
		defer mu.Unlock()
		out := append([]string(nil), reqs...)
		sort.Strings(out)
		return out
	}
}

// evaluatedShadowProfiles are the label settings the handler tests run under: the Loki-compatible profile (the one
// that reads the tenant's stream label names), underscores alone, and passthrough.
var evaluatedShadowProfiles = []struct {
	name  string
	style LabelStyle
	mode  MetadataFieldMode
}{
	{"loki", LabelStyleUnderscores, MetadataFieldModeTranslated},
	{"underscores", LabelStyleUnderscores, ""},
	{"passthrough", LabelStylePassthrough, ""},
}

func newEvaluatedShadowProxy(t *testing.T, sample float64, backend string, style LabelStyle) *Proxy {
	t.Helper()
	return newEvaluatedShadowProxyMode(t, sample, backend, style, "")
}

func newEvaluatedShadowProxyMode(t *testing.T, sample float64, backend string, style LabelStyle, mode MetadataFieldMode) *Proxy {
	t.Helper()
	p, err := New(Config{
		BackendURL:               backend,
		Cache:                    cache.New(60*time.Second, 1000),
		LogLevel:                 "error",
		LabelStyle:               style,
		MetadataFieldMode:        mode,
		TranslateShadowSample:    sample,
		DisableQueryPlanLowering: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	return p
}

// evaluatedShadowRequest runs a query through the query (instant) or query_range handler and waits for the
// comparisons it started.
func evaluatedShadowRequest(t *testing.T, p *Proxy, q string, ranged bool) string {
	t.Helper()
	v := url.Values{"query": {q}}
	target, handler := "/loki/api/v1/query?", p.handleQuery
	if ranged {
		v.Set("start", "1705312200")
		v.Set("end", "1705312500")
		v.Set("step", "60")
		target, handler = "/loki/api/v1/query_range?", p.handleQueryRange
	} else {
		v.Set("time", "1705312400")
	}
	req := httptest.NewRequest("GET", target+v.Encode(), nil)
	req.Header.Set("X-Scope-OrgID", "0")
	w := httptest.NewRecorder()
	handler(w, req)
	if p.loweringShadow != nil {
		p.loweringShadow.wg.Wait()
	}
	return w.Result().Status + " " + w.Body.String()
}

// evaluatedShadowQueries are proxy-evaluated roots and the outcome the live shadow gives each, on both endpoints.
var evaluatedShadowQueries = []struct{ query, want string }{
	{`topk(2, rate({app="api"}[5m]))`, "native_proxy_evaluated Rank"},
	{`bottomk(1, sum by (app) (count_over_time({app=~".+"}[1m])))`, "native_proxy_evaluated Rank"},
	{`sort_desc(sum by (app) (count_over_time({app=~".+"}[1m])))`, "native_proxy_evaluated Rank"},
	{`stddev by (app) (count_over_time({app=~".+"}[1m]))`, "native_proxy_evaluated VectorAgg stddev"},
	{`stdvar without (pod) (count_over_time({app=~".+"}[1m]))`, "native_proxy_evaluated VectorAgg stdvar"},
	{`sum(topk(2, count_over_time({app=~".+"}[1m])))`, "native_proxy_evaluated VectorAgg"},
	// The route ranks the operand's text, which drops the stddev: the plan holds it.
	{`topk(2, stddev by (app) (count_over_time({app=~".+"}[1m])))`, "known_divergence Rank"},
	// Grouped by a label the parser extracts: the route rewrites the operand as the series-level max by form, cut, and
	// the lowering makes the same rewrite on its typed pipes.
	{`stddev by (route) (count_over_time({app=~".+"} | logfmt [1m]))`, "native_proxy_evaluated VectorAgg stddev"},
	{`stdvar by (route) (rate({app=~".+"} | logfmt [1m]))`, "native_proxy_evaluated VectorAgg stdvar"},
	{`stddev by (app, route) (bytes_over_time({app=~".+"} | json [1m]))`, "native_proxy_evaluated VectorAgg stddev"},
	{`stddev by (route) (count_over_time({app=~".+"} | regexp "route=(?P<route>\\S+)" [1m]))`, "native_proxy_evaluated VectorAgg stddev"},
	{`stdvar(count_over_time({app=~".+"} | pattern "<_> route=<route> <_>" [1m])) by (route)`, "native_proxy_evaluated VectorAgg stdvar"},
	{`stddev by (route) (sum by (route, pod) (rate({app=~".+"} | logfmt [1m])))`, "native_proxy_evaluated VectorAgg stddev"},
	// An unwrapped value under the rewrite stays with the translator.
	{`stddev by (route) (last_over_time({app=~".+"} | logfmt | unwrap n [1m]))`, "not_lowered VectorAgg stddev parsed label operand"},
}

// Both post-aggregation handlers hand the whole query to the live shadow, which holds the lowering to the route.
// conformance: architecture-syntax-package-layering
func TestLoweringShadow_ProxyEvaluatedRootsAreHeldToTheRoute(t *testing.T) {
	for _, prof := range evaluatedShadowProfiles {
		for _, ranged := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/range=%t", prof.name, ranged), func(t *testing.T) {
				srv, _ := evaluatedShadowBackend(t)
				p := newEvaluatedShadowProxyMode(t, 1, srv.URL, prof.style, prof.mode)
				if prof.name == "loki" && !p.lokiCompatibleProfile() {
					t.Fatal("the loki settings are not the Loki-compatible profile")
				}
				for _, tc := range evaluatedShadowQueries {
					before := shadowCounts(t, p)
					evaluatedShadowRequest(t, p, tc.query, ranged)
					after := shadowCounts(t, p)
					if after[tc.want] != before[tc.want]+1 {
						t.Errorf("%s: %q not counted once: before %v after %v", tc.query, tc.want, before, after)
					}
				}
				got := shadowCounts(t, p)
				if n := shadowTotal(got, shadowMismatch) + shadowTotal(got, shadowNewError) + shadowTotal(got, shadowOldError) + shadowTotal(got, shadowPanic); n != 0 {
					t.Fatalf("mismatches or errors: %v", got)
				}
			})
		}
	}
}

// A lowering that differs from the route is a mismatch, counted under the root's construct and logged.
// conformance: architecture-syntax-package-layering
func TestLoweringShadow_ProxyEvaluatedMismatchIsCounted(t *testing.T) {
	srv, _ := evaluatedShadowBackend(t)
	p := newEvaluatedShadowProxy(t, 1, srv.URL, LabelStyleUnderscores)
	p.loweringShadow.lower = func(pl *queryplan.Plan, prof lower.Profile) (physical.Op, error) {
		op, err := lower.Plan(pl, prof)
		if r, ok := op.(physical.RankStep); ok {
			r.K++
			return r, err
		}
		if a, ok := op.(physical.SeriesAgg); ok && a.Op == "stddev" {
			a.Op = "stdvar"
			return a, err
		}
		return op, err
	}
	for _, ranged := range []bool{false, true} {
		evaluatedShadowRequest(t, p, `topk(2, rate({app="api"}[5m]))`, ranged)
		evaluatedShadowRequest(t, p, `stddev by (app) (count_over_time({app=~".+"}[1m]))`, ranged)
	}
	got := shadowCounts(t, p)
	if got["mismatch Rank"] != 2 || got["mismatch VectorAgg stddev"] != 2 || shadowTotal(got, shadowNativeProxyEvaluated) != 0 {
		t.Fatalf("%v", got)
	}
}

// At sample 0 the hook does nothing, and a sample that skips the query compares nothing either.
// conformance: architecture-syntax-package-layering
func TestLoweringShadow_ProxyEvaluatedOffAtSampleZero(t *testing.T) {
	srv, _ := evaluatedShadowBackend(t)
	p := newEvaluatedShadowProxy(t, 0, srv.URL, LabelStyleUnderscores)
	if p.loweringShadow != nil {
		t.Fatal("the shadow is on at sample 0")
	}
	skip := newEvaluatedShadowProxy(t, 0.5, srv.URL, LabelStyleUnderscores)
	skip.loweringShadow.rand = func() float64 { return 0.9 }
	for _, tc := range evaluatedShadowQueries {
		for _, ranged := range []bool{false, true} {
			evaluatedShadowRequest(t, p, tc.query, ranged)
			evaluatedShadowRequest(t, skip, tc.query, ranged)
		}
	}
	for _, px := range []*Proxy{p, skip} {
		if got := shadowCounts(t, px); len(got) != 0 {
			t.Fatalf("shadow work while the sample skips: %v", got)
		}
	}
}

// The hook allocates nothing on a request the sample skips.
// conformance: architecture-syntax-package-layering
func TestLoweringShadow_ProxyEvaluatedSkipAllocatesNothing(t *testing.T) {
	q := `stddev by (app) (count_over_time({app=~".+"}[1m]))`
	postAgg, ok := parseInstantMetricPostAggQuery(q)
	if !ok {
		t.Fatal("not routed")
	}
	off := newEvaluatedShadowProxy(t, 0, "http://unused", LabelStyleUnderscores)
	skip := newEvaluatedShadowProxy(t, 0.5, "http://unused", LabelStyleUnderscores)
	skip.loweringShadow.rand = func() float64 { return 0.9 }
	ctx := context.Background()
	for name, p := range map[string]*Proxy{"off": off, "skipped": skip} {
		if n := testing.AllocsPerRun(1000, func() { p.shadowProxyEvaluated(ctx, q, postAgg, "* | stats count()") }); n != 0 {
			t.Errorf("%s: %v allocations per request", name, n)
		}
	}
}

// Answers and VictoriaLogs requests are the same with the shadow on and off, on every proxy-evaluated route.
// conformance: architecture-syntax-package-layering
func TestLoweringShadow_ProxyEvaluatedChangesNoAnswer(t *testing.T) {
	for _, prof := range evaluatedShadowProfiles {
		style := prof.name
		run := func(sample float64) ([]string, []string, map[string]int) {
			srv, reqs := evaluatedShadowBackend(t)
			p := newEvaluatedShadowProxyMode(t, sample, srv.URL, prof.style, prof.mode)
			var bodies []string
			for pass := 0; pass < 2; pass++ { // the second pass is served from the caches
				for _, tc := range evaluatedShadowQueries {
					for _, ranged := range []bool{false, true} {
						bodies = append(bodies, evaluatedShadowRequest(t, p, tc.query, ranged))
					}
				}
			}
			return bodies, reqs(), shadowCounts(t, p)
		}
		offBodies, offReqs, offCounts := run(0)
		onBodies, onReqs, onCounts := run(1)
		if len(offCounts) != 0 || shadowTotal(onCounts, shadowNativeProxyEvaluated) == 0 {
			t.Fatalf("%s: off %v on %v", style, offCounts, onCounts)
		}
		if len(offReqs) == 0 || strings.Join(offReqs, "\n") != strings.Join(onReqs, "\n") {
			t.Fatalf("%s: VictoriaLogs requests differ:\noff %q\non  %q", style, offReqs, onReqs)
		}
		for i := range offBodies {
			if !sameJSON(stripTimings(offBodies[i]), stripTimings(onBodies[i])) {
				t.Fatalf("%s: response %d differs:\noff %s\non  %s", style, i, offBodies[i], onBodies[i])
			}
		}
	}
}

// A comparison running when the proxy closes ends without counting, a later one is not started, and no goroutine
// is left.
// conformance: architecture-syntax-package-layering
func TestLoweringShadow_ProxyEvaluatedShutdownLeavesNoGoroutines(t *testing.T) {
	srv, _ := evaluatedShadowBackend(t)
	p, err := New(Config{BackendURL: srv.URL, Cache: cache.New(time.Minute, 100), LogLevel: "error", TranslateShadowSample: 1, DisableQueryPlanLowering: true})
	if err != nil {
		t.Fatal(err)
	}
	before := runtime.NumGoroutine()
	hold := make(chan struct{})
	entered := make(chan struct{}, 4)
	p.loweringShadow.lower = func(pl *queryplan.Plan, prof lower.Profile) (physical.Op, error) {
		if _, ok := pl.Root.(*queryplan.Rank); ok {
			entered <- struct{}{}
			<-hold
		}
		return lower.Plan(pl, prof)
	}
	q := `topk(2, rate({app="api"}[5m]))`
	postAgg, _ := parseInstantMetricPostAggQuery(q)
	p.shadowProxyEvaluated(context.Background(), q, postAgg, `* | stats by (_stream) rate() as __lvp_inner`)
	<-entered
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	go func() { time.Sleep(60 * time.Millisecond); close(hold) }()
	_ = p.Shutdown(ctx)
	p.loweringShadow.wg.Wait()
	p.shadowProxyEvaluated(context.Background(), q, postAgg, `* | stats by (_stream) rate() as __lvp_inner`)
	if got := shadowCounts(t, p); len(got) != 0 {
		t.Fatalf("a closed shadow still counts: %v", got)
	}
	deadline := time.Now().Add(2 * time.Second)
	for runtime.NumGoroutine() > before && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if n := runtime.NumGoroutine(); n > before {
		buf := make([]byte, 1<<16)
		t.Fatalf("goroutines %d after Shutdown, %d before:\n%s", n, before, buf[:runtime.Stack(buf, true)])
	}
}

// BenchmarkShadowProxyEvaluatedSkipped is what a post-aggregation request pays for the hook when the sample skips
// it (0 allocations).
func BenchmarkShadowProxyEvaluatedSkipped(b *testing.B) {
	p, err := New(Config{BackendURL: "http://unused", Cache: cache.New(time.Minute, 100), LogLevel: "error", TranslateShadowSample: 0.5, DisableQueryPlanLowering: true})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	p.loweringShadow.rand = func() float64 { return 0.9 }
	q := `stddev by (app) (count_over_time({app=~".+"}[1m]))`
	postAgg, _ := parseInstantMetricPostAggQuery(q)
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p.shadowProxyEvaluated(ctx, q, postAgg, "* | stats count()")
	}
}

// BenchmarkProxyEvaluatedShadowCompare is the cost of one comparison (what a sampled request adds in the
// background).
func BenchmarkProxyEvaluatedShadowCompare(b *testing.B) {
	p, err := New(Config{BackendURL: "http://unused", Cache: cache.New(time.Minute, 100), LogLevel: "error", LabelStyle: LabelStyleUnderscores, TranslateShadowSample: 1, DisableQueryPlanLowering: true})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	q := `stddev by (app) (count_over_time({app=~".+"}[1m]))`
	postAgg, _ := parseInstantMetricPostAggQuery(q)
	operand, err := p.translatePostAggOperand(context.Background(), postAgg)
	if err != nil {
		b.Fatal(err)
	}
	p.loweringShadow.wg.Wait()
	in := evaluatedShadowInput{ctx: context.Background(), query: q, postAgg: postAgg, operand: operand}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p.runProxyEvaluatedShadow(p.loweringShadow, in)
	}
}

// A comparison that panics is recovered and counted under the root's construct; the answer is unchanged.
// conformance: architecture-syntax-package-layering
func TestLoweringShadow_ProxyEvaluatedPanicIsRecovered(t *testing.T) {
	srv, _ := evaluatedShadowBackend(t)
	p := newEvaluatedShadowProxy(t, 1, srv.URL, LabelStyleUnderscores)
	off := newEvaluatedShadowProxy(t, 0, srv.URL, LabelStyleUnderscores)
	p.loweringShadow.lower = func(pl *queryplan.Plan, prof lower.Profile) (physical.Op, error) {
		if _, ok := pl.Root.(*queryplan.Rank); ok {
			panic("boom")
		}
		return lower.Plan(pl, prof)
	}
	q := `topk(2, rate({app="api"}[5m]))`
	if got, want := evaluatedShadowRequest(t, p, q, false), evaluatedShadowRequest(t, off, q, false); !sameJSON(stripTimings(got), stripTimings(want)) {
		t.Fatalf("answer changed:\n%s\n%s", got, want)
	}
	if got := shadowCounts(t, p); got["panic Rank"] != 1 {
		t.Fatalf("%v", got)
	}
}

// The comparison reads the tenant's stream label names as last known: it starts no refresh (a request to
// VictoriaLogs) and records nothing, even when the names are due for one, and the profile of the whole query gets
// the names the operand's translation reads (here a parsed name that is also a stream label).
// conformance: architecture-syntax-package-layering
func TestLoweringShadow_ProxyEvaluatedReadsStreamLabelsWithoutRefresh(t *testing.T) {
	p := newEvaluatedShadowProxyMode(t, 1, "http://unused", LabelStyleUnderscores, MetadataFieldModeTranslated)
	t.Cleanup(p.stopStreamLabelNames)
	if !p.lokiCompatibleProfile() {
		t.Fatal("not the Loki-compatible profile")
	}
	req := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query", nil)
	req.Header.Set("X-Scope-OrgID", "t1")
	req = p.withRequestScope(req)
	ctx := context.WithValue(req.Context(), origRequestKey, req)
	st := &p.streamLabelNames
	st.mu.Lock()
	st.byKey = map[string]*streamLabelNamesEntry{"slnames:" + getOrgID(ctx): {names: []string{"route"}}} // never fresh: due
	st.mu.Unlock()
	q := `stddev by (route) (count_over_time({app="a"} | logfmt [5m]))`
	postAgg, ok := parseInstantMetricPostAggQuery(q)
	if !ok {
		t.Fatal("not routed")
	}
	text, rewritten := postAggTranslatedText(postAgg)
	if !rewritten {
		t.Fatal("the route does not rewrite a stddev by a parsed label")
	}
	got := p.plainStreamLabelNamesFrom(ctx, text, p.knownTenantStreamLabelNames)
	st.mu.Lock()
	refreshes := st.refreshes
	st.mu.Unlock()
	if strings.Join(got, ",") != "route" || refreshes != 0 {
		t.Fatalf("names %v, refreshes %d: want [route] and none", got, refreshes)
	}
	prof := p.evaluatedShadowProfile(evaluatedShadowInput{ctx: ctx, query: q, postAgg: postAgg})
	// The label function carries the names it reads as stream labels behind a probe name (translator.WithStreamLabels).
	if probe := "\x03"; prof.Label(probe) != probe+"=route" {
		t.Fatalf("the profile does not carry route as a stream label: %q", prof.Label(probe))
	}
	st.mu.Lock()
	refreshes = st.refreshes
	st.mu.Unlock()
	if refreshes != 0 {
		t.Fatalf("the shadow started %d refreshes", refreshes)
	}
	// The translation's own reader does start one: the entry is due.
	p.tenantStreamLabelNames(ctx)
	st.mu.Lock()
	refreshes = st.refreshes
	st.mu.Unlock()
	if refreshes != 1 {
		t.Fatalf("the translation's reader started %d refreshes, want 1 (the entry is due)", refreshes)
	}
}
