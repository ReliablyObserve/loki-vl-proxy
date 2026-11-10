package proxy

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/lower"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/physical"
)

// newShadowProxy is a proxy with the lowering shadow on at the given sample.
func newShadowProxy(t *testing.T, sample float64, backend string) *Proxy {
	t.Helper()
	p, err := New(Config{
		BackendURL:            backend,
		Cache:                 cache.New(60*time.Second, 1000),
		LogLevel:              "error",
		LabelStyle:            LabelStyleUnderscores,
		TranslateShadowSample: sample,
		// The shadow compares what the string translator translates; the lowering would serve these queries itself.
		DisableQueryPlanLowering: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	return p
}

var shadowSeriesRE = regexp.MustCompile(`(?m)^loki_vl_proxy_lowering_shadow_total\{result="([^"]+)",construct="([^"]+)"\} (\d+)$`)

// shadowCounts reads loki_vl_proxy_lowering_shadow_total from the proxy's /metrics, "result construct" -> count,
// after the comparisons in flight are done.
func shadowCounts(t *testing.T, p *Proxy) map[string]int {
	t.Helper()
	if p.loweringShadow != nil {
		p.loweringShadow.wg.Wait()
	}
	w := httptest.NewRecorder()
	p.metrics.Handler(w, httptest.NewRequest("GET", "/metrics", nil))
	out := map[string]int{}
	for _, m := range shadowSeriesRE.FindAllStringSubmatch(w.Body.String(), -1) {
		n := 0
		for _, c := range m[3] {
			n = n*10 + int(c-'0')
		}
		out[m[1]+" "+m[2]] = n
	}
	return out
}

// translateAndSettle translates a query and waits for its comparison, so the next query finds a free slot.
func translateAndSettle(t *testing.T, p *Proxy, q string) (string, error) {
	t.Helper()
	out, err := p.translateQuery(q)
	if p.loweringShadow != nil {
		p.loweringShadow.wg.Wait()
	}
	return out, err
}

// shadowTotal sums the counts of a result over all constructs.
func shadowTotal(counts map[string]int, result string) int {
	n := 0
	for k, v := range counts {
		if strings.HasPrefix(k, result+" ") {
			n += v
		}
	}
	return n
}

// conformance: architecture-syntax-package-layering
func TestLoweringShadow_OffByDefault(t *testing.T) {
	p := newTestProxy(t, "http://unused")
	if p.loweringShadow != nil {
		t.Fatal("the shadow is on without -translate-shadow-sample")
	}
	for _, off := range []float64{0, -1} {
		if newLoweringShadow(off) != nil {
			t.Fatalf("sample %v turns the shadow on", off)
		}
	}
	for _, q := range []string{`{app="a"}`, `{app="a"} |= "x"`, `rate({app="a"}[5m])`} {
		if _, err := p.translateQuery(q); err != nil {
			t.Fatal(err)
		}
	}
	// The queries the lowering serves itself are counted as flipped; nothing is compared.
	for k, n := range shadowCounts(t, p) {
		if !strings.HasPrefix(k, shadowFlipped+" ") {
			t.Fatalf("shadow work at sample 0: %s = %d", k, n)
		}
	}
	if on := newLoweringShadow(7); on == nil || on.sample != 1 {
		t.Fatalf("a sample above 1 is clamped to 1: %+v", on)
	}
}

// conformance: architecture-syntax-package-layering
func TestLoweringShadow_EveryMissIsComparedAtSampleOne(t *testing.T) {
	p := newShadowProxy(t, 1, "http://unused")
	queries := []string{`{app="a"}`, `{app="a"} |= "x" != "y"`, `{service_name="a"} |~ "e.r"`}
	for _, q := range queries {
		if _, err := translateAndSettle(t, p, q); err != nil {
			t.Fatal(err)
		}
	}
	got := shadowCounts(t, p)
	if got["match LogSelect"] != len(queries) || shadowTotal(got, "mismatch") != 0 {
		t.Fatalf("each translated query is compared and matches: %v", got)
	}
}

// conformance: architecture-syntax-package-layering
func TestLoweringShadow_CacheHitIsNotShadowed(t *testing.T) {
	p := newShadowProxy(t, 1, "http://unused")
	for i := 0; i < 4; i++ {
		if _, err := translateAndSettle(t, p, `{app="a"}`); err != nil {
			t.Fatal(err)
		}
	}
	if got := shadowCounts(t, p); got["match LogSelect"] != 1 || shadowTotal(got, "skipped") != 0 {
		t.Fatalf("one miss, three hits: %v", got)
	}
}

// conformance: architecture-syntax-package-layering
func TestLoweringShadow_SampleShare(t *testing.T) {
	p := newShadowProxy(t, 0.25, "http://unused")
	for _, r := range []float64{0.0, 0.24, 0.25, 0.9} {
		p.loweringShadow.rand = func() float64 { return r }
		if _, err := translateAndSettle(t, p, `{app="a_`+string(rune('a'+int(r*100)%26))+`"}`); err != nil {
			t.Fatal(err)
		}
	}
	if got := shadowCounts(t, p); shadowTotal(got, "match") != 2 {
		t.Fatalf("draws below the sample are compared (0.0, 0.24), the others not: %v", got)
	}
}

// What the shadow sees is what the translator saw: a profile that changes the translation (underscore labels,
// stream fields, keepLine, the detected_level body scan) is compared under that profile, not a default one.
// conformance: architecture-syntax-package-layering
func TestLoweringShadow_ComparesUnderTheRequestsOptions(t *testing.T) {
	p, err := New(Config{
		BackendURL:                   "http://unused",
		Cache:                        cache.New(60*time.Second, 1000),
		LogLevel:                     "error",
		LabelStyle:                   LabelStyleUnderscores,
		StreamFields:                 []string{"app", "namespace", "level"},
		DisableDetectedLevelBodyScan: true,
		TranslateShadowSample:        1,
		DisableQueryPlanLowering:     true,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	var seen lower.Profile
	var mu sync.Mutex
	p.loweringShadow.lower = func(pl *queryplan.Plan, prof lower.Profile) (physical.Op, error) {
		mu.Lock()
		seen = prof
		mu.Unlock()
		return lower.Plan(pl, prof)
	}
	for _, q := range []string{`{app="a", namespace="n"} |= "x"`, `{k8s_pod_name="a"} | detected_level="error"`} {
		if _, err := translateAndSettle(t, p, q); err != nil {
			t.Fatal(err)
		}
	}
	mu.Lock()
	plain := seen
	mu.Unlock()
	if _, err := p.translateQueryOpts(context.Background(), `{app="a"}`, true); err != nil {
		t.Fatal(err)
	}
	got := shadowCounts(t, p)
	if shadowTotal(got, "mismatch") != 0 || shadowTotal(got, "match")+shadowTotal(got, "not_lowered") != 3 {
		t.Fatalf("%v", got)
	}
	mu.Lock()
	keep := seen
	mu.Unlock()
	if plain.KeepLine || !keep.KeepLine {
		t.Fatalf("keepLine is not the request's: plain %+v keep %+v", plain, keep)
	}
	if !keep.Caps.NoDetectedLevelBodyScan || !keep.StreamFields["namespace"] || keep.Label == nil || keep.Label("k8s_pod_name") != "k8s.pod.name" {
		t.Fatalf("the profile is not the request's: %+v", keep)
	}
}

// conformance: architecture-syntax-package-layering
func TestLoweringShadow_ResultsByConstruct(t *testing.T) {
	p := newShadowProxy(t, 1, "http://unused")
	for _, q := range []string{
		`{app="a"}`,                              // match
		`{app="a"} | line_format "{{.a|upper}}"`, // LogSelect line format function: not lowered
		`avg_over_time({app="a"} | unwrap n [5m]) without (pod)`, // RangeAgg: not lowered (range-level without)
		`rate({app="a"}[5m])`,                     // RangeAgg: lowered, a match
		`sum by (app) (rate({app="a"}[5m]))`,      // VectorAgg: lowered, a match
		`sum( rate({app="a"}[5m]))`,               // VectorAgg: not lowered (blank padding, which the translator reads as a phrase)
		`stddev by (app) (rate({app="a"}[5m]))`,   // VectorAgg: not lowered (stddev)
		`topk(3, rate({app="a"}[5m]))`,            // Rank: lowered, a match
		`topk by (app) (3, rate({app="a"}[5m]))`,  // Rank: not lowered (grouping)
		`rate({app="a"}[5m]) / 2`,                 // BinOp: lowered, a match
		`rate({a="b"}[5m]) and rate({a="c"}[5m])`, // BinOp: not lowered (set operator)
		`vector(1)`,          // VectorLit
		`{app="a"} |> "<_>"`, // pattern filter: lowered, a match
		`{app="a"`,           // rejected by both
		`label_replace(rate({app="a"}[5m]), "x", "$1", "app", "(.*)")`,          // LabelReplace: lowered, a match
		`label_replace(topk(3, rate({app="a"}[5m])), "x", "$1", "app", "(.*)")`, // not lowered (Rank)
	} {
		_, _ = translateAndSettle(t, p, q)
	}
	got := shadowCounts(t, p)
	if shadowTotal(got, "mismatch") != 0 {
		t.Fatalf("%v", got)
	}
	// `{app="a"` is rejected by the translator and the lowering alike: nothing is compared, as in the corpus shadow.
	for key := range got {
		if strings.HasSuffix(key, " none") && !strings.HasPrefix(key, "skipped") {
			t.Errorf("a query both reject is counted: %q in %v", key, got)
		}
	}
	for _, want := range []string{"match LogSelect", "not_lowered LogSelect line format function", "match RangeAgg", "not_lowered RangeAgg grouping", "match VectorAgg", "not_lowered VectorAgg stddev", "not_lowered VectorAgg operand spelling", "match Rank", "not_lowered Rank grouping", "match BinOp", "not_lowered BinOp set operator", "match LabelReplace", "not_lowered LabelReplace operand Rank"} {
		if got[want] == 0 {
			t.Errorf("no %q in %v", want, got)
		}
	}
	if got["match LogSelect"] != 2 {
		t.Errorf("the selector and the pattern filter query are lowered alike: %v", got)
	}
}

// A query both reject agrees (match); one only the translator rejects is old_error, one only the lowering rejects
// is new_error.
// conformance: architecture-syntax-package-layering
func TestLoweringShadow_ErrorsAreCounted(t *testing.T) {
	p := newShadowProxy(t, 1, "http://unused")
	run := func(in shadowInput) {
		t.Helper()
		p.shadowLowering(in)
		p.loweringShadow.wg.Wait()
	}
	ok := shadowInput{query: `{app="a"}`, legacy: `{app="a"}`}
	bad := shadowInput{query: `{app="a"}`, legacyErr: errors.New("boom")}
	run(bad)
	got := shadowCounts(t, p)
	if got["old_error LogSelect"] != 1 {
		t.Fatalf("the translator errors where the lowering answers: %v", got)
	}
	p.loweringShadow.lower = func(*queryplan.Plan, lower.Profile) (physical.Op, error) { return nil, errors.New("lowering broke") }
	run(ok)
	run(bad)
	got = shadowCounts(t, p)
	if got["new_error LogSelect"] != 1 || got["old_error LogSelect"] != 1 || len(got) != 2 {
		t.Fatalf("the lowering errors where the translator answers (new_error); both error (not compared): %v", got)
	}
	// The translator rejects it and the lowering does not cover it: not compared either.
	p.loweringShadow.lower = lower.Plan
	run(shadowInput{query: `avg_over_time({app="a"} | unwrap n [5m]) without (pod)`, legacyErr: errors.New("boom")})
	if got = shadowCounts(t, p); got["not_lowered RangeAgg grouping"] != 0 || len(got) != 2 {
		t.Fatalf("a rejected query the lowering does not cover is counted: %v", got)
	}
	p.loweringShadow.lower = lower.Plan
	run(shadowInput{query: `{app="a"}`, legacy: `{app="other"}`})
	if got = shadowCounts(t, p); got["mismatch LogSelect"] != 1 {
		t.Fatalf("an answer that differs is a mismatch: %v", got)
	}
}

// conformance: architecture-syntax-package-layering
func TestLoweringShadow_SlotsFullSkips(t *testing.T) {
	p := newShadowProxy(t, 1, "http://unused")
	release := make(chan struct{})
	started := make(chan struct{}, 2)
	p.loweringShadow.lower = func(pl *queryplan.Plan, prof lower.Profile) (physical.Op, error) {
		started <- struct{}{}
		<-release
		return lower.Plan(pl, prof)
	}
	for _, q := range []string{`{app="1"}`, `{app="2"}`} {
		if _, err := p.translateQuery(q); err != nil {
			t.Fatal(err)
		}
	}
	<-started
	<-started
	begin := time.Now()
	for _, q := range []string{`{app="3"}`, `{app="4"}`, `{app="5"}`} {
		if _, err := p.translateQuery(q); err != nil {
			t.Fatal(err)
		}
	}
	if d := time.Since(begin); d > 50*time.Millisecond {
		t.Fatalf("a busy shadow delayed the translation by %v", d)
	}
	close(release)
	got := shadowCounts(t, p)
	if got["skipped none"] != 3 || got["match LogSelect"] != 2 {
		t.Fatalf("two run, three skip: %v", got)
	}
	// The slots are free again.
	if _, err := p.translateQuery(`{app="6"}`); err != nil {
		t.Fatal(err)
	}
	if got = shadowCounts(t, p); got["match LogSelect"] != 3 {
		t.Fatalf("a slot was not released: %v", got)
	}
}

// conformance: architecture-syntax-package-layering
func TestLoweringShadow_PanicIsRecoveredAndCounted(t *testing.T) {
	p := newShadowProxy(t, 1, "http://unused")
	var logs bytes.Buffer
	p.log = slog.New(slog.NewTextHandler(&logs, nil))
	p.loweringShadow.lower = func(*queryplan.Plan, lower.Profile) (physical.Op, error) { panic("lowering bug") }
	for _, q := range []string{`{app="1"}`, `{app="2"}`, `{app="3"}`} {
		got, err := translateAndSettle(t, p, q)
		if err != nil || got == "" {
			t.Fatalf("the translation was affected: %q %v", got, err)
		}
	}
	counts := shadowCounts(t, p)
	if counts["panic LogSelect"] != 3 {
		t.Fatalf("%v", counts)
	}
	if len(p.loweringShadow.slots) != 0 || strings.Count(logs.String(), "lowering shadow panicked") != 1 {
		t.Fatalf("slots held %d; the panic is logged once per second:\n%s", len(p.loweringShadow.slots), logs.String())
	}
	if strings.Contains(logs.String(), "lowering bug") || strings.Contains(logs.String(), `app=`) {
		t.Fatalf("the panic value or the query is in the log:\n%s", logs.String())
	}
}

// conformance: architecture-syntax-package-layering
func TestLoweringShadow_BudgetIsEnforced(t *testing.T) {
	p := newShadowProxy(t, 1, "http://unused")
	p.loweringShadow.budget = 5 * time.Millisecond
	p.loweringShadow.lower = func(pl *queryplan.Plan, prof lower.Profile) (physical.Op, error) {
		time.Sleep(40 * time.Millisecond)
		return lower.Plan(pl, prof)
	}
	if _, err := p.translateQuery(`{app="a"}`); err != nil {
		t.Fatal(err)
	}
	if got := shadowCounts(t, p); got["budget LogSelect"] != 1 || shadowTotal(got, "match") != 0 {
		t.Fatalf("a comparison over its budget is dropped, not compared: %v", got)
	}
	if loweringShadowBudget != 100*time.Millisecond || loweringShadowSlots != 2 {
		t.Fatal("the budget is 100 ms and the slots 2")
	}
}

// conformance: architecture-syntax-package-layering
func TestLoweringShadow_ConstructLabelIsBounded(t *testing.T) {
	for in, want := range map[string]string{
		"LogSelect": "LogSelect", "RangeAgg": "RangeAgg", "Opaque": "Opaque",
		"LogSelect pipeline": "LogSelect pipeline", "LogSelect pattern filter": "LogSelect pattern filter",
		"": "other", `{app="a"}`: "other", "Unknown": "other", "LogSelect " + strings.Repeat("x", 60): "other",
		"LogSelect {app}": "other", "rate({a})": "other",
	} {
		if got := shadowConstruct(in); got != want {
			t.Errorf("shadowConstruct(%q) = %q, want %q", in, got, want)
		}
	}
	// Whatever queries arrive, the series stay inside the fixed sets.
	p := newShadowProxy(t, 1, "http://unused")
	for i, q := range []string{`{app="a"}`, `{app="b"} |= "x"`, `rate({app="c"}[1m])`, `sum(rate({app="d"}[1m]))`, `{broken`, `vector(2)`, `1 + 1`, `{app="e"} | json | x="y"`} {
		_, _ = translateAndSettle(t, p, q)
		_ = i
	}
	results := map[string]bool{"match": true, "mismatch": true, "new_error": true, "old_error": true, "not_lowered": true, "skipped": true, "panic": true, "budget": true}
	for key := range shadowCounts(t, p) {
		result, construct, _ := strings.Cut(key, " ")
		if !results[result] || (construct != "none" && shadowConstruct(construct) != construct) {
			t.Errorf("series outside the fixed sets: %q", key)
		}
	}
}

// conformance: architecture-syntax-package-layering
func TestLoweringShadow_MismatchLogIsRateLimitedAndRedacted(t *testing.T) {
	p := newShadowProxy(t, 1, "http://unused")
	var logs bytes.Buffer
	p.log = slog.New(slog.NewTextHandler(&logs, nil))
	p.loweringShadow.lower = func(*queryplan.Plan, lower.Profile) (physical.Op, error) {
		return physical.VLQuery{Text: `"wrong"`}, nil
	}
	secret := `{app="secret-tenant-name"}`
	for i := 0; i < 5; i++ {
		if _, err := p.translateQuery(strings.Replace(secret, "name", "name"+string(rune('a'+i)), 1)); err != nil {
			t.Fatal(err)
		}
		p.loweringShadow.wg.Wait()
	}
	got := shadowCounts(t, p)
	if got["mismatch LogSelect"] != 5 {
		t.Fatalf("%v", got)
	}
	if n := strings.Count(logs.String(), "lowering shadow"); n != 1 {
		t.Fatalf("%d log lines in a burst, want 1:\n%s", n, logs.String())
	}
	line := logs.String()
	if strings.Contains(line, "secret-tenant") || !strings.Contains(line, "sha256:") || !strings.Contains(line, "keepLine=false") {
		t.Fatalf("the log line shows the query or lacks the profile:\n%s", line)
	}
	p.loweringShadow.lastLog.Store(time.Now().Add(-2 * time.Second).UnixNano())
	if _, err := p.translateQuery(`{app="again"}`); err != nil {
		t.Fatal(err)
	}
	p.loweringShadow.wg.Wait()
	if n := strings.Count(logs.String(), "lowering shadow"); n != 2 {
		t.Fatalf("a second later the next mismatch is logged: %d lines", n)
	}
}

// The shadow is observe-only: with it on, the answers and the requests VictoriaLogs receives are those of a proxy
// with it off (answers as documents: the order of a stream's labels in a body varies run to run without the shadow).
// conformance: architecture-syntax-package-layering
func TestLoweringShadow_ResponseAndBackendRequestsAreUnchanged(t *testing.T) {
	type record struct{ requests []string }
	newBackend := func() (*httptest.Server, *record, *sync.Mutex) {
		rec, mu := &record{}, &sync.Mutex{}
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			body, _ := io.ReadAll(r.Body)
			mu.Lock()
			rec.requests = append(rec.requests, r.Method+" "+r.URL.Path+"?"+r.URL.RawQuery+" "+string(body))
			mu.Unlock()
			w.Header().Set("Content-Type", "application/x-ndjson")
			_, _ = io.WriteString(w, `{"_time":"2024-01-15T10:30:00Z","_msg":"hello","_stream":"{app=\"api\"}","app":"api"}`+"\n")
		}))
		return srv, rec, mu
	}
	queries := []string{`{app="api"}`, `{app="api"} |= "hello" != "x"`, `{app="api"} | json`, `rate({app="api"}[5m])`, `sum by (app) (count_over_time({app="api"}[1m]))`, `{app="api"} |> "<_>"`, `{app="api"`}
	run := func(sample float64) ([]string, []string, map[string]int) {
		srv, rec, mu := newBackend()
		defer srv.Close()
		p := newShadowProxy(t, sample, srv.URL)
		var bodies []string
		for pass := 0; pass < 2; pass++ { // the second pass is served from the caches
			for _, q := range queries {
				v := url.Values{"query": {q}, "start": {"1705312200"}, "end": {"1705312500"}, "step": {"60"}, "limit": {"10"}}
				w := httptest.NewRecorder()
				p.handleQueryRange(w, httptest.NewRequest("GET", "/loki/api/v1/query_range?"+v.Encode(), nil))
				bodies = append(bodies, w.Result().Status+" "+w.Body.String())
			}
		}
		counts := shadowCounts(t, p)
		mu.Lock()
		defer mu.Unlock()
		reqs := append([]string(nil), rec.requests...)
		sort.Strings(reqs)
		return bodies, reqs, counts
	}
	offBodies, offReqs, offCounts := run(0)
	onBodies, onReqs, onCounts := run(1)
	// The reference for "byte for byte": a second run with the shadow off. The order of a stream's labels in a
	// body varies from run to run on its own (map order), so bodies are compared as documents.
	off2Bodies, _, _ := run(0)
	strict := 0
	for i := range offBodies {
		if offBodies[i] != off2Bodies[i] {
			strict++
		}
	}
	t.Logf("off vs off: %d of %d bodies differ as bytes", strict, len(offBodies))
	if len(offCounts) != 0 || shadowTotal(onCounts, "match")+shadowTotal(onCounts, "not_lowered") == 0 || shadowTotal(onCounts, "mismatch") != 0 {
		t.Fatalf("off %v on %v", offCounts, onCounts)
	}
	if len(offReqs) == 0 || strings.Join(offReqs, "\n") != strings.Join(onReqs, "\n") {
		t.Fatalf("VictoriaLogs requests differ:\noff %q\non  %q", offReqs, onReqs)
	}
	for i := range offBodies {
		// The stats block of a response times the request; the rest is the answer.
		if !sameJSON(stripTimings(offBodies[i]), stripTimings(onBodies[i])) {
			t.Fatalf("response %d differs:\noff %s\non  %s", i, offBodies[i], onBodies[i])
		}
	}
}

var timingRE = regexp.MustCompile(`"(execTime|queueTime|totalEntriesReturned|summary)":[^,}]*`)

func stripTimings(s string) string { return timingRE.ReplaceAllString(s, "") }

// sameJSON compares two "status body" strings as documents: the order of an object's keys is not part of an answer.
func sameJSON(a, b string) bool {
	sa, ba, _ := strings.Cut(a, "{")
	sb, bb, _ := strings.Cut(b, "{")
	var ja, jb any
	if sa != sb || json.Unmarshal([]byte("{"+ba), &ja) != nil || json.Unmarshal([]byte("{"+bb), &jb) != nil {
		return a == b
	}
	return reflect.DeepEqual(ja, jb)
}

// Nothing outlives Shutdown: a running comparison ends at its next stage, a later one is not started, and the
// goroutines are gone.
// conformance: architecture-syntax-package-layering
func TestLoweringShadow_ShutdownLeavesNoGoroutines(t *testing.T) {
	p, err := New(Config{BackendURL: "http://unused", Cache: cache.New(time.Minute, 100), LogLevel: "error", TranslateShadowSample: 1, DisableQueryPlanLowering: true})
	if err != nil {
		t.Fatal(err)
	}
	before := runtime.NumGoroutine()
	hold := make(chan struct{})
	entered := make(chan struct{}, 2)
	p.loweringShadow.lower = func(pl *queryplan.Plan, prof lower.Profile) (physical.Op, error) {
		entered <- struct{}{}
		<-hold
		return lower.Plan(pl, prof)
	}
	for _, q := range []string{`{app="1"}`, `{app="2"}`} {
		if _, err := p.translateQuery(q); err != nil {
			t.Fatal(err)
		}
	}
	<-entered
	<-entered
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	go func() { time.Sleep(60 * time.Millisecond); close(hold) }()
	_ = p.Shutdown(ctx)
	p.loweringShadow.wg.Wait() // the comparisons end once the lowering returns: the proxy is closing, so no result is counted
	if _, err := p.translateQuery(`{app="late"}`); err != nil {
		t.Fatal(err)
	}
	if got := shadowCounts(t, p); shadowTotal(got, "match") != 0 || shadowTotal(got, "skipped") != 0 || shadowTotal(got, "budget") != 0 {
		t.Fatalf("a closed shadow still counts: %v", got)
	}
	deadline := time.Now().Add(2 * time.Second)
	for runtime.NumGoroutine() > before && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if n := runtime.NumGoroutine(); n > before {
		buf := make([]byte, 1<<16)
		t.Fatalf("goroutines %d after Shutdown, %d before the queries:\n%s", n, before, buf[:runtime.Stack(buf, true)])
	}
}

// BenchmarkLoweringShadowCompare is the cost of one shadowed query, the comparison alone (what a sampled cache
// miss adds in the background).
func BenchmarkLoweringShadowCompare(b *testing.B) {
	p, err := New(Config{BackendURL: "http://unused", Cache: cache.New(time.Minute, 100), LogLevel: "error", LabelStyle: LabelStyleUnderscores, TranslateShadowSample: 1, DisableQueryPlanLowering: true})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	q := `{app="api", namespace="prod"} |= "error" != "debug"`
	legacy, err := p.translateQuery(q)
	if err != nil {
		b.Fatal(err)
	}
	p.loweringShadow.wg.Wait()
	in := shadowInput{query: q, label: p.labelTranslator.ToVL, legacy: legacy}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p.runLoweringShadow(p.loweringShadow, in)
	}
}

// BenchmarkTranslateQueryMissShadowed is the translate-on-miss path with the shadow on at sample 1: what the
// request path pays for handing the comparison off (the comparison itself runs on another goroutine).
func BenchmarkTranslateQueryMissShadowed(b *testing.B) {
	p, err := New(Config{BackendURL: "http://unused", Cache: cache.New(time.Minute, 1000), LogLevel: "error", LabelStyle: LabelStyleUnderscores, TranslateShadowSample: 1, DisableQueryPlanLowering: true})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := p.translateQueryOpts(context.Background(), `{app="api", namespace="prod"} |= "error `+strconv.Itoa(i)+`" != "debug"`, false); err != nil {
			b.Fatal(err)
		}
	}
}
