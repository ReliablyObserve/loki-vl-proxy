package proxy

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

// plainLabelsBackend answers stream_field_names with the given VictoriaLogs
// stream field names and records the queries of every other call.
type plainLabelsBackend struct {
	names   []string
	mu      sync.Mutex
	queries []string
	listed  int
	delay   time.Duration // before a stream_field_names answer
	status  int           // stream_field_names status when not 0
}

func (b *plainLabelsBackend) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if strings.HasSuffix(r.URL.Path, "/stream_field_names") {
		b.mu.Lock()
		b.listed++
		b.mu.Unlock()
		if b.delay > 0 {
			select {
			case <-time.After(b.delay):
			case <-r.Context().Done():
				return
			}
		}
		if b.status != 0 {
			w.WriteHeader(b.status)
			return
		}
		items := make([]string, 0, len(b.names))
		for _, n := range b.names {
			items = append(items, `{"value":"`+n+`","hits":1}`)
		}
		_, _ = w.Write([]byte(`{"values":[` + strings.Join(items, ",") + `]}`))
		return
	}
	b.mu.Lock()
	b.queries = append(b.queries, r.FormValue("query"))
	b.mu.Unlock()
	w.Header().Set("Content-Type", "application/x-ndjson")
	_, _ = w.Write([]byte(extractedJSONRow + "\n"))
}

func (b *plainLabelsBackend) sawQuery(substr string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, q := range b.queries {
		if strings.Contains(q, substr) {
			return true
		}
	}
	return false
}

// warmStreamLabels triggers the background refresh of the tenant's names and
// waits until it has stored them.
func warmStreamLabels(t *testing.T, p *Proxy, ctx context.Context) {
	t.Helper()
	p.tenantStreamLabelNames(ctx)
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		p.streamLabelNames.mu.Lock()
		fresh := false
		for _, e := range p.streamLabelNames.byKey {
			fresh = fresh || !e.fresh.IsZero()
		}
		p.streamLabelNames.mu.Unlock()
		if fresh {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("stream label names were not refreshed")
}

// streamLabelsProxy is a test proxy whose background refresh stops with the test.
func streamLabelsProxy(t *testing.T, backendURL, path string) *Proxy {
	t.Helper()
	p := lineFieldsProxy(t, backendURL, path)
	t.Cleanup(p.stopStreamLabelNames)
	return p
}

func scopedRequest(p *Proxy, target string) *http.Request {
	return p.withRequestScope(httptest.NewRequest(http.MethodGet, target, nil))
}

// A plain name after a parser that is a stream label of the tenant is read
// from the value stored before the parser, on the buffered, streamed and
// windowed paths of query_range.
//
// conformance: semantics/plain-name-after-parser-reads-stream-label, loki_api_v1_query_range
func TestPlainStreamLabel_QueryRangePaths(t *testing.T) {
	for _, path := range []string{"buffered", "streamed", "windowed"} {
		t.Run(path, func(t *testing.T) {
			b := &plainLabelsBackend{names: []string{"app", "env", "level"}}
			srv := httptest.NewServer(b)
			defer srv.Close()
			p := streamLabelsProxy(t, srv.URL, path)
			warmStreamLabels(t, p, scopedRequest(p, "/").Context())
			q := url.Values{"query": {`{app="col"} | json | level="debug"`}, "start": {"1790871720000000000"}, "end": {"1790875320000000000"}, "limit": {"10"}}
			w := httptest.NewRecorder()
			p.handleQueryRange(w, scopedRequest(p, "/loki/api/v1/query_range?"+q.Encode()))
			if w.Code != http.StatusOK {
				t.Fatalf("status %d: %s", w.Code, w.Body.String())
			}
			if !b.sawQuery("copy level as __lxsv_level") || !b.sawQuery(`filter __lxsv_level:="debug"`) {
				t.Fatalf("the filter must read the stored stream value: %v", b.queries)
			}
		})
	}
}

// A name that is no stream label of the tenant, a stream label named only in
// the selector, and a query without a parser keep the plain translation (the
// Drilldown fast paths match it); the label names are listed once per window and cached.
func TestPlainStreamLabel_OtherQueriesUnchanged(t *testing.T) {
	b := &plainLabelsBackend{names: []string{"app", "level", "namespace"}}
	srv := httptest.NewServer(b)
	defer srv.Close()
	p := streamLabelsProxy(t, srv.URL, "buffered")
	ctx := scopedRequest(p, "/").Context()
	warmStreamLabels(t, p, ctx)
	for _, q := range []string{
		`{app="col"} | json | user="u1"`,
		`{app="col",level="info"} | json | user="u1"`,
		`{app="col"} | json | msg="level"`,
		`{app="col"} | level="info"`,
		`{app="col"} | json | k8s.namespace.name="a" | namespace.x="b"`,
		`sum by (user) (count_over_time({app="col"} | logfmt [5m]))`,
	} {
		got, err := p.translateQueryWithContext(ctx, q)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(got, "__lxsv") {
			t.Errorf("%s: %s", q, got)
		}
	}
	got, err := p.translateQueryWithContext(ctx, `sum by (level) (count_over_time({app="col"} | logfmt [5m]))`)
	if err != nil || !strings.Contains(got, `format if (__lxsv_level:*) "<__lxsv_level>" as level`) {
		t.Errorf("metric grouping must restore the stored value: %s %v", got, err)
	}
	b.mu.Lock()
	listed := b.listed
	b.mu.Unlock()
	if _, err := p.translateQueryWithContext(ctx, `sum by (level) (count_over_time({app="col"} | json [5m]))`); err != nil {
		t.Fatal(err)
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if listed == 0 || b.listed != listed {
		t.Errorf("stream label names listed %d times, then %d: the listing must be cached", listed, b.listed)
	}
}

// A name VictoriaLogs stores under another name, a derived label and a query
// outside a client request are not read as stream values.
func TestPlainStreamLabel_ExcludedNames(t *testing.T) {
	b := &plainLabelsBackend{names: []string{"service_name", "k8s.namespace.name", "level"}}
	srv := httptest.NewServer(b)
	defer srv.Close()
	p := streamLabelsProxy(t, srv.URL, "buffered")
	ctx := scopedRequest(p, "/").Context()
	warmStreamLabels(t, p, ctx)
	for _, q := range []string{`{app="x"} | json | service_name="a"`, `{app="x"} | json | k8s_namespace_name="a"`} {
		if got, _ := p.translateQueryWithContext(ctx, q); strings.Contains(got, "__lxsv") {
			t.Errorf("%s: %s", q, got)
		}
	}
	if names := p.plainStreamLabelNames(httptest.NewRequest(http.MethodGet, "/", nil).Context(), `{app="x"} | json | level="a"`); names != nil {
		t.Errorf("no client request: %v", names)
	}
}

// A slow or failing listing never adds latency to a query: the first
// translation uses no names (the plain translation), a failed refresh is not
// retried within the retry window, and the query keeps working.
func TestPlainStreamLabel_SlowOrFailingListingAddsNoLatency(t *testing.T) {
	for name, mk := range map[string]func(*plainLabelsBackend){
		"slow": func(b *plainLabelsBackend) { b.delay = 2 * time.Second },
		"429":  func(b *plainLabelsBackend) { b.status = http.StatusTooManyRequests },
	} {
		t.Run(name, func(t *testing.T) {
			b := &plainLabelsBackend{names: []string{"app", "level"}}
			mk(b)
			srv := httptest.NewServer(b)
			defer srv.Close()
			p := streamLabelsProxy(t, srv.URL, "buffered")
			ctx := scopedRequest(p, "/").Context()
			begin := time.Now()
			got, err := p.translateQueryWithContext(ctx, `sum by (level) (count_over_time({app="col"} | logfmt [5m]))`)
			if err != nil || strings.Contains(got, "__lxsv") {
				t.Fatalf("cold translation must be the plain one: %s %v", got, err)
			}
			if d := time.Since(begin); d > 200*time.Millisecond {
				t.Fatalf("translation waited %s for the listing", d)
			}
			// The refresh ends (timeout or error) without storing names, and is not retried at once.
			deadline := time.Now().Add(5 * time.Second)
			for time.Now().Before(deadline) {
				p.streamLabelNames.mu.Lock()
				busy := false
				for _, e := range p.streamLabelNames.byKey {
					busy = busy || e.refreshing
				}
				p.streamLabelNames.mu.Unlock()
				if !busy {
					break
				}
				time.Sleep(20 * time.Millisecond)
			}
			for i := 0; i < 5; i++ {
				p.translateQueryWithContext(ctx, `sum by (level) (count_over_time({app="col"} | logfmt [5m]))`) //nolint:errcheck
			}
			p.streamLabelNames.mu.Lock()
			defer p.streamLabelNames.mu.Unlock()
			if p.streamLabelNames.refreshes != 1 {
				t.Errorf("refreshes = %d, want 1 within the retry window", p.streamLabelNames.refreshes)
			}
		})
	}
}

// Concurrent queries start one refresh (single flight).
func TestPlainStreamLabel_RefreshIsSingleFlight(t *testing.T) {
	b := &plainLabelsBackend{names: []string{"app", "level"}, delay: 300 * time.Millisecond}
	srv := httptest.NewServer(b)
	defer srv.Close()
	p := streamLabelsProxy(t, srv.URL, "buffered")
	ctx := scopedRequest(p, "/").Context()
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			p.translateQueryWithContext(ctx, `{app="col"} | json | level="a"`) //nolint:errcheck
		}()
	}
	wg.Wait()
	p.streamLabelNames.mu.Lock()
	n := p.streamLabelNames.refreshes
	p.streamLabelNames.mu.Unlock()
	if n != 1 {
		t.Fatalf("refreshes = %d, want 1", n)
	}
}

// Past the TTL the last known names are still served while the refresh runs.
func TestPlainStreamLabel_StaleNamesServedDuringRefresh(t *testing.T) {
	b := &plainLabelsBackend{names: []string{"app", "level"}}
	srv := httptest.NewServer(b)
	defer srv.Close()
	p := streamLabelsProxy(t, srv.URL, "buffered")
	ctx := scopedRequest(p, "/").Context()
	warmStreamLabels(t, p, ctx)
	b.mu.Lock()
	b.delay = 2 * time.Second
	b.mu.Unlock()
	p.streamLabelNames.mu.Lock()
	for _, e := range p.streamLabelNames.byKey {
		e.fresh = time.Now().Add(-2 * streamLabelNamesTTL)
		e.tried = time.Time{}
	}
	p.streamLabelNames.mu.Unlock()
	begin := time.Now()
	got, err := p.translateQueryWithContext(ctx, `{app="col"} | json | level="a"`)
	if err != nil || !strings.Contains(got, "__lxsv_level") || time.Since(begin) > 200*time.Millisecond {
		t.Fatalf("stale names must be served at once: %s %v %s", got, err, time.Since(begin))
	}
}

// A name read only by a label_format or line_format template counts as named.
func TestPlainStreamLabel_TemplateReadsName(t *testing.T) {
	b := &plainLabelsBackend{names: []string{"app", "level"}}
	srv := httptest.NewServer(b)
	defer srv.Close()
	p := streamLabelsProxy(t, srv.URL, "buffered")
	ctx := scopedRequest(p, "/").Context()
	warmStreamLabels(t, p, ctx)
	got, err := p.translateQueryWithContext(ctx, `{app="col"} | json | label_format x="{{.level}}-a" | x="info-a"`)
	if err != nil || !strings.Contains(got, `format "<__lxsv_level>-a" as x`) {
		t.Fatalf("%s %v", got, err)
	}
}

// Shutdown cancels a running refresh and starts none after it: no goroutine of
// the refresher outlives the proxy.
func TestPlainStreamLabel_ShutdownStopsRefresher(t *testing.T) {
	b := &plainLabelsBackend{names: []string{"app", "level"}, delay: 30 * time.Second}
	srv := httptest.NewServer(b)
	defer srv.Close()
	p := lineFieldsProxy(t, srv.URL, "buffered")
	ctx := scopedRequest(p, "/").Context()
	p.tenantStreamLabelNames(ctx) // starts the (blocked) refresh
	for seen := time.Now().Add(2 * time.Second); refresherGoroutines() == 0; {
		if time.Now().After(seen) {
			t.Fatal("the blocked refresh never showed up as a refreshStreamLabelNames goroutine")
		}
		time.Sleep(5 * time.Millisecond)
	}
	begin := time.Now()
	if err := p.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if d := time.Since(begin); d > 2*time.Second {
		t.Fatalf("Shutdown waited %s for the refresher", d)
	}
	p.streamLabelNames.mu.Lock()
	for _, e := range p.streamLabelNames.byKey {
		e.fresh, e.tried = time.Time{}, time.Time{}
	}
	started := p.streamLabelNames.refreshes
	p.streamLabelNames.mu.Unlock()
	p.tenantStreamLabelNames(ctx)
	p.streamLabelNames.mu.Lock()
	after := p.streamLabelNames.refreshes
	p.streamLabelNames.mu.Unlock()
	if after != started {
		t.Fatal("a refresh started after Shutdown")
	}
	// Count the refresher's own goroutines: the process-wide count also moves with
	// other tests' goroutines still finishing in a full-package run.
	deadline := time.Now().Add(3 * time.Second)
	for refresherGoroutines() > 0 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if n := refresherGoroutines(); n > 0 {
		t.Fatalf("%d refreshStreamLabelNames goroutine(s) still running after Shutdown", n)
	}
}

// refresherGoroutines counts the goroutines running refreshStreamLabelNames.
func refresherGoroutines() int {
	buf := make([]byte, 1<<20)
	for {
		n := runtime.Stack(buf, true)
		if n < len(buf) {
			buf = buf[:n]
			break
		}
		buf = make([]byte, 2*len(buf))
	}
	count := 0
	for _, g := range strings.Split(string(buf), "\n\n") {
		if strings.Contains(g, ".refreshStreamLabelNames(") {
			count++
		}
	}
	return count
}

// An answer made while the names were unknown (the plain translation) is not
// served from the response caches once the names are known: the cache keys
// carry the names a query was translated with.
func TestPlainStreamLabel_ColdAnswerNotServedAfterNamesKnown(t *testing.T) {
	b := &plainLabelsBackend{names: []string{"app", "level"}}
	srv := httptest.NewServer(b)
	defer srv.Close()
	p := streamLabelsProxy(t, srv.URL, "buffered")
	q := url.Values{"query": {`{app="col"} | json | level="debug"`}, "start": {"1790871720000000000"}, "end": {"1790875320000000000"}, "limit": {"10"}}
	do := func() {
		w := httptest.NewRecorder()
		p.handleQueryRange(w, scopedRequest(p, "/loki/api/v1/query_range?"+q.Encode()))
		if w.Code != http.StatusOK {
			t.Fatalf("status %d", w.Code)
		}
	}
	do() // cold: names unknown, plain translation, answer cached
	if b.sawQuery("__lxsv") {
		t.Fatal("cold query must use the plain translation")
	}
	warmStreamLabels(t, p, scopedRequest(p, "/").Context())
	do()
	if !b.sawQuery(`filter __lxsv_level:="debug"`) {
		t.Fatalf("the cached cold answer was served once the names were known: %v", b.queries)
	}
}

// Names are keyed by tenant: another auth scope of the tenant reuses them.
func TestPlainStreamLabel_NamesSharedAcrossAuthScopes(t *testing.T) {
	b := &plainLabelsBackend{names: []string{"app", "level"}}
	srv := httptest.NewServer(b)
	defer srv.Close()
	p := streamLabelsProxy(t, srv.URL, "buffered")
	warmStreamLabels(t, p, scopedRequest(p, "/").Context())
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Authorization", "Bearer other-user")
	got := p.plainStreamLabelNames(p.withRequestScope(req).Context(), `{app="col"} | json | level="a"`)
	if len(got) != 1 || got[0] != "level" {
		t.Fatalf("a second auth scope must see the tenant's names at once: %v", got)
	}
}
