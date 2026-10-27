package proxy

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"runtime"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// edgeVL is a VictoriaLogs whose near-now listings can be held: settled data
// carries the labels of base, and a listing whose range ends within the last
// two minutes also carries those of fresh. While gate is closed, every
// listing that reads the near-now edge waits for it (or for its request to
// end). Listings are answered per tenant (AccountID), with the tenant in the
// value names, so a cross-tenant answer is visible.
type edgeVL struct {
	mu        sync.Mutex
	base      []string
	fresh     []string
	gate      chan struct{} // nil: open
	edgeCalls atomic.Int64  // near-now listings received
	byStart   sync.Map      // start parameter -> *atomic.Int64 near-now listings with it
	waiting   atomic.Int64  // near-now listings held at the gate
	cancelled atomic.Int64  // held listings whose request ended
}

func (f *edgeVL) close() {
	f.mu.Lock()
	f.gate = make(chan struct{})
	f.mu.Unlock()
}

func (f *edgeVL) open() {
	f.mu.Lock()
	if f.gate != nil {
		close(f.gate)
		f.gate = nil
	}
	f.mu.Unlock()
}

func (f *edgeVL) addFresh(names ...string) {
	f.mu.Lock()
	f.fresh = append(f.fresh, names...)
	f.mu.Unlock()
}

func (f *edgeVL) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	_ = r.ParseForm()
	w.Header().Set("Content-Type", "application/json")
	if !metadataInventoryPaths[r.URL.Path] {
		if r.URL.Path == "/select/logsql/query" { // inventory row counts
			_, _ = w.Write([]byte(`{"rows":"1"}` + "\n"))
			return
		}
		_, _ = w.Write([]byte(`{"values":[]}`))
		return
	}
	end, _ := strconv.ParseInt(r.Form.Get("end"), 10, 64)
	nearNow := end == 0 || time.Since(time.Unix(0, end)) < 2*time.Minute
	f.mu.Lock()
	names := slices.Clone(f.base)
	gate := f.gate
	if nearNow {
		names = append(names, f.fresh...)
	}
	f.mu.Unlock()
	if nearNow {
		f.edgeCalls.Add(1)
		n, _ := f.byStart.LoadOrStore(r.Form.Get("start"), new(atomic.Int64))
		n.(*atomic.Int64).Add(1)
		if gate != nil {
			f.waiting.Add(1)
			select {
			case <-gate:
				f.waiting.Add(-1)
			case <-r.Context().Done():
				f.waiting.Add(-1)
				f.cancelled.Add(1)
				return
			}
		}
	}
	tenant := r.Header.Get("AccountID")
	items := []vlValueHits{}
	for _, n := range names {
		value := n
		if r.URL.Path == "/select/logsql/stream_field_values" || r.URL.Path == "/select/logsql/field_values" {
			value = n + "-" + tenant // values of the asked field
		}
		items = append(items, vlValueHits{Value: value, Hits: 1})
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"values": items})
}

type edgeProxyOptions struct {
	maxStaleness, budget time.Duration
	inventory            bool
}

func newEdgeProxy(t *testing.T, vl *edgeVL, o edgeProxyOptions) (*Proxy, *http.ServeMux) {
	t.Helper()
	srv := httptest.NewServer(vl)
	t.Cleanup(srv.Close)
	c := cache.New(time.Hour, 100000)
	t.Cleanup(c.Close)
	parallelism := -1 // one VictoriaLogs call per listing
	if o.inventory {
		parallelism = 4
	}
	p, err := New(Config{
		BackendURL:                   srv.URL,
		Cache:                        c,
		LogLevel:                     "error",
		DisableLabelsCacheWarm:       true,
		MetadataInventoryParallelism: parallelism,
		MetadataCacheFreshness:       24 * time.Hour,
		// Every cached near-now answer is past the freshness check, so each
		// request reads the edge again, as under steady Grafana refreshes.
		RecentTailRefreshMaxStaleness: time.Nanosecond,
		MetadataMaxStaleness:          o.maxStaleness,
		MetadataEdgeRefreshBudget:     o.budget,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	mux := http.NewServeMux()
	p.RegisterRoutes(mux)
	return p, mux
}

type edgeResponse struct {
	code  int
	data  []string
	stale bool
	took  time.Duration
}

// edgeRequest sends a /labels (label == "") or /label/{label}/values request
// for the last hour, as Grafana does, with tenant.
func edgeRequest(t *testing.T, mux *http.ServeMux, tenant, label string, end time.Time) edgeResponse {
	t.Helper()
	path := "/loki/api/v1/labels"
	if label != "" {
		path = "/loki/api/v1/label/" + label + "/values"
	}
	q := url.Values{"start": {strconv.FormatInt(end.Add(-time.Hour).UnixNano(), 10)}, "end": {strconv.FormatInt(end.UnixNano(), 10)}}
	req := httptest.NewRequest(http.MethodGet, path+"?"+q.Encode(), nil)
	if tenant != "" {
		req.Header.Set("X-Scope-OrgID", tenant)
	}
	rec := httptest.NewRecorder()
	began := time.Now()
	mux.ServeHTTP(rec, req)
	out := edgeResponse{code: rec.Code, stale: rec.Header().Get(staleResponseHeader) != "", took: time.Since(began)}
	var body struct {
		Data []string `json:"data"`
	}
	_ = json.Unmarshal(rec.Body.Bytes(), &body)
	out.data = body.Data
	return out
}

// callsFrom returns the near-now listings that started at the window start of
// a request ending at end (VictoriaLogs reads label bounds in milliseconds).
func (f *edgeVL) callsFrom(end time.Time) int64 {
	n, ok := f.byStart.Load(lokiLabelStartToVL(strconv.FormatInt(end.Add(-time.Hour).UnixNano(), 10)))
	if !ok {
		return 0
	}
	return n.(*atomic.Int64).Load()
}

// refreshedSince reports whether an answer read from VictoriaLogs after t has
// been stored and recorded.
func refreshedSince(p *Proxy, t time.Time) bool {
	p.metadataEdges.mu.Lock()
	defer p.metadataEdges.mu.Unlock()
	for _, at := range p.metadataEdges.read {
		if at.After(t) {
			return true
		}
	}
	return false
}

// quickEdgeRequest is edgeRequest for a request that must not wait for a
// held VictoriaLogs call: it fails the test after five seconds instead of
// hanging (the caller's cleanup opens the gate for the request left behind).
func quickEdgeRequest(t *testing.T, mux *http.ServeMux, tenant, label string, end time.Time) edgeResponse {
	t.Helper()
	done := make(chan edgeResponse, 1)
	go func() { done <- edgeRequest(t, mux, tenant, label, end) }()
	select {
	case r := <-done:
		return r
	case <-time.After(5 * time.Second):
		t.Fatal("the request waited for VictoriaLogs")
		return edgeResponse{}
	}
}

// await polls cond for up to five seconds.
func await(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// A near-now /labels or /label/{name}/values whose edge refresh is held in
// VictoriaLogs is answered within the budget from the last answer this
// replica read, marked stale; the refresh completes in the background and
// the following request lists the label or value that appeared meanwhile.
// The settled buckets come from the inventory throughout: only near-now
// listings reach the gate.
//
// conformance: quality/metadata-bounded-staleness, loki_api_v1_labels, loki_api_v1_label_name_values
func TestMetadataEdge_StaleWithinBudgetThenFreshOnTheNextRequest(t *testing.T) {
	for _, label := range []string{"", "app"} {
		name := "labels"
		if label != "" {
			name = "label values"
		}
		t.Run(name, func(t *testing.T) {
			vl := &edgeVL{base: []string{"app", "pod"}}
			p, mux := newEdgeProxy(t, vl, edgeProxyOptions{maxStaleness: 10 * time.Second, budget: 50 * time.Millisecond, inventory: true})
			first := edgeRequest(t, mux, "1", label, time.Now())
			if first.code != http.StatusOK || first.stale || len(first.data) == 0 {
				t.Fatalf("first answer %+v", first)
			}

			vl.addFresh("canary")
			vl.close()
			t.Cleanup(vl.open)
			held := vl.edgeCalls.Load()
			secondAt := time.Now()
			second := quickEdgeRequest(t, mux, "1", label, secondAt)
			if second.code != http.StatusOK || !second.stale {
				t.Fatalf("second answer %+v, want the last answer marked stale", second)
			}
			if !slices.Equal(second.data, first.data) {
				t.Fatalf("stale answer %v, want the last answer %v", second.data, first.data)
			}
			// The answer came while the gate held every near-now listing, so
			// it did not wait for VictoriaLogs; the refresh it started reaches
			// the gate and stays there (on a loaded host it may get there only
			// after the answer).
			await(t, "the background refresh at the gate", func() bool { return vl.waiting.Load() > 0 && vl.edgeCalls.Load() > held })
			if second.took > 2*time.Second {
				t.Fatalf("stale answer took %s with a 50ms budget", second.took)
			}

			vl.open()
			await(t, "the background refresh", func() bool { return refreshedSince(p, secondAt) })
			third := edgeRequest(t, mux, "1", label, time.Now())
			if third.code != http.StatusOK {
				t.Fatalf("third answer %+v", third)
			}
			want := "canary"
			if label != "" {
				want = "canary-1"
			}
			if !slices.Contains(third.data, want) {
				t.Fatalf("after the refresh the answer %v misses %q", third.data, want)
			}
		})
	}
}

// Past -metadata-max-staleness, and with stale serving off, a request waits
// for VictoriaLogs as before and answers exactly.
//
// conformance: quality/metadata-bounded-staleness
func TestMetadataEdge_PastTheBoundTheRequestWaits(t *testing.T) {
	for _, tc := range []struct {
		name  string
		opts  edgeProxyOptions
		age   bool     // make the last read older than the bound
		known []string // labels of the first answer
	}{
		{"older than the bound", edgeProxyOptions{maxStaleness: 10 * time.Second, budget: 20 * time.Millisecond}, true, []string{"app"}},
		{"stale serving off", edgeProxyOptions{maxStaleness: 0, budget: 20 * time.Millisecond}, false, []string{"app"}},
		// An empty answer is a negative entry (data may not have been
		// searchable yet), never a last known answer.
		{"last answer empty", edgeProxyOptions{maxStaleness: 10 * time.Second, budget: 20 * time.Millisecond}, false, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			vl := &edgeVL{base: tc.known}
			p, mux := newEdgeProxy(t, vl, tc.opts)
			if r := edgeRequest(t, mux, "0", "", time.Now()); r.code != http.StatusOK {
				t.Fatalf("first answer %+v", r)
			}
			if tc.age {
				p.metadataEdges.mu.Lock()
				for k := range p.metadataEdges.read {
					p.metadataEdges.read[k] = time.Now().Add(-time.Minute)
				}
				p.metadataEdges.mu.Unlock()
			}
			vl.addFresh("canary")
			vl.close()
			t.Cleanup(vl.open)
			done := make(chan edgeResponse, 1)
			go func() { done <- edgeRequest(t, mux, "0", "", time.Now()) }()
			await(t, "the edge listing at the gate", func() bool { return vl.waiting.Load() > 0 })
			select {
			case r := <-done:
				t.Fatalf("answered %+v while VictoriaLogs held the edge", r)
			case <-time.After(200 * time.Millisecond):
			}
			vl.open()
			r := <-done
			if r.code != http.StatusOK || r.stale || !slices.Contains(r.data, "canary") {
				t.Fatalf("answer %+v, want the exact answer with canary", r)
			}
		})
	}
}

// Concurrent requests for one key share one background refresh.
//
// conformance: quality/metadata-bounded-staleness
func TestMetadataEdge_ConcurrentRequestsShareOneRefresh(t *testing.T) {
	vl := &edgeVL{base: []string{"app"}}
	_, mux := newEdgeProxy(t, vl, edgeProxyOptions{maxStaleness: 10 * time.Second, budget: 20 * time.Millisecond})
	end := time.Now()
	if r := edgeRequest(t, mux, "0", "", end); r.code != http.StatusOK {
		t.Fatalf("first answer %+v", r)
	}
	vl.close()
	t.Cleanup(vl.open)
	before := vl.callsFrom(end)
	var wg sync.WaitGroup
	var stale atomic.Int64
	for range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if r := edgeRequest(t, mux, "0", "", end); r.code == http.StatusOK && r.stale {
				stale.Add(1)
			}
		}()
	}
	all := make(chan struct{})
	go func() { wg.Wait(); close(all) }()
	select {
	case <-all:
	case <-time.After(5 * time.Second):
		t.Fatalf("requests waited for VictoriaLogs (%d answered stale)", stale.Load())
	}
	// The shared refresh may reach VictoriaLogs only after the answers on a
	// loaded host; give any second refresh the same chance to show up.
	await(t, "the shared refresh at the gate", func() bool { return vl.callsFrom(end)-before >= 1 })
	time.Sleep(200 * time.Millisecond)
	if got := vl.callsFrom(end) - before; got != 1 {
		t.Fatalf("20 concurrent requests made %d VictoriaLogs calls, want one shared refresh", got)
	}
	if stale.Load() != 20 {
		t.Fatalf("%d of 20 requests were answered stale within the budget", stale.Load())
	}
	vl.open()
}

// Stale answers are kept per tenant: a tenant this replica has not answered
// waits for VictoriaLogs even while another tenant is served stale, and no
// tenant ever gets another's answer.
//
// conformance: quality/metadata-bounded-staleness, tenant-numeric-routing
func TestMetadataEdge_StaleAnswersStayPerTenant(t *testing.T) {
	vl := &edgeVL{base: []string{"app"}}
	_, mux := newEdgeProxy(t, vl, edgeProxyOptions{maxStaleness: 10 * time.Second, budget: 20 * time.Millisecond})
	end := time.Now()
	a := edgeRequest(t, mux, "1", "app", end)
	if a.code != http.StatusOK || !slices.Equal(a.data, []string{"app-1"}) {
		t.Fatalf("tenant 1 answer %+v", a)
	}
	vl.close()
	t.Cleanup(vl.open)
	if r := quickEdgeRequest(t, mux, "1", "app", end); !r.stale || !slices.Equal(r.data, []string{"app-1"}) {
		t.Fatalf("tenant 1 second answer %+v, want its own stale answer", r)
	}
	done := make(chan edgeResponse, 1)
	go func() { done <- edgeRequest(t, mux, "2", "app", end) }()
	select {
	case r := <-done:
		t.Fatalf("tenant 2 answered %+v without VictoriaLogs", r)
	case <-time.After(200 * time.Millisecond):
	}
	vl.open()
	if r := <-done; r.code != http.StatusOK || r.stale || !slices.Equal(r.data, []string{"app-2"}) {
		t.Fatalf("tenant 2 answer %+v, want its own exact answer", r)
	}
}

// A request whose end is older than max-metadata-cache-freshness reads
// settled data only: it is answered from its cached answer as before, never
// marked stale and never refreshed.
//
// conformance: quality/metadata-bounded-staleness
func TestMetadataEdge_SettledWindowsAreNeverStale(t *testing.T) {
	vl := &edgeVL{base: []string{"app"}}
	_, mux := newEdgeProxy(t, vl, edgeProxyOptions{maxStaleness: 10 * time.Second, budget: 20 * time.Millisecond})
	end := time.Now().Add(-25 * time.Hour)
	if r := edgeRequest(t, mux, "0", "", end); r.code != http.StatusOK || r.stale {
		t.Fatalf("first answer %+v", r)
	}
	vl.close()
	t.Cleanup(vl.open)
	calls := vl.edgeCalls.Load()
	if r := quickEdgeRequest(t, mux, "0", "", end); r.code != http.StatusOK || r.stale {
		t.Fatalf("settled answer %+v, want the cached exact answer", r)
	}
	if vl.edgeCalls.Load() != calls {
		t.Fatal("a settled window read VictoriaLogs")
	}
}

// Shutdown cancels a background edge refresh still held in VictoriaLogs and
// every goroutine the proxy started returns.
//
// conformance: quality/metadata-bounded-staleness
func TestMetadataEdge_ShutdownStopsBackgroundRefreshes(t *testing.T) {
	runtime.GC()
	baseline := runtime.NumGoroutine()
	before := goroutineCreators()

	vl := &edgeVL{base: []string{"app"}}
	srv := httptest.NewServer(vl)
	c := cache.New(time.Hour, 1000)
	p, err := New(Config{
		BackendURL:                    srv.URL,
		Cache:                         c,
		LogLevel:                      "error",
		DisableLabelsCacheWarm:        true,
		MetadataInventoryParallelism:  -1,
		MetadataCacheFreshness:        24 * time.Hour,
		RecentTailRefreshMaxStaleness: time.Nanosecond,
		MetadataMaxStaleness:          10 * time.Second,
		MetadataEdgeRefreshBudget:     10 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	p.RegisterRoutes(mux)
	end := time.Now()
	if r := edgeRequest(t, mux, "0", "", end); r.code != http.StatusOK {
		t.Fatalf("first answer %+v", r)
	}
	vl.close()
	t.Cleanup(vl.open)
	if r := quickEdgeRequest(t, mux, "0", "", end); !r.stale {
		t.Fatalf("second answer %+v, want stale", r)
	}
	await(t, "the background refresh at the gate", func() bool { return vl.waiting.Load() > 0 })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := p.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	await(t, "the held refresh to be cancelled", func() bool { return vl.cancelled.Load() > 0 })
	vl.open()
	c.Close()
	srv.Close()
	if report := settleGoroutines(baseline, before); report != "" {
		t.Fatal(report)
	}
	// After Shutdown no refresh starts: the request waits for VictoriaLogs.
	if got := p.startMetadataEdgeRefresh(context.Background(), httptest.NewRequest(http.MethodGet, "/", nil), "k", func(context.Context) ([]byte, error) { return nil, fmt.Errorf("ran") }); got != nil {
		t.Fatal("a refresh started after Shutdown")
	}
}
