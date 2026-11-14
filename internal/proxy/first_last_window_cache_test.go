package proxy

import (
	"bytes"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/testutil/vlemu"
)

// The caches of first_over_time and last_over_time on a window longer than the step: the window evaluator's
// stats_query_range reads (the earliest or latest row per series and bucket, then the row's value per group,
// rowBucketPick) are kept per bucket by the stats extent cache, and its answers per step by the settled steps cache,
// as for the other unwrap functions. A repeated request asks VictoriaLogs nothing, a refresh moved forward asks it
// only for the buckets the stored ones do not hold, and every answer is byte for byte the uncached proxy's.

// firstLastCacheQueries are the window evaluator's first and last shapes: a keep before the unwrap over logfmt and
// json, and streams without a parser.
var firstLastCacheQueries = []string{
	`sum by (app) (first_over_time({env="production"} | logfmt | keep app, amount | unwrap amount [5m]))`,
	`last_over_time({env="production"} | json | keep app, amount | unwrap amount [5m])`,
	`max by (app) (last_over_time({env="production"} | logfmt | keep app, amount | unwrap amount [2m]))`,
	`sum(first_over_time({env="production"} | json | keep app, amount | unwrap amount [90s]))`,
	`sum by (app) (last_over_time({env="production"} | unwrap amount [5m]))`,
}

// vlTraffic counts every request VictoriaLogs receives and the bytes it answers with.
type vlTraffic struct {
	inner http.Handler
	mu    sync.Mutex
	calls int
	bytes int
}

func (v *vlTraffic) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	rec := &countingWriter{ResponseWriter: w}
	v.inner.ServeHTTP(rec, r)
	v.mu.Lock()
	v.calls++
	v.bytes += rec.n
	v.mu.Unlock()
}

// take returns the calls and bytes since the last take.
func (v *vlTraffic) take() (calls, bytes int) {
	v.mu.Lock()
	defer v.mu.Unlock()
	calls, bytes, v.calls, v.bytes = v.calls, v.bytes, 0, 0
	return calls, bytes
}

type countingWriter struct {
	http.ResponseWriter
	n int
}

func (c *countingWriter) Write(b []byte) (int, error) {
	n, err := c.ResponseWriter.Write(b)
	c.n += n
	return n, err
}

// conformance: semantics/unwrap-first-last-range-longer-than-step, quality/query-range-extent-cache, quality/query-range-settled-steps, loki_api_v1_query_range
func TestFirstLastWindowCacheReuse(t *testing.T) {
	s0 := time.Unix(1_700_001_000, 0).UTC()
	rows := queryStepsRows(s0, 4*time.Hour, nil)
	end := s0.Add(3 * time.Hour)
	const step = 10 * time.Second
	cold := extentWindow{end.Add(-90 * time.Minute), end, step}
	const k = 3
	refresh := extentWindow{cold.start.Add(k * step), cold.end.Add(k * step), step}
	for _, query := range firstLastCacheQueries {
		t.Run(query, func(t *testing.T) {
			traffic := &vlTraffic{inner: (&vlemu.Emulator{T: t, Rows: rows, Field: "amount", Version: "v1.50.0"}).Handler()}
			onSrv, onShim := newExtentShim(t, traffic)
			offSrv, _ := newExtentShim(t, (&vlemu.Emulator{T: t, Rows: rows, Field: "amount", Version: "v1.50.0"}).Handler())
			on := newExtentTestProxy(t, onSrv.URL, true, 256<<20)
			off := newExtentTestProxy(t, offSrv.URL, false, 256<<20)
			same := func(label string, w extentWindow) {
				t.Helper()
				wantCode, want, _ := queryStepsGet(t, off, query, w)
				gotCode, got, _ := queryStepsGet(t, on, query, w)
				if gotCode != http.StatusOK || gotCode != wantCode || !bytes.Equal(got, want) {
					t.Fatalf("%s: the caches changed the answer\n with: %d %s\nwithout: %d %s", label, gotCode, got, wantCode, want)
				}
				if !bytes.Contains(want, []byte(`"values":[[`)) {
					t.Fatalf("%s: no data, the run proves nothing: %s", label, want)
				}
			}

			traffic.take() // the version probe of the proxy's start
			same("cold", cold)
			coldCalls, coldBuckets := onShim.bucketsRead()
			coldVL, coldBytes := traffic.take()
			if coldCalls == 0 {
				t.Fatal("cold: no stats_query_range read")
			}
			// The window evaluator's read, as VictoriaLogs receives it, is one the extent cache keeps.
			onShim.mu.Lock()
			read := onShim.calls[0].query
			onShim.mu.Unlock()
			if !strings.HasSuffix(read, rowBucketValue) || !statsExtentQueryCacheable(read) {
				t.Fatalf("the bucket pick read is not the window evaluator's or not cacheable: %s", read)
			}

			onShim.reset()
			same("repeat", cold)
			if calls, _ := onShim.bucketsRead(); calls != 0 {
				t.Fatalf("repeat: %d stats_query_range calls, want none", calls)
			}
			if calls, _ := traffic.take(); calls != 0 {
				t.Fatalf("repeat: %d VictoriaLogs requests, want none", calls)
			}

			onShim.reset()
			same("refresh", refresh)
			calls, buckets := onShim.bucketsRead()
			refreshVL, refreshBytes := traffic.take()
			// The new steps' buckets (the bucket is gcd(step, range) = the step), and VictoriaLogs' bucket edge.
			if buckets > k+2 {
				t.Fatalf("refresh by %d steps: %d calls read %d buckets, want at most %d (cold read %d)", k, calls, buckets, k+2, coldBuckets)
			}
			t.Logf("VictoriaLogs: cold %d requests (%d stats_query_range, %d buckets), %d bytes; repeat 0; refresh by %d steps %d requests (%d stats_query_range, %d buckets), %d bytes",
				coldVL, coldCalls, coldBuckets, coldBytes, k, refreshVL, calls, buckets, refreshBytes)
			outcomes := extentOutcomes(t, on)
			if outcomes["hit"]+outcomes["partial"] == 0 || outcomes["unreadable"] != 0 || outcomes["too_large"] != 0 {
				t.Fatalf("extent cache outcomes %v: the refresh must be read from stored buckets", outcomes)
			}
			t.Logf("extent cache %v, settled steps %v", outcomes, queryStepsOutcomes(t, on))

			// Through the routes, with the Loki-shaped response caches in front: a repeated request asks VictoriaLogs
			// nothing and answers the bytes of the first.
			full := newExtentTestProxy(t, onSrv.URL, true, 256<<20)
			params := url.Values{"query": {query}, "start": {strconv.FormatInt(cold.start.UnixNano(), 10)},
				"end": {strconv.FormatInt(cold.end.UnixNano(), 10)}, "step": {"10"}}
			target := "/loki/api/v1/query_range?" + params.Encode()
			first := doCompatProxyRequest(full, target, map[string]string{"X-Scope-OrgID": "0"})
			full.statsExtentStores.wait()
			traffic.take()
			again := doCompatProxyRequest(full, target, map[string]string{"X-Scope-OrgID": "0"})
			if calls, _ := traffic.take(); calls != 0 || first.Code != http.StatusOK || !bytes.Equal(first.Body.Bytes(), again.Body.Bytes()) {
				t.Fatalf("repeat through the routes: %d VictoriaLogs requests, %d then %d, same bytes %v", calls, first.Code, again.Code, bytes.Equal(first.Body.Bytes(), again.Body.Bytes()))
			}
		})
	}
}

// Over the settled steps' windows (refreshes by one and two steps and a minute, windows moved back, a re-run, a
// widened window, other steps and a grid off whole seconds), the cached first and last answers are the uncached
// proxy's.
//
// conformance: semantics/unwrap-first-last-range-longer-than-step, quality/query-range-extent-cache, quality/query-range-settled-steps
func TestFirstLastWindowCacheMatchesUncached(t *testing.T) {
	s0 := time.Unix(1_700_001_000, 0).UTC()
	rows := queryStepsRows(s0, 4*time.Hour, nil)
	outcomes, withData, _, _ := runQueryStepsDifferential(t, rows, firstLastCacheQueries, queryStepsWindows(s0.Add(3*time.Hour)))
	t.Logf("settled steps %v; %d answers with data", outcomes, withData)
	if withData < len(firstLastCacheQueries)*10 || outcomes["hit"]+outcomes["partial"] == 0 {
		t.Fatalf("the run must hold data and be answered from settled steps: %v, %d with data", outcomes, withData)
	}
}

// Identical requests that arrive together share VictoriaLogs' bucket reads, for first and last over time and for max
// over time on the same window evaluator. As many cold clients as the proxy's stats_query_range slots
// (-stats-query-range-concurrency, 4 by default) are read once; the clients that wait for a slot come after that read
// and ask only for the buckets not stored yet. The VictoriaLogs answer is held until every client has asked.
//
// conformance: semantics/unwrap-first-last-range-longer-than-step, quality/query-range-extent-cache
func TestFirstLastWindowCoalesced(t *testing.T) {
	s0 := time.Unix(1_700_001_000, 0).UTC()
	rows := queryStepsRows(s0, 4*time.Hour, nil)
	end := s0.Add(3 * time.Hour)
	w := extentWindow{end.Add(-90 * time.Minute), end, 10 * time.Second}
	for _, query := range []string{firstLastCacheQueries[0], firstLastCacheQueries[1], `sum by (app) (max_over_time({env="production"} | logfmt | unwrap amount [5m]))`} {
		for _, clients := range []int{4, 8} {
			t.Run(strconv.Itoa(clients)+"/"+query, func(t *testing.T) {
				emu := (&vlemu.Emulator{T: t, Rows: rows, Field: "amount", Version: "v1.50.0"}).Handler()
				release := make(chan struct{})
				held := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.URL.Path == "/select/logsql/stats_query_range" {
						<-release
					}
					emu.ServeHTTP(w, r)
				})
				srv, shim := newExtentShim(t, held)
				uncachedSrv, uncachedShim := newExtentShim(t, emu)
				_, want, _ := queryStepsGet(t, newExtentTestProxy(t, uncachedSrv.URL, false, 256<<20), query, w)
				_, one := uncachedShim.bucketsRead()
				p := newExtentTestProxy(t, srv.URL, true, 256<<20)
				var wg sync.WaitGroup
				answers := make([][]byte, clients)
				for i := range answers {
					wg.Add(1)
					go func(i int) {
						defer wg.Done()
						_, answers[i], _ = queryStepsGet(t, p, query, w)
					}(i)
				}
				time.Sleep(200 * time.Millisecond) // every client that holds a slot waits on the shared read
				close(release)
				wg.Wait()
				for i, got := range answers {
					if !bytes.Equal(got, want) {
						t.Fatalf("client %d: %s, want %s", i, got, want)
					}
				}
				calls, buckets := shim.bucketsRead()
				t.Logf("%d clients at once: %d stats_query_range calls, %d buckets (one read: %d)", clients, calls, buckets, one)
				// One read per slot round. The clients waiting for a slot read what the first round has not stored yet
				// (its buckets are stored off the request path, so how much depends on that race), never the window
				// once each.
				maxCalls, maxBuckets := 1, one
				if clients > 4 {
					maxCalls, maxBuckets = clients/2-1, 4*one
				}
				if calls > maxCalls || buckets > maxBuckets {
					t.Fatalf("%d identical cold requests made %d stats_query_range calls reading %d buckets, want at most %d and %d", clients, calls, buckets, maxCalls, maxBuckets)
				}
			})
		}
	}
}
