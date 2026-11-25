package proxy

import (
	"context"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// newMemoryInventoryProxy is newInventoryTestProxy with the bounded staleness
// of -metadata-max-staleness on (the flag's default is 10s), which lets a
// names-only listing be answered from memory.
func newMemoryInventoryProxy(t *testing.T, vl *fakeVictoriaLogs, staleness time.Duration) *Proxy {
	t.Helper()
	p := newInventoryTestProxy(t, vl, 4)
	p.metadataMaxStaleness = staleness
	p.metadataInventoryPrefetch = true
	p.metadataCacheFreshness = 24 * time.Hour
	return p
}

// settleBackground waits until no background inventory read of p is running,
// as seen three polls in a row (a read starts a moment after the listing that
// asks for it returns).
func settleBackground(t testing.TB, p *Proxy) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for idle := 0; idle < 3; {
		if time.Now().After(deadline) {
			t.Fatal("background inventory reads did not finish")
		}
		if p.inventoryBackgroundRunning.Load() == 0 {
			idle++
		} else {
			idle = 0
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// namesOnlyListing lists the names of [start, end] as /labels asks for them,
// below the response caches.
func namesOnlyListing(t testing.TB, p *Proxy, start, end time.Time) []string {
	t.Helper()
	ctx := context.WithValue(withNamesOnlyListing(context.Background()), labelCacheBypassKey{}, true)
	items, err := p.fetchVLListing(ctx, "/select/logsql/stream_field_names", url.Values{"query": {"*"}, "start": {strconv.FormatInt(start.UnixNano(), 10)}, "end": {strconv.FormatInt(end.UnixNano(), 10)}})
	if err != nil {
		t.Fatal(err)
	}
	names := listingValues(items)
	sort.Strings(names)
	return names
}

// dayNames are the names of the rows of every UTC day [start, end) touches
// whose part of the window holds a row: Loki's /labels for stored data.
func dayNames(f *fakeVictoriaLogs, start, end time.Time) map[string]bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := map[string]bool{}
	for d := start.Truncate(24 * time.Hour); d.Before(end); d = d.Add(24 * time.Hour) {
		lo, hi := d.UnixNano(), d.Add(24*time.Hour).UnixNano()
		if start.UnixNano() > lo {
			lo = start.UnixNano()
		}
		if end.UnixNano() < hi {
			hi = end.UnixNano()
		}
		holds := false
		for _, r := range f.rows {
			if r.ts >= lo && r.ts < hi {
				holds = true
				break
			}
		}
		if !holds {
			continue
		}
		for _, r := range f.rows {
			if r.ts >= d.UnixNano() && r.ts < d.Add(24*time.Hour).UnixNano() {
				for k := range r.stream {
					out[k] = true
				}
			}
		}
	}
	return out
}

func listingBase(p *Proxy) string {
	return p.inventoryKeyBase(context.Background(), "/select/logsql/stream_field_names", url.Values{"query": {"*"}})
}

// The A/B shape "refresh labels (label browser)" moves its window back by an
// hour (24h) or by 317 s (1h, 6h) per run. Once the first window has been
// read, the moved windows are answered from memory with no VictoriaLogs call:
// each piece of an end comes from its own entry or from the smallest cached
// bucket holding it (for the far end of a window moved back by an hour, the
// hour the previous listing read in the background). Every answer lists every name of
// one full-range call, and no name beyond those of the UTC days it touches.
//
// conformance: semantics/label-browser-latency-cold-and-refresh, semantics/label-inventory-exact-and-incremental, loki_api_v1_labels
func TestLabelsInventory_MovedWindowsAreAnsweredFromMemory(t *testing.T) {
	now := time.Now()
	vl := &fakeVictoriaLogs{}
	seedStableLabels(vl, now)
	end0 := now.Truncate(time.Hour).Add(-2*time.Hour + 26*time.Minute + 50*time.Second)
	for _, tc := range []struct {
		width, shift time.Duration
	}{{24 * time.Hour, 3617 * time.Second}, {6 * time.Hour, 317 * time.Second}, {time.Hour, 317 * time.Second}} {
		t.Run(tc.width.String(), func(t *testing.T) {
			p := newMemoryInventoryProxy(t, vl, 10*time.Second)
			full := newInventoryTestProxy(t, vl, -1)
			_ = namesOnlyListing(t, p, end0.Add(-tc.width), end0)
			settleBackground(t, p) // the hours the moved windows start in
			// The first moved window may still read its new ends; the next
			// ones are answered from memory.
			_ = namesOnlyListing(t, p, end0.Add(-tc.shift-tc.width), end0.Add(-tc.shift))
			settleBackground(t, p)
			for run := 2; run <= 3; run++ {
				end := end0.Add(-time.Duration(run) * tc.shift)
				vl.reset()
				got := namesOnlyListing(t, p, end.Add(-tc.width), end)
				calls, _ := vl.reset()
				if n := calls["/select/logsql/stream_field_names"] + calls["/select/logsql/query"]; n != 0 {
					t.Fatalf("run %d made %d VictoriaLogs calls (%v), want an answer from memory", run, n, calls)
				}
				exact := namesOnlyListing(t, full, end.Add(-tc.width), end)
				allowed := dayNames(vl, end.Add(-tc.width), end.Add(time.Millisecond))
				set := map[string]bool{}
				for _, n := range got {
					set[n] = true
					if !allowed[n] {
						t.Fatalf("run %d listed %q, which no UTC day of the window holds", run, n)
					}
				}
				for _, n := range exact {
					if !set[n] {
						t.Fatalf("run %d lost %q: memory %v, one call %v", run, n, got, exact)
					}
				}
			}
		})
	}
}

// The memory answer may list names that the window's UTC day holds outside
// the window (Loki lists the day's index), but never a name of another day,
// and never a day's names when the window's part of that day holds no row
// (Loki answers such a window with no names). Random windows over rows with
// labels of their own, compared with one full-range call and the days' names.
//
// conformance: semantics/label-inventory-exact-and-incremental, loki_api_v1_labels
func TestLabelsInventory_MemoryAnswersStayBetweenTheWindowAndItsDays(t *testing.T) {
	rng := rand.New(rand.NewPCG(5, 8))
	now := time.Now()
	for trial := 0; trial < 10; trial++ {
		vl := &fakeVictoriaLogs{}
		seedRows(vl, now, rng)
		for i := 0; i < 8; i++ {
			ts := now.Add(-time.Minute - time.Duration(rng.Int64N(int64(50*time.Hour))))
			vl.add(fakeVLRow{ts: ts.UnixNano(), stream: map[string]string{"only_" + strconv.Itoa(i): "x"}})
		}
		p := newMemoryInventoryProxy(t, vl, 10*time.Second)
		full := newInventoryTestProxy(t, vl, -1)
		width := []time.Duration{time.Hour, 6 * time.Hour, 24 * time.Hour, 3*time.Hour + 17*time.Minute}[trial%4]
		end := now.Add(-2*time.Minute - time.Duration(rng.Int64N(int64(26*time.Hour))))
		memory := 0
		for step := 0; step < 8; step++ {
			start := end.Add(-width)
			before := counterValue(p, "memory")
			got := namesOnlyListing(t, p, start, end)
			if counterValue(p, "memory") > before {
				memory++
			}
			exact := namesOnlyListing(t, full, start, end)
			allowed := dayNames(vl, start, end.Add(time.Millisecond))
			set := map[string]bool{}
			for _, n := range got {
				set[n] = true
				if !allowed[n] {
					t.Fatalf("trial %d step %d: %q is held by no UTC day of [%s, %s] that has rows in it", trial, step, n, start.UTC().Format(time.RFC3339), end.UTC().Format(time.RFC3339))
				}
			}
			for _, n := range exact {
				if !set[n] {
					t.Fatalf("trial %d step %d: lost %q (memory %v, one call %v)", trial, step, n, got, exact)
				}
			}
			settleBackground(t, p)
			if rng.IntN(2) == 0 {
				end = end.Add(-time.Duration(317+rng.IntN(3600)) * time.Second)
			} else {
				end = end.Add(-time.Duration(1+rng.IntN(120)) * time.Second)
			}
		}
		t.Logf("trial %d (%s): %d of 8 answered from memory", trial, width, memory)
	}
}

// counterValue reads the metadata inventory outcome counter of p.
func counterValue(p *Proxy, outcome string) float64 {
	w := httptest.NewRecorder()
	p.metrics.Handler(w, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	prefix := `loki_vl_proxy_internal_operation_total{operation="metadata_inventory",outcome="` + outcome + `"} `
	for _, line := range strings.Split(w.Body.String(), "\n") {
		if v, ok := strings.CutPrefix(line, prefix); ok {
			f, _ := strconv.ParseFloat(v, 64)
			return f
		}
	}
	return 0
}

// An entry inside the live window serves a memory answer only while a scan or
// a row count read it within -metadata-max-staleness. Past that the listing
// reads VictoriaLogs, and the entries the memory answer could not use are
// confirmed in the background, so the next listing is again answered from
// memory; a row written into a covered hour is listed once a confirmation
// finds the changed count.
//
// conformance: quality/labels-backfill-into-cached-bucket, semantics/label-inventory-exact-and-incremental
func TestLabelsInventory_MemoryAnswersKeepTheStalenessBound(t *testing.T) {
	now := time.Now()
	vl := &fakeVictoriaLogs{}
	seedStableLabels(vl, now)
	p := newMemoryInventoryProxy(t, vl, 300*time.Millisecond)
	p.recentTailRefreshMaxStaleness = 150 * time.Millisecond // row counts reused that long
	end := now.Truncate(time.Hour).Add(-2*time.Hour + 26*time.Minute + 50*time.Second)
	start := end.Add(-6 * time.Hour)
	_ = namesOnlyListing(t, p, start, end)
	settleBackground(t, p) // the hours of the ends, read in the background
	vl.reset()
	_ = namesOnlyListing(t, p, start.Add(-time.Second), end.Add(-time.Second))
	if calls, _ := vl.reset(); len(calls) != 0 {
		t.Fatalf("within the bound: %v, want an answer from memory", calls)
	}
	restore := cache.AdvanceClockForTesting(400 * time.Millisecond) // past the bound
	defer restore()
	_ = namesOnlyListing(t, p, start.Add(-2*time.Second), end.Add(-2*time.Second))
	if calls, _ := vl.reset(); len(calls) == 0 {
		t.Fatal("past the bound the listing was answered from memory without a confirmation")
	}

	// A row with a new label written into a settled hour of the window.
	hour := end.Truncate(time.Hour).Add(-3 * time.Hour)
	vl.add(fakeVLRow{ts: hour.Add(17 * time.Minute).UnixNano(), stream: map[string]string{"app": "api", "late": "1"}})
	// Each listing moves the window by a second; the clock moves past the
	// bound and the count reuse between two listings.
	for i := 3; i < 12; i++ {
		before := counterValue(p, "memory")
		got := namesOnlyListing(t, p, start.Add(-time.Duration(i)*time.Second), end.Add(-time.Duration(i)*time.Second))
		calls, _ := vl.reset()
		t.Logf("listing %d: from memory %v, VictoriaLogs calls %v", i, counterValue(p, "memory") > before, calls)
		if contains(got, "late") {
			return
		}
		settleBackground(t, p)
		defer cache.AdvanceClockForTesting(200 * time.Millisecond)()
	}
	t.Fatal("a row written into a covered hour was never listed")
}

// Off when -metadata-max-staleness is 0: every listing reads VictoriaLogs as
// before and no day is filled in the background.
//
// conformance: semantics/label-inventory-exact-and-incremental
func TestLabelsInventory_MemoryAnswersOffWithoutStaleness(t *testing.T) {
	now := time.Now()
	vl := &fakeVictoriaLogs{}
	seedStableLabels(vl, now)
	p := newMemoryInventoryProxy(t, vl, 0)
	end := now.Truncate(time.Hour).Add(-2*time.Hour + 26*time.Minute + 50*time.Second)
	_ = namesOnlyListing(t, p, end.Add(-24*time.Hour), end)
	settleBackground(t, p)
	day := end.Add(-24 * time.Hour).Truncate(24 * time.Hour)
	if _, ok := p.cache.Peek(inventoryBucketKey(listingBase(p), inventorySegment{start: day.UnixNano(), end: day.Add(24 * time.Hour).UnixNano(), level: 0})); ok {
		t.Fatal("a day was filled in the background with -metadata-max-staleness=0")
	}
	vl.reset()
	_ = namesOnlyListing(t, p, end.Add(-24*time.Hour-time.Second), end.Add(-time.Second))
	if calls, _ := vl.reset(); len(calls) == 0 {
		t.Fatal("answered from memory with -metadata-max-staleness=0")
	}
	if counterValue(p, "memory") != 0 {
		t.Fatal("memory answers counted with -metadata-max-staleness=0")
	}
}

// The background reads after a listing span at most the window's own width
// and are hours only: a 10-minute window over the end of yesterday reads
// nothing in the background, an hour reads one hour, six hours and a day at
// most three hours; with -metadata-inventory-prefetch false nothing is read.
//
// conformance: semantics/label-inventory-exact-and-incremental, limits/concurrent-full-retention-scans-exhaust-backend
func TestLabelsInventory_PrefetchStaysWithinTheWindowWidth(t *testing.T) {
	now := time.Now()
	vl := &fakeVictoriaLogs{}
	seedStableLabels(vl, now)
	midnight := now.Truncate(24 * time.Hour)
	if now.Sub(midnight) < 2*time.Hour {
		midnight = midnight.Add(-24 * time.Hour) // keep every window sealed
	}
	for _, tc := range []struct {
		name       string
		start, end time.Time
		prefetch   bool
	}{
		{"10m over the end of yesterday", midnight.Add(-10*time.Minute - 13*time.Second), midnight.Add(-13 * time.Second), true},
		{"1h", midnight.Add(-time.Hour - 13*time.Minute), midnight.Add(-13 * time.Minute), true},
		{"6h", midnight.Add(-6*time.Hour - 13*time.Minute), midnight.Add(-13 * time.Minute), true},
		{"24h", midnight.Add(-24*time.Hour - 13*time.Minute), midnight.Add(-13 * time.Minute), true},
		{"24h, prefetch off", midnight.Add(-24*time.Hour - 13*time.Minute), midnight.Add(-13 * time.Minute), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := newMemoryInventoryProxy(t, vl, 10*time.Second)
			p.metadataInventoryPrefetch = tc.prefetch
			params := url.Values{"query": {"*"}, "start": {strconv.FormatInt(tc.start.UnixNano(), 10)}, "end": {strconv.FormatInt(tc.end.UnixNano(), 10)}}
			plan := planInventorySegments(tc.start.UnixNano(), tc.end.UnixNano(), now.Add(-metadataInventorySealLag).UnixNano())
			vl.reset()
			p.fillTouchedDays(withNamesOnlyListing(context.Background()), "/select/logsql/stream_field_names", params, listingBase(p), plan, now, true)
			settleBackground(t, p)
			calls, spans := vl.reset()
			var read time.Duration
			for _, s := range spans {
				read += s
			}
			width := tc.end.Sub(tc.start)
			t.Logf("%s: background listings %v (%s), calls %v", tc.name, spans, read, calls)
			if read > width {
				t.Fatalf("the background read %s for a window of %s", read, width)
			}
			if (tc.name == "10m over the end of yesterday" || !tc.prefetch) && len(calls) != 0 {
				t.Fatalf("background calls %v, want none", calls)
			}
			if tc.name == "24h" && read != 3*time.Hour {
				t.Fatalf("the 24h window read %s in the background, want the hours of its ends and the hour before it", read)
			}
		})
	}
}
