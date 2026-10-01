package proxy

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// backfillVL is a fake VictoriaLogs holding rows only in the last 20 minutes.
// Once backfilled is set it also holds one row at ts (hours ago), as after a
// shipper outage is repaired. It answers row counts and name listings by range
// and counts both kinds of call.
type backfillVL struct {
	backfilled   atomic.Bool
	ts           int64
	counts, scan atomic.Int64
	countDelay   time.Duration // each row count takes this long
}

func (f *backfillVL) server(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/health" {
			w.WriteHeader(http.StatusOK)
			return
		}
		a, _ := parseLokiTimeToUnixNano(r.URL.Query().Get("start"))
		b, ok := parseLokiTimeToUnixNano(r.URL.Query().Get("end"))
		if !ok {
			b = time.Now().UnixNano()
		}
		live := b > time.Now().Add(-20*time.Minute).UnixNano()
		late := f.backfilled.Load() && a <= f.ts && f.ts < b
		if strings.Contains(r.URL.Query().Get("query"), "count()") {
			f.counts.Add(1)
			time.Sleep(f.countDelay)
			n := 0
			if live {
				n += 100
			}
			if late {
				n++
			}
			fmt.Fprintf(w, `{"rows":"%d"}`+"\n", n)
			return
		}
		f.scan.Add(1)
		var hits []fieldHit
		if live {
			hits = append(hits, fieldHit{"app", 100})
		}
		if late {
			hits = append(hits, fieldHit{"backfill_label", 1})
		}
		writeVLFieldNames(w, hits)
	}))
	t.Cleanup(srv.Close)
	return srv
}

// Empty hours cached during a shipper outage, then rows backfilled into them
// with old timestamps: the next near-now request after the negative TTL lists
// them, for one count per run of empty buckets and a rescan of the bucket that
// received the rows only.
//
// conformance: semantics/metadata-answers-include-last-24h-like-loki
func TestMetadataFreshness_BackfilledRowsInCachedEmptyHoursAppearWithinTheNegativeTTL(t *testing.T) {
	vl := &backfillVL{ts: time.Now().Add(-10 * time.Hour).UnixNano()}
	_, mux := newFreshnessProxy(t, vl.server(t).URL, 24*time.Hour)
	path := func() string {
		now := time.Now().UnixNano()
		return fmt.Sprintf("/loki/api/v1/labels?start=%d&end=%d", now-int64(7*24*time.Hour), now)
	}
	if got := getMetadataList(t, mux, path()); contains(got, "backfill_label") {
		t.Fatalf("before the backfill: %v", got)
	}
	coldScans, coldCounts := vl.scan.Load(), vl.counts.Load()

	// Unchanged data, past the negative TTL: one count per run, no bucket scans
	// beyond the live edges.
	restore := cache.AdvanceClockForTesting(metadataNegativeCacheTTL + 5*time.Second)
	defer restore()
	time.Sleep(200 * time.Millisecond)
	_ = getMetadataList(t, mux, path())
	idleScans, idleCounts := vl.scan.Load()-coldScans, vl.counts.Load()-coldCounts
	t.Logf("cold: %d scans, %d counts; unchanged refresh: %d scans, %d counts", coldScans, coldCounts, idleScans, idleCounts)
	if idleCounts > 2 {
		t.Fatalf("unchanged refresh made %d counts, want about one per run of empty buckets", idleCounts)
	}

	// Backfill a row ten hours ago, then refresh after the negative TTL.
	vl.backfilled.Store(true)
	restore2 := cache.AdvanceClockForTesting(metadataNegativeCacheTTL + 5*time.Second)
	defer restore2()
	time.Sleep(200 * time.Millisecond)
	scansBefore, countsBefore := vl.scan.Load(), vl.counts.Load()
	if got := getMetadataList(t, mux, path()); !contains(got, "backfill_label") {
		t.Fatalf("rows backfilled into cached empty hours were not listed: %v", got)
	}
	scans, counts := vl.scan.Load()-scansBefore, vl.counts.Load()-countsBefore
	t.Logf("backfill refresh: %d scans, %d counts", scans, counts)
	if scans > idleScans+1 {
		t.Fatalf("backfill refresh made %d scans (idle refresh %d): more than the one bucket that received rows", scans, idleScans)
	}
	if counts > 12 {
		t.Fatalf("backfill refresh made %d counts", counts)
	}
}

// Rows written with old timestamps into buckets that an earlier request cached
// as empty a moment before are listed by the next near-now request, as Loki
// reads the last max_metadata_cache_freshness live: an hour bucket (counted)
// and a 5-minute bucket more than an hour old (not counted at scan time). The
// earlier request is the same 6h window, so the plan reuses its buckets.
//
// conformance: semantics/metadata-answers-include-last-24h-like-loki
func TestMetadataFreshness_RowsBackfilledIntoJustCachedEmptyBucketsAppearOnTheNextRequest(t *testing.T) {
	now := time.Now()
	// The window starts 7m30s past an hour: an edge, 1m buckets to :10, 5m
	// buckets to the hour, then hours.
	start := now.Add(-6 * time.Hour).Truncate(time.Hour).Add(7*time.Minute + 30*time.Second)
	for _, tc := range []struct {
		name string
		ts   time.Time
	}{
		{name: "hour bucket", ts: now.Add(-3 * time.Hour).Truncate(time.Hour).Add(10 * time.Minute)},
		{name: "5m bucket older than an hour", ts: start.Add(25 * time.Minute)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			vl := &backfillVL{ts: tc.ts.UnixNano()}
			_, mux := newFreshnessProxy(t, vl.server(t).URL, 24*time.Hour)
			path := func() string {
				return fmt.Sprintf("/loki/api/v1/labels?start=%d&end=%d", start.UnixNano(), time.Now().UnixNano())
			}
			if got := getMetadataList(t, mux, path()); contains(got, "backfill_label") {
				t.Fatalf("before the backfill: %v", got)
			}

			// Unchanged data: the refresh confirms the cached empty buckets
			// with about one count per run and rescans none of them.
			time.Sleep(200 * time.Millisecond) // older than max-staleness
			scans, counts := vl.scan.Load(), vl.counts.Load()
			_ = getMetadataList(t, mux, path())
			idleScans, idleCounts := vl.scan.Load()-scans, vl.counts.Load()-counts
			t.Logf("unchanged refresh: %d scans, %d counts", idleScans, idleCounts)
			if idleCounts > 2 {
				t.Fatalf("unchanged refresh made %d counts, want about one per run of empty buckets", idleCounts)
			}

			vl.backfilled.Store(true)
			time.Sleep(200 * time.Millisecond)
			scans, counts = vl.scan.Load(), vl.counts.Load()
			if got := getMetadataList(t, mux, path()); !contains(got, "backfill_label") {
				t.Fatalf("rows backfilled into a bucket cached empty a moment before were not listed: %v", got)
			}
			scans, counts = vl.scan.Load()-scans, vl.counts.Load()-counts
			t.Logf("backfill refresh: %d scans, %d counts", scans, counts)
			if scans > idleScans+1 {
				t.Fatalf("backfill refresh made %d scans (unchanged refresh %d): more than the one bucket that received rows", scans, idleScans)
			}
			if counts > 16 {
				t.Fatalf("backfill refresh made %d counts", counts)
			}
		})
	}
}

// Explore's label browser asks for the values of every label at once: the
// listings of one window need the same counts, and share them instead of
// asking VictoriaLogs once each.
func TestMetadataFreshness_ConcurrentListingsShareTheirRowCounts(t *testing.T) {
	vl := &backfillVL{ts: time.Now().Add(-10 * time.Hour).UnixNano(), countDelay: 100 * time.Millisecond}
	_, mux := newFreshnessProxy(t, vl.server(t).URL, 24*time.Hour)
	window := func() string {
		now := time.Now().UnixNano()
		return fmt.Sprintf("start=%d&end=%d", now-int64(6*time.Hour), now)
	}
	_ = getMetadataList(t, mux, "/loki/api/v1/label/l0/values?"+window()) // caches the empty buckets
	time.Sleep(300 * time.Millisecond)                                    // past the recent-tail staleness
	before := vl.counts.Load()
	_ = getMetadataList(t, mux, "/loki/api/v1/label/l1/values?"+window())
	perListing := vl.counts.Load() - before
	if perListing == 0 {
		t.Fatal("a refresh made no row counts: the test does not exercise the empty runs")
	}
	time.Sleep(300 * time.Millisecond)
	before = vl.counts.Load()
	var wg sync.WaitGroup
	for i := 2; i < 18; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = getMetadataList(t, mux, fmt.Sprintf("/loki/api/v1/label/l%d/values?%s", i, window()))
		}()
	}
	wg.Wait()
	burst := vl.counts.Load() - before
	t.Logf("one listing: %d counts; 16 concurrent listings: %d counts", perListing, burst)
	if burst > 3*perListing {
		t.Fatalf("16 concurrent listings made %d row counts, one listing makes %d: counts are not shared", burst, perListing)
	}
}

// A follower takes a running count only when it was taken after the follower's
// listing began: rows written between would otherwise be missed.
func TestMetadataInventory_CountFollowerNeedsACountNotOlderThanItsListing(t *testing.T) {
	vl := &backfillVL{countDelay: 200 * time.Millisecond}
	p, _ := newFreshnessProxy(t, vl.server(t).URL, 24*time.Hour)
	end := time.Now().Add(-5 * time.Hour).Truncate(time.Hour).UnixNano()
	seg := inventorySegment{start: end - int64(time.Hour), end: end, level: -1}
	count := func(notBefore int64, wg *sync.WaitGroup) {
		defer wg.Done()
		if _, _, err := p.countInventoryRows(context.Background(), "*", seg, notBefore); err != nil {
			t.Error(err)
		}
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go count(0, &wg) // the leader
	time.Sleep(50 * time.Millisecond)
	wg.Add(2)
	go count(0, &wg)                      // needs no particular count: shares the leader's
	go count(cache.Now().UnixNano(), &wg) // began after the leader's count did: counts again
	wg.Wait()
	if got := vl.counts.Load(); got != 2 {
		t.Fatalf("%d row counts, want 2: the leader's, shared with the follower that accepts it, and one for the follower that began later", got)
	}
}

// Listing A's count finds rows in an empty bucket; listing B, whose count ran
// before the rows arrived, then confirms the same entry as empty. A still
// rescans the bucket: its decision is kept in memory, not in the cache entry
// B rewrote.
func TestMetadataInventory_RescanDecisionSurvivesAnotherListingConfirmingTheEntry(t *testing.T) {
	vl := &backfillVL{ts: time.Now().Add(-3 * time.Hour).UnixNano()}
	p, _ := newFreshnessProxy(t, vl.server(t).URL, 24*time.Hour)
	ctx := context.Background()
	const path = "/select/logsql/stream_field_names"
	now := time.Now()
	params := url.Values{"query": {"*"}, "start": {fmt.Sprint(now.Add(-6 * time.Hour).UnixNano())}, "end": {fmt.Sprint(now.UnixNano())}}
	if _, err := p.fetchVLListing(ctx, path, params); err != nil { // caches the empty buckets
		t.Fatal(err)
	}
	vl.backfilled.Store(true)
	plan, _ := p.inventoryPlan(path, params, now)
	base := p.inventoryKeyBase(ctx, path, params)
	rescan := p.revalidateEmptyRuns(ctx, path, params, base, plan)
	if len(rescan) != 1 {
		t.Fatalf("rescan set %v, want the one bucket that received the row", rescan)
	}
	var seg inventorySegment
	for _, s := range plan {
		if s.level >= 0 {
			if _, ok := rescan[inventoryBucketKey(base, s)]; ok {
				seg = s
			}
		}
	}
	// Listing B confirms the entry as empty and fresh.
	key := inventoryBucketKey(base, seg)
	entry, _ := p.loadInventoryEntry(key)
	entry.Rows, entry.Checked = 0, cache.Now().UnixNano()
	if err := p.storeInventoryEntry(key, entry); err != nil {
		t.Fatal(err)
	}
	items, _, err := p.fetchInventorySegment(ctx, path, params, base, seg, true, false, false)
	if err != nil || len(items) != 0 {
		t.Fatalf("without the in-memory decision the confirmed entry is served: %v, %v", items, err)
	}
	items, _, err = p.fetchInventorySegment(ctx, path, params, base, seg, true, false, true)
	if err != nil || len(items) == 0 || items[0].Value != "backfill_label" {
		t.Fatalf("forced rescan did not list the backfilled label: %v, %v", items, err)
	}
}
