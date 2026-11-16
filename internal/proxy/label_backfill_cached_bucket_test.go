package proxy

import (
	"fmt"
	"testing"
	"time"
)

// Rows written with old timestamps into an hour bucket the label inventory
// already holds with data are listed by the next near-now request after the
// recent-tail staleness, as Loki lists them as soon as they are ingested: one
// row count over the run of counted buckets finds more rows than the buckets
// held, the halving finds the bucket, and only that bucket is rescanned. A
// repeat inside the staleness sends no count, and a refresh of unchanged data
// sends one count per run and no bucket scan.
//
// conformance: quality/labels-backfill-into-cached-bucket, semantics/metadata-answers-include-last-24h-like-loki
func TestMetadataFreshness_RowsBackfilledIntoACachedNonEmptyHourAppearOnTheNextRequest(t *testing.T) {
	now := time.Now()
	start := now.Add(-6 * time.Hour).Truncate(time.Hour).Add(7*time.Minute + 30*time.Second)
	end := sealedWindowEnd(now)
	hour := now.Add(-3 * time.Hour).Truncate(time.Hour)
	vl := &backfillVL{held: hour.Add(5 * time.Minute).UnixNano(), ts: hour.Add(40 * time.Minute).UnixNano()}
	_, mux := newFreshnessProxy(t, vl.server(t).URL, 24*time.Hour)
	path := fmt.Sprintf("/loki/api/v1/labels?start=%d&end=%d", start.UnixNano(), end.UnixNano())
	edges := inventoryEdgeCount(start, end)

	got := getMetadataList(t, mux, path)
	if !contains(got, "held_label") || contains(got, "backfill_label") {
		t.Fatalf("before the backfill: %v", got)
	}

	// A repeat inside the staleness: the answer is the endpoint's cached one.
	scans, counts := vl.scan.Load(), vl.counts.Load()
	_ = getMetadataList(t, mux, path)
	if d := vl.scan.Load() - scans + vl.counts.Load() - counts; d != 0 {
		t.Fatalf("a repeat inside the staleness sent %d VictoriaLogs calls, want 0", d)
	}

	// Unchanged data past the staleness: one count per run, only the edges.
	time.Sleep(200 * time.Millisecond)
	scans, counts = vl.scan.Load(), vl.counts.Load()
	_ = getMetadataList(t, mux, path)
	idleScans, idleCounts := vl.scan.Load()-scans, vl.counts.Load()-counts
	t.Logf("unchanged refresh: %d scans, %d counts (%d edges)", idleScans, idleCounts, edges)
	if idleScans != edges {
		t.Fatalf("unchanged refresh made %d scans, want only the plan's %d uncached edges", idleScans, edges)
	}
	if idleCounts == 0 || idleCounts > 2 {
		t.Fatalf("unchanged refresh made %d counts, want about one per run of counted buckets", idleCounts)
	}

	// The backfill: the next request after the staleness lists it.
	vl.backfilled.Store(true)
	time.Sleep(200 * time.Millisecond)
	scans, counts = vl.scan.Load(), vl.counts.Load()
	got = getMetadataList(t, mux, path)
	if !contains(got, "backfill_label") || !contains(got, "held_label") {
		t.Fatalf("rows backfilled into a cached hour that held data were not listed: %v", got)
	}
	scans, counts = vl.scan.Load()-scans, vl.counts.Load()-counts
	t.Logf("backfill refresh: %d scans, %d counts", scans, counts)
	if scans != edges+1 {
		t.Fatalf("backfill refresh made %d scans, want the %d uncached edges and the one bucket that received rows", scans, edges)
	}
	if counts > 16 {
		t.Fatalf("backfill refresh made %d counts", counts)
	}

	// After the rescan the bucket is confirmed again: the next refresh sends no scan but the edges.
	time.Sleep(200 * time.Millisecond)
	scans = vl.scan.Load()
	if got := getMetadataList(t, mux, path); !contains(got, "backfill_label") {
		t.Fatalf("the backfilled label disappeared: %v", got)
	}
	if d := vl.scan.Load() - scans; d != edges {
		t.Fatalf("the refresh after the rescan made %d scans, want only the %d edges", d, edges)
	}
}
