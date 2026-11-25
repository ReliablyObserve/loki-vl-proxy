package proxy

import (
	"context"
	"math/rand/v2"
	"net/url"
	"strconv"
	"testing"
	"time"
)

// The label browser's regression gates (registry
// semantics/label-browser-moved-window-from-memory). /labels is the first
// request a Grafana user makes against Loki, so its answer path is held by
// deterministic checks that do not depend on timing: the A/B shape "refresh
// labels (label browser)" windows at 1h, 6h and 24h, moved back by 317 s or
// 3617 s per run. Each gate also runs with the optimisation off and must then
// see its check fail: a gate that cannot fail proves nothing.

// labelBrowserShapes are the A/B shape's ranges and per-run moves.
var labelBrowserShapes = []struct {
	width, shift time.Duration
}{{time.Hour, 317 * time.Second}, {6 * time.Hour, 317 * time.Second}, {24 * time.Hour, 3617 * time.Second}}

// labelBrowserEnd is a sealed, unaligned window end at a fixed place in an
// hour two hours back, so every run plans the same pieces.
func labelBrowserEnd(now time.Time) time.Time {
	return now.Truncate(time.Hour).Add(-2*time.Hour + 26*time.Minute + 50*time.Second)
}

// movedWindowCalls reads the shape's first window, then three moved windows,
// each after the previous listing's background reads finished, and returns
// the request-path VictoriaLogs calls of each moved window.
func movedWindowCalls(t *testing.T, p *Proxy, end0 time.Time, width, shift time.Duration) []int {
	t.Helper()
	_ = namesOnlyListing(t, p, end0.Add(-width), end0)
	var calls []int
	for run := 1; run <= 3; run++ {
		settleBackground(t, p)
		end := end0.Add(-time.Duration(run) * shift)
		_, n := namesOnlyListingCalls(t, p, end.Add(-width), end)
		calls = append(calls, n)
	}
	settleBackground(t, p)
	return calls
}

// Gate: a warm moved window of the label browser makes no VictoriaLogs call
// on its request path, at 1h, 6h and 24h. With -metadata-max-staleness=0 (no
// memory answers) or -metadata-inventory-prefetch=false (no background reads)
// the same windows read VictoriaLogs: the gate goes red without the
// optimisation.
//
// conformance: semantics/label-browser-moved-window-from-memory, semantics/label-browser-latency-cold-and-refresh, loki_api_v1_labels
func TestLabelBrowserGates_MovedWindowNoRequestPathCalls(t *testing.T) {
	now := time.Now()
	vl := &fakeVictoriaLogs{}
	seedStableLabels(vl, now)
	end0 := labelBrowserEnd(now)
	for _, tc := range labelBrowserShapes {
		t.Run(tc.width.String(), func(t *testing.T) {
			p := newMemoryInventoryProxy(t, vl, 10*time.Second)
			for run, n := range movedWindowCalls(t, p, end0, tc.width, tc.shift) {
				if n != 0 {
					t.Fatalf("moved window %d made %d VictoriaLogs calls on its request path, want 0", run+1, n)
				}
			}
		})
		t.Run(tc.width.String()+"/without memory answers", func(t *testing.T) {
			p := newMemoryInventoryProxy(t, vl, 0)
			if sum(movedWindowCalls(t, p, end0, tc.width, tc.shift)) == 0 {
				t.Fatal("with -metadata-max-staleness=0 the moved windows made no VictoriaLogs call: the gate cannot fail")
			}
		})
	}
	// The 1h window's next start lies in an hour only the prefetch reads.
	t.Run("1h/without prefetch", func(t *testing.T) {
		p := newMemoryInventoryProxy(t, vl, 10*time.Second)
		p.metadataInventoryPrefetch = false
		if sum(movedWindowCalls(t, p, end0, time.Hour, 317*time.Second)) == 0 {
			t.Fatal("with -metadata-inventory-prefetch=false the moved 1h windows made no VictoriaLogs call: the gate cannot fail")
		}
	})
}

func sum(xs []int) int {
	n := 0
	for _, x := range xs {
		n += x
	}
	return n
}

// Gate: the background reads a listing starts span at most the window's own
// width, in hours: a 10-minute window reads nothing in the background, 1h one
// hour, 6h and 24h at most three. With -metadata-inventory-prefetch=false
// nothing is read.
//
// conformance: semantics/label-browser-moved-window-from-memory, limits/concurrent-full-retention-scans-exhaust-backend
func TestLabelBrowserGates_BackgroundReadsWithinTheWindow(t *testing.T) {
	now := time.Now()
	vl := &fakeVictoriaLogs{}
	seedStableLabels(vl, now)
	end := labelBrowserEnd(now)
	for _, tc := range []struct {
		width, max time.Duration
		prefetch   bool
	}{
		{10 * time.Minute, 0, true},
		{time.Hour, time.Hour, true},
		{6 * time.Hour, 3 * time.Hour, true},
		{24 * time.Hour, 3 * time.Hour, true},
		{24 * time.Hour, 0, false},
	} {
		t.Run(tc.width.String()+"/prefetch="+strconv.FormatBool(tc.prefetch), func(t *testing.T) {
			p := newMemoryInventoryProxy(t, vl, 10*time.Second)
			p.metadataInventoryPrefetch = tc.prefetch
			start := end.Add(-tc.width)
			params := url.Values{"query": {"*"}, "start": {strconv.FormatInt(start.UnixNano(), 10)}, "end": {strconv.FormatInt(end.UnixNano(), 10)}}
			plan := planInventorySegments(start.UnixNano(), end.UnixNano(), now.Add(-metadataInventorySealLag).UnixNano())
			vl.reset()
			p.fillTouchedDays(withNamesOnlyListing(context.Background()), "/select/logsql/stream_field_names", params, listingBase(p), plan, now, true)
			settleBackground(t, p)
			_, spans := vl.reset()
			var read time.Duration
			for _, s := range spans {
				read += s
			}
			if read > tc.max {
				t.Fatalf("a %s window read %s in the background, want at most %s", tc.width, read, tc.max)
			}
		})
	}
}

// Gate: every answer of the label browser's windows lists every name of one
// full-range call, and only names of the UTC days the window touches whose
// part of the window holds rows (Loki's day granularity), over the A/B
// shape's moved windows and random ones with labels of their own.
//
// conformance: semantics/label-browser-moved-window-from-memory, semantics/label-inventory-exact-and-incremental, loki_api_v1_labels
func TestLabelBrowserGates_AnswersBetweenExactAndTheDays(t *testing.T) {
	now := time.Now()
	rng := rand.New(rand.NewPCG(31, 37))
	vl := &fakeVictoriaLogs{}
	seedRows(vl, now, rng)
	for i := 0; i < 10; i++ {
		ts := now.Add(-2*time.Minute - time.Duration(rng.Int64N(int64(40*time.Hour))))
		vl.add(fakeVLRow{ts: ts.UnixNano(), stream: map[string]string{"gate_only_" + strconv.Itoa(i): "x"}})
	}
	full := newInventoryTestProxy(t, vl, -1)
	end0 := labelBrowserEnd(now)
	for _, tc := range labelBrowserShapes {
		p := newMemoryInventoryProxy(t, vl, 10*time.Second)
		ends := []time.Time{end0}
		for run := 1; run <= 6; run++ {
			ends = append(ends, end0.Add(-time.Duration(run)*tc.shift))
		}
		for k := 0; k < 4; k++ {
			ends = append(ends, now.Add(-2*time.Minute-time.Duration(rng.Int64N(int64(30*time.Hour)))))
		}
		for _, end := range ends {
			start := end.Add(-tc.width)
			got := namesOnlyListing(t, p, start, end)
			settleBackground(t, p)
			exact := namesOnlyListing(t, full, start, end)
			allowed := dayNames(vl, start, end.Add(time.Millisecond))
			set := map[string]bool{}
			for _, n := range got {
				set[n] = true
				if !allowed[n] {
					t.Fatalf("%s window ending %s listed %q, which no UTC day of the window with rows holds", tc.width, end.UTC().Format(time.RFC3339), n)
				}
			}
			for _, n := range exact {
				if !set[n] {
					t.Fatalf("%s window ending %s lost %q: %v, one call %v", tc.width, end.UTC().Format(time.RFC3339), n, got, exact)
				}
			}
		}
	}
}

// Gate: a window whose part of a day holds no row gets none of the day's
// names, even when a cached bucket holding its pieces has names: Loki lists
// nothing for a window after the day's last row (measured on Loki 3.7.10).
//
// conformance: semantics/label-browser-moved-window-from-memory, semantics/label-inventory-exact-and-incremental
func TestLabelBrowserGates_WindowWithoutRowsListsNothing(t *testing.T) {
	now := time.Now()
	hour := now.Truncate(time.Hour).Add(-5 * time.Hour)
	vl := &fakeVictoriaLogs{}
	for ts := hour.Add(-3 * time.Hour); ts.Before(hour.Add(7 * time.Minute)); ts = ts.Add(20 * time.Second) {
		vl.add(fakeVLRow{ts: ts.UnixNano(), stream: map[string]string{"app": "api", "env": "prod"}})
	}
	p := newMemoryInventoryProxy(t, vl, 10*time.Second)
	// A listing that reads the hour whole: it is cached with its names.
	_ = namesOnlyListing(t, p, hour.Add(-2*time.Hour), hour.Add(2*time.Hour))
	settleBackground(t, p)
	// After the last row: the window's pieces in that hour are held by it.
	start, end := hour.Add(30*time.Minute+13*time.Second), hour.Add(90*time.Minute+13*time.Second)
	if got := namesOnlyListing(t, p, start, end); len(got) != 0 {
		t.Fatalf("a window after the last row listed %v, want none", got)
	}
}
