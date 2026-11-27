package proxy

import (
	"context"
	"math/rand/v2"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
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
//
// The gates run at pinned instants, never at the wall clock's: which pieces of
// a moved window are cached depends on where the window lies in its hour and
// its UTC day. TestLabelBrowserGates_MovedWindowsAtHourAndDayBoundaries holds
// the two places where a moved window still reads VictoriaLogs.

// labelBrowserShapes are the A/B shape's ranges and per-run moves.
var labelBrowserShapes = []struct {
	width, shift time.Duration
}{{time.Hour, 317 * time.Second}, {6 * time.Hour, 317 * time.Second}, {24 * time.Hour, 3617 * time.Second}}

// labelBrowserEnd is the pinned window end hour:minute:50 of a past UTC day:
// sealed and unaligned.
func labelBrowserEnd(hour, minute int) time.Time {
	return time.Date(2026, 3, 10, hour, minute, 50, 0, time.UTC)
}

// labelBrowserClearEnds are pinned ends at which every moved window of every
// shape is answered from memory.
var labelBrowserClearEnds = []time.Time{
	labelBrowserEnd(4, 26), labelBrowserEnd(12, 26), labelBrowserEnd(18, 45), labelBrowserEnd(23, 26),
}

// pinnedNow is the clock a window ending at end0 is asked at: two hours later.
func pinnedNow(end0 time.Time) time.Time { return end0.Add(2 * time.Hour) }

// newPinnedLabelProxy is newMemoryInventoryProxy for windows pinned around
// now: its live window reaches 24 hours before now, as the shipped
// -max-metadata-cache-freshness does from the wall clock. Bucket revalidation
// ages still follow the wall clock; an entry read during the test is fresh on
// every schedule.
func newPinnedLabelProxy(t *testing.T, vl *fakeVictoriaLogs, staleness time.Duration, now time.Time) *Proxy {
	t.Helper()
	p := newMemoryInventoryProxy(t, vl, staleness)
	p.metadataCacheFreshness = time.Since(now) + 24*time.Hour
	return p
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
// on its request path, at 1h, 6h and 24h, away from the hour and UTC day
// boundaries of MovedWindowsAtHourAndDayBoundaries. With
// -metadata-max-staleness=0 (no memory answers) or
// -metadata-inventory-prefetch=false (no background reads) the same windows
// read VictoriaLogs: the gate goes red without the optimisation.
//
// conformance: semantics/label-browser-moved-window-from-memory, semantics/label-browser-latency-cold-and-refresh, loki_api_v1_labels
func TestLabelBrowserGates_MovedWindowNoRequestPathCalls(t *testing.T) {
	for _, end0 := range labelBrowserClearEnds {
		now := pinnedNow(end0)
		vl := &fakeVictoriaLogs{}
		seedStableLabels(vl, now)
		at := end0.Format("15:04")
		for _, tc := range labelBrowserShapes {
			t.Run(at+"/"+tc.width.String(), func(t *testing.T) {
				p := newPinnedLabelProxy(t, vl, 10*time.Second, now)
				for run, n := range movedWindowCalls(t, p, end0, tc.width, tc.shift) {
					if n != 0 {
						t.Fatalf("moved window %d made %d VictoriaLogs calls on its request path, want 0", run+1, n)
					}
				}
			})
			t.Run(at+"/"+tc.width.String()+"/without memory answers", func(t *testing.T) {
				p := newPinnedLabelProxy(t, vl, 0, now)
				if sum(movedWindowCalls(t, p, end0, tc.width, tc.shift)) == 0 {
					t.Fatal("with -metadata-max-staleness=0 the moved windows made no VictoriaLogs call: the gate cannot fail")
				}
			})
		}
		// The 1h window's next start lies in an hour only the prefetch reads.
		t.Run(at+"/1h/without prefetch", func(t *testing.T) {
			p := newPinnedLabelProxy(t, vl, 10*time.Second, now)
			p.metadataInventoryPrefetch = false
			if sum(movedWindowCalls(t, p, end0, time.Hour, 317*time.Second)) == 0 {
				t.Fatal("with -metadata-inventory-prefetch=false the moved 1h windows made no VictoriaLogs call: the gate cannot fail")
			}
		})
	}
}

// Gate: where a moved window is not answered from memory, and why. Two cases,
// at every hour or around every UTC midnight:
//
//   - "no_rows_evidence": the window's part of a UTC day is shorter than an
//     hour and holds no bucket of its own that an earlier listing read, so
//     nothing in memory proves it has rows, and a window without rows in a day
//     must not get that day's names. A 24h window meets this when an end lies
//     between 23:00 and 01:00, a 6h or 1h window when it has just crossed
//     midnight or its start is about to.
//   - "uncached": the moved window starts in an hour that no earlier listing
//     read whole or in the background, because the background reads span at most
//     the window's width: a 1h window (one hour read per listing) whose start
//     moves back into an hour not read yet, a 24h window whose start moves back
//     over two hour boundaries.
//
// Both conditions are necessary, not sufficient: many windows that meet them
// are still answered from memory, from buckets earlier listings read.
//
// Such a window takes the path that reads VictoriaLogs (for the pieces not
// cached: none when an earlier moved window already read them) and still lists
// every name of one full-range call. A moved window of the same row that needs
// neither is answered from memory. The reasons and the calls are exact, so a
// change in either direction shows here; a change that answers one of these
// from memory turns its entry into memory and 0.
//
// conformance: semantics/label-browser-moved-window-from-memory, semantics/label-inventory-exact-and-incremental, loki_api_v1_labels
func TestLabelBrowserGates_MovedWindowsAtHourAndDayBoundaries(t *testing.T) {
	const memory, thinDay, unreadHour = "", "no_rows_evidence", "uncached"
	hour, six, day := labelBrowserShapes[0], labelBrowserShapes[1], labelBrowserShapes[2]
	for _, tc := range []struct {
		shape struct{ width, shift time.Duration }
		end0  time.Time
		want  [3]string // moved windows 1 to 3: the memory miss reason
		calls [3]int    // their request-path VictoriaLogs calls
	}{
		{day, labelBrowserEnd(0, 0), [3]string{thinDay, memory, unreadHour}, [3]int{1, 0, 2}},
		{day, labelBrowserEnd(0, 26), [3]string{thinDay, memory, memory}, [3]int{1, 0, 0}}, // the wall clock at 02:27 UTC
		{day, labelBrowserEnd(1, 26), [3]string{thinDay, thinDay, memory}, [3]int{1, 1, 0}},
		{day, labelBrowserEnd(2, 26), [3]string{memory, thinDay, thinDay}, [3]int{0, 1, 1}},
		{day, labelBrowserEnd(3, 26), [3]string{memory, memory, thinDay}, [3]int{0, 0, 1}},
		{day, labelBrowserEnd(12, 0), [3]string{memory, memory, unreadHour}, [3]int{0, 0, 2}}, // just after an hour
		{six, labelBrowserEnd(0, 7), [3]string{thinDay, memory, memory}, [3]int{1, 0, 0}},
		{six, labelBrowserEnd(0, 17), [3]string{memory, memory, thinDay}, [3]int{0, 0, 1}},
		{six, labelBrowserEnd(1, 7), [3]string{memory, thinDay, thinDay}, [3]int{0, 1, 0}},
		{six, labelBrowserEnd(6, 2), [3]string{thinDay, thinDay, thinDay}, [3]int{1, 0, 0}}, // starts just after midnight
		{hour, labelBrowserEnd(0, 7), [3]string{thinDay, memory, memory}, [3]int{7, 0, 0}},
		{hour, labelBrowserEnd(1, 2), [3]string{unreadHour, thinDay, memory}, [3]int{7, 7, 0}},
		{hour, labelBrowserEnd(12, 2), [3]string{unreadHour, memory, memory}, [3]int{7, 0, 0}}, // just after an hour
	} {
		t.Run(tc.shape.width.String()+"/"+tc.end0.Format("15:04"), func(t *testing.T) {
			now := pinnedNow(tc.end0)
			vl := &fakeVictoriaLogs{}
			seedStableLabels(vl, now)
			p := newPinnedLabelProxy(t, vl, 10*time.Second, now)
			full := newInventoryTestProxy(t, vl, -1)
			_ = namesOnlyListing(t, p, tc.end0.Add(-tc.shape.width), tc.end0)
			for run, want := range tc.want {
				settleBackground(t, p)
				end := tc.end0.Add(-time.Duration(run+1) * tc.shape.shift)
				start := end.Add(-tc.shape.width)
				missed := map[string]float64{thinDay: 0, unreadHour: 0, "unconfirmed": 0}
				for why := range missed {
					missed[why] = internalOperationCount(p, "metadata_inventory_memory_miss", why)
				}
				got, n := namesOnlyListingCalls(t, p, start, end)
				why := memory
				for reason, before := range missed {
					if internalOperationCount(p, "metadata_inventory_memory_miss", reason) > before {
						why = reason
					}
				}
				if why != want || n != tc.calls[run] {
					t.Fatalf("moved window %d: %d VictoriaLogs calls on its request path, memory miss %q, want %d and %q", run+1, n, why, tc.calls[run], want)
				}
				if exact := namesOnlyListing(t, full, start, end); strings.Join(got, ",") != strings.Join(exact, ",") {
					t.Fatalf("moved window %d listed %v, one call %v", run+1, got, exact)
				}
			}
			settleBackground(t, p)
		})
	}
}

// heldBackgroundHours is a VictoriaLogs transport that holds every
// background read of an hour bucket's names (its row count passes) until the
// test releases it: the label browser's next window then arrives while the
// reads the previous listing started are still running, as it does when the
// A/B run asks for windows back to back.
type heldBackgroundHours struct {
	inner   http.RoundTripper
	mu      sync.Mutex
	pending []chan struct{}
	waiting atomic.Int64
}

func (h *heldBackgroundHours) RoundTrip(req *http.Request) (*http.Response, error) {
	if isBackgroundInventory(req.Context()) && req.URL.Path == "/select/logsql/stream_field_names" {
		q := req.URL.Query()
		a, _ := strconv.ParseInt(q.Get("start"), 10, 64)
		b, _ := strconv.ParseInt(q.Get("end"), 10, 64)
		if time.Duration(b-a) == time.Hour {
			release := make(chan struct{})
			h.mu.Lock()
			h.pending = append(h.pending, release)
			h.mu.Unlock()
			h.waiting.Add(1)
			select {
			case <-release:
			case <-req.Context().Done():
			}
			h.waiting.Add(-1)
		}
	}
	return h.inner.RoundTrip(req)
}

// held returns the reads held since the last call.
func (h *heldBackgroundHours) held() []chan struct{} {
	h.mu.Lock()
	defer h.mu.Unlock()
	gen := h.pending
	h.pending = nil
	return gen
}

// steady waits until every running background read of p is held: the others
// have finished. Reads start only inside a listing, so none starts later.
func (h *heldBackgroundHours) steady(t *testing.T, p *Proxy) {
	t.Helper()
	h.until(t, func() bool { return p.inventoryBackgroundRunning.Load() == h.waiting.Load() })
}

// release lets the reads of gen go on and waits until they have finished.
func (h *heldBackgroundHours) release(t *testing.T, p *Proxy, gen []chan struct{}) {
	t.Helper()
	for _, c := range gen {
		close(c)
	}
	h.until(t, func() bool {
		h.mu.Lock()
		unreleased := int64(len(h.pending))
		h.mu.Unlock()
		return h.waiting.Load() == unreleased && p.inventoryBackgroundRunning.Load() == unreleased
	})
}

func (h *heldBackgroundHours) until(t *testing.T, done func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !done() {
		if time.Now().After(deadline) {
			t.Fatal("background inventory reads neither finished nor reached the hold")
		}
		time.Sleep(time.Millisecond)
	}
}

// Gate: the label browser's windows asked back to back. Each background read
// a listing starts lasts longer than one request and less than two: it is
// held until the next listing has its answer, then finishes before the one
// after. A moved window then finds the hour it starts in only when a listing
// two requests back read it, which is why a listing also reads the hour two
// before its start once the nearer hours are cached or being read. A 24h
// window moved back 3617 s is then answered from memory from its third run
// on, at the pinned instants of MovedWindowNoRequestPathCalls (12:26, 23:26,
// 18:45); reading only up to the hour before the start, every run reads
// VictoriaLogs.
//
// The windows that still read VictoriaLogs are listed exactly, and why: the
// first two after the first load, whose hours only the listing one request
// back read ("uncached"); a 24h window starting just after an hour, which
// some runs move back over two hour boundaries; the 1h window, whose one hour
// of background reads per listing leaves no room to read ahead; and the
// windows whose part of a UTC day is too thin to prove rows
// ("no_rows_evidence"). Every answer lists the names of one full-range call.
// The edge refresh budget is short: a window waits at most that long for a
// running read before it reads VictoriaLogs. With -metadata-max-staleness=0
// nothing is read in the background and every moved window reads
// VictoriaLogs.
//
// conformance: semantics/label-browser-moved-window-from-memory, semantics/label-browser-latency-cold-and-refresh, semantics/label-inventory-exact-and-incremental, loki_api_v1_labels
func TestLabelBrowserGates_BackToBackMovedWindows(t *testing.T) {
	hour, six, day := labelBrowserShapes[0], labelBrowserShapes[1], labelBrowserShapes[2]
	const mem = "memory:0"
	for _, tc := range []struct {
		shape struct{ width, shift time.Duration }
		end0  time.Time
		want  []string // moved windows 1 to 7: the memory miss reason (or memory) and the request-path VictoriaLogs calls
	}{
		{day, labelBrowserEnd(12, 26), []string{"uncached:3", "uncached:2", mem, mem, mem, mem, mem}},
		{day, labelBrowserEnd(23, 26), []string{"uncached:3", "uncached:2", mem, mem, mem, mem, mem}},
		{day, labelBrowserEnd(18, 45), []string{"uncached:9", "uncached:8", mem, mem, mem, mem, mem}},
		// Runs 4 and 5 end between 00:00 and 01:00 UTC.
		{day, labelBrowserEnd(4, 26), []string{"uncached:3", "uncached:2", mem, "no_rows_evidence:1", "no_rows_evidence:1", mem, mem}},
		{day, labelBrowserEnd(12, 0), []string{"uncached:3", "uncached:2", "uncached:2", "uncached:2", mem, mem, mem}},
		{six, labelBrowserEnd(12, 26), []string{"uncached:4", mem, mem, mem, mem, mem, mem}},
		{hour, labelBrowserEnd(12, 2), []string{"uncached:7", "uncached:7", mem, mem, mem, mem, mem}},
		{hour, labelBrowserEnd(0, 7), []string{"uncached:7", "uncached:7", mem, mem, mem, mem, mem}},
	} {
		t.Run(tc.shape.width.String()+"/"+tc.end0.Format("15:04"), func(t *testing.T) {
			now := pinnedNow(tc.end0)
			vl := &fakeVictoriaLogs{}
			seedStableLabels(vl, now)
			full := newInventoryTestProxy(t, vl, -1)
			for _, staleness := range []time.Duration{10 * time.Second, 0} {
				got := backToBackMovedWindows(t, vl, full, staleness, now, tc.end0, tc.shape.width, tc.shape.shift, len(tc.want))
				if staleness > 0 && strings.Join(got, " ") != strings.Join(tc.want, " ") {
					t.Fatalf("moved windows (memory miss reason:request-path calls) %v, want %v", got, tc.want)
				}
			}
		})
	}
}

// backToBackMovedWindows asks the first window, then runs moved windows, each
// while the background reads the listing before it started are held, and
// returns each moved window's memory miss reason and request-path calls.
func backToBackMovedWindows(t *testing.T, vl *fakeVictoriaLogs, full *Proxy, staleness time.Duration, now, end0 time.Time, width, shift time.Duration, runs int) []string {
	t.Helper()
	p := newPinnedLabelProxy(t, vl, staleness, now)
	p.metadataEdgeRefreshBudget = 5 * time.Millisecond
	h := &heldBackgroundHours{inner: p.client.Transport}
	p.client.Transport = h
	_ = namesOnlyListing(t, p, end0.Add(-width), end0)
	h.steady(t, p)
	var got []string
	for run := 1; run <= runs; run++ {
		previous := h.held()
		end := end0.Add(-time.Duration(run) * shift)
		start := end.Add(-width)
		missed := map[string]float64{"no_rows_evidence": 0, "uncached": 0, "unconfirmed": 0}
		for why := range missed {
			missed[why] = internalOperationCount(p, "metadata_inventory_memory_miss", why)
		}
		names, n := namesOnlyListingCalls(t, p, start, end)
		why := "memory"
		for reason, before := range missed {
			if internalOperationCount(p, "metadata_inventory_memory_miss", reason) > before {
				why = reason
			}
		}
		h.steady(t, p)
		h.release(t, p, previous)
		if exact := namesOnlyListing(t, full, start, end); strings.Join(names, ",") != strings.Join(exact, ",") {
			t.Fatalf("staleness %s, moved window %d listed %v, one call %v", staleness, run, names, exact)
		}
		if staleness == 0 && (n == 0 || p.inventoryBackgroundRunning.Load() != 0) {
			t.Fatalf("with -metadata-max-staleness=0 moved window %d made %d VictoriaLogs calls with %d background reads running, want calls and none", run, n, p.inventoryBackgroundRunning.Load())
		}
		got = append(got, why+":"+strconv.Itoa(n))
	}
	h.release(t, p, h.held())
	return got
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
	// Mid-hour, just after an hour and just after midnight.
	for _, end := range []time.Time{labelBrowserEnd(12, 26), labelBrowserEnd(12, 0), labelBrowserEnd(0, 26)} {
		t.Run(end.Format("15:04"), func(t *testing.T) { backgroundReadsWithinTheWindow(t, end) })
	}
}

func backgroundReadsWithinTheWindow(t *testing.T, end time.Time) {
	now := pinnedNow(end)
	vl := &fakeVictoriaLogs{}
	seedStableLabels(vl, now)
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
			p := newPinnedLabelProxy(t, vl, 10*time.Second, now)
			p.metadataInventoryPrefetch = tc.prefetch
			start := end.Add(-tc.width)
			params := url.Values{"query": {"*"}, "start": {strconv.FormatInt(start.UnixNano(), 10)}, "end": {strconv.FormatInt(end.UnixNano(), 10)}}
			plan := planInventorySegments(start.UnixNano(), end.UnixNano(), now.Add(-metadataInventorySealLag).UnixNano())
			vl.reset()
			p.fillTouchedDays(withNamesOnlyListing(context.Background()), "/select/logsql/stream_field_names", params, listingBase(p), plan, now, true, false)
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
	// Mid-day, and around UTC midnight, where some windows read VictoriaLogs.
	for _, end0 := range []time.Time{labelBrowserEnd(12, 26), labelBrowserEnd(0, 26), labelBrowserEnd(1, 2)} {
		t.Run(end0.Format("15:04"), func(t *testing.T) { answersBetweenExactAndTheDays(t, end0) })
	}
}

func answersBetweenExactAndTheDays(t *testing.T, end0 time.Time) {
	now := pinnedNow(end0)
	rng := rand.New(rand.NewPCG(31, 37))
	vl := &fakeVictoriaLogs{}
	seedRows(vl, now, rng)
	for i := 0; i < 10; i++ {
		ts := now.Add(-2*time.Minute - time.Duration(rng.Int64N(int64(40*time.Hour))))
		vl.add(fakeVLRow{ts: ts.UnixNano(), stream: map[string]string{"gate_only_" + strconv.Itoa(i): "x"}})
	}
	full := newInventoryTestProxy(t, vl, -1)
	for _, tc := range labelBrowserShapes {
		p := newPinnedLabelProxy(t, vl, 10*time.Second, now)
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
	// The rows end mid-day, or before midnight so the window crosses it.
	for _, hour := range []time.Time{labelBrowserEnd(7, 0), labelBrowserEnd(23, 0)} {
		hour := hour.Truncate(time.Hour)
		t.Run(hour.Format("15:04"), func(t *testing.T) {
			now := hour.Add(5 * time.Hour)
			vl := &fakeVictoriaLogs{}
			for ts := hour.Add(-3 * time.Hour); ts.Before(hour.Add(7 * time.Minute)); ts = ts.Add(20 * time.Second) {
				vl.add(fakeVLRow{ts: ts.UnixNano(), stream: map[string]string{"app": "api", "env": "prod"}})
			}
			p := newPinnedLabelProxy(t, vl, 10*time.Second, now)
			// A listing that reads the hour whole: it is cached with its names.
			_ = namesOnlyListing(t, p, hour.Add(-2*time.Hour), hour.Add(2*time.Hour))
			settleBackground(t, p)
			// After the last row: the window's pieces in that hour are held by it.
			start, end := hour.Add(30*time.Minute+13*time.Second), hour.Add(90*time.Minute+13*time.Second)
			if got := namesOnlyListing(t, p, start, end); len(got) != 0 {
				t.Fatalf("a window after the last row listed %v, want none", got)
			}
			settleBackground(t, p)
		})
	}
}
