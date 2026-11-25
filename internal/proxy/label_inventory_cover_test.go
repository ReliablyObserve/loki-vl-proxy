package proxy

import (
	"context"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// The label browser over a window moved back by an hour or more (the A/B
// shape "refresh labels (label browser)" moves its 24h window back 3617 s per
// run, and its 6h window 317 s) used to read each unaligned end of the window
// as an edge plus minute and 5-minute buckets that no earlier request cached:
// about 19 VictoriaLogs calls, five rounds at four in parallel. The pieces of
// each end are now answered by the hour that holds them: cached, or one count
// and one scan, and the answer lists exactly the names of one full-range call.
//
// conformance: semantics/label-browser-latency-cold-and-refresh, semantics/label-inventory-exact-and-incremental, loki_api_v1_labels
func TestLabelsInventory_ShiftedWindowReadsTheHoursOfItsEnds(t *testing.T) {
	vl := &fakeVictoriaLogs{}
	now := time.Now()
	seedStableLabels(vl, now)
	bucketed := newInventoryTestProxy(t, vl, 4)
	full := newInventoryTestProxy(t, vl, -1)
	// Sealed, unaligned and at a fixed place in its hour, so every run plans
	// the same pieces whatever the wall clock says.
	end0 := now.Truncate(time.Hour).Add(-2*time.Hour + 26*time.Minute + 50*time.Second)
	for _, tc := range []struct {
		window, shift   time.Duration
		maxScans, maxRT int
	}{
		{24 * time.Hour, 3617 * time.Second, 2, 4},
		{6 * time.Hour, 317 * time.Second, 3, 4},
	} {
		t.Run(tc.window.String(), func(t *testing.T) {
			_ = labelsAnswer(t, bucketed, end0.Add(-tc.window), end0) // the first load fills the buckets
			vl.reset()
			for run := 1; run <= 3; run++ {
				end := end0.Add(-time.Duration(run) * tc.shift)
				got := labelsAnswer(t, bucketed, end.Add(-tc.window), end)
				calls, spans := vl.reset()
				want := labelsAnswer(t, full, end.Add(-tc.window), end)
				vl.reset()
				if got != want {
					t.Fatalf("run %d: bucketed %s, one call %s", run, got, want)
				}
				scans := calls["/select/logsql/stream_field_names"]
				total := scans + calls["/select/logsql/query"]
				t.Logf("run %d: %d listings %v, %d counts", run, scans, spans, calls["/select/logsql/query"])
				if scans > tc.maxScans || total > tc.maxRT {
					t.Fatalf("run %d made %d listings and %d calls in all, want at most %d and %d: %v", run, scans, total, tc.maxScans, tc.maxRT, spans)
				}
			}
		})
	}
}

// A name that only the pieces at a window's end hold is never lost: the
// covering hour holds it too, so it is not among the names of the rest of the
// window, and the pieces are read exactly. Random windows, shifts and rows
// with a label of their own, against one full-range call.
//
// conformance: semantics/label-inventory-exact-and-incremental, loki_api_v1_labels
func TestLabelsInventory_CoveredEndsMatchOneFullRangeCall(t *testing.T) {
	rng := rand.New(rand.NewPCG(17, 23))
	now := time.Now()
	for trial := 0; trial < 12; trial++ {
		vl := &fakeVictoriaLogs{}
		seedRows(vl, now, rng)
		for i := 0; i < 6; i++ {
			ts := now.Add(-time.Duration(rng.Int64N(int64(50 * time.Hour))))
			vl.add(fakeVLRow{ts: ts.UnixNano(), stream: map[string]string{"app": "api", "only_" + strconv.Itoa(i): "x"}})
		}
		bucketed := newInventoryTestProxy(t, vl, 4)
		full := newInventoryTestProxy(t, vl, -1)
		window := []time.Duration{6 * time.Hour, 24 * time.Hour, 7 * 24 * time.Hour, 3*time.Hour + 17*time.Minute}[trial%4]
		end := now.Add(-time.Minute - time.Duration(rng.Int64N(int64(26*time.Hour))))
		for step := 0; step < 6; step++ {
			start := end.Add(-window)
			got, want := labelsAnswer(t, bucketed, start, end), labelsAnswer(t, full, start, end)
			if got != want {
				t.Fatalf("trial %d step %d, %s ending %s:\n bucketed %s\n one call %s", trial, step, window, end.UTC().Format(time.RFC3339Nano), got, want)
			}
			// The next window: moved back by up to two hours, or forward by
			// seconds (a refresh), never into the unsealed last minute.
			if rng.IntN(3) == 0 {
				end = end.Add(time.Duration(1+rng.IntN(90)) * time.Second)
				if limit := now.Add(-time.Minute); end.After(limit) {
					end = limit
				}
			} else {
				end = end.Add(-time.Duration(1+rng.Int64N(int64(2*time.Hour/time.Second))) * time.Second)
			}
		}
	}
}

// Only the /labels path reads names alone: a label values listing, and a
// names listing asked without the mark (detected labels, the stream label
// cache), keep VictoriaLogs' exact hits and order, and a names-only answer
// never lands in the exact field-name cache other readers share.
//
// conformance: semantics/label-inventory-exact-and-incremental, loki_api_v1_label_name_values
func TestLabelsInventory_ExactListingsKeepHitsAndOrder(t *testing.T) {
	vl := &fakeVictoriaLogs{}
	now := time.Now()
	seedStableLabels(vl, now)
	end0 := now.Truncate(time.Minute).Add(-2*time.Minute - 13*time.Second)
	end := end0.Add(-3617 * time.Second)
	// A label only the first seconds of the shifted window hold: a piece of
	// its left end.
	vl.add(fakeVLRow{ts: end.Add(-24*time.Hour + 30*time.Second).UnixNano(), stream: map[string]string{"rare": "1"}})
	bucketed := newInventoryTestProxy(t, vl, 4)
	full := newInventoryTestProxy(t, vl, -1)
	_ = labelsAnswer(t, bucketed, end0.Add(-24*time.Hour), end0)
	params := url.Values{"query": {"*"}, "start": {strconv.FormatInt(end.Add(-24*time.Hour).UnixNano(), 10)}, "end": {strconv.FormatInt(end.UnixNano(), 10)}}
	_ = labelsAnswer(t, bucketed, end.Add(-24*time.Hour), end)
	names := withNamesOnlyListing(context.Background())
	for _, l := range []struct {
		ctx   context.Context
		path  string
		extra url.Values
	}{
		{names, "/select/logsql/stream_field_values", url.Values{"field": {"pod"}}},
		{names, "/select/logsql/field_names", nil},
		{context.Background(), "/select/logsql/stream_field_names", nil},
	} {
		q := url.Values{}
		for k, v := range params {
			q[k] = v
		}
		for k, v := range l.extra {
			q[k] = v
		}
		got, err := bucketed.fetchVLListing(l.ctx, l.path, q)
		if err != nil {
			t.Fatal(err)
		}
		want, err := full.fetchVLListing(context.Background(), l.path, q)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("%s: bucketed %v, one call %v", l.path, got, want)
		}
	}
	// The exact field-name cache keeps VictoriaLogs' order (hits descending).
	got, err := bucketed.fetchStreamFieldNamesCached(context.Background(), params)
	if err != nil {
		t.Fatal(err)
	}
	want, err := full.fetchStreamFieldNamesCached(context.Background(), params)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("exact field names after a names-only listing: %v, want %v", got, want)
	}
	if !strings.Contains(labelsAnswer(t, bucketed, end.Add(-24*time.Hour), end), `"rare"`) {
		t.Fatal("the label held by one end of the window is missing")
	}
}

// FuzzLabelsInventoryCover holds the names-only listing of a window and of
// the window moved by a shift to one full-range call, over rows with labels
// of their own placed by the input.
func FuzzLabelsInventoryCover(f *testing.F) {
	f.Add(uint16(1440), int32(3617), uint16(77), uint16(1500), uint16(3000))
	f.Add(uint16(360), int32(317), uint16(5), uint16(361), uint16(10))
	f.Add(uint16(100), int32(-45), uint16(0), uint16(59), uint16(61))
	now := time.Now().Truncate(time.Minute)
	f.Fuzz(func(t *testing.T, windowMin uint16, shiftS int32, endAgoMin, rowA, rowB uint16) {
		window := time.Duration(windowMin%(4*1440)+11) * time.Minute
		end := now.Add(-time.Duration(endAgoMin%(2*1440)+2)*time.Minute - 13*time.Second)
		vl := &fakeVictoriaLogs{}
		for ts := end.Add(-window - 3*time.Hour); ts.Before(now); ts = ts.Add(7 * time.Minute) {
			vl.add(fakeVLRow{ts: ts.UnixNano(), stream: map[string]string{"app": "api"}})
		}
		vl.add(fakeVLRow{ts: end.Add(-time.Duration(rowA) * time.Minute).UnixNano(), stream: map[string]string{"only_a": "1"}})
		vl.add(fakeVLRow{ts: end.Add(-time.Duration(rowB) * 17 * time.Second).UnixNano(), stream: map[string]string{"only_b": "1"}})
		bucketed := newInventoryTestProxy(t, vl, 4)
		full := newInventoryTestProxy(t, vl, -1)
		for _, e := range []time.Time{end, end.Add(-time.Duration(shiftS%(3*3600)) * time.Second)} {
			if limit := now.Add(-time.Minute); e.After(limit) {
				e = limit
			}
			if got, want := labelsAnswer(t, bucketed, e.Add(-window), e), labelsAnswer(t, full, e.Add(-window), e); got != want {
				t.Fatalf("%s ending %s: bucketed %s, one call %s", window, e.UTC().Format(time.RFC3339), got, want)
			}
		}
	})
}

// newInventoryTestProxyAt is newInventoryTestProxy for a VictoriaLogs at url.
func newInventoryTestProxyAt(t *testing.T, url string, parallelism int) *Proxy {
	t.Helper()
	p, err := New(Config{
		BackendURL:                   url,
		Cache:                        cache.New(time.Hour, 100000),
		LogLevel:                     "error",
		MetadataInventoryParallelism: parallelism,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	return p
}

// spanLog records the [start, end) of every stream_field_names call.
type spanLog struct {
	mu    sync.Mutex
	f     *fakeVictoriaLogs
	calls [][2]int64
}

func (s *spanLog) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	_ = r.ParseForm()
	if r.URL.Path == "/select/logsql/stream_field_names" {
		a, _ := strconv.ParseInt(r.Form.Get("start"), 10, 64)
		b, _ := strconv.ParseInt(r.Form.Get("end"), 10, 64)
		s.mu.Lock()
		s.calls = append(s.calls, [2]int64{a, b})
		s.mu.Unlock()
	}
	s.f.ServeHTTP(w, r)
}

// readsInside counts the listings since the last call that lie inside [a, b).
func (s *spanLog) readsInside(a, b time.Time) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, c := range s.calls {
		if c[0] >= a.UnixNano() && c[1] <= b.UnixNano() {
			n++
		}
	}
	s.calls = nil
	return n
}

// A row written with an old timestamp into an end of the window that its hour
// answers is listed by the next near-now request, as it is when the end is
// read live: the hour ended an hour ago, so every listing's run count
// confirms it, the changed count makes the hour be read again, and the new
// name it then holds sends the pieces to VictoriaLogs. An hour that ended
// within the last hour never answers an end: its end is read live.
//
// conformance: quality/labels-backfill-into-cached-bucket, semantics/label-inventory-exact-and-incremental
func TestLabelsInventory_RowsBackfilledIntoACoveredEndAppearOnTheNextRequest(t *testing.T) {
	for _, tc := range []struct {
		name    string
		covered bool
		end     func(now time.Time) time.Time
	}{
		// The window's end sits in an hour that ended more than an hour ago.
		{"settled hour", true, func(now time.Time) time.Time {
			return now.Truncate(time.Hour).Add(-3*time.Hour + 26*time.Minute + 33*time.Second)
		}},
		// The end's hour ended within the last hour.
		{"newest hour", false, func(now time.Time) time.Time {
			return now.Truncate(time.Hour).Add(-17*time.Minute - 13*time.Second)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			now := time.Now()
			f := &fakeVictoriaLogs{}
			seedStableLabels(f, now)
			vl := &spanLog{f: f}
			srv := httptest.NewServer(vl)
			t.Cleanup(srv.Close)
			p, _ := newFreshnessProxy(t, srv.URL, 24*time.Hour)
			end := tc.end(now)
			endHour := end.Truncate(time.Hour)
			// The window an hour later fills the hour of this window's end.
			later := end.Add(time.Hour)
			_ = labelsAnswer(t, p, later.Add(-6*time.Hour), later)
			if got := labelsAnswer(t, p, end.Add(-6*time.Hour), end); strings.Contains(got, "late_edge") {
				t.Fatalf("before the backfill: %s", got)
			}
			time.Sleep(200 * time.Millisecond) // past the recent-tail staleness
			_ = vl.readsInside(endHour, endHour.Add(time.Hour))
			_ = labelsAnswer(t, p, end.Add(-6*time.Hour), end)
			reads := vl.readsInside(endHour, endHour.Add(time.Hour))
			if tc.covered && reads != 0 {
				t.Fatalf("a refresh read the end's pieces %d times, want them answered by their cached hour", reads)
			}
			if !tc.covered && reads == 0 {
				t.Fatal("a refresh did not read the end of a window in the newest hour live")
			}
			f.add(fakeVLRow{ts: end.Add(-10 * time.Second).UnixNano(), stream: map[string]string{"app": "api", "late_edge": "1"}})
			time.Sleep(200 * time.Millisecond)
			if got := labelsAnswer(t, p, end.Add(-6*time.Hour), end); !strings.Contains(got, `"late_edge"`) {
				t.Fatalf("a row backfilled into the end of the window was not listed: %s", got)
			}
		})
	}
}

// A row written with an old timestamp into an empty bucket of a covered end
// is listed by the next near-now request: the hour's row count changes, the
// hour is read again and holds the new name, and the pieces are then read
// from VictoriaLogs whatever their own schedule says (the empty bucket was
// cached by an earlier window that read that end as pieces).
//
// conformance: quality/labels-backfill-into-cached-bucket, semantics/label-inventory-exact-and-incremental
func TestLabelsInventory_RowsBackfilledIntoAnEmptyCoveredPieceAppearOnTheNextRequest(t *testing.T) {
	now := time.Now()
	hour := now.Truncate(time.Hour).Add(-3 * time.Hour) // ended two hours ago
	gapFrom, gapTo := hour.Add(10*time.Minute), hour.Add(15*time.Minute)
	f := &fakeVictoriaLogs{}
	for ts := now.Add(-30 * time.Hour).Truncate(time.Hour); ts.Before(now); ts = ts.Add(20 * time.Second) {
		if !ts.Before(gapFrom) && ts.Before(gapTo) {
			continue
		}
		f.add(fakeVLRow{ts: ts.UnixNano(), stream: map[string]string{"app": "api", "env": "prod"}})
	}
	vl := &spanLog{f: f}
	srv := httptest.NewServer(vl)
	t.Cleanup(srv.Close)
	p, _ := newFreshnessProxy(t, srv.URL, 24*time.Hour)
	end := hour.Add(26*time.Minute + 33*time.Second)
	// The first window reads its end as pieces and caches the empty one.
	_ = labelsAnswer(t, p, end.Add(-6*time.Hour), end)
	// A window an hour later caches the whole hour.
	_ = labelsAnswer(t, p, end.Add(time.Hour-6*time.Hour), end.Add(time.Hour))
	time.Sleep(200 * time.Millisecond) // past the recent-tail staleness
	_ = vl.readsInside(hour, hour.Add(time.Hour))
	_ = labelsAnswer(t, p, end.Add(-6*time.Hour), end)
	if reads := vl.readsInside(hour, hour.Add(time.Hour)); reads != 0 {
		t.Fatalf("the end was read %d times, want it answered by its cached hour", reads)
	}
	f.add(fakeVLRow{ts: gapFrom.Add(2 * time.Minute).UnixNano(), stream: map[string]string{"app": "api", "late_gap": "1"}})
	time.Sleep(200 * time.Millisecond)
	if got := labelsAnswer(t, p, end.Add(-6*time.Hour), end); !strings.Contains(got, `"late_gap"`) {
		t.Fatalf("a row backfilled into an empty piece of a covered end was not listed: %s", got)
	}
}

// Each gate of the decision to answer an end from its hour, on constructed
// cache state: the freshness gates (an hour that ended an hour ago; inside the
// live window a countable query and a known row count), the cost gates (a
// cached hour whose names are known, or pieces that span half the hour and
// need more than one round), and the first-load probe.
//
// conformance: semantics/label-inventory-exact-and-incremental, semantics/label-browser-latency-cold-and-refresh
func TestLabelsInventory_CoverGates(t *testing.T) {
	now := time.Now()
	settled := now.Truncate(time.Hour).Add(-3 * time.Hour) // ended two hours ago
	newest := now.Truncate(time.Hour).Add(-time.Hour)      // ended within the last hour
	entry := func(rows int64, names ...string) inventoryEntry {
		e := inventoryEntry{Values: names, Hits: make([]int64, len(names)), Rows: rows, Checked: cache.Now().UnixNano()}
		for i := range e.Hits {
			e.Hits[i] = 1
		}
		return e
	}
	type state struct {
		ownCached  bool
		hourCached bool
		hourRows   int64
		hourNames  []string
		// piecesCached caches every bucket of the judged end's run: only its edge
		// needs a call.
		piecesCached bool
	}
	for _, tc := range []struct {
		name      string
		start     time.Time // the left end's run lies in its hour
		end       time.Time // the right end's run lies in its hour
		side      string    // which end is judged: "left" or "right"
		countable bool
		freshness time.Duration
		st        state
		want      bool
	}{
		{"cached settled hour, names known", settled.Add(-5*time.Hour + 7*time.Minute), settled.Add(26 * time.Minute), "right", true, 24 * time.Hour, state{true, true, 10, []string{"app"}, false}, true},
		{"hour ended within the last hour", newest.Add(-5*time.Hour + 7*time.Minute), newest.Add(26 * time.Minute), "right", true, 24 * time.Hour, state{true, true, 10, []string{"app"}, false}, false},
		{"live window, not countable", settled.Add(-5*time.Hour + 7*time.Minute), settled.Add(26 * time.Minute), "right", false, 24 * time.Hour, state{true, true, 10, []string{"app"}, false}, false},
		{"outside the live window, not countable", settled.Add(-5*time.Hour + 7*time.Minute), settled.Add(26 * time.Minute), "right", false, time.Hour, state{true, true, 10, []string{"app"}, false}, true},
		{"live window, row count unknown", settled.Add(-5*time.Hour + 7*time.Minute), settled.Add(26 * time.Minute), "right", true, 24 * time.Hour, state{true, true, -1, []string{"app"}, false}, false},
		{"cached hour holds a name the window lacks", settled.Add(-5*time.Hour + 7*time.Minute), settled.Add(26 * time.Minute), "right", true, 24 * time.Hour, state{true, true, 10, []string{"app", "rare"}, false}, false},
		{"hour not cached, pieces span half of it", settled.Add(-5*time.Hour + 7*time.Minute + 30*time.Second), settled.Add(26 * time.Minute), "left", true, 24 * time.Hour, state{true, false, 0, nil, false}, true},
		{"hour not cached, pieces span less than half", settled.Add(-5*time.Hour + 41*time.Minute + 30*time.Second), settled.Add(26 * time.Minute), "left", true, 24 * time.Hour, state{true, false, 0, nil, false}, false},
		{"hour not cached, only the edge needs a call", settled.Add(-5*time.Hour + 7*time.Minute + 30*time.Second), settled.Add(26 * time.Minute), "left", true, 24 * time.Hour, state{true, false, 0, nil, true}, false},
		{"first load: the window's buckets are not in memory", settled.Add(-5*time.Hour + 7*time.Minute), settled.Add(26 * time.Minute), "right", true, 24 * time.Hour, state{false, true, 10, []string{"app"}, false}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := newInventoryTestProxy(t, &fakeVictoriaLogs{}, 4)
			p.metadataCacheFreshness = tc.freshness
			base := "test-base"
			plan := planInventorySegments(tc.start.UnixNano(), tc.end.UnixNano(), now.Add(-metadataInventorySealLag).UnixNano())
			if tc.st.ownCached {
				for _, seg := range plan {
					if seg.level == 0 || seg.level == 1 {
						if err := p.storeInventoryEntry(inventoryBucketKey(base, seg), entry(10, "app")); err != nil {
							t.Fatal(err)
						}
					}
				}
			}
			at := tc.end
			if tc.side == "left" {
				at = tc.start
			}
			hour := inventorySegment{start: at.Truncate(time.Hour).UnixNano(), end: at.Truncate(time.Hour).Add(time.Hour).UnixNano(), level: 1}
			if tc.st.piecesCached {
				for _, seg := range plan {
					if seg.level >= 2 && seg.start >= hour.start && seg.end <= hour.end {
						if err := p.storeInventoryEntry(inventoryBucketKey(base, seg), entry(3, "app")); err != nil {
							t.Fatal(err)
						}
					}
				}
			}
			if tc.st.hourCached {
				if err := p.storeInventoryEntry(inventoryBucketKey(base, hour), entry(tc.st.hourRows, tc.st.hourNames...)); err != nil {
					t.Fatal(err)
				}
			}
			out, covers := p.coverFinePieces("/select/logsql/stream_field_names", base, plan, now.Add(-metadataInventorySealLag).UnixNano(), tc.countable)
			got := false
			for i := range covers {
				if out[i] == hour {
					got = true
				}
			}
			if got != tc.want {
				t.Fatalf("the %s end answered by its hour: %v, want %v (covers %v)", tc.side, got, tc.want, covers)
			}
		})
	}
}
