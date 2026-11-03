package proxy

import (
	"bytes"
	"fmt"
	"net/http"
	"strconv"
	"testing"
	"time"
)

// largeAnswerFixture is six streams with a line every 5 s over [s0, s0+span):
// a by-app answer at a 10 s step is far larger than one split of it.
func largeAnswerFixture(s0 time.Time, span time.Duration) []slidingFixtureLine {
	var lines []slidingFixtureLine
	for i := 0; time.Duration(i)*5*time.Second < span; i++ {
		lines = append(lines, slidingFixtureLine{
			ts:  s0.Add(time.Duration(i) * 5 * time.Second).UnixNano(),
			app: "app-" + strconv.Itoa(i%6),
			msg: "n=" + strconv.Itoa(i%13),
		})
	}
	return lines
}

// largeAnswerRun sends windows to a proxy with the extent cache and to one
// without it, each over its own copy of the backend and with the same cache
// size and read bound, and requires the same status and bytes for each. It
// returns the extent cache's outcomes and the buckets each proxy read.
func largeAnswerRun(t testing.TB, lines []slidingFixtureLine, maxBytes, buffered int, queries []string, windows []extentWindow) (outcomes map[string]int, onBuckets, offBuckets int64) {
	t.Helper()
	newBackend := func() http.Handler {
		srv, _ := newSlidingFakeVL(t, lines)
		return srv.Config.Handler
	}
	onSrv, onShim := newExtentShim(t, newBackend())
	offSrv, offShim := newExtentShim(t, newBackend())
	on := newExtentTestProxy(t, onSrv.URL, true, maxBytes)
	off := newExtentTestProxy(t, offSrv.URL, false, maxBytes)
	if buffered > 0 {
		for _, p := range []*Proxy{on, off} {
			p.execLimits = defaultExecutionLimits()
			p.execLimits.BufferedBackendBodyBytes = buffered
		}
	}
	for _, query := range queries {
		for _, w := range windows {
			q := query
			wantCode, want := extentQueryRange(t, off, q, w.start, w.end, w.step, nil)
			gotCode, got := extentQueryRange(t, on, q, w.start, w.end, w.step, nil)
			if gotCode != wantCode || !bytes.Equal(got, want) {
				t.Fatalf("%s [%s, %s] step %s: the extent cache changed the answer\n with: %d %.300s\nwithout: %d %.300s", q, w.start.Format(time.RFC3339), w.end.Format(time.RFC3339), w.step, gotCode, got, wantCode, want)
			}
		}
	}
	_, onBuckets = onShim.bucketsRead()
	_, offBuckets = offShim.bucketsRead()
	return extentOutcomes(t, on), onBuckets, offBuckets
}

// An answer larger than one cache entry is kept split by split: each split of
// it is one entry, so the cache keeps every split that fits an entry, however
// large the whole answer is, up to -backend-max-buffered-response-bytes (the
// bound of the caller's own read). A window moved back by a step, a refresh
// and a window moved back by hours (a missing range larger than one entry)
// then read only the buckets they lack; the same window again is the response
// cache's. Where a whole split cannot fit one entry, or the answer is over the
// read bound, only the smaller reads (a refresh, a moved edge) are kept. Every
// answer is byte for byte the uncached proxy's, errors included.
// conformance: quality/query-range-extent-cache-large-answer, quality/query-range-extent-cache, loki_api_v1_query_range
func TestStatsExtentCacheKeepsAnswersLargerThanOneEntry(t *testing.T) {
	s0 := time.Unix(1700001000, 0).UTC().Truncate(time.Hour)
	span := 9 * time.Hour
	lines := largeAnswerFixture(s0, span)
	queries := []string{
		`sum by (app) (count_over_time({app=~".+"}[10s]))`,
		`sum by (app) (bytes_over_time({app=~".+"}[1m]))`,
	}
	step := 10 * time.Second
	end := s0.Add(8 * time.Hour)
	windows := []extentWindow{{end.Add(-4 * time.Hour), end, step}}
	for i := 1; i <= 3; i++ { // moved back a step at a time, as the bench repeats it
		e := end.Add(-time.Duration(i) * step)
		windows = append(windows, extentWindow{e.Add(-4 * time.Hour), e, step})
	}
	for i := 1; i <= 2; i++ { // a refresh
		e := end.Add(time.Duration(i) * time.Minute)
		windows = append(windows, extentWindow{e.Add(-4 * time.Hour), e, step})
	}
	windows = append(windows, windows[0]) // the same window again
	// Moved back by three hours: the missing range alone is larger than one entry.
	windows = append(windows, extentWindow{end.Add(-7 * time.Hour), end.Add(-3 * time.Hour), step})

	for _, tc := range []struct {
		name     string
		maxBytes int  // the cache's size; an entry takes at most a tenth
		buffered int  // -backend-max-buffered-response-bytes, 0 for the default
		kept     bool // every window but the first is answered from stored buckets
	}{
		// About 150 KB per answer and 40 KB per split at a 10 s step.
		{name: "answer over one entry, splits under it", maxBytes: 512 << 10, kept: true},
		{name: "answer and splits under one entry", maxBytes: 16 << 20, kept: true},
		{name: "a split over one entry", maxBytes: 128 << 10, kept: false},
		{name: "answer over the read bound", maxBytes: 16 << 20, buffered: 64 << 10, kept: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			outcomes, on, off := largeAnswerRun(t, lines, tc.maxBytes, tc.buffered, queries, windows)
			t.Logf("outcomes %v; buckets read: %d with the extent cache, %d without", outcomes, on, off)
			if !tc.kept {
				return // the answers are the uncached proxy's: nothing more to hold
			}
			// The repeated window is the response cache's while it fits there.
			if outcomes["stored"] == 0 || outcomes["miss"] != len(queries) || outcomes["hit"]+outcomes["partial"] < len(queries)*(len(windows)-2) || outcomes["unreadable"] != 0 {
				t.Fatalf("every window after the first must be answered from stored buckets: %v", outcomes)
			}
			if on*3 > off {
				t.Fatalf("the extent cache read %d buckets, the uncached proxy %d: it should read far fewer", on, off)
			}
		})
	}
}

// The bound of a read the cache keeps: one entry per split it spans, never
// over the read bound of the request, and the read bound alone when the
// entries would overflow it.
// conformance: quality/query-range-extent-cache-large-answer
func TestStatsExtentReadLimit(t *testing.T) {
	p := newExtentTestProxy(t, "http://127.0.0.1:1", true, 10<<20) // 1 MiB entries
	p.execLimits = defaultExecutionLimits()
	p.execLimits.BufferedBackendBodyBytes = 8 << 20
	req := &statsExtentRequest{chunk: 360}
	for _, tc := range []struct {
		first, last int64
		want        int64
	}{
		{0, 0, 1 << 20},               // one bucket: one split
		{0, 359, 1 << 20},             // one whole split
		{359, 360, 2 << 20},           // two buckets across a split edge
		{-1, 0, 2 << 20},              // before the epoch
		{0, 360*5 - 1, 5 << 20},       // five splits
		{0, 360 * 8, 8 << 20},         // nine splits: the read bound
		{0, 360 * 1_000_000, 8 << 20}, // never overflows
	} {
		if got := p.statsExtentReadLimit(req, tc.first, tc.last); got != tc.want {
			t.Errorf("[%d, %d]: %d, want %d", tc.first, tc.last, got, tc.want)
		}
	}
	p.execLimits.BufferedBackendBodyBytes = 0
	if got := p.statsExtentReadLimit(req, 0, 360*3-1); got != 3<<20 {
		t.Errorf("no read bound: %d, want three entries", got)
	}
}

// FuzzStatsExtentLargeAnswerSplits: whatever the cache size (so whatever the
// share of splits that fit one entry) and the windows, the extent cache gives
// the uncached proxy's answer byte for byte.
func FuzzStatsExtentLargeAnswerSplits(f *testing.F) {
	f.Add(uint16(1024), uint8(1), int16(-10), uint8(4))
	f.Add(uint16(96), uint8(6), int16(60), uint8(2))
	f.Add(uint16(4096), uint8(2), int16(-10800), uint8(5))
	s0 := time.Unix(1700001000, 0).UTC().Truncate(time.Hour)
	lines := largeAnswerFixture(s0, 9*time.Hour)
	f.Fuzz(func(t *testing.T, cacheKB uint16, stepSel uint8, shift int16, hours uint8) {
		maxBytes := int(cacheKB%4096+32) << 10
		step := []time.Duration{5 * time.Second, 10 * time.Second, 15 * time.Second, 30 * time.Second, time.Minute, 7 * time.Second, 2 * time.Minute}[int(stepSel)%7]
		width := time.Duration(hours%6+1) * time.Hour
		end := s0.Add(8 * time.Hour)
		moved := end.Add(time.Duration(shift) * time.Second)
		if moved.Add(-width).Before(s0) {
			moved = s0.Add(width)
		}
		windows := []extentWindow{{end.Add(-width), end, step}, {moved.Add(-width), moved, step}, {end.Add(-width), end, step}}
		query := fmt.Sprintf(`sum by (app) (count_over_time({app=~".+"}[%s]))`, step)
		largeAnswerRun(t, lines, maxBytes, 0, []string{query}, windows)
	})
}
