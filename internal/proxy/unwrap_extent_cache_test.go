package proxy

import (
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"
)

// The stats extent cache answers the stats_query_range calls of an unwrap
// metric the conversion check reads: the counting query is what the cache keys
// and stores, so a rejected value or a nan in a stored bucket reaches the
// check as it does from VictoriaLogs. Loki answers a query whose window holds
// a value its conversion rejects with its 400 (pkg/logql/evaluator.go:726-733),
// and a nan is a sample (sum and avg NaN, max and min ignore it).

// unwrapCacheRows: two streams with a value every 7 seconds over 3 hours, a
// word Loki's conversion rejects at bad and a nan at nan (both on pod a).
func unwrapCacheRows(s0 time.Time, bad, nan time.Duration) []emuRow {
	var rows []emuRow
	for i := 0; time.Duration(i)*7*time.Second < 3*time.Hour; i++ {
		at := time.Duration(i) * 7 * time.Second
		row := emuRow{ts: s0.Add(at).UnixNano(), stream: fmt.Sprintf(`{app="uw",pod=%q}`, []string{"a", "b"}[i%2]), value: float64(i%11) + 0.5}
		if i%2 == 0 && bad > 0 && at >= bad && at < bad+14*time.Second {
			row.raw = "x7"
		}
		if i%2 == 0 && nan > 0 && at >= nan && at < nan+14*time.Second {
			row.raw = "NaN"
		}
		rows = append(rows, row)
	}
	return rows
}

// TestUnwrapExtentCacheMatchesUncached: with the extent cache on, every answer
// of an unwrap metric is byte for byte the uncached proxy's, the 400 of a
// rejected value included, over a refresh moving a whole step at a time, a
// re-run, a widened window and a window moved back: the rejected value sits in
// a bucket stored by an earlier window, in the part only a later window reads
// fresh, or nowhere; the nan's windows are NaN for sum and avg; the error
// filter after the unwrap only counts the nan. The cached proxy reads far fewer
// buckets.
//
// conformance: quality/unwrap-query-range-extent-cache, quality/query-range-extent-cache, semantics/unwrap-conversion-error, semantics/unwrap-gate-parsefloat-divergences, loki_api_v1_query_range
func TestUnwrapExtentCacheMatchesUncached(t *testing.T) {
	s0 := time.Unix(1_700_001_000, 0).UTC()
	first := s0.Add(30 * time.Minute)
	var windows []extentWindow
	for k := 0; k < 6; k++ { // a refresh, a whole step at a time
		start := first.Add(time.Duration(k) * time.Minute)
		windows = append(windows, extentWindow{start: start, end: start.Add(time.Hour), step: time.Minute})
	}
	windows = append(windows,
		windows[0], // a re-run
		extentWindow{start: first.Add(-30 * time.Minute), end: first.Add(time.Hour), step: time.Minute}, // widened back
		extentWindow{start: first.Add(10 * time.Minute), end: first.Add(80 * time.Minute), step: time.Minute},
	)
	queries := []string{
		`sum by (pod) (sum_over_time({app="uw"} | logfmt | unwrap amount [5m]))`,
		`sum by (pod) (sum_over_time({app="uw"} | logfmt | unwrap amount | __error__="" [5m]))`,
		`max by (pod) (max_over_time({app="uw"} | logfmt | unwrap amount [5m]))`,
		`sum by (pod) (avg_over_time({app="uw"} | logfmt | unwrap amount | __error__="" [1m]))`,
		`sum by (pod) (sum_over_time({app="uw"} | logfmt | unwrap amount [30s]))`,
	}
	for _, tc := range []struct {
		name     string
		bad, nan time.Duration
		want400  bool
	}{
		// The first window ends at 1h30m: a rejected value at 1h33m is read only
		// in the fresh part of a moved window.
		{"rejected value only in the fresh part", 93 * time.Minute, 0, true},
		// At 50m every window holds it, the first stores its bucket.
		{"rejected value in a stored bucket", 50 * time.Minute, 0, true},
		{"nan", 0, 50 * time.Minute, false},
		{"clean", 0, 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rows := unwrapCacheRows(s0, tc.bad, tc.nan)
			onSrv, onShim := newExtentShim(t, (&vlEmulator{t: t, rows: rows, field: "amount", version: "v1.50.0"}).handler())
			offSrv, offShim := newExtentShim(t, (&vlEmulator{t: t, rows: rows, field: "amount", version: "v1.50.0"}).handler())
			on := newExtentTestProxy(t, onSrv.URL, true, 256<<20)
			off := newExtentTestProxy(t, offSrv.URL, false, 256<<20)
			rejected, nans := 0, 0
			for _, query := range queries {
				for _, w := range windows {
					wantCode, want := extentQueryRange(t, off, query, w.start, w.end, w.step, nil)
					gotCode, got := extentQueryRange(t, on, query, w.start, w.end, w.step, nil)
					if gotCode != wantCode || string(got) != string(want) {
						t.Fatalf("%s [%s, %s]: the extent cache changed the answer\n with: %d %s\nwithout: %d %s", query, w.start.Format(time.RFC3339), w.end.Format(time.RFC3339), gotCode, got, wantCode, want)
					}
					if wantCode == http.StatusBadRequest && strings.Contains(string(want), "SampleExtractionErr") {
						rejected++
					} else if wantCode != http.StatusOK {
						t.Fatalf("%s: %d %s", query, wantCode, want)
					}
					if strings.Contains(string(want), `"NaN"`) {
						nans++
					}
				}
			}
			if tc.want400 != (rejected > 0) || (tc.nan > 0) != (nans > 0) {
				t.Fatalf("%d answers with Loki's 400, %d with NaN: the case proves nothing", rejected, nans)
			}
			_, onBuckets := onShim.bucketsRead()
			_, offBuckets := offShim.bucketsRead()
			outcomes := extentOutcomes(t, on)
			t.Logf("%d answers with Loki's 400, %d with NaN; buckets read %d with the cache, %d without; outcomes %v", rejected, nans, onBuckets, offBuckets, outcomes)
			if outcomes["hit"]+outcomes["partial"] == 0 || outcomes["unreadable"] != 0 {
				t.Fatalf("the unwrap stats must be answered from stored buckets: %v", outcomes)
			}
			if onBuckets*2 > offBuckets {
				t.Fatalf("the extent cache read %d buckets, the uncached proxy %d", onBuckets, offBuckets)
			}
		})
	}
}
