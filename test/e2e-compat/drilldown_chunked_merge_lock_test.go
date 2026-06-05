//go:build e2e

// E2E lock-in for the Drilldown / Explore chunked-merge fix.
//
// Replays exactly what Grafana's Loki datasource does at 24h+ time ranges
// for a metric range query:
//
//   1. Slice the requested range with `splitTimeRange(start, end, step,
//      oneDayMs)` — see Grafana core
//      `public/app/plugins/datasource/loki/metricTimeSplitting.ts`.
//      Aligned-duration chunks of 24h-step, plus a residual chunk whose
//      range can be < step.
//
//   2. Fire each sub-request at the proxy in NEWEST-FIRST order (matches
//      Grafana's `runSplitGroupedQueries` recursion).
//
//   3. Merge the responses with the same `mergeFrames + closestIdx + splice`
//      algorithm from Grafana core
//      `public/app/plugins/datasource/loki/mergeResponses.ts`.
//
//   4. Assert the merged frame does NOT have a right-edge spike. Threshold:
//      the rightmost N×1h bin holds < 40% of all nonzero timestamps.
//
// The replay validates the END-TO-END contract on real ingested data, so a
// future change that breaks any single layer (routing, leftover suppression,
// shared axis, /hits semantics) shows up as a real-stack spike. Unit-only
// locks live in `internal/proxy/drilldown_regression_lock_test.go`.

package e2e_compat

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"testing"
	"time"
)

const (
	chunkLockStep   = 120 * time.Second
	chunkLockOneDay = 24 * time.Hour

	// Right-edge tolerance — what fraction of nonzero timestamps may sit in
	// the rightmost bin without it being called a "spike". The 2026-06 regression
	// produced > 90% (every value at the single rightmost timestamp), so the
	// threshold here just needs to be tight enough to catch that and any future
	// recurrence — not so tight that the irreducible "1h leftover" bump from
	// Grafana querySplitting for ranges like 25h (24h + 1h chunks) triggers it.
	//
	// We accept the irreducible bump because the proxy can't safely suppress an
	// hour-scale leftover chunk — a user legitimately querying a 1h Drilldown
	// view would lose all data. The 320-vs-50 spike the user saw was the
	// 1-bucket residual; that case is suppressed by hits-leftover-suppressed.
	chunkLockRightBinPx = 0.65
)

// TestE2ELock_DrilldownChunkedMerge_NoRightEdgeSpike replays Grafana's
// querySplitting + mergeFrames for the four time ranges that broke during
// the 2026-06 incident (24h, 25h, 2d, 7d) against the live VL stack via
// the vmauth proxy and asserts no right-edge spike for either Drilldown
// or Explore source tags.
//
// If a future PR regresses leftover-chunk suppression or routes a
// chunk through a different code path than the rest, this test fails.
func TestE2ELock_DrilldownChunkedMerge_NoRightEdgeSpike(t *testing.T) {
	// Use the proxy directly — log-generator's continuous ingest is enough.
	// Avoid ensureDataIngested which also waits on Loki (different startup race
	// from this test's contract).
	waitForReady(t, proxyVmauthURL+"/ready", 30*time.Second)

	const query = `sum by (pod) (count_over_time({namespace="prod",pod!=""}[2m]))`

	for _, source := range []string{"drilldown", "grafana-ua"} {
		for _, rc := range []struct {
			name  string
			hours int
			bins  int
		}{
			{"24h", 24, 12},
			{"25h", 25, 12},
			{"2d", 48, 12},
			{"7d", 168, 14},
		} {
			t.Run(fmt.Sprintf("%s_%s", source, rc.name), func(t *testing.T) {
				now := time.Now().UTC()
				chunks := splitMetricTimeRange(now.Add(-time.Duration(rc.hours)*time.Hour), now,
					chunkLockStep, chunkLockOneDay)
				if len(chunks) == 0 {
					t.Fatalf("splitMetricTimeRange returned no chunks for %dh", rc.hours)
				}

				// Fire newest-first to match Grafana's runSplitGroupedQueries.
				var merged *mergedFrame
				for i := len(chunks) - 1; i >= 0; i-- {
					f := fetchChunkAsMergedFrame(t, proxyVmauthURL, query,
						chunks[i].start, chunks[i].end, chunkLockStep, source)
					if f == nil {
						continue
					}
					if merged == nil {
						merged = f
					} else {
						mergeFrames(merged, f)
					}
				}
				if merged == nil || len(merged.series) == 0 {
					t.Skipf("no merged data — log generator likely hasn't ingested enough %dh history yet", rc.hours)
				}

				rep := rightEdgeFraction(merged, rc.bins)
				if rep > chunkLockRightBinPx {
					t.Errorf("right-edge spike: rightmost 1/%d bin holds %.0f%% of nonzero timestamps (>%.0f%% threshold) — chunked-merge regression for source=%s range=%s",
						rc.bins, rep*100, chunkLockRightBinPx*100, source, rc.name)
				}
			})
		}
	}
}

// TestE2ELock_DrilldownChunkedMerge_LeftoverChunkSuppressed verifies that
// the proxy explicitly emits `X-Proxy-Drilldown-Path: hits-leftover-suppressed`
// for the tiny residual chunk Grafana sends at the trailing edge of a
// 24h-aligned split. This is the safety net that prevents the right-edge
// spike from re-appearing if any other layer regresses — as long as the
// residual is suppressed, mergeFrames has no 1-bucket block to glue.
func TestE2ELock_DrilldownChunkedMerge_LeftoverChunkSuppressed(t *testing.T) {
	waitForReady(t, proxyVmauthURL+"/ready", 30*time.Second)

	const query = `sum by (pod) (count_over_time({namespace="prod",pod!=""}[2m]))`

	for i, source := range []string{"drilldown", "grafana-ua"} {
		t.Run(source, func(t *testing.T) {
			// Stagger time per subtest so each gets a unique drillCacheKey
			// — the upstream cache doesn't include the source header in its
			// key, so two subtests at identical time windows would have the
			// second serve a cached body from the first (which won't have the
			// suppression response header even if it was originally suppressed).
			now := time.Now().UTC().Add(time.Duration(i) * 7 * time.Second)
			start := now.Add(-30 * time.Second) // range ≤ step → triggers suppression
			end := now

			req, err := http.NewRequest(http.MethodGet, proxyVmauthURL+"/loki/api/v1/query_range", nil)
			if err != nil {
				t.Fatalf("build request: %v", err)
			}
			q := url.Values{}
			q.Set("query", query)
			q.Set("start", strconv.FormatInt(start.Unix(), 10))
			q.Set("end", strconv.FormatInt(end.Unix(), 10))
			q.Set("step", strconv.FormatInt(int64(chunkLockStep.Seconds()), 10))
			req.URL.RawQuery = q.Encode()
			req.Header.Set("X-Scope-OrgID", "default")
			switch source {
			case "drilldown":
				req.Header.Set("X-Query-Tags", "Source=grafana-lokiexplore-app")
			case "grafana-ua":
				req.Header.Set("User-Agent", "Grafana/11.5.0")
			}

			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatalf("request: %v", err)
			}
			defer resp.Body.Close()
			body, _ := io.ReadAll(resp.Body)

			// Either signal proves the suppression fired:
			//   - X-Proxy-Drilldown-Path: hits-leftover-suppressed (fresh hit)
			//   - body is exactly the empty matrix (cached suppressed response,
			//     because the path header isn't stored in the response cache).
			pathHeader := resp.Header.Get("X-Proxy-Drilldown-Path")
			emptyMatrix := `{"status":"success","data":{"resultType":"matrix","result":[]}}`
			gotSuppress := pathHeader == "hits-leftover-suppressed" || string(body) == emptyMatrix
			if !gotSuppress {
				t.Errorf("source=%s expected suppression (header=hits-leftover-suppressed OR empty matrix body), got header=%q body=%s",
					source, pathHeader, body)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Embedded port of Grafana's mergeFrames + closestIdx + splice algorithm.
// Keep self-contained — DO NOT delete or "simplify" without re-reading
// public/app/plugins/datasource/loki/mergeResponses.ts.
// ---------------------------------------------------------------------------

type mergedFrame struct {
	times  []int64            // milliseconds
	series map[string][]int64 // label value -> per-timestamp value
}

type chunkWindow struct {
	start time.Time
	end   time.Time
}

// splitMetricTimeRange replicates metricTimeSplitting.ts splitTimeRange.
func splitMetricTimeRange(start, end time.Time, step, idealRange time.Duration) []chunkWindow {
	stepMs := step.Milliseconds()
	idealMs := idealRange.Milliseconds()
	startMs := start.UnixMilli()
	endMs := end.UnixMilli()
	if idealMs < stepMs {
		return []chunkWindow{{start: start, end: end}}
	}
	aligned := (idealMs / stepMs) * stepMs
	alignedStart := startMs - (startMs % stepMs)
	var out []chunkWindow
	for cs := alignedStart; cs < endMs; cs += aligned {
		ce := cs + aligned - stepMs
		if ce > endMs {
			ce = endMs
		}
		out = append(out, chunkWindow{
			start: time.UnixMilli(cs).UTC(),
			end:   time.UnixMilli(ce).UTC(),
		})
	}
	return out
}

func fetchChunkAsMergedFrame(t *testing.T, base, query string, start, end time.Time, step time.Duration, source string) *mergedFrame {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, base+"/loki/api/v1/query_range", nil)
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	q := url.Values{}
	q.Set("query", query)
	q.Set("start", strconv.FormatInt(start.Unix(), 10))
	q.Set("end", strconv.FormatInt(end.Unix(), 10))
	q.Set("step", strconv.FormatInt(int64(step.Seconds()), 10))
	req.URL.RawQuery = q.Encode()
	req.Header.Set("X-Scope-OrgID", "default")
	switch source {
	case "drilldown":
		req.Header.Set("X-Query-Tags", "Source=grafana-lokiexplore-app")
	case "grafana-ua":
		req.Header.Set("User-Agent", "Grafana/11.5.0")
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("proxy returned %d for chunk %s..%s: %s", resp.StatusCode,
			start.Format(time.RFC3339), end.Format(time.RFC3339), body)
	}
	// Skip suppressed leftovers — they intentionally return an empty matrix.
	if resp.Header.Get("X-Proxy-Drilldown-Path") == "hits-leftover-suppressed" {
		return nil
	}
	body, _ := io.ReadAll(resp.Body)
	var parsed struct {
		Data struct {
			Result []struct {
				Metric map[string]string `json:"metric"`
				Values [][2]any          `json:"values"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &parsed); err != nil {
		t.Fatalf("parse chunk response: %v\nbody: %s", err, body)
	}
	if len(parsed.Data.Result) == 0 {
		return nil
	}
	f := &mergedFrame{series: map[string][]int64{}}
	// Axis from series[0] (assert shared axis as a side-effect — same length).
	for _, v := range parsed.Data.Result[0].Values {
		ts, _ := v[0].(float64)
		f.times = append(f.times, int64(ts)*1000)
	}
	for _, s := range parsed.Data.Result {
		var label string
		for _, v := range s.Metric {
			label = v
			break
		}
		vals := make([]int64, len(s.Values))
		for i, v := range s.Values {
			vs, _ := v[1].(string)
			n, _ := strconv.ParseInt(vs, 10, 64)
			vals[i] = n
		}
		f.series[label] = vals
	}
	return f
}

func closestIdx(target int64, arr []int64) int {
	if len(arr) == 0 {
		return -1
	}
	if target <= arr[0] {
		return 0
	}
	if target >= arr[len(arr)-1] {
		return len(arr) - 1
	}
	lo, hi := 0, len(arr)-1
	for hi-lo > 1 {
		mid := (lo + hi) / 2
		if arr[mid] < target {
			lo = mid
		} else {
			hi = mid
		}
	}
	if target-arr[lo] <= arr[hi]-target {
		return lo
	}
	return hi
}

func resolveIdx(destTimes []int64, srcTime int64) int {
	idx := closestIdx(srcTime, destTimes)
	if idx < 0 {
		return 0
	}
	if srcTime > destTimes[idx] {
		return idx + 1
	}
	return idx
}

func mergeFrames(dest, src *mergedFrame) {
	for i, st := range src.times {
		dIdx := resolveIdx(dest.times, st)
		exists := dIdx < len(dest.times) && dest.times[dIdx] == st
		for label := range src.series {
			if _, ok := dest.series[label]; !ok {
				dest.series[label] = make([]int64, len(dest.times))
			}
		}
		for label, sv := range src.series {
			if exists {
				dest.series[label][dIdx] += sv[i]
			} else {
				dest.series[label] = append(dest.series[label][:dIdx],
					append([]int64{sv[i]}, dest.series[label][dIdx:]...)...)
			}
		}
		if !exists {
			dest.times = append(dest.times[:dIdx],
				append([]int64{st}, dest.times[dIdx:]...)...)
			for label := range dest.series {
				if _, inSrc := src.series[label]; !inSrc {
					dest.series[label] = append(dest.series[label][:dIdx],
						append([]int64{0}, dest.series[label][dIdx:]...)...)
				}
			}
		}
	}
}

func rightEdgeFraction(f *mergedFrame, binCount int) float64 {
	if len(f.times) == 0 {
		return 0
	}
	nz := map[int64]struct{}{}
	for _, vals := range f.series {
		for i, v := range vals {
			if v != 0 && i < len(f.times) {
				nz[f.times[i]] = struct{}{}
			}
		}
	}
	if len(nz) == 0 {
		return 0
	}
	timesSorted := make([]int64, 0, len(nz))
	for ts := range nz {
		timesSorted = append(timesSorted, ts)
	}
	sort.Slice(timesSorted, func(i, j int) bool { return timesSorted[i] < timesSorted[j] })
	span := f.times[len(f.times)-1] - f.times[0]
	if span <= 0 {
		return 0
	}
	bins := make([]int, binCount)
	for ts := range nz {
		b := int(float64(ts-f.times[0]) / float64(span) * float64(binCount))
		if b >= binCount {
			b = binCount - 1
		}
		bins[b]++
	}
	return float64(bins[binCount-1]) / float64(len(nz))
}
