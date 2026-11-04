//go:build e2e

package e2e_compat

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"reflect"
	"strconv"
	"testing"
	"time"
)

// TestCompat_QueryRangeWindowMovedRangeReuse is the warm repeat of the O1
// A/B shape: a 24h backward log query with a line filter, asked again with
// its window moved a minute back five times (the A/B matrix's shifted runs).
// A line a second (more than -query-range-expensive-hit-threshold per hour by
// the /hits estimate, which counts the selector only) makes every window
// expensive, so the windows are read one at a time whatever the adaptive
// parallelism; one line in six matches. The newest, partial hour gives 400 of
// the 900 lines, the hour before it the rest; every move takes ten matching
// lines out of the newest hour, so the lines still missing after it change on
// every request. A kept window is read with the request's whole limit, so
// each moved request reads only its moved newest window from VictoriaLogs
// (one window cache miss) and the hour before it from the cache, and every
// answer is Loki's.
// conformance: semantics/query-range-window-full-limit-reads, window-bounds-and-step-alignment, loki_api_v1_query_range
func TestCompat_QueryRangeWindowMovedRangeReuse(t *testing.T) {
	anchor := time.Now().Add(-3 * time.Hour).UTC().Truncate(time.Hour)
	fx := slidingLiveFixture{app: fmt.Sprintf("window-moved-reuse-%d", time.Now().UnixNano())}
	i := 0
	for ts := anchor.Add(-2*time.Hour + 300*time.Millisecond); ts.Before(anchor.Add(50 * time.Minute)); ts = ts.Add(time.Second) {
		word := "skip"
		if i%6 == 0 {
			word = "keep-me"
		}
		fx.lines = append(fx.lines, slidingLiveLine{ts: ts, msg: fmt.Sprintf("moved-reuse-%d %s", ts.UnixMilli(), word)})
		i++
	}
	ingestSlidingFixtures(t, fx)

	query := func(base string, end time.Time) []string {
		t.Helper()
		params := url.Values{
			"query":     {fx.selector() + ` |= "keep-me"`},
			"start":     {strconv.FormatInt(end.Add(-24*time.Hour).UnixNano(), 10)},
			"end":       {strconv.FormatInt(end.UnixNano(), 10)},
			"limit":     {"900"},
			"direction": {"backward"},
		}
		status, body := hardeningRequest(t, http.MethodGet, base+"/loki/api/v1/query_range?"+params.Encode(), "", map[string]string{"X-Scope-OrgID": "0"})
		if status != http.StatusOK {
			t.Fatalf("%s query_range status %d: %s", base, status, body)
		}
		var response struct {
			Status string `json:"status"`
			Data   struct {
				ResultType string `json:"resultType"`
				Result     []struct {
					Values [][]string `json:"values"`
				} `json:"result"`
			} `json:"data"`
		}
		if err := json.Unmarshal(body, &response); err != nil {
			t.Fatalf("%s decode query_range: %v", base, err)
		}
		if response.Status != "success" || response.Data.ResultType != "streams" {
			t.Fatalf("%s unexpected query_range response: %s", base, body)
		}
		var lines []string
		for _, stream := range response.Data.Result {
			for _, value := range stream.Values {
				lines = append(lines, value[1])
			}
		}
		return lines
	}
	misses := func() int64 {
		return proxyMetricValue(t, proxyURL, "loki_vl_proxy_window_cache_miss_total ")
	}
	hits := func() int64 {
		return proxyMetricValue(t, proxyURL, "loki_vl_proxy_window_cache_hit_total ")
	}

	end := anchor.Add(40 * time.Minute)
	want := query(lokiURL, end)
	if len(want) != 900 {
		t.Fatalf("Loki answered %d lines; the fixture must fill the limit of 900", len(want))
	}
	if got := query(proxyURL, end); !reflect.DeepEqual(got, want) {
		t.Fatalf("first range: proxy %d lines (first %v), Loki %d (first %v)", len(got), firstLines(got), len(want), firstLines(want))
	}
	for move := 1; move <= 5; move++ {
		moved := end.Add(-time.Duration(move) * time.Minute)
		want := query(lokiURL, moved)
		missesBefore, hitsBefore := misses(), hits()
		got := query(proxyURL, moved)
		missesAfter, hitsAfter := misses(), hits()
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("moved %dm: proxy %d lines (first %v), Loki %d (first %v)", move, len(got), firstLines(got), len(want), firstLines(want))
		}
		if missesAfter-missesBefore != 1 {
			t.Errorf("moved %dm: %d window cache misses, want 1 (the moved newest window); the hour before it was read by the first request",
				move, missesAfter-missesBefore)
		}
		if hitsAfter-hitsBefore < 1 {
			t.Errorf("moved %dm: %d window cache hits, want the hour before the newest one from the cache", move, hitsAfter-hitsBefore)
		}
		t.Logf("moved %dm: proxy and Loki returned %d lines; window cache hits %d, misses %d", move, len(got), hitsAfter-hitsBefore, missesAfter-missesBefore)
	}
}

func firstLines(lines []string) []string {
	if len(lines) > 3 {
		return lines[:3]
	}
	return lines
}
