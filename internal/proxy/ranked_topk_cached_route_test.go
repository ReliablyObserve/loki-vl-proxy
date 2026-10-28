package proxy

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// A topk or bottomk over a grouped count or rate ranks every series at every
// step, so it needs each series' trailing-window value at each step. The
// stats_query_range buckets give those (the route every unranked window metric
// takes, answered by the stats extent cache on a repeat or a moved window); the
// three-grid recurrence over /select/logsql/query is the fallback for a answer
// the buckets cannot hold (more series than -max-stats-query-series, a
// Drilldown request that keeps partial series, no exact bucket grid).
//
// These tests count what VictoriaLogs is asked, and compare every answer with
// Loki's evaluation of the query (pkg/logql/evaluator.go: the sample at t covers
// (t-range, t], topk keeps the k largest of the series present at t) written
// apart from the proxy.

const rankedTopKEnd = 1700003600 // 2023-11-14T23:13:20Z, far past the freshness horizon

var rankedTopKApps = []string{"api-gateway", "web", "db-main", "cache", "queue"}

// rankedTopKLines is a fixture where the winners change over time: every app
// logs at its own pace, and queue bursts in the middle of the range.
func rankedTopKLines(seed int64, end time.Time, span time.Duration) []slidingFixtureLine {
	rng := rand.New(rand.NewSource(seed))
	var lines []slidingFixtureLine
	from := end.Add(-span)
	for i, app := range rankedTopKApps {
		pace := time.Duration(6+3*i) * time.Second
		for ts := from.Add(time.Duration(rng.Intn(5)) * time.Second); ts.Before(end.Add(time.Second)); ts = ts.Add(pace) {
			lines = append(lines, slidingFixtureLine{ts: ts.UnixNano(), app: app, msg: strings.Repeat("x", 1+i)})
		}
	}
	mid := from.Add(span / 2)
	for k := 0; k < 400; k++ {
		ts := mid.Add(time.Duration(rng.Intn(600)) * time.Second)
		lines = append(lines, slidingFixtureLine{ts: ts.UnixNano(), app: "queue", msg: "yyyyyyyy"})
	}
	sort.Slice(lines, func(i, j int) bool { return lines[i].ts < lines[j].ts })
	return lines
}

// rankedTopKReference is topk (or bottomk) of the count_over_time per app at
// every step. A step where the k-th and the next value are equal is ambiguous
// (Loki's heap picks one of them): its timestamp is returned in skip and left
// out of the comparison.
func rankedTopKReference(lines []slidingFixtureLine, fn string, k int, descending bool, start, end time.Time, step, window time.Duration) (want map[string]map[int64]string, skip map[int64]bool) {
	all := lokiSlidingReference(lines, fn, true, 0, start, end, step, window)
	want, skip = map[string]map[int64]string{}, map[int64]bool{}
	for ts := start; !ts.After(end); ts = ts.Add(step) {
		type entry struct {
			app string
			v   float64
		}
		var present []entry
		for app, points := range all {
			if s, ok := points[ts.Unix()]; ok {
				v, _ := strconv.ParseFloat(s, 64)
				present = append(present, entry{app, v})
			}
		}
		sort.Slice(present, func(i, j int) bool {
			if present[i].v != present[j].v {
				return (present[i].v > present[j].v) == descending
			}
			return present[i].app < present[j].app
		})
		if len(present) > k && present[k-1].v == present[k].v {
			skip[ts.Unix()] = true
		}
		for i := 0; i < len(present) && i < k; i++ {
			if want[present[i].app] == nil {
				want[present[i].app] = map[int64]string{}
			}
			want[present[i].app][ts.Unix()] = all[present[i].app][ts.Unix()]
		}
	}
	return want, skip
}

type rankedTopKSeries struct {
	Metric map[string]string `json:"metric"`
	Values [][]any           `json:"values"`
}

func decodeRankedTopK(t testing.TB, code int, body []byte) []rankedTopKSeries {
	t.Helper()
	if code != http.StatusOK {
		t.Fatalf("HTTP %d: %s", code, body)
	}
	var resp struct {
		Data struct {
			ResultType string             `json:"resultType"`
			Result     []rankedTopKSeries `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &resp); err != nil || resp.Data.ResultType != "matrix" {
		t.Fatalf("not a matrix (%v): %s", err, body)
	}
	return resp.Data.Result
}

// assertRankedTopK compares an answer with the reference; withShort expects the
// label_replace destination label next to app.
func assertRankedTopK(t testing.TB, label string, series []rankedTopKSeries, want map[string]map[int64]string, skip map[int64]bool, withShort bool) {
	t.Helper()
	got := map[string]map[int64]string{}
	for _, s := range series {
		app := s.Metric["app"]
		short, hasShort := s.Metric["app_short"]
		if withShort {
			wantShort := ""
			if i := strings.Index(app, "-"); i >= 0 && strings.HasPrefix(app, app[:i]+"-") {
				wantShort = app[:i]
			}
			if hasShort != (wantShort != "") || short != wantShort {
				t.Fatalf("%s: app=%q has app_short=%q (present %v), want %q", label, app, short, hasShort, wantShort)
			}
		} else if hasShort {
			t.Fatalf("%s: unexpected app_short on %v", label, s.Metric)
		}
		got[app] = map[int64]string{}
		for _, pair := range s.Values {
			ts, _ := pair[0].(float64)
			v, _ := pair[1].(string)
			if !skip[int64(ts)] {
				got[app][int64(ts)] = v
			}
		}
	}
	for app, points := range want {
		for ts, v := range points {
			if skip[ts] {
				continue
			}
			if gv, ok := got[app][ts]; !ok || gv != v {
				t.Fatalf("%s: app=%s ts=%d got %q (present %v), want %q", label, app, ts, gv, ok, v)
			}
		}
	}
	for app, points := range got {
		for ts, v := range points {
			if _, ok := want[app][ts]; !ok {
				t.Fatalf("%s: unexpected sample app=%s ts=%d value %s", label, app, ts, v)
			}
		}
	}
}

// rankedTopKStack is a proxy over the sliding fake VictoriaLogs behind the
// extent shim, with the extent cache on or off.
type rankedTopKStack struct {
	p     *Proxy
	fake  *slidingFakeVL
	shim  *extentShim
	lines []slidingFixtureLine
}

func newRankedTopKStack(t *testing.T, lines []slidingFixtureLine, extent bool, cfg func(*Config)) *rankedTopKStack {
	t.Helper()
	srv, fake := newSlidingFakeVL(t, lines)
	shimSrv, shim := newExtentShim(t, srv.Config.Handler)
	c := cache.NewWithMaxBytes(time.Minute, 100000, 256<<20)
	t.Cleanup(c.Close)
	conf := Config{BackendURL: shimSrv.URL, Cache: c, LogLevel: "error", QueryRangeExtentCache: extent, QueryRangeExtentCacheTTL: time.Hour}
	if cfg != nil {
		cfg(&conf)
	}
	p, err := New(conf)
	if err != nil {
		t.Fatalf("proxy: %v", err)
	}
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	p.storeBackendVersion("v1.50.0", "v1.50.0")
	return &rankedTopKStack{p: p, fake: fake, shim: shim, lines: lines}
}

// vlCalls is what VictoriaLogs was asked so far: stats_query_range bucket
// calls, window-recurrence grid fetches (/select/logsql/query stats pipes) and
// raw row scans.
func (s *rankedTopKStack) vlCalls() (buckets, grids, raw int) {
	calls, rawCalls := s.fake.snapshot()
	for _, c := range calls {
		if strings.Contains(c.query, "_time:") {
			grids++
		} else {
			buckets++
		}
	}
	return buckets, grids, rawCalls
}

func (s *rankedTopKStack) ask(t testing.TB, query string, start, end time.Time, step time.Duration, header http.Header) (int, []byte) {
	return extentQueryRange(t, s.p, query, start, end, step, header)
}

const rankedTopKInner = `sum by (app) (count_over_time({env="production"}[%s]))`
const rankedTopKReplace = `label_replace(` + rankedTopKInner + `, "app_short", "$1", "app", "(.*)-.*")`

// rankedTopKFn is the metric of the differential: the four window functions the ranked route serves.
func rankedTopKQuery(fn, rangeStr string, short bool) string {
	inner := fmt.Sprintf(`sum by (app) (%s({env="production"}[%s]))`, fn, rangeStr)
	if short {
		inner = `label_replace(` + inner + `, "app_short", "$1", "app", "(.*)-.*")`
	}
	return inner
}

// The L3 shape of the label_replace A/B set, topk over label_replace over a
// grouped count: the first request asks VictoriaLogs for stats buckets only
// (one call, no window recurrence), the identical repeat asks it for nothing,
// and the same query moved back a minute reads only the buckets it lacks.
// conformance: semantics/label-replace-new-label, quality/query-range-extent-cache, loki_api_v1_query_range
func TestRankedTopKRangeIsServedFromTheBucketCache(t *testing.T) {
	end := time.Unix(rankedTopKEnd, 0).UTC()
	lines := rankedTopKLines(1, end, 3*time.Hour)
	step, window := 15*time.Second, 5*time.Minute
	for _, tc := range []struct {
		name, query string
		short       bool
		k           int
		desc        bool
	}{
		{"topk over label_replace", `topk(3, ` + fmt.Sprintf(rankedTopKReplace, "5m") + `)`, true, 3, true},
		{"topk over the sum", `topk(3, ` + fmt.Sprintf(rankedTopKInner, "5m") + `)`, false, 3, true},
		{"bottomk over label_replace", `bottomk(2, ` + fmt.Sprintf(rankedTopKReplace, "5m") + `)`, true, 2, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newRankedTopKStack(t, lines, true, nil)
			start := end.Add(-time.Hour)
			check := func(label string, start, end time.Time) {
				code, body := s.ask(t, tc.query, start, end, step, nil)
				want, skip := rankedTopKReference(lines, "count_over_time", tc.k, tc.desc, start, end, step, window)
				assertRankedTopK(t, label, decodeRankedTopK(t, code, body), want, skip, tc.short)
			}

			check("cold", start, end)
			buckets, grids, raw := s.vlCalls()
			t.Logf("cold: %d stats_query_range calls, %d window grid fetches, %d raw scans", buckets, grids, raw)
			if grids != 0 || raw != 0 || buckets == 0 {
				t.Fatalf("cold request must read stats buckets only: %d bucket calls, %d grid fetches, %d raw scans", buckets, grids, raw)
			}

			before := buckets + grids + raw
			check("repeat", start, end)
			buckets, grids, raw = s.vlCalls()
			t.Logf("identical repeat: %d VictoriaLogs calls", buckets+grids+raw-before)
			if buckets+grids+raw != before {
				t.Fatalf("an identical repeat asked VictoriaLogs %d more times, want 0", buckets+grids+raw-before)
			}

			s.shim.reset()
			before = buckets + grids + raw
			check("moved back one minute", start.Add(-time.Minute), end.Add(-time.Minute))
			_, read := s.shim.bucketsRead()
			buckets, grids, raw = s.vlCalls()
			t.Logf("window moved back a minute: %d VictoriaLogs calls, %d buckets read of %d", buckets+grids+raw-before, read, int64(time.Hour/step)+int64(window/step))
			if grids != 0 || raw != 0 || read > int64(time.Hour/step)/2 {
				t.Fatalf("a moved window must read only the buckets it lacks: %d grid fetches, %d raw scans, %d buckets", grids, raw, read)
			}
		})
	}
}

// The ranking is the same with and without the cache and the same as Loki's,
// over random windows, steps (the range under, equal to and over the step),
// k and direction, with and without label_replace around the sum.
// conformance: semantics/label-replace-new-label, quality/query-range-extent-cache, loki_api_v1_query_range
func TestRankedTopKRangeDifferential(t *testing.T) {
	end := time.Unix(rankedTopKEnd, 0).UTC()
	lines := rankedTopKLines(2, end, 3*time.Hour)
	on := newRankedTopKStack(t, lines, true, nil)
	off := newRankedTopKStack(t, lines, false, nil)
	rng := rand.New(rand.NewSource(3))
	steps := []time.Duration{15 * time.Second, 30 * time.Second, time.Minute, 150 * time.Second, 10 * time.Minute}
	ranges := []string{"2m", "5m", "10m", "1m"}
	for i := 0; i < 40; i++ {
		step := steps[rng.Intn(len(steps))]
		rangeStr := ranges[rng.Intn(len(ranges))]
		window, _ := time.ParseDuration(rangeStr)
		k := 1 + rng.Intn(4)
		fn, desc := "topk", true
		if rng.Intn(3) == 0 {
			fn, desc = "bottomk", false
		}
		metric := []string{"count_over_time", "rate", "bytes_over_time", "bytes_rate"}[rng.Intn(4)]
		short := rng.Intn(2) == 0
		query := fmt.Sprintf("%s(%d, %s)", fn, k, rankedTopKQuery(metric, rangeStr, short))
		span := time.Duration(20+rng.Intn(60)) * step
		// Windows move back and forth over one grid (a refresh and a re-run) and
		// sometimes start off the step grid.
		endAt := end.Add(-time.Duration(rng.Intn(40)) * step).Truncate(step)
		if rng.Intn(4) == 0 {
			endAt = endAt.Add(time.Duration(rng.Intn(int(step/time.Second))) * time.Second)
		}
		start := endAt.Add(-span)
		name := fmt.Sprintf("%s [%s,%s] step %s", query, start.Format("15:04:05"), endAt.Format("15:04:05"), step)
		_, gridsBefore, _ := on.vlCalls()
		onCode, onBody := on.ask(t, query, start, endAt, step, nil)
		if _, g, _ := on.vlCalls(); g != gridsBefore && window != step {
			t.Fatalf("%s: a range under or over the step fell back to the window recurrence", name)
		}
		offCode, offBody := off.ask(t, query, start, endAt, step, nil)
		if onCode != offCode || !bytes.Equal(onBody, offBody) {
			t.Fatalf("%s: the cache changed the answer\n with: %d %s\nwithout: %d %s", name, onCode, onBody, offCode, offBody)
		}
		want, skip := rankedTopKReference(lines, metric, k, desc, start, endAt, step, window)
		assertRankedTopK(t, name, decodeRankedTopK(t, onCode, onBody), want, skip, short)
	}
	buckets, grids, _ := on.vlCalls()
	t.Logf("cached proxy: %d bucket calls, %d grid fetches (tumbling windows read their one grid)", buckets, grids)
	if outcomes := extentOutcomes(t, on.p); outcomes["hit"]+outcomes["partial"] == 0 {
		t.Fatalf("no request was answered from stored buckets: %v", outcomes)
	}
}

// The buckets cannot hold the answer when the grouping yields more series than
// -max-stats-query-series (topk ranks all of them, Loki's limit applies to the
// answer), or for a Drilldown request, which keeps the busiest series with a
// warning: the window recurrence ranks those, and the answer is still Loki's.
// conformance: semantics/label-replace-new-label, series-limits-and-partial-results, limits/top-n-ranking-not-silent
func TestRankedTopKRangeFallsBackToTheWindowRecurrence(t *testing.T) {
	end := time.Unix(rankedTopKEnd, 0).UTC()
	lines := rankedTopKLines(4, end, 3*time.Hour)
	start, step, window := end.Add(-time.Hour), 15*time.Second, 5*time.Minute
	query := `topk(2, ` + fmt.Sprintf(rankedTopKInner, "5m") + `)`
	want, skip := rankedTopKReference(lines, "count_over_time", 2, true, start, end, step, window)

	t.Run("more series than the limit", func(t *testing.T) {
		s := newRankedTopKStack(t, lines, true, func(c *Config) { c.MaxStatsQuerySeries = 3 })
		code, body := s.ask(t, query, start, end, step, nil)
		assertRankedTopK(t, "over the limit", decodeRankedTopK(t, code, body), want, skip, false)
		buckets, grids, _ := s.vlCalls()
		if buckets != 1 || grids != 3 {
			t.Fatalf("cold: %d bucket calls and %d grid fetches, want 1 and 3 (seed, leaving, entering)", buckets, grids)
		}
		// The verdict is remembered: the repeat does not read the buckets again.
		code, body = s.ask(t, query, start, end, step, nil)
		assertRankedTopK(t, "over the limit, repeat", decodeRankedTopK(t, code, body), want, skip, false)
		if b, g, _ := s.vlCalls(); b != 1 || g != 6 {
			t.Fatalf("repeat: %d bucket calls and %d grid fetches in all, want 1 and 6", b, g)
		}
	})
	t.Run("a failing backend is the answer, not a heavier read", func(t *testing.T) {
		s := newRankedTopKStack(t, lines, true, nil)
		s.shim.fail = func(start, end int64) bool { return true }
		code, body := s.ask(t, query, start, end, step, nil)
		if code < 500 {
			t.Fatalf("HTTP %d: %s, want the backend failure", code, body)
		}
		if _, grids, raw := s.vlCalls(); grids != 0 || raw != 0 {
			t.Fatalf("a failing backend was asked %d grid fetches and %d raw scans", grids, raw)
		}
	})
	t.Run("drilldown keeps partial series", func(t *testing.T) {
		s := newRankedTopKStack(t, lines, true, nil)
		code, body := s.ask(t, query, start, end, step, http.Header{"X-Query-Tags": {"Source=grafana-lokiexplore-app"}})
		assertRankedTopK(t, "drilldown", decodeRankedTopK(t, code, body), want, skip, false)
		if _, grids, _ := s.vlCalls(); grids != 3 {
			t.Fatalf("window grid fetches = %d, want 3", grids)
		}
	})
}

// A label_replace and a topk in the other nesting: the answers repeat byte for
// byte and a repeat asks VictoriaLogs no more than the first request did.
// conformance: semantics/label-replace-new-label, loki_api_v1_query_range
func TestLabelReplaceTopKRepeatsAreStable(t *testing.T) {
	end := time.Unix(rankedTopKEnd, 0).UTC()
	lines := rankedTopKLines(5, end, 3*time.Hour)
	s := newRankedTopKStack(t, lines, true, nil)
	start, step := end.Add(-time.Hour), 15*time.Second
	for _, query := range []string{
		`label_replace(topk(3, ` + fmt.Sprintf(rankedTopKInner, "5m") + `), "app_short", "$1", "app", "(.*)-.*")`,
		`topk(3, ` + fmt.Sprintf(rankedTopKReplace, "5m") + `)`,
		fmt.Sprintf(rankedTopKReplace, "5m"),
		`sort_desc(` + fmt.Sprintf(rankedTopKReplace, "5m") + `)`,
	} {
		t.Run(query, func(t *testing.T) {
			code, first := s.ask(t, query, start, end, step, nil)
			if code != http.StatusOK {
				t.Fatalf("HTTP %d: %s", code, first)
			}
			b, g, r := s.vlCalls()
			afterFirst := b + g + r
			code, second := s.ask(t, query, start, end, step, nil)
			b, g, r = s.vlCalls()
			if code != http.StatusOK || !bytes.Equal(first, second) {
				t.Fatalf("repeat differs:\n first: %s\nsecond: %d %s", first, code, second)
			}
			t.Logf("first request and repeat: %d and %d VictoriaLogs calls (%d window grid fetches in all)", afterFirst, b+g+r-afterFirst, g)
			if b+g+r != afterFirst {
				t.Fatalf("a repeat asked VictoriaLogs %d more times, want 0", b+g+r-afterFirst)
			}
		})
	}
}

// label_replace may give two series one label set; Loki ranks the samples (both
// are kept at each step) and files the survivors under one series, so
// topk(1, ...) answers the larger of the two at each step, one point per step
// (pkg/logql/evaluator.go VectorAggEvaluator.Next, pkg/logql/engine.go
// JoinSampleVector). Reading the buckets must not change that.
// conformance: semantics/label-replace-new-label, semantics/label-replace-duplicates-before-ranking, loki_api_v1_query_range
func TestRankedTopKOverLabelSetsLabelReplaceMadeEqual(t *testing.T) {
	end := time.Unix(rankedTopKEnd, 0).UTC()
	lines := rankedTopKLines(6, end, 3*time.Hour)
	start, step, window := end.Add(-time.Hour), 15*time.Second, 5*time.Minute
	s := newRankedTopKStack(t, lines, true, nil)
	for _, fn := range []string{"topk", "bottomk"} {
		query := fn + `(1, label_replace(sum by (app) (count_over_time({env="production"}[5m])), "app", "all", "app", ".+"))`
		code, body := s.ask(t, query, start, end, step, nil)
		got := decodeRankedTopK(t, code, body)
		want, skip := rankedTopKReference(lines, "count_over_time", 1, fn == "topk", start, end, step, window)
		if len(got) != 1 || len(got[0].Metric) != 1 || got[0].Metric["app"] != "all" {
			t.Fatalf("%s: %d series, want one {app=\"all\"}: %s", fn, len(got), body)
		}
		points := map[int64]string{}
		for _, pair := range got[0].Values {
			ts, _ := pair[0].(float64)
			if _, dup := points[int64(ts)]; dup {
				t.Fatalf("%s: two points at %d: %s", fn, int64(ts), body)
			}
			points[int64(ts)] = pair[1].(string)
		}
		ranked := map[int64]string{}
		for _, perApp := range want {
			for ts, v := range perApp {
				ranked[ts] = v
			}
		}
		for ts, v := range ranked {
			if !skip[ts] && points[ts] != v {
				t.Fatalf("%s: ts=%d got %q want %q", fn, ts, points[ts], v)
			}
		}
		if len(points) != len(ranked) {
			t.Fatalf("%s: %d points, want %d", fn, len(points), len(ranked))
		}
	}
}
