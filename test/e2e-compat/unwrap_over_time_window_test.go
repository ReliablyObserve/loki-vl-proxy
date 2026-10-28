//go:build e2e

package e2e_compat

import (
	"fmt"
	"math"
	"strconv"
	"sync"
	"testing"
	"time"
)

// Loki evaluates sum, avg, max and min over time on the window (t-range, t] of
// every step (pkg/logql/range_vector.go:102-189 batchRangeVectorIterator Next
// and load: a line on the lower edge belongs to the window before, the line at
// t to this one; range_vector.go:371-420 the functions), whatever the range
// and the step are, and an instant query on the one window (time-range, time].
// The proxy answered a range longer than the step from one VictoriaLogs step
// bucket (semantics/unwrap-over-time-range-windows). The shared fixture
// streams hold a line every 10s with a distinct amount, so every window value
// depends on which lines it holds; each `by (app, route)` group is one Loki
// series (the stream and the parsed route; msg is constant). The edge fixture
// puts a line on every 30s mark, so every window edge of these shapes holds one.

var unwrapWindowShapes = []unwrapEndShape{
	{"sliding", "5m", time.Minute, 6 * time.Minute, 26 * time.Minute},
	{"tumbling", "5m", 5 * time.Minute, 5 * time.Minute, 25 * time.Minute},
	{"range shorter than the step", "2m", 5 * time.Minute, 5 * time.Minute, 25 * time.Minute},
	{"range longer than the step", "10m", 3 * time.Minute, 12 * time.Minute, 27 * time.Minute},
	{"range that is not a multiple of the step", "90s", time.Minute, 5 * time.Minute, 25 * time.Minute},
	{"range shorter than the step, not a divisor", "45s", 2 * time.Minute, 4 * time.Minute, 28 * time.Minute},
}

type unwrapWindowEdgeFixtures struct {
	selLogfmt, selJSON string
	s0                 time.Time
}

var (
	unwrapWindowEdgeOnce sync.Once
	unwrapWindowEdge     *unwrapWindowEdgeFixtures
)

// ensureUnwrapWindowEdgeFixtures ingests one logfmt and one JSON stream with a
// line on every 30s mark for an hour, each with its own amount.
func ensureUnwrapWindowEdgeFixtures(t *testing.T) *unwrapWindowEdgeFixtures {
	t.Helper()
	unwrapWindowEdgeOnce.Do(func() {
		now := time.Now()
		id := now.UnixNano()
		fx := &unwrapWindowEdgeFixtures{
			s0:        now.Add(-8 * time.Hour).Truncate(time.Hour),
			selLogfmt: fmt.Sprintf(`{app="uwwin-lf-%d"}`, id),
			selJSON:   fmt.Sprintf(`{app="uwwin-js-%d"}`, id),
		}
		var all []slidingLiveFixture
		for _, format := range []string{"lf", "js"} {
			stream := slidingLiveFixture{app: fmt.Sprintf("uwwin-%s-%d", format, id)}
			for i := 0; i < 120; i++ {
				amount := float64((i*7)%13) + 0.25
				msg := fmt.Sprintf(`msg=edge amount=%v`, amount)
				if format == "js" {
					msg = fmt.Sprintf(`{"msg":"edge","amount":%v}`, amount)
				}
				stream.lines = append(stream.lines, slidingLiveLine{ts: fx.s0.Add(time.Duration(i) * 30 * time.Second), msg: msg})
			}
			all = append(all, stream)
		}
		ingestSlidingFixtures(t, all...)
		unwrapWindowEdge = fx
	})
	if unwrapWindowEdge == nil {
		t.Fatal("the unwrap window edge fixtures were not ingested; see the first test that ran")
	}
	fx := unwrapWindowEdge
	// Loki's range metric path answers empty for a few minutes on a fresh stack.
	deadline := time.Now().Add(240 * time.Second)
	for _, query := range []string{
		fmt.Sprintf(`sum(rate(%s | logfmt | unwrap amount [5m]))`, fx.selLogfmt),
		fmt.Sprintf(`sum(rate(%s | json | unwrap amount [5m]))`, fx.selJSON),
	} {
		at := fx.s0.Add(30 * time.Minute)
		for len(slidingRangeSeries(t, lokiURL, query, at.Add(-time.Minute), at, time.Minute, nil)) == 0 {
			if time.Now().After(deadline) {
				t.Fatalf("Loki's range metric path answered nothing for %s after 240s", query)
			}
			time.Sleep(2 * time.Second)
		}
	}
	return fx
}

// compareWindowSeries asserts the proxy's series, timestamps and values (to
// 1e-9 relative) are Loki's, and that Loki's answer is not empty.
func compareWindowSeries(t *testing.T, query string, loki, proxy map[string]map[string]string) {
	t.Helper()
	if len(loki) == 0 {
		t.Fatalf("%s: Loki answered nothing; parity would be empty-vs-empty", query)
	}
	if len(proxy) != len(loki) {
		t.Errorf("%s: Loki has %d series, proxy %d", query, len(loki), len(proxy))
	}
	for key, want := range loki {
		got, ok := proxy[key]
		if !ok {
			t.Errorf("%s: series {%s} missing from the proxy", query, key)
			continue
		}
		if fmt.Sprint(sortedSampleTimes(got)) != fmt.Sprint(sortedSampleTimes(want)) {
			t.Errorf("%s {%s}: proxy has samples at %v, Loki at %v", query, key, sortedSampleTimes(got), sortedSampleTimes(want))
		}
		for ts, w := range want {
			wv, _ := strconv.ParseFloat(w, 64)
			gv, err := strconv.ParseFloat(got[ts], 64)
			if err != nil || math.Abs(gv-wv) > 1e-9*math.Max(1, math.Abs(wv)) {
				t.Errorf("%s {%s} at %s: proxy %q, Loki %q", query, key, ts, got[ts], w)
			}
		}
	}
}

// TestCompat_UnwrapOverTimeRangeWindows: every series, timestamp and value of
// sum, avg, max and min over time with unwrap is Loki's, over a logfmt and a
// JSON stream group, grouped by series, ungrouped (sum over time), with an
// offset, for ranges equal to, longer and shorter than the step and not a
// multiple of it, and the max or min of a group's lines (max by, min by and
// an ungrouped max over max or min over time); over lines on every window edge,
// on query_range and at an instant on an edge.
// conformance: semantics/unwrap-over-time-range-windows, semantics/lower-bound-exclusive, window-bounds-and-step-alignment, loki_api_v1_query_range, loki_api_v1_query
func TestCompat_UnwrapOverTimeRangeWindows(t *testing.T) {
	fx := ensureUnwrapSumFixtures(t)
	fx.waitForLoki(t)
	functions := []string{"sum_over_time", "avg_over_time", "max_over_time", "min_over_time"}
	for _, g := range []struct{ name, selector, parser string }{
		{"logfmt", fx.selLogfmt, "logfmt"},
		{"json", fx.selJSON, "json"},
	} {
		for _, shape := range unwrapWindowShapes {
			var queries []string
			for _, function := range functions {
				queries = append(queries,
					fmt.Sprintf(`sum by (app, route) (%s(%s | %s | unwrap amount | __error__="" [%s]))`, function, g.selector, g.parser, shape.window),
					fmt.Sprintf(`sum by (app, route) (%s(%s | %s | unwrap amount | __error__="" [%s] offset 2m))`, function, g.selector, g.parser, shape.window))
			}
			queries = append(queries,
				fmt.Sprintf(`sum(sum_over_time(%s | %s | unwrap amount [%s]))`, g.selector, g.parser, shape.window),
				// The extreme of a group's lines, grouped upstream whatever the series.
				fmt.Sprintf(`max by (app) (max_over_time(%s | %s | unwrap amount [%s]))`, g.selector, g.parser, shape.window),
				fmt.Sprintf(`min by (route) (min_over_time(%s | %s | unwrap amount [%s]))`, g.selector, g.parser, shape.window),
				fmt.Sprintf(`max(max_over_time(%s | %s | unwrap amount [%s]))`, g.selector, g.parser, shape.window))
			for _, query := range queries {
				t.Run(g.name+"/"+shape.name+"/"+query, func(t *testing.T) {
					start, end := fx.s0.Add(shape.start), fx.s0.Add(shape.finish)
					compareWindowSeries(t, query, slidingRangeSeries(t, lokiURL, query, start, end, shape.step, nil), slidingRangeSeries(t, proxyURL, query, start, end, shape.step, nil))
				})
			}
		}
	}

	edge := ensureUnwrapWindowEdgeFixtures(t)
	for _, g := range []struct{ name, selector, parser string }{
		{"edge logfmt", edge.selLogfmt, "logfmt"},
		{"edge json", edge.selJSON, "json"},
	} {
		for _, shape := range []struct {
			window string
			step   time.Duration
		}{{"1m", time.Minute}, {"2m", time.Minute}, {"150s", time.Minute}, {"30s", time.Minute}, {"5m", 2 * time.Minute}} {
			for _, form := range []string{"sum by (app) (%s(%s | %s | unwrap amount [%s]))", "max by (app) (%s(%s | %s | unwrap amount [%s]))", "max(%s(%s | %s | unwrap amount [%s]))"} {
				for _, function := range functions {
					if form != "sum by (app) (%s(%s | %s | unwrap amount [%s]))" && function != "max_over_time" {
						continue
					}
					query := fmt.Sprintf(form, function, g.selector, g.parser, shape.window)
					t.Run(g.name+"/"+query, func(t *testing.T) {
						start, end := edge.s0.Add(10*time.Minute), edge.s0.Add(50*time.Minute)
						compareWindowSeries(t, query, slidingRangeSeries(t, lokiURL, query, start, end, shape.step, nil), slidingRangeSeries(t, proxyURL, query, start, end, shape.step, nil))
					})
					t.Run(g.name+"/instant/"+query, func(t *testing.T) {
						at := edge.s0.Add(20 * time.Minute)
						wrap := func(m map[string]string) map[string]map[string]string {
							out := map[string]map[string]string{}
							for key, value := range m {
								out[key] = map[string]string{strconv.FormatInt(at.Unix(), 10): value}
							}
							return out
						}
						compareWindowSeries(t, query, wrap(unwrapInstant(t, lokiURL, query, at)), wrap(unwrapInstant(t, proxyURL, query, at)))
					})
				}
			}
		}
	}
}
