package proxy

import (
	"fmt"
	"math"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/testutil/vlemu"
)

// first_over_time and last_over_time with unwrap on query_range when the range is longer than the step: Loki's
// sample at t is the first or the last sample of each series in the window (t-range, t], and the windows of two
// evaluation times overlap. The proxy reads the earliest (row_min) or the latest (row_max) row of each series in
// buckets of gcd(step, range) from one range before the start, and a window takes its earliest or latest bucket
// that holds a line (unwrapWindowBuckets, mergeUnwrapBuckets): a window whose latest step holds no line takes an
// earlier step's last, where one step bucket per point had none, or an earlier line's value.

// windowPicked reports whether the window evaluator read the bucket picks: a stats_query_range whose row of each
// bucket is taken again per group (rowBucketPick), and no raw rows.
func windowPicked(t *testing.T, label string, emu *vlemu.Emulator) {
	t.Helper()
	noFirstLastStatsFunction(t, label, emu)
	for _, q := range emu.EndpointCalls("/select/logsql/stats_query_range") {
		if strings.Contains(q, rowBucketUnpack) && strings.HasSuffix(q, rowBucketValue) {
			return
		}
	}
	t.Fatalf("%s: no bucket pick of the window evaluator in %q", label, emu.EndpointCalls("/select/logsql/stats_query_range"))
}

// firstLastQuery is agg (by the stream label by, "" for none; agg "" for the function alone) over fn with the
// window and offset off.
func firstLastQuery(fn, agg, by string, window, off time.Duration) string {
	inner := fmt.Sprintf(`%s({app="uw"} | unwrap amount [%s])`, fn, uwWindowText(window))
	if off > 0 {
		inner = fmt.Sprintf(`%s({app="uw"} | unwrap amount [%s] offset %s)`, fn, uwWindowText(window), uwWindowText(off))
	}
	switch {
	case agg == "":
		return inner
	case by != "":
		return fmt.Sprintf(`%s by (%s) (%s)`, agg, by, inner)
	}
	return fmt.Sprintf(`%s(%s)`, agg, inner)
}

// firstLastWant is Loki's answer to firstLastQuery: the function alone is a series per stream (the pod's sum over one
// series). An offset reads the windows off earlier and stamps them at the evaluation times.
func firstLastWant(rows []vlemu.Row, fn, agg, by string, window, off, step time.Duration, start, end int64, align bool) map[string]map[int64]float64 {
	if off > 0 {
		shifted := make([]vlemu.Row, len(rows))
		for i, row := range rows {
			row.TS += int64(off)
			shifted[i] = row
		}
		rows = shifted
	}
	if agg == "" {
		agg, by = "sum", "pod"
	}
	return firstLastRangeRef(rows, fn, agg, by, window, step, start, end, align)
}

// Edge cases of the windows: a window whose latest steps hold no line (last is an earlier step's), lines on the
// window's open start and its closed end and a nanosecond either side, a series of one sample (first and last are
// it), empty steps inside a window, a range that is not a multiple of the step, a start off the step grid, an
// offset, and no line in the request; every aggregation the evaluator folds after the windows and the function
// alone, ungrouped and by a stream label, both alignment profiles, VictoriaLogs v1.49.0 and v1.53.0. Every point
// carries Loki's timestamp and value, and none is missing or extra.
//
// conformance: semantics/unwrap-first-last-range-longer-than-step, window-bounds-and-step-alignment, loki_api_v1_query_range
func TestFirstLastOverTimeRangeLongerThanStep(t *testing.T) {
	base := int64(1_800_000_000) * 1e9 // a multiple of every step below
	sec := int64(time.Second)
	streams := []string{`{app="uw",pod="a"}`, `{app="uw",pod="b"}`}
	cases := []struct {
		name              string
		window, step, off time.Duration
		startOff          int64   // added to base: a start off the step grid
		at                []int64 // line offsets from base, in ns
	}{
		{"the latest steps of a window hold no line", 15 * time.Second, 5 * time.Second, 0, 0, []int64{sec, 2 * sec, 3*sec + 5e8, 4 * sec, 31 * sec, 32 * sec}},
		{"on the window's edges and a nanosecond either side", 15 * time.Second, 5 * time.Second, 0, 0, []int64{5 * sec, 5*sec + 1, 20*sec - 1, 20 * sec, 20*sec + 1, 35 * sec, 50 * sec}},
		{"a series of one sample", 20 * time.Second, 5 * time.Second, 0, 0, []int64{7*sec + 3e8, 7*sec + 4e8}},
		{"empty steps inside a window", 30 * time.Second, 5 * time.Second, 0, 0, []int64{sec, 2 * sec, 26 * sec, 27 * sec, 58 * sec, 59 * sec, 90 * sec}},
		{"a range that is not a multiple of the step", 7 * time.Second, 5 * time.Second, 0, 0, []int64{sec, 3 * sec, 6 * sec, 6*sec + 1, 9 * sec, 13 * sec, 14*sec + 5e8, 22 * sec}},
		{"a start off the step grid", 12 * time.Second, 5 * time.Second, 0, 3 * sec, []int64{2 * sec, 4 * sec, 8 * sec, 8*sec + 1, 16 * sec, 33*sec + 5e8}},
		{"an offset", 15 * time.Second, 5 * time.Second, 10 * time.Second, 0, []int64{sec, 4 * sec, 12 * sec, 17 * sec, 36 * sec, 44 * sec}},
		{"no line in the request", 15 * time.Second, 5 * time.Second, 0, 0, []int64{500 * sec}},
	}
	answered := 0
	for _, version := range []string{"v1.49.0", "v1.53.0"} {
		for _, align := range []bool{true, false} {
			for _, c := range cases {
				for _, fn := range []string{"first_over_time", "last_over_time"} {
					for _, agg := range []string{"", "sum", "avg", "min", "max", "count"} {
						for _, by := range []string{"", "pod"} {
							if agg == "" && by != "" {
								continue
							}
							name := fmt.Sprintf("%s/align=%v/%s/%s/%s/by=%s", version, align, c.name, fn, agg, by)
							t.Run(name, func(t *testing.T) {
								var rows []vlemu.Row
								for i, off := range c.at {
									// Distinct values, so a value read from the wrong step shows.
									rows = append(rows, vlemu.Row{TS: base + off, Stream: streams[i%len(streams)], Value: float64(i+1) * 1.5})
								}
								start, end := base+c.startOff, base+c.startOff+120*sec
								query := firstLastQuery(fn, agg, by, c.window, c.off)
								emu := &vlemu.Emulator{T: t, Rows: rows, Field: "amount", Version: version}
								run := runFirstLastConfig(t, emu, query, false, start, end, c.step, alignedConfig(align))
								windowPicked(t, query, emu)
								groupBy := by
								if agg == "" {
									groupBy = "pod"
								}
								want := firstLastWant(rows, fn, agg, by, c.window, c.off, c.step, start, end, align)
								if d := diffRangePoints(firstLastRangePoints(run, groupBy), want); d != "" {
									t.Fatalf("%s: %s", query, d)
								}
								if len(want) > 0 {
									answered++
								}
							})
						}
					}
				}
			}
		}
	}
	if answered == 0 {
		t.Fatal("no case produced a series: the test checks nothing")
	}
}

// A range up to the step keeps its step buckets (one window each), and a window longer than the step under an
// aggregation over a range by of its own is not merged (the folds do not take it): the window evaluator answers
// neither.
//
// conformance: semantics/unwrap-first-last-range-longer-than-step, semantics/unwrap-first-last-range-timestamps
func TestFirstLastOverTimeWindowRouteScreen(t *testing.T) {
	base := int64(1_800_000_000) * 1e9
	rows := []vlemu.Row{{TS: base + int64(time.Second), Stream: `{app="uw",pod="a"}`, Value: 2}}
	for _, c := range []struct {
		query        string
		window, step time.Duration
		picked       bool
	}{
		{`sum(first_over_time({app="uw"} | unwrap amount [15s]))`, 15 * time.Second, 5 * time.Second, true},
		{`sum(first_over_time({app="uw"} | unwrap amount [5s]))`, 5 * time.Second, 5 * time.Second, false},
		{`sum(last_over_time({app="uw"} | unwrap amount [2s]))`, 2 * time.Second, 5 * time.Second, false},
		{`sum by (pod) (last_over_time({app="uw"} | unwrap amount [15s]) by (pod))`, 15 * time.Second, 5 * time.Second, false},
	} {
		t.Run(c.query, func(t *testing.T) {
			emu := &vlemu.Emulator{T: t, Rows: rows, Field: "amount"}
			runFirstLastConfig(t, emu, c.query, false, base, base+60*int64(time.Second), c.step, nil)
			picked := false
			for _, q := range emu.EndpointCalls("/select/logsql/stats_query_range") {
				picked = picked || strings.HasSuffix(q, rowBucketValue)
			}
			if picked != c.picked {
				t.Fatalf("window evaluator %v, want %v: %q", picked, c.picked, emu.EndpointCalls("/select/logsql/stats_query_range"))
			}
		})
	}
}

// firstLastLongerScenario generates lines and a query_range request whose range is longer than the step, a
// multiple of it or not, with a start off the step grid one time in two and lines on step edges.
func firstLastLongerScenario(rnd *rand.Rand) (s uwScenario) {
	steps := []time.Duration{time.Second, 2 * time.Second, 5 * time.Second, 10 * time.Second, 30 * time.Second}
	s.step = steps[rnd.Intn(len(steps))]
	s.window = s.step * time.Duration(2+rnd.Intn(5))
	if rnd.Intn(2) == 0 {
		s.window += time.Duration(1+rnd.Int63n(int64(s.step/time.Second))) * time.Second / 2
	}
	base := int64(1_800_000_000) * 1e9
	s.start = base + int64(rnd.Intn(600))*1e9
	if rnd.Intn(2) == 0 {
		s.start -= s.start % int64(s.step)
	}
	s.end = s.start + int64(rnd.Intn(30))*int64(s.step) + int64(rnd.Intn(3))*1e9
	streams := []string{`{app="uw",pod="a"}`, `{app="uw",pod="b"}`, `{app="uw",pod="c"}`}
	values := []float64{0, 0.5, 1, 2.25, 3, 100.125, 7, -4}
	lo, hi := s.start-int64(s.window)-int64(s.step), s.end+int64(s.step)
	n := 1 + rnd.Intn(40)
	for i := 0; i < n; i++ {
		ts := lo + rnd.Int63n(hi-lo)
		if rnd.Intn(3) == 0 { // on a step edge or a nanosecond off it
			edge := s.start - s.start%int64(s.step) + int64(rnd.Intn(30))*int64(s.step)
			ts = edge + int64(rnd.Intn(3)-1)
		}
		s.rows = append(s.rows, vlemu.Row{TS: ts, Stream: streams[rnd.Intn(len(streams))], Value: values[rnd.Intn(len(values))]})
	}
	s.rows = firstLastRows(s)
	return s
}

// checkFirstLastLonger runs one generated scenario and compares every point with Loki's.
func checkFirstLastLonger(t *testing.T, s uwScenario, fn, agg, by string, off time.Duration, align bool) bool {
	t.Helper()
	query := firstLastQuery(fn, agg, by, s.window, off)
	emu := &vlemu.Emulator{T: t, Rows: s.rows, Field: "amount"}
	run := runFirstLastConfig(t, emu, query, false, s.start, s.end, s.step, alignedConfig(align))
	windowPicked(t, query, emu)
	groupBy := by
	if agg == "" {
		groupBy = "pod"
	}
	want := firstLastWant(s.rows, fn, agg, by, s.window, off, s.step, s.start, s.end, align)
	if d := diffRangePoints(firstLastRangePoints(run, groupBy), want); d != "" {
		t.Fatalf("%s step=%s align=%v start=%d end=%d: %s", query, s.step, align, s.start/1e9, s.end/1e9, d)
	}
	return len(want) > 0
}

// Differential: over generated lines and requests with a range longer than the step, both alignment profiles,
// every aggregation, the function alone, an offset one time in four and both functions, the proxy's query_range
// answer equals the Loki-semantics reference point for point.
//
// conformance: semantics/unwrap-first-last-range-longer-than-step, loki_api_v1_query_range
func TestFirstLastOverTimeRangeLongerThanStepDifferential(t *testing.T) {
	rnd := rand.New(rand.NewSource(20261114))
	compared := 0
	for i := 0; i < 160; i++ {
		s := firstLastLongerScenario(rnd)
		fn := []string{"first_over_time", "last_over_time"}[rnd.Intn(2)]
		agg := []string{"", "sum", "avg", "min", "max", "count"}[rnd.Intn(6)]
		by := ""
		if agg != "" && rnd.Intn(2) == 0 {
			by = "pod"
		}
		var off time.Duration
		if rnd.Intn(4) == 0 {
			off = s.step * time.Duration(1+rnd.Intn(3))
		}
		if checkFirstLastLonger(t, s, fn, agg, by, off, rnd.Intn(2) == 0) {
			compared++
		}
	}
	if compared < 80 {
		t.Fatalf("only %d scenarios produced a series", compared)
	}
}

// FuzzFirstLastOverTimeRangeLongerThanStep drives generated scenarios with a range longer than the step through the
// real handler against the emulated VictoriaLogs and compares every point with Loki's.
func FuzzFirstLastOverTimeRangeLongerThanStep(f *testing.F) {
	f.Add(int64(1), uint8(0), false)
	f.Add(int64(7), uint8(13), true)
	f.Add(int64(42), uint8(200), false)
	f.Fuzz(func(t *testing.T, seed int64, shape uint8, align bool) {
		s := firstLastLongerScenario(rand.New(rand.NewSource(seed)))
		fn := []string{"first_over_time", "last_over_time"}[int(shape)%2]
		agg := []string{"", "sum", "avg", "min", "max", "count"}[int(shape/2)%6]
		by := ""
		if agg != "" && shape >= 128 {
			by = "pod"
		}
		var off time.Duration
		if shape%5 == 0 {
			off = s.step
		}
		checkFirstLastLonger(t, s, fn, agg, by, off, align)
	})
}

// mergeUnwrapBuckets takes the window's earliest bucket for first and its latest for last, whatever the values: a
// NaN, an infinity or a zero is the window's value when its bucket is the one.
//
// conformance: semantics/unwrap-first-last-range-longer-than-step
func TestMergeUnwrapBucketsFirstLast(t *testing.T) {
	for _, c := range []struct {
		name        string
		values      []float64
		first, last float64
	}{
		{"one bucket", []float64{3}, 3, 3},
		{"several", []float64{4, -1, 9}, 4, 9},
		{"a zero first", []float64{0, 5}, 0, 5},
		{"a NaN last", []float64{1, math.NaN()}, 1, math.NaN()},
		{"an infinity first", []float64{math.Inf(-1), 2}, math.Inf(-1), 2},
	} {
		t.Run(c.name, func(t *testing.T) {
			buckets := make([]rangeMetricSample, len(c.values))
			for i, v := range c.values {
				buckets[i] = rangeMetricSample{ts: int64(i), value: v}
			}
			same := func(a, b float64) bool { return a == b || math.IsNaN(a) && math.IsNaN(b) }
			if got := mergeUnwrapBuckets("first", buckets, nil, nil); !same(got, c.first) {
				t.Errorf("first = %v, want %v", got, c.first)
			}
			if got := mergeUnwrapBuckets("last", buckets, nil, nil); !same(got, c.last) {
				t.Errorf("last = %v, want %v", got, c.last)
			}
		})
	}
	if rowBucketPick(avgBucketSum, " | stats ") != "" || rowBucketPick(deviationBucketStats, " | stats ") != "" {
		t.Fatal("only first and last read a bucket's row")
	}
	for _, agg := range []string{firstBucketStats, lastBucketStats} {
		if got, want := rowBucketPick(agg, " | stats by (pod) "), " "+rowBucketUnpack+" | stats by (pod) "+rowBucketValue; got != want {
			t.Fatalf("rowBucketPick(%q) = %q, want %q", agg, got, want)
		}
	}
}

// BenchmarkFirstLastRangeLongerThanStep measures first and last over time with a 30s range at a 5s step (the windows
// merged from 5s bucket picks) and controls on the window evaluator, through the real handler against the emulated
// VictoriaLogs over 15 minutes of two streams.
func BenchmarkFirstLastRangeLongerThanStep(b *testing.B) {
	base := int64(1_800_000_000) * 1e9
	var rows []vlemu.Row
	for i := 0; i < 2000; i++ {
		rows = append(rows, vlemu.Row{TS: base + int64(i)*450*int64(time.Millisecond), Stream: []string{`{app="uw",pod="a"}`, `{app="uw",pod="b"}`}[i%2], Value: float64(i % 97)})
	}
	shapes := []struct{ name, query string }{
		{"control_max_over_time_by_pod", `sum by (pod) (max_over_time({app="uw"} | unwrap amount [30s]))`},
		{"first_over_time_under_sum", `sum(first_over_time({app="uw"} | unwrap amount [30s]))`},
		{"last_over_time_by_pod", `sum by (pod) (last_over_time({app="uw"} | unwrap amount [30s]))`},
		{"last_over_time_bare", `last_over_time({app="uw"} | unwrap amount [30s])`},
	}
	for _, shape := range shapes {
		b.Run(shape.name, func(b *testing.B) {
			emu := &vlemu.Emulator{T: b, Rows: rows, Field: "amount"}
			backend := httptest.NewServer(emu.Handler())
			defer backend.Close()
			p, err := New(Config{BackendURL: backend.URL, LogLevel: "error", BackendMinVersion: "v1.49.0", BackendVersionCheckTimeout: time.Second, AlignQueriesWithStep: true})
			if err != nil {
				b.Fatal(err)
			}
			params := url.Values{"query": {shape.query}, "start": {strconv.FormatInt(base/1e9, 10)}, "end": {strconv.FormatInt(base/1e9+900, 10)}, "step": {"5"}}
			target := "/loki/api/v1/query_range?" + params.Encode()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if res := doCompatProxyRequest(p, target, map[string]string{"X-Scope-OrgID": "0"}); res.Code != http.StatusOK {
					b.Fatalf("%s: %d %s", shape.query, res.Code, res.Body)
				}
			}
			b.StopTimer()
			b.ReportMetric(float64(len(emu.Calls))/float64(b.N), "vl-calls/op")
		})
	}
}
