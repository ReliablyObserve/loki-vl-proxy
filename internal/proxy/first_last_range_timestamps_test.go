package proxy

import (
	"fmt"
	"math"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
)

// This file pins the evaluation timestamps of first_over_time and
// last_over_time with unwrap on query_range to Loki v3.7.9's. Loki evaluates a
// range aggregation at start, start+step, ..., end (pkg/logql/range_vector.go:
// 102-113 batchRangeVectorIterator.Next); the sample at t reads the window
// (t-range, t], the lower bound exclusive (range_vector.go:150-160 load), and is
// stamped t (range_vector.go:189 At, r.current), first or last of the window
// in timestamp order (range_vector.go:489-500). A range shorter than the step
// reads the same window (range_vector.go:54-73, streamRangeVectorIterator).
// The sharded first/last merge that keeps sample timestamps
// (first_last_over_time.go) runs only under shard_aggregations, which is off by
// default (shardmapper.go:30-48, 616). With align_queries_with_step the query
// frontend moves start and end down to multiples of the step first.
//
// VictoriaLogs labels a stats_query_range bucket by its start and the bucket
// covers [T, T+step): without the proxy's relabelling every point is one step
// early, and a line on a step edge lands in the next step.

// firstLastRangeRef is Loki's query_range answer for agg (by the stream label
// by, "" for none) over fn with the given window: group -> timestamp -> value.
func firstLastRangeRef(rows []emuRow, fn, agg, by string, window, step time.Duration, start, end int64, align bool) map[string]map[int64]float64 {
	if align {
		start -= start % int64(step)
		end -= end % int64(step)
	}
	out := map[string]map[int64]float64{}
	for at := start; at <= end; at += int64(step) {
		for g, v := range firstLastRef(rows, fn, agg, by, int64(window), at) {
			if out[g] == nil {
				out[g] = map[int64]float64{}
			}
			out[g][at] = v
		}
	}
	return out
}

// firstLastRangePoints is the proxy's answer keyed like firstLastRangeRef.
func firstLastRangePoints(run firstLastRun, by string) map[string]map[int64]float64 {
	out := map[string]map[int64]float64{}
	for i, metric := range run.series {
		out[metric[by]] = run.points[i]
	}
	return out
}

// diffRangePoints describes how got differs from want ("" when equal).
func diffRangePoints(got, want map[string]map[int64]float64) string {
	var diffs []string
	keys := map[string]bool{}
	for g := range got {
		keys[g] = true
	}
	for g := range want {
		keys[g] = true
	}
	for g := range keys {
		ts := map[int64]bool{}
		for t := range got[g] {
			ts[t] = true
		}
		for t := range want[g] {
			ts[t] = true
		}
		for t := range ts {
			gv, gok := got[g][t]
			wv, wok := want[g][t]
			if gok != wok || (gok && math.Abs(gv-wv) > 1e-9*math.Max(1, math.Abs(wv))) {
				diffs = append(diffs, fmt.Sprintf("%q@%d: got %v (%v) want %v (%v)", g, t/1e9, gv, gok, wv, wok))
			}
		}
	}
	sort.Strings(diffs)
	if len(diffs) > 6 {
		diffs = append(diffs[:6], fmt.Sprintf("... %d more", len(diffs)-6))
	}
	return strings.Join(diffs, "; ")
}

func alignedConfig(align bool) func(*Config) {
	return func(cfg *Config) { cfg.AlignQueriesWithStep = align }
}

// Edge cases: lines inside a step, on a step edge (the window's inclusive end)
// and one nanosecond either side, a range equal to and shorter than the step,
// a start that is not a multiple of the step, both alignment profiles, every
// aggregation the translation answers over first and last, grouped and not,
// with and without a parser, on VictoriaLogs v1.49.0 and v1.53.0. Every point
// carries Loki's timestamp and value, and none is missing or extra.
//
// conformance: semantics/unwrap-first-last-range-timestamps, semantics/unwrap-first-last-under-aggregation, loki_api_v1_query_range
func TestFirstLastOverTimeRangeTimestampsLikeLoki(t *testing.T) {
	base := int64(1_800_000_000) * 1e9 // a multiple of every step below
	sec := int64(time.Second)
	streams := []string{`{app="uw",pod="a"}`, `{app="uw",pod="b"}`}
	cases := []struct {
		name         string
		window, step time.Duration
		startOff     int64   // added to base: a start off the step grid
		at           []int64 // line offsets from base, in ns
	}{
		{"inside a step: stamped at the step that closes it", time.Second, time.Second, 0, []int64{2*sec + 5e8, 7*sec + 3e8, 40*sec + 9e8}},
		{"on a step edge: the window's inclusive end", 5 * time.Second, 5 * time.Second, 0, []int64{10 * sec, 25 * sec, 25*sec + 1, 30*sec - 1}},
		{"range equal to the step, several lines per window", 5 * time.Second, 5 * time.Second, 0, []int64{sec, 2 * sec, 4*sec + 5e8, 5 * sec, 6 * sec, 9 * sec, 60 * sec}},
		{"range shorter than the step: lines before the window are out", 3 * time.Second, 10 * time.Second, 0, []int64{sec, 6 * sec, 7 * sec, 7*sec + 1, 10 * sec, 18 * sec, 29 * sec}},
		{"start off the step grid", 5 * time.Second, 5 * time.Second, 3 * sec, []int64{4 * sec, 8 * sec, 8*sec + 1, 13 * sec, 33*sec + 5e8}},
		{"start off the grid, range shorter than the step", 2 * time.Second, 10 * time.Second, 7 * sec, []int64{5 * sec, 6 * sec, 16 * sec, 17 * sec, 26 * sec, 36*sec + 5e8}},
		{"lines on the first and last evaluation times", 5 * time.Second, 5 * time.Second, 0, []int64{0, 120 * sec}},
		{"no line in the request", 5 * time.Second, 5 * time.Second, 0, []int64{500 * sec}},
	}
	answered := 0
	for _, version := range []string{"v1.49.0", "v1.53.0"} {
		for _, align := range []bool{true, false} {
			for _, c := range cases {
				for _, parser := range []string{"", "logfmt"} {
					for _, fn := range []string{"first_over_time", "last_over_time"} {
						for _, agg := range []string{"sum", "avg", "min", "max", "count"} {
							for _, by := range []string{"", "pod"} {
								name := fmt.Sprintf("%s/align=%v/%s/parser=%s/%s/%s/by=%s", version, align, c.name, parser, fn, agg, by)
								t.Run(name, func(t *testing.T) {
									var s uwScenario
									s.parser = parser
									for i, off := range c.at {
										s.rows = append(s.rows, emuRow{ts: base + off, stream: streams[i%len(streams)], value: float64(i+1) * 1.5})
									}
									s.rows = firstLastRows(s)
									start, end := base+c.startOff, base+c.startOff+120*sec
									inner := fmt.Sprintf(`%s({app="uw"}%s | unwrap amount [%s])`, fn, parserPipe(parser), uwWindowText(c.window))
									query := fmt.Sprintf(`%s(%s)`, agg, inner)
									if by != "" {
										query = fmt.Sprintf(`%s by (%s) (%s)`, agg, by, inner)
									}
									emu := &vlEmulator{t: t, rows: s.rows, field: "amount", version: version}
									run := runFirstLastConfig(t, emu, query, false, start, end, c.step, alignedConfig(align))
									noFirstLastStatsFunction(t, query, emu)
									want := firstLastRangeRef(s.rows, fn, agg, by, c.window, c.step, start, end, align)
									if d := diffRangePoints(firstLastRangePoints(run, by), want); d != "" {
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
	}
	if answered == 0 {
		t.Fatal("no case produced a series: the test checks nothing")
	}
}

// A binary operation over first or last, the bare function and a topk of
// grouped values read the same relabelled buckets: Loki's timestamps and values.
//
// conformance: semantics/unwrap-first-last-range-timestamps, loki_api_v1_query_range
func TestFirstLastOverTimeRangeTimestampsBinaryAndTopk(t *testing.T) {
	base := int64(1_800_000_000) * 1e9
	sec := int64(time.Second)
	var rows []emuRow
	for i, off := range []int64{sec + 5e8, 5 * sec, 7*sec + 2e8, 12 * sec, 14*sec + 7e8, 31 * sec} {
		rows = append(rows, emuRow{ts: base + off, stream: []string{`{app="uw",pod="a"}`, `{app="uw",pod="b"}`}[i%2], value: float64(i + 2)})
	}
	start, end, step := base, base+60*sec, 5*time.Second
	for _, align := range []bool{true, false} {
		for _, fn := range []string{"first_over_time", "last_over_time"} {
			t.Run(fmt.Sprintf("align=%v/%s/binary", align, fn), func(t *testing.T) {
				query := fmt.Sprintf(`sum(%s({app="uw"} | unwrap amount [5s])) * 2`, fn)
				emu := &vlEmulator{t: t, rows: rows, field: "amount"}
				run := runFirstLastConfig(t, emu, query, false, start, end, step, alignedConfig(align))
				want := firstLastRangeRef(rows, fn, "sum", "", step, step, start, end, align)
				for _, points := range want {
					for ts := range points {
						points[ts] *= 2
					}
				}
				if d := diffRangePoints(firstLastRangePoints(run, ""), want); d != "" {
					t.Fatalf("%s: %s", query, d)
				}
			})
			t.Run(fmt.Sprintf("align=%v/%s/bare", align, fn), func(t *testing.T) {
				// One series per stream: its value is the pod's sum over one series.
				query := fmt.Sprintf(`%s({app="uw"} | unwrap amount [5s])`, fn)
				emu := &vlEmulator{t: t, rows: rows, field: "amount"}
				run := runFirstLastConfig(t, emu, query, false, start, end, step, alignedConfig(align))
				want := firstLastRangeRef(rows, fn, "sum", "pod", step, step, start, end, align)
				if d := diffRangePoints(firstLastRangePoints(run, "pod"), want); d != "" {
					t.Fatalf("%s: %s", query, d)
				}
			})
			t.Run(fmt.Sprintf("align=%v/%s/topk", align, fn), func(t *testing.T) {
				query := fmt.Sprintf(`topk(2, sum by (pod) (%s({app="uw"} | unwrap amount [5s])))`, fn)
				emu := &vlEmulator{t: t, rows: rows, field: "amount"}
				run := runFirstLastConfig(t, emu, query, false, start, end, step, alignedConfig(align))
				want := firstLastRangeRef(rows, fn, "sum", "pod", step, step, start, end, align)
				if d := diffRangePoints(firstLastRangePoints(run, "pod"), want); d != "" {
					t.Fatalf("%s: %s", query, d)
				}
			})
		}
	}
}

// firstLastRangeScenario generates lines and a query_range request whose range
// is at most the step (the shapes Grafana's $__auto sends: range == step), with
// a start off the step grid one time in two and lines on step edges.
func firstLastRangeScenario(rnd *rand.Rand, parser string) (s uwScenario) {
	steps := []time.Duration{time.Second, 2 * time.Second, 5 * time.Second, 10 * time.Second, 30 * time.Second, time.Minute}
	s.parser = parser
	s.step = steps[rnd.Intn(len(steps))]
	s.window = s.step
	if rnd.Intn(3) == 0 && s.step > time.Second {
		s.window = time.Duration(1+rnd.Int63n(int64(s.step/time.Second)-1)) * time.Second
	}
	base := int64(1_800_000_000) * 1e9
	s.start = base + int64(rnd.Intn(600))*1e9
	if rnd.Intn(2) == 0 {
		s.start -= s.start % int64(s.step)
	}
	s.end = s.start + int64(rnd.Intn(30))*int64(s.step) + int64(rnd.Intn(3))*1e9
	streams := []string{`{app="uw",pod="a"}`, `{app="uw",pod="b"}`, `{app="uw",pod="c"}`}
	values := []float64{0, 0.5, 1, 2.25, 3, 100.125, 7}
	lo, hi := s.start-int64(s.window)-int64(s.step), s.end+int64(s.step)
	n := 1 + rnd.Intn(60)
	for i := 0; i < n; i++ {
		ts := lo + rnd.Int63n(hi-lo)
		if rnd.Intn(3) == 0 { // on a step edge or a nanosecond off it
			edge := s.start - s.start%int64(s.step) + int64(rnd.Intn(30))*int64(s.step)
			ts = edge + int64(rnd.Intn(3)-1)
		}
		s.rows = append(s.rows, emuRow{ts: ts, stream: streams[rnd.Intn(len(streams))], value: values[rnd.Intn(len(values))]})
	}
	s.rows = firstLastRows(s)
	return s
}

// Differential: over generated lines and requests, both alignment profiles,
// every aggregation and both functions, the proxy's query_range answer equals
// the Loki-semantics reference point for point.
//
// conformance: semantics/unwrap-first-last-range-timestamps, loki_api_v1_query_range
func TestFirstLastOverTimeRangeTimestampsDifferential(t *testing.T) {
	rnd := rand.New(rand.NewSource(20261104))
	compared := 0
	for i := 0; i < 160; i++ {
		parser := []string{"", "logfmt", "json"}[i%3]
		s := firstLastRangeScenario(rnd, parser)
		fn := []string{"first_over_time", "last_over_time"}[rnd.Intn(2)]
		agg := []string{"sum", "avg", "min", "max", "count"}[rnd.Intn(5)]
		by := []string{"", "pod"}[rnd.Intn(2)]
		align := rnd.Intn(2) == 0
		inner := fmt.Sprintf(`%s({app="uw"}%s | unwrap amount [%s])`, fn, parserPipe(parser), uwWindowText(s.window))
		query := fmt.Sprintf(`%s(%s)`, agg, inner)
		if by != "" {
			query = fmt.Sprintf(`%s by (%s) (%s)`, agg, by, inner)
		}
		emu := &vlEmulator{t: t, rows: s.rows, field: "amount"}
		run := runFirstLastConfig(t, emu, query, false, s.start, s.end, s.step, alignedConfig(align))
		want := firstLastRangeRef(s.rows, fn, agg, by, s.window, s.step, s.start, s.end, align)
		if d := diffRangePoints(firstLastRangePoints(run, by), want); d != "" {
			t.Fatalf("#%d %s step=%s align=%v start=%d end=%d: %s", i, query, s.step, align, s.start/1e9, s.end/1e9, d)
		}
		if len(want) > 0 {
			compared++
		}
	}
	if compared < 80 {
		t.Fatalf("only %d scenarios produced a series", compared)
	}
}

// FuzzFirstLastOverTimeRangeTimestampsLikeLoki drives generated scenarios
// through the real handler against the emulated VictoriaLogs and compares every
// point with Loki's.
func FuzzFirstLastOverTimeRangeTimestampsLikeLoki(f *testing.F) {
	f.Add(int64(1), uint8(0), uint8(0), false)
	f.Add(int64(7), uint8(1), uint8(3), true)
	f.Add(int64(42), uint8(2), uint8(9), false)
	f.Fuzz(func(t *testing.T, seed int64, parserSel, shape uint8, align bool) {
		rnd := rand.New(rand.NewSource(seed))
		parser := []string{"", "logfmt", "json"}[int(parserSel)%3]
		s := firstLastRangeScenario(rnd, parser)
		fn := []string{"first_over_time", "last_over_time"}[int(shape)%2]
		agg := []string{"sum", "avg", "min", "max", "count"}[int(shape/2)%5]
		by := ""
		if shape >= 128 {
			by = "pod"
		}
		inner := fmt.Sprintf(`%s({app="uw"}%s | unwrap amount [%s])`, fn, parserPipe(parser), uwWindowText(s.window))
		query := fmt.Sprintf(`%s(%s)`, agg, inner)
		if by != "" {
			query = fmt.Sprintf(`%s by (%s) (%s)`, agg, by, inner)
		}
		emu := &vlEmulator{t: t, rows: s.rows, field: "amount"}
		run := runFirstLastConfig(t, emu, query, false, s.start, s.end, s.step, alignedConfig(align))
		want := firstLastRangeRef(s.rows, fn, agg, by, s.window, s.step, s.start, s.end, align)
		if d := diffRangePoints(firstLastRangePoints(run, by), want); d != "" {
			t.Fatalf("%s step=%s align=%v start=%d end=%d: %s", query, s.step, align, s.start/1e9, s.end/1e9, d)
		}
	})
}

// BenchmarkFirstLastRangeTimestamps measures the explore-unwrap-first-last-under-sum
// shape (range == step, as Grafana's $__auto sends it) and controls on the same
// path, through the real handler against the emulated VictoriaLogs: the
// relabelling adds no VictoriaLogs call.
func BenchmarkFirstLastRangeTimestamps(b *testing.B) {
	base := int64(1_800_000_000) * 1e9
	var rows []emuRow
	for i := 0; i < 2000; i++ {
		rows = append(rows, emuRow{ts: base + int64(i)*450*int64(time.Millisecond), stream: []string{`{app="uw",service_name="payment-service",pod="a"}`, `{app="uw",service_name="payment-service",pod="b"}`}[i%2], value: float64(i % 97)})
	}
	shapes := map[string]string{
		"last_over_time_under_sum":  `sum by (service_name) (last_over_time({app="uw"} | logfmt | unwrap amount [5s]))`,
		"first_over_time_under_sum": `sum by (service_name) (first_over_time({app="uw"} | logfmt | unwrap amount [5s]))`,
		"control_max_over_time":     `sum by (service_name) (max_over_time({app="uw"} | logfmt | unwrap amount [5s]))`,
		"control_count_over_time":   `sum by (service_name) (count_over_time({app="uw"} | logfmt [5s]))`,
	}
	names := make([]string, 0, len(shapes))
	for name := range shapes {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		query := shapes[name]
		b.Run(name, func(b *testing.B) {
			emu := &vlEmulator{t: b, rows: rows, field: "amount"}
			backend := httptest.NewServer(emu.handler())
			defer backend.Close()
			p, err := New(Config{BackendURL: backend.URL, LogLevel: "error", BackendMinVersion: "v1.49.0", BackendVersionCheckTimeout: time.Second, AlignQueriesWithStep: true})
			if err != nil {
				b.Fatal(err)
			}
			params := url.Values{"query": {query}, "start": {strconv.FormatInt(base/1e9, 10)}, "end": {strconv.FormatInt(base/1e9+900, 10)}, "step": {"5"}}
			target := "/loki/api/v1/query_range?" + params.Encode()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if res := doCompatProxyRequest(p, target, map[string]string{"X-Scope-OrgID": "0"}); res.Code != http.StatusOK {
					b.Fatalf("%s: %d %s", query, res.Code, res.Body)
				}
			}
			b.StopTimer()
			b.ReportMetric(float64(len(emu.calls))/float64(b.N), "vl-calls/op")
		})
	}
}
