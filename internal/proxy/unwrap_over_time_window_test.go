package proxy

import (
	"context"
	"encoding/json"
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

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// This file pins sum, avg, max and min over time with unwrap on query_range to
// Loki's window (t-range, t] at every step, whatever the range and the step are
// (pkg/logql/range_vector.go:148-189 batchRangeVectorIterator.load: the lower
// bound is exclusive, the evaluation time inclusive; range_vector.go:371-425
// sumOverTime, avgOverTime, maxOverTime, minOverTime). The reference below is
// written apart from the proxy. The proxy under test is the real handler over
// the emulated VictoriaLogs (vlEmulator), and the answer must come from stats
// buckets: raw rows only where the bucket grid cannot be anchored (VictoriaLogs
// before v1.45 with a start off its epoch grid).

// lokiOverTimeValue is Loki's range function over one series' window values,
// in order.
func lokiOverTimeValue(fn string, values []float64) float64 {
	switch fn {
	case "sum_over_time":
		var sum float64
		for _, v := range values {
			sum += v
		}
		return sum
	case "avg_over_time":
		var mean, count float64
		for _, v := range values {
			count++
			if math.IsInf(mean, 0) {
				if math.IsInf(v, 0) && (mean > 0) == (v > 0) {
					continue
				}
				if !math.IsInf(v, 0) && !math.IsNaN(v) {
					continue
				}
			}
			mean += v/count - mean/count
		}
		return mean
	case "max_over_time":
		out := values[0]
		for _, v := range values {
			if v > out || math.IsNaN(out) {
				out = v
			}
		}
		return out
	case "min_over_time":
		out := values[0]
		for _, v := range values {
			if v < out || math.IsNaN(out) {
				out = v
			}
		}
		return out
	}
	panic("reference: " + fn)
}

// lokiOverTimeGroups is the reference: at every evaluation time t, the window
// (t-offset-window, t-offset] of every series (stream plus parsed labels), the
// range function per series, and the outer sum of the series per group key
// (group returns it from the series' stream). A group whose series have no
// sample at t has no point there.
func lokiOverTimeGroups(rows []emuRow, fn string, window, offset int64, times []int64, group func(stream string) string) map[string]map[int64]float64 {
	sorted := append([]emuRow(nil), rows...)
	sort.SliceStable(sorted, func(i, j int) bool { return sorted[i].ts < sorted[j].ts })
	out := map[string]map[int64]float64{}
	for _, t := range times {
		end := t - offset
		perSeries := map[string][]float64{}
		groupOf := map[string]string{}
		for _, row := range sorted {
			if row.ts <= end-window || row.ts > end {
				continue
			}
			labels := make([]string, 0, len(row.labels))
			for k, v := range row.labels {
				labels = append(labels, k+"="+v)
			}
			sort.Strings(labels)
			key := row.stream + "|" + strings.Join(labels, ",")
			perSeries[key] = append(perSeries[key], row.value)
			groupOf[key] = group(row.stream)
		}
		for key, values := range perSeries {
			g := groupOf[key]
			if out[g] == nil {
				out[g] = map[int64]float64{}
			}
			out[g][t] += lokiOverTimeValue(fn, values)
		}
	}
	return out
}

// owShape is one query form: the query around a range aggregation, and the
// group key of a stream under it, as the response labels it (seriesKey).
type owShape struct {
	name string
	// query wraps the range aggregation text.
	query func(rangeAgg string) string
	// group maps a stream to its result series key.
	group func(stream string) string
	// parser reports whether the form runs with this parser ("" none).
	parser func(parser, fn string) bool
	// fold sums the answer's series per step before the comparison.
	fold bool
}

func owPod(stream string) string {
	return emuStreamLabelRE.FindAllStringSubmatch(stream, -1)[1][2]
}

var owShapes = []owShape{
	{
		name:   "sum by (pod)",
		query:  func(ra string) string { return "sum by (pod) (" + ra + ")" },
		group:  func(stream string) string { return "pod=" + owPod(stream) },
		parser: func(string, string) bool { return true },
	},
	{
		name:   "per stream",
		query:  func(ra string) string { return ra },
		group:  func(stream string) string { return "app=uw,pod=" + owPod(stream) },
		parser: func(string, string) bool { return true },
	},
	{
		// The streams fold into one series: per stream from VictoriaLogs and
		// added up after the window, or summed upstream for sum over time.
		name:   "sum",
		query:  func(ra string) string { return "sum(" + ra + ")" },
		group:  func(string) string { return "" },
		parser: func(parser, fn string) bool { return parser == "" || fn == "sum_over_time" },
	},
	{
		name:   "topk of sum by (pod)",
		query:  func(ra string) string { return "topk(5, sum by (pod) (" + ra + "))" },
		group:  func(stream string) string { return "pod=" + owPod(stream) },
		parser: func(string, string) bool { return true },
	},
	{
		// The series a `without` keeps are pinned apart (the translation of
		// sum without over unwrap); the windows are compared on their total.
		name:   "sum without (app)",
		query:  func(ra string) string { return "sum without (app) (" + ra + ")" },
		group:  func(string) string { return "" },
		parser: func(parser, _ string) bool { return parser == "" },
		fold:   true,
	},
	{
		// A binary operation evaluates its operand like a query of its own.
		name:   "binary with a scalar",
		query:  func(ra string) string { return "sum by (pod) (" + ra + ") * 2" },
		group:  func(stream string) string { return "pod=" + owPod(stream) },
		parser: func(string, string) bool { return true },
	},
}

// owSeriesKey is the response series key: its labels, sorted, k=v joined by
// ",", without the labels the proxy derives for a stream from VictoriaLogs on
// every route (detected_level, service_name).
func owSeriesKey(metric map[string]string) string {
	keys := make([]string, 0, len(metric))
	for k, v := range metric {
		if k == "detected_level" || k == "service_name" {
			continue
		}
		keys = append(keys, k+"="+v)
	}
	sort.Strings(keys)
	return strings.Join(keys, ",")
}

type owCase struct {
	fn, parser   string
	shape        owShape
	window, step time.Duration
	offset       time.Duration
	start, end   int64 // ns, whole seconds
	rows         []emuRow
}

func (c owCase) rangeAgg() string {
	pipeline := ""
	if c.parser != "" {
		pipeline = " | " + c.parser
	}
	offset := ""
	if c.offset > 0 {
		offset = " offset " + uwWindowText(c.offset)
	}
	post := ""
	if c.parser == "json" {
		post = ` | __error__=""` // the post filter that intercepts conversion errors
	}
	return fmt.Sprintf(`%s({app="uw"}%s | unwrap amount%s [%s]%s)`, c.fn, pipeline, post, uwWindowText(c.window), offset)
}

func (c owCase) query() string { return c.shape.query(c.rangeAgg()) }

func (c owCase) times() []int64 {
	var out []int64
	for t := c.start; t <= c.end; t += int64(c.step) {
		out = append(out, t)
	}
	return out
}

// want returns Loki's answer, the scalar of a binary shape applied.
func (c owCase) want() map[string]map[int64]float64 {
	out := lokiOverTimeGroups(c.rows, c.fn, int64(c.window), int64(c.offset), c.times(), c.shape.group)
	if strings.HasSuffix(c.query(), " * 2") {
		for _, points := range out {
			for t, v := range points {
				points[t] = v * 2
			}
		}
	}
	return out
}

// rawRowsAllowed reports whether the proxy may read raw rows: only when
// VictoriaLogs cannot anchor the bucket grid to the request (before v1.45, an
// anchor off the epoch grid of the bucket), as slidingStatsBucket decides, and
// for avg over time of a parser query per series, which the bare-parser route
// answers from raw rows (it merges only sum, max and min from buckets).
func (c owCase) rawRowsAllowed(version string) bool {
	if c.shape.name == "per stream" && c.parser != "" && c.fn == "avg_over_time" {
		return true
	}
	if version != "v1.40.0" {
		return false
	}
	start := c.start - int64(c.offset)
	bucket, anchor := int64(c.step), start-int64(c.step)
	if c.window >= c.step {
		a, b := int64(c.step), int64(c.window)
		for b > 0 {
			a, b = b, a%b
		}
		bucket, anchor = a, start-int64(c.window)
	}
	return anchor%bucket != 0
}

func runOverTimeCase(t *testing.T, emu *vlEmulator, c owCase, seriesCap int) map[string]map[int64]float64 {
	t.Helper()
	backend := httptest.NewServer(emu.handler())
	defer backend.Close()
	p, err := New(Config{BackendURL: backend.URL, Cache: cache.New(60*time.Second, 1000), LogLevel: "error", BackendMinVersion: "v1.40.0", BackendVersionCheckTimeout: time.Second, MaxStatsQuerySeries: seriesCap})
	if err != nil {
		t.Fatal(err)
	}
	if emu.version != "" {
		if err := p.ValidateBackendVersionCompatibility(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	params := url.Values{"query": {c.query()}, "start": {strconv.FormatInt(c.start/1e9, 10)}, "end": {strconv.FormatInt(c.end/1e9, 10)}, "step": {strconv.FormatInt(int64(c.step/time.Second), 10)}}
	res := doCompatProxyRequest(p, "/loki/api/v1/query_range?"+params.Encode(), map[string]string{"X-Scope-OrgID": "0"})
	if res.Code != http.StatusOK {
		t.Fatalf("%s: %d %s", c.query(), res.Code, res.Body)
	}
	var resp struct {
		Data struct {
			Result []struct {
				Metric map[string]string `json:"metric"`
				Values [][]any           `json:"values"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(res.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	got := map[string]map[int64]float64{}
	for _, series := range resp.Data.Result {
		key := owSeriesKey(series.Metric)
		if c.shape.fold {
			if got[""] == nil {
				got[""] = map[int64]float64{}
			}
			for _, point := range series.Values {
				v, _ := strconv.ParseFloat(point[1].(string), 64)
				got[""][int64(point[0].(float64))*1e9] += v
			}
			continue
		}
		if got[key] != nil {
			t.Fatalf("%s: series {%s} twice", c.query(), key)
		}
		got[key] = map[int64]float64{}
		for _, point := range series.Values {
			v, _ := strconv.ParseFloat(point[1].(string), 64)
			got[key][int64(point[0].(float64))*1e9] = v
		}
	}
	return got
}

func compareOverTime(t *testing.T, label string, c owCase, got, want map[string]map[int64]float64) {
	t.Helper()
	detail := fmt.Sprintf("\nquery %s step=%v window=%v start=%d rows=%d", c.query(), c.step, c.window, c.start/1e9, len(c.rows))
	for key, points := range want {
		for ts, w := range points {
			g, ok := got[key][ts]
			if !ok {
				t.Fatalf("%s {%s}: no point at %d, Loki has %g%s", label, key, ts/1e9, w, detail)
			}
			if !(g == w || math.Abs(g-w) <= 1e-9*math.Max(1, math.Abs(w)) || (math.IsNaN(g) && math.IsNaN(w))) {
				t.Fatalf("%s {%s}: at %d got %g, Loki has %g%s", label, key, ts/1e9, g, w, detail)
			}
		}
	}
	for key, points := range got {
		for ts, g := range points {
			if _, ok := want[key][ts]; !ok {
				t.Fatalf("%s {%s}: extra point at %d = %g, Loki has none%s", label, key, ts/1e9, g, detail)
			}
		}
	}
}

// checkOverTimeRoute: the answer came from stats buckets (raw rows only where
// rawRowsAllowed), and every bucket query still counts the unwrap conversion
// errors when they reach the result (no `| __error__=""` after the unwrap).
func checkOverTimeRoute(t *testing.T, label string, emu *vlEmulator, c owCase, version string) {
	t.Helper()
	raw := emu.endpointCalls("/select/logsql/query")
	if len(raw) > 0 && !c.rawRowsAllowed(version) {
		t.Fatalf("%s: the metric read raw rows: %s\nquery %s step=%v window=%v", label, raw[0], c.query(), c.step, c.window)
	}
	for _, q := range emu.endpointCalls("/select/logsql/stats_query_range") {
		if counted := strings.Contains(q, unwrapBadField); counted == (c.parser == "json") {
			t.Fatalf("%s: conversion error counter %v in %s\nquery %s", label, counted, q, c.query())
		}
	}
	if len(raw) == 0 && len(emu.endpointCalls("/select/logsql/stats_query_range")) == 0 {
		t.Fatalf("%s: no stats bucket query\nquery %s", label, c.query())
	}
}

// owGenerate draws a case: windows longer than, shorter than, equal to and not
// a multiple of the step, starts off every grid, and rows dense or sparse with
// values on window edges, zeros and repeated values.
func owGenerate(rnd *rand.Rand, fn, parser string, shape owShape) owCase {
	steps := []time.Duration{30 * time.Second, time.Minute, 90 * time.Second, 2 * time.Minute}
	windows := []time.Duration{30 * time.Second, 45 * time.Second, time.Minute, 90 * time.Second, 150 * time.Second, 5 * time.Minute, 7 * time.Minute}
	c := owCase{fn: fn, parser: parser, shape: shape, step: steps[rnd.Intn(len(steps))], window: windows[rnd.Intn(len(windows))]}
	if rnd.Intn(4) == 0 {
		c.offset = time.Duration(1+rnd.Intn(3)) * time.Minute
	}
	base := int64(1_800_000_000) * 1e9
	c.start = base + int64(rnd.Intn(900))*1e9
	c.end = c.start + int64(rnd.Intn(15))*int64(c.step)
	lo, hi := c.start-int64(c.offset)-int64(c.window)-5e9, c.end-int64(c.offset)+5e9
	values := []float64{0, 0.5, 1, 2.25, 3, 100.125, 7, -4}
	gap := int64(3e9) // dense: a line every few seconds
	if rnd.Intn(2) == 0 {
		gap = int64(90+rnd.Intn(300)) * 1e9 // sparse: minutes apart
	}
	for _, pod := range []string{"a", "b", "c"} {
		for ts := lo + rnd.Int63n(gap); ts < hi; ts += gap/2 + rnd.Int63n(gap) {
			row := emuRow{ts: ts, stream: fmt.Sprintf(`{app="uw",pod=%q}`, pod), value: values[rnd.Intn(len(values))]}
			if rnd.Intn(5) == 0 { // on a window edge, or a nanosecond beside it
				k := int64(rnd.Intn(16))
				edge := c.start - int64(c.offset) + k*int64(c.step)
				if rnd.Intn(2) == 0 {
					edge -= int64(c.window)
				}
				row.ts = edge + int64(rnd.Intn(3)-1)
			}
			c.rows = append(c.rows, row)
		}
	}
	return c
}

// TestUnwrapOverTimeWindowsDifferential: over generated rows and windows, every
// query form answers Loki's value at every step, and only the steps whose
// window holds a line, from VictoriaLogs stats buckets.
//
// conformance: semantics/unwrap-over-time-range-windows, window-bounds-and-step-alignment
func TestUnwrapOverTimeWindowsDifferential(t *testing.T) {
	for _, version := range []string{"", "v1.52.0", "v1.40.0"} {
		for _, parser := range []string{"", "logfmt", "json"} {
			for _, fn := range []string{"sum_over_time", "avg_over_time", "max_over_time", "min_over_time"} {
				for _, shape := range owShapes {
					if !shape.parser(parser, fn) {
						continue
					}
					name := fmt.Sprintf("version=%s/parser=%s/%s/%s", version, parser, fn, shape.name)
					t.Run(name, func(t *testing.T) {
						rnd := rand.New(rand.NewSource(int64(len(name))*7919 + int64(name[len(name)-1])))
						for i := 0; i < 12; i++ {
							c := owGenerate(rnd, fn, parser, shape)
							emu := &vlEmulator{t: t, rows: c.rows, field: "amount", version: version}
							got := runOverTimeCase(t, emu, c, 0)
							label := fmt.Sprintf("%s #%d", name, i)
							compareOverTime(t, label, c, got, c.want())
							checkOverTimeRoute(t, label, emu, c, version)
						}
					})
				}
			}
		}
	}
}

// TestUnwrapOverTimeSparseSamplePresence: one line every 4.5 minutes at step
// 60s. Loki has a sample at every step whose window holds a line: all 61 steps
// of the hour for [5m] and [10m], and for [30s] only the 7 steps whose last 30
// seconds hold a line (the lines fall 7s and 37s past a minute in turn).
//
// conformance: semantics/unwrap-over-time-range-windows, window-bounds-and-step-alignment
func TestUnwrapOverTimeSparseSamplePresence(t *testing.T) {
	const base = int64(1_800_000_000) * 1e9
	var rows []emuRow
	for ts := base - int64(10*time.Minute) + 7e9; ts < base+int64(time.Hour); ts += int64(270 * time.Second) {
		rows = append(rows, emuRow{ts: ts, stream: `{app="uw",pod="a"}`, value: float64(ts/1e9%17) + 0.25})
	}
	for _, tc := range []struct {
		window  time.Duration
		samples int
	}{{5 * time.Minute, 61}, {30 * time.Second, 7}, {10 * time.Minute, 61}} {
		for _, fn := range []string{"sum_over_time", "avg_over_time", "max_over_time", "min_over_time"} {
			t.Run(fmt.Sprintf("%s/%s", fn, uwWindowText(tc.window)), func(t *testing.T) {
				c := owCase{fn: fn, parser: "logfmt", shape: owShapes[0], window: tc.window, step: time.Minute, start: base, end: base + int64(time.Hour), rows: rows}
				emu := &vlEmulator{t: t, rows: rows, field: "amount", version: "v1.52.0"}
				got := runOverTimeCase(t, emu, c, 0)
				want := c.want()
				if n := len(want["pod=a"]); n != tc.samples {
					t.Fatalf("reference sanity: %d samples, want %d", n, tc.samples)
				}
				compareOverTime(t, fn, c, got, want)
				checkOverTimeRoute(t, fn, emu, c, "v1.52.0")
			})
		}
	}
}

// TestUnwrapOverTimeUngroupedSumIgnoresSeriesLimit: an ungrouped sum of avg,
// max or min over time is one series, so Loki counts one series against
// max_query_series however many streams it folds; the per-stream buckets the
// proxy reads for it do not count either.
//
// conformance: semantics/unwrap-over-time-range-windows, metric-series-identity
func TestUnwrapOverTimeUngroupedSumIgnoresSeriesLimit(t *testing.T) {
	const base = int64(1_800_000_000) * 1e9
	for _, fn := range []string{"avg_over_time", "max_over_time", "min_over_time"} {
		t.Run(fn, func(t *testing.T) {
			var rows []emuRow
			for i := 0; i < 12; i++ { // twelve streams, six times the limit
				for ts := base - int64(5*time.Minute); ts < base+int64(10*time.Minute); ts += int64(40*time.Second) + int64(i)*1e9 {
					rows = append(rows, emuRow{ts: ts, stream: fmt.Sprintf(`{app="uw",pod="p%d"}`, i), value: float64(i + 1)})
				}
			}
			c := owCase{fn: fn, shape: owShapes[2], window: 5 * time.Minute, step: time.Minute, start: base, end: base + int64(10*time.Minute), rows: rows}
			emu := &vlEmulator{t: t, rows: rows, field: "amount", version: "v1.52.0"}
			got := runOverTimeCase(t, emu, c, 2)
			compareOverTime(t, fn, c, got, c.want())
			checkOverTimeRoute(t, fn, emu, c, "v1.52.0")
		})
	}
}

// TestUnwrapOverTimeResidualShapesKeepStatsBuckets: the forms the window merge
// does not answer yet keep VictoriaLogs' step buckets and never read raw rows:
// a parser query under an ungrouped sum or a without of avg, max or min, whose
// series VictoriaLogs groups by stream and line, and
// an outer aggregation other than sum, which the window evaluator does not
// apply. Their windows over a range other than the step are the residual of
// semantics/unwrap-over-time-range-windows.
//
// conformance: semantics/unwrap-over-time-range-windows
func TestUnwrapOverTimeResidualShapesKeepStatsBuckets(t *testing.T) {
	const base = int64(1_800_000_000) * 1e9
	var rows []emuRow
	for ts := base - int64(10*time.Minute); ts < base+int64(10*time.Minute); ts += int64(17 * time.Second) {
		rows = append(rows, emuRow{ts: ts, stream: `{app="uw",pod="a"}`, value: float64(ts / 1e9 % 13)})
	}
	for _, query := range []string{
		`sum(max_over_time({app="uw"} | json | unwrap amount [5m]))`,
		`sum without (pod) (min_over_time({app="uw"} | logfmt | unwrap amount [5m]))`,
		`max by (pod) (sum_over_time({app="uw"} | unwrap amount [5m]))`,
		`avg by (pod) (avg_over_time({app="uw"} | logfmt | unwrap amount [5m]))`,
	} {
		t.Run(query, func(t *testing.T) {
			emu := &vlEmulator{t: t, rows: rows, field: "amount", version: "v1.52.0"}
			c := owCase{shape: owShape{query: func(string) string { return query }}, window: 5 * time.Minute, step: time.Minute, start: base, end: base + int64(10*time.Minute)}
			if got := runOverTimeCase(t, emu, c, 0); len(got) == 0 {
				t.Fatalf("%s: no series", query)
			}
			if raw := emu.endpointCalls("/select/logsql/query"); len(raw) > 0 {
				t.Fatalf("%s: read raw rows: %s", query, raw[0])
			}
			for _, q := range emu.endpointCalls("/select/logsql/stats_query_range") {
				if strings.Contains(q, "__sample_count") {
					t.Fatalf("%s: answered from the window merge: %s", query, q)
				}
			}
		})
	}
}
