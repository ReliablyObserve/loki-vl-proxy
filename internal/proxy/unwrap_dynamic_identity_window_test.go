package proxy

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/lower"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/testutil/vlemu"
)

// This file pins a parser's series identity over a range longer than the step (sliding windows) to Loki's answer,
// with a reference evaluator written apart from the proxy, from LogQL's documented meaning: a series is the stream
// and every label the parser extracts less the unwrapped one, the range function is taken per series over the window
// (t-range, t] at every step, and the outer sum, avg, max or min folds the series into the group its labels name.
// The reference is held to recorded Loki answers by the Loki differentials of internal/querycorpus
// (TestSeriesFoldMatchLoki, TestUnwrapDynamicIdentityMatchLoki). The proxy under test is the real handler;
// VictoriaLogs is the vlemu emulator, which computes the series hash from each line and runs the fold's subquery
// over the rows a read selects.

// dynWindowScenario is one sliding-window request of a parser's series identity.
type dynWindowScenario struct {
	outer, by, fn, parser string
	window, step          time.Duration
	start, end            int64 // ns, step-aligned evaluation times
	rows                  []vlemu.Row
}

func (s dynWindowScenario) query() string {
	clause := ""
	if s.by != "" {
		clause = " by (" + s.by + ")"
	}
	return fmt.Sprintf(`%s%s (%s({app="dw"} | %s | unwrap amount [%s]))`, s.outer, clause, s.fn, s.parser, uwWindowText(s.window))
}

func (s dynWindowScenario) times() []int64 {
	var out []int64
	for t := s.start; t <= s.end; t += int64(s.step) {
		out = append(out, t)
	}
	return out
}

// dynWindowRow is a row of stream pod p with the parsed labels labels and value v, its line written by parser.
func dynWindowRow(ts int64, pod, parser string, labels map[string]string, v float64) vlemu.Row {
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	amount := strconv.FormatFloat(v, 'f', -1, 64)
	var line string
	// The keys in an order of the line's own (rotated by its time): one series holds lines with its keys in any order.
	turn := int(ts % int64(len(keys)+1))
	if parser == "json" {
		fields := []string{`"amount":` + amount}
		for _, k := range keys {
			fields = append(fields, strconv.Quote(k)+":"+strconv.Quote(labels[k]))
		}
		line = "{" + strings.Join(append(fields[turn:], fields[:turn]...), ",") + "}"
	} else {
		fields := []string{"amount=" + amount}
		for _, k := range keys {
			fields = append(fields, k+"="+labels[k])
		}
		line = strings.Join(append(fields[turn:], fields[:turn]...), " ")
	}
	return vlemu.Row{TS: ts, Stream: `{app="dw",pod="` + pod + `"}`, Labels: labels, Value: v, Msg: line}
}

// generateDynWindowScenario draws a sliding window (a range longer than the step, a multiple of it or not), and rows
// of three streams: lines that differ only in the value (one series), a parsed label more (a series of its own), a
// key of its own on a line now and then (a series per line), and lines on window edges.
func generateDynWindowScenario(rnd *rand.Rand, outer, by, fn, parser string) dynWindowScenario {
	steps := []time.Duration{20 * time.Second, 30 * time.Second, time.Minute}
	s := dynWindowScenario{outer: outer, by: by, fn: fn, parser: parser, step: steps[rnd.Intn(len(steps))]}
	// A range longer than the step: twice, five times, or a range the step does not divide.
	switch rnd.Intn(3) {
	case 0:
		s.window = 2 * s.step
	case 1:
		s.window = 5 * s.step
	default:
		s.window = s.step + s.step/2 + 10*time.Second
	}
	base := int64(1_800_000_000) * 1e9
	s.start = base + int64(rnd.Intn(600))*1e9
	s.start -= s.start % int64(s.step)
	if rnd.Intn(2) == 0 {
		// Bounds off the step's grid (a proxy that does not move them, -align-queries-with-step off): the buckets, and
		// the grid the fold's subquery tells a series' buckets apart on, are offset from the minute.
		s.start += int64(1+rnd.Intn(int(s.step/time.Second)-1)) * 1e9
	}
	s.end = s.start + int64(3+rnd.Intn(8))*int64(s.step)
	pods := []string{"a", "b", "c"}
	labelSets := []map[string]string{{"route": "/x"}, {"route": "/x", "region": "eu"}, {"route": "/y"}}
	values := []float64{0, 0.5, 1, 2.25, 3, 100.125, 7, 42}
	lo, hi := s.start-int64(s.window)-5e9, s.end+5e9
	n := 10 + rnd.Intn(70)
	for i := 0; i < n; i++ {
		ts := lo + rnd.Int63n(hi-lo)
		if rnd.Intn(5) == 0 { // on a window edge
			edge := s.start + int64(rnd.Intn(8))*int64(s.step)
			if rnd.Intn(2) == 0 {
				edge -= int64(s.window)
			}
			ts = edge + int64(rnd.Intn(3)-1)
		}
		labels := map[string]string{}
		for k, v := range labelSets[rnd.Intn(len(labelSets))] {
			labels[k] = v
		}
		if rnd.Intn(6) == 0 {
			labels["req"] = "r" + strconv.Itoa(i) // a series of its own
		}
		s.rows = append(s.rows, dynWindowRow(ts, pods[rnd.Intn(len(pods))], parser, labels, values[rnd.Intn(len(values))]))
	}
	return s
}

// lokiDynamicWindows is the reference: at each evaluation time, the samples of each series (the stream and every
// parsed label) in (t-window, t], the range function per series, then the outer aggregation per group of the by
// labels (the stream's pod or a parsed label). A group with no series in the window has no point.
func lokiDynamicWindows(s dynWindowScenario) map[string]map[int64]float64 {
	out := map[string]map[int64]float64{}
	for _, t := range s.times() {
		perSeries := map[string][]float64{}
		groupOf := map[string]string{}
		for _, row := range s.rows {
			if row.TS <= t-int64(s.window) || row.TS > t {
				continue
			}
			labels := map[string]string{"app": "dw", "pod": streamLabel(row.Stream, "pod")}
			for k, v := range row.Labels {
				labels[k] = v
			}
			key := labelsSortedKey(labels)
			perSeries[key] = append(perSeries[key], row.Value)
			group := map[string]string{}
			if s.by != "" {
				for _, l := range strings.Split(s.by, ", ") {
					if v := labels[l]; v != "" {
						group[l] = v
					}
				}
			}
			groupOf[key] = labelsSortedKey(group)
		}
		groups := map[string][]float64{}
		for key, vs := range perSeries {
			groups[groupOf[key]] = append(groups[groupOf[key]], rangeFuncOf(s.fn, vs))
		}
		for g, vs := range groups {
			if out[g] == nil {
				out[g] = map[int64]float64{}
			}
			out[g][t] = outerFuncOf(s.outer, vs)
		}
	}
	return out
}

func labelsSortedKey(m map[string]string) string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k+"="+m[k])
	}
	sort.Strings(keys)
	return strings.Join(keys, ",")
}

// rangeFuncOf is a range function over one series' samples in a window; stddev and stdvar are the population ones.
func rangeFuncOf(fn string, vs []float64) float64 {
	var sum float64
	for _, v := range vs {
		sum += v
	}
	mean := sum / float64(len(vs))
	switch fn {
	case "avg_over_time":
		return mean
	case "max_over_time":
		return slices.Max(vs)
	case "min_over_time":
		return slices.Min(vs)
	case "stddev_over_time", "stdvar_over_time":
		var m2 float64
		for _, v := range vs {
			m2 += (v - mean) * (v - mean)
		}
		if fn == "stdvar_over_time" {
			return m2 / float64(len(vs))
		}
		return math.Sqrt(m2 / float64(len(vs)))
	}
	panic("reference: " + fn)
}

func outerFuncOf(op string, vs []float64) float64 {
	switch op {
	case "sum", "avg":
		var sum float64
		for _, v := range vs {
			sum += v
		}
		if op == "avg" {
			return sum / float64(len(vs))
		}
		return sum
	case "max":
		return slices.Max(vs)
	case "min":
		return slices.Min(vs)
	}
	panic("reference: " + op)
}

// runDynWindowScenario sends the scenario through the real handler and returns its series (labels key -> time ->
// value) and the response status and body.
func runDynWindowScenario(t *testing.T, emu *vlemu.Emulator, s dynWindowScenario, cfg Config) (map[string]map[int64]float64, int, string) {
	t.Helper()
	backend := httptest.NewServer(emu.Handler())
	defer backend.Close()
	cfg.BackendURL, cfg.LogLevel, cfg.BackendMinVersion, cfg.BackendVersionCheckTimeout = backend.URL, "error", "v1.49.0", time.Second
	if cfg.Cache == nil {
		c := cache.New(60*time.Second, 1000)
		defer c.Close()
		cfg.Cache = c
	}
	p, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { closeTestProxy(p) }()
	if emu.Version != "" {
		if err := p.ValidateBackendVersionCompatibility(t.Context()); err != nil {
			t.Fatal(err)
		}
	}
	params := url.Values{"query": {s.query()}, "start": {strconv.FormatInt(s.start/1e9, 10)}, "end": {strconv.FormatInt(s.end/1e9, 10)},
		"step": {strconv.FormatInt(int64(s.step/time.Second), 10)}}
	res := doCompatProxyRequest(p, "/loki/api/v1/query_range?"+params.Encode(), map[string]string{"X-Scope-OrgID": "0"})
	if res.Code != http.StatusOK {
		return nil, res.Code, res.Body.String()
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
		key := labelsSortedKey(series.Metric)
		got[key] = map[int64]float64{}
		for _, point := range series.Values {
			v, _ := strconv.ParseFloat(point[1].(string), 64)
			got[key][int64(point[0].(float64))*1e9] = v
		}
	}
	return got, res.Code, res.Body.String()
}

// closeTestProxy shuts a scenario's proxy down with its caches.
func closeTestProxy(p *Proxy) {
	_ = p.Shutdown(context.Background())
	p.translationCache.Close()
	p.streamFieldNamesCache.Close()
}

func compareDynWindows(t *testing.T, label string, s dynWindowScenario, got, want map[string]map[int64]float64) {
	t.Helper()
	ctx := fmt.Sprintf("\nquery %s step=%v window=%v rows=%d", s.query(), s.step, s.window, len(s.rows))
	for key, points := range want {
		g, ok := got[key]
		if !ok {
			t.Fatalf("%s: no series {%s}, Loki has %d points%s", label, key, len(points), ctx)
		}
		for ts, w := range points {
			v, ok := g[ts]
			if !ok {
				t.Fatalf("%s: {%s} has no point at %d, Loki has %g%s", label, key, ts/1e9, w, ctx)
			}
			if math.Abs(v-w) > 1e-9*math.Max(1, math.Abs(w)) {
				t.Fatalf("%s: {%s} at %d is %g, Loki has %g%s", label, key, ts/1e9, v, w, ctx)
			}
		}
		for ts, v := range g {
			if _, ok := points[ts]; !ok {
				t.Fatalf("%s: {%s} has a point at %d = %g, Loki has none%s", label, key, ts/1e9, v, ctx)
			}
		}
	}
	for key := range got {
		if _, ok := want[key]; !ok {
			t.Fatalf("%s: series {%s} Loki does not have%s", label, key, ctx)
		}
	}
}

// TestDynamicIdentityWindowsMatchLokiReference: over generated rows and sliding windows (a range twice or five times
// the step, and one the step does not divide, so the buckets are finer than the step), sum, avg, max and min, with no
// clause, by the stream's pod or by a parsed label, over avg, max, min, stddev and stdvar over time after a json or a
// logfmt parser, answer Loki's series and values at every step, on VictoriaLogs v1.49.0 and v1.54.0. VictoriaLogs
// answers one stats query, the series that lie in one bucket folded in it (lower.SeriesFold); the answer equals the
// one of the series read one by one (seriesFoldOff); no raw rows are read, and the hash never reaches a client.
//
// conformance: semantics/unwrap-per-series-identity-raw-rows, metric-series-identity, window-bounds-and-step-alignment
func TestDynamicIdentityWindowsMatchLokiReference(t *testing.T) {
	type shape struct{ outer, by string }
	shapes := []shape{{"sum", ""}, {"sum", "pod"}, {"avg", ""}, {"max", "pod"}, {"min", "route"}, {"sum", "pod, route"}}
	fns := []string{"avg_over_time", "max_over_time", "min_over_time", "stddev_over_time", "stdvar_over_time"}
	for _, version := range []string{"v1.49.0", "v1.54.0"} {
		for _, parser := range []string{"json", "logfmt"} {
			for _, fn := range fns {
				for _, sh := range shapes {
					if sh.outer == "max" && fn == "max_over_time" || sh.outer == "min" && fn == "min_over_time" {
						// The extreme of the group's samples: no series hash (TestDynamicIdentityWindowsKeepExtremes).
						continue
					}
					name := fmt.Sprintf("%s/%s/%s/%s_by_%s", version, parser, fn, sh.outer, sh.by)
					t.Run(name, func(t *testing.T) {
						rnd := rand.New(rand.NewSource(int64(len(name))*7919 + 31))
						for i := 0; i < 3; i++ {
							s := generateDynWindowScenario(rnd, sh.outer, sh.by, fn, parser)
							checkSeriesFoldScenario(t, fmt.Sprintf("#%d", i), s, version, i == 0)
						}
					})
				}
			}
		}
	}
}

// checkSeriesFoldScenario runs s with the fold, and with the series read one by one when both is set: each answers
// the reference's series and values, in one bucket read, the folded read holding the subquery, and no raw rows.
func checkSeriesFoldScenario(t *testing.T, label string, s dynWindowScenario, version string, both bool) {
	t.Helper()
	want := lokiDynamicWindows(s)
	for _, off := range []bool{false, true} {
		if off && !both {
			break
		}
		seriesFoldOff = off
		emu := &vlemu.Emulator{T: t, Rows: s.rows, Field: "amount", Version: version}
		got, code, body := runDynWindowScenario(t, emu, s, Config{})
		seriesFoldOff = false
		if code != http.StatusOK {
			t.Fatalf("%s %s (fold off %v): %d %s", label, s.query(), off, code, body)
		}
		if strings.Contains(body, dynamicSeriesHashLabel) {
			t.Fatalf("%s %s: the answer names the series hash: %.300s", label, s.query(), body)
		}
		compareDynWindows(t, fmt.Sprintf("%s fold off %v", label, off), s, got, want)
		if raw := emu.EndpointCalls("/select/logsql/query"); len(raw) > 0 {
			t.Fatalf("%s %s: read raw rows: %s", label, s.query(), raw[0])
		}
		reads := identityStatsCalls(emu)
		if len(reads) != 1 || strings.Contains(reads[0], seriesFoldMark) == off {
			t.Fatalf("%s %s (fold off %v): %d bucket reads grouped by the series hash, want one: %v", label, s.query(), off, len(reads), reads)
		}
	}
}

// max by (L) of max_over_time and min by (L) of min_over_time over a range longer than the step are the extreme of
// the group's samples in the window, whatever the series: they read no series hash and answer Loki's values.
//
// conformance: semantics/unwrap-per-series-identity-raw-rows
func TestDynamicIdentityWindowsKeepExtremes(t *testing.T) {
	for _, tc := range []struct{ outer, fn string }{{"max", "max_over_time"}, {"min", "min_over_time"}} {
		rnd := rand.New(rand.NewSource(int64(len(tc.fn)) * 104729))
		for i := 0; i < 6; i++ {
			s := generateDynWindowScenario(rnd, tc.outer, "pod", tc.fn, "logfmt")
			emu := &vlemu.Emulator{T: t, Rows: s.rows, Field: "amount", Version: "v1.54.0"}
			got, code, body := runDynWindowScenario(t, emu, s, Config{})
			if code != http.StatusOK {
				t.Fatalf("%s: %d %s", s.query(), code, body)
			}
			compareDynWindows(t, fmt.Sprintf("%s #%d", tc.fn, i), s, got, lokiDynamicWindows(s))
			for _, q := range emu.EndpointCalls("/select/logsql/stats_query_range") {
				if strings.Contains(q, "__lvp_sid") {
					t.Fatalf("%s: VictoriaLogs read the series hash: %s", s.query(), q)
				}
			}
		}
	}
}

// Loki counts max_query_series on the folded answer: a sum by (pod) over more series than the tenant's limit (a key
// of its own on every line) answers its two groups. The series that lie in one bucket are folded in VictoriaLogs, so
// a read budget of 600 (-manual-range-metric-row-limit, over a tenant limit of 500) under the 1,200 series answers
// in one read; read one by one (seriesFoldOff), the request is read in time chunks under that budget, and answers
// the same.
//
// conformance: semantics/unwrap-per-series-identity-raw-rows, metric-series-identity
func TestDynamicIdentityWindowsFoldBeyondSeriesLimit(t *testing.T) {
	s := dynWindowScenario{outer: "sum", by: "pod", fn: "avg_over_time", parser: "logfmt", window: 2 * time.Minute, step: 30 * time.Second}
	base := int64(1_800_000_000) * 1e9
	s.start, s.end = base, base+10*int64(30*time.Second)
	for i := 0; i < 1200; i++ { // 1200 series, more than twice the tenant's limit of 500
		ts := s.start - int64(s.window) + int64(i)*int64(time.Second)/3 + 7e6
		pod := []string{"a", "b"}[i%2]
		s.rows = append(s.rows, dynWindowRow(ts, pod, "logfmt", map[string]string{"req": "r" + strconv.Itoa(i)}, float64(i%17)))
	}
	t.Cleanup(func() { seriesFoldOff = false })
	for _, off := range []bool{false, true} {
		seriesFoldOff = off
		emu := &vlemu.Emulator{T: t, Rows: s.rows, Field: "amount", Version: "v1.54.0"}
		got, code, body := runDynWindowScenario(t, emu, s, Config{RangeMetricRowLimit: 600, MaxStatsQuerySeries: 500})
		if code != http.StatusOK || len(got) != 2 {
			t.Fatalf("fold off %v: %d, %d series %.300s; want the two pods", off, code, len(got), body)
		}
		compareDynWindows(t, fmt.Sprintf("fold off %v", off), s, got, lokiDynamicWindows(s))
		if reads := len(identityStatsCalls(emu)); off == (reads == 1) {
			t.Fatalf("fold off %v: %d bucket reads; want one folded, chunks one by one", off, reads)
		}
	}
}

// dynSpanScenario is a sliding window over many lines where every series spans buckets: each request id is logged
// twice, a bucket apart (the worst case of the fold, every series keeps its partials), over 40 minutes at a 30 s step
// under [2m].
func dynSpanScenario(lines int) dynWindowScenario {
	s := dynWindowScenario{outer: "sum", by: "pod", fn: "avg_over_time", parser: "logfmt", window: 2 * time.Minute, step: 30 * time.Second}
	base := int64(1_800_000_000) * 1e9
	s.start, s.end = base, base+80*int64(30*time.Second)
	span := s.end - s.start + int64(s.window) - int64(s.step)
	for i := 0; i < lines/2; i++ {
		ts := s.start - int64(s.window) + int64(i)*span/int64(lines/2) + 7e6
		labels := map[string]string{"route": []string{"/x", "/y"}[i%2], "req": "r" + strconv.Itoa(i)}
		pod := []string{"a", "b"}[i%2]
		s.rows = append(s.rows, dynWindowRow(ts, pod, "logfmt", labels, float64(i%17)),
			dynWindowRow(ts+int64(s.step), pod, "logfmt", labels, float64(i%13)))
	}
	return s
}

// The fold's worst case: every series spans buckets, so every one keeps its partials and the reads hold a series
// per request id. The time chunks of listed_window_chunks.go bound them: under a read budget of 300 series (over a
// tenant limit of 200) the request is read in chunks, no chunk holds more than the budget, and the answer is the one read whole and Loki's.
//
// conformance: semantics/unwrap-per-series-identity-raw-rows, metric-series-identity
func TestSeriesFoldSpanningChunks(t *testing.T) {
	s := dynSpanScenario(4000)
	want := lokiDynamicWindows(s)
	one, code, body := runDynWindowScenario(t, &vlemu.Emulator{T: t, Rows: s.rows, Field: "amount", Version: "v1.54.0"}, s, Config{})
	if code != http.StatusOK {
		t.Fatalf("one read: %d %.300s", code, body)
	}
	compareDynWindows(t, "one read", s, one, want)
	var chunks, widest int
	listedWindowChunksObserve = func(_, _ int64, m statsReadMeter) {
		chunks++
		if !m.over {
			widest = max(widest, m.series)
		}
	}
	t.Cleanup(func() { listedWindowChunksObserve = nil })
	emu := &vlemu.Emulator{T: t, Rows: s.rows, Field: "amount", Version: "v1.54.0"}
	got, code, body := runDynWindowScenario(t, emu, s, Config{RangeMetricRowLimit: 300, MaxStatsQuerySeries: 200})
	if code != http.StatusOK {
		t.Fatalf("chunked: %d %.300s", code, body)
	}
	compareDynWindows(t, "chunked vs Loki", s, got, want)
	compareDynWindows(t, "chunked vs one read", s, got, one)
	if chunks < 3 || widest > 300 || widest == 0 {
		t.Fatalf("%d chunk reads, the widest holding %d series; want several, each within the budget of 300", chunks, widest)
	}
	if raw := emu.EndpointCalls("/select/logsql/query"); len(raw) > 0 {
		t.Fatalf("read raw rows: %s", raw[0])
	}
	t.Logf("%d chunk reads, the widest %d series", chunks, widest)
}

// The proxy's memory for a folded read does not grow with the lines: a series per line, four times as many lines
// over the same window, answers Loki's values from a read of as many rows (the folded rows are one per bucket and
// group), where the series read one by one hold a row per line; for avg over time (the series of one line folded in
// the bucket stats) and for max over time (folded after them).
//
// conformance: semantics/unwrap-per-series-identity-raw-rows, metric-series-identity
func TestSeriesFoldMemoryBound(t *testing.T) {
	for _, fn := range []string{"avg_over_time", "max_over_time"} {
		read := func(lines int, off bool) int {
			s := dynChunkLines(lines)
			s.fn = fn
			var widest int
			listedWindowChunksObserve = func(_, _ int64, m statsReadMeter) { widest = max(widest, m.series) }
			defer func() { listedWindowChunksObserve = nil }()
			seriesFoldOff = off
			defer func() { seriesFoldOff = false }()
			got, code, body := runDynWindowScenario(t, &vlemu.Emulator{T: t, Rows: s.rows, Field: "amount", Version: "v1.54.0"}, s, Config{RangeMetricRowLimit: 100000})
			if code != http.StatusOK {
				t.Fatalf("%s %d lines (fold off %v): %d %.300s", fn, lines, off, code, body)
			}
			compareDynWindows(t, fmt.Sprintf("%s %d lines", fn, lines), s, got, lokiDynamicWindows(s))
			return widest
		}
		small, large := read(1000, false), read(4000, false)
		if large != small {
			t.Fatalf("%s: folded reads held %d series for 1000 lines and %d for 4000: they grow with the lines", fn, small, large)
		}
		if byLine := read(1000, true); byLine <= 4*small {
			t.Fatalf("%s: read one by one 1000 lines held %d series, folded %d: the fold should hold far fewer", fn, byLine, small)
		}
		t.Logf("%s folded: %d series for 1000 and 4000 lines", fn, small)
	}
}

// dynChunkLines is a sliding window of lines with a key of their own (a series per line), over 40 minutes at a 30 s
// step under [2m], the lines spread so none sits on a bucket edge.
func dynChunkLines(lines int) dynWindowScenario {
	s := dynWindowScenario{outer: "sum", by: "pod", fn: "avg_over_time", parser: "logfmt", window: 2 * time.Minute, step: 30 * time.Second}
	base := int64(1_800_000_000) * 1e9
	s.start, s.end = base, base+80*int64(30*time.Second)
	span := s.end - s.start + int64(s.window)
	for i := 0; i < lines; i++ {
		ts := s.start - int64(s.window) + int64(i)*span/int64(lines) + 7e6
		labels := map[string]string{"route": []string{"/x", "/y"}[i%2], "req": "r" + strconv.Itoa(i)}
		s.rows = append(s.rows, dynWindowRow(ts, []string{"a", "b"}[i%2], "logfmt", labels, float64(i%17)))
	}
	return s
}

// A repeat of a parser's series identity over a range longer than the step asks VictoriaLogs nothing and answers the
// same series. The folded buckets depend on the read's window (a series spans buckets in one read, not in another),
// so the stats extent cache never keeps them: a refresh one minute later reads its window again, in the same number
// of calls, and answers what an uncached proxy answers. The test logs VictoriaLogs' calls and bytes per request.
//
// conformance: semantics/unwrap-per-series-identity-raw-rows, quality/query-range-extent-cache
func TestDynamicIdentityWindowCacheReuse(t *testing.T) {
	const step = 10 * time.Second
	end := time.Unix(1700011800, 0).UTC()
	var rows []vlemu.Row
	for i := 0; i < 900; i++ { // a line every 5 s over the last 75 minutes, a series of its own now and then
		labels := map[string]string{"route": []string{"/x", "/y"}[i%2]}
		if i%7 == 0 {
			labels["req"] = "r" + strconv.Itoa(i)
		}
		ts := end.Add(time.Minute - time.Duration(i)*5*time.Second - 250*time.Millisecond).UnixNano()
		rows = append(rows, dynWindowRow(ts, []string{"a", "b"}[i%3%2], "json", labels, float64(i%23)))
	}
	emu := &vlemu.Emulator{T: t, Rows: rows, Field: "amount"}
	counter := &stubCallCounter{inner: emu.Handler()}
	shimSrv, shim := newExtentShim(t, counter)
	p := newExtentTestProxy(t, shimSrv.URL, true, 256<<20)
	uncachedSrv, _ := newExtentShim(t, emu.Handler())
	uncached := newExtentTestProxy(t, uncachedSrv.URL, false, 256<<20)
	for _, q := range []string{
		`sum by (pod) (avg_over_time({app="dw"} | json | unwrap amount [1m]))`,
		`sum(stddev_over_time({app="dw"} | json | unwrap amount [45s]))`,
		`avg(stdvar_over_time({app="dw"} | json | unwrap amount [1m]))`,
	} {
		get := func(e time.Time) []byte {
			t.Helper()
			code, body := extentQueryRange(t, p, q, e.Add(-time.Hour), e, step, nil)
			p.statsExtentStores.wait()
			if code != http.StatusOK {
				t.Fatalf("%s: %d %s", q, code, body)
			}
			return body
		}
		counter.take()
		shim.reset()
		cold := get(end)
		coldCalls, coldBytes := counter.take()
		if !shimReadsFold(shim, emu) {
			t.Errorf("%s: the stats calls do not fold the series hash written before the gate", q)
		}
		again := get(end)
		repCalls, repBytes := counter.take()
		if coldCalls == 0 || repCalls != 0 || string(dynamicIdentityResult(t, cold)) != string(dynamicIdentityResult(t, again)) {
			t.Errorf("%s: cold %d calls, repeat %d calls, want a repeat with no call and the same answer", q, coldCalls, repCalls)
		}
		refreshed := get(end.Add(time.Minute))
		refCalls, refBytes := counter.take()
		if refCalls != coldCalls {
			t.Errorf("%s refresh: %d calls, the cold request %d: the folded buckets are not kept", q, refCalls, coldCalls)
		}
		if _, want := extentQueryRange(t, uncached, q, end.Add(time.Minute-time.Hour), end.Add(time.Minute), step, nil); string(dynamicIdentityResult(t, refreshed)) != string(dynamicIdentityResult(t, want)) {
			t.Errorf("%s refresh: the answer differs from the uncached one\n cached: %.300s\nuncached: %.300s", q, refreshed, want)
		}
		t.Logf("%s: cold %d calls %d B, repeat %d calls %d B, refresh %d calls %d B", q, coldCalls, coldBytes, repCalls, repBytes, refCalls, refBytes)
	}
}

// shimReadsFold reports that the identity reads were folded: the extent shim saw no stats_query_range call of them,
// and every folded read the emulator saw writes the series hash before the unwrap gate (lower.Profile IdentityAhead)
// and groups by it and the gate's flag (lower.SeriesFold).
func shimReadsFold(s *extentShim, emu *vlemu.Emulator) bool {
	s.mu.Lock()
	for _, c := range s.calls {
		if strings.Contains(c.query, "| hash(__lvp_sid) as __lvp_sid |") {
			s.mu.Unlock()
			return false
		}
	}
	s.mu.Unlock()
	folded := emu.SeriesFoldCalls()
	for _, q := range folded {
		hash := strings.Index(q, "| hash(__lvp_sid) as __lvp_sid |")
		gate := strings.Index(q, `| copy "`)
		if hash < 0 || gate < hash || !strings.Contains(q, "__lvp_sid, __lvp_bad)") {
			return false
		}
	}
	return len(folded) > 0
}

// FuzzDynamicIdentityWindowsMark holds the window decision of a parser's series identity on any query and step: a
// request gets at most one mark; the hash-ahead mark only for a root identity (the one the lowering serves) whose
// range is longer than the step and whose hash the lowering writes ahead of the gate (never a max of max_over_time
// or a min of min_over_time, never an extraction list); and a marked request is not decided again.
//
// conformance: semantics/unwrap-per-series-identity-raw-rows
func FuzzDynamicIdentityWindowsMark(f *testing.F) {
	for _, s := range []struct {
		q    string
		step uint16
	}{
		{`sum by (app) (avg_over_time({app="a"} | json | unwrap n [5m]))`, 60},
		{`sum(stdvar_over_time({app="a"} | logfmt | unwrap n [90s]))`, 60},
		{`max by (app) (max_over_time({app="a"} | json | unwrap n [5m]))`, 60},
		{`min(min_over_time({app="a"} | json | unwrap n [5m]))`, 30},
		{`MAX BY (app) (AVG_OVER_TIME({app="a"} | JSON | UNWRAP n [5m]))`, 60},
		{`topk(1, sum by (app) (avg_over_time({app="a"} | json | unwrap n [5m])))`, 60},
		{`label_replace(sum(avg_over_time({app="a"} | json | unwrap n [5m])), "x", "$1", "app", "(.*)")`, 60},
		{`sum(avg_over_time({app="a"} | json | unwrap n [5m])) * 2`, 60},
		{`sum(avg_over_time({app="a"} | json | unwrap n [1m]))`, 300},
		{`sum(quantile_over_time(0.9, {app="a"} | json | unwrap n [5m]))`, 60},
		{`sum(max_over_time({app="a"} | logfmt | unwrap _TIME [5m]))`, 60},
		{`sum(max_over_time({app="a"} | logfmt n, m | unwrap n [5m]))`, 60},
	} {
		f.Add(s.q, s.step)
	}
	c := cache.New(time.Minute, 100)
	f.Cleanup(c.Close)
	p, err := New(Config{BackendURL: "http://127.0.0.1:1", Cache: c, LogLevel: "error"})
	if err != nil {
		f.Fatal(err)
	}
	f.Cleanup(func() { closeTestProxy(p) })
	f.Fuzz(func(t *testing.T, q string, step uint16) {
		if len(q) > 512 || step == 0 {
			return
		}
		r := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+url.Values{"step": {strconv.Itoa(int(step))}}.Encode(), nil)
		out := p.dynamicIdentityWindows(r, q)
		stream, ahead := streamIdentityOnly(out.Context()), identityAhead(out.Context())
		if stream && ahead {
			t.Fatalf("%q step %d: both marks", q, step)
		}
		if ahead {
			window, ok := dynamicWindowRange(q)
			if !p.dynamicUnwrapIdentity(q) || !ok || window <= time.Duration(step)*time.Second {
				t.Fatalf("%q step %d: the hash ahead without a root identity over a range longer than the step (%v %s)", q, step, ok, window)
			}
			if !identityAheadWritten(q) || listedIdentity(q) {
				t.Fatalf("%q step %d: the hash ahead for a query whose lowering writes no hash ahead of the gate", q, step)
			}
		}
		if (stream || ahead) && p.dynamicIdentityWindows(out, q) != out {
			t.Fatalf("%q step %d: a marked request decided again", q, step)
		}
	})
}

// A failed bucket read of a request without the hash-ahead mark falls back as before (no error); with it, the failure
// is the request's answer as it is (the time chunks cut a read past its bound and read it again).
//
// conformance: semantics/unwrap-per-series-identity-raw-rows
func TestIdentityAheadReadError(t *testing.T) {
	plain := context.Background()
	ahead := context.WithValue(plain, identityAheadKey{}, true)
	other := errors.New("backend returned 503")
	if err := identityAheadReadError(plain, errBodyTooLarge); err != nil {
		t.Errorf("an unmarked request: %v, want the fallback", err)
	}
	for _, err := range []error{other, errBodyTooLarge} {
		if got := identityAheadReadError(ahead, err); got != err {
			t.Errorf("a marked request's failure %v: %v, want it as it is", err, got)
		}
	}
}

// identityStatsCalls are the calls the emulator saw that read a parser's identity buckets (the hash before the gate,
// grouped by it): the folded reads (of /select/logsql/query) and the reads of every series (of stats_query_range).
func identityStatsCalls(emu *vlemu.Emulator) (buckets []string) {
	for _, q := range append(emu.EndpointCalls("/select/logsql/stats_query_range"), emu.SeriesFoldCalls()...) {
		if strings.Contains(q, "hash(__lvp_sid) as __lvp_sid") && strings.Contains(q, "__lvp_sid, __lvp_bad)") {
			buckets = append(buckets, q)
		}
	}
	return buckets
}

// seriesFoldMark is in the text of a folded read only: its series stats collect each series' bucket rows.
const seriesFoldMark = ") as " + lower.SeriesFoldRows

// streamLabel is the value of label in a stream's text, `{pod="a",app="b"}`.
func streamLabel(stream, label string) string {
	for _, part := range strings.Split(strings.Trim(stream, "{}"), ",") {
		if k, v, ok := strings.Cut(part, "="); ok && k == label {
			return strings.Trim(v, `"`)
		}
	}
	return ""
}

// FuzzSeriesFold holds the folded read of a parser's series identity to the series read one by one and to the
// reference over generated rows: any seed, outer aggregation, clause, function, parser and window (a range twice or
// five times the step, or one the step does not divide).
//
// conformance: semantics/unwrap-per-series-identity-raw-rows, metric-series-identity
func FuzzSeriesFold(f *testing.F) {
	for _, seed := range []struct {
		seed          int64
		outer, by, fn uint8
		json          bool
	}{{1, 0, 0, 0, true}, {7, 1, 1, 3, false}, {42, 2, 2, 4, true}, {99, 3, 3, 1, false}, {5, 0, 2, 2, true}} {
		f.Add(seed.seed, seed.outer, seed.by, seed.fn, seed.json)
	}
	outers := []string{"sum", "avg", "max", "min"}
	bys := []string{"", "pod", "route", "pod, route"}
	fns := []string{"avg_over_time", "max_over_time", "min_over_time", "stddev_over_time", "stdvar_over_time"}
	f.Fuzz(func(t *testing.T, seed int64, outer, by, fn uint8, json bool) {
		o, fun := outers[int(outer)%len(outers)], fns[int(fn)%len(fns)]
		if o == "max" && fun == "max_over_time" || o == "min" && fun == "min_over_time" {
			return // no series hash (TestDynamicIdentityWindowsKeepExtremes)
		}
		parser := "logfmt"
		if json {
			parser = "json"
		}
		s := generateDynWindowScenario(rand.New(rand.NewSource(seed)), o, bys[int(by)%len(bys)], fun, parser)
		checkSeriesFoldScenario(t, fmt.Sprintf("seed %d", seed), s, "v1.54.0", true)
	})
}

// A folded read whose text would pass VictoriaLogs' default query length is never sent: the read is sent again
// reading every series (a shorter text: the fold's pipes are not in it), which answers the reference's values with no
// error. The filter is as long as the read of every series allows.
//
// conformance: semantics/unwrap-per-series-identity-raw-rows
func TestSeriesFoldTooLongReadsEverySeries(t *testing.T) {
	s := generateDynWindowScenario(rand.New(rand.NewSource(11)), "sum", "pod", "avg_over_time", "json")
	// The same query with a short line filter is folded.
	short := s
	short.parser = `json != "nope"`
	shortEmu := &vlemu.Emulator{T: t, Rows: s.rows, Field: "amount", Version: "v1.54.0"}
	if _, code, body := runDynWindowScenario(t, shortEmu, short, Config{}); code != http.StatusOK || len(identityStatsCalls(shortEmu)) != 1 ||
		!strings.Contains(identityStatsCalls(shortEmu)[0], seriesFoldMark) {
		t.Fatalf("a short filter: %d, reads %v: %.200s", code, identityStatsCalls(shortEmu), body)
	}
	// The texts of the short filter, folded and read every series: the long filter fills the latter up to the length.
	shortOff := &vlemu.Emulator{T: t, Rows: s.rows, Field: "amount", Version: "v1.54.0"}
	seriesFoldOff = true
	_, code, _ := runDynWindowScenario(t, shortOff, short, Config{})
	seriesFoldOff = false
	if code != http.StatusOK || len(identityStatsCalls(shortOff)) != 1 {
		t.Fatalf("a short filter read every series: %d, %d reads", code, len(identityStatsCalls(shortOff)))
	}
	every, folded := len(identityStatsCalls(shortOff)[0]), len(identityStatsCalls(shortEmu)[0])
	long := strings.Repeat("x", 16384-every)
	if folded+len(long) <= 16384 {
		t.Fatalf("the fold adds %d characters: no filter both texts allow", folded-every)
	}
	emu := &vlemu.Emulator{T: t, Rows: s.rows, Field: "amount", Version: "v1.54.0"}
	backend := httptest.NewServer(emu.Handler())
	defer backend.Close()
	c := cache.New(time.Minute, 100)
	defer c.Close()
	p, err := New(Config{BackendURL: backend.URL, Cache: c, LogLevel: "error"})
	if err != nil {
		t.Fatal(err)
	}
	defer closeTestProxy(p)
	// A line filter no line holds: the emulator selects every row, as it does without it.
	q := strings.Replace(s.query(), `{app="dw"}`, `{app="dw"} != "`+long+`"`, 1)
	params := url.Values{"query": {q}, "start": {strconv.FormatInt(s.start/1e9, 10)}, "end": {strconv.FormatInt(s.end/1e9, 10)},
		"step": {strconv.FormatInt(int64(s.step/time.Second), 10)}}
	res := doCompatProxyRequest(p, "/loki/api/v1/query_range?"+params.Encode(), map[string]string{"X-Scope-OrgID": "0"})
	if res.Code != http.StatusOK {
		t.Fatalf("%d %.300s", res.Code, res.Body)
	}
	reads := identityStatsCalls(emu)
	if len(reads) != 1 || strings.Contains(reads[0], seriesFoldMark) || len(reads[0]) > 16384 {
		t.Fatalf("%d reads (folded %v), want one read of every series under the query length", len(reads), len(reads) > 0 && strings.Contains(reads[0], seriesFoldMark))
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
		key := labelsSortedKey(series.Metric)
		got[key] = map[int64]float64{}
		for _, point := range series.Values {
			v, _ := strconv.ParseFloat(point[1].(string), 64)
			got[key][int64(point[0].(float64))*1e9] = v
		}
	}
	compareDynWindows(t, "too long", s, got, lokiDynamicWindows(s))
}
