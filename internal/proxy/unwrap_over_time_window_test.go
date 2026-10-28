package proxy

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// This file pins sum, avg, max and min over time with unwrap to Loki's window
// (t-range, t] at every step, whatever the range and the step are, and on
// instant queries (pkg/logql/range_vector.go:102-189 batchRangeVectorIterator
// Next and load: the lower bound is exclusive, the evaluation time inclusive;
// range_vector.go:371-420 sumOverTime, avgOverTime, maxOverTime, minOverTime),
// with the outer aggregation applied per step (pkg/logql/evaluator.go
// VectorAggEvaluator.Next). The reference below is written apart from the
// proxy. The proxy under test is the real handler over the emulated
// VictoriaLogs (vlEmulator), and the answer must come from stats: raw rows
// only where the bucket grid would pass Loki's 11,000 points
// (heavyQueryBucketThreshold), which TestUnwrapOverTimeRawWindowBounds forces.

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
// range function per series, and the outer aggregation (sum, max, min, avg or
// count; "" is sum) of the series per group key (group returns it from the
// series' stream). A group whose series have no sample at t has no point there.
func lokiOverTimeGroups(rows []emuRow, fn, outer string, window, offset int64, times []int64, group func(stream string) string) map[string]map[int64]float64 {
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
		members := map[string][]float64{}
		for key, values := range perSeries {
			members[groupOf[key]] = append(members[groupOf[key]], lokiOverTimeValue(fn, values))
		}
		for g, values := range members {
			if out[g] == nil {
				out[g] = map[int64]float64{}
			}
			var v float64
			switch outer {
			case "", "sum":
				for _, x := range values {
					v += x
				}
			case "count":
				v = float64(len(values))
			case "avg":
				for _, x := range values {
					v += x
				}
				v /= float64(len(values))
			case "max", "min":
				v = values[0]
				for _, x := range values[1:] {
					if (outer == "max" && x > v) || (outer == "min" && x < v) {
						v = x
					}
				}
			default:
				panic("reference outer: " + outer)
			}
			out[g][t] = v
		}
	}
	return out
}

// owShape is one query form: the query around a range aggregation, and the
// group key of a stream under it, as the response labels it (owSeriesKey).
type owShape struct {
	name string
	// query wraps the range aggregation text.
	query func(rangeAgg string) string
	// outer is the outer aggregation the reference applies ("" sum).
	outer string
	// group maps a stream to its result series key.
	group func(stream string) string
	// parser reports whether the form runs with this parser ("" none).
	parser func(parser, fn string) bool
	// fold sums the answer's series per step before the comparison.
	fold bool
	// rangeOnly keeps the form off instant queries.
	rangeOnly bool
}

func owPod(stream string) string {
	return emuStreamLabelRE.FindAllStringSubmatch(stream, -1)[1][2]
}

func owAnyParser(string, string) bool { return true }

// owNoParser: the outer aggregations other than sum read one value per Loki
// series, which a parser names by the stream and the labels it extracts; that
// series identity is pinned apart (metric-series-identity).
func owNoParser(parser, _ string) bool { return parser == "" }

func owByPod(stream string) string { return "pod=" + owPod(stream) }

var owShapes = []owShape{
	{
		name:   "sum by (pod)",
		query:  func(ra string) string { return "sum by (pod) (" + ra + ")" },
		group:  owByPod,
		parser: owAnyParser,
	},
	{
		name:   "per stream",
		query:  func(ra string) string { return ra },
		group:  func(stream string) string { return "app=uw,pod=" + owPod(stream) },
		parser: owAnyParser,
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
		group:  owByPod,
		parser: owAnyParser,
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
		group:  owByPod,
		parser: owAnyParser,
	},
	{
		// The outer aggregations other than sum read one value per series
		// (foldVectorAggregations) after the window; max by over
		// max_over_time is grouped upstream (unwrapGroupPushdown), whatever
		// names the series.
		name:   "max by (pod)",
		query:  func(ra string) string { return "max by (pod) (" + ra + ")" },
		outer:  "max",
		group:  owByPod,
		parser: func(parser, fn string) bool { return parser == "" || fn == "max_over_time" },
	},
	{
		name:   "min by (pod)",
		query:  func(ra string) string { return "min by (pod) (" + ra + ")" },
		outer:  "min",
		group:  owByPod,
		parser: func(parser, fn string) bool { return parser == "" || fn == "min_over_time" },
	},
	{
		name:   "max",
		query:  func(ra string) string { return "max(" + ra + ")" },
		outer:  "max",
		group:  func(string) string { return "" },
		parser: func(parser, fn string) bool { return parser == "" || fn == "max_over_time" },
	},
	{
		// The ranking is applied after the max by (handleRangeMetricPostAggregation);
		// an instant topk over a series-level chain is a residual of its own.
		name:      "topk of max by (pod)",
		query:     func(ra string) string { return "topk(5, max by (pod) (" + ra + "))" },
		outer:     "max",
		group:     owByPod,
		parser:    func(parser, fn string) bool { return parser == "" || fn == "max_over_time" },
		rangeOnly: true,
	},
	{
		name:   "avg",
		query:  func(ra string) string { return "avg(" + ra + ")" },
		outer:  "avg",
		group:  func(string) string { return "" },
		parser: owNoParser,
	},
	{
		// A labelled without as the outermost aggregation is folded from a row
		// per stream (withWithoutFold).
		name:   "max without (pod)",
		query:  func(ra string) string { return "max without (pod) (" + ra + ")" },
		outer:  "max",
		group:  func(string) string { return "app=uw" },
		parser: owNoParser,
	},
	{
		name:   "count by (pod)",
		query:  func(ra string) string { return "count by (pod) (" + ra + ")" },
		outer:  "count",
		group:  owByPod,
		parser: owNoParser,
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
	window, step time.Duration // step 0: an instant query at end
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
	if c.step == 0 {
		return []int64{c.end}
	}
	var out []int64
	for t := c.start; t <= c.end; t += int64(c.step) {
		out = append(out, t)
	}
	return out
}

// want returns Loki's answer, the scalar of a binary shape applied.
func (c owCase) want() map[string]map[int64]float64 {
	out := lokiOverTimeGroups(c.rows, c.fn, c.shape.outer, int64(c.window), int64(c.offset), c.times(), c.shape.group)
	if strings.HasSuffix(c.query(), " * 2") {
		for _, points := range out {
			for t, v := range points {
				points[t] = v * 2
			}
		}
	}
	return out
}

func runOverTimeCase(t *testing.T, emu *vlEmulator, c owCase, seriesCap int) map[string]map[int64]float64 {
	t.Helper()
	backend := httptest.NewServer(emu.handler())
	defer backend.Close()
	rc := cache.New(60*time.Second, 1000)
	defer rc.Close()
	p, err := New(Config{BackendURL: backend.URL, Cache: rc, LogLevel: "error", BackendMinVersion: "v1.40.0", BackendVersionCheckTimeout: time.Second, MaxStatsQuerySeries: seriesCap})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = p.Shutdown(context.Background()) }()
	if emu.version != "" {
		if err := p.ValidateBackendVersionCompatibility(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	params := url.Values{"query": {c.query()}}
	target := "/loki/api/v1/query_range?"
	if c.step == 0 {
		target = "/loki/api/v1/query?"
		params.Set("time", strconv.FormatInt(c.end/1e9, 10))
	} else {
		params.Set("start", strconv.FormatInt(c.start/1e9, 10))
		params.Set("end", strconv.FormatInt(c.end/1e9, 10))
		params.Set("step", strconv.FormatInt(int64(c.step/time.Second), 10))
	}
	res := doCompatProxyRequest(p, target+params.Encode(), map[string]string{"X-Scope-OrgID": "0"})
	if res.Code != http.StatusOK {
		t.Fatalf("%s: %d %s", c.query(), res.Code, res.Body)
	}
	return owParseAnswer(t, c, res.Body.Bytes())
}

// owParseAnswer reads an answer as series key -> evaluation time (ns) -> value.
func owParseAnswer(t testing.TB, c owCase, body []byte) map[string]map[int64]float64 {
	t.Helper()
	var resp struct {
		Data struct {
			Result []struct {
				Metric map[string]string `json:"metric"`
				Values [][]any           `json:"values"`
				Value  []any             `json:"value"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatal(err)
	}
	got := map[string]map[int64]float64{}
	for _, series := range resp.Data.Result {
		points := series.Values
		if series.Value != nil {
			points = [][]any{series.Value}
		}
		key := owSeriesKey(series.Metric)
		if c.shape.fold {
			key = ""
		} else if got[key] != nil {
			t.Fatalf("%s: series {%s} twice", c.query(), key)
		}
		if got[key] == nil {
			got[key] = map[int64]float64{}
		}
		for _, point := range points {
			v, _ := strconv.ParseFloat(point[1].(string), 64)
			got[key][int64(point[0].(float64))*1e9] += v
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

// owWindowMergeRE matches the bucket stats of the window merge (unwrapBucketAgg).
var owWindowMergeRE = regexp.MustCompile(`\) as (c|__lvp_sum)(,|$|\n)`)

// checkOverTimeRoute: the answer came from VictoriaLogs stats, never from raw
// rows, and a range longer than the step merged its windows from buckets of
// the partial aggregates (the sample count is their presence). An instant
// query over a parser reads its one window's rows (proxyManualRangeMetricInstant).
func checkOverTimeRoute(t *testing.T, label string, emu *vlEmulator, c owCase) {
	t.Helper()
	if c.parser != "" && (c.step == 0 || (c.shape.name == "per stream" && c.fn == "avg_over_time")) {
		return
	}
	if raw := emu.endpointCalls("/select/logsql/query"); len(raw) > 0 {
		t.Fatalf("%s: the metric read raw rows: %s\nquery %s step=%v window=%v", label, raw[0], c.query(), c.step, c.window)
	}
	stats := append(emu.endpointCalls("/select/logsql/stats_query_range"), emu.endpointCalls("/select/logsql/stats_query")...)
	if len(stats) == 0 {
		t.Fatalf("%s: no stats query\nquery %s", label, c.query())
	}
	if c.step > 0 && c.window > c.step && (c.parser == "" || c.shape.name != "per stream") && !owWindowMergeRE.MatchString(strings.Join(stats, "\n")) {
		t.Fatalf("%s: a range longer than the step read step buckets: %s\nquery %s", label, stats[0], c.query())
	}
}

// owGenerate draws a case: windows longer than, shorter than, equal to and not
// a multiple of the step, starts off every grid, and rows dense or sparse with
// values on window edges, zeros and repeated values. step 0 draws an instant
// query.
func owGenerate(rnd *rand.Rand, fn, parser string, shape owShape, instant bool) owCase {
	steps := []time.Duration{30 * time.Second, time.Minute, 90 * time.Second, 2 * time.Minute}
	windows := []time.Duration{30 * time.Second, 45 * time.Second, time.Minute, 90 * time.Second, 150 * time.Second, 5 * time.Minute, 7 * time.Minute}
	c := owCase{fn: fn, parser: parser, shape: shape, step: steps[rnd.Intn(len(steps))], window: windows[rnd.Intn(len(windows))]}
	if rnd.Intn(4) == 0 {
		c.offset = time.Duration(1+rnd.Intn(3)) * time.Minute
	}
	base := int64(1_800_000_000) * 1e9
	c.start = base + int64(rnd.Intn(900))*1e9
	c.end = c.start + int64(rnd.Intn(15))*int64(c.step)
	edgeStep := c.step
	if instant {
		c.step, c.start = 0, c.end
	}
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
				edge := c.end - int64(c.offset)
				if !instant {
					edge = c.start - int64(c.offset) + int64(rnd.Intn(16))*int64(edgeStep)
				}
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
// window holds a line, from VictoriaLogs stats, on query_range and on instant
// queries, on the oldest and the newest supported VictoriaLogs and on an
// unknown version.
//
// conformance: semantics/unwrap-over-time-range-windows, window-bounds-and-step-alignment
func TestUnwrapOverTimeWindowsDifferential(t *testing.T) {
	for _, version := range []string{"", "v1.49.0", "v1.53.0"} {
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
							c := owGenerate(rnd, fn, parser, shape, i%6 == 5 && !shape.rangeOnly)
							emu := &vlEmulator{t: t, rows: c.rows, field: "amount", version: version}
							got := runOverTimeCase(t, emu, c, 0)
							label := fmt.Sprintf("%s #%d", name, i)
							compareOverTime(t, label, c, got, c.want())
							checkOverTimeRoute(t, label, emu, c)
						}
					})
				}
			}
		}
	}
}

// owRawAnswer runs the case through the handler and returns the status and the
// body as sent.
func owRawAnswer(t *testing.T, emu *vlEmulator, c owCase, seriesCap int) (int, string) {
	t.Helper()
	backend := httptest.NewServer(emu.handler())
	defer backend.Close()
	rc := cache.New(60*time.Second, 1000)
	defer rc.Close()
	p, err := New(Config{BackendURL: backend.URL, Cache: rc, LogLevel: "error", BackendMinVersion: "v1.40.0", BackendVersionCheckTimeout: time.Second, MaxStatsQuerySeries: seriesCap})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = p.Shutdown(context.Background()) }()
	if emu.version != "" {
		if err := p.ValidateBackendVersionCompatibility(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	params := url.Values{"query": {c.query()}, "start": {strconv.FormatInt(c.start/1e9, 10)}, "end": {strconv.FormatInt(c.end/1e9, 10)}, "step": {strconv.FormatInt(int64(c.step/time.Second), 10)}}
	res := doCompatProxyRequest(p, "/loki/api/v1/query_range?"+params.Encode(), map[string]string{"X-Scope-OrgID": "0"})
	if res.Code != http.StatusOK {
		return res.Code, res.Body.String()
	}
	// The series in label order, as Loki sorts its matrix, each with its labels
	// (a JSON object, whose key order means nothing) and every byte of its
	// values as sent.
	var resp struct {
		Data struct {
			Result []struct {
				Metric map[string]string `json:"metric"`
				Values json.RawMessage   `json:"values"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(res.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	series := make([]string, 0, len(resp.Data.Result))
	for _, r := range resp.Data.Result {
		metric, _ := json.Marshal(r.Metric)
		series = append(series, string(metric)+"="+string(r.Values))
	}
	sort.Strings(series)
	return res.Code, strings.Join(series, "\n")
}

// TestUnwrapGroupPushdownMatchesPerStreamFold: `max by (L) (max_over_time(...))`
// and `min by (L) (min_over_time(...))`, grouped or not and under topk, answer
// what the per-stream fold answers (unwrapGroupPushdownOff), every series'
// values byte for byte under the same labels (the fold's series order is not
// Loki's label order): the
// same series, timestamps, values and sample presence over generated rows with
// sparse and dense windows and lines on window edges, and the same 400 when the
// groups pass max_query_series (Loki counts the limit on the groups:
// pkg/logql/engine.go:530-590 JoinSampleVector), while VictoriaLogs reads one
// row per group and bucket instead of one per stream.
//
// conformance: semantics/unwrap-over-time-range-windows, metric-series-identity
func TestUnwrapGroupPushdownMatchesPerStreamFold(t *testing.T) {
	t.Cleanup(func() { unwrapGroupPushdownOff = false })
	byAppPod := func(stream string) string { return "app=uw,pod=" + owPod(stream) }
	shapes := []owShape{owShapes[6], owShapes[7], owShapes[8], owShapes[9],
		// Two outer labels in either order: each value under its own name.
		{name: "max by (app, pod)", query: func(ra string) string { return "max by (app, pod) (" + ra + ")" }, outer: "max", group: byAppPod},
		{name: "min by (pod, app)", query: func(ra string) string { return "min by (pod, app) (" + ra + ")" }, outer: "min", group: byAppPod},
		// detected_level (unknown for a stream with no level) keeps the fold.
		{name: "max by (detected_level, pod)", query: func(ra string) string { return "max by (detected_level, pod) (" + ra + ")" }, outer: "max", group: owByPod},
	}
	pushed := 0
	for _, version := range []string{"", "v1.49.0", "v1.53.0"} {
		for _, parser := range []string{"", "logfmt"} {
			for _, shape := range shapes {
				if parser != "" && shape.name == "max" {
					// Grouped per line without the push-down: no fold to compare
					// with (TestUnwrapOverTimeWindowsDifferential holds it to Loki).
					continue
				}
				fn := "max_over_time"
				if shape.outer == "min" {
					fn = "min_over_time"
				}
				name := fmt.Sprintf("version=%s/parser=%s/%s", version, parser, shape.name)
				t.Run(name, func(t *testing.T) {
					rnd := rand.New(rand.NewSource(int64(len(name))*6151 + 7))
					for i := 0; i < 15; i++ {
						c := owGenerate(rnd, fn, parser, shape, false)
						limit := []int{0, 1, 2, 3}[rnd.Intn(4)]
						emu := &vlEmulator{t: t, rows: c.rows, field: "amount", version: version}
						unwrapGroupPushdownOff = false
						gotCode, got := owRawAnswer(t, emu, c, limit)
						unwrapGroupPushdownOff = true
						wantCode, want := owRawAnswer(t, &vlEmulator{t: t, rows: c.rows, field: "amount", version: version}, c, limit)
						unwrapGroupPushdownOff = false
						if gotCode != wantCode || got != want {
							t.Fatalf("#%d %s step=%v window=%v limit=%d:\npushed %d %s\nfolded %d %s", i, c.query(), c.step, c.window, limit, gotCode, got, wantCode, want)
						}
						if c.window > c.step && !strings.Contains(shape.name, "detected_level") {
							for _, q := range emu.endpointCalls("/select/logsql/stats_query_range") {
								if strings.Contains(q, "_stream") {
									t.Fatalf("#%d %s: the pushed-down query reads a series per stream: %s", i, c.query(), q)
								}
								pushed++
							}
						}
					}
				})
			}
		}
	}
	if pushed == 0 {
		t.Fatal("no case took the pushed-down route")
	}
}

// TestUnwrapGroupPushdownDerivesServiceName: a pushed-down `max by
// (service_name)` groups by the service_name the translation derives ahead of
// the stats (the coalesce over the stored names), as `sum by (service_name)`
// does, so a stream without a stored service_name is grouped under the derived
// one rather than an empty label. The emulator runs no coalesce, so the
// query is checked.
//
// conformance: semantics/unwrap-over-time-range-windows, metric-series-identity
func TestUnwrapGroupPushdownDerivesServiceName(t *testing.T) {
	const base = int64(1_800_000_000) * 1e9
	rows := []emuRow{{ts: base - 60e9, stream: `{app="uw",pod="a"}`, value: 1}}
	for _, query := range []string{
		`max by (service_name) (max_over_time({app="uw"} | unwrap amount [5m]))`,
		`min by (service_name) (min_over_time({app="uw"} | logfmt | unwrap amount [5m]))`,
	} {
		emu := &vlEmulator{t: t, rows: rows, field: "amount", version: "v1.53.0"}
		c := owCase{shape: owShape{query: func(string) string { return query }}, step: time.Minute, start: base, end: base + 120e9}
		if code, body := owRawAnswer(t, emu, c, 0); code != http.StatusOK {
			t.Fatalf("%s: %d %s", query, code, body)
		}
		calls := emu.endpointCalls("/select/logsql/stats_query_range")
		if len(calls) != 1 {
			t.Fatalf("%s: %d stats calls", query, len(calls))
		}
		q := calls[0]
		derive, group := strings.Index(q, "coalesce(service_name"), strings.Index(q, "| stats by (service_name, __lvp_bad) ")
		if derive < 0 || group < derive || strings.Contains(q, "_stream") {
			t.Fatalf("%s: want service_name derived ahead of a grouping by it, without a series per stream: %s", query, q)
		}
	}
}

// TestUnwrapOverTimeRangeWindowEdgeCases: hand-picked rows and windows with
// their answers spelled out. The rows of pod a sit at base+10s (value 1),
// base+70s (2), base+130s (4) and base+190s (8), one on every window edge of a
// [2m] range at a 60s step from base+70s; pod b holds one line at base+100s
// (value 16). Each sample is Loki's window (t-120s, t]: the line on the lower
// edge belongs to the window before, the line at t to this one.
//
// conformance: semantics/unwrap-over-time-range-windows, window-bounds-and-step-alignment
func TestUnwrapOverTimeRangeWindowEdgeCases(t *testing.T) {
	const base = int64(1_800_000_000) * 1e9
	rows := []emuRow{
		{ts: base + 10e9, stream: `{app="uw",pod="a"}`, value: 1},
		{ts: base + 70e9, stream: `{app="uw",pod="a"}`, value: 2},
		{ts: base + 130e9, stream: `{app="uw",pod="a"}`, value: 4},
		{ts: base + 190e9, stream: `{app="uw",pod="a"}`, value: 8},
		{ts: base + 100e9, stream: `{app="uw",pod="b"}`, value: 16},
	}
	at := func(sec int64) int64 { return base + sec*1e9 }
	for _, tc := range []struct {
		fn    string
		shape owShape
		want  map[string]map[int64]float64
	}{
		// t=70: (−50, 70] holds a:1,2. t=130: (10, 130] holds a:2,4 and b:16.
		// t=190: (70, 190] holds a:4,8 and b:16. t=250: (130, 250] holds a:8.
		// t=310: (190, 310] holds nothing: no sample.
		{"sum_over_time", owShapes[0], map[string]map[int64]float64{
			"pod=a": {at(70): 3, at(130): 6, at(190): 12, at(250): 8},
			"pod=b": {at(130): 16, at(190): 16},
		}},
		{"avg_over_time", owShapes[0], map[string]map[int64]float64{
			"pod=a": {at(70): 1.5, at(130): 3, at(190): 6, at(250): 8},
			"pod=b": {at(130): 16, at(190): 16},
		}},
		{"max_over_time", owShapes[0], map[string]map[int64]float64{
			"pod=a": {at(70): 2, at(130): 4, at(190): 8, at(250): 8},
			"pod=b": {at(130): 16, at(190): 16},
		}},
		{"min_over_time", owShapes[0], map[string]map[int64]float64{
			"pod=a": {at(70): 1, at(130): 2, at(190): 4, at(250): 8},
			"pod=b": {at(130): 16, at(190): 16},
		}},
		// One series: the per-stream values added up per step.
		{"max_over_time", owShapes[2], map[string]map[int64]float64{
			"": {at(70): 2, at(130): 20, at(190): 24, at(250): 8},
		}},
		{"avg_over_time", owShapes[10], map[string]map[int64]float64{
			"": {at(70): 1.5, at(130): 9.5, at(190): 11, at(250): 8},
		}},
	} {
		for _, parser := range []string{"", "logfmt", "json"} {
			if !tc.shape.parser(parser, tc.fn) {
				continue
			}
			for _, version := range []string{"v1.49.0", "v1.53.0"} {
				label := fmt.Sprintf("%s/%s/%s/%s", tc.fn, tc.shape.name, parser, version)
				t.Run(label, func(t *testing.T) {
					c := owCase{fn: tc.fn, parser: parser, shape: tc.shape, window: 2 * time.Minute, step: time.Minute, start: at(70), end: at(310), rows: rows}
					if fmt.Sprint(c.want()) != fmt.Sprint(tc.want) {
						t.Fatalf("reference sanity: %v, spelled out %v", c.want(), tc.want)
					}
					emu := &vlEmulator{t: t, rows: rows, field: "amount", version: version}
					compareOverTime(t, label, c, runOverTimeCase(t, emu, c, 0), tc.want)
					checkOverTimeRoute(t, label, emu, c)
				})
			}
		}
	}
}

// TestUnwrapOverTimeRawWindowBounds: a bucket grid of more than Loki's 11,000
// points (a range 1 ms longer than the step makes 1 ms buckets) is answered
// from raw rows, which must read the same window (t-range, t]: the line on the
// lower edge is out, the line at t is in, for every function.
//
// conformance: semantics/unwrap-over-time-range-windows, semantics/lower-bound-exclusive, window-bounds-and-step-alignment
func TestUnwrapOverTimeRawWindowBounds(t *testing.T) {
	for _, fn := range []string{"sum_over_time", "avg_over_time", "max_over_time", "min_over_time"} {
		t.Run(fn, func(t *testing.T) {
			rnd := rand.New(rand.NewSource(int64(len(fn)) * 4241))
			for i := 0; i < 15; i++ {
				c := owGenerate(rnd, fn, "logfmt", owShapes[0], false)
				c.offset = 0
				c.window = c.step + time.Millisecond
				// Lines on the window edges of this grid, and a nanosecond beside them.
				for j := range c.rows {
					if j%4 == 0 {
						edge := c.start + int64(rnd.Intn(12))*int64(c.step)
						if j%8 == 0 {
							edge -= int64(c.window)
						}
						c.rows[j].ts = edge + int64(rnd.Intn(3)-1)
					}
				}
				emu := &vlEmulator{t: t, rows: c.rows, field: "amount", version: "v1.53.0"}
				got := runOverTimeCase(t, emu, c, 0)
				if len(emu.endpointCalls("/select/logsql/query")) == 0 {
					t.Fatalf("%s: expected the raw-row evaluator, got stats only", c.query())
				}
				compareOverTime(t, fmt.Sprintf("%s #%d", fn, i), c, got, c.want())
			}
		})
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
				emu := &vlEmulator{t: t, rows: rows, field: "amount", version: "v1.53.0"}
				got := runOverTimeCase(t, emu, c, 0)
				want := c.want()
				if n := len(want["pod=a"]); n != tc.samples {
					t.Fatalf("reference sanity: %d samples, want %d", n, tc.samples)
				}
				compareOverTime(t, fn, c, got, want)
				checkOverTimeRoute(t, fn, emu, c)
			})
		}
	}
}

// TestUnwrapOverTimeUngroupedSumIgnoresSeriesLimit: an ungrouped sum or avg of
// avg, max or min over time is one series, so Loki counts one series against
// max_query_series however many streams it folds; the per-stream buckets the
// proxy reads for it do not count either (they are bounded by the raw-row
// budget, as a `without` is). A grouped sum over more groups than the limit
// still answers Loki's error.
//
// conformance: semantics/unwrap-over-time-range-windows, metric-series-identity
func TestUnwrapOverTimeUngroupedSumIgnoresSeriesLimit(t *testing.T) {
	const base = int64(1_800_000_000) * 1e9
	var rows []emuRow
	for i := 0; i < 12; i++ { // twelve streams, six times the limit
		for ts := base - int64(5*time.Minute); ts < base+int64(10*time.Minute); ts += int64(40*time.Second) + int64(i)*1e9 {
			rows = append(rows, emuRow{ts: ts, stream: fmt.Sprintf(`{app="uw",pod="p%d"}`, i), value: float64(i + 1)})
		}
	}
	for _, fn := range []string{"avg_over_time", "max_over_time", "min_over_time"} {
		for _, shape := range []owShape{owShapes[2], owShapes[10]} {
			t.Run(fn+"/"+shape.name, func(t *testing.T) {
				c := owCase{fn: fn, shape: shape, window: 5 * time.Minute, step: time.Minute, start: base, end: base + int64(10*time.Minute), rows: rows}
				emu := &vlEmulator{t: t, rows: rows, field: "amount", version: "v1.53.0"}
				got := runOverTimeCase(t, emu, c, 2)
				compareOverTime(t, fn, c, got, c.want())
				checkOverTimeRoute(t, fn, emu, c)
			})
		}
	}
	t.Run("grouped over the limit", func(t *testing.T) {
		backend := httptest.NewServer((&vlEmulator{t: t, rows: rows, field: "amount", version: "v1.53.0"}).handler())
		defer backend.Close()
		rc := cache.New(60*time.Second, 1000)
		defer rc.Close()
		p, err := New(Config{BackendURL: backend.URL, Cache: rc, LogLevel: "error", BackendVersionCheckTimeout: time.Second, MaxStatsQuerySeries: 2})
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = p.Shutdown(context.Background()) }()
		params := url.Values{"query": {`sum by (pod) (max_over_time({app="uw"} | unwrap amount [5m]))`}, "start": {strconv.FormatInt(base/1e9, 10)}, "end": {strconv.FormatInt(base/1e9+600, 10)}, "step": {"60"}}
		res := doCompatProxyRequest(p, "/loki/api/v1/query_range?"+params.Encode(), map[string]string{"X-Scope-OrgID": "0"})
		if res.Code != http.StatusBadRequest || !strings.Contains(res.Body.String(), "maximum number of series (2) reached") {
			t.Fatalf("12 groups over a limit of 2: %d %s, want Loki's 400", res.Code, res.Body)
		}
	})
}

// TestUnwrapOverTimeResidualShapesKeepStatsBuckets: the forms whose series a
// JSON parser names (grouped per stream and line) are no stats grouping the
// window merge can read from buckets, and the nested aggregations the window
// evaluator does not apply, so they keep VictoriaLogs' step buckets and never
// read raw rows; their windows over a range longer than the step are the open
// row semantics/unwrap-over-time-range-windows-residual-forms.
//
// conformance: semantics/unwrap-over-time-range-windows-residual-forms, semantics/unwrap-over-time-range-windows
func TestUnwrapOverTimeResidualShapesKeepStatsBuckets(t *testing.T) {
	const base = int64(1_800_000_000) * 1e9
	var rows []emuRow
	for ts := base - int64(10*time.Minute); ts < base+int64(10*time.Minute); ts += int64(17 * time.Second) {
		rows = append(rows, emuRow{ts: ts, stream: `{app="uw",pod="a"}`, value: float64(ts / 1e9 % 13)})
	}
	for _, query := range []string{
		`sum(max_over_time({app="uw"} | json | unwrap amount [5m]))`,
		`sum without (pod) (min_over_time({app="uw"} | logfmt | unwrap amount [5m]))`,
		`max by (pod) (sum_over_time({app="uw"} | json | unwrap amount [5m]))`,
		`sum(avg_over_time({app="uw"} | json | unwrap amount | __error__="" [5m]))`,
		`sum by (pod) (sum by (pod, app) (max_over_time({app="uw"} | unwrap amount [5m])))`,
		`max(sum without (pod) (max_over_time({app="uw"} | unwrap amount [5m])))`,
		`sum without () (max_over_time({app="uw"} | unwrap amount [5m]))`,
	} {
		t.Run(query, func(t *testing.T) {
			emu := &vlEmulator{t: t, rows: rows, field: "amount", version: "v1.53.0"}
			c := owCase{shape: owShape{query: func(string) string { return query }}, window: 5 * time.Minute, step: time.Minute, start: base, end: base + int64(10*time.Minute)}
			if got := runOverTimeCase(t, emu, c, 0); len(got) == 0 {
				t.Fatalf("%s: no series", query)
			}
			if raw := emu.endpointCalls("/select/logsql/query"); len(raw) > 0 {
				t.Fatalf("%s: read raw rows: %s", query, raw[0])
			}
			for _, q := range emu.endpointCalls("/select/logsql/stats_query_range") {
				if owWindowMergeRE.MatchString(q) {
					t.Fatalf("%s: answered from the window merge: %s", query, q)
				}
			}
		})
	}
}

// TestWindowPhaseFilterKeepsExtentCacheAcrossSteps: a range shorter than the
// step reads phase-filtered step buckets, whose filter names the window phase
// modulo the step, so a window moved by whole steps (a dashboard refresh on
// the step grid) reuses the buckets the stats extent cache stored; a range
// longer than the step reads anchored sub-step buckets, which it reuses too.
// The answers are byte for byte the uncached proxy's. (The unwrap functions
// read VictoriaLogs whole while their conversion check counts the rows it
// rejects, on main as here.)
//
// conformance: window-bounds-and-step-alignment, quality/query-range-extent-cache
func TestWindowPhaseFilterKeepsExtentCacheAcrossSteps(t *testing.T) {
	s0 := time.Unix(1_700_001_000, 0).UTC()
	var rows []emuRow
	for i := 0; time.Duration(i)*7*time.Second < 3*time.Hour; i++ {
		rows = append(rows, emuRow{ts: s0.Add(time.Duration(i) * 7 * time.Second).UnixNano(), stream: fmt.Sprintf(`{app="uw",pod="p%d"}`, i%3), value: float64(i%11) + 0.5})
	}
	for _, tc := range []struct {
		query string
		step  time.Duration
	}{
		{`sum by (pod) (count_over_time({app="uw"} [30s]))`, 2 * time.Minute},
		{`sum by (pod) (count_over_time({app="uw"} [5m]))`, time.Minute},
	} {
		t.Run(tc.query, func(t *testing.T) {
			first := s0.Add(30 * time.Minute)
			var windows []extentWindow
			for k := 0; k < 6; k++ { // a refresh moving a whole step at a time, then a re-run
				start := first.Add(time.Duration(k) * tc.step)
				windows = append(windows, extentWindow{start: start, end: start.Add(time.Hour), step: tc.step})
			}
			windows = append(windows, windows[0])
			newBackend := func() http.Handler {
				return (&vlEmulator{t: t, rows: rows, field: "amount", version: "v1.50.0"}).handler()
			}
			on, off, withData := runExtentDifferential(t, newBackend, []string{tc.query}, windows, nil)
			if withData != len(windows) {
				t.Fatalf("%d of %d answers held data", withData, len(windows))
			}
			if on*3 > off {
				t.Fatalf("the extent cache read %d buckets, the uncached proxy %d: the stored buckets were not reused", on, off)
			}
		})
	}
}

// TestUnwrapOverTimeShortRangeStaysOnStepBuckets: the daily 7-day unwrap
// shapes ([1m] and [5m] ranges at a 10m step) keep one phase-filtered
// stats_query_range on step buckets, the route a range shorter than the step
// takes; only a range longer than the step merges sub-step buckets.
//
// conformance: semantics/unwrap-over-time-range-windows, window-bounds-and-step-alignment
func TestUnwrapOverTimeShortRangeStaysOnStepBuckets(t *testing.T) {
	const base = int64(1_800_000_000) * 1e9
	var rows []emuRow
	for i := int64(0); i < 300; i++ {
		rows = append(rows, emuRow{ts: base + i*int64(37*time.Minute), stream: `{app="uw",pod="a"}`, value: float64(i % 7)})
	}
	for _, query := range []string{
		`sum by (pod) (sum_over_time({app="uw"} | json | unwrap amount [1m]))`,
		`max by (pod) (max_over_time({app="uw"} | logfmt | unwrap amount [1m]))`,
		`sum by (pod) (sum_over_time({app="uw"} | json | unwrap amount [5m]))`,
		`sum by (pod) (sum_over_time({app="uw"} | logfmt | unwrap amount [5m]))`,
	} {
		t.Run(query, func(t *testing.T) {
			emu := &vlEmulator{t: t, rows: rows, field: "amount", version: "v1.53.0"}
			c := owCase{shape: owShape{query: func(string) string { return query }}, step: 10 * time.Minute, start: base, end: base + int64(7*24*time.Hour)}
			runOverTimeCase(t, emu, c, 0)
			calls := emu.endpointCalls("/select/logsql/stats_query_range")
			if len(calls) != 1 || !strings.Contains(calls[0], "__lvp_window_phase") || strings.Contains(calls[0], "__sample_count") {
				t.Fatalf("%s: want one phase-filtered step bucket query, got %d: %v", query, len(calls), calls)
			}
			for _, raw := range emu.endpointCalls("/select/logsql/query") {
				// The conversion check's lookup of the first window (at most
				// three flagged rows) runs beside a heavy read; no metric rows.
				if !strings.HasSuffix(raw, ` | filter __lvp_bad:="1" | limit 3`) {
					t.Fatalf("%s: read raw rows: %s", query, raw)
				}
			}
		})
	}
}

// TestUnwrapOverTimeRangeWindowsNaN: a nan value is a sample for Loki
// (strconv.ParseFloat reads it): sum and avg over a window that holds one are
// NaN (pkg/logql/range_vector.go:371-404), max and min ignore it unless every
// sample is one (range_vector.go:405-420). With a range longer than the step the
// nan group of the counting gate (flag 2) reaches the merged windows: the five
// steps whose window holds the nan bucket are NaN for sum and avg, and max
// keeps the clean value. The fake answers the bucket queries of the window merge.
//
// conformance: semantics/unwrap-over-time-range-windows, semantics/unwrap-gate-parsefloat-divergences
func TestUnwrapOverTimeRangeWindowsNaN(t *testing.T) {
	at := time.Unix(1700000040, 0)
	nanAt := at.Add(5*time.Minute + 20*time.Second)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		query := r.Form.Get("query")
		if r.URL.Path != "/select/logsql/stats_query_range" {
			_, _ = w.Write([]byte(`{"status":"success","data":{"resultType":"matrix","result":[]}}`))
			return
		}
		start, _ := parseFlexibleUnixNanos(r.Form.Get("start"))
		end, _ := parseFlexibleUnixNanos(r.Form.Get("end"))
		step := int64(parseVLStepParam(r.Form.Get("step")))
		offset, _ := time.ParseDuration(r.Form.Get("offset"))
		name := "c"
		if strings.Contains(query, "as __lvp_sum") {
			name = "__lvp_sum"
		}
		var values, counts, nanValues, nanCounts []string
		for k := floorDiv(start+int64(offset), step); k*step-int64(offset) < end; k++ {
			edge := k*step - int64(offset)
			ts := strconv.FormatFloat(float64(edge)/1e9, 'f', -1, 64)
			values = append(values, `[`+ts+`,"1"]`)
			counts = append(counts, `[`+ts+`,"1"]`)
			if nanAt.UnixNano() >= edge && nanAt.UnixNano() < edge+step {
				nanValues = append(nanValues, `[`+ts+`,"0"]`)
				nanCounts = append(nanCounts, `[`+ts+`,"1"]`)
			}
		}
		series := func(name, flag string, points []string) string {
			return `{"metric":{"__name__":"` + name + `","app":"x","__lvp_bad":"` + flag + `"},"values":[` + strings.Join(points, ",") + `]}`
		}
		withCounts := strings.Contains(query, "__sample_count")
		result := []string{series(name, "0", values)}
		if withCounts {
			result = append(result, series("__sample_count", "0", counts))
		}
		if strings.Contains(query, `:="2"`) && len(nanValues) > 0 {
			result = append(result, series(name, "2", nanValues))
			if withCounts {
				result = append(result, series("__sample_count", "2", nanCounts))
			}
		}
		_, _ = w.Write([]byte(`{"status":"success","data":{"resultType":"matrix","result":[` + strings.Join(result, ",") + `]}}`))
	}))
	t.Cleanup(srv.Close)
	for _, tc := range []struct {
		fn    string
		nans  int
		outer string
	}{{"sum_over_time", 5, "sum"}, {"avg_over_time", 5, "sum"}, {"max_over_time", 0, "sum"}, {"min_over_time", 0, "sum"}, {"max_over_time", 0, "max"}, {"min_over_time", 0, "min"}} {
		for _, post := range []string{"", ` | __error__=""`} {
			query := fmt.Sprintf(`%s by (app) (%s({app="x"} | logfmt | unwrap b%s [5m]))`, tc.outer, tc.fn, post)
			t.Run(query, func(t *testing.T) {
				p := newSlidingTestProxy(t, srv.URL)
				params := url.Values{"query": {query}, "start": {strconv.FormatInt(at.UnixNano(), 10)}, "end": {strconv.FormatInt(at.Add(12*time.Minute).UnixNano(), 10)}, "step": {"60"}}
				rec := httptest.NewRecorder()
				p.handleQueryRange(rec, httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+params.Encode(), nil))
				if rec.Code != http.StatusOK {
					t.Fatalf("%d %s", rec.Code, rec.Body)
				}
				if got := strings.Count(rec.Body.String(), `"NaN"`); got != tc.nans {
					t.Fatalf("%d NaN samples, want %d (the windows (t-5m, t] holding the nan line): %s", got, tc.nans, rec.Body)
				}
				if tc.nans == 0 && !strings.Contains(rec.Body.String(), `"1"]`) {
					t.Fatalf("max and min keep the clean samples: %s", rec.Body)
				}
			})
		}
	}
}

// TestDropKeptFlagMatchesReplacements: the one-pass removal of the kept
// conversion flag gives what the three replacements it stands for give
// (`,kept`, then `kept,`, then `kept`), over generated answers with the flag
// first, last, alone and between other labels.
//
// conformance: semantics/unwrap-conversion-error
func TestDropKeptFlagMatchesReplacements(t *testing.T) {
	kept := []byte(`"__lvp_bad":"0"`)
	parts := []string{`{`, `}`, `,`, `"app":"x"`, string(kept), `"__name__":"c"`, `[1,"2"]`, `"values":`, `"__lvp_bad":"1"`}
	rnd := rand.New(rand.NewSource(17))
	for i := 0; i < 5000; i++ {
		var b strings.Builder
		for n := rnd.Intn(12); n >= 0; n-- {
			b.WriteString(parts[rnd.Intn(len(parts))])
		}
		body := b.String()
		want := bytes.ReplaceAll([]byte(body), append([]byte(","), kept...), nil)
		want = bytes.ReplaceAll(want, append(append([]byte(nil), kept...), ','), nil)
		want = bytes.ReplaceAll(want, kept, nil)
		if got := dropKeptFlag([]byte(body), kept); !bytes.Equal(got, want) {
			t.Fatalf("%s: got %s, want %s", body, got, want)
		}
	}
}
