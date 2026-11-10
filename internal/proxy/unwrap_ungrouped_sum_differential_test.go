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
	"slices"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/testutil/vlemu"
)

// This file pins the answer of an ungrouped sum over a range function with
// unwrap to Loki's, over generated rows and generated windows, with a
// reference evaluator written apart from the proxy: Loki v3.7.7
// pkg/logql/evaluator.go VectorAggEvaluator.Next (a grouping of no labels folds
// every series of its input), pkg/logql/range_vector.go (rate over an unwrapped
// label is the sum of the values per second, sum_over_time their sum) over the
// window (t-range, t], and pkg/logql/log/metrics_extraction.go (a series is the
// stream plus the labels a parser extracted, less the unwrapped one).
//
// The proxy under test is the real handler. Only VictoriaLogs is simulated, by
// vlemu.Emulator: the stats_query_range and stats_query endpoints over the three
// stats pipes the translation uses (stats, math <field>/<number>, stats), with
// VictoriaLogs' bucket semantics, and the raw /select/logsql/query rows.

// lokiUnwrapSum is the reference: for every evaluation time t, the windows
// (t-window, t] of the series (stream plus parsed labels), the range function
// over each series' values, and the ungrouped sum over the series that have a
// sample. A time with no sample in any series has no point.
func lokiUnwrapSum(rows []vlemu.Row, fn string, window int64, times []int64) map[int64]float64 {
	out := map[int64]float64{}
	for _, t := range times {
		perSeries := map[string][]float64{}
		for _, row := range rows {
			if row.TS <= t-window || row.TS > t {
				continue
			}
			labels := make([]string, 0, len(row.Labels))
			for k, v := range row.Labels {
				labels = append(labels, k+"="+v)
			}
			sort.Strings(labels)
			key := row.Stream + "|" + strings.Join(labels, ",")
			perSeries[key] = append(perSeries[key], row.Value)
		}
		if len(perSeries) == 0 {
			continue
		}
		var total float64
		for _, values := range perSeries {
			var sum float64
			for _, v := range values {
				sum += v
			}
			switch fn {
			case "rate":
				total += sum / (float64(window) / 1e9)
			case "sum_over_time":
				total += sum
			case "avg_over_time":
				total += sum / float64(len(values))
			case "max_over_time":
				total += slices.Max(values)
			case "min_over_time":
				total += slices.Min(values)
			default:
				panic("reference: " + fn)
			}
		}
		out[t] = total
	}
	return out
}

type uwScenario struct {
	fn, parser string
	window     time.Duration
	step       time.Duration // 0: instant
	start, end int64         // ns, step-aligned evaluation times
	rows       []vlemu.Row
	wrap       func(string) string // wraps the query, e.g. in label_replace
	seriesCap  int                 // -max-stats-query-series; 0: the default
}

func (s uwScenario) times() []int64 {
	if s.step == 0 {
		return []int64{s.end}
	}
	var out []int64
	for t := s.start; t <= s.end; t += int64(s.step) {
		out = append(out, t)
	}
	return out
}

func (s uwScenario) query() string {
	pipeline := ""
	if s.parser != "" {
		pipeline = " | " + s.parser
	}
	q := fmt.Sprintf(`sum(%s({app="uw"}%s | unwrap amount [%s]))`, s.fn, pipeline, uwWindowText(s.window))
	if s.wrap != nil {
		q = s.wrap(q)
	}
	return q
}

func uwWindowText(d time.Duration) string {
	if d%time.Second != 0 {
		return strconv.Itoa(int(d/time.Millisecond)) + "ms"
	}
	if d%time.Minute == 0 {
		return strconv.Itoa(int(d/time.Minute)) + "m"
	}
	return strconv.Itoa(int(d/time.Second)) + "s"
}

// forceRawWindow makes the window one millisecond longer than the step, so the
// gcd(step, window) bucket grid is 1 ms wide and holds more than
// heavyQueryBucketThreshold buckets: the raw-sample evaluator answers, whatever
// the backend version. Instant scenarios have no grid and are left alone.
func (s uwScenario) forceRawWindow() uwScenario {
	if s.step > 0 {
		s.window = s.step + time.Millisecond
	}
	return s
}

func generateUnwrapScenario(rnd *rand.Rand, fn, parser string, instant bool) uwScenario {
	steps := []time.Duration{30 * time.Second, time.Minute, 90 * time.Second}
	windows := []time.Duration{30 * time.Second, time.Minute, 90 * time.Second, 5 * time.Minute, 150 * time.Second}
	s := uwScenario{fn: fn, parser: parser, window: windows[rnd.Intn(len(windows))]}
	base := int64(1_800_000_000) * 1e9
	if !instant {
		s.step = steps[rnd.Intn(len(steps))]
		if rnd.Intn(3) == 0 {
			s.window = s.step // a tumbling window
		}
	}
	// Evaluation times on whole seconds, as Grafana sends them.
	s.start = base + int64(rnd.Intn(600))*1e9
	if instant {
		s.end = s.start
	} else {
		s.end = s.start + int64(rnd.Intn(8))*int64(s.step)
	}
	streams := []string{`{app="uw",pod="a"}`, `{app="uw",pod="b"}`, `{app="uw",pod="c"}`}
	labelSets := []map[string]string{nil, {"route": "/x"}, {"route": "/y"}}
	values := []float64{0, 0.5, 1, 2.25, 3, 100.125, 7}
	lo, hi := s.start-int64(s.window)-5e9, s.end+5e9
	n := 5 + rnd.Intn(60)
	for i := 0; i < n; i++ {
		ts := lo + rnd.Int63n(hi-lo)
		if rnd.Intn(4) == 0 { // on a window edge
			edge := s.start + int64(rnd.Intn(8))*int64(max(s.step, time.Second))
			if rnd.Intn(2) == 0 {
				edge -= int64(s.window)
			}
			ts = edge + int64(rnd.Intn(3)-1)
		}
		row := vlemu.Row{TS: ts, Stream: streams[rnd.Intn(len(streams))], Value: values[rnd.Intn(len(values))]}
		if s.parser != "" {
			row.Labels = labelSets[rnd.Intn(len(labelSets))]
		}
		s.rows = append(s.rows, row)
	}
	return s
}

// runUnwrapScenario sends the scenario through the real handler against the
// emulated VictoriaLogs and returns timestamp -> value of the one series.
func runUnwrapScenario(t *testing.T, emu *vlemu.Emulator, s uwScenario) (map[int64]float64, int) {
	t.Helper()
	backend := httptest.NewServer(emu.Handler())
	defer backend.Close()
	p, err := New(Config{BackendURL: backend.URL, Cache: cache.New(60*time.Second, 1000), LogLevel: "error", BackendMinVersion: "v1.40.0", BackendVersionCheckTimeout: time.Second, MaxStatsQuerySeries: s.seriesCap})
	if err != nil {
		t.Fatal(err)
	}
	if emu.Version != "" {
		if err := p.ValidateBackendVersionCompatibility(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	query := s.query()
	params := url.Values{"query": {query}}
	target := "/loki/api/v1/query_range?"
	if s.step == 0 {
		target = "/loki/api/v1/query?"
		params.Set("time", strconv.FormatInt(s.end/1e9, 10))
	} else {
		params.Set("start", strconv.FormatInt(s.start/1e9, 10))
		params.Set("end", strconv.FormatInt(s.end/1e9, 10))
		params.Set("step", strconv.FormatInt(int64(s.step/time.Second), 10))
	}
	res := doCompatProxyRequest(p, target+params.Encode(), map[string]string{"X-Scope-OrgID": "0"})
	if res.Code != http.StatusOK {
		t.Fatalf("%s: %d %s", query, res.Code, res.Body)
	}
	var resp struct {
		Data struct {
			Result []struct {
				Metric map[string]string `json:"metric"`
				Values [][]any           `json:"values"`
				Value  []any             `json:"value"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(res.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	got := map[int64]float64{}
	for _, series := range resp.Data.Result {
		if len(series.Metric) != 0 {
			t.Fatalf("%s: the series carries labels %v", query, series.Metric)
		}
		points := series.Values
		if series.Value != nil {
			points = [][]any{series.Value}
		}
		for _, point := range points {
			v, _ := strconv.ParseFloat(point[1].(string), 64)
			got[int64(point[0].(float64))*1e9] = v
		}
	}
	return got, len(resp.Data.Result)
}

func compareUnwrapSum(t *testing.T, label string, s uwScenario, got, want map[int64]float64, series int) {
	t.Helper()
	if series > 1 {
		t.Fatalf("%s: %d series, want one\nquery %s", label, series, s.query())
	}
	for ts, w := range want {
		g, ok := got[ts]
		if !ok {
			t.Fatalf("%s: no point at %d, Loki has %g\nquery %s step=%v window=%v rows=%d", label, ts/1e9, w, s.query(), s.step, s.window, len(s.rows))
		}
		if math.Abs(g-w) > 1e-9*math.Max(1, math.Abs(w)) {
			t.Fatalf("%s: at %d got %g, Loki has %g\nquery %s step=%v window=%v rows=%d", label, ts/1e9, g, w, s.query(), s.step, s.window, len(s.rows))
		}
	}
	for ts, g := range got {
		if _, ok := want[ts]; !ok {
			t.Fatalf("%s: extra point at %d = %g, Loki has none\nquery %s step=%v window=%v rows=%d", label, ts/1e9, g, s.query(), s.step, s.window, len(s.rows))
		}
	}
}

// TestUngroupedSumUnwrapDifferential: over generated rows (several streams,
// parsed labels, zero values, lines on window edges) and generated windows
// (tumbling, sliding, shorter and longer than the step, a range that is not a
// multiple of the step, instant), the proxy's answer to an ungrouped
// `sum(rate(... | unwrap amount [r]))` is the one series Loki's vector aggregation
// makes of the per-series values, at every step, on every VictoriaLogs line the
// proxy supports (v1.49.0 and v1.52.0) and on an unknown version, which behaves
// as the floor. The answer comes from stats on buckets anchored with the offset
// argument; no raw rows are read. With raw=true a 1 ms bucket grid (see
// forceRawWindow) sends a query_range through the raw-sample evaluator, which
// must read the same (t-range, t] window (isLogRangeWindowFunc covers
// unwrap_rate) and call /select/logsql/query.
//
// conformance: semantics/unwrap-rate-ungrouped-sum-per-stream, metric-series-identity, window-bounds-and-step-alignment
func TestUngroupedSumUnwrapDifferential(t *testing.T) {
	for _, version := range []string{"", "v1.49.0", "v1.52.0"} {
		for _, parser := range []string{"", "logfmt", "json"} {
			for _, instant := range []bool{false, true} {
				for _, raw := range []bool{false, true} {
					if raw && instant {
						continue
					}
					name := fmt.Sprintf("version=%s/parser=%s/instant=%v/raw=%v", version, parser, instant, raw)
					t.Run(name, func(t *testing.T) {
						rnd := rand.New(rand.NewSource(int64(len(name))*7919 + 17))
						for i := 0; i < 25; i++ {
							s := generateUnwrapScenario(rnd, "rate", parser, instant)
							if raw {
								s = s.forceRawWindow()
							}
							emu := &vlemu.Emulator{T: t, Rows: s.rows, Field: "amount", Version: version}
							got, series := runUnwrapScenario(t, emu, s)
							want := lokiUnwrapSum(s.rows, "rate", int64(s.window), s.times())
							compareUnwrapSum(t, fmt.Sprintf("%s #%d", name, i), s, got, want, series)
							rawRows := emu.EndpointCalls("/select/logsql/query")
							switch {
							case raw && len(rawRows) == 0:
								t.Fatalf("%s #%d: expected the raw-sample evaluator, got stats calls only", name, i)
							case raw:
							case len(rawRows) > 0:
								t.Fatalf("%s #%d: the metric read raw rows: %s", name, i, rawRows[0])
							case emu.Widest > 1:
								// One series from VictoriaLogs, not one per stream for the proxy to add up.
								t.Fatalf("%s #%d: VictoriaLogs answered %d series for an ungrouped sum", name, i, emu.Widest)
							}
						}
					})
				}
			}
		}
	}
}

// TestUngroupedSumUnwrapPerSeriesFunctions: a function that takes a value per
// series (avg, max, min over time) sums those values over the series, at an
// instant, and the answer is the one series Loki's vector aggregation makes of
// them, whichever route answers. Every line is a series of its own here (a
// unique parsed label), which the stats route reads from `_msg` and matches
// Loki's values. The raw-row route (`| json`) keeps a series per stream, not per
// stream and parsed labels, so only its series count is pinned; the values are
// registered apart as semantics/unwrap-per-series-identity-raw-rows.
//
// conformance: semantics/unwrap-rate-ungrouped-sum-per-stream, metric-series-identity
func TestUngroupedSumUnwrapPerSeriesFunctions(t *testing.T) {
	for _, parser := range []string{"logfmt", "json"} {
		for _, fn := range []string{"avg_over_time", "max_over_time", "min_over_time"} {
			t.Run(parser+"/"+fn, func(t *testing.T) {
				rnd := rand.New(rand.NewSource(int64(len(parser+fn)) * 104729))
				for i := 0; i < 20; i++ {
					s := generateUnwrapScenario(rnd, fn, parser, true)
					for j := range s.rows {
						s.rows[j].Labels = map[string]string{"id": strconv.Itoa(j)}
						s.rows[j].Msg = fmt.Sprintf("amount=%v id=%d", s.rows[j].Value, j)
					}
					emu := &vlemu.Emulator{T: t, Rows: s.rows, Field: "amount"}
					got, series := runUnwrapScenario(t, emu, s)
					want := lokiUnwrapSum(s.rows, fn, int64(s.window), s.times())
					if parser == "json" {
						if series > 1 || (len(want) == 1 && len(got) != 1) {
							t.Fatalf("%s/%s #%d: %d series with %d points, want one series with Loki's %d point(s)", parser, fn, i, series, len(got), len(want))
						}
						continue
					}
					compareUnwrapSum(t, fmt.Sprintf("%s/%s #%d", parser, fn, i), s, got, want, series)
				}
			})
		}
	}
}

// A `label_replace` around the sum, the form Grafana's transformations and
// dashboards add, changes nothing about the fold: one series, Loki's values.
//
// conformance: semantics/unwrap-rate-ungrouped-sum-per-stream, metric-series-identity
func TestUngroupedSumUnwrapUnderLabelReplace(t *testing.T) {
	wrap := func(q string) string { return `label_replace(` + q + `, "dst", "$1", "src", "(.*)")` }
	for _, parser := range []string{"logfmt", "json"} {
		for _, instant := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/instant=%v", parser, instant), func(t *testing.T) {
				rnd := rand.New(rand.NewSource(int64(len(parser)) * 31337))
				for i := 0; i < 15; i++ {
					s := generateUnwrapScenario(rnd, "rate", parser, instant)
					s.wrap = wrap
					emu := &vlemu.Emulator{T: t, Rows: s.rows, Field: "amount"}
					got, series := runUnwrapScenario(t, emu, s)
					compareUnwrapSum(t, fmt.Sprintf("label_replace %s #%d", parser, i), s, got, lokiUnwrapSum(s.rows, "rate", int64(s.window), s.times()), series)
				}
			})
		}
	}
}

// Loki counts max_query_series on the answer, so a sum over more streams than
// the limit is still one series when the function is taken per series and the
// proxy adds the streams up itself (rate_counter, or avg over a JSON stream).
//
// conformance: semantics/unwrap-rate-ungrouped-sum-per-stream, metric-series-identity
func TestUngroupedSumUnwrapPerSeriesFoldIgnoresSeriesLimit(t *testing.T) {
	for _, tc := range []struct {
		fn      string
		instant bool
	}{{"rate_counter", false}, {"rate_counter", true}, {"avg_over_time", true}} {
		t.Run(fmt.Sprintf("%s/instant=%v", tc.fn, tc.instant), func(t *testing.T) {
			rnd := rand.New(rand.NewSource(99))
			s := generateUnwrapScenario(rnd, tc.fn, "json", tc.instant)
			s.seriesCap = 2
			s.rows = s.rows[:0]
			base := s.end - int64(s.window)/2
			for i := 0; i < 12; i++ { // twelve streams, six times the limit
				s.rows = append(s.rows, vlemu.Row{TS: base - int64(i)*1e9, Stream: fmt.Sprintf(`{app="uw",pod="p%d"}`, i), Value: float64(i + 1)})
				s.rows = append(s.rows, vlemu.Row{TS: base - int64(i)*1e9 + 1e8, Stream: fmt.Sprintf(`{app="uw",pod="p%d"}`, i), Value: float64(i + 2)})
			}
			emu := &vlemu.Emulator{T: t, Rows: s.rows, Field: "amount"}
			got, series := runUnwrapScenario(t, emu, s)
			if series != 1 || len(got) == 0 {
				t.Fatalf("%d series, %d points; want one series (Loki folds the streams before its limit)", series, len(got))
			}
		})
	}
}

// A grouped sum of a rate over an unwrapped label reads Loki's window
// (t-range, t]: a line on the lower edge belongs to the previous window, the
// line at t to this one. The raw-sample evaluator answers (a 1 ms bucket grid
// is over heavyQueryBucketThreshold, whatever the backend version) and must
// have read /select/logsql/query.
//
// conformance: semantics/unwrap-range-last-point, window-bounds-and-step-alignment
func TestGroupedSumUnwrapRateRawWindow(t *testing.T) {
	rnd := rand.New(rand.NewSource(424242))
	for i := 0; i < 30; i++ {
		s := generateUnwrapScenario(rnd, "rate", "", false).forceRawWindow()
		emu := &vlemu.Emulator{T: t, Rows: s.rows, Field: "amount", Version: "v1.49.0"}
		backend := httptest.NewServer(emu.Handler())
		p, err := New(Config{BackendURL: backend.URL, Cache: cache.New(60*time.Second, 1000), LogLevel: "error", BackendMinVersion: "v1.40.0", BackendVersionCheckTimeout: time.Second})
		if err != nil {
			t.Fatal(err)
		}
		if err := p.ValidateBackendVersionCompatibility(context.Background()); err != nil {
			t.Fatal(err)
		}
		query := fmt.Sprintf(`sum by (pod) (rate({app="uw"} | unwrap amount [%s]))`, uwWindowText(s.window))
		params := url.Values{"query": {query}, "start": {strconv.FormatInt(s.start/1e9, 10)}, "end": {strconv.FormatInt(s.end/1e9, 10)}, "step": {strconv.FormatInt(int64(s.step/time.Second), 10)}}
		res := doCompatProxyRequest(p, "/loki/api/v1/query_range?"+params.Encode(), map[string]string{"X-Scope-OrgID": "0"})
		backend.Close()
		if res.Code != http.StatusOK {
			t.Fatalf("%s: %d %s", query, res.Code, res.Body)
		}
		if len(emu.EndpointCalls("/select/logsql/query")) == 0 {
			t.Fatalf("%s: expected the raw-sample evaluator, got stats calls only", query)
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
			pts := map[int64]float64{}
			for _, point := range series.Values {
				v, _ := strconv.ParseFloat(point[1].(string), 64)
				pts[int64(point[0].(float64))*1e9] = v
			}
			got[series.Metric["pod"]] = pts
		}
		for _, pod := range []string{"a", "b", "c"} {
			var rows []vlemu.Row
			for _, row := range s.rows {
				if row.Stream == fmt.Sprintf(`{app="uw",pod=%q}`, pod) {
					rows = append(rows, row)
				}
			}
			want := lokiUnwrapSum(rows, "rate", int64(s.window), s.times())
			if len(want) == 0 && len(got[pod]) == 0 {
				continue
			}
			compareUnwrapSum(t, fmt.Sprintf("grouped #%d pod=%s", i, pod), s, got[pod], want, 1)
		}
	}
}
