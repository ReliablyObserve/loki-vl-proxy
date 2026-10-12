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
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// This file pins an aggregation over first_over_time and last_over_time with
// unwrap to Loki's, over generated rows and windows, with a reference written
// apart from the proxy: Loki v3.7.7 pkg/logql/range_vector.go (FirstOverTime and
// LastOverTime take the first and the last sample, in timestamp order, of each
// series' window (t-range, t]; first at :818-833, last at :835-843) and
// pkg/logql/evaluator.go VectorAggEvaluator.Next (the aggregation folds the
// per-series values of a step). VictoriaLogs has no first or last stats function,
// so the emulator (vlEmulator) answers only row_min(_time, f) and row_max(_time,
// f), as a JSON row that is a label to VictoriaLogs, and refuses a query whose
// last stats pipe holds only those.

// firstLastRef is the reference for the aggregation agg (sum, avg, min, max,
// count; by is the stream label to group by, "" for none) over fn at time t:
// group key -> value.
func firstLastRef(rows []emuRow, fn, agg, by string, window, t int64) map[string]float64 {
	type sample struct {
		ts    int64
		value float64
	}
	series := map[string][]sample{}
	groupOf := map[string]string{}
	for _, row := range rows {
		if row.ts <= t-window || row.ts > t {
			continue
		}
		labels := make([]string, 0, len(row.labels))
		for k, v := range row.labels {
			labels = append(labels, k+"="+v)
		}
		sort.Strings(labels)
		key := row.stream + "|" + strings.Join(labels, ",")
		series[key] = append(series[key], sample{row.ts, row.value})
		if by != "" {
			m := regexp.MustCompile(by + `="([^"]*)"`).FindStringSubmatch(row.stream)
			groupOf[key] = m[1]
		}
	}
	groups := map[string][]float64{}
	for key, samples := range series {
		sort.SliceStable(samples, func(i, j int) bool { return samples[i].ts < samples[j].ts })
		v := samples[0].value
		if fn == "last_over_time" {
			v = samples[len(samples)-1].value
		}
		groups[groupOf[key]] = append(groups[groupOf[key]], v)
	}
	out := map[string]float64{}
	for g, values := range groups {
		var sum float64
		for _, v := range values {
			sum += v
		}
		switch agg {
		case "sum":
			out[g] = sum
		case "avg":
			out[g] = sum / float64(len(values))
		case "min":
			out[g] = slices.Min(values)
		case "max":
			out[g] = slices.Max(values)
		case "count":
			out[g] = float64(len(values))
		default:
			panic("reference: " + agg)
		}
	}
	return out
}

// firstLastRows gives every line of a parsed stream a series of its own (a
// unique parsed label and a unique line), which is how the stats route reads a
// series (`_stream, _msg`: unwrap-per-series-identity-raw-rows tracks the
// approximation), and keeps a series' timestamps distinct (Loki and
// VictoriaLogs may order equal timestamps differently).
func firstLastRows(s uwScenario) []emuRow {
	rows := s.rows
	seen := map[string]map[int64]bool{}
	var out []emuRow
	for i, row := range rows {
		series := row.stream
		if s.parser != "" {
			row.labels = map[string]string{"id": strconv.Itoa(i)}
			row.msg = fmt.Sprintf("amount=%v id=%d", row.value, i)
			series = row.stream + "|" + row.labels["id"]
		}
		if seen[series] == nil {
			seen[series] = map[int64]bool{}
		}
		if seen[series][row.ts] {
			continue
		}
		seen[series][row.ts] = true
		out = append(out, row)
	}
	return out
}

type firstLastRun struct {
	code    int
	series  []map[string]string // the metric of each series
	points  []map[int64]float64 // the points of each series
	backend *vlEmulator
}

func runFirstLast(t *testing.T, emu *vlEmulator, query string, instant bool, start, end int64, step time.Duration) firstLastRun {
	t.Helper()
	backend := httptest.NewServer(emu.handler())
	defer backend.Close()
	p, err := New(Config{BackendURL: backend.URL, Cache: cache.New(60*time.Second, 1000), LogLevel: "error", BackendMinVersion: "v1.40.0", BackendVersionCheckTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if emu.version != "" {
		if err := p.ValidateBackendVersionCompatibility(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	params := url.Values{"query": {query}}
	target := "/loki/api/v1/query_range?"
	if instant {
		target = "/loki/api/v1/query?"
		params.Set("time", strconv.FormatInt(end/1e9, 10))
	} else {
		params.Set("start", strconv.FormatInt(start/1e9, 10))
		params.Set("end", strconv.FormatInt(end/1e9, 10))
		params.Set("step", strconv.FormatInt(int64(step/time.Second), 10))
	}
	res := doCompatProxyRequest(p, target+params.Encode(), map[string]string{"X-Scope-OrgID": "0"})
	run := firstLastRun{code: res.Code, backend: emu}
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
	for _, series := range resp.Data.Result {
		points := series.Values
		if series.Value != nil {
			points = [][]any{series.Value}
		}
		pts := map[int64]float64{}
		for _, point := range points {
			v, _ := strconv.ParseFloat(point[1].(string), 64)
			pts[int64(point[0].(float64))*1e9] = v
		}
		run.series = append(run.series, series.Metric)
		run.points = append(run.points, pts)
	}
	return run
}

// noFirstLastStatsFunction fails when the proxy sent VictoriaLogs a stats
// function it has not got, or read raw rows for a metric.
func noFirstLastStatsFunction(t *testing.T, label string, emu *vlEmulator) {
	t.Helper()
	for _, path := range []string{"/select/logsql/stats_query_range", "/select/logsql/stats_query"} {
		for _, q := range emu.endpointCalls(path) {
			if regexp.MustCompile(`\b(?:first|last)\(`).MatchString(q) {
				t.Fatalf("%s: VictoriaLogs has no first or last stats function: %s", label, q)
			}
		}
	}
	if rows := emu.endpointCalls("/select/logsql/query"); len(rows) > 0 {
		t.Fatalf("%s: the metric read raw rows: %s", label, rows[0])
	}
}

// TestFirstLastOverTimeUnderAggregationInstantDifferential: at an instant, sum,
// avg, min, max and count over first_over_time and last_over_time with unwrap
// are Loki's value, ungrouped (one series with no label) and grouped by a stream
// label, over every parser and VictoriaLogs line, from stats alone.
//
// conformance: semantics/unwrap-first-last-under-aggregation, metric-series-identity
func TestFirstLastOverTimeUnderAggregationInstantDifferential(t *testing.T) {
	for _, version := range []string{"", "v1.52.0", "v1.40.0"} {
		for _, parser := range []string{"", "logfmt", "json"} {
			for _, fn := range []string{"first_over_time", "last_over_time"} {
				for _, agg := range []string{"sum", "avg", "min", "max", "count"} {
					for _, by := range []string{"", "pod"} {
						name := fmt.Sprintf("version=%s/parser=%s/%s/%s/by=%s", version, parser, fn, agg, by)
						t.Run(name, func(t *testing.T) {
							rnd := rand.New(rand.NewSource(int64(len(name))*7919 + 3))
							for i := 0; i < 8; i++ {
								s := generateUnwrapScenario(rnd, fn, parser, true)
								s.rows = firstLastRows(s)
								query := fmt.Sprintf(`%s(%s({app="uw"}%s | unwrap amount [%s]))`, agg, fn, parserPipe(parser), uwWindowText(s.window))
								if by != "" {
									query = fmt.Sprintf(`%s by (%s) (%s({app="uw"}%s | unwrap amount [%s]))`, agg, by, fn, parserPipe(parser), uwWindowText(s.window))
								}
								emu := &vlEmulator{t: t, rows: s.rows, field: "amount", version: version}
								run := runFirstLast(t, emu, query, true, s.end, s.end, 0)
								want := firstLastRef(s.rows, fn, agg, by, int64(s.window), s.end)
								label := fmt.Sprintf("%s #%d %s", name, i, query)
								noFirstLastStatsFunction(t, label, emu)
								compareFirstLast(t, label, run, want, by, s.end)
							}
						})
					}
				}
			}
		}
	}
}

func parserPipe(parser string) string {
	if parser == "" {
		return ""
	}
	return " | " + parser
}

// compareFirstLast compares the instant answer with the reference: one series
// per group (the group's label, or none), Loki's value at the evaluation time.
func compareFirstLast(t *testing.T, label string, run firstLastRun, want map[string]float64, by string, at int64) {
	t.Helper()
	got := map[string]float64{}
	for i, metric := range run.series {
		key := ""
		if by != "" {
			key = metric[by]
		} else if len(metric) != 0 {
			t.Fatalf("%s: the series carries labels %v", label, metric)
		}
		if _, dup := got[key]; dup {
			t.Fatalf("%s: two series for group %q", label, key)
		}
		for _, v := range run.points[i] {
			got[key] = v
		}
	}
	if len(got) != len(want) {
		t.Fatalf("%s: %d series, Loki has %d (%v vs %v)", label, len(got), len(want), got, want)
	}
	for key, w := range want {
		g, ok := got[key]
		if !ok || math.Abs(g-w) > 1e-9*math.Max(1, math.Abs(w)) {
			t.Fatalf("%s: group %q got %v (present %v), Loki has %v", label, key, g, ok, w)
		}
	}
	_ = at
}

// TestFirstLastOverTimeUnderAggregationRange: on query_range the same queries
// answer 200 from stats alone, with the series Loki makes of them (one series
// without labels for an ungrouped aggregation, one per group for `by (pod)`),
// and every point is a value the aggregation of first or last samples can
// have. The window each point reads is the step-sized bucket native
// stats_query_range gives, as for avg, max and min over time
// (semantics/unwrap-over-time-range-windows tracks Loki's (t-range, t] window),
// so the values are not compared with Loki's here.
//
// conformance: semantics/unwrap-first-last-under-aggregation, metric-series-identity
func TestFirstLastOverTimeUnderAggregationRange(t *testing.T) {
	answered := 0
	for _, version := range []string{"", "v1.52.0", "v1.40.0"} {
		for _, parser := range []string{"", "logfmt", "json"} {
			for _, fn := range []string{"first_over_time", "last_over_time"} {
				for _, agg := range []string{"sum", "avg", "min", "max", "count"} {
					for _, by := range []string{"", "pod"} {
						name := fmt.Sprintf("version=%s/parser=%s/%s/%s/by=%s", version, parser, fn, agg, by)
						t.Run(name, func(t *testing.T) {
							rnd := rand.New(rand.NewSource(int64(len(name))*104729 + 5))
							for i := 0; i < 4; i++ {
								s := generateUnwrapScenario(rnd, fn, parser, false)
								s.rows = firstLastRows(s)
								query := fmt.Sprintf(`%s(%s({app="uw"}%s | unwrap amount [%s]))`, agg, fn, parserPipe(parser), uwWindowText(s.window))
								if by != "" {
									query = fmt.Sprintf(`%s by (%s) (%s({app="uw"}%s | unwrap amount [%s]))`, agg, by, fn, parserPipe(parser), uwWindowText(s.window))
								}
								emu := &vlEmulator{t: t, rows: s.rows, field: "amount", version: version}
								run := runFirstLast(t, emu, query, false, s.start, s.end, s.step)
								label := fmt.Sprintf("%s #%d %s", name, i, query)
								noFirstLastStatsFunction(t, label, emu)
								values := map[float64]bool{}
								for _, row := range s.rows {
									values[row.value] = true
								}
								pods := map[string]bool{}
								for i, metric := range run.series {
									if by == "" && len(metric) != 0 {
										t.Fatalf("%s: the series carries labels %v", label, metric)
									}
									if by != "" {
										if !slices.Contains([]string{"a", "b", "c"}, metric["pod"]) || pods[metric["pod"]] {
											t.Fatalf("%s: unexpected or repeated group %v", label, metric)
										}
										pods[metric["pod"]] = true
									}
									for ts, v := range run.points[i] {
										if (agg == "min" || agg == "max") && !values[v] {
											t.Fatalf("%s: %v at %d is not a sample value", label, v, ts/1e9)
										}
										if agg == "count" && (v < 1 || v != math.Trunc(v)) {
											t.Fatalf("%s: count %v at %d", label, v, ts/1e9)
										}
									}
								}
								if by == "" && len(run.series) > 1 {
									t.Fatalf("%s: %d series, Loki folds the series into one", label, len(run.series))
								}
								if len(run.series) > 0 {
									answered++
								}
							}
						})
					}
				}
			}
		}
	}
	if answered == 0 {
		t.Fatal("no scenario produced a series: the test checks nothing")
	}
}

// TestFirstLastOverTimeTopkBottomkInstantDifferential: topk and bottomk rank the
// per-series first or last values: the proxy asks VictoriaLogs for one value
// per series (row_min or row_max, unpacked) and keeps the k best, so the values
// returned are Loki's k best series values.
//
// conformance: semantics/unwrap-first-last-under-aggregation
func TestFirstLastOverTimeTopkBottomkInstantDifferential(t *testing.T) {
	for _, parser := range []string{"", "logfmt", "json"} {
		for _, fn := range []string{"first_over_time", "last_over_time"} {
			for _, op := range []string{"topk", "bottomk"} {
				name := fmt.Sprintf("parser=%s/%s/%s", parser, fn, op)
				t.Run(name, func(t *testing.T) {
					rnd := rand.New(rand.NewSource(int64(len(name))*31 + 1))
					for i := 0; i < 8; i++ {
						s := generateUnwrapScenario(rnd, fn, parser, true)
						s.rows = firstLastRows(s)
						query := fmt.Sprintf(`%s(2, %s({app="uw"}%s | unwrap amount [%s]))`, op, fn, parserPipe(parser), uwWindowText(s.window))
						emu := &vlEmulator{t: t, rows: s.rows, field: "amount"}
						run := runFirstLast(t, emu, query, true, s.end, s.end, 0)
						noFirstLastStatsFunction(t, query, emu)
						// Every series is a value of the per-series reference: one series
						// per (stream, labels), its sum over a single series is its value.
						all := firstLastRefSeries(s.rows, fn, int64(s.window), s.end)
						sort.Float64s(all)
						if op == "topk" {
							slices.Reverse(all)
						}
						all = all[:min(2, len(all))]
						var got []float64
						for _, pts := range run.points {
							for _, v := range pts {
								got = append(got, v)
							}
						}
						sort.Float64s(got)
						want := slices.Clone(all)
						sort.Float64s(want)
						if !slices.Equal(got, want) {
							t.Fatalf("%s #%d %s: got %v, Loki has %v", name, i, query, got, want)
						}
					}
				})
			}
		}
	}
}

// firstLastRefSeries is the value of each series at t (the reference of
// firstLastRef before the aggregation).
func firstLastRefSeries(rows []emuRow, fn string, window, t int64) []float64 {
	var out []float64
	seen := map[string]bool{}
	for _, row := range rows {
		labels := make([]string, 0, len(row.labels))
		for k, v := range row.labels {
			labels = append(labels, k+"="+v)
		}
		sort.Strings(labels)
		key := row.stream + "|" + strings.Join(labels, ",")
		if seen[key] {
			continue
		}
		seen[key] = true
		// A single-series reference: the sum over one series is its value.
		var one []emuRow
		for _, r := range rows {
			ls := make([]string, 0, len(r.labels))
			for k, v := range r.labels {
				ls = append(ls, k+"="+v)
			}
			sort.Strings(ls)
			if r.stream+"|"+strings.Join(ls, ",") == key {
				one = append(one, r)
			}
		}
		if v, ok := firstLastRef(one, fn, "sum", "", window, t)[""]; ok {
			out = append(out, v)
		}
	}
	return out
}

// An offset moves the window (t-offset-range, t-offset]: the proxy shifts the
// request's time, so the aggregation reads the shifted window, as Loki does.
//
// conformance: semantics/unwrap-first-last-under-aggregation
func TestFirstLastOverTimeUnderAggregationOffsetDifferential(t *testing.T) {
	for _, parser := range []string{"", "logfmt"} {
		for _, fn := range []string{"first_over_time", "last_over_time"} {
			name := fmt.Sprintf("parser=%s/%s", parser, fn)
			t.Run(name, func(t *testing.T) {
				rnd := rand.New(rand.NewSource(int64(len(name))*131 + 9))
				for i := 0; i < 10; i++ {
					s := generateUnwrapScenario(rnd, fn, parser, true)
					s.rows = firstLastRows(s)
					const offset = time.Minute
					query := fmt.Sprintf(`sum(%s({app="uw"}%s | unwrap amount [%s] offset 1m))`, fn, parserPipe(parser), uwWindowText(s.window))
					emu := &vlEmulator{t: t, rows: s.rows, field: "amount"}
					run := runFirstLast(t, emu, query, true, s.end, s.end, 0)
					want := firstLastRef(s.rows, fn, "sum", "", int64(s.window), s.end-int64(offset))
					noFirstLastStatsFunction(t, query, emu)
					compareFirstLast(t, fmt.Sprintf("%s #%d %s", name, i, query), run, want, "", s.end)
				}
			})
		}
	}
}
