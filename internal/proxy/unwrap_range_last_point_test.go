package proxy

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http/httptest"
	"net/url"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// Loki evaluates a range query at start, start+step, ... up to and including
// end (pkg/logql/range_vector.go step iteration), so a source that stops at
// `end` still has a sample there. VictoriaLogs' bucket labelled T holds the
// lines from T on: the sample at `end` was the bucket after the last line, and
// sum, avg, max and min over time with unwrap answered one point short.

// unwrapRangeResult runs a query_range through the real handler against the
// emulated VictoriaLogs and returns, per pod, timestamp -> value.
func unwrapRangeResult(t *testing.T, emu *vlEmulator, query string, startSec, endSec, stepSec int64) map[string]map[int64]float64 {
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
	params := url.Values{"query": {query}, "start": {strconv.FormatInt(startSec, 10)}, "end": {strconv.FormatInt(endSec, 10)}, "step": {strconv.FormatInt(stepSec, 10)}}
	res := doCompatProxyRequest(p, "/loki/api/v1/query_range?"+params.Encode(), map[string]string{"X-Scope-OrgID": "0"})
	if res.Code != 200 {
		t.Fatalf("%s: %d %s", query, res.Code, res.Body)
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
		points := map[int64]float64{}
		for _, point := range series.Values {
			v, _ := strconv.ParseFloat(point[1].(string), 64)
			points[int64(point[0].(float64))] = v
		}
		got[series.Metric["pod"]] = points
	}
	return got
}

func evaluationTimes(startSec, endSec, stepSec int64) []int64 {
	var out []int64
	for ts := startSec; ts <= endSec; ts += stepSec {
		out = append(out, ts)
	}
	return out
}

func sortedTimes(points map[int64]float64) []int64 {
	out := make([]int64, 0, len(points))
	for ts := range points {
		out = append(out, ts)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// TestUnwrapOverTimeRangeEmitsEndSample: whatever the window (tumbling,
// sliding, shorter than the step) and the function, a source whose lines stop
// at `end` answers every evaluation time from start to end inclusive, with the
// end not a multiple of the step away from start too.
//
// conformance: semantics/unwrap-range-last-point, window-bounds-and-step-alignment
func TestUnwrapOverTimeRangeEmitsEndSample(t *testing.T) {
	const base = int64(1_800_000_000)
	shapes := []struct {
		name         string
		window, step time.Duration
		lengthSec    int64
	}{
		{"tumbling", time.Minute, time.Minute, 3600},
		{"sliding", 5 * time.Minute, time.Minute, 3600},
		{"sliding-uneven", 150 * time.Second, 90 * time.Second, 3600},
		{"window-shorter-than-step", 30 * time.Second, time.Minute, 3600},
		{"end-off-the-step-grid", 5 * time.Minute, time.Minute, 3617},
	}
	for _, version := range []string{"", "v1.52.0", "v1.40.0"} {
		for _, parser := range []string{"json", "logfmt"} {
			for _, fn := range []string{"sum_over_time", "avg_over_time", "max_over_time", "min_over_time"} {
				for _, shape := range shapes {
					name := fmt.Sprintf("%s/%s/%s/%s", version, parser, fn, shape.name)
					t.Run(name, func(t *testing.T) {
						startSec, endSec, stepSec := base, base+shape.lengthSec, int64(shape.step/time.Second)
						rnd := rand.New(rand.NewSource(int64(len(name)) * 7919))
						var rows []emuRow
						for _, pod := range []string{"a", "b"} {
							// Lines every few seconds from before the first window to
							// just before end; none at or after it.
							for ts := startSec*1e9 - int64(shape.window) - int64(shape.step); ts < endSec*1e9; ts += (5 + int64(rnd.Intn(5))) * 1e9 {
								rows = append(rows, emuRow{ts: ts + 5e8, stream: fmt.Sprintf(`{app="uw",pod=%q}`, pod), value: float64(1 + rnd.Intn(9))})
							}
						}
						emu := &vlEmulator{t: t, rows: rows, field: "amount", version: version}
						query := fmt.Sprintf(`sum by (pod) (%s({app="uw"} | %s | unwrap amount | __error__="" [%s]))`, fn, parser, uwWindowText(shape.window))
						got := unwrapRangeResult(t, emu, query, startSec, endSec, stepSec)
						want := evaluationTimes(startSec, endSec, stepSec)
						for _, pod := range []string{"a", "b"} {
							times := sortedTimes(got[pod])
							if fmt.Sprint(times) != fmt.Sprint(want) {
								t.Fatalf("pod %s: %d samples, first %d last %d; Loki has %d, first %d last %d\nquery %s",
									pod, len(times), first(times)-startSec, last(times)-startSec, len(want), 0, want[len(want)-1]-startSec, query)
							}
						}
					})
				}
			}
		}
	}
}

func first(v []int64) int64 {
	if len(v) == 0 {
		return 0
	}
	return v[0]
}

func last(v []int64) int64 {
	if len(v) == 0 {
		return 0
	}
	return v[len(v)-1]
}

// TestUnwrapOverTimeTumblingMatchesLoki: with range == step the bucket answers
// Loki's window (t-range, t] exactly, so the values at every step, the end
// included, are Loki's, over generated rows (zero values, lines beside the
// window edges) and windows, per stream.
//
// conformance: semantics/unwrap-range-last-point, window-bounds-and-step-alignment
func TestUnwrapOverTimeTumblingMatchesLoki(t *testing.T) {
	for _, version := range []string{"", "v1.52.0", "v1.40.0"} {
		for _, parser := range []string{"json", "logfmt"} {
			for _, fn := range []string{"sum_over_time", "avg_over_time", "max_over_time", "min_over_time"} {
				name := fmt.Sprintf("%s/%s/%s", version, parser, fn)
				t.Run(name, func(t *testing.T) {
					rnd := rand.New(rand.NewSource(int64(len(name))*104729 + 3))
					for i := 0; i < 20; i++ {
						s := generateUnwrapScenario(rnd, fn, parser, false)
						s.window = s.step
						delta := -(s.start % int64(s.step)) // an epoch-aligned start: the only one v1.40 can place buckets on
						s.start, s.end = s.start+delta, s.end+delta
						for j := range s.rows {
							s.rows[j].ts += delta
							s.rows[j].labels = nil
							if version == "v1.40.0" && (s.rows[j].ts-s.start)%int64(s.step) == 0 {
								s.rows[j].ts++ // the unshifted bucket edges put a line at t in the next window
							}
						}
						emu := &vlEmulator{t: t, rows: s.rows, field: "amount", version: version}
						query := fmt.Sprintf(`sum by (pod) (%s({app="uw"} | %s | unwrap amount | __error__="" [%s]))`, fn, parser, uwWindowText(s.window))
						got := unwrapRangeResult(t, emu, query, s.start/1e9, s.end/1e9, int64(s.step/time.Second))
						for _, pod := range []string{"a", "b", "c"} {
							var rows []emuRow
							for _, row := range s.rows {
								if row.stream == fmt.Sprintf(`{app="uw",pod=%q}`, pod) {
									rows = append(rows, row)
								}
							}
							gotNs := map[int64]float64{}
							for ts, v := range got[pod] {
								gotNs[ts*1e9] = v
							}
							compareUnwrapSum(t, fmt.Sprintf("%s #%d pod=%s", name, i, pod), s, gotNs, lokiUnwrapSum(rows, fn, int64(s.window), s.times()), 1)
						}
					}
				})
			}
		}
	}
}
