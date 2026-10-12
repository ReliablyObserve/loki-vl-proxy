package proxy

import (
	"context"
	"encoding/json"
	"fmt"
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

// This file pins absent_over_time to Loki's answer over generated rows and
// generated windows, with a reference evaluator written apart from the proxy:
// Loki v3.7.7 pkg/logql/evaluator.go:647-664 (newRangeAggEvaluator) and
// :747-778 (AbsentRangeVectorEvaluator.Next: a step whose window (t-range, t]
// holds no sample is the one series valued 1, a step with a sample is nothing),
// :1319-1345 (absentLabels, pinned apart in TestAbsentOverTimeLabelsEdgeTable), and
// pkg/logql/range_vector.go:251 / :628 (the aggregation is a constant one).
// The steps are start, start+step, ... up to end: Loki aligns queries to their
// step only with -querier.align-querier-with-step, off by default
// (pkg/querier/queryrange/queryrangebase/roundtrip.go:49).
//
// The proxy under test is the real handler. Only VictoriaLogs is simulated, by
// vlEmulator, which keeps the rows a scenario gives it: a row is a line the
// pipeline of the query keeps.

type absentScenario struct {
	selector string // `{app="ab",pod="a"}`
	pipeline string // " | json", ...
	unwrap   bool   // `| unwrap amount` after the pipeline
	window   time.Duration
	step     time.Duration // 0: instant
	offset   time.Duration
	start    int64 // ns
	end      int64 // ns
	rows     []emuRow
	// wrap is the vector aggregation around absent_over_time (empty: none).
	wrap string // `sum`, `sum by (app)`, `count without (pod)`, ...
}

func (s absentScenario) rangeText() string {
	return "[" + uwWindowText(s.window) + "]" + s.offsetText()
}

func (s absentScenario) offsetText() string {
	if s.offset == 0 {
		return ""
	}
	return " offset " + uwWindowText(s.offset)
}

func (s absentScenario) query() string {
	body := s.selector + s.pipeline
	if s.unwrap {
		body += " | unwrap amount"
	}
	q := "absent_over_time(" + body + s.rangeText() + ")"
	switch {
	case s.wrap == "":
	case strings.HasSuffix(s.wrap, ","): // topk(k, ...)
		q = s.wrap + " " + q + ")"
	default:
		q = s.wrap + " (" + q + ")"
	}
	return q
}

// times are Loki's evaluation times.
func (s absentScenario) times() []int64 {
	if s.step == 0 {
		return []int64{s.end}
	}
	var out []int64
	for t := s.start; t <= s.end; t += int64(s.step) {
		out = append(out, t)
	}
	return out
}

// lokiAbsentSteps returns the evaluation times at which Loki's window
// (t-offset-range, t-offset] holds no row.
func lokiAbsentSteps(rows []emuRow, window, offset int64, times []int64) []int64 {
	var out []int64
next:
	for _, t := range times {
		for _, row := range rows {
			if row.ts > t-offset-window && row.ts <= t-offset {
				continue next
			}
		}
		out = append(out, t)
	}
	return out
}

func mapsEqualString(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if bv, ok := b[k]; !ok || bv != v {
			return false
		}
	}
	return true
}

type absentSeriesOut struct {
	metric map[string]string
	points map[int64]string
}

func runAbsentScenario(t *testing.T, emu *vlEmulator, s absentScenario) ([]absentSeriesOut, *httptest.ResponseRecorder) {
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
	params := url.Values{"query": {s.query()}}
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
		return nil, res
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
		t.Fatalf("%s: %v\n%s", s.query(), err, res.Body)
	}
	var out []absentSeriesOut
	for _, series := range resp.Data.Result {
		o := absentSeriesOut{metric: series.Metric, points: map[int64]string{}}
		points := series.Values
		if series.Value != nil {
			points = [][]any{series.Value}
		}
		for _, point := range points {
			o.points[int64(point[0].(float64))*1e9] = point[1].(string)
		}
		out = append(out, o)
	}
	return out, res
}

func (s absentScenario) describe() string {
	return fmt.Sprintf("query %s step=%v window=%v offset=%v start=%d end=%d rows=%d", s.query(), s.step, s.window, s.offset, s.start/1e9, s.end/1e9, len(s.rows))
}

// checkAbsent compares the answer with Loki's: one series carrying wantLabels
// and valued want at exactly the absent steps, or no series when none is absent.
func checkAbsent(t *testing.T, label string, s absentScenario, got []absentSeriesOut, absent []int64, wantLabels map[string]string, want string) {
	t.Helper()
	if len(absent) == 0 {
		if len(got) != 0 {
			t.Fatalf("%s: %d series, Loki has none (data in every window)\n%s\ngot %+v", label, len(got), s.describe(), got)
		}
		return
	}
	if len(got) != 1 {
		t.Fatalf("%s: %d series, Loki has one\n%s\ngot %+v", label, len(got), s.describe(), got)
	}
	if !mapsEqualString(got[0].metric, wantLabels) {
		t.Fatalf("%s: labels %v, Loki has %v\n%s", label, got[0].metric, wantLabels, s.describe())
	}
	wantSet := map[int64]bool{}
	for _, ts := range absent {
		wantSet[ts] = true
		if v, ok := got[0].points[ts]; !ok {
			t.Fatalf("%s: no point at %d, Loki has %s\n%s", label, ts/1e9, want, s.describe())
		} else if v != want {
			t.Fatalf("%s: at %d got %s, Loki has %s\n%s", label, ts/1e9, v, want, s.describe())
		}
	}
	var extra []int64
	for ts := range got[0].points {
		if !wantSet[ts] {
			extra = append(extra, ts/1e9)
		}
	}
	if len(extra) > 0 {
		sort.Slice(extra, func(i, j int) bool { return extra[i] < extra[j] })
		t.Fatalf("%s: extra points at %v, Loki has data in those windows\n%s", label, extra, s.describe())
	}
}

// generateAbsentScenario draws rows and windows. exactEdges puts rows exactly
// on a window edge; VictoriaLogs before v1.45 buckets by the step grid, which
// puts such a row in the neighbouring window for every count_over_time
// (registered separately), so its scenarios stay one nanosecond off the edge.
func generateAbsentScenario(rnd *rand.Rand, instant, exactEdges bool) absentScenario {
	steps := []time.Duration{30 * time.Second, time.Minute, 90 * time.Second}
	windows := []time.Duration{30 * time.Second, time.Minute, 90 * time.Second, 5 * time.Minute, 150 * time.Second}
	s := absentScenario{selector: `{app="ab"}`, window: windows[rnd.Intn(len(windows))]}
	base := int64(1_800_000_000) * 1e9
	if !instant {
		s.step = steps[rnd.Intn(len(steps))]
		if rnd.Intn(3) == 0 {
			s.window = s.step // a tumbling window
		}
	}
	// Evaluation times on whole seconds, not on the step: Loki steps from start.
	s.start = base + int64(rnd.Intn(600))*1e9
	if instant {
		s.end = s.start
	} else {
		s.end = s.start + int64(rnd.Intn(10))*int64(s.step)
	}
	if rnd.Intn(4) == 0 {
		s.offset = []time.Duration{time.Minute, 5 * time.Minute}[rnd.Intn(2)]
	}
	// Rows cluster in a few bursts so some windows hold data and some do not;
	// older rows sit just before the first window (a window that is empty must
	// not be answered from them).
	lo := s.start - int64(s.offset) - int64(s.window) - int64(10*time.Minute)
	hi := s.end - int64(s.offset) + 5e9
	bursts := rnd.Intn(4)
	for b := 0; b < bursts; b++ {
		centre := lo + rnd.Int63n(hi-lo)
		for i, n := 0, 1+rnd.Intn(4); i < n; i++ {
			s.rows = append(s.rows, emuRow{ts: centre + int64(rnd.Intn(40))*1e9 - 20e9, stream: `{app="ab",pod="a"}`, value: float64(rnd.Intn(3))})
		}
	}
	for i := 0; i < 3; i++ { // rows on window edges
		if rnd.Intn(2) == 0 {
			edge := s.start - int64(s.offset) + int64(rnd.Intn(10))*int64(max(s.step, time.Second))
			if rnd.Intn(2) == 0 {
				edge -= int64(s.window)
			}
			jitter := int64(rnd.Intn(3) - 1)
			if jitter == 0 && !exactEdges {
				jitter = 1
			}
			s.rows = append(s.rows, emuRow{ts: edge + jitter, stream: `{app="ab",pod="b"}`, value: 1})
		}
	}
	return s
}

// An absent_over_time over a plain selector, a line filter, a parser, or an
// unwrapped label is the one series valued 1 at the steps whose window holds no
// line, for a range and an instant query, with and without an offset, on every
// VictoriaLogs line the proxy supports. The presence comes from VictoriaLogs
// stats, never from raw rows.
//
// conformance: semantics/absent-over-time-semantics, window-bounds-and-step-alignment
func TestAbsentOverTimeDifferential(t *testing.T) {
	for _, version := range []string{"", "v1.52.0", "v1.40.0"} {
		for _, shape := range []struct{ name, pipeline string }{{"selector", ""}, {"line-filter", ` |= "x"`}, {"logfmt", " | logfmt"}, {"json", " | json"}, {"label-filter", ` | logfmt | level="error"`}, {"unwrap", " | logfmt"}} {
			for _, instant := range []bool{false, true} {
				name := fmt.Sprintf("version=%s/%s/instant=%v", version, shape.name, instant)
				t.Run(name, func(t *testing.T) {
					rnd := rand.New(rand.NewSource(int64(len(name))*7919 + 31))
					for i := 0; i < 30; i++ {
						func() {
							s := generateAbsentScenario(rnd, instant, version != "v1.40.0")
							s.pipeline = shape.pipeline
							s.unwrap = shape.name == "unwrap"
							if shape.name == "json" {
								for j := range s.rows {
									s.rows[j].msg = fmt.Sprintf(`{"amount":%v}`, s.rows[j].value)
								}
							}
							emu := &vlEmulator{t: t, rows: s.rows, field: "amount", version: version}
							defer func() {
								if t.Failed() {
									t.Logf("VictoriaLogs calls: %+v rows: %+v", emu.calls, s.rows)
								}
							}()
							got, res := runAbsentScenario(t, emu, s)
							if res.Code != http.StatusOK {
								t.Fatalf("%s #%d: %d %s\n%s", name, i, res.Code, res.Body, s.describe())
							}
							absent := lokiAbsentSteps(s.rows, int64(s.window), int64(s.offset), s.times())
							checkAbsent(t, fmt.Sprintf("%s #%d", name, i), s, got, absent, map[string]string{"app": "ab"}, "1")
							if raw := emu.endpointCalls("/select/logsql/query"); len(raw) > 0 && version != "v1.40.0" {
								t.Fatalf("%s #%d: the metric read raw rows: %s", name, i, raw[0])
							}
						}()
					}
				})
			}
		}
	}
}

// Under a vector aggregation the one absent series folds as Loki's vector
// aggregation folds a single series (pkg/logql/evaluator.go:400-529
// VectorAggEvaluator): `by` keeps the listed labels the series has, `without`
// drops them, `count` is 1, `stddev` and `stdvar` are 0, and topk keeps the
// series whole.
//
// conformance: semantics/absent-over-time-semantics, metric-series-identity
func TestAbsentOverTimeUnderVectorAggregation(t *testing.T) {
	for _, tc := range []struct {
		wrap   string
		labels map[string]string
		value  string
	}{
		{"sum", map[string]string{}, "1"},
		{"avg", map[string]string{}, "1"},
		{"min", map[string]string{}, "1"},
		{"max", map[string]string{}, "1"},
		{"count", map[string]string{}, "1"},
		{"stddev", map[string]string{}, "0"},
		{"stdvar", map[string]string{}, "0"},
		{"sum by (app)", map[string]string{"app": "ab"}, "1"},
		{"sum by (app, nope)", map[string]string{"app": "ab"}, "1"},
		{"sum by (nope)", map[string]string{}, "1"},
		{"sum without (pod)", map[string]string{"app": "ab", "env": "prod"}, "1"},
		{"count without (app)", map[string]string{"env": "prod"}, "1"},
		{"topk(1,", map[string]string{"app": "ab", "env": "prod"}, "1"},
		{"bottomk(2,", map[string]string{"app": "ab", "env": "prod"}, "1"},
	} {
		for _, instant := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/instant=%v", tc.wrap, instant), func(t *testing.T) {
				rnd := rand.New(rand.NewSource(int64(len(tc.wrap)) * 104729))
				for i := 0; i < 12; i++ {
					s := generateAbsentScenario(rnd, instant, true)
					s.selector = `{app="ab",env="prod"}`
					s.wrap = tc.wrap
					emu := &vlEmulator{t: t, rows: s.rows, field: "amount"}
					q := s.query()
					got, res := runAbsentScenario(t, emu, s)
					if res.Code != http.StatusOK {
						t.Fatalf("%s: %d %s", q, res.Code, res.Body)
					}
					absent := lokiAbsentSteps(s.rows, int64(s.window), int64(s.offset), s.times())
					checkAbsent(t, fmt.Sprintf("%s #%d", tc.wrap, i), s, got, absent, tc.labels, tc.value)
				}
			})
		}
	}
}
