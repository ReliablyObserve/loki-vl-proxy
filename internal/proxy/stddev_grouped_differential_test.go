package proxy

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"net/http/httptest"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// This file pins the answer of stddev and stdvar over a range aggregation to
// Loki's, over generated rows and generated windows, with a reference evaluator
// written apart from the proxy: Loki v3.7.7 pkg/logql/evaluator.go
// VectorAggEvaluator.Next (each series of the input falls into the group the
// grouping names, and the group takes the population standard deviation or
// variance of the series' VALUES by Welford's update, 0 for a group of one
// series), over count_over_time's per-series counts in the window (t-range, t].
// stddev_over_time and stdvar_over_time, which work over the samples inside one
// window, are a different function and not tested here.
//
// The proxy under test is the real handler. Only VictoriaLogs is simulated, by
// vlEmulator.

type stdGrouping struct {
	kind   string // "", "by", "without"
	labels []string
	// trailing writes the grouping after the operand: stddev (x) by (l).
	trailing bool
}

func (g stdGrouping) wrap(op, inner string) string {
	clause := ""
	if g.kind != "" {
		clause = fmt.Sprintf("%s (%s)", g.kind, strings.Join(g.labels, ", "))
	}
	switch {
	case clause == "":
		return fmt.Sprintf("%s(%s)", op, inner)
	case g.trailing:
		return fmt.Sprintf("%s(%s) %s", op, inner, clause)
	default:
		return fmt.Sprintf("%s %s (%s)", op, clause, inner)
	}
}

// groupLabels returns the labels of the group a series with these labels falls into.
func (g stdGrouping) groupLabels(series map[string]string) map[string]string {
	out := map[string]string{}
	switch g.kind {
	case "by":
		for _, l := range g.labels {
			if v, ok := series[l]; ok {
				out[l] = v
			}
		}
	case "without":
		for k, v := range series {
			out[k] = v
		}
		for _, l := range g.labels {
			delete(out, l)
		}
	}
	return out
}

func canonicalLabels(m map[string]string) string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	for _, k := range keys {
		b.WriteString(k + "=" + m[k] + ",")
	}
	return b.String()
}

var stdStreams = []map[string]string{
	{"app": "a", "pod": "p0"}, {"app": "a", "pod": "p1"}, {"app": "a", "pod": "p2"},
	{"app": "b", "pod": "p3"}, // the only series of its app: stddev 0
	{"app": "c", "pod": "p4"}, {"app": "c", "pod": "p5"},
}

// lokiSeriesLabels are the labels of a series in Loki: the stream's, and the
// service_name Loki derives from the app label at ingest (discover_service_name).
func lokiSeriesLabels(stream map[string]string) map[string]string {
	return map[string]string{"app": stream["app"], "pod": stream["pod"], "service_name": stream["app"]}
}

func stdStreamText(labels map[string]string) string {
	return fmt.Sprintf(`{app=%q,pod=%q}`, labels["app"], labels["pod"])
}

// lokiGroupedStd is the reference: for every evaluation time, the count of each
// series' lines in (t-window, t], folded per group by VectorAggEvaluator.
// Result: group -> time -> value.
func lokiGroupedStd(rows []emuRow, window int64, times []int64, op string, g stdGrouping) map[string]map[int64]float64 {
	out := map[string]map[int64]float64{}
	for _, t := range times {
		counts := map[string]float64{}
		series := map[string]map[string]string{}
		for _, row := range rows {
			if row.ts <= t-window || row.ts > t {
				continue
			}
			for _, s := range stdStreams {
				if stdStreamText(s) == row.stream {
					counts[row.stream]++
					series[row.stream] = lokiSeriesLabels(s)
				}
			}
		}
		type acc struct{ mean, m2, n float64 }
		groups := map[string]*acc{}
		for stream, value := range counts {
			key := canonicalLabels(g.groupLabels(series[stream]))
			a := groups[key]
			if a == nil {
				groups[key] = &acc{mean: value, n: 1}
				continue
			}
			a.n++
			delta := value - a.mean
			a.mean += delta / a.n
			a.m2 += delta * (value - a.mean)
		}
		for key, a := range groups {
			v := a.m2 / a.n
			if op == "stddev" {
				v = math.Sqrt(v)
			}
			if out[key] == nil {
				out[key] = map[int64]float64{}
			}
			out[key][t] = v
		}
	}
	return out
}

type stdScenario struct {
	op         string
	grouping   stdGrouping
	window     time.Duration
	step       time.Duration // 0: instant
	start, end int64
	rows       []emuRow
}

func (s stdScenario) times() []int64 {
	if s.step == 0 {
		return []int64{s.end}
	}
	var out []int64
	for t := s.start; t <= s.end; t += int64(s.step) {
		out = append(out, t)
	}
	return out
}

func (s stdScenario) query() string {
	return s.grouping.wrap(s.op, fmt.Sprintf(`count_over_time({env="production"}[%s])`, uwWindowText(s.window)))
}

// generateStdScenario draws rows around the evaluation times. A line exactly
// on a window edge is left out when edgeLines is false: below VictoriaLogs v1.45
// (no stats_query_range offset arg) the bucket of the range aggregation under
// the fold puts such a line one bucket late, which is registered apart as
// versions/stats-bucket-label-v1.45 and is not what is pinned here.
func generateStdScenario(rnd *rand.Rand, op string, g stdGrouping, instant, tumbling, edgeLines bool) stdScenario {
	steps := []time.Duration{30 * time.Second, time.Minute, 90 * time.Second}
	windows := []time.Duration{30 * time.Second, time.Minute, 90 * time.Second, 5 * time.Minute, 150 * time.Second}
	s := stdScenario{op: op, grouping: g, window: windows[rnd.Intn(len(windows))]}
	base := int64(1_800_000_000) * 1e9
	if !instant {
		s.step = steps[rnd.Intn(len(steps))]
		if tumbling {
			s.window = s.step
		}
	}
	s.start = base + int64(rnd.Intn(600))*1e9
	s.end = s.start
	if !instant {
		s.end = s.start + int64(1+rnd.Intn(7))*int64(s.step)
	}
	lo, hi := s.start-int64(s.window)-5e9, s.end+5e9
	n := 10 + rnd.Intn(80)
	for i := 0; i < n; i++ {
		ts := lo + rnd.Int63n(hi-lo)
		if rnd.Intn(4) == 0 { // on a window edge
			edge := s.start + int64(rnd.Intn(8))*int64(max(s.step, time.Second))
			if rnd.Intn(2) == 0 {
				edge -= int64(s.window)
			}
			ts = edge + int64(rnd.Intn(3)-1)
			if !edgeLines && ts == edge {
				ts++
			}
		}
		s.rows = append(s.rows, emuRow{ts: ts, stream: stdStreamText(stdStreams[rnd.Intn(len(stdStreams))]), value: 1, msg: "line"})
	}
	return s
}

func runStdScenario(t *testing.T, s stdScenario, version string) map[string]map[int64]float64 {
	t.Helper()
	emu := &vlEmulator{t: t, rows: s.rows, field: "amount", version: version}
	backend := httptest.NewServer(emu.handler())
	defer backend.Close()
	p, err := New(Config{BackendURL: backend.URL, Cache: cache.New(60*time.Second, 1000), LogLevel: "error", BackendMinVersion: "v1.40.0", BackendVersionCheckTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if version != "" {
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
	if res.Code != 200 {
		t.Fatalf("%s: %d %s", s.query(), res.Code, res.Body)
	}
	var resp struct {
		Status string `json:"status"`
		Data   struct {
			ResultType string `json:"resultType"`
			Result     []struct {
				Metric map[string]string `json:"metric"`
				Values [][]any           `json:"values"`
				Value  []any             `json:"value"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(res.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	wantType := "matrix"
	if s.step == 0 {
		wantType = "vector"
	}
	if resp.Status != "success" || resp.Data.ResultType != wantType {
		t.Fatalf("%s: status %q resultType %q, want success %s: %s", s.query(), resp.Status, resp.Data.ResultType, wantType, res.Body)
	}
	got := map[string]map[int64]float64{}
	for _, series := range resp.Data.Result {
		// Lines with no level are all in one detected_level="unknown"; the
		// sliding evaluator names it on every series, so it never splits a group.
		if series.Metric["detected_level"] == "unknown" {
			delete(series.Metric, "detected_level")
		}
		key := canonicalLabels(series.Metric)
		if got[key] != nil {
			t.Fatalf("%s: two series with the labels %v: %s", s.query(), series.Metric, res.Body)
		}
		got[key] = map[int64]float64{}
		points := series.Values
		if series.Value != nil {
			points = [][]any{series.Value}
		}
		for _, point := range points {
			v, _ := strconv.ParseFloat(point[1].(string), 64)
			got[key][int64(point[0].(float64))*1e9] = v
		}
	}
	return got
}

func compareStd(t *testing.T, label string, s stdScenario, got, want map[string]map[int64]float64) {
	t.Helper()
	ctx := func() string {
		return fmt.Sprintf("%s\nquery %s step=%v window=%v rows=%d", label, s.query(), s.step, s.window, len(s.rows))
	}
	for key, points := range want {
		for ts, w := range points {
			g, ok := got[key][ts]
			if !ok {
				t.Fatalf("no point for {%s} at %d, Loki has %g\n%s", key, ts/1e9, w, ctx())
			}
			if math.Abs(g-w) > 1e-9*math.Max(1, math.Abs(w)) {
				t.Fatalf("{%s} at %d got %g, Loki has %g\n%s", key, ts/1e9, g, w, ctx())
			}
		}
	}
	for key, points := range got {
		for ts, g := range points {
			if _, ok := want[key][ts]; !ok {
				t.Fatalf("extra point for {%s} at %d = %g, Loki has none\n%s", key, ts/1e9, g, ctx())
			}
		}
	}
}

// TestStddevStdvarGroupedDifferential: stddev and stdvar, with by, without,
// either clause position, and no grouping, over count_over_time on range
// (tumbling and sliding) and instant queries, give the groups, labels and values
// of Loki's vector aggregation at every step, on every VictoriaLogs line the
// proxy supports.
//
// conformance: semantics/stddev-stdvar-grouped-values, metric-series-identity, window-bounds-and-step-alignment
func TestStddevStdvarGroupedDifferential(t *testing.T) {
	groupings := []stdGrouping{
		{},
		{kind: "by", labels: []string{"app"}},
		{kind: "by", labels: []string{"app"}, trailing: true},
		{kind: "by", labels: []string{"app", "pod"}},
		{kind: "by"},
		{kind: "without", labels: []string{"pod"}},
		{kind: "without", labels: []string{"app"}, trailing: true},
		{kind: "without", labels: []string{"app", "pod"}},
	}
	for _, version := range []string{"", "v1.52.0", "v1.40.0"} {
		for _, op := range []string{"stddev", "stdvar"} {
			for _, g := range groupings {
				for _, mode := range []string{"sliding", "tumbling", "instant"} {
					name := fmt.Sprintf("%s/%s/%s %v/%s", version, op, g.kind, g.labels, mode)
					if g.trailing {
						name += "/trailing"
					}
					t.Run(name, func(t *testing.T) {
						rnd := rand.New(rand.NewSource(int64(len(name))*7919 + int64(len(g.labels))))
						for i := 0; i < 8; i++ {
							s := generateStdScenario(rnd, op, g, mode == "instant", mode == "tumbling", version != "v1.40.0")
							got := runStdScenario(t, s, version)
							want := lokiGroupedStd(s.rows, int64(s.window), s.times(), op, g)
							compareStd(t, fmt.Sprintf("%s #%d", name, i), s, got, want)
						}
					})
				}
			}
		}
	}
}
