package proxy

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/grafana/jsonparser"
)

// A `| json` metric whose range is shorter than the step (Grafana's step for a
// long panel over a `[1m]` query) is answered from VictoriaLogs stats, like the
// same query at a step equal to the range, and exactly as Loki answers it.
// Loki evaluates each step over (t-range, t] and never reads the lines between
// two windows (pkg/logql/range_vector.go: newRangeVectorIterator takes the
// batch iterator when selRange < step, and batchRangeVectorIterator.load
// consumes every sample at or before the window start without adding it), so
// a line its JSON parser rejects there never fails the query
// (pkg/logql/evaluator.go RangeVectorEvaluator.Next checks only the step's
// vector for __error__).

// stepWiderQueries are the shapes of the ordered JSON pushdown that the window
// filter changes: the strict detected_level count (Logs Drilldown and the
// DLV3 shape), its rate and byte forms, the `__error__!=""` and `__error__=""`
// splits, and the plain pushdown (`drop __error__`).
var stepWiderQueries = []struct {
	name, fn, pipeline, group string
	mode                      string // strict, errors, valid, all
}{
	{"strict count", "count_over_time", `| json`, "detected_level", "strict"},
	{"strict rate", "rate", `| json`, "detected_level", "strict"},
	{"strict bytes", "bytes_over_time", `| json`, "detected_level", "strict"},
	{"errors count", "count_over_time", `| json | __error__!=""`, "detected_level", "errors"},
	{"errors bytes rate", "bytes_rate", `| json | __error__!=""`, "detected_level", "errors"},
	{"valid by status", "count_over_time", `| json | __error__=""`, "status", "valid"},
	{"valid ungrouped", "count_over_time", `| json | __error__=""`, "", "valid"},
	{"drop error", "count_over_time", `| json | drop __error__`, "detected_level", "all"},
}

func stepWiderQuery(fn, pipeline, group string, window time.Duration) string {
	by := ""
	if group != "" {
		by = " by (" + group + ")"
	}
	return fmt.Sprintf(`sum%s (%s({env="prod"} %s [%s]))`, by, fn, pipeline, formatLogQLDurationForTest(window))
}

func formatLogQLDurationForTest(d time.Duration) string {
	if d%time.Minute == 0 {
		return strconv.FormatInt(int64(d/time.Minute), 10) + "m"
	}
	if d%time.Second == 0 {
		return strconv.FormatInt(int64(d/time.Second), 10) + "s"
	}
	return strconv.FormatInt(int64(d/time.Millisecond), 10) + "ms"
}

// stepWiderLine kinds: what Loki's JSON parser and VictoriaLogs' make of them.
var stepWiderLines = []struct {
	format   string // %s is the status, %d the row number
	rejected bool   // Loki's JSON parser rejects the line
}{
	{`{"status":"%s","n":%d}`, false},
	{`  {"status":"%s","n":%d}`, false},
	{`{"status":"%s","nested":{"a":%d}}`, false},
	{`{"status":"%s"} trailing %d`, false},
	{`{"status":"%s","msg":"trunc %d`, true},
	{`{"status":"%s","v":NaN,"n":%d}`, true},
	{`plain %s text %d`, true},
	{``, true},
}

func stepWiderRow(ts time.Time, level string, kind int, i int) pushdownRow {
	status := []string{"200", "404", "500"}[i%3]
	msg := stepWiderLines[kind].format
	if msg != "" {
		msg = fmt.Sprintf(msg, status, i)
	}
	return pushdownRow{ts: ts, stream: map[string]string{"env": "prod", "level": level}, msg: msg}
}

// lokiStepWiderMatrix is the Loki-semantics reference: each evaluation time t
// on start+k*step up to end reads the lines in (t-window, t], and a strict
// pipeline fails on the first rejected line any window reads. It returns nil
// and true for Loki's pipeline error. The parser's acceptance is
// lokiJSONAccepts (Loki's JSONParser.Process); every line here is flat or
// nested without a hazard below the top level, so the parser hints do not
// change it.
func lokiStepWiderMatrix(rows []pushdownRow, fn, group, mode string, start, end time.Time, step, window time.Duration) (map[string]map[int64]float64, bool) {
	out := map[string]map[int64]float64{}
	for t := start; !t.After(end); t = t.Add(step) {
		counts := map[string]float64{}
		present := map[string]bool{}
		for _, row := range rows {
			if !row.ts.After(t.Add(-window)) || row.ts.After(t) {
				continue
			}
			accepted := lokiJSONAccepts(row.msg, nil)
			switch {
			case mode == "strict" && !accepted:
				return nil, true
			case mode == "errors" && accepted, mode == "valid" && !accepted:
				continue
			}
			labels := map[string]string{}
			switch group {
			case "detected_level":
				labels[group] = row.stream["level"]
			case "status":
				if v, err := jsonparser.GetString([]byte(row.msg), "status"); err == nil && v != "" {
					labels[group] = v
				}
			}
			key := canonicalLabelsKey(labels)
			present[key] = true
			if strings.HasPrefix(fn, "bytes") {
				counts[key] += float64(len(row.msg))
			} else {
				counts[key]++
			}
		}
		for key := range present {
			v := counts[key]
			if fn == "rate" || fn == "bytes_rate" {
				v /= window.Seconds()
			}
			if out[key] == nil {
				out[key] = map[int64]float64{}
			}
			out[key][t.UnixMilli()] = v
		}
	}
	return out, false
}

// decodeStepWiderMatrix keys the points by millisecond: the evaluation times
// of an unaligned query are not whole seconds.
func decodeStepWiderMatrix(t testing.TB, body []byte) map[string]map[int64]float64 {
	t.Helper()
	var resp struct {
		Data struct {
			Result []struct {
				Metric map[string]string `json:"metric"`
				Values [][]any           `json:"values"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatalf("invalid matrix: %v: %s", err, body)
	}
	got := map[string]map[int64]float64{}
	for _, series := range resp.Data.Result {
		key := canonicalLabelsKey(series.Metric)
		if got[key] == nil {
			got[key] = map[int64]float64{}
		}
		for _, pair := range series.Values {
			ts, _ := pair[0].(float64)
			text, _ := pair[1].(string)
			v, err := strconv.ParseFloat(text, 64)
			if err != nil {
				t.Fatalf("invalid sample %v: %s", pair, body)
			}
			got[key][int64(math.Round(ts*1000))] = v
		}
	}
	return got
}

func stepWiderMatricesEqual(got, want map[string]map[int64]float64) bool {
	if len(got) != len(want) {
		return false
	}
	for key, points := range want {
		other, ok := got[key]
		if !ok || len(other) != len(points) {
			return false
		}
		for ts, v := range points {
			g, ok := other[ts]
			if !ok || math.Abs(g-v) > 1e-9*math.Max(1, math.Abs(v)) {
				return false
			}
		}
	}
	return true
}

// stepWiderBackend is one proxy over a recording fake VictoriaLogs; it records
// the step of every stats_query_range request.
type stepWiderBackend struct {
	p     *Proxy
	fake  *pushdownFakeVL
	mu    sync.Mutex
	steps []string
}

func newStepWiderBackend(t testing.TB, rows []pushdownRow) *stepWiderBackend {
	t.Helper()
	srv, fake := newPushdownFakeVL(t, rows, nil)
	fake.jsonUnpack = vlFastjsonUnpackFields
	b := &stepWiderBackend{fake: fake}
	fake.onStats = func(r *http.Request, _ string) {
		b.mu.Lock()
		b.steps = append(b.steps, r.Form.Get("step"))
		b.mu.Unlock()
	}
	b.p = newFilterPushdownProxy(t.(*testing.T), srv.URL)
	fake.stored = b.p.labelTranslator.ToVL
	return b
}

// query answers one range query through the proxy: the status, the matrix and
// the error body.
func (b *stepWiderBackend) query(t testing.TB, query string, start, end time.Time, step time.Duration) (int, map[string]map[int64]float64, string) {
	t.Helper()
	params := fmt.Sprintf("query=%s&start=%d&end=%d&step=%s", url.QueryEscape(query), start.UnixNano(), end.UnixNano(), strconv.FormatFloat(step.Seconds(), 'f', -1, 64))
	rec := httptest.NewRecorder()
	b.p.handleQueryRange(rec, httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+params, nil))
	if rec.Code != http.StatusOK {
		return rec.Code, nil, rec.Body.String()
	}
	return rec.Code, decodeStepWiderMatrix(t, rec.Body.Bytes()), ""
}

// checkStepWider compares one query with the reference and returns a failure
// description, or "".
func (b *stepWiderBackend) checkStepWider(t testing.TB, rows []pushdownRow, q struct {
	name, fn, pipeline, group string
	mode                      string
}, start, end time.Time, step, window time.Duration) string {
	query := stepWiderQuery(q.fn, q.pipeline, q.group, window)
	want, wantErr := lokiStepWiderMatrix(rows, q.fn, q.group, q.mode, start, end, step, window)
	code, got, body := b.query(t, query, start, end, step)
	switch {
	case wantErr:
		if code != http.StatusBadRequest || !strings.Contains(body, "JSONParserErr") {
			return fmt.Sprintf("%s: got %d %q, want Loki's 400 JSONParserErr pipeline error", query, code, body)
		}
	case code != http.StatusOK:
		return fmt.Sprintf("%s: status %d: %s", query, code, body)
	case !stepWiderMatricesEqual(got, want):
		return fmt.Sprintf("%s: differs from Loki's semantics\n got: %v\nwant: %v", query, got, want)
	}
	return ""
}

// stepWiderRows seeds rows over [s0, s0+20m): random millisecond times plus
// lines exactly on minute marks (evaluation times and window starts of the
// aligned grids), on canonical stream levels. rejectedOK false keeps every
// line one Loki's parser accepts.
func stepWiderRows(rng *rand.Rand, s0 time.Time, n int, rejectedOK bool) []pushdownRow {
	levels := []string{"info", "error", "warn", "debug"}
	var rows []pushdownRow
	for i := 0; i < n; i++ {
		ts := s0.Add(time.Duration(rng.Intn(1_200_000)) * time.Millisecond)
		if i%10 == 0 {
			ts = s0.Add(time.Duration(rng.Intn(21)) * time.Minute)
		}
		kind := rng.Intn(4)
		if rejectedOK && rng.Intn(4) == 0 {
			kind = 4 + rng.Intn(4)
		}
		rows = append(rows, stepWiderRow(ts, levels[rng.Intn(len(levels))], kind, i))
	}
	return rows
}

var stepWiderGrids = []struct{ window, step time.Duration }{
	{time.Minute, 30 * time.Second},
	{time.Minute, time.Minute},
	{time.Minute, 61 * time.Second},
	{time.Minute, 90 * time.Second},
	{time.Minute, 2 * time.Minute},
	{time.Minute, 5 * time.Minute},
	{2 * time.Minute, 150 * time.Second},
	{30 * time.Second, 45 * time.Second},
	{20 * time.Second, time.Minute},
	{10 * time.Second, 15 * time.Second},
}

// Every shape of the family over random rows answers as Loki's semantics do,
// for a range shorter than, equal to and longer than the step, steps that are
// not a multiple of the range, sub-minute ranges and steps, and both Loki
// profiles' windows: aligned to the step (align_queries_with_step) and
// unaligned, millisecond start (the defaults). One proxy serves every query of
// a dataset, so a probe verdict remembered for one step is reused for the
// others.
// conformance: semantics/json-metric-step-wider-than-range, semantics/json-detected-level-error-split, semantics/json-metric-detected-level-derivation
func TestOrderedJSONStepWiderThanRangeDifferential(t *testing.T) {
	seeds := 6
	if testing.Short() {
		seeds = 2
	}
	s0 := time.Unix(1700000400, 0).UTC()
	for seed := int64(1); seed <= int64(seeds); seed++ {
		rng := rand.New(rand.NewSource(seed))
		for _, rejected := range []bool{false, true} {
			rows := stepWiderRows(rng, s0, 300, rejected)
			b := newStepWiderBackend(t, rows)
			for _, grid := range stepWiderGrids {
				for _, aligned := range []bool{true, false} {
					start := s0.Add(4*time.Minute + time.Duration(rng.Intn(60_000))*time.Millisecond)
					if aligned {
						start = time.Unix(0, start.UnixNano()/int64(grid.step)*int64(grid.step)).UTC()
					}
					end := start.Add(12 * time.Minute)
					for _, q := range stepWiderQueries {
						if msg := b.checkStepWider(t, rows, q, start, end, grid.step, grid.window); msg != "" {
							t.Fatalf("seed %d rejected=%v [%v] step %v aligned=%v: %s", seed, rejected, grid.window, grid.step, aligned, msg)
						}
					}
				}
			}
		}
	}
}

// The edge table: one line of each kind placed exactly on a window's end
// (counted), exactly on its start (not counted), inside a window and in the
// gap between two windows, among ordinary lines, for [1m] at a 2m and a 90s
// step. A line Loki's parser rejects fails the strict count only inside a
// window; in a gap the strict count answers from VictoriaLogs stats with no
// raw-row read, the way Loki answers it.
// conformance: semantics/json-metric-step-wider-than-range, semantics/json-detected-level-error-split
func TestOrderedJSONStepWiderThanRangeEdgeTable(t *testing.T) {
	s0 := time.Unix(1700000400, 0).UTC()
	start := s0.Add(4 * time.Minute)
	end := start.Add(10 * time.Minute)
	for _, grid := range []struct{ window, step time.Duration }{{time.Minute, 2 * time.Minute}, {time.Minute, 90 * time.Second}} {
		eval := start.Add(4 * grid.step) // an evaluation time inside the range
		for _, place := range []struct {
			name     string
			at       time.Time
			inWindow bool
		}{
			{"on the evaluation time", eval, true},
			{"on the window start", eval.Add(-grid.window), false},
			{"one millisecond after the window start", eval.Add(-grid.window + time.Millisecond), true},
			{"in the gap", eval.Add(-grid.window - (grid.step-grid.window)/2), false},
			{"one millisecond before the next window", eval.Add(grid.step - grid.window), false},
		} {
			for kind, line := range stepWiderLines {
				var rows []pushdownRow
				for i := 0; i < 80; i++ {
					rows = append(rows, stepWiderRow(s0.Add(time.Duration(i)*10*time.Second+3*time.Second), []string{"info", "error"}[i%2], 0, i))
				}
				rows = append(rows, stepWiderRow(place.at, "warn", kind, 999))
				label := fmt.Sprintf("[%v] step %v, %q %s", grid.window, grid.step, line.format, place.name)
				b := newStepWiderBackend(t, rows)
				for _, q := range stepWiderQueries {
					if msg := b.checkStepWider(t, rows, q, start, end, grid.step, grid.window); msg != "" {
						t.Fatalf("%s: %s", label, msg)
					}
				}
				// The strict count on a fresh proxy (the answer above is cached):
				// an error only for a rejected line inside a window; otherwise
				// stats, and raw rows only from the windows.
				b = newStepWiderBackend(t, rows)
				code, got, body := b.query(t, stepWiderQuery("count_over_time", "| json", "detected_level", grid.window), start, end, grid.step)
				if line.rejected && place.inWindow {
					if code != http.StatusBadRequest {
						t.Fatalf("%s: strict got %d, want Loki's pipeline error", label, code)
					}
					continue
				}
				if code != http.StatusOK {
					t.Fatalf("%s: strict status %d: %s", label, code, body)
				}
				b.fake.mu.Lock()
				stats, raw := len(b.fake.stats), b.fake.rawQueries
				b.fake.mu.Unlock()
				if stats == 0 {
					t.Fatalf("%s: strict answered by the row evaluator", label)
				}
				for _, read := range raw {
					if !strings.Contains(read.query, "__lvp_window_phase") || !strings.Contains(read.query, jsonInvalidCondition(false)) && !strings.Contains(read.query, jsonInvalidCondition(true)) {
						t.Fatalf("%s: raw read %q is not limited to the window lines VictoriaLogs cannot vouch for", label, read.query)
					}
				}
				warn := got[`{detected_level="warn"}`]
				if (len(warn) > 0) != place.inWindow {
					t.Fatalf("%s: warn series %v, in window %v", label, warn, place.inWindow)
				}
			}
		}
	}
}

// A refreshed panel moves its window by whole steps, and the strict split
// keeps the settled steps of its earlier answer: the moved window answers as
// Loki does, a line Loki's parser rejects that appears only in a new window
// fails it, and one in a new gap does not.
// conformance: semantics/json-metric-step-wider-than-range, semantics/json-detected-level-error-split
func TestOrderedJSONStepWiderThanRangeRefresh(t *testing.T) {
	s0 := time.Unix(1700000400, 0).UTC()
	window, step := time.Minute, 2*time.Minute
	start, end := s0.Add(4*time.Minute), s0.Add(12*time.Minute)
	for _, tc := range []struct {
		name string
		at   time.Duration // after s0, of the unbraced line
	}{
		{"no rejected line", 0},
		{"a rejected line in a new window", 15*time.Minute + 30*time.Second},
		{"a rejected line in a new gap", 14*time.Minute + 30*time.Second},
	} {
		var rows []pushdownRow
		for i := 0; i < 120; i++ {
			rows = append(rows, stepWiderRow(s0.Add(time.Duration(i)*10*time.Second+3*time.Second), []string{"info", "error"}[i%2], 2*(i%2), i))
		}
		if tc.at != 0 {
			rows = append(rows, stepWiderRow(s0.Add(tc.at), "warn", 6, 999))
		}
		b := newStepWiderBackend(t, rows)
		for _, q := range stepWiderQueries {
			for shift := 0; shift <= 3; shift += 3 {
				from, to := start.Add(time.Duration(shift)*step), end.Add(time.Duration(shift)*step)
				if msg := b.checkStepWider(t, rows, q, from, to, step, window); msg != "" {
					t.Fatalf("%s, window moved %d steps: %s", tc.name, shift, msg)
				}
			}
		}
	}
}

// DLV3 at the 48h step: a range shorter than the step is answered from one
// stats_query_range request per attempt with buckets one step wide (not the
// gcd of step and range) and the window filter, never a raw scan; the
// evaluator counter records the stats answer. A step equal to the range keeps
// its unfiltered query.
// conformance: semantics/json-metric-step-wider-than-range, semantics/json-metric-detected-level-derivation
func TestOrderedJSONStepWiderThanRangeRoute(t *testing.T) {
	s0 := time.Unix(1700000400, 0).UTC()
	var rows []pushdownRow
	for i := 0; i < 240; i++ {
		// Line kinds VictoriaLogs vouches for: no raw rows to read.
		rows = append(rows, stepWiderRow(s0.Add(time.Duration(i)*5*time.Second), []string{"info", "error"}[i%2], 2*(i%2), i))
	}
	for _, tc := range []struct {
		step     time.Duration
		wantStep string
		phased   bool
	}{
		{time.Minute, "60s", false},
		{90 * time.Second, "90s", true},
		{2 * time.Minute, "120s", true},
		{5 * time.Minute, "300s", true},
		{604800 * time.Millisecond, "604800000000ns", true},
	} {
		b := newStepWiderBackend(t, rows)
		query := `sum by (detected_level) (count_over_time({env="prod"} | json [1m]))`
		code, _, body := b.query(t, query, s0.Add(2*time.Minute), s0.Add(20*time.Minute), tc.step)
		if code != http.StatusOK {
			t.Fatalf("step %v: status %d: %s", tc.step, code, body)
		}
		b.fake.mu.Lock()
		stats, raw := append([]string(nil), b.fake.stats...), b.fake.raw
		b.fake.mu.Unlock()
		b.mu.Lock()
		steps := append([]string(nil), b.steps...)
		b.mu.Unlock()
		if len(stats) == 0 || raw != 0 {
			t.Fatalf("step %v: stats=%d raw=%d, want stats only", tc.step, len(stats), raw)
		}
		for i, q := range stats {
			if strings.Contains(q, "__lvp_window_phase") != tc.phased || steps[i] != tc.wantStep {
				t.Fatalf("step %v: stats request %d step %s phased %v, want %s phased %v: %s", tc.step, i, steps[i], strings.Contains(q, "__lvp_window_phase"), tc.wantStep, tc.phased, q)
			}
		}
		rec := httptest.NewRecorder()
		b.p.metrics.Handler(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
		if want := `loki_vl_proxy_parser_metric_evaluations_total{evaluator="vl_stats_buckets",reason="pushdown"} 1`; !bytes.Contains(rec.Body.Bytes(), []byte(want)) {
			t.Fatalf("step %v: /metrics lacks %q", tc.step, want)
		}
		if bytes.Contains(rec.Body.Bytes(), []byte(`evaluator="raw_rows"`)) {
			t.Fatalf("step %v: the row evaluator answered", tc.step)
		}
	}
}

// FuzzOrderedJSONStepWiderThanRange drives one shape of the family over a
// small random dataset, range, step and start, against the Loki-semantics
// reference.
// conformance: semantics/json-metric-step-wider-than-range
func FuzzOrderedJSONStepWiderThanRange(f *testing.F) {
	f.Add(int64(1), uint8(60), uint16(120), uint16(0), uint8(0), true)
	f.Add(int64(2), uint8(60), uint16(90), uint16(1234), uint8(3), true)
	f.Add(int64(3), uint8(30), uint16(45), uint16(999), uint8(5), false)
	f.Add(int64(4), uint8(60), uint16(60), uint16(0), uint8(7), true)
	f.Add(int64(5), uint8(20), uint16(15), uint16(500), uint8(1), false)
	f.Fuzz(func(t *testing.T, seed int64, windowSec uint8, stepSec uint16, offsetMs uint16, shape uint8, rejected bool) {
		window := time.Duration(windowSec%120+1) * time.Second
		step := time.Duration(stepSec%600+1) * time.Second
		if (12*time.Minute)/step > 400 {
			return // the reference evaluates every step: keep an execution short
		}
		s0 := time.Unix(1700000400, 0).UTC()
		rows := stepWiderRows(rand.New(rand.NewSource(seed)), s0, 120, rejected)
		start := s0.Add(4*time.Minute + time.Duration(offsetMs%60000)*time.Millisecond)
		b := newStepWiderBackend(t, rows)
		q := stepWiderQueries[int(shape)%len(stepWiderQueries)]
		if msg := b.checkStepWider(t, rows, q, start, start.Add(12*time.Minute), step, window); msg != "" {
			t.Fatalf("[%v] step %v start %v: %s", window, step, start, msg)
		}
	})
}
