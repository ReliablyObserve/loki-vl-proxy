package proxy

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
)

// Hardening of the settled steps (query_range_steps.go): the answers of
// refresh sequences against Loki's window (t-range, t], and the parse, merge
// and write-back of the answers the evaluator writes, over generated input.

// qsBase is a whole second in 2023: the steps of a window are kept only once
// older than -query-range-freshness, so the rows must not be in the future.
const qsBase = int64(1_700_000_000) * 1e9

func TestQueryStepsAddSizeRejectsOverflowAndBodyLimit(t *testing.T) {
	maxInt := int(^uint(0) >> 1)
	for _, tc := range []struct {
		name              string
		size, part, limit int
		want              int
		ok                bool
	}{
		{"within limit", 8, 2, 10, 10, true},
		{"body limit", 8, 3, 10, 0, false},
		{"integer overflow", maxInt - 1, 2, maxInt, 0, false},
		{"negative part", 1, -1, maxInt, 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := queryStepsAddSize(tc.size, tc.part, tc.limit)
			if got != tc.want || ok != tc.ok {
				t.Fatalf("queryStepsAddSize(%d, %d, %d) = (%d, %t), want (%d, %t)", tc.size, tc.part, tc.limit, got, ok, tc.want, tc.ok)
			}
		})
	}
}

// qsGenerate draws a case for a refresh sequence: windows longer than, shorter
// than, equal to and not a multiple of the step, an offset now and then, rows
// dense or sparse with a gap that leaves some steps empty, and lines on the
// window edges of every step the sequence reaches and 1ns beside them.
func qsGenerate(rnd *rand.Rand, fn, parser string, shape owShape) owCase {
	steps := []time.Duration{10 * time.Second, 15 * time.Second, 30 * time.Second, time.Minute, 90 * time.Second}
	windows := []time.Duration{30 * time.Second, 45 * time.Second, time.Minute, 90 * time.Second, 150 * time.Second, 5 * time.Minute}
	c := owCase{fn: fn, parser: parser, shape: shape, step: steps[rnd.Intn(len(steps))], window: windows[rnd.Intn(len(windows))]}
	if rnd.Intn(4) == 0 {
		c.offset = time.Duration(1+rnd.Intn(3)) * time.Minute
	}
	nSteps := 8 + rnd.Intn(8)
	c.start = qsBase + int64(rnd.Intn(900))*1e9
	c.end = c.start + int64(nSteps)*int64(c.step)
	lo := c.start - 5*int64(c.step) - int64(c.offset) - int64(c.window) - 5e9
	hi := c.end + 6*int64(c.step) - int64(c.offset) + 5e9
	gapFrom, gapTo := hi, hi // no gap
	if rnd.Intn(2) == 0 {
		gapFrom = c.start + rnd.Int63n(c.end-c.start)
		gapTo = gapFrom + 2*int64(c.window) + int64(c.step)
	}
	values := []float64{0, 0.5, 1, 2.25, 3, 100.125, 7, -4}
	gap := int64(4e9)
	if rnd.Intn(2) == 0 {
		gap = int64(40+rnd.Intn(120)) * 1e9
	}
	for _, pod := range []string{"a", "b", "c"} {
		for ts := lo + rnd.Int63n(gap); ts < hi; ts += gap/2 + rnd.Int63n(gap) {
			row := emuRow{ts: ts, stream: fmt.Sprintf(`{app="uw",pod=%q}`, pod), value: values[rnd.Intn(len(values))]}
			if rnd.Intn(5) == 0 { // on a window edge of a step, or 1ns beside it
				row.ts = c.start - int64(c.offset) + int64(rnd.Intn(nSteps+8)-4)*int64(c.step) - int64(rnd.Intn(2))*int64(c.window) + int64(rnd.Intn(3)-1)
			}
			if row.ts >= gapFrom && row.ts < gapTo {
				continue
			}
			c.rows = append(c.rows, row)
		}
	}
	return c
}

// qsSequence is the windows a dashboard and its user send, in steps from the
// first one: a refresh by one step, by three, a re-run moved back, a window
// widened both ways, the first again, and a start a second off the grid.
var qsSequence = []struct{ from, to int }{{0, 0}, {1, 1}, {3, 3}, {-2, -2}, {-4, 5}, {0, 0}, {-1, 6}}

// TestQueryRangeStepsLokiDifferential: refresh sequences answered with the
// settled steps on are Loki's value at every step (pkg/logql/range_vector.go:102-189
// window (t-range, t], exclusive below; 371-420 sum, avg, max and min over
// time; evaluator.go VectorAggEvaluator.Next for the outer fold) and the
// uncached proxy's bytes, per VictoriaLogs v1.49.0 and v1.53.0, plain, logfmt
// and json, grouped, ungrouped and without, over random windows, steps,
// offsets and gaps. A second tenant sending the first window is never answered
// from the first tenant's steps.
//
// conformance: quality/query-range-settled-steps, semantics/unwrap-over-time-range-windows, window-bounds-and-step-alignment, tenant-numeric-routing
func TestQueryRangeStepsLokiDifferential(t *testing.T) {
	var served, requests, withData int
	tenant := func(id string) http.Header { return http.Header{"X-Scope-Orgid": {id}} }
	shapes := []owShape{owShapes[0], owShapes[1], owShapes[4], owShapes[3]}
	for vi, version := range []string{"v1.49.0", "v1.53.0"} {
		for pi, parser := range []string{"", "logfmt", "json"} {
			for fi, fn := range []string{"sum_over_time", "avg_over_time", "max_over_time", "min_over_time"} {
				for _, shape := range []owShape{shapes[(vi+pi+fi)%4], shapes[(vi+pi+fi+1)%4]} { // two of the four forms
					if !shape.parser(parser, fn) {
						continue
					}
					name := fmt.Sprintf("%s/%s/%s/%s", version, parser, fn, shape.name)
					t.Run(name, func(t *testing.T) {
						rnd := rand.New(rand.NewSource(int64(len(name))*7919 + int64(name[len(name)-3]) + int64(len(parser))))
						for range 2 {
							c := qsGenerate(rnd, fn, parser, shape)
							emuOn := &vlEmulator{t: t, rows: c.rows, field: "amount", version: version}
							emuOff := &vlEmulator{t: t, rows: c.rows, field: "amount", version: version}
							onSrv, _ := newExtentShim(t, emuOn.handler())
							offSrv, _ := newExtentShim(t, emuOff.handler())
							on := newExtentTestProxy(t, onSrv.URL, true, 64<<20)
							off := newExtentTestProxy(t, offSrv.URL, false, 64<<20)
							for _, p := range []*Proxy{on, off} {
								p.backendVersionSemver = "" // newExtentTestProxy stored v1.50.0
								p.storeBackendVersion(version, version)
							}
							step := int64(c.step)
							for i, w := range qsSequence {
								win := c
								win.start, win.end = c.start+int64(w.from)*step, c.end+int64(w.to)*step
								if i == len(qsSequence)-1 {
									win.start, win.end = win.start+1e9, win.end+1e9
								}
								label := fmt.Sprintf("%s window %d %+v: %s step=%v window=%v offset=%v", name, i, w, c.query(), c.step, c.window, c.offset)
								h := tenant("a")
								if i == 1 {
									h = tenant("b") // the first window again, as another tenant
								}
								before := queryStepsOutcomes(t, on)
								wantCode, want := extentQueryRange(t, off, c.query(), time.Unix(0, win.start), time.Unix(0, win.end), c.step, h)
								gotCode, got := extentQueryRange(t, on, c.query(), time.Unix(0, win.start), time.Unix(0, win.end), c.step, h)
								if wantCode != http.StatusOK || gotCode != wantCode || !sameQueryStepsAnswer(got, want) {
									t.Fatalf("%s: %d %.400s\nuncached: %d %.400s", label, gotCode, got, wantCode, want)
								}
								compareOverTime(t, label, win, owParseAnswer(t, win, got), win.want())
								after := queryStepsOutcomes(t, on)
								if i == 1 && after["hit"]+after["partial"] != before["hit"]+before["partial"] {
									t.Fatalf("%s: tenant b was answered from tenant a's steps: %v", label, after)
								}
								requests++
								if bytes.Contains(got, []byte(`"values":[[`)) {
									withData++
								}
							}
							o := queryStepsOutcomes(t, on)
							served += o["hit"] + o["partial"]
						}
					})
				}
			}
		}
	}
	t.Logf("%d requests, %d with data, %d answered from settled steps", requests, withData, served)
	if served < requests/8 || withData < requests/2 {
		t.Fatalf("%d of %d requests had data, %d were answered from settled steps: the run proves little", withData, requests, served)
	}
}

// TestQueryRangeStepsRejectedValueReachedByMovedWindow: a value Loki's
// conversion rejects (pkg/logql/log/metrics_extraction.go: a SampleExtractionErr
// series, a 400 for the query) is met by a window moved back over steps
// already kept, and by one that starts on the step where the value leaves
// the window (t-range, t]: Loki's 400 in the first, the clean answer in the
// second, the kept steps untouched by either.
//
// conformance: quality/query-range-settled-steps, semantics/unwrap-conversion-error, semantics/unwrap-over-time-range-windows
func TestQueryRangeStepsRejectedValueReachedByMovedWindow(t *testing.T) {
	const window = 2 * time.Minute
	t0 := qsBase - qsBase%int64(time.Hour)
	at := func(d time.Duration) int64 { return t0 + int64(d) }
	for _, version := range []string{"v1.49.0", "v1.53.0"} {
		for _, parser := range []string{"logfmt", "json"} {
			t.Run(version+"/"+parser, func(t *testing.T) {
				var rows []emuRow
				for i := 0; i < 40*60/5; i++ {
					for _, pod := range []string{"a", "b"} {
						rows = append(rows, emuRow{ts: at(time.Duration(i) * 5 * time.Second), stream: fmt.Sprintf(`{app="uw",pod=%q}`, pod), value: float64(i%7) + 0.5})
					}
				}
				// Two rejected values, one on the step 12m00s itself.
				rows = append(rows, emuRow{ts: at(10*time.Minute + 3*time.Second), stream: `{app="uw",pod="a"}`, raw: "x7"},
					emuRow{ts: at(12 * time.Minute), stream: `{app="uw",pod="b"}`, raw: "x7"})
				emuOn := &vlEmulator{t: t, rows: rows, field: "amount", version: version}
				emuOff := &vlEmulator{t: t, rows: rows, field: "amount", version: version}
				onSrv, _ := newExtentShim(t, emuOn.handler())
				offSrv, _ := newExtentShim(t, emuOff.handler())
				on := newExtentTestProxy(t, onSrv.URL, true, 64<<20)
				off := newExtentTestProxy(t, offSrv.URL, false, 64<<20)
				for _, p := range []*Proxy{on, off} {
					p.backendVersionSemver = ""
					p.storeBackendVersion(version, version)
				}
				c := owCase{fn: "sum_over_time", parser: parser, shape: owShapes[0], window: window, step: 10 * time.Second, rows: rows}
				// A window of 14m to 30m is clean; one from 12m includes the rejected value at 12m for the
				// steps up to 13m50s, and one from 14m00s does not (the lower bound is exclusive).
				for i, tc := range []struct {
					from, to time.Duration
					rejected bool
				}{{20 * time.Minute, 30 * time.Minute, false}, {9 * time.Minute, 30 * time.Minute, true}, {14 * time.Minute, 30 * time.Minute, false},
					{13*time.Minute + 50*time.Second, 30 * time.Minute, true}, {20 * time.Minute, 30 * time.Minute, false}, {5 * time.Minute, 8 * time.Minute, false}} {
					win := c
					win.start, win.end = at(tc.from), at(tc.to)
					wantCode, want := extentQueryRange(t, off, c.query(), time.Unix(0, win.start), time.Unix(0, win.end), c.step, nil)
					gotCode, got := extentQueryRange(t, on, c.query(), time.Unix(0, win.start), time.Unix(0, win.end), c.step, nil)
					rejected := tc.rejected && parser == "logfmt" // json: the __error__ filter drops the sample
					if rejected != (wantCode == http.StatusBadRequest) || gotCode != wantCode || !sameQueryStepsAnswer(got, want) {
						t.Fatalf("window %d %v..%v: %d %.300s\nuncached: %d %.300s (rejected %v)", i, tc.from, tc.to, gotCode, got, wantCode, want, rejected)
					}
					if rejected {
						if !bytes.Contains(got, []byte("SampleExtractionErr")) {
							t.Fatalf("window %d: %s", i, got)
						}
						continue
					}
					compareOverTime(t, fmt.Sprintf("window %d", i), win, owParseAnswer(t, win, got), win.want())
				}
				if o := queryStepsOutcomes(t, on); o["hit"]+o["partial"] < 1 {
					t.Fatalf("the repeated clean window was not answered from kept steps: %v", o)
				}
			})
		}
	}
}

// qsAnswer is an answer written as the evaluator writes it, with its grid.
type qsAnswer struct {
	grid     queryStepsGrid
	from, to int64
	body     []byte
	series   int
}

var qsValues = []string{"1", "2.5", "NaN", "+Inf", "-Inf", "0", "1e+06", "-0.25", "123456789.123"}
var qsLabelValues = []string{"a", "b", `x"y`, `back\slash`, "é", "", "a,b", `}{`}

// qsTimestamp writes a timestamp in seconds, whole or with the decimals it has,
// or cut to whole seconds the way the evaluator may write a grid off them.
func qsTimestamp(ns int64, cut bool) string {
	if cut {
		ns -= ns % int64(time.Second)
	}
	if frac := ns % int64(time.Second); frac != 0 {
		return fmt.Sprintf("%d.%s", ns/int64(time.Second), strings.TrimRight(fmt.Sprintf("%09d", frac), "0"))
	}
	return strconv.FormatInt(ns/int64(time.Second), 10)
}

// qsGenerateAnswer draws a matrix answer of the settled steps: a grid
// queryStepsFor accepts (a step of four seconds or more, or whole seconds), a
// run of steps, series sorted by one of the two orders the evaluator writes,
// each with points on some of the steps, in either envelope.
func qsGenerateAnswer(rnd *rand.Rand) qsAnswer {
	seconds := []int64{1, 2, 4, 5, 10, 15, 60}
	g := queryStepsGrid{step: seconds[rnd.Intn(len(seconds))] * int64(time.Second)}
	switch {
	case g.step >= 4*int64(time.Second):
		g.phase = int64(rnd.Intn(4)) * int64(250*time.Millisecond)
	case g.step == 2*int64(time.Second):
		g.phase = int64(rnd.Intn(2)) * int64(time.Second)
	}
	a := qsAnswer{grid: g, from: qsBase / g.step}
	a.to = a.from + int64(rnd.Intn(12))
	cut := rnd.Intn(2) == 0
	type series struct {
		metric []byte
		keys   queryStepsSeriesKeys
		text   string
	}
	var all []series
	seen := map[string]bool{}
	for i, n := 0, rnd.Intn(6); i < n; i++ {
		labels := map[string]string{}
		for _, name := range []string{"app", "env", "level", "pod"} {
			if rnd.Intn(2) == 0 {
				labels[name] = qsLabelValues[rnd.Intn(len(qsLabelValues))]
			}
		}
		metric, _ := json.Marshal(labels)
		keys, _ := queryStepsKeysOf(metric)
		if seen[keys.labels] {
			continue
		}
		seen[keys.labels] = true
		var points []string
		for k := a.from; k <= a.to; k++ {
			if rnd.Intn(3) != 0 {
				points = append(points, fmt.Sprintf(`[%s,%q]`, qsTimestamp(g.at(k), cut), qsValues[rnd.Intn(len(qsValues))]))
			}
		}
		if len(points) == 0 {
			continue
		}
		all = append(all, series{metric, keys, `{"metric":` + string(metric) + `,"values":[` + strings.Join(points, ",") + `]}`})
	}
	byFold := rnd.Intn(2) == 0
	sort.Slice(all, func(i, j int) bool {
		if byFold {
			return all[i].keys.fold < all[j].keys.fold
		}
		return all[i].keys.labels < all[j].keys.labels
	})
	texts := make([]string, len(all))
	for i, s := range all {
		texts[i] = s.text
	}
	head, tail := `{"status":"success","data":{"resultType":"matrix","result":[`, `]}}`
	if rnd.Intn(2) == 0 {
		head, tail = `{"data":{"result":[`, `],"resultType":"matrix"},"status":"success"}`
	}
	a.body = []byte(head + strings.Join(texts, ",") + tail)
	a.series = len(all)
	return a
}

// qsStamp names a parsed answer's route and headers as the evaluation does.
func qsStamp(f *statsExtentFragment, route string) {
	f.names = append(f.names, route, "")
}

// qsEqualSeries reports whether two fragments hold the same series and points.
func qsEqualSeries(a, b *statsExtentFragment) bool {
	if len(a.series) != len(b.series) {
		return false
	}
	for i := range a.series {
		x, y := a.series[i], b.series[i]
		if !bytes.Equal(x.metric, y.metric) || len(x.points) != len(y.points) {
			return false
		}
		for j := range x.points {
			if x.points[j].k != y.points[j].k || !bytes.Equal(x.points[j].raw, y.points[j].raw) {
				return false
			}
		}
	}
	return true
}

// checkQueryStepsMerge takes a generated answer through the chain a kept step
// takes: parse, stamp, store (encode, decode), split at a step into a stored
// and a freshly evaluated part (each side either, the fresh one written and
// parsed again), merge. The merged answer is the whole answer byte for byte or
// a refusal; a refusal is allowed only where the merge cannot know the series
// order (several series, none of the fresh parts holds two) or the pieces
// overlap on a point or come from different routes. It reports whether the
// answer was rendered.
func checkQueryStepsMerge(t testing.TB, p *Proxy, seed int64, mode uint8) (rendered bool) {
	t.Helper()
	rnd := rand.New(rand.NewSource(seed))
	a := qsGenerateAnswer(rnd)
	whole, ok := parseQueryStepsAnswer(a.body, a.grid, a.from, a.to)
	if !ok {
		t.Fatalf("seed %d: an answer written as the evaluator writes it is refused: %s (grid %+v, steps %d..%d)", seed, a.body, a.grid, a.from, a.to)
	}
	qsStamp(whole, "route")
	encoded, err := encodeStatsExtentChunk(whole)
	if err != nil {
		t.Fatalf("seed %d: %v", seed, err)
	}
	stored, err := decodeStatsExtentChunk(encoded)
	if err != nil || stored.from != whole.from || stored.to != whole.to || !qsEqualSeries(whole, stored) || !slicesEqual(whole.names, stored.names) {
		t.Fatalf("seed %d: the kept entry is not the answer: %v", seed, err)
	}
	cut := a.from + int64(rnd.Intn(int(a.to-a.from)+1)) // the first step of the right part
	left, right := [2]int64{a.from, cut - 1}, [2]int64{cut, a.to}
	overlap := mode&4 != 0 && cut-1 >= a.from
	if overlap {
		left[1] = cut // the step cut is in both parts
	}
	var pieces, fresh []*statsExtentFragment
	for i, rg := range [][2]int64{left, right} {
		if rg[1] < rg[0] {
			continue
		}
		piece := stored.slice(rg[0], rg[1])
		if mode&(1<<i) != 0 { // evaluated afresh: written as an answer, parsed again
			out := []byte(piece.names[0])
			for j, s := range piece.series {
				out = appendQueryStepsSeries(out, j, s)
			}
			f, ok := parseQueryStepsAnswer(append(out, piece.names[1]...), a.grid, rg[0], rg[1])
			if !ok {
				t.Fatalf("seed %d: the part %v written as an answer is refused: %s", seed, rg, out)
			}
			qsStamp(f, "route")
			if mode&8 != 0 && i == 1 {
				f.names[2] = "other" // an answer of another route
			}
			piece = f
			fresh = append(fresh, f)
		}
		pieces = append(pieces, piece)
	}
	out, ok := p.renderQuerySteps(context.Background(), "key", pieces, fresh)
	otherRoute := mode&8 != 0 && mode&2 != 0 && len(pieces) == 2
	switch {
	case ok && !bytes.Equal(out, a.body):
		t.Fatalf("seed %d mode %d: the merged answer differs from the whole\nmerged: %s\nwhole:  %s", seed, mode, out, a.body)
	case ok && otherRoute:
		t.Fatalf("seed %d mode %d: merged answers of two routes: %s", seed, mode, out)
	case !ok && a.series == 1 && !otherRoute && !overlap:
		t.Fatalf("seed %d mode %d: refused a single series split at step %d: %s", seed, mode, cut, a.body)
	}
	return ok
}

func slicesEqual(a, b []string) bool {
	return len(a) == len(b) && strings.Join(a, "\x00") == strings.Join(b, "\x00")
}

// checkQueryStepsRaw gives arbitrary bytes to the parser: it never panics, and
// an answer it takes apart is one it writes back exactly, with every point on
// a step of the range, in order, once per series, and a timestamp within a
// second of its step.
func checkQueryStepsRaw(t testing.TB, raw []byte, grid queryStepsGrid, from, to int64) {
	t.Helper()
	f, ok := parseQueryStepsAnswer(raw, grid, from, to)
	if !ok {
		return
	}
	out := []byte(f.names[0])
	for i, s := range f.series {
		out = appendQueryStepsSeries(out, i, s)
	}
	if !bytes.Equal(append(out, f.names[1]...), raw) {
		t.Fatalf("taken apart and not written back: %q", raw)
	}
	seen := map[string]bool{}
	for _, s := range f.series {
		if seen[string(s.metric)] || len(s.points) == 0 {
			t.Fatalf("series %s twice or without a point: %q", s.metric, raw)
		}
		seen[string(s.metric)] = true
		prev := int64(math.MinInt64)
		for _, pt := range s.points {
			text, _, _ := bytes.Cut(bytes.TrimPrefix(pt.raw, []byte("[")), []byte(","))
			ns, ok := queryStepsSecondsToNanos(text)
			if d := ns - grid.at(pt.k); !ok || pt.k < from || pt.k > to || pt.k <= prev || d <= -int64(time.Second) || d >= int64(time.Second) {
				t.Fatalf("point %s of %s at step %d (range %d..%d) kept: %q", pt.raw, s.metric, pt.k, from, to, raw)
			}
			prev = pt.k
		}
	}
}

// TestQueryStepsMergeMatchesWhole: over generated answers (both envelopes and
// orders, whole, fractional and cut timestamps, NaN and infinite values,
// labels that need escaping), splitting at any step into a kept and a fresh
// part, in either role, and merging gives the whole answer byte for byte; the
// pieces of two routes, and the parts that overlap on a point, are refused.
//
// conformance: quality/query-range-settled-steps
func TestQueryStepsMergeMatchesWhole(t *testing.T) {
	srv, _ := newExtentShim(t, http.NotFoundHandler())
	p := newExtentTestProxy(t, srv.URL, true, 1<<20)
	rendered, several := 0, 0
	for seed := int64(0); seed < 2000; seed++ {
		if checkQueryStepsMerge(t, p, seed, uint8(seed%16)) {
			rendered++
			if a := qsGenerateAnswer(rand.New(rand.NewSource(seed))); a.series > 1 {
				several++
			}
		}
	}
	t.Logf("%d of 2000 merged, %d with several series", rendered, several)
	if rendered < 500 || several < 300 {
		t.Fatalf("only %d answers merged, %d with several series: the run proves little", rendered, several)
	}
}

// FuzzQueryStepsRoundTrip: arbitrary bytes never panic the parse, and what is
// taken apart is written back exactly (checkQueryStepsRaw); generated answers
// split at a step and merged are the whole answer or a refusal
// (checkQueryStepsMerge).
//
// conformance: quality/query-range-settled-steps
func FuzzQueryStepsRoundTrip(f *testing.F) {
	for i, tc := range queryStepsRoundTripCases {
		f.Add([]byte(tc.body), uint32(tc.g.step/int64(time.Millisecond)), uint32(tc.g.phase/int64(time.Millisecond)), tc.from, uint8(tc.to-tc.from), int64(i), uint8(i))
	}
	f.Add([]byte(nil), uint32(1000), uint32(0), int64(0), uint8(0), int64(7), uint8(15))
	f.Add([]byte(`{"status":"success","data":{"resultType":"matrix","result":[{"metric":{},"values":[[0.5,"1"],[1.5,"2"]]}]}}`), uint32(1000), uint32(500), int64(0), uint8(3), int64(8), uint8(5))
	srv, _ := newExtentShim(f, http.NotFoundHandler())
	p := newExtentTestProxy(f, srv.URL, true, 1<<20)
	f.Fuzz(func(t *testing.T, raw []byte, stepMs, phaseMs uint32, from int64, span uint8, seed int64, mode uint8) {
		if len(raw) > 64<<10 {
			t.Skip()
		}
		grid := queryStepsGrid{step: (int64(stepMs)%3_600_000 + 1) * int64(time.Millisecond)}
		grid.phase = int64(phaseMs) % (grid.step / int64(time.Millisecond)) * int64(time.Millisecond)
		from %= 1 << 40
		checkQueryStepsRaw(t, raw, grid, from, from+int64(span))
		checkQueryStepsMerge(t, p, seed, mode)
	})
}
