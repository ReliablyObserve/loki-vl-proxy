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
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/testutil/vlemu"
)

// Sub-second steps. Grafana sends a step of 200 ms for a short panel, and Loki
// answers a point every 200 ms: its evaluation times are start + k*step with
// the step in whole milliseconds, and its JSON writes each time as seconds with
// a millisecond fraction.
//
// The reference below is written from Loki v3.7.9, apart from the proxy:
//   - pkg/loghttp/params.go:122-129, 163-186, 191-203 (step: float seconds
//     scaled by float64(time.Second) and truncated to a Duration, else a
//     Prometheus duration of whole units; a timestamp with a dot is float
//     seconds whose fraction is rounded to a millisecond, an integer of at most
//     ten digits is seconds, else nanoseconds);
//   - pkg/loghttp/query.go:473-515 (step <= 0 is 400, and so is a range of more
//     than 11,000 steps, the quotient of two durations in nanoseconds);
//   - pkg/querier/queryrange/codec.go:2265-2284 (Step is Milliseconds(), a
//     truncation);
//   - pkg/logql/range_vector.go:42-89, 189-204 (the times are start,
//     start+step, ... up to and including end; the sample time is current/1e6,
//     a truncation to milliseconds; a sample at t covers (t-range, t]);
//   - pkg/util/marshal/query.go (the time is model.Time(ms).String(), that is
//     FormatFloat(ms/1000, 'f', -1, 64)).

// lokiMs is Loki's time of an evaluation at ns: whole milliseconds, truncated.
func lokiMs(ns int64) int64 { return vlemu.FloorDiv(ns, int64(time.Millisecond)) }

// lokiTimeText is the text Loki writes for a time in milliseconds.
func lokiTimeText(ms int64) string { return strconv.FormatFloat(float64(ms)/1000, 'f', -1, 64) }

// lokiEvalTimes are the evaluation times (ns) of a range query: Loki keeps the
// step in whole milliseconds and walks from start to end inclusive.
func lokiEvalTimes(startNs, endNs, stepNs int64) []int64 {
	stepMs := stepNs / int64(time.Millisecond)
	if stepMs <= 0 {
		return nil
	}
	var out []int64
	for t := startNs; t <= endNs; t += stepMs * int64(time.Millisecond) {
		out = append(out, t)
	}
	return out
}

var promDurationRE = regexp.MustCompile(`^(?:(\d+)y)?(?:(\d+)w)?(?:(\d+)d)?(?:(\d+)h)?(?:(\d+)m)?(?:(\d+)s)?(?:(\d+)ms)?$`)

// lokiStep is Loki's parseSecondsOrDuration (pkg/loghttp/params.go:191-203).
// A Prometheus duration has no fractions and no unit below the millisecond.
func lokiStep(text string) (time.Duration, bool) {
	if f, err := strconv.ParseFloat(text, 64); err == nil {
		d := f * float64(time.Second)
		if d > float64(math.MaxInt64) || d < float64(math.MinInt64) {
			return 0, false
		}
		return time.Duration(d), true
	}
	m := promDurationRE.FindStringSubmatch(text)
	if m == nil || text == "" {
		return 0, false
	}
	mult := []time.Duration{365 * 24 * time.Hour, 7 * 24 * time.Hour, 24 * time.Hour, time.Hour, time.Minute, time.Second, time.Millisecond}
	var total time.Duration
	for i, part := range m[1:] {
		if part != "" {
			n, _ := strconv.ParseInt(part, 10, 64)
			total += time.Duration(n) * mult[i]
		}
	}
	return total, true
}

// lokiTimestamp is Loki's parseTimestamp (pkg/loghttp/params.go:163-186).
func lokiTimestamp(text string) (int64, bool) {
	if strings.Contains(text, ".") {
		if f, err := strconv.ParseFloat(text, 64); err == nil {
			whole, ns := math.Modf(f)
			ns = math.Round(ns*1000) / 1000
			return int64(whole)*int64(time.Second) + int64(ns*float64(time.Second)), true
		}
	}
	n, err := strconv.ParseInt(text, 10, 64)
	if err != nil {
		parsed, err := time.Parse(time.RFC3339Nano, text)
		return parsed.UnixNano(), err == nil
	}
	if len(text) <= 10 {
		return n * int64(time.Second), true
	}
	return n, true
}

// vlDurationNs reads a duration the way VictoriaLogs does (lib/logstorage
// tryParseDuration, identical in v1.49.0 and v1.53.0): one or more
// <float><unit> parts, every part truncated to whole nanoseconds on its own.
// A bare number has no unit and is refused.
func vlDurationNs(s string) (int64, bool) {
	neg := strings.HasPrefix(s, "-")
	s = strings.TrimPrefix(s, "-")
	if s == "" {
		return 0, false
	}
	units := []struct {
		name string
		ns   float64
	}{{"µs", 1e3}, {"ms", 1e6}, {"ns", 1}, {"y", 365 * 24 * 3600e9}, {"w", 7 * 24 * 3600e9}, {"d", 24 * 3600e9}, {"h", 3600e9}, {"m", 60e9}, {"s", 1e9}}
	var total int64
	for s != "" {
		n := 0
		for n < len(s) && (s[n] >= '0' && s[n] <= '9' || s[n] == '.') {
			n++
		}
		f, err := strconv.ParseFloat(s[:n], 64)
		if n == 0 || err != nil {
			return 0, false
		}
		s = s[n:]
		matched := false
		for _, u := range units {
			if strings.HasPrefix(s, u.name) {
				total += int64(f * u.ns)
				s = s[len(u.name):]
				matched = true
				break
			}
		}
		if !matched {
			return 0, false
		}
	}
	if neg {
		total = -total
	}
	return total, true
}

// vlTimestampNs reads a start or end as VictoriaLogs does (VictoriaMetrics
// lib/timeutil ParseTimeAt, v1.49.0 and v1.53.0): RFC 3339, or a unix
// timestamp of decimal digits whose unit follows from its magnitude (seconds
// below 2^31, then milliseconds, microseconds, nanoseconds), a fraction
// counting as more digits, exactly.
func vlTimestampNs(s string) (int64, bool) {
	if strings.Contains(s, "-") {
		parsed, err := time.Parse(time.RFC3339Nano, s)
		return parsed.UnixNano(), err == nil
	}
	whole, frac, _ := strings.Cut(s, ".")
	for len(frac)%3 != 0 {
		frac += "0"
	}
	n, err := strconv.ParseInt(whole+frac, 10, 64)
	if err != nil {
		return 0, false
	}
	switch {
	case n < 1<<31:
		return n * 1e9, true
	case n < 1e3*(1<<31):
		return n * 1e6, true
	case n < 1e6*(1<<31):
		return n * 1e3, true
	}
	return n, true
}

// statsByTimeRE is the bucketed count the proxy reads through /query:
// `| stats by (_time:<N>ns offset <M>ns[, pod]) count() as c`.
var statsByTimeRE = regexp.MustCompile(`\| stats by \(_time:(\d+)ns offset (-?\d+)ns(, pod)?\) count\(\) as c$`)

// strictVL stands in front of the emulated VictoriaLogs and holds it to what
// the real one accepts: the step and offset of a stats call must read as a
// VictoriaLogs duration, which it forwards as exact nanoseconds, the unit
// VictoriaLogs buckets by. It keeps what it read for the test to assert.
type strictVL struct {
	t     testing.TB
	next  http.Handler
	emu   *vlemu.Emulator
	mu    sync.Mutex
	steps []int64 // the ns steps of the stats_query_range calls
	// lastEvalNs is the last evaluation time of the request; a stats read must
	// reach past it (VictoriaLogs' end is exclusive).
	lastEvalNs int64
	short      []string // reads that stop before lastEvalNs
}

// serveStatsByTime answers the bucketed count the way VictoriaLogs does: one
// line per bucket [edge, edge+N), edges at k*N-M, labelled by the edge.
func (s *strictVL) serveStatsByTime(w http.ResponseWriter, r *http.Request, m []string) {
	size, _ := strconv.ParseInt(m[1], 10, 64)
	offset, _ := strconv.ParseInt(m[2], 10, 64)
	type bucket struct {
		edge int64
		pod  string
	}
	counts := map[bucket]int{}
	var order []bucket
	for _, row := range s.emu.SelectRows(r) {
		b := bucket{edge: vlemu.FloorDiv(row.TS+offset, size)*size - offset}
		if m[3] != "" {
			b.pod = strings.TrimSuffix(row.Stream[strings.Index(row.Stream, `pod="`)+5:], `"}`)
		}
		if _, ok := counts[b]; !ok {
			order = append(order, b)
		}
		counts[b]++
	}
	w.Header().Set("Content-Type", "application/x-ndjson")
	for _, b := range order {
		line := map[string]string{"_time": time.Unix(0, b.edge).UTC().Format(time.RFC3339Nano), "c": strconv.Itoa(counts[b])}
		if m[3] != "" {
			line["pod"] = b.pod
		}
		enc, _ := json.Marshal(line)
		_, _ = w.Write(append(enc, '\n'))
	}
}

func (s *strictVL) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	_ = r.ParseForm()
	if r.URL.Path == "/select/logsql/query" {
		if m := statsByTimeRE.FindStringSubmatch(r.FormValue("query")); m != nil {
			s.emu.Record(r.URL.Path, r.FormValue("query"))
			s.serveStatsByTime(w, r, m)
			return
		}
	}
	if r.URL.Path == "/select/logsql/stats_query_range" {
		raw := r.FormValue("step")
		ns, ok := vlDurationNs(raw)
		if !ok || ns <= 0 {
			http.Error(w, fmt.Sprintf("cannot parse duration from the arg 'step=%s'", raw), http.StatusBadRequest)
			return
		}
		form := url.Values{}
		for k, v := range r.Form {
			form[k] = v
		}
		form.Set("step", strconv.FormatInt(ns, 10)+"ns")
		if off := r.FormValue("offset"); off != "" {
			offNs, ok := vlDurationNs(off)
			if !ok {
				http.Error(w, fmt.Sprintf("cannot parse duration from the arg 'offset=%s'", off), http.StatusBadRequest)
				return
			}
			form.Set("offset", strconv.FormatInt(offNs, 10)+"ns")
		}
		// VictoriaLogs reads start and end as it does: fractional seconds are
		// exact decimal digits. The emulator reads RFC 3339 exactly.
		endNs := int64(math.MaxInt64)
		for _, name := range []string{"start", "end"} {
			if raw := r.FormValue(name); raw != "" {
				ns, ok := vlTimestampNs(raw)
				if !ok {
					http.Error(w, fmt.Sprintf("cannot parse %s=%s", name, raw), http.StatusBadRequest)
					return
				}
				if name == "end" {
					endNs = ns
				}
				form.Set(name, time.Unix(0, ns).UTC().Format(time.RFC3339Nano))
			}
		}
		s.mu.Lock()
		s.steps = append(s.steps, ns)
		if s.lastEvalNs != 0 && endNs <= s.lastEvalNs {
			s.short = append(s.short, fmt.Sprintf("stats_query_range end=%s (%dns) is not past the last evaluation time %d", r.FormValue("end"), endNs, s.lastEvalNs))
		}
		s.mu.Unlock()
		r2 := r.Clone(r.Context())
		r2.Form, r2.PostForm = form, form
		r = r2
	}
	s.next.ServeHTTP(w, r)
}

// ssKind is a query shape of the sub-second differential.
type ssKind struct {
	name  string
	query func(window string) string
	// value is the shape's sample for one series at one time, from the rows of
	// that series that fall in the window (t-window, t]; false is no sample.
	value func(rows []vlemu.Row, windowSeconds float64) (float64, bool)
	// groupBy: the answer is one series per pod ("by (pod)" or per stream);
	// otherwise one series that folds every pod (an ungrouped sum).
	groupBy bool
}

var ssKinds = []ssKind{
	{
		name:  "sum-rate-unwrap",
		query: func(w string) string { return fmt.Sprintf(`sum(rate({app="uw"} | unwrap amount [%s]))`, w) },
		value: func(rows []vlemu.Row, ws float64) (float64, bool) {
			var sum float64
			for _, r := range rows {
				sum += r.Value
			}
			return sum / ws, len(rows) > 0
		},
	},
	{
		name:  "sum-count-over-time",
		query: func(w string) string { return fmt.Sprintf(`sum(count_over_time({app="uw"}[%s]))`, w) },
		value: func(rows []vlemu.Row, _ float64) (float64, bool) { return float64(len(rows)), len(rows) > 0 },
	},
	{
		name:    "sum-by-pod-count-over-time",
		query:   func(w string) string { return fmt.Sprintf(`sum by (pod) (count_over_time({app="uw"}[%s]))`, w) },
		value:   func(rows []vlemu.Row, _ float64) (float64, bool) { return float64(len(rows)), len(rows) > 0 },
		groupBy: true,
	},
	{
		name: "avg-over-time-unwrap-by-pod",
		query: func(w string) string {
			return fmt.Sprintf(`avg_over_time({app="uw"} | unwrap amount [%s]) by (pod)`, w)
		},
		value: func(rows []vlemu.Row, _ float64) (float64, bool) {
			var sum float64
			for _, r := range rows {
				sum += r.Value
			}
			return sum / float64(len(rows)), len(rows) > 0
		},
		groupBy: true,
	},
	{
		name:    "rate-per-stream",
		query:   func(w string) string { return fmt.Sprintf(`rate({app="uw"}[%s])`, w) },
		value:   func(rows []vlemu.Row, ws float64) (float64, bool) { return float64(len(rows)) / ws, len(rows) > 0 },
		groupBy: true,
	},
}

// ssScenario is one request and the rows behind it. The bounds and the step
// are the texts a client sends; the evaluation times follow from them as Loki
// reads them.
type ssScenario struct {
	kind                     ssKind
	startText, endText, step string
	window                   time.Duration
	instant                  bool // the time is endText
	align                    bool // -align-queries-with-step: the bounds move down to the step grid
	rows                     []vlemu.Row
}

func (s ssScenario) startNs() int64 { n, _ := lokiTimestamp(s.startText); return n }
func (s ssScenario) endNs() int64   { n, _ := lokiTimestamp(s.endText); return n }

// lokiStepNs is the step Loki evaluates on: the text read as Loki reads it, in
// whole milliseconds.
func (s ssScenario) lokiStepNs() int64 {
	d, _ := lokiStep(s.step)
	return d.Milliseconds() * int64(time.Millisecond)
}

func ssWindowText(d time.Duration) string {
	if d%time.Second == 0 && d >= time.Second {
		return strconv.FormatInt(int64(d/time.Second), 10) + "s"
	}
	return strconv.FormatInt(int64(d/time.Millisecond), 10) + "ms"
}

// beyondBucketBudget reports whether the buckets that hold every window edge
// (the gcd of step and window wide, over range plus window) outnumber what the
// proxy asks VictoriaLogs for in one call (heavyQueryBucketThreshold). Past it
// the raw-sample evaluator answers, bounded by its row limit: an existing
// design for a window far wider than its step, here reached by sub-second
// steps at much shorter ranges.
func (s ssScenario) beyondBucketBudget() bool {
	step, window := time.Duration(s.lokiStepNs()), s.window
	if s.instant {
		return false
	}
	bucket, rest := step, window
	for rest > 0 {
		bucket, rest = rest, bucket%rest
	}
	if bucket < time.Millisecond {
		return true
	}
	if window < step {
		bucket = step // disjoint windows: one bucket per step
	}
	start, end := s.evalBounds()
	return (time.Duration(end-start)+window)/bucket > heavyQueryBucketThreshold
}

// evalBounds are the bounds a range query is evaluated between: as sent, or,
// with alignment, each moved down to a multiple of the step in milliseconds
// after truncating it to milliseconds (Loki's StepAlignMiddleware,
// queryrangebase/step_align.go:20-24).
func (s ssScenario) evalBounds() (int64, int64) {
	start, end := s.startNs(), s.endNs()
	if s.align && !s.instant {
		stepMs := s.lokiStepNs() / int64(time.Millisecond)
		start = lokiMs(start) / stepMs * stepMs * int64(time.Millisecond)
		end = lokiMs(end) / stepMs * stepMs * int64(time.Millisecond)
	}
	return start, end
}

func (s ssScenario) times() []int64 {
	if s.instant {
		return []int64{s.endNs()}
	}
	start, end := s.evalBounds()
	return lokiEvalTimes(start, end, s.lokiStepNs())
}

// expected is Loki's answer: series -> time text -> value.
func (s ssScenario) expected() map[string]map[string]float64 {
	out := map[string]map[string]float64{}
	ws := s.window.Seconds()
	for _, t := range s.times() {
		var inWindow []vlemu.Row
		for _, r := range s.rows {
			if r.TS > t-int64(s.window) && r.TS <= t {
				inWindow = append(inWindow, r)
			}
		}
		add := func(key string, rows []vlemu.Row) {
			v, ok := s.kind.value(rows, ws)
			if !ok {
				return
			}
			if out[key] == nil {
				out[key] = map[string]float64{}
			}
			out[key][lokiTimeText(lokiMs(t))] = v
		}
		if !s.kind.groupBy {
			add("", inWindow)
			continue
		}
		for _, pod := range []string{"a", "b"} {
			var rows []vlemu.Row
			for _, r := range inWindow {
				if r.Stream == fmt.Sprintf(`{app="uw",pod=%q}`, pod) {
					rows = append(rows, r)
				}
			}
			add(pod, rows)
		}
	}
	return out
}

func (s ssScenario) label() string {
	mode := "range"
	if s.instant {
		mode = "instant"
	}
	mode += map[bool]string{true: " aligned", false: ""}[s.align]
	return fmt.Sprintf("%s %s step=%q window=%s start=%q end=%q rows=%d", mode, s.kind.name, s.step, ssWindowText(s.window), s.startText, s.endText, len(s.rows))
}

// ssResult is what the proxy answered.
type ssResult struct {
	series map[string]map[string]float64 // series -> time text -> value
	strict *strictVL
	code   int
	body   string
}

// run sends the scenario through the real handler against the emulated
// VictoriaLogs.
func (s ssScenario) run(t *testing.T, version string) ssResult {
	t.Helper()
	emu := &vlemu.Emulator{T: t, Rows: s.rows, Field: "amount", Version: version, FloatPhase: true}
	strict := &strictVL{t: t, next: emu.Handler(), emu: emu}
	if times := s.times(); len(times) > 0 {
		strict.lastEvalNs = times[len(times)-1]
	}
	backend := httptest.NewServer(strict)
	defer backend.Close()
	p, err := New(Config{BackendURL: backend.URL, Cache: cache.New(60*time.Second, 1000), LogLevel: "error", BackendMinVersion: "v1.40.0", BackendVersionCheckTimeout: time.Second, AlignQueriesWithStep: s.align})
	if err != nil {
		t.Fatal(err)
	}
	if version != "" {
		if err := p.ValidateBackendVersionCompatibility(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	params := url.Values{"query": {s.kind.query(ssWindowText(s.window))}}
	target := "/loki/api/v1/query_range?"
	if s.instant {
		target = "/loki/api/v1/query?"
		params.Set("time", s.endText)
	} else {
		params.Set("start", s.startText)
		params.Set("end", s.endText)
		params.Set("step", s.step)
	}
	res := doCompatProxyRequest(p, target+params.Encode(), map[string]string{"X-Scope-OrgID": "0"})
	out := ssResult{strict: strict, code: res.Code, body: res.Body.String()}
	if res.Code != http.StatusOK {
		return out
	}
	// The times are read as the text the proxy wrote, not as numbers.
	dec := json.NewDecoder(strings.NewReader(out.body))
	dec.UseNumber()
	var resp struct {
		Data struct {
			Result []struct {
				Metric map[string]string `json:"metric"`
				Values [][]any           `json:"values"`
				Value  []any             `json:"value"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := dec.Decode(&resp); err != nil {
		t.Fatalf("%s: %v\n%s", s.label(), err, out.body)
	}
	out.series = map[string]map[string]float64{}
	for _, series := range resp.Data.Result {
		points := series.Values
		if series.Value != nil {
			points = [][]any{series.Value}
		}
		key := series.Metric["pod"]
		if out.series[key] == nil {
			out.series[key] = map[string]float64{}
		}
		for _, point := range points {
			v, _ := strconv.ParseFloat(point[1].(string), 64)
			text := fmt.Sprint(point[0])
			if _, dup := out.series[key][text]; dup {
				t.Fatalf("%s: series %q repeats the time %s\n%s", s.label(), key, text, out.body)
			}
			out.series[key][text] = v
		}
	}
	return out
}

// check holds the answer to Loki's: the same series, the same times as text,
// the same values.
func (s ssScenario) check(t *testing.T, version string) ssResult {
	t.Helper()
	res := s.run(t, version)
	if res.code != http.StatusOK {
		t.Fatalf("%s: HTTP %d %s", s.label(), res.code, res.body)
	}
	if len(res.strict.short) > 0 {
		t.Fatalf("%s: VictoriaLogs was asked to stop before the last evaluation time: %s", s.label(), res.strict.short[0])
	}
	// Every bucket VictoriaLogs was asked for is a whole number of milliseconds
	// as it reads the step (a bare 1.001s reads as 1000999999ns there).
	for _, ns := range res.strict.steps {
		if ns%int64(time.Millisecond) != 0 {
			t.Fatalf("%s: VictoriaLogs read a step of %dns, not whole milliseconds", s.label(), ns)
		}
	}
	want := s.expected()
	for key, pts := range want {
		for ts, w := range pts {
			g, ok := res.series[key][ts]
			if !ok {
				t.Fatalf("%s: series %q has no point at %s, Loki has %g\nproxy %s", s.label(), key, ts, w, res.body)
			}
			if math.Abs(g-w) > 1e-9*math.Max(1, math.Abs(w)) {
				t.Fatalf("%s: series %q at %s got %g, Loki has %g", s.label(), key, ts, g, w)
			}
		}
	}
	for key, pts := range res.series {
		for ts, g := range pts {
			if _, ok := want[key][ts]; !ok {
				t.Fatalf("%s: series %q has an extra point at %s = %g, Loki has none\nproxy %s", s.label(), key, ts, g, res.body)
			}
		}
	}
	if raw := res.strict.emu.EndpointCalls("/select/logsql/query"); len(raw) > 0 && !s.beyondBucketBudget() {
		t.Fatalf("%s: the metric read raw rows within the bucket budget: %s", s.label(), raw[0])
	}
	return res
}

// ssRows are rows over [lo, hi): random times, and some on a window edge or one
// nanosecond either side of it.
func ssRows(rnd *rand.Rand, s ssScenario, stepNs, lo, hi int64) []vlemu.Row {
	streams := []string{`{app="uw",pod="a"}`, `{app="uw",pod="b"}`}
	values := []float64{0, 0.5, 1, 2.25, 3, 100.125, 7}
	var rows []vlemu.Row
	n := 8 + rnd.Intn(60)
	for i := 0; i < n; i++ {
		ts := lo + rnd.Int63n(hi-lo)
		if rnd.Intn(4) == 0 {
			edge := s.startNs() + rnd.Int63n(8)*stepNs
			if rnd.Intn(2) == 0 {
				edge -= int64(s.window)
			}
			// On the edge or a millisecond either side: VictoriaLogs reads a time
			// as float64, so a line a few nanoseconds off an edge is not told apart
			// from one on it (windowPhaseFilter).
			ts = edge + int64(rnd.Intn(3)-1)*int64(time.Millisecond)
		}
		rows = append(rows, vlemu.Row{TS: ts, Stream: streams[rnd.Intn(2)], Value: values[rnd.Intn(len(values))]})
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].TS < rows[j].TS })
	return rows
}

func ssBase() int64 { return int64(1_793_653_187_704) * int64(time.Millisecond) }

// ssTimestampText writes ns the way a client does: nanoseconds, float seconds
// to the millisecond or the microsecond (Loki rounds the fraction to the
// millisecond), or RFC 3339.
func ssTimestampText(ns int64, form int) string {
	switch form {
	case 1:
		return fmt.Sprintf("%d.%03d", ns/1e9, ns%1e9/1e6)
	case 2:
		return fmt.Sprintf("%d.%06d", ns/1e9, ns%1e9/1e3)
	case 3:
		return time.Unix(0, ns).UTC().Format(time.RFC3339Nano)
	}
	return strconv.FormatInt(ns, 10)
}

// ssStepText writes a step of ms milliseconds as a client does: a Prometheus
// duration, or float seconds, which may carry microseconds beyond the
// millisecond (Loki keeps whole milliseconds of it).
func ssStepText(ms int64, asDuration bool, extraUs int64) string {
	if asDuration {
		return strconv.FormatInt(ms, 10) + "ms"
	}
	return strconv.FormatFloat(float64(ms*1000+extraUs)/1e6, 'f', -1, 64)
}

// ssGenerate draws a request: a step of 1 ms to 3 s, a window shorter than,
// equal to, or longer than the step, a start on or off the step grid, on the
// millisecond or the nanosecond, written in one of four forms, up to 12 steps.
func ssGenerate(rnd *rand.Rand, kind ssKind, instant, align bool) ssScenario {
	stepsMs := []int64{1, 7, 20, 100, 200, 250, 500, 999, 1000, 1001, 1003, 1500, 2000, 3000}
	stepMs := stepsMs[rnd.Intn(len(stepsMs))]
	var extraUs int64
	asDuration := rnd.Intn(2) == 0
	if !asDuration && rnd.Intn(6) == 0 {
		extraUs = 1 + rnd.Int63n(999)
	}
	s := ssScenario{kind: kind, step: ssStepText(stepMs, asDuration, extraUs), instant: instant, align: align}
	step := time.Duration(stepMs) * time.Millisecond
	switch rnd.Intn(4) {
	case 0:
		s.window = step // tumbling
	case 1:
		s.window = step / 2 // shorter than the step
	case 2:
		s.window = step * time.Duration(2+rnd.Intn(4)) // a multiple
	default:
		s.window = step*time.Duration(1+rnd.Intn(3)) + time.Duration(1+rnd.Intn(97))*time.Millisecond // not a multiple
	}
	if s.window = s.window.Truncate(time.Millisecond); s.window < time.Millisecond {
		s.window = time.Millisecond
	}
	startNs := ssBase() + int64(rnd.Intn(5000))*int64(time.Millisecond)
	if rnd.Intn(4) == 0 { // a client that sends nanoseconds: Loki keeps them
		startNs += rnd.Int63n(int64(time.Millisecond))
	}
	if rnd.Intn(3) == 0 { // on the step grid, as step alignment leaves it
		startNs = startNs / step.Nanoseconds() * step.Nanoseconds()
	}
	endNs := startNs + int64(rnd.Intn(13))*step.Nanoseconds()
	if rnd.Intn(3) == 0 { // an end that is not on the step grid
		endNs += int64(rnd.Intn(int(stepMs))) * int64(time.Millisecond)
	}
	form := rnd.Intn(4)
	s.startText, s.endText = ssTimestampText(startNs, form), ssTimestampText(endNs, form)
	if instant {
		s.startText = s.endText
	}
	if s.lokiStepNs() == 0 || s.endNs() < s.startNs() {
		return ssGenerate(rnd, kind, instant, align)
	}
	s.rows = ssRows(rnd, s, s.lokiStepNs(), s.startNs()-s.lokiStepNs()-int64(s.window)-int64(time.Second), s.endNs()+int64(time.Second))
	return s
}

// conformance: semantics/sub-second-step-grid
func TestSubSecondStepDifferential(t *testing.T) {
	for _, version := range []string{"", "v1.49.0", "v1.53.0"} {
		for _, instant := range []bool{false, true} {
			for _, align := range []bool{false, true} {
				if instant && align {
					continue // alignment moves range queries only
				}
				for _, kind := range ssKinds {
					t.Run(fmt.Sprintf("version=%s/instant=%v/align=%v/%s", version, instant, align, kind.name), func(t *testing.T) {
						rnd := rand.New(rand.NewSource(int64(len(kind.name))*7919 + int64(len(version))))
						for i := 0; i < 120; i++ {
							ssGenerate(rnd, kind, instant, align).check(t, version)
						}
					})
				}
			}
		}
	}
}

// ssEdgeRows are dense rows, one every 37 ms on alternating pods, over
// [from, to], plus rows on the edges of the first and the last window.
func ssEdgeRows(s ssScenario, from, to int64) []vlemu.Row {
	values := []float64{0, 0.5, 1, 2.25, 3, 100.125, 7}
	var rows []vlemu.Row
	from -= from % int64(time.Millisecond) // lines on the millisecond, as Loki's clients write them
	for ts, i := from, 0; ts <= to; ts, i = ts+37*int64(time.Millisecond), i+1 {
		rows = append(rows, vlemu.Row{TS: ts, Stream: fmt.Sprintf(`{app="uw",pod=%q}`, []string{"a", "b"}[i%2]), Value: values[i%len(values)]})
	}
	for _, t := range []int64{s.startNs(), s.endNs()} {
		for _, edge := range []int64{t, t - int64(s.window)} {
			for d := int64(-1); d <= 1; d++ {
				rows = append(rows, vlemu.Row{TS: edge + d*int64(time.Millisecond), Stream: `{app="uw",pod="a"}`, Value: 3})
			}
		}
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].TS < rows[j].TS })
	return rows
}

// The edge table: steps of 1 ms, 100 ms, 200 ms, 250 ms, 1.5 s, 0.999 s and 1 s;
// a range shorter than the step (none: one point), equal to it (two points) and
// longer without being a multiple; a window shorter than, equal to and longer
// than the step; a start on the step grid and off it, with the proxy's
// -align-queries-with-step off and on (the default); every shape, on v1.49.0 and v1.53.0.
//
// conformance: semantics/sub-second-step-grid, window-bounds-and-step-alignment
func TestSubSecondStepEdgeTable(t *testing.T) {
	steps := []struct{ text string }{{"0.001"}, {"0.1"}, {"0.2"}, {"250ms"}, {"1.5"}, {"0.999"}, {"1"}}
	for _, version := range []string{"v1.49.0", "v1.53.0"} {
		for _, step := range steps {
			stepDur, _ := lokiStep(step.text)
			stepNs := stepDur.Milliseconds() * int64(time.Millisecond)
			for _, rng := range []struct {
				name string
				span func(stepNs int64) int64
			}{
				{"none", func(int64) int64 { return 0 }},
				{"equals-step", func(n int64) int64 { return n }},
				{"not-a-multiple", func(n int64) int64 { return n*7/2 + 13*int64(time.Millisecond) }},
			} {
				for _, win := range []struct {
					name string
					d    func(step time.Duration) time.Duration
				}{
					{"shorter", func(d time.Duration) time.Duration { return max(d/2, time.Millisecond).Truncate(time.Millisecond) }},
					{"equal", func(d time.Duration) time.Duration { return d }},
					{"longer", func(d time.Duration) time.Duration { return 3*d + 7*time.Millisecond }},
				} {
					for _, aligned := range []bool{false, true} {
						for _, proxyAligns := range []bool{false, true} {
							name := fmt.Sprintf("%s/step=%s/range=%s/window=%s/start-on-grid=%v/proxy-aligns=%v", version, step.text, rng.name, win.name, aligned, proxyAligns)
							t.Run(name, func(t *testing.T) {
								start := ssBase()
								if aligned {
									start = start / stepNs * stepNs
								} else {
									start += 3*int64(time.Millisecond) + 1
								}
								s := ssScenario{step: step.text, window: win.d(time.Duration(stepNs)), align: proxyAligns}
								s.startText, s.endText = strconv.FormatInt(start, 10), strconv.FormatInt(start+rng.span(stepNs), 10)
								s.rows = ssEdgeRows(s, start-int64(s.window)-2*int64(time.Second), s.endNs()+int64(time.Second))
								for _, kind := range ssKinds {
									s.kind = kind
									s.check(t, version)
								}
							})
						}
					}
				}
			}
		}
	}
}

// The evidence shape of the CI trace: a 5 minute window, a 200 ms step over a
// 5 minute range, and the two short ranges Grafana sends with a 1 ms and a
// 20 ms interval. The first is aggregated; the other two make more buckets than
// one VictoriaLogs call is given (see beyondBucketBudget) and fall to the
// bounded raw-sample evaluator, which must give Loki's answer all the same.
//
// conformance: semantics/sub-second-step-grid
func TestSubSecondStepGrafanaShapes(t *testing.T) {
	for _, tc := range []struct {
		name, step string
		span       time.Duration
		raw        bool
	}{
		{"200ms-5m-range-5m-window", "0.2", 5 * time.Minute, false},
		{"1ms-1s-range-5m-window", "0.001", time.Second, true},
		{"20ms-10s-range-5m-window", "0.02", 10 * time.Second, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			start := int64(1_793_653_187_704) * int64(time.Millisecond)
			s := ssScenario{kind: ssKinds[3], step: tc.step, window: 5 * time.Minute}
			s.startText, s.endText = strconv.FormatInt(start, 10), strconv.FormatInt(start+int64(tc.span), 10)
			var rows []vlemu.Row
			for ts, i := start-int64(s.window)-int64(time.Second), 0; ts <= start+int64(tc.span); ts, i = ts+127*int64(time.Millisecond), i+1 {
				rows = append(rows, vlemu.Row{TS: ts, Stream: fmt.Sprintf(`{app="uw",pod=%q}`, []string{"a", "b"}[i%2]), Value: float64(i % 11)})
			}
			s.rows = rows
			if s.beyondBucketBudget() != tc.raw {
				t.Fatalf("beyondBucketBudget = %v, want %v", s.beyondBucketBudget(), tc.raw)
			}
			res := s.check(t, "v1.53.0")
			t.Logf("%s: %d series, raw-row reads: %d", tc.name, len(res.series), len(res.strict.emu.EndpointCalls("/select/logsql/query")))
		})
	}
}

// Loki answers a step that is positive but under a millisecond with a 500: its
// frontend keeps the step in whole milliseconds, 0, and the metric splitter
// divides by it. A range that is more than 11,000 steps is a 400 first, on the
// step as sent.
//
// conformance: semantics/sub-second-step-grid
func TestSubMillisecondStepIsLokisInternalError(t *testing.T) {
	emu := &vlemu.Emulator{T: t, Field: "amount"}
	backend := httptest.NewServer(&strictVL{t: t, next: emu.Handler(), emu: emu})
	defer backend.Close()
	p, err := New(Config{BackendURL: backend.URL, Cache: cache.New(time.Minute, 100), LogLevel: "error", BackendMinVersion: "v1.40.0", BackendVersionCheckTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		step     string
		span     time.Duration
		wantCode int
		wantBody string // Loki's text, no JSON envelope
	}{
		{"0.0005", time.Second, http.StatusInternalServerError, errLokiStepBelowMillisecond},
		{"0.0009999", time.Second, http.StatusInternalServerError, errLokiStepBelowMillisecond},
		{"0.0005", 20 * time.Second, http.StatusBadRequest, errLokiStepTooSmall},
		{"0.001", time.Second, http.StatusOK, ""},
		{"0.0010001", time.Second, http.StatusOK, ""},
	} {
		params := url.Values{"query": {`sum(count_over_time({app="uw"}[1s]))`}, "start": {strconv.FormatInt(ssBase(), 10)}, "end": {strconv.FormatInt(ssBase()+int64(tc.span), 10)}, "step": {tc.step}}
		res := doCompatProxyRequest(p, "/loki/api/v1/query_range?"+params.Encode(), map[string]string{"X-Scope-OrgID": "0"})
		if res.Code != tc.wantCode {
			t.Fatalf("step=%s range=%v: HTTP %d, want %d: %s", tc.step, tc.span, res.Code, tc.wantCode, res.Body.String())
		}
		if tc.wantBody != "" {
			if got := res.Body.String(); got != tc.wantBody {
				t.Fatalf("step=%s range=%v: body %q, want Loki's text %q", tc.step, tc.span, got, tc.wantBody)
			}
			if ct := res.Header().Get("Content-Type"); !strings.HasPrefix(ct, "text/plain") {
				t.Fatalf("step=%s: Content-Type %q, want text/plain", tc.step, ct)
			}
		}
	}
}

// The times of the answer are Loki's text, a millisecond fraction in seconds.
//
// conformance: semantics/sub-second-step-grid
func TestLokiSampleSecondsIsModelTimeText(t *testing.T) {
	check := func(ns int64) {
		t.Helper()
		got := strconv.FormatFloat(lokiSampleSeconds(ns), 'f', -1, 64)
		if want := lokiTimeText(lokiMs(ns)); got != want {
			t.Fatalf("lokiSampleSeconds(%d) = %s, Loki prints %s", ns, got, want)
		}
	}
	// 1793653191.65 is where float64(ns)/1e9 prints ...6499999.
	for _, ns := range []int64{1793653191650000000, 1793653187704000000, 1793653187704999999, 1793653187703999999, 1793653187000000000, 1793653187000000001, 0, 1, 999999, 1000000, -1} {
		check(ns)
	}
	rnd := rand.New(rand.NewSource(7))
	for i := 0; i < 20000; i++ {
		check(1_700_000_000_000_000_000 + rnd.Int63n(300_000_000_000_000_000))
	}
}

// A time VictoriaLogs is asked for keeps its fraction, as digits it reads
// exactly; a whole second is the same text as before.
//
// conformance: semantics/sub-second-step-grid
func TestNanosToVLTimestampKeepsTheFraction(t *testing.T) {
	for ns, want := range map[int64]string{
		1793653187000000000: "1793653187",
		1793653187704000000: "1793653187.704",
		1793653187000000001: "1793653187.000000001",
		1793653187500000000: "1793653187.5",
		1793653187123456789: "1793653187.123456789",
	} {
		if got := nanosToVLTimestamp(ns); got != want {
			t.Fatalf("nanosToVLTimestamp(%d) = %q, want %q", ns, got, want)
		}
	}
	rnd := rand.New(rand.NewSource(11))
	for i := 0; i < 20000; i++ {
		ns := 1_000_000_000_000_000_000 + rnd.Int63n(1_100_000_000_000_000_000)
		if read, ok := vlTimestampNs(nanosToVLTimestamp(ns)); !ok || read != ns {
			t.Fatalf("VictoriaLogs reads %q as %d, want %d", nanosToVLTimestamp(ns), read, ns)
		}
	}
}

// Loki rounds the fraction of a float-seconds timestamp to a millisecond and
// keeps nanoseconds sent as an integer; every parser of the proxy reads them
// the same way, without the 256 ns float64 error of multiplying by 1e9.
//
// conformance: semantics/sub-second-step-grid
func TestTimestampParsersReadLokisInstant(t *testing.T) {
	rnd := rand.New(rand.NewSource(13))
	texts := []string{"1793653187.704", "1793653187.7046", "1793653187.7044", "1793653187.999", "1793653187.9996", "1793653187.0004", "1793653187.0005", "1793653187.5", "1793653187"}
	for i := 0; i < 5000; i++ {
		ns := ssBase() + rnd.Int63n(int64(time.Hour))
		texts = append(texts, ssTimestampText(ns, 1+rnd.Intn(2)))
	}
	for _, text := range texts {
		want, _ := lokiTimestamp(text)
		for name, read := range map[string]func(string) (int64, bool){
			"parseLokiTimeToUnixNano": parseLokiTimeToUnixNano,
			"parseFlexibleUnixNanos":  parseFlexibleUnixNanos,
			"parseInstantVectorTime":  func(s string) (int64, bool) { return parseInstantVectorTime(s), true },
		} {
			if got, ok := read(text); !ok || got != want {
				t.Fatalf("%s(%q) = %d, Loki reads %d", name, text, got, want)
			}
		}
	}
}

// The step the handler evaluates on is the one Loki's codec keeps: the parsed
// step truncated to whole milliseconds, sent to VictoriaLogs as an exact count.
//
// conformance: semantics/sub-second-step-grid
func TestWholeMillisecondStepIsLokisStep(t *testing.T) {
	for _, tc := range []struct {
		text string
		want string // "": left as sent
	}{
		{"0.2", ""}, {"200ms", ""}, {"1", ""}, {"60", ""}, {"5m", ""}, {"1.5", ""}, {"0.999", ""},
		{"0.2005", "200ms"}, {"1.0005", "1000ms"}, {"1.001", "1000ms"}, {"1.003", "1002ms"},
		{"0.0010001", "1ms"}, {"2.9999999", "2999ms"}, {"0.1004", "100ms"},
	} {
		r := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?step="+url.QueryEscape(tc.text), nil)
		wholeMs, sub := lokiWholeMillisecondStep(r)
		if (tc.want != "") != sub {
			t.Fatalf("step=%s: sub-millisecond remainder %v, want %v", tc.text, sub, tc.want != "")
		}
		ref, _ := lokiStep(tc.text)
		if sub {
			if wholeMs != ref.Milliseconds() {
				t.Fatalf("step=%s: %d ms, Loki keeps %d", tc.text, wholeMs, ref.Milliseconds())
			}
			r = withLokiStep(r, wholeMs)
			if got := r.FormValue("step"); got != tc.want {
				t.Fatalf("step=%s: rewritten to %q, want %q", tc.text, got, tc.want)
			}
			if got := r.URL.Query().Get("step"); got != tc.want {
				t.Fatalf("step=%s: URL carries %q, want %q", tc.text, got, tc.want)
			}
		}
		if read, ok := vlDurationNs(formatVLStep(r.FormValue("step"))); !ok || read != ref.Milliseconds()*int64(time.Millisecond) {
			t.Fatalf("step=%s: VictoriaLogs reads %dns (%v), Loki keeps %dms", tc.text, read, ok, ref.Milliseconds())
		}
	}
}

// An exact duration reads back as itself in VictoriaLogs, which float seconds
// do not.
//
// conformance: semantics/sub-second-step-grid
func TestExactVLDurationReadsBack(t *testing.T) {
	rnd := rand.New(rand.NewSource(17))
	for i := 0; i < 20000; i++ {
		d := time.Duration(1+rnd.Int63n(int64(10*time.Minute))) / time.Millisecond * time.Millisecond
		if i%3 == 0 {
			d += time.Duration(rnd.Intn(1000000)) // a sub-millisecond rest
		}
		if read, ok := vlDurationNs(exactVLDuration(d)); !ok || read != int64(d) {
			t.Fatalf("exactVLDuration(%v) = %q reads as %d", d, exactVLDuration(d), read)
		}
	}
	// What the float form does, and why the exact one exists.
	if read, _ := vlDurationNs("1.003s"); read == int64(1003*time.Millisecond) {
		t.Fatal("expected VictoriaLogs to read 1.003s as 1002999999ns")
	}
	if got := formatVLStep("1.003s"); got != "1003ms" {
		t.Fatalf("formatVLStep(1.003s) = %q", got)
	}
}

// FuzzSubSecondStepAndTimestamps: for any step and timestamp text, the step the
// handler evaluates on is Loki's whole milliseconds and VictoriaLogs reads it
// back exactly, up to what its float64 duration parser holds
// (vlStepReadWithinFloat64); the timestamp parsers read a float-seconds text as Loki does;
// the time written for a nanosecond is Loki's text, and VictoriaLogs reads the
// time asked for back to the nanosecond.
func FuzzSubSecondStepAndTimestamps(f *testing.F) {
	for _, step := range []string{"0.2", "200ms", "1", "0.001", "0.0005", "1.003", "0.2005", "1.5", "0.999", "5m", "1h30m", "", "abc", "0", "-1", "1e-3", "NaN", "Inf"} {
		f.Add(step, "1793653187.704", int64(1793653187704000000))
	}
	f.Add("0.1", "1793653187.7046", int64(1793653191650000000))
	f.Add("2", "1793653187", int64(1793653187000000001))
	f.Fuzz(func(t *testing.T, step, ts string, ns int64) {
		r := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range", nil)
		r.Form = url.Values{"step": {step}}
		if wholeMs, sub := lokiWholeMillisecondStep(r); sub {
			d, ok := parsePositiveStepDuration(step)
			if !ok || wholeMs != d.Milliseconds() || d%time.Millisecond == 0 {
				t.Fatalf("step %q: %d ms for %v", step, wholeMs, d)
			}
			if wholeMs > 0 {
				clone := withLokiStep(r, wholeMs)
				read, ok := vlDurationNs(formatVLStep(clone.FormValue("step")))
				if !ok || !vlStepReadWithinFloat64(read, wholeMs*int64(time.Millisecond)) {
					t.Fatalf("step %q: VictoriaLogs reads %dns (%v), want %dms", step, read, ok, wholeMs)
				}
			}
		}
		// Where the proxy and Loki read the step, they read the same.
		if want, ok := lokiStep(step); ok && want > 0 {
			if got, ok := parsePositiveStepDuration(step); ok && got != want {
				t.Fatalf("step %q: the proxy reads %v, Loki %v", step, got, want)
			}
		}
		if strings.Contains(ts, ".") && len(ts) < 24 && !strings.ContainsAny(ts, "eEnNiIxXpP_+-") {
			// Up to 9.2e9 s int64 nanoseconds hold the time and the proxy reads it as Loki does; from 9.3e9 s on
			// Loki wraps it to another time and the proxy refuses it (the request time bounds).
			if f, err := strconv.ParseFloat(ts, 64); err == nil && f >= 9.3e9 && f < 1e10 {
				if got, ok := parseLokiTimeToUnixNano(ts); ok {
					t.Fatalf("time %q: parseLokiTimeToUnixNano = %d, want refused", ts, got)
				}
			}
			if f, err := strconv.ParseFloat(ts, 64); err == nil && f >= 0 && f < 9.2e9 {
				want, _ := lokiTimestamp(ts)
				if got, ok := parseLokiTimeToUnixNano(ts); !ok || got != want {
					t.Fatalf("time %q: parseLokiTimeToUnixNano = %d, Loki reads %d", ts, got, want)
				}
				if got, ok := parseFlexibleUnixNanos(ts); !ok || got != want {
					t.Fatalf("time %q: parseFlexibleUnixNanos = %d, Loki reads %d", ts, got, want)
				}
			}
		}
		if ns > 1_000_000_000_000_000_000 && ns < 2_100_000_000_000_000_000 {
			if got, want := strconv.FormatFloat(lokiSampleSeconds(ns), 'f', -1, 64), lokiTimeText(lokiMs(ns)); got != want {
				t.Fatalf("lokiSampleSeconds(%d) = %s, Loki prints %s", ns, got, want)
			}
			if read, ok := vlTimestampNs(nanosToVLTimestamp(ns)); !ok || read != ns {
				t.Fatalf("VictoriaLogs reads %q as %d, want %d", nanosToVLTimestamp(ns), read, ns)
			}
		}
	})
}

// restampInstantVector writes Loki's time into every sample and leaves the rest
// of the body as it was, for any body: with no sample, with a marker at the
// very end, and with a body of many megabytes that the buffer has to grow to
// (its size is not computed from the body's length).
//
// conformance: semantics/sub-second-step-grid
func TestRestampInstantVectorBodies(t *testing.T) {
	ns := int64(1793653199583000000)
	for name, tc := range map[string]struct{ in, want string }{
		"empty":                                  {"", ""},
		"no sample":                              {`{"status":"success","data":{"resultType":"vector","result":[]}}`, `{"status":"success","data":{"resultType":"vector","result":[]}}`},
		"one sample":                             {`{"result":[{"metric":{"a":"b"},"value":[1793653199.5830002,"7"]}]}`, `{"result":[{"metric":{"a":"b"},"value":[1793653199.583,"7"]}]}`},
		"two samples":                            {`[{"value":[1,"1"]},{"value":[2,"2"]}]`, `[{"value":[1793653199.583,"1"]},{"value":[1793653199.583,"2"]}]`},
		"marker at the end":                      {`{"value":[`, `{"value":[`},
		"marker without comma":                   {`{"value":[1793653199`, `{"value":[1793653199`},
		"a label value that looks like a marker": {`{"metric":{"k":"\"value\":["},"value":[1,"1"]}`, `{"metric":{"k":"\"value\":["},"value":[1793653199.583,"1"]}`},
	} {
		if got := string(restampInstantVector([]byte(tc.in), ns)); got != tc.want {
			t.Fatalf("%s: got %q, want %q", name, got, tc.want)
		}
	}
	// 4 MiB of samples (about 150,000): bounded work, every one restamped.
	var in strings.Builder
	in.WriteString(`{"result":[`)
	samples := 0
	for in.Len() < 4<<20 {
		if samples > 0 {
			in.WriteByte(',')
		}
		in.WriteString(`{"metric":{"pod":"p"},"value":[1793653199.5830002,"1"]}`)
		samples++
	}
	in.WriteString(`]}`)
	out := string(restampInstantVector([]byte(in.String()), ns))
	if got := strings.Count(out, `"value":[1793653199.583,`); got != samples || strings.Contains(out, "5830002") {
		t.Fatalf("restamped %d of %d samples", got, samples)
	}
}

// A line exactly on an evaluation time is in that window ((t-window, t]), and
// one exactly window earlier is not, however the time falls in float64. The CI
// case of TestCompat_SubSecondStep: step 0.999 and a 600 ms window, aligned
// from 1793671800000 ms, a line at 1793671902630 ms, which is a step time.
// VictoriaLogs read it as phase 600000064 ns and the window filter dropped it,
// so the proxy answered 8 samples where Loki answered 9. Lines sit on every
// step time, on every window start, and a millisecond either side of both.
//
// conformance: semantics/sub-second-step-grid, window-bounds-and-step-alignment
func TestSubSecondStepLineOnTheWindowEdge(t *testing.T) {
	const stepMs, windowMs = 999, 600
	startMs := int64(1793671800000)
	for _, version := range []string{"", "v1.49.0", "v1.53.0"} {
		for _, shiftMs := range []int64{0, 1, 3, 111, 998} { // the grid moves with the hour of the fixture
			for _, kind := range ssKinds {
				s := ssScenario{kind: kind, step: "0.999", window: windowMs * time.Millisecond, align: true}
				first := (startMs + shiftMs) / stepMs * stepMs
				s.startText = strconv.FormatInt(first*int64(time.Millisecond), 10)
				s.endText = strconv.FormatInt((first+400*stepMs)*int64(time.Millisecond), 10)
				for k := int64(0); k <= 400; k++ {
					at := (first + k*stepMs) * int64(time.Millisecond)
					for _, off := range []int64{0, -windowMs, 1, -1, -windowMs + 1, -windowMs - 1} {
						// A line dropped from a window and one let in from the edge
						// below must not cancel: they differ in series and value.
						pod, value := "a", 100.0
						switch off {
						case 0:
							value = 1
						case -windowMs:
							pod, value = "b", 10
						}
						s.rows = append(s.rows, vlemu.Row{TS: at + off*int64(time.Millisecond), Stream: fmt.Sprintf(`{app="uw",pod=%q}`, pod), Value: value})
					}
				}
				sort.Slice(s.rows, func(i, j int) bool { return s.rows[i].TS < s.rows[j].TS })
				s.check(t, version)
			}
		}
	}
}
