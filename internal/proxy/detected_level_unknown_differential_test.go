package proxy

import (
	"context"
	"encoding/json"
	"fmt"
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

// This file pins the detected_level label of metric answers to Loki's, on
// every route a metric query takes to VictoriaLogs stats: an instant query, a
// tumbling window (range == step) and a sliding window (range > step).
//
// Loki attaches detected_level to every entry at ingest and falls back to
// "unknown" (pkg/distributor/field_detection.go: LogLevelUnknown), so the
// series of a stream whose lines carry no level are still labelled
// detected_level="unknown", and a grouping by it always has that group
// (pkg/logql/log/metrics_extraction.go streamLineSampleExtractor.Process adds
// the structured metadata to the builder; labels.go GroupedLabels returns it
// for an ungrouped or without() grouping). The reference below is written apart
// from the proxy: it takes the generated rows and computes the answer from
// Loki's series identity, the stream plus the level plus detected_level.
//
// The proxy under test is the real handler. VictoriaLogs is simulated by
// vlEmulator, which applies Loki's detected_level rule (dlDerive) where the
// translator's detected_level pipe chain is, and the stats pipes with
// VictoriaLogs' bucket semantics. A series the model knows is the stream, a
// stored level field and detected_level; the chain is pinned apart in the
// translator tests.

type dlRow struct {
	ts     int64
	stream string // {app="dl",pod="a"}
	level  string // the stored level field; empty: the row has none
	msg    string // the line; empty: a line without a level word
}

var dlKeywordRE = regexp.MustCompile(`(?i)(?:^|[ \t\n\[({"=])(trace|debug|fatal|critical|error|err|warning|warn|info)(?:$|[ \t\n\[\](){}:,!"=])`)

// dlDerive is Loki's detected_level of an entry: a stored level field, else the
// level of a JSON or logfmt line, else a level word of the line, else unknown
// (pkg/distributor/field_detection.go; the proxy's translator chain computes
// the same on VictoriaLogs).
func dlDerive(level, msg string) string {
	if level != "" {
		return strings.ToLower(level)
	}
	var obj map[string]any
	if json.Unmarshal([]byte(msg), &obj) == nil {
		if v, ok := obj["level"].(string); ok && v != "" {
			return dlNormalise(v)
		}
	}
	for _, field := range strings.Fields(msg) {
		if v, ok := strings.CutPrefix(field, "level="); ok && v != "" {
			return dlNormalise(v)
		}
	}
	if m := dlKeywordRE.FindStringSubmatch(msg); m != nil {
		return dlNormalise(m[1])
	}
	return "unknown"
}

func dlNormalise(v string) string {
	switch v = strings.ToLower(v); v {
	case "err":
		return "error"
	case "warning":
		return "warn"
	}
	return v
}

type dlScenario struct {
	query      string
	drop       []string // labels a `| drop` stage of the query removes
	window     time.Duration
	step       time.Duration // 0: instant
	start, end int64         // ns, evaluation times
	rows       []dlRow
}

func (s dlScenario) route() string {
	if s.step == 0 {
		return "instant"
	}
	if s.step == s.window {
		return "tumbling"
	}
	return "sliding"
}

func (s dlScenario) times() []int64 {
	if s.step == 0 {
		return []int64{s.end}
	}
	var out []int64
	for t := s.start; t <= s.end; t += int64(s.step) {
		out = append(out, t)
	}
	return out
}

func (s dlScenario) emuRows() []emuRow {
	out := make([]emuRow, len(s.rows))
	for i, r := range s.rows {
		out[i] = emuRow{ts: r.ts, stream: r.stream, msg: r.msg, value: 1}
		if r.msg == "" {
			out[i].msg = fmt.Sprintf("line %d", i)
		}
		if r.level != "" {
			out[i].labels = map[string]string{"level": r.level}
		}
	}
	return out
}

// dlSeries is one Loki series: its labels and the value at each time.
type dlSeries struct {
	labels map[string]string
	values map[int64]float64
}

func dlKey(labels map[string]string) string {
	b, _ := json.Marshal(labels) // keys are sorted
	return string(b)
}

func dlStreamLabels(stream string) map[string]string {
	out := map[string]string{}
	for _, m := range emuStreamLabelRE.FindAllStringSubmatch(stream, -1) {
		out[m[1]] = m[2]
	}
	return out
}

// lokiDetectedLevelAnswer is the reference. kind is the metric: "" (a bare
// count_over_time), "rate" (a bare rate), "unwrap" (a bare avg_over_time over
// an unwrapped label whose value is 1) or the vector aggregation around
// count_over_time: "sum by (detected_level)", "count by (detected_level)",
// "sum without (pod)"... A series is the stream labels, the level when the row
// has one, and detected_level.
func lokiDetectedLevelAnswer(rows []dlRow, kind string, drop []string, window int64, times []int64) map[string]*dlSeries {
	out := map[string]*dlSeries{}
	add := func(labels map[string]string, t int64, v float64) {
		key := dlKey(labels)
		s := out[key]
		if s == nil {
			s = &dlSeries{labels: labels, values: map[int64]float64{}}
			out[key] = s
		}
		s.values[t] += v
	}
	for _, t := range times {
		perSeries := map[string]float64{}
		identity := map[string]map[string]string{}
		for _, r := range rows {
			if r.ts <= t-window || r.ts > t {
				continue
			}
			labels := dlStreamLabels(r.stream)
			// Loki names a stream that has no service label after its app
			// label (distributor discover_service_name).
			labels["service_name"] = labels["app"]
			if r.level != "" {
				labels["level"] = r.level
			}
			labels["detected_level"] = dlDerive(r.level, r.msg)
			for _, name := range drop {
				delete(labels, name)
			}
			key := dlKey(labels)
			perSeries[key]++
			identity[key] = labels
		}
		for key, v := range perSeries {
			labels := identity[key]
			switch kind {
			case "":
				add(labels, t, v)
			case "rate":
				add(labels, t, v/(float64(window)/1e9))
			case "unwrap":
				add(labels, t, 1)
			case "sum by (detected_level)", "count by (detected_level)":
				by := map[string]string{}
				if dl := labels["detected_level"]; dl != "" { // a dropped label is no group
					by["detected_level"] = dl
				}
				if kind == "sum by (detected_level)" {
					add(by, t, v)
				} else {
					add(by, t, 1)
				}
			case "sum without (pod)", "sum without (pod, detected_level)", "sum without (level)":
				drop := strings.Split(strings.TrimSuffix(strings.TrimPrefix(kind, "sum without ("), ")"), ", ")
				rest := map[string]string{}
				for k, lv := range labels {
					if !slices.Contains(drop, k) {
						rest[k] = lv
					}
				}
				add(rest, t, v)
			default:
				panic("reference: " + kind)
			}
		}
	}
	return out
}

func dlQuery(kind, window, pipe string) string {
	inner := fmt.Sprintf(`count_over_time({app="dl"}%s[%s])`, pipe, window)
	switch kind {
	case "":
		return inner
	case "rate":
		return fmt.Sprintf(`rate({app="dl"}%s[%s])`, pipe, window)
	case "unwrap":
		return fmt.Sprintf(`avg_over_time({app="dl"}%s | unwrap amount [%s])`, pipe, window)
	}
	return fmt.Sprintf("%s (%s)", kind, inner)
}

// dlKinds are the metric shapes under test. count by over a sliding window is
// left out: the sliding route does not reduce count, min, max and avg yet
// (semantics/non-sum-vector-aggregation-unreduced, a separate gap); an unwrap
// range function is left to the instant route, as its tumbling and sliding
// windows are labelled at other times than Loki's
// (semantics/unwrap-over-time-range-windows, a separate gap).
func dlKinds(route string) []string {
	kinds := []string{"", "rate", "unwrap", "sum by (detected_level)", "count by (detected_level)", "sum without (pod)", "sum without (pod, detected_level)", "sum without (level)"}
	return slices.DeleteFunc(kinds, func(k string) bool {
		return route == "sliding" && strings.HasPrefix(k, "count") || route != "instant" && k == "unwrap"
	})
}

func generateDLScenario(rnd *rand.Rand, kind, route string) dlScenario {
	windows := []time.Duration{time.Minute, 2 * time.Minute, 5 * time.Minute}
	w := windows[rnd.Intn(len(windows))]
	s := dlScenario{window: w}
	switch route {
	case "tumbling":
		s.step = w
	case "sliding":
		s.step = w / 2
	}
	base := int64(1_800_000_000) * 1e9
	s.start = base + int64(rnd.Intn(10))*int64(w)
	s.end = s.start
	if s.step > 0 {
		s.end = s.start + int64(1+rnd.Intn(4))*int64(s.step)
	}
	s.query = dlQuery(kind, strconv.Itoa(int(w/time.Minute))+"m", "")
	streams := []string{`{app="dl",pod="a"}`, `{app="dl",pod="b"}`}
	levels := []string{"", "", "", "info", "error", "warn"}
	// Lines without a stored level: Loki reads the line.
	lines := []string{"", "", "level=warn msg=slow", `{"level":"error","code":500}`, "ERROR: upstream failed", "request done"}
	lo, hi := s.start-int64(w)+1e9, s.end
	for i, n := 0, 6+rnd.Intn(40); i < n; i++ {
		s.rows = append(s.rows, dlRow{
			ts:     lo + rnd.Int63n(hi-lo),
			stream: streams[rnd.Intn(len(streams))],
			level:  levels[rnd.Intn(len(levels))],
			msg:    lines[rnd.Intn(len(lines))],
		})
	}
	return s
}

func runDLScenario(t *testing.T, s dlScenario, version string) map[string]*dlSeries {
	t.Helper()
	emu := &vlEmulator{t: t, rows: s.emuRows(), field: "amount", version: version, deriveLevel: dlDerive}
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
	params := url.Values{"query": {s.query}}
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
		t.Fatalf("%s: %d %s", s.query, res.Code, res.Body)
	}
	if raw := emu.endpointCalls("/select/logsql/query"); len(raw) > 0 {
		t.Fatalf("%s: the metric read raw rows: %s", s.query, raw[0])
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
	got := map[string]*dlSeries{}
	for _, series := range resp.Data.Result {
		points := series.Values
		if series.Value != nil {
			points = [][]any{series.Value}
		}
		key := dlKey(series.Metric)
		if got[key] != nil {
			t.Fatalf("%s: series %s twice", s.query, key)
		}
		ds := &dlSeries{labels: series.Metric, values: map[int64]float64{}}
		for _, point := range points {
			v, _ := strconv.ParseFloat(point[1].(string), 64)
			ds.values[int64(point[0].(float64))*1e9] = v
		}
		got[key] = ds
	}
	return got
}

func describeDLAnswer(a map[string]*dlSeries) string {
	lines := make([]string, 0, len(a))
	for key, s := range a {
		ts := make([]int64, 0, len(s.values))
		for t := range s.values {
			ts = append(ts, t)
		}
		sort.Slice(ts, func(i, j int) bool { return ts[i] < ts[j] })
		var pts []string
		for _, t := range ts {
			pts = append(pts, fmt.Sprintf("%d=%g", t/1e9%100000, s.values[t]))
		}
		lines = append(lines, key+" "+strings.Join(pts, ","))
	}
	sort.Strings(lines)
	return strings.Join(lines, "\n  ")
}

// compareDLAnswer demands the same series (label sets) and, at every time
// Loki has a point, the same value.
func compareDLAnswer(t *testing.T, label string, s dlScenario, got, want map[string]*dlSeries) {
	t.Helper()
	fail := func(format string, args ...any) {
		t.Helper()
		t.Fatalf("%s: %s\nquery %s route %s rows=%d\nLoki:\n  %s\nproxy:\n  %s", label, fmt.Sprintf(format, args...), s.query, s.route(), len(s.rows), describeDLAnswer(want), describeDLAnswer(got))
	}
	for key, w := range want {
		g := got[key]
		if g == nil {
			fail("series %s missing", key)
		}
		for ts, wv := range w.values {
			if gv, ok := g.values[ts]; !ok || gv != wv {
				fail("series %s at %d: got %v (present %v), Loki has %g", key, ts/1e9, gv, ok, wv)
			}
		}
	}
	for key, g := range got {
		w := want[key]
		if w == nil {
			fail("series %s is not in Loki's answer", key)
		}
		for ts, gv := range g.values {
			if _, ok := w.values[ts]; !ok && gv != 0 {
				fail("series %s has an extra point at %d = %g", key, ts/1e9, gv)
			}
		}
	}
}

// TestDetectedLevelUnknownAcrossRoutes: over generated rows (two streams, rows
// with and without a stored level) the proxy's answer to a count_over_time
// metric — bare, with a vector aggregation grouping by detected_level, and with
// without() — carries Loki's detected_level labels, "unknown" for the lines
// that carry no level, on the instant, tumbling and sliding routes alike.
//
// conformance: semantics/detected-level-missing-from-stats-answers, severity-exposure-surfaces
func TestDetectedLevelUnknownAcrossRoutes(t *testing.T) {
	for _, version := range []string{"", "v1.52.0", "v1.40.0"} {
		for _, route := range []string{"instant", "tumbling", "sliding"} {
			for _, kind := range dlKinds(route) {
				name := fmt.Sprintf("version=%s/%q/%s", version, kind, route)
				t.Run(name, func(t *testing.T) {
					rnd := rand.New(rand.NewSource(int64(len(name))*104729 + 5))
					for i := 0; i < 12; i++ {
						s := generateDLScenario(rnd, kind, route)
						got := runDLScenario(t, s, version)
						want := lokiDetectedLevelAnswer(s.rows, kind, s.drop, int64(s.window), s.times())
						compareDLAnswer(t, fmt.Sprintf("%s #%d", name, i), s, got, want)
					}
				})
			}
		}
	}
}

// TestDetectedLevelUnknownEdgeTable pins the shapes the generator may not
// reach: every line without a level, a single stream, one level, and an empty
// window.
func TestDetectedLevelUnknownEdgeTable(t *testing.T) {
	base := int64(1_800_000_000) * 1e9
	in := func(sec int64) int64 { return base + sec*1e9 }
	a, b := `{app="dl",pod="a"}`, `{app="dl",pod="b"}`
	cases := []struct {
		name string
		rows []dlRow
	}{
		{"no row has a level", []dlRow{{in(10), a, "", ""}, {in(20), b, "", ""}}},
		{"every row has a level", []dlRow{{in(10), a, "info", ""}, {in(20), b, "error", ""}}},
		{"one stream mixes levels and none", []dlRow{{in(10), a, "", ""}, {in(20), a, "info", ""}, {in(30), a, "", ""}}},
		{"empty window", nil},
		{"a level the proxy does not normalise", []dlRow{{in(10), a, "notice", ""}, {in(20), a, "", ""}}},
		{"logfmt line level", []dlRow{{in(10), a, "", "ts=1 level=error msg=x"}, {in(20), a, "", "ts=2 msg=y"}}},
		{"json line level", []dlRow{{in(10), a, "", `{"level":"warn","m":1}`}, {in(20), a, "", `{"m":2}`}}},
		{"keyword in the line", []dlRow{{in(10), a, "", "ERROR: boom"}, {in(20), a, "", "nothing here"}, {in(30), b, "", "got a WARNING"}}},
		{"stored level beats the line", []dlRow{{in(10), a, "info", "ERROR: boom"}}},
		{"one stream, every spelling of a level source", []dlRow{{in(10), a, "debug", ""}, {in(20), a, "", "level=debug"}, {in(30), a, "", `{"level":"debug"}`}, {in(40), a, "", "DEBUG x"}}},
	}
	// A `| drop` stage removes the label from the series after the derivation,
	// as in Loki, whichever route answers.
	pipes := []struct {
		text string
		drop []string
	}{{"", nil}, {" | drop detected_level", []string{"detected_level"}}, {" | drop level", []string{"level"}}}
	for _, tc := range cases {
		for _, pipe := range pipes {
			for _, route := range []string{"instant", "tumbling", "sliding"} {
				for _, kind := range dlKinds(route) {
					t.Run(fmt.Sprintf("%s%s/%q/%s", tc.name, pipe.text, kind, route), func(t *testing.T) {
						s := dlScenario{window: time.Minute, start: base + int64(time.Minute), rows: tc.rows, drop: pipe.drop}
						switch route {
						case "instant":
							s.end = s.start
						case "tumbling":
							s.step, s.end = time.Minute, s.start+int64(time.Minute)
						case "sliding":
							s.step, s.end = 30*time.Second, s.start+int64(time.Minute)
						}
						s.query = dlQuery(kind, "1m", pipe.text)
						got := runDLScenario(t, s, "")
						want := lokiDetectedLevelAnswer(s.rows, kind, s.drop, int64(s.window), s.times())
						compareDLAnswer(t, tc.name, s, got, want)
					})
				}
			}
		}
	}
}

// withoutDetectedLevelShape removes what the translator adds to a
// stream-identity grouping (the detected_level pipes and the grouping on it),
// for the tests that pin the stats pipes apart from detected_level.
func withoutDetectedLevelShape(query string) string {
	if start := strings.Index(query, " "+derivedLevelChainStart); start >= 0 {
		if stop := strings.Index(query[start:], derivedLevelChainEnd); stop >= 0 {
			query = query[:start] + query[start+stop+len(derivedLevelChainEnd):]
		}
	}
	return strings.ReplaceAll(query, ", detected_level)", ")")
}

// A stats query without its internal clean-up pipes counts the same lines; a
// delete of a label the client dropped changes what the stats group by, so it
// stays (the tumbling and instant routes used to remove every delete pipe).
func TestStripInternalDeletePipes(t *testing.T) {
	for _, tc := range []struct{ name, in, want string }{
		{"parse error fields", `app:="x" | unpack_json | delete __error__, __error_details__`, `app:="x" | unpack_json`},
		{"detected_level scratch fields", `app:="x" | format "<__dl>" as detected_level | delete __dl, __dl_*, __j_*, __l_* | filter a:b`, `app:="x" | format "<__dl>" as detected_level | filter a:b`},
		{"a dropped label stays", `app:="x" | delete detected_level`, `app:="x" | delete detected_level`},
		{"a dropped label next to an internal field stays whole", `app:="x" | delete __error__, level | filter a:b`, `app:="x" | delete __error__, level | filter a:b`},
		{"internal pipe, then a dropped label", `app:="x" | delete __error__ | delete level`, `app:="x" | delete level`},
		{"no delete", `app:="x" | filter a:b`, `app:="x" | filter a:b`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := strings.TrimSpace(stripInternalDeletePipes(tc.in)); got != tc.want {
				t.Fatalf("got %q, want %q", got, tc.want)
			}
		})
	}
}
