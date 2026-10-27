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
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// A `<op> without (...)` over a tumbling window is folded by VictoriaLogs
// (translator.PushDownWithout) when every excluded label is plain; the proxy
// folds the series rows itself otherwise. Both answer Loki's: the series fall
// into the group the labels left name, and the group takes the operator's value
// (pkg/logql/evaluator.go VectorAggEvaluator.Next). This file pins the two
// routes to a reference written apart from them, and to each other, over
// streams whose label sets differ: a label the stream lacks, the excluded label
// being the only one that tells two series apart, values with quotes, commas,
// backslashes and text that looks like a pair, and excluded names the cut does
// not take (a service_name source, an underscore name).

var wpLabelPool = []string{"abc", "pod", "node", "zone", "svc", "rack", "pod_name", "container"}

var wpValuePool = []string{"a", "b", "c", "x", "y"}

var wpNoteValues = []string{`a,b`, `x"y`, `back\slash`, `,pod="q"`, `pod="p",zone="z"`, `end\`}

var wpEscape = strings.NewReplacer(`\`, `\\`, `"`, `\"`)

type wpStream struct {
	labels map[string]string
	text   string
}

func wpGenerateStreams(rnd *rand.Rand) []wpStream {
	n := 2 + rnd.Intn(7)
	seen := map[string]bool{}
	var out []wpStream
	for i := 0; i < n*3 && len(out) < n; i++ {
		labels := map[string]string{"app": "uw"}
		for _, name := range wpLabelPool {
			if rnd.Intn(3) != 0 { // present on two streams in three
				labels[name] = wpValuePool[rnd.Intn(len(wpValuePool))]
			}
		}
		if rnd.Intn(3) == 0 {
			labels["note"] = wpNoteValues[rnd.Intn(len(wpNoteValues))]
		}
		names := make([]string, 0, len(labels))
		for name := range labels {
			names = append(names, name)
		}
		sort.Strings(names)
		parts := make([]string, len(names))
		for j, name := range names {
			parts[j] = name + `="` + wpEscape.Replace(labels[name]) + `"`
		}
		text := "{" + strings.Join(parts, ",") + "}"
		if seen[text] {
			continue
		}
		seen[text] = true
		out = append(out, wpStream{labels, text})
	}
	return out
}

func wpGenerateScenario(rnd *rand.Rand, fn string) (nvScenario, []wpStream) {
	streams := wpGenerateStreams(rnd)
	step := []time.Duration{30 * time.Second, time.Minute, 90 * time.Second}[rnd.Intn(3)]
	s := nvScenario{fn: fn, window: step, step: step, trailingBy: rnd.Intn(2) == 0}
	base := int64(1_800_000_000) * 1e9
	s.start = base + int64(rnd.Intn(600))*1e9
	s.start -= s.start % int64(step)
	s.end = s.start + int64(rnd.Intn(5))*int64(step)
	lo, hi := s.start-int64(s.window)-5e9, s.end+5e9
	for i, n := 0, 10+rnd.Intn(80); i < n; i++ {
		ts := lo + rnd.Int63n(hi-lo)
		if rnd.Intn(4) == 0 {
			ts = s.start + int64(rnd.Intn(5))*int64(step) - int64(rnd.Intn(2))*int64(s.window) + int64(rnd.Intn(3)-1)
		}
		stream := streams[rnd.Intn(len(streams))]
		if rnd.Intn(2) == 0 {
			stream = streams[rnd.Intn(min(2, len(streams)))]
		}
		s.rows = append(s.rows, emuRow{ts: ts, stream: stream.text, msg: "x=1"})
	}
	s.dropEdgeRows()
	// An innermost `without` over names from the pool (some absent from some
	// streams), `baz` that no stream has, or a name the cut does not take.
	excluded := []string{"abc", "pod", "node", "zone", "svc", "rack", "baz", "app", "pod_name", "container", "note"}
	rnd.Shuffle(len(excluded), func(i, j int) { excluded[i], excluded[j] = excluded[j], excluded[i] })
	ops := []string{"sum", "avg", "min", "max", "count"}
	s.levels = []nvLevel{nvWithout(ops[rnd.Intn(len(ops))], excluded[:1+rnd.Intn(3)]...)}
	for depth := []int{0, 0, 1, 2}[rnd.Intn(4)]; depth > 0; depth-- {
		by := []string{}
		for _, name := range []string{"pod", "node", "zone", "svc", "rack", "baz", "pod_name", "app"} {
			if rnd.Intn(3) == 0 {
				by = append(by, name)
			}
		}
		level := nvBy(ops[rnd.Intn(len(ops))], by...)
		if len(by) == 0 && rnd.Intn(2) == 0 {
			level = nvLevel{op: level.op} // no grouping at all
		}
		s.levels = append(s.levels, level)
	}
	return s, streams
}

// wpReference is Loki's answer: the value of the range function of every stream
// that has a line in (t-window, t], folded by the levels, innermost first.
func wpReference(s nvScenario, streams []wpStream) map[string]map[int64]float64 {
	out := map[string]map[int64]float64{}
	byText := map[string]wpStream{}
	for _, st := range streams {
		byText[st.text] = st
	}
	for _, t := range s.times() {
		perStream := map[string]float64{}
		for _, row := range s.rows {
			if row.ts > t-int64(s.window) && row.ts <= t {
				perStream[row.stream]++
			}
		}
		var vec []nvSeries
		for text, count := range perStream {
			value := count
			if s.fn == "rate" {
				value = count / s.window.Seconds()
			}
			labels := map[string]string{"service_name": "uw"}
			for name, v := range byText[text].labels {
				labels[name] = v
			}
			vec = append(vec, nvSeries{labels, value})
		}
		for _, level := range s.levels {
			vec = nvFold(vec, level)
		}
		for _, series := range vec {
			key := nvLabelsKey(series.labels)
			if out[key] == nil {
				out[key] = map[int64]float64{}
			}
			out[key][t] = series.value
		}
	}
	return out
}

// wpRun sends the scenario through the real handler and returns the answer as
// labels key -> time -> value, and the queries VictoriaLogs received.
func wpRun(t *testing.T, s nvScenario, pushdownOff bool, mappings ...FieldMapping) (map[string]map[int64]float64, []string) {
	t.Helper()
	emu := &vlEmulator{t: t, rows: s.rows, field: "amount"}
	backend := httptest.NewServer(emu.handler())
	defer backend.Close()
	responseCache := cache.New(60*time.Second, 1000)
	p, err := New(Config{BackendURL: backend.URL, Cache: responseCache, LogLevel: "error", FieldMappings: mappings})
	if err != nil {
		responseCache.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = p.Shutdown(context.Background())
		responseCache.Close()
	})
	withoutPushdownOff.Store(pushdownOff)
	t.Cleanup(func() { withoutPushdownOff.Store(false) })
	params := url.Values{"query": {s.query()}, "start": {strconv.FormatInt(s.start/1e9, 10)}, "end": {strconv.FormatInt(s.end/1e9, 10)}, "step": {strconv.FormatInt(int64(s.step/time.Second), 10)}}
	res := doCompatProxyRequest(p, "/loki/api/v1/query_range?"+params.Encode(), map[string]string{"X-Scope-OrgID": "0"})
	if res.Code != http.StatusOK {
		t.Fatalf("%s: %d %s", s, res.Code, res.Body)
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
		delete(series.Metric, "detected_level")
		key := nvLabelsKey(series.Metric)
		if _, dup := got[key]; dup {
			t.Fatalf("%s: two series with the labels %v", s, series.Metric)
		}
		got[key] = map[int64]float64{}
		for _, point := range series.Values {
			v, _ := strconv.ParseFloat(point[1].(string), 64)
			got[key][int64(point[0].(float64))*1e9] = v
		}
	}
	return got, emu.endpointCalls("/select/logsql/stats_query_range")
}

func wpEqual(a, b map[string]map[int64]float64) string {
	for key, points := range a {
		if b[key] == nil {
			return "series " + key + " is missing"
		}
		for ts, v := range points {
			w, ok := b[key][ts]
			if !ok {
				return fmt.Sprintf("%s has no point at %d", key, ts/1e9)
			}
			if math.Abs(v-w) > 1e-9*math.Max(1, math.Abs(w)) {
				return fmt.Sprintf("%s at %d: %g against %g", key, ts/1e9, v, w)
			}
		}
		for ts := range b[key] {
			if _, ok := points[ts]; !ok {
				return fmt.Sprintf("%s has an extra point at %d", key, ts/1e9)
			}
		}
	}
	for key := range b {
		if a[key] == nil {
			return "extra series " + key
		}
	}
	return ""
}

// conformance: semantics/nested-aggregation-outer-by-native-routes, semantics/without-over-inner-aggregation-collapses, metric-series-identity
func TestWithoutPushdownMatchesFoldAndLoki(t *testing.T) {
	pushed, folded := 0, 0
	for _, fn := range []string{"count_over_time", "rate"} {
		rnd := rand.New(rand.NewSource(int64(len(fn)) * 104729))
		for i := 0; i < 160; i++ {
			s, streams := wpGenerateScenario(rnd, fn)
			want := wpReference(s, streams)
			viaVL, queries := wpRun(t, s, false)
			viaProxy, _ := wpRun(t, s, true)
			if diff := wpEqual(want, viaVL); diff != "" {
				t.Fatalf("#%d %s\nVictoriaLogs side: %s\nqueries: %v", i, s, diff, queries)
			}
			if diff := wpEqual(want, viaProxy); diff != "" {
				t.Fatalf("#%d %s\nproxy fold: %s", i, s, diff)
			}
			if diff := wpEqual(viaVL, viaProxy); diff != "" {
				t.Fatalf("#%d %s\nthe two routes differ: %s", i, s, diff)
			}
			if len(queries) > 0 && strings.Contains(queries[0], "replace_regexp") {
				pushed++
			} else {
				folded++
			}
		}
	}
	// Both routes are exercised: names the cut takes and names it does not.
	if pushed < 100 || folded < 20 {
		t.Fatalf("scenarios folded by VictoriaLogs: %d, by the proxy: %d", pushed, folded)
	}
}

// Streams that tell apart only by the excluded label, that lack it, that hold
// it first, in the middle and last of the sorted pairs, and a value that looks
// like a pair: every operator, both functions, alone and under a `by` level.
//
// conformance: semantics/nested-aggregation-outer-by-native-routes, semantics/without-over-inner-aggregation-collapses, metric-series-identity
func TestWithoutPushdownDirectedStreams(t *testing.T) {
	mk := func(pairs ...string) wpStream {
		labels := map[string]string{}
		var parts []string
		for i := 0; i < len(pairs); i += 2 {
			labels[pairs[i]] = pairs[i+1]
			parts = append(parts, pairs[i]+`="`+wpEscape.Replace(pairs[i+1])+`"`)
		}
		return wpStream{labels, "{" + strings.Join(parts, ",") + "}"}
	}
	streams := []wpStream{
		mk("abc", "x", "app", "uw", "pod", "a", "zone", "x"),
		mk("abc", "y", "app", "uw", "pod", "a", "zone", "x"),
		mk("app", "uw", "pod", "a", "zone", "x"),
		mk("app", "uw", "pod", "b", "zone", "y"),
		mk("app", "uw", "note", `,pod="q"`, "pod", "a", "zone", "x"),
		mk("app", "uw", "note", `end\`, "zone", "y"),
		mk("app", "uw", "pod", "c", "zone", "x", "zzz", "1"),
		mk("app", "uw", "pod", "c", "zone", "x", "zzz", "2"),
	}
	base := int64(1_800_000_000) * 1e9
	for _, fn := range []string{"count_over_time", "rate"} {
		for _, op := range []string{"sum", "avg", "min", "max", "count"} {
			for _, excluded := range [][]string{{"abc"}, {"pod"}, {"zone"}, {"zzz"}, {"abc", "pod"}, {"note"}, {"pod", "zzz", "abc"}, {"baz"}} {
				for _, above := range [][]nvLevel{nil, {nvBy("max", "zone")}, {nvBy("sum", "pod", "zone"), nvBy("min")}} {
					s := nvScenario{fn: fn, window: time.Minute, step: time.Minute, levels: append([]nvLevel{nvWithout(op, excluded...)}, above...)}
					s.start = base - base%int64(time.Minute)
					s.end = s.start + int64(2*time.Minute)
					for i, st := range streams {
						for k := 0; k <= i%4+int(s.start/1e9)%2; k++ { // counts differ per stream and window
							s.rows = append(s.rows, emuRow{ts: s.start + int64(k)*1e9 + int64(i)*7e8 + 1, stream: st.text, msg: "x=1"})
						}
						s.rows = append(s.rows, emuRow{ts: s.start + int64(time.Minute) + int64(i)*3e8 + 1, stream: st.text, msg: "x=1"})
					}
					want := wpReference(s, streams)
					for _, off := range []bool{false, true} {
						got, queries := wpRun(t, s, off)
						if diff := wpEqual(want, got); diff != "" {
							t.Fatalf("%s pushdownOff=%v: %s\n%v", s.query(), off, diff, queries)
						}
					}
				}
			}
		}
	}
}

// Only a range query whose window is the step is answered by VictoriaLogs'
// buckets, so only it is folded there: a sliding window is evaluated by the
// proxy from the series rows, and an instant query asks for the rows too.
//
// conformance: semantics/without-over-inner-aggregation-collapses
func TestWithoutPushdownOnlyForTumblingWindows(t *testing.T) {
	streams := []wpStream{
		{map[string]string{"app": "uw", "pod": "a"}, `{app="uw",pod="a"}`},
		{map[string]string{"app": "uw", "pod": "b"}, `{app="uw",pod="b"}`},
	}
	base := int64(1_800_000_000) * 1e9
	mk := func(window, step time.Duration) nvScenario {
		s := nvScenario{fn: "count_over_time", window: window, step: step, levels: []nvLevel{nvWithout("avg", "pod")}}
		s.start = base - base%int64(time.Minute)
		s.end = s.start + int64(2*time.Minute)
		for i := 0; i < 8; i++ {
			s.rows = append(s.rows, emuRow{ts: s.start - int64(window) + int64(i+1)*int64(window)/9 + 1, stream: streams[i%2].text, msg: "x=1"})
		}
		return s
	}
	for _, tc := range []struct {
		name         string
		window, step time.Duration
		pushed       bool
	}{
		{"tumbling", time.Minute, time.Minute, true},
		{"sliding, window over step", 2 * time.Minute, time.Minute, false},
		{"sliding, window under step", 30 * time.Second, time.Minute, false},
	} {
		s := mk(tc.window, tc.step)
		got, queries := wpRun(t, s, false)
		if want := wpReference(s, streams); wpEqual(want, got) != "" {
			t.Errorf("%s: %s", tc.name, wpEqual(want, got))
		}
		if pushed := len(queries) > 0 && strings.Contains(strings.Join(queries, "\n"), "replace_regexp"); pushed != tc.pushed {
			t.Errorf("%s: folded by VictoriaLogs = %v: %v", tc.name, pushed, queries)
		}
	}
	// An instant query.
	s := mk(time.Minute, time.Minute)
	emu := &vlEmulator{t: t, rows: s.rows, field: "amount"}
	backend := httptest.NewServer(emu.handler())
	defer backend.Close()
	responseCache := cache.New(60*time.Second, 1000)
	p, err := New(Config{BackendURL: backend.URL, Cache: responseCache, LogLevel: "error"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Shutdown(context.Background()); responseCache.Close() })
	params := url.Values{"query": {s.query()}, "time": {strconv.FormatInt(s.end/1e9, 10)}}
	if res := doCompatProxyRequest(p, "/loki/api/v1/query?"+params.Encode(), map[string]string{"X-Scope-OrgID": "0"}); res.Code != http.StatusOK {
		t.Fatalf("%d %s", res.Code, res.Body)
	}
	for _, path := range []string{"/select/logsql/stats_query", "/select/logsql/stats_query_range"} {
		for _, query := range emu.endpointCalls(path) {
			if strings.Contains(query, "replace_regexp") {
				t.Errorf("an instant query was folded by VictoriaLogs: %s", query)
			}
		}
	}
}

// A custom field mapping renames a label in one direction or the other, and the
// fold then excludes the name the client wrote, which is not the stream's key:
// the proxy keeps folding the series rows itself.
//
// conformance: semantics/without-over-inner-aggregation-collapses
func TestWithoutPushdownLeavesMappedLabelsToTheProxyFold(t *testing.T) {
	streams := []wpStream{
		{map[string]string{"app": "uw", "pod": "a"}, `{app="uw",pod="a"}`},
		{map[string]string{"app": "uw", "pod": "b"}, `{app="uw",pod="b"}`},
	}
	base := int64(1_800_000_000) * 1e9
	s := nvScenario{fn: "count_over_time", window: time.Minute, step: time.Minute, levels: []nvLevel{nvWithout("sum", "pod")}}
	s.start = base - base%int64(time.Minute)
	s.end = s.start
	for i := 0; i < 6; i++ {
		s.rows = append(s.rows, emuRow{ts: s.start - int64(i+1)*1e9, stream: streams[i%2].text, msg: "x=1"})
	}
	for _, mapping := range []FieldMapping{{VLField: "pod", LokiLabel: "instance"}, {VLField: "host", LokiLabel: "pod"}} {
		_, queries := wpRun(t, s, false, mapping)
		if strings.Contains(strings.Join(queries, "\n"), "replace_regexp") {
			t.Errorf("%+v: folded by VictoriaLogs: %v", mapping, queries)
		}
	}
	if _, queries := wpRun(t, s, false); !strings.Contains(strings.Join(queries, "\n"), "replace_regexp") {
		t.Errorf("control: not folded by VictoriaLogs: %v", queries)
	}
}
