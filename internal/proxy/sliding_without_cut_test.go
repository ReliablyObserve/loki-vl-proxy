package proxy

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// A `sum without (...)` over count_over_time or bytes_over_time on a sliding
// window (range != step) has VictoriaLogs fold the series rows before the
// proxy computes the windows (withSlidingWithoutCut): the buckets of the
// streams that share the labels left are summed by VictoriaLogs, and a window
// of those sums is the sum of the streams' windows. This file pins that route
// to a reference written apart from it (Loki's evaluation: the window (t-range,
// t] of every stream with a line in it, then the fold) and to the proxy's own
// fold of the per-stream windows, byte for byte.

// swReference is Loki's answer: the count or byte sum of every stream with a
// line in (t-window, t], folded by the levels, innermost first.
func swReference(s nvScenario, streams []wpStream) map[string]map[int64]float64 {
	out := map[string]map[int64]float64{}
	byText := map[string]wpStream{}
	for _, st := range streams {
		byText[st.text] = st
	}
	for _, t := range s.times() {
		perStream := map[string]float64{}
		present := map[string]bool{}
		for _, row := range s.rows {
			if row.ts > t-int64(s.window) && row.ts <= t {
				present[row.stream] = true
				switch s.fn {
				case "bytes_over_time":
					perStream[row.stream] += float64(len(row.msg))
				case "rate":
					perStream[row.stream] += 1 / s.window.Seconds()
				default:
					perStream[row.stream]++
				}
			}
		}
		var vec []nvSeries
		for text := range present {
			labels := map[string]string{"service_name": "uw"}
			for name, v := range byText[text].labels {
				labels[name] = v
			}
			vec = append(vec, nvSeries{labels, perStream[text]})
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

// swRunner sends scenarios through the real handler of two proxies over one
// emulator, one with the cut and one without, so a test does not start a
// server and a proxy per scenario. The proxies cache nothing, so each answer
// is read from the scenario's rows.
type swRunner struct {
	emu     *vlEmulator
	proxies [2]*Proxy // [0] cuts, [1] folds
}

func newSWRunner(t testing.TB) *swRunner {
	t.Helper()
	emu := &vlEmulator{t: t, field: "amount"}
	backend := httptest.NewServer(emu.handler())
	t.Cleanup(backend.Close)
	r := &swRunner{emu: emu}
	for i := range r.proxies {
		c := cache.NewDisabled()
		p, err := New(Config{BackendURL: backend.URL, Cache: c, LogLevel: "error"})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = p.Shutdown(context.Background()); c.Close() })
		r.proxies[i] = p
	}
	t.Cleanup(func() { withoutPushdownOff.Store(false) })
	return r
}

// run returns the body of the scenario's query_range answer and the stats
// queries VictoriaLogs received for it.
func (r *swRunner) run(t testing.TB, s nvScenario, cutOff bool) (string, []string) {
	t.Helper()
	r.emu.mu.Lock()
	r.emu.rows, r.emu.calls = s.rows, nil
	r.emu.mu.Unlock()
	withoutPushdownOff.Store(cutOff)
	p := r.proxies[0]
	if cutOff {
		p = r.proxies[1]
	}
	params := url.Values{"query": {s.query()}, "start": {strconv.FormatInt(s.start/1e9, 10)}, "end": {strconv.FormatInt(s.end/1e9, 10)}, "step": {strconv.FormatInt(int64(s.step/time.Second), 10)}}
	res := doCompatProxyRequest(p, "/loki/api/v1/query_range?"+params.Encode(), map[string]string{"X-Scope-OrgID": "0"})
	if res.Code != http.StatusOK {
		t.Fatalf("%s: %d %s", s, res.Code, res.Body)
	}
	return res.Body.String(), r.emu.endpointCalls("/select/logsql/stats_query_range")
}

// swParseMatrix reads a matrix body as labels key -> time -> value, without
// the detected_level the proxy adds.
func swParseMatrix(t testing.TB, s nvScenario, body string) map[string]map[int64]float64 {
	t.Helper()
	var resp struct {
		Data struct {
			Result []struct {
				Metric map[string]string `json:"metric"`
				Values [][]any           `json:"values"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal([]byte(body), &resp); err != nil {
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
	return got
}

// swGenerateScenario builds streams, rows (some on window edges and one
// nanosecond off them, with gaps) and a sliding window over them.
func swGenerateScenario(rnd *rand.Rand, fn string) (nvScenario, []wpStream) {
	streams := wpGenerateStreams(rnd)
	type grid struct{ window, step time.Duration }
	grids := []grid{{time.Minute, 30 * time.Second}, {90 * time.Second, 30 * time.Second}, {5 * time.Minute, time.Minute}, {time.Minute, 5 * time.Second},
		{30 * time.Second, time.Minute}, {time.Minute, 90 * time.Second}, {2 * time.Minute, 50 * time.Second}}
	g := grids[rnd.Intn(len(grids))]
	s := nvScenario{fn: fn, window: g.window, step: g.step, trailingBy: rnd.Intn(2) == 0}
	base := int64(1_800_000_000) * 1e9
	s.start = base + int64(rnd.Intn(600))*1e9
	s.end = s.start + int64(rnd.Intn(8))*int64(g.step)
	lo, hi := s.start-int64(s.window)-5e9, s.end+5e9
	msgs := []string{"x=1", "a longer line", "x"}
	for i, n := 0, 10+rnd.Intn(80); i < n; i++ {
		ts := lo + rnd.Int63n(hi-lo)
		switch rnd.Intn(5) {
		case 0: // on an evaluation time or a window's start, or 1ns off either
			ts = s.start + int64(rnd.Intn(8))*int64(g.step) - int64(rnd.Intn(2))*int64(s.window) + int64(rnd.Intn(3)-1)
		case 1: // a gap: every line of the scenario in the first half
			ts = lo + rnd.Int63n((hi-lo)/2)
		}
		stream := streams[rnd.Intn(len(streams))]
		if rnd.Intn(2) == 0 {
			stream = streams[rnd.Intn(min(2, len(streams)))]
		}
		s.rows = append(s.rows, emuRow{ts: ts, stream: stream.text, msg: msgs[rnd.Intn(len(msgs))]})
	}
	excluded := []string{"abc", "pod", "node", "zone", "svc", "rack", "baz", "app", "pod_name", "container", "note"}
	rnd.Shuffle(len(excluded), func(i, j int) { excluded[i], excluded[j] = excluded[j], excluded[i] })
	op := "sum"
	if rnd.Intn(5) == 0 {
		op = []string{"avg", "min", "max", "count"}[rnd.Intn(4)]
	}
	s.levels = []nvLevel{nvWithout(op, excluded[:1+rnd.Intn(3)]...)}
	if rnd.Intn(6) == 0 {
		s.levels = append(s.levels, nvBy("max", "zone"))
	}
	return s, streams
}

// swCheck sends the scenario through both routes and requires them to agree
// byte for byte and with Loki's answer; it reports whether VictoriaLogs folded
// the streams (the cut), which only a sum of a count or a byte sum with names
// the cut takes does.
func swCheck(t testing.TB, runner *swRunner, label string, s nvScenario, streams []wpStream, fn string) bool {
	t.Helper()
	want := swReference(s, streams)
	viaVL, queries := runner.run(t, s, false)
	viaProxy, _ := runner.run(t, s, true)
	if viaVL != viaProxy {
		t.Fatalf("%s %s\nthe two routes differ:\ncut:  %s\nfold: %s\nqueries: %v", label, s, viaVL, viaProxy, queries)
	}
	if diff := wpEqual(want, swParseMatrix(t, s, viaVL)); diff != "" {
		t.Fatalf("%s %s\nagainst Loki: %s\nqueries: %v", label, s, diff, queries)
	}
	pushed := strings.Contains(strings.Join(queries, "\n"), "replace_regexp")
	wantPushed := fn != "rate" && len(s.levels) == 1 && s.levels[0].op == "sum" && swCutTakes(s.levels[0].by)
	if pushed != wantPushed {
		t.Fatalf("%s %s: folded by VictoriaLogs = %v, want %v: %v", label, s, pushed, wantPushed, queries)
	}
	return pushed
}

// conformance: semantics/without-sliding-sum-folded-buckets, semantics/without-over-inner-aggregation-collapses, metric-series-identity
func TestSlidingSumWithoutCutMatchesFoldAndLoki(t *testing.T) {
	cut, folded := 0, 0
	runner := newSWRunner(t)
	for _, fn := range []string{"count_over_time", "bytes_over_time", "rate"} {
		rnd := rand.New(rand.NewSource(int64(len(fn)) * 7919))
		for i := 0; i < 120; i++ {
			s, streams := swGenerateScenario(rnd, fn)
			if swCheck(t, runner, fmt.Sprintf("#%d", i), s, streams, fn) {
				cut++
			} else {
				folded++
			}
		}
	}
	if cut < 60 || folded < 60 {
		t.Fatalf("scenarios folded by VictoriaLogs: %d, by the proxy: %d", cut, folded)
	}
}

// swCutTakes reports whether every excluded name is one the stream cut takes
// (translator.pushDownLabel): letters and digits, not a derived label or a
// service_name source.
func swCutTakes(names []string) bool {
	for _, name := range names {
		switch name {
		case "app", "pod_name", "container":
			return false
		}
	}
	return true
}

// Streams that tell apart only by the excluded label, that lack it, that hold
// it first, in the middle and last of the sorted pairs, a value that looks like
// a pair, lines on both window edges and 1ns off them, and an empty line.
//
// conformance: semantics/without-sliding-sum-folded-buckets, metric-series-identity
func TestSlidingSumWithoutCutDirectedStreams(t *testing.T) {
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
	runner := newSWRunner(t)
	for _, fn := range []string{"count_over_time", "bytes_over_time"} {
		for _, excluded := range [][]string{{"abc"}, {"pod"}, {"zone"}, {"zzz"}, {"abc", "pod"}, {"note"}, {"pod", "zzz", "abc"}, {"baz"}} {
			for _, g := range []struct{ window, step time.Duration }{{time.Minute, 20 * time.Second}, {30 * time.Second, time.Minute}} {
				s := nvScenario{fn: fn, window: g.window, step: g.step, levels: []nvLevel{nvWithout("sum", excluded...)}}
				s.start = base - base%int64(time.Minute)
				s.end = s.start + int64(3*g.step)
				for i, st := range streams {
					for _, t := range s.times() {
						for _, off := range []int64{-int64(s.window), -int64(s.window) + 1, -1, 0, 1} {
							if (int64(i)+off+t/1e9)%3 != 0 {
								s.rows = append(s.rows, emuRow{ts: t + off, stream: st.text, msg: strings.Repeat("x", i+1)})
							}
						}
					}
				}
				want := swReference(s, streams)
				viaVL, queries := runner.run(t, s, false)
				viaProxy, _ := runner.run(t, s, true)
				if viaVL != viaProxy {
					t.Fatalf("%s: the two routes differ:\ncut:  %s\nfold: %s", s, viaVL, viaProxy)
				}
				if diff := wpEqual(want, swParseMatrix(t, s, viaVL)); diff != "" {
					t.Fatalf("%s: %s\n%v", s, diff, queries)
				}
				if !strings.Contains(strings.Join(queries, "\n"), "replace_regexp") {
					t.Fatalf("%s: not folded by VictoriaLogs: %v", s, queries)
				}
			}
		}
	}
}

// Only a `sum` of a count or a byte sum over a selector and line filters
// takes the cut on a sliding window: a rate (a float divide per series), any
// other operator, a nest, a parser or a label stage keep the proxy's fold of
// the per-stream windows, and so does a tumbling window (folded by
// PushDownWithout) and an instant query.
//
// conformance: semantics/without-sliding-sum-folded-buckets
func TestSlidingSumWithoutCutScope(t *testing.T) {
	base := int64(1_800_000_000) * 1e9
	start := base - base%int64(time.Minute)
	rows := []emuRow{{ts: start - 1e9, stream: `{app="uw",pod="a"}`, msg: "x=1"}, {ts: start - 2e9, stream: `{app="uw",pod="b"}`, msg: "x=1"}}
	for _, tc := range []struct {
		query string
		step  string
		cut   bool
	}{
		{`sum without (pod) (count_over_time({app="uw"}[1m]))`, "20", true},
		{`sum without (pod) (bytes_over_time({app="uw"} |= "x" != "y"[1m]))`, "20", true},
		{`sum(count_over_time({app="uw"}[1m])) without (pod)`, "20", true},
		{`sum without (pod) (count_over_time({app="uw"}[1m]))`, "60", true}, // tumbling: PushDownWithout
		{`sum without (pod) (rate({app="uw"}[1m]))`, "20", false},
		{`sum without (pod) (bytes_rate({app="uw"}[1m]))`, "20", false},
		{`avg without (pod) (count_over_time({app="uw"}[1m]))`, "20", false},
		{`max by (app) (sum without (pod) (count_over_time({app="uw"}[1m])))`, "20", false},
		{`sum without (pod) (count_over_time({app="uw"} | logfmt [1m]))`, "20", false},
		{`sum without (pod) (count_over_time({app="uw"} | json | __error__="" [1m]))`, "20", false},
		{`sum without (pod) (count_over_time({app="uw"} | drop zone [1m]))`, "20", false},
		{`sum without (pod) (count_over_time({app="uw"} | label_format pod=app [1m]))`, "20", false},
		{`sum without (level) (count_over_time({app="uw"}[1m]))`, "20", false},
		{`topk(2, sum without (pod) (count_over_time({app="uw"}[1m])))`, "20", true}, // the ranking takes the folded series (TestSlidingSumWithoutCutUnderTopk)
		{`sum without (pod) (count_over_time({app="uw"}[1m])) / 2`, "20", true},      // the operand is evaluated alone
	} {
		emu := &vlEmulator{t: t, rows: rows, field: "amount"}
		backend := httptest.NewServer(emu.handler())
		responseCache := cache.New(60*time.Second, 1000)
		p, err := New(Config{BackendURL: backend.URL, Cache: responseCache, LogLevel: "error"})
		if err != nil {
			t.Fatal(err)
		}
		params := url.Values{"query": {tc.query}, "start": {strconv.FormatInt(start/1e9, 10)}, "end": {strconv.FormatInt(start/1e9+60, 10)}, "step": {tc.step}}
		res := doCompatProxyRequest(p, "/loki/api/v1/query_range?"+params.Encode(), map[string]string{"X-Scope-OrgID": "0"})
		if res.Code != http.StatusOK {
			t.Errorf("%s: %d %s", tc.query, res.Code, res.Body)
		}
		var all []string
		for _, path := range []string{"/select/logsql/stats_query_range", "/select/logsql/stats_query", "/select/logsql/query"} {
			all = append(all, emu.endpointCalls(path)...)
		}
		cut := strings.Contains(strings.Join(all, "\n"), "replace_regexp")
		if cut != tc.cut {
			t.Errorf("%s step %s: folded by VictoriaLogs = %v, want %v: %v", tc.query, tc.step, cut, tc.cut, all)
		}
		_ = p.Shutdown(context.Background())
		responseCache.Close()
		backend.Close()
	}
	params := url.Values{"query": {`sum without (pod) (count_over_time({app="uw"}[1m]))`}, "time": {strconv.FormatInt(start/1e9, 10)}}
	emu := &vlEmulator{t: t, rows: rows, field: "amount"}
	backend := httptest.NewServer(emu.handler())
	defer backend.Close()
	responseCache := cache.New(60*time.Second, 1000)
	defer responseCache.Close()
	p, err := New(Config{BackendURL: backend.URL, Cache: responseCache, LogLevel: "error"})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = p.Shutdown(context.Background()) }()
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

// With the extent cache on, as the stack runs, a request and a refresh one
// step later answer what the per-stream route answers, and the cut read is
// one the extent cache keeps like any stats_query_range call.
//
// conformance: semantics/without-sliding-sum-folded-buckets
func TestSlidingSumWithoutCutExtentCache(t *testing.T) {
	s0 := time.Now().Add(-3 * time.Hour).Truncate(time.Hour)
	var rows []emuRow
	for i := 0; i < 600; i++ {
		stream := fmt.Sprintf(`{app="uw",pod="p%d",zone="%s"}`, i%37, []string{"x", "y"}[i%2])
		rows = append(rows, emuRow{ts: s0.UnixNano() + int64(i)*int64(11*time.Second) + int64(i%7)*1e6, stream: stream, msg: "x=1"})
	}
	query := `sum without (pod) (count_over_time({app="uw"}[1m]))`
	run := func(cutOff bool) ([2]string, int64, []string) {
		emu := &vlEmulator{t: t, rows: rows, field: "amount"}
		backend, shim := newExtentShim(t, emu.handler())
		p := newExtentTestProxy(t, backend.URL, true, 64<<20)
		p.labelTranslator = NewLabelTranslator(LabelStyleUnderscores, nil)
		withoutPushdownOff.Store(cutOff)
		defer withoutPushdownOff.Store(false)
		var bodies [2]string
		for i, shift := range []time.Duration{0, 20 * time.Second} {
			start, end := s0.Add(10*time.Minute+shift), s0.Add(70*time.Minute+shift)
			params := url.Values{"query": {query + strings.Repeat(" ", i)}, "start": {strconv.FormatInt(start.Unix(), 10)}, "end": {strconv.FormatInt(end.Unix(), 10)}, "step": {"20"}}
			res := doCompatProxyRequest(p, "/loki/api/v1/query_range?"+params.Encode(), map[string]string{"X-Scope-OrgID": "0"})
			if res.Code != http.StatusOK {
				t.Fatalf("cutOff=%v: %d %s", cutOff, res.Code, res.Body)
			}
			bodies[i] = res.Body.String()
			if i == 0 {
				p.statsExtentStores.wait()
				shim.reset()
			}
		}
		_, buckets := shim.bucketsRead()
		return bodies, buckets, emu.endpointCalls("/select/logsql/stats_query_range")
	}
	cut, cutRefresh, cutQueries := run(false)
	fold, foldRefresh, _ := run(true)
	t.Logf("buckets read by the refresh: cut %d, per-stream %d", cutRefresh, foldRefresh)
	if cutRefresh > foldRefresh || cutRefresh > 12 {
		t.Fatalf("the refresh read %d buckets, the per-stream route %d: the kept buckets were not reused", cutRefresh, foldRefresh)
	}
	for i := range cut {
		if cut[i] != fold[i] {
			t.Fatalf("request %d: the two routes differ:\ncut:  %.600s\nfold: %.600s", i, cut[i], fold[i])
		}
		if !strings.Contains(cut[i], `"zone":"x"`) || strings.Contains(cut[i], `"pod"`) {
			t.Fatalf("request %d: %.600s", i, cut[i])
		}
	}
	// The cut read is a call the extent cache keeps: every stats pipe of it is
	// grouped by the bucket, so each bucket's answer depends on its rows alone
	// (statsExtentQueryCacheable).
	var cutRead string
	for _, q := range cutQueries {
		if strings.Contains(q, "replace_regexp") {
			cutRead = q
		}
	}
	if cutRead == "" {
		t.Fatalf("not folded by VictoriaLogs: %v", cutQueries)
	}
	if !statsExtentQueryCacheable(cutRead) {
		t.Fatalf("the extent cache does not keep the cut read: %s", cutRead)
	}
}

// A ranking over the fold takes the folded series: topk and bottomk over the
// cut read answer what they answer over the per-stream route, byte for byte.
//
// conformance: semantics/without-sliding-sum-folded-buckets
func TestSlidingSumWithoutCutUnderTopk(t *testing.T) {
	base := int64(1_800_000_000) * 1e9
	runner := newSWRunner(t)
	var rows []emuRow
	start := base - base%int64(time.Minute)
	for i := 0; i < 400; i++ {
		stream := fmt.Sprintf(`{app="uw",pod="p%d",zone="%s"}`, i%13, []string{"x", "y", "z"}[i%3])
		rows = append(rows, emuRow{ts: start - 120e9 + int64(i)*350e6, stream: stream, msg: "x=1"})
	}
	for _, query := range []string{
		`topk(2, sum without (pod) (count_over_time({app="uw"}[1m])))`,
		`bottomk(2, sum without (pod) (count_over_time({app="uw"}[90s])))`,
		`topk(1, sum without (pod) (bytes_over_time({app="uw"}[1m])))`,
	} {
		var bodies [2]string
		var cut bool
		for i, cutOff := range []bool{false, true} {
			runner.emu.mu.Lock()
			runner.emu.rows, runner.emu.calls = rows, nil
			runner.emu.mu.Unlock()
			withoutPushdownOff.Store(cutOff)
			params := url.Values{"query": {query}, "start": {strconv.FormatInt(start/1e9-60, 10)}, "end": {strconv.FormatInt(start/1e9+60, 10)}, "step": {"20"}}
			res := doCompatProxyRequest(runner.proxies[i], "/loki/api/v1/query_range?"+params.Encode(), map[string]string{"X-Scope-OrgID": "0"})
			if res.Code != http.StatusOK {
				t.Fatalf("%s: %d %s", query, res.Code, res.Body)
			}
			bodies[i] = res.Body.String()
			if !cutOff {
				cut = strings.Contains(strings.Join(runner.emu.endpointCalls("/select/logsql/stats_query_range"), "\n"), "replace_regexp")
			}
		}
		if !cut || !strings.Contains(bodies[0], `"values":[[`) {
			t.Fatalf("%s: folded by VictoriaLogs = %v: %.300s", query, cut, bodies[0])
		}
		if bodies[0] != bodies[1] {
			t.Fatalf("%s: the two routes differ:\ncut:  %.600s\nfold: %.600s", query, bodies[0], bodies[1])
		}
	}
}
