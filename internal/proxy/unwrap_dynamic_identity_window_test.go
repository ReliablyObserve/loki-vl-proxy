package proxy

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/testutil/vlemu"
)

// This file pins a parser's series identity over a range longer than the step (sliding windows) to Loki's answer,
// with a reference evaluator written apart from the proxy, from Loki's documented semantics: a series is the stream
// and every label the parser extracts less the unwrapped one (pkg/logql/log/metrics_extraction.go), the range
// function is taken per series over the window (t-range, t] at every step (pkg/logql/range_vector.go), and the outer
// sum, avg, max or min folds the series into the group its labels name (pkg/logql/evaluator.go
// VectorAggEvaluator.Next). The proxy under test is the real handler; VictoriaLogs is the vlemu emulator, which
// computes the series hash from each line.

// dynWindowScenario is one sliding-window request of a parser's series identity.
type dynWindowScenario struct {
	outer, by, fn, parser string
	window, step          time.Duration
	start, end            int64 // ns, step-aligned evaluation times
	rows                  []vlemu.Row
}

func (s dynWindowScenario) query() string {
	clause := ""
	if s.by != "" {
		clause = " by (" + s.by + ")"
	}
	return fmt.Sprintf(`%s%s (%s({app="dw"} | %s | unwrap amount [%s]))`, s.outer, clause, s.fn, s.parser, uwWindowText(s.window))
}

func (s dynWindowScenario) times() []int64 {
	var out []int64
	for t := s.start; t <= s.end; t += int64(s.step) {
		out = append(out, t)
	}
	return out
}

// dynWindowRow is a row of stream pod p with the parsed labels labels and value v, its line written by parser.
func dynWindowRow(ts int64, pod, parser string, labels map[string]string, v float64) vlemu.Row {
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	amount := strconv.FormatFloat(v, 'f', -1, 64)
	var line string
	if parser == "json" {
		fields := []string{`"amount":` + amount}
		for _, k := range keys {
			fields = append(fields, strconv.Quote(k)+":"+strconv.Quote(labels[k]))
		}
		line = "{" + strings.Join(fields, ",") + "}"
	} else {
		fields := []string{"amount=" + amount}
		for _, k := range keys {
			fields = append(fields, k+"="+labels[k])
		}
		line = strings.Join(fields, " ")
	}
	return vlemu.Row{TS: ts, Stream: `{app="dw",pod="` + pod + `"}`, Labels: labels, Value: v, Msg: line}
}

// generateDynWindowScenario draws a sliding window (a range longer than the step, a multiple of it or not), and rows
// of three streams: lines that differ only in the value (one series), a parsed label more (a series of its own), a
// key of its own on a line now and then (a series per line), and lines on window edges.
func generateDynWindowScenario(rnd *rand.Rand, outer, by, fn, parser string) dynWindowScenario {
	steps := []time.Duration{20 * time.Second, 30 * time.Second, time.Minute}
	s := dynWindowScenario{outer: outer, by: by, fn: fn, parser: parser, step: steps[rnd.Intn(len(steps))]}
	// A range longer than the step: twice, five times, or a range the step does not divide.
	switch rnd.Intn(3) {
	case 0:
		s.window = 2 * s.step
	case 1:
		s.window = 5 * s.step
	default:
		s.window = s.step + s.step/2 + 10*time.Second
	}
	base := int64(1_800_000_000) * 1e9
	s.start = base + int64(rnd.Intn(600))*1e9
	s.start -= s.start % int64(s.step)
	s.end = s.start + int64(3+rnd.Intn(8))*int64(s.step)
	pods := []string{"a", "b", "c"}
	labelSets := []map[string]string{{"route": "/x"}, {"route": "/x", "region": "eu"}, {"route": "/y"}}
	values := []float64{0, 0.5, 1, 2.25, 3, 100.125, 7, 42}
	lo, hi := s.start-int64(s.window)-5e9, s.end+5e9
	n := 10 + rnd.Intn(70)
	for i := 0; i < n; i++ {
		ts := lo + rnd.Int63n(hi-lo)
		if rnd.Intn(5) == 0 { // on a window edge
			edge := s.start + int64(rnd.Intn(8))*int64(s.step)
			if rnd.Intn(2) == 0 {
				edge -= int64(s.window)
			}
			ts = edge + int64(rnd.Intn(3)-1)
		}
		labels := map[string]string{}
		for k, v := range labelSets[rnd.Intn(len(labelSets))] {
			labels[k] = v
		}
		if rnd.Intn(6) == 0 {
			labels["req"] = "r" + strconv.Itoa(i) // a series of its own
		}
		s.rows = append(s.rows, dynWindowRow(ts, pods[rnd.Intn(len(pods))], parser, labels, values[rnd.Intn(len(values))]))
	}
	return s
}

// lokiDynamicWindows is the reference: at each evaluation time, the samples of each series (the stream and every
// parsed label) in (t-window, t], the range function per series, then the outer aggregation per group of the by
// labels (the stream's pod or a parsed label). A group with no series in the window has no point.
func lokiDynamicWindows(s dynWindowScenario) map[string]map[int64]float64 {
	out := map[string]map[int64]float64{}
	for _, t := range s.times() {
		perSeries := map[string][]float64{}
		groupOf := map[string]string{}
		for _, row := range s.rows {
			if row.TS <= t-int64(s.window) || row.TS > t {
				continue
			}
			labels := map[string]string{"app": "dw", "pod": streamLabel(row.Stream, "pod")}
			for k, v := range row.Labels {
				labels[k] = v
			}
			key := labelsSortedKey(labels)
			perSeries[key] = append(perSeries[key], row.Value)
			group := map[string]string{}
			if s.by != "" {
				for _, l := range strings.Split(s.by, ", ") {
					if v := labels[l]; v != "" {
						group[l] = v
					}
				}
			}
			groupOf[key] = labelsSortedKey(group)
		}
		groups := map[string][]float64{}
		for key, vs := range perSeries {
			groups[groupOf[key]] = append(groups[groupOf[key]], rangeFuncOf(s.fn, vs))
		}
		for g, vs := range groups {
			if out[g] == nil {
				out[g] = map[int64]float64{}
			}
			out[g][t] = outerFuncOf(s.outer, vs)
		}
	}
	return out
}

func labelsSortedKey(m map[string]string) string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k+"="+m[k])
	}
	sort.Strings(keys)
	return strings.Join(keys, ",")
}

// rangeFuncOf is a range function over one series' samples in a window; stddev and stdvar are the population ones.
func rangeFuncOf(fn string, vs []float64) float64 {
	var sum float64
	for _, v := range vs {
		sum += v
	}
	mean := sum / float64(len(vs))
	switch fn {
	case "avg_over_time":
		return mean
	case "max_over_time":
		return slices.Max(vs)
	case "min_over_time":
		return slices.Min(vs)
	case "stddev_over_time", "stdvar_over_time":
		var m2 float64
		for _, v := range vs {
			m2 += (v - mean) * (v - mean)
		}
		if fn == "stdvar_over_time" {
			return m2 / float64(len(vs))
		}
		return math.Sqrt(m2 / float64(len(vs)))
	}
	panic("reference: " + fn)
}

func outerFuncOf(op string, vs []float64) float64 {
	switch op {
	case "sum", "avg":
		var sum float64
		for _, v := range vs {
			sum += v
		}
		if op == "avg" {
			return sum / float64(len(vs))
		}
		return sum
	case "max":
		return slices.Max(vs)
	case "min":
		return slices.Min(vs)
	}
	panic("reference: " + op)
}

// runDynWindowScenario sends the scenario through the real handler and returns its series (labels key -> time ->
// value) and the response status and body.
func runDynWindowScenario(t *testing.T, emu *vlemu.Emulator, s dynWindowScenario, cfg Config) (map[string]map[int64]float64, int, string) {
	t.Helper()
	backend := httptest.NewServer(emu.Handler())
	defer backend.Close()
	cfg.BackendURL, cfg.LogLevel, cfg.BackendMinVersion, cfg.BackendVersionCheckTimeout = backend.URL, "error", "v1.49.0", time.Second
	if cfg.Cache == nil {
		c := cache.New(60*time.Second, 1000)
		defer c.Close()
		cfg.Cache = c
	}
	p, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { closeTestProxy(p) }()
	if emu.Version != "" {
		if err := p.ValidateBackendVersionCompatibility(t.Context()); err != nil {
			t.Fatal(err)
		}
	}
	params := url.Values{"query": {s.query()}, "start": {strconv.FormatInt(s.start/1e9, 10)}, "end": {strconv.FormatInt(s.end/1e9, 10)},
		"step": {strconv.FormatInt(int64(s.step/time.Second), 10)}}
	res := doCompatProxyRequest(p, "/loki/api/v1/query_range?"+params.Encode(), map[string]string{"X-Scope-OrgID": "0"})
	if res.Code != http.StatusOK {
		return nil, res.Code, res.Body.String()
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
		key := labelsSortedKey(series.Metric)
		got[key] = map[int64]float64{}
		for _, point := range series.Values {
			v, _ := strconv.ParseFloat(point[1].(string), 64)
			got[key][int64(point[0].(float64))*1e9] = v
		}
	}
	return got, res.Code, res.Body.String()
}

// closeTestProxy shuts a scenario's proxy down with its caches.
func closeTestProxy(p *Proxy) {
	_ = p.Shutdown(context.Background())
	p.translationCache.Close()
	p.streamFieldNamesCache.Close()
}

func compareDynWindows(t *testing.T, label string, s dynWindowScenario, got, want map[string]map[int64]float64) {
	t.Helper()
	ctx := fmt.Sprintf("\nquery %s step=%v window=%v rows=%d", s.query(), s.step, s.window, len(s.rows))
	for key, points := range want {
		g, ok := got[key]
		if !ok {
			t.Fatalf("%s: no series {%s}, Loki has %d points%s", label, key, len(points), ctx)
		}
		for ts, w := range points {
			v, ok := g[ts]
			if !ok {
				t.Fatalf("%s: {%s} has no point at %d, Loki has %g%s", label, key, ts/1e9, w, ctx)
			}
			if math.Abs(v-w) > 1e-9*math.Max(1, math.Abs(w)) {
				t.Fatalf("%s: {%s} at %d is %g, Loki has %g%s", label, key, ts/1e9, v, w, ctx)
			}
		}
		for ts, v := range g {
			if _, ok := points[ts]; !ok {
				t.Fatalf("%s: {%s} has a point at %d = %g, Loki has none%s", label, key, ts/1e9, v, ctx)
			}
		}
	}
	for key := range got {
		if _, ok := want[key]; !ok {
			t.Fatalf("%s: series {%s} Loki does not have%s", label, key, ctx)
		}
	}
}

// TestDynamicIdentityWindowsMatchLokiReference: over generated rows and sliding windows (a range twice or five times
// the step, and one the step does not divide), sum, avg, max and min, with no clause, by the stream's pod or by a
// parsed label, over avg, max, min, stddev and stdvar over time after a json or a logfmt parser, answer Loki's series
// and values at every step, on VictoriaLogs v1.49.0 and v1.53.0. VictoriaLogs answers one stats query grouped by the
// series hash on anchored buckets; no raw rows are read, and the hash never reaches a client.
//
// conformance: semantics/unwrap-per-series-identity-raw-rows, metric-series-identity, window-bounds-and-step-alignment
func TestDynamicIdentityWindowsMatchLokiReference(t *testing.T) {
	type shape struct{ outer, by string }
	shapes := []shape{{"sum", ""}, {"sum", "pod"}, {"avg", ""}, {"max", "pod"}, {"min", "route"}, {"sum", "pod, route"}}
	fns := []string{"avg_over_time", "max_over_time", "min_over_time", "stddev_over_time", "stdvar_over_time"}
	for _, version := range []string{"v1.49.0", "v1.53.0"} {
		for _, parser := range []string{"json", "logfmt"} {
			for _, fn := range fns {
				for _, sh := range shapes {
					if sh.outer == "max" && fn == "max_over_time" || sh.outer == "min" && fn == "min_over_time" {
						// The extreme of the group's samples: no series hash (TestDynamicIdentityWindowsKeepExtremes).
						continue
					}
					name := fmt.Sprintf("%s/%s/%s/%s_by_%s", version, parser, fn, sh.outer, sh.by)
					t.Run(name, func(t *testing.T) {
						rnd := rand.New(rand.NewSource(int64(len(name))*7919 + 31))
						for i := 0; i < 6; i++ {
							s := generateDynWindowScenario(rnd, sh.outer, sh.by, fn, parser)
							emu := &vlemu.Emulator{T: t, Rows: s.rows, Field: "amount", Version: version}
							got, code, body := runDynWindowScenario(t, emu, s, Config{})
							if code != http.StatusOK {
								t.Fatalf("#%d %s: %d %s", i, s.query(), code, body)
							}
							if strings.Contains(body, dynamicSeriesHashLabel) {
								t.Fatalf("#%d %s: the answer names the series hash: %.300s", i, s.query(), body)
							}
							compareDynWindows(t, fmt.Sprintf("#%d", i), s, got, lokiDynamicWindows(s))
							if raw := emu.EndpointCalls("/select/logsql/query"); len(raw) > 0 {
								t.Fatalf("#%d %s: read raw rows: %s", i, s.query(), raw[0])
							}
							calls, counts := identityStatsCalls(emu)
							if len(calls) != 1 || counts != 1 {
								t.Fatalf("#%d %s: VictoriaLogs read %d bucket queries grouped by the series hash and %d row counts, want one each", i, s.query(), len(calls), counts)
							}
						}
					})
				}
			}
		}
	}
}

// max by (L) of max_over_time and min by (L) of min_over_time over a range longer than the step are the extreme of
// the group's samples in the window, whatever the series: they read no series hash and answer Loki's values.
//
// conformance: semantics/unwrap-per-series-identity-raw-rows
func TestDynamicIdentityWindowsKeepExtremes(t *testing.T) {
	for _, tc := range []struct{ outer, fn string }{{"max", "max_over_time"}, {"min", "min_over_time"}} {
		rnd := rand.New(rand.NewSource(int64(len(tc.fn)) * 104729))
		for i := 0; i < 6; i++ {
			s := generateDynWindowScenario(rnd, tc.outer, "pod", tc.fn, "logfmt")
			emu := &vlemu.Emulator{T: t, Rows: s.rows, Field: "amount", Version: "v1.53.0"}
			got, code, body := runDynWindowScenario(t, emu, s, Config{})
			if code != http.StatusOK {
				t.Fatalf("%s: %d %s", s.query(), code, body)
			}
			compareDynWindows(t, fmt.Sprintf("%s #%d", tc.fn, i), s, got, lokiDynamicWindows(s))
			for _, q := range emu.EndpointCalls("/select/logsql/stats_query_range") {
				if strings.Contains(q, "__lvp_sid") {
					t.Fatalf("%s: VictoriaLogs read the series hash: %s", s.query(), q)
				}
			}
		}
	}
}

// Loki counts max_query_series on the folded answer: a sum by (pod) over more series than the tenant's limit (a key
// of its own on every line) answers its two groups, the reads bounded by the raw-row budget of a request that folds
// its series (-manual-range-metric-row-limit). Below that budget, the answer is Loki's limit error naming the flag's
// budget, never a partial answer.
//
// conformance: semantics/unwrap-per-series-identity-raw-rows, metric-series-identity
func TestDynamicIdentityWindowsFoldBeyondSeriesLimit(t *testing.T) {
	s := dynWindowScenario{outer: "sum", by: "pod", fn: "avg_over_time", parser: "logfmt", window: 2 * time.Minute, step: 30 * time.Second}
	base := int64(1_800_000_000) * 1e9
	s.start, s.end = base, base+10*int64(30*time.Second)
	for i := 0; i < 1200; i++ { // 1200 series, more than twice the default limit of 500
		ts := s.start - int64(s.window) + int64(i)*int64(time.Second)/3 + 7e6
		pod := []string{"a", "b"}[i%2]
		s.rows = append(s.rows, dynWindowRow(ts, pod, "logfmt", map[string]string{"req": "r" + strconv.Itoa(i)}, float64(i%17)))
	}
	emu := &vlemu.Emulator{T: t, Rows: s.rows, Field: "amount", Version: "v1.53.0"}
	got, code, body := runDynWindowScenario(t, emu, s, Config{})
	if code != http.StatusOK {
		t.Fatalf("%s: %d %s", s.query(), code, body)
	}
	if len(got) != 2 {
		t.Fatalf("%d series, want the two pods", len(got))
	}
	compareDynWindows(t, "beyond the limit", s, got, lokiDynamicWindows(s))
	// A budget under the series the window reads: Loki's limit error, which names the budget.
	emu = &vlemu.Emulator{T: t, Rows: s.rows, Field: "amount", Version: "v1.53.0"}
	_, code, body = runDynWindowScenario(t, emu, s, Config{RangeMetricRowLimit: 600})
	if code == http.StatusOK || !strings.Contains(body, "maximum") && !strings.Contains(body, "limit") {
		t.Fatalf("a budget of 600 over 1200 series answered %d %.300s, want a limit error", code, body)
	}
	t.Logf("budget of 600: %d %.200s", code, body)
}

// dynChunkScenario is a sliding window over many lines: a key of its own on most lines (a series per line) and a few
// series of many lines, over 40 minutes at a 30 s step under [2m].
func dynChunkScenario(lines int, perLine bool) dynWindowScenario {
	s := dynWindowScenario{outer: "sum", by: "pod", fn: "avg_over_time", parser: "logfmt", window: 2 * time.Minute, step: 30 * time.Second}
	base := int64(1_800_000_000) * 1e9
	s.start, s.end = base, base+80*int64(30*time.Second)
	span := s.end - s.start + int64(s.window)
	for i := 0; i < lines; i++ {
		ts := s.start - int64(s.window) + int64(i)*span/int64(lines) + 7e6
		labels := map[string]string{"route": []string{"/x", "/y"}[i%2]}
		if perLine && i%5 != 0 {
			labels["req"] = "r" + strconv.Itoa(i)
		}
		s.rows = append(s.rows, dynWindowRow(ts, []string{"a", "b"}[i%2], "logfmt", labels, float64(i%17)))
	}
	return s
}

// A window read whose bucket answer would exceed the bytes it is sized for is split by time into chunks, each read on
// its own and folded, and the merged answer equals the one read in a single call and Loki's: the read holds a series
// per line, so one read over a long window of a busy service is as large as its lines. No raw rows are read, every
// chunk answer fits the bytes the proxy reads from one stats answer, and few series of many lines (rows far above
// their buckets) are read in few chunks once the first chunk's answer measured what a row costs.
//
// conformance: semantics/unwrap-per-series-identity-raw-rows
func TestDynamicIdentityWindowsChunked(t *testing.T) {
	old := maxStatsBucketResponseBytes
	t.Cleanup(func() { maxStatsBucketResponseBytes = old })
	for _, tc := range []struct {
		name      string
		perLine   bool
		maxChunks int
	}{{"a series per line", true, 40}, {"few series", false, 3}} {
		s := dynChunkScenario(6000, tc.perLine)
		want := lokiDynamicWindows(s)
		maxStatsBucketResponseBytes = old
		single := &vlemu.Emulator{T: t, Rows: s.rows, Field: "amount", Version: "v1.53.0"}
		one, code, body := runDynWindowScenario(t, single, s, Config{})
		if code != http.StatusOK {
			t.Fatalf("%s, one read: %d %.300s", tc.name, code, body)
		}
		if reads, _ := identityStatsCalls(single); len(reads) != 1 {
			t.Fatalf("%s: the default budget read %d bucket queries, want one", tc.name, len(reads))
		}
		maxStatsBucketResponseBytes = 1 << 20
		emu := &vlemu.Emulator{T: t, Rows: s.rows, Field: "amount", Version: "v1.53.0"}
		got, code, body := runDynWindowScenario(t, emu, s, Config{})
		if code != http.StatusOK {
			t.Fatalf("%s, chunked: %d %.300s", tc.name, code, body)
		}
		compareDynWindows(t, tc.name+" chunked vs Loki", s, got, want)
		compareDynWindows(t, tc.name+" chunked vs one read", s, got, one)
		reads, counts := identityStatsCalls(emu)
		if raw := emu.EndpointCalls("/select/logsql/query"); len(raw) > 0 {
			t.Fatalf("%s: read raw rows", tc.name)
		}
		if counts != 1 || len(reads) < 2 && tc.perLine || len(reads) > tc.maxChunks {
			t.Fatalf("%s: %d row counts and %d chunk reads, want one count and 2..%d reads", tc.name, counts, len(reads), tc.maxChunks)
		}
		t.Logf("%s: %d chunk reads under a %d KiB answer bound", tc.name, len(reads), maxStatsBucketResponseBytes>>10)
	}
}

// The floor of the chunks is one evaluation step, whose window reaches one range back: a window holding more rows than
// one stats answer can carry answers Loki's response-too-large error, a per-request outcome (the breaker stays
// closed), never a read of the window's raw rows.
//
// conformance: semantics/unwrap-per-series-identity-raw-rows
func TestDynamicIdentityWindowsChunkFloor(t *testing.T) {
	old := maxStatsBucketResponseBytes
	t.Cleanup(func() { maxStatsBucketResponseBytes = old })
	maxStatsBucketResponseBytes = 8 << 10
	s := dynChunkScenario(6000, true)
	emu := &vlemu.Emulator{T: t, Rows: s.rows, Field: "amount", Version: "v1.53.0"}
	_, code, body := runDynWindowScenario(t, emu, s, Config{})
	if raw := emu.EndpointCalls("/select/logsql/query"); len(raw) > 0 {
		t.Fatalf("read raw rows: %s", raw[0])
	}
	if code != http.StatusInternalServerError || !strings.Contains(body, "ResourceExhausted") {
		t.Fatalf("%d %.300s, want Loki's response-too-large error", code, body)
	}
}

// A repeat of a parser's series identity over a range longer than the step asks VictoriaLogs nothing and answers the
// same series; a refresh one minute later reads only the buckets it does not hold, and answers what an uncached proxy
// answers. Every stats call groups by the series hash. The test logs VictoriaLogs' calls and bytes per request.
//
// conformance: semantics/unwrap-per-series-identity-raw-rows, quality/query-range-extent-cache
func TestDynamicIdentityWindowCacheReuse(t *testing.T) {
	const step = 10 * time.Second
	end := time.Unix(1700011800, 0).UTC()
	var rows []vlemu.Row
	for i := 0; i < 900; i++ { // a line every 5 s over the last 75 minutes, a series of its own now and then
		labels := map[string]string{"route": []string{"/x", "/y"}[i%2]}
		if i%7 == 0 {
			labels["req"] = "r" + strconv.Itoa(i)
		}
		ts := end.Add(time.Minute - time.Duration(i)*5*time.Second - 250*time.Millisecond).UnixNano()
		rows = append(rows, dynWindowRow(ts, []string{"a", "b"}[i%3%2], "json", labels, float64(i%23)))
	}
	emu := &vlemu.Emulator{T: t, Rows: rows, Field: "amount"}
	counter := &stubCallCounter{inner: emu.Handler()}
	shimSrv, shim := newExtentShim(t, counter)
	p := newExtentTestProxy(t, shimSrv.URL, true, 256<<20)
	uncachedSrv, _ := newExtentShim(t, emu.Handler())
	uncached := newExtentTestProxy(t, uncachedSrv.URL, false, 256<<20)
	for _, q := range []string{
		`sum by (pod) (avg_over_time({app="dw"} | json | unwrap amount [1m]))`,
		`sum(stddev_over_time({app="dw"} | json | unwrap amount [45s]))`,
		`avg(stdvar_over_time({app="dw"} | json | unwrap amount [1m]))`,
	} {
		get := func(e time.Time) []byte {
			t.Helper()
			code, body := extentQueryRange(t, p, q, e.Add(-time.Hour), e, step, nil)
			p.statsExtentStores.wait()
			if code != http.StatusOK {
				t.Fatalf("%s: %d %s", q, code, body)
			}
			return body
		}
		counter.take()
		shim.reset()
		cold := get(end)
		coldCalls, coldBytes := counter.take()
		_, coldBuckets := shim.bucketsRead()
		if !shimReadHashAhead(shim) {
			t.Errorf("%s: the stats calls do not group by the series hash written before the gate", q)
		}
		shim.reset()
		again := get(end)
		repCalls, repBytes := counter.take()
		if coldCalls == 0 || repCalls != 0 || string(dynamicIdentityResult(t, cold)) != string(dynamicIdentityResult(t, again)) {
			t.Errorf("%s: cold %d calls, repeat %d calls, want a repeat with no call and the same answer", q, coldCalls, repCalls)
		}
		shim.reset()
		refreshed := get(end.Add(time.Minute))
		refCalls, refBytes := counter.take()
		_, refBuckets := shim.bucketsRead()
		if refBuckets == 0 || refBuckets*10 > coldBuckets {
			t.Errorf("%s refresh: read %d buckets, the cold request %d: it should read the new ones only", q, refBuckets, coldBuckets)
		}
		if _, want := extentQueryRange(t, uncached, q, end.Add(time.Minute-time.Hour), end.Add(time.Minute), step, nil); string(dynamicIdentityResult(t, refreshed)) != string(dynamicIdentityResult(t, want)) {
			t.Errorf("%s refresh: the cached answer differs from the uncached one\n cached: %.300s\nuncached: %.300s", q, refreshed, want)
		}
		t.Logf("%s: cold %d calls %d B (%d buckets), repeat %d calls %d B, refresh %d calls %d B (%d buckets)", q, coldCalls, coldBytes, coldBuckets, repCalls, repBytes, refCalls, refBytes, refBuckets)
	}
	// Another outer aggregation over the same function and clause reads the same buckets (the window evaluator's stats
	// query is the first stats pipe, grouped by the clause and the hash): the first request of each asks VictoriaLogs
	// nothing, and answers what an uncached proxy answers.
	for _, q := range []string{
		`avg by (pod) (avg_over_time({app="dw"} | json | unwrap amount [1m]))`,
		`max by (pod) (avg_over_time({app="dw"} | json | unwrap amount [1m]))`,
	} {
		counter.take()
		code, body := extentQueryRange(t, p, q, end.Add(-time.Hour), end, step, nil)
		p.statsExtentStores.wait()
		calls, _ := counter.take()
		if code != http.StatusOK || calls != 0 {
			t.Errorf("%s: %d after sum by (pod) over the same function, %d VictoriaLogs calls; want 200 and none", q, code, calls)
		}
		if _, want := extentQueryRange(t, uncached, q, end.Add(-time.Hour), end, step, nil); string(dynamicIdentityResult(t, body)) != string(dynamicIdentityResult(t, want)) {
			t.Errorf("%s: the answer from the shared buckets differs from the uncached one", q)
		}
	}
}

// shimReadHashAhead reports stats_query_range calls the extent shim saw, every one writing the series hash before the
// unwrap gate (lower.Profile IdentityAhead) and grouping by it, the chunk plan's row count aside.
func shimReadHashAhead(s *extentShim) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, c := range s.calls {
		if strings.HasSuffix(c.query, "| stats count() as __lvp_rows") {
			continue // the chunk plan's row count
		}
		hash := strings.Index(c.query, "| hash(__lvp_sid) as __lvp_sid |")
		gate := strings.Index(c.query, "| copy ")
		if hash < 0 || gate < hash || !strings.Contains(c.query, "__lvp_sid, __lvp_bad)") {
			return false
		}
	}
	return len(s.calls) > 0
}

// FuzzDynamicIdentityWindowsMark holds the window decision of a parser's series identity on any query and step: a
// request gets at most one mark; the hash-ahead mark only for a root identity (the one the lowering serves) whose
// range is longer than the step, never for a max of max_over_time or a min of min_over_time; and a marked request is
// not decided again.
//
// conformance: semantics/unwrap-per-series-identity-raw-rows
func FuzzDynamicIdentityWindowsMark(f *testing.F) {
	for _, s := range []struct {
		q    string
		step uint16
	}{
		{`sum by (app) (avg_over_time({app="a"} | json | unwrap n [5m]))`, 60},
		{`sum(stdvar_over_time({app="a"} | logfmt | unwrap n [90s]))`, 60},
		{`max by (app) (max_over_time({app="a"} | json | unwrap n [5m]))`, 60},
		{`min(min_over_time({app="a"} | json | unwrap n [5m]))`, 30},
		{`MAX BY (app) (AVG_OVER_TIME({app="a"} | JSON | UNWRAP n [5m]))`, 60},
		{`topk(1, sum by (app) (avg_over_time({app="a"} | json | unwrap n [5m])))`, 60},
		{`label_replace(sum(avg_over_time({app="a"} | json | unwrap n [5m])), "x", "$1", "app", "(.*)")`, 60},
		{`sum(avg_over_time({app="a"} | json | unwrap n [5m])) * 2`, 60},
		{`sum(avg_over_time({app="a"} | json | unwrap n [1m]))`, 300},
		{`sum(quantile_over_time(0.9, {app="a"} | json | unwrap n [5m]))`, 60},
		{`sum(max_over_time({app="a"} | logfmt | unwrap _TIME [5m]))`, 60},
	} {
		f.Add(s.q, s.step)
	}
	c := cache.New(time.Minute, 100)
	f.Cleanup(c.Close)
	p, err := New(Config{BackendURL: "http://127.0.0.1:1", Cache: c, LogLevel: "error"})
	if err != nil {
		f.Fatal(err)
	}
	f.Cleanup(func() { closeTestProxy(p) })
	f.Fuzz(func(t *testing.T, q string, step uint16) {
		if len(q) > 512 || step == 0 {
			return
		}
		r := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+url.Values{"step": {strconv.Itoa(int(step))}}.Encode(), nil)
		out := p.dynamicIdentityWindows(r, q)
		stream, ahead := streamIdentityOnly(out.Context()), identityAhead(out.Context())
		if stream && ahead {
			t.Fatalf("%q step %d: both marks", q, step)
		}
		if ahead {
			window, ok := dynamicWindowRange(q)
			if !p.dynamicUnwrapIdentity(q) || !ok || window <= time.Duration(step)*time.Second {
				t.Fatalf("%q step %d: the hash ahead without a root identity over a range longer than the step (%v %s)", q, step, ok, window)
			}
			if extremeOfExtremes(q) || !identityAheadWritten(q) {
				t.Fatalf("%q step %d: an extreme of extremes, or an unwrap whose gate the evaluator cannot read, reads the hash", q, step)
			}
		}
		if (stream || ahead) && p.dynamicIdentityWindows(out, q) != out {
			t.Fatalf("%q step %d: a marked request decided again", q, step)
		}
	})
}

// A failed bucket read of a request without the hash-ahead mark falls back as before (no error); with it, an
// oversized answer is Loki's response-too-large error and any other failure is answered as it is.
//
// conformance: semantics/unwrap-per-series-identity-raw-rows
func TestIdentityAheadReadError(t *testing.T) {
	plain := context.Background()
	ahead := context.WithValue(plain, identityAheadKey{}, true)
	other := errors.New("backend returned 503")
	if err := identityAheadReadError(plain, errBodyTooLarge); err != nil {
		t.Errorf("an unmarked request: %v, want the fallback", err)
	}
	if err := identityAheadReadError(ahead, other); err != other {
		t.Errorf("a marked request's backend failure: %v, want it as it is", err)
	}
	err := identityAheadReadError(ahead, errBodyTooLarge)
	if err == nil || !strings.Contains(err.Error(), "ResourceExhausted") && !strings.Contains(err.Error(), "larger than max") {
		t.Errorf("a marked request's oversized answer: %v, want Loki's response-too-large error", err)
	}
}

// identityStatsCalls are the stats_query_range calls the emulator saw that read a parser's identity buckets (the hash
// before the gate, grouped by it), and how many row counts the chunk plan read (identityCountQuery).
func identityStatsCalls(emu *vlemu.Emulator) (buckets []string, counts int) {
	for _, q := range emu.EndpointCalls("/select/logsql/stats_query_range") {
		switch {
		case strings.HasSuffix(q, "| stats count() as __lvp_rows"):
			counts++
		case strings.Contains(q, "hash(__lvp_sid) as __lvp_sid") && strings.Contains(q, "__lvp_sid, __lvp_bad)"):
			buckets = append(buckets, q)
		}
	}
	return buckets, counts
}
