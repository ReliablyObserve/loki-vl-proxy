package proxy

import (
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// slidingFixtureLine is one stored log line of the fake VictoriaLogs backend.
type slidingFixtureLine struct {
	ts  int64 // Unix nanoseconds
	app string
	msg string
}

type slidingStatsCall struct {
	query, start, end, step, offset string
}

type slidingFakeVL struct {
	mu           sync.Mutex
	lines        []slidingFixtureLine
	statsCalls   []slidingStatsCall
	hitsCalls    []slidingStatsCall
	rawCalls     int
	metrics      string // /metrics body; empty answers 503
	metricsCalls int
}

func (f *slidingFakeVL) snapshot() ([]slidingStatsCall, int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]slidingStatsCall(nil), f.statsCalls...), f.rawCalls
}

func (f *slidingFakeVL) hitsSnapshot() []slidingStatsCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]slidingStatsCall(nil), f.hitsCalls...)
}

// slidingLogfmtFields returns the key=value pairs of a fixture line, as
// VictoriaLogs unpack_logfmt exposes them.
func slidingLogfmtFields(msg string) map[string]string {
	fields := map[string]string{}
	for _, token := range strings.Fields(msg) {
		if k, v, ok := strings.Cut(token, "="); ok && k != "" {
			fields[k] = v
		}
	}
	return fields
}

// slidingBucketParams parses the start, end, step and offset args shared by
// stats_query_range and /hits.
func slidingBucketParams(t testing.TB, r *http.Request) (start, end, step, offset int64, ok bool) {
	start = parseFakeVLTime(t, r.Form.Get("start"))
	end = parseFakeVLTime(t, r.Form.Get("end"))
	stepDur, err := time.ParseDuration(r.Form.Get("step"))
	if err != nil || stepDur <= 0 {
		t.Errorf("fake VL: bad step %q", r.Form.Get("step"))
		return 0, 0, 0, 0, false
	}
	var offsetDur time.Duration
	if raw := r.Form.Get("offset"); raw != "" {
		if offsetDur, err = time.ParseDuration(raw); err != nil {
			t.Errorf("fake VL: bad offset %q", raw)
			return 0, 0, 0, 0, false
		}
	}
	return start, end, int64(stepDur), int64(offsetDur), true
}

// parseFakeVLTime accepts the timestamp forms VictoriaLogs accepts on its
// query args: integer or fractional Unix seconds, and RFC3339Nano.
func parseFakeVLTime(t testing.TB, raw string) int64 {
	t.Helper()
	if parsed, err := time.Parse(time.RFC3339Nano, raw); err == nil {
		return parsed.UnixNano()
	}
	if n, err := strconv.ParseInt(raw, 10, 64); err == nil {
		if n > 1e15 {
			return n
		}
		return n * int64(time.Second)
	}
	if f, err := strconv.ParseFloat(raw, 64); err == nil {
		return int64(f * 1e9)
	}
	t.Fatalf("fake VL: cannot parse time %q", raw)
	return 0
}

// vlTruncate mirrors VictoriaLogs truncateTimestamp for `_time:step offset X`
// bucketing (lib/logstorage/block_result.go).
func vlTruncate(ts, bucket, offset int64) int64 {
	ts += offset
	r := ts % bucket
	if r < 0 {
		r += bucket
	}
	return ts - r - offset
}

// newSlidingFakeVL serves stats_query_range with VictoriaLogs bucket semantics
// ([start, end) filter, buckets aligned to step at the given offset, labelled by
// their start) and the raw /select/logsql/query endpoint for the exact evaluator.
func newSlidingFakeVL(t testing.TB, lines []slidingFixtureLine) (*httptest.Server, *slidingFakeVL) {
	t.Helper()
	fake := &slidingFakeVL{lines: lines}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			t.Errorf("parse form: %v", err)
			return
		}
		switch r.URL.Path {
		case "/select/logsql/stats_query_range":
			q := r.Form.Get("query")
			fake.mu.Lock()
			fake.statsCalls = append(fake.statsCalls, slidingStatsCall{query: q, start: r.Form.Get("start"), end: r.Form.Get("end"), step: r.Form.Get("step"), offset: r.Form.Get("offset")})
			fake.mu.Unlock()
			start, end, step, offset, ok := slidingBucketParams(t, r)
			if !ok {
				return
			}
			byApp := strings.Contains(q, "stats by (app)")
			byStream := strings.Contains(q, "stats by (_stream)")
			metricName := "c"
			bytesMetric := strings.Contains(q, "sum_len(_msg) as c")
			withPresence := strings.Contains(q, "count() as __sample_count")
			unwrapAgg := ""
			for _, agg := range []string{"sum", "max", "min"} {
				if strings.Contains(q, "stats by (_stream) "+agg+"(n) as c") {
					unwrapAgg = agg
				}
			}
			type key struct {
				name, app string
			}
			points := map[key]map[int64]float64{}
			add := func(k key, ts int64, v float64) {
				if points[k] == nil {
					points[k] = map[int64]float64{}
				}
				prev, seen := points[k][ts]
				switch {
				case !seen:
					points[k][ts] = v
				case unwrapAgg == "max" && k.name == metricName:
					points[k][ts] = math.Max(prev, v)
				case unwrapAgg == "min" && k.name == metricName:
					points[k][ts] = math.Min(prev, v)
				default:
					points[k][ts] = prev + v
				}
			}
			for _, line := range fake.lines {
				if line.ts < start || line.ts >= end {
					continue
				}
				app := ""
				if byApp || byStream {
					app = line.app
				}
				bucket := vlTruncate(line.ts, step, offset)
				switch {
				case unwrapAgg != "":
					n, err := strconv.ParseFloat(slidingLogfmtFields(line.msg)["n"], 64)
					if err != nil {
						continue
					}
					add(key{metricName, app}, bucket, n)
				case bytesMetric:
					add(key{metricName, app}, bucket, float64(len(line.msg)))
				default:
					add(key{metricName, app}, bucket, 1)
				}
				if withPresence {
					add(key{"__sample_count", app}, bucket, 1)
				}
			}
			keys := make([]key, 0, len(points))
			for k := range points {
				keys = append(keys, k)
			}
			sort.Slice(keys, func(i, j int) bool { return keys[i].name+keys[i].app < keys[j].name+keys[j].app })
			var sb strings.Builder
			sb.WriteString(`{"status":"success","data":{"resultType":"matrix","result":[`)
			for i, k := range keys {
				if i > 0 {
					sb.WriteByte(',')
				}
				fmt.Fprintf(&sb, `{"metric":{"__name__":%q`, k.name)
				if byApp {
					fmt.Fprintf(&sb, `,"app":%q`, k.app)
				}
				if byStream {
					fmt.Fprintf(&sb, `,"_stream":%q`, `{app="`+k.app+`"}`)
				}
				sb.WriteString(`},"values":[`)
				tss := make([]int64, 0, len(points[k]))
				for ts := range points[k] {
					tss = append(tss, ts)
				}
				sort.Slice(tss, func(i, j int) bool { return tss[i] < tss[j] })
				for j, ts := range tss {
					if j > 0 {
						sb.WriteByte(',')
					}
					// VictoriaLogs formats float64(ns)/1e9 with the shortest representation.
					fmt.Fprintf(&sb, `[%s,%q]`, strconv.FormatFloat(float64(ts)/1e9, 'f', -1, 64), strconv.FormatFloat(points[k][ts], 'f', -1, 64))
				}
				sb.WriteString(`]}`)
			}
			sb.WriteString(`]}}`)
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(sb.String()))
		case "/select/logsql/hits":
			fake.mu.Lock()
			fake.hitsCalls = append(fake.hitsCalls, slidingStatsCall{query: r.Form.Get("query"), start: r.Form.Get("start"), end: r.Form.Get("end"), step: r.Form.Get("step"), offset: r.Form.Get("offset")})
			fake.mu.Unlock()
			start, end, step, offset, ok := slidingBucketParams(t, r)
			if !ok {
				return
			}
			byApp := false
			for _, field := range r.Form["field"] {
				byApp = byApp || field == "app"
			}
			counts := map[string]map[int64]int{}
			for _, line := range fake.lines {
				if line.ts < start || line.ts >= end {
					continue
				}
				app := ""
				if byApp {
					app = line.app
				}
				if counts[app] == nil {
					counts[app] = map[int64]int{}
				}
				counts[app][vlTruncate(line.ts, step, offset)]++
			}
			type hit struct {
				Fields     map[string]string `json:"fields"`
				Timestamps []string          `json:"timestamps"`
				Values     []int             `json:"values"`
				Total      int               `json:"total"`
			}
			apps := make([]string, 0, len(counts))
			for app := range counts {
				apps = append(apps, app)
			}
			sort.Strings(apps)
			hits := make([]hit, 0, len(apps))
			for _, app := range apps {
				h := hit{Fields: map[string]string{}}
				if byApp {
					h.Fields["app"] = app
				}
				tss := make([]int64, 0, len(counts[app]))
				for ts := range counts[app] {
					tss = append(tss, ts)
				}
				sort.Slice(tss, func(i, j int) bool { return tss[i] < tss[j] })
				for _, ts := range tss {
					// VictoriaLogs /hits formats bucket starts as RFC3339 with nanoseconds.
					h.Timestamps = append(h.Timestamps, time.Unix(0, ts).UTC().Format(time.RFC3339Nano))
					h.Values = append(h.Values, counts[app][ts])
					h.Total += counts[app][ts]
				}
				hits = append(hits, h)
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{"hits": hits})
		case "/select/logsql/query":
			fake.mu.Lock()
			fake.rawCalls++
			fake.mu.Unlock()
			start := parseFakeVLTime(t, r.Form.Get("start"))
			end := parseFakeVLTime(t, r.Form.Get("end"))
			w.Header().Set("Content-Type", "application/x-ndjson")
			for _, line := range fake.lines {
				if line.ts < start || line.ts >= end {
					continue
				}
				fields := slidingLogfmtFields(line.msg)
				fields["_time"] = time.Unix(0, line.ts).UTC().Format(time.RFC3339Nano)
				fields["_msg"] = line.msg
				fields["_stream"] = `{app="` + line.app + `"}`
				fields["app"] = line.app
				row, _ := json.Marshal(fields)
				_, _ = w.Write(append(row, '\n'))
			}
		case "/metrics":
			fake.mu.Lock()
			fake.metricsCalls++
			body := fake.metrics
			fake.mu.Unlock()
			if body == "" {
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			_, _ = w.Write([]byte(body))
		default:
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{}`))
		}
	}))
	t.Cleanup(srv.Close)
	return srv, fake
}

// lokiSlidingReference evaluates a range aggregation with Loki semantics: the
// sample at t covers (t-window, t], and a step without samples is absent. The
// unwrap functions read the logfmt field n of every line.
func lokiSlidingReference(lines []slidingFixtureLine, fn string, byApp bool, divisor float64, start, end time.Time, step, window time.Duration) map[string]map[int64]string {
	out := map[string]map[int64]string{}
	for t := start; !t.After(end); t = t.Add(step) {
		counts := map[string]float64{}
		bytes := map[string]float64{}
		values := map[string]float64{} // unwrap n: sum, max or min
		for _, line := range lines {
			if line.ts <= t.Add(-window).UnixNano() || line.ts > t.UnixNano() {
				continue
			}
			app := ""
			if byApp {
				app = line.app
			}
			n, _ := strconv.ParseFloat(slidingLogfmtFields(line.msg)["n"], 64)
			switch {
			case counts[app] == 0 || (fn == "max_over_time" && n > values[app]) || (fn == "min_over_time" && n < values[app]):
				values[app] = n
			case fn == "sum_over_time":
				values[app] += n
			}
			counts[app]++
			bytes[app] += float64(len(line.msg))
		}
		for app, count := range counts {
			var v float64
			switch fn {
			case "count_over_time":
				v = count
			case "rate":
				v = count / window.Seconds()
			case "bytes_over_time":
				v = bytes[app]
			case "bytes_rate":
				v = bytes[app] / window.Seconds()
			case "sum_over_time", "max_over_time", "min_over_time":
				v = values[app]
			}
			if divisor != 0 {
				v /= divisor
			}
			if out[app] == nil {
				out[app] = map[int64]string{}
			}
			out[app][t.Unix()] = strconv.FormatFloat(v, 'f', -1, 64)
		}
	}
	return out
}

func runSlidingQueryRange(t testing.TB, p *Proxy, query string, start, end time.Time, step time.Duration, headers map[string]string) map[string]map[int64]string {
	t.Helper()
	params := url.Values{}
	params.Set("query", query)
	params.Set("start", strconv.FormatInt(start.UnixNano(), 10))
	params.Set("end", strconv.FormatInt(end.UnixNano(), 10))
	params.Set("step", strconv.FormatFloat(step.Seconds(), 'f', -1, 64))
	req := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+params.Encode(), nil)
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	p.handleQueryRange(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("%s: expected 200, got %d: %s", query, rec.Code, rec.Body.String())
	}
	var resp struct {
		Status string `json:"status"`
		Data   struct {
			ResultType string `json:"resultType"`
			Result     []struct {
				Metric map[string]string `json:"metric"`
				Values [][]interface{}   `json:"values"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("%s: decode: %v: %s", query, err, rec.Body.String())
	}
	if resp.Data.ResultType != "matrix" {
		t.Fatalf("%s: expected matrix, got %q: %s", query, resp.Data.ResultType, rec.Body.String())
	}
	got := map[string]map[int64]string{}
	for _, series := range resp.Data.Result {
		app := series.Metric["app"]
		if got[app] == nil {
			got[app] = map[int64]string{}
		}
		for _, pair := range series.Values {
			ts, _ := pair[0].(float64)
			v, _ := pair[1].(string)
			got[app][int64(ts)] = v
		}
	}
	return got
}

func assertSlidingSeriesEqual(t *testing.T, query string, want, got map[string]map[int64]string) {
	t.Helper()
	if len(want) == 0 {
		t.Fatalf("%s: reference is empty; fixture does not exercise the query", query)
	}
	for app, wantPoints := range want {
		gotPoints := got[app]
		zeros := 0
		for _, v := range gotPoints {
			if v == "0" {
				zeros++
			}
		}
		if len(gotPoints) != len(wantPoints) {
			t.Errorf("%s app=%q: got %d points (%d zero), want %d", query, app, len(gotPoints), zeros, len(wantPoints))
		}
		for ts, wv := range wantPoints {
			if gv, ok := gotPoints[ts]; !ok || gv != wv {
				t.Errorf("%s app=%q ts=%d: got %q (present=%v), want %q", query, app, ts, gv, ok, wv)
			}
		}
		for ts, gv := range gotPoints {
			if _, ok := wantPoints[ts]; !ok {
				t.Errorf("%s app=%q ts=%d: unexpected point %q (Loki omits steps whose window has no samples)", query, app, ts, gv)
			}
		}
	}
	for app := range got {
		if _, ok := want[app]; !ok {
			t.Errorf("%s: unexpected series app=%q", query, app)
		}
	}
}

// Loki emits a sliding-window sample only when (t-range, t] contains log
// lines. A data gap must stay absent for every client, including Drilldown.
func TestSlidingRangeMetric_GapStepsAreAbsentLikeLoki(t *testing.T) {
	hour0 := time.Unix(1699999200, 0).UTC() // hour-aligned
	var lines []slidingFixtureLine
	for _, h := range []int{0, 5} {
		for i := 0; i < 60; i++ {
			ts := hour0.Add(time.Duration(h)*time.Hour + time.Duration(i)*time.Minute + 7*time.Second)
			lines = append(lines, slidingFixtureLine{ts: ts.UnixNano(), app: "gap-app", msg: strings.Repeat("x", 10+i%7)})
		}
	}
	srv, fake := newSlidingFakeVL(t, lines)
	p := newSlidingTestProxy(t, srv.URL)

	start, end := hour0, hour0.Add(8*time.Hour)
	cases := []struct {
		query   string
		fn      string
		divisor float64
		step    time.Duration
		window  time.Duration
	}{
		{`sum by (app) (count_over_time({app="gap-app"}[1h]))`, "count_over_time", 0, 300 * time.Second, time.Hour},
		{`sum by (app) (count_over_time({app="gap-app"}[5m]))`, "count_over_time", 0, time.Minute, 5 * time.Minute},
		{`sum by (app) (rate({app="gap-app"}[1h]))`, "rate", 0, 300 * time.Second, time.Hour},
		{`sum by (app) (bytes_rate({app="gap-app"}[1h]))`, "bytes_rate", 0, 300 * time.Second, time.Hour},
		{`sum by (app) (count_over_time({app="gap-app"}[1h])) / 3600`, "count_over_time", 3600, 300 * time.Second, time.Hour},
	}
	for _, tc := range cases {
		for _, tagged := range []bool{false, true} {
			headers := map[string]string{}
			name := tc.query
			if tagged {
				headers["X-Query-Tags"] = "Source=grafana-lokiexplore-app"
				name += " [drilldown]"
			}
			t.Run(name, func(t *testing.T) {
				want := lokiSlidingReference(lines, tc.fn, true, tc.divisor, start, end, tc.step, tc.window)
				got := runSlidingQueryRange(t, p, tc.query, start, end, tc.step, headers)
				assertSlidingSeriesEqual(t, name, want, got)
			})
		}
	}
	if _, raw := fake.snapshot(); raw != 0 {
		t.Fatalf("expected stats_query_range buckets only, raw log scans: %d", raw)
	}
}

// Every evaluation window must be an exact union of backend buckets: a 90s
// window with a 60s step needs 30s buckets, and an unaligned start needs bucket
// edges anchored to the request instead of the epoch.
func TestSlidingRangeMetric_WindowsAreExactBucketUnions(t *testing.T) {
	s0 := time.Unix(1700000400, 0).UTC() // minute-aligned
	var lines []slidingFixtureLine
	for i := 0; i < 180; i++ { // one line every 10s for 30 minutes
		lines = append(lines, slidingFixtureLine{ts: s0.Add(time.Duration(i) * 10 * time.Second).UnixNano(), app: "tick-app", msg: "tick"})
	}

	t.Run("90s window aligned start", func(t *testing.T) {
		srv, fake := newSlidingFakeVL(t, lines)
		p := newSlidingTestProxy(t, srv.URL)
		query := `sum(count_over_time({app="tick-app"}[90s]))`
		start, end := s0.Add(10*time.Minute), s0.Add(20*time.Minute)
		got := runSlidingQueryRange(t, p, query, start, end, time.Minute, nil)
		want := lokiSlidingReference(lines, "count_over_time", false, 0, start, end, time.Minute, 90*time.Second)
		for _, v := range want[""] {
			if v != "9" {
				t.Fatalf("reference sanity: expected 9 lines per 90s window, got %s", v)
			}
		}
		assertSlidingSeriesEqual(t, query, want, got)
		calls, raw := fake.snapshot()
		if raw != 0 || len(calls) != 1 {
			t.Fatalf("expected one stats_query_range call and no raw scans, got %d stats calls and %d raw scans", len(calls), raw)
		}
		if calls[0].step != "30s" || calls[0].offset != "-1ns" {
			t.Fatalf("expected 30s buckets with right-closed edges (offset -1ns), got step=%q offset=%q", calls[0].step, calls[0].offset)
		}
	})

	t.Run("2m window unaligned start", func(t *testing.T) {
		srv, fake := newSlidingFakeVL(t, lines)
		p := newSlidingTestProxy(t, srv.URL)
		query := `sum(count_over_time({app="tick-app"}[2m]))`
		start, end := s0.Add(30*time.Second), s0.Add(10*time.Minute+30*time.Second)
		got := runSlidingQueryRange(t, p, query, start, end, time.Minute, nil)
		want := lokiSlidingReference(lines, "count_over_time", false, 0, start, end, time.Minute, 2*time.Minute)
		if want[""][start.Unix()] != "4" || want[""][start.Add(time.Minute).Unix()] != "10" {
			t.Fatalf("reference sanity: expected 4 and 10 at the first two steps, got %v", want[""])
		}
		assertSlidingSeriesEqual(t, query, want, got)
		calls, raw := fake.snapshot()
		if raw != 0 || len(calls) != 1 {
			t.Fatalf("expected one stats_query_range call and no raw scans, got %d stats calls and %d raw scans", len(calls), raw)
		}
		// Buckets start at start-range+1ns = s0-90s+1ns, and (s0-90s+1ns) mod 60s = 30s+1ns.
		if calls[0].step != "60s" || calls[0].offset != "-30000000001ns" {
			t.Fatalf("expected 60s buckets anchored to the request start, got step=%q offset=%q", calls[0].step, calls[0].offset)
		}
	})

	t.Run("lines on window edges follow Loki boundaries", func(t *testing.T) {
		// Minute marks carry 1, 2 or 3 lines, and every window edge sits on a
		// minute mark: Loki excludes the lower edge and includes the evaluation
		// time, so a left-closed bucket sum would shift the counts by one minute.
		var edgeLines []slidingFixtureLine
		for i := 0; i < 30; i++ {
			for n := 0; n <= i%3; n++ {
				edgeLines = append(edgeLines, slidingFixtureLine{ts: s0.Add(time.Duration(i) * time.Minute).UnixNano(), app: "edge-app", msg: "edge"})
			}
		}
		srv, fake := newSlidingFakeVL(t, edgeLines)
		p := newSlidingTestProxy(t, srv.URL)
		query := `sum(count_over_time({app="edge-app"}[2m]))`
		start, end := s0.Add(5*time.Minute), s0.Add(15*time.Minute)
		got := runSlidingQueryRange(t, p, query, start, end, time.Minute, nil)
		want := lokiSlidingReference(edgeLines, "count_over_time", false, 0, start, end, time.Minute, 2*time.Minute)
		assertSlidingSeriesEqual(t, query, want, got)
		if calls, raw := fake.snapshot(); raw != 0 || len(calls) != 1 {
			t.Fatalf("expected one stats_query_range call and no raw scans, got %d stats calls and %d raw scans", len(calls), raw)
		}
	})
}

// Fine gcd buckets stay on one stats_query_range call. VictoriaLogs returns only
// non-empty buckets, so tens of thousands of buckets cost less than scanning the
// same lines raw, which could exceed -manual-range-metric-row-limit.
func TestSlidingRangeMetric_FineBucketsStayOnStats(t *testing.T) {
	s0 := time.Unix(1699920000, 0).UTC() // day-aligned
	for _, tc := range []struct {
		name, query, fn, wantStep string
		byApp                     bool
		span, step, window, every time.Duration
	}{
		// gcd(7m, 1h) = 60s: 43207 buckets over 30 days.
		{name: "topk rate 7m over 30d at 1h step", query: `topk(1, sum by (app) (rate({app="fine-app"}[7m])))`, fn: "rate", wantStep: "60s", byApp: true, span: 30 * 24 * time.Hour, step: time.Hour, window: 7 * time.Minute, every: 7*time.Minute + 13*time.Second},
		// gcd(17s, 5m) = 1s: 86700 buckets over 24 hours.
		{name: "count 5m over 24h at 17s step", query: `sum(count_over_time({app="fine-app"}[5m]))`, fn: "count_over_time", wantStep: "1s", span: 24 * time.Hour, step: 17 * time.Second, window: 5 * time.Minute, every: 40 * time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var lines []slidingFixtureLine
			for ts := s0; ts.Before(s0.Add(tc.span)); ts = ts.Add(tc.every) {
				lines = append(lines, slidingFixtureLine{ts: ts.UnixNano(), app: "fine-app", msg: "fine"})
			}
			srv, fake := newSlidingFakeVL(t, lines)
			p := newSlidingTestProxy(t, srv.URL)
			start, end := s0, s0.Add(tc.span)
			got := runSlidingQueryRange(t, p, tc.query, start, end, tc.step, nil)
			assertSlidingSeriesEqual(t, tc.query, lokiSlidingReference(lines, tc.fn, tc.byApp, 0, start, end, tc.step, tc.window), got)
			calls, raw := fake.snapshot()
			if raw != 0 || len(calls) != 1 || calls[0].step != tc.wantStep || calls[0].offset == "" {
				t.Fatalf("expected one anchored stats call with %s buckets, got %+v and %d raw scans", tc.wantStep, calls, raw)
			}
		})
	}
}

// VictoriaLogs before v1.45 ignores the stats_query_range offset arg and aligns
// buckets to the epoch, and a failed version probe leaves the release unknown.
// Both keep buckets only for epoch-aligned grids (a line exactly on a window
// edge then counts in the neighbouring window, a documented limit, so these
// fixtures keep lines off the edges); other grids use the raw evaluator instead
// of silently misaligned buckets. This includes the range == step topk path.
func TestSlidingRangeMetric_BackendWithoutOffsetSupport(t *testing.T) {
	s0 := time.Unix(1700000400, 0).UTC()
	var lines []slidingFixtureLine
	for i := 0; i < 180; i++ {
		lines = append(lines, slidingFixtureLine{ts: s0.Add(time.Duration(i)*10*time.Second + 3*time.Second).UnixNano(), app: "old-vl", msg: "tick"})
	}
	sliding := `sum(count_over_time({app="old-vl"}[2m]))`
	ranked := `topk(1, sum by (app) (count_over_time({app="old-vl"}[1m])))`
	for _, version := range []string{"v1.44.0", ""} {
		for _, tc := range []struct {
			name, query string
			byApp       bool
			window      time.Duration
			start       time.Time
			wantRaw     bool
		}{
			{name: "sliding epoch-aligned grid", query: sliding, window: 2 * time.Minute, start: s0.Add(5 * time.Minute)},
			{name: "sliding unaligned grid", query: sliding, window: 2 * time.Minute, start: s0.Add(5*time.Minute + 30*time.Second), wantRaw: true},
			{name: "topk range equals step epoch-aligned grid", query: ranked, byApp: true, window: time.Minute, start: s0.Add(5 * time.Minute)},
			{name: "topk range equals step unaligned grid", query: ranked, byApp: true, window: time.Minute, start: s0.Add(5*time.Minute + 30*time.Second), wantRaw: true},
		} {
			t.Run(fmt.Sprintf("version=%q/%s", version, tc.name), func(t *testing.T) {
				srv, fake := newSlidingFakeVL(t, lines)
				p := newGapTestProxy(t, srv.URL)
				if version != "" {
					p.storeBackendVersion(version, version)
				}
				end := tc.start.Add(10 * time.Minute)
				got := runSlidingQueryRange(t, p, tc.query, tc.start, end, time.Minute, nil)
				assertSlidingSeriesEqual(t, tc.query, lokiSlidingReference(lines, "count_over_time", tc.byApp, 0, tc.start, end, time.Minute, tc.window), got)
				calls, raw := fake.snapshot()
				if tc.wantRaw {
					if len(calls) != 0 || raw == 0 {
						t.Fatalf("expected raw evaluator, got %+v stats calls and %d raw scans", calls, raw)
					}
					return
				}
				if raw != 0 || len(calls) != 1 || calls[0].step != "60s" || calls[0].offset != "" {
					t.Fatalf("expected one offset-free stats call with 60s buckets, got %+v and %d raw scans", calls, raw)
				}
			})
		}
	}
}

func newSlidingTestProxy(t *testing.T, backendURL string) *Proxy {
	t.Helper()
	p := newGapTestProxy(t, backendURL)
	p.storeBackendVersion("v1.50.0", "v1.50.0") // stats_query_range offset (v1.45+)
	return p
}

// A failed startup version probe must not pin the raw evaluator: while the
// version is unknown the proxy retries the metrics probe in the background, and
// once it succeeds unaligned grids use anchored buckets.
func TestSlidingRangeMetric_VersionReprobeEnablesAnchoredBuckets(t *testing.T) {
	s0 := time.Unix(1700000400, 0).UTC()
	var lines []slidingFixtureLine
	for i := 0; i < 180; i++ {
		lines = append(lines, slidingFixtureLine{ts: s0.Add(time.Duration(i) * 10 * time.Second).UnixNano(), app: "reprobe-app", msg: "tick"})
	}
	srv, fake := newSlidingFakeVL(t, lines)
	p := newGapTestProxy(t, srv.URL)
	query := `sum(count_over_time({app="reprobe-app"}[2m]))`
	start := s0.Add(5*time.Minute + 30*time.Second)
	// Each request uses a different end so the response cache never answers it.
	check := func(minutes int) {
		t.Helper()
		end := start.Add(time.Duration(minutes) * time.Minute)
		want := lokiSlidingReference(lines, "count_over_time", false, 0, start, end, time.Minute, 2*time.Minute)
		assertSlidingSeriesEqual(t, query, want, runSlidingQueryRange(t, p, query, start, end, time.Minute, nil))
	}

	metricsCalls := func() int {
		fake.mu.Lock()
		defer fake.mu.Unlock()
		return fake.metricsCalls
	}
	waitMetricsCalls := func(want int) {
		t.Helper()
		deadline := time.Now().Add(5 * time.Second)
		for metricsCalls() < want && time.Now().Before(deadline) {
			time.Sleep(10 * time.Millisecond)
		}
		time.Sleep(50 * time.Millisecond) // let any extra probe land before counting
		if got := metricsCalls(); got != want {
			t.Fatalf("expected %d /metrics probes, got %d", want, got)
		}
	}

	// Unknown version within the probe interval: raw evaluator, no probe yet.
	check(10)
	if calls, raw := fake.snapshot(); len(calls) != 0 || raw != 1 {
		t.Fatalf("unknown version must use the raw evaluator, got %+v stats calls and %d raw scans", calls, raw)
	}
	waitMetricsCalls(0)

	// Once the interval elapses, a failing probe runs once however many requests
	// arrive; the next attempt waits for the following interval.
	p.backendVersionProbedAt.Store(time.Now().Add(-backendVersionReprobeInterval).UnixNano())
	for minutes := 20; minutes < 25; minutes++ {
		check(minutes)
	}
	waitMetricsCalls(1)
	if _, semver, _ := p.backendVersionState(); semver != "" {
		t.Fatalf("a failing probe must leave the version unknown, got %q", semver)
	}

	// The backend becomes probeable and the retry interval has elapsed.
	fake.mu.Lock()
	fake.metrics = "vm_app_version{version=\"victoria-logs-20260414-184331-tags-v1.50.0-0-g80d223f95f\", short_version=\"v1.50.0\"} 1\n"
	fake.mu.Unlock()
	p.backendVersionProbedAt.Store(time.Now().Add(-backendVersionReprobeInterval).UnixNano())
	check(11)
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, semver, _ := p.backendVersionState(); semver != "" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("background version probe did not record the backend version")
		}
		time.Sleep(10 * time.Millisecond)
	}
	check(12)
	waitMetricsCalls(2)
	calls, _ := fake.snapshot()
	if len(calls) != 1 || calls[0].offset != "-30000000001ns" {
		t.Fatalf("expected one anchored stats call once the version is known, got %+v", calls)
	}
}
