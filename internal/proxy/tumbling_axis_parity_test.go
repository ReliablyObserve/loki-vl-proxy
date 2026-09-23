package proxy

import (
	"encoding/json"
	"fmt"
	"math"
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
)

// tumblingLine is one stored log line of the labelled fake VictoriaLogs.
type tumblingLine struct {
	ts     int64 // Unix nanoseconds
	labels map[string]string
	msg    string
}

var (
	fakeInFilterRE    = regexp.MustCompile(`\| filter \(?([A-Za-z_]+):in\(([^)]*)\)( or ([A-Za-z_]+):"")?\)?`)
	fakeWindowPhaseRE = regexp.MustCompile(`\| math \(\(_time - (-?\d+)\) % (\d+)\) as __lvp_window_phase \| filter __lvp_window_phase:>0 __lvp_window_phase:<=(\d+)`)
	tumblingStatsByRE = regexp.MustCompile(`\| stats (?:by \(([^)]*)\) )?(count\(\)|sum_len\(_msg\))( as [A-Za-z_]+)?`)
	tumblingMathRE    = regexp.MustCompile(`\| math __lvp_inner/(\d+) as __lvp_rate \| stats (?:by \(([^)]*)\) )?sum\(__lvp_rate\)`)
)

// fakeWindowPhaseKeep applies the windowPhaseFilter pipes of a query to a line
// timestamp, as VictoriaLogs math and filter pipes do; queries without them keep
// every line.
func fakeWindowPhaseKeep(t testing.TB, q string, ts int64) bool {
	m := fakeWindowPhaseRE.FindStringSubmatch(q)
	if m == nil {
		return true
	}
	anchor, err1 := strconv.ParseInt(m[1], 10, 64)
	step, err2 := strconv.ParseInt(m[2], 10, 64)
	window, err3 := strconv.ParseInt(m[3], 10, 64)
	if err1 != nil || err2 != nil || err3 != nil || step <= 0 {
		t.Errorf("fake VL: bad window phase filter in %q", q)
		return false
	}
	phase := (ts - anchor) % step
	return phase > 0 && phase <= window
}

// fakeInFilterKeep applies a `| filter f:in(...)` pipe (optionally
// `(f:in(...) or f:"")`) of a query to a line's labels.
func fakeInFilterKeep(q string, labels map[string]string) bool {
	m := fakeInFilterRE.FindStringSubmatch(q)
	if m == nil {
		return true
	}
	value := labels[m[1]]
	if value == "" {
		return m[3] != ""
	}
	for _, v := range strings.Split(m[2], ",") {
		if strings.Trim(v, `"`) == value {
			return true
		}
	}
	return false
}

type tumblingFakeVL struct {
	mu         sync.Mutex
	statsCalls []slidingStatsCall
	instant    string // stats_query response body
	rawCalls   int
}

// newTumblingFakeVL serves stats_query_range with VictoriaLogs semantics for the
// label-grouped queries the translator emits: rows in [start, end), buckets on
// the step grid shifted by offset and labelled by their start, one group per
// distinct tuple of by() values where an absent field groups as "". stats_query
// answers the single window [start, end) the same way (unless a fixed instant
// body is set), and the raw query endpoint serves the exact evaluator.
func newTumblingFakeVL(t *testing.T, lines []tumblingLine) (*httptest.Server, *tumblingFakeVL) {
	t.Helper()
	fake := &tumblingFakeVL{}
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
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write(tumblingStatsBody(t, q, lines, start, end, step, offset))
		case "/select/logsql/stats_query":
			w.Header().Set("Content-Type", "application/json")
			if fake.instant != "" {
				_, _ = w.Write([]byte(fake.instant))
				return
			}
			start := parseFakeVLTime(t, r.Form.Get("start"))
			end := parseFakeVLTime(t, r.Form.Get("end"))
			window := end - start
			offset := ((-start)%window + window) % window
			var matrix struct {
				Data struct {
					Result []struct {
						Metric map[string]string `json:"metric"`
						Values [][2]any          `json:"values"`
					} `json:"result"`
				} `json:"data"`
			}
			_ = json.Unmarshal(tumblingStatsBody(t, r.Form.Get("query"), lines, start, end, window, offset), &matrix)
			type sample struct {
				Metric map[string]string `json:"metric"`
				Value  [2]any            `json:"value"`
			}
			vector := []sample{}
			for _, series := range matrix.Data.Result {
				if len(series.Values) == 1 {
					vector = append(vector, sample{Metric: series.Metric, Value: [2]any{json.Number(strconv.FormatInt(end/int64(time.Second), 10)), series.Values[0][1]}})
				}
			}
			// `| sort by (_c desc) | limit N` after the stats pipe.
			if m := regexp.MustCompile(`\| sort by \(_c desc\) \| limit (\d+)`).FindStringSubmatch(r.Form.Get("query")); m != nil {
				value := func(i int) float64 { f, _ := strconv.ParseFloat(fmt.Sprint(vector[i].Value[1]), 64); return f }
				sort.SliceStable(vector, func(i, j int) bool { return value(i) > value(j) })
				if n, _ := strconv.Atoi(m[1]); n < len(vector) {
					vector = vector[:n]
				}
			}
			body, _ := json.Marshal(map[string]any{"status": "success", "data": map[string]any{"resultType": "vector", "result": vector}})
			_, _ = w.Write(body)
		case "/select/logsql/query":
			fake.mu.Lock()
			fake.rawCalls++
			fake.mu.Unlock()
			start := parseFakeVLTime(t, r.Form.Get("start"))
			end := parseFakeVLTime(t, r.Form.Get("end"))
			w.Header().Set("Content-Type", "application/x-ndjson")
			for _, line := range lines {
				if line.ts < start || line.ts >= end || !fakeWindowPhaseKeep(t, r.Form.Get("query"), line.ts) {
					continue
				}
				fields := map[string]string{"_time": time.Unix(0, line.ts).UTC().Format(time.RFC3339Nano), "_msg": line.msg, "_stream": tumblingStream(line.labels)}
				for k, v := range line.labels {
					fields[k] = v
				}
				row, _ := json.Marshal(fields)
				_, _ = w.Write(append(row, '\n'))
			}
		default:
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{}`))
		}
	}))
	t.Cleanup(srv.Close)
	return srv, fake
}

func tumblingStream(labels map[string]string) string {
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, fmt.Sprintf("%s=%q", k, labels[k]))
	}
	return "{" + strings.Join(parts, ",") + "}"
}

func tumblingStatsBody(t *testing.T, q string, lines []tumblingLine, start, end, step, offset int64) []byte {
	t.Helper()
	m := tumblingStatsByRE.FindStringSubmatch(q)
	if m == nil {
		t.Errorf("fake VL: unsupported stats query %q", q)
		return []byte(`{"status":"success","data":{"resultType":"matrix","result":[]}}`)
	}
	divisor, groupBy := 1.0, m[1]
	// A rate pipeline divides the inner counts and regroups them.
	if dm := tumblingMathRE.FindStringSubmatch(q); dm != nil {
		divisor, _ = strconv.ParseFloat(dm[1], 64)
		groupBy = dm[2]
	}
	var by []string
	for _, f := range strings.Split(groupBy, ",") {
		if f = strings.TrimSpace(f); f != "" {
			by = append(by, f)
		}
	}
	bytesAgg := m[2] == "sum_len(_msg)"
	withPresence := strings.Contains(q, "count() as __sample_count")
	// unpack_logfmt, and the `level=<level> <_>` pattern the tests use, expose
	// the logfmt fields of a line.
	parsed := strings.Contains(q, "| unpack_logfmt") || strings.Contains(q, `| extract "level=<level> <_>"`)
	type group struct {
		metric  map[string]string
		buckets map[int64]float64
		present map[int64]float64
	}
	groups := map[string]*group{}
	for _, line := range lines {
		if line.ts < start || line.ts >= end || !fakeWindowPhaseKeep(t, q, line.ts) || !fakeInFilterKeep(q, line.labels) {
			continue
		}
		metric := map[string]string{}
		for _, f := range by {
			switch {
			case f == "_stream":
				metric[f] = tumblingStream(line.labels)
			case line.labels[f] == "" && parsed:
				metric[f] = slidingLogfmtFields(line.msg)[f]
			default:
				metric[f] = line.labels[f]
			}
		}
		raw, _ := json.Marshal(metric)
		g := groups[string(raw)]
		if g == nil {
			g = &group{metric: metric, buckets: map[int64]float64{}, present: map[int64]float64{}}
			groups[string(raw)] = g
		}
		bucket := vlTruncate(line.ts, step, offset)
		if bytesAgg {
			g.buckets[bucket] += float64(len(line.msg))
		} else {
			g.buckets[bucket]++
		}
		g.present[bucket]++
	}
	keys := make([]string, 0, len(groups))
	for k := range groups {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	type series struct {
		Metric map[string]string `json:"metric"`
		Values [][2]any          `json:"values"`
	}
	values := func(points map[int64]float64, div float64) [][2]any {
		tss := make([]int64, 0, len(points))
		for ts := range points {
			tss = append(tss, ts)
		}
		sort.Slice(tss, func(i, j int) bool { return tss[i] < tss[j] })
		out := make([][2]any, 0, len(tss))
		for _, ts := range tss {
			out = append(out, [2]any{json.Number(strconv.FormatFloat(float64(ts)/1e9, 'f', -1, 64)), strconv.FormatFloat(points[ts]/div, 'f', -1, 64)})
		}
		return out
	}
	result := []series{}
	for _, k := range keys {
		g := groups[k]
		metric := map[string]string{"__name__": "c"}
		for name, v := range g.metric {
			metric[name] = v
		}
		result = append(result, series{Metric: metric, Values: values(g.buckets, divisor)})
		if withPresence {
			presence := map[string]string{"__name__": "__sample_count"}
			for name, v := range g.metric {
				presence[name] = v
			}
			result = append(result, series{Metric: presence, Values: values(g.present, 1)})
		}
	}
	body, _ := json.Marshal(map[string]any{"status": "success", "data": map[string]any{"resultType": "matrix", "result": result}})
	return body
}

// lokiTumblingReference evaluates fn with Loki semantics: the sample at t covers
// (t-window, t], steps without lines are absent, and a group label whose value
// is empty is not part of the series labels. by == nil keeps every stream label.
func lokiTumblingReference(lines []tumblingLine, fn string, by []string, start, end time.Time, step, window time.Duration) map[string]map[int64]string {
	return lokiTumblingReferenceParsed(lines, fn, by, start, end, step, window, false)
}

// lokiTumblingReferenceParsed is lokiTumblingReference for a pipeline with
// | logfmt when parsed is set: by() also reads the line's logfmt fields.
func lokiTumblingReferenceParsed(lines []tumblingLine, fn string, by []string, start, end time.Time, step, window time.Duration, parsed bool) map[string]map[int64]string {
	out := map[string]map[int64]string{}
	for t := start; !t.After(end); t = t.Add(step) {
		values := map[string]float64{}
		for _, line := range lines {
			if line.ts <= t.Add(-window).UnixNano() || line.ts > t.UnixNano() {
				continue
			}
			metric := map[string]string{}
			if by == nil {
				for k, v := range line.labels {
					metric[k] = v
				}
			}
			for _, f := range by {
				v := line.labels[f]
				if v == "" && parsed {
					v = slidingLogfmtFields(line.msg)[f]
				}
				if v != "" {
					metric[f] = v
				}
			}
			key := canonicalLabelsKey(metric)
			if strings.HasPrefix(fn, "bytes_") {
				values[key] += float64(len(line.msg))
			} else {
				values[key]++
			}
		}
		for key, v := range values {
			if strings.HasSuffix(fn, "rate") {
				v /= window.Seconds()
			}
			if out[key] == nil {
				out[key] = map[int64]string{}
			}
			out[key][t.Unix()] = strconv.FormatFloat(v, 'f', -1, 64)
		}
	}
	return out
}

// runTumblingQuery runs a range or instant query and returns series labels
// (canonical key) → timestamp seconds → value. service_name and detected_level,
// which Loki and the proxy synthesize for raw stream series, are left out of
// the key.
func runTumblingQuery(t *testing.T, p *Proxy, path string, params url.Values) map[string]map[int64]string {
	t.Helper()
	rec := serveTumblingQuery(p, path, params, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("%s: expected 200, got %d: %s", params.Get("query"), rec.Code, rec.Body.String())
	}
	return decodeTumblingSeries(t, params.Get("query"), rec.Body.Bytes())
}

func serveTumblingQuery(p *Proxy, path string, params url.Values, header http.Header) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodGet, path+"?"+params.Encode(), nil)
	for k, v := range header {
		req.Header[k] = v
	}
	rec := httptest.NewRecorder()
	if strings.HasSuffix(path, "query_range") {
		p.handleQueryRange(rec, req)
	} else {
		p.handleQuery(rec, req)
	}
	return rec
}

func decodeTumblingSeries(t *testing.T, query string, body []byte) map[string]map[int64]string {
	t.Helper()
	var resp struct {
		Data struct {
			Result []struct {
				Metric map[string]string `json:"metric"`
				Values [][]any           `json:"values"`
				Value  []any             `json:"value"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatalf("%s: decode: %v: %s", query, err, string(body))
	}
	got := map[string]map[int64]string{}
	for _, series := range resp.Data.Result {
		delete(series.Metric, "service_name")
		delete(series.Metric, "detected_level")
		key := canonicalLabelsKey(series.Metric)
		if got[key] != nil {
			t.Errorf("%s: duplicate series %s: %s", query, key, string(body))
		} else {
			got[key] = map[int64]string{}
		}
		points := series.Values
		if series.Value != nil {
			points = append(points, series.Value)
		}
		for _, pair := range points {
			ts, _ := pair[0].(float64)
			v, _ := pair[1].(string)
			got[key][int64(ts)] = v
		}
	}
	return got
}

func tumblingFixture(base time.Time) []tumblingLine {
	var lines []tumblingLine
	add := func(labels map[string]string, every time.Duration, msg func(int) string) {
		for i := 0; i < int(20*time.Minute/every); i++ {
			lines = append(lines, tumblingLine{ts: base.Add(time.Duration(i) * every).UnixNano(), labels: labels, msg: msg(i)})
		}
	}
	// Every stream writes a line exactly on each window edge.
	add(map[string]string{"app": "tumble", "pod": "a"}, 10*time.Second, func(i int) string { return fmt.Sprintf("level=info msg=%q", strings.Repeat("a", i%5)) })
	add(map[string]string{"app": "tumble", "pod": "b"}, 20*time.Second, func(i int) string { return fmt.Sprintf("level=error msg=%q", strings.Repeat("b", i%3)) })
	add(map[string]string{"app": "tumble"}, 30*time.Second, func(i int) string { return "no pod line" })
	sort.Slice(lines, func(i, j int) bool { return lines[i].ts < lines[j].ts })
	return lines
}

func assertTumblingEqual(t *testing.T, query string, want, got map[string]map[int64]string) {
	t.Helper()
	if len(want) == 0 {
		t.Fatalf("%s: reference is empty", query)
	}
	for key, wantPoints := range want {
		gotPoints, ok := got[key]
		if !ok {
			t.Errorf("%s: missing series %s (got series %v)", query, key, tumblingKeys(got))
			continue
		}
		for ts, wv := range wantPoints {
			if gv, ok := gotPoints[ts]; !ok || gv != wv {
				t.Errorf("%s %s ts=%d: got %q (present=%v), want %q", query, key, ts, gv, ok, wv)
			}
		}
		for ts, gv := range gotPoints {
			if _, ok := wantPoints[ts]; !ok {
				t.Errorf("%s %s ts=%d: unexpected point %q", query, key, ts, gv)
			}
		}
	}
	for key := range got {
		if _, ok := want[key]; !ok {
			t.Errorf("%s: unexpected series %s", query, key)
		}
	}
}

func tumblingKeys(m map[string]map[int64]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func tumblingRangeParams(query string, start, end time.Time, step time.Duration) url.Values {
	return url.Values{
		"query": {query},
		"start": {strconv.FormatInt(start.UnixNano(), 10)},
		"end":   {strconv.FormatInt(end.UnixNano(), 10)},
		"step":  {strconv.Itoa(int(step.Seconds()))},
	}
}

// Range == step: Loki's sample at T covers (T-range, T], so the first step holds
// only the line written exactly at start and every later bucket moves forward by
// one window. VictoriaLogs labels buckets by their start, so grouped and bare
// log range metrics must be fetched one window early on an anchored grid and
// relabelled, like rate already was.
func TestTumblingRangeMetric_BucketsLabelledAtLokiEvaluationTime(t *testing.T) {
	base := time.Unix(1700000400, 0).UTC() // 300s-aligned
	lines := tumblingFixture(base)
	step := 5 * time.Minute
	cases := []struct {
		query, fn string
		by        []string
	}{
		{`count_over_time({app="tumble"}[5m])`, "count_over_time", nil},
		{`sum by (pod) (count_over_time({app="tumble"}[5m]))`, "count_over_time", []string{"pod"}},
		{`sum(count_over_time({app="tumble"}[5m]))`, "count_over_time", []string{}},
		{`rate({app="tumble"}[5m])`, "rate", nil},
		{`bytes_over_time({app="tumble"}[5m])`, "bytes_over_time", nil},
		{`sum by (pod) (bytes_rate({app="tumble"}[5m]))`, "bytes_rate", []string{"pod"}},
		{`sum by (pod) (rate({app="tumble"}[5m]))`, "rate", []string{"pod"}},
		{`sum by (pod) (bytes_over_time({app="tumble"}[5m]))`, "bytes_over_time", []string{"pod"}},
	}
	// Grafana sends millisecond starts: VictoriaLogs formats the anchored
	// bucket labels as float seconds, which cannot carry the exact edge.
	for _, offset := range []time.Duration{0, 7 * time.Millisecond, 21 * time.Millisecond, 123 * time.Millisecond, 999 * time.Millisecond} {
		start, end := base.Add(offset), base.Add(15*time.Minute+offset)
		for _, tc := range cases {
			t.Run(fmt.Sprintf("start+%s/%s", offset, tc.query), func(t *testing.T) {
				srv, fake := newTumblingFakeVL(t, lines)
				p := newSlidingTestProxy(t, srv.URL)
				got := runTumblingQuery(t, p, "/loki/api/v1/query_range", tumblingRangeParams(tc.query, start, end, step))
				assertTumblingEqual(t, tc.query, lokiTumblingReference(lines, tc.fn, tc.by, start, end, step, 5*time.Minute), got)
				fake.mu.Lock()
				defer fake.mu.Unlock()
				for _, call := range fake.statsCalls {
					if call.offset == "" {
						t.Errorf("%s: stats call without an anchored grid: %+v", tc.query, call)
					}
				}
			})
		}
	}
}

// A grouped count over hours is one anchored bucket scan: no ranking query
// ahead of it, Loki's windows, and the lines without the grouped label as the
// unlabelled series.
func TestTumblingRangeMetric_LongRangeSingleScanKeepsLokiWindows(t *testing.T) {
	base := time.Unix(1700000400, 0).UTC()
	lines := tumblingFixture(base)
	start, end, step := base, base.Add(3*time.Hour), 5*time.Minute
	query := `sum by (pod) (count_over_time({app="tumble"}[5m]))`
	srv, fake := newTumblingFakeVL(t, lines)
	p := newSlidingTestProxy(t, srv.URL)
	got := runTumblingQuery(t, p, "/loki/api/v1/query_range", tumblingRangeParams(query, start, end, step))
	assertTumblingEqual(t, query, lokiTumblingReference(lines, "count_over_time", []string{"pod"}, start, end, step, 5*time.Minute), got)
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if len(fake.statsCalls) != 1 || fake.statsCalls[0].offset == "" || fake.rawCalls != 0 {
		t.Fatalf("expected one anchored bucket scan, got %+v and %d raw scans", fake.statsCalls, fake.rawCalls)
	}
}

// conformance: series-limits-and-partial-results, limits/series-limit-error, limits/drilldown-partial-with-warning
// conformance: loki_api_v1_query_range, loki_api_v1_query
// A grouped count with more series than -max-stats-query-series fails like
// Loki's max_query_series limit, naming the proxy flag. Logs Drilldown gets
// Loki's partial result instead: the busiest series, exact, and a warning. When
// the bucket response overflows its byte bound, a stats_query ranks the values
// and the bucket query restricted to them returns those series.
func TestShortRangeMetric_TooManySeriesFollowsLokiLimit(t *testing.T) {
	base := time.Unix(1700000400, 0).UTC()
	var lines []tumblingLine
	for i := 0; i < 400; i++ {
		pod := fmt.Sprintf("p%03d", i)
		for k := 0; k <= i; k++ { // pod i writes i+1 lines, spread over 15 minutes
			ts := base.Add(time.Duration(k) * 15 * time.Minute / time.Duration(i+1))
			lines = append(lines, tumblingLine{ts: ts.UnixNano(), labels: map[string]string{"app": "top", "pod": pod}, msg: "x"})
		}
	}
	total := func(points map[int64]string) float64 {
		sum := 0.0
		for _, v := range points {
			f, _ := strconv.ParseFloat(v, 64)
			sum += f
		}
		return sum
	}
	t.Run("instant query", func(t *testing.T) {
		srv, fake := newTumblingFakeVL(t, lines)
		p := newSlidingTestProxy(t, srv.URL)
		p.maxStatsQuerySeries = 150
		query := `sum by (pod) (count_over_time({app="top"}[5m]))`
		params := url.Values{"query": {query}, "time": {strconv.FormatInt(base.Add(10*time.Minute).Unix(), 10)}}

		rec := serveTumblingQuery(p, "/loki/api/v1/query", params, nil)
		if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), "maximum number of series (150) reached for a single query") {
			t.Fatalf("expected Loki's series limit error, got %d: %s", rec.Code, rec.Body.String())
		}

		rec = serveTumblingQuery(p, "/loki/api/v1/query", params, http.Header{"X-Query-Tags": {"Source=grafana-lokiexplore-app"}})
		if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"warnings":["maximum number of series (150) reached for a single query; returning partial results"]`) {
			t.Fatalf("Drilldown: expected a partial vector with Loki's warning, got %d: %s", rec.Code, rec.Body.String())
		}
		got := decodeTumblingSeries(t, query, rec.Body.Bytes())
		if len(got) != 150 {
			t.Fatalf("Drilldown: expected 150 series, got %d", len(got))
		}
		fake.mu.Lock()
		defer fake.mu.Unlock()
		if fake.rawCalls != 0 {
			t.Fatalf("the series limit must not fall back to raw scans, got %d", fake.rawCalls)
		}
	})

	t.Run("range equals step", func(t *testing.T) {
		srv, fake := newTumblingFakeVL(t, lines)
		p := newSlidingTestProxy(t, srv.URL)
		p.maxStatsQuerySeries = 150
		query := `sum by (pod) (count_over_time({app="top"}[5m]))`
		rec := serveTumblingQuery(p, "/loki/api/v1/query_range", tumblingRangeParams(query, base.Add(5*time.Minute), base.Add(15*time.Minute), 5*time.Minute), nil)
		// Loki's message, word for word: no proxy-internal text reaches Grafana.
		if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), "maximum number of series (150) reached for a single query; consider reducing query cardinality") || strings.Contains(rec.Body.String(), "-max-stats-query-series") {
			t.Fatalf("expected Loki's series limit error, got %d: %s", rec.Code, rec.Body.String())
		}
		fake.mu.Lock()
		defer fake.mu.Unlock()
		if fake.rawCalls != 0 {
			t.Fatalf("the series limit must not fall back to raw scans, got %d", fake.rawCalls)
		}
	})
	for _, tc := range []struct {
		name      string
		span      time.Duration
		limit     int64
		wantCalls int
		wantIn    bool
	}{
		{"response overflow ranks values", 15 * time.Minute, 40 << 10, 2, true},
		{"series over the limit over hours", 3 * time.Hour, 64 << 20, 1, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			old := maxStatsBucketResponseBytes
			maxStatsBucketResponseBytes = tc.limit
			t.Cleanup(func() { maxStatsBucketResponseBytes = old })

			start, end, step := base, base.Add(tc.span), 5*time.Minute
			query := `sum by (pod) (count_over_time({app="top"}[1m]))`
			srv, fake := newTumblingFakeVL(t, lines)
			p := newSlidingTestProxy(t, srv.URL)
			p.maxStatsQuerySeries = 150 // -max-stats-query-series
			params := tumblingRangeParams(query, start, end, step)

			rec := serveTumblingQuery(p, "/loki/api/v1/query_range", params, nil)
			if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), "maximum number of series (150) reached for a single query; consider reducing query cardinality") || strings.Contains(rec.Body.String(), "-max-stats-query-series") {
				t.Fatalf("expected Loki's series limit error, got %d: %s", rec.Code, rec.Body.String())
			}
			fake.mu.Lock()
			rawBefore := fake.rawCalls
			fake.statsCalls = nil
			fake.mu.Unlock()
			if rawBefore != 0 {
				t.Fatalf("the series limit must not fall back to raw scans, got %d", rawBefore)
			}

			rec = serveTumblingQuery(p, "/loki/api/v1/query_range", params, http.Header{"X-Query-Tags": {"Source=grafana-lokiexplore-app"}})
			if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"warnings":["maximum number of series (150) reached for a single query; returning partial results"]`) {
				t.Fatalf("Drilldown: expected a partial result with Loki's warning, got %d: %s", rec.Code, rec.Body.String())
			}
			got := decodeTumblingSeries(t, query, rec.Body.Bytes())
			want := lokiTumblingReference(lines, "count_over_time", []string{"pod"}, start, end, step, time.Minute)
			if want := p.resolvedMaxStatsQuerySeries(t.Context()); len(got) != want {
				t.Fatalf("expected the %d busiest series (-max-stats-query-series), got %d", want, len(got))
			}
			// Every kept series is exact, and none is quieter than a dropped one
			// (the ranking counts the lines inside the evaluated windows).
			kept, minKept, maxDropped := map[string]map[int64]string{}, math.Inf(1), 0.0
			for key, points := range want {
				if _, ok := got[key]; ok {
					kept[key] = points
					minKept = math.Min(minKept, total(points))
				} else {
					maxDropped = math.Max(maxDropped, total(points))
				}
			}
			assertTumblingEqual(t, query, kept, got)
			if maxDropped > minKept {
				t.Fatalf("a dropped series (%v window lines) is busier than a kept one (%v)", maxDropped, minKept)
			}
			fake.mu.Lock()
			defer fake.mu.Unlock()
			last := fake.statsCalls[len(fake.statsCalls)-1]
			if fake.rawCalls != 0 || len(fake.statsCalls) != tc.wantCalls || strings.Contains(last.query, "pod:in(") != tc.wantIn {
				t.Fatalf("expected %d bucket calls ending with the top-value one and no raw scans, got %d raw scans and %d calls", tc.wantCalls, fake.rawCalls, len(fake.statsCalls))
			}
		})
	}
}

// A backend known to be without the stats_query_range offset arg has only
// epoch-aligned buckets. They serve the tumbling relabel when the start is
// aligned to the range; any other start would count lines of the wrong windows,
// so the window evaluator answers. Lines sit off window edges, as epoch buckets
// are [T, T+range) while Loki's windows are (T-range, T].
func TestTumblingRangeMetric_BackendWithoutOffsetSupport(t *testing.T) {
	base := time.Unix(1700000400, 0).UTC()
	lines := tumblingFixture(base.Add(2500 * time.Millisecond))
	step := 5 * time.Minute
	query := `sum by (pod) (count_over_time({app="tumble"}[5m]))`
	for _, offset := range []time.Duration{0, 123 * time.Millisecond, 7 * time.Second, 150 * time.Second} {
		t.Run(fmt.Sprintf("start+%s", offset), func(t *testing.T) {
			start, end := base.Add(5*time.Minute+offset), base.Add(15*time.Minute+offset)
			srv, fake := newTumblingFakeVL(t, lines)
			p := newGapTestProxy(t, srv.URL)
			p.storeBackendVersion("v1.44.0", "v1.44.0")
			got := runTumblingQuery(t, p, "/loki/api/v1/query_range", tumblingRangeParams(query, start, end, step))
			assertTumblingEqual(t, query, lokiTumblingReference(lines, "count_over_time", []string{"pod"}, start, end, step, 5*time.Minute), got)
			fake.mu.Lock()
			defer fake.mu.Unlock()
			if offset == 0 {
				if len(fake.statsCalls) != 1 || fake.statsCalls[0].offset != "" || fake.rawCalls != 0 {
					t.Fatalf("aligned start: expected one offset-free stats call, got %+v and %d raw scans", fake.statsCalls, fake.rawCalls)
				}
				return
			}
			if len(fake.statsCalls) != 0 || fake.rawCalls != 1 {
				t.Fatalf("unaligned start: expected the window evaluator, got %+v and %d raw scans", fake.statsCalls, fake.rawCalls)
			}
		})
	}
}

// Grafana Logs Drilldown panels build a bucket-start axis in the hits and hybrid
// paths. A Drilldown request keeps that axis for every grouped count panel, a
// value-filter panel (routed through the direct path's windowed /hits) as well
// as an existence-filter histogram, so all panels of a page line up.
func TestTumblingRangeMetric_DrilldownPanelsShareOneAxis(t *testing.T) {
	const startSec, endSec, stepSec = int64(1700006400), int64(1700017200), int64(300) // 3h, step-aligned
	backend := newRecorderBackend()
	backend.on("/select/logsql/hits", func(w http.ResponseWriter, r *http.Request) {
		points := [][2]any{}
		for ts := startSec; ts <= endSec; ts += stepSec {
			points = append(points, [2]any{time.Unix(ts, 0).UTC().Format(time.RFC3339), 3})
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(hitsResponse("pod", map[string][][2]any{"a": points})))
	})
	vl := backend.server()
	defer vl.Close()

	// axis returns the first series as timestamp=value pairs.
	axis := func(query, logsql string) []string {
		t.Helper()
		p := newTestProxy(t, vl.URL)
		p.storeBackendVersion("v1.50.0", "v1.50.0")
		r := drilldownRequest(t, query, startSec, endSec, strconv.FormatInt(stepSec, 10), "drilldown")
		w := httptest.NewRecorder()
		p.proxyStatsQueryRange(w, r, logsql)
		var resp struct {
			Data struct {
				Result []struct {
					Values [][2]any `json:"values"`
				} `json:"result"`
			} `json:"data"`
		}
		if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil || len(resp.Data.Result) == 0 {
			t.Fatalf("%s: expected series, got %s (%v)", query, w.Body.String(), err)
		}
		var out []string
		for _, v := range resp.Data.Result[0].Values {
			ts, _ := v[0].(float64)
			out = append(out, fmt.Sprintf("%d=%v", int64(ts), v[1]))
		}
		return out
	}
	existence := axis(`sum by (pod) (count_over_time({namespace="prod"} | pod!="" [5m]))`,
		`namespace:="prod" | filter pod:!"" | stats by (pod) count()`)
	valueFilter := axis(`sum by (pod) (count_over_time({namespace="prod"} | detected_level="error" [5m]))`,
		`namespace:="prod" | filter level:="error" | stats by (pod) count()`)
	if fmt.Sprint(existence) != fmt.Sprint(valueFilter) {
		t.Fatalf("Drilldown panels on different axes:\nexistence filter: %v\nvalue filter:     %v", existence, valueFilter)
	}
	// The /hits bucket labelled start keeps its label.
	if want := fmt.Sprintf("%d=3", startSec); existence[0] != want {
		t.Fatalf("Drilldown axis must keep the bucket-start labels (first point %s), got %v", want, existence)
	}
}

// Range < step: each sample covers only (T-range, T]; the lines in the rest of
// the step are not part of any window, and steps whose window holds no lines
// are absent. Range > step windows overlap. Both are answered from one call of
// anchored gcd(step, range) buckets, per stream and with parser stages too,
// never from a raw log scan.
func TestShortRangeMetric_RangeBelowStepCountsOnlyTheWindow(t *testing.T) {
	base := time.Unix(1700000400, 0).UTC()
	lines := tumblingFixture(base)
	// A 5m gap in pod a: the step at base+10m has no line of pod a in its window.
	kept := lines[:0]
	for _, line := range lines {
		if line.labels["pod"] == "a" && line.ts > base.Add(9*time.Minute).UnixNano() && line.ts <= base.Add(14*time.Minute).UnixNano() {
			continue
		}
		kept = append(kept, line)
	}
	lines = kept
	start, end := base, base.Add(15*time.Minute)
	cases := []struct {
		query, fn    string
		by           []string
		window, step time.Duration
		parsed       bool
	}{
		{`rate({app="tumble"}[1m])`, "rate", nil, time.Minute, 5 * time.Minute, false},
		{`count_over_time({app="tumble"}[1m])`, "count_over_time", nil, time.Minute, 5 * time.Minute, false},
		{`sum by (pod) (count_over_time({app="tumble"}[1m]))`, "count_over_time", []string{"pod"}, time.Minute, 5 * time.Minute, false},
		{`sum(rate({app="tumble"}[1m]))`, "rate", []string{}, time.Minute, 5 * time.Minute, false},
		{`sum by (pod) (bytes_over_time({app="tumble"}[1m]))`, "bytes_over_time", []string{"pod"}, time.Minute, 5 * time.Minute, false},
		{`sum by (level) (count_over_time({app="tumble"} | logfmt [1m]))`, "count_over_time", []string{"level"}, time.Minute, 5 * time.Minute, true},
		{`sum by (level) (count_over_time({app="tumble"} | pattern "level=<level> <_>" [1m]))`, "count_over_time", []string{"level"}, time.Minute, 5 * time.Minute, true},
		{`sum by (level) (count_over_time({app="tumble"} | pattern "level=<level> <_>" [5m]))`, "count_over_time", []string{"level"}, 5 * time.Minute, time.Minute, true},
		{`rate({app="tumble"}[5m])`, "rate", nil, 5 * time.Minute, time.Minute, false},
		{`count_over_time({app="tumble"}[90s])`, "count_over_time", nil, 90 * time.Second, time.Minute, false},
		// A step the range does not divide: gcd buckets would be 7s wide.
		{`sum by (pod) (count_over_time({app="tumble"}[1m]))`, "count_over_time", []string{"pod"}, time.Minute, 67 * time.Second, false},
		{`rate({app="tumble"}[2m])`, "rate", nil, 2 * time.Minute, 7 * time.Minute, false},
	}
	for _, tc := range cases {
		t.Run(tc.query+"@"+tc.step.String(), func(t *testing.T) {
			srv, fake := newTumblingFakeVL(t, lines)
			p := newSlidingTestProxy(t, srv.URL)
			got := runTumblingQuery(t, p, "/loki/api/v1/query_range", tumblingRangeParams(tc.query, start, end, tc.step))
			assertTumblingEqual(t, tc.query, lokiTumblingReferenceParsed(lines, tc.fn, tc.by, start, end, tc.step, tc.window, tc.parsed), got)
			fake.mu.Lock()
			defer fake.mu.Unlock()
			// Range < step: one bucket per step over window-phase-filtered lines.
			// Range > step: gcd(step, range) buckets.
			bucket, phased := tc.step, tc.window < tc.step
			if !phased {
				for rest := tc.window; rest > 0; {
					bucket, rest = rest, bucket%rest
				}
			}
			if fake.rawCalls != 0 || len(fake.statsCalls) != 1 || fake.statsCalls[0].step != fmt.Sprintf("%ds", int(bucket.Seconds())) ||
				strings.Contains(fake.statsCalls[0].query, "__lvp_window_phase") != phased {
				t.Errorf("%s: expected one %s stats bucket call (window phase filter %v) and no raw scan, got %d raw scans and %+v", tc.query, bucket, phased, fake.rawCalls, fake.statsCalls)
			}
		})
	}
}

// Loki's label builder drops empty values: grouping by a label that only some
// streams carry, or that lives in the log body only, never emits label="".
func TestStatsMetricLabels_EmptyGroupValuesAreOmitted(t *testing.T) {
	base := time.Unix(1700000400, 0).UTC()
	lines := tumblingFixture(base)
	start, end, step := base, base.Add(15*time.Minute), 5*time.Minute
	for _, tc := range []struct {
		query string
		by    []string
	}{
		{`sum by (level) (count_over_time({app="tumble"}[5m]))`, []string{"level"}},
		{`sum by (app, pod) (count_over_time({app="tumble"}[5m]))`, []string{"app", "pod"}},
	} {
		t.Run(tc.query, func(t *testing.T) {
			srv, _ := newTumblingFakeVL(t, lines)
			p := newSlidingTestProxy(t, srv.URL)
			got := runTumblingQuery(t, p, "/loki/api/v1/query_range", tumblingRangeParams(tc.query, start, end, step))
			assertTumblingEqual(t, tc.query, lokiTumblingReference(lines, "count_over_time", tc.by, start, end, step, 5*time.Minute), got)
		})
	}

	// Loki's instant window is (time-range, time]: a line at time-range is out,
	// a line at time is in.
	t.Run("instant_window_edges", func(t *testing.T) {
		srv, _ := newTumblingFakeVL(t, lines)
		p := newSlidingTestProxy(t, srv.URL)
		for _, at := range []time.Time{base, base.Add(5 * time.Minute), base.Add(7*time.Minute + 30*time.Second)} {
			query := `sum by (pod) (count_over_time({app="tumble"}[5m]))`
			got := runTumblingQuery(t, p, "/loki/api/v1/query", url.Values{"query": {query}, "time": {strconv.FormatInt(at.Unix(), 10)}})
			assertTumblingEqual(t, query+"@"+at.String(), lokiTumblingReference(lines, "count_over_time", []string{"pod"}, at, at, time.Minute, 5*time.Minute), got)
		}
	})

	t.Run("instant", func(t *testing.T) {
		srv, fake := newTumblingFakeVL(t, nil)
		fake.instant = `{"status":"success","data":{"resultType":"vector","result":[` +
			`{"metric":{"app":"tumble","pod":""},"value":[1700001300,"10"]},` +
			`{"metric":{"app":"tumble","pod":"a"},"value":[1700001300,"30"]}]}}`
		p := newSlidingTestProxy(t, srv.URL)
		query := `sum by (app, pod) (count_over_time({app="tumble"}[5m]))`
		got := runTumblingQuery(t, p, "/loki/api/v1/query", url.Values{"query": {query}, "time": {"1700001300"}})
		want := map[string]map[int64]string{
			canonicalLabelsKey(map[string]string{"app": "tumble"}):             {1700001300: "10"},
			canonicalLabelsKey(map[string]string{"app": "tumble", "pod": "a"}): {1700001300: "30"},
		}
		assertTumblingEqual(t, query, want, got)
	})
}

// conformance: window-bounds-and-step-alignment, semantics/offset-sample-timestamps
// conformance: loki_api_v1_query_range, loki_api_v1_query
// Loki evaluates `[5m] offset 5m` at T over (T-10m, T-5m] and stamps the sample
// at T, on range and instant queries alike.
func TestTumblingRangeMetric_OffsetStampsEvaluationTime(t *testing.T) {
	base := time.Unix(1700000400, 0).UTC()
	lines := tumblingFixture(base)
	offset := 5 * time.Minute
	shifted := make([]tumblingLine, len(lines)) // offset windows as plain windows
	for i, line := range lines {
		shifted[i] = line
		shifted[i].ts += int64(offset)
	}
	query := `sum by (pod) (count_over_time({app="tumble"}[5m] offset 5m))`
	start, end, step := base.Add(10*time.Minute), base.Add(25*time.Minute), 5*time.Minute

	srv, _ := newTumblingFakeVL(t, lines)
	p := newSlidingTestProxy(t, srv.URL)
	got := runTumblingQuery(t, p, "/loki/api/v1/query_range", tumblingRangeParams(query, start, end, step))
	assertTumblingEqual(t, query, lokiTumblingReference(shifted, "count_over_time", []string{"pod"}, start, end, step, 5*time.Minute), got)

	at := base.Add(17 * time.Minute)
	got = runTumblingQuery(t, p, "/loki/api/v1/query", url.Values{"query": {query}, "time": {strconv.FormatInt(at.Unix(), 10)}})
	assertTumblingEqual(t, query+"@instant", lokiTumblingReference(shifted, "count_over_time", []string{"pod"}, at, at, time.Minute, 5*time.Minute), got)
}
