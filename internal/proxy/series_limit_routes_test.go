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

// seriesLimitLine is one log line of the series limit fixture: a pod stream of
// app="top" whose body is JSON with the fields the queries parse.
type seriesLimitLine struct {
	ts  int64
	pod string
}

func (l seriesLimitLine) stream() string { return `{app="top",pod="` + l.pod + `"}` }

func (l seriesLimitLine) fields() map[string]string {
	return map[string]string{"app": "top", "pod": l.pod, "pipeline": "logs/loki", "latency": "2"}
}

const seriesLimitLineMsg = `{"pipeline":"logs/loki","latency":"2"}`

var (
	seriesLimitStatsRE = regexp.MustCompile(`\| stats (?:by \(([^)]*)\) )?(.+?)(?: \| .*)?$`)
	seriesLimitAggRE   = regexp.MustCompile(`^(count|sum_len|sum|max|min)\(([^)]*)\)(?: as ([A-Za-z_]+))?$`)
)

// seriesLimitFakeVL answers every VictoriaLogs endpoint the metric routes use,
// over the fixture lines: raw rows (/select/logsql/query, with the stats pipe
// of the sliding window evaluator), grouped buckets (/select/logsql/stats_query_range)
// and hits (/select/logsql/hits). Filters in the queries are ignored: every
// fixture line matches every query. It records which endpoints were called.
type seriesLimitFakeVL struct {
	t     *testing.T
	lines []seriesLimitLine

	mu    sync.Mutex
	calls map[string]int
}

func (f *seriesLimitFakeVL) record(kind string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.calls == nil {
		f.calls = map[string]int{}
	}
	f.calls[kind]++
}

func (f *seriesLimitFakeVL) snapshot() map[string]int {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make(map[string]int, len(f.calls))
	for k, v := range f.calls {
		out[k] = v
	}
	return out
}

func (f *seriesLimitFakeVL) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		f.t.Errorf("fake VL: parse form: %v", err)
		return
	}
	query := r.Form.Get("query")
	switch r.URL.Path {
	case "/select/logsql/query":
		start, end := parseFakeVLTime(f.t, r.Form.Get("start")), parseFakeVLTime(f.t, r.Form.Get("end"))
		w.Header().Set("Content-Type", "application/x-ndjson")
		if strings.HasSuffix(query, " | limit 1") {
			f.record("probe") // the stats pushdown's parse-risk probe finds nothing
			return
		}
		if body, _, ok := emulateVLStatsPipe(query, start, end, func(yield func(int64, map[string]string, string)) {
			for _, line := range f.lines {
				yield(line.ts, line.fields(), seriesLimitLineMsg)
			}
		}); ok {
			f.record("stats-pipe")
			_, _ = w.Write(body)
			return
		}
		f.record("raw")
		for _, line := range f.lines {
			if line.ts < start || line.ts >= end {
				continue
			}
			row := line.fields()
			row["_time"] = time.Unix(0, line.ts).UTC().Format(time.RFC3339Nano)
			row["_msg"] = seriesLimitLineMsg
			row["_stream"] = line.stream()
			row["level"] = "info"
			encoded, _ := json.Marshal(row)
			_, _ = w.Write(append(encoded, '\n'))
		}
	case "/select/logsql/stats_query_range":
		f.record("stats_query_range")
		start, end, step, offset, ok := slidingBucketParams(f.t, r)
		if !ok {
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(f.statsQueryRange(query, start, end, step, offset))
	case "/select/logsql/hits":
		f.record("hits")
		start, end, step, offset, ok := slidingBucketParams(f.t, r)
		if !ok {
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(f.hits(r.Form["field"], start, end, step, offset))
	default:
		f.t.Errorf("fake VL: unexpected %s %s", r.URL.Path, query)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{}`))
	}
}

// statsQueryRange groups the fixture lines of [start, end) by the stats pipe's
// by() fields on the step grid shifted by offset, one result per group and
// aggregate named by the aggregate's alias, like VictoriaLogs.
func (f *seriesLimitFakeVL) statsQueryRange(query string, start, end, step, offset int64) []byte {
	m := seriesLimitStatsRE.FindStringSubmatch(query)
	if m == nil {
		f.t.Errorf("fake VL: unsupported stats query %q", query)
		return []byte(`{"status":"success","data":{"resultType":"matrix","result":[]}}`)
	}
	var by []string
	for _, field := range strings.Split(m[1], ",") {
		if field = strings.Trim(strings.TrimSpace(field), `"`); field != "" {
			by = append(by, field)
		}
	}
	type result struct {
		metric map[string]string
		values map[int64]float64
	}
	results := map[string]*result{}
	for _, agg := range strings.Split(m[2], ", ") {
		am := seriesLimitAggRE.FindStringSubmatch(agg)
		if am == nil {
			f.t.Errorf("fake VL: unsupported aggregate %q in %q", agg, query)
			continue
		}
		for _, line := range f.lines {
			if line.ts < start || line.ts >= end {
				continue
			}
			name := am[3]
			if name == "" {
				name = am[1] + "(" + am[2] + ")"
			}
			metric := map[string]string{"__name__": name}
			for _, field := range by {
				if field == "_stream" {
					metric[field] = line.stream()
				} else {
					metric[field] = line.fields()[field]
				}
			}
			key := fmt.Sprint(metric)
			res := results[key]
			if res == nil {
				res = &result{metric: metric, values: map[int64]float64{}}
				results[key] = res
			}
			bucket := vlTruncate(line.ts, step, offset)
			value := 1.0
			switch am[1] {
			case "sum_len":
				value = float64(len(seriesLimitLineMsg))
			case "sum", "max", "min":
				value, _ = strconv.ParseFloat(line.fields()[am[2]], 64)
			}
			switch _, seen := res.values[bucket]; {
			case am[1] == "max" && seen:
				res.values[bucket] = math.Max(res.values[bucket], value)
			case am[1] == "min" && seen:
				res.values[bucket] = math.Min(res.values[bucket], value)
			case am[1] == "max" || am[1] == "min":
				res.values[bucket] = value
			default:
				res.values[bucket] += value
			}
		}
	}
	keys := make([]string, 0, len(results))
	for key := range results {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	type series struct {
		Metric map[string]string `json:"metric"`
		Values [][2]any          `json:"values"`
	}
	out := make([]series, 0, len(keys))
	for _, key := range keys {
		res := results[key]
		buckets := make([]int64, 0, len(res.values))
		for bucket := range res.values {
			buckets = append(buckets, bucket)
		}
		sort.Slice(buckets, func(i, j int) bool { return buckets[i] < buckets[j] })
		s := series{Metric: res.metric}
		for _, bucket := range buckets {
			s.Values = append(s.Values, [2]any{json.Number(strconv.FormatFloat(float64(bucket)/1e9, 'f', -1, 64)), strconv.FormatFloat(res.values[bucket], 'f', -1, 64)})
		}
		out = append(out, s)
	}
	body, _ := json.Marshal(map[string]any{"status": "success", "data": map[string]any{"resultType": "matrix", "result": out}})
	return body
}

// hits counts the fixture lines of [start, end) per bucket grouped by fields.
func (f *seriesLimitFakeVL) hits(fields []string, start, end, step, offset int64) []byte {
	type hit struct {
		Fields     map[string]string `json:"fields"`
		Timestamps []string          `json:"timestamps"`
		Values     []int             `json:"values"`
		Total      int               `json:"total"`
	}
	groups := map[string]map[int64]int{}
	labels := map[string]map[string]string{}
	for _, line := range f.lines {
		if line.ts < start || line.ts >= end {
			continue
		}
		group := map[string]string{}
		for _, field := range fields {
			group[field] = line.fields()[field]
		}
		key := fmt.Sprint(group)
		if groups[key] == nil {
			groups[key] = map[int64]int{}
			labels[key] = group
		}
		groups[key][vlTruncate(line.ts, step, offset)]++
	}
	keys := make([]string, 0, len(groups))
	for key := range groups {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	out := make([]hit, 0, len(keys))
	for _, key := range keys {
		buckets := make([]int64, 0, len(groups[key]))
		for bucket := range groups[key] {
			buckets = append(buckets, bucket)
		}
		sort.Slice(buckets, func(i, j int) bool { return buckets[i] < buckets[j] })
		h := hit{Fields: labels[key]}
		for _, bucket := range buckets {
			h.Timestamps = append(h.Timestamps, time.Unix(0, bucket).UTC().Format(time.RFC3339Nano))
			h.Values = append(h.Values, groups[key][bucket])
			h.Total += groups[key][bucket]
		}
		out = append(out, h)
	}
	body, _ := json.Marshal(map[string]any{"hits": out})
	return body
}

// seriesLimitFixture writes, for pods p0..p(n-1), i+1 lines for pod pi at each
// of the given instants, so a higher index is always the busier pod.
func seriesLimitFixture(pods int, instants ...time.Time) []seriesLimitLine {
	var lines []seriesLimitLine
	for _, at := range instants {
		for i := 0; i < pods; i++ {
			for k := 0; k <= i; k++ {
				lines = append(lines, seriesLimitLine{ts: at.Add(time.Duration(k) * time.Millisecond).UnixNano(), pod: fmt.Sprintf("p%d", i)})
			}
		}
	}
	return lines
}

type seriesLimitAnswer struct {
	Status   string   `json:"status"`
	Error    string   `json:"error"`
	Warnings []string `json:"warnings"`
	Data     struct {
		Result []struct {
			Metric map[string]string `json:"metric"`
		} `json:"result"`
	} `json:"data"`
}

// Every metric route answers a result over -max-stats-query-series the way
// Loki answers one over max_query_series (pkg/logql/engine.go JoinSampleVector,
// v3.7.7): a plain client gets logqlmodel.NewSeriesLimitError as HTTP 400, word
// for word; a Logs Drilldown request (X-Query-Tags Source=grafana-lokiexplore-app)
// gets HTTP 200 with the series it has and the warning "maximum number of
// series (N) reached for a single query; returning partial results". The proxy
// keeps the busiest series. A result with exactly the limit's series passes on
// both, without a warning.
//
// conformance: series-limits-and-partial-results, limits/series-limit-error, limits/drilldown-partial-with-warning
// conformance: loki_api_v1_query_range, loki_api_v1_query
func TestSeriesLimit_EveryMetricRouteFollowsLoki(t *testing.T) {
	base := time.Unix(1700000400, 0).UTC() // on the minute grid
	start, end := base.Add(5*time.Minute), base.Add(10*time.Minute)
	instants := []time.Time{base.Add(6*time.Minute + 10*time.Second), base.Add(8*time.Minute + 20*time.Second), base.Add(9*time.Minute + 30*time.Second)}
	const pods = 6
	rangeParams := func(query string, step time.Duration) url.Values {
		return tumblingRangeParams(query, start, end, step)
	}
	instantParams := func(query string) url.Values {
		return url.Values{"query": {query}, "time": {strconv.FormatInt(end.Unix(), 10)}}
	}
	for _, tc := range []struct {
		name     string
		path     string
		params   url.Values
		declared []string
		// route is the VictoriaLogs call that proves the route was taken.
		route string
	}{
		// A second parser keeps an ordered | json metric off the stats pushdown.
		{"ordered json raw evaluator range", "/loki/api/v1/query_range", rangeParams(`sum by (pod) (count_over_time({app="top"} | json | drop __error__, __error_details__ | json | drop __error__, __error_details__ | pipeline="logs/loki" [5m]))`, time.Minute), nil, "raw"},
		{"ordered json raw evaluator instant", "/loki/api/v1/query", instantParams(`sum by (pod) (count_over_time({app="top"} | detected_level="info" | json | drop __error__, __error_details__ | pipeline="logs/loki" [5m]))`), nil, "raw"},
		{"ordered json stats pushdown", "/loki/api/v1/query_range", rangeParams(`sum by (pod) (count_over_time({app="top"} | json | drop __error__, __error_details__ | pipeline="logs/loki" [5m]))`, time.Minute), nil, "stats_query_range"},
		// The reported Drilldown labels breakdown: a label filter before the
		// parser, pushed down with the stats query.
		{"ordered json stats pushdown with a pre-parser filter", "/loki/api/v1/query_range", rangeParams(`sum by (pod) (count_over_time({app="top"} | detected_level="info" | json | drop __error__, __error_details__ | pipeline="logs/loki" [5m]))`, time.Minute), nil, "stats_query_range"},
		{"stats buckets range", "/loki/api/v1/query_range", rangeParams(`sum by (pod) (count_over_time({app="top"}[5m]))`, time.Minute), nil, "stats_query_range"},
		{"manual raw-row evaluator range", "/loki/api/v1/query_range", rangeParams(`quantile_over_time(0.5, {app="top"} | unwrap latency [5m]) by (pod)`, time.Minute), nil, "raw"},
		{"manual raw-row evaluator instant", "/loki/api/v1/query", instantParams(`quantile_over_time(0.5, {app="top"} | unwrap latency [5m]) by (pod)`), nil, "raw"},
		{"bare parser raw evaluator range", "/loki/api/v1/query_range", rangeParams(`avg_over_time({app="top"} | logfmt | unwrap latency [5m])`, time.Minute), nil, "raw"},
		{"bare parser raw evaluator instant", "/loki/api/v1/query", instantParams(`avg_over_time({app="top"} | logfmt | unwrap latency [5m])`), nil, "raw"},
		{"bare parser stats buckets", "/loki/api/v1/query_range", rangeParams(`count_over_time({app="top"} | logfmt [5m])`, time.Minute), nil, "stats_query_range"},
		{"bare parser hits buckets", "/loki/api/v1/query_range", rangeParams(`count_over_time({app="top"} | logfmt [5m])`, time.Minute), []string{"app", "pod"}, "hits"},
		{"bare parser unwrap stats buckets", "/loki/api/v1/query_range", rangeParams(`sum_over_time({app="top"} | logfmt | unwrap latency [5m])`, time.Minute), nil, "stats_query_range"},
		{"binary with scalar", "/loki/api/v1/query_range", rangeParams(`sum by (pod) (count_over_time({app="top"}[5m])) * 2`, time.Minute), nil, "stats_query_range"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, limit := range []int{3, pods} {
				for _, drilldown := range []bool{false, true} {
					fake := &seriesLimitFakeVL{t: t, lines: seriesLimitFixture(pods, instants...)}
					srv := httptest.NewServer(fake)
					p := newSlidingTestProxy(t, srv.URL)
					p.maxStatsQuerySeries = limit
					p.declaredLabelFields = tc.declared
					header := http.Header{}
					if drilldown {
						header.Set("X-Query-Tags", "Source=grafana-lokiexplore-app")
					}
					rec := serveTumblingQuery(p, tc.path, tc.params, header)
					srv.Close()
					calls := fake.snapshot()
					if calls[tc.route] == 0 {
						t.Fatalf("limit=%d drilldown=%v: route not taken, VictoriaLogs calls %v", limit, drilldown, calls)
					}
					assertSeriesLimitAnswer(t, rec, limit, pods, drilldown, "pod", "[p3 p4 p5]")
				}
			}
		})
	}
}

// assertSeriesLimitAnswer checks a response to a query whose full result holds
// total series against Loki's contract for the series limit: whole below or
// at the limit; above it Loki's 400 error for a plain client, or for Logs
// Drilldown the busiest limit series (their label values, sorted, are kept)
// with Loki's partial-result warning.
func assertSeriesLimitAnswer(t *testing.T, rec *httptest.ResponseRecorder, limit, total int, drilldown bool, label, kept string) {
	t.Helper()
	var answer seriesLimitAnswer
	if err := json.Unmarshal(rec.Body.Bytes(), &answer); err != nil {
		t.Fatalf("limit=%d drilldown=%v: decode %d %s: %v", limit, drilldown, rec.Code, rec.Body.String(), err)
	}
	switch {
	case total <= limit:
		if rec.Code != http.StatusOK || len(answer.Data.Result) != total || answer.Warnings != nil {
			t.Fatalf("limit=%d drilldown=%v: a result within the limit must pass whole without a warning, got %d %s", limit, drilldown, rec.Code, rec.Body.String())
		}
	case !drilldown:
		want := fmt.Sprintf("maximum number of series (%d) reached for a single query; consider reducing query cardinality by adding more specific stream selectors, reducing the time range, or aggregating results with functions like sum(), count() or topk()", limit)
		if rec.Code != http.StatusBadRequest || answer.Error != want || answer.Data.Result != nil {
			t.Fatalf("limit=%d: expected Loki's series limit error, got %d %s", limit, rec.Code, rec.Body.String())
		}
	default:
		want := []string{fmt.Sprintf("maximum number of series (%d) reached for a single query; returning partial results", limit)}
		if rec.Code != http.StatusOK || fmt.Sprint(answer.Warnings) != fmt.Sprint(want) || len(answer.Data.Result) != limit {
			t.Fatalf("limit=%d: Drilldown expected %d series with Loki's warning, got %d %s", limit, limit, rec.Code, rec.Body.String())
		}
		var values []string
		for _, series := range answer.Data.Result {
			values = append(values, series.Metric[label])
		}
		sort.Strings(values)
		if fmt.Sprint(values) != kept {
			t.Fatalf("Drilldown must keep the busiest series %s, kept %v", kept, values)
		}
	}
}

// The sliding window evaluator (grouped range != step over more buckets than
// Loki's 11,000-point limit) answers the series limit like every other route.
//
// conformance: series-limits-and-partial-results, limits/series-limit-error, limits/drilldown-partial-with-warning
// conformance: loki_api_v1_query_range
func TestSeriesLimit_SlidingWindowStatsFollowsLoki(t *testing.T) {
	end := time.Date(2026, 9, 15, 9, 0, 0, 0, time.UTC)
	rng := 24 * time.Hour
	step := rng / 1000
	start := end.Add(-rng)
	var rows []emulatedRow
	for _, at := range []time.Time{start.Add(time.Hour), end.Add(-time.Hour)} {
		for i := 0; i < 6; i++ {
			for k := 0; k <= i; k++ {
				rows = append(rows, emulatedRow{ts: at.Add(time.Duration(k) * time.Second).UnixNano(), pod: fmt.Sprintf("p%d", i)})
			}
		}
	}
	params := url.Values{
		"query": {`sum by (pod) (count_over_time({app="api"}[5m]))`},
		"start": {strconv.FormatInt(start.UnixNano(), 10)},
		"end":   {strconv.FormatInt(end.UnixNano(), 10)},
		"step":  {strconv.FormatFloat(step.Seconds(), 'f', -1, 64)},
	}
	for _, limit := range []int{3, 6} {
		for _, drilldown := range []bool{false, true} {
			emu := &vlStatsRangeEmulator{t: t, rows: rows}
			srv := httptest.NewServer(emu)
			p := newSlidingTestProxy(t, srv.URL)
			p.maxStatsQuerySeries = limit
			header := http.Header{}
			if drilldown {
				header.Set("X-Query-Tags", "Source=grafana-lokiexplore-app")
			}
			rec := serveTumblingQuery(p, "/loki/api/v1/query_range", params, header)
			srv.Close()
			emu.mu.Lock()
			stats, raw := len(emu.calls), emu.rawCalls
			emu.mu.Unlock()
			if stats == 0 || raw != 0 {
				t.Fatalf("limit=%d drilldown=%v: expected the sliding window stats route, got %d stats pipes and %d raw fetches", limit, drilldown, stats, raw)
			}
			assertSeriesLimitAnswer(t, rec, limit, 6, drilldown, "pod", "[p3 p4 p5]")
		}
	}
}

// A binary operand cut by the series limit reaches the combined answer: a
// Drilldown request gets Loki's warning on it, and a union (the one operation
// that can hold more series than its operands) is held to the limit too.
//
// conformance: series-limits-and-partial-results, limits/series-limit-error, limits/drilldown-partial-with-warning
// conformance: loki_api_v1_query_range
func TestSeriesLimit_BinaryUnionFollowsLoki(t *testing.T) {
	base := time.Unix(1700000400, 0).UTC()
	lines := seriesLimitFixture(6, base.Add(6*time.Minute), base.Add(8*time.Minute), base.Add(9*time.Minute))
	// Six pod series on the left, one app series on the right: seven in all.
	query := `sum by (pod) (count_over_time({app="top"}[5m])) or sum by (app) (count_over_time({app="top"}[5m]))`
	params := tumblingRangeParams(query, base.Add(5*time.Minute), base.Add(10*time.Minute), time.Minute)
	for _, limit := range []int{6, 7} {
		for _, drilldown := range []bool{false, true} {
			fake := &seriesLimitFakeVL{t: t, lines: lines}
			srv := httptest.NewServer(fake)
			p := newSlidingTestProxy(t, srv.URL)
			p.maxStatsQuerySeries = limit
			header := http.Header{}
			if drilldown {
				header.Set("X-Query-Tags", "Source=grafana-lokiexplore-app")
			}
			rec := serveTumblingQuery(p, "/loki/api/v1/query_range", params, header)
			srv.Close()
			// The app series holds every line, so it is the busiest; p0 is the quietest.
			assertSeriesLimitAnswer(t, rec, limit, 7, drilldown, "pod", "[ p1 p2 p3 p4 p5]")
		}
	}
}

// A binary operand answered from the proxy's cache still carries its Drilldown
// cut to the combined answer: the cached operand holds Loki's warning, and the
// expression built from it keeps it.
//
// conformance: series-limits-and-partial-results, limits/drilldown-partial-with-warning
// conformance: loki_api_v1_query_range
func TestSeriesLimit_BinaryOperandFromCacheKeepsWarning(t *testing.T) {
	base := time.Unix(1700000400, 0).UTC()
	fake := &seriesLimitFakeVL{t: t, lines: seriesLimitFixture(6, base.Add(6*time.Minute), base.Add(8*time.Minute), base.Add(9*time.Minute))}
	srv := httptest.NewServer(fake)
	defer srv.Close()
	p := newSlidingTestProxy(t, srv.URL)
	p.maxStatsQuerySeries = 3
	header := http.Header{"X-Query-Tags": {"Source=grafana-lokiexplore-app"}}
	start, end := base.Add(5*time.Minute), base.Add(10*time.Minute)
	operand := `sum by (pod) (count_over_time({app="top"}[5m]))`
	rec := serveTumblingQuery(p, "/loki/api/v1/query_range", tumblingRangeParams(operand, start, end, time.Minute), header)
	assertSeriesLimitAnswer(t, rec, 3, 6, true, "pod", "[p3 p4 p5]")
	before := fake.snapshot()["stats_query_range"]
	rec = serveTumblingQuery(p, "/loki/api/v1/query_range", tumblingRangeParams(operand+" * 2", start, end, time.Minute), header)
	if after := fake.snapshot()["stats_query_range"]; after != before {
		t.Fatalf("the operand was not answered from the cache (%d VictoriaLogs calls)", after-before)
	}
	assertSeriesLimitAnswer(t, rec, 3, 6, true, "pod", "[p3 p4 p5]")
}

// A multi-tenant metric query is limited on the merged answer, as Loki's
// engine limits the one result it builds across tenants: two tenants within
// the limit can still exceed it together, and a tenant's own Drilldown cut
// keeps its warning through the merge.
//
// conformance: series-limits-and-partial-results, limits/series-limit-error, limits/drilldown-partial-with-warning
// conformance: loki_api_v1_query_range
func TestSeriesLimit_MultiTenantMergeFollowsLoki(t *testing.T) {
	base := time.Unix(1700000400, 0).UTC()
	lines := seriesLimitFixture(6, base.Add(6*time.Minute), base.Add(8*time.Minute), base.Add(9*time.Minute))
	params := tumblingRangeParams(`sum by (pod) (count_over_time({app="top"}[5m]))`, base.Add(5*time.Minute), base.Add(10*time.Minute), time.Minute)
	// Every tenant holds the same six pods: twelve tenant series in all.
	for _, tc := range []struct {
		limit int
		kept  string
	}{{12, ""}, {6, "[p3 p3 p4 p4 p5 p5]"}, {3, "[p4 p5 p5]"}} {
		for _, drilldown := range []bool{false, true} {
			srv := httptest.NewServer(&seriesLimitFakeVL{t: t, lines: lines})
			p, _, mux := newTwoTenantProxy(t, srv.URL)
			p.storeBackendVersion("v1.50.0", "v1.50.0")
			p.maxStatsQuerySeries = tc.limit
			req := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+params.Encode(), nil)
			req.Header.Set("X-Scope-OrgID", "tenant-a|tenant-b")
			if drilldown {
				req.Header.Set("X-Query-Tags", "Source=grafana-lokiexplore-app")
			}
			rec := httptest.NewRecorder()
			mux.ServeHTTP(rec, req)
			srv.Close()
			assertSeriesLimitAnswer(t, rec, tc.limit, 12, drilldown, "pod", tc.kept)
		}
	}
}
