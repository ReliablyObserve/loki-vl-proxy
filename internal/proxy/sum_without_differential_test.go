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
	"slices"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// This file pins `<op> without (pod) (<range function>)` to Loki's answer over
// generated rows and windows: Loki v3.7.7 pkg/logql/evaluator.go
// VectorAggEvaluator.Next (:451-466) groups the series of its input by every
// label but the excluded ones (HashWithoutLabels, lb.Del) and folds each group
// with the operator, so the answer keeps a series per remaining label set. The
// range function is taken per series (the stream plus the labels a parser
// extracted, less the unwrapped one) over (t-range, t].
//
// The proxy keeps the stream labels of a series, not the labels a parser
// extracted (semantics/unwrap-per-series-identity-raw-rows), so Loki's answer
// is compared after projecting it onto the stream labels with the same
// operator, which sum, max and min allow. Every line carries a unique parsed
// label in the parser cases: each line is a series of its own, which the stats
// route reads from `_msg`. count and avg take one value per series, which the
// proxy keeps per stream: they are pinned without a parser.

// lokiWithout is the reference: timestamp -> stream labels without pod ->
// Loki's value projected onto them.
func lokiWithout(rows []emuRow, fn, op string, window int64, times []int64) map[int64]map[string]float64 {
	return lokiWithoutIdentity(rows, fn, op, window, times, func(row emuRow) string {
		parsed := make([]string, 0, len(row.labels))
		for k, v := range row.labels {
			parsed = append(parsed, k+"="+v)
		}
		sort.Strings(parsed)
		return row.stream + "|" + strings.Join(parsed, ",")
	}, func(row emuRow) string { return withoutPodKey(row.stream) })
}

// lokiWithoutIdentity is lokiWithout for a series identity and a result group
// the caller names for a row.
func lokiWithoutIdentity(rows []emuRow, fn, op string, window int64, times []int64, identity, group func(emuRow) string) map[int64]map[string]float64 {
	out := map[int64]map[string]float64{}
	for _, t := range times {
		type series struct {
			group  string
			values []float64
		}
		perSeries := map[string]*series{}
		for _, row := range rows {
			if row.ts <= t-window || row.ts > t {
				continue
			}
			key := identity(row)
			if perSeries[key] == nil {
				perSeries[key] = &series{group: group(row)}
			}
			perSeries[key].values = append(perSeries[key].values, row.value)
		}
		if len(perSeries) == 0 {
			continue
		}
		members := map[string][]float64{}
		for _, s := range perSeries {
			var v float64
			switch fn {
			case "rate":
				for _, x := range s.values {
					v += x
				}
				v /= float64(window) / 1e9
			case "count_rate": // rate without unwrap: lines per second
				v = float64(len(s.values)) / (float64(window) / 1e9)
			case "sum_over_time":
				for _, x := range s.values {
					v += x
				}
			case "avg_over_time":
				for _, x := range s.values {
					v += x
				}
				v /= float64(len(s.values))
			case "max_over_time":
				v = slices.Max(s.values)
			case "min_over_time":
				v = slices.Min(s.values)
			default:
				panic("reference: " + fn)
			}
			members[s.group] = append(members[s.group], v)
		}
		groups := map[string]float64{}
		for group, values := range members {
			var sum float64
			for _, v := range values {
				sum += v
			}
			switch op {
			case "sum":
				groups[group] = sum
			case "max":
				groups[group] = slices.Max(values)
			case "min":
				groups[group] = slices.Min(values)
			case "count":
				groups[group] = float64(len(values))
			case "avg":
				groups[group] = sum / float64(len(values))
			default:
				panic("reference: " + op)
			}
		}
		out[t] = groups
	}
	return out
}

// withoutPodKey is the label set of a stream selector without pod, as a key.
func withoutPodKey(stream string) string {
	var parts []string
	for _, m := range emuStreamLabelRE.FindAllStringSubmatch(stream, -1) {
		if m[1] != "pod" {
			parts = append(parts, m[1]+"="+m[2])
		}
	}
	sort.Strings(parts)
	return strings.Join(parts, ",")
}

type withoutScenario struct {
	uwScenario
	op string
}

func (s withoutScenario) query() string {
	pipeline := ""
	if s.parser != "" {
		pipeline = " | " + s.parser
	}
	fn := s.fn
	if fn == "count_rate" {
		return fmt.Sprintf(`%s without (pod) (rate({app="uw"}%s [%s]))`, s.op, pipeline, uwWindowText(s.window))
	}
	return fmt.Sprintf(`%s without (pod) (%s({app="uw"}%s | unwrap amount [%s]))`, s.op, fn, pipeline, uwWindowText(s.window))
}

func generateWithoutScenario(rnd *rand.Rand, fn, op, parser string, instant bool) withoutScenario {
	s := withoutScenario{uwScenario: generateUnwrapScenario(rnd, fn, parser, instant), op: op}
	// Two labels survive `without (pod)` (app, ns), and two pods share each.
	streams := []string{`{app="uw",ns="x",pod="a"}`, `{app="uw",ns="x",pod="b"}`, `{app="uw",ns="y",pod="a"}`, `{app="uw",ns="y",pod="c"}`}
	for j := range s.rows {
		s.rows[j].stream = streams[rnd.Intn(len(streams))]
		s.rows[j].labels = nil
		s.rows[j].msg = ""
		if parser != "" {
			s.rows[j].labels = map[string]string{"id": strconv.Itoa(j)}
			s.rows[j].msg = fmt.Sprintf("amount=%v id=%d", s.rows[j].value, j)
		}
	}
	return s
}

// runWithoutScenario sends the scenario through the real handler against the
// emulated VictoriaLogs and returns timestamp -> stream labels without pod ->
// value, failing on a series that keeps pod or two series with the same stream
// labels.
func runWithoutScenario(t *testing.T, emu *vlEmulator, s withoutScenario) map[int64]map[string]float64 {
	return runWithoutScenarioQ(t, emu, s, s.query())
}

func runWithoutScenarioQ(t *testing.T, emu *vlEmulator, s withoutScenario, query string) map[int64]map[string]float64 {
	t.Helper()
	backend := httptest.NewServer(emu.handler())
	defer backend.Close()
	p, err := New(Config{BackendURL: backend.URL, Cache: cache.New(60*time.Second, 1000), LogLevel: "error", BackendMinVersion: "v1.40.0", BackendVersionCheckTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if emu.version != "" {
		if err := p.ValidateBackendVersionCompatibility(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	params := url.Values{"query": {query}}
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
		t.Fatalf("%s: %d %s", query, res.Code, res.Body)
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
	got := map[int64]map[string]float64{}
	seen := map[string]bool{}
	for _, series := range resp.Data.Result {
		if _, ok := series.Metric["pod"]; ok {
			t.Fatalf("%s: a series keeps the excluded label: %v", query, series.Metric)
		}
		var parts []string
		for _, k := range []string{"app", "ns", "dst", "route"} {
			if v, ok := series.Metric[k]; ok {
				parts = append(parts, k+"="+v)
			}
		}
		key := strings.Join(parts, ",")
		if seen[key] {
			t.Fatalf("%s: two series for the labels %s:\n%s", query, key, res.Body)
		}
		seen[key] = true
		points := series.Values
		if series.Value != nil {
			points = [][]any{series.Value}
		}
		for _, point := range points {
			v, _ := strconv.ParseFloat(point[1].(string), 64)
			ts := int64(point[0].(float64)) * 1e9
			if got[ts] == nil {
				got[ts] = map[string]float64{}
			}
			got[ts][key] = v
		}
	}
	return got
}

// withoutCompare is how much of Loki's answer a scenario pins.
type withoutCompare int

const (
	compareValues withoutCompare = iota // the series and their values
	compareSeries                       // the series
	compareSubset                       // no series Loki does not have
)

// compareWithout compares the proxy's answer with Loki's, as much as mode says.
func compareWithout(t *testing.T, label string, s withoutScenario, got, want map[int64]map[string]float64, mode withoutCompare) {
	t.Helper()
	keys := func(m map[int64]map[string]float64) []string {
		set := map[string]bool{}
		for _, groups := range m {
			for k := range groups {
				set[k] = true
			}
		}
		out := make([]string, 0, len(set))
		for k := range set {
			out = append(out, k)
		}
		sort.Strings(out)
		return out
	}
	g, w := keys(got), keys(want)
	if mode == compareSubset {
		for _, key := range g {
			if !slices.Contains(w, key) {
				t.Fatalf("%s: series %v, Loki has %v\nquery %s step=%v window=%v rows=%d", label, g, w, s.query(), s.step, s.window, len(s.rows))
			}
		}
		return
	}
	if !slices.Equal(g, w) {
		t.Fatalf("%s: series %v, Loki has %v\nquery %s step=%v window=%v rows=%d", label, g, w, s.query(), s.step, s.window, len(s.rows))
	}
	if mode == compareSeries {
		return
	}
	for ts, groups := range want {
		for key, w := range groups {
			g, ok := got[ts][key]
			if !ok {
				t.Fatalf("%s: no point for {%s} at %d, Loki has %g\nquery %s step=%v window=%v", label, key, ts/1e9, w, s.query(), s.step, s.window)
			}
			if math.Abs(g-w) > 1e-9*math.Max(1, math.Abs(w)) {
				t.Fatalf("%s: {%s} at %d got %g, Loki has %g\nquery %s step=%v window=%v", label, key, ts/1e9, g, w, s.query(), s.step, s.window)
			}
		}
	}
	for ts, groups := range got {
		for key, g := range groups {
			if _, ok := want[ts][key]; !ok {
				t.Fatalf("%s: extra point {%s} at %d = %g, Loki has none\nquery %s step=%v window=%v", label, key, ts/1e9, g, s.query(), s.step, s.window)
			}
		}
	}
}

// TestSumWithoutDifferential: `sum without (pod)`, and max and min, over a
// range function keep a series per label set left once pod is dropped, on
// query_range and on an instant query, with Loki's values, on every
// VictoriaLogs line the proxy supports. VictoriaLogs answers a row per stream
// (stats, no raw rows) and the proxy drops pod and folds the rows.
//
// On query_range, max, min and sum over time read a sliding window as the
// step-sized bucket (semantics/unwrap-over-time-range-windows), so a series can
// be missing at a step; there no series may be one Loki does not have. A rate
// on VictoriaLogs before v1.45 puts a line on a bucket edge one bucket late
// (versions/stats-bucket-label-v1.45); there the series are compared.
//
// conformance: semantics/unwrap-sum-without-collapses, metric-series-identity
func TestSumWithoutDifferential(t *testing.T) {
	type tc struct{ fn, op, parser string }
	var cases []tc
	for _, parser := range []string{"", "logfmt"} {
		for _, fn := range []string{"rate", "sum_over_time", "avg_over_time", "max_over_time", "min_over_time"} {
			cases = append(cases, tc{fn, "sum", parser})
		}
		cases = append(cases, tc{"count_rate", "sum", parser}, tc{"max_over_time", "max", parser}, tc{"min_over_time", "min", parser}, tc{"sum_over_time", "max", parser})
	}
	cases = append(cases, tc{"max_over_time", "count", ""}, tc{"sum_over_time", "avg", ""}, tc{"min_over_time", "avg", ""})
	for _, version := range []string{"v1.52.0", "v1.40.0"} {
		for _, c := range cases {
			for _, instant := range []bool{true, false} {
				name := fmt.Sprintf("%s/%s_without(%s)/parser=%s/instant=%v", version, c.op, c.fn, c.parser, instant)
				t.Run(name, func(t *testing.T) {
					rnd := rand.New(rand.NewSource(int64(len(name)) * 7907))
					for i := 0; i < 12; i++ {
						s := generateWithoutScenario(rnd, c.fn, c.op, c.parser, instant)
						emu := &vlEmulator{t: t, rows: s.rows, field: "amount", version: version}
						got := runWithoutScenario(t, emu, s)
						want := lokiWithout(s.rows, c.fn, c.op, int64(s.window), s.times())
						mode := compareValues
						switch {
						case instant:
						case c.fn != "rate" && c.fn != "count_rate":
							mode = compareSubset
						case version == "v1.40.0":
							mode = compareSeries
						}
						compareWithout(t, fmt.Sprintf("%s #%d", name, i), s, got, want, mode)
						if rawRows := emu.endpointCalls("/select/logsql/query"); len(rawRows) > 0 && version != "v1.40.0" {
							t.Fatalf("%s #%d: the metric read raw rows: %s", name, i, rawRows[0])
						}
					}
				})
			}
		}
	}
}

// TestSumWithoutSeriesLimitOnFoldedAnswer: Loki counts max_query_series on the
// answer of the vector aggregation (pkg/logql/engine.go:536-548 and :575-590
// JoinSampleVector), so four streams folded into two series by `without (pod)`
// fit a limit of two, and a limit of one fails with Loki's 400 for a plain
// client and keeps one series with Loki's warning for Logs Drilldown. The
// proxy reads a row per stream from VictoriaLogs and applies the limit after
// the fold, on query_range and at an instant, on the stats routes (no parser,
// logfmt) and the raw route (json). `sum by` keeps its limit as before.
//
// conformance: semantics/unwrap-sum-without-collapses, metric-series-identity, limits/series-limit-error, series-limits-and-partial-results, limits/drilldown-partial-with-warning
func TestSumWithoutSeriesLimitOnFoldedAnswer(t *testing.T) {
	base := int64(1_800_000_000) * 1e9
	var rows []emuRow
	for i := 0; i < 40; i++ {
		stream := fmt.Sprintf(`{app="uw",ns=%q,pod=%q}`, []string{"x", "y"}[(i/4)%2], []string{"a", "b", "c", "d"}[i%4])
		rows = append(rows, emuRow{ts: base - int64(i)*5e9 - 3e8, stream: stream, value: float64(i + 1), msg: fmt.Sprintf(`{"amount":%d}`, i+1)})
	}
	logfmtRows := make([]emuRow, len(rows))
	for i, row := range rows {
		row.msg = fmt.Sprintf("amount=%v", row.value)
		logfmtRows[i] = row
	}
	run := func(t *testing.T, rows []emuRow, query string, instant bool, seriesCap int, drilldown bool, orgID ...string) (int, string, int) {
		t.Helper()
		emu := &vlEmulator{t: t, rows: rows, field: "amount", version: "v1.52.0"}
		backend := httptest.NewServer(emu.handler())
		defer backend.Close()
		tenants := map[string]TenantMapping{"t1": {AccountID: "1", ProjectID: "0"}, "t2": {AccountID: "2", ProjectID: "0"}}
		p, err := New(Config{BackendURL: backend.URL, Cache: cache.New(60*time.Second, 1000), LogLevel: "error", BackendMinVersion: "v1.40.0", BackendVersionCheckTimeout: time.Second, MaxStatsQuerySeries: seriesCap, TenantMap: tenants})
		if err != nil {
			t.Fatal(err)
		}
		if err := p.ValidateBackendVersionCompatibility(context.Background()); err != nil {
			t.Fatal(err)
		}
		params := url.Values{"query": {query}}
		target := "/loki/api/v1/query_range?"
		if instant {
			target = "/loki/api/v1/query?"
			params.Set("time", strconv.FormatInt(base/1e9, 10))
		} else {
			params.Set("start", strconv.FormatInt(base/1e9-120, 10))
			params.Set("end", strconv.FormatInt(base/1e9, 10))
			params.Set("step", "60")
		}
		headers := map[string]string{"X-Scope-OrgID": "0"}
		if len(orgID) > 0 {
			headers["X-Scope-OrgID"] = orgID[0]
		}
		if drilldown {
			headers["X-Query-Tags"] = "Source=grafana-lokiexplore-app"
		}
		res := doCompatProxyRequest(p, target+params.Encode(), headers)
		var resp struct {
			Data struct {
				Result []struct {
					Metric map[string]string `json:"metric"`
				} `json:"result"`
			} `json:"data"`
		}
		_ = json.Unmarshal(res.Body.Bytes(), &resp)
		for _, s := range resp.Data.Result {
			if _, ok := s.Metric["pod"]; ok {
				t.Fatalf("%s: a series keeps pod: %v", query, s.Metric)
			}
		}
		return res.Code, res.Body.String(), len(resp.Data.Result)
	}
	const limitMsg = "maximum number of series (1) reached for a single query"
	const warning = "maximum number of series (1) reached for a single query; returning partial results"
	for _, tc := range []struct {
		query string
		rows  []emuRow
	}{
		{`sum without (pod) (max_over_time({app="uw"} | unwrap amount [5m]))`, logfmtRows},
		{`sum without (pod) (max_over_time({app="uw"} | logfmt | unwrap amount [5m]))`, logfmtRows},
		{`sum without (pod) (rate({app="uw"} | logfmt | unwrap amount [5m]))`, logfmtRows},
		{`sum without (pod) (rate({app="uw"} | json | unwrap amount [5m]))`, rows},
		{`sum without (pod) (rate({app="uw"}[5m]))`, logfmtRows},
		{`sum without (pod) (count_over_time({app="uw"}[1m]))`, logfmtRows},
		{`max without (pod) (max_over_time({app="uw"} | unwrap amount [5m]))`, logfmtRows},
		{`topk(5, sum without (pod) (rate({app="uw"}[5m])))`, logfmtRows},
	} {
		for _, instant := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/instant=%v", tc.query, instant), func(t *testing.T) {
				if code, body, n := run(t, tc.rows, tc.query, instant, 2, false); code != http.StatusOK || n != 2 {
					t.Fatalf("limit 2: %d with %d series, want 200 with Loki's 2: %s", code, n, body)
				}
				if code, body, _ := run(t, tc.rows, tc.query, instant, 1, false); code != http.StatusBadRequest || !strings.Contains(body, limitMsg) {
					t.Fatalf("limit 1: %d %s, want Loki's 400 series limit error", code, body)
				}
				code, body, n := run(t, tc.rows, tc.query, instant, 1, true)
				if code != http.StatusOK || n != 1 || !strings.Contains(body, warning) {
					t.Fatalf("limit 1, Logs Drilldown: %d with %d series, want 200 with one series and Loki's warning: %s", code, n, body)
				}
				// The series kept is the busiest of the folded answer, not the
				// busiest stream's: a limit applied before the fold would keep a
				// stream and fold it alone.
				_, full, _ := run(t, tc.rows, tc.query, instant, 1000, false)
				all, kept := seriesTotalsByNS(t, full), seriesTotalsByNS(t, body)
				busiest, busiestTotal, tied := "", math.Inf(-1), false
				for key, total := range all {
					if total > busiestTotal {
						busiest, busiestTotal, tied = key, total, false
					} else if total == busiestTotal {
						tied = true
					}
				}
				if got, ok := kept[busiest]; !tied && (!ok || math.Abs(got-busiestTotal) > 1e-9*math.Max(1, busiestTotal)) {
					t.Fatalf("limit 1, Logs Drilldown kept %v, want the busiest folded series {ns=%s} = %g of %v: %s", kept, busiest, busiestTotal, all, body)
				}
			})
		}
	}
	for _, instant := range []bool{false, true} {
		// Control: a grouping VictoriaLogs answers itself keeps its limit, and
		// a failing instant label_replace keeps its status.
		for _, query := range []string{
			`sum by (ns) (max_over_time({app="uw"} | unwrap amount [5m]))`,
			`label_replace(sum by (ns) (max_over_time({app="uw"} | unwrap amount [5m])), "dst", "$1", "ns", "(.*)")`,
		} {
			if code, _, n := run(t, logfmtRows, query, instant, 2, false); code != http.StatusOK || n != 2 {
				t.Fatalf("%s instant=%v limit 2: %d with %d series", query, instant, code, n)
			}
			if code, body, _ := run(t, logfmtRows, query, instant, 1, false); code != http.StatusBadRequest || !strings.Contains(body, limitMsg) {
				t.Fatalf("%s instant=%v limit 1: %d %s", query, instant, code, body)
			}
		}
		// Two tenants: Loki keeps __tenant_id__ on every series
		// (pkg/querier/multi_tenant_querier.go), so the answer is two series per
		// tenant and the limit counts the merged answer.
		query := `sum without (pod) (max_over_time({app="uw"} | unwrap amount [5m]))`
		code, body, n := run(t, logfmtRows, query, instant, 4, false, "t1|t2")
		if code != http.StatusOK || n != 4 || strings.Count(body, `"__tenant_id__"`) != 4 {
			t.Fatalf("%s instant=%v two tenants, limit 4: %d with %d series, want 200 with 4 tenant series: %s", query, instant, code, n, body)
		}
		if code, body, _ := run(t, logfmtRows, query, instant, 3, false, "t1|t2"); code != http.StatusBadRequest || !strings.Contains(body, "maximum number of series (3)") {
			t.Fatalf("%s instant=%v two tenants, limit 3: %d %s", query, instant, code, body)
		}
	}
}

// TestInstantAggregationFailureIsRedacted: an instant query that is rewritten
// after VictoriaLogs answers (without, group, label_replace, label_join) and
// whose backend call fails keeps the failure's status and answers it in Loki's
// error format, with the backend's echo of the query removed, as query_range does.
//
// conformance: semantics/unwrap-sum-without-collapses, loki_api_v1_query
func TestInstantAggregationFailureIsRedacted(t *testing.T) {
	const sel = `{app="billing"}`
	for _, backendStatus := range []int{http.StatusBadRequest, http.StatusInternalServerError, http.StatusServiceUnavailable} {
		for _, query := range []string{
			`sum without (pod) (count_over_time(` + sel + `[5m]))`,
			`sum without (pod) (max_over_time(` + sel + ` | logfmt | unwrap amount [5m]))`,
			`group(count_over_time(` + sel + `[5m]))`,
			`label_replace(sum by (app) (count_over_time(` + sel + `[5m])), "dst", "$1", "app", "(.*)")`,
		} {
			t.Run(fmt.Sprintf("%d/%s", backendStatus, query), func(t *testing.T) {
				vl := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					http.Error(w, vlParseErrorBody, backendStatus)
				}))
				defer vl.Close()
				p, err := New(Config{BackendURL: vl.URL, Cache: cache.New(60*time.Second, 1000), LogLevel: "error"})
				if err != nil {
					t.Fatal(err)
				}
				req := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query?"+url.Values{"query": {query}}.Encode(), nil)
				w := httptest.NewRecorder()
				p.handleQuery(w, req)
				if backendStatus == http.StatusBadRequest {
					assertLokiBadData(t, w)
					return
				}
				if w.Code < 400 {
					t.Fatalf("backend %d: status %d, want the failure kept: %s", backendStatus, w.Code, w.Body.String())
				}
				var body struct {
					Status string `json:"status"`
					Error  string `json:"error"`
				}
				if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil || body.Status != "error" || body.Error == "" {
					t.Fatalf("backend %d: not Loki's error format: %v %s", backendStatus, err, w.Body.String())
				}
				if strings.Contains(w.Body.String(), "billing-secret-value") {
					t.Fatalf("backend %d: the query echo reached the client: %s", backendStatus, w.Body.String())
				}
			})
		}
	}
}

// TestWithoutWrappedByAnOperator: the operator of a `<op> without (...)` decides
// how the series that share a label set are folded, whatever wraps it (the
// wrappers the proxy applies to the folded series: topk, sort, label_replace and
// a comparison). Every pod holds a different value, so a fold by sum instead of
// the operator is visible: a max without (pod) over pods valued 10 and 4 is 10,
// not 14. A label_replace is pinned by the label it adds, and a comparison by a
// threshold between the groups' values, so it drops some of them.
//
// conformance: semantics/unwrap-sum-without-collapses, metric-series-identity
func TestWithoutWrappedByAnOperator(t *testing.T) {
	type answer = map[int64]map[string]float64
	type wrapper struct {
		name    string
		wrap    func(inner string, threshold float64) string
		instant bool                                        // only an instant query
		anyKey  bool                                        // compared by value, not by label set
		filters bool                                        // the wrapper drops series: some must be dropped and some kept
		keep    func(want answer, threshold float64) answer // the wrapper applied to Loki's answer
		// keepInstant is keep for an instant query, when it differs.
		keepInstant func(want answer, threshold float64) answer
	}
	identity := func(w answer, _ float64) answer { return w }
	above := func(w answer, threshold float64) answer {
		out := answer{}
		for ts, groups := range w {
			for key, v := range groups {
				if v > threshold {
					if out[ts] == nil {
						out[ts] = map[string]float64{}
					}
					out[ts][key] = v
				}
			}
		}
		return out
	}
	text := func(v float64) string { return strconv.FormatFloat(v, 'f', -1, 64) }
	wrappers := []wrapper{
		{"topk(10)", func(q string, _ float64) string { return "topk(10, " + q + ")" }, false, false, false, identity, nil},
		// Ties between label sets pick either: only the winning value is pinned.
		{"topk(1)", func(q string, _ float64) string { return "topk(1, " + q + ")" }, false, true, false, func(w answer, _ float64) answer {
			out := answer{}
			for ts, groups := range w {
				best := math.Inf(-1)
				for _, v := range groups {
					best = math.Max(best, v)
				}
				out[ts] = map[string]float64{"*": best}
			}
			return out
		}, nil},
		{"sort", func(q string, _ float64) string { return "sort(" + q + ")" }, true, false, false, identity, nil},
		// The label the wrapper adds is part of the series: dst holds app.
		{"label_replace", func(q string, _ float64) string { return `label_replace(` + q + `, "dst", "$1", "app", "(.*)")` }, false, false, false, func(w answer, _ float64) answer {
			out := answer{}
			for ts, groups := range w {
				out[ts] = map[string]float64{}
				for key, v := range groups {
					out[ts][key+",dst=uw"] = v
				}
			}
			return out
		}, identity /* an instant label_replace on a vector adds no label (open row semantics/label-replace-instant-vector) */},
		// On query_range a topk over a binary expression is answered 502 with or
		// without `without` (the binary marker reaches VictoriaLogs): instant only.
		{"topk(10) of a comparison", func(q string, threshold float64) string { return "topk(10, " + q + " > " + text(threshold) + ")" }, true, false, true, above, nil},
		{"comparison", func(q string, threshold float64) string { return q + " > " + text(threshold) }, false, false, true, above, nil},
	}
	type inner struct{ fn, op string }
	inners := []inner{{"rate", "max"}, {"rate", "min"}, {"max_over_time", "max"}, {"min_over_time", "min"}, {"max_over_time", "count"}}
	count := func(w answer) (n int) {
		for _, groups := range w {
			n += len(groups)
		}
		return n
	}
	for _, version := range []string{"v1.52.0", "v1.40.0"} {
		for _, w := range wrappers {
			for _, in := range inners {
				for _, instant := range []bool{true, false} {
					if w.instant && !instant {
						continue
					}
					name := fmt.Sprintf("%s/%s/%s_without(%s)/instant=%v", version, w.name, in.op, in.fn, instant)
					t.Run(name, func(t *testing.T) {
						rnd := rand.New(rand.NewSource(int64(len(name)) * 6101))
						dropped, kept, varied := 0, 0, false
						for i := 0; i < 8; i++ {
							s := generateWithoutScenario(rnd, in.fn, in.op, "", instant)
							// A value per pod that differs from every other, so the operators differ.
							for j := range s.rows {
								s.rows[j].value = float64(1+j%7) * 1.5
							}
							ref := lokiWithout(s.rows, in.fn, in.op, int64(s.window), s.times())
							// A threshold between the groups' values: the lowest drops.
							lo, hi := math.Inf(1), math.Inf(-1)
							for _, groups := range ref {
								for _, v := range groups {
									lo, hi = math.Min(lo, v), math.Max(hi, v)
								}
							}
							varied = varied || lo < hi
							threshold := math.Round((lo+hi)/2*1e6) / 1e6
							if !instant && in.fn != "rate" {
								// The over-time functions of a query_range read a step-sized bucket for
								// the window (semantics/unwrap-over-time-range-windows): their values
								// are not Loki's, so a threshold between them is not pinned here.
								threshold = lo - 1
							}
							query := w.wrap(s.query(), threshold)
							emu := &vlEmulator{t: t, rows: s.rows, field: "amount", version: version}
							got := runWithoutScenarioQ(t, emu, s, query)
							keep := w.keep
							if instant && w.keepInstant != nil {
								keep = w.keepInstant
							}
							want := keep(ref, threshold)
							if instant || in.fn == "rate" {
								dropped += count(ref) - count(want)
								kept += count(want)
							}
							if w.anyKey {
								rekey := func(m answer) answer {
									out := answer{}
									for ts, groups := range m {
										for _, v := range groups {
											if out[ts] == nil {
												out[ts] = map[string]float64{}
											}
											out[ts]["*"] = v
										}
									}
									return out
								}
								got = rekey(got)
							}
							mode := compareValues
							switch {
							case instant:
							case in.fn != "rate":
								mode = compareSubset
							case version == "v1.40.0":
								mode = compareSeries
							}
							compareWithout(t, fmt.Sprintf("%s #%d: %s", name, i, query), s, got, want, mode)
						}
						if w.filters && (instant || in.fn == "rate") && varied && (dropped == 0 || kept == 0) {
							t.Fatalf("%s: the comparison dropped %d and kept %d points over the scenarios, it must do both", name, dropped, kept)
						}
					})
				}
			}
		}
	}
}

// TestWithoutOffset: `<op> without (pod) (<range function> [w] offset o)` is the
// fold of the window that ends o earlier, at the evaluation time: the points
// are Loki's values at t-o, labelled t.
//
// conformance: semantics/unwrap-sum-without-collapses, metric-series-identity
func TestWithoutOffset(t *testing.T) {
	type tc struct{ fn, op string }
	cases := []tc{{"rate", "sum"}, {"max_over_time", "max"}, {"sum_over_time", "sum"}, {"count_rate", "sum"}}
	for _, version := range []string{"v1.52.0", "v1.40.0"} {
		for _, c := range cases {
			for _, instant := range []bool{true, false} {
				name := fmt.Sprintf("%s/%s_without(%s) offset/instant=%v", version, c.op, c.fn, instant)
				t.Run(name, func(t *testing.T) {
					rnd := rand.New(rand.NewSource(int64(len(name)) * 3301))
					for i := 0; i < 8; i++ {
						const offset = 5 * time.Minute
						s := generateWithoutScenario(rnd, c.fn, c.op, "", instant)
						for j := range s.rows {
							s.rows[j].value = float64(1+j%7) * 1.5
						}
						// Loki's window at t is (t-o-w, t-o]: the same rows, o earlier, are
						// what an evaluation without an offset reads at t.
						unshifted := slices.Clone(s.rows)
						for j := range s.rows {
							s.rows[j].ts -= int64(offset)
						}
						window := uwWindowText(s.window)
						query := fmt.Sprintf(`%s without (pod) (%s({app="uw"} | unwrap amount [%s] offset %s))`, c.op, c.fn, window, uwWindowText(offset))
						if c.fn == "count_rate" {
							query = fmt.Sprintf(`%s without (pod) (rate({app="uw"} [%s] offset %s))`, c.op, window, uwWindowText(offset))
						}
						emu := &vlEmulator{t: t, rows: s.rows, field: "amount", version: version}
						got := runWithoutScenarioQ(t, emu, s, query)
						// Loki's window at t-o holds the rows the rows' shift put there.
						want := lokiWithout(unshifted, c.fn, c.op, int64(s.window), s.times())
						mode := compareValues
						switch {
						case instant:
						case c.fn != "rate" && c.fn != "count_rate":
							mode = compareSubset
						case version == "v1.40.0":
							mode = compareSeries
						}
						compareWithout(t, fmt.Sprintf("%s #%d: %s", name, i, query), s, got, want, mode)
					}
				})
			}
		}
	}
}

// TestWithoutBinaryOperands: the operands of a binary expression fold by their
// own `without` and the expression joins them on the labels that remain, so
// `sum without (pod) (rate(a)) / sum without (pod) (rate(b))` is a per-label-set
// ratio, not the ratio of the totals. A count without folds once, not again over
// the combined answer.
//
// conformance: semantics/unwrap-sum-without-collapses, metric-series-identity
func TestWithoutBinaryOperands(t *testing.T) {
	for _, version := range []string{"v1.52.0", "v1.40.0"} {
		for _, instant := range []bool{true, false} {
			name := fmt.Sprintf("%s/instant=%v", version, instant)
			t.Run(name, func(t *testing.T) {
				rnd := rand.New(rand.NewSource(int64(len(name)) * 4409))
				for i := 0; i < 8; i++ {
					s := generateWithoutScenario(rnd, "rate", "sum", "", instant)
					for j := range s.rows {
						s.rows[j].value = float64(1+j%7) * 1.5
					}
					if !instant {
						// A binary range query joins its operands on a step-aligned axis.
						s.start, s.end = s.start/int64(s.step)*int64(s.step), s.end/int64(s.step)*int64(s.step)
					}
					window := uwWindowText(s.window)
					query := fmt.Sprintf(`sum without (pod) (rate({app="uw"} | unwrap amount [%s])) / sum without (pod) (rate({app="uw"} [%s]))`, window, window)
					emu := &vlEmulator{t: t, rows: s.rows, field: "amount", version: version}
					got := runWithoutScenarioQ(t, emu, s, query)
					num := lokiWithout(s.rows, "rate", "sum", int64(s.window), s.times())
					den := lokiWithout(s.rows, "count_rate", "sum", int64(s.window), s.times())
					want := map[int64]map[string]float64{}
					for ts, groups := range num {
						for key, v := range groups {
							if d, ok := den[ts][key]; ok && d != 0 {
								if want[ts] == nil {
									want[ts] = map[string]float64{}
								}
								want[ts][key] = v / d
							}
						}
					}
					mode := compareValues
					if !instant && version == "v1.40.0" {
						mode = compareSeries
					}
					compareWithout(t, fmt.Sprintf("%s #%d: %s", name, i, query), s, got, want, mode)
				}
			})
		}
	}
}

// TestWithoutFoldRowBudgetIsNotASeriesLimit: while a `without` request reads a
// row per stream, the bound on those reads is the raw-row budget
// (-manual-range-metric-row-limit), here above the tenant's series limit of 2, not that limit: tripping it
// answers the row-limit error that names the flag, as a rejection (400), and
// never Loki's series message carrying the budget.
//
// conformance: semantics/unwrap-sum-without-collapses, limits/series-limit-error
func TestWithoutFoldRowBudgetIsNotASeriesLimit(t *testing.T) {
	base := int64(1_800_000_000) * 1e9
	var rows []emuRow
	for i := 0; i < 40; i++ {
		stream := fmt.Sprintf(`{app="uw",ns=%q,pod=%q}`, []string{"x", "y"}[(i/4)%2], []string{"a", "b", "c", "d"}[i%4])
		rows = append(rows, emuRow{ts: base - int64(i)*5e9 - 3e8, stream: stream, value: float64(i + 1), msg: fmt.Sprintf("amount=%d", i+1)})
	}
	for _, query := range []string{
		`sum without (pod) (max_over_time({app="uw"} | logfmt | unwrap amount [5m]))`,
		`sum without (pod) (rate({app="uw"} | logfmt | unwrap amount [5m]))`,
		`sum without (pod) (count_over_time({app="uw"}[1m]))`,
		// An ungrouped sum folded afterwards (#716) keeps the tenant's limit for
		// everything but the rows it reads: never the budget as a series limit.
		`sum(rate({app="uw"} | logfmt | unwrap amount [5m]))`,
	} {
		for _, instant := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/instant=%v", query, instant), func(t *testing.T) {
				emu := &vlEmulator{t: t, rows: rows, field: "amount", version: "v1.52.0"}
				backend := httptest.NewServer(emu.handler())
				defer backend.Close()
				p, err := New(Config{BackendURL: backend.URL, Cache: cache.New(60*time.Second, 1000), LogLevel: "error", BackendMinVersion: "v1.40.0", BackendVersionCheckTimeout: time.Second, MaxStatsQuerySeries: 2, RangeMetricRowLimit: 3})
				if err != nil {
					t.Fatal(err)
				}
				if err := p.ValidateBackendVersionCompatibility(context.Background()); err != nil {
					t.Fatal(err)
				}
				params := url.Values{"query": {query}}
				target := "/loki/api/v1/query_range?"
				if instant {
					target = "/loki/api/v1/query?"
					params.Set("time", strconv.FormatInt(base/1e9, 10))
				} else {
					params.Set("start", strconv.FormatInt(base/1e9-120, 10))
					params.Set("end", strconv.FormatInt(base/1e9, 10))
					params.Set("step", "60")
				}
				res := doCompatProxyRequest(p, target+params.Encode(), map[string]string{"X-Scope-OrgID": "0"})
				body := res.Body.String()
				if !strings.HasPrefix(query, "sum without") && res.Code == http.StatusOK {
					return // one series: it fits, whatever the rows
				}
				if res.Code != http.StatusBadRequest || strings.Contains(body, "maximum number of series") || !strings.Contains(body, "-manual-range-metric-row-limit") {
					t.Fatalf("%d %s, want the row-limit error naming the flag as a 400", res.Code, body)
				}
			})
		}
	}
}

// TestWithoutFoldRowBudgetError: the error a request that folds its series hits
// before the fold is told by the request, not by whether the budget and the
// tenant's limit happen to be equal: under the budget (budget at or above the
// tenant's limit) the row-limit message names the flag, as a rejection (400);
// when the tenant's limit is the lower bound it stays Loki's series message; and
// a check against the tenant's own limit is Loki's message even at the budget's
// value.
//
// conformance: semantics/unwrap-sum-without-collapses, limits/series-limit-error
func TestWithoutFoldRowBudgetError(t *testing.T) {
	base := int64(1_800_000_000) * 1e9
	var rows []emuRow
	for i := 0; i < 40; i++ {
		stream := fmt.Sprintf(`{app="uw",ns=%q,pod=%q}`, []string{"x", "y"}[(i/4)%2], []string{"a", "b", "c", "d"}[i%4])
		rows = append(rows, emuRow{ts: base - int64(i)*5e9 - 3e8, stream: stream, value: float64(i + 1), msg: fmt.Sprintf("amount=%d", i+1)})
	}
	const query = `sum without (pod) (max_over_time({app="uw"} | logfmt | unwrap amount [5m]))`
	for _, tc := range []struct {
		name        string
		series, row int
		wantRowText bool
	}{
		{"budget above the tenant's limit", 2, 3, true},
		{"equal limits", 3, 3, true},
		{"budget below the tenant's limit", 4, 3, false},
	} {
		for _, instant := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/instant=%v", tc.name, instant), func(t *testing.T) {
				emu := &vlEmulator{t: t, rows: rows, field: "amount", version: "v1.52.0"}
				backend := httptest.NewServer(emu.handler())
				defer backend.Close()
				p, err := New(Config{BackendURL: backend.URL, Cache: cache.New(60*time.Second, 1000), LogLevel: "error", BackendMinVersion: "v1.40.0", BackendVersionCheckTimeout: time.Second, MaxStatsQuerySeries: tc.series, RangeMetricRowLimit: tc.row})
				if err != nil {
					t.Fatal(err)
				}
				if err := p.ValidateBackendVersionCompatibility(context.Background()); err != nil {
					t.Fatal(err)
				}
				params := url.Values{"query": {query}}
				target := "/loki/api/v1/query_range?"
				if instant {
					target = "/loki/api/v1/query?"
					params.Set("time", strconv.FormatInt(base/1e9, 10))
				} else {
					params.Set("start", strconv.FormatInt(base/1e9-120, 10))
					params.Set("end", strconv.FormatInt(base/1e9, 10))
					params.Set("step", "60")
				}
				res := doCompatProxyRequest(p, target+params.Encode(), map[string]string{"X-Scope-OrgID": "0"})
				body := res.Body.String()
				if tc.wantRowText && (res.Code != http.StatusBadRequest || strings.Contains(body, "maximum number of series") || !strings.Contains(body, "-manual-range-metric-row-limit")) {
					t.Fatalf("%d %s, want the row-limit error naming the flag as a 400", res.Code, body)
				}
				if !tc.wantRowText && (res.Code != http.StatusBadRequest || !strings.Contains(body, "maximum number of series (4)") || strings.Contains(body, "-manual-range-metric-row-limit")) {
					t.Fatalf("%d %s, want Loki's series limit error of the tenant's limit as a 400", res.Code, body)
				}
			})
		}
	}
	// The error constructor: the scope tells whether the budget is the bound.
	folding := func(foldBudget int) context.Context {
		ctx := context.WithValue(context.Background(), foldedSumKey{}, true)
		return context.WithValue(ctx, seriesLimitScopeKey{}, &seriesLimitScope{foldBudget: foldBudget})
	}
	if err := newSeriesLimitError(folding(7), 7); !strings.Contains(err.Error(), "-manual-range-metric-row-limit") {
		t.Fatalf("a request reading under the budget: %v", err)
	}
	if err := newSeriesLimitError(folding(7), 3); !strings.Contains(err.Error(), "-manual-range-metric-row-limit") {
		t.Fatalf("a request reading under the budget, whatever the limit it reports: %v", err)
	}
	if err := newSeriesLimitError(folding(0), 7); strings.Contains(err.Error(), "-manual-range-metric-row-limit") || !strings.Contains(err.Error(), "maximum number of series (7)") {
		t.Fatalf("a request the tenant's limit bounds: %v", err)
	}
	if err := seriesLimitReached(tenantSeriesLimitContext(folding(7)), 7); strings.Contains(err.Error(), "-manual-range-metric-row-limit") || !strings.Contains(err.Error(), "maximum number of series (7)") {
		t.Fatalf("the tenant's own limit, equal to the budget: %v", err)
	}
}

// seriesTotalsByNS is the sum of the sample values of each series of a response,
// keyed by its ns label.
func seriesTotalsByNS(t *testing.T, body string) map[string]float64 {
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
	if err := json.Unmarshal([]byte(body), &resp); err != nil {
		t.Fatalf("%v: %s", err, body)
	}
	out := map[string]float64{}
	for _, series := range resp.Data.Result {
		points := series.Values
		if series.Value != nil {
			points = [][]any{series.Value}
		}
		for _, point := range points {
			v, _ := strconv.ParseFloat(point[1].(string), 64)
			out[series.Metric["ns"]] += v
		}
	}
	return out
}
