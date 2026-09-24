package proxy

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"strings"
	"testing"
	"time"
)

// Grafana Logs Drilldown label and field breakdowns send the requests below
// (captured from Grafana 12 with the Logs Drilldown plugin 2.x: the plugin
// neither caps nor samples the series it gets, it only sorts them). Loki
// answers them like any other metric query: every series, exact values, and
// above max_query_series a partial result with a warning for Drilldown and a
// 400 for every other client (pkg/logql/engine.go JoinSampleVector and
// pkg/querier/queryrange/limits.go seriesLimiter, v3.7.7).
var drilldownBreakdownShapes = []struct {
	name, query, field string
	parsed             bool
	busiest            []string // the field values by line count, busiest first
}{
	{"labels breakdown", `sum(count_over_time({app="tumble" ,pod != ""}        [5m])) by (pod)`, "pod", false, []string{"p0", "p1", "p2", "p3", "p4", "p5"}},
	{"fields breakdown", `sum by (user_id) (count_over_time({app="tumble"}      | logfmt | drop __error__, __error_details__ | user_id!=""  [5m]))`, "user_id", true, []string{"u-a", "u-b", "u-c", "u-d"}},
}

// drilldownBreakdownFixture writes six pods at different rates; every line
// carries a logfmt user_id, 40/30/20/10% of the lines u-a/u-b/u-c/u-d.
func drilldownBreakdownFixture(base time.Time) []tumblingLine {
	users := []string{"u-a", "u-a", "u-a", "u-a", "u-b", "u-b", "u-b", "u-c", "u-c", "u-d"}
	var lines []tumblingLine
	for pod, every := range []time.Duration{3 * time.Second, 5 * time.Second, 7 * time.Second, 11 * time.Second, 13 * time.Second, 17 * time.Second} {
		labels := map[string]string{"app": "tumble", "pod": fmt.Sprintf("p%d", pod)}
		for i := 0; i < int(25*time.Minute/every); i++ {
			lines = append(lines, tumblingLine{ts: base.Add(time.Duration(i) * every).UnixNano(), labels: labels, msg: fmt.Sprintf("level=info user_id=%s", users[(i+pod)%len(users)])})
		}
	}
	sort.Slice(lines, func(i, j int) bool { return lines[i].ts < lines[j].ts })
	return lines
}

// drilldownBreakdownStack is a proxy with tenant-a and tenant-b in front of a
// fake VictoriaLogs holding lines.
type drilldownBreakdownStack struct {
	fake *tumblingFakeVL
	mux  *http.ServeMux
}

func newDrilldownBreakdownStack(t *testing.T, cfg Config, lines []tumblingLine) *drilldownBreakdownStack {
	t.Helper()
	srv, fake := newTumblingFakeVL(t, lines)
	_, mux := newTenantLimitsProxy(t, srv.URL, cfg)
	return &drilldownBreakdownStack{fake: fake, mux: mux}
}

// run serves one breakdown request for a tenant, from Logs Drilldown or from a
// plain client, and returns the response with the stats calls it sent.
func (s *drilldownBreakdownStack) run(params url.Values, tenant string, drilldown bool) (int, string, []slidingStatsCall) {
	s.fake.mu.Lock()
	before := len(s.fake.statsCalls)
	s.fake.mu.Unlock()
	req := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+params.Encode(), nil)
	req.Header.Set("X-Scope-OrgID", tenant)
	if drilldown {
		req.Header.Set("X-Query-Tags", "Source=grafana-lokiexplore-app")
	}
	rec := httptest.NewRecorder()
	s.mux.ServeHTTP(rec, req)
	s.fake.mu.Lock()
	defer s.fake.mu.Unlock()
	return rec.Code, rec.Body.String(), append([]slidingStatsCall(nil), s.fake.statsCalls[before:]...)
}

// scans reports raw log scans and calls to endpoints other than stats.
func (s *drilldownBreakdownStack) scans() (int, []string) {
	s.fake.mu.Lock()
	defer s.fake.mu.Unlock()
	return s.fake.rawCalls, append([]string(nil), s.fake.otherCalls...)
}

// Under the tenant's max_query_series a Drilldown breakdown holds every series
// with Loki's values and no warning, from one stats_query_range call: the same
// answer a plain client gets, and the same as Loki's.
//
// conformance: limits/drilldown-breakdown-exact, series-limits-and-partial-results, loki_api_v1_query_range
func TestDrilldownBreakdown_UnderLimitIsExactLikeLoki(t *testing.T) {
	base := time.Unix(1700000400, 0).UTC()
	lines := drilldownBreakdownFixture(base)
	step := 5 * time.Minute
	// Drilldown sends millisecond starts that are not step-aligned.
	start := base.Add(2*time.Minute + 123*time.Millisecond)
	end := start.Add(20 * time.Minute)
	for _, shape := range drilldownBreakdownShapes {
		t.Run(shape.name, func(t *testing.T) {
			params := tumblingRangeParams(shape.query, start, end, step)
			want := lokiTumblingReferenceParsed(lines, "count_over_time", []string{shape.field}, start, end, step, step, shape.parsed)
			if len(want) != len(shape.busiest) {
				t.Fatalf("fixture: expected %d reference series, got %d", len(shape.busiest), len(want))
			}
			for _, drilldown := range []bool{true, false} {
				stack := newDrilldownBreakdownStack(t, Config{}, lines)
				code, body, calls := stack.run(params, "tenant-a", drilldown)
				if code != http.StatusOK || strings.Contains(body, `"warnings"`) {
					t.Fatalf("drilldown=%v: expected every series without a warning, got %d: %s", drilldown, code, body)
				}
				assertTumblingEqual(t, shape.query, want, decodeTumblingSeries(t, shape.query, []byte(body)))
				raw, other := stack.scans()
				if len(calls) != 1 || strings.Contains(calls[0].query, "__lvp_rank") || raw != 0 || len(other) != 0 {
					t.Fatalf("drilldown=%v: expected one plain stats_query_range call, got %+v, %d raw scans and %v", drilldown, calls, raw, other)
				}
			}
		})
	}
}

// Above the tenant's max_query_series Drilldown gets that many series, each one
// exact, with Loki's partial-result warning; a plain client gets Loki's 400.
// The proxy keeps the busiest series where Loki keeps the first it encounters.
// Another tenant keeps its own limit.
//
// conformance: limits/drilldown-breakdown-exact, limits/drilldown-partial-with-warning, limits/series-limit-error
// conformance: series-limits-and-partial-results, tenant-query-limits, limits/per-tenant-override
func TestDrilldownBreakdown_OverLimitFollowsLoki(t *testing.T) {
	base := time.Unix(1700000400, 0).UTC()
	lines := drilldownBreakdownFixture(base)
	step := 5 * time.Minute
	start := base.Add(2*time.Minute + 123*time.Millisecond)
	end := start.Add(20 * time.Minute)
	cfg := Config{TenantLimits: map[string]map[string]any{"tenant-a": {"max_query_series": 2.0}}}
	for _, shape := range drilldownBreakdownShapes {
		t.Run(shape.name, func(t *testing.T) {
			params := tumblingRangeParams(shape.query, start, end, step)
			want := lokiTumblingReferenceParsed(lines, "count_over_time", []string{shape.field}, start, end, step, step, shape.parsed)

			stack := newDrilldownBreakdownStack(t, cfg, lines)
			code, body, _ := stack.run(params, "tenant-a", false)
			if code != http.StatusBadRequest || !strings.Contains(body, "maximum number of series (2) reached for a single query; consider reducing query cardinality") {
				t.Fatalf("plain client: expected Loki's series limit error, got %d: %s", code, body)
			}

			kept := map[string]map[int64]string{}
			for _, value := range shape.busiest[:2] {
				key := canonicalLabelsKey(map[string]string{shape.field: value})
				kept[key] = want[key]
			}
			// The first Drilldown request finds the breakdown over the limit and asks
			// again with VictoriaLogs ranking the values; the next one ranks at once.
			// The second request ends a second later (past the response cache), on
			// the same evaluation timestamps.
			for i, wantCalls := range []int{2, 1} {
				code, body, calls := stack.run(tumblingRangeParams(shape.query, start, end.Add(time.Duration(i)*time.Second), step), "tenant-a", true)
				if code != http.StatusOK || !strings.Contains(body, `"warnings":["maximum number of series (2) reached for a single query; returning partial results"]`) {
					t.Fatalf("drilldown request %d: expected a partial result with Loki's warning, got %d: %s", i, code, body)
				}
				assertTumblingEqual(t, shape.query, kept, decodeTumblingSeries(t, shape.query, []byte(body)))
				if len(calls) != wantCalls || !strings.Contains(calls[len(calls)-1].query, "| limit 3 |") {
					t.Fatalf("drilldown request %d: expected %d calls ending with the ranked one for limit+1 values, got %+v", i, wantCalls, calls)
				}
			}
			if raw, other := stack.scans(); raw != 0 || len(other) != 0 {
				t.Fatalf("expected no raw scans and no other endpoints, got %d and %v", raw, other)
			}

			// tenant-b keeps Loki's default of 500 and gets every series.
			code, body, _ = stack.run(params, "tenant-b", true)
			if code != http.StatusOK || strings.Contains(body, `"warnings"`) {
				t.Fatalf("tenant-b: expected every series without a warning, got %d: %s", code, body)
			}
			assertTumblingEqual(t, shape.query, want, decodeTumblingSeries(t, shape.query, []byte(body)))
		})
	}
}

// Lines after Loki's last evaluation timestamp (the end is not on the step
// grid) and lines exactly on the start edge belong to no sample: a value seen
// only there is not a series and does not count toward the limit.
//
// conformance: limits/drilldown-breakdown-exact, series-limits-and-partial-results
func TestDrilldownBreakdown_ValuesOutsideEveryWindowAreNotSeries(t *testing.T) {
	base := time.Unix(1700000400, 0).UTC()
	start := base.Add(2*time.Minute + 123*time.Millisecond)
	end := start.Add(20*time.Minute + 2*time.Minute) // last evaluation at start+20m
	step := 5 * time.Minute
	lines := drilldownBreakdownFixture(base)
	lines = append(lines,
		tumblingLine{ts: start.Add(21 * time.Minute).UnixNano(), labels: map[string]string{"app": "tumble", "pod": "tail"}, msg: "level=info user_id=u-tail"},
		tumblingLine{ts: start.Add(-step).UnixNano(), labels: map[string]string{"app": "tumble", "pod": "edge"}, msg: "level=info user_id=u-edge"})
	sort.Slice(lines, func(i, j int) bool { return lines[i].ts < lines[j].ts })
	cfg := Config{TenantLimits: map[string]map[string]any{"tenant-a": {"max_query_series": 6.0}}}
	shape := drilldownBreakdownShapes[0]
	params := tumblingRangeParams(shape.query, start, end, step)
	want := lokiTumblingReference(lines, "count_over_time", []string{"pod"}, start, end, step, step)
	if len(want) != 6 {
		t.Fatalf("fixture: expected the six pods in Loki's answer, got %v", tumblingKeys(want))
	}
	for _, drilldown := range []bool{true, false} {
		stack := newDrilldownBreakdownStack(t, cfg, lines)
		code, body, _ := stack.run(params, "tenant-a", drilldown)
		if code != http.StatusOK || strings.Contains(body, `"warnings"`) {
			t.Fatalf("drilldown=%v: expected the six series without a warning, got %d: %s", drilldown, code, body)
		}
		assertTumblingEqual(t, shape.query, want, decodeTumblingSeries(t, shape.query, []byte(body)))
	}
}

// Drilldown breakdowns share the stats_query_range slots, and a slot is free
// again only after the inter-query pause, which does not delay the response.
func TestDrilldownBreakdown_SharesStatsSlotsWithoutDelayingTheAnswer(t *testing.T) {
	base := time.Unix(1700000400, 0).UTC()
	lines := drilldownBreakdownFixture(base)
	start := base.Add(2 * time.Minute)
	step := 5 * time.Minute
	stack := newDrilldownBreakdownStack(t, Config{StatsQueryRangeConcurrency: 1, StatsQueryRangeInterQueryDelayMs: 1500}, lines)
	shape := drilldownBreakdownShapes[0]
	began := time.Now()
	if code, body, _ := stack.run(tumblingRangeParams(shape.query, start, start.Add(20*time.Minute), step), "tenant-a", true); code != http.StatusOK {
		t.Fatalf("first breakdown: %d %s", code, body)
	}
	first := time.Since(began)
	if code, body, _ := stack.run(tumblingRangeParams(shape.query, start, start.Add(15*time.Minute), step), "tenant-a", true); code != http.StatusOK {
		t.Fatalf("second breakdown: %d %s", code, body)
	}
	if second := time.Since(began); first >= time.Second || second < 1500*time.Millisecond {
		t.Fatalf("expected the first answer before the pause and the second after it, got %s and %s", first, second)
	}
}

// A series whose buckets all fall outside the request after the relabel is
// dropped: Loki never returns a matrix series without samples.
func TestRelabelStatsQueryRange_DropsSeriesWithoutPoints(t *testing.T) {
	body := []byte(`{"status":"success","data":{"resultType":"matrix","result":[` +
		`{"metric":{"pod":"a"},"values":[[1700000000,"1"],[1700000300,"2"]]},` +
		`{"metric":{"pod":"b"},"values":[[1699999400,"5"]]}]}}`)
	got := string(relabelSnappedTumblingStatsQueryRange(body, 1700000300e9, 1700000600e9, 300e9))
	want := `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"pod":"a"},"values":[[1700000300,"1"],[1700000600,"2"]]}]}}`
	if got != want {
		t.Fatalf("got  %s\nwant %s", got, want)
	}
}

// The ranking subquery counts the values over the query's own filters and
// keeps the rest of the pipeline after the in() filter, so the outer stats
// pipe is the one the translator built.
func TestRankedSingleFieldQuery(t *testing.T) {
	cases := []struct {
		query, want string
		ok          bool
	}{
		{`app:="x" pod:!"" | stats by (pod) count()`,
			`app:="x" pod:!"" | filter pod:in(app:="x" pod:!"" | stats by (pod) count() as __lvp_rank | sort by (__lvp_rank desc, pod) | limit 11 | fields pod) | stats by (pod) count()`, true},
		{`app:="x" | unpack_logfmt | filter user_id:!"" | stats by (user_id) count()`,
			`app:="x" | unpack_logfmt | filter user_id:!"" | filter user_id:in(app:="x" | unpack_logfmt | filter user_id:!"" | stats by (user_id) count() as __lvp_rank | sort by (__lvp_rank desc, user_id) | limit 11 | fields user_id) | stats by (user_id) count()`, true},
		{`app:="x" | stats by ("service.name") count()`,
			`app:="x" | filter "service.name":in(app:="x" | stats by ("service.name") count() as __lvp_rank | sort by (__lvp_rank desc, "service.name") | limit 11 | fields "service.name") | stats by ("service.name") count()`, true},
		{`app:="x" | stats by (pod, app) count()`, "", false},
		{`app:="x" | stats by (pod) sum_len(_msg)`, "", false},
	}
	for _, tc := range cases {
		got, ok := rankedSingleFieldQuery(tc.query, 10)
		if ok != tc.ok || got != tc.want {
			t.Errorf("rankedSingleFieldQuery(%q)\n got %v %q\nwant %v %q", tc.query, ok, got, tc.ok, tc.want)
		}
	}
}
