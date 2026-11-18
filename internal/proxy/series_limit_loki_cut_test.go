package proxy

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/testutil/vlemu"
)

// The staggered fixture tells the three possible rankings apart. On the 1m grid of [base+5m, base+10m] with a 5m
// range the pods are first met at these steps: p4 6m, p1 7m, p5 8m, p0 and p3 9m, p2 10m. Loki's first-met answer at
// a limit of 3 keeps p1, p4 and p5 and ends at 9m, where p0 and p3 trip the limit; the busiest three are p0, p4 and
// p5; label order would keep p0, p1 and p2.
const (
	seriesLimitStaggeredKept = "[p1 p4 p5]"
	seriesLimitStaggeredTrip = 9 * time.Minute
)

func seriesLimitStaggeredFixture(base time.Time) []seriesLimitLine {
	var lines []seriesLimitLine
	for _, pod := range []struct {
		name  string
		at    time.Duration
		lines int
	}{
		{"p4", 5*time.Minute + 10*time.Second, 2},
		{"p1", 6*time.Minute + 10*time.Second, 2},
		{"p5", 7*time.Minute + 10*time.Second, 3},
		{"p0", 8*time.Minute + 10*time.Second, 6},
		{"p3", 8*time.Minute + 20*time.Second, 4},
		{"p2", 9*time.Minute + 10*time.Second, 5},
	} {
		for k := 0; k < pod.lines; k++ {
			lines = append(lines, seriesLimitLine{ts: base.Add(pod.at + time.Duration(k)*time.Millisecond).UnixNano(), pod: pod.name})
		}
	}
	return lines
}

// seriesLimitCutAnswer reads a Drilldown range answer: its warnings, the label values of label (sorted) and the
// latest and earliest sample times over every series.
type seriesLimitCutAnswer struct {
	warnings []string
	kept     string
	last     time.Time
	first    time.Time
	series   int
}

func readSeriesLimitCutAnswer(t *testing.T, rec *httptest.ResponseRecorder, label string) seriesLimitCutAnswer {
	t.Helper()
	var resp struct {
		Warnings []string `json:"warnings"`
		Data     struct {
			Result []struct {
				Metric map[string]string `json:"metric"`
				Values [][]any           `json:"values"`
			} `json:"result"`
		} `json:"data"`
	}
	if rec.Code != http.StatusOK || json.Unmarshal(rec.Body.Bytes(), &resp) != nil {
		t.Fatalf("expected a 200 matrix: %d %s", rec.Code, rec.Body.String())
	}
	out := seriesLimitCutAnswer{warnings: resp.Warnings, series: len(resp.Data.Result)}
	var values []string
	for _, s := range resp.Data.Result {
		values = append(values, s.Metric[label])
		for _, point := range s.Values {
			sec, err := strconv.ParseFloat(fmt.Sprint(point[0]), 64)
			if err != nil {
				t.Fatalf("point time %v: %v", point[0], err)
			}
			at := time.Unix(0, int64(sec*1e9)).UTC()
			if at.After(out.last) {
				out.last = at
			}
			if out.first.IsZero() || at.Before(out.first) {
				out.first = at
			}
		}
	}
	sort.Strings(values)
	out.kept = fmt.Sprint(values)
	return out
}

func seriesLimitCalls(f *seriesLimitFakeVL) int {
	total := 0
	for _, n := range f.snapshot() {
		total += n
	}
	return total
}

// assertLokiCut checks a Drilldown range answer over the limit against Loki's: Loki's warning, the first-met series
// and no sample after the trip step, which every kept series reaches here.
func assertLokiCut(t *testing.T, rec *httptest.ResponseRecorder, limit int, kept string, trip time.Time) {
	t.Helper()
	got := readSeriesLimitCutAnswer(t, rec, "pod")
	want := fmt.Sprintf("maximum number of series (%d) reached for a single query; returning partial results", limit)
	if fmt.Sprint(got.warnings) != fmt.Sprint([]string{want}) {
		t.Fatalf("warnings %v, want Loki's: %s", got.warnings, rec.Body.String())
	}
	if got.kept != kept {
		t.Fatalf("kept %s, want the first-met series %s: %s", got.kept, kept, rec.Body.String())
	}
	if !got.last.Equal(trip) {
		t.Fatalf("answer ends at %s, want the trip step %s: %s", got.last, trip, rec.Body.String())
	}
}

// Every Drilldown range route cut by the series limit answers as Loki does in the default mode: the first series
// met, ending at the trip step, with Loki's warning; a repeat answered from the response cache is the cold answer
// byte for byte with no VictoriaLogs call; and an answer within the limit passes whole in both modes. Instant
// answers keep the busiest series in both modes.
//
// conformance: limits/drilldown-series-limit-range-cut-at-trip-step, limits/drilldown-partial-with-warning, series-limits-and-partial-results
// conformance: loki_api_v1_query_range, loki_api_v1_query
func TestSeriesLimit_LokiModeEveryRangeRoute(t *testing.T) {
	base := time.Unix(1700000400, 0).UTC() // on the minute grid
	start, end := base.Add(5*time.Minute), base.Add(10*time.Minute)
	lines := seriesLimitStaggeredFixture(base)
	rangeParams := func(query string) url.Values { return tumblingRangeParams(query, start, end, time.Minute) }
	instantParams := func(query string) url.Values {
		return url.Values{"query": {query}, "time": {strconv.FormatInt(end.Unix(), 10)}}
	}
	drilldown := http.Header{"X-Query-Tags": {"Source=grafana-lokiexplore-app"}}
	for _, tc := range []struct {
		name     string
		path     string
		params   url.Values
		declared []string
		route    string
	}{
		{"ordered json raw evaluator", "/loki/api/v1/query_range", rangeParams(`sum by (pod) (count_over_time({app="top"} | json | drop __error__, __error_details__ | json | drop __error__, __error_details__ | pipeline="logs/loki" [5m]))`), nil, "raw"},
		{"ordered json stats pushdown", "/loki/api/v1/query_range", rangeParams(`sum by (pod) (count_over_time({app="top"} | json | drop __error__, __error_details__ | pipeline="logs/loki" [5m]))`), nil, "stats_query_range"},
		{"ordered json stats pushdown with a pre-parser filter", "/loki/api/v1/query_range", rangeParams(`sum by (pod) (count_over_time({app="top"} | detected_level="info" | json | drop __error__, __error_details__ | pipeline="logs/loki" [5m]))`), nil, "stats_query_range"},
		{"stats buckets", "/loki/api/v1/query_range", rangeParams(`sum by (pod) (count_over_time({app="top"}[5m]))`), nil, "stats_query_range"},
		{"manual raw-row evaluator", "/loki/api/v1/query_range", rangeParams(`rate_counter({app="top"} | unwrap latency [5m])`), nil, "raw"},
		{"quantile value counts", "/loki/api/v1/query_range", rangeParams(`quantile_over_time(0.5, {app="top"} | unwrap latency [5m]) by (pod)`), nil, "stats_query_range"},
		{"bare parser raw evaluator", "/loki/api/v1/query_range", rangeParams(`avg_over_time({app="top"} | logfmt | unwrap latency [5m])`), nil, "raw"},
		{"bare parser stats buckets", "/loki/api/v1/query_range", rangeParams(`count_over_time({app="top"} | logfmt [5m])`), nil, "stats_query_range"},
		{"bare parser hits buckets", "/loki/api/v1/query_range", rangeParams(`count_over_time({app="top"} | logfmt [5m])`), []string{"app", "pod"}, "hits"},
		{"bare parser unwrap stats buckets", "/loki/api/v1/query_range", rangeParams(`sum_over_time({app="top"} | logfmt | unwrap latency [5m])`), nil, "stats_query_range"},
		{"binary with scalar", "/loki/api/v1/query_range", rangeParams(`sum by (pod) (count_over_time({app="top"}[5m])) * 2`), nil, "stats_query_range"},
		{"label_replace capture", "/loki/api/v1/query_range", rangeParams(`label_replace(sum by (pod) (count_over_time({app="top"}[5m])), "copy", "$1", "pod", "(.*)")`), nil, "stats_query_range"},
		{"instant ordered json raw evaluator", "/loki/api/v1/query", instantParams(`sum by (pod) (count_over_time({app="top"} | json | drop __error__, __error_details__ | json | drop __error__, __error_details__ | pipeline="logs/loki" [5m]))`), nil, "raw"},
		{"instant stats", "/loki/api/v1/query", instantParams(`sum by (pod) (count_over_time({app="top"}[5m]))`), nil, ""},
		{"instant quantile value counts", "/loki/api/v1/query", instantParams(`quantile_over_time(0.5, {app="top"} | unwrap latency [5m]) by (pod)`), nil, "stats_query"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			serve := func(mode string, limit int) (*seriesLimitFakeVL, *Proxy, *httptest.ResponseRecorder) {
				fake := &seriesLimitFakeVL{t: t, lines: lines}
				srv := httptest.NewServer(fake)
				t.Cleanup(srv.Close)
				p := newSlidingTestProxy(t, srv.URL)
				p.drilldownSeriesLimitMode = mode
				p.maxStatsQuerySeries = limit
				p.declaredLabelFields = tc.declared
				rec := serveTumblingQuery(p, tc.path, tc.params, drilldown)
				if tc.route != "" && fake.snapshot()[tc.route] == 0 {
					t.Fatalf("%s: route not taken, VictoriaLogs calls %v", mode, fake.snapshot())
				}
				return fake, p, rec
			}
			// Within the limit: whole, no warning, the same in both modes.
			_, _, whole := serve(drilldownSeriesLimitLoki, 6)
			if _, _, busiest := serve(drilldownSeriesLimitBusiest, 6); whole.Code != http.StatusOK || bytes.Contains(whole.Body.Bytes(), []byte(`"warnings"`)) || whole.Body.String() != busiest.Body.String() {
				t.Fatalf("an answer within the limit must pass whole in both modes: %d %s / %s", whole.Code, whole.Body.String(), busiest.Body.String())
			}

			_, p, cold := serve(drilldownSeriesLimitLoki, 3)
			if tc.path == "/loki/api/v1/query" {
				_, _, busiest := serve(drilldownSeriesLimitBusiest, 3)
				if cold.Code != http.StatusOK || cold.Body.String() != busiest.Body.String() {
					t.Fatalf("an instant answer keeps the busiest series in both modes: loki %s, busiest %s", cold.Body.String(), busiest.Body.String())
				}
			} else {
				assertLokiCut(t, cold, 3, seriesLimitStaggeredKept, base.Add(seriesLimitStaggeredTrip))
			}
			// A repeat through the handler (its own response cache where the route has one) is the cold answer.
			if warm := serveTumblingQuery(p, tc.path, tc.params, drilldown); warm.Code != cold.Code || warm.Body.String() != cold.Body.String() {
				t.Fatalf("a repeat through the handler differs from the cold answer:\ncold %s\nwarm %s", cold.Body.String(), warm.Body.String())
			}
			// As served (the compatibility cache in front): the same answer, a repeat that is the cold answer, and the
			// VictoriaLogs calls of the busiest mode, cold and repeated (a route the served cache keeps asks nothing).
			target := tc.path + "?" + tc.params.Encode()
			headers := map[string]string{"X-Query-Tags": drilldown.Get("X-Query-Tags")}
			servedTwice := func(mode string) (cold, warm string, coldCalls, warmCalls int) {
				fake, p, _ := serve(mode, 3)
				base := seriesLimitCalls(fake)
				cold = doCompatProxyRequest(p, target, headers).Body.String()
				coldCalls = seriesLimitCalls(fake) - base
				warm = doCompatProxyRequest(p, target, headers).Body.String()
				return cold, warm, coldCalls, seriesLimitCalls(fake) - base - coldCalls
			}
			served, repeat, coldCalls, warmCalls := servedTwice(drilldownSeriesLimitLoki)
			if served != cold.Body.String() || repeat != served {
				t.Fatalf("served answers differ from the handler's:\nhandler %s\nserved  %s\nrepeat  %s", cold.Body.String(), served, repeat)
			}
			_, _, busiestCold, busiestWarm := servedTwice(drilldownSeriesLimitBusiest)
			if coldCalls != busiestCold || warmCalls != busiestWarm {
				t.Fatalf("VictoriaLogs calls cold/repeat: loki %d/%d, busiest %d/%d", coldCalls, warmCalls, busiestCold, busiestWarm)
			}
		})
	}
}

// The sliding window evaluator (grouped range != step over more buckets than Loki's 11,000-point limit) cuts a
// Drilldown answer as Loki does: the first series met, ending at the trip step.
//
// conformance: limits/drilldown-series-limit-range-cut-at-trip-step, series-limits-and-partial-results
// conformance: loki_api_v1_query_range
func TestSeriesLimit_LokiModeSlidingWindowStats(t *testing.T) {
	end := time.Date(2026, 9, 15, 9, 0, 0, 0, time.UTC)
	rng := 24 * time.Hour
	step := rng / 1000
	start := end.Add(-rng)
	// Every pod writes once an hour from its first hour on: p4 from 1h, p1 from 2h, p5 from 3h, p0 and p3 from 4h,
	// p2 from 5h. p0 and p3 trip the limit; the kept series run past that step unless the answer is cut there.
	var rows []emulatedRow
	for _, pod := range []struct {
		name  string
		first time.Duration
	}{{"p4", time.Hour}, {"p1", 2 * time.Hour}, {"p5", 3 * time.Hour}, {"p0", 4 * time.Hour}, {"p3", 4 * time.Hour}, {"p2", 5 * time.Hour}} {
		for at := pod.first; at < rng; at += time.Hour {
			rows = append(rows, emulatedRow{ts: start.Add(at).UnixNano(), pod: pod.name})
		}
	}
	params := url.Values{
		"query": {`sum by (pod) (count_over_time({app="api"}[5m]))`},
		"start": {strconv.FormatInt(start.UnixNano(), 10)},
		"end":   {strconv.FormatInt(end.UnixNano(), 10)},
		"step":  {strconv.FormatFloat(step.Seconds(), 'f', -1, 64)},
	}
	emu := &vlStatsRangeEmulator{t: t, rows: rows}
	srv := httptest.NewServer(emu)
	defer srv.Close()
	p := newSlidingTestProxy(t, srv.URL)
	p.maxStatsQuerySeries = 3
	header := http.Header{"X-Query-Tags": {"Source=grafana-lokiexplore-app"}}
	cold := serveTumblingQuery(p, "/loki/api/v1/query_range", params, header)
	emu.mu.Lock()
	stats, raw := len(emu.calls), emu.rawCalls
	emu.mu.Unlock()
	if stats == 0 || raw != 0 {
		t.Fatalf("expected the sliding window stats route, got %d stats pipes and %d raw fetches", stats, raw)
	}
	// p0 and p3 trip the limit at the first step that meets 4h, on the 86.4s grid from start.
	trip := start.Add((4*time.Hour + step - 1) / step * step)
	assertLokiCut(t, cold, 3, seriesLimitStaggeredKept, trip)
	warm := serveTumblingQuery(p, "/loki/api/v1/query_range", params, header)
	if warm.Body.String() != cold.Body.String() {
		t.Fatalf("a repeat differs from the cold answer:\ncold %s\nwarm %s", cold.Body.String(), warm.Body.String())
	}
}

// The ordered JSON result cache keeps the answer as sent: a repeat of a Drilldown answer cut by the limit is the
// cold answer, cut and warning alike, in both modes, with no VictoriaLogs call.
//
// conformance: limits/drilldown-series-limit-range-cut-at-trip-step, limits/drilldown-partial-with-warning
// conformance: loki_api_v1_query_range
func TestOrderedJSONResultCacheRepeatsTheCutAnswer(t *testing.T) {
	base := time.Unix(1700000400, 0).UTC()
	shape := dlvBaselineShapes[1]
	for _, mode := range []string{drilldownSeriesLimitLoki, drilldownSeriesLimitBusiest} {
		backend, _ := newDLVBaselineBackend(t, shape, base)
		p := newDLVBaselineProxy(t, backend.URL, false)
		p.drilldownSeriesLimitMode = mode
		p.maxStatsQuerySeries = 1
		origins := traceVLCallOrigins(p)
		target, headers := dlvBaselineRequest(shape, base.Add(time.Minute), base.Add(3*time.Minute))
		headers["X-Query-Tags"] = "Source=grafana-lokiexplore-app"
		cold := doCompatProxyRequest(p, target, withHeader(headers, vlCallOriginHeader, "cold"))
		if cold.Code != http.StatusOK || !bytes.Contains(cold.Body.Bytes(), []byte(`"warnings"`)) || origins.made("cold") == 0 {
			t.Fatalf("%s: expected a capped answer with Loki's warning: %d %s", mode, cold.Code, cold.Body)
		}
		warm := doCompatProxyRequest(p, target, withHeader(headers, vlCallOriginHeader, "warm"))
		if warm.Code != cold.Code || !bytes.Equal(cold.Body.Bytes(), warm.Body.Bytes()) || origins.made("warm") != 0 {
			t.Fatalf("%s: a repeat differs from the cold answer or asked VictoriaLogs (%d calls):\ncold %s\nwarm %s", mode, origins.made("warm"), cold.Body, warm.Body)
		}
	}
}

// Through the routes as served (the compatibility cache in front of the handler) a Drilldown answer cut by the
// limit repeats byte for byte with no VictoriaLogs call, and a binary operand reused from the cache keeps the
// operand's cut: `operand * 2` answers the operand's series, cut at the same step, values doubled. A union over
// the limit is cut where the union trips it.
//
// conformance: limits/drilldown-series-limit-range-cut-at-trip-step, limits/drilldown-partial-with-warning
// conformance: loki_api_v1_query_range
func TestSeriesLimit_LokiModeServedAndBinaryOperandRepeat(t *testing.T) {
	base := time.Unix(1700000400, 0).UTC()
	start, end := base.Add(5*time.Minute), base.Add(10*time.Minute)
	fake := &seriesLimitFakeVL{t: t, lines: seriesLimitStaggeredFixture(base)}
	srv := httptest.NewServer(fake)
	defer srv.Close()
	p := newSlidingTestProxy(t, srv.URL)
	p.maxStatsQuerySeries = 3
	headers := map[string]string{"X-Query-Tags": "Source=grafana-lokiexplore-app"}
	target := func(query string) string {
		return "/loki/api/v1/query_range?" + tumblingRangeParams(query, start, end, time.Minute).Encode()
	}
	operand := `sum by (pod) (count_over_time({app="top"}[5m]))`
	trip := base.Add(seriesLimitStaggeredTrip)

	cold := doCompatProxyRequest(p, target(operand), headers)
	assertLokiCut(t, cold, 3, seriesLimitStaggeredKept, trip)
	before := seriesLimitCalls(fake)
	if warm := doCompatProxyRequest(p, target(operand), headers); warm.Body.String() != cold.Body.String() || seriesLimitCalls(fake) != before {
		t.Fatalf("a served repeat differs or asked VictoriaLogs:\ncold %s\nwarm %s", cold.Body.String(), warm.Body.String())
	}

	// The operand alone through the handler fills its response cache; the binary reads it from there.
	direct := serveTumblingQuery(p, "/loki/api/v1/query_range", tumblingRangeParams(operand, start, end, time.Minute), http.Header{"X-Query-Tags": {headers["X-Query-Tags"]}})
	assertLokiCut(t, direct, 3, seriesLimitStaggeredKept, trip)
	before = seriesLimitCalls(fake)
	doubled := doCompatProxyRequest(p, target(operand+" * 2"), headers)
	if calls := seriesLimitCalls(fake) - before; calls != 0 {
		t.Fatalf("the operand was not reused (%d VictoriaLogs calls)", calls)
	}
	assertLokiCut(t, doubled, 3, seriesLimitStaggeredKept, trip)
	if a, b := readSeriesLimitCutAnswer(t, doubled, "pod"), readSeriesLimitCutAnswer(t, direct, "pod"); a.first != b.first || a.series != b.series {
		t.Fatalf("operand * 2 is not the operand's cut answer: %s / %s", doubled.Body.String(), direct.Body.String())
	}
	if again := doCompatProxyRequest(p, target(operand+" * 2"), headers); again.Body.String() != doubled.Body.String() {
		t.Fatalf("a repeat of the binary differs:\n%s\n%s", doubled.Body.String(), again.Body.String())
	}

	// A union with a second grouping: the app series and p4 are met at 6m, p1 at 7m fills the limit and p5 at 8m
	// trips it, so the union ends at 8m with three series.
	union := doCompatProxyRequest(p, target(operand+` or sum by (app) (count_over_time({app="top"}[5m]))`), headers)
	got := readSeriesLimitCutAnswer(t, union, "pod")
	if got.series != 3 || !got.last.Equal(base.Add(8*time.Minute)) {
		t.Fatalf("a union over the limit: %d series ending %s, want 3 ending at the union's trip step 8m: %s", got.series, got.last, union.Body.String())
	}
	if repeat := doCompatProxyRequest(p, target(operand+` or sum by (app) (count_over_time({app="top"}[5m]))`), headers); repeat.Body.String() != union.Body.String() {
		t.Fatalf("a repeat of the union differs:\n%s\n%s", union.Body.String(), repeat.Body.String())
	}
}

// The stats extent cache keeps VictoriaLogs' buckets, not the cut answer: over refreshes, re-runs and random
// windows a Drilldown answer cut by the limit is, byte for byte, the answer of a proxy without the extent cache, while
// the cached proxy reads stored buckets.
//
// conformance: limits/drilldown-series-limit-range-cut-at-trip-step, quality/query-range-extent-cache
// conformance: loki_api_v1_query_range
func TestSeriesLimit_LokiModeExtentCacheKeepsTheCut(t *testing.T) {
	s0 := time.Unix(1700001000, 0).UTC()
	span := 6 * time.Hour
	// Apps start an hour apart, so most windows trip a limit of two at a step inside them.
	var lines []slidingFixtureLine
	for i := 0; time.Duration(i)*13*time.Second < span; i++ {
		at := time.Duration(i) * 13 * time.Second
		apps := []string{"a", "b", "c", "d", "e"}[:min(5, 1+int(at/time.Hour))]
		lines = append(lines, slidingFixtureLine{ts: s0.Add(at).UnixNano(), app: apps[i%len(apps)], msg: "tick"})
	}
	query := `sum by (app) (count_over_time({app=~".+"}[5m]))`
	header := http.Header{"X-Query-Tags": {"Source=grafana-lokiexplore-app"}}
	onSrv, _ := newExtentShim(t, func() http.Handler { srv, _ := newSlidingFakeVL(t, lines); return srv.Config.Handler }())
	offSrv, _ := newExtentShim(t, func() http.Handler { srv, _ := newSlidingFakeVL(t, lines); return srv.Config.Handler }())
	on := newExtentTestProxy(t, onSrv.URL, true, 256<<20)
	off := newExtentTestProxy(t, offSrv.URL, false, 256<<20)
	on.maxStatsQuerySeries, off.maxStatsQuerySeries = 2, 2
	cut := 0
	for _, w := range extentWindows(3, s0, s0.Add(span)) {
		wantCode, want := extentQueryRange(t, off, query, w.start, w.end, w.step, header)
		gotCode, got := extentQueryRange(t, on, query, w.start, w.end, w.step, header)
		if gotCode != wantCode || !bytes.Equal(got, want) {
			t.Fatalf("[%s, %s] step %s: the extent cache changed the answer\n with: %d %s\nwithout: %d %s", w.start.Format(time.RFC3339), w.end.Format(time.RFC3339), w.step, gotCode, got, wantCode, want)
		}
		if bytes.Contains(want, []byte(`"warnings"`)) {
			cut++
		}
	}
	outcomes := extentOutcomes(t, on)
	if cut == 0 || outcomes["hit"]+outcomes["partial"] == 0 || outcomes["unreadable"] != 0 {
		t.Fatalf("the run must cut answers (%d did) and read stored buckets: %v", cut, outcomes)
	}
}

// A multi-tenant metric answer is limited and cut on the merged answer, as Loki's one evaluation across the tenants
// is: two tenants within the limit trip it together, the merged answer ends at that step, and a tenant cut by its
// own limit is cut again where the merge trips. A repeat is the cold answer with no VictoriaLogs call.
//
// conformance: limits/drilldown-series-limit-range-cut-at-trip-step, limits/drilldown-partial-with-warning, series-limits-and-partial-results
// conformance: loki_api_v1_query_range
func TestSeriesLimit_LokiModeMultiTenantMerge(t *testing.T) {
	base := time.Unix(1700000400, 0).UTC()
	params := tumblingRangeParams(`sum by (pod) (count_over_time({app="top"}[5m]))`, base.Add(5*time.Minute), base.Add(10*time.Minute), time.Minute)
	// Every tenant holds the same six pods: twelve tenant series, met in pairs.
	for _, tc := range []struct {
		limit int
		kept  string
		trip  time.Duration
	}{
		// Six: both tenants pass whole; p4, p1 and p5 of both fill the limit and p0/p3 trip it at 9m.
		{6, "[p1 p1 p4 p4 p5 p5]", 9 * time.Minute},
		// Three: each tenant ends at 9m on its own; merged, p4 of both and p1 of one fill the limit at 7m.
		{3, "[p1 p4 p4]", 7 * time.Minute},
	} {
		fake := &seriesLimitFakeVL{t: t, lines: seriesLimitStaggeredFixture(base)}
		srv := httptest.NewServer(fake)
		p, _, mux := newTwoTenantProxy(t, srv.URL)
		p.storeBackendVersion("v1.50.0", "v1.50.0")
		p.maxStatsQuerySeries = tc.limit
		serve := func() *httptest.ResponseRecorder {
			req := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+params.Encode(), nil)
			req.Header.Set("X-Scope-OrgID", "tenant-a|tenant-b")
			req.Header.Set("X-Query-Tags", "Source=grafana-lokiexplore-app")
			rec := httptest.NewRecorder()
			mux.ServeHTTP(rec, req)
			return rec
		}
		cold := serve()
		assertLokiCut(t, cold, tc.limit, tc.kept, base.Add(tc.trip))
		before := seriesLimitCalls(fake)
		warm := serve()
		srv.Close()
		if warm.Body.String() != cold.Body.String() || seriesLimitCalls(fake) != before {
			t.Fatalf("limit %d: a repeat differs or asked VictoriaLogs:\ncold %s\nwarm %s", tc.limit, cold.Body.String(), warm.Body.String())
		}
	}
}

// A Drilldown answer cut by the limit on one replica is served by another from the peer cache: the same bytes,
// with no VictoriaLogs call on the second replica.
//
// conformance: limits/drilldown-series-limit-range-cut-at-trip-step
// conformance: loki_api_v1_query_range
func TestSeriesLimit_LokiModePeerServesTheCutAnswer(t *testing.T) {
	base := time.Unix(1700000400, 0).UTC()
	fake := &seriesLimitFakeVL{t: t, lines: seriesLimitStaggeredFixture(base)}
	srv := httptest.NewServer(fake)
	defer srv.Close()
	listeners := make([]net.Listener, 2)
	addrs := make([]string, 2)
	for i := range listeners {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		listeners[i], addrs[i] = l, l.Addr().String()
	}
	handlers := make([]http.Handler, 2)
	peers := make([]*cache.PeerCache, 2)
	for i := range listeners {
		// Two replicas, each a replica of every key: the replica that answers pushes the shared entry to the other.
		pc := cache.NewPeerCache(cache.PeerConfig{SelfAddr: addrs[i], DiscoveryType: "static", StaticPeers: strings.Join(addrs, ","),
			Timeout: 2 * time.Second, AuthToken: "fleet-token", WriteThrough: true, WriteThroughMinTTL: time.Second, ReplicationFactor: 2})
		t.Cleanup(pc.Close)
		peers[i] = pc
		c := cache.New(60*time.Second, 1000)
		c.SetL3(pc)
		compat := cache.New(60*time.Second, 1000)
		t.Cleanup(compat.Close)
		p, err := New(Config{BackendURL: srv.URL, Cache: c, CompatCache: compat, LogLevel: "error", PeerCache: pc, PeerAuthToken: "fleet-token", MaxStatsQuerySeries: 3})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
		p.storeBackendVersion("v1.50.0", "v1.50.0")
		mux := http.NewServeMux()
		p.RegisterRoutes(mux)
		handlers[i] = mux
		server := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
		go func(l net.Listener) { _ = server.Serve(l) }(listeners[i])
		t.Cleanup(func() { _ = server.Close() })
	}
	target := "/loki/api/v1/query_range?" + tumblingRangeParams(`sum by (pod) (count_over_time({app="top"}[5m]))`, base.Add(5*time.Minute), base.Add(10*time.Minute), time.Minute).Encode()
	serve := func(h http.Handler) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodGet, target, nil)
		req.Header.Set("X-Query-Tags", "Source=grafana-lokiexplore-app")
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		rec.Body = bytes.NewBuffer(stripQueryStats(rec.Body.Bytes()))
		return rec
	}
	cold := serve(handlers[0])
	assertLokiCut(t, cold, 3, seriesLimitStaggeredKept, base.Add(seriesLimitStaggeredTrip))
	waitFor(t, func() bool { return peers[0].WTPushes.Load() > 0 })
	before := seriesLimitCalls(fake)
	peer := serve(handlers[1])
	if calls := seriesLimitCalls(fake) - before; calls != 0 {
		t.Fatalf("the second replica asked VictoriaLogs %d times, want the peer's entry", calls)
	}
	if peer.Body.String() != cold.Body.String() {
		t.Fatalf("the peer's answer differs from the cold answer:\ncold %s\npeer %s", cold.Body.String(), peer.Body.String())
	}
}

// BenchmarkSeriesLimitDrilldownRangeRequest is one Drilldown range request over the series limit (120 pods first
// met one per step, limit 60) through the handler, with no cache hit, in each mode: busiest is the answer before
// the Loki mode existed, so the two sub-benchmarks are the request path's cost before and after.
func BenchmarkSeriesLimitDrilldownRangeRequest(b *testing.B) {
	base := time.Unix(1700000400, 0).UTC()
	var lines []seriesLimitLine
	for i := 0; i < 120; i++ {
		for k := 0; k <= i%5; k++ {
			lines = append(lines, seriesLimitLine{ts: base.Add(time.Duration(i)*30*time.Second + time.Duration(k)*time.Millisecond).UnixNano(), pod: fmt.Sprintf("p%03d", i)})
		}
	}
	header := http.Header{"X-Query-Tags": {"Source=grafana-lokiexplore-app"}}
	for _, mode := range []string{drilldownSeriesLimitBusiest, drilldownSeriesLimitLoki} {
		b.Run(mode, func(b *testing.B) {
			srv := httptest.NewServer(&seriesLimitFakeVL{t: &testing.T{}, lines: lines})
			defer srv.Close()
			c := cache.New(60*time.Second, 1000)
			defer c.Close()
			p, err := New(Config{BackendURL: srv.URL, Cache: c, LogLevel: "error", DrilldownSeriesLimitMode: mode, MaxStatsQuerySeries: 60})
			if err != nil {
				b.Fatal(err)
			}
			defer func() { _ = p.Shutdown(context.Background()) }()
			p.storeBackendVersion("v1.50.0", "v1.50.0")
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				// A new end each time: every request misses the response cache.
				end := base.Add(time.Hour + time.Duration(i)*time.Millisecond)
				params := tumblingRangeParams(`sum by (pod) (count_over_time({app="top"}[5m]))`, base, end, time.Minute)
				params.Set("end", strconv.FormatInt(end.UnixNano(), 10))
				if rec := serveTumblingQuery(p, "/loki/api/v1/query_range", params, header); rec.Code != http.StatusOK {
					b.Fatalf("%d %s", rec.Code, rec.Body.String())
				}
			}
		})
	}
}

// A Drilldown breakdown refreshed over a moved window takes the ranked read (VictoriaLogs ranks the field's values
// by their first line and returns limit+1 series): every refresh keeps the series met first in its own window and
// ends at that window's trip step, as a fresh proxy answers it.
//
// conformance: limits/drilldown-series-limit-range-cut-at-trip-step, series-limits-and-partial-results
// conformance: loki_api_v1_query_range
func TestSeriesLimit_LokiModeRankedRefresh(t *testing.T) {
	rows := drilldownSeriesLimitRows(8, true) // p00..p07, one every 5 minutes
	newProxy := func() (*vlemu.Emulator, *Proxy) {
		emu := &vlemu.Emulator{T: t, Rows: rows, Field: "amount", Version: "v1.53.0"}
		srv := httptest.NewServer(emu.Handler())
		t.Cleanup(srv.Close)
		p, err := New(Config{BackendURL: srv.URL, Cache: cache.New(60*time.Second, 1000), LogLevel: "error", BackendMinVersion: "v1.40.0",
			BackendVersionCheckTimeout: time.Second, MaxStatsQuerySeries: drilldownSeriesLimit})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
		if err := p.ValidateBackendVersionCompatibility(context.Background()); err != nil {
			t.Fatal(err)
		}
		return emu, p
	}
	headers := map[string]string{"X-Scope-OrgID": "0", "X-Query-Tags": drilldownSeriesLimitTags}
	target := func(shift int64) string {
		return "/loki/api/v1/query_range?" + url.Values{"query": {`sum by (pod) (count_over_time({app="sl"}[1m]))`},
			"start": {strconv.FormatInt(drilldownSeriesLimitEnd-3600+shift, 10)}, "end": {strconv.FormatInt(drilldownSeriesLimitEnd+shift, 10)},
			"step": {"60"}}.Encode()
	}
	emu, p := newProxy()
	ranked := 0
	for _, shift := range []int64{0, 60, 120, 600} {
		got := doCompatProxyRequest(p, target(shift), headers)
		_, fresh := newProxy()
		want := doCompatProxyRequest(fresh, target(shift), headers)
		if got.Body.String() != want.Body.String() {
			t.Fatalf("shift %ds: a refresh differs from a fresh proxy's answer:\nrefresh %s\nfresh   %s", shift, got.Body.String(), want.Body.String())
		}
		// p00, p01 and p02 are met first; p03 starts 15 minutes in and trips the limit at the first step after it.
		first := drilldownSeriesLimitEnd - 3600 + 15*60 // p03's first line is 0.3 s after this
		windowStart := drilldownSeriesLimitEnd - 3600 + shift
		trip := windowStart + ((first-windowStart)/60+1)*60
		assertLokiCut(t, got, drilldownSeriesLimit, "[p00 p01 p02]", time.Unix(trip, 0).UTC())
	}
	emu.Mu.Lock()
	for _, call := range emu.Calls {
		if strings.Contains(call.Query, "min(_time) as __lvp_rank") {
			ranked++
		}
	}
	emu.Mu.Unlock()
	if ranked == 0 {
		t.Fatal("no refresh took the ranked read")
	}
}
