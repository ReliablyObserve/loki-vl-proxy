package proxy

import (
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// totalsStubVL answers raw queries with `rows` log rows and stats queries
// with one bucket per step, each holding a deterministic count, so a test can
// tell totals taken from a count answer from the rows the proxy read.
type totalsStubVL struct {
	rows      int
	statsReqs atomic.Int64
	delay     time.Duration // before a stats answer
}

func (s *totalsStubVL) count(bucket int64) int64 { return 3 + (bucket/int64(time.Second))%17 }

func (s *totalsStubVL) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	_ = r.ParseForm()
	w.Header().Set("Content-Type", "application/json")
	switch r.URL.Path {
	case "/select/logsql/query":
		var b strings.Builder
		now := time.Now().Add(-time.Minute)
		for i := 0; i < s.rows; i++ {
			fmt.Fprintf(&b, `{"_time":%q,"_stream":"{app=\"api\"}","_msg":"line %d","app":"api"}`+"\n",
				now.Add(time.Duration(i)*time.Millisecond).UTC().Format(time.RFC3339Nano), i)
		}
		_, _ = w.Write([]byte(b.String()))
	case "/select/logsql/stats_query_range":
		s.statsReqs.Add(1)
		if s.delay > 0 {
			time.Sleep(s.delay)
		}
		start, _ := parseLokiTimeToUnixNano(r.Form.Get("start"))
		end, _ := parseLokiTimeToUnixNano(r.Form.Get("end"))
		step, ok := parseStepToNanos(r.Form.Get("step"))
		if !ok || step <= 0 {
			step = int64(time.Minute)
		}
		var vals []string
		for t := start - start%step; t <= end && len(vals) < 20000; t += step {
			vals = append(vals, fmt.Sprintf(`[%d,"%d"]`, t/int64(time.Second), s.count(t)))
		}
		fmt.Fprintf(w, `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"level":"info"},"values":[%s]}]}}`, strings.Join(vals, ","))
	case "/select/logsql/stats_query":
		_, _ = w.Write([]byte(`{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1,"41"]}]}}`))
	case "/select/logsql/hits":
		_, _ = w.Write([]byte(`{"hits":[]}`))
	default:
		_, _ = w.Write([]byte(`{"values":[]}`))
	}
}

func newTotalsTestProxy(t *testing.T, backendURL string) http.Handler {
	t.Helper()
	return newTotalsTestProxyMode(t, backendURL, "")
}

func newTotalsTestProxyMode(t *testing.T, backendURL, mode string) http.Handler {
	t.Helper()
	p, err := New(Config{BackendURL: backendURL, Cache: cache.New(time.Hour, 10000), CompatCache: cache.New(time.Hour, 1000), LogLevel: "error",
		QueryStatsMode: mode})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Shutdown(t.Context()) })
	mux := http.NewServeMux()
	p.RegisterRoutes(mux)
	return mux
}

type totalsAnswer struct {
	Data struct {
		Result []struct {
			Values [][]json.RawMessage `json:"values"`
			Value  []json.RawMessage   `json:"value"`
		} `json:"result"`
		Stats struct {
			Summary struct {
				TotalLinesProcessed  int64 `json:"totalLinesProcessed"`
				TotalBytesProcessed  int64 `json:"totalBytesProcessed"`
				TotalPostFilterLines int64 `json:"totalPostFilterLines"`
			} `json:"summary"`
		} `json:"stats"`
	} `json:"data"`
}

func getTotals(t *testing.T, h http.Handler, path string, params url.Values) totalsAnswer {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, path+"?"+params.Encode(), nil)
	req.Header.Set("X-Scope-OrgID", "0")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("%s %v: %d %s", path, params, rec.Code, rec.Body)
	}
	var a totalsAnswer
	if err := json.Unmarshal(rec.Body.Bytes(), &a); err != nil {
		t.Fatal(err)
	}
	return a
}

// sumInside sums a matrix answer's samples whose step (t-step, t] lies wholly
// inside (start, end] (seconds), and all of them when inside is false.
func (a totalsAnswer) sum(t *testing.T, step, start, end int64, inside bool) int64 {
	var n int64
	for _, s := range a.Data.Result {
		for _, v := range s.Values {
			ts, _ := strconv.ParseFloat(string(v[0]), 64)
			var str string
			_ = json.Unmarshal(v[1], &str)
			x, _ := strconv.ParseFloat(str, 64)
			if inside && (int64(ts)-step < start || int64(ts) > end) {
				continue
			}
			n += int64(x)
		}
	}
	return n
}

// TestQueryStatsTotals_MetricAnswerSumsToLokisWindow: a tumbling
// count_over_time answer reports, as Loki does, the lines of its window: the
// sum of its own samples (totalLinesProcessed and totalPostFilterLines without
// a filter, only totalPostFilterLines with one); a sliding one keeps what the
// proxy read. Random windows and steps; the stub's counts differ per step.
//
// conformance: drilldown/logs-footer-query-stats, query-stats-envelope
func TestQueryStatsTotals_MetricAnswerSumsToLokisWindow(t *testing.T) {
	rng := rand.New(rand.NewSource(5))
	stub := &totalsStubVL{rows: 3}
	vl := httptest.NewServer(stub)
	defer vl.Close()
	h := newTotalsTestProxy(t, vl.URL)
	measured := newTotalsTestProxyMode(t, vl.URL, QueryStatsModeVictoriaLogs)
	base := time.Now().Add(-2 * time.Hour).Truncate(time.Hour).Unix()
	for i := 0; i < 12; i++ {
		step := []int64{15, 60, 300}[rng.Intn(3)]
		end := base + int64(rng.Intn(3600)/int(step))*step + int64(i)*7200
		start := end - step*int64(5+rng.Intn(40))
		rng := fmt.Sprintf("%ds", step)
		params := func(q string) url.Values {
			return url.Values{"query": {q}, "start": {fmt.Sprint(start)}, "end": {fmt.Sprint(end)}, "step": {fmt.Sprint(step)}}
		}
		bare := getTotals(t, h, "/loki/api/v1/query_range", params(`sum by (level) (count_over_time({app="api"} | drop __error__ [`+rng+`]))`))
		want := bare.sum(t, step, start, end, false)
		if s := bare.Data.Stats.Summary; want == 0 || s.TotalLinesProcessed != want || s.TotalPostFilterLines != want {
			t.Fatalf("step %d [%d,%d]: lines %d post-filter %d, the answer sums to %d", step, start, end, s.TotalLinesProcessed, s.TotalPostFilterLines, want)
		}
		filtered := getTotals(t, h, "/loki/api/v1/query_range", params(`sum(count_over_time({app="api"} |= "GET" [`+rng+`]))`))
		wantF := filtered.sum(t, step, start, end, false)
		if s := filtered.Data.Stats.Summary; s.TotalPostFilterLines != wantF || s.TotalLinesProcessed != want {
			t.Fatalf("filtered: post-filter %d want %d, lines %d want the selector's %d", s.TotalPostFilterLines, wantF, s.TotalLinesProcessed, want)
		}
		// Sliding windows count a line in about range/step samples: Loki's
		// accounting estimates the window's lines as the sum scaled by
		// step/range; the measured accounting takes none from the answer.
		slidingParams := url.Values{"query": {`sum(count_over_time({app="api"}[` + fmt.Sprintf("%ds", 2*step) + `]))`},
			"start": {fmt.Sprint(start)}, "end": {fmt.Sprint(end)}, "step": {fmt.Sprint(step)}}
		sliding := getTotals(t, h, "/loki/api/v1/query_range", slidingParams)
		est := int64(math.Round(float64(sliding.sum(t, step, start, end, false)) / 2))
		if s := sliding.Data.Stats.Summary; s.TotalPostFilterLines != est || s.TotalLinesProcessed != est {
			t.Fatalf("sliding window: %+v, want the estimate %d (the sum times step/range)", s, est)
		}
		if s := getTotals(t, measured, "/loki/api/v1/query_range", slidingParams).Data.Stats.Summary; s.TotalPostFilterLines != 0 {
			t.Fatalf("victorialogs mode: a sliding window took totals from its answer: %+v", s)
		}
	}
}

// TestQueryStatsTotals_LogQueryFromItsVolumeAnswer: the logs query of a panel
// takes its window's lines from the volume answer of the same expression
// (what Explore sends with it), counting only the steps wholly inside the
// part of its window the volume answer covers; a window covered under 90%
// keeps what the proxy read, and a different expression or tenant never
// matches.
//
// conformance: drilldown/logs-footer-query-stats, query-stats-envelope
func TestQueryStatsTotals_LogQueryFromItsVolumeAnswer(t *testing.T) {
	stub := &totalsStubVL{rows: 9}
	vl := httptest.NewServer(stub)
	defer vl.Close()
	h := newTotalsTestProxy(t, vl.URL)
	end := time.Now().Add(-3 * time.Hour).Truncate(time.Minute).Unix()
	start, step := end-3600, int64(60)
	volume := getTotals(t, h, "/loki/api/v1/query_range", url.Values{
		"query": {`sum by (level, detected_level) (count_over_time({app="api"} |= "x" | drop __error__ [1m]))`},
		"start": {fmt.Sprint(start)}, "end": {fmt.Sprint(end)}, "step": {"60"}})
	logs := func(q string, s, e int64, org string) totalsAnswer {
		req := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+url.Values{"query": {q}, "limit": {"100"},
			"start": {fmt.Sprint(s * 1e9)}, "end": {fmt.Sprint(e * 1e9)}}.Encode(), nil)
		req.Header.Set("X-Scope-OrgID", org)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		var a totalsAnswer
		_ = json.Unmarshal(rec.Body.Bytes(), &a)
		return a
	}
	want := volume.sum(t, step, start, end, true)
	got := logs(`{app="api"} |= "x"`, start, end, "0").Data.Stats.Summary
	if want == 0 || got.TotalPostFilterLines != want {
		t.Fatalf("post-filter lines %d, the volume answer's steps inside the window sum to %d", got.TotalPostFilterLines, want)
	}
	// With a filter and no selector-only count, the lines processed are at
	// least the post-filter lines they hold.
	if got.TotalLinesProcessed != want {
		t.Fatalf("lines processed %d: with a filter and no selector-only count it is the %d post-filter lines", got.TotalLinesProcessed, want)
	}
	// A refresh 5 minutes later: the covered 55 minutes (92%) are reported.
	if g := logs(`{app="api"} |= "x"`, start+300, end+300, "0").Data.Stats.Summary; g.TotalPostFilterLines != volume.sum(t, step, start+300, end, true) {
		t.Fatalf("refresh: %d, want the covered part's %d", g.TotalPostFilterLines, volume.sum(t, step, start+300, end, true))
	}
	for _, tc := range []struct {
		name, query, org string
		s, e             int64
	}{
		{"window past the volume answer", `{app="api"} |= "x"`, "0", start + 600, end + 600},
		{"other expression", `{app="api"} |= "y"`, "0", start, end},
		{"other tenant", `{app="api"} |= "x"`, "1", start, end},
	} {
		// No companion: the post-filter lines are the 9 rows VictoriaLogs
		// returned for the filter.
		if g := logs(tc.query, tc.s, tc.e, tc.org).Data.Stats.Summary; g.TotalPostFilterLines != 9 {
			t.Fatalf("%s: %d post-filter lines, want the 9 rows read (no companion)", tc.name, g.TotalPostFilterLines)
		}
	}
}

// TestQueryStatsTotals_CompanionCoverage replays Grafana-shaped request
// sequences and logs how many logs answers took their totals from a
// companion: Explore's first load (logs and volume queries sent together),
// Explore refreshes (the window moves by 10 s) and a Logs Drilldown service
// page (the same, plus its detected_level volume). One proxy: behind a load
// balancer the two queries of a panel can reach different replicas, which
// read companions through the peer cache.
//
// conformance: query-stats-envelope
func TestQueryStatsTotals_CompanionCoverage(t *testing.T) {
	for _, delay := range []time.Duration{0, 5 * time.Millisecond} {
		stub := &totalsStubVL{rows: 50, delay: delay}
		vl := httptest.NewServer(stub)
		h := newTotalsTestProxy(t, vl.URL)
		const loads = 40
		covered := map[string]int{}
		end := time.Now().Add(-4 * time.Hour).Truncate(time.Minute)
		for i := 0; i < loads; i++ {
			e := end.Add(time.Duration(i) * 7 * time.Minute) // a new window per first load
			for _, phase := range []string{"first load", "refresh", "refresh 2"} {
				if phase != "first load" {
					e = e.Add(10 * time.Second)
				}
				s := e.Add(-time.Hour)
				sel := `{app="api"}`
				var wg sync.WaitGroup
				var logsAns totalsAnswer
				wg.Add(2)
				go func() {
					defer wg.Done()
					logsAns = getTotals(t, h, "/loki/api/v1/query_range", url.Values{"query": {sel}, "limit": {"1000"}, "direction": {"backward"},
						"start": {fmt.Sprint(s.UnixNano())}, "end": {fmt.Sprint(e.UnixNano())}})
				}()
				go func() {
					defer wg.Done()
					getTotals(t, h, "/loki/api/v1/query_range", url.Values{"query": {`sum by (level, detected_level) (count_over_time(` + sel + ` | drop __error__ [5s]))`},
						"start": {fmt.Sprint(s.UnixNano())}, "end": {fmt.Sprint(e.UnixNano())}, "step": {"5s"}})
				}()
				wg.Wait()
				if logsAns.Data.Stats.Summary.TotalLinesProcessed != 50 {
					covered[phase]++
				}
			}
		}
		// Logs Drilldown service page: volume_range by detected_level, the
		// detected_level count volume and the logs query, together.
		base := getTotals(t, h, "/loki/api/v1/query_range", url.Values{"query": {`{app="drill"}`}, "limit": {"1000"},
			"start": {fmt.Sprint(end.Add(-30 * time.Hour).UnixNano())}, "end": {fmt.Sprint(end.Add(-29 * time.Hour).UnixNano())}}).Data.Stats.Summary
		for i := 0; i < loads; i++ {
			e := end.Add(-time.Duration(i+1) * 7 * time.Minute)
			for _, phase := range []string{"drilldown first load", "drilldown refresh"} {
				if phase != "drilldown first load" {
					e = e.Add(10 * time.Second)
				}
				s := e.Add(-time.Hour)
				var wg sync.WaitGroup
				var logsAns totalsAnswer
				wg.Add(3)
				go func() {
					defer wg.Done()
					logsAns = getTotals(t, h, "/loki/api/v1/query_range", url.Values{"query": {`{app="drill"}`}, "limit": {"1000"},
						"start": {fmt.Sprint(s.UnixNano())}, "end": {fmt.Sprint(e.UnixNano())}})
				}()
				go func() {
					defer wg.Done()
					getTotals(t, h, "/loki/api/v1/index/volume_range", url.Values{"query": {`{app="drill"}`}, "targetLabels": {"detected_level"},
						"start": {fmt.Sprint(s.UnixNano())}, "end": {fmt.Sprint(e.UnixNano())}, "step": {"60"}})
				}()
				go func() {
					defer wg.Done()
					getTotals(t, h, "/loki/api/v1/query_range", url.Values{"query": {`sum by (detected_level) (count_over_time({app="drill"} | drop __error__ [60s]))`},
						"start": {fmt.Sprint(s.UnixNano())}, "end": {fmt.Sprint(e.UnixNano())}, "step": {"60"}})
				}()
				wg.Wait()
				if logsAns.Data.Stats.Summary.TotalLinesProcessed != base.TotalLinesProcessed {
					covered[phase+" lines"]++
				}
				if logsAns.Data.Stats.Summary.TotalBytesProcessed != base.TotalBytesProcessed {
					covered[phase+" bytes"]++
				}
			}
		}
		vl.Close()
		t.Logf("volume answer delay %v: Explore logs answers with companion totals: first load %d/%d, refresh %d/%d, second refresh %d/%d",
			delay, covered["first load"], loads, covered["refresh"], loads, covered["refresh 2"], loads)
		t.Logf("volume answer delay %v: Drilldown logs answers with companion lines/bytes: first load %d/%d and %d/%d, refresh %d/%d and %d/%d",
			delay, covered["drilldown first load lines"], loads, covered["drilldown first load bytes"], loads,
			covered["drilldown refresh lines"], loads, covered["drilldown refresh bytes"], loads)
	}
}

// TestStatsTotalsStepsWithin: a window is summed from the companion's whole
// steps inside it, and refused when those span under 90% of it: a coarse
// companion never answers a short window with 0.
//
// conformance: query-stats-envelope
func TestStatsTotalsStepsWithin(t *testing.T) {
	min := int64(time.Minute)
	fine := statsTotalsSteps{step: min, start: 0, end: 60 * min}
	for i := int64(1); i <= 60; i++ {
		fine.ts, fine.vals = append(fine.ts, i*min), append(fine.vals, float64(i))
	}
	coarse := statsTotalsSteps{step: 60 * min, start: 0, end: 60 * min, ts: []int64{60 * min}, vals: []float64{1830}}
	for _, tc := range []struct {
		name       string
		s          statsTotalsSteps
		start, end int64
		want       float64
		ok         bool
	}{
		{"whole window", fine, 0, 60 * min, 1830, true},
		{"refresh 5m later", fine, 5 * min, 65 * min, 1830 - 15, true},
		{"refresh 10m later", fine, 10 * min, 70 * min, 0, false},
		{"unaligned edges", fine, 30*1e9 + 0, 60 * min, 1830 - 1, true},
		{"coarse step, short window", coarse, 10 * min, 25 * min, 0, false},
		{"coarse step, its own window", coarse, 0, 60 * min, 1830, true},
	} {
		got, ok := tc.s.within(tc.start, tc.end)
		if ok != tc.ok || (ok && got != tc.want) {
			t.Fatalf("%s: %v %t, want %v %t", tc.name, got, ok, tc.want, tc.ok)
		}
	}
}

// TestQueryStatsTotals_CompanionsStayInTheirTenant: a companion is found by
// the same tenant only, and an instant count of the same expression neither
// replaces nor zeroes the panel's companion.
//
// conformance: query-stats-envelope
func TestQueryStatsTotals_CompanionsStayInTheirTenant(t *testing.T) {
	stub := &totalsStubVL{rows: 9}
	vl := httptest.NewServer(stub)
	defer vl.Close()
	_, _, mux := newTwoTenantProxy(t, vl.URL)
	end := time.Now().Add(-6 * time.Hour).Truncate(time.Minute).Unix()
	start := end - 1800
	get := func(path, org string, params url.Values) totalsAnswer {
		req := httptest.NewRequest(http.MethodGet, path+"?"+params.Encode(), nil)
		req.Header.Set("X-Scope-OrgID", org)
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("%s %s: %d %s", org, path, rec.Code, rec.Body)
		}
		var a totalsAnswer
		_ = json.Unmarshal(rec.Body.Bytes(), &a)
		return a
	}
	vol := get("/loki/api/v1/query_range", "tenant-a", url.Values{"query": {`sum(count_over_time({app="api"} | drop __error__ [60s]))`},
		"start": {fmt.Sprint(start)}, "end": {fmt.Sprint(end)}, "step": {"60"}})
	want := vol.sum(t, 60, start, end, true)
	get("/loki/api/v1/query", "tenant-a", url.Values{"query": {`sum(count_over_time({app="api"}[30m]))`}, "time": {fmt.Sprint(end)}})
	logs := url.Values{"query": {`{app="api"}`}, "limit": {"100"}, "start": {fmt.Sprint(start * 1e9)}, "end": {fmt.Sprint(end * 1e9)}}
	own := get("/loki/api/v1/query_range", "tenant-a", logs).Data.Stats.Summary
	if want == 0 || own.TotalLinesProcessed != want || own.TotalBytesProcessed <= 0 {
		t.Fatalf("tenant-a: lines %d (want %d), bytes %d", own.TotalLinesProcessed, want, own.TotalBytesProcessed)
	}
	other := get("/loki/api/v1/query_range", "tenant-b", logs).Data.Stats.Summary
	if other.TotalLinesProcessed != 9 || other.TotalBytesProcessed == own.TotalBytesProcessed {
		t.Fatalf("tenant-b took tenant-a's companions: lines %d bytes %d", other.TotalLinesProcessed, other.TotalBytesProcessed)
	}
}
