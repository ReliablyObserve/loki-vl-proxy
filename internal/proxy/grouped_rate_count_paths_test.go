package proxy

import (
	"encoding/json"
	"fmt"
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

// groupedRateFakeVL emulates VictoriaLogs stats_query_range for the query shapes
// the proxy emits for single-field grouped range metrics. Every group value
// logs at a constant line rate, so each epoch-aligned bucket of width step
// holds rate*step lines (each line lineBytes long). A `| math __lvp_inner/N`
// stage divides the bucket value by N, `field:in(...)` restricts the groups
// and the grouped field name is taken from the first `stats by (...)`.
type groupedRateFakeVL struct {
	mu      sync.Mutex
	queries []string
	paths   []string
}

const groupedRateLineBytes = 10

var (
	groupedRateLineRates = map[string]float64{"api": 4, "web": 1}
	groupedRateMathRE    = regexp.MustCompile(`\|\s*math\s+__lvp_inner\s*/\s*([0-9.]+)`)
	groupedRateByRE      = regexp.MustCompile(`\|\s*stats\s+by\s*\(\s*"?([A-Za-z0-9_.]+)"?`)
	groupedRateInRE      = regexp.MustCompile(`:in\(([^)]*)\)`)
)

func (f *groupedRateFakeVL) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	_ = r.ParseForm()
	f.mu.Lock()
	f.paths = append(f.paths, r.URL.Path)
	f.queries = append(f.queries, r.FormValue("query"))
	f.mu.Unlock()
	if r.URL.Path != "/select/logsql/stats_query_range" {
		http.Error(w, "unexpected endpoint "+r.URL.Path, http.StatusInternalServerError)
		return
	}
	q := r.FormValue("query")
	from, ok1 := parseLokiTimeToUnixNano(r.FormValue("start"))
	to, ok2 := parseLokiTimeToUnixNano(r.FormValue("end"))
	step, ok3 := parsePositiveStepDuration(r.FormValue("step"))
	by := groupedRateByRE.FindStringSubmatch(q)
	if !ok1 || !ok2 || !ok3 || len(by) != 2 {
		http.Error(w, "unsupported request "+q, http.StatusBadRequest)
		return
	}
	perLine := 1.0
	if strings.Contains(q, "sum_len(_msg)") {
		perLine = groupedRateLineBytes
	}
	divisor := 1.0
	if m := groupedRateMathRE.FindStringSubmatch(q); len(m) == 2 {
		divisor, _ = strconv.ParseFloat(m[1], 64)
	}
	allowed := map[string]bool{}
	if m := groupedRateInRE.FindStringSubmatch(q); len(m) == 2 {
		for _, v := range strings.Split(m[1], ",") {
			allowed[strings.Trim(strings.TrimSpace(v), `"`)] = true
		}
	}
	stepSec := int64(step / time.Second)
	fromSec, toSec := from/int64(time.Second), to/int64(time.Second)
	values := make([]string, 0, len(groupedRateLineRates))
	for v := range groupedRateLineRates {
		if len(allowed) == 0 || allowed[v] {
			values = append(values, v)
		}
	}
	sort.Strings(values)
	series := make([]string, 0, len(values))
	for _, v := range values {
		bucket := groupedRateLineRates[v] * float64(stepSec) * perLine / divisor
		var points []string
		for ts := fromSec - fromSec%stepSec; ts <= toSec; ts += stepSec {
			points = append(points, fmt.Sprintf(`[%d,%q]`, ts, strconv.FormatFloat(bucket, 'f', -1, 64)))
		}
		series = append(series, fmt.Sprintf(`{"metric":{%q:%q},"values":[%s]}`, by[1], v, strings.Join(points, ",")))
	}
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, `{"status":"success","data":{"resultType":"matrix","result":[%s]}}`, strings.Join(series, ","))
}

func (f *groupedRateFakeVL) snapshot() (paths, queries []string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.paths...), append([]string(nil), f.queries...)
}

// Loki evaluates rate and bytes_rate per second of the range window, so a
// grouped `sum by (app) (rate({...}[5m]))` over data logging 4 lines/s returns 4
// at every step — regardless of how long the requested range is. The
// single-field count fast paths (two-phase top-N and windowed /hits) rebuild
// the query as a bare `| stats by (field) count()`; they must only serve genuine
// count queries, never rate-like ones whose per-second division lives in a later
// `| math` stage.
func TestGroupedRangeMetricSingleFieldLongRangeMatchesLoki(t *testing.T) {
	end := time.Unix(1700006400, 0) // multiple of 3600
	cases := []struct {
		name      string
		query     string
		step      time.Duration
		rangeDur  time.Duration
		grafana   bool
		field     string
		want      map[string]float64
		twoPhase  bool // Drilldown count: the bounded two-phase path serves it
		forbidHit bool
	}{
		{name: "rate 5m step 300 over 6h", query: `sum by (app) (rate({namespace="prod"}[5m]))`, step: 5 * time.Minute, rangeDur: 6 * time.Hour, field: "app", want: map[string]float64{"api": 4, "web": 1}},
		{name: "rate 1h step 3600 over 24h", query: `sum by (app) (rate({namespace="prod"}[1h]))`, step: time.Hour, rangeDur: 24 * time.Hour, field: "app", want: map[string]float64{"api": 4, "web": 1}},
		{name: "rate with existence filter over 6h", query: `sum by (app) (rate({namespace="prod", app!=""}[5m]))`, step: 5 * time.Minute, rangeDur: 6 * time.Hour, field: "app", want: map[string]float64{"api": 4, "web": 1}},
		{name: "bytes_rate 5m step 300 over 6h", query: `sum by (app) (bytes_rate({namespace="prod"}[5m]))`, step: 5 * time.Minute, rangeDur: 6 * time.Hour, field: "app", want: map[string]float64{"api": 4 * groupedRateLineBytes, "web": groupedRateLineBytes}},
		{name: "grafana high-card rate over 6h", query: `sum by (trace_id) (rate({namespace="prod"}[5m]))`, step: 5 * time.Minute, rangeDur: 6 * time.Hour, grafana: true, field: "trace_id", want: map[string]float64{"api": 4, "web": 1}, forbidHit: true},
		{name: "count_over_time over 6h", query: `sum by (app) (count_over_time({namespace="prod"}[5m]))`, step: 5 * time.Minute, rangeDur: 6 * time.Hour, field: "app", want: map[string]float64{"api": 1200, "web": 300}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fake := &groupedRateFakeVL{}
			backend := httptest.NewServer(fake)
			defer backend.Close()
			p := newTestProxy(t, backend.URL)

			start := end.Add(-tc.rangeDur)
			params := url.Values{
				"query": {tc.query},
				"start": {strconv.FormatInt(start.Unix(), 10)},
				"end":   {strconv.FormatInt(end.Unix(), 10)},
				"step":  {strconv.FormatInt(int64(tc.step/time.Second), 10)},
			}
			req := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+params.Encode(), nil)
			if tc.grafana {
				req.Header.Set("User-Agent", "Grafana/12.0.0")
			}
			rec := httptest.NewRecorder()
			p.handleQueryRange(rec, req)
			if rec.Code != http.StatusOK {
				t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
			}
			var resp struct {
				Data struct {
					ResultType string `json:"resultType"`
					Result     []struct {
						Metric map[string]string `json:"metric"`
						Values [][]interface{}   `json:"values"`
					} `json:"result"`
				} `json:"data"`
			}
			if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
				t.Fatalf("decode %s: %v", rec.Body.String(), err)
			}
			if resp.Data.ResultType != "matrix" || len(resp.Data.Result) != len(tc.want) {
				t.Fatalf("want %d matrix series, got %s", len(tc.want), rec.Body.String())
			}
			wantPoints := int(tc.rangeDur/tc.step) + 1
			for _, s := range resp.Data.Result {
				if len(s.Metric) != 1 {
					t.Fatalf("series labels %v, want only %q", s.Metric, tc.field)
				}
				want, ok := tc.want[s.Metric[tc.field]]
				if !ok {
					t.Fatalf("unexpected series %v", s.Metric)
				}
				if len(s.Values) != wantPoints {
					t.Fatalf("series %v has %d points, want %d", s.Metric, len(s.Values), wantPoints)
				}
				for _, pt := range s.Values {
					got, err := strconv.ParseFloat(fmt.Sprint(pt[1]), 64)
					if err != nil || got != want {
						t.Fatalf("series %v point %v = %v, want %v (Loki per-window value)", s.Metric, pt[0], pt[1], want)
					}
				}
			}

			paths, queries := fake.snapshot()
			sawTwoPhase := false
			for i, q := range queries {
				if tc.forbidHit && paths[i] != "/select/logsql/stats_query_range" {
					t.Errorf("rate query must not use the count-only %s path", paths[i])
				}
				if strings.Contains(q, "sort by (_c desc)") {
					sawTwoPhase = true
				}
			}
			if sawTwoPhase != tc.twoPhase {
				t.Errorf("two-phase top-N used=%v, want %v; backend queries: %q", sawTwoPhase, tc.twoPhase, queries)
			}
		})
	}
}

// The Drilldown single-field detectors and the count fast paths share one shape
// gate: only a bare `| stats by (field) count()` may be rebuilt as a count.
func TestParseSingleFieldCountSpecRejectsRateLikeTranslations(t *testing.T) {
	cases := []struct {
		query string
		want  bool
	}{
		{`namespace:="prod" | stats by (app) count()`, true},
		{`namespace:="prod" app:!"" | stats by (app) count()`, true},
		{`namespace:="prod" | unpack_logfmt | stats by (pod) count()`, true},
		{`namespace:="prod" | stats by (app) count() as __lvp_inner | math __lvp_inner/300 as __lvp_rate | stats by (app) sum(__lvp_rate)`, false},
		{`namespace:="prod" app:!"" | stats by (app) count() as __lvp_inner | math __lvp_inner/60 as __lvp_rate | stats by (app) sum(__lvp_rate)`, false},
		{`namespace:="prod" | stats by (app) sum_len(_msg)`, false},
		{`namespace:="prod" | stats by (app, pod) count()`, false},
		{`namespace:="prod" | stats by (app) count() as _c | sort by (_c desc) | limit 500`, false},
	}
	for _, tc := range cases {
		if _, got := parseSingleFieldCountSpec(tc.query); got != tc.want {
			t.Errorf("parseSingleFieldCountSpec(%q) = %v, want %v", tc.query, got, tc.want)
		}
	}
	rate := `namespace:="prod" app:!"" | stats by (app) count() as __lvp_inner | math __lvp_inner/60 as __lvp_rate | stats by (app) sum(__lvp_rate)`
	if _, _, ok := detectDrilldownSingleField(rate); ok {
		t.Errorf("detectDrilldownSingleField must not treat a rate translation as a count: %q", rate)
	}
	if _, _, ok := detectDrilldownSingleFieldWithParser(rate); ok {
		t.Errorf("detectDrilldownSingleFieldWithParser must not treat a rate translation as a count: %q", rate)
	}
}
