package proxy

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// LogQL has no subquery grammar. Loki v3.7 answers every [range:step] form with
// HTTP 400 at parse time, so the proxy must reject them before any backend call
// instead of evaluating them step by step against VictoriaLogs.
func TestSubquery_RejectedLikeLoki(t *testing.T) {
	var calls atomic.Int64
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"success","data":{"resultType":"vector","result":[]}}`))
	}))
	defer backend.Close()
	p := newTestProxy(t, backend.URL)

	cases := []struct {
		query string
		want  string
	}{
		{`max_over_time(sum(rate({app="nginx"}[1m]))[30m:5m])`, "parse error at line 1, col 15: syntax error: unexpected SUM, expecting NUMBER or { or ("},
		{`max_over_time(rate({app="nginx"}[1m])[30m:5m])`, "parse error at line 1, col 15: syntax error: unexpected RATE, expecting NUMBER or { or ("},
		{`quantile_over_time(0.5, rate({app="nginx"}[1m])[30m:5m])`, "parse error at line 1, col 25: syntax error: unexpected RATE, expecting { or ("},
		{`max_over_time((rate({app="nginx"}[1m]))[30m:5m])`, "parse error at line 1, col 16: syntax error: unexpected RATE, expecting { or ("},
		{`sum(max_over_time(rate({app="nginx"}[1m])[30m:5m]))`, "parse error at line 1, col 19: syntax error: unexpected RATE, expecting NUMBER or { or ("},
		{`count_over_time({app="nginx"}[30m:5m])`, `unknown unit "m:" in duration "30m:5m"`},
	}
	for _, tc := range cases {
		for _, target := range []string{
			"/loki/api/v1/query?time=1609462800&query=" + url.QueryEscape(tc.query),
			"/loki/api/v1/query_range?start=1609459200&end=1609462800&step=60&query=" + url.QueryEscape(tc.query),
		} {
			t.Run(target, func(t *testing.T) {
				w := doCompatProxyRequest(p, target, map[string]string{"X-Scope-OrgID": "0"})
				if w.Code != http.StatusBadRequest {
					t.Fatalf("status=%d body=%s", w.Code, w.Body)
				}
				var body struct{ ErrorType, Error string }
				if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil || body.ErrorType != "bad_data" || !strings.Contains(body.Error, tc.want) {
					t.Fatalf("body=%s, want bad_data error containing %q", w.Body, tc.want)
				}
			})
		}
	}
	if calls.Load() != 0 {
		t.Fatalf("rejected subqueries reached the backend %d times", calls.Load())
	}
}

// Loki v3.7 (loghttp.ParseRangeQuery) rejects a query_range whose
// (end-start)/step exceeds 11,000 points before it parses the query, so the
// resolution error wins over every parse and validation error; with a valid
// step the validation error is reported. Verified against Loki 3.7.1.
func TestQueryRange_ResolutionLimitPrecedesQueryValidation(t *testing.T) {
	var calls atomic.Int64
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		http.Error(w, "unexpected backend call", http.StatusTeapot)
	}))
	defer backend.Close()
	p := newTestProxy(t, backend.URL)

	cases := []struct{ query, validationErr string }{
		{`max_over_time(rate({app="a"}[1m])[30m:5m])`, "parse error at line 1, col 15: syntax error: unexpected RATE, expecting NUMBER or { or ("},
		{`{app=""}`, `parse error : queries require at least one regexp or equality matcher that does not have an empty-compatible value. For instance, app=~".*" does not meet this requirement, but app=~".+" will`},
		{`topk(0, count_over_time({app="a"}[5m]))`, "parse error : invalid parameter (must be greater than 0) topk(0"},
		{`{app="a"} | json foo="bar["`, `parse error : stage '| json foo="bar["' : cannot parse expression [bar[]: syntax error: unexpected $end, expecting STRING or INDEX`},
	}
	for _, tc := range cases {
		for _, step := range []struct{ value, want string }{{"0.05", errLokiStepTooSmall}, {"60", tc.validationErr}} {
			params := url.Values{"query": {tc.query}, "start": {"1789449300"}, "end": {"1789450381"}, "step": {step.value}}
			t.Run(step.value+"/"+tc.query, func(t *testing.T) {
				w := doCompatProxyRequest(p, "/loki/api/v1/query_range?"+params.Encode(), map[string]string{"X-Scope-OrgID": "0"})
				var body struct{ ErrorType, Error string }
				if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil || w.Code != http.StatusBadRequest || body.ErrorType != "bad_data" || body.Error != step.want {
					t.Fatalf("status=%d body=%s, want 400 %q", w.Code, w.Body, step.want)
				}
			})
		}
	}
	if calls.Load() != 0 {
		t.Fatalf("rejected queries reached the backend %d times", calls.Load())
	}
}

func TestMetricEvalPointCountBounds(t *testing.T) {
	start := time.Unix(1700000000, 0)
	for _, step := range []time.Duration{0, -time.Second, time.Nanosecond, time.Second} {
		if _, err := metricEvalPointCount(start, start.Add(365*24*time.Hour), step); err == nil {
			t.Fatalf("unbounded range accepted with step %s", step)
		}
	}
	if _, err := metricEvalPointCount(start, start.Add(-time.Second), time.Second); err == nil {
		t.Fatal("inverted window accepted")
	}
	if n, err := metricEvalPointCount(start, start.Add(time.Minute), 10*time.Second); err != nil || n != 7 {
		t.Fatalf("points=%d err=%v", n, err)
	}
	// Loki accepts (end-start)/step up to lokiMaxPointsPerSeries inclusive.
	if n, err := metricEvalPointCount(start, start.Add(lokiMaxPointsPerSeries*time.Second), time.Second); err != nil || n != lokiMaxPointsPerSeries+1 {
		t.Fatalf("Loki-accepted resolution rejected: points=%d err=%v", n, err)
	}
	if _, err := metricEvalPointCount(start, start.Add((lokiMaxPointsPerSeries+1)*time.Second), time.Second); err == nil {
		t.Fatal("resolution above Loki's limit accepted")
	}
}

// A Grafana logs volume chunk over 24h at an 8s step has 10,800 points. Loki
// answers it, so the ordered JSON metric route must not reject it.
func TestOrderedJSONMetricTimesAcceptsLokiResolution(t *testing.T) {
	end := int64(1789450000)
	for _, tc := range []struct {
		start, step string
		ok          bool
	}{
		{strconv.FormatInt(end-86400, 10), "8", true},
		{strconv.FormatInt(end-lokiMaxPointsPerSeries, 10), "1", true},
		{strconv.FormatInt(end-lokiMaxPointsPerSeries-1, 10), "1", false},
	} {
		params := url.Values{"start": {tc.start}, "end": {strconv.FormatInt(end, 10)}, "step": {tc.step}}
		r := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+params.Encode(), nil)
		_, _, _, err := orderedJSONMetricTimes(r, true)
		if tc.ok && err != nil {
			t.Fatalf("start=%s step=%s rejected: %v", tc.start, tc.step, err)
		}
		if !tc.ok && (err == nil || err.Error() != errLokiStepTooSmall) {
			t.Fatalf("start=%s step=%s err=%v, want %q", tc.start, tc.step, err, errLokiStepTooSmall)
		}
	}
}

func TestParseLokiDuration(t *testing.T) {
	tests := []struct {
		input string
		want  time.Duration
	}{
		{"5m", 5 * time.Minute},
		{"1h", time.Hour},
		{"30s", 30 * time.Second},
		{"1d", 24 * time.Hour},
		{"2h", 2 * time.Hour},
		{"10m", 10 * time.Minute},
		{"1h30m", 90 * time.Minute},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := parseLokiDuration(tt.input)
			if got != tt.want {
				t.Errorf("parseLokiDuration(%q) = %v, want %v", tt.input, got, tt.want)
			}
		})
	}
}

// BenchmarkParseLokiDuration benchmarks duration parsing.
func BenchmarkParseLokiDuration(b *testing.B) {
	durations := []string{"5m", "1h", "30s", "1d", "2h30m"}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		for _, d := range durations {
			parseLokiDuration(d)
		}
	}
}

func TestParseTimestamp(t *testing.T) {
	tests := []struct {
		input  string
		wantOk bool
	}{
		{"1609459200", true},
		{"1609459200.123", true},
		{"2021-01-01T00:00:00Z", true},
		{"", true}, // defaults to now
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			_, err := parseTimestamp(tt.input)
			if tt.wantOk && err != nil {
				t.Errorf("parseTimestamp(%q) error: %v", tt.input, err)
			}
		})
	}
}

func TestParseTimestamp_MillisecondRegression(t *testing.T) {
	// Millisecond timestamps (13-digit) must not be interpreted as seconds (year ~58366).
	anchor := time.Date(2025, 5, 25, 0, 0, 0, 0, time.UTC)
	cases := []struct {
		name  string
		input string
		want  time.Time
	}{
		{"seconds", strconv.FormatInt(anchor.Unix(), 10), anchor},
		{"milliseconds", strconv.FormatInt(anchor.UnixMilli(), 10), anchor},
		{"nanoseconds", strconv.FormatInt(anchor.UnixNano(), 10), anchor},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseTimestamp(tc.input)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !got.Equal(tc.want) {
				t.Errorf("parseTimestamp(%q) = %v, want %v", tc.input, got, tc.want)
			}
			// Guard: result must not be in year 58366 (ms-as-seconds bug)
			if got.Year() > 3000 {
				t.Errorf("parseTimestamp(%q) produced year %d — ms-as-seconds bug", tc.input, got.Year())
			}
		})
	}
}
