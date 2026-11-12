package proxy

import (
	"bufio"
	"context"
	"encoding/json"
	"math"
	"math/big"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// Bounds checks for request times and offsets: a request whose window, moved
// by its offset and widened by its range, leaves the range int64 nanoseconds
// can hold is refused, and every loop that steps through time is safe on its
// own. Each request below must be answered within timeBoundsBudget; a request
// that is not fails its test.
const timeBoundsBudget = 2 * time.Second

// timeBoundsBackend is a VictoriaLogs that holds no data and records the time
// parameters of the calls it receives.
type timeBoundsBackend struct {
	mu    sync.Mutex
	calls []url.Values
}

func (b *timeBoundsBackend) handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		b.mu.Lock()
		b.calls = append(b.calls, r.Form)
		b.mu.Unlock()
		switch {
		case strings.HasPrefix(r.URL.Path, "/select/logsql/stats_query_range"):
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"status":"success","data":{"resultType":"matrix","result":[]}}`))
		case strings.HasPrefix(r.URL.Path, "/select/logsql/stats_query"):
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"status":"success","data":{"resultType":"vector","result":[]}}`))
		}
	})
}

// newTimeBoundsProxy is a proxy over a backend with the response caches and the
// step alignment on, the layers a request passes before the check.
func newTimeBoundsProxy(backendURL string) (*Proxy, error) {
	return New(Config{
		BackendURL:           backendURL,
		Cache:                cache.New(60*time.Second, 1000),
		CompatCache:          cache.New(60*time.Second, 1000),
		LogLevel:             "error",
		AlignQueriesWithStep: true,
	})
}

// timeBoundsProxy is a proxy over an empty backend.
func timeBoundsProxy(t *testing.T) (*Proxy, *timeBoundsBackend) {
	t.Helper()
	backend := &timeBoundsBackend{}
	server := httptest.NewServer(backend.handler())
	t.Cleanup(server.Close)
	p, err := newTimeBoundsProxy(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	return p, backend
}

// answerWithinBudget sends the request through the proxy's routes and fails
// the test when the answer takes longer than timeBoundsBudget.
func answerWithinBudget(t *testing.T, p *Proxy, target string) *httptest.ResponseRecorder {
	t.Helper()
	mux := http.NewServeMux()
	p.RegisterRoutes(mux)
	done := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		req := httptest.NewRequest(http.MethodGet, target, nil)
		req.Header.Set("X-Scope-OrgID", "0")
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		done <- rec
	}()
	select {
	case rec := <-done:
		return rec
	case <-time.After(timeBoundsBudget):
		t.Fatalf("no answer within %s: %s", timeBoundsBudget, target)
		return nil
	}
}

func timeBoundsTarget(endpoint string, params map[string]string) string {
	values := url.Values{}
	for k, v := range params {
		values.Set(k, v)
	}
	return "/loki/api/v1/" + endpoint + "?" + values.Encode()
}

func timeBoundsErrorText(t testing.TB, rec *httptest.ResponseRecorder) string {
	t.Helper()
	return readErrorRec(t, rec).Error
}

// The function families the check and the loops are held to: count_over_time,
// rate, absent_over_time (alone, under an aggregation and under label_replace),
// the same functions over a parser, a function over an unwrapped label, a
// constant expression and a log query. Offsets are written into the selector.
var timeBoundsQueries = map[string]string{
	"absent":   `absent_over_time({app="x"}[1m]%s)`,
	"count":    `count_over_time({app="x"}[1m]%s)`,
	"rate":     `rate({app="x"}[1m]%s)`,
	"sum":      `sum by (app) (count_over_time({app="x"}[1m]%s))`,
	"label":    `label_replace(absent_over_time({app="x"}[1m]%s), "dst", "$1", "app", "(.*)")`,
	"json":     `count_over_time({app="x"} | json [1m]%s)`,
	"logfmt":   `rate({app="x"} | logfmt | level="error" [1m]%s)`,
	"unwrap":   `sum_over_time({app="x"} | logfmt | unwrap n [1m]%s)`,
	"unwrapby": `sum by (app) (avg_over_time({app="x"} | logfmt | unwrap n [1m]%s))`,
	"binary":   `count_over_time({app="x"}[1m]%s) + 1`,
	"constant": `1 + 1`,
	"log":      `{app="x"}`,
}

func timeBoundsQuery(family, offset string) string {
	if offset != "" {
		offset = " offset " + offset
	}
	return strings.ReplaceAll(timeBoundsQueries[family], "%s", offset)
}

// The request layer refuses a window outside the range and answers one inside
// it, for the instant and the range endpoint, every function family, and
// offsets of both signs near both limits.
//
// conformance: semantics/request-time-bounds
func TestQueryRangeRefusesWindowsOutsideTheTimeRange(t *testing.T) {
	p, _ := timeBoundsProxy(t)
	const hour = int64(time.Hour)
	now := time.Now().UnixNano()
	maxNs, minNs := int64(math.MaxInt64), int64(math.MinInt64)
	reqMax, reqMin := int64(maxRequestTimeNs), int64(minRequestTimeNs)
	minute := int64(time.Minute)
	cases := []struct {
		name       string
		offset     string
		start, end int64
		refused    bool
		rangeOnly  bool // the start of an instant query is its time, which is the end
	}{
		{name: "no offset, now", start: now - hour, end: now},
		{name: "year 2200", start: 7258118400000000000 - hour, end: 7258118400000000000},
		{name: "year 1700", start: -8520336000000000000, end: -8520336000000000000 + hour},
		{name: "zero", start: 0, end: hour},
		{name: "end at the top of int64", start: maxNs - hour, end: maxNs, refused: true},
		{name: "end ten minutes under the top", start: maxNs - hour, end: maxNs - 10*minute, refused: true},
		{name: "end an hour under the top", start: maxNs - 2*hour, end: maxNs - hour},
		{name: "start at the bottom of int64", start: minNs, end: minNs + hour, refused: true, rangeOnly: true},
		{name: "offset 1h", offset: "1h", start: now - hour, end: now},
		{name: "offset -1h", offset: "-1h", start: now - hour, end: now},
		{name: "offset 106751d, now", offset: "106751d", start: now - hour, end: now},
		{name: "offset 2562047h, now", offset: "2562047h", start: now - hour, end: now},
		{name: "offset 2562047h, zero", offset: "2562047h", start: 0, end: hour},
		{name: "offset 7456146435854775806ns, now", offset: "7456146435854775806ns", start: now - hour, end: now},
		{name: "offset 7456146435854775806ns, zero", offset: "7456146435854775806ns", start: 0, end: hour},
		{name: "offset 9223372036854775807ns, now", offset: "9223372036854775807ns", start: now - hour, end: now},
		{name: "offset 9223372036854775807ns, zero", offset: "9223372036854775807ns", start: 0, end: hour, refused: true, rangeOnly: true},
		{name: "offset -2562047h, now", offset: "-2562047h", start: now - hour, end: now, refused: true},
		{name: "offset -2562047h, zero", offset: "-2562047h", start: 0, end: hour, refused: true},
		{name: "offset -7456146435854775806ns, now", offset: "-7456146435854775806ns", start: now - hour, end: now, refused: true},
		{name: "offset -7456146435854775806ns, zero", offset: "-7456146435854775806ns", start: 0, end: hour},
		{name: "offset -7456146435854775806ns, end 2026-01-01T00:00:01Z", offset: "-7456146435854775806ns", start: 1767225601000000000 - hour, end: 1767225601000000000, refused: true},
		{name: "offset -9223372036854775807ns, zero", offset: "-9223372036854775807ns", start: 0, end: hour, refused: true},
		{name: "offset 1h, end at the top", offset: "1h", start: maxNs - hour, end: maxNs, refused: true},
		{name: "offset -1h moves the end past the top of the range", offset: "-1h", start: reqMax - 2*hour, end: reqMax - 30*minute, refused: true},
		{name: "offset -1h keeps the end inside the range", offset: "-1h", start: reqMax - 3*hour, end: reqMax - 2*hour},
		{name: "offset 1h moves the start past the bottom of the range", offset: "1h", start: reqMin + 30*minute, end: reqMin + 2*hour, refused: true, rangeOnly: true},
		{name: "offset 1h keeps the start inside the range", offset: "1h", start: reqMin + 2*hour, end: reqMin + 3*hour},
	}
	for family := range timeBoundsQueries {
		noSelector := family == "constant" || family == "log" // nothing for an offset to move
		for _, tc := range cases {
			if noSelector && tc.offset != "" {
				continue
			}
			t.Run(family+"/range/"+tc.name, func(t *testing.T) {
				rec := answerWithinBudget(t, p, timeBoundsTarget("query_range", map[string]string{
					"query": timeBoundsQuery(family, tc.offset), "start": timeParam(tc.start), "end": timeParam(tc.end), "step": "60s",
				}))
				assertTimeBoundsAnswer(t, rec, tc.refused)
			})
			if tc.rangeOnly || family == "log" { // Loki refuses a log query at the instant endpoint
				continue
			}
			t.Run(family+"/instant/"+tc.name, func(t *testing.T) {
				rec := answerWithinBudget(t, p, timeBoundsTarget("query", map[string]string{
					"query": timeBoundsQuery(family, tc.offset), "time": timeParam(tc.end),
				}))
				assertTimeBoundsAnswer(t, rec, tc.refused)
			})
		}
	}
}

// The handlers answer a window outside the range within the budget when they
// are called without the request layer in front of them.
//
// conformance: semantics/request-time-bounds
func TestHandlersAnswerWindowsOutsideTheTimeRange(t *testing.T) {
	p, _ := timeBoundsProxy(t)
	maxNs := int64(math.MaxInt64)
	for _, tc := range []struct {
		name, query string
		start, end  int64
		step        string
	}{
		{"absent, offset -7456146435854775806ns, end 2026-01-01T00:00:01Z", `absent_over_time({app="x"}[1m] offset -7456146435854775806ns)`, 1767225601000000000 - int64(time.Hour), 1767225601000000000, "60s"},
		{"absent, end at the top", `absent_over_time({app="x"}[1m])`, maxNs - int64(time.Hour), maxNs, "60s"},
		{"absent, end at the top, step 1ns", `absent_over_time({app="x"}[1m])`, maxNs - 100, maxNs, "1ns"},
		{"absent, bottom to top", `absent_over_time({app="x"}[1m])`, math.MinInt64, maxNs, "9223372036s"},
		{"count, offset -7456146435854775806ns, end 2026-01-01T00:00:01Z", `count_over_time({app="x"}[1m] offset -7456146435854775806ns)`, 1767225601000000000 - int64(time.Hour), 1767225601000000000, "60s"},
		{"rate, end at the top", `rate({app="x"}[1m])`, maxNs - int64(time.Hour), maxNs, "60s"},
		{"json parser, end at the top", `count_over_time({app="x"} | json [1m])`, maxNs - int64(time.Hour), maxNs, "60s"},
		{"json parser, bottom to top, step 1s", `rate({app="x"} | json | level="error" [1m])`, math.MinInt64, maxNs, "1s"},
		{"unwrap, end at the top", `sum_over_time({app="x"} | logfmt | unwrap n [1m])`, maxNs - int64(time.Hour), maxNs, "60s"},
		{"unwrap, offset -7456146435854775806ns, end 2026-01-01T00:00:01Z", `avg_over_time({app="x"} | logfmt | unwrap n [1m] offset -7456146435854775806ns)`, 1767225601000000000 - int64(time.Hour), 1767225601000000000, "60s"},
		{"unwrap under sum, bottom to top", `sum by (app) (max_over_time({app="x"} | logfmt | unwrap n [1m]))`, math.MinInt64, maxNs, "9223372036s"},
		{"binary with a constant, end at the top", `count_over_time({app="x"}[1m]) + 1`, maxNs - int64(time.Hour), maxNs, "60s"},
		{"constant, bottom to top", `1 + 1`, math.MinInt64, maxNs, "1s"},
		{"constant, end at the top", `1 + 1`, maxNs - int64(time.Hour), maxNs, "1ns"},
		{"log query, bottom to top", `{app="x"}`, math.MinInt64, maxNs, "60s"},
		{"absent under sum, most negative offset", `sum(absent_over_time({app="x"}[1m] offset -9223372036854775807ns))`, 1767225601000000000 - int64(time.Hour), 1767225601000000000, "60s"},
	} {
		for _, endpoint := range []string{"query_range", "query"} {
			t.Run(tc.name+"/"+endpoint, func(t *testing.T) {
				params := map[string]string{"query": tc.query, "start": strconv.FormatInt(tc.start, 10), "end": strconv.FormatInt(tc.end, 10), "step": tc.step}
				handler := p.handleQueryRange
				if endpoint == "query" {
					handler, params = p.handleQuery, map[string]string{"query": tc.query, "time": strconv.FormatInt(tc.end, 10)}
				}
				done := make(chan int, 1)
				go func() {
					req := httptest.NewRequest(http.MethodGet, timeBoundsTarget(endpoint, params), nil)
					req.Header.Set("X-Scope-OrgID", "0")
					rec := httptest.NewRecorder()
					handler(rec, req)
					done <- rec.Code
				}()
				select {
				case <-done:
				case <-time.After(timeBoundsBudget):
					t.Fatalf("no answer within %s", timeBoundsBudget)
				}
			})
		}
	}
}

// Behind the tenant limits, the step alignment and the response caches, the
// check still gives the answers recorded for Loki: the limits layers read the
// times first and must not turn an unrepresentable one into another.
//
// conformance: semantics/request-time-bounds
func TestRequestTimeBoundsBehindTenantLimits(t *testing.T) {
	backend := &timeBoundsBackend{}
	server := httptest.NewServer(backend.handler())
	t.Cleanup(server.Close)
	p, err := New(Config{
		BackendURL:           server.URL,
		Cache:                cache.New(60*time.Second, 1000),
		CompatCache:          cache.New(60*time.Second, 1000),
		LogLevel:             "error",
		AlignQueriesWithStep: true,
		TenantDefaultLimits: map[string]any{
			"max_query_length":            "720h",
			"max_query_lookback":          "8760h",
			"max_query_range":             "24h",
			"max_entries_limit_per_query": 5000,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	now := time.Now().UnixNano()
	const q = `count_over_time({app="x"}[1m])`
	for _, tc := range []struct {
		name, start, end string
		want             string
	}{
		{"integer beyond int64", "1", "9223372036854775808", `could not parse 'end' parameter: strconv.ParseInt: parsing "9223372036854775808": value out of range`},
		{"integer far beyond int64", "99999999999999999999", strconv.FormatInt(now, 10), `could not parse 'start' parameter: strconv.ParseInt: parsing "99999999999999999999": value out of range`},
		{"decimal beyond int64", "1e30", "1e30", ""},
		{"decimal seconds beyond int64", "9223372036.855", "9223372040.855", errRequestTimeOutOfRange},
		{"rfc3339 past 2262", "2262-04-12T00:00:00Z", "2262-04-12T01:00:00Z", errRequestTimeOutOfRange},
		{"nanoseconds at the top", strconv.FormatInt(math.MaxInt64-int64(time.Hour), 10), strconv.FormatInt(math.MaxInt64, 10), errRequestTimeOutOfRange},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec := answerWithinBudget(t, p, timeBoundsTarget("query_range", map[string]string{"query": q, "start": tc.start, "end": tc.end, "step": "60s"}))
			if tc.want == "" { // not a number: the handlers' own answer, within the budget
				return
			}
			if rec.Code != http.StatusBadRequest || timeBoundsErrorText(t, rec) != tc.want {
				t.Fatalf("got %d %s, want 400 %q", rec.Code, rec.Body, tc.want)
			}
		})
	}
}

// A time that int64 nanoseconds cannot hold is not a time: the shared reader
// says so for every form instead of returning a wrapped one.
//
// conformance: semantics/request-time-bounds
func TestParseLokiTimeRefusesTimesInt64CannotHold(t *testing.T) {
	for _, raw := range []string{
		"9223372036854775808", "-9223372036854775809", "99999999999999999999", "-9223372036854775808",
		"1e30", "-1e30", "1.5e19", "9223372036.855", "-9223372036.855", "99999999999.5", "NaN", "Inf", "-Inf",
		"2262-04-12T00:00:00Z", "1677-09-20T00:00:00Z", "9999-12-31T23:59:59Z", "0001-01-01T00:00:00Z",
		"now+2600000h", "now-2600000h",
	} {
		if ns, ok := parseLokiTimeToUnixNano(raw); ok {
			t.Errorf("%q was read as %d", raw, ns)
		}
	}
	for raw, want := range map[string]int64{
		"9223372036854775807": math.MaxInt64, "-9223372036854775807": -math.MaxInt64,
		"2262-04-11T23:47:16.854775807Z": math.MaxInt64, "1677-09-21T00:12:43.145224192Z": math.MinInt64,
		"0": 0, "1": int64(time.Second), "1794490620.5": 1794490620500000000,
	} {
		if ns, ok := parseLokiTimeToUnixNano(raw); !ok || ns != want {
			t.Errorf("%q: got (%d, %v), want %d", raw, ns, ok, want)
		}
	}
}

// timeParam writes a time as the proxy reads it back: integer nanoseconds from
// 1e17 up, seconds below that (smaller integers are read as seconds,
// milliseconds or microseconds).
func timeParam(ns int64) string {
	if ns != 0 && ns > -1e17 && ns < 1e17 {
		if ns%int64(time.Second) != 0 {
			panic("a time below 1e17 ns must be whole seconds")
		}
		return strconv.FormatInt(ns/int64(time.Second), 10)
	}
	return strconv.FormatInt(ns, 10)
}

func assertTimeBoundsAnswer(t *testing.T, rec *httptest.ResponseRecorder, refused bool) {
	t.Helper()
	if refused {
		if rec.Code != http.StatusBadRequest || timeBoundsErrorText(t, rec) != errRequestTimeOutOfRange {
			t.Fatalf("expected the 400 for a window outside the range, got %d %s", rec.Code, rec.Body)
		}
		return
	}
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d %s", rec.Code, rec.Body)
	}
}

// A log query has no offset, but its bounds are checked the same way, and an
// instant log query keeps Loki's own refusal.
//
// conformance: semantics/request-time-bounds
func TestQueryRangeLogQueryTimeBounds(t *testing.T) {
	p, _ := timeBoundsProxy(t)
	rec := answerWithinBudget(t, p, timeBoundsTarget("query_range", map[string]string{
		"query": `{app="x"}`, "start": strconv.FormatInt(math.MaxInt64-int64(time.Hour), 10), "end": strconv.FormatInt(math.MaxInt64, 10),
	}))
	assertTimeBoundsAnswer(t, rec, true)
	rec = answerWithinBudget(t, p, timeBoundsTarget("query_range", map[string]string{
		"query": `{app="x"}`, "start": strconv.FormatInt(math.MaxInt64-2*int64(time.Hour), 10), "end": strconv.FormatInt(math.MaxInt64-int64(time.Hour), 10),
	}))
	assertTimeBoundsAnswer(t, rec, false)
	rec = answerWithinBudget(t, p, timeBoundsTarget("query", map[string]string{"query": `{app="x"}`, "time": strconv.FormatInt(math.MaxInt64, 10)}))
	if rec.Code != http.StatusBadRequest || timeBoundsErrorText(t, rec) != lokiInstantLogQueryError {
		t.Fatalf("instant log query: %d %s", rec.Code, rec.Body)
	}
}

// An integer time beyond int64 gets Loki's own 400, end read before start
// (recorded in testdata/loki_time_bounds.jsonl); a time that is not a time is
// left to the handlers.
//
// conformance: semantics/request-time-bounds
func TestRequestTimeBeyondInt64KeepsLokisText(t *testing.T) {
	p, _ := timeBoundsProxy(t)
	const q = `count_over_time({app="x"}[1m])`
	for _, tc := range []struct {
		name, endpoint string
		params         map[string]string
		want           string
	}{
		{"instant", "query", map[string]string{"time": "9223372036854775808"}, `strconv.ParseInt: parsing "9223372036854775808": value out of range`},
		{"instant, negative", "query", map[string]string{"time": "-9223372036854775809"}, `strconv.ParseInt: parsing "-9223372036854775809": value out of range`},
		{"range, end", "query_range", map[string]string{"start": "1", "end": "99999999999999999999", "step": "60s"}, `could not parse 'end' parameter: strconv.ParseInt: parsing "99999999999999999999": value out of range`},
		{"range, start", "query_range", map[string]string{"start": "99999999999999999999", "end": "1794494220103375104", "step": "60s"}, `could not parse 'start' parameter: strconv.ParseInt: parsing "99999999999999999999": value out of range`},
		{"range, both: end first", "query_range", map[string]string{"start": "9223372036854775808", "end": "9223372036854775808", "step": "60s"}, `could not parse 'end' parameter: strconv.ParseInt: parsing "9223372036854775808": value out of range`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			params := map[string]string{"query": q}
			for k, v := range tc.params {
				params[k] = v
			}
			rec := answerWithinBudget(t, p, timeBoundsTarget(tc.endpoint, params))
			if rec.Code != http.StatusBadRequest || timeBoundsErrorText(t, rec) != tc.want {
				t.Fatalf("got %d %s, want 400 %q", rec.Code, rec.Body, tc.want)
			}
		})
	}
}

// Times written other than as nanoseconds are held to the same range.
//
// conformance: semantics/request-time-bounds
func TestRequestTimeBoundsOtherTimeForms(t *testing.T) {
	p, _ := timeBoundsProxy(t)
	const q = `count_over_time({app="x"}[1m])`
	for _, tc := range []struct {
		name, start, end string
		refused          bool
	}{
		{"rfc3339 inside", "2200-01-01T00:00:00Z", "2200-01-01T01:00:00Z", false},
		{"rfc3339 past 2262", "2262-04-12T00:00:00Z", "2262-04-12T01:00:00Z", true},
		{"rfc3339 before 1677", "1677-01-01T00:00:00Z", "1677-01-01T01:00:00Z", true},
		{"rfc3339 year 9999", "9999-12-31T22:59:59Z", "9999-12-31T23:59:59Z", true},
		{"seconds inside", "1794490620", "1794494220", false},
		{"fractional seconds inside", "1794490620.5", "1794494220.5", false},
		{"fractional seconds past int64", "9223372036.855", "9223372040.855", true},
		{"huge fractional seconds", "1.5e18", "1.5e18", false},
		{"seconds beyond int64 nanoseconds", "99999999999", "99999999999", true},
		{"now moved forward by 290 years", "now+2540000h", "now+2540001h", true},
		{"now", "now-1h", "now", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec := answerWithinBudget(t, p, timeBoundsTarget("query_range", map[string]string{"query": q, "start": tc.start, "end": tc.end, "step": "60s"}))
			assertTimeBoundsAnswer(t, rec, tc.refused)
		})
	}
}

// A step is not a reason to refuse a window: the evaluation loops count the
// steps without wrapping, so a step as long as the range, or longer than int64
// seconds allow, answers one point at the start, as Loki does (recorded).
//
// conformance: semantics/request-time-bounds
func TestQueryRangeStepNearTheLimitsAnswers(t *testing.T) {
	p, _ := timeBoundsProxy(t)
	now := time.Now().UnixNano()
	for _, step := range []string{"1s", "9223372036s", "106751d", "3600s"} {
		for _, family := range []string{"absent", "count", "rate"} {
			t.Run(family+"/"+step, func(t *testing.T) {
				rec := answerWithinBudget(t, p, timeBoundsTarget("query_range", map[string]string{
					"query": timeBoundsQuery(family, ""), "start": strconv.FormatInt(now-int64(time.Hour), 10), "end": strconv.FormatInt(now, 10), "step": step,
				}))
				if rec.Code != http.StatusOK {
					t.Fatalf("step %s: %d %s", step, rec.Code, rec.Body)
				}
			})
		}
	}
	// The same steps on a window that ends two years under the top of the range.
	end := int64(maxRequestTimeNs) - int64(2*365*24*time.Hour)
	for _, step := range []string{"9223372036s", "106751d", "1000000000s"} {
		t.Run("absent/near the top/"+step, func(t *testing.T) {
			rec := answerWithinBudget(t, p, timeBoundsTarget("query_range", map[string]string{
				"query": timeBoundsQuery("absent", ""), "start": strconv.FormatInt(end-int64(time.Hour), 10), "end": strconv.FormatInt(end, 10), "step": step,
			}))
			if rec.Code != http.StatusOK {
				t.Fatalf("step %s: %d %s", step, rec.Code, rec.Body)
			}
		})
	}
}

// The offset moves the window the backend is asked for, in both directions.
//
// conformance: semantics/request-time-bounds
func TestOffsetMovesTheWindowTheBackendIsAskedFor(t *testing.T) {
	const end = int64(631155600) // 1990-01-01T01:00:00Z
	for _, tc := range []struct {
		name, endpoint, offset string
		wantEnd                float64
	}{
		{"range, offset 1h", "query_range", "1h", float64(end - 3600)},
		{"range, negative offset", "query_range", "-1h", float64(end + 3600)},
		{"range, offset 1d", "query_range", "1d", float64(end - 86400)},
		{"instant, offset 1h", "query", "1h", float64(end - 3600)},
		{"instant, negative offset", "query", "-90m", float64(end + 5400)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, backend := timeBoundsProxy(t)
			params := map[string]string{"query": timeBoundsQuery("count", tc.offset)}
			if tc.endpoint == "query" {
				params["time"] = strconv.FormatInt(end, 10)
			} else {
				params["start"], params["end"], params["step"] = strconv.FormatInt(end-3600, 10), strconv.FormatInt(end, 10), "60s"
			}
			if rec := answerWithinBudget(t, p, timeBoundsTarget(tc.endpoint, params)); rec.Code != http.StatusOK {
				t.Fatalf("%d %s", rec.Code, rec.Body)
			}
			backend.mu.Lock()
			defer backend.mu.Unlock()
			for _, call := range backend.calls {
				raw := call.Get(map[bool]string{true: "time", false: "end"}[tc.endpoint == "query"])
				if raw == "" {
					continue
				}
				got, ok := vlSeconds(raw)
				if !ok || math.Abs(got-tc.wantEnd) > 1 {
					t.Fatalf("backend time %q, want %v", raw, tc.wantEnd)
				}
				return
			}
			t.Fatalf("the backend was not called with a time: %v", backend.calls)
		})
	}
}

// A moved bound is written so that the handlers read it back as the time it
// is: integer nanoseconds where the proxy reads those as nanoseconds, RFC 3339
// where an integer would be read as seconds, milliseconds or microseconds, and
// saturated at the ends of int64.
//
// conformance: semantics/request-time-bounds
func TestMovedBoundReadsBackAsTheTimeItIs(t *testing.T) {
	hour := time.Hour
	for _, tc := range []struct {
		name   string
		ns     int64
		offset time.Duration
		want   int64
	}{
		{"now, 1h", 1794490620103375104, hour, 1794490620103375104 - int64(hour)},
		{"just over 1e17, 2h: the result is below 1e17", 100000000000000000 + int64(hour), 2 * hour, 100000000000000000 + int64(hour) - 2*int64(hour)},
		{"epoch, 1h: before 1970", 0, hour, -int64(hour)},
		{"epoch, negative offset", 0, -hour, int64(hour)},
		{"1990, 1h", 631155600000000000, hour, 631155600000000000 - int64(hour)},
		{"year 1700, 1h", -8520336000000000000, hour, -8520336000000000000 - int64(hour)},
		{"top of int64, negative offset saturates", math.MaxInt64 - 5, -time.Duration(100), math.MaxInt64},
		{"bottom of int64, offset saturates", math.MinInt64 + 5, 100, math.MinInt64},
		{"the most negative offset", 0, math.MinInt64, math.MaxInt64},
	} {
		t.Run(tc.name, func(t *testing.T) {
			raw := movedBound(tc.ns, tc.offset)
			got, ok := parseLokiTimeToUnixNano(raw)
			if !ok || got != tc.want {
				t.Fatalf("movedBound(%d, %s) = %q, read back as %d (%v), want %d", tc.ns, tc.offset, raw, got, ok, tc.want)
			}
		})
	}
}

// vlSeconds reads a time as VictoriaLogs does: Unix seconds with an optional
// fraction, or RFC 3339.
func vlSeconds(raw string) (float64, bool) {
	if f, err := strconv.ParseFloat(raw, 64); err == nil {
		return f, true
	}
	if t, err := time.Parse(time.RFC3339Nano, raw); err == nil {
		return float64(t.UnixNano()) / 1e9, true
	}
	return 0, false
}

// The absent_over_time evaluation counts the steps of its window with checked
// arithmetic, whatever the caller passes.
//
// conformance: semantics/request-time-bounds
func TestBuildAbsentOverTimeMatrixNearTheLimits(t *testing.T) {
	spec, ok := parseAbsentOverTimeCompatSpec(`absent_over_time({app="x"}[1m])`)
	if !ok {
		t.Fatal("spec")
	}
	const step = int64(time.Minute)
	for _, tc := range []struct {
		name           string
		start, end, st int64
		wantPoints     int
		present        map[int64]bool
	}{
		{"window ends at the top of int64", math.MaxInt64 - 3*step, math.MaxInt64, step, 4, nil},
		{"one step before the top", math.MaxInt64 - 2*step, math.MaxInt64 - step, step, 2, nil},
		{"step beyond the window", 1, math.MaxInt64, math.MaxInt64, 1, nil},
		{"bottom to top with a huge step", math.MinInt64, math.MaxInt64, math.MaxInt64, 3, nil},
		{"step of 1 over a short window at the top", math.MaxInt64 - 2, math.MaxInt64, 1, 3, nil},
		{"window around zero", -2 * step, 2 * step, step, 5, nil},
		{"a present step", 0, 2 * step, step, 2, map[int64]bool{step: true}},
		{"bottom to top with a step of one nanosecond", math.MinInt64, math.MaxInt64, 1, absentMaxPoints, nil},
		{"inverted window", 10, 5, 1, 0, nil},
		{"no step", 0, 10, 0, 0, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			done := make(chan []byte, 1)
			go func() { done <- buildAbsentOverTimeMatrix(spec, tc.present, tc.start, tc.end, tc.st) }()
			var body []byte
			select {
			case body = <-done:
			case <-time.After(timeBoundsBudget):
				t.Fatalf("no answer within %s", timeBoundsBudget)
			}
			var resp struct {
				Data struct {
					Result []struct {
						Values [][]any `json:"values"`
					} `json:"result"`
				} `json:"data"`
			}
			if err := json.Unmarshal(body, &resp); err != nil {
				t.Fatal(err)
			}
			got := 0
			if len(resp.Data.Result) > 0 {
				got = len(resp.Data.Result[0].Values)
			}
			if got != tc.wantPoints {
				t.Fatalf("points: got %d want %d: %s", got, tc.wantPoints, body)
			}
		})
	}
}

// The helpers every step loop is built on.
//
// conformance: semantics/request-time-bounds
func TestStepCountAndCheckedArithmeticEdgeTable(t *testing.T) {
	maxI, minI := int64(math.MaxInt64), int64(math.MinInt64)
	for _, tc := range []struct {
		start, end, step, want int64
	}{
		{0, 0, 1, 1},
		{0, 10, 3, 4},
		{0, 9, 3, 4},
		{5, 4, 1, 0},
		{0, 10, 0, 0},
		{0, 10, -1, 0},
		{maxI - 2, maxI, 1, 3},
		{maxI - 1, maxI, maxI, 1},
		{minI, maxI, maxI, 3},
		{minI, maxI, 1, maxI},
		{minI, minI, 1, 1},
		{-5, 5, 5, 3},
	} {
		if got := stepCount(tc.start, tc.end, tc.step); got != tc.want {
			t.Errorf("stepCount(%d, %d, %d) = %d, want %d", tc.start, tc.end, tc.step, got, tc.want)
		}
		// Every time the count names is within the window, wrapping arithmetic included.
		for _, i := range []int64{0, stepCount(tc.start, tc.end, tc.step) - 1} {
			if i < 0 || tc.want == 0 {
				continue
			}
			if ts := tc.start + i*tc.step; ts < tc.start || ts > tc.end {
				t.Errorf("time %d of index %d leaves [%d, %d]", ts, i, tc.start, tc.end)
			}
		}
	}
	for _, tc := range []struct {
		start, end, step, want int64
	}{
		{0, 0, 1, 0},
		{0, 1, 1, 1},
		{0, 10, 5, 2},
		{0, 11, 5, 3},
		{7, 3, 1, 0},
		{minI, minI + 1, 1, 1},
		{minI + 1, minI, 1, 0},
		{maxI - 1, maxI, 5, 1},
	} {
		if got := halfOpenStepCount(tc.start, tc.end, tc.step); got != tc.want {
			t.Errorf("halfOpenStepCount(%d, %d, %d) = %d, want %d", tc.start, tc.end, tc.step, got, tc.want)
		}
	}
	for _, tc := range []struct {
		a, b, want int64
		ok         bool
	}{
		{1, 2, -1, true},
		{minI, 1, minI, false},
		{maxI, -1, maxI, false},
		{0, minI, minI, false},
		{-1, minI, maxI, true},
		{maxI, maxI, 0, true},
		{minI, minI, 0, true},
	} {
		if got, ok := subNs(tc.a, tc.b); ok != tc.ok || (ok && got != tc.want) {
			t.Errorf("subNs(%d, %d) = %d, %v", tc.a, tc.b, got, ok)
		}
		if got, ok := addNs(tc.a, -tc.b); tc.b != minI && (ok != tc.ok || (ok && got != tc.want)) {
			t.Errorf("addNs(%d, %d) = %d, %v", tc.a, -tc.b, got, ok)
		}
	}
	if got := shiftNs(minI, 1); got != minI {
		t.Errorf("shiftNs(min, 1) = %d", got)
	}
	if got := shiftNs(maxI, -1); got != maxI {
		t.Errorf("shiftNs(max, -1) = %d", got)
	}
	if got := shiftNs(0, minI); got != maxI {
		t.Errorf("shiftNs(0, min) = %d", got)
	}
	if got := movedBound(3600e9, time.Hour); got != "1970-01-01T00:00:00Z" && got != "0" {
		t.Errorf("movedBound(1h, 1h) = %q", got)
	}
}

// ordered JSON rows are matched to the evaluation that includes them without
// overflowing, for a start far below zero and a row far above it.
//
// conformance: semantics/request-time-bounds
func TestOrderedJSONSampleVisibleNearTheLimits(t *testing.T) {
	maxI, minI := int64(math.MaxInt64), int64(math.MinInt64)
	for _, tc := range []struct {
		name                      string
		ts, start, end, step, win int64
		want                      bool
	}{
		{"row far above a start far below", maxI - 10, minI + 10, maxI, 100, 1000, true},
		{"row the next evaluation is past int64", maxI - 5, maxI - 20, maxI, 100, 1000, false},
		{"row on an evaluation", 100, 0, 200, 100, 10, true},
		{"row in a gap", 150, 0, 200, 100, 10, false},
		{"zero window sees nothing", 100, 0, 200, 100, 0, false},
		{"row before the start", -5, 0, 200, 100, 10, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := orderedJSONSampleVisible(tc.ts, tc.start, tc.end, tc.step, tc.win); got != tc.want {
				t.Fatalf("got %v want %v", got, tc.want)
			}
		})
	}
}

// queryShiftOf reads the offsets and ranges from the parsed query, including
// through the functions kept as typed expressions.
//
// conformance: semantics/request-time-bounds
func TestQueryShiftOfEdgeTable(t *testing.T) {
	for _, tc := range []struct {
		query                          string
		offsetMin, offsetMax, rangeMax time.Duration
	}{
		{`{app="x"}`, 0, 0, 0},
		{`rate({app="x"}[5m])`, 0, 0, 5 * time.Minute},
		{`rate({app="x"}[5m] offset 1h)`, 0, time.Hour, 5 * time.Minute},
		{`rate({app="x"}[5m] offset -1h)`, -time.Hour, 0, 5 * time.Minute},
		{`sum(rate({app="x"}[5m] offset 1h)) / sum(rate({app="y"}[1h] offset 1h))`, 0, time.Hour, time.Hour},
		{`absent_over_time({app="x"}[1m] offset -90m)`, -90 * time.Minute, 0, time.Minute},
		{`label_replace(absent_over_time({app="x"}[1m] offset 2h), "d", "$1", "app", "(.*)")`, 0, 2 * time.Hour, time.Minute},
		{`sum by (app) (count_over_time({app="x"}[10m] offset 7456146435854775806ns))`, 0, 7456146435854775806, 10 * time.Minute},
		{`rate({app="x"}[5m] offset 1d)`, 0, 24 * time.Hour, 5 * time.Minute},
	} {
		t.Run(tc.query, func(t *testing.T) {
			v := cachedQueryValidation(tc.query)
			if v.msg != "" {
				t.Fatalf("query does not validate: %s", v.msg)
			}
			if v.shift.offsetMin != tc.offsetMin || v.shift.offsetMax != tc.offsetMax || v.shift.rangeMax != tc.rangeMax {
				t.Fatalf("got %+v", v.shift)
			}
		})
	}
}

// The recorded answers of Loki 3.7.9 to requests at the ends of int64
// nanoseconds (testdata/loki_time_bounds.jsonl), replayed through the proxy:
// where Loki refuses, the proxy gives Loki's text; where Loki answers 200, the
// proxy answers 200 for a window inside the representable range and its own
// 400 for one outside it, judged here with arbitrary-precision arithmetic.
//
// conformance: semantics/request-time-bounds
func TestRequestTimeBoundsRecordedLokiAnswers(t *testing.T) {
	file, err := os.Open("testdata/loki_time_bounds.jsonl")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = file.Close() }()
	type row struct {
		ID       string            `json:"id"`
		Endpoint string            `json:"endpoint"`
		Family   string            `json:"family"`
		Offset   string            `json:"offset"`
		Params   map[string]string `json:"params"`
		Status   int               `json:"status"`
		Loki     json.RawMessage   `json:"loki"`
	}
	p, _ := timeBoundsProxy(t)
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 1<<16), 1<<20)
	scanner.Scan() // the header names the Loki version
	var header struct {
		Loki string `json:"loki"`
	}
	if err := json.Unmarshal(scanner.Bytes(), &header); err != nil || header.Loki != "3.7.9" {
		t.Fatalf("header %s: %v", scanner.Bytes(), err)
	}
	identical := regexp.MustCompile(`^(strconv\.ParseInt: parsing .*: value out of range|could not parse '(start|end)' parameter: strconv\.ParseInt: parsing .*: value out of range|log queries are not supported as an instant query type.*)$`)
	rows, replayed := 0, 0
	for scanner.Scan() {
		var r row
		if err := json.Unmarshal(scanner.Bytes(), &r); err != nil {
			t.Fatal(err)
		}
		rows++
		t.Run(r.ID, func(t *testing.T) {
			endpoint := "query"
			if r.Endpoint == "query_range" {
				endpoint = "query_range"
			}
			rec := answerWithinBudget(t, p, timeBoundsTarget(endpoint, r.Params))
			replayed++
			var lokiText string
			if r.Status != 200 {
				_ = json.Unmarshal(r.Loki, &lokiText)
			}
			switch {
			case r.Status == http.StatusBadRequest && identical.MatchString(lokiText):
				if rec.Code != http.StatusBadRequest || timeBoundsErrorText(t, rec) != lokiText {
					t.Fatalf("Loki answered 400 %q, the proxy %d %s", lokiText, rec.Code, rec.Body)
				}
			case r.Status == http.StatusOK:
				fits, known := recordedWindowFits(r.Endpoint, r.Family, r.Offset, r.Params)
				if !known {
					return
				}
				assertTimeBoundsAnswer(t, rec, !fits)
			}
			// Loki's other refusals (a step it cannot read, an inverted range,
			// a time that is not a number) and its 500s are not decided by the
			// time bounds: the answer only has to arrive within the budget.
		})
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	if rows < 300 || replayed != rows {
		t.Fatalf("replayed %d of %d rows", replayed, rows)
	}
}

// recordedWindowFits judges a recorded request with big integers, apart from
// the code under test: its window moved by the offset and widened by the
// one-minute range must lie inside [minRequestTimeNs, maxRequestTimeNs].
func recordedWindowFits(endpoint, family, offset string, params map[string]string) (fits, known bool) {
	read := func(raw string) (*big.Int, bool) {
		if n, ok := new(big.Int).SetString(raw, 10); ok {
			// The proxy reads an integer below 1e17 as seconds, milliseconds or
			// microseconds where Loki reads nanoseconds: those rows are compared
			// for the budget only.
			if n.Sign() != 0 && n.CmpAbs(big.NewInt(1e17)) < 0 {
				return nil, false
			}
			return n, true
		}
		if tm, err := time.Parse(time.RFC3339Nano, raw); err == nil {
			ns := new(big.Int).Mul(big.NewInt(tm.Unix()), big.NewInt(1e9))
			return ns.Add(ns, big.NewInt(int64(tm.Nanosecond()))), true
		}
		return nil, false
	}
	var start, end *big.Int
	var ok bool
	if endpoint == "query" {
		if end, ok = read(params["time"]); !ok {
			return false, false
		}
		start = end
	} else {
		if start, ok = read(params["start"]); !ok {
			return false, false
		}
		if end, ok = read(params["end"]); !ok {
			return false, false
		}
		if start.Cmp(end) > 0 {
			return false, false
		}
	}
	off, window := new(big.Int), new(big.Int)
	if family != "log" {
		window.SetInt64(int64(time.Minute))
	}
	if offset != "" {
		switch {
		case strings.HasSuffix(offset, "d"):
			days, err := strconv.ParseInt(strings.TrimSuffix(offset, "d"), 10, 64)
			if err != nil {
				return false, false
			}
			off.Mul(big.NewInt(days), big.NewInt(int64(24*time.Hour)))
		default:
			d, err := time.ParseDuration(offset)
			if err != nil {
				return false, false
			}
			off.SetInt64(int64(d))
		}
	}
	oMin, oMax := new(big.Int), new(big.Int)
	if off.Sign() < 0 {
		oMin.Set(off)
	} else {
		oMax.Set(off)
	}
	lo := new(big.Int).Sub(start, oMax)
	lo.Sub(lo, window)

	hi := new(big.Int).Sub(end, oMin)
	lowest, highest := big.NewInt(minRequestTimeNs), big.NewInt(maxRequestTimeNs)
	fits = start.Cmp(lowest) >= 0 && end.Cmp(highest) <= 0 && lo.Cmp(lowest) >= 0 && hi.Cmp(highest) <= 0
	return fits, true
}

// FuzzRequestTimeBounds sends arbitrary start, end, step and offset to both
// endpoints: the proxy must answer within the budget without a panic, with 200
// or a 400 whose text is the proxy's range limit, Loki's text for an integer
// beyond int64, or a parameter error of the handlers. The step is a number of
// seconds and the offset a number of nanoseconds, the forms that reach the
// arithmetic; a step or offset that is not a duration is the parser's.
func FuzzRequestTimeBounds(f *testing.F) {
	now := time.Now().UnixNano()
	seeds := []struct {
		start, end, stepSeconds, offsetNs int64
	}{
		{now - int64(time.Hour), now, 60, 0},
		{math.MaxInt64 - int64(time.Hour), math.MaxInt64, 60, 0},
		{now - int64(time.Hour), now, 60, -7456146435854775806},
		{1767225601000000000 - int64(time.Hour), 1767225601000000000, 60, -7456146435854775806},
		{0, int64(time.Hour), 60, -2562047 * int64(time.Hour)},
		{math.MinInt64, math.MinInt64 + int64(time.Hour), 60, 3600e9},
		{now - int64(time.Hour), now, 9223372036, 0},
		{math.MaxInt64 - 3*int64(time.Minute), math.MaxInt64, 60, -1e6},
		{1700000000000000000, 1700000000000000003, 1, math.MaxInt64},
	}
	for _, s := range seeds {
		f.Add(s.start, s.end, s.stepSeconds, s.offsetNs)
	}
	// One proxy and one backend serve every input.
	backend := &timeBoundsBackend{}
	server := httptest.NewServer(backend.handler())
	f.Cleanup(server.Close)
	p, err := newTimeBoundsProxy(server.URL)
	if err != nil {
		f.Fatal(err)
	}
	f.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	mux := http.NewServeMux()
	p.RegisterRoutes(mux)
	allowed := regexp.MustCompile(`^(strconv\.ParseInt: parsing .*|could not parse .*|exceeded maximum resolution.*|end timestamp must not be before.*|cannot parse .*|parse error.*|invalid .*|log queries are not supported.*)$`)
	f.Fuzz(func(t *testing.T, start, end, stepSeconds, offsetNs int64) {
		step := strconv.FormatInt(stepSeconds, 10) + "s"
		offsetClause := ""
		if offsetNs != 0 && offsetNs != math.MinInt64 {
			offsetClause = " offset " + strconv.FormatInt(offsetNs, 10) + "ns"
		}
		for _, family := range []string{"absent", "count", "rate", "json", "unwrap", "binary", "constant", "log"} {
			query := strings.ReplaceAll(timeBoundsQueries[family], "%s", offsetClause)
			for _, target := range []string{
				timeBoundsTarget("query_range", map[string]string{"query": query, "start": strconv.FormatInt(start, 10), "end": strconv.FormatInt(end, 10), "step": step}),
				timeBoundsTarget("query", map[string]string{"query": query, "time": strconv.FormatInt(end, 10)}),
			} {
				done := make(chan *httptest.ResponseRecorder, 1)
				go func() {
					req := httptest.NewRequest(http.MethodGet, target, nil)
					req.Header.Set("X-Scope-OrgID", "0")
					rec := httptest.NewRecorder()
					mux.ServeHTTP(rec, req)
					done <- rec
				}()
				var rec *httptest.ResponseRecorder
				select {
				case rec = <-done:
				case <-time.After(timeBoundsBudget):
					t.Fatalf("no answer within %s: %s", timeBoundsBudget, target)
				}
				if rec.Body.Len() > 1<<20 {
					t.Fatalf("answer of %d bytes: %s", rec.Body.Len(), target)
				}
				switch rec.Code {
				case http.StatusOK:
				case http.StatusBadRequest:
					if text := readErrorBody(t, rec.Header(), rec.Body.Bytes()).Error; text != errRequestTimeOutOfRange && !allowed.MatchString(text) {
						t.Fatalf("unexpected 400 %q for %s", text, target)
					}
				default:
					t.Fatalf("status %d %s for %s", rec.Code, rec.Body, target)
				}
			}
		}
	})
}

// The hits-folded range metric builder jumps over the steps that hold no line;
// its points are those of a walk over every step, on sparse and dense samples
// and for a window shorter, equal and longer than the step.
//
// conformance: semantics/request-time-bounds
func TestHitsRangeMetricMatrixSkipsEmptyStepsWithoutChangingPoints(t *testing.T) {
	start := time.Unix(1_700_000_000, 0)
	for _, tc := range []struct {
		name         string
		step, window time.Duration
		samples      []int64 // offsets from start, in seconds
	}{
		{"sparse, window below step", 60 * time.Second, 30 * time.Second, []int64{5, 4000, 4001, 9000}},
		{"sparse, window equal to step", 60 * time.Second, 60 * time.Second, []int64{0, 61, 3600, 7199}},
		{"sparse, window above step", 60 * time.Second, 5 * time.Minute, []int64{100, 3700, 3701}},
		{"dense", 15 * time.Second, 30 * time.Second, []int64{0, 10, 20, 30, 45, 60, 61, 62, 200, 201, 202, 203}},
		{"one sample at the end", 60 * time.Second, 60 * time.Second, []int64{7200}},
		{"no sample", 60 * time.Second, 60 * time.Second, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			end := start.Add(2 * time.Hour)
			var samples []rangeMetricSample
			for _, s := range tc.samples {
				samples = append(samples, rangeMetricSample{ts: start.Add(time.Duration(s) * time.Second).UnixNano(), value: 1})
			}
			series := map[string]manualSeriesSamples{"a": {Metric: map[string]string{"app": "a"}, Samples: samples}}
			body := mustBuildHitsRangeMetricMatrix(t, "count_over_time", series, start, end, tc.step, tc.window)
			var resp struct {
				Data struct {
					Result []struct {
						Values [][]any `json:"values"`
					} `json:"result"`
				} `json:"data"`
			}
			if err := json.Unmarshal(body, &resp); err != nil {
				t.Fatal(err)
			}
			got := map[int64]string{}
			for _, r := range resp.Data.Result {
				for _, v := range r.Values {
					got[int64(math.Round(v[0].(float64)))] = v[1].(string)
				}
			}
			// The reference walks every step: the samples in [t-window, t).
			want := map[int64]string{}
			for ts := start.UnixNano(); ts <= end.UnixNano(); ts += tc.step.Nanoseconds() {
				n := 0
				for _, s := range samples {
					if s.ts >= ts-tc.window.Nanoseconds() && s.ts < ts {
						n++
					}
				}
				if n > 0 {
					want[ts/int64(time.Second)] = strconv.Itoa(n)
				}
			}
			if len(got) != len(want) {
				t.Fatalf("points: got %d want %d\n got=%v\nwant=%v", len(got), len(want), got, want)
			}
			for ts, v := range want {
				if got[ts] != v {
					t.Fatalf("at %d: got %q want %q", ts, got[ts], v)
				}
			}
		})
	}
}

// BenchmarkHitsRangeMetricMatrixSparse is the hits-folded builder over a day at
// 15 s steps with a handful of lines, where the jump over empty steps decides
// the cost, and over dense lines, where it does not apply.
func BenchmarkHitsRangeMetricMatrixSparse(b *testing.B) {
	start := time.Unix(1_700_000_000, 0)
	end := start.Add(24 * time.Hour)
	for _, tc := range []struct {
		name  string
		every time.Duration
	}{{"sparse", 3 * time.Hour}, {"dense", time.Minute}} {
		var samples []rangeMetricSample
		for ts := start; ts.Before(end); ts = ts.Add(tc.every) {
			samples = append(samples, rangeMetricSample{ts: ts.UnixNano(), value: 1})
		}
		series := map[string]manualSeriesSamples{"a": {Metric: map[string]string{"app": "a"}, Samples: samples}}
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if _, err := buildHitsRangeMetricMatrix("count_over_time", series, start, end, 15*time.Second, 30*time.Second, 1<<30); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
