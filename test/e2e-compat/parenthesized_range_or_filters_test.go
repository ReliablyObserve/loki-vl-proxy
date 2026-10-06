//go:build e2e

package e2e_compat

import (
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// orFilterFixture holds the same 180 logfmt lines, one every 10s and cycling
// through six words and six source addresses, in two streams written
// byte-identically to Loki and VictoriaLogs: logs derives detected_level on
// each backend (the log queries compare stream labels), metrics stores
// detected_level=unknown on both, so bare parser metrics have identical label
// sets.
type orFilterFixture struct {
	logs, metrics slidingLiveFixture
	start         time.Time
}

var (
	orFilterOnce sync.Once
	orFixture    *orFilterFixture
)

func ensureOrFilterFixture(t *testing.T) *orFilterFixture {
	t.Helper()
	orFilterOnce.Do(func() {
		now := time.Now()
		fx := &orFilterFixture{
			logs:    slidingLiveFixture{app: fmt.Sprintf("paren-or-logs-%d", now.UnixNano()), service: true, derivedLevel: true},
			metrics: slidingLiveFixture{app: fmt.Sprintf("paren-or-metrics-%d", now.UnixNano()), service: true},
			start:   now.Add(-5 * time.Hour).Truncate(time.Hour),
		}
		words := []string{"red", "blue", "green", "amber", "teal", "pink"}
		ips := []string{"10.0.0.1", "10.0.0.2", "10.0.0.3", "192.168.1.4", "192.168.1.5", "172.16.0.6"}
		for i := 0; i < 180; i++ {
			line := slidingLiveLine{
				ts:  fx.start.Add(time.Duration(i) * 10 * time.Second),
				msg: fmt.Sprintf("tick=%03d word=%s n=%d src=%s", i, words[i%6], i%7, ips[i%6]),
			}
			fx.logs.lines = append(fx.logs.lines, line)
			fx.metrics.lines = append(fx.metrics.lines, line)
		}
		ingestSlidingFixtures(t, fx.logs, fx.metrics)
		orFixture = fx
	})
	if orFixture == nil {
		t.Fatal("or-filter fixture was not ingested; see the first test that ran")
	}
	return orFixture
}

// logStreams runs a log query_range and returns stream labels -> "ts line"
// entries, failing unless the response is a healthy, non-empty streams result.
func logStreams(t *testing.T, baseURL, query string, start, end time.Time) map[string][]string {
	t.Helper()
	params := url.Values{
		"query": {query}, "limit": {"1000"}, "direction": {"forward"},
		"start": {strconv.FormatInt(start.UnixNano(), 10)}, "end": {strconv.FormatInt(end.UnixNano(), 10)},
	}
	status, body, resp := doJSONGET(t, baseURL+"/loki/api/v1/query_range?"+params.Encode(), map[string]string{"X-Scope-OrgID": "0"})
	if status != http.StatusOK || resp["status"] != "success" || resp["warnings"] != nil || resp["error"] != nil {
		t.Fatalf("%s %s: unhealthy response %d %s", baseURL, query, status, body)
	}
	data := extractMap(resp, "data")
	if data["resultType"] != "streams" {
		t.Fatalf("%s %s: expected streams, got %s", baseURL, query, body)
	}
	out := map[string][]string{}
	for _, item := range extractArray(data, "result") {
		stream, _ := item.(map[string]interface{})
		labels, _ := stream["stream"].(map[string]interface{})
		keys := make([]string, 0, len(labels))
		for k, v := range labels {
			keys = append(keys, fmt.Sprintf("%s=%v", k, v))
		}
		sort.Strings(keys)
		key := strings.Join(keys, ",")
		for _, raw := range stream["values"].([]interface{}) {
			pair := raw.([]interface{})
			out[key] = append(out[key], fmt.Sprintf("%v %v", pair[0], pair[1]))
		}
	}
	return out
}

func assertStreamParity(t *testing.T, query string, loki, proxy map[string][]string, wantLines int) {
	t.Helper()
	lines := 0
	for _, v := range loki {
		lines += len(v)
	}
	if lines != wantLines {
		t.Fatalf("%s: Loki returned %d lines, the fixture selects %d; parity would not prove the filter", query, lines, wantLines)
	}
	if len(loki) != len(proxy) {
		t.Errorf("%s: proxy returned %d streams, Loki %d", query, len(proxy), len(loki))
	}
	for key, want := range loki {
		got := proxy[key]
		if len(got) != len(want) {
			t.Errorf("%s {%s}: proxy has %d lines, Loki %d", query, key, len(got), len(want))
			continue
		}
		for i := range want {
			if got[i] != want[i] {
				t.Errorf("%s {%s}[%d]: proxy %q, Loki %q", query, key, i, got[i], want[i])
				break
			}
		}
	}
}

// rejection runs a query_range on both sides and returns the status and error
// text of each; the proxy wraps the text in Loki's JSON error envelope.
func rejection(t *testing.T, baseURL string, params url.Values) (int, string) {
	t.Helper()
	status, body, resp := doJSONGET(t, baseURL+"/loki/api/v1/query_range?"+params.Encode(), map[string]string{"X-Scope-OrgID": "0"})
	if msg, ok := resp["message"].(string); ok {
		return status, msg
	}
	return status, strings.TrimSpace(body)
}

// assertSameAsPlain proves a range metric in an alternative form against its
// plain form: Loki answers both alike, the proxy answers both alike, and
// wherever the proxy's plain form matches Loki's (the sliding-window and
// first-bucket differences of older VictoriaLogs lines are not this change's),
// so does the alternative.
func assertSameAsPlain(t *testing.T, alt, plain string, start, end time.Time) {
	t.Helper()
	step := time.Minute
	lokiPlain := slidingRangeSeries(t, lokiURL, plain, start, end, step, nil)
	assertSlidingParity(t, alt+" [Loki, plain form]", lokiPlain, slidingRangeSeries(t, lokiURL, alt, start, end, step, nil))
	proxyPlain := slidingRangeSeries(t, proxyURL, plain, start, end, step, nil)
	proxyAlt := slidingRangeSeries(t, proxyURL, alt, start, end, step, nil)
	assertSlidingParity(t, alt+" [proxy, plain form]", proxyPlain, proxyAlt)
	if fmt.Sprint(lokiPlain) == fmt.Sprint(proxyPlain) {
		assertSlidingParity(t, alt, lokiPlain, proxyAlt)
	} else {
		t.Logf("%s: the plain form differs from Loki on this VictoriaLogs; the alternative is held to the plain form", plain)
	}
}

// instantVector runs an instant query and returns series -> value.
func instantVector(t *testing.T, baseURL, query string, at time.Time) map[string]string {
	t.Helper()
	params := url.Values{"query": {query}, "time": {strconv.FormatInt(at.UnixNano(), 10)}}
	status, body, resp := doJSONGET(t, baseURL+"/loki/api/v1/query?"+params.Encode(), map[string]string{"X-Scope-OrgID": "0"})
	if status != http.StatusOK || resp["status"] != "success" || resp["warnings"] != nil || resp["error"] != nil {
		t.Fatalf("%s %s: unhealthy response %d %s", baseURL, query, status, body)
	}
	data := extractMap(resp, "data")
	if data["resultType"] != "vector" {
		t.Fatalf("%s %s: expected vector, got %s", baseURL, query, body)
	}
	out := map[string]string{}
	for _, item := range extractArray(data, "result") {
		series, _ := item.(map[string]interface{})
		metric, _ := series["metric"].(map[string]interface{})
		keys := make([]string, 0, len(metric))
		for k, v := range metric {
			keys = append(keys, fmt.Sprintf("%s=%v", k, v))
		}
		sort.Strings(keys)
		out[strings.Join(keys, ",")] = fmt.Sprint(series["value"].([]interface{})[1])
	}
	return out
}

// conformance: semantics/parenthesized-log-range
// conformance: loki_api_v1_query_range, loki_api_v1_query
// TestCompat_ParenthesizedLogRange: Loki evaluates a log expression in
// parentheses under a range function, with an offset, an unwrap on either side
// of the range, and a pipeline after the range, like the plain form. The proxy
// answered these with a VictoriaLogs parse error (range) or "log queries are
// not supported as an instant query type" (instant).
func TestCompat_ParenthesizedLogRange(t *testing.T) {
	fx := ensureOrFilterFixture(t)
	sel := fx.metrics.selector()
	start, end := fx.start.Add(-2*time.Minute), fx.start.Add(33*time.Minute)
	unwrap := `sum by (word) (sum_over_time(` + sel + ` | logfmt | unwrap n [1m]))`
	unwrapOffset := `sum by (word) (sum_over_time(` + sel + ` | logfmt | unwrap n [1m] offset 1m))`
	for _, tc := range []struct{ alt, plain string }{
		{`count_over_time((` + sel + ` |= "red")[1m])`, `count_over_time(` + sel + ` |= "red" [1m])`},
		{`count_over_time((` + sel + `)[1m])`, `count_over_time(` + sel + `[1m])`},
		{`sum by (word) (rate((` + sel + ` | logfmt)[1m] offset 1m))`, `sum by (word) (rate(` + sel + ` | logfmt [1m] offset 1m))`},
		{`bytes_over_time(((` + sel + ` |= "green")[1m]))`, `bytes_over_time(` + sel + ` |= "green" [1m])`},
		{`count_over_time((` + sel + `[1m] |= "blue"))`, `count_over_time(` + sel + ` |= "blue" [1m])`},
		{`count_over_time(` + sel + `[1m] |= "teal")`, `count_over_time(` + sel + ` |= "teal" [1m])`},
		{`sum by (word) (count_over_time((` + sel + ` | logfmt)[1m]))`, `sum by (word) (count_over_time(` + sel + ` | logfmt [1m]))`},
		{`sum(rate((` + sel + ` |= "red")[1m])) / sum(rate((` + sel + `)[1m]))`, `sum(rate(` + sel + ` |= "red" [1m])) / sum(rate(` + sel + `[1m]))`},
		// The first argument of label_replace is a metric expression too.
		{`label_replace(rate((` + sel + ` |= "red")[1m]), "kind", "$1", "service_name", "(.*)")`, `label_replace(rate(` + sel + ` |= "red" [1m]), "kind", "$1", "service_name", "(.*)")`},
		// Every unwrap form is the plain form.
		{`sum by (word) (sum_over_time((` + sel + ` | logfmt | unwrap n)[1m]))`, unwrap},
		{`sum by (word) (sum_over_time((` + sel + ` | logfmt | unwrap n [1m])))`, unwrap},
		{`sum by (word) (sum_over_time(` + sel + `[1m] | logfmt | unwrap n))`, unwrap},
		{`sum by (word) (sum_over_time((` + sel + ` | logfmt | unwrap n)[1m] offset 1m))`, unwrapOffset},
		{`sum by (word) (sum_over_time(` + sel + `[1m] offset 1m | logfmt | unwrap n))`, unwrapOffset},
	} {
		t.Run(tc.alt, func(t *testing.T) { assertSameAsPlain(t, tc.alt, tc.plain, start, end) })
	}
	// Instant queries over the whole fixture.
	at := fx.start.Add(30 * time.Minute)
	for _, query := range []string{
		`count_over_time((` + sel + ` |= "red")[30m])`,
		`sum(count_over_time((` + sel + ` | logfmt)[30m]))`,
		`sum by (word) (sum_over_time((` + sel + ` | logfmt | unwrap n)[30m]))`,
	} {
		t.Run("instant "+query, func(t *testing.T) {
			loki := instantVector(t, lokiURL, query, at)
			if len(loki) == 0 {
				t.Fatalf("Loki returned no series; parity would be empty-vs-empty")
			}
			proxy := instantVector(t, proxyURL, query, at)
			if fmt.Sprint(loki) != fmt.Sprint(proxy) {
				t.Errorf("proxy %v, Loki %v", proxy, loki)
			}
		})
	}
	// A parenthesised log query is a log query.
	t.Run("log query in parentheses", func(t *testing.T) {
		query := `(` + fx.logs.selector() + ` |= "red")`
		assertStreamParity(t, query, logStreams(t, lokiURL, query, start, end), logStreams(t, proxyURL, query, start, end), 30)
	})
	// What Loki rejects, the proxy rejects with Loki's status and text.
	for _, query := range []string{
		`rate(((` + sel + ` |= "e"))[5m])`,
		`rate((` + sel + ` | json)[5m] | unwrap v)`,
		`rate((` + sel + `)[5m] | json)`,
		`rate((` + sel + ` |= "e")[5m]))`,
		`rate((` + sel + ` |= "e"))`,
	} {
		t.Run("rejected "+query, func(t *testing.T) {
			params := url.Values{"query": {query}, "start": {strconv.FormatInt(start.UnixNano(), 10)}, "end": {strconv.FormatInt(end.UnixNano(), 10)}, "step": {"60"}}
			lokiStatus, lokiBody := rejection(t, lokiURL, params)
			proxyStatus, proxyBody := rejection(t, proxyURL, params)
			if lokiStatus != http.StatusBadRequest {
				t.Fatalf("Loki sanity: expected 400, got %d %s", lokiStatus, lokiBody)
			}
			if proxyStatus != lokiStatus || proxyBody != lokiBody {
				t.Errorf("proxy %d %s, Loki %d %s", proxyStatus, proxyBody, lokiStatus, lokiBody)
			}
		})
	}
}

// conformance: semantics/line-filter-or-alternatives
// conformance: loki_api_v1_query_range
// TestCompat_LineFilterOrAlternatives: `|= "a" or "b"` matches a line holding
// either string, `!= "a" or "b"` a line holding neither (Loki turns the negated
// chain into one filter per alternative); |~, !~, |> and !> chain the same way.
// Under |=, |~ and |> an ip() alternative is plain text, under != it is an ip
// match.
func TestCompat_LineFilterOrAlternatives(t *testing.T) {
	fx := ensureOrFilterFixture(t)
	sel := fx.logs.selector()
	start, end := fx.start.Add(-2*time.Minute), fx.start.Add(33*time.Minute)
	for _, tc := range []struct {
		filter string
		lines  int
	}{
		{`|= "red" or "blue"`, 60},
		{`|= "red" or "blue" or "teal"`, 90},
		{"|= `red` or `green`", 60},
		{`!= "red" or "blue"`, 120},
		{`!= "red" or "blue" != "green" or "amber"`, 60},
		{`|~ "gr.en" or "^tick=00[0-3]"`, 33},
		{`!~ "gr.en" or "^tick=00[0-3]"`, 147},
		{`|> "<_>=red<_>" or "<_>=teal<_>"`, 60},
		{`!> "<_>=red<_>" or "<_>=teal<_>"`, 120},
		{`|= "red" or ip("10.0.0.2")`, 60},
		{`|= ip("10.0.0.1") or "blue"`, 60},
		{`!= "red" or ip("10.0.0.2")`, 120},
		{`!= ip("10.0.0.2") or "green"`, 120},
		{`|= "red" or "blue" |~ "n=[0-3] " != "tick=000" or "tick=006"`, 34},
		// An ip(...) alternative ends Loki's orFilter: the next `or` drops what came before.
		{`|= "red" or ip("10.0.0.2") or "teal"`, 60},
		{`|= "red" or "blue" or ip("10.0.0.3") or "teal"`, 60},
		{`|= ip("10.0.0.2") or "teal" or "pink"`, 90},
		{"|= `red` or ip(`10.0.0.2`)", 60},
		// Keywords are case-insensitive and any white space may follow `or`.
		{`|= "red" OR "blue"`, 60},
		{"|= \"red\" or\n\"blue\"", 60},
		// Loki also drops a regexp that simplifies to match-all.
		{`|~ "red" or "(.*)"`, 30},
		// Loki drops an alternative that matches every line from a positive chain.
		{`|= "" or "teal"`, 30},
		{`|~ ".*" or "blue"`, 30},
	} {
		query := sel + " " + tc.filter
		t.Run(query, func(t *testing.T) {
			loki := logStreams(t, lokiURL, query, start, end)
			assertStreamParity(t, query, loki, logStreams(t, proxyURL, query, start, end), tc.lines)
		})
	}
	// The same chains inside a range metric, against the regexp that means the same.
	msel := fx.metrics.selector()
	for _, tc := range []struct{ alt, plain string }{
		{`count_over_time(` + msel + ` |= "red" or "blue" [1m])`, `count_over_time(` + msel + ` |~ "red|blue" [1m])`},
		{`count_over_time(` + msel + ` != "red" or "blue" [1m])`, `count_over_time(` + msel + ` !~ "red|blue" [1m])`},
		{`sum by (word) (rate(` + msel + ` |~ "gr.en" or "teal" | logfmt [1m]))`, `sum by (word) (rate(` + msel + ` |~ "gr.en|teal" | logfmt [1m]))`},
		{`sum by (word) (count_over_time(` + msel + ` | logfmt !~ "red" or "blue" [1m]))`, `sum by (word) (count_over_time(` + msel + ` | logfmt !~ "red|blue" [1m]))`},
	} {
		t.Run(tc.alt, func(t *testing.T) { assertSameAsPlain(t, tc.alt, tc.plain, start, end) })
	}
	// Invalid chains: Loki's status and error text.
	for _, query := range []string{
		sel + ` |= "a" or`,
		sel + ` |= or "b"`,
		sel + ` |= "a" or ("b")`,
		sel + ` !~ "a" or ip("1.2.3.4")`,
		sel + ` |~ "a" or "("`,
		sel + ` != "a" or ip("999.1.1.1")`,
	} {
		t.Run("rejected "+query, func(t *testing.T) {
			params := url.Values{"query": {query}, "start": {strconv.FormatInt(start.UnixNano(), 10)}, "end": {strconv.FormatInt(end.UnixNano(), 10)}}
			lokiStatus, lokiBody := rejection(t, lokiURL, params)
			proxyStatus, proxyBody := rejection(t, proxyURL, params)
			if lokiStatus != http.StatusBadRequest {
				t.Fatalf("Loki sanity: expected 400, got %d %s", lokiStatus, lokiBody)
			}
			if proxyStatus != lokiStatus || proxyBody != lokiBody {
				t.Errorf("proxy %d %s, Loki %d %s", proxyStatus, proxyBody, lokiStatus, lokiBody)
			}
		})
	}
}
