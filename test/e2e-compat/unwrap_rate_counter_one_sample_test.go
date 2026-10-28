//go:build e2e

package e2e_compat

import (
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"
)

// TestCompat_UnwrapRateCounterOneSampleSeries: Loki rates rate_counter per
// series of its sample extractor, the stream plus every label `| logfmt`
// extracts less the unwrapped one (pkg/logql/log/metrics_extraction.go:160-166),
// and a sum does not hand it its grouping (pkg/logql/syntax/ast.go:1630-1640).
// With a transaction id on every line each line is a series of its own, which
// rates 0 (pkg/logql/range_vector.go:286-291): `sum` and `sum by (lvl)` answer
// 0 at every step whose window holds a line, sliding, tumbling and shorter than
// the step. The proxy answers the same series and points.
// conformance: semantics/unwrap-rate-counter-one-sample-series, semantics/unwrap-rate-ungrouped-sum-per-stream, metric-series-identity, loki_api_v1_query_range
func TestCompat_UnwrapRateCounterOneSampleSeries(t *testing.T) {
	now := time.Now()
	fx := slidingLiveFixture{app: fmt.Sprintf("rate-counter-one-%d", now.UnixNano()), service: true}
	s0 := now.Add(-6 * time.Hour).Truncate(time.Hour)
	// One logfmt line every 10s for 30 minutes, 7s past each 10s mark, a
	// counter value that resets every 9 lines, a level, a transaction id.
	for i := 0; i < 180; i++ {
		lvl := []string{"info", "warn"}[i%2]
		msg := fmt.Sprintf("amount=%d lvl=%s txn=t%d-%d", (i%9)*10, lvl, i, now.UnixNano())
		fx.lines = append(fx.lines, slidingLiveLine{ts: s0.Add(7*time.Second + time.Duration(i)*10*time.Second), msg: msg})
	}
	ingestSlidingFixtures(t, fx)
	sel := fx.selector()

	rangeParams := func(q string, start, end time.Duration, step int) url.Values {
		return url.Values{"query": {q}, "start": {strconv.FormatInt(s0.Add(start).UnixNano(), 10)},
			"end": {strconv.FormatInt(s0.Add(end).UnixNano(), 10)}, "step": {strconv.Itoa(step)}}
	}
	// Loki answers range metrics of a new stream only once its ingester cut
	// the chunks (the fresh-stack blank window): wait for a numeric control.
	control := rangeParams(`sum(count_over_time(`+sel+` [5m]))`, 10*time.Minute, 25*time.Minute, 60)
	deadline := time.Now().Add(180 * time.Second)
	for {
		if got := rateCounterMatrix(t, lokiURL, control); len(got) == 1 && len(got[""]) > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("Loki does not answer the count control")
		}
		time.Sleep(5 * time.Second)
	}

	for _, tc := range []struct {
		name   string
		params url.Values
	}{
		{"sliding", rangeParams(`sum(rate_counter(`+sel+` | logfmt | unwrap amount [5m]))`, 10*time.Minute, 25*time.Minute, 60)},
		{"sliding, by level", rangeParams(`sum by (lvl) (rate_counter(`+sel+` | logfmt | unwrap amount [5m]))`, 10*time.Minute, 25*time.Minute, 60)},
		{"tumbling", rangeParams(`sum(rate_counter(`+sel+` | logfmt | unwrap amount [1m]))`, 10*time.Minute, 25*time.Minute, 60)},
		{"range shorter than the step", rangeParams(`sum(rate_counter(`+sel+` | logfmt | unwrap amount [2m]))`, 10*time.Minute, 30*time.Minute, 300)},
		{"range not a multiple of the step, line filter", rangeParams(`sum(rate_counter(`+sel+` |= "lvl=info" | logfmt | unwrap amount [90s]))`, 10*time.Minute, 25*time.Minute, 60)},
		{"the window reaches past the lines", rangeParams(`sum(rate_counter(`+sel+` | logfmt | unwrap amount [5m]))`, 20*time.Minute, 40*time.Minute, 60)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			loki := rateCounterMatrix(t, lokiURL, tc.params)
			proxy := rateCounterMatrix(t, proxyURL, tc.params)
			points := 0
			for _, series := range loki {
				points += len(series)
				for ts, v := range series {
					if v != "0" {
						t.Fatalf("Loki answers %s at %s: the fixture is not one sample per series", v, ts)
					}
				}
			}
			if points == 0 {
				t.Fatal("Loki answered no sample; parity would be empty-vs-empty")
			}
			if fmt.Sprint(loki) != fmt.Sprint(proxy) {
				t.Fatalf("query %s\nLoki  %v\nproxy %v", tc.params.Get("query"), loki, proxy)
			}
		})
	}
}

// rateCounterMatrix returns a 200 matrix as the series' lvl label (or "") ->
// timestamp -> value text.
func rateCounterMatrix(t *testing.T, base string, params url.Values) map[string]map[string]string {
	t.Helper()
	status, body, resp := doJSONGET(t, base+"/loki/api/v1/query_range?"+params.Encode(), map[string]string{"X-Scope-OrgID": "0"})
	if status != http.StatusOK || resp["status"] != "success" || resp["warnings"] != nil {
		t.Fatalf("%s %s: unhealthy response %d %s", base, params.Get("query"), status, body)
	}
	out := map[string]map[string]string{}
	for _, item := range extractArray(extractMap(resp, "data"), "result") {
		series, _ := item.(map[string]any)
		metric, _ := series["metric"].(map[string]any)
		key := fmt.Sprint(metric["lvl"])
		if metric["lvl"] == nil {
			key = ""
		}
		if len(metric) > 1 || (len(metric) == 1 && metric["lvl"] == nil) {
			t.Fatalf("%s %s: series labels %v", base, params.Get("query"), metric)
		}
		if out[key] != nil {
			t.Fatalf("%s %s: two series for %q", base, params.Get("query"), key)
		}
		out[key] = map[string]string{}
		values, _ := series["values"].([]any)
		for _, raw := range values {
			pair, _ := raw.([]any)
			if len(pair) != 2 {
				t.Fatalf("%s: malformed sample %v", base, raw)
			}
			out[key][strings.TrimSuffix(fmt.Sprint(pair[0]), ".0")] = fmt.Sprint(pair[1])
		}
	}
	return out
}

// TestCompat_UnwrapConversionErrorFirstWindowHeavyRead: a range metric reading
// -backend-heavy-query-min-range (6h) or more whose first evaluation window
// holds the only value its conversion rejects answers Loki's 400, text/plain,
// naming that line (pkg/logql/evaluator.go:726-733: Loki stops at the first
// rejected value), found by the proxy's lookup of the first window beside the
// metric's stats read.
// conformance: semantics/unwrap-conversion-error, status-400, loki_api_v1_query_range
func TestCompat_UnwrapConversionErrorFirstWindowHeavyRead(t *testing.T) {
	now := time.Now()
	fx := slidingLiveFixture{app: fmt.Sprintf("unwrap-converr-first-%d", now.UnixNano()), service: true}
	s0 := now.Add(-10 * time.Hour).Truncate(time.Hour)
	// One logfmt line every 10s for 7 hours; ttl is a number except on line 20
	// (s0+3m27s), inside the first window of a query starting at s0+5m.
	for i := 0; i < 7*360; i++ {
		ttl := strconv.Itoa(i%60 + 1)
		if i == 20 {
			ttl = "5m"
		}
		fx.lines = append(fx.lines, slidingLiveLine{ts: s0.Add(7*time.Second + time.Duration(i)*10*time.Second), msg: fmt.Sprintf("ttl=%s n=%d", ttl, i%4)})
	}
	ingestSlidingFixtures(t, fx)
	sel := fx.selector()
	params := url.Values{
		"query": {`max by (service_name) (max_over_time(` + sel + ` | logfmt | unwrap ttl [5m]))`},
		"start": {strconv.FormatInt(s0.Add(5*time.Minute).UnixNano(), 10)},
		"end":   {strconv.FormatInt(s0.Add(6*time.Hour+55*time.Minute).UnixNano(), 10)},
		"step":  {"60"},
	}
	control := url.Values{"query": {`sum(count_over_time(` + sel + ` [5m]))`}, "start": params["start"], "end": params["end"], "step": {"60"}}
	deadline := time.Now().Add(180 * time.Second)
	for {
		if got := rateCounterMatrix(t, lokiURL, control); len(got) == 1 && len(got[""]) > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("Loki does not answer the count control")
		}
		time.Sleep(5 * time.Second)
	}
	loki := unwrapErrorGet(t, lokiURL, "/loki/api/v1/query_range", params)
	proxy := unwrapErrorGet(t, proxyURL, "/loki/api/v1/query_range", params)
	if loki.status != http.StatusBadRequest || !strings.Contains(loki.body, `ttl="5m"`) {
		t.Fatalf("Loki: %d %s", loki.status, loki.body)
	}
	if proxy.status != loki.status || proxy.contentType != loki.contentType || unwrapErrorShape(t, proxy.body) != unwrapErrorShape(t, loki.body) || !strings.Contains(proxy.body, `ttl="5m"`) {
		t.Fatalf("proxy %d %q %s\nLoki  %d %q %s", proxy.status, proxy.contentType, proxy.body, loki.status, loki.contentType, loki.body)
	}
}
