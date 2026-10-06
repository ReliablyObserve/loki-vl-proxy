//go:build e2e

package e2e_compat

import (
	"fmt"
	"math"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// Loki makes a sample of a line only when its unwrapped label is present and
// converts (pkg/logql/log/metrics_extraction.go): a missing or empty label
// makes no sample, a value convertFloat/convertDuration/convertBytes rejects is
// marked __error__ and `| __error__=""` drops it. VictoriaLogs' stats parse
// leniently (units become numbers, max compares strings, an empty group answers
// "" or NaN), so the proxy has to select the numeric rows itself.

type unwrapValidityFixture struct {
	app string
	s0  time.Time
}

var (
	unwrapValidityOnce     sync.Once
	unwrapValidityFixture_ *unwrapValidityFixture
)

// ensureUnwrapValidityFixture ingests one stream of logfmt lines, one every 10s
// for 30 minutes (off the whole-10s marks, so no line sits on a window edge),
// into Loki and VictoriaLogs. The fields:
//
//	n     a number on every line
//	m     a number on every third line, absent otherwise
//	val   a number on even lines, a non-number ("x3") on odd lines
//	ttl   "86282s" on every line: a unit string, not a number
//	dur   1.5s, 500ms, then 1d and 42, which Go's time.ParseDuration rejects
//	size  2KiB, 1kb, then 1.5B: humanize.ParseBytes reads all three
//	big   "5 kB" (what go-humanize prints), 1PB, 1EiB: uppercase B and large prefixes
func ensureUnwrapValidityFixture(t *testing.T) *unwrapValidityFixture {
	t.Helper()
	unwrapValidityOnce.Do(func() {
		now := time.Now()
		fx := &unwrapValidityFixture{
			app: fmt.Sprintf("unwrap-validity-%d", now.UnixNano()),
			s0:  now.Add(-9 * time.Hour).Truncate(time.Hour),
		}
		live := slidingLiveFixture{app: fx.app, service: true}
		durations := []string{"1.5s", "500ms", "1d", "42"}
		sizes := []string{"2KiB", "1kb", "1.5B"}
		big := []string{`"5 kB"`, "1PB", "1EiB"}
		for i := 0; i < 180; i++ {
			parts := []string{fmt.Sprintf("n=%d", i%7+1)}
			if i%3 == 0 {
				parts = append(parts, fmt.Sprintf("m=%d", i%5+1))
			}
			if i%2 == 0 {
				parts = append(parts, fmt.Sprintf("val=%d", i%9+1))
			} else {
				parts = append(parts, fmt.Sprintf("val=x%d", i%9+1))
			}
			parts = append(parts, "ttl=86282s", "dur="+durations[i%4], "size="+sizes[i%3], "big="+big[i%3])
			live.lines = append(live.lines, slidingLiveLine{ts: fx.s0.Add(7*time.Second + time.Duration(i)*10*time.Second), msg: strings.Join(parts, " ")})
		}
		ingestSlidingFixtures(t, live)
		unwrapValidityFixture_ = fx
	})
	if unwrapValidityFixture_ == nil {
		t.Fatal("the unwrap validity fixture was not ingested; see the first test that ran")
	}
	return unwrapValidityFixture_
}

// unwrapInstant runs an instant query and returns series -> value, failing
// unless the answer is a 200 vector.
func unwrapInstant(t *testing.T, base, query string, at time.Time) map[string]string {
	t.Helper()
	params := url.Values{"query": {query}, "time": {strconv.FormatInt(at.UnixNano(), 10)}}
	status, body, resp := doJSONGET(t, base+"/loki/api/v1/query?"+params.Encode(), map[string]string{"X-Scope-OrgID": "0"})
	if status != http.StatusOK || resp["status"] != "success" || resp["warnings"] != nil {
		t.Fatalf("%s %s: unhealthy answer %d %s", base, query, status, body)
	}
	out := map[string]string{}
	for _, item := range extractArray(extractMap(resp, "data"), "result") {
		series, _ := item.(map[string]interface{})
		metric, _ := series["metric"].(map[string]interface{})
		keys := make([]string, 0, len(metric))
		for k, v := range metric {
			keys = append(keys, fmt.Sprintf("%s=%v", k, v))
		}
		sort.Strings(keys)
		pair, _ := series["value"].([]interface{})
		if len(pair) != 2 {
			t.Fatalf("%s %s: malformed sample %v", base, query, series["value"])
		}
		out[strings.Join(keys, ",")] = fmt.Sprint(pair[1])
	}
	return out
}

// TestCompat_UnwrapMissingAndNonNumeric: an instant unwrap metric over the
// fixture has exactly Loki's series and values: lines without the label and
// values that do not convert make no sample, duration() and bytes() parse like
// Go and humanize, and rate over an unwrapped label is its sum per second.
// conformance: semantics/unwrap-sample-validity, parser-error-and-label-collision, loki_api_v1_query_range
func TestCompat_UnwrapMissingAndNonNumeric(t *testing.T) {
	fx := ensureUnwrapValidityFixture(t)
	selector := `{service_name="` + fx.app + `"}`
	at := fx.s0.Add(25 * time.Minute)
	const window = "[10m]"
	for _, tc := range []struct {
		name, query string
		wantEmpty   bool // Loki has no sample at all
	}{
		{"sum of a numeric field", `sum by (service_name) (sum_over_time(` + selector + ` | logfmt | unwrap n | __error__="" ` + window + `))`, false},
		{"max skips the non-numeric lines", `max by (service_name) (max_over_time(` + selector + ` | logfmt | unwrap val | __error__="" ` + window + `))`, false},
		{"min of a sparse field", `min by (service_name) (min_over_time(` + selector + ` | logfmt | unwrap m | __error__="" ` + window + `))`, false},
		{"rate is a sum per second", `sum by (service_name) (rate(` + selector + ` | logfmt | unwrap n | __error__="" ` + window + `))`, false},
		{"duration() reads Go durations", `sum by (service_name) (sum_over_time(` + selector + ` | logfmt | unwrap duration(dur) | __error__="" ` + window + `))`, false},
		{"bytes() reads humanize sizes", `sum by (service_name) (sum_over_time(` + selector + ` | logfmt | unwrap bytes(size) | __error__="" ` + window + `))`, false},
		{"bytes() reads an uppercase B and large prefixes", `max by (service_name) (max_over_time(` + selector + ` | logfmt | unwrap bytes(big) | __error__="" ` + window + `))`, false},
		{"bytes() sums an uppercase B and large prefixes", `sum by (service_name) (sum_over_time(` + selector + ` | logfmt | unwrap bytes(big) | __error__="" ` + window + `))`, false},
		{"min of bytes() with an uppercase B", `min by (service_name) (min_over_time(` + selector + ` | logfmt | unwrap bytes(big) | __error__="" ` + window + `))`, false},
		{"a unit string is not a number", `sum by (service_name) (sum_over_time(` + selector + ` | logfmt | unwrap ttl | __error__="" ` + window + `))`, true},
		{"max of a unit string", `max by (service_name) (max_over_time(` + selector + ` | logfmt | unwrap ttl | __error__="" ` + window + `))`, true},
		{"missing label after a parser", `max by (service_name) (max_over_time(` + selector + ` | logfmt | unwrap nosuch ` + window + `))`, true},
		{"missing label, no parser, sum", `sum by (service_name) (sum_over_time(` + selector + ` | unwrap nosuch ` + window + `))`, true},
		{"missing label, no parser, max", `max by (service_name) (max_over_time(` + selector + ` | unwrap nosuch ` + window + `))`, true},
		{"missing label, no parser, ungrouped", `sum(sum_over_time(` + selector + ` | unwrap nosuch ` + window + `))`, true},
		{"missing label, no parser, rate", `sum by (service_name) (rate(` + selector + ` | unwrap nosuch ` + window + `))`, true},
		{"missing label, series-level aggregation", `max by (service_name) (sum_over_time(` + selector + ` | unwrap nosuch ` + window + `))`, true},
		{"non-numeric structured metadata", `max by (service_name) (max_over_time(` + selector + ` | unwrap detected_level | __error__="" ` + window + `))`, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			want := unwrapInstant(t, lokiURL, tc.query, at)
			if tc.wantEmpty && len(want) != 0 {
				t.Fatalf("Loki answered %v; the case expects no sample", want)
			}
			if !tc.wantEmpty && len(want) == 0 {
				t.Fatalf("Loki answered no sample; the case would compare empty with empty")
			}
			got := unwrapInstant(t, proxyURL, tc.query, at)
			if len(got) != len(want) {
				t.Fatalf("proxy series %v, Loki series %v", got, want)
			}
			for series, w := range want {
				if g, ok := got[series]; !ok || g != w {
					t.Errorf("{%s}: proxy %q (present=%v), Loki %q", series, g, ok, w)
				}
			}
		})
	}
}

// TestCompat_UnwrapRangeSamplesAreNumbers: a range query answered by VictoriaLogs
// stats buckets or the raw evaluator has Loki's series, and every sample is a
// number Loki could have produced: never "", NaN or a unit string. A sliding
// window the buckets answer exactly (rate over an unwrapped label) also has
// Loki's value at every step.
// conformance: semantics/unwrap-sample-validity, parser-error-and-label-collision, loki_api_v1_query_range
func TestCompat_UnwrapRangeSamplesAreNumbers(t *testing.T) {
	fx := ensureUnwrapValidityFixture(t)
	selector := `{service_name="` + fx.app + `"}`
	start, end := fx.s0.Add(10*time.Minute), fx.s0.Add(25*time.Minute)
	for _, tc := range []struct {
		name, query string
		wantEmpty   bool
		allowed     map[float64]bool // the values a window of the fixture can have; nil: any finite number
		exact       bool             // every sample equals Loki's at its timestamp
	}{
		{"max skips the non-numeric lines", `max by (service_name) (max_over_time(` + selector + ` | logfmt | unwrap val | __error__="" [1m]))`, false,
			map[float64]bool{1: true, 2: true, 3: true, 4: true, 5: true, 6: true, 7: true, 8: true, 9: true}, false},
		{"min skips the non-numeric lines", `min by (service_name) (min_over_time(` + selector + ` | logfmt | unwrap val | __error__="" [2m]))`, false,
			map[float64]bool{1: true, 2: true, 3: true, 4: true, 5: true, 6: true, 7: true, 8: true, 9: true}, false},
		{"sum of a numeric field", `sum by (service_name) (sum_over_time(` + selector + ` | logfmt | unwrap n | __error__="" [1m]))`, false, nil, false},
		{"a unit string is not a number", `max by (service_name) (max_over_time(` + selector + ` | logfmt | unwrap ttl | __error__="" [1m]))`, true, nil, false},
		{"missing label after a parser", `sum by (service_name) (sum_over_time(` + selector + ` | logfmt | unwrap nosuch [1m]))`, true, nil, false},
		{"missing label, no parser", `max by (service_name) (max_over_time(` + selector + ` | unwrap nosuch [1m]))`, true, nil, false},
		{"missing label, no parser, sliding window", `sum by (service_name) (sum_over_time(` + selector + ` | unwrap nosuch [3m]))`, true, nil, false},
		{"missing label, rate", `sum by (service_name) (rate(` + selector + ` | logfmt | unwrap nosuch [2m]))`, true, nil, false},
		{"rate of a numeric field", `sum by (service_name) (rate(` + selector + ` | logfmt | unwrap n | __error__="" [2m]))`, false, nil, true},
		{"rate of a sparse field", `sum by (service_name) (rate(` + selector + ` | logfmt | unwrap m | __error__="" [3m]))`, false, nil, true},
		{"quantile, tumbling window", `sum by (service_name) (quantile_over_time(0.5, ` + selector + ` | logfmt | unwrap n | __error__="" [1m]))`, false, nil, false},
		{"rate_counter, tumbling window", `sum by (service_name) (rate_counter(` + selector + ` | logfmt | unwrap n | __error__="" [1m]))`, false, nil, false},
		{"rate of a duration, sliding window", `sum by (service_name) (rate(` + selector + ` | logfmt | unwrap duration(dur) | __error__="" [2m]))`, false, nil, true},
		{"max of bytes() with an uppercase B", `max by (service_name) (max_over_time(` + selector + ` | logfmt | unwrap bytes(big) | __error__="" [1m]))`, false, nil, false},
		{"rate of a size, sliding window", `sum by (service_name) (rate(` + selector + ` | logfmt | unwrap bytes(size) | __error__="" [3m]))`, false, nil, true},
		{"rate of a conversion, tumbling window", `sum by (service_name) (rate(` + selector + ` | logfmt | unwrap duration(dur) | __error__="" [1m]))`, false, nil, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			loki := slidingRangeSeries(t, lokiURL, tc.query, start, end, time.Minute, nil)
			if tc.wantEmpty && len(loki) != 0 {
				t.Fatalf("Loki answered %d series; the case expects none", len(loki))
			}
			if !tc.wantEmpty && len(loki) == 0 {
				t.Fatal("Loki answered no series; the case would compare empty with empty")
			}
			proxy := slidingRangeSeries(t, proxyURL, tc.query, start, end, time.Minute, nil)
			if tc.exact {
				assertSlidingParity(t, tc.query, loki, proxy)
			}
			if len(proxy) != len(loki) {
				t.Fatalf("proxy answered %d series (%v), Loki %d", len(proxy), proxy, len(loki))
			}
			for key, points := range proxy {
				if len(points) == 0 {
					t.Errorf("series {%s} has no samples", key)
				}
				for ts, value := range points {
					f, err := strconv.ParseFloat(value, 64)
					if err != nil || math.IsNaN(f) || math.IsInf(f, 0) {
						t.Errorf("{%s} t=%s: sample %q is not a number", key, ts, value)
						continue
					}
					if tc.allowed != nil && !tc.allowed[f] {
						t.Errorf("{%s} t=%s: sample %v is not a value of the numeric lines", key, ts, f)
					}
				}
			}
		})
	}
}
