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
	"testing"
	"time"
)

// unwrapValueForm is one spelling of an unwrapped value: the conversion Loki
// reads it with, and the value of the line's `u` label.
type unwrapValueForm struct {
	name, conv, value string
}

// unwrapValueForms are values Loki's conversions accept that a VictoriaLogs stats
// function reads differently or not at all (the translator gate converts them:
// registry case semantics/unwrap-gate-parsefloat-divergences).
var unwrapValueForms = []unwrapValueForm{
	{"hex float", "", "0x1p4"},
	{"negative hex float with a fraction", "", "-0x1.8p1"},
	{"hex float with separators", "", "0x_1p1_0"},
	{"exponent separators", "", "1e1_0"},
	{"separators", "", "1_000.5"},
	{"exponent 308", "", "1e308"},
	{"largest float", "", "1.7976931348623157e308"},
	{"infinity", "", "infinity"},
	{"negative infinity", "", "-Infinity"},
	{"inf", "", "inf"},
	{"leading comma", "bytes", ",5"},
	{"comma after the point", "bytes", "1.5,0"},
	{"no-break space before the unit", "bytes", "5 kB"},
	{"em space around the unit", "bytes", "5 kB "},
	{"kelvin sign unit", "bytes", "5KB"},
	{"largest size", "bytes", "18EB"},
	{"leading zeros in a duration", "duration", "01m"},
	{"duration", "duration", "1h30m"},
}

// unwrapOverflowForms are syntactically numbers whose conversion overflows:
// Loki makes no sample and fails the query with SampleExtractionErr.
var unwrapOverflowForms = []unwrapValueForm{
	{"float above the range", "", "1e309"},
	{"mantissa that overflows", "", "1000e307"},
	{"just above the largest float", "", "1.8e308"},
	{"hex float above the range", "", "0x1p2000"},
	{"duration beyond int64 nanoseconds", "duration", "2562048h"},
	{"negative duration beyond int64 nanoseconds", "duration", "-2562048h"},
	{"size of 2^64 bytes", "bytes", "19EB"},
	{"size of 16 EiB", "bytes", "16EiB"},
}

// unwrapEqualValues compares two sample values as Loki's float64s: NaN equals
// NaN, an infinity its sign, finite values to 1e-9.
func unwrapEqualValues(a, b string) bool {
	x, xerr := strconv.ParseFloat(a, 64)
	y, yerr := strconv.ParseFloat(b, 64)
	if xerr != nil || yerr != nil {
		return a == b
	}
	switch {
	case math.IsNaN(x) || math.IsNaN(y):
		return math.IsNaN(x) && math.IsNaN(y)
	case math.IsInf(x, 0) || math.IsInf(y, 0):
		return x == y
	}
	return math.Abs(x-y) <= 1e-9*math.Max(1, math.Abs(x))
}

// unwrapInstantSeries is the vector an instant query answers at `at`, by series.
func unwrapInstantSeries(t *testing.T, baseURL, query string, at time.Time) map[string]string {
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
		pair, _ := series["value"].([]interface{})
		if len(pair) != 2 {
			t.Fatalf("%s %s: malformed sample %v", baseURL, query, series["value"])
		}
		out[strings.Join(keys, ",")], _ = pair[1].(string)
	}
	return out
}

// TestCompat_UnwrapValueFormsParity: the values of unwrapValueForms make the
// sample Loki makes, for sum, avg, max and min over time and a rate, in a
// tumbling and a sliding window, grouped and not; NaN too: sum, avg, stddev and
// rate over a window that holds a nan are NaN and max and min ignore it
// (pkg/logql/range_vector.go). A rate is compared on query_range; the over-time
// functions are compared on instant queries grouped by the service, because their
// range answers still differ from Loki's for reasons that are not the value gate.
// conformance: semantics/unwrap-gate-parsefloat-divergences, semantics/unwrap-sample-validity, loki_api_v1_query_range
func TestCompat_UnwrapValueFormsParity(t *testing.T) {
	now := time.Now()
	s0 := now.Add(-6 * time.Hour).Truncate(time.Hour)
	// One logfmt line every 10s cycling through the forms; the nan lines alternate
	// a nan and a number, so a window holds both.
	fx := slidingLiveFixture{app: fmt.Sprintf("unwrap-forms-%d", now.UnixNano()), service: true}
	forms := append(append([]unwrapValueForm{}, unwrapValueForms...), unwrapValueForm{"nan and a number", "", "nan"}, unwrapValueForm{"nan only", "", "NaN"})
	for i := 0; i < 360; i++ {
		f := forms[i%len(forms)]
		value := f.value
		if f.name == "nan and a number" && (i/len(forms))%2 == 1 {
			value = "7"
		}
		fx.lines = append(fx.lines, slidingLiveLine{ts: s0.Add(7*time.Second + time.Duration(i)*10*time.Second), msg: fmt.Sprintf("n=1 form=\"%s\" u=\"%s\"", f.name, value)})
	}
	ingestSlidingFixtures(t, fx)
	sel := fx.selector()
	control := `sum by (service_name) (sum_over_time(` + sel + ` | logfmt | unwrap n [5m]))`
	deadline := time.Now().Add(180 * time.Second)
	for {
		a := unwrapErrorGet(t, lokiURL, "/loki/api/v1/query_range", url.Values{"query": {control}, "start": {strconv.FormatInt(s0.Add(10*time.Minute).UnixNano(), 10)}, "end": {strconv.FormatInt(s0.Add(25*time.Minute).UnixNano(), 10)}, "step": {"60"}})
		if a.status == http.StatusOK && unwrapResultCount(t, a) == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("Loki does not answer the numeric control: %d %s", a.status, a.body)
		}
		time.Sleep(5 * time.Second)
	}
	start, end := s0.Add(10*time.Minute), s0.Add(40*time.Minute)
	for _, f := range forms {
		unwrap := "unwrap u"
		if f.conv != "" {
			unwrap = "unwrap " + f.conv + "(u)"
		}
		pipeline := sel + ` | logfmt | form="` + f.name + `" | ` + unwrap
		for _, shape := range []struct{ name, window string }{{"tumbling", "1m"}, {"sliding", "5m"}} {
			// The core group shares one timeout: the spellings run three functions and two
			// outers (the conversion is the same for every function); the nan forms, whose
			// result depends on the function, run the whole matrix.
			functions, outers := []string{"sum_over_time", "max_over_time", "rate"}, []string{"", "sum by (service_name)"}
			nan := strings.HasPrefix(f.name, "nan")
			if nan {
				functions, outers = []string{"sum_over_time", "avg_over_time", "max_over_time", "min_over_time", "rate"}, []string{"", "sum by (service_name)", "max by (service_name)", "sum"}
			}
			for _, function := range functions {
				for _, outer := range outers {
					if nan && (outer == "max by (service_name)" || (f.name == "nan only" && (function == "max_over_time" || function == "min_over_time"))) {
						// Registered residuals (semantics/unwrap-gate-parsefloat-divergences): a
						// window whose every sample is nan under max or min over time, and a
						// max over series that sum, avg, stddev or rate made NaN.
						continue
					}
					query := function + "(" + pipeline + " [" + shape.window + "])"
					if outer != "" {
						query = outer + " (" + query + ")"
					}
					t.Run(f.name+"/"+shape.name+"/"+function+"/"+outer, func(t *testing.T) {
						if function != "rate" {
							// The over-time functions are compared at instants, which match Loki
							// (registered: semantics/unwrap-over-time-range-windows): their range
							// answers are one step early when the range is the step and the step's
							// bucket when it is not, which says nothing about the value the gate
							// converts. An ungrouped series also lacks the parsed labels Loki keeps
							// (semantics/unwrap-per-series-identity-raw-rows), so only the grouped
							// outer is compared.
							if outer == "" {
								t.Skip("ungrouped over-time series identity is registered apart")
							}
							for at := start; !at.After(end); at = at.Add(time.Minute) {
								loki := unwrapInstantSeries(t, lokiURL, query, at)
								proxy := unwrapInstantSeries(t, proxyURL, query, at)
								if nan && (function == "max_over_time" || function == "min_over_time") {
									// Registered residual (semantics/unwrap-gate-parsefloat-divergences): a
									// window that holds only a nan is NaN for Loki's max and min; the
									// windows that also hold a number stay compared.
									for key, want := range loki {
										if want == "NaN" {
											delete(loki, key)
										}
									}
								}
								if len(loki) != len(proxy) {
									t.Fatalf("%s at %s: Loki has %d series, proxy %d (%v, %v)", query, at.UTC().Format(time.RFC3339), len(loki), len(proxy), loki, proxy)
								}
								for key, want := range loki {
									if got, ok := proxy[key]; !ok || !unwrapEqualValues(got, want) {
										t.Errorf("%s {%s} at %s: proxy %q, Loki %q", query, key, at.UTC().Format(time.RFC3339), got, want)
									}
								}
							}
							return
						}
						loki := slidingRangeSeries(t, lokiURL, query, start, end, time.Minute, nil)
						proxy := slidingRangeSeries(t, proxyURL, query, start, end, time.Minute, nil)
						if len(loki) != len(proxy) {
							t.Fatalf("%s: Loki has %d series, proxy %d", query, len(loki), len(proxy))
						}
						for key, want := range loki {
							got, ok := proxy[key]
							if !ok || len(got) != len(want) {
								t.Fatalf("%s {%s}: proxy %d samples, Loki %d", query, key, len(got), len(want))
							}
							for ts, wv := range want {
								if gv, ok := got[ts]; !ok || !unwrapEqualValues(gv, wv) {
									t.Errorf("%s {%s} t=%s: proxy %q, Loki %q", query, key, ts, gv, wv)
								}
							}
						}
					})
				}
			}
		}
	}
}

// TestCompat_UnwrapOverflowParity: a value that is a number by syntax and
// overflows (ParseFloat's range error, ParseDuration's overflow, ParseBytes's
// "too large") is a SampleExtractionErr for Loki, so the query fails with the
// 400 text, unless the error is dropped after the unwrap. It is no sample, no
// matter the position.
// conformance: semantics/unwrap-gate-parsefloat-divergences, semantics/unwrap-conversion-error, status-400, loki_api_v1_query_range, loki_api_v1_query
func TestCompat_UnwrapOverflowParity(t *testing.T) {
	now := time.Now()
	s0 := now.Add(-6 * time.Hour).Truncate(time.Hour)
	fx := slidingLiveFixture{app: fmt.Sprintf("unwrap-overflow-%d", now.UnixNano()), service: true}
	for i := 0; i < 360; i++ {
		f := unwrapOverflowForms[i%len(unwrapOverflowForms)]
		fx.lines = append(fx.lines, slidingLiveLine{ts: s0.Add(7*time.Second + time.Duration(i)*10*time.Second), msg: fmt.Sprintf("n=1 form=\"%s\" u=\"%s\"", f.name, f.value)})
	}
	ingestSlidingFixtures(t, fx)
	sel := fx.selector()
	window := func(q string) url.Values {
		return url.Values{"query": {q}, "start": {strconv.FormatInt(s0.Add(10*time.Minute).UnixNano(), 10)}, "end": {strconv.FormatInt(s0.Add(40*time.Minute).UnixNano(), 10)}, "step": {"60"}}
	}
	control := `sum by (service_name) (sum_over_time(` + sel + ` | logfmt | unwrap n [5m]))`
	deadline := time.Now().Add(180 * time.Second)
	for {
		a := unwrapErrorGet(t, lokiURL, "/loki/api/v1/query_range", window(control))
		if a.status == http.StatusOK && unwrapResultCount(t, a) == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("Loki does not answer the numeric control: %d %s", a.status, a.body)
		}
		time.Sleep(5 * time.Second)
	}
	for _, f := range unwrapOverflowForms {
		unwrap := "unwrap u"
		if f.conv != "" {
			unwrap = "unwrap " + f.conv + "(u)"
		}
		pipeline := sel + ` | logfmt | form="` + f.name + `" | ` + unwrap
		for _, tc := range []struct {
			name, query string
			want        int
		}{
			{"sum_over_time", `sum_over_time(` + pipeline + ` [5m])`, 400},
			{"max_over_time grouped", `sum by (service_name) (max_over_time(` + pipeline + ` [1m]))`, 400},
			{"rate", `rate(` + pipeline + ` [5m])`, 400},
			{"errors dropped", `sum_over_time(` + pipeline + ` | __error__="" [5m])`, 200},
		} {
			t.Run(f.name+"/"+tc.name, func(t *testing.T) {
				loki := unwrapErrorGet(t, lokiURL, "/loki/api/v1/query_range", window(tc.query))
				if loki.status != tc.want {
					t.Fatalf("Loki answered %d (%s), the captured answer is %d", loki.status, loki.body, tc.want)
				}
				proxy := unwrapErrorGet(t, proxyURL, "/loki/api/v1/query_range", window(tc.query))
				if proxy.status != loki.status {
					t.Fatalf("proxy %d %s, Loki %d %s", proxy.status, proxy.body, loki.status, loki.body)
				}
				if loki.status == http.StatusBadRequest {
					if p, l := unwrapErrorShape(t, proxy.body), unwrapErrorShape(t, loki.body); p != l || !strings.Contains(proxy.body, f.value) {
						t.Fatalf("pipeline error differs\nproxy %s\nloki  %s", proxy.body, loki.body)
					}
				}
			})
		}
	}
}
