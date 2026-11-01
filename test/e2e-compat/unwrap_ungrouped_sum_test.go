//go:build e2e

package e2e_compat

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// Loki folds every series of a range aggregation into one with a bare outer
// sum (pkg/logql/evaluator.go newVectorAggEvaluator: a grouping of no labels;
// VectorAggEvaluator.Next adds the values of the input vector per step), and a
// rate over an unwrapped label is the sum of its values per second
// (pkg/logql/range_vector.go rateLogs with computeValues). The series of the
// range aggregation are the streams and the labels a parser extracts, so their
// number varies with the data while the answer stays one series per step.
// The proxy answered one series per stream and parsed label set on query_range
// for a window that is not the step, and the same on an instant query over a
// `| json` pipeline (semantics/unwrap-rate-ungrouped-sum-per-stream).

type unwrapSumFixtures struct {
	logfmt, json []slidingLiveFixture
	selLogfmt    string
	selJSON      string
	s0           time.Time
}

var (
	unwrapSumOnce      sync.Once
	unwrapSumFixtures_ *unwrapSumFixtures
)

// ensureUnwrapSumFixtures ingests two groups of three streams, one logfmt and one
// JSON, a line every 10s for 30 minutes per stream (off the whole-10s marks and
// each stream on its own offset, so no line sits on a window edge). Every line
// holds a numeric `amount` and one of three `route` values, so a stream holds
// three series of the parsed label set, and the values differ per line.
func ensureUnwrapSumFixtures(t *testing.T) *unwrapSumFixtures {
	t.Helper()
	unwrapSumOnce.Do(func() {
		now := time.Now()
		id := now.UnixNano()
		fx := &unwrapSumFixtures{
			s0:        now.Add(-9 * time.Hour).Truncate(time.Hour),
			selLogfmt: fmt.Sprintf(`{app=~"uwsum-lf-%d-.+"}`, id),
			selJSON:   fmt.Sprintf(`{app=~"uwsum-js-%d-.+"}`, id),
		}
		var all []slidingLiveFixture
		for _, format := range []string{"lf", "js"} {
			for k, name := range []string{"a", "b", "c"} {
				stream := slidingLiveFixture{app: fmt.Sprintf("uwsum-%s-%d-%s", format, id, name)}
				for i := 0; i < 180; i++ {
					amount := float64((i*3+k*5)%11)*1.25 + 0.5
					route := fmt.Sprintf("/r%d", (i+k)%3)
					ts := fx.s0.Add(time.Duration(3+2*k)*time.Second + time.Duration(i)*10*time.Second)
					msg := fmt.Sprintf(`msg=tick route=%s amount=%v`, route, amount)
					if format == "js" {
						msg = fmt.Sprintf(`{"msg":"tick","route":%q,"amount":%v}`, route, amount)
					}
					stream.lines = append(stream.lines, slidingLiveLine{ts: ts, msg: msg})
				}
				all = append(all, stream)
				if format == "lf" {
					fx.logfmt = append(fx.logfmt, stream)
				} else {
					fx.json = append(fx.json, stream)
				}
			}
		}
		ingestSlidingFixtures(t, all...)
		unwrapSumFixtures_ = fx
	})
	if unwrapSumFixtures_ == nil {
		t.Fatal("the unwrap sum fixtures were not ingested; see the first test that ran")
	}
	return unwrapSumFixtures_
}

// waitForLoki polls until Loki's range metric path answers for the fixture
// window: right after a fresh stack starts it answers empty for a few minutes
// (the ingester reports no bytes until its head blocks are cut), and the old
// fixture would otherwise compare against an empty answer.
func (fx *unwrapSumFixtures) waitForLoki(t *testing.T) {
	t.Helper()
	at := fx.s0.Add(20 * time.Minute)
	deadline := time.Now().Add(240 * time.Second)
	for _, selector := range []string{fx.selLogfmt, fx.selJSON} {
		query := fmt.Sprintf("sum(rate(%s | unwrap amount [5m]))", selector)
		query = strings.Replace(query, "| unwrap", "| "+map[bool]string{true: "logfmt", false: "json"}[selector == fx.selLogfmt]+" | unwrap", 1)
		for {
			if series := slidingRangeSeries(t, lokiURL, query, at.Add(-time.Minute), at, time.Minute, nil); len(series) > 0 {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("Loki's range metric path answered nothing for %s after 240s", selector)
			}
			time.Sleep(2 * time.Second)
		}
	}
}

// unwrapSumShape is one evaluated window: range and step of a range query, or an
// instant query when step is 0.
type unwrapSumShape struct {
	name        string
	window      string
	step        time.Duration
	start, stop time.Duration // offsets from the fixture start; an instant query evaluates at stop
}

var unwrapSumShapes = []unwrapSumShape{
	{"sliding", "5m", time.Minute, 6 * time.Minute, 26 * time.Minute},
	{"tumbling", "5m", 5 * time.Minute, 5 * time.Minute, 25 * time.Minute},
	{"range shorter than the step", "2m", 5 * time.Minute, 5 * time.Minute, 25 * time.Minute},
	{"range longer than the step", "10m", 3 * time.Minute, 12 * time.Minute, 27 * time.Minute},
	{"range that is not a multiple of the step", "90s", time.Minute, 5 * time.Minute, 25 * time.Minute},
	{"instant", "5m", 0, 0, 20 * time.Minute},
}

func (s unwrapSumShape) run(t *testing.T, fx *unwrapSumFixtures, base, query string) map[string]map[string]string {
	t.Helper()
	if s.step == 0 {
		out := map[string]map[string]string{}
		at := fx.s0.Add(s.stop)
		for key, value := range unwrapInstant(t, base, query, at) {
			out[key] = map[string]string{strconv.FormatInt(at.Unix(), 10): value}
		}
		return out
	}
	return slidingRangeSeries(t, base, query, fx.s0.Add(s.start), fx.s0.Add(s.stop), s.step, nil)
}

// compareUnwrapSumSeries asserts the proxy's series set, timestamps and values
// (to 1e-9 relative: the sum of floats in another order) are Loki's, and that
// Loki's answer is not empty.
func compareUnwrapSumSeries(t *testing.T, query string, loki, proxy map[string]map[string]string) {
	t.Helper()
	points := 0
	for _, series := range loki {
		points += len(series)
	}
	if points == 0 {
		t.Fatalf("%s: Loki answered no sample; parity would be empty-vs-empty", query)
	}
	if len(loki) != len(proxy) {
		t.Errorf("%s: Loki has %d series, proxy %d", query, len(loki), len(proxy))
	}
	for key, want := range loki {
		got, ok := proxy[key]
		if !ok {
			t.Errorf("%s: series {%s} missing from the proxy", query, key)
			continue
		}
		if len(got) != len(want) {
			t.Errorf("%s {%s}: proxy has %d samples, Loki %d", query, key, len(got), len(want))
		}
		for ts, wv := range want {
			gv, ok := got[ts]
			if !ok {
				t.Errorf("%s {%s} t=%s: proxy has no sample, Loki %s", query, key, ts, wv)
				continue
			}
			w, werr := strconv.ParseFloat(wv, 64)
			g, gerr := strconv.ParseFloat(gv, 64)
			if werr != nil || gerr != nil || math.Abs(g-w) > 1e-9*math.Max(1, math.Abs(w)) {
				t.Errorf("%s {%s} t=%s: proxy %s, Loki %s", query, key, ts, gv, wv)
			}
		}
	}
}

func unwrapSumQuery(outer, function, selector, parser, window string) string {
	pipeline := selector
	if parser != "" {
		pipeline += " | " + parser
	}
	return fmt.Sprintf("%s(%s(%s | unwrap amount [%s]))", outer, function, pipeline, window)
}

// TestCompat_UnwrapUngroupedSum: `sum(...)` over rate and sum_over_time with
// unwrap is Loki's one series per step, with Loki's values, over a logfmt and a
// JSON stream group, with and without the error filter, grouped by nothing, by a
// stream label and by a parsed label, for a window equal to, shorter than,
// longer than and not a multiple of the step, and at an instant.
// conformance: semantics/unwrap-rate-ungrouped-sum-per-stream, metric-series-identity, window-bounds-and-step-alignment, loki_api_v1_query_range, loki_api_v1_query
func TestCompat_UnwrapUngroupedSum(t *testing.T) {
	fx := ensureUnwrapSumFixtures(t)
	fx.waitForLoki(t)
	type group struct{ name, selector, parser string }
	groups := []group{
		{"logfmt", fx.selLogfmt, "logfmt"},
		{"json", fx.selJSON, "json"},
		{"logfmt without errors", fx.selLogfmt, `logfmt | __error__=""`},
		{"json without errors", fx.selJSON, `json | __error__=""`},
	}
	for _, g := range groups {
		for _, shape := range unwrapSumShapes {
			for _, function := range []string{"rate", "sum_over_time"} {
				if function == "sum_over_time" && shape.step != 0 {
					// A sum_over_time range query is a separate gap
					// (semantics/unwrap-over-time-range-windows).
					continue
				}
				for _, outer := range []string{"sum", "sum by (app)", "sum by (route)"} {
					query := unwrapSumQuery(outer, function, g.selector, g.parser, shape.window)
					t.Run(g.name+"/"+shape.name+"/"+function+"/"+outer, func(t *testing.T) {
						loki := shape.run(t, fx, lokiURL, query)
						proxy := shape.run(t, fx, proxyURL, query)
						if outer == "sum" {
							if len(loki) != 1 {
								t.Fatalf("%s: Loki sanity: %d series, want one", query, len(loki))
							}
							if unwrapHasLabeledSeries(loki) {
								t.Fatalf("%s: Loki sanity: the series carries labels: %v", query, loki)
							}
						}
						compareUnwrapSumSeries(t, query, loki, proxy)
					})
				}
			}
		}
	}
}

// TestCompat_UnwrapUngroupedSumPerSeriesFunctions: a function that takes a value
// per series (avg, max, min over time, rate_counter) under a bare sum answers
// the one series Loki's vector aggregation makes, at an instant and on a range
// query. Their values depend on the series identity the parser gives (the stream
// and the parsed labels, which a VictoriaLogs stats group cannot name), which is
// registered apart (semantics/unwrap-per-series-identity-raw-rows), so this pins
// the shape: one series without labels.
// conformance: semantics/unwrap-rate-ungrouped-sum-per-stream, metric-series-identity
func TestCompat_UnwrapUngroupedSumPerSeriesFunctions(t *testing.T) {
	fx := ensureUnwrapSumFixtures(t)
	fx.waitForLoki(t)
	for _, g := range []struct{ name, selector, parser string }{
		{"logfmt", fx.selLogfmt, "logfmt"},
		{"json", fx.selJSON, "json"},
	} {
		for _, shape := range unwrapSumShapes {
			for _, function := range []string{"avg_over_time", "max_over_time", "min_over_time", "rate_counter"} {
				query := unwrapSumQuery("sum", function, g.selector, g.parser, shape.window)
				t.Run(g.name+"/"+shape.name+"/"+function, func(t *testing.T) {
					loki := shape.run(t, fx, lokiURL, query)
					proxy := shape.run(t, fx, proxyURL, query)
					if len(loki) > 1 || unwrapHasLabeledSeries(loki) {
						t.Fatalf("%s: Loki sanity: %d series with labels: %v", query, len(loki), loki)
					}
					if len(proxy) > 1 || unwrapHasLabeledSeries(proxy) {
						keys := make([]string, 0, len(proxy))
						for k := range proxy {
							keys = append(keys, k)
							if len(keys) == 3 {
								break
							}
						}
						t.Errorf("%s: proxy has %d series, Loki one without labels: %s ...", query, len(proxy), strings.Join(keys, " | "))
					}
				})
			}
		}
	}
}

func unwrapHasLabeledSeries(series map[string]map[string]string) bool {
	for key := range series {
		if len(metricKeyLabels(key)) > 0 {
			return true
		}
	}
	return false
}

func TestCompat_UnwrapUngroupedSumLabelIdentity(t *testing.T) {
	for _, key := range []string{"", metricJSONKey(map[string]string{})} {
		if unwrapHasLabeledSeries(map[string]map[string]string{key: {"1.5": "1"}}) {
			t.Fatalf("unlabelled series reported as labelled: %q", key)
		}
	}
	for _, key := range []string{"app=api", metricJSONKey(map[string]string{"app": ""}), metricJSONKey(map[string]string{"app": "api,a=b"})} {
		if !unwrapHasLabeledSeries(map[string]map[string]string{key: {"1.5": "1"}}) {
			t.Fatalf("labelled series reported as unlabelled: %q", key)
		}
	}
}
