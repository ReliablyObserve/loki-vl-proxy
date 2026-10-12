//go:build e2e

package e2e_compat

import (
	"fmt"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"
)

// absent_over_time is one series, labelled by the stream selector's equality
// matchers and valued 1, at the steps whose window (t-range, t] holds no line
// the pipeline keeps, and nothing at the steps that do
// (pkg/logql/evaluator.go:647-664 and :754-778 AbsentRangeVectorEvaluator,
// :1320-1345 absentLabels; the parser stages do not change that: the extractor
// keeps no labels, syntax/extractor.go:44-48). The proxy answered the inverse
// with a parser stage: a series per stream where data exists, nothing where no
// line matches (semantics/absent-over-time-semantics).
//
// The fixtures sit 20 to 22 hours back, outside the hours other tests ingest
// into, in two bursts of ten minutes with a ten minute gap, so a window can be
// full, empty or half and half.

type absentFixtures struct {
	logfmt, json slidingLiveFixture
	s0           time.Time
	id           int64
}

var (
	absentOnce      sync.Once
	absentFixtures_ *absentFixtures
)

func ensureAbsentFixtures(t *testing.T) *absentFixtures {
	t.Helper()
	absentOnce.Do(func() {
		now := time.Now()
		fx := &absentFixtures{s0: now.Add(-21 * time.Hour).Truncate(time.Hour), id: now.UnixNano()}
		fx.logfmt = slidingLiveFixture{app: fmt.Sprintf("absent-lf-%d", fx.id)}
		fx.json = slidingLiveFixture{app: fmt.Sprintf("absent-js-%d", fx.id)}
		for _, burst := range []int{0, 20} { // minutes into the hour
			for i := 0; i < 60; i++ { // a line every 10s for ten minutes, off the whole-10s marks
				ts := fx.s0.Add(time.Duration(burst)*time.Minute + time.Duration(i)*10*time.Second + 3*time.Second)
				fx.logfmt.lines = append(fx.logfmt.lines, slidingLiveLine{ts: ts, msg: fmt.Sprintf(`level=info msg=tick amount=%d`, i%7+1)})
				fx.json.lines = append(fx.json.lines, slidingLiveLine{ts: ts, msg: fmt.Sprintf(`{"level":"info","msg":"tick","amount":%d}`, i%7+1)})
			}
		}
		ingestSlidingFixtures(t, fx.logfmt, fx.json)
		absentFixtures_ = fx
	})
	if absentFixtures_ == nil {
		t.Fatal("the absent_over_time fixtures were not ingested; see the first test that ran")
	}
	return absentFixtures_
}

// waitForLoki polls until Loki's metric path answers for the fixture window; a
// fresh Loki answers empty, which absent_over_time would read as absence.
func (fx *absentFixtures) waitForLoki(t *testing.T) {
	t.Helper()
	at := fx.s0.Add(5 * time.Minute)
	deadline := time.Now().Add(240 * time.Second)
	for _, selector := range []string{`{app="` + fx.logfmt.app + `"}`, `{app="` + fx.json.app + `"}`} {
		for {
			if series := slidingRangeSeries(t, lokiURL, "count_over_time("+selector+" [5m])", at.Add(-time.Minute), at, time.Minute, nil); len(series) > 0 {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("Loki's range metric path answered nothing for %s after 240s", selector)
			}
			time.Sleep(2 * time.Second)
		}
	}
}

func absentShapes(fx *absentFixtures) []struct{ name, expr string } {
	lf, js := `{app="`+fx.logfmt.app+`"}`, `{app="`+fx.json.app+`"}`
	return []struct{ name, expr string }{
		{"selector", lf},
		{"regexp selector has no labels", `{app=~"` + fx.logfmt.app + `"}`},
		{"equality and regexp on one name has no label", `{app="` + fx.logfmt.app + `", app=~".+"}`},
		{"line filter that matches", lf + ` |= "tick"`},
		{"line filter that never matches", lf + ` |= "zzz-never"`},
		{"logfmt", lf + ` | logfmt`},
		{"logfmt, label filter that matches", lf + ` | logfmt | level="info"`},
		{"logfmt, label filter no line passes", lf + ` | logfmt | nonexistant_field="foo"`},
		{"json", js + ` | json`},
		{"json, label filter that matches", js + ` | json | level="info"`},
		{"json, label filter no line passes", js + ` | json | nonexistant_field="foo"`},
		{"logfmt unwrap", lf + ` | logfmt | unwrap amount`},
		{"json unwrap", js + ` | json | unwrap amount`},
		{"unwrap of a label no line has", lf + ` | logfmt | unwrap nonexistant_field`},
		{"stream with no data", `{app="absent-none-` + strconv.FormatInt(fx.id, 10) + `"}`},
	}
}

func compareAbsentSeries(t *testing.T, query string, loki, proxy map[string]map[string]string) {
	t.Helper()
	keys := func(m map[string]map[string]string) []string {
		var out []string
		for k := range m {
			out = append(out, k)
		}
		sort.Strings(out)
		return out
	}
	if fmt.Sprint(keys(loki)) != fmt.Sprint(keys(proxy)) {
		t.Errorf("%s: Loki series %v, proxy series %v", query, keys(loki), keys(proxy))
		return
	}
	for key, want := range loki {
		got := proxy[key]
		if len(got) != len(want) {
			t.Errorf("%s {%s}: Loki has %d points, proxy %d", query, key, len(want), len(got))
		}
		for ts, wv := range want {
			if gv, ok := got[ts]; !ok || gv != wv {
				t.Errorf("%s {%s} t=%s: Loki %s, proxy %q (present: %v)", query, key, ts, wv, gv, ok)
			}
		}
		for ts, gv := range got {
			if _, ok := want[ts]; !ok {
				t.Errorf("%s {%s} t=%s: proxy has %s, Loki has no point", query, key, ts, gv)
			}
		}
	}
}

// TestCompat_AbsentOverTime: every pipeline shape, for ranges equal to, longer
// and shorter than the step, with and without an offset, on a range query over
// the bursts and the gap, and at an instant inside a burst and inside the gap,
// answers Loki's series, steps and labels. A control count_over_time over the
// same window must hold data on both sides, so an unwarmed Loki cannot pass as
// "absent everywhere".
// conformance: semantics/absent-over-time-semantics, window-bounds-and-step-alignment, loki_api_v1_query_range, loki_api_v1_query
func TestCompat_AbsentOverTime(t *testing.T) {
	fx := ensureAbsentFixtures(t)
	fx.waitForLoki(t)
	start, end := fx.s0.Add(-4*time.Minute), fx.s0.Add(36*time.Minute)
	for _, window := range []string{"1m", "150s", "5m"} {
		control := fmt.Sprintf(`count_over_time({app="%s"} [%s])`, fx.logfmt.app, window)
		if len(slidingRangeSeries(t, lokiURL, control, start, end, time.Minute, nil)) == 0 || len(slidingRangeSeries(t, proxyURL, control, start, end, time.Minute, nil)) == 0 {
			t.Fatalf("control %s: Loki or the proxy holds no data for the fixture", control)
		}
		for _, shape := range absentShapes(fx) {
			for _, offset := range []string{"", " offset 5m"} {
				query := fmt.Sprintf(`absent_over_time(%s [%s]%s)`, shape.expr, window, offset)
				t.Run(shape.name+"/"+window+offset, func(t *testing.T) {
					loki := slidingRangeSeries(t, lokiURL, query, start, end, time.Minute, nil)
					proxy := slidingRangeSeries(t, proxyURL, query, start, end, time.Minute, nil)
					compareAbsentSeries(t, query, loki, proxy)
				})
			}
		}
	}
	for _, at := range []time.Duration{5 * time.Minute, 15 * time.Minute, 29*time.Minute + 30*time.Second} {
		for _, shape := range absentShapes(fx) {
			query := fmt.Sprintf(`absent_over_time(%s [2m])`, shape.expr)
			t.Run(fmt.Sprintf("instant/%s/%s", at, shape.name), func(t *testing.T) {
				loki := map[string]string{}
				proxy := map[string]string{}
				for k, v := range unwrapInstant(t, lokiURL, query, fx.s0.Add(at)) {
					loki[k] = v
				}
				for k, v := range unwrapInstant(t, proxyURL, query, fx.s0.Add(at)) {
					proxy[k] = v
				}
				if fmt.Sprint(loki) != fmt.Sprint(proxy) {
					t.Errorf("%s at %s: Loki %v, proxy %v", query, at, loki, proxy)
				}
			})
		}
	}
}

// TestCompat_AbsentOverTimeUnderVectorAggregation: a vector aggregation around
// absent_over_time folds the one series as Loki does: grouping keeps or drops
// its labels, count is 1, stddev is 0.
// conformance: semantics/absent-over-time-semantics, metric-series-identity, loki_api_v1_query_range, loki_api_v1_query
func TestCompat_AbsentOverTimeUnderVectorAggregation(t *testing.T) {
	fx := ensureAbsentFixtures(t)
	fx.waitForLoki(t)
	start, end := fx.s0.Add(-4*time.Minute), fx.s0.Add(36*time.Minute)
	selector := `{app="` + fx.json.app + `"}`
	for _, outer := range []string{"sum", "count", "max", "stddev", "sum by (app)", "sum by (nope)", "sum without (app)", "count without (nope)"} {
		for _, inner := range []string{selector + ` | json`, selector + ` | json | nonexistant_field="foo"`, selector + ` |= "tick"`} {
			query := fmt.Sprintf(`%s (absent_over_time(%s [2m]))`, outer, inner)
			t.Run("range/"+outer+"/"+inner, func(t *testing.T) {
				compareAbsentSeries(t, query,
					slidingRangeSeries(t, lokiURL, query, start, end, time.Minute, nil),
					slidingRangeSeries(t, proxyURL, query, start, end, time.Minute, nil))
			})
			t.Run("instant/"+outer+"/"+inner, func(t *testing.T) {
				for _, at := range []time.Duration{5 * time.Minute, 15 * time.Minute} {
					loki := unwrapInstant(t, lokiURL, query, fx.s0.Add(at))
					proxy := unwrapInstant(t, proxyURL, query, fx.s0.Add(at))
					if fmt.Sprint(loki) != fmt.Sprint(proxy) {
						t.Errorf("%s at %s: Loki %v, proxy %v", query, at, loki, proxy)
					}
				}
			})
		}
	}
	for _, outer := range []string{"topk(1,", "bottomk(2,"} {
		query := fmt.Sprintf(`%s absent_over_time(%s | json [2m]))`, outer, selector)
		t.Run("range/"+outer, func(t *testing.T) {
			compareAbsentSeries(t, query,
				slidingRangeSeries(t, lokiURL, query, start, end, time.Minute, nil),
				slidingRangeSeries(t, proxyURL, query, start, end, time.Minute, nil))
		})
	}
}
