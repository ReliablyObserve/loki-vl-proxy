//go:build e2e

package e2e_compat

import (
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"
)

// Loki's first_over_time and last_over_time are the first and the last sample,
// in timestamp order, of each series in the window (t-range, t]
// (pkg/logql/range_vector.go FirstOverTime, LastOverTime), and an outer
// aggregation folds those values per step (pkg/logql/evaluator.go
// VectorAggEvaluator.Next). VictoriaLogs has no first or last stats function: the
// proxy asked it for one and answered 422 (semantics/unwrap-first-last-under-aggregation).
// It now reads each series' value with row_min(_time, v) or row_max(_time, v).
//
// Every line of this fixture carries its own `id` label, so a series is a line
// and the stats grouping of the proxy (the stream and the line) is exactly
// Loki's series identity; a stream with several lines per parsed label set is
// registered apart (semantics/unwrap-per-series-identity-raw-rows).

type firstLastFixtures struct {
	logfmt, json string // selectors
	s0           time.Time
}

var (
	firstLastOnce sync.Once
	firstLastFx   *firstLastFixtures
)

func ensureFirstLastFixtures(t *testing.T) *firstLastFixtures {
	t.Helper()
	firstLastOnce.Do(func() {
		now := time.Now()
		id := now.UnixNano()
		fx := &firstLastFixtures{
			s0:     now.Add(-9 * time.Hour).Truncate(time.Hour),
			logfmt: fmt.Sprintf(`{app=~"flat-lf-%d-.+"}`, id),
			json:   fmt.Sprintf(`{app=~"flat-js-%d-.+"}`, id),
		}
		var all []slidingLiveFixture
		for _, format := range []string{"lf", "js"} {
			for k, name := range []string{"a", "b", "c"} {
				stream := slidingLiveFixture{app: fmt.Sprintf("flat-%s-%d-%s", format, id, name)}
				for i := 0; i < 180; i++ {
					amount := float64((i*7+k*3)%13)*1.25 + 0.5
					ts := fx.s0.Add(time.Duration(3+2*k)*time.Second + time.Duration(i)*10*time.Second)
					msg := fmt.Sprintf(`msg=tick id=%d amount=%v`, i, amount)
					if format == "js" {
						msg = fmt.Sprintf(`{"msg":"tick","id":%d,"amount":%v}`, i, amount)
					}
					stream.lines = append(stream.lines, slidingLiveLine{ts: ts, msg: msg})
				}
				all = append(all, stream)
			}
		}
		ingestSlidingFixtures(t, all...)
		firstLastFx = fx
	})
	if firstLastFx == nil {
		t.Fatal("the first/last fixtures were not ingested; see the first test that ran")
	}
	return firstLastFx
}

func firstLastQuery(outer, function, selector, parser, window string) string {
	return fmt.Sprintf("%s(%s(%s | %s | unwrap amount [%s]))", outer, function, selector, parser, window)
}

func seriesKeys(series map[string]map[string]string) []string {
	keys := make([]string, 0, len(series))
	for k := range series {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// TestCompat_UnwrapFirstLastUnderAggregation: sum, avg, min, max and count (also
// by (app)) over first_over_time and last_over_time with unwrap answer 200 with
// Loki's series. At an instant the values are Loki's; on query_range the window
// each point reads is the step-sized bucket native stats_query_range gives, as
// for avg, max and min over time, so only the series are compared there
// (semantics/unwrap-over-time-range-windows tracks the window).
// conformance: semantics/unwrap-first-last-under-aggregation, metric-series-identity, loki_api_v1_query_range, loki_api_v1_query
func TestCompat_UnwrapFirstLastUnderAggregation(t *testing.T) {
	fx := ensureFirstLastFixtures(t)
	ufx := &unwrapSumFixtures{selLogfmt: fx.logfmt, selJSON: fx.json, s0: fx.s0}
	ufx.waitForLoki(t)
	for _, g := range []struct{ name, selector, parser string }{{"logfmt", fx.logfmt, "logfmt"}, {"json", fx.json, "json"}} {
		for _, function := range []string{"first_over_time", "last_over_time"} {
			for _, outer := range []string{"sum", "avg", "min", "max", "count", "sum by (app)", "max by (app)"} {
				for _, shape := range []unwrapSumShape{
					{"instant", "5m", 0, 0, 20 * time.Minute},
					{"sliding", "5m", time.Minute, 6 * time.Minute, 26 * time.Minute},
					{"tumbling", "5m", 5 * time.Minute, 5 * time.Minute, 25 * time.Minute},
				} {
					query := firstLastQuery(outer, function, g.selector, g.parser, shape.window)
					t.Run(g.name+"/"+function+"/"+outer+"/"+shape.name, func(t *testing.T) {
						loki := shape.run(t, ufx, lokiURL, query)
						proxy := shape.run(t, ufx, proxyURL, query)
						if shape.step == 0 {
							compareUnwrapSumSeries(t, query, loki, proxy)
							return
						}
						if len(loki) == 0 {
							t.Fatalf("%s: Loki answered no series", query)
						}
						lk, pk := seriesKeys(loki), seriesKeys(proxy)
						if fmt.Sprint(lk) != fmt.Sprint(pk) {
							t.Errorf("%s: Loki series %v, proxy %v", query, lk, pk)
						}
					})
				}
			}
		}
	}
}

// TestCompat_UnwrapFirstLastTopkBottomk: topk and bottomk over the per-series
// values answer 200 and at most k series per step on both backends. Only the
// values are compared: with a parser the proxy's series carry the line as a `_msg`
// label (the stats grouping that stands for the parsed labels), where Loki's
// carry the parsed labels (semantics/unwrap-per-series-identity-raw-rows).
// conformance: semantics/unwrap-first-last-under-aggregation, loki_api_v1_query
func TestCompat_UnwrapFirstLastTopkBottomk(t *testing.T) {
	fx := ensureFirstLastFixtures(t)
	ufx := &unwrapSumFixtures{selLogfmt: fx.logfmt, selJSON: fx.json, s0: fx.s0}
	ufx.waitForLoki(t)
	at := fx.s0.Add(20 * time.Minute)
	for _, g := range []struct{ name, selector, parser string }{{"logfmt", fx.logfmt, "logfmt"}, {"json", fx.json, "json"}} {
		for _, function := range []string{"first_over_time", "last_over_time"} {
			for _, op := range []string{"topk", "bottomk"} {
				query := fmt.Sprintf("%s(3, %s(%s | %s | unwrap amount [5m]))", op, function, g.selector, g.parser)
				t.Run(g.name+"/"+function+"/"+op, func(t *testing.T) {
					loki := unwrapInstant(t, lokiURL, query, at)
					proxy := unwrapInstant(t, proxyURL, query, at)
					if len(loki) != 3 || len(proxy) != 3 {
						t.Fatalf("%s: Loki %d series, proxy %d, want 3", query, len(loki), len(proxy))
					}
					values := func(m map[string]string) []string {
						out := make([]string, 0, len(m))
						for _, v := range m {
							out = append(out, v)
						}
						sort.Strings(out)
						return out
					}
					if fmt.Sprint(values(loki)) != fmt.Sprint(values(proxy)) {
						t.Errorf("%s: Loki values %v, proxy %v", query, values(loki), values(proxy))
					}
				})
			}
		}
	}
}

// TestCompat_UnwrapFirstLastRangeLevelBy: `agg(fn(... [5m]) by (app))` groups the
// samples by app before the range function, so first and last are taken over each
// app's samples and differ from each other and from sum_over_time (every line of
// the fixture is a series of its own, where the aggregations above cannot tell
// them apart). The instant value is Loki's; on query_range the series are
// compared (the point values follow semantics/unwrap-over-time-range-windows).
// conformance: semantics/unwrap-first-last-under-aggregation, metric-series-identity, loki_api_v1_query_range, loki_api_v1_query
func TestCompat_UnwrapFirstLastRangeLevelBy(t *testing.T) {
	fx := ensureFirstLastFixtures(t)
	ufx := &unwrapSumFixtures{selLogfmt: fx.logfmt, selJSON: fx.json, s0: fx.s0}
	ufx.waitForLoki(t)
	for _, g := range []struct{ name, selector, parser string }{{"logfmt", fx.logfmt, "logfmt"}, {"json", fx.json, "json"}} {
		for _, outer := range []string{"sum", "max", "count"} {
			for _, function := range []string{"first_over_time", "last_over_time"} {
				for _, shape := range []unwrapSumShape{
					{"instant", "5m", 0, 0, 20 * time.Minute},
					{"sliding", "5m", time.Minute, 6 * time.Minute, 26 * time.Minute},
				} {
					query := fmt.Sprintf("%s(%s(%s | %s | unwrap amount [%s]) by (app))", outer, function, g.selector, g.parser, shape.window)
					t.Run(g.name+"/"+function+"/"+outer+"/"+shape.name, func(t *testing.T) {
						loki := shape.run(t, ufx, lokiURL, query)
						proxy := shape.run(t, ufx, proxyURL, query)
						if shape.step == 0 {
							compareUnwrapSumSeries(t, query, loki, proxy)
							return
						}
						if len(loki) == 0 {
							t.Fatalf("%s: Loki answered no series", query)
						}
						if lk, pk := seriesKeys(loki), seriesKeys(proxy); fmt.Sprint(lk) != fmt.Sprint(pk) {
							t.Errorf("%s: Loki series %v, proxy %v", query, lk, pk)
						}
					})
				}
			}
		}
	}
	// First and last differ in the fixture (the sanity the shapes above rely on).
	at := fx.s0.Add(20 * time.Minute)
	q := func(fn string) map[string]string {
		return unwrapInstant(t, lokiURL, fmt.Sprintf("sum(%s(%s | logfmt | unwrap amount [5m]) by (app))", fn, fx.logfmt), at)
	}
	if fmt.Sprint(q("first_over_time")) == fmt.Sprint(q("last_over_time")) {
		t.Fatal("Loki's first and last are equal on the fixture: the test cannot tell them apart")
	}
}
