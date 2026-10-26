//go:build e2e

package e2e_compat

import (
	"fmt"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"testing"
	"time"
)

// TestCompat_UnwrapExtentCacheRefresh: an unwrap metric asked again with its
// window moved a step at a time, re-run, moved back and widened (the proxy
// answers the repeated buckets from its stats extent cache) answers Loki's
// series and values, and Loki's 400 as soon as the window holds a value Loki's
// conversion rejects, whether that value's bucket was read fresh or stored by
// an earlier window; a nan is a sample (sum NaN), and the error filter after the
// unwrap drops the rejected value.
// conformance: quality/unwrap-query-range-extent-cache, semantics/unwrap-conversion-error, loki_api_v1_query_range
func TestCompat_UnwrapExtentCacheRefresh(t *testing.T) {
	now := time.Now()
	fx := slidingLiveFixture{app: fmt.Sprintf("unwrap-extent-%d", now.UnixNano()), service: true}
	s0 := now.Add(-5 * time.Hour).Truncate(time.Hour)
	// A logfmt line every 10s for an hour, 7s past each 10s mark: w is a number
	// but "bad" on line 240 (s0+40m07s), x a number but "NaN" on line 120
	// (s0+20m07s).
	for i := 0; i < 360; i++ {
		w, x := strconv.Itoa(i%7+1), strconv.Itoa(i%5+1)
		if i == 240 {
			w = "bad"
		}
		if i == 120 {
			x = "NaN"
		}
		fx.lines = append(fx.lines, slidingLiveLine{ts: s0.Add(7*time.Second + time.Duration(i)*10*time.Second), msg: "w=" + w + " x=" + x})
	}
	ingestSlidingFixtures(t, fx)
	sel := fx.selector()
	window := func(q string, from, to time.Duration) url.Values {
		return url.Values{"query": {q}, "start": {strconv.FormatInt(s0.Add(from).UnixNano(), 10)}, "end": {strconv.FormatInt(s0.Add(to).UnixNano(), 10)}, "step": {"60"}}
	}
	control := `sum by (service_name) (sum_over_time(` + sel + ` | logfmt | unwrap x [5m]))`
	deadline := time.Now().Add(180 * time.Second)
	for {
		a := unwrapErrorGet(t, lokiURL, "/loki/api/v1/query_range", window(control, 10*time.Minute, 15*time.Minute))
		if a.status == http.StatusOK && unwrapResultCount(t, a) == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("Loki does not answer the numeric control: %d %s", a.status, a.body)
		}
		time.Sleep(5 * time.Second)
	}

	// Windows in order: the rejected value enters only the seventh, in its fresh
	// part; the re-run reads it from stored buckets.
	var windows [][2]time.Duration
	for k := 0; k < 7; k++ {
		windows = append(windows, [2]time.Duration{10*time.Minute + time.Duration(k)*time.Minute, 35*time.Minute + time.Duration(k)*time.Minute})
	}
	windows = append(windows, windows[6], windows[0], [2]time.Duration{5 * time.Minute, 45 * time.Minute})
	for _, query := range []string{
		`sum by (service_name) (sum_over_time(` + sel + ` | logfmt | unwrap w [5m]))`,
		`sum by (service_name) (sum_over_time(` + sel + ` | logfmt | unwrap w | __error__="" [5m]))`,
		`max by (service_name) (max_over_time(` + sel + ` | logfmt | unwrap w [5m]))`,
		control,
	} {
		for i, w := range windows {
			t.Run(fmt.Sprintf("%s/%d", query, i), func(t *testing.T) {
				params := window(query, w[0], w[1])
				loki := unwrapErrorGet(t, lokiURL, "/loki/api/v1/query_range", params)
				proxy := unwrapErrorGet(t, proxyURL, "/loki/api/v1/query_range", params)
				if proxy.status != loki.status {
					t.Fatalf("proxy %d %s, Loki %d %s", proxy.status, proxy.body, loki.status, loki.body)
				}
				if loki.status == http.StatusBadRequest {
					if p, l := unwrapErrorShape(t, proxy.body), unwrapErrorShape(t, loki.body); p != l {
						t.Fatalf("pipeline error differs\nproxy %s\nloki  %s", p, l)
					}
					return
				}
				if loki.status != http.StatusOK {
					t.Fatalf("Loki answered %d %s", loki.status, loki.body)
				}
				lv := slidingRangeSeries(t, lokiURL, query, s0.Add(w[0]), s0.Add(w[1]), time.Minute, nil)
				pv := slidingRangeSeries(t, proxyURL, query, s0.Add(w[0]), s0.Add(w[1]), time.Minute, nil)
				if len(lv) == 0 || len(pv) != len(lv) {
					t.Fatalf("Loki has %d series, proxy %d", len(lv), len(pv))
				}
				for key, want := range lv {
					for ts, wv := range want {
						gv := pv[key][ts]
						a, _ := strconv.ParseFloat(wv, 64)
						b, err := strconv.ParseFloat(gv, 64)
						if err != nil || !(a == b || math.Abs(a-b) <= 1e-9*math.Max(1, math.Abs(a)) || (math.IsNaN(a) && math.IsNaN(b))) {
							t.Fatalf("{%s} at %s: proxy %q, Loki %q", key, ts, gv, wv)
						}
					}
				}
			})
		}
	}
}
