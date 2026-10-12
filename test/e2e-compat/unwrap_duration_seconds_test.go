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

// TestCompat_UnwrapDurationSeconds: Loki converts `unwrap duration_seconds(x)`
// with convertDuration, exactly as `unwrap duration(x)` (pkg/logql/syntax/extractor.go:63-71,
// pkg/logql/log/metrics_extraction.go convertDuration: time.ParseDuration, in
// seconds). The proxy answered an empty matrix, because it translated only
// duration() and bytes() (semantics/unwrap-duration-seconds). The values Go's
// time.ParseDuration reads (250ms, 1.5s, 1h, -1s, 5.s, .5s) are rated and
// summed like Loki does (values compared) on range and instant windows; the
// over-time shapes whose range windows are a separate open gap are checked for
// their series and for the proxy answering duration_seconds() as duration(); a
// value Go rejects ("abc", a plain number) fails the query with Loki's 400
// SampleExtractionErr text unless a label filter after the unwrap drops the
// error. The expected Loki statuses come from the sources, not a capture.
// conformance: semantics/unwrap-duration-seconds, loki_api_v1_query_range, loki_api_v1_query, status-400
func TestCompat_UnwrapDurationSeconds(t *testing.T) {
	now := time.Now()
	// Older than every other unwrap fixture, so no window of theirs sees it.
	s0 := now.Add(-21 * time.Hour).Truncate(time.Hour)
	// One logfmt line every 10s for 30 minutes, 7s past each 10s mark. okdur
	// cycles through the forms time.ParseDuration reads; bad is text on every
	// line; n is the same plain number on every line, so every line parses to one
	// label set and a stream is one series (the series identity of a function
	// taken per series is a separate gap).
	okdurs := []string{"250ms", "1.5s", "1h", "-1s", "5.s", ".5s"}
	fx := slidingLiveFixture{app: fmt.Sprintf("unwrap-durs-%d", now.UnixNano()), service: true}
	for i := 0; i < 180; i++ {
		fx.lines = append(fx.lines, slidingLiveLine{
			ts:  s0.Add(7*time.Second + time.Duration(i)*10*time.Second),
			msg: fmt.Sprintf("okdur=%s bad=abc n=3", okdurs[i%len(okdurs)]),
		})
	}
	ingestSlidingFixtures(t, fx)
	sel := fx.selector()

	window := func(q string) url.Values {
		return url.Values{"query": {q}, "start": {strconv.FormatInt(s0.Add(10*time.Minute).UnixNano(), 10)},
			"end": {strconv.FormatInt(s0.Add(25*time.Minute).UnixNano(), 10)}, "step": {"60"}}
	}
	const rangePath, instantPath = "/loki/api/v1/query_range", "/loki/api/v1/query"

	// Right after a fresh stack starts Loki answers range metrics of a new
	// stream empty for a few minutes: wait for the control.
	control := `sum by (service_name) (sum_over_time(` + sel + ` | logfmt | unwrap duration(okdur) [5m]))`
	deadline := time.Now().Add(240 * time.Second)
	for {
		if len(slidingRangeSeries(t, lokiURL, control, s0.Add(10*time.Minute), s0.Add(25*time.Minute), time.Minute, nil)) > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("Loki's range metric path answered nothing for the duration_seconds fixture after 240s")
		}
		time.Sleep(5 * time.Second)
	}

	// The values of the shapes that are exact today are compared with Loki's.
	for _, shape := range []string{
		`sum by (service_name) (rate(%s | logfmt | unwrap %s(okdur) [5m]))`,
		`sum(rate(%s | logfmt | unwrap %s(okdur) [5m]))`,
	} {
		query := fmt.Sprintf(shape, sel, "duration_seconds")
		t.Run("range/"+query, func(t *testing.T) {
			loki := slidingRangeSeries(t, lokiURL, query, s0.Add(10*time.Minute), s0.Add(25*time.Minute), time.Minute, nil)
			proxy := slidingRangeSeries(t, proxyURL, query, s0.Add(10*time.Minute), s0.Add(25*time.Minute), time.Minute, nil)
			compareUnwrapSumSeries(t, query, loki, proxy)
		})
	}

	// sum, max, min and quantile over time on query_range are windowed by the
	// proxy as it windows every unwrap (semantics/unwrap-over-time-range-windows,
	// open), whatever the conversion, so their values are not compared with
	// Loki's here. What this gap owns is asserted instead: Loki answers a
	// non-empty matrix, the proxy answers the series Loki does, and the proxy
	// answers duration_seconds() exactly as it answers duration().
	for _, shape := range []string{
		`sum by (service_name) (sum_over_time(%s | logfmt | unwrap %s(okdur) [5m]))`,
		`sum by (service_name) (sum_over_time(%s | logfmt | unwrap %s(okdur) [1m]))`,
		`max by (service_name) (max_over_time(%s | logfmt | unwrap %s(okdur) [5m]))`,
		`min by (service_name) (min_over_time(%s | logfmt | unwrap %s(okdur) [5m]))`,
		`max by (service_name) (quantile_over_time(0.5, %s | logfmt | unwrap %s(okdur) [5m]))`,
	} {
		query := fmt.Sprintf(shape, sel, "duration_seconds")
		t.Run("range series/"+query, func(t *testing.T) {
			loki := slidingRangeSeries(t, lokiURL, query, s0.Add(10*time.Minute), s0.Add(25*time.Minute), time.Minute, nil)
			proxy := slidingRangeSeries(t, proxyURL, query, s0.Add(10*time.Minute), s0.Add(25*time.Minute), time.Minute, nil)
			points := 0
			for _, series := range loki {
				points += len(series)
			}
			if points == 0 {
				t.Fatalf("%s: Loki answered no sample; parity would be empty-vs-empty", query)
			}
			if len(proxy) != len(loki) {
				t.Fatalf("%s: Loki has %d series, proxy %d", query, len(loki), len(proxy))
			}
			for key := range loki {
				if len(proxy[key]) == 0 {
					t.Errorf("%s: series {%s} is missing or empty on the proxy", query, key)
				}
			}
			asDuration := slidingRangeSeries(t, proxyURL, fmt.Sprintf(shape, sel, "duration"), s0.Add(10*time.Minute), s0.Add(25*time.Minute), time.Minute, nil)
			if fmt.Sprint(asDuration) != fmt.Sprint(proxy) {
				t.Errorf("%s: duration_seconds() and duration() differ on the proxy\n duration_seconds: %v\n duration:         %v", query, proxy, asDuration)
			}
		})
	}

	at := s0.Add(20 * time.Minute)
	for _, query := range []string{
		`sum by (service_name) (sum_over_time(` + sel + ` | logfmt | unwrap duration_seconds(okdur) [5m]))`,
		`sum(rate(` + sel + ` | logfmt | unwrap duration_seconds(okdur) [5m]))`,
		`max by (service_name) (max_over_time(` + sel + ` | logfmt | unwrap duration_seconds(okdur) [5m]))`,
	} {
		t.Run("instant/"+query, func(t *testing.T) {
			loki := unwrapInstant(t, lokiURL, query, at)
			proxy := unwrapInstant(t, proxyURL, query, at)
			if len(loki) == 0 {
				t.Fatalf("%s: Loki answered no sample; parity would be empty-vs-empty", query)
			}
			compareUnwrapSumSeries(t, query, durationSecondsInstantMatrix(loki, at), durationSecondsInstantMatrix(proxy, at))
		})
	}

	// A value time.ParseDuration rejects is Loki's SampleExtractionErr for
	// duration_seconds() as for duration(); a filter after the unwrap that
	// drops the error turns it into the 200 of the rows that convert.
	for _, tc := range []struct {
		name, path string
		params     url.Values
		want       int
	}{
		{"text", rangePath, window(`sum by (service_name) (sum_over_time(` + sel + ` | logfmt | unwrap duration_seconds(bad) [5m]))`), http.StatusBadRequest},
		{"text, ungrouped rate", rangePath, window(`sum(rate(` + sel + ` | logfmt | unwrap duration_seconds(bad) [5m]))`), http.StatusBadRequest},
		{"text, quantile", rangePath, window(`max by (service_name) (quantile_over_time(0.5, ` + sel + ` | logfmt | unwrap duration_seconds(bad) [5m]))`), http.StatusBadRequest},
		{"text, instant", instantPath, url.Values{"query": {`sum by (service_name) (sum_over_time(` + sel + ` | logfmt | unwrap duration_seconds(bad) [5m]))`}, "time": {strconv.FormatInt(at.UnixNano(), 10)}}, http.StatusBadRequest},
		{"a plain number is not a duration", rangePath, window(`sum by (service_name) (sum_over_time(` + sel + ` | logfmt | unwrap duration_seconds(n) [5m]))`), http.StatusBadRequest},
		{"error dropped after the unwrap", rangePath, window(`sum by (service_name) (sum_over_time(` + sel + ` | logfmt | unwrap duration_seconds(bad) | __error__="" [5m]))`), http.StatusOK},
		{"missing label", rangePath, window(`sum by (service_name) (sum_over_time(` + sel + ` | logfmt | unwrap duration_seconds(nosuch) [5m]))`), http.StatusOK},
	} {
		t.Run(tc.name, func(t *testing.T) {
			loki := unwrapErrorGet(t, lokiURL, tc.path, tc.params)
			if loki.status != tc.want {
				t.Fatalf("Loki answered %d (%s), the expected answer is %d: Loki is not healthy for this case", loki.status, loki.body, tc.want)
			}
			proxy := unwrapErrorGet(t, proxyURL, tc.path, tc.params)
			if proxy.status != loki.status {
				t.Fatalf("proxy %d %s, Loki %d %s", proxy.status, proxy.body, loki.status, loki.body)
			}
			if loki.status == http.StatusBadRequest {
				if proxy.contentType != loki.contentType {
					t.Fatalf("content type: proxy %q, Loki %q", proxy.contentType, loki.contentType)
				}
				if p, l := unwrapErrorShape(t, proxy.body), unwrapErrorShape(t, loki.body); p != l {
					t.Fatalf("pipeline error differs\nproxy %s\nloki  %s", p, l)
				}
				if !strings.Contains(proxy.body, "time: invalid duration") && !strings.Contains(proxy.body, "time: missing unit") {
					t.Fatalf("expected time.ParseDuration's message, got %q", proxy.body)
				}
			}
		})
	}
}

// durationSecondsInstantMatrix gives an instant answer the shape of a range answer (one
// sample per series, at the evaluation time) for compareUnwrapSumSeries.
func durationSecondsInstantMatrix(series map[string]string, at time.Time) map[string]map[string]string {
	out := make(map[string]map[string]string, len(series))
	for key, value := range series {
		out[key] = map[string]string{strconv.FormatInt(at.Unix(), 10): value}
	}
	return out
}
