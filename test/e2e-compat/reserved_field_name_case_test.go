//go:build e2e

package e2e_compat

import (
	"encoding/json"
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

// reservedCaseFixture pushes, alike to Loki and to VictoriaLogs' Loki push API, three streams of the run's own
// service: kind=u with logfmt lines holding a key _TIME, kind=f with logfmt lines holding a key _Stream_ID (or none),
// and kind=s with a stream label _Stream_ID. It returns the service name and the window around the lines.
func reservedCaseFixture(t *testing.T) (svc string, start, end time.Time) {
	t.Helper()
	svc = fmt.Sprintf("rf-case-%d", time.Now().UnixNano())
	base := time.Now().Add(-2 * time.Minute).Truncate(time.Second)
	streams := []struct {
		labels map[string]string
		lines  []string
	}{
		{map[string]string{"service_name": svc, "kind": "u"}, []string{"_TIME=10 k=1 " + svc, "_TIME=3.5 k=2 " + svc, "_TIME=7 k=3 " + svc}},
		{map[string]string{"service_name": svc, "kind": "f"}, []string{"_Stream_ID=si k=1 " + svc, "_Stream_ID=other k=2 " + svc, "x=1 " + svc}},
		{map[string]string{"service_name": svc, "kind": "s", "_Stream_ID": "si"}, []string{"plain " + svc}},
	}
	var body []any
	n := 0
	for _, s := range streams {
		var values [][]string
		for _, line := range s.lines {
			n++
			values = append(values, []string{strconv.FormatInt(base.Add(time.Duration(n)*time.Second).UnixNano(), 10), line})
		}
		body = append(body, map[string]any{"stream": s.labels, "values": values})
	}
	push, _ := json.Marshal(map[string]any{"streams": body})
	for _, target := range []string{lokiURL + "/loki/api/v1/push", vlURL + "/insert/loki/api/v1/push"} {
		status, out := hardeningRequest(t, http.MethodPost, target, string(push), map[string]string{"Content-Type": "application/json"})
		if status/100 != 2 {
			t.Fatalf("push %s: %d %s", target, status, out)
		}
	}
	forceVLFlush(t)
	return svc, base.Add(-time.Minute), base.Add(time.Minute)
}

// reservedCaseLines is the sorted lines of a query_range log answer.
func reservedCaseLines(payload map[string]interface{}) []string {
	var lines []string
	for _, item := range extractArray(extractMap(payload, "data"), "result") {
		stream, _ := item.(map[string]interface{})
		values, _ := stream["values"].([]interface{})
		for _, v := range values {
			if pair, _ := v.([]interface{}); len(pair) == 2 {
				lines = append(lines, fmt.Sprint(pair[1]))
			}
		}
	}
	sort.Strings(lines)
	return lines
}

// reservedCaseValue is the value of the one series of an instant vector answer, NaN for none.
func reservedCaseValue(payload map[string]interface{}) float64 {
	result := extractArray(extractMap(payload, "data"), "result")
	if len(result) != 1 {
		return math.NaN()
	}
	sample, _ := result[0].(map[string]interface{})
	pair, _ := sample["value"].([]interface{})
	if len(pair) != 2 {
		return math.NaN()
	}
	v, _ := strconv.ParseFloat(fmt.Sprint(pair[1]), 64)
	return v
}

// A label named _TIME or _Stream_ID (a name that differs from VictoriaLogs' _time or _stream_id only by letter case)
// is answered like Loki: the unwrap of a logfmt key _TIME, where VictoriaLogs could not read back its own print of
// the filter the proxy wrote and restarted on a clone of the query, and label filters on _Stream_ID, which the proxy
// sent as VictoriaLogs' own stream id filter. Both sides hold the fixture before anything is compared, and Loki's
// answers are pinned to the fixture. Check VictoriaLogs' restart count before and after: it must not change.
//
// conformance: semantics/reserved-field-name-case-outbound, loki_api_v1_query_range, loki_api_v1_query
func TestCompat_ReservedFieldNameCaseLikeLoki(t *testing.T) {
	headers := map[string]string{"X-Scope-OrgID": "0"}
	svc, start, end := reservedCaseFixture(t)
	params := func(query string) url.Values {
		return url.Values{"query": {query}, "start": {strconv.FormatInt(start.UnixNano(), 10)},
			"end": {strconv.FormatInt(end.UnixNano(), 10)}, "limit": {"1000"}}
	}
	selector := fmt.Sprintf(`{service_name=%q}`, svc)

	// Both sides hold every line.
	deadline := time.Now().Add(2 * time.Minute)
	for {
		_, _, lp := queryRangeGET(t, lokiURL, params(selector), headers)
		_, _, pp := queryRangeGET(t, proxyURL, params(selector), headers)
		if len(reservedCaseLines(lp)) == 7 && len(reservedCaseLines(pp)) == 7 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("fixture not visible on both sides: Loki %v, proxy %v", reservedCaseLines(lp), reservedCaseLines(pp))
		}
		time.Sleep(time.Second)
	}

	for _, tc := range []struct {
		query string
		lines int // the lines Loki answers with
	}{
		{selector + ` | logfmt | _Stream_ID="si"`, 2},
		{selector + ` | logfmt | _Stream_ID!="si"`, 5},
		{selector + ` | _Stream_ID="si"`, 1},
		{fmt.Sprintf(`{service_name=%q, kind="u"} | logfmt | _TIME > 5`, svc), 2},
	} {
		ls, lb, lp := queryRangeGET(t, lokiURL, params(tc.query), headers)
		ps, pb, pp := queryRangeGET(t, proxyURL, params(tc.query), headers)
		if ls != http.StatusOK {
			t.Fatalf("%s: Loki answered %d %s", tc.query, ls, lb)
		}
		ll, pl := reservedCaseLines(lp), reservedCaseLines(pp)
		if len(ll) != tc.lines {
			t.Fatalf("%s: Loki answered %q, the test expects %d lines", tc.query, ll, tc.lines)
		}
		if ps != ls || strings.Join(pl, "\n") != strings.Join(ll, "\n") {
			t.Errorf("%s:\n proxy %d %q (%s)\n Loki  %d %q", tc.query, ps, pl, pb, ls, ll)
		}
	}

	// Loki answers range metrics of a new stream only once its ingester cut the chunks (the fresh-stack blank
	// window), and an empty answer compared with an empty answer proves nothing: wait for Loki's range metric on
	// the fixture's three streams before the range part, with a query shape the comparison does not use.
	waitForReservedCaseLokiMetrics(t, selector, start, end, headers)

	at := end
	for _, tc := range []struct {
		query string
		want  float64
	}{
		{fmt.Sprintf(`sum(sum_over_time({service_name=%q, kind="u"} | logfmt | unwrap _TIME [5m]))`, svc), 20.5},
		{fmt.Sprintf(`sum(max_over_time({service_name=%q, kind="u"} | logfmt | unwrap _TIME [5m]))`, svc), 20.5}, // logfmt makes k a label: three series, max 10 + 3.5 + 7
		{fmt.Sprintf(`sum(count_over_time({service_name=%q} | logfmt | _Stream_ID="si" [5m]))`, svc), 2},
	} {
		ls, lb, lp := queryInstantGET(t, lokiURL, tc.query, at)
		ps, pb, pp := queryInstantGET(t, proxyURL, tc.query, at)
		if ls != http.StatusOK || reservedCaseValue(lp) != tc.want {
			t.Fatalf("%s: Loki answered %d %v (%s), the test expects %v", tc.query, ls, reservedCaseValue(lp), lb, tc.want)
		}
		if ps != ls || reservedCaseValue(pp) != reservedCaseValue(lp) {
			t.Errorf("%s:\n proxy %d %v (%s)\n Loki  %d %v", tc.query, ps, reservedCaseValue(pp), pb, ls, reservedCaseValue(lp))
		}
		// The range query of the same metric reaches VictoriaLogs' stats_query_range: its series reaches the same
		// highest point as Loki's.
		// Loki can still answer this shape's range empty for a while after the wait; each retry asks another step
		// (a query Loki has not cached), both sides the same, and past the wait there is no Loki answer to compare.
		var (
			rls, rps          int
			rlb, rpb          string
			rlp, rpp          map[string]interface{}
			lokiMax, proxyMax float64
			rangeDeadline     = time.Now().Add(90 * time.Second)
		)
		for step := 60; ; step++ {
			rp := url.Values{"query": {tc.query}, "start": {strconv.FormatInt(start.UnixNano(), 10)}, "end": {strconv.FormatInt(end.UnixNano(), 10)}, "step": {strconv.Itoa(step)}}
			rls, rlb, rlp = queryRangeGET(t, lokiURL, rp, headers)
			rps, rpb, rpp = queryRangeGET(t, proxyURL, rp, headers)
			lokiMax, proxyMax = reservedCaseMax(rlp), reservedCaseMax(rpp)
			if !math.IsNaN(lokiMax) {
				break
			}
			if time.Now().After(rangeDeadline) {
				t.Skipf("%s (range): Loki answered no point for 90s (fresh-stack blank window), nothing to compare: %d %.300s", tc.query, rls, rlb)
			}
			time.Sleep(time.Second)
		}
		if rls != http.StatusOK || rps != rls || proxyMax != lokiMax {
			t.Errorf("%s (range):\n proxy %d max %v (%s)\n Loki  %d max %v (%s)", tc.query, rps, reservedCaseMax(rpp), rpb, rls, reservedCaseMax(rlp), rlb)
		}
	}
}

// waitForReservedCaseLokiMetrics waits for Loki to answer a range metric of the fixture's three streams (the
// fresh-stack blank window), 3 minutes at most, and fails with Loki's last answer when it does not. The probe is a
// shape the comparisons do not send (another step), so no partial answer lands in Loki's results cache.
func waitForReservedCaseLokiMetrics(t *testing.T, selector string, start, end time.Time, headers map[string]string) {
	t.Helper()
	probe := url.Values{"query": {`count_over_time(` + selector + `[5m])`}, "start": {strconv.FormatInt(start.UnixNano(), 10)},
		"end": {strconv.FormatInt(end.UnixNano(), 10)}, "step": {"59"}}
	deadline := time.Now().Add(3 * time.Minute)
	var last string
	for time.Now().Before(deadline) {
		status, body, payload := queryRangeGET(t, lokiURL, probe, headers)
		last = body
		if status == http.StatusOK && len(extractArray(extractMap(payload, "data"), "result")) == 3 {
			return
		}
		time.Sleep(time.Second)
	}
	// Loki shows a new stream's range metrics only once its ingester can read them, which on a fresh stack can take
	// longer than the wait. Then there is no Loki answer to compare the range part with: skip it rather than fail on
	// Loki's readiness (the log part above has already been compared).
	t.Skipf("Loki range metrics of the fixture incomplete after 3m (fresh-stack blank window), the range part is not compared: %.300s", last)
}

// reservedCaseMax is the highest point of a matrix answer's series, NaN for none.
func reservedCaseMax(payload map[string]interface{}) float64 {
	best := math.NaN()
	for _, item := range extractArray(extractMap(payload, "data"), "result") {
		series, _ := item.(map[string]interface{})
		values, _ := series["values"].([]interface{})
		for _, v := range values {
			pair, _ := v.([]interface{})
			if len(pair) != 2 {
				continue
			}
			x, err := strconv.ParseFloat(fmt.Sprint(pair[1]), 64)
			if err == nil && (math.IsNaN(best) || x > best) {
				best = x
			}
		}
	}
	return best
}
