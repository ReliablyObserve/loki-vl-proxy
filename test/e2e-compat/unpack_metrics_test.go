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

// unpackFixture pushes, to Loki and VictoriaLogs, a stream of packed lines (a JSON object with a string `_entry`),
// lines without an `_entry` and a numeric `_entry`, one every 5 seconds for the last 20 minutes, under the app label
// it returns, and waits until Loki and the proxy count every line.
func unpackFixture(t *testing.T, prefix string, extra ...string) (app string, now time.Time) {
	t.Helper()
	now = time.Now().UTC().Truncate(time.Second)
	app = fmt.Sprintf("%s%d", prefix, time.Now().UnixNano())
	shapes := append([]string{`{"agent":"a","_entry":"packed %d"}`, `{"agent":"a","sample":%d}`, `{"agent":"b","_entry":%d}`}, extra...)
	var lines []string
	for i := 0; i < 240; i++ {
		lines = append(lines, fmt.Sprintf(shapes[i%len(shapes)], i))
	}
	pushStream(t, now.Add(-20*time.Minute), streamDef{Labels: map[string]string{"app": app}, Lines: lines, Interval: 5 * time.Second})
	forceVLFlush(t)
	deadline := time.Now().Add(240 * time.Second)
	for _, base := range []string{lokiURL, proxyURL} {
		for {
			params := url.Values{"query": {fmt.Sprintf(`sum(count_over_time({app=%q}[30m]))`, app)}, "time": {strconv.FormatInt(time.Now().Unix(), 10)}}
			_, _, resp := doJSONGET(t, base+"/loki/api/v1/query?"+params.Encode(), map[string]string{"X-Scope-OrgID": "0"})
			if got := unpackSamples(resp); len(got) == 1 {
				for _, v := range got {
					if v == float64(len(lines)) {
						goto ready
					}
				}
			}
			if time.Now().After(deadline) {
				t.Fatalf("%s never counted the %d lines of %s", base, len(lines), app)
			}
			time.Sleep(2 * time.Second)
		}
	ready:
	}
	return app, now
}

// unpackSamples reads a metric answer as {label set @ timestamp: value}.
func unpackSamples(payload map[string]interface{}) map[string]float64 {
	out := map[string]float64{}
	data, _ := payload["data"].(map[string]interface{})
	result, _ := data["result"].([]interface{})
	for _, item := range result {
		series, _ := item.(map[string]interface{})
		metric, _ := series["metric"].(map[string]interface{})
		key := canonicalMetricKey(metric)
		points, _ := series["values"].([]interface{})
		if value, ok := series["value"].([]interface{}); ok {
			points = []interface{}{value}
		}
		for _, p := range points {
			pair, _ := p.([]interface{})
			if len(pair) == 2 {
				v, err := strconv.ParseFloat(fmt.Sprint(pair[1]), 64)
				if err == nil {
					out[key+" @"+fmt.Sprint(pair[0])] = v
				}
			}
		}
	}
	return out
}

// unpackAgainstLoki asks Loki and the proxy one metric query, instant at `at` or over the ten minutes before it, and
// holds the proxy to Loki's status and, for an answer, its series and values.
func unpackAgainstLoki(t *testing.T, query, endpoint string, at time.Time) {
	t.Helper()
	params := url.Values{"query": {query}}
	if endpoint == "query" {
		params.Set("time", strconv.FormatInt(at.UnixNano(), 10))
	} else {
		params.Set("start", strconv.FormatInt(at.Add(-10*time.Minute).Truncate(time.Minute).UnixNano(), 10))
		params.Set("end", strconv.FormatInt(at.Truncate(time.Minute).UnixNano(), 10))
		params.Set("step", "60")
	}
	headers := map[string]string{"X-Scope-OrgID": "0"}
	full := "/loki/api/v1/" + endpoint + "?" + params.Encode()
	lokiStatus, lokiBody, lokiPayload := doJSONGET(t, lokiURL+full, headers)
	proxyStatus, proxyBody, proxyPayload := doJSONGET(t, proxyURL+full, headers)
	if lokiStatus != proxyStatus {
		t.Fatalf("HTTP status loki=%d proxy=%d\nloki:  %s\nproxy: %s", lokiStatus, proxyStatus, lokiBody, proxyBody)
	}
	if lokiStatus != http.StatusOK {
		return
	}
	want, got := unpackSamples(lokiPayload), unpackSamples(proxyPayload)
	if len(want) == 0 {
		t.Fatalf("Loki answered no sample for %s", query)
	}
	if len(got) != len(want) {
		t.Fatalf("%d samples, Loki %d\nloki:  %s\nproxy: %s", len(got), len(want), lokiBody, proxyBody)
	}
	for k, v := range want {
		if g, ok := got[k]; !ok || math.Abs(g-v) > 1e-9*math.Max(1, math.Abs(v)) {
			t.Errorf("%s: proxy %v, Loki %v", k, g, v)
		}
	}
}

// A sum with no clause over an unpack is one series in Loki, at an instant too: the proxy answered a sum of a rate
// with one series per stream there. A count over a line an unpack rejects fails with Loki's pipeline error under any
// function but a sum with no grouping, where the proxy answered.
//
// conformance: semantics/unpack-instant-sum-rate-per-stream, semantics/unpack-parser-error-not-raised, loki_api_v1_query_range, loki_api_v1_query
func TestCompat_UnpackSumsLikeLoki(t *testing.T) {
	app, now := unpackFixture(t, "unpacksum")
	selector := fmt.Sprintf(`{app=%q}`, app)
	badApp, badNow := unpackFixture(t, "unpackbad", `not json %d`, `["%d"]`, `{"a":"%d"`)
	bad := fmt.Sprintf(`count_over_time({app=%q} | unpack [5m])`, badApp)
	for i, query := range []string{
		bad, `sum by (app) (` + bad + `)`, `topk(2, ` + bad + `)`, `sum(` + bad + `)`,
		`rate({app="` + badApp + `"} |= "packed" | unpack [5m])`,
		`count_over_time({app="` + badApp + `"} | unpack | drop __error__, __error_details__ [5m])`,
	} {
		for _, endpoint := range []string{"query", "query_range"} {
			t.Run("errors/"+endpoint+"/"+strconv.Itoa(i), func(t *testing.T) {
				unpackAgainstLoki(t, query, endpoint, badNow)
			})
		}
	}
	for i, query := range []string{
		`sum(rate(` + selector + ` | unpack [5m]))`,
		`sum(rate(` + selector + ` | unpack | drop agent [5m]))`,
		`sum(count_over_time(` + selector + ` | unpack [5m]))`,
		`label_replace(sum(rate(` + selector + ` | unpack [5m])), "d", "x", "d", ".*")`,
		`sum(absent_over_time({app="no-unpack-stream"} | unpack [5m]))`,
		`absent_over_time({app="no-unpack-stream"} | unpack [5m])`,
	} {
		for _, endpoint := range []string{"query", "query_range"} {
			t.Run(endpoint+"/"+strconv.Itoa(i), func(t *testing.T) {
				unpackAgainstLoki(t, query, endpoint, now)
			})
		}
	}
}
