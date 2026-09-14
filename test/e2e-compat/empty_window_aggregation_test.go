//go:build e2e

package e2e_compat

import (
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"
)

type emptyWindowVectorResponse struct {
	parserErrorMetricResponse
	Warnings []string `json:"warnings"`
}

// Loki aggregates an empty input vector into an empty vector. VictoriaLogs
// answers an ungrouped stats pipe over no rows with one zero or NaN row, which
// must not leak through as a sample.
func TestRangeMetricCompatibilityEmptyWindowAggregations(t *testing.T) {
	app := fmt.Sprintf("empty-window-agg-%d", time.Now().UnixNano())
	fixtureEnd := time.Now().Add(-3 * time.Hour).Truncate(time.Minute)
	const lines = 6
	var vlRows strings.Builder
	lokiValues := map[string][][]string{}
	for i := 0; i < lines; i++ {
		stamp := fixtureEnd.Add(time.Duration(-240+40*i) * time.Second)
		pod := "p" + strconv.Itoa(i%2)
		message := fmt.Sprintf("empty window fixture line %d pod=%s", i, pod)
		row, _ := json.Marshal(map[string]string{"_time": stamp.UTC().Format(time.RFC3339Nano), "_msg": message, "app": app, "pod": pod})
		vlRows.Write(row)
		vlRows.WriteByte('\n')
		lokiValues[pod] = append(lokiValues[pod], []string{strconv.FormatInt(stamp.UnixNano(), 10), message})
	}
	status, body := hardeningRequest(t, http.MethodPost, vlURL+"/insert/jsonline?_stream_fields=app,pod", vlRows.String(), map[string]string{"Content-Type": "application/stream+json"})
	if status != http.StatusOK {
		t.Fatalf("VL ingest: %d %s", status, body)
	}
	var streams []any
	for pod, values := range lokiValues {
		streams = append(streams, map[string]any{"stream": map[string]string{"app": app, "pod": pod}, "values": values})
	}
	payload, _ := json.Marshal(map[string]any{"streams": streams})
	status, body = hardeningRequest(t, http.MethodPost, lokiURL+"/loki/api/v1/push", string(payload), map[string]string{"Content-Type": "application/json"})
	if status != http.StatusNoContent {
		t.Fatalf("Loki ingest: %d %s", status, body)
	}
	forceVLFlush(t)
	// The fixture is older than query_ingesters_within; flush it to the store.
	if status, body = hardeningRequest(t, http.MethodPost, lokiURL+"/flush", "", nil); status >= 300 {
		t.Fatalf("Loki flush: %d %s", status, body)
	}

	selector := fmt.Sprintf(`{app=%q}`, app)
	waitForEmptyWindowFixture(t, app, selector, fixtureEnd, lines)

	emptyTime := fixtureEnd.Add(10 * time.Minute)
	for _, query := range []string{
		`sum(rate(%s[5m]))`,
		`sum(bytes_rate(%s[5m]))`,
		`sum(count_over_time(%s[5m]))`,
		`sum(bytes_over_time(%s[5m]))`,
		`sum by () (count_over_time(%s[5m]))`,
		`avg(rate(%s[5m]))`,
		`max(rate(%s[5m]))`,
		`count(rate(%s[5m]))`,
		`stddev(rate(%s[5m]))`,
		`stdvar(rate(%s[5m]))`,
		`sum(count_over_time(%s[5m])) / 60`,
		`sum(rate(%s[5m])) * 2`,
		`topk(3, sum(rate(%s[5m])))`,
		`sort(sum(rate(%s[5m])))`,
		`sum(sum by (pod) (count_over_time(%s[5m])))`,
	} {
		query := fmt.Sprintf(query, selector)
		t.Run(query, func(t *testing.T) {
			lokiInside := emptyWindowInstant(t, lokiURL, query, fixtureEnd)
			proxyInside := emptyWindowInstant(t, proxyURL, query, fixtureEnd)
			if len(lokiInside) != 1 || len(proxyInside) != 1 {
				t.Fatalf("inside the fixture both sides need one sample: loki=%v proxy=%v", lokiInside, proxyInside)
			}
			if math.IsNaN(lokiInside[0]) || math.Abs(lokiInside[0]-proxyInside[0]) > 1e-9 {
				t.Fatalf("inside the fixture: loki=%v proxy=%v", lokiInside, proxyInside)
			}
			if loki, proxy := emptyWindowInstant(t, lokiURL, query, emptyTime), emptyWindowInstant(t, proxyURL, query, emptyTime); len(loki) != 0 || len(proxy) != 0 {
				t.Fatalf("empty window: loki=%v proxy=%v", loki, proxy)
			}
		})
	}
	t.Run("filtered to no lines inside the fixture", func(t *testing.T) {
		query := fmt.Sprintf(`sum(count_over_time(%s |= "no such line" [5m]))`, selector)
		if loki, proxy := emptyWindowInstant(t, lokiURL, query, fixtureEnd), emptyWindowInstant(t, proxyURL, query, fixtureEnd); len(loki) != 0 || len(proxy) != 0 {
			t.Fatalf("loki=%v proxy=%v", loki, proxy)
		}
	})
}

// emptyWindowInstant returns the sample values of an unlabelled instant vector.
func emptyWindowInstant(t *testing.T, base, query string, at time.Time) []float64 {
	t.Helper()
	params := url.Values{"query": {query}, "time": {strconv.FormatInt(at.Unix(), 10)}}
	status, body := hardeningRequest(t, http.MethodGet, base+"/loki/api/v1/query?"+params.Encode(), "", map[string]string{"X-Scope-OrgID": "0"})
	var response emptyWindowVectorResponse
	if status != http.StatusOK || json.Unmarshal(body, &response) != nil || response.Status != "success" || response.Data.ResultType != "vector" || len(response.Warnings) != 0 {
		t.Fatalf("%s: %d %s", base, status, body)
	}
	values := make([]float64, 0, len(response.Data.Result))
	for _, sample := range response.Data.Result {
		var raw string
		if len(sample.Metric) != 0 || len(sample.Value) != 2 || json.Unmarshal(sample.Value[1], &raw) != nil {
			t.Fatalf("%s: unexpected sample in %s", base, body)
		}
		value, err := strconv.ParseFloat(raw, 64)
		if err != nil {
			t.Fatalf("%s: invalid value in %s", base, body)
		}
		values = append(values, value)
	}
	return values
}

// waitForEmptyWindowFixture proves both backends hold the whole fixture before
// any comparison: Loki's instant count and VictoriaLogs' native count match.
func waitForEmptyWindowFixture(t *testing.T, app, selector string, fixtureEnd time.Time, lines int) {
	t.Helper()
	want := strconv.Itoa(lines)
	vlParams := url.Values{
		"query": {fmt.Sprintf(`app:=%q | stats count() as n`, app)},
		"time":  {strconv.FormatInt(fixtureEnd.Unix(), 10)},
		"start": {strconv.FormatInt(fixtureEnd.Add(-5*time.Minute).Unix(), 10)},
		"end":   {strconv.FormatInt(fixtureEnd.Unix(), 10)},
	}
	deadline := time.Now().Add(3 * time.Minute)
	for {
		loki := emptyWindowInstantRaw(t, lokiURL+"/loki/api/v1/query?"+url.Values{"query": {"sum(count_over_time(" + selector + "[5m]))"}, "time": {strconv.FormatInt(fixtureEnd.Unix(), 10)}}.Encode())
		vl := emptyWindowInstantRaw(t, vlURL+"/select/logsql/stats_query?"+vlParams.Encode())
		if loki == want && vl == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("fixture not visible on both backends: loki=%q vl=%q want %s", loki, vl, want)
		}
		select {
		case <-t.Context().Done():
			t.Fatal(t.Context().Err())
		case <-time.After(2 * time.Second):
		}
	}
}

func emptyWindowInstantRaw(t *testing.T, target string) string {
	t.Helper()
	status, body := hardeningRequest(t, http.MethodGet, target, "", map[string]string{"X-Scope-OrgID": "0"})
	var response emptyWindowVectorResponse
	if status != http.StatusOK || json.Unmarshal(body, &response) != nil || len(response.Warnings) != 0 || len(response.Data.Result) != 1 || len(response.Data.Result[0].Value) != 2 {
		return ""
	}
	var raw string
	_ = json.Unmarshal(response.Data.Result[0].Value[1], &raw)
	return raw
}
