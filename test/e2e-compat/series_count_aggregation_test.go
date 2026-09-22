//go:build e2e

package e2e_compat

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"
)

// conformance: metric-series-identity, series-limits-and-partial-results
// count/min/max/avg of a range aggregation read one value per Loki series.
// count by (pod) (count_over_time(...)) is the number of series per pod, not
// the number of lines, and max by (...) is the largest per-series count.
func TestCompat_SeriesCountAggregations(t *testing.T) {
	probe := fmt.Sprintf("seragg%d", time.Now().UnixNano())
	// pod -> lines in the window.
	pods := map[string]int{"p1": 5, "p2": 3, "p3": 1}
	base := time.Now().Add(-2 * time.Minute).Truncate(time.Second)
	var vlRows strings.Builder
	var lokiStreams []any
	for pod, lines := range pods {
		labels := map[string]string{"probe": probe, "pod": pod, "level": "info"}
		var values [][]string
		for i := 0; i < lines; i++ {
			stamp := base.Add(time.Duration(i) * time.Second)
			line := fmt.Sprintf("series aggregation fixture %s line %d", pod, i)
			values = append(values, []string{strconv.FormatInt(stamp.UnixNano(), 10), line})
			row := map[string]string{"_time": stamp.UTC().Format(time.RFC3339Nano), "_msg": line}
			for k, v := range labels {
				row[k] = v
			}
			encoded, _ := json.Marshal(row)
			vlRows.Write(encoded)
			vlRows.WriteByte('\n')
		}
		lokiStreams = append(lokiStreams, map[string]any{"stream": labels, "values": values})
	}
	status, body := hardeningRequest(t, http.MethodPost, vlURL+"/insert/jsonline?_stream_fields="+url.QueryEscape("probe,pod,level"), vlRows.String(), map[string]string{"Content-Type": "application/stream+json"})
	if status != http.StatusOK {
		t.Fatalf("VL ingest: %d %s", status, body)
	}
	payload, _ := json.Marshal(map[string]any{"streams": lokiStreams})
	if status, body := hardeningRequest(t, http.MethodPost, lokiURL+"/loki/api/v1/push", string(payload), map[string]string{"Content-Type": "application/json"}); status != http.StatusNoContent {
		t.Fatalf("Loki ingest: %d %s", status, body)
	}
	forceVLFlush(t)

	at := base.Add(time.Minute)
	selector := fmt.Sprintf(`{probe=%q}`, probe)
	// Expected values are Loki's: three streams, one per pod.
	for _, tc := range []struct {
		query string
		want  map[string]float64
	}{
		{fmt.Sprintf(`count by (pod) (count_over_time(%s[5m]))`, selector), map[string]float64{"p1": 1, "p2": 1, "p3": 1}},
		{fmt.Sprintf(`count(count_over_time(%s[5m]))`, selector), map[string]float64{"": 3}},
		{fmt.Sprintf(`max(count_over_time(%s[5m]))`, selector), map[string]float64{"": 5}},
		{fmt.Sprintf(`min(count_over_time(%s[5m]))`, selector), map[string]float64{"": 1}},
		{fmt.Sprintf(`avg(count_over_time(%s[5m]))`, selector), map[string]float64{"": 3}},
		{fmt.Sprintf(`max by (pod) (count_over_time(%s[5m]))`, selector), map[string]float64{"p1": 5, "p2": 3, "p3": 1}},
		// `without` drops the label and aggregates what remains, per operator.
		{fmt.Sprintf(`count without (pod) (count_over_time(%s[5m]))`, selector), map[string]float64{"": 3}},
		{fmt.Sprintf(`max without (pod) (count_over_time(%s[5m]))`, selector), map[string]float64{"": 5}},
		{fmt.Sprintf(`min without (pod) (count_over_time(%s[5m]))`, selector), map[string]float64{"": 1}},
		{fmt.Sprintf(`avg without (pod) (count_over_time(%s[5m]))`, selector), map[string]float64{"": 3}},
		{fmt.Sprintf(`sum without (pod) (count_over_time(%s[5m]))`, selector), map[string]float64{"": 9}},
		// sum keeps counting lines.
		{fmt.Sprintf(`sum by (pod) (count_over_time(%s[5m]))`, selector), map[string]float64{"p1": 5, "p2": 3, "p3": 1}},
		{fmt.Sprintf(`sum(count_over_time(%s[5m]))`, selector), map[string]float64{"": 9}},
	} {
		t.Run(tc.query, func(t *testing.T) {
			var loki, proxy map[string]float64
			deadline := time.Now().Add(60 * time.Second)
			for {
				loki = seriesAggregationInstant(t, lokiURL, tc.query, at)
				proxy = seriesAggregationInstant(t, proxyURL, tc.query, at)
				// Both sides must have the whole fixture: a match on Loki
				// alone can read a proxy answer that VictoriaLogs has not
				// finished ingesting.
				if (fmt.Sprint(loki) == fmt.Sprint(tc.want) && fmt.Sprint(proxy) == fmt.Sprint(tc.want)) || time.Now().After(deadline) {
					break
				}
				time.Sleep(2 * time.Second)
			}
			if fmt.Sprint(loki) != fmt.Sprint(tc.want) {
				t.Fatalf("Loki answered %v, expected %v", loki, tc.want)
			}
			if fmt.Sprint(proxy) != fmt.Sprint(loki) {
				t.Fatalf("proxy=%v loki=%v", proxy, loki)
			}
		})
	}
}

// seriesAggregationInstant returns an instant vector as pod label -> value.
func seriesAggregationInstant(t *testing.T, base, query string, at time.Time) map[string]float64 {
	t.Helper()
	params := url.Values{"query": {query}, "time": {strconv.FormatInt(at.Unix(), 10)}}
	status, body := hardeningRequest(t, http.MethodGet, base+"/loki/api/v1/query?"+params.Encode(), "", map[string]string{"X-Scope-OrgID": "0"})
	var response struct {
		Status string `json:"status"`
		Data   struct {
			ResultType string `json:"resultType"`
			Result     []struct {
				Metric map[string]string `json:"metric"`
				Value  []json.RawMessage `json:"value"`
			} `json:"result"`
		} `json:"data"`
	}
	if status != http.StatusOK || json.Unmarshal(body, &response) != nil || response.Status != "success" || response.Data.ResultType != "vector" {
		t.Fatalf("%s %s: %d %s", base, query, status, body)
	}
	out := map[string]float64{}
	for _, sample := range response.Data.Result {
		var raw string
		if len(sample.Value) != 2 || json.Unmarshal(sample.Value[1], &raw) != nil {
			t.Fatalf("%s %s: unexpected sample in %s", base, query, body)
		}
		value, err := strconv.ParseFloat(raw, 64)
		if err != nil {
			t.Fatalf("%s: invalid value in %s", base, body)
		}
		out[sample.Metric["pod"]] = value
	}
	return out
}
